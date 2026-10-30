"""TSP(티타늄 승화 펌프) 드라이버 — RS-232 Window 프로토콜, MOXA NPort TCP(192.168.1.50:4004) 경유.
v1 device/tsp.py(AsyncTSP)의 프레임·응답 해석·연결 방식을 다시 쓴 것(import 하지 않음).

프레임(v1 device/tsp.py:72-87)
  <STX 02><ADDR 0x80><WIN 3 ASCII><COM '0' 읽기/'1' 쓰기><DATA ASCII?><ETX 03><CRC 2 ASCII>
  CRC = ADDR~ETX 의 XOR 을 대문자 16진 2글자. 명령은 셋뿐: ON(011 ← '1')·OFF(011 ← '0')·상태(205 읽기).
응답(device/tsp.py:266-336): 1바이트 코드(ACK 06·NACK 15·32~35 — TSP_TOLERATE_SHORT_RESP 일 때) 또는
  STX…ETX+CRC 프레임 — 코드만 담은 <ADDR><CODE> 또는 데이터 <ADDR><WIN>[<COM>]<DATA>. CRC 가 틀리면 프로토콜 오류.
  ⚠ v1 해석 그대로: 데이터 첫 글자가 '0'/'1' 이면 COM 으로 보고 뗀다(구현 A 의 데이터가 0/1 로 시작하면 잘림).
읽기 한 바이트마다 TSP_WRITE_TIMEOUT_S(1.2 s) 제한(v1 readexactly(1) 마다 wait_for). 보낸 뒤 TSP_POST_SEND_DELAY_MS(20 ms) 쉼.

연결(v1 근거: device/tsp.py:212-233·339-378, 사실 표 devices_v1facts "tsp")
- 필요할 때만 연결한다(ensure_open) — 상시 연결·keep-alive 질의 없음. 명령 하나가 통신 예외(무응답·NACK·CRC·
  창 오류 포함 — v1 read_win/write_win_logic 의 except Exception)로 끝나면 연결을 닫고, 다음 호출이 다시 연결한다.
  on/off 상태 확인 시간초과(E808)는 v1 처럼 닫지 않는다(_wait_status 는 read_win 밖).
  그래서 worker 의 연결 상태 기계(백오프)는 쓰지 않는다: OnDemandTransport 가 worker 에는 늘 '열림'으로 보이고
  실제 소켓은 명령이 처음 보낼 때 연다. 드라이버는 통신 예외를 TspError 로 바꿔 worker 가 끊고 백오프하지 않게 한다.
- SO_KEEPALIVE 는 TSP_TCP_KEEPALIVE(실효값 false — settings.json common, inv_cfg CFG-019)를 따른다.

동작 단위(3단계 tsp_engine 이 부른다 — 사이클·IG 연동은 여기 없음)
- on(on_done)  : 011 ← '1' → ACK 확인 → (TSP_VERIFY_WITH_STATUS) 205 를 바로 한 번, 그 뒤 TSP_STATUS_POLL_INTERVAL_S(0.05 s)
                 간격으로 1.5 s 창 안에서 {3 RAMP, 4 WAIT_SUBLIM, 5 SUBLIM} 이 나올 때까지(device/tsp.py:391-431).
- off(on_done) : 011 ← '0' → ACK → 205 == 0 STOP 확인(같은 창). safety=True 면 worker 의 안전 요청(purge)으로 한 번에.
- read_status(on_done) : 205 한 번. close() : 연결 닫기(v1 aclose — 공정 끝).
결과는 on_done(TspDone) — ok·사유 코드(reason)·v1 오류 코드(code: E804~E808, v1 errors/error_registry.py 에 등록된 것)·문구.
  v1 은 이 코드를 예외에 붙이지 않았다(등록만) — v2 는 이벤트·결과에 실어 둔다(문구는 v1 registry 의 것).

1초 행: TSP 를 쓰는 동안(begin_use ~ end_use)만 205 를 tick 마다 읽는다. 그 밖은 off(포트에 닿지 않음 — v1 처럼 상주 연결 없음).
  on/off 확인 중인 tick 은 205 를 따로 보내지 않고 확인 폴링의 마지막 값을 싣는다(v1 바이트 순서 유지).
bench(writes_allowed=False): on·off 는 BENCH_READ_ONLY(worker + 드라이버 이중 보호), 205 읽기는 허용.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from v2.common.snapshot import DeviceBlock, FieldDef, FType
from v2.common.snapshot_bus import Source
from v2.common.transport import Cancelled, ReadTimeout, TcpSettings, TcpTransport, Transport, TransportError
from v2.common.worker import SEQ_DONE, Request, Result, Sequence, Status, Worker

# ── 프로토콜 상수(v1 device/tsp.py:25-48) ──
STX, ETX = 0x02, 0x03
ADDR_RS232 = 0x80
WIN_ONOFF = "011"
WIN_STATUS = "205"
ACK, NACK = 0x06, 0x15
ERR_UNKNOWN_WIN, ERR_DATA_TYPE, ERR_OUT_OF_RANGE, ERR_WIN_DISABLED = 0x32, 0x33, 0x34, 0x35
SIMPLE_RESP_CODES = frozenset({ACK, NACK, ERR_UNKNOWN_WIN, ERR_DATA_TYPE, ERR_OUT_OF_RANGE, ERR_WIN_DISABLED})
WINDOW_ERROR_TEXT = {ERR_UNKNOWN_WIN: "Unknown Window", ERR_DATA_TYPE: "Data Type Error",
                     ERR_OUT_OF_RANGE: "Out of Range", ERR_WIN_DISABLED: "Window Disabled"}

STATUS_STOP, STATUS_FAIL, STATUS_WAIT_INTERLOCK = 0, 1, 2
STATUS_RAMP, STATUS_WAIT_SUBLIM, STATUS_SUBLIM = 3, 4, 5
STATUS_TEXT = {0: "STOP", 1: "FAIL", 2: "WAIT_INTERLOCK", 3: "RAMP", 4: "WAIT_SUBLIM", 5: "SUBLIM"}
WANT_ON = frozenset({STATUS_RAMP, STATUS_WAIT_SUBLIM, STATUS_SUBLIM})
WANT_OFF = frozenset({STATUS_STOP})

VERIFY_WINDOW_S = 1.5        # v1 on/off(wait_ok=1.5) — 설정 키 아님(device/tsp.py:391·401)
EXEC_TIMEOUT_S = 10.0        # 요청 하나 상한(v1 은 바이트마다 제한만 — 끝없이 이어지지 않게 둔 v2 상한)
TICK_DEADLINE_MS = 900

# v1 오류 코드(errors/error_registry.py:1456-1499·1565-1589) — 문구는 registry 의 title
V1_CODES = {
    "E804": "TSP 연결 안 됨", "E805": "TSP 프로토콜 응답 오류", "E806": "TSP 명령 거부",
    "E807": "TSP 예상 외 응답", "E808": "TSP 상태 대기 시간 초과", "E816": "TSP WIN 형식 오류",
    "E817": "TSP 데이터 누락", "E818": "TSP 프레임 구성 오류",
}

BUDGET = dict(
    queries_per_tick=1, bytes_tx=9, bytes_rx=13, min_gap_ms=20, fits_1s=True, alternate=None,
    note="TSP 를 쓰는 동안만 205 1회(보냄 9 B: 02 80 '205' '0' 03 CRC2, 받음 약 10~13 B — 데이터 길이 미확인) + "
         "보낸 뒤 20 ms(TSP_POST_SEND_DELAY_MS). 9600 bps 가정 시 약 25 ms + 장비 응답. 쓰지 않을 때 0(off). "
         "on/off 확인 중에는 확인 폴링(0.05 s)의 값을 싣고 따로 보내지 않는다.",
)


# ═══════════════════════ 예외 ═══════════════════════
class TspError(Exception):
    """TSP 명령 실패 공통 — reason(v2 사유 코드)·code(v1 E 코드)."""
    reason = "DEVICE_REJECTED"
    code: Optional[str] = None


class TspLinkError(TspError):
    """연결·송신 실패(v1 asyncio.open_connection/drain 예외)."""
    reason, code = "LINK_DOWN", "E804"


class TspNoResponse(TspError):
    """응답 바이트가 제한 시간 안에 오지 않음(v1 asyncio.TimeoutError)."""
    reason, code = "DEVICE_NO_RESPONSE", None


class TspProtocolError(TspError):
    """STX 아님·CRC 불일치·빈 응답(v1 TSPProtocolError)."""
    reason, code = "DEVICE_REJECTED", "E805"


class TspUnexpectedReply(TspProtocolError):
    """쓰기에 ACK 가 아닌 응답(v1 TSPProtocolError '예상치 못한 응답')."""
    code = "E807"


class TspNackError(TspError):
    reason, code = "DEVICE_REJECTED", "E806"


class TspWindowError(TspError):
    reason, code = "DEVICE_REJECTED", "E806"

    def __init__(self, err: int):
        super().__init__(WINDOW_ERROR_TEXT.get(err, f"Error code 0x{err:02X}"))
        self.err = err


class TspVerifyTimeout(TspError):
    """205 가 기대 상태로 바뀌지 않음(v1 TSPError '상태 확인 시간초과')."""
    reason, code = "VERIFY_MISMATCH", "E808"


class TspFrameError(TspError):
    """WIN 형식·쓰기 DATA 누락(v1 _build_frame 의 ValueError) — 보내기 전."""
    reason, code = "BAD_ARGS", "E818"


# ═══════════════════════ 프레임·해석(순수 함수) ═══════════════════════
def xor_bytes(data: bytes) -> int:
    x = 0
    for b in data:
        x ^= b
    return x & 0xFF


def build_frame(win3: str, write: bool, data_ascii: Optional[str], *, addr: int = ADDR_RS232) -> bytes:
    """v1 _build_frame 과 같은 바이트·같은 검사(device/tsp.py:72-87)."""
    if len(win3) != 3 or not win3.isdigit():
        raise ValueError(f"WIN은 3자리 숫자여야 합니다: {win3!r}")
    com = 0x31 if write else 0x30
    body = bytes([addr & 0xFF]) + win3.encode("ascii") + bytes([com])
    if write and data_ascii is None:
        raise ValueError("write에는 DATA가 필요합니다.")
    if data_ascii:
        body += data_ascii.encode("ascii")
    body += bytes([ETX])
    crc = f"{xor_bytes(body):02X}".encode("ascii")
    return bytes([STX]) + body + crc


def decode_ascii_numeric(s: str) -> int:
    return int(s.strip() or "0")


def parse_response(raw: bytes) -> Tuple[str, Optional[str]]:
    """(kind, data) — kind ACK/NACK/ERR/DATA/RAW. CRC 불일치는 TspProtocolError(v1 _parse_response, device/tsp.py:296-336)."""
    if len(raw) == 1 and raw[0] in SIMPLE_RESP_CODES:
        code = raw[0]
        if code == ACK:
            return ("ACK", None)
        if code == NACK:
            return ("NACK", None)
        return ("ERR", f"{code:02x}")
    if len(raw) < 1 + 1 + 1 + 2 or raw[0] != STX or raw[-3] != ETX:
        return ("RAW", raw.hex())
    if f"{xor_bytes(raw[1:-2]):02X}".encode("ascii") != raw[-2:]:
        raise TspProtocolError("CRC 불일치")
    body = raw[1:-3]
    if len(body) == 2 and body[1] in SIMPLE_RESP_CODES:
        code = body[1]
        if code == ACK:
            return ("ACK", None)
        if code == NACK:
            return ("NACK", None)
        return ("ERR", f"{code:02x}")
    if len(body) >= 4:
        data_start = 4
        if len(body) >= 5 and body[4] in (0x30, 0x31):
            data_start = 5
        return ("DATA", body[data_start:].decode("ascii", errors="ignore"))
    return ("RAW", raw.hex())


# ═══════════════════════ 필요할 때만 여는 전송 ═══════════════════════
class OnDemandTransport(Transport):
    """worker 에는 open() 이 늘 성공하는 전송으로 보이고, 실제 연결(inner)은 처음 보낼 때 연다(v1 ensure_open).
    close_link() 는 실제 연결만 닫는다(v1 aclose) — 다음 write 가 다시 연다. retarget() 은 주소가 바뀌었을 때 새 전송으로."""

    def __init__(self, clock, make_inner: Callable[[Any], Transport], key: Any = None) -> None:
        super().__init__(clock)
        self._make = make_inner
        self._key = key
        self.inner = make_inner(key)
        self._armed = False
        self.opens = 0                     # 실제로 연 횟수(시험·진단)

    @property
    def is_open(self) -> bool:
        return self._armed

    @property
    def link_open(self) -> bool:
        return self.inner.is_open

    def open(self) -> None:
        self._armed = True

    def close(self) -> None:
        self._armed = False
        self.close_link()

    def close_link(self) -> None:
        if self.inner.is_open:
            self.inner.close()

    def retarget(self, key: Any) -> None:
        if key != self._key:
            self.close_link()
            self._key = key
            self.inner = self._make(key)

    def ensure_open(self) -> None:
        if not self.inner.is_open:
            self.inner.open()
            self.opens += 1

    def reset_input(self) -> None:
        if self.inner.is_open:
            self.inner.reset_input()

    def write(self, data: bytes) -> None:
        self.ensure_open()
        self.inner.write(data)

    def read_until(self, deadline, terminator=None, size=None, cancel=None) -> bytes:
        if not self.inner.is_open:
            raise TransportError("TSP 연결 안 됨")
        return self.inner.read_until(deadline, terminator, size, cancel)


def socket_factory(keepalive: bool) -> Callable[[TcpSettings], Any]:
    """v1 ensure_open: SO_KEEPALIVE 를 TSP_TCP_KEEPALIVE 로(device/tsp.py:221-225). asyncio 기본처럼 TCP_NODELAY."""
    def make(s: TcpSettings):
        import socket
        sock = socket.create_connection((s.host, s.port), timeout=s.connect_timeout_s)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1 if keepalive else 0)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setblocking(False)
        return sock
    return make


def endpoint(cfg: Dict[str, Any]) -> Tuple[str, int, float, bool]:
    return (str(cfg["TSP_TCP_HOST"]), int(cfg["TSP_TCP_PORT"]),
            max(0.1, float(cfg["TSP_CONNECT_TIMEOUT_S"])), bool(cfg["TSP_TCP_KEEPALIVE"]))


def make_transport(clock, cfg: Dict[str, Any]) -> OnDemandTransport:
    """실장비용 전송(NPort TCP). 주소·연결 제한·keepalive 가 바뀌면 begin_use 가 retarget 한다."""
    def mk(key):
        host, port, ct, ka = key
        return TcpTransport(clock, TcpSettings(host, port, ct), socket_factory=socket_factory(ka))
    return OnDemandTransport(clock, mk, endpoint(cfg))


def make_sim_transport(clock, sim) -> OnDemandTransport:
    from v2.common.transport import SimTransport

    def mk(_k):
        f = getattr(sim, "make_transport", None)
        return f(clock) if f is not None else SimTransport(clock, sim)
    return OnDemandTransport(clock, mk)


# ═══════════════════════ 1초 행 블록 ═══════════════════════
def blocks() -> List[DeviceBlock]:
    return [DeviceBlock("tsp", "TSP", (
        FieldDef("status", FType.INT, "", "상태 코드", source="WIN 205",
                 desc="0 STOP·1 FAIL·2 WAIT_INTERLOCK·3 RAMP·4 WAIT_SUBLIM·5 SUBLIM(device/tsp.py:42-48)"),
        FieldDef("state", FType.STR, "", "상태", source="WIN 205",
                 enum_labels=tuple((v, v) for v in STATUS_TEXT.values())),
        FieldDef("link", FType.BOOL, "", "연결 열림", source="소켓", detail=True,
                 desc="필요할 때만 연결 — 통신 예외 뒤 닫힘"),
        FieldDef("last_cmd", FType.STR, "", "마지막 명령", source="driver", detail=True),
        FieldDef("last_ok", FType.BOOL, "", "마지막 명령 성공", source="driver", detail=True),
    ), desc="RS-232 Window 프로토콜, NPort 192.168.1.50:4004, 주소 0x80")]


# ═══════════════════════ 드라이버 ═══════════════════════
@dataclass(frozen=True)
class TspEvent:
    kind: str                      # command_result / link_closed / status_read_failed
    detail: str
    reason: Optional[str] = None
    code: Optional[str] = None     # v1 E 코드
    ok: Optional[bool] = None


@dataclass(frozen=True)
class TspDone:
    ok: bool
    action: str                    # on / off / status / close
    reason: Optional[str] = None
    code: Optional[str] = None
    detail: str = ""
    status: Optional[int] = None   # 마지막으로 읽은 205


class TspDriver:
    def __init__(self, clock, worker: Worker, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[TspEvent], None]] = None) -> None:
        self.clock, self.worker, self.cfg = clock, worker, cfg
        self.writes_allowed = writes_allowed
        self.on_event = on_event
        self.last_status: Optional[int] = None
        self.last_status_t: Optional[float] = None
        self.last_cmd: Optional[str] = None
        self.last_ok: Optional[bool] = None
        self._cmd_active = 0               # on/off 확인 중(1초 읽기는 그 값을 싣는다)
        self._lock = threading.Lock()
        self._source = Source("tsp", worker, self.source_read, ("tsp",), TICK_DEADLINE_MS, enabled=False)

    # ───────── 설정(호출 시점에 읽는다 — v1 reload_runtime_cfg 의 클램프 그대로) ─────────
    def _addr(self) -> int:
        a = self.cfg.get("TSP_RS232_ADDR")
        if a is None:
            a = self.cfg.get("TSP_ADDR", ADDR_RS232)           # 레거시 키(device/tsp.py:176-184)
        return int(a) & 0xFF

    def _io_s(self) -> float:
        return max(0.1, float(self.cfg["TSP_WRITE_TIMEOUT_S"]))

    def _post_s(self) -> float:
        return max(0.0, float(self.cfg["TSP_POST_SEND_DELAY_MS"]) / 1000.0)

    def _poll_s(self) -> float:
        return max(0.01, float(self.cfg["TSP_STATUS_POLL_INTERVAL_S"]))

    def _emit(self, kind: str, detail: str, reason=None, code=None, ok=None) -> None:
        if self.on_event:
            self.on_event(TspEvent(kind, detail, reason, code, ok))

    # ───────── 1초 읽기 ─────────
    def source(self) -> Source:
        """SnapshotBus 소스 — begin_use 전에는 꺼져 있다(off)."""
        return self._source

    @property
    def in_use(self) -> bool:
        return self._source.enabled

    def begin_use(self) -> None:
        """TSP 공정 시작(tsp_engine) — 1초 행에 205 읽기를 켠다. 주소가 바뀌었으면 다음 연결부터 새 주소(v1 은 실행마다 새 AsyncTSP)."""
        key = endpoint(self.cfg)

        def fn(session):                                      # 전송 바꾸기는 worker 스레드에서(입출력과 겹치지 않게)
            tr = session.transport
            if isinstance(tr, OnDemandTransport) and isinstance(tr.inner, TcpTransport):
                tr.retarget(key)
        self.worker.submit(Request("read", fn, deadline=self.clock.monotonic() + EXEC_TIMEOUT_S,
                                   name="tsp:retarget", confirms_link=False))
        self._source.enabled = True

    def end_use(self, close: bool = True) -> None:
        """TSP 공정 끝 — 1초 읽기를 끄고(off) 연결을 닫는다(v1 tsp_runtime finally 의 aclose)."""
        self._source.enabled = False
        if close:
            self.close()

    def source_read(self, session, seq: int) -> Dict[str, Any]:
        if self._cmd_active:
            st = self.last_status                              # on/off 확인 중 — 따로 보내지 않는다(확인 폴링의 값)
        else:
            st = self._guarded(session, lambda: self._get_status(session))
        return self._row(st)

    def _row(self, st: Optional[int]) -> Dict[str, Any]:
        tr = self.worker.transport
        link = tr.link_open if isinstance(tr, OnDemandTransport) else bool(getattr(tr, "is_open", False))
        return {"tsp.status": st, "tsp.state": None if st is None else STATUS_TEXT.get(st, str(st)),
                "tsp.link": link, "tsp.last_cmd": self.last_cmd, "tsp.last_ok": self.last_ok}

    # ───────── 주고받기(v1 _send·_recv_any·read_win·write_win_logic) ─────────
    def _close_link(self, session) -> None:
        tr = session.transport if session is not None else self.worker.transport
        if isinstance(tr, OnDemandTransport):
            was = tr.link_open
            tr.close_link()
            if was:
                self._emit("link_closed", "통신 예외 — 연결 닫음(다음 호출이 다시 연결)")
        else:
            tr.reset_input()

    def _guarded(self, session, fn: Callable[[], Any]) -> Any:
        """명령 하나 — 통신 예외면 연결을 닫고(v1 except Exception: aclose) TspError 로 바꾼다(worker 가 백오프하지 않게)."""
        try:
            return fn()
        except Cancelled:
            self._close_link(session)      # 안전 요청이 끊은 읽기 — 늦은 응답이 다음 명령에 섞이지 않게(v2: 닫음)
            raise
        except TspError:
            self._close_link(session)
            raise
        except ReadTimeout as e:
            self._close_link(session)
            raise TspNoResponse(f"응답 없음(부분 {e.partial.hex(' ') or '-'})") from e
        except TransportError as e:
            self._close_link(session)
            raise TspLinkError(f"연결·송신 실패: {e}") from e
        except ValueError as e:
            self._close_link(session)
            raise TspFrameError(str(e)) from e

    def _wait(self, session, dt: float) -> None:
        if dt <= 0:
            return
        if session.clock.wait_until(session.clock.monotonic() + dt, lambda: session.cancel.is_set()):
            raise Cancelled()

    def _read_byte(self, session) -> bytes:
        return session.read_until(size=1, deadline=session.clock.monotonic() + self._io_s())

    def _recv_any(self, session) -> bytes:
        b0 = self._read_byte(session)
        if bool(self.cfg["TSP_TOLERATE_SHORT_RESP"]) and b0[0] in SIMPLE_RESP_CODES:
            return b0
        if b0[0] != STX:
            raise TspProtocolError(f"STX 아님: 0x{b0[0]:02X}")
        buf = bytearray(b0)
        while True:
            b = self._read_byte(session)
            buf += b
            if b[0] == ETX:
                break
        crc = session.read_until(size=2, deadline=session.clock.monotonic() + self._io_s())
        return bytes(buf) + crc

    def _exchange(self, session, frame: bytes) -> Tuple[str, Optional[str]]:
        tr = session.transport
        if isinstance(tr, OnDemandTransport):
            tr.ensure_open()                                  # v1 ensure_open — 실패면 TransportError → TspLinkError
        session.write(frame)
        self._wait(session, self._post_s())
        return parse_response(self._recv_any(session))

    def _read_win(self, session, win3: str) -> Optional[str]:
        kind, data = self._exchange(session, build_frame(win3, False, None, addr=self._addr()))
        if kind == "DATA":
            return data
        if kind == "NACK":
            raise TspNackError("NACK")
        if kind == "ERR":
            raise TspWindowError(int(data, 16) if data else -1)
        return None                                           # ACK·RAW → 값 없음(v1 그대로)

    def _write_win_logic(self, session, win3: str, on: bool) -> None:
        kind, data = self._exchange(session, build_frame(win3, True, "1" if on else "0", addr=self._addr()))
        if kind == "ACK":
            return
        if kind == "NACK":
            raise TspNackError("NACK")
        if kind == "ERR":
            raise TspWindowError(int(data, 16) if data else -1)
        raise TspUnexpectedReply(f"예상치 못한 응답: {kind} {data}")

    def _get_status(self, session) -> Optional[int]:
        resp = self._read_win(session, WIN_STATUS)
        st: Optional[int] = None
        if resp is not None:
            try:
                st = decode_ascii_numeric(resp)
            except Exception:
                st = None
        with self._lock:
            self.last_status, self.last_status_t = st, self.clock.monotonic()
        return st

    # ───────── 동작 단위 ─────────
    def read_status(self, on_done: Optional[Callable[[TspDone], None]] = None) -> None:
        def fn(session):
            return self._guarded(session, lambda: self._get_status(session))

        def done(res: Result) -> None:
            self._finish_simple("status", res, on_done)
        self.worker.submit(Request("read", fn, deadline=self.clock.monotonic() + EXEC_TIMEOUT_S, on_done=done,
                                   name="tsp:status", exec_timeout_s=EXEC_TIMEOUT_S))

    def close(self, on_done: Optional[Callable[[TspDone], None]] = None) -> None:
        """연결 닫기(v1 aclose). 포트에 아무것도 보내지 않는다 — bench 에서도 허용(읽기 요청)."""
        def fn(session):
            tr = session.transport
            if isinstance(tr, OnDemandTransport):
                tr.close_link()

        def done(res: Result) -> None:
            self._finish_simple("close", res, on_done)
        self.worker.submit(Request("read", fn, deadline=self.clock.monotonic() + EXEC_TIMEOUT_S, on_done=done,
                                   name="tsp:close", confirms_link=False))

    def on(self, on_done: Optional[Callable[[TspDone], None]] = None, wait_ok: float = VERIFY_WINDOW_S) -> None:
        self._switch(True, on_done, wait_ok, safety=False)

    def off(self, on_done: Optional[Callable[[TspDone], None]] = None, wait_ok: float = VERIFY_WINDOW_S,
            safety: bool = False) -> None:
        self._switch(False, on_done, wait_ok, safety=safety)

    def _steps(self, on: bool, wait_ok: float) -> List[tuple]:
        """[보내기+첫 확인, 확인, 확인, …] — v1: 쓰기 ACK 직후 205 바로 한 번, 그 뒤 poll 간격마다(창 wait_ok)."""
        want = WANT_ON if on else WANT_OFF
        target = "on" if on else "off"
        verify = bool(self.cfg["TSP_VERIFY_WITH_STATUS"])
        box: Dict[str, float] = {}

        def check(session) -> Any:
            # 205 읽기만 통신 예외 처리(닫음) — 시간초과 자체는 v1 _wait_status 처럼 연결을 닫지 않는다
            st = self._guarded(session, lambda: self._get_status(session))
            if st in want:
                return SEQ_DONE
            if session.clock.monotonic() >= box["end"]:
                raise TspVerifyTimeout(f"상태 확인 시간초과(target={target}, last={st})")
            return None

        def first(session):
            self._guarded(session, lambda: self._write_win_logic(session, WIN_ONOFF, on))
            if not verify:
                return SEQ_DONE
            box["end"] = session.clock.monotonic() + max(0.0, float(wait_ok))
            return check(session)

        def last(session):
            if check(session) is SEQ_DONE:
                return SEQ_DONE
            raise TspVerifyTimeout(f"상태 확인 시간초과(target={target}, last={self.last_status})")

        if not verify:
            return [("send", first)]
        n = int(max(0.0, float(wait_ok)) / self._poll_s()) + 2
        return [("send", first)] + [("verify", check)] * n + [("verify", last)]

    def _switch(self, on: bool, on_done, wait_ok: float, safety: bool) -> None:
        action = "on" if on else "off"
        if not self.writes_allowed:                           # 드라이버 이중 보호(worker 도 거부)
            self._done(action, False, "BENCH_READ_ONLY", None, "bench 모드 — TSP 쓰기 금지", on_done)
            return
        steps = self._steps(on, wait_ok)
        with self._lock:
            self._cmd_active += 1
            self.last_cmd = action

        def finish(_name, reason, results: List[Result]) -> None:
            with self._lock:
                self._cmd_active = max(0, self._cmd_active - 1)
            if reason is None:
                self._done(action, True, None, None, f"TSP {action.upper()} 확인", on_done)
                return
            exc = self._exc_of(results)
            if exc is None:
                self._done(action, False, reason, None, str(reason), on_done)
            else:
                self._done(action, False, exc.reason or reason, exc.code, str(exc), on_done)

        if safety:
            gap = self._poll_s()

            def run_all(session):
                for i, (_n, fn) in enumerate(steps):
                    if fn(session) is SEQ_DONE:
                        return SEQ_DONE
                    if i + 1 < len(steps):
                        self._wait(session, gap)
                return None

            self.worker.submit(Request("safety", run_all, purge=True, name=f"tsp:{action}",
                                       exec_timeout_s=EXEC_TIMEOUT_S,
                                       on_done=lambda res: finish("tsp", None if res.status == Status.OK
                                                                  else (res.reason or res.status), [res])))
            return
        self.worker.submit_sequence(Sequence(f"tsp:{action}", steps, gap_s=self._poll_s(),
                                             step_timeout_s=EXEC_TIMEOUT_S, on_done=finish))

    # ───────── 결과 ─────────
    @staticmethod
    def _exc_of(results: List[Result]) -> Optional["_ExcText"]:
        for r in reversed(results):
            if r.status != Status.OK:
                return _ExcText(r)
        return None

    def _finish_simple(self, action: str, res: Result, on_done) -> None:
        if res.status == Status.OK:
            self._done(action, True, None, None, "", on_done, quiet=True)
            return
        exc = _ExcText(res)
        self._done(action, False, exc.reason, exc.code, str(exc), on_done, quiet=(action == "close"))

    def _done(self, action, ok, reason, code, detail, on_done, quiet: bool = False) -> None:
        if action in ("on", "off"):
            self.last_ok = ok
        st = self.last_status
        if not quiet or not ok:
            self._emit("command_result" if action in ("on", "off") else f"{action}_failed",
                       f"{action}: {detail}" + (f" [{code} {V1_CODES.get(code, '')}]" if code else ""),
                       reason, code, ok)
        if on_done:
            on_done(TspDone(ok, action, reason, code, detail, st))


class _ExcText(Exception):
    """worker Result 에서 사유·코드를 되찾는다(worker 는 예외 객체 대신 repr 을 남긴다)."""

    _BY_NAME = {c.__name__: c for c in (TspLinkError, TspNoResponse, TspProtocolError, TspUnexpectedReply,
                                        TspNackError, TspWindowError, TspVerifyTimeout, TspFrameError)}

    def __init__(self, res: Result) -> None:
        err = res.error or ""
        name = err.split("(", 1)[0]
        cls = self._BY_NAME.get(name)
        super().__init__(err or (res.reason or res.status))
        if cls is not None:
            self.reason, self.code = cls.reason, cls.code
        else:
            self.reason, self.code = (res.reason or res.status), None

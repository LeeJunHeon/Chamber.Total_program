"""PLC 저수준 입출력(챔버12) — v1 device/plc.py 의 원시 연산·재시도·재연결 정책과 링크 상태(lib/link_state.py)를 다시 쓴 것.

worker(포트 독점 스레드) 안에서만 쓴다. 드라이버(plc.py)가 요청 fn 안에서 PlcIo.op(...) 를 부른다.

정책(device/plc.py:1252-1367 — 이 PLC 는 소켓을 닫으면 1.5~3분 새 SYN 에 응답하지 않는다, device/plc.py:410-412)
- 원시 연산 앞마다 직전 연산 시작 뒤 PLC_CMD_GAP_MS(150 ms)가 지나야 보낸다(device/plc.py:838-844 — 시작 시각 기준).
- 응답 제한 PLC_TIMEOUT_S(2.0 s, pymodbus retries=0 — 재시도는 이 정책만). 요청 마감(Session.deadline)이 더 이르면 그것.
- high(명령·하트비트): E403(Modbus 예외 응답)은 그대로 실패. E401(소켓 끊김·reset) → 링크 down 표시 + 소켓 재생성
  (닫고 1회 접속) + 1회 재시도. E402(무응답·프레임 오류) → 연속 카운터 +1, PLC_TIMEOUT_CLOSE_AFTER(3) 이상이면
  링크 down + 재생성 + 1회 재시도, 미만이면 '같은 소켓'에서 늦은 프레임을 버리고(drain) 1회 재시도(또 E402 면 +1).
  → 무응답 1회로는 소켓을 닫지 않고, 연속 3회째에 재생성한다.
- low(1초 묶음 읽기 — v1 코일 로거·히터 폴링): 재시도·재연결·닫기 없이 즉시 실패, 공통 카운터는 건드리지 않고 low 카운터만 +1.
- 성공: high 는 high·low 카운터 0, low 는 low 만 0. 재생성(접속) 성공 때 두 카운터 0(새 소켓은 새 예산).
- 재생성 접속이 실패하면 TransportError 로 올려 worker 가 끊고 PLC_RECONNECT_BACKOFF_S(5 s) 뒤 다시 접속한다.
worker 쪽 접속(백그라운드)은 RetryOpenTransport 가 PLC_RECONNECT_RETRY+1 = 3회, 시도 사이 PLC_RECONNECT_DELAY_S 0.5 s.

오류 → worker 분류: E401 = PlcLinkError(TransportError → 끊고 재연결), E402 = PlcTimeout(ReadTimeout → 무응답),
E403 = PlcModbusError(장비 거부 — 연결 유지). 메시지는 v1 PLCError 문구.
"""
from __future__ import annotations

import datetime as _dt
import struct
from typing import Any, Callable, Dict, Optional, Tuple

from v2.common.modbus_rtu import (READ_COILS, READ_REGS, WRITE_FUNCS, FrameError, ModbusException,
                                  ReadOnlyViolation)
from v2.common.modbus_tcp import MBAP_LEN, ModbusTcpClient, parse_pdu
from v2.common.transport import Cancelled, ReadTimeout, Transport, TransportError


class PlcLinkError(TransportError):
    code = "E401"


class PlcTimeout(ReadTimeout):
    code = "E402"

    def __init__(self, msg: str = "PLC 응답 없음", partial: bytes = b"") -> None:
        super().__init__(partial)
        self.args = (msg,)
        self.msg = msg

    def __str__(self) -> str:
        return self.msg


class PlcModbusError(Exception):
    code = "E403"


def code_of(e: BaseException) -> str:
    """v1 _to_plc_error 분류 코드(device/plc.py:1217-1250)."""
    if isinstance(e, (PlcLinkError, PlcTimeout, PlcModbusError)):
        return e.code
    if isinstance(e, TransportError):
        return "E401"
    if isinstance(e, (ReadTimeout, FrameError)):
        return "E402"
    return "E403"


# ───────── 재시도 접속 전송(worker 의 백그라운드 접속) ─────────
class RetryOpenTransport(Transport):
    """다른 전송을 감싼다. open() = v1 full=True 접속(시도 attempts 회, 시도 '사이'에만 delay_s 쉼, device/plc.py:872-905),
    open_once() = v1 full=False(PLC_FG_CONNECT_ATTEMPTS 1회 — 드라이버의 소켓 재생성)."""

    def __init__(self, inner: Transport, attempts: int = 3, delay_s: float = 0.5, fg_attempts: int = 1) -> None:
        super().__init__(inner.clock)
        self.inner = inner
        self.attempts = max(1, int(attempts))
        self.fg_attempts = max(1, int(fg_attempts))
        self.delay_s = max(0.0, float(delay_s))
        self.last_attempts = 0

    @property
    def is_open(self) -> bool:
        return self.inner.is_open

    def _open_n(self, n: int) -> None:
        last: Optional[Exception] = None
        for i in range(n):
            try:
                self.inner.open()
                self.last_attempts = i + 1
                return
            except TransportError as e:
                last = e
                self.inner.close()
                if self.delay_s > 0 and i < n - 1:
                    self.clock.sleep_until(self.clock.monotonic() + self.delay_s)
        self.last_attempts = n
        raise TransportError(f"{last} (시도 {n}회)")

    def open(self) -> None:
        self._open_n(self.attempts)

    def open_once(self) -> None:
        self._open_n(self.fg_attempts)

    def close(self) -> None:
        self.inner.close()

    def reset_input(self) -> None:
        self.inner.reset_input()

    def write(self, data: bytes) -> None:
        self.inner.write(data)

    def read_until(self, deadline, terminator=None, size=None, cancel=None) -> bytes:
        return self.inner.read_until(deadline, terminator, size, cancel)


# ───────── 링크 상태(lib/link_state.py) ─────────
class LinkState:
    """PLC 링크 down/up 전이의 단일 소스. 끊김 1회당 카드 2장(down 즉시·up 즉시, 대기 임계 없음).
    링크 down 동안 PLC 연결계 개별 오류(CHAT_PLC_LINK_CODES E401·E402)는 건수만 센다(note_suppressed) — 재연결 카드에 싣는다.
    emit(kind, detail, data) 로 'link_down' / 'link_up' 이벤트를 낸다(data 에 v1 카드 제목·부제·상태)."""

    def __init__(self, clock, emit: Callable[[str, str, Dict[str, Any]], None], endpoint: str = "") -> None:
        self.clock = clock
        self._emit = emit
        self.endpoint = endpoint
        self.down = False
        self.since = 0.0
        self.reason = ""
        self.op = ""
        self.suppressed: Dict[str, int] = {}

    def note_down(self, reason: str = "", op: str = "") -> bool:
        with self.clock.cond:
            if self.down:
                return False
            self.down, self.since, self.reason, self.op = True, self.clock.monotonic(), str(reason or ""), str(op or "")
        at = self.clock.wall()
        ts = _dt.datetime.fromtimestamp(at).strftime("%H:%M:%S")
        sub = f"[CH1&2] PLC 연결 끊김 ({self.endpoint}) | 감지 {ts} | 사유={self.reason or '미확인'}"
        if self.op:
            sub += f" | op={self.op}"
        self._emit("link_down", sub, dict(reason=self.reason, op=self.op, at=at,
                                          card=dict(title="PLC 연결 끊김", subtitle=sub, status="FAIL", urgent=True)))
        return True

    def note_up(self, detail: str = "", diag: str = "") -> bool:
        with self.clock.cond:
            if not self.down:
                return False
            elapsed = max(0.0, self.clock.monotonic() - self.since) if self.since else 0.0
            reason, op, snap = self.reason, self.op, dict(self.suppressed)
            self.suppressed.clear()
            self.down, self.since, self.reason, self.op = False, 0.0, "", ""
        tot = sum(snap.values())
        sub = str(detail or f"[CH1&2] PLC 재연결 성공 ({self.endpoint})") + f" | 끊김 {elapsed:.1f}초"
        if tot > 0:
            sub += f" | 끊긴 동안 억제된 연결 알림 {tot}건 ({format_suppressed(tot, snap)})"
        if diag and diag not in ("미확인", "진단중"):
            sub += f" | 진단={diag}"
        self._emit("link_up", sub, dict(detail=detail, diag=diag, elapsed_s=elapsed, reason=reason, op=op,
                                        suppressed_total=tot, suppressed_by_code=snap,
                                        card=dict(title="PLC 재연결", subtitle=sub, status="SUCCESS", urgent=True)))
        return True

    def set_down(self, down: bool) -> None:
        """카드 없이 상태만."""
        with self.clock.cond:
            self.down = bool(down)
            if not self.down:
                self.since, self.reason, self.op = 0.0, "", ""
            elif self.since == 0.0:
                self.since = self.clock.monotonic()

    def down_info(self) -> Tuple[bool, float, str, str]:
        return self.down, self.since, self.reason, self.op

    def note_suppressed(self, code: str) -> None:
        key = (code or "").strip().upper() or "?"
        with self.clock.cond:
            self.suppressed[key] = self.suppressed.get(key, 0) + 1

    def suppressed_snapshot(self) -> Tuple[int, Dict[str, int]]:
        snap = dict(self.suppressed)
        return sum(snap.values()), snap

    def consume_suppressed(self) -> Tuple[int, Dict[str, int]]:
        with self.clock.cond:
            snap = dict(self.suppressed)
            self.suppressed.clear()
        return sum(snap.values()), snap


def probe_connect(host: str, port: int, timeout_s: float) -> str:
    """raw socket 접속으로 errno 를 사람이 읽을 분류로(device/plc.py:1057-1081). 실제 장비 진단용 — 조립(prod)만 넣는다.
    ping(Windows 만, device/plc.py:1083-1094)은 v1 로그 부가 정보라 넣지 않았다(분류 문구는 같다)."""
    import socket
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sk:
            sk.settimeout(max(0.5, float(timeout_s)))
            rc = sk.connect_ex((host, int(port)))
    except socket.timeout:
        return "SYN 무응답(타임아웃)"
    except Exception as e:
        return f"probe 실패: {type(e).__name__}"
    table = (((0,), "접속됨"), ((110, 10060), "SYN 무응답(타임아웃, errno={})"), ((111, 10061), "접속 거부(RST, errno={})"),
             ((113, 101, 10065, 10051), "도달 불가(errno={})"), ((10035, 115, 36), "판정보류(접속 진행중, errno={})"),
             ((10056, 106), "접속됨(이미 연결, errno={})"), ((10013, 13), "차단됨(방화벽/권한, errno={})"))
    for codes, text in table:
        if rc in codes:
            return text.format(rc)
    return f"미분류(errno={rc})"


def format_suppressed(total: int, by_code: Dict[str, int]) -> str:
    """'E401 12건, E402 3건' (코드 정렬). total 0 이면 빈 문자열."""
    if not total:
        return ""
    return ", ".join(f"{k} {v}건" for k, v in sorted(by_code.items()) if v)


# ───────── 원시 연산 ─────────
class ShortResponse(Exception):
    """블록이 아닌 단일 읽기의 짧은 응답 — v1 은 resp.bits[0]/registers[0] IndexError → E403 'PLC 통신 오류'."""


def parse_read(pdu: bytes, function: int, count: int, pad: bool):
    """읽기 응답 PDU. 바이트 수 칸과 실제 길이가 맞으면 받은 만큼 풀고, 모자라면 pad=True 일 때 v1 처럼
    False/0 으로 채운다(device/plc.py:1434-1436·1705-1707). 바이트 수 칸이 실제 길이와 다르면 프레임 오류(E402)."""
    if not pdu:
        raise FrameError("빈 PDU")
    if pdu[0] == (function | 0x80):
        raise ModbusException(function, pdu[1] if len(pdu) > 1 else 0)
    if pdu[0] != function or len(pdu) < 2 or len(pdu) != 2 + pdu[1]:
        raise FrameError(f"응답 형식 이상: {pdu[:8].hex(' ')}")
    nb = pdu[1]
    if function == READ_COILS:
        got = [bool(pdu[2 + i // 8] >> (i % 8) & 1) for i in range(min(count, nb * 8))]
        fill = False
    else:
        if nb % 2:
            raise FrameError("레지스터 바이트 수 홀수")
        got = list(struct.unpack(">" + "H" * (nb // 2), pdu[2:2 + nb]))[:count]
        fill = 0
    if len(got) < count:
        if not pad:
            raise ShortResponse(f"짧은 응답 {len(got)}/{count}")
        got += [fill] * (count - len(got))
    return got


class _Client(ModbusTcpClient):
    """트랜잭션 id 를 PlcIo 하나가 이어서 센다(결정 D02 — 1 부터 하나씩). 읽기 응답은 parse_read(짧은 응답 채움)."""

    def __init__(self, io: "PlcIo", session, **kw) -> None:
        super().__init__(session, **kw)
        self._io = io

    def next_tid(self) -> int:
        return self._io.next_tid()

    def transact_v1(self, pdu: bytes, function: int, count: int, pad: bool):
        if function in WRITE_FUNCS and self.read_only:
            raise ReadOnlyViolation(f"읽기 전용 — 함수 {function} 보내지 않음")
        clock = self.s.clock
        dl = min(self.s.deadline, clock.monotonic() + self.txn_timeout_s)
        tid = self.next_tid()
        req = struct.pack(">HHHB", tid, 0, len(pdu) + 1, self.unit) + pdu
        self.s.write(req)
        if self.on_frame:
            self.on_frame("tx", req)
        while True:
            head = self.s.read_until(size=MBAP_LEN, deadline=dl)
            rtid, proto, ln, unit = struct.unpack(">HHHB", head)
            if proto != 0 or not 2 <= ln <= 254:
                raise FrameError(f"MBAP 머리 이상: {head.hex(' ')}")
            body = self.s.read_until(size=ln - 1, deadline=dl)
            if self.on_frame:
                self.on_frame("rx", head + body)
            if rtid == tid:
                break                     # 다르면 늦게 온 앞 응답 — 버리고 다시(pymodbus 도 tid 로 거른다)
        if unit != self.unit:
            raise FrameError(f"유닛 {unit} ≠ {self.unit}")
        self.transactions += 1
        if function in (READ_COILS, READ_REGS):
            return parse_read(body, function, count, pad)
        out = parse_pdu(body, function, count)
        if function in WRITE_FUNCS and out != struct.unpack(">HH", pdu[1:5]):
            raise FrameError(f"쓰기 응답이 요청과 다름: {out}")
        return out


class PlcIo:
    def __init__(self, clock, cfg: Dict[str, Any], link: LinkState, read_only: bool,
                 log: Callable[[str], None] = lambda m: None) -> None:
        self.clock = clock
        self.cfg = cfg
        self.link = link
        self.read_only = read_only
        self.log = log
        self.consec = 0                 # high 연속 E402(소켓 재생성 판단에만)
        self.consec_low = 0             # low 연속 실패(1초 묶음 읽기의 코일 로그 자기 억제에만)
        self.reconnect_count = 0
        self.connected_since = 0.0
        self.last_success = 0.0
        self.last_io: Optional[float] = None
        self.frame_listeners: list = []
        self._tid = 0

    # 설정(접속 때마다 다시 읽는 v1 _apply_cfg_from_config 와 같이 매번 cfg 에서)
    @property
    def gap_s(self) -> float:
        return float(self.cfg["PLC_CMD_GAP_MS"]) / 1000.0

    @property
    def timeout_s(self) -> float:
        return float(self.cfg["PLC_TIMEOUT_S"])

    def next_tid(self) -> int:
        self._tid = self._tid % 0xFFFF + 1
        return self._tid

    def _frame(self, direction: str, data: bytes) -> None:
        for f in list(self.frame_listeners):
            f(direction, data)

    def client(self, session) -> ModbusTcpClient:
        return _Client(self, session, unit=int(self.cfg["PLC_UNIT"]), txn_timeout_s=self.timeout_s, retries=0,
                       read_only=self.read_only, on_frame=(self._frame if self.frame_listeners else None))

    def _throttle(self, session) -> None:
        """직전 원시 연산 시작 뒤 간격이 모자라면 남은 만큼 쉰 뒤 이번 시작 시각을 기록(device/plc.py:838-844)."""
        if self.last_io is not None:
            target = self.last_io + self.gap_s
            if self.clock.monotonic() < target:
                cancel = session.cancel
                self.clock.wait_until(target, (lambda: cancel.is_set()) if cancel is not None else None)
                if cancel is not None and cancel.is_set():
                    raise Cancelled()
        self.last_io = self.clock.monotonic()

    def _attempt(self, session, pdu: bytes, function: int, count: int, pad: bool):
        self._throttle(session)
        return self.client(session).transact_v1(pdu, function, count, pad)

    def _success(self, priority: str) -> None:
        if priority == "low":
            self.consec_low = 0
        else:
            self.consec = self.consec_low = 0
        self.last_success = self.clock.monotonic()

    def _reopen(self, session, reason: str, op: str) -> None:
        """소켓 재생성(device/plc.py:992-998 로그 + close + connect(full=False))."""
        self.reconnect_count += 1
        held = (self.clock.monotonic() - self.connected_since) if self.connected_since else 0.0
        self.log(f"WARN PLC 소켓 재생성 #{self.reconnect_count} (사유={reason}, op={op}, 이전 연결 유지 {held:.0f}초)")
        tr = session.transport
        tr.close()
        try:
            (tr.open_once if hasattr(tr, "open_once") else tr.open)()
        except TransportError as e:
            self.link.note_down(f"접속 실패(시도 1회, 분류=미확인)", "connect")
            raise PlcLinkError(f"Modbus TCP 연결 실패 ({self.link.endpoint}) - {e!r}") from e
        self.on_connected()
        self.link.note_up(f"[CH1&2] PLC 재연결 성공 ({self.link.endpoint})", "미확인")   # 접속 성공 = 링크 up

    def on_connected(self) -> None:
        self.connected_since = self.clock.monotonic()
        self.consec = self.consec_low = 0

    @staticmethod
    def _convert(e: BaseException, op: str, addr) -> BaseException:
        if isinstance(e, (PlcLinkError, PlcTimeout, PlcModbusError, Cancelled, ReadOnlyViolation)):
            return e
        if isinstance(e, ModbusException):
            return PlcModbusError(f"PLC Modbus ExceptionResponse: {e} (op={op}, addr={addr})")
        if isinstance(e, TransportError):
            return PlcLinkError(f"PLC 연결 오류: {type(e).__name__}: {e} (op={op}, addr={addr})")
        if isinstance(e, (ReadTimeout, FrameError)):
            return PlcTimeout(f"PLC 응답 없음/프레임 오류: {type(e).__name__}: {e} (op={op}, addr={addr})",
                              getattr(e, "partial", b""))
        return PlcModbusError(f"PLC 통신 오류: {type(e).__name__}: {e} (op={op}, addr={addr})")

    def _try(self, session, pdu, function, count, op, addr, pad):
        try:
            return self._attempt(session, pdu, function, count, pad)
        except Exception as e:
            ce = self._convert(e, op, addr)
            if ce is e:
                raise
            raise ce from e

    def op(self, session, op: str, addr, pdu: bytes, function: int, count: int = 0, priority: str = "high",
           pad: bool = False):
        """원시 연산 1회(재시도·재연결 정책 포함, device/plc.py:1252-1367)."""
        try:
            out = self._try(session, pdu, function, count, op, addr, pad)
            self._success(priority)
            return out
        except (PlcModbusError, Cancelled, ReadOnlyViolation):
            raise
        except PlcLinkError:
            self.link.note_down("RST/연결 끊김", op)
            if priority == "low":
                self.consec_low += 1
                raise
            self._reopen(session, "E401/reset", op)
            out = self._try(session, pdu, function, count, op, addr, pad)
            self._success(priority)
            return out
        except PlcTimeout:
            if priority == "low":
                self.consec_low += 1
                raise
            self.consec += 1
            close_after = max(1, int(self.cfg["PLC_TIMEOUT_CLOSE_AFTER"]))
            if self.consec >= close_after:
                self.link.note_down(f"연속 타임아웃 {self.consec}회", op)
                self._reopen(session, f"연속 타임아웃 {self.consec}회", op)
                out = self._try(session, pdu, function, count, op, addr, pad)
                self._success(priority)
                return out
            session.transport.reset_input()           # 늦게 온 부분 프레임 버리기(drain)
            try:
                out = self._try(session, pdu, function, count, op, addr, pad)
            except PlcTimeout:
                self.consec += 1
                raise
            self._success(priority)
            return out

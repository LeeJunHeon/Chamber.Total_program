"""두께 모니터 Sycon STM-100/MF 드라이버 — MOXA NPort TCP(CH2 192.168.1.50:4008 임시값) 경유 RS-232 9600-8-N-1.
v1 device/thickness_monitor.py(AsyncThicknessMonitor) + controller/thickness_service.py 의 통신 부분을 다시 쓴 것(import 하지 않음).
프레임·해석은 stm100_proto.py.

읽기(worker 안 자기 루프 — v1 ThicknessService._run 과 같은 순서·주기)
  한 번(poll) = S·T(두께·증착률) → 때가 되면 U·V·M(주파수·수명·크리스탈, THICKNESS_SLOW_POLL_S 5 s)
               → 때가 되면 E?·F?·J?(장비에 적용된 재료값, 연결 직후 + MATERIAL_READ_S 60 s).
  다음 poll = max(이번 시작 + THICKNESS_POLL_INTERVAL_S(0.5 s), 끝난 뒤 0.05 s). 연결 직후 첫 poll 은 전부 읽는다.
  S 가 '값 없음'(XTAL FAIL)으로 막 바뀌면 그 poll 에서 U·V·M 을 바로 읽는다(M 으로 XTAL FAIL 확인). 값 없음은 통신 정상.
  명령 하나 = 보내기 전 수신 비움 → 보냄 → 응답 프레임(THICKNESS_CMD_TIMEOUT_S 1 s) — 무응답·체크섬·모르는 코드면
  0.1 s 쉬고 다시(2번 더). 전원 리셋 플래그(B·G·I·K)를 새로 보면 reset_seen+1, 'L' 로 확인(2 s 에 한 번).
실패 정책(v1 thickness_service 독스트링 그대로): 통신 실패(응답 없음·송신 실패)·프레임 오류(체크섬·길이·모르는 코드)가
  MAX_FAIL_BEFORE_RECONNECT(6)회 연속일 때만 재연결. 형식 오류·명령 거부·값 없음은 재연결 사유가 아니다(오류 줄만).
  재연결 = worker 가 연결을 끊고 백오프(1 s 부터 ×2, 최대 30 s — worker_kwargs) 뒤 다시 연다.
1초 행: 포트에 닿지 않는다 — 드라이버가 들고 있는 마지막 값과 나이(value_age_s)를 싣는다(cache_source).
두께 종료 판정(3단계 thickness.py)용: status()["seq"] 가 새 S 값마다 +1, add_listener(cb) 는 poll 마다 status 사본으로 불린다.
명령(동작 단위, worker 의 write 요청 — 폴링 사이에 먼저): zero(mode B/C)·set_material(density, z, tooling).
  끝나면 바로 한 번 더 poll(v1: 요청이 들어오면 대기를 깨고 요청 → poll).
bench(writes_allowed=False): zero·set_material 은 BENCH_READ_ONLY(worker + 드라이버 이중 보호), 읽기는 허용.
"""
from __future__ import annotations

import datetime as _dt
import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from v2.common.snapshot import DeviceBlock, FieldDef, FType
from v2.common.snapshot_bus import Source
from v2.common.transport import Cancelled, ReadTimeout, TcpSettings, TcpTransport, TransportError
from v2.common.worker import Request, Result, Status, Worker
from v2.ch12.drivers.cache_source import CacheWorker
from v2.ch12.drivers.stm100_proto import (
    CODE_MEANING, OK_CODES, Reply, StmChecksumError, StmCommandRejected, StmCommError, StmError, StmFrameError,
    StmLinkLost, StmTimeout, StmValueUnavailable, StmVerifyError, build_frame, check_material,
    error_text, extract_frame, material_matches, parse_crystal_ok, parse_frequency_hz, parse_life_pct, parse_number,
    parse_rate_a_s, parse_thickness_a,
)

# ── v1 상수(파일:줄) ──
A_TO_NM = 0.1                     # controller/thickness_service.py:44
MAX_FAIL_BEFORE_RECONNECT = 6     # controller/thickness_service.py:45
FORMAT_LOG_INTERVAL_S = 60.0      # controller/thickness_service.py:46
MATERIAL_READ_S = 60.0            # controller/thickness_service.py:47
LISTENER_LOG_INTERVAL_S = 60.0    # controller/thickness_service.py:49
JOB_TIMEOUT_S = 15.0              # controller/thickness_service.py:51
RETRIES = 2                       # device/thickness_monitor.py:244
CONNECT_TIMEOUT_S = 3.0           # device/thickness_monitor.py:245
BACKOFF_START_S, BACKOFF_MAX_S = 1.0, 30.0   # device/thickness_monitor.py:246-247
SETTLE_S = 0.1                    # device/thickness_monitor.py:248
RETRY_GAP_S = 0.1                 # device/thickness_monitor.py:56
RESET_ACK_MIN_INTERVAL_S = 2.0    # device/thickness_monitor.py:57
MIN_POLL_WAIT_S = 0.05            # controller/thickness_service.py:272
POLL_EXEC_TIMEOUT_S = 60.0        # poll 하나 상한(명령 8개 × 3회 × 1 s 넘게) — v2 안전 상한
TICK_DEADLINE_MS = 900

BUDGET = dict(
    queries_per_tick=0, bytes_tx=20, bytes_rx=40, min_gap_ms=0, fits_1s=True, alternate=None,
    note="1초 행은 포트에 닿지 않음(마지막 값). 장비 쪽 상시 읽기(v1 그대로): S·T 0.5 s 마다 = 초당 4 질의(보냄 4 B씩, "
         "받음 약 7~10 B씩) + U·V·M 5 s 마다(초당 0.6) + E?·F?·J? 60 s 마다(초당 0.05) ≈ 초당 4.65 질의·보냄 약 19 B·"
         "받음 약 40 B. 9600 bps(약 960 B/s) 의 5 % 남짓. 상시 0.5 s 읽기가 NPort 60 s 무통신 끊김을 막는다.",
)


def _clamp(v: float, lo: float, hi: float) -> float:
    return min(max(float(v), lo), hi)


@dataclass(frozen=True)
class StmSettings:
    """v1 lib/thickness_config.load_thickness_config 와 같은 검증·클램프(lib/thickness_config.py:110-159)."""
    enabled: bool
    host: str
    port: int
    poll_s: float
    slow_s: float
    stale_s: float
    cmd_timeout_s: float
    warnings: tuple


def settings(cfg: Dict[str, Any], chamber: str) -> StmSettings:
    p = chamber.upper() + "_"
    warns: List[str] = []
    enabled = bool(cfg[p + "SUPPORTS_THICKNESS"])
    host = str(cfg[p + "THICKNESS_TCP_HOST"] or "").strip()
    port = int(cfg[p + "THICKNESS_TCP_PORT"])
    if enabled and (not host or not (1 <= port <= 65535)):
        warns.append(f"THICKNESS_TCP_HOST/PORT={host!r}:{port} 가 올바르지 않음 → 두께 모니터 끔")
        enabled = False
    poll = _clamp(cfg[p + "THICKNESS_POLL_INTERVAL_S"], 0.25, 5.0)
    slow = _clamp(cfg[p + "THICKNESS_SLOW_POLL_S"], poll, 60.0)
    tmo = _clamp(cfg[p + "THICKNESS_CMD_TIMEOUT_S"], 0.3, 5.0)
    stale = float(cfg[p + "THICKNESS_STALE_S"])
    stale_min = max(2.0, 4.0 * poll)
    if stale < stale_min:
        warns.append(f"THICKNESS_STALE_S={stale:g} < {stale_min:g}(폴링 4회) → {stale_min:g}")
        stale = stale_min
    return StmSettings(enabled, host or "192.168.1.50", port, poll, slow, stale, max(0.1, tmo), tuple(warns))


def worker_kwargs() -> Dict[str, Any]:
    """worker 연결 정책 — v1 워치독 재연결 백오프 1 s → ×2 → 30 s(장기 두절 구분 없음). 무응답 횟수는 드라이버가 센다."""
    return dict(reconnect_start_s=BACKOFF_START_S, reconnect_max_s=BACKOFF_MAX_S, long_outage_s=1e12,
                probe_s=BACKOFF_MAX_S, fail_streak=10 ** 9, connect_init_timeout_s=1.0)


def _socket_factory(s: TcpSettings):
    """v1 asyncio.open_connection 과 같게 — TCP_NODELAY(asyncio 기본), SO_KEEPALIVE 는 건드리지 않음(devices_v1facts NPORT_IDLE_V1)."""
    import socket
    sock = socket.create_connection((s.host, s.port), timeout=s.connect_timeout_s)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.setblocking(False)
    return sock


def make_transport(clock, cfg: Dict[str, Any], chamber: str) -> TcpTransport:
    st = settings(cfg, chamber)
    return TcpTransport(clock, TcpSettings(st.host, st.port, CONNECT_TIMEOUT_S), socket_factory=_socket_factory)


# ═══════════════════════ 1초 행 블록 ═══════════════════════
def block_name(chamber: str) -> str:
    return f"{chamber}.stm100"


def blocks(chamber: str = "ch2") -> List[DeviceBlock]:
    return [DeviceBlock(block_name(chamber), f"{chamber.upper()} 두께 모니터", (
        FieldDef("thickness_nm", FType.FLOAT, "nm", "두께", "{:.2f}", source="S(Å × 0.1)"),
        FieldDef("rate_nm_s", FType.FLOAT, "nm/s", "증착률", "{:.3f}", source="T(Å/s × 0.1)", summary="mean"),
        FieldDef("value_age_s", FType.FLOAT, "s", "두께 값 나이", "{:.1f}", source="driver",
                 desc="마지막 정상 S 뒤 시간(장비 0.5 s 읽기 — 1초 행은 마지막 값)"),
        FieldDef("seq", FType.INT, "", "두께 값 번호", source="driver", detail=True, desc="새 S 값마다 +1(종료 판정 중복 방지)"),
        FieldDef("crystal_ok", FType.BOOL, "", "크리스탈 정상", source="M", summary="any"),
        FieldDef("life_pct", FType.FLOAT, "%", "크리스탈 수명", "{:.1f}", source="V"),
        FieldDef("freq_hz", FType.INT, "Hz", "센서 주파수", source="U", detail=True),
        FieldDef("link_ok", FType.BOOL, "", "응답 정상", source="driver", desc="최근 THICKNESS_STALE_S 안에 정상 프레임"),
        FieldDef("stale", FType.BOOL, "", "두께 값 오래됨", source="driver", detail=True),
        FieldDef("reset_seen", FType.INT, "", "전원 리셋 플래그 횟수", source="B/G/I/K", detail=True),
        FieldDef("density", FType.FLOAT, "g/cm³", "밀도(적용값)", source="E?", detail=True),
        FieldDef("z_factor", FType.FLOAT, "", "Z-factor(적용값)", source="F?", detail=True),
        FieldDef("tooling", FType.FLOAT, "%", "tooling(적용값)", source="J?", detail=True),
        FieldDef("material", FType.STR, "", "재료", source="driver", detail=True),
        FieldDef("last_error", FType.STR, "", "마지막 오류", source="driver", detail=True),
    ), desc="Sycon STM-100/MF, NPort TCP, 9600-8-N-1")]


_ROW_KEYS = ("thickness_nm", "rate_nm_s", "seq", "crystal_ok", "life_pct", "freq_hz", "link_ok", "stale",
             "reset_seen", "density", "z_factor", "tooling", "material", "last_error")


# ═══════════════════════ 드라이버 ═══════════════════════
@dataclass(frozen=True)
class StmEvent:
    kind: str           # unresponsive / recovered / reset_flag / reset_ack_failed / format_error / unavailable /
                        # zero / material_applied / job_failed / listener_error / config_warning / link / loop_error
    detail: str
    reason: Optional[str] = None
    code: Optional[str] = None


@dataclass(frozen=True)
class StmDone:
    ok: bool
    action: str          # zero / set_material
    reason: Optional[str] = None
    code: Optional[str] = None
    detail: str = ""
    value: Any = None


class _Reconnect(Exception):
    pass


def _reason_of(e: BaseException) -> str:
    if isinstance(e, StmTimeout):
        return "DEVICE_NO_RESPONSE"
    if isinstance(e, StmCommError):
        return "LINK_DOWN"
    if isinstance(e, StmVerifyError):
        return "VERIFY_MISMATCH"
    if isinstance(e, ValueError):
        return "BAD_ARGS"
    return "DEVICE_REJECTED"


class Stm100Driver:
    def __init__(self, clock, worker: Worker, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[StmEvent], None]] = None, chamber: str = "ch2") -> None:
        self.clock, self.worker, self.cfg = clock, worker, cfg
        self.writes_allowed = writes_allowed
        self.on_event = on_event
        self.chamber = chamber
        self.block = block_name(chamber)
        self.p = settings(cfg, chamber)          # v1 도 서비스 만들 때 한 번 읽는다(재시작 적용)
        self.enabled = self.p.enabled
        worker.on_connect = self._on_connect
        self._prev_worker_event = worker.on_event
        worker.on_event = self._on_worker_event
        self._conn_fail_logged = False
        self._reconnect_why: Optional[str] = None
        self._lock = threading.Lock()
        self._listeners: List[Callable[[dict], None]] = []
        self._st: Dict[str, Any] = self._blank_status()
        self._seq = 0
        self._ok_ts: Optional[float] = None
        self._link_ts: Optional[float] = None
        self._fail_n = 0
        self._unavail_n = 0
        self._unresponsive = False
        self._fresh_conn = False
        self._next_slow = 0.0
        self._next_material = 0.0
        self._throttle: Dict[str, float] = {}
        self.reset_seen = 0
        self._reset_active = False
        self._reset_ack_t = -1e9
        self.last_rx_t: Optional[float] = None
        self._gen = 0
        self._running = False
        self.polls = 0
        self._source = Source(self.block, CacheWorker(clock, lambda: self.worker.connected, self.block),
                              self.source_read, (self.block,), TICK_DEADLINE_MS, enabled=self.enabled)
        for w in self.p.warnings:
            self._emit("config_warning", f"⚠ 두께 모니터 설정: {w}")          # main.py:1307-1309

    def _on_worker_event(self, ev) -> None:
        """연결 로그(device/thickness_monitor.py:333-381) — 응답 없음(quiet) 동안은 연결·재연결·끊김 줄을 생략, 실패 줄은 연결될 때까지 1번."""
        k = getattr(ev, "kind", "")
        quiet = self._unresponsive
        if k == "connected":
            self._conn_fail_logged = False
            if not quiet:
                self._emit("link", f"두께 모니터 연결 {self.p.host}:{self.p.port}")
        elif k == "connect_failed" and not self._conn_fail_logged:
            self._conn_fail_logged = True
            self._emit("link", f"두께 모니터 연결 실패 {self.p.host}:{self.p.port} — 계속 재시도: {getattr(ev, 'detail', '')}")
        elif k == "disconnected":
            why, self._reconnect_why = self._reconnect_why, None
            if not quiet:
                self._emit("link", (f"두께 모니터 재연결: {why}" if why else "두께 모니터 재연결") if why is not None
                           else "두께 모니터 연결 끊김 — 재연결 대기")
        if self._prev_worker_event:
            self._prev_worker_event(ev)

    # ═══════════════════════ 공개 API ═══════════════════════
    def start(self) -> None:
        """상시 읽기 시작(설정에서 꺼져 있으면 아무것도 하지 않는다 — 포트도 열지 않게 시스템이 worker 를 띄우지 않는다)."""
        if not self.enabled or self._running:
            return
        self._running = True
        self._kick(None)

    def stop(self) -> None:
        self._running = False
        self._gen += 1

    def source(self) -> Source:
        return self._source

    @property
    def seq(self) -> int:
        return self._seq

    def add_listener(self, cb: Callable[[dict], None]) -> None:
        if cb not in self._listeners:
            self._listeners.append(cb)

    def remove_listener(self, cb: Callable[[dict], None]) -> None:
        if cb in self._listeners:
            self._listeners.remove(cb)

    def status(self) -> dict:
        """v1 ThicknessService.status() 와 같은 키(공정 기록용 process_running/name 은 런타임 몫이라 뺐다). 호출 시각 기준."""
        now = self.clock.monotonic()
        with self._lock:
            st = dict(self._st)
            ok_ts, link_ts = self._ok_ts, self._link_ts
        conn = bool(self.worker.connected)
        st["t"] = now
        st["connected"] = conn
        st["ok_ts"] = ok_ts
        st["age_s"] = None if ok_ts is None else now - ok_ts
        st["stale"] = ok_ts is None or (now - ok_ts) > self.p.stale_s
        st["link_age_s"] = None if link_ts is None else now - link_ts
        st["link_ok"] = conn and link_ts is not None and (now - link_ts) <= self.p.stale_s
        st["reset_seen"] = int(self.reset_seen)
        return st

    def zero(self, mode: str = "B", on_done: Optional[Callable[[StmDone], None]] = None, why: str = "") -> None:
        """영점 — B = 두께·타이머, C = 두께만(device/thickness_monitor.py:545-550). 다른 글자는 보내지 않고 BAD_ARGS."""
        m = str(mode or "").strip().upper()

        def fn(session):
            if m not in ("B", "C"):
                raise ValueError(f"영점 방식은 'B' 또는 'C': {mode!r}")
            self._query(session, m)

        def ok(_v):
            self._emit("zero", f"두께 영점({m})" + (f" — {why}" if why else ""))
        self._job("zero", f"영점({m})", fn, on_done, ok, why=why)

    def set_material(self, density: float, z_factor: float, tooling: float, material: str = "",
                     on_done: Optional[Callable[[StmDone], None]] = None, why: str = "") -> None:
        """E=/F=/J= 로 쓰고 E?/F?/J? 로 되읽어 허용오차 비교 — 불일치면 1회 다시(device/thickness_monitor.py:552-571).
        실패 결과에는 v1 E846(두께 모니터 재료값 적용 실패, runtime/chamber_runtime.py:6703)을 싣는다."""
        def fn(session):
            return self._set_material(session, density, z_factor, tooling)

        def ok(res: dict):
            with self._lock:
                self._st.update(density=res.get("density"), z_factor=res.get("z_factor"),
                                tooling=res.get("tooling"), material=str(material or ""))
            self._emit("material_applied",
                       f"두께 모니터 재료값 적용{f'({why})' if why else ''}{(' ' + material) if material else ''} "
                       f"density {res.get('density')} · Z {res.get('z_factor')} · tooling {res.get('tooling')}%")
            self._publish()
        self._job("set_material", "재료값 적용", fn, on_done, ok, code="E846")

    # ═══════════════════════ 1초 읽기 ═══════════════════════
    def source_read(self, session, seq: int) -> Dict[str, Any]:
        if not self.worker.connected:
            raise StmCommError("두께 모니터 미연결")
        st = self.status()
        b = self.block
        row = {f"{b}.{k}": st.get(k) for k in _ROW_KEYS}
        row[f"{b}.value_age_s"] = None if st["age_s"] is None else round(st["age_s"], 3)
        return row

    # ═══════════════════════ 내부: 요청 ═══════════════════════
    def _emit(self, kind: str, detail: str, reason: Optional[str] = None, code: Optional[str] = None) -> None:
        if self.on_event:
            self.on_event(StmEvent(kind, detail, reason, code))

    def _emit_throttled(self, key: str, kind: str, detail: str, interval: float) -> None:
        now = self.clock.monotonic()
        if now - self._throttle.get(key, -1e9) >= interval:
            self._throttle[key] = now
            self._emit(kind, detail)

    def _kick(self, not_before: Optional[float]) -> None:
        g = self._gen
        self.worker.submit(Request("read", lambda s, g=g: self._poll(s, g), not_before=not_before,
                                   on_done=lambda r, g=g: self._after_poll(r, g), name="stm100:poll",
                                   exec_timeout_s=POLL_EXEC_TIMEOUT_S))

    def _after_poll(self, res: Result, g: int) -> None:
        if g != self._gen or not self._running:
            return
        if res.status != Status.OK:
            if res.reason == "DEVICE_REJECTED":               # 드라이버 처리 예외 — v1 '⚠ 두께 폴링 처리 예외'(60 s 1번), 계속
                self._emit_throttled("loop", "loop_error", f"⚠ 두께 폴링 처리 예외: {res.error}", FORMAT_LOG_INTERVAL_S)
            self._publish()
        now = self.clock.monotonic()
        t0 = res.started if res.started is not None else now
        self._kick(max(t0 + self.p.poll_s, now + MIN_POLL_WAIT_S))

    def _job(self, action: str, name: str, fn, on_done, on_ok, code: Optional[str] = None, why: str = "") -> None:
        if not self.enabled:
            self._job_done(action, False, "DEVICE_DISABLED", None, "두께 모니터가 꺼져 있음", None, on_done)
            return
        if not self.writes_allowed:
            self._job_done(action, False, "BENCH_READ_ONLY", None, "bench 모드 — 두께 모니터 쓰기 금지", None, on_done)
            return
        box: Dict[str, BaseException] = {}

        def run(session):
            try:
                return fn(session)
            except StmLinkLost as e:
                box["e"] = e
                raise TransportError(str(e)) from e          # worker 가 끊고 백오프 뒤 다시 연다
            except Exception as e:
                box["e"] = e
                raise

        def done(res: Result) -> None:
            elapsed = (res.finished or 0.0) - (res.started or 0.0)
            if res.status == Status.OK:
                on_ok(res.value)
                self._job_done(action, True, None, None, "", res.value, on_done)
            else:
                e = box.get("e")
                if elapsed >= JOB_TIMEOUT_S - 1e-9:
                    reason, detail = "TIMEOUT", f"{name} 시간 초과({JOB_TIMEOUT_S:g}초)"
                elif e is not None:
                    reason, detail = _reason_of(e), error_text(e) if isinstance(e, StmError) else str(e)
                else:
                    reason, detail = (res.reason or res.status), (res.error or str(res.reason or res.status))
                c = code if reason not in ("BENCH_READ_ONLY", "BAD_ARGS") else None
                self._emit("job_failed", f"⚠ 두께 {name} 실패{(' — ' + why) if why else ''}: {detail}", reason, c)
                self._job_done(action, False, reason, c, detail, None, on_done)
            if self._running:                                    # 요청 뒤 바로 한 번 더 읽는다(v1 _run 대기 깨움)
                self._gen += 1
                self._kick(None)

        self.worker.submit(Request("write", run, on_done=done, name=f"stm100:{action}", exec_timeout_s=JOB_TIMEOUT_S))

    @staticmethod
    def _job_done(action, ok, reason, code, detail, value, on_done) -> None:
        if on_done:
            on_done(StmDone(ok, action, reason, code, detail, value))

    # ═══════════════════════ 내부: 주고받기(v1 exchange) ═══════════════════════
    def _wait(self, session, dt: float) -> None:
        if session.clock.wait_until(session.clock.monotonic() + dt, lambda: session.cancel.is_set()):
            raise Cancelled()

    def _read_frame(self, session, timeout_s: float) -> str:
        deadline = session.clock.monotonic() + timeout_s
        buf = bytearray()
        while True:
            try:
                b = session.read_until(size=1, deadline=deadline)
            except ReadTimeout:
                raise StmTimeout("응답 없음")
            except TransportError as e:
                raise StmLinkLost(f"수신 중 연결 끊김: {e}") from e
            buf += b
            payload = extract_frame(buf)              # 체크섬·길이 오류는 StmChecksumError
            if payload is not None:
                self.last_rx_t = session.clock.monotonic()
                return payload

    def _exchange_locked(self, session, cmd: str, frame: bytes, timeout_s: Optional[float] = None,
                         retries: Optional[int] = None) -> Reply:
        tmo = self.p.cmd_timeout_s if timeout_s is None else max(0.05, float(timeout_s))
        attempts = 1 + (RETRIES if retries is None else max(0, int(retries)))
        last: Optional[Exception] = None
        for i in range(attempts):
            if i > 0:
                self._wait(session, RETRY_GAP_S)
            try:
                session.transport.reset_input()        # 늦게 온 앞 응답을 이 명령의 응답으로 읽지 않게
                session.write(frame)
            except TransportError as e:
                raise StmLinkLost(f"송신 실패 {cmd!r}: {e!r}") from e
            try:
                payload = self._read_frame(session, tmo)
            except (StmTimeout, StmChecksumError) as e:
                last = e
                continue
            code = payload[:1]
            if code not in CODE_MEANING:
                last = StmFrameError(f"모르는 응답 코드 {payload!r} (명령 {cmd!r})")
                continue
            return Reply(code=code, body=payload[1:], raw=payload)
        assert last is not None
        if isinstance(last, StmTimeout):
            raise StmTimeout(f"응답 없음 {cmd!r} ({attempts}회)")
        raise last

    def _exchange(self, session, cmd: str) -> Reply:
        rep = self._exchange_locked(session, cmd, build_frame(cmd))
        self._track_reset(session, rep, cmd)
        return rep

    def _track_reset(self, session, rep: Reply, cmd: str) -> None:
        if not rep.reset:
            self._reset_active = False
            return
        if not self._reset_active:
            self._reset_active = True
            self.reset_seen += 1
            self._emit("reset_flag", f"⚠ 두께 모니터 전원 리셋 플래그({rep.code}) — L 로 확인 (누적 {self.reset_seen})")
        now = session.clock.monotonic()
        if cmd == "L" or (now - self._reset_ack_t) < RESET_ACK_MIN_INTERVAL_S:
            return
        self._reset_ack_t = now
        try:
            self._exchange_locked(session, "L", build_frame("L"))
        except Cancelled:
            raise
        except Exception as e:
            self._emit("reset_ack_failed", f"⚠ 리셋 플래그 확인(L) 실패: {e!r}")

    def _query(self, session, cmd: str) -> str:
        rep = self._exchange(session, cmd)
        if rep.code not in OK_CODES:
            raise StmCommandRejected(rep.code, cmd, rep.body)
        return rep.body

    def _set_material(self, session, density: float, z_factor: float, tooling: float, verify_retries: int = 1) -> dict:
        d_s, z_s, t_s = check_material(density, z_factor, tooling)
        want = (float(d_s), float(z_s), float(t_s))
        got: Tuple[Any, Any, Any] = (None, None, None)
        for _attempt in range(1 + max(0, int(verify_retries))):
            self._query(session, "E=" + d_s)
            self._query(session, "F=" + z_s)
            self._query(session, "J=" + t_s)
            if SETTLE_S:
                self._wait(session, SETTLE_S)
            got = (parse_number(self._query(session, "E?"), "E"), parse_number(self._query(session, "F?"), "F"),
                   parse_number(self._query(session, "J?"), "J"))
            if material_matches(want, got):
                return {"density": got[0], "z_factor": got[1], "tooling": got[2]}
        raise StmVerifyError(
            f"되읽기 불일치 — density 보냄 {want[0]:g}/읽음 {got[0]}, Z 보냄 {want[1]:g}/읽음 {got[1]}, "
            f"tooling 보냄 {want[2]:g}/읽음 {got[2]}")

    # ═══════════════════════ 내부: 상시 읽기(v1 poll_once) ═══════════════════════
    def _on_connect(self, session) -> None:
        self._fresh_conn = True                       # 입출력 없음 — 첫 poll 이 전부 읽는다

    def _poll(self, session, g: int):
        if g != self._gen:
            return None                               # 요청 뒤 앞당긴 poll 이 대신함
        self.polls += 1
        if self._fresh_conn:
            self._fresh_conn = False
            self._fail_n = self._unavail_n = 0
            self._next_slow = self._next_material = 0.0
        lost: Optional[_Reconnect] = None
        try:
            self._poll_fast(session)
            now = session.clock.monotonic()
            if now >= self._next_slow:
                self._next_slow = now + self.p.slow_s
                self._poll_slow(session)
            if now >= self._next_material:
                self._next_material = now + MATERIAL_READ_S
                self._read_material(session)
        except _Reconnect as e:
            lost = e
        self._publish()
        if lost is not None:
            self._reconnect_why = str(lost) if str(lost).startswith("연속 실패") else None   # 링크 끊김은 '연결 끊김' 줄
            raise TransportError(str(lost))
        return True

    def _read_value(self, session, cmd: str, parser):
        return parser(self._query(session, cmd))

    def _poll_fast(self, session) -> None:
        try:
            a = self._read_value(session, "S", parse_thickness_a)
        except StmValueUnavailable as e:
            self._on_unavailable(e)
            if self._unavail_n == 1 and self._st.get("crystal_ok") is not False:
                self._next_slow = 0.0                 # 값이 막 없어졌다 — 이번 poll 에서 M 을 바로(XTAL FAIL 확인)
        except StmError as e:
            if self._on_error(e):
                return                                # 통신 실패면 T 는 건너뛴다
        else:
            self._link_seen()
            self._unavail_n = 0
            with self._lock:
                self._seq += 1
                self._ok_ts = session.clock.monotonic()
                self._st["thickness_nm"] = a * A_TO_NM
                self._st["seq"] = self._seq
        try:
            r = self._read_value(session, "T", parse_rate_a_s)
        except StmValueUnavailable:
            self._link_seen()
            self._st["rate_nm_s"] = None
        except StmError as e:
            self._on_error(e)
        else:
            self._link_seen()
            self._st["rate_nm_s"] = r * A_TO_NM

    def _poll_items(self, session, items) -> None:
        for key, cmd, parser in items:
            try:
                v = self._read_value(session, cmd, parser)
            except StmValueUnavailable:
                self._link_seen()
                self._st[key] = None                  # XTAL FAIL 이면 U/V 가 빈값 — 실패로 세지 않는다
            except StmError as e:
                self._on_error(e)
            else:
                self._link_seen()
                self._st[key] = v

    def _poll_slow(self, session) -> None:
        self._poll_items(session, (("freq_hz", "U", parse_frequency_hz), ("life_pct", "V", parse_life_pct),
                                   ("crystal_ok", "M", parse_crystal_ok)))

    def _read_material(self, session) -> None:
        self._poll_items(session, (("density", "E?", lambda b: parse_number(b, "E")),
                                   ("z_factor", "F?", lambda b: parse_number(b, "F")),
                                   ("tooling", "J?", lambda b: parse_number(b, "J"))))

    def _link_seen(self) -> None:
        self._link_ts = self.clock.monotonic()
        self._fail_n = 0
        if self._unresponsive:
            self._set_unresponsive(False)

    def _on_unavailable(self, e: Exception) -> None:
        self._link_seen()
        self._unavail_n += 1
        self._set_error(f"값 없음 {self._unavail_n}회 연속")
        if self._unavail_n == 1:
            self._emit("unavailable", f"두께 값 없음(XTAL FAIL 가능) — {e}")

    def _on_error(self, e: Exception) -> bool:
        """통신 실패·프레임 오류면 실패로 세고 True, 형식 오류·명령 거부면 기록만 하고 False(v1 _on_error)."""
        if isinstance(e, (StmCommError, StmFrameError)):
            self._on_fail(e)
            return True
        self._link_seen()
        txt = error_text(e)
        self._set_error(txt)
        self._emit_throttled(f"fmt:{txt}", "format_error", f"⚠ 두께 모니터 {txt}", FORMAT_LOG_INTERVAL_S)
        return False

    def _on_fail(self, e: Exception) -> None:
        self._fail_n += 1
        self._set_error(error_text(e))
        if self._fail_n >= MAX_FAIL_BEFORE_RECONNECT:
            self._fail_n = self._unavail_n = 0
            if not self._unresponsive:
                self._set_unresponsive(True, e)
            raise _Reconnect(f"연속 실패: {e}")
        if isinstance(e, StmLinkLost):
            raise _Reconnect(str(e))                  # 연결이 이미 끊김 — 남은 읽기는 건너뛴다(v1 is_connected 확인)

    def _set_unresponsive(self, on: bool, e: Optional[Exception] = None) -> None:
        self._unresponsive = bool(on)
        if on:
            self._emit("unresponsive", "⚠ 두께 모니터 응답 없음 — 전원·케이블·MOXA 포트 확인 (재연결 계속 시도)"
                       + (f" [{e}]" if e is not None else ""), "DEVICE_NO_RESPONSE")
        else:
            self._emit("recovered", "두께 모니터 응답 복귀")

    def _set_error(self, text: str) -> None:
        t = _dt.datetime.fromtimestamp(self.clock.wall())
        self._st["last_error"] = f"{t:%H:%M:%S} {text}"

    @staticmethod
    def _blank_status() -> dict:
        return {
            "connected": False, "ok_ts": None, "age_s": None, "stale": True, "link_ok": False, "link_age_s": None,
            "thickness_nm": None, "rate_nm_s": None, "freq_hz": None, "life_pct": None, "crystal_ok": None,
            "reset_seen": 0, "seq": 0, "density": None, "z_factor": None, "tooling": None, "material": "",
            "last_error": "", "t": None,
        }

    def _publish(self) -> None:
        if not self._listeners:
            return
        snap = self.status()
        for cb in list(self._listeners):
            try:
                cb(dict(snap))
            except Exception as e:
                self._emit_throttled(f"listener:{e!r}", "listener_error", f"두께 상태 리스너 예외: {e!r}",
                                     LISTENER_LOG_INTERVAL_S)


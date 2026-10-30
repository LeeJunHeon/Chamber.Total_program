"""챔버12 이온 게이지(IG) 드라이버 — IG ×2(CH1 192.168.1.50:4001, CH2 :4002, MOXA NPort TCP, ASCII 명령 + CR).
TSP 운전이 쓰는 IG(TSP_IG_TCP_PORT 4001 — CH1 게이지와 같은 포트)는 chamber="tsp" — 포트 말고는 CH2 설정값을 쓴다
(v1 TSP 런타임이 cfg 없이 AsyncIG 를 만들면 기본 cfg 가 config_ch2 — device/ig.py:23, runtime/tsp_runtime.py:522-523, D189).

v1 device/ig.py(AsyncIG)를 끝까지 읽고 다시 쓴 것(import 하지 않음).
명령: 'SIG 1'(점등 — 응답이 'OK' 로 시작해야 성공), 'SIG 0'(소등 — 응답 안 기다림), 'RDI'(압력 — '1.2x10e-5' → 소문자·'x10e'→'e'
  → float Torr, 또는 'IG OFF'). 보내기 전 30 ms 늦은 줄 흡수, 응답(또는 무응답) 뒤 IG_GAP_MS(1000), 응답 없는 명령 뒤 min(150, gap) + gap.
  질의 응답 한도 IG_TIMEOUT_MS(3000) — 워커 쪽 재시도 0(무응답이면 None, 연결 유지). 호출 쪽 대기 = 한도 + 0.5 s, 그 안에 못 받으면
  (재시도가 남은 호출만) 연결을 끊고 다시(device/ig.py:941-972). 'OK' 가 아니면 연결을 끊고 다음 시도(device/ig.py:929-939).
베이스 압력 대기(wait_for_base_pressure): SIG 1(5회) → 첫 RDI 지연 IG_FIRST_READ_DELAY_MS(5000) → RDI → 폴링(interval, 기본
  IG_POLLING_INTERVAL_MS 10000) → 압력 ≤ 목표면 base_reached + 정리(SIG 0). 'IG OFF' 면 자동 재점등(IG_REIGNITE_MAX_ATTEMPTS 3,
  IG_REIGNITE_BACKOFF_MS). 대기 한도 = 채널 IG_WAIT_TIMEOUT(v1 실효값 900 s — settings.json ch1·ch2 구역, D137), 하드 한도 = 시작 +
  한도 + 첫 지연 + 5 s. 실패 문구는 v1 그대로 — 'IG ON 실패'·'목표 압력 도달 실패'·'ReigniteLimit'·'HardTimeout'·'InternalError'.
  '목표 도달 실패' 일 때만 붙이는 꼬리(최종·목표 Torr)는 실제 문구가 '목표 압력 도달 실패'라 붙지 않는다(device/ig.py:813 vs 1100 — D181,
  v1 과 같게 둠).
1초 읽기(합의된 변경): v1 은 공정 중·베이스 대기 중에만 10 s 마다 RDI. v2 는 tick 마다 RDI 하나(간격 1 s 때문에 한 tick 에 하나).
  v1 판정(베이스 도달·재점등·시간 초과)과 pressure 이벤트는 v1 주기의 자기 RDI 로(worker 안 op). 상태를 바꾸는 명령(SIG 1·SIG 0)은
  v1 과 같은 계기에서만. 점등 뒤 첫 RDI 지연 동안은 1초 읽기도 보내지 않는다(품질 suppressed).
bench(writes_allowed=False): RDI 만 — SIG 1·SIG 0 은 BENCH_READ_ONLY.
결과: on_done(명령 이름, 사유 코드 또는 None, 문구). 이벤트: IgEvent(v1 IGEvent 와 같은 kind — status·pressure·base_reached·base_failed).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from v2.common.snapshot import DeviceBlock, FieldDef, FType
from v2.common.transport import ReadTimeout, TcpSettings, TransportError
from v2.common.worker import Worker
from v2.ch12.drivers.line_ops import LineReader, OpRunner, Query, Send
from v2.ch12.drivers import ig_ops

PRE_SEND_ABSORB_S = 0.03            # device/ig.py:709 — 보내기 전 늦은 줄 흡수
TICK_GAP_WAIT_S = 0.5
CACHE_FRESH_S = 1.0
TICK_TAG = "[TICK RDI]"             # 1초 읽기(이벤트를 내지 않음)
NEVER_DISCONNECT = 10 ** 9          # v1 IG 워커는 무응답으로 끊지 않는다(재시도 0 질의 — device/ig.py:741-755)


def parse_rdi(line: Optional[str]) -> tuple:
    """(Torr 또는 None, 상태). 'IG OFF' → off, 빈 줄·None → none, 숫자 아님 → invalid(device/ig.py:843-891)."""
    s = (line or "").strip()
    if not s:
        return None, "none"
    if s.upper() == "IG OFF":
        return None, "off"
    try:
        return float(s.lower().replace("x10e", "e")), "ok"
    except Exception:
        return None, "invalid"


@dataclass(frozen=True)
class IgEvent:
    kind: str                      # status / pressure / base_reached / base_failed / io
    detail: str = ""
    reason: Optional[str] = None
    pressure: Optional[float] = None
    code: Optional[str] = None     # v1 오류 코드 — v1 장비 계층은 붙이지 않았다(공정 쪽 E608 등)


class IgSkip(Exception):
    """점등 직후 첫 RDI 지연 — 이번 tick 은 보내지 않음(worker 억제 구간이라 품질 suppressed)."""


class IgNoReply(Exception):
    """1초 읽기 RDI 무응답(v1 처럼 연결은 유지)."""


class IgDriver:
    def __init__(self, clock, worker: Worker, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[IgEvent], None]] = None, chamber: str = "ch1") -> None:
        if chamber not in ("ch1", "ch2", "tsp"):
            raise ValueError(chamber)
        self.clock, self.worker, self.cfg, self.chamber = clock, worker, cfg, chamber
        self.writes_allowed = writes_allowed
        self.on_event = on_event
        self.block = f"{chamber}.ig"
        self.cfg_section = "CH2" if chamber == "tsp" else chamber.upper()      # D189
        self.runner = OpRunner(self, worker, clock)
        self.reader = LineReader()
        self.connected_at: Optional[float] = None
        self.ready_at = -1e9
        self.read_block_until = -1e9
        self.latest: Optional[tuple] = None        # (시각, RDI 줄)
        # v1 상태(device/ig.py:96-130)
        self.target_pressure = 0.0
        self.waiting_active = False
        self.wait_start = 0.0
        self.suspend_reignite = False
        self.total_reignite = 0
        self.last_wait_success = False
        self.last_pressure: Optional[float] = None
        self.cleaning = False
        self.wait_op = None
        self.wait_cb = None
        self.bg_op = None
        worker.on_connect = self._on_connect
        self._prev_worker_event = worker.on_event
        worker.on_event = self._on_worker_event

    def _on_worker_event(self, ev) -> None:
        """끊김 문구 'IG TCP 연결 끊김'(device/ig.py:660-661 — 정리 중이 아닐 때)."""
        if getattr(ev, "kind", "") == "disconnected" and not self.cleaning:
            self.status("IG TCP 연결 끊김")
        if self._prev_worker_event:
            self._prev_worker_event(ev)

    # ───────── 설정 ─────────
    def c(self, key: str, default: Any = None) -> Any:
        k = f"{self.cfg_section}_{key}"
        if k in self.cfg:
            return self.cfg[k]
        return self.cfg.get(key, default)

    def ms(self, key: str, default: float) -> float:
        return float(self.c(key, default)) / 1000.0

    def emit(self, kind: str, detail: str = "", **kw) -> None:
        if self.on_event:
            self.on_event(IgEvent(kind, detail, **kw))

    def status(self, msg: str) -> None:
        self.emit("status", msg)

    # ───────── 줄 입출력(worker 스레드) ─────────
    def _on_connect(self, session) -> None:
        self.connected_at = self.clock.monotonic()
        self.ready_at = self.connected_at
        self.reader.clear()
        host, port = endpoint(self.cfg, self.chamber)
        self.status(f"{host}:{port} 연결 성공 (TCP)")

    def _write(self, session, cmd: str, tag: str = "") -> float:
        if cmd != "RDI" and not self.writes_allowed:
            raise PermissionError(f"bench — 보내지 않음: {cmd}")
        if self.ready_at > self.clock.monotonic():
            self.clock.sleep_until(self.ready_at)
        self.clock.sleep_until(self.clock.monotonic() + PRE_SEND_ABSORB_S)       # 늦은 줄 흡수
        session.transport.reset_input()
        self.reader.clear()
        try:
            session.write((cmd + "\r").encode("ascii", "ignore"))
        except TransportError as e:
            self.status(f"[SEND-ERROR] {tag} {cmd} 전송 오류: {e!r}")             # device/ig.py:717
            raise
        return self.clock.monotonic()

    def io_send(self, session, item: Send) -> float:
        t = self._write(session, item.cmd, item.tag)
        self.ready_at = t + item.gap_s
        self.emit("io", f"[SEND] {item.cmd} (tag={item.tag})")
        return t

    def io_query(self, session, item: Query, retries_left: Optional[int] = None) -> Any:
        sent = item.cmd
        self._write(session, sent, item.tag)
        tick = item.tag == TICK_TAG
        if not tick:
            self.emit("io", f"[SEND] {sent} (tag={item.tag})")
        deadline = min(session.deadline, self.clock.monotonic() + item.timeout_s)
        gap = self.ms("IG_GAP_MS", 1000)
        try:
            while True:
                line = self.reader.read_line(session, deadline)
                if bool(self.c("IG_SKIP_ECHO", True)) and line.strip().upper() == sent.strip().upper():
                    continue
                break
        except ReadTimeout:
            self.status(f"[TIMEOUT] {item.tag} {sent} / 재시도 소진")
            self.reader.clear()
            self.ready_at = self.clock.monotonic() + gap
            return None
        self.ready_at = self.clock.monotonic() + gap
        if sent == "RDI":
            self.latest = (self.clock.monotonic(), line)
        if not tick:
            self.emit("io", f"[RECV] {item.tag} {sent} ← {line}")
        return line

    def exec_timeout_s(self, item) -> float:
        return self.ms("IG_GAP_MS", 1000) + 0.2 + PRE_SEND_ABSORB_S + self.ms("IG_TIMEOUT_MS", 3000) + 1.0

    def query(self, cmd: str, tag: str) -> Query:
        """v1 _send_and_wait_line 한 시도 — 워커 재시도 0, 호출 쪽 대기 = 한도 + 0.5 s."""
        t = self.ms("IG_TIMEOUT_MS", 3000)
        return Query(cmd, t, t + 0.5, (), 0, tag, mark_timeout=True)

    def send_off(self, wait_gap_ms: int) -> Send:
        """SIG 0 — gap max(wait_gap, IG_GAP_MS), 응답 없음, 워커 재시도 0(device/ig.py:976-994)."""
        gap_ms = max(int(wait_gap_ms), int(self.c("IG_GAP_MS", 1000)))
        bound = max(600, int(wait_gap_ms) + 300) / 1000.0
        return Send("SIG 0", (gap_ms + min(150, gap_ms)) / 1000.0, bound_s=bound, withdraw=False, retries=0, tag="[IG OFF]")

    def on_withdraw(self, item: Send, why: str) -> None:
        pass

    def on_ghost_sent(self, item: Send, t: float) -> None:
        self.status(f"[SEND] {item.cmd} (대기열에 남아 늦게 전송)")

    def on_ghost_gave_up(self, item: Send) -> None:
        host, port = endpoint(self.cfg, self.chamber)
        self.status(f"직접 OFF 전송 실패({host}:{port} one-shot): 링크 없음")

    # ───────── 1초 읽기 ─────────
    def _idle(self, now: float) -> bool:
        bu = self.runner.busy_until()
        return bu is None or bu >= now + self.ms("IG_GAP_MS", 1000) + TICK_GAP_WAIT_S + 0.2

    def source_read(self, session, seq: int) -> Dict[str, Any]:
        now = self.clock.monotonic()
        if now < self.read_block_until:
            raise IgSkip("점등 직후 첫 RDI 지연 — 보내지 않음")
        if self._idle(now) and self.ready_at <= now + TICK_GAP_WAIT_S:
            line = self.io_query(session, Query("RDI", self.ms("IG_TIMEOUT_MS", 3000), 0.0, (), 0, TICK_TAG))
            if line is None:
                raise IgNoReply("RDI 무응답")
        return self._fields(self.clock.monotonic())

    def _fields(self, now: float) -> Dict[str, Any]:
        b = self.block
        out: Dict[str, Any] = {f"{b}.waiting": self.waiting_active, f"{b}.target_torr": self.target_pressure or None}
        if self.latest and now - self.latest[0] <= CACHE_FRESH_S:
            p, st = parse_rdi(self.latest[1])
            out[f"{b}.pressure_torr"] = p
            out[f"{b}.state"] = st
        return out

    def block_reads(self, seconds: float) -> None:
        """점등 뒤 첫 RDI 지연 — 1초 읽기를 보내지 않고 품질을 suppressed 로."""
        until = self.clock.monotonic() + seconds
        self.read_block_until = until
        self.worker.suppress(until, "IG 점등 직후 첫 읽기 대기")

    # ───────── 동작(공정 엔진·TSP 가 부름) — ig_ops 에 생성기 ─────────
    def _run(self, key: str, gen, on_done, kind: str = "write") -> None:
        if kind == "write" and not self.writes_allowed:
            if on_done:
                on_done(key, "BENCH_READ_ONLY", "bench — 장비 쓰기 금지")
            return

        def fin(op, reason, detail):
            if reason == ig_ops.SILENT:
                return
            if on_done:
                on_done(key, reason, detail)
        return self.runner.start(key, gen, fin, kind=kind)

    def wait_for_base_pressure(self, base_pressure: float, interval_ms: Optional[int] = None, on_done=None) -> None:
        """베이스 압력 대기(device/ig.py:280-397). on_done(…, None) = 도달, 아니면 사유 코드와 v1 실패 문구."""
        if not self.writes_allowed:
            if on_done:
                on_done("WAIT_FOR_BASE", "BENCH_READ_ONLY", "bench — 장비 쓰기 금지")
            return
        self.runner.call_in_worker(lambda: ig_ops.start_wait(self, float(base_pressure), interval_ms, on_done), "ig:wait")

    def cancel_wait(self, reason: str = "user cancel / stop", on_done=None) -> None:
        self._run("CANCEL_WAIT", ig_ops.cancel_wait(self, reason), on_done)

    def set_process_status(self, should_poll: bool) -> None:
        """device/ig.py:1248-1274 — 공정 중 10 s RDI(pressure 이벤트) 켜기/끄기. 끄면 대기 해제·재점등 금지·대기열 폐기."""
        self.runner.call_in_worker(lambda: ig_ops.set_process_status(self, bool(should_poll)), "ig:process")

    def ensure_on(self, on_done=None) -> None:
        self._run("ENSURE_ON", ig_ops.ensure_on(self), on_done)

    def ensure_off(self, on_done=None) -> None:
        self._run("ENSURE_OFF", ig_ops.ensure_off(self), on_done)

    def read_pressure(self, on_done=None) -> None:
        """RDI 1회(재시도 0) — on_done(…, None, 'Torr 값') 또는 'RDI timeout'·'parse error: …'(device/ig.py:1162-1177)."""
        self._run("READ_PRESSURE", ig_ops.read_pressure(self), on_done, kind="read")

    def cleanup(self, on_done=None) -> None:
        """v1 cleanup(device/ig.py:192-254)의 장비 부분 — SIG 0 최선 전송·대기열 폐기. 포트는 1초 읽기 때문에 유지."""
        self._run("CLEANUP", ig_ops.cleanup(self), on_done)

    def command(self, cmd: str, on_done=None, **kwargs) -> None:
        """v1 command 라우터(device/ig.py:1329-1371) — v1 안에서 호출처 없음(DEV-118). SET_ENDPOINT 는 v2 조립 몫."""
        ig_ops.route(self, cmd, kwargs, on_done)

    def is_connected(self) -> bool:
        return bool(self.worker.connected)


# ───────── 블록·예산·조립 도움 ─────────
def blocks(chamber: str) -> List[DeviceBlock]:
    f = (
        FieldDef("pressure_torr", FType.FLOAT, "Torr", "진공도(IG)", fmt=".3e", source="RDI", own_quality=True, summary="mean"),
        FieldDef("state", FType.STR, "", "IG 상태", source="RDI", own_quality=True,
                 enum_labels=(("ok", "정상"), ("off", "IG OFF"), ("invalid", "값 오류"), ("none", "빈 응답"))),
        FieldDef("waiting", FType.BOOL, "", "베이스 대기 중", source="wait_for_base_pressure", summary="any", detail=True),
        FieldDef("target_torr", FType.FLOAT, "Torr", "베이스 목표", fmt=".3e", source="wait_for_base_pressure", detail=True),
    )
    name = {"ch1": "CH1 IG", "ch2": "CH2 IG", "tsp": "TSP IG"}[chamber]
    return [DeviceBlock(f"{chamber}.ig", name, f, desc="MOXA NPort TCP ASCII(RDI·SIG 1·SIG 0)")]


BUDGET = dict(
    queries_per_tick=1, bytes_tx=4, bytes_rx=12, min_gap_ms=1030, fits_1s=False, alternate=None,
    note="RDI(4 B → '1.2x10e-5\\r\\n' 약 11 B) 하나. 응답 뒤 IG_GAP_MS 1 s + 보내기 전 흡수 30 ms 라 한 주기 ≈ 1.03 s + 응답 시간 — "
         "약 10 tick 에 한 번은 간격을 못 맞춰 그 tick 은 안 읽음(stale). 점등 뒤 첫 RDI 지연 5 s 는 안 읽음(suppressed). "
         "v1 판정용 RDI(베이스 대기·공정 중 10 s)는 제어 요청으로 먼저. 마감 = 대기 0.5 s + IG_TIMEOUT_MS 3 s",
)


def source_deadline_ms(cfg: Dict[str, Any]) -> int:
    return int((TICK_GAP_WAIT_S + PRE_SEND_ABSORB_S) * 1000 + float(cfg.get("IG_TIMEOUT_MS", 3000)) + 100)


def endpoint(cfg: Dict[str, Any], chamber: str) -> tuple:
    host = cfg.get("IG_TCP_HOST", "192.168.1.50")
    if chamber == "tsp":
        return str(host), int(cfg.get("TSP_IG_TCP_PORT", 4001))
    return str(host), int(cfg[f"{chamber.upper()}_IG_TCP_PORT"])


def tcp_settings(cfg: Dict[str, Any], chamber: str) -> TcpSettings:
    host, port = endpoint(cfg, chamber)
    return TcpSettings(host, port, connect_timeout_s=max(0.5, float(cfg.get("IG_CONNECT_TIMEOUT_S", 3.0))))


def make_worker(clock, transport, cfg: Dict[str, Any], chamber: str, writes_allowed: bool, on_event=None) -> Worker:
    """v1 재연결 규칙 — 무응답으로 끊지 않음(끊기는 드라이버가 v1 계기에서), 백오프 1 s ×2 최대 20 s."""
    mx = float(cfg.get("IG_RECONNECT_BACKOFF_MAX_MS", 20000)) / 1000.0
    return Worker(f"{chamber}.ig", transport, writes_allowed=writes_allowed, fail_streak=NEVER_DISCONNECT,
                  reconnect_start_s=float(cfg.get("IG_RECONNECT_BACKOFF_START_MS", 1000)) / 1000.0,
                  reconnect_max_s=mx, long_outage_s=600.0, probe_s=mx, log_every_s=600.0, on_event=on_event)

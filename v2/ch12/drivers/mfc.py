"""챔버12 MFC 드라이버 — MFC ×2(CH1 192.168.1.50:4003, CH2 :4006, MOXA NPort TCP, ASCII 명령 + CR).

v1 device/mfc.py(AsyncMFC)를 끝까지 읽고 다시 쓴 것(import 하지 않음). v1 운영 구성은 main.py:419-436 —
enable_verify=False·enable_stabilization=True. 그래서 v1 운영에서 실제로 도는 경로만 옮겼다(검증 경로 R6{4+ch}·R6·R7·L0+R69 는
main 이 끈 경로라 v1 운영에서 보내지 않는다 — DEV-096).

명령 글자(v1 lib/config_common.py:300-330 MFC_COMMANDS 와 같은 바이트, 끝에 '\\r'):
  FLOW_SET 'Q{ch} {값}' — 값 = str(float(sccm × MFC_SCALE_FACTORS[ch])) 그대로(예 'Q1 10.0', 'Q2 11.000000000000002' — D183)
  FLOW_ON 'L{ch}1' · FLOW_OFF 'L{ch}0' · MFC_ZEROING 'L{4+ch}1' · PS_ZEROING 'Z1' · VALVE_OPEN 'O' · VALVE_CLOSE 'C'
  SPn_ON 'D{n}' · SPn_SET 'S{n} {값}' — 값 = str(round(UI × MFC_PRESSURE_SCALE 0.1, MFC_PRESSURE_DECIMALS 3))
  질의 R60(전체 유량, 'Q0…') · R5(압력, 'P…') · R1~R4(SP 값, 'S{n}…')
응답 없는 명령(no-reply)은 v1 _await_sent 처럼 '실제로 보낸 것'을 최대 30 s 기다려 확정한다. 넘으면 켜는 계열은 철회 후 실패,
  끄는 계열(FLOW_OFF·VALVE_OPEN)은 실패를 알리되 명령은 남겨 순서대로 보낸다(device/mfc.py:2032-2064).
간격: 응답을 받은 뒤 MFC_GAP_MS(1000) — no-reply 는 보낸 뒤 gap + 늦은 줄 흡수 MFC_ALLOW_NO_REPLY_DRAIN_MS(80).
  연결 직후 첫 명령 전 MFC_POST_OPEN_QUIET_MS(800). 질의 응답 한도 MFC_TIMEOUT(2000).
무응답: 연결 뒤 2 s 안이면 재시도(연결 유지), 그 밖은 응답 1회 늦어도 연결을 끊고 재연결(v1 device/mfc.py:1419-1437 — D182,
  worker fail_streak=1). 끊기면 재연결 간격 MFC_RECONNECT_BACKOFF_START_MS 1000 ×2, 최대 20000.
1초 읽기(합의된 변경): v1 은 공정 중(set_process_status True)에만 3 s 마다 R60 → R5 를 읽었다. v2 는 tick 마다 하나씩 읽는다 —
  명령 간격 1 s 때문에 1초에 질의 하나라 R60·R5 를 번갈아(자기 품질·나이). 제어 동작(op)이 진행 중이거나 막 끝났으면(1.5 s)
  읽지 않고 양보한다(v1 폴링도 비-폴링 명령에 양보 — device/mfc.py:1554-1557) — 그때 행에는 제어가 읽은 R60·R5 가 1 s 안의 값이면 들어간다.
  v1 의 '폴링' 판정(유량 이탈 경고·flow/pressure 이벤트 → 공정 기록·모니터)은 v1 주기(MFC_POLLING_INTERVAL_MS 3 s, 폴링 사용자가
  있을 때만, 가스/압력 마스크대로) 그대로 — 1초 읽기 값 중 그 주기에 처음 온 값을 쓴다.
판정(안정화 R60 1 s × 최대 180 · WAIT_PRESSURE R5 1 s)은 v1 타이밍 그대로 worker 안에서(op 생성기).
폴링 소유권: 사용자 집합(acquire_poll/release_poll) — 마지막 사용자가 끌 때만 폴링을 멈춘다(runtime/chamber_runtime.py:6533-6558),
  플라즈마 클리닝은 set_poll_mask 로 gas-only/pressure-only(runtime/plasma_cleaning_runtime.py:529-551).
bench(writes_allowed=False): R 질의만 — 쓰기 동작은 BENCH_READ_ONLY(worker 거부 + 드라이버 이중 보호).
명령 동작의 결과: on_done(명령 이름, 사유 코드 또는 None(확정), v1 실패 문구). 이벤트: MfcEvent(v1 MFCEvent 와 같은 kind).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from v2.common.snapshot import DeviceBlock, FieldDef, FType
from v2.common.transport import ReadTimeout, TcpSettings
from v2.common.worker import Worker
from v2.ch12.drivers.line_ops import RETRY, LineReader, OpRunner, Query, Send, echo_queue
from v2.ch12.drivers import mfc_ops

GAS_MAP = {1: "Ar", 2: "O2", 3: "N2"}            # device/mfc.py:166
CONNECT_RETRY_WINDOW_S = 2.0                     # device/mfc.py:1425 — 연결 뒤 이 안의 무응답은 연결 유지 재시도
TICK_GAP_WAIT_S = 0.5                            # 1초 읽기가 명령 간격을 기다려 주는 최대(넘으면 이번 tick 은 안 읽음)
IDLE_HOLD_S = 1.5                                # 제어 동작이 끝난 뒤 1초 읽기를 쉬는 시간(공정 엔진의 다음 명령 몫)
CACHE_FRESH_S = 1.0                              # 제어가 읽은 값을 1초 행에 쓰는 최대 나이

# v1 lib/config_common.py:300-330 MFC_COMMANDS — 같은 글자(함수/문자열)
MFC_COMMANDS: Dict[str, Any] = {
    "SET_ONOFF_MASK": lambda bits: f"L0{bits}",
    "FLOW_ON": lambda channel: f"L{int(channel)}1",
    "FLOW_OFF": lambda channel: f"L{int(channel)}0",
    "MFC_ZEROING": lambda channel: f"L{4 + int(channel)}1",
    "FLOW_SET": lambda channel, value: f"Q{int(channel)} {value}",
    "READ_FLOW_ALL": "R60",
    "READ_FLOW": lambda channel: f"R6{int(channel)}",
    "READ_MFC_ON_OFF_STATUS": "R69",
    "READ_PRESSURE": "R5",
    "READ_SP1_VALUE": "R1", "READ_SP2_VALUE": "R2", "READ_SP3_VALUE": "R3", "READ_SP4_VALUE": "R4",
    "READ_VALVE_POSITION": "R6",
    "READ_SYSTEM_STATUS": "R7",
    "READ_FLOW_SET": lambda channel: f"R6{4 + int(channel)}",
    "VALVE_OPEN": "O",
    "VALVE_CLOSE": "C",
    "PS_ZEROING": "Z1",
    "SP1_ON": "D1", "SP2_ON": "D2", "SP3_ON": "D3", "SP4_ON": "D4",
    "SP1_SET": lambda value: f"S1 {value}", "SP2_SET": lambda value: f"S2 {value}",
    "SP3_SET": lambda value: f"S3 {value}", "SP4_SET": lambda value: f"S4 {value}",
}


def mk_cmd(key: str, *args, **kwargs) -> str:
    """v1 _mk_cmd(device/mfc.py:2117-2125)."""
    v = MFC_COMMANDS[key]
    return str(v(*args, **kwargs)) if callable(v) else str(v)


# ───────── 파서(v1 _parse_* 와 같은 규칙) ─────────
_NUM = r"([+\-]?\d+(?:\.\d+)?)"


def parse_r60(line: Optional[str]) -> Optional[List[float]]:
    """device/mfc.py:1863-1871 — 'Q0' 로 시작해야 하고 뒤의 부호 있는 숫자 전부."""
    s = (line or "").strip()
    if not s.startswith("Q0"):
        return None
    try:
        return [float(x) for x in re.findall(_NUM, s[2:])]
    except Exception:
        return None


def parse_pressure(line: Optional[str]) -> Optional[float]:
    """device/mfc.py:1907-1924 — 대문자화 후 '+' 뒤 숫자, 없으면 마지막 숫자."""
    s = (line or "").strip().upper()
    if not s:
        return None
    m = re.search(r"\+\s*" + _NUM, s)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            pass
    nums = re.findall(_NUM, s)
    if not nums:
        return None
    try:
        return float(nums[-1])
    except Exception:
        return None


@dataclass(frozen=True)
class MfcEvent:
    kind: str                      # status / flow / pressure / command_confirmed / command_failed / io / link
    detail: str = ""               # status 문구 · 실패 사유(v1 reason 문구)
    reason: Optional[str] = None   # 사유 코드(실패)
    cmd: Optional[str] = None
    owner: Optional[str] = None
    channel: Optional[int] = None
    gas: Optional[str] = None
    value: Optional[float] = None
    text: Optional[str] = None
    code: Optional[str] = None     # v1 오류 코드(E###) — v1 장비 계층은 붙이지 않았다(공정 쪽이 붙임)


class MfcReadOnly(Exception):
    reason = "BENCH_READ_ONLY"


class MfcDriver:
    def __init__(self, clock, worker: Worker, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[MfcEvent], None]] = None, chamber: str = "ch1",
                 enable_stabilization: bool = True) -> None:
        if chamber not in ("ch1", "ch2"):
            raise ValueError(chamber)
        self.clock, self.worker, self.cfg, self.chamber = clock, worker, cfg, chamber
        self.writes_allowed = writes_allowed
        self.on_event = on_event
        self.block = f"{chamber}.mfc"
        self.resource_key = "MFC1" if chamber == "ch1" else "MFC2"       # main.py:426·436
        self.stab_enabled = bool(enable_stabilization)
        self.runner = OpRunner(self, worker, clock)
        self.reader = LineReader()
        self.echo = echo_queue()
        self.connected_at: Optional[float] = None
        self.ready_at = -1e9
        # v1 상태(device/mfc.py:166-182)
        self.last_setpoints = {1: 0.0, 2: 0.0, 3: 0.0}          # 장비 단위(%FS)
        self.flow_error_counters = {1: 0, 2: 0, 3: 0}
        self.flow_on_flags = {1: False, 2: False, 3: False}
        self.ch_owner: Dict[int, Optional[str]] = {1: None, 2: None, 3: None}
        self.selected_ch: Optional[int] = None
        self.selected_owner: Optional[str] = None
        self.mask_shadow = "0000"
        self.stab_jobs: Dict[int, mfc_ops.StabJob] = {}
        self.stab_op = None
        self.shutdown = False
        # 폴링(v1 공정 중 3 s 판정)·소유권
        self.polling = False
        self.poll_users: set = set()
        self.poll_gas = True
        self.poll_pressure = True
        self._poll_next = 0.0
        self._poll_stage: Optional[str] = None
        self.latest: Dict[str, tuple] = {}                       # "R60"/"R5" → (시각, 줄)
        worker.on_connect = self._on_connect

    # ───────── 설정 ─────────
    def c(self, key: str, default: Any = None) -> Any:
        """채널 구역 키("CH1_"…)가 있으면 그 값, 아니면 공통 키."""
        k = f"{self.chamber.upper()}_{key}"
        if k in self.cfg:
            return self.cfg[k]
        return self.cfg.get(key, default)

    def ms(self, key: str, default: float) -> float:
        return float(self.c(key, default)) / 1000.0

    def scale(self, ch: int) -> float:
        """MFC_SCALE_FACTORS[ch] — 채널 cfg 값(없는 채널은 1.0, device/mfc.py:2225-2242)."""
        return float(self.c(f"MFC_SCALE_FACTORS_{int(ch)}", 1.0))

    def ui_to_hw(self, ch: int, ui: float) -> float:
        return float(ui) * self.scale(ch)

    def hw_to_ui(self, ch: int, hw: float) -> float:
        sf = self.scale(ch)
        return float(hw) / (sf if sf != 0 else 1.0)

    # ───────── 이벤트 ─────────
    def emit(self, kind: str, detail: str = "", **kw) -> None:
        if self.on_event:
            self.on_event(MfcEvent(kind, detail, **kw))

    def status(self, msg: str, **kw) -> None:
        self.emit("status", msg, **kw)

    # ───────── 줄 입출력(worker 스레드) ─────────
    def _on_connect(self, session) -> None:
        now = self.clock.monotonic()
        self.connected_at = now
        self.ready_at = now + self.ms("MFC_POST_OPEN_QUIET_MS", 800)        # device/mfc.py:1370-1377
        self.reader.clear()
        host, port = endpoint(self.cfg, self.chamber)
        self.status(f"{host}:{port} 연결 성공 (TCP)")

    def _wait_ready(self) -> None:
        if self.ready_at > self.clock.monotonic():
            self.clock.sleep_until(self.ready_at)

    def _write(self, session, cmd: str) -> float:
        if not cmd.startswith("R") and not self.writes_allowed:
            raise MfcReadOnly(f"bench — 보내지 않음: {cmd}")
        self._wait_ready()
        session.write((cmd + "\r").encode("ascii", "ignore"))
        return self.clock.monotonic()

    def io_send(self, session, item: Send) -> float:
        """no-reply 명령 — 보낸 시각. 다음 명령은 gap + 늦은 줄 흡수(80 ms) 뒤(device/mfc.py:1400-1410)."""
        t = self._write(session, item.cmd)
        self.echo.append(item.cmd)
        self.ready_at = t + item.gap_s + self.ms("MFC_ALLOW_NO_REPLY_DRAIN_MS", 80)
        self.emit("io", f"[SEND] {item.cmd} ({item.tag})" if item.tag else f"[SEND] {item.cmd}")
        return t

    def _read_reply(self, session, sent: str, prefixes: tuple, timeout_s: float) -> str:
        """v1 _read_one_line_skip_echo(device/mfc.py:1445-1475): 보낸 명령 에코·과거 no-reply 에코·접두사 불일치 줄은 건너뜀."""
        deadline = min(session.deadline, self.clock.monotonic() + timeout_s)
        skip_echo = bool(self.c("MFC_SKIP_ECHO", True))
        while True:
            line = self.reader.read_line(session, deadline)
            if skip_echo and line == sent.strip():
                continue
            if self.echo and line == self.echo[0]:
                self.echo.popleft()
                continue
            if prefixes and not any(line.startswith(p) for p in prefixes):
                continue
            return line

    def io_query(self, session, item: Query, retries_left: Optional[int] = None) -> Any:
        sent = item.cmd
        self._write(session, sent)
        if item.tag:                                                    # v1 '[SEND] R60 ([STAB R60 ch1])'(device/mfc.py:1359)
            self.emit("io", f"[SEND] {sent} ({item.tag})")
        try:
            line = self._read_reply(session, sent, item.prefixes, item.timeout_s)
        except ReadTimeout:
            left = item.retries if retries_left is None else retries_left
            if (left > 0 and self.connected_at is not None
                    and self.clock.monotonic() - self.connected_at < CONNECT_RETRY_WINDOW_S):
                # 연결 직후 — 줄 큐만 흡수(100 ms), max(0.15 s, gap) 쉬고 다시(연결 유지)
                self.clock.sleep_until(self.clock.monotonic() + 0.1)
                session.transport.reset_input()
                self.reader.clear()
                self.ready_at = self.clock.monotonic() + max(0.15, self.ms("MFC_GAP_MS", 1000))
                self.status(f"[TIMEOUT] {item.tag} {sent}".replace("  ", " "))
                return RETRY
            self.status(f"[TIMEOUT] {item.tag} {sent}".replace("  ", " "))
            self.reader.clear()
            raise                                                       # worker: 1회 → 연결 끊고 재연결(D182)
        self.ready_at = self.clock.monotonic() + self.ms("MFC_GAP_MS", 1000)
        if sent in ("R60", "R5"):
            self.latest[sent] = (self.clock.monotonic(), line)
        if item.tag:                                                    # 1초 읽기(tag 없음)는 이벤트를 내지 않는다
            self.emit("io", f"[RECV] {item.tag} ← {line}")
        return line

    def query(self, cmd: str, prefixes: tuple, tag: str = "", retries: int = 1) -> Query:
        """v1 _send_and_wait_line 한 번 — 호출 쪽 대기 = timeout + 2 s (+ 연결 뒤 2 s 안이면 2 s 더)."""
        t = self.ms("MFC_TIMEOUT", 2000)
        extra = 0.0
        if self.connected_at is not None and self.clock.monotonic() - self.connected_at < CONNECT_RETRY_WINDOW_S:
            extra = self.ms("MFC_FIRST_CMD_EXTRA_TIMEOUT_MS", 2000)
        return Query(cmd, t, t + 2.0 + extra, prefixes, retries, tag)

    def send(self, cmd: str, tag: str, gap_ms: Optional[float] = None, withdraw: bool = True) -> Send:
        gap = self.ms("MFC_GAP_MS", 1000) if gap_ms is None else float(gap_ms) / 1000.0
        return Send(cmd, gap, withdraw=withdraw, tag=tag)

    # OpRunner 콜백
    def exec_timeout_s(self, item) -> float:
        """요청 하나의 실행 한도 — 간격 대기(연결 직후 쉼 포함) + 응답 한도 + 여유."""
        return (self.ms("MFC_GAP_MS", 1000) + self.ms("MFC_ALLOW_NO_REPLY_DRAIN_MS", 80)
                + self.ms("MFC_POST_OPEN_QUIET_MS", 800) + self.ms("MFC_TIMEOUT", 2000) + 1.0)

    def on_withdraw(self, item: Send, why: str) -> None:
        self.status(f"[WITHDRAW] {item.tag} 대기열에서 철회 ({why})")

    def on_ghost_sent(self, item: Send, t: float) -> None:
        self.status(f"{item.tag} 늦게 전송됨(대기열에 남겨 둔 명령)")

    def on_ghost_gave_up(self, item: Send) -> None:
        self.status(f"{item.tag} 전송 포기 — 링크 없음", )

    # ───────── 1초 읽기(스냅샷 묶기 소스) ─────────
    def _idle(self, now: float) -> bool:
        bu = self.runner.busy_until()
        gap = self.ms("MFC_GAP_MS", 1000)
        return ((bu is None or bu >= now + gap + TICK_GAP_WAIT_S + 0.2)
                and now - self.runner.last_end >= IDLE_HOLD_S and not self.runner.ghosts)

    def source_read(self, session, seq: int) -> Dict[str, Any]:
        now = self.clock.monotonic()
        if self._idle(now) and self.ready_at <= now + TICK_GAP_WAIT_S:
            want = {"gas": "R60", "pressure": "R5"}.get(self._poll_stage or "")
            if want is None:
                ages = {k: (self.latest[k][0] if k in self.latest else -1e9) for k in ("R60", "R5")}
                want = min(("R60", "R5"), key=lambda k: ages[k])
            pre = ("Q0",) if want == "R60" else ("P",)
            self.io_query(session, Query(want, self.ms("MFC_TIMEOUT", 2000), 0.0, pre, 1, ""))
            self._v1_poll(self.clock.monotonic())
        return self._fields(self.clock.monotonic())

    def _fields(self, now: float) -> Dict[str, Any]:
        b = self.block
        out: Dict[str, Any] = {}
        r60 = self.latest.get("R60")
        if r60 and now - r60[0] <= CACHE_FRESH_S:
            vals = parse_r60(r60[1]) or []
            for ch, gas in GAS_MAP.items():
                if ch - 1 < len(vals):
                    out[f"{b}.{gas.lower()}_sccm"] = self.hw_to_ui(ch, vals[ch - 1])
        r5 = self.latest.get("R5")
        if r5 and now - r5[0] <= CACHE_FRESH_S:
            hw = parse_pressure(r5[1])
            if hw is not None:
                out[f"{b}.pressure_mtorr"] = hw / float(self.c("MFC_PRESSURE_SCALE", 0.1))
        for ch, gas in GAS_MAP.items():
            out[f"{b}.{gas.lower()}_set_sccm"] = self.hw_to_ui(ch, self.last_setpoints[ch])
            out[f"{b}.{gas.lower()}_on"] = self.flow_on_flags[ch]
        out[f"{b}.polling"] = self.polling
        return out

    def _v1_poll(self, now: float) -> None:
        """v1 폴링 사이클(device/mfc.py:1543-1587)의 판정·이벤트를 v1 주기로 — 사이클 = (가스 마스크) R60 → (압력 마스크) R5."""
        if not self.polling:
            self._poll_stage = None
            return
        if self._poll_stage is None:
            if now < self._poll_next:
                return
            self._poll_stage = "gas" if self.poll_gas else ("pressure" if self.poll_pressure else None)
            if self._poll_stage is None:
                self._poll_next = now + self.ms("MFC_POLLING_INTERVAL_MS", 3000)
                return
            self._poll_t0 = now - 1e-9 - CACHE_FRESH_S
        if self._poll_stage == "gas":
            r = self.latest.get("R60")
            if not r or r[0] < self._poll_t0:
                return
            vals = parse_r60(r[1])
            if vals:
                for ch, name in GAS_MAP.items():
                    if ch - 1 < len(vals):
                        self.emit("flow", gas=name, value=self.hw_to_ui(ch, vals[ch - 1]))
                        self.monitor_flow(ch, float(vals[ch - 1]))
            self._poll_stage = "pressure" if self.poll_pressure else "end"
        if self._poll_stage == "pressure":
            r = self.latest.get("R5")
            if not r or r[0] < self._poll_t0:
                return
            self.emit_pressure(r[1])
            self._poll_stage = "end"
        if self._poll_stage == "end":
            self._poll_stage = None
            self._poll_next = now + self.ms("MFC_POLLING_INTERVAL_MS", 3000)

    def emit_pressure(self, line: str) -> Optional[float]:
        """v1 _emit_pressure_from_line_sync(device/mfc.py:1926-1941) — UI = hw / 0.1, 문자열 소수 3자리."""
        hw = parse_pressure(line)
        if hw is None:
            return None
        ui = float(hw) / float(self.c("MFC_PRESSURE_SCALE", 0.1))
        self.emit("pressure", value=ui, text=("{:." + str(int(self.c("MFC_PRESSURE_DECIMALS", 3))) + "f}").format(ui))
        return ui

    def monitor_flow(self, channel: int, actual_hw: float) -> None:
        """v1 _monitor_flow(device/mfc.py:1943-1969) — ON 된 채널만, 경고만(공정 계속)."""
        if not self.flow_on_flags.get(channel, False):
            self.flow_error_counters[channel] = 0
            return
        target = float(self.last_setpoints.get(channel, 0.0))
        if target < 0.1:
            self.flow_error_counters[channel] = 0
            return
        tol = float(self.c("FLOW_ERROR_TOLERANCE", 0.05))
        if abs(actual_hw - target) > target * tol:
            self.flow_error_counters[channel] += 1
            if self.flow_error_counters[channel] >= int(self.c("FLOW_ERROR_MAX_COUNT", 3)):
                self.status(f"Ch{channel} GAS 불안정! (목표: {target:.2f}, 현재: {actual_hw:.2f})")
                self.flow_error_counters[channel] = 0
        else:
            self.flow_error_counters[channel] = 0

    # ───────── 폴링 소유권 ─────────
    def set_poll_mask(self, *, gas: bool = True, pressure: bool = True) -> None:
        """device/mfc.py:665-673 — 플라즈마 클리닝이 두 MFC 를 gas-only/pressure-only 로 나눌 때."""
        self.poll_gas, self.poll_pressure = bool(gas), bool(pressure)

    def set_process_status(self, should_poll: bool) -> None:
        """device/mfc.py:1154-1168 — v1 폴링 켜기/끄기(v2: 1초 읽기는 늘 — 이것은 v1 주기 판정·이벤트만)."""
        if should_poll and not self.polling:
            self.polling = True
            self._poll_next, self._poll_stage = self.clock.monotonic(), None
            self.status("Polling read 시작")
        elif not should_poll:
            self.polling, self._poll_stage = False, None
            self.status("Polling read 중지")

    def acquire_poll(self, owner: str) -> None:
        """챔버가 MFC 폴링을 켤 때: 사용자 획득 → 마스크 원복 → 폴링(runtime/chamber_runtime.py:6539-6545)."""
        self.poll_users.add(owner)
        self.set_poll_mask(gas=True, pressure=True)
        self.set_process_status(True)

    def release_poll(self, owner: str) -> int:
        """사용자 해제 — 남은 사용자가 0 일 때만 폴링 중단. 남은 사용자 수(chamber_runtime.py:6547-6556)."""
        self.poll_users.discard(owner)
        left = len(self.poll_users)
        if left == 0:
            self.set_process_status(False)
        else:
            self.status(f"{self.resource_key} 폴링 유지 — 사용 중: {', '.join(sorted(self.poll_users))}")
        return left

    def other_users(self, owner: str) -> set:
        return set(self.poll_users) - {owner}

    # ───────── 동작(공정 엔진이 부름) — mfc_ops 에 생성기 ─────────
    def is_connected(self) -> bool:
        return bool(self.worker.connected) and not self.shutdown

    def _run(self, key: str, gen, on_done, owner=None, channel=None, kind: str = "write", quiet: bool = False) -> None:
        """op 시작 — 끝나면 v1 처럼 command_confirmed/command_failed 이벤트(quiet 면 생성기가 직접 낸다) + on_done."""
        if kind == "write" and not self.writes_allowed:
            if on_done:
                on_done(key, "BENCH_READ_ONLY", "bench — 장비 쓰기 금지")
            return

        def fin(op, reason, detail):
            if reason == mfc_ops.PENDING:
                return
            if not quiet:
                if reason is None:
                    self.emit("command_confirmed", cmd=key, owner=owner, channel=channel)
                else:
                    self.emit("command_failed", detail, reason=reason, cmd=key, owner=owner, channel=channel)
            if on_done:
                on_done(key, reason, detail)
        self.runner.start(key, gen, fin, kind=kind)

    def set_flow(self, channel: int, ui_value: float, owner: Optional[str] = None, on_done=None) -> None:
        self._run("FLOW_SET", mfc_ops.set_flow(self, int(channel), float(ui_value), owner), on_done, owner, int(channel))

    def flow_on(self, channel: int, owner: Optional[str] = None, on_done=None) -> None:
        self._run("FLOW_ON", mfc_ops.flow_on(self, int(channel), owner, on_done), on_done, owner, int(channel))

    def flow_off(self, channel: int, owner: Optional[str] = None, on_done=None) -> None:
        self._run("FLOW_OFF", mfc_ops.flow_off(self, int(channel), owner), on_done, owner, int(channel))

    def gas_select(self, gas_idx: int, owner: Optional[str] = None) -> None:
        """device/mfc.py:526-533 — 플라즈마 클리닝 선택 가스(없는 채널은 ValueError)."""
        gi = int(gas_idx)
        if gi not in GAS_MAP:
            raise ValueError(f"지원하지 않는 가스 채널: {gas_idx}")
        self.selected_ch, self.selected_owner = gi, owner
        self.ch_owner[gi] = owner
        self.status(f"가스 선택: ch{gi} ({GAS_MAP.get(gi, '-')})")

    def _require_selected(self) -> int:
        gi = int(self.selected_ch or 0)
        if gi not in GAS_MAP:
            raise RuntimeError("선택된 가스가 없습니다. gas_select()를 먼저 호출하세요.")
        return gi

    def flow_set_on(self, ui_value: float, owner: Optional[str] = None, on_done=None) -> None:
        ch = self._require_selected()
        self._run("FLOW_ON", mfc_ops.flow_set_on(self, ch, float(ui_value), owner, on_done), on_done, owner, ch)

    def flow_off_selected(self, owner: Optional[str] = None, on_done=None) -> None:
        self.flow_off(self._require_selected(), owner, on_done)

    def valve_open(self, owner: Optional[str] = None, on_done=None) -> None:
        self._run("VALVE_OPEN", mfc_ops.valve(self, "VALVE_OPEN", owner), on_done, owner)

    def valve_close(self, owner: Optional[str] = None, on_done=None) -> None:
        self._run("VALVE_CLOSE", mfc_ops.valve(self, "VALVE_CLOSE", owner), on_done, owner)

    def sp_set(self, n: int, ui_value: float, owner: Optional[str] = None, on_done=None) -> None:
        """SP1/SP2/SP4_SET — v1 에 SP3_SET API 는 없다(device/mfc.py:675-738, handle_command 도 없음)."""
        if int(n) not in (1, 2, 4):
            raise ValueError(f"SP{n}_SET 없음")
        self._run(f"SP{int(n)}_SET", mfc_ops.sp_set(self, int(n), float(ui_value), owner), on_done, owner)

    def sp_on(self, n: int, owner: Optional[str] = None, on_done=None) -> None:
        if int(n) not in (1, 2, 3, 4):
            raise ValueError(n)
        key = f"SP{int(n)}_ON"
        self._run(key, mfc_ops.noreply(self, key, mk_cmd(key), f"[{key}]", owner, None,
                                       min_delay_s=self.ms("MFC_GAP_MS", 1000)), on_done, owner)

    def ps_zeroing(self, owner: Optional[str] = None, on_done=None) -> None:
        self._run("PS_ZEROING", mfc_ops.zeroing(self, None, owner), on_done, owner)

    def mfc_zeroing(self, channel: int, owner: Optional[str] = None, on_done=None) -> None:
        self._run("MFC_ZEROING", mfc_ops.zeroing(self, int(channel), owner), on_done, owner, int(channel))

    def wait_pressure(self, target: float, timeout_sec: Optional[float] = None, use_sp_target: bool = False,
                      sp_index: Optional[int] = None, owner: Optional[str] = None, on_done=None) -> None:
        self._run("WAIT_PRESSURE", mfc_ops.wait_pressure(self, float(target), timeout_sec, use_sp_target, sp_index, owner),
                  on_done, owner, kind="read")

    def read_flow_all(self, owner: Optional[str] = None, on_done=None) -> None:
        self._run("READ_FLOW", mfc_ops.read_flow_all(self, owner), on_done, owner, kind="read", quiet=True)

    def read_pressure(self, owner: Optional[str] = None, on_done=None) -> None:
        self._run("READ_PRESSURE", mfc_ops.read_pressure_cmd(self, owner), on_done, owner, kind="read", quiet=True)

    def command(self, cmd: str, args: Optional[dict] = None, owner: Optional[str] = None, on_done=None) -> None:
        """v1 handle_command(device/mfc.py:984-1145) — 문자열 명령 라우터."""
        mfc_ops.route(self, cmd, args or {}, owner, on_done)

    # ───────── 정리 ─────────
    def on_process_finished(self, success: bool, reason: Optional[str] = None) -> None:
        """device/mfc.py:1170-1189 — 공정 종료 전체 리셋(대기열 전체 폐기). 공유 MFC 에 다른 사용자가 있으면 release_owner."""
        label = reason or f"process finished ({'ok' if success else 'fail'})"
        self.runner.call_in_worker(lambda: mfc_ops.finish_all(self, label), "mfc:finished")

    def on_process_cleanup(self) -> None:
        self.on_process_finished(False, reason="cleanup")

    def release_owner(self, owner: Optional[str], reason: str = "") -> None:
        """device/mfc.py:1191-1216 — owner 몫만 정리(대기열 유지)."""
        if owner:
            self.runner.call_in_worker(lambda: mfc_ops.release_owner(self, owner, reason), "mfc:release")

    def cleanup(self) -> None:
        """v1 cleanup(device/mfc.py:304-372)의 상태 부분 — 종료 중 표시·안정화 실패 통지·전체 폐기.
        포트 닫기는 worker(조립) 몫 — v2 는 1초 읽기를 위해 연결을 유지한다."""
        self.shutdown = True
        self.runner.call_in_worker(lambda: mfc_ops.cleanup(self), "mfc:cleanup")

    def start(self) -> None:
        self.shutdown = False


# ───────── 블록·예산·조립 도움 ─────────
def blocks(chamber: str) -> List[DeviceBlock]:
    f = []
    for gas in GAS_MAP.values():
        g = gas.lower()
        f.append(FieldDef(f"{g}_sccm", FType.FLOAT, "sccm", f"{gas} 유량", fmt=".2f", source="R60 ÷ MFC_SCALE_FACTORS",
                          own_quality=True, summary="mean"))
    f.append(FieldDef("pressure_mtorr", FType.FLOAT, "mTorr", "챔버 압력(MFC)", fmt=".3f", source="R5 ÷ MFC_PRESSURE_SCALE",
                      own_quality=True, summary="mean"))
    for gas in GAS_MAP.values():
        g = gas.lower()
        f.append(FieldDef(f"{g}_set_sccm", FType.FLOAT, "sccm", f"{gas} 설정 유량", fmt=".2f", source="FLOW_SET 확정값",
                          detail=True))
        f.append(FieldDef(f"{g}_on", FType.BOOL, "", f"{gas} ON", source="FLOW_ON/OFF", summary="any", detail=True))
    f.append(FieldDef("polling", FType.BOOL, "", "v1 폴링(공정 중)", source="set_process_status", summary="any", detail=True))
    return [DeviceBlock(f"{chamber}.mfc", f"{chamber.upper()} MFC", tuple(f), desc="MOXA NPort TCP ASCII")]


# 1초 예산 — v1 근거 간격(MFC_GAP_MS 1000: 응답 뒤 1 s)으로 계산. 실측은 bench.
BUDGET = dict(
    queries_per_tick=1, bytes_tx=4, bytes_rx=26, min_gap_ms=1000, fits_1s=False, alternate="R60/R5 격초(자기 품질·나이)",
    note="R60(4 B → 'Q0+aa.a+bb.b+cc.c+dd.d\\r\\n' 약 26 B)과 R5(3 B → 'P+x.xx\\r\\n' 약 9 B)를 번갈아 한 tick 에 하나. "
         "명령 간격이 응답 뒤 1 s 라 한 주기 = 1 s + 응답 시간 — 응답 50 ms 면 약 10 tick 에 한 번은 간격을 못 맞춰 그 tick 은 안 읽음(stale). "
         "제어 동작 중·직후 1.5 s 는 읽지 않음(그동안 제어가 읽은 값이 1 s 안이면 씀). 마감은 MFC_TIMEOUT + 대기 0.5 s",
)


def source_deadline_ms(cfg: Dict[str, Any]) -> int:
    """스냅샷 Source.deadline_ms — 간격 대기(TICK_GAP_WAIT_S) + v1 응답 한도 MFC_TIMEOUT + 여유."""
    return int(TICK_GAP_WAIT_S * 1000 + float(cfg.get("MFC_TIMEOUT", 2000)) + 100)


def endpoint(cfg: Dict[str, Any], chamber: str) -> tuple:
    host = cfg.get(f"{chamber.upper()}_MFC_TCP_HOST", cfg.get("MFC_TCP_HOST", "192.168.1.50"))
    return str(host), int(cfg[f"{chamber.upper()}_MFC_TCP_PORT"])


def tcp_settings(cfg: Dict[str, Any], chamber: str) -> TcpSettings:
    host, port = endpoint(cfg, chamber)
    # 쓰기 한도 = MFC_DRAIN_TIMEOUT_S(v1 device/mfc.py:1385-1386 — writer.drain 한도, 넘으면 끊고 재연결)
    return TcpSettings(host, port, connect_timeout_s=max(0.5, float(cfg.get("MFC_CONNECT_TIMEOUT_S", 3.0))),
                       write_timeout_s=float(cfg.get("MFC_DRAIN_TIMEOUT_S", 2.0)))


def make_worker(clock, transport, cfg: Dict[str, Any], chamber: str, writes_allowed: bool, on_event=None) -> Worker:
    """v1 재연결 규칙의 worker — 무응답 1회에 끊기(D182), 백오프 1 s ×2 최대 20 s(장기 두절 뒤에도 20 s)."""
    mx = float(cfg.get("MFC_RECONNECT_BACKOFF_MAX_MS", 20000)) / 1000.0
    return Worker(f"{chamber}.mfc", transport, writes_allowed=writes_allowed, fail_streak=1,
                  reconnect_start_s=float(cfg.get("MFC_RECONNECT_BACKOFF_START_MS", 1000)) / 1000.0,
                  reconnect_max_s=mx, long_outage_s=600.0, probe_s=mx, log_every_s=600.0, on_event=on_event,
                  connect_init_timeout_s=5.0)

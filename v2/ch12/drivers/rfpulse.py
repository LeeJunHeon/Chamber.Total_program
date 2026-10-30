"""챔버12 RF Pulse 드라이버 — AE CESAR(AE Bus RS-232, MOXA NPort TCP). v1 device/rf_pulse.py 전부를 다시 쓴 것(import 안 함).

연결: CH1 192.168.1.50:4007(DC Pulse 와 같은 포트), CH2 4005 — 공정이 RF Pulse 를 고를 때만 pulse_port 가 Worker 를 붙인다.
  SO_KEEPALIVE 는 설정(RFP_TCP_KEEPALIVE)과 무관하게 늘 1(v1 rf_pulse.py:1057-1063 — pulse_port.tcp_transport_factory).
  무응답으로는 끊지 않는다(v1 은 쓰기 오류·EOF 때만 끊음 — fail_streak 아주 큼). 재연결 2 s 부터 ×2, 최대 30 s.
명령 하나(v1 _cmd_worker_loop 1115-1306): 간격(CMD_GAP_MS 1500 — 앞 명령이 끝난 뒤 쉼, 보낸 시각 기준 간격 둘 다) → 보냄 →
  같은 주소·명령 프레임(ACK 06 은 건너뜀, NAK 15 면 실패, 체크섬 틀린 프레임은 버림). 설정 명령 성공 = CSR data[0] == 0.
  실패면 재시도(기본 3): 쉼 max(150 ms, 간격), CSR 5 면 max(간격 × 1.5, 1200 ms). CSR 1(HOST 아님) → 앞에 14(02) HOST 를 끼워 넣고
  다시, START 단계([START…])의 CSR 2(RF ON 중) → 앞에 RF_OFF 를 끼워 넣고 다시(이것도 재시도 하나를 쓴다).
  응답 한도: 설정 ACK_TIMEOUT_MS 2000(RF ON/OFF 는 2500), 질의 QUERY_TIMEOUT_MS 4500, 폴링 질의 POLL_QUERY_TIMEOUT_MS 9000.
  호출자 한도 = 응답 한도 + 2 s(v1 _exec_and_csr·_query_and_data) — 그 안에서만 재시도한다(아래 '다른 점').
정지 뒤(_stop_requested)에는 RF_OFF·SET_ACTIVE_CTRL 외 설정 명령을 보내지 않는다(STOP_GUARD, v1 1908-1915).
1초 읽기: v1 폴링 순서·간격 그대로(RF ON + 0.8 s 뒤부터): 162 STATUS → 165 FWD → 166 REF(질의 사이 1.5 s), REF 뒤 POLL_INTERVAL_MS
  (5 s) 쉬고 다시. tick 마다 '지금 마감 안에 보낼 수 있는' 다음 질의 하나만 — 나머지 tick 은 장비 값 열 stale(자기 품질).
  REF 를 읽을 때마다 FORP/REFP 감시(v1 1511-1589): FORP |fwd − 설정| ≥ 설정 × 5 % 를 3회 연속 → command_failed FORP_MONITOR 후
  카운터 0(그래서 계속 이탈이면 3회마다 다시 — v1 동작), REFP ≥ 20 W 3회 → REFP_MONITOR, REFP_WARN(채널 값, CH2 5 W)은 문구만.
다른 점(v1 과): 호출자 한도가 지난 뒤 남은 재시도는 보내지 않는다(v1 은 큐에 남은 재시도를 늦게 보낼 수 있었다).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

from v2.ch12.drivers.pulse_port import IO, OpRunner, Sleep, chan_get, endpoint, norm_endpoint
from v2.ch12.drivers.rfpulse_poll import CSR_CODES, RfpStopPollMixin, parse_status, status_str
from v2.common import aebus
from v2.common.snapshot import DeviceBlock, FieldDef, FType
from v2.common.transport import ReadTimeout

KIND = "rfpulse"
RF_OFF, RF_ON, SET_MODE, SET_SETPOINT, SET_ACTIVE_CTRL, SET_PULSING, SET_FREQ, SET_DUTY = 1, 2, 3, 8, 14, 27, 93, 96
Q_ACTIVE_CTRL, Q_STATUS, Q_FWD, Q_REF, Q_FREQ, Q_DUTY = 155, 162, 165, 166, 193, 196
MODE_FWD = 6
PULSING_MODES = (0, 1, 2, 3, 4, 5)
FREQ_MIN_HZ, MIN_ON_OFF_US = 1, 16.0
CMD_NAMES = {1: "RF_OFF", 2: "RF_ON", 3: "SET_CTRL_MODE", 8: "SET_SETPOINT", 14: "SET_ACTIVE_CTRL", 27: "SET_PULSING",
             93: "SET_PULSE_FREQ", 96: "SET_PULSE_DUTY", 155: "READ_ACTIVE_CTRL", 162: "REPORT_STATUS", 165: "REPORT_FORWARD",
             166: "REPORT_REFLECTED", 193: "REPORT_PULSE_FREQ", 196: "REPORT_PULSE_DUTY"}
NEVER_DROP = 10 ** 9            # fail_streak — 무응답으로 끊지 않음(v1)


class RfpNak(Exception):
    pass


@dataclass(frozen=True)
class RfpEvent:
    kind: str                   # status / rf_status / power / command_failed / target_reached / power_off_finished
    detail: str = ""
    reason: Optional[str] = None
    cmd: Optional[str] = None
    code: Optional[str] = None  # v1 RF Pulse 드라이버는 오류 코드를 싣지 않았다(None)
    data: Optional[dict] = None


def duty_limits_pct(f_khz: float) -> Tuple[float, float]:
    """최소 On/Off 16 us 규칙의 듀티 범위(v1 rfp_duty_limits_pct)."""
    lo = round(MIN_ON_OFF_US * float(f_khz) / 10.0, 2)
    return lo, round(100.0 - lo, 2)


def cmd_label(cmd: int) -> str:
    n = CMD_NAMES.get(cmd)
    return f"{n}(0x{cmd:02X})" if n else f"0x{cmd:02X}"


def _f(name, t, unit, label, fmt="", src="", own=False, summary="last", detail=False, desc=""):
    return FieldDef(name, t, unit, label, fmt, src, own, summary, detail, desc)


def blocks(chamber: str = "ch1") -> list:
    F, I, B, S = FType.FLOAT, FType.INT, FType.BOOL, FType.STR
    return [DeviceBlock(f"{chamber}.rfpulse", f"{chamber.upper()} RF Pulse", (
        _f("fwd_w", F, "W", "순방향", ".0f", "165", own=True, summary="mean"),
        _f("ref_w", F, "W", "반사", ".0f", "166", own=True, summary="max"),
        _f("out_on", B, "", "출력", src="162 byte1 bit5", own=True, summary="any"),
        _f("itl_open", B, "", "인터록 열림", src="162 byte2 bit7", own=True, summary="any"),
        _f("status_raw", S, "", "상태 코드(hex)", src="162", own=True, detail=True, desc="상태 4바이트"),
        _f("set_w", F, "W", "목표", ".0f", "driver(8 성공 값)"),
        _f("freq_hz", I, "Hz", "펄스 주파수(되읽기)", "d", "193(시작 때)"),
        _f("duty_pct", I, "%", "듀티(되읽기)", "d", "196(시작 때)"),
        _f("polling", B, "", "읽는 중", src="driver", detail=True, desc="v1 폴링(RF ON + 0.8 s 뒤 ~ 정지)"),
    ), desc="AE CESAR — AE Bus, MOXA NPort TCP")]


BUDGET = dict(queries_per_tick=1, bytes_tx=3, bytes_rx=8, min_gap_ms=1500, fits_1s=False,
              alternate="v1 폴링 그대로: 162 → (1.5 s) 165 → (1.5 s) 166 → 5 s 쉼, 한 바퀴 ≈ 8 s. tick 마다 마감 안에 보낼 수 있는 "
                        "다음 질의 하나(값마다 자기 품질·나이)",
              note="질의 3 B(HDR CMD CS), 응답 ACK 1 B + 프레임 4~7 B. CMD_GAP_MS 1500 이라 1초에 질의 하나도 못 넣는 tick 이 있다. "
                   "RF ON 뒤에만 읽음(v1). 실측은 bench")


class RfPulseDriver(RfpStopPollMixin):
    KIND = KIND

    def __init__(self, clock, worker, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[RfpEvent], None]] = None, chamber: str = "ch1") -> None:
        self.clock, self.cfg, self.writes_allowed, self.on_event, self.chamber = clock, cfg, writes_allowed, on_event, chamber
        self.block = f"{chamber}.rfpulse"
        self.worker = None
        self._ready_at = -1e9                     # 다음 명령을 보낼 수 있는 가장 이른 시각
        self._last_send = -1e9
        self._op: Optional[OpRunner] = None
        self._off_op: Optional[OpRunner] = None
        self._off_waiters: list = []
        self._stop_requested = False
        self._output_maybe_on = False
        self.output_off_unconfirmed = False
        self.cleanup_off_unconfirmed = False
        self.polling = False
        self._poll_step = 0
        self._cycle_next = -1e9
        self._target_w = 0.0
        self._forp_n = self._refp_n = 0
        self._refp_warn_active = False
        self.last_fwd: Optional[float] = None
        self.last_ref: Optional[float] = None
        self.last_status: Optional[Dict[str, Any]] = None
        self.freq_hz: Optional[int] = None
        self.duty_pct: Optional[int] = None
        if worker is not None:
            self.bind(worker)

    # ───────── 설정 ─────────
    def _c(self, key: str, default=None):
        return chan_get(self.cfg, self.chamber, key, default)

    def _ms(self, key: str, default: int) -> float:
        try:
            return int(float(self._c(key, default))) / 1000.0
        except Exception:
            return default / 1000.0

    @property
    def addr(self) -> int:
        return int(self._c("RFPULSE_ADDR", 1))

    def _gap_s(self) -> float:
        return self._ms("CMD_GAP_MS", 1500)

    def endpoint_key(self) -> str:
        return norm_endpoint(*endpoint(self.cfg, self.chamber, KIND))

    def worker_params(self) -> dict:
        start = self._ms("RFPULSE_RECONNECT_BACKOFF_START_MS", 2000)
        mx = max(start, self._ms("RFPULSE_RECONNECT_BACKOFF_MAX_MS", 30000))
        return dict(fail_streak=NEVER_DROP, reconnect_start_s=start, reconnect_max_s=mx, probe_s=mx, connect_init_timeout_s=1.0)

    def bind(self, worker) -> None:
        self.worker = worker
        worker.on_connect = self._on_connect

    def unbind(self) -> None:
        for op in (self._op, self._off_op):
            if op is not None:
                op.abort("LINK_DOWN")
        self.worker, self.polling = None, False

    def _on_connect(self, session) -> None:
        self._emit("status", f"{':'.join(map(str, endpoint(self.cfg, self.chamber, KIND)))} 연결 성공 (TCP)")

    def _emit(self, kind: str, detail: str = "", reason: Optional[str] = None, cmd: Optional[str] = None,
              data: Optional[dict] = None) -> None:
        if self.on_event:
            self.on_event(RfpEvent(kind, detail, reason, cmd, None, data))

    def _failed(self, cmd: str, why: str) -> None:
        self._emit("command_failed", why, reason="RFP_OFF_UNCONFIRMED" if cmd == "RF_OFF" else "RFP_COMMAND_FAILED", cmd=cmd)

    # ───────── 한 번 주고받기(worker 스레드) ─────────
    def _transact(self, s, cmd: int, data: bytes, timeout_s: float, until: float, tag: str, is_exec: bool) -> bytes:
        """보내고 같은 주소·명령 프레임의 data 를 돌려준다. NAK → RfpNak, 시간 초과 → ReadTimeout."""
        if self._ready_at >= until:
            raise ReadTimeout()                              # 간격을 기다리면 호출자 한도를 넘는다 — 보내지 않음
        if self._ready_at > self.clock.monotonic():
            self.clock.sleep_until(self._ready_at)
        now = self.clock.monotonic()
        if now >= until:
            raise ReadTimeout()
        pkt = aebus.build(self.addr, cmd, data)
        if is_exec:
            s.transport.reset_input()                        # v1: exec 직전 토큰 비우기
        raw = self._raw_on(tag)
        if raw:
            self._emit("status", f"[RFP][RAW][TX] addr={self.addr} cmd={cmd_label(cmd)} "
                                 f"data={' '.join(f'{x:02X}' for x in data)} raw={' '.join(f'{x:02X}' for x in pkt)} tag={tag}")
        if cmd == RF_ON:
            self._output_maybe_on = True
        self._last_send = now
        s.write(pkt)
        dl = min(now + timeout_s, until)
        while True:
            b = s.read_until(size=1, deadline=dl)
            if b[0] == aebus.ACK:
                continue
            if b[0] == aebus.NAK:
                self._emit("status", "[RFP][RAW][RX] NAK(0x15)")
                raise RfpNak()
            head = b + s.read_until(size=1, deadline=dl)
            n = head[0] & 0x07
            if n == 7:
                ln = s.read_until(size=1, deadline=dl)
                frame = head + ln + s.read_until(size=ln[0] + 1, deadline=dl)
            else:
                frame = head + s.read_until(size=n + 1, deadline=dl)
            if aebus.xor(frame) != 0:
                self._emit("status", f"[RFP][RAW][RX] FRAME(cs_bad) raw={' '.join(f'{x:02X}' for x in frame)}")
                continue
            a_addr, a_cmd, payload = aebus.parse(frame)
            if raw:
                self._emit("status", f"[RFP][RAW][RX] FRAME addr={a_addr} cmd={cmd_label(a_cmd)} len={len(payload)} "
                                     f"raw={' '.join(f'{x:02X}' for x in frame)}")
            if a_addr == self.addr and a_cmd == cmd:
                return payload

    def _raw_on(self, tag: str) -> bool:
        """RFPULSE_RAW_LOG 가 false 면 '[POLL' 태그(정주기 폴링)만 원시 프레임 문구를 생략(v1 1099-1112)."""
        return bool(self._c("RFPULSE_RAW_LOG", False)) or not str(tag or "").startswith("[POLL")

    # ───────── 명령 단계(generator) ─────────
    def _io(self, cmd, data, timeout_s, until, tag, is_exec, kind):
        def fn(s):
            return self._transact(s, cmd, data, timeout_s, until, tag, is_exec)
        lim = max(0.05, until - self.clock.monotonic()) + 0.2
        return IO(fn, lim, kind=kind, name=tag or cmd_label(cmd))

    def _g_exec(self, cmd: int, data: bytes, tag: str = "", timeout_ms: Optional[int] = None, retries: int = 3,
                kind: str = "write", until: Optional[float] = None, csr2_fix: Optional[bool] = None):
        """v1 _enqueue_exec + 워커 재시도·CSR 자동 복구. (성공, CSR 바이트|None)."""
        eff = (timeout_ms if timeout_ms else int(self._c("ACK_TIMEOUT_MS", 2000))) / 1000.0
        if until is None:
            until = self.clock.monotonic() + eff + 2.0
        gap = self._gap_s()
        left = retries
        while True:
            if self._stop_requested and cmd not in (RF_OFF, SET_ACTIVE_CTRL):
                self._emit("status", f"[STOP_GUARD] {cmd_label(cmd)} 전송 차단")
                return False, None
            r = yield self._io(cmd, data, eff, until, tag, True, kind)
            t_send = self._last_send
            csr_b = r.value if r.ok else None
            csr = csr_b[0] if csr_b else None
            if csr == 0:
                if cmd == RF_OFF:
                    self._output_maybe_on = False
                    self.output_off_unconfirmed = False
                self._ready_at = self.clock.monotonic() + gap
                return True, csr_b
            if csr is not None:
                self._emit("status", f"CSR {csr} ({CSR_CODES.get(csr, 'Unknown')}) for {cmd_label(cmd)}")
                fix = None
                if csr == 1 and cmd != SET_ACTIVE_CTRL and left > 0:
                    fix = (SET_ACTIVE_CTRL, b"\x02", "[AUTO HOST]")
                elif csr == 2 and (tag.startswith("[START") if csr2_fix is None else csr2_fix) and cmd != RF_OFF and left > 0:
                    fix = (RF_OFF, b"", "[AUTO RF_OFF]")
                if fix is not None:
                    left -= 1
                    self._ready_at = t_send + max(0.2, gap)
                    yield from self._g_exec(fix[0], fix[1], fix[2], int(self._c("ACK_TIMEOUT_MS", 2000)), 1, kind, until,
                                            csr2_fix=False)
                    continue
            back = max(gap * 1.5, 1.2) if csr == 5 else max(0.15, gap)
            if left > 0 and self.clock.monotonic() + back < until:
                left -= 1
                self._ready_at = self.clock.monotonic() + back
                continue
            self._ready_at = self.clock.monotonic() + gap
            return False, csr_b

    def _g_query(self, cmd: int, tag: str = "", timeout_ms: Optional[int] = None, retries: int = 3, kind: str = "write",
                 wait_s: Optional[float] = None):
        eff = (timeout_ms if timeout_ms else int(self._c("QUERY_TIMEOUT_MS", 4500))) / 1000.0
        until = self.clock.monotonic() + (eff + 2.0 if wait_s is None else wait_s)
        gap = self._gap_s()
        left = retries
        while True:
            r = yield self._io(cmd, b"", eff, until, tag, False, kind)
            if r.ok:
                self._ready_at = self.clock.monotonic() + gap
                return r.value
            if left > 0 and self.clock.monotonic() + max(0.15, gap) < until:
                left -= 1
                self._ready_at = self.clock.monotonic() + max(0.15, gap)
                continue
            self._ready_at = self.clock.monotonic() + gap
            return None

    # ───────── 동작 실행 ─────────
    def _run(self, name: str, gen, on_done, off: bool = False) -> None:
        why = ("BENCH_READ_ONLY" if not self.writes_allowed else "LINK_DOWN" if self.worker is None
               else "STATE_NOT_ALLOWED" if (not off and self._op is not None) else None)
        if why:
            gen.close()
            for w in (self._off_waiters if off else []):
                w(why, False)
            if off:
                self._off_waiters = []
            if on_done:
                on_done(why, False)
            return

        def done(reason, value):
            ok = bool(value) and reason is None
            r = reason or (None if ok else ("RFP_OFF_UNCONFIRMED" if off else "RFP_START_FAILED"))
            if off:
                self._off_op = None
                waiters, self._off_waiters = self._off_waiters, []
                for w in waiters:
                    w(r, ok)
            elif self._op is runner:
                self._op = None
            self.clock.notify()
            if on_done:
                on_done(r, ok)
        runner = OpRunner(self.clock, self.worker, f"rfp:{name}", gen, done, "safety" if off else "write")
        if off:
            self._off_op = runner
        else:
            self._op = runner
        runner.start()

    # ───────── 시작(v1 start_pulse_process 419-633) ─────────
    def start_pulse_process(self, target_w: float, freq_hz: Optional[int] = None, duty_percent: Optional[int] = None,
                            on_done: Optional[Callable[[Optional[str], bool], None]] = None) -> None:
        """HOST(필요할 때만) → 사전 RF OFF → 0.2 s → MODE FWD → SETP → [FREQ] → [DUTY] → [193·196 되읽기(경고만)] → PULSING
        → RF ON → 0.8 s → 폴링. on_done(사유|None, 성공). 실패 문구는 command_failed(START_SEQUENCE)."""
        self._stop_requested = False
        self.polling = False
        self._target_w = float(target_w or 0.0)
        self._forp_n = self._refp_n = 0
        self._refp_warn_active = False
        self._run("start", self._g_start(target_w, freq_hz, duty_percent), on_done)

    def _g_start(self, target_w, freq_hz, duty):
        def stopped():
            if self._stop_requested:
                self._emit("status", "STOP 요청 → RF Pulse 시작 시퀀스 중단")
                return True
            return False

        def fail(why):
            if self._stop_requested:
                self._emit("status", "STOP 요청 → RF Pulse 시작 시퀀스 중단")
            else:
                self._failed("START_SEQUENCE", why)
            return False

        onoff_ms = max(int(self._c("ACK_TIMEOUT_MS", 2000)), 2500)
        if stopped():
            return False
        md = yield from self._g_query(Q_ACTIVE_CTRL, "[READ CTRL MODE]")
        cur = md[0] if md else None
        if cur is None:
            return fail("HOST 모드 확인 실패 (READ_ACTIVE_CTRL 응답 없음)")
        if stopped():
            return False
        if cur != 2:
            ok, cb = yield from self._g_exec(SET_ACTIVE_CTRL, b"\x02", "[START HOST]")
            if not ok:
                return fail(f"HOST 전환 실패 (CSR={cb[0] if cb else None}, 이전 모드={cur})")
            self._emit("status", f"HOST 모드 전환 완료 (이전 모드={cur})")
        else:
            self._emit("status", "이미 HOST 모드 → SET_ACTIVE_CTRL 생략")
        if stopped():
            return False
        ok, _ = yield from self._g_exec(RF_OFF, b"", "[START PRE RF OFF]", onoff_ms)
        if not ok:
            return fail("RF OFF(사전) 실패")
        yield Sleep(0.2)
        if stopped():
            return False
        ok, _ = yield from self._g_exec(SET_MODE, bytes([MODE_FWD]), "[START MODE FWD]")
        if not ok:
            return fail("MODE=FWD 실패")
        if stopped():
            return False
        sp = int(round(float(target_w)))
        ok, _ = yield from self._g_exec(SET_SETPOINT, bytes([sp & 0xFF, (sp >> 8) & 0xFF]), f"[START SETP {sp}W]")
        if not ok:
            return fail("SETP 실패")
        if freq_hz is not None and duty is not None:
            fk = float(freq_hz) / 1000.0
            lo, hi = duty_limits_pct(fk)
            if not (lo <= float(duty) <= hi):
                self._emit("status", f"[WARN] duty {float(duty):g}% @{fk:g}kHz — CESAR 매뉴얼(16us 규칙) 기준 범위 {lo:g}~{hi:g}% 밖 "
                                     "— 장비가 CSR 51로 거부할 수 있음")

        def csr_txt(c):
            return f" (CSR {c}: {CSR_CODES.get(c, 'Unknown')})" if c is not None else ""
        fd = (bytes([int(freq_hz) & 0xFF, (int(freq_hz) >> 8) & 0xFF, (int(freq_hz) >> 16) & 0xFF])
              if freq_hz is not None else b"")
        dd = bytes([int(duty) & 0xFF, (int(duty) >> 8) & 0xFF]) if duty is not None else b""
        if stopped():
            return False
        retry_freq = False
        if freq_hz is not None:
            ok, cb = yield from self._g_exec(SET_FREQ, fd, "[START FREQ]")
            c = cb[0] if cb else None
            if not ok:
                if c in (50, 51) and duty is not None:
                    self._emit("status", f"[RETRY] FREQ 거부 (CSR {c}: {CSR_CODES.get(c, 'Unknown')}) → DUTY 먼저 적용 후 FREQ 재시도")
                    retry_freq = True
                else:
                    return fail("PULSE FREQ 실패" + csr_txt(c))
        if stopped():
            return False
        if duty is not None:
            ok, cb = yield from self._g_exec(SET_DUTY, dd, "[START DUTY]")
            if not ok:
                return fail("PULSE DUTY 실패" + csr_txt(cb[0] if cb else None))
        if retry_freq:
            ok, cb = yield from self._g_exec(SET_FREQ, fd, "[START FREQ]")
            if not ok:
                return fail("PULSE FREQ 실패(DUTY 선적용 후 재시도)" + csr_txt(cb[0] if cb else None))
        if stopped():
            return False
        if freq_hz is not None or duty is not None:
            pr = yield from self._g_read_params()
            if pr:
                got_hz = int(round(float(pr["freq_khz"]) * 1000.0))
                self._emit("status", f"[VERIFY] RF Pulse f={got_hz} Hz, duty={pr['duty_pct']}%")
                if freq_hz is not None and got_hz != int(freq_hz):
                    self._emit("status", f"[WARN] RF Pulse 주파수 불일치 (요청 {int(freq_hz)} Hz ≠ 장비 {got_hz} Hz)")
                if duty is not None and int(pr["duty_pct"]) != int(duty):
                    self._emit("status", f"[WARN] RF Pulse 듀티 불일치 (요청 {int(duty)}% ≠ 장비 {pr['duty_pct']}%)")
            else:
                self._emit("status", "[VERIFY] RF Pulse 설정 read-back 실패(통신) → 검증 생략")
        if stopped():
            return False
        mode = int(self._c("RFPULSE_PULSE_MODE", 1))
        if mode not in PULSING_MODES:
            return fail(f"PULSING 모드 범위 오류: {mode} (허용: {list(PULSING_MODES)})")
        ok, _ = yield from self._g_exec(SET_PULSING, bytes([mode]), f"[START PULSING {mode}]")
        if not ok:
            return fail("PULSING 설정 실패")
        if stopped():
            return False
        ok, _ = yield from self._g_exec(RF_ON, b"", "[START RF ON]", onoff_ms)
        if not ok:
            return fail("RF ON 실패")
        yield Sleep(self._ms("POLL_START_DELAY_AFTER_RF_ON_MS", 800))
        if stopped():
            return False
        self._start_polling()
        self._emit("target_reached", "OK")
        return True

    def _g_read_params(self):
        f = yield from self._g_query(Q_FREQ, "[READ PULSE FREQ]")
        d = yield from self._g_query(Q_DUTY, "[READ PULSE DUTY]")
        if f is None or len(f) < 3 or d is None or len(d) < 2:
            return None
        hz, duty = aebus.u24(f), aebus.u16(d)
        self.freq_hz, self.duty_pct = hz, duty
        off = round(1_000_000.0 / hz * (1.0 - duty / 100.0), 2) if hz > 0 else None
        return {"freq_khz": round(hz / 1000.0, 3), "duty_pct": duty, "off_time_us": off}

    def read_actual_pulse_params(self, on_done: Callable[[Optional[str], Optional[dict]], None]) -> None:
        box: dict = {}

        def gen():
            box["v"] = yield from self._g_read_params()
            return True
        self._run("read_params", gen(), lambda r, ok: on_done(r, box.get("v")))

    def set_reference_power(self, target_w: float, pause_polling: bool = True,
                            on_done: Optional[Callable[[Optional[str], bool], None]] = None) -> None:
        """RF ON 중 셋포인트(8)만 바꾸기(v1 671-727). START 태그가 아니라 CSR 2 에 RF_OFF 를 끼워 넣지 않는다."""
        def gen():
            if self._stop_requested:
                self._emit("status", "STOP 요청 → SETP 변경 생략")
                return False
            try:
                sp = int(round(float(target_w)))
            except Exception:
                self._failed("SETP_CHANGE", f"invalid power: {target_w!r}")
                return False
            if not (0 <= sp <= 65535):
                self._failed("SETP_CHANGE", f"out of range: {sp} (0..65535)")
                return False
            was = self.polling
            if pause_polling and was:
                self.polling = False
            ok, cb = yield from self._g_exec(SET_SETPOINT, bytes([sp & 0xFF, (sp >> 8) & 0xFF]), f"[RUN SETP {sp}W]")
            if ok:
                self._target_w = float(sp)
                self._forp_n = self._refp_n = 0
                self._emit("status", f"SETP 변경 OK: {sp}W")
            elif self._stop_requested:
                self._emit("status", "STOP 요청 → SETP 변경 실패 통지 생략")
            else:
                c = cb[0] if cb else None
                self._failed("SETP_CHANGE", f"CSR {c} ({CSR_CODES.get(c, 'Unknown')})" if c is not None else "no reply / timeout")
            if pause_polling and was:
                self.polling = True
            return ok
        self._run("set_power", gen(), on_done)


__all__ = ["RfPulseDriver", "RfpEvent", "blocks", "BUDGET", "KIND", "CSR_CODES", "parse_status", "status_str",
           "duty_limits_pct", "cmd_label"]

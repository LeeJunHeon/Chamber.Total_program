"""챔버12 DC Pulse 드라이버 — EnerPulse EP5(RS-232, MOXA NPort TCP). v1 device/dc_pulse.py 전부를 다시 쓴 것(import 안 함).

연결: CH1 192.168.1.50:4007(RF Pulse 와 같은 포트), CH2 4005(MOXA Port 5 를 RF Pulse 와 바꿔 씀) — 공정이 DC Pulse 를 고를
  때만 pulse_port 가 Worker 를 붙인다(쓰지 않으면 1초 행 off). 무응답 1회면 세션 재시작(v1 '[TIMEOUT] … → 세션 재시작' —
  fail_streak 1), 재연결 1 s 부터 ×2, 최대 10 s(DCP_RECONNECT_BACKOFF_*). 연결 직후 DCP_POST_OPEN_QUIET_S(0.8 s) 조용히,
  연결 뒤 2 s 안의 명령은 응답 한도에 DCP_FIRST_CMD_EXTRA_TIMEOUT_MS(2 s) 더함. 응답 한도 = DCP_TIMEOUT_MS(2.5 s) + 2 s.
  보내기 간격 DCP_GAP_MS 150(보낸 시각 기준 — v1 _last_send_mono). SO_KEEPALIVE 는 DCP_TCP_KEEPALIVE(false) 그대로.
명령 바이트·순서·되읽기·재시도는 dcpulse_ops(동작)·dcpulse_proto(프레임)·dcpulse_monitor(출력 중 감시·아크) 참고.

1초 읽기(source_read): v1 폴링 조건(연결·출력 ON·폴링 켜짐 — 시작 순서·OFF 중에는 폴링 off)일 때만 장비를 읽는다.
  매 tick PIV(0x9A)·SAT(0x96)·ANT(0x99), 점화 창 밖에서는 AE(0xAE)/AF(0xAF) 를 번갈아(판정 tick 에는 둘 다 — 시간이 되면).
  판정(과전류·저전류·이탈·아크)은 v1 주기(점화 창 1 s, 그 밖 5 s)의 tick 에서만 — dcpulse_monitor. 판정 tick 의 PIV 가
  실패하면 v1 처럼 0.3 s 뒤 1회 다시. AUTO_STOP 이면 0x9E 를 읽어 사유에 붙이고 command_failed(AUTO_STOP) → 확인형 OFF.
  읽지 않는 tick 은 장비 값 열이 stale(자기 품질), 드라이버 값(set_w·out_on·polling·되읽은 주파수/Off)은 그대로.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Dict, Optional

from v2.ch12.drivers import dcpulse_proto as P
from v2.ch12.drivers.dcpulse_monitor import DcpMonitorMixin
from v2.ch12.drivers.dcpulse_ops import DcpOpsMixin
from v2.ch12.drivers.pulse_port import IO, OpRunner, WaitLink, chan_get, endpoint, norm_endpoint
from v2.common.snapshot import DeviceBlock, FieldDef, FType
from v2.common.transport import ReadTimeout

KIND = "dcpulse"
JUDGE_SLACK_S = 0.05            # 판정 주기 비교 여유(tick 지터)
PIV_RETRY_S = 0.3               # v1 폴링: PIV 실패 → 0.3 s 뒤 1회


@dataclass(frozen=True)
class DcpEvent:
    kind: str                   # status / telemetry / command_confirmed / command_failed / arc_threshold_reached
    detail: str = ""
    reason: Optional[str] = None
    cmd: Optional[str] = None   # v1 DCPEvent.cmd(라벨)
    code: Optional[str] = None  # v1 오류 코드(E###) — v1 DC Pulse 드라이버는 코드를 싣지 않았다(None)
    data: Optional[dict] = None


def _f(name, t, unit, label, fmt="", src="", own=False, summary="last", detail=False, desc=""):
    return FieldDef(name, t, unit, label, fmt, src, own, summary, detail, desc)


def blocks(chamber: str = "ch1") -> list:
    F, I, B = FType.FLOAT, FType.INT, FType.BOOL
    return [DeviceBlock(f"{chamber}.dcpulse", f"{chamber.upper()} DC Pulse", (
        _f("p_w", F, "W", "출력 전력", ".1f", "0x9A P×DCP_P_MEAS_W_PER_LSB", own=True, summary="mean"),
        _f("v_v", F, "V", "출력 전압", ".0f", "0x9A V×DCP_V_MEAS_V_PER_LSB", own=True, summary="mean"),
        _f("i_a", F, "A", "출력 전류", ".3f", "0x9A I×DCP_I_MEAS_A_PER_LSB", own=True, summary="max"),
        _f("arc_soft", I, "", "SAT 누적(장비)", "d", "0x96", own=True, detail=True),
        _f("arc_hard", I, "", "ANT 누적(장비)", "d", "0x99", own=True, detail=True),
        _f("arc_soft_rate", I, "/s", "AE(장비 표시값)", "d", "0xAE", own=True, detail=True, desc="실측 0 고정 — 표시만"),
        _f("arc_hard_rate", I, "/s", "AF(장비 표시값)", "d", "0xAF", own=True, detail=True, desc="실측 1 고정 — 표시만"),
        _f("arc_run_soft", I, "", "런 Soft 아크", "d", "driver(판정 주기)"),
        _f("arc_run_hard", I, "", "런 Hard 아크", "d", "driver(판정 주기)"),
        _f("set_w", F, "W", "설정 전력", ".0f", "driver(0x83 확인값)"),
        _f("out_on", B, "", "출력 ON", src="driver(0x80 확인)", summary="any"),
        _f("polling", B, "", "읽는 중", src="driver", detail=True, desc="v1 폴링 조건(출력 ON·시작/OFF 순서 밖)"),
        _f("freq_khz", I, "kHz", "펄스 주파수(되읽기)", "d", "0xA6(시작 때)"),
        _f("off_raw", I, "", "Off Time raw(되읽기)", "d", "0xA7(시작 때)", desc="9 = DC, 10~100 = 1.0~10.0 us"),
    ), desc="EnerPulse EP5 — MOXA NPort TCP")]


BUDGET = dict(queries_per_tick=4, bytes_tx=16, bytes_rx=28, min_gap_ms=150, fits_1s=True,
              alternate="점화 창 밖: AE(0xAE)/AF(0xAF) 번갈아(판정 tick 은 둘 다), 점화 창 안: PIV·SAT·ANT 만",
              note="읽기 프레임 4 B(02 CMD 03 CHK), 응답 PIV 10 B·그 밖 6 B. 4질의 × (150 ms 간격 + 왕복) ≈ 0.65 s. "
                   "v1 폴링 조건(출력 ON)일 때만 읽음. 판정은 v1 주기(1 s/5 s). 실측(응답 지연·NAK)은 bench")


class DcPulseDriver(DcpMonitorMixin, DcpOpsMixin):
    KIND = KIND

    def __init__(self, clock, worker, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[DcpEvent], None]] = None, chamber: str = "ch1") -> None:
        self.clock, self.cfg, self.writes_allowed, self.on_event, self.chamber = clock, cfg, writes_allowed, on_event, chamber
        self.block = f"{chamber}.dcpulse"
        self.worker = None
        self.parser = P.FrameParser()
        self._tokens: Deque[bytes] = deque()
        self._last_send = -1e9
        self._connect_t: Optional[float] = None
        self._just_reopened = False
        self._op: Optional[OpRunner] = None
        self._off_op: Optional[OpRunner] = None
        self._off_waiters: list = []
        self._out_on = False
        self._output_maybe_on = False             # OUTPUT_ON 을 보내기 직전 True, OFF 확인 때 False
        self.output_off_unconfirmed = False
        self.last_failure: Optional[str] = None
        self._prepare_active = False
        self._last_ref_power_w: Optional[float] = None
        self._stop_guard = False
        self.polling = False
        self._judge_last = -1e9
        self._tick_n = 0
        self.freq_khz: Optional[int] = None
        self.off_raw: Optional[int] = None
        self._mon_init()
        if worker is not None:
            self.bind(worker)

    # ───────── 설정 ─────────
    def _c(self, key: str, default=None):
        return chan_get(self.cfg, self.chamber, key, default)

    def _gap_s(self) -> float:
        return max(0, int(self._c("DCP_GAP_MS", 150))) / 1000.0

    def _timeout_s(self) -> float:
        return max(200, int(self._c("DCP_TIMEOUT_MS", 2500))) / 1000.0

    def _extra_s(self) -> float:
        now = self.clock.monotonic()
        if self._connect_t is not None and now - self._connect_t < 2.0:
            return max(0, int(self._c("DCP_FIRST_CMD_EXTRA_TIMEOUT_MS", 2000))) / 1000.0
        return 0.0

    def _connect_timeout_s(self) -> float:
        return max(0.5, float(self._c("DCP_CONNECT_TIMEOUT_S", 3.0)))

    def _io_limit(self) -> float:
        quiet = max(0.0, float(self._c("DCP_POST_OPEN_QUIET_S", 0.8)))
        return self._gap_s() + quiet + self._timeout_s() + 2.0 + max(0, int(self._c("DCP_FIRST_CMD_EXTRA_TIMEOUT_MS", 2000))) / 1000.0 + 0.2

    def _recover_attempts(self) -> int:
        return max(1, int(self._c("DCP_RECOVER_MAX_ATTEMPTS", 5)))

    def _fault_recover(self) -> bool:
        return bool(self._c("DCP_ENABLE_FAULT_RECOVER", True))

    def _p_step(self) -> float:
        s = float(self._c("DCP_P_SET_STEP_W", 10.0))
        return s if s > 0 else 10.0

    def _max_power_w(self) -> float:
        return max(0.0, float(self._c("DCP_MAX_POWER_W", 1000.0)))

    def _scale(self, raw: dict) -> dict:
        return P.piv_scale(raw, self._c("DCP_P_MEAS_W_PER_LSB", 10.0), self._c("DCP_I_MEAS_A_PER_LSB", 0.01),
                           self._c("DCP_V_MEAS_V_PER_LSB", 1.0))

    def endpoint_key(self) -> str:
        return norm_endpoint(*endpoint(self.cfg, self.chamber, KIND))

    def worker_params(self) -> dict:
        start = max(200, int(self._c("DCP_RECONNECT_BACKOFF_START_MS", 1000))) / 1000.0
        mx = max(start, int(self._c("DCP_RECONNECT_BACKOFF_MAX_MS", 10000)) / 1000.0)
        return dict(fail_streak=1, reconnect_start_s=start, reconnect_max_s=mx, probe_s=mx, connect_init_timeout_s=1.0)

    # ───────── 이벤트 ─────────
    def _emit(self, kind: str, detail: str = "", reason: Optional[str] = None, cmd: Optional[str] = None,
              data: Optional[dict] = None) -> None:
        if self.on_event:
            self.on_event(DcpEvent(kind, detail, reason, cmd, None, data))

    def _confirmed(self, label: str) -> None:
        self._emit("command_confirmed", label, cmd=label)

    def _failed(self, label: str, why: str, phase: Optional[str] = None) -> None:
        """v1 _emit_failed — 시작 순서 중이면 phase="prepare"(OUTPUT_OFF·AUTO_STOP 은 붙이지 않음)."""
        if phase is None and self._prepare_active and not str(label).upper().startswith(("OUTPUT_OFF", "AUTO_STOP")):
            phase = "prepare"
        self.last_failure = f"{label}: {why}"
        reason = "DCP_OFF_UNCONFIRMED" if str(label).upper().startswith("OUTPUT_OFF") else "DCP_COMMAND_FAILED"
        self._emit("command_failed", why, reason=reason, cmd=label, data={"phase": phase} if phase else None)

    # ───────── 포트 붙이기(pulse_port) ─────────
    def bind(self, worker) -> None:
        self.worker = worker
        worker.on_connect = self._on_connect

    def unbind(self) -> None:
        for op in (self._op, self._off_op):
            if op is not None:
                op.abort("LINK_DOWN")
        self.worker, self.polling = None, False

    def _on_connect(self, session) -> None:
        """I/O 없음 — v1 은 연결 직후 아무것도 보내지 않는다. 조용한 구간·첫 명령 여유의 기준 시각만."""
        self._connect_t = self.clock.monotonic()
        self._just_reopened = True
        self.parser.reset()
        self._tokens.clear()
        self._judge_last = -1e9
        self._spdev_n = self._low_curr_n = 0

    # ───────── 한 번 주고받기(worker 스레드) ─────────
    def _token(self, s, deadline: float) -> bytes:
        while not self._tokens:
            b = s.read_until(size=1, deadline=deadline)
            self._tokens.extend(self.parser.feed(b))
            while self.parser.notes:
                self._emit("status", self.parser.notes.popleft())
        return self._tokens.popleft()

    def _xfer(self, s, payload: bytes, label: str, is_read: bool) -> bytes:
        """v1 _cmd_worker_loop 한 명령: 간격 → (연결 직후 조용히) → 보냄 → '자기 응답'만 인정.
        쓰기 = 1 B ACK/ERR, 읽기 = 같은 CMD 프레임 또는 ERR. 시간 초과는 ReadTimeout(→ worker 가 세션 재시작)."""
        ready = self._last_send + self._gap_s()
        if ready > self.clock.monotonic():
            self.clock.sleep_until(ready)
        if self._just_reopened and self._connect_t is not None:
            t = self._connect_t + max(0.0, float(self._c("DCP_POST_OPEN_QUIET_S", 0.8)))
            if t > self.clock.monotonic():
                self.clock.sleep_until(t)
            self._just_reopened = False
        if not is_read:
            s.transport.reset_input()                       # v1 _purge_rx_frames(쓰기 직전)
            self._tokens.clear()
        if label.split("[", 1)[0].strip() == "OUTPUT_ON":
            self._output_maybe_on = True
        now = self.clock.monotonic()
        self._last_send = now
        s.write(payload)
        dl = now + self._timeout_s() + 2.0 + self._extra_s()
        exp = payload[1]
        while True:
            tok = self._token(s, dl)
            if not is_read:
                if len(tok) == 1 and tok[0] in (P.ACK, P.ERR):
                    return tok
                continue
            if (len(tok) == 1 and tok[0] == P.ERR) or tok[0] == exp:
                return tok

    def _q(self, s, code: int) -> Optional[bytes]:
        return self._xfer(s, P.pack_read(code), P.READ_LABELS[code], True)

    # ───────── 동작 실행 ─────────
    def _run(self, name: str, gen, on_done, base_kind: str = "write", off: bool = False) -> None:
        """동작 하나를 OpRunner 로. 쓰기 동작은 한 번에 하나(진행 중이면 STATE_NOT_ALLOWED), OFF 는 따로(safety)."""
        why = ("BENCH_READ_ONLY" if not self.writes_allowed else "LINK_DOWN" if self.worker is None
               else "STATE_NOT_ALLOWED" if (not off and self._op is not None) else None)
        if why:
            gen.close()
            if name == "prepare":
                self._prepare_active = False
            if off:
                waiters, self._off_waiters = self._off_waiters, []
                for w in waiters:
                    w(why, False)
            if on_done:
                on_done(why, False)
            return

        def done(reason, value):
            ok = bool(value) and reason is None
            r = reason or (None if ok else ("DCP_OFF_UNCONFIRMED" if off else "DCP_COMMAND_FAILED"))
            if off:
                self._off_op = None
                waiters, self._off_waiters = self._off_waiters, []
                for w in waiters:
                    w(r, ok)
            elif self._op is runner:
                self._op = None
            if name == "prepare":
                self._prepare_active = False
            if on_done:
                on_done(r, ok)
        runner = OpRunner(self.clock, self.worker, f"dcp:{name}", gen, done, base_kind)
        if off:
            self._off_op = runner
        else:
            self._op = runner
        runner.start()

    def prepare_and_start(self, power_w: float, freq=None, off_time_us=None, duty=None, sync=None, master: str = "host",
                          on_done: Optional[Callable[[Optional[str], bool], None]] = None) -> None:
        """v1 prepare_and_start(power_w, freq=kHz|"keep"|None, off_time_us=us|"DC"|"keep"|None, duty=%(레거시), sync, master).
        on_done(사유|None, 성공). 실패 사유 문구는 last_failure(v1 과 같은 문구)."""
        self._prepare_active = True
        self._run("prepare", self._g_prepare(float(power_w), freq, off_time_us, duty, sync, master), on_done)

    def set_reference_power(self, value_w: float, pause_polling: bool = True,
                            on_done: Optional[Callable[[Optional[str], bool], None]] = None) -> None:
        """공정 중 0x83 만 바꾸기(v1 set_reference_power). 폴링을 잠시 멈췄다가 출력 ON·정지 아님이면 다시."""
        def gen():
            was = self.polling
            if pause_polling and was:
                self.polling = False
            try:
                return (yield from self._g_set_ref(value_w))
            finally:
                if pause_polling and was and self._out_on and not self._stop_guard:
                    self._start_polling(keep_judge=True)
        self._run("set_power", gen(), on_done)

    def read_actual_pulse_params(self, on_done: Callable[[Optional[str], Optional[dict]], None]) -> None:
        """0xA6 + 0xA7 → {"freq_khz", "off_time_us"(DC 면 None), "duty_pct"} (v1 read_actual_pulse_params)."""
        box: dict = {}

        def gen():
            f = yield from self._g_read_u16(P.R_FREQ, "write")
            o = yield from self._g_read_u16(P.R_OFF, "write")
            box["v"] = P.actual_pulse_params(f, o)
            if f is not None:
                self.freq_khz = int(f)
            if o is not None:
                self.off_raw = int(o)
            return True
        self._run("read_params", gen(), lambda r, ok: on_done(r, box.get("v")))

    def off_needs_confirm(self) -> bool:
        """켰거나(_out_on)·켜려고 보냈거나(_output_maybe_on)·이전 OFF 가 미확인이면 확인형 OFF 가 필요."""
        return bool(self._out_on or self._output_maybe_on or self.output_off_unconfirmed)

    def output_off(self, on_done: Optional[Callable[[Optional[str], bool], None]] = None) -> None:
        """확인형 OUTPUT_OFF(safety — 대기 중 쓰기·읽기를 버리고 맨 앞). 진행 중이면 그 결과를 같이 받는다."""
        self._stop_guard = True
        self.polling = False
        if self._off_op is not None:
            if on_done:
                self._off_waiters.append(on_done)
            return

        def gen():
            yield IO(lambda s: None, 0.5, kind="safety", purge=True, confirms_link=False, name="purge")
            return (yield from self._g_off_impl())
        if on_done:
            self._off_waiters.append(on_done)
        self._run("off", gen(), None, base_kind="safety", off=True)

    def output_off_unused(self, on_done: Optional[Callable[[Optional[str], bool], None]] = None) -> None:
        """켠 적 없는 출력의 정지 — OUTPUT_OFF 1회, 확인·재시도 없이 DCP_UNUSED_OFF_WAIT_S(3 s)까지만 기다린다.
        기다리는 사이 OUTPUT_ON 이 나갔으면 확인형 OFF 로 넘긴다(v1 output_off_unused)."""
        self._stop_guard = True
        self.polling = False
        if self._off_op is not None:
            if on_done:
                self._off_waiters.append(on_done)
            return

        def gen():
            t_end = self.clock.monotonic() + max(0.5, float(self._c("DCP_UNUSED_OFF_WAIT_S", 3.0)))
            yield IO(lambda s: None, 0.5, kind="safety", purge=True, confirms_link=False, name="purge")
            self._tokens.clear()
            resp = None
            if (yield WaitLink(t_end - self.clock.monotonic())) and t_end > self.clock.monotonic():
                payload = P.pack_write(P.OUTPUT, 0x0002, 2)
                r = yield IO(lambda s: self._xfer(s, payload, "OUTPUT_OFF[1회]", False),
                             max(0.05, t_end - self.clock.monotonic()), kind="safety", name="OUTPUT_OFF[1회]")
                resp = r.value if r.ok else None
            if self.off_needs_confirm():
                self._emit("status", "OUTPUT_OFF 1회 대기 중 OUTPUT_ON 이 나감 → 확인형 OUTPUT_OFF 로 전환")
                return (yield from self._g_off_impl())
            how = ("응답 없음" if not resp else "ACK" if resp == b"\x06" else "ERR(04)" if resp == b"\x04"
                   else "응답 " + resp.hex(" "))
            self._emit("status", f"이번 런에서 출력을 켠 적 없음 → OUTPUT_OFF 1회만 보냄({how}) — 확인 대기 없이 정지 단계 완료")
            self._confirmed("OUTPUT_OFF_UNUSED")
            return True
        if on_done:
            self._off_waiters.append(on_done)
        self._run("off_unused", gen(), None, base_kind="safety", off=True)

    def _start_polling(self, keep_judge: bool = False) -> None:
        if not self.polling:
            self._emit("status", f"Polling read 시작({max(0.1, float(self._c('DCP_POLL_INTERVAL_S', 5.0))):.1f}s)")
        self.polling = True
        if not keep_judge:
            self._judge_last = -1e9

    # ───────── 1초 읽기 ─────────
    def _state(self) -> Dict[str, Any]:
        b = self.block
        return {f"{b}.set_w": self._last_ref_power_w, f"{b}.out_on": self._out_on, f"{b}.polling": self.polling,
                f"{b}.freq_khz": self.freq_khz, f"{b}.off_raw": self.off_raw,
                f"{b}.arc_run_soft": self._soft_arc_total if self._arc_run_start_ts > 0 else None,
                f"{b}.arc_run_hard": self._hard_arc_total if self._arc_run_start_ts > 0 else None}

    def source_read(self, session, seq: int) -> Dict[str, Any]:
        out = self._state()
        busy = self._op is not None or self._off_op is not None
        if not (self.polling and self._out_on) or busy:
            return out
        b, now = self.block, self.clock.monotonic()
        judge = now >= self._judge_last + self._effective_poll_period(now) - JUDGE_SLACK_S
        if judge:
            self._judge_last = now
        fast = self._in_arc_fast_window(now)
        piv = self._q(session, P.R_PIV)
        raw = P.piv_raw(piv)
        if raw is None and judge and self._room(session, PIV_RETRY_S):
            self.clock.sleep_until(self.clock.monotonic() + PIV_RETRY_S)
            raw = P.piv_raw(self._q(session, P.R_PIV))
        res = None
        if raw is not None:
            res = {"raw": raw, "eng": self._scale(raw)}
            e = res["eng"]
            out.update({f"{b}.p_w": e["P_W"], f"{b}.v_v": e["V_V"], f"{b}.i_a": e["I_A"]})
        soft = P.u16_tail(self._q(session, P.R_ARC_SOFT)) if self._room(session) else None
        hard = P.u16_tail(self._q(session, P.R_ARC_HARD)) if self._room(session) else None
        out[f"{b}.arc_soft"], out[f"{b}.arc_hard"] = soft, hard
        s_rate = h_rate = None
        if not fast:
            want = (P.R_ARC_SOFT_RATE, P.R_ARC_HARD_RATE) if judge else ((P.R_ARC_SOFT_RATE,) if seq % 2 == 0 else (P.R_ARC_HARD_RATE,))
            for code in want:
                if self._room(session):
                    v = P.u16_tail(self._q(session, code))
                    if code == P.R_ARC_SOFT_RATE:
                        s_rate = out[f"{b}.arc_soft_rate"] = v
                    else:
                        h_rate = out[f"{b}.arc_hard_rate"] = v
        for k in ("p_w", "v_v", "i_a", "arc_soft", "arc_hard"):
            if out.get(f"{b}.{k}", 0) is None:
                out.pop(f"{b}.{k}")                          # 못 읽은 값은 이 tick 새 값이 아님(자기 품질 stale)
        if judge:
            stop = self._mon_cycle(res, now)
            if stop is None:
                self._mon_arc(soft, hard, s_rate, h_rate, now)
            else:
                self._auto_stop_inline(session, *stop)
            out.update(self._state())
        return out

    def _room(self, s, extra: float = 0.0) -> bool:
        """이 tick 읽기 마감 안에 질의 하나(간격 + 왕복 여유 0.08 s)가 더 들어가는가."""
        start = max(self.clock.monotonic() + extra, self._last_send + self._gap_s())
        return start + 0.08 <= s.deadline

    def _auto_stop_inline(self, s, reason: str, msg: str, p, v, i, eng) -> None:
        fault = None
        try:
            if self._room(s):
                r = self._q(s, P.R_FAULT)
                if r and len(r) >= 2 and r[0] == P.R_FAULT:
                    fault = ((r[-2] << 8) | r[-1]) if len(r) >= 3 else r[-1]
        except ReadTimeout:
            fault = None
        if fault:
            reason += f", fault=0x{fault:04X}"
        self._emit("command_failed", reason, cmd="AUTO_STOP", reason="DCP_AUTO_STOP",
                   data={"power": p, "voltage": v, "current": i, "eng": eng})
        self._emit("status", msg)
        self.output_off()


__all__ = ["DcPulseDriver", "DcpEvent", "blocks", "BUDGET", "KIND"]

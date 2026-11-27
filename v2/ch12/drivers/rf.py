"""RF 연속 전원 — PLC D/A 쓰기·A/D 읽기. v1 device/rf_power.py(RFPowerAsync) + runtime 결선.

결선
- CH2 RF 연속(mode="ch2", runtime/chamber_runtime.py:750-786): SET_2(M00052=82)·WRITE_2(D6)·READ_4/5(D8 fwd·D9 ref),
  제로 보정 CHAMBER_RF_CONT_ZEROING(False), 쓰기 역변환 W' = 1.6546·W + 2.6323(W ≤ 0.01 이면 0, 0~600) → D6 = round(W'×4).
- 플라즈마 클리닝 RF(mode="pc", runtime/plasma_cleaning_runtime.py:1015-1048): SET_1(81)·WRITE_1(D5)·READ_2/3(D2·D3, CH1 보정·
  제로 보정), 직접 모드(kick+ramp), 역변환 1.74/0.0, 쓰기마다 피드백 1회 읽어 로그.
v1 이 실제로 쓰던 값: RFPowerAsync 는 cfg 없이 생성 → config_common(= settings common) 값. blind 도달 시간 150 s(코드 60),
  정지 램프 간격 1000 ms(코드·런타임 폴백 50), REF 임계 50 W(코드 20), 일반 모드 fine_up 3.0·overshoot ramp_down 1.0 W
  (reload 가 갱신하지 않는 생성자 기본값, device/rf_power.py:113-117).
일반 모드(v1 그대로)
- start(목표): SET ON(실패면 중단) → RAMPING_UP → 초기 1.0 W 즉시 전송. 목표 > 70 W 면 blind ramp(REF 감시 OFF).
- 측정(CHAMBER_RF_CONT_POLL_INTERVAL_MS = 1 s 마다, 1초 PLC 묶음 읽기의 D8/D9): 시작 구간 FWD 0.0 은 판정 안 함(150 s 넘게
  계속이면 실패). blind 도달 시간·Ref.p 대기(최대 60 s)·저출력(FWD ≤ 1 W 3회) → 보정.
  RAMPING_UP: FWD ≥ 70 이면 BLIND_SETTLE(60 s, REF ≤ 1 W 면 조기 종료). |목표−FWD| ≤ 1 W 면 MAINTAINING·target_reached.
  모자라면 스텝 < 목표 → +RF_RAMP_STEP(1 W/측정, 목표에서 멈춤), 스텝 ≥ 목표 → +3 W, 넘치면 −1 W. 값이 바뀔 때만 WRITE.
  MAINTAINING: 오차가 연속 2회면 ±0.1 W.
- stop(): 마지막 전송값에서 RF_RAMP_STEP 씩 CHAMBER_RF_CONT_RAMPDOWN_INTERVAL_MS(1 s) 마다 WRITE(검증 없음) → 0 →
  0 W 확인 → SET OFF(plc_power.ZeroThenOff) → power_off_finished. 쓰기는 safety 요청.
이벤트: PowerEvent(kind = v1 RFPowerEvent: status/display/state_changed/target_reached/target_failed/power_off_finished/
  power_off_failed). 1초 행 블록 'ch2.rf'(PC 는 'pc.rf').
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from v2.common.snapshot import DeviceBlock, FieldDef, FType
from v2.common.worker import Request, Result, Status
from v2.ch12.drivers.plc import PlcDriver, PlcPoll, rf_scale
from v2.ch12.drivers.plc_power import PowerEvent, ZeroThenOff, err_text

MODES = {
    "ch2": dict(rf_ch=2, set_coil=82, f_reg=8, r_reg=9, block="ch2.rf", source="CH2"),
    "pc": dict(rf_ch=1, set_coil=81, f_reg=2, r_reg=3, block="pc.rf", source="PC"),
}
# 일반 모드 생성자 기본값(reload 에서 갱신 안 됨, device/rf_power.py:113-117)
NORMAL_FINE_UP_W = 3.0
NORMAL_RAMP_DOWN_W = 1.0
NORMAL_KICK = dict(kick_th=100.0, kick_w=50.0, cut_w=50.0)
INITIAL_STEP_W = 1.0
MAINTAIN_NEED = 2
LATCH_HOLD_S = 3.0          # H1 — 래치를 못 읽은 tick(명령 양보)에 마지막 래치 값을 쓰는 시간

BUDGET = dict(queries_per_tick=0, bytes_tx=0, bytes_rx=0, min_gap_ms=150, fits_1s=True, alternate="",
              note="읽기는 PLC 1초 묶음(D0..D11)에 포함. 쓰기만 PLC worker 로(램프 중 측정마다 최대 1회, 정지 1 s 마다 1회)")


def blocks(mode: str = "ch2") -> List[DeviceBlock]:
    m = MODES[mode]
    f = (FieldDef("fwd_w", FType.FLOAT, "W", "RF Forward", fmt=".1f", source=f"plc:D{m['f_reg']}", summary="mean"),
         FieldDef("ref_w", FType.FLOAT, "W", "RF Reflected", fmt=".1f", source=f"plc:D{m['r_reg']}", summary="mean"),
         FieldDef("set_w", FType.FLOAT, "W", "RF 설정(내부 FWD 좌표)", fmt=".1f", source="driver"),
         FieldDef("write_w", FType.FLOAT, "W", "RF 쓰기값(역변환 뒤)", fmt=".3f", source="driver", detail=True),
         FieldDef("target_w", FType.FLOAT, "W", "RF 목표", fmt=".1f", source="driver"),
         FieldDef("state", FType.STR, "", "RF 상태", source="driver", detail=True),
         FieldDef("running", FType.BOOL, "", "RF 동작 중", source="driver", summary="any"),
         FieldDef("enable", FType.BOOL, "", "RF SET 래치", source=f"plc:FC1 {m['set_coil']}", summary="any", detail=True),
         # W-21·22 대조 B — 화면 표시용(기록·안전 판정은 fwd_w·ref_w 읽은 값 그대로)
         FieldDef("cmd_on", FType.BOOL, "", "RF 명령 출력", source="driver(SET 래치 · 쓰기값 > 0 또는 모름)", summary="any"),
         FieldDef("uncommanded", FType.BOOL, "", "명령 없는 RF 출력", source="driver(cmd_on 아님 · Forward ≥ RF_UNCOMMANDED_FWD_W)",
                  summary="any"))
    return [DeviceBlock(m["block"], "RF 연속(PLC)" if mode == "ch2" else "플라즈마 클리닝 RF(PLC)", f)]


class RfPower:
    def __init__(self, clock, plc: PlcDriver, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[PowerEvent], None]] = None, chamber: str = "ch2", mode: str = "ch2") -> None:
        self.clock, self.plc, self.cfg = clock, plc, cfg
        self.writes_allowed = writes_allowed
        self.on_event = on_event
        self.mode = mode
        self.m = MODES[mode]
        self.rf_ch = self.m["rf_ch"]
        self.source = self.m["source"]
        self.block = self.m["block"]
        self.state = self.previous_state = "IDLE"
        self.running = False
        self.ramping_down = False
        self.enabled = False
        self.target_power = 0.0
        self.current_step = INITIAL_STEP_W
        self.forward_w = self.reflected_w = 0.0
        self.last_sent: Optional[float] = None
        self.rampdown_w = 0.0
        self.maintain_count = 0
        self.low_power_n = 0
        self.ref_armed = False
        self.blind_reach_ts: Optional[float] = None
        self.blind_start_ts: Optional[float] = None
        self.ref_wait_start: Optional[float] = None
        self._ref_wait_open = False          # 반사파 대기 사건 시작을 냈고 끝을 아직 안 냄(W-21·22 A — 시작·끝 한 번씩)
        self._uncmd = False                  # 명령 없는 RF 출력 사건 시작을 냈음(W-21·22 B — 시작·끝 한 번씩)
        self._latch_seen: Optional[tuple] = None   # (마지막으로 읽은 SET 래치, 읽은 시각) — H1 양보한 tick 은 LATCH_HOLD_S 까지 이 값
        self._cmd_on_t: Optional[float] = None     # 마지막으로 명령 출력(cmd_on)이었던 시각 — 끈 직후 RF_UNCOMMANDED_GRACE_SEC 는 꺼지는 중
        self.output_off_unconfirmed = False
        self.polling = True
        self._due: Optional[float] = None
        self._now: Optional[float] = None          # 지금 판정 중인 측정의 시각(읽기 시작) — 경과 시간 기준
        self._gen = 0
        self.polls = 0                       # 판정에 쓴 측정 수(시험·진단)
        self._stop_done: List[Callable] = []
        plc.register_decoder((self.block,), self._decode, self._read_failed)

    # ───────── 설정(매번 cfg 에서 — v1 reload_runtime_cfg) ─────────
    def _g(self, k: str, default=None):
        v = self.cfg.get(k, default)
        return default if v is None else v

    @property
    def direct(self) -> bool:
        return bool(self._g("PC_RF_DIRECT_MODE", True)) if self.mode == "pc" else bool(self._g("CHAMBER_RF_CONT_DIRECT_MODE", False))

    def p(self, name: str) -> float:
        """모드별 운전값."""
        pc = self.mode == "pc"
        table = {
            "poll_ms": ("PC_RF_POLL_INTERVAL_MS", 1000) if pc else ("CHAMBER_RF_CONT_POLL_INTERVAL_MS", 1000),
            "rampdown_ms": ("PC_RF_RAMPDOWN_INTERVAL_MS", 50) if pc else ("CHAMBER_RF_CONT_RAMPDOWN_INTERVAL_MS", 1000),
            "inv_a": ("PC_RF_WRITE_INV_A", 1.74) if pc else ("CHAMBER_RF_CONT_WRITE_INV_A", 1.6546),
            "inv_b": ("PC_RF_WRITE_INV_B", 0.0) if pc else ("CHAMBER_RF_CONT_WRITE_INV_B", 2.6323),
            "ramp_step": ("PC_RF_RAMP_STEP", 50.0) if pc else ("RF_RAMP_STEP", 1.0),
            "fine_up": ("PC_RF_RAMP_FINE_UP_STEP", 3.0) if pc else (None, NORMAL_FINE_UP_W),
            "ramp_down": ("PC_RF_RAMP_DOWN_STEP", 1.0) if pc else (None, NORMAL_RAMP_DOWN_W),
            "kick_th": ("PC_RF_RAMP_KICK_THRESHOLD_W", 100.0) if pc else (None, NORMAL_KICK["kick_th"]),
            "kick_w": ("PC_RF_RAMP_INITIAL_KICK_W", 50.0) if pc else (None, NORMAL_KICK["kick_w"]),
            "cut_w": ("PC_RF_RAMP_SHUTDOWN_CUT_W", 50.0) if pc else (None, NORMAL_KICK["cut_w"]),
        }
        key, d = table[name]
        return float(d if key is None else self._g(key, d))

    def c(self, k: str) -> float:
        return float(self.cfg[k])

    def op_max_key(self) -> str:
        """운전 상한 설정 이름(R) — Plasma Cleaning RF 는 PC_RF_OPERATING_MAX_W, CH2 RF 는 RF_OPERATING_MAX_W."""
        return "PC_RF_OPERATING_MAX_W" if self.mode == "pc" else "RF_OPERATING_MAX_W"

    def op_max(self) -> float:
        """운전 상한 W(R — 사용자 결정 2026-11-27, D432): 이 값을 넘는 목표는 거부하고 보내는 W(램프·보정 포함)도 이 값까지.
        전원 정격 RF_MAX_POWER 보다 크지 않게(설정 규칙 RF01 이 막지만 한 번 더). 키가 없으면 RF_MAX_POWER(옛 설정 파일)."""
        rated = self.c("RF_MAX_POWER")
        try:
            v = float(self._g(self.op_max_key(), rated))
        except (TypeError, ValueError):
            v = rated
        return max(0.0, min(rated, v))

    def over_limit(self, w: float) -> Optional[str]:
        """운전 상한을 넘는 W 면 거부 글(상한·설정 이름), 아니면 None — 고쳐 쓰지 않는다."""
        lim = self.op_max()
        if float(w) > lim + 1e-9:
            return f"{float(w):g} W 가 운전 상한 {lim:g} W({self.op_max_key()})를 넘음"
        return None

    def xform_write(self, w: float) -> float:
        """역변환(device/rf_power.py:927-932): 0.01 W 이하는 진짜 0, 아니면 a·W + b.
        v1 은 이 값을 RF_MAX_POWER(600)로 잘라 CH2 RF 는 약 361 W, Plasma Cleaning 은 약 345 W 에서 막혔다(목표는 받아들이고 닿지 못함).
        R(D432 — D136 의 이 항목만 바꿈): D/A 범위(PLC_DC_POWER_MAX_W)로만 자른다. 보내는 W 는 운전 상한 아래라 상한 기본값
        (360·340 W)에서는 v1 과 같은 값이 나간다(시험 test_rf_limit.py — 0~상한 1 W 간격)."""
        if w <= 0.01:
            return 0.0
        return max(0.0, min(self.c("PLC_DC_POWER_MAX_W"), self.p("inv_a") * float(w) + self.p("inv_b")))

    # ───────── 이벤트 ─────────
    def _emit(self, kind: str, detail: str = "", reason: Optional[str] = None, **data) -> None:
        if self.on_event:
            self.on_event(PowerEvent(kind, detail, reason, None, dict(data, mode=self.mode)))

    def _status(self, msg: str) -> None:
        self._emit("status", msg)

    def _ref_wait_event(self, phase: str, why: str = "") -> None:
        """반사파 대기 웹 사건(W-21·22 A) — 대기 시작 1번·끝 1번(풀림·시간 초과·RF 끄기). 공정 txt 의 v1 문구(status)와 따로."""
        if not self.on_event:
            return
        th, wait_to = self.c("RF_REFLECTED_THRESHOLD_W"), self.c("RF_REFLECTED_WAIT_TIMEOUT_S")
        ref = round(float(self.reflected_w or 0.0), 1)
        if phase == "start":
            text = f"{ref:g} W 가 기준 {th:g} W 를 넘어 대기 시작 · 최대 {int(wait_to)} s"     # 화면 줄: RF Power | Reflected 대기 | 이 글
            data = dict(phase="start", ref_w=ref, threshold=th, max_s=wait_to)
            self.on_event(PowerEvent("ref_wait", text, None, None, dict(data, mode=self.mode), level="warn"))
            return
        st = self.ref_wait_start
        waited = None if st is None else round(max(0.0, self._mono() - st), 1)
        text = f"끝 · {why} · {ref:g} W" + (f" · {waited:g} s 대기" if waited is not None else "")
        data = dict(phase="end", why=why, ref_w=ref, waited_s=waited)
        self.on_event(PowerEvent("ref_wait", text, None, None, dict(data, mode=self.mode),
                                 level="warn" if why == "시간 초과" else "info"))

    def _ref_wait_end(self, why: str) -> None:
        if self._ref_wait_open:
            self._ref_wait_open = False
            self._ref_wait_event("end", why)

    def _after(self, delay_s: float, fn: Callable[[], None]) -> None:
        """delay_s 뒤 fn — worker 큐의 빈 요청(safety)으로 시각을 맞춘다(정지 램프 간격)."""
        self.plc.worker.submit(Request("safety", lambda s: None, not_before=self.clock.monotonic() + delay_s,
                                       on_done=lambda r: fn(), confirms_link=False, name=f"rf:{self.mode}:timer"))

    # ───────── 쓰기 ─────────
    def _write_raw(self, w_scaled: float, on_done, kind: str = "write", guard=None) -> None:
        def cb(res: Result) -> None:
            if on_done:
                on_done(res)
            if self.mode == "pc" and res.status == Status.OK and res.value is not None:
                self.plc.rf_read_fwd_ref(rf_ch=1, source=self.source, on_done=self._pc_readback)
        self.plc.rf_write_w(w_scaled, rf_ch=self.rf_ch, source=self.source, on_done=cb, kind=kind, guard=guard)

    def _pc_readback(self, res: Result) -> None:
        if res.status == Status.OK:
            self._emit("log", f"RF READ FWD={res.value['forward']:.1f} W, REF={res.value['reflected']:.1f} W")
        else:
            self._emit("log", f"RF READ 실패: {err_text(res)}")

    def _send(self, power_w: float, after: Optional[Callable[[bool], None]] = None, guard=True) -> bool:
        """검증 경로 전송(클램프 0~운전 상한·같은 값 억제). 실제로 보냈으면 True. after(ok) 는 결과 뒤."""
        power_w = max(0.0, min(self.op_max(), float(power_w)))
        if self.last_sent is not None and abs(power_w - self.last_sent) < 1e-6:
            return False
        g = self._gen

        def cb(res: Result) -> None:
            ok = res.status == Status.OK and res.value is not None
            if ok:
                self.last_sent = power_w
            elif res.status != Status.OK:
                self._status(f"RF 설정 전송 실패(verified): {err_text(res)}")
            if after and (ok or res.status != Status.OK):
                after(ok)
        self._write_raw(self.xform_write(power_w), cb,
                        guard=(lambda: self.running and self._gen == g) if guard else None)
        return True

    def _send_unverified(self, power_w: float, after: Callable[[Result], None]) -> None:
        power_w = max(0.0, min(self.op_max(), float(power_w)))

        def cb(res: Result) -> None:
            if res.status != Status.OK:
                self._status(f"RF 설정 전송 실패(unverified): {err_text(res)}")
            after(res)
        self._write_raw(self.xform_write(power_w), cb, kind="safety")

    def retarget(self, target_power: float) -> Optional[float]:
        """공정 중 새 목표(PROCESS_ADJUST 지금 블록) — start() 와 같은 제한·반사파 감시 시작 조건·카운터 0. 적용한 목표를 돌려준다.
        운전 상한을 넘으면 바꾸지 않고 None(R — 공정 중 값 바꾸기가 먼저 거부하지만 드라이버도 한 번 더)."""
        bad = self.over_limit(target_power)
        if bad:
            self._status(f"RF 새 목표 거부: {bad}")
            return None
        self.target_power = float(max(0.0, min(self.c("RF_MAX_POWER"), float(target_power))))
        self.low_power_n = 0
        self.blind_reach_ts = None
        th = self.c("RF_BLIND_RAMP_FWD_THRESHOLD_W")
        self.ref_armed = self.direct or th <= 0.0 or self.target_power <= th
        if hasattr(self, "sent_target_reached"):
            self.sent_target_reached = False
        self._held = None
        return self.target_power

    # ───────── 일시정지(11-09 C3) ─────────
    def hold(self) -> Optional[float]:
        """파워 램프 일시정지 — 그 순간 측정 출력(Forward)으로 목표를 고정(램프·보정·도달 이벤트 멈춤. 저출력·반사파 감시는 계속)."""
        if not self.running or getattr(self, "_held", None) is not None:
            return None
        self._held = (float(self.target_power), self.state)
        meas = float(max(0.0, min(self.c("RF_MAX_POWER"), float(self.forward_w or 0.0))))
        self.target_power = meas
        self._status(f"[HOLD] 일시정지 — 목표 {self._held[0]:.1f} W → 지금 출력 {meas:.1f} W 로 고정")
        return meas

    def release(self) -> Optional[float]:
        """재개 — 원래 목표·상태로 램프 계속. 고정 중이 아니면 None."""
        held = getattr(self, "_held", None)
        if held is None:
            return None
        self._held = None
        self.target_power = float(held[0])
        if held[1] in ("RAMPING_UP", "BLIND_SETTLE") and self.state == "MAINTAINING":
            self.state = held[1]
        self.low_power_n = 0
        self._status(f"[HOLD] 재개 — 목표 {held[0]:.1f} W 로 램프 계속")
        return float(held[0])

    # ───────── 시작 ─────────
    def start(self, target_power: float, on_done: Optional[Callable[[Optional[str]], None]] = None) -> None:
        done = on_done or (lambda r: None)
        if self.running:
            self._status("경고: RF 파워가 이미 동작 중입니다.")
            done("STATE_NOT_ALLOWED")
            return
        bad = self.over_limit(target_power)
        if bad:                                              # R — 고쳐 쓰지 않고 거부(레시피 검사·PC 시작이 먼저 거부한다)
            self._status(f"RF 시작 거부: {bad}")
            done("BAD_ARGS")
            return
        self.target_power = float(max(0.0, min(self.c("RF_MAX_POWER"), float(target_power))))
        self._held = None
        self.current_step = INITIAL_STEP_W
        self.last_sent = None
        self.low_power_n = 0
        self.blind_reach_ts = None
        self.blind_start_ts = self.clock.monotonic()
        th = self.c("RF_BLIND_RAMP_FWD_THRESHOLD_W")
        self.ref_armed = self.direct or th <= 0.0 or self.target_power <= th
        self._gen += 1
        g = self._gen

        def set_done(res: Result) -> None:
            if g != self._gen:
                done("CANCELLED")
                return
            if res.status != Status.OK:
                self._status(f"RF SET ON 실패: {err_text(res)}")
                done(res.reason or "DEVICE_REJECTED")
                return
            self.enabled = True
            self._status("RF SET ON")
            self.running = True
            self._emit("state_changed", running=True)
            self.state = "RAMPING_UP"
            if self.direct:
                self._start_direct(done)
                return
            self._status(f"프로세스 시작. 목표: {self.target_power:.1f} W")
            self.polling = True
            self._due = self.clock.monotonic()
            step = self.current_step

            def first(ok: bool) -> None:
                self._status(f"Ramp-Up 시작: 초기 {step:.1f}W 전송")
                done(None)
            self._send(step, after=first)
        self.plc.rf_enable(True, rf_ch=self.rf_ch, source=self.source, on_done=set_done)

    def _start_direct(self, done) -> None:
        if self.target_power > self.p("kick_th"):
            kick = min(self.p("kick_w"), self.target_power)
            self._status(f"Kick+Ramp 시작: {kick:.1f}W kick → {self.target_power:.1f}W까지 "
                         f"↑{self.p('ramp_step'):.1f}W/s, ↓{self.p('ramp_down'):.1f}W/s")

            def kicked(ok: bool) -> None:          # v1 _send_rf_power 는 실패를 삼켜 '전송 완료'로 이어간다
                self.current_step = kick
                self._status(f"Kick {kick:.1f}W 전송 완료 — ramp 시작 대기")
                self.polling, self._due = True, self.clock.monotonic()
                done(None)
            self._send(kick, after=kicked)
        else:
            self._status(f"Direct set: {self.target_power:.1f} W")

            def direct(ok: bool) -> None:
                self.current_step = float(self.target_power)
                self.last_sent = float(self.target_power)
                self._status(f"Direct set {self.target_power:.1f}W 전송 — 도달 판정 대기")
                self.polling, self._due = True, self.clock.monotonic()
                done(None)
            self._send(self.target_power, after=direct)

    def set_process_status(self, active: bool) -> None:
        self.polling = bool(active)
        if active and self._due is None:
            self._due = self.clock.monotonic()

    # ───────── 측정 ─────────
    def _decode(self, poll: PlcPoll) -> Dict[str, Any]:
        fr, rr = poll.regs.get(self.m["f_reg"]), poll.regs.get(self.m["r_reg"])
        b = self.block
        out: Dict[str, Any] = {f"{b}.set_w": self.last_sent,
                               f"{b}.write_w": None if self.last_sent is None else round(self.xform_write(self.last_sent), 3),
                               f"{b}.target_w": self.target_power if self.running else None, f"{b}.state": self.state,
                               f"{b}.running": self.running, f"{b}.enable": poll.coils.get(self.m["set_coil"])}
        # 명령 출력(W-21·22 대조 B): SET 래치 켜짐 그리고 (드라이버 쓰기값 > 0 또는 쓰기값 모름 — 다시 시작 직후·읽기 전용이라 이 프로그램이 쓴 적 없음)
        # H1: PLC 읽기가 명령에 양보한 tick 은 래치를 못 읽는다 — 마지막으로 읽은 값을 LATCH_HOLD_S 까지 쓰고, 그보다 오래 모르면 None
        latch, t = poll.coils.get(self.m["set_coil"]), poll.t_mono
        if latch is not None:
            self._latch_seen = (bool(latch), t)
        elif self._latch_seen is not None and t - self._latch_seen[1] <= LATCH_HOLD_S:
            latch = self._latch_seen[0]
        cmd_on = None if latch is None else (bool(latch) and (self.last_sent is None or self.xform_write(self.last_sent) > 0.0))
        out[f"{b}.cmd_on"] = cmd_on
        if cmd_on:
            self._cmd_on_t = t
        if fr is None or rr is None:
            out.update({f"{b}.fwd_w": None, f"{b}.ref_w": None, f"{b}.uncommanded": None})
            return out
        zero = None if self.mode == "pc" else bool(self._g("CHAMBER_RF_CONT_ZEROING", False))
        meas = rf_scale(self.cfg, fr, rr, self.rf_ch, zero)
        out.update({f"{b}.fwd_w": meas["forward"], f"{b}.ref_w": meas["reflected"]})
        th_u = float(self._g("RF_UNCOMMANDED_FWD_W", 5.0) or 5.0)
        grace = max(0.0, float(self._g("RF_UNCOMMANDED_GRACE_SEC", 5.0)))      # N — 예전 고정값 UNCMD_GRACE_S 5 s(챔버K 와 같은 설정)
        settling = self._cmd_on_t is not None and t - self._cmd_on_t < grace   # 명령으로 끈 직후 — A/D 가 내려가는 중
        unc = None if cmd_on is None else (not cmd_on and not settling and float(meas["forward"]) >= th_u)
        out[f"{b}.uncommanded"] = unc
        self._uncommanded_track(unc, meas["forward"], th_u)
        if self.running and self.polling and self._due is not None and poll.t_tick >= self._due - 1e-6:
            self._due = poll.t_tick + max(50.0, self.p("poll_ms")) / 1000.0 - 0.5
            self._now = poll.t_tick
            self.polls += 1
            try:
                self._ingest(meas["forward"], meas["reflected"])
            finally:
                self._now = None
        return out

    def _uncommanded_track(self, unc: Optional[bool], fwd: float, th: float) -> None:
        """명령 없는 RF 출력 사건(W-21·22 B) — 시작(warn)·끝(info) 한 번씩. 모름(None)이면 지금 상태를 유지."""
        if unc is None or bool(unc) == self._uncmd or not self.on_event:
            return
        self._uncmd = bool(unc)
        pre = "Plasma Cleaning · " if self.mode == "pc" else ""          # 화면 줄: RF Power | 명령 없는 RF 출력 | 이 글(같은 말 반복 없이)
        if unc:
            self.on_event(PowerEvent("rf_uncommanded", f"{pre}Forward {fwd:g} W · 기준 {th:g} W — 전원 장치 앞면(LOCAL)에서 켰는지 확인",
                                     None, None, dict(forward=fwd, threshold=th, mode=self.mode), level="warn"))
        else:
            self.on_event(PowerEvent("rf_uncommanded_end", f"{pre}Forward {fwd:g} W", None, None,
                                     dict(forward=fwd, mode=self.mode), level="info"))

    def _mono(self) -> float:
        return self.clock.monotonic() if self._now is None else self._now

    def _read_failed(self, why: str) -> None:
        if self.running and self.polling and self._due is not None and self.clock.monotonic() >= self._due - 1e-6:
            self._due = self.clock.monotonic() + max(50.0, self.p("poll_ms")) / 1000.0 - 0.5
            self._status(f"상태 읽기 요청 실패: {why}")

    def _ingest(self, fwd: float, ref: float) -> None:
        if fwd == 0.0 and self.state in ("RAMPING_UP", "BLIND_SETTLE"):
            self._zero_start_check()            # 시작 구간 0.0 W 는 판정에 넣지 않는다(device/rf_power.py:589-597)
            return
        self.update_measurements(fwd, ref)

    def _zero_start_check(self) -> None:
        to = self.c("RF_BLIND_RAMP_REACH_TIMEOUT_S")
        if not self.running or to <= 0.0 or self.blind_start_ts is None:
            return
        if self._mono() - self.blind_start_ts > to:
            self._status("RF 출력이 나오지 않아 RF 공정을 중단합니다.")
            self._emit("target_failed", f"Blind ramp 도달 시간({int(to)}s) 초과: 시작 뒤 FWD 0.0 W(출력 없음)",
                       reason="RF_NO_OUTPUT")
            self.stop()

    def update_measurements(self, forward_w: float, reflected_w: float) -> None:
        if not self.running:
            return
        self.forward_w, self.reflected_w = float(forward_w or 0.0), float(reflected_w or 0.0)
        self._emit("display", forward=self.forward_w, reflected=self.reflected_w)
        now = self._mono()
        to = self.c("RF_BLIND_RAMP_REACH_TIMEOUT_S")
        if not self.ref_armed and to > 0.0 and self.state == "RAMPING_UP" and self.blind_start_ts is not None:
            if now - self.blind_start_ts > to:
                self._status("Blind ramp 도달 시간 초과. RF 공정을 중단합니다.")
                self._emit("target_failed", f"Blind ramp 도달 시간({int(to)}s) 초과: FWD={self.forward_w:.1f}W < 임계 "
                                            f"{self.c('RF_BLIND_RAMP_FWD_THRESHOLD_W'):.1f}W (REF={self.reflected_w:.1f}W)",
                           reason="RF_BLIND_TIMEOUT")
                self.stop()
                return
        th, wait_to = self.c("RF_REFLECTED_THRESHOLD_W"), self.c("RF_REFLECTED_WAIT_TIMEOUT_S")
        # 여유 폭(W-21·22 A): 대기는 REF > 기준이면 시작하고 'REF ≤ 기준 − RF_REFLECTED_HYST_W' 일 때만 풀린다. 그 사이면 대기를 이어 가고
        # 시간은 대기 시작부터 이어서 센다(기준을 오가도 시간 초과가 난다). 0 이면 v1 그대로(기준 이하면 곧바로 풀림)
        hyst = max(0.0, float(self._g("RF_REFLECTED_HYST_W", 1.0) or 0.0))
        if self.ref_armed:
            waiting = self.state == "REF_P_WAITING"
            if self.reflected_w > th or (waiting and self.reflected_w > th - hyst):
                if not waiting:
                    self.previous_state, self.state = self.state, "REF_P_WAITING"
                    self.ref_wait_start = now
                    self._status(f"Ref.p({self.reflected_w:.1f}W) 안정화 대기 시작 (최대 {int(wait_to)}초)")
                    self._ref_wait_open = True
                    self._ref_wait_event("start")
                elif now - (self.ref_wait_start or 0.0) > wait_to:
                    self._status("Ref.p 안정화 시간 초과. 즉시 중단합니다.")
                    self._ref_wait_end("시간 초과")
                    self._emit("target_failed", f"Ref.p(REF) 안정화 시간({int(wait_to)}s) 초과: REF={self.reflected_w:.1f}W "
                                                f"> TH={th:.1f}W", reason="RF_REF_TIMEOUT")
                    self.stop()
                return
            if waiting:
                self._status(f"Ref.p 안정화 완료({self.reflected_w:.1f}W). 공정 재개")
                self._ref_wait_end("풀림")
                self.state, self.ref_wait_start = self.previous_state, None
        if self.target_power > 0.0:
            lo, n = self.c("RF_LOW_POWER_THRESH_W"), int(self.c("RF_LOW_POWER_COUNT_MAX_N"))
            if self.forward_w <= lo:
                self.low_power_n += 1
                self._status(f"저출력 감지: Forward={self.forward_w:.1f}W ({self.low_power_n}/{n})")
                if self.low_power_n >= n:
                    self._status("Forward power가 너무 낮아 RF 공정을 중단합니다.")
                    self._emit("target_failed", f"Forward power <= {lo:.1f}W {self.low_power_n}회 연속", reason="RF_LOW_POWER")
                    self.stop()
                    return
            else:
                self.low_power_n = 0
        self._adjust_once()

    def _adjust_once(self) -> None:
        if not self.running or self.state == "REF_P_WAITING" or getattr(self, "_held", None) is not None:
            return                                           # 일시정지 중에는 램프·보정·도달 이벤트 없음
        last = self.last_sent
        if self.state == "RAMPING_UP":
            th = self.c("RF_BLIND_RAMP_FWD_THRESHOLD_W")
            if not self.ref_armed and self.forward_w >= th:
                self.state, self.blind_reach_ts = "BLIND_SETTLE", self._mono()
                self._status(f"Blind ramp 완료: FWD={self.forward_w:.1f}W 도달 (임계 {th:.1f}W) → "
                             f"{int(self.c('RF_BLIND_RAMP_SETTLE_S'))}초 안정화 대기 (REF.p 감시 OFF)")
                return
            diff = self.target_power - self.forward_w
            if abs(diff) <= self.c("RF_TOLERANCE_POWER"):
                self._status(f"{self.target_power:.1f}W 도달. 파워 유지 시작")
                self.state = "MAINTAINING"
                if self.last_sent is not None:
                    self.current_step = float(self.last_sent)
                self._emit("target_reached")
                return
            if diff > 0:
                if self.current_step < self.target_power:
                    new = min(self.current_step + self.p("ramp_step"), self.target_power)
                else:
                    new = min(self.current_step + self.p("fine_up"), self.op_max())
            else:
                new = max(0.0, self.current_step - self.p("ramp_down"))
                self._status(f"목표 파워 초과. Ramp-Down 시도... (step→{new:.1f}W, FWD={self.forward_w:.1f}W)")
            new = max(0.0, min(self.op_max(), float(new)))      # R — 보정으로 목표를 넘겨 올려도 운전 상한까지(v1 은 환산값 600 에서 막힘)
            sent = (last is None) or (abs(new - last) > 1e-6)
            self.current_step = float(new)
            fwd = self.forward_w
            if sent:
                self._send(new, after=lambda ok: self._status(f"Ramp-Up... 목표스텝:{new:.1f}W, 현재:{fwd:.1f}W"))
            return
        if self.state == "BLIND_SETTLE":
            now = self._mono()
            el = now - (self.blind_reach_ts or now)
            early = self.c("RF_BLIND_SETTLE_EARLY_EXIT_REF_W")
            if early > 0.0 and self.reflected_w <= early:
                self.ref_armed, self.state = True, "RAMPING_UP"
                self._status(f"REF.p({self.reflected_w:.1f}W) ≤ {early:.1f}W → 안정화 대기 조기 종료 ({el:.1f}s 경과). "
                             f"REF.p 감시 ON → ramp-up 재개")
                return
            if el >= self.c("RF_BLIND_RAMP_SETTLE_S"):
                self.ref_armed, self.state = True, "RAMPING_UP"
                self._status(f"안정화 대기 완료 ({el:.1f}s 경과). REF.p 감시 ON → ramp-up 재개")
            return
        if self.state == "MAINTAINING":
            err = self.target_power - self.forward_w
            if abs(err) <= self.c("RF_TOLERANCE_POWER"):
                self.maintain_count = 0
                return
            self.maintain_count += 1
            if self.maintain_count < MAINTAIN_NEED:
                return
            self.maintain_count = 0
            step = self.c("RF_MAINTAIN_STEP") * (1 if err > 0 else -1)
            new = max(0.0, min(self.op_max(), float(self.current_step) + step))
            self.current_step = float(new)
            if last is None or abs(new - last) > 1e-6:
                fwd, tgt = self.forward_w, self.target_power
                self._send(new, after=lambda ok: self._status(f"유지 보정: meas={fwd:.1f}W, target={tgt:.1f}W → set {new:.1f}W"))

    # ───────── 정지 ─────────
    def used_this_run(self) -> bool:
        return bool(self.enabled) or (self.last_sent not in (None, 0.0))

    def stop(self, on_done: Optional[Callable[[Optional[str]], None]] = None) -> None:
        """정지(cleanup). on_done(None)=0 W 확인·완료, 'POWER_OFF_UNCONFIRMED'=0 W 미확인(완료 신호 없음)."""
        if on_done:
            self._stop_done.append(on_done)
        if self.ramping_down and not self.running:
            return
        self._status("정지 신호 수신됨.")
        self._ref_wait_end("RF 끄기")                 # 대기 중에 멈추면 끝 사건 한 번(W-21·22 A)
        self._held = None
        self.running = False
        self._gen += 1
        self.polling, self._due = False, None
        self._emit("state_changed", running=False)
        if self.direct:
            if self.target_power > self.p("kick_th"):
                self.state = "IDLE"
                self._status(f"Kick+Ramp shutdown: {self.p('ramp_step'):.1f}W/s 하강 → {self.p('cut_w'):.1f}W 도달 시 즉시 OFF")
                self.ramping_down = True
                self.rampdown_w = self.last_sent if self.last_sent is not None else float(self.target_power)
                self._kick_step()
                return
            self.ramping_down = True
            self._finish_power_off(lambda ok: self._end(ok, idle=True))
            return
        self.state = "IDLE"
        self._status("RF 파워 ramp-down 시작")
        self.ramping_down = True
        self.rampdown_w = self.last_sent if self.last_sent is not None else float(self.current_step)
        self._ramp_step()

    cleanup = stop

    def _ramp_step(self) -> None:
        if not self.ramping_down:
            return
        if self.rampdown_w <= 0.0:
            self._status(f"Ramp-Down final: target=0.0W → write={self.xform_write(0.0):.3f}W")
            self._finish_power_off(lambda ok: self._end(ok, msg="RF 파워 ramp-down 완료"))
            return
        self.rampdown_w = max(0.0, self.rampdown_w - self.p("ramp_step"))
        self.last_sent = self.rampdown_w
        self._status(f"Ramp-Down step: target={self.rampdown_w:.1f}W → write={self.xform_write(self.rampdown_w):.3f}W")
        self._send_unverified(self.rampdown_w, lambda res: self._after(self.p("rampdown_ms") / 1000.0, self._ramp_step))

    def _kick_step(self) -> None:
        if not self.ramping_down:
            return
        cut = self.p("cut_w")
        if self.rampdown_w <= cut:
            self._status(f"Ramp-Down: {self.rampdown_w:.1f}W ≤ {cut:.1f}W → 즉시 OFF")

            def fin(ok: bool) -> None:
                if ok:
                    self._emit("display", forward=0.0, reflected=0.0)
                self._end(ok, msg="RF 파워 ramp-down 완료")
            self._finish_power_off(fin)
            return
        self.rampdown_w = max(cut, self.rampdown_w - self.p("ramp_step"))
        self.last_sent = self.rampdown_w
        self._status(f"Ramp-Down step: {self.rampdown_w:.1f}W")
        self._send_unverified(self.rampdown_w, lambda res: self._after(1.0, self._kick_step))

    def _end(self, ok: bool, msg: str = "", idle: bool = False) -> None:
        self.ramping_down = False
        if idle:
            self.state = "IDLE"
        if ok:
            if msg:
                self._status(msg)
            self._emit("power_off_finished")
        done, self._stop_done = self._stop_done, []
        for f in done:
            f(None if ok else "POWER_OFF_UNCONFIRMED")

    def _finish_power_off(self, cb: Callable[[bool], None]) -> None:
        """0 W 확인 뒤에만 SET OFF(device/rf_power.py:863-925). 안 쓴 런은 0 W 1회만(실패해도 성공)."""
        if not self.used_this_run():
            self._send_unverified(0.0, lambda res: cb(True))
            return
        t0 = self.clock.monotonic()

        def write_zero(done, nb):
            def z(res: Result) -> None:
                if res.status == Status.OK:
                    self.last_sent = 0.0
                done(res)
            if nb is None:
                self._write_raw(0.0, z, kind="safety")
            else:
                self._after(max(0.0, nb - self.clock.monotonic()), lambda: self._write_raw(0.0, z, kind="safety"))

        def set_off(done, nb):
            def o(res: Result) -> None:
                if res.status == Status.OK:
                    self.enabled = False
                done(res)
            self.plc.rf_enable(False, rf_ch=self.rf_ch, source=self.source, on_done=o, kind="safety", not_before=nb)

        def zero_fail(n: int, el: float, err: str) -> None:
            self.output_off_unconfirmed = True
            msg = f"RF 0W 쓰기 실패 — 출력 상태 미확인 ({el:.1f}초, {n}회, {err})"
            self._status(msg)
            self._emit("power_off_failed", msg, reason="POWER_OFF_UNCONFIRMED")

        def off_result(ok: bool, err: str) -> None:
            self._status("RF SET OFF" if ok else f"RF SET OFF 실패 — 0W 는 확인됨 ({err})")

        def fin(ok: bool) -> None:
            if ok:
                self.output_off_unconfirmed = False
            cb(ok)
        ZeroThenOff(self.clock, self.c("POWER_OFF_ZERO_DEADLINE_S"), write_zero, set_off, self.enabled,
                    zero_fail, off_result, fin).start()

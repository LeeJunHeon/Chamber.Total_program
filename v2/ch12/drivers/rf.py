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

import time as _time
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
         FieldDef("enable", FType.BOOL, "", "RF SET 래치", source=f"plc:FC1 {m['set_coil']}", summary="any", detail=True))
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
        self.output_off_unconfirmed = False
        self.polling = True
        self._due: Optional[float] = None
        self._gen = 0
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

    def xform_write(self, w: float) -> float:
        """역변환(device/rf_power.py:927-932): 0.01 W 이하는 진짜 0, 아니면 a·W + b 를 0~RF_MAX_POWER."""
        if w <= 0.01:
            return 0.0
        return max(0.0, min(self.c("RF_MAX_POWER"), self.p("inv_a") * float(w) + self.p("inv_b")))

    # ───────── 이벤트 ─────────
    def _emit(self, kind: str, detail: str = "", reason: Optional[str] = None, **data) -> None:
        if self.on_event:
            self.on_event(PowerEvent(kind, detail, reason, None, dict(data, mode=self.mode)))

    def _status(self, msg: str) -> None:
        self._emit("status", msg)

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
        """검증 경로 전송(클램프·같은 값 억제). 실제로 보냈으면 True. after(ok) 는 결과 뒤."""
        power_w = max(0.0, min(self.c("RF_MAX_POWER"), float(power_w)))
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
        power_w = max(0.0, min(self.c("RF_MAX_POWER"), float(power_w)))

        def cb(res: Result) -> None:
            if res.status != Status.OK:
                self._status(f"RF 설정 전송 실패(unverified): {err_text(res)}")
            after(res)
        self._write_raw(self.xform_write(power_w), cb, kind="safety")

    # ───────── 시작 ─────────
    def start(self, target_power: float, on_done: Optional[Callable[[Optional[str]], None]] = None) -> None:
        done = on_done or (lambda r: None)
        if self.running:
            self._status("경고: RF 파워가 이미 동작 중입니다.")
            done("STATE_NOT_ALLOWED")
            return
        self.target_power = float(max(0.0, min(self.c("RF_MAX_POWER"), float(target_power))))
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
            if not self._send(self.current_step, after=lambda ok: None):
                pass
            self._status(f"Ramp-Up 시작: 초기 {self.current_step:.1f}W 전송")
            done(None)
        self.plc.rf_enable(True, rf_ch=self.rf_ch, source=self.source, on_done=set_done)

    def _start_direct(self, done) -> None:
        if self.target_power > self.p("kick_th"):
            kick = min(self.p("kick_w"), self.target_power)
            self._status(f"Kick+Ramp 시작: {kick:.1f}W kick → {self.target_power:.1f}W까지 "
                         f"↑{self.p('ramp_step'):.1f}W/s, ↓{self.p('ramp_down'):.1f}W/s")

            def kicked(ok: bool) -> None:
                if not ok:
                    self._status("Kick 전송 실패")
                    done("DEVICE_REJECTED")
                    return
                self.current_step = kick
                self._status(f"Kick {kick:.1f}W 전송 완료 — ramp 시작 대기")
                self.polling, self._due = True, self.clock.monotonic()
                done(None)
            self._send(kick, after=kicked)
        else:
            self._status(f"Direct set: {self.target_power:.1f} W")

            def direct(ok: bool) -> None:
                if not ok:
                    self._status("Direct set 실패")
                    done("DEVICE_REJECTED")
                    return
                self.current_step = float(self.target_power)
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
        if fr is None or rr is None:
            out.update({f"{b}.fwd_w": None, f"{b}.ref_w": None})
            return out
        zero = None if self.mode == "pc" else bool(self._g("CHAMBER_RF_CONT_ZEROING", False))
        meas = rf_scale(self.cfg, fr, rr, self.rf_ch, zero)
        out.update({f"{b}.fwd_w": meas["forward"], f"{b}.ref_w": meas["reflected"]})
        if self.running and self.polling and self._due is not None and poll.t_mono >= self._due - 1e-6:
            self._due = poll.t_mono + max(50.0, self.p("poll_ms")) / 1000.0 - 0.5
            self._ingest(meas["forward"], meas["reflected"])
        return out

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
        if self.clock.monotonic() - self.blind_start_ts > to:
            self._status("RF 출력이 나오지 않아 RF 공정을 중단합니다.")
            self._emit("target_failed", f"Blind ramp 도달 시간({int(to)}s) 초과: 시작 뒤 FWD 0.0 W(출력 없음)",
                       reason="RF_NO_OUTPUT")
            self.stop()

    def update_measurements(self, forward_w: float, reflected_w: float) -> None:
        if not self.running:
            return
        self.forward_w, self.reflected_w = float(forward_w or 0.0), float(reflected_w or 0.0)
        self._emit("display", forward=self.forward_w, reflected=self.reflected_w)
        now = self.clock.monotonic()
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
        if self.ref_armed:
            if self.reflected_w > th:
                if self.state != "REF_P_WAITING":
                    self.previous_state, self.state = self.state, "REF_P_WAITING"
                    self.ref_wait_start = now
                    self._status(f"Ref.p({self.reflected_w:.1f}W) 안정화 대기 시작 (최대 {int(wait_to)}초)")
                elif now - (self.ref_wait_start or 0.0) > wait_to:
                    self._status("Ref.p 안정화 시간 초과. 즉시 중단합니다.")
                    self._emit("target_failed", f"Ref.p(REF) 안정화 시간({int(wait_to)}s) 초과: REF={self.reflected_w:.1f}W "
                                                f"> TH={th:.1f}W", reason="RF_REF_TIMEOUT")
                    self.stop()
                return
            if self.state == "REF_P_WAITING":
                self._status(f"Ref.p 안정화 완료({self.reflected_w:.1f}W). 공정 재개")
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
        if not self.running or self.state == "REF_P_WAITING":
            return
        last = self.last_sent
        if self.state == "RAMPING_UP":
            th = self.c("RF_BLIND_RAMP_FWD_THRESHOLD_W")
            if not self.ref_armed and self.forward_w >= th:
                self.state, self.blind_reach_ts = "BLIND_SETTLE", self.clock.monotonic()
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
                    new = min(self.current_step + self.p("fine_up"), self.c("RF_MAX_POWER"))
            else:
                new = max(0.0, self.current_step - self.p("ramp_down"))
                self._status(f"목표 파워 초과. Ramp-Down 시도... (step→{new:.1f}W, FWD={self.forward_w:.1f}W)")
            new = max(0.0, min(self.c("RF_MAX_POWER"), float(new)))
            sent = (last is None) or (abs(new - last) > 1e-6)
            self.current_step = float(new)
            fwd = self.forward_w
            if sent:
                self._send(new, after=lambda ok: self._status(f"Ramp-Up... 목표스텝:{new:.1f}W, 현재:{fwd:.1f}W"))
            return
        if self.state == "BLIND_SETTLE":
            now = self.clock.monotonic()
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
            new = max(0.0, min(self.c("RF_MAX_POWER"), float(self.current_step) + step))
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

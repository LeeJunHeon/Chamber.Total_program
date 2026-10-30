"""CH2 DC 연속 전원(DC1·DC2) — PLC D/A 쓰기·A/D 읽기로 제어. v1 device/dc_power.py(DCPowerAsync) + runtime 결선.

결선(runtime/chamber_runtime.py:693-748): DC1 = SET_0(M00050=80)·WRITE_0(D4)·READ_0/1(D0 V·D1 I),
  DC2 = SET_3(M00053=83)·WRITE_3(D7)·READ_6/7(D10·D11). 연속 제어 쓰기는 WRITE 만(SET 은 시작·끝에서만).
  DC2 는 CH2_SUPPORTS_DC2 이고 CH2_SUPPORTS_HEATER 가 아닐 때만(D/A CH3 = 히터 출력, runtime/chamber_runtime.py:389-392).
동작(v1 그대로)
- start(목표): 이미 동작 중이면 경고만. 목표 0~DC_MAX_POWER, 카운터 0, 과전류 인터락 상태 1줄 → SET ON(실패면 중단) →
  유지 상태(램프 없음) → 목표 W 즉시 WRITE → 보정 1회(측정이 아직 없으면 마지막 측정 기준 — v1 과 같게 +1 W 가 나갈 수 있다).
- 측정: DC_INTERVAL_MS(5 s)마다 1초 PLC 묶음 읽기의 D0/D1(D10/D11)으로 (P, V, I) = (V·I, raw×0.50316, raw×0.00097405).
  표시 → 과전류(목표 기준 한계, 연속 N) → 저전력(P ≤ 1 W 3회) → 목표 도달 뒤 저전류(I ≤ 0.05 A 3회) → 보정.
  보정: |목표 − P| ≤ 1 W 면 target_reached 1회, 아니면 마지막 전송값 ±1 W(0~최대), 데드밴드 0.5 W 이상 바뀔 때만 WRITE.
  판정에 걸리면 target_failed + 정지(v1 처럼 그 측정의 보정 쓰기가 먼저 나간 뒤 0 W).
- stop(): 폴링 중지 → 'ramp-down 시작' → 이번 런에 안 썼으면 바로 완료, 썼으면 0 W 확인 → SET OFF → 완료
  (실패면 power_off_failed, output_off_unconfirmed). 쓰기는 safety 요청.
이벤트: on_event(PowerEvent(kind, detail, reason, data)) — kind 는 v1 DCPowerEvent 와 같다. 1초 행 블록 'ch2.dc'.
"""
from __future__ import annotations

import math
from typing import Any, Callable, Dict, List, Optional

from v2.common.snapshot import DeviceBlock, FieldDef, FType
from v2.common.worker import Result, Status
from v2.ch12.drivers.plc import PlcDriver, PlcPoll, dc_scale
from v2.ch12.drivers.plc_power import PowerEvent, ZeroThenOff, err_text

UNITS = {1: dict(set_idx=0, write_idx=0, v_reg=0, i_reg=1, set_coil=80, name="DC1"),
         2: dict(set_idx=3, write_idx=3, v_reg=10, i_reg=11, set_coil=83, name="DC2")}

BUDGET = dict(queries_per_tick=0, bytes_tx=0, bytes_rx=0, min_gap_ms=150, fits_1s=True, alternate="",
              note="읽기는 PLC 1초 묶음(D0..D11)에 포함. 쓰기만 PLC worker 로(측정 5 s 마다 최대 1회)")


def dc_overcurrent_limit_a(cfg: Dict[str, Any], p_set_w) -> float:
    """세트포인트 W 에 대한 전류 상한(lib/power_limits.py:20-55) — 순수 함수, 비정상 인자는 I1."""
    try:
        i1 = float(cfg["DC_OVERCURRENT_I1_A"])
    except Exception:
        i1 = 1.0
    if not math.isfinite(i1) or i1 <= 0.0:
        i1 = 1.0
    try:
        p1, p2 = float(cfg["DC_OVERCURRENT_P1_W"]), float(cfg["DC_OVERCURRENT_P2_W"])
        i2, ext = float(cfg["DC_OVERCURRENT_I2_A"]), bool(cfg["DC_OVERCURRENT_EXTRAPOLATE"])
        p = float(p_set_w) if p_set_w is not None else 0.0
    except Exception:
        return i1
    if not all(math.isfinite(x) for x in (p1, p2, i2, p)) or p2 <= p1 or i2 <= 0.0 or p <= p1:
        return i1
    slope = (i2 - i1) / (p2 - p1)
    lim = (i2 + (p - p2) * slope if ext else i2) if p >= p2 else i1 + (p - p1) * slope
    return i1 if (not math.isfinite(lim) or lim <= 0.0) else lim


def blocks(chamber: str = "ch2") -> List[DeviceBlock]:
    f = []
    for u in (1, 2):
        n = f"dc{u}"
        r = UNITS[u]
        f += [FieldDef(f"{n}_p", FType.FLOAT, "W", f"DC{u} 전력", fmt=".1f", source=f"plc:D{r['v_reg']}×D{r['i_reg']}",
                       summary="mean"),
              FieldDef(f"{n}_v", FType.FLOAT, "V", f"DC{u} 전압", fmt=".1f", source=f"plc:D{r['v_reg']}×0.50316", summary="mean"),
              FieldDef(f"{n}_i", FType.FLOAT, "A", f"DC{u} 전류", fmt=".3f", source=f"plc:D{r['i_reg']}×0.00097405", summary="mean"),
              FieldDef(f"{n}_set_w", FType.FLOAT, "W", f"DC{u} 마지막 전송값", fmt=".1f", source="driver"),
              FieldDef(f"{n}_target_w", FType.FLOAT, "W", f"DC{u} 목표", fmt=".1f", source="driver"),
              FieldDef(f"{n}_running", FType.BOOL, "", f"DC{u} 동작 중", source="driver", summary="any"),
              FieldDef(f"{n}_enable", FType.BOOL, "", f"DC{u} SET 래치", source=f"plc:FC1 {r['set_coil']}", summary="any",
                       detail=True)]
    return [DeviceBlock(f"{chamber}.dc", "DC 전원(PLC)", tuple(f), desc="CH2 DC1·DC2 — PLC D/A·A/D")]


class DcPower:
    """DC 한 대(unit 1 = DC1, 2 = DC2)."""

    def __init__(self, clock, plc: PlcDriver, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[PowerEvent], None]] = None, chamber: str = "ch2", unit: int = 1) -> None:
        self.clock, self.plc, self.cfg = clock, plc, cfg
        self.writes_allowed = writes_allowed
        self.on_event = on_event
        self.chamber, self.unit = chamber, int(unit)
        self.u = UNITS[self.unit]
        self.name = self.u["name"]
        self.source = chamber.upper()
        self.block = f"{chamber}.dc"
        self.state = "IDLE"
        self.running = False
        self.enabled = False                 # SET 래치(이번 런)
        self.target_power = 0.0
        self.power_w = self.voltage_v = self.current_a = 0.0
        self.last_sent: Optional[float] = None
        self.sent_target_reached = False
        self.output_off_unconfirmed = False
        self.low_power_streak = self.low_current_streak = self.overcurrent_streak = 0
        self.polling = True
        self._due: Optional[float] = None
        self._gen = 0
        self._latest: Optional[PlcPoll] = None
        plc.register_decoder((self.block,), self._decode, self._read_failed)

    # ───────── 설정 ─────────
    @property
    def allowed(self) -> bool:
        if self.unit == 1:
            return True
        return bool(self.cfg.get(f"{self.chamber.upper()}_SUPPORTS_DC2", False)) and \
            not bool(self.cfg.get(f"{self.chamber.upper()}_SUPPORTS_HEATER", False))

    def _c(self, k: str):
        return self.cfg[k]

    # ───────── 이벤트 ─────────
    def _emit(self, kind: str, detail: str = "", reason: Optional[str] = None, **data) -> None:
        if self.on_event:
            self.on_event(PowerEvent(kind, detail, reason, None, dict(data, unit=self.name)))

    def _status(self, msg: str) -> None:
        self._emit("status", msg)

    # ───────── 시작 ─────────
    def start(self, target_power: float, on_done: Optional[Callable[[Optional[str]], None]] = None) -> None:
        done = on_done or (lambda r: None)
        if not self.allowed:
            self._status(f"{self.name} 사용 불가(CH2_SUPPORTS_DC2 꺼짐 또는 히터 사용)")
            done("DEVICE_DISABLED")
            return
        if self.running:
            self._status("경고: DC 파워가 이미 동작 중입니다.")
            done("STATE_NOT_ALLOWED")
            return
        self.target_power = float(max(0.0, min(float(self._c("DC_MAX_POWER")), float(target_power))))
        self.low_power_streak = self.low_current_streak = self.overcurrent_streak = 0
        self._arm_status()
        self._gen += 1
        g = self._gen

        def set_done(res: Result) -> None:
            if g != self._gen:
                done("CANCELLED")
                return
            if res.status != Status.OK:
                self._status(f"DCV SET ON 실패: {err_text(res)}")
                done(res.reason or "DEVICE_REJECTED")
                return
            self._status("DCV SET ON")
            self.enabled = True
            self.running = True
            self._emit("state_changed", running=True)
            self.state = "MAINTAINING"
            self.sent_target_reached = False
            self._status(f"프로세스 시작. 목표: {self.target_power:.1f} W (직접 설정)")
            self.polling = True
            self._due = self.clock.monotonic()

            def sent(res2: Result) -> None:
                if res2.status == Status.OK and res2.value is not None:
                    self.last_sent = float(self.target_power)
                    self._status(f"목표 {self.target_power:.1f} W 즉시 전송")
                elif res2.status != Status.OK:
                    self._status(f"초기 전송 실패: {err_text(res2)}")
                self._adjust_once()
                done(None)
            self._write(self.target_power, sent, guard=self._guard(g))
        self.plc.power_enable(True, family="DCV", set_idx=self.u["set_idx"], source=self.source, on_done=set_done)

    def _arm_status(self) -> None:
        if not bool(self._c("DC_OVERCURRENT_ENABLE_CONT")):
            self._status("[overcurrent] ⚠ 인터락 OFF (settings.json common.DC_OVERCURRENT_ENABLE_CONT=false) — 과전류로 정지하지 않음")
            return
        tp = float(self.target_power or 0.0)
        lim = dc_overcurrent_limit_a(self.cfg, tp if tp > 0.0 else None)
        n = max(1, int(self._c("DC_OVERCURRENT_STREAK_N")))
        self._status(f"[overcurrent] 인터락 ON — 한계 {lim:.2f}A (설정 {tp:.0f}W, 연속 {n}회)")

    def _guard(self, g: int) -> Callable[[], bool]:
        return lambda: self.running and self._gen == g

    def _write(self, w: float, on_done, guard=None, kind: str = "write", not_before=None) -> None:
        self.plc.power_write(float(w), family="DCV", write_idx=self.u["write_idx"], source=self.source, on_done=on_done,
                             kind=kind, guard=guard, not_before=not_before)

    def set_process_status(self, active: bool) -> None:
        """폴링 on/off(v1 set_process_status) — 다시 켜면 다음 읽기부터."""
        self.polling = bool(active)
        if active and self._due is None:
            self._due = self.clock.monotonic()

    # ───────── 측정(1초 PLC 읽기 → DC_INTERVAL_MS 마다) ─────────
    def _decode(self, poll: PlcPoll) -> Dict[str, Any]:
        self._latest = poll
        vr, ir = poll.regs.get(self.u["v_reg"]), poll.regs.get(self.u["i_reg"])
        n = f"{self.block}.dc{self.unit}"
        out: Dict[str, Any] = {f"{n}_set_w": self.last_sent, f"{n}_target_w": self.target_power if self.running else None,
                               f"{n}_running": self.running, f"{n}_enable": poll.coils.get(self.u["set_coil"])}
        if vr is None or ir is None:
            out.update({f"{n}_p": None, f"{n}_v": None, f"{n}_i": None})
            return out
        p, v, i = dc_scale(self.cfg, vr, ir)
        out.update({f"{n}_p": round(p, 3), f"{n}_v": round(v, 3), f"{n}_i": round(i, 5)})
        if self.running and self.polling and self._due is not None and poll.t_mono >= self._due - 1e-6:
            self._due = poll.t_mono + max(50, int(self._c("DC_INTERVAL_MS"))) / 1000.0 - 0.5
            self._on_measure(p, v, i)
        return out

    def _read_failed(self, why: str) -> None:
        if self.running and self.polling and self._due is not None and self.clock.monotonic() >= self._due - 1e-6:
            self._due = self.clock.monotonic() + max(50, int(self._c("DC_INTERVAL_MS"))) / 1000.0 - 0.5
            self._emit("log", f"read failed: {why}")

    def update_measurements(self, power: float, voltage: float, current: float) -> None:
        """외부에서 측정을 넣을 때(v1 공개 API) — 판정 없이 표시·보정만."""
        if not self.running:
            return
        self.power_w, self.voltage_v, self.current_a = float(power or 0.0), float(voltage or 0.0), float(current or 0.0)
        self._emit("display", power=self.power_w, voltage=self.voltage_v, current=self.current_a)
        self._adjust_once()

    def _on_measure(self, p: float, v: float, i: float) -> None:
        self.power_w, self.voltage_v, self.current_a = float(p), float(v), float(i)
        self._emit("display", power=self.power_w, voltage=self.voltage_v, current=self.current_a)
        if self._check_overcurrent() or self._check_low_power() or self._check_low_current():
            self._adjust_once(committed=True)        # v1: 같은 측정의 보정 태스크가 정지보다 먼저 돈다
            self.stop()
            return
        self._adjust_once()

    def _check_overcurrent(self) -> bool:
        if not bool(self._c("DC_OVERCURRENT_ENABLE_CONT")):
            return False
        ia = float(self.current_a or 0.0)
        if ia != ia or ia in (float("inf"), float("-inf")):
            return False
        tp = float(self.target_power or 0.0)
        lim = dc_overcurrent_limit_a(self.cfg, tp if tp > 0.0 else None)
        if ia > lim:
            self.overcurrent_streak += 1
        else:
            self.overcurrent_streak = 0
            return False
        n = max(1, int(self._c("DC_OVERCURRENT_STREAK_N")))
        if self.overcurrent_streak < n:
            self._status(f"과전류 감시: {self.overcurrent_streak}/{n} (meas={ia:.3f}A > {lim:.3f}A)")
            return False
        msg = (f"DC 과전류: meas={ia:.3f}A > 한계 {lim:.3f}A (목표 {tp:.1f}W 기준, {self.overcurrent_streak}회 연속) "
               f"→ 공정 중단")
        scale = float(self._c("PLC_DC_I_SCALE"))
        if scale > 0.0:
            msg += f" | I_scale={scale:g} (raw≈{ia / scale:.0f})"
        self._status(msg)
        self._emit("target_failed", msg, reason="DC_OVERCURRENT")
        return True

    def _check_low_power(self) -> bool:
        p, th, n = float(self.power_w or 0.0), float(self._c("DC_LOW_W_THRESH")), max(1, int(self._c("DC_LOW_STREAK_N")))
        if p > th:
            self.low_power_streak = 0
            return False
        self.low_power_streak += 1
        self._status(f"저전력 감시: {self.low_power_streak}/{n} (meas={p:.1f}W ≤ {th:.1f}W)")
        if self.low_power_streak < n:
            return False
        self._status(f"DC 파워가 {n}회 연속 ≤ {th:.1f}W → 공정 중단")
        self._emit("target_failed", f"저전력 연속 {self.low_power_streak}/{n} (meas={p:.1f}W ≤ {th:.1f}W)",
                   reason="DC_LOW_POWER")
        return True

    def _check_low_current(self) -> bool:
        if not self.sent_target_reached:
            return False
        ia, th = float(self.current_a or 0.0), float(self._c("DC_LOW_CURRENT_THRESH_A"))
        n = max(1, int(self._c("DC_LOW_CURRENT_STREAK_N")))
        if ia > th:
            self.low_current_streak = 0
            return False
        self.low_current_streak += 1
        self._status(f"저전류 감시(목표 도달 후): {self.low_current_streak}/{n} (meas={ia:.3f}A ≤ {th:.3f}A)")
        if self.low_current_streak < n:
            return False
        self._status(f"DC 전류가 목표 도달 후 {n}회 연속 ≤ {th:.3f}A → 공정 중단")
        self._emit("target_failed", f"저전류 연속 {self.low_current_streak}/{n} (meas={ia:.3f}A ≤ {th:.3f}A)",
                   reason="DC_LOW_CURRENT")
        return True

    def _adjust_once(self, committed: bool = False) -> None:
        """유지 보정(device/dc_power.py:513-550). committed=True 면 실행 때 정지 여부를 다시 보지 않는다(이미 보낸 것)."""
        if not self.running:
            return
        error = float(self.target_power) - float(self.power_w)
        if abs(error) <= float(self._c("DC_TOLERANCE_POWER")):
            if not self.sent_target_reached:
                self._emit("target_reached")
                self.sent_target_reached = True
            return
        last = self.last_sent
        base = float(last if last is not None else self.target_power)
        step = float(self._c("DC_MAINTAIN_STEP")) * (1 if error > 0 else -1)
        new = max(0.0, min(float(self._c("DC_MAX_POWER")), base + step))
        if last is not None and abs(new - last) < float(self._c("DC_WATT_DEADBAND")):
            return
        meas, tgt = self.power_w, self.target_power

        def sent(res: Result) -> None:
            if res.status == Status.OK and res.value is not None:
                self.last_sent = float(new)
            elif res.status != Status.OK:
                self._status(f"DC 설정 전송 실패: {err_text(res)}")
            if res.value is not None or res.status != Status.OK:
                self._status(f"오차 보정: meas={meas:.1f}W, target={tgt:.1f}W → set {new:.1f}W")
        self._write(new, sent, guard=None if committed else self._guard(self._gen),
                    kind="safety" if committed else "write")

    # ───────── 정지 ─────────
    def used_this_run(self) -> bool:
        return bool(self.enabled) or (self.last_sent not in (None, 0.0))

    def stop(self, on_done: Optional[Callable[[Optional[str]], None]] = None) -> None:
        """정지(cleanup): 0 W 확인 → SET OFF. on_done(None)=완료, 'POWER_OFF_UNCONFIRMED'=0 W 미확인."""
        done = on_done or (lambda r: None)
        self._gen += 1
        self.running = False
        self.polling = False
        self._due = None
        self._emit("state_changed", running=False)
        self.state = "IDLE"
        self._status("DC 파워 ramp-down 시작")
        if not self.used_this_run():
            self._finish(done)
            return

        def write_zero(cb, nb):
            def z(res: Result) -> None:
                if res.status == Status.OK:
                    self.last_sent = 0.0
                cb(res)
            self._write(0.0, z, kind="safety", not_before=nb)

        def set_off(cb, nb):
            def o(res: Result) -> None:
                if res.status == Status.OK:
                    self.enabled = False
                cb(res)
            self.plc.power_enable(False, family="DCV", set_idx=self.u["set_idx"], source=self.source, on_done=o,
                                  kind="safety", not_before=nb)

        def zero_fail(n: int, el: float, err: str) -> None:
            self._status(f"DC 0W 쓰기 실패({n}회): {err}")
            self.output_off_unconfirmed = True
            msg = f"DC 0W 쓰기 실패 — 출력 상태 미확인 ({el:.1f}초, {n}회)"
            self._status(msg)
            self._emit("power_off_failed", msg, reason="POWER_OFF_UNCONFIRMED")

        def off_result(ok: bool, err: str) -> None:
            self._status("DCV SET OFF" if ok else f"DCV SET OFF 실패 — 0W 는 확인됨 ({err})")

        def fin(ok: bool) -> None:
            if ok:
                self._finish(done)
            else:
                done("POWER_OFF_UNCONFIRMED")
        ZeroThenOff(self.clock, float(self._c("POWER_OFF_ZERO_DEADLINE_S")), write_zero, set_off, self.enabled,
                    zero_fail, off_result, fin).start()

    cleanup = stop

    def _finish(self, done) -> None:
        self._status("DC 파워 ramp-down 완료 (snap-to-zero)")
        self.output_off_unconfirmed = False
        self._emit("power_off_finished")
        done(None)

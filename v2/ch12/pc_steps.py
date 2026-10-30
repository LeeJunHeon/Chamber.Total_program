"""플라즈마 클리닝 공정 순서 — v1 controller/plasma_cleaning_controller.py(PlasmaCleaningController) + 런타임이 넣던 장비 콜백.

PcController(v1 컨트롤러 그대로 — PC-040~058·099)
- 결과 last_result('success'/'stop'/'fail')·last_reason, 최초 실패만 보존하는 last_error_code/source/detail/meta(_set_failure).
- request_stop(사유): 실행 중일 때만 — 사유가 있고 fail 이 아니면 stop·사유. 정지 신호(stop_set)는 실행마다 새로(run 시작에서) —
  그래서 run 이 시작되기 전(프리플라이트 중)의 STOP 은 컨트롤러가 보지 못한다(v1 그대로, 런타임 판정은 런타임 쪽 플래그).
- on_mfc_confirmed·on_mfc_failed(실패 기록 + 중단 신호)·on_rf_failed.
- run(p): GV 인터락(3 s) → GV OPEN_SW 순간 → gv_open_lamp_delay_s → OPEN_LAMP → IG base 대기(STOP 동시) → 가스 선택 →
  (유량>0) FLOW_SET_ON + FLOW_ON 확정 대기(wait_timeout_s) → SP4_SET → SP4_ON → SP4 압력 대기(STOP 동시) → RF 시작(STOP 동시) →
  공정 카운트다운 total..0(total+1 초 — [이상] v1 그대로 D109). 예외는 실패로, 취소(PcCancelled)는 fail 이 아니면 stop.
  [이상] 카운트다운 중 MFC 실패는 루프가 'stop'·'사용자 STOP' 으로 덮는다(PC-099, D107 — v1 그대로).
StepIO(엔진이 섞어 씀 — 런타임 _bind_pc_controller 의 콜백들, PC-041~050)
- 장비 호출은 드라이버 동작(worker 요청) + on_done 을 시계 대기(Box)로 기다린다. v1 이 await 하던 곳에서 기다린다.
- RF 시작 판정: rf.start → 목표 이벤트 대기 max(PC_RF_TARGET_WAIT_TIMEOUT_S 120, RF_REFLECTED_WAIT_TIMEOUT_S 60 + 5) = 120 s.
  실패 문구(REF 과다·0 W·'timeout 60s' — [이상] 실제 120 s, PC-095·D103)는 v1 그대로.
  RF 실패(목표 실패·시간 초과·예외)는 on_rf_failed(중단 신호) → v1 은 그 신호로 컨트롤러가 RF 시작 태스크를 취소해 안쪽 안전 정지가
  첫 대기에서 끊긴다(로그 'STOP during RF START → cancel rf_start & shutdown'). v2 는 안쪽 정지를 'RF cleanup 요청'까지만 하고 같은
  로그를 남긴 뒤 취소 — 실제 정지 순서는 종료 정리(_final_cleanup)가 한다(v1 asyncio 취소 경합을 단순화 — 결정 후보).
"""
from __future__ import annotations

import traceback
from typing import Any, Callable, Dict, Optional

from v2.common.worker import Status
from v2.ch12.drivers import mfc_ops
from v2.ch12.drivers.mfc import GAS_MAP


class PcCancelled(BaseException):
    """v1 asyncio.CancelledError 자리(일반 except Exception 에 잡히지 않는다)."""


class Box:
    """콜백 결과를 시계 대기로 받는 칸."""

    def __init__(self, clock) -> None:
        self.clock = clock
        self.done = False
        self.value: Any = None

    def set(self, *value) -> None:
        with self.clock.cond:
            self.value = value if len(value) != 1 else value[0]
            self.done = True
            self.clock.cond.notify_all()


def err_of(res) -> str:
    return str(getattr(res, "error", None) or getattr(res, "reason", None) or getattr(res, "status", "") or "")


class PcController:
    def __init__(self, clock, log: Callable[[str, str], None]) -> None:
        self.clock = clock
        self._log = log
        self.is_running = False
        self.stop_set = False
        self.flow_on_set = False
        self.last_result = "success"
        self.last_reason = ""
        self.last_error_code: Optional[str] = None
        self.last_error_source = ""
        self.last_error_detail = ""
        self.last_error_meta: Dict[str, Any] = {}
        self.step = ""

    def _signal(self) -> None:
        self.clock.notify()

    # ───────── 외부 API(controller 138-260) ─────────
    def request_stop(self, reason: Optional[str] = None) -> None:
        if not self.is_running:
            return
        if reason and self.last_result != "fail":
            self.last_result, self.last_reason = "stop", reason
        self._log("PC", f"정지 요청 수신 ({reason})" if reason else "정지 요청 수신")
        self.stop_set = True
        self._signal()

    @staticmethod
    def normalize(reason, code=None, meta=None):
        err_code, detail, m = code, "", {}
        if isinstance(reason, BaseException):
            if err_code is None:
                err_code = getattr(reason, "code", None) or getattr(reason, "error_code", None)
            detail = getattr(reason, "detail", None) or getattr(reason, "message", None) or str(reason)
            src = getattr(reason, "meta", None)
            if isinstance(src, dict):
                m.update(src)
            m["cause_type"] = type(reason).__name__
        else:
            detail = str(reason or "").strip()
        if isinstance(meta, dict):
            m.update(meta)
        if not detail:
            detail = "unknown"
        if err_code is not None:
            err_code = str(err_code).strip().upper() or None
        return err_code, detail, m

    def set_failure(self, source: str, reason, *, code=None, meta=None, result: str = "fail") -> None:
        c, d, m = self.normalize(reason, code=code, meta=meta)
        self.last_result, self.last_reason = result, d
        if not self.last_error_detail:
            self.last_error_code, self.last_error_source = c, str(source or "").strip() or "PC"
            self.last_error_detail, self.last_error_meta = d, dict(m or {})

    def on_mfc_confirmed(self, cmd: str) -> None:
        self._log("MFC", f"명령 완료: {cmd}")
        if cmd == "FLOW_ON":
            self.flow_on_set = True
            self._signal()

    def on_mfc_failed(self, cmd: str, reason, *, code=None, meta=None) -> None:
        c, d, m = self.normalize(reason, code=code, meta={"cmd": cmd, **(meta or {})})
        self._log("MFC", f"명령 실패: {cmd} - {d}")
        if cmd == "FLOW_ON":
            self.flow_on_set = True
        self.set_failure("MFC", d, code=c, meta=m, result="fail")
        self.stop_set = True
        self._signal()

    def on_rf_failed(self, reason, *, code=None, meta=None) -> None:
        c, d, m = self.normalize(reason, code=code, meta=meta)
        self._log("RF", f"실패: {d}")
        self.set_failure("RF", d, code=c, meta=m, result="fail")
        self.stop_set = True
        self._signal()

    # ───────── 순서(controller 265-592) ─────────
    def _wait_stop_or(self, box: Box, deadline: Optional[float] = None) -> str:
        """box 끝 / 정지 신호 / 마감 중 먼저 온 것: 'done'·'stop'·'timeout'. 둘 다면 정지 우선(v1 은 stop_task 를 먼저 본다)."""
        self.clock.wait_until(deadline, lambda: box.done or self.stop_set)
        if self.stop_set:
            return "stop"
        return "done" if box.done else "timeout"

    def run(self, p, io) -> None:
        self.last_result, self.last_reason = "success", ""
        self.last_error_code, self.last_error_source, self.last_error_detail, self.last_error_meta = None, "", "", {}
        self.stop_set = False
        self.is_running = True
        ab = getattr(io, "abort_reason", None)
        if ab:                                         # v2 안전 중단(4단계)이 프리플라이트 중에 왔으면 지우지 않는다
            self.stop_set, self.last_result, self.last_reason = True, "stop", str(ab)
        io.show_state("Preparing…")
        self._log("PC", "플라즈마 클리닝 시작")
        self._log("STEP", (f"PARAMS gas_idx={p.gas_idx}, flow={p.gas_flow_sccm:.1f} sccm, "
                           f"IG_target={p.target_pressure:.3e} Torr, SP4={p.sp4_setpoint_mTorr:.2f} mTorr, "
                           f"RF={p.rf_power_w:.1f} W, time={p.process_time_min:.1f} min"))
        try:
            self._sequence(p, io)
            self.last_result, self.last_reason = "success", ""
        except PcCancelled:
            if self.last_result != "fail":
                self.last_result = "stop"
                if not self.last_reason:
                    self.last_reason = "사용자 STOP"
            self._log("PC", "CancelledError: 외부 STOP 또는 경쟁 종료로 중단")
        except Exception as e:
            self.set_failure("PC", e, result="fail")
            self._log("PC", f"오류: {e!r}\n{traceback.format_exc()}")
        finally:
            self._log("STEP", "종료 시퀀스 진입")
            self.is_running = False
            self.step = ""
            io.show_state("대기 중")
            self._log("PC", "플라즈마 클리닝 종료")

    def _sequence(self, p, io) -> None:
        clk = self.clock
        # 1) GV 인터락 → OPEN_SW → 램프
        self.step = "gv_interlock"
        self._log("PLC", "GV 인터락 확인")
        ok = io.plc_check_gv_interlock()
        self._log("PLC", f"GV 인터락={ok}")
        if not ok:
            raise RuntimeError("게이트밸브 인터락 FALSE")
        self.step = "gv_open"
        self._log("PLC", "GV OPEN_SW 실행")
        io.plc_gv_open()
        clk.wait_until(clk.monotonic() + max(0.0, float(p.gv_open_lamp_delay_s)))
        lamp = io.plc_read_gv_open_lamp()
        self._log("PLC", f"GV OPEN_LAMP={lamp}")
        if not lamp:
            raise RuntimeError("GV OPEN_LAMP가 TRUE가 아님(오픈 실패?)")
        # 2) IG base 대기
        self.step = "ig_base"
        interval_ms = int(p.ig_interval_ms or 10_000)
        self._log("IG", f"IG.wait_for_base_pressure: {p.target_pressure:.3e} Torr 대기 "
                        f"(RDI={interval_ms / 1000.0:.1f}s 간격, 외부 폴링 없음)")
        io.show_state("IG base wait…")
        box = io.ig_wait_for_base_torr(p.target_pressure, interval_ms)
        if self._wait_stop_or(box) == "stop":
            raise PcCancelled()
        if not box.value:
            raise RuntimeError("Base pressure not reached (IG API)")
        io.show_state("Base pressure OK")
        # 3) 가스·SP4
        self._log("STEP", "3: Gas/Pressure 설정 시작")
        self.step = "gas_select"
        try:
            io.show_state(f"Gas select ch{p.gas_idx}")
            io.mfc_gas_select(p.gas_idx)
            if self.stop_set:
                raise PcCancelled()
        except Exception as e:
            self._log("MFC", f"GAS SELECT 실패: {e!r}")
            raise
        if p.gas_flow_sccm > 0.0:
            self.step = "gas_flow"
            self._log("MFC", f"FLOW_SET_ON start ch={p.gas_idx} -> {p.gas_flow_sccm:.1f} sccm")
            try:
                self.flow_on_set = False
                io.mfc_flow_set_on(p.gas_flow_sccm)
                if self.stop_set:
                    raise PcCancelled()
                io.show_state(f"Gas Flow {p.gas_flow_sccm:.1f} sccm (stabilizing)")
                clk.wait_until(clk.monotonic() + float(p.wait_timeout_s), lambda: self.flow_on_set or self.stop_set)
                if not (self.flow_on_set or self.stop_set):
                    msg = f"Gas Flow가 {p.wait_timeout_s:.1f}s 내에 안정적으로 setpoint에 도달하지 못했습니다."
                    self._log("MFC", msg)
                    raise RuntimeError(msg)
                if self.stop_set:
                    raise PcCancelled()
                io.show_state(f"Gas Flow {p.gas_flow_sccm:.1f} sccm OK")
            except Exception as e:
                self._log("MFC", f"FLOW_SET_ON 실패: {e!r}")
                raise
        else:
            self._log("MFC", "FLOW_SET_ON 스킵(flow=0.0)")
        self.step = "sp4_set"
        self._log("MFC", f"SP4_SET -> {p.sp4_setpoint_mTorr:.2f} mTorr")
        try:
            io.mfc_sp4_set(p.sp4_setpoint_mTorr)
            io.show_state(f"SP4 Set {p.sp4_setpoint_mTorr:.2f} mTorr")
            if self.stop_set:
                raise PcCancelled()
        except Exception as e:
            self._log("MFC", f"SP4_SET 실패: {e!r}")
            raise
        self._log("MFC", "SP4_ON")
        try:
            io.mfc_sp4_on()
            if self.stop_set:
                raise PcCancelled()
        except Exception as e:
            self._log("MFC", f"SP4_ON 실패: {e!r}")
            raise
        self.step = "sp4_wait"
        self._log("MFC", f"SP4_ON → 압력 안정화 대기 시작 (target={p.sp4_setpoint_mTorr:.2f} mTorr, "
                         f"timeout={p.wait_timeout_s:.1f}s)")
        io.show_state(f"SP4 Pressure Wait {p.sp4_setpoint_mTorr:.2f} mTorr")
        box = io.mfc_wait_sp4_pressure(p.sp4_setpoint_mTorr, p.wait_timeout_s)
        if self._wait_stop_or(box) == "stop":
            self._log("STEP", "STOP during SP4 pressure wait → abort before RF")
            io.cancel_sp4_wait()
            raise PcCancelled()
        if not box.value:
            msg = (f"SP4 압력이 목표에 도달하지 못했습니다 (target={p.sp4_setpoint_mTorr:.2f} mTorr, "
                   f"timeout={p.wait_timeout_s:.1f}s)")
            self._log("MFC", msg)
            raise RuntimeError(msg)
        self._log("MFC", "SP4 작업압 안정화 완료")
        # 4) RF
        self.step = "rf_start"
        self._log("RF", f"RF Power 적용: {p.rf_power_w:.1f} W")
        io.show_state(f"RF Start {p.rf_power_w:.1f} W")
        try:
            io.rf_start(p.rf_power_w, lambda: self.stop_set)
        except PcCancelled:
            self._log("STEP", "STOP during RF START → cancel rf_start & shutdown")
            raise
        except Exception as e:
            self._log("RF", f"RF START 실패: {e!r}")
            raise
        self._log("RF", "RF START OK")
        # 5) 공정 카운트다운
        self.step = "process"
        io.show_state(f"PROCESS {p.process_time_min:.1f} min")
        total = int(max(0, p.process_time_min * 60.0))
        start_left = io.resume_left(total)
        for left in range(start_left, -1, -1):
            if self.stop_set:
                self._log("STEP", f"STOP 이벤트 감지 → 남은 {left}s 시점에서 종료 진입")
                self.last_result = "stop"
                self.last_reason = io.stop_reason_word()
                raise PcCancelled()
            io.show_countdown(left)
            clk.wait_until(clk.monotonic() + 1.0)


class StepIO:
    """엔진(PlasmaCleaning)이 섞어 쓰는 장비 콜백 — self.clock·self.dev·self.log·self.ch·self.cfg·self.pc(PcController) 를 쓴다."""

    # ── PLC ──
    def _plc_read_bit(self, key: str, timeout_s: Optional[float]):
        """(값 또는 None, 실패 문구 또는 None, 시간 초과)."""
        box = Box(self.clock)
        self.dev.plc.read_bit(key, on_done=box.set)
        dl = None if timeout_s is None else self.clock.monotonic() + float(timeout_s)
        self.clock.wait_until(dl, lambda: box.done)
        if not box.done:
            return None, None, True
        res = box.value
        if res.status != Status.OK:
            return None, err_of(res), False
        return bool(res.value), None, False

    def plc_check_gv_interlock(self) -> bool:
        if self.dev.plc is None:
            return True
        key = f"G_V_{self.ch}_인터락"
        v, err, to = self._plc_read_bit(key, 3.0)
        if to:
            self.log("PLC", f"GV 인터락 read timeout (3s) — lock 경합 추정: {key}")
            return False
        if err is not None:
            raise RuntimeError(err)
        return bool(v)

    def plc_gv_open(self) -> None:
        if self.dev.plc is None:
            return
        box = Box(self.clock)
        self.dev.plc.gate_valve(self.ch, open=True, momentary=True, on_done=box.set)
        self.clock.wait_until(None, lambda: box.done)
        if box.value is not None and box.value.status != Status.OK:
            raise RuntimeError(err_of(box.value))

    def plc_read_gv_open_lamp(self) -> bool:
        if self.dev.plc is None:
            return True
        v, err, to = self._plc_read_bit(f"G_V_{self.ch}_OPEN_LAMP", None)
        if err is not None:
            raise RuntimeError(err)
        return bool(v)

    # ── IG ──
    def ig_wait_for_base_torr(self, target_torr: float, interval_ms: int) -> Box:
        if self.dev.ig is None:
            raise RuntimeError("IG not bound")
        box = Box(self.clock)
        self.dev.ig.wait_for_base_pressure(float(target_torr), interval_ms=int(interval_ms),
                                           on_done=lambda k, r, d: box.set(r is None))
        return box

    # ── MFC ──
    def _mfc_wait(self, start: Callable[[Callable], None]) -> None:
        box = Box(self.clock)
        start(lambda k, r, d: box.set((k, r, d)))
        self.clock.wait_until(None, lambda: box.done)

    def mfc_gas_select(self, gas_idx: int) -> None:
        m = self.dev.mfc_gas
        if m is None:
            raise RuntimeError("mfc_gas not bound")
        gi = int(gas_idx)
        self.gas_idx = gi
        self.log("PC", f"GasFlow → {self.mfc_name(m)} ch{gi}")
        m.gas_select(gi, owner=self.mfc_owner())

    def mfc_flow_set_on(self, flow_sccm: float) -> None:
        m = self.dev.mfc_gas
        if m is None:
            raise RuntimeError("mfc_gas not bound")
        ch = self.gas_idx if self.gas_idx is not None else 3
        ui = float(max(0.0, flow_sccm))
        self.log("MFC", f"FLOW_SET_ON(sel ch={ch}) -> {ui:.1f} sccm")
        owner = self.mfc_owner()
        box = Box(self.clock)
        t0 = self.clock.monotonic()

        def stab_started() -> bool:                 # v1 await flow_set_on 이 돌아오는 시점 = 안정화 작업 등록
            job = m.stab_jobs.get(int(m.selected_ch or 0))
            return job is not None and job.owner == owner and job.created >= t0
        m.flow_set_on(ui, owner=owner, on_done=lambda k, r, d: box.set((k, r, d)))
        self.clock.wait_until(None, lambda: box.done or stab_started())

    def mfc_sp4_set(self, mtorr: float) -> None:
        m = self.dev.mfc_pressure
        if m is None:
            raise RuntimeError("mfc_pressure not bound")
        self._mfc_wait(lambda cb: m.sp_set(4, float(mtorr), owner=self.mfc_owner(), on_done=cb))

    def mfc_sp4_on(self) -> None:
        m = self.dev.mfc_pressure
        if m is None:
            raise RuntimeError("mfc_pressure not bound")
        self._mfc_wait(lambda cb: m.sp_on(4, owner=self.mfc_owner(), on_done=cb))

    def mfc_wait_sp4_pressure(self, target_mtorr: float, timeout_s: float) -> Box:
        """wait_for_pressure_reached(target, timeout_sec=…) 직접 호출(명령 이벤트 없음 — device/mfc.py:894-982)."""
        box = Box(self.clock)
        m = self.dev.mfc_pressure
        if m is None:
            self.log("MFC", "[SP4 WAIT] mfc_pressure not bound")
            box.set(False)
            return box
        target, timeout = float(target_mtorr), float(timeout_s)
        self.log("MFC", f"[SP4 WAIT] target={target:.3f} mTorr, timeout={timeout:.1f}s")
        out: Dict[str, Any] = {}
        interval = float(m.c("MFC_PRESSURE_CHECK_INTERVAL_SEC", 1.0))

        def gen():
            ok, last = yield from mfc_ops._wait_reached(m, target, timeout, interval, self.mfc_owner())
            out["ok"], out["last"] = ok, last

        def fin(op, reason, detail):
            if reason is not None:
                self.log("MFC", f"[SP4 WAIT] 예외: {detail or reason!r}")
                box.set(False)
                return
            ok, last = bool(out.get("ok")), out.get("last")
            if last is not None:
                self.log("MFC", f"[SP4 WAIT] 결과 ok={ok}, last={float(last):.3f} mTorr")
            else:
                self.log("MFC", f"[SP4 WAIT] 결과 ok={ok}")
            box.set(ok)
        self._sp4_op = m.runner.start("WAIT_PRESSURE", gen(), fin, kind="read")
        return box

    def cancel_sp4_wait(self) -> None:
        op, m = getattr(self, "_sp4_op", None), self.dev.mfc_pressure
        if op is not None and m is not None and not op.done:
            m.runner.cancel(op, "CANCELLED", "")

    # ── RF ──
    def rf_start(self, power_w: float, stop_pred: Callable[[], bool]) -> None:
        rf = self.dev.rf
        if rf is None:
            return
        with self.clock.cond:
            self.rf_target_ok, self.rf_target_reason, self.rf_target_evt = None, "", False
        last_fwd = last_ref = None
        box = Box(self.clock)
        rf.start(float(power_w), on_done=box.set)
        self.clock.wait_until(None, lambda: box.done or stop_pred())
        if stop_pred():
            raise PcCancelled()
        cfg = self.cfg
        wait_s = float(cfg.get("PC_RF_TARGET_WAIT_TIMEOUT_S", 60.0))
        extra = float(cfg.get("PC_RF_TARGET_WAIT_EXTRA_S", 5.0))
        try:
            wait_s = max(wait_s, float(rf.c("RF_REFLECTED_WAIT_TIMEOUT_S")) + extra)
        except Exception:
            pass
        self.clock.wait_until(self.clock.monotonic() + wait_s, lambda: self.rf_target_evt or stop_pred())
        if not self.rf_target_evt and stop_pred():
            raise PcCancelled()
        if self.rf_target_evt:
            if self.rf_target_ok is False:
                reason = self.rf_target_reason or "RF 목표 파워 실패"
                self.log("RF", f"RF START 실패 확정: {reason}")
                self.forced_fail, self.forced_fail_reason = True, reason
                self._rf_fail_cancel()
            if self.rf_target_ok is True:
                self.log("RF", "목표 파워 안정 → 프로세스 타이머 시작 가능")
                self.process_timer_active = True
                return
            reason = "RF START 실패: target 이벤트 수신했지만 결과 플래그 없음"
            self.log("RF", reason)
            self.pc.on_rf_failed(reason, meta={"stage": "_rf_start", "power_w": float(power_w),
                                               "selected_ch": self.ch, "last_fwd": last_fwd, "last_ref": last_ref})
            self._rf_fail_cancel()
        # 시간 초과 — FWD/REF 읽어 사유 분류(869-929)
        rbox = Box(self.clock)
        if self.dev.plc is not None:
            self.dev.plc.rf_read_fwd_ref(rf_ch=1, on_done=rbox.set)
            self.clock.wait_until(None, lambda: rbox.done)
            res = rbox.value
            if res is not None and res.status == Status.OK and isinstance(res.value, dict):
                last_fwd = float(res.value.get("forward", 0.0) or 0.0)
                last_ref = float(res.value.get("reflected", 0.0) or 0.0)
        ref_th = float(cfg.get("PC_RF_FAIL_REF_THRESHOLD_W", 20.0))
        try:
            ref_th = float(rf.c("RF_REFLECTED_THRESHOLD_W"))
        except Exception:
            pass
        req = float(power_w)
        if last_ref is not None and last_ref > ref_th:
            reason = (f"RF Ref.p(REF) 과다로 파워 도달 실패: REF={last_ref:.1f}W > TH={ref_th:.1f}W "
                      f"(FWD={0.0 if last_fwd is None else last_fwd:.1f}W, SET={req:.1f}W)")
        elif last_fwd is not None and last_fwd <= 0.5:
            reason = "RF Power 도달 실패: 장비 OFF/SET 미설정(0W 지속/응답 없음)"
        elif last_fwd is not None and last_ref is not None:
            reason = f"RF Power 도달 실패: 목표({req:.1f}W) 미도달, timeout 60s (FWD={last_fwd:.1f}W, REF={last_ref:.1f}W)"
        elif last_fwd is not None:
            reason = f"RF Power 도달 실패: 목표({req:.1f}W) 미도달, timeout 60s (FWD={last_fwd:.1f}W)"
        else:
            reason = f"RF Power 도달 실패: 목표({req:.1f}W) 미도달, timeout 60s"
        self.log("RF", reason)
        self.forced_fail, self.forced_fail_reason = True, reason
        self.pc.on_rf_failed(reason, meta={"stage": "_rf_start", "power_w": req, "selected_ch": self.ch,
                                           "last_fwd": last_fwd, "last_ref": last_ref})
        self._rf_fail_cancel()

    def _rf_fail_cancel(self) -> None:
        """RF 실패 → 타이머 끔 → 안전 정지 시작(rf cleanup 요청) → 컨트롤러가 취소(위 독스트링)."""
        self.process_timer_active = False
        if self.dev.rf is not None:
            self.dev.rf.stop()
        raise PcCancelled()

    # ── 화면 ──
    def show_state(self, text: str) -> None:
        self.state_header = text or ""
        self.set_state_text(self.state_header)

    def show_countdown(self, sec: int) -> None:
        try:
            sec = int(sec)
        except Exception:
            sec = 0
        mm, ss = divmod(max(sec, 0), 60)
        tail = f"{mm:02d}:{ss:02d}" if mm else f"{ss:02d}s"
        full = f"{tail} ({max(sec, 0) / 60.0:.2f} min)"
        self.set_state_text(f"{self.state_header} · {full}" if self.state_header else full)
        self.countdown_left = sec
        if self.process_timer_active:
            self.ui["process_time"] = tail

    @staticmethod
    def gas_name(gas_idx: Optional[int]) -> str:
        return GAS_MAP.get(int(gas_idx or 3), "N2")

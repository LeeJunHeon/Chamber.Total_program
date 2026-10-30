"""플라즈마 클리닝 — 엔진 계약(1초 행 pc.proc.*·pc.set.*, 상태, 저널, 이어가기 확인).

1초 행: pc.proc.state(idle·preflight·run·test·stopping), 실행 중이면 run_id·owner(web/local/robot)·block_type(단계 id)·block_label·
  block_no/count(STEPS)·block_elapsed_s·remain_s(클리닝 카운트다운 남은 초). ETA·min_total_s 는 v1 에 없어 비움(null).
  pc.set.*(실공정만): rf_w·{가스}_sccm(gas_idx 3 = N2)·pressure_mtorr(SP4)·time_s.
저널(state_for_journal): 채널·run id·소유자·단계·파라미터·화면 값·불러온 행·카운트다운 남은 초 등(JSON 값만).
이어가기(resume_checks): 상태를 유지하는 블록은 메인 클리닝 시간 대기(step 'process')뿐 — 그 단계만 가능(나머지·TEST 는 불가).
  가능하면 다시 읽은 1초 행으로 power_output(RF SET 래치 ON·FWD 가 목표 ±10%)·gas(MFC1 선택 가스 ON)·pressure(SP4 압력이 MFC
  허용 범위 안 — mfc_ops.within)를 본다. 판정 규칙은 5단계 resume.py 와 맞춰 바꿀 수 있는 자리(결정 후보).
"""
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple

from v2.ch12 import pc_recipe as R
from v2.ch12.drivers import mfc_ops

STEPS = (("preflight", "프리플라이트"), ("gv_interlock", "GV 인터락"), ("gv_open", "GV 열기"), ("ig_base", "IG base 대기"),
         ("gas_select", "가스 선택"), ("gas_flow", "가스 유량"), ("sp4_set", "SP4 설정"), ("sp4_wait", "SP4 압력 대기"),
         ("rf_start", "RF 시작"), ("process", "클리닝"), ("cleanup", "종료 정리"))
STEP_NO = {k: i + 1 for i, (k, _) in enumerate(STEPS)}
STEP_LABEL = dict(STEPS)
RESUMABLE_STEPS = frozenset({"process"})            # 상태를 유지하는 블록 = 메인 클리닝 시간 대기뿐
COOLDOWN_S = 60.0                                   # runtime_state.pc_block_reason(ch, cooldown_s=60.0) — 1244


class ViewIO:
    # ───────── 엔진 계약(system.register_engine) ─────────
    @property
    def busy(self) -> bool:
        return bool(self.running)

    def _set_step(self, step: str) -> None:
        self._last_step = step
        self._step_t0 = self.clock.monotonic() if step else None

    def _current_step(self) -> str:
        if self.phase in ("run",) and self.pc.step:
            return self.pc.step
        return self._last_step

    def on_tick_start(self, tick=None) -> None:
        st = self._current_step()
        if st != getattr(self, "_seen_step", None):
            self._seen_step = st
            self._seen_t0 = self.clock.monotonic()

    def on_row(self, view) -> None:
        self.last_view = view

    def snapshot_fields(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"pc.proc.state": self.phase, "pc.proc.paused": False if self.running else None}
        if not self.running:
            return out
        st = self._current_step()
        t0 = getattr(self, "_seen_t0", None)
        out.update({"pc.proc.run_id": self.run_id, "pc.proc.owner": self.owner_src, "pc.proc.block_type": st or None,
                    "pc.proc.block_label": STEP_LABEL.get(st), "pc.proc.block_no": STEP_NO.get(st),
                    "pc.proc.block_count": len(STEPS),
                    "pc.proc.block_elapsed_s": None if t0 is None else round(self.clock.monotonic() - t0, 1),
                    "pc.proc.remain_s": float(self.countdown_left) if (st == "process" and self.countdown_left is not None)
                    else None})
        p = self.params
        if p is not None and not self.test_mode_active and self.phase == "run":
            gas = {1: "ar", 2: "o2", 3: "n2"}.get(int(p.gas_idx), "n2")
            out.update({"pc.set.rf_w": float(p.rf_power_w), f"pc.set.{gas}_sccm": float(p.gas_flow_sccm),
                        "pc.set.pressure_mtorr": float(p.sp4_setpoint_mTorr),
                        "pc.set.time_s": float(p.process_time_min) * 60.0})
        return out

    def state(self) -> Dict[str, Any]:
        return {"running": self.running, "phase": self.phase, "ch": self.ch, "step": self._current_step() or None,
                "test_mode": self.test_mode_active, "state_text": self.state_text, "ui": dict(self.ui),
                "fwd": self.ui_fwd, "ref": self.ui_ref, "countdown_left": self.countdown_left,
                "run_id": self.run_id if self.running else None, "owner": self.owner_src if self.running else None,
                "stop_requested": self.stop_requested, "last_final": self.last_final,
                "cooldown_s": self.run_state.pc_block_reason(self.ch, COOLDOWN_S)[1],
                "error": self.run_state.last_error("pc", self.ch), "loaded_row": self.loaded_row,
                "test_loaded": R.test_mode_of(self.loaded_row)[0]}

    def state_for_journal(self) -> Dict[str, Any]:
        return {"running": self.running, "phase": self.phase, "ch": self.ch, "run_id": self.run_id,
                "owner": self.owner_src, "by": self.by, "origin": self.origin, "test_mode": self.test_mode_active,
                "step": self._current_step(), "params": self.params.as_dict() if self.params else None,
                "ui": dict(self.ui), "loaded_row": self.loaded_row, "countdown_left": self.countdown_left,
                "process_timer_active": self.process_timer_active, "gas_idx": self.gas_idx,
                "last_process_time_min": self.last_process_time_min, "process_name": self.process_name,
                "readings_n": {k: len(v) for k, v in self.readings.items()}}

    def resume_checks(self, state: Optional[Mapping[str, Any]]) -> List[Tuple[str, bool, str]]:
        """이어가기 전 확인 — [(id, 맞음, 문구)]. 메인 클리닝 시간 대기(step 'process', TEST 아님)만 이어가기 가능."""
        want = dict(state or {})
        step = str(want.get("step") or "")
        out = [("resumable_step", step in RESUMABLE_STEPS and not want.get("test_mode"),
                f"단계 {step or '-'}({STEP_LABEL.get(step, '-')}) — 이어가기 가능 단계는 클리닝 시간 대기뿐")]
        if not out[0][1]:
            return out
        p = dict(want.get("params") or {})
        v = self.last_view
        ch = 2 if int(want.get("ch") or 1) == 2 else 1

        def val(col):                                 # 번갈아 읽는 값(MFC R60/R5)은 3 s 안의 마지막 값까지
            if v is None:
                return None
            x = v.value(col)
            if x is None and v.last(col) is not None and (v.age(col) or 0.0) <= 3.0:
                x = v.last(col)
            return x
        tgt = float(p.get("rf_power_w") or 0.0)
        fwd, en = val("pc.rf.fwd_w"), val("pc.rf.enable")
        ok = en is True and fwd is not None and tgt > 0 and abs(float(fwd) - tgt) <= max(1.0, 0.1 * tgt)
        out.append(("power_output", ok, f"RF SET {en} · FWD {fwd} W (목표 {tgt:.1f} W ±10%)"))
        flow = float(p.get("gas_flow_sccm") or 0.0)
        if flow > 0:
            gas = {1: "ar", 2: "o2", 3: "n2"}.get(int(p.get("gas_idx") or 3), "n2")
            on = val(f"ch1.mfc.{gas}_on")
            out.append(("gas", on is True, f"MFC1 {gas.upper()} ON {on}"))
        else:
            out.append(("gas", True, "유량 0 — 가스 확인 없음"))
        sp, pm = float(p.get("sp4_setpoint_mTorr") or 0.0), val(f"ch{ch}.mfc.pressure_mtorr")
        m = self.dev.mfc.get(f"ch{ch}")
        p_ok = pm is not None and m is not None and mfc_ops.within(m, sp, float(pm))
        out.append(("pressure", bool(p_ok), f"SP4 {sp:.2f} mTorr · 압력 {pm} mTorr"))
        return out


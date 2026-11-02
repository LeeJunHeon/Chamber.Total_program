"""플라즈마 클리닝 — 엔진 계약(1초 행 pc.proc.*·pc.set.*, 상태, 저널, 이어가기 확인).

1초 행: pc.proc.state(idle·preflight·run·test·stopping), 실행 중이면 run_id·owner(web/local/robot)·block_type(단계 id)·block_label·
  block_no/count(STEPS)·block_elapsed_s·remain_s(클리닝 카운트다운 남은 초). ETA·min_total_s 는 v1 에 없어 비움(null).
  pc.set.*(실공정만): rf_w·{가스}_sccm(gas_idx 3 = N2)·pressure_mtorr(SP4)·time_s.
저널(state_for_journal): 채널·run id·소유자·단계·파라미터·화면 값·불러온 행·카운트다운 남은 초 등(JSON 값만).
이어가기(사용자 결정 2026-11-02 D94 — CH1·CH2·TSP 와 같은 규칙, 판단은 resume.py): 정지 순서 도중이면 안전 정지를 끝까지, 기다리는 중
  (step 'process' 클리닝 시간·TEST)이면 남은 만큼, 장비 설정 쓰기 도중(gv_interlock~rf_start)이면 그 묶음(PC_BLOCK_START)을 처음부터
  다시 쓴다. 켜짐/꺼짐(RF SET·GV 인터락·장비 오류 없음)만 보고 값(FWD·압력) 허용 폭은 보지 않는다(D79).
"""
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple

from v2.ch12 import pc_recipe as R

STEPS = (("preflight", "프리플라이트"), ("gv_interlock", "GV 인터락"), ("gv_open", "GV 열기"), ("ig_base", "IG base 대기"),
         ("gas_select", "가스 선택"), ("gas_flow", "가스 유량"), ("sp4_set", "SP4 설정"), ("sp4_wait", "SP4 압력 대기"),
         ("rf_start", "RF 시작"), ("process", "클리닝"), ("cleanup", "종료 정리"))
STEP_NO = {k: i + 1 for i, (k, _) in enumerate(STEPS)}
STEP_LABEL = dict(STEPS)
RESUMABLE_STEPS = frozenset({"process"})            # 기다리는 단계(hold) = 메인 클리닝 시간 대기
# 장비 설정 쓰기 도중이면 그 묶음을 처음부터 다시 쓴다(D94) — 단계 → 다시 시작할 묶음(pc_steps.SEQ_ORDER)
PC_BLOCK_START = {"gv_interlock": "gv_interlock", "gv_open": "gv_interlock", "ig_base": "ig_base", "gas_select": "gas_select",
                  "gas_flow": "gas_select", "sp4_set": "sp4_set", "sp4_wait": "sp4_set", "rf_start": "rf_start"}
COOLDOWN_S = 60.0                                   # runtime_state.pc_block_reason(ch, cooldown_s=60.0) — 1244


class ViewIO:
    # ───────── 엔진 계약(system.register_engine) ─────────
    @property
    def busy(self) -> bool:
        return bool(self.running or self._restart_pending)

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

    def _resume_mode(self) -> tuple:
        """재시작 뒤 이어가기 방식(D94 — CH1·CH2·TSP 와 같은 규칙) → (방식, 시작 묶음).
        stop_sequence = 정지 순서 도중(STOP·중단·종료 정리) · hold = 기다리는 중(클리닝 시간·TEST) ·
        rewrite = 장비 설정 쓰기 도중(그 묶음을 처음부터 다시 — 묶음 = PC_BLOCK_START) · none = 시작 전(프리플라이트)."""
        if not self.running or (self.params is None and not self.test_mode_active):
            return "none", None                           # 시작 전(프리플라이트 — 아직 장비를 바꾸지 않음)
        step = self._current_step()
        if (self.stop_requested or self.pc.stop_set or self.cleanup_started or self.abort_reason or self.forced_fail
                or self.phase == "stopping" or step == "cleanup"):
            return "stop_sequence", None
        if self.test_mode_active or self.phase == "test":
            return "hold", None
        if step == "process":
            return "hold", "process"
        if step in PC_BLOCK_START:
            return "rewrite", PC_BLOCK_START[step]
        return "none", None

    def state_for_journal(self) -> Dict[str, Any]:
        mode, begin = self._resume_mode()
        return {"running": self.running, "phase": self.phase, "ch": self.ch, "run_id": self.run_id,
                "owner": self.owner_src, "by": self.by, "origin": self.origin, "test_mode": self.test_mode_active,
                "step": self._current_step(), "params": self.params.as_dict() if self.params else None,
                "ui": dict(self.ui), "loaded_row": self.loaded_row, "countdown_left": self.countdown_left,
                "process_timer_active": self.process_timer_active, "gas_idx": self.gas_idx,
                "last_process_time_min": self.last_process_time_min, "process_name": self.process_name,
                "readings_n": {k: len(v) for k, v in self.readings.items()},
                "resume_mode": mode, "resume_begin": begin, "resumable": mode in ("hold", "rewrite"),
                "test_remaining_s": self.test_remaining_s}

    def resume_checks(self, state: Optional[Mapping[str, Any]]) -> List[Tuple[str, bool, str]]:
        """이어가기 전 확인 — [(id, 맞음, 문구)]. 방식만(장비 켜짐/꺼짐은 resume.py 가 1초 행으로 본다 — 값 허용 폭 없음, D79)."""
        want = dict(state or {})
        mode, step = want.get("resume_mode"), str(want.get("step") or "")
        return [("resumable_step", mode in ("hold", "rewrite"),
                 f"단계 {step or '-'}({STEP_LABEL.get(step, '-')}) — 방식 {mode or '-'}")]

"""챔버12 공정 엔진 — 보기·기록 쪽(ProcessEngine 믹스인): 블록 추적(블록 시작·끝 ProcEvent "block", 실행 순서 executed),
1초 행 열(<c>.proc.*·<c>.set.* — snapshot_fields.EXTRA_FIELDS 의 공정 맥락·목표값, 확정 전 ETA 는 null), 상태(state_dict),
저널(state_for_journal — 5단계 journal.py 가 1초마다 싣는 값)·이어가기 자리(resume_from — 5단계 resume.py).
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from v2.ch12 import blocks as BK


class ProcessViewMixin:
    # ───────── 블록 추적(StepRunner.on_step) ─────────
    def on_step(self, idx: int, step, shutdown: bool) -> None:
        if shutdown:
            return
        run = self.runs[self.run_idx]
        inst = self.cur_insts[step.block] if 0 <= step.block < len(self.cur_insts) else None
        if inst is None or inst is self.cur_inst:
            return
        if self.cur_inst is not None:
            self._block_end("done")
        self.cur_inst = inst
        gi = None if inst.auto else run.first + inst.index
        self._block_begin(gi, inst)

    def _block_begin(self, gi: Optional[int], inst=None) -> None:
        self.cur_block = gi
        self.block_t0 = self.clock.monotonic()
        b = self.blocks[gi] if gi is not None and 0 <= gi < len(self.blocks) else None
        rec = {"no": self.block_no(), "type": (inst.type if inst else b["type"] if b else None),
               "label": (inst.label if inst else (b or {}).get("label", "")), "auto": bool(inst and inst.auto),
               "start_t": self.clock.wall(), "result": None}
        self.executed.append(rec)
        self.emit("block", f"{rec['no']} {rec['type']} 시작", data=dict(rec, phase="start", run_id=self.run_id))

    def _block_end(self, result: str) -> None:
        if self.executed and self.executed[-1]["result"] is None:
            rec = self.executed[-1]
            rec["result"], rec["end_t"] = result, self.clock.wall()
            self.emit("block", f"{rec['no']} {rec['type']} {result}", data=dict(rec, phase="end", run_id=self.run_id))

    def block_no(self) -> Optional[int]:
        if self.cur_block is None:
            return len(self.blocks) + 1 if self.cur_inst is not None else None
        return self.cur_block + 1 if self.cur_block >= 0 else None

    # ═══════════════════════ 1초 행 열 ═══════════════════════
    def snapshot_fields(self) -> Dict[str, Any]:
        c = f"{self.ch}."
        out: Dict[str, Any] = {c + "proc.state": self.state_name, c + "proc.paused": bool(self.paused)}
        if self.state_name == "IDLE" and not self.runner.is_running:
            return out
        inst = self.cur_inst
        btype = inst.type if inst else (self.blocks[self.cur_block]["type"]
                                        if isinstance(self.cur_block, int) and 0 <= self.cur_block < len(self.blocks) else None)
        out.update({c + "proc.run_id": self.run_id, c + "proc.owner": self.owner,
                    c + "proc.recipe_rev": self.seq.rev if self.seq else 0, c + "proc.block_no": self.block_no(),
                    c + "proc.block_count": len(self.blocks), c + "proc.block_type": btype,
                    c + "proc.block_label": inst.label if inst else None,
                    c + "proc.block_elapsed_s": None if self.block_t0 is None else round(self.clock.monotonic() - self.block_t0, 1),
                    c + "proc.remain_s": self.runner.remaining_s(), c + "proc.eta_remaining_s": self.eta_remaining_s(),
                    c + "proc.min_total_s": self.min_total_s()})
        out.update(self._targets(c))
        return out

    def _targets(self, c: str) -> Dict[str, Any]:
        p = self.params or {}
        t: Dict[str, Any] = {}

        def put(name, v):
            if v is not None:
                t[c + "set." + name] = v
        ov = self.overrides
        if p.get("use_dc_pulse"):
            put("dcpulse_w", ov.get("dcp.power", p.get("dc_pulse_power")))
            put("dcpulse_freq_khz", p.get("dc_pulse_freq"))
        if p.get("use_rf_pulse"):
            put("rfpulse_w", ov.get("rfp.power", p.get("rf_pulse_power")))
            f = p.get("rf_pulse_freq")
            put("rfpulse_freq_hz", int(round(float(f) * 1000)) if f not in (None, "") else None)
            put("rfpulse_duty_pct", p.get("rf_pulse_duty_cycle"))
        if self.n == 2:
            if p.get("use_dc_power"):
                put("dc_w", ov.get("dc.power", p.get("dc_power")))
            if p.get("use_dc_power2"):
                put("dc2_w", ov.get("dc2.power", p.get("dc_power2")))
            if p.get("use_rf_power"):
                put("rf_w", ov.get("rf.power", p.get("rf_power")))
            if p.get("use_n2"):
                put("n2_sccm", ov.get("mfc.n2_flow", p.get("n2_flow")))
            if p.get("use_heater"):
                put("heater_c", ov.get("heater.target", p.get("heater_temp")))
        for g in ("ar", "o2"):
            if p.get(f"use_{g}"):
                put(f"{g}_sccm", ov.get(f"mfc.{g}_flow", p.get(f"{g}_flow")))
        put("pressure_mtorr", ov.get("mfc.wp", p.get("working_pressure")))
        put("base_torr", p.get("base_pressure"))
        if p.get("thickness_mode"):
            put("thickness_nm", p.get("thickness"))
        pt = p.get("process_time")
        put("time_s", float(pt) * 60.0 if pt not in (None, "") else None)
        return {k: (float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) and not k.endswith(("_hz", "_khz"))
                    else v) for k, v in t.items() if not isinstance(v, str)}

    # ═══════════════════════ 상태·저널(5단계 자리) ═══════════════════════
    def state_dict(self) -> Dict[str, Any]:
        return {"chamber": self.ch, "state": self.state_name, "text": self.state_text, "running": self.runner.is_running,
                "run_id": self.run_id, "owner": self.owner, "paused": self.paused, "block_no": self.block_no(),
                "block_count": len(self.blocks), "recipe_rev": self.seq.rev if self.seq else 0,
                "queue": {"index": self.run_idx, "total": len(self.runs), "mode": self.queue_mode},
                "eta_remaining_s": self.eta_remaining_s(), "min_total_s": self.min_total_s(), "error": self.error,
                "adjust": self.adjust_state(), "last_result": self.last_result}

    def state_for_journal(self) -> Dict[str, Any]:
        """저널(5단계 journal.py)이 1초마다 싣는 값 — 레시피 전체·판·지금 블록·블록/메인 경과·수정값·실행 순서."""
        now = self.clock.monotonic()
        cur = self.runner.current
        inst = self.cur_inst
        return {"chamber": self.ch, "run_id": self.run_id, "owner": self.owner, "state": self.state_name,
                "running": self.runner.is_running, "shutdown": self.runner.shutdown,
                "recipe": self.seq.record() if self.seq else None, "rows": self.rows, "run_idx": self.run_idx,
                "queue_mode": self.queue_mode, "block_index": self.cur_block,
                "block_type": inst.type if inst else None,
                "resumable": bool(inst is not None and not inst.auto and BK.by_type()[inst.type].resumable
                                  and cur is not None and cur.holding),
                "resume_checks": list(BK.by_type()[inst.type].resume_checks) if inst is not None else [],
                "step_index": self.runner.idx, "step_action": cur.action if cur else None,
                "block_elapsed_s": None if self.block_t0 is None else now - self.block_t0,
                "main_elapsed_s": None if self.main_t0 is None else now - self.main_t0,
                "remain_s": self.runner.remaining_s(), "overrides": dict(self.overrides), "paused": self.paused,
                "params": dict(self.params), "executed": list(self.executed)}

    def resume_from(self, journal: Dict[str, Any], gap_s: float) -> Optional[str]:
        """재시작 뒤 이어가기(5단계 resume.py 가 판단한 뒤 부른다) — 아직 없음: 'RESUME_NOT_SUPPORTED'."""
        return "RESUME_NOT_SUPPORTED"

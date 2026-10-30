"""챔버12 공정 엔진 — 보기·기록 쪽(ProcessEngine 믹스인): 블록 추적(블록 시작·끝 ProcEvent "block", 실행 순서 executed),
1초 행 열(<c>.proc.*·<c>.set.* — snapshot_fields.EXTRA_FIELDS 의 공정 맥락·목표값, 확정 전 ETA 는 null), 상태(state_dict),
저널·재시작(사용자 지시 3장 (6) — 판단은 resume.py):
- state_for_journal(): journal.py 가 1초마다 싣는 값 — 레시피 전체·판·런 목록·지금 블록·스텝·블록/메인 경과·카운트다운·수정값·
  보낸 설정값(writes)·출력 상태(outputs)·실행 순서·공정 txt 경로. resumable = 블록 정의 resumable ∧ 지금 스텝 holding
  (정지 순서·STOP 요청 중이면 False) 또는 delay 행 대기 중.
- resume_from(저널, 공백): 런을 되살려(같은 블록을 다시 펼쳐 스텝이 저널과 같은지 확인) 러너 스레드에서 설정값 다시 쓰기·되읽기
  확인(실패면 안전 정지) → 이벤트 kind=resumed '복구됨(공백 N s)' → 지금 스텝을 남은 만큼(대기 = 남은 시간, 압력 대기 = 남은 한도,
  두께 = 두께 엔진 resume, 히터 SOAK = 남은 SOAK)으로 바꿔 이어간다. 꺼져 있던 시간도 경과에 넣는다(장비는 계속 켜져 있었음).
  공정 txt 는 같은 파일에 이어 쓴다. RunStats 평균·OES 측정은 이어가지 않는다(재시작 뒤 값만 — 기록의 빈 구간은 채우지 않음).
- stop_from_journal(저널, 공백, 사유): 런을 되살려 STOP 과 같은 정지 순서(지금 구간 장비 — shutdown_steps)로 끄고
  run_end error 사유(PROGRAM_RESTART). 시작 전 단계(프리플라이트·쿨다운)면 STOP 과 같은 정리.
  드라이버가 출력을 켠 줄 알아야 끄기를 보내므로 resume.adopt_outputs 가 먼저 넘겨준다(DC·RF 연속 SET 래치·마지막 전송값,
  펄스 '켜졌을 수 있음').
- program_exit(): 의도적 종료 — 러너를 정지 순서 없이 멈추고 run_end error PROGRAM_EXIT(파워 출력만 끄기는 resume.power_off_only).
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


    # ═══════════════════════ 저널(journal.py 가 1초마다 싣는 값) ═══════════════════════
    def _cur_block_def(self):
        inst = self.cur_inst
        bt = inst.type if inst else (self.blocks[self.cur_block]["type"]
                                     if isinstance(self.cur_block, int) and 0 <= self.cur_block < len(self.blocks) else None)
        return bt, (BK.by_type().get(bt) if bt else None)

    def _writes(self) -> Dict[str, Any]:
        """지금 런에서 이미 보낸 설정값(공정 중 수정값 반영) — 이어갈 때 다시 쓰고 되읽을 것."""
        r = self.runner
        if not r.is_running or r.shutdown:
            return {}
        flow: Dict[str, Any] = {}
        sp: Dict[str, Any] = {}
        power: Dict[str, Any] = {}
        heater = None
        sp_target = None
        ov = self.overrides
        for s in r.steps[:max(0, r.idx)]:
            v = ov.get(s.ref, None) if s.ref else None
            a = s.action
            if a == "MFC_CMD":
                cmd, args = s.params
                if cmd == "FLOW_SET":
                    flow[str(args["channel"])] = float(v if v is not None else args["value"])
                elif cmd == "FLOW_OFF":
                    flow.pop(str(args.get("channel")), None)
                elif cmd in ("SP1_SET", "SP2_SET"):
                    sp[cmd[2]] = float(v if v is not None else args["value"])
                    sp_target = sp[cmd[2]]
                elif cmd == "WAIT_PRESSURE":
                    sp_target = float(args.get("target")) if args.get("target") is not None else sp_target
            elif a in ("DC_POWER_SET", "DC2_POWER_SET", "RF_POWER_SET", "DC_PULSE_START", "DC_PULSE_SET",
                       "RF_PULSE_START", "RF_PULSE_SET"):
                key = {"DC_POWER_SET": "dc1", "DC2_POWER_SET": "dc2", "RF_POWER_SET": "rf"}.get(a) or \
                    ("dcp" if a.startswith("DC_PULSE") else "rfp")
                power[key] = float(v if v is not None else s.value)
            elif a in ("DC_POWER_STOP", "DC2_POWER_STOP", "RF_POWER_STOP", "DC_PULSE_STOP", "RF_PULSE_STOP"):
                power.pop({"DC_POWER_STOP": "dc1", "DC2_POWER_STOP": "dc2", "RF_POWER_STOP": "rf",
                           "DC_PULSE_STOP": "dcp", "RF_PULSE_STOP": "rfp"}[a], None)
            elif a == "HEATER_SET":
                heater = float(v if v is not None else s.value)
        cur = r.current
        if cur is not None and cur.action == "MFC_CMD" and cur.params[0] == "WAIT_PRESSURE":
            sp_target = cur.params[1].get("target", sp_target)
        if sp_target is not None and "1" in sp and ov.get("mfc.wp") is not None:
            sp_target = float(ov["mfc.wp"])
        return {"flow": flow, "sp": sp, "power": power, "heater": heater, "sp_target": sp_target}

    def _outputs(self) -> Dict[str, Any]:
        a = self.actions
        o: Dict[str, Any] = {}
        for u in (1, 2):
            d = a.dc.get(u)
            if d is not None:
                o[f"dc{u}"] = {"enabled": bool(d.enabled), "running": bool(d.running), "target": d.target_power,
                               "last_sent": d.last_sent}
        if a.rf is not None:
            o["rf"] = {"enabled": bool(a.rf.enabled), "running": bool(a.rf.running), "target": a.rf.target_power,
                       "last_sent": a.rf.last_sent, "state": a.rf.state}
        for k, d in (("dcp", a.dcp), ("rfp", a.rfp)):
            if d is not None:
                o[k] = {"bound": d.worker is not None, "maybe_on": bool(d.off_needs_confirm())}
        return o

    def state_for_journal(self) -> Dict[str, Any]:
        """1초 저널 — 레시피 전체·판·런 목록·지금 블록·스텝·블록/메인 경과·카운트다운·수정값·보낸 설정값·실행 순서(JSON 값만)."""
        now = self.clock.monotonic()
        r = self.runner
        cur = r.current
        inst = self.cur_inst
        run = self.runs[self.run_idx] if 0 <= self.run_idx < len(self.runs) else None
        bt, d = self._cur_block_def()
        delay_run = bool(run is not None and run.kind == "delay" and self.state_name == "COOLDOWN" and not r.is_running)
        holding = bool(cur is not None and cur.holding and r.is_running and not r.shutdown and not r.stop_requested)
        resumable = bool(d is not None and d.resumable and ((inst is not None and not inst.auto and holding) or delay_run))
        cd = r.cd
        return {"chamber": self.ch, "run_id": self.run_id, "owner": self.owner, "state": self.state_name,
                "running": r.is_running, "shutdown": r.shutdown, "stop_requested": r.stop_requested, "failed": r.failed,
                "test_mode": r.test_mode, "recipe": self.seq.record() if self.seq else None, "recipe_name": self.recipe_name,
                "rows": self.rows, "runs": [run_record(x) for x in self.runs], "run_idx": self.run_idx,
                "run_kind": run.kind if run else None, "queue_mode": self.queue_mode, "block_index": self.cur_block,
                "block_type": bt, "block_no": self.block_no(), "resumable": resumable,
                "resume_checks": list(d.resume_checks) if d is not None else [],
                "step_index": r.idx, "step_action": cur.action if cur else None, "step_message": cur.message if cur else None,
                "step_count": len(r.steps), "step_holding": bool(cur.holding) if cur else False,
                "block_elapsed_s": None if self.block_t0 is None else now - self.block_t0,
                "main_elapsed_s": None if self.main_t0 is None else now - self.main_t0,
                "run_elapsed_s": (now - r.t0) if r.is_running else None,
                "countdown": None if cd is None else {"total_ms": cd.total_ms, "elapsed_s": cd.elapsed_s(now),
                                                      "paused": cd.paused_at is not None, "msg": cd.msg},
                "remain_s": r.remaining_s(), "overrides": dict(self.overrides), "adjusted": dict(self.adjusted),
                "paused": self.paused, "pause_kind": self.pause_kind, "params": dict(self.params),
                "executed": list(self.executed), "origin": self.origin, "origin_meta": self.origin_meta,
                "txt_path": str(self.txt.path) if self.txt.path is not None else None,
                "actual_sd_ms": r.actual_sd_ms, "actual_pt_ms": r.actual_pt_ms,
                "stats_collecting": bool(self.stats.collecting), "polling_active": bool(self.polling_active),
                "writes": self._writes(), "outputs": self._outputs()}

    # ═══════════════════════ 재시작 뒤(resume.py 가 판단한 뒤 부른다) ═══════════════════════
    def resume_from(self, journal: Dict[str, Any], gap_s: float, plan: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """이어가기 — 저널 상태로 런을 되살리고(꺼져 있던 시간도 경과에 넣음) 러너 스레드에서: 설정값 다시 쓰기·되읽기 확인 →
        (실패면 안전 정지) → '복구됨(공백 N s)' → 지금 스텝부터 계속. 거부 사유 코드(BUSY·RESUME_STATE_MISMATCH) 또는 None."""
        return self._restart(journal, float(gap_s), "resume", dict(plan or {}))

    def stop_from_journal(self, journal: Dict[str, Any], gap_s: float = 0.0, reason: str = "PROGRAM_RESTART",
                          plan: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """이어가지 않음 — 저널의 런을 되살려 STOP 과 같은 정지 순서(지금 구간 장비)로 끄고 run_end error(reason)."""
        return self._restart(journal, float(gap_s), "stop", dict(plan or {}, reason=reason))

    def _restart(self, st: Dict[str, Any], gap: float, mode: str, plan: Dict[str, Any]) -> Optional[str]:
        if self.busy:
            return "BUSY"
        try:
            why = self._restore(st, gap, strict=(mode == "resume"))
        except Exception as e:
            self.reject_detail = f"저널 복원 실패: {e!r}"[:300]
            if mode == "resume":
                return "RESUME_STATE_MISMATCH"
            raise
        if why:
            self.reject_detail = why
            return "RESUME_STATE_MISMATCH"
        hub = getattr(self.sys, "hub", None)
        if hub is not None and self.run_id and st.get("state") in ("RUNNING", "CLEANUP"):
            hub._open_runs[self.ch] = self.run_id          # 지난 실행이 보낸 run_start — run_end 를 run_end 로 내게
        plan.update(state=st, gap_s=gap, mode=mode)
        with self.lk:
            self._resume_plan = plan
            self._cmd = ("RESUME", None)
            self.lk.notify_all()
        self._ensure_thread()
        return None

    def _restore(self, st: Dict[str, Any], gap: float, strict: bool) -> Optional[str]:
        """저널 → 엔진 상태(레시피·런·블록·러너). strict(이어가기)면 다시 펼친 스텝이 저널과 다를 때 사유를 돌려준다."""
        import copy
        from pathlib import Path
        from v2.ch12.blocks_expand import template_from_rows
        from v2.ch12.process_list import plan_runs
        now = self.clock.monotonic()
        rec = st.get("recipe") or {}
        blocks = copy.deepcopy(rec.get("blocks") or [])
        if not blocks:
            raise ValueError("저널에 레시피 블록 없음")
        rows = st.get("rows")
        rts = template_from_rows(rows, self.n, self.cfg, caps=self.caps)[1] if rows else None
        runs = [run_from_record(x) for x in st.get("runs") or []] or plan_runs(blocks, rts, rows)
        ri = int(st.get("run_idx", -1))
        if not 0 <= ri < len(runs):
            raise ValueError(f"런 위치 {ri} 없음")
        run = runs[ri]
        steps, insts = BK.expand(run.blocks(blocks), self.cfg, self.n, caps=self.caps)
        ov = dict(st.get("overrides") or {})
        for s in steps:
            if s.ref in ov and s.ref != "process.main_remaining":
                self._set_step_value(s, ov[s.ref])
        idx = int(st.get("step_index", -1))
        running = bool(st.get("running"))
        same = (0 <= idx < len(steps) and steps[idx].action == st.get("step_action")
                and steps[idx].message == st.get("step_message") and len(steps) == int(st.get("step_count") or -1))
        if strict and running and not same:
            return f"다시 펼친 스텝이 저널과 다름(스텝 {idx + 1} {st.get('step_action')})"
        with self.lk:
            self.seq = BK.SequenceState(copy.deepcopy(rec.get("original") or blocks), copy.deepcopy(blocks),
                                        int(rec.get("rev") or 0), list(rec.get("history") or []))
            self.blocks, self.rows, self.rts, self.runs, self.run_idx = blocks, rows, rts, runs, ri
            self.queue_mode = bool(st.get("queue_mode"))
            self.recipe_name = str(st.get("recipe_name") or "")
            self.params = dict(st.get("params") or {})
            self.run_id, self.owner = st.get("run_id"), st.get("owner") or "local"
            self.origin, self.origin_meta = st.get("origin") or "ui", st.get("origin_meta")
            self.overrides, self.adjusted = ov, dict(st.get("adjusted") or {})
            self.executed = [dict(x) for x in st.get("executed") or []]
            self.error, self._cancel, self._stop_user, self._abort_code = None, False, False, None
            self.cur_block = st.get("block_index")
            be, me = st.get("block_elapsed_s"), st.get("main_elapsed_s")
            self.block_t0 = None if be is None else now - float(be) - gap
            self.main_t0 = None if me is None else now - float(me) - gap
            self.paused, self.pause_kind = bool(st.get("paused")), str(st.get("pause_kind") or "")
            self.cur_steps, self.cur_insts = steps, insts
            self.cur_inst = insts[steps[idx].block] if (running and same and 0 <= steps[idx].block < len(insts)) else None
            r = self.runner
            r._reset()
            if running:
                r.params, r.test_mode = self.params, bool(st.get("test_mode"))
                r.steps, r.idx = (steps, idx) if same else ([], 0)
                r.is_running = True
                r.t0 = now - float(st.get("run_elapsed_s") or 0.0) - gap
                r.actual_sd_ms, r.actual_pt_ms = int(st.get("actual_sd_ms") or 0), int(st.get("actual_pt_ms") or 0)
                self.state_name = "RUNNING"
            else:
                self.state_name = str(st.get("state") or "PREFLIGHT")
            self.lk.notify_all()
        p = st.get("txt_path")
        if p:
            self.txt._path = Path(p)                      # 같은 공정 txt 에 이어 쓴다(새 파일을 만들지 않음)
        return None

    def _restart_event(self, result: str, plan: Dict[str, Any]) -> str:
        """재시작 결과 — 공정 기록(txt) 한 줄. 이어가면 이벤트 kind=resumed {run_id, gap_s, name, block, checks}(포털 카드 1장),
        이어가지 않으면 이벤트를 따로 내지 않는다 — run_end error PROGRAM_RESTART 가 그 카드('❌ 공정 실패 이유' chat 도 없음)."""
        gap = float(plan.get("gap_s") or 0.0)
        reasons = [x for x in plan.get("reasons") or [] if not x[1]]
        if result == "resumed":
            text = f"복구됨(공백 {gap:.0f} s) — 블록 {self.block_no()} 에서 이어감"
            self.log("MAIN", f"[RESTART] {text}")
            inst = self.cur_inst
            self.emit("resumed", text, "RESUMED", level="warn", run_id=self.run_id, gap_s=round(gap, 1),
                      name=str(self.params.get("process_name") or self.recipe_name or ""),
                      block={"no": self.block_no(), "type": inst.type if inst else None,
                             "label": inst.label if inst else None},
                      checks=[list(x) for x in plan.get("reasons") or []])
            return text
        why = "; ".join(f"{x[0]}: {x[2]}" for x in reasons) or str(plan.get("detail") or "")
        text = f"재시작(공백 {gap:.0f} s) — 이어가지 않고 안전 정지: {why}"[:480]
        self.log("MAIN", f"[RESTART] {text}")
        return text

    def _resume_one(self, run, plan: Dict[str, Any]):
        """러너 스레드 — 되살린 런 하나. (ok, stopped) 또는 None(delay 행을 이어 마침 — 다음 런으로)."""
        from v2.ch12 import process_preflight as PF
        from v2.ch12.process_runner import delay_countdown, fmt_hms
        st, gap, r = plan["state"], float(plan.get("gap_s") or 0.0), self.runner
        if not r.is_running:
            if plan["mode"] == "resume" and run.kind == "delay":
                left = max(0.0, float(run.delay_s) - float(st.get("block_elapsed_s") or 0.0) - gap)
                self._restart_event("resumed", plan)
                self.log("Process", f"[Runner] '{run.disp}' 단계: {run.label} 대기 이어감(남은 {left:.0f}s)")
                if not delay_countdown(self.clock, left,
                                       lambda x: self.state(f"지연 대기 중: {run.label} · 남은 시간 {fmt_hms(x)}"),
                                       self.stage_cancelled, lambda: self.paused):
                    raise PF.StageCancelled()
                self._block_end("done")
                self.log("Process", f"[Runner] '{run.disp}' 지연 완료 → 다음 스텝")
                return None
            self._abort_code = str(plan.get("reason") or "PROGRAM_RESTART")
            self._restart_event("stopped", plan)
            raise PF.StageCancelled()                      # → _stop_before_start: run_end error PROGRAM_RESTART                      # 시작 전 단계 — STOP 과 같은 정리 + run_end error
        if plan["mode"] == "resume":
            why = self._rewrite_setpoints(st)
            if why:
                plan["mode"], plan["reason"] = "stop", "PROGRAM_RESTART"
                plan["reasons"] = list(plan.get("reasons") or []) + [("setpoint_rewrite", False, why)]
        if plan["mode"] == "resume":
            self._restart_event("resumed", plan)
            self._prepare_continue(st, gap, plan)
        else:
            text = self._restart_event("stopped", plan)
            with self.lk:
                self._abort_code = str(plan.get("reason") or "PROGRAM_RESTART")
                r.failed = True                            # failures 는 비움 — '공정 실패 이유' chat 을 내지 않게
                r.err = {"code": self._abort_code, "source": "RESTART", "detail": text,
                         "meta": {"checks": [list(x) for x in plan.get("reasons") or []]}}
            r._start_shutdown()
        try:
            r._loop()
            res = r._last
        except Exception as e:                              # v1 _runner except → finish(False)
            r._log(f"러너 예외: {e}")
            res = r._finish(False)
        self.traces.append(list(r.trace))
        detail = dict(res.detail)
        if self._exiting:
            return False, True
        if plan["mode"] == "stop":                         # 카드는 run_end PROGRAM_RESTART 하나 — '공정 실패 이유' chat 을 내지 않게
            detail["shutdown_errors"], detail["errors"] = list(detail.get("errors") or []), []
            detail.pop("reason", None)
        self._after_finish(run, res.ok, detail)
        return res.ok, bool(detail.get("stopped"))

    def _rewrite_setpoints(self, st: Dict[str, Any]) -> Optional[str]:
        """이어가기 전 설정값을 저널대로 다시 쓰고 되읽어 확인(드라이버 확인 경로) — 실패 문구 또는 None."""
        a, w, out = self.actions, st.get("writes") or {}, st.get("outputs") or {}
        jobs = []
        for ch, v in sorted((w.get("flow") or {}).items()):
            jobs.append((f"MFC ch{ch} FLOW_SET {v:g}",
                         lambda cb, c=int(ch), v=v: a.mfc.set_flow(c, v, owner=a.owner, on_done=lambda k, r, d: cb(r))))
        for n_, v in sorted((w.get("sp") or {}).items()):
            jobs.append((f"MFC SP{n_}_SET {v:g}",
                         lambda cb, n_=int(n_), v=v: a.mfc.sp_set(n_, v, owner=a.owner, on_done=lambda k, r, d: cb(r))))
        pw = w.get("power") or {}
        for key, u in (("dc1", 1), ("dc2", 2)):
            d = a.dc.get(u)
            if key in pw and d is not None:
                d.running = False
                jobs.append((f"DC{u} {pw[key]:g} W", lambda cb, d=d, v=pw[key]: d.start(v, on_done=cb)))
        if "rf" in pw and a.rf is not None:
            jobs.append((f"RF {pw['rf']:g} W", lambda cb, v=pw["rf"], o=out.get("rf") or {}: self._rf_adopt(v, o, cb)))
        for key, d, lab in (("dcp", a.dcp, "DC Pulse"), ("rfp", a.rfp, "RF Pulse")):
            if key in pw and d is not None:
                jobs.append((f"{lab} {pw[key]:g} W", lambda cb, d=d, v=pw[key]: d.set_reference_power(
                    v, pause_polling=True, on_done=lambda reason, ok: cb(None if ok else (reason or "FAILED")))))
        h = self.heater
        hs = getattr(h, "state_for_journal", None) and h.state_for_journal() if h is not None else None
        if h is not None and hs and hs.get("run") and hs.get("owner") == "process" and not hs.get("ramp") \
                and hs.get("target") is not None:
            jobs.append((f"히터 {float(hs['target']):g}°C", lambda cb, v=float(hs["target"]): h.apply_target(
                v, owner="process", on_done=lambda ok, why="": cb(None if ok else (why or "FAILED")))))
        box: Dict[str, Any] = {}

        def done(name):
            def f(reason=None):
                box[name] = reason
                self.clock.notify()
            return f
        for name, start in jobs:
            try:
                start(done(name))
            except Exception as e:
                box[name] = repr(e)
        ok = self.clock.wait_until(self.clock.monotonic() + REWRITE_TIMEOUT_S, lambda: len(box) >= len(jobs))
        for name, _ in jobs:
            if name not in box:
                return f"{name}: 확인 시간 초과({REWRITE_TIMEOUT_S:g}s)" if not ok else f"{name}: 결과 없음"
            if box[name] is not None:
                return f"{name}: {box[name]}"
            self.log("MAIN", f"[RESTART] 설정 다시 쓰기 확인: {name}")
        return None

    def _rf_adopt(self, power_w: float, out: Dict[str, Any], cb) -> None:
        """CH2 RF 연속 — 램프를 다시 하지 않고 유지 상태로 넘겨받아 마지막 전송값을 다시 쓴다(검증 쓰기)."""
        rf, now = self.actions.rf, self.clock.monotonic()
        last = out.get("last_sent")
        rf._gen += 1
        rf.enabled = rf.running = True
        rf.state, rf.target_power = "MAINTAINING", float(power_w)
        rf.ref_armed, rf.blind_start_ts, rf.blind_reach_ts = True, now, now
        rf.low_power_n = rf.maintain_count = 0
        rf.polling, rf._due = True, now
        rf.current_step = float(last if last is not None else power_w)
        rf.last_sent = None
        if not rf._send(rf.current_step, after=lambda ok: cb(None if ok else "RF_WRITE_FAILED")):
            cb(None)

    def _prepare_continue(self, st: Dict[str, Any], gap: float, plan: Dict[str, Any]) -> None:
        """지금 스텝을 남은 만큼으로 바꿔 러너가 그 스텝부터 다시 돌게 한다(꺼져 있던 시간도 경과)."""
        import dataclasses
        from datetime import datetime
        r = self.runner
        idx = r.idx
        step = r.steps[idx]
        cdj = st.get("countdown") or {}
        el = float(cdj.get("elapsed_s") or 0.0) + (0.0 if cdj.get("paused") else gap)
        bel = float(st.get("block_elapsed_s") or 0.0) + gap
        a, new = step.action, None
        if a == "DELAY":
            total = int(cdj.get("total_ms") or step.duration or 0)
            done_ms = int(min(el * 1000.0, total))
            new = dataclasses.replace(step, duration=max(1, total - done_ms))
            m = (step.message or "").strip()
            if m.startswith("Shutter Delay"):
                r.actual_sd_ms += done_ms
            elif m.startswith("메인 공정 진행"):
                r.actual_pt_ms += done_ms
        elif a == "ROW_DELAY":
            new = dataclasses.replace(step, duration=max(1, int(step.duration - bel * 1000.0)))
        elif a == "MFC_CMD" and step.params[0] == "WAIT_PRESSURE":
            args = dict(step.params[1])
            args["timeout_sec"] = max(1.0, float(args.get("timeout_sec") or 60.0) - bel)
            new = dataclasses.replace(step, params=(step.params[0], args))
        elif a == "HEATER_WAIT":
            job = ((plan.get("heater") or {}).get("job") or {})
            if step.params and job.get("soak_elapsed_s") is not None:
                left = max(0.0, float(step.params[0]) - float(job["soak_elapsed_s"]) - gap)
                new = dataclasses.replace(step, params=(left,))
        elif a == "THICKNESS_WAIT":
            exp_s, lim_s = (float(x) for x in step.params)
            new = dataclasses.replace(step, params=(max(0.0, exp_s - el), max(1.0, lim_s - el)))
            th = plan.get("thickness")
            if self.actions.thickness is not None and th and th.get("phase") == "wait":
                self.actions.thickness = _ThicknessResume(self.actions, th, gap)
        if new is not None:
            r.steps[idx] = new
        with self.lk:
            r.idx = idx - 1
            if self.paused:
                r.pause_req = True
        self.stats.start(self.params)
        self.stats.collecting = bool(st.get("stats_collecting"))
        if self.thickness is not None and self.thickness.supported:
            thc = self.sys.outputs.thickness.get(self.ch)
            if thc is not None:
                thc.set_recording(True, str(self.params.get("process_name") or ""), self.thickness.status(),
                                  datetime.fromtimestamp(self.clock.wall()))
        exp = (plan.get("expect") or {}).get("shutter") or {}
        if exp.get(f"plc.main_shutter_{self.n}_sw"):
            self.actions._camera(True)

    # ═══════════════════════ 의도적 종료(챔버K 결정 4 — 파워만 끄기는 부르는 쪽) ═══════════════════════
    def program_exit(self) -> bool:
        """콘솔 닫기·로그오프·종료 신호 — 러너를 정지 순서 없이 멈추고 run_end error PROGRAM_EXIT. 공정 중이 아니면 False."""
        if not self.busy:
            return False
        with self.lk:
            self._exiting = self._closing = self._cancel = True
            self.queue_mode = False
            r = self.runner
            running = r.is_running
            if running:
                r.abort_gen += 1
                r.shutdown = True                           # 정지 순서를 새로 만들지 않게
                r.steps, r.idx, r.skip_to, r.pause_req = [], 0, None, False
                if r.expect is not None:
                    r.expect.cancel("program_exit")
                    r.expect = None
                r._cancel_countdown()
            self.lk.notify_all()
        self.log("MAIN", "[EXIT] 프로그램 종료 — 공정 중단(PROGRAM_EXIT), 파워 출력만 끔")
        self.error = "PROGRAM_EXIT"
        self.emit("run_end", "프로그램 종료(PROGRAM_EXIT)", "PROGRAM_EXIT", level="error", result="error",
                  run_id=self.run_id, executed=list(self.executed), recipe=self.seq.record() if self.seq else None)
        return True


REWRITE_TIMEOUT_S = 20.0                 # 이어가기 설정 다시 쓰기·되읽기 확인 상한


class _ThicknessResume:
    """이어가는 THICKNESS_WAIT 한 번 — 러너의 wait_start 를 두께 엔진 resume(저널 상태, 공백)으로 바꾼다."""

    def __init__(self, actions, state: Dict[str, Any], gap_s: float) -> None:
        self._a, self._real, self._state, self._gap = actions, actions.thickness, state, gap_s

    def wait_start(self, target, exp_s, lim_s, on_end, message="", dep_rate=None) -> None:
        self._a.thickness = self._real
        self._real.resume(self._state, self._gap, on_end)

    def __getattr__(self, k):
        return getattr(self._real, k)


def run_record(run) -> Dict[str, Any]:
    return {"kind": run.kind, "first": run.first, "last": run.last, "row": run.row, "params": dict(run.params or {}),
            "raw": run.raw, "logs": [list(x) for x in run.logs or []], "error": run.error, "delay_s": run.delay_s,
            "label": run.label, "disp": run.disp}


def run_from_record(d: Dict[str, Any]):
    from v2.ch12.process_list import RunPlan
    return RunPlan(kind=d["kind"], first=int(d["first"]), last=int(d["last"]), row=d.get("row"),
                   params=dict(d.get("params") or {}), raw=d.get("raw"), logs=[tuple(x) for x in d.get("logs") or []],
                   error=str(d.get("error") or ""), delay_s=float(d.get("delay_s") or 0.0), label=str(d.get("label") or ""),
                   disp=str(d.get("disp") or ""))

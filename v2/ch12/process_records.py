"""챔버12 공정 엔진 — 런 시작·끝 기록(ProcessEngine 믹스인). v1 runtime/chamber_runtime.py 의 'started'(1701-1793)·
'finished'(1795-2101) 처리와 AFTER_FINISH(4977-5041)·_clear_queue_and_reset_ui(7740-7788).

- 시작: 공정명 키 통일(비면 'Run CHn'), t0_wall·started_at, RunStats 세션(use_ms 면 셔터 열릴 때부터 평균), 두께 기록 시작·두께 CSV
  set_recording(공정 중에만), DC Pulse 아크 카운터 리셋, 'Sputter Calib 로그 세션 시작', '▶️ CHn '이름' 시작 (t=…s)'(값은 분 — v1 그대로).
- 끝: 결과 문구(성공·stop·실패) → RunStats end_update(실제 Shutter Delay·메인 시간으로 덮어씀) → Calib CSV(성공·실패·STOP 모두)
  → 엑셀(성공만, DC Pulse 아크 수) → 실패 이유 채팅 문구(진짜 실패만) → 폴링 OFF → run_end(done/aborted/error + 사유 코드,
  실행 순서·레시피 기록) → 히터(다음 공정 행도 히터면 유지) → 장비 정리(TEST 행 STOP 은 생략) → END(0.3 s 정착 뒤).
"""
from __future__ import annotations

from datetime import datetime

from v2.ch12.process_list import RunPlan, next_process_uses_heater

FOOTER_SETTLE_S = 0.3                 # _RUN_LOG_FOOTER_SETTLE_S


class ProcessRecordsMixin:
    # ───────── 런 시작·끝 기록(v1 'started'·'finished' 1701-2101) ─────────
    def _run_started(self, params: dict) -> None:
        pname = str(params.get("process_name") or params.get("Process_name") or params.get("process_note") or "").strip() \
            or f"Run CH{self.n}"
        for k in ("process_name", "Process_name", "process_note"):
            params[k] = pname
        t0 = params.get("t0_pressed_wall") or datetime.fromtimestamp(self.clock.wall()).isoformat(timespec="seconds")
        params["t0_wall"] = params["started_at"] = t0
        self.stats.start(params)
        if self.thickness is not None:
            self.thickness.new_run()
            self.thickness.run_started(params)
            th = self.sys.outputs.thickness.get(self.ch)
            if th is not None and self.thickness.supported:
                th.set_recording(True, pname, self.thickness.status(), datetime.fromtimestamp(self.clock.wall()))
        if self.actions.dcp is not None:
            self.actions.dcp.reset_arc_counts()
        self.log("CSV", "Sputter Calib 로그 세션 시작")
        self.log("MAIN", f"▶️ CH{self.n} '{pname}' 시작 (t={float(params.get('process_time', 0) or 0):.1f}s)")
        self.executed = []
        self.main_t0 = self.clock.monotonic()
        data = {"run_id": self.run_id, "owner": self.owner, "params": dict(params), "recipe_rev": self.seq.rev if self.seq else 0}
        if self.queue_mode:
            data.update(process_index=self.run_idx + 1, process_total=len(self.runs))
        self.emit("run_start", pname, data=data)

    def _after_finish(self, run: RunPlan, ok: bool, d: dict) -> None:
        stopped = bool(d.get("stopped"))
        raw = str(d.get("result") or "").upper()
        result = "성공" if raw == "SUCCESS" else "stop" if raw == "STOP" else "실패" if raw == "FAIL" else \
            ("stop" if stopped else "성공" if ok else "실패")
        self.stats.end_update(process_name=d.get("process_name", ""), result=result, stopped=stopped,
                              actual_shutter_delay_min=d.get("actual_shutter_delay_min"),
                              actual_process_time_min=d.get("actual_process_time_min"))
        self.log("CSV", f"Sputter Calib CSV 기록 요청 (ok={ok}, result={result})")
        if self.thickness is not None and self.thickness.supported:
            self.stats.set_thickness(self.thickness.run_finished())
            th = self.sys.outputs.thickness.get(self.ch)
            if th is not None:
                th.set_recording(False)
        if self.calib is not None:
            self.calib.record(self.stats, ok)
        if ok and self.excel is not None:
            if self.actions.dcp is not None:
                s, h = self.actions.dcp.arc_counts
                self.stats.params["soft_arc_count"], self.stats.params["hard_arc_count"] = s, h
            pp = self.stats.params
            self.excel.save_main(self.stats, operator=pp.get("operator", ""), substrate=pp.get("substrate", ""),
                                 note=pp.get("note", ""))
        if self.run_warnings:
            d.setdefault("warnings", list(self.run_warnings))
        if params_chuck := str(self.params.get("chuck_position") or "").strip():
            d.setdefault("chuck_position", params_chuck)
        if not ok and not stopped:
            reason = str(d.get("reason") or (d.get("errors") or [""])[0] or "")
            if reason:
                self.chat(f"❌ CH{self.n} 공정 실패 이유: {reason}")
        self.actions.apply_polling(False, {})
        self.last_end_t = self.clock.monotonic()
        kind = "done" if ok else ("aborted" if stopped else "error")
        code = None if ok else ("USER_STOP" if stopped else (self._abort_code or d.get("error_code") or "PROCESS_FAILED"))
        self.error = None if (ok or stopped) else code
        if not (ok or stopped):
            name = d.get("process_name") or "(process)"
            reason = str(d.get("reason") or (d.get("errors") or ["process failed"])[0])
            self.popup(f"CH{self.n} 공정 실패", f"{name}\n\n사유: {reason}\n\n확인을 누르면 상태 표시가 Idle로 변경됩니다.", "error")
        self.last_result = dict(d, end=kind, code=code)
        self.emit("run_end", d.get("process_name", ""), code, level="info" if ok else "warn", result=kind,
                  run_id=self.run_id, result_detail=dict(d), executed=list(self.executed),
                  recipe=self.seq.record() if self.seq else None)
        self._abort_code = None
        # AFTER_FINISH — 히터 → 장비 정리(TEST STOP 은 생략) → END
        self.state_name = "CLEANUP"
        if self.heater is not None:
            done = {"v": False}
            self.heater.stop_user = self._stop_user
            self.heater.after_row(ok, stopped, d, queue_mode=self.queue_mode,
                                  next_uses_heater=next_process_uses_heater(self.runs, self.run_idx),
                                  on_done=lambda: (done.update(v=True), self.clock.notify()))
            self.clock.wait_until(self.clock.monotonic() + 30.0, lambda: done["v"])
        if bool(d.get("test_mode")) and stopped:
            self.log("MAIN", "[Runner] [TEST] STOP finished → cleanup skip")
        else:
            self.log("MAIN", "[Runner] finished → device cleanup")
            self.actions.cleanup_devices()
            self._close_log()
        self.cur_block, self.cur_inst, self.block_t0 = -1, None, None
        self.paused = False

    def _close_log(self) -> None:
        if self.txt.path is not None:
            self.clock.wait_until(self.clock.monotonic() + FOOTER_SETTLE_S, None)
            self.txt.close_run()

    def _reset_after_list(self) -> None:
        """_clear_queue_and_reset_ui(7740-7788)."""
        if self.heater is not None and getattr(self.heater, "owner", None) == "process":
            self.heater.queue_end()
        self.queue_mode = False
        self.run_idx = -1
        if self.rows is not None:
            self.blocks, self.rows, self.rts, self.runs, self.seq = [], None, None, [], None
        self._close_log()
        self.state("대기 중")
        self.params = {}

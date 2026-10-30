"""챔버12 공정 엔진 — 챔버마다 하나(CH1·CH2 가 같은 system 에서 동시에 돈다). v1 runtime/chamber_runtime.py 의 Runner
(START/START_QUEUE/STOP/PC_FINISHED 4529-5244)·_start_after_preflight(3799-4119)·finished 처리(1795-2101)와
controller/process_controller.py(실행 — process_runner.StepRunner)를 v2 방식으로.

구조
- 러너 스레드 하나(clock.start_thread "Runner.CHn"): 시작 명령을 받아 런(process_list.RunPlan)마다 delay 대기 / TEST /
  값 검사 → 새 공정 txt → 쿨다운(첫 행 빼고, 자기 챔버 마지막 종료 기준 60 s) → 프리플라이트(process_preflight) → StepRunner.run
  → 끝 처리(기록·히터·장비 정리) → 다음 런(큐 모드·성공·정지 아님) 또는 리셋. 모든 대기는 clock.wait_until.
- STOP(stop)·이상 중단(abort)은 부른 스레드에서 바로: 공정이 돌고 있으면 StepRunner.request_stop / step_failed(정지 순서는
  v1 _start_normal_shutdown 그대로 — 지금 구간 장비의 shutdown_steps), 시작 전 단계(프리플라이트·delay 행·쿨다운)면 그 대기를
  끊고 v1 '공정 시작 전 STOP' 정리. 리스트 취소(v1 request_stop_all): 대기 행·쿨다운·큐 자동 진행까지 멈춘다.
- 장비 이벤트: system.event_listeners → ProcessActions.on_event(v1 펌프와 같은 처리).
- 1초 행: snapshot_fields() → <c>.proc.*·<c>.set.*, on_row(view) → 두께 poll·히터 상태 줄(기록은 1초 행에서 하지 않는다 —
  RunStats 는 v1 처럼 장비 이벤트 표본).
- 공정 중 자유 제어(process_adjust.AdjustMixin): adjust·skip_block·pause/resume_pause·edit_sequence.
결과 이벤트(ProcEvent → system.emit — 기록·웹): run_start / run_end(done·aborted·error + 사유 코드) / block / adjust / edit /
  warn / chat(v1 구글챗이 보내던 문구 — 포털이 보낸다) / popup(v1 경고·오류 창).
사유 코드(run_end): done=None, aborted=USER_STOP, error=v1 대표 오류 코드(E###) 또는 PROCESS_FAILED·PREFLIGHT_FAILED·
  GATE_CHECK_FAILED·MAIN_VALVE_CHECK_FAILED·PULSE_PORT_BUSY·RECIPE_PARAM_ERROR·START_FAILED·SAFETY_ABORT(abort 의 reason).
"""
from __future__ import annotations

import collections
import copy
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from v2.ch12 import blocks as BK
from v2.ch12 import process_preflight as PF
from v2.ch12.blocks_expand import template_from_rows
from v2.ch12.eta import estimate_min_total_s
from v2.ch12.outputs.base import QueueWriter
from v2.ch12.outputs.calib_csv import CalibCsv
from v2.ch12.outputs.excel_log import ExcelLog
from v2.ch12.outputs.process_txt import ProcessTxtLog
from v2.ch12.outputs.run_stats import RunStats
from v2.ch12.process_actions import ProcessActions
from v2.ch12.process_adjust import AdjustMixin
from v2.ch12.process_list import (RunPlan, USE_KEYS, derive_params, next_process_uses_heater, plan_runs,
                                  raw_rows_from, stop_uses)
from v2.ch12.process_runner import StepRunner, delay_countdown, fmt_hms
from v2.ch12.process_steps import caps_from_cfg, shutdown_steps, step_cfg
from v2.ch12.recipe_check import check_norm_params

EVENTS_KEEP = 20000
COOLDOWN_S = 60.0                     # runtime_state.remaining_cooldown(…, 60.0)
FOOTER_SETTLE_S = 0.3                 # _RUN_LOG_FOOTER_SETTLE_S
OWNERS = ("robot", "web", "local")


@dataclass
class ProcEvent:
    kind: str
    detail: str = ""
    code: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
    level: str = "info"


class ProcessEngine(AdjustMixin):
    def __init__(self, system, chamber: str, *, heater=None, thickness=None, excel_main_dir=None,
                 outputs: bool = True) -> None:
        self.sys, self.clock, self.lk = system, system.clock, system.clock.cond
        self.cfg, self.ch, self.n = system.cfg, chamber, 1 if chamber == "ch1" else 2
        self.caps, self.sc = caps_from_cfg(self.cfg, self.n), step_cfg(self.cfg, self.n)
        self.heater, self.thickness = heater, thickness
        self.plc = system.shared.plc.proxy(self.ch.upper())
        self.events: "collections.deque[ProcEvent]" = collections.deque(maxlen=EVENTS_KEEP)
        self.log_lines: "collections.deque[str]" = collections.deque(maxlen=5000)
        now = lambda: datetime.fromtimestamp(self.clock.wall())          # noqa: E731
        self.writer = QueueWriter(f"proc-{chamber}")
        self.txt = ProcessTxtLog(self.n, self.cfg, writer=self.writer, now=now, on_ui=self.log_lines.append)
        self.stats = RunStats(now=now)
        self.calib = CalibCsv(self.n, self.cfg, thickness_columns=self.caps.thickness, writer=self.writer,
                              log_func=lambda m: self.log("CSV", m)) if outputs else None
        self.excel = ExcelLog(self.n, self.cfg, thickness_columns=self.caps.thickness, dev_mode=bool(self.cfg.get("DEV_MODE")),
                              writer=self.writer, now=now, main_dir=excel_main_dir,
                              log_func=lambda m: self.log("CSV", m)) if outputs else None
        self.actions = ProcessActions(self)
        system.event_listeners.append(self.actions.on_event)
        self.runner = StepRunner(self)
        self.actions.runner = self.runner
        if thickness is not None:
            thickness._log_cb = self.log
            thickness._on_state = self.state
            thickness._oes_stop = self.actions.oes_stop
        # 레시피·런
        self.recipe_name = ""
        self.blocks: List[dict] = []
        self.rows: Optional[List[dict]] = None
        self.rts: Optional[list] = None
        self.seq: Optional[BK.SequenceState] = None
        self.runs: List[RunPlan] = []
        self.run_idx = -1
        self.queue_mode = False
        self.params: Dict[str, Any] = {}
        self.cur_steps: list = []
        self.cur_insts: list = []
        self.cur_block = -1                     # 전체 블록 목록 위치(자동 끝 정지면 None 블록 — 런 끝 위치)
        self.cur_inst = None
        self.block_t0: Optional[float] = None
        self.main_t0: Optional[float] = None
        self.executed: List[dict] = []
        self.run_warnings: List[str] = []
        # 상태
        self.state_name = "IDLE"                # IDLE · PREFLIGHT · COOLDOWN · RUNNING · CLEANUP
        self.state_text = "대기 중"
        self.run_id: Optional[str] = None
        self.owner: Optional[str] = None
        self.origin = "ui"
        self.origin_meta: Optional[dict] = None
        self.last_end_t: Optional[float] = None
        self.error: Optional[str] = None
        self.polling_active = False
        self.paused = False
        self.pause_kind = ""
        self.stopping = False
        self._cancel = False
        self._cmd: Optional[tuple] = None
        self._closing = False
        self._thread = None
        self.preflight_hooks = {"gate": PF.gate_check, "main_valve": PF.main_valve_check, "chuck": PF.chuck_move}
        self.gun_targets = None                 # () -> {"G1 Target": ..} 재고 DB(core — v1 _load_gun_targets 3 s)
        self.on_host_start = None               # (ok, 문구) — 로봇 START_SPUTTER 응답 시점(v1 _host_report_start)
        self.last_result: Optional[dict] = None
        self.traces: "collections.deque[list]" = collections.deque(maxlen=50)   # 런마다 실행한 스텝(시험·기록)
        self._init_adjust()

    # ═══════════════════════ 공통 ═══════════════════════
    def cget(self, key: str, default: Any = None) -> Any:
        v = self.cfg.get(f"CH{self.n}_{key}")
        if v is None:
            v = self.cfg.get(key)
        return default if v is None else v

    def log(self, src: str, msg: str) -> None:
        self.txt.log(src, msg)

    def state(self, text: str) -> None:
        self.state_text = str(text)

    def emit(self, kind: str, detail: str = "", code: Optional[str] = None, level: str = "info", **data) -> ProcEvent:
        e = ProcEvent(kind, detail, code, data, level)
        self.events.append(e)
        self.sys.emit(f"engine:{self.ch}", kind, detail, level=level, code=code, event=e)
        return e

    def chat(self, text: str) -> None:
        self.emit("chat", text, level="warn")

    def popup(self, title: str, text: str, level: str = "warn") -> None:
        self.emit("popup", text, level=level, title=title)

    @property
    def busy(self) -> bool:
        """is_busy(3279-3290) — 시작 명령 접수 ~ 큐 완주/정리 완료."""
        return self._cmd is not None or self.state_name != "IDLE" or self.runner.is_running

    @property
    def is_running(self) -> bool:
        return self.runner.is_running

    def stage_cancelled(self) -> bool:
        return self._cancel or self._closing

    def ig_in_use_by_pc(self) -> bool:
        pc = self.sys.engine("pc")
        f = getattr(pc, "ig_in_use", None)
        return bool(f(self.ch)) if callable(f) else False

    def heater_context(self) -> str:
        from v2.ch12.heater_link import process_context
        total = len(self.runs) if self.queue_mode else 0
        return process_context(self.params.get("process_name", ""), max(0, self.run_idx), total)

    def shutdown_steps(self) -> list:
        """STOP·이상 중단 정지 순서 — 지금 구간 장비(v1 _create_shutdown_sequence(current_params))."""
        run = self.runs[self.run_idx] if 0 <= self.run_idx < len(self.runs) else None
        bl = run.blocks(self.blocks) if run is not None else []
        cur = (self.cur_block - run.first) if run is not None and self.cur_block is not None else len(bl)
        return shutdown_steps(self.sc, stop_uses(bl, cur, self.sc))

    def on_main_shutter(self, opened: bool) -> None:
        (self.stats.shutter_open if opened else self.stats.shutter_close)()

    # ═══════════════════════ 레시피 적재 ═══════════════════════
    def load(self, recipe, source: str = "web", by: Optional[str] = None, name: str = "") -> Optional[str]:
        """rows(v1 레시피 행 목록 — 파일 레시피 = 큐) 또는 {"blocks": [...]} / 블록 목록. 거부 사유 코드 또는 None."""
        if self.busy:
            return "BUSY"
        rows = None
        if isinstance(recipe, dict) and "blocks" in recipe:
            blocks, rts = recipe["blocks"], None
            name = name or str(recipe.get("name") or "")
        elif isinstance(recipe, list) and recipe and isinstance(recipe[0], dict) and "type" in recipe[0]:
            blocks, rts = recipe, None
        elif isinstance(recipe, list):
            from v2.ch12.recipe_check import check_raw_rows, e230_message
            errs = check_raw_rows(recipe)
            if errs:
                self.log("File", e230_message(errs))
                self.reject_detail = e230_message(errs)
                return "E230"
            if not recipe:
                return "RECIPE_EMPTY"
            rows = [dict(r) for r in recipe]
            blocks, rts = template_from_rows(rows, self.n, self.cfg, caps=self.caps)
        else:
            return "BAD_ARGS"
        if rows is None:
            clean, errs, _w = BK.check_blocks(blocks, self.cfg, self.n, caps=self.caps)
            if errs:
                self.reject_detail = "; ".join(errs)[:500]
                return "RECIPE_INVALID"
            blocks = clean
        with self.lk:
            self.blocks, self.rows, self.rts = copy.deepcopy(blocks), rows, rts
            self.seq = BK.SequenceState.start(self.blocks)
            self.recipe_name = name
            self.runs = plan_runs(self.blocks, rts, rows)
            self.run_idx = -1
        if rows is not None:
            self.log("File", f"총 {len(rows)}개 공정 읽음.")
        self.emit("recipe_loaded", name, data={"blocks": len(self.blocks), "rows": len(rows or []), "source": source, "by": by})
        return None

    def snapshot_recipe_queue(self) -> tuple:
        """프리스퍼터 — 적재된 파일 레시피(큐)를 비우고 보관(v1 snapshot_recipe_queue 7953-7967)."""
        with self.lk:
            if self.rows is None:
                return (None, -1)
            q = (self.blocks, self.rows, self.rts, self.runs, self.seq, self.recipe_name)
            idx = self.run_idx
            self.blocks, self.rows, self.rts, self.runs, self.seq, self.run_idx = [], None, None, [], None, -1
        self.log("MAIN", f"[CH{self.n}] 레시피 큐 {len(q[1])}건 임시 보관 (Pre-Sputter)")
        return (q, idx)

    def restore_recipe_queue(self, snap: tuple) -> None:
        """보관분 되돌림 — 아직 바쁘면(프리플라이트·큐 행 사이 포함) 버린다(7969-7989)."""
        q, idx = snap if isinstance(snap, tuple) and len(snap) == 2 else (None, -1)
        if not q:
            return
        if self.busy:
            self.log("MAIN", f"[CH{self.n}] 공정 진행 중 → 레시피 큐 복구 생략(실행 큐 보호). "
                             f"보관분 {len(q[1])}건은 파기됨 — 필요 시 파일을 다시 여십시오.")
            return
        with self.lk:
            self.blocks, self.rows, self.rts, self.runs, self.seq, self.recipe_name = q
            self.run_idx = int(idx)
        self.log("MAIN", f"[CH{self.n}] 레시피 큐 {len(q[1])}건 복구 완료")

    def clear(self) -> None:
        if not self.busy:
            self.blocks, self.rows, self.rts, self.runs, self.seq, self.run_idx = [], None, None, [], None, -1

    # ═══════════════════════ 시작 ═══════════════════════
    def start_guard(self) -> Optional[str]:
        """시작 가드(v1 _handle_start_clicked 4375-4416 중 엔진이 아는 것) — core 가 먼저 부른다."""
        if self.last_end_t is not None:
            remain = COOLDOWN_S - (self.clock.monotonic() - self.last_end_t)
            if remain > 0:
                self.reject_detail = f"cooldown {remain:.0f}s remaining"
                self.popup("대기 필요", f"이전 공정 종료 후 1분 대기 필요합니다.\n{int(remain + 0.999)}초 후에 시작하십시오.")
                return "COOLDOWN"
        if self.state_name != "IDLE" or self._cmd is not None:
            self.reject_detail = f"runner busy: {self.state_name}"
            return "RUNNER_BUSY"
        if self.runner.is_running:
            self.reject_detail = "process controller busy"
            return "RUNNER_BUSY"
        return None

    def start(self, source: str = "web", by: Optional[str] = None, owner: str = "web", origin: str = "ui",
              origin_meta: Optional[dict] = None, params: Optional[dict] = None) -> Optional[str]:
        """적재된 레시피(params 를 주면 단일 공정 — v1 화면 시작)를 시작. 거부 사유 코드 또는 None."""
        why = self.start_guard()
        if why:
            self._host(False, self.reject_detail)
            return why
        if params is None and not self.runs:
            return "NO_RECIPE"
        with self.lk:
            self.owner = owner if owner in OWNERS else "local"
            self.origin, self.origin_meta = origin, origin_meta
            self.error = None
            self._cancel = False
            if params is not None:
                self._single(params)
            self._cmd = ("START_QUEUE" if self.rows is not None else "START", by)
            self.lk.notify_all()
        if self.rows is not None:
            self.log("MAIN", f"[CH{self.n}] 파일 기반 자동 공정 시작")
        self._ensure_thread()
        return None

    def _single(self, params: dict) -> None:
        """단일 공정(v1 화면 값) — 행 하나를 템플릿 블록으로."""
        from v2.ch12.blocks_expand import template_blocks
        p = dict(params)
        bl, logs = template_blocks(p, self.n, self.cfg, caps=self.caps)
        self.blocks, self.rows, self.rts = bl, None, None
        self.seq = BK.SequenceState.start(bl)
        self.runs = plan_runs(bl)
        self.runs[0].params, self.runs[0].logs = p, list(logs)
        if p.get("test_mode"):
            self.runs[0].kind = "test"

    def _ensure_thread(self) -> None:
        if self._thread is None or not self._thread.is_alive():
            self._thread = self.clock.start_thread(self._main, f"Runner.CH{self.n}")

    def _main(self) -> None:
        while True:
            self.clock.wait_until(None, lambda: self._cmd is not None or self._closing)
            if self._closing:
                return
            kind, _by = self._cmd
            try:
                if kind == "STOP_IDLE":
                    self._stop_before_start()
                else:
                    self._run_list(kind == "START_QUEUE")
            except Exception as e:                                     # v1 '[Runner] loop exception'
                self.log("MAIN", f"[Runner] loop exception: {e!r}")
                self._reset_after_list()
            finally:
                with self.lk:
                    self._cmd = None
                    self.state_name = "IDLE"
                    self.lk.notify_all()

    # ═══════════════════════ 리스트(v1 ADVANCE_QUEUE·AFTER_FINISH) ═══════════════════════
    def _run_list(self, queue: bool) -> None:
        self.queue_mode = queue
        self.run_idx = -1
        while True:
            with self.lk:
                self.run_idx += 1
                if self.run_idx >= len(self.runs):
                    if queue:
                        self.log("MAIN", "[Runner] queue finished → reset")
                    self._reset_after_list()
                    return
                run = self.runs[self.run_idx]
            if queue:
                self.log("UI", f"[CH{self.n}] 자동 공정 ({self.run_idx + 1}/{len(self.runs)}) 준비: "
                               f"'{(run.raw or {}).get('Process_name', run.params.get('Process_name', ''))}'")
            try:
                if run.kind == "delay":
                    self.state_name = "COOLDOWN"
                    self._host(True, f"delay accepted: {run.label}")
                    self._block_begin(run.first)
                    self.log("Process", f"[Runner] '{run.disp}' 단계: {run.label} 대기 시작")
                    if not delay_countdown(self.clock, run.delay_s,
                                           lambda r: self.state(f"지연 대기 중: {run.label} · 남은 시간 {fmt_hms(r)}"),
                                           self.stage_cancelled, lambda: self.paused):
                        raise PF.StageCancelled()
                    self._block_end("done")
                    self.log("Process", f"[Runner] '{run.disp}' 지연 완료 → 다음 스텝")
                    continue
                ok, stopped = self._run_one(run, first=(self.run_idx == 0))
            except PF.StageCancelled:
                self._stop_before_start()
                return
            if not (queue and ok and not stopped and self.queue_mode):
                self.log("MAIN", f"[Runner] queue end (ok={ok}, stopped={stopped}) → reset")
                self._reset_after_list()
                return
            self.log("MAIN", "[Runner] queue advance (ok)")

    def _run_one(self, run: RunPlan, first: bool) -> tuple:
        """런 하나 — (ok, stopped). 시작 전 실패는 (False, False)."""
        params = run.params
        params = dict(params) if params else derive_params(run.blocks(self.blocks), self.sc)
        if self.recipe_name and not params.get("Process_name") and not params.get("process_note"):
            params["Process_name"] = self.recipe_name
        self.params = params
        for src, msg in run.logs:
            self.log(src, msg)
        if run.kind == "error":
            self.txt.open_run(params, origin=self.origin, origin_meta=self.origin_meta)
            self.log("Process", f"공정 시작 오류: {run.error}")
            self.state_name = "RUNNING"
            detail = {"process_name": params.get("process_name") or params.get("Process_name") or "Untitled",
                      "result": "FAIL", "stopped": False, "errors": [run.error], "error_code": run.error[:4],
                      "error_detail": run.error, "test_mode": False}
            self._after_finish(run, False, detail)
            return False, False
        errs = check_norm_params(params, self.n, self.caps, self.cfg, heater_available=self.heater is not None)
        if errs:
            if self.txt.path is None:
                self.txt.open_run(params, origin=self.origin, origin_meta=self.origin_meta)
            self.log("Validate", "CSV 공정 파라미터 오류:\n - " + "\n - ".join(errs))
            self._host(False, "CSV 공정 파라미터 오류: " + "; ".join(errs))
            self.queue_mode = False
            self.error = "RECIPE_PARAM_ERROR"
            self.emit("run_end", "; ".join(errs), "RECIPE_PARAM_ERROR", level="error", result="error")
            return False, False
        if self.txt.path is not None:
            self.txt.close_run()
        self.txt.open_run(params, origin=self.origin, origin_meta=self.origin_meta)
        if not first and self.last_end_t is not None:
            delay_s = max(0.0, COOLDOWN_S - (self.clock.monotonic() - self.last_end_t))
            if delay_s > 0:
                self.state_name = "COOLDOWN"
                self.log("MAIN", f"[Runner] cooldown wait: {delay_s:.1f}s (쿨다운 대기)")
                if not delay_countdown(self.clock, delay_s,
                                       lambda r: self.state(f"다음 공정 대기중 (쿨다운 대기) · 남은 시간 {fmt_hms(r)}"),
                                       self.stage_cancelled):
                    raise PF.StageCancelled()
        self.state_name = "PREFLIGHT"
        steps, insts = BK.expand(run.blocks(self.blocks), self.cfg, self.n, caps=self.caps)
        self.cur_steps, self.cur_insts = steps, insts
        if not self._preflight(run, params):
            return False, False
        self.run_id = uuid.uuid4().hex[:12]
        self.run_warnings = list(self.run_warnings)
        self._run_started(params)
        self.state_name = "RUNNING"
        res = self.runner.run(params, steps)
        self.traces.append(list(self.runner.trace))
        detail = dict(res.detail)
        self._after_finish(run, res.ok, detail)
        return res.ok, bool(detail.get("stopped"))

    def _preflight(self, run: RunPlan, params: dict) -> bool:
        """_start_after_preflight(3799-4119). False = 시작 실패(정리까지 끝냄)."""
        if run.kind == "test" or params.get("test_mode"):
            dur = float(params.get("test_duration_sec", 0.0) or 0.0) or float(params.get("process_time", 0) or 0) * 60.0
            dur = max(1.0, dur)
            params.setdefault("process_note", params.get("Process_name") or "TEST")
            self.log("MAIN", f"[TEST MODE] '{params.get('process_note') or 'TEST'}' 장비 제어 스킵 / {dur:.1f}s 시뮬레이션")
            self._host(True, f"TEST MODE: {str(params.get('time', '')).strip() or f'{dur:.0f}s'}")
            return True
        sel = {"dcpulse": bool(params.get("use_dc_pulse")) and self.caps.dcp,
               "rfpulse": bool(params.get("use_rf_pulse")) and self.caps.rfp}
        why = PF.claim_pulses(self, sel)
        if why:
            self.log("MAIN", why)
            self.popup("공유 장비 사용 중", why)
            return self._start_fail(why, "PULSE_PORT_BUSY", cleanup=False)
        PF.start_devices(self, sel)
        t = float(self.cget("CHAMBER_PREFLIGHT_TIMEOUT_WITH_PULSE_S" if any(sel.values()) else "CHAMBER_PREFLIGHT_TIMEOUT_S",
                            10.0 if any(sel.values()) else 8.0))
        failed = PF.wait_connected(self, sel, t)
        if failed:
            fl = ", ".join(failed)
            self._host(False, "장비 연결 실패: " + fl)
            self.log("MAIN", f"필수 장비 연결 실패: {fl} → 시작 중단")
            self.popup("장비 연결 실패", f"다음 장비 연결을 확인하지 못했습니다:\n - {fl}\n\n케이블/전원/포트 설정 확인 후 재시도",
                       level="error")
            return self._start_fail(f"preflight connect failed: {fl}", "PREFLIGHT_FAILED")
        self._host(True, "preflight OK")
        note = params.get("process_note") or params.get("Process_name") or "알 수 없음"
        for key, label, code in (("gate", "Gate", "GATE_CHECK_FAILED"), ("main_valve", "Main Valve", "MAIN_VALVE_CHECK_FAILED")):
            ok, why = self.preflight_hooks[key](self)
            if not ok:
                self.log("MAIN", f"[CH{self.n}] {label} 체크 실패 ({why}) → 공정 시작 차단")
                self.emit("chat", f"{label} 체크 실패: {why} → 공정 시작 차단 (공정: {note})", "E301", level="error")
                return self._start_fail(f"{label.lower().replace(' ', ' ')} check failed: {why}", code)
        thk = self.thickness is not None and self.thickness.thickness_mode(params)
        if thk:
            code, msg = self.thickness.check_basic(params)
            if code:
                return self._thickness_reject(params, code, msg)
        self.run_warnings = []
        chuck = self.preflight_hooks["chuck"](self, params)
        if not chuck:
            pos = str(params.get("chuck_position") or "")
            warn = f"Chuck 위치 이동 실패 (target='{pos}')" if pos else "Chuck 위치 이동 실패"
            self.log("MAIN", f"⚠️ {warn} → 공정은 계속 진행")
            self.run_warnings.append(warn)
        if not params.get("chuck_position"):
            actual = PF.read_chuck_pos(self)
            if actual != "unknown":
                params["chuck_position"] = actual
        if thk:
            box: dict = {}
            self.thickness.apply_material(params, bool(chuck), lambda r: (box.setdefault("r", r), self.clock.notify()))
            self.clock.wait_until(self.clock.monotonic() + 30.0, lambda: "r" in box or self.stage_cancelled())
            if self.stage_cancelled():
                raise PF.StageCancelled()
            code, msg = box.get("r", ("E846", "재료값 쓰기 응답 없음"))
            if code:
                return self._thickness_reject(params, code, msg)
            params["thickness_mode"] = True
        self.log("MAIN", "장비 연결 확인 완료 → 공정 시작")
        if self.heater is not None:
            self.heater.process_started(bool(params.get("use_heater")))
        return True

    def _thickness_reject(self, params, code: str, msg: str) -> bool:
        log_txt, chat_txt = self.thickness.reject_texts(code, msg, params)
        self.log("MAIN", log_txt)
        self.emit("chat", chat_txt, code, level="error")
        return self._start_fail(msg, code)

    def _start_fail(self, why: str, code: str, cleanup: bool = True) -> bool:
        self._host(False, why)
        if cleanup:
            self.actions.cleanup_devices()
        else:
            self.sys.shared.pulse.release_chamber(self.ch)
        self.error = code
        self.last_end_t = self.clock.monotonic()
        self.emit("run_end", why, code, level="error", result="error")
        self.queue_mode = False
        return False

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

    def _host(self, ok: bool, msg: str) -> None:
        f = self.on_host_start
        if f is not None:
            try:
                f(ok, msg)
            except Exception:
                pass

    # ═══════════════════════ STOP·이상 중단 ═══════════════════════
    def stop(self, source: str = "web", by: Optional[str] = None, user: bool = True) -> None:
        """챔버 STOP = v1 request_stop_all — 대기 행·쿨다운·큐 자동 진행까지 멈춘다(어떤 상태에서도 받음)."""
        self.log("MAIN", f"[Runner] STOP 요청(user={user}) state={self.state_name}")
        self._stop_user = bool(user)
        with self.lk:
            self.queue_mode = False
            self.paused = False
            if self.runner.is_running:
                self.log("MAIN", "[Runner] STOP → process_controller.request_stop()")
                self.state("STOP 요청 중... (공정 종료 대기)")
            elif self._cmd is not None:
                self._cancel = True
            else:
                self._cmd = ("STOP_IDLE", by)
            self.lk.notify_all()
        if self.runner.is_running:
            self.runner.set_pause(False)
            self.runner.request_stop()
        else:
            self._ensure_thread()

    def abort(self, reason: str, detail: str = "", source: str = "SAFETY") -> None:
        """안전 규칙·장비 이상 중단 — 공정 중이면 v1 _step_failed 와 같은 경로(정지 순서 첫 단계 Main Shutter 닫기),
        시작 전 단계면 STOP 과 같은 정리. run_end error, 사유 코드 = reason."""
        with self.lk:
            self._abort_code = reason
            self.queue_mode = False
            self.paused = False
            running = self.runner.is_running
        if running:
            self.runner.set_pause(False)
            self.runner.step_failed(source, detail or reason, code=reason)
        elif self._cmd is not None:
            with self.lk:
                self._cancel = True
                self.lk.notify_all()

    def _stop_before_start(self) -> None:
        """_runner_handle_stop 의 '공정 시작 전' 경로(4820-4839)."""
        self.log("MAIN", "[Runner] STOP → 공정 시작 전 상태, 즉시 정리(cleanup)")
        self.actions.apply_polling(False, {})
        self.actions.cleanup_devices()
        if self._abort_code:
            self.emit("run_end", self._abort_code, self._abort_code, level="error", result="error")
            self.error, self._abort_code = self._abort_code, None
        else:
            self.emit("run_end", "사용자 STOP (시작 전)", "USER_STOP", result="aborted")
        self._reset_after_list()

    def close(self) -> None:
        with self.lk:
            self._closing = True
            self.lk.notify_all()
        try:
            self.sys.event_listeners.remove(self.actions.on_event)
        except ValueError:
            pass
        for o in (self.calib, self.excel):
            if o is not None:
                try:
                    o.sender.stop(1.0)
                except Exception:
                    pass
        self.writer.stop(3.0)

    # ═══════════════════════ 1초 행·ETA ═══════════════════════
    def on_tick_start(self, tick) -> None:
        if self.thickness is not None:
            self.thickness.poll()

    def on_row(self, view) -> None:
        if self.thickness is not None:
            self.thickness.on_row(view)

    def eta_remaining_s(self) -> Optional[int]:
        """eta.CHn.remaining_s — 큐의 마지막 행이 아니면 null(host/handlers.py:520-524)."""
        if self.queue_mode and self.run_idx < len(self.runs) - 1:
            return None
        return self.runner.eta_remaining_s()

    def min_total_s(self) -> Optional[int]:
        """START_SPUTTER min_total_s — 큐(파일 레시피)가 있을 때만(rows[현재:]), 단일 시작이면 null."""
        if self.rows is None:
            return None
        sup = bool(self.thickness is not None and self.thickness.supported)
        return estimate_min_total_s(raw_rows_from(self.runs, max(0, self.run_idx)), self.cfg, self.n, sup)

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

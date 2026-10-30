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
from v2.ch12.process_records import ProcessRecordsMixin
from v2.ch12.process_view import ProcessViewMixin
from v2.ch12.process_list import USE_KEYS, RunPlan, derive_params, plan_runs, raw_rows_from, stop_uses
from v2.ch12.process_runner import StepRunner, delay_countdown, fmt_hms
from v2.ch12.process_steps import caps_from_cfg, shutdown_steps, step_cfg
from v2.ch12.recipe_check import check_norm_params

EVENTS_KEEP = 20000
COOLDOWN_S = 60.0                     # runtime_state.remaining_cooldown(…, 60.0)
OWNERS = ("robot", "web", "local")


@dataclass
class ProcEvent:
    kind: str
    detail: str = ""
    code: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
    level: str = "info"


class ProcessEngine(AdjustMixin, PF.PreflightMixin, ProcessRecordsMixin, ProcessViewMixin):
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
        if heater is not None:
            heater.next_step_fn = self._next_step_message           # 히터 도달 카드의 '다음 단계'(v1 next_step_message)
            heater.on_sample = self._heater_sample                 # Calib CSV·엑셀 히터 열 표본(v1 DataLogger.log_heater)
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
        self._exiting = False                   # 의도적 종료(program_exit) — 끝 처리·정리 없이 멈춤
        self._resume_plan: Optional[dict] = None   # 재시작 뒤 이어가기/안전 정지(resume_from·stop_from_journal)
        self._restart_pending = False           # 재시작 판단(resume.py) 전 — 새 시작을 받지 않는다
        self._thread = None
        self.preflight_hooks = {"gate": PF.gate_check, "main_valve": PF.main_valve_check, "chuck": PF.chuck_move}
        self.gun_targets = None                 # (run) -> None 재고 DB(inventory_db.attach — v1 5080-5098, 최대 3 s)
        self.shutter_listeners: list = []       # (opened) — Main Shutter 확인 때(process_monitor.attach)
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

    def emit(self, kind: str, detail: str = "", code: Optional[str] = None, level: str = "info",
             data: Optional[dict] = None, **kw) -> ProcEvent:
        e = ProcEvent(kind, detail, code, dict(data or {}, **kw), level)
        self.events.append(e)
        self.sys.emit(f"engine:{self.ch}", kind, detail, level=level, code=code, event=e)
        return e

    def chat(self, text: str) -> None:
        self.emit("chat", text, level="warn")

    def popup(self, title: str, text: str, level: str = "warn") -> None:
        self.emit("popup", text, level=level, title=title)

    @property
    def busy(self) -> bool:
        """is_busy(3279-3290) — 시작 명령 접수 ~ 큐 완주/정리 완료. 재시작 뒤 이어가기 판단 전(_restart_pending)도 busy
        (안전 규칙의 '시작 때 남은 파워' 정리가 이어갈 공정의 출력을 끄지 않게)."""
        return bool(getattr(self, "_restart_pending", False)) or self._cmd is not None or self.state_name != "IDLE" \
            or self.runner.is_running

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

    def _next_step_message(self) -> str:
        r = self.runner
        i = r.idx + 1
        return str(r.steps[i].message or "") if 0 <= i < len(r.steps) else ""

    def on_main_shutter(self, opened: bool) -> None:
        (self.stats.shutter_open if opened else self.stats.shutter_close)()
        for f in list(self.shutter_listeners):
            try:
                f(opened)
            except Exception as e:
                self.log("MAIN", f"셔터 연동 예외: {e!r}")

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
        if self._restart_pending:
            self.reject_detail = "재시작 뒤 이어가기 판단 중"
            return "RESTART_PENDING"
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
            if params is not None and self.rows is None:   # v1: 파일 큐가 있으면 화면 값은 쓰지 않는다(START_QUEUE)
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
        from v2.ch12.recipe import thickness_mode_of
        p = dict(params)
        if not p.get("test_mode") and thickness_mode_of(p.get("thickness"), self.caps.thickness):
            p["thickness_mode"] = True                     # v1: 시작 전 점검 뒤 params thickness_mode → 컨트롤러가 두께 메인
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
                    self._stop_before_start(idle=True)
                elif kind == "RESUME":
                    plan, self._resume_plan = self._resume_plan, None
                    self._run_list(self.queue_mode, resume=plan)
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
    def _run_list(self, queue: bool, resume: Optional[dict] = None) -> None:
        """런을 차례로. resume = 재시작 뒤 되살린 런(process_view._resume_one) — 그 런부터 이어서."""
        self.queue_mode = queue
        self.run_idx = -1 if resume is None else self.run_idx - 1
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
                if self.gun_targets is not None and resume is None:
                    self.gun_targets(run)              # G1~G3 Target 이 모두 비면 재고 DB 최대 3 s(v1 5080-5098)
            try:
                if resume is not None:
                    plan, resume = resume, None
                    got = self._resume_one(run, plan)
                    if got is None:
                        continue
                    ok, stopped = got
                elif run.kind == "delay":
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
                else:
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
        if run.params and self.seq is not None and self.seq.rev:          # 공정 중 편집이 있었으면 사용 플래그는 블록에서
            d = derive_params(run.blocks(self.blocks), self.sc)
            params.update({k: d[k] for k in USE_KEYS if k in d})
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
        if errs and not self.queue_mode:                   # 단일(화면) 시작 — 로그 파일을 먼저 열고 검사(v1 4472-4480)
            if self.txt.path is None:
                self.txt.open_run(params, origin=self.origin, origin_meta=self.origin_meta)
            self._host(False, "; ".join(errs))
            self.popup("입력값 확인", "\n".join(f"- {x}" for x in errs))
            self.error = "RECIPE_PARAM_ERROR"
            self.emit("run_end", "; ".join(errs), "RECIPE_PARAM_ERROR", level="error", result="error")
            return False, False
        if not self.queue_mode:
            self.log("MAIN", "입력 검증 통과 → Runner START")
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
        if self._exiting:                                  # 의도적 종료 — run_end 는 program_exit 가 냈다
            return False, True
        self._after_finish(run, res.ok, detail)
        return res.ok, bool(detail.get("stopped"))

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

    def _stop_before_start(self, idle: bool = False) -> None:
        """_runner_handle_stop 의 '공정 시작 전' 경로(4820-4839). idle = 아무 단계도 없을 때 받은 STOP(정리만, run_end 없음)."""
        if self._exiting:                                  # 의도적 종료 — 장비 정리 없이(파워만 끄기는 부른 쪽)
            self._reset_after_list()
            return
        self.log("MAIN", "[Runner] STOP → 공정 시작 전 상태, 즉시 정리(cleanup)")
        self.actions.apply_polling(False, {})
        self.actions.cleanup_devices()
        if idle:
            pass
        elif self._abort_code:
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

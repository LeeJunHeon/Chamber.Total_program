"""플라즈마 클리닝 엔진 — v1 runtime/plasma_cleaning_runtime.py(PlasmaCleaningRuntime) + controller/plasma_cleaning_controller.py 동작 그대로.

나눔: 공정 순서·실패 기록은 pc_steps(PcController·StepIO), 프리플라이트는 pc_preflight, 정지·정리는 pc_stop, 화면 값·CSV·TEST·호스트
응답 코드는 pc_recipe, 공정 로그 파일은 pc_log, 쿨다운·실행 표시는 run_state(v1 runtime_state). 이 파일은 시작·STOP·채널·결과 판정·
종료 통지·이벤트 받기·1초 행·저널.

장비(PC-001·003·064): PLC 는 출처 'PC' proxy, Gas Flow 는 늘 MFC1(ch1.mfc), SP4 압력은 선택 챔버 MFC, IG 도 선택 챔버, RF 는 선택과
  상관없이 DCV ch1(system.rf['pc'] — SET 래치 DCV_SET_1 은 RfPower 의 power_enable 한 경로만, 강제 SET OFF 는 정지 정리만).
채널 선택(select_channel — v1 main._on_pc_radio_toggled·_apply_pc_ch_selection·set_selected_ch): 실행 중이면 거절(로그
  '플라즈마 클리닝 실행 중에는 챔버 전환이 불가합니다.'), 같은 선택이면 무시, 아니면 IG·MFC 교체 → 'Bind MFC: …' → 'Selected Chamber → CHn'
  → '[Plasma Cleaning] Use CHn IG, SP4 → MFCn, GasFlow → MFC1 ch3'. 레시피 use_ch 도 같은 길(실행 중이면 무시 — PC-004).
시작(start — v1 _on_click_start 1213-1657): TEST 판정(마지막으로 불러온 행) → 실행 중 E205 → 쿨다운(PC 전역·같은 CH 챔버·CH1 이면 TSP 60 s,
  코드 없음 → 핸들러 E700) → 같은 CH PC 실행 중 E205 → 같은 CH 챔버 실행 중·바쁨 E205 → 수락: 결과 플래그 초기화(거절 전에는 안 함 —
  TEST-112) → mark_started. 실공정은 프리플라이트(연결 실패 → 장치 명령 없이 정리, GV 인터락 읽기 실패·FALSE(E302) → _final_cleanup —
  PC-096) → 'preflight OK' → MFC 소유권·폴링 → 공정 로그 → 시작 카드 → 컨트롤러 → 판정 → [A] 정리 → [A2] RF OFF 미확인이면 실패 →
  [B] 화면 복구 → [C] 종료 통지(쿨다운 시작 — 프리플라이트 실패도 쿨다운, PC-101) → [D] 성공만 엑셀 'Plasma Cleaning' 시트 → [D-2] 완료 콜백.
  [이상] 프리플라이트 중 STOP 은 지워진다(컨트롤러는 실행마다 새 정지 신호, 런타임도 공정 로그를 연 뒤 _stop_requested=False — 결정 후보).
호스트(로봇) START(host_start — v1 start_with_recipe_string 3190-3301): E206 중복 → CSV(E207·E209·E227·E208)·형식(E209) → 실행 중 E205(레시피
  적용 전) → 시작 → 프리플라이트 신호를 PC_HOST_START_WAIT_TIMEOUT_S(20 s) 기다림(넘으면 'preflight timeout(20.0s) …' → E700). 결과는
  on_result(HostStartResult) — code 는 핸들러 분류까지 끝난 값(host/handlers.py:989-1001), raw_code 는 런타임이 실은 코드.
STOP(stop — 1659-1688): 실행 중 아님/이미 종료 통지/이미 정지 요청이면 무시 로그, 아니면 '[STOP] 사용자 정지 요청 수신'·컨트롤러 정지.
  [이상] 사용자 STOP 도 ok=False 라 '❌ Plasma Cleaning 실패 이유: 사용자 STOP' 카드가 나간다(PC-100, D108).
카드·팝업: 이벤트로 낸다(PcEvent kind 'card'·'popup' — 구글챗·창은 포털 몫, 같은 계기·같은 내용). 'log' 는 PC 로그창 한 줄.
엔진 계약(system.register_engine): on_tick_start·on_row·snapshot_fields()(pc.proc.*·pc.set.*)·busy·close, 명령 start·stop·select_channel·
  state·host_start·load_recipe_rows·clear_error, 안전 규칙 abort(사유), 저널 state_for_journal()·resume_checks(상태).
  이어가기: 상태를 유지하는 블록은 메인 클리닝 시간 대기(step 'process')뿐 — 그 단계만 이어가기 가능, 나머지는 불가.
"""
from __future__ import annotations

import collections
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional, Tuple

from v2.ch12 import pc_recipe as R
from v2.ch12.drivers import mfc as M
from v2.ch12.drivers import mfc_ops
from v2.ch12.pc_log import PcRunLog, hidden
from v2.ch12.pc_preflight import PreflightIO
from v2.ch12.pc_recipe import HostStartError, HostStartResult, PcParams
from v2.ch12.pc_steps import Box, PcController, StepIO
from v2.ch12.pc_stop import StopIO, apply_rf_off_unconfirmed
from v2.ch12.run_state import RunState

COOLDOWN_S = 60.0                     # runtime_state.pc_block_reason(ch, cooldown_s=60.0) — 1244
UI_LOG_MAX = 5000                     # 196-210
EVENTS_KEEP = 20000
STEPS = (("preflight", "프리플라이트"), ("gv_interlock", "GV 인터락"), ("gv_open", "GV 열기"), ("ig_base", "IG base 대기"),
         ("gas_select", "가스 선택"), ("gas_flow", "가스 유량"), ("sp4_set", "SP4 설정"), ("sp4_wait", "SP4 압력 대기"),
         ("rf_start", "RF 시작"), ("process", "클리닝"), ("cleanup", "종료 정리"))
STEP_NO = {k: i + 1 for i, (k, _) in enumerate(STEPS)}
STEP_LABEL = dict(STEPS)
RESUMABLE_STEPS = frozenset({"process"})            # 상태를 유지하는 블록 = 메인 클리닝 시간 대기뿐


@dataclass(frozen=True)
class PcEvent:
    kind: str                       # log / card / popup / state / host
    detail: str = ""
    reason: Optional[str] = None
    code: Optional[str] = None
    level: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StartResult:
    accepted: bool
    message: str = ""
    code: Optional[str] = None


class PcDevices:
    """PC 가 쓰는 장비 묶음 — Gas 는 늘 MFC1, SP4·IG 는 선택 챔버(bind)."""

    def __init__(self, plc, plc_drv, rf, mfc: Mapping[str, Any], ig: Mapping[str, Any], camera=None,
                 camera_share=None) -> None:
        self.plc, self.plc_drv, self.rf = plc, plc_drv, rf
        self.mfc, self.igs = dict(mfc), dict(ig)
        self.camera, self.camera_share = camera, camera_share
        self.mfc_gas = self.mfc.get("ch1")
        self.mfc_pressure = self.mfc.get("ch1")
        self.ig = self.igs.get("ch1")

    def bind(self, ch: int) -> None:
        self.mfc_gas = self.mfc.get("ch1")
        self.mfc_pressure = self.mfc.get("ch1" if ch == 1 else "ch2")
        self.ig = self.igs.get("ch1" if ch == 1 else "ch2")


class PlasmaCleaning(StepIO, StopIO, PreflightIO):
    def __init__(self, clock, cfg: Mapping[str, Any], dev: PcDevices, *, run_state: Optional[RunState] = None,
                 chamber_busy: Optional[Callable[[int], bool]] = None, host_log=None, excel: Optional[dict] = None,
                 run_log: Optional[PcRunLog] = None, on_event: Optional[Callable[[PcEvent], None]] = None,
                 on_pc_done: Optional[Callable[[int, str], None]] = None) -> None:
        self.clock, self.cfg, self.dev = clock, cfg, dev
        self.run_state = run_state if run_state is not None else RunState(clock)
        self.chamber_busy_probe = chamber_busy
        self.host_log = host_log
        self.excel = excel if excel is not None else {}
        self.run_log = run_log if run_log is not None else PcRunLog(cfg, now=self._now_dt)
        self.on_event = on_event
        self.on_pc_done = on_pc_done
        self.events: Deque[PcEvent] = collections.deque(maxlen=EVENTS_KEEP)
        self.ui_log: Deque[str] = collections.deque(maxlen=UI_LOG_MAX)
        self.pc = PcController(clock, self.log)
        self._lock = threading.RLock()
        self.ch = 1
        self.ui: Dict[str, Any] = R.blank_ui()
        self.ui_fwd = self.ui_ref = ""
        self.loaded_row: Optional[Dict[str, str]] = None
        self.running = False
        self.phase = "idle"
        self.stop_requested = False
        self.final_notified = False
        self.test_mode_active = False
        self.cleanup_started = False
        self.forced_fail = False
        self.forced_fail_reason: Optional[str] = None
        self.rf_off_unconfirmed_reason: Optional[str] = None
        self.abort_reason: Optional[str] = None
        self.process_timer_active = False
        self.rf_target_ok: Optional[bool] = None
        self.rf_target_reason = ""
        self.rf_target_evt = False
        self.state_header = ""
        self.state_text = "대기 중"
        self.last_process_time_min: Optional[float] = None
        self.countdown_left: Optional[int] = None
        self.gas_idx: Optional[int] = None
        self.params: Optional[PcParams] = None
        self.run_owner: Optional[str] = None
        self.mfc_claims: Dict[str, str] = {}
        self.mfc_release_done = False
        self.pumps: Dict[str, str] = {}           # 블록 → 펌프 라벨(떠 있는 동안만)
        self.ig_pump: Optional[Tuple[str, str]] = None
        self.cam_owner_active = ""
        self.readings: Dict[str, List[float]] = {k: [] for k in ("forp", "refp", "flow", "pressure", "ig")}
        self.process_name = ""
        self.run_id: Optional[str] = None
        self.owner_src: Optional[str] = None
        self.by = ""
        self.origin = "ui"
        self.origin_meta: Dict[str, Any] = {}
        self.host_key: Optional[str] = None
        self.host_recipe_path = ""
        self.host_recipe_rows = 1
        self._host_pending: Optional[Box] = None
        self._host_start_code: Optional[str] = None
        self._on_preflight: Optional[Callable[[bool, str, Optional[str]], None]] = None
        self._step_t0: Optional[float] = None
        self._last_step = ""
        self._resume_left: Optional[int] = None
        self._thread = None
        self.last_view = None
        self.last_final: Optional[Dict[str, Any]] = None
        self._n_runs = 0

    # ───────── 공통 ─────────
    def _now_dt(self) -> datetime:
        return datetime.fromtimestamp(self.clock.wall())

    def emit(self, ev: PcEvent) -> None:
        self.events.append(ev)
        if self.on_event is not None:
            try:
                self.on_event(ev)
            except Exception:
                pass

    def log(self, src: str, msg: str) -> None:
        """append_log(2314-2394) — '[poll]' 줄은 버림, 화면 줄 + 파일(없으면 SYSTEM)."""
        if hidden(src, msg):
            return
        line = f"[{self._now_dt().strftime('%H:%M:%S')}] {src}: {msg}"
        self.ui_log.append(line)
        try:
            self.run_log.line(line)
        except Exception:
            pass
        self.emit(PcEvent("log", line, data={"source": src, "msg": msg}))

    def card(self, api: str, level: str = "info", code: Optional[str] = None, **data) -> None:
        """구글챗 카드 자리(v1 chat_pc 호출과 같은 이름·인자) — 포털이 보낸다."""
        self.emit(PcEvent("card", str(data.get("text") or data.get("payload", {}).get("status", "") or api),
                          code=code, level=level, data=dict(data, api=api, chat="PC")))

    def popup(self, kind: str, title: str, text: str, ch: Optional[int] = None, clear_on_close: bool = False) -> None:
        """경고(warning — 5 s 자동 닫힘)·치명(critical — 닫으면 clear_error) 창 자리(2854-2942)."""
        self.emit(PcEvent("popup", f"{title}: {text}", level="error" if kind == "critical" else "warn",
                          data={"kind": kind, "title": title, "text": text, "ch": ch, "clear_on_close": clear_on_close}))

    def set_state_text(self, text: str) -> None:
        self.state_text = str(text)

    def mfc_owner(self) -> str:
        return self.run_owner or f"pc{int(self.ch)}"

    def mfc_name(self, m) -> str:
        if m is None:
            return "None"
        host, port = M.endpoint(self.cfg, m.chamber)
        return f"{host}:{port}"

    def stop_reason_word(self) -> str:
        return self.abort_reason or "사용자 STOP"

    def resume_left(self, total: int) -> int:
        left, self._resume_left = self._resume_left, None
        return total if left is None else max(0, min(int(left), total))

    # ───────── 채널 ─────────
    def set_selected_ch(self, ch: int) -> bool:
        new = 1 if int(ch) != 2 else 2
        if self.running and new != self.ch:
            self.log("PC", f"실행 중 → 챔버 선택 변경 무시 (요청 CH{new}, 현재 CH{self.ch})")
            return False
        self.ch = new
        self.log("PC", f"Selected Chamber → CH{self.ch}")
        return True

    def select_channel(self, ch: int) -> Tuple[bool, str]:
        """라디오(v1 main 855-967). (적용됨, 문구)."""
        new = 2 if int(ch) == 2 else 1
        if self.running:
            msg = "플라즈마 클리닝 실행 중에는 챔버 전환이 불가합니다."
            self.log("PC", msg)
            return False, msg
        if new == self.ch:
            return True, ""
        self._apply_channel(new)
        return True, ""

    def _apply_channel(self, ch: int) -> None:
        had = bool(self.mfc_claims)
        for key, owner in list(self.mfc_claims.items()):
            for m in self.mfc_targets():
                if m.resource_key == key:
                    m.poll_users.discard(owner)
        self.mfc_claims.clear()
        self.dev.bind(ch)
        if had:
            self.acquire_mfcs()
        if self.run_owner:
            self.run_state.begin_run_use(self.run_owner, [m.resource_key for m in self.mfc_targets()])
        self.log("PC", f"Bind MFC: GAS={self.mfc_name(self.dev.mfc_gas)}, SP4={self.mfc_name(self.dev.mfc_pressure)}")
        self.set_selected_ch(ch)
        self.log("PC", f"[Plasma Cleaning] Use CH{ch} IG, SP4 → MFC{ch}, GasFlow → MFC1 ch3")

    def _apply_row(self, row: Mapping[str, Any]) -> None:
        use = R.apply_row(self.ui, row)
        if use is not None:
            v = 2 if use == 2 else 1
            if not self.running and v != self.ch:
                self.dev.bind(v)                         # v1: 라디오 setChecked → main 이 장치 교체
                self.log("PC", f"Bind MFC: GAS={self.mfc_name(self.dev.mfc_gas)}, "
                               f"SP4={self.mfc_name(self.dev.mfc_pressure)}")
            self.set_selected_ch(use)

    def load_recipe_rows(self, rows, path: str = "") -> Tuple[bool, str]:
        """CSV 불러오기(PC-087) — 첫 '비어 있지 않은' 행을 화면에(공정은 시작하지 않음). 불러온 행은 TEST 판정에 남는다(PC-094)."""
        row = R.first_row_of(rows)
        if not row:
            self.popup("warning", "CSV 오류", "데이터 행이 없습니다.")
            return False, "데이터 행이 없습니다."
        self.loaded_row = dict(row)
        self._apply_row(row)
        self.log("File", f"CSV 로드 완료: {path}\n→ UI에 값 세팅")
        return True, ""

    # ───────── 시작 ─────────
    def _host_report(self, ok: bool, reason: str = "", code: Optional[str] = None) -> None:
        if code is not None:
            self._host_start_code = str(code)
        cb, self._on_preflight = self._on_preflight, None
        if cb is not None:
            try:
                cb(bool(ok), str(reason), code)
            except Exception:
                pass
        hp = self._host_pending
        if hp is not None and not hp.done:
            hp.set((bool(ok), str(reason)))

    def _chamber_busy(self, ch: int) -> bool:
        p = self.chamber_busy_probe
        if not callable(p):
            return False
        try:
            return bool(p(int(ch)))
        except Exception:
            return False

    def start(self, params: Optional[Mapping[str, Any]] = None, rows=None, ch: Optional[int] = None,
              source: str = "local", by: str = "", origin: str = "ui", origin_meta: Optional[Mapping[str, Any]] = None,
              on_preflight: Optional[Callable[[bool, str, Optional[str]], None]] = None) -> StartResult:
        """명령 start — rows(CSV 행)·params(화면 값)·ch 를 먼저 넣고 v1 _on_click_start. on_preflight(ok, 사유, 코드)는 v1
        _host_report_start 와 같은 시점(거절 즉시·TEST 시작·프리플라이트 결과)에 한 번."""
        if rows is not None and not self.running:
            self.load_recipe_rows(rows)
        if params:
            if not self.running:
                for k in R.UI_KEYS:
                    if k in params and params[k] is not None:
                        self.ui[k] = str(params[k])
        if ch is not None and not self.running:
            self.select_channel(ch)
        self._on_preflight = on_preflight
        return self._click_start(source=source, by=by, origin=origin, origin_meta=dict(origin_meta or {}))

    def _reject(self, title: str, msg: str, code: Optional[str], host_msg: Optional[str] = None) -> StartResult:
        self.popup("warning", title, msg)
        self._host_report(False, host_msg or msg, code)
        return StartResult(False, host_msg or msg, code)

    def _click_start(self, source: str, by: str, origin: str, origin_meta: Dict[str, Any]) -> StartResult:
        test_mode, test_dur = R.test_mode_of(self.loaded_row)
        if self.running:
            return self._reject("실행 중", "이미 Plasma Cleaning이 실행 중입니다.", "E205")
        ch = int(self.ch)
        ok_cool, remain, _ = self.run_state.pc_block_reason(ch, cooldown_s=COOLDOWN_S)
        if not ok_cool:
            secs = int(float(remain) + 0.999)
            msg = f"이전 공정 종료 후 1분 대기 필요합니다. 약 {secs}초 후에 다시 시도하십시오."
            self.popup("warning", "대기 필요", f"이전 공정 종료 후 1분 대기 필요합니다.\n{secs}초 후에 시작하십시오.")
            self._host_report(False, msg)
            return StartResult(False, msg, None)
        if self.run_state.is_running("pc", ch):
            return self._reject("실행 오류", f"CH{ch} Plasma Cleaning이 이미 실행 중입니다.", "E205")
        if self.run_state.is_running("chamber", ch) or self._chamber_busy(ch):
            return self._reject("실행 오류", f"CH{ch}는 이미 다른 공정이 실행 중입니다.", "E205")
        # 수락 — 결과 플래그 초기화는 여기서만(TEST-112)
        self.cleanup_started = False
        self.forced_fail, self.forced_fail_reason = False, None
        self.rf_off_unconfirmed_reason = None
        self.abort_reason = None
        self.run_state.mark_started("pc", ch)
        self._n_runs += 1
        self.run_id = f"pc-{self._now_dt():%Y%m%d%H%M%S}-{self._n_runs}"
        self.owner_src, self.by, self.origin, self.origin_meta = source, by, origin, origin_meta
        self.running = True
        self.stop_requested = False
        self.final_notified = False
        self.phase = "test" if test_mode else "preflight"
        self._set_step("preflight")
        target = (lambda: self._run_test(ch, test_dur)) if test_mode else (lambda: self._run_real(ch))
        self._thread = self.clock.start_thread(target, f"pc-run-ch{ch}")
        return StartResult(True, "TEST MODE" if test_mode else "", None)

    # ───────── TEST 모드(1297-1400) ─────────
    def _run_test(self, ch: int, dur: float) -> None:
        ok_final, stopped_final, final_reason = True, False, None
        try:
            self.pumps_off()
            self.log("MAIN", f"[TEST MODE] {dur:.1f}s 동안 시뮬레이션 실행 (장비 제어 없음)")
            self._host_report(True, "TEST MODE (skip preflight)")
            self.ui["process_time"] = f"{(dur / 60.0):.2f}"
            self.run_state.set_running("pc", True, ch)
            p = R.read_params(self.ui, self.cfg)
            self.params = p
            self.last_process_time_min = float(p.process_time_min)
            self.test_mode_active = True
            self.stop_requested = bool(self.abort_reason)        # 안전 중단(v2)은 지우지 않는다
            self.final_notified = False
            self.set_state_text("TEST MODE · Preparing…")
            self._open_run_log(p)
            self.log("PC", "파일 로그 시작 (TEST MODE)")
            self._set_step("process")
            try:
                self.card("notify_process_started", payload={
                    "process_note": "Plasma Cleaning (TEST)", "process_time": float(p.process_time_min),
                    "use_rf_power": False, "rf_power": 0.0, "prefix": f"CH{self.ch} Plasma Cleaning (TEST)",
                    "ch": self.ch}, flush=True)
                end_t = self.clock.monotonic() + float(dur)
                last = None
                while True:
                    if self.stop_requested:
                        stopped_final, ok_final, final_reason = True, False, self.stop_reason_word()
                        break
                    remain = int(max(0.0, end_t - self.clock.monotonic()) + 0.999)
                    if last != remain:
                        last = remain
                        mm, ss = divmod(remain, 60)
                        self.set_state_text(f"TEST MODE · {mm:02d}:{ss:02d} ({remain / 60.0:.2f} min)")
                        self.ui["process_time"] = f"{mm:02d}:{ss:02d}"
                        self.countdown_left = remain
                    if remain <= 0:
                        break
                    self.clock.wait_until(self.clock.monotonic() + 0.2)
            except Exception as e:
                ok_final, stopped_final, final_reason = False, False, f"{type(e).__name__}: {e!s}"
                self.log("PC", f"[TEST MODE] 오류: {e!r}")
        finally:
            self._set_step("cleanup")
            self.final_cleanup()
            self.running = False
            self.process_timer_active = False
            self.test_mode_active = False
            self._reset_ui(self.last_process_time_min)
            self.set_state_text("대기 중")
            self.log("MAIN", "[TEST MODE] 종료 및 UI 복구 완료")
            self._notify_finish_once(ok_final, final_reason, stopped_final, ch)
            self._idle()

    # ───────── 실공정(1402-1657) ─────────
    def _run_real(self, ch: int) -> None:
        p: Optional[PcParams] = None
        self._host_run_begin()
        self.run_state.set_running("pc", True, ch)
        self.run_owner = f"pc{ch}"
        self.mfc_release_done = False
        self.run_state.begin_run_use(self.run_owner, [m.resource_key for m in self.mfc_targets()])
        self.set_state_text("프리플라이트 중…")
        try:
            self.preflight_connect()
        except Exception as e:
            msg = f"장치 연결에 실패했습니다: {e}"
            self.popup("critical", "연결 실패", f"장치 연결에 실패했습니다.\n\n{e}\n\n확인을 누르면 상태 표시가 Idle로 변경됩니다.",
                       ch=ch, clear_on_close=True)
            self._host_report(False, msg)
            self.run_state.set_error("pc", ch, msg)
            self.run_state.mark_finished("pc", ch)
            self.end_mfc_run_use(clear_owner=True)
            self.cleanup_tasks_only("preflight_connect failed")
            self.running = False
            self._reset_ui(None)
            self._idle()
            return
        self._set_step("gv_interlock")
        gv, err = self.preflight_gv_interlock()
        if gv is None or not gv:
            if gv is None:
                msg = f"게이트밸브 인터락 상태 확인 실패: {err}"
                self.popup("critical", "GV 인터락 오류", f"{msg}\n\n확인을 누르면 상태 표시가 Idle로 변경됩니다.", ch=ch,
                           clear_on_close=True)
                self._host_report(False, msg)
            else:
                msg = f"게이트밸브 인터락 FALSE (G_V_{ch}_인터락=FALSE) → LoadLock의 압력을 확인하십시오."
                self.log("PLC", msg)
                self.popup("critical", "게이트밸브 인터락", msg, ch=ch, clear_on_close=True)
                self._host_report(False, msg, "E302")
            self._set_step("cleanup")
            try:
                self.final_cleanup()
            except Exception:
                pass
            self.run_state.set_error("pc", ch, msg)
            self.run_state.mark_finished("pc", ch)
            self.running = False
            self._reset_ui(None)
            self._idle()
            return
        self._host_report(True, "preflight OK")
        self.readings = {k: [] for k in self.readings}
        self.process_name = str(self.ui.get("process_name") or "").strip()
        try:
            self.acquire_mfcs()
            if self.dev.mfc_gas is not None:
                self.dev.mfc_gas.set_process_status(True)
            if self.dev.mfc_pressure is not None and self.dev.mfc_pressure is not self.dev.mfc_gas:
                self.dev.mfc_pressure.set_process_status(True)
        except Exception:
            pass
        p = R.read_params(self.ui, self.cfg)
        self.params = p
        self.last_process_time_min = float(p.process_time_min)
        self.phase = "run"
        self.set_state_text("Preparing…")
        self._open_run_log(p)
        self.log("PC", "파일 로그 시작")
        self.stop_requested = False                      # [이상] 프리플라이트 중 STOP 은 여기서 지워진다(v1 1541)
        self.final_notified = False
        ok_final, stopped_final, final_reason = False, False, None
        try:
            self.card("notify_process_started", payload={
                "process_note": "Plasma Cleaning", "process_time": float(p.process_time_min), "use_rf_power": True,
                "rf_power": float(p.rf_power_w), "prefix": f"CH{self.ch} Plasma Cleaning", "ch": self.ch}, flush=True)
            self.pc.run(p, self)
            lr = str(self.pc.last_result or "").strip().lower()
            ls = str(self.pc.last_reason or "").strip()
            led = str(self.pc.last_error_detail or "").strip()
            stopped_final = bool(self.stop_requested or lr == "stop")
            ok_final = lr == "success"
            final_reason = None if ok_final else (led or ls or "runtime/controller error")
            if self.forced_fail:
                ok_final, stopped_final = False, False
                if self.forced_fail_reason:
                    final_reason = str(self.forced_fail_reason)
            if lr == "success" and not self.stop_requested and not self.forced_fail:
                ok_final, stopped_final, final_reason = True, False, None
            self.log("PC", f"Final notify ok={ok_final}, stopped={stopped_final}, lr={lr!r}, reason={final_reason!r}")
        except Exception as e:
            ok_final, stopped_final, final_reason = False, False, f"{type(e).__name__}: {e!s}"
            self.log("PC", f"오류: {e!r}")
        finally:
            self._set_step("cleanup")
            self.final_cleanup()
            ok_final, stopped_final, final_reason = apply_rf_off_unconfirmed(ok_final, stopped_final, final_reason,
                                                                             self.rf_off_unconfirmed_reason)
            self.running = False
            self.process_timer_active = False
            self._reset_ui(self.last_process_time_min)
            self.set_state_text("대기 중")
            self.log("MAIN", "[FINALLY] idle UI 복구 완료")
            self._notify_finish_once(ok_final, final_reason, stopped_final, ch)
            if ok_final and not stopped_final and p is not None:
                self._save_excel(ch, p)
            if self.on_pc_done is not None:
                try:
                    self.on_pc_done(int(ch), self.process_name)
                except Exception:
                    pass
            self._idle()

    def _idle(self) -> None:
        self.phase = "idle"
        self._set_step("")
        self.countdown_left = None

    def _open_run_log(self, p: PcParams) -> None:
        try:
            self.run_log.open(self.ch, p, self.mfc_name(self.dev.mfc_gas), self.mfc_name(self.dev.mfc_pressure))
        except Exception as e:
            self.log("PC", f"런 로그 파일 열기 실패 → 파일 기록 없이 진행: {e!r}")

    def _reset_ui(self, restore_time_min: Optional[float]) -> None:
        self.process_timer_active = False
        self.state_header = ""
        self.rf_target_evt, self.rf_target_ok, self.rf_target_reason = False, None, ""
        self.forced_fail, self.forced_fail_reason = False, None
        self.set_state_text("대기 중")
        if restore_time_min is not None:
            self.ui["process_time"] = f"{float(restore_time_min):.2f}"
        self.ui_fwd = self.ui_ref = ""

    def build_pc_params(self, p: PcParams) -> Dict[str, Any]:
        """_build_pc_params_dict(2168-2188) — base_pressure 는 IG 마지막 값."""
        def avg(xs):
            return round(sum(xs) / len(xs), 4) if xs else None
        r = self.readings
        return {"process_name": self.process_name or "Plasma Cleaning", "time": float(p.process_time_min),
                "base_pressure": r["ig"][-1] if r["ig"] else None, "sp_ar": float(p.gas_flow_sccm),
                "avg_ar": avg(r["flow"]), "sp_pressure": float(p.sp4_setpoint_mTorr), "avg_pressure": avg(r["pressure"]),
                "sp_power": float(p.rf_power_w), "avg_forp": avg(r["forp"]), "avg_refp": avg(r["refp"]),
                "avg_load": None, "avg_tune": None}

    def _save_excel(self, ch: int, p: PcParams) -> None:
        try:
            x = self.excel.get(int(ch))
            if x is None:
                from v2.ch12.outputs.excel_log import ExcelLog
                x = self.excel[int(ch)] = ExcelLog(int(ch), self.cfg, now=self._now_dt)
            x.save_pc(self.build_pc_params(p))
        except Exception:
            pass

    # ───────── 호스트 공정 로그(2688-2728) ─────────
    def _host_run_begin(self) -> None:
        self.host_key = None
        if str(self.origin or "ui") != "host":
            return
        key = str(self.origin_meta.get("key") or "")
        hl = self.host_log
        if hl is None or not key:
            return
        try:
            hl.mark_owned(key)
            pname = str((self.loaded_row or {}).get("Process_name") or (self.loaded_row or {}).get("process_name")
                        or "").strip()
            hl.update(key, recipe_name=os.path.basename(self.host_recipe_path) if self.host_recipe_path else "",
                      row_count=int(self.host_recipe_rows or 1), process_names=pname)
            hl.mark_started(key, self._now_dt(), log_file=None)         # [이상] log_file 늘 빈칸(PC-098)
        except Exception:
            pass
        self.host_key = key

    # ───────── 종료 통지(2730-2852) ─────────
    def _notify_finish_once(self, ok: bool, reason: Optional[str], stopped: bool, ch: int) -> None:
        if self.final_notified:
            return
        self.final_notified = True
        run_ch = int(ch)
        key, self.host_key = self.host_key, None
        if key and self.host_log is not None:
            try:
                if stopped:
                    res, rsn = "STOP", (reason or "사용자 STOP")
                elif ok:
                    res, rsn = "성공", ""
                else:
                    res, rsn = "실패", (reason or "error")
                self.host_log.finalize(key, res, rsn, self._now_dt())
            except Exception:
                pass
        if ok or stopped:
            self.run_state.clear_error("pc", run_ch)
        else:
            why = reason or "error"
            self.run_state.set_error("pc", run_ch, why)
            self.popup("critical", f"CH{run_ch} Plasma Cleaning 실패",
                       f"사유: {why}\n\n확인을 누르면 상태 표시가 Idle로 변경됩니다.", ch=run_ch, clear_on_close=True)
        self.run_state.mark_finished("pc", run_ch)
        status = "stopped" if stopped else ("success" if ok else "failed")
        payload: Dict[str, Any] = {"process_name": "Plasma Cleaning", "prefix": f"CH{run_ch} Plasma Cleaning",
                                   "ch": run_ch, "status": status, "stopped": bool(stopped)}
        pc = self.pc
        if pc.last_error_code:
            payload["error_code"] = pc.last_error_code
        if pc.last_error_source:
            payload["error_source"] = pc.last_error_source
        if pc.last_error_detail:
            payload["error_detail"] = pc.last_error_detail
        if pc.last_error_meta:
            payload["error_meta"] = dict(pc.last_error_meta)
        if reason:
            payload["reason"] = str(reason)
            payload["errors"] = [str(reason)]
        self.last_final = {"ok": bool(ok), "stopped": bool(stopped), "reason": reason, "ch": run_ch, "status": status}
        self.card("notify_process_finished_detail", level="info" if ok else "warn", ok=bool(ok), payload=payload,
                  flush=True)
        if not ok:
            why = str(payload.get("reason") or "").strip()
            if not why and payload.get("errors"):
                why = str(payload["errors"][0])
            if why:
                self.card("notify_text", level="warn", text=f"❌ Plasma Cleaning 실패 이유: {why}", flush=True)
        self.emit(PcEvent("state", status, level="info" if ok else "warn",
                          data={"ok": bool(ok), "stopped": bool(stopped), "reason": reason, "ch": run_ch}))

    def clear_error(self, ch: Optional[int] = None) -> None:
        """치명 팝업을 닫음(확인/X/ESC 무관) → Idle(PC-086·TEST-088)."""
        self.run_state.clear_error("pc", int(ch or self.ch))

    # ───────── STOP·안전 중단 ─────────
    def stop(self, source: str = "local") -> Tuple[bool, str]:
        if not self.running:
            self.log("UI", "[STOP] 실행 중이 아님 → 무시")
            return False, "실행 중이 아님"
        if self.final_notified:
            self.log("UI", "[STOP] 이미 종료 통지됨 → 무시")
            return False, "이미 종료 통지됨"
        if self.stop_requested:
            self.log("UI", "[STOP] 이미 정지 요청됨 → 무시")
            return False, "이미 정지 요청됨"
        self.stop_requested = True
        self.log("MAIN", "[STOP] 사용자 정지 요청 수신")
        self.set_state_text("정지 중…")
        try:
            self.pc.request_stop()
            self.log("CTRL", "[STOP] pc.request_stop() 전달")
        except Exception:
            pass
        self.clock.notify()
        return True, ""

    def abort(self, reason: str) -> bool:
        """안전 규칙(4단계)이 부르는 중단 — STOP 과 같은 정지 순서, 결과 사유는 reason(v1 request_stop(사유) 경로)."""
        if not self.running:
            return False
        self.abort_reason = str(reason)
        self.log("MAIN", f"[ABORT] {reason}")
        if self.pc.is_running:
            self.pc.request_stop(str(reason))
        else:
            self.stop_requested = True
        self.clock.notify()
        return True

    # ───────── 호스트 START(3190-3301) ─────────
    def host_start(self, recipe: str, origin_meta: Optional[Mapping[str, Any]] = None,
                   on_result: Optional[Callable[[HostStartResult], None]] = None, source: str = "robot") -> Box:
        """로봇 START_PLASMA_CLEANING — 바로 돌아오고 결과는 on_result(HostStartResult)(반환한 Box 에도). 빈 recipe 는 E202,
        PC 없음 E204 는 핸들러(robot_ops) 몫. 여기서는 v1 start_with_recipe_string 그대로."""
        out = Box(self.clock)

        def finish(res: HostStartResult) -> None:
            out.set(res)
            if on_result is not None:
                try:
                    on_result(res)
                except Exception:
                    pass
        hp = self._host_pending
        if hp is not None and not hp.done:
            finish(self._host_fail(HostStartError("다른 Plasma Cleaning start 요청이 이미 처리 중입니다.", "E206")))
            return out
        self._host_pending = Box(self.clock)
        self._host_start_code = None
        self.clock.start_thread(lambda: finish(self._host_start_body(recipe, dict(origin_meta or {}), source)),
                                "pc-host-start")
        return out

    def _host_fail(self, e: BaseException) -> HostStartResult:
        raw = getattr(e, "code", None)
        return HostStartResult(False, str(e), R.host_error_code(str(e), raw), raw)

    def _reject_host_if_running(self) -> None:
        if self.running:
            msg = "이미 Plasma Cleaning이 실행 중입니다."
            self.popup("warning", "실행 중", msg)
            raise HostStartError(msg, "E205")

    def _host_start_body(self, recipe: str, meta: Dict[str, Any], source: str) -> HostStartResult:
        hp = self._host_pending
        try:
            s = (recipe or "").strip()
            if not s:
                self._reject_host_if_running()
                self.host_recipe_path, self.host_recipe_rows = "", 1
            elif s.lower().endswith(".csv"):
                row, n = self._load_csv_with_timeout(s)
                if not row:
                    raise HostStartError("CSV에 데이터 행이 없습니다.", "E208")
                self._reject_host_if_running()
                self.loaded_row = dict(row)
                self._apply_row(row)
                self.log("File", f"CSV 로드 완료: {s} → UI에 값 세팅")
                self.host_recipe_path, self.host_recipe_rows = s, int(n or 1)
            else:
                raise HostStartError("지원하지 않는 레시피 형식입니다. CSV 파일 경로만 허용됩니다.", "E209")
            self._on_preflight = None
            self._click_start(source=source, by="", origin="host", origin_meta=meta)
            wait_s = self._cfgf("PC_HOST_START_WAIT_TIMEOUT_S", 10.0)
            self.clock.wait_until(self.clock.monotonic() + wait_s, lambda: hp.done)
            if not hp.done:
                raise RuntimeError(f"preflight timeout({wait_s:.1f}s) (쿨다운/가드 등으로 프리플라이트에 도달하지 못했습니다)")
            ok, reason = hp.value
            if not ok:
                code = self._host_start_code
                raise HostStartError(reason or "Plasma Cleaning start failed", code or None)
            return HostStartResult(True, reason, None, None)
        except Exception as e:
            return self._host_fail(e)
        finally:
            self._host_pending = None
            self._host_start_code = None

    def _load_csv_with_timeout(self, path: str):
        box = Box(self.clock)

        def work():
            try:
                box.set(("ok", R.load_host_csv(path, self.log)))
            except HostStartError as e:
                box.set(("coded", e))
            except RuntimeError as e:
                box.set(("rt", e))
            except Exception as e:
                box.set(("exc", e))
        self.clock.start_thread(work, "pc-csv-load")
        self.clock.wait_until(self.clock.monotonic() + R.CSV_LOAD_TIMEOUT_S, lambda: box.done)
        if not box.done:
            raise HostStartError(f"CSV 로드 30초 timeout (NAS 응답 지연): {path}", "E227")
        kind, v = box.value
        if kind == "ok":
            return v
        if kind in ("coded", "rt"):
            raise v
        raise R.e209_read_failed(path, v)

    # ───────── 이벤트(펌프 — 268-431) ─────────
    def pumps_on_now(self) -> None:
        g, pr = self.dev.mfc_gas, self.dev.mfc_pressure
        sel = f"CH{self.ch}"
        self.pumps = {}
        if g is not None and g is pr:
            self.pumps[g.block] = f"MFC(SP4/GAS-{sel})"
        else:
            if g is not None:
                self.pumps[g.block] = "MFC(GAS)"
            if pr is not None:
                self.pumps[pr.block] = f"MFC(SP4-{sel})"
        self.ig_pump = (self.dev.ig.block, f"IG-{sel}") if self.dev.ig is not None else None
        self.rf_pump = self.dev.rf is not None

    def pumps_off(self) -> None:
        self.pumps = {}
        self.ig_pump = None
        self.rf_pump = False

    rf_pump = False

    def on_system_event(self, e) -> None:
        """system.event_listeners 에 건다 — 드라이버 이벤트(Logged.event)."""
        src, ev = getattr(e, "source", ""), getattr(e, "event", None)
        if ev is None or src == "pc":
            return
        try:
            if src == "pc.rf":
                self._on_rf(ev)
            elif src in self.pumps:
                self._on_mfc(src, ev)
            elif self.ig_pump is not None and src == self.ig_pump[0]:
                self._on_ig(ev)
        except Exception as ex:
            self.log("PC", f"이벤트 펌프 오류: {ex!r}")

    def _on_mfc(self, src: str, ev) -> None:
        label = self.pumps.get(src)
        if label is None:
            return
        k = getattr(ev, "kind", None)
        if k in ("command_confirmed", "command_failed"):
            ow = getattr(ev, "owner", None)
            if ow is not None and ow != self.mfc_owner():
                return
        if k == "status":
            self.log(label, ev.detail or "")
        elif k == "command_confirmed":
            self.log(label, f"OK: {ev.cmd or ''}")
            self.pc.on_mfc_confirmed(ev.cmd or "")
        elif k == "command_failed":
            self.log(label, f"FAIL: {ev.cmd or ''} ({ev.detail or 'unknown'})")
            self.pc.on_mfc_failed(ev.cmd or "", ev.detail or "unknown", code=ev.code,
                                  meta={"kind": k, "gas": ev.gas, "label": label, "ch": self.ch})
        elif k == "flow":
            gas, flow = ev.gas or "", float(ev.value or 0.0)
            self.log(label, f"[poll] {gas}: {flow:.2f} sccm")
            g = self.dev.mfc_gas
            if self.process_timer_active and g is not None and src == g.block:
                sel = M.GAS_MAP.get(int(self.gas_idx or 3), "N2")
                if not sel or gas == sel:
                    self.readings["flow"].append(flow)
        elif k == "pressure":
            txt = ev.text or (f"{ev.value:.3g}" if ev.value is not None else "")
            self.log(label, f"[poll] ChamberP: {txt}")
            pr = self.dev.mfc_pressure
            if self.process_timer_active and isinstance(ev.value, (int, float)) and pr is not None and src == pr.block:
                self.readings["pressure"].append(float(ev.value))

    def _on_ig(self, ev) -> None:
        label = self.ig_pump[1]
        k = getattr(ev, "kind", None)
        if k == "status":
            self.log(label, ev.detail or "")
        elif k == "pressure":
            pv = ev.pressure
            self.log(label, f"[poll] {pv:.3e} Torr" if isinstance(pv, (int, float)) else f"[poll] {ev.detail or ''}")
            if isinstance(pv, (int, float)) and self.running:
                self.readings["ig"].append(float(pv))
        elif k == "base_reached":
            self.log(label, "Base pressure reached")
        elif k == "base_failed":
            self.log(label, f"Base pressure failed: {ev.detail or ''}")

    def _on_rf(self, ev) -> None:
        k = getattr(ev, "kind", None)
        data = getattr(ev, "data", {}) or {}
        if k == "state_changed" and data.get("running") is True:
            self._camera_join()
        if k == "status" and ev.detail == "RF SET OFF":
            self._camera_leave()
        if not self.rf_pump:
            return
        if k == "display":
            fwd, ref = float(data.get("forward") or 0.0), float(data.get("reflected") or 0.0)
            self.ui_fwd, self.ui_ref = f"{fwd:.1f}", f"{ref:.1f}"
            self.log("RF", f"FWD={fwd:.1f}, REF={ref:.1f} (W)")
            if self.process_timer_active:
                self.readings["forp"].append(fwd)
                self.readings["refp"].append(ref)
        elif k == "status":
            self.log("RF", ev.detail or "")
        elif k == "log":
            self.log("PLC", ev.detail or "")
        elif k == "target_reached":
            self.log("RF", "목표 파워 도달")
            with self.clock.cond:
                self.rf_target_ok, self.rf_target_reason, self.rf_target_evt = True, "", True
                self.clock.cond.notify_all()
        elif k == "target_failed":
            reason = ev.detail or "RF 목표 파워 실패 (REF.P 과다 또는 저출력)"
            with self.clock.cond:                    # 기다리는 RF 시작이 '목표 실패'를 보고 깨도록 한 묶음으로(v1 이벤트 순서)
                self.log("RF", f"목표 파워 실패: {reason}")
                self.rf_target_ok, self.rf_target_reason = False, reason
                self.forced_fail, self.forced_fail_reason = True, reason
                self.pc.on_rf_failed(reason, code=ev.code, meta={"kind": k, "forward": self.dev.rf.forward_w,
                                                                  "reflected": self.dev.rf.reflected_w, "ch": self.ch})
                self.log("PC", "RF 실패 감지 → PC 컨트롤러에 FAIL 전달")
                self.rf_target_evt = True
                self.clock.cond.notify_all()
        elif k == "power_off_finished":
            self.log("RF", "Power OFF finished")

    # ───────── 카메라(1049-1065) ─────────
    def _camera_join(self) -> None:
        share, cam = self.dev.camera_share, self.dev.camera
        if share is None:
            return
        owner = self.cam_owner_active = f"pc{int(self.ch)}"

        def work():
            try:
                if cam is not None:
                    cam.set_log_callback(lambda m: self.log("CAM", m), owner=owner)
                share.join(owner)
                self.log("CAM", f"카메라 녹화 시작 (mode={cam.current_mode if cam is not None else '-'})")
            except Exception:
                pass
        self.clock.start_thread(work, "pc-camera-join")

    def _camera_leave(self) -> None:
        share = self.dev.camera_share
        if share is None:
            return
        owner = self.cam_owner_active or f"pc{int(self.ch)}"
        try:
            ok = share.leave(owner)
            self.cam_owner_active = ""
            if not ok:
                self.log("CAM", "카메라 사용 종료 — 다른 공정이 계속 촬영 중")
        except Exception:
            pass

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

        def val(col):
            return None if v is None else v.value(col)
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

    def close(self) -> None:
        try:
            self.run_log.stop()
        except Exception:
            pass
        for x in list(self.excel.values()):
            try:
                x.stop()
            except Exception:
                pass


def attach(system, *, run_state: Optional[RunState] = None, chamber_busy: Optional[Callable[[int], bool]] = None,
           host_log=None, name: str = "pc") -> PlasmaCleaning:
    """조립(Ch12System)에 꽂는다 — 장비 묶음·이벤트 구독·register_engine. run_state 가 없으면 system.run_state(없으면 새로)."""
    rs = run_state or getattr(system, "run_state", None) or RunState(system.clock)
    if getattr(system, "run_state", None) is None:
        system.run_state = rs
    dev = PcDevices(system.shared.plc.proxy("PC"), system.plc, system.rf.get("pc"), system.mfc, system.ig,
                    camera=system.camera, camera_share=system.shared.camera)
    eng = PlasmaCleaning(system.clock, system.cfg, dev, run_state=rs, chamber_busy=chamber_busy, host_log=host_log,
                         on_event=system._ev_for("pc"))
    system.event_listeners.append(eng.on_system_event)
    system.register_engine(name, eng)
    return eng

"""챔버12 두께 모니터 공정 연동 — ThicknessEngine(챔버마다 하나, 두께 모니터가 있는 채널 = 운전값 CH2).

v1 근거(읽고 다시 씀, import 안 함): runtime/chamber_runtime.py:1559-1632(THICKNESS_ZERO·판정 시작·끝·취소 콜백),
4042-4078(시작 전 점검·재료값 쓰기·예상/상한), 6591-6843(점검·적용·거부·리스너·이벤트·OES 멈춤), controller/process_controller.py
:1046-1081(on_thickness_*·set_thickness_state·_cancel_thickness), 1488-1613(_thickness_zero_step·_thickness_wait),
controller/thickness_service.py:174-219(Factor·재료 다시 읽기·영점·재료값 적용), ui/thickness_panel.py:318-357([영점]),
ui/thickness_factor_dialog.py:252-304([장비에 쓰기]). 판정은 thickness_judge.py, 기록은 thickness_record.py.

읽기: 드라이버(drivers/stm100.py)가 0.5 s 마다 S·T 를 읽고 add_listener 콜백으로 상태 사본(seq = 새 S 값 번호)을 준다 —
  이 엔진의 _on_status 가 받는다(STM worker 스레드). 판정·기록은 거기서 한다(v1 _on_thickness_status 와 같은 순서).
  S 값 없음·stale 이어도 thickness_nm 은 드라이버가 마지막 값을 유지한다(지우지 않음).
시간 마감(시간 대체·상한·영점 확인 시간·영점 단계 한도)은 poll() 이 본다 — 리스너(0.5 s)와 엔진 on_row(1 s)에서 부르고,
  엔진은 next_deadline() 시각에 poll() 을 더 불러도 된다(v1 은 마감 시각에 정확히 깼다).

엔진이 부를 것(콜백은 STM worker 스레드나 부른 스레드에서 온다 — 잠금 밖에서 부른다)
  supported                         : Thickness 모드를 쓸 수 있나(SUPPORTS_THICKNESS + 드라이버 붙음 — 모드·ETA 공용)
  thickness_mode(params)            : thickness > 0 && supported
  check_basic(params) -> (code, msg): Chuck 이동 전 점검 E840~E844(통과 ("", ""))
  apply_material(params, chuck_ok, on_done, actual_chuck=None) : Chuck 확정 뒤 재료값 쓰기 → on_done((code, msg)) E845·E846
  reject_texts(code, msg, params)   : 점검 실패 로그·챗 문구(거부 흐름 자체는 엔진 — v1 _thickness_reject)
  new_run() / run_started(params) / run_finished() -> 기록 값 / card_fields(payload) / set_main_polling(on) / run_warnings
  zero_start(confirm_s, on_done(ZeroDone))           : THICKNESS_ZERO — 어떤 경우에도 공정을 멈추지 않는다
  wait_start(target, expected_s, limit_s, on_end(WaitEnd), message) : THICKNESS_WAIT — Thickness/시간대체/상한(E848)/중단
  abort()                           : STOP·실패·종료 절차(v1 _cancel_thickness + 대기 '중단')
  poll(now=None) / next_deadline() / on_row(view=None)
  snapshot_fields() / state_for_journal() / resume_checks(state) / resume(state, gap_s, on_end)
  eta_remaining_s()                 : Thickness 모드 공정은 남은 시간을 확정하지 않는다(None — remaining_s=null)
명령(화면·웹 — 출처 검사는 commands 몫): cmd_zero·factor_window·factor_save·factor_write·materials_window·materials_save·
  reload_factors(thickness_factors·thickness_materials 의 검사·문구).
"""
from __future__ import annotations

import threading
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from v2.ch12 import thickness_factors as TF
from v2.ch12 import thickness_materials as TM
from v2.ch12.thickness_judge import (END_ABORT, END_FALLBACK, END_LIMIT, END_THICKNESS, JudgeParams, ThicknessEndJudge,
                                     ThicknessEvent, ZeroConfirm, cfg_get, chamber_no, expected_seconds, judge_params,
                                     max_time_s, thickness_mode_of)
from v2.ch12.thickness_record import RunRecord

ZERO_STEP_SLACK_S = 30.0          # controller/process_controller.py:141 — 영점 단계 한도 = 확인 최대 초 + 이 값
GUNS = ("G1", "G2", "G3")         # runtime/chamber_runtime.py:6589
ETA_LOG = "[ETA] Thickness 모드 — 메인 공정 종료 시각이 정해지지 않아 남은 시간을 확정하지 않습니다(remaining_s=null)"
_LABELS = ("응답 없음", "통신 실패", "체크섬 오류", "프레임 오류", "명령 거부", "값 없음", "되읽기 불일치", "응답 형식 오류")


def _exc_text(detail: str) -> str:
    """드라이버 실패 문구('종류 — 내용', error_text) → v1 이 보이던 예외 문구(str(e)). 종류가 없으면 그대로."""
    d = str(detail or "")
    for lab in _LABELS:
        if d.startswith(lab + " — "):
            return d[len(lab) + 3:]
    return d


@dataclass
class ZeroDone:
    """THICKNESS_ZERO 결과 — 엔진은 무엇이든 다음 단계로 간다(aborted 면 정지 흐름)."""
    base: Optional[float]
    aborted: bool = False
    timed_out: bool = False
    log: str = ""
    warning: str = ""


@dataclass
class WaitEnd:
    """THICKNESS_WAIT 결과. ok=False 이고 code=E848 이면 공정 실패(_step_failed 'THICKNESS' — 종료 절차 첫 단계 Main Shutter 닫기)."""
    end: str                       # Thickness / 시간대체 / 상한 / 중단
    ok: bool
    elapsed_s: float               # 메인 공정 실제 시간(엔진이 누적 — v1 _cancel_countdown)
    code: Optional[str] = None
    source: str = ""
    detail: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)


class ThicknessEngine:
    def __init__(self, clock, cfg: Mapping[str, Any], chamber: str = "ch2", driver=None, *, settings_dir,
                 on_log: Optional[Callable[[str, str], None]] = None,
                 on_side_log: Optional[Callable[[str, str], None]] = None,
                 on_state: Optional[Callable[[str], None]] = None,
                 oes_stop: Optional[Callable[[], bool]] = None) -> None:
        self.clock, self.cfg, self.chamber = clock, cfg, chamber
        self.n = chamber_no(chamber)
        self.driver = driver
        self.settings_dir = Path(settings_dir)
        self._log_cb, self._side_cb = on_log, on_side_log or on_log
        self._on_state, self._oes_stop = on_state, oes_stop
        self._lock = threading.RLock()
        self._calls: List[Tuple[Callable, tuple]] = []
        self._gen = 0
        self._zero: Optional[dict] = None
        self._zero_info: Optional[dict] = None
        self._wait: Optional[dict] = None
        self._judge: Optional[ThicknessEndJudge] = None
        self._last_judge: Optional[ThicknessEndJudge] = None
        self._pre: Optional[dict] = None
        self._params: Dict[str, Any] = {}
        self._running = self._main_polling = False
        self._cmd_busy = False
        self.state_text = ""
        self.record = RunRecord(self._log)
        self.factor_path = TF.factor_file(cfg, chamber, self.settings_dir)
        self.materials_path = TM.materials_file(self.settings_dir)
        self.factors, self.materials = TF.ThicknessFactors(), []
        self._load_files(log=self.supported)
        if self.supported:
            driver.add_listener(self._on_status)
            for w in judge_params(cfg, chamber)[1]:
                self._side("THICKNESS", f"⚠ 두께 판정 설정: {w}")

    # ═══════════════════════ 기본 ═══════════════════════
    @property
    def supported(self) -> bool:
        """SUPPORTS_THICKNESS=true 이고 드라이버가 붙어 켜져 있음(runtime/chamber_runtime.py:6612-6619)."""
        d = self.driver
        return d is not None and bool(getattr(d, "enabled", True)) and bool(cfg_get(self.cfg, self.n, "SUPPORTS_THICKNESS", False))

    def thickness_mode(self, params: Mapping[str, Any]) -> bool:
        return thickness_mode_of((params or {}).get("thickness"), self.supported)

    def params(self) -> JudgeParams:
        return judge_params(self.cfg, self.n)[0]

    def status(self) -> dict:
        return dict(self.driver.status()) if self.driver is not None else {}

    @property
    def run_warnings(self) -> List[str]:
        return self.record.warnings

    def _log(self, src: str, msg: str) -> None:
        if self._log_cb:
            try:
                self._log_cb(src, msg)
            except Exception:
                pass

    def _side(self, src: str, msg: str) -> None:
        if self._side_cb:
            try:
                self._side_cb(src, msg)
            except Exception:
                pass

    def _later(self, fn: Optional[Callable], *a) -> None:
        if fn is not None:
            self._calls.append((fn, a))

    def _flush(self) -> None:
        while True:
            with self._lock:
                if not self._calls:
                    return
                fn, a = self._calls.pop(0)
            fn(*a)

    # ═══════════════════════ Factor·재료 파일 ═══════════════════════
    def _load_files(self, log: bool = True) -> List[str]:
        fac, fw = TF.load_factors(self.factor_path)
        mats, mw = TM.load_materials(self.materials_path)
        self.factors, self.materials = fac, mats
        if log:
            for w in fw:
                self._side("THICKNESS", f"⚠ Factor 파일: {w}")
            for w in mw:
                self._side("THICKNESS", f"⚠ 재료 목록: {w}")
        return list(fw), list(mw)

    def reload_factors(self) -> List[str]:
        """설정창 저장 뒤 다시 읽기 — 진행 중 공정에는 영향 없음(다음 공정부터). controller/thickness_service.py:185-193."""
        with self._lock:
            fw, mw = self._load_files(log=False)
            for w in mw:
                self._side("THICKNESS", f"⚠ 재료 목록: {w}")
            warns = fw + mw
            self._side("THICKNESS", "Factor 파일·재료 목록 다시 읽음" + (f" — 경고 {len(warns)}건" if warns else ""))
            for w in warns:
                self._side("THICKNESS", f"⚠ Factor 파일: {w}")
            return warns

    def find_material(self, name: str):
        return TM.find_material(self.materials, name)

    # ═══════════════════════ 시작 전 점검·재료값 ═══════════════════════
    def check_basic(self, params: Mapping[str, Any]) -> Tuple[str, str]:
        """Chuck 이동 전 점검 — (코드, 문구), 통과면 ("", "")(runtime/chamber_runtime.py:6621-6650)."""
        from v2.ch12.process_sequence import thickness_conflict
        from v2.ch12.process_steps import caps_from_cfg
        jp = self.params()
        sup = self.driver is not None
        st = self.status() if sup else {}
        now = self.clock.monotonic()
        ok_ts = st.get("ok_ts")
        fresh = ok_ts is not None and sup and (now - float(ok_ts)) <= jp.ready_fresh_s
        link = st.get("link_ok") if "link_ok" in st else fresh
        age = "" if (not sup or ok_ts is None) else f" (마지막 정상 값 {now - float(ok_ts):.0f}초 전)"
        if not sup or not st.get("connected") or not link:
            return "E840", f"두께 모니터 응답 없음 — 연결·최근 {jp.ready_fresh_s:g}초 안 정상 두께 값이 필요합니다{age}"
        if st.get("crystal_ok") is False:
            return "E841", "두께 모니터 XTAL FAIL(크리스탈 이상) — 크리스탈을 확인하십시오"
        if not fresh:
            return "E840", f"두께 값 없음(통신은 정상) — 최근 {jp.ready_fresh_s:g}초 안 정상 두께 값이 필요합니다{age}"
        dep, thk = params.get("dep_rate"), params.get("thickness")
        try:
            dep_v, thk_v = float(dep or 0), float(thk or 0)
        except Exception:
            dep_v, thk_v = 0.0, 0.0
        if dep_v <= 0 or thk_v <= 0:
            return "E842", f"Thickness 모드에는 dep_rate·thickness 가 0 보다 커야 합니다(dep_rate={dep}, thickness={thk})"
        guns = [g for g in GUNS if params.get(f"use_{g.lower()}")]
        if len(guns) != 1:
            what = "co-sputter(" + "+".join(guns) + ")" if guns else "사용 건 없음"
            return "E843", f"Thickness 모드는 건 1개만 쓸 수 있습니다 — {what}"
        conflict = thickness_conflict(params, caps_from_cfg(self.cfg, self.n))
        if conflict:
            return "E844", conflict
        return "", ""

    def apply_material(self, params: Dict[str, Any], chuck_ok: bool, on_done: Callable[[Tuple[str, str]], None],
                       actual_chuck: Optional[str] = None) -> None:
        """Chuck 확정 뒤 Factor 확인 → 장비에 density/Z/tooling 쓰기(되읽기 확인, 1회 재시도) → on_done((코드, 문구)).
        chuck_ok=False 이고 actual_chuck(PLC 로 다시 읽은 위치)을 주면 그 위치 기준(runtime/chamber_runtime.py:6652-6725).
        통과하면 params 에 thickness_mode·thickness_expected_s·thickness_limit_s·thickness_factor_note 를 넣는다(4073-4078)."""
        chuck = str(params.get("chuck_position") or "").strip().lower()
        if not chuck_ok and actual_chuck is not None:
            if actual_chuck != chuck:
                self._log("THICKNESS", f"Chuck 이동 실패 — Factor 는 실제 위치 '{actual_chuck}' 기준")
            chuck = actual_chuck
        if chuck not in ("up", "mid", "down"):
            on_done(("E845", f"Chuck 위치를 알 수 없어(값 '{chuck or '-'}') tooling 을 정할 수 없습니다"))
            return
        gun = [g for g in GUNS if params.get(f"use_{g.lower()}")][0]
        fac = self.factors
        where = f"(화면 [Factor 설정] 또는 {self.factor_path})"

        def _rv(key):
            v = params.get(key)
            try:
                return None if v in (None, "") else float(v)
            except Exception:
                return None
        r_tool, r_dens, r_z = _rv("tooling"), _rv("density"), _rv("z_factor")
        missing: List[str] = []
        tooling = r_tool if r_tool is not None else fac.tooling_for(chuck, gun)
        if tooling is None:
            missing.append(f"tooling — 레시피 없음, Factor {chuck}×{gun} 미설정")
        mat, mat_err = None, ""
        if r_dens is None or r_z is None:                          # 재료는 레시피에 없는 값이 있을 때만 본다
            name = fac.material_name(gun)
            if not name:
                mat_err = f"Factor {gun} 재료 미선택"
            else:
                mat = self.find_material(name)
                if mat is None:
                    mat_err = f"Factor {gun} 재료 '{name}' 가 재료 목록에 없음(화면 [Factor 설정] → [재료 목록 편집])"
        density = r_dens if r_dens is not None else (float(mat.density) if mat is not None else None)
        z_factor = r_z if r_z is not None else (float(mat.z_factor) if mat is not None else None)
        lack = [n for n, v in (("density", density), ("Z", z_factor)) if v is None]
        if lack:
            missing.append(f"{'·'.join(lack)} — 레시피 없음, {mat_err}")
        if missing:
            on_done(("E845", f"Factor 미설정 — {' / '.join(missing)} {where}"))
            return
        src_t = "recipe" if r_tool is not None else "factor"
        src_d = "recipe" if r_dens is not None else "factor"
        src_z = "recipe" if r_z is not None else "factor"
        used_mat = mat if (src_d == "factor" or src_z == "factor") else None

        def done(d) -> None:
            if not d.ok:
                on_done(("E846", f"두께 모니터에 재료값을 쓰지 못함(되읽기 확인 실패 포함): {_exc_text(d.detail)}"))
                return
            got = d.value or {}
            recipe_used = [n for n, s in (("tooling", src_t), ("density", src_d), ("z_factor", src_z)) if s == "recipe"]
            params["thickness_factor_note"] = f"레시피 지정({', '.join(recipe_used)})" if recipe_used else ""
            with self._lock:
                self._pre = {"chuck": chuck, "gun": gun, "tooling": got.get("tooling"), "density": got.get("density"),
                             "z_factor": got.get("z_factor"), "material": used_mat.name if used_mat is not None else "",
                             "factor_source": f"T:{src_t} D:{src_d} Z:{src_z}"}

            def _src(s, factor_txt):
                return "(레시피)" if s == "recipe" else f"({factor_txt})"
            mat_txt = f"Factor {gun} {used_mat.symbol or used_mat.name}" if used_mat is not None else ""
            jp = self.params()
            life = self.status().get("life_pct")
            if life is not None and float(life) < jp.life_warn_pct:
                warn = f"크리스탈 수명 {float(life):.1f}% (경고 기준 {jp.life_warn_pct:g}% 미만) — 교체 준비"
                self._log("THICKNESS", f"⚠ {warn}")
                self.record.warnings.append(warn)
            self._log("THICKNESS", f"Thickness 모드 — 목표 {float(params.get('thickness')):g} nm · {gun} · chuck {chuck} · "
                                   f"tooling {float(got.get('tooling')):g} % {_src(src_t, f'Factor {chuck}×{gun}')} · "
                                   f"density {float(got.get('density')):g} {_src(src_d, mat_txt)} · "
                                   f"Z {float(got.get('z_factor')):.3f} {_src(src_z, mat_txt)}")
            params["thickness_mode"] = True
            try:
                exp = expected_seconds(float(params["thickness"]), float(params["dep_rate"]))
                params["thickness_expected_s"] = exp
                params["thickness_limit_s"] = max_time_s(exp, jp)
            except Exception:
                pass
            on_done(("", ""))
        self.driver.set_material(float(density), float(z_factor), float(tooling),
                                 material=used_mat.name if used_mat is not None else "", on_done=done)

    def reject_texts(self, code: str, msg: str, params: Mapping[str, Any]) -> Tuple[str, str]:
        """(공정 로그 MAIN, 챗 장비 오류 카드 문구) — runtime/chamber_runtime.py:6729-6734."""
        note = params.get("process_note") or params.get("Process_name") or "알 수 없음"
        return (f"[CH{self.n}] 두께 모니터 점검 실패 [{code}] {msg} → 공정 시작 차단",
                f"두께 모니터 점검 실패: {msg} → 공정 시작 차단 (공정: {note})")

    # ═══════════════════════ 공정 시작·끝·기록 ═══════════════════════
    def new_run(self) -> None:
        """공정 시작 처리 맨 앞(점검 전) — 지난 런의 시작 전 값·이번 런 경고 목록을 비운다(v1 _thk_pre = None·_run_warnings = [],
        runtime/chamber_runtime.py:4042·4052). 경고 목록(run_warnings)은 종료 카드 warnings 에 엔진이 합친다."""
        with self._lock:
            self._pre = None
            self.record.warnings = []

    def run_started(self, params: Mapping[str, Any]) -> None:
        """공정 시작 이벤트 — 이번 런 기록 초기화(두께 모니터 붙은 채널만, _thk_run_started). 연속 CSV 는 outputs 몫."""
        with self._lock:
            self._params = dict(params or {})
            self._running = True
            self._main_polling = False
            self.record.start(params, self._pre, self.supported)
            self._last_judge = None

    def set_main_polling(self, on: bool) -> None:
        """메인 공정 폴링 구간(v1 _proc_polling_active) — Time 모드 두께 기록 창."""
        with self._lock:
            self._main_polling = bool(on)

    def run_finished(self) -> dict:
        """공정 끝 — 기록 값(Calib CSV·엑셀 두께 열)·패널 마지막 결과·두께 요약 로그. 다음 공정 전 값(_pre)은 지운다."""
        with self._lock:
            self._running = self._main_polling = False
            return self.record.finalize()

    def record_values(self) -> dict:
        with self._lock:
            return self.record.values()

    def card_fields(self, payload: dict) -> None:
        with self._lock:
            self.record.card_fields(payload)

    def eta_remaining_s(self) -> Optional[float]:
        """Thickness 모드 공정은 남은 시간을 확정하지 않는다(controller/process_controller.py:1545·1736-1738)."""
        return None

    # ═══════════════════════ THICKNESS_ZERO ═══════════════════════
    def zero_start(self, confirm_s: float, on_done: Callable[[ZeroDone], None]) -> None:
        """영점(B) → 영점 확인(최대 confirm_s) → 기준점(cb_thickness_zero). 한도 confirm_s + 30 s 넘으면 기다림만 끊는다."""
        with self._lock:
            self._gen += 1
            gen = self._gen
            self._zero_info = None
            if self.driver is None:
                self._zero_info = {"base": None, "reset0": 0}
                self._later(on_done, ZeroDone(None))
            else:
                now = self.clock.monotonic()
                self._zero = {"gen": gen, "confirm_s": float(confirm_s), "on_done": on_done, "zc": None,
                              "reset0": int(self.status().get("reset_seen") or 0), "answered": False,
                              "budget_t": now + float(confirm_s) + ZERO_STEP_SLACK_S}
                self.driver.zero("B", on_done=lambda d, g=gen: self._zero_cmd_done(d, g), why="메인 셔터 열기 전 영점")
        self._flush()

    def _zero_cmd_done(self, d, gen: int) -> None:
        with self._lock:
            z = self._zero
            if z is None or z["gen"] != gen or gen != self._gen:
                return
            if not d.ok:
                self._log("THICKNESS", f"⚠ 영점 명령 실패(영점 실패: {_exc_text(d.detail)})")
            st = self.status()
            z["zc"] = ZeroConfirm(self.params(), start_t=self.clock.monotonic(), seq0=st.get("seq"), zero_ok=bool(d.ok))
            self.record.set_view("Thickness", "—", 0.0, "영점 확인 중")
            self._zero_tick(st, self.clock.monotonic())
        self._flush()

    def _zero_tick(self, st: Mapping[str, Any], now: float) -> None:
        z = self._zero
        if z is None:
            return
        if z["zc"] is not None:
            r = z["zc"].tick(st, now)
            if r is not None:
                self._zero = None
                self._log("THICKNESS", r.log)
                if r.warning:
                    self.record.warnings.append(r.warning)
                self._zero_info = {"base": r.base, "reset0": z["reset0"]}
                if not z["answered"]:
                    self._later(z["on_done"], ZeroDone(r.base, log=r.log, warning=r.warning))
                return
        if not z["answered"] and now >= z["budget_t"]:
            z["answered"] = True                      # v1: 기다림만 끊고 진행 — 확인은 끝까지(늦은 결과는 기록만)
            budget = z["confirm_s"] + ZERO_STEP_SLACK_S
            self._log("Process", f"⚠ 두께 영점·확인 {budget:.0f}초 초과 → 기다림을 끊고 진행")
            self._later(z["on_done"], ZeroDone(None, timed_out=True))

    # ═══════════════════════ THICKNESS_WAIT ═══════════════════════
    def wait_start(self, target: float, expected_s: float, limit_s: float, on_end: Callable[[WaitEnd], None],
                   message: str = "메인 공정 진행", dep_rate: Optional[float] = None) -> None:
        """메인 셔터를 연 뒤 — 판정기(기준점 = THICKNESS_ZERO 결과) 시작, 끝 = 목표 도달 | 시간 대체 마감 | 상한 | abort."""
        with self._lock:
            now = self.clock.monotonic()
            self._log("Process", f"{message} 시작 — 목표 {float(target):g} nm, 예상 {float(expected_s):.0f}초, "
                                 f"최대 {float(limit_s):.0f}초 (목표 도달 시 종료, 이상 시 시간 대체, 상한 도달 시 정상 종료 절차)")
            self._log("Process", ETA_LOG)
            self._wait = {"target": float(target), "expected_s": float(expected_s), "limit_s": float(limit_s),
                          "message": message, "on_end": on_end, "start_t": now, "limit_t": now + float(limit_s),
                          "fb_t": None}
            self.state_text = ""
            try:
                self._judge_start(float(target), float(expected_s), float(limit_s), dep_rate)
            except Exception as e:
                self._log("Process", f"⚠ 두께 판정 시작 실패({e!r}) → 예상시간으로 시간 대체")
                if self._wait is not None:
                    self._wait["fb_t"] = now + float(expected_s)
            self._check_deadlines(self.clock.monotonic())
        self._flush()

    def _judge_start(self, target: float, expected_s: float, limit_s: float, dep_rate: Optional[float]) -> None:
        """cb_thickness_start(runtime/chamber_runtime.py:1597-1621)."""
        if self.driver is None:
            self._fallback(expected_s, "두께 서비스 없음 — 예상시간으로 시간 대체")
            return
        st = self.status()
        zinfo = self._zero_info or {"base": None, "reset0": int(st.get("reset_seen") or 0)}
        raw = dep_rate if dep_rate is not None else self._params.get("dep_rate")
        dep = float(raw or 0.0) or (target / max(1.0, expected_s))     # 레시피 dep_rate, 없으면 목표 ÷ 예상
        j = ThicknessEndJudge(target, dep, self.params(), start_t=self.clock.monotonic(), base=zinfo.get("base"),
                              seq0=st.get("seq"), reset0=int(zinfo.get("reset0") or 0),
                              log=lambda m: self._log("THICKNESS", m))
        self._judge = self._last_judge = j
        base = zinfo.get("base")
        self._log("THICKNESS", f"두께 판정 시작 — 목표 {target:g} nm · 기준점 "
                               f"{'없음' if base is None else f'{float(base):.2f} nm'} · 레시피 증착률 {dep:g} nm/s · "
                               f"예상 {expected_s:.0f}초 · 상한 {limit_s:.0f}초")
        self.record.set_view("Thickness", f"{target:.1f} nm", 0.0, "메인 공정 진행")
        ev = j.start_event()
        if ev is not None:
            self._apply_event(ev)

    def _fallback(self, remaining_s: float, reason: str = "") -> None:
        """on_thickness_fallback(process_controller.py:1054-1061) — 대기 중일 때만 마감 = 지금 + 남은 초."""
        w = self._wait
        if w is None:
            return
        rem = max(0.0, float(remaining_s))
        w["fb_t"] = self.clock.monotonic() + rem
        self._log("Process", f"두께 → 시간 대체: {rem:.0f}초 뒤 메인 공정 종료" + (f" ({reason})" if reason else ""))

    def _resume(self) -> None:
        w = self._wait
        if w is not None and w["fb_t"] is not None:
            w["fb_t"] = None
            self._log("Process", "두께 판정 복귀 — 시간 대체 타이머 해제")

    def _apply_event(self, ev: ThicknessEvent) -> None:
        if ev.kind == "done":
            self._judge = None
            self._finish_wait(END_THICKNESS)
        elif ev.kind == "fallback":
            if ev.detail:
                self._log("THICKNESS", f"⚠ 두께 판정 예외 traceback:\n{ev.detail.rstrip()}")
            self._fallback(float(ev.remaining_s or 0.0), ev.reason)
        elif ev.kind == "resume":
            self._resume()

    def _set_state(self, text: str) -> None:
        if self._wait is not None:
            self.state_text = str(text)
            self._later(self._on_state, self.state_text)

    def _judge_error(self, e: Exception, j: ThicknessEndJudge) -> None:
        """판정 호출부 예외 — 곧바로 영구 시간 대체(traceback 1번, runtime/chamber_runtime.py:6818-6830)."""
        self._judge = None
        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__)).rstrip()
        self._log("THICKNESS", f"⚠ 두께 판정 예외 — 시간 대체로 전환: {tb}")
        try:
            rem = float(j.fallback_remaining_s())
        except Exception:
            rem = 0.0
        try:
            j.mode, j.permanent, j.fallback_reason = "fallback", True, f"판정 예외({e!r})"
            j.fallback_count += 1
        except Exception:
            pass
        self._fallback(rem, f"판정 예외({e!r})")

    def _finish_wait(self, end: str) -> None:
        w, self._wait = self._wait, None
        if w is None:
            return
        elapsed = self.clock.monotonic() - w["start_t"]
        if end == END_ABORT:
            self._log("Process", f"{w['message']} 중단됨 ({elapsed:.0f}초)")
        else:
            self._log("Process", f"{w['message']} 완료 — 종료 방식 {end} ({elapsed:.0f}초)")
        # cb_thickness_end(1623-1628): 세대 +1, 판정기 내림, OES 멈춤 요청(기다리지 않음), 종료 기록
        self._gen += 1
        self._judge = None
        try:
            if self._oes_stop is not None and self._oes_stop():
                self._log("OES", "OES 측정 종료 요청 — 메인 공정 끝(Thickness 모드)")
        except Exception:
            pass
        got = None
        try:
            got = self.record.on_wait_end(end, self._last_judge)
        except Exception as e:
            self._log("Process", f"⚠ 두께 종료 처리 예외(무시): {e!r}")
        res = WaitEnd(end, end != END_LIMIT and end != END_ABORT, elapsed)
        if end == END_LIMIT:
            what = str(got or f"유효 -- / 목표 {w['target']:g} nm")
            res.code, res.source = "E848", "THICKNESS"
            res.detail = (f"두께 목표 미도달 — 최대 시간 초과: {what}, 경과 {elapsed / 60.0:.1f}분"
                          f"(상한 {w['limit_s'] / 60.0:.1f}분)")
            res.meta = {"ch": self.n, "target_nm": w["target"], "limit_s": w["limit_s"]}
        self._later(w["on_end"], res)

    def abort(self) -> None:
        """STOP·실패·종료 절차 — 판정 취소(세대 +1·판정기 해제), 영점 대기는 '중단됨', THICKNESS_WAIT 는 '중단'으로 끝."""
        with self._lock:
            self._gen += 1
            self._judge = None
            z, self._zero = self._zero, None
            if z is not None and not z["answered"]:
                self._log("Process", "두께 영점·확인 중단됨")
                self._later(z["on_done"], ZeroDone(None, aborted=True))
            if self._wait is not None:
                self._finish_wait(END_ABORT)
        self._flush()

    # ═══════════════════════ 리스너·마감 ═══════════════════════
    def _on_status(self, st: dict) -> None:
        """드라이버 poll 마다(STM worker 스레드) — 기록 → 영점 확인 → 판정 tick → 이벤트 → 상태 문구(_on_thickness_status)."""
        with self._lock:
            now = self.clock.monotonic()
            self.record.sample(st, now, self._main_polling, self._running)
            self._zero_tick(st, now)
            j = self._judge
            if j is not None:
                try:
                    ev = j.tick(st, now)
                    if ev is not None:
                        self._apply_event(ev)
                    if self._judge is j or (ev is not None and ev.kind == "fallback"):
                        text, pct = j.view(st, now)
                        self._set_state(text)
                        self.record.set_view("시간 대체" if j.mode == "fallback" else "Thickness",
                                             f"{j.target:.1f} nm", pct, text)
                except Exception as e:
                    self._judge_error(e, j)
            self._check_deadlines(now)
        self._flush()

    def _check_deadlines(self, now: float) -> None:
        w = self._wait
        if w is not None:
            if w["fb_t"] is not None and now >= w["fb_t"]:
                self._finish_wait(END_FALLBACK)
            elif now >= w["limit_t"]:
                self._finish_wait(END_LIMIT)
        if self._zero is not None:
            self._zero_tick(self.status(), now)

    def poll(self, now: Optional[float] = None) -> None:
        with self._lock:
            self._check_deadlines(self.clock.monotonic() if now is None else float(now))
        self._flush()

    def on_row(self, view: Optional[Mapping[str, Any]] = None) -> None:
        """1초 tick 마다 — 마감 확인(드라이버가 없거나 멈춰 리스너가 오지 않아도 상한·시간 대체가 끝나게)."""
        self.poll()

    def next_deadline(self) -> Optional[float]:
        with self._lock:
            ts = []
            w, z = self._wait, self._zero
            if w is not None:
                ts += [w["limit_t"]] + ([w["fb_t"]] if w["fb_t"] is not None else [])
            if z is not None:
                if z["zc"] is not None:
                    ts.append(z["zc"].t0 + z["confirm_s"])
                if not z["answered"]:
                    ts.append(z["budget_t"])
            return min(ts) if ts else None

    # ═══════════════════════ 1초 행 ═══════════════════════
    def snapshot_fields(self) -> Dict[str, Any]:
        """두께 nm·rate(장비 마지막 값 — 값 없음·stale 이어도 유지)·목표·유효·남은 nm·모드·연속 횟수·종료 방식·문구."""
        st = self.status()
        with self._lock:
            j, w = self._judge, self._wait
            jj = j or (self._last_judge if w is not None else None)
            target = w["target"] if w is not None else None
            eff = jj.eff if jj is not None else None
            phase = "wait" if w is not None else ("zero" if self._zero is not None else "idle")
            return {"thickness_nm": st.get("thickness_nm"), "rate_nm_s": st.get("rate_nm_s"),
                    "target_nm": target, "eff_nm": eff,
                    "remaining_nm": None if (target is None or eff is None) else max(0.0, target - eff),
                    "mode": self.record.view.get("mode") or "", "phase": phase,
                    "judge": "" if jj is None else jj.mode, "permanent": bool(jj.permanent) if jj is not None else None,
                    "done_n": jj.done_n if jj is not None else None, "done_need": self.params().done_consecutive,
                    "pct": self.record.view.get("pct"), "text": self.record.view.get("text"),
                    "state_text": self.state_text if w is not None else ""}

    # ═══════════════════════ 저널·이어가기(v2) ═══════════════════════
    def state_for_journal(self) -> dict:
        """1초 저널용 — 시각은 상대(초). phase: idle / zero(이어갈 수 없음 — 장비 명령 도중) / wait."""
        st = self.status()
        with self._lock:
            now = self.clock.monotonic()
            w, j = self._wait, self._judge
            d: Dict[str, Any] = {"phase": "wait" if w is not None else ("zero" if self._zero is not None else "idle"),
                                 "params": {"dep_rate": self._params.get("dep_rate")}, "zero_info": self._zero_info,
                                 "pre": self._pre, "running": self._running, "main_polling": self._main_polling,
                                 "record": self.record.to_state(now), "last_device_nm": st.get("thickness_nm")}
            if w is not None:
                d["wait"] = {k: w[k] for k in ("target", "expected_s", "limit_s", "message")}
                d["wait"]["elapsed_s"] = now - w["start_t"]
                d["wait"]["fb_left_s"] = None if w["fb_t"] is None else w["fb_t"] - now
                d["judge"] = None if j is None else dict(j.to_state(), t0_age_s=now - j.t0)
            return d

    def resume_checks(self, state: Mapping[str, Any]) -> List[Dict[str, Any]]:
        """이어가기 확인 목록 [{name, ok, detail}] — 하나라도 ok=False 면 엔진은 안전 정지(PROGRAM_RESTART)."""
        phase = state.get("phase")
        if phase == "zero":
            return [{"name": "thickness.phase", "ok": False, "detail": "두께 영점·확인 중(장비 명령 도중) — 이어갈 수 없음"}]
        if phase != "wait":
            return []
        j = state.get("judge")
        if j is None or j.get("permanent"):
            return [{"name": "thickness.time_only", "ok": True, "detail": "판정 없음·영구 시간 대체 — 시간으로만 진행"}]
        st, jp = self.status(), self.params()
        age = st.get("age_s")
        last = state.get("last_device_nm")
        out = [
            {"name": "thickness.link", "ok": bool(st.get("connected") and st.get("link_ok")), "detail": "두께 모니터 응답"},
            {"name": "thickness.crystal", "ok": st.get("crystal_ok") is not False, "detail": "XTAL FAIL 아님"},
            {"name": "thickness.reset", "ok": int(st.get("reset_seen") or 0) == 0, "detail": "전원 리셋 플래그 없음"},
            {"name": "thickness.fresh", "ok": age is not None and age <= jp.ready_fresh_s,
             "detail": f"최근 {jp.ready_fresh_s:g}초 안 정상 두께 값"},
            {"name": "thickness.counter", "ok": last is None or (st.get("thickness_nm") is not None
                                                                  and float(st["thickness_nm"]) >= float(last) - jp.zero_confirm_nm),
             "detail": f"장비 두께가 저널 값({last}) 뒤로 줄지 않음(영점·리셋 없음)"},
        ]
        return out

    def resume(self, state: Mapping[str, Any], gap_s: float, on_end: Callable[[WaitEnd], None]) -> None:
        """저널 상태로 THICKNESS_WAIT 를 이어간다 — 꺼져 있던 시간(gap_s)도 경과에 넣는다(장비는 계속 증착)."""
        st = self.status()
        with self._lock:
            now = self.clock.monotonic()
            gap = max(0.0, float(gap_s))
            self._params = dict(state.get("params") or {})
            self._zero_info, self._pre = state.get("zero_info"), state.get("pre")
            self._running, self._main_polling = bool(state.get("running", True)), bool(state.get("main_polling"))
            self.record.from_state(state.get("record") or {}, now - gap)
            w = state.get("wait")
            if w is None:
                return
            start = now - float(w["elapsed_s"]) - gap
            fb = w.get("fb_left_s")
            self._wait = {"target": float(w["target"]), "expected_s": float(w["expected_s"]),
                          "limit_s": float(w["limit_s"]), "message": w.get("message") or "메인 공정 진행",
                          "on_end": on_end, "start_t": start, "limit_t": start + float(w["limit_s"]),
                          "fb_t": None if fb is None else now + float(fb) - gap}
            js = state.get("judge")
            if js is not None:
                t0 = now - float(js["t0_age_s"]) - gap
                self._judge = self._last_judge = ThicknessEndJudge.from_state(
                    js, t0=t0, seq0=-1, reset0=int(st.get("reset_seen") or 0), log=lambda m: self._log("THICKNESS", m))
                if js.get("permanent"):
                    self._judge = None
            self._log("THICKNESS", f"두께 판정 이어감 — 공백 {gap:.0f}초, 경과 {now - start:.0f}초 / 상한 {float(w['limit_s']):.0f}초")
            self._check_deadlines(now)
        self._flush()

    # ═══════════════════════ 명령(패널·Factor·재료 창) ═══════════════════════
    def cmd_zero(self, process_running: bool, on_done: Callable[[bool, str], None]) -> None:
        """패널 [영점](ui/thickness_panel.py:318-357) — 확인창은 화면 몫, 여기서 다시 금지 사유를 본다."""
        st = self.status()
        if process_running or st.get("process_running"):
            on_done(False, "공정 중에는 영점을 할 수 없습니다.")
            return
        block = TF.device_write_block(st, process_running) if self.driver is not None else "두께 모니터 미연결"
        if block or self._cmd_busy:
            on_done(False, f"{block} — 영점을 보내지 않았습니다." if block else "")
            return
        self._cmd_busy = True

        def done(d) -> None:
            self._cmd_busy = False
            on_done(bool(d.ok), "" if d.ok else f"영점 실패: {_exc_text(d.detail)}")
        self.driver.zero("B", on_done=done, why="패널")

    def factor_window(self) -> dict:
        mats, mw = TM.load_materials(self.materials_path)
        return TF.factor_window(self.factor_path, mats, mw, ch=self.n, materials_path=str(self.materials_path))

    def factor_save(self, tooling_cells, material_texts) -> Tuple[bool, str, List[str]]:
        mats, _w = TM.load_materials(self.materials_path)
        ok, text, bad = TF.save_window(self.factor_path, tooling_cells, material_texts, mats)
        if ok:
            self.reload_factors()
        return ok, text, bad

    def factor_write(self, chuck: str, gun: str, tooling_text, material_text, process_running: bool,
                     on_done: Callable[[dict], None]) -> None:
        """Factor 창 [장비에 쓰기] — 창의 값(저장 무관)을 장비에 바로 쓰고 되읽어 확인. 저장 파일은 건드리지 않는다."""
        mats, _w = TM.load_materials(self.materials_path)
        tooling, mat, errs, bad = TF.write_values(chuck, gun, tooling_text, material_text, mats)
        if errs:
            on_done({"ok": False, "title": "장비에 쓰기", "bad": bad,
                     "text": "고칠 칸이 있어 장비에 쓰지 않았습니다:\n\n" + "\n".join(errs)})
            return
        why = TF.device_write_block(self.status(), process_running) if self.driver is not None else "두께 모니터 미연결"
        if why:
            on_done({"ok": False, "title": "장비에 쓰기", "bad": [], "text": f"{why} — 장비에 쓰지 않았습니다."})
            return

        def done(d) -> None:
            if not d.ok:
                on_done({"ok": False, "title": "장비에 쓰기", "bad": [],
                         "text": f"장비에 쓰지 못했습니다(되읽기 확인 포함): {_exc_text(d.detail)}"})
                return
            g = d.value or {}
            on_done({"ok": True, "title": "장비에 쓰기", "bad": [],
                     "text": f"장비에 적용됨 — {mat.name} · density {float(g.get('density')):g} · "
                             f"Z {float(g.get('z_factor')):.3f} · tooling {float(g.get('tooling')):g}%"})
        self.driver.set_material(float(mat.density), float(mat.z_factor), float(tooling), material=mat.name,
                                 on_done=done, why="Factor 창 수동")

    def materials_window(self) -> dict:
        return TM.materials_window(self.materials_path)

    def materials_save(self, rows: Sequence[Sequence], used_names: Sequence[str] = (),
                       confirmed: bool = False) -> Dict[str, Any]:
        """재료 목록 [저장] — Factor 에서 쓰는 재료가 빠지면 먼저 확인 문구(confirmed=True 로 다시 부르면 저장)."""
        old, _w = TM.load_materials(self.materials_path)
        keep = {str(r[0]).strip().casefold() for r in rows if r and str(r[0] or "").strip()}
        gone = [m.name for m in old if m.name.casefold() not in keep]
        ask = TM.delete_confirm_text(gone, used_names)
        if ask and not confirmed:
            return {"ok": False, "confirm": ask, "text": "", "bad": []}
        ok, text, bad = TM.save_rows(self.materials_path, rows)
        if ok:
            self.reload_factors()
        return {"ok": ok, "confirm": "", "text": text, "bad": bad}

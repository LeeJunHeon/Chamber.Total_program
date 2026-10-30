"""ThicknessEngine(두께 공정 연동) — v1 tests/test_thickness_runtime.py·test_thickness_zero_confirm.py(런타임)·
test_thickness_process_steps.py(THICKNESS_ZERO·WAIT 타이머)·test_thickness_round14.py(메인 공정 전 종료·[영점] 재검사)·
test_thickness_panel.py([장비에 쓰기])를 v2 로 옮긴 것(v1 import 없음).

가짜 드라이버(상태 dict·영점·재료값 호출 기록)와 손으로 넘기는 시계 — 기대값은 v1 시험 값(근거 v1 파일:줄).
실제 드라이버·시뮬레이터·가상 시계 실행은 test_thickness_sim.py.
"""
import dataclasses

import pytest

from v2.common.paramdef import defaults
from v2.ch12.drivers.stm100 import StmDone
from v2.ch12.params import PARAMS
from v2.ch12.thickness import ThicknessEngine, WaitEnd, ZeroDone
from v2.ch12.thickness_factors import ThicknessFactors, save_factors
from v2.ch12.thickness_judge import JudgeParams, ThicknessEndJudge
from v2.ch12.thickness_materials import Material, save_materials

CFG = defaults(PARAMS)                              # 운전 기본값 전체(CH2 DC Pulse·두께 모니터 켬, DONE_CONSECUTIVE 2)


class Clock:
    def __init__(self, t=100.0):
        self.t = t

    def monotonic(self):
        return self.t

    def wall(self):
        return 1_760_000_000.0 + self.t


class FakeDriver:
    """Stm100Driver 의 엔진이 쓰는 부분 — status·add_listener·zero·set_material(on_done 은 바로 또는 hold 면 나중에)."""

    def __init__(self, clock):
        self.clock = clock
        self.enabled = True
        self.st = {"connected": True, "ok_ts": 99.0, "crystal_ok": True, "seq": 7, "reset_seen": 0, "life_pct": 50.0,
                   "thickness_nm": 3.0, "stale": False, "link_ok": True, "age_s": 1.0}
        self.listeners = []
        self.zero_calls, self.mat_calls = [], []
        self.zero_ok, self.mat_error = True, None
        self.hold = False
        self.pending = []

    def status(self):
        return dict(self.st)

    def add_listener(self, cb):
        self.listeners.append(cb)

    def publish(self):
        for cb in list(self.listeners):
            cb(self.status())

    def _done(self, fn, d):
        if self.hold:
            self.pending.append((fn, d))
        else:
            fn(d)

    def zero(self, mode="B", on_done=None, why=""):
        self.zero_calls.append((mode, why))
        self._done(on_done, StmDone(self.zero_ok, "zero", None if self.zero_ok else "DEVICE_NO_RESPONSE",
                                    detail="" if self.zero_ok else "응답 없음 — 응답 없음 'B' (3회)"))

    def set_material(self, d, z, t, material="", on_done=None, why=""):
        self.mat_calls.append((d, z, t, material, why))
        if self.mat_error:
            self._done(on_done, StmDone(False, "set_material", "VERIFY_MISMATCH", "E846", self.mat_error))
        else:
            self._done(on_done, StmDone(True, "set_material", value={"density": d, "z_factor": z, "tooling": t}))

    def release(self):
        p, self.pending = self.pending, []
        for fn, d in p:
            fn(d)


class Rig:
    def __init__(self, tmp, driver=True, cfg=None, factors=True):
        self.c = Clock()
        self.d = FakeDriver(self.c) if driver else None
        self.logs, self.side, self.states, self.oes = [], [], [], []
        if factors:
            f = ThicknessFactors()
            f.tooling["mid"]["G1"] = 110.0
            f.material["G1"] = "Aluminum"
            save_factors(tmp / "thickness_factors_ch2.json", f)
            save_materials(tmp / "thickness_materials.json",
                           [Material("Aluminum", "Al", 2.73, 1.08), Material("Titanium", "Ti", 4.5, 0.628)])
        self.oes_active = True
        self.e = ThicknessEngine(self.c, dict(CFG, **(cfg or {})), "ch2", self.d, settings_dir=tmp,
                                 on_log=lambda s, m: self.logs.append((s, m)),
                                 on_side_log=lambda s, m: self.side.append((s, m)),
                                 on_state=self.states.append, oes_stop=self._oes)
        self.ends, self.zeros = [], []

    def _oes(self):
        self.oes.append(self.c.t)
        return self.oes_active

    def msgs(self, src=None):
        return [m for s, m in self.logs if src is None or s == src]

    def feed(self, s, dt=0.5, **kw):
        self.c.t += dt
        self.d.st["seq"] += 1
        self.d.st.update(thickness_nm=s, ok_ts=self.c.t, **kw)
        self.d.publish()

    def idle(self, dt=1.0):
        self.c.t += dt
        self.d.publish()
        self.e.poll()

    def zero(self, confirm=1.0):
        self.e.zero_start(confirm, self.zeros.append)

    def wait(self, target=100.0, expected=2000.0, limit=4000.0):
        self.e.wait_start(target, expected, limit, self.ends.append)


def _params(**kw):
    p = dict(thickness=100.0, dep_rate=0.05, use_g1=True, use_g2=False, use_g3=False, shutter_delay=5.0,
             chuck_position="mid", process_note="t1", use_dc_pulse=True, dc_pulse_power=80.0)
    p.update(kw)
    return p


def _apply(r, p, chuck_ok=True, actual=None):
    box = []
    r.e.apply_material(p, chuck_ok, box.append, actual_chuck=actual)
    assert box
    return box[0]


# ═══════════════════════ 모드·점검(v1 test_thickness_runtime.py:136-195) ═══════════════════════
def test_supported_requires_driver_and_flag(tmp_path):
    assert not Rig(tmp_path, driver=False).e.supported
    assert not Rig(tmp_path, cfg={"CH2_SUPPORTS_THICKNESS": False}).e.supported
    r = Rig(tmp_path)
    assert r.e.supported and r.e.thickness_mode({"thickness": "100"}) and not r.e.thickness_mode({"thickness": ""})
    r.d.enabled = False                                         # 포트 0 등으로 드라이버가 끈 채널
    assert not r.e.supported


@pytest.mark.parametrize("mut,code", [
    (lambda s, p: s.update(connected=False), "E840"),
    (lambda s, p: s.update(ok_ts=90.0), "E840"),
    (lambda s, p: s.update(ok_ts=None), "E840"),
    (lambda s, p: s.update(crystal_ok=False), "E841"),
    (lambda s, p: s.update(link_ok=False), "E840"),
    (lambda s, p: s.update(link_ok=True, ok_ts=None, stale=True, crystal_ok=False), "E841"),
    (lambda s, p: s.update(link_ok=True, ok_ts=90.0, stale=True), "E840"),
    (lambda s, p: p.update(dep_rate=None), "E842"),
    (lambda s, p: p.update(thickness=0), "E842"),
    (lambda s, p: p.update(use_g2=True), "E843"),
    (lambda s, p: p.update(use_g1=False), "E843"),
    (lambda s, p: p.update(power_change_time="6m", change_power_value="100"), "E844"),
])
def test_basic_check_rejects(tmp_path, mut, code):
    r = Rig(tmp_path)
    p = _params()
    mut(r.d.st, p)
    c, msg = r.e.check_basic(p)
    assert c == code and msg


def test_basic_check_messages_and_pass(tmp_path):
    r = Rig(tmp_path)
    assert r.e.check_basic(_params()) == ("", "")
    assert r.e.check_basic(_params(power_change_time="2m", change_power_value="100")) == ("", "")   # Shutter Delay 안
    r.d.st.update(link_ok=False, ok_ts=None)
    assert r.e.check_basic(_params())[1] == "두께 모니터 응답 없음 — 연결·최근 5초 안 정상 두께 값이 필요합니다"
    r.d.st.update(link_ok=True, ok_ts=90.0)
    assert r.e.check_basic(_params()) == ("E840", "두께 값 없음(통신은 정상) — 최근 5초 안 정상 두께 값이 필요합니다 (마지막 정상 값 10초 전)")
    r.d.st.update(ok_ts=None, crystal_ok=False)
    assert r.e.check_basic(_params()) == ("E841", "두께 모니터 XTAL FAIL(크리스탈 이상) — 크리스탈을 확인하십시오")
    r.d.st.update(crystal_ok=True, ok_ts=99.0)
    assert r.e.check_basic(_params(use_g2=True)) == ("E843", "Thickness 모드는 건 1개만 쓸 수 있습니다 — co-sputter(G1+G2)")
    assert Rig(tmp_path, driver=False).e.check_basic(_params())[0] == "E840"


# ═══════════════════════ 재료값 적용(v1 :196-294) ═══════════════════════
def test_apply_material_factor_and_errors(tmp_path):
    r = Rig(tmp_path)
    c, msg = _apply(r, _params(chuck_position="up"))
    assert c == "E845" and "tooling" in msg and r.d.mat_calls == []
    r.e.factors.material["G1"] = None
    c, msg = _apply(r, _params())
    assert c == "E845" and "G1 재료 미선택" in msg and r.d.mat_calls == []
    r.e.factors.material["G1"] = "Unobtainium"
    c, msg = _apply(r, _params())
    assert c == "E845" and "G1 재료 'Unobtainium' 가 재료 목록에 없음" in msg
    r.e.factors.material["G1"] = "aluminum"
    assert _apply(r, _params(chuck_position=""))[0] == "E845"
    r.d.mat_error = "되읽기 불일치 — 되읽기 불일치 — density 보냄 2.73/읽음 2.6"
    c, msg = _apply(r, _params())
    assert c == "E846" and msg == "두께 모니터에 재료값을 쓰지 못함(되읽기 확인 실패 포함): 되읽기 불일치 — density 보냄 2.73/읽음 2.6"
    r.d.mat_error = None
    r.d.st["life_pct"] = 4.0
    p = _params()
    assert _apply(r, p) == ("", "")
    assert r.d.mat_calls[-1][:4] == (2.73, 1.08, 110.0, "Aluminum")
    pre = r.e._pre
    assert (pre["tooling"], pre["chuck"], pre["gun"], pre["material"]) == (110.0, "mid", "G1", "Aluminum")
    assert pre["factor_source"] == "T:factor D:factor Z:factor"
    assert any("tooling 110 % (Factor mid×G1) · density 2.73 (Factor G1 Al) · Z 1.080 (Factor G1 Al)" in m for m in r.msgs())
    assert any("크리스탈 수명" in w for w in r.e.run_warnings)
    assert p["thickness_mode"] is True and p["thickness_expected_s"] == pytest.approx(2000.0) and p["thickness_limit_s"] == 4000.0


def test_apply_material_uses_actual_chuck_when_move_failed(tmp_path):
    r = Rig(tmp_path)
    r.e.factors.tooling["down"]["G1"] = 90.0
    assert _apply(r, _params(chuck_position="mid"), chuck_ok=False, actual="down") == ("", "")
    assert r.d.mat_calls[-1][2] == 90.0 and "Chuck 이동 실패 — Factor 는 실제 위치 'down' 기준" in r.msgs("THICKNESS")
    assert _apply(r, _params(), chuck_ok=False, actual="unknown")[0] == "E845"


_ALL, _NONE = "all", "none"


@pytest.mark.parametrize("recipe,fac,call,source", [
    ({"tooling": 120.0, "density": 3.0, "z_factor": 0.5}, _NONE, (3.0, 0.5, 120.0), "T:recipe D:recipe Z:recipe"),
    ({"tooling": 120.0, "density": 3.0, "z_factor": 0.5}, _ALL, (3.0, 0.5, 120.0), "T:recipe D:recipe Z:recipe"),
    ({"tooling": 120.0}, _ALL, (2.73, 1.08, 120.0), "T:recipe D:factor Z:factor"),
    ({"density": 3.0}, _ALL, (3.0, 1.08, 110.0), "T:factor D:recipe Z:factor"),
    ({"z_factor": 0.5}, _ALL, (2.73, 0.5, 110.0), "T:factor D:factor Z:recipe"),
    ({}, _ALL, (2.73, 1.08, 110.0), "T:factor D:factor Z:factor"),
    ({"density": 3.0, "z_factor": 0.5}, "tool_only", (3.0, 0.5, 110.0), "T:factor D:recipe Z:recipe"),
])
def test_apply_recipe_values_take_priority_per_value(tmp_path, recipe, fac, call, source):
    r = Rig(tmp_path)
    if fac in (_NONE, "tool_only"):
        r.e.factors.material["G1"] = None
    if fac == _NONE:
        r.e.factors.tooling["mid"]["G1"] = None
    p = _params(**recipe)
    assert _apply(r, p) == ("", "")
    assert r.d.mat_calls[-1][:3] == call and r.e._pre["factor_source"] == source
    used = [n for n in ("tooling", "density", "z_factor") if n in recipe]
    assert p["thickness_factor_note"] == (f"레시피 지정({', '.join(used)})" if used else "")
    both = "density" in recipe and "z_factor" in recipe
    assert r.e._pre["material"] == ("" if both else "Aluminum") and r.d.mat_calls[-1][3] == r.e._pre["material"]
    line = [m for m in r.msgs() if m.startswith("Thickness 모드 — ")][-1]
    assert ("tooling 120 % (레시피)" in line) == ("tooling" in recipe) and ("(Factor G1 Al)" in line) == (not both)


@pytest.mark.parametrize("recipe,parts,absent", [
    ({}, ["tooling — 레시피 없음, Factor mid×G1 미설정", "density·Z — 레시피 없음, Factor G1 재료 미선택"], []),
    ({"tooling": 120.0}, ["density·Z — 레시피 없음, Factor G1 재료 미선택"], ["tooling —"]),
    ({"tooling": 120.0, "density": 3.0}, ["Z — 레시피 없음, Factor G1 재료 미선택"], ["tooling —", "density"]),
    ({"density": 3.0, "z_factor": 0.5}, ["tooling — 레시피 없음, Factor mid×G1 미설정"], ["재료"]),
])
def test_apply_e845_only_when_value_missing_in_both(tmp_path, recipe, parts, absent):
    r = Rig(tmp_path)
    r.e.factors.material["G1"] = None
    r.e.factors.tooling["mid"]["G1"] = None
    c, msg = _apply(r, _params(**recipe))
    assert c == "E845" and r.d.mat_calls == []
    assert all(s in msg for s in parts) and not any(s in msg for s in absent)
    assert msg.endswith(f"(화면 [Factor 설정] 또는 {tmp_path / 'thickness_factors_ch2.json'})")


def test_time_mode_ignores_recipe_factor_columns_and_reject_texts(tmp_path):
    r = Rig(tmp_path)
    p = _params(thickness=None, tooling=120.0, density=3.0, z_factor=0.5)
    assert not r.e.thickness_mode(p)
    r.e.new_run()
    r.e.run_started(p)
    assert r.e.record.rec["mode"] == "Time" and "factor_source" not in r.e.record.rec and r.d.mat_calls == []
    assert r.e.record.view == {"mode": "Time", "target": "—", "pct": None, "text": "Time 모드 공정(두께는 기록만)"}
    log, chat = r.e.reject_texts("E843", "co-sputter", {"process_note": "run1"})
    assert log == "[CH2] 두께 모니터 점검 실패 [E843] co-sputter → 공정 시작 차단"
    assert chat == "두께 모니터 점검 실패: co-sputter → 공정 시작 차단 (공정: run1)"


# ═══════════════════════ THICKNESS_ZERO(v1 test_thickness_zero_confirm.py:180-220, process_steps:171-187) ═══════════════════════
def test_zero_step_confirms_and_sets_base_zero(tmp_path):
    r = Rig(tmp_path)
    r.zero(1.0)
    assert r.d.zero_calls == [("B", "메인 셔터 열기 전 영점")] and r.zeros == []
    assert r.e.record.view["text"] == "영점 확인 중"
    r.feed(350.0, dt=0.1)
    r.feed(0.0, dt=0.1)
    assert r.zeros and r.zeros[0].base == 0.0 and not r.zeros[0].timed_out and r.e.run_warnings == []
    assert any(m.startswith("영점 확인 — 영점 뒤 0.2초 값 0.00 nm") for m in r.msgs("THICKNESS"))
    r.e._params = {"dep_rate": 0.05}
    r.wait()
    j = r.e._judge
    assert j is not None and j.base == 0.0 and j.target == 100.0 and j.dep_rate == 0.05 and r.d.zero_calls == [("B", "메인 셔터 열기 전 영점")]


Z1 = {"CH2_THICKNESS_ZERO_CONFIRM_S": 1.0}                 # v1 시험도 1초로(test_thickness_zero_confirm.py:155)


def test_zero_step_not_confirmed_or_cmd_failed(tmp_path):
    r = Rig(tmp_path, cfg=Z1)
    r.zero(1.0)
    for _ in range(12):
        r.feed(50.0, dt=0.1)
    assert r.zeros[0].base == 50.0 and r.e.run_warnings[0].startswith("영점 확인 안 됨 — 50.00 nm 를 기준점으로(1초")
    r2 = Rig(tmp_path, cfg=Z1)
    r2.d.zero_ok = False
    r2.zero(1.0)
    assert "⚠ 영점 명령 실패(영점 실패: 응답 없음 'B' (3회))" in r2.msgs("THICKNESS")
    r2.feed(12.0, dt=0.1)
    assert r2.zeros[0].base == 12.0 and r2.e.run_warnings[0].startswith("영점 명령 실패 — 첫 값 12.00 nm")


def test_zero_step_comm_lost_then_permanent_time_fallback(tmp_path):
    r = Rig(tmp_path, cfg=Z1)
    r.zero(1.0)
    for _ in range(11):
        r.c.t += 0.1
        r.d.publish()                                           # 새 값 없음
    assert r.zeros[0].base is None and any("기준점 없음" in w for w in r.e.run_warnings)
    r.e._params = {"dep_rate": 0.05}
    r.wait()
    assert r.e._judge.permanent and r.e._wait["fb_t"] == pytest.approx(r.c.t + 2000.0)
    assert "두께 → 시간 대체: 2000초 뒤 메인 공정 종료 (영점 확인 중 두께 값 없음 — 기준점 없음)" in r.msgs("Process")


def test_zero_step_hard_budget_never_hangs_and_no_driver(tmp_path):
    r = Rig(tmp_path)
    r.d.hold = True                                             # 영점 요청이 큐에서 끝나지 않음
    r.zero(1.0)
    r.c.t += 30.9
    r.e.poll()
    assert r.zeros == [] and r.e.next_deadline() == pytest.approx(100.0 + 31.0)
    r.c.t += 0.2
    r.e.poll()
    assert r.zeros[0].timed_out and "⚠ 두께 영점·확인 31초 초과 → 기다림을 끊고 진행" in r.msgs("Process")
    n = Rig(tmp_path, driver=False)
    n.zero(5.0)
    assert n.zeros == [ZeroDone(None)] and n.e._zero_info == {"base": None, "reset0": 0}


def test_zero_step_abort(tmp_path):
    r = Rig(tmp_path)
    r.zero(5.0)
    r.e.abort()
    assert r.zeros[0].aborted and "두께 영점·확인 중단됨" in r.msgs("Process")
    r.feed(0.0)
    assert len(r.zeros) == 1


# ═══════════════════════ THICKNESS_WAIT 타이머(v1 test_thickness_process_steps.py:225-273) ═══════════════════════
def _waiting(tmp_path, **kw):
    r = Rig(tmp_path, **kw)
    r.e._zero_info = {"base": 0.0, "reset0": 0}
    r.e._params = {"dep_rate": 0.05}
    return r


def test_limit_timer_ends_without_listener_e848(tmp_path):
    r = _waiting(tmp_path)
    r.e.new_run()
    r.e.run_started({"thickness_mode": True, "thickness": 10.0})
    r.e.wait_start(10.0, 0.3, 0.6, r.ends.append, message="메인 공정 진행 (두께 10nm)")
    assert r.e.next_deadline() == pytest.approx(100.6)
    r.c.t += 0.59
    r.e.on_row()
    assert r.ends == []
    r.c.t += 0.02
    r.e.on_row()
    end = r.ends[0]
    assert (end.end, end.ok, end.code, end.source) == ("상한", False, "E848", "THICKNESS") and end.elapsed_s == pytest.approx(0.61)
    assert end.detail == "두께 목표 미도달 — 최대 시간 초과: 유효 0.0 / 목표 10.0 nm, 경과 0.0분(상한 0.0분)"
    assert end.meta == {"ch": 2, "target_nm": 10.0, "limit_s": 0.6}
    assert "메인 공정 진행 (두께 10nm) 완료 — 종료 방식 상한 (1초)" in r.msgs("Process")
    assert "⚠ 최대 시간 상한 도달 — 유효 0.0 / 목표 10.0 nm → 공정 실패(E848)" in r.msgs("THICKNESS")
    assert r.msgs("Process")[1] == "[ETA] Thickness 모드 — 메인 공정 종료 시각이 정해지지 않아 남은 시간을 확정하지 않습니다(remaining_s=null)"
    assert r.e.eta_remaining_s() is None and r.e._judge is None and r.e.run_warnings == []


def test_done_ends_thickness_and_stops_oes_once(tmp_path):
    r = _waiting(tmp_path)
    r.e.run_started({"thickness_mode": True, "thickness": 1.0})
    r.e.wait_start(1.0, 20.0, 40.0, r.ends.append)
    r.feed(0.5)
    r.feed(1.0)
    assert r.ends == [] and r.e.snapshot_fields()["done_n"] == 1          # CH2 운전값 2번 연속
    r.feed(1.0)                                                  # 같은 값이어도 새 seq 면 새 값
    assert [e.end for e in r.ends] == ["Thickness"] and r.ends[0].ok
    assert r.oes == [r.c.t] and "OES 측정 종료 요청 — 메인 공정 끝(Thickness 모드)" in r.msgs("OES")
    assert "메인 공정 종료 — 종료 방식 Thickness, 유효 1.0 / 목표 1.0 nm" in r.msgs("THICKNESS")
    r.feed(2.0)
    assert len(r.ends) == 1 and len(r.oes) == 1


def test_fallback_timer_ends_and_resume_cancels(tmp_path):
    r = _waiting(tmp_path)
    r.e.wait_start(10.0, 100.0, 60.0, r.ends.append)
    r.feed(1.0)
    for _ in range(11):
        r.idle(1.0)                                             # 새 값 없음 11초 → 시간 대체
    fb = r.e._wait["fb_t"]
    assert fb is not None and any(m.startswith("두께 → 시간 대체: ") for m in r.msgs("Process"))
    r.feed(2.0)                                                 # 복귀 → 시간 대체 타이머 해제(상한 그대로)
    assert r.e._wait["fb_t"] is None and "두께 판정 복귀 — 시간 대체 타이머 해제" in r.msgs("Process")
    for _ in range(11):
        r.idle(1.0)
    r.c.t = r.e._wait["fb_t"] + 0.01
    r.e.poll()
    assert [e.end for e in r.ends] == ["시간대체"] and r.ends[0].ok           # 시간 대체는 실패가 아니다


def test_start_failure_and_no_driver_use_expected_time(tmp_path):
    r = _waiting(tmp_path)
    r.e._params = {"dep_rate": "bad"}
    r.e.wait_start(10.0, 0.2, 5.0, r.ends.append)
    assert any(m.startswith("⚠ 두께 판정 시작 실패(") and m.endswith("→ 예상시간으로 시간 대체") for m in r.msgs("Process"))
    r.c.t += 0.21
    r.e.poll()
    assert [e.end for e in r.ends] == ["시간대체"]
    n = Rig(tmp_path, driver=False)
    n.e.wait_start(10.0, 0.2, 5.0, n.ends.append)
    assert "두께 → 시간 대체: 0초 뒤 메인 공정 종료 (두께 서비스 없음 — 예상시간으로 시간 대체)" in n.msgs("Process")
    n.c.t += 0.25
    n.e.on_row()
    assert [e.end for e in n.ends] == ["시간대체"]


def test_abort_ends_wait_and_records(tmp_path):
    r = _waiting(tmp_path)
    r.e.run_started({"thickness_mode": True, "thickness": 10.0})
    r.e.wait_start(10.0, 100.0, 200.0, r.ends.append, message="메인 공정 진행")
    r.feed(3.0)
    r.e.abort()
    assert [(e.end, e.ok) for e in r.ends] == [("중단", False)] and r.e._judge is None
    assert "메인 공정 진행 중단됨 (0초)" in r.msgs("Process") and r.e.record_values()["end"] == "중단"
    assert r.e.record_values()["final_nm"] == pytest.approx(3.0)


# ═══════════════════════ 리스너(v1 test_thickness_runtime.py:361-440) ═══════════════════════
def test_listener_done_fallback_resume(tmp_path):
    r = Rig(tmp_path)
    r.e._zero_info = {"base": 3.0, "reset0": 0}
    r.e._params = {"dep_rate": 0.1}
    r.e.wait_start(3.0, 30.0, 100.0, r.ends.append)
    for i in range(10):
        r.feed(3.0 + 0.05 * (i + 1))
    assert r.states and r.ends == []
    for _ in range(12):
        r.idle(1.0)
    assert sum(m.startswith("두께 → 시간 대체") for m in r.msgs("Process")) == 1         # (3 − 0.5)/0.1 − 11 = 14 s
    assert r.states[-1].startswith("[시간 대체] 두께 0.5 / 3.0 nm")
    r.feed(3.95)
    assert "두께 판정 복귀 — 시간 대체 타이머 해제" in r.msgs("Process")
    r.feed(6.0)
    r.feed(6.05)
    assert [e.end for e in r.ends] == ["Thickness"] and r.e._judge is None


def test_listener_exception_falls_back_once_with_traceback(tmp_path):
    r = Rig(tmp_path)
    r.e._zero_info = {"base": 0.0, "reset0": 0}
    r.e.wait_start(10.0, 100.0, 1000.0, r.ends.append)
    j = ThicknessEndJudge(10.0, 0.1, JudgeParams(), start_t=r.c.t, base=0.0, seq0=7)

    def boom(*a):
        raise RuntimeError("주입 예외")
    j.tick = boom
    r.e._judge = j
    r.d.publish()
    r.d.publish()
    fb = [m for m in r.msgs("Process") if m.startswith("두께 → 시간 대체")]
    assert fb == ["두께 → 시간 대체: 100초 뒤 메인 공정 종료 (판정 예외(RuntimeError('주입 예외')))"]   # (10 − 0) ÷ 0.1
    assert len([m for m in r.msgs("THICKNESS") if "Traceback" in m]) == 1 and r.e._judge is None


def test_judge_internal_exception_event_logs_traceback(tmp_path):
    r = Rig(tmp_path)
    r.e._zero_info = {"base": 0.0, "reset0": 0}
    r.e.wait_start(10.0, 100.0, 1000.0, r.ends.append)
    r.feed("bad")
    assert any(m.startswith("두께 → 시간 대체") for m in r.msgs("Process"))
    assert any(m.startswith("⚠ 두께 판정 예외 traceback:\nTraceback") for m in r.msgs("THICKNESS"))


# ═══════════════════════ 기록(v1 :443-492, round14:223-245) ═══════════════════════
def test_time_mode_record_uses_main_window_values(tmp_path):
    r = Rig(tmp_path)
    r.e.run_started({"process_note": "x"})
    r.feed(1.0)                                                 # 메인 공정 전 — 버림
    r.e.set_main_polling(True)
    r.feed(2.0, dt=10.0)
    r.d.publish()                                               # 같은 seq
    r.feed(3.0, dt=10.0)
    r.feed(4.0, dt=10.0)
    r.feed(9.0, dt=10.0, stale=True)                            # stale 은 버림
    v = r.e.record_values()
    assert v["end"] == "Time" and v["final_nm"] == pytest.approx(2.0) and v["avg_rate"] == pytest.approx(2.0 / 20.0)
    p = {}
    r.e.card_fields(p)
    assert p == {"thickness_end": "Time", "thickness_final": "2.0 nm"}
    r.e.run_finished()
    assert r.e.record.view == {"mode": "Time", "target": "—", "pct": None, "text": "종료(Time) — 2.0 nm"}
    assert "두께 요약 — 종료 방식 Time · 최종 2.00 nm · 평균 0.1000 nm/s" in r.msgs("THICKNESS")
    n = Rig(tmp_path, driver=False)
    n.e.run_started({"process_note": "x"})
    p = {}
    n.e.card_fields(p)
    assert p == {} and n.e.run_finished() == {}


def test_limit_record_and_card(tmp_path):
    r = Rig(tmp_path)
    r.e._pre = {"chuck": "mid", "gun": "G1", "tooling": 110.0, "density": 2.7, "z_factor": 1.08}
    r.e.run_started({"thickness_mode": True, "thickness": 100.0})
    j = ThicknessEndJudge(100.0, 0.05, JudgeParams(), start_t=0, base=0.0, seq0=0)
    j.eff, j.eff_t, j.jump_count = 80.0, 1600.0, 2
    r.e._last_judge = j
    assert r.e.record.on_wait_end("상한", j) == "유효 80.0 / 목표 100.0 nm"
    v = r.e.record_values()
    assert v["end"] == "상한" and v["final_nm"] == 80.0 and v["avg_rate"] == pytest.approx(0.05)
    assert v["tooling"] == 110.0 and v["jumps"] == 2 and r.e.run_warnings == []
    p = {}
    r.e.card_fields(p)
    assert p == {"thickness_end": "상한", "thickness_final": "80.0 / 100 nm", "thickness_jumps": "2회(기록만)"}


def test_stop_before_main_records_abort(tmp_path):
    r = Rig(tmp_path)
    r.e._pre = {"chuck": "mid", "gun": "G1", "tooling": 110.0, "density": 2.7, "z_factor": 1.08}
    r.e.run_started({"thickness_mode": True, "thickness": 100.0})
    assert r.e.record.view["text"] == "공정 시작 — 메인 공정 대기"
    v = r.e.record_values()
    assert v["end"] == "중단" and v["final_nm"] is None and v["tooling"] == 110.0
    card = {}
    r.e.card_fields(card)
    assert card == {"thickness_end": "중단", "thickness_final": "-- / 100 nm"}
    rec = r.e.run_finished()
    assert rec["end"] == "중단" and rec["final_nm"] is None
    assert r.e.record.view == {"mode": "중단", "target": "100.0 nm", "pct": None, "text": "종료(중단) — 메인 공정 전"}


def test_fallback_end_adds_run_warning(tmp_path):
    r = _waiting(tmp_path)
    r.e.run_started({"thickness_mode": True, "thickness": 10.0})
    r.e.wait_start(10.0, 100.0, 300.0, r.ends.append)
    r.feed(1.0)
    r.d.st["crystal_ok"] = False
    r.idle(0.5)
    r.c.t = r.e._wait["fb_t"]
    r.e.poll()
    assert r.ends[0].end == "시간대체"
    assert r.e.run_warnings == ["시간 대체로 종료(XTAL FAIL(크리스탈 이상)) — 마지막 확인 유효 1.0 / 목표 10.0 nm"]


# ═══════════════════════ 1초 행 ═══════════════════════
def test_snapshot_fields_keep_thickness_when_stale(tmp_path):
    r = _waiting(tmp_path, cfg={"CH2_THICKNESS_DONE_CONSECUTIVE": 2})
    r.e.wait_start(10.0, 100.0, 300.0, r.ends.append)
    r.feed(4.0)
    r.feed(10.0)
    f = r.e.snapshot_fields()
    assert (f["thickness_nm"], f["target_nm"], f["eff_nm"], f["remaining_nm"]) == (10.0, 10.0, 10.0, 0.0)
    assert (f["mode"], f["phase"], f["done_n"], f["done_need"], f["judge"]) == ("Thickness", "wait", 1, 2, "thickness")
    r.d.st.update(stale=True)                                    # 값 없음·stale — 드라이버가 마지막 값을 유지
    assert r.e.snapshot_fields()["thickness_nm"] == 10.0
    assert Rig(tmp_path, driver=False).e.snapshot_fields()["phase"] == "idle"


def test_dataclasses_exported():
    assert dataclasses.is_dataclass(WaitEnd) and dataclasses.is_dataclass(ZeroDone)


def test_snapshot_block_fields_match_engine(tmp_path):
    from v2.ch12.thickness_record import BLOCK_FIELDS, blocks
    b = blocks("ch2")[0]
    assert b.name == "ch2.thk" and tuple(f.name for f in b.fields) == BLOCK_FIELDS
    assert set(BLOCK_FIELDS) <= set(Rig(tmp_path).e.snapshot_fields())

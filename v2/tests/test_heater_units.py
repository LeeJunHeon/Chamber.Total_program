"""CH2 히터 판정 모듈 단위 시험 — v1 시험의 입력·기대값을 그대로 옮김(v1 을 import 하지 않음, 근거 = v1 시험 파일:줄).

옮긴 v1 시험: tests/test_heater_profile.py(4), tests/test_heater_ramp.py(10), tests/test_heater_hold.py(9),
tests/test_heater_saturation.py(16). 나머지 판정 모듈(응답·설정·대기)은 test_heater_guards.py.
"""
from __future__ import annotations

import math

import pytest

from v2.ch12.heater_hold import HeaterHold
from v2.ch12.heater_profile import HeaterCfg, approach_rate, ramp_minutes, solve_rate_for_minutes
from v2.ch12.heater_ramp import RampProfiler
from v2.ch12.heater_saturation import HeaterSaturationGuard


class _Clock:
    def __init__(self, t: float = 1000.0):
        self.t = t

    def __call__(self):
        return self.t


# ═════════════════════ 감속 접근 프로파일(tests/test_heater_profile.py:16-53) ═════════════════════
Z, M = 20.0, 1.0


def test_approach_rate_linear_in_zone():
    assert approach_rate(30.0, 12.0, Z, M) == 12.0
    assert approach_rate(20.0, 12.0, Z, M) == 12.0
    assert abs(approach_rate(10.0, 12.0, Z, M) - 6.5) < 1e-9
    assert approach_rate(0.0, 12.0, Z, M) == 1.0
    assert approach_rate(-3.0, 12.0, Z, M) == 1.0
    assert approach_rate(5.0, 0.8, Z, M) == 0.8
    assert approach_rate(5.0, 12.0, 0.0, M) == 12.0


def test_ramp_minutes_approach_integral():
    t = ramp_minutes(20.0, 12.0, Z, M)
    assert abs(t - math.log(12.0) / 0.55) < 1e-9 and abs(t - 4.52) < 0.01
    assert abs(ramp_minutes(100.0, 12.0, Z, M) - (80.0 / 12.0 + t)) < 1e-9
    assert ramp_minutes(10.0, 12.0, 0.0, M) == pytest.approx(10.0 / 12.0)
    assert ramp_minutes(0.0, 12.0, Z, M) == 0.0
    assert ramp_minutes(10.0, 0.0, Z, M) == 0.0


@pytest.mark.parametrize("span,rate", [(100.0, 12.0), (300.0, 6.0), (15.0, 30.0), (500.0, 60.0)])
def test_solve_rate_roundtrip(span, rate):
    minutes = ramp_minutes(span, rate, Z, M)
    r = solve_rate_for_minutes(span, minutes, Z, M)
    assert abs(r - rate) < 1e-6 * max(1.0, rate) and r >= span / minutes


def test_solve_rate_edges():
    assert solve_rate_for_minutes(0.0, 10.0, Z, M) == 0.0
    assert solve_rate_for_minutes(10.0, 0.0, Z, M) == 0.0
    assert solve_rate_for_minutes(10.0, 20.0, Z, M) == 0.5


# ═════════════════════ RampProfiler(tests/test_heater_ramp.py:41-160) ═════════════════════
def _ramp(st, **cfg_kw):
    writes, logs = [], []
    clk = _Clock(100.0)
    p = RampProfiler(lambda: st, lambda v: writes.append(("sv", v)), lambda c: writes.append(("ramp", c)),
                     logs.append, HeaterCfg(**cfg_kw), clock=clk)
    return p, writes, logs, clk


def test_ramp_start_writes_ramp_then_sv_base():
    p, w, _, _ = _ramp({"run": False, "pv": 100.0, "sv_ramp": 99.0})
    assert p.start(200.0, 12.0, "수동") is True
    assert w == [("ramp", 3), ("sv", 100.0)]                     # max(2, ceil(12/6)+1)=3, 출발 = PV(정지 중)
    assert p.is_active() and p.target() == 200.0 and p.rate() == 12.0


def test_ramp_start_from_sv_ramp_when_running():
    p, w, _, _ = _ramp({"run": True, "pv": 150.0, "sv_ramp": 160.0})
    assert p.start(200.0, 30.0)
    assert w == [("ramp", 6), ("sv", 160.0)]


def test_ramp_target_not_above_base_returns_false():
    p, w, _, _ = _ramp({"run": False, "pv": 100.0, "sv_ramp": 100.0})
    assert p.start(100.05, 12.0) is False and p.start(80.0, 12.0) is False
    assert w == [] and not p.is_active()


def test_ramp_tick_rate_and_lead_limit():
    st = {"run": True, "pv": 100.0, "sv_ramp": 100.0}
    p, w, _, clk = _ramp(st)
    p.start(300.0, 12.0)
    w.clear()
    clk.t += 1.0
    p.tick()
    assert w == [("sv", 100.2)]
    p2, w2, _, clk2 = _ramp(dict(st), approach_lead=1.0)
    p2.start(300.0, 120.0)
    w2.clear()
    clk2.t += 3.0
    p2.tick()
    assert w2 == [("sv", 101.0)]


def test_ramp_dt_capped_at_5s():
    p, w, _, clk = _ramp({"run": True, "pv": 100.0, "sv_ramp": 100.0}, approach_lead=50.0)
    p.start(300.0, 12.0)
    w.clear()
    clk.t += 100.0
    p.tick()
    assert w == [("sv", 101.0)]


def test_ramp_approach_decelerates_near_target():
    p, w, _, clk = _ramp({"run": True, "pv": 195.0, "sv_ramp": 195.0})
    p.start(200.0, 12.0)
    w.clear()
    clk.t += 5.0
    p.tick()
    assert w == [("sv", 195.3)]
    pf, wf, _, clkf = _ramp({"run": True, "pv": 100.0, "sv_ramp": 100.0})
    pf.start(200.0, 12.0)
    wf.clear()
    clkf.t += 5.0
    pf.tick()
    assert wf == [("sv", 101.0)]


def test_ramp_completion():
    p, w, logs, clk = _ramp({"run": True, "pv": 199.9, "sv_ramp": 199.9}, approach_lead=5.0)
    p.start(200.0, 12.0)
    for _ in range(40):
        clk.t += 5.0
        p.tick()
        if not p.is_active():
            break
    assert w[-1] == ("sv", 200.0) and not p.is_active() and p.target() is None
    assert any("램프 완료" in m for m in logs)


def test_ramp_completion_waits_for_ladder():
    st = {"run": True, "pv": 199.0, "sv_ramp": 199.0}
    p, _, _, clk = _ramp(st, approach_lead=5.0)
    p.start(200.0, 600.0)
    clk.t += 5.0
    p.tick()
    assert p.is_active()
    st["sv_ramp"] = 199.9
    clk.t += 1.0
    p.tick()
    assert not p.is_active()


def test_ramp_stop_restores_ramp_rate():
    p, w, _, _ = _ramp({"run": True, "pv": 100.0, "sv_ramp": 100.0})
    p.start(300.0, 12.0)
    w.clear()
    p.stop(restore_rate=True)
    assert w == [("ramp", 2)]
    w.clear()
    p.start(300.0, 12.0)
    w.clear()
    p.stop(restore_rate=False)
    assert w == []
    p.stop(restore_rate=True)
    assert w == []


def test_ramp_write_failure_falls_back():
    def boom(_v):
        raise RuntimeError("link")
    p = RampProfiler(lambda: {"run": False, "pv": 100.0, "sv_ramp": 100.0}, boom, boom, lambda m: None, HeaterCfg())
    assert p.start(200.0, 12.0) is False and not p.is_active()


# ═════════════════════ HeaterHold(tests/test_heater_hold.py:28-128) ═════════════════════
def _hold(mode="dac"):
    clk = _Clock()
    ev = {"req": [], "msg": [], "eng": [], "give": []}
    h = HeaterHold(mode, mv_limit=520, mv_min=400, enter_tol_c=3.0, enter_sec=60.0, arrive_tol_c=1.0,
                   drift_pv_c=0.5, drift_mv=15.0, clock=clk, request_mv_limit=ev["req"].append,
                   message=lambda lv, t: ev["msg"].append((lv, t)), engaged=ev["eng"].append, give_up=ev["give"].append)
    return h, clk, ev


def _hst(pv=300.0, mv=470, **kw):
    d = {"ok": True, "run": True, "fault": False, "pv": pv, "sv": 300.0, "sv_ramp": 300.0, "mv": mv, "mv_limit": 520}
    d.update(kw)
    return d


def _hfeed(h, clk, secs, fn):
    for i in range(int(secs)):
        h.tick(fn(i), 300.0)
        clk.t += 1.0


def test_hold_capture_after_enter_window():
    h, clk, ev = _hold()
    _hfeed(h, clk, 61, lambda i: _hst())
    assert h.is_holding() and h.value == 470 and ev["req"] == [470] and ev["eng"] == ["dac"] and h.arrived


def test_hold_needs_arrival_latch():
    h, clk, ev = _hold()
    _hfeed(h, clk, 80, lambda i: _hst(pv=302.0))
    assert not h.is_holding() and not h.arrived and ev["req"] == []


def test_hold_drift_remeasures():
    h, clk, ev = _hold()
    _hfeed(h, clk, 61, lambda i: _hst(mv=450 if i < 31 else 490))
    assert not h.is_holding() and ev["req"] == [] and any("재측정" in t for _lv, t in ev["msg"])
    assert h.last_window_mv is not None
    _hfeed(h, clk, 62, lambda i: _hst(mv=480))
    assert h.is_holding() and ev["req"] == [480]


def test_hold_give_up_near_limit():
    h, clk, ev = _hold()
    _hfeed(h, clk, 61, lambda i: _hst(mv=515))
    assert not h.is_holding() and ev["req"] == [] and len(ev["give"]) == 1 and h.gave_up
    _hfeed(h, clk, 70, lambda i: _hst(mv=515))
    assert len(ev["give"]) == 1


def test_hold_floor_not_captured():
    h, clk, ev = _hold()
    _hfeed(h, clk, 61, lambda i: _hst(mv=410))
    assert not h.is_holding() and ev["req"] == [] and any("최소치" in t for _lv, t in ev["msg"])


@pytest.mark.parametrize("kill", ["run", "fault", "target"])
def test_hold_release_restores_on_run_off_fault_target_change(kill):
    h, clk, ev = _hold()
    _hfeed(h, clk, 61, lambda i: _hst())
    assert h.is_holding()
    ev["req"].clear()
    if kill == "run":
        h.tick(_hst(run=False), 300.0)
    elif kill == "fault":
        h.tick(_hst(fault=True), 300.0)
    else:
        h.tick(_hst(sv=310.0), 310.0)
    assert not h.is_holding() and ev["req"] == [520]


def test_hold_reapply_when_reverted():
    h, clk, ev = _hold()
    _hfeed(h, clk, 61, lambda i: _hst())
    ev["req"].clear()
    h.tick(_hst(mv_limit=520), 300.0)
    assert ev["req"] == []
    clk.t += 5.0
    h.tick(_hst(mv_limit=520), 300.0)
    assert ev["req"] == [470]
    h.tick(_hst(mv_limit=470), 300.0)
    assert ev["req"] == [470]


def test_hold_mode_off_does_nothing():
    h, clk, ev = _hold("off")
    _hfeed(h, clk, 80, lambda i: _hst())
    assert not h.is_holding() and ev["req"] == []


def test_hold_force_dac_hold():
    h, clk, ev = _hold()
    assert h.force_dac_hold(None) is False
    assert h.force_dac_hold(1000) is True and h.value == 520 and ev["req"] == [520]


# ═════════════════════ HeaterSaturationGuard(tests/test_heater_saturation.py:24-202) ═════════════════════
def _sat():
    clk = _Clock()
    ev = {"req": [], "msg": [], "sat": [], "stall": []}
    g = HeaterSaturationGuard(mv_limit=520, mv_min=400, sat_sec=120, arrive_tol_c=1.0, clock=clk,
                              request_mv_limit=ev["req"].append, message=lambda lv, t: ev["msg"].append((lv, t)),
                              saturated=ev["sat"].append, stalled=ev["stall"].append)
    return g, clk, ev


def _sst(mv, pv=300.0, sv=300.0, svr=None, **kw):
    d = {"ok": True, "run": True, "fault": False, "pv": pv, "sv": sv, "sv_ramp": sv if svr is None else svr,
         "mv": mv, "mv_limit": 520}
    d.update(kw)
    return d


def _sfeed(g, clk, secs, mv, hold=False, final=None, ramp=False, **kw):
    for _ in range(int(secs)):
        st = _sst(mv, **kw)
        g.tick(st, hold, "dac", final_target=(final if final is not None else st["sv"]), ramp_active=ramp)
        clk.t += 1.0


def _msgs(ev, word):
    return [t for _lv, t in ev["msg"] if word in t]


def test_sat_post_arrival_clamp_to_pre_average_once():
    g, clk, ev = _sat()
    _sfeed(g, clk, 60, 480)
    assert g.arrived
    _sfeed(g, clk, 122, 515)
    assert ev["req"] == [480] and g.clamped == 480 and len(ev["sat"]) == 1 and ev["sat"][0]["clamp"] == 480
    assert _msgs(ev, "도달 후")
    _sfeed(g, clk, 300, 515)
    assert ev["req"] == [480] and len(ev["sat"]) == 1


@pytest.mark.parametrize("kill", ["run", "fault", "sv"])
def test_sat_release_restores_limit(kill):
    g, clk, ev = _sat()
    _sfeed(g, clk, 60, 480)
    _sfeed(g, clk, 122, 515)
    assert ev["req"] == [480]
    ev["req"].clear()
    if kill == "run":
        g.tick(_sst(515, run=False), False, "dac", final_target=300.0)
    elif kill == "fault":
        g.tick(_sst(515, fault=True), False, "dac", final_target=300.0)
    else:
        g.tick(_sst(515, sv=320.0), False, "dac", final_target=320.0)
    assert ev["req"] == [520] and g.clamped is None


def test_sat_judged_on_readback_limit():
    g, clk, ev = _sat()
    _sfeed(g, clk, 60, 450, mv_limit=470)
    _sfeed(g, clk, 122, 470, mv_limit=470)
    assert len(ev["sat"]) == 1 and ev["sat"][0]["limit"] == 470 and ev["req"] == [450]


def test_sat_arrival_at_limit_is_not_saturation():
    g, clk, ev = _sat()
    _sfeed(g, clk, 600, 515)
    assert g.arrived and ev["req"] == [] and ev["sat"] == [] and not _msgs(ev, "도달 후")


def test_sat_pre_average_also_at_limit_no_clamp_warn_once():
    g, clk, ev = _sat()
    _sfeed(g, clk, 60, 505)
    _sfeed(g, clk, 300, 510, mv_limit=510)
    assert ev["req"] == [] and ev["sat"] == [] and len(_msgs(ev, "클램프하지 않음")) == 1


def test_sat_short_pre_history_no_clamp():
    g, clk, ev = _sat()
    _sfeed(g, clk, 10, 480)
    _sfeed(g, clk, 130, 515)
    assert ev["req"] == [] and ev["sat"] == [] and len(_msgs(ev, "이력 부족")) == 1


def test_sat_saturation_right_after_ramp_uses_ramp_history():
    g, clk, ev = _sat()
    for i in range(60):
        sv = 240.0 + i
        g.tick(_sst(480, pv=sv - 1.0, sv=sv), False, "dac", final_target=300.0, ramp_active=True)
        clk.t += 1.0
    assert not g.arrived
    _sfeed(g, clk, 1, 480)
    _sfeed(g, clk, 122, 515)
    assert ev["req"] == [480]


def test_sat_not_arrived_while_ramp_or_ladder_ramp_behind():
    g, clk, ev = _sat()
    _sfeed(g, clk, 30, 480, ramp=True)
    assert not g.arrived
    _sfeed(g, clk, 30, 480, svr=299.0)
    assert not g.arrived
    _sfeed(g, clk, 1, 480)
    assert g.arrived


def test_sat_target_change_resets_arrival():
    g, clk, ev = _sat()
    _sfeed(g, clk, 10, 480)
    assert g.arrived
    _sfeed(g, clk, 1, 480, sv=320.0)
    assert not g.arrived


def test_sat_before_arrival_never_clamps():
    g, clk, ev = _sat()
    _sfeed(g, clk, 900, 520, pv=250.0)
    assert ev["req"] == [] and ev["sat"] == [] and not _msgs(ev, "DAC 출력 상한")


def test_sat_stall_once_no_clamp():
    g, clk, ev = _sat()
    _sfeed(g, clk, 301, 520, pv=250.0)
    assert len(ev["stall"]) == 1 and ev["req"] == []
    s = ev["stall"][0]
    assert s["target"] == 300.0 and s["limit"] == 520 and s["eta_min"] is None and len(_msgs(ev, "승온 정체")) == 1
    _sfeed(g, clk, 900, 520, pv=250.0)
    assert len(ev["stall"]) == 1


def test_sat_no_stall_below_limit_or_short_eta():
    g, clk, ev = _sat()
    _sfeed(g, clk, 600, 480, pv=250.0)
    assert ev["stall"] == []
    g, clk, ev = _sat()
    for i in range(600):
        g.tick(_sst(520, pv=289.0 + 0.4 * i / 60.0), False, "dac", final_target=300.0)
        clk.t += 1.0
    assert ev["stall"] == []


def test_sat_stall_rejudged_after_target_change():
    g, clk, ev = _sat()
    _sfeed(g, clk, 301, 520, pv=250.0)
    _sfeed(g, clk, 301, 520, pv=250.0, sv=310.0)
    assert len(ev["stall"]) == 2


def test_sat_dac_hold_no_warning_no_clamp():
    g, clk, ev = _sat()
    _sfeed(g, clk, 600, 515, hold=True, mv_limit=515)
    assert ev["req"] == [] and ev["sat"] == [] and ev["msg"] == []


def test_sat_dac_hold_low_pv_info_once():
    g, clk, ev = _sat()
    _sfeed(g, clk, 400, 480, hold=True, pv=280.0, mv_limit=480)
    assert len(_msgs(ev, "유지 중 PV")) == 1 and all(lv == "정보" for lv, t in ev["msg"])
    assert ev["req"] == [] and ev["sat"] == []


def test_sat_nothing_below_limit():
    g, clk, ev = _sat()
    _sfeed(g, clk, 600, 500)
    assert ev == {"req": [], "msg": [], "sat": [], "stall": []}

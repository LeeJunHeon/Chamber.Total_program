"""CH2 히터 판정 모듈 단위 시험 — v1 시험의 입력·기대값을 그대로 옮김(v1 을 import 하지 않음, 근거 = v1 시험 파일:줄).

옮긴 v1 시험: tests/test_heater_profile.py(4+3), tests/test_heater_ramp.py(10), tests/test_heater_hold.py(9),
tests/test_heater_saturation.py(16), tests/test_heater_response.py 판정 단위(15+3), tests/test_heater_clamp_loss.py 판정 단위(5),
tests/test_heater_config.py(14), tests/test_heater_process_sim.py 대기 판정(1), tests/test_heater_recipe_soak.py 대기 판정(1).
"""
from __future__ import annotations

import math

import pytest

from v2.ch12.drivers.heater import float_to_words, words_to_float
from v2.ch12.heater_hold import HeaterHold
from v2.ch12.heater_process import HeaterSoakWaiter, heater_wait_timeout_s
from v2.ch12.heater_profile import HeaterCfg, approach_rate, cfg_to_dict, load_heater_cfg, ramp_minutes, solve_rate_for_minutes
from v2.ch12.heater_ramp import RampProfiler
from v2.ch12.heater_response import HeaterResponseGuard
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


# ═════════════════════ HeaterResponseGuard(tests/test_heater_response.py:27-163) ═════════════════════
def _rg(**kw):
    clk = _Clock()
    return HeaterResponseGuard(clock=clk, **kw), clk


def _rst(pv, mv, **kw):
    d = {"run": True, "ok": True, "fault": False, "stale": False, "pv": pv, "mv": mv, "mv_limit": 1200}
    d.update(kw)
    return d


def _drive(g, clk, secs, pv_fn, mv_fn, target=200.0, holding=False, **kw):
    for i in range(int(secs)):
        r = g.tick(_rst(pv_fn(i), mv_fn(i), **kw), target, holding=holding)
        clk.t += 1.0
        if r is not None:
            return i, r
    return None


def test_resp_a_dead_cold_start_trips_once():
    g, clk = _rg()
    r = _drive(g, clk, 400, lambda i: 25.0, lambda i: 700)
    i, info = r
    assert i == 300 and info["kind"] == "noresp" and info["label"] == "출력 무응답"
    assert info["text"] == "MV 700(≥600) 5분 동안 PV 25.0→25.0°C (운전 시작 PV 25.0°C, 목표 200.0°C)"
    assert "VARITAP" in info["hint"] and g.tripped
    assert _drive(g, clk, 600, lambda i: 25.0, lambda i: 700) is None


def test_resp_a_threshold_follows_actual_limit():
    g, clk = _rg()
    assert _drive(g, clk, 600, lambda i: 25.0, lambda i: 550) is None
    g2, clk2 = _rg()
    r = _drive(g2, clk2, 600, lambda i: 25.0, lambda i: 550, mv_limit=560)
    assert r is not None and r[0] == 300 and "MV 550(≥549)" in r[1]["text"]


def test_resp_a_working_heater_no_trip():
    g, clk = _rg()
    assert _drive(g, clk, 3000, lambda i: 25.0 + 0.1 * i, lambda i: 700) is None


def test_resp_a_slow_but_rising_counts_as_heated():
    g, clk = _rg()
    assert _drive(g, clk, 3000, lambda i: 25.0 + 0.02 * i, lambda i: 900) is None


def test_resp_a_barely_rising_trips():
    g, clk = _rg()
    r = _drive(g, clk, 400, lambda i: 25.0 + 0.01 * i, lambda i: 900)
    assert r is not None and r[0] == 300


def test_resp_a_hot_restart_falling_then_recovering_no_trip():
    g, clk = _rg()
    pv = lambda i: (300.0 - 0.2 * i) if i < 120 else (276.0 + 0.05 * (i - 120))   # noqa: E731
    assert _drive(g, clk, 3000, pv, lambda i: 800, target=320.0) is None


def test_resp_a_no_heating_demand_no_trip():
    g, clk = _rg()
    assert _drive(g, clk, 3000, lambda i: 300.0, lambda i: 800, target=302.0) is None


def test_resp_b_loss_trips():
    g, clk = _rg()
    i, info = _drive(g, clk, 600, lambda i: 200.0 - 0.1 * i, lambda i: 1200)
    assert i == 280 and info["kind"] == "loss" and info["label"] == "출력 끊김"
    assert info["text"].startswith("출력 상한(MV 1200/1200)인데 3분 동안 PV ") and info["text"].endswith("하강(목표 200.0°C)")
    assert "출력이 모자란 채 부하" in info["hint"]


def test_resp_b_uses_readback_limit():
    g, clk = _rg()
    r = _drive(g, clk, 600, lambda i: 200.0 - 0.1 * i, lambda i: 980, mv_limit=980)
    assert r is not None and r[1]["kind"] == "loss" and "MV 980/980" in r[1]["text"]


def test_resp_b_slow_fall_or_not_at_limit_no_trip():
    g, clk = _rg()
    assert _drive(g, clk, 3000, lambda i: 200.0 - 0.03 * i, lambda i: 1200) is None
    g, clk = _rg()
    assert _drive(g, clk, 3000, lambda i: 200.0 - 0.1 * i, lambda i: 1100) is None


def test_resp_b_skipped_while_holding():
    g, clk = _rg()
    assert _drive(g, clk, 600, lambda i: 200.0 - 0.1 * i, lambda i: 1200, holding=True) is None


def test_resp_run_off_resets_and_rearms():
    g, clk = _rg()
    assert _drive(g, clk, 400, lambda i: 25.0, lambda i: 700) is not None
    assert g.tick(_rst(25.0, 0, run=False), 200.0) is None and not g.tripped
    r = _drive(g, clk, 400, lambda i: 25.0, lambda i: 700)
    assert r is not None and r[0] == 300


@pytest.mark.parametrize("bad", [{"stale": True}, {"fault": True}, {"ok": False}, {"pv": None}])
def test_resp_bad_sample_restarts_streak(bad):
    g, clk = _rg()
    assert _drive(g, clk, 200, lambda i: 25.0, lambda i: 700) is None
    st = _rst(25.0, 700)
    st.update(bad)
    assert g.tick(st, 200.0) is None
    clk.t += 1.0
    r = _drive(g, clk, 400, lambda i: 25.0, lambda i: 700)
    assert r is not None and r[0] == 300


def test_resp_gap_restarts_streak():
    g, clk = _rg()
    assert _drive(g, clk, 200, lambda i: 25.0, lambda i: 700) is None
    clk.t += 30.0
    r = _drive(g, clk, 400, lambda i: 25.0, lambda i: 700)
    assert r is not None and r[0] == 300


def test_resp_disabled_never_trips():
    g, clk = _rg(enabled=False)
    assert _drive(g, clk, 1000, lambda i: 25.0, lambda i: 1200) is None
    assert _drive(g, clk, 1000, lambda i: 200.0 - 0.1 * i, lambda i: 1200) is None


# ═════════════════════ 클램프 ↔ B 판정 단위(tests/test_heater_clamp_loss.py:195-264) ═════════════════════
def _cdrive(g, clk, secs, pv_fn, mv_fn, limit_fn, clamped_fn, target=850.0):
    for i in range(int(secs)):
        st = {"run": True, "ok": True, "fault": False, "stale": False, "pv": pv_fn(i), "mv": mv_fn(i),
              "mv_limit": limit_fn(i)}
        r = g.tick(st, target, clamped=clamped_fn(i))
        clk.t += 1.0
        if r is not None:
            return i, r
    return None


def test_unit_clamp_during_fast_fall_keeps_judging():
    clk = _Clock()
    g = HeaterResponseGuard(clock=clk)
    r = _cdrive(g, clk, 600, lambda i: 840.0 - 0.2 * i, lambda i: 1200 if i < 120 else 1117,
                lambda i: 1200 if i < 120 else 1117, lambda i: i >= 120)
    assert r is not None and r[1]["kind"] == "loss" and r[0] == 180


def test_unit_clamp_without_fall_rests_until_released():
    clk = _Clock()
    g = HeaterResponseGuard(clock=clk)
    pv = lambda i: 849.0 if i < 120 else 849.0 - 0.2 * (i - 120)                    # noqa: E731
    r = _cdrive(g, clk, 900, pv, lambda i: 1200 if i < 120 else 1117, lambda i: 1200 if i < 120 else 1117,
                lambda i: i >= 120)
    assert r is None and not g.tripped
    r = _cdrive(g, clk, 600, lambda i: 669.0 - 0.2 * i, lambda i: 1200, lambda i: 1200, lambda i: False)
    assert r is not None and r[1]["kind"] == "loss"


def test_unit_loss_wait_only_while_window_short():
    clk = _Clock()
    g = HeaterResponseGuard(clock=clk)
    assert not g.loss_wait()
    _cdrive(g, clk, 30, lambda i: 840.0, lambda i: 1200, lambda i: 1200, lambda i: False)
    assert g.loss_wait()
    _cdrive(g, clk, 40, lambda i: 840.0, lambda i: 1200, lambda i: 1200, lambda i: False)
    assert not g.loss_wait()
    assert not HeaterResponseGuard(enabled=False, clock=_Clock()).loss_wait()


def _sat2():
    clk = _Clock()
    ev = {"req": [], "msg": []}
    g = HeaterSaturationGuard(mv_limit=1200, mv_min=400, sat_sec=120, arrive_tol_c=1.0, clock=clk,
                              request_mv_limit=ev["req"].append, message=lambda lv, t: ev["msg"].append((lv, t)))
    return g, clk, ev


def _feed2(g, clk, secs, mv, defer, pv=850.0):
    for _ in range(int(secs)):
        g.tick({"ok": True, "run": True, "fault": False, "pv": pv, "sv": 850.0, "sv_ramp": 850.0, "mv": mv,
                "mv_limit": 1200}, False, "dac", final_target=850.0, defer_clamp=defer)
        clk.t += 1.0


def test_unit_saturation_defers_clamp_then_uses_same_pre_average():
    g, clk, ev = _sat2()
    _feed2(g, clk, 60, 1100, False)
    _feed2(g, clk, 125, 1200, True)
    assert g.clamped is None and not ev["req"]
    assert len([t for _lv, t in ev["msg"] if "하강 속도를 재는 중" in t]) == 1
    _feed2(g, clk, 30, 1200, True)
    assert g.clamped is None and len([t for _lv, t in ev["msg"] if "하강 속도를 재는 중" in t]) == 1
    _feed2(g, clk, 1, 1200, False)
    assert ev["req"] == [1100] and g.clamped == 1100


def test_unit_defer_ignored_when_clamp_disabled():
    g, clk, ev = _sat2()
    g.set_clamp_enabled(False)
    _feed2(g, clk, 60, 1100, False)
    _feed2(g, clk, 125, 1200, True)
    assert g.clamped is None and not ev["req"]
    assert [t for _lv, t in ev["msg"] if "클램프하지 않음" in t]
    assert not [t for _lv, t in ev["msg"] if "하강 속도를 재는 중" in t]


# ═════════════════════ 설정(tests/test_heater_config.py:19-139 · test_heater_response.py:166-180) ═════════════════════
def _load(**kv):
    return load_heater_cfg({f"CH2_{k}": v for k, v in kv.items()})


def test_cfg_defaults_without_keys():
    c, w = _load()
    assert w == [] and c.enabled is False and c == HeaterCfg()


def test_cfg_enabled_flag():
    assert _load(SUPPORTS_HEATER=True)[0].enabled is True
    assert _load(SUPPORTS_HEATER="on")[0].enabled is True and _load(SUPPORTS_HEATER="no")[0].enabled is False


@pytest.mark.parametrize("sv,ot,exp_sv,exp_ot", [(0, 550, 500.0, 550.0), (1000, 950, 900.0, 950.0), (500, 0, 500.0, 550.0),
                                                 (500, 1000, 500.0, 950.0), (600, 550, 600.0, 630.0), (500, 500, 500.0, 530.0)])
def test_cfg_temp_limits(sv, ot, exp_sv, exp_ot):
    c, w = _load(HEATER_SV_LIMIT_C=sv, HEATER_OT_LIMIT_C=ot)
    assert (c.sv_limit_c, c.ot_limit_c) == (exp_sv, exp_ot) and w


def test_cfg_temp_limits_at_ladder_max_no_warning():
    c, w = _load(HEATER_SV_LIMIT_C=900, HEATER_OT_LIMIT_C=950, HEATER_MAX_TEMP=900)
    assert w == [] and (c.sv_limit_c, c.ot_limit_c, c.max_temp) == (900.0, 950.0, 900.0)


def test_cfg_mv_rules():
    c, w = _load(HEATER_MV_ABS_MAX=2000, HEATER_MV_LIMIT=1800)
    assert c.mv_abs_max == 1600 and c.mv_limit == 1600 and w
    c, w = _load(HEATER_MV_LIMIT=300)
    assert c.mv_limit == 420 and w
    c, w = _load(HEATER_MV_MIN=350)
    assert any("래더 고정값 400" in x for x in w)


def test_cfg_ramp_holdback_rules():
    c, w = _load(HEATER_RAMP_RATE_C_PER_MIN=3, HEATER_SLOW_RATE_C_PER_MIN=1, HEATER_SLOW_ZONE_C=-5, HEATER_HOLDBACK_C=0)
    assert (c.ramp_rate, c.slow_rate, c.slow_zone_c, c.holdback_c) == (6.0, 6.0, 0.0, 10.0) and len(w) == 4


def test_cfg_pid_out_of_range_marked_unused():
    c, w = _load(HEATER_KP=60, HEATER_TI_S=5)
    assert c.kp_valid is False and c.ti_valid is False and any("쓰지 않음" in x for x in w)
    c, w = _load(HEATER_KP=0.1, HEATER_TI_S=3000)
    assert c.kp_valid and c.ti_valid and w == []


def test_cfg_max_temp_and_approach():
    assert _load(HEATER_MAX_TEMP=900)[0].max_temp == 500.0
    assert _load(HEATER_MAX_TEMP=0)[0].max_temp == 500.0
    c, _ = _load(HEATER_APPROACH_ZONE_C=-1, HEATER_APPROACH_MIN_RATE_C_PER_MIN=0, HEATER_APPROACH_LEAD_C=0.05)
    assert (c.approach_zone, c.approach_min_rate, c.approach_lead) == (0.0, 1.0, 0.2)
    assert _load(HEATER_APPROACH_MIN_RATE_C_PER_MIN=9)[0].approach_min_rate == 6.0


def test_cfg_hold_mode_tc2_becomes_dac():
    c, w = _load(HEATER_HOLD_MODE="tc2")
    assert c.hold_mode == "dac" and any("tc2" in x for x in w)
    c, w = _load(HEATER_HOLD_MODE="off")
    assert c.hold_mode == "off" and w == []


def test_cfg_hold_and_period_clamps():
    c, _ = _load(HEATER_HOLD_MV_ENTER_TOL_C=0.1, HEATER_HOLD_MV_ENTER_SEC=5, HEATER_HOLD_MV_ARRIVE_TOL_C=50,
                 HEATER_HOLD_MV_DRIFT_PV_C=0, HEATER_HOLD_MV_DRIFT_MV=500, HEATER_MV_SAT_SEC=1)
    assert (c.hold_enter_tol, c.hold_enter_sec, c.hold_arrive_tol, c.hold_drift_pv, c.hold_drift_mv, c.sat_sec) == \
        (0.5, 10.0, 10.0, 0.1, 200.0, 10.0)
    c, _ = _load(HEATER_POLL_INTERVAL_S=0.1, HEATER_POLL_IDLE_S=30, HEATER_WD_PERIOD_S=20, HEATER_STALE_SEC=1,
                 HEATER_LOG_PERIOD_S=0)
    assert (c.poll_interval_s, c.poll_idle_s, c.wd_period_s, c.stale_sec, c.log_period_s) == (0.5, 10.0, 9.0, 20.0, 1.0)
    assert _load(HEATER_POLL_INTERVAL_S=2, HEATER_POLL_IDLE_S=1)[0].poll_idle_s == 2.0


@pytest.mark.parametrize("val,expect,warned", [(None, 10.0, False), (0, 0.0, False), (1, 2.0, True),
                                                (30, 30.0, False), (1000, 600.0, True), (-5, 0.0, True)])
def test_cfg_run_log_period(val, expect, warned):
    c, w = _load() if val is None else _load(HEATER_RUN_LOG_PERIOD_S=val)
    assert c.run_log_period_s == expect and bool([x for x in w if "HEATER_RUN_LOG_PERIOD_S" in x]) is warned


@pytest.mark.parametrize("reg,expect", [(120, 0), (149, 0), (110, 0), (160, 160), (155, 155), (0, 0), (300, 0)])
def test_cfg_marker_register_overlap(reg, expect):
    c, w = _load(HEATER_SESSION_MARK_REG=reg)
    assert c.session_mark_reg == expect
    if reg not in (0, 155, 160):
        assert w


def test_cfg_noresp_defaults_and_clamps():
    c, w = load_heater_cfg({})
    assert (c.noresp_enable, c.noresp_mv, c.noresp_sec, c.noresp_rise_c) == (True, 600, 300.0, 5.0)
    assert (c.noresp_loss_sec, c.noresp_loss_below_c, c.noresp_loss_fall_c) == (180.0, 10.0, 10.0)
    c, w = _load(HEATER_NORESP_ENABLE="false", HEATER_NORESP_MV=300, HEATER_NORESP_SEC=5, HEATER_NORESP_RISE_C=0,
                 HEATER_NORESP_LOSS_SEC=9999, HEATER_NORESP_LOSS_BELOW_C=1, HEATER_NORESP_LOSS_FALL_C=0)
    assert c.noresp_enable is False
    assert (c.noresp_mv, c.noresp_sec, c.noresp_rise_c) == (450, 60.0, 1.0)
    assert (c.noresp_loss_sec, c.noresp_loss_below_c, c.noresp_loss_fall_c) == (1800.0, 2.0, 0.5)
    assert len([x for x in w if "HEATER_NORESP_" in x]) == 6


def test_cfg_v2_param_defaults_are_v1_runtime_values():
    """v2 파라미터 기본값(params/core_heater.py) = v1 ch2 운전값(HEAT-083) — 그대로 읽으면 경고 없음."""
    from v2.common.paramdef import defaults
    from v2.ch12.params import core_heater as PH
    from v2.ch12.params import outputs as PO
    d = dict(defaults(PH.PARAMS))
    d.update({p.key: p.default for p in PO.PARAMS if p.key.startswith("CH2_HEATER_")})
    c, w = load_heater_cfg(d)
    assert w == []
    assert (c.enabled, c.max_temp, c.sv_limit_c, c.ot_limit_c, c.ramp_rate, c.mv_abs_max, c.mv_limit) == \
        (True, 900.0, 900.0, 950.0, 6.0, 1300, 1200)
    assert (c.hold_mode, c.wait_timeout_s, c.hold_fail_action, c.run_log_period_s, c.session_mark_reg) == \
        ("off", 5400.0, "force", 10.0, 160)
    assert (c.noresp_enable, c.noresp_mv, c.noresp_sec, c.noresp_loss_sec) == (True, 600, 300.0, 180.0)


def test_cfg_roundtrip_dict():
    c = HeaterCfg(enabled=True, mv_limit=700, hold_mode="off", log_enabled=False)
    assert load_heater_cfg(cfg_to_dict(c))[0] == c


def test_real_words():
    """tests/test_heater_config.py:123-132."""
    import struct
    assert float_to_words(3.0) == (0x0000, 0x4040) and float_to_words(300.0) == (0x0000, 0x4396)
    for v, bits in ((0.1, 0x3DCCCCCD), (50.0, 0x42480000), (10.0, 0x41200000), (3000.0, 0x453B8000)):
        lo, hi = float_to_words(v)
        assert (hi << 16) | lo == bits
        assert words_to_float(lo, hi) == struct.unpack(">f", struct.pack(">f", v))[0]


def test_helpers():
    c = HeaterCfg()
    assert c.c_to_raw(500.0) == 5000 and c.c_to_raw(-1) == 0
    assert c.rate_counts(12.0) == 2 and c.rate_counts(60.0) == 10 and c.rate_counts(1.0) == 1


# ═════════════════════ 대기 판정(tests/test_heater_process_sim.py:207 · test_heater_recipe_soak.py:194) ═════════════════════
def _wst(**kw):
    d = {"ok": True, "run": True, "fault": False, "stale": False, "itl": True, "pv": 200.0, "sv": 200.0, "sv_ramp": 200.0,
         "tc_ok": True, "da_ok": True, "water_ok": True, "vac_ok": True}
    d.update(kw)
    return d


def test_soak_waiter_failures_and_timeout_estimate():
    clk = _Clock()
    w = HeaterSoakWaiter(200.0, tol=2.0, soak_sec=60, timeout_s=600, clock=clk)
    for _ in range(59):
        assert w.tick(_wst(), False) is None
        clk.t += 1.0
    clk.t += 1.0
    r = w.tick(_wst(), False)
    assert r is not None and r[0] and r[2] is None
    w = HeaterSoakWaiter(200.0, tol=2.0, soak_sec=60, timeout_s=600, clock=clk)
    assert w.tick(_wst(), True) is None                               # 램프 중이면 도달 아님
    clk.t += 601.0
    r = w.tick(_wst(pv=150.0), False)
    assert r == (False, "히터 승온 시간 초과(10분) — PV 150.0 / 목표 200.0°C", "E831")
    w = HeaterSoakWaiter(200.0, tol=2.0, soak_sec=60, timeout_s=600, clock=clk)
    assert w.tick(_wst(fault=True), False) == (False, "히터 이상 발생", "E832")
    assert w.tick(_wst(run=False), False)[2] == "E830"
    w = HeaterSoakWaiter(200.0, tol=2.0, soak_sec=60, timeout_s=600, clock=clk)
    assert w.tick(_wst(stale=True), False) is None
    clk.t += 30.0
    assert w.tick(_wst(stale=True), False) is None
    clk.t += 31.0
    r = w.tick(_wst(stale=True), False)
    assert r[0] is False and r[2] == "E830" and "PLC 응답 없음" in r[1]
    t, why = heater_wait_timeout_s(25.0, 200.0, 6.0, base_timeout_s=3600, approach_zone=20, approach_min_rate=1, soak_sec=60)
    assert t == pytest.approx(max(3600.0, ramp_minutes(175, 6, 20, 1) * 60 + 60 + 1800)) and "예상 승온" in why
    t, why = heater_wait_timeout_s(None, 200.0, 6.0, base_timeout_s=5400, approach_zone=20, approach_min_rate=1, soak_sec=60)
    assert t == 5400 and "미상" in why
    t, why = heater_wait_timeout_s(25.0, 900.0, 0.1, base_timeout_s=5400, approach_zone=20, approach_min_rate=1, soak_sec=60)
    assert t == 43200.0 and "상한 12시간" in why
    t, why = heater_wait_timeout_s(300.0, 200.0, 6.0, base_timeout_s=600, approach_zone=20, approach_min_rate=1, soak_sec=0)
    assert t == 100 * 60 + 1800 and "자연 냉각" in why


def test_waiter_hold_counts_time_and_ignores_arrival_timeout():
    """SOAK 은 시간만 센다 — 도달 한도는 SOAK 중 보지 않고, 범위를 벗어났다 돌아와도 다시 세지 않는다(test_heater_recipe_soak.py:194-327)."""
    clk = _Clock()
    logs = []
    w = HeaterSoakWaiter(200.0, tol=2.0, soak_sec=60, timeout_s=120, hold_sec=600, clock=clk, log=logs.append)
    for _ in range(61):
        assert w.tick(_wst(), False) is None
        clk.t += 1.0
    assert w.hold_t0 > 0
    for i in range(300):
        assert w.tick(_wst(pv=205.0 if 100 <= i < 109 else 200.3), False) is None    # 한도 120 s 지나도 계속
        clk.t += 1.0
    assert any("SOAK 시간은 계속 셈" in m for m in logs)
    assert any("히터 PV 범위 복귀 — 200.3°C (9초 벗어났었음)" in m for m in logs)
    r = None
    while r is None:
        r = w.tick(_wst(pv=200.3), False)
        clk.t += 1.0
    assert r[0] and "SOAK 10분 끝" in r[1] and "±2°C 밖 9초" in r[1]
    w = HeaterSoakWaiter(200.0, tol=2.0, soak_sec=60, timeout_s=120, hold_sec=600, clock=clk)
    for _ in range(61):
        w.tick(_wst(), False)
        clk.t += 1.0
    assert w.tick(_wst(run=False), False)[2] == "E830"                               # SOAK 중에도 꺼짐은 실패

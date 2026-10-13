# -*- coding: utf-8 -*-
"""controller/heater_saturation.py — 도달 후 새 포화만 클램프 / 도달 전 승온 정체 / 유지 중 경고 없음(가짜 시계).
pytest / 직접 실행 모두 가능."""
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import pytest                                                   # noqa: E402
from controller.heater_saturation import HeaterSaturationGuard  # noqa: E402


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _mk():
    clk = _Clock()
    ev = {"req": [], "msg": [], "sat": [], "stall": []}
    g = HeaterSaturationGuard(mv_limit=520, mv_min=400, sat_sec=120, arrive_tol_c=1.0, clock=clk,
                              request_mv_limit=ev["req"].append,
                              message=lambda lv, t: ev["msg"].append((lv, t)),
                              saturated=ev["sat"].append, stalled=ev["stall"].append)
    return g, clk, ev


def _st(mv, pv=300.0, sv=300.0, svr=None, **kw):
    d = {"ok": True, "run": True, "fault": False, "pv": pv, "sv": sv,
         "sv_ramp": sv if svr is None else svr, "mv": mv, "mv_limit": 520}
    d.update(kw)
    return d


def _feed(g, clk, secs, mv, hold=False, final=None, ramp=False, **kw):
    for _ in range(int(secs)):
        st = _st(mv, **kw)
        g.tick(st, hold, "dac", final_target=(final if final is not None else st["sv"]), ramp_active=ramp)
        clk.t += 1.0


def _msgs(ev, word):
    return [t for _lv, t in ev["msg"] if word in t]


# ───────────────────────── 도달 후 포화 ─────────────────────────
def test_post_arrival_clamp_to_pre_average_once():
    g, clk, ev = _mk()
    _feed(g, clk, 60, 480)
    assert g.arrived
    _feed(g, clk, 122, 515)
    assert ev["req"] == [480] and g.clamped == 480
    assert len(ev["sat"]) == 1 and ev["sat"][0]["clamp"] == 480
    assert _msgs(ev, "도달 후")
    _feed(g, clk, 300, 515)
    assert ev["req"] == [480] and len(ev["sat"]) == 1


def test_release_restores_limit():
    for kill in ("run", "fault", "sv"):
        g, clk, ev = _mk()
        _feed(g, clk, 60, 480)
        _feed(g, clk, 122, 515)
        assert ev["req"] == [480]
        ev["req"].clear()
        if kill == "run":
            g.tick(_st(515, run=False), False, "dac", final_target=300.0)
        elif kill == "fault":
            g.tick(_st(515, fault=True), False, "dac", final_target=300.0)
        else:
            g.tick(_st(515, sv=320.0), False, "dac", final_target=320.0)
        assert ev["req"] == [520], kill
        assert g.clamped is None


def test_judged_on_readback_limit():
    g, clk, ev = _mk()
    _feed(g, clk, 60, 450, mv_limit=470)
    _feed(g, clk, 122, 470, mv_limit=470)                         # 실제 상한 470 에 물림
    assert len(ev["sat"]) == 1 and ev["sat"][0]["limit"] == 470 and ev["req"] == [450]


def test_arrival_at_limit_is_not_saturation():
    g, clk, ev = _mk()
    _feed(g, clk, 600, 515)                                       # 도달 시점부터 상한 — 무장 안 됨
    assert g.arrived and ev["req"] == [] and ev["sat"] == []
    assert not _msgs(ev, "도달 후")


def test_pre_average_also_at_limit_no_clamp_warn_once():
    g, clk, ev = _mk()
    _feed(g, clk, 60, 505)                                        # 상한 520 기준 아래(무장)
    _feed(g, clk, 300, 510, mv_limit=510)                         # 되읽은 상한이 510 으로 내려가 물림
    assert ev["req"] == [] and ev["sat"] == []
    assert len(_msgs(ev, "클램프하지 않음")) == 1                 # 같은 에피소드에 다시 알리지 않음


def test_short_pre_history_no_clamp():
    g, clk, ev = _mk()
    _feed(g, clk, 10, 480)                                        # 직전 이력 10초 < 30초
    _feed(g, clk, 130, 515)
    assert ev["req"] == [] and ev["sat"] == []
    assert len(_msgs(ev, "이력 부족")) == 1


def test_saturation_right_after_ramp_uses_ramp_history():
    g, clk, ev = _mk()
    for i in range(60):                                           # 파이썬 램프 중: SV 가 매초 바뀜
        sv = 240.0 + i
        g.tick(_st(480, pv=sv - 1.0, sv=sv), False, "dac", final_target=300.0, ramp_active=True)
        clk.t += 1.0
    assert not g.arrived
    _feed(g, clk, 1, 480)                                         # 램프 끝 · 도달 · 무장
    _feed(g, clk, 122, 515)
    assert ev["req"] == [480]                                     # 램프 끝 이력으로 클램프


# ───────────────────────── 도달 판정 ─────────────────────────
def test_not_arrived_while_ramp_or_ladder_ramp_behind():
    g, clk, ev = _mk()
    _feed(g, clk, 30, 480, ramp=True)
    assert not g.arrived
    _feed(g, clk, 30, 480, svr=299.0)                             # D119 ≠ SV
    assert not g.arrived
    _feed(g, clk, 1, 480)
    assert g.arrived


def test_target_change_resets_arrival():
    g, clk, ev = _mk()
    _feed(g, clk, 10, 480)
    assert g.arrived
    _feed(g, clk, 1, 480, sv=320.0)                               # 새 목표, PV 아직 300
    assert not g.arrived


def test_before_arrival_never_clamps():
    g, clk, ev = _mk()
    _feed(g, clk, 900, 520, pv=250.0)                             # SV 고정·MV 상한·미도달(승온 중)
    assert ev["req"] == [] and ev["sat"] == []
    assert not _msgs(ev, "DAC 출력 상한")


# ───────────────────────── 승온 정체 ─────────────────────────
def test_stall_once_no_clamp():
    g, clk, ev = _mk()
    _feed(g, clk, 301, 520, pv=250.0)
    assert len(ev["stall"]) == 1 and ev["req"] == []
    s = ev["stall"][0]
    assert s["target"] == 300.0 and s["limit"] == 520 and s["eta_min"] is None
    assert len(_msgs(ev, "승온 정체")) == 1
    _feed(g, clk, 900, 520, pv=250.0)
    assert len(ev["stall"]) == 1


def test_no_stall_below_limit_or_short_eta():
    g, clk, ev = _mk()
    _feed(g, clk, 600, 480, pv=250.0)                             # 상한 아님
    assert ev["stall"] == []
    g, clk, ev = _mk()
    for i in range(600):                                          # 0.4°C/min 이지만 남은 거리 짧음(ETA < 60분)
        pv = 289.0 + 0.4 * i / 60.0
        g.tick(_st(520, pv=pv), False, "dac", final_target=300.0)
        clk.t += 1.0
    assert ev["stall"] == []


def test_stall_rejudged_after_target_change():
    g, clk, ev = _mk()
    _feed(g, clk, 301, 520, pv=250.0)
    assert len(ev["stall"]) == 1
    _feed(g, clk, 301, 520, pv=250.0, sv=310.0)
    assert len(ev["stall"]) == 2


# ───────────────────────── 유지 중 ─────────────────────────
def test_dac_hold_no_warning_no_clamp():
    g, clk, ev = _mk()
    _feed(g, clk, 600, 515, hold=True, mv_limit=515)
    assert ev["req"] == [] and ev["sat"] == [] and ev["msg"] == []


def test_dac_hold_low_pv_info_once():
    g, clk, ev = _mk()
    _feed(g, clk, 400, 480, hold=True, pv=280.0, mv_limit=480)
    infos = _msgs(ev, "유지 중 PV")
    assert len(infos) == 1 and all(lv == "정보" for lv, t in ev["msg"])
    assert ev["req"] == [] and ev["sat"] == []


def test_nothing_below_limit():
    g, clk, ev = _mk()
    _feed(g, clk, 600, 500)
    assert ev == {"req": [], "msg": [], "sat": [], "stall": []}


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

# -*- coding: utf-8 -*-
"""controller/heater_saturation.py — DAC 포화 감시(가짜 시계). pytest / 직접 실행 모두 가능."""
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
    ev = {"req": [], "msg": [], "sat": []}
    g = HeaterSaturationGuard(mv_limit=520, mv_min=400, sat_sec=120, clock=clk,
                              request_mv_limit=ev["req"].append,
                              message=lambda lv, t: ev["msg"].append((lv, t)),
                              saturated=ev["sat"].append)
    return g, clk, ev


def _st(mv, **kw):
    d = {"run": True, "fault": False, "pv": 300.0, "sv": 300.0, "mv": mv, "mv_limit": 520}
    d.update(kw)
    return d


def _feed(g, clk, secs, mv, hold=False, **kw):
    for _ in range(int(secs)):
        g.tick(_st(mv, **kw), hold, "dac")
        clk.t += 1.0


def test_clamp_to_pre_saturation_average():
    g, clk, ev = _mk()
    _feed(g, clk, 60, 480)
    _feed(g, clk, 122, 515)                                       # ≥ 0.98×520 이 120초 연속
    assert ev["req"] == [480] and g.clamped == 480
    assert len(ev["sat"]) == 1 and ev["sat"][0]["clamp"] == 480
    _feed(g, clk, 30, 515)                                        # 에피소드당 1회
    assert ev["req"] == [480]


def test_clamp_without_history_uses_90pct():
    g, clk, ev = _mk()
    _feed(g, clk, 122, 515)
    assert ev["req"] == [468]                                     # 520×0.9


def test_release_restores_limit():
    for kill in ("run", "fault", "sv"):
        g, clk, ev = _mk()
        _feed(g, clk, 60, 480)
        _feed(g, clk, 122, 515)
        ev["req"].clear()
        if kill == "run":
            g.tick(_st(515, run=False), False, "dac")
        elif kill == "fault":
            g.tick(_st(515, fault=True), False, "dac")
        else:
            g.tick(_st(515, sv=320.0), False, "dac")
        assert ev["req"] == [520], kill
        assert g.clamped is None


def test_hold_dac_warns_only_once():
    g, clk, ev = _mk()
    _feed(g, clk, 200, 515, hold=True)
    assert ev["req"] == []                                        # 클램프하지 않는다(유지 모드 소유)
    assert len(ev["sat"]) == 1 and ev["sat"][0]["clamp"] is None


def test_no_saturation_below_limit():
    g, clk, ev = _mk()
    _feed(g, clk, 300, 500)
    assert ev["req"] == [] and ev["sat"] == []


def test_actual_limit_uses_readback():
    g, clk, ev = _mk()
    _feed(g, clk, 122, 470, mv_limit=470)                         # 실제 상한 470 에 물려 있음
    assert len(ev["sat"]) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

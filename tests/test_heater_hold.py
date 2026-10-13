# -*- coding: utf-8 -*-
"""controller/heater_hold.py — dac 유지 모드(가짜 시계). pytest / 직접 실행 모두 가능."""
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import pytest                                                   # noqa: E402
from controller.heater_hold import HeaterHold                   # noqa: E402


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _mk(mode="dac"):
    clk = _Clock()
    ev = {"req": [], "msg": [], "eng": [], "give": []}
    h = HeaterHold(mode, mv_limit=520, mv_min=400, enter_tol_c=3.0, enter_sec=60.0, arrive_tol_c=1.0,
                   drift_pv_c=0.5, drift_mv=15.0, clock=clk,
                   request_mv_limit=ev["req"].append, message=lambda lv, t: ev["msg"].append((lv, t)),
                   engaged=ev["eng"].append, give_up=ev["give"].append)
    return h, clk, ev


def _st(pv=300.0, mv=470, **kw):
    d = {"ok": True, "run": True, "fault": False, "pv": pv, "sv": 300.0, "sv_ramp": 300.0,
         "mv": mv, "mv_limit": 520}
    d.update(kw)
    return d


def _feed(h, clk, secs, fn):
    for i in range(int(secs)):
        h.tick(fn(i), 300.0)
        clk.t += 1.0


def test_capture_after_enter_window():
    h, clk, ev = _mk()
    _feed(h, clk, 61, lambda i: _st())
    assert h.is_holding() and h.value == 470
    assert ev["req"] == [470] and ev["eng"] == ["dac"]
    assert h.arrived is True


def test_needs_arrival_latch():
    h, clk, ev = _mk()
    _feed(h, clk, 80, lambda i: _st(pv=302.0))                    # |PV−final| 2 > ARRIVE 1 → 래치 안 됨
    assert not h.is_holding() and not h.arrived and ev["req"] == []


def test_drift_remeasures():
    h, clk, ev = _mk()
    # 창 전반 450 / 후반 490 → MV 드리프트 40 > max(15, 26) → 재측정
    _feed(h, clk, 61, lambda i: _st(mv=450 if i < 31 else 490))
    assert not h.is_holding() and ev["req"] == []
    assert any("재측정" in t for _lv, t in ev["msg"])
    assert h.last_window_mv is not None
    # 이후 안정되면 다시 61초 뒤 캡처
    _feed(h, clk, 62, lambda i: _st(mv=480))
    assert h.is_holding() and ev["req"] == [480]


def test_give_up_near_limit():
    h, clk, ev = _mk()
    _feed(h, clk, 61, lambda i: _st(mv=515))                      # ≥ 0.98×520
    assert not h.is_holding() and ev["req"] == []
    assert len(ev["give"]) == 1 and h.gave_up
    _feed(h, clk, 70, lambda i: _st(mv=515))                      # 경고·give_up 은 1회
    assert len(ev["give"]) == 1


def test_floor_not_captured():
    h, clk, ev = _mk()
    _feed(h, clk, 61, lambda i: _st(mv=410))                      # ≤ MV_MIN+20
    assert not h.is_holding() and ev["req"] == []
    assert any("최소치" in t for _lv, t in ev["msg"])


def test_release_restores_on_run_off_fault_target_change():
    for kill in ("run", "fault", "target"):
        h, clk, ev = _mk()
        _feed(h, clk, 61, lambda i: _st())
        assert h.is_holding()
        ev["req"].clear()
        if kill == "run":
            h.tick(_st(run=False), 300.0)
        elif kill == "fault":
            h.tick(_st(fault=True), 300.0)
        else:
            h.tick(_st(sv=310.0), 310.0)
        assert not h.is_holding(), kill
        assert ev["req"] == [520], kill


def test_reapply_when_reverted():
    h, clk, ev = _mk()
    _feed(h, clk, 61, lambda i: _st())
    ev["req"].clear()
    h.tick(_st(mv_limit=520), 300.0)                              # 되돌아감 — 아직 5초 안 지남
    assert ev["req"] == []
    clk.t += 5.0
    h.tick(_st(mv_limit=520), 300.0)
    assert ev["req"] == [470]
    h.tick(_st(mv_limit=470), 300.0)                              # 맞으면 그대로
    assert ev["req"] == [470]


def test_mode_off_does_nothing():
    h, clk, ev = _mk("off")
    _feed(h, clk, 80, lambda i: _st())
    assert not h.is_holding() and ev["req"] == []


def test_force_dac_hold():
    h, clk, ev = _mk()
    assert h.force_dac_hold(None) is False                         # 값 없음
    assert h.force_dac_hold(1000) is True and h.value == 520 and ev["req"] == [520]   # 클램프


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

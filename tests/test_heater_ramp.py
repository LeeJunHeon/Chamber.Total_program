# -*- coding: utf-8 -*-
"""controller/heater_ramp.py — RampProfiler(가짜 상태·가짜 시계). pytest / 직접 실행 모두 가능."""
import os
import sys
import asyncio

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import pytest                                                   # noqa: E402
from controller.heater_ramp import RampProfiler                 # noqa: E402
from lib.heater_config import HeaterConfig                      # noqa: E402


class _Clock:
    def __init__(self):
        self.t = 100.0

    def __call__(self):
        return self.t


def _mk(st, **cfg_kw):
    writes, logs = [], []
    clk = _Clock()

    async def _sv(v):
        writes.append(("sv", v))

    async def _ramp(c):
        writes.append(("ramp", c))
    cfg = HeaterConfig(**cfg_kw)
    p = RampProfiler(lambda: st, _sv, _ramp, logs.append, cfg, clock=clk)
    return p, writes, logs, clk


def _run(c):
    return asyncio.run(c)


def test_start_writes_ramp_then_sv_base():
    st = {"run": False, "pv": 100.0, "sv_ramp": 99.0}
    p, w, _, _ = _mk(st)
    assert _run(p.start(200.0, 12.0, "수동")) is True
    assert w == [("ramp", 3), ("sv", 100.0)]                    # max(2, ceil(12/6)+1)=3, 출발점 = PV(정지 중)
    assert p.is_active() and p.target() == 200.0 and p.rate() == 12.0


def test_start_from_sv_ramp_when_running():
    st = {"run": True, "pv": 150.0, "sv_ramp": 160.0}
    p, w, _, _ = _mk(st)
    assert _run(p.start(200.0, 30.0))
    assert w == [("ramp", 6), ("sv", 160.0)]                    # ceil(30/6)+1 = 6


def test_target_not_above_base_returns_false():
    st = {"run": False, "pv": 100.0, "sv_ramp": 100.0}
    p, w, _, _ = _mk(st)
    assert _run(p.start(100.05, 12.0)) is False
    assert _run(p.start(80.0, 12.0)) is False
    assert w == [] and not p.is_active()


def test_tick_rate_and_lead_limit():
    st = {"run": True, "pv": 100.0, "sv_ramp": 100.0}
    p, w, _, clk = _mk(st)
    _run(p.start(300.0, 12.0))
    w.clear()
    clk.t += 1.0
    _run(p.tick())                                               # 12°C/min × 1초 = +0.2
    assert w == [("sv", 100.2)]
    # 앞섬 한계: 속도가 커도 D119 + LEAD 까지만
    p2, w2, _, clk2 = _mk(dict(st), approach_lead=1.0)
    _run(p2.start(300.0, 120.0))
    w2.clear()
    clk2.t += 3.0
    _run(p2.tick())                                              # 120×3/60 = 6 → LEAD 1 로 제한
    assert w2 == [("sv", 101.0)]


def test_dt_capped_at_5s():
    st = {"run": True, "pv": 100.0, "sv_ramp": 100.0}
    p, w, _, clk = _mk(st, approach_lead=50.0)
    _run(p.start(300.0, 12.0))
    w.clear()
    clk.t += 100.0
    _run(p.tick())
    assert w == [("sv", 101.0)]                                  # 5초분(+1.0)만


def test_approach_decelerates_near_target():
    st = {"run": True, "pv": 195.0, "sv_ramp": 195.0}
    p, w, _, clk = _mk(st)
    _run(p.start(200.0, 12.0))
    w.clear()
    clk.t += 5.0
    _run(p.tick())                                               # 남은 5°C → 1+11×5/20 = 3.75°C/min → +0.3125
    assert w == [("sv", 195.3)]
    far = {"run": True, "pv": 100.0, "sv_ramp": 100.0}
    pf, wf, _, clkf = _mk(far)
    _run(pf.start(200.0, 12.0))
    wf.clear()
    clkf.t += 5.0
    _run(pf.tick())                                              # 접근 구간 밖 → +1.0
    assert wf == [("sv", 101.0)]


def test_completion():
    st = {"run": True, "pv": 199.9, "sv_ramp": 199.9}
    p, w, logs, clk = _mk(st, approach_lead=5.0)
    _run(p.start(200.0, 12.0))
    for _ in range(40):
        clk.t += 5.0
        _run(p.tick())
        if not p.is_active():
            break
    assert w[-1] == ("sv", 200.0)
    assert not p.is_active() and p.target() is None
    assert any("램프 완료" in m for m in logs)


def test_completion_waits_for_ladder():
    st = {"run": True, "pv": 199.0, "sv_ramp": 199.0}
    p, _, _, clk = _mk(st, approach_lead=5.0)
    _run(p.start(200.0, 600.0))
    clk.t += 5.0
    _run(p.tick())                                               # SV 는 목표지만 D119 가 아직 199
    assert p.is_active()
    st["sv_ramp"] = 199.9
    clk.t += 1.0
    _run(p.tick())
    assert not p.is_active()


def test_stop_restores_ramp_rate():
    st = {"run": True, "pv": 100.0, "sv_ramp": 100.0}
    p, w, _, _ = _mk(st)
    _run(p.start(300.0, 12.0))
    w.clear()
    _run(p.stop(restore_rate=True))
    assert w == [("ramp", 2)]                                    # 12/6
    w.clear()
    _run(p.start(300.0, 12.0)); w.clear()
    _run(p.stop(restore_rate=False))
    assert w == []
    _run(p.stop(restore_rate=True))                              # 이미 꺼짐 → 쓰지 않음
    assert w == []


def test_write_failure_falls_back():
    st = {"run": False, "pv": 100.0, "sv_ramp": 100.0}

    async def _boom(_v):
        raise RuntimeError("link")
    p = RampProfiler(lambda: st, _boom, _boom, lambda m: None, HeaterConfig())
    assert asyncio.run(p.start(200.0, 12.0)) is False and not p.is_active()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

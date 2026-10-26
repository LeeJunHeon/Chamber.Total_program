# -*- coding: utf-8 -*-
"""히터 공정 대기 판정·감시 코드에서 예상 못 한 예외가 나면 공정을 한 번 실패시킨다 — runtime/chamber_runtime.py.

예전: 대기 판정(HeaterSoakWaiter·HeaterHoldWaiter) 안에서 예외가 나면 히터 상태 리스너가 예외를 삼켜(로그만)
공정은 그 단계를 끝없이 기다렸다 — 히터는 켜진 채, 폴링마다 같은 예외 로그 1줄(SOAK 중 주입: 4시간 대기·14,400줄).
지금: 대기·감시를 끝내고 공정을 실패시킨다 — 대기(도달·유지) 중이면 E830, 공정 중 감시면 E832, 예외는 traceback 과 함께 1번만 기록.
(히터 정리는 공정 종료 후 러너가 한다.) 예외를 일부러 넣어 확인한다 — 알려진 예외 경로는 없다(방어용).
래더·열 에뮬레이터(tests/heater_ladder_sim.py), 장치 접근 없음.
"""
import os
import sys
import asyncio
import contextlib

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import heater_ladder_sim as sim                                  # noqa: E402
import runtime.chamber_runtime as cr                             # noqa: E402
from controller.heater_process import HeaterSoakWaiter, HeaterHoldWaiter   # noqa: E402


class _PCRec:
    """공정 컨트롤러 대신 on_heater_* 호출만 기록."""

    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        if name.startswith("on_heater"):
            return lambda *a, **k: self.calls.append((name, a, k))
        raise AttributeError(name)

    def names(self):
        return [c[0] for c in self.calls]

    def fails(self):
        return [(str(c[1][0]), c[2].get("code")) for c in self.calls if c[0] == "on_heater_failed"]


def _rt():
    rt = cr.ChamberRuntime.__new__(cr.ChamberRuntime)
    rt.ch = 2
    rt.logs = []
    rt.append_log = lambda s, m: rt.logs.append((s, m))
    rt._get_text = lambda name: ""
    rt.supports_rf_pulse = rt.supports_dc_pulse = rt.supports_dc_cont = rt.supports_rf_cont = True
    rt.supports_dc_cont2 = False
    rt._loop = asyncio.get_running_loop()
    rt._ensure_task_alive = lambda *a, **k: None
    rt._heater = None
    rt._heater_gen = 0
    rt._heater_job = None
    rt._heater_watch = False
    rt._heater_rate = None
    rt._runner_queue_mode = True
    rt.process_queue = []
    rt.current_process_index = -1
    rt._bind_process_controller()
    pc = rt.process_controller
    rec = _PCRec()
    rt.process_controller = rec
    return rt, pc, rec


async def _tick(n=20):
    for _ in range(n):
        await asyncio.sleep(0)


async def _until(svc, plc, clk, cond, limit_s):
    t0 = clk.t
    while not cond():
        await sim.run(svc, plc, clk, 1)
        await _tick(3)
        if clk.t - t0 >= limit_s:
            return False
    return True


async def _start(target=200.0, soak_s=None, **cfg):
    """공정처럼 HEATER_SET → HEATER_WAIT."""
    rt, pc, rec = _rt()
    svc, plc, clk, logs, chat = sim.make(mv_limit=700, **cfg)
    rt.attach_heater(svc)
    await sim.run(svc, plc, clk, 5)
    pc._heater_set(float(target), None)
    await _tick()
    await sim.run(svc, plc, clk, 2)
    assert rec.names() == ["on_heater_set_ok"], rec.calls
    if soak_s is None:
        pc._heater_wait(float(target))
    else:
        pc._heater_wait(float(target), float(soak_s))
    return rt, pc, rec, svc, plc, clk, logs


def _boom(*_a, **_k):
    raise RuntimeError("injected judge bug")


def _judge_err_logs(rt):
    return [m for s, m in rt.logs if "히터 공정 판정 예외" in m]


def test_exception_during_soak_fails_once_e830(monkeypatch):
    async def go():
        rt, pc, rec, svc, plc, clk, logs = await _start(200.0, 600.0)
        ok = await _until(svc, plc, clk, lambda: bool([m for s, m in rt.logs if "SOAK 10분 시작" in m]), 3 * 3600)
        assert ok and not rec.fails()
        monkeypatch.setattr(HeaterSoakWaiter, "_tick_hold", _boom)
        ok = await _until(svc, plc, clk, lambda: bool(rec.fails()), 30)
        assert ok, rt.logs[-5:]                                      # 예전: 끝없이 기다림(폴링마다 예외 로그)
        await sim.run(svc, plc, clk, 60)
        await _tick()
        assert rec.fails() == [(rec.fails()[0][0], "E830")] and "도달 대기 판정 중 프로그램 오류" in rec.fails()[0][0]
        assert rt._heater_job is None and not rt._heater_watch
        assert len(_judge_err_logs(rt)) == 1 and "Traceback" in _judge_err_logs(rt)[0]
        assert not [m for _t, m in logs if "상태 리스너 예외" in m]
    asyncio.run(go())


def test_exception_before_reach_fails_once_e830(monkeypatch):
    async def go():
        monkeypatch.setattr(HeaterSoakWaiter, "tick", _boom)
        rt, pc, rec, svc, plc, clk, logs = await _start(200.0)
        ok = await _until(svc, plc, clk, lambda: bool(rec.fails()), 30)
        assert ok
        await sim.run(svc, plc, clk, 30)
        await _tick()
        assert [c for _m, c in rec.fails()] == ["E830"] and len(_judge_err_logs(rt)) == 1
    asyncio.run(go())


def test_exception_in_hold_wait_fails_once_e830(monkeypatch):
    async def go():
        rt, pc, rec, svc, plc, clk, logs = await _start(150.0, hold_mode="dac")
        ok = await _until(svc, plc, clk, lambda: "on_heater_ready" in rec.names(), 3 * 3600)
        assert ok
        monkeypatch.setattr(HeaterHoldWaiter, "tick", _boom)
        pc._heater_hold()
        await _tick()
        ok = await _until(svc, plc, clk, lambda: bool(rec.fails()), 30)
        assert ok
        assert rec.fails()[0][1] == "E830" and "유지 대기 판정 중 프로그램 오류" in rec.fails()[0][0]
        assert len(rec.fails()) == 1 and rt._heater_job is None and not rt._heater_watch
    asyncio.run(go())


def test_exception_in_watch_fails_once_e832(monkeypatch):
    async def go():
        rt, pc, rec, svc, plc, clk, logs = await _start(150.0)
        ok = await _until(svc, plc, clk, lambda: "on_heater_ready" in rec.names(), 3 * 3600)
        assert ok and rt._heater_watch
        monkeypatch.setattr(cr, "itl_missing_text", _boom)      # 감시가 꺼짐 사유를 만들 때 예외
        plc.vac_ok = False                                       # 인터락 해제 → 래더가 RUN 을 끔
        ok = await _until(svc, plc, clk, lambda: bool(rec.fails()), 30)
        assert ok
        assert rec.fails()[0][1] == "E832" and "감시 중 프로그램 오류" in rec.fails()[0][0]
        await sim.run(svc, plc, clk, 10)
        await _tick()
        assert len(rec.fails()) == 1 and not rt._heater_watch and len(_judge_err_logs(rt)) == 1
    asyncio.run(go())


def test_normal_soak_unchanged():
    """예외가 없으면 예전 그대로 — SOAK 뒤 준비 완료(on_heater_ready), 감시 계속."""
    async def go():
        rt, pc, rec, svc, plc, clk, logs = await _start(200.0, 120.0)
        ok = await _until(svc, plc, clk, lambda: "on_heater_ready" in rec.names(), 3 * 3600)
        assert ok and not rec.fails() and rt._heater_watch and not _judge_err_logs(rt)
    asyncio.run(go())

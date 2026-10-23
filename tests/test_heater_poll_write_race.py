# -*- coding: utf-8 -*-
"""히터 폴링이 PLC 를 읽는 동안(잠금 밖) 켜기·끄기 같은 쓰기가 끼면, 그 폴링의 값(쓰기 전 상태)을 확정·발행하지 않는다.

예전: 공정이 꺼진 히터를 켜는 순간 폴링이 먼저 읽어 둔 RUN=0 이 감시(_heater_watch)가 켜진 뒤 발행돼
'공정 중 히터가 꺼짐' E832 로 공정이 멈췄다(에뮬레이터 기준 약 3%). 패널 끄기 직후에도 옛 RUN=1 이 한 번 발행됐다.
워치독(D114) 갱신은 상태를 바꾸지 않으므로 폴링을 버리지 않는다. 래더·열 에뮬레이터(tests/heater_ladder_sim.py), 장치 접근 없음.
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
from runtime.chamber_runtime import ChamberRuntime               # noqa: E402


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


def _rt():
    rt = ChamberRuntime.__new__(ChamberRuntime)
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


def _gate_coil_read(plc):
    """다음 코일 읽기 하나를 '읽은 순간의 값'으로 붙잡아 두었다가 gate 가 열리면 돌려준다(느린 PLC 응답 흉내)."""
    orig = plc.read_coils_block
    gate, entered = asyncio.Event(), asyncio.Event()
    state = {"armed": True}

    async def slow(start, count, *, priority="high"):
        if not state["armed"]:
            return await orig(start, count, priority=priority)
        state["armed"] = False
        snap = await orig(start, count, priority=priority)      # 읽는 순간의 값(쓰기 전)
        entered.set()
        await gate.wait()
        return snap
    plc.read_coils_block = slow
    return gate, entered


def test_process_turn_on_during_poll_read_no_false_e832():
    async def go():
        rt, pc, rec = _rt()
        svc, plc, clk, logs, chat = sim.make(mv_limit=700)
        rt.attach_heater(svc)
        await sim.run(svc, plc, clk, 5)
        published = []
        svc.add_listener(lambda st: published.append(bool(st.get("run"))))
        gate, entered = _gate_coil_read(plc)
        poll = asyncio.create_task(svc._poll_once())
        await entered.wait()                                     # 폴링이 RUN=0 을 읽고 응답 대기 중
        pc._heater_set(200.0, None)                              # 공정이 히터를 켬(RUN=1) → 감시 시작
        await _tick()
        assert rec.names() == ["on_heater_set_ok"] and rt._heater_watch and plc.M[640]
        gate.set()
        await poll
        await _tick()
        assert published == [] and rec.names() == ["on_heater_set_ok"], (published, rec.calls)   # 예전: run=False 발행 → E832
        await sim.run(svc, plc, clk, 3)
        await _tick()
        assert svc.status()["run"] and published[-1] is True and rec.names() == ["on_heater_set_ok"]
    asyncio.run(go())


def test_panel_turn_off_during_poll_read_no_stale_run():
    async def go():
        svc, plc, clk, logs, chat = sim.make(mv_limit=700)
        await sim.run(svc, plc, clk, 5)
        ok, why = await svc.turn_on(150.0, owner="manual")
        assert ok, why
        await sim.run(svc, plc, clk, 5)
        published = []
        svc.add_listener(lambda st: published.append(bool(st.get("run"))))
        gate, entered = _gate_coil_read(plc)
        poll = asyncio.create_task(svc._poll_once())
        await entered.wait()                                     # RUN=1 을 읽은 채 대기
        ok, why = await svc.turn_off("정지(패널)", owner="manual")
        assert ok, why
        gate.set()
        await poll
        assert published == []                                    # 예전: 끈 직후 옛 RUN=1 발행
        await sim.run(svc, plc, clk, 3)
        assert not svc.status()["run"] and published and not any(published)
    asyncio.run(go())


def test_watchdog_write_during_read_does_not_drop_poll():
    async def go():
        svc, plc, clk, logs, chat = sim.make(mv_limit=700)
        await sim.run(svc, plc, clk, 5)
        ok, why = await svc.turn_on(150.0, owner="manual")
        assert ok, why
        await sim.run(svc, plc, clk, 5)
        published = []
        svc.add_listener(lambda st: published.append(bool(st.get("run"))))
        gate, entered = _gate_coil_read(plc)
        poll = asyncio.create_task(svc._poll_once())
        await entered.wait()
        await svc._kick_wd()                                      # 워치독 갱신(D114)은 상태를 바꾸지 않는다
        gate.set()
        await poll
        assert published == [True]
    asyncio.run(go())

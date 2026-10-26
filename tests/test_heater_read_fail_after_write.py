# -*- coding: utf-8 -*-
"""켜기·끄기 같은 쓰기 직후 상태 읽기가 실패하면 쓰기 전 상태를 다시 발행하지 않는다 — controller/heater_service.py _on_fail.

예전: 공정이 히터를 켠 직후(RUN=1 쓰기) 다음 폴링의 읽기가 한 번 실패하면, 켜기 전에 확정된 상태(RUN=0)가 stale 아님
(age 1초)으로 다시 발행돼 도달 대기(HEATER_WAIT)는 E830, 공정 중 감시는 E832 로 멈췄다(히터는 실제로 켜져 있음).
지금: 마지막 확정 상태 뒤에 상태를 바꾸는 쓰기가 있었으면 stale 이 되기 전(HEATER_STALE_SEC)에는 다시 발행하지 않는다.
쓰기가 없었던 읽기 실패는 예전처럼 다시 발행하고, 실패가 길어지면 예전처럼 stale 로 발행한다.
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

import pytest                                                    # noqa: E402
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


def _fail_reads(plc, n):
    """다음 상태 읽기 n 번을 실패시킨다(E402 같은 일시적 응답 없음 흉내)."""
    orig = plc.read_regs_block
    left = {"n": int(n)}

    async def flaky(start, count, *, priority="high"):
        if left["n"] > 0:
            left["n"] -= 1
            raise RuntimeError("E402 timeout (일시적)")
        return await orig(start, count, priority=priority)
    plc.read_regs_block = flaky


def _listen(svc, clk):
    pub = []
    svc.add_listener(lambda st: pub.append((clk.t, bool(st.get("run")), bool(st.get("stale")), st.get("age_s"))))
    return pub


@pytest.mark.parametrize("wait", [True, False])
def test_process_turn_on_then_one_read_fail_no_false_stop(wait):
    """HEATER_SET(켜기) 직후 읽기 1회 실패 — 도달 대기(wait)·공정 중 감시(watch) 모두 멈추지 않는다."""
    async def go():
        rt, pc, rec = _rt()
        svc, plc, clk, logs, chat = sim.make(mv_limit=700)
        rt.attach_heater(svc)
        await sim.run(svc, plc, clk, 5)
        pub = _listen(svc, clk)
        pc._heater_set(200.0, None)                              # 공정이 히터를 켬(RUN=1) → 감시 시작
        await _tick()
        if wait:
            pc._heater_wait(200.0)                               # 바로 도달 대기(HEATER_WAIT)로 넘어감
        assert rec.names() == ["on_heater_set_ok"] and rt._heater_watch and plc.M[640]
        _fail_reads(plc, 1)
        await sim.run(svc, plc, clk, 3)
        await _tick()
        assert rec.names() == ["on_heater_set_ok"], rec.calls     # 예전: 켜기 전 RUN=0 발행 → E830 / E832
        assert not [p for p in pub if not p[1] and not p[2]], pub  # stale 아닌 RUN=0 발행 없음
        assert pub and pub[-1][1] and not pub[-1][2]               # 다음 폴링이 RUN=1 을 발행
        assert [m for _t, m in logs if "히터 상태 읽기 실패" in m]   # 실패 로그는 예전처럼
    asyncio.run(go())


def test_panel_turn_off_then_read_fail_no_old_run():
    """패널 끄기 직후 읽기 1회 실패 — 끄기 전 RUN=1 을 다시 발행하지 않는다."""
    async def go():
        svc, plc, clk, logs, chat = sim.make(mv_limit=700)
        await sim.run(svc, plc, clk, 5)
        ok, why = await svc.turn_on(150.0, owner="manual")
        assert ok, why
        await sim.run(svc, plc, clk, 5)
        pub = _listen(svc, clk)
        ok, why = await svc.turn_off("정지(패널)", owner="manual")
        assert ok, why
        _fail_reads(plc, 1)
        await sim.run(svc, plc, clk, 3)
        assert pub and not any(p[1] for p in pub), pub
    asyncio.run(go())


def test_read_fail_without_write_still_republishes():
    """쓰기가 없었으면 예전 그대로 — 읽기 실패 때 마지막 상태를 stale 아님(age 1초)으로 다시 발행한다."""
    async def go():
        svc, plc, clk, logs, chat = sim.make(mv_limit=700)
        await sim.run(svc, plc, clk, 5)
        pub = _listen(svc, clk)
        _fail_reads(plc, 1)
        await sim.run(svc, plc, clk, 2)
        assert pub[0] == (pub[0][0], False, False, 1.0) and len(pub) == 2, pub
    asyncio.run(go())


def test_long_read_fail_after_turn_on_publishes_stale_then_waiter_fails_as_before():
    """켠 뒤 읽기 실패가 길어지면 HEATER_STALE_SEC 뒤부터 예전처럼 stale 로 발행 — 도달 대기는 'PLC 응답 없음'으로 끝난다(꺼짐 아님)."""
    async def go():
        rt, pc, rec = _rt()
        svc, plc, clk, logs, chat = sim.make(mv_limit=700)
        rt.attach_heater(svc)
        await sim.run(svc, plc, clk, 5)
        pub = _listen(svc, clk)
        pc._heater_set(200.0, None)
        await _tick()
        pc._heater_wait(200.0)
        t0 = clk.t
        _fail_reads(plc, 10 ** 6)
        await sim.run(svc, plc, clk, 120)
        await _tick()
        assert not [p for p in pub if not p[2]], pub[:5]           # stale 아닌 옛 상태 발행 없음
        first = min(p[0] for p in pub)
        assert first - t0 <= svc.cfg.stale_sec + 2                  # stale_sec 뒤부터 stale 로 발행(예전과 같은 시점)
        fails = [c for c in rec.calls if c[0] == "on_heater_failed"]
        assert len(fails) == 1 and "PLC 응답 없음" in fails[0][1][0], rec.calls
    asyncio.run(go())

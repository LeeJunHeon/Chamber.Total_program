# -*- coding: utf-8 -*-
"""Pre-Sputter 예약 루프 — 회차 실행 예외가 나도 루프가 끝나지 않고 다음 예약을 유지하는지."""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timedelta

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from runtime.pre_sputter_runtime import PreSputterRuntime   # noqa: E402


def _rt(*, daily: bool):
    rt = PreSputterRuntime(None, None, hh=6, mm=0, tick_s=0.01)
    rt._repeat_daily = daily
    logs = []
    rt.set_pc_logger(logs.append)
    calls = []

    async def _boom(*, parallel):
        calls.append(parallel)
        raise RuntimeError("가짜 실행 실패")

    rt._run = _boom
    return rt, logs, calls


def test_daily_exception_keeps_next_day_schedule():
    async def go():
        rt, logs, calls = _rt(daily=True)
        when = datetime.now() - timedelta(seconds=1)
        task = asyncio.create_task(rt._loop(when))
        rt._task = task
        await asyncio.sleep(0.2)
        try:
            assert calls == [rt.parallel]                      # 한 번 실행했고
            assert not task.done()                             # 루프는 살아서
            assert rt.is_reserved
            nxt = (when + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
            assert any("실행 중 예외: RuntimeError('가짜 실행 실패') — 이번 회차 실패, 다음 예약 유지" in s
                       for s in logs), logs
            assert any(f"다음 반복 예약: {nxt}" in s for s in logs), logs   # 하루 뒤로
            assert rt.status_text == "실행 오류 · 다음 06:00"
            assert rt.left_text != "00:00:00"                 # 다음 회차 대기 중
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(go())


def test_once_exception_ends_normally():
    async def go():
        rt, logs, calls = _rt(daily=False)
        task = asyncio.create_task(rt._loop(datetime.now() - timedelta(seconds=1)))
        rt._task = task
        await asyncio.wait_for(task, timeout=2.0)              # 예외 없이 정상 종료
        assert calls == [rt.parallel]
        assert task.exception() is None
        assert any("1회 예약 실행 완료(반복 없음)" in s for s in logs), logs
        assert rt._task is None
        assert rt.status_text == "실행 오류"
    asyncio.run(go())


def test_cancel_ends_loop():
    async def go():
        rt, logs, calls = _rt(daily=True)
        task = asyncio.create_task(rt._loop(datetime.now() + timedelta(hours=1)))
        rt._task = task
        await asyncio.sleep(0.05)
        assert not task.done()
        task.cancel()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), timeout=2.0)
        assert task.done()
        assert calls == []
        assert not any("실행 중 예외" in s for s in logs)
    asyncio.run(go())

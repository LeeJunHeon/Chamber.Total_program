# -*- coding: utf-8 -*-
"""TASK CRASHED 오탐 제거 + 중복 억제 검증 (Qt 불필요, 실제 sleep 없음).

1) AWAITED_TASK_PREFIX 이름의 태스크가 예외로 끝나면 TASK CRASHED 가 기록되지 않는다
2) 이름 없는 태스크의 예외는 기존과 동일하게 1회 기록된다
3) 같은 예외가 창 안에서 N회 반복되면 기록 1회 + 억제 카운트 N-1
4) 창이 지난 뒤 같은 예외가 오면 "반복 억제" 요약 1줄이 먼저 나온다
5) 키가 다르면 각각 독립적으로 1회씩 기록된다
"""
import os
import sys
import asyncio
import logging
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                   # noqa: E402
import util.app_logging as AL                   # noqa: E402


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class _Cap(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def crashes(self):
        return [r for r in self.records if r.getMessage() == "TASK CRASHED"]

    def summaries(self):
        return [r for r in self.records if r.getMessage().startswith("TASK CRASHED (반복 억제)")]


@pytest.fixture
def env(monkeypatch):
    clock = _Clock()
    monkeypatch.setattr(AL.time, "monotonic", clock)
    AL._task_crash_seen.clear()
    logger = logging.getLogger("test_task_crash_dedup")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    cap = _Cap()
    logger.addHandler(cap)
    yield clock, logger, cap
    logger.removeHandler(cap)
    AL._task_crash_seen.clear()


def _run(logger, coros_with_names):
    """패치된 루프에서 태스크를 만들고 예외를 호출부가 소비한 뒤 콜백까지 흘려보낸다."""
    async def _main():
        loop = asyncio.get_running_loop()
        AL.install_asyncio_exception_logging(loop, logger)
        tasks = []
        for coro, name in coros_with_names:
            t = loop.create_task(coro)
            if name:
                t.set_name(name)
            tasks.append(t)
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(0)   # done_callback 소비
    asyncio.run(_main())


async def _boom(msg="boom"):
    raise RuntimeError(msg)


async def _boom2():
    raise ValueError("other")


def test_1_awaited_prefix_not_logged(env):
    clock, logger, cap = env
    _run(logger, [(_boom(), AL.AWAITED_TASK_PREFIX + "plc:_connect_sync")])
    assert cap.crashes() == []
    assert cap.summaries() == []


def test_2_unnamed_task_logged_once(env):
    clock, logger, cap = env
    _run(logger, [(_boom(), None)])
    cs = cap.crashes()
    assert len(cs) == 1
    assert cs[0].exc_info and isinstance(cs[0].exc_info[1], RuntimeError)


def test_3_repeat_in_window_logged_once(env):
    clock, logger, cap = env
    N = 7
    for _ in range(N):
        _run(logger, [(_boom(), None)])
        clock.t += 1.0
    assert len(cap.crashes()) == 1
    assert cap.summaries() == []
    (st,) = AL._task_crash_seen.values()
    assert st["n"] == N - 1


def test_4_summary_after_window(env):
    clock, logger, cap = env
    for _ in range(4):
        _run(logger, [(_boom(), None)])
        clock.t += 1.0
    clock.t += AL.TASK_CRASH_DEDUP_S + 1.0
    _run(logger, [(_boom(), None)])
    sums = cap.summaries()
    assert len(sums) == 1, [r.getMessage() for r in cap.records]
    m = sums[0].getMessage()
    assert "같은 오류 3회" in m and "RuntimeError: boom" in m, m
    assert sums[0].exc_info is None
    # 요약이 먼저, 그 다음 새 사이클의 전체 기록
    assert len(cap.crashes()) == 2
    assert cap.records.index(sums[0]) < cap.records.index(cap.crashes()[1])


def test_5_distinct_keys_independent(env):
    clock, logger, cap = env
    _run(logger, [(_boom(), None), (_boom2(), None), (_boom("boom"), None)])
    cs = cap.crashes()
    assert len(cs) == 2
    assert {type(r.exc_info[1]).__name__ for r in cs} == {"RuntimeError", "ValueError"}
    assert len(AL._task_crash_seen) == 2


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

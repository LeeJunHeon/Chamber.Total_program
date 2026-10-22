# -*- coding: utf-8 -*-
"""PLC 로그 작업별 경로(plc.log_route) + 호스트 _plc_call 회귀 테스트 ([EL-072] [EL-043])."""
import asyncio
import logging

from device.plc import AsyncPLC
from host.context import HostContext
from host.handlers import HostHandlers
from util.app_logging import classify_log_level


def _plc():
    base = []

    def _log(fmt, *a):
        base.append(fmt % a if a else str(fmt))

    p = AsyncPLC(ip="127.0.0.1", port=1502, unit=1, timeout_s=0.5, logger=_log)
    p._closed = True          # resume_watchdog 가 하트비트 태스크를 만들지 않게
    return p, base


def test_route_only_for_current_task():
    p, base = _plc()
    routed = []

    async def go():
        other_started = asyncio.Event()
        release = asyncio.Event()

        async def other():
            other_started.set()
            await release.wait()
            p.log("other-during")              # 다른 작업 → 기본 로거

        t = asyncio.create_task(other())
        await other_started.wait()
        with p.log_route(lambda f, *a: routed.append(f % a if a else str(f))):
            p.log("mine %s", 1)                # 내 작업 → 경로
            child = asyncio.create_task(asyncio.sleep(0, result=None))
            await child

            async def kid():
                p.log("child")                 # 자식 작업 → 기본 로거
            await asyncio.create_task(kid())
            await asyncio.to_thread(lambda: p.log("thread"))   # 스레드 → 기본 로거
            release.set()
            await t
        p.log("mine-after")                    # 블록 밖 → 기본 로거

    asyncio.run(go())
    assert routed == ["mine 1"]
    assert base == ["child", "thread", "other-during", "mine-after"]


def test_setter_changes_base_not_routes():
    p, base = _plc()
    other = []

    async def go():
        with p.log_route(lambda f, *a: None):
            p.log = lambda f, *a: other.append(f)      # 기본 로거 교체
            p.log("in-route")                          # 경로가 우선
        p.log("after")
    asyncio.run(go())
    assert other == ["after"] and base == []


def test_log_route_outside_loop_is_noop():
    p, base = _plc()
    with p.log_route(lambda *a: None):
        p.log("x")
    assert base == ["x"]


def _handlers(p):
    ui = []
    ctx = HostContext(
        log=lambda tag, msg: ui.append((tag, msg)), plc=p, ch1=None, ch2=None, pc=None,
        runtime_state=None, lock_plc=asyncio.Lock(), lock_ch1=asyncio.Lock(), lock_ch2=asyncio.Lock(),
    )
    return HostHandlers(ctx), ui


def test_overlapping_plc_calls_never_stick():
    """예전 코드: 요청 B 가 A 의 교체 중에 '원래 값'을 읽어 끝난 뒤에도 plc.log 가 파일 로거로 굳었다."""
    p, base = _plc()
    base_fn = p.log
    h, ui = _handlers(p)

    async def go():
        a_in = asyncio.Event()
        a_go = asyncio.Event()

        async def req_a():
            async with h._plc_command("REQ_A"):
                async with h._plc_call():
                    a_in.set()
                    p.log("A io")
                    await a_go.wait()

        async def req_b():
            await a_in.wait()
            async with h._plc_command("REQ_B"):
                async with h._plc_call():          # A 가 락을 쥔 동안 대기 → 이후 진입
                    p.log("B io")

        async def chamber():
            await a_in.wait()
            p.log("chamber io during A")         # 다른 작업 → 기본 로거

        ta, tb, tc = (asyncio.create_task(c()) for c in (req_a, req_b, chamber))
        await tc
        await asyncio.sleep(0.01)
        a_go.set()
        await asyncio.gather(ta, tb)
        p.log("after both")

    asyncio.run(go())
    assert p.log is base_fn
    assert base == ["chamber io during A", "after both"]
    msgs = [m for tag, m in ui if tag == "PLC_REMOTE"]
    assert any(m.endswith("[REQ_A] A io") for m in msgs)
    assert any(m.endswith("[REQ_B] B io") for m in msgs)
    assert not any("chamber io" in m for m in msgs)


def test_classify_log_level():
    W, E, I = logging.WARNING, logging.ERROR, logging.INFO
    assert classify_log_level("PLC_REMOTE", "2026-10-22 10:00:00 [GET_SPUTTER_STATUS] WARN in-lock IO 2.5s") == W
    assert classify_log_level("PLC_REMOTE", "2026-10-22 10:00:00 WARN lock-wait 3.0s") == W
    assert classify_log_level("PLC_REMOTE", "2026-10-22 10:00:00 [X] ❌ fail") == E
    assert classify_log_level("PLC_REMOTE", "2026-10-22 10:00:00 [CH1_GATE_OPEN] read L_ATM -> True") == I
    # 시각으로 시작하지 않는 줄은 예전 판정 그대로
    assert classify_log_level("PLC", "WARN lock-wait 1.2s") == W
    assert classify_log_level("PLC", "[CH1&2] PLC 연결 끊김") == I
    assert classify_log_level("ERROR/HOST", "x") == E
    assert classify_log_level("CH1", "⚠ 경고") == W
    assert classify_log_level("CH1", "ERROR: x") == E
    assert classify_log_level("CH1", "정상 진행") == I

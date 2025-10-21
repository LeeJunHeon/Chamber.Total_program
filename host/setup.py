# host/setup.py
# -*- coding: utf-8 -*-
"""
조립/기동/정리
- main.py 에서는 install_host(...) 한 줄만 호출하면 된다.
"""
from __future__ import annotations
import asyncio
from typing import Any, Callable
from .context import HostContext
from .handlers import HostHandlers
from .router import Router
from .server import HostServer

LogFn = Callable[[str, str], None]


class HostHandle:
    """메인에서 종료 시 aclose()만 호출하면 됨"""
    def __init__(self, server: HostServer) -> None:
        self._server = server

    async def aclose(self) -> None:
        await self._server.aclose()


async def install_host(*,
    host: str,
    port: int,
    log: LogFn,
    plc: Any,
    ch1: Any,
    ch2: Any,
    pc: Any,
    runtime_state: Any,
) -> HostHandle:
    # 컨텍스트/락
    ctx = HostContext(
        log=log, plc=plc, ch1=ch1, ch2=ch2, pc=pc, runtime_state=runtime_state,
        lock_plc=asyncio.Lock(), lock_ch1=asyncio.Lock(), lock_ch2=asyncio.Lock()
    )

    # 핸들러/라우터
    h = HostHandlers(ctx)
    r = Router()
    r.register("GET_SPUTTER_STATUS", h.get_sputter_status)
    r.register("START_SPUTTER", h.start_sputter)
    r.register("START_PLASMA_CLEANING", h.start_plasma_cleaning)
    r.register("VACUUM_ON", h.vacuum_on)
    r.register("VACUUM_OFF", h.vacuum_off)
    r.register("4PIN_UP", h.four_pin_up)
    r.register("4PIN_DOWN", h.four_pin_down)
    # CH1/CH2는 command 자체를 고정 문자열로 받고, 내부에서 ch를 주입
    r.register("CH1_GATE_OPEN",  lambda d: h.gate_open({**d, "ch": 1}))
    r.register("CH2_GATE_OPEN",  lambda d: h.gate_open({**d, "ch": 2}))
    r.register("CH1_GATE_CLOSE", lambda d: h.gate_close({**d, "ch": 1}))
    r.register("CH2_GATE_CLOSE", lambda d: h.gate_close({**d, "ch": 2}))
    r.register("CH1_CHUCK_UP",   lambda d: h.chuck_up({**d, "ch": 1}))
    r.register("CH2_CHUCK_UP",   lambda d: h.chuck_up({**d, "ch": 2}))
    r.register("CH1_CHUCK_DOWN", lambda d: h.chuck_down({**d, "ch": 1}))
    r.register("CH2_CHUCK_DOWN", lambda d: h.chuck_down({**d, "ch": 2}))

    # 서버 기동
    server = HostServer(host, port, r, log)
    await server.start()
    return HostHandle(server)

# host/context.py
# -*- coding: utf-8 -*-
"""
공유 컨텍스트
- 기존 프로젝트의 런타임/PLC/상태/로그 콜백과 락을 한 곳에 모아
  handlers에서만 참조하도록 한다.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, Optional
import asyncio

LogFn = Callable[[str, str], None]
PopupFn = Callable[[str, str], None]

@dataclass
class HostContext:
    log: LogFn
    plc: Any
    ch1: Any
    ch2: Any
    pc: Any
    runtime_state: Any
    lock_plc: asyncio.Lock
    lock_ch1: asyncio.Lock
    lock_ch2: asyncio.Lock

    chat: Optional[Any] = None
    popup: Optional[PopupFn] = None

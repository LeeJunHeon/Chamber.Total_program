# host/router.py
# -*- coding: utf-8 -*-
"""
문자열 커맨드 → 핸들러 매핑
- 응답 커맨드는 자동으로 "*_RESULT"로 통일
- 미지원 커맨드는 표준 실패 포맷 반환
"""
from __future__ import annotations
from typing import Awaitable, Callable, Dict, Tuple, Any

Json = Dict[str, Any]
Handler = Callable[[Json], Awaitable[Json]]


class Router:
    def __init__(self) -> None:
        self._handlers: Dict[str, Handler] = {}

    def register(self, command: str, handler: Handler) -> None:
        self._handlers[command.upper().strip()] = handler

    async def dispatch(self, command: str, data: Json) -> Tuple[str, Json]:
        c = command.upper().strip()
        h = self._handlers.get(c)
        if not h:
            return f"{c}_RESULT", {"result": "fail", "message": f"Unknown command: {command}"}
        res = await h(data or {})
        return f"{c}_RESULT", res

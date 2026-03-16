# host/router.py
# -*- coding: utf-8 -*-
"""
문자열 커맨드 → 핸들러 매핑
- 응답 커맨드는 자동으로 "*_RESULT"로 통일
- 미지원 커맨드는 표준 실패 포맷 반환
"""
from __future__ import annotations

from typing import Awaitable, Callable, Dict, Tuple, Any

from errors.app_error import AppError
from errors.error_payload import build_error_payload_from_code

Json = Dict[str, Any]
Handler = Callable[[Json], Awaitable[Json]]


def _normalize_command(command: str) -> str:
    return str(command or "").upper().strip()


class Router:
    def __init__(self) -> None:
        self._handlers: Dict[str, Handler] = {}

    def register(self, command: str, handler: Handler) -> None:
        c = _normalize_command(command)
        if not c:
            raise ValueError("register(): command must not be empty")
        if not callable(handler):
            raise TypeError("register(): handler must be callable")
        self._handlers[c] = handler

    async def dispatch(self, command: str, data: Json) -> Tuple[str, Json]:
        c = command.upper().strip()
        h = self._handlers.get(c)

        if not c:
            return "UNKNOWN_RESULT", build_error_payload_from_code(
                "E105",
                detail="Missing command",
                meta={"command": command},
            )

        if not h:
            return f"{c}_RESULT", build_error_payload_from_code(
                "E106",
                detail=f"Unknown command: {command}",
                meta={"command": command},
            )
        
        payload = data if isinstance(data, dict) else {}
        res = await h(payload)

        if not isinstance(res, dict):
            raise AppError(
                "E111",
                detail=f"Handler returned non-dict: {type(res).__name__} raw={res!r}",
                meta={"command": c},
            )

        return f"{c}_RESULT", res

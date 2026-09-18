# host/request_ctx.py
# -*- coding: utf-8 -*-
"""
요청 단위 컨텍스트 (ContextVar)

host/server.py 가 바디 파싱을 마친 직후(= 명령을 받은 시각) 세팅하고,
router.dispatch 가 끝나면 reset 한다. 핸들러는 get_request_ctx() 로 읽는다.
동시 요청이 섞이지 않도록 handlers.py 의 _cmd_tag_var 와 같은 방식을 쓴다.
"""
from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass(frozen=True)
class RequestCtx:
    request_id: str = ""
    peer: str = ""
    received_at: Optional[datetime] = None


_request_ctx_var: ContextVar[Optional[RequestCtx]] = ContextVar("host_request_ctx", default=None)


def set_request_ctx(d: Any) -> Token:
    """dict 또는 RequestCtx 를 받아 세팅하고 reset 용 토큰을 돌려준다."""
    if isinstance(d, RequestCtx):
        ctx = d
    else:
        d = d or {}
        rx = d.get("received_at")
        ctx = RequestCtx(
            request_id=str(d.get("request_id") or ""),
            peer=str(d.get("peer") or ""),
            received_at=rx if isinstance(rx, datetime) else None,
        )
    return _request_ctx_var.set(ctx)


def reset_request_ctx(token: Optional[Token]) -> None:
    if token is None:
        return
    try:
        _request_ctx_var.reset(token)
    except Exception:
        pass


def get_request_ctx() -> RequestCtx:
    """없으면 빈 컨텍스트(received_at=None)를 돌려준다 — 호출부에서 now() 로 대체."""
    return _request_ctx_var.get() or RequestCtx()


__all__ = ["RequestCtx", "set_request_ctx", "reset_request_ctx", "get_request_ctx"]

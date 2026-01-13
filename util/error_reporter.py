# util/error_reporter.py
from __future__ import annotations
from typing import Any, Callable, Optional

from .error_catalog import ErrorCatalog, ErrorInfo

_CATALOG = ErrorCatalog()

def _to_text(err: Any) -> str:
    try:
        return "" if err is None else str(err)
    except Exception:
        return repr(err)

def build_error_info(*, code: Optional[str] = None, message: Any = "") -> ErrorInfo:
    msg = _to_text(message).strip()

    if code:
        c = code if code.startswith("E") else f"E{code}"
        return _CATALOG.get(c, default_message=msg)

    guessed = _CATALOG.guess_code(msg)
    if guessed:
        return _CATALOG.get(guessed, default_message=msg)

    # 최후 fallback
    return _CATALOG.get("E110", default_message=msg or "Handler crash")

def format_error_message(info: ErrorInfo, *, detail: str = "") -> str:
    d = (detail or "").strip()
    if not d or d == info.cause or info.cause in d:
        return f"{info.code} | {info.cause}\n해결방법: {info.fix}".strip()
    return f"{info.code} | {info.cause}\n상세: {d}\n해결방법: {info.fix}".strip()

def build_fail_payload(*, code: Optional[str] = None, message: Any = "", detail: Any = None) -> dict:
    msg = _to_text(message).strip()
    info = build_error_info(code=code, message=msg)

    det = _to_text(detail if detail is not None else msg)
    human = format_error_message(info, detail=det)

    return {
        "result": "fail",
        "message": human,      # client에게 보여줄 텍스트
        "error_code": info.code,
        "cause": info.cause,
        "fix": info.fix,
        "detail": det,
    }

def notify_all(
    *,
    log: Optional[Callable[[str, str], None]] = None,
    chat: Any = None,
    popup: Optional[Callable[[str, str], None]] = None,
    src: str = "HOST",
    code: Optional[str] = None,
    message: Any = "",
) -> dict:
    payload = build_fail_payload(code=code, message=message)
    text = payload.get("message", "")

    # 1) UI 로그창
    if callable(log):
        try:
            log(f"ERROR/{src}", text)
        except Exception:
            pass

    # 2) Google Chat
    if chat is not None:
        try:
            fn = getattr(chat, "notify_error_with_src", None)
            if callable(fn):
                fn(src, text)
        except Exception:
            pass

    # 3) 알림창(팝업)
    if callable(popup):
        try:
            popup(f"오류({src})", text)
        except Exception:
            pass

    return payload

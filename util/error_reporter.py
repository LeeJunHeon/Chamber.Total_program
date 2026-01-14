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
    # ✅ message에는 "원인 + 해결방법"만 보냄
    # ✅ 줄바꿈(\n, \r) 자체를 만들지 않도록 1줄로 정리
    cause = " ".join((info.cause or "").replace("\r", "\n").splitlines()).strip()
    fix   = " ".join((info.fix   or "").replace("\r", "\n").splitlines()).strip()

    if cause and fix:
        return f"{cause} 해결방법: {fix}".strip()
    if cause:
        return cause
    if fix:
        return f"해결방법: {fix}".strip()
    return ""

def build_fail_payload(*, code: Optional[str] = None, message: Any = "", detail: Any = None) -> dict:
    msg = _to_text(message).strip()
    info = build_error_info(code=code, message=msg)

    # ✅ message는 원인/해결방법만 (상세는 message에 넣지 않음)
    human = format_error_message(info)

    return {
        "result": "fail",
        "message": human,      # ✅ 원인 + 해결방법만
        "error_code": info.code,
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
    #    - HOST/통신 계열 오류는 '끝'이 없어서 집계하지 말고 발생 즉시 전송
    #    - ChatNotifier에 notify_error_event(src, error_code, message) 경로를 우선 사용
    if chat is not None:
        try:
            err_code = str(payload.get("error_code", "") or "").strip()

            fn = getattr(chat, "notify_error_event", None)
            if callable(fn):
                fn(src, err_code, text)
            else:
                # 하위 호환(구버전 ChatNotifier)
                fn2 = getattr(chat, "notify_error_with_src", None)
                if callable(fn2):
                    fn2(src, text)
        except Exception:
            pass

    # 3) 알림창(팝업)
    if callable(popup):
        try:
            popup(f"오류({src})", text)
        except Exception:
            pass

    return payload

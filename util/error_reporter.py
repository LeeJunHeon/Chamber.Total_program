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

def _one_line(s: str, limit: int = 2000) -> str:
    s = " ".join((s or "").replace("\r", "\n").splitlines()).strip()
    if len(s) > limit:
        s = s[:limit] + "…"
    return s

def build_fail_payload(*, code: Optional[str] = None, message: Any = "", detail: Any = None) -> dict:
    raw = _one_line(_to_text(message).strip())
    info = build_error_info(code=code, message=raw)

    # ✅ 사용자 표시용(원인+해결방법)
    human = format_error_message(info)
    if not human:
        human = raw or "오류가 발생했습니다."

    # ✅ 디버깅용(detail) — 기본은 원래 message를 저장
    det = _one_line(_to_text(detail).strip()) if detail is not None else raw

    return {
        "result": "fail",
        "message": human,        # 팝업/채팅/기본 표시용
        "error_code": info.code,
        "detail": det,           # ✅ 로그/분석용
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
    # ✅ detail에 원래 message를 그대로 넣어둠(호출부 수정 불필요)
    payload = build_fail_payload(code=code, message=message, detail=message)
    text = payload.get("message", "")
    detail = payload.get("detail", "")

    # 1) UI 로그창/파일 로그에는 detail까지 남김
    if callable(log):
        try:
            err_code = str(payload.get("error_code", "") or "").strip()
            log_text = text if not detail or detail == text else f"{text} | detail: {detail}"
            if err_code:
                log_text = f"[{err_code}] {log_text}"
            log(f"ERROR/{src}", log_text)
        except Exception:
            pass

    # 2) Chat/Popup은 기존처럼 text만(너무 길어지는 것 방지)
    if chat is not None:
        try:
            err_code = str(payload.get("error_code", "") or "").strip()
            fn = getattr(chat, "notify_error_event", None)
            if callable(fn):
                fn(src, err_code, text)
            else:
                fn2 = getattr(chat, "notify_error_with_src", None)
                if callable(fn2):
                    fn2(src, text)
        except Exception:
            pass

    if callable(popup):
        try:
            popup(f"오류({src})", text)
        except Exception:
            pass

    return payload

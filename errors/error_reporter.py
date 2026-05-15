from __future__ import annotations

from typing import Any, Callable, Optional

from .error_payload import (
    build_error_payload,
    build_error_payload_from_code,
    build_handler_error_payload,
    build_unknown_error_payload,
)


def _to_text(value: Any) -> str:
    try:
        return "" if value is None else str(value)
    except Exception:
        return repr(value)


def _one_line(text: str, limit: int = 2000) -> str:
    s = " ".join((text or "").replace("\r", "\n").splitlines()).strip()
    if len(s) > limit:
        s = s[:limit] + "…"
    return s


def _build_log_text(payload: dict) -> str:
    err_code = str(payload.get("error_code", "") or "").strip()
    action = str(payload.get("client_action", "") or "").strip()
    message = _one_line(_to_text(payload.get("message", "")))
    detail = _one_line(_to_text(payload.get("detail", "")))

    log_text = message
    if detail and detail != message:
        log_text = f"{message} | 상세: {detail}"

    prefix = []
    if err_code:
        prefix.append(err_code)
    if action:
        prefix.append(action)

    if prefix:
        log_text = f"[{' / '.join(prefix)}] {log_text}"

    return log_text.strip()


def _build_user_text(payload: dict) -> str:
    err_code = str(payload.get("error_code", "") or "").strip()
    message = _one_line(_to_text(payload.get("message", "")))
    detail = _one_line(_to_text(payload.get("detail", "")))

    prefix = f"[{err_code}] " if err_code else ""

    if detail and detail != message:
        return f"{prefix}{message} | 상세: {detail}"
    return f"{prefix}{message}"


def report_payload(
    payload: dict,
    *,
    log: Optional[Callable[[str, str], None]] = None,
    chat: Any = None,
    popup: Optional[Callable[[str, str], None]] = None,
    src: str = "HOST",
) -> dict:
    """
    이미 만들어진 표준 fail payload를
    log/chat/popup 으로 전파한다.
    """
    text = _build_user_text(payload)
    err_code = str(payload.get("error_code", "") or "").strip()

    # 1) 로그
    if callable(log):
        try:
            log(f"ERROR/{src}", _build_log_text(payload))
        except Exception:
            pass

    # 2) Chat
    if chat is not None:
        try:
            fn = getattr(chat, "notify_error_event", None)
            if callable(fn):
                fn(src, err_code, text)
            else:
                fn2 = getattr(chat, "notify_error_with_src", None)
                if callable(fn2):
                    fn2(src, text)
        except Exception:
            pass

    # 3) Popup
    if callable(popup):
        try:
            title = f"오류({src})"
            if err_code:
                title = f"오류({src}, {err_code})"

            popup(title, text)
        except Exception:
            pass

    return payload


def notify_error(
    exc: BaseException,
    *,
    log: Optional[Callable[[str, str], None]] = None,
    chat: Any = None,
    popup: Optional[Callable[[str, str], None]] = None,
    src: str = "HOST",
    default_code: str = "E999",
    detail: str | None = None,
    meta: Optional[dict] = None,
) -> dict:
    """
    어떤 예외든 표준 fail payload로 만든 뒤 report한다.

    일반적인 runtime/device/controller 경계에서 사용.
    """
    payload = build_error_payload(
        exc,
        default_code=default_code,
        detail=detail,
        meta=meta,
    )
    return report_payload(
        payload,
        log=log,
        chat=chat,
        popup=popup,
        src=src,
    )


def notify_handler_error(
    exc: BaseException,
    *,
    log: Optional[Callable[[str, str], None]] = None,
    chat: Any = None,
    popup: Optional[Callable[[str, str], None]] = None,
    src: str = "HOST",
    meta: Optional[dict] = None,
) -> dict:
    """
    handler 경계용.
    - AppError면 본래 code 유지
    - 일반 예외면 E110 fallback
    """
    payload = build_handler_error_payload(exc, meta=meta)
    return report_payload(
        payload,
        log=log,
        chat=chat,
        popup=popup,
        src=src,
    )


def notify_unknown_error(
    exc: BaseException,
    *,
    log: Optional[Callable[[str, str], None]] = None,
    chat: Any = None,
    popup: Optional[Callable[[str, str], None]] = None,
    src: str = "HOST",
    meta: Optional[dict] = None,
) -> dict:
    """
    최상단 경계용.
    알 수 없는 예외를 E999로 표준화해서 report한다.
    """
    payload = build_unknown_error_payload(exc, meta=meta)
    return report_payload(
        payload,
        log=log,
        chat=chat,
        popup=popup,
        src=src,
    )


def notify_error_code(
    code: str,
    *,
    log: Optional[Callable[[str, str], None]] = None,
    chat: Any = None,
    popup: Optional[Callable[[str, str], None]] = None,
    src: str = "HOST",
    detail: str = "",
    meta: Optional[dict] = None,
) -> dict:
    """
    예외 객체 없이 에러코드만으로 즉시 report할 때 사용.
    """
    payload = build_error_payload_from_code(
        code,
        detail=detail,
        meta=meta,
    )
    return report_payload(
        payload,
        log=log,
        chat=chat,
        popup=popup,
        src=src,
    )


__all__ = [
    "report_payload",
    "notify_error",
    "notify_handler_error",
    "notify_unknown_error",
    "notify_error_code",
]
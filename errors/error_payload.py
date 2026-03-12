from __future__ import annotations

from typing import Any, Dict, Optional

from .app_error import AppError, ensure_app_error
from .error_registry import get_error_definition


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _merge_meta(
    base: Optional[Dict[str, Any]],
    extra: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    if isinstance(base, dict):
        merged.update(base)
    if isinstance(extra, dict):
        merged.update(extra)
    return merged


def build_error_payload_from_code(
    code: str,
    *,
    detail: str = "",
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    에러코드만으로 표준 fail payload 생성.

    사용 예:
        payload = build_error_payload_from_code(
            "E302",
            detail="G_V_1_인터락=FALSE",
            meta={"ch": 1}
        )
    """
    err = get_error_definition(code)

    final_detail = _clean_text(detail) or err.cause

    payload: Dict[str, Any] = {
        "result": "fail",
        "error_code": err.code,
        "client_action": err.client_action,
        "category": err.category,
        "severity": err.severity,
        "message": err.title,
        "detail": final_detail,
        "guide": err.fix,
    }

    merged_meta = _merge_meta(None, meta)
    if merged_meta:
        payload["meta"] = merged_meta

    return payload


def build_error_payload(
    exc: BaseException,
    *,
    default_code: str = "E999",
    detail: str | None = None,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    어떤 예외가 들어오더라도 표준 fail payload 반환.

    규칙:
    - AppError면 그 code를 그대로 사용
    - 일반 예외면 default_code(E999 기본값)로 감쌈
    - detail 우선순위:
        1) 함수 인자로 받은 detail
        2) AppError.detail
        3) 원본 예외 문자열
        4) registry의 cause
    """
    app_err = ensure_app_error(
        exc,
        default_code=default_code,
        detail=detail,
        meta=meta,
    )
    err = get_error_definition(app_err.code)

    final_detail = (
        _clean_text(detail)
        or _clean_text(app_err.detail)
        or (str(app_err.cause).strip() if app_err.cause else "")
        or err.cause
    )

    payload: Dict[str, Any] = {
        "result": "fail",
        "error_code": err.code,
        "client_action": err.client_action,
        "category": err.category,
        "severity": err.severity,
        "message": err.title,
        "detail": final_detail,
        "guide": err.fix,
    }

    merged_meta = _merge_meta(app_err.meta, meta)
    if merged_meta:
        payload["meta"] = merged_meta

    if app_err.cause is not None:
        payload["cause_type"] = type(app_err.cause).__name__

    return payload


def build_unknown_error_payload(
    exc: BaseException,
    *,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    정의되지 않은 예외를 E999로 표준화.
    host/server 최상단 except에서 사용하기 좋다.
    """
    return build_error_payload(
        exc,
        default_code="E999",
        meta=meta,
    )


def build_handler_error_payload(
    exc: BaseException,
    *,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    handler 내부 예외를 E110 fallback으로 표준화.
    단, AppError는 본래 code 유지.
    """
    if isinstance(exc, AppError):
        return build_error_payload(exc, meta=meta)

    return build_error_payload(
        exc,
        default_code="E110",
        meta=meta,
    )


def is_fail_payload(payload: Any) -> bool:
    return isinstance(payload, dict) and payload.get("result") == "fail"


__all__ = [
    "build_error_payload_from_code",
    "build_error_payload",
    "build_unknown_error_payload",
    "build_handler_error_payload",
    "is_fail_payload",
]
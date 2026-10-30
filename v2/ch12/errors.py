"""v1 오류 코드 카탈로그를 그대로 잇는다 — 로봇에 나가는 코드·문구·필드가 v1 과 같다.

v1 대응(동작·필드·순서 동일)
- errors/error_registry.py : ErrorDefinition·ERROR_REGISTRY(표는 v2/ch12/error_codes.py)·get_error_definition(없는 코드 → E999)·
                             get_error_* 조회·list_error_codes·build_registry_snapshot·build_legacy_default_codes
- errors/error_catalog.py  : has_error_code·normalize_error_code("302" → "E302", 모르면 fallback)·get_error_info·
                             extract_error_code(글 속 E### 만, 추정 없음)·is_fallback_error
- errors/app_error.py      : AppError(code 대문자·공백 제거, detail 문자열화, meta dict)·ensure_app_error·CodedRuntimeError
- errors/error_payload.py  : 로봇 실패 응답 payload — build_error_payload_from_code·build_error_payload·
                             build_unknown_error_payload(E999)·build_handler_error_payload(E110, AppError 는 제 코드)·is_fail_payload
- errors/error_reporter.py : 로그·사용자 문구(format_log_text·format_user_text — v1 _build_log_text·_build_user_text)

payload 규칙(v1 errors/error_payload.py 그대로)
- 필드 순서: result("fail")·error_code·client_action·category·severity·message·detail·guide, 그다음 meta(있을 때)·
  cause_type(원인 예외가 있을 때, build_error_payload 만).
- message = 표 title, guide = 표 fix, 미등록 코드 → E999 의 정의(error_code 도 "E999").
- detail 우선순위: 함수 인자 detail → AppError.detail → 원인 예외 문구 → 표 cause.
  (v1 그대로: ensure_app_error 가 일반 예외의 문구를 AppError.detail 에 넣으므로 일반 예외는 예외 문구가 detail 이 된다.)
- 시험: v2/tests/test_errors.py — v1 errors 모듈을 실제로 돌려 만든 골든(v2/tools/v1golden_errors.py → v2/tests/golden/errors.json).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from v2.ch12 import error_codes as _codes

ACTION_RETRY = _codes.RETRY
ACTION_STOP = _codes.STOP
ACTION_IGNORE = _codes.IGNORE
ACTION_RETRY_LATER = _codes.RETRY_LATER
SEV_WARN = _codes.WARN
SEV_ERROR = _codes.ERROR
SEV_FATAL = _codes.FATAL


@dataclass(frozen=True)
class ErrorDefinition:
    code: str
    title: str
    category: str
    client_action: str
    severity: str
    cause: str
    fix: str
    is_fallback: bool = False

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "title": self.title,
            "category": self.category,
            "client_action": self.client_action,
            "severity": self.severity,
            "cause": self.cause,
            "fix": self.fix,
            "is_fallback": self.is_fallback,
        }

    def to_legacy_dict(self) -> dict:
        """v1 error_codes_default 호환 모양 {cause, fix}."""
        return {"cause": self.cause, "fix": self.fix}


def _build_registry() -> Dict[str, ErrorDefinition]:
    reg: Dict[str, ErrorDefinition] = {}
    for row in _codes.ROWS:
        if len(row) == 8:
            code, title, cat, act, sev, fb, cause, fix = row
        else:
            (code, title, cat, act, sev, cause, fix), fb = row, False
        if code in reg:
            raise RuntimeError(f"오류 코드 중복: {code}")
        reg[code] = ErrorDefinition(code, title, cat, act, sev, cause, fix, bool(fb))
    return reg


ERROR_REGISTRY: Dict[str, ErrorDefinition] = _build_registry()


# ───────────── 조회(v1 errors/error_registry.py) ─────────────
def get_error_definition(code: Optional[str]) -> ErrorDefinition:
    """코드가 비었거나 없으면 E999."""
    if not code:
        return ERROR_REGISTRY["E999"]
    return ERROR_REGISTRY.get(str(code).strip().upper(), ERROR_REGISTRY["E999"])


def get_error_dict(code: str) -> dict:
    return get_error_definition(code).to_dict()


def get_legacy_error_dict(code: str) -> dict:
    return get_error_definition(code).to_legacy_dict()


def get_error_title(code: str) -> str:
    return get_error_definition(code).title


def get_error_category(code: str) -> str:
    return get_error_definition(code).category


def get_client_action(code: str) -> str:
    return get_error_definition(code).client_action


def get_error_severity(code: str) -> str:
    return get_error_definition(code).severity


def get_error_cause(code: str) -> str:
    return get_error_definition(code).cause


def get_error_fix(code: str) -> str:
    return get_error_definition(code).fix


def list_error_codes() -> List[str]:
    return list(ERROR_REGISTRY.keys())


def iter_error_definitions() -> Iterable[ErrorDefinition]:
    return ERROR_REGISTRY.values()


def build_legacy_default_codes() -> Dict[str, dict]:
    return {code: item.to_legacy_dict() for code, item in ERROR_REGISTRY.items()}


def build_registry_snapshot() -> Dict[str, dict]:
    return {code: item.to_dict() for code, item in ERROR_REGISTRY.items()}


# ───────────── 카탈로그(v1 errors/error_catalog.py) ─────────────
_ERROR_CODE_RE = re.compile(r"\b(E\d{3})\b", re.IGNORECASE)


def has_error_code(code: Optional[str]) -> bool:
    if not code:
        return False
    return str(code).strip().upper() in ERROR_REGISTRY


def normalize_error_code(code: Optional[str], *, fallback: str = "E999") -> str:
    """"e302 " → "E302", 숫자 세 자리 "302" → "E302", 모르는 코드 → fallback."""
    if not code:
        return fallback
    norm = str(code).strip().upper()
    if norm.isdigit() and len(norm) == 3:
        norm = f"E{norm}"
    return norm if norm in ERROR_REGISTRY else fallback


def get_error_info(code: Optional[str], *, fallback: str = "E999") -> ErrorDefinition:
    return get_error_definition(normalize_error_code(code, fallback=fallback))


def extract_error_code(text: Optional[str]) -> Optional[str]:
    """글 안에 E### 가 그대로 있고 표에 있는 코드일 때만(의미 추정 없음)."""
    if not text:
        return None
    m = _ERROR_CODE_RE.search(str(text).strip())
    if not m:
        return None
    code = m.group(1).upper()
    return code if code in ERROR_REGISTRY else None


def is_fallback_error(code: Optional[str]) -> bool:
    """v1 errors/error_registry.py 판 — 코드를 그대로(대문자·공백 제거만) 조회, 없으면 E999(True). "302" 도 E999."""
    return get_error_definition(code).is_fallback


def catalog_is_fallback(code: Optional[str]) -> bool:
    """v1 errors/error_catalog.py 판(같은 이름 is_fallback_error) — normalize 뒤 조회("302" → E302)."""
    return get_error_info(code).is_fallback


# ───────────── 예외(v1 errors/app_error.py) ─────────────
@dataclass(eq=False)
class AppError(Exception):
    """프로그램 전체 공통 오류 — code(E###), detail(현장 상세), meta(추가 정보), cause(원인 예외)."""
    code: str
    detail: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    cause: Optional[BaseException] = None

    def __post_init__(self) -> None:
        if not isinstance(self.code, str) or not self.code.strip():
            raise ValueError("AppError.code must be a non-empty string")
        self.code = self.code.strip().upper()
        self.detail = "" if self.detail is None else str(self.detail).strip()
        if self.meta is None:
            self.meta = {}
        elif not isinstance(self.meta, dict):
            raise ValueError("AppError.meta must be a dict")

    @property
    def error_code(self) -> str:
        return self.code

    def with_detail(self, detail: str) -> "AppError":
        return AppError(code=self.code, detail=detail, meta=dict(self.meta), cause=self.cause)

    def with_meta(self, **kwargs: Any) -> "AppError":
        merged = dict(self.meta)
        merged.update(kwargs)
        return AppError(code=self.code, detail=self.detail, meta=merged, cause=self.cause)

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {"error_code": self.code, "detail": self.detail, "meta": dict(self.meta)}
        if self.cause is not None:
            data["cause_type"] = type(self.cause).__name__
            data["cause"] = str(self.cause)
        return data

    def __str__(self) -> str:
        return f"{self.code}: {self.detail}" if self.detail else self.code

    def __repr__(self) -> str:
        return (f"AppError(code={self.code!r}, detail={self.detail!r}, meta={self.meta!r}, cause={self.cause!r})")


def ensure_app_error(exc: BaseException, *, default_code: str = "E999", detail: Optional[str] = None,
                     meta: Optional[Dict[str, Any]] = None) -> AppError:
    """어떤 예외든 AppError 로. AppError 면 코드 유지(detail 인자가 비어 있지 않으면 그것으로, meta 는 합침)."""
    if isinstance(exc, AppError):
        merged = dict(exc.meta)
        if meta:
            merged.update(meta)
        final_detail = exc.detail
        if detail is not None and str(detail).strip():
            final_detail = str(detail).strip()
        return AppError(code=exc.code, detail=final_detail, meta=merged,
                        cause=exc.cause if exc.cause is not None else exc)
    return AppError(code=default_code,
                    detail=(str(detail).strip() if detail is not None else str(exc).strip()),
                    meta=dict(meta or {}), cause=exc)


class CodedRuntimeError(RuntimeError):
    """RuntimeError + 오류 코드(.code) — v1 host/handlers.py 는 RuntimeError 의 code 를 먼저 응답 코드로 썼다."""

    def __init__(self, message: str, code: str):
        super().__init__(message)
        self.code = str(code).strip().upper()


# ───────────── 실패 응답 payload(v1 errors/error_payload.py) ─────────────
def _clean_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _merge_meta(base: Optional[Dict[str, Any]], extra: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    if isinstance(base, dict):
        merged.update(base)
    if isinstance(extra, dict):
        merged.update(extra)
    return merged


def _base_payload(err: ErrorDefinition, detail: str) -> Dict[str, Any]:
    return {
        "result": "fail",
        "error_code": err.code,
        "client_action": err.client_action,
        "category": err.category,
        "severity": err.severity,
        "message": err.title,
        "detail": detail,
        "guide": err.fix,
    }


def build_error_payload_from_code(code: str, *, detail: str = "", meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """코드만으로 실패 payload. detail 이 비면 표 cause."""
    err = get_error_definition(code)
    payload = _base_payload(err, _clean_text(detail) or err.cause)
    merged = _merge_meta(None, meta)
    if merged:
        payload["meta"] = merged
    return payload


def build_error_payload(exc: BaseException, *, default_code: str = "E999", detail: Optional[str] = None,
                        meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """어떤 예외든 실패 payload. AppError 면 그 코드, 아니면 default_code."""
    app_err = ensure_app_error(exc, default_code=default_code, detail=detail, meta=meta)
    err = get_error_definition(app_err.code)
    final_detail = (_clean_text(detail) or _clean_text(app_err.detail)
                    or (str(app_err.cause).strip() if app_err.cause else "") or err.cause)
    payload = _base_payload(err, final_detail)
    merged = _merge_meta(app_err.meta, meta)
    if merged:
        payload["meta"] = merged
    if app_err.cause is not None:
        payload["cause_type"] = type(app_err.cause).__name__
    return payload


def build_unknown_error_payload(exc: BaseException, *, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """최상단 경계 — 정의되지 않은 예외를 E999 로."""
    return build_error_payload(exc, default_code="E999", meta=meta)


def build_handler_error_payload(exc: BaseException, *, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """핸들러 경계 — 일반 예외는 E110, AppError 는 제 코드."""
    if isinstance(exc, AppError):
        return build_error_payload(exc, meta=meta)
    return build_error_payload(exc, default_code="E110", meta=meta)


def is_fail_payload(payload: Any) -> bool:
    return isinstance(payload, dict) and payload.get("result") == "fail"


# ───────────── 로그·사용자 문구(v1 errors/error_reporter.py) ─────────────
def _to_text(value: Any) -> str:
    try:
        return "" if value is None else str(value)
    except Exception:  # noqa: BLE001 — v1 과 같게 repr 로
        return repr(value)


def _one_line(text: str, limit: int = 2000) -> str:
    s = " ".join((text or "").replace("\r", "\n").splitlines()).strip()
    return s[:limit] + "…" if len(s) > limit else s


def format_log_text(payload: dict) -> str:
    """"[E302 / STOP] 제목 | 상세: …" (v1 _build_log_text)."""
    err_code = str(payload.get("error_code", "") or "").strip()
    action = str(payload.get("client_action", "") or "").strip()
    message = _one_line(_to_text(payload.get("message", "")))
    detail = _one_line(_to_text(payload.get("detail", "")))
    text = f"{message} | 상세: {detail}" if detail and detail != message else message
    prefix = [x for x in (err_code, action) if x]
    if prefix:
        text = f"[{' / '.join(prefix)}] {text}"
    return text.strip()


def format_user_text(payload: dict) -> str:
    """"[E302] 제목 | 상세: …" (v1 _build_user_text — 챗·팝업 문구)."""
    err_code = str(payload.get("error_code", "") or "").strip()
    message = _one_line(_to_text(payload.get("message", "")))
    detail = _one_line(_to_text(payload.get("detail", "")))
    prefix = f"[{err_code}] " if err_code else ""
    return f"{prefix}{message} | 상세: {detail}" if detail and detail != message else f"{prefix}{message}"


def popup_title(src: str, code: str = "") -> str:
    """v1 report_payload 팝업 제목 "오류(HOST, E302)"."""
    return f"오류({src}, {code})" if code else f"오류({src})"


__all__ = [
    "ACTION_RETRY", "ACTION_STOP", "ACTION_IGNORE", "ACTION_RETRY_LATER", "SEV_WARN", "SEV_ERROR", "SEV_FATAL",
    "ErrorDefinition", "ERROR_REGISTRY", "get_error_definition", "get_error_dict", "get_legacy_error_dict",
    "get_error_title", "get_error_category", "get_client_action", "get_error_severity", "get_error_cause",
    "get_error_fix", "list_error_codes", "iter_error_definitions", "build_legacy_default_codes",
    "build_registry_snapshot", "has_error_code", "normalize_error_code", "get_error_info", "extract_error_code",
    "is_fallback_error", "catalog_is_fallback", "AppError", "ensure_app_error", "CodedRuntimeError", "build_error_payload_from_code",
    "build_error_payload", "build_unknown_error_payload", "build_handler_error_payload", "is_fail_payload",
    "format_log_text", "format_user_text", "popup_title",
]

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(eq=False)
class AppError(Exception):
    """
    프로그램 전체에서 공통으로 사용하는 에러 객체.

    code:
        에러코드 (예: E302, E711, E999)

    detail:
        실제 현장에서 확인할 상세 원인
        예: "G_V_1_인터락=FALSE"

    meta:
        추가 정보
        예: {"ch": 1, "signal": "G_V_1_인터락"}

    cause:
        원본 예외 객체(선택)
        예: TimeoutError(...), ValueError(...)
    """
    code: str
    detail: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    cause: Optional[BaseException] = None

    def __post_init__(self) -> None:
        if not isinstance(self.code, str) or not self.code.strip():
            raise ValueError("AppError.code must be a non-empty string")
        self.code = self.code.strip().upper()

        if self.detail is None:
            self.detail = ""
        else:
            self.detail = str(self.detail).strip()

        if self.meta is None:
            self.meta = {}
        elif not isinstance(self.meta, dict):
            raise ValueError("AppError.meta must be a dict")

    @property
    def error_code(self) -> str:
        """
        기존 코드와의 호환용 alias.
        일부 기존 로직이 e.error_code를 볼 수도 있으므로 같이 제공.
        """
        return self.code

    def with_detail(self, detail: str) -> "AppError":
        """
        detail만 바꾼 새 AppError 반환.
        """
        return AppError(
            code=self.code,
            detail=detail,
            meta=dict(self.meta),
            cause=self.cause,
        )

    def with_meta(self, **kwargs: Any) -> "AppError":
        """
        meta를 덧붙인 새 AppError 반환.
        """
        merged = dict(self.meta)
        merged.update(kwargs)
        return AppError(
            code=self.code,
            detail=self.detail,
            meta=merged,
            cause=self.cause,
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        로그/디버깅용 dict 변환.
        실제 응답 payload는 별도 error_payload.py에서 만드는 것을 권장.
        """
        data = {
            "error_code": self.code,
            "detail": self.detail,
            "meta": dict(self.meta),
        }
        if self.cause is not None:
            data["cause_type"] = type(self.cause).__name__
            data["cause"] = str(self.cause)
        return data

    def __str__(self) -> str:
        if self.detail:
            return f"{self.code}: {self.detail}"
        return self.code

    def __repr__(self) -> str:
        return (
            f"AppError(code={self.code!r}, "
            f"detail={self.detail!r}, "
            f"meta={self.meta!r}, "
            f"cause={self.cause!r})"
        )


def ensure_app_error(
    exc: BaseException,
    *,
    default_code: str = "E999",
    detail: str | None = None,
    meta: Optional[Dict[str, Any]] = None,
) -> AppError:
    """
    어떤 예외가 들어오더라도 AppError로 정규화한다.

    사용 예:
        try:
            ...
        except Exception as e:
            raise ensure_app_error(e, default_code="E999")
    """
    if isinstance(exc, AppError):
        merged_meta = dict(exc.meta)
        if meta:
            merged_meta.update(meta)

        final_detail = exc.detail
        if detail is not None and str(detail).strip():
            final_detail = str(detail).strip()

        return AppError(
            code=exc.code,
            detail=final_detail,
            meta=merged_meta,
            cause=exc.cause if exc.cause is not None else exc,
        )

    return AppError(
        code=default_code,
        detail=(str(detail).strip() if detail is not None else str(exc).strip()),
        meta=dict(meta or {}),
        cause=exc,
    )
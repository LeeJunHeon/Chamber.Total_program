from __future__ import annotations

import re
from typing import Iterable, Optional

from .error_registry import (
    ERROR_REGISTRY,
    ErrorDefinition,
    get_error_definition,
)

_ERROR_CODE_RE = re.compile(r"\b(E\d{3})\b", re.IGNORECASE)


class ErrorCatalog:
    """
    새 구조용 에러 카탈로그.

    역할:
    - 에러코드 존재 여부 확인
    - 에러코드 정규화
    - registry에서 에러 정의 조회
    - 문자열 안에 직접 포함된 E### 코드만 추출

    주의:
    - cause 문자열 최장 매칭 같은 "추정 로직"은 사용하지 않는다.
    - 에러코드는 예외 발생 지점에서 AppError로 직접 결정하는 것을 원칙으로 한다.
    """

    def has(self, code: str | None) -> bool:
        if not code:
            return False
        return str(code).strip().upper() in ERROR_REGISTRY

    def normalize_code(self, code: str | None, *, fallback: str = "E999") -> str:
        if not code:
            return fallback

        norm = str(code).strip().upper()
        if norm.isdigit() and len(norm) == 3:
            norm = f"E{norm}"

        if norm in ERROR_REGISTRY:
            return norm
        return fallback

    def get(self, code: str | None, *, fallback: str = "E999") -> ErrorDefinition:
        norm = self.normalize_code(code, fallback=fallback)
        return get_error_definition(norm)

    def extract_code_from_text(self, text: str | None) -> Optional[str]:
        """
        문자열 안에 명시적으로 E### 코드가 들어있는 경우에만 추출.
        의미 추정은 하지 않는다.
        """
        if not text:
            return None

        match = _ERROR_CODE_RE.search(str(text).strip())
        if not match:
            return None

        code = match.group(1).upper()
        if code in ERROR_REGISTRY:
            return code
        return None

    def is_fallback(self, code: str | None) -> bool:
        return self.get(code).is_fallback

    def all_codes(self) -> tuple[str, ...]:
        return tuple(ERROR_REGISTRY.keys())

    def all_definitions(self) -> Iterable[ErrorDefinition]:
        return ERROR_REGISTRY.values()


CATALOG = ErrorCatalog()


def has_error_code(code: str | None) -> bool:
    return CATALOG.has(code)


def normalize_error_code(code: str | None, *, fallback: str = "E999") -> str:
    return CATALOG.normalize_code(code, fallback=fallback)


def get_error_info(code: str | None, *, fallback: str = "E999") -> ErrorDefinition:
    return CATALOG.get(code, fallback=fallback)


def extract_error_code(text: str | None) -> Optional[str]:
    return CATALOG.extract_code_from_text(text)


def is_fallback_error(code: str | None) -> bool:
    return CATALOG.is_fallback(code)


__all__ = [
    "ErrorCatalog",
    "CATALOG",
    "has_error_code",
    "normalize_error_code",
    "get_error_info",
    "extract_error_code",
    "is_fallback_error",
]
# util/error_catalog.py
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Dict, Optional, Iterable

from .error_codes_default import DEFAULT_CODES

@dataclass(frozen=True)
class ErrorInfo:
    code: str
    cause: str
    fix: str

class ErrorCatalog:
    """
    - DEFAULT_CODES 를 기본으로 사용
    - message 안에 E### 가 있으면 그 코드 사용
    - 없으면 cause 텍스트 매칭으로 '추정' (최장 매칭)
    """

    def __init__(self) -> None:
        self._codes: Dict[str, Dict[str, str]] = dict(DEFAULT_CODES)

    def get(self, code: str, *, default_message: str = "") -> ErrorInfo:
        info = self._codes.get(code)
        if info:
            return ErrorInfo(code=code, cause=info.get("cause", ""), fix=info.get("fix", ""))
        return ErrorInfo(
            code=code,
            cause=default_message or f"Unmapped error code: {code}",
            fix="코드 매핑이 없습니다. 서버 로그/스택트레이스를 확인 후 메뉴얼에 코드 추가",
        )

    def guess_code(self, message: str) -> Optional[str]:
        if not message:
            return None

        msg = str(message).strip()

        # 1) 직접 코드가 포함된 경우
        m = re.search(r"\b(E\d{3})\b", msg)
        if m:
            return m.group(1)

        # 2) cause 최장 매칭
        best_code = None
        best_len = 0
        for code, info in self._codes.items():
            cause = (info.get("cause") or "").strip()
            if not cause:
                continue

            # cause 그대로 포함
            if cause in msg and len(cause) > best_len:
                best_code, best_len = code, len(cause)

            # msg가 cause에 포함(짧은 메시지)
            if msg in cause and len(msg) > best_len:
                best_code, best_len = code, len(msg)

            # ':' prefix 매칭 보정
            if ":" in cause:
                prefix = cause.split(":", 1)[0]
                if prefix in msg and len(prefix) > best_len:
                    best_code, best_len = code, len(prefix)

        return best_code

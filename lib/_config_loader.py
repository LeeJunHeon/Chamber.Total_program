# lib/_config_loader.py
# -*- coding: utf-8 -*-
"""
설정 JSON 로더
==============
config/settings.json을 읽어서 기존 config_common / config_ch1 / config_ch2
모듈 변수에 안전하게 덮어쓴다.

원칙
----
- .py 파일의 하드코딩 값이 **항상 먼저 로드**된다 (안전망).
- JSON 값이 존재하고, 타입이 기존 값과 호환되면 **덮어쓴다**.
- JSON이 없거나, 키가 잘못되었거나, 타입이 다르면 **기존 .py 값을 유지**하고 로그를 남긴다.
- JSON에서 못 다루는 값(Path, bytes, lambda 등)은 .py에서만 관리한다.

사용법 (main.py 시작부에서 호출)
---------------------------------
    from lib._config_loader import load_settings
    load_settings()
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────
# JSON 경로 결정
# ──────────────────────────────────────────────────────────

def _settings_path() -> Path:
    """
    settings.json 위치:
      - PyInstaller exe: exe가 있는 폴더/config/settings.json
      - 소스 실행:       프로젝트 루트/config/settings.json
    """
    if getattr(sys, "frozen", False):
        base = Path(sys.executable).resolve().parent
    else:
        # lib/_config_loader.py → parents[1] = 프로젝트 루트
        base = Path(__file__).resolve().parents[1]
    return base / "config" / "settings.json"


# ──────────────────────────────────────────────────────────
# 타입 호환 검사
# ──────────────────────────────────────────────────────────

# int와 float은 서로 호환 (JSON에서 1.0 → int로 쓰거나 그 반대)
_NUMERIC = (int, float)


def _type_compatible(old_val: Any, new_val: Any) -> bool:
    """기존 값과 새 값의 타입이 호환되는지 확인."""

    # 기존 값이 None이면 어떤 JSON 값이든 허용
    if old_val is None:
        return True

    # int ↔ float 호환
    if isinstance(old_val, _NUMERIC) and isinstance(new_val, _NUMERIC):
        return True

    # 같은 타입
    if type(old_val) is type(new_val):
        return True

    # list ↔ tuple 호환
    if isinstance(old_val, (list, tuple)) and isinstance(new_val, (list, tuple)):
        return True

    return False


# ──────────────────────────────────────────────────────────
# dict 키 변환 (MFC_SCALE_FACTORS 등 int-key dict 처리)
# ──────────────────────────────────────────────────────────

def _convert_dict_keys(old_val: Any, new_val: Any) -> Any:
    """
    기존 값이 {int: ...} dict이고 JSON 값이 {"str": ...} dict이면
    키를 int로 변환해서 반환한다.
    예: 기존 {1: 1.0, 2: 10.0} / JSON {"1": 1.0, "2": 10.0}
        → {1: 1.0, 2: 10.0}
    """
    if not isinstance(old_val, dict) or not isinstance(new_val, dict):
        return new_val

    if not old_val:
        return new_val

    # 기존 키가 전부 int이고, 새 키가 전부 str(숫자)이면 변환
    old_keys_are_int = all(isinstance(k, int) for k in old_val)
    if old_keys_are_int:
        converted = {}
        for k, v in new_val.items():
            try:
                converted[int(k)] = v
            except (ValueError, TypeError):
                converted[k] = v
        return converted

    return new_val


# ──────────────────────────────────────────────────────────
# 섹션 → 모듈 적용
# ──────────────────────────────────────────────────────────

def _apply_section(module: Any, section_data: Dict[str, Any], section_name: str) -> tuple[int, int, int]:
    """
    JSON 섹션의 값들을 모듈에 적용한다.

    Returns: (applied, skipped, errors)
    """
    applied = 0
    skipped = 0
    errors = 0

    for key, new_val in section_data.items():
        # __section__으로 시작하는 키는 JSON 주석용 → 무시
        if key.startswith("__"):
            continue

        # 모듈에 해당 변수가 없으면 스킵
        if not hasattr(module, key):
            logger.debug("[ConfigLoader] %s.%s: 모듈에 없는 키 → 무시", section_name, key)
            skipped += 1
            continue

        old_val = getattr(module, key)

        # JSON으로 관리 불가능한 타입(Path, bytes, callable)은 건드리지 않음
        if isinstance(old_val, (Path, bytes)) or callable(old_val):
            logger.debug("[ConfigLoader] %s.%s: 비-JSON 타입(%s) → 무시",
                         section_name, key, type(old_val).__name__)
            skipped += 1
            continue

        # 타입 호환 검사
        if not _type_compatible(old_val, new_val):
            logger.warning(
                "[ConfigLoader] %s.%s: 타입 불일치 (기존=%s[%s], JSON=%s[%s]) → 기존 값 유지",
                section_name, key,
                repr(old_val), type(old_val).__name__,
                repr(new_val), type(new_val).__name__,
            )
            errors += 1
            continue

        # dict 키 변환 (int-key dict 처리)
        if isinstance(old_val, dict) and isinstance(new_val, dict):
            new_val = _convert_dict_keys(old_val, new_val)

        # 적용
        try:
            setattr(module, key, new_val)
            applied += 1
        except Exception as e:
            logger.warning("[ConfigLoader] %s.%s: setattr 실패: %s → 기존 값 유지",
                           section_name, key, e)
            errors += 1

    return applied, skipped, errors


# ──────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────

def load_settings(path: Optional[Path] = None) -> Dict[str, Any]:
    """
    settings.json을 읽어서 config 모듈들에 적용한다.

    - config_common ← JSON["common"]
    - config_ch1    ← JSON["ch1"]
    - config_ch2    ← JSON["ch2"]

    Returns: 로드된 JSON dict (빈 dict이면 로드 실패)
    """
    json_path = path or _settings_path()

    # ── JSON 파일 읽기 ──
    if not json_path.exists():
        logger.info("[ConfigLoader] %s 없음 → 기존 .py 값 사용", json_path)
        return {}

    try:
        raw = json_path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error("[ConfigLoader] JSON 파싱 실패: %s → 기존 .py 값 사용", e)
        return {}
    except Exception as e:
        logger.error("[ConfigLoader] 파일 읽기 실패: %s → 기존 .py 값 사용", e)
        return {}

    if not isinstance(data, dict):
        logger.error("[ConfigLoader] JSON 최상위가 dict가 아님 → 기존 .py 값 사용")
        return {}

    logger.info("[ConfigLoader] 설정 로드: %s", json_path)

    # ── 각 섹션 적용 ──
    from lib import config_common
    from lib import config_ch1
    from lib import config_ch2

    section_map = {
        "common": (config_common, "config_common"),
        "ch1":    (config_ch1,    "config_ch1"),
        "ch2":    (config_ch2,    "config_ch2"),
    }

    total_applied = 0
    total_errors = 0

    for section_key, (module, module_name) in section_map.items():
        section_data = data.get(section_key)
        if not isinstance(section_data, dict):
            continue

        applied, skipped, errors = _apply_section(module, section_data, module_name)
        total_applied += applied
        total_errors += errors

        if applied > 0 or errors > 0:
            logger.info(
                "[ConfigLoader] %s: 적용 %d개, 스킵 %d개, 오류 %d개",
                module_name, applied, skipped, errors,
            )

    if total_errors > 0:
        logger.warning("[ConfigLoader] 총 %d개 오류 발생 — 해당 값은 기존 .py 값으로 유지됨", total_errors)

    logger.info("[ConfigLoader] 완료: 총 %d개 값 적용", total_applied)
    return data
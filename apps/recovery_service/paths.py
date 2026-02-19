# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path

def project_root() -> Path:
    # .../apps/recovery_service/paths.py -> parents[2] == 프로젝트 루트(main.py 있는 폴더)
    return Path(__file__).resolve().parents[2]

def recovery_service_dir() -> Path:
    return Path(__file__).resolve().parent

def recovery_state_base_dir() -> Path:
    """
    복구 데이터 베이스 디렉토리.
    - 환경변수 우선(launcher가 넘겨줌)
    - 없으면 apps/recovery_service/recovery_state
    """
    env = os.environ.get("CH12_RECOVERY_BASE_DIR", "").strip()
    if env:
        p = Path(env).expanduser()
        if not p.is_absolute():
            p = (project_root() / p).resolve()
        return p.resolve()

    return (recovery_service_dir() / "recovery_state").resolve()

def recovery_state_dir() -> Path:
    return (recovery_state_base_dir() / "state").resolve()

def recovery_logs_dir() -> Path:
    return (recovery_state_base_dir() / "logs").resolve()

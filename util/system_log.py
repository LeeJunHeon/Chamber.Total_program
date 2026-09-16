# util/system_log.py
# -*- coding: utf-8 -*-
"""
어디에도 귀속되지 않는(공정 로그 파일이 열려 있지 않은) 로그를
일자별 SYSTEM 파일에 남기는 공통 헬퍼.

- 정본: cfgc.LOG_ROOT_DIR / "SYSTEM" / "YYYYMMDD.log"
- 폴백: cfgc.LOCAL_FALLBACK_SYSTEM_DIR / "YYYYMMDD.log"
- 경로는 DEC-033 패턴대로 '호출 시점'에 getattr 로 읽는다.
- 동기 쓰기. asyncio 를 쓰지 않는다.
  (loop.create_task(asyncio.to_thread(...)) 경로는 종료 시퀀스에서
   executor 가 내려간 뒤 pending 으로 버려져 마지막 줄을 잃는다.)
- 어떤 예외도 밖으로 내보내지 않는다. 로그 실패가 공정을 막으면 안 된다.
"""
from __future__ import annotations

import sys
import threading
from datetime import datetime
from pathlib import Path

_WRITE_LOCK = threading.Lock()


def _exe_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def system_log_dirs() -> tuple[Path, Path]:
    """(정본, 폴백) SYSTEM 디렉터리."""
    from lib import config_common as _cc

    primary = Path(getattr(_cc, "LOG_ROOT_DIR", r"C:\VanaM_Logs\CH1&2")) / "SYSTEM"
    fb = getattr(_cc, "LOCAL_FALLBACK_SYSTEM_DIR", None)
    if fb is None:
        fb = _exe_base_dir() / "Logs_LocalFallback" / "SYSTEM"
    return primary, Path(fb)


def system_log_write_sync(dirs: tuple[Path, Path], fname: str, line: str) -> None:
    """정본 → 실패 시 폴백. append 모드로 열고 쓰고 닫는다(날짜 변경 자동 처리)."""
    for d in dirs:
        try:
            d.mkdir(parents=True, exist_ok=True)
            with open(d / fname, "a", encoding="utf-8") as f:
                f.write(line)
            return
        except Exception:
            continue


def system_log_append(source: str, msg: str) -> None:
    """SYSTEM 로그 한 줄 추가. 어떤 예외도 밖으로 내보내지 않는다."""
    try:
        now = datetime.now()
        line = f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [{source}] {msg}\n"
        fname = f"{now.strftime('%Y%m%d')}.log"
        dirs = system_log_dirs()
    except Exception:
        return

    try:
        with _WRITE_LOCK:
            system_log_write_sync(dirs, fname, line)
    except Exception:
        pass


__all__ = ["system_log_append", "system_log_dirs", "system_log_write_sync"]

"""SYSTEM 일자 로그 — v1 util/system_log.py 의 파일 출력(LOG-021·022).

- 정본 {LOG_ROOT_DIR}\\SYSTEM\\YYYYMMDD.log, 실패 시 {LOCAL_FALLBACK_ROOT}\\SYSTEM\\YYYYMMDD.log
- 줄 '[YYYY-mm-dd HH:MM:SS] [{출처}] {메시지}\\n', utf-8, append 로 열고 쓰고 닫음(v1 과 같은 open — 줄 끝 변환도 같다:
  윈도우에서는 텍스트 모드라 \\r\\n). 예외는 밖으로 내지 않는다.
- 남는 것: 챔버 txt 가 열려 있지 않을 때의 챔버 로그(출처 'CH{n}:{src}'), 공정 무관 줄(append_side_log), 방송 줄, HOSTLOG.
- v1 은 부른 스레드에서 동기 쓰기(락)였다 — v2 는 시각·줄을 부른 쪽에서 만들고 쓰기는 쓰기 스레드에서(순서 같음).

부르는 법: SystemLog(cfg, writer=..., now=...).append(source, msg)
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from v2.ch12.outputs.base import Owned, QueueWriter, wall_now
from v2.ch12.outputs.paths import Paths


def write_sync(dirs: Sequence[Path], fname: str, line: str) -> Optional[Path]:
    """정본 → 실패 시 폴백(v1 system_log_write_sync). 쓴 파일(실패면 None)."""
    for d in dirs:
        try:
            Path(d).mkdir(parents=True, exist_ok=True)
            with open(Path(d) / fname, "a", encoding="utf-8") as f:
                f.write(line)
            return Path(d) / fname
        except Exception:
            continue
    return None


def format_line(now: datetime, source: str, msg: str) -> "tuple[str, str]":
    """(파일 이름, 줄)."""
    return f"{now.strftime('%Y%m%d')}.log", f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [{source}] {msg}\n"


class SystemLog(Owned):
    def __init__(self, cfg: Optional[Mapping[str, Any]] = None, *, writer: Optional[QueueWriter] = None,
                 now: Callable[[], datetime] = wall_now) -> None:
        super().__init__(writer, "system-log")
        self.paths = Paths(cfg)
        self.now = now

    def append(self, source: str, msg: str) -> None:
        """한 줄 추가(큐에만). 어떤 예외도 밖으로 내지 않는다."""
        try:
            fname, line = format_line(self.now(), source, msg)
            dirs = self.paths.system_dirs()
        except Exception:
            return
        self.writer.put(write_sync, dirs, fname, line)

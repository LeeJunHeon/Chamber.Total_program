# lib/thickness_logger.py
# -*- coding: utf-8 -*-
"""두께 모니터 연속 기록 — 그 채널 공정 중에만(ThicknessService.set_recording) THICKNESS_LOG_DIR\\CH{n}\\Thickness_YYYYMMDD.csv 에 1초 1행.
공정이 끝나면 파일을 닫고, 다음 공정에서 다시 연다(같은 날이면 같은 파일에 이어 씀).

날짜가 바뀌면 새 파일. 같은 날 파일이 이미 있으면 이어 쓴다(머리글은 새 파일에만).
로컬 디스크라 동기 쓰기(매 행 flush). 실패는 경고 1회 후 60초 뒤 다시 연다 — 폴링을 멈추게 하지 않는다.
열 수 없으면 LOCAL_FALLBACK_ROOT\\Thickness\\CH{n} 에 쓴다. (log_sync 워커가 C:\\VanaM_Logs\\CH1&2 를 NAS 로 복사한다)
"""
from __future__ import annotations

import csv
import datetime
import time
from pathlib import Path
from typing import Callable, Optional

COLUMNS = ["timestamp", "connected", "thickness_nm", "rate_nm_s", "freq_hz", "life_pct",
           "crystal_ok", "stale", "process_running", "process_name"]
REOPEN_RETRY_S = 60.0


def _fallback_dir(ch: int) -> Path:
    from lib import config_common as cc
    root = getattr(cc, "LOCAL_FALLBACK_ROOT", None)
    return (Path(root) if root else Path.cwd() / "Logs_LocalFallback") / "Thickness" / f"CH{int(ch)}"


def _b(v) -> str:
    return "" if v is None else ("1" if v else "0")


def _n(v, fmt: str) -> str:
    if v is None:
        return ""
    try:
        return fmt.format(float(v))
    except Exception:
        return ""


class ThicknessCsvLogger:
    def __init__(self, log_dir: str, ch: int, *, warn: Optional[Callable[[str], None]] = None,
                 clock: Callable[[], float] = time.monotonic):
        self._root = str(log_dir or "")
        self._ch = int(ch)
        self._warn_cb = warn
        self._now = clock
        self._fp = None
        self._writer = None
        self._day: Optional[str] = None
        self._path: Optional[Path] = None
        self._retry_at = 0.0
        self._warned = False

    @property
    def path(self) -> Optional[Path]:
        return self._path

    def is_open(self) -> bool:
        return self._fp is not None

    def _warn(self, text: str) -> None:
        if self._warned:
            return
        self._warned = True
        if self._warn_cb:
            try:
                self._warn_cb(text)
            except Exception:
                pass

    def _open(self, day: str) -> bool:
        cands = [Path(self._root) / f"CH{self._ch}"] if self._root else []
        cands.append(_fallback_dir(self._ch))
        for i, d in enumerate(cands):
            try:
                d.mkdir(parents=True, exist_ok=True)
                path = d / f"Thickness_{day}.csv"
                new = not path.exists() or path.stat().st_size == 0
                fp = open(path, "a", encoding="utf-8-sig" if new else "utf-8", newline="")
                w = csv.writer(fp)
                if new:
                    w.writerow(COLUMNS)
                    fp.flush()
            except Exception as e:
                if i == len(cands) - 1:
                    self._warn(f"두께 기록 CSV 를 열 수 없어 기록하지 않습니다(60초 뒤 재시도): {e!r}")
                continue
            if i > 0 and cands[0] != d:
                self._warn(f"두께 기록 폴더({cands[0]})를 쓸 수 없어 {d} 에 저장합니다")
            self._fp, self._writer, self._path, self._day = fp, w, path, day
            return True
        return False

    def write_row(self, st: dict, wall: Optional[datetime.datetime] = None) -> None:
        try:
            now_w = wall or datetime.datetime.now()
            day = now_w.strftime("%Y%m%d")
            if self._fp is not None and day != self._day:
                self.close()                 # 날짜가 바뀜 → 새 파일
            if self._fp is None:
                if self._now() < self._retry_at:
                    return
                if not self._open(day):
                    self._retry_at = self._now() + REOPEN_RETRY_S
                    return
            self._writer.writerow([
                now_w.strftime("%Y-%m-%d %H:%M:%S"),
                _b(st.get("connected")),
                _n(st.get("thickness_nm"), "{:.2f}"),
                _n(st.get("rate_nm_s"), "{:.3f}"),
                "" if st.get("freq_hz") is None else str(int(st.get("freq_hz"))),
                _n(st.get("life_pct"), "{:.1f}"),
                _b(st.get("crystal_ok")),
                _b(st.get("stale")),
                _b(st.get("process_running")),
                str(st.get("process_name") or ""),
            ])
            self._fp.flush()
        except Exception as e:
            self._warn(f"두께 기록 CSV 쓰기 실패(60초 뒤 다시 엶): {e!r}")
            self.close()
            self._retry_at = self._now() + REOPEN_RETRY_S

    def close(self) -> None:
        fp, self._fp, self._writer = self._fp, None, None
        if fp is not None:
            try:
                fp.close()
            except Exception:
                pass

"""두께 연속 CSV — v1 lib/thickness_logger.py(ThicknessCsvLogger) 의 파일 출력(LOG-064~067).

v1 그대로
- CHn_THICKNESS_LOG_DIR(기본 C:\\VanaM_Logs\\CH1&2\\Thickness, G: 면 기본값)\\CH{n}\\Thickness_YYYYMMDD.csv. 그 채널 공정 중에만
  (set_recording) 1초 1행, 시작 즉시 1행. 열 timestamp, connected(1/0/빈), thickness_nm '%.2f', rate_nm_s '%.3f', freq_hz 정수,
  life_pct '%.1f', crystal_ok, stale, process_running(1/0), process_name.
- 새 파일(없거나 0바이트)만 utf-8-sig + 머리글, 이어 쓸 때는 utf-8, 매 행 flush, 날짜가 바뀌면 새 파일, 공정 끝나면 닫음.
  열기 실패면 LOCAL_FALLBACK_ROOT\\Thickness\\CH{n}('두께 기록 폴더(..)를 쓸 수 없어 .. 에 저장합니다'), 둘 다 실패·쓰기 실패면
  경고 1회 후 60 s 뒤 다시 엶.
- 공정명 = process_name → Process_name → process_note(set_recording(True, 이름) — chamber_runtime.py:6756-6759).

부르는 법
  t = ThicknessCsv(ch, cfg, writer=w, warn=fn, monotonic=clock.monotonic)
  t.set_recording(True, name, st, wall)   # 공정 시작(즉시 1행) / set_recording(False) — 닫음
  t.on_tick(st: dict, wall: datetime)     # 1초마다(기록 중일 때만 행). st 키: connected, thickness_nm, rate_nm_s,
                                          # freq_hz, life_pct, crystal_ok, stale (process_running·process_name 은 여기서 채움)
"""
from __future__ import annotations

import csv
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from v2.ch12.outputs.base import Owned, QueueWriter
from v2.ch12.outputs.paths import Paths

COLUMNS = ["timestamp", "connected", "thickness_nm", "rate_nm_s", "freq_hz", "life_pct",
           "crystal_ok", "stale", "process_running", "process_name"]
REOPEN_RETRY_S = 60.0


def _b(v) -> str:
    return "" if v is None else ("1" if v else "0")


def _n(v, fmt: str) -> str:
    if v is None:
        return ""
    try:
        return fmt.format(float(v))
    except Exception:
        return ""


class ThicknessFile:
    """파일 부분(v1 ThicknessCsvLogger 와 같은 동작) — 쓰기 스레드 안에서만 쓴다."""

    def __init__(self, root: str, ch: int, fallback: Path, warn: Callable[[str], None],
                 clock: Callable[[], float]) -> None:
        self._root = str(root or "")
        self._ch = int(ch)
        self._fallback = Path(fallback)
        self._warn_cb = warn
        self._now = clock
        self._fp = None
        self._writer = None
        self._day: Optional[str] = None
        self.path: Optional[Path] = None
        self._retry_at = 0.0
        self._warned = False

    def _warn(self, text: str) -> None:
        if self._warned:
            return
        self._warned = True
        try:
            self._warn_cb(text)
        except Exception:
            pass

    def _open(self, day: str) -> bool:
        cands = [Path(self._root) / f"CH{self._ch}"] if self._root else []
        cands.append(self._fallback)
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
            self._fp, self._writer, self.path, self._day = fp, w, path, day
            return True
        return False

    def write_row(self, st: Mapping[str, Any], now_w: datetime, mono: float) -> None:
        try:
            day = now_w.strftime("%Y%m%d")
            if self._fp is not None and day != self._day:
                self.close()
            if self._fp is None:
                if mono < self._retry_at:
                    return
                if not self._open(day):
                    self._retry_at = mono + REOPEN_RETRY_S
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
            self._retry_at = mono + REOPEN_RETRY_S

    def close(self) -> None:
        fp, self._fp, self._writer = self._fp, None, None
        if fp is not None:
            try:
                fp.close()
            except Exception:
                pass


class ThicknessCsv(Owned):
    def __init__(self, ch: int, cfg: Optional[Mapping[str, Any]] = None, *, writer: Optional[QueueWriter] = None,
                 warn: Optional[Callable[[str], None]] = None,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        super().__init__(writer, f"thickness-csv-ch{int(ch)}")
        self.ch = int(ch)
        paths = Paths(cfg)
        self.monotonic = monotonic
        self.file = ThicknessFile(paths.thickness_root(self.ch), self.ch, paths.thickness_fallback(self.ch),
                                  warn or (lambda m: None), monotonic)
        self.recording = False
        self.process_name = ""

    def set_recording(self, on: bool, process_name: str = "", st: Optional[Mapping[str, Any]] = None,
                      wall: Optional[datetime] = None) -> None:
        if on:
            self.recording = True
            self.process_name = str(process_name or "")
            if st is not None:
                self.on_tick(st, wall or datetime.now())
        else:
            self.recording = False
            self.writer.put(self.file.close)

    def on_tick(self, st: Mapping[str, Any], wall: datetime) -> None:
        if not self.recording:
            return
        row = dict(st)
        row["process_running"] = True
        row["process_name"] = self.process_name
        self.writer.put(self.file.write_row, row, wall, self.monotonic())

    def stop(self, timeout_s: float = 5.0) -> None:
        self.writer.put(self.file.close)
        self.stop_writer(timeout_s)

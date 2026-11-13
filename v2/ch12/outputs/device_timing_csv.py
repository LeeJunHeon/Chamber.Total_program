"""장비 응답 시간 1분 집계 CSV(2026-11-13 D409) — {LOCAL_DATA_DIR}/device_timing/timing_YYYYMMDD.csv.

DEVICE_TIMING_LOG_INTERVAL_S(기본 60)마다 모음통(v2.common.timing_stats)을 비워 장비·명령별 개수·최소·평균·최대·p95·시간 초과 수를 한 줄씩.
헤더 Timestamp,Device,Cmd,Count,Min_ms,Avg_ms,Max_ms,P95_ms,Timeouts (utf-8-sig). 집계·파일 쓰기는 기록 스레드(QueueWriter) — 통신 경로는 deque.append 뿐.
DEVICE_TIMING_RETENTION_DAYS 보다 오래된 날짜 파일은 날짜가 바뀔 때 지운다. sim 은 LOCAL_DATA_DIR 이 sim_out 아래로 강제되므로 sim_out 아래.
최근 집계는 latest(웹 진단 화면용 — 허브가 읽는다)에 남긴다.
"""
from __future__ import annotations

import csv
import io
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional

from v2.ch12.outputs.base import Owned, QueueWriter
from v2.common import timing_stats as T

HEADER = ["Timestamp", "Device", "Cmd", "Count", "Min_ms", "Avg_ms", "Max_ms", "P95_ms", "Timeouts"]
_NAME = re.compile(r"^timing_(\d{8})\.csv$")


def _fmt(v: Any) -> str:
    return "" if v is None else f"{float(v):.1f}"


class DeviceTimingCsv(Owned):
    def __init__(self, cfg: Optional[Mapping[str, Any]] = None, *, stats: Optional[T.TimingStats] = None,
                 writer: Optional[QueueWriter] = None, log: Optional[Callable[[str], None]] = None) -> None:
        super().__init__(writer, "device-timing-csv")
        self.cfg = cfg if cfg is not None else {}
        self._log_fn = log
        self.stats = stats if stats is not None else T.TimingStats()
        self._next: Optional[float] = None
        self._day: Optional[str] = None
        self.latest: List[Dict[str, Any]] = []              # 가장 최근 집계(화면용)
        self.latest_ts: Optional[str] = None
        self.rows_written = 0

    def web_summary(self) -> Optional[Dict[str, Any]]:
        """웹 진단 화면용 — 가장 최근 집계를 장비별로 줄인 것(장비의 명령 중 p95·최대가 가장 큰 값, 개수·시간 초과는 합). 아직 집계 전이면 None."""
        if not self.latest:
            return None
        by: Dict[str, Dict[str, Any]] = {}
        for r in self.latest:
            d = by.setdefault(r["device"], {"device": r["device"], "count": 0, "timeouts": 0, "p95Ms": None, "maxMs": None, "worstCmd": None})
            d["count"] += int(r["count"])
            d["timeouts"] += int(r["timeouts"])
            if r["p95_ms"] is not None and (d["p95Ms"] is None or r["p95_ms"] > d["p95Ms"]):
                d["p95Ms"], d["worstCmd"] = round(float(r["p95_ms"]), 1), r["cmd"]
            if r["max_ms"] is not None and (d["maxMs"] is None or r["max_ms"] > d["maxMs"]):
                d["maxMs"] = round(float(r["max_ms"]), 1)
        return {"at": self.latest_ts, "intervalS": float(self.cfg.get("DEVICE_TIMING_LOG_INTERVAL_S", 60.0)), "devices": list(by.values())}

    def _enabled(self) -> bool:
        return bool(self.cfg.get("DEVICE_TIMING_LOG_ENABLED", True))

    def directory(self) -> Path:
        return Path(str(self.cfg.get("LOCAL_DATA_DIR", "."))) / "device_timing"

    def on_tick(self, ts: datetime, row: Mapping[str, Any], mono: float) -> None:
        if not self._enabled():
            self._next = None
            self.stats.drain()                              # 꺼져 있는 동안은 쌓이지 않게
            return
        period = max(1.0, float(self.cfg.get("DEVICE_TIMING_LOG_INTERVAL_S", 60.0)))
        if self._next is None:
            self._next = mono + period
            return
        if mono < self._next - 1e-6:
            return
        self._next = mono + period
        samples = self.stats.drain()
        if not samples:
            return
        rows = T.summarize(samples)
        self.latest, self.latest_ts = rows, ts.strftime("%Y-%m-%d %H:%M:%S")
        self.writer.put(self._append, ts, rows)

    def _append(self, ts: datetime, rows: List[Dict[str, Any]]) -> None:
        try:
            d = self.directory()
            d.mkdir(parents=True, exist_ok=True)
            day = ts.strftime("%Y%m%d")
            path = d / f"timing_{day}.csv"
            new = not path.exists() or path.stat().st_size == 0
            buf = io.StringIO()
            w = csv.writer(buf, lineterminator="\r\n")
            if new:
                w.writerow(HEADER)
            stamp = ts.strftime("%Y-%m-%d %H:%M:%S")
            for r in rows:
                w.writerow([stamp, r["device"], r["cmd"], r["count"], _fmt(r["min_ms"]), _fmt(r["avg_ms"]), _fmt(r["max_ms"]),
                            _fmt(r["p95_ms"]), r["timeouts"]])
            with open(path, "a", encoding="utf-8-sig" if new else "utf-8", newline="") as f:
                f.write(buf.getvalue())
            self.rows_written += len(rows)
            if day != self._day:
                self._day = day
                self._prune(d, ts)
        except Exception as e:
            if self._log_fn:
                self._log_fn(f"장비 응답 시간 CSV 기록 실패: {e!r}")

    def _prune(self, d: Path, ts: datetime) -> None:
        keep = float(self.cfg.get("DEVICE_TIMING_RETENTION_DAYS", 14.0))
        limit = ts.timestamp() - keep * 86400.0
        for f in d.iterdir():
            m = _NAME.match(f.name)
            if not m:
                continue
            try:
                t = time.mktime(time.strptime(m.group(1), "%Y%m%d")) + 86400.0
                if t < limit:
                    f.unlink()
            except (OSError, ValueError):
                continue

    def stop(self) -> None:
        pass

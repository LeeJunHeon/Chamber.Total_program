"""GPC3000(MFC) 압력·쓰로틀 밸브 위치 상시 CSV — v1 controller/mfc_state_logger.py(main 3112ea5·3992a58)의 파일 출력.

{CH1|CH2}_YYYYMMDD.csv (MFC_STATE_LOG_DIR, 폴백 LOCAL_FALLBACK_ROOT\\CH1&2_MFC), utf-8-sig, 헤더 Timestamp,Pressure_mTorr,Valve_pct.
Timestamp 는 밀리초까지(YYYY-MM-DD HH:MM:SS.mmm). 읽기 실패한 값은 빈 칸, 둘 다 없으면 그 초는 행을 쓰지 않는다. 공정과 무관하게 MFC_STATE_LOG_INTERVAL_S
(v2 최소 1 s)마다. v1 은 따로 저우선 읽기(R5·R6)를 했지만 v2 는 1초 행의 값(mfc.pressure_mtorr·mfc.valve_pct)에서 뽑는다 — 장비에 질의를 더 보내지 않는다.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Mapping, Optional

from v2.ch12.outputs.base import Owned, QueueWriter
from v2.ch12.outputs.log_hub import DailyCsvListAppender
from v2.ch12.outputs.paths import Paths
from v2.ch12.params import outputs as P

HEADER = ["Timestamp", "Pressure_mTorr", "Valve_pct"]


class MfcStateCsv(Owned):
    def __init__(self, cfg: Optional[Mapping[str, Any]] = None, *, writer: Optional[QueueWriter] = None,
                 log: Optional[Callable[[str], None]] = None, monotonic: Optional[Callable[[], float]] = None) -> None:
        super().__init__(writer, "mfc-state-csv")
        self.cfg = cfg if cfg is not None else {}
        self.paths = Paths(self.cfg)
        self._log_fn = log
        primary, fb = self.paths.mfc_state_dirs()
        self._apps = {ch: DailyCsvListAppender(primary_dir=primary, fallback_dir=fb,
                                               filename_builder=lambda dt, ch=ch: f"{ch.upper()}_{dt:%Y%m%d}.csv",
                                               encoding="utf-8-sig", retry_primary_every_s=10.0,
                                               monotonic=monotonic or __import__("time").monotonic) for ch in ("ch1", "ch2")}
        self._next = {"ch1": None, "ch2": None}
        self.rows_queued = 0

    def on_tick(self, ts: datetime, row: Mapping[str, Any], mono: float) -> None:
        if not bool(P.get(self.cfg, "MFC_STATE_LOG_ENABLED")):
            return
        period = max(1.0, float(P.get(self.cfg, "MFC_STATE_LOG_INTERVAL_S")))
        for ch in ("ch1", "ch2"):
            nxt = self._next[ch]
            if nxt is not None and mono < nxt - 1e-6:
                continue
            p, v = row.get(f"{ch}.mfc.pressure_mtorr"), row.get(f"{ch}.mfc.valve_pct")
            if p is None and v is None:
                continue
            self._next[ch] = mono + period
            line = [ts.strftime("%Y-%m-%d %H:%M:%S.") + f"{ts.microsecond // 1000:03d}",
                    "" if p is None else f"{float(p):.3f}", "" if v is None else f"{float(v):.1f}"]
            self.writer.put(self._append, ch, ts, line)
            self.rows_queued += 1

    def _append(self, ch: str, ts: datetime, line: list) -> None:
        try:
            app = self._apps[ch]
            app.append_row(dt=ts, header=HEADER, row=line)
            if app.consume_switched_flag() and self._log_fn:
                self._log_fn(f"{ch.upper()} MFC 상태 CSV: 폴더에 쓸 수 없어 폴백으로 전환")
        except Exception as e:
            if self._log_fn:
                self._log_fn(f"{ch.upper()} MFC 상태 CSV 기록 실패: {e!r}")

    def stop(self) -> None:
        for a in self._apps.values():
            self.writer.put(a.close)

# lib/heater_logger.py
# -*- coding: utf-8 -*-
"""CH2 히터 운전 구간 CSV — RUN 상승 엣지에 새 파일, 운전 중 LOG_PERIOD 마다 1행, 하강 엣지에 마지막 행 후 닫기.

위치: HEATER_LOG_DIR(비면 config_common.LOG_ROOT_DIR\\CH2_Heater, 호출 시점에 읽음).
      만들 수 없으면 LOCAL_FALLBACK_ROOT\\CH2_Heater. (log_sync 워커가 LOG_ROOT_DIR 를 NAS 로 복사한다)
로컬 디스크라 동기 쓰기를 허용한다(매 행 flush). 예외는 삼키고 경고 1회.
"""
from __future__ import annotations

import csv
import datetime
import time
from pathlib import Path
from typing import Callable, Optional

COLUMNS = [
    "timestamp", "elapsed_sec", "owner",
    "pv_c", "sv_c", "ramp_sv_c", "cur_sv_c",
    "mv", "mv_limit", "mv_pct", "pid_err",
    "ramp_rate_c_per_min", "holdback_c", "ot_limit_c",
    "run", "itl", "fault", "ot", "tc_err", "wd_err", "tc_ok", "da_ok", "pid_run", "output_dead",
    "hold", "hold_mv", "sat_clamp", "ramp_target_c", "note",
]


def _default_dir() -> Path:
    from lib import config_common as cc
    return Path(str(getattr(cc, "LOG_ROOT_DIR", "Logs"))) / "CH2_Heater"


def _fallback_dir() -> Path:
    from lib import config_common as cc
    root = getattr(cc, "LOCAL_FALLBACK_ROOT", None)
    return (Path(root) if root else Path.cwd() / "Logs_LocalFallback") / "CH2_Heater"


class HeaterCsvLogger:
    def __init__(self, log_dir: str = "", warn: Optional[Callable[[str], None]] = None,
                 clock: Callable[[], float] = time.monotonic):
        self._log_dir = str(log_dir or "")
        self._warn_cb = warn
        self._now = clock
        self._fp = None
        self._writer = None
        self._path: Optional[Path] = None
        self._t0 = 0.0
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

    def _open_dir(self) -> Optional[Path]:
        cands = [Path(self._log_dir)] if self._log_dir else [_default_dir()]
        cands.append(_fallback_dir())
        for i, d in enumerate(cands):
            try:
                d.mkdir(parents=True, exist_ok=True)
                if i > 0:
                    self._warn(f"히터 CSV 폴더({cands[0]})를 만들 수 없어 {d} 에 저장합니다")
                return d
            except Exception:
                continue
        return None

    def start(self) -> Optional[Path]:
        """새 CSV 를 열고 헤더를 쓴다. 이미 열려 있으면 그대로 둔다."""
        if self._fp is not None:
            return self._path
        base = self._open_dir()
        if base is None:
            self._warn("히터 CSV 폴더를 만들 수 없어 기록하지 않습니다")
            return None
        stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        path = base / f"CH2_HEATER_{stamp}.csv"
        k = 1
        while path.exists():
            k += 1
            path = base / f"CH2_HEATER_{stamp}_{k}.csv"
        try:
            fp = open(path, "w", encoding="utf-8-sig", newline="")
            w = csv.writer(fp)
            w.writerow(COLUMNS)
            fp.flush()
        except Exception as e:
            self._warn(f"히터 CSV 열기 실패: {e!r}")
            return None
        self._fp, self._writer, self._path = fp, w, path
        self._t0 = self._now()
        return path

    def write_row(self, st: dict, *, owner: str = "", note: str = "", hold=None,
                  sat_clamp=None, ramp_target=None) -> None:
        if self._fp is None or self._writer is None:
            return

        def _n(key, fmt="{:.1f}"):
            v = st.get(key)
            if v is None:
                return ""
            try:
                return fmt.format(float(v))
            except Exception:
                return ""

        def _b(key):
            return 1 if st.get(key) else 0

        try:
            self._writer.writerow([
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                f"{self._now() - self._t0:.1f}", owner or "",
                _n("pv"), _n("sv"), _n("sv_ramp"), _n("cur_sv"),
                _n("mv", "{:.0f}"), _n("mv_limit", "{:.0f}"), _n("mv_pct"), _n("pid_err", "{:.0f}"),
                _n("ramp_rate", "{:.0f}"), _n("holdback"), _n("ot_limit"),
                _b("run"), _b("itl"), _b("fault"), _b("ot"), _b("tc_err"), _b("wd_err"),
                _b("tc_ok"), _b("da_ok"), _b("pid_run"), _b("output_dead"),
                (1 if (hold and hold[0]) else 0),
                ("" if (not hold or hold[1] is None) else int(hold[1])),
                ("" if sat_clamp is None else int(sat_clamp)),
                ("" if ramp_target is None else f"{float(ramp_target):.1f}"),
                note or "",
            ])
            self._fp.flush()
        except Exception as e:
            self._warn(f"히터 CSV 기록 실패(이후 경고 생략): {e!r}")

    def stop(self) -> None:
        if self._fp is not None:
            try:
                self._fp.flush()
                self._fp.close()
            except Exception:
                pass
        self._fp = None
        self._writer = None
        self._path = None

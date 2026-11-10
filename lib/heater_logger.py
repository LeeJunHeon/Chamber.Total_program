# lib/heater_logger.py
# -*- coding: utf-8 -*-
"""CH2 히터 CSV — 하루 1개 파일(CH2_HEATER_YYYYMMDD.csv)에 프로그램이 도는 동안 항상 기록.

- 운전 중(RUN=1) HEATER_LOG_PERIOD_S(1초)마다 1행, 정지 중 HEATER_POLL_IDLE_S(3초)마다 1행(주기 판단은 heater_service).
- RUN 상승/하강 폴링의 행은 note="운전 시작"/"운전 종료", 프로그램 종료 때 note="프로그램 종료" 1행.
- PLC 를 못 읽은 폴링도 행을 남긴다(timestamp·note 만, 값 열은 전부 빈칸 — 옛 값을 반복해 쓰지 않는다).
- 날짜는 행의 timestamp 기준, 자정이 지나면 다음 행부터 새 파일. 같은 날 다시 켜면 이어 쓴다(빈 파일일 때만 헤더).
- 운전별 파일(CH2_HEATER_YYYYMMDD_HHMMSS.csv)은 더 만들지 않는다(옛 파일은 그대로 둔다).

위치: HEATER_LOG_DIR(비면 config_common.LOG_ROOT_DIR\\CH2_Heater, 호출 시점에 읽음).
      만들 수 없으면 LOCAL_FALLBACK_ROOT\\CH2_Heater. (log_sync 워커가 LOG_ROOT_DIR 를 NAS 로 복사한다)
파일 IO 는 util.log_hub.DailyCsvListAppender(keep-handle, 날짜 바뀌면 새 파일, primary 실패 시 fallback).
로컬 디스크라 동기 쓰기를 허용한다(매 행 flush). 예외는 삼키고 경고 1회.
"""
from __future__ import annotations

import datetime
import time
from pathlib import Path
from typing import Callable, Optional

from util.log_hub import DailyCsvListAppender

COLUMNS = [
    "timestamp", "elapsed_sec", "owner",
    "pv_c", "sv_c", "ramp_sv_c", "cur_sv_c",
    "mv", "mv_limit", "mv_pct", "pid_err",
    "ramp_rate_c_per_min", "holdback_c", "ot_limit_c",
    "run", "itl", "fault", "ot", "tc_err", "wd_err", "tc_ok", "da_ok", "water_ok", "vac_ok", "pid_run", "output_dead",
    "hold", "hold_mv", "sat_clamp", "ramp_target_c", "note",
]


def _default_dir() -> Path:
    from lib import config_common as cc
    return Path(str(getattr(cc, "LOG_ROOT_DIR", "Logs"))) / "CH2_Heater"


def _fallback_dir() -> Path:
    from lib import config_common as cc
    root = getattr(cc, "LOCAL_FALLBACK_ROOT", None)
    return (Path(root) if root else Path.cwd() / "Logs_LocalFallback") / "CH2_Heater"


def file_name(dt: datetime.datetime) -> str:
    return f"CH2_HEATER_{dt:%Y%m%d}.csv"


class HeaterCsvLogger:
    def __init__(self, log_dir: str = "", warn: Optional[Callable[[str], None]] = None,
                 clock: Callable[[], float] = time.monotonic,
                 wall: Callable[[], datetime.datetime] = datetime.datetime.now):
        self._log_dir = str(log_dir or "")
        self._warn_cb = warn
        self._now = clock                    # (호환) 단조 시계 — 경과 초는 호출부가 넘긴다
        self._wall = wall                    # 벽시계 — 행 timestamp·파일 날짜(시험에서 주입)
        self._app: Optional[DailyCsvListAppender] = None
        self._warned = False

    # ───────── 상태 ─────────
    @property
    def path(self) -> Optional[Path]:
        """지금 열려 있는 파일(아직 한 행도 안 썼으면 None)."""
        return self._app.current_path if self._app is not None else None

    def is_open(self) -> bool:
        return self.path is not None

    def today_path(self) -> Path:
        """지금 시각 기준으로 행이 들어갈 파일 경로(열지는 않는다)."""
        app = self._ensure()
        base = app._fallback_dir if app.state.using_fallback else app._primary_dir
        return base / file_name(self._wall())

    def _warn(self, text: str) -> None:
        if self._warned:
            return
        self._warned = True
        if self._warn_cb:
            try:
                self._warn_cb(text)
            except Exception:
                pass

    def _ensure(self) -> DailyCsvListAppender:
        if self._app is None:
            primary = Path(self._log_dir) if self._log_dir else _default_dir()
            self._app = DailyCsvListAppender(
                primary_dir=primary, fallback_dir=_fallback_dir(), filename_builder=file_name,
                encoding="utf-8-sig", retry_primary_every_s=0.0)
        return self._app

    # ───────── 기록 ─────────
    def write_row(self, st: Optional[dict], *, owner: str = "", note: str = "", hold=None,
                  sat_clamp=None, ramp_target=None, elapsed: Optional[float] = None) -> None:
        """한 행 기록. st=None 이면(PLC 를 못 읽음) timestamp·note 만 채우고 나머지는 빈칸."""
        now = self._wall()
        if st is None:
            row = [now.strftime("%Y-%m-%d %H:%M:%S")] + [""] * (len(COLUMNS) - 2) + [note or ""]
        else:
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

            row = [
                now.strftime("%Y-%m-%d %H:%M:%S"),
                ("" if elapsed is None else f"{float(elapsed):.1f}"), owner or "",
                _n("pv"), _n("sv"), _n("sv_ramp"), _n("cur_sv"),
                _n("mv", "{:.0f}"), _n("mv_limit", "{:.0f}"), _n("mv_pct"), _n("pid_err", "{:.0f}"),
                _n("ramp_rate", "{:.0f}"), _n("holdback"), _n("ot_limit"),
                _b("run"), _b("itl"), _b("fault"), _b("ot"), _b("tc_err"), _b("wd_err"),
                _b("tc_ok"), _b("da_ok"), _b("water_ok"), _b("vac_ok"), _b("pid_run"), _b("output_dead"),
                (1 if (hold and hold[0]) else 0),
                ("" if (not hold or hold[1] is None) else int(hold[1])),
                ("" if sat_clamp is None else int(sat_clamp)),
                ("" if ramp_target is None else f"{float(ramp_target):.1f}"),
                note or "",
            ]
        try:
            app = self._ensure()
            app.append_row(dt=now, header=COLUMNS, row=row)
            if app.consume_switched_flag():
                self._warn(f"히터 CSV 폴더({app._primary_dir})에 쓸 수 없어 {app._fallback_dir} 에 저장합니다")
        except Exception as e:
            self._warn(f"히터 CSV 기록 실패(이후 경고 생략): {e!r}")

    def stop(self) -> None:
        """파일을 닫는다(다음 기록 때 다시 연다 — 같은 날이면 이어 쓴다)."""
        if self._app is not None:
            try:
                self._app.close()
            except Exception:
                pass

"""CH2 히터 CSV — v1 lib/heater_logger.py(HeaterCsvLogger, main 9cde7c4 — 2026-11-10 반영)의 파일 출력(LOG-068~070).

하루 1개 파일(CH2_HEATER_YYYYMMDD.csv)에 프로그램이 도는 동안 항상 기록한다.
- CH2_HEATER_LOG_DIR(비면 LOG_ROOT_DIR\\CH2_Heater)\\CH2_HEATER_{YYYYMMDD}.csv(행의 timestamp 날짜 기준 — 자정이 지나면 다음 행부터 새 파일), 폴더를
  못 만들면 LOCAL_FALLBACK_ROOT\\CH2_Heater('히터 CSV 폴더(..)에 쓸 수 없어 .. 에 저장합니다'), utf-8-sig, 같은 날 다시 켜면 이어 쓴다(빈 파일일 때만 헤더),
  매 행 flush, 쓰기 실패는 경고 1회. 운전별 파일(CH2_HEATER_YYYYMMDD_HHMMSS.csv)은 더 만들지 않는다(옛 파일은 그대로 둔다).
- 열 31개(COLUMNS) — 숫자 형식 '%.1f'(pv·sv·…), '%.0f'(mv·mv_limit·pid_err·ramp_rate), 불리언 1/0, hold(1/0), hold_mv 정수,
  sat_clamp 정수, ramp_target_c '%.1f', note. elapsed_sec 는 운전 중에만(호출부가 넘김), 정지 중 행은 빈칸.
- 행: 운전 중 CH2_HEATER_LOG_PERIOD_S(1.0 s) 마다, 정지 중 CH2_HEATER_POLL_IDLE_S(3 s) 마다(주기 판단은 히터 엔진), RUN 상승/하강 폴링은 note
  '운전 시작'/'운전 종료', 프로그램 종료 때 '프로그램 종료' 1행. PLC 를 못 읽은 폴링도 행을 남긴다(st=None: timestamp·note 만, 나머지는 빈칸).
부르는 법(히터 엔진 — v1 heater_service 와 같은 API)
  h = HeaterCsv(cfg, writer=w, warn=fn, monotonic=clock.monotonic, now=clock_dt)
  h.today_path() / h.write_row(st|None, owner=.., note=.., hold=.., sat_clamp=.., ramp_target=.., elapsed=..) / h.stop() / h.is_open()
  (옛 API start() 는 today_path 와 같다 — 파일을 열지 않고 오늘 경로만 돌려준다.)
  st 키(v1 heater_service 상태 사전): pv, sv, sv_ramp, cur_sv, mv, mv_limit, mv_pct, pid_err, ramp_rate, holdback, ot_limit,
  run, itl, fault, ot, tc_err, wd_err, tc_ok, da_ok, water_ok, vac_ok, pid_run, output_dead
"""
from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from v2.ch12.outputs.base import Owned, QueueWriter, wall_now
from v2.ch12.outputs.log_hub import DailyCsvListAppender
from v2.ch12.outputs.paths import Paths
from v2.ch12.params import outputs as P

COLUMNS = [
    "timestamp", "elapsed_sec", "owner",
    "pv_c", "sv_c", "ramp_sv_c", "cur_sv_c",
    "mv", "mv_limit", "mv_pct", "pid_err",
    "ramp_rate_c_per_min", "holdback_c", "ot_limit_c",
    "run", "itl", "fault", "ot", "tc_err", "wd_err", "tc_ok", "da_ok", "water_ok", "vac_ok", "pid_run", "output_dead",
    "hold", "hold_mv", "sat_clamp", "ramp_target_c", "note",
]


def build_row(st: Optional[Mapping[str, Any]], *, ts: datetime, elapsed: Optional[float] = None, owner: str = "", note: str = "",
              hold=None, sat_clamp=None, ramp_target=None) -> list:
    if st is None:                                   # PLC 를 못 읽음 — timestamp·note 만(옛 값을 반복해 쓰지 않는다)
        return [ts.strftime("%Y-%m-%d %H:%M:%S")] + [""] * (len(COLUMNS) - 2) + [note or ""]

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
    return [
        ts.strftime("%Y-%m-%d %H:%M:%S"),
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


class HeaterCsv(Owned):
    def __init__(self, cfg: Optional[Mapping[str, Any]] = None, *, writer: Optional[QueueWriter] = None,
                 warn: Optional[Callable[[str], None]] = None, monotonic: Callable[[], float] = time.monotonic,
                 now: Callable[[], datetime] = wall_now) -> None:
        super().__init__(writer, "heater-csv")
        self.cfg = cfg if cfg is not None else {}
        self.paths = Paths(self.cfg)
        self._warn_cb = warn
        self.monotonic = monotonic
        self.now = now
        self._app = None
        self._warned = False
        self._written = False         # 이번 실행에 한 행이라도 큐에 넣음(is_open 호환)

    def _warn(self, text: str) -> None:
        if self._warned:
            return
        self._warned = True
        if self._warn_cb:
            try:
                self._warn_cb(text)
            except Exception:
                pass

    @property
    def path(self) -> Optional[Path]:
        return self._app.current_path if self._app is not None else None

    def is_open(self) -> bool:
        return self.path is not None or self._written

    def _ensure(self):
        if self._app is None:
            primary, fallback = self.paths.heater_dir(), self.paths.heater_fallback()
            self._app = DailyCsvListAppender(primary_dir=primary, fallback_dir=fallback,
                                             filename_builder=lambda dt: f"CH2_HEATER_{dt:%Y%m%d}.csv",
                                             encoding="utf-8-sig", retry_primary_every_s=0.0,
                                             monotonic=self.monotonic)
        return self._app

    def today_path(self) -> Path:
        """지금 시각 기준으로 행이 들어갈 파일 경로(열지는 않는다)."""
        app = self._ensure()
        base = app._fallback_dir if app.state.using_fallback else app._primary_dir
        return Path(base) / f"CH2_HEATER_{self.now():%Y%m%d}.csv"

    def start(self) -> Path:
        """v1 호환 — 하루 파일이라 열 것이 없다. 오늘 경로를 돌려준다."""
        return self.today_path()

    def write_row(self, st: Optional[Mapping[str, Any]], *, owner: str = "", note: str = "", hold=None, sat_clamp=None,
                  ramp_target=None, elapsed: Optional[float] = None) -> None:
        """한 행(큐에만). st=None 이면 PLC 를 못 읽은 행."""
        ts = self.now()
        row = build_row(st, ts=ts, elapsed=elapsed, owner=owner, note=note, hold=hold, sat_clamp=sat_clamp,
                        ramp_target=ramp_target)
        self._written = True
        self.writer.put(self._do_write, ts, row)

    def stop(self) -> None:
        """파일을 닫는다(다음 기록 때 다시 연다 — 같은 날이면 이어 쓴다)."""
        self.writer.put(self._do_stop)

    def shutdown(self, st: Optional[Mapping[str, Any]] = None, owner_label: str = "") -> None:
        """프로그램 종료: '프로그램 종료' 행 1개 후 닫기."""
        self.write_row(st, owner=owner_label, note="프로그램 종료")
        self.stop()
        self.stop_writer()

    # ── 쓰기 스레드 ──
    def _do_write(self, ts: datetime, row: list) -> None:
        try:
            app = self._ensure()
            app.append_row(dt=ts, header=COLUMNS, row=row)
            if app.consume_switched_flag():
                self._warn(f"히터 CSV 폴더({app._primary_dir})에 쓸 수 없어 {app._fallback_dir} 에 저장합니다")
        except Exception as e:
            self._warn(f"히터 CSV 기록 실패(이후 경고 생략): {e!r}")

    def _do_stop(self) -> None:
        if self._app is not None:
            try:
                self._app.close()
            except Exception:
                pass

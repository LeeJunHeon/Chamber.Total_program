"""CH2 히터 운전 CSV — v1 lib/heater_logger.py(HeaterCsvLogger) 의 파일 출력(LOG-068~070).

v1 그대로
- CH2_HEATER_LOG_DIR(비면 LOG_ROOT_DIR\\CH2_Heater)\\CH2_HEATER_{YYYYmmdd_HHMMSS}.csv(있으면 _2, _3 …), RUN 상승 엣지에 새 파일
  (CH2_HEATER_LOG_ENABLED), 폴더를 못 만들면 LOCAL_FALLBACK_ROOT\\CH2_Heater('히터 CSV 폴더(..)를 만들 수 없어 .. 에 저장합니다'),
  utf-8-sig(새 파일 'w'), 매 행 flush, 쓰기 실패는 경고 1회.
- 열 31개(COLUMNS) — 숫자 형식 '%.1f'(pv·sv·…), '%.0f'(mv·mv_limit·pid_err·ramp_rate), 불리언 1/0, hold(1/0), hold_mv 정수,
  sat_clamp 정수, ramp_target_c '%.1f', note.
- 주기: RUN 중 CH2_HEATER_LOG_PERIOD_S(3.0 s, 1~60) 마다 1행, RUN 하강 엣지에 note '운전 종료' 마지막 행 후 닫기,
  프로그램 종료 때 '프로그램 종료' 행 후 닫기(controller/heater_service.py:850-855,1113-1146,461-465).
- elapsed_sec = 파일을 연 뒤 경과(시계 monotonic) '%.1f'.

부르는 법
  h = HeaterCsv(cfg, writer=w, warn=fn, monotonic=clock.monotonic, now=clock_dt)
  h.on_tick(st, owner_label=.., hold=(holding, hold_mv), sat_clamp=.., ramp_target=..)   # 1초마다(엣지·주기는 여기서)
  또는 v1 API 그대로 h.start() / h.write_row(st, owner=.., note=.., hold=.., sat_clamp=.., ramp_target=..) / h.stop()
  st 키(v1 heater_service 상태 사전): pv, sv, sv_ramp, cur_sv, mv, mv_limit, mv_pct, pid_err, ramp_rate, holdback, ot_limit,
  run, itl, fault, ot, tc_err, wd_err, tc_ok, da_ok, water_ok, vac_ok, pid_run, output_dead
"""
from __future__ import annotations

import csv
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from v2.ch12.outputs.base import Owned, QueueWriter, wall_now
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


def build_row(st: Mapping[str, Any], *, ts: datetime, elapsed: float, owner: str = "", note: str = "",
              hold=None, sat_clamp=None, ramp_target=None) -> list:
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
        f"{elapsed:.1f}", owner or "",
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
        self._open = False          # 부르는 쪽 기준(파일 열기 요청함)
        self._t0 = 0.0
        self._fp = None
        self._writer = None
        self.path: Optional[Path] = None
        self._warned = False
        self._last_run = False
        self._csv_last_t = -1e9

    def _warn(self, text: str) -> None:
        if self._warned:
            return
        self._warned = True
        if self._warn_cb:
            try:
                self._warn_cb(text)
            except Exception:
                pass

    def is_open(self) -> bool:
        return self._open

    # ── v1 API(큐에만) ──
    def start(self) -> None:
        if self._open:
            return
        self._open = True
        self._t0 = self.monotonic()
        self.writer.put(self._do_start, self.now())

    def write_row(self, st: Mapping[str, Any], *, owner: str = "", note: str = "", hold=None, sat_clamp=None,
                  ramp_target=None) -> None:
        if not self._open:
            return
        row = build_row(st, ts=self.now(), elapsed=self.monotonic() - self._t0, owner=owner, note=note, hold=hold,
                        sat_clamp=sat_clamp, ramp_target=ramp_target)
        self.writer.put(self._do_write, row)

    def stop(self) -> None:
        self._open = False
        self.writer.put(self._do_stop)

    # ── 1초 tick(엣지·주기 — heater_service 의 _on_run_rise/_on_run_fall/주기 기록과 같은 규칙) ──
    def on_tick(self, st: Mapping[str, Any], *, owner_label: str = "", hold=None, sat_clamp=None,
                ramp_target=None) -> None:
        run = bool(st.get("run"))
        if run and not self._last_run:
            if bool(P.get(self.cfg, "CH2_HEATER_LOG_ENABLED")):
                self.start()
            self._csv_last_t = -1e9
        elif not run and self._last_run:
            if self._open:
                self.write_row(st, owner=owner_label, hold=hold, sat_clamp=sat_clamp, note="운전 종료")
            self.stop()
        self._last_run = run
        if run and self._open:
            period = min(60.0, max(1.0, float(P.get(self.cfg, "CH2_HEATER_LOG_PERIOD_S"))))
            now = self.monotonic()
            if now - self._csv_last_t >= period:
                self._csv_last_t = now
                self.write_row(st, owner=owner_label, hold=hold, sat_clamp=sat_clamp, ramp_target=ramp_target)

    def shutdown(self, st: Optional[Mapping[str, Any]] = None, owner_label: str = "") -> None:
        """프로그램 종료: 열려 있으면 '프로그램 종료' 행 후 닫기."""
        if self._open and st is not None:
            self.write_row(st, owner=owner_label, note="프로그램 종료")
        self.stop()
        self.stop_writer()

    # ── 쓰기 스레드 ──
    def _open_dir(self) -> Optional[Path]:
        cands = [self.paths.heater_dir(), self.paths.heater_fallback()]
        for i, d in enumerate(cands):
            try:
                d.mkdir(parents=True, exist_ok=True)
                if i > 0:
                    self._warn(f"히터 CSV 폴더({cands[0]})를 만들 수 없어 {d} 에 저장합니다")
                return d
            except Exception:
                continue
        return None

    def _do_start(self, ts: datetime) -> None:
        if self._fp is not None:
            return
        base = self._open_dir()
        if base is None:
            self._warn("히터 CSV 폴더를 만들 수 없어 기록하지 않습니다")
            return
        stamp = ts.strftime("%Y%m%d_%H%M%S")
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
            return
        self._fp, self._writer, self.path = fp, w, path

    def _do_write(self, row: list) -> None:
        if self._fp is None or self._writer is None:
            return
        try:
            self._writer.writerow(row)
            self._fp.flush()
        except Exception as e:
            self._warn(f"히터 CSV 기록 실패(이후 경고 생략): {e!r}")

    def _do_stop(self) -> None:
        if self._fp is not None:
            try:
                self._fp.flush()
                self._fp.close()
            except Exception:
                pass
        self._fp = None
        self._writer = None

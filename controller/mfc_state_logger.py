# controller/mfc_state_logger.py
# -*- coding: utf-8 -*-
"""GPC3000(코드상 MFC) 압력·쓰로틀 밸브 위치 상시 CSV 기록 — 공정과 무관하게 1초마다, 가장 낮은 우선순위.

- 매 tick(MFC_STATE_LOG_INTERVAL_S): 연결돼 있으면 AsyncMFC.read_state_quiet()(R5·R6 를 low_priority·quiet 로) → CSV 1행.
  Timestamp 는 실제로 값을 읽은 시각(R5 응답을 받은 시각) — 공정 중에는 기록 읽기가 앞 명령의 1초 간격 뒤로 밀려
  tick 시작 시각과 측정 시각이 1초 이상 벌어질 수 있다.
- CSV: MFC_STATE_LOG_DIR\\{label}_YYYYMMDD.csv, utf-8-sig, 헤더 Timestamp,Pressure_mTorr,Valve_pct. Timestamp 는 밀리초까지(YYYY-MM-DD HH:MM:SS.mmm).
  읽기 실패한 값은 빈 칸. 둘 다 실패했거나 연결 안 됨이면 그 초는 행을 쓰지 않는다.
  파일 쓰기는 PLC 코일 로거와 같이 util.log_hub.DailyCsvListAppender + asyncio.to_thread.
- 연결 유지: MFC 가 내려가 있으면(워커·워치독이 안 돌고 cleanup 진행 중도 아님) 그 상태가 REVIVE_AFTER_S 이어질 때만
  TCP 가 열리는지 한 번 확인(probe)한 뒤 mfc.start() 를 부른다. 장비(MOXA/GPC) 전원이 꺼져 있으면 start() 하지 않고
  PROBE_BACKOFF 로 물러난다(워치독의 '연결 실패' 반복을 막기 위해).
  기록기는 cleanup()/on_process_finished()/set_process_status()/set_poll_mask() 를 부르지 않고 runtime_state 에도 등록하지 않는다.
  → 공정이 끝나면 지금처럼 연결이 닫히고 몇 초 뒤 기록기가 다시 연다(그 몇 초는 CSV 공백).
- 실패·건너뜀은 개수만 세고, 문제가 있었을 때만 1분에 한 줄 요약을 CSV 와 같은 폴더의 {label}_YYYYMMDD.log 에 남긴다
  (공정 txt 로그·프리버퍼에 섞이지 않게). 예외는 모두 안에서 삼키고 계속 돈다.
"""
from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from lib import config_common as cfgc
from util.log_hub import DailyCsvListAppender

HEADER = ["Timestamp", "Pressure_mTorr", "Valve_pct"]
REVIVE_AFTER_S = 5.0      # MFC 가 내려간 채 이 시간이 지나야 다시 올린다(공정 종료 정리 mfc.cleanup 은 ~1.5초 안에 끝난다)
PROBE_BACKOFF_S = (30.0, 60.0, 120.0, 300.0)   # 장비가 응답하지 않을 때 다음 확인까지 대기(점점 길게)
SUMMARY_PERIOD_S = 60.0


def _fallback_dir() -> Path:
    root = getattr(cfgc, "LOCAL_FALLBACK_ROOT", None)
    return (Path(root) if root else Path.cwd() / "Logs_LocalFallback") / "CH1&2_MFC"


class MfcStateLogger:
    def __init__(self, mfc, label: str, *, cfg=None,
                 clock: Callable[[], float] = time.monotonic,
                 wall: Callable[[], datetime] = datetime.now,
                 to_thread: Callable = asyncio.to_thread,
                 log_dir: Optional[str] = None):
        self._mfc = mfc
        self.label = str(label)
        self._cfg = cfg if cfg is not None else cfgc
        self._now = clock
        self._wall = wall
        self._to_thread = to_thread
        d = log_dir if log_dir is not None else getattr(self._cfg, "MFC_STATE_LOG_DIR", "")
        self.log_dir = Path(str(d)) if d else (Path(str(getattr(cfgc, "LOG_ROOT_DIR", "Logs"))) / "CH1&2_MFC")
        self._task: Optional[asyncio.Task] = None
        self._stopped = False
        self._app: Optional[DailyCsvListAppender] = None
        self._down_since: Optional[float] = None
        self._probe_fail_n = 0
        self._probe_next_t = 0.0
        self.stats = {"rows": 0, "disconnected": 0, "empty": 0, "partial": 0, "write_failed": 0,
                      "exc": 0, "revive": 0, "probe_fail": 0}
        self._last_summary_t = self._now()
        self._summary_snapshot = dict(self.stats)

    # ───────── 설정 ─────────
    def _f(self, name: str, default: float) -> float:
        try:
            return float(getattr(self._cfg, name, default))
        except Exception:
            return float(default)

    @property
    def interval_s(self) -> float:
        return max(0.2, self._f("MFC_STATE_LOG_INTERVAL_S", 1.0))

    # ───────── 수명 ─────────
    def start(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopped = False
        lp = loop or asyncio.get_event_loop()
        self._task = lp.create_task(self._loop(), name=f"MfcStateLog.{self.label}")

    async def stop(self) -> None:
        """태스크 정지 + 파일 닫기. 이후 MFC 를 다시 올리지 않는다(프로그램 종료 경로에서 MFC cleanup 보다 먼저 부른다)."""
        self._stopped = True
        t, self._task = self._task, None
        if t is not None and not t.done():
            t.cancel()
            with contextlib.suppress(BaseException):
                await t
        app, self._app = self._app, None
        if app is not None:
            with contextlib.suppress(Exception):
                await self._to_thread(app.close)

    # ───────── 루프 ─────────
    async def _loop(self) -> None:
        while not self._stopped:
            t0 = self._now()
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.stats["exc"] += 1
                self._last_exc = repr(e)
            try:
                await self._maybe_summary()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            await asyncio.sleep(max(0.0, self.interval_s - (self._now() - t0)))

    def _mfc_down(self) -> bool:
        """워커·워치독이 안 돌고 cleanup 진행 중도 아니면 True(= 아무도 올려 두지 않은 상태)."""
        m = self._mfc
        if getattr(m, "_closing", False):
            return False
        for name in ("_watchdog_task", "_cmd_worker_task"):
            t = getattr(m, name, None)
            if t is not None and not t.done():
                return False
        return True

    async def _tick(self) -> None:
        m = self._mfc
        if not m.is_connected():
            self.stats["disconnected"] += 1
            await self._maybe_revive()
            return
        self._down_since = None
        self._probe_fail_n = 0
        timeout_ms = int(self._f("MFC_STATE_LOG_TIMEOUT_MS", 500))
        gap_ms = int(self._f("MFC_STATE_LOG_GAP_MS", 100))
        p, v, read_at = await m.read_state_quiet(timeout_ms=timeout_ms, gap_ms=gap_ms)
        if p is None and v is None:
            self.stats["empty"] += 1
            return
        if p is None or v is None:
            self.stats["partial"] += 1
        dt = read_at or self._wall()
        row = [dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond // 1000:03d}",   # 밀리초까지 — 읽기가 밀렸다 몰려도 같은 시각이 안 찍히게
               "" if p is None else f"{p:.3f}",
               "" if v is None else f"{v:.1f}"]
        app = self._appender()
        try:
            await self._to_thread(app.append_row, dt=dt, header=HEADER, row=row)
            self.stats["rows"] += 1
        except Exception as e:
            self.stats["write_failed"] += 1
            self._last_exc = repr(e)

    def _appender(self) -> DailyCsvListAppender:
        if self._app is None:
            self._app = DailyCsvListAppender(
                primary_dir=self.log_dir, fallback_dir=_fallback_dir(),
                filename_builder=lambda dt: f"{self.label}_{dt:%Y%m%d}.csv",
                encoding="utf-8-sig", retry_primary_every_s=10.0)
        return self._app

    # ───────── 연결 유지 ─────────
    async def _probe(self) -> bool:
        """장비 TCP 포트가 열리는지 한 번 확인(열고 바로 닫는다). 시험에서 바꿔 끼운다."""
        host, port = self._mfc._resolve_endpoint()
        try:
            _r, w = await asyncio.wait_for(asyncio.open_connection(host, int(port)), timeout=2.0)
        except asyncio.CancelledError:
            raise
        except Exception:
            return False
        with contextlib.suppress(Exception):
            w.close()
            await asyncio.wait_for(w.wait_closed(), timeout=1.0)
        return True

    async def _maybe_revive(self) -> None:
        if self._stopped or not self._mfc_down():
            self._down_since = None
            return
        now = self._now()
        if self._down_since is None:
            self._down_since = now
            return
        if (now - self._down_since) < REVIVE_AFTER_S or now < self._probe_next_t:
            return
        if not await self._probe():
            self.stats["probe_fail"] += 1
            self._probe_next_t = now + PROBE_BACKOFF_S[min(self._probe_fail_n, len(PROBE_BACKOFF_S) - 1)]
            self._probe_fail_n += 1
            return
        if self._stopped or not self._mfc_down():      # probe 하는 사이 다른 쪽이 올렸거나 정리 중이면 손대지 않는다
            return
        self.stats["revive"] += 1
        await self._mfc.start()
        self._down_since = None

    # ───────── 요약 ─────────
    async def _maybe_summary(self) -> None:
        now = self._now()
        if (now - self._last_summary_t) < SUMMARY_PERIOD_S:
            return
        self._last_summary_t = now
        prev, cur = self._summary_snapshot, dict(self.stats)
        self._summary_snapshot = cur
        d = {k: cur[k] - prev.get(k, 0) for k in cur}
        bad = {k: n for k, n in d.items() if n and k not in ("rows",)}
        if not bad:
            return
        line = (f"{self._wall():%Y-%m-%d %H:%M:%S} [{self.label}] 최근 {SUMMARY_PERIOD_S:.0f}초: 기록 {d['rows']}행, "
                + ", ".join(f"{k} {n}" for k, n in bad.items())
                + (f", 마지막 예외 {getattr(self, '_last_exc', '')}" if getattr(self, "_last_exc", "") else ""))
        self._last_exc = ""
        path = self.log_dir / f"{self.label}_{self._wall():%Y%m%d}.log"

        def _w():
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as fp:
                fp.write(line + "\n")
        await self._to_thread(_w)

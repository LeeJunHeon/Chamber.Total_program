"""오래 켜 두기(가상 시계) — 실행 프로그램과 같은 조립·같은 기록 크기(Ch12Rig + system.APP_RETENTION)로 hours 시간을 돌리고
시간마다 메모리·큐·링 버퍼·스레드를 잰다. 시험(v2/tests/test_long_run.py)은 10 시간, 최종 단계는 엔진을 꽂고 7일(168 시간).

돌리는 동안(엔진 없이 공유 장비 잡기·놓기를 되풀이 — 잡을 때마다 worker 를 새로 만드는 곳에서 스레드·메모리가 새지 않는지):
  시작은 로컬 19:00(5 시간 뒤 자정 — 일별 SQLite 바뀜·지난 날 CSV 내보내기·보관 정리가 도는 중에).
  PERIOD_S 마다 CH1 DC Pulse·CH2 RF Pulse 점유(HOLD_S 동안) → 해제, TSP 게이지 잡기 → 놓기, 카메라 chamber1 참여 → 떠남,
  TSP 205 읽기 켜기 → 끄기. engines 를 주면 register_engine 으로 꽂고 hook(rig, tick) 은 tick 마다 부른다.
표(시간마다): h, private_mb·ws_mb(프로세스 전용·작업 집합 — gc 뒤), threads, rec_pending·rec_dropped, out_pending, events,
  sim_log 최대 길이, rows, missed, wall_s.
"""
from __future__ import annotations

import datetime as _dt
import gc
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from v2.common.clock import VirtualClock
from v2.ch12.sim.rig import SIM_LOGS, Ch12Rig
from v2.ch12.system import APP_RETENTION

PERIOD_S = 900
HOLD_S = 30
START_WALL = _dt.datetime(2025, 10, 9, 19, 0, 0, 250000).timestamp()   # 로컬 19:00 — 5 시간 뒤 자정(일별 파일·CSV 내보내기)


def mem() -> Dict[str, Optional[float]]:
    from v2.ch12.snapshot_fields import program_state
    st = program_state()
    return {"private_mb": st.get("prog.private_mb"), "ws_mb": st.get("prog.working_set_mb")}


def _churn(rig: Ch12Rig, t) -> None:
    k = t.seq % PERIOD_S
    if k == 5:
        rig.shared.pulse.claim("dcpulse", "ch1")
        rig.shared.pulse.claim("rfpulse", "ch2")
        rig.shared.tsp_ig.acquire("tsp")
        rig.tsp.begin_use()
        rig.shared.camera.join("chamber1")
    elif k == 5 + HOLD_S:
        rig.shared.pulse.release_chamber("ch1")
        rig.shared.pulse.release_chamber("ch2")
        rig.shared.tsp_ig.release("tsp")
        rig.tsp.end_use()
        rig.shared.camera.leave("chamber1")


def run_virtual(hours: float, out_dir: Optional[Path] = None, engines: Optional[Dict[str, Any]] = None,
                hook: Optional[Callable[[Ch12Rig, Any], None]] = None, churn: bool = True,
                cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """가상 hours 시간. {"table": [...], "rig": rig(멈춘 뒤), "stats": tick 통계}."""
    out = Path(out_dir) if out_dir is not None else Path(tempfile.mkdtemp(prefix="ch12_soak_"))
    c = VirtualClock(start_wall=START_WALL)
    c.register()
    rig = Ch12Rig(c, out, cfg=dict({"CAMERA_MIN_FREE_GB": 0.0}, **(cfg or {})), retention=APP_RETENTION)
    for n, e in (engines or {}).items():
        rig.register_engine(n, e)
    table: List[Dict[str, Any]] = []
    w0 = time.perf_counter()
    n_ticks = int(hours * 3600) + 1

    def sample(t) -> None:
        if churn:
            _churn(rig, t)
        if hook is not None:
            hook(rig, t)
        if t.seq % 3600 == 0:
            # 지난 날 CSV 내보내기(recorder-csv)는 실제 시간으로 수 분 걸린다 — 실제 운전은 하루 안에 끝나지만 가상 시계는
            # 몇 시간을 실제 수 분에 돌므로 표본 때 아직 살아 있어 스레드 수·메모리가 늘어 보인다(종합 7일 시험과 같은 처리:
            # 끝날 때까지 기다림 — 가상 시각은 멈춰 있고, 기다리는 참가자가 '진행 없음'으로 끝나지 않게 깨워 둔다)
            for th in list(rig.recorder._exports):
                while th.is_alive():
                    th.join(1.0)
                    rig.clock.notify()
            gc.collect()
            m = mem()
            logs = [len(getattr(x, k)) for x in rig.sims.values() for k in SIM_LOGS if hasattr(x, k)]
            table.append({"h": t.seq // 3600, **m, "threads": threading.active_count(),
                          "rec_pending": rig.recorder._q.qsize(), "rec_dropped": rig.recorder.dropped,
                          "out_pending": rig.outputs.pending, "events": len(rig.events),
                          "sim_log_max": max(logs) if logs else 0, "rows": rig.loop.rows, "missed": rig.loop.missed,
                          "wall_s": round(time.perf_counter() - w0, 1)})
    rig.loop.tick_hooks.append(sample)
    assert rig.start()
    try:
        rig.loop.run(n_ticks)
    finally:
        rig.stop()
        c.close()
    return {"table": table, "rig": rig, "stats": rig.tick_stats(), "out_dir": out}

"""운전 코드 반복 시험(사용자 결정 2026-11-02 자원 정리) — 공정을 100회(CH1·CH2·플라즈마 클리닝·TSP 를 섞어서) 돌린 뒤
스레드 수·전용 메모리가 늘지 않는다.

실행 프로그램과 같은 조립(sim.rig.Ch12FullRig)을 가상 시계로 돌린다. 한 회 = 웹 명령으로 시작 → 끝까지 → 다음.
종류는 CH1 → CH2 → PC → TSP 순으로 25번씩(100회). RUNS//10 회마다(기본 10회) gc 뒤 표본(스레드 수·전용 메모리·기록 큐·출력 큐)을 잰다.
단언: 30회 이후(첫 바퀴의 worker·스레드 생성이 끝난 뒤) 같은 종류 바퀴 끝에서 스레드 수가 같고, 마지막 40회 사이 전용 메모리 증가가
GROWTH_MB 안쪽, 큐가 쌓이지 않음, 오류 0. 시험 본문은 느리므로 slow 이다(-m "not slow" 에서 빠짐).
"""
from __future__ import annotations

import gc
import threading

import pytest

from v2.common.commands import Source
from v2.ch12 import process_states as PS
from v2.ch12.sim import soak
from pathlib import Path

from v2.common.clock import VirtualClock
from v2.ch12.sim.rig import Ch12FullRig
from v2.ch12.system import APP_RETENTION
from v2.tests.test_sim_comprehensive import CFG, P1, P2, PC_UI, W0, drive, ends, web

RUNS = int(__import__("os").environ.get("CH12_REPEAT_RUNS", "100"))
GROWTH_MB = 3.0                       # 60~100회 사이 전용 메모리 증가 상한(7일 시험과 같은 종류의 기준)
STEP = max(1, RUNS // 10)
KINDS = ("ch1", "ch2", "pc", "tsp")


def _idle(rig) -> bool:
    a = rig.asm
    return not (a.proc["ch1"].busy or a.proc["ch2"].busy or a.pc.running or a.tsp.busy)


def _one(rig, kind: str, i: int) -> None:
    a = rig.asm
    if kind in ("ch1", "ch2"):
        n = 1 if kind == "ch1" else 2
        r = web(rig, "PROCESS_START", {"ch": n, "params": P1 if n == 1 else P2}, by=f"rep{i}@web")
        assert r.ok, (i, kind, r)
        drive(rig, lambda: _idle(rig), 3000)
        assert ends(a.proc[kind])[-1] == ("done", None), (i, kind, ends(a.proc[kind])[-3:])
    elif kind == "pc":
        r = web(rig, "PC_START", {"ch": 1, "params": PC_UI}, by=f"rep{i}@web")
        assert r.ok, (i, kind, r)
        drive(rig, lambda: _idle(rig), 3000)
        assert a.pc.last_final["status"] == "success", (i, a.pc.last_final)
        rig.loop.run(62)                                         # PC 쿨다운
        for on in (True, False):                                 # PC 는 CH1 GV 를 연 채 끝냄(v1 PC-077) — 다음 CH1 공정 전에 닫는다
            assert rig.command("PLC_BUTTON", {"target": "g_v_1_close_sw", "on": on}, source=Source.HMI).ok
            rig.loop.run(2)
    else:
        r = web(rig, "TSP_START", {"target": "2.5e-7", "cycles": "1"}, by=f"rep{i}@web")
        assert r.ok, (i, kind, r)
        drive(rig, lambda: _idle(rig), 3000)
        rig.loop.run(31)                                         # TSP 쿨다운 여유


@pytest.mark.slow
def test_100_runs_threads_and_memory_flat(tmp_path):
    # 실행 프로그램과 같은 보존(APP_RETENTION) — 시험 도우미 full() 의 행·메시지 보관(시험 확인용 목록)은 쓰지 않는다(그 목록이 쌓이는 것이
    # 메모리 증가로 잡히므로)
    c = VirtualClock(start_wall=W0)
    c.register()
    rig = Ch12FullRig(c, Path(tmp_path), cfg=dict(CFG), robot_api=False, schedules=False, retention=APP_RETENTION)
    rig.sims["ch1.mfc"].sp[2] = 3.0 * 0.1
    rig.sims["ch2.mfc"].sp[3] = 5.0 * 0.1
    assert rig.start()
    rig.loop.run(2)
    rows = []
    try:
        for i in range(RUNS):
            _one(rig, KINDS[i % 4], i)
            if (i + 1) % STEP == 0:
                gc.collect()
                m = soak.mem()
                rows.append(dict(n=i + 1, threads=threading.active_count(), private_mb=m["private_mb"],
                                 rec_pending=rig.recorder._q.qsize(), out_pending=rig.outputs.pending))
        for r in rows:
            print(f"[repeat] {r}")
        by = {r["n"]: r for r in rows}
        assert rig.internal_errors == 0 and rig.loop.missed == 0, (rig.internal_errors, rig.loop.missed)
        assert list(rig.recorder.errors) == [] and list(rig.outputs.writer.errors) == [], (list(rig.recorder.errors)[:3])
        for r in rows:
            assert r["rec_pending"] < 100 and r["out_pending"] < 100, rows
        k40, k60, k100 = by[RUNS * 4 // 10], by[RUNS * 6 // 10], by[RUNS]
        assert k100["threads"] <= k40["threads"] and k60["threads"] <= k40["threads"], rows     # 스레드가 새지 않음
        if k60["private_mb"] is not None:
            assert k100["private_mb"] - k60["private_mb"] < GROWTH_MB, rows
        assert PS.chamber_state(rig.asm.proc["ch1"]) != "RUNNING"
    finally:
        rig.stop()
        rig.clock.close()

"""전수 목록 영역 ui(v2/ch12/spec/inv_ui.py) — v1 노트북 화면 동작 중 '장비 쪽 명령'으로 옮긴 것을 아직 다른 시험이 덮지 않은 부분.

화면 표시(버튼·칸·로그 줄·팝업 글)는 포털 vanam-process components/ops2/ch12(노트북 local 빌드와 같은 부품)가 맡고,
그 확인은 vanam-process/scripts/ops2-ch12-ui-check.mjs(이 저장소의 sim 시험대를 돌려 받은 묶음·state·사건으로 부품을 그려 본다).
여기는 장비 쪽: Power Select(SW_POWER_SELECT 코일 215·로그·실패 문구), 종료 거부 판단(v1 closeEvent 실행 중 목록 — UI-042·UI-148).
시험대: sim.rig.Ch12FullRig(실행 프로그램과 같은 조립 — 시뮬레이터·실제 드라이버·엔진·core, 가상 시계). v1 을 import 하지 않는다.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.common.commands import Source
from v2.ch12 import app as A
from v2.ch12.drivers import plc_map as PM
from v2.ch12.sim.rig import Ch12FullRig

HMI = Source.HMI
TEST_ROWS = [{"#": "test", "Process_name": "T1", "time": "60s"}]


@pytest.fixture
def rig(tmp_path):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    r = Ch12FullRig(c, Path(tmp_path), cfg={"CAMERA_MIN_FREE_GB": 0.0}, robot_api=False)
    assert r.start()
    r.loop.run(2)
    yield r
    r.stop()
    c.close()


def _logs(rig, n0=0):
    return [(e.source, e.kind, e.detail, e.level) for e in list(rig.events)[n0:]]


def test_power_select_coil_215_and_v1_log(rig):
    """UI-032 — main.py:1241-1243·device/plc.py:145·1787-1789: 토글마다 SW_POWER_SELECT(코일 215 = M00137, momentary 아님) 쓰기,
    로그 'POWER_SELECT(SW) <- {on}'. 켬·끔 모두 그 코일(SW_RF_SELECT 214 가 아님 — 결정 D06)."""
    assert PM.COIL_MAP["SW_POWER_SELECT"] == 215 and PM.COIL_MAP["SW_RF_SELECT"] == 214
    sim = rig.sims["plc"]
    n0 = len(rig.events)
    assert rig.command("POWER_SELECT", {"on": True}, source=HMI).ok
    rig.loop.run(2)
    assert sim.coil("SW_POWER_SELECT") and not sim.coil("SW_RF_SELECT")
    assert rig.command("POWER_SELECT", {"on": False}, source=HMI).ok
    rig.loop.run(2)
    assert not sim.coil("SW_POWER_SELECT")
    lines = [d for s, k, d, _ in _logs(rig, n0) if k == "log" and d.startswith("POWER_SELECT(SW)")]
    assert lines == ["POWER_SELECT(SW) <- True", "POWER_SELECT(SW) <- False"]


def test_power_select_write_failure_log_and_link_down(rig):
    """UI-033 — main.py:1244-1255: 쓰기 실패면 'Power Select 실패(PLC 연결 확인): …' 경고 로그(버튼 원복은 화면 — 장비 상태를 그대로
    보여 주므로 v1 의 글자 불일치 UI-142 는 생기지 않는다: 화면은 코일 값(plc.sw_power_select)을 그린다). PLC 연결이 없으면
    명령 자체를 LINK_DOWN 으로 거부(v1 은 쓰기 예외 → 같은 문구)."""
    sim = rig.sims["plc"]
    t = rig.clock.monotonic()
    sim.add_fault("no_response", t + 0.5, t + 30.0)
    n0 = len(rig.events)
    rig.loop.run(1)
    assert rig.command("POWER_SELECT", {"on": True}, source=HMI).ok          # 연결은 살아 있음 — 쓰기가 응답 없이 실패
    rig.loop.run(12)
    warn = [(d, lv) for s, k, d, lv in _logs(rig, n0) if d.startswith("Power Select 실패(PLC 연결 확인): ")]
    assert warn and warn[0][1] == "warn", _logs(rig, n0)[-10:]
    assert not sim.coil("SW_POWER_SELECT")
    t = rig.clock.monotonic()
    sim.add_fault("disconnect", t + 0.1, t + 60.0)
    rig.loop.run(25)
    r = rig.command("POWER_SELECT", {"on": True}, source=HMI)
    assert (r.ok, r.reason) == (False, "LINK_DOWN")


def test_exit_check_lists_v1_running_labels(rig):
    """UI-042·UI-148 — main.py:1120-1168·1487-1526: 실행 중 목록 'CH1 Sputter'(is_running)·'Plasma Cleaning (CHn)'·'TSP'
    가 있으면 종료 거부(v1 WARN/EXIT '프로그램 종료 차단: 실행 중 공정 = …'), 없으면 종료. busy 만 참인 구간(시작 접수 직후·
    큐 행 사이 등)은 막지 않는다(v1 이상 동작 그대로 — D120)."""
    app = A.App("unused", print_fn=lambda s: None)
    app.system = rig
    assert app.exit_check() == ("ok", "")
    e = rig.asm.proc["ch1"]
    assert rig.command("RECIPE_LOAD", {"ch": 1, "rows": TEST_ROWS, "name": "t"}, source=HMI).ok
    assert rig.command("PROCESS_START", {"ch": 1}, source=HMI).ok
    rig.loop.run(3)
    assert e.is_running
    kind, msg = app.exit_check()
    assert kind == "refuse" and msg.startswith("프로그램 종료 차단: 실행 중 공정 = CH1 Sputter")
    rs = rig.run_state
    rs.mark_started("pc", 2)
    rs.mark_started("tsp")
    assert app.exit_check()[1].startswith("프로그램 종료 차단: 실행 중 공정 = CH1 Sputter, Plasma Cleaning (CH2), TSP")
    rs.mark_finished("pc", 2)
    rs.mark_finished("tsp")
    assert rig.command("PROCESS_STOP", {"ch": 1}, source=HMI).ok
    rig.loop.run(30)
    assert not e.is_running and app.exit_check() == ("ok", "")
    # busy 만(정지 순서·쿨다운 대기 등 is_running 아님) — v1 처럼 막지 않음
    e._cmd = ("START", "local")                   # 시작 명령 접수 직후(러너가 아직 집지 않음) = v1 is_busy 만 참
    try:
        assert e.busy and not e.is_running and app.exit_check() == ("ok", "")
    finally:
        e._cmd = None

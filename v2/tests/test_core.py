"""챔버12 명령 처리 한 곳(core.Ch12Core.command) — 출처 거부·인자 거부·상태 거부·인터락(시작 가드)·ALL STOP·bench·동시 호출.

시험대: sim.rig.Ch12FullRig(실행 프로그램과 같은 조립 assembly.build — 시뮬레이터·실제 드라이버·실제 엔진, 가상 시계).
TEST 행(v1 '#' = test — 장비 제어 없이 시간만)으로 공정 상태를 만든다(가드·상태 판정이 실제 엔진 상태에서 나오는지).
"""
import dataclasses
import threading
import time
from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.common.commands import Source
from v2.ch12 import commands as C
from v2.ch12.core import CmdResult
from v2.ch12.sim.rig import Ch12FullRig

TEST_ROWS = [{"#": "test", "Process_name": "T1", "time": "20s"}]
WEB, HMI, ROBOT = Source.WEB, Source.HMI, Source.ROBOT


def make(tmp, cfg=None):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = Ch12FullRig(c, Path(tmp), cfg=dict({"CAMERA_MIN_FREE_GB": 0.0}, **(cfg or {})), robot_api=False)
    assert rig.start()
    rig.loop.run(2)
    return rig


def done(rig):
    rig.stop()
    rig.clock.close()


@pytest.fixture
def rig(tmp_path):
    r = make(tmp_path)
    yield r
    done(r)


def test_system_command_is_core_and_unknown(rig):
    assert rig.command == rig.core.command and rig.hub is not None
    r = rig.command("NOPE", {}, source=WEB)
    assert isinstance(r, CmdResult) and (r.ok, r.reason) == (False, "UNKNOWN_COMMAND")
    assert rig.command("PROCESS_STOP", {"ch": 1}, source="local").ok             # 문자열 출처도(로컬 서버 'local')
    assert rig.command("PROCESS_STOP", {"ch": 1}, source="nope").reason == "SOURCE_NOT_ALLOWED"


def test_source_rejected(rig):
    assert rig.command("CONFIG_APPLY", {"version": 2, "values": {}, "schemaHash": "x"}, source=HMI).reason == \
        "SOURCE_NOT_ALLOWED"
    assert rig.command("ROBOT_GET_SPUTTER_STATUS", {}, source=WEB).reason == "SOURCE_NOT_ALLOWED"
    assert rig.command("PROCESS_ADJUST", {"ch": 1, "device": "mfc", "item": "ar_flow", "value": 1}, source=ROBOT).reason == \
        "SOURCE_NOT_ALLOWED"
    assert rig.command("PROCESS_START", {"ch": 1}, source=HMI).reason == "NO_RECIPE"         # 기본 — 로컬에서도 받음
    rig.cfg["COMMAND_SOURCES_LOCAL_ALL"] = False                                             # 설정만 바꿔 '포털에서만'
    r = rig.command("PROCESS_START", {"ch": 1}, source=HMI)
    assert r.reason == "SOURCE_NOT_ALLOWED" and "hmi" in r.detail
    assert rig.command("RECIPE_LOAD", {"ch": 1, "rows": TEST_ROWS}, source=HMI).reason == "SOURCE_NOT_ALLOWED"
    assert rig.command("PROCESS_START", {"ch": 1}, source=WEB).reason == "NO_RECIPE"
    assert rig.command("PROCESS_STOP", {"ch": 1}, source=HMI).ok                            # STOP 은 늘 로컬에서도
    rig.cfg["COMMAND_SOURCES_LOCAL_ALL"] = True


def test_args_rejected_with_detail(rig):
    r = rig.command("PROCESS_STOP", {}, source=HMI)
    assert (r.reason, r.detail) == ("BAD_ARGS", "ch — 값 필요")
    assert rig.command("PROCESS_STOP", {"ch": 3}, source=HMI).detail == "ch 3 — 범위 1 ~ 2"
    assert rig.command("PLC_BUTTON", {"target": "nope", "on": True}, source=WEB).reason == "BAD_ARGS"
    assert rig.command("RECIPE_LOAD", {"ch": 1}, source=WEB).detail == "rows·blocks·path 중 하나 필요"
    r = rig.command("RECIPE_LOAD", {"ch": 1, "rows": [{"Process_name": "x", "base_pressure": "abc"}]}, source=WEB)
    assert r.reason == "E230" and "숫자가 아닙니다" in r.detail                               # 엔진 문구(reject_detail)
    assert rig.command("TSP_SCHEDULE", {"on": True, "time": "25:99"}, source=WEB).reason == "TIME_FORMAT"
    assert rig.command("PRESPUTTER_RESERVE", {"ch": 1, "time": "x"}, source=WEB).reason == "TIME_FORMAT"
    assert rig.command("HEATER_APPLY", {"target": "abc"}, source=WEB).reason == "BAD_ARGS"


def test_state_rejected_and_start_guard_interlocks(rig):
    e = rig.asm.proc["ch1"]
    assert rig.command("PROCESS_ADJUST", {"ch": 1, "device": "mfc", "item": "ar_flow", "value": 5}, source=WEB).reason == \
        "STATE_NOT_ALLOWED"
    assert rig.command("RECIPE_LOAD", {"ch": 1, "rows": TEST_ROWS}, source=WEB).ok
    assert rig.command("PROCESS_START", {"ch": 1}, source=WEB).ok
    rig.loop.run(2)
    assert e.state_name == "RUNNING" and rig.run_state.is_running("chamber", 1)
    r = rig.command("PROCESS_START", {"ch": 1}, source=WEB)
    assert r.reason == "STATE_NOT_ALLOWED" and "RUNNING" in r.detail
    assert rig.command("RECIPE_LOAD", {"ch": 1, "rows": TEST_ROWS}, source=WEB).reason == "STATE_NOT_ALLOWED"
    t = rig.command("TSP_START", {}, source=WEB)                                       # v1 TSP 가드 — CH1 공정 중
    assert t.reason == "CH1_PROCESS_RUNNING"
    pc = rig.command("PC_START", {"ch": 1}, source=WEB)                                # v1 PC 가드 — 같은 CH 챔버 바쁨
    assert (pc.reason, pc.detail) == ("E205", "CH1는 이미 다른 공정이 실행 중입니다.")
    rig.loop.run(30)
    assert e.state_name == "IDLE" and [x.data.get("result") for x in e.events if x.kind == "run_end"] == ["done"]
    assert rig.command("RECIPE_LOAD", {"ch": 1, "rows": TEST_ROWS}, source=WEB).ok
    r = rig.command("PROCESS_START", {"ch": 1}, source=WEB)                            # v1 쿨다운 60 s
    assert r.reason == "COOLDOWN" and r.detail.startswith("cooldown ")
    assert any(x.kind == "popup" and "1분 대기" in x.detail for x in e.events)
    # 같은 CH 플라즈마 클리닝 실행 중 → 챔버 시작 거부(v1 4404-4411), 다른 CH 는 허용
    e2 = rig.asm.proc["ch2"]
    assert rig.command("RECIPE_LOAD", {"ch": 2, "rows": TEST_ROWS}, source=WEB).ok
    rig.run_state.mark_started("pc", 2)
    r = rig.command("PROCESS_START", {"ch": 2}, source=WEB)
    assert (r.reason, r.detail) == ("PC_RUNNING", "CH2 Plasma Cleaning 실행 중") and not e2.busy
    rig.run_state.mark_finished("pc", 2)
    rig.run_state._last_finish["pc"].clear()
    assert rig.command("PROCESS_START", {"ch": 2}, source=WEB).ok


def test_all_stop_does_not_wait_for_lock_and_stops_everything(rig):
    for ch in (1, 2):
        assert rig.command("RECIPE_LOAD", {"ch": ch, "rows": TEST_ROWS}, source=WEB).ok
        assert rig.command("PROCESS_START", {"ch": ch}, source=WEB).ok
    rig.loop.run(2)
    assert all(rig.asm.proc[c].state_name == "RUNNING" for c in ("ch1", "ch2"))
    got = []
    with rig.core._lock:                                    # 다른 명령이 잠금을 쥔 동안에도
        th = threading.Thread(target=lambda: got.append(rig.command("ALL_STOP", {}, source=HMI)))
        th.start()
        th.join(5)
    assert got and got[0].ok and got[0].value == ["ch1", "ch2", "pc", "tsp"]
    rig.loop.run(5)
    for c in ("ch1", "ch2"):
        e = rig.asm.proc[c]
        assert e.state_name == "IDLE" and [(x.data.get("result"), x.code) for x in e.events if x.kind == "run_end"] == \
            [("aborted", "USER_STOP")]
    kinds = [(x.kind, x.rule) for x in rig.safety.events]
    assert ("fired", "S30") in kinds and ("released", "S30") in kinds and rig.safety.stopping is None


def test_bench_rejects_device_writes_only(rig):
    rig.policy = dataclasses.replace(rig.policy, device_writes=False)
    try:
        for kind, a in (("PROCESS_STOP", {"ch": 1}), ("ALL_STOP", {}), ("PLC_BUTTON", {"target": "vent_1_sw", "on": True}),
                        ("PROCESS_START", {"ch": 1}), ("POWER_SELECT", {"on": True}), ("TSP_START", {}),
                        ("ROBOT_VACUUM_ON", {"data": {}})):
            src = ROBOT if kind.startswith("ROBOT_") else WEB
            assert rig.command(kind, a, source=src).reason == "BENCH_READ_ONLY", kind
        assert rig.command("RECIPE_LOAD", {"ch": 1, "rows": TEST_ROWS}, source=WEB).ok           # 장비에 쓰지 않음
        assert rig.command("CHAMBER_ERROR_CLEAR", {"ch": 1}, source=HMI).ok
        r = rig.command("ROBOT_GET_LOADING_1_SENSOR", {"data": {}, "raw": {"request_id": "b"}}, source=ROBOT)
        assert r.ok and r.value[0] == "GET_LOADING_1_SENSOR_RESULT"
        bench_ok = {c.kind for c in C.COMMANDS if c.bench_ok}
        assert "RECIPE_LOAD" in bench_ok and "PROCESS_STOP" not in bench_ok
    finally:
        rig.policy = dataclasses.replace(rig.policy, device_writes=True)


def test_concurrent_calls_run_one_at_a_time(rig):
    """여러 스레드가 동시에 불러도 잠금 하나로 하나씩 — 처리기 안에 같은 순간 둘이 들어가지 않는다."""
    inside, peak, lk = [0], [0], threading.Lock()
    orig = rig.core.handlers["RECIPE_CLEAR"]

    def slow(a, src, by):
        with lk:
            inside[0] += 1
            peak[0] = max(peak[0], inside[0])
        time.sleep(0.01)
        with lk:
            inside[0] -= 1
        return orig(a, src, by)
    rig.core.handlers["RECIPE_CLEAR"] = slow
    res = []
    ths = [threading.Thread(target=lambda i=i: res.append(rig.command("RECIPE_CLEAR", {"ch": 1 + i % 2}, source=WEB)))
           for i in range(12)]
    for t in ths:
        t.start()
    for t in ths:
        t.join(10)
    rig.core.handlers["RECIPE_CLEAR"] = orig
    assert len(res) == 12 and all(r.ok for r in res) and peak[0] == 1


def test_internal_error_does_not_raise(rig):
    def boom(a, src, by):
        raise ValueError("x")
    rig.core.handlers["LOG_CLEAR"] = boom
    r = rig.command("LOG_CLEAR", {}, source=HMI)
    assert (r.ok, r.reason) == (False, "INTERNAL_ERROR") and "ValueError" in r.detail
    assert any(e.kind == "internal_error" and e.code == "INTERNAL_ERROR" for e in rig.events)


def test_panel_and_plc_commands_reach_engines(rig):
    sim = rig.sims["plc"]
    assert rig.command("PLC_BUTTON", {"target": "vent_1_sw", "on": True}, source=HMI).ok
    assert rig.command("POWER_SELECT", {"on": True}, source=HMI).ok
    assert rig.command("BUZZ_STOP", {}, source=HMI).ok                                   # v1 device/plc.py:1810 buzzer_stop
    rig.loop.run(2)
    assert sim.coil("VENT_1_SW") and sim.coil("SW_POWER_SELECT") and sim.coil("BUZZER_STOP_SW")
    assert rig.command("PC_SELECT_CH", {"ch": 2}, source=HMI).ok and rig.asm.pc.ch == 2
    assert rig.command("TSP_SCHEDULE", {"on": True, "time": "05:10"}, source=WEB).ok
    st = rig.asm.tsp.schedule.state()
    assert rig.asm.tsp.schedule.alive and st["checked"] and st["time_text"] == "05:10"
    assert rig.command("TSP_SCHEDULE", {"on": False}, source=WEB).ok and not rig.asm.tsp.schedule.alive
    assert rig.command("PRESPUTTER_RESERVE", {"ch": 1, "time": "06:30"}, source=WEB).ok
    assert rig.asm.presputter["ch1"].is_reserved
    assert rig.command("PRESPUTTER_CANCEL", {"ch": 1}, source=WEB).ok and not rig.asm.presputter["ch1"].is_reserved
    assert rig.command("THICKNESS_RELOAD", {}, source=HMI).ok
    assert rig.command("THICKNESS_ZERO", {"ch": 1}, source=HMI).reason == "BAD_ARGS"       # CH2 만(ch 2)
    assert rig.command("LOG_CLEAR", {"target": "ch1"}, source=HMI).ok
    assert rig.command("RUNTIME_DUMP", {"reason": "manual_button"}, source=HMI).ok

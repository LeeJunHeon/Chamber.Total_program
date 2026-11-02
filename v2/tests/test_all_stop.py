"""ALL STOP(S30 — 결정 D05, 사용자 결정 2026-11-02) 시험: CH1·CH2·플라즈마 클리닝·TSP 를 한꺼번에 멈춘다(각각 v1 정지 순서 그대로).

- 동시에 도는 조합마다(가드가 허용하는 짝: CH1+CH2 · CH1+PC2 · CH2+PC1 · CH2+TSP · PC2+TSP — TSP 는 CH1·PC1 과, PC 는 같은 CH 챔버와 같이
  못 돈다) ALL STOP 한 번에 둘 다 멈추고 장비가 꺼진다. 넷에 보내는 순서는 CH1·CH2·PC·TSP(안 도는 쪽은 무시).
- 하나가 이미 정지 순서 중일 때(CH1 STOP 뒤 곧바로 ALL STOP) — 정지 순서를 두 번 시작하지 않고 둘 다 끝까지.
- 재시작 이어가기 판단 중일 때 — 아직 판단하지 않은 부분은 이어가지 않고 정지 순서(확인 all_stop), 이미 이어간 부분은 판단이 끝나는 대로 STOP.
- 이어가기로 되살아난 공정(CH1·PC·TSP)도 ALL STOP 으로 멈춘다.
"""
import json
from pathlib import Path

import pytest

from v2.common.commands import Source
from v2.tests import restart_rig as RR
from v2.tests import test_resume as T

LONG_CH1 = dict(T.P_CH1, process_time=3.0)
LONG_CH2 = dict(T.P_CH2, process_time=3.0)
PC_PARAMS = dict(T.PC_PARAMS, process_time="3")
TSP_CFG = {"TSP_ON_SEC": 20.0, "TSP_OFF_SEC": 20.0, "TSP_POLL_SEC": 2.0, "TSP_FIRST_CHECK_DELAY_SEC": 3.0,
           "TSP_TOTAL_TIMEOUT_MARGIN_S": 600.0}
PAIRS = [("ch1", "ch2"), ("ch1", "pc2"), ("ch2", "pc1"), ("ch2", "tsp"), ("pc2", "tsp")]


def _start(rig, what: str):
    if what in ("ch1", "ch2"):
        e = rig.eng[what]
        assert e.start(params=dict(LONG_CH1 if what == "ch1" else LONG_CH2, process_note=what), owner="local",
                       source="local") is None
        return e
    if what.startswith("pc"):
        e = rig.engine("pc")
        assert e.start(params=PC_PARAMS, ch=int(what[2]), source="local").accepted
        return e
    e = rig.engine("tsp")
    assert e.start(params={"target": "1e-12", "cycles": "5"}, source="local")["ok"]
    return e


def _running(what: str, e) -> bool:
    if what in ("ch1", "ch2"):
        return e.runner.is_running and not e.runner.shutdown
    if what.startswith("pc"):
        return e.running and e.params is not None and e.phase == "run"
    return e.busy and e.phase in ("tsp_on", "tsp_off")


def _ended(what: str, e) -> bool:
    return not e.busy


def _check_stopped(rig, what: str, e):
    if what in ("ch1", "ch2"):
        assert [(x.data.get("result"), x.code) for x in e.events if x.kind == "run_end"] == [("aborted", "USER_STOP")], what
    elif what.startswith("pc"):
        assert e.last_final["stopped"] is True, e.last_final
    else:
        assert e.last_result["outcome"] == "stopped", e.last_result


def _devices_off(rig):
    v = rig.journal.last_view
    assert not v.last("plc.dcv_set_1") and not v.last("plc.dcv_write_1")
    for k in ("ch1.rfpulse", "ch1.dcpulse", "ch2.rfpulse", "ch2.dcpulse"):
        assert not rig.sims[k].out, k
    assert rig.sims["tsp"].on is False
    assert rig.internal_errors == 0
    assert not [x for x in rig.safety.events if x.kind == "error"]


@pytest.mark.parametrize("a,b", PAIRS)
def test_all_stop_stops_concurrent_pair(tmp_path, a, b):
    rig = RR.boot(tmp_path, cfg=TSP_CFG)
    try:
        RR.drive(rig, lambda: rig.resumer.done, 30)
        ea, eb = _start(rig, a), _start(rig, b)
        RR.drive(rig, lambda: _running(a, ea) and _running(b, eb), 900)
        assert _running(a, ea) and _running(b, eb), (a, b)
        r = rig.command("ALL_STOP", {}, source=Source.HMI)
        assert r.ok
        step = [x for x in rig.safety.events if x.kind == "step" and x.rule == "S30"]
        assert step and step[-1].detail == "보냄: ch1, ch2, pc, tsp"                   # 순서 고정, 안 도는 쪽도 보냄(무시)
        assert rig.safety.stopping and rig.safety.stopping.get("allStop")
        RR.drive(rig, lambda: _ended(a, ea) and _ended(b, eb) and rig.safety.stopping is None, 900)
        assert _ended(a, ea) and _ended(b, eb) and rig.safety.stopping is None
        _check_stopped(rig, a, ea)
        _check_stopped(rig, b, eb)
        _devices_off(rig)
    finally:
        RR.finish(rig)


def test_all_stop_while_one_is_already_stopping(tmp_path):
    """CH1 STOP 으로 정지 순서가 시작된 직후 ALL STOP — 정지 순서를 두 번 만들지 않고 둘 다 끝까지(CH1 run_end 는 하나)."""
    rig = RR.boot(tmp_path)
    try:
        RR.drive(rig, lambda: rig.resumer.done, 30)
        e1, e2 = _start(rig, "ch1"), _start(rig, "ch2")
        RR.drive(rig, lambda: _running("ch1", e1) and _running("ch2", e2), 900)
        assert rig.command("PROCESS_STOP", {"ch": 1}, source=Source.HMI).ok
        RR.drive(rig, lambda: e1.runner.shutdown, 60)
        assert e1.runner.shutdown and _running("ch2", e2)                          # CH1 은 정지 순서 중, CH2 는 공정 중
        assert rig.command("ALL_STOP", {}, source=Source.WEB).ok
        RR.drive(rig, lambda: not e1.busy and not e2.busy and rig.safety.stopping is None, 900)
        assert not e1.busy and not e2.busy
        for e in (e1, e2):
            assert [(x.data.get("result"), x.code) for x in e.events if x.kind == "run_end"] == [("aborted", "USER_STOP")]
        _devices_off(rig)
    finally:
        RR.finish(rig)


def _crash_two(tmp):
    """CH1(메인 대기)과 CH2 가 같이 돌다 죽는다."""
    rig = RR.boot(tmp)
    RR.drive(rig, lambda: rig.resumer.done, 30)
    e1, e2 = _start(rig, "ch1"), _start(rig, "ch2")
    RR.drive(rig, lambda: e1.cur_inst is not None and e1.cur_inst.type == "main" and e1.runner.is_running, 900)
    RR.drive(rig, lambda: False, 4)
    RR.kill(rig)
    return rig


def test_all_stop_during_resume_judgement_does_not_resume(tmp_path):
    """재시작 이어가기 판단 중에 ALL STOP — 아직 판단하지 않은 부분은 이어가지 않고 정지 순서(확인 all_stop)."""
    rig = _crash_two(tmp_path)
    rig2 = RR.reboot(tmp_path, rig, 2.0)
    try:
        assert not rig2.resumer.done
        assert rig2.command("ALL_STOP", {}, source=Source.HMI).ok
        RR.drive(rig2, lambda: rig2.resumer.done and not rig2.safety.any_busy() and rig2.safety.stopping is None, 1500)
        dec = rig2.resumer.decisions
        for ch in ("ch1", "ch2"):
            if ch in dec:
                assert dec[ch]["result"] == "stopped" and "all_stop" in T.failed(dec[ch]), dec[ch]
            assert RR.ends(rig2.eng[ch]) == [("error", "PROGRAM_RESTART")] or ch not in dec
        assert rig2.safety.stopping is None and not rig2.safety.any_busy()
        v = rig2.journal.last_view
        assert not v.last("plc.main_shutter_1_sw") and not v.last("plc.main_1_gas_sw")
        assert not rig2.sims["ch1.rfpulse"].out
        assert rig2.internal_errors == 0
    finally:
        RR.finish(rig2)


def test_all_stop_right_after_resume_stops_resumed_process(tmp_path):
    """이어가기로 되살아난 CH1·CH2 공정에 ALL STOP — 정지 순서로 끝(USER_STOP)."""
    rig = _crash_two(tmp_path)
    rig2 = RR.reboot(tmp_path, rig, 2.0)
    try:
        RR.drive(rig2, lambda: rig2.resumer.done, 600)
        assert {ch: rig2.resumer.decisions[ch]["result"] for ch in ("ch1", "ch2")} == {"ch1": "resumed", "ch2": "resumed"}
        assert rig2.command("ALL_STOP", {}, source=Source.HMI).ok
        RR.drive(rig2, lambda: not rig2.safety.any_busy() and rig2.safety.stopping is None, 1500)
        for ch in ("ch1", "ch2"):
            assert RR.ends(rig2.eng[ch]) == [("aborted", "USER_STOP")], (ch, RR.ends(rig2.eng[ch]))
        assert not rig2.sims["ch1.rfpulse"].out
        assert rig2.internal_errors == 0
    finally:
        RR.finish(rig2)


def test_all_stop_stops_resumed_pc(tmp_path):
    """이어가기로 되살아난 플라즈마 클리닝(CH1 선택)에 ALL STOP."""
    rig = RR.boot(tmp_path, cfg=TSP_CFG)
    RR.drive(rig, lambda: rig.resumer.done, 30)
    pc = _start(rig, "pc1")
    RR.drive(rig, lambda: pc.running and pc._current_step() == "process" and (pc.countdown_left or 999) < 150, 900)
    RR.kill(rig)
    rig2 = RR.reboot(tmp_path, rig, 2.0, cfg=TSP_CFG)
    try:
        RR.drive(rig2, lambda: rig2.resumer.done, 600)
        assert rig2.resumer.decisions["pc"]["result"] == "resumed"
        pc2 = rig2.engine("pc")
        assert rig2.command("ALL_STOP", {}, source=Source.HMI).ok
        RR.drive(rig2, lambda: not pc2.busy and rig2.safety.stopping is None, 900)
        assert pc2.last_final["stopped"] is True
        assert not rig2.journal.last_view.last("plc.dcv_set_1")
        assert rig2.internal_errors == 0
    finally:
        RR.finish(rig2)

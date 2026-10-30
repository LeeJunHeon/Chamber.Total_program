"""로봇 명령 처리(v2/ch12/robot_ops) — v1 host/handlers.py 를 실제로 돌린 골든과 명령 × 상태 전 조합 비교.

골든 v2/tests/golden/robot_handlers.json 은 v2/tools/v1golden_robot_handlers.py 가 v1 핸들러·라우터·AsyncPLC·요청 공정 로그를
가상 시각 루프에서 실제로 실행해 만든 것(사례 표 v2/tools/robot_golden_cases.py — 사례가 골든 안에 같이 들어 있다).
여기서는 같은 사례를 v2 시험대(sim.robot_rig.RobotRig — v2 PLC 시뮬레이터·worker·드라이버·가짜 공정 어댑터, 가상 시계)로 돌려
  응답 명령·body 바이트(JSON 직렬화)·로그 줄·PLC 에 쓴 코일 순서·Robot CSV 사본 행이 같은지, 응답까지 가상 초가 같은지(±0.05 s) 본다.
가리는 것(골든과 같은 정규화 sim.robot_rig.normalize): 로그 시각·기록키 시각·경로, 핸들러 예외 traceback, PLC 무응답 문구
  (v1 은 pymodbus 예외 문구, v2 는 드라이버 문구 — 자리표 <PLC_ERR>, differences 에 기록).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from v2.ch12 import robot_base
from v2.ch12.sim import robot_rig as RR

GOLDEN = json.loads((Path(__file__).parent / "golden" / "robot_handlers.json").read_text(encoding="utf-8"))
CASES = GOLDEN["cases"]
TIME_TOL_S = 0.05


@pytest.fixture
def plc_err_placeholder(monkeypatch):
    monkeypatch.setattr(robot_base, "plc_error_text", lambda code, res, op, addr: RR.PLC_ERR)


def run_v2(case: dict, tmp: Path) -> dict:
    rig = RR.RobotRig(out_dir=tmp, robot_csv=True)
    try:
        o = rig.run_case(case)
    finally:
        rig.done()
    roots = [(o["recipe"], "<RECIPE>"), (str(Path(rig.cfg["ROBOT_RECIPE_ROOT_DIR"])), "<ROOT>")]
    return RR.normalize(o, roots)


def test_golden_shape():
    from v2.ch12.robot_ops import COMMANDS
    assert GOLDEN["meta"]["cases"] == len(CASES) >= 200
    cmds = {c["cmd"].upper().strip() for c in CASES} | {j[1] for c in CASES for j in (c.get("jobs") or [])}
    assert set(COMMANDS) <= cmds, set(COMMANDS) - cmds          # 18개 명령 모두 사례가 있다


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_matches_v1(case, tmp_path, plc_err_placeholder):
    want = case["golden"]
    got = run_v2(case, tmp_path)
    assert len(got["results"]) == len(want["results"])
    for g, w in zip(got["results"], want["results"]):
        assert g["reply_cmd"] == w["reply_cmd"]
        assert g["body"] == w["body"]
        assert g["log"] == w["log"]
        assert abs(g["elapsed"] - w["elapsed"]) <= TIME_TOL_S, (g["elapsed"], w["elapsed"])
    assert got["writes"] == want["writes"]
    assert got["robot_csv"] == want["robot_csv"]


# ───────────────────────── v2 쪽 부품 ─────────────────────────
def test_heartbeat_paused_during_plc_calls(tmp_path):
    """v1 _plc_call 의 pause_watchdog/resume_watchdog — PLC 구간마다 하트비트를 쉬고 반드시 다시 켠다."""
    rig = RR.RobotRig(out_dir=tmp_path)
    seen = []
    orig = rig.plc.read_coil

    def spy(*a, **k):
        seen.append((rig.plc.hb_paused, rig.link.robot_active))
        return orig(*a, **k)
    rig.ops.plc = rig.link.proxy("ROBOT")
    rig.plc.read_coil = spy
    try:
        rig.set_engines(None)
        out = rig.run("CH1_GATE_CLOSE")
    finally:
        rig.done()
    assert out["body"]["result"] == "success" and seen and all(p and n == 1 for p, n in seen)
    assert rig.plc.hb_paused is False and rig.link.robot_active == 0


def test_reports_and_adapter_protocol(tmp_path):
    from v2.ch12.robot_engines import ADAPTER_METHODS, RobotEngines
    rig = RR.RobotRig(out_dir=tmp_path)
    try:
        eng = rig.set_engines({"rs": {}, "rt": {"1": {}}, "pc": {"selected_ch": 1}})
        assert isinstance(eng, RobotEngines) and all(callable(getattr(eng, m)) for m in ADAPTER_METHODS)
        rig.run("START_SPUTTER", {"ch": 9, "recipe": "x.csv"})
    finally:
        rig.done()
    assert rig.reports and rig.reports[-1][0] == "HOST" and rig.reports[-1][1]["error_code"] == "E201"


def test_helpers():
    from v2.ch12.robot_base import cfg_tuple, peer_text
    from v2.ch12.robot_engines import loadlock_error_channel
    assert peer_text("127.0.0.1:50123") == "('127.0.0.1', 50123)" and peer_text("weird") == "weird"
    assert cfg_tuple('["CH1", "CH2", "ALD"]') == ("CH1", "CH2", "ALD") and cfg_tuple(["A"]) == ("A",)
    assert loadlock_error_channel({"finished": {1: 5.0, 2: 9.0}, "error": {1: True}}) is None
    assert loadlock_error_channel({"finished": {1: 9.0, 2: 5.0}, "error": {1: True}}) == 1
    assert loadlock_error_channel({"finished": {}, "error": {2: True}}) == 2


def test_default_recipe_scan(tmp_path):
    from v2.ch12.robot_engines import scan_recipe_folder
    (tmp_path / "CH1").mkdir()
    for n in ("b.csv", "A.XLSX", "x.txt"):
        (tmp_path / "CH1" / n).write_text("x")
    assert scan_recipe_folder(str(tmp_path / "CH1"), 5.0) == ["A.XLSX", "b.csv"]
    with pytest.raises(FileNotFoundError):
        scan_recipe_folder(str(tmp_path / "nope"), 5.0)


def test_clock_lock_waits_on_virtual_clock():
    from v2.common.clock import VirtualClock
    from v2.ch12.robot_base import ClockLock
    c = VirtualClock()
    c.register()
    lk = ClockLock(c)
    order = []

    def other():
        with lk:
            order.append(("b", c.monotonic()))
    with lk:
        c.start_thread(other, "lk-other")
        c.wait_until(c.monotonic() + 2.0)                   # 다른 스레드가 잠금을 기다리는 동안 시각이 넘어간다
        assert lk.locked() and order == []
    c.wait_until(c.monotonic() + 1.0, lambda: bool(order))
    assert order == [("b", 2.0)]

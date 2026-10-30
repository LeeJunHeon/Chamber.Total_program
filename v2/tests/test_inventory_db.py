"""재고 DB Gun Target(v2/ch12/inventory_db.py) — v1 util/inventory_client.py·chamber_runtime.py 5080-5098·8925-8950 그대로.

모두 127.0.0.1 가짜 서버(v2/ch12/sim/inventory_sim.py)만 쓴다 — 실제 URL 에 접속하지 않는다(sim·bench 모드는 밖 URL 을 쓰지 않음을
확인하는 시험 포함). 끝에 실제 공정 엔진(Ch12Rig) 큐 행이 받은 값을 공정 기록(Calib CSV 의 G1 Target)에 싣는지 본다.
"""
from __future__ import annotations

import concurrent.futures as cf
import csv
import time
from types import SimpleNamespace

import pytest

from v2.ch12 import inventory_db as INV
from v2.ch12.sim.inventory_sim import SIM_SLOTS, FakeInventoryServer

SLOTS = [
    {"locationName": "Chamber 1 - Gun 1", "itemName": "Ti", "targetUnitId": 11},
    {"locationName": "Chamber 1 - Gun 2", "itemName": "X", "targetUnitId": 12},
    {"locationName": "Chamber 2 - Gun 1", "itemName": "Al", "targetUnitId": 21},
    {"locationName": "Chamber 2 - Gun 3", "itemName": "ITO", "targetUnitId": None},
    {"locationName": "Shelf A", "itemName": "nope", "targetUnitId": 1},
    {"locationName": "Chamber x - Gun 1", "itemName": "nope2"},
    {"itemName": "no location"},
]


@pytest.fixture
def srv():
    with FakeInventoryServer(SLOTS) as s:
        yield s


def _log():
    lines = []
    return lines, (lambda src, msg: lines.append((src, msg)))


def test_parse_slot_and_targets_rules():
    assert INV.parse_slot("Chamber 1 - Gun 1") == (1, "G1 Target")
    assert INV.parse_slot(" Chamber 2 -  Gun 3 ") == (2, "G3 Target")
    assert INV.parse_slot("Chamber 1 - Gun 1 - x") is None and INV.parse_slot("Shelf") is None
    assert INV.targets_from_slots(SLOTS, 1) == {"G1 Target": "Ti (11)", "G2 Target": "X (12)", "G3 Target": ""}
    assert INV.targets_from_slots(SLOTS, 2) == {"G1 Target": "Al (21)", "G2 Target": "", "G3 Target": "ITO"}
    with pytest.raises(AttributeError):                 # v1: 목록이 아니면(사전 → 키 문자열) slot.get 에서 예외
        INV.targets_from_slots({"a": 1}, 1)


def test_wait_targets_from_fake_server(srv):
    db = INV.InventoryDb(srv.url)
    lines, log = _log()
    try:
        assert db.wait_targets(2, log) == {"G1 Target": "Al (21)", "G2 Target": "", "G3 Target": "ITO"}
        assert db.latest(2)["G1 Target"] == "Al (21)" and db.latest(1) == {"G1 Target": "", "G2 Target": "", "G3 Target": ""}
        assert lines == [] and srv.requests == ["/inventory/api/chamber-slots"]
    finally:
        db.close()


@pytest.mark.parametrize("kind", ["http500", "not_json", "refused"])
def test_network_failure_is_empty_without_log(kind):
    if kind == "refused":
        db = INV.InventoryDb("http://127.0.0.1:9/inventory/api/chamber-slots")
        lines, log = _log()
        assert db.wait_targets(1, log) == {"G1 Target": "", "G2 Target": "", "G3 Target": ""}
        db.close()
        assert lines == []
        return
    s = FakeInventoryServer(status=500) if kind == "http500" else FakeInventoryServer(body=b"<html>")
    with s:
        db = INV.InventoryDb(s.url)
        lines, log = _log()
        assert db.wait_targets(1, log) == {"G1 Target": "", "G2 Target": "", "G3 Target": ""}
        db.close()
    assert lines == []                                   # v1: 네트워크 실패 → 빈 딕셔너리, 로그 없음


def test_bad_shape_logs_load_failed():
    with FakeInventoryServer(body=b'{"slots": []}') as s:
        db = INV.InventoryDb(s.url)
        lines, log = _log()
        assert db.wait_targets(2, log) is None
        db.close()
    assert lines == [("TARGET", "[CH2] 타겟 정보 불러오기 실패: AttributeError(\"'str' object has no attribute 'get'\")")]


def test_wait_over_3s_logs_timeout_and_late_result_not_used():
    with FakeInventoryServer(SLOTS, delay_s=0.6) as s:
        db = INV.InventoryDb(s.url, wait_s=0.2)          # v1 3 s → 시험은 0.2 s(같은 경로)
        lines, log = _log()
        t0 = time.monotonic()
        assert db.wait_targets(1, log) is None
        assert time.monotonic() - t0 < 0.5
        assert lines == [("TARGET", "[CH1] Gun Target DB 3초 초과 — 빈 값으로 진행 (공정 진행)")]
        time.sleep(0.6)
        db.close()
    assert INV.START_WAIT_S == 3.0 and INV.HTTP_TIMEOUT_S == 5.0


def test_wait_exception_logs_db_failed():
    db = INV.InventoryDb("http://127.0.0.1:1/x")
    db.request = lambda n, log: (_ for _ in ()).throw(RuntimeError("pool down"))
    lines, log = _log()
    assert db.wait_targets(1, log) is None
    assert lines == [("TARGET", "[CH1] Gun Target DB 실패: RuntimeError('pool down') — 빈 값으로 진행")]
    db.close()


def test_mode_policy_no_external_access_outside_prod(monkeypatch):
    calls = []
    monkeypatch.setattr(INV, "http_get_slots", lambda url, timeout_s=5.0: calls.append(url) or [])
    cfg = {"INVENTORY_API_URL": INV.DEFAULT_URL}
    prod = INV.make_inventory(cfg, "prod")
    assert prod.url == INV.DEFAULT_URL                   # 실행 프로그램만 밖 URL(여기서는 접속하지 않는다)
    prod.close()
    bench = INV.make_inventory(cfg, "bench")
    lines, log = _log()
    assert not bench.enabled and bench.wait_targets(1, log) is None and lines == []
    bench.close()
    sim = INV.make_inventory(cfg, "sim")                  # 밖 URL 대신 127.0.0.1 가짜 서버
    try:
        assert sim.url.startswith("http://127.0.0.1:") and sim.fake is not None
    finally:
        sim.close()
    loop = INV.make_inventory({"INVENTORY_API_URL": "http://127.0.0.1:5/x"}, "bench")
    assert loop.url == "http://127.0.0.1:5/x"
    loop.close()
    assert calls == []


def test_sim_mode_fake_server_serves_sim_slots():
    db = INV.make_inventory({}, "sim")
    lines, log = _log()
    try:
        assert db.wait_targets(1, log) == INV.targets_from_slots(SIM_SLOTS, 1)
        assert db.latest(1)["G1 Target"] == "SIM-Ti (101)"
    finally:
        db.close()


def test_refresh_does_not_wait(srv):
    srv.delay_s = 0.3
    db = INV.InventoryDb(srv.url)
    t0 = time.monotonic()
    fut = db.refresh(2)
    assert time.monotonic() - t0 < 0.1 and isinstance(fut, cf.Future)
    assert fut.result(timeout=5)["G1 Target"] == "Al (21)"
    db.close()
    assert INV.InventoryDb(None).refresh(1) is None


def test_row_empty_and_apply_rules():
    assert INV.row_targets_empty({"G1 Target": "", "G2_target_name": " "})
    assert not INV.row_targets_empty({"G2_target_name": "Cu"})
    assert INV.row_targets_empty(None)
    got = {"G1 Target": "A", "G2 Target": "B", "G3 Target": "C"}
    p1 = {"use_g1": False, "use_g2": True}
    INV.apply_targets(p1, got, 1)                         # CH1: G1 만(사용 여부와 무관)
    assert p1 == {"use_g1": False, "use_g2": True, "G1 Target": "A", "G1_target_name": "A"}
    p2 = {"use_g1": True, "use_g2": False, "use_g3": True, "G3 Target": "mine"}
    INV.apply_targets(p2, got, 2)                         # CH2: use_gN 인 건만, 값이 있으면 그대로
    assert p2["G1 Target"] == "A" and "G2 Target" not in p2 and p2["G3 Target"] == "mine"


def test_attach_engine_hook(srv):
    lines = []
    eng = SimpleNamespace(n=2, log=lambda s, m: lines.append((s, m)), gun_targets=None)
    db = INV.InventoryDb(srv.url)
    try:
        INV.attach(eng, db)
        run = SimpleNamespace(raw={"Process_name": "a", "G1 Target": ""}, params={"use_g1": True, "use_g3": True})
        eng.gun_targets(run)
        assert run.params["G1 Target"] == "Al (21)" and run.params["G3 Target"] == "ITO"
        run2 = SimpleNamespace(raw={"G2 Target": "Cu"}, params={"use_g1": True})
        eng.gun_targets(run2)                              # 행에 하나라도 있으면 DB 를 부르지 않는다
        assert "G1 Target" not in run2.params and len(srv.requests) == 1
        run3 = SimpleNamespace(raw=None, params=None)      # 처리 예외 → 로그만(러너를 멈추지 않음)
        db.wait_targets = lambda n, log: (_ for _ in ()).throw(ValueError("x"))
        eng.gun_targets(run3)
        assert lines[-1] == ("TARGET", "[CH2] Gun Target DB 실패: ValueError('x') — 빈 값으로 진행")
    finally:
        db.close()


def test_engine_queue_row_gets_db_target_in_calib_csv(tmp_path, srv):
    """실제 엔진 큐(파일 레시피 행 — G Target 비움) → 재고 DB 값이 공정 params·Calib CSV G1 Target 에."""
    from v2.ch12.recipe import TEMPLATE_COLUMNS as CSV_COLUMNS
    from v2.tests import proc_rig as R
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    db = INV.InventoryDb(srv.url)
    INV.attach(e, db)
    row = {k: "" for k in CSV_COLUMNS}
    row.update({"Process_name": "inv", "base_pressure": "1e-5", "working_pressure": "3", "process_time": "0.05",
                "shutter_delay": "0", "Ar": "T", "Ar_flow": "20", "use_rf_pulse": "T", "rf_pulse_power": "100",
                "rf_pulse_freq": "0.5", "rf_pulse_duty_cycle": "50", "main_shutter": "F", "gun1": "T"})
    assert e.load([row], source="local", name="inv") is None
    try:
        R.run(rig, R.idle(e), max_ticks=1500, hooks=[R.at(2, lambda: e.start(owner="local", source="local"))])
    finally:
        db.close()
    assert R.ends(e) == [("done", None)], R.logs(e)[-20:]
    calib = list(tmp_path.rglob("Ch1_log.csv"))
    assert calib, "Calib CSV 없음"
    with open(calib[0], encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows[-1]["G1 Target"] == "Ti (11)"
    assert srv.requests == ["/inventory/api/chamber-slots"]

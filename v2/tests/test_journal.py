"""1초 저널(v2/ch12/journal.py) — 원자적 저장·깨진 파일·내용·쓰기 스레드·의도적 종료 표시·공백 계산.

내용 시험은 실제 조립(가상 시계 + 시뮬레이터 + assembly.build — restart_rig)에서 CH1 공정을 돌리며 저널 파일을 읽는다:
run id·소유자·레시피 전체·판 번호(공정 중 편집)·지금 블록·블록/메인 경과·공정 중 수정값·보낸 설정값·기대 장비 상태·마지막 저장 시각
(벽시계·단조)·재시작 횟수, 공유 부분(히터·처리한 명령 id — 로봇 내부 명령 API).
"""
import json
import os
import threading
import time

import pytest

from v2.common.clock import VirtualClock
from v2.ch12 import journal as J
from v2.tests import restart_rig as RR

P_CH1 = dict(base_pressure=1e-5, working_pressure=3, process_time=1.0, shutter_delay=0.2, use_ar=True, use_o2=True,
             ar_flow=20, o2_flow=1, use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True)


# ───────────── 원자적 저장·깨진 파일 ─────────────
def test_atomic_write_replaces_and_leaves_no_tmp(tmp_path):
    p = tmp_path / "j" / "ch1.json"
    J.atomic_write(p, b'{"a":1}')
    J.atomic_write(p, b'{"a":2}')
    assert p.read_bytes() == b'{"a":2}' and not list(p.parent.glob("*.tmp"))


def test_atomic_write_crash_before_replace_keeps_old(tmp_path, monkeypatch):
    p = tmp_path / "ch1.json"
    J.atomic_write(p, b"old")

    def boom(a, b):
        raise OSError("power cut")
    monkeypatch.setattr(J.os, "replace", boom)
    with pytest.raises(OSError):
        J.atomic_write(p, b"new-half")
    assert p.read_bytes() == b"old"                       # 옛 파일 그대로 — 반쯤 쓴 파일 없음


def test_atomic_write_retries_permission_error(tmp_path, monkeypatch):
    p = tmp_path / "ch1.json"
    real, calls = os.replace, []

    def flaky(a, b):
        calls.append(1)
        if len(calls) < 3:
            raise PermissionError("locked by scanner")
        real(a, b)
    monkeypatch.setattr(J.os, "replace", flaky)
    J.atomic_write(p, b"x")
    assert p.read_bytes() == b"x" and len(calls) == 3


def test_read_part_missing_corrupt_version(tmp_path):
    assert J.read_part(tmp_path / "none.json") == (None, "missing")
    bad = tmp_path / "ch1.json"
    bad.write_bytes(b'{"v": 1, "saved_wall": 1.0, "part": "ch1"')        # 잘린 파일
    d, err = J.read_part(bad)
    assert d is None and err.startswith("corrupt") and (tmp_path / "ch1.json.corrupt").exists() and not bad.exists()
    old = tmp_path / "ch2.json"
    old.write_text(json.dumps({"v": 99, "saved_wall": 1.0}), encoding="utf-8")
    assert J.read_part(old)[1].startswith("corrupt")
    good = tmp_path / "pc.json"
    good.write_text(json.dumps({"v": J.VERSION, "saved_wall": 5.0, "part": "pc"}), encoding="utf-8")
    assert J.read_part(good) == ({"v": J.VERSION, "saved_wall": 5.0, "part": "pc"}, None)
    got = J.load(tmp_path)
    assert got["pc"]["error"] is None and got["tsp"]["error"] == "missing"


def test_gap_same_boot_uses_monotonic_reboot_uses_wall():
    c = VirtualClock(start_wall=1000.0)
    c.advance(50.0)
    doc = {"saved_wall": c.wall(), "saved_mono": c.monotonic(), "boot": J.boot_mark(c)}
    c.advance(12.5)
    assert J.gap_of(doc, c) == (12.5, False)
    c2 = VirtualClock(start_wall=1000.0 + 50.0 + 40.0)                  # 재부팅: 단조 시계 0 부터, 벽시계는 40 s 뒤
    gap, reboot = J.gap_of(doc, c2)
    assert reboot and gap == pytest.approx(40.0)


# ───────────── 실제 조립에서 내용 ─────────────
def test_journal_content_during_process(tmp_path):
    rig = RR.boot(tmp_path, robot_api=True)
    try:
        e = rig.eng["ch1"]
        RR.drive(rig, lambda: rig.resumer.done, 30)
        assert e.start(params=dict(P_CH1, process_note="jr"), owner="web", source="web") is None
        RR.drive(rig, lambda: e.cur_inst is not None and e.cur_inst.type == "main" and e.runner.remaining_s() is not None
                 and e.runner.remaining_s() < 50, 400)
        assert e.adjust("mfc", "ar_flow", 22.0, source="web") is None        # 공정 중 수정
        assert e.edit_sequence({"op": "insert", "at": len(e.blocks) - 1,
                                "block": {"type": "wait", "params": {"sec": 1}}}, source="web") is None
        api = rig.asm.robot_api
        api._run("GET_SPUTTER_STATUS", {"id": "rob-1", "cmd": "GET_SPUTTER_STATUS", "data": {}})
        rig.clock.register()                               # _run 이 요청 스레드처럼 등록·해제했다
        RR.drive(rig, lambda: False, 3)
        rig.journal.flush()                                # 쓰기 스레드는 실제 시간 — 가상 tick 이 앞서 가므로 지금 것을 쓴다
        d = json.loads((tmp_path / "data" / "journal" / "ch1.json").read_text(encoding="utf-8"))
        st = d["state"]
        assert d["v"] == J.VERSION and d["active"] and d["run_id"] == e.run_id and d["restarts"] == 0 and d["exit"] is None
        assert abs(d["saved_wall"] - rig.clock.wall()) <= 1.5 and abs(d["saved_mono"] - rig.clock.monotonic()) <= 1.5
        assert st["owner"] == "web" and st["block_type"] == "main" and st["resumable"] and st["step_holding"]
        assert st["recipe"]["rev"] == 1 and len(st["recipe"]["blocks"]) == len(st["recipe"]["original"]) + 1
        assert st["overrides"] == {"mfc.ar_flow": 22.0} and st["writes"]["flow"]["1"] == 22.0
        assert st["block_elapsed_s"] > 10 and st["main_elapsed_s"] > st["block_elapsed_s"]
        assert st["countdown"]["total_ms"] == 60000 and st["countdown"]["msg"].startswith("메인 공정 진행")
        assert st["writes"]["power"] == {"rfp": 120.0} and st["outputs"]["rfp"]["bound"]
        assert {"type", "result"} <= set(st["executed"][0]) and st["txt_path"].endswith(".txt")
        exp = d["expect"]
        assert exp["shutter"]["plc.main_shutter_1_sw"] is True and exp["gas"]["plc.ar_1_gas_sw"] is True
        assert exp["power_output"]["ch1.rfpulse.out_on"] is True and exp["power_output"]["ch1.rfpulse.set_w"] == 120.0
        idle = json.loads((tmp_path / "data" / "journal" / "ch2.json").read_text(encoding="utf-8"))
        assert idle["active"] is False and idle["run_id"] is None
        sh = json.loads((tmp_path / "data" / "journal" / "shared.json").read_text(encoding="utf-8"))
        assert any(k == "rob-1" for k, _v in sh["commands"]["robot"]["done"])
        assert sh["heater"]["run"] is False
        for part in ("pc", "tsp"):
            assert json.loads((tmp_path / "data" / "journal" / f"{part}.json").read_text(encoding="utf-8"))["active"] is False
        assert rig.journal.errors == 0 and rig.journal.writes >= 5 * 3
    finally:
        RR.finish(rig)


def test_on_row_does_not_block_on_disk(tmp_path, monkeypatch):
    """디스크 쓰기는 쓰기 스레드에서 — on_row(tick) 는 느린 디스크를 기다리지 않는다."""
    rig = RR.boot(tmp_path, start=False)
    slow = threading.Event()
    real = J.atomic_write

    def slow_write(p, b):
        slow.wait(2.0)
        real(p, b)
    monkeypatch.setattr(J, "atomic_write", slow_write)
    try:
        t0 = time.perf_counter()
        for i in range(5):
            rig.journal.on_row({"seq": i, "ts": "x"})
        assert time.perf_counter() - t0 < 1.0
        slow.set()
        deadline = time.time() + 5
        while rig.journal.writes < 5 and time.time() < deadline:
            time.sleep(0.05)
        assert rig.journal.writes >= 5                     # 최신 것만 — 부분마다 한 번 이상
    finally:
        slow.set()
        RR.finish(rig)


def test_hold_mark_exit_and_freeze(tmp_path):
    rig = RR.boot(tmp_path, start=False)
    j = rig.journal
    try:
        j.flush()
        p = tmp_path / "data" / "journal" / "ch1.json"
        before = json.loads(p.read_text(encoding="utf-8"))
        j.hold(["ch1"])
        j.flush()
        assert json.loads(p.read_text(encoding="utf-8"))["seq"] == before["seq"]      # 잡아 둔 부분은 덮어쓰지 않음
        j.release("ch1")
        j.mark_exit("CTRL_CLOSE")
        for part in J.PARTS:
            d = json.loads((tmp_path / "data" / "journal" / f"{part}.json").read_text(encoding="utf-8"))
            assert d["exit"]["intentional"] is True and d["exit"]["why"] == "CTRL_CLOSE"
        n = j.writes
        j.on_row({"seq": 99, "ts": "x"})
        time.sleep(0.2)
        assert j.writes == n and j.frozen                  # 표시 뒤 1초 쓰기 멈춤
    finally:
        RR.finish(rig)

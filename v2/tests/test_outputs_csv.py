"""v1 기록 출력 — CSV 계열(Sputter Calib CSV·PLC 코일 CSV·두께 연속 CSV·CH2 히터 운전 CSV) 골든 바이트 비교 + D07(로컬 먼저).

골든은 v1 코드(controller/data_logger.py·device/plc.py 의 _plc_coil_log_loop·lib/thickness_logger.py·lib/heater_logger.py)를
실제로 돌려 만든 파일(v2/tools/v1golden_outputs.py).
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from v2.ch12.outputs.base import QueueWriter
from v2.ch12.outputs.calib_csv import CalibCsv, build_row, header_for
from v2.ch12.outputs.heater_csv import HeaterCsv
from v2.ch12.outputs.paths import CopySender
from v2.ch12.outputs.plc_coil_csv import PlcCoilCsv, v1_coil_keys
from v2.ch12.outputs.run_stats import RunStats
from v2.ch12.outputs.thickness_csv import ThicknessCsv
from v2.tests.outputs_golden import GOLDEN, Clock, assert_same_tree, load, make_root, v2_cfg, write_pre
from v2.tools import v1golden_outputs_scen as SC

_KEEP = lambda rel: not rel.startswith("로컬정본")   # noqa: E731


def _calib_run(tmp_path, name):
    scen = load(name)
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG, scen.get("cfg"))
    write_pre(root, scen.get("pre_files"))
    clock = Clock(scen.get("t0", SC.T0))
    w = QueueWriter("t")
    s = CopySender(cfg, retry_s=3600)
    logs = []
    c = CalibCsv(scen["ch"], cfg, thickness_columns=scen.get("thickness", False), log_func=logs.append,
                 writer=w, sender=s)
    try:
        for rec in scen["records"]:
            for rel in rec.get("unblock", []):
                (root / rel).unlink()
                (root / rel).mkdir(parents=True)
            st = RunStats(now=clock.now)
            st.apply_events(rec["events"])
            c.record(st, rec["ok"])
            w.flush()
            s.flush()
            if rec.get("stage"):
                assert_same_tree(root, GOLDEN / name / f"files_{rec['stage']}", _KEEP)
        assert_same_tree(root, GOLDEN / name / "files", _KEEP)
    finally:
        w.stop()
        s.stop()
    return root, c, logs


@pytest.mark.parametrize("name", sorted(SC.CALIB))
def test_calib_golden(tmp_path, name):
    root, c, logs = _calib_run(tmp_path, name)
    if name == "calib_upgrade_fail":
        assert any("머리글 갱신 실패 — 기존 열 순서로 기록합니다" in x for x in logs)
    # D07: 로컬 정본에 같은 형식으로 먼저(머리글 = 새 머리글, 행 = 이번 기록들)
    local = root / "로컬정본" / "Calib" / f"Ch{c.ch}_log.csv"
    rows = local.read_bytes().decode("utf-8-sig").splitlines()
    assert rows[0].split(",")[0] == "Timestamp" and len(rows) == 1 + len(load(name)["records"])


def test_calib_pending_then_merge_d07(tmp_path):
    """G: 가 막히면 행은 로컬 정본 + v1 pending 에, 풀리면 사본 스레드가 다음 회차에 보낸다(다음 기록을 기다리지 않음)."""
    root = make_root(tmp_path)
    write_pre(root, {"G/막힘": "x"})
    cfg = v2_cfg(root, SC.BASE_CFG, {"SPUTTER_CALIB_DB_DIR": "@/G/막힘/Database"})
    w = QueueWriter("t")
    s = CopySender(cfg, retry_s=0.05)
    logs = []
    c = CalibCsv(1, cfg, log_func=logs.append, writer=w, sender=s)
    st = RunStats(now=Clock("2026-10-30T11:00:00").now)
    st.apply_events([["start", dict(SC.P_CH1, t0_wall="2026-10-30T11:00:00")]])
    try:
        c.record(st, True)
        w.flush()
        assert not s.flush(1.0)                        # 실패 → 남아 있음(다음에 다시)
        assert c.pending_file.exists() and (root / "로컬정본" / "Calib" / "Ch1_log.csv").exists()
        assert sum("NAS 기록 실패" in x for x in logs) == 1   # 재시도마다 되풀이하지 않음
        (root / "G" / "막힘").unlink()
        (root / "G" / "막힘").mkdir()
        for _ in range(200):
            if not c.pending_file.exists():
                break
            threading.Event().wait(0.01)
        assert not c.pending_file.exists()
        nas = (root / "G" / "막힘" / "Database" / "Ch1_log.csv").read_bytes()
        assert nas.startswith(b"\xef\xbb\xbfTimestamp,") and nas.count(b"\r\n") == 2
        assert any("pending → NAS 병합 완료" in x for x in logs)
    finally:
        w.stop()
        s.stop()


def test_calib_record_does_not_block(tmp_path):
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG)
    gate = threading.Event()
    w = QueueWriter("t")
    w.put(gate.wait)
    s = CopySender(cfg, retry_s=3600)
    c = CalibCsv(2, cfg, writer=w, sender=s)
    st = RunStats()
    st.start({"use_ms": False})
    row = c.record(st, False)
    assert row["Result"] == "FAIL" and c.record(st, None) is None      # None = 기록 생략(v1)
    assert list(row) == header_for(2, False)
    gate.set()
    w.stop()
    s.stop()


def test_calib_build_row_formats():
    st = RunStats(now=lambda: datetime(2026, 10, 30, 8, 0, 0))
    st.start({"t0_wall": 1761811200.0, "use_dc_pulse": True, "dc_pulse_off_time_us": "3.33", "process_time": 1})
    r = build_row(1, st, True, False)
    assert r["Timestamp"] == datetime.fromtimestamp(1761811200.0).strftime("%Y-%m-%d %H:%M:%S")
    assert r["DC Pulse: Off Time"] == "3.3" and r["Process Time"] == "1" and r["Base Pressure"] == "0.00e+00"


# ───────────────────────── PLC 코일 CSV ─────────────────────────
def test_plc_coil_golden(tmp_path):
    scen = load("plc_coil")
    assert v1_coil_keys() == scen["coil_keys"]          # v2 spec 표 = v1 PLC_COIL_MAP(정의 순)
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG, scen["cfg"])
    t0 = datetime.fromisoformat(scen["t0"])
    w = QueueWriter("t")
    logs = []
    pc = PlcCoilCsv(cfg, reg_keys=scen["reg_keys"], writer=w, log=logs.append)
    keys = scen["coil_keys"]
    wrote = []
    for t in range(len(scen["steps"]) * 5):            # 1초 tick, v1 은 5 s 주기
        st = scen["steps"][t // 5]
        kind = st["kind"]
        coils = None if kind == "empty" else {k: (k in set(st.get("on", []))) for k in keys}
        if pc.on_tick(t0 + timedelta(seconds=t), connected=kind != "disconnected", backoff=kind == "backoff",
                      coils=coils, regs=st.get("regs"), reg_failed=bool(st.get("reg_fail")), mono=float(t),
                      plan=0, blocks=2):
            wrote.append(t)
    pc.stop()
    w.stop()
    assert wrote == [0, 5, 25, 30, 35, 40, 45, 55, 60, 65, 70]
    assert_same_tree(root, GOLDEN / "plc_coil" / "files", _KEEP)
    want = [x for x in scen["logs"] if x.startswith("WARN PLC COIL LOG summary")]
    assert [x for x in logs if x.startswith("WARN PLC COIL LOG summary")] == want


def test_plc_coil_fallback_switch(tmp_path):
    root = make_root(tmp_path)
    (root / "막힘").write_text("x", encoding="utf-8")
    cfg = v2_cfg(root, SC.BASE_CFG, {"PLC_COIL_LOG_NAS_DIR": "@/막힘/PLC"})
    w = QueueWriter("t")
    logs = []
    pc = PlcCoilCsv(cfg, coil_keys=["A", "B"], reg_keys=["R"], writer=w, log=logs.append)
    pc.on_tick(datetime(2026, 10, 30, 1, 2, 3), connected=True, coils={"A": True}, regs={"R": 5}, mono=0.0)
    pc.stop()
    w.stop()
    f = root / "exe 폴더" / "Logs_LocalFallback" / "PLC_Coil" / "20261030.csv"
    assert f.read_bytes() == "\ufeffTimestamp,A,B,R\r\n2026-10-30T01:02:03,TRUE,FALSE,5\r\n".encode("utf-8")
    assert "PLC COIL LOG: NAS write failed -> switched to LOCAL (keep-handle)" in logs


# ───────────────────────── 두께·히터 ─────────────────────────
def test_thickness_golden(tmp_path):
    scen = load("thickness")
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG, scen["cfg"])
    clock = Clock(scen["t0"])
    w = QueueWriter("t")
    t = ThicknessCsv(scen["ch"], cfg, writer=w, monotonic=clock.monotonic)
    for ev in scen["events"]:
        if ev[0] == "t":
            clock.set(ev[1])
        elif ev[0] == "row":
            if not t.recording:
                t.set_recording(True, scen["process_name"])
            t.on_tick(ev[1], clock.now())
        elif ev[0] == "close":
            t.set_recording(False)
    t.stop()
    w.stop()
    assert_same_tree(root, GOLDEN / "thickness" / "files", _KEEP)


def test_thickness_gdrive_dir_replaced_and_fallback(tmp_path):
    from v2.ch12.outputs.paths import Paths
    assert Paths({"CH2_THICKNESS_LOG_DIR": r"G:\공유 드라이브\x"}).thickness_root(2) == r"C:\VanaM_Logs\CH1&2\Thickness"
    root = make_root(tmp_path)
    (root / "막힘").write_text("x", encoding="utf-8")
    cfg = v2_cfg(root, SC.BASE_CFG, {"THICKNESS_LOG_DIR": "@/막힘/Thickness"})
    warns = []
    w = QueueWriter("t")
    t = ThicknessCsv(1, cfg, writer=w, warn=warns.append)
    t.set_recording(True, "공정", {"connected": True}, datetime(2026, 10, 30, 9, 0, 0))
    t.stop()
    w.stop()
    assert (root / "exe 폴더" / "Logs_LocalFallback" / "Thickness" / "CH1" / "Thickness_20261030.csv").exists()
    assert warns and "를 쓸 수 없어" in warns[0]


def test_heater_golden(tmp_path):
    scen = load("heater")
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG, scen["cfg"])
    clock = Clock(scen["t0"])
    w = QueueWriter("t")
    h = HeaterCsv(cfg, writer=w, monotonic=clock.monotonic, now=clock.now)
    for ev in scen["events"]:
        if ev[0] == "t":
            clock.set(ev[1])
            clock.mono = ev[2]
        elif ev[0] == "start":
            h.start()
        elif ev[0] == "row":
            h.write_row(ev[1], **ev[2])
        elif ev[0] == "stop":
            h.stop()
    w.stop()
    assert_same_tree(root, GOLDEN / "heater" / "files", _KEEP)


def test_heater_on_tick_edges_and_suffix(tmp_path):
    """RUN 상승 엣지 → 새 파일(같은 초면 _2), 주기 3 s, 하강 엣지 → '운전 종료' 행 후 닫기."""
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG)
    clock = Clock("2026-10-30T10:00:00")
    w = QueueWriter("t")
    h = HeaterCsv(cfg, writer=w, monotonic=clock.monotonic, now=clock.now)
    for i in range(8):                       # 0~7 s RUN
        h.on_tick({"run": True, "pv": 20 + i}, owner_label="공정")
        clock.advance(1.0)
    h.on_tick({"run": False, "pv": 30}, owner_label="공정")
    h.on_tick({"run": True, "pv": 31})       # 10:00:08 새 파일
    h.on_tick({"run": False, "pv": 31})
    h.on_tick({"run": True, "pv": 32})       # 같은 초에 다시 RUN → _2
    h.on_tick({"run": False, "pv": 32})
    h.shutdown()
    w.stop()
    d = root / "로컬" / "VanaM_Logs" / "CH1&2" / "CH2_Heater"
    f1, f2 = d / "CH2_HEATER_20261030_100000.csv", d / "CH2_HEATER_20261030_100008_2.csv"
    lines = f1.read_text(encoding="utf-8-sig").splitlines()
    assert [x.split(",")[3] for x in lines[1:]] == ["20.0", "23.0", "26.0", "30.0"]
    assert lines[-1].endswith(",운전 종료") and (d / "CH2_HEATER_20261030_100008.csv").exists() and f2.exists()

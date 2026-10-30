"""v1 기록 출력 — 텍스트 계열(SYSTEM 일자 로그·공정 txt·ERROR 일자 로그·런타임 덤프·selfwatch) 골든 바이트 비교.

골든은 v1 코드를 실제로 돌려 만든 파일(v2/tools/v1golden_outputs.py). 경로는 한글이 든 임시 폴더.
"""
from __future__ import annotations

import json
import logging
import logging.handlers
import os
import queue
import re
from datetime import date, datetime

import pytest

from v2.ch12.outputs import app_log, runtime_dump
from v2.ch12.outputs.base import QueueWriter
from v2.ch12.outputs.process_txt import FOOTER_SETTLE_S, ProcessTxtLog, heater_status_line, safe_process_name
from v2.ch12.outputs.selfwatch import SelfWatchLog, check_thresholds
from v2.ch12.outputs.system_log import SystemLog
from v2.tests.outputs_golden import GOLDEN, Clock, assert_same_tree, load, make_root, norm_logs, v2_cfg
from v2.tools import v1golden_outputs_scen as SC

_NOT_LOCAL_FIRST = lambda rel: not rel.startswith("로컬정본")   # noqa: E731


def test_system_log_golden(tmp_path):
    scen = load("system_log")
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG)
    clock = Clock(scen["lines"][0][0])
    w = QueueWriter("t")
    sl = SystemLog(cfg, writer=w, now=clock.now)
    for t, src, msg in scen["lines"]:
        clock.set(t)
        sl.append(src, msg)
    w.stop()
    assert_same_tree(root, GOLDEN / "system_log" / "files", _NOT_LOCAL_FIRST)


@pytest.mark.parametrize("name", sorted(SC.PROCESS_TXT))
def test_process_txt_golden(tmp_path, name):
    scen = load(name)
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG)
    clock = Clock(scen["t0"])
    w = QueueWriter("t")
    ui = []
    t = ProcessTxtLog(scen["ch"], cfg, system_log=SystemLog(cfg, writer=w, now=clock.now), writer=w,
                      now=clock.now, on_ui=ui.append)
    for ev in scen["events"]:
        k = ev[0]
        if k == "t":
            clock.set(ev[1])
        elif k == "log":
            t.log(ev[1], ev[2])
        elif k == "side":
            t.side_log(ev[1], ev[2])
        elif k == "open":
            t.open_run(ev[1], origin=ev[2], origin_meta=ev[3])
        elif k == "drain":
            w.flush()
        elif k == "close":
            t.close_run()
    w.stop()
    assert_same_tree(root, GOLDEN / name / "files", _NOT_LOCAL_FIRST)
    assert norm_logs(root, ui) == scen["ui"]


def test_process_txt_fallback_and_rules(tmp_path):
    """LOG_ROOT_DIR 를 못 쓰면 LOCAL_FALLBACK_CH{n}_DIR\\<같은 이름> 으로 1회 전환('로컬 폴백으로 전환' 화면 1회)."""
    root = make_root(tmp_path)
    (root / "막힌 뿌리").write_text("파일", encoding="utf-8")       # 폴더 자리에 파일 → mkdir 실패
    cfg = v2_cfg(root, SC.BASE_CFG, {"LOG_ROOT_DIR": "@/막힌 뿌리/CH1&2"})
    clock = Clock("2026-10-30T10:00:00.250000")
    ui = []
    w = QueueWriter("t")
    t = ProcessTxtLog(1, cfg, writer=w, now=clock.now, on_ui=ui.append)
    p = t.open_run({"process_note": "폴백 시험"})
    t.log("Process", "한 줄")
    t.close_run()
    t.stop()
    w.stop()
    fb = root / "exe 폴더" / "Logs_LocalFallback" / "CH1" / p.name
    assert fb.read_text(encoding="utf-8").endswith("한 줄\n# ==== END ====\n")
    assert sum("세션 로그를 로컬 폴백으로 전환" in x for x in ui) == 1
    assert any("NAS 폴더 접근 실패" in x for x in ui)
    assert safe_process_name({}) == "Untitled" and FOOTER_SETTLE_S == 0.3
    assert heater_status_line(pv=200.04, tc_ok=False, sv_ramp=None, target=250, mv=1200.7, mv_limit=None,
                              holding=True, hold_value=1450, ramp_active=True, owner_label="공정") == \
        "히터 상태 — PV --.-°C · SV --.-°C · 목표 250.0°C · MV 1200/- · 유지(DAC 상한 1450) · 공정"


def test_process_txt_never_blocks(tmp_path):
    """쓰기 스레드가 막혀도 log 는 바로 돌아오고, 큐가 4096 을 넘으면 가장 오래된 줄부터 버린다(v1)."""
    import threading
    gate = threading.Event()
    w = QueueWriter("t")
    w.put(gate.wait)
    root = make_root(tmp_path)
    t = ProcessTxtLog(1, v2_cfg(root, SC.BASE_CFG), writer=w, now=Clock("2026-10-30T10:00:00").now)
    t.open_run({"process_note": "x"})
    for i in range(5000):
        t.log("P", f"{i}")
    assert t.dropped > 0
    gate.set()
    t.close_run()
    t.stop()
    w.stop()
    text = next((root / "로컬" / "VanaM_Logs" / "CH1&2" / "CH1").glob("*.txt")).read_text(encoding="utf-8")
    assert text.endswith("[CH1:P] 4999\n# ==== END ====\n") and "[CH1:P] 0\n" not in text


def _records(scen):
    out = []
    for r in scen["records"]:
        rec = logging.LogRecord(scen["logger"], getattr(logging, r["level"]), "x.py", 1, r["msg"], tuple(r["args"]),
                                (ValueError, ValueError(r["exc"]), None) if r.get("exc") else None)
        ts = datetime.fromisoformat(r["t"]).timestamp()
        rec.created = ts
        rec.msecs = int((ts - int(ts)) * 1000) + 0.0
        rec.process = 4242
        rec.thread = 777
        out.append(rec)
    return out


@pytest.mark.parametrize("via_queue", [False, True])
def test_app_log_golden(tmp_path, via_queue):
    scen = load("app_log")
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG)
    clock = Clock(scen["t0"])
    from v2.ch12.outputs.paths import Paths
    prim, fb = Paths(cfg).error_dirs()
    h = app_log.DailyFileHandler(scen["logger"], prim, fb, today=lambda: date(2026, 10, 30), now=clock.now)
    h.setFormatter(logging.Formatter(app_log.FORMAT, app_log.DATEFMT))
    if via_queue:      # setup_app_logging 의 길(QueueHandler → QueueListener)과 같은 결과인지
        q = queue.Queue()
        qh = logging.handlers.QueueHandler(q)
        lis = logging.handlers.QueueListener(q, h)
        lis.start()
        for rec in _records(scen):
            qh.emit(rec)
        lis.stop()
    else:
        for rec in _records(scen):
            h.emit(rec)
    h.close()
    assert_same_tree(root, GOLDEN / "app_log" / "files", _NOT_LOCAL_FIRST)
    assert [[s, m, app_log.classify_log_level(s, m)] for s, m in scen["classify"]] == scen["levels"]


def test_app_log_setup_files(tmp_path):
    """setup_app_logging: ERROR\\CH_1_2_program_YYYYMMDD.log + .fault.log, 'process exiting (atexit)'."""
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG)
    lg = app_log.setup_app_logging(cfg, app_name="CH_1_2_program_test", install_hooks=False)
    try:
        lg.warning("[%s] %s", "WARN", "경고 한글")
    finally:
        app_log.shutdown_app_logging()
        import faulthandler
        faulthandler.enable()          # pytest 의 faulthandler 를 되돌림(shutdown 이 disable 함)
    d = root / "로컬" / "VanaM_Logs" / "CH1&2" / "ERROR"
    log = next(d.glob("CH_1_2_program_test_????????.log"))
    assert re.fullmatch(r"CH_1_2_program_test_\d{8}\.log", log.name)
    text = log.read_text(encoding="utf-8")
    assert re.search(r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3} \[WARNING\] \[CH_1_2_program_test\] "
                     r"\[pid=\d+ tid=\d+\] \[WARN\] 경고 한글$", text, re.M)
    assert "process exiting (atexit)" in text and "faulthandler enabled ->" in text
    assert list(d.glob(f"CH_1_2_program_test_*_pid{os.getpid()}.fault.log"))


def test_runtime_dump_format(tmp_path):
    scen = load("runtime_dump")
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG)
    p = runtime_dump.request_dump(cfg, state=scen["state"], reason=scen["reason"], wait=True)
    assert p.parent == root / "로컬" / "VanaM_Logs" / "CH1&2" / "RUNTIME_DUMP"
    pat = re.escape(scen["v1_name_pattern"]).replace(r"<pid>", str(os.getpid()))
    pat = re.sub(r"\d{8}_\d{6}", r"\\d{8}_\\d{6}", pat)
    assert re.fullmatch(pat, p.name.replace(str(os.getpid()), str(os.getpid())))
    text = p.read_text(encoding="utf-8")
    snap = json.loads(text.split("=== SNAPSHOT(JSON) ===\n", 1)[1].split("\n\n=== THREAD TRACEBACK", 1)[0])
    assert list(snap) == scen["v1_snapshot_keys"] and list(snap["meta"]) == scen["v1_meta_keys"]
    assert snap["ui"] == scen["v1_ui"] and snap["asyncio"] == scen["v1_asyncio"]
    assert snap["objects"] == scen["v1_objects"]
    assert [text.split("\n", 1)[0], "=== THREAD TRACEBACK (faulthandler) ===" in text,
            text.endswith("\n[end]\n")] == scen["v1_frame"]
    with pytest.raises(RuntimeError, match="already in progress"):
        runtime_dump._DUMP_IN_PROGRESS = True
        try:
            runtime_dump.request_dump(cfg, state={}, reason="x")
        finally:
            runtime_dump._DUMP_IN_PROGRESS = False


def test_selfwatch_golden(tmp_path):
    scen = load("selfwatch")
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG, scen["cfg"])
    clock = Clock(scen["t0"])
    events = []
    w = QueueWriter("t")
    sw = SelfWatchLog(cfg, writer=w, now=clock.now, monotonic=clock.monotonic, on_event=lambda *a: events.append(a))
    reasons = []
    for t, sample in scen["samples"]:
        clock.set(t)
        s = sw.tick(sample)
        reasons.append(check_thresholds(cfg, s))
    w.stop()
    assert reasons == scen["reasons"]

    def _norm(rel, b):          # 덤프의 스레드 목록은 프로세스마다 다르다
        return b.split("[threads".encode("utf-8"))[0] if "dump_" in rel else b
    assert_same_tree(root, GOLDEN / "selfwatch" / "files", _NOT_LOCAL_FIRST, normalize=_norm)
    assert events and events[0][0] == "SELFWATCH" and "dump_20261030_090100.txt" in events[0][2]


def test_selfwatch_interval_and_purge(tmp_path):
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG, {"SELFWATCH_INTERVAL_S": 1.0})
    clock = Clock("2026-10-30T09:00:00")
    d = root / "로컬" / "VanaM_Logs" / "CH1&2" / "selfwatch"
    d.mkdir(parents=True)
    old = d / "selfwatch_20261001.csv"
    old.write_text("x", encoding="utf-8")
    os.utime(old, (clock.time() - 8 * 86400,) * 2)
    w = QueueWriter("t")
    sw = SelfWatchLog(cfg, writer=w, now=clock.now, monotonic=clock.monotonic, wall_time=clock.time)
    sw.start()
    n = 0
    for _ in range(20):           # 최소 5 s(v1 max(5, 값)) → 20 s 동안 첫 주기 뒤 3행
        clock.advance(1.0)
        n += sw.on_tick({"threads": 3}) is not None
    w.stop()
    assert n == 3 and not old.exists()
    assert (d / "selfwatch_20261030.csv").read_text(encoding="utf-8").count("\n") == 4

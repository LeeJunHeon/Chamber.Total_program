"""Robot_YYYYMMDD.csv(ALD 공용) — v1 util/host_process_log.py 골든 바이트 비교 + 실제 파일로 동시 쓰기·잠금·복구·중복 키.

골든: v1 HostProcessLog 를 고정 시계로 실제 실행(v2/tools/v1golden_outputs.py) — 기록키 중복 접미사(-2·-3), 거절 사유 한 줄,
NAS 중복(줄 끝 ',기록키'), 끊긴 줄 보정, 낡은 잠금(120 s·설정 예외면 30 s), 막힌 NAS → pending → 풀리면 전송, 재시작 복구.
ALD 동시 쓰기: 다른 프로세스(같은 잠금 규약 — O_EXCL 잠금 파일)가 같은 NAS 파일에 번갈아 쓴다.
"""
from __future__ import annotations

import csv
import io
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import pytest

from v2.ch12.outputs.base import QueueWriter
from v2.ch12.outputs.robot_csv import HEADER, RobotCsv, csv_line
from v2.tests.outputs_golden import GOLDEN, Clock, assert_same_tree, load, make_root, v2_cfg, write_pre
from v2.tools import v1golden_outputs_scen as SC

_KEEP = lambda rel: not rel.startswith("로컬정본") and "/SYSTEM/" not in rel   # noqa: E731


def _mk(cfg, w, clock, **kw):
    return RobotCsv(cfg, writer=w, now=clock.now, time_fn=clock.time, sleep_fn=clock.advance, pid=4242,
                    start_worker=False, **kw)


@pytest.mark.parametrize("name", sorted(SC.ROBOT))
def test_robot_golden(tmp_path, name):
    scen = load(name)
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG, scen.get("cfg"))
    for k in scen.get("del_cfg", []):
        cfg[k] = "not-a-number"
    clock = Clock(scen["t0"])
    write_pre(root, scen.get("pre_files"))
    for rel, spec in (scen.get("pre_files") or {}).items():
        if "age_s" in spec:
            ts = clock.time() - spec["age_s"]
            os.utime(root / rel, (ts, ts))
    w = QueueWriter("t")
    r = _mk(cfg, w, clock)
    keys = {}
    try:
        for ev in scen["events"]:
            k, a = ev[0], ev[1:]
            if k == "t":
                clock.set(a[0])
            elif k == "request":
                kw = dict(a[1])
                if kw.get("received_at"):
                    kw["received_at"] = datetime.fromisoformat(kw["received_at"])
                keys[a[0]] = r.request(**kw)
            elif k == "update":
                r.update(keys[a[0]], **a[1])
            elif k == "started":
                r.mark_started(keys[a[0]], clock.now(), a[1])
            elif k == "finalize":
                keys[a[0] + "_ret"] = r.finalize(keys[a[0]], a[1], a[2], clock.now())
            elif k == "reject":
                r.reject(keys[a[0]], a[1])
            elif k == "flush":
                w.flush()
                r.flush_once()
            elif k == "restart":
                w.flush()
                r = _mk(cfg, w, clock)
            elif k == "recover":
                r.startup_recover()
                w.flush()
            elif k == "unblock":
                (root / a[0]).unlink()
                (root / a[0]).mkdir(parents=True)
            elif k == "stage":
                w.flush()
                assert_same_tree(root, GOLDEN / name / f"files_{a[0]}", _KEEP)
        w.flush()
        assert keys == scen["keys"]
        assert_same_tree(root, GOLDEN / name / "files", _KEEP)
    finally:
        r.close()
        w.stop()


def test_robot_calls_never_block(tmp_path):
    """쓰기 스레드가 막혀도 request·finalize 는 바로 돌아온다(키 중복 검사는 메모리 거울)."""
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG)
    gate = threading.Event()
    w = QueueWriter("t")
    w.put(gate.wait)
    clock = Clock(SC.T0)
    r = _mk(cfg, w, clock)
    t0 = time.monotonic()
    k1 = r.request(target="CH1", request_id="X", peer="p", received_at=clock.now())
    k2 = r.request(target="CH1", request_id="X", peer="p", received_at=clock.now())
    assert r.finalize(k1, "성공") and not r.finalize(k1, "성공")         # 멱등
    assert time.monotonic() - t0 < 0.5 and k2 == k1 + "-2"
    gate.set()
    w.flush()
    k3 = r.request(target="CH1", request_id="X", peer="p", received_at=clock.now())
    assert k3 == k1 + "-3"                                  # pending 에 있는 키와도 겹치지 않음(직전 키와도)
    r.close()
    w.stop()


def test_robot_recover_skips_keys_already_pending(tmp_path):
    """open 에 남은 키가 이미 pending 에 있으면(기록 직후 꺼짐) open 에서만 지우고 '중단' 줄을 쓰지 않는다."""
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG)
    clock = Clock(SC.T0)
    w = QueueWriter("t")
    r = _mk(cfg, w, clock)
    k = r.request(target="CH1", request_id="A", peer="p")
    r.finalize(k, "성공")
    w.flush()
    local = Path(cfg["HOST_LOG_LOCAL_DIR"])
    import json
    (local / "open_sputter.json").write_text(json.dumps({k: {"key": k, "target": "CH1", "received_at": ""}}),
                                             encoding="utf-8")
    events = []
    r2 = _mk(cfg, w, clock)
    r2.set_event_sink(lambda kind, info: events.append((kind, info["key"])))
    r2.startup_recover()
    w.flush()
    assert json.loads((local / "open_sputter.json").read_text(encoding="utf-8")) == {}
    assert (local / "pending_sputter.csv").read_bytes().count(b"\r\n") == 1 and events == []
    r2.flush_once()
    nas = Path(cfg["HOST_LOG_NAS_DIR"]) / "Robot_20261030.csv"
    assert nas.read_bytes().count(k.encode()) == 1 and (local / "pending_sputter.csv").read_bytes() == b""
    r.close()
    r2.close()
    w.stop()


_ALD = r'''
import os, sys, time
nas, n, tag = sys.argv[1], int(sys.argv[2]), sys.argv[3]
lock = os.path.join(nas, "_lock", "Robot.lock")
target = os.path.join(nas, "Robot_20261030.csv")
HEADER = "대상,요청시각,시작시각,종료시각,소요(분),결과,사유,공정명,레시피,행개수,request_id,peer,로그파일,프로그램,기록키\r\n"
os.makedirs(os.path.dirname(lock), exist_ok=True)
for i in range(n):
    while True:
        try:
            fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, b"ald")
            break
        except FileExistsError:
            time.sleep(0.001)
    try:
        new = not os.path.exists(target)
        if not new:
            with open(target, "rb") as f:
                f.seek(-1, 2)
                last = f.read(1)
            if last != b"\n":
                with open(target, "ab") as f:
                    f.write(b"\r\n")
        with open(target, "a", encoding="utf-8-sig" if new else "utf-8", newline="") as f:
            if new:
                f.write(HEADER)
            f.write(f"ALD1,09:00:00,,,,성공,,공정 {i},r,1,A{i},ald,,ald,{tag}-{i}\r\n")
    finally:
        os.close(fd)
        os.unlink(lock)
    time.sleep(0.002)
'''


def test_robot_recover_only_keys_open_at_startup(tmp_path):
    """startup_recover 는 쓰기 스레드에서 늦게 돌 수 있다(부하·앞선 기록) — 그 사이 새로 받은 요청은 '중단(재시작)' 으로 닫지
    않는다(대상 = 부른 순간 open 에 있던 지난 실행 것). 쓰기 스레드를 막아 두고 확인."""
    import json
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG)
    clock = Clock(SC.T0)
    local = Path(cfg["HOST_LOG_LOCAL_DIR"])
    local.mkdir(parents=True, exist_ok=True)
    (local / "open_sputter.json").write_text(json.dumps({"old-1": {"key": "old-1", "target": "CH1", "request_id": "O",
                                                                    "received_at": "2026-10-30T09:00:00.000"}}),
                                             encoding="utf-8")
    gate = threading.Event()
    w = QueueWriter("t")
    w.put(gate.wait)                                         # 쓰기 스레드가 바쁨
    r = _mk(cfg, w, clock)
    r.startup_recover()
    k = r.request(target="CH1", request_id="NEW", peer="p", received_at=clock.now())   # 시작 직후 온 새 요청
    gate.set()
    assert w.flush(10)
    rows = (local / "pending_sputter.csv").read_text(encoding="utf-8-sig")
    assert "old-1" in rows and k not in rows                  # 지난 실행 것만 '중단(재시작)'
    assert set(json.loads((local / "open_sputter.json").read_text(encoding="utf-8"))) == {k}
    r.close()
    w.stop()


def test_robot_concurrent_with_ald_process(tmp_path):
    """다른 프로세스(ALD 흉내)와 같은 NAS 파일에 번갈아 쓴다 — 줄이 섞이거나 빠지거나 머리글이 두 번 생기지 않는다."""
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG)
    nas = Path(cfg["HOST_LOG_NAS_DIR"])
    nas.mkdir(parents=True)
    script = tmp_path / "ald_sim.py"
    script.write_text(_ALD, encoding="utf-8")
    w = QueueWriter("t")
    r = RobotCsv(cfg, writer=w, now=lambda: datetime(2026, 10, 30, 9, 15, 20), start_worker=False)
    keys = []
    for i in range(60):
        k = r.request(target="CH1", request_id=f"S{i}", peer="p",
                      received_at=datetime(2026, 10, 30, 9, 15, 20, i * 1000))
        r.finalize(k, "성공")
        keys.append(k)
    w.flush()
    proc = subprocess.Popen([sys.executable, "-I", str(script), str(nas), "200", "ald-key"])
    target = nas / "Robot_20261030.csv"
    try:
        deadline = time.monotonic() + 30
        while (not target.exists() or target.read_bytes().count(b"ald-key-") < 5) and time.monotonic() < deadline:
            time.sleep(0.005)                      # ALD 가 쓰기 시작한 뒤에 보낸다(번갈아 쓰기)
        while (Path(cfg["HOST_LOG_LOCAL_DIR"]) / "pending_sputter.csv").read_bytes() and time.monotonic() < deadline:
            r.flush_once()
        assert proc.wait(30) == 0
    finally:
        r.close()
        w.stop()
    data = (nas / "Robot_20261030.csv").read_bytes()
    assert data.startswith(b"\xef\xbb\xbf") and data.count(b"\xef\xbb\xbf") == 1
    rows = list(csv.reader(io.StringIO(data.decode("utf-8-sig"), newline="")))
    assert rows[0] == HEADER and all(len(x) == 15 for x in rows[1:]) and len(rows) == 261
    last = [x[-1] for x in rows[1:]]
    assert sorted(last) == sorted(keys + [f"ald-key-{i}" for i in range(200)])
    first_v2 = min(i for i, k in enumerate(last) if k.startswith("sputter-"))
    assert any(k.startswith("ald-key-") for k in last[first_v2:])     # 실제로 번갈아 씀
    assert data.endswith(b"\r\n") and b"\r\n\r\n" not in data
    assert not (nas / "_lock" / "Robot.lock").exists()


def test_robot_disabled_noop(tmp_path):
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG, {"HOST_LOG_ENABLED": False})
    w = QueueWriter("t")
    r = RobotCsv(cfg, writer=w, start_worker=False)
    assert r.request(target="CH1", request_id="x", peer="p") == "" and not r.finalize("k", "성공")
    r.startup_recover()
    r.close()
    w.stop()
    assert not Path(cfg["HOST_LOG_LOCAL_DIR"]).exists()
    assert csv_line(["a,b", 'c"d']) == '"a,b","c""d"\r\n'

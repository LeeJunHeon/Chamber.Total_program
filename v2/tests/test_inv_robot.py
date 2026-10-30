"""전수 목록 로봇 영역(inv_robot.py) 중 다른 시험이 덮지 않던 항목 — 실제 실행 시험.

- 로봇 서버(v2/robot_server): 명령 CSV 는 응답을 보낸 뒤 기록(ROB-020)·10열 머리글·우선 경로 10 s 재시도(ROB-025·026)·
  '__LOGGING_EXCEPTION__' 행(ROB-028)·'<<JSON_DUMP_ERROR ..>>' 칸(GAPC-001)·CSV 실패 무시(ROB-030)·바인드 기본값과
  기동 로그(ROB-024·109)·종료 때 열린 연결도 닫고 빨리 끝남(ROB-110).
- Robot CSV(v2/ch12/outputs/robot_csv.py): 보내기 스레드 재시도·close 2 s(ROB-129·137), 메모리 큐 10000줄(ROB-131),
  started/finished 이벤트 1회(ROB-136), 거절 뒤 늦은 기록 없음(ROB-146).
- 조립(v2/ch12/assembly.py EngineAdapter): 시작 때 startup_recover(ROB-128), 레시피 파일 30 s(E227 — ROB-064),
  챔버 런타임의 Robot CSV 단계(소유·update·mark_started·둘째 행 로그파일·확정 — ROB-140~144), 시작 거절 문구(ROB-063).
- 플라즈마 클리닝 호스트 런(ROB-145).
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import socket
import struct
import threading
import time
from datetime import datetime
from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.robot_server import app as RA
from v2.robot_server import protocol
from v2.robot_server.log import CSV_HEADER, CommandCsv, ServerLog
from v2.robot_server.server import RobotServer, _safe_json

HDR = "!BBHIQ"


# ───────────────────────── 로봇 서버 ─────────────────────────
class Fwd:
    """가짜 Forwarder — 정해 둔 응답(body)을 돌려준다(지연 가능)."""

    def __init__(self, body=None, delay_s: float = 0.0) -> None:
        self.body = body if body is not None else {"result": "success", "message": "OK"}
        self.delay_s = delay_s
        self.calls = []

    async def forward(self, cmd, data, raw, peer, received_at):
        self.calls.append((cmd, peer, received_at))
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        return f"{cmd}_RESULT", self.body, []

    async def send_event(self, ev):
        return False


async def _read_frame(r, timeout=10.0):
    h = await asyncio.wait_for(r.readexactly(16), timeout)
    _v, _f, _cl, n, _ts = struct.unpack(HDR, h)
    return json.loads((await asyncio.wait_for(r.readexactly(n), timeout)).decode("utf-8"))


def _rows(root: Path) -> list:
    out = []
    for p in sorted(root.rglob("remote_cmd_*.csv")):
        with open(p, encoding="utf-8-sig", newline="") as f:
            out += list(csv.DictReader(f))
    return out


def test_command_csv_written_after_reply_duration_ms(tmp_path):
    """ROB-020: 응답 write/drain 뒤에 CSV — CSV 쓰기가 늦어도 응답은 기다리지 않는다. duration_ms = dispatch 소요(int ms)."""
    lines = []
    cc = CommandCsv(tmp_path / "root", tmp_path / "fb")
    orig = cc.append
    order = []

    async def slow_append(row):
        order.append(("csv_start", time.monotonic()))
        await asyncio.sleep(1.0)
        await orig(row)
    cc.append = slow_append
    srv = RobotServer("127.0.0.1", 0, Fwd(delay_s=0.25), lambda t, x: lines.append((t, x)), csv=cc)

    async def run():
        await srv.start()
        try:
            r, w = await asyncio.open_connection("127.0.0.1", srv.bound_port)
            t0 = time.monotonic()
            w.write(protocol.pack_request("CH1_GATE_OPEN", {}, "a1"))
            await w.drain()
            got = await _read_frame(r)
            t_reply = time.monotonic() - t0
            await asyncio.sleep(1.3)
            w.close()
            return got, t_reply
        finally:
            await srv.aclose()
    got, t_reply = asyncio.run(run())
    assert got["command"] == "CH1_GATE_OPEN_RESULT" and 0.2 <= t_reply < 0.9
    rows = _rows(tmp_path / "root")
    assert len(rows) == 1 and 240 <= int(rows[0]["duration_ms"]) < 900
    res = [x for t, x in lines if t == "PLC_HOST" and x.startswith("[RES]")]
    assert res and order                                    # [RES] 로그는 CSV 뒤(v1 순서)


def test_command_csv_header_and_primary_retry(tmp_path):
    """ROB-025·026: 10열 머리글(utf-8-sig) · 쓰기 실패면 폴백으로 1회 전환 + '__NAS_MISSING__' · 10 s 뒤 우선 경로 재시도."""
    blocker = tmp_path / "root"
    blocker.write_text("x")
    cc = CommandCsv(blocker, tmp_path / "fb", now=lambda: datetime(2026, 10, 30, 1, 2, 3))

    async def run():
        await cc.append({"server_time": "t", "req_command": "A"})
        blocker.unlink()
        blocker.mkdir()
        await cc.append({"server_time": "t", "req_command": "B"})      # 10 s 안 — 아직 폴백
        cc._writer._last_try -= 11.0                                    # 10 s 지남
        await cc.append({"server_time": "t", "req_command": "C"})
        await cc.aclose()
    asyncio.run(run())
    fb = (tmp_path / "fb" / "remote_cmd_20261030.csv").read_bytes()
    pri = (blocker / "CH1&2_Server" / "remote_cmd_20261030.csv").read_bytes()
    want_head = ("\ufeff" + ",".join(CSV_HEADER) + "\r\n").encode("utf-8")
    assert CSV_HEADER == ["server_time", "peer", "request_id", "req_command", "req_data_json", "res_command",
                          "res_result", "res_message", "res_data_json", "duration_ms"]
    assert fb.startswith(want_head) and pri.startswith(want_head)
    assert [r["req_command"] for r in csv.DictReader(io.StringIO(fb.decode("utf-8-sig")))] == ["A", "__NAS_MISSING__", "B"]
    assert [r["req_command"] for r in csv.DictReader(io.StringIO(pri.decode("utf-8-sig")))] == ["C"]


class _BadStr:
    def __str__(self):
        raise ValueError("str 불가")

    def __repr__(self):
        return "<BadStr>"


def test_logging_exception_row_and_json_dump_error_cell(tmp_path):
    """ROB-028: 행 만들기 실패 → '__LOGGING_EXCEPTION__' 행. GAPC-001: json.dumps 실패 칸은 '<<JSON_DUMP_ERROR ..>> repr 300자'."""
    circ: dict = {"a": 1}
    circ["self"] = circ
    assert _safe_json(circ).startswith("<<JSON_DUMP_ERROR ValueError('Circular reference detected')>> {'a': 1")
    assert len(_safe_json({"x": [circ] * 100})) < 400

    async def run(body):
        cc = CommandCsv(tmp_path / body[0], tmp_path / "fb")
        srv = RobotServer("127.0.0.1", 0, Fwd(body=body[1]), lambda t, x: None, csv=cc)
        await srv.start()
        try:
            r, w = await asyncio.open_connection("127.0.0.1", srv.bound_port)
            w.write(protocol.pack_request("CH1_GATE_OPEN", {}, "q"))
            await w.drain()
            got = await _read_frame(r)
            w.close()
            return got
        finally:
            await srv.aclose()
    got = asyncio.run(run(("r1", {"result": "success", "message": _BadStr()})))
    assert got["data"]["error_code"] == "E107"                      # 응답 직렬화도 실패 → E107
    (row,) = _rows(tmp_path / "r1")
    assert row["res_command"] == "__LOGGING_EXCEPTION__" and row["res_result"] == "fail"
    assert row["res_message"].startswith("Failed to build/append csv row: ValueError('str 불가')")
    assert json.loads(row["res_data_json"]) == {"res_cmd": "CH1_GATE_OPEN_RESULT",
                                                "res_data_repr": "{'result': 'success', 'message': <BadStr>}"}
    c2: dict = {"result": "success", "message": "OK"}
    c2["loop"] = c2
    got = asyncio.run(run(("r2", c2)))
    (row,) = _rows(tmp_path / "r2")
    assert row["res_command"] == "CH1_GATE_OPEN_RESULT" and row["res_data_json"].startswith("<<JSON_DUMP_ERROR ")
    assert got["data"]["error_code"] == "E107"


def test_command_csv_failure_never_affects_reply(tmp_path):
    """ROB-030: 명령 CSV 쓰기가 예외여도 응답·연결에 영향 없음."""
    class Broken:
        async def append(self, row):
            raise OSError("디스크 없음")

        async def aclose(self):
            raise OSError("x")
    srv = RobotServer("127.0.0.1", 0, Fwd(), lambda t, x: None, csv=Broken())

    async def run():
        await srv.start()
        try:
            r, w = await asyncio.open_connection("127.0.0.1", srv.bound_port)
            out = []
            for pkt in (protocol.pack_request("GET_SPUTTER_STATUS", {}, "1"), protocol.pack_request("", {}, "2"),
                        b"\x01\x00\x00\x00\x00\x00\x00\x02" + b"\x00" * 8 + b"{x", protocol.pack_request("4PIN_UP", {}, "3")):
                w.write(pkt)
                await w.drain()
                out.append(await _read_frame(r))
            w.close()
            return out
        finally:
            await srv.aclose()
    out = asyncio.run(run())
    assert [o["command"] for o in out] == ["GET_SPUTTER_STATUS_RESULT", "UNKNOWN_RESULT", "PARSE_ERROR_RESULT",
                                           "4PIN_UP_RESULT"]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_bind_defaults_and_boot_logs(tmp_path):
    """ROB-024: 0.0.0.0:50070 기본. ROB-109: 기동 성공 'Host started on {HOST}:{PORT}', 바인드 실패 'Host start failed: ..'(종료 코드 1)."""
    from v2.ch12.params.robot import robot_cfg
    c = robot_cfg({})
    assert (c["HOST_SERVER_HOST"], c["HOST_SERVER_PORT"]) == ("0.0.0.0", 50070)
    srv, slog = RA.build({"LOG_ROOT_DIR": str(tmp_path / "a"), "ROBOT_CACHE_PATH": ""})
    assert (srv.host, srv.port) == ("0.0.0.0", 50070)
    slog.close()
    port = _free_port()
    cfg = {"HOST_SERVER_HOST": "127.0.0.1", "HOST_SERVER_PORT": port, "LOG_ROOT_DIR": str(tmp_path / "logs"),
           "ROBOT_CACHE_PATH": "", "ROBOT_LOG_ECHO": False, "LOCAL_FALLBACK_SERVER_DIR": str(tmp_path / "fb")}

    async def ok_then_cancel():
        t = asyncio.ensure_future(RA._run(cfg, False))
        for _ in range(200):
            await asyncio.sleep(0.01)
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                pass
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
    asyncio.run(ok_then_cancel())
    with socket.socket() as hold:
        hold.bind(("127.0.0.1", port))
        hold.listen(1)
        assert asyncio.run(RA._run(cfg, False)) == 1
    text = (tmp_path / "logs" / "CH1&2_Server" / f"server_page_{datetime.now():%Y%m%d}.log").read_text(encoding="utf-8")
    assert f"[NET] Host started on ('127.0.0.1', {port})" in text and f"[NET] Host started on 127.0.0.1:{port}" in text
    assert "[NET] Host closed" in text
    assert "[NET] Host start failed: OSError(" in text


def test_shutdown_closes_open_connections_quickly(tmp_path):
    """ROB-110: 종료 때 열린 로봇 연결이 있어도 닫고 끝남(v1 은 3 s 까지만 기다림 — v2 aclose 는 연결을 먼저 닫는다)."""
    lines = []
    srv = RobotServer("127.0.0.1", 0, Fwd(), lambda t, x: lines.append((t, x)))

    async def run():
        await srv.start()
        r, w = await asyncio.open_connection("127.0.0.1", srv.bound_port)
        w.write(protocol.pack_request("GET_SPUTTER_STATUS", {}, "1"))
        await w.drain()
        await _read_frame(r)
        t0 = time.monotonic()
        await srv.aclose()
        dt = time.monotonic() - t0
        w.close()
        return dt
    dt = asyncio.run(run())
    assert dt < 3.0 and ("NET", "Host closed") in lines


# ───────────────────────── Robot CSV ─────────────────────────
def _robot_cfg(tmp_path, **kw):
    c = {"HOST_LOG_ENABLED": True, "HOST_LOG_NAS_DIR": str(tmp_path / "nas" / "Robot"),
         "HOST_LOG_LOCAL_DIR": str(tmp_path / "local" / "Robot"), "HOST_LOG_RETRY_S": 30, "HOST_LOG_LOCK_STALE_S": 120}
    c.update(kw)
    return c


class _SysLog:
    def __init__(self):
        self.lines = []

    def append(self, src, msg):
        self.lines.append((src, msg))


def test_robot_csv_worker_thread_retry_and_close(tmp_path):
    """ROB-129: 보내기 스레드 HostProcessLogWorker — 막힌 NAS 는 HOST_LOG_RETRY_S(최소 1 s)마다 다시. ROB-137: close 최대 2 s."""
    from v2.ch12.outputs.robot_csv import RobotCsv
    (tmp_path / "nas").write_text("막힘")
    cfg = _robot_cfg(tmp_path, HOST_LOG_RETRY_S=0.2)
    r = RobotCsv(cfg, now=lambda: datetime(2026, 10, 30, 9, 0, 0))
    assert r._retry_s() == 1.0
    k = r.request(target="CH1", request_id="w1", peer="p", received_at=datetime(2026, 10, 30, 9, 0, 0))
    assert r.finalize(k, "성공")
    r.flush()
    deadline = time.monotonic() + 5
    while (r._worker is None or not r._worker.is_alive()) and time.monotonic() < deadline:
        time.sleep(0.02)
    assert r._worker.name == "HostProcessLogWorker" and r._worker.daemon
    time.sleep(0.3)
    assert (tmp_path / "local" / "Robot" / "pending_sputter.csv").read_bytes().count(b"\r\n") == 1
    (tmp_path / "nas").unlink()                 # NAS 풀림 → 다음 재시도(1 s) 안에 전송
    nas = tmp_path / "nas" / "Robot" / "Robot_20261030.csv"
    deadline = time.monotonic() + 4
    while not nas.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert nas.exists() and k.encode() in nas.read_bytes()
    t0 = time.monotonic()
    r.close()
    assert time.monotonic() - t0 < 2.5 and not r._worker.is_alive()


def test_robot_csv_mem_queue_cap_warns_once(tmp_path):
    """ROB-131: pending 쓰기 실패 줄은 메모리 큐(상한 10000 — 넘으면 가장 오래된 것부터 버림), 경고는 SYSTEM 로그 HOSTLOG 1회."""
    from v2.ch12.outputs.robot_csv import MEM_QUEUE_MAX, RobotCsv
    sl = _SysLog()
    r = RobotCsv(_robot_cfg(tmp_path), system_log=sl, start_worker=False)
    r._mem_queue.extend(f"line{i}\r\n" for i in range(MEM_QUEUE_MAX + 3))
    r._trim_mem_queue()
    r._mem_queue.extend(["x\r\n", "y\r\n"])
    r._trim_mem_queue()
    assert MEM_QUEUE_MAX == 10000 and len(r._mem_queue) == 10000 and r._mem_queue[0] == "line5\r\n"
    assert r._mem_queue_dropped == 5
    warns = [m for s, m in sl.lines if s == "HOSTLOG" and "메모리 큐 상한(10000줄) 초과" in m]
    assert len(warns) == 1
    # 실제 경로: 로컬 폴더가 막히면 finalize 줄이 메모리 큐로, open 은 유지
    (tmp_path / "blk").write_text("x")
    r2 = RobotCsv(_robot_cfg(tmp_path, HOST_LOG_LOCAL_DIR=str(tmp_path / "blk" / "Robot")), system_log=sl,
                  start_worker=False)
    k = r2.request(target="CH2", request_id="m", peer="p")
    r2.finalize(k, "실패", "x")
    r2.flush()
    assert len(r2._mem_queue) == 1 and k in r2._open
    r.close()
    r2.close()


def test_robot_csv_event_sink_started_finished_once(tmp_path):
    """ROB-136: 'started'(mark_started 첫 회)·'finished'(finalize·reject, 기록키당 1회) → sink. 카드는 포털(client_process)."""
    from v2.ch12.outputs.robot_csv import RobotCsv
    from v2.ch12.robot_api import client_process_to_emit
    ev, emitted = [], []
    r = RobotCsv(_robot_cfg(tmp_path), start_worker=False)
    r.set_event_sink(lambda kind, info: ev.append((kind, info["key"], info.get("result"))))
    k = r.request(target="CH1", request_id="e1", peer="p", received_at=datetime(2026, 10, 30, 9, 0, 0))
    r.mark_started(k, datetime(2026, 10, 30, 9, 0, 5), "a.txt")
    r.mark_started(k, datetime(2026, 10, 30, 9, 0, 9), "b.txt")
    r.finalize(k, "성공", "", datetime(2026, 10, 30, 9, 10, 0))
    r.finalize(k, "성공", "", datetime(2026, 10, 30, 9, 10, 0))
    k2 = r.request(target="CH2", request_id="e2", peer="p")
    r.reject(k2, "E205 x")
    r.flush()
    assert ev == [("started", k, None), ("finished", k, "성공"), ("finished", k2, "거절")]
    sink = client_process_to_emit(lambda *a, **kw: emitted.append((a, kw)))
    sink("finished", {"key": k, "result": "성공", "target": "CH1"})
    assert emitted and "client_process" in json.dumps(emitted, ensure_ascii=False, default=str)
    r.close()


def test_robot_csv_late_writes_after_reject_are_ignored(tmp_path):
    """ROB-146(D127): 거절로 확정된 키에 뒤늦게 update·mark_started·finalize 가 와도 아무것도 쓰지 않는다."""
    from v2.ch12.outputs.robot_csv import RobotCsv
    r = RobotCsv(_robot_cfg(tmp_path), start_worker=False)
    k = r.request(target="PC(CH1)", request_id="t", peer="p", received_at=datetime(2026, 10, 30, 9, 0, 0))
    assert r.reject(k, "E700 공정 시작 대기 시간 초과")
    r.mark_owned(k)
    r.update(k, recipe_name="pc.csv", row_count=1)
    r.mark_started(k, datetime(2026, 10, 30, 9, 0, 30), None)
    assert r.finalize(k, "성공", "", datetime(2026, 10, 30, 9, 5, 0)) is False
    r.flush()
    rows = list(csv.reader(io.StringIO((tmp_path / "local" / "Robot" / "Robot_20261030_sputter.csv")
                                       .read_text(encoding="utf-8-sig"))))
    assert len(rows) == 2 and rows[1][5] == "거절" and rows[1][2] == ""
    r.close()


# ───────────────────────── 조립(EngineAdapter) — 챔버 런타임 ─────────────────────────
@pytest.fixture
def full(tmp_path):
    from v2.ch12.sim.rig import Ch12FullRig
    c = VirtualClock(start_wall=1_793_318_400.0)
    c.register()
    holder = {}

    def make(pre=None):
        if pre is not None:
            pre(tmp_path / "rig")
        rig = Ch12FullRig(c, tmp_path / "rig", cfg={"CAMERA_MIN_FREE_GB": 0.0}, robot_api=False, schedules=False)
        holder["rig"] = rig
        assert rig.start()
        return rig
    yield c, make
    if "rig" in holder:
        holder["rig"].stop()
    c.close()


def _mine_rows(rig) -> list:
    rig.asm.robot_csv.flush()
    d = Path(rig.cfg["HOST_LOG_LOCAL_DIR"])
    out = []
    for p in sorted(d.glob("Robot_*_sputter.csv")):
        out += list(csv.reader(io.StringIO(p.read_text(encoding="utf-8-sig"))))[1:]
    return out


def _until(rig, pred, limit=400):
    n = 0
    while not pred() and n < limit:
        rig.loop.run(1)
        n += 1
    assert pred()


def test_assembly_startup_recover_at_build(full):
    """ROB-128: 조립 때 Robot CSV startup_recover — 남은 open 은 '중단(재시작)'(시작 전/공정 중) 줄로 pending·사본에."""
    c, make = full

    def pre(out: Path):
        d = out / "robot"
        d.mkdir(parents=True)
        it = {"key": "sputter-20261030090000000-X", "target": "CH1", "request_id": "X", "peer": "p",
              "received_at": "2026-10-30T09:00:00.000", "started_at": "2026-10-30T09:00:10.000", "recipe_name": "a.csv",
              "row_count": 1, "process_names": "A", "log_files": ["CH1_A.txt"]}
        (d / "open_sputter.json").write_text(json.dumps({it["key"]: it}), encoding="utf-8")
    rig = make(pre)
    rows = _mine_rows(rig)
    assert [r[5] for r in rows] == ["중단(재시작)"] and rows[0][6] == "프로그램 재시작 (공정 중)"
    assert rows[0][3] == "" and rows[0][4] == "" and rows[0][-1] == "sputter-20261030090000000-X"
    assert json.loads((Path(rig.cfg["HOST_LOG_LOCAL_DIR"]) / "open_sputter.json").read_text(encoding="utf-8")) == {}


def _recipe(tmp: Path, name: str, rows: list) -> str:
    p = tmp / name
    p.write_text("#,Process_name,time\n" + "".join(f"test,{n},{t}\n" for n, t in rows), encoding="utf-8")
    return str(p)


def test_chamber_host_run_robot_csv_steps(full, tmp_path):
    """ROB-140~144: 수락 때 mark_owned, 검증 통과 뒤 update(레시피·행 수·공정명 ' / '), 첫 행 mark_started(로그 파일),
    둘째 행부터 로그 파일만 update, 큐 끝 '성공' 1줄. 직전 런이 남아 있으면 먼저 '미확인' 으로 닫는다."""
    c, make = full
    rig = make()
    a = rig.asm
    hl = a.robot_csv
    got = []
    rec = _recipe(tmp_path, "두 행.csv", [("R1", "3s"), ("R2", "3s")])
    k0 = hl.request(target="CH1", request_id="old", peer="p", received_at=datetime(2026, 10, 30, 9, 0, 0))
    hl.mark_owned(k0)
    a.adapter.host_runs[1] = {"key": k0, "started": True, "last": None, "log_files": []}
    k = hl.request(target="CH1", request_id="r1", peer="('10.0.0.5', 5000)", received_at=datetime(2026, 10, 30, 9, 1, 0))
    a.adapter.start_sputter(1, rec, {"key": k, "request_id": "r1", "peer": "p"}, lambda ok, code, d: got.append((ok, code, d)))
    _until(rig, lambda: bool(got))
    assert got[0][0] is True and hl.is_owned(k)
    e = a.proc["ch1"]
    _until(rig, lambda: not e.busy and (k in hl._finalizing or k not in hl._open))   # 마감 요청까지(쓰기는 아래 flush)
    assert hl.flush(30.0) and k not in hl._open
    rows = _mine_rows(rig)
    by_key = {r[-1]: r for r in rows}
    assert by_key[k0][5] == "미확인" and by_key[k0][6] == "다음 요청 수락 전 종료 미확인"
    r = by_key[k]
    assert r[0] == "CH1" and r[5] == "성공" and r[6] == "" and r[7] == "R1 / R2" and r[8] == "두 행.csv" and r[9] == "2"
    files = r[12].split(" / ")
    assert len(files) == 2 and all(f.startswith("CH1_") and f.endswith(".txt") for f in files) and files[0] != files[1]
    assert r[2] and r[3] and r[4] != ""


def test_chamber_host_run_stop_and_reject_texts(full, tmp_path):
    """ROB-143·144: 사용자 STOP → 'STOP'(사유 없으면 '사용자 STOP'). ROB-063: 시작 거절 문구 — 'runner busy: ..'(적재 거부),
    시작 가드 'cooldown Ns remaining'(→ E700), 같은 CH PC 실행 중 'CHn Plasma Cleaning 실행 중'(→ E511)."""
    from v2.ch12.recipe_check import host_error_code
    c, make = full
    rig = make()
    a = rig.asm
    hl = a.robot_csv
    e = a.proc["ch1"]
    got = []
    rec = _recipe(tmp_path, "긴 행.csv", [("L1", "60s")])
    k = hl.request(target="CH1", request_id="s1", peer="p")
    a.adapter.start_sputter(1, rec, {"key": k}, lambda ok, code, d: got.append((ok, code, d)))
    _until(rig, lambda: e.state_name == "RUNNING")
    with pytest.raises(RuntimeError, match="^runner busy: "):
        a.adapter.start_sputter(1, rec, {"key": "x"}, lambda *x: None)
    e.stop(source="web")
    # 마감 줄은 기록 쓰기 스레드(실제 시간 — 다른 v1 기록과 같은 큐)가 open 에서 뺀다: tick 수로 기다리지 않고, 마감이 요청된
    # 것(finalize — 메모리)까지 tick 으로, 그 뒤 쓰기 큐를 비우고(flush) 확인
    _until(rig, lambda: not e.busy and (k in hl._finalizing or k not in hl._open))
    assert hl.flush(30.0) and k not in hl._open
    r = {x[-1]: x for x in _mine_rows(rig)}[k]
    assert r[5] == "STOP" and r[6]
    k2 = hl.request(target="CH1", request_id="s2", peer="p")
    got.clear()
    a.adapter.start_sputter(1, rec, {"key": k2}, lambda ok, code, d: got.append((ok, code, d)))
    assert got and got[0][0] is False and got[0][2].startswith("cooldown ") and got[0][2].endswith("s remaining")
    assert host_error_code(got[0][2]) == "E700" and not hl.is_owned(k2)
    rig.loop.run(61)
    a.run_state.set_running("pc", True, 1)
    got.clear()
    a.adapter.start_sputter(1, rec, {"key": k2}, lambda ok, code, d: got.append((ok, code, d)))
    a.run_state.set_running("pc", False, 1)
    assert got == [(False, None, "CH1 Plasma Cleaning 실행 중")] and host_error_code(got[0][2]) == "E511"


def test_recipe_load_timeout_e227():
    """ROB-064: 레시피 파일 읽기 30 s 넘으면 'CSV 로드 30초 timeout (NAS 응답 지연): ..' E227."""
    from v2.ch12 import recipe as RC
    from v2.ch12.assembly import EngineAdapter
    from v2.ch12.errors import CodedRuntimeError
    c = VirtualClock()
    c.register()

    class Sys:
        clock = c

    class Asm:
        system = Sys()
    orig = RC.load_recipe_file
    RC.load_recipe_file = lambda path, origin="host": c.sleep_until(c.monotonic() + 40.0)
    try:
        with pytest.raises(CodedRuntimeError) as ei:
            EngineAdapter(Asm())._load_file(r"\\NAS\느린.csv")
    finally:
        RC.load_recipe_file = orig
        c.close()
    assert ei.value.code == "E227" and str(ei.value) == r"CSV 로드 30초 timeout (NAS 응답 지연): \\NAS\느린.csv"


# ───────────────────────── 플라즈마 클리닝 호스트 런 ─────────────────────────
def test_pc_host_run_robot_csv(tmp_path):
    """ROB-145: PC 프리플라이트 통과 뒤 mark_owned → update(레시피 basename·데이터 행 수·첫 행 Process_name) →
    mark_started(로그 파일 빈칸 — PC-098) → 끝에 '성공'."""
    from v2.ch12 import plasma_cleaning as PCM
    from v2.ch12.outputs.robot_csv import RobotCsv
    from v2.ch12.sim.rig import Ch12Rig
    c = VirtualClock(start_wall=1_793_318_400.0)
    c.register()
    r = Ch12Rig(c, tmp_path / "rig", cfg={"CAMERA_MIN_FREE_GB": 0.0})
    hl = RobotCsv(r.cfg, start_worker=False)
    eng = PCM.attach(r, host_log=hl)
    try:
        assert r.start()
        f = tmp_path / "pc 레시피.csv"
        f.write_text("use_ch,process_name,base_pressure,gas_flow,working_pressure,rf_power,process_time\r\n"
                     ",PC-1,5.00E-06,10,2,100,0.1\r\n,,,,,,\r\n,PC-2,,,,,\r\n", encoding="utf-8-sig")
        k = hl.request(target="PC(CH1)", request_id="pc1", peer="p")
        box = eng.host_start(str(f), origin_meta={"key": k})
        n = 0
        while not (box.done and eng.last_final is not None and not eng.running) and n < 900:
            r.loop.run(1)
            n += 1
        assert box.value.ok and eng.last_final["ok"]
        r.loop.run(1)
        hl.flush()
        rows = list(csv.reader(io.StringIO(next((Path(r.cfg["HOST_LOG_LOCAL_DIR"])).glob("Robot_*_sputter.csv"))
                                           .read_text(encoding="utf-8-sig"))))
        (row,) = [x for x in rows[1:] if x[-1] == k]
        assert row[5] == "성공" and row[7] == "PC-1" and row[8] == "pc 레시피.csv" and row[9] == "2" and row[12] == ""
        assert row[2] and row[3]
    finally:
        r.stop()
        hl.close()
        c.close()

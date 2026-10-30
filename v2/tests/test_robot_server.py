"""로봇 서버(v2/robot_server) 시험 — v1 host/server.py 와 바이트·로그·명령 CSV 가 같은지, 전달·캐시·꺼짐 처리.

골든 v2/tests/golden/robot_server_frames.json 은 v2/tools/v1golden_robot_server.py 가 v1 HostServer 를 실제로 띄워 만든 것
(핸들러만 가짜). 여기서는 v1 을 import 하지 않는다. v2 서버 앞에는 가짜 내부 API(fake_api)를 두고, 골든의 api 항목
(v1 dispatch 가 만든 응답 명령·data·그 사이 로그 줄)을 그대로 돌려주게 한다 — 18개 명령의 응답 내용(명령 × 상태)은
제어 프로그램 쪽 단계에서 비교한다.

접속 peer(임의 포트)와 응답 헤더 timestamp 만 사례마다 다르므로: 시각은 v1·v2 모두 고정(FIXED_TS), peer 는 골든의 peer
문자열을 이번 접속의 peer 문자열로 바꾸고 바뀐 바디 길이로 헤더 body_len 을 다시 계산해 비교한다.
"""
from __future__ import annotations

import asyncio
import csv
import json
import os
import re
import signal
import socket
import struct
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pytest

from v2.robot_server import protocol
from v2.robot_server.cache import ReplyCache, as_busy
from v2.robot_server.fake_api import FakeApi
from v2.robot_server.forward import COMMANDS, Forwarder, parse_log_entries, reply_timeout_s
from v2.robot_server.log import CommandCsv, ServerLog, format_line
from v2.robot_server.server import RobotServer

GOLDEN = json.loads((Path(__file__).parent / "golden" / "robot_server_frames.json").read_text(encoding="utf-8"))
FIXED_TS = GOLDEN["meta"]["fixed_ts"]
CASES = {c["name"]: c for c in GOLDEN["cases"]}
HDR = "!BBHIQ"
ROOT = Path(__file__).resolve().parents[2]


# ───────────────────────── 도우미 ─────────────────────────

def frames(rx: bytes) -> list:
    out, i = [], 0
    while len(rx) - i >= 16:
        h = struct.unpack(HDR, rx[i:i + 16])
        if len(rx) - i - 16 < h[3]:
            break
        out.append((h, rx[i + 16:i + 16 + h[3]]))
        i += 16 + h[3]
    return out


def sub_peer_frames(rx: bytes, p1: str, p2: str) -> bytes:
    """골든 바이트의 peer 문자열을 바꾸고 body_len 을 다시 계산."""
    out = b""
    fr = frames(rx)
    assert sum(16 + len(b) for _, b in fr) == len(rx)
    for (v, f, cl, _bl, ts), body in fr:
        nb = body.replace(p1.encode("utf-8"), p2.encode("utf-8"))
        out += struct.pack(HDR, v, f, cl, len(nb), ts) + nb
    return out


def sub(obj, p1: str, p2: str):
    if isinstance(obj, str):
        return obj.replace(p1, p2)
    if isinstance(obj, list):
        return [sub(x, p1, p2) for x in obj]
    if isinstance(obj, dict):
        return {k: sub(v, p1, p2) for k, v in obj.items()}
    return obj


async def read_frames(reader, expect: int, wait_eof: bool) -> tuple:
    """골든 도구와 같은 읽기(기대 응답 수만큼, 뒤에 EOF 를 잠깐 확인)."""
    buf, closed = b"", False
    while len(frames(buf)) < expect:
        try:
            ch = await asyncio.wait_for(reader.read(65536), timeout=5.0)
        except asyncio.TimeoutError:
            break
        if not ch:
            closed = True
            break
        buf += ch
    if not closed:
        try:
            ch = await asyncio.wait_for(reader.read(65536), timeout=3.0 if wait_eof else 0.2)
            if ch:
                buf += ch
            else:
                closed = True
        except asyncio.TimeoutError:
            pass
        except ConnectionError:
            closed = True
    return buf, closed


class Rig:
    """v2 서버 + 가짜 내부 API + 로그 수집."""

    def __init__(self, tmp: Path, respond=None, cfg: dict | None = None, api: FakeApi | None = None,
                 cache_path: Path | None = None) -> None:
        self.tmp = tmp
        self.api = api if api is not None else FakeApi(respond).start()
        self.own_api = api is None
        self.lines: list = []
        c = {"ROBOT_API_PORT": self.api.port, "ROBOT_API_CONNECT_TIMEOUT_S": 1.0}
        c.update(cfg or {})
        self.log = ServerLog(tmp / "logroot", tmp / "fallback", on_line=lambda t, x, line: self.lines.append([t, x]))
        self.csv = CommandCsv(tmp / "logroot", tmp / "fallback")
        self.cache = ReplyCache(cache_path)
        self.srv = RobotServer("127.0.0.1", 0, Forwarder(c), self.log, csv=self.csv, cache=self.cache,
                               now=lambda: FIXED_TS)

    async def __aenter__(self) -> "Rig":
        await self.srv.start()
        self.lines.clear()
        return self

    async def __aexit__(self, *a) -> None:
        await self.srv.aclose()
        self.log.close()
        if self.own_api:
            self.api.stop()

    async def connect(self):
        r, w = await asyncio.open_connection("127.0.0.1", self.srv.bound_port)
        return r, w, str(w.get_extra_info("sockname")[:2])

    async def wait_closed_line(self, peer: str, start: int = 0) -> None:
        for _ in range(300):
            if ["NET", f"Client closed: {peer}"] in self.lines[start:]:
                return
            await asyncio.sleep(0.01)
        raise AssertionError(f"'Client closed: {peer}' 로그 없음")

    async def ask(self, r, w, packet: bytes, n: int = 1) -> list:
        w.write(packet)
        await w.drain()
        buf = b""
        while len(frames(buf)) < n:
            ch = await asyncio.wait_for(r.read(65536), timeout=10.0)
            assert ch, "서버가 연결을 끊음"
            buf += ch
        return [(h, json.loads(b.decode("utf-8"))) for h, b in frames(buf)]


def csv_rows(tmp: Path) -> list:
    rows = []
    for p in sorted((tmp / "logroot").rglob("remote_cmd_*.csv")):
        with open(p, encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                rows.append(row)
    return rows


# ───────────────────────── v1 골든 바이트 동일 ─────────────────────────

@pytest.mark.parametrize("name", list(CASES))
def test_v1_golden_case(name, tmp_path):
    case = CASES[name]
    p1 = case["peer"]
    queue = list(case["api"])
    seen = []
    peer_box = {}

    def respond(req):
        e = queue.pop(0)
        p2 = peer_box["peer"]
        seen.append(req)
        assert req["cmd"] == e["cmd"]
        assert req["data"] == e["data"]
        assert req["peer"] == "127.0.0.1:" + p2.rsplit(", ", 1)[1].rstrip(")")
        assert re.fullmatch(r"[0-9a-f]{32}", req["id"])
        datetime.fromisoformat(req["received_at"])
        assert isinstance(req["raw"], dict) and req["raw"].get("command") is not None
        return {"reply_cmd": e["reply_cmd"], "body": sub(e["body"], p1, p2),
                "log": [f"[{t}] {sub(x, p1, p2)}" for t, x in e["log"]]}

    async def run():
        async with Rig(tmp_path, respond) as rig:
            r, w, p2 = await rig.connect()
            peer_box["peer"] = p2
            for s in case["sends"]:
                w.write(bytes.fromhex(s["hex"]))
                await w.drain()
                if s["delay_s"]:
                    await asyncio.sleep(s["delay_s"])
            if case["half_close"]:
                w.write_eof()
            rx, closed = await read_frames(r, case["expect"], wait_eof=case["half_close"] or case["expect"] == 0)
            w.close()
            await rig.wait_closed_line(p2)
            await asyncio.sleep(0.05)
            return rx, closed, p2, list(rig.lines)

    rx, closed, p2, lines = asyncio.run(run())
    want = sub_peer_frames(bytes.fromhex(case["rx_hex"]), p1, p2)
    assert rx == want, f"{name}: 받은 바이트가 v1 과 다름\nv2={rx[:400]!r}\nv1={want[:400]!r}"
    assert closed == case["server_closed"]
    assert lines == sub(case["log"], p1, p2)
    assert not queue, "가짜 API 응답이 남음(전달 안 된 명령)"
    assert len(seen) == len(case["api"])
    got_rows = [{k: v for k, v in row.items() if k not in ("server_time", "duration_ms")} for row in csv_rows(tmp_path)]
    assert got_rows == sub(case["csv"], p1, p2)


def test_golden_covers_all_18_commands():
    names = [e["cmd"] for e in CASES["all18"]["api"]]
    assert names == list(COMMANDS) and len(names) == 18
    rx_cmds = [json.loads(b)["command"] for _, b in frames(bytes.fromhex(CASES["all18"]["rx_hex"]))]
    assert rx_cmds == [f"{c}_RESULT" for c in COMMANDS]


def test_error_defs_match_v1_registry():
    assert {k: list(v) for k, v in protocol.ERROR_DEFS.items()} == GOLDEN["error_defs"]


def test_down_payloads_match_v1_builder():
    c = ReplyCache(None)
    for cmd, want in GOLDEN["down_payloads"].items():
        reply_cmd, body, used = c.down_reply(cmd, {})
        assert reply_cmd == f"{cmd}_RESULT" and not used
        assert body == want
        assert list(body) == list(want)


# ───────────────────────── 프레임·로그 단위 ─────────────────────────

def test_pack_message_bytes():
    pkt = protocol.pack_message("GET_SPUTTER_STATUS_RESULT", {"request_id": "a", "data": {"x": "한글", "y": 1.5}},
                                now=lambda: 1793318400.9)
    v, f, cl, bl, ts = struct.unpack(HDR, pkt[:16])
    assert (v, f, cl, ts) == (1, 0, len("GET_SPUTTER_STATUS_RESULT"), 1793318400)
    assert pkt[16:] == '{"request_id": "a", "command": "GET_SPUTTER_STATUS_RESULT", "data": {"x": "한글", "y": 1.5}}'.encode()
    assert bl == len(pkt) - 16
    with pytest.raises(ValueError):
        protocol.unpack_header(b"\x01" * 15)


def test_log_line_format_and_entries():
    fixed = lambda: datetime(2026, 10, 30, 9, 8, 7)
    assert format_line("NET", "Client connected: x", fixed) == "2026-10-30 09:08:07 [NET] Client connected: x"
    assert format_line("PLC_REMOTE", "2026-10-30 12:00:00 [VACUUM_ON] read L_ATM", fixed) == \
        "2026-10-30 12:00:00 [PLC_REMOTE] [VACUUM_ON] read L_ATM"
    assert format_line("NET", "2026-10-30 12:00:00 a\nb", fixed) == "2026-10-30 09:08:07 [NET] 2026-10-30 12:00:00 a\nb"
    assert parse_log_entries(["[ERROR/HOST] [E205 / RETRY] x", ["NET", "y"], "no tag", "[NET] a\nb"]) == \
        [("ERROR/HOST", "[E205 / RETRY] x"), ("NET", "y"), ("HOST", "no tag"), ("NET", "a\nb")]


def test_server_log_file_daily(tmp_path):
    lg = ServerLog(tmp_path / "root", tmp_path / "fb", now=lambda: datetime(2026, 10, 30, 1, 2, 3))
    lg.start()
    lg.log("net", "Client connected: ('1.2.3.4', 5)")
    lg.log("PLC_HOST", "[REQ] 한글")
    lg.close()
    p = tmp_path / "root" / "CH1&2_Server" / "server_page_20261030.log"
    assert p.read_text(encoding="utf-8") == ("2026-10-30 01:02:03 [NET] Client connected: ('1.2.3.4', 5)\n"
                                             "2026-10-30 01:02:03 [PLC_HOST] [REQ] 한글\n")


def test_server_log_fallback_when_root_unusable(tmp_path):
    blocker = tmp_path / "root"
    blocker.write_text("파일이라 폴더를 못 만든다")
    lg = ServerLog(blocker, tmp_path / "fb", now=lambda: datetime(2026, 10, 30))
    lg.log("NET", "x")
    lg.close()
    assert (tmp_path / "fb" / "server_page_20261030.log").exists()


def test_command_csv_fallback_marker(tmp_path):
    blocker = tmp_path / "root"
    blocker.write_text("x")
    cc = CommandCsv(blocker, tmp_path / "fb", now=lambda: datetime(2026, 10, 30, 1, 2, 3))

    async def run():
        await cc.append({"server_time": "t", "peer": "p", "request_id": "r", "req_command": "C"})
        await cc.append({"server_time": "t2", "peer": "p", "request_id": "r2", "req_command": "C2"})
        await cc.aclose()
    asyncio.run(run())
    with open(tmp_path / "fb" / "remote_cmd_20261030.csv", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    assert [r["req_command"] for r in rows] == ["C", "__NAS_MISSING__", "C2"]
    assert rows[1]["res_result"] == "WARN" and rows[1]["res_message"].startswith("NAS write failed -> switched to LOCAL")


# ───────────────────────── 응답 한도 ─────────────────────────

def test_reply_timeouts_from_v1_timing():
    assert reply_timeout_s("GET_SPUTTER_STATUS", {}) == 5.0
    assert reply_timeout_s("GET_LOADING_2_SENSOR", {}) == 5.0
    assert reply_timeout_s("GET_RECIPE", {}) == 20.0 + 30.0
    assert reply_timeout_s("START_SPUTTER", {}) == 30.0 + 20.0 + 30.0
    assert reply_timeout_s("START_PLASMA_CLEANING", {}) == 30.0 + 20.0 + 30.0
    assert reply_timeout_s("VACUUM_ON", {}) == pytest.approx(660.0 + 35.3 + 30.0)
    assert reply_timeout_s("VACUUM_ON", {"timeout_s": 100}) == pytest.approx(100.0 + 35.3 + 30.0)
    assert reply_timeout_s("VACUUM_OFF", {}) == 240.0 + 5.0 + 30.0
    assert reply_timeout_s("4PIN_UP", {}) == pytest.approx(5.0 + 20.0 + 0.18 + 30.0)
    assert reply_timeout_s("CH2_GATE_OPEN", {"wait_s": "x"}) == pytest.approx(0.18 + 5.0 + 30.0)
    assert reply_timeout_s("CH1_CHUCK_UP", {}) == pytest.approx(90.0 + 0.2 + 90.0 + 30.0)
    assert reply_timeout_s("CH2_CHUCK_DOWN", {"wait_s": 10}) == pytest.approx(0.2 + 10.0 + 30.0)
    assert reply_timeout_s("START_SPUTTER", {}, {"CHAMBER_HOST_START_WAIT_TIMEOUT_S": 40.0,
                                                  "ROBOT_API_TIMEOUT_MARGIN_S": 5.0}) == 75.0


# ───────────────────────── 제어 프로그램 꺼짐 ─────────────────────────

STATUS_BODY = {"result": "success", "message": "OK", "Chamber_1": "idle", "Chamber_2": "error",
               "Loadlock_Chamber": "idle", "vacuum": True, "eta": {"CH1": {"remaining_s": 300}, "CH2": {"remaining_s": None}}}
RECIPE_BODY = {"result": "success", "message": "OK", "base_dir": "C:\\R", "folder": "CH1", "files": ["a.csv"], "count": 1}


def _respond(req):
    if req["cmd"] == "GET_SPUTTER_STATUS":
        return {"reply_cmd": "GET_SPUTTER_STATUS_RESULT", "body": STATUS_BODY, "log": []}
    if req["cmd"] == "GET_RECIPE":
        return {"reply_cmd": "GET_RECIPE_RESULT", "body": RECIPE_BODY, "log": []}
    return {"reply_cmd": f"{req['cmd']}_RESULT", "body": {"result": "success", "message": "OK"}, "log": []}


def _busy_status():
    want = json.loads(json.dumps(STATUS_BODY))
    want.update(Chamber_1="running", Chamber_2="running", Loadlock_Chamber="running")
    want["eta"] = {"CH1": {"remaining_s": None}, "CH2": {"remaining_s": None}}
    return want


def test_down_keeps_connection_and_uses_cache(tmp_path):
    async def run():
        async with Rig(tmp_path, _respond) as rig:
            r, w, _ = await rig.connect()
            (h, m), = await rig.ask(r, w, protocol.pack_request("GET_SPUTTER_STATUS", {}, "s1"))
            assert m["data"] == STATUS_BODY
            rig.api.stop()                       # 제어 프로그램 꺼짐(연결 거부)
            (h, m), = await rig.ask(r, w, protocol.pack_request("get_sputter_status", {}, "s2"))
            assert m == {"request_id": "s2", "command": "GET_SPUTTER_STATUS_RESULT", "data": _busy_status()}
            assert list(m["data"]) == list(STATUS_BODY)
            (h, m), = await rig.ask(r, w, protocol.pack_request("GET_LOADING_1_SENSOR", {}, "l1"))
            assert m["data"] == GOLDEN["down_payloads"]["GET_LOADING_1_SENSOR"]
            (h, m), = await rig.ask(r, w, protocol.pack_request("GET_RECIPE", {"folder": "ch2"}, "g1"))
            assert m["data"]["error_code"] == "E227"
            for cmd in COMMANDS[4:]:
                (h, m), = await rig.ask(r, w, protocol.pack_request(cmd, {"ch": 1}, cmd))
                assert m["command"] == f"{cmd}_RESULT"
                assert m["data"]["error_code"] == "E205" and m["data"]["client_action"] == "RETRY"
                assert m["data"]["detail"] == f"{cmd} 불가 — 제어 프로그램 응답 없음(재시작 중)"
                assert "meta" not in m["data"]
            assert rig.srv.down_count == 3 + 14
            assert any(t == "NET" and "control program unreachable: cmd=START_SPUTTER" in x for t, x in rig.lines)
            assert ["ERROR/HOST", "[E205 / RETRY] 공정 실행 중 | 상세: START_SPUTTER 불가 — 제어 프로그램 응답 없음(재시작 중)"] \
                in rig.lines
            # 연결은 계속 살아 있다 — 끊긴 기록 없음
            assert not any(x.startswith("Client closed") for _, x in rig.lines)
            w.close()
    asyncio.run(run())


def test_down_no_cache_status_e203(tmp_path):
    api = FakeApi(_respond)
    port = api.port
    api._httpd.server_close()                    # 듣지 않는 포트

    async def run():
        async with Rig(tmp_path, cfg={"ROBOT_API_PORT": port}, api=api) as rig:
            r, w, _ = await rig.connect()
            (h, m), = await rig.ask(r, w, protocol.pack_request("GET_SPUTTER_STATUS", {}, "s1"))
            assert m["data"] == GOLDEN["down_payloads"]["GET_SPUTTER_STATUS"]
            w.close()
    asyncio.run(run())


def test_disk_cache_survives_restart(tmp_path):
    cache_path = tmp_path / "state" / "robot_last_replies.json"

    async def first():
        async with Rig(tmp_path, _respond, cache_path=cache_path) as rig:
            r, w, _ = await rig.connect()
            await rig.ask(r, w, protocol.pack_request("GET_SPUTTER_STATUS", {}, "a"))
            await rig.ask(r, w, protocol.pack_request("GET_RECIPE", {"folder": "ch1"}, "b"))
            await rig.ask(r, w, protocol.pack_request("START_SPUTTER", {"ch": 1}, "c"))   # 캐시 안 함
            w.close()
    asyncio.run(first())
    saved = json.loads(cache_path.read_text(encoding="utf-8"))
    assert set(saved["replies"]) == {"GET_SPUTTER_STATUS", "GET_RECIPE|CH1"}

    api = FakeApi(_respond)
    port = api.port
    api._httpd.server_close()

    async def second():
        async with Rig(tmp_path, cfg={"ROBOT_API_PORT": port}, api=api, cache_path=cache_path) as rig:
            r, w, _ = await rig.connect()
            (h, m), = await rig.ask(r, w, protocol.pack_request("GET_SPUTTER_STATUS", {}, "x"))
            assert m["data"] == _busy_status()
            (h, m), = await rig.ask(r, w, protocol.pack_request("GET_RECIPE", {"folder": " Ch1 "}, "y"))
            assert m["data"] == RECIPE_BODY
            (h, m), = await rig.ask(r, w, protocol.pack_request("GET_RECIPE", {"folder": "ALD"}, "z"))
            assert m["data"]["error_code"] == "E227"
            w.close()
    asyncio.run(second())


def test_broken_cache_file_is_ignored(tmp_path):
    p = tmp_path / "c.json"
    p.write_text("{깨짐", encoding="utf-8")
    c = ReplyCache(p)
    assert c.load_error and c.get("GET_SPUTTER_STATUS") is None
    assert c.put("GET_SPUTTER_STATUS", {}, STATUS_BODY) is True
    assert c.put("GET_SPUTTER_STATUS", {}, STATUS_BODY) is False          # 같으면 다시 쓰지 않음
    assert c.put("GET_SPUTTER_STATUS", {}, {"result": "fail"}) is False   # 실패 응답은 캐시 안 함
    assert ReplyCache(p).get("GET_SPUTTER_STATUS")["body"] == STATUS_BODY
    assert as_busy("GET_LOADING_1_SENSOR", {"result": "success", "message": "OK", "value": True})["value"] is True


def test_api_timeout_then_recovery_same_connection(tmp_path):
    async def run():
        async with Rig(tmp_path, _respond, cfg={"ROBOT_API_STATUS_TIMEOUT_S": 0.3}) as rig:
            r, w, _ = await rig.connect()
            await rig.ask(r, w, protocol.pack_request("GET_SPUTTER_STATUS", {}, "a"))
            rig.api.delay_s = 1.0
            t0 = time.monotonic()
            (h, m), = await rig.ask(r, w, protocol.pack_request("GET_SPUTTER_STATUS", {}, "b"))
            assert time.monotonic() - t0 < 0.9
            assert m["data"]["Chamber_1"] == "running"
            await asyncio.sleep(1.0)
            rig.api.delay_s = 0.0
            (h, m), = await rig.ask(r, w, protocol.pack_request("GET_SPUTTER_STATUS", {}, "c"))
            assert m["data"] == STATUS_BODY                      # 다시 살아나면 그대로 전달
            w.close()
    asyncio.run(run())


def test_api_500_or_bad_shape_is_down(tmp_path):
    def bad(req):
        if req["cmd"] == "VACUUM_ON":
            raise RuntimeError("제어 프로그램 내부 오류")
        return {"no_reply_cmd": 1}

    async def run():
        async with Rig(tmp_path, bad) as rig:
            r, w, _ = await rig.connect()
            (h, m), = await rig.ask(r, w, protocol.pack_request("VACUUM_ON", {}, "a"))
            assert m["data"]["error_code"] == "E205"
            (h, m), = await rig.ask(r, w, protocol.pack_request("4PIN_DOWN", {}, "b"))
            assert m["data"]["error_code"] == "E205"
            assert any("HTTP 500" in x for _, x in rig.lines)
            assert any("응답 형식 오류" in x for _, x in rig.lines)
            w.close()
    asyncio.run(run())


def test_server_defense_non_dict_body(tmp_path):
    """제어 프로그램이 dict 아닌 body 를 주면 v1 server.py:369-383 방어 E111."""
    def respond(req):
        return {"reply_cmd": "GET_LOADING_1_SENSOR_RESULT", "body": [1, 2], "log": []}

    async def run():
        async with Rig(tmp_path, respond) as rig:
            r, w, peer = await rig.connect()
            (h, m), = await rig.ask(r, w, protocol.pack_request("GET_LOADING_1_SENSOR", {}, "q"))
            assert m["command"] == "GET_LOADING_1_SENSOR_RESULT"
            assert m["data"]["error_code"] == "E111"
            assert m["data"]["detail"] == "Handler returned non-dict: list raw=[1, 2]"
            assert m["data"]["meta"] == {"peer": peer, "request_id": "q", "cmd": "GET_LOADING_1_SENSOR"}
            w.close()
    asyncio.run(run())


# ───────────────────────── 동시 접속·순차 처리 ─────────────────────────

def test_connections_concurrent_requests_sequential(tmp_path):
    def respond(req):
        if req["cmd"] == "VACUUM_ON":
            time.sleep(0.6)
        return _respond(req)

    async def run():
        async with Rig(tmp_path, respond) as rig:
            r1, w1, _ = await rig.connect()
            r2, w2, _ = await rig.connect()
            # 연결 1: 긴 명령 + 상태 질의를 붙여 보냄 → 순서대로
            w1.write(protocol.pack_request("VACUUM_ON", {}, "v") + protocol.pack_request("GET_SPUTTER_STATUS", {}, "s"))
            await w1.drain()
            await asyncio.sleep(0.1)
            t0 = time.monotonic()
            (h, m), = await rig.ask(r2, w2, protocol.pack_request("GET_LOADING_2_SENSOR", {}, "other"))
            assert time.monotonic() - t0 < 0.4                    # 다른 연결은 기다리지 않음
            buf = b""
            while len(frames(buf)) < 2:
                buf += await asyncio.wait_for(r1.read(65536), timeout=5.0)
            got = [json.loads(b)["request_id"] for _, b in frames(buf)]
            assert got == ["v", "s"]
            w1.close()
            w2.close()
    asyncio.run(run())


# ───────────────────────── 별도 프로그램 실행 ─────────────────────────

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.skipif(sys.platform == "win32", reason="SIGINT 종료 확인은 리눅스에서 — 윈도우 확인 필요(Ctrl+C)")
def test_cli_runs_as_separate_program(tmp_path):
    port, api_port = _free_port(), _free_port()
    cfg = tmp_path / "cfg.json"
    cfg.write_text(json.dumps({"values": {"HOST_SERVER_HOST": "127.0.0.1", "ROBOT_CACHE_PATH": str(tmp_path / "c.json"),
                                          "LOCAL_FALLBACK_SERVER_DIR": str(tmp_path / "fb"), "ROBOT_LOG_ECHO": True}}),
                   encoding="utf-8")
    p = subprocess.Popen([sys.executable, "-m", "v2.robot_server", "--config", str(cfg), "--port", str(port),
                          "--api-port", str(api_port), "--log-root", str(tmp_path / "logs"), "--fake-api"],
                         cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8")
    try:
        s = None
        for _ in range(100):
            try:
                s = socket.create_connection(("127.0.0.1", port), timeout=0.5)
                break
            except OSError:
                time.sleep(0.1)
        assert s is not None, "로봇 서버가 뜨지 않음"
        s.settimeout(5.0)
        s.sendall(protocol.pack_request("CH1_GATE_OPEN", {}, "cli"))
        buf = b""
        while len(frames(buf)) < 1:
            buf += s.recv(65536)
        (h, body), = frames(buf)
        m = json.loads(body)
        assert m == {"request_id": "cli", "command": "CH1_GATE_OPEN_RESULT", "data": {"result": "success", "message": "OK"}}
        s.close()
        time.sleep(0.2)
    finally:
        p.send_signal(signal.SIGINT)
        out, _ = p.communicate(timeout=10)
    assert p.returncode == 0, out
    assert "[NET] Host started on ('127.0.0.1', " in out and "[NET] Host closed" in out
    log = (tmp_path / "logs" / "CH1&2_Server" / f"server_page_{datetime.now():%Y%m%d}.log").read_text(encoding="utf-8")
    assert "[PLC_HOST] [REQ] peer=('127.0.0.1', " in log and "cmd=CH1_GATE_OPEN_RESULT" in log
    assert (tmp_path / "logs" / "CH1&2_Server" / f"remote_cmd_{datetime.now():%Y%m%d}.csv").exists()

"""내부 명령 API(v2/ch12/robot_api) — 실제 HTTP(127.0.0.1)로 계약·중복 id·이벤트·저널 상태, 그리고 로봇 서버와 이어 붙인 끝-끝 시험.

끝-끝: 로봇 TCP 클라이언트(16바이트 헤더 + JSON) → v2/robot_server(RobotServer·Forwarder) → robot_api(HTTP) → robot_ops →
PLC 드라이버·worker → PLC 시뮬레이터. 실제 시계(빠른 명령만 — 게이트 대기 같은 긴 시퀀스는 test_robot_ops 가 가상 시계로 본다).
응답 data 는 v1 골든(robot_handlers.json — v1 handlers 실제 실행)과 바이트가 같아야 한다(peer 포트·경로·PLC 로그 시각만 바뀜).
"""
from __future__ import annotations

import asyncio
import http.client
import json
import struct
import threading
import time
from pathlib import Path

import pytest

from v2.common.clock import RealClock
from v2.ch12.robot_api import CMD_PATH, EVENT_PATH, HEALTH_PATH, RobotApi, dispatch_robot
from v2.ch12.sim import robot_rig as RR

GOLDEN = {c["name"]: c for c in json.loads((Path(__file__).parent / "golden" / "robot_handlers.json")
                                            .read_text(encoding="utf-8"))["cases"]}
HDR = "!BBHIQ"


def post(port: int, path: str, body, raw: bool = False):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    data = body if raw else json.dumps(body, ensure_ascii=False).encode("utf-8")
    c.request("POST", path, body=data, headers={"Content-Type": "application/json; charset=utf-8"})
    r = c.getresponse()
    out = r.status, json.loads(r.read().decode("utf-8"))
    c.close()
    return out


class FakeOps:
    """handle 만 있는 가짜(실행 횟수·지연)."""

    def __init__(self, delay_s: float = 0.0) -> None:
        self.calls = []
        self.delay_s = delay_s
        self.on_report = None

    def handle(self, cmd, data, raw=None, peer="", received_at=None, request_id=None):
        self.calls.append((cmd, data, raw, peer, received_at))
        if self.delay_s:
            time.sleep(self.delay_s)
        return f"{cmd}_RESULT", {"result": "success", "message": "OK", "n": len(self.calls)}, [["PLC_REMOTE", "x"]]


def cmd_body(cmd="GET_SPUTTER_STATUS", rid="id-1", data=None):
    return {"id": rid, "cmd": cmd, "data": data or {}, "raw": {"request_id": "r1", "command": cmd, "data": data or {}},
            "peer": "127.0.0.1:50123", "received_at": "2026-10-30T09:00:00.000"}


# ───────────────────────── API 계약(가짜 처리) ─────────────────────────
def test_command_contract_and_bind():
    ops = FakeOps()
    with RobotApi(ops, port=0) as api:
        assert api._httpd.server_address[0] == "127.0.0.1"
        st, out = post(api.port, CMD_PATH, cmd_body())
        assert st == 200 and out == {"reply_cmd": "GET_SPUTTER_STATUS_RESULT",
                                     "body": {"result": "success", "message": "OK", "n": 1}, "log": [["PLC_REMOTE", "x"]]}
        cmd, data, raw, peer, rx = ops.calls[0]
        assert (cmd, data, raw["request_id"], peer, rx) == ("GET_SPUTTER_STATUS", {}, "r1", "127.0.0.1:50123",
                                                           "2026-10-30T09:00:00.000")
        assert post(api.port, CMD_PATH, b"{bad", raw=True)[0] == 400
        assert post(api.port, CMD_PATH, [1, 2])[0] == 400
        assert post(api.port, CMD_PATH, {"id": "x"})[0] == 400
        assert post(api.port, "/nope", {})[0] == 404
        c = http.client.HTTPConnection("127.0.0.1", api.port, timeout=5)
        c.request("GET", HEALTH_PATH)
        assert json.loads(c.getresponse().read()) == {"ok": True, "inflight": 0}
        c.close()


def test_same_id_not_executed_twice():
    ops = FakeOps()
    with RobotApi(ops, port=0) as api:
        a = post(api.port, CMD_PATH, cmd_body(rid="dup"))[1]
        b = post(api.port, CMD_PATH, cmd_body(rid="dup"))[1]
        assert len(ops.calls) == 1 and api.executed == 1
        assert b.pop("duplicate") is True and a == b
        post(api.port, CMD_PATH, cmd_body(rid="other"))
        assert len(ops.calls) == 2


def test_same_id_concurrent_waits_for_first():
    ops = FakeOps(delay_s=0.3)
    with RobotApi(ops, port=0) as api:
        res = []
        ts = [threading.Thread(target=lambda: res.append(post(api.port, CMD_PATH, cmd_body(rid="same"))[1]))
              for _ in range(3)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(10)
        assert len(ops.calls) == 1 and len(res) == 3
        assert sum(1 for r in res if r.get("duplicate")) == 2
        assert all(r["body"] == res[0]["body"] for r in res)


def test_state_roundtrip_prevents_reexecution():
    ops = FakeOps()
    with RobotApi(ops, port=0, keep=3) as api:
        for i in range(5):
            post(api.port, CMD_PATH, cmd_body(rid=f"k{i}"))
        st = api.to_state()
        assert [k for k, _ in st["done"]] == ["k2", "k3", "k4"]                   # 최근 keep 개
    json.dumps(st)                                                              # 저널에 그대로 쓸 수 있다
    ops2 = FakeOps()
    with RobotApi(ops2, port=0) as api2:
        assert api2.from_state(st) == 3 and api2.seen("k4")
        out = post(api2.port, CMD_PATH, cmd_body(rid="k4"))[1]
        assert out["duplicate"] is True and ops2.calls == []
        assert api2.from_state(None) == 0 and api2.from_state({"done": [["x", "bad"]]}) == 0


def test_event_endpoint_emits():
    got = []
    ops = FakeOps()
    with RobotApi(ops, port=0, emit=lambda *a, **k: got.append((a, k))) as api:
        st, out = post(api.port, EVENT_PATH, {"kind": "host_error", "code": "E104", "detail": "[E104 / STOP] 잘못된 JSON 본문"})
        assert st == 200 and out == {"ok": True}
    assert got == [(("robot_server", "host_error", "[E104 / STOP] 잘못된 JSON 본문"), {"level": "warn", "code": "E104"})]
    assert ops.on_report is not None                     # 실패 보고도 같은 emit 로(D09)
    ops.on_report({"error_code": "E301", "message": "게이트 닫힘 미확인", "detail": "x"}, "HOST")
    assert got[-1] == (("robot", "host_error", "[E301] 게이트 닫힘 미확인 | 상세: x"), {"level": "warn", "code": "E301"})


# ───────────────────────── 실제 robot_ops(실제 시계) ─────────────────────────
@pytest.fixture
def real_rig(tmp_path):
    rig = RR.RobotRig(out_dir=tmp_path, robot_csv=True, clock=RealClock())
    yield rig
    rig.done()


def _norm_body(body, rig) -> str:
    roots = [(str(Path(rig.cfg["ROBOT_RECIPE_ROOT_DIR"])), "<ROOT>")]
    return RR.norm_text(json.dumps(body, ensure_ascii=False, default=str), roots)


@pytest.mark.parametrize("name", ["status_idle_atm", "status_ch1_running", "loading1_true", "loading2_false",
                                  "recipe_ch1", "recipe_bad_folder", "start_ch3", "g1close_already", "g2close_already",
                                  "g1open_atm", "c1down_already", "start_ch_abc_lower"])
def test_http_matches_v1_golden(real_rig, name):
    case = GOLDEN[name]
    RR.prepare_files(Path(real_rig.cfg["ROBOT_RECIPE_ROOT_DIR"]), case)
    RR.apply_state(real_rig.sim, case.get("state"))
    real_rig.set_engines(case.get("eng"))
    raw = dict(case.get("raw") or {})
    raw.setdefault("request_id", "r1")
    raw.setdefault("command", case["cmd"])
    data = RR.subst(case.get("data") or {}, "")
    with RobotApi(real_rig.ops, real_rig.c, port=0) as api:
        st, out = post(api.port, CMD_PATH, {"id": f"{name}-1", "cmd": case["cmd"].upper().strip(), "data": data,
                                            "raw": raw, "peer": RR.PEER, "received_at": "2026-10-30T09:00:00.000"})
    want = case["golden"]["results"][0]
    assert st == 200 and out["reply_cmd"] == want["reply_cmd"]
    assert _norm_body(out["body"], real_rig) == want["body"]
    assert [[t, RR.TS_RE.sub("<TS> ", x).split("\n", 1)[0] if t == "NET" else RR.TS_RE.sub("<TS> ", x)]
            for t, x in out["log"]] == want["log"]


def test_dispatch_robot_is_the_one_path(real_rig):
    rc, body, log = dispatch_robot(real_rig.ops, "GET_LOADING_1_SENSOR", {}, {"request_id": "z"}, RR.PEER, None)
    assert rc == "GET_LOADING_1_SENSOR_RESULT" and body == {"result": "success", "message": "OK", "value": False}
    assert log and log[0][0] == "PLC_REMOTE" and "read LOADING_1_SENSOR_LAMP (addr=480) -> False" in log[0][1]


# ───────────────────────── 끝-끝: 로봇 TCP → robot_server → robot_api → robot_ops → PLC sim ─────────────────────────
def _pack(command, data, rid):
    body = json.dumps({"request_id": rid, "command": command, "data": data}, ensure_ascii=False).encode("utf-8")
    return struct.pack(HDR, 1, 0, len(command.encode("utf-8")), len(body), 0) + body


async def _read_frame(r):
    h = await asyncio.wait_for(r.readexactly(16), 15)
    _v, _f, _cl, n, _ts = struct.unpack(HDR, h)
    return json.loads((await asyncio.wait_for(r.readexactly(n), 15)).decode("utf-8"))


def test_end_to_end_robot_server(real_rig, tmp_path):
    from v2.robot_server.forward import Forwarder
    from v2.robot_server.log import CommandCsv, ServerLog
    from v2.robot_server.server import RobotServer
    lines = []
    for name in ("loading1_true",):
        RR.apply_state(real_rig.sim, GOLDEN[name].get("state"))
    real_rig.set_engines(GOLDEN["status_idle_atm"]["eng"])
    api = RobotApi(real_rig.ops, real_rig.c, port=0).start()
    slog = ServerLog(tmp_path / "srvlog", tmp_path / "fb", on_line=lambda t, x, line: lines.append([t, x]))
    csvlog = CommandCsv(tmp_path / "srvlog", tmp_path / "fb")
    srv = RobotServer("127.0.0.1", 0, Forwarder({"ROBOT_API_PORT": api.port, "ROBOT_API_CONNECT_TIMEOUT_S": 2.0}),
                      slog, csv=csvlog)
    reqs = [("GET_SPUTTER_STATUS", {}, "e1", "status_idle_atm"), ("GET_LOADING_1_SENSOR", {}, "e2", "loading1_true"),
            ("START_SPUTTER", {"ch": 3, "recipe": "{RECIPE}"}, "e3", "start_ch3"),
            ("CH1_GATE_CLOSE", {}, "e4", "g1close_already"), ("NOPE", {}, "e5", None)]

    async def run():
        await srv.start()
        try:
            r, w = await asyncio.open_connection("127.0.0.1", srv.bound_port)
            peer = str(w.get_extra_info("sockname")[:2])
            out = []
            for cmd, data, rid, _ in reqs:
                w.write(_pack(cmd, RR.subst(data, "recipe_a.csv"), rid))
                await w.drain()
                out.append(await _read_frame(r))
            w.close()
            return peer, out
        finally:
            await srv.aclose()
    try:
        peer, out = asyncio.run(run())
    finally:
        api.stop()
        slog.close()
    gold_peer = str(("127.0.0.1", 50123))
    for (cmd, _data, rid, gname), got in zip(reqs, out):
        assert got["request_id"] == rid
        if gname is None:
            assert got["command"] == "NOPE_RESULT" and got["data"]["error_code"] == "E106"
            continue
        want = GOLDEN[gname]["golden"]["results"][0]
        assert got["command"] == want["reply_cmd"]
        assert json.dumps(got["data"], ensure_ascii=False) == want["body"].replace(gold_peer, peer)
    assert api.executed == 4
    plc_lines = [x for t, x in lines if t == "PLC_REMOTE"]                      # 제어 프로그램 로그 줄이 로봇 서버 로그로
    assert any("[GET_SPUTTER_STATUS] read L_ATM (addr=99) -> True" in x for x in plc_lines)
    assert any(t == "ERROR/HOST" and "E201" in x for t, x in lines)

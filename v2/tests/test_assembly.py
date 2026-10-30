"""챔버12 조립(assembly.build — 실행 프로그램과 시험대 Ch12FullRig 가 같은 것) — 웹·로컬·로봇 명령이 모두 core 한 곳을 지난다,
로봇 끝-끝(로봇 TCP → robot_server → robot_api → core → RobotOps → 실제 엔진 START_SPUTTER, CH1·CH2 동시), 허브 state 의 엔진·
로봇 상태, 1초 행의 CH2 히터·두께 블록.
"""
import asyncio
import json
import struct
import threading
from pathlib import Path

from v2.common.clock import RealClock, VirtualClock
from v2.common.commands import Source
from v2.ch12 import snapshot_fields as SF
from v2.ch12.sim.rig import Ch12FullRig

TEST_ROWS = [{"#": "test", "Process_name": "T1", "time": "20s"}]
HDR = "!BBHIQ"


def test_build_wires_everything_and_all_paths_pass_core(tmp_path):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = Ch12FullRig(c, tmp_path, cfg={"CAMERA_MIN_FREE_GB": 0.0}, robot_api=False)
    try:
        a = rig.asm
        assert {"proc.ch1", "proc.ch2", "pc", "tsp", "heater", "thk.ch2", "robot", "assembly"} <= set(rig.engines)
        assert rig.command == a.core.command and rig.safety is a.safety and rig.run_state is a.run_state
        assert a.proc["ch2"].heater is a.heater and a.proc["ch2"].thickness is a.thickness["ch2"]
        assert a.robot_ops.engines is a.adapter and a.robot_ops.core is a.core and set(a.presputter) == {"ch1", "ch2"}
        assert getattr(a.proc["ch1"], "monitor", None) is not None and a.proc["ch1"].gun_targets is not None
        assert rig.start()
        rig.loop.run(3)
        seen = []
        orig = a.core.command

        def spy(kind, args=None, source=Source.WEB, cid=None, by=None):
            seen.append((kind, getattr(source, "value", source)))
            return orig(kind, args, source=source, cid=cid, by=by)
        a.core.command = spy
        rig.command = spy
        assert rig.hub.run_command("PROCESS_STOP", {"ch": 1}, Source.WEB, cid="w1", by="op@web").ok       # 웹(허브 길)
        assert rig.hub.run_command("PROCESS_STOP", {"ch": 2}, Source.HMI, cid="l1", by="local").ok        # 로컬 화면
        rc, body, _log = a.robot_status.dispatch(a.robot_ops, "GET_SPUTTER_STATUS", {}, {"request_id": "r1"}, "('127.0.0.1', 5)")
        assert rc == "GET_SPUTTER_STATUS_RESULT" and body["result"] == "success"
        rc, body, _log = a.robot_status.dispatch(a.robot_ops, "NOPE", {}, {"request_id": "r2"}, "x")                  # 18개 밖 — v1 E106
        assert rc == "NOPE_RESULT" and body["error_code"] == "E106"
        assert seen == [("PROCESS_STOP", "web"), ("PROCESS_STOP", "hmi"), ("ROBOT_GET_SPUTTER_STATUS", "robot")]
        msgs = [m for m in rig.hub.recent if m["type"] == "cmd_result"] if hasattr(rig.hub, "recent") else []
        assert not msgs or {m["data"]["cmdId"] for m in msgs} >= {"w1", "l1"}
        # 허브 state — 엔진·로봇 상태, 1초 행 — CH2 히터·두께 블록
        assert rig.command("RECIPE_LOAD", {"ch": 1, "rows": TEST_ROWS}, source=Source.WEB).ok
        assert rig.command("PROCESS_START", {"ch": 1}, source=Source.WEB).ok
        rows = []
        rig.row_listeners.append(rows.append)
        rig.loop.run(3)
        st = rig.hub.state_data(rows[-1])
        p1 = st["engines"]["proc.ch1"]
        assert p1["state"] == "RUNNING" and p1["blocks"] and p1["blocks"][0]["type"] and "started_upto" in p1
        assert "adjust" in p1 and p1["paused"] is False
        assert set(st["engines"]["robot"]) >= {"apiPort", "count", "last", "inflight"} and st["engines"]["robot"]["count"] == 2
        assert st["heater"] and "run" in st["heater"]
        assert "ch2.heater" in SF.BLOCK_ORDER and "ch2.thk" in SF.BLOCK_ORDER
        assert rows[-1]["ch2.heater.q"] == "ok" and rows[-1]["ch2.heater.pv"] is not None
        assert rows[-1]["ch2.heater.ready"] is not None and "ch2.thk.phase" in rows[-1]
        assert rows[-1]["ch2.thk.phase"] == "idle" and not rig.unknown_fields
        json.dumps(st, default=str)
    finally:
        rig.stop()
        c.close()


def _pack(command, data, rid):
    body = json.dumps({"request_id": rid, "command": command, "data": data}, ensure_ascii=False).encode("utf-8")
    return struct.pack(HDR, 1, 0, len(command.encode("utf-8")), len(body), 0) + body


async def _read_frame(r, timeout=60):
    h = await asyncio.wait_for(r.readexactly(16), timeout)
    _v, _f, _cl, n, _ts = struct.unpack(HDR, h)
    return json.loads((await asyncio.wait_for(r.readexactly(n), timeout)).decode("utf-8"))


def test_robot_tcp_end_to_end_start_sputter_ch1_ch2(tmp_path):
    """로봇 TCP → robot_server → 내부 명령 API(127.0.0.1) → core.command(ROBOT_START_SPUTTER) → RobotOps(v1 handlers) →
    EngineAdapter → 실제 공정 엔진(CH1·CH2 동시 — TEST 행). 실제 시계(로봇 서버·HTTP·시뮬레이터 스레드가 함께 돈다)."""
    from v2.robot_server.forward import Forwarder
    from v2.robot_server.log import CommandCsv, ServerLog
    from v2.robot_server.server import RobotServer
    clock = RealClock()
    rig = Ch12FullRig(clock, tmp_path / "rig", cfg={"CAMERA_MIN_FREE_GB": 0.0}, robot_api=True, schedules=False)
    rec = tmp_path / "recipes"
    rec.mkdir()
    for ch in (1, 2):
        (rec / f"t{ch}.csv").write_text(f"#,Process_name,time\ntest,R{ch},2s\n", encoding="utf-8")   # 실제 시계 — 짧게
    tick = None
    srv = None
    try:
        assert rig.start()
        tick = clock.start_thread(rig.loop.run, "tick")
        a = rig.asm
        counts = {}
        orig = a.core.command

        def spy(kind, args=None, source=Source.WEB, cid=None, by=None):
            counts[kind] = counts.get(kind, 0) + 1
            return orig(kind, args, source=source, cid=cid, by=by)
        a.core.command = spy
        slog = ServerLog(tmp_path / "srvlog", tmp_path / "fb")
        srv = RobotServer("127.0.0.1", 0, Forwarder({"ROBOT_API_PORT": a.robot_api.port, "ROBOT_API_CONNECT_TIMEOUT_S": 2.0}),
                          slog, csv=CommandCsv(tmp_path / "srvlog", tmp_path / "fb"))

        async def one(ch):
            r, w = await asyncio.open_connection("127.0.0.1", srv.bound_port)
            w.write(_pack("START_SPUTTER", {"ch": ch, "recipe": str(rec / f"t{ch}.csv")}, f"s{ch}"))
            await w.drain()
            got = await _read_frame(r)
            w.write(_pack("GET_SPUTTER_STATUS", {}, f"g{ch}"))
            await w.drain()
            st = await _read_frame(r)
            w.close()
            return got, st

        async def run():
            await srv.start()
            try:
                return await asyncio.gather(one(1), one(2))
            finally:
                await srv.aclose()
        (g1, s1), (g2, s2) = asyncio.run(run())
        for ch, g in ((1, g1), (2, g2)):
            assert g["command"] == "START_SPUTTER_RESULT" and g["request_id"] == f"s{ch}", g
            assert g["data"]["result"] == "success" and g["data"]["message"] == "SPUTTER START OK" and g["data"]["ch"] == ch, g
        assert {s1["data"]["Chamber_1"], s2["data"]["Chamber_2"]} == {"running"}
        assert counts["ROBOT_START_SPUTTER"] == 2 and counts["ROBOT_GET_SPUTTER_STATUS"] == 2
        e1, e2 = a.proc["ch1"], a.proc["ch2"]
        assert e1.owner == "robot" and e2.owner == "robot" and e1.origin == "host"
        ok = clock.wait_until(clock.monotonic() + 30, lambda: not e1.busy and not e2.busy)
        assert ok
        for e in (e1, e2):
            assert [x.data.get("result") for x in e.events if x.kind == "run_end"] == ["done"]
        assert a.robot_status.count >= 4
    finally:
        if srv is not None:
            pass
        rig.stop()
        if tick is not None:
            tick.join(5)

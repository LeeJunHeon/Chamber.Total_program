"""sim 종합 시나리오(사용자 지시 7장 ③) — 실행 프로그램과 같은 조립(sim.rig.Ch12FullRig = assembly.build — 시뮬레이터·실제 드라이버·
실제 엔진·안전·명령 처리 core·로봇·허브)을 가상 시계로 끝까지 돌린다. 시나리오마다 독립 시험 함수 하나.

- test_ch1_ch2_concurrent_process: 웹 명령으로 CH1(RF Pulse)·CH2(DC Pulse) 공정을 겹쳐 시작 — 둘 다 done, 서로 막지 않음
  (CH1 이 RUNNING 인 동안 CH2 시작이 받아들여지고, 겹친 구간에 두 챔버 블록이 모두 진행), 펄스 포트·MFC 는 챔버마다.
- test_robot_full_flow: 로봇 TCP 클라이언트 → v2/robot_server(16바이트 헤더+JSON) → 내부 명령 API(127.0.0.1) → core.command(ROBOT_*)
  → RobotOps(v1 handlers) → 엔진. GET_SPUTTER_STATUS → VACUUM_ON → CH1_GATE_OPEN → CH1_CHUCK_UP/DOWN → 4PIN_UP/DOWN →
  CH1_GATE_CLOSE → START_SPUTTER(시작 가드 통과 시점의 응답) → 상태 질의로 공정 끝까지 → START_PLASMA_CLEANING(쿨다운 뒤)
  → 상태 질의로 끝까지 → CH1_GATE_CLOSE(PC 가 GV 를 연 채 끝냄 — v1 PC-077) → VACUUM_OFF. 응답 형식 v1(헤더·request_id·
  '{명령}_RESULT'·data.result·message). Robot CSV 요청 카드(client_process started·finished 성공).
- test_plasma_cleaning: 웹 PC_START(CH1) → 같은 CH 챔버 시작 거부(PC_RUNNING)·채널 전환 거부, CH2 챔버는 동시에 돈다 →
  PC 성공(RF 0 W·SET OFF·GV 유지·MFC1 정리) → 바로 다시 시작은 쿨다운 거부 → 쿨다운 뒤 로컬 PC_STOP(STOP 결과).
- test_ch2_heater_process: CH2 히터 승온(램프)·도달 대기·유지(메인 동안 목표 근처) → 공정 끝 히터 OFF, 1초 행 ch2.heater.*.
- test_thickness_end_process: CH2 두께 모드(목표 nm) — 영점 → 메인 셔터 → 장비 두께가 목표에 닿아 끝(종료 방식 Thickness),
  ETA 는 끝까지 null, 1초 행 ch2.thk.*.
- test_adjust_skip_pause_edit: 공정 중 자유 제어 4종을 실제 명령 경로로 — 웹(가짜 웹 fake_web → uplink SSE → hub → core)과
  로컬(로컬 서버 POST /api/v1/command → hub → core). 일시정지·재개, 시퀀스 편집(삽입·시작한 블록 삭제 거부), 뒤 블록 값 예약,
  건너뛰기(허용·거부), 지금 블록 값 수정(MFC 되읽기 확인). cmd_result 가 웹에 by 와 함께.
- test_restart_resume_and_safe_stop: CH1 메인 증착 중·CH2 파워 켜기 중에 제어 프로그램을 죽이고(조립 버림 — 시계 닫기, 저널만
  남김) 같은 시뮬레이터에 새 조립 → CH1 이어가기(resumed·공백 포함 경과·done), CH2 안전 정지(PROGRAM_RESTART·출력 꺼짐).
- test_web_disconnect_reconnect: 가짜 웹 끊김 동안 공정은 계속(로컬 명령도), 보관함에 쌓임·UPLINK_DOWN → 다시 올라오면 끊긴 동안
  낸 메시지가 모두 도착·UPLINK_UP, 같은 웹 명령 id 를 다시 보내도 한 번만 실행.
- test_virtual_7_days(slow): 가상 시계 168 시간 연속 운전(APP_RETENTION) — 날마다 CH1·CH2 동시 공정, PC, TSP 매일 예약,
  CH2 프리스퍼터 매일 예약(전용 레시피). 매시 gc 뒤 메모리·스레드·큐·링 버퍼를 재고 날마다 표(환경 변수 CH12_7DAY_TABLE 이 있으면
  JSON 으로). 6~7 일 사이 전용 메모리 증가 상한 DAY7_GROWTH_MB, 스레드 수·큐·링 버퍼가 늘지 않음.
"""
from __future__ import annotations

import asyncio
import collections
import datetime as dt
import gc
import http.client
import json
import os
import struct
import threading
import time
from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.common.commands import Source
from v2.ch12 import process_states as PS
from v2.ch12.drivers import plc_map as PM
from v2.ch12.sim.rig import Ch12FullRig
from v2.tests import restart_rig as RR
from v2.tests.fake_web import FakeWeb

W0 = 1_760_000_000.0
CFG = dict(RR.CFG)                     # CH1 RF Pulse·CH2 DC Pulse·CH2 RF 연속 — 골든 시나리오가 쓰는 장비
P1 = dict(base_pressure=1e-5, working_pressure=3, process_time=0.3, shutter_delay=0.1, use_ar=True, ar_flow=20,
          use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True, process_note="c1")
P2 = dict(base_pressure=1e-5, working_pressure=5, process_time=0.3, shutter_delay=0.1, use_ar=True, ar_flow=20,
          use_dc_pulse=True, dc_pulse_power=80, dc_pulse_freq=20, use_ms=True, use_g1=True, process_note="c2")
PC_UI = {"gas_flow": "10", "rf_power": "100", "process_time": "0.1", "working_pressure": "2"}
HDR = "!BBHIQ"
PACE_AT = 0.85                         # 로봇 흐름 — 가상 초의 이 자리에서 실제 시간을 쉰다(test_robot_full_flow pacer)


# ───────────── 도우미 ─────────────
def full(tmp, cfg=None, robot_api=False, start=True, **kw) -> Ch12FullRig:
    c = VirtualClock(start_wall=kw.pop("wall", W0))
    c.register()
    rig = Ch12FullRig(c, Path(tmp), cfg=dict(CFG, **(cfg or {})), robot_api=robot_api, schedules=False, **kw)
    rig.sims["ch1.mfc"].sp[2] = 3.0 * 0.1            # SP3(CH1) 작업 압력 3 mTorr(proc_rig 와 같게)
    rig.sims["ch2.mfc"].sp[3] = 5.0 * 0.1            # SP4(CH2) 5 mTorr
    rig.rows = collections.deque(maxlen=4000)
    rig.row_listeners.append(rig.rows.append)
    keep_messages(rig)
    if start:
        assert rig.start()
        rig.loop.run(2)
    return rig


def keep_messages(rig) -> None:
    """허브가 낸 메시지(state 빼고)를 모두 남긴다 — 허브 링 버퍼(recent)는 오래된 것을 버리므로 시험 확인용 사본."""
    rig.msgs = []
    orig = rig.hub.publish

    def publish(type_, data):
        env = orig(type_, data)
        if type_ != "state":
            rig.msgs.append(env)
        return env
    rig.hub.publish = publish


def finish(rig) -> None:
    try:
        rig.stop()
    finally:
        rig.clock.close()


def drive(rig, until, max_ticks=3000, hooks=()) -> int:
    for h in hooks:
        rig.loop.tick_hooks.append(h)
    n = 0
    while n < max_ticks and not until():
        rig.loop.run(1)
        n += 1
    assert until(), f"{max_ticks} tick 안에 조건이 안 됨"
    return n


def web(rig, kind, args=None, by="op@web", cid=None):
    """웹 명령(허브 길 — uplink 가 부르는 것과 같은 hub.run_command(Source.WEB))."""
    return rig.hub.run_command(kind, dict(args or {}), Source.WEB, cid=cid or f"w-{kind}-{time.monotonic_ns()}", by=by)


def local(rig, kind, args=None, cid=None):
    return rig.hub.run_command(kind, dict(args or {}), Source.HMI, cid=cid or f"l-{kind}-{time.monotonic_ns()}", by="local")


def ends(e):
    return [(x.data.get("result"), x.code) for x in e.events if x.kind == "run_end"]


def blk(e, t):
    return lambda: e.cur_inst is not None and e.cur_inst.type == t and e.runner.is_running and not e.runner.shutdown


def hub_msgs(rig, type_):
    src = getattr(rig, "msgs", None)
    return [m for m in list(rig.hub.recent if src is None else src) if m["type"] == type_]


# ───────────── CH1·CH2 동시 공정 ─────────────
def test_ch1_ch2_concurrent_process(tmp_path):
    rig = full(tmp_path)
    try:
        a = rig.asm
        e1, e2 = a.proc["ch1"], a.proc["ch2"]
        seen = {"both": 0, "b1": set(), "b2": set(), "ports": False, "robot_both": False}

        def watch(t):
            s1, s2 = PS.chamber_state(e1), PS.chamber_state(e2)
            if s1 == "RUNNING" and s2 == "RUNNING":
                seen["both"] += 1
                seen["b1"].add(e1.cur_inst.type if e1.cur_inst else None)
                seen["b2"].add(e2.cur_inst.type if e2.cur_inst else None)
                own = rig.shared.snapshot()
                if own.get("pulse 192.168.1.50:4007") == "ch1.rfpulse" and own.get("pulse 192.168.1.50:4005") == "ch2.dcpulse":
                    seen["ports"] = True
                st = [a.adapter.chamber_state(n)["chamber_running"] for n in (1, 2)]
                seen["robot_both"] |= all(st)
        rig.loop.tick_hooks.append(watch)
        r1 = web(rig, "PROCESS_START", {"ch": 1, "params": P1}, by="op1@web")
        assert r1.ok, r1
        drive(rig, lambda: PS.chamber_state(e1) == "RUNNING" and e1.cur_inst is not None, 120)
        r2 = web(rig, "PROCESS_START", {"ch": 2, "params": P2}, by="op2@web")      # CH1 이 도는 중에 CH2 시작 — 막지 않음
        assert r2.ok, r2
        drive(rig, lambda: not e1.busy and not e2.busy, 1500)
        assert ends(e1) == [("done", None)] and ends(e2) == [("done", None)]
        assert e1.run_id != e2.run_id and e1.owner == "web" and e2.owner == "web"
        assert seen["both"] >= 20 and len(seen["b1"]) >= 2 and len(seen["b2"]) >= 2, seen     # 겹친 구간에 둘 다 진행
        assert seen["ports"] and seen["robot_both"]
        assert all(p.owner is None for p in rig.shared.pulse.ports.values()) and rig.shared.camera.users == set()
        assert any(c.startswith("Q1 20") for c in rig.sims["ch1.mfc"].set_commands_seen())          # 챔버마다 자기 MFC
        assert any(c.startswith("Q1 20") for c in rig.sims["ch2.mfc"].set_commands_seen())
        ran = [m["data"] for m in hub_msgs(rig, "run_end")]
        assert sorted((d["chamber"], d["result"]) for d in ran) == [("ch1", "done"), ("ch2", "done")], ran
        res = {m["data"]["cmdId"]: m["data"] for m in hub_msgs(rig, "cmd_result")}
        assert {d["by"] for d in res.values() if d["kind"] == "PROCESS_START"} == {"op1@web", "op2@web"}
        both_rows = [r for r in rig.rows if r.get("ch1.proc.state") == "RUNNING" and r.get("ch2.proc.state") == "RUNNING"]
        assert both_rows and all(r["ch1.proc.run_id"] == e1.run_id and r["ch2.proc.run_id"] == e2.run_id for r in both_rows)
        assert rig.internal_errors == 0 and not a.run_state.has_error("chamber", 1) and not a.run_state.has_error("chamber", 2)
    finally:
        finish(rig)


# ───────────── 로봇 명령 전체 흐름 ─────────────
def _pack(command, data, rid):
    body = json.dumps({"request_id": rid, "command": command, "data": data}, ensure_ascii=False).encode("utf-8")
    return struct.pack(HDR, 1, 0, len(command.encode("utf-8")), len(body), 0) + body


async def _frame(r, timeout=120):
    h = await asyncio.wait_for(r.readexactly(16), timeout)
    ver, flags, cl, n, ts = struct.unpack(HDR, h)
    body = await asyncio.wait_for(r.readexactly(n), timeout)
    obj = json.loads(body.decode("utf-8"))
    assert (ver, flags) == (1, 0) and cl == len(obj["command"].encode("utf-8")) and ts > 1_700_000_000   # v1 헤더
    assert list(obj) == ["request_id", "command", "data"]                                              # v1 바디 순서
    return obj


@pytest.mark.slow
def test_robot_full_flow(tmp_path):
    from v2.robot_server.forward import Forwarder
    from v2.robot_server.log import CommandCsv, ServerLog
    from v2.robot_server.server import RobotServer
    rig = full(tmp_path / "rig", robot_api=True)
    c = rig.clock
    a = rig.asm
    e1 = a.proc["ch1"]
    rec = tmp_path / "recipes"
    rec.mkdir()
    sputter = rec / "ch1_ar_rfp.csv"
    sputter.write_text("#,Process_name,Ar,Ar_flow,base_pressure,working_pressure,shutter_delay,process_time,main_shutter,"
                       "use_rf_pulse,rf_pulse_power,rf_pulse_freq,rf_pulse_duty_cycle\n"
                       "1,ROBOT-R1,T,20,1.00E-05,3,0.1,0.3,T,T,120,0.5,40\n", encoding="utf-8")
    pcr = rec / "pc.csv"
    pcr.write_text("use_ch,process_name,base_pressure,gas_flow,working_pressure,rf_power,process_time\r\n"
                   "1,PC-ROBOT,5.00E-06,10,2,100,0.1\r\n", encoding="utf-8-sig")
    kinds = collections.Counter()
    orig = a.core.command

    def spy(kind, args=None, source=Source.WEB, cid=None, by=None):
        kinds[kind] += 1
        return orig(kind, args, source=source, cid=cid, by=by)
    a.core.command = spy
    c.unregister()                                     # 시계는 tick 스레드·장비 worker·로봇 API 요청 스레드가 움직인다

    stop_pacer = threading.Event()

    def pacer():
        """가상 시간 속도 조절 — 매 초 PACE_AT(PLC 1초 읽기 세션이 끝난 뒤) 에서 실제 시간을 조금 쉰다. 로봇 요청은 실제 시간으로
        오므로 대개 이 자리(초의 뒷부분)에 도착한다 — 실제 운전처럼 tick 경계(1초 읽기가 PLC 를 잡고 있는 0~0.75 s)에 몰리지 않게.
        (tick 경계에 도착한 요청의 짧은 v1 마감 — PC GV 인터락 읽기 0.6 s — 은 결정 후보로 보고)"""
        while not stop_pacer.is_set():
            nxt = int(c.monotonic()) + PACE_AT
            if nxt <= c.monotonic() + 1e-6:
                nxt += 1.0
            c.wait_until(nxt)
            time.sleep(0.002)
    tick = c.start_thread(rig.loop.run, "tick")
    pace = c.start_thread(pacer, "pacer")
    slog = ServerLog(tmp_path / "srvlog", tmp_path / "fb")
    srv = RobotServer("127.0.0.1", 0, Forwarder({"ROBOT_API_PORT": a.robot_api.port, "ROBOT_API_CONNECT_TIMEOUT_S": 2.0}),
                      slog, csv=CommandCsv(tmp_path / "srvlog", tmp_path / "fb"))
    got = []
    statuses = []
    w0 = time.perf_counter()

    async def flow():
        r, w = await asyncio.open_connection("127.0.0.1", srv.bound_port)
        n = [0]

        async def call(cmd, data=None, ok=True):
            n[0] += 1
            rid = f"rq-{n[0]:03d}"
            w.write(_pack(cmd, data or {}, rid))
            await w.drain()
            obj = await _frame(r)
            got.append((cmd, obj))
            assert obj["request_id"] == rid and obj["command"] == f"{cmd}_RESULT", obj
            d = obj["data"]
            if ok and d.get("result") != "success":
                raise AssertionError(json.dumps({"cmd": cmd, "data": d, "pc": list(a.pc.ui_log)[-12:]}, ensure_ascii=False))
            return d

        async def until_idle(key, limit_s=90.0):
            t_end = time.monotonic() + limit_s
            saw_running = False
            while time.monotonic() < t_end:
                d = await call("GET_SPUTTER_STATUS")
                statuses.append(dict(d))
                if d[key] == "running":
                    saw_running = True
                elif saw_running:
                    return d
                await asyncio.sleep(0.05)
            raise AssertionError(f"{key} 가 끝나지 않음: {statuses[-3:]}")

        d = await call("GET_SPUTTER_STATUS")
        assert (d["Chamber_1"], d["Chamber_2"], d["vacuum"]) == ("idle", "idle", False)
        assert d["eta"] == {"CH1": {"remaining_s": None}, "CH2": {"remaining_s": None}}
        assert "L_VAC_READY_SW=TRUE" in (await call("VACUUM_ON"))["message"]
        assert (await call("GET_SPUTTER_STATUS"))["vacuum"] is True
        assert "G_V_1_OPEN_LAMP=TRUE" in (await call("CH1_GATE_OPEN"))["message"]
        assert (await call("CH1_CHUCK_UP"))["current"]["position"] == "mid"
        assert (await call("CH1_CHUCK_DOWN"))["current"]["position"] == "down"
        assert "L_PIN_UP_LAMP=TRUE" in (await call("4PIN_UP"))["message"]
        assert "L_PIN_DOWN_LAMP=TRUE" in (await call("4PIN_DOWN"))["message"]
        rig.sims["plc"].loading[1] = True                        # 로봇이 웨이퍼를 내려놓음(로딩 센서)
        d = await call("CH1_GATE_CLOSE")
        assert "CLOSE" in d["message"].upper(), d
        d = await call("START_SPUTTER", {"ch": 1, "recipe": str(sputter)})
        assert (d["message"], d["ch"]) == ("SPUTTER START OK", 1)
        assert PS.chamber_state(e1) in ("PREFLIGHT", "RUNNING")  # 응답은 시작 가드 통과 시점(공정은 아직 진행 중)
        assert not ends(e1)
        end = await until_idle("Chamber_1")
        assert end["Chamber_1"] == "idle" and end["eta"]["CH1"]["remaining_s"] is None
        etas = [s["eta"]["CH1"]["remaining_s"] for s in statuses if s["Chamber_1"] == "running"]
        assert any(isinstance(x, int) and x > 0 for x in etas), etas                   # 공정 중 ETA(확정 뒤 초)
        t_end = c.monotonic()
        while c.monotonic() < t_end + 65.0:                       # PC 쿨다운(같은 CH 챔버 끝 60 s)
            await asyncio.sleep(0.02)
        d = await call("START_PLASMA_CLEANING", {"ch": 1, "recipe": str(pcr)})
        assert d["result"] == "success", d
        statuses.clear()
        t_lim = time.monotonic() + 90
        while not a.pc.running and time.monotonic() < t_lim:
            await asyncio.sleep(0.02)
        while a.pc.running and time.monotonic() < t_lim:
            statuses.append(await call("GET_SPUTTER_STATUS"))
            await asyncio.sleep(0.05)
        assert a.pc.last_final and a.pc.last_final["ok"], a.pc.last_final
        assert statuses and any(s["Chamber_1"] == "running" for s in statuses), statuses[-3:]   # PC 중 CH1 'running'
        d = await call("CH1_GATE_CLOSE")                          # PC 가 GV 를 연 채 끝냄(v1 PC-077)
        d = await call("VACUUM_OFF")
        assert d["result"] == "success"
        d = await call("GET_SPUTTER_STATUS")
        assert (d["Chamber_1"], d["vacuum"]) == ("idle", False), d
        w.close()

    async def run():
        await srv.start()
        try:
            await flow()
        finally:
            await srv.aclose()
    try:
        asyncio.run(run())
        wall = time.perf_counter() - w0
        assert ends(e1) == [("done", None)] and e1.owner == "robot" and e1.origin == "host"
        order = [g[0] for g in got if g[0] != "GET_SPUTTER_STATUS"]
        assert order == ["VACUUM_ON", "CH1_GATE_OPEN", "CH1_CHUCK_UP", "CH1_CHUCK_DOWN", "4PIN_UP", "4PIN_DOWN",
                         "CH1_GATE_CLOSE", "START_SPUTTER", "START_PLASMA_CLEANING", "CH1_GATE_CLOSE", "VACUUM_OFF"]
        assert all(kinds[f"ROBOT_{cmd}"] >= 1 for cmd in order)                          # 모두 core 를 지남
        assert kinds["ROBOT_GET_SPUTTER_STATUS"] == sum(1 for g in got if g[0] == "GET_SPUTTER_STATUS")
        cp = [x.event.data for x in rig.events if x.kind == "client_process" and x.event is not None]
        fin = [(d["info"].get("target"), d["info"].get("result")) for d in cp if d["kind"] == "finished"]
        assert ("CH1", "성공") in fin and len([f for f in fin if f[1] == "성공"]) == 2, cp           # 공정·PC 요청 카드
        print(f"\n[robot_full_flow] 명령 {len(got)}개 · 가상 {c.monotonic():.0f} s · 실제 {wall:.1f} s")
    finally:
        stop_pacer.set()
        c.register()
        rig.stop()
        c.close()
        tick.join(5)
        pace.join(5)


# ───────────── 플라즈마 클리닝 ─────────────
def test_plasma_cleaning(tmp_path):
    rig = full(tmp_path)
    try:
        a = rig.asm
        pc, e1, e2 = a.pc, a.proc["ch1"], a.proc["ch2"]
        r = web(rig, "PC_START", {"ch": 1, "params": PC_UI}, by="pc@web")
        assert r.ok, r
        assert a.run_state.is_running("pc", 1)
        drive(rig, lambda: pc.pc.step == "process", 600)
        rej = web(rig, "PROCESS_START", {"ch": 1, "params": P1})                   # 같은 CH 챔버 — 거부
        assert not rej.ok and rej.reason == "PC_RUNNING", rej
        sw = local(rig, "PC_SELECT_CH", {"ch": 2})
        assert not sw.ok and sw.reason == "CHANNEL_SWITCH_RUNNING", sw
        r2 = web(rig, "PROCESS_START", {"ch": 2, "params": P2})                    # 다른 CH 챔버는 같이 돈다
        assert r2.ok, r2
        rig.loop.run(2)
        row = rig.rows[-1]
        assert row["pc.proc.state"] == "run" and row["pc.proc.owner"] == "web" and row["pc.set.rf_w"] == 100.0
        assert row["ch2.proc.state"] in ("PREFLIGHT", "RUNNING")
        drive(rig, lambda: not pc.running, 600)
        assert pc.last_final == {"ok": True, "stopped": False, "reason": None, "ch": 1, "status": "success"}
        plc = rig.sims["plc"]
        assert plc.regs[PM.REG_MAP["DCV_WRITE_1"]] == 0 and plc.coils[PM.COIL_MAP["DCV_SET_1"]] is False
        assert rig.chambers["ch1"].valves["gate"] is True                          # GV 는 연 채(PC-077)
        assert rig.sims["ch1.mfc"].set_commands_seen()[-6:] == ["Q3 10.0", "L31", "S4 0.2", "D4", "L30", "O"]
        again = web(rig, "PC_START", {"ch": 1, "params": PC_UI})
        assert not again.ok and again.reason == "COOLDOWN", again                  # 끝난 뒤 60 s 쿨다운
        drive(rig, lambda: not e2.busy, 1500)
        assert ends(e2) == [("done", None)]
        rig.loop.run(62)
        assert web(rig, "PC_START", {"ch": 1, "params": dict(PC_UI, process_time="1")}).ok
        drive(rig, lambda: pc.pc.step == "process", 600)
        stop = local(rig, "PC_STOP")
        assert stop.ok, stop
        drive(rig, lambda: not pc.running, 300)
        assert pc.last_final["stopped"] and pc.last_final["status"] == "stopped"
        assert plc.regs[PM.REG_MAP["DCV_WRITE_1"]] == 0 and plc.coils[PM.COIL_MAP["DCV_SET_1"]] is False
        cards = [x.data for x in pc.events if x.kind == "card" and x.data.get("api") == "notify_process_finished_detail"]
        assert [cd["payload"]["status"] for cd in cards] == ["success", "stopped"]
        assert not ends(e1) and rig.internal_errors == 0
    finally:
        finish(rig)


# ───────────── CH2 히터 공정 ─────────────
HEAT = dict(base_pressure=7e-6, working_pressure=5, process_time=0.3, shutter_delay=0.1, use_ar=True, ar_flow=20,
            use_dc_power=True, dc_power=100, use_ms=True, use_g1=True, use_heater=True, heater_temp=40, heater_ramp=10,
            heater_soak=0.5, process_note="heat")


def test_ch2_heater_process(tmp_path):
    rig = full(tmp_path)
    try:
        a = rig.asm
        e2 = a.proc["ch2"]
        seen = []

        def watch(t):
            r = rig.rows[-1] if rig.rows else {}
            seen.append((e2.cur_inst.type if e2.cur_inst else None, r.get("ch2.heater.pv"), r.get("ch2.heater.sv"),
                         r.get("ch2.heater.run")))
        rig.loop.tick_hooks.append(watch)
        assert web(rig, "PROCESS_START", {"ch": 2, "params": HEAT}).ok
        drive(rig, lambda: not e2.busy, 3000)
        assert ends(e2) == [("done", None)]
        types = [s[0] for s in seen]
        assert "heater_wait" in types and types.index("heater_wait") < types.index("base_wait") < types.index("main")
        wait_sv = [s[2] for s in seen if s[0] == "heater_wait" and s[2] is not None]
        assert wait_sv[0] < 40.0 and max(wait_sv) == pytest.approx(40.0) and wait_sv == sorted(wait_sv)   # 램프 승온
        main = [s for s in seen if s[0] == "main" and s[1] is not None]
        assert main and all(s[3] for s in main) and all(abs(s[1] - 40.0) < 3.0 for s in main), main[:3]   # 메인 동안 유지
        rig.loop.run(3)
        assert rig.rows[-1]["ch2.heater.run"] is False                                          # 공정 끝 — 히터 OFF
        logs = list(e2.log_lines)
        assert any("히터 요약(메인 공정 구간) — 목표 40.0°C" in x for x in logs)
        assert any("공정이 히터를 끕니다 — 공정 종료" in x for x in logs)
        st = rig.hub.state_data(rig.rows[-1])
        assert st["heater"] and st["heater"]["run"] is False
        assert all(r["ch2.heater.q"] == "ok" for r in list(rig.rows)[-50:])
        assert rig.internal_errors == 0
    finally:
        finish(rig)


# ───────────── 두께 종료 공정 ─────────────
THK = dict(base_pressure=1e-5, working_pressure=5, process_time=0, shutter_delay=0.05, thickness=3.0, dep_rate=0.05,
           use_ar=True, ar_flow=20, use_dc_pulse=True, dc_pulse_power=80, dc_pulse_freq=20, use_ms=True, use_g1=True,
           tooling=100, density=2.0, z_factor=1.0, process_note="thk")


def test_thickness_end_process(tmp_path):
    rig = full(tmp_path)
    try:
        a = rig.asm
        e2 = a.proc["ch2"]
        th = a.thickness["ch2"]
        etas, phases = [], []

        def watch(t):
            etas.append(e2.eta_remaining_s())
            if rig.rows:
                phases.append((rig.rows[-1].get("ch2.thk.phase"), rig.rows[-1].get("ch2.proc.block_type")))
        rig.loop.tick_hooks.append(watch)
        assert web(rig, "PROCESS_START", {"ch": 2, "params": THK}).ok
        drive(rig, lambda: not e2.busy, 1500)
        assert ends(e2) == [("done", None)]
        acts = [x[2] for x in e2.traces[-1] if not x[4]]
        assert acts.index("THICKNESS_ZERO") < acts.index("PLC_CMD", acts.index("THICKNESS_ZERO")) < acts.index("THICKNESS_WAIT")
        logs = list(e2.log_lines)
        assert any("완료 — 종료 방식 Thickness" in x for x in logs) and any("[ETA] Thickness 모드" in x for x in logs)
        assert all(v is None for v in etas)                                     # 두께 모드 — ETA 확정 안 함(null)
        assert any(p[1] == "thickness_main" for p in phases)
        summ = [x for x in logs if "두께 요약 — 종료 방식 Thickness" in x]
        assert summ, logs[-20:]
        nm = float(summ[0].split("최종 ")[1].split(" nm")[0])
        assert 3.0 <= nm < 3.5, summ                                            # 장비 두께가 목표(3 nm)에 닿아 끝
        assert th.snapshot_fields().get("phase") in ("idle", None, "done")
        plc = rig.sims["plc"]
        assert not plc.coil("MAIN_SHUTTER_2_SW") and not rig.sims["ch2.dcpulse"].out
        assert e2.stats.thickness_record and rig.internal_errors == 0
    finally:
        finish(rig)


# ───────────── 공정 중 수정·건너뛰기·일시정지·시퀀스 편집(웹·로컬 경로) ─────────────
FC = dict(base_pressure=1e-5, working_pressure=3, process_time=1.0, shutter_delay=0.2, use_ar=True, ar_flow=20,
          use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True, process_note="fc")


def _iso(sec):
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=sec)).isoformat().replace("+00:00", "Z")


def _wcmd(cid, kind, args, by="op@web"):
    return {"id": cid, "kind": kind, "args": args, "requestedBy": by, "requestedAt": _iso(0), "expiresAt": _iso(120)}


def _post(port, obj):
    cn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        cn.request("POST", "/api/v1/command", body=json.dumps(obj), headers={"Content-Type": "application/json"})
        rr = cn.getresponse()
        return rr.status, json.loads(rr.read())
    finally:
        cn.close()


class WebPaths:
    """가짜 웹 + uplink(SSE 명령) + 로컬 서버(POST /api/v1/command) — 명령은 실제 길로, tick 은 시험이 돌린다(명령 결과가 올
    때까지 가상 시간은 멈춰 있다)."""

    def __init__(self, rig, tmp):
        from v2.ch12.local_server import LocalServer
        from v2.ch12.uplink import Uplink
        self.rig = rig
        self.web = FakeWeb(keepalive_s=0.5).up()
        self.up = Uplink(rig, self.web.url, self.web.token, Path(tmp) / "uplink")
        rig.uplink = self.up
        self.up.start()
        self.srv = LocalServer(0, str(Path(tmp) / "hmi"))
        self.srv.bind()
        self.srv.attach(rig)
        self.srv.start()
        assert self.web.wait(lambda: self.web.sse_connects >= 1, 10)
        self.n = 0
        self.web_ids = []

    def web_cmd(self, kind, args, by="op@web", cid=None):
        self.n += 1
        cid = cid or f"web-{self.n}"
        self.web_ids.append(cid)
        mine = lambda: [m["data"] for m in hub_msgs(self.rig, "cmd_result") if m["data"]["cmdId"] == cid]   # noqa: E731
        n0 = len(mine())
        assert self.web.push(_wcmd(cid, kind, args, by)) >= 1
        assert self.web.wait(lambda: len(mine()) > n0, 10), f"웹 명령 {cid} 결과 없음"
        return mine()[-1]

    def local_cmd(self, kind, args):
        self.n += 1
        code, out = _post(self.srv.port, {"id": f"loc-{self.n}", "kind": kind, "args": args})
        assert code == 200 and out["id"] == f"loc-{self.n}"
        return out

    def close(self):
        self.up.stop()
        self.srv.stop()
        self.web.down()


def test_adjust_skip_pause_edit(tmp_path):
    rig = full(tmp_path / "rig")
    wp = WebPaths(rig, tmp_path)
    try:
        e = rig.asm.proc["ch1"]
        got = {}
        assert wp.web_cmd("PROCESS_START", {"ch": 1, "params": FC})["status"] == "accepted"
        drive(rig, blk(e, "gas_intro"), 300)
        got["skip_gas"] = wp.web_cmd("BLOCK_SKIP", {"ch": 1})                     # 건너뛸 수 없는 블록 — 거부
        got["pause_gas"] = wp.local_cmd("PROCESS_PAUSE", {"ch": 1})
        drive(rig, blk(e, "wait"), 300)
        rem0 = e.runner.remaining_s()
        got["pause"] = wp.web_cmd("PROCESS_PAUSE", {"ch": 1}, by="op1@web")
        rig.loop.run(10)
        rem1 = e.runner.remaining_s()
        got["resume"] = wp.local_cmd("PROCESS_RESUME", {"ch": 1})
        main_at = next(i for i, b in enumerate(e.blocks) if b["type"] == "main")
        got["edit_started"] = wp.local_cmd("SEQUENCE_EDIT", {"ch": 1, "op": {"op": "delete", "at": 0}})
        got["edit_ins"] = wp.web_cmd("SEQUENCE_EDIT", {"ch": 1, "op": {"op": "insert", "at": main_at, "block": {
            "type": "wait", "params": {"sec": 4}, "label": "추가 대기"}}}, by="op2@web")
        got["sched"] = wp.web_cmd("PROCESS_ADJUST", {"ch": 1, "device": "process", "item": "main_remaining", "value": 8})
        got["skip_wait"] = wp.local_cmd("BLOCK_SKIP", {"ch": 1})                  # 지금 대기(Power Delay) 건너뛰기
        drive(rig, lambda: blk(e, "main")() and e.runner.cd is not None, 300)
        got["live"] = wp.web_cmd("PROCESS_ADJUST", {"ch": 1, "device": "mfc", "item": "ar_flow", "value": 25.0})
        drive(rig, lambda: not e.busy, 600)
        # 결과
        assert got["skip_gas"]["status"] == "rejected" and got["skip_gas"]["reasonCode"] == "SKIP_NOT_ALLOWED"
        assert got["pause_gas"]["ok"] is False and got["pause_gas"]["reason"] == "PAUSE_NOT_ALLOWED"
        assert got["pause"]["status"] == "accepted" and got["pause"]["by"] == "op1@web" and got["resume"]["ok"] is True
        assert rem0 is not None and rem1 is not None and abs(rem1 - rem0) <= 1.5, (rem0, rem1)   # 일시정지 = 타이머 정지
        assert got["edit_started"]["ok"] is False and got["edit_started"]["reason"] == "EDIT_STARTED_BLOCK"
        assert got["edit_ins"]["status"] == "accepted" and got["sched"]["status"] == "accepted"
        assert got["skip_wait"]["ok"] is True and got["live"]["status"] == "accepted"
        assert ends(e) == [("done", None)]
        end = [x for x in e.events if x.kind == "run_end"][-1]
        ex = [(r["type"], r["label"], r["result"]) for r in end.data["executed"]]
        assert ("wait", "Power Delay 20초", "skipped") in ex and any(x[1] == "추가 대기" and x[2] == "done" for x in ex)
        rec = end.data["recipe"]
        assert rec["rev"] == 1 and len(rec["blocks"]) == len(rec["original"]) + 1
        adj = [(x.data["device"] + "." + x.data["item"], x.data["result"]) for x in e.events if x.kind == "adjust"]
        assert ("process.main_remaining", "scheduled") in adj and ("mfc.ar_flow", "confirmed") in adj
        assert "Q1 25.0" in rig.sims["ch1.mfc"].set_commands_seen()
        tr = [x for x in e.traces[-1] if not x[4]]
        i = next(k for k, x in enumerate(tr) if x[3].startswith("메인 공정 진행"))
        assert 7.5 < tr[i + 1][0] - tr[i][0] < 9.5                              # 60 s 메인이 예약한 8 s 로
        assert [x.data["on"] for x in e.events if x.kind == "pause"] == [True, False]
        assert any(x[2] == "RF_PULSE_STOP" for x in e.traces[-1])               # 어떤 편집에도 끝 정지(파워 끄기)는 그대로
        assert wp.web.wait(lambda: {m["data"]["cmdId"] for m in wp.web.messages("cmd_result")} >= set(wp.web_ids), 10)
        srcs = {(m["data"]["source"], m["data"]["by"]) for m in hub_msgs(rig, "cmd_result")}
        assert ("hmi", "local") in srcs and ("web", "op2@web") in srcs
    finally:
        wp.close()
        finish(rig)


# ───────────── 재시작 뒤 이어가기 + 안전 정지 ─────────────
P_CH1 = dict(base_pressure=1e-5, working_pressure=3, process_time=1.0, shutter_delay=0.2, use_ar=True, ar_flow=20,
             use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True, process_note="rs1")
P_CH2 = dict(P2, process_time=0.5, process_note="rs2")


def test_restart_resume_and_safe_stop(tmp_path):
    rig = RR.boot(tmp_path)
    rig2 = None
    try:
        RR.drive(rig, lambda: rig.resumer.done, 30)
        e1, e2 = rig.eng["ch1"], rig.eng["ch2"]
        assert rig.command("PROCESS_START", {"ch": 1, "params": P_CH1}, source=Source.WEB).ok
        RR.drive(rig, lambda: e1.cur_inst is not None and e1.cur_inst.type == "main" and e1.runner.cd is not None, 400)
        assert rig.command("PROCESS_START", {"ch": 2, "params": P_CH2}, source=Source.HMI).ok
        box = {}

        def on_ev(lg):                                       # CH2 '파워 켜기' 블록 시작 순간(러너 스레드)에 죽인다
            ev = getattr(lg, "event", None)
            if ("t" not in box and getattr(ev, "kind", None) == "block" and ev.data.get("phase") == "start"
                    and ev.data.get("type") == "power_on" and lg.source == "engine:ch2"):
                box["t"] = RR.kill(rig)
        rig.event_listeners.append(on_ev)
        RR.drive(rig, lambda: "t" in box, 400)
        assert "t" in box and e1.busy
        j1 = json.loads((tmp_path / "data" / "journal" / "ch1.json").read_text(encoding="utf-8"))
        j2 = json.loads((tmp_path / "data" / "journal" / "ch2.json").read_text(encoding="utf-8"))
        assert j1["active"] and j2["active"] and j1["state"]["block_type"] == "main"
        rid1, rid2 = j1["run_id"], j2["run_id"]
        # 새 조립(같은 시뮬레이터 — 장비는 켜진 채) 4 s 뒤
        rig2 = RR.reboot(tmp_path, rig, 4.0)
        f1, f2 = rig2.eng["ch1"], rig2.eng["ch2"]
        RR.drive(rig2, lambda: rig2.resumer.done and not f1.busy and not f2.busy, 1500)
        d1, d2 = rig2.resumer.decisions["ch1"], rig2.resumer.decisions["ch2"]
        assert d1["result"] == "resumed" and f1.run_id == rid1 and d1["gap_s"] >= 4.0
        assert RR.ends(f1) == [("done", None)] and f1.last_result["actual_process_time_min"] == pytest.approx(1.0, abs=0.02)
        r = RR.resumed(f1)
        assert len(r) == 1 and r[0]["block"]["type"] == "main" and "복구됨(공백" in r[0]["detail"]
        assert d2["result"] == "stopped" and {c[0] for c in d2["checks"] if not c[1]} == {"resumable"}, d2["checks"]
        assert RR.ends(f2) == [("error", "PROGRAM_RESTART")] and f2.run_id == rid2 and not RR.resumed(f2)
        v = rig2.journal.last_view
        assert v.last("plc.main_shutter_2_sw") is False and v.last("plc.main_2_gas_sw") is False
        assert not rig2.sims["ch2.dcpulse"].out and not rig2.sims["ch1.rfpulse"].out
        ev = [m["data"] for m in rig2.hub.recent if m["type"] == "run_end"]
        assert {(d["chamber"], d["result"], d["reasonCode"]) for d in ev} >= {("ch1", "done", None),
                                                                              ("ch2", "error", "PROGRAM_RESTART")}
        f1.writer.flush(10)
        txts = sorted((tmp_path / "logs" / "CH1").glob("CH1_rs1_*.txt"))
        assert len(txts) == 1 and "[RESTART] 복구됨(공백" in txts[0].read_text(encoding="utf-8")
    finally:
        if rig2 is not None:
            RR.finish(rig2)
        else:
            RR.cleanup(rig)


# ───────────── 웹 끊김·재연결 ─────────────
def test_web_disconnect_reconnect(tmp_path):
    rig = full(tmp_path / "rig", cfg={"UPLINK_INTERVAL_MS": 300, "UPLINK_TIMEOUT_SEC": 2.0, "UPLINK_DOWN_SEC": 5})
    wp = WebPaths(rig, tmp_path)
    try:
        e = rig.asm.proc["ch1"]
        calls = collections.Counter()
        real = rig.command

        def counting(kind, args, **kw):
            calls[kind] += 1
            return real(kind, args, **kw)
        rig.command = counting
        start = wp.web_cmd("PROCESS_START", {"ch": 1, "params": dict(FC, process_time=0.5)}, cid="w-start")
        assert start["status"] == "accepted"
        drive(rig, lambda: e.cur_inst is not None and e.cur_inst.type == "gas_intro", 200)
        assert wp.web.wait(lambda: wp.up.status()["connected"] and wp.up.spool.count == 0, 10)
        mark = rig.hub.seq
        wp.web.down()
        t0 = time.monotonic()
        blocks = set()
        while time.monotonic() - t0 < 6.5:                    # 웹이 끊긴 동안 — 공정은 계속, 로컬 명령도 된다
            rig.loop.run(1)
            blocks.add(e.cur_inst.type if e.cur_inst else None)
            time.sleep(0.05)
        r = wp.local_cmd("PROCESS_ADJUST", {"ch": 1, "device": "process", "item": "main_remaining", "value": 10})
        assert r["ok"] is True
        st = wp.up.status()
        assert not st["connected"] and st["queued"] > 0 and len(blocks) >= 2, (st, blocks)
        assert [x for x in hub_msgs(rig, "event") if x["data"]["code"] == "UPLINK_DOWN"]
        drive(rig, lambda: e.cur_inst is not None and e.cur_inst.type == "main", 300)
        while_down = [m["id"] for m in rig.msgs if m["seq"] > mark and m["type"] not in ("state", "hello")]
        assert len(while_down) >= 10
        wp.web.up()
        assert wp.web.wait(lambda: set(while_down) <= {m["id"] for m in wp.web.messages()}, 20), "끊긴 동안 낸 메시지가 다 오지 않음"
        assert wp.web.wait(lambda: any(m["data"]["code"] == "UPLINK_UP" for m in wp.web.messages("event")), 10)
        assert wp.web.wait(lambda: wp.web.sse_connects >= 2, 15)
        again = wp.web_cmd("PROCESS_START", {"ch": 1, "params": FC}, cid="w-start")   # 같은 명령 id 다시 — 실행 안 함
        assert again == start and wp.up.dup_cmds == 1 and calls["PROCESS_START"] == 1
        drive(rig, lambda: not e.busy, 600)
        assert ends(e) == [("done", None)]
        assert wp.web.wait(lambda: any(m["data"].get("runId") == e.run_id for m in wp.web.messages("run_end")), 10)
        assert wp.web.wait(lambda: wp.up.spool.count == 0, 10)
        ids = [m["id"] for m in wp.web.messages()]
        assert len(ids) == len(set(ids))                     # 웹은 같은 메시지를 한 번만 저장(다시 보낸 것은 dups)
        res = [m["data"] for m in wp.web.messages("cmd_result") if m["data"]["cmdId"] == "w-start"]
        assert len(res) == 2 and res[0] == res[1] == start                  # 같은 결과를 다시 보냄(실행은 한 번)
    finally:
        wp.close()
        finish(rig)


# ───────────── 가상 시계 7일 연속 운전 ─────────────
DAYS = 7
DAY7_GROWTH_MB = 4.0                   # 6~7 일 사이 전용 메모리 증가 상한(하루 공정 2·PC·TSP·프리스퍼터를 돌린 뒤 — 시간마다 gc)
SCHED = {"22:00:00": "process", "23:30:00": "pc"}      # 매일(웹 명령) — TSP 03:00·프리스퍼터 05:00 은 엔진 예약이 스스로
PRE_CSV = "#,Process_name,Ar,Ar_flow,base_pressure,working_pressure,shutter_delay,process_time,main_shutter,gun1,use_dc_pulse," \
          "dc_pulse_power,dc_pulse_freq\n1,PRE-CH2,T,20,1.00E-05,5,0.1,0.2,T,T,T,80,20\n"


def soak_full(tmp_path, hours: float) -> dict:
    """가상 hours 시간 — 실행 프로그램과 같은 조립·같은 기록 크기(APP_RETENTION). 매시 gc 뒤 표 한 줄(24 시간마다 날 표)."""
    from v2.ch12.sim import soak
    from v2.ch12.system import APP_RETENTION
    c = VirtualClock(start_wall=soak.START_WALL)            # 로컬 19:00 — 5 시간 뒤 자정(일별 파일 바뀜)
    c.register()
    rig = Ch12FullRig(c, tmp_path, cfg=dict(CFG, CAMERA_MIN_FREE_GB=0.0), retention=APP_RETENTION, robot_api=False,
                      schedules=True)
    rig.sims["ch1.mfc"].sp[2] = 0.3
    rig.sims["ch2.mfc"].sp[3] = 0.5
    a = rig.asm
    pre = tmp_path / "pre_ch2.csv"
    pre.write_text(PRE_CSV, encoding="utf-8")
    days, per_hour = [], []
    counts = collections.Counter()
    fired = set()
    w0 = time.perf_counter()

    def on_ev(lg):
        if getattr(lg, "kind", "") != "run_end":
            return
        d = getattr(getattr(lg, "event", None), "data", None) or {}
        src = str(lg.source)
        if src == "pc":
            counts[f"pc:{d.get('result')}"] += 1
        elif src == "engine:tsp":
            counts[f"tsp:{d.get('result')}"] += 1
        elif src in ("engine:ch1", "engine:ch2"):
            ch = src[7:]
            counts[f"{ch}:{d.get('result')}:{a.proc[ch].origin}"] += 1
    rig.event_listeners.append(on_ev)

    def hook(t):
        now = dt.datetime.fromtimestamp(c.wall())
        what = SCHED.get(now.strftime("%H:%M:%S"))
        if what and (now.date(), what) not in fired:
            fired.add((now.date(), what))

            def go(what=what):                               # 명령은 초의 뒷부분에(사람·웹 명령처럼 — PLC 1초 읽기 구간 밖)
                c.wait_until(c.monotonic() + PACE_AT)
                if what == "process":
                    for ch, p in ((1, P1), (2, P2)):
                        r = rig.command("PROCESS_START", {"ch": ch, "params": p}, source=Source.WEB, by="soak")
                        counts[f"start{ch}:{r.ok}"] += 1
                else:
                    r = rig.command("PC_START", {"ch": 1, "params": PC_UI}, source=Source.WEB, by="soak")
                    counts[f"pcstart:{r.ok}"] += 1
            c.start_thread(go, f"soak-{what}")
        if t.seq % 3600 == 0:
            gc.collect()
            m = soak.mem()
            row = {"h": t.seq // 3600, **m, "threads": threading.active_count(),
                   "rec_pending": rig.recorder._q.qsize(), "rec_dropped": rig.recorder.dropped,
                   "out_pending": rig.outputs.pending, "events": len(rig.events), "hub_recent": len(rig.hub.recent),
                   "eng_events": sum(len(e.events) for e in (a.proc["ch1"], a.proc["ch2"], a.pc, a.tsp)),
                   "eng_logs": sum(len(e.log_lines) for e in a.proc.values()) + len(a.pc.ui_log),
                   "ps_lines": sum(len(p.lines) for p in a.presputter.values()),
                   "sim_log_max": max(len(getattr(x, k)) for x in rig.sims.values()
                                      for k in ("received", "frames", "cmds", "rx_log") if hasattr(x, k)),
                   "missed": rig.loop.missed, "wall_s": round(time.perf_counter() - w0, 1), "done": dict(counts)}
            per_hour.append(row)
            if t.seq % 86400 == 0:
                days.append(dict(row, day=t.seq // 86400))
    rig.loop.tick_hooks.append(hook)
    assert rig.start()
    try:
        rig.loop.run(2)
        assert rig.command("TSP_SCHEDULE", {"on": True, "time": "03:00"}, source=Source.WEB).ok
        assert rig.command("PRESPUTTER_RECIPE", {"ch": 2, "path": str(pre)}, source=Source.HMI).ok
        assert rig.command("PRESPUTTER_RESERVE", {"ch": 2, "time": "05:00"}, source=Source.HMI).ok
        rig.loop.run(int(hours * 3600) - 1)
    finally:
        rig.stop()
        c.close()
    return {"days": days, "hours": per_hour, "counts": counts, "rig": rig, "wall_s": time.perf_counter() - w0}


@pytest.mark.slow
def test_virtual_7_days(tmp_path):
    from v2.ch12.system import APP_RETENTION
    res = soak_full(tmp_path, DAYS * 24)
    table, hours, counts, rig, wall = res["days"], res["hours"], res["counts"], res["rig"], res["wall_s"]
    path = os.environ.get("CH12_7DAY_TABLE")
    if path:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"days": table, "hours": hours, "wall_s": wall, "counts": dict(counts)}, f, ensure_ascii=False,
                      indent=1, default=str)
    print(f"\n[virtual_7_days] 실제 {wall:.0f} s · 행 {rig.loop.rows} · 놓친 tick {rig.loop.missed}")
    for r in table:
        print(f"  day {r['day']}: 전용 {r['private_mb']} MB · 작업 집합 {r['ws_mb']} MB · 스레드 {r['threads']} · 기록 큐 "
              f"{r['rec_pending']} · 출력 큐 {r['out_pending']} · 이벤트 {r['events']} · 허브 {r['hub_recent']} · 엔진 이벤트 "
              f"{r['eng_events']} · 실행 {r['done']}")
    by = {r["day"]: r for r in table}
    assert sorted(by) == list(range(DAYS + 1)) and rig.loop.missed == 0
    for r in hours:
        assert r["rec_pending"] < 100 and r["rec_dropped"] == 0 and r["out_pending"] < 100, r
        assert r["events"] <= APP_RETENTION.events and r["sim_log_max"] <= APP_RETENTION.sim_log, r
    for d in range(2, DAYS + 1):
        assert by[d]["threads"] == by[1]["threads"], table                        # 스레드가 늘지 않음
    for k in ("hub_recent", "eng_events", "eng_logs", "ps_lines"):
        assert by[DAYS][k] <= max(by[d][k] for d in range(1, DAYS)) * 1.05 + 50, (k, table)   # 링 버퍼 상한 안(다 찬 뒤 그대로)
    assert by[DAYS]["private_mb"] - by[DAYS - 1]["private_mb"] < DAY7_GROWTH_MB, table
    assert counts["start1:True"] == DAYS and counts["start2:True"] == DAYS and counts["pcstart:True"] == DAYS, counts
    assert counts["ch1:done:ui"] == DAYS and counts["ch2:done:ui"] == DAYS, counts                  # 매일 공정 2
    assert counts["ch2:done:presputter"] == DAYS and counts["pc:done"] == DAYS, counts              # 프리스퍼터·PC 매일
    assert counts["tsp:done"] == DAYS, counts                                                       # TSP 매일 예약
    assert list(rig.recorder.errors) == [] and list(rig.outputs.writer.errors) == [] and rig.internal_errors == 0

"""재시작 뒤 이어가기/안전 정지(v2/ch12/resume.py·journal.py·process_view 재시작 부분 — 사용자 지시 3장 (6) 합의 규칙).

'제어 프로그램을 죽였다 살리기'(restart_rig): 실제 조립(Ch12System + assembly.build — 시뮬레이터·실제 드라이버·가상 시계)에서
공정 중 아무 때나 시계를 닫아 모든 스레드를 그 자리에서 끝내고(장비 시뮬레이터 상태는 그대로, 저널만 남김), 같은 시뮬레이터에
새 조립을 붙여 공백 뒤 시작한다.
- 블록 종류 × 임의 시각: 상태 유지 블록(정의 resumable ∧ holding 스텝)이면 이어가 끝까지(done), 아니면 PROGRAM_RESTART 안전 정지.
- 조건 하나씩: ① 공백 > RESUME_MAX_GAP_S·재부팅 ② 두 번째 재시작 ③ 장비 상태가 저널과 다름(출력 꺼짐·셔터 닫힘)
  ④ 상태 유지 블록 아님 ⑤ RESUME_ENABLED 꺼짐 — 각각 안전 정지. 설정 다시 쓰기 되읽기 실패도 안전 정지.
- 이어갈 때: 경과에 공백 포함(남은 메인 시간), 설정 다시 쓰기 확인 로그, 'resumed' 이벤트 모양(포털 계약), 공정 txt 한 파일.
- 안전 정지 때: run_end error PROGRAM_RESTART 하나(포털 run_end reasonCode), '공정 실패 이유' chat 없음, 출력 꺼짐.
- 명령 중복 실행 없음(웹 uplink 명령 id·로봇 내부 명령 API id 가 재시작 뒤 다시 와도).
- 반복 크래시 제한·실제 하위 프로세스: 감시 프로그램(v2.supervisor)이 v2.ch12 를 켜고, 공정 중 SIGKILL → 다시 켜서 저널로 이어감 →
  SIGTERM(의도적 종료) → 종료 코드 0(다시 켜지 않음). 10분에 3번 넘게 죽으면 포기.
"""
import json
import random
import signal
import sys
import threading
import time
from pathlib import Path

import pytest

from v2.ch12 import blocks as BK
from v2.ch12 import journal as J
from v2.ch12.drivers import plc_map as PM
from v2.tests import restart_rig as RR

ROOT = Path(__file__).resolve().parents[2]
P_CH1 = dict(base_pressure=1e-5, working_pressure=3, process_time=1.0, shutter_delay=0.2, use_ar=True, use_o2=True,
             ar_flow=20, o2_flow=1, use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True)
P_CH2 = dict(base_pressure=7e-6, working_pressure=5, process_time=0.5, shutter_delay=0.2, use_ar=True, ar_flow=20,
             use_dc_power=True, dc_power=100, use_rf_pulse=True, rf_pulse_power=100, rf_pulse_freq=1.0, rf_pulse_duty=50,
             use_power_select=True, use_ms=True, use_g1=True, use_heater=True, heater_temp=27, heater_soak=0.2)
P_THK = dict(base_pressure=1e-5, working_pressure=5, process_time=0, shutter_delay=0.05, thickness=3.0, dep_rate=0.05,
             use_ar=True, ar_flow=20, use_dc_pulse=True, dc_pulse_power=80, dc_pulse_freq=20, use_ms=True, use_g1=True,
             tooling=100, density=2.0, z_factor=1.0)
P_TEST = dict(test_mode=True, test_duration_sec=30)
B_CH1 = [{"type": "gas_intro", "params": {"ar": True, "ar_flow": 20}},
         {"type": "pressure_ctrl", "params": {"target": 3, "timeout_sec": 60}},
         {"type": "power_on", "params": {"rfp_power": 100, "rfp_freq_khz": 1.0, "rfp_duty": 50}},
         {"type": "pulse_set", "params": {"kind": "rf", "power": 110}},
         {"type": "row_delay", "params": {"sec": 12, "label": "안정화", "name": "d1"}},
         {"type": "end_stop"}]
SCEN = {"ch1": ("ch1", P_CH1, None), "ch2": ("ch2", P_CH2, None), "thk": ("ch2", P_THK, None),
        "test": ("ch2", P_TEST, None), "blocks": ("ch1", None, B_CH1)}
INSTANT = {"heater_set", "base_wait", "rga_scan", "gas_prep", "gas_intro", "shutter", "power_select", "power_on",
           "pressure_set", "pulse_set", "thickness_zero", "end_stop", "heater_hold"}   # 짧은 블록 — 블록 시작 순간에 죽임
MATRIX = [("ch1", t) for t in ("base_wait", "rga_scan", "gas_prep", "gas_intro", "pressure_ctrl", "power_on", "wait",
                               "pressure_boost", "pressure_set", "shutter", "main", "end_stop")] + \
         [("ch2", t) for t in ("heater_set", "heater_wait", "power_select", "power_on", "heater_hold", "main")] + \
         [("thk", t) for t in ("thickness_zero", "thickness_main")] + [("test", "test")] + \
         [("blocks", t) for t in ("pulse_set", "row_delay")]
FAST = {("ch1", "main"), ("ch1", "gas_intro"), ("ch1", "wait"), ("ch1", "pressure_ctrl"), ("blocks", "row_delay"),
        ("test", "test"), ("ch1", "end_stop")}


# ───────────── 도우미 ─────────────
def start_run(tmp, scen: str, **kw):
    ch, p, blocks = SCEN[scen]
    rig = RR.boot(tmp, **kw)
    e = rig.eng[ch]
    RR.drive(rig, lambda: rig.resumer.done, 30)
    if blocks is not None:
        assert e.load({"blocks": blocks, "name": scen}, source="local") is None, e.reject_detail
        assert e.start(owner="local", source="local") is None
    else:
        assert e.start(params=dict(p, process_note=scen), owner="local", source="local") is None
    return rig, e


def kill_at_block(rig, e, btype: str, after_s: float = 0.0, max_ticks: int = 1500) -> dict:
    """그 블록에 들어가면(after_s 뒤 tick, 0 이면 블록 시작 순간 — 러너 스레드) 저널을 쓰고 죽인다."""
    box: dict = {}

    def kill():
        if "t" not in box:
            box["t"] = RR.kill(rig)

    if after_s <= 0:
        def on_ev(lg):
            ev = getattr(lg, "event", None)
            if getattr(ev, "kind", None) == "block" and ev.data.get("phase") == "start" and ev.data.get("type") == btype \
                    and lg.source == f"engine:{e.ch}":
                kill()
        rig.event_listeners.append(on_ev)
        RR.drive(rig, lambda: "t" in box or not e.busy, max_ticks)
    else:
        RR.drive(rig, lambda: "t" in box or not e.busy, max_ticks, hooks=[RR.block_hook(e, btype, kill, after_s)])
    assert "t" in box, f"{btype} 블록에서 죽이지 못함(끝남: {RR.ends(e)})"
    return box


def journal_of(tmp, part="ch1") -> dict:
    return json.loads((Path(tmp) / "data" / "journal" / f"{part}.json").read_text(encoding="utf-8"))


def revive(tmp, old, gap_s: float = 3.0, ch: str = "ch1", max_ticks: int = 1500, to_end: bool = True, **kw):
    """다시 켜서 판단까지(to_end 면 그 챔버 공정이 끝날 때까지)."""
    rig = RR.reboot(tmp, old, gap_s, **kw)
    e = rig.eng[ch]
    RR.drive(rig, lambda: rig.resumer.done and (not to_end or not e.busy), max_ticks)
    assert rig.resumer.done and (not to_end or not e.busy)
    return rig, e, rig.resumer.decisions.get(ch)


def failed(dec) -> set:
    return {c[0] for c in dec["checks"] if not c[1]}


def no_fail_chat(e) -> bool:
    return not any(x.kind == "chat" and "공정 실패 이유" in x.detail for x in e.events)


def assert_stopped(rig, e, dec, why: set = None):
    assert dec["result"] == "stopped" and not dec["resume"]
    if why is not None:
        assert why <= failed(dec), dec["checks"]
    assert RR.ends(e) == [("error", "PROGRAM_RESTART")] and not RR.resumed(e) and no_fail_chat(e)
    v = rig.journal.last_view
    n = e.n
    assert v.last(f"plc.main_shutter_{n}_sw") is False and v.last(f"plc.main_{n}_gas_sw") is False
    assert not rig.sims[f"{e.ch}.rfpulse"].out and not rig.sims[f"{e.ch}.dcpulse"].out
    if n == 2:
        assert not v.last("plc.dcv_set_0") and not v.last("plc.dcv_write_0")


def hub_types(rig, kind=None):
    return [m for m in rig.hub.recent if (kind is None or m["type"] == kind)] if hasattr(rig.hub, "recent") else []


# ───────────── 블록 종류 × 임의 시각 ─────────────
@pytest.mark.parametrize("scen,btype", [x if x in FAST else pytest.param(*x, marks=pytest.mark.slow) for x in MATRIX])
def test_every_block_kind_random_time(tmp_path, scen, btype):
    rng = random.Random(f"{scen}:{btype}")
    rig, e = start_run(tmp_path, scen)
    after = 0.0 if btype in INSTANT else round(rng.uniform(0.5, 4.0), 1)
    kill_at_block(rig, e, btype, after)
    st = journal_of(tmp_path, e.ch)["state"]
    d = BK.by_type()[btype]
    want = bool(d.resumable and st["step_holding"])
    assert st["block_type"] == btype and st["resumable"] == want, st
    if d.resumable and btype not in INSTANT:
        assert want, "상태 유지 블록을 holding 스텝에서 죽였다"
    rig2, e2, dec = revive(tmp_path, rig, 3.0, ch=e.ch)
    try:
        if want:
            assert dec["result"] == "resumed", dec["checks"]
            assert RR.ends(e2) == [("done", None)] and len(RR.resumed(e2)) == 1
        else:
            assert_stopped(rig2, e2, dec, {"resumable"})
    finally:
        RR.finish(rig2)


# ───────────── 이어가기(경과·다시 쓰기·이벤트·기록) ─────────────
def test_resume_main_counts_gap_rewrites_and_reports(tmp_path):
    rig, e = start_run(tmp_path, "ch1")
    kill_at_block(rig, e, "main", 20.0)
    st = journal_of(tmp_path)["state"]
    rid, el = st["run_id"], st["countdown"]["elapsed_s"]
    rig2, e2, dec = revive(tmp_path, rig, 5.0, ch="ch1", to_end=False)
    try:
        RR.drive(rig2, lambda: e2.runner.cd is not None, 60)
        assert dec["result"] == "resumed" and e2.run_id == rid
        gap = dec["gap_s"]
        assert gap >= 5.0
        left = e2.runner.remaining_s()
        assert left == pytest.approx(60.0 - el - gap, abs=2.5)                 # 꺼져 있던 시간도 경과
        assert journal_of(tmp_path)["restarts"] == 1 if rig2.journal.flush() else True
        logs = list(e2.log_lines)
        for name in ("MFC ch1 FLOW_SET 20", "MFC ch2 FLOW_SET 1", "MFC SP1_SET 3", "RF Pulse 120 W"):
            assert any(f"[RESTART] 설정 다시 쓰기 확인: {name}" in x for x in logs), name
        r = RR.resumed(e2)
        assert len(r) == 1 and r[0]["code"] == "RESUMED" and r[0]["run_id"] == rid
        assert r[0]["gap_s"] == pytest.approx(gap, abs=1.5) and r[0]["name"] == "ch1" and r[0]["block"]["type"] == "main"
        assert all(len(c) == 3 and c[1] for c in r[0]["checks"])
        assert "복구됨(공백" in r[0]["detail"]
        RR.drive(rig2, lambda: not e2.busy, 200)
        assert RR.ends(e2) == [("done", None)] and e2.last_result["actual_process_time_min"] == pytest.approx(1.0, abs=0.02)
        e2.writer.flush(10)
        txts = sorted((tmp_path / "logs" / "CH1").glob("CH1_ch1_*.txt"))
        assert len(txts) == 1                                                    # 같은 공정 txt 에 이어 씀
        body = txts[0].read_text(encoding="utf-8")
        assert "[RESTART] 복구됨(공백" in body and body.rstrip().endswith("# ==== END ====")
        ev = [m for m in rig2.hub.recent if m["type"] == "event" and m["data"]["context"].get("kind") == "resumed"]
        assert ev and ev[0]["data"]["context"]["source"] == "process" and ev[0]["data"]["context"]["chamber"] == "ch1"
        assert ev[0]["data"]["context"]["data"]["run_id"] == rid
        ends = [m for m in rig2.hub.recent if m["type"] == "run_end"]
        assert ends and ends[-1]["data"]["runId"] == rid and ends[-1]["data"]["reasonCode"] is None
    finally:
        RR.finish(rig2)


def test_restart_pending_blocks_new_start(tmp_path):
    rig, e = start_run(tmp_path, "ch1")
    kill_at_block(rig, e, "main", 3.0)
    rig2 = RR.reboot(tmp_path, rig, 2.0, start=False)
    try:
        e2 = rig2.eng["ch1"]
        assert e2.busy and e2.start_guard() == "RESTART_PENDING"
        assert rig2.journal.held == {"ch1"} and journal_of(tmp_path)["restarts"] == 1   # 판단 전에 횟수부터 올림
    finally:
        RR.finish(rig2)


# ───────────── 조건 하나씩 — 아니면 안전 정지 ─────────────
def test_gap_too_long_stops(tmp_path):
    rig, e = start_run(tmp_path, "ch1")
    kill_at_block(rig, e, "main", 5.0)
    rig2, e2, dec = revive(tmp_path, rig, 40.0)
    try:
        assert_stopped(rig2, e2, dec, {"gap"})
        ends = [m for m in rig2.hub.recent if m["type"] == "run_end"]
        assert ends and ends[-1]["data"]["reasonCode"] == "PROGRAM_RESTART" and ends[-1]["data"]["result"] == "error"
        assert not [m for m in rig2.hub.recent if m["type"] == "event" and m["data"]["context"].get("kind") == "chat"]
    finally:
        RR.finish(rig2)


def test_reboot_always_stops(tmp_path):
    """재부팅(BSOD·정전) — 단조 시계가 0 부터 다시(벽시계는 이어짐) — 공백이 짧아도 늘 안전 정지."""
    rig, e = start_run(tmp_path, "ch1")
    kill_at_block(rig, e, "main", 5.0)
    w = rig.clock.wall()
    RR.cleanup(rig)
    rig2 = RR.boot(tmp_path, sims=rig.sims, chambers=rig.chambers, t0=0.0, wall0=w + 8.0)
    try:
        e2 = rig2.eng["ch1"]
        RR.drive(rig2, lambda: rig2.resumer.done and not e2.busy, 1500)
        dec = rig2.resumer.decisions["ch1"]
        assert_stopped(rig2, e2, dec, {"gap"})
        assert "재부팅" in dict((c[0], c[2]) for c in dec["checks"])["gap"]
    finally:
        RR.finish(rig2)


def test_second_restart_in_same_run_stops(tmp_path):
    rig, e = start_run(tmp_path, "ch1")
    kill_at_block(rig, e, "main", 5.0)
    rig2, e2, dec = revive(tmp_path, rig, 2.0, to_end=False)
    assert dec["result"] == "resumed"
    RR.drive(rig2, lambda: e2.runner.cd is not None, 30)
    RR.drive(rig2, lambda: False, 4)
    RR.kill(rig2)
    assert journal_of(tmp_path)["restarts"] == 1
    rig3, e3, dec3 = revive(tmp_path, rig2, 2.0)
    try:
        assert_stopped(rig3, e3, dec3, {"first_restart"})
    finally:
        RR.finish(rig3)


@pytest.mark.parametrize("what", ["rfpulse_off", "shutter_closed", "gas_flow_off"])
def test_device_state_mismatch_stops(tmp_path, what):
    rig, e = start_run(tmp_path, "ch1")
    kill_at_block(rig, e, "main", 5.0)
    if what == "rfpulse_off":
        rig.sims["ch1.rfpulse"].out = False                     # 꺼져 있는 동안 RF Pulse 출력이 꺼짐
        want = {"power_output.RF Pulse"}
    elif what == "shutter_closed":
        rig.sims["plc"].coils[PM.resolve("MAIN_SHUTTER_1_SW")] = False
        want = {"shutter"}
    else:
        rig.sims["ch1.mfc"].on = [False] * 4
        want = {"gas.ar"}
    rig2, e2, dec = revive(tmp_path, rig, 2.0)
    try:
        assert_stopped(rig2, e2, dec, want)
    finally:
        RR.finish(rig2)


def test_resume_disabled_stops(tmp_path):
    rig, e = start_run(tmp_path, "ch1")
    kill_at_block(rig, e, "main", 5.0)
    rig2, e2, dec = revive(tmp_path, rig, 2.0, cfg={"RESUME_ENABLED": False})
    try:
        assert_stopped(rig2, e2, dec, {"enabled"})
    finally:
        RR.finish(rig2)


def test_rewrite_readback_failure_stops(tmp_path):
    """설정 다시 쓰기 — RF Pulse 셋포인트 되읽기가 다르면 안전 정지."""
    rig, e = start_run(tmp_path, "ch1")
    kill_at_block(rig, e, "main", 5.0)
    rig.sims["ch1.rfpulse"].csr_once = {8: [4, 4, 4, 4]}         # RF Pulse 셋포인트(8) 를 장비가 거부(CSR ≠ 0)
    rig2, e2, dec = revive(tmp_path, rig, 2.0)
    try:
        assert dec["result"] == "resumed"                      # 판단은 통과 — 엔진이 다시 쓰다 실패
        assert RR.ends(e2) == [("error", "PROGRAM_RESTART")] and not RR.resumed(e2) and no_fail_chat(e2)
        assert any("[RESTART] 재시작" in x and "setpoint_rewrite" in x and "RF Pulse" in x for x in e2.log_lines)
        assert not rig2.sims["ch1.rfpulse"].out
    finally:
        RR.finish(rig2)


def test_intentional_exit_mark_prevents_resume(tmp_path):
    """저널에 의도적 종료 표시가 있으면(종료 도중 죽음 — 공정이 아직 '공정 중') 이어가지 않는다."""
    rig, e = start_run(tmp_path, "ch1")
    kill_at_block(rig, e, "main", 5.0)
    p = tmp_path / "data" / "journal" / "ch1.json"
    d = json.loads(p.read_text(encoding="utf-8"))
    d["exit"] = {"intentional": True, "why": "CTRL_CLOSE", "t_wall": d["saved_wall"]}
    J.atomic_write(p, J.dumps(d))
    rig2, e2, dec = revive(tmp_path, rig, 2.0)
    try:
        assert_stopped(rig2, e2, dec, {"resumable"})
    finally:
        RR.finish(rig2)


def test_corrupt_journal_starts_clean(tmp_path):
    rig, e = start_run(tmp_path, "ch1")
    kill_at_block(rig, e, "main", 5.0)
    (tmp_path / "data" / "journal" / "ch1.json").write_bytes(b'{"v":1,"saved_wall":')
    rig2 = RR.reboot(tmp_path, rig, 2.0)
    try:
        RR.drive(rig2, lambda: rig2.resumer.done, 60)
        assert rig2.resumer.done and "ch1" not in rig2.resumer.decisions and not rig2.eng["ch1"].busy
        assert (tmp_path / "data" / "journal" / "ch1.json.corrupt").exists()
        # 공정이 없는데 남은 파워(RF Pulse 는 포트를 잡지 않아 보지 않음 — PLC D/A 만)는 안전 규칙 S31 몫(겹치지 않게)
        assert rig2.resumer.residual is None
    finally:
        RR.finish(rig2)


def test_residual_dc_power_without_process_is_turned_off(tmp_path):
    """공정이 없을 때 켜졌는데 CH2 DC1 출력이 남아 있으면 파워만 끈다(4단계 safety S31 이 있으면 그쪽 — 한 번만)."""
    rig = RR.boot(tmp_path)
    RR.drive(rig, lambda: rig.resumer.done, 30)
    RR.kill(rig)
    plc = rig.sims["plc"]
    plc.coils[PM.resolve("DCV_SET_0")] = True
    plc.regs[PM.REG_MAP["DCV_WRITE_0"]] = 400
    rig2 = RR.reboot(tmp_path, rig, 2.0)
    try:
        RR.drive(rig2, lambda: rig2.resumer.done and not rig2.journal.last_view.last("plc.dcv_set_0"), 60)
        v = rig2.journal.last_view
        assert not v.last("plc.dcv_set_0") and not v.last("plc.dcv_write_0")
        assert rig2.resumer.residual is None and rig2.safety.residual          # S31 이 끔(이 모듈은 겹쳐 끄지 않음)
    finally:
        RR.finish(rig2)


# ───────────── 명령 중복 실행 없음 ─────────────
def test_commands_not_repeated_after_restart(tmp_path):
    import urllib.request
    from v2.ch12.uplink import Uplink
    rig = RR.boot(tmp_path, robot_api=True)
    RR.drive(rig, lambda: rig.resumer.done, 30)
    calls = []
    real = rig.command

    def counting(kind, args, **kw):
        calls.append(kind)
        return real(kind, args, **kw)
    rig.command = counting
    rig.uplink = Uplink(rig, "https://127.0.0.1:9/ops2", "t", tmp_path / "up")
    exp = "2099-01-01T00:00:00Z"
    cmd = {"id": "web-1", "kind": "CH1_STOP", "args": {}, "expiresAt": exp, "requestedBy": "web"}
    first = rig.uplink.handle_command(cmd)
    api = rig.asm.robot_api

    def post(port, rid):
        body = json.dumps({"id": rid, "cmd": "GET_SPUTTER_STATUS", "data": {}, "raw": {}, "peer": "1.2.3.4:5"}).encode()
        rq = urllib.request.Request(f"http://127.0.0.1:{port}/robot/v1/command", body, {"Content-Type": "application/json"})
        out = {}
        t = threading.Thread(target=lambda: out.update(json.loads(urllib.request.urlopen(rq, timeout=10).read())))
        t.start()
        while t.is_alive():
            RR.drive(rig_now[0], lambda: not t.is_alive(), 1)
        t.join()
        return out
    rig_now = [rig]
    r1 = post(api.port, "rob-1")
    assert calls == ["CH1_STOP"] and api.executed == 1 and not r1.get("duplicate")
    RR.drive(rig, lambda: False, 2)
    RR.kill(rig)
    rig2 = RR.reboot(tmp_path, rig, 2.0, robot_api=True)
    rig_now[0] = rig2
    try:
        RR.drive(rig2, lambda: rig2.resumer.done, 30)
        calls2 = []
        real2 = rig2.command
        rig2.command = lambda kind, args, **kw: (calls2.append(kind), real2(kind, args, **kw))[1]
        rig2.uplink = Uplink(rig2, "https://127.0.0.1:9/ops2", "t", tmp_path / "up2")
        rig2.journal.bind_commands()
        again = rig2.uplink.handle_command(cmd)
        assert calls2 == [] and again == first and rig2.uplink.dup_cmds == 1      # 같은 웹 명령 id — 실행 안 함
        r2 = post(rig2.asm.robot_api.port, "rob-1")
        assert r2.get("duplicate") is True and rig2.asm.robot_api.executed == 0   # 같은 로봇 id — 실행 안 함
        assert r2["reply_cmd"] == r1["reply_cmd"] and r2["body"] == r1["body"]
        r3 = post(rig2.asm.robot_api.port, "rob-2")
        assert rig2.asm.robot_api.executed == 1 and not r3.get("duplicate")
    finally:
        RR.finish(rig2)


# ───────────── 실제 하위 프로세스(감시 프로그램 + v2.ch12) ─────────────
CHILD = r"""
import json, sys, threading, time
from pathlib import Path
sys.path.insert(0, sys.argv.pop(1))
from v2.ch12 import app as A
start_file = Path(sys.argv[1])
orig = A.App.start_journal
def sj(self, s):
    orig(self, s)
    if not start_file.exists():
        return
    def go():
        while not (s.started and self.resumer.done):
            time.sleep(0.1)
        e = s.engine("proc.ch1")
        e.preflight_hooks = {"gate": lambda e: (True, ""), "main_valve": lambda e: (True, ""), "chuck": lambda e, p: True}
        if e.busy:
            return
        start_file.unlink()
        print("START", e.start(params={"test_mode": True, "test_duration_sec": 25, "process_note": "sub"},
                               owner="local", source="local"), flush=True)
    threading.Thread(target=go, daemon=True).start()
A.App.start_journal = sj
sys.exit(A.main(sys.argv[2:]))
"""


def _free_port() -> int:
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


@pytest.mark.slow                                  # 실제 하위 프로세스 둘(감시 프로그램 + v2.ch12) — 약 25 s
@pytest.mark.skipif(sys.platform == "win32", reason="POSIX 신호로 시험 — 윈도우는 콘솔 이벤트(윈도우 확인 필요)")
def test_real_process_killed_and_resumed_under_supervisor(tmp_path):
    from v2.ch12.sim.rig import local_paths
    from v2.supervisor.supervisor import Supervisor
    cfg = tmp_path / "config"
    cfg.mkdir()
    vals = dict(local_paths(tmp_path / "root"), LOCAL_HMI_PORT=_free_port(), ROBOT_API_PORT=_free_port())
    (cfg / "local_override.json").write_text(json.dumps({"values": vals}), encoding="utf-8")
    flag = tmp_path / "start.flag"
    flag.write_text("1", encoding="utf-8")
    script = tmp_path / "child.py"
    script.write_text(CHILD, encoding="utf-8")
    out = []
    sup = Supervisor([sys.executable, str(script), str(ROOT), str(flag), "--config-dir", str(cfg)], tmp_path / "sup",
                     restart_delay_s=0.5, say=out.append)
    res = {}
    th = threading.Thread(target=lambda: res.update(code=sup.run()), daemon=True)
    try:
        th.start()
        jdir = tmp_path / "root" / "data" / "journal"

        def jr():
            try:
                return json.loads((jdir / "ch1.json").read_text(encoding="utf-8"))
            except Exception:
                return {}
        t_end = time.time() + 90
        while time.time() < t_end and not ((jr().get("state") or {}).get("block_elapsed_s") or 0) > 4 and th.is_alive():
            time.sleep(0.2)
        d1 = jr()
        assert d1.get("active") and d1["state"]["block_type"] == "test" and d1["state"]["resumable"], out
        pid1 = sup.child.pid
        sup.child.send_signal(signal.SIGKILL)                       # 비정상 종료(감시 프로그램이 다시 켬)
        t_end = time.time() + 90
        while time.time() < t_end and not (sup.starts >= 2 and jr().get("restarts") == 1 and
                                           (jr().get("state") or {}).get("running") and jr().get("pid") != pid1):
            time.sleep(0.2)
        d2 = jr()
        assert sup.starts == 2 and d2["run_id"] == d1["run_id"] and d2["restarts"] == 1, (d2.get("restarts"), out)
        assert d2["state"]["block_elapsed_s"] >= d1["state"]["block_elapsed_s"]   # 공백까지 경과로 이어짐
        sup.child.send_signal(signal.SIGTERM)                       # 의도적 종료 — 종료 코드 0, 다시 켜지 않음
        th.join(60)
        assert res.get("code") == 0 and sup.starts == 2 and not sup.gave_up
        d3 = jr()
        assert d3["exit"]["intentional"] is True and d3["active"] is False
        log = (tmp_path / "root" / "data" / "logs" / "ch12_v2.log").read_text(encoding="utf-8")
        assert "복구됨(공백" in log or "RESUMED" in log
    finally:
        sup.stop_requested = True
        if sup.child is not None and sup.child.poll() is None:
            sup.child.kill()
        th.join(10)


def test_supervisor_gives_up_after_repeated_crashes(tmp_path):
    """10분에 3번 넘게 죽으면 재시작을 멈춘다(실제 하위 프로세스 — 바로 죽는 제어 프로그램)."""
    from v2.supervisor.supervisor import EXIT_GAVE_UP, Supervisor
    sup = Supervisor([sys.executable, "-c", "import sys; sys.exit(1)"], tmp_path, restart_delay_s=0.01, say=lambda m: None)
    assert sup.run() == EXIT_GAVE_UP and sup.starts == 4 and sup.gave_up
    st = json.loads((tmp_path / "supervisor_state.json").read_text(encoding="utf-8"))
    assert st["gave_up"] is True and len(st["crashes"]) == 4


# ───────────── 플라즈마 클리닝·TSP — 늘 안전 정지 ─────────────
def _fake_active(tmp, part: str, state: dict, run_id: str) -> None:
    p = Path(tmp) / "data" / "journal" / f"{part}.json"
    d = json.loads(p.read_text(encoding="utf-8"))
    d.update(active=True, run_id=run_id, restarts=0, state=dict(d.get("state") or {}, **state))
    J.atomic_write(p, J.dumps(d))


def test_pc_and_tsp_are_stopped_not_resumed(tmp_path):
    """PC(클리닝 시간 대기 중이어도)·TSP 는 상태 유지 블록 목록에 없다 — 다시 켜면 RF 0 W·SET OFF·가스 OFF / TSP OFF·IG OFF."""
    rig = RR.boot(tmp_path)
    RR.drive(rig, lambda: rig.resumer.done, 30)
    RR.kill(rig)
    _fake_active(tmp_path, "pc", {"running": True, "phase": "run", "ch": 1, "run_id": "pc-1", "step": "process",
                                  "gas_idx": 3, "owner": "local"}, "pc-1")
    _fake_active(tmp_path, "tsp", {"busy": True, "phase": "tsp_on", "run_id": "tsp-1", "owner": "local"}, "tsp-1")
    plc = rig.sims["plc"]
    plc.coils[PM.resolve("DCV_SET_1")] = True                   # PC RF 가 켜져 있던 채로
    plc.regs[PM.REG_MAP["DCV_WRITE_1"]] = 300
    rig.sims["tsp"].on = True
    seen = []
    rig2 = RR.reboot(tmp_path, rig, 2.0, before_start=lambda r: r.event_listeners.append(seen.append))
    try:
        RR.drive(rig2, lambda: rig2.resumer.done and not rig2.journal.last_view.last("plc.dcv_set_1"), 90)
        dec = rig2.resumer.decisions
        assert dec["pc"]["result"] == "stopped" and dec["tsp"]["result"] == "stopped"
        assert {"pc", "tsp"} <= failed(dec["pc"]) | failed(dec["tsp"])
        ends = {(x.source, x.code) for x in seen if x.kind == "run_end"}
        assert ("engine:pc", "PROGRAM_RESTART") in ends and ("engine:tsp", "PROGRAM_RESTART") in ends
        v = rig2.journal.last_view
        assert not v.last("plc.dcv_set_1") and not v.last("plc.dcv_write_1")
        assert rig2.sims["tsp"].on is False
        runs = [m["data"] for m in rig2.hub.recent if m["type"] == "run_end"]
        assert {(r["chamber"], r["runId"], r["reasonCode"]) for r in runs} >= {("pc", "pc-1", "PROGRAM_RESTART"),
                                                                                 ("tsp", "tsp-1", "PROGRAM_RESTART")}
    finally:
        RR.finish(rig2)

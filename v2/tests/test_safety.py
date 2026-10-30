"""챔버12 안전(safety_rules.py 표·safety.Ch12Safety) — 시작 때 남은 파워 정리(S31)·의도적 종료 파워만(S32)·ALL STOP(S30) 순서·
정지 순서 고정(S28)·끝 정지 skip 금지(S29, D32)·규칙 표가 v1 근거·실행 위치를 갖는지.

시험대: sim.rig.Ch12FullRig(실행 프로그램과 같은 조립 — 시뮬레이터·실제 드라이버·실제 엔진, 가상 시계).
"""
from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.common.commands import Source
from v2.common.paramdef import defaults
from v2.ch12 import blocks as BK
from v2.ch12 import commands as C
from v2.ch12 import process_steps as PST
from v2.ch12 import safety_rules as SR
from v2.ch12.drivers import plc_map as PM
from v2.ch12.params import PARAMS
from v2.ch12.sim.rig import Ch12FullRig

TEST_ROWS = [{"#": "test", "Process_name": "T1", "time": "30s"}]
KINDS = {"trip", "reject", "notify", "recover", "maintain", "guard"}


def make(tmp, cfg=None, before=None):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = Ch12FullRig(c, Path(tmp), cfg=dict({"CAMERA_MIN_FREE_GB": 0.0, "CH1_SUPPORTS_RFPULSE": True}, **(cfg or {})),
                      robot_api=False)
    if before:
        before(rig)
    assert rig.start()
    return rig


def done(rig):
    rig.stop()
    rig.clock.close()


# ───────────── 표 ─────────────
def test_rule_table_has_v1_basis_and_location():
    keys = {p.key for p in PARAMS}
    ids = [r.id for r in SR.RULES]
    assert len(ids) == len(set(ids)) and ids == [f"S{i}" for i in range(1, len(ids) + 1)]
    codes = C.reason_codes()
    for r in SR.RULES:
        assert r.kind in KINDS and r.v1 and r.impl and r.when, r.id
        assert r.where in ("tick", "command", "config", "worker") or r.where.startswith(("event:", "state:")), r.id
        assert set(r.params) <= keys, (r.id, set(r.params) - keys)
        assert not r.reason or r.reason in codes or r.reason.startswith("E"), (r.id, r.reason)
    assert SR.IMPLEMENTED_IN == {r.id: r.impl for r in SR.RULES}
    from v2.ch12.safety import Ch12Safety
    assert all(hasattr(Ch12Safety, m) for m in ("all_stop", "first_read", "power_off_only"))
    eng = {r.id for r in SR.RULES if r.impl.startswith("safety.")}
    assert eng == set(SR.ENGINE_RULES) == {"S30", "S31", "S32"}
    pub = SR.public()
    assert pub["fixed"] is True and len(pub["steps"]) == len(SR.STOP_SEQUENCE) and pub["allStopSequence"]
    assert [s["part"] for s in pub["allStopSequence"]] == ["ch1", "ch2", "pc", "tsp"]
    from v2.ch12 import web_schema as W
    assert W.stop_part() == pub


def test_stop_sequence_is_v1_shutdown_order_and_fixed():
    """STOP·이상 중단 = v1 _create_shutdown_sequence 전체(표 순서) — 끝 정지의 skip 을 줘도 파워·릴레이는 빠지지 않는다(D32)."""
    cfg = defaults(PARAMS)
    sc = PST.step_cfg(cfg, 2)
    u = PST.Uses(dc=True, dc2=True, rf=True, dcp=True, rfp=True, power_select=True, gases={"AR": True, "O2": True},
                 guns={"G1": True, "G2": True, "G3": True}) if hasattr(PST, "Uses") else None
    full = PST.shutdown_steps(sc, u, force_all=True)
    acts = [s.action for s in full]
    order = ["PLC_CMD", "DC_POWER_STOP", "DC2_POWER_STOP", "RF_POWER_STOP", "DC_PULSE_STOP", "RF_PULSE_STOP"]
    assert acts[:6] == order and acts[-1] == "PLC_CMD" and full[-1].message == "메인 밸브 닫기"
    skipped = PST.shutdown_steps(sc, u, force_all=True, skip=tuple(p[0] for p in PST.SHUTDOWN_PARTS))
    msgs = [s.message for s in skipped]
    for keep in ("DC1 Power Off", "DC2 Power Off", "RF Power Off", "DC Pulse Off", "RF Pulse Off",
                 "Power_select 종료: Power Select OFF (SW_RF_SELECT)"):
        assert keep in msgs, keep                                         # 파워·릴레이는 skip 해도 남는다
    assert "메인 밸브 닫기" not in msgs and "전체 MFC Valve Open" not in msgs
    assert SR.END_STOP_MUST_KEEP == ("power", "relay") == PST.SHUTDOWN_MUST_KEEP
    assert [s.part for s in SR.STOP_SEQUENCE if not s.skippable] == ["power"] * 5 + ["relay"]
    parts_in_order = []
    for s in SR.STOP_SEQUENCE:
        if not parts_in_order or parts_in_order[-1] != s.part:
            parts_in_order.append(s.part)
    assert parts_in_order == [p[0] for p in PST.SHUTDOWN_PARTS]


def test_end_stop_skip_of_power_is_rejected():
    cfg = defaults(PARAMS)
    bl = [{"type": "end_stop", "params": {"skip": ["power"]}}]
    _clean, errs, _w = BK.check_blocks(bl, cfg, 1)
    assert errs                                                            # D32 — 레시피 검사에서 거부
    _clean, errs, _w = BK.check_blocks([{"type": "end_stop", "params": {"skip": ["mv_close", "flow_off"]}}], cfg, 1)
    assert not errs


def test_engine_stop_ignores_end_stop_skip(tmp_path):
    """적재 레시피의 끝 정지 skip(뺄 수 있는 것 전부)이 있어도 STOP 정지 순서(engine.shutdown_steps)는 전체 v1 순서."""
    rig = make(tmp_path)
    try:
        e = rig.asm.proc["ch1"]
        blocks = [{"type": "wait", "params": {"sec": 5}},
                  {"type": "end_stop", "params": {"skip": list(PST.SHUTDOWN_SKIPPABLE)}}]
        assert rig.command("RECIPE_LOAD", {"ch": 1, "blocks": blocks}, source=Source.WEB).ok, e.reject_detail
        msgs = [s.message for s in e.shutdown_steps()]
        assert msgs[0] == "Main Shutter 닫기 (항상)" and msgs[-1] == "메인 밸브 닫기" and "전체 MFC Valve Open" in msgs
    finally:
        done(rig)


# ───────────── S31 시작 때 남은 파워 ─────────────
def _residual(rig):
    sim = rig.sims["plc"]
    for set_name, wr_name, val in (("DCV_SET_0", "DCV_WRITE_0", 200), ("DCV_SET_2", "DCV_WRITE_2", 120)):
        sim.coils[PM.COIL_MAP[set_name]] = True
        sim.regs[PM.REG_MAP[wr_name]] = val


def test_residual_power_turned_off_when_idle(tmp_path):
    rig = make(tmp_path, before=_residual)
    try:
        sim = rig.sims["plc"]
        rig.loop.run(80)
        assert not sim.coil("DCV_SET_0") and sim.reg("DCV_WRITE_0") == 0         # CH2 DC1 0 W → SET OFF
        assert not sim.coil("DCV_SET_2") and sim.reg("DCV_WRITE_2") == 0         # CH2 RF 램프다운 → 0 W → SET OFF
        ev = [(x.kind, x.rule, x.code) for x in rig.safety.events]
        assert ("fired", "S31", "RESIDUAL_POWER_OFF") in ev and ev.count(("released", "S31", "RESIDUAL_POWER_OFF")) == 2
        assert sorted(n for n, _k in rig.safety.residual) == ["DC1", "RF(CH2)"]
        coils = [PM.COIL_MAP[n] for n in ("M_V_1_SW", "M_V_2_SW", "TURBO_1_SW", "R_P_2_SW")]
        assert all(sim.coils[a] for a in coils)                                  # 펌프·밸브는 건드리지 않는다
        n = len(rig.safety.events)
        rig.loop.run(5)
        assert len(rig.safety.events) == n                                       # 한 번뿐
    finally:
        done(rig)


def test_residual_power_kept_while_engine_busy(tmp_path):
    def before(rig):
        _residual(rig)
        rig.asm.proc["ch2"]._cmd = ("HOLD", None)                               # 공정 중(시작 명령 접수 상태)
    rig = make(tmp_path, before=before)
    try:
        rig.loop.run(10)
        sim = rig.sims["plc"]
        assert sim.coil("DCV_SET_0") and sim.reg("DCV_WRITE_0") == 200
        assert ("fired", "S31", "RESIDUAL_POWER_KEPT") in [(x.kind, x.rule, x.code) for x in rig.safety.events]
        assert rig.safety.residual == []
    finally:
        rig.asm.proc["ch2"]._cmd = None
        done(rig)


# ───────────── S30 ALL STOP ─────────────
def test_all_stop_stops_ch1_ch2_pc_tsp_together(tmp_path):
    rig = make(tmp_path)
    try:
        rig.loop.run(2)
        for ch in (1, 2):
            assert rig.command("RECIPE_LOAD", {"ch": ch, "rows": TEST_ROWS}, source=Source.WEB).ok
            assert rig.command("PROCESS_START", {"ch": ch}, source=Source.WEB).ok
        rig.loop.run(2)
        assert rig.safety.any_busy()
        calls = []
        for name, obj in (("pc", rig.asm.pc), ("tsp", rig.asm.tsp)):
            orig = obj.stop
            obj.stop = (lambda orig=orig, name=name, **k: (calls.append(name), orig(**k))[1])
        for ch in ("ch1", "ch2"):
            e = rig.asm.proc[ch]
            orig = e.stop
            e.stop = (lambda orig=orig, ch=ch, **k: (calls.append(ch), orig(**k))[1])
        r = rig.command("ALL_STOP", {}, source=Source.HMI)
        assert r.ok and calls == ["ch1", "ch2", "pc", "tsp"] == [s.part for s in SR.ALL_STOP_SEQUENCE]
        assert rig.safety.stopping and rig.safety.stopping.get("allStop")
        rig.loop.run(5)
        for ch in ("ch1", "ch2"):
            assert [(x.data.get("result"), x.code) for x in rig.asm.proc[ch].events if x.kind == "run_end"] == \
                [("aborted", "USER_STOP")]
        assert rig.safety.stopping is None and not rig.safety.any_busy()
    finally:
        done(rig)


# ───────────── S32 의도적 종료 — 파워 출력만 ─────────────
def test_power_off_only_keeps_relays_and_mfc(tmp_path):
    rig = make(tmp_path)
    try:
        rig.sims["ch1.mfc"].sp[2] = 3.0 * 0.1
        rig.loop.run(2)
        p = dict(base_pressure=1e-5, working_pressure=3, process_time=2.0, shutter_delay=0.1, use_ar=True, ar_flow=20,
                 use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True,
                 process_note="exit")
        assert rig.command("PROCESS_START", {"ch": 1, "params": p}, source=Source.WEB).ok
        e = rig.asm.proc["ch1"]
        rfp = rig.sims["ch1.rfpulse"]
        n = 0
        while n < 900 and not (rfp.out and e.runner.is_running and e.runner.steps[e.runner.idx].action == "DELAY"):
            rig.loop.run(1)
            n += 1
        assert rfp.out, e.log_lines and list(e.log_lines)[-20:]
        plc = rig.sims["plc"]
        before = {nm: plc.coil(nm) for nm in ("M_V_1_SW", "Ar_1_GAS_SW", "R_P_1_SW", "TURBO_1_SW")}
        assert before["M_V_1_SW"] and before["Ar_1_GAS_SW"]
        flow_before = rig.chambers["ch1"].gas_sccm["ar"]
        res = rig.safety.power_off_only("PROGRAM_EXIT")
        assert res is not None and "ch1" in res["process_was_running"] and "ch1 RF Pulse OFF" in res["done"]
        assert rig.safety.power_off_only("PROGRAM_EXIT") is None                # 두 번째는 없음
        rig.loop.run(3)
        assert not rfp.out                                                       # 파워 출력은 끔
        assert {nm: plc.coil(nm) for nm in before} == before                     # PLC 릴레이 그대로
        assert flow_before > 0 and rig.chambers["ch1"].gas_sccm["ar"] > 0        # MFC 그대로
        assert [x.code for x in e.events if x.kind == "run_end"] == ["PROGRAM_EXIT"]
        ev = [(x.kind, x.rule) for x in rig.safety.events]
        assert ("fired", "S32") in ev and ("released", "S32") in ev
    finally:
        done(rig)


def test_power_off_only_pc_running_turns_off_pc_rf_only(tmp_path):
    """S32 에 플라즈마 클리닝이 돌고 있으면 — PC 를 붙잡고(program_exit) PC RF 만 즉시 0 W·SET OFF. 가스 FLOW OFF('L30')·SP4 VALVE
    OPEN('O') 은 보내지 않는다(챔버K 결정 4 'MFC 그대로'). 결과는 실패 PROGRAM_EXIT."""
    rig = make(tmp_path)
    try:
        rig.loop.run(2)
        ui = {"gas_flow": "10", "rf_power": "100", "process_time": "1", "working_pressure": "2"}
        assert rig.command("PC_START", {"ch": 1, "params": ui}, source=Source.WEB).ok
        pc = rig.asm.pc
        n = 0
        while n < 600 and not (pc.pc.step == "process" and pc.countdown_left is not None and pc.countdown_left < 55):
            rig.loop.run(1)
            n += 1
        plc = rig.sims["plc"]
        assert plc.coils[PM.COIL_MAP["DCV_SET_1"]] and plc.regs[PM.REG_MAP["DCV_WRITE_1"]] > 0
        mfc = rig.sims["ch1.mfc"]
        n_cmds = len(mfc.set_commands_seen())
        flow = rig.chambers["ch1"].gas_sccm
        flow_before = dict(flow)
        res = rig.safety.power_off_only("PROGRAM_EXIT")
        assert "pc" in res["process_was_running"] and "RF(pc) 0 W·SET OFF" in res["done"], res
        n = 0
        while n < 30 and pc.running:
            rig.loop.run(1)
            n += 1
        assert not pc.running
        assert plc.regs[PM.REG_MAP["DCV_WRITE_1"]] == 0 and plc.coils[PM.COIL_MAP["DCV_SET_1"]] is False   # PC RF 끔
        after = mfc.set_commands_seen()[n_cmds:]
        assert not any(c.startswith("L3") or c == "O" for c in after), after                            # 가스·밸브 그대로
        assert dict(rig.chambers["ch1"].gas_sccm) == flow_before and any(v > 0 for v in flow_before.values())
        assert pc.last_final["ok"] is False and pc.last_final["reason"] == "PROGRAM_EXIT"
        logs = [ln.split("] ", 1)[1] for ln in pc.ui_log]
        assert any("[CLEANUP] 의도적 종료" in ln for ln in logs) and not any("종료: MFC GAS OFF" in ln for ln in logs)
    finally:
        done(rig)

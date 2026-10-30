"""CH2 RF 연속 전원(PLC D/A·A/D) 시험 — v1 골든(RFPowerAsync 를 v1 런타임 결선 그대로 실제 PLC 소켓으로 돌린 쓰기 PDU·
이벤트)과 같은 측정 대본에서 같은 쓰기 바이트·같은 문구인지, 램프(1 W/측정)·정지 램프(1 W 씩 1 s)·blind·Ref.p·저출력·0 W 확인."""
import json
from pathlib import Path

import pytest

from v2.ch12.drivers.plc_conv import dac_code
from v2.ch12.sim.plc_rig import BASE_CFG, PlcRig

GOLDEN = json.loads((Path(__file__).parent / "golden" / "dev_plc.json").read_text(encoding="utf-8"))
KINDS = ("status", "target_reached", "target_failed", "power_off_finished", "power_off_failed", "state_changed")


def norm_v2(events):
    return [[e.kind, e.data.get("running")] if e.kind == "state_changed" else [e.kind, e.detail or None]
            for e in events if e.kind in KINDS]


def norm_v1(events):
    return [e for e in events if e[0] in KINDS]


def run_script(r: PlcRig, ctl, target, meas):
    t0 = r.c.monotonic()
    box = []
    ctl.start(target, on_done=box.append)
    r.wait(lambda: bool(box), 10.0)
    for m in meas:
        r.sim.forced_regs[8], r.sim.forced_regs[9] = m
        n0 = ctl.polls
        for _ in range(5):
            if ctl.polls > n0 or not ctl.running:
                break
            r.second()
        if not ctl.running:
            break
    stop = []
    if ctl.running:
        r.after(0.3)
        ctl.stop(on_done=stop.append)
    for _ in range(80):
        if stop or any(e.kind in ("power_off_finished", "power_off_failed") for e in r.pevents):
            break
        r.second()
    r.after(0.5)
    return t0, [p.hex() for p in r.pdus(t0) if p[0] in (5, 6)]


@pytest.mark.parametrize("scen", GOLDEN["rf"], ids=[s["name"] for s in GOLDEN["rf"]])
def test_rf_matches_v1_golden(scen):
    r = PlcRig(rf=True)
    _t0, writes = run_script(r, r.rf, scen["target"], [tuple(m) for m in scen["meas"][:scen["reads_used"]]])
    assert writes == scen["writes"]
    assert norm_v2(r.pevents) == norm_v1(scen["events"])
    r.done()


def test_v1_values_actually_used():
    g = GOLDEN["rf_cfg"]
    assert BASE_CFG["RF_REFLECTED_THRESHOLD_W"] == g["RF_REFLECTED_THRESHOLD_W"] == 50.0
    assert BASE_CFG["RF_BLIND_RAMP_REACH_TIMEOUT_S"] == g["RF_BLIND_RAMP_REACH_TIMEOUT_S"] == 150.0
    assert BASE_CFG["CHAMBER_RF_CONT_RAMPDOWN_INTERVAL_MS"] == g["CHAMBER_RF_CONT_RAMPDOWN_INTERVAL_MS"] == 1000


def test_write_transform_and_dac():
    r = PlcRig(rf=True)
    x = r.rf.xform_write
    assert x(0.0) == 0.0 and x(0.01) == 0.0 and x(1.0) == pytest.approx(4.2869)
    assert x(600.0) == 600.0                                       # 0~RF_MAX_POWER 클램프
    assert dac_code(r.cfg, x(1.0)) == 17 and dac_code(r.cfg, x(2.0)) == 24 and dac_code(r.cfg, x(100.0)) == 672
    r.done()


def test_rampdown_1w_per_second_timing():
    r = PlcRig(rf=True)
    scen = next(s for s in GOLDEN["rf"] if s["name"] == "rf_ramp_5w")
    t0, writes = run_script(r, r.rf, scen["target"], [tuple(m) for m in scen["meas"]])
    d6 = [(t, p) for t, p in r.frames(t0) if p[:3] == bytes.fromhex("060006")]
    i = next(k for k, s in enumerate(r.pevents) if s.detail == "RF 파워 ramp-down 시작")
    assert i
    down = [t for t, p in d6][-6:]                                 # 3.1 → 2.1 → 1.1 → 0.1 → 0 → 0(확인)
    gaps = [b - a for a, b in zip(down, down[1:])]
    # v1 과 같이 '앞 쓰기 끝난 뒤 1 s'(device/rf_power.py:656) — 그 사이 1초 묶음 읽기(≈0.6 s)가 진행 중이면 그만큼 늦는다
    assert all(1.0 - 1e-6 <= g <= 1.0 + 0.8 for g in gaps[:4])
    r.done()


def test_blind_reach_timeout_150s_with_zero_output():
    r = PlcRig(rf=True)
    box = []
    r.rf.start(100.0, on_done=box.append)
    r.sim.forced_regs[8] = r.sim.forced_regs[9] = 0               # FWD 0.0 W 계속
    for _ in range(155):
        r.second()
        if not r.rf.running:
            break
    fail = [e for e in r.pevents if e.kind == "target_failed"]
    assert fail and fail[0].detail == "Blind ramp 도달 시간(150s) 초과: 시작 뒤 FWD 0.0 W(출력 없음)"
    assert 149.0 <= r.c.monotonic() <= 153.0
    r.done()


def test_ref_wait_timeout_60s():
    r = PlcRig(rf=True)
    box = []
    r.rf.start(10.0, on_done=box.append)                         # 목표 ≤ 70 → 처음부터 REF 감시
    r.sim.forced_regs[8], r.sim.forced_regs[9] = 40, 500           # FWD 5.4 W, REF 64.4 W > 50
    for _ in range(70):
        r.second()
        if not r.rf.running:
            break
    msgs = [e.detail for e in r.pevents if e.kind in ("status", "target_failed")]
    assert "Ref.p(64.4W) 안정화 대기 시작 (최대 60초)" in msgs
    assert "Ref.p(REF) 안정화 시간(60s) 초과: REF=64.4W > TH=50.0W" in msgs
    r.done()


def test_sim_rf_closed_loop_reaches_target_and_row():
    r = PlcRig(rf=True, tick=True)
    ch2 = r.sim.ch[2]
    ch2.gas_sccm["ar"] = 20.0
    ch2.pressure_override = 0.005
    box = []
    r.rf.start(20.0, on_done=box.append)
    r.after(60.0)
    assert any(e.kind == "target_reached" for e in r.pevents)
    row = r.rows[-1]
    assert row["ch2.rf.q"] == "ok" and row["ch2.rf.fwd_w"] == pytest.approx(20.0, abs=1.5) and row["ch2.rf.running"]
    stop = []
    r.rf.stop(on_done=stop.append)
    r.wait(lambda: bool(stop), 60.0)
    assert stop == [None] and r.sim.regs[6] == 0 and not r.sim.coils[82]
    r.done()


def test_pc_direct_mode_kick_and_shutdown():
    r = PlcRig()
    from v2.ch12.drivers.rf import RfPower
    pc = RfPower(r.c, r.d, r.cfg, True, r.pevents.append, mode="pc")
    t0 = r.c.monotonic()
    box = []
    pc.start(150.0, on_done=box.append)
    r.wait(lambda: bool(box), 5.0)
    w = [p.hex() for p in r.pdus(t0) if p[0] in (5, 6)]
    assert w[0] == "050051ff00" and w[1] == "06000501" + f"{dac_code(r.cfg, 50.0 * 1.74) & 0xFF:02x}"   # kick 50 W
    msgs = [e.detail for e in r.pevents if e.kind == "status"]
    assert "Kick 50.0W 전송 완료 — ramp 시작 대기" in msgs
    stop = []
    pc.stop(on_done=stop.append)
    r.wait(lambda: bool(stop), 30.0)
    assert stop == [None] and r.sim.regs[5] == 0 and not r.sim.coils[81]
    assert any(m.startswith("Ramp-Down: 50.0W ≤ 50.0W → 즉시 OFF") for m in (e.detail for e in r.pevents))
    r.done()


def test_bench_rejects_start():
    r = PlcRig(rf=True, writes=False)
    box = []
    r.rf.start(10.0, on_done=box.append)
    r.wait(lambda: bool(box), 5.0)
    assert box == ["BENCH_READ_ONLY"] and r.sim.write_pdus() == [] and not r.rf.running
    r.done()

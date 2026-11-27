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
    # R(D432): 환산값은 D/A 범위(PLC_DC_POWER_MAX_W 1000)로만 자른다 — v1 은 RF_MAX_POWER(600)에서 잘라 약 361 W 에서 막혔다
    assert x(600.0) == pytest.approx(1.6546 * 600.0 + 2.6323) and x(1000.0) == 1000.0
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


# ───────── W-21·22 대조 A — 반사파 대기 여유 폭(RF_REFLECTED_HYST_W) ─────────
def _raw_for(w, a=0.12940, b=-0.267):
    return int(round((w - b) / a))


def _osc_run(hyst, secs=75):
    """REF 가 임계 50 W 의 ±0.5 W 를 측정마다 오간다(FWD 5.4 W, 목표 10 W — 처음부터 REF 감시)."""
    r = PlcRig(rf=True, cfg={"RF_REFLECTED_HYST_W": hyst})
    box = []
    r.rf.start(10.0, on_done=box.append)
    hi, lo = _raw_for(50.5), _raw_for(49.6)
    t0 = r.c.monotonic()
    for i in range(secs):
        r.sim.forced_regs[8], r.sim.forced_regs[9] = 40, (hi if i % 2 == 0 else lo)
        r.second()
        if not r.rf.running:
            break
    return r, r.c.monotonic() - t0


def test_ref_wait_hysteresis_times_out_when_ref_hovers_around_threshold():
    r, took = _osc_run(1.0)
    msgs = [e.detail for e in r.pevents if e.kind in ("status", "target_failed")]
    assert sum(m.startswith("Ref.p(") and "안정화 대기 시작" in m for m in msgs) == 1          # v1 문구 그대로, 한 번만
    assert not any(m.startswith("Ref.p 안정화 완료") for m in msgs)
    assert "Ref.p 안정화 시간 초과. 즉시 중단합니다." in msgs and not r.rf.running and 60 <= took <= 66
    fails = [e for e in r.pevents if e.kind == "target_failed"]
    assert fails and fails[-1].reason == "RF_REF_TIMEOUT"
    ws = [e for e in r.pevents if e.kind == "ref_wait"]
    assert [e.data["phase"] for e in ws] == ["start", "end"] and ws[1].data["why"] == "시간 초과" and ws[1].level == "warn"
    assert ws[0].level == "warn" and ws[0].data["threshold"] == 50.0 and ws[0].data["max_s"] == 60.0
    r.done()


def test_ref_wait_hysteresis_zero_is_v1_behaviour():
    r, _ = _osc_run(0.0, secs=75)
    msgs = [e.detail for e in r.pevents if e.kind in ("status", "target_failed")]
    starts = sum("안정화 대기 시작" in m for m in msgs)
    assert starts >= 30 and sum(m.startswith("Ref.p 안정화 완료") for m in msgs) >= 30         # 오갈 때마다 시작·완료(v1)
    assert "Ref.p 안정화 시간 초과. 즉시 중단합니다." not in msgs and r.rf.running             # 시간 초과가 나지 않음(v1 문제 그대로)
    ws = [e for e in r.pevents if e.kind == "ref_wait"]
    assert len(ws) == 2 * starts or len(ws) == 2 * starts - 1                                # 대기 하나마다 시작·끝 한 번씩
    r.done()


def test_ref_wait_released_below_threshold_minus_hyst_and_end_on_stop():
    r = PlcRig(rf=True, cfg={"RF_REFLECTED_HYST_W": 1.0})
    box = []
    r.rf.start(10.0, on_done=box.append)
    r.wait(lambda: bool(box), 10.0)
    for raw in (_raw_for(55.0), _raw_for(49.6), _raw_for(49.6), _raw_for(48.0)):         # 시작 → 여유 폭 안(유지) → 풀림
        r.sim.forced_regs[8], r.sim.forced_regs[9] = 40, raw
        n0 = r.rf.polls
        for _ in range(5):
            r.second()
            if r.rf.polls > n0:
                break
    ws = [e for e in r.pevents if e.kind == "ref_wait"]
    assert [e.data["phase"] for e in ws] == ["start", "end"] and ws[1].data["why"] == "풀림" and ws[1].level == "info"
    msgs = [e.detail for e in r.pevents if e.kind == "status"]
    assert sum(m.startswith("Ref.p 안정화 완료(48.0W)") for m in msgs) == 1
    r.sim.forced_regs[9] = _raw_for(60.0)                                                  # 다시 대기 → 정지
    for _ in range(3):
        r.second()
    stop = []
    r.rf.stop(on_done=stop.append)
    for _ in range(30):
        r.second()
    ws = [e for e in r.pevents if e.kind == "ref_wait"]
    assert [(e.data["phase"], e.data.get("why")) for e in ws][2:] == [("start", None), ("end", "RF 끔")]
    r.done()


# ───────── W-21·22 대조 B — RF 꺼짐 표시(cmd_on·uncommanded) ─────────
def test_cmd_on_and_uncommanded_fields_and_events():
    from v2.ch12.drivers.plc import PlcPoll
    for mode, coil, fr in (("ch2", 82, 8), ("pc", 81, 2)):
        r = PlcRig(rf=True)
        ctl = r.rf if mode == "ch2" else __import__("v2.ch12.drivers.rf", fromlist=["RfPower"]).RfPower(
            r.c, r.d, r.cfg, True, r.pevents.append, mode="pc")
        b = "ch2.rf" if mode == "ch2" else "pc.rf"
        raw_noise = 0                                                     # A/D 0 → 보정식 오프셋만(CH2 0 W, PC 3.1−4 → 0)
        t = [100.0]

        def dec(latch, f_raw, dt=1.0):
            t[0] += dt
            regs = {fr: f_raw, fr + 1: 0}
            return ctl._decode(PlcPoll(0.0, t[0], {} if latch is None else {coil: latch}, regs, {}))
        o = dec(False, raw_noise)
        assert o[f"{b}.cmd_on"] is False and o[f"{b}.uncommanded"] is False
        o = dec(None, raw_noise, dt=3.0)                                   # 양보한 tick(래치 못 읽음) — 3 s 까지 마지막 값(H1)
        assert o[f"{b}.cmd_on"] is False and o[f"{b}.enable"] is None
        o = dec(None, raw_noise, dt=0.5)                                   # 3 s 넘게 모름 → 모름
        assert o[f"{b}.cmd_on"] is None and o[f"{b}.uncommanded"] is None
        o = dec(True, raw_noise)                                           # 다시 시작 직후 — 쓰기값 모름 + 래치 켜짐 → 명령 출력으로 봄
        assert ctl.last_sent is None and o[f"{b}.cmd_on"] is True and o[f"{b}.uncommanded"] is False
        ctl.last_sent = 0.0                                                # 쓰기값 0 + 래치 켜짐 → 명령 출력 아님
        assert dec(True, raw_noise)[f"{b}.cmd_on"] is False
        ctl.last_sent = 50.0
        assert dec(True, raw_noise)[f"{b}.cmd_on"] is True
        assert dec(None, raw_noise)[f"{b}.cmd_on"] is True                # 공정 중 양보한 tick 도 켜짐 그대로(깜빡이지 않음)
        ctl.last_sent = None
        f_hi = 400 if mode == "ch2" else 300                               # 명령 없이 Forward ≥ 5 W(CH2 59.6 W, PC 44.2 W)
        n0 = len(r.pevents)
        o = dec(False, f_hi)                                               # 명령으로 끈 직후(5 s 안) — 내려가는 중, 경고 없음
        assert o[f"{b}.cmd_on"] is False and o[f"{b}.uncommanded"] is False
        for _ in range(3):
            o = dec(False, f_hi, dt=2.0)
        assert o[f"{b}.uncommanded"] is True and o[f"{b}.fwd_w"] >= 5.0
        for _ in range(2):
            o = dec(False, raw_noise)
        assert o[f"{b}.uncommanded"] is False
        ev = [e for e in r.pevents[n0:] if e.kind.startswith("rf_uncommanded")]
        assert [e.kind for e in ev] == ["rf_uncommanded", "rf_uncommanded_end"] and ev[0].level == "warn" and ev[1].level == "info"
        assert "전원 장치 앞면(LOCAL)" in ev[0].detail and "RF" not in ev[0].detail        # 화면 줄 출처·이름과 같은 말 없이
        r.done()

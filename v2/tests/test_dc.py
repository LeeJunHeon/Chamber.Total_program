"""CH2 DC 연속 전원(PLC D/A·A/D) 시험 — v1 골든(DCPowerAsync 를 v1 런타임 결선 그대로 실제 PLC 소켓으로 돌린 쓰기 PDU·
이벤트)과 같은 측정 대본에서 같은 쓰기 바이트·같은 이벤트 문구인지 본다(가상 시계·PlcSim)."""
import json
from pathlib import Path

import pytest

from v2.common.worker import Status
from v2.ch12.drivers import dc as DC
from v2.ch12.drivers.plc_conv import dac_code
from v2.ch12.sim.plc_rig import PlcRig

GOLDEN = json.loads((Path(__file__).parent / "golden" / "dev_plc.json").read_text(encoding="utf-8"))
KINDS = ("status", "target_reached", "target_failed", "power_off_finished", "power_off_failed", "state_changed")


def norm_v2(events, unit="DC1"):
    out = []
    for e in events:
        if e.data.get("unit") != unit or e.kind not in KINDS:
            continue
        out.append([e.kind, e.data.get("running")] if e.kind == "state_changed" else [e.kind, e.detail or None])
    return out


def norm_v1(events):
    return [e for e in events if e[0] in KINDS]


def run_script(r: PlcRig, ctl, target, meas, regs, step_s=1):
    t0 = r.c.monotonic()
    box = []
    ctl.start(target, on_done=box.append)
    r.wait(lambda: bool(box), 10.0)
    for m in meas:
        r.sim.forced_regs[regs[0]], r.sim.forced_regs[regs[1]] = m
        n0 = ctl.polls
        for _ in range(10):
            if ctl.polls > n0 or not ctl.running:
                break
            r.second()
        if not ctl.running:
            break
    stop = []
    if ctl.running:
        r.second()
        ctl.stop(on_done=stop.append)
    for _ in range(60):
        if stop or (not ctl.running and any(e.kind in ("power_off_finished", "power_off_failed") for e in r.pevents)):
            break
        r.second()
    r.after(0.5)
    return [p.hex() for p in r.pdus(t0) if p[0] in (5, 6)]


@pytest.mark.parametrize("scen", GOLDEN["dc"], ids=[s["name"] for s in GOLDEN["dc"]])
def test_dc_matches_v1_golden(scen):
    r = PlcRig(dc=True, cfg={"CH2_SUPPORTS_HEATER": False})
    writes = run_script(r, r.dc1, scen["target"], [tuple(m) for m in scen["meas"][:scen["reads_used"]]], (0, 1))
    assert writes == scen["writes"]
    assert norm_v2(r.pevents) == norm_v1(scen["events"])
    r.done()


def test_dc_poll_interval_5s_and_row_values():
    r = PlcRig(dc=True, tick=True, cfg={"CH2_SUPPORTS_HEATER": False})
    r.sim.ch[2].gas_sccm["ar"] = 20.0
    r.sim.ch[2].pressure_override = 0.005                    # 5 mTorr — 점화
    box = []
    r.dc1.start(100.0, on_done=box.append)
    r.after(21.0)
    disp = [e for e in r.pevents if e.kind == "display" and e.data["unit"] == "DC1"]
    assert 4 <= len(disp) <= 5                                # DC_INTERVAL_MS 5 s
    row = r.rows[-1]
    assert row["ch2.dc.q"] == "ok" and row["ch2.dc.dc1_running"] is True and row["ch2.dc.dc1_enable"] is True
    assert row["ch2.dc.dc1_p"] == pytest.approx(100.0, abs=3.0)
    assert any(e.kind == "target_reached" for e in r.pevents)
    stop = []
    r.dc1.stop(on_done=stop.append)
    r.wait(lambda: bool(stop), 20.0)
    assert stop == [None] and r.sim.regs[4] == 0 and not r.sim.coils[80]
    r.done()


def test_dc_zero_write_failure_power_off_unconfirmed():
    r = PlcRig(dc=True, cfg={"CH2_SUPPORTS_HEATER": False})
    box = []
    r.dc1.start(50.0, on_done=box.append)
    r.wait(lambda: bool(box), 5.0)
    r.after(1.0)
    t = r.c.monotonic()
    r.sim.add_fault("exception", t, t + 60.0)                 # PLC 가 모든 쓰기를 거부(E403)
    stop = []
    r.dc1.stop(on_done=stop.append)
    r.wait(lambda: bool(stop), 30.0)
    assert stop == ["POWER_OFF_UNCONFIRMED"] and r.dc1.output_off_unconfirmed
    fail = [e for e in r.pevents if e.kind == "power_off_failed"]
    assert fail and fail[0].detail.startswith("DC 0W 쓰기 실패 — 출력 상태 미확인 (10.")
    zeros = [p for p in r.pdus(t) if p == bytes.fromhex("0600040000")]
    assert len(zeros) >= 15                                   # 0.5 s 간격으로 10 s 까지 재시도
    assert r.sim.coils[80]                                    # 0 W 미확인이면 SET OFF 하지 않는다
    r.done()


def test_dc2_disabled_with_heater_and_bench():
    r = PlcRig(dc=True)                                       # v1 운영값: CH2_SUPPORTS_HEATER = true → DC2 금지
    box = []
    r.dc2.start(100.0, on_done=box.append)
    assert box == ["DEVICE_DISABLED"] and r.sim.write_pdus() == []
    r.done()
    r = PlcRig(dc=True, writes=False, cfg={"CH2_SUPPORTS_HEATER": False})
    box = []
    r.dc1.start(100.0, on_done=box.append)
    r.wait(lambda: bool(box), 5.0)
    assert box == ["BENCH_READ_ONLY"] and r.sim.write_pdus() == []
    stop = []
    r.dc1.stop(on_done=stop.append)                           # 안 쓴 런 → 바로 완료
    assert stop == [None]
    r.done()


def test_dc2_channel_registers():
    r = PlcRig(dc=True, cfg={"CH2_SUPPORTS_HEATER": False})
    box = []
    t0 = r.c.monotonic()
    r.dc2.start(500.0, on_done=box.append)
    r.wait(lambda: bool(box), 5.0)
    w = [p.hex() for p in r.pdus(t0) if p[0] in (5, 6)]
    assert w[:2] == ["050053ff00", "06000707d0"]               # SET_3(M00053) · D7 = 2000
    r.done()


def test_overcurrent_limit_and_dac():
    cfg = PlcRig.__init__.__globals__["BASE_CFG"]
    lim = lambda p: DC.dc_overcurrent_limit_a(cfg, p)
    assert lim(None) == 1.0 and lim(300) == 1.0 and lim(450) == pytest.approx(1.5) and lim(600) == pytest.approx(2.0)
    assert lim(900) == pytest.approx(3.0)
    assert lim(-5) == 1.0
    g = {tuple(c["args"]): c["value"] for c in GOLDEN["plc"]["cases"] if c["name"] == "power_write"}
    for (w, _idx), code in g.items():
        assert dac_code(cfg, w) == code

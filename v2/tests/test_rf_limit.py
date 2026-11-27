"""R — RF 운전 상한(사용자 결정 2026-11-27, D432 — 결정 D136 'v1 과 같게'를 이 항목만 바꿈).

v1 은 W→D/A 환산값 a·W+b 를 RF_MAX_POWER(600)로 잘라 CH2 RF 는 약 361 W, Plasma Cleaning 은 약 345 W 에서 막혔다(그보다 큰
레시피는 받아들인 뒤 목표에 닿지 못함). v2 는 환산값을 D/A 범위(PLC_DC_POWER_MAX_W)로만 자르고, 넣을 수 있는 W 를 새 설정
RF_OPERATING_MAX_W(CH2, 기본 360)·PC_RF_OPERATING_MAX_W(Plasma Cleaning, 기본 340)로 정해 넘으면 거부한다(고쳐 쓰지 않음):
레시피 시작 전 확인·블록 레시피 검사·Plasma Cleaning 시작·공정 중 값 바꾸기·드라이버. 상한 이하의 모든 W 에서 PLC 로 나가는 값은 v1 과 같다.
"""
import pytest

from v2.ch12.process_steps import caps_from_cfg
from v2.ch12.drivers.plc_conv import dac_code
from v2.ch12.drivers.rf import RfPower
from v2.ch12.params import table
from v2.ch12.recipe_check import check_norm_params, pc_rf_operating_max, rf_operating_max
from v2.ch12.sim.plc_rig import PlcRig
from v2.tests import proc_rig as R

DEF = {d.key: d.default for d in table().params}


def v1_scaled(cfg, mode, w):
    """v1 device/rf_power.py:927-932 _xform_write — 0.01 W 이하는 0, 아니면 a·W+b 를 0~RF_MAX_POWER(600)."""
    a, b = ((cfg["PC_RF_WRITE_INV_A"], cfg["PC_RF_WRITE_INV_B"]) if mode == "pc"
            else (cfg["CHAMBER_RF_CONT_WRITE_INV_A"], cfg["CHAMBER_RF_CONT_WRITE_INV_B"]))
    if w <= 0.01:
        return 0.0
    return max(0.0, min(cfg["RF_MAX_POWER"], a * float(w) + b))


def test_defaults_and_names():
    assert DEF["RF_OPERATING_MAX_W"] == 360.0 and DEF["PC_RF_OPERATING_MAX_W"] == 340.0 and DEF["RF_MAX_POWER"] == 600.0
    # 기본값은 v1 에서 실제로 나가던 최대보다 낮다 — 장비 출력이 달라지지 않는다
    ch2_v1_max = (600.0 - DEF["CHAMBER_RF_CONT_WRITE_INV_B"]) / DEF["CHAMBER_RF_CONT_WRITE_INV_A"]
    pc_v1_max = (600.0 - DEF["PC_RF_WRITE_INV_B"]) / DEF["PC_RF_WRITE_INV_A"]
    assert 360.0 < ch2_v1_max < 361.1 and 340.0 < pc_v1_max < 345.0
    t = table()
    d = {p.key: p for p in t.params}
    assert d["RF_OPERATING_MAX_W"].min == 1.0 and d["PC_RF_OPERATING_MAX_W"].min == 1.0
    rule = next(r for r in t.rules if r.id == "RF01")
    assert rule.check(dict(DEF)) is None
    assert rule.check(dict(DEF, RF_OPERATING_MAX_W=601.0)) and rule.check(dict(DEF, PC_RF_OPERATING_MAX_W=700.0))
    assert rf_operating_max(DEF) == 360.0 and pc_rf_operating_max(DEF) == 340.0
    assert rf_operating_max({"RF_MAX_POWER": 600.0}) == 600.0                    # 옛 설정(키 없음) — 전원 정격
    assert rf_operating_max(dict(DEF, RF_MAX_POWER=300.0)) == 300.0              # 정격보다 크지 않게


@pytest.mark.parametrize("mode,lim", [("ch2", 360), ("pc", 340)])
def test_plc_value_matches_v1_for_every_watt_up_to_limit(mode, lim):
    """상한 이하 0~상한 W 를 1 W 간격으로 — PLC 로 나가는 D/A 코드가 v1 과 같다(CH2 는 0~360 W, Plasma Cleaning 은 0~340 W)."""
    r = PlcRig(rf=True, cfg={"PC_RF_OPERATING_MAX_W": 340.0, "PC_RF_WRITE_INV_A": DEF["PC_RF_WRITE_INV_A"],
                             "PC_RF_WRITE_INV_B": DEF["PC_RF_WRITE_INV_B"]})
    drv = r.rf if mode == "ch2" else RfPower(r.c, r.d, r.cfg, True, r.pevents.append, mode="pc")
    try:
        assert drv.op_max() == float(lim)
        diff = [w for w in range(0, lim + 1)
                if dac_code(r.cfg, drv.xform_write(float(w))) != dac_code(r.cfg, v1_scaled(r.cfg, mode, w))]
        assert diff == []
        # 상한 위는 v1 이 600 에서 막혔지만 v2 는 D/A 범위까지(상한을 올렸을 때만 쓰인다)
        assert drv.xform_write(500.0) > 600.0 and drv.xform_write(5000.0) == r.cfg["PLC_DC_POWER_MAX_W"]
    finally:
        r.done()


def test_plc_write_pdu_at_limit_is_v1_value():
    """드라이버가 실제로 보내는 PLC 쓰기(D/A 레지스터)가 v1 환산값의 코드와 같다 — 360 W."""
    r = PlcRig(rf=True)
    try:
        t0 = r.c.monotonic()
        res = r.call(r.d.rf_write_w, r.rf.xform_write(360.0), rf_ch=2, source="ch2.rf")
        assert res.status == "ok"
        code = dac_code(r.cfg, v1_scaled(r.cfg, "ch2", 360))
        writes = [p for p in r.pdus(t0) if p[0] == 6]
        assert writes and int.from_bytes(writes[-1][3:5], "big") == code
    finally:
        r.done()


def test_driver_rejects_target_above_limit_and_caps_sent_watts():
    r = PlcRig(rf=True)
    try:
        box = []
        r.rf.start(361.0, on_done=box.append)
        assert box == ["BAD_ARGS"] and not r.rf.running
        st = [e.detail for e in r.pevents if e.kind == "status"]
        assert any("RF 시작 거부" in s and "360 W" in s and "RF_OPERATING_MAX_W" in s for s in st), st
        # 상한까지는 시작하고, 보정(fine-up·유지)으로 목표를 넘겨 올려도 보내는 W 는 상한까지
        r.rf.running, r.rf.target_power, r.rf.current_step, r.rf.state = True, 360.0, 359.0, "RAMPING_UP"
        r.rf.ref_armed, r.rf.forward_w, r.rf.last_sent = True, 300.0, 359.0
        sent = []
        orig = r.rf._send
        r.rf._send = lambda w, after=None, guard=True: (sent.append(float(w)), orig(w, after, guard))[1]
        for _ in range(5):
            r.rf._adjust_once()
        assert sent and max(sent) <= 360.0 and r.rf.current_step == 360.0
        assert r.rf.retarget(400.0) is None and r.rf.target_power == 360.0
        assert r.rf.retarget(200.0) == 200.0
        r.rf.running = False
    finally:
        r.done()


def test_recipe_start_check_rejects_rf_above_limit():
    caps = caps_from_cfg(dict(DEF, CH2_SUPPORTS_RF_CONT=True), 2)
    base = dict(use_ar=True, ar_flow=20, use_g1=True, use_rf_power=True)
    assert check_norm_params(dict(base, rf_power=360.0), 2, caps, DEF) == []
    errs = check_norm_params(dict(base, rf_power=361.0), 2, caps, DEF)
    assert errs == ["RF Target Power(W)는 운전 상한 360 W 이하여야 합니다 (현재 361, RF_OPERATING_MAX_W)"]
    assert check_norm_params(dict(base, rf_power=450.0), 2, caps, dict(DEF, RF_OPERATING_MAX_W=500.0)) == []


def test_block_recipe_check_rejects_rf_above_limit():
    from v2.ch12.blocks import check_blocks
    cfg = dict(DEF, CH2_SUPPORTS_RF_CONT=True)
    caps = caps_from_cfg(cfg, 2)
    _o, errs, _w = check_blocks([{"type": "power_on", "params": {"rf_power": 400}}], cfg, 2, caps=caps)
    assert any("rf_power: 400 > 360 W(운전 상한 RF_OPERATING_MAX_W)" in e for e in errs), errs
    _o, errs, _w = check_blocks([{"type": "power_on", "params": {"rf_power": 360}}], cfg, 2, caps=caps)
    assert not any("운전 상한" in e for e in errs), errs


P2 = dict(base_pressure=7e-6, working_pressure=5, process_time=1.0, shutter_delay=0.2, use_ar=True, ar_flow=20,
          use_rf_power=True, rf_power=100, use_ms=True, use_g1=True, process_note="r-limit")


def _main(e):
    return lambda: (e.cur_inst is not None and e.cur_inst.type == "main" and e.runner.is_running
                    and not e.runner.shutdown and e.runner.cd is not None)


def test_process_adjust_rejects_rf_above_limit(tmp_path):
    rig = R.make(tmp_path, chambers=("ch2",))
    e = rig.eng["ch2"]
    got = {}

    def in_main():
        d = e.actions.rf
        got["over"] = e.adjust("rf", "power", 361)
        got["detail"] = e.reject_detail
        got["target_kept"] = d.target_power
        got["max"] = {(a["device"], a["item"]): a["max"] for a in e.adjust_state()}[("rf", "power")]
        got["ok"] = e.adjust("rf", "power", 150)
        got["target_after"] = d.target_power
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=dict(P2))), R.when(_main(e), in_main)])
    assert got["over"] == "ADJUST_OUT_OF_RANGE" and "361 > 360" in got["detail"] and "RF_OPERATING_MAX_W" in got["detail"]
    assert got["target_kept"] == 100.0 and got["max"] == 360.0
    assert got["ok"] is None and got["target_after"] == 150.0


def test_recipe_start_above_limit_is_rejected_before_running(tmp_path):
    rig = R.make(tmp_path, chambers=("ch2",))
    e = rig.eng["ch2"]
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=dict(P2, rf_power=400)))], max_ticks=60)
    ends = [x for x in e.events if x.kind == "run_end"]
    assert ends and ends[-1].code == "RECIPE_PARAM_ERROR" and "운전 상한 360 W" in ends[-1].detail
    assert not any(x.kind == "status" and "RF" in (x.detail or "") and "Ramp-Up" in (x.detail or "") for x in rig.events)


def test_plasma_cleaning_start_rejects_rf_above_limit(tmp_path):
    from v2.tests.test_plasma_cleaning import UI, make, popups
    r, eng = make(tmp_path)
    try:
        res = eng.start(params=dict(UI, rf_power="341"), source="web", by="op")
        assert not res.accepted and res.code == "E230"
        assert res.message == "RF Power 341 W 는 운전 상한 340 W 를 넘습니다. (PC_RF_OPERATING_MAX_W)"
        assert ("warning", "입력값 확인", "RF Power 341 W 는 운전 상한 340 W 를 넘습니다.") in popups(eng)
        assert not eng.running and not r.run_state.is_running("pc", 1)
        ok = eng.start(params=dict(UI, rf_power="340"), source="web", by="op")
        assert ok.accepted
        eng.stop()
    finally:
        r.stop()

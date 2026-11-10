"""2026-11-09 검토 반영 C — 공정 중 값 수정 상한·직접 쓰기 경로·파워 램프 일시정지 고정.

C2: 파워(dc·dc2·rf·dcp·rfp)는 장비 최대(DC_MAX_POWER·RF_MAX_POWER·DCP_MAX_POWER_W)를 넘으면 거부(D11 — 고쳐 쓰지 않음), 히터 목표는 상한,
    유한하지 않은 숫자(inf·NaN)·음수 유량은 거부. 지금 블록에 바로 쓸 때도 start() 와 같은 경로(제한·카운터·과전류 한계 — 제한된 목표 기준).
C3: 파워 램프(power_on 블록) 일시정지 = 지금 측정 출력으로 목표 고정, 재개하면 원래 목표로 램프 계속, 고정 중에도 드라이버 감시는 계속.
"""
import math

import pytest

from v2.tests import proc_rig as R

P2 = dict(base_pressure=7e-6, working_pressure=5, process_time=1.0, shutter_delay=0.2, use_ar=True, ar_flow=20,
          use_dc_power=True, dc_power=100, use_ms=True, use_g1=True, process_note="s1109")
P1 = dict(base_pressure=1e-5, working_pressure=3, process_time=1.0, shutter_delay=0.2, use_ar=True, ar_flow=20,
          use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True, process_note="s1109")


def _blk(e, t):
    return lambda: e.cur_inst is not None and e.cur_inst.type == t and e.runner.is_running and not e.runner.shutdown


def _main(e):
    return lambda: _blk(e, "main")() and e.runner.cd is not None


def test_adjust_power_above_equipment_max_is_rejected_not_clamped(tmp_path):
    rig = R.make(tmp_path, chambers=("ch2",), cfg={"DC_MAX_POWER": 300.0})
    e = rig.eng["ch2"]
    got = {}

    def in_main():
        d = e.actions.dc[1]
        got["over"] = e.adjust("dc", "power", 301)
        got["detail"] = e.reject_detail
        got["inf"] = e.adjust("dc", "power", math.inf)
        got["nan"] = e.adjust("dc", "power", math.nan)
        got["flow_neg"] = e.adjust("mfc", "ar_flow", -0.1)
        got["flow_inf"] = e.adjust("mfc", "ar_flow", math.inf)
        got["target_unchanged"] = d.target_power
        got["ok"] = e.adjust("dc", "power", 250)
        got["target_after"] = d.target_power
        got["max_in_state"] = {(a["device"], a["item"]): a["max"] for a in e.adjust_state()}
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=dict(P2))), R.when(_main(e), in_main)])
    assert got["over"] == "ADJUST_OUT_OF_RANGE" and "DC_MAX_POWER" in got["detail"] and "301" in got["detail"]
    assert got["inf"] == "BAD_ARGS" and got["nan"] == "BAD_ARGS" and got["flow_neg"] == "ADJUST_OUT_OF_RANGE" and got["flow_inf"] == "BAD_ARGS"
    assert got["target_unchanged"] == 100.0                                     # 고쳐 쓰지 않음(거부하고 그대로)
    assert got["ok"] is None and got["target_after"] == 250.0
    assert got["max_in_state"][("dc", "power")] == 300.0 and got["max_in_state"][("mfc", "ar_flow")] is None
    rej = [x for x in e.events if x.kind == "adjust" and x.data["result"] == "rejected"]
    assert {x.code for x in rej} == {"ADJUST_OUT_OF_RANGE", "BAD_ARGS"}


def test_adjust_dc_live_goes_through_start_path_limit_and_overcurrent_basis(tmp_path):
    """직접 쓰기 = driver.retarget — start() 와 같은 제한(0~DC_MAX_POWER)·카운터 0·과전류 한계 문구(제한된 목표 기준)."""
    rig = R.make(tmp_path, chambers=("ch2",), cfg={"DC_MAX_POWER": 300.0})
    e = rig.eng["ch2"]
    got = {}

    def in_main():
        d = e.actions.dc[1]
        d.overcurrent_streak = d.low_power_streak = 2
        got["r"] = e.adjust("dc", "power", 200)
        got["t"], got["streaks"] = d.target_power, (d.overcurrent_streak, d.low_power_streak)
        got["clamp"] = d.retarget(1e9)                                          # 안전망 — 어떤 경로도 최대를 넘지 않음
        got["neg"] = d.retarget(-5)
        d.retarget(200)
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=dict(P2))), R.when(_main(e), in_main)])
    assert got["r"] is None and got["t"] == 200.0 and got["streaks"] == (0, 0)
    assert got["clamp"] == 300.0 and got["neg"] == 0.0
    d_logs = [x.detail for x in e.sys.events if x.kind == "status" and "[overcurrent]" in (x.detail or "")] if hasattr(e.sys, "events") else []
    assert any("(설정 200W" in m for m in d_logs), d_logs[-5:]                 # 한계는 제한된 목표(200 W) 기준으로 다시 계산·기록


def test_adjust_pulse_and_rf_and_heater_limits(tmp_path):
    rig = R.make(tmp_path, chambers=("ch1",), cfg={"RF_MAX_POWER": 150.0})
    e = rig.eng["ch1"]
    got = {}

    def in_main():
        got["rfp_over"] = e.adjust("rfp", "power", 151)
        got["rfp_ok"] = e.adjust("rfp", "power", 140)
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=dict(P1))), R.when(_main(e), in_main)])
    assert got["rfp_over"] == "ADJUST_OUT_OF_RANGE" and got["rfp_ok"] is None
    from v2.ch12 import blocks as BK
    from v2.ch12.process_adjust import AdjustMixin
    eng = type("_E", (AdjustMixin,), {"cfg": {"DC_MAX_POWER": 1000.0, "DCP_MAX_POWER_W": 500.0, "RF_MAX_POWER": 600.0,
                                              "CH2_HEATER_MAX_TEMP": 400.0, "CH2_HEATER_SV_LIMIT_C": 900.0}})()
    m = {(a.device, a.item): eng.adjust_max(a) for a in BK.ADJUST_ITEMS}
    assert m[("dcp", "power")] == 500.0 and m[("rf", "power")] == 600.0 and m[("rfp", "power")] == 600.0
    assert m[("heater", "target")] == 400.0 and m[("dc", "power")] == 1000.0 and m[("mfc", "wp")] is None


def test_pause_power_on_holds_target_at_measured_and_resume_restores(tmp_path):
    rig = R.make(tmp_path, chambers=("ch2",))
    e = rig.eng["ch2"]
    got = {}

    def pause():
        d = e.actions.dc[1]
        got["orig"] = d.target_power
        got["p"] = e.pause()
        got["meas"] = float(d.power_w or 0.0)
        got["held_target"] = d.target_power
        got["held"] = getattr(d, "_held", None)
        got["t"] = e.clock.monotonic()
        d.power_w = 0.0                                                          # 이탈(측정 0 W) — 고정 중에도 저전력 감시는 계속
        n0 = d.low_power_streak
        d._check_low_power()
        got["low_counted"] = d.low_power_streak > n0
        d.low_power_streak = 0

    def resume():
        d = e.actions.dc[1]
        got["r"] = e.resume_pause()
        got["restored"], got["held_after"] = d.target_power, getattr(d, "_held", None)
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=dict(P2, process_time=0.05))),
                                 R.when(lambda: _blk(e, "power_on")() and e.actions.dc[1].running and (e.actions.dc[1].power_w or 0) > 0
                                        and "p" not in got, pause),
                                 R.when(lambda: "t" in got and "r" not in got and e.clock.monotonic() - got["t"] >= 5.0, resume)])
    assert got["p"] is None and got["held"] == got["orig"] == 100.0
    assert got["held_target"] == pytest.approx(got["meas"], abs=1e-6) and got["held_target"] < 100.0 or got["meas"] >= 100.0
    assert got["low_counted"]
    assert got["r"] is None and got["restored"] == 100.0 and got["held_after"] is None
    assert R.ends(e) == [("done", None)]
    logs = R.logs(e)
    assert any("[PAUSE] 파워 램프 고정: DC1" in x for x in logs) and any("[PAUSE] 파워 목표 복원: DC1" in x for x in logs)


def test_sequence_edit_rejected_once_stop_started_even_before_runner_flags(tmp_path):
    """11-09 C4 — 확인과 편집이 같은 잠금 안. STOP 요청이 잠금 안에서 _stopping 을 세우므로, 러너가 shutdown·stop_requested 를
    세우기 전 틈(여기서는 request_stop 을 막아 둔 채)에도 편집은 거부된다."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    got = {}

    def stop_then_edit():
        r = e.runner
        got["before"] = e.edit_sequence({"op": "insert", "at": len(e.blocks) - 1,
                                         "block": {"type": "wait", "params": {"sec": 1}}}, source="web")
        orig = r.request_stop
        r.request_stop = lambda: None                    # 러너 쪽 플래그가 아직 안 선 틈
        e.stop("web")
        got["flags"] = (r.stop_requested, r.shutdown)
        got["during"] = e.edit_sequence({"op": "delete", "at": len(e.blocks) - 2}, source="web")
        got["detail"] = e.reject_detail
        got["rev"] = e.seq.rev
        r.request_stop = orig
        orig()                                           # 실제 정지는 그대로 진행
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=dict(P1))), R.when(_main(e), stop_then_edit)])
    assert got["before"] is None and got["rev"] == 1                             # 정지 전 편집은 그대로 됨
    assert got["flags"] == (False, False) and got["during"] == "PROCESS_NOT_RUNNING" and "정지" in got["detail"]
    assert not e._stopping                                                       # 런이 정리된 뒤 풀림

"""CH2 히터 래더·열 모델 시나리오 — v1 에뮬레이터 시험(tests/heater_ladder_sim.py 하니스)의 입력·기대값(v1 import 없음).

시험대: FastHeaterRig(v2/ch12/sim/heater_rig.py — plc_ladder.HeaterLadder + HeaterDriver + HeaterEngine, 가상 시계·1초 스캔).
옮긴 v1 시험: tests/test_heater_sim.py(13), tests/test_heater_response.py 에뮬레이터(9), tests/test_heater_clamp_loss.py 에뮬레이터(10).
"""
from __future__ import annotations

import csv
import re

import pytest

from v2.ch12.heater_process import HeaterSoakWaiter
from v2.ch12.sim.heater_rig import FastHeaterRig


def _rig(**kw):
    r = FastHeaterRig(**kw)
    r.run(5)
    return r


# ═════════════════════ tests/test_heater_sim.py ═════════════════════
def test_reachable_200_hold_no_false_saturation():
    r = _rig()
    assert r.call(r.eng.turn_on, 200.0)[0]
    r.run(3600)
    assert r.eng.hold.is_holding() and r.plc.D[118] == r.eng.hold.value and 480 <= r.eng.hold.value <= 510
    assert not r.cards("포화") and not r.cards("정체")
    assert not r.log_texts("DAC 출력 포화") and not r.log_texts("승온 정체") and abs(r.plc.T - 200.0) <= 2.0


def test_unreachable_300_no_clamp_one_stall_card():
    r = _rig()
    r.call(r.eng.turn_on, 300.0)
    seen = {"d118": set(), "pv_max": 0.0}

    def step(rr):
        seen["d118"].add(rr.plc.D[118])
        seen["pv_max"] = max(seen["pv_max"], rr.plc.D[110] / 10.0)
    r.run(5400, on_step=step)
    assert seen["d118"] == {520} and r.plc.D[110] / 10.0 >= seen["pv_max"] - 1.0
    assert len(r.cards("정체")) == 1 and not r.cards("포화")


def test_external_run_ladder_ramp_no_clamp():
    r = _rig()
    r.plc.D[112] = 2200
    r.plc.M[640] = True
    seen = set()
    r.run(2400, on_step=lambda rr: seen.add(rr.plc.D[118]))
    assert seen == {520} and not r.cards("포화") and r.plc.D[110] / 10.0 >= 214.0


def test_post_arrival_windup_still_clamped_when_hold_off():
    r = _rig(hold_mode="off")
    r.call(r.eng.turn_on, 200.0)
    r.run(2400)
    r.plc.tc_offset = -30.0
    r.run(1200)
    assert len(r.cards("포화")) == 1 and 485 <= r.plc.D[118] <= 505 and abs(r.plc.T - 200.0) <= 5.0


def test_barely_reachable_248_no_clamp():
    r = _rig()
    r.call(r.eng.turn_on, 248.0)
    seen = set()
    r.run(7200, on_step=lambda rr: seen.add(rr.plc.D[118]))
    assert seen == {520} and not r.cards("포화") and r.plc.T >= 247.0 and len(r.cards("유지 모드 포기")) == 1


def test_dac_hold_shutter_offset_info_only():
    r = _rig()
    r.call(r.eng.turn_on, 200.0)
    r.run(2200)
    assert r.eng.hold.is_holding()
    n = len(r.cards())
    r.plc.tc_offset = -80.0
    r.run(1800)
    assert r.eng.hold.is_holding() and len(r.cards()) == n and len(r.log_texts("유지 중 PV")) == 1
    assert abs(r.plc.T - 200.0) <= 3.0


def test_plc_restart_end_reason():
    r = _rig()
    r.call(r.eng.turn_on, 150.0)
    r.run(300)
    r.plc.connected = False
    r.run(20)
    r.plc.restart(keep_temp=True)
    r.plc.connected = True
    r.run(10)
    ends = r.cards("히터 종료")
    assert len(ends) == 1 and "재기동" in ends[0][2]
    assert r.plc.D[160] == r.eng._marker and r.plc.D[113] == 5000


@pytest.mark.parametrize("attr,label", [("water_ok", "냉각수"), ("vac_ok", "진공(메인밸브 인터락)")])
def test_interlock_loss_stops_run_with_cause(attr, label):
    r = _rig()
    st = r.eng.status()
    assert st["water_ok"] and st["vac_ok"] and st["itl"]
    assert r.call(r.eng.turn_on, 150.0)[0]
    r.run(60)
    assert r.plc.M[640] and r.eng.status()["run"]
    setattr(r.plc, attr, False)
    r.run(3)
    st = r.eng.status()
    assert not r.plc.M[640] and not st["run"] and not st["itl"] and not st[attr]
    assert r.plc.D[141] == 0 and not r.plc.M[652]
    ends = r.cards("히터 종료")
    assert len(ends) == 1 and ends[0][2] == f"수동 · 인터락 해제({label}) — PLC 가 RUN 을 끔" and ends[0][3] == "FAIL"
    assert r.log_texts(f"운전 종료 — 사유 인터락 해제({label}) — PLC 가 RUN 을 끔")
    assert r.eng.owner is None and r.eng.is_ready() == (False, f"인터락 미충족: {label}")
    setattr(r.plc, attr, True)
    r.run(10)
    assert r.eng.status()["itl"] and not r.plc.M[640] and not r.eng.status()["run"]
    assert r.eng.is_ready()[0] and len(r.cards("히터 종료")) == 1


def test_interlock_loss_recorded_in_csv(tmp_path):
    from v2.ch12.heater_profile import HeaterCfg, cfg_to_dict
    from v2.ch12.outputs.heater_csv import HeaterCsv
    cfg = cfg_to_dict(HeaterCfg(enabled=True, log_enabled=True, log_dir=str(tmp_path)))
    r = FastHeaterRig(cfg=cfg)
    sink = HeaterCsv(cfg, monotonic=r.clk.monotonic)
    r.eng.csv = sink
    r.run(5)
    assert r.call(r.eng.turn_on, 150.0)[0]
    r.run(30)
    r.plc.vac_ok = False
    r.run(3)
    sink.shutdown()
    files = sorted(tmp_path.glob("CH2_HEATER_*"))
    assert len(files) == 1
    with open(files[0], encoding="utf-8-sig", newline="") as fp:
        rows = list(csv.DictReader(fp))
    assert rows[0]["water_ok"] == "1" and rows[0]["vac_ok"] == "1"
    last = rows[-1]
    assert last["note"] == "운전 종료" and last["itl"] == "0" and last["water_ok"] == "1" and last["vac_ok"] == "0"


def test_brief_interlock_drop_between_polls():
    r = _rig()
    assert r.call(r.eng.turn_on, 150.0)[0]
    r.run(30)
    r.plc.vac_ok = False
    r.plc.scan(0.0)
    r.plc.vac_ok = True
    r.run(3)
    st = r.eng.status()
    assert not st["run"] and st["itl"]
    ends = r.cards("히터 종료")
    assert len(ends) == 1 and ends[0][2] == "수동 · 외부 OFF 또는 순간 인터락 해제(이 프로그램이 끄지 않음)" and ends[0][3] == "FAIL"


def test_interlock_off_before_start_blocks_turn_on():
    r = FastHeaterRig()
    r.plc.vac_ok = False
    r.plc.water_ok = False
    r.run(5)
    ok, why = r.call(r.eng.turn_on, 150.0)
    assert not ok and "냉각수, 진공(메인밸브 인터락)" in why and not r.plc.M[640]


def test_ladder_h0_accepts_900_950_limits():
    r = FastHeaterRig()
    r.plc.allowed_regs.update({113, 126})
    r.plc.io_write_reg(None, 113, 9000)
    r.plc.io_write_reg(None, 126, 9500)
    assert (r.plc.D[113], r.plc.D[126]) == (9000, 9500)


# ═════════════════════ tests/test_heater_response.py 에뮬레이터(EL-092) ═════════════════════
def _nr(gain, **cfg):
    for k, v in (("max_temp", 900.0), ("sv_limit_c", 900.0), ("ot_limit_c", 950.0)):
        cfg.setdefault(k, v)
    return _rig(gain=gain, mv_limit=1200, mv_abs_max=1300, ramp_rate=6.0, hold_mode="off", **cfg)


def _start(r, target):
    pub = []
    r.eng.add_listener(lambda st: pub.append(st.get("noresp")))
    assert r.call(r.eng.turn_on, target)[0]
    return r.clk.t, pub


def test_sim_dead_output_cold_start_turns_off():
    r = _nr(0.0)
    t0, pub = _start(r, 200.0)
    r.run(900)
    assert not r.plc.M[640] and not r.eng.status()["run"]
    trip = [t for t, m in r.logs("히터 출력 무응답:")]
    assert len(trip) == 1 and 600 <= trip[0] - t0 <= 720
    c = r.cards("출력 무응답")
    assert len(c) == 1 and c[0][3] == "FAIL"
    end = r.cards("히터 종료")
    assert len(end) == 1 and end[0][2].endswith("· 출력 무응답") and end[0][3] == "FAIL"
    hits = [x for x in pub if x]
    assert len(hits) == 1 and hits[0].startswith("출력 무응답 — MV ") and r.eng.owner is None


@pytest.mark.parametrize("target,secs", [(200.0, 3600), (450.0, 7200)])
def test_sim_normal_heating_no_trip(target, secs):
    r = _nr(700.0)
    _t, pub = _start(r, target)
    r.run(secs)
    assert r.plc.M[640] and abs(r.plc.D[110] / 10.0 - target) <= 2.0
    assert not [x for x in pub if x] and not r.cards("출력")


def test_sim_unreachable_plateau_stall_only():
    r = _nr(700.0)
    _t, pub = _start(r, 600.0)
    r.run(9000)
    assert r.plc.M[640] and not [x for x in pub if x]
    assert len(r.cards("승온 정체")) == 1 and not r.cards("출력")


@pytest.mark.parametrize("target", [250.0, 300.0, 350.0])
def test_sim_hot_restart_working_no_trip(target):
    r = _nr(700.0)
    _t, pub = _start(r, 300.0)
    r.run(4000)
    r.call(r.eng.turn_off, "정지(패널)", owner="manual")
    r.run(300)
    assert r.call(r.eng.turn_on, target)[0]
    r.run(2400)
    assert r.plc.M[640] and not [x for x in pub if x] and not r.cards("출력")


@pytest.mark.parametrize("target,loss_at", [(200.0, 3000), (450.0, 2000)])
def test_sim_output_loss_turns_off(target, loss_at):
    r = _nr(700.0)
    _t, pub = _start(r, target)
    r.run(loss_at)
    assert r.plc.M[640]
    r.plc.gain = 0.0
    t1 = r.clk.t
    r.run(900)
    trip = [t for t, m in r.logs("히터 출력 끊김:")]
    assert len(trip) == 1 and trip[0] - t1 <= 600 and not r.plc.M[640]
    assert len(r.cards("출력 끊김")) == 1 and r.cards("히터 종료")[0][2].endswith("· 출력 끊김")
    assert [x for x in pub if x][0].startswith("출력 끊김 — 출력 상한(MV ")


# ═════════════════════ tests/test_heater_clamp_loss.py 에뮬레이터 ═════════════════════
def _reach(gain, target):
    """공정처럼: 히터 설정(클램프 꺼짐) → 도달 확인 → 300초 → HEATER_HOLD(유지 off → 포화 클램프 켬) → 120초."""
    r = _rig(gain=gain, mv_limit=1200, mv_abs_max=1300, ramp_rate=6.0, hold_mode="off",
             max_temp=900.0, sv_limit_c=900.0, ot_limit_c=950.0)
    pub = []
    r.eng.add_listener(lambda st: pub.append(st.get("noresp")))
    r.eng.take_over("process")
    assert r.call(r.eng.set_hold_armable, False, "공정이 작업 조건에서 유지값을 다시 잰다")[0]
    assert r.call(r.eng.turn_on, target, rate=12.0, owner="process")[0]
    w = HeaterSoakWaiter(target, tol=2.0, soak_sec=60, timeout_s=6 * 3600, clock=r.clk)
    res = None
    while res is None:
        r.run(1)
        res = w.tick(r.eng.status(), r.eng.profiler.is_active())
    assert res[0], res[1]
    r.run(300)
    assert r.call(r.eng.set_hold_armable, True, "")[0]
    r.run(120)
    return r, pub


def _trips(r, t0=0.0):
    return [(t - t0, m) for t, m in r.logs() if ("히터 출력 끊김:" in m or "히터 출력 무응답:" in m) and t >= t0]


def _clamps(r, t0=0.0):
    return [(t - t0, m) for t, m in r.logs("로 클램프") if t >= t0]


@pytest.mark.parametrize("gain,target,load", [(1400.0, 850.0, 0.2), (1400.0, 800.0, 0.2), (1400.0, 900.0, 0.1)])
def test_overload_near_capability_clamp_keeps_heater_on(gain, target, load):
    r, pub = _reach(gain, target)
    t0 = r.clk.t
    r.plc.gas_loss = load
    r.run(1800)
    assert _clamps(r, t0)
    assert r.eng.sat.clamped is not None and r.plc.D[118] == r.eng.sat.clamped
    assert not _trips(r, t0) and r.plc.M[640] and r.eng.status()["run"] and not [x for x in pub if x]
    sat = r.cards("포화")
    assert sat and sat[0][3] == "FAIL"


@pytest.mark.parametrize("gain,target", [(1400.0, 850.0), (1400.0, 700.0), (700.0, 200.0)])
def test_output_loss_after_arrival_still_turns_off(gain, target):
    r, pub = _reach(gain, target)
    t0 = r.clk.t
    r.plc.gain = 0.0
    r.run(600)
    tr = [(t, m) for t, m in _trips(r, t0) if "히터 출력 끊김:" in m]
    assert len(tr) == 1 and tr[0][0] <= 300
    cl = _clamps(r, t0)
    assert cl and cl[0][0] < tr[0][0] and not r.plc.M[640]
    assert [x for x in pub if x][0].startswith("출력 끊김 — 출력 상한(MV ")


def test_output_loss_with_comm_gap_before_clamp_still_turns_off():
    r, pub = _reach(1400.0, 850.0)
    t0 = r.clk.t
    r.plc.gain = 0.0

    def gap(rr):
        rr.plc.connected = not (60.0 <= rr.clk.t - t0 < 75.0)
    r.run(600, on_step=gap)
    tr = [t for t, m in _trips(r, t0) if "히터 출력 끊김:" in m]
    assert len(tr) == 1 and tr[0] <= 300 and not r.plc.M[640]
    assert [m for t, m in r.logs("하강 속도를 재는 중") if t >= t0]


def test_tc_step_detach_clamp_timing_unchanged():
    r, pub = _reach(1400.0, 400.0)
    t0 = r.clk.t
    top = [r.plc.T]

    def detach(rr):
        rr.plc.tc_offset = -(rr.plc.T - 25.0) * 0.5
        top[0] = max(top[0], rr.plc.T)
    detach(r)
    r.run(1800, on_step=detach)
    cl = _clamps(r, t0)
    assert cl
    sec = int(re.search(r"도달 후 (\d+)초째", cl[0][1]).group(1))
    assert sec <= r.eng.cfg.sat_sec + 2 and not _trips(r, t0) and r.plc.M[640]
    assert top[0] < 480.0 and r.plc.T < 420.0


def test_fast_cooling_at_limit_before_clamp_still_trips():
    r, pub = _reach(1400.0, 850.0)
    t0 = r.clk.t
    r.plc.gas_loss = 0.4
    r.run(900)
    assert [t for t, m in _trips(r, t0) if "히터 출력 끊김:" in m] and not r.plc.M[640]


def test_output_loss_after_clamp_released_still_trips():
    r, pub = _reach(1400.0, 850.0)
    r.plc.gas_loss = 0.2
    r.run(600)
    assert r.eng.sat.clamped is not None
    r.plc.gas_loss = 0.0
    assert r.call(r.eng.apply_target, 700.0, owner="process")[0]
    r.run(30)
    assert r.eng.sat.clamped is None and r.plc.D[118] == 1200
    r.run(3600)
    assert r.plc.M[640] and not _trips(r)
    r.plc.gain = 0.0
    t1 = r.clk.t
    r.run(600)
    assert [t for t, m in _trips(r, t1) if "히터 출력 끊김:" in m] and not r.plc.M[640]

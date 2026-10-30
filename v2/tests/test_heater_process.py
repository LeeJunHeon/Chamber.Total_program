"""CH2 히터 공정 연동 시험 — v1 런타임 히터 연동 시험의 입력·기대값(v1 import 없음, 근거 = v1 시험 파일:줄).

v1 의 ChamberRuntime(cb_heater_set·cb_heater_wait·cb_heater_hold·_on_heater_status·_heater_after_row) 이 하던 일을 v2 는
HeaterEngine(heater_link) 이 하고, 공정 결과(on_heater_set_ok·on_heater_ready·on_heater_hold_ok·on_heater_failed)는
process_results 의 (set_ok|ready|hold_ok|failed, ok, 문구, 코드) 로 낸다.
옮긴 v1 시험: tests/test_runtime_heater.py(12 — 정규화·검증은 v2/ch12/recipe 담당이라 제외), tests/test_heater_judge_exception.py(5),
tests/test_heater_response.py 런타임(3), tests/test_heater_process_sim.py(7), tests/test_heater_recipe_soak.py 런타임(5),
tests/test_heater_process_logs.py 상태 줄(2).
"""
from __future__ import annotations

import re

import pytest

from v2.ch12 import heater_link as HL
from v2.ch12.heater_link import next_row_uses_heater
from v2.ch12.heater_process import HeaterHoldWaiter, HeaterSoakWaiter
from v2.ch12.sim.heater_rig import FastHeaterRig

GAS_FILL_LOSS, WORK_LOSS, PLASMA_C = 0.05, 0.25, 20.0
REAL = dict(mv_limit=1200, mv_abs_max=1300, ramp_rate=6.0, max_temp=900.0, sv_limit_c=900.0, ot_limit_c=950.0,
            hold_mode="off")


class _Rec:
    """공정 결과 기록(v1 _PCRec)."""

    def __init__(self, r: FastHeaterRig):
        self.calls = []
        r.eng.on_process = lambda kind, ok, msg, code: self.calls.append((kind, msg, code))

    def names(self):
        return [c[0] for c in self.calls]

    def codes(self):
        return [c[2] for c in self.calls if c[0] == "failed"]

    def fails(self):
        return [(c[1], c[2]) for c in self.calls if c[0] == "failed"]


def _mk(**over):
    r = FastHeaterRig(**over)
    rec = _Rec(r)
    r.run(5)
    return r, rec


def _set(r, target=200.0, rate=None, secs=2):
    r.eng.heater_set(float(target), rate)
    r.run(secs)


def _until(r, cond, limit_s=20000):
    t0 = r.clk.t
    while not cond():
        r.run(1)
        if r.clk.t - t0 >= limit_s:
            return False
    return True


# ═════════════════════ tests/test_runtime_heater.py ═════════════════════
def test_next_row_lookahead():
    cases = [
        ([{"use_heater": "T"}, {"use_heater": "T"}], True),
        ([{"use_heater": "T"}, {"Process_name": "x"}], False),
        ([{"use_heater": "T"}, {"Process_name": "w", "delay": "20m"}, {"use_heater": "t"}], True),
        ([{"use_heater": "T"}, {"Process_name": "delay 10m"}, {"use_heater": "F"}], False),
        ([{"use_heater": "T"}, {"#": "test", "use_heater": "T"}], False),
        ([{"use_heater": "T"}], False),
    ]
    for q, want in cases:
        assert next_row_uses_heater(q, 0) is want, q


def _process_owned_running():
    r, rec = _mk(mv_limit=700)
    _set(r, 200.0, secs=30)
    assert r.eng.owner == "process" and r.eng.status()["run"]
    return r, rec


@pytest.mark.parametrize("ok,stopped,queue,next_rows,keep", [
    (True, False, True, [{"use_heater": "T"}], True),
    (True, False, True, [{"Process_name": "w", "delay": "5m"}, {"use_heater": "T"}], True),
    (True, False, True, [{"Process_name": "x"}], False),
    (True, False, True, [], False),
    (False, False, True, [{"use_heater": "T"}], False),
    (False, True, True, [{"use_heater": "T"}], False),
    (True, False, False, [{"use_heater": "T"}], False),
])
def test_after_row_keep_or_off(ok, stopped, queue, next_rows, keep):
    r, rec = _process_owned_running()
    q = [{"use_heater": "T"}] + next_rows
    r.eng.after_row(ok=ok, stopped=stopped, queue_mode=queue, next_uses_heater=next_row_uses_heater(q, 0))
    r.run(3)
    if keep:
        assert r.eng.status()["run"] and r.eng.owner == "process" and r.log_texts("히터 유지")
    else:
        assert not r.eng.status()["run"] and r.eng.owner is None and r.log_texts("공정이 히터를 끕니다 — ")


def test_after_row_ignores_manual_heater():
    r, rec = _mk()
    assert r.call(r.eng.turn_on, 150.0, owner="manual")[0]
    r.run(5)
    r.eng.after_row(ok=False, stopped=False)
    r.run(3)
    assert r.eng.status()["run"] and r.eng.owner == "manual"


def test_queue_end_safety_net_turns_off():
    r, rec = _process_owned_running()
    r.eng.queue_end()
    r.run(3)
    assert not r.eng.status()["run"] and r.eng.owner is None and r.log_texts("공정이 히터를 끕니다 — 공정 큐 종료")


def test_set_wait_hold_then_fault_in_process():
    r, rec = _mk(mv_limit=700)
    _set(r)
    assert rec.names() == ["set_ok"]
    assert r.eng.owner == "process" and not r.eng.hold.armable and not r.eng.sat.clamp_enabled
    r.eng.heater_wait(200.0)
    assert _until(r, lambda: "ready" in rec.names())
    assert not r.eng.hold.is_holding()
    r.run(300)
    r.plc.gas_loss, r.plc.plasma_heat = 0.25, 20.0
    r.run(30)
    r.eng.heater_hold()
    assert _until(r, lambda: "hold_ok" in rec.names(), 600)
    assert r.eng.hold.is_holding() and 500 <= r.eng.hold.value <= 512 and r.plc.D[118] == r.eng.hold.value
    assert r.eng._watch
    r.plc.M[644] = True
    r.run(3)
    assert rec.codes() == ["E832"] and not r.eng._watch


@pytest.mark.parametrize("attr,label", [("water_ok", "냉각수"), ("vac_ok", "진공(메인밸브 인터락)")])
def test_interlock_loss_in_process_e832_names_cause(attr, label):
    r, rec = _process_owned_running()
    assert r.eng._watch
    setattr(r.plc, attr, False)
    r.run(3)
    assert rec.codes() == ["E832"] and not r.eng._watch
    assert rec.fails()[0][0] == f"공정 중 히터 인터락 해제({label}) — 공정을 중단합니다"


def test_interlock_loss_during_wait_e830_names_cause():
    r, rec = _mk(mv_limit=700)
    _set(r)
    r.eng.heater_wait(200.0)
    r.run(60)
    assert rec.names() == ["set_ok"]
    r.plc.vac_ok = False
    r.run(3)
    assert rec.codes() == ["E830"] and rec.fails()[0][0] == "히터 인터락 해제(진공(메인밸브 인터락)) — PLC 가 히터를 끔"


def test_set_rejected_when_interlock_off():
    r = FastHeaterRig()
    rec = _Rec(r)
    r.plc.vac_ok = False
    r.run(5)
    r.eng.heater_set(200.0, None)
    assert rec.codes() == ["E830"] and rec.fails()[0][0] == "히터 사용 불가 — 인터락 미충족: 진공(메인밸브 인터락)"
    assert r.eng.owner is None and not r.plc.M[640]


def test_take_over_manual_run_uses_apply_target():
    r, rec = _mk(mv_limit=700)
    assert r.call(r.eng.turn_on, 150.0, owner="manual")[0]
    r.run(1200)
    assert r.eng.hold.is_holding()
    _set(r, 180.0, 6.0)
    assert rec.names() == ["set_ok"] and r.eng.owner == "process" and r.log_texts("넘겨받음")
    assert not r.eng.hold.is_holding() and r.plc.D[118] == 700
    assert r.eng.profiler.is_active() and abs(r.eng.profiler.rate() - 6.0) < 1e-9


def test_set_failures_report_e830_and_release_owner():
    r, rec = _mk()
    r.plc.M[644] = True
    r.run(2)
    r.eng.heater_set(200.0, None)
    assert rec.codes() == ["E830"] and r.eng.owner is None and not r.eng.status()["run"]
    r2 = FastHeaterRig()
    rec2 = _Rec(r2)
    r2.plc.reject_regs = {112}                        # SV(D112) 쓰기 거부 → 켜기 실패(PLC 쓰기 실패)
    r2.run(5)
    r2.eng.heater_set(200.0, None)
    assert rec2.codes() == ["E830"] and rec2.fails()[0][0].startswith("히터 설정 실패 — ")
    assert r2.eng.owner is None and not r2.eng.status()["run"]


def test_cancel_during_set_does_not_touch_process():
    """v1: _heater_set 뒤 바로 취소 — 공정 결과는 버리고(정리는 after_row) 히터는 공정 소유로 남음."""
    r = FastHeaterRig()
    rec = _Rec(r)
    r.run(5)
    gen = r.eng._gen
    orig = r.eng._turn_on_locked

    def cancel_then_on(*a):
        r.eng.heater_cancel()                         # 설정 쓰기 도중 정지가 옴(세대 +1)
        return orig(*a)
    r.eng._turn_on_locked = cancel_then_on
    r.eng.heater_set(200.0, None)
    r.run(2)
    assert r.eng._gen == gen + 1 and rec.names() == [] and not r.eng._watch and r.eng.owner == "process"
    r.eng.after_row(ok=False, stopped=True)
    r.run(3)
    assert not r.eng.status()["run"] and r.eng.owner is None


@pytest.mark.parametrize("action,mode,want", [("abort", "timeout", "E833"), ("force", "timeout", "hold_ok_forced"),
                                              ("force", "give_up", "hold_ok_no_force"), ("abort", "give_up", "E833")])
def test_hold_failure_handling(action, mode, want):
    over = {"mv_limit": 700 if mode == "timeout" else 520, "hold_fail_action": action, "hold_wait_s": 120.0}
    if mode == "timeout":
        over["hold_enter_sec"] = 600.0
    r, rec = _mk(**over)
    r.eng.heater_set(200.0, None)
    r.eng.heater_wait(200.0)
    assert _until(r, lambda: "ready" in rec.names())
    r.run(300)
    r.plc.gas_loss = 0.25
    r.plc.plasma_heat = 20.0 if mode == "timeout" else 0.0
    r.run(120)
    r.eng.heater_hold()
    assert _until(r, lambda: ("hold_ok" in rec.names()) or rec.codes(), 400)
    if want == "E833":
        assert rec.codes() == ["E833"]
    elif want == "hold_ok_forced":
        assert "hold_ok" in rec.names() and r.eng.hold.is_holding()
        assert 500 <= r.eng.hold.value <= 512 and r.cards("강제 진입")
    else:
        assert "hold_ok" in rec.names() and not r.eng.hold.is_holding() and r.plc.D[118] == 520
        assert r.log_texts("상한이 곧 출력 제한")


def test_hold_mode_off_passes_and_rearms_guard():
    r, rec = _mk(hold_mode="off")
    r.eng.heater_set(150.0, None)
    assert not r.eng.sat.clamp_enabled
    r.eng.heater_hold()
    assert "hold_ok" in rec.names() and r.eng.sat.clamp_enabled
    assert r.log_texts("유지 모드 꺼짐(HEATER_HOLD_MODE=off) — 포화 감시만 다시 켜고 진행")


def test_process_started_without_heater_logs_once():
    r, rec = _mk()
    assert r.call(r.eng.turn_on, 150.0)[0]
    r.run(2)
    r.eng.process_started(uses_heater=False)
    r.eng.process_started(uses_heater=True)
    assert r.log_texts() .count("히터가 운전 중이지만 이 공정은 히터를 쓰지 않습니다(use_heater=F) — 히터는 그대로 둡니다") == 1


# ═════════════════════ tests/test_heater_judge_exception.py ═════════════════════
def _boom(*_a, **_k):
    raise RuntimeError("injected judge bug")


def _start(target=200.0, soak_s=None, **cfg):
    r, rec = _mk(mv_limit=700, **cfg)
    _set(r, target)
    assert rec.names() == ["set_ok"], rec.calls
    r.eng.heater_wait(float(target), soak_s)
    return r, rec


def _judge_err(r):
    return r.log_texts("히터 공정 판정 예외")


def test_exception_during_soak_fails_once_e830(monkeypatch):
    r, rec = _start(200.0, 600.0)
    assert _until(r, lambda: bool(r.log_texts("SOAK 10분 시작")), 3 * 3600) and not rec.fails()
    monkeypatch.setattr(HeaterSoakWaiter, "_tick_hold", _boom)
    assert _until(r, lambda: bool(rec.fails()), 30)
    r.run(60)
    assert rec.codes() == ["E830"] and "도달 대기 판정 중 프로그램 오류" in rec.fails()[0][0]
    assert r.eng._job is None and not r.eng._watch
    assert len(_judge_err(r)) == 1 and "Traceback" in _judge_err(r)[0] and not r.log_texts("상태 리스너 예외")


def test_exception_before_reach_fails_once_e830(monkeypatch):
    monkeypatch.setattr(HeaterSoakWaiter, "tick", _boom)
    r, rec = _start(200.0)
    assert _until(r, lambda: bool(rec.fails()), 30)
    r.run(30)
    assert rec.codes() == ["E830"] and len(_judge_err(r)) == 1


def test_exception_in_hold_wait_fails_once_e830(monkeypatch):
    r, rec = _start(150.0, hold_mode="dac")
    assert _until(r, lambda: "ready" in rec.names(), 3 * 3600)
    monkeypatch.setattr(HeaterHoldWaiter, "tick", _boom)
    r.eng.heater_hold()
    assert _until(r, lambda: bool(rec.fails()), 30)
    assert rec.fails()[0][1] == "E830" and "유지 대기 판정 중 프로그램 오류" in rec.fails()[0][0]
    assert len(rec.fails()) == 1 and r.eng._job is None and not r.eng._watch


def test_exception_in_watch_fails_once_e832(monkeypatch):
    r, rec = _start(150.0)
    assert _until(r, lambda: "ready" in rec.names(), 3 * 3600) and r.eng._watch
    monkeypatch.setattr(HL, "itl_missing_text", _boom)
    r.plc.vac_ok = False
    assert _until(r, lambda: bool(rec.fails()), 30)
    assert rec.fails()[0][1] == "E832" and "감시 중 프로그램 오류" in rec.fails()[0][0]
    r.run(10)
    assert len(rec.fails()) == 1 and not r.eng._watch and len(_judge_err(r)) == 1


def test_normal_soak_unchanged():
    r, rec = _start(200.0, 120.0)
    assert _until(r, lambda: "ready" in rec.names(), 3 * 3600)
    assert not rec.fails() and r.eng._watch and not _judge_err(r)


# ═════════════════════ tests/test_heater_response.py 런타임(EL-092) ═════════════════════
def test_runtime_noresp_during_wait_fails_once_with_cause():
    r, rec = _mk(gain=0.0, **{k: v for k, v in REAL.items() if k != "ramp_rate"}, ramp_rate=6.0)
    _set(r, 200.0)
    assert rec.names() == ["set_ok"] and r.eng._watch
    r.eng.heater_wait(200.0)
    r.run(900)
    fails = rec.fails()
    assert len(fails) == 1 and fails[0][1] == "E832"
    assert fails[0][0].startswith("공정 중 히터 출력 무응답 — MV ") and fails[0][0].endswith(" — 공정을 중단합니다")
    assert r.eng._job is None and not r.eng._watch and not r.plc.M[640]


def test_runtime_loss_while_watching_fails_once():
    r, rec = _mk(gain=700.0, **REAL)
    _set(r, 200.0, secs=3000)
    assert r.eng._watch and r.eng._job is None and r.plc.M[640]
    r.plc.gain = 0.0
    r.run(900)
    fails = rec.fails()
    assert len(fails) == 1 and fails[0][1] == "E832" and fails[0][0].startswith("공정 중 히터 출력 끊김 — ")
    assert not r.eng._watch and not r.plc.M[640]


def test_runtime_manual_heater_trip_does_not_touch_process():
    r, rec = _mk(gain=0.0, **REAL)
    assert r.call(r.eng.turn_on, 200.0, owner="manual")[0]
    r.run(900)
    assert not r.plc.M[640] and rec.calls == []


# ═════════════════════ tests/test_heater_process_sim.py ═════════════════════
def _heat(r, target=200.0, process=True):
    if process:
        r.eng.take_over("process")
        assert r.call(r.eng.set_hold_armable, False, "공정 승온")[0]
    assert r.call(r.eng.turn_on, target, rate=12.0, owner="process" if process else "manual")[0]
    w = HeaterSoakWaiter(target, tol=2.0, soak_sec=60, timeout_s=7200, clock=r.clk)
    res = None
    while res is None:
        r.run(1)
        res = w.tick(r.eng.status(), r.eng.profiler.is_active())
    assert res[0], res[1]


def _to_work_condition(r):
    r.run(300)
    r.plc.gas_loss = GAS_FILL_LOSS
    r.run(60)
    r.plc.gas_loss, r.plc.plasma_heat = WORK_LOSS, PLASMA_C
    r.run(30)


def _hold_wait(r, timeout_s=300):
    hw = HeaterHoldWaiter(timeout_s=timeout_s, give_up_seq0=r.eng.hold_snapshot()["give_up_seq"], clock=r.clk)
    res = None
    while res is None:
        r.run(1)
        res = hw.tick(r.eng.status(), r.eng.hold_snapshot())
    return res + (hw.fail_kind,)


def _deposit(r, tc_offset):
    tmax = [0.0]

    def step(rr):
        tmax[0] = max(tmax[0], rr.plc.T)
    r.run(300, on_step=step)
    r.plc.tc_offset = tc_offset
    r.run(1800, on_step=step)
    return tmax[0]


@pytest.mark.parametrize("tc_offset", [0.0, -100.0])
def test_capture_in_work_condition_keeps_temperature(tc_offset):
    r = FastHeaterRig(mv_limit=700)
    r.run(5)
    _heat(r)
    assert not r.eng.hold.is_holding()
    _to_work_condition(r)
    assert r.call(r.eng.set_hold_armable, True, "")[0]
    res = _hold_wait(r)
    assert res[0], res[1]
    assert 500 <= r.eng.hold.value <= 512 and r.plc.D[118] == r.eng.hold.value
    tmax = _deposit(r, tc_offset)
    assert abs(r.plc.T - 200.0) <= 1.5 and tmax <= 202.0


def test_vacuum_capture_starves_under_gas():
    r = FastHeaterRig(mv_limit=700)
    r.run(5)
    _heat(r, process=False)
    r.run(300)
    assert r.eng.hold.is_holding() and r.eng.hold.value <= 498
    _to_work_condition(r)
    _deposit(r, 0.0)
    assert r.plc.T <= 190.0


@pytest.mark.parametrize("gas_sec", [100, 200])
def test_gas_load_rise_not_clamped_and_guard_rearms(gas_sec):
    r = FastHeaterRig(mv_limit=520)
    r.run(5)
    _heat(r)
    r.run(300)
    r.plc.gas_loss, r.plc.plasma_heat = WORK_LOSS, 0.0
    d118 = []
    r.run(gas_sec, on_step=lambda rr: d118.append(rr.plc.D[118]))
    r.call(r.eng.set_hold_armable, True, "")
    r.run(600, on_step=lambda rr: d118.append(rr.plc.D[118]))
    assert min(d118) == 520 and len(r.log_texts("클램프하지 않음")) <= 1 and not r.log_texts("로 클램프")
    assert abs(r.plc.T - 200.0) <= 1.5


def test_hold_give_up_is_reported_once_after_start():
    r = FastHeaterRig(mv_limit=520)
    r.run(5)
    _heat(r)
    r.run(300)
    r.plc.gas_loss = WORK_LOSS
    r.run(200)
    r.call(r.eng.set_hold_armable, True, "")
    res = _hold_wait(r)
    assert res[0] is False and res[2] == "E833" and res[3] == "give_up" and "포기" in res[1]
    assert len(r.cards("유지 모드 포기")) == 1 and r.plc.D[118] == 520


def test_hold_timeout_then_force_uses_recent_average():
    r = FastHeaterRig(mv_limit=700, hold_enter_sec=600.0)
    r.run(5)
    _heat(r)
    _to_work_condition(r)
    r.run(90)
    r.call(r.eng.set_hold_armable, True, "")
    res = _hold_wait(r, timeout_s=120)
    assert res[0] is False and res[2] == "E833" and res[3] == "timeout" and "시간 초과" in res[1]
    ok, why = r.call(r.eng.force_hold, "유지 모드 진입 시간 초과")
    assert ok, why
    assert r.eng.hold.is_holding() and 500 <= r.eng.hold.value <= 512 and r.plc.D[118] == r.eng.hold.value
    assert len(r.cards("강제 진입")) == 1
    _deposit(r, -100.0)
    assert abs(r.plc.T - 200.0) <= 2.0


def test_disarm_releases_hold_and_run_off_restores():
    r = FastHeaterRig(mv_limit=700)
    r.run(5)
    _heat(r, process=False)
    r.run(300)
    assert r.eng.hold.is_holding()
    r.eng.take_over("process")
    assert r.log_texts("넘겨받음")
    r.call(r.eng.set_hold_armable, False, "공정이 작업 조건에서 다시 측정")
    assert not r.eng.hold.is_holding() and r.plc.D[118] == 700 and r.eng.hold.last_window_mv is None
    r.run(600)
    assert not r.eng.hold.is_holding() and r.plc.D[118] == 700
    ok, why = r.call(r.eng.turn_off, "정지", owner="manual")
    assert not ok and "공정" in why
    assert not r.call(r.eng.apply_target, 150.0, owner="manual")[0]
    assert r.call(r.eng.turn_off, "공정 종료", owner="process")[0]
    r.run(5)
    assert r.eng.owner is None and r.eng.hold.armable and r.eng.sat.clamp_enabled


def test_panel_can_still_stop_external_run():
    r = FastHeaterRig()
    r.run(5)
    r.plc.allowed_regs.add(112)
    r.plc.io_write_reg(None, 112, 1500)
    r.plc.io_write_coil(None, 640, True)
    r.run(3)
    assert r.eng.status()["run"] and r.eng.owner == "external"
    assert r.call(r.eng.panel_apply, "140")[0]              # 패널 [적용] 은 owner 없이(결정 D144)
    assert r.call(r.eng.panel_onoff, "140")[0]              # RUN 이면 끄기('정지(패널)', manual)
    r.run(3)
    assert not r.eng.status()["run"] and r.eng.owner is None


def test_panel_apply_changes_process_target_v1_anomaly():
    """v1 이상 동작 그대로(HEAT-114·결정 D144) — 공정 소유 운전 중에도 패널 [적용] 이 목표를 바꾼다(공정 잠금 안 걸림)."""
    r, rec = _process_owned_running()
    ok, why = r.call(r.eng.panel_apply, "180")
    assert ok, why
    assert r.eng.final_target() == 180.0
    assert not r.call(r.eng.apply_target, 170.0, owner="manual")[0]     # owner 를 주면 잠금
    assert r.eng.panel_target("abc") == (None, "목표 온도가 숫자가 아닙니다")
    assert r.eng.panel_target("") == (None, "목표 온도를 입력하십시오")
    assert r.eng.panel_target("901") == (None, "목표 범위 밖 — 0 초과 500.0°C 이하")


# ═════════════════════ tests/test_heater_recipe_soak.py 런타임 ═════════════════════
def _soak_heat(target, soak_s, **cfg):
    r, rec = _mk(**cfg)
    t0 = r.clk.t
    _set(r, target)
    assert rec.names() == ["set_ok"]
    r.eng.heater_wait(float(target), soak_s)
    return r, rec, t0


def _done(rec):
    return lambda: any(k in ("ready", "failed") for k in rec.names())


def _tlogs(r, word):
    return r.logs(word)


def test_400c_soak_60min_counts_time_after_arrival_with_heater_on():
    r, rec, t0 = _soak_heat(400.0, 3600.0, **REAL)
    r.eng.next_step_fn = None
    runs = []
    t_start = r.clk.t
    while not _done(rec)():
        r.run(1)
        runs.append(bool(r.eng.status().get("run")))
        assert r.clk.t - t_start < 4 * 3600
    assert rec.names() == ["set_ok", "ready"] and all(runs)
    st = r.eng.status()
    assert st["run"] and r.eng.owner == "process" and abs(st["pv"] - 400.0) <= 2.0
    (t_band, _m), = _tlogs(r, "히터 목표 도달 —")
    (t_hold, _m2), = _tlogs(r, "SOAK 60분 시작")
    (t_ok, m_ok), = _tlogs(r, "히터 준비 완료")
    assert 60 <= t_hold - t_band <= 61 and 3600 <= t_ok - t_hold <= 3601
    assert 50 * 60 <= t_band - t0 <= 80 * 60
    assert "SOAK 60분 끝" in m_ok and "SOAK 중 " in m_ok
    (_t, m_start), = _tlogs(r, "그 뒤 SOAK 60분(레시피 heater_soak")
    assert "도달 한도" in m_start
    prog = _tlogs(r, "히터 SOAK — ")
    assert len(prog) >= 55 and all(t_hold <= t <= t_ok for t, _ in prog) and "남은" in prog[0][1]
    reach = [c for c in r.cards_full() if c[0] == "[CH2] 히터 도달"]
    assert len(reach) == 1 and reach[0][2] == "SUCCESS"
    f = reach[0][3]
    assert list(f) == ["목표", "도달 PV", "승온 소요", "SOAK", "SOAK 중 PV"]
    assert f["SOAK"] == "60분 0초" and f["SOAK 중 PV"].endswith("°C")
    ramp = int(t_hold - (t0 + 2.0))                              # 대기 시작(_set 뒤 2 s) 기준 — v1 은 HEATER_WAIT 시각
    assert f["승온 소요"] == f"{ramp // 60}분 {ramp % 60}초"


def test_blank_soak_uses_setting_and_card_unchanged():
    r, rec, t0 = _soak_heat(200.0, None, mv_limit=700)
    assert _until(r, _done(rec), 3 * 3600)
    assert rec.names() == ["set_ok", "ready"] and r.eng.cfg.soak_time_s == 60
    (t_in, _m), = _tlogs(r, "히터 목표 도달 —")
    (t_ok, _m2), = _tlogs(r, "히터 준비 완료")
    assert 60 <= t_ok - t_in <= 61 and r.log_texts("±2°C 를 60초 유지하면 진행")
    reach = [c for c in r.cards_full() if c[0] == "[CH2] 히터 도달"]
    assert list(reach[0][3]) == ["목표", "도달 PV", "승온 소요"]


def _into_soak(soak_s):
    r, rec, t0 = _soak_heat(200.0, soak_s, mv_limit=700)
    word = f"SOAK {soak_s / 60:g}분 시작"
    assert _until(r, lambda: bool(_tlogs(r, word)), 3 * 3600)
    (t_hold, _m), = _tlogs(r, word)
    return r, rec, t_hold


def test_wobble_during_soak_is_recorded_not_restarted():
    r, rec, t_hold = _into_soak(600.0)
    r.run(300)
    r.plc.tc_offset = 5.0
    r.run(10)
    r.plc.tc_offset = 0.0
    assert _until(r, _done(rec), 3600)
    assert rec.names() == ["set_ok", "ready"]
    (t_ok, m_ok), = _tlogs(r, "히터 준비 완료")
    assert 600 <= t_ok - t_hold <= 601
    assert _tlogs(r, "목표 ±2°C 밖(SOAK 시간은 계속 셈") and "벗어났었음" in _tlogs(r, "히터 PV 범위 복귀")[0][1]
    assert "±2°C 밖" in m_ok


@pytest.mark.parametrize("fault,code", [("interlock", "E830"), ("output_loss", "E832")])
def test_heater_trouble_during_soak_still_stops(fault, code):
    r, rec, t_hold = _into_soak(3600.0)
    r.run(60)
    if fault == "interlock":
        r.plc.vac_ok = False
    else:
        r.plc.gain = 0.0
    assert _until(r, _done(rec), 1800)
    assert len(rec.fails()) == 1 and rec.fails()[0][1] == code
    assert not r.log_texts("히터 준비 완료") and r.clk.t - t_hold < 1800


# ═════════════════════ tests/test_heater_process_logs.py 상태 줄 ═════════════════════
def _status_lines(r):
    return [m for m in r.log_texts() if m.startswith("히터 상태 —")]


def test_runtime_status_lines_and_samples():
    r, rec = _mk()
    samples = []
    r.eng.on_sample = lambda pv, sv, mv, run: samples.append((pv, sv, mv, run))
    assert r.call(r.eng.turn_on, 150.0)[0]
    r.eng.set_process_flags(running=False, polling=False, log_open=True)
    r.run(30)
    assert _status_lines(r) == [] and samples == []
    r.eng.set_process_flags(running=True, polling=False, log_open=True)
    r.run(60)
    lines = _status_lines(r)
    assert 5 <= len(lines) <= 7 and samples == []
    assert re.fullmatch(r"히터 상태 — PV \d+\.\d°C · SV \d+\.\d°C · 목표 150\.0°C · MV \d+/520 · "
                        r"(램프|제어|유지\(DAC 상한 \d+\)) · 수동", lines[-1]), lines[-1]
    r.eng.set_process_flags(running=True, polling=True, log_open=True)
    r.run(30)
    assert 28 <= len(samples) <= 31 and all(s[1] == 150.0 for s in samples)
    r.eng.set_process_flags(running=True, polling=True, log_open=False)
    before = len(_status_lines(r))
    r.run(30)
    assert len(_status_lines(r)) == before
    r.plc.connected = False
    r.run(15)
    n1 = len(samples)
    r.run(10)
    assert len(samples) == n1


def test_runtime_status_lines_off_when_period_zero():
    r, rec = _mk(run_log_period_s=0.0)
    assert r.call(r.eng.turn_on, 150.0)[0]
    r.eng.set_process_flags(running=True, polling=False, log_open=True)
    r.run(60)
    assert _status_lines(r) == []

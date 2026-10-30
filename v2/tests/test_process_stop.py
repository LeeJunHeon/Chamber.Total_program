"""STOP·이상 중단·종료 절차 타이밍 — StepRunner(v1 ProcessController 실행 부분)를 가짜 장비로(가상 시계, 정확한 시각),
그리고 실제 시험대(Ch12Rig)에서 엔진 STOP·abort.

v1 기대값(controller/process_controller.py): 종료 절차 단계 사이 SHUTDOWN_STEP_GAP_MS 500, 일반 단계 확인 2500 ms(초과 로그 후
다음), 전원 OFF 단계 PC_POWER_OFF_TIMEOUT_MS 240000(초과 = 실패 기록 '[Shutdown - {ACTION} timeout (240s)]' 후 계속),
RF 연속 OFF = max(240000, 600/1×1000 + (10+60)×1000) = 670000 ms, 종료 절차 중에는 abort 무시, PLC 실패는 늘 중단,
MFC 실패는 지금 기다리는 명령일 때만, TEST 행 STOP 은 종료 절차 없이 STOP, 첫 실패만 대표 원인.
"""
import pytest

from v2.common.clock import VirtualClock
from v2.ch12.process_runner import StepRunner, rf_power_stop_timeout_ms, tokens_for
from v2.ch12.process_steps import ProcessStep as S, Uses, norm_plc_name, shutdown_steps, step_cfg
from v2.tests import proc_rig as R


class FakeActions:
    thickness = None

    def __init__(self, host, delays):
        self.h, self.delays = host, delays
        self.norm_plc = norm_plc_name
        self.sent = []

    def send(self, step, runner):
        self.sent.append((round(self.h.clock.monotonic(), 3), step.action, step.message))
        d = self.delays.get(step.action, self.delays.get(step.message, 0.1))
        if d is None:
            return
        if isinstance(d, str):                                         # "fail:…" — 실패를 돌려준다
            src, why = d.split(":", 1)
            c = self.h.clock
            c.start_thread(lambda: (c.wait_until(c.monotonic() + 0.1, None), runner.step_failed(src, why, code="E401")),
                           "fail")
            return
        for k, sp in tokens_for(step, norm_plc_name):
            c = self.h.clock
            c.start_thread(lambda k=k, sp=sp: (c.wait_until(c.monotonic() + d, None), runner.match(k, sp)), "ok")

    def polling_targets(self, active, params):
        return {"mfc": bool(active)}

    def apply_polling(self, active, targets):
        pass

    def cancel_waits(self, final=False):
        pass


class Host:
    def __init__(self, delays=None, cfg=None):
        self.clock = VirtualClock(start_wall=1_760_000_000.0)
        self.clock.register()
        self.lk, self.cfg, self.n = self.clock.cond, dict(cfg or {}), 1
        self.lines = []
        self.actions = FakeActions(self, delays or {})
        self.sc = step_cfg(self.cfg, 1)

    def log(self, src, msg):
        self.lines.append(msg)

    def state(self, text):
        pass

    def on_step(self, idx, step, sd):
        pass

    def shutdown_steps(self):
        return shutdown_steps(self.sc, Uses(dcp=True, gases={"AR": True}))


def _steps():
    return [S("PLC_CMD", "메인 밸브 열기", params=("MV", True, 1)),
            S("MFC_CMD", "Ch1(AR) GAS 공급 시작", params=("FLOW_ON", {"channel": 1})),
            S("DC_PULSE_START", "DC Pulse 설정 및 ON", value=60.0, params=(50, "DC")),
            S("DELAY", "Shutter Delay 1.0분", duration=60_000),
            S("DELAY", "메인 공정 진행 (5분)", duration=300_000, polling=True)]


def _go(h, steps=None, at=None, act=None):
    r = StepRunner(h)
    res = {}
    t = h.clock.start_thread(lambda: res.setdefault("r", r.run({"process_note": "T"}, steps or _steps())), "runner")
    if at is not None:
        h.clock.wait_until(h.clock.monotonic() + at, None)
        act(r)
    h.clock.wait_until(h.clock.monotonic() + 3600, lambda: "r" in res)
    t.join(2)
    h.clock.close()
    return r, res["r"]


def test_stop_during_main_runs_shutdown_with_gap_500ms():
    h = Host()
    r, res = _go(h, at=90.0, act=lambda r: r.request_stop())
    assert not res.ok and res.detail["result"] == "STOP" and res.detail["stopped"] is True
    sd = [x for x in r.trace if x[4]]
    assert [x[2] for x in sd] == [s.action for s in h.shutdown_steps()]
    gaps = [round(b[0] - a[0], 3) for a, b in zip(sd, sd[1:])]
    assert all(g == pytest.approx(0.6, abs=0.02) for g in gaps), gaps          # 확인 0.1 s + 간격 0.5 s
    assert "정지 요청을 받았습니다." in h.lines and "정지 요청 - 안전한 종료 절차를 시작합니다." in h.lines
    assert f"종료 절차 생성 완료: {len(sd)}단계" in h.lines
    assert res.detail["actual_shutter_delay_min"] == 1.0 and 0.4 < res.detail["actual_process_time_min"] < 0.6
    assert " 중단됨" in h.lines and "=== 공정이 중단되었습니다 ===" in h.lines       # v1: 카운트다운 문구가 먼저 지워짐
    assert r.eta_remaining_s() is None


def test_shutdown_step_without_confirm_waits_2500ms_then_next():
    h = Host(delays={"Ch1(AR) Flow Off": None, "전체 MFC Valve Open": None})
    r, res = _go(h, at=5.0, act=lambda r: r.request_stop())
    sd = [x for x in r.trace if x[4]]
    t = {x[3]: x[0] for x in sd}
    assert t["전체 MFC Valve Open"] - t["Ch1(AR) Flow Off"] == pytest.approx(3.0, abs=0.01)   # 2.5 s + 0.5 s
    assert h.lines.count("종료 스텝 확인 시간 초과 → 다음 스텝 진행") == 2
    assert res.detail["result"] == "STOP" and res.detail["errors"] == []


def test_power_off_timeout_240s_recorded_and_continues():
    h = Host(delays={"DC_PULSE_STOP": None})
    r, res = _go(h, at=5.0, act=lambda r: r.request_stop())
    sd = [x for x in r.trace if x[4]]
    i = next(k for k, x in enumerate(sd) if x[2] == "DC_PULSE_STOP")
    assert sd[i + 1][0] - sd[i][0] == pytest.approx(240.5, abs=0.01)
    assert res.detail["errors"] == ["Step 2 DC_PULSE_STOP: [Shutdown - DC_PULSE_STOP timeout (240s)]"]
    assert "경고: 종료 중 단계 실패 → 계속 진행 (DC_PULSE_STOP, 사유: [Shutdown - DC_PULSE_STOP timeout (240s)])" in h.lines


def test_rf_continuous_off_timeout_is_rampdown_plus_70s():
    from v2.ch12.sim.rig import base_cfg
    assert rf_power_stop_timeout_ms(base_cfg(), 2, 240000) == 670000.0
    assert rf_power_stop_timeout_ms({"RF_MAX_POWER": 0}, 2, 240000) == 240000.0


def test_plc_failure_always_aborts_and_first_error_kept():
    h = Host(delays={"PLC_CMD": "PLC:write failed", "메인 밸브 열기": "PLC:write failed"})
    r, res = _go(h)
    assert res.detail["result"] == "FAIL" and res.detail["error_code"] == "E401"
    assert res.detail["error_source"] == "PLC" and res.detail["error_detail"] == "write failed"
    assert res.detail["errors"][0] == "Step 1 PLC_CMD: [PLC - E401: write failed]"
    assert "오류 발생: [PLC - E401: write failed]. 종료 절차를 시작합니다." in h.lines


def test_mfc_failure_only_when_waited():
    h = Host(delays={"Ch1(AR) GAS 공급 시작": 30.0})
    holder = {}

    def act(r):
        r.mfc_failed("FLOW_OFF", "late fail")                                # 기다리는 명령 아님 → 무시
        holder["ok"] = r.is_running and not r.shutdown
        r.mfc_failed("FLOW_ON", "stab fail")                                 # 기다리는 중 → 중단
    r, res = _go(h, at=1.0, act=act)
    assert holder["ok"] is True
    assert "경고(무시): FLOW_OFF: late fail (현재 스텝과 무관)" in h.lines
    assert res.detail["error_source"] == "MFC" and res.detail["error_detail"] == "stab fail"


def test_power_off_failed_for_other_device_keeps_wait():
    h = Host(delays={"DC_PULSE_STOP": 5.0})

    def act(r):
        r.request_stop()
        h.clock.wait_until(h.clock.monotonic() + 1.0, None)          # 이제 DC_PULSE_STOP 대기 중
        r.power_off_failed("RF Power", "RF_OFF", "RF off 미확인")
    r, res = _go(h, at=5.0, act=act)
    assert any("경고: 종료 중 RF Power 실패 기록 — 현재 단계(DC_PULSE_STOP) 대기는 유지" in l for l in h.lines)
    sd = [x for x in r.trace if x[4]]
    i = next(k for k, x in enumerate(sd) if x[2] == "DC_PULSE_STOP")
    assert sd[i + 1][0] - sd[i][0] == pytest.approx(5.5, abs=0.01)  # 끊지 않고 확인(5 s) + 간격


def test_test_mode_stop_skips_shutdown():
    h = Host()
    r = StepRunner(h)
    res = {}
    steps = [S("DELAY", "TEST MODE (30s)", duration=30_000)]
    h.clock.start_thread(lambda: res.setdefault("r", r.run({"process_note": "T", "test_mode": True}, steps)), "runner")
    h.clock.wait_until(h.clock.monotonic() + 5, None)
    r.request_stop()
    h.clock.wait_until(h.clock.monotonic() + 60, lambda: "r" in res)
    h.clock.close()
    assert res["r"].detail["result"] == "STOP" and not [x for x in r.trace if x[4]]
    assert "[TEST MODE] STOP: 장비 종료 절차 스킵 (딜레이만 취소)" in h.lines
    assert "[TEST MODE] 정지 요청 감지 - 종료 절차 스킵" in h.lines


def test_abort_ignored_during_shutdown_and_second_stop_logged():
    h = Host(delays={"Ch1(AR) Flow Off": 2.0})

    def act(r):
        r.request_stop()
        h.clock.wait_until(h.clock.monotonic() + 1.5, None)
        r.request_stop()
    r, res = _go(h, at=5.0, act=act)
    assert "정지 요청: 이미 정지 처리 중입니다." in h.lines
    assert [x[2] for x in r.trace if x[4]] == [s.action for s in h.shutdown_steps()]


# ───────────── 시험대(실제 드라이버) ─────────────
def _rfp_params():
    return dict(base_pressure=1e-5, working_pressure=3, process_time=2.0, shutter_delay=0.1, use_ar=True, ar_flow=20,
                use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True, process_note="stop")


def test_engine_stop_during_main_closes_shutter_and_valves(tmp_path):
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    main = lambda: e.cur_inst is not None and e.cur_inst.type == "main" and e.runner.cd is not None   # noqa: E731
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=_rfp_params(), owner="robot")),
                                 R.when(main, lambda: e.stop(source="local"))])
    assert R.ends(e) == [("aborted", "USER_STOP")]
    s = rig.sims["plc"]
    assert not s.coil("MAIN_SHUTTER_1_SW") and not s.coil("MAIN_1_GAS_SW") and not s.coil("Ar_1_GAS_SW")
    assert R.has_log(e, "[Runner] STOP → process_controller.request_stop()")
    assert R.has_log(e, "[종료절차 1/") and R.has_log(e, "Main Shutter 닫기 (항상)")
    end = [x for x in e.events if x.kind == "run_end"][-1]
    assert end.data["result_detail"]["result"] == "STOP" and end.data["executed"][-1]["type"] == "main"


def test_engine_abort_from_safety_is_error_with_reason(tmp_path):
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    gas = lambda: e.cur_inst is not None and e.cur_inst.type == "pressure_ctrl"   # noqa: E731
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=_rfp_params(), owner="local")),
                                 R.when(gas, lambda: e.abort("S12_LINK_DOWN", "MFC 두절"))])
    assert R.ends(e) == [("error", "S12_LINK_DOWN")]
    assert R.has_log(e, "오류 발생: [SAFETY - S12_LINK_DOWN: MFC 두절]. 종료 절차를 시작합니다.")
    assert e.error == "S12_LINK_DOWN" and not rig.sims["plc"].coil("MAIN_1_GAS_SW")


def test_engine_stop_before_start_during_preflight(tmp_path):
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    rig.sims["plc"].gate[1] = 0.5                                              # 게이트 이동 중 → 재확인하는 동안 STOP
    R.run(rig, R.idle(e), max_ticks=60, hooks=[R.at(2, lambda: e.start(params=_rfp_params(), owner="local")),
                                                R.when(lambda: e.state_name == "PREFLIGHT", lambda: e.stop())])
    assert R.ends(e)[-1] in (("aborted", "USER_STOP"), ("error", "GATE_CHECK_FAILED"))
    assert not e.busy and rig.shared.pulse.owner("rfpulse", "ch1") is None

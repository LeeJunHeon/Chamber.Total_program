"""플라즈마 클리닝 순수 규칙·정지 경로 단위 시험 — v1 tests/test_pc_rf_stop.py·test_pc_start_flags.py 의 기대값(v1 import 없이).

- 강제 RF OFF(test_pc_rf_stop.py:126-274): 완료 신호면 강제 경로 없음(a), 강제 경로는 램프다운을 먼저 멈춘 뒤 0 W → SET OFF(b),
  0 W 계속 실패 → SET OFF 안 함·미확인 사유·알림 1회(재호출에도 1회)(c), 0 W 확인 뒤 SET OFF 예외는 경고만(d), 결과 반영 규칙(e).
- 시작 거절은 진행 중 런의 결과 플래그를 지우지 않고, 수락은 mark_started 시점에 초기화돼 있다(test_pc_start_flags.py:82-143).
- TEST 시간 정규식(PC-093)·행 → 화면 값(빈 칸 '0.0')·기본값·호스트 응답 코드 분류(host/handlers.py:989-1001)·쿨다운 규칙.
"""
import pytest

from v2.common.clock import VirtualClock
from v2.common.worker import Request, Result, Status
from v2.ch12 import pc_recipe as R
from v2.ch12.plasma_cleaning import PcDevices, PlasmaCleaning
from v2.ch12.pc_stop import apply_rf_off_unconfirmed
from v2.ch12.run_state import RunState

CFG = {"PC_RF_CLEANUP_TIMEOUT_S": 5.0, "PC_RF_WAIT_POWER_OFF_TIMEOUT_S": 15.0, "POWER_OFF_ZERO_DEADLINE_S": 10.0}


def _res(ok: bool, value=None):
    return Result(Request("write", lambda s: None), Status.OK if ok else Status.FAIL, value=value,
                  reason=None if ok else "DEVICE_NO_RESPONSE", error=None if ok else "PLCError E402: no response")


class FakePlc:
    def __init__(self, write_fail=False, set_off_fail=False):
        self.write_fail, self.set_off_fail = write_fail, set_off_fail
        self.calls = []

    def power_write(self, w, *, family="DCV", write_idx=1, kind="write", on_done=None):
        self.calls.append(("write" if not self.write_fail else "write_FAIL", float(w), kind))
        on_done(_res(not self.write_fail, 0))

    def power_enable(self, on, *, family="DCV", set_idx=1, kind="write", on_done=None):
        self.calls.append(("set" if not self.set_off_fail else "set_FAIL", bool(on), kind))
        on_done(_res(not self.set_off_fail))


class FakeRf:
    """RfPower 가 정지 정리에서 쓰는 속성만."""

    def __init__(self, clock, finish_ok=True, target=300.0):
        self.clock, self.finish_ok = clock, finish_ok
        self.ramping_down = self.running = True
        self.direct, self.target_power, self.enabled = True, target, True
        self.state, self.current_step, self.last_sent = "MAINTAINING", target, target
        self.polling, self._due, self._gen = True, 0.0, 0
        self._stop_done = []
        self.stops = 0
        self.order = []

    def p(self, name):
        return 100.0

    def stop(self, on_done=None):
        self.stops += 1
        if self.finish_ok:
            on_done(None)
        elif on_done:
            self._stop_done.append(on_done)


def eng_with(plc=None, rf=None, cfg=None):
    c = VirtualClock()
    c.register()
    e = PlasmaCleaning(c, dict(CFG, **(cfg or {})), PcDevices(plc, None, rf, {}, {}))
    e.rest = 0
    e.shutdown_rest_devices = lambda: setattr(e, "rest", e.rest + 1)
    return c, e


def alerts(e):
    return [x.data for x in e.events if x.kind == "card" and x.data.get("api") == "notify_error_with_src"]


def test_a_power_off_ok_no_force_path():
    c, e = eng_with(FakePlc(), None)
    e.dev.rf = FakeRf(c, finish_ok=True)
    e.safe_rf_stop()
    assert e.dev.plc.calls == [] and e.rf_off_unconfirmed_reason is None and alerts(e) == [] and e.rest == 1
    assert c.monotonic() < 0.01


def test_b_force_path_stops_rampdown_first_then_zero_then_set_off():
    c, e = eng_with(FakePlc(), None)
    rf = e.dev.rf = FakeRf(c, finish_ok=False, target=300.0)            # kick 램프다운 — cleanup 은 바로 돌아옴
    seen = []
    orig = e.dev.plc.power_write

    def pw(*a, **k):
        seen.append((rf.ramping_down, rf.polling))                     # 0 W 를 쓸 때 램프다운은 이미 멈춤
        return orig(*a, **k)
    e.dev.plc.power_write = pw
    e.safe_rf_stop()
    assert 14.9 <= c.monotonic() <= 15.1                                # 5 s cleanup 문구 없음(램프다운 경로) + 15 s
    assert seen == [(False, False)]
    assert [x[:2] for x in e.dev.plc.calls] == [("write", 0.0), ("set", False)]
    assert all(x[2] == "safety" for x in e.dev.plc.calls)                # 램프다운 쓰기와 같은 줄(FIFO)
    assert (rf.state, rf.running, rf.enabled, rf.target_power, rf.last_sent) == ("IDLE", False, False, 0.0, 0.0)
    assert rf._stop_done == [] and e.rest == 1
    assert not any("cleanup timeout" in ln for ln in e.ui_log)
    assert any(ln.endswith("RF: ramp-down 완료 신호 timeout → 강제 0W/SET OFF") for ln in e.ui_log)


def test_b2_direct_off_path_logs_cleanup_timeout():
    c, e = eng_with(FakePlc(), None)
    e.dev.rf = FakeRf(c, finish_ok=False, target=100.0)
    e.dev.rf.ramping_down = False
    e.safe_rf_stop()
    assert any(ln.endswith("RF: cleanup timeout(5.0s) → 계속 진행") for ln in e.ui_log)
    assert 19.9 <= c.monotonic() <= 20.1


def test_c_force_zero_always_fails_alert_once():
    c, e = eng_with(FakePlc(write_fail=True), None)
    e.ch = 1
    assert e.force_rf_zero_then_set_off() is False
    assert not any(x[0].startswith("set") for x in e.dev.plc.calls)      # 0 W 미확인 → SET OFF 안 함
    n = len(e.dev.plc.calls)
    assert n == 21 and 10.0 <= c.monotonic() <= 10.1                    # 0.5 s 간격, 10 s 마감
    assert e.rf_off_unconfirmed_reason.startswith("강제 0W 쓰기 실패 (10.0초, 21회, ")
    a = alerts(e)
    assert len(a) == 1 and a[0]["src"] == "PC" and "출력 상태 미확인" in a[0]["text"]
    assert a[0]["text"] == ("⚠️ CH1 Plasma Cleaning RF Power OFF 실패 - 출력 상태 미확인 (켜져 있을 가능성 있음). "
                            "RF 전원 출력과 PLC D/A(DCV_SET_1/DCV_WRITE_1)를 즉시 확인하세요.")
    e.force_rf_zero_then_set_off()
    assert len(alerts(e)) == 1                                          # 런당 1회


def test_c2_no_plc():
    c, e = eng_with(None, None)
    e.dev.rf = FakeRf(c, finish_ok=False)
    e.safe_rf_stop()
    assert e.rf_off_unconfirmed_reason == "PLC 없음 — 강제 0W 를 보낼 수 없음" and len(alerts(e)) == 1


def test_d_zero_ok_set_off_fails_is_warning_only():
    c, e = eng_with(FakePlc(set_off_fail=True), None)
    assert e.force_rf_zero_then_set_off() is True
    assert [x[0] for x in e.dev.plc.calls] == ["write", "set_FAIL", "set_FAIL", "set_FAIL"]
    assert 0.39 <= c.monotonic() <= 0.41                                # 0.2 s 간격 3회
    assert e.rf_off_unconfirmed_reason is None and alerts(e) == []
    assert any("RF SET OFF 실패 — 0W 는 확인됨 (" in ln for ln in e.ui_log)


def test_e_apply_rf_off_unconfirmed():
    assert apply_rf_off_unconfirmed(True, False, None, "사유") == (False, False, "사유")
    assert apply_rf_off_unconfirmed(False, False, "원래", "사유") == (False, False, "원래")
    assert apply_rf_off_unconfirmed(False, True, "STOP", "사유") == (False, True, "STOP")
    assert apply_rf_off_unconfirmed(True, False, None, None) == (True, False, None)
    assert apply_rf_off_unconfirmed(True, True, None, "사유") == (False, False, "사유")


# ───────────────────────── 시작 플래그(TEST-112) ─────────────────────────
LIVE = {"cleanup_started": True, "forced_fail": True, "forced_fail_reason": "RF OFF 미확인",
        "rf_off_unconfirmed_reason": "강제 0W 쓰기 실패 (10.0초, 21회)"}


@pytest.mark.parametrize("name,setup,word", [
    ("이미 실행 중", lambda e, rs: setattr(e, "running", True), "이미 Plasma Cleaning이 실행 중"),
    ("쿨다운", lambda e, rs: rs.mark_finished("pc", 1), "1분 대기 필요"),
    ("같은 CH PC 실행 중", lambda e, rs: rs._running["pc"].__setitem__(1, True), "Plasma Cleaning이 이미 실행 중"),
    ("같은 CH 챔버 실행 중", lambda e, rs: rs._running["chamber"].__setitem__(1, True), "다른 공정이 실행 중"),
])
def test_rejected_start_keeps_live_flags(name, setup, word):
    c, e = eng_with()
    for k, v in LIVE.items():
        setattr(e, k, v)
    setup(e, e.run_state)
    got = []
    res = e.start(on_preflight=lambda *a: got.append(a))
    assert not res.accepted and got and got[0][0] is False and word in got[0][1], (name, got)
    assert {k: getattr(e, k) for k in LIVE} == LIVE, name


def test_accepted_start_resets_flags_before_mark_started():
    c, e = eng_with()
    for k, v in LIVE.items():
        setattr(e, k, v)
    seen = {}

    class Stop(BaseException):
        pass

    class RS(RunState):
        def mark_started(self, kind, channel=None):
            seen.update({k: getattr(e, k) for k in LIVE})
            raise Stop()
    e.run_state = RS(c)
    with pytest.raises(Stop):
        e.start()
    assert seen == {"cleanup_started": False, "forced_fail": False, "forced_fail_reason": None,
                    "rf_off_unconfirmed_reason": None}


# ───────────────────────── 레시피·TEST·응답 코드 ─────────────────────────
def test_parse_duration_and_test_mode():
    p = R.parse_duration_seconds
    assert (p("1h2m3s"), p("10m"), p("1.5m"), p("30s"), p("1h"), p("")) == (3723.0, 600.0, 90.0, 30.0, 3600.0, 0.0)
    assert p("5") == 0.0 and p("1.5") == 0.0                            # [이상] 단위 없는 숫자 → 0(PC-093)
    assert R.test_mode_of({"#": " Test ", "time": "5"}) == (True, 60.0)
    assert R.test_mode_of({"#": "test", "TIME": "2m"}) == (True, 120.0)
    assert R.test_mode_of({"#": "test", "Time": "10s"}) == (True, 10.0)
    assert R.test_mode_of({"#": "PC1", "time": "10m"}) == (False, 0.0)
    assert R.test_mode_of(None) == (False, 0.0)


def test_apply_row_and_read_params_defaults():
    ui = R.blank_ui()
    assert R.read_params(ui, {}).as_dict() == {"gas_idx": 3, "gas_flow_sccm": 0.0, "target_pressure": 5e-06,
                                               "tol_mTorr": 0.2, "wait_timeout_s": 90.0, "sp4_setpoint_mTorr": 2.0,
                                               "rf_power_w": 100.0, "process_time_min": 1.0,
                                               "gv_open_lamp_delay_s": 5.0, "ig_interval_ms": 10000}
    ch = R.apply_row(ui, {"use_ch": "2.0", "Process_name": " P1 ", "base_pressure": "5.00E-06", "rf_power": ""})
    assert ch == 2 and ui == {"process_name": "P1", "base_pressure": "5.00E-06", "gas_flow": "0.0",
                              "working_pressure": "0.0", "rf_power": "0.0", "process_time": "0.0"}
    p = R.read_params(ui, {})
    assert (p.rf_power_w, p.process_time_min, p.target_pressure) == (0.0, 0.0, 5e-06)   # 빈 칸 → 0(기본값 아님)
    assert R.apply_row(R.blank_ui(), {"use_ch": "abc"}) == 1 and R.apply_row(R.blank_ui(), {}) is None
    ui2 = dict(R.blank_ui(), rf_power="x", gas_flow=" 12.5 ")
    p2 = R.read_params(ui2, {"PC_DEFAULT_RF_POWER_W": 150.0})
    assert (p2.rf_power_w, p2.gas_flow_sccm) == (150.0, 12.5)


def test_host_error_code_classification():
    assert R.host_error_code("이전 공정 종료 후 1분 대기 필요합니다. 약 3초 후에 다시 시도하십시오.", None) == "E700"
    assert R.host_error_code("preflight timeout(20.0s) (…)", None) == "E700"
    assert R.host_error_code("MFC Cooldown", None) == "E700"
    assert R.host_error_code("게이트밸브 인터락 상태 확인 실패: x", None) == "E511"
    assert R.host_error_code("아무거나", "E302") == "E302"


def test_first_row_and_count(tmp_path):
    f = tmp_path / "a.csv"
    f.write_text("#,time,rf_power\r\n,,\r\n test ,10s, 100 \r\n,,\r\nPC,1m,50\r\n", encoding="utf-8-sig")
    assert R.read_first_row(str(f)) == {"#": "test", "time": "10s", "rf_power": "100"}
    assert R.count_data_rows(str(f)) == 2
    assert R.first_row_of([{"a": " "}, {"a": "1 ", " b": " 2"}]) == {"a": "1", "b": "2"}
    g = tmp_path / "hdr.csv"
    g.write_text("a,b\r\n", encoding="utf-8-sig")
    assert R.load_host_csv(str(g)) == (None, 1)


def test_run_state_block_rules():
    c = VirtualClock()
    c.register()
    rs = RunState(c)
    assert rs.pc_block_reason(1) == (True, 0.0, "")
    rs.mark_finished("tsp", 0)
    assert rs.pc_block_reason(1)[0] is False and rs.pc_block_reason(2)[0] is True
    c.advance(30)
    ok, rem, why = rs.pc_block_reason(1)
    assert (ok, round(rem), why) == (False, 30, "이전 공정 종료 후 1분 대기 필요: TSP 30s")
    rs.mark_started("pc", 2)
    assert rs.chamber_block_reason(1) == (False, 0.0, "Plasma Cleaning 실행 중")
    rs.mark_finished("pc", 2)
    assert rs.chamber_block_reason(2)[2] == "쿨다운 대기: PC 60s"
    rs.set_error("pc", 1, "x")
    rs.mark_started("pc", 1)
    assert rs.last_error("pc", 1) is None                               # 새 런이 직전 에러 해제
    rs.begin_run_use("pc2", ["MFC1", "MFC2"])
    rs.begin_run_use("chamber1", ["MFC1"])
    assert rs.run_users("MFC1") == {"pc2", "chamber1"} and rs.run_users("MFC2") == {"pc2"}
    rs.end_run_use("pc2")
    assert rs.snapshot()["run_users"] == {"chamber1": ["MFC1"]}

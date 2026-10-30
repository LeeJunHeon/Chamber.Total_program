"""공정 리스트(파일 레시피 행 여러 개) — delay 행·TEST 행·행 사이 쿨다운 60 s·리스트 취소·큐 연속 실행 시 히터 유지·ETA.

v1 기대값(runtime/chamber_runtime.py _runner_stage_advance_queue 5044-5244·AFTER_FINISH 4977-5041, host/handlers.py:477-528):
- delay 행: '[Runner] '{이름}' 단계: {label} 대기 시작' → 1초마다 '지연 대기 중: {label} · 남은 시간 HH:MM:SS' → '… 지연 완료 → 다음 스텝',
  장비 명령·새 로그 없음.
- TEST 행: '[TEST MODE] '{note}' 장비 제어 스킵 / X.Xs 시뮬레이션', DELAY 1단계, 끝나면 장비 정리.
- 행 사이 쿨다운: 첫 행이 아니면 남은 쿨다운(자기 챔버의 마지막 종료 + 60 s)만큼 '[Runner] cooldown wait: X.Xs (쿨다운 대기)'.
- STOP(request_stop_all): 대기 행·쿨다운·큐 자동 진행까지 멈춘다 — 다음 행을 시작하지 않고 큐를 비운다.
- 다음 '공정' 행(delay 건너뜀)이 use_heater=T 면 행 사이 히터 유지(after_row next_uses_heater=True).
- min_total_s = Σ 행 하한 + 90(파일 레시피일 때만), eta.CHn.remaining_s 는 큐의 마지막 행이 아니면 null.
"""
from v2.tests import proc_rig as R

PROC = {"Process_name": "A", "Ar": "T", "Ar_flow": "20", "base_pressure": "1e-5", "working_pressure": "3",
        "shutter_delay": "0.05", "process_time": "0.05", "use_rf_pulse": "T", "rf_pulse_power": "120",
        "rf_pulse_freq": "0.5", "rf_pulse_duty_cycle": "40", "main_shutter": "T"}


def _starts(e):
    return [x for x in e.events if x.kind == "run_start"]


def test_delay_test_and_cooldown_rows(tmp_path):
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    rows = [{"Process_name": "warm", "delay": "5s"}, {"#": "test", "Process_name": "T1", "time": "10s"},
            dict(PROC, Process_name="A"), dict(PROC, Process_name="B")]
    assert e.load(rows, source="local") is None
    seen = {"delay": set(), "cool": set(), "eta": []}

    def watch(t):
        s = e.state_text
        if s.startswith("지연 대기 중"):
            seen["delay"].add(s)
        if s.startswith("다음 공정 대기중"):
            seen["cool"].add(s)
        if e.busy:
            seen["eta"].append((e.run_idx, e.eta_remaining_s()))
    mt = {}
    R.run(rig, R.idle(e), max_ticks=1200,
          hooks=[R.at(2, lambda: (mt.setdefault("v", e.min_total_s()), e.start(source="local", owner="local"))), watch])
    assert mt["v"] == round(5 + 10 * 0 + (0.05 + 0.05) * 60 * 2 + 90)            # delay 5 + TEST(time 열은 ETA 에 없음) + 6×2 + 90
    assert R.has_log(e, "[Runner] 'warm' 단계: 5s 대기 시작") and R.has_log(e, "[Runner] 'warm' 지연 완료 → 다음 스텝")
    assert "지연 대기 중: 5s · 남은 시간 00:00:05" in seen["delay"] or seen["delay"]
    assert R.has_log(e, "[TEST MODE] 'T1' 장비 제어 스킵 / 10.0s 시뮬레이션")
    assert [x.data.get("result") for x in e.events if x.kind == "run_end"] == ["done", "done", "done"]
    st = _starts(e)
    assert [x.detail for x in st] == ["T1", "A", "B"]
    ends = [x for x in e.events if x.kind == "run_end"]
    assert R.has_log(e, "[Runner] cooldown wait:") and seen["cool"]
    assert R.has_log(e, "[CH1] 자동 공정 (4/4) 준비: 'B'") and R.has_log(e, "[Runner] queue finished → reset")
    # 쿨다운 — B 의 시작(프리플라이트 포함) ≥ A 끝 + 60 s
    tr = list(e.traces)
    assert tr[2][0][0] - tr[1][-1][0] >= 60.0
    # ETA: 마지막 행 전에는 null, 마지막 행에서 확정되면 숫자
    assert all(v is None for i, v in seen["eta"] if i < 3)
    assert any(v is not None for i, v in seen["eta"] if i == 3)
    assert e.rows is None and e.runs == []                                      # 큐 종료 → 비움(다음 Start 는 단일)


def test_stop_during_delay_row_cancels_list(tmp_path):
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    e.load([{"Process_name": "delay 2m"}, dict(PROC)], source="local")
    R.run(rig, R.idle(e), max_ticks=300,
          hooks=[R.at(2, lambda: e.start(source="local")), R.at(20, lambda: e.stop(source="local"))])
    assert R.ends(e) == [("aborted", "USER_STOP")]
    assert not _starts(e) and R.has_log(e, "[Runner] STOP → 공정 시작 전 상태, 즉시 정리(cleanup)")
    assert not any(a == R.PM.resolve("MAIN_1_GAS_SW") for a, _on in R.plc_writes(rig.sims["plc"]))
    assert e.rows is None and not e.busy


def test_stop_during_cooldown_stops_queue(tmp_path):
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    e.load([dict(PROC, Process_name="A"), dict(PROC, Process_name="B")], source="local")
    R.run(rig, R.idle(e), max_ticks=600,
          hooks=[R.at(2, lambda: e.start(source="local")),
                 R.when(lambda: e.state_name == "COOLDOWN", lambda: e.stop(source="local"))])
    assert [x.detail for x in _starts(e)] == ["A"]
    assert R.ends(e) == [("done", None), ("aborted", "USER_STOP")]


class FakeHeater:
    """히터 엔진 대역 — heater_link 의 공정 API 와 같은 이름(결과는 on_process 로 바로)."""

    def __init__(self):
        self.on_process = None
        self.owner = None
        self.stop_user = True
        self.calls = []

    def _ok(self, kind):
        self.on_process(kind, True, "", None)

    def heater_set(self, target, rate=None, context=None):
        self.owner = "process"
        self.calls.append(("set", target, context))
        self._ok("set_ok")

    def heater_wait(self, target, soak_s=None):
        self.calls.append(("wait", target, soak_s))
        self._ok("ready")

    def heater_hold(self):
        self.calls.append(("hold",))
        self._ok("hold_ok")

    def heater_cancel(self):
        self.calls.append(("cancel",))

    def process_started(self, uses):
        self.calls.append(("started", uses))

    def set_process_flags(self, *a):
        pass

    def after_row(self, ok, stopped, detail=None, queue_mode=False, next_uses_heater=False, on_done=None):
        self.calls.append(("after_row", ok, stopped, queue_mode, next_uses_heater))
        if not (queue_mode and ok and not stopped and next_uses_heater):
            self.owner = None
        if on_done:
            on_done()

    def queue_end(self):
        self.calls.append(("queue_end",))

    def apply_target(self, v, owner=None, on_done=None):
        self.calls.append(("apply", v))
        if on_done:
            on_done(True, "")


H2 = {"Process_name": "H", "Ar": "T", "Ar_flow": "20", "base_pressure": "1e-5", "working_pressure": "5",
      "shutter_delay": "0", "process_time": "0.05", "use_dc_pulse": "T", "dc_pulse_power": "80", "dc_pulse_freq": "20",
      "gun1": "T", "main_shutter": "T", "use_heater": "T", "heater_temp": "120"}


def test_queue_keeps_heater_between_heater_rows(tmp_path):
    h = FakeHeater()
    rig = R.make(tmp_path, chambers=("ch2",), ch2={"heater": h})
    e = rig.eng["ch2"]
    assert e.load([dict(H2, Process_name="H1"), {"Process_name": "d", "delay": "3s"}, dict(H2, Process_name="H2")]) is None
    R.run(rig, R.idle(e), max_ticks=900, hooks=[R.at(2, lambda: e.start(source="local"))])
    assert R.ends(e) == [("done", None), ("done", None)], R.logs(e)[-20:]
    rows = [c for c in h.calls if c[0] == "after_row"]
    assert rows == [("after_row", True, False, True, True), ("after_row", True, False, True, False)]
    sets = [c for c in h.calls if c[0] == "set"]
    assert [s[1] for s in sets] == [120.0, 120.0] and sets[0][2] == '공정 "H1" (1/3)'
    assert ("wait", 120.0, None) in h.calls and ("hold",) in h.calls


def test_test_row_stop_skips_cleanup(tmp_path):
    """TEST-159: TEST 행 STOP — 종료 절차 없이 STOP, 장비 정리 생략 '[Runner] [TEST] STOP finished → cleanup skip'."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    e.load([{"#": "test", "Process_name": "T", "time": "60s"}], source="local")
    R.run(rig, R.idle(e), max_ticks=120, hooks=[R.at(2, lambda: e.start(source="local")),
                                                R.when(lambda: e.runner.cd is not None, lambda: e.stop())])
    assert R.ends(e) == [("aborted", "USER_STOP")]
    assert R.has_log(e, "[Runner] [TEST] STOP finished → cleanup skip") and not R.has_log(e, "[종료절차")


class FailHeater(FakeHeater):
    def heater_set(self, target, rate=None, context=None):
        self.calls.append(("set", target, context))
        self.on_process("failed", False, "히터 사용 불가 — 세션 마커 없음", "E830")


def test_heater_set_failure_aborts_before_ig(tmp_path):
    """TEST-128: HEATER_SET 실패 E830 → IG 대기 전에 중단(종료 절차), 대기 취소(heater_cancel)."""
    h = FailHeater()
    rig = R.make(tmp_path, chambers=("ch2",), ch2={"heater": h})
    e = rig.eng["ch2"]
    e.load([dict(H2)])
    R.run(rig, R.idle(e), max_ticks=300, hooks=[R.at(2, lambda: e.start(source="local"))])
    assert R.ends(e) == [("error", "E830")]
    acts = [a for _t, _i, a, _m, sd in e.traces[-1] if not sd]
    assert acts == ["HEATER_SET"] and ("cancel",) in h.calls
    assert R.has_log(e, "오류 발생: [HEATER - E830: 히터 사용 불가 — 세션 마커 없음]. 종료 절차를 시작합니다.")

"""전수 목록 pc 영역(inv_pc.py)에서 기존 시험이 직접 확인하지 않던 동작 — 실제 실행(v1 import 없음, 근거 = v1 파일:줄).

플라즈마 클리닝(가상 시계 + Ch12Rig, 또는 가짜 장비 단위)
- 채널 정규화·같은 선택 무시(PC-002·004), MFC 다시 묶기(PC-005), 상태창 문구 순서·카운트다운 표시·화면 복구(PC-020·031·040·043~048·
  052·053·080), 프리플라이트 '이미 연결됨'(PC-021), 이벤트 펌프 라벨(PC-024), 폴링 시작·런 사용 등록(PC-019·029), RF 표시(PC-059),
  호스트 공정 로그(PC-018·082·097·098), 완료 콜백(PC-063), 명령 표 버튼(PC-020·081), 화면 로그 상한(PC-038)
- TEST 카운트다운·종료(PC-016·017), 컨트롤러 단계별 실패 문구·카운트다운 중 MFC 실패(PC-041~047·052·054~056·099), 실패 정규화(PC-055),
  공유 MFC 이벤트 필터(PC-057), MFC 해제 ②(PC-078), CSV 빈 행(PC-087), 실행 스레드 예외(PC-092), RF 목표 대기 중 예외(PC-051),
  공정 로그 폴백·닫힌 동안의 줄(PC-033·036), 경고 팝업 자동 닫힘 정보(PC-085)
프리스퍼터: 시각 형식 오류(PC-109), 화면 상태(PC-115), 전용 레시피 로그(PC-116)
TSP: 설정 다시 읽기(PC-121), invalid_cycles·보정(PC-138), 로그 폴백·상한(PC-129)
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from v2.common.clock import VirtualClock
from v2.ch12 import commands as CM
from v2.ch12 import plasma_cleaning as PCM
from v2.ch12 import pre_sputter as PS
from v2.ch12.pc_log import PcRunLog
from v2.ch12.pc_recipe import PcParams
from v2.ch12.pc_steps import Box, PcCancelled, PcController
from v2.ch12.plasma_cleaning import PcDevices, PlasmaCleaning
from v2.ch12.sim.rig import Ch12Rig

UI = {"gas_flow": "10", "rf_power": "100", "process_time": "0.1", "working_pressure": "2"}


# ───────────────────────── 시험대 ─────────────────────────
def make(tmp_path, cfg=None, **kw):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    r = Ch12Rig(c, tmp_path, cfg=cfg)
    eng = PCM.attach(r, **kw)
    assert r.start()
    return r, eng


def ticks(r, pred, limit=900):
    n = 0
    while not pred() and n < limit:
        r.loop.run(1)
        n += 1
    assert pred(), f"{limit} tick 안에 조건이 안 됨"


def run_to_end(r, eng, limit=900):
    ticks(r, lambda: eng.last_final is not None and not eng.running, limit)
    r.loop.run(1)


def logs(eng):
    return [ln.split("] ", 1)[1] for ln in eng.ui_log]


def unit(tmp_path, plc=None, rf=None, ig=None, cfg=None):
    c = VirtualClock()
    c.register()
    base = {"LOG_ROOT_DIR": str(tmp_path / "logs"), "LOCAL_FALLBACK_ROOT": str(tmp_path / "fb")}
    e = PlasmaCleaning(c, dict(base, **(cfg or {})), PcDevices(plc, None, rf, {}, {"ch1": ig} if ig else {}))
    return c, e


class HostLog:
    def __init__(self):
        self.calls = []

    def mark_owned(self, key):
        self.calls.append(("mark_owned", key))

    def update(self, key, **kw):
        self.calls.append(("update", key, kw))

    def mark_started(self, key, t, log_file=None):
        self.calls.append(("mark_started", key, log_file))

    def finalize(self, key, res, reason, t):
        self.calls.append(("finalize", key, res, reason))


def _states(eng):
    seen = []
    orig = eng.set_state_text

    def rec(text):
        seen.append(str(text))
        orig(text)
    eng.set_state_text = rec
    return seen


# ───────────────────────── 채널 ─────────────────────────
def test_pc002_004_same_selection_ignored_and_normalized(tmp_path):
    c, e = unit(tmp_path)
    n0 = len(e.ui_log)
    assert e.select_channel(1) == (True, "") and len(e.ui_log) == n0           # 같은 선택이면 무시(main.py:855-901)
    assert e.set_selected_ch(3) is True and e.ch == 1                         # 2 가 아니면 1
    assert logs(e)[-1] == "PC: Selected Chamber → CH1"


def test_pc005_rebind_moves_mfc_claims(tmp_path):
    r, eng = make(tmp_path)
    try:
        m1, m2 = r.mfc["ch1"], r.mfc["ch2"]
        eng.run_owner = "pc1"
        eng.mfc_claims = {m1.resource_key: "pc1"}
        m1.poll_users.add("pc1")
        eng._apply_channel(2)
        assert set(eng.mfc_claims) == {m1.resource_key, m2.resource_key}       # 해제 후 새 장치로 재획득
        assert "pc1" in m1.poll_users and "pc1" in m2.poll_users
        assert "pc1" in r.run_state.run_users(m2.resource_key)                # 런 사용 등록을 새 장치 키로
        assert "PC: Bind MFC: GAS=192.168.1.50:4003, SP4=192.168.1.50:4006" in logs(eng)
        for m in (m1, m2):
            m.poll_users.discard("pc1")
        r.run_state.end_run_use("pc1")
    finally:
        r.stop()


# ───────────────────────── 실공정 한 번(화면·로그·호스트 기록) ─────────────────────────
def test_pc_real_run_states_host_log_and_done_callback(tmp_path):
    hl = HostLog()
    r, eng = make(tmp_path, host_log=hl)
    try:
        done = []
        eng.on_pc_done = lambda ch, name: done.append((ch, name))
        seen = _states(eng)
        f = tmp_path / "pc_h.csv"
        f.write_text("process_name,base_pressure,gas_flow,working_pressure,rf_power,process_time\r\n"
                     "PC-H,5e-6,10,2,100,0.1\r\n", encoding="utf-8-sig")
        box = eng.host_start(str(f), origin_meta={"key": "K1"})
        ticks(r, lambda: box.done, 200)
        assert box.value.ok
        ticks(r, lambda: eng.pc.step == "process")
        r.loop.run(2)
        m1 = r.mfc["ch1"]
        assert "pc1" in r.run_state.run_users(m1.resource_key) and m1.polling         # PC-019·029
        assert eng.state()["running"] and eng.ui_fwd != ""                             # PC-059
        assert eng.ui["process_time"].endswith("s")                                    # PC-053 공정 타이머 표시
        assert CM.check_command("PC_START", CM.WEB, f"pc.{eng.phase}") == "STATE_NOT_ALLOWED"   # 버튼(PC-020·081)
        assert CM.check_command("PC_STOP", CM.WEB, f"pc.{eng.phase}") is None
        run_to_end(r, eng)
        want = ["프리플라이트 중…", "Preparing…", "IG base wait…", "Base pressure OK", "Gas select ch3",
                "Gas Flow 10.0 sccm (stabilizing)", "Gas Flow 10.0 sccm OK", "SP4 Set 2.00 mTorr",
                "SP4 Pressure Wait 2.00 mTorr", "RF Start 100.0 W", "PROCESS 0.1 min",
                "PROCESS 0.1 min · 06s (0.10 min)", "PROCESS 0.1 min · 00s (0.00 min)", "대기 중"]
        idx = [seen.index(w) for w in want]
        assert idx == sorted(idx), seen
        assert eng.ui["process_time"] == "0.10" and (eng.ui_fwd, eng.ui_ref) == ("", "")     # PC-080 화면 복구
        assert eng.state_text == "대기 중" and r.run_state.run_users(m1.resource_key) == set()
        L = logs(eng)
        for name in ("PLC", "MFC(GAS)", "MFC(SP4)", "IG"):                                  # PC-021
            assert f"PC: {name} 이미 연결됨" in L
        assert any(x.startswith("MFC(SP4/GAS-CH1):") for x in L)                           # PC-024 같은 MFC
        assert "IG-CH1: Base pressure reached" in L and "RF: Power OFF finished" in L
        assert any(x.startswith("RF: FWD=") and x.endswith(" (W)") for x in L)
        assert "PC: 플라즈마 클리닝 시작" in L and "PC: 플라즈마 클리닝 종료" in L              # PC-040·054
        assert ("STEP: PARAMS gas_idx=3, flow=10.0 sccm, IG_target=5.000e-06 Torr, SP4=2.00 mTorr, RF=100.0 W, "
                "time=0.1 min") in L
        assert hl.calls == [("mark_owned", "K1"),
                            ("update", "K1", {"recipe_name": "pc_h.csv", "row_count": 1, "process_names": "PC-H"}),
                            ("mark_started", "K1", None),                                  # PC-098 log_file 늘 빈칸
                            ("finalize", "K1", "성공", "")]
        assert done == [(1, "PC-H")]                                                        # PC-063
        assert eng.ui_log.maxlen == PCM.UI_LOG_MAX == 5000                                   # PC-038
    finally:
        r.stop()


def test_pc_host_log_preflight_fail_and_stop(tmp_path):
    from v2.ch12.drivers import plc_map as PM
    hl = HostLog()
    r, eng = make(tmp_path, host_log=hl)
    try:
        done = []
        eng.on_pc_done = lambda ch, name: done.append((ch, name))
        coil = PM.COIL_MAP["G_V_1_인터락"]
        r.sims["plc"].coils[coil] = False
        box = eng.host_start("", origin_meta={"key": "K2"})
        ticks(r, lambda: box.done and not eng.running, 200)
        assert box.value.code == "E302"
        assert [c[0] for c in hl.calls] == ["mark_owned", "update", "mark_started"]       # PC-097 결과 확정 없음
        assert done == []                                                                  # 프리플라이트 실패는 완료 콜백 없음
        r.sims["plc"].coils[coil] = True
        r.loop.run(61)
        hl.calls.clear()
        eng.ui.update(dict(UI, process_time="1"))
        box = eng.host_start("", origin_meta={"key": "K3"})
        ticks(r, lambda: eng.pc.step == "process")
        eng.stop()
        run_to_end(r, eng)
        assert hl.calls[-1] == ("finalize", "K3", "STOP", "사용자 STOP")                    # PC-082
        assert done == [(1, "")]                                                           # 실패·정지도 콜백(PC-063)
        assert "PC: CancelledError: 외부 STOP 또는 경쟁 종료로 중단" in logs(eng)
    finally:
        r.stop()


def test_pc078_other_run_keeps_queue(tmp_path):
    r, eng = make(tmp_path)
    try:
        key = r.mfc["ch1"].resource_key
        r.run_state.begin_run_use("chamber1", [key])
        eng.start(params=UI)
        run_to_end(r, eng)
        assert (f"MFC: {key} 대기열/상태 유지 — 다른 공정 진행 중: chamber1 (폴링만 중지, PC 몫만 정리)") in logs(eng)
        assert eng.last_final["ok"] is True
        r.run_state.end_run_use("chamber1")
    finally:
        r.stop()


# ───────────────────────── TEST 모드 ─────────────────────────
def test_pc016_017_test_countdown_and_end(tmp_path):
    hl = HostLog()
    r, eng = make(tmp_path, host_log=hl)
    try:
        done = []
        eng.on_pc_done = lambda ch, name: done.append((ch, name))
        seen = _states(eng)
        eng.load_recipe_rows([{"#": "test", "time": "10s"}])
        assert eng.start(origin="host", origin_meta={"key": "K9"}).accepted
        r.loop.run(3)
        assert eng.ui["process_time"].count(":") == 1                                     # 'mm:ss'
        run_to_end(r, eng)
        assert "TEST MODE · 00:10 (0.17 min)" in seen and "TEST MODE · 00:01 (0.02 min)" in seen
        assert seen.index("TEST MODE · 00:10 (0.17 min)") < seen.index("TEST MODE · 00:01 (0.02 min)")
        assert done == [] and hl.calls == []                                                # PC-017
        assert not (Path(r.cfg["GDRIVE_LOG_DIR"]) / "CH1.xlsx").exists()
    finally:
        r.stop()


# ───────────────────────── 컨트롤러(가짜 장비 콜백) ─────────────────────────
class IO:
    abort_reason = None

    def __init__(self, clock, **kw):
        self.clock = clock
        self.states, self.counts = [], []
        self.gv = self.lamp = self.base = self.sp4 = self.flow_ok = True
        self.pc = None
        self.on_count = None
        self.__dict__.update(kw)

    def show_state(self, t):
        self.states.append(t)

    def plc_check_gv_interlock(self):
        return self.gv

    def plc_gv_open(self):
        pass

    def plc_read_gv_open_lamp(self):
        return self.lamp

    def _box(self, v):
        b = Box(self.clock)
        b.set(v)
        return b

    def ig_wait_for_base_torr(self, t, ms):
        return self._box(self.base)

    def mfc_gas_select(self, i):
        pass

    def mfc_flow_set_on(self, f):
        if self.flow_ok:
            self.pc.on_mfc_confirmed("FLOW_ON")

    def mfc_sp4_set(self, v):
        pass

    def mfc_sp4_on(self):
        pass

    def mfc_wait_sp4_pressure(self, v, t):
        return self._box(self.sp4)

    def cancel_sp4_wait(self):
        pass

    def rf_start(self, w, pred):
        pass

    def resume_left(self, total):
        return total

    def show_countdown(self, left):
        self.counts.append(left)
        if self.on_count:
            self.on_count(left)

    def stop_reason_word(self):
        return "사용자 STOP"


def _ctl(**kw):
    c = VirtualClock()
    c.register()
    lines = []
    pc = PcController(c, lambda s, m: lines.append(f"{s}: {m}"))
    io = IO(c, **kw)
    io.pc = pc
    return c, pc, io, lines


@pytest.mark.parametrize("attr,flow,why", [
    ("gv", 10.0, "게이트밸브 인터락 FALSE"),
    ("lamp", 10.0, "GV OPEN_LAMP가 TRUE가 아님(오픈 실패?)"),
    ("base", 10.0, "Base pressure not reached (IG API)"),
    ("flow_ok", 10.0, "Gas Flow가 90.0s 내에 안정적으로 setpoint에 도달하지 못했습니다."),
    ("sp4", 10.0, "SP4 압력이 목표에 도달하지 못했습니다 (target=2.00 mTorr, timeout=90.0s)"),
])
def test_pc041_047_step_failures(attr, flow, why):
    c, pc, io, lines = _ctl(**{attr: False})
    t0 = c.monotonic()
    pc.run(PcParams(gas_flow_sccm=flow, process_time_min=0.1), io)
    assert pc.last_result == "fail" and pc.last_error_detail == why and pc.last_error_source == "PC"
    assert "STEP: 종료 시퀀스 진입" in lines and io.states[-1] == "대기 중"
    if attr == "flow_ok":
        assert c.monotonic() - t0 >= 95.0                                                  # 5 s 램프 + 90 s
    if attr == "gv":
        assert "PLC: GV 인터락=False" in lines


def test_pc045_052_flow_skip_and_countdown_total_plus_one():
    c, pc, io, lines = _ctl()
    t0 = c.monotonic()
    pc.run(PcParams(gas_flow_sccm=0.0, process_time_min=0.1), io)
    assert pc.last_result == "success" and "MFC: FLOW_SET_ON 스킵(flow=0.0)" in lines
    assert io.counts == [6, 5, 4, 3, 2, 1, 0] and c.monotonic() - t0 == pytest.approx(5.0 + 7.0)
    assert "IG base wait…" in io.states and "PROCESS 0.1 min" in io.states


def test_pc099_mfc_failure_during_countdown_is_stop():
    c, pc, io, lines = _ctl()
    io.on_count = lambda left: left == 3 and pc.on_mfc_failed("VALVE_OPEN", "valve fault")
    pc.run(PcParams(gas_flow_sccm=10.0, process_time_min=0.1), io)
    assert (pc.last_result, pc.last_reason) == ("stop", "사용자 STOP")                    # [이상] 루프가 덮음(D107)
    assert (pc.last_error_detail, pc.last_error_source) == ("valve fault", "MFC")
    assert pc.last_error_meta["cmd"] == "VALVE_OPEN"
    assert "MFC: 명령 실패: VALVE_OPEN - valve fault" in lines
    assert any(x.startswith("STEP: STOP 이벤트 감지 → 남은 ") for x in lines)
    assert "PC: CancelledError: 외부 STOP 또는 경쟁 종료로 중단" in lines


def test_pc055_056_failure_normalize_and_first_kept():
    class E(RuntimeError):
        code, meta = "e511", {"k": 1}
    assert PcController.normalize(E("x")) == ("E511", "x", {"k": 1, "cause_type": "E"})
    assert PcController.normalize("", meta={"a": 1}) == (None, "unknown", {"a": 1})
    c, pc, io, lines = _ctl()
    pc.set_failure("RF", "첫 실패", meta={"stage": "s"})
    pc.set_failure("MFC", "둘째")
    assert (pc.last_reason, pc.last_error_detail, pc.last_error_source) == ("둘째", "첫 실패", "RF")
    pc.on_mfc_confirmed("FLOW_ON")
    assert pc.flow_on_set and lines[-1] == "MFC: 명령 완료: FLOW_ON"


# ───────────────────────── 장비 콜백 단위 ─────────────────────────
class NoRespPlc:
    def read_bit(self, key, on_done=None):
        pass                                                                                # 응답 없음


def test_pc041_043_gv_read_timeout_and_unbound(tmp_path):
    c, e = unit(tmp_path, plc=NoRespPlc())
    t0 = c.monotonic()
    assert e.plc_check_gv_interlock() is False
    assert c.monotonic() - t0 == pytest.approx(3.0)
    assert logs(e)[-1] == "PLC: GV 인터락 read timeout (3s) — lock 경합 추정: G_V_1_인터락"
    c2, e2 = unit(tmp_path)
    assert e2.plc_check_gv_interlock() is True and e2.plc_read_gv_open_lamp() is True    # PLC 없으면 통과
    with pytest.raises(RuntimeError, match="IG not bound"):
        e2.ig_wait_for_base_torr(5e-6, 10000)


class QuickRf:
    def start(self, w, on_done=None):
        on_done(None)

    def stop(self, on_done=None):
        self.stopped = True

    def c(self, k):
        return 60.0


class BadCfg(dict):
    def get(self, k, d=None):
        if k == "PC_RF_TARGET_WAIT_TIMEOUT_S":
            raise KeyError("boom")
        return super().get(k, d)


def test_pc051_rf_wait_exception_is_rf_failure(tmp_path):
    rf = QuickRf()
    c, e = unit(tmp_path, rf=rf)
    e.cfg = BadCfg(e.cfg)
    with pytest.raises(PcCancelled):
        e.rf_start(100.0, lambda: False)
    want = "RF Power 도달 실패: KeyError: 'boom'"
    assert e.forced_fail and e.forced_fail_reason == want and f"RF: {want}" in logs(e)
    assert e.pc.last_error_source == "RF" and e.pc.last_error_meta["stage"] == "_rf_start" and rf.stopped


def test_pc057_shared_mfc_event_filter(tmp_path):
    c, e = unit(tmp_path)
    e.pumps = {"ch1.mfc": "MFC(GAS)"}

    def ev(kind, owner, cmd="FLOW_ON", detail=""):
        return SimpleNamespace(source="ch1.mfc", event=SimpleNamespace(kind=kind, owner=owner, cmd=cmd, detail=detail,
                                                                       code=None, gas="N2"))
    n0 = len(e.ui_log)
    e.on_system_event(ev("command_confirmed", "chamber1"))
    assert len(e.ui_log) == n0 and not e.pc.flow_on_set                                    # 다른 owner 는 무시
    e.on_system_event(ev("command_confirmed", None))
    assert logs(e)[-2:] == ["MFC(GAS): OK: FLOW_ON", "MFC: 명령 완료: FLOW_ON"] and e.pc.flow_on_set
    e.on_system_event(ev("command_failed", "pc1", "SP4_ON", "nak"))
    assert "MFC(GAS): FAIL: SP4_ON (nak)" in logs(e) and e.pc.last_error_source == "MFC" and e.pc.stop_set
    e.on_system_event(ev("status", "x", detail="연결됨"))
    assert logs(e)[-1] == "MFC(GAS): 연결됨"


def test_pc087_empty_csv_and_pc085_warning_auto_close(tmp_path):
    c, e = unit(tmp_path)
    assert e.load_recipe_rows([{"a": " "}], "x.csv") == (False, "데이터 행이 없습니다.")
    p = [x for x in e.events if x.kind == "popup"][-1].data
    assert (p["kind"], p["title"], p["text"], p["auto_close_ms"]) == ("warning", "CSV 오류", "데이터 행이 없습니다.", 5000)
    e.popup("critical", "t", "b", ch=1, clear_on_close=True)
    p = [x for x in e.events if x.kind == "popup"][-1].data
    assert p["auto_close_ms"] is None and p["clear_on_close"] is True


def test_pc092_thread_exception_does_not_leave_running(tmp_path):
    c, e = unit(tmp_path)

    def boom(ch):
        raise ValueError("x")
    e._run_real = boom
    assert e.start().accepted
    c.wait_until(c.monotonic() + 1.0, lambda: not e.running)
    assert not e.running and e.internal_error == "ValueError('x')" and logs(e)[-1] == "PC: 오류: ValueError('x')"
    assert e.run_state.pc_block_reason(1)[0] is False                                     # mark_finished(쿨다운)


class SysLog:
    def __init__(self):
        self.lines = []

    def append(self, src, msg):
        self.lines.append((src, msg))


def test_pc033_036_run_log_fallback_and_closed_lines(tmp_path):
    bad = tmp_path / "not_a_dir"
    bad.write_text("x")
    sl = SysLog()
    log = PcRunLog({"LOG_ROOT_DIR": str(bad), "LOCAL_FALLBACK_ROOT": str(tmp_path / "fb")}, system_log=sl,
                   now=lambda: datetime(2025, 1, 2, 3, 4, 5))
    log.line("[03:04:05] PC: 닫힌 동안")
    assert sl.lines == [("PC", "PC: 닫힌 동안")]
    p = log.open(2, PcParams(), "a:1", "b:2")
    assert p == tmp_path / "fb" / "Plasma_Cleaning" / "PC_20250102_030405_CH2.log"
    log.line("[03:04:06] PC: 열림")
    log.flush()
    log.stop()
    assert p.read_text(encoding="utf-8").splitlines()[-1] == "[03:04:06] PC: 열림"


# ───────────────────────── 프리스퍼터 ─────────────────────────
class PEng:
    ch, n = "ch1", 1
    busy = False
    is_running = False


def test_pc109_115_116_presputter_format_state_and_recipe_log(tmp_path):
    c = VirtualClock(start_wall=datetime(2025, 10, 9, 5, 58, 0).timestamp())
    c.register()
    sink = []
    p = PS.PreSputter(c, PEng(), settings_dir=tmp_path, log_sink=sink.append)
    assert p.schedule("x") is False and p.status_text == "시간 형식 오류 (HH:MM)"
    assert p.lines[-1].endswith("[PreSputter] CH1 [설정] 잘못된 시간 형식(HH:MM)")
    assert p.state() == {"reserved": False, "hhmm": "06:00", "status_text": "시간 형식 오류 (HH:MM)", "left_text": "--:--:--",
                         "when": None, "recipe_path": "", "recipe_display": "UI 현재값 사용"}
    assert p.schedule("06:30")
    st = p.state()
    assert st["reserved"] and st["status_text"] == "예약됨 · 매일 06:30" and st["left_text"] == "00:32:00"
    p.set_recipe_path(str(tmp_path / "none.csv"))
    assert sink[-1].endswith(f"Auto: PreSputter CH1 레시피 지정: {tmp_path / 'none.csv'}")
    assert p.state()["recipe_display"] == "none.csv (파일 없음)"
    q = PS.PreSputter(c, PEng(), settings_dir=tmp_path, log_sink=sink.append)
    q.restore()
    assert sink[-2].endswith(f"Auto: PreSputter 레시피 파일 없음: {tmp_path / 'none.csv'}")
    p.close()
    q.close()


# ───────────────────────── TSP ─────────────────────────
def _tsp(tmp_path, **cfg):
    from v2.ch12.run_state import RunState
    from v2.ch12.tsp_engine import TspEngine
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    base = {"LOG_ROOT_DIR": str(tmp_path / "logs"), "LOCAL_FALLBACK_ROOT": str(tmp_path / "fb")}
    base.update(cfg)
    return c, TspEngine(c, base, None, None, RunState(c))


def test_pc121_138_tsp_cfg_reread_and_invalid_cycles(tmp_path):
    c, eng = _tsp(tmp_path)
    try:
        s = eng._s
        assert (s["on"], s["off"], s["poll"], s["first"], s["verify"], s["cooldown"], s["margin"]) == \
            (120.0, 150.0, 10.0, 5.0, True, 60.0, 300.0)
        assert (s["host"], s["tsp_port"], s["ig_port"]) == ("192.168.1.50", 4004, 4001)
        assert eng.inputs == {"target": "2.5e-07", "cycles": "10"}                       # PC-122
        eng.cfg["TSP_ON_SEC"] = 7
        assert eng.start({"target": "", "cycles": "1"})["reason"] == "BAD_ARGS"
        assert eng._s["on"] == 7.0                                                         # 시작마다 다시 읽음
        r = eng._ctrl_run(1e-7, 0)
        assert (r.success, r.cycles_done, r.reason) == (False, 0, "invalid_cycles")
    finally:
        eng.close()


def test_pc129_tsp_log_fallback_and_limits(tmp_path):
    from v2.ch12.outputs import tsp_txt as TT
    bad = tmp_path / "not_a_dir"
    bad.write_text("x")
    log = TT.TspTxtLog({"LOG_ROOT_DIR": str(bad), "LOCAL_FALLBACK_ROOT": str(tmp_path / "fb")},
                       now=lambda: datetime(2025, 1, 2, 3, 4, 5))
    try:
        assert (TT.PRESTART_MAX, TT.QUEUE_MAX) == (1000, 4096)
        assert log.prestart.maxlen == 1000 and log.pending.maxlen == 4096
        assert log.log("첫 줄") == "[03:04:05] [TSP] 첫 줄"
        notes = []
        path = log.open_run(lambda m: notes.append(m) or log.log(m))
        assert path.parent == tmp_path / "fb" / "TSP" and path.name == "TSP_20250102_030405.txt"
        assert notes and notes[0].startswith("[Logger] 로그 폴더 접근 실패(")
        log.writer.flush(5.0)
        text = path.read_text(encoding="utf-8")
        assert "[2025-01-02 03:04:05] [TSP] 첫 줄\n" in text
    finally:
        log.stop_writer()

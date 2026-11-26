"""영점 보호(W-21·22 대조 C — 챔버K 실장비 시험 2026-11-25, 추가 지시 10·11).

압력 센서·MFC 영점(Z1·L{4+ch}1)을 보내기 전에 그 챔버에서 모든 채널 유량 꺼짐(R69)·MFC 밸브 열림(R6)이 ZERO_MIN_IDLE_SEC(10 s) 이상 ·
PLC 가스 밸브(Ar·O₂·N₂·Main 가스) 닫힘. 시간만 모자라면 남은 만큼 기다렸다 보내고(11), 다른 조건이면 걸린 조건을 적어 건너뛴다(공정은 계속).
이미 꺼진 채널에 끄기(FLOW_OFF)를 다시 보내도 시간을 다시 세지 않는다(10). 기다리는 동안 대기열 폐기(정지)가 오면 보내지 않는다.
드라이버 시험대는 test_mfc.Rig(가상 시계 + MFC 시뮬레이터, 1초 읽기 TickLoop) — zero_ready=False 로 조건을 실제 읽기에서 만든다.
"""
from pathlib import Path

from v2.common.clock import VirtualClock
from v2.common.commands import Source
from v2.tests.test_mfc import Rig


def _bus_run(r, secs):
    bus = r.bus()
    return r.run(secs, bus)


def _zero(r, fn, *a):
    t0 = r.c.monotonic()
    res = r.call(fn, *a)
    return res, r.c.monotonic() - t0, r.sent(t0)


def _rig(**cfg):
    r = Rig("ch1", zero_ready=False, cfg=dict({"MFC_READ_MODE": "split"}, **cfg))
    r.d.zero_plc = lambda: (True, "")
    return r


def test_zero_passes_without_extra_time_when_idle_long_enough():
    r = _rig()
    _bus_run(r, 15)                                        # R69(6 s 마다)·R6(매 tick) — 유량 꺼짐·밸브 열림이 10 s 넘게
    assert r.d.zero_check(r.c.monotonic())[0] == "ok"
    res, took, sent = _zero(r, r.d.ps_zeroing)
    assert res[1] is None and "Z1" in sent and took < 2.0                      # 시간이 늘지 않음(기본 템플릿 공정)
    assert not r.kinds("zero_wait") and not r.kinds("zero_skipped")
    res, took, sent = _zero(r, r.d.mfc_zeroing, 1)
    assert res[1] is None and "L51" in sent and took < 2.0
    r.done()


def test_zero_waits_only_for_missing_time_after_valve_reopen():
    """11 — 대기 중 MFC 밸브가 닫혀 있던 경우: 열고 곧바로 영점이면 남은 시간만 기다렸다 보낸다(건너뛰지 않음)."""
    r = _rig()
    _bus_run(r, 15)
    r.call(r.d.valve_close)
    _bus_run(r, 3)
    assert r.d.zero_check(r.c.monotonic())[0] == "skip"                       # 닫혀 있으면 조건 아님
    r.call(r.d.valve_open)
    _bus_run(r, 2)                                                            # 열린 것을 R6 로 봄(몇 초 걸림)
    t_open = r.d.zero_valve_since
    assert t_open is not None and r.d.zero_check(r.c.monotonic())[0] == "wait"
    res, took, sent = _zero(r, r.d.ps_zeroing)
    assert res[1] is None and "Z1" in sent and not r.kinds("zero_skipped")
    w = r.kinds("zero_wait")
    assert len(w) == 1 and 0 < w[0].value <= 10.0 and r.c.monotonic() - t_open >= 10.0
    r.done()


def test_zero_skipped_names_actual_condition_flow_on_and_plc_open():
    r = _rig()
    _bus_run(r, 15)
    r.call(r.d.flow_on, 1)
    _bus_run(r, 7)                                                            # R69 를 다시 읽어 Ar 켜짐
    res, took, sent = _zero(r, r.d.ps_zeroing)
    assert res[1] is None and "Z1" not in sent and took < 2.0                 # 건너뜀 — 공정은 계속(확인 사건)
    s = r.kinds("zero_skipped")
    assert len(s) == 1 and s[0].level == "warn" and "Ar 유량 켜짐" in s[0].detail and "압력 센서 영점" in s[0].detail
    r.call(r.d.flow_off, 1)
    _bus_run(r, 12)
    r.d.zero_plc = lambda: (False, "Ar 밸브·Main 가스")
    res, _t, sent = _zero(r, r.d.mfc_zeroing, 1)
    s = r.kinds("zero_skipped")
    assert "L51" not in sent and len(s) == 2 and "PLC 가스 밸브 열림: Ar 밸브·Main 가스" in s[1].detail
    assert "유량" not in s[1].detail                                           # 실제로 걸린 조건만
    r.d.zero_plc = lambda: (None, "")
    _zero(r, r.d.ps_zeroing)
    assert "PLC 가스 밸브 상태 모름" in r.kinds("zero_skipped")[-1].detail
    r.done()


def test_flow_off_again_on_already_off_channel_does_not_restart_timer():
    """10 — 이미 꺼진 채널에 FLOW_OFF 를 다시 보내도 시간을 다시 세지 않는다(챔버K 0d68b02). 켜진 채널을 끄면 그때부터 센다."""
    r = _rig()
    _bus_run(r, 15)
    before = dict(r.d.zero_off_since)
    for ch in (1, 2, 3):
        assert r.call(r.d.flow_off, ch)[1] is None
    assert r.d.zero_off_since == before
    res, took, sent = _zero(r, r.d.ps_zeroing)
    assert "Z1" in sent and took < 2.0 and not r.kinds("zero_wait")
    r.call(r.d.flow_on, 2)
    _bus_run(r, 7)
    assert r.d.zero_flow[2] is True
    t_off = r.c.monotonic()
    r.call(r.d.flow_off, 2)                                                   # 켜진 채널을 실제로 끔 → 그때부터
    assert r.d.zero_off_since[2] >= t_off
    res, took, sent = _zero(r, r.d.ps_zeroing)
    assert "Z1" in sent and len(r.kinds("zero_wait")) == 1 and took >= 9.0
    r.done()


def test_stop_while_waiting_does_not_send_zero():
    r = _rig()
    _bus_run(r, 15)
    r.call(r.d.flow_on, 1)
    _bus_run(r, 7)
    r.call(r.d.flow_off, 1)
    box = []
    t0 = r.c.monotonic()
    r.d.ps_zeroing(on_done=lambda k, rs, t: box.append((k, rs, t)))
    r.c.wait_until(t0 + 2.0)
    assert r.kinds("zero_wait") and not box
    r.d.on_process_finished(False, reason="STOP")                             # 정지 — 대기열 폐기
    r.c.wait_until(t0 + 15.0)
    assert box and box[0][1] == "PURGED" and "Z1" not in r.sent(t0)
    r.done()


def test_zero_min_idle_zero_needs_conditions_only():
    r = _rig(ZERO_MIN_IDLE_SEC=0.0)
    _bus_run(r, 7)
    r.call(r.d.flow_off, 1)                                                   # 모르던 채널 → 지금부터(0 s 면 바로 됨)
    res, took, sent = _zero(r, r.d.ps_zeroing)
    assert "Z1" in sent and not r.kinds("zero_wait")
    r.done()


# ───────── 공정 전체(실제 조립 assembly.build — 시뮬레이터·실제 엔진, 가상 시계) ─────────
def _full(tmp):
    from v2.ch12.sim.rig import Ch12FullRig
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = Ch12FullRig(c, Path(tmp), cfg={"CAMERA_MIN_FREE_GB": 0.0}, robot_api=False)
    assert rig.start()
    rig.loop.run(20)
    return rig


def _row(name):
    return {"Process_name": name, "base_pressure": "7.00E-06", "working_pressure": "2", "Ar": "T", "Ar_flow": "20",
            "shutter_delay": "1", "process_time": "0.3", "main_shutter": "T", "use_rf_pulse": "T", "rf_pulse_power": "120",
            "rf_pulse_freq": "0.5", "rf_pulse_duty_cycle": "40"}


def _run_rows(rig, rows, ends):
    assert rig.command("RECIPE_LOAD", {"ch": 1, "rows": rows, "name": "zero"}, source=Source.WEB).ok
    assert rig.command("PROCESS_START", {"ch": 1}, source=Source.WEB).ok
    for _ in range(60):
        rig.loop.run(30)
        if len([e for e in rig.events if e.kind == "run_end"]) >= ends:
            break
    ev = [e for e in rig.events if e.source == "ch1.mfc"]
    return ([e for e in ev if e.kind in ("zero_wait", "zero_skipped")],
            [e.detail for e in ev if e.kind == "status" and "Zeroing 명령 전송" in e.detail],
            [e for e in rig.events if e.kind == "run_end"])


def test_two_row_recipe_second_row_zero_passes(tmp_path):
    """11 — 여러 행 레시피의 둘째 행도 영점을 건너뛰지 않는다(시간만 모자라면 기다림)."""
    rig = _full(tmp_path)
    try:
        zev, sent, ends = _run_rows(rig, [_row("R1"), _row("R2")], 2)
        assert len(ends) == 2 and not [e for e in zev if e.kind == "zero_skipped"]
        assert sent.count("압력 센서 Zeroing 명령 전송") == 2 and sent.count("Ch1 MFC Zeroing 명령 전송") == 2
    finally:
        rig.stop()
        rig.clock.close()


def test_valve_closed_before_start_waits_then_zeroes(tmp_path):
    """11 — 대기 중 MFC 밸브가 닫혀 있던 경우: gas_prep 의 밸브 열기 뒤 남은 시간만 기다렸다 영점(건너뛰지 않음)."""
    rig = _full(tmp_path)
    try:
        rig.mfc["ch1"].valve_close()
        rig.loop.run(30)
        zev, sent, ends = _run_rows(rig, [_row("R1")], 1)
        waits = [e for e in zev if e.kind == "zero_wait"]
        assert len(ends) == 1 and len(waits) == 1 and 0 < waits[0].event.value <= 10.0
        assert not [e for e in zev if e.kind == "zero_skipped"] and "압력 센서 Zeroing 명령 전송" in sent
    finally:
        rig.stop()
        rig.clock.close()

"""통신 끊김 공정 중단(S37)·PLC 재연결 뒤 남은 파워 정리(S38) — F(사용자 결정 2026-11-26, v1 에 없는 새 규칙).

판단 값: 1초 행 품질 plc.q·chN.mfc.q — fail(또는 링크가 끊긴 채 stale)인 행이면 앞 행부터의 시간만큼 늘고, ok·late 면 0, 명령에 양보한
stale(링크 살아 있음)·suppressed 는 그대로. 챔버 공정이 전원 켜기 단계를 시작한 뒤·정지 순서 전에만 판정(PLC 15 s·MFC 30 s, 0 = 끔).
앞부분은 가짜 시스템(1초 행을 직접 넣음 — 시간 경계를 정확히), 뒷부분은 실제 조립(Ch12FullRig — 시뮬레이터·실제 엔진, 가상 시계).
"""
from pathlib import Path
from types import SimpleNamespace as NS

from v2.common.clock import VirtualClock
from v2.common.commands import Source
from v2.common.worker import Request
from v2.ch12 import snapshot_fields as SF
from v2.ch12.safety import Ch12Safety


# ───────────── 가짜 시스템 ─────────────
class Clk:
    def __init__(self):
        self.t = 0.0

    def monotonic(self):
        return self.t

    def wall(self):
        return 1_760_000_000.0 + self.t


class Eng:
    def __init__(self, armed=True):
        self.runner = NS(is_running=True, shutdown=False, failed=False, stop_requested=False,
                         trace=[(0.0, 5, "RF_PULSE_START", "RF Pulse 설정 및 ON", False)] if armed else
                         [(0.0, 1, "MFC_CMD", "Ch1(AR) Flow Off", False)])
        self.aborts = []
        self.busy = True

    def abort(self, code, detail="", source="SAFETY"):
        self.aborts.append(code)
        self.runner.failed = True


class Drv:
    def __init__(self):
        self.enabled, self.last_sent, self.stops, self.running = False, None, 0, False

    def stop(self, on_done=None):
        self.stops += 1


def _sys(cfg=None):
    clk = Clk()
    s = NS(clock=clk, cfg=dict({"PLC_COMM_LOSS_ABORT_SEC": 15.0, "MFC_COMM_LOSS_ABORT_SEC": 30.0}, **(cfg or {})),
           policy=NS(device_writes=True), emitted=[],
           plc=NS(worker=NS(connected=True)), mfc={"ch1": NS(worker=NS(connected=True)), "ch2": NS(worker=NS(connected=True))},
           rf={"ch2": Drv(), "pc": Drv()}, dc={1: Drv(), 2: Drv()})
    s.emit = lambda *a, **k: s.emitted.append((a, k))
    return s


def _row(plc="ok", m1="ok", m2="ok", **extra):
    r = {"plc.q": plc, "ch1.mfc.q": m1, "ch2.mfc.q": m2}
    r.update(extra)
    return SF.RowView(r)


def _feed(sf, s, n, **q):
    for _ in range(n):
        s.clock.t += 1.0
        sf.on_row(_row(**q))


def _s37(sf):
    return [(e.kind, e.code, e.level) for e in sf.events if e.rule == "S37"]


def test_plc_loss_14s_continues_16s_aborts():
    s = _sys()
    e1, e2 = Eng(), Eng(armed=False)
    sf = Ch12Safety(s, {"ch1": e1, "ch2": e2})
    _feed(sf, s, 3)
    s.plc.worker.connected = False
    _feed(sf, s, 14, plc="fail")
    assert e1.aborts == [] and sf.comm_lost_s["plc"] == 14.0
    s.plc.worker.connected = True
    _feed(sf, s, 1)                                               # 다시 읽힘 → 0
    assert sf.comm_lost_s["plc"] == 0.0 and e1.aborts == []
    s.plc.worker.connected = False
    _feed(sf, s, 16, plc="fail")
    assert e1.aborts == ["PLC_COMM_LOST"] and e2.aborts == []      # 전원 켜기 전 챔버(ch2)는 그대로
    assert _s37(sf) == [("fired", "PLC_COMM_LOST", "warn")]       # 경고 사건 1번
    _feed(sf, s, 10, plc="fail")
    assert e1.aborts == ["PLC_COMM_LOST"] and len(_s37(sf)) == 1


def test_mfc_loss_29s_continues_31s_aborts_own_chamber_only():
    s = _sys()
    e1, e2 = Eng(), Eng()
    sf = Ch12Safety(s, {"ch1": e1, "ch2": e2})
    _feed(sf, s, 2)
    s.mfc["ch1"].worker.connected = False
    _feed(sf, s, 29, m1="fail")
    assert e1.aborts == [] and e2.aborts == []
    _feed(sf, s, 1, m1="ok")
    _feed(sf, s, 31, m1="fail")
    assert e1.aborts == ["MFC_COMM_LOSS"] and e2.aborts == []      # 그 챔버 MFC 만
    assert _s37(sf) == [("fired", "MFC_COMM_LOSS", "warn")]


def test_zero_setting_is_current_behaviour():
    s = _sys({"PLC_COMM_LOSS_ABORT_SEC": 0.0, "MFC_COMM_LOSS_ABORT_SEC": 0.0})
    e1 = Eng()
    sf = Ch12Safety(s, {"ch1": e1})
    s.plc.worker.connected = False
    _feed(sf, s, 200, plc="fail", m1="fail")
    assert e1.aborts == [] and not _s37(sf)


def test_no_abort_before_power_on():
    s = _sys()
    e1 = Eng(armed=False)                                         # 가스 준비 등 전원 켜기 전 단계
    sf = Ch12Safety(s, {"ch1": e1})
    s.plc.worker.connected = False
    _feed(sf, s, 60, plc="fail", m1="fail")
    assert e1.aborts == []
    e1.runner.shutdown = True                                     # 정지 순서 중에도 판정하지 않는다
    e1.runner.trace.append((61.0, 9, "RF_PULSE_START", "x", False))
    _feed(sf, s, 5, plc="fail")
    assert e1.aborts == []


def test_command_yield_stale_rows_are_not_counted():
    """로봇 PLC 명령이 30 s 이어져 1초 읽기가 양보(stale, 링크 살아 있음) — 세지 않는다. 링크가 끊긴 stale 은 센다."""
    s = _sys()
    e1 = Eng()
    sf = Ch12Safety(s, {"ch1": e1})
    _feed(sf, s, 30, plc="stale", m1="suppressed")
    assert e1.aborts == [] and sf.comm_lost_s["plc"] == 0.0 and sf.comm_lost_s["ch1.mfc"] == 0.0
    _feed(sf, s, 1, plc="fail")                                   # 명령 뒤 한 번 실패 — 1 s 만
    assert sf.comm_lost_s["plc"] == 1.0
    _feed(sf, s, 30, plc="stale")
    assert sf.comm_lost_s["plc"] == 1.0 and e1.aborts == []
    s.plc.worker.connected = False
    _feed(sf, s, 15, plc="stale")                                 # 링크가 끊긴 채 stale — 센다
    assert e1.aborts == ["PLC_COMM_LOST"]


def test_plasma_cleaning_while_pc_rf_on():
    s = _sys()
    pc = NS(running=True, stop_requested=False, abort_reason=None, ch=2, busy=True, aborts=[])
    pc.abort = lambda code: pc.aborts.append(code) or True
    sf = Ch12Safety(s, {"ch1": Eng(armed=False)}, pc=pc)
    s.mfc["ch2"].worker.connected = False
    _feed(sf, s, 40, m2="fail")
    assert pc.aborts == []                                        # PC RF 가 꺼져 있으면 판정하지 않는다
    s.rf["pc"].running = True
    _feed(sf, s, 1, m2="fail")
    assert pc.aborts == ["MFC_COMM_LOSS"]                         # PC 가 쓰는 챔버(CH2) MFC
    ev = [e for e in sf.events if e.rule == "S37"]
    assert len(ev) == 1 and ev[0].detail.startswith("Plasma Cleaning")


def _res_row(**q):
    return dict(q, **{"ch2.dc.dc1_enable": True, "plc.dcv_write_0": 404, "ch2.rf.enable": False, "plc.dcv_write_2": 0,
                      "pc.rf.enable": False, "plc.dcv_write_1": 0, "ch2.dc.dc2_enable": False, "plc.dcv_write_3": 0})


def test_reconnect_turns_off_residual_only_when_idle():
    s = _sys()
    e1 = Eng(armed=False)
    e1.busy = False
    e1.runner.is_running = False
    sf = Ch12Safety(s, {"ch1": e1})
    sf.on_row(SF.RowView(_res_row(**{"plc.q": "ok", "ch1.mfc.q": "ok", "ch2.mfc.q": "ok"})))   # 시작 때(S31) — 남은 DC1 끔
    assert s.dc[1].stops == 1
    s.plc.worker.connected = False
    for _ in range(5):
        s.clock.t += 1
        sf.on_row(SF.RowView(_res_row(**{"plc.q": "fail"})))
    e1.busy = True                                                 # 공정·운전 중 재연결 — 아무것도 하지 않는다(알림도 없음)
    s.plc.worker.connected = True
    n = len(sf.events)
    for _ in range(3):
        s.clock.t += 1
        sf.on_row(SF.RowView(_res_row(**{"plc.q": "ok"})))
    assert s.dc[1].stops == 1 and len(sf.events) == n
    e1.busy = False                                                # 모두 대기가 된 첫 정상 행 — S31 과 같은 확인으로 끔
    s.clock.t += 1
    sf.on_row(SF.RowView(_res_row(**{"plc.q": "ok"})))
    assert s.dc[1].stops == 2 and [(e.rule, e.code) for e in sf.events if e.kind == "fired"][-1] == ("S38", "RESIDUAL_POWER_OFF")
    s.clock.t += 1
    sf.on_row(SF.RowView(_res_row(**{"plc.q": "ok"})))
    assert s.dc[1].stops == 2                                      # 재연결당 한 번


# ───────────── 실제 조립(가상 시계) ─────────────
def _rig(tmp, cfg=None):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    from v2.ch12.sim.rig import Ch12FullRig
    rig = Ch12FullRig(c, Path(tmp), cfg=dict({"CAMERA_MIN_FREE_GB": 0.0}, **(cfg or {})), robot_api=False)
    assert rig.start()
    rig.loop.run(20)
    return rig


def _main(rig, ch, row, on):
    rows = []
    rig.loop.sinks.append(rows.append)
    assert rig.command("RECIPE_LOAD", {"ch": int(ch[-1]), "rows": [row], "name": "comm"}, source=Source.WEB).ok
    assert rig.command("PROCESS_START", {"ch": int(ch[-1])}, source=Source.WEB).ok
    for _ in range(900):
        rig.loop.run(1)
        if on() and "메인 공정" in (rows[-1].get(f"{ch}.proc.step_label") or ""):
            break
    rig.loop.run(5)
    assert on()
    return rows


ROW = {"Process_name": "COMM", "base_pressure": "7.00E-06", "working_pressure": "2", "Ar": "T", "Ar_flow": "20",
       "shutter_delay": "0.1", "process_time": "5", "main_shutter": "T"}


def test_plc_cut_ch2_rf_dc_aborts_live_off_within_20s_then_residual_off(tmp_path):
    rig = _rig(tmp_path)
    try:
        S = rig.sims
        _main(rig, "ch2", dict(ROW, use_rf_power="T", rf_power="200", use_dc_power="T", dc_power="100", gun1="T", gun2="T"),
              lambda: S["plc"].out_w[2] > 1.0)
        t0 = rig.clock.monotonic()
        S["plc"].add_fault("disconnect", t0, t0 + 60)
        fired = flow_off = None
        for _ in range(120):
            rig.loop.run(1)
            ev = [e for e in rig.safety.events if e.rule == "S37"]
            if fired is None and ev:
                fired = rig.clock.monotonic()
            if fired is not None and flow_off is None and not any(S["ch2.mfc"].on):
                flow_off = rig.clock.monotonic()
        assert fired is not None and 14.0 <= fired - t0 <= 18.0
        assert flow_off is not None and flow_off - fired <= 20.0                 # 살아 있는 장치(MFC) 끄기 — 판정 뒤 20 s 안
        ends = [e for e in rig.events if e.kind == "run_end"]
        assert ends and ends[-1].code == "PLC_COMM_LOST"
        s38 = [e for e in rig.safety.events if e.rule == "S38"]
        assert ("fired", "RESIDUAL_POWER_OFF") in [(e.kind, e.code) for e in s38]
        assert S["plc"].out_w[0] < 1.0 and not S["plc"].coil("DCV_SET_0")      # CH2 DC1 — PLC 가 돌아온 뒤 꺼짐
    finally:
        rig.stop()
        rig.clock.close()


def test_long_plc_command_and_short_blip_during_process_do_nothing(tmp_path):
    """로봇 PLC 명령이 30 s 이어져도(1초 읽기 양보 — stale) 중단 없음. 공정 중 짧은 끊김(5 s) 뒤 재연결은 아무것도 하지 않는다."""
    rig = _rig(tmp_path, {"CH1_SUPPORTS_RFPULSE": True})
    try:
        S = rig.sims
        _main(rig, "ch1", dict(ROW, use_rf_pulse="T", rf_pulse_power="120", rf_pulse_freq="0.5", rf_pulse_duty_cycle="40"),
              lambda: S["ch1.rfpulse"].out)
        t0 = rig.clock.monotonic()
        rig.plc.worker.submit(Request("write", lambda s: rig.clock.wait_until(rig.clock.monotonic() + 30.0) or True,
                                      deadline=t0 + 60, exec_timeout_s=60, name="plc:-:robot-hold"))
        rig.loop.run(35)
        t1 = rig.clock.monotonic()
        S["plc"].add_fault("disconnect", t1, t1 + 5)
        rig.loop.run(20)
        assert not [e for e in rig.safety.events if e.rule in ("S37", "S38")]
        assert not [e for e in rig.events if e.kind == "run_end"] and S["ch1.rfpulse"].out
    finally:
        rig.stop()
        rig.clock.close()


def test_plasma_cleaning_plc_cut_aborts_and_gas_off_within_20s(tmp_path):
    rig = _rig(tmp_path)
    try:
        S = rig.sims
        assert rig.command("PC_START", {"ch": 1, "params": {"gas_flow": "10", "rf_power": "100", "process_time": "5",
                                                           "working_pressure": "2"}}, source=Source.WEB).ok
        for _ in range(600):
            rig.loop.run(1)
            if S["plc"].out_w[1] > 1.0:
                break
        rig.loop.run(5)
        t0 = rig.clock.monotonic()
        S["plc"].add_fault("disconnect", t0, t0 + 60)
        fired = flow_off = None
        for _ in range(110):
            rig.loop.run(1)
            if fired is None and [e for e in rig.safety.events if e.rule == "S37"]:
                fired = rig.clock.monotonic()
            if fired is not None and flow_off is None and not any(S["ch1.mfc"].on):
                flow_off = rig.clock.monotonic()
        assert fired is not None and flow_off is not None and flow_off - fired <= 20.0
        assert rig.asm.pc.abort_reason == "PLC_COMM_LOST" and not rig.asm.pc.running
        assert S["plc"].out_w[1] < 1.0                                               # PC RF — PLC 가 돌아온 뒤(S38) 꺼짐
    finally:
        rig.stop()
        rig.clock.close()

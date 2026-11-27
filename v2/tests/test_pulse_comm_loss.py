"""P — RF Pulse·DC Pulse 통신 끊김 중단(S39)·재연결 뒤 끄기(S40) — 사용자 결정 2026-11-27, v1 에 없는 새 규칙(S37 과 같은 방식).

판단 값: 1초 행 품질 chN.rfpulse.q·chN.dcpulse.q — fail(또는 링크가 끊긴 채 stale)이면 앞 행부터의 시간만큼 늘고, ok·late·off 면 0.
그 전원의 켜기 단계(RF_PULSE_START·DC_PULSE_START)를 시작한 뒤·정지 순서 전에만 RFPULSE_COMM_LOSS_ABORT_SEC·DCPULSE_COMM_LOSS_ABORT_SEC
(기본 30 s, 0 = 끔) 이상이면 그 챔버 공정 중단(RFP_COMM_LOST·DCP_COMM_LOST, 경고 사건 1번). 정지 순서는 그 링크가 끊겨 있으면 그 전원의
꺼짐 확인을 기다리지 않는다. 링크가 돌아오고 공정·운전이 없으면 끄기를 다시 보낸다(정보 사건 1번), 공정 중이면 아무것도 하지 않는다.
앞부분은 가짜 시스템(1초 행을 직접 넣음 — 시간 경계를 정확히), 뒷부분은 실제 조립(Ch12FullRig — 시뮬레이터·실제 엔진, 가상 시계).
"""
from pathlib import Path
from types import SimpleNamespace as NS

from v2.common.clock import VirtualClock
from v2.common.commands import Source
from v2.ch12 import snapshot_fields as SF
from v2.ch12.params import table
from v2.ch12.safety import Ch12Safety

DEF = {d.key: d.default for d in table().params}


class Clk:
    def __init__(self):
        self.t = 0.0

    def monotonic(self):
        return self.t

    def wall(self):
        return 1_760_000_000.0 + self.t


class Eng:
    def __init__(self, action="RF_PULSE_START", armed=True):
        tr = [(0.0, 4, "MFC_CMD", "Ch1(AR) Flow On", False)]
        if armed:
            tr.append((0.0, 5, action, "펄스 설정 및 ON", False))
        self.runner = NS(is_running=True, shutdown=False, failed=False, stop_requested=False, trace=tr)
        self.aborts = []
        self.busy = True

    def abort(self, code, detail="", source="SAFETY"):
        self.aborts.append(code)
        self.runner.failed = True


class PDrv:
    """가짜 펄스 드라이버 — 끄기 호출을 센다."""
    def __init__(self):
        self.worker = NS(connected=True)
        self.offs = []

    def off_needs_confirm(self):
        return True

    def _off(self, name):
        def f(on_done=None):
            self.offs.append(name)
            if on_done:
                on_done(None, True)
        return f

    stop_process = property(lambda self: self._off("stop_process"))
    stop_unused = property(lambda self: self._off("stop_unused"))
    output_off = property(lambda self: self._off("output_off"))
    output_off_unused = property(lambda self: self._off("output_off_unused"))


class Ports:
    """가짜 PulsePorts — (챔버, 종류) 점유·안전 엔진 잡기."""
    def __init__(self):
        self.own, self.hold, self.log = {}, {}, []

    def owner(self, kind, ch):
        return self.own.get((ch, kind))

    def held(self, kind, ch):
        return self.own.get((ch, kind)) == (ch, kind) and self.hold.get((ch, kind), False)

    def claim(self, kind, ch, hold=False):
        self.log.append(("claim", ch, kind, hold))
        self.own[(ch, kind)], self.hold[(ch, kind)] = (ch, kind), hold
        return True, "", None

    def release_hold(self, kind, ch):
        self.log.append(("release_hold", ch, kind))
        if self.held(kind, ch):
            self.own.pop((ch, kind), None)
            return True
        return False


def _sys(cfg=None):
    s = NS(clock=Clk(), cfg=dict({"PLC_COMM_LOSS_ABORT_SEC": 15.0, "MFC_COMM_LOSS_ABORT_SEC": 30.0,
                                  "RFPULSE_COMM_LOSS_ABORT_SEC": 30.0, "DCPULSE_COMM_LOSS_ABORT_SEC": 30.0}, **(cfg or {})),
           policy=NS(device_writes=True), emitted=[],
           plc=NS(worker=NS(connected=True)), mfc={"ch1": NS(worker=NS(connected=True)), "ch2": NS(worker=NS(connected=True))},
           rf={}, dc={}, rfpulse={"ch1": PDrv(), "ch2": PDrv()}, dcpulse={"ch1": PDrv(), "ch2": PDrv()}, pulse=Ports())
    s.emit = lambda *a, **k: s.emitted.append((a, k))
    return s


def _row(**q):
    r = {"plc.q": "ok", "ch1.mfc.q": "ok", "ch2.mfc.q": "ok", "ch1.rfpulse.q": "ok", "ch1.dcpulse.q": "off",
         "ch2.rfpulse.q": "off", "ch2.dcpulse.q": "ok"}
    r.update({f"{k.replace('_', '.')}.q": v for k, v in q.items()})
    return SF.RowView(r)


def _feed(sf, s, n, **q):
    for _ in range(n):
        s.clock.t += 1.0
        sf.on_row(_row(**q))


def _ev(sf, rule):
    return [(e.kind, e.code, e.level) for e in sf.events if e.rule == rule]


def test_defaults_and_poll_miss_margin():
    assert DEF["RFPULSE_COMM_LOSS_ABORT_SEC"] == 30.0 and DEF["DCPULSE_COMM_LOSS_ABORT_SEC"] == 30.0
    d = {p.key: p for p in table().params}
    assert (d["RFPULSE_COMM_LOSS_ABORT_SEC"].min, d["RFPULSE_COMM_LOSS_ABORT_SEC"].max) == (0.0, 120.0)
    assert (d["DCPULSE_COMM_LOSS_ABORT_SEC"].min, d["DCPULSE_COMM_LOSS_ABORT_SEC"].max) == (0.0, 120.0)
    # 폴링 한 번이 빠져도 중단되지 않는 길이 — 응답 한도 + 판정 주기보다 길다
    assert DEF["POLL_QUERY_TIMEOUT_MS"] / 1000 + DEF["POLL_INTERVAL_MS"] / 1000 < DEF["RFPULSE_COMM_LOSS_ABORT_SEC"]
    assert (DEF["DCP_TIMEOUT_MS"] + 2000) / 1000 + DEF["DCP_POLL_INTERVAL_S"] < DEF["DCPULSE_COMM_LOSS_ABORT_SEC"]


def test_rf_pulse_loss_29s_continues_31s_aborts_once():
    s = _sys()
    e1, e2 = Eng("RF_PULSE_START"), Eng("DC_PULSE_START", armed=False)
    sf = Ch12Safety(s, {"ch1": e1, "ch2": e2})
    _feed(sf, s, 3)
    s.rfpulse["ch1"].worker.connected = False
    _feed(sf, s, 29, ch1_rfpulse="fail")
    assert e1.aborts == [] and _ev(sf, "S39") == []
    _feed(sf, s, 2, ch1_rfpulse="fail")
    assert e1.aborts == ["RFP_COMM_LOST"] and e2.aborts == []
    assert _ev(sf, "S39") == [("fired", "RFP_COMM_LOST", "warn")]
    _feed(sf, s, 10, ch1_rfpulse="fail")
    assert len(_ev(sf, "S39")) == 1                                     # 경고 사건 1번
    assert ("ch1", "rfpulse") in sf.pulse_off_pending


def test_dc_pulse_loss_aborts_own_chamber_only():
    s = _sys()
    e1, e2 = Eng("RF_PULSE_START"), Eng("DC_PULSE_START")
    sf = Ch12Safety(s, {"ch1": e1, "ch2": e2})
    s.dcpulse["ch2"].worker.connected = False
    _feed(sf, s, 31, ch2_dcpulse="stale")                                # 링크가 끊긴 채 stale 도 센다
    assert e2.aborts == ["DCP_COMM_LOST"] and e1.aborts == []
    assert _ev(sf, "S39") == [("fired", "DCP_COMM_LOST", "warn")]


def test_single_missed_polls_do_not_abort():
    """폴링 한 번 빠짐(응답 한도 9 s + 다음 판정 8 s 안 — fail 약 17 s) 뒤 ok 가 오면 0 부터 — 여러 번 되풀이돼도 중단 없음."""
    s = _sys()
    e1 = Eng("RF_PULSE_START")
    sf = Ch12Safety(s, {"ch1": e1})
    for _ in range(5):
        _feed(sf, s, 17, ch1_rfpulse="fail")
        _feed(sf, s, 1)
    assert e1.aborts == [] and _ev(sf, "S39") == []
    # 링크가 살아 있는 채 stale(명령에 양보)은 세지 않는다
    _feed(sf, s, 60, ch1_rfpulse="stale")
    assert e1.aborts == []


def test_zero_setting_is_current_behaviour():
    s = _sys({"RFPULSE_COMM_LOSS_ABORT_SEC": 0.0})
    e1 = Eng("RF_PULSE_START")
    sf = Ch12Safety(s, {"ch1": e1})
    s.rfpulse["ch1"].worker.connected = False
    _feed(sf, s, 200, ch1_rfpulse="fail")
    assert e1.aborts == [] and _ev(sf, "S39") == []


def test_no_abort_before_pulse_output_on():
    """그 전원의 켜기 단계 전(다른 전원만 켰어도)은 판정하지 않는다. 쓰지 않는 펄스(off 품질)는 0."""
    s = _sys()
    e1 = Eng("DC_POWER_SET")                                             # RF Pulse 켜기 단계 없음
    sf = Ch12Safety(s, {"ch1": e1})
    s.rfpulse["ch1"].worker.connected = False
    _feed(sf, s, 40, ch1_rfpulse="fail")
    assert e1.aborts == []
    _feed(sf, s, 1, ch1_rfpulse="off")
    assert sf.comm_lost_s["ch1.rfpulse"] == 0.0


def test_reconnect_sends_off_once_only_when_idle():
    s = _sys()
    e1 = Eng("RF_PULSE_START")
    sf = Ch12Safety(s, {"ch1": e1})
    s.rfpulse["ch1"].worker.connected = False
    _feed(sf, s, 31, ch1_rfpulse="fail")
    assert e1.aborts == ["RFP_COMM_LOST"]
    # 정지 정리 중(공정 busy) — 링크가 돌아와도 아무것도 하지 않는다
    s.rfpulse["ch1"].worker.connected = True
    _feed(sf, s, 5, ch1_rfpulse="off")
    assert s.rfpulse["ch1"].offs == [] and s.pulse.log == [] and _ev(sf, "S40") == []
    # 공정이 끝나 포트를 놓음(off) — 대기가 되면 포트를 잡고(연결 기다림), 연결되면 끄기 1번 + 정보 사건 1번, 잡기를 푼다
    e1.busy = False
    s.rfpulse["ch1"].worker.connected = False
    _feed(sf, s, 1, ch1_rfpulse="off")
    assert s.pulse.log == [("claim", "ch1", "rfpulse", True)] and s.rfpulse["ch1"].offs == []
    _feed(sf, s, 3, ch1_rfpulse="fail")
    assert s.rfpulse["ch1"].offs == []
    s.rfpulse["ch1"].worker.connected = True
    _feed(sf, s, 1)
    assert s.rfpulse["ch1"].offs == ["stop_process"]
    assert _ev(sf, "S40") == [("info", "RESIDUAL_POWER_OFF", "info")]
    assert ("release_hold", "ch1", "rfpulse") in s.pulse.log and sf.pulse_off_pending == {}
    _feed(sf, s, 10)
    assert s.rfpulse["ch1"].offs == ["stop_process"] and len(_ev(sf, "S40")) == 1


def test_recovery_during_process_does_nothing_and_process_takes_over():
    """공정 중 복구 — 새 공정이 그 전원을 잡아 연결되면(공정이 끈다) 다시 보낼 일이 없다."""
    s = _sys()
    e1 = Eng("RF_PULSE_START")
    sf = Ch12Safety(s, {"ch1": e1})
    s.rfpulse["ch1"].worker.connected = False
    _feed(sf, s, 31, ch1_rfpulse="fail")
    assert ("ch1", "rfpulse") in sf.pulse_off_pending
    e2 = Eng("RF_PULSE_START")                                           # 다음 공정이 같은 전원을 잡음
    sf.proc["ch1"] = e2
    s.pulse.own[("ch1", "rfpulse")], s.pulse.hold[("ch1", "rfpulse")] = ("ch1", "rfpulse"), False
    s.rfpulse["ch1"].worker.connected = True
    _feed(sf, s, 3)
    assert sf.pulse_off_pending == {} and s.rfpulse["ch1"].offs == [] and _ev(sf, "S40") == []
    assert e2.aborts == []


def test_pulse_port_hold_is_handed_to_a_process():
    """안전 엔진 잡기(hold)는 공정이 잡으면 넘겨준다 — 같은 주소의 다른 종류도 시작을 막지 않는다."""
    from v2.ch12.drivers.pulse_port import PulsePort

    class D:
        def __init__(self):
            self.bound = None

        def worker_params(self):
            return {}

        def bind(self, w):
            self.bound = w

        def unbind(self):
            self.bound = None

    class W:
        def __init__(self, *a, **k):
            self.running, self.stopped = False, False

        def start(self):
            self.running = True

        def stop(self):
            self.running, self.stopped = False, True

    import v2.ch12.drivers.pulse_port as PPM
    old = PPM.Worker
    PPM.Worker = W
    try:
        port = PulsePort(Clk(), "192.168.1.50:4007", lambda k, c: object(), True)
        rfp, dcp = D(), D()
        port.add("rfpulse", "ch1", rfp)
        port.add("dcpulse", "ch1", dcp)
        assert port.claim("rfpulse", "ch1", hold=True)[0] and port.hold and rfp.bound is not None
        ok, _msg, _o = port.claim("dcpulse", "ch1")                         # 공정이 다른 종류를 잡음 → 넘겨줌
        assert ok and port.owner == ("ch1", "dcpulse") and not port.hold and rfp.bound is None and dcp.bound is not None
        assert port.claim("rfpulse", "ch1", hold=True)[0] is False          # 공정이 가진 것은 안전 엔진이 빼앗지 않는다
        port.release("ch1")
        assert port.claim("rfpulse", "ch1", hold=True)[0] and port.claim("rfpulse", "ch1")[0] and not port.hold
    finally:
        PPM.Worker = old


# ───────────── 실제 조립(가상 시계) ─────────────
def _rig(tmp, cfg=None):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    from v2.ch12.sim.rig import Ch12FullRig
    rig = Ch12FullRig(c, Path(tmp), cfg=dict({"CAMERA_MIN_FREE_GB": 0.0, "CH1_SUPPORTS_RFPULSE": True}, **(cfg or {})),
                      robot_api=False)
    assert rig.start()
    rig.loop.run(20)
    return rig


ROW = {"Process_name": "PCOMM", "base_pressure": "7.00E-06", "working_pressure": "2", "Ar": "T", "Ar_flow": "20",
       "shutter_delay": "0.1", "process_time": "5", "main_shutter": "T", "use_rf_pulse": "T", "rf_pulse_power": "120",
       "rf_pulse_freq": "0.5", "rf_pulse_duty_cycle": "40"}


def _main(rig, rows):
    assert rig.command("RECIPE_LOAD", {"ch": 1, "rows": [ROW], "name": "pcomm"}, source=Source.WEB).ok
    assert rig.command("PROCESS_START", {"ch": 1}, source=Source.WEB).ok
    for _ in range(900):
        rig.loop.run(1)
        if rig.sims["ch1.rfpulse"].out and "메인 공정" in (rows[-1].get("ch1.proc.step_label") or ""):
            break
    rig.loop.run(5)
    assert rig.sims["ch1.rfpulse"].out


def test_rf_pulse_cut_aborts_skips_off_confirm_live_off_within_20s_then_off_after_reconnect(tmp_path):
    rig = _rig(tmp_path)
    try:
        S, rows = rig.sims, []
        rig.loop.sinks.append(rows.append)
        _main(rig, rows)
        t0 = rig.clock.monotonic()
        S["ch1.rfpulse"].add_fault("disconnect", t0, t0 + 150)
        fired = flow_off = None
        for _ in range(140):
            rig.loop.run(1)
            if fired is None and [e for e in rig.safety.events if e.rule == "S39"]:
                fired = rig.clock.monotonic()
            if fired is not None and flow_off is None and not any(S["ch1.mfc"].on):
                flow_off = rig.clock.monotonic()
        assert fired is not None and 29.0 <= fired - t0 <= 34.0
        assert flow_off is not None and flow_off - fired <= 20.0                 # 살아 있는 장치(MFC) 끄기 — 판정 뒤 20 s 안
        ends = [e for e in rig.events if e.kind == "run_end"]
        assert ends and ends[-1].code == "RFP_COMM_LOST"
        eng = rig.engine("proc.ch1")
        assert any("RF Pulse 통신 끊김 — RF_PULSE_STOP 확인을 기다리지 않고 다음 단계로" in ln for ln in eng.log_lines)
        assert [(e.kind, e.code) for e in rig.safety.events if e.rule == "S39"] == [("fired", "RFP_COMM_LOST")]
        assert S["ch1.rfpulse"].out                                            # 끊긴 동안 OFF 를 받지 못함(sim 출력 그대로)
        for _ in range(120):                                                   # 링크가 돌아옴(t0+150) → 대기 → 끄기 다시
            rig.loop.run(1)
            if not S["ch1.rfpulse"].out:
                break
        assert not S["ch1.rfpulse"].out
        s40 = [(e.kind, e.code) for e in rig.safety.events if e.rule == "S40"]
        assert s40 == [("info", "RESIDUAL_POWER_OFF")]
        rig.loop.run(10)
        assert rig.pulse.owner("rfpulse", "ch1") is None and rig.safety.pulse_off_pending == {}
    finally:
        rig.stop()
        rig.clock.close()


def test_one_missed_poll_during_process_does_nothing(tmp_path):
    """공정 중 RF Pulse 가 한 번 응답하지 않음(9 s 응답 한도) — 중단·사건 없음, 공정은 그대로."""
    rig = _rig(tmp_path)
    try:
        S, rows = rig.sims, []
        rig.loop.sinks.append(rows.append)
        _main(rig, rows)
        t0 = rig.clock.monotonic()
        S["ch1.rfpulse"].add_fault("no_response", t0, t0 + 3)
        rig.loop.run(40)
        assert not [e for e in rig.safety.events if e.rule in ("S39", "S40")]
        assert not [e for e in rig.events if e.kind == "run_end"] and S["ch1.rfpulse"].out
    finally:
        rig.stop()
        rig.clock.close()

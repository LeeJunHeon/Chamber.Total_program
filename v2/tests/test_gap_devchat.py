"""v1 에서 쓰던 장비 설정 중 v2 에 빠졌던 것(gap — 장비·콘솔 쪽) 시험. 근거는 각 시험 독스트링의 v1 파일:줄.

- *_DRAIN_TIMEOUT_S(IG·MFC·DC Pulse·RF Pulse): v1 asyncio `wait_for(writer.drain(), …)` 한도 → v2 TcpTransport 쓰기 한도
  (TcpSettings.write_timeout_s). 넘으면 TransportError → worker 가 끊고 재연결.
- DEBUG_PRINT: v1 드라이버 debug_print 콘솔 줄 → v2 app 콘솔(장비 info 이벤트 '[출처][종류] 내용').
- CHAMBER_PULSE_RECONNECT_TIMEOUT_S: v1 시작 전 펄스 set_endpoint_reconnect 한도 → v2 점유 뒤 재연결(옛 Worker 멈춤·새 Worker 돎) 한도.
- PC_PREFLIGHT_DEVICE_CONNECT_TIMEOUT_S: v1 PC 프리플라이트 connect() 한도 → v2 그 장비 worker 가 도는지 보는 한도.
"""
from __future__ import annotations

import errno
import threading
from types import SimpleNamespace

import pytest

from v2.common.clock import VirtualClock
from v2.common.transport import TcpSettings, TcpTransport, TransportError
from v2.common.worker import Request, Status, Worker
from v2.ch12 import app as A
from v2.ch12 import process_preflight as PF
from v2.ch12.drivers import ig as IG
from v2.ch12.drivers import mfc as MFC
from v2.ch12.drivers.pulse_port import tcp_transport_factory
from v2.ch12.params import table
from v2.ch12.pc_preflight import PreflightIO
from v2.ch12.system import Logged
from v2.tests import proc_rig as R

DEF = {p.key: p.default for p in table().params}


# ───────────── 쓰기 한도(*_DRAIN_TIMEOUT_S) ─────────────
class FakeSock:
    """non-blocking 소켓 흉내 — accept 바이트만 받고 나머지는 EAGAIN(버퍼 가득)."""

    def __init__(self, accept: int = 10 ** 9, per_call: int = 10 ** 9) -> None:
        self.accept, self.per_call, self.got, self.closed = accept, per_call, b"", False

    def send(self, data) -> int:
        room = min(self.accept - len(self.got), self.per_call, len(data))
        if room <= 0:
            raise BlockingIOError(errno.EAGAIN, "full")
        self.got += bytes(data[:room])
        return room

    def sendall(self, data) -> None:
        if len(self.got) + len(data) > self.accept:
            raise BlockingIOError(errno.EAGAIN, "full")
        self.got += bytes(data)

    def recv(self, n):
        raise BlockingIOError(errno.EAGAIN, "none")

    def close(self) -> None:
        self.closed = True


def _tcp(clock, sock, wto):
    tr = TcpTransport(clock, TcpSettings("127.0.0.1", 1, 1.0, write_timeout_s=wto), socket_factory=lambda s: sock)
    tr.open()
    return tr


def test_write_timeout_partial_sends_then_error():
    """v1 device/ig.py:713-714·mfc.py:1385-1386·dc_pulse.py:2273-2274·rf_pulse.py:1186-1190 — write 뒤 drain 을 한도까지 기다림.
    v2: 버퍼에 들어갈 때까지 조금씩 보내고(여러 번 send), 한도 안에 다 못 넣으면 TransportError('write timeout(…)')."""
    c = VirtualClock()
    c.register()
    s = FakeSock(per_call=2)
    tr = _tcp(c, s, 2.0)
    tr.write(b"RDI\r")                                     # 2 B 씩 두 번 — 다 들어감
    assert s.got == b"RDI\r"
    s2 = FakeSock(accept=3)
    tr2 = _tcp(c, s2, 2.0)
    t0 = c.monotonic()
    with pytest.raises(TransportError, match=r"write timeout\(2\.0s\) — 3/5 B 보냄"):
        tr2.write(b"Q1 10")
    assert c.monotonic() - t0 == pytest.approx(2.0, abs=0.01)
    tr3 = _tcp(c, FakeSock(accept=0), 0.0)                 # 한도 0(DCP max(0, 값)) — 한 번 시도하고 바로 오류
    with pytest.raises(TransportError, match=r"write timeout\(0\.0s\)"):
        tr3.write(b"x")
    tr4 = _tcp(c, FakeSock(accept=0), None)                # 한도 없음(PLC·TSP·STM — 지금까지와 같음): sendall 오류 그대로
    with pytest.raises(TransportError, match="write: "):
        tr4.write(b"x")
    c.close()


def test_write_timeout_makes_worker_reconnect():
    """v1 drain 실패 → _on_tcp_disconnected(재연결, ig.py:715-724). v2: 쓰기 TransportError → worker 가 연결을 끊고 다시 연다."""
    c = VirtualClock()
    c.register()
    socks = [FakeSock(accept=0), FakeSock()]
    tr = TcpTransport(c, TcpSettings("127.0.0.1", 1, 1.0, write_timeout_s=2.0), socket_factory=lambda s: socks.pop(0))
    w = Worker("t", tr, writes_allowed=True, fail_streak=1, reconnect_start_s=1.0, reconnect_max_s=1.0)
    out = []
    w.start()
    try:
        c.wait_until(c.monotonic() + 5.0, lambda: w.connected)
        w.submit(Request("write", lambda s: s.write(b"OUT1"), name="w1", on_done=out.append))
        c.wait_until(c.monotonic() + 10.0, lambda: bool(out))
        assert out[0].status != Status.OK and "write timeout(2.0s)" in (out[0].error or "")
        assert c.wait_until(c.monotonic() + 10.0, lambda: w.connected and not socks)     # 두 번째 소켓으로 다시 연결
    finally:
        w.stop()
        c.close()


def test_drivers_pass_drain_timeouts():
    """설정 키 = v1 이름, 기본값 = v1 실효값(IG 2.0 — ig.py:144, MFC 2.0 — mfc.py:1385, DCP 1.0 — dc_pulse.py:496 max(0, 값),
    RF Pulse 2.0 — rf_pulse.py:1189-1190 max(0.1, 값))."""
    assert (DEF["IG_DRAIN_TIMEOUT_S"], DEF["MFC_DRAIN_TIMEOUT_S"], DEF["DCP_DRAIN_TIMEOUT_S"], DEF["RFPULSE_DRAIN_TIMEOUT_S"]) == \
        (2.0, 2.0, 1.0, 2.0)
    cfg = dict(DEF, IG_DRAIN_TIMEOUT_S=0.7, MFC_DRAIN_TIMEOUT_S=1.3, DCP_DRAIN_TIMEOUT_S=0.0, RFPULSE_DRAIN_TIMEOUT_S=0.0)
    assert IG.tcp_settings(cfg, "ch1").write_timeout_s == 0.7 and MFC.tcp_settings(cfg, "ch2").write_timeout_s == 1.3
    mk = tcp_transport_factory(VirtualClock(), cfg)
    assert mk("dcpulse", "ch1").settings.write_timeout_s == 0.0
    assert mk("rfpulse", "ch2").settings.write_timeout_s == 0.1                    # v1 max(0.1, 값)
    mk = tcp_transport_factory(VirtualClock(), DEF)
    assert (mk("dcpulse", "ch2").settings.write_timeout_s, mk("rfpulse", "ch1").settings.write_timeout_s) == (1.0, 2.0)


# ───────────── DEBUG_PRINT ─────────────
def _ev(source, kind="status", detail="d", level="info", code=None, reason=None):
    return Logged(0.0, source, kind, detail, reason, code, level, None)


def test_debug_print_console_lines(tmp_path):
    """v1 lib/config_common.py:74 DEBUG_PRINT=False(운영값), true 면 드라이버가 print(f"[MFC][status] {msg}")(mfc.py:2175-2177),
    print(f"[{src}] {msg}")(_dbg — dc_pulse.py:3130-3132 등). v2: 장비 이벤트(info)를 '[출처][종류] 내용'으로 콘솔에 — 설정은 바로 반영,
    엔진·조립 이벤트·경고(이미 콘솔)·스펙트럼은 빼고."""
    assert DEF["DEBUG_PRINT"] is False
    rig = R.make(tmp_path, chambers=("ch1",))
    try:
        assert {"plc", "ch1.mfc", "ch1.ig", "ch1.dcpulse", "ch1.rfpulse", "pulse", "tsp", "camera"} <= rig.device_sources
        assert not ({"pc", "engine:tsp", "heater"} & rig.device_sources)       # 엔진(조립) 이벤트 길은 장비가 아니다
        lines = []
        app = A.App(str(tmp_path / "cfg"), print_fn=lines.append, clock=rig.clock)
        app.system = rig
        rig.event_listeners.append(app._debug_event)
        rig._ev_for("ch1.mfc")(SimpleNamespace(kind="status", detail="[SEND] R60"))
        assert lines == []                                                     # 끔(운영값) — 아무것도 안 찍음
        rig.cfg["DEBUG_PRINT"] = True                                          # LIVE — 바로
        rig._ev_for("ch1.mfc")(SimpleNamespace(kind="status", detail="[SEND] R60"))
        rig._ev_for("ch1.rfpulse")(SimpleNamespace(kind="command_confirmed", detail="RF_ON"))
        rig.emit("proc.ch1", "log", "엔진 줄")                                   # 엔진 — 안 찍음
        rig._ev_for("ch1.ig")(SimpleNamespace(kind="disconnected", detail="끊김"))   # warn — 로그 처리기가 이미 콘솔에
        rig._ev_for("ch1.mfc")(SimpleNamespace(kind="spectrum", detail="x"))          # 화면 전용(TRANSIENT_KINDS)
        assert lines == ["[ch1.mfc][status] [SEND] R60", "[ch1.rfpulse][command_confirmed] RF_ON"]
        assert A.debug_line(_ev("plc", "connected", "192.168.1.2:502", code="C1")) == "[plc][connected] 192.168.1.2:502 (C1)"
    finally:
        rig.clock.close()


# ───────────── CHAMBER_PULSE_RECONNECT_TIMEOUT_S ─────────────
class _Eng:
    def __init__(self, settled, cfg=None):
        self.clock = VirtualClock()
        self.clock.register()
        self.cfg = dict(DEF, **(cfg or {}))
        self.ch, self.n, self.lines = "ch1", 1, []
        self.cancel = False
        self.sys = SimpleNamespace(shared=SimpleNamespace(pulse=SimpleNamespace(settled=settled)))

    def cget(self, key, default=None):
        v = self.cfg.get(f"CH{self.n}_{key}", self.cfg.get(key))
        return default if v is None else v

    def log(self, src, msg):
        self.lines.append((src, msg))

    def stage_cancelled(self):
        return self.cancel


def test_pulse_reconnect_logs_and_timeout():
    """v1 runtime/chamber_runtime.py:3896-3927 — 고른 펄스마다 _pulse_reconnect_safe(3760-3797): '[Runner] {label} reconnect 시작:
    host:port' → wait_for(set_endpoint_reconnect, CHAMBER_PULSE_RECONNECT_TIMEOUT_S 2.0) → '[Runner] {label} reconnect 완료',
    넘으면 RuntimeError('{label} reconnect timeout(2.0s)'), 의도한 취소는 그대로, 다른 예외는 '{label} reconnect failed: {e!r}'."""
    assert DEF["CHAMBER_PULSE_RECONNECT_TIMEOUT_S"] == 2.0
    e = _Eng(lambda kind, ch: True)
    PF.reconnect_pulses(e, {"dcpulse": True, "rfpulse": True})
    assert e.lines == [("MAIN", "[Runner] DC-Pulse reconnect 시작: 192.168.1.50:4007"), ("MAIN", "[Runner] DC-Pulse reconnect 완료"),
                       ("MAIN", "[Runner] RF-Pulse reconnect 시작: 192.168.1.50:4007"), ("MAIN", "[Runner] RF-Pulse reconnect 완료")]
    e.clock.close()
    e = _Eng(lambda kind, ch: False, cfg={"CHAMBER_PULSE_RECONNECT_TIMEOUT_S": 1.5})
    t0 = e.clock.monotonic()
    with pytest.raises(RuntimeError, match=r"^RF-Pulse reconnect timeout\(1\.5s\)$"):
        PF.reconnect_pulses(e, {"rfpulse": True})
    assert e.clock.monotonic() - t0 == pytest.approx(1.5, abs=0.06) and e.lines[-1][1].endswith("reconnect 시작: 192.168.1.50:4007")
    e.clock.close()

    def boom(kind, ch):
        raise OSError("x")
    e = _Eng(boom)
    with pytest.raises(RuntimeError, match=r"^DC-Pulse reconnect failed: OSError\('x'\)$"):
        PF.reconnect_pulses(e, {"dcpulse": True})
    e.clock.close()
    e = _Eng(lambda kind, ch: False)
    e.cancel = True
    with pytest.raises(PF.StageCancelled):
        PF.reconnect_pulses(e, {"dcpulse": True})
    e.clock.close()


def test_pulse_port_settled_waits_old_worker():
    """v1 set_endpoint_reconnect(dc_pulse.py:628-637) = 옛 워치독 멈춤(pause_watchdog — 끝날 때까지 기다림)·끊기 → 새 워치독 시작.
    v2 PulsePort.settled: 지금 점유한 Worker 가 돌고, 놓은 옛 Worker 가 모두 멈췄을 때만 참."""
    from v2.tests.test_pulse_port import Plant
    p = Plant()
    try:
        assert not p.ports.settled("rfpulse", "ch1")                     # 점유 전
        assert p.ports.claim("rfpulse", "ch1")[0] and p.ports.settled("rfpulse", "ch1")
        assert not p.ports.settled("dcpulse", "ch1")                     # 같은 포트의 다른 종류는 아님
        port = p.ports.port_of("rfpulse", "ch1")
        old = SimpleNamespace(stopped=False)
        port._retired.append(old)                                        # 옛 세션이 아직 닫히는 중
        assert not p.ports.settled("rfpulse", "ch1")
        old.stopped = True
        assert p.ports.settled("rfpulse", "ch1")
        p.ports.release_chamber("ch1")
        assert not p.ports.settled("rfpulse", "ch1") and not port._retired      # 놓은 Worker 는 멈춤(release 가 기다림) — 정리됨
        assert p.ports.claim("rfpulse", "ch1")[0] and p.ports.settled("rfpulse", "ch1") and not port._retired
    finally:
        p.ports.release_chamber("ch1")
        p.c.close()


def test_pulse_reconnect_timeout_fails_start_like_v1(tmp_path):
    """한도를 넘으면 v1 except(4097-4119)와 같이 "오류: '{note}' 시작 실패. (RF-Pulse reconnect timeout(2.0s))" 로그·'오류' 창,
    START_SINGLE failed → run_end error PREFLIGHT_FAILED(문구 '시작 실패: …'), 점유는 풀린다."""
    from v2.tests.test_inv_proc import S1_P
    rig = R.make(tmp_path, chambers=("ch1",))
    rig.shared.pulse.settled = lambda kind, ch: False
    e = rig.eng["ch1"]
    R.run(rig, R.idle(e), max_ticks=200, hooks=[R.at(2, lambda: e.start(params=dict(S1_P), owner="local", source="local"))])
    msg = "오류: 's1' 시작 실패. (RF-Pulse reconnect timeout(2.0s))"
    assert R.has_log(e, "[Runner] RF-Pulse reconnect 시작: 192.168.1.50:4007") and R.has_log(e, msg)
    assert not R.has_log(e, "reconnect 완료") and not R.has_log(e, "already connected → skip")    # 장비 기동 전에 멈춤
    assert any(x.kind == "popup" and x.data.get("title") == "오류" for x in e.events)
    end = [x for x in e.events if x.kind == "run_end"][-1]
    assert (end.data.get("result"), end.code, end.detail) == ("error", "PREFLIGHT_FAILED", f"시작 실패: {msg}")
    assert rig.shared.pulse.owner("rfpulse", "ch1") is None


# ───────────── PC_PREFLIGHT_DEVICE_CONNECT_TIMEOUT_S ─────────────
class _Pc(PreflightIO):
    def __init__(self, devs, cfg=None):
        self.clock = VirtualClock()
        self.clock.register()
        self.cfg = dict(DEF, **(cfg or {}))
        self.dev = SimpleNamespace(plc=None, plc_drv=None, mfc_gas=devs.get("gas"), mfc_pressure=devs.get("sp4"), ig=devs.get("ig"))
        self.lines, self.pumps = [], 0
        self.run_state = SimpleNamespace(run_users=lambda key: [])

    def _cfgf(self, key, default):
        return float(self.cfg.get(key, default))

    def log(self, src, msg):
        self.lines.append(msg)

    def mfc_owner(self):
        return "PC"

    def pumps_on_now(self):
        self.pumps += 1


class _Dev:
    def __init__(self, running=True, connected=False):
        self.worker = SimpleNamespace(running=running)
        self.conn = connected

    def is_connected(self):
        return self.conn


def test_pc_device_connect_timeout():
    """v1 runtime/plasma_cleaning_runtime.py:454·470-485 — 연결 안 된 장비는 wait_for(connect(), PC_PREFLIGHT_DEVICE_CONNECT_TIMEOUT_S 3.0)
    (connect = start — 워치독·명령 워커 기동), 넘으면 RuntimeError('{이름} 연결 실패: RuntimeError(\\'{이름} connect timeout(3.0s)\\')').
    v2: 그 장비 worker 가 돌고 있는지(멈추는 중이면 다시 돌 때까지) 한도 안에 보고 '{이름} connect 호출', 연결 성립은 전체 10 s 대기."""
    assert DEF["PC_PREFLIGHT_DEVICE_CONNECT_TIMEOUT_S"] == 3.0
    gas = _Dev(running=False)
    pc = _Pc({"gas": gas, "ig": _Dev(connected=True)})
    t0 = pc.clock.monotonic()
    with pytest.raises(RuntimeError) as ex:
        pc.preflight_connect()
    assert str(ex.value) == "MFC(GAS) 연결 실패: RuntimeError('MFC(GAS) connect timeout(3.0s)')"
    assert pc.clock.monotonic() - t0 == pytest.approx(3.0, abs=0.01) and "MFC(GAS) connect 호출" not in pc.lines
    pc.clock.close()
    # worker 가 돌면 'connect 호출' — 연결은 전체 대기(PC_PREFLIGHT_TIMEOUT_S) 안에 되면 통과
    gas = _Dev(running=True)
    pc = _Pc({"gas": gas, "sp4": gas, "ig": _Dev(connected=True)}, cfg={"PC_PREFLIGHT_DEVICE_CONNECT_TIMEOUT_S": 1.0})
    th = pc.clock.start_thread(lambda: (pc.clock.wait_until(pc.clock.monotonic() + 4.0), setattr(gas, "conn", True)), "conn")
    pc.preflight_connect()
    th.join(5)
    assert pc.lines[:4] == ["프리플라이트: 장비 연결 확인/시작", "MFC(GAS) connect 호출", "MFC(SP4) connect 호출", "IG 이미 연결됨"]
    assert pc.pumps == 1
    pc.clock.close()

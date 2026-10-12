# -*- coding: utf-8 -*-
"""켠 적 없는 펄스 출력의 정지 = OFF 1회 검증 (2026-10-08 밤 CH2 DC Pulse 무응답 → 종료 절차 240초·가스 계속·오경보).

DC Pulse: device/dc_pulse.py off_needs_confirm() / output_off_unused() — 워커 입구(_enqueue)를 가짜로 바꿔 응답을 흉내낸다
RF Pulse: device/rf_pulse.py off_needs_confirm() / stop_unused() — 127.0.0.1 가짜 AE Bus 서버로 실제 경로를 돌린다
런타임  : runtime/chamber_runtime.py 의 정지 콜백(cb_dc_pulse_stop / cb_rf_pulse_stop)이 '켠 적 없음'일 때만 1회 OFF 를 고르고,
          DC Pulse 이벤트 펌프가 OUTPUT_OFF_UNUSED 확인으로 정지 단계를 끝내는지(실제 소스 그대로 실행)
네트워크는 127.0.0.1 만 쓴다. 기존 확인형 OFF(output_off / stop_process) 동작은 바꾸지 않는다.
"""
import os
import sys
import ast
import time
import asyncio
import contextlib
import textwrap

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                  # noqa: E402
import device.rf_pulse as RFP                                  # noqa: E402
from device.dc_pulse import AsyncDCPulse, DCPEvent             # noqa: E402
from device.rf_pulse import RFPulseAsync, _build_packet        # noqa: E402

CMD_RF_OFF = RFP.CMD_RF_OFF
CMD_RF_ON = RFP.CMD_RF_ON
_OFF_FRAME = bytes.fromhex("0280000203 87")    # 장비 로그의 OUTPUT_OFF 프레임(02 80 00 02 03 87)


async def _wait(cond, timeout=5.0, step=0.02):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if cond():
            return True
        await asyncio.sleep(step)
    return False


# ═════════════════════════════ DC Pulse ═════════════════════════════
class _DcpCfg:
    DCP_UNUSED_OFF_WAIT_S = 0.6
    DEBUG_PRINT = False


def _mk_dcp(reply="ack", on_enqueue=None):
    """실제 AsyncDCPulse. 워커 대신 _enqueue 에서 응답을 흉내낸다.
    reply: "ack" → 0x06, "err" → 0x04, "none" → 콜백 None, "silent" → 콜백 안 부름(무응답)"""
    d = AsyncDCPulse(host="127.0.0.1", port=9, cfg=_DcpCfg())
    d.sent = []

    def _enqueue(cmd):
        d.sent.append(cmd)
        if on_enqueue:
            on_enqueue(d, cmd)
        loop = asyncio.get_running_loop()
        if reply == "ack":
            loop.call_later(0.05, cmd.callback, b"\x06")
        elif reply == "err":
            loop.call_later(0.05, cmd.callback, b"\x04")
        elif reply == "none":
            loop.call_later(0.05, cmd.callback, None)
    d._enqueue = _enqueue
    return d


def _drain_events(d):
    evs = []
    while True:
        try:
            evs.append(d._event_q.get_nowait())
        except asyncio.QueueEmpty:
            return evs


def test_d1_needs_confirm_truth_table():
    d = AsyncDCPulse(host="127.0.0.1", port=9, cfg=_DcpCfg())
    assert d.off_needs_confirm() is False                     # 프로그램 시작 후 켠 적 없음
    for name in ("_out_on", "_output_maybe_on", "output_off_unconfirmed"):
        d2 = AsyncDCPulse(host="127.0.0.1", port=9, cfg=_DcpCfg())
        setattr(d2, name, True)
        assert d2.off_needs_confirm() is True, name


def test_d2_unused_off_ack_finishes_once_without_alarm():
    async def _main():
        d = _mk_dcp("ack")
        t0 = time.perf_counter()
        ok = await d.output_off_unused()
        dt = time.perf_counter() - t0
        evs = _drain_events(d)
        return d, ok, dt, evs

    d, ok, dt, evs = asyncio.run(_main())
    assert ok is True and dt < 0.5, dt
    assert len(d.sent) == 1
    c = d.sent[0]
    assert c.payload == _OFF_FRAME and c.label == "OUTPUT_OFF[1회]" and c.retries_left == 0
    conf = [e for e in evs if e.kind == "command_confirmed"]
    assert [e.cmd for e in conf] == ["OUTPUT_OFF_UNUSED"]
    assert [e for e in evs if e.kind == "command_failed"] == []
    msgs = [e.message or "" for e in evs if e.kind == "status"]
    assert any("켠 적 없음" in m and "(ACK)" in m for m in msgs), msgs
    assert d._stop_guard is True                               # 켜는/세팅 write 차단
    assert d._out_on is False and d._output_maybe_on is False and d.output_off_unconfirmed is False


@pytest.mark.parametrize("reply,how", [("silent", "(응답 없음)"), ("none", "(응답 없음)"), ("err", "(ERR(04))")])
def test_d3_unused_off_no_reply_still_finishes_quickly(reply, how):
    async def _main():
        d = _mk_dcp(reply)
        seen_busy = []
        t = asyncio.ensure_future(d.output_off_unused())
        await asyncio.sleep(0.02)
        seen_busy.append(d._off_in_progress())                  # cleanup 은 이 동안 기다린다
        t0 = time.perf_counter()
        ok = await t
        dt = time.perf_counter() - t0
        return d, ok, dt, _drain_events(d), seen_busy

    d, ok, dt, evs, busy = asyncio.run(_main())
    assert busy == [True]
    assert d._off_in_progress() is False
    assert ok is True and dt < _DcpCfg.DCP_UNUSED_OFF_WAIT_S + 0.5, dt
    assert [e.cmd for e in evs if e.kind == "command_confirmed"] == ["OUTPUT_OFF_UNUSED"]
    assert [e for e in evs if e.kind == "command_failed"] == []    # 켠 적 없으므로 경고 없음
    msgs = [e.message or "" for e in evs if e.kind == "status"]
    assert any("켠 적 없음" in m and how in m for m in msgs), msgs
    assert d.output_off_unconfirmed is False


def test_d4_race_on_sent_during_wait_falls_back_to_confirm_off():
    """기다리는 사이 OUTPUT_ON 이 나가면(_output_maybe_on=True) 기존 확인형 output_off() 로 넘어간다."""
    def _on_enq(d, cmd):
        d._output_maybe_on = True                               # 큐에 있던 OUTPUT_ON 이 먼저 나갔다고 가정

    async def _main():
        d = _mk_dcp("silent", on_enqueue=_on_enq)
        calls = []

        async def _confirm_off():
            calls.append("output_off")
            await d._emit_confirmed("OUTPUT_OFF")
            return True
        d.output_off = _confirm_off
        ok = await d.output_off_unused()
        return d, ok, calls, _drain_events(d)

    d, ok, calls, evs = asyncio.run(_main())
    assert ok is True and calls == ["output_off"]
    assert [e.cmd for e in evs if e.kind == "command_confirmed"] == ["OUTPUT_OFF"]
    msgs = [e.message or "" for e in evs if e.kind == "status"]
    assert any("확인형 OUTPUT_OFF 로 전환" in m for m in msgs), msgs


# ═════════════════════════════ RF Pulse ═════════════════════════════
class FakeAEBus:
    """exec → ACK + CSR 프레임, query → ACK + 데이터. silent=True 면 받기만 한다(장비 무응답)."""
    QUERY_LEN = {155: 1, 162: 4, 165: 2, 166: 2, 193: 3, 196: 2}

    def __init__(self):
        self.server = None
        self.port = 0
        self.cmds = []
        self.silent = False
        self.status_on = False
        self._conns = []

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def stop(self):
        for w in list(self._conns):
            with contextlib.suppress(Exception):
                w.close()
        self._conns.clear()
        if self.server:
            self.server.close()
            with contextlib.suppress(Exception):
                await self.server.wait_closed()
            self.server = None

    async def _handle(self, reader, writer):
        self._conns.append(writer)
        buf = bytearray()
        try:
            while True:
                chunk = await reader.read(256)
                if not chunk:
                    break
                buf += chunk
                while True:
                    pkt = self._take(buf)
                    if pkt is None:
                        break
                    await self._reply(writer, pkt)
        except Exception:
            pass
        finally:
            with contextlib.suppress(ValueError):
                self._conns.remove(writer)
            with contextlib.suppress(Exception):
                writer.close()

    @staticmethod
    def _take(buf):
        if len(buf) < 3:
            return None
        nbits = buf[0] & 0x07
        if nbits == 7:
            if len(buf) < 4:
                return None
            total = 3 + buf[2] + 1
        else:
            total = 2 + nbits + 1
        if len(buf) < total:
            return None
        pkt = bytes(buf[:total])
        del buf[:total]
        return pkt

    async def _reply(self, writer, pkt):
        cmd = pkt[1]
        self.cmds.append(cmd)
        if self.silent:
            return
        writer.write(b"\x06")
        if cmd in self.QUERY_LEN:
            n = self.QUERY_LEN[cmd]
            if cmd == 162:
                data = bytes([(1 << 5) if self.status_on else 0, 0, 0, 0])
            elif cmd == 155:
                data = bytes([2])
            elif cmd == 193:
                data = bytes([0xE8, 0x03, 0x00])
            elif cmd == 196:
                data = bytes([10, 0])
            else:
                data = bytes(n)
            writer.write(_build_packet(1, cmd, data))
        else:
            writer.write(_build_packet(1, cmd, bytes([0])))
        with contextlib.suppress(Exception):
            await writer.drain()


class _RfpCfg:
    CMD_GAP_MS = 50
    ACK_TIMEOUT_MS = 300
    QUERY_TIMEOUT_MS = 500
    POLL_QUERY_TIMEOUT_MS = 500
    RFPULSE_RECONNECT_BACKOFF_START_MS = 200
    RFPULSE_RECONNECT_BACKOFF_MAX_MS = 200
    RFPULSE_WATCHDOG_INTERVAL_MS = 100
    RFPULSE_CONNECT_TIMEOUT_S = 1.0
    RFPULSE_DRAIN_TIMEOUT_S = 1.0
    RFPULSE_OFF_DEADLINE_S = 1.5
    RFPULSE_CLEANUP_OFF_WAIT_S = 2.0
    RFPULSE_UNUSED_OFF_WAIT_S = 0.6
    POLL_START_DELAY_AFTER_RF_ON_MS = 50
    RFPULSE_ADDR = 1
    RFPULSE_PULSE_MODE = 1
    DEBUG_PRINT = False
    RFPULSE_RAW_LOG = False


def _mk_rfp(port):
    d = RFPulseAsync(cfg=_RfpCfg())
    d.set_endpoint("127.0.0.1", port)
    return d


class _Events:
    def __init__(self, dev):
        self.evs = []
        self._t = asyncio.create_task(self._pump(dev))

    async def _pump(self, dev):
        with contextlib.suppress(asyncio.CancelledError):
            async for ev in dev.events():
                self.evs.append(ev)

    def stop(self):
        self._t.cancel()

    def kinds(self, kind, cmd=None):
        return [e for e in self.evs
                if e.kind == kind and (cmd is None or str(getattr(e, "cmd", "")) == cmd)]

    def status(self):
        return [e.message or "" for e in self.evs if e.kind == "status"]


async def _rf_shutdown(dev, ev):
    ev.stop()
    with contextlib.suppress(Exception):
        await asyncio.wait_for(dev.cleanup(), timeout=8.0)


def test_r1_unused_stop_normal_device_single_rf_off():
    async def _main():
        srv = FakeAEBus(); await srv.start()
        dev = _mk_rfp(srv.port); ev = _Events(dev)
        try:
            await dev.start()
            assert await _wait(lambda: dev.is_connected(), 3.0)
            assert dev.off_needs_confirm() is False
            srv.cmds.clear()
            t0 = time.perf_counter()
            dev.stop_unused()
            assert await _wait(lambda: ev.kinds("power_off_finished"), 3.0)
            dt = time.perf_counter() - t0
            await asyncio.sleep(0.2)
            assert len(ev.kinds("power_off_finished")) == 1
            assert ev.kinds("command_failed", "RF_OFF") == []
            assert srv.cmds.count(CMD_RF_OFF) == 1, srv.cmds
            assert any("켠 적 없음" in m and "CSR 0" in m for m in ev.status()), ev.status()
            assert dev.output_off_unconfirmed is False and dt < 2.0, dt
        finally:
            await _rf_shutdown(dev, ev); await srv.stop()
    asyncio.run(_main())


def test_r2_unused_stop_silent_device_no_alarm():
    """연결은 되는데 장비가 응답하지 않는 경우(10/08·10/11 과 같은 상황) — 대기 한도 안에 끝나고 경고 없음."""
    async def _main():
        srv = FakeAEBus(); await srv.start()
        dev = _mk_rfp(srv.port); ev = _Events(dev)
        try:
            await dev.start()
            assert await _wait(lambda: dev.is_connected(), 3.0)
            srv.silent = True
            srv.cmds.clear()
            t0 = time.perf_counter()
            dev.stop_unused()
            assert await _wait(lambda: ev.kinds("power_off_finished"), 3.0)
            dt = time.perf_counter() - t0
            await asyncio.sleep(0.2)
            assert dt < _RfpCfg.RFPULSE_UNUSED_OFF_WAIT_S + 1.0, dt
            assert len(ev.kinds("power_off_finished")) == 1
            assert ev.kinds("command_failed", "RF_OFF") == []
            assert CMD_RF_OFF in srv.cmds, srv.cmds              # 1회는 보냈다(응답이 없었을 뿐)
            assert any("켠 적 없음" in m and "응답 없음" in m for m in ev.status()), ev.status()
            assert dev.output_off_unconfirmed is False
        finally:
            await _rf_shutdown(dev, ev); await srv.stop()
    asyncio.run(_main())


def test_r3_unused_stop_server_down_no_alarm():
    """장비 쪽 연결 자체가 안 되는 경우도 경고 없이 끝난다(같은 상황에서 stop_process 는 command_failed)."""
    async def _main():
        srv = FakeAEBus(); await srv.start()
        dev = _mk_rfp(srv.port); ev = _Events(dev)
        try:
            await dev.start()
            assert await _wait(lambda: dev.is_connected(), 3.0)
            await srv.stop()
            await _wait(lambda: not dev.is_connected(), 3.0)
            dev.stop_unused()
            assert await _wait(lambda: ev.kinds("power_off_finished"), 4.0)
            await asyncio.sleep(0.2)
            assert ev.kinds("command_failed", "RF_OFF") == []
            assert dev.output_off_unconfirmed is False
        finally:
            await _rf_shutdown(dev, ev)
    asyncio.run(_main())


def test_r4_race_rf_on_during_wait_falls_back_to_confirm():
    """기다리는 사이 RF_ON 이 나가면(_output_maybe_on=True) 확인형 OFF 로 넘어가고, 무응답이면 기존 경고가 난다."""
    async def _main():
        srv = FakeAEBus(); await srv.start()
        dev = _mk_rfp(srv.port); ev = _Events(dev)
        try:
            await dev.start()
            assert await _wait(lambda: dev.is_connected(), 3.0)
            srv.silent = True
            dev.stop_unused()
            dev._output_maybe_on = True                          # 대기 중 RF_ON 이 나갔다고 가정
            assert await _wait(lambda: ev.kinds("command_failed", "RF_OFF"), 6.0)
            await asyncio.sleep(0.2)
            assert len(ev.kinds("command_failed", "RF_OFF")) == 1
            assert ev.kinds("power_off_finished") == []
            assert any("확인형 RF OFF 로 전환" in m for m in ev.status()), ev.status()
            assert dev.output_off_unconfirmed is True
        finally:
            await _rf_shutdown(dev, ev); await srv.stop()
    asyncio.run(_main())


def test_r5_after_rf_on_needs_confirm_until_confirmed_off():
    async def _main():
        srv = FakeAEBus(); await srv.start()
        dev = _mk_rfp(srv.port); ev = _Events(dev)
        try:
            await dev.start()
            assert await _wait(lambda: dev.is_connected(), 3.0)
            ok = await dev.start_pulse_process(100, freq_hz=1000, duty_percent=10)
            assert ok is True and CMD_RF_ON in srv.cmds
            assert dev.off_needs_confirm() is True               # 켠 런 → 기존 확인형 OFF 대상
            dev.stop_process()
            assert await _wait(lambda: ev.kinds("power_off_finished"), 4.0)
            assert dev.off_needs_confirm() is False              # OFF 확인 뒤에는 다시 '켠 적 없음'
        finally:
            await _rf_shutdown(dev, ev); await srv.stop()
    asyncio.run(_main())


def test_r6_double_stop_unused_single_sequence():
    async def _main():
        srv = FakeAEBus(); await srv.start()
        dev = _mk_rfp(srv.port); ev = _Events(dev)
        try:
            await dev.start()
            assert await _wait(lambda: dev.is_connected(), 3.0)
            srv.cmds.clear()
            dev.stop_unused()
            first = dev._off_task
            dev.stop_unused()
            dev.stop_process()                                   # 섞어 불러도 OFF 시퀀스는 1개
            assert dev._off_task is first
            assert await _wait(lambda: ev.kinds("power_off_finished"), 3.0)
            await asyncio.sleep(0.3)
            assert len(ev.kinds("power_off_finished")) == 1
            assert srv.cmds.count(CMD_RF_OFF) == 1, srv.cmds
        finally:
            await _rf_shutdown(dev, ev); await srv.stop()
    asyncio.run(_main())


# ═════════════════════════════ 런타임 연결 ═════════════════════════════
def _nested_func(path, name):
    """메서드 안에 정의된 콜백(cb_dc_pulse_stop 등)의 실제 소스를 꺼낸다."""
    src = open(path, encoding="utf-8").read()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return textwrap.dedent(ast.get_source_segment(src, node))
    raise AssertionError(f"{name} 없음")


class _FakeDcp:
    def __init__(self, needs, has_method=True):
        self.calls = []
        if has_method:
            self.off_needs_confirm = lambda: needs

    async def output_off(self):
        self.calls.append("output_off")

    async def output_off_unused(self):
        self.calls.append("output_off_unused")


class _FakeRfp:
    def __init__(self, needs, has_method=True):
        self.calls = []
        if has_method:
            self.off_needs_confirm = lambda: needs

    def stop_process(self):
        self.calls.append("stop_process")

    def stop_unused(self):
        self.calls.append("stop_unused")


class _Self:
    def __init__(self, dcp=None, rfp=None):
        self.dc_pulse = dcp
        self.rf_pulse = rfp
        self.ch = 2
        self.logs = []
        self.coros = []
        self.process_controller = type("_PC", (), {
            "on_dc_pulse_failed": lambda *a, **k: None,
            "on_rf_pulse_failed": lambda *a, **k: None})()

    def append_log(self, src, msg):
        self.logs.append((src, msg))

    def _spawn_detached(self, coro, **k):
        self.coros.append(coro)


def _run_cb(name, me):
    path = os.path.join(_ROOT, "runtime", "chamber_runtime.py")
    ns = {"self": me, "asyncio": asyncio, "contextlib": contextlib}
    exec(compile(_nested_func(path, name), path, "exec"), ns)
    ns[name]()
    for c in me.coros:
        asyncio.run(c)


@pytest.mark.parametrize("needs,has_method,expect", [
    (False, True, ["output_off_unused"]),     # 켠 적 없음 → 1회 OFF
    (True, True, ["output_off"]),             # 켠 적 있음/미확인 → 기존 확인형
    (False, False, ["output_off"]),           # 판정 함수가 없는 장치 → 기존 동작
])
def test_w1_runtime_dc_pulse_stop_routes(needs, has_method, expect):
    me = _Self(dcp=_FakeDcp(needs, has_method))
    _run_cb("cb_dc_pulse_stop", me)
    assert me.dc_pulse.calls == expect


@pytest.mark.parametrize("needs,has_method,expect", [
    (False, True, ["stop_unused"]),
    (True, True, ["stop_process"]),
    (False, False, ["stop_process"]),
])
def test_w2_runtime_rf_pulse_stop_routes(needs, has_method, expect):
    me = _Self(rfp=_FakeRfp(needs, has_method))
    _run_cb("cb_rf_pulse_stop", me)
    assert me.rf_pulse.calls == expect


def test_w3_dc_pulse_pump_finishes_stop_step_on_unused_confirm():
    """DC Pulse 이벤트 펌프: command_confirmed(OUTPUT_OFF_UNUSED) → on_dc_pulse_off_finished(정지 단계 완료), 경고 없음."""
    from runtime.chamber_runtime import ChamberRuntime

    class _PC:
        def __init__(self):
            self.calls = []

        def __getattr__(self, name):
            def _f(*a, **k):
                self.calls.append(name)
            return _f

    class _Chat:
        def __init__(self):
            self.cards = []

        def notify_error_with_src(self, src, msg):
            self.cards.append((src, msg))

        def flush(self):
            pass

    class _Dev:
        async def events(self):
            yield DCPEvent(kind="command_confirmed", cmd="OUTPUT_OFF_UNUSED")

    c = ChamberRuntime.__new__(ChamberRuntime)
    c.ch = 2
    c.chat = _Chat()
    c.process_controller = _PC()
    c.logs = []
    c.append_log = lambda src, msg: c.logs.append(f"[{src}] {msg}")
    c.dc_pulse = _Dev()
    asyncio.run(c._pump_dcpulse_events())
    assert c.process_controller.calls == ["on_dc_pulse_off_finished"]
    assert c.chat.cards == []


def test_c1_config_keys_reach_devices():
    """대기 설정 키(DCP_UNUSED_OFF_WAIT_S / RFPULSE_UNUSED_OFF_WAIT_S)가 채널 설정(CH1/CH2)에 보이고 장치가 그 값을 읽는다."""
    import lib.config_ch1 as c1
    import lib.config_ch2 as c2
    for m in (c1, c2):
        d = AsyncDCPulse(host="127.0.0.1", port=9, cfg=m)
        r = RFPulseAsync(cfg=m)
        assert d._cfg_float("DCP_UNUSED_OFF_WAIT_S", -1.0) == float(m.DCP_UNUSED_OFF_WAIT_S) > 0
        assert r._cfg_float("RFPULSE_UNUSED_OFF_WAIT_S", -1.0) == float(m.RFPULSE_UNUSED_OFF_WAIT_S) > 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-s"]))

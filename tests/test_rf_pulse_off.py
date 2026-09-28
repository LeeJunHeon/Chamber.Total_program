# -*- coding: utf-8 -*-
"""RF Pulse 출력 OFF 근본 수정 검증 (2026-09-23 02:15 CH1: RF_OFF 가 240초 동안 전송조차 안 된 사고).

127.0.0.1 가짜 AE Bus TCP 서버로 실제 device/rf_pulse.py 경로를 그대로 돌린다.
  exec  → ACK(0x06) + CSR 프레임
  query → ACK(0x06) + 데이터 프레임 (155→1B, 162→4B, 165/166→2B, 193→3B, 196→2B)
"""
import os
import sys
import time
import asyncio
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                  # noqa: E402
import device.rf_pulse as RFP                                  # noqa: E402
from device.rf_pulse import RFPulseAsync, _build_packet        # noqa: E402

CMD_RF_OFF = RFP.CMD_RF_OFF          # 1
CMD_RF_ON = RFP.CMD_RF_ON            # 2
CMD_STATUS = RFP.CMD_REPORT_STATUS   # 162


# ───────────────────────── 가짜 AE Bus 서버 ─────────────────────────
class FakeAEBus:
    """받은 명령 번호와 연결 수락 횟수를 기록한다."""

    QUERY_LEN = {155: 1, 162: 4, 165: 2, 166: 2, 193: 3, 196: 2}

    def __init__(self):
        self.server = None
        self.port = 0
        self.accepts = 0
        self.cmds: list[int] = []            # 수신한 명령 번호(순서대로)
        self.csr_for: dict[int, int] = {}    # {cmd: CSR} — 기본 0
        self.status_on = True                # 162 응답의 rf_output_on 비트
        self.reject_until = 0.0              # 이 시각까지는 새 연결을 즉시 끊는다
        self._conns: list[asyncio.StreamWriter] = []

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

    def kill_connections(self):
        for w in list(self._conns):
            with contextlib.suppress(Exception):
                w.close()
        self._conns.clear()

    async def _handle(self, reader, writer):
        self.accepts += 1
        if time.monotonic() < self.reject_until:
            with contextlib.suppress(Exception):
                writer.close()
            return
        self._conns.append(writer)
        buf = bytearray()
        try:
            while True:
                chunk = await reader.read(256)
                if not chunk:
                    break
                buf += chunk
                while True:
                    pkt = self._take_packet(buf)
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
    def _take_packet(buf: bytearray):
        if len(buf) < 3:
            return None
        hdr = buf[0]
        nbits = hdr & 0x07
        if nbits == 7:
            if len(buf) < 4:
                return None
            dlen = buf[2]
            total = 3 + dlen + 1
        else:
            dlen = nbits
            total = 2 + dlen + 1
        if len(buf) < total:
            return None
        pkt = bytes(buf[:total])
        del buf[:total]
        return pkt

    async def _reply(self, writer, pkt: bytes):
        cmd = pkt[1]
        self.cmds.append(cmd)
        writer.write(b"\x06")                                  # ACK
        if cmd in self.QUERY_LEN:
            n = self.QUERY_LEN[cmd]
            if cmd == 162:
                b1 = (1 << 5) if self.status_on else 0         # rf_output_on
                data = bytes([b1, 0, 0, 0])
            elif cmd == 155:
                data = bytes([2])                              # HOST 모드
            elif cmd == 193:
                data = bytes([0xE8, 0x03, 0x00])               # 1000 Hz
            elif cmd == 196:
                data = bytes([10, 0])                          # 10 %
            else:
                data = bytes(n)
            writer.write(_build_packet(1, cmd, data))
        else:
            csr = int(self.csr_for.get(cmd, 0))
            writer.write(_build_packet(1, cmd, bytes([csr])))  # CSR 프레임
        with contextlib.suppress(Exception):
            await writer.drain()


class _Cfg:
    """테스트용 짧은 타이밍 cfg."""
    CMD_GAP_MS = 50
    ACK_TIMEOUT_MS = 300
    QUERY_TIMEOUT_MS = 500
    POLL_QUERY_TIMEOUT_MS = 500
    RFPULSE_RECONNECT_BACKOFF_START_MS = 200
    RFPULSE_RECONNECT_BACKOFF_MAX_MS = 200
    RFPULSE_WATCHDOG_INTERVAL_MS = 100
    RFPULSE_CONNECT_TIMEOUT_S = 1.0
    RFPULSE_DRAIN_TIMEOUT_S = 1.0
    RFPULSE_OFF_DEADLINE_S = 3.0
    RFPULSE_CLEANUP_OFF_WAIT_S = 2.0
    POLL_START_DELAY_AFTER_RF_ON_MS = 50
    RFPULSE_ADDR = 1
    RFPULSE_PULSE_MODE = 1
    DEBUG_PRINT = False
    RFPULSE_RAW_LOG = False


def _mk(port: int):
    d = RFPulseAsync(cfg=_Cfg())
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


async def _wait(cond, timeout=5.0, step=0.02):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if cond():
            return True
        await asyncio.sleep(step)
    return False


async def _shutdown(dev, ev):
    ev.stop()
    with contextlib.suppress(Exception):
        await asyncio.wait_for(dev.cleanup(), timeout=8.0)


# ───────────────────────── (a) 정상 ─────────────────────────
def test_a_normal_stop_sends_rf_off_and_finishes():
    async def _main():
        srv = FakeAEBus(); await srv.start()
        dev = _mk(srv.port); ev = _Events(dev)
        try:
            await dev.start()
            assert await _wait(lambda: dev.is_connected(), 3.0)
            t0 = time.perf_counter()
            dev.stop_process()
            assert await _wait(lambda: ev.kinds("power_off_finished"), 4.0)
            dt = time.perf_counter() - t0
            assert srv.cmds.count(CMD_RF_OFF) == 1, srv.cmds
            assert len(ev.kinds("power_off_finished")) == 1
            assert ev.kinds("command_failed", "RF_OFF") == []
            assert dev._want_connected is True, "OFF 도중에는 재연결을 끊지 않는다"
            assert dev.output_off_unconfirmed is False
            print(f"  (a) {dt:.2f}s RF_OFF={srv.cmds.count(CMD_RF_OFF)}")
        finally:
            await _shutdown(dev, ev); await srv.stop()
    asyncio.run(_main())


# ───────────────────────── (b) 9/23 재현 ─────────────────────────
def test_b_incident_disconnected_then_recovers():
    """끊긴 상태 + 0.5초 동안 새 연결 거절 → 서버가 받기 시작하면 RF_OFF 전송, deadline 안에 완료."""
    async def _main():
        srv = FakeAEBus(); await srv.start()
        dev = _mk(srv.port); ev = _Events(dev)
        try:
            await dev.start()
            assert await _wait(lambda: dev.is_connected(), 3.0)
            srv.cmds.clear()
            srv.reject_until = time.monotonic() + 0.5      # 새 연결을 바로 끊는다
            srv.kill_connections()                          # 링크 상실
            await _wait(lambda: not dev.is_connected(), 2.0)
            t0 = time.perf_counter()
            dev.stop_process()
            assert await _wait(lambda: ev.kinds("power_off_finished"), 4.0), \
                [f"{e.kind}:{getattr(e,'message',None) or getattr(e,'reason',None)}" for e in ev.evs]
            dt = time.perf_counter() - t0
            assert CMD_RF_OFF in srv.cmds
            assert len(ev.kinds("power_off_finished")) == 1
            assert dev._want_connected is True
            print(f"  (b) {dt:.2f}s accepts={srv.accepts} RF_OFF={srv.cmds.count(CMD_RF_OFF)}")
        finally:
            await _shutdown(dev, ev); await srv.stop()
    asyncio.run(_main())


# ───────────────────────── (c) 연결 불가 ─────────────────────────
def test_c_server_down_fails_with_command_failed():
    async def _main():
        srv = FakeAEBus(); await srv.start()
        port = srv.port
        dev = _mk(port); ev = _Events(dev)
        try:
            await dev.start()
            assert await _wait(lambda: dev.is_connected(), 3.0)
            await srv.stop()                                # 서버 중지
            await _wait(lambda: not dev.is_connected(), 3.0)
            t0 = time.perf_counter()
            dev.stop_process()
            assert await _wait(lambda: ev.kinds("command_failed", "RF_OFF"), 8.0)
            dt = time.perf_counter() - t0
            assert len(ev.kinds("command_failed", "RF_OFF")) == 1
            assert ev.kinds("power_off_finished") == []
            assert dev.output_off_unconfirmed is True
            print(f"  (c) {dt:.2f}s (deadline {_Cfg.RFPULSE_OFF_DEADLINE_S}s)")
        finally:
            await _shutdown(dev, ev)
    asyncio.run(_main())


# ───────────────────────── (d) stop 직후 폴링 off ─────────────────────────
def test_d_set_process_status_false_keeps_rf_off():
    async def _main():
        srv = FakeAEBus(); await srv.start()
        dev = _mk(srv.port); ev = _Events(dev)
        try:
            await dev.start()
            assert await _wait(lambda: dev.is_connected(), 3.0)
            srv.cmds.clear()
            dev.stop_process()
            dev.set_process_status(False)                   # 폴링 조회만 지워야 한다
            assert await _wait(lambda: ev.kinds("power_off_finished"), 4.0)
            assert srv.cmds.count(CMD_RF_OFF) == 1, srv.cmds
            print(f"  (d) RF_OFF={srv.cmds.count(CMD_RF_OFF)} cmds={srv.cmds}")
        finally:
            await _shutdown(dev, ev); await srv.stop()
    asyncio.run(_main())


# ───────────────────────── (e) 시작 시퀀스 중 정지 ─────────────────────────
def test_e_stop_during_start_blocks_rf_on():
    async def _main():
        srv = FakeAEBus(); await srv.start()
        dev = _mk(srv.port); ev = _Events(dev)
        try:
            await dev.start()
            assert await _wait(lambda: dev.is_connected(), 3.0)

            async def _stopper():
                # MODE(4) 또는 SETP(8) 를 서버가 받은 시점에 정지
                await _wait(lambda: any(c in (RFP.CMD_SET_CTRL_MODE, RFP.CMD_SET_SETPOINT)
                                        for c in srv.cmds), 5.0)
                dev.stop_process()
            st = asyncio.create_task(_stopper())
            ok = await dev.start_pulse_process(100, freq_hz=1000, duty_percent=10)
            await st
            await asyncio.sleep(0.15)          # 이벤트 펌프가 큐를 비울 틈
            assert ok is False
            assert CMD_RF_ON not in srv.cmds, srv.cmds
            assert ev.kinds("command_failed", "START_SEQUENCE") == []
            assert ev.kinds("target_reached") == []
            assert any("시작 시퀀스 중단" in (e.message or "") for e in ev.kinds("status"))
            print(f"  (e) RF_ON={srv.cmds.count(CMD_RF_ON)} cmds={srv.cmds}")
        finally:
            await _shutdown(dev, ev); await srv.stop()
    asyncio.run(_main())


# ───────────────────────── (f) stop 없이 cleanup ─────────────────────────
def test_f_cleanup_without_stop_sends_rf_off():
    async def _main():
        srv = FakeAEBus(); await srv.start()
        dev = _mk(srv.port); ev = _Events(dev)
        try:
            await dev.start()
            assert await _wait(lambda: dev.is_connected(), 3.0)
            ok = await dev.start_pulse_process(100, freq_hz=1000, duty_percent=10)
            assert ok is True and CMD_RF_ON in srv.cmds
            assert dev._output_maybe_on is True
            srv.status_on = False                           # OFF 뒤 STATUS 는 off
            srv.cmds.clear()
            ev.stop()
            t0 = time.perf_counter()
            await asyncio.wait_for(dev.cleanup(), timeout=8.0)
            dt = time.perf_counter() - t0
            assert CMD_RF_OFF in srv.cmds, srv.cmds
            assert dev.cleanup_off_unconfirmed is False
            assert dev._output_maybe_on is False
            print(f"  (f) {dt:.2f}s cmds={srv.cmds}")
        finally:
            with contextlib.suppress(Exception):
                ev.stop()
            await srv.stop()
    asyncio.run(_main())


# ───────────────────────── (g) 안 쓴 장치는 연결하지 않는다 ─────────────────────────
def test_g_cleanup_does_not_connect_when_worker_absent():
    async def _main():
        srv = FakeAEBus(); await srv.start()
        dev = _mk(srv.port)
        try:
            dev._output_maybe_on = True                     # 출력이 켜져 있을 수 있다고 표시만
            assert dev._worker_alive() is False
            await asyncio.wait_for(dev.cleanup(), timeout=5.0)
            assert srv.accepts == 0, f"연결 시도 {srv.accepts}회"
            assert srv.cmds == []
            assert dev.cleanup_off_unconfirmed is True      # 확인 못 했음을 남긴다
            print(f"  (g) accepts={srv.accepts} cleanup_off_unconfirmed={dev.cleanup_off_unconfirmed}")
        finally:
            await srv.stop()
    asyncio.run(_main())


# ───────────────────────── (h) stop 두 번 ─────────────────────────
def test_h_double_stop_single_sequence():
    async def _main():
        srv = FakeAEBus(); await srv.start()
        dev = _mk(srv.port); ev = _Events(dev)
        try:
            await dev.start()
            assert await _wait(lambda: dev.is_connected(), 3.0)
            srv.cmds.clear()
            dev.stop_process()
            first = dev._off_task
            dev.stop_process()
            assert dev._off_task is first, "OFF 시퀀스는 1개만"
            assert await _wait(lambda: ev.kinds("power_off_finished"), 4.0)
            await asyncio.sleep(0.3)
            assert len(ev.kinds("power_off_finished")) == 1
            assert ev.kinds("command_failed", "RF_OFF") == []
            assert srv.cmds.count(CMD_RF_OFF) == 1, srv.cmds
            print(f"  (h) off_finished={len(ev.kinds('power_off_finished'))} RF_OFF={srv.cmds.count(CMD_RF_OFF)}")
        finally:
            await _shutdown(dev, ev); await srv.stop()
    asyncio.run(_main())


# ───────────────────────── (i) CSR 거부 → STATUS 확인 ─────────────────────────
def test_i_csr_reject_then_status_confirms_off():
    async def _main():
        srv = FakeAEBus(); await srv.start()
        srv.csr_for[CMD_RF_OFF] = 9                         # RF_OFF 를 거부
        srv.status_on = False                               # 실제로는 출력 OFF
        dev = _mk(srv.port); ev = _Events(dev)
        try:
            await dev.start()
            assert await _wait(lambda: dev.is_connected(), 3.0)
            srv.cmds.clear()
            dev.stop_process()
            assert await _wait(lambda: ev.kinds("power_off_finished"), 5.0), \
                [f"{e.kind}:{getattr(e,'message',None) or getattr(e,'reason',None)}" for e in ev.evs]
            assert CMD_STATUS in srv.cmds, srv.cmds
            assert len(ev.kinds("power_off_finished")) == 1
            print(f"  (i) cmds={srv.cmds[:8]}")
        finally:
            await _shutdown(dev, ev); await srv.stop()
    asyncio.run(_main())


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-s"]))

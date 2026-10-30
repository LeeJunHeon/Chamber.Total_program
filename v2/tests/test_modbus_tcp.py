"""Modbus TCP 프레임(MBAP) — PDU 가 RTU 와 같은 바이트, 트랜잭션 id, 늦은 응답 버리기, 읽기 전용, 실제 소켓(127.0.0.1)."""
import socket
import struct
import threading

import pytest

from v2.common import modbus_tcp as MT
from v2.common.clock import RealClock
from v2.common.modbus_rtu import ModbusException, ReadOnlyViolation
from v2.common.transport import TcpSettings, TcpTransport


class _Sess:
    """worker Session 흉내 — clock·deadline·write·read_until·transport."""
    def __init__(self, tr, clock, budget=2.0):
        self.transport, self.clock = tr, clock
        self.deadline = clock.monotonic() + budget

    def write(self, b):
        self.transport.write(b)

    def read_until(self, deadline, terminator=None, size=None):
        return self.transport.read_until(deadline, terminator, size)


def test_pdu_bytes():
    assert MT.pdu_read_coils(0x10, 8) == bytes([1, 0, 0x10, 0, 8])
    assert MT.pdu_write_coil(5, True) == bytes([5, 0, 5, 0xFF, 0])
    assert MT.mbap(1, 1, MT.pdu_read_regs(100, 2)) == bytes([0, 1, 0, 0, 0, 6, 1, 3, 0, 100, 0, 2])


def test_parse_errors():
    with pytest.raises(ModbusException):
        MT.parse_pdu(bytes([0x83, 2]), 3, 1)
    assert MT.parse_pdu(bytes([1, 1, 0b101]), 1, 3) == [True, False, True]


def _server(handler):
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)

    def run():
        c, _ = srv.accept()
        buf = b""
        while True:
            d = c.recv(1024)
            if not d:
                break
            buf += d
            while True:
                got = MT.split_request(buf)
                if not got:
                    break
                tid, unit, pdu, buf = got
                for out in handler(tid, unit, pdu):
                    c.sendall(out)
        c.close()
    threading.Thread(target=run, daemon=True).start()
    return srv.getsockname()[1], srv


def test_real_socket_roundtrip_and_stale_drop():
    regs = {100: 7, 101: 9}

    def h(tid, unit, pdu):
        if pdu[0] == 3:
            addr, n = struct.unpack(">HH", pdu[1:5])
            body = bytes([3, 2 * n]) + b"".join(struct.pack(">H", regs.get(addr + i, 0)) for i in range(n))
            stale = MT.mbap((tid - 1) or 0xFFFF, unit, bytes([3, 2 * n]) + b"\0" * (2 * n))
            return [stale, MT.mbap(tid, unit, body)]           # 늦은 앞 응답이 먼저 온다
        if pdu[0] == 5:
            return [MT.mbap(tid, unit, pdu)]
        return []
    port, srv = _server(h)
    clock = RealClock()
    tr = TcpTransport(clock, TcpSettings("127.0.0.1", port))
    tr.open()
    try:
        cl = MT.ModbusTcpClient(_Sess(tr, clock), unit=1)
        assert cl.read_regs(100, 2) == [7, 9]
        cl.write_coil(3, True)
        assert cl.transactions == 2
        ro = MT.ModbusTcpClient(_Sess(tr, clock), unit=1, read_only=True)
        with pytest.raises(ReadOnlyViolation):
            ro.write_coil(3, False)
    finally:
        tr.close()
        srv.close()

# -*- coding: utf-8 -*-
"""DC Pulse OUTPUT_OFF 확인 — 장비 STATUS 가 HV On 이면 출력 전력(P)이 0 이어도 '꺼짐'으로 보지 않는다.

예전: _confirm_off_quick 이 STATUS=HV On 인데도 P==0 이면 OK 를 돌려줘, OUTPUT_OFF 가 ERR(04)로 거절됐는데
1회 시도로 OUTPUT_OFF_VERIFIED → 재시도(세션 재연결·재전송)와 '출력 상태 미확인' 경보 없이 HV 가 켜진 채 끝났다.
P==0 보조판정은 STATUS 를 읽지 못할 때만 쓴다(독스트링대로). 장치·네트워크 접근 없음(로컬 가짜 EP5).
"""
import os
import sys
import asyncio
import contextlib

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from lib import config_ch2                                 # noqa: E402
from device.dc_pulse import AsyncDCPulse, _chk_nibble_sum  # noqa: E402


def _frame(cmd, data=b""):
    core = b"\x02" + bytes([cmd]) + data + b"\x03"
    return core + bytes([_chk_nibble_sum(core)])


class _EP5:
    """EP5 RS-232 Type4 프레임을 TCP 로 흉내 — OUTPUT_OFF 거절 횟수·STATUS 무응답을 고를 수 있다."""

    def __init__(self, *, reject_off=0, status_silent=False):
        self.hv_on = True              # 출력 켜짐
        self.reject_off = reject_off   # 앞에서부터 이 횟수만큼 OUTPUT_OFF 를 ERR(04)로 거절
        self.status_silent = status_silent
        self.off_cmds = 0

    async def handle(self, r, w):
        buf = bytearray()
        try:
            while True:
                b = await r.read(64)
                if not b:
                    break
                buf += b
                while True:
                    try:
                        i = buf.index(0x02)
                    except ValueError:
                        buf.clear()
                        break
                    del buf[:i]
                    try:
                        j = buf.index(0x03, 2)
                    except ValueError:
                        break
                    if len(buf) < j + 2:
                        break
                    pkt = bytes(buf[:j + 2])
                    del buf[:j + 2]
                    cmd, data = pkt[1], pkt[2:j]
                    if cmd == 0x80:
                        val = (data[0] << 8) | data[1]
                        if val == 2:
                            self.off_cmds += 1
                            if self.off_cmds <= self.reject_off:
                                w.write(b"\x04")
                            else:
                                self.hv_on = False
                                w.write(b"\x06")
                        else:
                            self.hv_on = (val == 1)
                            w.write(b"\x06")
                    elif cmd == 0x90:
                        if self.status_silent:
                            continue
                        w.write(_frame(0x90, bytes([0x00, 0x01 if self.hv_on else 0x00])))
                    elif cmd == 0x9A:                      # P=0, I=0, V=600 (켜져 있어도 방전 없음)
                        w.write(_frame(0x9A, bytes([0, 0, 0, 0, 600 >> 8, 600 & 255])))
                    elif cmd == 0x9E:
                        w.write(_frame(cmd, b"\x00\x00"))
                    elif cmd in (0x96, 0x99, 0x9C, 0xA5, 0xA6, 0xA7, 0xAE, 0xAF, 0xBB, 0xBC, 0xBD, 0x91):
                        w.write(_frame(cmd, b"\x00\x01" if cmd == 0x9C else b"\x00\x00"))
                    else:
                        w.write(b"\x06")
                    await w.drain()
        except Exception:
            pass


async def _off(dev):
    srv = await asyncio.start_server(dev.handle, "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    dcp = AsyncDCPulse(host="127.0.0.1", port=port, cfg=config_ch2)
    await dcp.start()
    try:
        assert await dcp._wait_until_connected(3.0)
        dcp._out_on = True
        dcp._output_maybe_on = True                       # 공정 중 출력이 켜진 상태
        ok = await asyncio.wait_for(dcp.output_off(), timeout=120)
        cmds = []
        while not dcp._event_q.empty():
            e = dcp._event_q.get_nowait()
            if e.kind in ("command_confirmed", "command_failed"):
                cmds.append((e.kind, e.cmd))
        return ok, dcp.output_off_unconfirmed, cmds
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(dcp.cleanup(), timeout=10)
        srv.close()


def test_off_rejected_hv_on_p0_is_not_confirmed():
    dev = _EP5(reject_off=10_000)
    ok, unconfirmed, cmds = asyncio.run(_off(dev))
    assert ok is False and unconfirmed is True, cmds      # 예전: True(OUTPUT_OFF_VERIFIED), 미확인 경보 없음
    assert ("command_confirmed", "OUTPUT_OFF_VERIFIED") not in cmds
    assert dev.off_cmds >= 2                              # 재시도(세션 재연결 후 재전송)를 탔다


def test_off_rejected_once_then_accepted_on_retry():
    dev = _EP5(reject_off=1)
    ok, unconfirmed, cmds = asyncio.run(_off(dev))
    assert ok is True and unconfirmed is False, cmds
    assert dev.off_cmds == 2 and dev.hv_on is False      # 예전: 1회로 끝, HV 켜진 채


def test_off_ack_unchanged():
    dev = _EP5(reject_off=0)
    ok, unconfirmed, cmds = asyncio.run(_off(dev))
    assert ok is True and unconfirmed is False and dev.off_cmds == 1, cmds


@pytest.mark.parametrize("flags,p,expect", [
    (0x0000, 0.0, True),     # HV Off → 꺼짐 (그대로)
    (0x0001, 0.0, False),    # HV On, P==0 → 아직 켜짐 (예전: True)
    (0x0001, 250.0, False),  # HV On, 방전 중 → 아직 켜짐 (그대로)
    (None, 0.0, True),       # STATUS 못 읽음 → 보조판정 P==0 → 꺼짐 (그대로)
    (None, 12.0, False),     # STATUS 못 읽음, P>0 → 아직 켜짐 (그대로)
])
def test_confirm_off_quick_table(flags, p, expect):
    dcp = AsyncDCPulse.__new__(AsyncDCPulse)

    async def read_flags():
        return flags

    async def read_piv():
        return {"eng": {"P_W": p}}
    dcp.read_status_flags = read_flags
    dcp.read_output_piv = read_piv
    ok, _p, _hv = asyncio.run(dcp._confirm_off_quick())
    assert ok is expect

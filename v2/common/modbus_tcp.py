"""Modbus TCP (장비 공통) — 마스터 쪽 MBAP 프레임 만들기·풀기와 한 번 주고받기. 의존성 없음.

챔버12 PLC 는 MOXA 를 거치지 않고 192.168.1.2:502 에 직결(v1 device/plc.py:315·377 — pymodbus ModbusTcpClient).
v2 는 pymodbus 를 쓰지 않고 직접 프레임을 만든다(의존성 최소). PDU(함수·주소·값)는 modbus_rtu 와 같은 바이트이고,
앞에 MBAP 머리 7바이트가 붙고 CRC 가 없다:
  MBAP = 트랜잭션 id(2, 빅엔디언) + 프로토콜 id 0(2) + 뒤 길이(2, 유닛 id 포함) + 유닛 id(1)
트랜잭션 id 는 1 부터 하나씩(65535 다음 1). v1 pymodbus 도 요청마다 늘린다 — 시작 값만 다를 수 있다(의도된 차이 표).
응답은 MBAP 7바이트를 먼저 읽고 길이만큼 더 읽는다. 트랜잭션 id 가 다르면(늦게 온 앞 응답) 버리고 같은 마감까지 다시 읽는다.
읽기 전용(read_only=True)은 쓰기 함수(5·6·15·16)를 보내지 않는다 — bench 보호(modbus_rtu 와 같음).
"""
from __future__ import annotations

import struct
from typing import List, Optional, Sequence

from v2.common.modbus_rtu import (READ_COILS, READ_REGS, WRITE_COIL, WRITE_COILS, WRITE_FUNCS, WRITE_REG, WRITE_REGS,
                                  FrameError, ModbusException, ReadOnlyViolation, req_read_coils, req_read_regs,
                                  req_write_coil, req_write_coils, req_write_reg, req_write_regs)
from v2.common.transport import ReadTimeout

MBAP_LEN = 7


def pdu_of_rtu(rtu_frame: bytes) -> bytes:
    """modbus_rtu 요청(국번 + PDU + CRC) → PDU(함수 + 데이터)."""
    return rtu_frame[1:-2]


def mbap(tid: int, unit: int, pdu: bytes) -> bytes:
    return struct.pack(">HHHB", tid & 0xFFFF, 0, len(pdu) + 1, unit) + pdu


def pdu_read_coils(addr: int, count: int) -> bytes:
    return pdu_of_rtu(req_read_coils(0, addr, count))


def pdu_read_regs(addr: int, count: int) -> bytes:
    return pdu_of_rtu(req_read_regs(0, addr, count))


def pdu_write_coil(addr: int, on: bool) -> bytes:
    return pdu_of_rtu(req_write_coil(0, addr, on))


def pdu_write_reg(addr: int, value: int) -> bytes:
    return pdu_of_rtu(req_write_reg(0, addr, value))


def pdu_write_coils(addr: int, values: Sequence[bool]) -> bytes:
    return pdu_of_rtu(req_write_coils(0, addr, values))


def pdu_write_regs(addr: int, values: Sequence[int]) -> bytes:
    return pdu_of_rtu(req_write_regs(0, addr, values))


def parse_pdu(pdu: bytes, function: int, count: int = 0):
    """응답 PDU → 읽기 1 [bool]*count, 읽기 3 [int]*count, 쓰기 (주소, 값/개수)."""
    if not pdu:
        raise FrameError("빈 PDU")
    if pdu[0] == (function | 0x80):
        raise ModbusException(function, pdu[1] if len(pdu) > 1 else 0)
    if pdu[0] != function:
        raise FrameError(f"함수 {pdu[0]} ≠ {function}")
    if function == READ_COILS:
        nb = (count + 7) // 8
        if len(pdu) != 2 + nb or pdu[1] != nb:
            raise FrameError("바이트 수 불일치")
        return [bool(pdu[2 + i // 8] >> (i % 8) & 1) for i in range(count)]
    if function == READ_REGS:
        if len(pdu) != 2 + 2 * count or pdu[1] != 2 * count:
            raise FrameError("바이트 수 불일치")
        return list(struct.unpack(">" + "H" * count, pdu[2:2 + 2 * count]))
    if function in WRITE_FUNCS:
        if len(pdu) != 5:
            raise FrameError(f"쓰기 응답 길이 {len(pdu)}")
        return struct.unpack(">HH", pdu[1:5])
    raise FrameError(f"지원하지 않는 함수 {function}")


class ModbusTcpClient:
    """Session(worker) 위에서 한 번 주고받기 — modbus_rtu.ModbusClient 와 같은 모양(드라이버가 둘을 바꿔 쓸 수 있게)."""

    def __init__(self, session, unit: int = 1, txn_timeout_s: float = 1.0, retries: int = 1,
                 retry_delay_s: float = 0.05, read_only: bool = False, on_frame=None) -> None:
        self.s = session
        self.unit = unit
        self.txn_timeout_s = txn_timeout_s
        self.retries = retries
        self.retry_delay_s = retry_delay_s
        self.read_only = read_only
        self.on_frame = on_frame
        self.transactions = 0
        self._tid = 0

    def next_tid(self) -> int:
        self._tid = self._tid % 0xFFFF + 1
        return self._tid

    def _once(self, pdu: bytes, function: int, count: int):
        clock = self.s.clock
        dl = min(self.s.deadline, clock.monotonic() + self.txn_timeout_s)
        tid = self.next_tid()
        req = mbap(tid, self.unit, pdu)
        self.s.write(req)
        if self.on_frame:
            self.on_frame("tx", req)
        while True:
            head = self.s.read_until(size=MBAP_LEN, deadline=dl)
            rtid, proto, ln, unit = struct.unpack(">HHHB", head)
            if proto != 0 or not 2 <= ln <= 254:
                raise FrameError(f"MBAP 머리 이상: {head.hex(' ')}")
            body = self.s.read_until(size=ln - 1, deadline=dl)
            if self.on_frame:
                self.on_frame("rx", head + body)
            if rtid != tid:
                continue                    # 늦게 온 앞 응답 — 버리고 다시
            break
        if unit != self.unit:
            raise FrameError(f"유닛 {unit} ≠ {self.unit}")
        self.transactions += 1
        out = parse_pdu(body, function, count)
        if function in WRITE_FUNCS and out != struct.unpack(">HH", pdu[1:5]):
            raise FrameError(f"쓰기 응답이 요청과 다름: {out}")
        return out

    def transact(self, pdu: bytes, function: int, count: int = 0):
        if function in WRITE_FUNCS and self.read_only:
            raise ReadOnlyViolation(f"읽기 전용 — 함수 {function} 보내지 않음")
        last: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            if attempt:
                self.s.transport.reset_input()
                self.s.clock.sleep_until(self.s.clock.monotonic() + self.retry_delay_s)
            try:
                return self._once(pdu, function, count)
            except ModbusException:
                raise
            except (ReadTimeout, FrameError) as e:
                last = e
                if self.s.clock.monotonic() >= self.s.deadline:
                    break
        raise last

    def read_coils(self, addr: int, count: int) -> List[bool]:
        return self.transact(pdu_read_coils(addr, count), READ_COILS, count)

    def read_regs(self, addr: int, count: int) -> List[int]:
        return self.transact(pdu_read_regs(addr, count), READ_REGS, count)

    def write_coil(self, addr: int, on: bool) -> None:
        self.transact(pdu_write_coil(addr, on), WRITE_COIL)

    def write_reg(self, addr: int, value: int) -> None:
        self.transact(pdu_write_reg(addr, value), WRITE_REG)

    def write_coils(self, addr: int, values: Sequence[bool]) -> None:
        self.transact(pdu_write_coils(addr, values), WRITE_COILS)

    def write_regs(self, addr: int, values: Sequence[int]) -> None:
        self.transact(pdu_write_regs(addr, values), WRITE_REGS)


# ───────── 받는 쪽(시뮬레이터) ─────────
def split_request(frame: bytes):
    """요청 바이트 → (tid, unit, pdu, 남은 바이트) 또는 None(아직 덜 옴)."""
    if len(frame) < MBAP_LEN:
        return None
    tid, proto, ln, unit = struct.unpack(">HHHB", frame[:MBAP_LEN])
    end = 6 + ln
    if len(frame) < end:
        return None
    return tid, unit, frame[MBAP_LEN:end], frame[end:]

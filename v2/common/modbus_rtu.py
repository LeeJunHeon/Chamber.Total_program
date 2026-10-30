"""Modbus RTU (장비 공통) — 마스터 쪽 프레임 만들기·풀기와 한 번 주고받기. 의존성 없음.

지원 함수: 1(코일 읽기) 3(레지스터 읽기) 5(코일 1개 쓰기) 6(레지스터 1개 쓰기) 15(코일 여러 개) 16(레지스터 여러 개).
프레임 = 국번 1 + 함수 1 + 데이터 + CRC16(LSB 먼저). 레지스터 값은 빅엔디언 16비트.
응답 길이: 읽기 1 → 5 + ceil(n/8), 읽기 3 → 5 + 2n, 쓰기 5·6·15·16 → 8, 예외 → 5.

받는 쪽 처리: 앞 2바이트(국번·함수)를 먼저 읽고, 함수의 최상위 비트가 서 있으면 예외 응답(3바이트 더),
아니면 정해진 길이만큼 더 읽는다. 시간 안에 다 오지 않으면 ReadTimeout(잘린 프레임 포함),
CRC 가 틀리면 CrcError, 국번·함수가 다르면 FrameError, 예외 응답이면 ModbusException.
읽기 전용(read_only=True) 클라이언트는 쓰기 함수(5·6·15·16)를 보내지 않고 ReadOnlyViolation 을 낸다 — bench 보호.
"""
from __future__ import annotations

import struct
from typing import List, Optional, Sequence

from v2.common.transport import ReadTimeout

READ_COILS, READ_REGS, WRITE_COIL, WRITE_REG, WRITE_COILS, WRITE_REGS = 1, 3, 5, 6, 15, 16
WRITE_FUNCS = frozenset({WRITE_COIL, WRITE_REG, WRITE_COILS, WRITE_REGS})

EXCEPTION_TEXT = {1: "허용되지 않는 함수", 2: "허용되지 않는 주소", 3: "허용되지 않는 값", 4: "장치 오류",
                  5: "확인(처리 중)", 6: "장치 바쁨", 8: "메모리 패리티 오류", 10: "게이트웨이 경로 없음",
                  11: "게이트웨이 대상 무응답"}


class ModbusError(Exception):
    pass


class CrcError(ModbusError):
    pass


class FrameError(ModbusError):
    pass


class ModbusException(ModbusError):
    def __init__(self, function: int, code: int) -> None:
        super().__init__(f"Modbus 예외 함수 {function} 코드 {code}({EXCEPTION_TEXT.get(code, '?')})")
        self.function, self.code = function, code


class ReadOnlyViolation(ModbusError):
    pass


def crc16(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc


def with_crc(body: bytes) -> bytes:
    return body + struct.pack("<H", crc16(body))


def crc_ok(frame: bytes) -> bool:
    return len(frame) >= 4 and crc16(frame) == 0


# ───────── 요청 ─────────
def req_read_coils(slave: int, addr: int, count: int) -> bytes:
    if not 1 <= count <= 2000:
        raise ValueError("코일 수 1~2000")
    return with_crc(struct.pack(">BBHH", slave, READ_COILS, addr, count))


def req_read_regs(slave: int, addr: int, count: int) -> bytes:
    if not 1 <= count <= 125:
        raise ValueError("레지스터 수 1~125")
    return with_crc(struct.pack(">BBHH", slave, READ_REGS, addr, count))


def req_write_coil(slave: int, addr: int, on: bool) -> bytes:
    return with_crc(struct.pack(">BBHH", slave, WRITE_COIL, addr, 0xFF00 if on else 0x0000))


def req_write_reg(slave: int, addr: int, value: int) -> bytes:
    return with_crc(struct.pack(">BBHH", slave, WRITE_REG, addr, value & 0xFFFF))


def req_write_coils(slave: int, addr: int, values: Sequence[bool]) -> bytes:
    n = len(values)
    if not 1 <= n <= 1968:
        raise ValueError("코일 수 1~1968")
    data = bytearray((n + 7) // 8)
    for i, v in enumerate(values):
        if v:
            data[i // 8] |= 1 << (i % 8)
    return with_crc(struct.pack(">BBHHB", slave, WRITE_COILS, addr, n, len(data)) + bytes(data))


def req_write_regs(slave: int, addr: int, values: Sequence[int]) -> bytes:
    n = len(values)
    if not 1 <= n <= 123:
        raise ValueError("레지스터 수 1~123")
    return with_crc(struct.pack(">BBHHB", slave, WRITE_REGS, addr, n, 2 * n)
                    + b"".join(struct.pack(">H", v & 0xFFFF) for v in values))


def response_length(function: int, count: int = 0) -> int:
    if function == READ_COILS:
        return 5 + (count + 7) // 8
    if function == READ_REGS:
        return 5 + 2 * count
    if function in WRITE_FUNCS:
        return 8
    raise ValueError(f"지원하지 않는 함수 {function}")


# ───────── 응답 ─────────
def parse_response(frame: bytes, slave: int, function: int, count: int = 0):
    """검사 후 값: 읽기 1 → [bool]*count, 읽기 3 → [int]*count, 쓰기 → (주소, 값/개수)."""
    if not crc_ok(frame):
        raise CrcError(f"CRC 오류: {frame.hex(' ')}")
    if frame[0] != slave:
        raise FrameError(f"국번 {frame[0]} ≠ {slave}")
    if frame[1] == (function | 0x80):
        raise ModbusException(function, frame[2])
    if frame[1] != function:
        raise FrameError(f"함수 {frame[1]} ≠ {function}")
    if len(frame) != response_length(function, count):
        raise FrameError(f"길이 {len(frame)} ≠ {response_length(function, count)}")
    if function == READ_COILS:
        nb = frame[2]
        if nb != (count + 7) // 8:
            raise FrameError("바이트 수 불일치")
        bits = []
        for i in range(count):
            bits.append(bool(frame[3 + i // 8] >> (i % 8) & 1))
        return bits
    if function == READ_REGS:
        if frame[2] != 2 * count:
            raise FrameError("바이트 수 불일치")
        return list(struct.unpack(">" + "H" * count, frame[3:3 + 2 * count]))
    return struct.unpack(">HH", frame[2:6])


def to_signed(v: int) -> int:
    return v - 0x10000 if v >= 0x8000 else v


class ModbusClient:
    """Session(worker) 위에서 한 번 주고받기. 실패하면 retries 번 다시(retry_delay_s 쉬고)."""

    def __init__(self, session, slave: int, txn_timeout_s: float = 0.15, retries: int = 1,
                 retry_delay_s: float = 0.03, read_only: bool = False, on_frame=None) -> None:
        self.s = session
        self.slave = slave
        self.txn_timeout_s = txn_timeout_s
        self.retries = retries
        self.retry_delay_s = retry_delay_s
        self.read_only = read_only
        self.on_frame = on_frame          # (방향 "tx"/"rx", 바이트) — 녹화·기록용
        self.transactions = 0

    def _once(self, req: bytes, function: int, count: int):
        clock = self.s.clock
        dl = min(self.s.deadline, clock.monotonic() + self.txn_timeout_s)
        self.s.write(req)
        if self.on_frame:
            self.on_frame("tx", req)
        head = self.s.read_until(size=2, deadline=dl)
        if head[1] & 0x80:
            rest = self.s.read_until(size=3, deadline=dl)
        else:
            rest = self.s.read_until(size=response_length(function, count) - 2, deadline=dl)
        frame = head + rest
        if self.on_frame:
            self.on_frame("rx", frame)
        self.transactions += 1
        out = parse_response(frame, self.slave, function, count)
        if function in WRITE_FUNCS and out != struct.unpack(">HH", req[2:6]):
            raise FrameError(f"쓰기 응답이 요청과 다름: {out} ≠ {struct.unpack('>HH', req[2:6])}")
        return out

    def transact(self, req: bytes, function: int, count: int = 0):
        if function in WRITE_FUNCS and self.read_only:
            raise ReadOnlyViolation(f"읽기 전용 — 함수 {function} 보내지 않음")
        last: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            if attempt:
                self.s.transport.reset_input()
                self.s.clock.sleep_until(self.s.clock.monotonic() + self.retry_delay_s)
            try:
                return self._once(req, function, count)
            except ModbusException:
                raise                      # 장비가 거부 — 다시 해도 같다
            except (ReadTimeout, CrcError, FrameError) as e:
                last = e
                if self.s.clock.monotonic() >= self.s.deadline:
                    break
        raise last

    # 편의 함수
    def read_coils(self, addr: int, count: int) -> List[bool]:
        return self.transact(req_read_coils(self.slave, addr, count), READ_COILS, count)

    def read_regs(self, addr: int, count: int) -> List[int]:
        return self.transact(req_read_regs(self.slave, addr, count), READ_REGS, count)

    def write_coil(self, addr: int, on: bool) -> None:
        self.transact(req_write_coil(self.slave, addr, on), WRITE_COIL)

    def write_reg(self, addr: int, value: int) -> None:
        self.transact(req_write_reg(self.slave, addr, value), WRITE_REG)

    def write_coils(self, addr: int, values: Sequence[bool]) -> None:
        self.transact(req_write_coils(self.slave, addr, values), WRITE_COILS)

    def write_regs(self, addr: int, values: Sequence[int]) -> None:
        self.transact(req_write_regs(self.slave, addr, values), WRITE_REGS)

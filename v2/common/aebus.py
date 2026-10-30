"""AE Bus (장비 공통 — Advanced Energy CESAR 등) 프레임과 한 번 주고받기.

프레임 = Header(bit3~7 주소, bit0~2 데이터 길이 0~6, 7 이면 길이 바이트 추가) + Command + [Length] + Data(LSB 먼저) + XOR 체크섬
  [CESAR p.79-81 / 4-45~47]. 장비는 패킷을 받으면 ACK 0x06(체크섬 오류면 NAK 0x15)을 보내고, 이어서
  설정 명령(1~127)은 CSR 1바이트 데이터 프레임, 읽기 명령(128~255)은 데이터 프레임을 보낸다 [p.82-83 / 4-48~49].
주소 0 은 브로드캐스트(응답 없음) — 쓰지 않는다.
"""
from __future__ import annotations

from typing import Optional, Tuple

from v2.common.transport import ReadTimeout

ACK, NAK = 0x06, 0x15


class AeBusError(Exception):
    pass


class AeNak(AeBusError):
    pass


class AeChecksumError(AeBusError):
    pass


class AeFrameError(AeBusError):
    pass


def xor(data: bytes) -> int:
    c = 0
    for b in data:
        c ^= b
    return c


def build(addr: int, cmd: int, data: bytes = b"") -> bytes:
    if not 1 <= addr <= 31:
        raise ValueError("AE Bus 주소 1~31(0 은 브로드캐스트)")
    n = len(data)
    if n <= 6:
        body = bytes([((addr & 0x1F) << 3) | n, cmd & 0xFF]) + data
    else:
        body = bytes([((addr & 0x1F) << 3) | 7, cmd & 0xFF, n & 0xFF]) + data
    return body + bytes([xor(body)])


def parse(frame: bytes) -> Tuple[int, int, bytes]:
    """(주소, 명령, 데이터). 체크섬·길이 검사."""
    if len(frame) < 3:
        raise AeFrameError("프레임이 짧음")
    if xor(frame) != 0:
        raise AeChecksumError(f"체크섬 오류: {frame.hex(' ')}")
    hdr = frame[0]
    n = hdr & 0x07
    if n == 7:
        n = frame[2]
        data = frame[3:3 + n]
        need = 4 + n
    else:
        data = frame[2:2 + n]
        need = 3 + n
    if len(frame) != need:
        raise AeFrameError(f"길이 {len(frame)} ≠ {need}")
    return hdr >> 3, frame[1], bytes(data)


def u16(b: bytes) -> int:
    return b[0] | (b[1] << 8)


def u24(b: bytes) -> int:
    return b[0] | (b[1] << 8) | (b[2] << 16)


def le(v: int, n: int) -> bytes:
    return bytes((v >> (8 * i)) & 0xFF for i in range(n))


def transact(session, addr: int, cmd: int, data: bytes = b"", timeout_s: float = 0.25,
             on_frame=None) -> bytes:
    """보내고 ACK → 데이터 프레임까지. 돌려주는 값 = 데이터(설정 명령이면 CSR 1바이트).
    NAK → AeNak, 시간 초과 → ReadTimeout, 체크섬·형식 → AeChecksumError/AeFrameError."""
    clock = session.clock
    dl = min(session.deadline, clock.monotonic() + timeout_s)
    pkt = build(addr, cmd, data)
    session.transport.reset_input()
    session.write(pkt)
    if on_frame:
        on_frame("tx", pkt)
    a = session.read_until(size=1, deadline=dl)
    if a[0] == NAK:
        raise AeNak(f"NAK(명령 {cmd})")
    if a[0] != ACK:
        raise AeFrameError(f"ACK 대신 0x{a[0]:02X}")
    head = session.read_until(size=2, deadline=dl)
    n = head[0] & 0x07
    if n == 7:
        ln = session.read_until(size=1, deadline=dl)
        rest = session.read_until(size=ln[0] + 1, deadline=dl)
        frame = head + ln + rest
    else:
        frame = head + session.read_until(size=n + 1, deadline=dl)
    if on_frame:
        on_frame("rx", frame)
    a_addr, a_cmd, payload = parse(frame)
    if a_addr != addr or a_cmd != cmd:
        raise AeFrameError(f"응답 주소·명령 {a_addr}/{a_cmd} ≠ {addr}/{cmd}")
    return payload

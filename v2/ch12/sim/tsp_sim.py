"""TSP 시뮬레이터 — RS-232 Window 프로토콜(v1 device/tsp.py 의 프레임). 값·지연은 가정값이다(실제 장비 사실 아님).

받는 것: 02 {ADDR} {WIN 3} {COM} {DATA} 03 {CRC 2}. 주소가 다르거나 CRC 가 틀리면 응답 없음(가정 — 미확인).
- 011 쓰기 '1' → ACK, 상태 2(WAIT_INTERLOCK)… 가 아니라 바로 3 RAMP → RAMP_S 뒤 5 SUBLIM. '0' → ACK, OFF_S 뒤 0 STOP.
- 205 읽기 → 데이터 프레임 02 {ADDR} '205' '0' {6자리 상태} 03 CRC (데이터 형식은 미확인 — v1 해석이 받는 모양).
- 모르는 창 → 02 {ADDR} 0x32 03 CRC(Unknown Window).
응답은 기본으로 코드 프레임(02 80 06 03 CRC). short_codes=True 면 1바이트(06).
장비 고유 이상: interlock(켜도 2 WAIT_INTERLOCK 에 머묾 — ON 확인 시간초과)·fail(상태 1)·nack(쓰기 NACK)·
  script(받은 프레임마다 정해진 응답 — 골든 비교용; None 이면 무응답, "close" 면 연결 끊김).
통신 이상은 SimDevice 고장(no_response·delay·disconnect·garbage).
"""
from __future__ import annotations

from typing import List, Optional, Sequence, Union

from v2.common.simdev import SimDevice
from v2.common.transport import SimTransport

STX, ETX, ACK, NACK, ERR_UNKNOWN_WIN = 0x02, 0x03, 0x06, 0x15, 0x32
RAMP_S = 2.0          # 켠 뒤 RAMP → SUBLIM(가정)
OFF_S = 0.1           # 끈 뒤 STOP 까지(가정)
LATENCY_S = 0.03


def xor(data: bytes) -> int:
    x = 0
    for b in data:
        x ^= b
    return x & 0xFF


def code_frame(code: int, addr: int = 0x80) -> bytes:
    body = bytes([addr, code, ETX])
    return bytes([STX]) + body + f"{xor(body):02X}".encode()


def data_frame(win3: str, data: str, addr: int = 0x80, com: Optional[str] = "0") -> bytes:
    body = bytes([addr]) + win3.encode() + (com.encode() if com else b"") + data.encode() + bytes([ETX])
    return bytes([STX]) + body + f"{xor(body):02X}".encode()


def status_frame(st: int, addr: int = 0x80) -> bytes:
    return data_frame("205", f"{st:06d}", addr)


Script = Sequence[Union[bytes, None, str]]


class ConnSimTransport(SimTransport):
    """연결을 열 때 장치에 알린다 — 대본의 'close'(장비가 연결을 끊음)가 다음 연결까지만 이어지게."""

    def open(self) -> None:
        hook = getattr(self.device, "on_open", None)
        if hook is not None:
            hook(self.clock.monotonic())
        super().open()


class TspSim(SimDevice):
    def __init__(self, addr: int = 0x80, short_codes: bool = False, script: Optional[Script] = None) -> None:
        super().__init__("tsp", latency_s=LATENCY_S)
        self.addr = addr
        self.short_codes = short_codes
        self.on = False
        self.status = 0
        self._pending: Optional[tuple] = None      # (시각, 상태)
        self.interlock = False
        self.fail = False
        self.nack = False
        self.script: Optional[List] = list(script) if script is not None else None
        self.frames: List[bytes] = []              # 받은 프레임(시험용)
        self._killed = False                       # 장비가 이번 연결을 끊음(다음 열기까지)
        self.opens = 0

    def make_transport(self, clock) -> SimTransport:
        return ConnSimTransport(clock, self)

    def on_open(self, now: float) -> None:
        self._killed = False
        self.opens += 1

    # ───────── 상태 ─────────
    def _advance(self, now: float) -> None:
        if self._pending and now >= self._pending[0]:
            self.status = self._pending[1]
            self._pending = None
            if self.on and self.status == 3:
                self._pending = (now + RAMP_S, 5)

    def is_connected(self, now: float) -> bool:
        return not self._killed and super().is_connected(now)

    # ───────── 통신 ─────────
    def respond(self, data: bytes, now: float):
        self.frames.append(bytes(data))
        if self.script is not None:
            self.rx_log.append((now, data))
            if not self.script:
                return []
            r = self.script.pop(0)
            if r is None:
                return []
            if r == "close":
                self._killed = True                  # 연결을 끊는다(다음 열기는 바로 된다)
                return []
            return [(now + self.latency_s, r)]
        return super().respond(data, now)

    def _code(self, c: int) -> bytes:
        return bytes([c]) if self.short_codes else code_frame(c, self.addr)

    def handle(self, data: bytes, now: float) -> Optional[bytes]:
        self._advance(now)
        if len(data) < 8 or data[0] != STX or data[-3] != ETX:
            return None
        if f"{xor(data[1:-2]):02X}".encode() != data[-2:] or data[1] != self.addr:
            return None
        win, com, payload = data[2:5].decode(), data[5:6], data[6:-3].decode(errors="replace")
        if win == "011" and com == b"1":
            if self.nack:
                return self._code(NACK)
            if payload == "1":
                self.on = True
                st = 1 if self.fail else (2 if self.interlock else 3)
                self._pending = (now + 0.05, st)
            elif payload == "0":
                self.on = False
                self._pending = (now + OFF_S, 0)
            return self._code(ACK)
        if win == "205" and com == b"0":
            return status_frame(self.status, self.addr)
        return self._code(ERR_UNKNOWN_WIN)

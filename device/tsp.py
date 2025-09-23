# device/tsp.py
# RFC2217/로컬 직렬 모두 지원: port에 "COM5" 또는 "rfc2217://ip:port"를 그대로 넣는다.

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import time
import asyncio

try:
    import serial
    from serial import serial_for_url
except ImportError as e:
    raise RuntimeError("pyserial 미설치. `pip install pyserial` 필요") from e


def _xor7(*chunks: bytes) -> int:
    x = 0
    for c in chunks:
        for b in c:
            x ^= b
    return x & 0x7F


@dataclass
class TSPLetterClient:
    port: str                     # "COM5" 또는 "rfc2217://192.168.1.50:4004"
    baudrate: Optional[int] = 9600
    addr: int = 0x01              # 0x80 | addr => 0x81 = 주소 1
    timeout: Optional[float] = 1.0

    _ser: Optional[serial.SerialBase] = None

    def open(self) -> None:
        if self._ser and getattr(self._ser, "is_open", False):
            return
        # URL/COM 문자열을 그대로 사용. 여기서 ':9600' 등은 붙이지 않는다.
        baud = int(self.baudrate or 9600)
        to = float(self.timeout or 1.0)
        self._ser = serial_for_url(self.port, baudrate=baud, timeout=to)

    def close(self) -> None:
        if self._ser:
            try:
                self._ser.close()
            finally:
                self._ser = None

    @property
    def is_open(self) -> bool:
        return bool(self._ser and getattr(self._ser, "is_open", False))

    def _build_frame(self, cmd_ascii: str) -> bytes:
        # 프레임: [0x80|addr] + ASCII_LEN(2) + ASCII_CMD + CHECKSUM(1)
        if not cmd_ascii or not all(32 <= ord(ch) <= 126 for ch in cmd_ascii):
            raise ValueError(f"ASCII 명령 형식 오류: {cmd_ascii!r}")
        header = bytes([0x80 | (self.addr & 0x0F)])
        payload = cmd_ascii.encode("ascii")
        len_ascii = f"{len(payload):02d}".encode("ascii")
        cs = _xor7(header, len_ascii, payload)
        return header + len_ascii + payload + bytes([cs])

    def send_ascii(self, cmd_ascii: str) -> bytes:
        if not self.is_open:
            raise RuntimeError("포트가 열려있지 않습니다. open() 호출 필요")
        frame = self._build_frame(cmd_ascii)
        assert self._ser is not None
        self._ser.write(frame)
        self._ser.flush()
        time.sleep(0.01)  # 장비가 응답을 안 줄 수 있으므로 대기만
        return frame

    # 편의 명령
    def cmd_start(self) -> None:  self.send_ascii("G1")
    def cmd_stop(self)  -> None:  self.send_ascii("G0")
    def cmd_status(self)-> None:  self.send_ascii("S?")
    def cmd_error(self) -> None:  self.send_ascii("E?")


class AsyncTSP:
    """동기 드라이버를 asyncio에서 편하게 쓰기 위한 래퍼"""
    def __init__(self, client: TSPLetterClient):
        self.client = client

    @property
    def is_connected(self) -> bool:
        return self.client.is_open

    async def ensure_open(self) -> None:
        if not self.client.is_open:
            await asyncio.to_thread(self.client.open)

    async def start(self) -> None:
        await self.ensure_open()
        await asyncio.to_thread(self.client.cmd_start)

    async def stop(self) -> None:
        if not self.client.is_open:
            return
        await asyncio.to_thread(self.client.cmd_stop)

    async def ensure_off(self) -> None:
        try:
            await self.stop()
        except Exception:
            pass

    async def aclose(self) -> None:
        await asyncio.to_thread(self.client.close)

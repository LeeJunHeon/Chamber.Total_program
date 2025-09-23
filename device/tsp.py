# tsp.py
# RFC2217로 TSP에 접속하고 Letter 프레임(0x8A~)으로 명령 전송
# 예: G1/G0/S?/E? ... (체크섬 = header ^ len_ascii ^ payload 의 XOR 전체 & 0x7F)

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
class TSPLetterClientRFC2217:
    host: str
    port: int
    baudrate: int = 9600
    addr: int = 0x01  # 0x80 | addr => 0x81 = 주소 1
    timeout: float = 1.0

    _ser: Optional[serial.SerialBase] = None

    def open(self) -> None:
        if self._ser and self._ser.is_open:
            return
        url = f"rfc2217://{self.host}:{self.port}"
        self._ser = serial_for_url(url, baudrate=self.baudrate, timeout=self.timeout)
        # 장비가 \r/\n을 요구하지 않는 Letter 프레임이므로 추가 설정 불필요.

    def close(self) -> None:
        if self._ser:
            try:
                self._ser.close()
            finally:
                self._ser = None

    @property
    def is_open(self) -> bool:
        return bool(self._ser and self._ser.is_open)

    def _build_frame(self, cmd_ascii: str) -> bytes:
        # 프레임: [0x80|addr] + ASCII_LEN(2) + ASCII_CMD + CHECKSUM(1)
        # checksum = XOR(all previous bytes) & 0x7F  => 예시 G1 -> 0x75('u')와 일치
        if not cmd_ascii or not all(32 <= ord(ch) <= 126 for ch in cmd_ascii):
            raise ValueError(f"ASCII 명령 형식 오류: {cmd_ascii!r}")
        header = bytes([0x80 | (self.addr & 0x0F)])
        payload = cmd_ascii.encode("ascii")
        len_ascii = f"{len(payload):02d}".encode("ascii")
        cs = _xor7(header, len_ascii, payload)
        frame = header + len_ascii + payload + bytes([cs])
        return frame

    def send_ascii(self, cmd_ascii: str) -> bytes:
        """동기 전송. 필요 시 반환 프레임 기록용으로 사용."""
        if not self.is_open:
            raise RuntimeError("포트가 열려있지 않습니다. open() 호출 필요")
        frame = self._build_frame(cmd_ascii)
        assert self._ser is not None
        self._ser.write(frame)
        self._ser.flush()
        # 장비 응답을 안 줄 수 있으므로 여기선 읽지 않음. 필요 시 self._ser.read() 사용.
        time.sleep(0.01)
        return frame

    # 편의 함수
    def cmd_start(self) -> None:
        self.send_ascii("G1")

    def cmd_stop(self) -> None:
        self.send_ascii("G0")

    def cmd_status(self) -> None:
        self.send_ascii("S?")

    def cmd_error(self) -> None:
        self.send_ascii("E?")


class AsyncTSP:
    """동기 드라이버를 asyncio에서 편하게 쓰기 위한 래퍼"""
    def __init__(self, client: TSPLetterClientRFC2217):
        self.client = client

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

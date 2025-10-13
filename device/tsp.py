# -*- coding: utf-8 -*-
"""
tsp.py — asyncio 기반 TSP 컨트롤러 (TCP Server 직결, Window 프로토콜)
- 프레임: <STX><ADDR><WIN(3 ASCII)><COM('0'=read/'1'=write)><DATA?><ETX><CRC(ASCII 2)>
- RS-232: ADDR=0x80 / RS-485: ADDR=0x80+(노드 0~31)
- 기존 사용처와 상위 시그니처 유지(AsyncTSP.ensure_open/start/stop/ensure_off/aclose)
- 기본 정책: 장비가 응답을 보내지 않아도(write-only) 예외 없이 통과
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Literal
import asyncio, socket, contextlib

# 프로젝트 설정(기존 그대로 사용)
from lib.config_ch2 import (
    TSP_TCP_HOST, TSP_TCP_PORT,
    TSP_ADDR,                      # 주의: 이제 RS-485 노드로 해석(0~31). RS-232면 -1 또는 0xFF 권장.
    TSP_CONNECT_TIMEOUT_S, TSP_WRITE_TIMEOUT_S, TSP_POST_SEND_DELAY_MS
)

# ──────────────────────────────────────────────────────────────
STX = 0x02
ETX = 0x03

def _xor_bytes(data: bytes) -> int:
    x = 0
    for b in data:
        x ^= b
    return x & 0xFF  # Window CRC는 1바이트 전체

def _addr_byte(addr_like: int) -> int:
    """
    RS-232이면 0x80, RS-485이면 0x80 + (노드 0~31).
    config의 TSP_ADDR을 '노드 번호'로 해석하고,
    RS-232를 쓰는 경우에는 TSP_ADDR=-1(또는 255)로 설정하는 것을 권장.
    """
    if addr_like is None:
        return 0x80  # 안전 디폴트: RS-232
    try:
        n = int(addr_like)
    except Exception:
        n = -1
    if n < 0 or n > 31:
        return 0x80
    return 0x80 + (n & 0x1F)

def _build_window_frame(win3: str, write: bool, data_ascii: Optional[str], rs485_addr_like: int) -> bytes:
    if len(win3) != 3 or not win3.isdigit():
        raise ValueError(f"WIN은 3자리 숫자여야 합니다: {win3!r}")
    addr = _addr_byte(rs485_addr_like)
    com  = 0x31 if write else 0x30  # '1' or '0'
    body = bytes([addr]) + win3.encode('ascii') + bytes([com])
    if write and data_ascii is None:
        raise ValueError("write에는 DATA가 필요합니다.")
    if data_ascii:
        body += data_ascii.encode('ascii')
    body += bytes([ETX])
    crc = f"{_xor_bytes(body):02X}".encode('ascii')  # ASCII 2글자
    return bytes([STX]) + body + crc

# ──────────────────────────────────────────────────────────────
@dataclass
class TSPLetterTCPClient:
    """
    (이름은 호환을 위해 유지) — 실제 구현은 'Window' 프로토콜.
    """
    host: str = TSP_TCP_HOST
    port: int = TSP_TCP_PORT
    addr: int = TSP_ADDR              # RS-485 노드(0~31) / RS-232면 -1 권장
    connect_timeout_s: float = float(TSP_CONNECT_TIMEOUT_S)
    write_timeout_s: float = float(TSP_WRITE_TIMEOUT_S)
    post_send_delay_ms: int = int(TSP_POST_SEND_DELAY_MS)

    _reader: Optional[asyncio.StreamReader] = None
    _writer: Optional[asyncio.StreamWriter] = None

    # ── 연결/종료 ───────────────────────────────────────────
    async def open(self) -> None:
        if self.is_open:
            return
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=max(0.1, self.connect_timeout_s)
        )
        # TCP keepalive (가능 시)
        with contextlib.suppress(Exception):
            sock = self._writer.get_extra_info("socket")
            if isinstance(sock, socket.socket):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    async def close(self) -> None:
        if self._writer is not None:
            with contextlib.suppress(Exception):
                self._writer.close()
                await asyncio.wait_for(self._writer.wait_closed(), timeout=max(0.1, self.write_timeout_s))
            self._reader = None
            self._writer = None

    @property
    def is_open(self) -> bool:
        return bool(self._writer) and not self._writer.is_closing()  # type: ignore[union-attr]

    # ── 저수준 송신 ─────────────────────────────────────────
    async def _send_frame(self, frame: bytes) -> None:
        if not self.is_open:
            raise RuntimeError("TSP TCP가 열려있지 않습니다. open() 필요")
        assert self._writer is not None
        self._writer.write(frame)
        # drain에 타임아웃 적용
        await asyncio.wait_for(self._writer.drain(), timeout=max(0.1, self.write_timeout_s))
        # 장비 반영 여유
        await asyncio.sleep(max(0, self.post_send_delay_ms) / 1000.0)

    # ── Window 유틸 ─────────────────────────────────────────
    async def write_logic(self, win3: str, on: bool) -> None:
        """Logic 타입 창구에 '0'/'1' 쓰기 (ACK 없어도 예외 없이 통과)."""
        frame = _build_window_frame(win3, write=True, data_ascii=("1" if on else "0"), rs485_addr_like=self.addr)
        await self._send_frame(frame)

    async def write_raw(self, win3: str, data_ascii: str) -> None:
        """임의 창구에 ASCII 데이터 쓰기."""
        frame = _build_window_frame(win3, write=True, data_ascii=data_ascii, rs485_addr_like=self.addr)
        await self._send_frame(frame)

    async def read_raw(self, win3: str, read_timeout_s: float = 0.8) -> Optional[str]:
        """
        읽기 프레임을 보내고 응답을 기다려 '데이터 부분'만 ASCII로 반환.
        - 실패/타임아웃 시 None
        - 현재 디바이스 코어에서는 사용처가 적어 선택적 제공
        """
        if not self.is_open:
            raise RuntimeError("TSP TCP가 열려있지 않습니다. open() 필요")
        frame = _build_window_frame(win3, write=False, data_ascii=None, rs485_addr_like=self.addr)
        await self._send_frame(frame)
        if self._reader is None:
            return None

        # 첫 바이트 판정 (ACK/NACK/ERR or STX)
        try:
            b0 = await asyncio.wait_for(self._reader.readexactly(1), timeout=max(0.1, read_timeout_s))
        except Exception:
            return None

        # 단문 코드(ACK/NACK/ERR)인 경우: 데이터 없음 → None
        if b0 in (b"\x06", b"\x15", b"\x32", b"\x33", b"\x34", b"\x35"):
            return None

        # 데이터 프레임(STX)라면 ETX+CRC까지 수신
        if b0 != bytes([STX]):
            return None

        buf = bytearray(b0)
        try:
            # ETX까지
            while True:
                b = await asyncio.wait_for(self._reader.readexactly(1), timeout=max(0.1, read_timeout_s))
                buf += b
                if b == bytes([ETX]):
                    break
            # CRC 2글자
            crc = await asyncio.wait_for(self._reader.readexactly(2), timeout=max(0.1, read_timeout_s))
            raw = bytes(buf) + crc
        except Exception:
            return None

        # CRC 검증(STX 제외 ~ ETX 포함)
        try:
            crc_ok = (f"{_xor_bytes(raw[1:-2]):02X}".encode("ascii") == raw[-2:])
        except Exception:
            crc_ok = False
        if not crc_ok:
            return None

        body = raw[1:-3]  # [ADDR][WIN(3)][DATA...]  (ETX 제외)
        if len(body) < 1 + 3:
            return None
        # 데이터만 추출
        data_ascii = body[1+3:].decode("ascii", errors="ignore")
        return data_ascii

    # ── 고수준 명령 ────────────────────────────────────────
    async def cmd_start(self)  -> None:  await self.write_logic("011", True)
    async def cmd_stop(self)   -> None:  await self.write_logic("011", False)

    # 선택 사용: 상태/통신방식 읽기 등
    async def cmd_read_status_numeric(self) -> Optional[str]:
        return await self.read_raw("205")

    async def cmd_read_serial_type(self) -> Optional[str]:
        return await self.read_raw("504")

# ──────────────────────────────────────────────────────────────
class AsyncTSP:
    """
    상위 사용처 호환:
      - await tsp.ensure_open()
      - await tsp.start() / await tsp.stop()
      - await tsp.ensure_off()
      - await tsp.aclose()
    """
    def __init__(self, client: Optional[TSPLetterTCPClient] = None):
        self.client = client or TSPLetterTCPClient()

    @property
    def is_connected(self) -> bool:
        return self.client.is_open

    async def ensure_open(self) -> None:
        if not self.client.is_open:
            await self.client.open()

    async def start(self) -> None:
        await self.ensure_open()
        await self.client.cmd_start()   # Window: WIN 011 = '1'

    async def stop(self) -> None:
        if not self.client.is_open:
            return
        await self.client.cmd_stop()    # Window: WIN 011 = '0'

    async def ensure_off(self) -> None:
        # write-only 가정: best-effort
        with contextlib.suppress(Exception):
            await self.stop()

    async def aclose(self) -> None:
        await self.client.close()

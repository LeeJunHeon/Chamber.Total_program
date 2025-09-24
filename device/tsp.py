# -*- coding: utf-8 -*-
"""
tsp.py — asyncio 기반 TSP 컨트롤러 (TCP Server 직결)
- pyserial 불필요, RFC2217 미지원(순수 TCP)
- 프레임: [0x80|addr] + ASCII_LEN(2) + ASCII_CMD + CHECKSUM(1)
- 장비가 응답을 보내지 않아도 되는(write-only) 시나리오 가정
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import asyncio, socket

from lib.config_ch2 import (
    TSP_TCP_HOST, TSP_TCP_PORT,
    TSP_ADDR, TSP_CONNECT_TIMEOUT_S, TSP_WRITE_TIMEOUT_S, TSP_POST_SEND_DELAY_MS
)

def _xor7(*chunks: bytes) -> int:
    x = 0
    for c in chunks:
        for b in c:
            x ^= b
    return x & 0x7F


@dataclass
class TSPLetterTCPClient:
    host: str = TSP_TCP_HOST
    port: int = TSP_TCP_PORT
    addr: int = TSP_ADDR
    connect_timeout_s: float = float(TSP_CONNECT_TIMEOUT_S)
    write_timeout_s: float = float(TSP_WRITE_TIMEOUT_S)
    post_send_delay_ms: int = int(TSP_POST_SEND_DELAY_MS)

    _reader: Optional[asyncio.StreamReader] = None
    _writer: Optional[asyncio.StreamWriter] = None

    async def open(self) -> None:
        if self.is_open:
            return
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=max(0.1, self.connect_timeout_s)
        )
        # TCP keepalive(가능하면)
        try:
            sock = self._writer.get_extra_info("socket")
            if isinstance(sock, socket.socket):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except Exception:
            pass

    async def close(self) -> None:
        if self._writer is not None:
            try:
                self._writer.close()
                await asyncio.wait_for(self._writer.wait_closed(), timeout=max(0.1, self.write_timeout_s))
            except Exception:
                pass
            finally:
                self._reader = None
                self._writer = None

    @property
    def is_open(self) -> bool:
        return bool(self._writer) and not self._writer.is_closing()  # type: ignore[union-attr]

    def _build_frame(self, cmd_ascii: str) -> bytes:
        if not cmd_ascii or not all(32 <= ord(ch) <= 126 for ch in cmd_ascii):
            raise ValueError(f"ASCII 명령 형식 오류: {cmd_ascii!r}")
        header = bytes([0x80 | (self.addr & 0x0F)])
        payload = cmd_ascii.encode("ascii")
        len_ascii = f"{len(payload):02d}".encode("ascii")
        cs = _xor7(header, len_ascii, payload)
        return header + len_ascii + payload + bytes([cs])

    async def send_ascii(self, cmd_ascii: str) -> bytes:
        if not self.is_open:
            raise RuntimeError("TSP TCP가 열려있지 않습니다. open() 필요")
        frame = self._build_frame(cmd_ascii)
        assert self._writer is not None
        self._writer.write(frame)
        # drain에 타임아웃 적용
        await asyncio.wait_for(self._writer.drain(), timeout=max(0.1, self.write_timeout_s))
        # 장비가 응답을 안 주는 모델 → 아주 짧게만 대기(스펙 호환)
        await asyncio.sleep(max(0, self.post_send_delay_ms) / 1000.0)
        return frame

    # 편의 명령 (모두 비동기)
    async def cmd_start(self)  -> None:  await self.send_ascii("G1")
    async def cmd_stop(self)   -> None:  await self.send_ascii("G0")
    async def cmd_status(self) -> None:  await self.send_ascii("S?")
    async def cmd_error(self)  -> None:  await self.send_ascii("E?")


class AsyncTSP:
    """
    기존 사용처와 시그니처 유지:
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
        await self.client.cmd_start()

    async def stop(self) -> None:
        if not self.client.is_open:
            return
        await self.client.cmd_stop()

    async def ensure_off(self) -> None:
        # 응답 필요 없음: best-effort
        try:
            await self.stop()
        except Exception:
            pass

    async def aclose(self) -> None:
        await self.client.close()

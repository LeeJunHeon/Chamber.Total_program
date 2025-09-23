# -*- coding: utf-8 -*-
"""
tsp_controller.py — TSP 컨트롤러 드라이버 (RFC2217 + Letter 프로토콜)

- RFC2217 (원격 RS-232) URL 예: rfc2217://192.168.1.50:4001
- 프레임: <ADR><LDAT><DATA><CRC>
  * ADR = 0x80 + addr(1..32)
  * LDAT = DATA 길이를 나타내는 ASCII 2자리 '00'..'99'
  * DATA 예: 'G1','G0','S?','E?','T00150' ...
  * CRC  = (ADR+LDAT+DATA) 바이트 XOR 후 MSB=0 ( &0x7F )
- 쓰기 명령 응답: ACK(0x06) 1바이트
- 읽기 명령 응답: <adr(msb=0)><ldat><data><crc>
"""
from __future__ import annotations
import time, asyncio
from dataclasses import dataclass
from typing import Optional
import serial

# ─────────────────────────────────────────────────────────────
# 공통 유틸
# ─────────────────────────────────────────────────────────────

def _crc_letter(body: bytes) -> int:
    x = 0
    for b in body:
        x ^= b
    return x & 0x7F

def build_letter_frame(addr: int, data_ascii: str) -> bytes:
    if not (1 <= addr <= 32):
        raise ValueError("addr must be 1..32")
    adr = bytes([0x80 + addr])
    ldat = f"{len(data_ascii):02d}".encode("ascii")
    data = data_ascii.encode("ascii")
    body = adr + ldat + data
    crc = bytes([_crc_letter(body)])
    return body + crc

# ─────────────────────────────────────────────────────────────
# 동기 클라이언트
# ─────────────────────────────────────────────────────────────

@dataclass
class TSPLetterClientRFC2217:
    host: str
    port: int
    baudrate: int = 9600
    addr: int = 1
    timeout: float = 1.0
    write_timeout: float = 1.0
    rtscts: bool = False
    dsrdtr: bool = False
    dtr: Optional[bool] = None
    rts: Optional[bool] = None

    def __post_init__(self):
        self._ser: Optional[serial.Serial] = None

    def open(self):
        url = f"rfc2217://{self.host}:{self.port}"
        self._ser = serial.serial_for_url(
            url,
            baudrate=self.baudrate,
            timeout=self.timeout,
            write_timeout=self.write_timeout,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            rtscts=self.rtscts,
            dsrdtr=self.dsrdtr,
        )
        if self.dtr is not None:
            self._ser.dtr = self.dtr
        if self.rts is not None:
            self._ser.rts = self.rts

    def close(self):
        if self._ser:
            try: self._ser.close()
            finally: self._ser = None

    def _write(self, frame: bytes):
        assert self._ser is not None, "serial not open"
        self._ser.reset_input_buffer()
        self._ser.write(frame)
        self._ser.flush()

    def _read_exact(self, n: int) -> bytes:
        assert self._ser is not None, "serial not open"
        buf = bytearray()
        deadline = time.monotonic() + (self.timeout or 1.0)
        while len(buf) < n:
            chunk = self._ser.read(n - len(buf))
            if chunk:
                buf += chunk
            elif time.monotonic() > deadline:
                break
        return bytes(buf)

    def _txrx_write(self, data_ascii: str) -> bool:
        frame = build_letter_frame(self.addr, data_ascii)
        self._write(frame)
        b = self._read_exact(1)
        return len(b) == 1 and b[0] == 0x06  # ACK

    def _txrx_read(self, data_ascii: str) -> Optional[str]:
        frame = build_letter_frame(self.addr, data_ascii)
        self._write(frame)
        b0 = self._read_exact(1)
        if len(b0) == 0: return None
        if b0[0] == 0x06:
            b0 = self._read_exact(1)
            if len(b0) == 0: return None
        adr = b0[0]
        ldat = self._read_exact(2)
        if len(ldat) != 2: return None
        try:
            n = int(ldat.decode("ascii"))
        except Exception:
            return None
        rest = self._read_exact(n + 1)
        if len(rest) != n + 1: return None
        data, crc = rest[:-1], rest[-1]
        expect = _crc_letter(bytes([adr]) + ldat + data)
        if crc != expect: return None
        try:
            return data.decode("ascii")
        except Exception:
            return None

    # High-level
    def start(self) -> bool:  # G1
        return self._txrx_write("G1")

    def stop(self) -> bool:   # G0
        return self._txrx_write("G0")

# ─────────────────────────────────────────────────────────────
# asyncio 어댑터
# ─────────────────────────────────────────────────────────────

class AsyncTSP:
    def __init__(self, client: TSPLetterClientRFC2217):
        self.c = client
    async def start(self) -> bool:
        return await asyncio.to_thread(self.c.start)
    async def stop(self) -> bool:
        return await asyncio.to_thread(self.c.stop)

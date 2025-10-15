# -*- coding: utf-8 -*-
"""
device/tsp.py — RS-232 Window 프로토콜 기반 TSP 제어 (ON/OFF + 상태확인)
프레임 형식:
  <STX=0x02><ADDR=0x80><WIN(3 ASCII) or CODE><COM('0':read/'1':write) or none><DATA?><ETX=0x03><CRC(ASCII 2)>
- READ: STX..ADDR..WIN..'0'..DATA..ETX..CRC (데이터 타입에 따라 DATA 길이 상이)
- WRITE: 성공 시 ACK(0x06)를 담은 프레임 <STX><ADDR><0x06><ETX><CRC> 로 응답 (오류 시 0x15/0x32~0x35)

공개 API (async):
  - AsyncTSP(host, port, ...).on(wait_ok=1.5)   # 011 ← '1', (옵션) 205로 상태확인
  - AsyncTSP(host, port, ...).off(wait_ok=1.5)  # 011 ← '0', (옵션) 205로 상태확인
  - get_status() -> Optional[int]               # 205 읽기 (0:STOP, 3/4/5: 구동 상태)
  - ensure_open(), aclose(), is_connected
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple
import asyncio, socket, contextlib

# ── 프로토콜 상수 ─────────────────────────────────────────────
STX = 0x02
ETX = 0x03
ADDR_RS232 = 0x80         # RS-232 고정 주소

WIN_ONOFF  = "011"        # Start/Stop: write '1'/'0'
WIN_STATUS = "205"        # Status: read numeric

# 단문/프레임 모두에서 쓰이는 코드
ACK = 0x06
NACK = 0x15
ERR_UNKNOWN_WIN = 0x32
ERR_DATA_TYPE   = 0x33
ERR_OUT_OF_RANGE= 0x34
ERR_WIN_DISABLED= 0x35
SIMPLE_RESP_CODES = {ACK, NACK, ERR_UNKNOWN_WIN, ERR_DATA_TYPE, ERR_OUT_OF_RANGE, ERR_WIN_DISABLED}

# 205 상태값
STATUS_STOP = 0
STATUS_FAIL = 1
STATUS_WAIT_INTERLOCK = 2
STATUS_RAMP = 3
STATUS_WAIT_SUBLIM   = 4
STATUS_SUBLIM        = 5

# ── 예외 ─────────────────────────────────────────────────────
class TSPError(Exception): ...
class TSPNackError(TSPError): ...
class TSPProtocolError(TSPError): ...
class TSPWindowError(TSPError):
    def __init__(self, code: int):
        msg = {
            ERR_UNKNOWN_WIN: "Unknown Window",
            ERR_DATA_TYPE:   "Data Type Error",
            ERR_OUT_OF_RANGE:"Out of Range",
            ERR_WIN_DISABLED:"Window Disabled",
        }.get(code, f"Error code 0x{code:02X}")
        super().__init__(msg)
        self.code = code

# ── 유틸 ─────────────────────────────────────────────────────
def _xor_bytes(data: bytes) -> int:
    x = 0
    for b in data:
        x ^= b
    return x & 0xFF  # 1바이트 XOR

def _build_frame(win3: str, write: bool, data_ascii: Optional[str]) -> bytes:
    """
    MESSAGE: <STX><ADDR><WIN(3)><COM('0' read/'1' write)><DATA?><ETX><CRC(ASCII 2)>
    - CRC는 (STX 제외 ~ ETX 포함) XOR 결과를 대문자 16진 ASCII 2글자로 부호화
    """
    if len(win3) != 3 or not win3.isdigit():
        raise ValueError(f"WIN은 3자리 숫자여야 합니다: {win3!r}")
    com  = 0x31 if write else 0x30  # '1'/'0'
    body = bytes([ADDR_RS232]) + win3.encode("ascii") + bytes([com])
    if write and data_ascii is None:
        raise ValueError("write에는 DATA가 필요합니다.")
    if data_ascii:
        body += data_ascii.encode("ascii")
    body += bytes([ETX])
    crc = f"{_xor_bytes(body):02X}".encode("ascii")
    return bytes([STX]) + body + crc

def _decode_ascii_numeric(s: str) -> int:
    return int(s.strip() or "0")

# ── 본체 ─────────────────────────────────────────────────────
@dataclass
class AsyncTSP:
    host: str
    port: int
    connect_timeout: float = 2.0     # 연결 타임아웃
    io_timeout: float = 1.2          # 수신/드레인 타임아웃
    post_send_delay: float = 0.02    # 전송 후 장비 반영 여유(sec)
    verify_with_status: bool = True  # ON/OFF 후 205 상태 재확인
    tolerate_short_resp: bool = True # 드물게 1바이트만 오는 구현 허용(관용)

    _reader: Optional[asyncio.StreamReader] = None
    _writer: Optional[asyncio.StreamWriter] = None

    # ── 연결/종료 ───────────────────────────────────────────
    @property
    def is_connected(self) -> bool:
        return bool(self._writer) and not self._writer.is_closing()  # type: ignore[union-attr]

    async def ensure_open(self) -> None:
        if self.is_connected:
            return
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=max(0.1, self.connect_timeout)
        )
        # TCP keepalive
        with contextlib.suppress(Exception):
            sock = self._writer.get_extra_info("socket")
            if isinstance(sock, socket.socket):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    async def aclose(self) -> None:
        if self._writer is not None:
            with contextlib.suppress(Exception):
                self._writer.close()
                await asyncio.wait_for(self._writer.wait_closed(), timeout=max(0.1, self.io_timeout))
        self._reader = None
        self._writer = None

    # ── 송수신 ──────────────────────────────────────────────
    async def _send(self, b: bytes) -> None:
        if not self.is_connected:
            raise TSPError("연결되지 않음")
        assert self._writer is not None
        self._writer.write(b)
        await asyncio.wait_for(self._writer.drain(), timeout=max(0.1, self.io_timeout))
        if self.post_send_delay > 0:
            await asyncio.sleep(self.post_send_delay)

    async def _recv_any(self) -> bytes:
        """
        응답 수신:
          - 표준: STX…ETX + CRC 프레임
          - 관용: 1바이트 코드(ACK/NACK/ERR)도 허용(tolerate_short_resp=True)
        """
        assert self._reader is not None
        b0 = await asyncio.wait_for(self._reader.readexactly(1), timeout=max(0.1, self.io_timeout))
        if not b0:
            raise TSPProtocolError("빈 응답")

        # 1) (관용) 단문 코드 허용
        if self.tolerate_short_resp and b0[0] in SIMPLE_RESP_CODES:
            return b0

        # 2) 프레임(STX)이어야 정상
        if b0[0] != STX:
            raise TSPProtocolError(f"STX 아님: 0x{b0[0]:02X}")

        buf = bytearray(b0)
        # ETX까지 수신
        while True:
            b = await asyncio.wait_for(self._reader.readexactly(1), timeout=max(0.1, self.io_timeout))
            buf += b
            if b[0] == ETX:
                break
        # CRC(ASCII 2)
        crc = await asyncio.wait_for(self._reader.readexactly(2), timeout=max(0.1, self.io_timeout))
        return bytes(buf) + crc

    def _parse_response(self, raw: bytes) -> Tuple[str, Optional[str]]:
        """
        Returns: (kind, data)
          - kind: "ACK" / "NACK" / "ERR" / "DATA" / "RAW"
          - data: "ERR" → 코드(hex str), "DATA" → 데이터 문자열
        """
        # (관용) 단문 1바이트
        if len(raw) == 1 and raw[0] in SIMPLE_RESP_CODES:
            code = raw[0]
            if code == ACK:  return ("ACK", None)
            if code == NACK: return ("NACK", None)
            return ("ERR", f"{code:02x}")

        # 프레임 검사
        if len(raw) < 1+1+1+2 or raw[0] != STX or raw[-3] != ETX:
            return ("RAW", raw.hex())

        # CRC 확인(STX 제외 ~ ETX 포함)
        crc_ok = (f"{_xor_bytes(raw[1:-2]):02X}".encode('ascii') == raw[-2:])
        if not crc_ok:
            raise TSPProtocolError("CRC 불일치")

        body = raw[1:-3]  # ADDR..(ETX 제외)
        # (A) 코드만 담긴 프레임: <ADDR><CODE>
        if len(body) == 2 and body[1] in SIMPLE_RESP_CODES:
            code = body[1]
            if code == ACK:  return ("ACK", None)
            if code == NACK: return ("NACK", None)
            return ("ERR", f"{code:02x}")

        # AFTER (정상) ─ 메뉴얼: 응답은 <ADDR><WIN(3)><DATA...> 이므로 4바이트만 건너뜀
        # (B) 데이터 프레임: <ADDR><WIN(3)><DATA...>
        if len(body) >= 4:
            data_ascii = body[4:].decode('ascii', errors='ignore')
            return ("DATA", data_ascii)

        return ("RAW", raw.hex())

    # ── 윈도우 원시 API ─────────────────────────────────────
    async def read_win(self, win3: str) -> Optional[str]:
        await self.ensure_open()
        await self._send(_build_frame(win3, write=False, data_ascii=None))
        raw = await self._recv_any()
        kind, data = self._parse_response(raw)
        if kind == "DATA":
            return data
        if kind == "ACK":
            # 읽기인데 ACK만 오면 장비 구현 상 이례적 → None
            return None
        if kind == "NACK":
            raise TSPNackError("NACK")
        if kind == "ERR":
            raise TSPWindowError(int(data, 16) if data else -1)
        return None

    async def write_win_logic(self, win3: str, on: bool) -> None:
        await self.ensure_open()
        await self._send(_build_frame(win3, write=True, data_ascii=("1" if on else "0")))
        raw = await self._recv_any()
        kind, data = self._parse_response(raw)
        if kind == "ACK":
            return
        if kind == "NACK":
            raise TSPNackError("NACK")
        if kind == "ERR":
            raise TSPWindowError(int(data, 16) if data else -1)
        raise TSPProtocolError(f"예상치 못한 응답: {kind} {data}")

    # ── 상태 읽기(205) ──────────────────────────────────────
    async def get_status(self) -> Optional[int]:
        resp = await self.read_win(WIN_STATUS)
        if resp is None:
            return None
        try:
            return _decode_ascii_numeric(resp)
        except Exception:
            return None

    # ── 공개 API: ON/OFF ────────────────────────────────────
    async def on(self, *, wait_ok: float = 1.5) -> None:
        """
        TSP ON — WIN 011 ← '1'
        - ACK/NACK/ERR 확인
        - verify_with_status=True면 205(Status) 폴링으로 성공 여부 점검
        """
        await self.write_win_logic(WIN_ONOFF, True)
        if self.verify_with_status:
            await self._wait_status(target="on", timeout=wait_ok)

    async def off(self, *, wait_ok: float = 1.5) -> None:
        """
        TSP OFF — WIN 011 ← '0'
        - ACK/NACK/ERR 확인
        - verify_with_status=True면 205(Status) 폴링으로 정지 확인
        """
        await self.write_win_logic(WIN_ONOFF, False)
        if self.verify_with_status:
            await self._wait_status(target="off", timeout=wait_ok)

    # ── 내부: 상태 폴링 ─────────────────────────────────────
    async def _wait_status(self, *, target: str, timeout: float) -> None:
        """
        target='on'  → {RAMP(3), WAIT_SUBLIM(4), SUBLIM(5)} 중 하나면 OK
        target='off' → {STOP(0)} 이면 OK
        """
        end_t = asyncio.get_event_loop().time() + max(0.0, timeout)
        desired_on = {STATUS_RAMP, STATUS_WAIT_SUBLIM, STATUS_SUBLIM}
        desired_off = {STATUS_STOP}
        want = desired_on if target == "on" else desired_off
        last = None
        while True:
            st = await self.get_status()
            last = st
            if st in want:
                return
            if asyncio.get_event_loop().time() >= end_t:
                raise TSPError(f"상태 확인 시간초과(target={target}, last={last})")
            await asyncio.sleep(0.05)

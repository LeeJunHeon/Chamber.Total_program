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

from dataclasses import dataclass, field
from typing import Optional, Tuple, Any
import asyncio, socket, contextlib

from lib import config_common as cfgc


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

def _build_frame(win3: str, write: bool, data_ascii: Optional[str], *, addr: int = ADDR_RS232) -> bytes:
    """
    MESSAGE: <STX><ADDR><WIN(3)><COM('0' read/'1' write)><DATA?><ETX><CRC(ASCII 2)>
    - CRC는 (STX 제외 ~ ETX 포함) XOR 결과를 대문자 16진 ASCII 2글자로 부호화
    """
    if len(win3) != 3 or not win3.isdigit():
        raise ValueError(f"WIN은 3자리 숫자여야 합니다: {win3!r}")
    com  = 0x31 if write else 0x30  # '1'/'0'
    body = bytes([addr & 0xFF]) + win3.encode("ascii") + bytes([com])
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

    # (기존 필드 유지) - 생성자에서 직접 넣어도 되고, config로 덮어쓸 수도 있게
    connect_timeout: float = 2.0
    io_timeout: float = 1.2
    post_send_delay: float = 0.02
    verify_with_status: bool = True
    tolerate_short_resp: bool = True

    # ✅ 추가: 채널 cfg 주입(없으면 config_common 사용)
    cfg: Any | None = None

    _reader: Optional[asyncio.StreamReader] = None
    _writer: Optional[asyncio.StreamWriter] = None

    # 내부 캐시
    _cfg_mod: Any = field(init=False, repr=False)
    _io_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    _tcp_keepalive: bool = field(default=False, init=False, repr=False)
    _addr_rs232: int = field(default=ADDR_RS232, init=False, repr=False)
    _status_poll_interval_s: float = field(default=0.05, init=False, repr=False)

    def __post_init__(self) -> None:
        self._cfg_mod = self.cfg if self.cfg is not None else cfgc
        # 초기 로딩(=UI 적용 대비). 단, 키가 없으면 현재 값 유지되게 구성
        self.reload_runtime_cfg()

    def reload_runtime_cfg(self) -> None:
        """
        UI에서 config 값을 바꾼 뒤 호출하면 런타임에 반영되도록.
        cfg(채널) → 없으면 config_common 순으로 읽는다.
        """
        mod = self._cfg_mod

        # endpoint: host/port는 "비어있을 때만" config로 채움(기존 생성 방식 깨지지 않게)
        if not self.host:
            self.host = str(getattr(mod, "TSP_TCP_HOST", getattr(cfgc, "TSP_TCP_HOST", self.host)))
        if not self.port:
            self.port = int(getattr(mod, "TSP_TCP_PORT", getattr(cfgc, "TSP_TCP_PORT", self.port or 0)))

        # 타임아웃/딜레이
        self.connect_timeout = float(getattr(mod, "TSP_CONNECT_TIMEOUT_S",
                                             getattr(cfgc, "TSP_CONNECT_TIMEOUT_S", self.connect_timeout)))

        # config는 WRITE_TIMEOUT_S로 되어 있음 → io_timeout에 매핑
        self.io_timeout = float(getattr(mod, "TSP_WRITE_TIMEOUT_S",
                                        getattr(cfgc, "TSP_WRITE_TIMEOUT_S", self.io_timeout)))

        post_ms = float(getattr(mod, "TSP_POST_SEND_DELAY_MS",
                                getattr(cfgc, "TSP_POST_SEND_DELAY_MS", self.post_send_delay * 1000.0)))
        self.post_send_delay = max(0.0, post_ms / 1000.0)

        # verify / tolerate
        self.verify_with_status = bool(getattr(mod, "TSP_VERIFY_WITH_STATUS",
                                               getattr(cfgc, "TSP_VERIFY_WITH_STATUS", self.verify_with_status)))
        self.tolerate_short_resp = bool(getattr(mod, "TSP_TOLERATE_SHORT_RESP",
                                                getattr(cfgc, "TSP_TOLERATE_SHORT_RESP", self.tolerate_short_resp)))

        # keepalive는 config_common 공통키(TSP_TCP_KEEPALIVE) 사용
        self._tcp_keepalive = bool(getattr(mod, "TSP_TCP_KEEPALIVE",
                                           getattr(cfgc, "TSP_TCP_KEEPALIVE", self._tcp_keepalive)))

        # 주소/폴링 주기
        addr = getattr(mod, "TSP_RS232_ADDR", None)
        if addr is None:
            addr = getattr(mod, "TSP_ADDR", None)  # ✅ 레거시 키 호환
        if addr is None:
            addr = getattr(cfgc, "TSP_RS232_ADDR", None)
        if addr is None:
            addr = getattr(cfgc, "TSP_ADDR", self._addr_rs232)

        self._addr_rs232 = int(addr) & 0xFF

        self._status_poll_interval_s = float(getattr(mod, "TSP_STATUS_POLL_INTERVAL_S",
                                                     getattr(cfgc, "TSP_STATUS_POLL_INTERVAL_S", self._status_poll_interval_s)))

        # 방어
        if self.connect_timeout < 0.1:
            self.connect_timeout = 0.1
        if self.io_timeout < 0.1:
            self.io_timeout = 0.1
        if self._status_poll_interval_s < 0.01:
            self._status_poll_interval_s = 0.01

    # ── 연결/종료 ───────────────────────────────────────────
    @property
    def is_connected(self) -> bool:
        return bool(self._writer) and not self._writer.is_closing()  # type: ignore[union-attr]

    async def ensure_open(self) -> None:
        if self.is_connected:
            return

        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=max(0.1, self.connect_timeout),
        )

        # TCP keepalive (config 반영)
        with contextlib.suppress(Exception):
            sock = self._writer.get_extra_info("socket")
            if isinstance(sock, socket.socket):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1 if self._tcp_keepalive else 0)

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

        # (B) 데이터 프레임:
        #   - 구현 A: <ADDR><WIN(3)><DATA...>
        #   - 구현 B: <ADDR><WIN(3)><COM('0'/'1')><DATA...>
        if len(body) >= 4:
            data_start = 4
            if len(body) >= 5 and body[4] in (0x30, 0x31):  # '0' or '1'
                data_start = 5
            data_ascii = body[data_start:].decode("ascii", errors="ignore")
            return ("DATA", data_ascii)

        return ("RAW", raw.hex())

    # ── 윈도우 원시 API ─────────────────────────────────────
    async def read_win(self, win3: str) -> Optional[str]:
        async with self._io_lock:
            try:
                await self.ensure_open()
                await self._send(_build_frame(win3, write=False, data_ascii=None, addr=self._addr_rs232))
                raw = await self._recv_any()
                kind, data = self._parse_response(raw)
                if kind == "DATA":
                    return data
                if kind == "ACK":
                    return None
                if kind == "NACK":
                    raise TSPNackError("NACK")
                if kind == "ERR":
                    raise TSPWindowError(int(data, 16) if data else -1)
                return None
            except Exception:
                # 통신 예외 후에는 세션을 확실히 끊어 다음 호출에서 재연결되게
                with contextlib.suppress(Exception):
                    await self.aclose()
                raise

    async def write_win_logic(self, win3: str, on: bool) -> None:
        async with self._io_lock:
            try:
                await self.ensure_open()
                await self._send(_build_frame(win3, write=True, data_ascii=("1" if on else "0"), addr=self._addr_rs232))
                raw = await self._recv_any()
                kind, data = self._parse_response(raw)
                if kind == "ACK":
                    return
                if kind == "NACK":
                    raise TSPNackError("NACK")
                if kind == "ERR":
                    raise TSPWindowError(int(data, 16) if data else -1)
                raise TSPProtocolError(f"예상치 못한 응답: {kind} {data}")
            except Exception:
                with contextlib.suppress(Exception):
                    await self.aclose()
                raise

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
        loop = asyncio.get_running_loop()
        end_t = loop.time() + max(0.0, timeout)

        desired_on = {STATUS_RAMP, STATUS_WAIT_SUBLIM, STATUS_SUBLIM}
        desired_off = {STATUS_STOP}
        want = desired_on if target == "on" else desired_off
        last = None
        while True:
            st = await self.get_status()
            last = st
            if st in want:
                return
            if loop.time() >= end_t:
                raise TSPError(f"상태 확인 시간초과(target={target}, last={last})")
            await asyncio.sleep(self._status_poll_interval_s)

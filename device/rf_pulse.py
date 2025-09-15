# device/RFPulse_async.py
# -*- coding: utf-8 -*-
"""
rf_pulse.py — asyncio 기반 CESAR AE RS-232 Pulse 컨트롤러

의존성:
    pip install pyserial-asyncio

핵심(구 PyQt6 버전과 동등):
  - serial_asyncio + asyncio.Protocol 기반 완전 비동기 시리얼 I/O
  - 단일 명령 큐(타임아웃/재시도/인터커맨드 gap)로 송수신 직렬화
  - 워치독(지수 백오프) 자동 재연결
  - ACK(0x06) / NAK(0x15) / AE Bus 프레임 파서
  - exec(쓰기/CSR 확인) / query(읽기/데이터 프레임) 분리 처리
  - 폴링: REPORT_STATUS → FWD → REF 순서, 중첩 금지
  - RF Pulse 시퀀스: HOST→MODE(FWD)→SETP→(FREQ/DUTY)→PULSING=1→RF ON
  - 안전정지: HOST(유지)→PULSING=0→RF OFF (no-reply 허용)
  - 기존 상수/타임아웃/백오프/파싱/검증 로직 유지
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Deque, Callable, AsyncGenerator, Literal, Tuple
from collections import deque
import asyncio
import time
import re

try:
    import serial_asyncio
except Exception as e:
    raise RuntimeError("pyserial-asyncio가 필요합니다. `pip install pyserial-asyncio`") from e

from lib.config_ch2 import RFPULSE_PORT, RFPULSE_BAUD, RFPULSE_ADDR

# ===== 타이밍/타임아웃 상수 =====
ACK_TIMEOUT_MS         = 2000   # 쓰기(설정) 명령 후 ACK/프레임 대기 시간
QUERY_TIMEOUT_MS       = 4500   # 읽기(리드백) 명령 후 전체 대기 시간
RECV_FRAME_TIMEOUT_MS  = 4000   # 저수준 프레임 대기 타임아웃
CMD_GAP_MS             = 1500   # 명령 간 최소 간격(밀리초)
POST_WRITE_DELAY_MS    = 1500   # 각 쓰기 명령 후 여유 대기(밀리초)

# ★ ACK 뒤 CSR/데이터 프레임을 잠깐 더 기다리는 그레이스 윈도우
ACK_FOLLOWUP_GRACE_MS  = 500

# 폴링
POLL_INTERVAL_MS       = 30_000
POLL_QUERY_TIMEOUT_MS  = 9000
POLL_START_DELAY_AFTER_RF_ON_MS = 800

# 워치독/재연결(지수 백오프)
RFPULSE_WATCHDOG_INTERVAL_MS        = 2000
RFPULSE_RECONNECT_BACKOFF_START_MS  = 2000
RFPULSE_RECONNECT_BACKOFF_MAX_MS    = 15000

# ===== AE Bus command numbers =====
CMD_RF_OFF              = 1
CMD_RF_ON               = 2
CMD_SET_CTRL_MODE       = 3
CMD_SET_SETPOINT        = 8
CMD_SET_ACTIVE_CTRL     = 14

# Reads (report)
CMD_REPORT_STATUS       = 162
CMD_REPORT_SETPOINT     = 164
CMD_REPORT_FORWARD      = 165
CMD_REPORT_REFLECTED    = 166
CMD_REPORT_DELIVERED    = 167

# Pulsing
CMD_SET_PULSING         = 27     # 0=off, 1=int, 2=ext, 3=ext_inv, 4=int_by_ext
CMD_SET_PULSE_FREQ      = 93     # 3 bytes (Hz, LSB first)
CMD_SET_PULSE_DUTY      = 96     # 2 bytes (percent, LSB first)

# Pulsing 리드백(선택)
CMD_REPORT_PULSING      = 177
CMD_REPORT_PULSE_FREQ   = 193
CMD_REPORT_PULSE_DUTY   = 196

CSR_CODES = {
    0: "OK",
    1: "Command Not Recognized",
    2: "Not in Host Mode",
    3: "Not Implemented",
    4: "Bad Data Value",
    5: "Busy",
}

MODE_SET  = {"fwd": 6, "load": 7, "ext": 8}
MODE_NAME = {6: "FWD", 7: "LOAD", 8: "EXT"}

CMD_NAMES = {
    CMD_RF_OFF: "RF_OFF",
    CMD_RF_ON: "RF_ON",
    CMD_SET_CTRL_MODE: "SET_CTRL_MODE",
    CMD_SET_SETPOINT: "SET_SETPOINT",
    CMD_SET_ACTIVE_CTRL: "SET_ACTIVE_CTRL",
    CMD_REPORT_STATUS: "REPORT_STATUS",
    CMD_REPORT_SETPOINT: "REPORT_SETPOINT",
    CMD_REPORT_FORWARD: "REPORT_FORWARD",
    CMD_REPORT_REFLECTED: "REPORT_REFLECTED",
    CMD_REPORT_DELIVERED: "REPORT_DELIVERED",
    CMD_SET_PULSING: "SET_PULSING",
    CMD_SET_PULSE_FREQ: "SET_PULSE_FREQ",
    CMD_SET_PULSE_DUTY: "SET_PULSE_DUTY",
    CMD_REPORT_PULSING: "REPORT_PULSING",
    CMD_REPORT_PULSE_FREQ: "REPORT_PULSE_FREQ",
    CMD_REPORT_PULSE_DUTY: "REPORT_PULSE_DUTY",
}

# ---- REPORT_STATUS(0xA2) 파싱 ----
@dataclass
class RfStatus:
    rf_output_on: bool          # Byte1 bit5
    rf_on_requested: bool       # Byte1 bit6
    setpoint_mismatch: bool     # Byte1 bit7 (True면 아직 목표 미도달)
    interlock_open: bool        # Byte2 bit7
    overtemp: bool              # Byte2 bit3
    current_limit: bool         # Byte4 bit0
    extended_fault: bool        # Byte4 bit5
    cex_lock: bool              # Byte4 bit7
    raw: bytes = b""

def _u16le(buf: bytes, i: int = 0) -> int:
    return buf[i] | (buf[i+1] << 8)

# ===== 프레임 빌더 =====
def _build_packet(addr: int, cmd: int, data: bytes=b"") -> bytes:
    if not (0 <= addr <= 31):
        raise ValueError("addr 0..31")
    L = len(data)
    if L <= 6:
        header = ((addr & 0x1F) << 3) | L
        body = bytes([header, cmd]) + data
    else:
        header = ((addr & 0x1F) << 3) | 0x07
        body = bytes([header, cmd, L & 0xFF]) + data
    cs = 0
    for b in body:
        cs ^= b
    return body + bytes([cs & 0xFF])

# ===== 큐 명령 구조 =====
@dataclass
class RfCommand:
    kind: Literal["exec", "query"]                 # exec=쓰기(CSR 필요), query=읽기(데이터 프레임)
    cmd: int
    data: bytes
    timeout_ms: int
    gap_ms: int
    tag: str
    retries_left: int
    allow_no_reply: bool
    allow_when_closing: bool
    callback: Callable[[Optional[bytes]], None]    # 성공: bytes(빈바이트 허용), 실패: None

# ===== 이벤트 모델 =====
RFPEventKind = Literal[
    "status", "rf_status", "power",
    "command_confirmed", "command_failed",
    "target_reached", "power_off_finished"
]

@dataclass
class RFPulseEvent:
    kind: RFPEventKind
    message: Optional[str] = None
    cmd: Optional[str] = None
    reason: Optional[str] = None
    rfstatus: Optional[RfStatus] = None
    forward_w: Optional[float] = None
    reflected_w: Optional[float] = None

# ===== Protocol (바이트 토큰 스트리머) =====
Token = Tuple[Literal["ACK", "NAK", "FRAME"], Optional[bytes]]

class _RFPProtocol(asyncio.Protocol):
    def __init__(self, owner: "RFPulseAsync"):
        self.owner = owner
        self.transport: Optional[asyncio.Transport] = None
        self._rx = bytearray()

    def connection_made(self, transport: asyncio.BaseTransport):
        self.transport = transport  # type: ignore
        self.owner._on_connection_made(self.transport)

    def data_received(self, data: bytes):
        if not data:
            return
        self._rx.extend(data)

        # 토큰화 루프
        while True:
            if not self._rx:
                break

            b0 = self._rx[0]

            # ACK/NAK 단일 토큰
            if b0 == 0x06:
                del self._rx[:1]
                self.owner._on_token(("ACK", None))
                continue
            if b0 == 0x15:
                del self._rx[:1]
                self.owner._on_token(("NAK", None))
                continue

            # 프레임: header + cmd + [optlen] + data + cs
            if len(self._rx) < 2:
                break
            hdr = self._rx[0]
            cmd_b = self._rx[1]
            length_bits = hdr & 0x07

            if length_bits == 7:
                if len(self._rx) < 3:
                    break
                data_len = self._rx[2]
                total = 1 + 1 + 1 + data_len + 1
                if len(self._rx) < total:
                    break
                pkt = bytes(self._rx[:total])
                del self._rx[:total]
            else:
                data_len = length_bits
                total = 1 + 1 + data_len + 1
                if len(self._rx) < total:
                    break
                pkt = bytes(self._rx[:total])
                del self._rx[:total]

            # 체크섬
            cs = 0
            for x in pkt[:-1]:
                cs ^= x
            if (cs ^ pkt[-1]) != 0:
                # 불일치 → 버리고 계속
                continue

            self.owner._on_token(("FRAME", pkt))

    def connection_lost(self, exc: Optional[Exception]):
        self.owner._on_connection_lost(exc)

# ===== 메인 컨트롤러 =====
class RFPulseAsync:
    def __init__(self, *, debug_print: bool = True):
        self.debug_print = debug_print

        # 연결/프로토콜
        self._transport: Optional[asyncio.Transport] = None
        self._protocol: Optional[_RFPProtocol] = None
        self._connected: bool = False

        # 명령 큐/상태
        self._cmd_q: Deque[RfCommand] = deque()
        self._inflight: Optional[RfCommand] = None
        self._last_send_mono: float = 0.0           # 인터커맨드 최소 간격

        # 토큰/이벤트 큐
        self._tok_q: asyncio.Queue[Token] = asyncio.Queue(maxsize=2048)
        self._event_q: asyncio.Queue[RFPulseEvent] = asyncio.Queue(maxsize=512)

        # 태스크
        self._want_connected: bool = False
        self._watchdog_task: Optional[asyncio.Task] = None
        self._cmd_worker_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None

        # 재연결 백오프
        self._reconnect_backoff_ms = RFPULSE_RECONNECT_BACKOFF_START_MS

        # 런타임 상태
        self.addr = int(RFPULSE_ADDR) if RFPULSE_ADDR is not None else 0
        self._closing: bool = False
        self._stop_requested: bool = False
        self._need_reopen: bool = False

        # 폴링/전력 캐시
        self._poll_busy: bool = False
        self._last_forward_w: Optional[float] = None
        self._last_reflected_w: Optional[float] = None
        self._last_status: Optional[RfStatus] = None

    # ---------- 공용 API ----------
    async def start(self):
        """워치독 + 명령 워커 시작."""
        if self._watchdog_task or self._cmd_worker_task:
            return
        self._want_connected = True
        loop = asyncio.get_running_loop()
        self._watchdog_task = loop.create_task(self._watchdog_loop(), name="RFPWatchdog")
        self._cmd_worker_task = loop.create_task(self._cmd_worker_loop(), name="RFPCmdWorker")
        await self._emit_status("RFPulse 워치독/워커 시작")

    async def stop(self):
        """안전 종료: 폴링 off → 큐 purge → safe off → 연결 종료."""
        self._closing = True
        self._want_connected = False
        self.set_process_status(False)      # safe off 큐잉
        await asyncio.sleep(0.2)

        await self._cancel_task("_poll_task")
        await self._cancel_task("_cmd_worker_task")
        await self._cancel_task("_watchdog_task")

        self._purge_pending("shutdown")
        if self._transport:
            try:
                self._transport.close()
            except Exception:
                pass
        self._transport = None
        self._protocol = None
        self._connected = False
        await self._emit_status("RFPulse 연결 종료됨")

    async def events(self) -> AsyncGenerator[RFPulseEvent, None]:
        """상위(UI/브리지)에서 구독하는 이벤트 스트림."""
        while True:
            ev = await self._event_q.get()
            yield ev

    # ---------- 고수준 시퀀스 ----------
    async def start_pulse_process(self, target_w: float, freq_hz: Optional[int] = None, duty_percent: Optional[int] = None):
        """
        HOST(14,02) → MODE(FWD=6) → SETP → (FREQ/DUTY) → PULSING=1 → RF ON
        실패 시 'target_failed' 이벤트, 성공 시 RF ON 직후 폴링 시작.
        """
        self._stop_requested = False
        self.set_process_status(False)

        async def fail(why: str):
            await self._emit_failed("START_SEQUENCE", why)
            await self._event_q.put(RFPulseEvent(kind="target_reached", message="FAILED"))  # 호환을 위해 알림
            return False

        # HOST
        ok, _ = await self._exec_and_csr(CMD_SET_ACTIVE_CTRL, b"\x02", tag="[START HOST]")
        if not ok: return await fail("HOST 실패")

        # MODE FWD
        ok, _ = await self._exec_and_csr(CMD_SET_CTRL_MODE, bytes([MODE_SET["fwd"]]), tag="[START MODE FWD]")
        if not ok: return await fail("MODE=FWD 실패")

        # SETPOINT
        sp = int(round(float(target_w)))
        ok, _ = await self._exec_and_csr(CMD_SET_SETPOINT, bytes([sp & 0xFF, (sp >> 8) & 0xFF]),
                                         tag=f"[START SETP {sp}W]")
        if not ok: return await fail("SETP 실패")

        # FREQ
        if freq_hz is not None:
            hz = int(freq_hz)
            data_f = bytes([hz & 0xFF, (hz >> 8) & 0xFF, (hz >> 16) & 0xFF])
            ok, _ = await self._exec_and_csr(CMD_SET_PULSE_FREQ, data_f, tag="[START FREQ]")
            if not ok: return await fail("PULSE FREQ 실패")

        # DUTY
        if duty_percent is not None:
            v = int(duty_percent) & 0xFFFF
            data_d = bytes([v & 0xFF, (v >> 8) & 0xFF])
            ok, _ = await self._exec_and_csr(CMD_SET_PULSE_DUTY, data_d, tag="[START DUTY]")
            if not ok: return await fail("PULSE DUTY 실패")

        # PULSING=1
        ok, _ = await self._exec_and_csr(CMD_SET_PULSING, bytes([1]), tag="[START PULSING 1]")
        if not ok: return await fail("PULSING=1 실패")

        # RF ON
        ok, _ = await self._exec_and_csr(CMD_RF_ON, b"", tag="[START RF ON]", timeout_ms=max(ACK_TIMEOUT_MS, 2500))
        if not ok: return await fail("RF ON 실패")

        # 폴링 시작
        await asyncio.sleep(POLL_START_DELAY_AFTER_RF_ON_MS / 1000.0)
        self.set_process_status(True)
        # 구버전 호환: RF ON 완료 알림
        await self._event_q.put(RFPulseEvent(kind="target_reached", message="OK"))

    def set_process_status(self, should_poll: bool):
        """True→ 폴링 시작, False→ 폴링 중지 + safe off 큐잉."""
        if should_poll:
            if self._poll_task is None or self._poll_task.done():
                self._poll_task = asyncio.create_task(self._poll_loop())
            return

        # 폴링 중지 & 큐 정리
        if self._poll_task:
            self._poll_task.cancel()
            self._poll_task = None
        self._poll_busy = False
        self._purge_pending("polling off")

        # 안전 초기화 시퀀스 (응답 미보장 허용)
        self._enqueue_exec(CMD_SET_ACTIVE_CTRL, b"\x02", tag="[SAFE HOST]", allow_no_reply=True, allow_when_closing=True)
        self._enqueue_exec(CMD_SET_PULSING, bytes([0]), tag="[SAFE PULSING 0]", allow_no_reply=True, allow_when_closing=True)

        async def _notify_off():
            await self._event_q.put(RFPulseEvent(kind="power_off_finished"))
        # RF OFF (no-reply 허용)
        self._enqueue_exec(CMD_RF_OFF, b"", tag="[SAFE RF OFF]", allow_no_reply=True, allow_when_closing=True,
                           callback=lambda _b: asyncio.create_task(_notify_off()))

    def stop_process(self):
        """외부 stop: 폴링 off → safe off → power_off_finished 이벤트."""
        self._stop_requested = True
        self._want_connected = False
        self.set_process_status(False)

    async def poll_once(self):
        """원샷 WAKE→FWD→REF 읽기 및 이벤트 방출."""
        st = await self._read_status()
        if st:
            await self._emit_status(f"[ONCE] STATUS {self._status_summary_str(st)}")
        f = await self._query_and_data(CMD_REPORT_FORWARD, b"", tag="[ONCE FWD]")
        r = await self._query_and_data(CMD_REPORT_REFLECTED, b"", tag="[ONCE REF]")
        if f is not None:
            self._last_forward_w = float(_u16le(f, 0) if len(f) >= 2 else 0.0)
        if r is not None:
            self._last_reflected_w = float(_u16le(r, 0) if len(r) >= 2 else 0.0)
        if (self._last_forward_w is not None) and (self._last_reflected_w is not None):
            await self._event_q.put(RFPulseEvent(kind="power",
                                                 forward_w=self._last_forward_w,
                                                 reflected_w=self._last_reflected_w))

    # ---------- 내부: 연결/워치독 ----------
    async def _watchdog_loop(self):
        backoff = self._reconnect_backoff_ms
        while True:
            if not self._want_connected:
                await asyncio.sleep(0.05)
                continue

            if self._connected and not self._need_reopen:
                await asyncio.sleep(RFPULSE_WATCHDOG_INTERVAL_MS / 1000.0)
                continue

            await self._emit_status(f"재연결 시도... ({backoff} ms)")
            await asyncio.sleep(backoff / 1000.0)

            if not self._want_connected:
                continue

            # 기존 연결 강제 종료가 필요한 경우
            if self._need_reopen and self._transport:
                try:
                    self._transport.close()
                except Exception:
                    pass
                self._transport = None
                self._protocol = None
                self._connected = False
                self._need_reopen = False

            try:
                loop = asyncio.get_running_loop()
                transport, protocol = await serial_asyncio.create_serial_connection(
                    loop, lambda: _RFPProtocol(self), RFPULSE_PORT, baudrate=RFPULSE_BAUD,
                    parity='O', stopbits=1, bytesize=8, xonxoff=False, rtscts=False
                )
                self._transport = transport
                self._protocol = protocol  # type: ignore
                self._connected = True
                backoff = RFPULSE_RECONNECT_BACKOFF_START_MS
                await self._emit_status(f"{RFPULSE_PORT} 연결 성공 (asyncio)")
            except Exception as e:
                await self._emit_status(f"{RFPULSE_PORT} 연결 실패: {e}")
                backoff = min(backoff * 2, RFPULSE_RECONNECT_BACKOFF_MAX_MS)

    def _on_connection_made(self, transport: asyncio.Transport):
        pass

    def _on_connection_lost(self, exc: Optional[Exception]):
        self._connected = False
        if self._transport:
            try:
                self._transport.close()
            except Exception:
                pass
        self._transport = None
        self._protocol = None
        self._need_reopen = True
        self._dbg("RFP", f"연결 끊김: {exc}")

        # 인플라이트 복구/취소
        if self._inflight is not None:
            cmd = self._inflight
            self._inflight = None
            if cmd.retries_left > 0:
                cmd.retries_left -= 1
                self._cmd_q.appendleft(cmd)
            else:
                self._safe_callback(cmd.callback, None)

    def _on_token(self, tok: Token):
        # 큐가 꽉 차면 가장 오래된 토큰을 버리고 새 토큰을 삽입
        try:
            self._tok_q.put_nowait(tok)
        except asyncio.QueueFull:
            try:
                self._tok_q.get_nowait()
            except Exception:
                pass
            try:
                self._tok_q.put_nowait(tok)
            except Exception:
                pass

    # ---------- 내부: 명령 워커 ----------
    async def _cmd_worker_loop(self):
        while True:
            await asyncio.sleep(0)  # cancel-friendly

            if not self._cmd_q:
                await asyncio.sleep(0.01)
                continue
            if not (self._connected and self._transport):
                await asyncio.sleep(0.05)
                continue

            cmd = self._cmd_q.popleft()
            self._inflight = cmd

            # 최소 인터커맨드 간격 보장
            now = time.monotonic()
            gap_need = (CMD_GAP_MS / 1000.0) - (now - self._last_send_mono)
            if gap_need > 0:
                await asyncio.sleep(gap_need)

            # 전송
            try:
                pkt = _build_packet(self.addr, cmd.cmd, cmd.data)
                self._transport.write(pkt)
                await self._transport.drain() if hasattr(self._transport, "drain") else None
                self._last_send_mono = time.monotonic()
                self._dbg("RFP TX", f"{cmd.tag or ('exec' if cmd.kind=='exec' else 'query')} "
                                    f"{self._cmd_label(cmd.cmd)} len={len(cmd.data)}")
            except Exception as e:
                self._dbg("RFP", f"전송 오류: {e}")
                self._inflight = None
                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    self._cmd_q.appendleft(cmd)
                else:
                    self._safe_callback(cmd.callback, None)
                self._need_reopen = True
                continue

            # no-reply
            if cmd.allow_no_reply and cmd.kind == "exec":
                self._safe_callback(cmd.callback, b"")
                self._inflight = None
                await asyncio.sleep(cmd.gap_ms / 1000.0)
                continue

            # 응답 대기
            ok = False
            result: Optional[bytes] = None
            fail_reason: Optional[str] = None

            try:
                if cmd.kind == "exec":
                    ok, result = await self._await_exec_csr(cmd)
                else:
                    ok, result = await self._await_query_data(cmd)
            except asyncio.TimeoutError:
                ok = False
                fail_reason = "timeout"
            except Exception as e:
                ok = False
                fail_reason = f"error:{e}"
                self._need_reopen = True

            # 결과 처리
            if ok:
                self._dbg("RFP OK", f"{cmd.tag} {self._cmd_label(cmd.cmd)}")
                self._safe_callback(cmd.callback, result)
                self._inflight = None
                await asyncio.sleep(cmd.gap_ms / 1000.0)
            else:
                self._dbg("RFP FAIL", f"{cmd.tag} {self._cmd_label(cmd.cmd)}"
                                      + (f" ({fail_reason})" if fail_reason else ""))
                if cmd.retries_left > 0 and not self._closing:
                    cmd.retries_left -= 1
                    self._cmd_q.appendleft(cmd)
                    # Busy(5)면 조금 더 여유
                    backoff_ms = max(150, cmd.gap_ms)
                    if isinstance(fail_reason, str) and fail_reason.startswith("csr=5"):
                        backoff_ms = max(int(cmd.gap_ms * 1.5), 1200)
                    await asyncio.sleep(backoff_ms / 1000.0)
                else:
                    self._safe_callback(cmd.callback, None)
                    self._inflight = None
                    await asyncio.sleep(cmd.gap_ms / 1000.0)

    # ---------- 내부: exec/query 대기 ----------
    async def _await_exec_csr(self, cmd: RfCommand) -> Tuple[bool, Optional[bytes]]:
        """ACK phase → CSR 프레임(동일 cmd, 동일 addr) 확보 → CSR=0 확인."""
        start = time.monotonic()
        ack_deadline = start + min(ACK_TIMEOUT_MS, cmd.timeout_ms) / 1000.0
        end_deadline = start + cmd.timeout_ms / 1000.0

        csr_bytes: Optional[bytes] = None

        # ACK phase: ACK/NAK 또는 즉시 온 프레임 처리
        while time.monotonic() < ack_deadline:
            remain = ack_deadline - time.monotonic()
            tok = await self._get_token(remain)
            if tok is None:
                break
            kind, payload = tok
            if kind == "NAK":
                return False, None
            if kind == "FRAME" and payload:
                if self._frame_match(payload, cmd.cmd):
                    csr_bytes = self._extract_data(payload)
                    break
            # ACK는 기록만 하고 계속
            # (ACK만으로 성공 처리하지 않음)

        # CSR 프레임을 아직 못 받았다면 전체 타임아웃까지 대기
        while (csr_bytes is None) and (time.monotonic() < end_deadline):
            remain = end_deadline - time.monotonic()
            tok = await self._get_token(remain)
            if tok is None:
                break
            kind, payload = tok
            if kind == "FRAME" and payload and self._frame_match(payload, cmd.cmd):
                csr_bytes = self._extract_data(payload)
                break

        if not csr_bytes or len(csr_bytes) < 1:
            return False, None

        csr = csr_bytes[0]
        if csr != 0:
            await self._emit_status(f"CSR {csr} ({CSR_CODES.get(csr, 'Unknown')}) for {self._cmd_label(cmd.cmd)}")
            return False, None

        # RF ON 성공 시점 알림(호환)
        if cmd.cmd == CMD_RF_ON:
            await self._event_q.put(RFPulseEvent(kind="target_reached", message="OK"))
        return True, csr_bytes

    async def _await_query_data(self, cmd: RfCommand) -> Tuple[bool, Optional[bytes]]:
        """ACK phase(짧게) → 데이터 프레임(동일 cmd, 동일 addr) 확보."""
        start = time.monotonic()
        ack_deadline = start + min(ACK_TIMEOUT_MS, cmd.timeout_ms // 3) / 1000.0
        end_deadline = start + cmd.timeout_ms / 1000.0

        data_bytes: Optional[bytes] = None

        # 빠른 경로(ACK phase에서 바로 데이터 프레임 도착)
        while time.monotonic() < ack_deadline:
            remain = ack_deadline - time.monotonic()
            tok = await self._get_token(remain)
            if tok is None:
                break
            kind, payload = tok
            if kind == "FRAME" and payload and self._frame_match(payload, cmd.cmd):
                data_bytes = self._extract_data(payload)
                break
            # NAK는 무시하지 말고 실패 처리
            if kind == "NAK":
                return False, None

        # 남은 시간 동안 데이터 프레임 대기
        while (data_bytes is None) and (time.monotonic() < end_deadline):
            remain = end_deadline - time.monotonic()
            tok = await self._get_token(remain)
            if tok is None:
                break
            kind, payload = tok
            if kind == "FRAME" and payload and self._frame_match(payload, cmd.cmd):
                data_bytes = self._extract_data(payload)
                break

        return (data_bytes is not None), data_bytes

    # ---------- 내부: 폴링 ----------
    async def _poll_loop(self):
        try:
            while True:
                if self._poll_busy or not self._connected:
                    await asyncio.sleep(0.01)
                    continue
                self._poll_busy = True

                # WAKE/STATUS
                st = await self._read_status()
                if st:
                    await self._emit_status(f"STATUS {self._status_summary_str(st)}")

                # FWD
                f = await self._query_and_data(CMD_REPORT_FORWARD, b"", tag="[POLL FWD]",
                                               timeout_ms=POLL_QUERY_TIMEOUT_MS)
                # REF
                r = await self._query_and_data(CMD_REPORT_REFLECTED, b"", tag="[POLL REF]",
                                               timeout_ms=POLL_QUERY_TIMEOUT_MS)

                if f is not None:
                    self._last_forward_w = float(_u16le(f, 0) if len(f) >= 2 else 0.0)
                if r is not None:
                    self._last_reflected_w = float(_u16le(r, 0) if len(r) >= 2 else 0.0)

                if (self._last_forward_w is not None) and (self._last_reflected_w is not None):
                    await self._event_q.put(RFPulseEvent(
                        kind="power", forward_w=self._last_forward_w, reflected_w=self._last_reflected_w
                    ))

                self._poll_busy = False
                await asyncio.sleep(POLL_INTERVAL_MS / 1000.0)
        except asyncio.CancelledError:
            self._poll_busy = False

    # ---------- 내부: 쿼리/exec 유틸 ----------
    async def _read_status(self) -> Optional[RfStatus]:
        data = await self._query_and_data(CMD_REPORT_STATUS, b"", tag="[POLL WAKE]", timeout_ms=POLL_QUERY_TIMEOUT_MS)
        st = self._parse_status_0xA2(data)
        if st:
            self._last_status = st
            await self._event_q.put(RFPulseEvent(kind="rf_status", rfstatus=st))
            self._validate_status(st)
        return st

    async def _exec_and_csr(self, cmd: int, data: bytes, *, tag: str = "", timeout_ms: Optional[int] = None) -> Tuple[bool, Optional[bytes]]:
        fut: asyncio.Future[Optional[bytes]] = asyncio.get_running_loop().create_future()
        self._enqueue_exec(cmd, data, tag=tag, timeout_ms=timeout_ms or ACK_TIMEOUT_MS,
                           callback=lambda b: (not fut.done()) and fut.set_result(b))
        try:
            res = await asyncio.wait_for(fut, timeout=(timeout_ms or ACK_TIMEOUT_MS)/1000.0 + 2.0)
        except asyncio.TimeoutError:
            return False, None
        return (res is not None), res

    async def _query_and_data(self, cmd: int, data: bytes, *, tag: str = "", timeout_ms: int = QUERY_TIMEOUT_MS) -> Optional[bytes]:
        fut: asyncio.Future[Optional[bytes]] = asyncio.get_running_loop().create_future()
        self._enqueue_query(cmd, data, tag=tag, timeout_ms=timeout_ms,
                            callback=lambda b: (not fut.done()) and fut.set_result(b))
        try:
            return await asyncio.wait_for(fut, timeout=timeout_ms/1000.0 + 2.0)
        except asyncio.TimeoutError:
            return None

    def _enqueue_exec(self, cmd: int, data: bytes, *, tag: str = "", timeout_ms: int = ACK_TIMEOUT_MS,
                      gap_ms: int = CMD_GAP_MS, retries: int = 3, allow_no_reply: bool = False,
                      allow_when_closing: bool = False, callback: Optional[Callable[[Optional[bytes]], None]] = None):
        if self._closing and not allow_when_closing:
            return
        cb = callback or (lambda _b: None)
        self._cmd_q.append(RfCommand(
            kind="exec", cmd=cmd, data=data, timeout_ms=timeout_ms, gap_ms=gap_ms,
            tag=tag, retries_left=retries, allow_no_reply=allow_no_reply,
            allow_when_closing=allow_when_closing, callback=cb
        ))

    def _enqueue_query(self, cmd: int, data: bytes, *, tag: str = "", timeout_ms: int = QUERY_TIMEOUT_MS,
                       gap_ms: int = CMD_GAP_MS, retries: int = 3, allow_when_closing: bool = False,
                       callback: Optional[Callable[[Optional[bytes]], None]] = None):
        if self._closing and not allow_when_closing:
            return
        cb = callback or (lambda _b: None)
        self._cmd_q.append(RfCommand(
            kind="query", cmd=cmd, data=data, timeout_ms=timeout_ms, gap_ms=gap_ms,
            tag=tag, retries_left=retries, allow_no_reply=False,
            allow_when_closing=allow_when_closing, callback=cb
        ))

    # ---------- 내부: 토큰/프레임 도우미 ----------
    async def _get_token(self, timeout_s: float) -> Optional[Token]:
        if timeout_s <= 0:
            timeout_s = 0.001
        try:
            tok = await asyncio.wait_for(self._tok_q.get(), timeout=timeout_s)
            return tok
        except asyncio.TimeoutError:
            return None

    def _frame_match(self, payload: bytes, expected_cmd: int) -> bool:
        if not payload or len(payload) < 3:
            return False
        hdr = payload[0]
        cmd_b = payload[1]
        rx_addr = (hdr >> 3) & 0x1F
        return (rx_addr == self.addr) and (cmd_b == expected_cmd)

    def _extract_data(self, payload: bytes) -> bytes:
        hdr = payload[0]
        length_bits = hdr & 0x07
        idx = 2
        if length_bits == 7:
            dlen = payload[idx]; idx += 1
        else:
            dlen = length_bits
        return bytes(payload[idx:idx+dlen])

    # ---------- 파싱/검증/로그 ----------
    def _parse_status_0xA2(self, data: Optional[bytes]) -> Optional[RfStatus]:
        if not data or len(data) < 4:
            asyncio.create_task(self._emit_status("STATUS payload too short"))
            return None
        b1, b2, b3, b4 = data[0], data[1], data[2], data[3]
        return RfStatus(
            rf_output_on      = bool(b1 & (1 << 5)),
            rf_on_requested   = bool(b1 & (1 << 6)),
            setpoint_mismatch = bool(b1 & (1 << 7)),
            interlock_open    = bool(b2 & (1 << 7)),
            overtemp          = bool(b2 & (1 << 3)),
            current_limit     = bool(b4 & (1 << 0)),
            extended_fault    = bool(b4 & (1 << 5)),
            cex_lock          = bool(b4 & (1 << 7)),
            raw = bytes(data[:4])
        )

    def _status_summary_str(self, st: RfStatus) -> str:
        return (f"on={int(st.rf_output_on)} req={int(st.rf_on_requested)} "
                f"sp_miss={int(st.setpoint_mismatch)} ilock={int(st.interlock_open)} "
                f"ot={int(st.overtemp)} limI={int(st.current_limit)} "
                f"xflt={int(st.extended_fault)} cex={int(st.cex_lock)}")

    def _validate_status(self, st: RfStatus) -> None:
        # 필요 시 강한 게이팅 가능. 여기선 경고만 로깅.
        if st.interlock_open:
            asyncio.create_task(self._emit_status("STATUS: Interlock OPEN detected"))
        if st.overtemp:
            asyncio.create_task(self._emit_status("STATUS: Over-Temperature detected"))
        if st.extended_fault:
            asyncio.create_task(self._emit_status("STATUS: Extended fault present"))
        if st.rf_on_requested and not st.rf_output_on:
            asyncio.create_task(self._emit_status("STATUS: RF requested but output not ON yet"))

    # ---------- 이벤트/유틸 ----------
    async def _emit_status(self, msg: str):
        if self.debug_print:
            print(f"[RFP][status] {msg}")
        await self._event_q.put(RFPulseEvent(kind="status", message=msg))

    async def _emit_failed(self, cmd: str, why: str):
        await self._event_q.put(RFPulseEvent(kind="command_failed", cmd=cmd, reason=why))

    def _safe_callback(self, cb: Optional[Callable[[Optional[bytes]], None]], arg: Optional[bytes]):
        if cb is None:
            return
        try:
            cb(arg)
        except Exception as e:
            self._dbg("RFP", f"콜백 오류: {e}")

    async def _cancel_task(self, name: str):
        t: Optional[asyncio.Task] = getattr(self, name)
        if t:
            t.cancel()
            try:
                await t
            except Exception:
                pass
            setattr(self, name, None)

    def _cmd_label(self, cmd: int) -> str:
        name = CMD_NAMES.get(cmd)
        return f"{name}(0x{cmd:02X})" if name else f"0x{cmd:02X}"

    def _dbg(self, src: str, msg: str):
        if self.debug_print:
            print(f"[{src}] {msg}")

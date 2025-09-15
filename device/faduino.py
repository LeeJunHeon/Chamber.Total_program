# -*- coding: utf-8 -*-
"""
faduino.py — asyncio 기반 Faduino 컨트롤러

의존성:
    pip install pyserial-asyncio

개요:
  - serial_asyncio + asyncio.Protocol 로 라인 프레이밍(CR/LF) 시리얼 통신
  - 단일 명령 큐(타임아웃/재시도/인터커맨드 gap) → 송수신 충돌 제거
  - 연결 워치독(지수 백오프) → 중간 단선도 자동 복구
  - 폴링: 주기마다 'S' 전체 상태(릴레이/ RF / DC) 한 사이클, 중첩 금지
  - 공개 API: set_relay / set_rf_power / set_dc_power / *_unverified / force_*_read / set_process_status 등
  - 이벤트 스트림(events): status / rf_power / dc_power / command_confirmed / command_failed

Qt 의존성 없음. UI는 qasync로 통합 이벤트 루프에서 이 모듈을 await/consume 하면 됨.
"""

from __future__ import annotations
from dataclasses import dataclass
from collections import deque
from typing import Optional, Deque, Callable, AsyncGenerator, Literal
import asyncio
import time
import re

try:
    import serial_asyncio
except Exception as e:
    raise RuntimeError("pyserial-asyncio가 필요합니다. `pip install pyserial-asyncio`") from e

from lib.config_ch2 import (
    FADUINO_PORT, FADUINO_BAUD, BUTTON_TO_PIN,
    RF_PARAM_ADC_TO_WATT, RF_OFFSET_ADC_TO_WATT,
    DC_PARAM_ADC_TO_VOLT, DC_OFFSET_ADC_TO_VOLT,
    DC_PARAM_ADC_TO_AMP,  DC_OFFSET_ADC_TO_AMP,
    ADC_FULL_SCALE, ADC_INPUT_VOLT, RF_WATT_PER_VOLT,
    DAC_FULL_SCALE, FADUINO_POLLING_INTERVAL_MS,
    FADUINO_WATCHDOG_INTERVAL_MS, FADUINO_TIMEOUT_MS,
    FADUINO_GAP_MS, FADUINO_RECONNECT_BACKOFF_START_MS,
    FADUINO_RECONNECT_BACKOFF_MAX_MS, DEBUG_PRINT,
)

# ================== 이벤트 모델 ==================
EventKind = Literal["status", "rf_power", "dc_power", "command_confirmed", "command_failed"]

@dataclass
class FaduinoEvent:
    kind: EventKind
    message: Optional[str] = None        # status / failed
    cmd: Optional[str] = None            # confirmed/failed
    reason: Optional[str] = None         # failed
    rf_forward: Optional[float] = None   # rf_power
    rf_reflected: Optional[float] = None # rf_power
    dc_p: Optional[float] = None         # dc_power
    dc_v: Optional[float] = None         # dc_power
    dc_c: Optional[float] = None         # dc_power

# ================== 명령 레코드 ==================
@dataclass
class Command:
    cmd_str: str
    callback: Optional[Callable[[Optional[str]], None]]
    timeout_ms: int
    gap_ms: int
    tag: str
    retries_left: int
    allow_no_reply: bool

# ================== Protocol (라인 프레이밍) ==================
class _FaduinoProtocol(asyncio.Protocol):
    def __init__(self, owner: "AsyncFaduino"):
        self.owner = owner
        self.transport: Optional[asyncio.Transport] = None
        self._rx = bytearray()
        self._RX_MAX = 16 * 1024
        self._LINE_MAX = 512

    def connection_made(self, transport: asyncio.BaseTransport):
        self.transport = transport  # type: ignore
        self.owner._on_connection_made(self.transport)

    def data_received(self, data: bytes):
        if not data:
            return
        self._rx.extend(data)
        if len(self._rx) > self._RX_MAX:
            del self._rx[:-self._RX_MAX]
            self.owner._dbg("Faduino", f"수신 버퍼 과다(RX>{self._RX_MAX}); 최근 {self._RX_MAX}B만 보존.")

        while True:
            i_cr = self._rx.find(b'\r')
            i_lf = self._rx.find(b'\n')
            if i_cr == -1 and i_lf == -1:
                break
            idx = i_cr if i_lf == -1 else (i_lf if i_cr == -1 else min(i_cr, i_lf))
            line_bytes = self._rx[:idx]

            drop = idx + 1
            if drop < len(self._rx):
                ch = self._rx[idx]; nxt = self._rx[idx+1]
                if (ch == 13 and nxt == 10) or (ch == 10 and nxt == 13):
                    drop += 1
            del self._rx[:drop]

            if len(line_bytes) > self._LINE_MAX:
                self.owner._dbg("Faduino", f"Rx line too long (+{len(line_bytes)-self._LINE_MAX}B), truncating")
                line_bytes = line_bytes[:self._LINE_MAX]

            try:
                line = line_bytes.decode('ascii', errors='ignore').strip()
            except Exception:
                line = ""

            if line:
                self.owner._on_line_from_serial(line)
                break

        while self._rx[:1] in (b'\r', b'\n'):
            del self._rx[0:1]

    def connection_lost(self, exc: Optional[Exception]):
        self.owner._on_connection_lost(exc)

# ================== Async 컨트롤러 ==================
class AsyncFaduino:
    def __init__(self):
        self.debug_print = DEBUG_PRINT

        # 연결 상태
        self._transport: Optional[asyncio.Transport] = None
        self._protocol: Optional[_FaduinoProtocol] = None
        self._connected: bool = False

        # 명령 큐/인플라이트
        self._cmd_q: Deque[Command] = deque()
        self._inflight: Optional[Command] = None

        # 수신 라인 큐 (Protocol → 워커)
        self._line_q: asyncio.Queue[str] = asyncio.Queue(maxsize=1024)

        # 이벤트 큐 (상위 UI/브리지 소비)
        self._event_q: asyncio.Queue[FaduinoEvent] = asyncio.Queue(maxsize=1024)

        # 태스크들
        self._want_connected: bool = False
        self._watchdog_task: Optional[asyncio.Task] = None
        self._cmd_worker_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None

        # 재연결 백오프
        self._reconnect_backoff_ms = FADUINO_RECONNECT_BACKOFF_START_MS

        # 런타임 상태
        self.expected_relay_mask = 0
        self._is_first_poll = True
        self._poll_cycle_active: bool = False
        self.is_rf_active = False
        self.is_dc_active = False
        self.rf_forward = 0.0
        self.rf_reflected = 0.0
        self.dc_voltage = 0.0
        self.dc_current = 0.0

    # ---------- 공용 API ----------
    async def start(self):
        """워치독/커맨드 워커 시작(연결은 워치독이 관리)."""
        if self._watchdog_task or self._cmd_worker_task:
            return
        self._want_connected = True
        loop = asyncio.get_running_loop()
        self._watchdog_task = loop.create_task(self._watchdog_loop(), name="FaduinoWatchdog")
        self._cmd_worker_task = loop.create_task(self._cmd_worker_loop(), name="FaduinoCmdWorker")
        await self._emit_status("Faduino 워치독/워커 시작")

    async def cleanup(self):
        """컨트롤러 완전 종료 (폴링 중지, 큐 비움, 연결 해제)."""
        await self._emit_status("Faduino 종료 절차 시작")
        self._want_connected = False

        # 폴링 중지
        await self._cancel_task("_poll_task")
        # 워커/워치독 중지
        await self._cancel_task("_cmd_worker_task")
        await self._cancel_task("_watchdog_task")

        # 큐 정리
        self._purge_pending("shutdown")

        # 연결 종료
        if self._transport:
            try:
                self._transport.close()
            except Exception:
                pass
        self._transport = None
        self._protocol = None
        self._connected = False

        await self._emit_status("Faduino 연결 종료됨")

    async def events(self) -> AsyncGenerator[FaduinoEvent, None]:
        """상위에서 소비하는 이벤트 스트림."""
        while True:
            ev = await self._event_q.get()
            yield ev

    # ---- 고수준 제어 API ----
    async def handle_named_command(self, name: str, state: bool):
        if name not in BUTTON_TO_PIN:
            await self._emit_status(f"알 수 없는 버튼명: {name}")
            return
        await self.set_relay(BUTTON_TO_PIN[name], state)

    async def set_relay(self, pin: int, state: bool):
        cmd = f"R,{pin},{1 if state else 0}"
        def on_reply(line: Optional[str], pin=pin, state=state):
            if (line or '').strip() == 'ACK_R':
                if state: self.expected_relay_mask |= (1 << pin)
                else:     self.expected_relay_mask &= ~(1 << pin)
                self._ev_nowait(FaduinoEvent(kind="command_confirmed", cmd=f"R,{pin},{1 if state else 0}"))
                self._log_status_sync(f"Relay({pin}) → {'ON' if state else 'OFF'}")
            else:
                self._ev_nowait(FaduinoEvent(kind="command_failed", cmd="R",
                                             reason=f"Relay({pin}) 응답 불일치: {repr(line)}"))
        self._enqueue(cmd, on_reply, timeout_ms=FADUINO_TIMEOUT_MS, gap_ms=FADUINO_GAP_MS, tag=f'[R {pin}]')

    async def set_rf_power(self, value: int):
        v = self._clamp_dac(value)
        cmd = f"W,{v}"
        def on_reply(line: Optional[str], v=v):
            if (line or '').strip() == 'ACK_W':
                self._log_status_sync(f"RF DAC = {v}")  # 확인 이벤트는 보내지 않음(기존 정책)
            else:
                self._ev_nowait(FaduinoEvent(kind="command_failed", cmd="W",
                                             reason=f"응답 불일치: {repr(line)}"))
        self._enqueue(cmd, on_reply, timeout_ms=FADUINO_TIMEOUT_MS, gap_ms=FADUINO_GAP_MS, tag='[W]')

    async def set_dc_power(self, value: int):
        v = self._clamp_dac(value)
        cmd = f"D,{v}"
        def on_reply(line: Optional[str], v=v):
            if (line or '').strip() == 'ACK_D':
                self._log_status_sync(f"DC DAC = {v}")  # 확인 이벤트는 보내지 않음(기존 정책)
            else:
                self._ev_nowait(FaduinoEvent(kind="command_failed", cmd="D",
                                             reason=f"응답 불일치: {repr(line)}"))
        self._enqueue(cmd, on_reply, timeout_ms=FADUINO_TIMEOUT_MS, gap_ms=FADUINO_GAP_MS, tag='[D]')

    async def set_dc_power_unverified(self, value: int):
        v = self._clamp_dac(value)
        self._enqueue(f"D,{v}", None, timeout_ms=FADUINO_TIMEOUT_MS, gap_ms=FADUINO_GAP_MS,
                      tag='[Du]', allow_no_reply=True)

    async def set_rf_power_unverified(self, value: int):
        v = self._clamp_dac(value)
        self._enqueue(f"W,{v}", None, timeout_ms=FADUINO_TIMEOUT_MS, gap_ms=FADUINO_GAP_MS,
                      tag='[Wu]', allow_no_reply=True)

    async def force_status_read(self):
        """S: 릴레이/ RF / DC 한번에 갱신."""
        line = await self._send_and_wait_line('S', tag='[FORCE S]', timeout_ms=FADUINO_TIMEOUT_MS)
        self._handle_S_line_sync(line)

    async def force_rf_read(self):
        """폴링 중이면 S로, 아니면 r로 읽어서 RF만 갱신."""
        if self._poll_task and not self._poll_task.done():
            line = await self._send_and_wait_line('S', tag='[FORCE S via rf]', timeout_ms=FADUINO_TIMEOUT_MS)
            self._handle_S_line_sync(line, rf_only=True)
            return
        line = await self._send_and_wait_line('r', tag='[FORCE r]', timeout_ms=FADUINO_TIMEOUT_MS)
        p = self._parse_ok_and_compute(line or "")
        if p and p.get("type") == "OK_r" and "rf" in p and self.is_rf_active:
            rf_for, rf_ref = p["rf"]
            self._update_rf_sync(rf_for, rf_ref)

    async def force_dc_read(self):
        """폴링 중이면 S로, 아니면 d로 읽어서 DC만 갱신."""
        if self._poll_task and not self._poll_task.done():
            line = await self._send_and_wait_line('S', tag='[FORCE S via dc]', timeout_ms=FADUINO_TIMEOUT_MS)
            self._handle_S_line_sync(line, dc_only=True)
            return
        line = await self._send_and_wait_line('d', tag='[FORCE d]', timeout_ms=FADUINO_TIMEOUT_MS)
        p = self._parse_ok_and_compute(line or "")
        if p and p.get("type") == "OK_d" and "dc" in p and self.is_dc_active:
            dc_p, dc_v, dc_c = p["dc"]
            self._update_dc_sync(dc_p, dc_v, dc_c)

    async def force_pin_read(self):
        line = await self._send_and_wait_line('P', tag='[FORCE P]', timeout_ms=FADUINO_TIMEOUT_MS)
        p = self._parse_ok_and_compute(line or "")
        if p and p.get("type") == "OK_P":
            relay_mask = int(p["relay_mask"])
            self._initial_sync_if_needed(relay_mask)
            if relay_mask != self.expected_relay_mask:
                await self._emit_status(
                    f"[경고] 릴레이 상태 불일치! 예상 {self.expected_relay_mask}, 실제 {relay_mask}"
                )

    # ---- 폴링 on/off (Process와 연동) ----
    def set_process_status(self, should_poll: bool):
        """공정 시작/종료 시 폴링 제어."""
        if should_poll:
            if self._poll_task is None or self._poll_task.done():
                self._is_first_poll = True
                self._poll_cycle_active = False
                self._poll_task = asyncio.create_task(self._poll_loop())
        else:
            if self._poll_task:
                self._poll_task.cancel()
                self._poll_task = None
            self._poll_cycle_active = False

    def on_rf_state_changed(self, is_active: bool):
        self.is_rf_active = is_active
        self._log_status_sync(f"RF 컨트롤러 상태 감지: {'활성' if is_active else '비활성'}")

    def on_dc_state_changed(self, is_active: bool):
        self.is_dc_active = is_active
        self._log_status_sync(f"DC 컨트롤러 상태 감지: {'활성' if is_active else '비활성'}")

    def on_process_finished(self, success: bool):
        """공정 종료(성공/실패 공통) 시 폴링 중지 및 큐 정리."""
        self.set_process_status(False)
        self.is_rf_active = False
        self.is_dc_active = False
        self._purge_pending(f"process finished ({'ok' if success else 'fail'})")

    # ---------- 내부: 워치독/연결 ----------
    async def _watchdog_loop(self):
        backoff = self._reconnect_backoff_ms
        while self._want_connected:
            if self._connected:
                await asyncio.sleep(FADUINO_WATCHDOG_INTERVAL_MS / 1000.0)
                continue

            await self._emit_status(f"재연결 시도 예약... ({backoff} ms)")
            await asyncio.sleep(backoff / 1000.0)

            if not self._want_connected or self._connected:
                continue

            try:
                loop = asyncio.get_running_loop()
                transport, protocol = await serial_asyncio.create_serial_connection(
                    loop, lambda: _FaduinoProtocol(self), FADUINO_PORT, baudrate=FADUINO_BAUD
                )
                self._transport = transport
                self._protocol = protocol  # type: ignore
                self._connected = True
                backoff = FADUINO_RECONNECT_BACKOFF_START_MS
                self._rx_clear_pending_echo = False
                await self._emit_status(f"{FADUINO_PORT} 연결 성공 (asyncio)")
            except Exception as e:
                await self._emit_status(f"{FADUINO_PORT} 연결 실패: {e}")
                backoff = min(backoff * 2, FADUINO_RECONNECT_BACKOFF_MAX_MS)

    def _on_connection_made(self, transport: asyncio.Transport):
        # 옵션: DTR/RTS 제어가 필요한 장치면 여기서 serial 인스턴스를 얻어 설정 가능
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
        self._dbg("Faduino", f"연결 끊김: {exc}")

        # 진행 중 명령 복구/취소
        if self._inflight is not None:
            cmd = self._inflight
            self._inflight = None
            if cmd.retries_left > 0:
                cmd.retries_left -= 1
                self._cmd_q.appendleft(cmd)
            else:
                self._safe_callback(cmd.callback, None)

    def _on_line_from_serial(self, line: str):
        try:
            self._line_q.put_nowait(line)
        except asyncio.QueueFull:
            self._dbg("Faduino", "라인 큐가 가득 찼습니다. 가장 오래된 라인을 폐기합니다.")
            try:
                self._line_q.get_nowait()
            except Exception:
                pass
            try:
                self._line_q.put_nowait(line)
            except Exception:
                pass

    # ---------- 내부: 명령 워커 ----------
    async def _cmd_worker_loop(self):
        while True:
            await asyncio.sleep(0)  # cancel 친화
            if not self._cmd_q:
                await asyncio.sleep(0.01)
                continue
            if not self._connected or not self._transport:
                await asyncio.sleep(0.05)
                continue

            cmd = self._cmd_q.popleft()
            self._inflight = cmd
            sent_txt = cmd.cmd_str.strip()
            self._dbg("Faduino", f"[SEND] {sent_txt} (tag={cmd.tag})")

            # write
            try:
                payload = cmd.cmd_str.encode("ascii")
                self._transport.write(payload)
                await self._transport.drain() if hasattr(self._transport, "drain") else None
            except Exception as e:
                self._dbg("Faduino", f"{cmd.tag} {sent_txt} 전송 오류: {e}")
                self._inflight = None
                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    self._cmd_q.appendleft(cmd)
                else:
                    self._safe_callback(cmd.callback, None)
                if self._transport:
                    try: self._transport.close()
                    except Exception: pass
                self._connected = False
                continue

            # no-reply
            if cmd.allow_no_reply:
                self._safe_callback(cmd.callback, None)
                self._inflight = None
                await asyncio.sleep(cmd.gap_ms / 1000.0)
                continue

            # wait reply (echo skip)
            try:
                line = await self._read_one_line_skip_echo(sent_txt, cmd.timeout_ms / 1000.0)
            except asyncio.TimeoutError:
                self._dbg("Faduino", f"[TIMEOUT] {cmd.tag} {sent_txt}")
                self._inflight = None
                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    self._dbg("Faduino", f"{cmd.tag} {sent_txt} 재시도 남은횟수={cmd.retries_left}")
                    self._cmd_q.appendleft(cmd)
                    if self._transport:
                        try: self._transport.close()
                        except Exception: pass
                    self._connected = False
                else:
                    self._safe_callback(cmd.callback, None)
                    await asyncio.sleep(cmd.gap_ms / 1000.0)
                continue

            recv_txt = (line or "").strip()
            self._dbg("Faduino < 응답", f"{cmd.tag} {sent_txt} ← {recv_txt}")
            self._safe_callback(cmd.callback, recv_txt)
            self._inflight = None
            await asyncio.sleep(cmd.gap_ms / 1000.0)

    async def _read_one_line_skip_echo(self, sent_no_cr: str, timeout_s: float) -> str:
        deadline = time.monotonic() + timeout_s
        while True:
            remain = max(0.0, deadline - time.monotonic())
            if remain <= 0:
                raise asyncio.TimeoutError()
            line = await asyncio.wait_for(self._line_q.get(), timeout=remain)
            if not line:
                continue
            if line == sent_no_cr:
                continue  # echo skip
            return line

    # ---------- 내부: 폴링 ----------
    async def _poll_loop(self):
        try:
            while True:
                if self._poll_cycle_active:
                    await asyncio.sleep(0.01)
                    continue
                self._poll_cycle_active = True

                line = await self._send_and_wait_line('S', tag='[POLL S]', timeout_ms=FADUINO_TIMEOUT_MS)
                self._handle_S_line_sync(line)

                self._poll_cycle_active = False
                await asyncio.sleep(FADUINO_POLLING_INTERVAL_MS / 1000.0)
        except asyncio.CancelledError:
            self._poll_cycle_active = False

    def _handle_S_line_sync(self, line: Optional[str], rf_only: bool = False, dc_only: bool = False):
        p = self._parse_ok_and_compute(line or "")
        if p and p.get("type") == "ERROR":
            self._ev_nowait(FaduinoEvent(kind="command_failed", cmd="Faduino", reason=p.get("msg", "ERROR")))
            return
        if not p or p.get("type") != "OK_S":
            return
        relay_mask = int(p["relay_mask"])
        if self._initial_sync_if_needed(relay_mask):
            pass
        else:
            if relay_mask != self.expected_relay_mask:
                self._log_status_sync(f"[경고] 릴레이 상태 불일치! 예상 {self.expected_relay_mask}, 실제 {relay_mask}")
        if not dc_only and self.is_rf_active and "rf" in p:
            rf_for, rf_ref = p["rf"]
            self._update_rf_sync(rf_for, rf_ref)
        if not rf_only and self.is_dc_active and "dc" in p:
            dc_p, dc_v, dc_c = p["dc"]
            self._update_dc_sync(dc_p, dc_v, dc_c)

    # ---------- 내부: 공통 송수신 ----------
    def _enqueue(self, cmd_str: str, on_reply: Optional[Callable[[Optional[str]], None]],
                 *, timeout_ms: int = FADUINO_TIMEOUT_MS, gap_ms: int = FADUINO_GAP_MS,
                 tag: str = "", retries_left: int = 5, allow_no_reply: bool = False):
        if not cmd_str.endswith("\r"):
            cmd_str += "\r"
        self._cmd_q.append(Command(cmd_str, on_reply, timeout_ms, gap_ms, tag, retries_left, allow_no_reply))

    async def _send_and_wait_line(self, cmd_str: str, *, tag: str, timeout_ms: int, retries: int = 1) -> Optional[str]:
        fut: asyncio.Future[Optional[str]] = asyncio.get_running_loop().create_future()

        def _cb(line: Optional[str]):
            if not fut.done():
                fut.set_result(line)

        self._enqueue(cmd_str, _cb, timeout_ms=timeout_ms, gap_ms=FADUINO_GAP_MS,
                      tag=tag, retries_left=max(0, int(retries)), allow_no_reply=False)
        try:
            return await asyncio.wait_for(fut, timeout=(timeout_ms / 1000.0) + 2.0)
        except asyncio.TimeoutError:
            return None

    def _purge_pending(self, reason: str = "") -> int:
        purged = 0
        if self._inflight is not None:
            cmd = self._inflight
            self._inflight = None
            purged += 1
            self._safe_callback(cmd.callback, None)
        while self._cmd_q:
            c = self._cmd_q.popleft()
            purged += 1
            self._safe_callback(c.callback, None)
        if reason:
            self._dbg("Faduino", f"대기 중 명령 {purged}개 폐기 ({reason})")
        return purged

    # ---------- 내부: 파싱/계산 ----------
    def _parse_ok_and_compute(self, response: str):
        s = (response or "").strip()
        if s.startswith("OK_S,"):
            parts = s.split(",")
            if len(parts) != 6: 
                return None
            try:
                relay_mask = int(parts[1])
                rf_for, rf_ref = self._compute_rf(parts[2], parts[3])
                dc_p, dc_v, dc_c = self._compute_dc(parts[4], parts[5])
                return {"type":"OK_S","relay_mask":relay_mask,"rf":(rf_for,rf_ref),"dc":(dc_p,dc_v,dc_c)}
            except Exception:
                return None
        if s.startswith("OK_P,"):
            parts = s.split(",")
            if len(parts) != 2: 
                return None
            try:
                return {"type":"OK_P","relay_mask":int(parts[1])}
            except Exception:
                return None
        if s.startswith("OK_r,"):
            parts = s.split(",")
            if len(parts) != 3: 
                return None
            try:
                rf_for, rf_ref = self._compute_rf(parts[1], parts[2])
                return {"type":"OK_r","rf":(rf_for,rf_ref)}
            except Exception:
                return None
        if s.startswith("OK_d,"):
            parts = s.split(",")
            if len(parts) != 3: 
                return None
            try:
                dc_p, dc_v, dc_c = self._compute_dc(parts[1], parts[2])
                return {"type":"OK_d","dc":(dc_p,dc_v,dc_c)}
            except Exception:
                return None
        if s in ("ACK_R","ACK_W","ACK_D"):
            return {"type":s}
        if s.startswith("ERROR"):
            return {"type":"ERROR","msg":s}
        return None

    def _compute_rf(self, rf_for_raw, rf_ref_raw):
        rf_for_raw = float(rf_for_raw); rf_ref_raw = float(rf_ref_raw)
        rf_for = max(0.0, (RF_PARAM_ADC_TO_WATT * rf_for_raw) + RF_OFFSET_ADC_TO_WATT)
        rf_ref_v = (rf_ref_raw / ADC_FULL_SCALE) * ADC_INPUT_VOLT
        rf_ref = max(0.0, rf_ref_v * RF_WATT_PER_VOLT)
        return rf_for, rf_ref

    def _compute_dc(self, dc_v_raw, dc_c_raw):
        dc_v_raw = float(dc_v_raw); dc_c_raw = float(dc_c_raw)
        dc_v = max(0.0, (DC_PARAM_ADC_TO_VOLT * dc_v_raw) + DC_OFFSET_ADC_TO_VOLT)
        dc_c = max(0.0, (DC_PARAM_ADC_TO_AMP  * dc_c_raw) + DC_OFFSET_ADC_TO_AMP)
        dc_p = dc_v * dc_c
        return dc_p, dc_v, dc_c

    # ---------- 내부: 상태 업데이트/이벤트 ----------
    def _update_rf_sync(self, rf_for: float, rf_ref: float):
        self.rf_forward, self.rf_reflected = rf_for, rf_ref
        self._ev_nowait(FaduinoEvent(kind="rf_power", rf_forward=rf_for, rf_reflected=rf_ref))

    def _update_dc_sync(self, dc_p: float, dc_v: float, dc_c: float):
        self.dc_voltage, self.dc_current = dc_v, dc_c
        self._ev_nowait(FaduinoEvent(kind="dc_power", dc_p=dc_p, dc_v=dc_v, dc_c=dc_c))

    def _initial_sync_if_needed(self, relay_mask: int) -> bool:
        if self._is_first_poll:
            self.expected_relay_mask = int(relay_mask)
            self._is_first_poll = False
            self._log_status_sync(f"초기 릴레이 상태 동기화 완료: {relay_mask}")
            return True
        return False

    # ---------- 내부: 유틸 ----------
    def _clamp_dac(self, value: int) -> int:
        try:
            v = int(round(value))
        except Exception:
            v = 0
        if v < 0:
            v = 0
        if v > DAC_FULL_SCALE:
            v = DAC_FULL_SCALE
        return v

    async def _emit_status(self, msg: str):
        if self.debug_print:
            print(f"[Faduino][status] {msg}")
        await self._event_q.put(FaduinoEvent(kind="status", message=msg))

    def _log_status_sync(self, msg: str):
        if self.debug_print:
            print(f"[Faduino][status] {msg}")
        self._ev_nowait(FaduinoEvent(kind="status", message=msg))

    def _ev_nowait(self, ev: FaduinoEvent):
        try:
            self._event_q.put_nowait(ev)
        except Exception:
            pass

    def _safe_callback(self, cb: Optional[Callable[[Optional[str]], None]], arg: Optional[str]):
        if cb is None:
            return
        try:
            cb(arg)
        except Exception as e:
            self._dbg("Faduino", f"콜백 오류: {e}")

    async def _cancel_task(self, name: str):
        t: Optional[asyncio.Task] = getattr(self, name)
        if t:
            t.cancel()
            try:
                await t
            except Exception:
                pass
            setattr(self, name, None)

    def _dbg(self, src: str, msg: str):
        if self.debug_print:
            print(f"[{src}] {msg}")

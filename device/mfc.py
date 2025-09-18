# -*- coding: utf-8 -*-
"""
mfc.py — asyncio 기반 MFC 컨트롤러

의존성: pyserial-asyncio
    pip install pyserial-asyncio

기능 요약(구 MFC.py와 동등):
  - serial_asyncio + asyncio.Protocol 로 라인 프레이밍(CR/LF) 시리얼 통신
  - 단일 명령 큐(타임아웃/재시도/인터커맨드 gap) → 송수신 충돌 제거
  - 연결 워치독(지수 백오프) → 중간 단선도 자동 복구
  - 폴링: 주기마다 R60(전체 유량) → R5(압력) 한 사이클, 중첩 금지
  - FLOW_SET 후 READ_FLOW_SET 검증, FLOW_ON 시 안정화 루프(목표 도달 확인)
  - 밸브 OPEN/CLOSE 검증, SP1_SET/ON, SP4_ON 검증
  - 압력 스케일: UI↔HW 변환 유지, tolerance/모니터링 규칙 유지

상위(UI/브리지)와의 통신:
  - async 제너레이터 events() 로 상태/측정/확인/실패 이벤트를 전달
  - 공개 메서드는 모두 asyncio에서 await로 호출
"""

from __future__ import annotations
from dataclasses import dataclass
from collections import deque
from typing import Optional, Deque, Callable, AsyncGenerator, Literal
import asyncio
import re
import time

try:
    import serial_asyncio
except Exception as e:
    raise RuntimeError("pyserial-asyncio가 필요합니다. `pip install pyserial-asyncio`") from e

from lib.config_ch2 import (
    MFC_PORT,
    MFC_BAUD,
    MFC_COMMANDS,
    FLOW_ERROR_TOLERANCE,
    FLOW_ERROR_MAX_COUNT,
    MFC_SCALE_FACTORS,
    MFC_POLLING_INTERVAL_MS,
    MFC_STABILIZATION_INTERVAL_MS,
    MFC_WATCHDOG_INTERVAL_MS,
    MFC_RECONNECT_BACKOFF_START_MS,
    MFC_RECONNECT_BACKOFF_MAX_MS,
    MFC_TIMEOUT,
    MFC_GAP_MS,
    MFC_DELAY_MS,
    MFC_DELAY_MS_VALVE,
    DEBUG_PRINT,
    MFC_PRESSURE_SCALE,
    MFC_PRESSURE_DECIMALS,
    MFC_SP1_VERIFY_TOL,
)

# =============== 이벤트 모델 ===============
EventKind = Literal["status", "flow", "pressure", "command_confirmed", "command_failed"]

@dataclass
class MFCEvent:
    kind: EventKind
    message: Optional[str] = None                 # status/failed
    cmd: Optional[str] = None                     # confirmed/failed
    reason: Optional[str] = None                  # failed
    gas: Optional[str] = None                     # flow
    value: Optional[float] = None                 # flow/pressure numeric(UI 단위)
    text: Optional[str] = None                    # pressure 문자열 표시값

# =============== 명령 레코드 ===============
@dataclass
class Command:
    cmd_str: str
    callback: Optional[Callable[[Optional[str]], None]]
    timeout_ms: int
    gap_ms: int
    tag: str
    retries_left: int
    allow_no_reply: bool

# =============== Protocol (라인 프레이밍) ===============
class _MFCProtocol(asyncio.Protocol):
    def __init__(self, owner: "AsyncMFC"):
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
            self.owner._dbg("MFC", f"수신 버퍼 과다(RX>{self._RX_MAX}); 최근 {self._RX_MAX}B만 보존.")

        processed = 0  # ✅ 이번 이벤트에서 처리한 라인 수를 바깥에 둔다
        while True:
            i_cr = self._rx.find(b'\r')
            i_lf = self._rx.find(b'\n')
            if i_cr == -1 and i_lf == -1:
                break

            idx = i_cr if i_lf == -1 else (i_lf if i_cr == -1 else min(i_cr, i_lf))
            line_bytes = self._rx[:idx]

            # CR/LF 혹은 LF/CR 쌍 같이 제거
            drop = idx + 1
            if drop < len(self._rx):
                ch = self._rx[idx]
                nxt = self._rx[idx + 1]
                if (ch == 13 and nxt == 10) or (ch == 10 and nxt == 13):
                    drop += 1
            del self._rx[:drop]

            if len(line_bytes) > self._LINE_MAX:
                self.owner._dbg("MFC", f"Rx line too long (+{len(line_bytes)-self._LINE_MAX}B), truncating")
                line_bytes = line_bytes[:self._LINE_MAX]

            try:
                line = line_bytes.decode("ascii", errors="ignore").strip()
            except Exception:
                line = ""

            if not line:
                continue

            # ✅ 라인 하나를 바로 전달
            self.owner._on_line_from_serial(line)
            processed += 1
            if processed >= 32:  # ✅ 과도 루프 안전장치
                self.owner._dbg("MFC", "한 번에 32라인 초과 수신 → 루프 종료")
                break

        # 선행 CR/LF 정리
        while self._rx[:1] in (b'\r', b'\n'):
            del self._rx[0:1]

    def connection_lost(self, exc: Optional[Exception]):
        self.owner._on_connection_lost(exc)

# =============== Async 컨트롤러 ===============
class AsyncMFC:
    def __init__(self):
        self.debug_print = DEBUG_PRINT

        # 연결 상태
        self._transport: Optional[asyncio.Transport] = None
        self._protocol: Optional[_MFCProtocol] = None
        self._connected: bool = False
        self._ever_connected: bool = False

        # 명령 큐/인플라이트
        self._cmd_q: Deque[Command] = deque()
        self._inflight: Optional[Command] = None

        # 수신 라인 큐 (Protocol → 워커)
        self._line_q: asyncio.Queue[str] = asyncio.Queue(maxsize=1024)

        # 이벤트 큐 (상위 UI/브리지 소비)
        self._event_q: asyncio.Queue[MFCEvent] = asyncio.Queue(maxsize=1024)

        # 태스크들
        self._want_connected: bool = False
        self._watchdog_task: Optional[asyncio.Task] = None
        self._cmd_worker_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._stab_task: Optional[asyncio.Task] = None

        # 재연결 백오프
        self._reconnect_backoff_ms = MFC_RECONNECT_BACKOFF_START_MS

        # 런타임/스케일/모니터링
        self.gas_map = {1: "Ar", 2: "O2", 3: "N2"}
        self.last_setpoints = {1: 0.0, 2: 0.0, 3: 0.0}      # 장비 단위(HW)
        self.flow_error_counters = {1: 0, 2: 0, 3: 0}

        # 폴링 사이클 중첩 방지 플래그
        self._poll_cycle_active: bool = False

        # ★ 과거 no-reply 명령의 에코를 1회성으로 버리기 위한 대기열
        self._skip_echos: deque[str] = deque()

        # 안정화 상태
        self._stab_ch: Optional[int] = None
        self._stab_target_hw: float = 0.0
        self._stab_attempts: int = 0
        self._stab_pending_cmd: Optional[str] = None  # FLOW_ON 확정 시점 관리

    # ---------- 공용 API ----------
    async def start(self):
        """워치독/커맨드 워커 시작(연결은 워치독이 관리). 재호출/죽은 태스크 회복 안전."""
        # 1) 죽은 태스크 정리
        if self._watchdog_task and self._watchdog_task.done():
            self._watchdog_task = None
        if self._cmd_worker_task and self._cmd_worker_task.done():
            self._cmd_worker_task = None

        # 2) 이미 둘 다 살아 있으면 종료
        if self._watchdog_task and self._cmd_worker_task:
            return

        # 3) 재가동
        self._want_connected = True
        loop = asyncio.get_running_loop()
        if not self._watchdog_task:
            self._watchdog_task = loop.create_task(self._watchdog_loop(), name="MFCWatchdog")
        if not self._cmd_worker_task:
            self._cmd_worker_task = loop.create_task(self._cmd_worker_loop(), name="MFCCmdWorker")
        await self._emit_status("MFC 워치독/워커 시작")

    async def cleanup(self):
        """컨트롤러 완전 종료."""
        await self._emit_status("MFC 종료 절차 시작")
        self._want_connected = False

        # 폴링/안정화 태스크 중지
        await self._cancel_task("_poll_task")
        await self._cancel_task("_stab_task")

        # 명령 워커/워치독 중지
        await self._cancel_task("_cmd_worker_task")
        await self._cancel_task("_watchdog_task")

        # 큐/인플라이트 정리
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

        await self._emit_status("MFC 연결 종료됨")

    async def events(self) -> AsyncGenerator[MFCEvent, None]:
        """상위에서 소비하는 이벤트 스트림."""
        while True:
            ev = await self._event_q.get()
            yield ev

    # ---- 고수준 제어 API (기존 handle_command 세분화) ----
    async def set_flow(self, channel: int, ui_value: float):
        """FLOW_SET + READ_FLOW_SET 검증."""
        sf = float(MFC_SCALE_FACTORS.get(channel, 1.0))
        scaled = float(ui_value) * sf
        await self._emit_status(f"Ch{channel} 유량 스케일: {ui_value:.2f}sccm → 장비 {scaled:.2f}")

        # SET (no-reply)
        set_cmd = self._mk_cmd("FLOW_SET", channel=channel, value=scaled)
        self._enqueue(set_cmd, None, allow_no_reply=True, tag=f"[SET ch{channel}]")

        # 검증
        ok = await self._verify_flow_set(channel, scaled)
        if ok:
            self.last_setpoints[channel] = scaled
            await self._emit_confirmed("FLOW_SET")
        else:
            await self._emit_failed("FLOW_SET", f"Ch{channel} FLOW_SET 확인 실패")

    async def flow_on(self, channel: int):
        """R69 읽어 비트 수정 → L0 적용/검증 → 안정화 시작 → 안정화 성공 시 FLOW_ON 확정."""
        # 현재 마스크 읽기
        now = await self._read_r69_bits()
        if not now:
            await self._emit_failed("FLOW_ON", "R69 읽기 실패")
            return

        bits = list(now.ljust(4, '0'))
        if 1 <= channel <= len(bits):
            bits[channel-1] = '1'
        target = ''.join(bits[:4])

        # 안정화 상태 초기화(이 채널 대상으로 재시작)
        await self._cancel_task("_stab_task")
        self._stab_ch = None
        self._stab_target_hw = 0.0
        self._stab_pending_cmd = None

        # L0 적용/검증
        ok = await self._set_onoff_mask_and_verify(target)
        if not ok:
            await self._emit_failed("FLOW_ON", f"L0 적용 불일치(now!=want)")
            return

        # 켜진 채널이 맞고, 목표 유량이 존재하면 안정화 시작
        if 1 <= channel <= len(target) and target[channel-1] == '1':
            tgt = float(self.last_setpoints.get(channel, 0.0))
            if tgt > 0:
                self._stab_ch = channel
                self._stab_target_hw = tgt
                self._stab_attempts = 0
                self._stab_pending_cmd = "FLOW_ON"
                self._stab_task = asyncio.create_task(self._stabilization_loop())
                await self._emit_status(f"FLOW_ON: ch{channel} 안정화 시작 (목표 HW {tgt:.2f})")
                return

        # 안정화가 불필요하면 바로 확정
        await self._emit_confirmed("FLOW_ON")

    async def flow_off(self, channel: int):
        """R69 읽어 비트 수정 → L0 적용/검증 → 즉시 FLOW_OFF 확정."""
        # 이 채널 대상 안정화 중이면 취소
        if self._stab_ch == channel:
            await self._cancel_task("_stab_task")
            self._stab_ch = None
            self._stab_target_hw = 0.0
            self._stab_pending_cmd = None
            await self._emit_status(f"FLOW_OFF 요청: ch{channel} 안정화 취소")

        # 목표 0으로 초기화 (경고 오경보 방지)
        self.last_setpoints[channel] = 0.0
        self.flow_error_counters[channel] = 0

        now = await self._read_r69_bits()
        if not now:
            await self._emit_failed("FLOW_OFF", "R69 읽기 실패")
            return
        bits = list(now.ljust(4, '0'))
        if 1 <= channel <= len(bits):
            bits[channel-1] = '0'
        target = ''.join(bits[:4])

        ok = await self._set_onoff_mask_and_verify(target)
        if ok:
            await self._emit_confirmed("FLOW_OFF")
        else:
            await self._emit_failed("FLOW_OFF", "L0 적용 불일치")

    async def valve_open(self):
        await self._valve_move_and_verify("VALVE_OPEN")

    async def valve_close(self):
        await self._valve_move_and_verify("VALVE_CLOSE")

    async def sp1_set(self, ui_value: float):
        """SP1_SET (UI→HW 변환) + READ_SP1_VALUE 검증."""
        hw_val = round(float(ui_value) * float(MFC_PRESSURE_SCALE), int(MFC_PRESSURE_DECIMALS))
        await self._emit_status(f"SP1 스케일: UI {ui_value:.2f} → 장비 {hw_val:.{int(MFC_PRESSURE_DECIMALS)}f}")

        # SET (no-reply)
        self._enqueue(self._mk_cmd("SP1_SET", value=hw_val), None, allow_no_reply=True, tag="[SP1_SET]")

        # 검증
        ok = await self._verify_sp1_set(hw_val, ui_value)
        if ok:
            await self._emit_confirmed("SP1_SET")
        else:
            await self._emit_failed("SP1_SET", "SP1 설정 확인 실패")

    async def sp1_on(self):
        ok = await self._verify_simple_flag("SP1_ON", expect_mask='1')
        if ok: await self._emit_confirmed("SP1_ON")
        else:  await self._emit_failed("SP1_ON", "SP1 상태 확인 실패")

    async def sp4_on(self):
        ok = await self._verify_simple_flag("SP4_ON", expect_mask='4')
        if ok: await self._emit_confirmed("SP4_ON")
        else:  await self._emit_failed("SP4_ON", "SP4 상태 확인 실패")

    async def read_flow_all(self):
        """R60 한 번 읽고 이벤트로 각 채널 흐름을 방출."""
        vals = await self._read_r60_values()
        if not vals:
            await self._emit_failed("READ_FLOW", "R60 파싱 실패")
            return
        for ch, name in self.gas_map.items():
            idx = ch - 1
            if idx < len(vals):
                v_hw = float(vals[idx])
                sf = float(MFC_SCALE_FACTORS.get(ch, 1.0))
                await self._emit_flow(name, v_hw / sf)
                self._monitor_flow(ch, v_hw)

    async def read_pressure(self):
        """R5(예: READ_PRESSURE) 읽고 UI 문자열/숫자로 이벤트."""
        line = await self._send_and_wait_line(self._mk_cmd("READ_PRESSURE"), tag="[READ_PRESSURE]", timeout_ms=MFC_TIMEOUT)
        if not (line and line.strip()):
            await self._emit_failed("READ_PRESSURE", "응답 없음")
            return
        self._emit_pressure_from_line_sync(line.strip())

    async def handle_command(self, cmd: str, args: dict | None = None) -> None:
        """
        main/process에서 넘어오는 문자열 명령을 고수준 메서드로 라우팅한다.
        - cmd: 'FLOW_SET', 'FLOW_ON', 'FLOW_OFF', 'VALVE_OPEN', 'VALVE_CLOSE',
               'PS_ZEROING', 'MFC_ZEROING', 'SP4_ON', 'SP1_ON', 'SP1_SET',
               'READ_FLOW_ALL', 'READ_PRESSURE'
        - args: 필요한 인자 (channel, value 등)
        """
        args = args or {}
        key = (cmd or "").strip().upper()

        def _req(name: str, cast=float):
            if name not in args:
                raise KeyError(f"'{name}' is required for {key}")
            try:
                return cast(args[name])
            except Exception as e:
                raise ValueError(f"invalid {name} for {key}: {args[name]!r}") from e

        try:
            if key == "FLOW_SET":
                ch = _req("channel", int)
                val_ui = _req("value", float)
                await self.set_flow(ch, val_ui)

            elif key == "FLOW_ON":
                ch = _req("channel", int)
                await self.flow_on(ch)

            elif key == "FLOW_OFF":
                ch = _req("channel", int)
                await self.flow_off(ch)

            elif key == "VALVE_OPEN":
                await self.valve_open()

            elif key == "VALVE_CLOSE":
                await self.valve_close()

            elif key == "PS_ZEROING":
                # 워커가 gap_ms 만큼 쉬고 나서 호출 → 그 시점에 확인 이벤트 방출
                def _ok_cb(_):
                    asyncio.create_task(self._emit_confirmed("PS_ZEROING"))

                self._enqueue(self._mk_cmd("PS_ZEROING"), _ok_cb,
                            allow_no_reply=True, tag="[PS_ZEROING]",
                            gap_ms=MFC_GAP_MS)  # 필요시 MFC_DELAY_MS 로 바꿔 더 길게도 가능
                await self._emit_status("압력 센서 Zeroing 명령 전송")

            elif key == "MFC_ZEROING":
                ch = _req("channel", int)

                def _ok_cb(_):
                    asyncio.create_task(self._emit_confirmed("MFC_ZEROING"))

                self._enqueue(self._mk_cmd("MFC_ZEROING", channel=ch), _ok_cb,
                            allow_no_reply=True, tag=f"[MFC_ZEROING ch{ch}]",
                            gap_ms=MFC_GAP_MS)  # 필요시 MFC_DELAY_MS 로 조절 가능
                await self._emit_status(f"Ch{ch} MFC Zeroing 명령 전송")

            elif key == "SP4_ON":
                await self.sp4_on()

            elif key == "SP1_ON":
                await self.sp1_on()

            elif key == "SP1_SET":
                val_ui = _req("value", float)
                await self.sp1_set(val_ui)

            elif key in ("READ_FLOW_ALL", "READ_FLOW"):  # 호환용
                await self.read_flow_all()

            elif key in ("READ_PRESSURE",):
                await self.read_pressure()

            else:
                await self._emit_failed(key, "지원되지 않는 MFC 명령")
        except Exception as e:
            await self._emit_failed(key, f"예외: {e}")

    # ---- 폴링 on/off (Process와 연동) ----
    def set_process_status(self, should_poll: bool):
        if should_poll:
            if self._poll_task is None or self._poll_task.done():
                self._ev_nowait(MFCEvent(kind="status", message="주기적 읽기(Polling) 시작"))
                self._poll_task = asyncio.create_task(self._poll_loop())
        else:
            if self._poll_task:
                self._poll_task.cancel()
                self._poll_task = None
            self._poll_cycle_active = False
            purged = self._purge_poll_reads_only(cancel_inflight=True, reason="polling off")
            self._ev_nowait(MFCEvent(kind="status", message="주기적 읽기(Polling) 중지"))
            if purged:
                self._ev_nowait(MFCEvent(kind="status", message=f"[QUIESCE] 폴링 읽기 {purged}건 제거 (polling off)"))

    def on_process_finished(self, success: bool):
        """공정 종료 시 내부 상태 리셋."""
        self.set_process_status(False)
        # 안정화 중지
        if self._stab_task:
            self._stab_task.cancel()
            self._stab_task = None
        self._stab_ch = None
        self._stab_target_hw = 0.0
        self._stab_pending_cmd = None
        # 큐 정리 및 카운터 리셋
        self._purge_pending(f"process finished ({'ok' if success else 'fail'})")
        self.last_setpoints = {1: 0.0, 2: 0.0, 3: 0.0}
        self.flow_error_counters = {1: 0, 2: 0, 3: 0}
        self._poll_cycle_active = False

    # ---------- 내부: 워치독/연결 ----------
    async def _watchdog_loop(self):
        backoff = self._reconnect_backoff_ms
        while self._want_connected:
            if self._connected:
                await asyncio.sleep(MFC_WATCHDOG_INTERVAL_MS / 1000.0)
                continue

            if self._ever_connected:
                await self._emit_status(f"재연결 시도 예약... ({backoff} ms)")
                await asyncio.sleep(backoff / 1000.0)

            if not self._want_connected or self._connected:
                continue

            try:
                loop = asyncio.get_running_loop()
                transport, protocol = await serial_asyncio.create_serial_connection(
                    loop, lambda: _MFCProtocol(self), MFC_PORT, baudrate=MFC_BAUD
                )
                self._transport = transport
                self._protocol = protocol  # type: ignore
                self._connected = True
                self._ever_connected = True
                backoff = MFC_RECONNECT_BACKOFF_START_MS
                await self._emit_status(f"{MFC_PORT} 연결 성공 (asyncio)")
            except Exception as e:
                await self._emit_status(f"{MFC_PORT} 연결 실패: {e}")
                backoff = min(backoff * 2, MFC_RECONNECT_BACKOFF_MAX_MS)

    def _on_connection_made(self, transport: asyncio.Transport):
        self._transport = transport  # 이미 너의 코드에 있으면 생략
        # ★ pyserial 객체 접근 → 입력/출력 버퍼 리셋 & 라인 제어
        ser = getattr(transport, "serial", None)
        if ser:
            try:
                ser.reset_input_buffer()
                ser.reset_output_buffer()
                # Qt 버전이 하던 것과 유사하게
                ser.dtr = True
                ser.rts = False
            except Exception:
                pass

        # 잔여 라인/에코 정리
        self._skip_echos.clear()
        asyncio.create_task(self._absorb_late_lines(150))  # 100 → 150ms로 살짝 증대

    def _on_connection_lost(self, exc: Optional[Exception]):
        self._connected = False
        if self._transport:
            try:
                self._transport.close()
            except Exception:
                pass
        self._transport = None
        self._protocol = None
        self._dbg("MFC", f"연결 끊김: {exc}")

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
        # ★ 1) 과거 no-reply 에코면 여기서 즉시 버림 (큐로 가지 않게)
        if self._skip_echos and line == self._skip_echos[0]:
            self._skip_echos.popleft()
            self._dbg("MFC", f"[ECHO] 과거 no-reply 에코 스킵: {line}")
            return

        # 2) 일반 라인은 큐로 전달
        try:
            self._line_q.put_nowait(line)
        except asyncio.QueueFull:
            self._dbg("MFC", "라인 큐가 가득 찼습니다. 가장 오래된 라인을 폐기합니다.")
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
            self._dbg("MFC", f"[SEND] {sent_txt} (tag={cmd.tag})")

            # write (전송 직전: OS 입력버퍼 리셋 + 짧은 드레인)
            ser = getattr(self._transport, "serial", None)
            if ser:
                try:
                    ser.reset_input_buffer()    # ★ OS 입력버퍼 싹 비우기
                except Exception:
                    pass
            await self._absorb_late_lines(60)   # ★ 20 → 60ms로 상향

            # write
            try:
                payload = cmd.cmd_str.encode("ascii")
                self._transport.write(payload)
            except Exception as e:
                self._dbg("MFC", f"{cmd.tag} {sent_txt} 전송 오류: {e}")
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
                # 먼저 inflight 해제, 그 다음 gap_ms만큼 대기, 마지막에 콜백 호출
                self._inflight = None
                await asyncio.sleep(cmd.gap_ms / 1000.0)
                self._safe_callback(cmd.callback, None)
                continue

            # wait reply (echo skip)
            try:
                line = await self._read_one_line_skip_echo(sent_txt, cmd.timeout_ms / 1000.0)
            except asyncio.TimeoutError:
                self._dbg("MFC", f"[TIMEOUT] {cmd.tag} {sent_txt}")
                self._inflight = None
                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    self._dbg("MFC", f"{cmd.tag} {sent_txt} 재시도 남은횟수={cmd.retries_left}")
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
            self._dbg("MFC < 응답", f"{cmd.tag} {sent_txt} ← {recv_txt}")
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
            # ① 방금 보낸 명령의 에코
            if line == sent_no_cr:
                # 추적 로그 (optional)
                self._dbg("MFC", f"[ECHO] 현재 명령 에코 스킵: {line}")
                continue
            # ② 과거 no-reply 에코 (드물게 큐에 남아있을 수 있음)
            if self._skip_echos and line == self._skip_echos[0]:
                self._skip_echos.popleft()
                await self._emit_status(f"[ECHO] 과거 no-reply 에코 스킵(큐): {line}")
                continue
            return line

    # ---------- 내부: 폴링 ----------
    async def _poll_loop(self):
        try:
            while True:
                # 연결 안 됐으면 대기
                if not self._connected:
                    await asyncio.sleep(MFC_POLLING_INTERVAL_MS / 1000.0)
                    continue
                # ★ 비-폴링 명령이 대기/진행 중이면 폴링 양보
                if self._has_pending_non_poll_cmds():
                    await asyncio.sleep(0.02)
                    continue
                # 중첩 금지
                if self._poll_cycle_active:
                    await asyncio.sleep(0.01)
                    continue
                self._poll_cycle_active = True

                # R60 → flow 이벤트 + 모니터링
                vals = await self._read_r60_values(tag="[POLL R60]")
                if vals:
                    for ch, name in self.gas_map.items():
                        idx = ch - 1
                        if idx < len(vals):
                            v_hw = float(vals[idx])
                            sf = float(MFC_SCALE_FACTORS.get(ch, 1.0))
                            await self._emit_flow(name, v_hw / sf)
                            self._monitor_flow(ch, v_hw)

                # R5 → pressure 이벤트
                line = await self._send_and_wait_line(self._mk_cmd("READ_PRESSURE"),
                                                      tag="[POLL PRESS]", timeout_ms=MFC_TIMEOUT)
                if line:
                    self._emit_pressure_from_line_sync(line.strip())

                self._poll_cycle_active = False
                await asyncio.sleep(MFC_POLLING_INTERVAL_MS / 1000.0)
        except asyncio.CancelledError:
            self._poll_cycle_active = False

    # ---------- 내부: 안정화 ----------
    async def _stabilization_loop(self):
        try:
            while True:
                ch = self._stab_ch
                target = float(self._stab_target_hw)
                if ch is None or target <= 0:
                    await self._emit_failed("FLOW_ON", "안정화 대상 없음")
                    return

                vals = await self._read_r60_values(tag=f"[STAB R60 ch{ch}]")
                actual = None
                if vals and (ch - 1) < len(vals):
                    actual = float(vals[ch - 1])

                sf = float(MFC_SCALE_FACTORS.get(ch, 1.0))
                tol = target * float(FLOW_ERROR_TOLERANCE)

                self._stab_attempts += 1
                await self._emit_status(
                    f"유량 확인... (목표: {target:.2f}/{target/sf:.2f}sccm, "
                    f"현재: {(-1 if actual is None else actual):.2f}/"
                    f"{(-1 if actual is None else actual/sf):.2f}sccm)"
                )

                if (actual is not None) and (abs(actual - target) <= tol):
                    await self._emit_confirmed("FLOW_ON")
                    self._stab_ch = None
                    self._stab_target_hw = 0.0
                    self._stab_pending_cmd = None
                    return

                if self._stab_attempts >= 30:
                    await self._emit_failed("FLOW_ON", "유량 안정화 시간 초과")
                    self._stab_ch = None
                    self._stab_target_hw = 0.0
                    self._stab_pending_cmd = None
                    return

                await asyncio.sleep(MFC_STABILIZATION_INTERVAL_MS / 1000.0)
        except asyncio.CancelledError:
            pass

    # ---------- 내부: 고수준 시퀀스/검증 ----------
    async def _verify_flow_set(self, ch: int, scaled_value: float) -> bool:
        """READ_FLOW_SET(ch)으로 확인; 불일치면 재설정 후 재확인(최대 5회)."""
        for attempt in range(1, 6):
            line = await self._send_and_wait_line(self._mk_cmd("READ_FLOW_SET", channel=ch),
                                                  tag=f"[VERIFY SET ch{ch}]", timeout_ms=MFC_TIMEOUT)
            val = self._parse_q_value_with_prefixes(line or "", prefixes=(f"Q{4 + int(ch)}",))
            ok = (val is not None) and (abs(val - scaled_value) < 0.1)
            if ok:
                await self._emit_status(f"Ch{ch} 목표 {scaled_value:.2f} 설정 확인")
                return True

            # 재전송 후 지연 → 재확인
            self._enqueue(self._mk_cmd("FLOW_SET", channel=ch, value=scaled_value), None,
                          allow_no_reply=True, tag=f"[RE-SET ch{ch}]")
            await self._emit_status(f"[FLOW_SET 검증 재시도] ch{ch}: 기대={scaled_value:.2f}, 응답={repr(line)} (시도 {attempt}/5)")
            await asyncio.sleep(MFC_DELAY_MS / 1000.0)
        return False

    async def _set_onoff_mask_and_verify(self, bits_target: str) -> bool:
        """L0 적용 후 R69로 확인. 에코/반영 지연 고려해 재시도(최대 5회)."""
        for attempt in range(1, 6):
            # L0 적용 (no-reply)
            self._enqueue(self._mk_cmd("SET_ONOFF_MASK", bits_target), None,
                        allow_no_reply=True, tag=f"[L0 {bits_target}]")

            # 장비 반영 시간 대기 (최소 200ms 보장)
            await asyncio.sleep(max(MFC_DELAY_MS, 200) / 1000.0)

            # 검증 (의미없는 빈 라인 방지용으로 최대 2회 읽기)
            now = ""
            for _ in range(2):
                line = await self._send_and_wait_line(self._mk_cmd("READ_MFC_ON_OFF_STATUS"),
                                                    tag="[VERIFY R69]", timeout_ms=MFC_TIMEOUT)
                now = self._parse_r69_bits(line or "")
                if now:
                    break

            if now == bits_target:
                await self._emit_status(f"L0 적용 확인: {bits_target}")
                return True

            await self._emit_status(f"[L0 검증 재시도] now={now or '∅'}, want={bits_target} (시도 {attempt}/5)")
            await asyncio.sleep(MFC_DELAY_MS / 1000.0)
        return False

    async def _valve_move_and_verify(self, origin_cmd: str):
        """VALVE_OPEN/CLOSE → READ_VALVE_POSITION 확인(재시도 시 재전송 포함)."""
        # 명령 전송 (no-reply)
        self._enqueue(self._mk_cmd(origin_cmd), None, allow_no_reply=True, tag=f"[{origin_cmd}]")
        await self._emit_status(f"밸브 이동 대기 ({MFC_DELAY_MS_VALVE/1000:.0f}초)...")
        await asyncio.sleep(MFC_DELAY_MS_VALVE / 1000.0)

        for attempt in range(1, 6):
            line = await self._send_and_wait_line(self._mk_cmd("READ_VALVE_POSITION"),
                                                  tag=f"[VERIFY VALVE {origin_cmd}]",
                                                  timeout_ms=MFC_TIMEOUT)
            pos_ok = self._parse_valve_ok(origin_cmd, line or "")
            if pos_ok:
                await self._emit_status(f"{origin_cmd} 완료")
                await self._emit_confirmed(origin_cmd)
                return
            # 일부 시점에서 재전송
            if attempt in (2, 4):
                self._enqueue(self._mk_cmd(origin_cmd), None, allow_no_reply=True, tag=f"[RE-{origin_cmd}]")
                await self._emit_status(f"{origin_cmd} 재전송 (시도 {attempt}/5)")
                await asyncio.sleep(max(MFC_DELAY_MS, MFC_DELAY_MS_VALVE) / 1000.0)
            else:
                await self._emit_status(f"[{origin_cmd} 검증 재시도] 응답={repr(line)} (시도 {attempt}/5)")
                await asyncio.sleep(MFC_DELAY_MS / 1000.0)

        await self._emit_failed(origin_cmd, "밸브 위치 확인 실패")

    async def _verify_sp1_set(self, hw_val: float, ui_val: float) -> bool:
        """READ_SP1_VALUE 로 HW값 비교(허용오차 MFC_SP1_VERIFY_TOL)."""
        tol = max(float(MFC_SP1_VERIFY_TOL), 1e-9)
        for attempt in range(1, 6):
            line = await self._send_and_wait_line(self._mk_cmd("READ_SP1_VALUE"),
                                                  tag="[VERIFY SP1_SET]", timeout_ms=MFC_TIMEOUT)
            cur_hw = self._parse_pressure_value(line or "")
            if cur_hw is not None:
                cur_hw = round(cur_hw, int(MFC_PRESSURE_DECIMALS))
            ok = (cur_hw is not None) and (abs(cur_hw - hw_val) <= tol)
            if ok:
                await self._emit_status(
                    f"SP1 설정 완료: UI {ui_val:.2f} (장비 {hw_val:.{int(MFC_PRESSURE_DECIMALS)}f})"
                )
                return True
            await self._emit_status(f"[SP1_SET 검증 재시도] 응답={repr(line)} (시도 {attempt}/5)")
            await asyncio.sleep(MFC_DELAY_MS / 1000.0)
        return False

    async def _verify_simple_flag(self, cmd_key: str, expect_mask: str) -> bool:
        """SP1_ON/SP4_ON → READ_SYSTEM_STATUS 확인(Mn...)"""
        # 전송(no-reply)
        self._enqueue(self._mk_cmd(cmd_key), None, allow_no_reply=True, tag=f"[{cmd_key}]")
        for attempt in range(1, 6):
            line = await self._send_and_wait_line(self._mk_cmd("READ_SYSTEM_STATUS"),
                                                  tag=f"[VERIFY {cmd_key}]", timeout_ms=MFC_TIMEOUT)
            s = (line or "").strip().upper()
            ok = bool(s and s.startswith("M") and s[1:2] == expect_mask)
            if ok:
                await self._emit_status(f"{cmd_key} 활성화 확인")
                return True
            await self._emit_status(f"[{cmd_key} 검증 재시도] 응답={repr(line)} (시도 {attempt}/5)")
            await asyncio.sleep(MFC_DELAY_MS / 1000.0)
        return False

    # ---------- 내부: 단위 파서/도우미 ----------
    async def _read_r60_values(self, tag: str = "[READ R60]") -> Optional[list[float]]:
        line = await self._send_and_wait_line(self._mk_cmd("READ_FLOW_ALL"), tag=tag, timeout_ms=MFC_TIMEOUT)
        vals = self._parse_r60_values(line or "")
        return vals

    async def _read_r69_bits(self) -> Optional[str]:
        line = await self._send_and_wait_line(self._mk_cmd("READ_MFC_ON_OFF_STATUS"),
                                              tag="[READ R69]", timeout_ms=MFC_TIMEOUT)
        return self._parse_r69_bits(line or "")

    def _parse_r60_values(self, line: str) -> Optional[list[float]]:
        s = (line or "").strip()
        if not s.startswith("Q0"):
            return None
        nums = re.findall(r'([+\-]?\d+(?:\.\d+)?)', s[2:])
        try:
            return [float(x) for x in nums]
        except Exception:
            return None

    def _parse_q_value_with_prefixes(self, line: str, prefixes: tuple[str, ...]) -> Optional[float]:
        s = (line or "").strip()
        for p in prefixes:
            if s.startswith(p):
                m = re.match(r'^' + re.escape(p) + r'\s*([+\-]?\d+(?:\.\d+)?)$', s)
                if m:
                    try:
                        return float(m.group(1))
                    except Exception:
                        return None
                return None
        return None

    def _parse_r69_bits(self, resp: str) -> str:
        s = (resp or "").strip()
        if s.startswith("L0"):
            payload = s[2:]
        elif s.startswith("L"):
            payload = s[1:]
        else:
            payload = s
        bits = "".join(ch for ch in payload if ch in "01")
        return bits[:4]

    def _parse_valve_ok(self, origin_cmd: str, line: str) -> bool:
        s = (line or "").strip()
        m = re.match(r'^(?:V\s*)?\+?([+\-]?\d+(?:\.\d+)?)$', s)
        pos = float(m.group(1)) if m else None
        if pos is None:
            return False
        return (origin_cmd == "VALVE_CLOSE" and pos < 1.0) or (origin_cmd == "VALVE_OPEN" and pos > 99.0)

    def _parse_pressure_value(self, line: str) -> Optional[float]:
        s = (line or "").strip().upper()
        if not s:
            return None
        m = re.search(r'\+\s*([+\-]?\d+(?:\.\d+)?)', s)
        if m:
            try:
                return float(m.group(1))
            except Exception:
                pass
        nums = re.findall(r'([+\-]?\d+(?:\.\d+)?)', s)
        if not nums:
            return None
        try:
            return float(nums[-1])
        except Exception:
            return None

    def _emit_pressure_from_line_sync(self, line: str):
        val_hw = self._parse_pressure_value(line)
        if val_hw is None:
            return
        ui_val = float(val_hw) / float(MFC_PRESSURE_SCALE)
        fmt = "{:." + str(int(MFC_PRESSURE_DECIMALS)) + "f}"
        text = fmt.format(ui_val)
        # 이벤트 두 형태를 하나로 통합해 전달
        self._ev_nowait(MFCEvent(kind="pressure", value=ui_val, text=text))

    def _monitor_flow(self, channel: int, actual_flow_hw: float):
        target_flow = float(self.last_setpoints.get(channel, 0.0))
        if target_flow < 0.1:
            self.flow_error_counters[channel] = 0
            return
        if abs(actual_flow_hw - target_flow) > (target_flow * float(FLOW_ERROR_TOLERANCE)):
            self.flow_error_counters[channel] += 1
            if self.flow_error_counters[channel] >= int(FLOW_ERROR_MAX_COUNT):
                self._ev_nowait(MFCEvent(kind="status",
                                         message=f"Ch{channel} 유량 불안정! (목표: {target_flow:.2f}, 현재: {actual_flow_hw:.2f})"))
                self.flow_error_counters[channel] = 0
        else:
            self.flow_error_counters[channel] = 0

    # ---------- 내부: 공통 송수신 ----------
    def _enqueue(self, cmd_str: str, on_reply: Optional[Callable[[Optional[str]], None]],
                *, timeout_ms: int = MFC_TIMEOUT, gap_ms: int = MFC_GAP_MS,
                tag: str = "", retries_left: int = 5, allow_no_reply: bool = False):
        if not cmd_str.endswith("\r"):
            cmd_str += "\r"
        self._cmd_q.append(Command(cmd_str, on_reply, timeout_ms, gap_ms, tag, retries_left, allow_no_reply))

        # ★ no-reply 명령의 '에코 라인'은 나중에 도착해도 스킵하도록 등록
        if allow_no_reply:
            no_cr = cmd_str.rstrip("\r")
            self._skip_echos.append(no_cr)
            # 추적 로그를 UI/챗으로도 올림
            self._dbg("MFC", f"[ECHO] no-reply 등록: {no_cr}")

    async def _send_and_wait_line(self, cmd_str: str, *, tag: str, timeout_ms: int, retries: int = 1) -> Optional[str]:
        fut: asyncio.Future[Optional[str]] = asyncio.get_running_loop().create_future()

        def _cb(line: Optional[str]):
            if not fut.done():
                fut.set_result(line)

        self._enqueue(cmd_str, _cb, timeout_ms=timeout_ms, gap_ms=MFC_GAP_MS,
                      tag=tag, retries_left=max(0, int(retries)), allow_no_reply=False)
        try:
            return await asyncio.wait_for(fut, timeout=(timeout_ms / 1000.0) + 2.0)
        except asyncio.TimeoutError:
            return None

    def _mk_cmd(self, key: str, *args, **kwargs) -> str:
        """MFC_COMMANDS 값이 함수/문자열 모두 허용."""
        v = MFC_COMMANDS[key]
        if callable(v):
            return str(v(*args, **kwargs))
        return str(v)

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
            self._ev_nowait(MFCEvent(kind="status", message=f"대기 중 명령 {purged}개 폐기 ({reason})"))
        return purged

    # ---------- 내부: 이벤트/로그 ----------
    async def _emit_status(self, msg: str):
        if self.debug_print:
            print(f"[MFC][status] {msg}")
        await self._event_q.put(MFCEvent(kind="status", message=msg))

    async def _emit_flow(self, gas: str, value_ui: float):
        if self.debug_print:
            print(f"[MFC][flow] {gas}: {value_ui:.2f} sccm")
        await self._event_q.put(MFCEvent(kind="flow", gas=gas, value=value_ui))

    async def _emit_confirmed(self, cmd: str):
        await self._event_q.put(MFCEvent(kind="command_confirmed", cmd=cmd))

    async def _emit_failed(self, cmd: str, why: str):
        await self._event_q.put(MFCEvent(kind="command_failed", cmd=cmd, reason=why))

    def _ev_nowait(self, ev: MFCEvent):
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
            self._dbg("MFC", f"콜백 오류: {e}")

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

    # --- 유틸 ---
    def _is_poll_read_cmd(self, cmd_str: str, tag: str = "") -> bool:
        return (tag or "").startswith("[POLL ")
    
    def _has_pending_non_poll_cmds(self) -> bool:
        if self._inflight and not self._is_poll_read_cmd(self._inflight.cmd_str, self._inflight.tag):
            return True
        for c in self._cmd_q:
            if not self._is_poll_read_cmd(c.cmd_str, c.tag):
                return True
        return False

    def _purge_poll_reads_only(self, cancel_inflight: bool = True, reason: str = "") -> int:
        purged = 0
        if cancel_inflight and self._inflight and self._is_poll_read_cmd(self._inflight.cmd_str, self._inflight.tag):
            self._safe_callback(self._inflight.callback, None)
            self._inflight = None
            purged += 1
            self._dbg("MFC", f"[QUIESCE] 폴링 inflight 취소: {reason}")
        kept = deque()
        while self._cmd_q:
            c = self._cmd_q.popleft()
            if self._is_poll_read_cmd(c.cmd_str, c.tag):
                purged += 1
                continue
            kept.append(c)
        self._cmd_q = kept
        if purged:
            self._ev_nowait(MFCEvent(kind="status", message=f"[QUIESCE] 폴링 읽기 {purged}건 제거: {reason}"))
        return purged

    async def _absorb_late_lines(self, budget_ms: int = 60):
        """짧은 시간 동안 라인 큐에 남은 잔류 에코/ACK를 비운다."""
        deadline = time.monotonic() + (budget_ms / 1000.0)
        while time.monotonic() < deadline:
            drained = False
            try:
                self._line_q.get_nowait()
                drained = True
            except Exception:
                pass
            await asyncio.sleep(0 if drained else 0.005)  # ★ 비었으면 아주 살짝 더 대기

    # 각 장치 클래스 내부
    async def pause_watchdog(self):
        self._want_connected = False
        t = getattr(self, "_watchdog_task", None)
        if t:
            try:
                t.cancel()
                await t
            except Exception:
                pass
            self._watchdog_task = None



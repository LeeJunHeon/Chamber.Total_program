# device/RFPulse_async.py
# -*- coding: utf-8 -*-
"""
rf_pulse.py — asyncio 기반 CESAR AE RS-232 Pulse 컨트롤러

핵심(구 PyQt6 버전과 동등):
  - asyncio.open_connection + 전용 TCP reader loop 기반 비동기 I/O
  - 단일 명령 큐(타임아웃/재시도/인터커맨드 gap)로 송수신 직렬화
  - 워치독(지수 백오프) 자동 재연결
  - ACK(0x06) / NAK(0x15) / AE Bus 프레임 파서
  - exec(쓰기/CSR 확인) / query(읽기/데이터 프레임) 분리 처리
  - 폴링: REPORT_STATUS → FWD → REF 순서, 중첩 금지
  - RF Pulse 시퀀스: HOST→MODE(FWD)→SETP→(FREQ/DUTY)→PULSING=1→RF ON
  - 기존 상수/타임아웃/백오프/파싱/검증 로직 유지
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Deque, Callable, AsyncGenerator, Literal, Tuple
from collections import deque
import asyncio, time, re, socket, contextlib

from typing import Any
from lib import config_common as cfgc

# ===== RF Pulse 파워 모니터링 상수 =====
# FORP: setpoint 대비 허용 오차(%)
FORP_TOLERANCE_PERCENT = 5.0          # 요구사항: 5% 이상 이탈
FORP_CONSECUTIVE_LIMIT = 3            # 3회 연속이면 공정 중지

# REFP: 허용 상한 (실제 운전 조건에 맞게 조정 가능)
REFP_LIMIT_WATTS       = 20.0         # 예: 20W 이상이면 위험으로 판단
REFP_CONSECUTIVE_LIMIT = 3            # 3회 연속이면 공정 중지

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
CMD_SET_PULSING = 27  # AE Bus: set pulsing configuration (data byte: 0~5)

PULSING_TX = {
    0: 0x00,  # Pulsing off
    1: 0x01,  # Internal pulsing
    2: 0x02,  # External pulsing
    3: 0x03,  # External pulsing inverted
    4: 0x04,  # Gated internal pulsing
    5: 0x05,  # Gated internal pulsing inverted
}

CMD_SET_PULSE_FREQ      = 93     # 3 bytes (Hz, LSB first)
CMD_SET_PULSE_DUTY      = 96     # 2 bytes (percent, LSB first)

# Pulsing 리드백(선택)
CMD_REPORT_PULSING      = 177
CMD_REPORT_PULSE_FREQ   = 193
CMD_REPORT_PULSE_DUTY   = 196

CSR_CODES = {
    0:  "Command accepted",
    1:  "Wrong control mode (not HOST)",
    2:  "RF output is ON (cannot change this setting now)",
    4:  "Data out of range / bad value",
    5:  "User Port RF signal OFF / interlock (model dependent)",
    7:  "Active faults exist",
    9:  "Data byte count incorrect",
    19: "Recipe mode active",
    50: "Frequency out of range",
    51: "Duty out of range",
    99: "Command not implemented",
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

def _u24le(buf: bytes, i: int = 0) -> int:
    return buf[i] | (buf[i+1] << 8) | (buf[i+2] << 16)

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
    forward: Optional[float] = None     
    reflected: Optional[float] = None   

# ===== Protocol (바이트 토큰 스트리머) =====
Token = Tuple[Literal["ACK", "NAK", "FRAME"], Optional[bytes]]

# ===== 메인 컨트롤러 =====
class RFPulseAsync:
    def __init__(self, *, cfg: Any = None, debug_print: Optional[bool] = None):
        """
        cfg: lib.config_ch2 같은 모듈을 넣으면 해당 값을 우선 사용.
             (UI에서 cfg 값을 바꾸면, rf_pulse가 그 값을 읽도록 만드는 핵심 구조)
        debug_print: 강제 지정 시 cfg보다 우선
        """
        self._cfg = cfg if cfg is not None else cfgc

        def _cfg_get(name: str, default=None):
            if hasattr(self._cfg, name):
                return getattr(self._cfg, name)
            if hasattr(cfgc, name):
                return getattr(cfgc, name)
            return default

        self._cfg_get = _cfg_get

        # debug_print는 캐시 성격 → reload_runtime_cfg로 갱신 가능하게
        if debug_print is None:
            self.debug_print = self._cfg_bool("DEBUG_PRINT", False)
        else:
            self.debug_print = bool(debug_print)

        # TCP Streams
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._connected: bool = False
        self._ever_connected: bool = False

        # 명령 큐/인플라이트
        self._cmd_q: Deque[RfCommand] = deque()
        self._inflight: Optional[RfCommand] = None
        self._last_send_mono: float = 0.0  # 인터커맨드 간격 계산용

        # 토큰/이벤트 큐
        self._tok_q: asyncio.Queue[Token] = asyncio.Queue(maxsize=2048)
        self._event_q: asyncio.Queue[RFPulseEvent] = asyncio.Queue(maxsize=512)

        # Tasks
        self._watchdog_task: Optional[asyncio.Task] = None
        self._cmd_worker_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._want_connected: bool = False

        # start/resume_watchdog 동시 호출 방지용 락
        self._start_lock = asyncio.Lock()

        # 재연결 상태(백오프는 cfg 기반 초기화)
        self._reconnect_backoff_ms = self._cfg_int("RFPULSE_RECONNECT_BACKOFF_START_MS", 2000)
        self._just_reopened: bool = False

        # 런타임 상태
        self.addr = self._cfg_int("RFPULSE_ADDR", 1)
        self._closing: bool = False
        self._stop_requested: bool = False

        # 폴링/전력 캐시
        self._poll_busy: bool = False
        self._last_forward_w: Optional[float] = None
        self._last_reflected_w: Optional[float] = None
        self._last_status: Optional[RfStatus] = None

        # 파워 모니터링용 상태
        self._target_setpoint_w: float = 0.0
        self._forp_out_of_range_count: int = 0
        self._refp_over_limit_count: int = 0
        # ★ 신규: REFP 경고 디바운스 (한 번 경고하면 정상 복귀 전까지 재발송 안 함)
        self._refp_warn_active: bool = False

    # ---------- cfg helper ----------
    def _cfg_int(self, name: str, default: int) -> int:
        v = self._cfg_get(name, default)
        try:
            return int(float(v))
        except Exception:
            return int(default)

    def _cfg_float(self, name: str, default: float) -> float:
        v = self._cfg_get(name, default)
        try:
            return float(v)
        except Exception:
            return float(default)

    def _cfg_bool(self, name: str, default: bool = False) -> bool:
        v = self._cfg_get(name, default)
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        s = str(v).strip().lower()
        if s in ("1", "true", "t", "yes", "y", "on"):
            return True
        if s in ("0", "false", "f", "no", "n", "off", ""):
            return False
        return bool(default)

    def reload_runtime_cfg(self) -> None:
        """
        UI에서 cfg를 바꾼 뒤 즉시 반영이 필요한 값들만 갱신.
        (예: debug_print/addr/백오프 시작값)
        """
        self.debug_print = self._cfg_bool("DEBUG_PRINT", self.debug_print)
        self.addr = self._cfg_int("RFPULSE_ADDR", int(self.addr or 1))
        self._reconnect_backoff_ms = self._cfg_int("RFPULSE_RECONNECT_BACKOFF_START_MS", int(self._reconnect_backoff_ms or 2000))

    # ---------- 공용 API ----------
    async def start(self):
        # ★ 여러 coroutine 에서 동시에 start()가 들어와도
        #    한 번만 워치독/워커를 생성하도록 보호
        async with self._start_lock:
            # 1) 죽어버린 태스크는 정리
            if self._watchdog_task and self._watchdog_task.done():
                self._watchdog_task = None
            if self._cmd_worker_task and self._cmd_worker_task.done():
                self._cmd_worker_task = None

            # 2) 태스크가 아직 살아 있으면 새로 만들 필요 없이
            #    단순히 다시 연결을 시도하도록 플래그만 올려준다.
            if self._watchdog_task and self._cmd_worker_task:
                self._closing = False
                self._want_connected = True
                return

            # 3) ✅ 필요한 것만 생성 (덮어쓰기 금지 → orphan task 방지)
            self._closing = False
            self._want_connected = True
            loop = asyncio.get_running_loop()

            if not self._watchdog_task:
                self._watchdog_task = loop.create_task(self._watchdog_loop(), name="RFPWatchdog")

            if not self._cmd_worker_task:
                self._cmd_worker_task = loop.create_task(self._cmd_worker_loop(), name="RFP-CmdWorker")

    async def cleanup(self):
        self._closing = True
        self._want_connected = False
        self.set_process_status(False)  # 폴링만 중지, SAFE 시퀀스는 여기서 안 함
        await asyncio.sleep(0.2)

        await self._cancel_task("_poll_task")
        await self._cancel_task("_cmd_worker_task")
        await self._cancel_task("_watchdog_task")

        self._purge_pending("shutdown")

        # TCP 종료
        if self._reader_task:
            self._reader_task.cancel()
            with contextlib.suppress(Exception):
                await self._reader_task
            self._reader_task = None

        if self._writer:
            try:
                self._writer.close()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(self._writer.wait_closed(), timeout=0.8)
            except Exception:
                transport = getattr(self._writer, "transport", None)
                if transport:
                    with contextlib.suppress(Exception):
                        transport.abort()

        # 남아 있을 수 있는 ACK/NAK/FRAME 토큰 제거
        while True:
            try:
                self._tok_q.get_nowait()
            except asyncio.QueueEmpty:
                break

        self._reader = None
        self._writer = None
        self._connected = False
        self._just_reopened = False
        self._last_forward_w = None
        self._last_reflected_w = None
        self._last_status = None

        await self._emit_status("RFPulse 연결 종료됨")

    async def events(self) -> AsyncGenerator[RFPulseEvent, None]:
        """상위(UI/브리지)에서 구독하는 이벤트 스트림."""
        while True:
            ev = await self._event_q.get()
            yield ev

    # ---------- 고수준 시퀀스 ----------
    async def start_pulse_process(self, target_w: float, freq_hz: Optional[int] = None, duty_percent: Optional[int] = None):
        """
        HOST(14,02) → MODE(FWD=6) → SETP → (FREQ/DUTY) → PULSING → RF ON
        실패 시 command_failed 이벤트로만 통지(=호환용 FAILED target_reached 삭제)
        성공 시 target_reached(OK) 1회만 발행
        """
        self._stop_requested = False
        self.set_process_status(False)

        # 모니터링용 setpoint/카운터 초기화
        self._target_setpoint_w = float(target_w or 0.0)
        self._forp_out_of_range_count = 0
        self._refp_over_limit_count = 0
        self._refp_warn_active = False

        async def fail(why: str):
            await self._emit_failed("START_SEQUENCE", why)
            return False

        ack_ms = self._cfg_int("ACK_TIMEOUT_MS", 2000)

        # HOST
        # HOST 모드 확인 → 필요할 때만 전환
        mode_data = await self._query_and_data(155, b"", tag="[READ CTRL MODE]")
        current_mode = mode_data[0] if mode_data else None

        if current_mode is None:
            return await fail("HOST 모드 확인 실패 (READ_ACTIVE_CTRL 응답 없음)")

        if current_mode != 2:
            ok, csr_bytes = await self._exec_and_csr(CMD_SET_ACTIVE_CTRL, b"\x02", tag="[START HOST]")
            if not ok:
                csr = csr_bytes[0] if csr_bytes else None
                return await fail(f"HOST 전환 실패 (CSR={csr}, 이전 모드={current_mode})")
            await self._emit_status(f"HOST 모드 전환 완료 (이전 모드={current_mode})")
        else:
            await self._emit_status("이미 HOST 모드 → SET_ACTIVE_CTRL 생략")

        # RF OFF로 출력 상태 정리 (이전 공정 비정상 종료 대비)
        ok, _ = await self._exec_and_csr(CMD_RF_OFF, b"", tag="[START PRE RF OFF]", timeout_ms=max(ack_ms, 2500))
        if not ok:
            return await fail("RF OFF(사전) 실패")

        await asyncio.sleep(0.2)  # 릴레이/출력 안정화 여유

        # MODE FWD
        ok, _ = await self._exec_and_csr(CMD_SET_CTRL_MODE, bytes([MODE_SET["fwd"]]), tag="[START MODE FWD]")
        if not ok:
            return await fail("MODE=FWD 실패")

        # SETPOINT
        sp = int(round(float(target_w)))
        ok, _ = await self._exec_and_csr(
            CMD_SET_SETPOINT,
            bytes([sp & 0xFF, (sp >> 8) & 0xFF]),
            tag=f"[START SETP {sp}W]",
        )
        if not ok:
            return await fail("SETP 실패")

        # FREQ
        if freq_hz is not None:
            hz = int(freq_hz)
            data_f = bytes([hz & 0xFF, (hz >> 8) & 0xFF, (hz >> 16) & 0xFF])
            ok, _ = await self._exec_and_csr(CMD_SET_PULSE_FREQ, data_f, tag="[START FREQ]")
            if not ok:
                return await fail("PULSE FREQ 실패")

        # DUTY
        if duty_percent is not None:
            v = int(duty_percent) & 0xFFFF
            data_d = bytes([v & 0xFF, (v >> 8) & 0xFF])
            ok, _ = await self._exec_and_csr(CMD_SET_PULSE_DUTY, data_d, tag="[START DUTY]")
            if not ok:
                return await fail("PULSE DUTY 실패")

        # PULSING (cfg로 모드 선택 가능)
        pulse_mode = self._cfg_int("RFPULSE_PULSE_MODE", 1)
        if pulse_mode not in PULSING_TX:
            return await fail(f"PULSING 모드 범위 오류: {pulse_mode} (허용: {sorted(PULSING_TX.keys())})")

        ok, _ = await self._exec_and_csr(
            CMD_SET_PULSING,
            bytes([PULSING_TX[pulse_mode]]),
            tag=f"[START PULSING {pulse_mode}]",
        )
        if not ok:
            return await fail("PULSING 설정 실패")

        # RF ON
        ok, _ = await self._exec_and_csr(CMD_RF_ON, b"", tag="[START RF ON]", timeout_ms=max(ack_ms, 2500))
        if not ok:
            return await fail("RF ON 실패")

        # 폴링 시작
        start_delay_ms = self._cfg_int("POLL_START_DELAY_AFTER_RF_ON_MS", 800)
        await asyncio.sleep(start_delay_ms / 1000.0)

        self.set_process_status(True)

        # 성공 알림(1회만)
        await self._event_q.put(RFPulseEvent(kind="target_reached", message="OK"))
        return True
    
    async def read_actual_pulse_params(self) -> Optional[dict]:
        """cmd 193(freq) + 196(duty) 읽고 off_time 역산. 실패 시 None.
        반환: {"freq_khz": float, "duty_pct": int, "off_time_us": float|None}
        """
        f_data = await self._query_and_data(CMD_REPORT_PULSE_FREQ, b"", tag="[READ PULSE FREQ]")
        d_data = await self._query_and_data(CMD_REPORT_PULSE_DUTY, b"", tag="[READ PULSE DUTY]")
        if f_data is None or len(f_data) < 3:
            return None
        if d_data is None or len(d_data) < 2:
            return None
        freq_hz  = _u24le(f_data, 0)
        duty_pct = _u16le(d_data, 0)
        freq_khz = round(freq_hz / 1000.0, 3)
        if freq_hz > 0:
            period_us   = 1_000_000.0 / freq_hz
            off_time_us = round(period_us * (1.0 - duty_pct / 100.0), 2)
        else:
            off_time_us = None
        return {"freq_khz": freq_khz, "duty_pct": duty_pct, "off_time_us": off_time_us}
    
    async def set_reference_power(self, target_w: float, *, pause_polling: bool = True) -> bool:
        """
        공정 중(RF 출력 ON 상태) setpoint(Command 8)만 변경하는 API.
        - CSV 리스트 공정에서 "특정 시간에 power setpoint 변경"할 때 사용
        - 성공/실패를 bool로 반환 (상위 ProcessController 콜백에서 사용하기 좋게)
        """
        # 1) 입력 정규화
        try:
            sp = int(round(float(target_w)))
        except Exception:
            await self._emit_failed("SETP_CHANGE", f"invalid power: {target_w!r}")
            return False

        # 2) AE Bus setpoint는 2바이트(0~65535) 범위 밖이면 애초에 전송 의미가 없음
        #    (실제 허용 상한은 'nominal power'에 의해 더 작을 수 있고,
        #     그 경우 장비가 CSR=4(Data out of range)로 거부할 수 있음)
        if not (0 <= sp <= 65535):
            await self._emit_failed("SETP_CHANGE", f"out of range: {sp} (0..65535)")
            return False

        # 3) (선택) 폴링이 setpoint 변경 타이밍을 늦출 수 있으니 잠깐 끄고 다시 켬
        #    - start_pulse_process도 시퀀스 중에는 폴링을 껐다가 켜는 구조라(호환성 OK)
        was_polling = bool(self._poll_task and not self._poll_task.done())
        if pause_polling and was_polling:
            self.set_process_status(False)

        # 4) START 태그가 아니어야 함!
        #    - _cmd_worker_loop에서 CSR=2 자동복구(RF_OFF)는 [START...]에만 적용됨 :contentReference[oaicite:7]{index=7}
        ok, csr_bytes = await self._exec_and_csr(
            CMD_SET_SETPOINT,
            bytes([sp & 0xFF, (sp >> 8) & 0xFF]),
            tag=f"[RUN SETP {sp}W]",
        )

        if ok:
            # 5) 모니터링 기준도 함께 갱신 (중요)
            #    - _poll_loop의 FORP/REFP 감시가 _target_setpoint_w 기반 :contentReference[oaicite:8]{index=8}
            self._target_setpoint_w = float(sp)
            self._forp_out_of_range_count = 0
            self._refp_over_limit_count = 0
            await self._emit_status(f"SETP 변경 OK: {sp}W")
        else:
            csr = csr_bytes[0] if csr_bytes else None
            if csr is not None:
                await self._emit_failed("SETP_CHANGE", f"CSR {csr} ({CSR_CODES.get(csr, 'Unknown')})")
            else:
                await self._emit_failed("SETP_CHANGE", "no reply / timeout")

        if pause_polling and was_polling:
            self.set_process_status(True)

        return bool(ok)

    def set_process_status(self, should_poll: bool):
        """
        True → 폴링 시작
        False → 폴링 중지 + 큐 정리 (SAFE 시퀀스는 여기서 수행하지 않음)
        """
        if should_poll:
            if self._poll_task is None or self._poll_task.done():
                self._poll_task = self._spawn(self._poll_loop())
            return

        # 폴링 중지
        if self._poll_task:
            self._poll_task.cancel()
            self._poll_task = None
        self._poll_busy = False

        # ✅ 이미 큐도 없고 inflight도 없으면, 'polling off' purge/log 스킵
        need_purge = (self._inflight is not None) or bool(self._cmd_q)
        if need_purge:
            self._purge_pending("polling off")

    def stop_process(self):
            """외부 stop: 폴링 off → RF OFF → power_off_finished 이벤트."""
            # 공정 중단 플래그만 세팅
            self._stop_requested = True

            # 폴링만 먼저 정지 (SAFE는 수행하지 않음)
            self.set_process_status(False)

            async def _notify_off():
                # ProcessController에서 RFPULSE_OFF 토큰으로 처리
                await self._event_q.put(RFPulseEvent(kind="power_off_finished"))

            # 조건 없이 RF OFF 한 번 전송
            self._enqueue_exec(
                CMD_RF_OFF,
                b"",
                tag="[RF OFF]",
                allow_no_reply=True,        # 응답 없어도 콜백 호출
                allow_when_closing=True,
                callback=lambda _b: self._spawn(_notify_off()),
            )

            # 필요하다면 여기서 _want_connected 를 False로 둘 수도 있음
            # (지금 구조를 유지하고 싶으면 아래 줄은 그냥 그대로 두면 됨)
            self._want_connected = False

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
                                                forward=self._last_forward_w,
                                                reflected=self._last_reflected_w))

    # ---------- 내부: 연결/워치독 ----------
    def _resolve_endpoint(self) -> tuple[str, int]:
        host = getattr(self, "_override_host", None) or self._cfg_get("RFPULSE_TCP_HOST", None)
        port = getattr(self, "_override_port", None) or self._cfg_get("RFPULSE_TCP_PORT", None)

        if host is None or port is None:
            raise RuntimeError("RFPULSE_TCP_HOST / RFPULSE_TCP_PORT 가 config에 정의되어 있어야 합니다.")

        return str(host), int(port)

    def _on_tcp_disconnected(self):
        self._connected = False
        if self._reader_task:
            self._reader_task.cancel()
        self._reader_task = None
        if self._writer:
            with contextlib.suppress(Exception):
                self._writer.close()
        self._reader = None
        self._writer = None
        # 토큰 큐는 그대로(명령 워커가 타임아웃 처리)
        # 인플라이트 복구/취소
        if self._inflight is not None:
            cmd = self._inflight
            self._inflight = None
            if cmd.retries_left > 0:
                cmd.retries_left -= 1
                self._cmd_q.appendleft(cmd)
            else:
                self._safe_callback(cmd.callback, None)

    async def _watchdog_loop(self):
        backoff = self._cfg_int("RFPULSE_RECONNECT_BACKOFF_START_MS", 2000)
        while True:
            if not self._want_connected:
                await asyncio.sleep(0.05)
                continue

            if self._connected:
                wd_ms = self._cfg_int("RFPULSE_WATCHDOG_INTERVAL_MS", 3000)
                await asyncio.sleep(wd_ms / 1000.0)
                continue

            if self._ever_connected:
                await self._emit_status(f"재연결 시도... ({backoff} ms)")
                await asyncio.sleep(backoff / 1000.0)

            if not self._want_connected:
                continue

            try:
                host, port = self._resolve_endpoint()
                connect_timeout_s = self._cfg_float("RFPULSE_CONNECT_TIMEOUT_S", 1.5)

                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=max(0.3, float(connect_timeout_s)),
                )
                self._reader, self._writer = reader, writer
                self._connected = True
                self._ever_connected = True
                backoff = self._cfg_int("RFPULSE_RECONNECT_BACKOFF_START_MS", 2000)

                # TCP keepalive (가능하면)
                try:
                    sock = writer.get_extra_info("socket")
                    if sock is not None:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                except Exception:
                    pass

                # 리더 태스크 기동
                if self._reader_task and not self._reader_task.done():
                    self._reader_task.cancel()
                    with contextlib.suppress(Exception):
                        await self._reader_task

                self._reader_task = asyncio.create_task(self._tcp_reader_loop(), name="RFP-TcpReader")
                self._just_reopened = True
                await self._emit_status(f"{host}:{port} 연결 성공 (TCP)")

            except Exception as e:
                # host/port 재확인 로그
                try:
                    host, port = self._resolve_endpoint()
                    await self._emit_status(f"{host}:{port} 연결 실패: {type(e).__name__}: {e!r}")
                except Exception:
                    await self._emit_status(f"RFPulse 연결 실패: {type(e).__name__}: {e!r}")

                backoff = min(backoff * 2, self._cfg_int("RFPULSE_RECONNECT_BACKOFF_MAX_MS", 30_000))

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
            if not (self._connected and self._writer):
                await asyncio.sleep(0.05)
                continue

            cmd = self._cmd_q.popleft()
            self._inflight = cmd

            # 최소 인터커맨드 간격 보장 (cmd.gap_ms 사용)
            now = time.monotonic()
            gap_need = (cmd.gap_ms / 1000.0) - (now - self._last_send_mono)
            if gap_need > 0:
                await asyncio.sleep(gap_need)

            # exec일 때만 짧게 토큰 비우기
            if cmd.kind == "exec":
                deadline = time.monotonic() + 0.15
                while time.monotonic() < deadline:
                    try:
                        self._tok_q.get_nowait()
                    except asyncio.QueueEmpty:
                        break

            # 전송 전 상태 확인
            if self._closing or not (self._connected and self._writer):
                self._inflight = None
                await asyncio.sleep(0)
                continue

            pkt = _build_packet(self.addr, cmd.cmd, cmd.data)
            try:
                # Raw data log (debug)
                asyncio.create_task(self._emit_status(
                    f"[RFP][RAW][TX] addr={self.addr} cmd={self._cmd_label(cmd.cmd)} "
                    f"data={' '.join(f'{x:02X}' for x in (cmd.data or b''))} "
                    f"raw={' '.join(f'{x:02X}' for x in pkt)} tag={cmd.tag or ''}"
                ))

                self._writer.write(pkt)

                drain_timeout_s = self._cfg_float("RFPULSE_DRAIN_TIMEOUT_S", 2.0)
                await asyncio.wait_for(self._writer.drain(), timeout=max(0.1, float(drain_timeout_s)))

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
                self._on_tcp_disconnected()
                continue

            # no-reply
            if cmd.allow_no_reply and cmd.kind == "exec":
                self._safe_callback(cmd.callback, b"")
                self._inflight = None
                await asyncio.sleep(cmd.gap_ms / 1000.0)
                continue

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
                self._on_tcp_disconnected()

            # CSR 기반 자동 복구
            if (not ok) and (cmd.kind == "exec") and (fail_reason is None) and result and (len(result) >= 1):
                csr = result[0]
                fail_reason = f"csr={csr}"

                ack_ms = self._cfg_int("ACK_TIMEOUT_MS", 2000)
                cmd_gap_ms = self._cfg_int("CMD_GAP_MS", 1500)

                # CSR=1: HOST가 아니어서 거부 → HOST 재설정 후 재시도
                if csr == 1 and (cmd.cmd != CMD_SET_ACTIVE_CTRL) and (cmd.retries_left > 0) and (not self._closing):
                    cmd.retries_left -= 1

                    self._cmd_q.appendleft(cmd)
                    self._cmd_q.appendleft(RfCommand(
                        kind="exec",
                        cmd=CMD_SET_ACTIVE_CTRL,
                        data=b"\x02",
                        timeout_ms=ack_ms,
                        callback=(lambda _b: None),
                        tag="[AUTO HOST]",
                        gap_ms=max(200, cmd_gap_ms),
                        retries_left=1,
                        allow_no_reply=False,
                        allow_when_closing=False,
                    ))

                    self._inflight = None
                    await asyncio.sleep(0)
                    continue

                is_start = (cmd.tag or "").startswith("[START")

                # CSR=2: RF output ON → START 시퀀스에서만 RF_OFF 후 재시도
                if csr == 2 and is_start and (cmd.cmd != CMD_RF_OFF) and (cmd.retries_left > 0) and (not self._closing):
                    cmd.retries_left -= 1

                    self._cmd_q.appendleft(cmd)
                    self._cmd_q.appendleft(RfCommand(
                        kind="exec",
                        cmd=CMD_RF_OFF,
                        data=b"",
                        timeout_ms=ack_ms,
                        callback=(lambda _b: None),
                        tag="[AUTO RF_OFF]",
                        gap_ms=max(200, cmd_gap_ms),
                        retries_left=1,
                        allow_no_reply=False,
                        allow_when_closing=False,
                    ))

                    self._inflight = None
                    await asyncio.sleep(0)
                    continue

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
                    backoff_ms = max(150, cmd.gap_ms)
                    if isinstance(fail_reason, str) and fail_reason.startswith("csr=5"):
                        backoff_ms = max(int(cmd.gap_ms * 1.5), 1200)
                    await asyncio.sleep(backoff_ms / 1000.0)
                else:
                    self._safe_callback(cmd.callback, result)
                    self._inflight = None
                    await asyncio.sleep(cmd.gap_ms / 1000.0)

    async def _tcp_reader_loop(self):
        assert self._reader is not None
        buf = bytearray()
        RX_MAX = 64 * 1024
        try:
            while self._connected and self._reader:
                chunk = await self._reader.read(256)
                if not chunk:
                    break
                buf.extend(chunk)
                if len(buf) > RX_MAX:
                    del buf[:-RX_MAX]

                # === AE Bus 토큰화: ACK(0x06)/NAK(0x15)/FRAME ===
                while True:
                    if not buf:
                        break

                    # 1) ACK/NAK 단일 토큰
                    if buf[0] == 0x06:
                        del buf[:1]
                        # ★ 받을 때 1줄 (ACK) - debug
                        asyncio.create_task(self._emit_status("[RFP][RAW][RX] ACK(0x06)"))
                        self._on_token(("ACK", None))
                        continue
                    if buf[0] == 0x15:
                        del buf[:1]
                        # ★ 받을 때 1줄 (NAK) - debug
                        asyncio.create_task(self._emit_status("[RFP][RAW][RX] NAK(0x15)"))
                        self._on_token(("NAK", None))
                        continue

                    # 2) 프레임 헤더 점검
                    if len(buf) < 2:
                        break
                    hdr = buf[0]
                    length_bits = hdr & 0x07

                    if length_bits == 7:
                        if len(buf) < 3:
                            break
                        data_len = buf[2]
                        total = 1 + 1 + 1 + data_len + 1
                        if len(buf) < total:
                            break
                        pkt = bytes(buf[:total])
                        del buf[:total]
                    else:
                        data_len = length_bits
                        total = 1 + 1 + data_len + 1
                        if len(buf) < total:
                            break
                        pkt = bytes(buf[:total])
                        del buf[:total]

                    # 3) XOR 체크섬 검증
                    cs = 0
                    for x in pkt[:-1]:
                        cs ^= x
                    if (cs ^ pkt[-1]) != 0:
                        # 체크섬 불일치 log - debug
                        asyncio.create_task(self._emit_status(
                            f"[RFP][RAW][RX] FRAME(cs_bad) raw={' '.join(f'{x:02X}' for x in pkt)}"
                        ))

                        # 체크섬 불일치 → 폐기
                        continue

                    # 4) 프레임 토큰 방출 직전: ★ 받을 때 1줄 (FRAME RAW) - debug
                    hdr = pkt[0]
                    rx_addr = (hdr >> 3) & 0x1F
                    rx_cmd  = pkt[1]
                    length_bits = hdr & 0x07
                    data_len = pkt[2] if length_bits == 7 else length_bits
                    decoded_suffix = self._decode_frame_suffix(rx_cmd, pkt)
                    asyncio.create_task(self._emit_status(
                        f"[RFP][RAW][RX] FRAME addr={rx_addr} cmd={self._cmd_label(rx_cmd)} "
                        f"len={data_len} raw={' '.join(f'{x:02X}' for x in pkt)}{decoded_suffix}"
                    ))

                    # 4) 프레임 토큰 방출
                    self._on_token(("FRAME", pkt))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._dbg("RFP", f"리더 루프 예외: {e!r}")
        finally:
            self._on_tcp_disconnected()

    # ---------- 내부: exec/query 대기 ----------
    async def _await_exec_csr(self, cmd: RfCommand) -> Tuple[bool, Optional[bytes]]:
        """ACK phase → CSR 프레임(동일 cmd, 동일 addr) 확보 → CSR=0 확인."""
        start = time.monotonic()

        ack_ms = self._cfg_int("ACK_TIMEOUT_MS", 2000)
        ack_deadline = start + min(ack_ms, cmd.timeout_ms) / 1000.0
        end_deadline = start + cmd.timeout_ms / 1000.0

        csr_bytes: Optional[bytes] = None

        # 1) ACK phase
        while time.monotonic() < ack_deadline:
            remain = ack_deadline - time.monotonic()
            tok = await self._get_token(remain)
            if tok is None:
                break
            kind, payload = tok
            if kind == "NAK":
                return False, None
            if kind == "FRAME" and payload and self._frame_match(payload, cmd.cmd):
                csr_bytes = self._extract_data(payload)
                break

        # 2) CSR 프레임 대기
        while (csr_bytes is None) and (time.monotonic() < end_deadline):
            remain = end_deadline - time.monotonic()
            tok = await self._get_token(remain)
            if tok is None:
                break
            kind, payload = tok
            if kind == "NAK":
                return False, None
            if kind == "FRAME" and payload and self._frame_match(payload, cmd.cmd):
                csr_bytes = self._extract_data(payload)
                break

        if (not csr_bytes) or (len(csr_bytes) < 1):
            return False, None

        csr = csr_bytes[0]
        if csr != 0:
            await self._emit_status(
                f"CSR {csr} ({CSR_CODES.get(csr, 'Unknown')}) for {self._cmd_label(cmd.cmd)}"
            )
            return False, csr_bytes

        # ✅ 여기서 target_reached를 쏘지 않는다(중복/부작용 제거)
        return True, csr_bytes


    async def _await_query_data(self, cmd: RfCommand) -> Tuple[bool, Optional[bytes]]:
        """ACK phase(짧게) → 데이터 프레임(동일 cmd, 동일 addr) 확보."""
        start = time.monotonic()

        ack_ms = self._cfg_int("ACK_TIMEOUT_MS", 2000)
        ack_deadline = start + min(ack_ms, (2 * cmd.timeout_ms) // 3) / 1000.0
        end_deadline = start + cmd.timeout_ms / 1000.0

        data_bytes: Optional[bytes] = None

        # 빠른 경로
        while time.monotonic() < ack_deadline:
            remain = ack_deadline - time.monotonic()
            tok = await self._get_token(remain)
            if tok is None:
                break
            kind, payload = tok
            if kind == "FRAME" and payload and self._frame_match(payload, cmd.cmd):
                data_bytes = self._extract_data(payload)
                break
            if kind == "NAK":
                return False, None

        # 남은 시간 동안 대기
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
                    await asyncio.sleep(0.05)
                    continue

                self._poll_busy = True
                try:
                    poll_q_ms = self._cfg_int("POLL_QUERY_TIMEOUT_MS", self._cfg_int("QUERY_TIMEOUT_MS", 4500))
                    poll_interval_ms = self._cfg_int("POLL_INTERVAL_MS", 1000)

                    st = await self._read_status()
                    if st:
                        await self._emit_status(f"STATUS {self._status_summary_str(st)}")

                    f = await self._query_and_data(CMD_REPORT_FORWARD, b"", tag="[POLL FWD]", timeout_ms=poll_q_ms)
                    r = await self._query_and_data(CMD_REPORT_REFLECTED, b"", tag="[POLL REF]", timeout_ms=poll_q_ms)

                    if f is not None:
                        self._last_forward_w = float(_u16le(f, 0) if len(f) >= 2 else 0.0)
                    if r is not None:
                        self._last_reflected_w = float(_u16le(r, 0) if len(r) >= 2 else 0.0)

                    # FORP/REFP 모니터링(임계값 cfg로)
                    # ★ 수정: rf_on(=STATUS.rf_output_on) 가드 제거
                    #   - 이전 동작: 장비가 fault 없이 자체적으로 RF OFF되면 STATUS.on=0이 되고,
                    #     이 가드 때문에 FORP=0인데도 검출 로직 자체가 스킵되어 공정이 무한 진행됨.
                    #   - 의도적인 OFF(stop_requested) / target=0은 바깥쪽 if에서 이미 차단됨.
                    if (
                        self._target_setpoint_w > 0.0
                        and self._last_forward_w is not None
                        and self._last_reflected_w is not None
                        and not self._stop_requested
                    ):
                        forp_tol_pct = self._cfg_float("RFPULSE_FORP_TOLERANCE_PERCENT", 5.0)
                        forp_limit_n = self._cfg_int("RFPULSE_FORP_CONSECUTIVE_LIMIT", 3)

                        refp_limit_w = self._cfg_float("RFPULSE_REFP_LIMIT_WATTS", 20.0)
                        refp_limit_n = self._cfg_int("RFPULSE_REFP_CONSECUTIVE_LIMIT", 3)

                        # ★ 신규: REFP 경고 임계값 (알림 전용, 공정은 계속 진행)
                        # 0.0 이면 기능 비활성 (CH1에서 키를 안 두면 자동으로 OFF)
                        refp_warn_w = self._cfg_float("RFPULSE_REFP_WARN_WATTS", 0.0)

                        # 1) FORP
                        tol = self._target_setpoint_w * (forp_tol_pct / 100.0)
                        diff = abs(self._last_forward_w - self._target_setpoint_w)

                        if diff >= tol:
                            self._forp_out_of_range_count += 1
                        else:
                            self._forp_out_of_range_count = 0

                        if self._forp_out_of_range_count >= forp_limit_n:
                            await self._emit_status(
                                "FORP setpoint 이탈: "
                                f"meas={self._last_forward_w:.1f}W, "
                                f"target={self._target_setpoint_w:.1f}W, "
                                f"허용오차=±{forp_tol_pct:.1f}% "
                                f"({self._forp_out_of_range_count}회 연속)"
                            )
                            await self._emit_failed(
                                "FORP_MONITOR",
                                f"FORP가 setpoint에서 {forp_tol_pct:.1f}% 이상 이탈({forp_limit_n}회 연속)",
                            )
                            self._forp_out_of_range_count = 0

                        # 2) REFP
                        if self._last_reflected_w >= refp_limit_w:
                            self._refp_over_limit_count += 1
                        else:
                            self._refp_over_limit_count = 0

                        if self._refp_over_limit_count >= refp_limit_n:
                            await self._emit_status(
                                "REFP 과다 반사: "
                                f"meas={self._last_reflected_w:.1f}W, "
                                f"limit={refp_limit_w:.1f}W "
                                f"({self._refp_over_limit_count}회 연속)"
                            )
                            await self._emit_failed(
                                "REFP_MONITOR",
                                f"REFP가 {refp_limit_w:.1f}W 이상 ({refp_limit_n}회 연속)",
                            )
                            self._refp_over_limit_count = 0

                        # ★ 신규: REFP 경고 (즉시 알림, 공정 중단 X)
                        if refp_warn_w > 0.0:
                            if self._last_reflected_w >= refp_warn_w:
                                if not self._refp_warn_active:
                                    await self._emit_status(
                                        f"⚠ REFP_WARN: REFP={self._last_reflected_w:.1f}W "
                                        f"≥ {refp_warn_w:.1f}W (공정은 계속 진행)"
                                    )
                                    self._refp_warn_active = True
                            else:
                                if self._refp_warn_active:
                                    await self._emit_status(
                                        f"REFP 정상 복귀: REFP={self._last_reflected_w:.1f}W "
                                        f"< {refp_warn_w:.1f}W"
                                    )
                                    self._refp_warn_active = False

                    if (self._last_forward_w is not None) and (self._last_reflected_w is not None):
                        await self._event_q.put(RFPulseEvent(
                            kind="power",
                            forward=self._last_forward_w,
                            reflected=self._last_reflected_w,
                        ))

                finally:
                    self._poll_busy = False

                await asyncio.sleep(poll_interval_ms / 1000.0)

        except asyncio.CancelledError:
            self._poll_busy = False

    # ---------- 내부: 쿼리/exec 유틸 ----------
    async def _read_status(self) -> Optional[RfStatus]:
        # 폴링용 쿼리 타임아웃(ms): config에 없으면 QUERY_TIMEOUT_MS(기본 4500ms)를 fallback
        poll_q_ms = self._cfg_int("POLL_QUERY_TIMEOUT_MS", self._cfg_int("QUERY_TIMEOUT_MS", 4500))

        data = await self._query_and_data(
            CMD_REPORT_STATUS,
            b"",
            tag="[POLL WAKE]",
            timeout_ms=poll_q_ms,
        )

        st = self._parse_status_0xA2(data)
        if st:
            self._last_status = st
            await self._event_q.put(RFPulseEvent(kind="rf_status", rfstatus=st))
            self._validate_status(st)
        return st

    async def _exec_and_csr(self, cmd: int, data: bytes, *, tag: str = "", timeout_ms: Optional[int] = None) -> Tuple[bool, Optional[bytes]]:
        fut: asyncio.Future[Optional[bytes]] = asyncio.get_running_loop().create_future()

        ack_ms = self._cfg_int("ACK_TIMEOUT_MS", 2000)
        eff_timeout_ms = int(timeout_ms or ack_ms)

        self._enqueue_exec(
            cmd, data,
            tag=tag,
            timeout_ms=eff_timeout_ms,
            callback=lambda b: (not fut.done()) and fut.set_result(b),
        )

        try:
            res = await asyncio.wait_for(fut, timeout=eff_timeout_ms / 1000.0 + 2.0)
        except asyncio.TimeoutError:
            return False, None

        if not res or len(res) < 1:
            return False, None
        return (res[0] == 0), res


    async def _query_and_data(self, cmd: int, data: bytes, *, tag: str = "", timeout_ms: Optional[int] = None) -> Optional[bytes]:
        fut: asyncio.Future[Optional[bytes]] = asyncio.get_running_loop().create_future()

        q_ms = self._cfg_int("QUERY_TIMEOUT_MS", 4500)
        eff_timeout_ms = int(timeout_ms or q_ms)

        self._enqueue_query(
            cmd, data,
            tag=tag,
            timeout_ms=eff_timeout_ms,
            callback=lambda b: (not fut.done()) and fut.set_result(b),
        )

        try:
            return await asyncio.wait_for(fut, timeout=eff_timeout_ms / 1000.0 + 2.0)
        except asyncio.TimeoutError:
            return None


    def _enqueue_exec(
        self, cmd: int, data: bytes, *,
        tag: str = "", timeout_ms: Optional[int] = None, gap_ms: Optional[int] = None,
        retries: int = 3, allow_no_reply: bool = False, allow_when_closing: bool = False,
        callback: Optional[Callable[[Optional[bytes]], None]] = None
    ):
        if self._closing and not allow_when_closing:
            return

        cb = callback or (lambda _b: None)

        ack_ms = self._cfg_int("ACK_TIMEOUT_MS", 2000)
        cmd_gap_ms = self._cfg_int("CMD_GAP_MS", 1500)

        eff_timeout_ms = int(timeout_ms or ack_ms)
        eff_gap_ms = int(gap_ms or cmd_gap_ms)

        self._cmd_q.append(RfCommand(
            kind="exec",
            cmd=cmd,
            data=data,
            timeout_ms=eff_timeout_ms,
            gap_ms=eff_gap_ms,
            tag=tag,
            retries_left=retries,
            allow_no_reply=allow_no_reply,
            allow_when_closing=allow_when_closing,
            callback=cb,
        ))


    def _enqueue_query(
        self, cmd: int, data: bytes, *,
        tag: str = "", timeout_ms: Optional[int] = None, gap_ms: Optional[int] = None,
        retries: int = 3, allow_when_closing: bool = False,
        callback: Optional[Callable[[Optional[bytes]], None]] = None
    ):
        if self._closing and not allow_when_closing:
            return

        cb = callback or (lambda _b: None)

        q_ms = self._cfg_int("QUERY_TIMEOUT_MS", 4500)
        cmd_gap_ms = self._cfg_int("CMD_GAP_MS", 1500)

        eff_timeout_ms = int(timeout_ms or q_ms)
        eff_gap_ms = int(gap_ms or cmd_gap_ms)

        self._cmd_q.append(RfCommand(
            kind="query",
            cmd=cmd,
            data=data,
            timeout_ms=eff_timeout_ms,
            gap_ms=eff_gap_ms,
            tag=tag,
            retries_left=retries,
            allow_no_reply=False,
            allow_when_closing=allow_when_closing,
            callback=cb,
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
    
    # ---------- 태스크 안전 스폰 ----------
    def _spawn(self, coro):
        """
        running loop가 있을 때만 task로 스케줄.
        종료/루프없음 상황이면 coro.close()로 'never awaited' 경고를 막는다.
        """
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            # running loop 없음(대부분 종료 타이밍) → 경고 방지
            try:
                coro.close()
            except Exception:
                pass
            return None

    # ---------- 파싱/검증/로그 ----------
    def _decode_frame_suffix(self, cmd: int, pkt: bytes) -> str:
        """
        RX FRAME 로그용 해석 문자열 생성.
        raw 로그는 유지하고, 사람이 읽을 수 있는 10진수 값을 뒤에 덧붙인다.
        """
        try:
            data = self._extract_data(pkt)

            if cmd == CMD_REPORT_FORWARD:
                if len(data) >= 2:
                    return f" decoded={_u16le(data, 0)} W"
                return " decoded=<short_payload>"

            if cmd == CMD_REPORT_REFLECTED:
                if len(data) >= 2:
                    return f" decoded={_u16le(data, 0)} W"
                return " decoded=<short_payload>"

            if cmd == CMD_REPORT_DELIVERED:
                if len(data) >= 2:
                    return f" decoded={_u16le(data, 0)} W"
                return " decoded=<short_payload>"

            if cmd == CMD_REPORT_SETPOINT:
                if len(data) >= 2:
                    return f" decoded={_u16le(data, 0)} W"
                return " decoded=<short_payload>"

            if cmd == CMD_REPORT_PULSE_FREQ:
                if len(data) >= 3:
                    return f" decoded={_u24le(data, 0)} Hz"
                return " decoded=<short_payload>"

            if cmd == CMD_REPORT_PULSE_DUTY:
                if len(data) >= 2:
                    return f" decoded={_u16le(data, 0)} %"
                return " decoded=<short_payload>"

            if cmd == CMD_REPORT_PULSING:
                if len(data) >= 1:
                    mode_map = {
                        0: "OFF",
                        1: "INTERNAL",
                        2: "EXTERNAL",
                        3: "EXTERNAL_INVERTED",
                        4: "GATED_INTERNAL",
                        5: "GATED_INTERNAL_INVERTED",
                    }
                    mode = data[0]
                    return f" decoded={mode} ({mode_map.get(mode, 'UNKNOWN')})"
                return " decoded=<short_payload>"

            if cmd == CMD_REPORT_STATUS:
                st = self._parse_status_0xA2(data)
                if st is not None:
                    return f" decoded={self._status_summary_str(st)}"
                return " decoded=<status_parse_fail>"

            return ""

        except Exception as e:
            return f" decoded_error={type(e).__name__}:{e}"

    def _parse_status_0xA2(self, data: Optional[bytes]) -> Optional[RfStatus]:
        if not data or len(data) < 4:
            self._spawn(self._emit_status("STATUS payload too short"))
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
            self._spawn(self._emit_status("STATUS: Interlock OPEN detected"))
        if st.overtemp:
            self._spawn(self._emit_status("STATUS: Over-Temperature detected"))
        if st.extended_fault:
            self._spawn(self._emit_status("STATUS: Extended fault present"))
        if st.rf_on_requested and not st.rf_output_on:
            self._spawn(self._emit_status("STATUS: RF requested but output not ON yet"))

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
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            setattr(self, name, None)

    def _cmd_label(self, cmd: int) -> str:
        name = CMD_NAMES.get(cmd)
        return f"{name}(0x{cmd:02X})" if name else f"0x{cmd:02X}"

    def _dbg(self, src: str, msg: str):
        if self.debug_print:
            print(f"[{src}] {msg}")

    def _purge_pending(self, reason: str = "") -> int:
        """
        명령 큐/인플라이트를 폐기하고 콜백에 실패(None) 통지.
        MFC와 동일한 패턴으로 구현하여 shutdown/polling off 시 충돌 방지.
        """
        purged = 0

        # Inflight 하나 정리
        if self._inflight is not None:
            cmd = self._inflight
            self._inflight = None
            purged += 1
            self._safe_callback(cmd.callback, None)

        # 큐 비우기
        while self._cmd_q:
            c = self._cmd_q.popleft()
            purged += 1
            self._safe_callback(c.callback, None)

        # ✅ polling off + purged==0 은 로그 스팸이므로 생략
        if reason:
            if (purged > 0) or (reason != "polling off"):
                self._spawn(self._emit_status(f"대기 중 명령 {purged}개 폐기 ({reason})"))
        return purged
    
    # =========== chamber_runtime.py에 맞춘 함수들 ===========
    def set_endpoint(self, host: str, port: int) -> None:
        self._override_host = str(host)
        self._override_port = int(port)

    async def set_endpoint_reconnect(self, host: str, port: int) -> None:
        self._override_host = str(host)
        self._override_port = int(port)
        await self.pause_watchdog()
        try:
            self._on_tcp_disconnected()
        except Exception:
            pass
        await self.start()

    def is_connected(self) -> bool:
        return bool(self._connected)

    async def pause_watchdog(self) -> None:
        self._want_connected = False
        t = self._watchdog_task
        if t:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._watchdog_task = None

    async def resume_watchdog(self) -> None:
        self._want_connected = True
        await self.start()   # ✅ start()가 알아서 watchdog/worker 상태를 정리/보장
    # =========== chamber_runtime.py에 맞춘 함수들 ===========

# device/dc_pulse.py
# -*- coding: utf-8 -*-
"""
dc_pulse.py — EnerPulse 5 Pulser RS-232 제어 (MOXA NPort 등 TCP-Serial 게이트웨이 경유)
- asyncio Streams + 단일 명령 큐 + 워치독
- 프로토콜 Type4(STX/ETX/CHK) 바이너리 프레이밍 (RS-232 전용)
- 장비에서 Host/Mode/펄스 파라미터는 수동 설정, 코드는 Power setpoint(0x83)와 Output On/Off(0x80)만 제어

사용 예:
    dcp = AsyncDCPulse(host="192.168.1.50", port=4010)
    await dcp.start()
    await dcp.prepare_and_start(power_w=2500.0)  # Host 설정 → Power 모드 → 2.5kW 설정 → 출력 ON
    ...
    await dcp.output_off()
    await dcp.cleanup()
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Callable, Deque, AsyncGenerator, Literal, Union
from collections import deque
import asyncio, time, contextlib, socket
from lib import config_common as cfgc   # 공통 config(런타임 reload용)

# ========= 이벤트 모델 =========
EventKind = Literal["status", "telemetry", "command_confirmed", "command_failed"]

@dataclass
class DCPEvent:
    kind: EventKind
    message: Optional[str] = None
    cmd: Optional[str] = None
    reason: Optional[str] = None
    data: Optional[dict] = None
    # ↓↓↓ 추가: chamber_runtime 호환용 편의 필드
    power: Optional[float] = None
    voltage: Optional[float] = None
    current: Optional[float] = None
    eng: Optional[dict] = None

# ========= 명령 레코드 =========
@dataclass
class Command:
    payload: bytes
    label: str
    timeout_ms: int
    gap_ms: int
    retries_left: int
    callback: Optional[Callable[[Optional[bytes]], None]]

# ========= 프로토콜 인터페이스/구현 =========
class IProtocol:
    """EnerPulse RS-232 프레임 인/디코더 인터페이스."""
    def pack_write(self, code: int, value: Optional[int] = None, *, width: int = 0) -> bytes: ...
    def pack_read(self, code: int) -> bytes: ...
    def filter_and_decode(self, payload: bytes) -> Optional[bytes]: ...

def _chk_nibble_sum(items: bytes) -> int:
    """
    매뉴얼 방식: 상/하 니블 합산, 하니블 캐리는 상니블에 전달
    """
    hi_sum = 0
    lo_sum = 0
    
    for b in items:
        hi_sum += (b >> 4) & 0x0F
        lo_sum += b & 0x0F
    
    # 하니블 캐리를 상니블에 전달
    hi_sum += (lo_sum >> 4)
    
    # 최종 mod 16
    return ((hi_sum & 0x0F) << 4) | (lo_sum & 0x0F)

def _is_keep(x) -> bool:
    return isinstance(x, str) and x.strip().lower() == "keep"

class BinaryProtocol(IProtocol):
    """
    Protocol Type 4: STX(0x02) + [IP?] + CMD(1B) + DATA(0~2B) + ETX(0x03) + CHK(1B)
      - RS-232: STX + CMD + DATA + ETX + CHK
      - RS-485: STX + IP + CMD + DATA + ETX + CHK
      - DATA 폭(width): 0/1/2 바이트
    """
    def __init__(self):
        pass # RS-232 only

    def _frame(self, cmd: int, data: bytes) -> bytes:
        stx = b"\x02"; etx = b"\x03"
        core = stx + bytes([cmd & 0xFF]) + data + etx
        chk  = bytes([_chk_nibble_sum(core)])
        return core + chk

    def pack_write(self, code: int, value: Optional[int] = None, *, width: int = 0) -> bytes:
        # width: 0=데이터없음, 1=1B, 2=2B
        if width == 0 or value is None:
            data = b""
        elif width == 1:
            data = bytes([int(value) & 0xFF])
        elif width == 2:
            v = int(value) & 0xFFFF
            # 매뉴얼 예제와 일치하도록 MSB, LSB 순서 사용
            data = bytes([(v >> 8) & 0xFF, v & 0xFF])
        else:
            raise ValueError("width must be 0/1/2")
        return self._frame(code, data)

    def pack_read(self, code: int) -> bytes:
        # 읽기 요청도 CMD만 담아 전송 (장비가 상태 프레임 반환)
        return self._frame(code, b"")

    def filter_and_decode(self, payload: bytes) -> Optional[bytes]:
        # 워커가 완전한 payload(RS-232: CMD+DATA.. / RS-485: IP+CMD+DATA..)를 전달.
        # 필요 시 여기서 파싱/검증 추가 가능.
        return payload if payload else None
    
# ========= EnerPulse 컨트롤러 =========
class AsyncDCPulse:
    """
    EnerPulse RS-232 Async 컨트롤러
    - start()/cleanup(), events() 제공
    - 고수준 API:
        set_master_host_all() → Host 마스터 강제
        set_regulation_power() → 제어모드 Power
        set_reference_power(w) → 출력 레벨(전력) 설정
        output_on()/output_off()
        prepare_and_start(power_w) → 위 4단계 일괄 수행
    """
    def __init__(
        self,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        protocol: Optional[IProtocol] = None,
        on_telemetry: Optional[Callable[[float, float, float], None]] = None,
        cfg: Optional[object] = None,   # ✅ 추가: config_ch1/config_ch2/config_common 주입
    ):
        # Endpoint override
        self._override_host = host
        self._override_port = port

        # ✅ cfg 저장(없으면 config_common 사용)
        self._cfg = cfg if cfg is not None else cfgc

        # Protocol (기본: Type4 Binary)
        self._proto: IProtocol = protocol if protocol else BinaryProtocol()

        # TCP
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._connected = False
        self._ever_connected = False

        # Queues / Tasks
        self._cmd_q: Deque[Command] = deque()
        self._inflight: Optional[Command] = None
        self._frame_q: asyncio.Queue[bytes] = asyncio.Queue(maxsize=256)
        self._event_q: asyncio.Queue[DCPEvent] = asyncio.Queue(maxsize=1024)
        self._watchdog_task: Optional[asyncio.Task] = None
        self._cmd_worker_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._want_connected = False

        # 기타
        self._last_connect_mono: float = 0.0
        self._just_reopened: bool = False

        # 런타임 캐시(초기값은 reload에서 채움)
        self.debug_print = False

        self._max_power_w = 0.0
        self._p_set_tol_pct = 0.0
        self._p_set_tol_w = 0.0
        self._p_set_deviate_max_n = 1

        self._i_low_thresh_a = 0.0
        self._i_low_count_max_n = 1

        self._cmd_max_retries = 0
        self._recover_max_attempts = 1
        self._write_worker_retries = 0
        self._enable_fault_recover = True

        self._activation_check_delay_s = 0.0
        self._poll_period_s = 5.0
        self._connect_timeout_s = 1.0

        self._timeout_ms = 1000
        self._gap_ms = 0
        self._last_send_mono = 0.0   # ✅ 커맨드 최소 간격(gap) 기준 시각
        self._watchdog_interval_ms = 1000

        self._reconnect_backoff_start_ms = 1000
        self._reconnect_backoff_max_ms = 10000
        self._first_cmd_extra_timeout_ms = 0

        self._post_open_quiet_s = 0.0
        self._drain_timeout_s = 1.0

        self._inactivity_s = 0.0
        self._tcp_keepalive = False

        # ---- 스케일/스텝 캐시 ----
        self._v_meas_v_per_lsb = 1.0
        self._i_meas_a_per_lsb = 1.0
        self._p_meas_w_per_lsb = 1.0

        self._ramp_ms_per_lsb = 1.0
        self._arc_us_per_lsb = 1.0

        self._v_set_step_v = 1.0
        self._i_set_step_a = 1.0
        self._p_set_step_w = 1.0

        self._last_io_mono: float = 0.0
        self._out_on: bool = False
        self._last_ref_power_w: Optional[float] = None  # ← 세트포인트 저장

        self._spdev_n: int = 0
        self._low_curr_n: int = 0

        # ✅ STOP/종료 중에 ON/SET 계열 write 재전송을 막기 위한 가드
        self._stop_guard: bool = False

        # ✅ Arc 누적 카운터 및 알림 플래그 초기화 (공정 시작 시 reset_arc_counts()로 재초기화)
        self._soft_arc_total: int = 0
        self._hard_arc_total: int = 0
        self._arc_alert_sent: bool = False

        # ★ 추가: on_telemetry 콜백 저장 (3/19 리팩토링 시 누락됨)
        self._on_telemetry = on_telemetry

        # ✅ 여기서 config를 다시 읽어 런타임 값 반영
        self.reload_runtime_cfg()

    def _cfg_get(self, key: str, default):
        # cfg(ch1/ch2) 우선 → 없으면 config_common → 없으면 default
        if self._cfg is not None and hasattr(self._cfg, key):
            return getattr(self._cfg, key)
        if hasattr(cfgc, key):
            return getattr(cfgc, key)
        return default

    def _cfg_int(self, key: str, default: int) -> int:
        try:
            return int(self._cfg_get(key, default))
        except Exception:
            return int(default)

    def _cfg_float(self, key: str, default: float) -> float:
        try:
            return float(self._cfg_get(key, default))
        except Exception:
            return float(default)

    def _cfg_bool(self, key: str, default: bool) -> bool:
        try:
            return bool(self._cfg_get(key, default))
        except Exception:
            return bool(default)

    def reload_runtime_cfg(self) -> None:
        """
        UI에서 config 버튼으로 값을 바꾼 뒤:
        - chamber_runtime에서 이 메서드를 호출하면
        - dc_pulse 드라이버가 즉시 런타임 파라미터를 갱신한다.
        핵심 로직은 손대지 않고 '숫자 읽는 방식'만 동적으로 만든다.
        """
        # DEBUG
        self.debug_print = self._cfg_bool("DEBUG_PRINT", False)

        # Power clamp
        self._max_power_w = max(0.0, self._cfg_float("DCP_MAX_POWER_W", 1000.0))

        # 감시 파라미터
        self._p_set_tol_pct = max(0.0, self._cfg_float("DCP_P_SET_TOL_PCT", 0.05))
        self._p_set_tol_w = max(0.0, self._cfg_float("DCP_P_SET_TOL_W", 15.0))
        self._p_set_deviate_max_n = max(1, self._cfg_int("DCP_P_SET_DEVIATE_MAX_N", 3))

        self._i_low_thresh_a = max(0.0, self._cfg_float("DCP_I_LOW_THRESH_A", 0.05))
        self._i_low_count_max_n = max(1, self._cfg_int("DCP_I_LOW_COUNT_MAX_N", 3))

        # 명령/복구 정책
        self._cmd_max_retries = max(0, self._cfg_int("DCP_CMD_MAX_RETRIES", 5))
        self._recover_max_attempts = max(1, self._cfg_int("DCP_RECOVER_MAX_ATTEMPTS", 5))
        self._write_worker_retries = max(0, self._cfg_int("DCP_WRITE_WORKER_RETRIES", 0))
        self._enable_fault_recover = self._cfg_bool("DCP_ENABLE_FAULT_RECOVER", True)

        # 타이밍
        self._activation_check_delay_s = max(0.0, self._cfg_float("DCP_ACTIVATION_CHECK_DELAY_S", 5.0))
        self._poll_period_s = max(0.1, self._cfg_float("DCP_POLL_INTERVAL_S", 5.0))
        self._connect_timeout_s = max(0.5, self._cfg_float("DCP_CONNECT_TIMEOUT_S", 3.0))

        self._timeout_ms = max(200, self._cfg_int("DCP_TIMEOUT_MS", 2500))
        self._gap_ms = max(0, self._cfg_int("DCP_GAP_MS", 1000))
        self._watchdog_interval_ms = max(200, self._cfg_int("DCP_WATCHDOG_INTERVAL_MS", 1000))

        self._reconnect_backoff_start_ms = max(200, self._cfg_int("DCP_RECONNECT_BACKOFF_START_MS", 1000))
        self._reconnect_backoff_max_ms = max(self._reconnect_backoff_start_ms,
                                            self._cfg_int("DCP_RECONNECT_BACKOFF_MAX_MS", 10000))
        self._first_cmd_extra_timeout_ms = max(0, self._cfg_int("DCP_FIRST_CMD_EXTRA_TIMEOUT_MS", 2000))

        self._post_open_quiet_s = max(0.0, self._cfg_float("DCP_POST_OPEN_QUIET_S", 0.8))
        self._drain_timeout_s = max(0.0, self._cfg_float("DCP_DRAIN_TIMEOUT_S", 1.0))

        # TCP 전략(이 키는 config_common 상단에 이미 존재)
        self._inactivity_s = max(0.0, self._cfg_float("DCP_INACTIVITY_REOPEN_S", 0.0))
        self._tcp_keepalive = self._cfg_bool("DCP_TCP_KEEPALIVE", False)

        # ---- 스케일/스텝(새로 추가) ----
        self._v_meas_v_per_lsb = self._cfg_float("DCP_V_MEAS_V_PER_LSB", 1.468815)
        self._i_meas_a_per_lsb = self._cfg_float("DCP_I_MEAS_A_PER_LSB", 0.01)
        self._p_meas_w_per_lsb = self._cfg_float("DCP_P_MEAS_W_PER_LSB", 10.0)

        self._ramp_ms_per_lsb = self._cfg_float("DCP_RAMP_MS_PER_LSB", 1.0)
        self._arc_us_per_lsb  = self._cfg_float("DCP_ARC_US_PER_LSB", 1.0)

        self._v_set_step_v = self._cfg_float("DCP_V_SET_STEP_V", 1.0)
        self._i_set_step_a = self._cfg_float("DCP_I_SET_STEP_A", 0.1)
        self._p_set_step_w = self._cfg_float("DCP_P_SET_STEP_W", 10.0)

        # 방어: 0 방지
        if self._p_set_step_w <= 0:
            self._p_set_step_w = 10.0
        if self._v_set_step_v <= 0:
            self._v_set_step_v = 1.0
        if self._i_set_step_a <= 0:
            self._i_set_step_a = 0.1

    # ====== 공용 API ======
    async def start(self):
        # 1) 죽은 태스크 정리
        if self._watchdog_task and self._watchdog_task.done():
            self._watchdog_task = None
        if self._cmd_worker_task and self._cmd_worker_task.done():
            self._cmd_worker_task = None

        # 2) 둘 다 살아있으면 중복 생성 불필요
        if self._watchdog_task and self._cmd_worker_task:
            return

        self._want_connected = True
        loop = asyncio.get_running_loop()

        # 3) ✅ 없는 것만 생성 (살아있는 태스크 절대 overwrite 금지 → orphan 방지)
        if not self._watchdog_task:
            self._watchdog_task = loop.create_task(self._watchdog_loop(), name="DCPWatchdog")
        if not self._cmd_worker_task:
            self._cmd_worker_task = loop.create_task(self._cmd_worker_loop(), name="DCPCmdWorker")

    async def cleanup(self):
        await self._emit_status("DCP 종료 절차 시작")
        self._want_connected = False
        await self._cancel_task("_poll_task")
        await self._cancel_task("_cmd_worker_task")
        await self._cancel_task("_watchdog_task")
        self._purge_pending("shutdown")

        if self._reader_task:
            # ✅ [FIX] suppress(Exception)은 CancelledError를 잡지 못해 cleanup 전체가 중단됨
            #    (→ writer close / 프레임 큐 비움 스킵 → 다음 런에 잔여 상태 유입)
            self._reader_task.cancel()
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await self._reader_task
            self._reader_task = None

        # ── TCP 세션 완전 종료: wait_closed()까지 대기, 실패 시 abort 보강
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

        # ── 프레임 큐/잔여물 비움(이전 런 찌꺼기 제거)
        while True:
            try:
                self._frame_q.get_nowait()
            except asyncio.QueueEmpty:
                break

        # ── 상태 리셋(다음 런이 항상 깨끗하게 시작)
        self._reader = None
        self._writer = None
        self._connected = False
        self._just_reopened = False
        self._out_on = False
        self._last_io_mono = 0.0

        # ✅ Arc 상태 리셋 (다음 공정 오염 방지)
        self._soft_arc_total = 0
        self._hard_arc_total = 0
        self._arc_alert_sent = False

    async def events(self) -> AsyncGenerator[DCPEvent, None]:
        while True:
            ev = await self._event_q.get()
            yield ev

    def set_endpoint(self, host: str, port: int) -> None:
        self._override_host = str(host)
        self._override_port = int(port)

    async def set_endpoint_reconnect(self, host: str, port: int) -> None:
        """엔드포인트 변경 + 즉시 재연결."""
        self._override_host = str(host)
        self._override_port = int(port)
        await self.pause_watchdog()
        try:
            self._on_tcp_disconnected()
        except Exception:
            pass
        await self.start()

    def set_process_status(self, should_poll: bool):
        if should_poll:
            if self._poll_task is None or self._poll_task.done():
                self._ev_nowait(DCPEvent(kind="status", message=f"Polling read 시작({self._poll_period_s:.1f}s)"))
                self._poll_task = asyncio.create_task(self._poll_loop())
        else:
            # ✅ 실제로 실행 중이었을 때만 로그 (중복 호출 시 노이즈 방지)
            was_running = self._poll_task is not None and not self._poll_task.done()
            if self._poll_task:
                self._poll_task.cancel()
                self._poll_task = None
            self._purge_pending("polling off", only_poll_reads=True, drop_inflight=False)
            if was_running:
                self._ev_nowait(DCPEvent(kind="status", message="Polling read 중지"))

    # 추가: 연결 완료 대기 헬퍼
    async def _wait_until_connected(self, timeout: Optional[float] = None) -> bool:
        if timeout is None:
            timeout = float(self._connect_timeout_s)
        deadline = time.monotonic() + float(timeout)
        while time.monotonic() < deadline:
            if self._connected and self._writer and not self._writer.is_closing():
                return True
            await asyncio.sleep(0.05)
        return False

    # ====== 상위 시퀀스 편의 API ======
    async def prepare_and_start(
        self,
        power_w: float,
        *,
        # 'keep' 또는 None이면 변경하지 않음
        freq: Optional[Union[float, int, str]] = None,
        duty: Optional[Union[float, int, str]] = None,
        # 펄스 동기 모드: 'int' 또는 'ext' (None이면 유지)
        sync: Optional[Literal["int", "ext"]] = None,
        # 마스터 모드: 기본 host (기존 동작 유지), 필요 시 'remote' 등으로 지정
        master: Literal["host", "remote", "local", "origin", "always"] = "host",
    ):
        '''
        # 1) 항상 Host 권한으로 고정
        await self.set_master_host_all()

        # 3) 펄스 파라미터(옵션): sync / freq / duty
        #    EnerPulse 통신 명령: 0x65(Pulse Sync), 0x66(Pulse Freq[kHz 20~150]),
        #                        0x67(Off Time: DC=9, 1.0~10.0us -> 10~100)
        if sync is not None:
            await self.set_pulse_sync(sync)  # 0x65
        '''
        """
        OUTPUT_ON 이전 단계에서 하나라도 실패하면
        그 즉시 False 를 리턴하고 나머지 단계는 수행하지 않는다.
        (실패 이벤트는 각 명령에서 command_failed 로 이미 올라감)
        """

        # 0) 연결 준비
        ok_conn = await self._wait_until_connected(timeout=float(self._connect_timeout_s))
        if not ok_conn:
            await self._emit_failed("CONNECT", "연결 준비 실패")
            return False
        
        # ✅ STOP/종료 가드는 이전 런에서 남아 있을 수 있으므로 공정 시작 시 해제
        self._stop_guard = False
        # ✅ 세트포인트 캐시는 성공 시에만 갱신하도록(아래 set_reference_power 수정) 시작 전 초기화
        self._last_ref_power_w = None

        # ✅ 추가: Arc 누적 카운터 (공정 시작 시 reset_arc_counts() 호출)
        self._soft_arc_total: int = 0
        self._hard_arc_total: int = 0

        # ✅ Host 제어 공정이면 ONOFF/REFER/MODE master를 Host로 다시 강제
        if master == "host":
            ok_master = await self.set_master_host_all()
            if not ok_master:
                await self._emit_failed("PRECHECK", "ONOFF/REFER/MODE master를 HOST로 강제하지 못함")
                return False
        
        # ✅ [ADD] 공정 시작 전: 폴링 OFF + 버퍼 정리 + Ctrl/Fault 사전 점검
        self.set_process_status(False)
        self._drain_rx_frames()

        ctrl = await self.read_control_mode()
        # ✅ READ_CTRL_MODE가 None이면 즉시 중단 (뒤 단계 진행 금지)
        if ctrl is None:
            await self._emit_status("[PRECHECK] READ_CTRL_MODE 실패 → OUTPUT_ON 시퀀스 중단")
            return False
        # ✅ UNKNOWN도 안전상 중단 권장
        if ctrl not in ("HOST", "REMOTE", "LOCAL"):
            await self._emit_status(f"[PRECHECK] Control mode={ctrl} → OUTPUT_ON 시퀀스 중단")
            return False
        if ctrl == "LOCAL":
            await self._emit_failed("PRECHECK", "Control mode=LOCAL (패널에서 HOST/REMOTE 전환 필요)")
            return False

        fault = await self.read_fault_code()
        # ✅ READ_FAULT가 None이면 안전상 중단
        if fault is None:
            await self._emit_status("[PRECHECK] READ_FAULT 실패 → OUTPUT_ON 시퀀스 중단")
            return False
        if fault != 0:
            await self._emit_status(f"[PRECHECK] fault=0x{fault:04X} → FAULT_RESET(0x6F) 시도")
            ok_reset = await self.fault_reset()
            if not ok_reset:
                await self._emit_failed("PRECHECK", "FAULT_RESET 실패(인터락/점화/케이블/진공 상태 확인 필요)")
                return False

        # 2) (옵션) freq/duty 모두 숫자면 off_time_us를 계산해서 0x67로 전송
        if not _is_keep(freq) and freq is not None:
            f_khz = float(freq)
            ok_f = await self.set_pulse_freq_khz(f_khz)  # 0x66

            if not ok_f:
                await self._emit_status("PULSE_FREQ 설정 실패 → OUTPUT_ON 시퀀스 중단")
                return False

            if not _is_keep(duty) and duty is not None:
                d_pct = float(duty)
                # 주기[us] = 1,000 / f[kHz]
                period_us = 1000.0 / max(1e-6, f_khz)
                # off_time_us = period * (1 - duty)
                off_time_us = max(0.0, period_us * (1.0 - d_pct / 100.0))

                if off_time_us > 10.0:
                    await self._emit_status(
                        f"요청 듀티 {d_pct:.1f}% @ {f_khz:.0f}kHz 불가 → "
                        f"Off가 {off_time_us:.1f}us로 10.0us 상한 초과 → 장비가 10.0us로 클램프"
                    )

                # 장비 스펙: DC=9, 1.0~10.0us → 10~100 (x10 스케일)
                if d_pct >= 100.0 or off_time_us < 1.0:
                    ok_dc = await self.set_off_time_dc()         # 0x67, DC=9
                    if not ok_dc:
                        await self._emit_status("OFF_TIME(DC) 설정 실패 → OUTPUT_ON 시퀀스 중단")
                        return False
                else:
                    ok_off = await self.set_off_time_us(off_time_us)  # 0x67
                    if not ok_off:
                        await self._emit_status("OFF_TIME 설정 실패 → OUTPUT_ON 시퀀스 중단")
                        return False
            # duty가 keep/None이면 주파수만 적용(Off Time 유지)

        # duty만 숫자인 경우(주파수 미지정)는 off_time_us 계산 불가 → 유지
        # 필요하면 별도 API(set_off_time_us)로 직접 지정하세요.

        # ✅ [EL: 2026-08-25 런3] 쓰기 ACK 후 미적용(40kHz 잔존) 실측 → read-back 검증
        if not _is_keep(freq) and freq is not None:
            want_khz = int(round(float(freq)))
            applied = False
            for attempt in (1, 2):
                # ✅ [FIX] read_actual_pulse_params()는 A6+A7을 함께 읽어 A7만 실패해도
                #    전체가 None → 검증이 무력화됨. 주파수 검증은 A6 단독으로 수행.
                got = await self.read_pulse_freq_khz()
                if got is None:
                    await self._emit_status("[VERIFY] PULSE_FREQ read-back 실패(통신) → 검증 생략")
                    applied = True
                    break
                got_khz = int(got)
                if got_khz == want_khz:
                    applied = True
                    break
                if attempt == 1:
                    await self._emit_status(
                        f"[VERIFY] PULSE_FREQ 미적용 감지 (요청 {want_khz}kHz ≠ 장비 {got_khz}kHz) → 0.5s 후 재기록"
                    )
                    await asyncio.sleep(0.5)
                    if not await self.set_pulse_freq_khz(float(freq)):
                        break
                    await asyncio.sleep(0.5)
            if not applied:
                await self._emit_failed(
                    "PULSE_FREQ",
                    f"쓰기 ACK 후에도 미적용 지속 (요청 {want_khz}kHz) — 장비 마스터/패널 상태 점검 필요"
                )
                return False

        # 3) 제어 모드 = Power
        ok_reg = await self.set_regulation_power()
        if not ok_reg:
            await self._emit_status("REG_POWER 실패 → OUTPUT_ON 시퀀스 중단")
            return False

        # 4) 출력 Setpoint(Power) 설정
        ok = await self.set_reference_power(power_w)
        if not ok:
            # 여기서는 output_off() 를 직접 호출하지 않고,
            # 실패 이벤트 + False 리턴만으로 상위 종료 시퀀스에 맡긴다.
            await self._emit_status("REF_POWER 실패 → OUTPUT_ON 생략")
            return False

        # 5) 출력 ON (성공시에만)
        ok2 = await self.output_on()
        if not ok2:
            return False

        # ✅ [EL: 2026-08-25 런3] ON ACK 후에도 출력 미기동(V=I=P=0) 실측
        #    → STATUS의 HV-On 비트로 실확인. 읽기 실패(None)는 오탐 방지 위해 통과(기존 동작 유지)
        flags = await self.read_status_flags()
        if flags is not None and not self._hv_on_from_status(flags):
            f2 = await self.read_fault_code()
            m_on  = await self._read_raw(0xBB, "READ_MASTER_ONOFF")
            m_ref = await self._read_raw(0xBC, "READ_MASTER_REFER")
            m_md  = await self._read_raw(0xBD, "READ_MASTER_MODE")
            def _hx(b): return b.hex() if b else "-"
            await self._emit_failed(
                "OUTPUT_ON",
                "ACK 수신했지만 HV Off 상태 — 장비가 ON을 무시함 "
                f"(fault={('0x%04X' % f2) if f2 is not None else '조회실패'}, "
                f"master ONOFF/REFER/MODE={_hx(m_on)}/{_hx(m_ref)}/{_hx(m_md)})"
            )
            return False
        return True

    # ====== 고수준 제어 ======
    async def set_master_host_all(self) -> bool:
        """
        매뉴얼:
        - 0x7B = ONOFF Master
        - 0x7C = Refer. Master
        - 0x7D = Mode Master
        - 0x0003 = Host
        """
        for cmd, name in (
            (0x7B, "MASTER_ONOFF"),
            (0x7C, "MASTER_REFER"),
            (0x7D, "MASTER_MODE"),
        ):
            ok = await self._write_cmd_data(cmd, 0x0003, 2, label=name)
            if not ok:
                await self._emit_failed(name, "HOST master 강제 실패")
                return False

        await asyncio.sleep(0.2)  # 장비 내부 반영 유예
        return True

    async def set_regulation(self, mode: Literal["V","I","P"]) -> bool:
        """0x81: 제어 모드 설정 (1=V, 2=I, 3=P)."""
        code_map = {"V":1, "I":2, "P":3}
        val = code_map[mode.upper()]
        return await self._write_cmd_data(0x81, val, 2, label=f"REG_{mode.upper()}")

    async def set_regulation_power(self) -> bool:
        """제어 모드 = Power."""
        return await self._write_cmd_data(0x81, 3, 2, label="REG_POWER")

    async def set_reference(self, mode: Literal["V","I","P"], value: float):
        """0x83: 출력 레벨(참조) 설정 — 모드별 스케일 적용."""
        if mode.upper() == "V":
            raw = int(round(value / self._v_set_step_v))
        elif mode.upper() == "I":
            raw = int(round(value / self._i_set_step_a))
        else:
            raw = int(round(float(value) / self._p_set_step_w))
            raw = max(0, min(int(self._max_power_w // self._p_set_step_w), raw))

        await self._write_cmd_data(0x83, raw, 2, label=f"REF_{mode.upper()}({value})")

    async def set_reference_power(self, value_w: float, *, pause_polling: bool = False) -> bool:
        """출력 레벨(전력) 설정 — 10 W/step → 0~500."""
        raw = int(round(float(value_w) / self._p_set_step_w))
        raw = max(0, min(int(self._max_power_w // self._p_set_step_w), raw))

        was_polling = bool(self._poll_task and not self._poll_task.done())

        if pause_polling and was_polling:
            self.set_process_status(False)

        try:
            ok = await self._write_cmd_data(0x83, raw, 2, label=f"REF_POWER({value_w:.0f}W)")
            if ok:
                self._last_ref_power_w = float(value_w)
                self._spdev_n = 0
                self._low_curr_n = 0
            return bool(ok)

        finally:
            # STOP/종료 중이면 polling을 다시 켜지 않는다.
            if pause_polling and was_polling and self._out_on and not self._stop_guard:
                self.set_process_status(True)

    async def output_on(self) -> bool:
        """0x80: 1=ON, 2=OFF."""
        self._drain_rx_frames()  # ← 잔여 0x9A 등 제거
        self._spdev_n = 0               # ★ 세트포인트 이탈 카운터 초기화
        self._low_curr_n = 0            # ★ 저전류 카운터도 초기화
        return await self._write_cmd_data(0x80, 0x0001, 2, label="OUTPUT_ON")

    async def output_off(self) -> bool:
        # ✅ STOP/종료 중 재전송 루프가 REG/REF/OUTPUT_ON으로 흘러가는 것을 차단
        # (STOP 시퀀스에서 OUTPUT_OFF 이후 OUTPUT_ON이 다시 실행되는 현상 방지)
        self._stop_guard = True
        self._drain_rx_frames()  # ← 잔여 0x9A 등 제거
        return await self._write_cmd_data(0x80, 0x0002, 2, label="OUTPUT_OFF")

    async def set_pulse_sync(self, mode: Literal["int","ext"]) -> bool:
        # 0x65: Int=0, Ext=1
        val = 0 if mode == "int" else 1
        return await self._write_cmd_data(0x65, val, 2, label=f"PULSE_SYNC({mode.upper()})")

    async def set_pulse_freq_khz(self, freq_khz: float) -> bool:
        # 0x66: 20~150 (kHz)
        val = int(round(freq_khz))
        val = min(150, max(20, val))
        return await self._write_cmd_data(0x66, val, 2, label=f"PULSE_FREQ({val}kHz)")

    async def set_off_time_us(self, off_time_us: float) -> bool:
        # 0x67: DC=9, 1.0~10.0us → 10~100 (x10 스케일)
        x10 = int(round(off_time_us * 10.0))
        x10 = min(100, max(10, x10))
        applied_us = x10 / 10.0
        return await self._write_cmd_data(0x67, x10, 2, label=f"OFF_TIME({applied_us:.1f}us)")

    async def set_off_time_dc(self) -> bool:
        return await self._write_cmd_data(0x67, 9, 2, label="OFF_TIME(DC)")

    # ====== 선택: 기타 설정(원 코드 호환) ======
    async def set_arc_params(self, *, detection_us: float, pause_us: float,
                             arc_voltage_v: float|int, arc_current_a: float|int, soft_level: int):
        await self._write_cmd_data(0x05, int(round(detection_us / self._arc_us_per_lsb)), 2, label="ARC_DET_US")
        await self._write_cmd_data(0x06, int(round(pause_us     / self._arc_us_per_lsb)), 2, label="ARC_PAUSE_US")
        await self._write_cmd_data(0x07, int(round(float(arc_voltage_v) / self._v_set_step_v)), 2, label="ARC_VOLT_V")
        await self._write_cmd_data(0x08, int(round(float(arc_current_a) / self._i_set_step_a)), 2, label="ARC_CURR_A")
        await self._write_cmd_data(0x09, int(soft_level), 2, label="SOFT_ARC_LV")

    async def set_shutdown(self, *, delay_ms: int, pause_ms: int):
        await self._write_cmd_data(0x0A, int(delay_ms), 2, label="SHDN_DELAY_MS")
        await self._write_cmd_data(0x0B, int(pause_ms), 2, label="SHDN_PAUSE_MS")

    async def set_limits(self, *, p_w: float, i_a: float, v_v: float):
        p_raw = int(round(float(p_w) / self._p_set_step_w))
        p_raw = max(0, min(int(self._max_power_w // self._p_set_step_w), p_raw))
        i_raw = int(round(i_a / self._i_set_step_a))
        v_raw = int(round(v_v / self._v_set_step_v))

        await self._write_cmd_data(0x0C, p_raw, 2, label="LIM_P_W")
        await self._write_cmd_data(0x0D, i_raw, 2, label="LIM_I_A")
        await self._write_cmd_data(0x0E, v_raw, 2, label="LIM_V_V")

    async def set_ramp_and_ignition(self, *, ramp_ms: int, ignition_v: float):
        await self._write_cmd_data(0x0F, int(round(ramp_ms   / self._ramp_ms_per_lsb)), 2, label="RAMP_MS")
        await self._write_cmd_data(0x10, int(round(ignition_v / self._v_set_step_v)),  2, label="IGN_V")

    # ====== 읽기(모니터링/상태) - 필요 시 확장 ======
    # 1) 원시 바이트를 그대로 돌려주는 읽기 헬퍼
    async def _read_raw(self, code: int, label: str) -> Optional[bytes]:
        fut = asyncio.get_running_loop().create_future()
        def _cb(resp: Optional[bytes]):
            if not fut.done():
                fut.set_result(resp)
        payload = self._proto.pack_read(code)
        retries = 2
        self._enqueue(Command(payload, label, self._timeout_ms, self._gap_ms, retries, _cb))
        return await self._await_reply_bytes(
            label, fut,
            timeout_ms=self._timeout_ms,
            retries=retries,
            gap_ms=self._gap_ms
        )

    # 2) 현재 출력값 P/I/V 읽기 (0x9A → P,I,V 각 2바이트)
    async def read_output_piv(self) -> Optional[dict]:
        resp = await self._read_raw(0x9A, "READ_PIV")
        if not resp:
            await self._emit_status("READ_PIV: 응답 없음")
            return None

        # NAK(읽기 불가) → None
        if len(resp) == 1 and resp[0] == 0x04:
            await self._emit_status("READ_PIV: 장비가 읽기 불가 상태(ERR)")
            return None

        # 어떤 형태든 '뒤에서 6바이트'를 P,I,V로 해석 (CMD 유무 무시)
        if len(resp) < 6:
            await self._emit_status(f"READ_PIV: 응답 길이 부족: {resp!r}")
            return None

        data = resp[-6:]  # 항상 꼬리 6바이트 사용
        P_raw = (data[0] << 8) | data[1]
        I_raw = (data[2] << 8) | data[3]
        V_raw = (data[4] << 8) | data[5]

        P_W = P_raw * self._p_meas_w_per_lsb
        I_A = I_raw * self._i_meas_a_per_lsb
        V_V = V_raw * self._v_meas_v_per_lsb

        return {"raw": {"P": P_raw, "I": I_raw, "V": V_raw},
                "eng": {"P_W": P_W, "I_A": I_A, "V_V": V_V}}
    
    # Soft/Hard Arc 누적값 읽기 (0x96 / 0x99) — 출력 ON 이후 장비가 누적 관리
    async def read_soft_arc_total(self) -> Optional[int]:
        """0x96: 출력 ON 이후 누적 Soft Arc 수. 실패 시 None."""
        resp = await self._read_raw(0x96, "READ_ARC_SOFT")
        if not resp or len(resp) < 2:
            return None
        if len(resp) == 1 and resp[0] == 0x04:
            return None
        return (resp[-2] << 8) | resp[-1]

    async def read_hard_arc_total(self) -> Optional[int]:
        """0x99: 출력 ON 이후 누적 Hard Arc 수. 실패 시 None."""
        resp = await self._read_raw(0x99, "READ_ARC_HARD")
        if not resp or len(resp) < 2:
            return None
        if len(resp) == 1 and resp[0] == 0x04:
            return None
        return (resp[-2] << 8) | resp[-1]

    async def read_pulse_freq_khz(self) -> Optional[int]:
        """0xA6: 장비에 현재 설정된 Pulse Freq (kHz)."""
        resp = await self._read_raw(0xA6, "READ_PULSE_FREQ")
        if not resp or len(resp) < 2:
            return None
        if len(resp) == 1 and resp[0] == 0x04:
            return None
        return (resp[-2] << 8) | resp[-1]

    async def read_off_time_raw(self) -> Optional[int]:
        """0xA7: 장비에 현재 설정된 Off Time raw값 (DC=9, 10~100 = 1.0~10.0us x10)."""
        resp = await self._read_raw(0xA7, "READ_OFF_TIME")
        if not resp or len(resp) < 2:
            return None
        if len(resp) == 1 and resp[0] == 0x04:
            return None
        return (resp[-2] << 8) | resp[-1]

    async def read_actual_pulse_params(self) -> Optional[dict]:
        """freq + off_time 읽고 duty_cycle 역산. 실패 시 None.
        반환: {"freq_khz": int, "off_time_us": float|None, "duty_pct": float}
        """
        freq_khz = await self.read_pulse_freq_khz()
        off_raw  = await self.read_off_time_raw()
        if freq_khz is None or off_raw is None:
            return None
        if off_raw == 9:  # DC 모드
            return {"freq_khz": freq_khz, "off_time_us": None, "duty_pct": 100.0}
        off_time_us = off_raw / 10.0
        period_us   = 1000.0 / max(freq_khz, 1)
        duty_pct    = round((period_us - off_time_us) / period_us * 100, 1)
        return {"freq_khz": freq_khz, "off_time_us": off_time_us, "duty_pct": duty_pct}
    
    def reset_arc_counts(self) -> None:
        """공정 시작 시 호출 — Arc 누적 카운터 초기화."""
        self._soft_arc_total = 0
        self._hard_arc_total = 0
        self._arc_alert_sent: bool = False

    @property
    def arc_counts(self) -> tuple[int, int]:
        """(soft_arc_total, hard_arc_total) 반환."""
        return self._soft_arc_total, self._hard_arc_total
    
    # 3) 현재 Control Mode 읽기 (0x9C) READ_CTRL_MODE: CHK 제거 후 최하위 바이트 사용
    async def read_control_mode(self) -> Optional[str]:
        resp = await self._read_raw(0x9C, "READ_CTRL_MODE")
        if not resp or len(resp) < 2:
            await self._emit_failed("READ_CTRL_MODE", f"응답 길이 부족: {resp!r}")
            return None
        cmd, data, chk = self._unpack_rs232_payload(resp)
        if cmd != 0x9C or not data:
            await self._emit_failed("READ_CTRL_MODE", f"형식 오류: raw={resp.hex(' ')}")
            return None
        val = data[-1] & 0xFF  # 데이터의 LSB만 사용(CHK 제외)
        mapping = {1: "HOST", 2: "REMOTE", 4: "LOCAL"}
        return mapping.get(val, f"UNKNOWN({val})")

    # 4) Fault Code 읽기 (0x9E) READ_FAULT: CHK 제외 후 1B/2B 모두 허용
    async def read_fault_code(self) -> Optional[int]:
        resp = await self._read_raw(0x9E, "READ_FAULT")
        if not resp or len(resp) < 2:
            await self._emit_failed("READ_FAULT", f"응답 길이 부족: {resp!r}")
            return None
        cmd, data, chk = self._unpack_rs232_payload(resp)
        if cmd != 0x9E or not data:
            await self._emit_failed("READ_FAULT", f"형식 오류: raw={resp.hex(' ')}")
            return None
        if len(data) >= 2:
            return (data[-2] << 8) | data[-1]
        return data[-1]
    
    # [ADD] Fault State Reset (0x6F) : data=0x0001(clear)
    async def fault_reset(self) -> bool:
        """
        매뉴얼(Protocol): 0x6F Fault State Reset
        - Data: 2 bytes
        - clear = 1 (0x0001)
        """
        label = "FAULT_RESET"

        # 연결 상태 보장(끊긴 직후면 잠깐 대기)
        if not await self._wait_until_connected(timeout=1.5):
            await self._emit_failed(label, "연결 안됨")
            return False

        # 잔여 echo 제거
        self._purge_rx_frames()

        fut = asyncio.get_running_loop().create_future()

        def _cb(resp: Optional[bytes]):
            if not fut.done():
                fut.set_result(resp)

        payload = self._proto.pack_write(0x6F, 0x0001, width=2)

        # ✅ write는 워커 blind retry 없이 1회만(재시도는 상위 루프가 제어)
        self._enqueue(Command(payload, label, self._timeout_ms, self._gap_ms, self._write_worker_retries, _cb))

        resp = await self._await_reply_bytes(
            label, fut,
            timeout_ms=self._timeout_ms,
            retries=self._write_worker_retries,
            gap_ms=self._gap_ms
        )

        if not self._ok_from_resp(resp, label=label):
            await self._emit_failed(label, f"ACK 미수신: {resp!r}")
            return False

        await self._emit_confirmed(label)

        # reset 후 fault가 남아있는지 재확인 (best-effort)
        f = await self.read_fault_code()
        if f is None:
            await self._emit_status("FAULT_RESET: fault 재확인 실패(통신)")
            return True

        if f != 0:
            await self._emit_failed(label, f"fault 남음: 0x{f:04X}")
            return False

        return True
    

    async def _reopen_session_for_retry(self, label: str, timeout: float = 3.0) -> bool:
        """
        OUTPUT_OFF 같은 크리티컬 명령이 ERR(04) 연속으로 막힐 때
        현재 TCP 세션을 강제로 내리고 watchdog reconnect로 새 세션을 연다.
        """
        await self.start()  # watchdog/cmd worker 살아있게 보장

        try:
            self._on_tcp_disconnected()
        except Exception:
            pass

        ok = await self._wait_until_connected(timeout=timeout)
        if not ok:
            await self._emit_status(f"[{label}] 세션 재연결 실패")
            return False

        self._purge_rx_frames()
        self._drain_rx_frames()
        await asyncio.sleep(0.25)
        await self._emit_status(f"[{label}] 세션 재연결 완료 → 재시도")
        return True


    # [ADD] 명령 실패 시 fault 확인/클리어 후 재전송 여부 결정
    async def _recover_and_prepare_retry(self, label: str, resp: Optional[bytes]) -> bool:
        """
        write 명령 실패(NAK/timeout) 시 복구 판단.
        핵심:
        - timeout/disconnect(None)면 재연결 대기
        - OUTPUT_OFF 에서 ERR(04) + fault 조회 실패/무fault 지속이면
        세션 꼬임 가능성을 보고 세션 재연결 후 재시도
        """
        # 1) timeout/disconnect
        if resp is None:
            if not self._want_connected:   # ← 추가
                await self._emit_status(f"[{label}] cleanup 중 → 재연결 시도 안 함")
                return False
            await self.start()
            ok_conn = await self._wait_until_connected(timeout=float(self._connect_timeout_s))
            if not ok_conn:
                await self._emit_status(f"[{label}] 실패 후 재연결 안됨 → 복구 중단")
                return False

        # 2) 잔여 프레임/버퍼 정리
        self._purge_rx_frames()
        self._drain_rx_frames()

        # 3) fault 확인
        fault = await self.read_fault_code()

        # OUTPUT_OFF 인데 fault 조회 자체가 안 되면,
        # 단순 재전송보다 세션 재연결이 훨씬 안전하다.
        if fault is None:
            if label == "OUTPUT_OFF":
                await self._emit_status(f"[{label}] 실패 후 fault 조회 실패 → 세션 재연결 후 재전송")
                return await self._reopen_session_for_retry(label)

            await self._emit_status(f"[{label}] 실패 후 fault 조회 실패 → 1초 대기 후 재전송")
            await asyncio.sleep(1.0)
            return True

        # fault=0 이어도 OUTPUT_OFF 에서 명시적 ERR(04)가 왔다면
        # 장비 fault가 아니라 세션 꼬임/타이밍 문제일 수 있으므로 재연결을 우선
        if fault == 0:
            if label == "OUTPUT_OFF" and resp is not None and len(resp) == 1 and resp[0] == 0x04:
                await self._emit_status(f"[{label}] fault=0 이지만 ERR(04) 지속 → 세션 재연결 후 재전송")
                return await self._reopen_session_for_retry(label)

            await self._emit_status(f"[{label}] 실패 후 fault=0 → 1초 대기 후 재전송")
            await asyncio.sleep(1.0)
            return True

        # 4) 실제 fault면 reset 시도
        await self._emit_status(f"[{label}] 실패 후 fault=0x{fault:04X} → FAULT_RESET 후 재전송")
        ok_reset = await self.fault_reset()
        if not ok_reset:
            await self._emit_status(f"[{label}] FAULT_RESET 실패 → 재전송 중단")
            return False

        await asyncio.sleep(1.0)
        return True

    # ====== 내부: 명령 헬퍼 ======
    def _ok_from_resp(self, resp: Optional[bytes], *, label: str = "") -> bool:
        if label in ("OUTPUT_ON", "OUTPUT_OFF"):
            # 출력 on/off 는 반드시 1바이트 ACK(0x06)만 성공으로 인정
            return bool(resp) and len(resp) == 1 and resp[0] == 0x06
    
        # ✅ 모든 쓰기 명령의 정상 응답은 ACK(0x06) 1바이트뿐
        return bool(resp) and len(resp) == 1 and resp[0] == 0x06
    
    # ===================== 실패시 검증하는 로직 =====================
    # ✅ 추가: 수신 프레임 큐 비우기
    def _purge_rx_frames(self, max_n: int = 32) -> None:
        if not hasattr(self, "_frame_q"):  # 방어
            return
        for _ in range(max_n):
            try:
                self._frame_q.get_nowait()
            except Exception:
                break

    async def _write_cmd_data(self, cmd: int, value: int, width: int, *, label: str) -> bool:
        """
        ✅ 신규 정책:
        - write는 워커 blind retry(NAK 반복) 대신,
        1회 전송 → 실패 즉시 fault read/reset 판단 → 재전송
        이 사이클을 총 DCP_RECOVER_MAX_ATTEMPTS 회 반복.
        """
        base_label = label

        # ▶ 크리티컬 명령 전, 폴링 잠시 중지 + 수신버퍼 비우기(기존 유지)
        if base_label in ("OUTPUT_ON", "OUTPUT_OFF"):
            try:
                self.set_process_status(False)
            except Exception:
                pass
            self._purge_rx_frames()

        payload = self._proto.pack_write(cmd, value, width=width)

        last_resp: Optional[bytes] = None

        # ✅ STOP/종료 중에는 OUTPUT_OFF 외에 REG/REF/OUTPUT_ON 같은 "켜는/세팅하는" write가 절대 나가면 안 됨.
        if self._stop_guard and base_label not in ("OUTPUT_OFF",):
            await self._emit_status(f"[{base_label}] STOP_GUARD active → skip write")
            return False

        for attempt in range(1, self._recover_max_attempts + 1):
            attempt_label = f"{base_label}[{attempt}/{self._recover_max_attempts}]"

            # STOP 중간에 가드가 켜지면(예: 다른 태스크가 OUTPUT_OFF 호출), 이미 진입한 write도 즉시 중단
            if self._stop_guard and base_label not in ("OUTPUT_OFF",):
                await self._emit_status(f"[{base_label}] STOP_GUARD active → abort attempts")
                return False

            # 시도 전 RX 잔여 제거(혼선 방지)
            self._purge_rx_frames()

            fut = asyncio.get_running_loop().create_future()
            def _cb(resp: Optional[bytes]):
                if not fut.done():
                    fut.set_result(resp)

            # ✅ write는 워커 재시도 0 (1회 전송)
            self._enqueue(Command(
                payload, attempt_label,
                self._timeout_ms, self._gap_ms,
                self._write_worker_retries,
                _cb
            ))

            resp = await self._await_reply_bytes(
                attempt_label, fut,
                timeout_ms=self._timeout_ms,
                retries=self._write_worker_retries,
                gap_ms=self._gap_ms
            )
            last_resp = resp

            # ---- OUTPUT_ON/OFF 특수 처리(기존 판정 로직 최대한 유지) ----
            if base_label in ("OUTPUT_ON", "OUTPUT_OFF"):
                intended_on = (base_label == "OUTPUT_ON")
                ack_ok = bool(resp and len(resp) == 1 and resp[0] == 0x06)

                # ===== OUTPUT_ON =====
                if intended_on:
                    if ack_ok:
                        self._out_on = True
                        await self._emit_confirmed(base_label)
                        with contextlib.suppress(Exception):
                            await asyncio.sleep(self._activation_check_delay_s)
                        self.set_process_status(True)
                        return True

                    # ACK 미수신이어도 상태로 ON 확인되면 성공(기존 유지)
                    await asyncio.sleep(0.08)
                    ver = await self._verify_output_state()
                    if ver is True:
                        self._out_on = True
                        await self._emit_confirmed(base_label + "_VERIFIED")
                        with contextlib.suppress(Exception):
                            await asyncio.sleep(self._activation_check_delay_s)
                        self.set_process_status(True)
                        return True

                    # 실패 → 즉시 fault 처리 후 다음 attempt로
                    if self._enable_fault_recover:
                        ok_retry = await self._recover_and_prepare_retry(base_label, resp)
                        if not ok_retry:
                            await self._emit_failed(base_label, "FAULT_RESET 실패/복구 불가")
                            self.set_process_status(False)
                            return False

                    continue  # 다음 attempt 재전송

                # ===== OUTPUT_OFF =====
                else:
                    # (1) ACK 성공이면 성공
                    if ack_ok:
                        self._out_on = False
                        self._last_ref_power_w = None
                        await self._emit_confirmed(base_label)
                        self.set_process_status(False)
                        return True

                    # (2) ACK 없어도 STATUS로 HV Off면 성공
                    try:
                        flags = await self.read_status_flags()
                        if flags is not None and (not self._hv_on_from_status(flags)):
                            self._out_on = False
                            await self._emit_confirmed(base_label + "_VERIFIED")
                            self.set_process_status(False)
                            return True
                    except Exception:
                        pass

                    # (3) P==0 또는 HV Off면 성공(기존 quick confirm)
                    ok_off, p, hv_on = await self._confirm_off_quick()
                    if ok_off:
                        self._out_on = False
                        await self._emit_confirmed(base_label + "_VERIFIED")
                        self.set_process_status(False)
                        return True

                    # 실패 → 즉시 fault 처리 후 다음 attempt로
                    if self._enable_fault_recover:
                        ok_retry = await self._recover_and_prepare_retry(base_label, resp)
                        if not ok_retry:
                            await self._emit_failed(base_label, "FAULT_RESET 실패/복구 불가")
                            self.set_process_status(False)
                            return False

                    continue  # 다음 attempt 재전송

            # ---- 일반 write(REF_POWER, REG_POWER 등) ----
            ok = self._ok_from_resp(resp, label=base_label)
            if ok:
                await self._emit_confirmed(base_label)
                return True

            # 실패 → 즉시 fault 처리 후 다음 attempt로
            if self._enable_fault_recover:
                ok_retry = await self._recover_and_prepare_retry(base_label, resp)
                if not ok_retry:
                    await self._emit_failed(base_label, "FAULT_RESET 실패/복구 불가")
                    return False

            # fault=0 또는 fault 읽기 실패면 reset 없이 다음 attempt로 재전송
            continue

        # 여기까지 왔으면 총 시도 횟수 소진
        if base_label == "OUTPUT_OFF":
            await self._emit_failed(
                base_label,
                f"OUTPUT_OFF 미확인 — 총 {self._recover_max_attempts}회 시도, last={last_resp!r} (출력 상태 미확인)"
            )
        else:
            await self._emit_failed(
                base_label,
                f"응답 없음/실패 — 총 {self._recover_max_attempts}회 시도, last={last_resp!r}"
            )

        if base_label == "OUTPUT_ON":
            self.set_process_status(False)
        if base_label == "OUTPUT_OFF":
            self.set_process_status(False)
        return False
        
    # ❶ [ADD] RS-232 payload 분해 헬퍼: [CMD][DATA...][(ETX?)][CHK] → (cmd, data, chk)
    def _unpack_rs232_payload(self, resp: bytes):
        if not resp or len(resp) < 2:
            return None, b"", None
        cmd = resp[0]
        # ✅ [FIX] 파서(_tcp_reader_loop)가 이미 STX/ETX/CHK를 제거하고 CMD+DATA만 큐에 넣는다.
        #    여기서 끝의 0x03을 또 벗기면 하위바이트가 3인 값(STATUS 0x0003=HV ON, FAULT 0x0003 등)이
        #    0x0000으로 붕괴되어 HV/fault를 오판한다. → trailing 0x03 제거 금지.
        data = resp[1:]
        return cmd, data, None   # ✅ CHK는 원래 큐에 안 들어오므로 None

    # ❷ [ADD] 1B/2B 데이터 모두 수용하는 플래그 추출
    def _flags16_from_data(self, data: bytes) -> int | None:
        if not data:
            return None
        if len(data) >= 2:
            return ((data[-2] << 8) | data[-1]) & 0xFFFF
        return data[-1] & 0xFF

    # ❸ [REPLACE] READ_STATUS 파싱: CHK 제외 + 1B/2B 모두 처리
    async def read_status_flags(self) -> Optional[int]:
        """
        상태 플래그(0x90)를 읽어오는 보조 헬퍼.

        ⚠ 중요:
        - 이 함수는 OUTPUT_ON/OFF 성공 여부를 '보조로' 확인하는 용도이기 때문에,
          여기서 직접 command_failed 이벤트를 쏘지 않는다.
        - 읽기에 실패하면 로그만 남기고 None 을 리턴하고,
          최종 성공/실패 판정은 호출 측(_verify_output_state, _confirm_off_quick)에서 한다.
        """
        # 0x90 응답이 나올 때까지 짧게 3회 재시도 (중간 0x9A 등은 무시)
        for attempt in range(3):
            resp = await self._read_raw(0x90, "READ_STATUS")

            # 응답이 없거나 너무 짧으면 소프트 에러로만 기록하고 재시도
            if not resp or len(resp) < 2:
                # 예: NAK 후 세션 재시작 등으로 인해 payload 가 비었을 수 있음
                await self._emit_status(
                    f"READ_STATUS: 응답 없음/길이 부족({resp!r}) → 재시도({2 - attempt})"
                )
                await asyncio.sleep(0.03)
                continue

            cmd, data, chk = self._unpack_rs232_payload(resp)
            if cmd == 0x90:
                flags = self._flags16_from_data(data)
                if flags is None:
                    # 데이터가 비정상이면 역시 소프트 로그만 남기고 실패로 보고 종료
                    await self._emit_status(
                        f"READ_STATUS: 데이터 없음: raw={resp.hex(' ')}"
                    )
                    return None
                return flags

            # ❗ 0x90이 아닌 프레임(예: 0x9A)은 스팬으로 들어온 읽기 결과이므로 무시하고 재시도
            await self._emit_status(
                f"READ_STATUS: 다른 프레임(0x{cmd:02X}) 수신 → 무시하고 재시도"
            )
            await asyncio.sleep(0.03)

        # 여기까지 왔다는 것은 여러 번 시도했지만 플래그를 못 읽었다는 뜻.
        # 하지만 이것만으로 공정을 '실패'로 보지는 않고, 호출 측에서 판단하게 둔다.
        await self._emit_status(
            "READ_STATUS: 연속 실패로 STATUS 플래그 확인 불가 (non-fatal)"
        )
        return None

    @staticmethod
    def _hv_on_from_status(flags: int) -> bool:
        # 매뉴얼 표기: f0 nibble = SetPoint | Ramp | START | HV On (LSB)
        return bool(flags & 0x0001)

    async def _verify_output_state(self) -> Optional[bool]:
        flags = await self.read_status_flags()
        if flags is None:
            return None
        return self._hv_on_from_status(flags)
    
    async def _confirm_off_quick(self) -> tuple[bool, Optional[float], Optional[bool]]:
        """
        OUTPUT_OFF 후 빠른 교차 확인:
        - 주판정: READ_STATUS(0x90) → HV Off이면 OK
        - 보조판정: STATUS 확인 불가일 때만 READ_PIV(0x9A) → P==0W이면 OK
        반환: (ok, P_W or None, hv_on or None)
        """
        # 1) 먼저 상태(HV On) 확인
        flags = await self.read_status_flags()
        hv_on = None
        if flags is not None:
            hv_on = self._hv_on_from_status(flags)
            if hv_on is False:
                return True, None, False

        # 2) STATUS 확인이 안 되거나 아직 HV On이면, 보조로 실제 전력(P) 확인
        piv = await self.read_output_piv()
        p = None
        if piv and "eng" in piv:
            try:
                p = float(piv["eng"].get("P_W", 0.0))
            except Exception:
                p = None

        ok = (p is not None and p == 0.0)
        return ok, p, hv_on

    # ===================== 실패시 검증하는 로직 =====================
    async def _write_simple(self, code: int, label: str):
        fut = asyncio.get_running_loop().create_future()
        def _cb(_resp: Optional[bytes]):
            if not fut.done():
                fut.set_result(_resp)

        payload = self._proto.pack_write(code, None, width=0)
        retries = int(self._cmd_max_retries)
        self._enqueue(Command(payload, label, int(self._timeout_ms), int(self._gap_ms), retries, _cb))
        resp = await self._await_reply_bytes(
            label, fut,
            timeout_ms=int(self._timeout_ms),
            retries=retries,
            gap_ms=int(self._gap_ms)
        )

        if self._ok_from_resp(resp):
            await self._emit_confirmed(label)
        else:
            await self._emit_failed(label, "응답 없음/실패")

    async def _await_reply_bytes(
        self,
        label: str,
        fut: "asyncio.Future[Optional[bytes]]",
        *,
        timeout_ms: int,
        retries: int,
        gap_ms: int,
        extra_timeout_s: float = 0.0,
    ) -> Optional[bytes]:
        # 오픈 직후 여유
        extra = 0.0
        if self._last_connect_mono > 0.0 and (time.monotonic() - self._last_connect_mono) < 2.0:
            extra = self._first_cmd_extra_timeout_ms / 1000.0

        # 워커 쪽 per-attempt 대기 시간(현재 워커도 동일 계산 사용)
        per_attempt_s = (timeout_ms / 1000.0) + 2.0

        # ✅ retries_left 만큼 실제로 재시도하는 구조이므로 호출자도 그 총합을 기다려야 함
        # 시도 횟수 = 1 + retries
        total_s = (retries + 1) * (per_attempt_s + (gap_ms / 1000.0)) + extra + extra_timeout_s

        try:
            resp = await asyncio.wait_for(fut, timeout=total_s)

            if resp is not None:
                if len(resp) == 1 and resp[0] in (0x06, 0x04):
                    name = "ACK" if resp[0] == 0x06 else "ERR"
                    await self._emit_status(f"[RECV] {label} ← {name}({resp.hex(' ')})")
                else:
                    await self._emit_status(f"[RECV] {label} ← {resp.hex(' ')}")

            return resp

        except asyncio.TimeoutError:
            await self._emit_status(f"[TIMEOUT] {label} (total≈{total_s:.1f}s) → 세션 재시작")
            self._on_tcp_disconnected()
            return None

    # ====== 내부: 연결/워치독/워커/리더 ======
    async def _watchdog_loop(self):
        backoff = int(self._reconnect_backoff_start_ms)
        while self._want_connected:
            if self._connected:
                await asyncio.sleep(float(self._watchdog_interval_ms) / 1000.0)
                continue
            if self._ever_connected:
                await self._emit_status(f"재연결 예약... ({backoff} ms)")
                await asyncio.sleep(backoff / 1000.0)
            if not self._want_connected:
                break
            try:
                host, port = self._resolve_endpoint()
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=float(self._connect_timeout_s)
                )
                self._reader, self._writer = reader, writer
                self._connected = True
                self._ever_connected = True
                backoff = int(self._reconnect_backoff_start_ms)

                # ★ Keepalive는 설정에 따름(기본 False 권장)
                try:
                    sock = writer.get_extra_info("socket")
                    if sock is not None:
                        if bool(self._tcp_keepalive):
                            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                        else:
                            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 0)
                except Exception:
                    pass

                # reader task
                if self._reader_task and not self._reader_task.done():
                    self._reader_task.cancel()
                    with contextlib.suppress(Exception):
                        await self._reader_task
                self._reader_task = asyncio.create_task(self._tcp_reader_loop(), name="DCP-TcpReader")
                # ★ 연결 직후 IO 타임스탬프 초기화
                self._last_connect_mono = time.monotonic()
                self._last_io_mono = self._last_connect_mono
                self._just_reopened = True
                await self._emit_status(f"{host}:{port} 연결 성공 (TCP)")
            except Exception as e:
                host, port = self._resolve_endpoint()
                await self._emit_status(f"{host}:{port} 연결 실패: {e}")
                backoff = min(backoff * 2, int(self._reconnect_backoff_max_ms))

    def _on_tcp_disconnected(self):
        self._connected = False
        if self._reader_task:
            self._reader_task.cancel()
        self._reader_task = None

        if self._writer:
            with contextlib.suppress(Exception):
                self._writer.close()
            # 동기 컨텍스트라 await 불가 → transport.abort()로 즉시 끊기
            transport = getattr(self._writer, "transport", None)
            if transport:
                with contextlib.suppress(Exception):
                    transport.abort()

        self._reader = None
        self._writer = None

        # 프레임 큐 비움 (정상 종료)
        while True:
            try:
                self._frame_q.get_nowait()
            except asyncio.QueueEmpty:
                break

        # 세션/타이밍 플래그 리셋
        self._just_reopened = False
        self._last_io_mono = 0.0

        self._dbg("DCP", "연결 끊김")
        # inflight 복구/취소
        if self._inflight is not None:
            cmd = self._inflight
            self._inflight = None
            if cmd.retries_left > 0:
                cmd.retries_left -= 1
                self._cmd_q.appendleft(cmd)
            else:
                self._safe_callback(cmd.callback, None)

    async def _cmd_worker_loop(self):
        while True:
            await asyncio.sleep(0)
            if not self._cmd_q:
                await asyncio.sleep(0.01); continue
            if not self._connected or not self._writer:
                await asyncio.sleep(0.05); continue

            cmd = self._cmd_q.popleft()
            self._inflight = cmd

            # ✅ [FIX] 최소 인터커맨드 간격 보장 (매뉴얼 권장: 통신주기 100ms 이상)
            #    기존에는 gap_ms가 timeout 계산에만 쓰이고 실제 대기가 없어 연속 전송됨
            #    → 장비 수신 파서 과부하/NAK 유발. rf_pulse.py와 동일 방식으로 적용.
            _gap_need = (cmd.gap_ms / 1000.0) - (time.monotonic() - self._last_send_mono)
            if _gap_need > 0:
                await asyncio.sleep(_gap_need)

            # ▶ 송신 바이트(hex)까지 함께 기록
            await self._emit_status(f"[SEND] {cmd.label} → {cmd.payload.hex(' ')}")
            
            # ★ 전송 직전 유휴/세션 프리플라이트
            await self._reopen_if_inactive()
            if not self._connected or not self._writer:
                # 아직 워치독이 다시 붙지 못했으면 되돌리고 잠깐 쉼
                self._cmd_q.appendleft(cmd)
                self._inflight = None
                await asyncio.sleep(0.15)
                continue

            # 연결 직후 quiet 기간
            if self._just_reopened and self._last_connect_mono > 0.0:
                remain = (self._last_connect_mono + self._post_open_quiet_s) - time.monotonic()
                if remain > 0:
                    await asyncio.sleep(remain)
                self._just_reopened = False

            # 전송
            try:
                self._last_io_mono = time.monotonic()   # ★ 송신 직전 IO 시각
                self._last_send_mono = self._last_io_mono   # ✅ gap 기준 시각 갱신
                self._writer.write(cmd.payload)
                await asyncio.wait_for(self._writer.drain(), timeout=self._drain_timeout_s)
            except Exception as e:
                self._dbg("DCP", f"{cmd.label} 전송 오류: {e}")
                self._inflight = None
                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    self._cmd_q.appendleft(cmd)
                else:
                    self._safe_callback(cmd.callback, None)
                self._on_tcp_disconnected()
                continue

            # === 응답 대기: '자신의 응답'만 인정 ===
            extra = 0.0
            if self._last_connect_mono > 0.0 and (time.monotonic() - self._last_connect_mono) < 2.0:
                extra = self._first_cmd_extra_timeout_ms / 1000.0

            deadline = time.monotonic() + (cmd.timeout_ms/1000.0) + 2.0 + extra
            exp_cmd = cmd.payload[1] if len(cmd.payload) >= 2 else None
            is_read  = cmd.label.startswith("READ_")

            try:
                frame: Optional[bytes] = None
                while True:
                    remain = deadline - time.monotonic()
                    if remain <= 0:
                        raise asyncio.TimeoutError()

                    f = await self._read_one_frame(remain)

                    if not is_read:
                        # 쓰기(예: OUTPUT_ON/OFF): 1바이트 ACK/ERR만 응답으로 인정
                        if len(f) == 1 and f[0] in (0x06, 0x04):
                            frame = f
                            break
                        # 그 외(예: 0x9A 텔레메트리, 과거 읽기 잔여 등)는 무시
                        continue
                    else:
                        # 읽기: 요청 CMD와 동일한 프레임 또는 NAK(0x04)만 인정
                        if len(f) == 1 and f[0] == 0x04:   # NAK → 기존 재시도 로직으로
                            frame = f
                            break
                        if exp_cmd is not None and len(f) >= 1 and f[0] == exp_cmd:
                            frame = f
                            break
                        continue

                # inflight clear + 콜백
                self._inflight = None
                self._safe_callback(cmd.callback, frame)

            except asyncio.TimeoutError:
                await self._emit_status(f"[TIMEOUT] {cmd.label} → 세션 재시작")
                self._inflight = None
                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    self._cmd_q.appendleft(cmd)
                else:
                    self._safe_callback(cmd.callback, None)
                self._on_tcp_disconnected()
            except Exception as e:
                await self._emit_status(f"[cmd] 예외: {e!r}")
                self._inflight = None
                self._safe_callback(cmd.callback, None)
                self._on_tcp_disconnected()

    async def _tcp_reader_loop(self):
        assert self._reader is not None
        buf = bytearray()
        RX_MAX = 32 * 1024
        try:
            while self._connected and self._reader:
                chunk = await self._reader.read(256)
                if not chunk:
                    break
                self._last_io_mono = time.monotonic()   # ★ 수신 시각 갱신
                buf.extend(chunk)
                if len(buf) > RX_MAX:
                    del buf[:-RX_MAX]

                # === 프레임 파서: STX(0x02) .. ETX(0x03) + CHK(1B) ===
                while True:
                    # 0) 먼저 선두의 에코(ACK/ERR)를 처리 (RS-232: 1바이트)
                    emitted = False
                    while buf and buf[0] in (0x06, 0x04):
                        b = buf[0]
                        try:
                            self._last_io_mono = time.monotonic()     # ★
                            self._frame_q.put_nowait(bytes([b]))
                        except asyncio.QueueFull:
                            with contextlib.suppress(Exception):
                                _ = self._frame_q.get_nowait()
                            self._frame_q.put_nowait(bytes([b]))
                        del buf[0]
                        emitted = True

                    if emitted:
                        # 에코를 하나 이상 내보냈으면 다시 루프 돌며 추가 에코/프레임을 검사
                        continue

                    # 1) STX(0x02) 위치 찾기
                    try:
                        i_stx = buf.index(0x02)
                    except ValueError:
                        # STX가 아예 없으면, 버퍼 안에 섞여 들어온 에코 바이트(0x06/0x04)를 걷어내서 전달
                        i = 0; found_echo = False
                        while i < len(buf):
                            if buf[i] in (0x06, 0x04):
                                try:
                                    self._frame_q.put_nowait(bytes([buf[i]]))
                                except asyncio.QueueFull:
                                    with contextlib.suppress(Exception):
                                        _ = self._frame_q.get_nowait()
                                    self._frame_q.put_nowait(bytes([buf[i]]))
                                del buf[i]
                                found_echo = True
                                continue
                            i += 1
                        if not found_echo:
                            buf.clear()
                        break

                    # STX 앞쪽 프리픽스에도 혹시 에코가 섞였으면 살려서 올리고 나머지는 버린다
                    if i_stx > 0:
                        prefix = bytes(buf[:i_stx])
                        # prefix 안의 0x06/0x04만 추려서 방출
                        for b in prefix:
                            if b in (0x06, 0x04):
                                try:
                                    self._frame_q.put_nowait(bytes([b]))
                                except asyncio.QueueFull:
                                    with contextlib.suppress(Exception):
                                        _ = self._frame_q.get_nowait()
                                    self._frame_q.put_nowait(bytes([b]))
                        del buf[:i_stx]

                    # 2) ETX 위치 결정 — [EL: READ 응답 데이터에 0x03 포함 시(예: soft arc≥768,
                    #    arc=3, PIV의 P raw=3 등) 첫 0x03을 ETX로 오인 → CHKFAIL/프레임 유실 버그 수정]
                    #    ① 길이 규칙 우선: 읽기 응답 데이터 = 0x9A(PIV) 6B, 그 외 2B
                    #    ② 불일치 시 체크섬 유도 탐색(0x03 후보 중 CHK 일치, ETX idx ≤ 8)
                    #    ③ 후보 없음: 미완성 → 추가 수신 대기 / 10B 이상 정체 시 1B 재동기
                    i_etx = -1
                    if len(buf) >= 2:
                        _dlen = 6 if buf[1] == 0x9A else 2
                        _pos = 2 + _dlen
                        if len(buf) >= _pos + 2:
                            if buf[_pos] == 0x03 and (_chk_nibble_sum(bytes(buf[:_pos + 1])) & 0xFF) == (buf[_pos + 1] & 0xFF):
                                i_etx = _pos
                            else:
                                _j = 2
                                while _j <= min(8, len(buf) - 2):
                                    try:
                                        _j = buf.index(0x03, _j)
                                    except ValueError:
                                        break
                                    if _j > 8 or len(buf) < _j + 2:
                                        break
                                    if (_chk_nibble_sum(bytes(buf[:_j + 1])) & 0xFF) == (buf[_j + 1] & 0xFF):
                                        i_etx = _j
                                        break
                                    _j += 1
                    if i_etx < 0:
                        if len(buf) >= 10:
                            self._ev_nowait(DCPEvent(
                                kind="status",
                                message=f"[CHKFAIL] resync: head={bytes(buf[:10]).hex(' ')}"
                            ))
                            del buf[0]
                            continue
                        break

                    core = bytes(buf[:i_etx + 1])   # STX..ETX
                    chk  = buf[i_etx + 1]

                    expect = _chk_nibble_sum(core) & 0xFF
                    got    = chk & 0xFF

                    if expect == got:
                        # RS-232: payload = CMD + DATA.. (STX/ETX 제외)
                        payload = core[1:-1]
                        try:
                            self._last_io_mono = time.monotonic()     # ★
                            self._frame_q.put_nowait(payload)
                        except asyncio.QueueFull:
                            self._dbg("DCP", "프레임 큐 포화 → 가장 오래된 프레임 폐기")
                            with contextlib.suppress(Exception):
                                _ = self._frame_q.get_nowait()
                            with contextlib.suppress(Exception):
                                self._frame_q.put_nowait(payload)
                    else:
                        # ✅ 디버그 여부와 상관없이 이벤트 로그로 남김
                        self._ev_nowait(DCPEvent(
                            kind="status",
                            message=f"[CHKFAIL] core={core.hex(' ')} recv_chk={got:02X} expect={expect:02X}"
                        ))
                        # 추가 디버그 로그(선택): DEBUG_PRINT=True일 때 콘솔에도 출력
                        self._dbg("DCP", f"CHK FAIL: core={core.hex()} recv={got:02X} expect={expect:02X}")

                    del buf[:i_etx + 2]

        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._dbg("DCP", f"리더 루프 예외: {e!r}")
        finally:
            self._on_tcp_disconnected()

    async def _read_one_frame(self, timeout_s: float) -> bytes:
        return await asyncio.wait_for(self._frame_q.get(), timeout=timeout_s)

    # ====== Poll 루프(필요 시 항목 확장) ======
    async def _poll_loop(self):
        try:
            _piv_last: float = 0.0   # 마지막 PIV 읽기 시각
            while True:
                now = time.monotonic()
                try:
                    if self._connected and self._out_on:

                        # ─── PIV 읽기 (_poll_period_s 마다) ───────────────────
                        if now - _piv_last >= self._poll_period_s:
                            _piv_last = time.monotonic()
                            res = await self.read_output_piv()
                            if not (res and "eng" in res):
                                # ✅ 산발성 NAK 완화: 0.3s 후 1회 재시도 (버스트 구간엔 무효 → 케이블 조치 필요)
                                await asyncio.sleep(0.3)
                                res = await self.read_output_piv()
                            # ✅ [EL: 2026-08-25 런4] 플라즈마 중 시리얼 NAK 간헐 폭주 →
                            #    연속 5회(≈25초) 읽기 실패 시 1회 경고(감시 공백 가시화)
                            if res and "eng" in res:
                                if getattr(self, "_poll_nak_n", 0) >= 5:
                                    await self._emit_status("[INFO] 텔레메트리 읽기 회복 (NAK 연속 종료)")
                                self._poll_nak_n = 0
                            else:
                                self._poll_nak_n = getattr(self, "_poll_nak_n", 0) + 1
                                if self._poll_nak_n == 5:
                                    await self._emit_status(
                                        "[WARN] 텔레메트리 연속 5회 NAK — DCP 시리얼 라인 노이즈 의심 "
                                        "(저전류/이탈 감시 공백 중)"
                                    )
                            if res and "eng" in res:
                                eng = res["eng"]
                                p = float(eng.get("P_W", 0.0))
                                v = float(eng.get("V_V", 0.0))
                                i = float(eng.get("I_A", 0.0))

                                # ① 저전류 감시
                                ref = float(self._last_ref_power_w or 0.0)
                                if ref > 0.0:
                                    if i <= self._i_low_thresh_a:
                                        self._low_curr_n += 1
                                        await self._emit_status(
                                            f"[WARN] 저전류 감지: I={i:.3f} A "
                                            f"({self._low_curr_n}/{self._i_low_count_max_n})"
                                        )
                                        if self._low_curr_n >= self._i_low_count_max_n:
                                            reason = (
                                                f"low_current: I <= {self._i_low_thresh_a:.3f}A "
                                                f"({self._low_curr_n}회 연속)"
                                            )
                                            fault = None
                                            with contextlib.suppress(Exception):
                                                fault = await self.read_fault_code()
                                            if fault is not None and fault != 0:
                                                reason += f", fault=0x{fault:04X}"
                                            self._ev_nowait(DCPEvent(
                                                kind="command_failed",
                                                cmd="AUTO_STOP",
                                                reason=reason,
                                                power=p, voltage=v, current=i, eng=eng,
                                            ))
                                            await self._emit_status(
                                                "[AUTO-STOP] 저전류가 연속 발생 → OUTPUT_OFF & stop polling"
                                            )
                                            with contextlib.suppress(Exception):
                                                await self.output_off()
                                            return
                                    else:
                                        if self._low_curr_n:
                                            self._low_curr_n = 0
                                else:
                                    if self._low_curr_n:
                                        self._low_curr_n = 0

                                # ② 세트포인트 근접 확인
                                if ref > 0.0:
                                    tol = max(self._p_set_tol_w, abs(ref) * self._p_set_tol_pct)
                                    if abs(p - ref) > tol:
                                        self._spdev_n += 1
                                        await self._emit_status(
                                            f"[WARN] 현재 P={p:.1f} W, Set={ref:.1f} W, Tol=±{tol:.1f} W — 세트포인트 이탈 "
                                            f"({self._spdev_n}/{self._p_set_deviate_max_n})"
                                        )
                                        if self._spdev_n >= self._p_set_deviate_max_n:
                                            self._ev_nowait(DCPEvent(
                                                kind="command_failed",
                                                cmd="AUTO_STOP",
                                                reason="target_failed",
                                                power=p, voltage=v, current=i, eng=eng,
                                            ))
                                            await self._emit_status(
                                                "[AUTO-STOP] 세트포인트 이탈이 연속 발생 → OUTPUT_OFF & stop polling"
                                            )
                                            with contextlib.suppress(Exception):
                                                await self.output_off()
                                            return
                                    else:
                                        if self._spdev_n:
                                            self._spdev_n = 0
                                else:
                                    if self._spdev_n:
                                        self._spdev_n = 0

                                # ③ 텔레메트리 이벤트 전송
                                ev = DCPEvent(
                                    kind="telemetry",
                                    data=eng, power=p, voltage=v, current=i, eng=eng,
                                )
                                self._ev_nowait(ev)
                                cb = getattr(self, "_on_telemetry", None)
                                if cb:
                                    try:
                                        cb(p, v, i)
                                    except Exception:
                                        pass

                        # ─── ARC 읽기 (PIV와 동일 주기, 장비 누적값 직접 읽기) ───
                        # PIV 블록이 실행된 직후 (now - _piv_last 가 방금 갱신됨)에 함께 읽는다.
                        # _piv_last가 방금 갱신됐으면 ARC도 읽도록 동일 조건 사용
                        if now - _piv_last >= self._poll_period_s - 0.05:
                            soft: Optional[int] = None
                            hard: Optional[int] = None
                            with contextlib.suppress(Exception):
                                soft = await self.read_soft_arc_total()
                            with contextlib.suppress(Exception):
                                hard = await self.read_hard_arc_total()
                            if soft is not None or hard is not None:
                                s = soft if soft is not None else self._soft_arc_total
                                h = hard if hard is not None else self._hard_arc_total
                                # 장비 누적값으로 갱신
                                if soft is not None:
                                    self._soft_arc_total = soft
                                if hard is not None:
                                    self._hard_arc_total = hard
                                self._ev_nowait(DCPEvent(
                                    kind="status",
                                    message=f"[arc] Soft(total)={s} Hard(total)={h}"
                                ))
                                # 임계값 도달 시 1회만 이벤트 emit
                                thresh = int(getattr(self, "_arc_alert_thresh", 5))
                                if (s + h) >= thresh and not getattr(self, "_arc_alert_sent", False):
                                    self._arc_alert_sent = True
                                    self._ev_nowait(DCPEvent(
                                        kind="arc_threshold_reached",
                                        message=f"Arc 누적 임계값 도달 (Soft={s}, Hard={h}, 합계={s+h})",
                                        power=float(s),
                                        voltage=float(h),
                                    ))

                    else:
                        # 연결이 없거나 출력 OFF 상태면 카운터·타임스탬프 리셋
                        _piv_last = 0.0
                        if self._spdev_n:
                            self._spdev_n = 0
                        if self._low_curr_n:
                            self._low_curr_n = 0

                except Exception as e:
                    self._ev_nowait(DCPEvent(kind="status", message=f"[poll] 예외: {e!r}"))

                # ─── sleep: PIV 주기에 맞춰 깨어남 (ARC는 PIV와 동일 주기)
                now3 = time.monotonic()
                next_piv = max(0.0, _piv_last + self._poll_period_s - now3)
                await asyncio.sleep(max(0.05, next_piv))

        except asyncio.CancelledError:
            pass

    # ====== 내부 유틸 ======
    def _drain_rx_frames(self, max_n: int = 128) -> int:
        """응답 직전, RX 프레임 큐 잔여물을 비워 상관관계 혼선 방지."""
        n = 0
        try:
            while n < max_n:
                self._frame_q.get_nowait()
                n += 1
        except asyncio.QueueEmpty:
            pass
        return n

    def set_telemetry_callback(self, cb: Optional[Callable[[float, float, float], None]]) -> None:
        self._on_telemetry = cb

    def _resolve_endpoint(self) -> tuple[str, int]:
        host = self._override_host if self._override_host else self._cfg_get("DCPULSE_TCP_HOST", "192.168.1.50")
        port = self._override_port if self._override_port else self._cfg_int("DCPULSE_TCP_PORT", 4007)
        return str(host), int(port)

    def _enqueue(self, cmd: Command):
        self._cmd_q.append(cmd)

    def _safe_callback(self, cb: Optional[Callable[[Optional[bytes]], None]], arg: Optional[bytes]):
        if cb is None:
            return
        try:
            cb(arg)
        except Exception as e:
            self._dbg("DCP", f"콜백 오류: {e}")

    async def _emit_status(self, msg: str):
        if self.debug_print:
            print(f"[DCP][status] {msg}")
        await self._event_q.put(DCPEvent(kind="status", message=msg))

    async def _emit_confirmed(self, label: str):
        await self._event_q.put(DCPEvent(kind="command_confirmed", cmd=label))

    async def _emit_failed(self, label: str, why: str):
        await self._event_q.put(DCPEvent(kind="command_failed", cmd=label, reason=why))

    def _ev_nowait(self, ev: DCPEvent):
        try:
            self._event_q.put_nowait(ev)
        except Exception:
            pass

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

    def _base_cmd_label(self, label: str) -> str:
        return str(label or "").split("[", 1)[0].strip().upper()

    def _is_poll_read_label(self, label: str) -> bool:
        # 현재 poll loop가 주기적으로 넣는 읽기는 READ_PIV, READ_ARC_SOFT, READ_ARC_HARD
        return self._base_cmd_label(label) in {"READ_PIV", "READ_ARC_SOFT", "READ_ARC_HARD",
                                        "READ_ARC_TOTAL_SOFT", "READ_ARC_TOTAL_HARD"}

    def _purge_pending(
        self,
        reason: str = "",
        *,
        only_poll_reads: bool = False,
        drop_inflight: bool = True,
    ) -> int:
        purged = 0

        # inflight는 polling off에서 건드리지 않는다.
        if drop_inflight and self._inflight is not None:
            cmd = self._inflight
            if (not only_poll_reads) or self._is_poll_read_label(cmd.label):
                self._inflight = None
                purged += 1
                self._safe_callback(cmd.callback, None)

        kept = deque()
        while self._cmd_q:
            c = self._cmd_q.popleft()

            if (not only_poll_reads) or self._is_poll_read_label(c.label):
                purged += 1
                self._safe_callback(c.callback, None)
            else:
                kept.append(c)

        self._cmd_q = kept

        if reason:
            self._ev_nowait(DCPEvent(kind="status", message=f"대기 중 명령 {purged}개 폐기 ({reason})"))
        return purged

    def _dbg(self, src: str, msg: str):
        if self.debug_print:
            print(f"[{src}] {msg}")

    # =========== chamber_runtime.py에 맞춘 함수들 ===========
    def is_connected(self) -> bool:
        """프리플라이트/상태 체크용: 현재 TCP 연결 여부."""
        return bool(self._connected)
    
    async def cleanup_quick(self):
        """빠른 종료 경로(현재는 cleanup과 동일)."""
        await self.cleanup()

    async def pause_watchdog(self) -> None:
        """워치독(자동 재연결) 일시 중지 — 기존 연결은 유지."""
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
        """pause_watchdog 이후 워치독 재개."""
        if self._watchdog_task and not self._watchdog_task.done():
            return
        self._want_connected = True
        loop = asyncio.get_running_loop()
        self._watchdog_task = loop.create_task(self._watchdog_loop(), name="DCPWatchdog")
    # =========== chamber_runtime.py에 맞춘 함수들 ===========

    async def _reopen_if_inactive(self):
        """
        보내기 직전에 유휴시간 초과/세션 이상을 점검하고 필요 시 즉시 세션을 내려
        워치독이 재연결하도록 만든다.
        """
        # 세션 자체가 없거나 닫혔으면 즉시 정리
        if not self._writer or self._writer.is_closing() or not self._connected:
            self._on_tcp_disconnected()
            return

        # 유휴 초과면 세션 재시작
        if self._inactivity_s > 0:
            idle = time.monotonic() - (self._last_io_mono or 0.0)
            if idle >= self._inactivity_s:
                await self._emit_status(f"[DCP] idle {idle:.1f}s ≥ {self._inactivity_s:.1f}s → 세션 재시작")
                self._on_tcp_disconnected()


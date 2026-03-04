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
from lib import config_common as cfgc
cfg_default = cfgc  # AsyncDCPulse 기본 cfg (원하면 ch1/ch2 모듈을 넘겨서 채널별 파라미터 사용)

# ======================================================================
# 런타임 파라미터 기본값
# - UI에서 config 값을 바꿀 수 있도록, 장비 파라미터는 "모듈 전역 상수"로 고정하지 않고
#   AsyncDCPulse 인스턴스가 config를 읽어 런타임 필드로 유지한다.
# - config에 키가 없으면 아래 DEFAULT_* 값이 사용된다.
# ======================================================================
DEFAULT_DCPULSE_TCP_HOST = "192.168.1.50"
DEFAULT_DCPULSE_TCP_PORT = 4007

DEFAULT_DCP_P_SET_TOL_PCT = 0.05
DEFAULT_DCP_P_SET_TOL_W = 15.0
DEFAULT_DCP_P_SET_DEVIATE_MAX_N = 3

DEFAULT_DCP_I_LOW_THRESH_A = 0.05
DEFAULT_DCP_I_LOW_COUNT_MAX_N = 3

DEFAULT_DCP_CMD_MAX_RETRIES = 5
DEFAULT_DCP_RECOVER_MAX_ATTEMPTS = 5
DEFAULT_DCP_WRITE_WORKER_RETRIES = 0
DEFAULT_DCP_ENABLE_FAULT_RECOVER = True

DEFAULT_DCP_ACTIVATION_CHECK_DELAY_S = 5.0
DEFAULT_DCP_POLL_INTERVAL_S = 5.0
DEFAULT_DCP_CONNECT_TIMEOUT_S = 3.0

DEFAULT_DCP_TIMEOUT_MS = 2500
DEFAULT_DCP_GAP_MS = 1000
DEFAULT_DCP_WATCHDOG_INTERVAL_MS = 1000
DEFAULT_DCP_RECONNECT_BACKOFF_START_MS = 1000
DEFAULT_DCP_RECONNECT_BACKOFF_MAX_MS = 10000
DEFAULT_DCP_FIRST_CMD_EXTRA_TIMEOUT_MS = 2000
DEFAULT_DCP_POST_OPEN_QUIET_S = 0.8
DEFAULT_DCP_DRAIN_TIMEOUT_S = 1.0

DEFAULT_DCP_INACTIVITY_REOPEN_S = 60.0
DEFAULT_DCP_TCP_KEEPALIVE = False

# ===== 통일된 스케일 상수 =====
# (측정 raw -> 공학단위) 한 LSB가 얼마인지
V_MEAS_V_PER_LSB = 1.468815 # 1 count ≈ 1.5 V  (매뉴얼 표준)
I_MEAS_A_PER_LSB = 0.01     # 1 count = 0.01 A  (전류 10배 과다표시 교정)
P_MEAS_W_PER_LSB = 10.0     # 1 count = 10 W

RAMP_MS_PER_LSB  = 1.0      # 1 count = 1 ms
ARC_US_PER_LSB   = 1.0      # 1 count = 1 us

# (설정 공학단위 -> raw) 한 스텝 크기
V_SET_STEP_V = 1.0          # 1 step = 1 V
I_SET_STEP_A = 0.1          # 1 step = 0.1 A
P_SET_STEP_W = 10.0         # 1 step = 10 W  (기존 POWER_SET_STEP_W)

# 장비 정격(예: 1 kW면 1000)
MAX_POWER_W = 1000          # → 10 W/step 기준 0..100 step

DEBUG_PRINT = False

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
    def __init__(self, *, host: Optional[str] = None, port: Optional[int] = None,
                 protocol: Optional[IProtocol] = None,
                 on_telemetry: Optional[Callable[[float, float, float], None]] = None,
                 cfg=None):
        # ===== config 소스 =====
        # - cfg: 채널별(config_ch1/config_ch2 등) 모듈을 넘겨주면 그 값을 1순위로 사용
        # - cfgc(config_common): 공통 기본값/공통 키(2순위)
        self._cfg = cfg if cfg is not None else cfg_default

        def _cfg_get(name: str, default=None):
            if hasattr(self._cfg, name):
                return getattr(self._cfg, name)
            if hasattr(cfgc, name):
                return getattr(cfgc, name)
            return default

        self._cfg_get = _cfg_get

        # Endpoint override
        self._override_host = host
        self._override_port = port

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
        self.debug_print = DEBUG_PRINT

        # ↓↓↓ 추가: 측정값 알림용 콜백 (DataLogger.log_dcpulse_power 연결)
        self._on_telemetry = on_telemetry

        self._last_io_mono: float = 0.0

        self._out_on: bool = False                 # 출력 ON/OFF 내부 기억
        self._last_ref_power_w: Optional[float] = None  # ← 세트포인트 저장

        self._spdev_n: int = 0                     # ← 연속 세트포인트 이탈 카운터
        self._low_curr_n: int = 0                  # ← 연속 저전류(I<=0.05A) 카운터

        # ✅ STOP/종료 중에 ON/SET 계열 write 재전송을 막기 위한 가드
        self._stop_guard: bool = False

        # ✅ UI에서 config 값을 바꾼 뒤, 이 메서드를 다시 호출하면 런타임에 반영됨
        #    (chamber_runtime 쪽에서 config 저장 직후 dcp.reload_runtime_cfg() 호출 권장)
        self.reload_runtime_cfg()

    # ===== config 헬퍼 =====
    def _cfg_int(self, name: str, default: int) -> int:
        v = self._cfg_get(name, default)
        try:
            return int(v)
        except Exception:
            return int(default)

    def _cfg_float(self, name: str, default: float) -> float:
        v = self._cfg_get(name, default)
        try:
            return float(v)
        except Exception:
            return float(default)

    def _cfg_bool(self, name: str, default: bool) -> bool:
        v = self._cfg_get(name, default)
        if isinstance(v, bool):
            return v
        if v is None:
            return bool(default)
        if isinstance(v, (int, float)):
            return bool(int(v))
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("1", "true", "t", "yes", "y", "on"):
                return True
            if s in ("0", "false", "f", "no", "n", "off", ""):
                return False
        return bool(v) if v is not None else bool(default)

    def reload_runtime_cfg(self) -> None:
        """
        UI에서 config 값을 변경한 뒤 호출하면 런타임 동작에 반영된다.
        - 연결을 즉시 끊지 않고도 바뀐 값들이 다음 명령/워치독 사이클부터 적용된다.
        - 엔드포인트(host/port)는 _resolve_endpoint()에서 매번 config를 읽으므로 별도 캐시하지 않는다.
        """
        # 디버그
        self.debug_print = self._cfg_bool("DEBUG_PRINT", DEBUG_PRINT)

        # 검증/감시
        self._p_set_tol_pct = self._cfg_float("DCP_P_SET_TOL_PCT", DEFAULT_DCP_P_SET_TOL_PCT)
        self._p_set_tol_w = self._cfg_float("DCP_P_SET_TOL_W", DEFAULT_DCP_P_SET_TOL_W)
        self._p_set_deviate_max_n = max(1, self._cfg_int("DCP_P_SET_DEVIATE_MAX_N", DEFAULT_DCP_P_SET_DEVIATE_MAX_N))

        self._i_low_thresh_a = self._cfg_float("DCP_I_LOW_THRESH_A", DEFAULT_DCP_I_LOW_THRESH_A)
        self._i_low_count_max_n = max(1, self._cfg_int("DCP_I_LOW_COUNT_MAX_N", DEFAULT_DCP_I_LOW_COUNT_MAX_N))

        # 명령/복구 정책
        self._cmd_max_retries = max(0, self._cfg_int("DCP_CMD_MAX_RETRIES", DEFAULT_DCP_CMD_MAX_RETRIES))
        self._recover_max_attempts = max(1, self._cfg_int("DCP_RECOVER_MAX_ATTEMPTS", DEFAULT_DCP_RECOVER_MAX_ATTEMPTS))
        self._write_worker_retries = max(0, self._cfg_int("DCP_WRITE_WORKER_RETRIES", DEFAULT_DCP_WRITE_WORKER_RETRIES))
        self._enable_fault_recover = self._cfg_bool("DCP_ENABLE_FAULT_RECOVER", DEFAULT_DCP_ENABLE_FAULT_RECOVER)

        # 타이밍
        self._activation_check_delay_s = max(0.0, self._cfg_float("DCP_ACTIVATION_CHECK_DELAY_S", DEFAULT_DCP_ACTIVATION_CHECK_DELAY_S))
        self._poll_period_s = max(0.1, self._cfg_float("DCP_POLL_INTERVAL_S", DEFAULT_DCP_POLL_INTERVAL_S))
        self._connect_timeout_s = max(0.5, self._cfg_float("DCP_CONNECT_TIMEOUT_S", DEFAULT_DCP_CONNECT_TIMEOUT_S))

        self._timeout_ms = max(200, self._cfg_int("DCP_TIMEOUT_MS", DEFAULT_DCP_TIMEOUT_MS))
        self._gap_ms = max(0, self._cfg_int("DCP_GAP_MS", DEFAULT_DCP_GAP_MS))
        self._watchdog_interval_ms = max(200, self._cfg_int("DCP_WATCHDOG_INTERVAL_MS", DEFAULT_DCP_WATCHDOG_INTERVAL_MS))

        self._reconnect_backoff_start_ms = max(200, self._cfg_int("DCP_RECONNECT_BACKOFF_START_MS", DEFAULT_DCP_RECONNECT_BACKOFF_START_MS))
        self._reconnect_backoff_max_ms = max(self._reconnect_backoff_start_ms, self._cfg_int("DCP_RECONNECT_BACKOFF_MAX_MS", DEFAULT_DCP_RECONNECT_BACKOFF_MAX_MS))
        self._first_cmd_extra_timeout_ms = max(0, self._cfg_int("DCP_FIRST_CMD_EXTRA_TIMEOUT_MS", DEFAULT_DCP_FIRST_CMD_EXTRA_TIMEOUT_MS))

        self._post_open_quiet_s = max(0.0, self._cfg_float("DCP_POST_OPEN_QUIET_S", DEFAULT_DCP_POST_OPEN_QUIET_S))
        self._drain_timeout_s = max(0.0, self._cfg_float("DCP_DRAIN_TIMEOUT_S", DEFAULT_DCP_DRAIN_TIMEOUT_S))

        # TCP 전략
        self._inactivity_s = max(0.0, self._cfg_float("DCP_INACTIVITY_REOPEN_S", DEFAULT_DCP_INACTIVITY_REOPEN_S))
        self._tcp_keepalive = self._cfg_bool("DCP_TCP_KEEPALIVE", DEFAULT_DCP_TCP_KEEPALIVE)

    # ====== 공용 API ======
    async def start(self):
        if self._watchdog_task and self._watchdog_task.done():
            self._watchdog_task = None
        if self._cmd_worker_task and self._cmd_worker_task.done():
            self._cmd_worker_task = None
        if self._watchdog_task and self._cmd_worker_task:
            return
        self._want_connected = True
        loop = asyncio.get_running_loop()
        self._watchdog_task = loop.create_task(self._watchdog_loop(), name="DCPWatchdog")
        self._cmd_worker_task = loop.create_task(self._cmd_worker_loop(), name="DCPCmdWorker")

    async def cleanup(self):
        await self._emit_status("DCP 종료 절차 시작")
        self._want_connected = False
        await self._cancel_task("_poll_task")
        await self._cancel_task("_cmd_worker_task")
        await self._cancel_task("_watchdog_task")
        self._purge_pending("shutdown")

        if self._reader_task:
            self._reader_task.cancel()
            with contextlib.suppress(Exception):
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
            if self._poll_task:
                self._poll_task.cancel()
                self._poll_task = None
            self._purge_pending("polling off")  # ✅ 추가: 공정 종료/STOP 라이트 정리에서도 잔여 제거
            self._ev_nowait(DCPEvent(kind="status", message="Polling read 중지"))

    # 추가: 연결 완료 대기 헬퍼
    async def _wait_until_connected(self, timeout: float = 3.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._connected and self._writer and not self._writer.is_closing():
                return True
            await asyncio.sleep(0.05)
        return False

    async def pause_watchdog(self):
        """워치독 루프에서 connect 시도를 잠시 멈춤(이미 연결이면 유지)."""
        self._wd_paused = True

    async def resume_watchdog(self):
        self._wd_paused = False

    async def set_master_host_all(self) -> bool:
        return await self._write_cmd_data(0x84, 0x0007, 2, label="HOST_MASTER_ALL")

    async def set_regulation_power(self) -> bool:
        return await self._write_cmd_data(0x81, 0x0001, 2, label="REG_POWER")

    async def set_reference_power(self, power_w: Union[float, str]) -> bool:
        if _is_keep(power_w):
            return True
        w = float(power_w)
        w = max(0.0, min(float(MAX_POWER_W), w))
        raw = int(round(w / P_SET_STEP_W))
        self._last_ref_power_w = w
        return await self._write_cmd_data(0x83, raw, 2, label="REF_POWER")

    async def output_on(self) -> bool:
        self._stop_guard = False
        self._drain_rx_frames()  # ← 잔여 0x9A 등 제거
        return await self._write_cmd_data(0x80, 0x0001, 2, label="OUTPUT_ON")

    async def output_off(self) -> bool:
        # ✅ STOP/종료 중 재전송 루프가 REG/REF/OUTPUT_ON으로 흘러가는 것을 차단
        # (STOP 시퀀스에서 OUTPUT_OFF 이후 OUTPUT_ON이 다시 실행되는 현상 방지)
        self._stop_guard = True
        self._drain_rx_frames()  # ← 잔여 0x9A 등 제거
        return await self._write_cmd_data(0x80, 0x0002, 2, label="OUTPUT_OFF")

    async def prepare_and_start(self, power_w: Union[float, str]) -> bool:
        ok = await self.set_master_host_all()
        if not ok:
            return False
        ok = await self.set_regulation_power()
        if not ok:
            return False
        ok = await self.set_reference_power(power_w)
        if not ok:
            return False
        ok = await self.output_on()
        return ok

    async def read_output_piv(self) -> Optional[dict]:
        """
        0x9A Read Output( P / I / V )  — 응답:
        STX + CMD(0x9A) + P(2B) + I(2B) + V(2B) + ETX + CHK
        """
        payload = self._proto.pack_read(0x9A)
        label = "READ_PIV"
        fut = asyncio.get_running_loop().create_future()

        def _cb(_resp: Optional[bytes]):
            if not fut.done():
                fut.set_result(_resp)

        retries = self._cmd_max_retries
        self._enqueue(Command(payload, label, self._timeout_ms, self._gap_ms, retries, _cb))
        resp = await self._await_reply_bytes(
            label, fut,
            timeout_ms=self._timeout_ms,
            retries=retries,
            gap_ms=self._gap_ms
        )
        if not resp:
            return None

        # payload는 워커가 core(CMD+DATA...)만 전달하므로 여기서는 길이만 체크
        # CMD(1) + P(2) + I(2) + V(2) = 7
        if len(resp) < 7 or resp[0] != 0x9A:
            return None

        p_raw = (resp[1] << 8) | resp[2]
        i_raw = (resp[3] << 8) | resp[4]
        v_raw = (resp[5] << 8) | resp[6]

        eng = {
            "P_W": p_raw * P_MEAS_W_PER_LSB,
            "I_A": i_raw * I_MEAS_A_PER_LSB,
            "V_V": v_raw * V_MEAS_V_PER_LSB,
        }
        return {"raw": {"P": p_raw, "I": i_raw, "V": v_raw}, "eng": eng}

    async def read_fault_code(self) -> Optional[int]:
        payload = self._proto.pack_read(0x8C)
        label = "READ_FAULT"
        fut = asyncio.get_running_loop().create_future()

        def _cb(_resp: Optional[bytes]):
            if not fut.done():
                fut.set_result(_resp)

        retries = self._cmd_max_retries
        self._enqueue(Command(payload, label, self._timeout_ms, self._gap_ms, retries, _cb))
        resp = await self._await_reply_bytes(
            label, fut,
            timeout_ms=self._timeout_ms,
            retries=retries,
            gap_ms=self._gap_ms
        )
        if not resp:
            return None
        # CMD(1) + DATA(2) expected
        if len(resp) < 3 or resp[0] != 0x8C:
            return None
        val = (resp[1] << 8) | resp[2]
        return int(val)

    async def fault_reset(self) -> bool:
        return await self._write_cmd_data(0x8D, 0x0001, 2, label="FAULT_RESET")

    async def query_output_state(self) -> Optional[bool]:
        """
        0x88 Read Output State
        - 예상 응답: CMD(0x88) + DATA(2B) ... (매뉴얼/실기기 편차 가능)
        """
        payload = self._proto.pack_read(0x88)
        label = "READ_OUTPUT_STATE"
        fut = asyncio.get_running_loop().create_future()

        def _cb(_resp: Optional[bytes]):
            if not fut.done():
                fut.set_result(_resp)

        retries = self._cmd_max_retries
        self._enqueue(Command(payload, label, self._timeout_ms, self._gap_ms, retries, _cb))
        resp = await self._await_reply_bytes(
            label, fut,
            timeout_ms=self._timeout_ms,
            retries=retries,
            gap_ms=self._gap_ms
        )
        if not resp:
            return None

        # 최소: CMD 1바이트 + 상태 1~2바이트 정도
        if len(resp) < 2 or resp[0] != 0x88:
            return None

        # 장비 상태비트 정의가 명확치 않아, 0이면 OFF, 그 외는 ON으로 간주(보수적으로)
        # (필요 시 매뉴얼 비트맵에 맞춰 수정)
        state_val = resp[1]
        return bool(state_val)

    async def _verify_output_state(self) -> Optional[bool]:
        with contextlib.suppress(Exception):
            st = await self.query_output_state()
            return st
        return None

    # ====== Poll 루프(필요 시 항목 확장) ======
    async def _poll_loop(self):
        try:
            while True:
                t0 = time.monotonic()
                try:
                    if self._connected and self._out_on:
                        res = await self.read_output_piv()
                        # 👉 응답없음(None)은 '0이 아님'으로 간주하므로 그대로 지나감(pass)
                        if res and "eng" in res:
                            eng = res["eng"]
                            p = float(eng.get("P_W", 0.0))
                            v = float(eng.get("V_V", 0.0))
                            i = float(eng.get("I_A", 0.0))

                            # ① 저전류 감시: I <= 0.05 A가 연속 3회면 AUTO_STOP
                            ref = float(self._last_ref_power_w or 0.0)

                            if ref > 0.0:
                                # 세트포인트가 잡혀 있을 때만 저전류 감시
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

                                        # ✅ (추가) AUTO-STOP 시점 fault code 동봉 (원인 추적용)
                                        fault = None
                                        with contextlib.suppress(Exception):
                                            fault = await self.read_fault_code()
                                        if fault is not None and fault != 0:
                                            reason += f", fault=0x{fault:04X}"

                                        self._ev_nowait(DCPEvent(
                                            kind="command_failed",
                                            cmd="AUTO_STOP",
                                            reason=reason,
                                            power=p,
                                            voltage=v,
                                            current=i,
                                            eng=eng,
                                        ))
                                        await self._emit_status(
                                            "[AUTO-STOP] 저전류가 연속 발생 → OUTPUT_OFF & stop polling"
                                        )
                                        with contextlib.suppress(Exception):
                                            await self.output_off()
                                        return
                                else:
                                    # 전류가 다시 정상으로 올라오면 저전류 카운터 리셋
                                    if self._low_curr_n:
                                        self._low_curr_n = 0
                            else:
                                # 세트포인트가 없으면 저전류 카운터도 리셋
                                if self._low_curr_n:
                                    self._low_curr_n = 0

                            # ② 세트포인트 근접 확인 (허용오차: max(절대 W, 퍼센트))
                            if ref > 0.0:
                                tol = max(self._p_set_tol_w, abs(ref) * self._p_set_tol_pct)
                                if abs(p - ref) > tol:
                                    # 연속 이탈 카운터 증가
                                    self._spdev_n += 1
                                    await self._emit_status(
                                        f"[WARN] P dev: P={p:.1f} W, Set={ref:.1f} W, Tol=±{tol:.1f} W — 세트포인트 이탈 "
                                        f"({self._spdev_n}/{self._p_set_deviate_max_n})"
                                    )
                                    # 연속 N회 이탈 시 자동 정지
                                    if self._spdev_n >= self._p_set_deviate_max_n:
                                        self._ev_nowait(DCPEvent(
                                            kind="command_failed",
                                            cmd="AUTO_STOP",
                                            reason="target_failed",
                                            power=p,
                                            voltage=v,
                                            current=i,
                                            eng=eng,
                                        ))
                                        await self._emit_status(
                                            "[AUTO-STOP] 세트포인트 이탈이 연속 발생 → OUTPUT_OFF & stop polling"
                                        )
                                        with contextlib.suppress(Exception):
                                            await self.output_off()
                                        return
                                else:
                                    # 정상범위이면 카운터 리셋
                                    if self._spdev_n:
                                        self._spdev_n = 0
                            else:
                                # ref가 0 이하이면 카운터 리셋(비교대상 없음)
                                if self._spdev_n:
                                    self._spdev_n = 0

                            # ③ 텔레메트리 이벤트 전송 (기존 그대로 유지)
                            ev = DCPEvent(
                                kind="telemetry",
                                data=eng,
                                power=p,
                                voltage=v,
                                current=i,
                                eng=eng,
                            )
                            self._ev_nowait(ev)

                            cb = getattr(self, "_on_telemetry", None)
                            if cb:
                                try:
                                    cb(p, v, i)
                                except Exception:
                                    pass
                    else:
                        # 연결이 없거나 출력 OFF 상태면 카운터들 리셋
                        if self._spdev_n:
                            self._spdev_n = 0
                        if self._low_curr_n:
                            self._low_curr_n = 0

                except Exception as e:
                    self._ev_nowait(DCPEvent(kind="status", message=f"[poll] 예외: {e!r}"))

                dt = time.monotonic() - t0
                await asyncio.sleep(max(0.05, self._poll_period_s - dt))
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
        host = self._override_host if self._override_host else self._cfg_get("DCPULSE_TCP_HOST", DEFAULT_DCPULSE_TCP_HOST)
        port = self._override_port if self._override_port else self._cfg_int("DCPULSE_TCP_PORT", DEFAULT_DCPULSE_TCP_PORT)
        return str(host), int(port)

    def _enqueue(self, cmd: Command):
        self._cmd_q.append(cmd)

    def _safe_callback(self, cb: Optional[Callable[[Optional[bytes]], None]], arg: Optional[bytes]):
        if cb is None:
            return
        try:
            cb(arg)
        except Exception:
            pass

    def _ev_nowait(self, ev: DCPEvent):
        try:
            self._event_q.put_nowait(ev)
        except Exception:
            pass

    async def _emit_status(self, msg: str):
        self._ev_nowait(DCPEvent(kind="status", message=msg))

    async def _emit_confirmed(self, label: str):
        self._ev_nowait(DCPEvent(kind="command_confirmed", cmd=label))

    async def _emit_failed(self, label: str, reason: str):
        self._ev_nowait(DCPEvent(kind="command_failed", cmd=label, reason=reason))

    async def _cancel_task(self, name: str):
        t = getattr(self, name, None)
        if not t:
            return
        try:
            t.cancel()
            with contextlib.suppress(Exception):
                await t
        finally:
            setattr(self, name, None)

    def _purge_pending(self, why: str):
        # 큐/인플라이트 정리
        self._cmd_q.clear()
        self._inflight = None
        self._drain_rx_frames()
        try:
            while True:
                self._event_q.get_nowait()
        except Exception:
            pass

    def _on_tcp_disconnected(self):
        if self._writer:
            try:
                self._writer.close()
            except Exception:
                pass
        self._reader = None
        self._writer = None
        self._connected = False
        self._just_reopened = False
        self._inflight = None
        self._drain_rx_frames()

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
        backoff = self._reconnect_backoff_start_ms
        while self._want_connected:
            # 워치독 일시정지(엔드포인트 변경 등)
            if getattr(self, "_wd_paused", False):
                await asyncio.sleep(0.1)
                continue

            if self._connected:
                await asyncio.sleep(self._watchdog_interval_ms / 1000.0)
                continue
            if self._ever_connected:
                await self._emit_status(f"재연결 예약. ({backoff} ms)")
                await asyncio.sleep(backoff / 1000.0)
            if not self._want_connected:
                break
            try:
                host, port = self._resolve_endpoint()
                await self._emit_status(f"연결 시도: {host}:{port}")
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=self._connect_timeout_s
                )
                self._connected = True
                self._ever_connected = True
                self._last_connect_mono = time.monotonic()
                self._just_reopened = True

                # TCP keepalive (필요 시)
                try:
                    sock = self._writer.get_extra_info("socket")
                    if sock is not None:
                        if self._tcp_keepalive:
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
                self._reader_task = asyncio.create_task(self._reader_loop(), name="DCPReader")

                await self._emit_status("연결 성공")
                backoff = self._reconnect_backoff_start_ms

            except Exception as e:
                self._on_tcp_disconnected()
                backoff = min(int(backoff * 2), self._reconnect_backoff_max_ms)
                await self._emit_status(f"연결 실패: {e!r}")

    async def _reader_loop(self):
        """
        NPort 등 TCP-Serial 게이트웨이는 RS-232 바이트 스트림을 그대로 흘려보냄.
        여기서는 STX..ETX..CHK 프레임을 찾아 잘라서 _frame_q로 전달.
        """
        buf = bytearray()
        try:
            while self._connected and self._reader:
                chunk = await self._reader.read(1024)
                if not chunk:
                    raise ConnectionError("TCP EOF")
                buf.extend(chunk)

                # 프레임 파싱: STX(0x02) ... ETX(0x03) CHK(1B)
                # 최소 길이: STX(1)+CMD(1)+ETX(1)+CHK(1)=4
                while True:
                    # STX 찾기
                    try:
                        stx_i = buf.index(0x02)
                    except ValueError:
                        buf.clear()
                        break
                    if stx_i > 0:
                        del buf[:stx_i]

                    if len(buf) < 4:
                        break

                    # ETX 찾기
                    try:
                        etx_i = buf.index(0x03, 1)
                    except ValueError:
                        break
                    if etx_i + 1 >= len(buf):
                        break

                    # core = STX..ETX, chk = 1B
                    core = bytes(buf[:etx_i + 1])
                    chk = buf[etx_i + 1]
                    del buf[:etx_i + 2]

                    # checksum verify
                    if _chk_nibble_sum(core) != chk:
                        await self._emit_status(f"[WARN] CHK mismatch: core={core.hex(' ')} chk={chk:02X}")
                        continue

                    # strip STX/ETX, keep CMD+DATA...
                    payload = core[1:-1]
                    if payload:
                        try:
                            self._frame_q.put_nowait(payload)
                        except Exception:
                            pass

        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._emit_status(f"[reader] 예외: {e!r}")
        finally:
            self._on_tcp_disconnected()

    async def _cmd_worker_loop(self):
        """
        단일 인플라이트 명령 + 프레임 큐로 응답을 연결.
        - Command.retries_left 만큼 타임아웃/미응답 시 재전송
        """
        try:
            while self._want_connected:
                # inactivity reopen
                if self._connected and self._inactivity_s > 0:
                    idle = time.monotonic() - self._last_io_mono if self._last_io_mono > 0 else 0.0
                    if idle >= self._inactivity_s:
                        await self._emit_status(f"[DCP] idle {idle:.1f}s ≥ {self._inactivity_s:.1f}s → 세션 재시작")
                        self._on_tcp_disconnected()
                        await asyncio.sleep(0.2)

                if not self._connected or not self._writer:
                    await asyncio.sleep(0.05)
                    continue

                if self._inflight is None:
                    if not self._cmd_q:
                        await asyncio.sleep(0.01)
                        continue
                    self._inflight = self._cmd_q.popleft()

                    # (추가) 재연결 직후 안정화 시간
                    if self._just_reopened:
                        self._just_reopened = False
                        await asyncio.sleep(self._post_open_quiet_s)

                    # (추가) 재연결 직후 drain (남은 bytes/프레임 제거)
                    t_deadline = time.monotonic() + self._drain_timeout_s
                    while time.monotonic() < t_deadline:
                        try:
                            self._frame_q.get_nowait()
                        except Exception:
                            break

                cmd = self._inflight
                if cmd is None:
                    continue

                # 전송
                try:
                    self._writer.write(cmd.payload)
                    await self._writer.drain()
                    self._last_io_mono = time.monotonic()

                    await self._emit_status(f"[SEND] {cmd.label} → {cmd.payload.hex(' ')}")

                except Exception as e:
                    await self._emit_status(f"[SEND_FAIL] {cmd.label}: {e!r}")
                    self._safe_callback(cmd.callback, None)
                    self._inflight = None
                    self._on_tcp_disconnected()
                    continue

                # 응답 대기: frame_q.get with timeout
                try:
                    per_attempt_s = (cmd.timeout_ms / 1000.0) + 2.0
                    resp = await asyncio.wait_for(self._frame_q.get(), timeout=per_attempt_s)
                    self._last_io_mono = time.monotonic()
                    self._safe_callback(cmd.callback, resp)
                    self._inflight = None
                    await asyncio.sleep(cmd.gap_ms / 1000.0)
                    continue
                except asyncio.TimeoutError:
                    cmd.retries_left -= 1
                    await self._emit_status(f"[NO_REPLY] {cmd.label} retries_left={cmd.retries_left}")
                    if cmd.retries_left < 0:
                        self._safe_callback(cmd.callback, None)
                        self._inflight = None
                        # 세션을 끊어 다음 watchdog reconnect로 회복
                        self._on_tcp_disconnected()
                    else:
                        # 재시도 전 gap
                        await asyncio.sleep(cmd.gap_ms / 1000.0)
                    continue

        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._emit_status(f"[cmd_worker] 예외: {e!r}")
        finally:
            self._on_tcp_disconnected()

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

    async def _recover_and_prepare_retry(self, label: str, last_resp: Optional[bytes]) -> bool:
        # fault read
        fault = None
        with contextlib.suppress(Exception):
            fault = await self.read_fault_code()

        if fault is None:
            await self._emit_status(f"[{label}] fault 조회 실패(None) — 재전송 중단")
            return False

        if fault == 0:
            # fault=0인데 NAK인 경우: origin/remote 상태 등의 문제 가능 → reset이 오히려 독
            await self._emit_status(f"[{label}] fault=0 — reset 생략, 재전송")
            return True

        await self._emit_status(f"[{label}] fault=0x{fault:04X} — FAULT_RESET 시도")
        ok_reset = await self.fault_reset()
        if not ok_reset:
            await self._emit_status(f"[{label}] FAULT_RESET 실패 → 재전송 중단")
            return False

        # 장비 내부 정리 시간(너무 짧으면 바로 NAK가 재발할 수 있음)
        await asyncio.sleep(1.0) # 1초
        return True

    async def _write_cmd_data(self, cmd: int, value: int, width: int, *, label: str) -> bool:
        """
        ✅ 신규 정책:
        - write는 워커 blind retry(NAK 반복) 대신,
        1회 전송 → 실패 즉시 fault read/reset 판단 → 재전송
        이 사이클을 총 self._recover_max_attempts 회 반복.
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
                await self._emit_status(f"[{attempt_label}] STOP_GUARD turned on → abort")
                return False

            # 1회 전송 (워커 retries_left=0으로 '블라인드' 재전송 방지)
            fut = asyncio.get_running_loop().create_future()

            def _cb(_resp: Optional[bytes]):
                if not fut.done():
                    fut.set_result(_resp)

            self._enqueue(Command(
                payload,
                attempt_label,
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
                    await asyncio.sleep(0.08)
                    ver = await self._verify_output_state()
                    if ver is False:
                        self._out_on = False
                        self._last_ref_power_w = None
                        await self._emit_confirmed(base_label + "_VERIFIED")
                        self.set_process_status(False)
                        return True

                    # (3) 실패 → fault 처리 후 다음 attempt
                    if self._enable_fault_recover:
                        ok_retry = await self._recover_and_prepare_retry(base_label, resp)
                        if not ok_retry:
                            await self._emit_failed(base_label, "FAULT_RESET 실패/복구 불가")
                            self.set_process_status(False)
                            return False
                    continue

            # ---- 일반 write 명령(ACK만 성공) ----
            if self._ok_from_resp(resp, label=base_label):
                await self._emit_confirmed(base_label)
                return True

            # 실패 → fault 복구 후 재시도
            if self._enable_fault_recover:
                ok_retry = await self._recover_and_prepare_retry(base_label, resp)
                if not ok_retry:
                    await self._emit_failed(base_label, "FAULT_RESET 실패/복구 불가")
                    return False
            await asyncio.sleep(0.05)

        await self._emit_failed(base_label, f"응답 없음/실패 — 총 {self._recover_max_attempts}회 시도, last={last_resp!r}")
        return False
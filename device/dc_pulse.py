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
from lib.config_ch1 import DCPULSE_TCP_HOST, DCPULSE_TCP_PORT
from lib import config_common as cfgc   # ★ 추가

# ===== 파워 확인 파라미터 =====
P_SET_TOL_PCT = getattr(cfgc, "DCP_P_SET_TOL_PCT", 0.10)  # ±10 %
P_SET_TOL_W   = getattr(cfgc, "DCP_P_SET_TOL_W",   15.0)  # ±15 W

# OFF 이후 P=0 강제 여부(기본 False: 로그만 확인, True: 0W 아니면 실패 처리)
STRICT_OFF_CONFIRM_BY_PIV     = getattr(cfgc, "DCP_STRICT_OFF_CONFIRM_BY_PIV", True)
OFF_CONFIRM_TIMEOUT_S         = getattr(cfgc, "DCP_OFF_CONFIRM_TIMEOUT_S", 3.0)
OFF_CONFIRM_POLL_INTERVAL_S   = getattr(cfgc, "DCP_OFF_CONFIRM_POLL_INTERVAL_S", 0.2)


# === OUTPUT_ON 직후 간단 활성 확인 ===
ACTIVATION_CHECK_DELAY_S = 5.0      # OUTPUT_ON 후 첫 측정까지 대기 (초)

# 폴링 주기(초)
DCP_POLL_INTERVAL_S = 5.0
DCP_CONNECT_TIMEOUT_S = 1.5

# 타이밍/리트라이
DCP_TIMEOUT_MS = 1500               # 개별 명령 타임아웃
DCP_GAP_MS = 1000                  # 명령 간 최소 간격
DCP_WATCHDOG_INTERVAL_MS = 1000
DCP_RECONNECT_BACKOFF_START_MS = 1000
DCP_RECONNECT_BACKOFF_MAX_MS = 10000
DCP_FIRST_CMD_EXTRA_TIMEOUT_MS = 1000

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
                 on_telemetry: Optional[Callable[[float, float, float], None]] = None):
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

        # ★ Inactivity 전략 필드
        self._inactivity_s: float = float(getattr(cfgc, "DCP_INACTIVITY_REOPEN_S", 0.0))
        self._last_io_mono: float = 0.0

        self._out_on: bool = False                 # 출력 ON/OFF 내부 기억
        self._poll_period_s: float = DCP_POLL_INTERVAL_S
        self._last_ref_power_w: Optional[float] = None  # ← 세트포인트 저장

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
        with contextlib.suppress(Exception):
            while True:
                self._frame_q.get_nowait()

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
            self._ev_nowait(DCPEvent(kind="status", message="Polling read 중지"))

    # 추가: 연결 완료 대기 헬퍼
    async def _wait_until_connected(self, timeout: float = 3.0) -> bool:
        deadline = time.monotonic() + timeout
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
        # 명령 전송전 잠깐 대기
        ok_conn = await self._wait_until_connected(timeout=3.0)
        if not ok_conn:
            await self._emit_failed("CONNECT", "연결 준비 실패")
            return False

        # freq/duty 모두 숫자면 off_time_us를 계산해서 0x67로 전송
        if not _is_keep(freq) and freq is not None:
            f_khz = float(freq)
            await self.set_pulse_freq_khz(f_khz)  # 0x66

            if not _is_keep(duty) and duty is not None:
                d_pct = float(duty)
                # 주기[us] = 1,000 / f[kHz]
                period_us = 1000.0 / max(1e-6, f_khz)
                # off_time_us = period * (1 - duty)
                off_time_us = max(0.0, period_us * (1.0 - d_pct / 100.0))
                # 장비 스펙: DC=9, 1.0~10.0us → 10~100 (x10 스케일)
                if d_pct >= 100.0 or off_time_us < 1.0:
                    await self.set_off_time_dc()         # 0x67, DC=9
                else:
                    await self.set_off_time_us(off_time_us)  # 0x67
            # duty가 keep/None이면 주파수만 적용(Off Time 유지)

        # duty만 숫자인 경우(주파수 미지정)는 off_time_us 계산 불가 → 유지
        # 필요하면 별도 API(set_off_time_us)로 직접 지정하세요.
        
        # 2) 제어 모드 = Power
        await self.set_regulation_power()

        # 4) 출력 Setpoint(Power) 설정
        ok = await self.set_reference_power(power_w)
        if not ok:
            # REF 실패 시 안전을 위해 OFF까지 보장 (이미 OFF여도 무해)
            await self.output_off()
            await self._emit_status("REF_POWER 실패 → OUTPUT_ON 생략")
            return False

        # 5) 출력 ON (성공시에만)
        ok2 = await self.output_on()
        return bool(ok2)

    # ====== 고수준 제어 ======
    async def set_master_host_all(self):
        for cmd, name in ((0x7B, "MASTER_ONOFF"),
                        (0x7C, "MASTER_REFER"),
                        (0x7D, "MASTER_MODE")):
            await self._write_cmd_data(cmd, 0x0003, 2, label=name)
        await asyncio.sleep(0.2)  # 전환 유예

    async def set_regulation(self, mode: Literal["V","I","P"]):
        """0x81: 제어 모드 설정 (1=V, 2=I, 3=P)."""
        code_map = {"V":1, "I":2, "P":3}
        val = code_map[mode.upper()]
        await self._write_cmd_data(0x81, val, 2, label=f"REG_{mode.upper()}")

    async def set_regulation_power(self):
        """제어 모드 = Power."""
        await self._write_cmd_data(0x81, 3, 2, label="REG_POWER")

    async def set_reference(self, mode: Literal["V","I","P"], value: float):
        """0x83: 출력 레벨(참조) 설정 — 모드별 스케일 적용."""
        if mode.upper() == "V":
            raw = int(round(value / V_SET_STEP_V))
        elif mode.upper() == "I":
            raw = int(round(value / I_SET_STEP_A))
        else:  # "P"
            raw = int(round(float(value) / P_SET_STEP_W))
            raw = max(0, min(int(MAX_POWER_W // P_SET_STEP_W), raw))
        await self._write_cmd_data(0x83, raw, 2, label=f"REF_{mode.upper()}({value})")

    async def set_reference_power(self, value_w: float) -> bool:
        """출력 레벨(전력) 설정 — 10 W/step → 0~500."""
        # 10 W/step → 0..500 (5 kW)
        raw = int(round(float(value_w) / P_SET_STEP_W))
        raw = max(0, min(int(MAX_POWER_W // P_SET_STEP_W), raw))
        self._last_ref_power_w = float(value_w)  # ← 세트포인트 기억
        return await self._write_cmd_data(0x83, raw, 2, label=f"REF_POWER({value_w:.0f}W)")

    async def output_on(self) -> bool:
        """0x80: 1=ON, 2=OFF."""
        self._drain_rx_frames()  # ← 잔여 0x9A 등 제거
        return await self._write_cmd_data(0x80, 0x0001, 2, label="OUTPUT_ON")

    async def output_off(self) -> bool:
        self._drain_rx_frames()  # ← 잔여 0x9A 등 제거
        return await self._write_cmd_data(0x80, 0x0002, 2, label="OUTPUT_OFF")

    async def set_pulse_sync(self, mode: Literal["int","ext"]):
        # 0x65: Int=0, Ext=1
        val = 0 if mode == "int" else 1
        await self._write_cmd_data(0x65, val, 2, label=f"PULSE_SYNC({mode.upper()})")

    async def set_pulse_freq_khz(self, freq_khz: float):
        # 0x66: 20~150 (kHz)
        val = int(round(freq_khz))
        val = min(150, max(20, val))
        await self._write_cmd_data(0x66, val, 2, label=f"PULSE_FREQ({val}kHz)")

    async def set_off_time_us(self, off_time_us: float):
        # 0x67: DC=9, 1.0~10.0us → 10~100 (x10 스케일)
        x10 = int(round(off_time_us * 10.0))
        x10 = min(100, max(10, x10))
        await self._write_cmd_data(0x67, x10, 2, label=f"OFF_TIME({off_time_us:.1f}us)")

    async def set_off_time_dc(self):
        await self._write_cmd_data(0x67, 9, 2, label="OFF_TIME(DC)")

    # ====== 선택: 기타 설정(원 코드 호환) ======
    async def set_arc_params(self, *, detection_us: float, pause_us: float,
                             arc_voltage_v: float|int, arc_current_a: float|int, soft_level: int):
        await self._write_cmd_data(0x05, int(round(detection_us / ARC_US_PER_LSB)), 2, label="ARC_DET_US")
        await self._write_cmd_data(0x06, int(round(pause_us     / ARC_US_PER_LSB)), 2, label="ARC_PAUSE_US")
        await self._write_cmd_data(0x07, int(round(float(arc_voltage_v) / V_SET_STEP_V)), 2, label="ARC_VOLT_V")
        await self._write_cmd_data(0x08, int(round(float(arc_current_a) / I_SET_STEP_A)), 2, label="ARC_CURR_A")
        await self._write_cmd_data(0x09, int(soft_level), 2, label="SOFT_ARC_LV")

    async def set_shutdown(self, *, delay_ms: int, pause_ms: int):
        await self._write_cmd_data(0x0A, int(delay_ms), 2, label="SHDN_DELAY_MS")
        await self._write_cmd_data(0x0B, int(pause_ms), 2, label="SHDN_PAUSE_MS")

    async def set_limits(self, *, p_w: float, i_a: float, v_v: float):
        p_raw = int(round(float(p_w) / P_SET_STEP_W))
        p_raw = max(0, min(int(MAX_POWER_W // P_SET_STEP_W), p_raw))
        i_raw = int(round(i_a / I_SET_STEP_A))
        v_raw = int(round(v_v / V_SET_STEP_V))

        await self._write_cmd_data(0x0C, p_raw, 2, label="LIM_P_W")
        await self._write_cmd_data(0x0D, i_raw, 2, label="LIM_I_A")
        await self._write_cmd_data(0x0E, v_raw, 2, label="LIM_V_V")

    async def set_ramp_and_ignition(self, *, ramp_ms: int, ignition_v: float):
        await self._write_cmd_data(0x0F, int(round(ramp_ms   / RAMP_MS_PER_LSB)), 2, label="RAMP_MS")
        await self._write_cmd_data(0x10, int(round(ignition_v / V_SET_STEP_V)),   2, label="IGN_V")

    # ====== 읽기(모니터링/상태) - 필요 시 확장 ======
    # 1) 원시 바이트를 그대로 돌려주는 읽기 헬퍼
    async def _read_raw(self, code: int, label: str) -> Optional[bytes]:
        fut = asyncio.get_running_loop().create_future()
        def _cb(resp: Optional[bytes]):
            if not fut.done():
                fut.set_result(resp)
        payload = self._proto.pack_read(code)
        self._enqueue(Command(payload, label, DCP_TIMEOUT_MS, DCP_GAP_MS, 2, _cb))
        return await self._await_reply_bytes(label, fut)

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

        P_W = P_raw * P_MEAS_W_PER_LSB
        I_A = I_raw * I_MEAS_A_PER_LSB
        V_V = V_raw * V_MEAS_V_PER_LSB
        return {"raw": {"P": P_raw, "I": I_raw, "V": V_raw},
                "eng": {"P_W": P_W, "I_A": I_A, "V_V": V_V}}
    
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

    # ====== 내부: 명령 헬퍼 ======
    def _ok_from_resp(self, resp: Optional[bytes], *, label: str = "") -> bool:
        if label in ("OUTPUT_ON", "OUTPUT_OFF"):
            # 출력 on/off 는 반드시 1바이트 ACK(0x06)만 성공으로 인정
            return bool(resp) and len(resp) == 1 and resp[0] == 0x06
    
        if not resp:
            return False
        # RS-232 write echo: 0x06=ACK(성공), 0x04=ERR(실패)
        if len(resp) == 1:
            return resp[0] == 0x06
        # 그 외(읽기 응답 등 프레임 payload)는 일단 수신만 되면 성공 처리
        return True
    
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
        # ▶ 크리티컬 명령 전, 폴링 잠시 중지 + 수신버퍼 비우기
        if label in ("OUTPUT_ON", "OUTPUT_OFF"):
            try:
                self.set_process_status(False)   # 폴링 중지
            except Exception:
                pass
            self._purge_rx_frames()             # 수신 프레임 잔여분 제거

        fut = asyncio.get_running_loop().create_future()
        def _cb(resp: Optional[bytes]):
            if not fut.done():
                fut.set_result(resp)

        payload = self._proto.pack_write(cmd, value, width=width)
        self._enqueue(Command(payload, label, DCP_TIMEOUT_MS, DCP_GAP_MS, 3, _cb))
        resp = await self._await_reply_bytes(label, fut)

        # ✅ OUTPUT_ON/OFF는 반드시 0x06(ACK)만 성공으로 간주
        if label in ("OUTPUT_ON", "OUTPUT_OFF"):
            intended_on = (label == "OUTPUT_ON")
            ack_ok = bool(resp and len(resp) == 1 and resp[0] == 0x06)

            if ack_ok:
                self._out_on = intended_on
                await self._emit_confirmed(label)
                if intended_on:
                    # 활성화 유예(기존과 동일)
                    try:
                        await asyncio.sleep(ACTIVATION_CHECK_DELAY_S)
                    except Exception:
                        pass
                    self.set_process_status(True)  # 폴링 재개
                else:
                    self.set_process_status(False) # OFF면 폴링 유지 중지
                return True

            # ❗ ACK 미수신: 짧게 대기 후 상태로 재검증
            await asyncio.sleep(0.08)
            ver = await self._verify_output_state()
            if ver is not None:
                self._out_on = ver
                if ver == intended_on:
                    await self._emit_confirmed(label + "_VERIFIED")
                    if intended_on:
                        try:
                            await asyncio.sleep(ACTIVATION_CHECK_DELAY_S)
                        except Exception:
                            pass
                        self.set_process_status(True)
                    else:
                        self.set_process_status(False)
                    return True

            await self._emit_failed(label, "응답 없음/실패 (상태 불일치/확인 불가)")
            return False

        # (그 외 명령은 기존 로직 유지)
        ok = self._ok_from_resp(resp)
        if ok:
            await self._emit_confirmed(label)
            return True
        await self._emit_failed(label, "응답 없음/실패")
        return False
        
    # ❶ [ADD] RS-232 payload 분해 헬퍼: [CMD][DATA...][(ETX?)][CHK] → (cmd, data, chk)
    def _unpack_rs232_payload(self, resp: bytes):
        if not resp or len(resp) < 2:
            return None, b"", None
        cmd = resp[0]
        chk = resp[-1]
        data = resp[1:-1]  # 끝 1바이트는 체크섬 제외
        # 워커가 ETX(0x03)를 남기는 경우 제거
        if data and data[-1] == 0x03:
            data = data[:-1]
        return cmd, data, chk

    # ❷ [ADD] 1B/2B 데이터 모두 수용하는 플래그 추출
    def _flags16_from_data(self, data: bytes) -> int | None:
        if not data:
            return None
        if len(data) >= 2:
            return ((data[-2] << 8) | data[-1]) & 0xFFFF
        return data[-1] & 0xFF

    # ❸ [REPLACE] READ_STATUS 파싱: CHK 제외 + 1B/2B 모두 처리
    async def read_status_flags(self) -> Optional[int]:
        # 0x90 응답이 나올 때까지 짧게 3~4회 재시도 (중간 0x9A 등은 무시)
        for attempt in range(3):
            resp = await self._read_raw(0x90, "READ_STATUS")
            if not resp or len(resp) < 2:
                await self._emit_failed("READ_STATUS", f"응답 길이 부족: {resp!r}")
                return None
            cmd, data, chk = self._unpack_rs232_payload(resp)
            if cmd == 0x90:
                flags = self._flags16_from_data(data)
                if flags is None:
                    await self._emit_failed("READ_STATUS", f"데이터 없음: raw={resp.hex(' ')}")
                    return None
                return flags

            # ❗ 0x90이 아닌 프레임(예: 0x9A)은 스팬으로 들어온 읽기 결과이므로 무시하고 재시도
            await self._emit_status(f"READ_STATUS: 다른 프레임(0x{cmd:02X}) 수신 → 무시하고 재시도")
            await asyncio.sleep(0.03)

        await self._emit_failed("READ_STATUS", "연속 이질 프레임으로 STATUS 확인 실패")
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
    # ===================== 실패시 검증하는 로직 =====================

    async def _write_simple(self, code: int, *, label: str):
        """데이터 없는 쓰기 명령(필요 시 사용)."""
        fut = asyncio.get_running_loop().create_future()
        def _cb(_resp: Optional[bytes]):
            if not fut.done():
                fut.set_result(_resp)

        payload = self._proto.pack_write(code, None, width=0)
        self._enqueue(Command(payload, label, DCP_TIMEOUT_MS, DCP_GAP_MS, 3, _cb))
        resp = await self._await_reply_bytes(label, fut)
        if self._ok_from_resp(resp):
            await self._emit_confirmed(label)
        else:
            await self._emit_failed(label, "응답 없음/실패")

    async def _await_reply_bytes(self, label: str, fut: "asyncio.Future[Optional[bytes]]") -> Optional[bytes]:
        # 오픈 직후 여유
        extra = 0.0
        if self._last_connect_mono > 0.0 and (time.monotonic() - self._last_connect_mono) < 2.0:
            extra = DCP_FIRST_CMD_EXTRA_TIMEOUT_MS / 1000.0
        try:
            resp = await asyncio.wait_for(fut, timeout=(DCP_TIMEOUT_MS/1000.0) + 2.0 + extra)
            if resp is not None:
                # ▶ 1바이트 에코면 ACK/ERR 라벨링, 그 외는 그대로 hex 덤프
                if len(resp) == 1 and resp[0] in (0x06, 0x04):
                    name = "ACK" if resp[0] == 0x06 else "ERR"
                    await self._emit_status(f"[RECV] {label} ← {name}({resp.hex(' ')})")
                else:
                    await self._emit_status(f"[RECV] {label} ← {resp.hex(' ')}")
            return resp
        except asyncio.TimeoutError:
            await self._emit_status(f"[TIMEOUT] {label} → 세션 재시작")
            self._on_tcp_disconnected()
            return None


    # ====== 내부: 연결/워치독/워커/리더 ======
    async def _watchdog_loop(self):
        backoff = DCP_RECONNECT_BACKOFF_START_MS
        while self._want_connected:
            if self._connected:
                await asyncio.sleep(DCP_WATCHDOG_INTERVAL_MS / 1000.0)
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
                    timeout=DCP_CONNECT_TIMEOUT_S
                )
                self._reader, self._writer = reader, writer
                self._connected = True
                self._ever_connected = True
                backoff = DCP_RECONNECT_BACKOFF_START_MS

                # ★ Keepalive는 설정에 따름(기본 False 권장)
                try:
                    sock = writer.get_extra_info("socket")
                    if sock is not None:
                        if bool(getattr(cfgc, "DCP_TCP_KEEPALIVE", False)):
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
                backoff = min(backoff * 2, DCP_RECONNECT_BACKOFF_MAX_MS)

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

        # 프레임 큐 비움
        with contextlib.suppress(Exception):
            while True:
                self._frame_q.get_nowait()

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
                remain = (self._last_connect_mono + 0.3) - time.monotonic()
                if remain > 0:
                    await asyncio.sleep(remain)
                self._just_reopened = False

            # 전송
            try:
                self._last_io_mono = time.monotonic()   # ★ 송신 직전 IO 시각
                self._writer.write(cmd.payload)
                await self._writer.drain()
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

            # 응답 대기(프레임)
            try:
                frame = await self._read_one_frame((cmd.timeout_ms/1000.0) + 2.0)
            except asyncio.TimeoutError:
                await self._emit_status(f"[TIMEOUT] {cmd.label}")
                self._inflight = None
                # 🔸 재시도 전, 짧은 백오프(명령 간격 준수)
                try:
                    await asyncio.sleep(max(0.05, cmd.gap_ms / 1000.0))
                except Exception:
                    pass
                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    self._cmd_q.appendleft(cmd)
                else:
                    self._safe_callback(cmd.callback, None)
                self._on_tcp_disconnected()   # ← 죽은 세션 재사용 방지
                continue

            self._inflight = None
            decoded = self._proto.filter_and_decode(frame)

            # ★ READ_* 요청에 대해 1바이트 NAK(0x04) 수신 시 재시도
            if decoded is not None and len(decoded) == 1 and decoded[0] == 0x04 and cmd.label.startswith("READ_"):
                await self._emit_status(f"[NAK] {cmd.label} — retry({cmd.retries_left})")
                await asyncio.sleep(max(0.05, cmd.gap_ms / 1000.0))  # 짧은 유예
                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    self._cmd_q.appendleft(cmd)   # 같은 명령 재시도
                else:
                    self._safe_callback(cmd.callback, None)  # 재시도 소진 → 상위에 실패(None) 통지
                continue  # 다음 루프(콜백 호출/간격슬립은 건너뜀)

            self._safe_callback(cmd.callback, decoded)
            await asyncio.sleep(cmd.gap_ms / 1000.0)

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

                    # 2) 여기부터는 기존 STX..ETX+CHK 프레이밍 파서 그대로
                    try:
                        i_etx = buf.index(0x03, 1)
                    except ValueError:
                        break

                    if len(buf) < i_etx + 2:
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

                            # ★ 항상 비교: Power가 '정확히 0'이면 공정 중단
                            if p == 0.0:
                                await self._emit_status("[AUTO-STOP] Power=0W → OUTPUT_OFF & stop polling")
                                with contextlib.suppress(Exception):
                                    await self.output_off()   # 내부에서 set_process_status(False) 처리
                                return                       # poll task 종료
                            
                            # ② 세트포인트 근접 확인 (허용오차: max(절대 W, 퍼센트))
                            ref = float(self._last_ref_power_w or 0.0)
                            if ref > 0.0:
                                tol = max(P_SET_TOL_W, abs(ref) * P_SET_TOL_PCT)
                                if abs(p - ref) > tol:
                                    await self._emit_status(
                                        f"[WARN] 현재 P={p:.1f} W, Set={ref:.1f} W, Tol=±{tol:.1f} W — 세트포인트 이탈"
                                    )

                            # 0이 아니면 계속 진행(기존 텔레메트리 전송 유지)
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
        host = self._override_host if self._override_host else DCPULSE_TCP_HOST
        port = self._override_port if self._override_port else DCPULSE_TCP_PORT
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
            except Exception:
                pass
            setattr(self, name, None)

    def _purge_pending(self, reason: str = "") -> int:
        purged = 0
        if self._inflight is not None:
            cmd = self._inflight
            self._inflight = None
            purged += 1
            self._safe_callback(cmd.callback, None)
        kept = deque()
        while self._cmd_q:
            c = self._cmd_q.popleft()
            # 모두 폐기
            purged += 1
            self._safe_callback(c.callback, None)
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


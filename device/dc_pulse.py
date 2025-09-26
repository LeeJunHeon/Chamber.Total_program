# device/dc_pulse.py
# -*- coding: utf-8 -*-
"""
dc_pulse.py — EnerPulse 5/10 Pulser RS-232 제어 (MOXA NPort 등 TCP-Serial 게이트웨이 경유)
- asyncio Streams + 단일 명령 큐 + 워치독
- 프로토콜 Type4(STX/ETX/CHK) 바이너리 프레이밍
- Host Master 설정 → 제어모드 Power → UI값 참조 설정 → 출력 ON 시퀀스 제공

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
from typing import Optional, Callable, Deque, AsyncGenerator, Literal
from collections import deque
import asyncio, time, contextlib, socket
from lib.config_ch1 import DCPULSE_TCP_IP, DCPULSE_TCP_HOST

# ========= 기본 설정(필요 시 config_* 모듈에서 override 가능) =========
DCP_CONNECT_TIMEOUT_S = 1.5

# 프로토콜(Type4: STX/ETX/CHK) 및 RS-485 옵션
DCP_PROTOCOL_TYPE = 4          # EnerPulse 매뉴얼의 Type 4
DCP_USE_RS485 = False          # RS-485이면 True, RS-232이면 False
DCP_DEVICE_ID = 0x01           # RS-485일 때 장치 ID(0~250)

# 타이밍/리트라이
DCP_TIMEOUT_MS = 800               # 개별 명령 타임아웃
DCP_GAP_MS = 50                    # 명령 간 최소 간격
DCP_WATCHDOG_INTERVAL_MS = 1000
DCP_RECONNECT_BACKOFF_START_MS = 1000
DCP_RECONNECT_BACKOFF_MAX_MS = 10000
DCP_FIRST_CMD_EXTRA_TIMEOUT_MS = 500

# 스케일(장비 셋업에 맞게 조정)
SCALE_POWER_W = 1.0                # e.g., 5000W → 5000 (필요 시 보정)
SCALE_VOLT_V = 1.0                 # e.g., 800V   → 800
SCALE_CURR_A = 10.0                # e.g., 12.5A  → 125 (0.1A step 가정)
SCALE_RAMP_MS = 1.0                # 500~2000 ms  → 값 그대로
SCALE_ARC_US  = 1.0                # 0~5 us, 40~200 us → 값 그대로

DEBUG_PRINT = True

# ========= 이벤트 모델 =========
EventKind = Literal["status", "telemetry", "command_confirmed", "command_failed"]

@dataclass
class DCPEvent:
    kind: EventKind
    message: Optional[str] = None
    cmd: Optional[str] = None
    reason: Optional[str] = None
    data: Optional[dict] = None

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

def _csum_low8(items: bytes) -> int:
    """체크섬 = (STX부터 ETX까지의 모든 바이트 합)의 하위 8비트(carry 제외)."""
    return sum(items) & 0xFF

class BinaryProtocol(IProtocol):
    """
    Protocol Type 4: STX(0x02) + [IP?] + CMD(1B) + DATA(0~2B) + ETX(0x03) + CHK(1B)
      - RS-232: STX + CMD + DATA + ETX + CHK
      - RS-485: STX + IP + CMD + DATA + ETX + CHK
      - DATA 폭(width): 0/1/2 바이트
    """
    def __init__(self, use_rs485: bool = DCP_USE_RS485, dev_id: int = DCP_DEVICE_ID):
        self.use_rs485 = bool(use_rs485)
        self.dev_id = int(dev_id) & 0xFF

    def _frame(self, cmd: int, data: bytes) -> bytes:
        stx = b"\x02"
        etx = b"\x03"
        core = (stx + (bytes([self.dev_id]) if self.use_rs485 else b"")
                + bytes([cmd & 0xFF]) + data + etx)
        chk = bytes([_csum_low8(core)])
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
                 protocol: Optional[IProtocol] = None):
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
        if self._writer:
            with contextlib.suppress(Exception):
                self._writer.close()
        self._reader = None
        self._writer = None
        self._connected = False
        await self._emit_status("DCP 연결 종료됨")

    async def events(self) -> AsyncGenerator[DCPEvent, None]:
        while True:
            ev = await self._event_q.get()
            yield ev

    def set_endpoint(self, host: str, port: int) -> None:
        self._override_host = str(host)
        self._override_port = int(port)

    def set_process_status(self, should_poll: bool):
        if should_poll:
            if self._poll_task is None or self._poll_task.done():
                self._ev_nowait(DCPEvent(kind="status", message="주기적 읽기(Polling) 시작"))
                self._poll_task = asyncio.create_task(self._poll_loop())
        else:
            if self._poll_task:
                self._poll_task.cancel()
                self._poll_task = None
            self._ev_nowait(DCPEvent(kind="status", message="주기적 읽기(Polling) 중지"))

    # ====== 상위 시퀀스 편의 API ======
    async def prepare_and_start(self, power_w: float):
        """
        1) Host Master 3종을 Host(0x0003)로 설정: 0x7B, 0x7C, 0x7D
        2) 제어 모드 Power: 0x81 (데이터=3)
        3) 출력 레벨(참조) Power: 0x83 (단위/스케일에 따라 raw 계산)
        4) 출력 ON: 0x80 (데이터=1)
        """
        await self.set_master_host_all()
        await self.set_regulation_power()
        await self.set_reference_power(power_w)
        await self.output_on()

    # ====== 고수준 제어 ======
    async def set_master_host_all(self):
        """ONOFF/REFER/MODE master를 Host(0x0003)로 강제."""
        for cmd, name in ((0x7B, "MASTER_ONOFF"),
                          (0x7C, "MASTER_REFER"),
                          (0x7D, "MASTER_MODE")):
            await self._write_cmd_data(cmd, 0x0003, 2, label=name)

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
            raw = int(round(value * SCALE_VOLT_V))
        elif mode.upper() == "I":
            raw = int(round(value * SCALE_CURR_A))
        else:  # "P"
            raw = int(round(value * SCALE_POWER_W))
        await self._write_cmd_data(0x83, raw, 2, label=f"REF_{mode.upper()}({value})")

    async def set_reference_power(self, value_w: float):
        """출력 레벨(전력) 설정."""
        raw = int(round(float(value_w) * SCALE_POWER_W)) & 0xFFFF
        await self._write_cmd_data(0x83, raw, 2, label=f"REF_POWER({value_w:.0f}W)")

    async def output_on(self):
        """0x80: 1=ON, 2=OFF."""
        await self._write_cmd_data(0x80, 0x0001, 2, label="OUTPUT_ON")

    async def output_off(self):
        await self._write_cmd_data(0x80, 0x0002, 2, label="OUTPUT_OFF")

    # ====== 선택: 기타 설정(원 코드 호환) ======
    async def set_arc_params(self, *, detection_us: float, pause_us: float,
                             arc_voltage_v: float|int, arc_current_a: float|int, soft_level: int):
        await self._write_cmd_data(0x05, int(round(detection_us * SCALE_ARC_US)), 2, label="ARC_DET_US")
        await self._write_cmd_data(0x06, int(round(pause_us * SCALE_ARC_US)), 2, label="ARC_PAUSE_US")
        await self._write_cmd_data(0x07, int(round(float(arc_voltage_v) * SCALE_VOLT_V)), 2, label="ARC_VOLT_V")
        await self._write_cmd_data(0x08, int(round(float(arc_current_a) * SCALE_CURR_A)), 2, label="ARC_CURR_A")
        await self._write_cmd_data(0x09, int(soft_level), 2, label="SOFT_ARC_LV")

    async def set_shutdown(self, *, delay_ms: int, pause_ms: int):
        await self._write_cmd_data(0x0A, int(delay_ms), 2, label="SHDN_DELAY_MS")
        await self._write_cmd_data(0x0B, int(pause_ms), 2, label="SHDN_PAUSE_MS")

    async def set_limits(self, *, p_w: float, i_a: float, v_v: float):
        await self._write_cmd_data(0x0C, int(round(p_w * SCALE_POWER_W)), 2, label="LIM_P_W")
        await self._write_cmd_data(0x0D, int(round(i_a * SCALE_CURR_A)), 2, label="LIM_I_A")
        await self._write_cmd_data(0x0E, int(round(v_v * SCALE_VOLT_V)), 2, label="LIM_V_V")

    async def set_ramp_and_ignition(self, *, ramp_ms: int, ignition_v: float):
        await self._write_cmd_data(0x0F, int(round(ramp_ms * SCALE_RAMP_MS)), 2, label="RAMP_MS")
        await self._write_cmd_data(0x10, int(round(ignition_v * SCALE_VOLT_V)), 2, label="IGN_V")

    # ====== 읽기(모니터링/상태) - 필요 시 확장 ======
    async def read_regulation(self) -> Optional[int]:
        resp = await self._read_simple(0x13, "READ_REG")  # 예시 코드 (실제 읽기 코드는 장비 스펙에 맞춰 보정)
        return resp

    async def read_reference(self) -> Optional[int]:
        resp = await self._read_simple(0x14, "READ_REF")  # 예시 코드
        return resp

    async def read_limits(self) -> dict:
        return {
            "P": await self._read_simple(0x1C, "READ_LIM_P"),
            "I": await self._read_simple(0x1D, "READ_LIM_I"),
            "V": await self._read_simple(0x1E, "READ_LIM_V"),
        }

    # ====== 내부: 명령 헬퍼 ======
    def _ok_from_resp(self, resp: Optional[bytes]) -> bool:
        """응답 성공 판정(장비 캡처 후 보정 권장). 현재는 수신만 되면 성공 처리."""
        return bool(resp)

    async def _write_cmd_data(self, cmd: int, value: int, width: int, *, label: str):
        fut = asyncio.get_running_loop().create_future()
        def _cb(resp: Optional[bytes]):
            if not fut.done():
                fut.set_result(resp)

        payload = self._proto.pack_write(cmd, value, width=width)
        self._enqueue(Command(payload, label, DCP_TIMEOUT_MS, DCP_GAP_MS, 3, _cb))
        resp = await self._await_reply_bytes(label, fut)
        if self._ok_from_resp(resp):
            await self._emit_confirmed(label)
        else:
            await self._emit_failed(label, "응답 없음/실패")

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

    async def _read_simple(self, code: int, label: str) -> Optional[int]:
        """간단 읽기(정수 하나 파싱) — 실제 항목은 장비 문서에 맞춰 디코딩 보완 필요."""
        fut = asyncio.get_running_loop().create_future()
        def _cb(resp: Optional[bytes]):
            if not fut.done():
                fut.set_result(resp)

        payload = self._proto.pack_read(code)
        self._enqueue(Command(payload, label, DCP_TIMEOUT_MS, DCP_GAP_MS, 2, _cb))
        resp = await self._await_reply_bytes(label, fut)
        if resp is None:
            await self._emit_failed(label, "응답 없음/실패")
            return None
        # TODO: 실제 프레임 포맷에 맞춰 값 추출(여기선 자리표시자)
        try:
            # RS-232 payload: [CMD][DATA..] → 마지막 2바이트를 정수로 가정 (예시)
            if len(resp) >= 3:
                val = (resp[-2] << 8) | resp[-1]
                return int(val)
        except Exception:
            pass
        await self._emit_failed(label, f"파싱 실패: {resp!r}")
        return None

    async def _await_reply_bytes(self, label: str, fut: "asyncio.Future[Optional[bytes]]") -> Optional[bytes]:
        # 오픈 직후 여유
        extra = 0.0
        if self._last_connect_mono > 0.0 and (time.monotonic() - self._last_connect_mono) < 2.0:
            extra = DCP_FIRST_CMD_EXTRA_TIMEOUT_MS / 1000.0
        try:
            resp = await asyncio.wait_for(fut, timeout=(DCP_TIMEOUT_MS/1000.0) + 2.0 + extra)
            if resp is not None:
                await self._emit_status(f"[RECV] {label} ← {resp.hex(' ')}")
            return resp
        except asyncio.TimeoutError:
            await self._emit_status(f"[TIMEOUT] {label}")
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
                # TCP keepalive
                try:
                    sock = writer.get_extra_info("socket")
                    if sock is not None:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                except Exception:
                    pass
                # reader task
                if self._reader_task and not self._reader_task.done():
                    self._reader_task.cancel()
                    with contextlib.suppress(Exception):
                        await self._reader_task
                self._reader_task = asyncio.create_task(self._tcp_reader_loop(), name="DCP-TcpReader")
                self._last_connect_mono = time.monotonic()
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
        self._reader = None
        self._writer = None
        # 프레임 큐 비움
        with contextlib.suppress(Exception):
            while True:
                self._frame_q.get_nowait()
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
            await self._emit_status(f"[SEND] {cmd.label}")

            # 연결 직후 quiet 기간
            if self._just_reopened and self._last_connect_mono > 0.0:
                remain = (self._last_connect_mono + 0.3) - time.monotonic()
                if remain > 0:
                    await asyncio.sleep(remain)
                self._just_reopened = False

            # 전송
            try:
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
                frame = await self._read_one_frame(cmd.timeout_ms / 1000.0)
            except asyncio.TimeoutError:
                await self._emit_status(f"[TIMEOUT] {cmd.label}")
                self._inflight = None
                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    self._cmd_q.appendleft(cmd)
                else:
                    self._safe_callback(cmd.callback, None)
                self._on_tcp_disconnected()
                continue

            self._inflight = None
            decoded = self._proto.filter_and_decode(frame)
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
                buf.extend(chunk)
                if len(buf) > RX_MAX:
                    del buf[:-RX_MAX]

                # === 프레임 파서: STX(0x02) .. ETX(0x03) + CHK(1B) ===
                while True:
                    # 1) STX 찾기
                    try:
                        i_stx = buf.index(0x02)
                    except ValueError:
                        buf.clear()
                        break
                    if i_stx > 0:
                        del buf[:i_stx]  # STX 앞 제거

                    # 2) ETX 위치 찾기 (STX 뒤에서)
                    try:
                        i_etx = buf.index(0x03, 1)
                    except ValueError:
                        # ETX 아직 안 들어옴 → 다음 read
                        break

                    # 3) ETX 다음에 CHK 1바이트가 더 필요
                    if len(buf) < i_etx + 2:
                        break  # 더 받아와야 함

                    # 4) 프레임 슬라이스: core = STX..ETX, chk = 다음 1바이트
                    core = bytes(buf[:i_etx + 1])
                    chk = buf[i_etx + 1]

                    # 5) 검증
                    if (_csum_low8(core) & 0xFF) == (chk & 0xFF):
                        # payload = [IP?] + CMD + DATA (STX/ETX 제외)
                        if DCP_USE_RS485:
                            payload = core[2:-1]  # IP부터 ETX 앞까지
                        else:
                            payload = core[1:-1]  # CMD부터 ETX 앞까지
                        try:
                            self._frame_q.put_nowait(payload)
                        except asyncio.QueueFull:
                            self._dbg("DCP", "프레임 큐 포화 → 가장 오래된 프레임 폐기")
                            with contextlib.suppress(Exception):
                                _ = self._frame_q.get_nowait()
                            with contextlib.suppress(Exception):
                                self._frame_q.put_nowait(payload)
                    else:
                        self._dbg("DCP", f"CHK FAIL: core={core.hex()} chk={chk:02X}")

                    # 6) 사용한 바이트 폐기
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
                if not self._connected:
                    await asyncio.sleep(1.0)
                    continue
                # 예: 주기 상태 요청(필요 시 실제 읽기 코드로 교체)
                # await self._read_simple(0x91, "POLL_OPERATION")
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass

    # ====== 내부 유틸 ======
    def _resolve_endpoint(self) -> tuple[str, int]:
        host = self._override_host if self._override_host else DCPULSE_TCP_IP
        port = self._override_port if self._override_port else DCPULSE_TCP_HOST
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

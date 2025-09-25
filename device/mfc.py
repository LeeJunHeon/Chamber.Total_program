# -*- coding: utf-8 -*-
"""
mfc.py — asyncio 기반 MFC 컨트롤러 (MOXA NPort TCP Server 직결)

의존성: 표준 라이브러리만 사용 (pyserial-asyncio 불필요)

기능 요약(구 MFC.py와 동등):
  - asyncio TCP streams + 자체 라인 프레이밍(CR/LF) 통신
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
import asyncio, re, time, contextlib, socket


from lib.config_ch2 import (
    MFC_TCP_HOST, MFC_TCP_PORT, MFC_TX_EOL, MFC_SKIP_ECHO, MFC_CONNECT_TIMEOUT_S,
    MFC_COMMANDS, FLOW_ERROR_TOLERANCE, FLOW_ERROR_MAX_COUNT, MFC_POLLING_INTERVAL_MS, 
    MFC_STABILIZATION_INTERVAL_MS, MFC_WATCHDOG_INTERVAL_MS, MFC_RECONNECT_BACKOFF_START_MS, 
    MFC_RECONNECT_BACKOFF_MAX_MS, MFC_TIMEOUT, MFC_GAP_MS, MFC_DELAY_MS, MFC_DELAY_MS_VALVE, 
    DEBUG_PRINT, MFC_PRESSURE_DECIMALS, MFC_SP1_VERIFY_TOL, MFC_POST_OPEN_QUIET_MS, 
    MFC_FIRST_CMD_EXTRA_TIMEOUT_MS, SENSOR_FS_TORR, MFC_PRESSURE_SCALE 
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
    expect_prefixes: tuple[str, ...] = ()

# =============== Async 컨트롤러 ===============
class AsyncMFC:
    def __init__(self):
        self.debug_print = DEBUG_PRINT

        # ✅ TCP Streams
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._tx_eol: bytes = MFC_TX_EOL
        self._tx_eol_str: str = MFC_TX_EOL.decode("ascii", "ignore")
        self._skip_echo_flag: bool = bool(MFC_SKIP_ECHO)

        self._connected: bool = False
        self._ever_connected: bool = False

        # 명령 큐/인플라이트
        self._cmd_q: Deque[Command] = deque()
        self._inflight: Optional[Command] = None

        # 수신 라인 큐 (TCP 리더 → 워커)
        self._line_q: asyncio.Queue[str] = asyncio.Queue(maxsize=1024)

        # 이벤트 큐 (상위 UI/브리지 소비)
        self._event_q: asyncio.Queue[MFCEvent] = asyncio.Queue(maxsize=1024)

        # 태스크들
        self._want_connected: bool = False
        self._watchdog_task: Optional[asyncio.Task] = None
        self._cmd_worker_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._stab_task: Optional[asyncio.Task] = None

        # 런타임/스케일/모니터링
        self.gas_map = {1: "Ar", 2: "O2", 3: "N2"}
        self.last_setpoints = {1: 0.0, 2: 0.0, 3: 0.0}      # %FS 기준 목표치
        self.flow_error_counters = {1: 0, 2: 0, 3: 0}

        # 채널별 FS/UNIT 캐시 (%FS↔sccm 변환용)
        self._mfc_fs: dict[int, float] = {}    # FS (단위 원형값)
        self._mfc_unit: dict[int, int] = {}    # 1=SCCM, 2=SLM

        # 압력 FS (UI 환산)
        self.SENSOR_FS_TORR = float(SENSOR_FS_TORR)

        # 폴링 사이클 중첩 방지 플래그
        self._poll_cycle_active: bool = False

        # ★ 과거 no-reply 명령의 에코를 1회성으로 버리기 위한 대기열
        self._skip_echos: deque[str] = deque()

        # 안정화 상태
        self._stab_ch: Optional[int] = None
        self._stab_target_hw: float = 0.0
        self._stab_attempts: int = 0

        # Qt의 clear+soft-drain 타이밍을 모사하기 위한 플래그
        self._last_connect_mono: float = 0.0
        self._just_reopened: bool = False

        self._process_starting: bool = False  # 공정 시작 중복 방지

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
        #await self._emit_status("MFC 워치독/워커 시작")

    async def cleanup(self):
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

        # TCP 종료
        if self._reader_task:
            self._reader_task.cancel()
            with contextlib.suppress(Exception):
                await self._reader_task
            self._reader_task = None
        if self._writer:
            with contextlib.suppress(Exception):
                self._writer.close()
                await self._writer.wait_closed()

        self._reader = None
        self._writer = None
        self._connected = False

        await self._emit_status("MFC 연결 종료됨")

    async def events(self) -> AsyncGenerator[MFCEvent, None]:
        """상위에서 소비하는 이벤트 스트림."""
        while True:
            ev = await self._event_q.get()
            yield ev

    # ---- 고수준 제어 API (기존 handle_command 세분화) ----
    async def set_flow(self, channel: int, ui_sccm: float):
        # ✅ FLOW_SET 전에 채널 스케일(FS/UNIT) 선조회(캐시 확보)
        fs, unit = await self._ensure_mfc_meta(channel)   # unit: 1=SCCM, 2=SLM
        fs_sccm = self._fs_in_sccm(fs, unit)              # SLM이면 ×1000 해서 sccm FS로 환산
        await self._emit_status(f"Ch{channel} FS 준비: FS={fs_sccm:.3f} sccm (원 단위:{'SLM' if unit==2 else 'SCCM'})")

        # UI(sccm) → %FS
        pct = await self._sccm_to_pct(channel, float(ui_sccm))
        await self._emit_status(f"Ch{channel} 유량: {ui_sccm:.2f} sccm → {pct:.2f}%FS")

        # SET (no-reply)
        set_cmd = self._mk_cmd("FLOW_SET", channel=channel, value=round(pct, 2))
        self._enqueue(set_cmd, None, allow_no_reply=True, tag=f"[SET ch{channel}]")

        # 검증
        ok = await self._verify_flow_set(channel, pct)
        if ok:
            self.last_setpoints[channel] = pct  # %FS 보관
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
                self._stab_target_hw = tgt   # 내부는 %FS
                self._stab_attempts = 0      # ✅ 시도 카운터 리셋
                ui_tgt = await self._pct_to_sccm(channel, tgt)
                await self._emit_status(
                    f"FLOW_ON: ch{channel} 안정화 시작 (목표 {tgt:.2f}%FS/{ui_tgt:.2f}sccm)"
                )
                # ✅ 안정화 루프 시작
                self._stab_task = asyncio.create_task(self._stabilization_loop(), name="MFCStabilization")
                return

        # 안정화가 불필요하면 바로 확정
        await self._emit_confirmed("FLOW_ON")

    async def flow_off(self, channel: int):
        # 이 채널 대상 안정화 중이면 취소
        if self._stab_ch == channel:
            await self._cancel_task("_stab_task")
            self._stab_ch = None
            self._stab_target_hw = 0.0
            await self._emit_status(f"FLOW_OFF 요청: ch{channel} 안정화 취소")

        # 목표 0으로 초기화 (경고 오경보 방지)
        self.last_setpoints[channel] = 0.0
        self.flow_error_counters[channel] = 0

        now = await self._read_r69_bits()
        if not now:
            await self._emit_failed("FLOW_OFF", "R69 읽기 실패")
            return

        bits_now = list(now.ljust(4, '0'))
        if 1 <= channel <= len(bits_now) and bits_now[channel-1] == '0':
            # ✅ 이미 OFF → L0 전송 생략, 그러나 상위 호환 위해 confirmed는 방출
            await self._emit_status(f"FLOW_OFF: ch{channel} 이미 OFF 상태 → L0 생략")
            await self._emit_confirmed("FLOW_OFF")
            return

        # 실제로 마스크 변경이 필요한 경우에만 L0 수행
        if 1 <= channel <= len(bits_now):
            bits_now[channel-1] = '0'
        target = ''.join(bits_now[:4])

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
        ok = await self._verify_sp_active(1)
        if ok: await self._emit_confirmed("SP1_ON")
        else:  await self._emit_failed("SP1_ON", "SP1 상태 확인 실패")

    async def sp4_on(self):
        ok = await self._verify_sp_active(4)
        if ok: await self._emit_confirmed("SP4_ON")
        else:  await self._emit_failed("SP4_ON", "SP4 상태 확인 실패")

    async def read_flow_all(self):
        vals = await self._read_r60_values()
        if not vals:
            await self._emit_failed("READ_FLOW", "R60 파싱 실패")
            return
        for ch, name in self.gas_map.items():
            idx = ch - 1
            if idx < len(vals):
                pct = float(vals[idx])                 # %FS
                ui_sccm = await self._pct_to_sccm(ch, pct)
                await self._emit_flow(name, ui_sccm)
                self._monitor_flow(ch, pct)           # %FS 기준 모니터링

    async def read_pressure(self):
        """R5 읽기: P+xx.x (%FS)만 허용."""
        line = await self._send_and_wait_line(
            self._mk_cmd("READ_PRESSURE"),
            tag="[READ_PRESSURE]", timeout_ms=MFC_TIMEOUT,
            expect_prefixes=("P",)   # ← P만 허용
        )
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
            # ✅ 공정 시작 시퀀스는 1개만 스케줄
            if (self._poll_task is None or self._poll_task.done()) and not self._process_starting:
                self._process_starting = True
                asyncio.create_task(self._on_process_started())
        else:
            if self._poll_task:
                self._poll_task.cancel()
                self._poll_task = None
            self._poll_cycle_active = False
            self._process_starting = False  # ← 안전하게 리셋
            purged = self._purge_poll_reads_only(cancel_inflight=True, reason="polling off")
            #self._ev_nowait(MFCEvent(kind="status", message="주기적 읽기(Polling) 중지"))
            if purged:
                self._ev_nowait(MFCEvent(kind="status", message=f"[QUIESCE] 폴링 읽기 {purged}건 제거 (polling off)"))

    async def _on_process_started(self):
        try:
            # 연결 준비가 안 되었으면 아주 짧게 대기 (최대 ~0.5s)
            for _ in range(10):
                if self._connected:
                    break
                await asyncio.sleep(0.05)

            # 시작 상태 정렬: 모든 채널 OFF 보장 (이미 0000이면 L0 생략)
            await self._ensure_all_off_on_start()

            # 폴링 시작 (중복 방지)
            if self._poll_task is None or self._poll_task.done():
                self._ev_nowait(MFCEvent(kind="status", message="주기적 읽기(Polling) 시작"))
                self._poll_task = asyncio.create_task(self._poll_loop(), name="MFCPoll")
        finally:
            self._process_starting = False

    def on_process_finished(self, success: bool):
        """공정 종료 시 내부 상태 리셋."""
        self.set_process_status(False)
        # 안정화 중지
        if self._stab_task:
            self._stab_task.cancel()
            self._stab_task = None
        self._stab_ch = None
        self._stab_target_hw = 0.0
        # 큐 정리 및 카운터 리셋
        self._purge_pending(f"process finished ({'ok' if success else 'fail'})")
        self.last_setpoints = {1: 0.0, 2: 0.0, 3: 0.0}
        self.flow_error_counters = {1: 0, 2: 0, 3: 0}
        self._poll_cycle_active = False

    # ---------- 내부: 워치독/연결 ----------
    async def _watchdog_loop(self):
        backoff = MFC_RECONNECT_BACKOFF_START_MS
        while self._want_connected:
            if self._connected:
                await asyncio.sleep(MFC_WATCHDOG_INTERVAL_MS / 1000.0)
                continue

            if self._ever_connected:
                await self._emit_status(f"재연결 시도 예약... ({backoff} ms)")
                await asyncio.sleep(backoff / 1000.0)

            if not self._want_connected:
                break

            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(MFC_TCP_HOST, MFC_TCP_PORT),
                    timeout=max(0.5, float(MFC_CONNECT_TIMEOUT_S))
                )
                self._reader, self._writer = reader, writer
                self._connected = True
                self._ever_connected = True
                backoff = MFC_RECONNECT_BACKOFF_START_MS

                # (선택) TCP keepalive
                try:
                    sock = writer.get_extra_info("socket")
                    if sock is not None:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                except Exception:
                    pass

                # 리더 태스크 시작
                if self._reader_task and not self._reader_task.done():
                    self._reader_task.cancel()
                    with contextlib.suppress(Exception):
                        await self._reader_task
                self._reader_task = asyncio.create_task(self._tcp_reader_loop(), name="MFC-TcpReader")

                self._last_connect_mono = time.monotonic()
                self._just_reopened = True
                await self._emit_status(f"{MFC_TCP_HOST}:{MFC_TCP_PORT} 연결 성공 (TCP)")
            except Exception as e:
                await self._emit_status(f"{MFC_TCP_HOST}:{MFC_TCP_PORT} 연결 실패: {e}")
                backoff = min(backoff * 2, MFC_RECONNECT_BACKOFF_MAX_MS)

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

        # 라인 큐 비움
        with contextlib.suppress(Exception):
            while True:
                self._line_q.get_nowait()

        self._dbg("MFC", "연결 끊김")

        # inflight 복구/취소
        if self._inflight is not None:
            cmd = self._inflight
            self._inflight = None
            if cmd.retries_left > 0:
                cmd.retries_left -= 1
                self._cmd_q.appendleft(cmd)
            else:
                self._safe_callback(cmd.callback, None)

    # ---------- 내부: 명령 워커 ----------
    async def _cmd_worker_loop(self):
        while True:
            await asyncio.sleep(0)  # cancel 친화
            if not self._cmd_q:
                await asyncio.sleep(0.01)
                continue
            if not self._connected or not self._writer:
                await asyncio.sleep(0.05)
                continue

            cmd = self._cmd_q.popleft()
            self._inflight = cmd
            sent_txt = cmd.cmd_str.strip()
            await self._emit_status(f"[SEND] {sent_txt} {('('+cmd.tag+')' if cmd.tag else '')}".strip())

            # 연결 직후에는 '한 번만' 조용히 기다리고(quiet), 강한 드레인은 금지
            if self._just_reopened and self._last_connect_mono > 0.0:
                remain = (self._last_connect_mono + (MFC_POST_OPEN_QUIET_MS / 1000.0)) - time.monotonic()
                if remain > 0:
                    await asyncio.sleep(remain)
                # 여기서는 드레인하지 않음: 초기 배너/ACK를 날려서 첫 응답 유실 가능
                self._just_reopened = False
            # 평상시에도 전송 직전 강제 드레인은 하지 않음
            # (잔여 라인은 읽기 루틴의 에코/접두사 처리로 흡수)

            # _cmd_worker_loop에서 write 직전, just_reopened가 아니면 짧게 비웁니다.
            # if not self._just_reopened:
            #     await self._absorb_late_lines(80)

            # write
            try:
                payload = cmd.cmd_str.encode("ascii", "ignore")
                self._writer.write(payload)
                await self._writer.drain()
            except Exception as e:
                self._dbg("MFC", f"{cmd.tag} {sent_txt} 전송 오류: {e}")
                self._inflight = None
                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    self._cmd_q.appendleft(cmd)
                else:
                    self._safe_callback(cmd.callback, None)
                self._on_tcp_disconnected()
                continue

            # no-reply
            if cmd.allow_no_reply:
                self._inflight = None
                await asyncio.sleep(cmd.gap_ms / 1000.0)
                # OS 버퍼 purge 대신 라인 큐만 '가볍게' 흡수 → 레이스 최소화
                #await self._absorb_late_lines(int(MFC_ALLOW_NO_REPLY_DRAIN_MS))
                self._safe_callback(cmd.callback, None)
                continue

            # wait reply (echo skip)
            try:
                line = await self._read_one_line_skip_echo(
                    sent_txt, 
                    cmd.timeout_ms / 1000.0,
                    expect_prefixes=cmd.expect_prefixes
                )
            except asyncio.TimeoutError:
                await self._emit_status(f"[TIMEOUT] {cmd.tag} {sent_txt}")
                self._inflight = None


                # ❌ 여기에서 retries_left로 재큐잉/재시도 하지 말 것
                # ❌ 마지막 실패에서 _on_tcp_disconnected()로 연결 끊지도 말 것

                # ✅ 고수준으로 "이번 시도는 실패" 알림만 보내고 끝
                self._safe_callback(cmd.callback, None)

                # 약간의 소프트 드레인으로 늦은 에코 흡수
                #await self._absorb_late_lines(100)
                await asyncio.sleep(max(0.05, cmd.gap_ms / 1000.0))
                continue

            # ✅ 여기부터가 "성공" 경로 (누락분)
            recv_txt = (line or "").strip()
            await self._emit_status(f"[RECV] {cmd.tag} ← {recv_txt}")
            self._safe_callback(cmd.callback, recv_txt)
            self._inflight = None
            await asyncio.sleep(cmd.gap_ms / 1000.0)

    async def _read_one_line_skip_echo(
        self,
        sent_no_cr: str,
        timeout_s: float,
        *,
        expect_prefixes: tuple[str, ...] = ()
    ) -> str:
        deadline = time.monotonic() + timeout_s
        while True:
            remain = max(0.0, deadline - time.monotonic())
            if remain <= 0:
                raise asyncio.TimeoutError()
            line = await asyncio.wait_for(self._line_q.get(), timeout=remain)
            if not line:
                continue
            # ① 현재 명령 에코
            if self._skip_echo_flag and line.strip() == sent_no_cr.strip():
                self._dbg("MFC", f"[ECHO] 현재 명령 에코 스킵: {line}")
                continue
            # ② 과거 no-reply 에코
            #    ✅ 검증/읽기(expect_prefixes 지정된 경우)에는 스킵하지 않는다!
            if (not expect_prefixes) and self._skip_echos and line == self._skip_echos[0]:
                self._skip_echos.popleft()
                await self._emit_status(f"[ECHO] 과거 no-reply 에코 스킵(큐): {line}")
                continue
            # ③ 접두사 필터링(있다면)
            s = (line or "").strip()
            if expect_prefixes and not any(s.startswith(p) for p in expect_prefixes):
                # 접두사 불일치 → 다음 라인을 더 읽어본다(타임아웃 내에서)
                self._dbg("MFC", f"[SKIP] 접두사 불일치: got={s[:12]!r}, need={expect_prefixes}")
                continue
            return line
        
    async def _tcp_reader_loop(self):
        assert self._reader is not None
        buf = bytearray()
        RX_MAX, LINE_MAX = 16*1024, 512
        try:
            while self._connected and self._reader:
                chunk = await self._reader.read(128)
                if not chunk:
                    break
                buf.extend(chunk)
                if len(buf) > RX_MAX:
                    del buf[:-RX_MAX]

                # CR/LF split
                while True:
                    i_cr, i_lf = buf.find(b"\r"), buf.find(b"\n")
                    if i_cr == -1 and i_lf == -1:
                        break
                    idx = i_cr if i_lf == -1 else (i_lf if i_cr == -1 else min(i_cr, i_lf))
                    line_bytes = buf[:idx]

                    drop = idx + 1
                    if drop < len(buf):
                        ch, nxt = buf[idx], buf[idx + 1]
                        if (ch == 13 and nxt == 10) or (ch == 10 and nxt == 13):
                            drop += 1
                    del buf[:drop]

                    if len(line_bytes) > LINE_MAX:
                        line_bytes = line_bytes[:LINE_MAX]

                    try:
                        line = line_bytes.decode("latin-1").strip()
                    except Exception:
                        line = ""

                    if line:
                        self._on_line_from_tcp(line)

                while buf[:1] in (b"\r", b"\n"):
                    del buf[0:1]
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._dbg("MFC", f"리더 루프 예외: {e!r}")
        finally:
            self._on_tcp_disconnected()

    def _on_line_from_tcp(self, line: str):
        # ⚠️ 리더 단계에서는 에코를 버리지 않는다.
        #    (검증/읽기 문맥은 워커에서 판단해 스킵)
        try:
            self._line_q.put_nowait(line)
        except asyncio.QueueFull:
            self._dbg("MFC", "라인 큐 포화 → 가장 오래된 라인 폐기")
            with contextlib.suppress(Exception):
                _ = self._line_q.get_nowait()
            with contextlib.suppress(Exception):
                self._line_q.put_nowait(line)

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
                # ★ 폴링 읽기가 이미 대기/진행 중이면 이번 틱 스킵
                if self._has_pending_poll_reads():
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
                            pct = float(vals[idx])                      # %FS
                            ui_sccm = await self._pct_to_sccm(ch, pct) # 화면 표시는 sccm
                            await self._emit_flow(name, ui_sccm)
                            self._monitor_flow(ch, pct)                # 모니터링은 %FS 기준

                # R5 → pressure 이벤트
                line = await self._send_and_wait_line(self._mk_cmd("READ_PRESSURE"),
                                                      tag="[POLL PRESS]", timeout_ms=MFC_TIMEOUT,
                                                      expect_prefixes=("P",))
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

                tol = target * float(FLOW_ERROR_TOLERANCE)  # %FS 기준
                ui_tgt = await self._pct_to_sccm(ch, target)
                ui_act = await self._pct_to_sccm(ch, -1 if actual is None else actual)

                await self._emit_status(
                    f"Flow 확인... (목표: {target:.2f}%FS/{ui_tgt:.2f}sccm, "
                    f"현재: {(-1 if actual is None else actual):.2f}%FS/{ui_act:.2f}sccm)"
                )

                if (actual is not None) and (abs(actual - target) <= tol):
                    await self._emit_confirmed("FLOW_ON")
                    self._stab_ch = None
                    self._stab_target_hw = 0.0
                    return
                
                self._stab_attempts += 1  # ✅ 시도 횟수 증가

                if self._stab_attempts >= 30:
                    await self._emit_failed("FLOW_ON", "유량 안정화 시간 초과")
                    self._stab_ch = None
                    self._stab_target_hw = 0.0
                    return

                await asyncio.sleep(MFC_STABILIZATION_INTERVAL_MS / 1000.0)
        except asyncio.CancelledError:
            pass

    # ---------- 내부: 고수준 시퀀스/검증 ----------
    async def _verify_flow_set(self, ch: int, pct_value: float) -> bool:
        for attempt in range(1, 6):
            line = await self._send_and_wait_line(
                self._mk_cmd("READ_FLOW_SET", channel=ch),
                tag=f"[VERIFY SET ch{ch}]", timeout_ms=MFC_TIMEOUT,
                expect_prefixes=(f"Q{4 + int(ch)}",)
            )
            val = self._parse_q_value_with_prefixes(line or "", prefixes=(f"Q{4 + int(ch)}",))
            ok = (val is not None) and (abs(val - pct_value) < 0.1)
            if ok:
                await self._emit_status(f"Ch{ch} 목표 {pct_value:.2f}%FS 설정 확인")
                return True

            # 재전송(여전히 %FS)
            self._enqueue(self._mk_cmd("FLOW_SET", channel=ch, value=round(pct_value, 2)),
                        None, allow_no_reply=True, tag=f"[RE-SET ch{ch}]")
            await self._emit_status(f"[FLOW_SET 검증 재시도] ch{ch}: 기대={pct_value:.2f}%FS, 응답={repr(line)} (시도 {attempt}/5)")
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

            # 설정값으로 드레인 시간 통일 (기본 80ms, 필요시 설정에서 조정)
            #await self._absorb_late_lines(int(MFC_ALLOW_NO_REPLY_DRAIN_MS))

            # 검증 (의미없는 빈 라인 방지용으로 최대 2회 읽기)
            now = ""
            for _ in range(2):
                verify_to = max(int(MFC_TIMEOUT), 2000)  # 최소 2초 권장
                line = await self._send_and_wait_line(
                    self._mk_cmd("READ_MFC_ON_OFF_STATUS"),
                    tag="[VERIFY R69]", timeout_ms=verify_to,
                    expect_prefixes=("L0","L")
                )
                now = self._parse_r69_bits(line or "")
                if not now and line:
                    hx = " ".join(f"{ord(c):02X}" for c in line[:16])
                    await self._emit_status(f"[R69 parse fail] raw={repr(line)} hex={hx}")
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
            line = await self._send_and_wait_line(
                self._mk_cmd("READ_VALVE_POSITION"),
                tag=f"[VERIFY VALVE {origin_cmd}]",
                timeout_ms=MFC_TIMEOUT, expect_prefixes=("V",)
            )

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
            line = await self._send_and_wait_line(
                self._mk_cmd("READ_SP1_VALUE"),
                tag="[VERIFY SP1_SET]", timeout_ms=MFC_TIMEOUT, expect_prefixes=("S1",)
            )
            cur_hw = self._parse_sp_value(line or "", 1)  # ✅ S1 응답 전용
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

    async def _verify_sp_active(self, sp_index: int) -> bool:
        """D{n} 후 R37=MXYZ 읽어 Z코드로 SP 활성 확인(Z: 3~7 → SP1~SP5)."""
        cmd_key = f"SP{sp_index}_ON"
        self._enqueue(self._mk_cmd(cmd_key), None, allow_no_reply=True, tag=f"[{cmd_key}]")
        want_z = str(2 + sp_index)  # SP1→'3', SP4→'6'
        for attempt in range(1, 6):
            line = await self._send_and_wait_line(
                self._mk_cmd("READ_SYSTEM_STATUS"),  # R37
                tag=f"[VERIFY {cmd_key}]", timeout_ms=MFC_TIMEOUT,
                expect_prefixes=("M",)
            )
            s = (line or "").strip().upper()
            # MXYZ → Z만 취득
            m = re.match(r'^M(.)(.)(.)$', s)
            ok = bool(m and m.group(3) == want_z)
            if ok:
                await self._emit_status(f"{cmd_key} 활성화 확인")
                return True
            await asyncio.sleep(MFC_DELAY_MS / 1000.0)
        return False

    # ---------- 내부: 단위 파서/도우미 ----------
    async def _read_r60_values(self, tag: str = "[READ R60]") -> Optional[list[float]]:
        line = await self._send_and_wait_line(
            self._mk_cmd("READ_FLOW_ALL"),
            tag=tag, timeout_ms=MFC_TIMEOUT,
            expect_prefixes=("Q0",)
        )
        return self._parse_r60_values(line or "")

    async def _read_r69_bits(self) -> Optional[str]:
        # PLC/IG와 동시 동작/폴링 중 레이턴시를 감안해 최소 3000ms 보장
        verify_to = max(int(MFC_TIMEOUT), 3000)
        line = await self._send_and_wait_line(
            self._mk_cmd("READ_MFC_ON_OFF_STATUS"),
            tag="[READ R69]", timeout_ms=verify_to,
            retries=5, expect_prefixes=("L0", "L")
        )
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
        if not s:
            return ""

        # 제어문자/공백류 정리
        s = "".join(ch for ch in s if ch >= " " and ch != "\x7f")
        up = s.upper()

        # 'L'을 앵커로 삼아 그 뒤만 사용 (앞에 노이즈 있을 수 있음)
        i = up.find("L")
        if i != -1:
            up = up[i+1:]
        # 바로 뒤에 오는 선택적 '0' 또는 'O' 제거 (L0..., LO... 모두 허용)
        if up[:1] in ("0", "O"):
            up = up[1:]

        # 헷갈리는 글자들을 숫자로 정규화
        trans = str.maketrans({
            "O": "0",    # 알파벳 O
            "I": "1",    # 대문자 I
            "L": "1",    # 대문자 L (혹시 남아있을 때)
            "|": "1",    # 파이프
            "0": "0",    # 안전하게 재지정
        })
        up = up.translate(trans)

        # 0/1만 남기고 앞에서 4비트 취득
        bits = "".join(ch for ch in up if ch in "01")
        return bits[:4]

    def _parse_valve_ok(self, origin_cmd: str, line: str) -> bool:
        s = (line or "").strip()
        m = re.match(r'^(?:V\s*)?\+?([+\-]?\d+(?:\.\d+)?)$', s)
        pos = float(m.group(1)) if m else None
        if pos is None:
            return False
        return (origin_cmd == "VALVE_CLOSE" and pos < 1.0) or (origin_cmd == "VALVE_OPEN" and pos > 99.0)
    
    def _parse_sp_value(self, line: str, sp_index: int) -> Optional[float]:
        s = (line or "").strip().upper()
        m = re.match(rf'^S{int(sp_index)}\s*\+?([+\-]?\d+(?:\.\d+)?)$', s)
        if not m:
            return None
        try:
            return float(m.group(1))
        except Exception:
            return None

    def _parse_pressure_percent(self, line: str) -> Optional[float]:
        """'P' 접두사의 %FS만 파싱."""
        s = (line or "").strip().upper()
        m = re.match(r'^P\s*\+?([+\-]?\d+(?:\.\d+)?)$', s)
        if not m:
            return None
        try:
            return float(m.group(1))  # %FS
        except Exception:
            return None

    def _emit_pressure_from_line_sync(self, line: str):
        pct = self._parse_pressure_percent(line)
        if pct is None:
            return
        val_ui = (pct / 100.0) * self.SENSOR_FS_TORR  # ★ __init__에서 주입된 FS 사용
        fmt = "{:." + str(int(MFC_PRESSURE_DECIMALS)) + "f}"
        text = fmt.format(val_ui)
        self._ev_nowait(MFCEvent(kind="pressure", value=val_ui, text=text))

    def _monitor_flow(self, channel: int, actual_pct: float):
        target_pct = float(self.last_setpoints.get(channel, 0.0))  # %FS
        if target_pct < 0.1:
            self.flow_error_counters[channel] = 0
            return
        if abs(actual_pct - target_pct) > (target_pct * float(FLOW_ERROR_TOLERANCE)):
            self.flow_error_counters[channel] += 1
            if self.flow_error_counters[channel] >= int(FLOW_ERROR_MAX_COUNT):
                self._ev_nowait(MFCEvent(
                    kind="status",
                    message=f"Ch{channel} 유량 불안정! (목표: {target_pct:.2f}%FS)"
                ))
                self.flow_error_counters[channel] = 0
        else:
            self.flow_error_counters[channel] = 0

    # ---------- 내부: 공통 송수신 ----------
    def _enqueue(self, cmd_str: str, on_reply: Optional[Callable[[Optional[str]], None]],
                *, timeout_ms: int = MFC_TIMEOUT, gap_ms: int = MFC_GAP_MS,
                tag: str = "", retries_left: int = 5, allow_no_reply: bool = False,
                expect_prefixes: tuple[str, ...] = ()):
        if not cmd_str.endswith(self._tx_eol_str):
            cmd_str += self._tx_eol_str
        self._cmd_q.append(Command(
            cmd_str, on_reply, timeout_ms, gap_ms, tag, retries_left, allow_no_reply,
            expect_prefixes=expect_prefixes
        ))

        # ★ no-reply 명령의 '에코 라인'은 나중에 도착해도 스킵하도록 등록
        if allow_no_reply:
            no_eol = cmd_str[:-len(self._tx_eol_str)] if cmd_str.endswith(self._tx_eol_str) else cmd_str
            self._skip_echos.append(no_eol)
            if len(self._skip_echos) > 64:
                self._skip_echos.popleft()
            # 추적 로그를 UI/챗으로도 올림
            self._dbg("MFC", f"[ECHO] no-reply 등록: {no_eol}")

    async def _send_and_wait_line(
        self, cmd_str: str, *, tag: str, timeout_ms: int, retries: int = 1,
        expect_prefixes: tuple[str, ...] = (),
    ) -> Optional[str]:
        """읽기 타임아웃/무응답에 대해서도 'retries' 횟수만큼 재시도."""
        attempts = max(1, int(retries))
        for attempt in range(1, attempts + 1):
            fut: asyncio.Future[Optional[str]] = asyncio.get_running_loop().create_future()

            def _cb(line: Optional[str]):
                if line is None:
                    if not fut.done():
                        fut.set_result(None)
                    return
                s = (line or "").strip()
                if not fut.done():
                    fut.set_result(s)

            self._enqueue(
                cmd_str, _cb,
                timeout_ms=timeout_ms, gap_ms=MFC_GAP_MS,
                tag=tag if attempt == 1 else f"{tag} (retry {attempt}/{attempts})",
                retries_left=0,                     # 전송 오류 재시도는 워커에 맡기지 않음(읽기 재시도는 여기서)
                allow_no_reply=False,
                expect_prefixes=expect_prefixes
            )

            extra = 0.0
            if self._last_connect_mono > 0.0 and (time.monotonic() - self._last_connect_mono) < 2.0:
                extra = MFC_FIRST_CMD_EXTRA_TIMEOUT_MS / 1000.0

            try:
                res = await asyncio.wait_for(fut, timeout=(timeout_ms / 1000.0) + 2.0 + extra)
                if res:
                    return res
            except asyncio.TimeoutError:
                # pass → 다음 루프로 재시도
                pass

            # 짧은 소프트 드레인 & 간격 (늦게 도착한 에코/라인 흡수)
            await self._absorb_late_lines(80)
            await asyncio.sleep(max(0.05, MFC_GAP_MS / 1000.0))

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
    
    def _has_pending_poll_reads(self) -> bool:
        """인플라이트/큐에 폴링 읽기(R60/R5)가 있으면 True."""
        if self._inflight and self._is_poll_read_cmd(self._inflight.cmd_str, self._inflight.tag):
            return True
        for c in self._cmd_q:
            if self._is_poll_read_cmd(c.cmd_str, c.tag):
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

    async def _ensure_mfc_meta(self, ch: int) -> tuple[float, int]:
        """채널 FS/UNIT 캐시 보장. UNIT: 1=SCCM, 2=SLM"""
        if ch not in self._mfc_fs:
            line = await self._send_and_wait_line(
                self._mk_cmd("READ_MFC_FS", ch), tag=f"[READ FS ch{ch}]", timeout_ms=MFC_TIMEOUT
            )
            fs = self._parse_simple_number(line or "")
            if fs is None:
                raise RuntimeError(f"Ch{ch} FS 읽기 실패: {repr(line)}")
            self._mfc_fs[ch] = float(fs)

        if ch not in self._mfc_unit:
            line = await self._send_and_wait_line(
                self._mk_cmd("READ_MFC_UNIT", ch), tag=f"[READ UNIT ch{ch}]", timeout_ms=MFC_TIMEOUT
            )
            unit_code = self._parse_unit_code(line or "")
            if unit_code not in (1, 2):
                raise RuntimeError(f"Ch{ch} UNIT 읽기 실패: {repr(line)}")
            self._mfc_unit[ch] = unit_code

        return self._mfc_fs[ch], self._mfc_unit[ch]
    
    async def _ensure_all_off_on_start(self) -> bool:
        """
        공정 시작용: R69 선조회 없이 L0 0000 한 번 전송 → R69 한 번만 읽어 확인.
        (재시도/루프 없음)
        """
        # (선택) 시작 시 모니터/목표 리셋
        self.last_setpoints = {1: 0.0, 2: 0.0, 3: 0.0}
        self.flow_error_counters = {1: 0, 2: 0, 3: 0}

        # 1) L0 0000 전송(no-reply)
        self._enqueue(self._mk_cmd("SET_ONOFF_MASK", "0000"), None,
                    allow_no_reply=True, tag="[L0 0000]")

        # 2) 장비 반영 대기(기존 딜레이 재사용; 최소 200ms 보장)
        await asyncio.sleep(max(MFC_DELAY_MS, 200) / 1000.0)

        # ✅ (중요) 과거 L0 에코가 남아있을 수 있으므로 짧게 드레인
        await self._absorb_late_lines(80)

        # 3) R69 한 번만 읽어서 확인(최소 2초 타임아웃 권장)
        verify_to = max(int(MFC_TIMEOUT), 2000)
        line = await self._send_and_wait_line(
            self._mk_cmd("READ_MFC_ON_OFF_STATUS"),
            tag="[VERIFY R69 1shot]", timeout_ms=verify_to,
            expect_prefixes=("L0", "L"),  # L0/L 접두 허용
        )
        now = self._parse_r69_bits(line or "")

        ok = (now == "0000")
        await self._emit_status(
            "ensure_all_off_on_start: L0→R69 1회 확인 "
            + ("성공(0000)" if ok else f"불일치(now={now or '∅'})")
        )
        return ok
    
    async def _all_off_oneshot(self) -> bool:
        """L0 0000 한 번 전송하고 R69 한 번만 읽어 '0000'인지 확인."""
        self._enqueue(self._mk_cmd("SET_ONOFF_MASK", "0000"), None,
                    allow_no_reply=True, tag="[L0 0000]")

        await asyncio.sleep(max(MFC_DELAY_MS, 200) / 1000.0)
        await self._absorb_late_lines(80)  # ✅ 과거 L0 에코 제거

        verify_to = max(int(MFC_TIMEOUT), 2000)
        line = await self._send_and_wait_line(
            self._mk_cmd("READ_MFC_ON_OFF_STATUS"),
            tag="[VERIFY R69 1shot]", timeout_ms=verify_to,
            expect_prefixes=("L0", "L"),
        )
        now = self._parse_r69_bits(line or "")
        ok = (now == "0000")
        await self._emit_status("ALL_OFF(1shot): " + ("OK" if ok else f"NG(now={now or '∅'})"))
        return ok

    def _fs_in_sccm(self, fs_value: float, unit_code: int) -> float:
        # UNIT: 1=SCCM, 2=SLM
        return float(fs_value) * (1000.0 if unit_code == 2 else 1.0)

    async def _pct_to_sccm(self, ch: int, pct: float) -> float:
        fs, unit = await self._ensure_mfc_meta(ch)
        return (float(pct) / 100.0) * self._fs_in_sccm(fs, unit)

    async def _sccm_to_pct(self, ch: int, sccm: float) -> float:
        fs, unit = await self._ensure_mfc_meta(ch)
        fs_sccm = self._fs_in_sccm(fs, unit)
        if fs_sccm <= 0:
            raise RuntimeError(f"Ch{ch} FS=0")
        return max(0.0, (float(sccm) / fs_sccm) * 100.0)

    def _parse_simple_number(self, line: str) -> Optional[float]:
        s = (line or "").strip().upper()
        # 예: "F1 +500.0" / "FS 500.0" / "+500.0"
        m = re.match(r'^(?:[A-Z0-9]+\s+)?\+?([+\-]?\d+(?:\.\d+)?)$', s)
        if not m: return None
        try:
            return float(m.group(1))
        except Exception:
            return None

    def _parse_unit_code(self, line: str) -> Optional[int]:
        s = (line or "").strip().upper()
        # 예: "U1", "UNIT 1", "1"
        m = re.match(r'^(?:[A-Z]+\s*)?([12])$', s)
        if not m: return None
        return int(m.group(1))


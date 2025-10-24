# -*- coding: utf-8 -*-
"""
mfc.py — asyncio 기반 MFC 컨트롤러 (MOXA NPort TCP Server 직결)

의존성: 표준 라이브러리만 사용 (pyserial-asyncio 불필요)

기능 요약(구 MFC.py와 동등):
  - asyncio TCP streams + 자체 라인 프레이밍(CR/LF) 통신
  - 단일 명령 큐(타임아웃/재시도/인터커맨드 gap) → 송수신 충돌 제거
  - 연결 워치독(지수 백오프) → 중간 단선도 자동 복구
  - 폴링: 주기마다 R60(전체 GAS) → R5(압력) 한 사이클, 중첩 금지
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

from lib import config_common as cfgc # ★ 추가
from lib.config_ch2 import (
    MFC_TCP_HOST, MFC_TCP_PORT, MFC_TX_EOL, MFC_SKIP_ECHO, MFC_CONNECT_TIMEOUT_S,
    MFC_COMMANDS, FLOW_ERROR_TOLERANCE, FLOW_ERROR_MAX_COUNT, MFC_SCALE_FACTORS, 
    MFC_POLLING_INTERVAL_MS, MFC_STABILIZATION_INTERVAL_MS, MFC_WATCHDOG_INTERVAL_MS, 
    MFC_RECONNECT_BACKOFF_START_MS, MFC_RECONNECT_BACKOFF_MAX_MS, MFC_TIMEOUT, MFC_GAP_MS, 
    MFC_DELAY_MS, MFC_DELAY_MS_VALVE, DEBUG_PRINT, MFC_PRESSURE_SCALE, MFC_PRESSURE_DECIMALS,
    MFC_SP1_VERIFY_TOL, MFC_POST_OPEN_QUIET_MS, MFC_ALLOW_NO_REPLY_DRAIN_MS,
    MFC_FIRST_CMD_EXTRA_TIMEOUT_MS
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
    def __init__(self, *, enable_verify: bool = True, enable_stabilization: Optional[bool] = None,
                 host: Optional[str] = None, port: Optional[int] = None, scale_factors: Optional[dict[int, float]] = None):
        self.debug_print = DEBUG_PRINT

        # ← 런타임에서 채널별로 덮어쓸 TCP 엔드포인트(없으면 config 기본값 사용)
        self._override_host: Optional[str] = host
        self._override_port: Optional[int] = port

        # ★ 인스턴스별 스케일 맵(없으면 기존 전역값 사용)
        self.scale_factors: dict[int, float] = dict(scale_factors or MFC_SCALE_FACTORS)

        # ▼ 추가: 검증/안정화 플래그
        self._verify_enabled: bool = bool(enable_verify)
        self._stab_enabled: bool = (self._verify_enabled if enable_stabilization is None
                                    else bool(enable_stabilization))

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
        self._wd_paused: bool = False    # ← 추가 (워치독 일시정지 상태)

        # 재연결 백오프
        self._reconnect_backoff_ms = MFC_RECONNECT_BACKOFF_START_MS

        # 런타임/스케일/모니터링
        self.gas_map = {1: "Ar", 2: "O2", 3: "N2"}
        self.last_setpoints = {1: 0.0, 2: 0.0, 3: 0.0}      # 장비 단위(HW)
        self.flow_error_counters = {1: 0, 2: 0, 3: 0}

        # ⬇ PlasmaCleaning 전용: 선택된 가스 채널 기억
        self._selected_ch: Optional[int] = None

        # 폴링 사이클 중첩 방지 플래그
        self._poll_cycle_active: bool = False

        # ★ 과거 no-reply 명령의 에코를 1회성으로 버리기 위한 대기열
        self._skip_echos: deque[str] = deque()

        # 안정화 상태
        self._stab_ch: Optional[int] = None
        self._stab_target_hw: float = 0.0
        self._stab_attempts: int = 0
        self._stab_pending_cmd: Optional[str] = None  # FLOW_ON 확정 시점 관리

        # Qt의 clear+soft-drain 타이밍을 모사하기 위한 플래그
        self._last_connect_mono: float = 0.0
        self._just_reopened: bool = False

        # ★ Inactivity 전략 필드
        self._inactivity_s: float = float(getattr(cfgc, "MFC_INACTIVITY_REOPEN_S", 0.0))
        self._last_io_mono: float = 0.0

# =============== debug, R69 하지 않는 ==================
        # 현재 ON/OFF 상태를 R69 없이 자체 추적하기 위한 섀도우 마스크(좌→우: ch1..ch4)
        self._mask_shadow: str = "0000"
# =============== debug, R69 하지 않는 ==================

    def is_connected(self) -> bool:
        """프리플라이트/상태 체크용: 현재 TCP 연결 여부를 반환."""
        return bool(self._connected)

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

    async def connect(self):
        """start()와 동일 의미의 별칭 — 호출측 일관성 확보."""
        await self.start()

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

        # TCP 종료 (결정적 종료: wait_closed 대기 + 라인큐/에코큐 비움)
        if self._reader_task:
            self._reader_task.cancel()
            with contextlib.suppress(Exception):
                await self._reader_task
            self._reader_task = None

        if self._writer:
            try:
                self._writer.close()
                # IG와 동일 패턴: 종료 확정 대기
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(self._writer.wait_closed(), timeout=1.5)
            except Exception:
                pass

        self._reader = None
        self._writer = None
        self._connected = False

        # 잔류 라인 제거 (표준 종료 경로와 동등한 청소)
        with contextlib.suppress(Exception):
            while True:
                self._line_q.get_nowait()

        # 과거 no-reply 에코 대기열도 초기화
        try:
            self._skip_echos.clear()
        except Exception:
            pass

        # ★★★ IG와 동일: NPort 포트 강제 해제 추가 (Windows 전용)
        # try:
        #     await self._force_release_nport_port()
        # except Exception as e:
        #     await self._emit_status(f"IPSerial reset skip/fail: {e!r}")

        await self._emit_status("MFC 연결 종료됨")

    async def cleanup_quick(self):
        """빠른 종료 경로가 필요할 때 호출 — 현 단계에서는 cleanup에 위임."""
        await self.cleanup()

    async def events(self) -> AsyncGenerator[MFCEvent, None]:
        """상위에서 소비하는 이벤트 스트림."""
        while True:
            ev = await self._event_q.get()
            yield ev

    # ---- 고수준 제어 API (기존 handle_command 세분화) ----
    async def set_flow(self, channel: int, ui_value: float):
        """FLOW_SET + (옵션) READ_FLOW_SET 검증."""
        sf = float(self.scale_factors.get(channel, 1.0))
        scaled = float(ui_value) * sf
        await self._emit_status(f"Ch{channel} GAS 스케일: {ui_value:.2f}sccm → 장비 {scaled:.2f}")

        # SET (no-reply)
        set_cmd = self._mk_cmd("FLOW_SET", channel=channel, value=scaled)
        self._enqueue(set_cmd, None, allow_no_reply=True, tag=f"[SET ch{channel}]")

        # ▼ 검증 비활성화면 즉시 확정
        if not self._verify_enabled:
            self.last_setpoints[channel] = scaled
            await self._emit_confirmed("FLOW_SET")
            return

        # 검증
        ok = await self._verify_flow_set(channel, scaled)
        if ok:
            self.last_setpoints[channel] = scaled
            await self._emit_confirmed("FLOW_SET")
        else:
            await self._emit_failed("FLOW_SET", f"Ch{channel} FLOW_SET 확인 실패")

    async def flow_on(self, channel: int):
        """R69를 읽지 않고 내부 섀도우 마스크만 갱신하여 L0 적용."""
        # 안정화 상태 초기화
        await self._cancel_task("_stab_task")
        self._stab_ch = None
        self._stab_target_hw = 0.0
        self._stab_pending_cmd = None

        '''
        비트 마스크 사용 -> 단일 채널로(Plasma Cleaning 때문)
        # 섀도우 마스크에서 해당 채널만 1로 켜기
        target = self._mask_set(channel, True)

        # L0 전송(no-reply) → 섀도우 갱신
        self._enqueue(self._mk_cmd("SET_ONOFF_MASK", target), None,
                      allow_no_reply=True, tag=f"[L0 {target}]", gap_ms=4000) # flow 검증을 안하니 여유있게
        self._mask_shadow = target
        '''

        # 섀도우 마스크 갱신(내부 상태 유지용)
        target = self._mask_set(channel, True)

        # ▶ 개별 채널 ON (L{ch}1) — 마스크(L0) 금지
        self._enqueue(self._mk_cmd("FLOW_ON", channel=channel), None,
                    allow_no_reply=True, tag=f"[FLOW_ON ch{channel}]")
        self._mask_shadow = target

        # 장비 반영 대기(예전 코드와 동일한 최소 대기 보장)
        await asyncio.sleep(max(MFC_DELAY_MS, 200) / 1000.0)

        # (옵션) 안정화 루프 유지 — R69 없이도 R60 기반 안정화는 가능
        if self._stab_enabled:
            tgt = float(self.last_setpoints.get(channel, 0.0))
            if tgt > 0:
                self._stab_ch = channel
                self._stab_target_hw = tgt
                self._stab_attempts = 0
                self._stab_pending_cmd = "FLOW_ON"
                self._stab_task = asyncio.create_task(self._stabilization_loop())
                await self._emit_status(f"FLOW_ON: ch{channel} 안정화 시작 (목표 HW {tgt:.2f})")
                return

        await self._emit_confirmed("FLOW_ON")

    async def flow_off(self, channel: int):
        """R69를 읽지 않고 내부 섀도우 마스크만 갱신하여 L0 적용."""
        # 해당 채널 안정화 중이면 중단
        if self._stab_ch == channel:
            await self._cancel_task("_stab_task")
            self._stab_ch = None
            self._stab_target_hw = 0.0
            self._stab_pending_cmd = None
            await self._emit_status(f"FLOW_OFF 요청: ch{channel} 안정화 취소")

        # 목표 GAS/모니터링 카운터 리셋
        self.last_setpoints[channel] = 0.0
        self.flow_error_counters[channel] = 0

        '''
        비트 마스크 사용 -> 단일 채널로(Plasma Cleaning 때문)
        # 섀도우 마스크에서 해당 채널만 0으로 끄기
        target = self._mask_set(channel, False)

        # L0 전송(no-reply) → 섀도우 갱신
        self._enqueue(self._mk_cmd("SET_ONOFF_MASK", target), None,
                      allow_no_reply=True, tag=f"[L0 {target}]")
        self._mask_shadow = target
        '''

        # 섀도우 마스크 갱신(내부 상태 유지용)
        target = self._mask_set(channel, False)

        # ▶ 개별 채널 OFF (L{ch}0) — 마스크(L0) 금지
        self._enqueue(self._mk_cmd("FLOW_OFF", channel=channel), None,
                    allow_no_reply=True, tag=f"[FLOW_OFF ch{channel}]")
        self._mask_shadow = target

        # 장비 반영 대기 후 확정
        await asyncio.sleep(max(MFC_DELAY_MS, 200) / 1000.0)
        await self._emit_confirmed("FLOW_OFF")

    # === PlasmaCleaning: 선택 가스 전용 API (L{ch}{1/0} 개별 명령 사용) ===
    async def gas_select(self, gas_idx: int) -> None:
        gi = int(gas_idx)
        if gi not in self.gas_map:
            raise ValueError(f"지원하지 않는 가스 채널: {gas_idx}")
        self._selected_ch = gi
        await self._emit_status(f"가스 선택: ch{gi} ({self.gas_map.get(gi, '-')})")

    def _require_selected_ch(self) -> int:
        gi = int(self._selected_ch or 0)
        if gi not in self.gas_map:
            raise RuntimeError("선택된 가스가 없습니다. gas_select()를 먼저 호출하세요.")
        return gi

    async def flow_set_on(self, ui_value: float) -> None:
        """선택 채널에 FLOW_SET → FLOW_ON(개별 명령)"""
        ch = self._require_selected_ch()
        await self.set_flow(ch, float(ui_value))
        # 개별 ON (마스크 L0 금지)
        self._enqueue(self._mk_cmd("FLOW_ON", channel=ch), None,
                    allow_no_reply=True, tag=f"[FLOW_ON ch{ch}]")
        # (옵션) 기존 안정화 루프 재사용
        if self._stab_enabled:
            tgt = float(self.last_setpoints.get(ch, 0.0))
            if tgt > 0:
                await self._cancel_task("_stab_task")
                self._stab_ch = ch
                self._stab_target_hw = tgt
                self._stab_attempts = 0
                self._stab_pending_cmd = "FLOW_ON"
                self._stab_task = asyncio.create_task(self._stabilization_loop())
                await self._emit_status(f"FLOW_ON: ch{ch} 안정화 시작 (목표 HW {tgt:.2f})")
                return
        await asyncio.sleep(max(MFC_DELAY_MS, 200) / 1000.0)
        await self._emit_confirmed("FLOW_ON")

    async def flow_off_selected(self) -> None:
        """선택 채널만 FLOW_OFF(개별 명령)"""
        ch = self._require_selected_ch()
        if self._stab_ch == ch:
            await self._cancel_task("_stab_task")
            self._stab_ch = None
            self._stab_target_hw = 0.0
            self._stab_pending_cmd = None
            await self._emit_status(f"FLOW_OFF: ch{ch} 안정화 취소")
        self.last_setpoints[ch] = 0.0
        self.flow_error_counters[ch] = 0
        self._enqueue(self._mk_cmd("FLOW_OFF", channel=ch), None,
                    allow_no_reply=True, tag=f"[FLOW_OFF ch{ch}]")
        await asyncio.sleep(max(MFC_DELAY_MS, 200) / 1000.0)
        await self._emit_confirmed("FLOW_OFF")

    # async def flow_on(self, channel: int):
    #     """R69 → L0 적용, (옵션) 검증, (옵션) 안정화 → 확정."""
    #     now = await self._read_r69_bits()
    #     if not now:
    #         await self._emit_failed("FLOW_ON", "R69 읽기 실패")
    #         return

    #     bits = list(now.ljust(5, '0'))
    #     if 1 <= channel <= len(bits):
    #         bits[channel-1] = '1'
    #     target = ''.join(bits[:5])

    #     # 안정화 상태 초기화
    #     await self._cancel_task("_stab_task")
    #     self._stab_ch = None
    #     self._stab_target_hw = 0.0
    #     self._stab_pending_cmd = None

    #     if not self._verify_enabled:
    #         # 검증 없이 마스크만 적용 후 확정
    #         self._enqueue(self._mk_cmd("SET_ONOFF_MASK", target), None,
    #                     allow_no_reply=True, tag=f"[L0 {target}]")
    #         await asyncio.sleep(max(MFC_DELAY_MS, 200) / 1000.0)
    #         await self._emit_confirmed("FLOW_ON")
    #         return

    #     ok = await self._set_onoff_mask_and_verify(target)
    #     if not ok:
    #         await self._emit_failed("FLOW_ON", "L0 적용 불일치(now!=want)")
    #         return

    #     # 안정화 스킵 모드면 바로 확정
    #     if not self._stab_enabled:
    #         await self._emit_confirmed("FLOW_ON")
    #         return

    #     # 안정화 필요 시 시작
    #     if 1 <= channel <= len(target) and target[channel-1] == '1':
    #         tgt = float(self.last_setpoints.get(channel, 0.0))
    #         if tgt > 0:
    #             self._stab_ch = channel
    #             self._stab_target_hw = tgt
    #             self._stab_attempts = 0
    #             self._stab_pending_cmd = "FLOW_ON"
    #             self._stab_task = asyncio.create_task(self._stabilization_loop())
    #             await self._emit_status(f"FLOW_ON: ch{channel} 안정화 시작 (목표 HW {tgt:.2f})")
    #             return

    #     await self._emit_confirmed("FLOW_ON")

    # async def flow_off(self, channel: int):
    #     """R69 → L0 적용, (옵션) 검증 → 확정."""
    #     if self._stab_ch == channel:
    #         await self._cancel_task("_stab_task")
    #         self._stab_ch = None
    #         self._stab_target_hw = 0.0
    #         self._stab_pending_cmd = None
    #         await self._emit_status(f"FLOW_OFF 요청: ch{channel} 안정화 취소")

    #     self.last_setpoints[channel] = 0.0
    #     self.flow_error_counters[channel] = 0

    #     now = await self._read_r69_bits()
    #     if not now:
    #         await self._emit_failed("FLOW_OFF", "R69 읽기 실패")
    #         return

    #     bits = list(now.ljust(5, '0'))
    #     if 1 <= channel <= len(bits):
    #         bits[channel-1] = '0'
    #     target = ''.join(bits[:5])

    #     if not self._verify_enabled:
    #         self._enqueue(self._mk_cmd("SET_ONOFF_MASK", target), None,
    #                     allow_no_reply=True, tag=f"[L0 {target}]")
    #         await asyncio.sleep(max(MFC_DELAY_MS, 200) / 1000.0)
    #         await self._emit_confirmed("FLOW_OFF")
    #         return

    #     ok = await self._set_onoff_mask_and_verify(target)
    #     if ok:
    #         await self._emit_confirmed("FLOW_OFF")
    #     else:
    #         await self._emit_failed("FLOW_OFF", "L0 적용 불일치")

    async def valve_open(self):
        if not self._verify_enabled:
            self._enqueue(self._mk_cmd("VALVE_OPEN"), None, allow_no_reply=True, tag="[VALVE_OPEN]")
            await asyncio.sleep(MFC_DELAY_MS_VALVE / 1000.0)
            await self._emit_confirmed("VALVE_OPEN")
            return
        await self._valve_move_and_verify("VALVE_OPEN")

    async def valve_close(self):
        if not self._verify_enabled:
            self._enqueue(self._mk_cmd("VALVE_CLOSE"), None, allow_no_reply=True, tag="[VALVE_CLOSE]")
            await asyncio.sleep(MFC_DELAY_MS_VALVE / 1000.0)
            await self._emit_confirmed("VALVE_CLOSE")
            return
        await self._valve_move_and_verify("VALVE_CLOSE")

    async def sp1_set(self, ui_value: float):
        """SP1_SET (UI→HW 변환) + (옵션) READ_SP1_VALUE 검증."""
        hw_val = round(float(ui_value) * float(MFC_PRESSURE_SCALE), int(MFC_PRESSURE_DECIMALS))
        await self._emit_status(f"SP1 스케일: UI {ui_value:.2f} → 장비 {hw_val:.{int(MFC_PRESSURE_DECIMALS)}f}")

        self._enqueue(self._mk_cmd("SP1_SET", value=hw_val), None, allow_no_reply=True, tag="[SP1_SET]")

        if not self._verify_enabled:
            await asyncio.sleep(MFC_GAP_MS / 1000.0)
            await self._emit_confirmed("SP1_SET")
            return

        ok = await self._verify_sp1_set(hw_val, ui_value)
        if ok: await self._emit_confirmed("SP1_SET")
        else:  await self._emit_failed("SP1_SET", "SP1 설정 확인 실패")

    async def sp4_set(self, ui_value: float):
        """SP4_SET (UI→HW 변환) + (옵션) READ_SP4_VALUE 검증."""
        hw_val = round(float(ui_value) * float(MFC_PRESSURE_SCALE), int(MFC_PRESSURE_DECIMALS))
        await self._emit_status(
            f"SP4 스케일: UI {ui_value:.2f} → 장비 {hw_val:.{int(MFC_PRESSURE_DECIMALS)}f}"
        )

        # 설정 전송 (no-reply)
        self._enqueue(self._mk_cmd("SP4_SET", value=hw_val), None,
                      allow_no_reply=True, tag="[SP4_SET]")

        # 검증 비활성화면 즉시 확정
        if not self._verify_enabled:
            await asyncio.sleep(MFC_GAP_MS / 1000.0)
            await self._emit_confirmed("SP4_SET")
            return

        # 검증 (READ_SP4_VALUE가 정의되어 있지 않으면 스킵하고 통과)
        ok = await self._verify_sp_set(4, hw_val, ui_value)
        if ok: await self._emit_confirmed("SP4_SET")
        else:  await self._emit_failed("SP4_SET", "SP4 설정 확인 실패")


    async def sp1_on(self):
        if not self._verify_enabled:
            self._enqueue(self._mk_cmd("SP1_ON"), None, allow_no_reply=True, tag="[SP1_ON]")
            await asyncio.sleep(MFC_GAP_MS / 1000.0)
            await self._emit_confirmed("SP1_ON")
            return
        ok = await self._verify_simple_flag("SP1_ON", expect_mask='1')
        if ok: await self._emit_confirmed("SP1_ON")
        else:  await self._emit_failed("SP1_ON", "SP1 상태 확인 실패")

    async def sp3_on(self):
        if not self._verify_enabled:
            self._enqueue(self._mk_cmd("SP3_ON"), None, allow_no_reply=True, tag="[SP3_ON]")
            await asyncio.sleep(MFC_GAP_MS / 1000.0)
            await self._emit_confirmed("SP3_ON")
            return
        ok = await self._verify_simple_flag("SP3_ON", expect_mask='3')
        if ok: await self._emit_confirmed("SP3_ON")
        else:  await self._emit_failed("SP3_ON", "SP3 상태 확인 실패")

    async def sp4_on(self):
        if not self._verify_enabled:
            self._enqueue(self._mk_cmd("SP4_ON"), None, allow_no_reply=True, tag="[SP4_ON]")
            await asyncio.sleep(MFC_GAP_MS / 1000.0)
            await self._emit_confirmed("SP4_ON")
            return
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
                sf = float(self.scale_factors.get(ch, 1.0))
                await self._emit_flow(name, v_hw / sf)
                self._monitor_flow(ch, v_hw)

    async def read_pressure(self):
        """R5(예: READ_PRESSURE) 읽고 UI 문자열/숫자로 이벤트."""
        line = await self._send_and_wait_line(
            self._mk_cmd("READ_PRESSURE"),
            tag="[READ_PRESSURE]", timeout_ms=MFC_TIMEOUT,
            expect_prefixes=("P",)  
        )
        if not (line and line.strip()):
            await self._emit_failed("READ_PRESSURE", "응답 없음")
            return
        self._emit_pressure_from_line_sync(line.strip())

    async def handle_command(self, cmd: str, args: dict | None = None) -> None:
        """
        main/process에서 넘어오는 문자열 명령을 고수준 메서드로 라우팅한다.
        - cmd: 'FLOW_SET', 'FLOW_ON', 'FLOW_OFF', 'VALVE_OPEN', 'VALVE_CLOSE',
               'PS_ZEROING', 'MFC_ZEROING', 'SP4_ON', 'SP1_ON', 'SP1_SET', 'SP4_SET',
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

            elif key == "SP3_ON":
                await self.sp3_on()

            elif key == "SP1_ON":
                await self.sp1_on()

            elif key == "SP1_SET":
                val_ui = _req("value", float)
                await self.sp1_set(val_ui)

            elif key == "SP4_SET":
                val_ui = _req("value", float)
                await self.sp4_set(val_ui)

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
                self._ev_nowait(MFCEvent(kind="status", message="Polling read 시작"))
                self._poll_task = asyncio.create_task(self._poll_loop())
        else:
            if self._poll_task:
                self._poll_task.cancel()
                self._poll_task = None
            self._poll_cycle_active = False
            purged = self._purge_poll_reads_only(cancel_inflight=True, reason="polling off")
            self._ev_nowait(MFCEvent(kind="status", message="Polling read 중지"))
            if purged:
                self._ev_nowait(MFCEvent(kind="status", message=f"[QUIESCE] Polling read {purged}건 제거 (polling off)"))

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

    def set_endpoint(self, host: str, port: int, *, reconnect: bool = True) -> None:
        """런타임 엔드포인트 변경. reconnect=True면 즉시 재연결 루틴 트리거."""
        self._override_host = str(host)
        self._override_port = int(port)
        if reconnect:
            # 워치독만 잠깐 멈추고, 현재 연결은 정리
            asyncio.create_task(self._bounce_connection())

    async def _bounce_connection(self) -> None:
        # 워치독 일시정지
        await self.pause_watchdog()
        # TCP 정리(조용히)
        try:
            self._on_tcp_disconnected()
        except Exception:
            pass
        # 재개
        await self.resume_watchdog()

    def _resolve_endpoint(self) -> tuple[str, int]:
        """최종 접속할 host/port 결정: override > config 기본값."""
        host = self._override_host if self._override_host else MFC_TCP_HOST
        port = self._override_port if self._override_port else MFC_TCP_PORT
        return str(host), int(port)

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
                host, port = self._resolve_endpoint()
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=max(0.5, float(MFC_CONNECT_TIMEOUT_S))
                )
                self._reader, self._writer = reader, writer
                self._connected = True
                self._ever_connected = True
                backoff = MFC_RECONNECT_BACKOFF_START_MS

                # ★ Keepalive는 config에 따름(기본 False 권장)
                try:
                    sock = writer.get_extra_info("socket")
                    if sock is not None:
                        if bool(getattr(cfgc, "MFC_TCP_KEEPALIVE", False)):
                            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                        else:
                            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 0)
                except Exception:
                    pass

                # ★ 연결 직후 IO 시각 초기화
                self._last_connect_mono = time.monotonic()
                self._last_io_mono = self._last_connect_mono
                self._just_reopened = True

                # 리더 태스크 시작
                if self._reader_task and not self._reader_task.done():
                    self._reader_task.cancel()
                    with contextlib.suppress(Exception):
                        await self._reader_task
                self._reader_task = asyncio.create_task(self._tcp_reader_loop(), name="MFC-TcpReader")

                await self._emit_status(f"{host}:{port} 연결 성공 (TCP)")
            except Exception as e:
                host, port = self._resolve_endpoint()
                await self._emit_status(f"{host}:{port} 연결 실패: {e}")
                backoff = min(backoff * 2, MFC_RECONNECT_BACKOFF_MAX_MS)

    def _on_tcp_disconnected(self):
        self._connected = False

        # 자기 자신 cancel 방지
        current = asyncio.current_task()
        t = self._reader_task
        if t and not t.done() and t is not current:
            t.cancel()
        self._reader_task = None

        # writer 종료 + 하드 클로즈 보강
        if self._writer:
            with contextlib.suppress(Exception):
                self._writer.close()
            # FIN hang 대비: transport.abort()로 RST
            transport = getattr(self._writer, "transport", None)
            if transport:
                with contextlib.suppress(Exception):
                    transport.abort()

        self._reader = None
        self._writer = None

        # 라인 큐 비우기
        with contextlib.suppress(Exception):
            while True:
                self._line_q.get_nowait()

        # 상태 이벤트(로그/UI용)
        self._ev_nowait(MFCEvent(kind="status", message="MFC TCP 연결 끊김"))

        # inflight 재시도/콜백 처리(기존 로직 유지)
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
            
            # ★ 전송 직전 유휴/세션 프리플라이트
            await self._reopen_if_inactive()
            if not self._connected or not self._writer:
                # 아직 워치독이 다시 붙지 못했으면 명령을 되돌리고 잠깐 쉼
                self._cmd_q.appendleft(cmd)
                self._inflight = None
                await asyncio.sleep(0.15)
                continue

            # 연결 직후에는 '한 번만' 조용히 기다리고(quiet), 강한 드레인은 금지
            if self._just_reopened and self._last_connect_mono > 0.0:
                remain = (self._last_connect_mono + (MFC_POST_OPEN_QUIET_MS / 1000.0)) - time.monotonic()
                if remain > 0:
                    await asyncio.sleep(remain)
                # 여기서는 드레인하지 않음: 초기 배너/ACK를 날려서 첫 응답 유실 가능
                self._just_reopened = False
            # 평상시에도 전송 직전 강제 드레인은 하지 않음
            # (잔여 라인은 읽기 루틴의 에코/접두사 처리로 흡수)

            # write
            try:
                payload = cmd.cmd_str.encode("ascii", "ignore")
                self._last_io_mono = time.monotonic()      # ★ 송신 직전 IO 시각 갱신
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
                await self._absorb_late_lines(150)
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

                if cmd.retries_left > 0 and (time.monotonic() - self._last_connect_mono) < 2.0:
                    cmd.retries_left -= 1
                    self._cmd_q.appendleft(cmd)
                    await self._absorb_late_lines(100)   # ✅ 라인큐만 가볍게 비움
                    await asyncio.sleep(max(0.15, cmd.gap_ms / 1000.0))
                    continue

                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    self._cmd_q.appendleft(cmd)
                else:
                    self._safe_callback(cmd.callback, None)

                self._on_tcp_disconnected()  # ✅ 표준화된 TCP 연결정리
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
            if self._skip_echos and line == self._skip_echos[0]:
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
                self._last_io_mono = time.monotonic()   # ★ 수신 시각
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
                        line = line_bytes.decode("ascii", "ignore").strip()
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
        self._last_io_mono = time.monotonic()  # ★ 라인 단위 갱신
        # 과거 no-reply 에코 1회 스킵
        if self._skip_echos and line == self._skip_echos[0]:
            self._skip_echos.popleft()
            self._dbg("MFC", f"[ECHO] 과거 no-reply 에코 스킵: {line}")
            return
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
                            v_hw = float(vals[idx])
                            sf = float(self.scale_factors.get(ch, 1.0))
                            await self._emit_flow(name, v_hw / sf)
                            self._monitor_flow(ch, v_hw)

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

                sf = float(self.scale_factors.get(ch, 1.0))
                tol = target * float(FLOW_ERROR_TOLERANCE)

                self._stab_attempts += 1
                await self._emit_status(
                    f"GAS 확인... (목표: {target:.2f}/{target/sf:.2f}sccm, "
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
                    await self._emit_failed("FLOW_ON", "GAS 안정화 시간 초과")
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
            line = await self._send_and_wait_line(
                self._mk_cmd("READ_FLOW_SET", channel=ch),
                tag=f"[VERIFY SET ch{ch}]", timeout_ms=MFC_TIMEOUT,
                expect_prefixes=(f"Q{4 + int(ch)}",)
            )

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

            # ★ 직전 L0 에코/배너가 섞이지 않도록 라인 큐만 짧게 드레인
            await self._absorb_late_lines(120)

            # 검증 (의미없는 빈 라인 방지용으로 최대 2회 읽기)
            now = ""
            for _ in range(2):
                line = await self._send_and_wait_line(self._mk_cmd("READ_MFC_ON_OFF_STATUS"),
                                                    tag="[VERIFY R69]", timeout_ms=MFC_TIMEOUT,
                                                    expect_prefixes=("L0","L"))
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
                tag="[VERIFY SP1_SET]", 
                timeout_ms=MFC_TIMEOUT,
                expect_prefixes=("S1",)
            )
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
    
    async def _verify_sp_set(self, sp_idx: int, hw_val: float, ui_val: float) -> bool:
        """
        READ_SP{sp_idx}_VALUE 로 HW값 비교(허용오차 = MFC_SP1_VERIFY_TOL 재사용).
        - config에 READ_SP{sp_idx}_VALUE 키가 없으면 '검증 스킵'으로 간주하여 True 반환.
        - 장비 응답 접두사는 'S{sp_idx}'로 기대.
        """
        tol = max(float(MFC_SP1_VERIFY_TOL), 1e-9)
        key_read = f"READ_SP{sp_idx}_VALUE"

        # 구성에 읽기 명령이 정의되지 않은 경우 검증 스킵
        if key_read not in MFC_COMMANDS:
            await self._emit_status(f"[VERIFY SP{sp_idx}_SET] 스킵: '{key_read}' 미정의 → 통과 처리")
            return True

        for attempt in range(1, 6):
            line = await self._send_and_wait_line(
                self._mk_cmd(key_read),
                tag=f"[VERIFY SP{sp_idx}_SET]",
                timeout_ms=MFC_TIMEOUT,
                expect_prefixes=(f"S{sp_idx}",)
            )
            cur_hw = self._parse_pressure_value(line or "")
            if cur_hw is not None:
                cur_hw = round(cur_hw, int(MFC_PRESSURE_DECIMALS))

            ok = (cur_hw is not None) and (abs(cur_hw - hw_val) <= tol)
            if ok:
                await self._emit_status(
                    f"SP{sp_idx} 설정 완료: UI {ui_val:.2f} (장비 {hw_val:.{int(MFC_PRESSURE_DECIMALS)}f})"
                )
                return True

            await self._emit_status(
                f"[SP{sp_idx}_SET 검증 재시도] 응답={repr(line)} (시도 {attempt}/5)"
            )
            await asyncio.sleep(MFC_DELAY_MS / 1000.0)

        return False

    async def _verify_simple_flag(self, cmd_key: str, expect_mask: str) -> bool:
        """SP1_ON/SP4_ON → READ_SYSTEM_STATUS 확인(Mn...)"""
        # 전송(no-reply)
        self._enqueue(self._mk_cmd(cmd_key), None, allow_no_reply=True, tag=f"[{cmd_key}]")
        for attempt in range(1, 6):
            line = await self._send_and_wait_line(
                self._mk_cmd("READ_SYSTEM_STATUS"),
                tag=f"[VERIFY {cmd_key}]", timeout_ms=MFC_TIMEOUT,
                expect_prefixes=("M",)
            )

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
        line = await self._send_and_wait_line(
            self._mk_cmd("READ_FLOW_ALL"),
            tag=tag, timeout_ms=MFC_TIMEOUT,
            expect_prefixes=("Q0",)
        )
        return self._parse_r60_values(line or "")

    async def _read_r69_bits(self) -> Optional[str]:
        line = await self._send_and_wait_line(
            self._mk_cmd("READ_MFC_ON_OFF_STATUS"),
            tag="[READ R69]", timeout_ms=MFC_TIMEOUT,
            retries=3, expect_prefixes=("L0", "L")
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
                                         message=f"Ch{channel} GAS 불안정! (목표: {target_flow:.2f}, 현재: {actual_flow_hw:.2f})"))
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
        self,
        cmd_str: str,
        *,
        tag: str,
        timeout_ms: int,
        retries: int = 1,
        expect_prefixes: tuple[str, ...] = (),  # ← 추가
    ) -> Optional[str]:
        fut: asyncio.Future[Optional[str]] = asyncio.get_running_loop().create_future()

        def _cb(line: Optional[str]):
            if line is None:
                if not fut.done():
                    fut.set_result(None)
                return
            s = (line or "").strip()
            if not fut.done():
                fut.set_result(s)  # ★ 필터링 제거 (워커가 보장)

        self._enqueue(
            cmd_str, _cb, 
            timeout_ms=timeout_ms, gap_ms=MFC_GAP_MS,
            tag=tag, retries_left=max(0, int(retries)), allow_no_reply=False,
            expect_prefixes=expect_prefixes # ★ 워커에게 전달
        )

        # 오픈 직후 첫 응답은 여유 부여
        extra = 0.0
        if self._last_connect_mono > 0.0 and (time.monotonic() - self._last_connect_mono) < 2.0:
            extra = MFC_FIRST_CMD_EXTRA_TIMEOUT_MS / 1000.0
        try:
            return await asyncio.wait_for(fut, timeout=(timeout_ms / 1000.0) + 2.0 + extra)
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
            self._dbg("MFC", f"[QUIESCE] Polling inflight 취소: {reason}")
        kept = deque()
        while self._cmd_q:
            c = self._cmd_q.popleft()
            if self._is_poll_read_cmd(c.cmd_str, c.tag):
                purged += 1
                continue
            kept.append(c)
        self._cmd_q = kept
        if purged:
            self._ev_nowait(MFCEvent(kind="status", message=f"[QUIESCE] Polling read {purged}건 제거: {reason}"))
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

# =============== debug, R69 하지 않는 ==================
    def _mask_set(self, channel: int, on: bool) -> str:
        """섀도우 마스크를 바탕으로 특정 채널 비트만 갱신한 목표 마스크 문자열 반환."""
        bits = list((self._mask_shadow or "0000").ljust(4, '0')[:4])
        if 1 <= channel <= 4:
            bits[channel - 1] = '1' if on else '0'
        return ''.join(bits)
# =============== debug, R69 하지 않는 ==================

    async def pause_watchdog(self) -> None:
        """자동 재연결 워치독만 잠시 멈춤(현재 연결은 유지)."""
        self._wd_paused = True
        self._want_connected = False
        t = self._watchdog_task
        if t and not t.done():
            t.cancel()
            try:
                await t
            except Exception:
                pass
        self._watchdog_task = None

    async def resume_watchdog(self) -> None:
        """pause_watchdog 이후 워치독/워커 재개."""
        self._wd_paused = False
        # start()는 워치독/워커가 죽어있으면 살려주고, 살아있으면 아무것도 안 함
        await self.start()

    # ====================== NPort 시리얼 해제 함수 (Windows 전용) ======================
    # 클래스 차원의 최근 리셋 타임스탬프(너무 잦은 리셋 억제)
    # _last_reset_mono: float = 0.0

    # async def _force_release_nport_port(
    #     self,
    #     *,
    #     dll_path: str | None = None,
    #     override_port_index: int | None = None,
    #     lock_timeout_ms: int = 15000,
    #     cooldown_sec: float = 2.0,
    # ):
    #     """
    #     IPSerial.dll(nsio_resetport)로 NPort 시리얼 포트의 TCP 세션을 강제 해제.
    #     - 시스템 전역 네임드 뮤텍스(Global\...)로 '동시 호출'을 절대 허용하지 않음.
    #     - 최근 몇 초 내 리셋 호출이 있었다면 쿨다운으로 skip.
    #     - MFC_DISABLE_IPSERIAL_RESET=1 이면 즉시 skip.
    #     """
    #     if os.name != "nt":
    #         return  # 비-Windows는 조용히 skip

    #     # 긴급 차단 스위치(운영 중 신속 디버그용)
    #     if os.environ.get("MFC_DISABLE_IPSERIAL_RESET", "").strip() in ("1","true","TRUE","yes","Y"):
    #         await self._emit_status("[IPSerial] reset disabled by env(MFC_DISABLE_IPSERIAL_RESET)")
    #         return

    #     # 너무 잦은 리셋 방지
    #     now = time.monotonic()
    #     if (now - getattr(self, "_last_reset_mono", 0.0)) < float(cooldown_sec):
    #         await self._emit_status(f"[IPSerial] skip: cooldown {cooldown_sec:.1f}s")
    #         return

    #     host, tcp_port = self._resolve_endpoint()
    #     port_index = _guess_nport_index_from_tcp_port(tcp_port, override_port_index)

    #     # 전역 뮤텍스 구간 — IG 등 다른 장치와 '절대' 동시에 들어가지 않음
    #     with _WinGlobalMutex(_IPSERIAL_MUTEX_NAME, timeout_ms=int(lock_timeout_ms)) as mx:
    #         if not mx.acquired:
    #             await self._emit_status("[IPSerial] skip: failed to acquire global lock (timeout)")
    #             return

    #         def _work():
    #             exe_dir = Path(sys.argv[0]).resolve().parent
    #             default_dll = exe_dir / "dll" / "IPSerial.dll"
    #             final_dll = str(dll_path or default_dll)
    #             ipser = _MoxaIPSerial(final_dll)  # ← 싱글톤 DLL 재사용
    #             rc = ipser.reset_port(host, port_index)
    #             return rc, final_dll

    #         loop = asyncio.get_running_loop()
    #         rc, used_dll = await loop.run_in_executor(None, _work)

    #     # 뮤텍스 해제 후 기록(성공/실패 여부와 무관)
    #     self._last_reset_mono = time.monotonic()
    #     await self._emit_status(
    #         f"[IPSerial] reset: host={host}, index={port_index}, rc={rc}, dll='{used_dll}'"
    #     )

    # ====================== NPort 시리얼 해제 함수 (Windows 전용) ======================

    async def _reopen_if_inactive(self):
        """
        보내기 직전에 유휴시간 초과/세션 이상을 점검하고 필요 시 즉시 세션을 내렸다가(논블로킹)
        워치독이 다시 붙게 한다.
        """
        # writer가 없거나 닫힌 경우 → 즉시 disconnect
        if not self._writer or self._writer.is_closing() or not self._connected:
            self._on_tcp_disconnected()
            return

        # 유휴 시간 초과면 세션 재시작
        if self._inactivity_s > 0:
            idle = time.monotonic() - (self._last_io_mono or 0.0)
            if idle >= self._inactivity_s:
                await self._emit_status(f"[MFC] idle {idle:.1f}s ≥ {self._inactivity_s:.1f}s → 세션 재시작")
                self._on_tcp_disconnected()



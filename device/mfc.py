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

from lib import config_common as cfgc
cfg_default = cfgc  # AsyncMFC 기본 cfg (원하면 ch1/ch2 모듈을 넘겨서 채널별 파라미터 사용)

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
    owner: Optional[str] = None                   # 명령을 낸 주체("chamber1"/"pc2"). None = 공용 → 모든 소비자
    channel: Optional[int] = None                 # 관련 가스 채널(있을 때만)

# =============== 명령 레코드 ===============
@dataclass(eq=False)
class Command:
    cmd_str: str
    callback: Optional[Callable[[Optional[str]], None]]
    timeout_ms: int
    gap_ms: int
    tag: str
    retries_left: int
    allow_no_reply: bool
    expect_prefixes: tuple[str, ...] = ()
    owner: Optional[str] = None                   # 명령 주체(공유 MFC 에서 결과를 그 주체에게만 돌려주기 위함)
    done: Optional["asyncio.Future[bool]"] = None # no-reply 전송 결과: True=실제 전송됨, False=전송 전 폐기/실패
    enq_mono: float = 0.0
    sent_mono: float = 0.0


# no-reply 명령(가스 ON/OFF·설정·밸브·Zeroing)의 '실제 전송' 확인 최대 대기(초).
# 공유 사용 중 대기열이 밀려도 충분한 값. 넘으면 켜는 계열은 대기열에서 철회 후 실패, 끄는 계열은 남겨 두고 실패 보고.
SEND_CONFIRM_TIMEOUT_S = 30.0


@dataclass(eq=False)
class _StabJob:
    """채널별 FLOW_ON 안정화 작업(공유 MFC 에서 채널마다 따로 관리)."""
    channel: int
    target_hw: float
    owner: Optional[str]
    created_mono: float
    attempts: int = 0

# =============== Async 컨트롤러 ===============
def mfc_resource_key(mfc) -> str:
    """런타임이 소유권 관리에 쓰는 자원 키. main.py 가 붙인 resource_key 가 없으면
    "MFC@host:port" 로 폴백(로그용 안전값). 채널 번호로 키를 추측하지 않는다."""
    key = str(getattr(mfc, "resource_key", "") or "").strip()
    if key:
        return key
    host = getattr(mfc, "host", None) or getattr(mfc, "_override_host", None) or "?"
    port = getattr(mfc, "port", None) or getattr(mfc, "_override_port", None) or "?"
    return f"MFC@{host}:{port}"


class AsyncMFC:
    def __init__(self, *, enable_verify: bool = True, enable_stabilization: Optional[bool] = None, host: Optional[str] = None, 
                 port: Optional[int] = None, scale_factors: Optional[dict[int, float]] = None, cfg=None):
        self._cfg = cfg if cfg is not None else cfg_default

        def _cfg_get(name: str, default=None):
            if hasattr(self._cfg, name):
                return getattr(self._cfg, name)
            if hasattr(cfgc, name):
                return getattr(cfgc, name)
            return default

        self._cfg_get = _cfg_get

        # 공유 자원 키("MFC1"/"MFC2"). main.py 가 생성 직후 붙인다. 런타임은 mfc_resource_key() 로만 읽는다.
        self.resource_key: str = ""

        # ✅ config에서 즉시 읽기(=UI에서 값 바꾸면 반영 가능)
        self.debug_print = self._cfg_bool("DEBUG_PRINT", False)

        # ← 런타임에서 채널별로 덮어쓸 TCP 엔드포인트(없으면 config 기본값 사용)
        self._override_host: Optional[str] = host
        self._override_port: Optional[int] = port

        # ★ 인스턴스별 스케일 맵(인자 우선, 없으면 config의 MFC_SCALE_FACTORS)
        self.scale_factors: dict[int, float] = dict(
            scale_factors or self._cfg_dict("MFC_SCALE_FACTORS", {1: 1.0, 2: 1.0, 3: 1.0})
        )

        # ▼ 추가: 검증/안정화 플래그
        self._verify_enabled: bool = bool(enable_verify)
        self._stab_enabled: bool = (self._verify_enabled if enable_stabilization is None
                                    else bool(enable_stabilization))

        # ✅ TCP Streams
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._reader_task: Optional[asyncio.Task] = None

        # ✅ TCP Streams (EOL/echo는 런타임 필드로 캐시)
        self._tx_eol: bytes = self._cfg_bytes("MFC_TX_EOL", b"\r")
        self._tx_eol_str: str = self._tx_eol.decode("ascii", "ignore")
        self._skip_echo_flag: bool = self._cfg_bool("MFC_SKIP_ECHO", True)

        self._connected: bool = False
        self._ever_connected: bool = False

        # 명령 큐/인플라이트
        self._cmd_q: Deque[Command] = deque()
        self._inflight: Optional[Command] = None

        # 수신 라인 큐 (TCP 리더 → 워커)
        self._line_q: asyncio.Queue[str] = asyncio.Queue(maxsize=1024)

        # 이벤트 큐 (상위 UI/브리지 소비)
        self._event_q: asyncio.Queue[MFCEvent] = asyncio.Queue(maxsize=1024)

        # ★ 여러 런타임(Chamber / Plasma Cleaning)이 동시에 구독할 수 있도록
        #   브로드캐스트용 서브 큐와 태스크를 추가
        self._event_subscribers: list[asyncio.Queue[MFCEvent]] = []
        self._event_broadcast_task: Optional[asyncio.Task] = None

        # 태스크들
        self._want_connected: bool = False

        # ★ 폴링 마스크 — 인스턴스가 gas-only/pressure-only로 동작해야 할 때 사용
        #   기본은 둘 다 True (기존 동작 유지). PC에서 일시적으로 한쪽만 켤 수 있음.
        self._poll_gas_enabled: bool = True
        self._poll_pressure_enabled: bool = True

        self._watchdog_task: Optional[asyncio.Task] = None
        self._cmd_worker_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._stab_task: Optional[asyncio.Task] = None
        self._wd_paused: bool = False    # ← 추가 (워치독 일시정지 상태)

        # 재연결 백오프(시작값)
        self._reconnect_backoff_ms = self._cfg_int("MFC_RECONNECT_BACKOFF_START_MS", 1000)

        # 런타임/스케일/모니터링
        self.gas_map = {1: "Ar", 2: "O2", 3: "N2"}
        self.last_setpoints = {1: 0.0, 2: 0.0, 3: 0.0}      # 장비 단위(HW)
        self.flow_error_counters = {1: 0, 2: 0, 3: 0}

        # ⬇ PlasmaCleaning 전용: 선택된 가스 채널 기억
        self._selected_ch: Optional[int] = None
        self._selected_owner: Optional[str] = None

        # ⬇ 채널별 ON 상태(마스크 미사용 시 모니터 필터 기준)
        self._flow_on_flags = {1: False, 2: False, 3: False}
        # ⬇ 채널별 마지막 사용 주체(공유 MFC 에서 '그 주체 몫'만 정리할 때 기준)
        self._ch_owner: dict[int, Optional[str]] = {1: None, 2: None, 3: None}
        # ⬇ 연결 수명주기(start/cleanup 직렬화) + 종료 진행 중 표시(진행 중엔 is_connected()=False)
        self._lifecycle_lock: Optional[asyncio.Lock] = None
        self._closing: bool = False

        # 폴링 사이클 중첩 방지 플래그
        self._poll_cycle_active: bool = False

        # ★ 과거 no-reply 명령의 에코를 1회성으로 버리기 위한 대기열
        self._skip_echos: deque[str] = deque()

        # 안정화 상태: 채널별 작업(하나의 감시 태스크 _stab_task 가 R60 한 번으로 모두 판정)
        self._stab_jobs: dict[int, _StabJob] = {}

        # Qt의 clear+soft-drain 타이밍을 모사하기 위한 플래그
        self._last_connect_mono: float = 0.0
        self._just_reopened: bool = False

        # ★ Inactivity 전략 필드
        self._inactivity_s: float = self._cfg_float("MFC_INACTIVITY_REOPEN_S", 0.0)
        self._last_io_mono: float = 0.0

# =============== debug, R69 하지 않는 ==================
        # 현재 ON/OFF 상태를 R69 없이 자체 추적하기 위한 섀도우 마스크(좌→우: ch1..ch4)
        self._mask_shadow: str = "0000"
# =============== debug, R69 하지 않는 ==================

    # ---------- config helper (NEW) ----------
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

    def _cfg_bytes(self, name: str, default: bytes) -> bytes:
        v = self._cfg_get(name, default)
        if isinstance(v, (bytes, bytearray)):
            return bytes(v)
        if isinstance(v, str):
            return v.encode("ascii", "ignore")
        return bytes(default)

    def _cfg_dict(self, name: str, default: dict) -> dict:
        v = self._cfg_get(name, default)
        return v if isinstance(v, dict) else dict(default)

    def reload_runtime_cfg(self) -> None:
        """
        config 값을 런타임 필드로 다시 반영.
        (특히 TX_EOL/echo/inactivity 같은 __init__ 캐시 성격)
        """
        self.debug_print = self._cfg_bool("DEBUG_PRINT", False)
        self._tx_eol = self._cfg_bytes("MFC_TX_EOL", b"\r")
        self._tx_eol_str = self._tx_eol.decode("ascii", "ignore")
        self._skip_echo_flag = self._cfg_bool("MFC_SKIP_ECHO", True)
        self._inactivity_s = self._cfg_float("MFC_INACTIVITY_REOPEN_S", 0.0)
        self._reconnect_backoff_ms = self._cfg_int("MFC_RECONNECT_BACKOFF_START_MS", 1000)

    def is_connected(self) -> bool:
        """프리플라이트/상태 체크용: 현재 TCP 연결 여부를 반환. 종료(cleanup) 진행 중이면 False."""
        return bool(self._connected) and not self._closing

    def _lifecycle(self) -> asyncio.Lock:
        """start()/cleanup() 직렬화용 락(루프 안에서 처음 쓸 때 생성)."""
        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        return self._lifecycle_lock

    # ---------- 공용 API ----------
    async def start(self):
        """워치독/커맨드 워커 시작(연결은 워치독이 관리). 재호출/죽은 태스크 회복 안전.
        cleanup() 이 진행 중이면 끝날 때까지 기다렸다가 다시 올린다(공유 MFC: 한쪽 종료 ↔ 다른 쪽 시작 경합)."""
        async with self._lifecycle():
            await self._start_locked()

    async def _start_locked(self):
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

        # ✅ 이벤트 큐 적체 방지: events() 구독자가 늦게 붙어도 중앙 큐를 비워준다
        self._ensure_event_broadcast_task()
        #await self._emit_status("MFC 워치독/워커 시작")

    async def connect(self):
        """start()와 동일 의미의 별칭 — 호출측 일관성 확보."""
        await self.start()

    async def cleanup(self):
        # 진행 중에는 is_connected()=False → 다른 런타임이 '이미 연결됨'으로 보고 start 를 건너뛰지 않는다.
        # start() 는 같은 락을 기다리므로 종료가 끝난 뒤 다시 올린다.
        async with self._lifecycle():
            self._closing = True
            try:
                await self._cleanup_locked()
            finally:
                self._closing = False

    async def _cleanup_locked(self):
        await self._emit_status("MFC 종료 절차 시작")
        self._want_connected = False

        # 폴링/안정화 태스크 중지
        await self._cancel_task("_poll_task")
        self._cancel_all_stab_jobs(reason="MFC 연결 종료", notify=True)
        await self._cancel_task("_stab_task")

        # 명령 워커/워치독 중지
        await self._cancel_task("_cmd_worker_task")
        await self._cancel_task("_watchdog_task")
        # ⚠ 이벤트 브로드캐스트 태스크와 구독자 목록은 유지한다: 챔버의 MFC 이벤트 펌프는 상주(keep-alive) 태스크라
        #   구독자를 비우면 펌프가 살아 있는 채로 이벤트를 영영 못 받는다(재생성도 안 됨).
        #   (_cancel_task 가 CancelledError 를 못 잡아 이 아래가 그동안 실행되지 않았던 것 — 함께 수정)

        # 큐/인플라이트 정리
        self._purge_pending("shutdown")

        # TCP 종료 (결정적 종료: wait_closed 대기 + 라인큐/에코큐 비움)
        if self._reader_task:
            self._reader_task.cancel()
            with contextlib.suppress(Exception, asyncio.CancelledError):
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

    def _ensure_event_broadcast_task(self) -> None:
        """중앙 이벤트 큐(_event_q) → 구독자 큐로 복사하는 태스크 보장."""
        if self._event_broadcast_task and not self._event_broadcast_task.done():
            return
        loop = asyncio.get_running_loop()
        self._event_broadcast_task = loop.create_task(
            self._event_broadcast_loop(), name="MFCEventBroadcast"
        )

    async def _event_broadcast_loop(self) -> None:
        """_event_q에서 꺼낸 이벤트를 모든 구독자 큐에 브로드캐스트."""
        try:
            while True:
                ev = await self._event_q.get()
                # 구독자 리스트 스냅샷을 떠서 순회 중 변경에 안전하게 처리
                for q in list(self._event_subscribers):
                    try:
                        q.put_nowait(ev)
                    except Exception:
                        # 개별 구독자 큐가 가득 찼거나 이미 정리된 경우는 조용히 스킵
                        pass
        except asyncio.CancelledError:
            # cleanup() 등으로 태스크가 취소될 때 조용히 종료
            return

    async def events(self) -> AsyncGenerator[MFCEvent, None]:
        """
        상위에서 소비하는 이벤트 스트림.
        여러 소비자가 동시에 호출해도 '모두' 같은 이벤트를 받도록 브로드캐스트한다.
        """
        # 브로드캐스트 루프 기동 보장
        self._ensure_event_broadcast_task()

        q: asyncio.Queue[MFCEvent] = asyncio.Queue(maxsize=1024)
        self._event_subscribers.append(q)
        try:
            while True:
                ev = await q.get()
                yield ev
        finally:
            # 구독 해제 (cleanup/태스크 취소 시)
            with contextlib.suppress(ValueError):
                self._event_subscribers.remove(q)

    # ---- 고수준 제어 API (기존 handle_command 세분화) ----
    async def set_flow(self, channel: int, ui_value: float, *, owner: Optional[str] = None) -> bool:
        """FLOW_SET + (옵션) READ_FLOW_SET 검증. 실제 전송(또는 검증)된 뒤에만 확정, 아니면 실패. 결과를 bool 로도 돌려준다."""
        scaled = self._ui_to_hw(channel, float(ui_value))  # %FS
        await self._emit_status(f"Ch{channel} GAS 스케일: {ui_value:.2f}sccm → 장비 {scaled:.2f}%FS")
        if channel in self._ch_owner:
            self._ch_owner[channel] = owner

        # SET (no-reply)
        set_cmd = self._mk_cmd("FLOW_SET", channel=channel, value=scaled)
        cmd = self._enqueue(set_cmd, None, allow_no_reply=True, tag=f"[SET ch{channel}]",
                            owner=owner, track=True)

        # ▼ 검증 비활성화: 실제로 전송된 뒤 확정(전송 전 폐기/대기 초과면 실패)
        if not self._verify_enabled:
            ok, why = await self._await_sent(cmd)
            if ok:
                self.last_setpoints[channel] = scaled
                await self._emit_confirmed("FLOW_SET", owner=owner, channel=channel)
            else:
                await self._emit_failed("FLOW_SET", f"Ch{channel} FLOW_SET {why}", owner=owner, channel=channel)
            return ok

        # 검증
        ok = await self._verify_flow_set(channel, scaled, owner=owner)
        if ok:
            self.last_setpoints[channel] = scaled
            await self._emit_confirmed("FLOW_SET", owner=owner, channel=channel)
        else:
            await self._emit_failed("FLOW_SET", f"Ch{channel} FLOW_SET 확인 실패", owner=owner, channel=channel)
        return ok

    async def flow_on(self, channel: int, *, owner: Optional[str] = None) -> None:
        """개별 채널 ON(L{ch}1). 실제 전송 + 기존 최소 대기 뒤 (옵션) 채널별 안정화 → 확정."""
        # 같은 채널의 이전 안정화만 정리(다른 채널·다른 주체의 안정화는 건드리지 않는다)
        prev_owner = self._stab_owner(channel)
        self._cancel_stab_job(channel, reason="같은 채널 FLOW_ON 재요청",
                              notify=(prev_owner is not None and prev_owner != owner))

        # 섀도우 마스크 갱신(내부 상태 유지용)
        target = self._mask_set(channel, True)
        if channel in self._ch_owner:
            self._ch_owner[channel] = owner

        # ▶ 개별 채널 ON (L{ch}1) — 마스크(L0) 금지
        cmd = self._enqueue(self._mk_cmd("FLOW_ON", channel=channel), None,
                            allow_no_reply=True, tag=f"[FLOW_ON ch{channel}]",
                            owner=owner, track=True)
        self._mask_shadow = target

        # 실제 전송 + 장비 반영 최소 대기(기존과 같은 MFC_DELAY_MS, 최소 200ms)
        ok, why = await self._await_sent(
            cmd, min_delay_s=max(self._cfg_int("MFC_DELAY_MS", 1000), 200) / 1000.0)
        if not ok:
            await self._emit_failed("FLOW_ON", f"ch{channel} FLOW_ON {why}", owner=owner, channel=channel)
            return

        # ✅ 플래그(유량 감시 기준)
        self._flow_on_flags[channel] = True

        # (옵션) 채널별 안정화 — R60 기반
        if self._stab_enabled:
            tgt = float(self.last_setpoints.get(channel, 0.0))
            if tgt > 0:
                self._start_stab_job(channel, tgt, owner)
                await self._emit_status(f"FLOW_ON: ch{channel} 안정화 시작 (목표 HW {tgt:.2f})")
                return

        await self._emit_confirmed("FLOW_ON", owner=owner, channel=channel)

    async def flow_off(self, channel: int, *, owner: Optional[str] = None) -> None:
        """개별 채널 OFF(L{ch}0). OFF 는 대기열에서 철회하지 않는다(늦더라도 반드시 전송)."""
        # 해당 채널 안정화 중이면 중단(다른 주체의 안정화였으면 그 주체에게 실패로 알림)
        prev_owner = self._stab_owner(channel)
        if self._cancel_stab_job(channel, reason="FLOW_OFF 요청",
                                 notify=(prev_owner is not None and prev_owner != owner)):
            await self._emit_status(f"FLOW_OFF 요청: ch{channel} 안정화 취소")

        # 목표 GAS/모니터링 카운터 리셋
        self.last_setpoints[channel] = 0.0
        self.flow_error_counters[channel] = 0
        self._flow_on_flags[channel] = False

        # 섀도우 마스크 갱신(내부 상태 유지용)
        target = self._mask_set(channel, False)

        # ★ 보호: shutdown 진행 중이면 송신 불가 → 가짜 OK 방지
        #   (일시 TCP 끊김은 cmd_worker가 reconnect 후 자동 처리하도록 enqueue 허용)
        if not self._want_connected:
            await self._emit_failed("FLOW_OFF", "MFC not ready (shutdown)", owner=owner, channel=channel)
            return

        # ▶ 개별 채널 OFF (L{ch}0) — 마스크(L0) 금지
        cmd = self._enqueue(self._mk_cmd("FLOW_OFF", channel=channel), None,
                            allow_no_reply=True, tag=f"[FLOW_OFF ch{channel}]",
                            owner=owner, track=True)
        self._mask_shadow = target

        # 실제 전송 + 장비 반영 대기 후 확정
        ok, why = await self._await_sent(
            cmd, min_delay_s=max(self._cfg_int("MFC_DELAY_MS", 1000), 200) / 1000.0,
            withdraw_on_timeout=False)
        if ok:
            await self._emit_confirmed("FLOW_OFF", owner=owner, channel=channel)
        else:
            await self._emit_failed("FLOW_OFF", f"ch{channel} FLOW_OFF {why}", owner=owner, channel=channel)

    # === PlasmaCleaning: 선택 가스 전용 API (L{ch}{1/0} 개별 명령 사용) ===
    async def gas_select(self, gas_idx: int, *, owner: Optional[str] = None) -> None:
        gi = int(gas_idx)
        if gi not in self.gas_map:
            raise ValueError(f"지원하지 않는 가스 채널: {gas_idx}")
        self._selected_ch = gi
        self._selected_owner = owner
        self._ch_owner[gi] = owner
        await self._emit_status(f"가스 선택: ch{gi} ({self.gas_map.get(gi, '-')})")

    def _require_selected_ch(self) -> int:
        gi = int(self._selected_ch or 0)
        if gi not in self.gas_map:
            raise RuntimeError("선택된 가스가 없습니다. gas_select()를 먼저 호출하세요.")
        return gi

    async def flow_set_on(self, ui_value: float, *, owner: Optional[str] = None) -> None:
        """선택 채널에 FLOW_SET → FLOW_ON(개별 명령). FLOW_SET 이 실패하면 FLOW_ON 도 실패로 알린다."""
        ch = self._require_selected_ch()
        if not await self.set_flow(ch, float(ui_value), owner=owner):
            await self._emit_failed("FLOW_ON", f"ch{ch} FLOW_SET 실패로 FLOW_ON 생략", owner=owner, channel=ch)
            return
        await self.flow_on(ch, owner=owner)

    async def flow_off_selected(self, *, owner: Optional[str] = None) -> None:
        """선택 채널만 FLOW_OFF(개별 명령)"""
        ch = self._require_selected_ch()
        await self.flow_off(ch, owner=owner)

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
    #         await asyncio.sleep(max(self._cfg_int("MFC_DELAY_MS", 1000), 200) / 1000.0)
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
    #         await asyncio.sleep(max(self._cfg_int("MFC_DELAY_MS", 1000), 200) / 1000.0)
    #         await self._emit_confirmed("FLOW_OFF")
    #         return

    #     ok = await self._set_onoff_mask_and_verify(target)
    #     if ok:
    #         await self._emit_confirmed("FLOW_OFF")
    #     else:
    #         await self._emit_failed("FLOW_OFF", "L0 적용 불일치")

    async def valve_open(self, *, owner: Optional[str] = None):
        # ★ 보호: shutdown 진행 중이면 송신 불가 → 가짜 OK 방지
        if not self._want_connected:
            await self._emit_failed("VALVE_OPEN", "MFC not ready (shutdown)", owner=owner)
            return
        if not self._verify_enabled:
            # 실제 전송된 뒤 확정(VALVE_OPEN 은 안전 방향이라 대기 초과여도 철회하지 않는다)
            await self._send_noreply_confirmed("VALVE_OPEN", self._mk_cmd("VALVE_OPEN"), tag="[VALVE_OPEN]", owner=owner,
                                               min_delay_s=self._cfg_int("MFC_DELAY_MS_VALVE", 5000) / 1000.0,
                                               withdraw_on_timeout=False)
            return
        await self._valve_move_and_verify("VALVE_OPEN", owner=owner)

    async def valve_close(self, *, owner: Optional[str] = None):
        # ★ 보호: shutdown 진행 중이면 송신 불가 → 가짜 OK 방지
        if not self._want_connected:
            await self._emit_failed("VALVE_CLOSE", "MFC not ready (shutdown)", owner=owner)
            return
        if not self._verify_enabled:
            # 실제 전송된 뒤 확정
            await self._send_noreply_confirmed("VALVE_CLOSE", self._mk_cmd("VALVE_CLOSE"), tag="[VALVE_CLOSE]", owner=owner,
                                               min_delay_s=self._cfg_int("MFC_DELAY_MS_VALVE", 5000) / 1000.0,
                                               withdraw_on_timeout=True)
            return
        await self._valve_move_and_verify("VALVE_CLOSE", owner=owner)

    def set_poll_mask(self, *, gas: bool = True, pressure: bool = True) -> None:
        """
        폴링 루프에서 어떤 register를 읽을지 마스크.
        - 기본은 둘 다 True (R60 gas + R5 pressure 모두 읽음 = 기존 동작).
        - Plasma Cleaning에서 두 MFC 인스턴스를 gas-only/pressure-only로
          일시 분리할 때 사용. PC 종료 시 반드시 둘 다 True로 원복할 것.
        """
        self._poll_gas_enabled = bool(gas)
        self._poll_pressure_enabled = bool(pressure)

    async def sp1_set(self, ui_value: float, *, owner: Optional[str] = None):
        """SP1_SET (UI→HW 변환) + (옵션) READ_SP1_VALUE 검증."""
        scale = self._cfg_float("MFC_PRESSURE_SCALE", 0.1)
        dec = self._cfg_int("MFC_PRESSURE_DECIMALS", 3)

        hw_val = round(float(ui_value) * float(scale), int(dec))
        await self._emit_status(f"SP1 스케일: UI {ui_value:.2f} → 장비 {hw_val:.{dec}f}")

        cmd = self._enqueue(self._mk_cmd("SP1_SET", value=hw_val), None, allow_no_reply=True, tag="[SP1_SET]",
                            owner=owner, track=True)

        if not self._verify_enabled:
            await self._confirm_sent_or_fail("SP1_SET", cmd, owner=owner)
            return

        ok = await self._verify_sp1_set(hw_val, ui_value)
        if ok: await self._emit_confirmed("SP1_SET", owner=owner)
        else:  await self._emit_failed("SP1_SET", "SP1 설정 확인 실패", owner=owner)

    async def sp2_set(self, ui_value: float, *, owner: Optional[str] = None):
        """SP2_SET (UI→HW 변환) + (옵션) READ_SP2_VALUE 검증."""
        scale = self._cfg_float("MFC_PRESSURE_SCALE", 0.1)
        dec = self._cfg_int("MFC_PRESSURE_DECIMALS", 3)

        hw_val = round(float(ui_value) * float(scale), int(dec))
        await self._emit_status(f"SP2 스케일: UI {ui_value:.2f} → 장비 {hw_val:.{dec}f}")

        # 설정 전송 (no-reply)
        cmd = self._enqueue(self._mk_cmd("SP2_SET", value=hw_val), None,
                            allow_no_reply=True, tag="[SP2_SET]", owner=owner, track=True)

        # 검증 비활성화면 실제 전송된 뒤 확정(전송 전 폐기/대기 초과면 실패)
        if not self._verify_enabled:
            await self._confirm_sent_or_fail("SP2_SET", cmd, owner=owner)
            return

        # READ_SP2_VALUE가 정의되어 있지 않으면 _verify_sp_set 내부에서 스킵/통과
        ok = await self._verify_sp_set(2, hw_val, ui_value)
        if ok:
            await self._emit_confirmed("SP2_SET", owner=owner)
        else:
            await self._emit_failed("SP2_SET", "SP2 설정 확인 실패", owner=owner)

    async def sp4_set(self, ui_value: float, *, owner: Optional[str] = None):
        """SP4_SET (UI→HW 변환) + (옵션) READ_SP4_VALUE 검증."""
        scale = self._cfg_float("MFC_PRESSURE_SCALE", 0.1)
        dec = self._cfg_int("MFC_PRESSURE_DECIMALS", 3)

        hw_val = round(float(ui_value) * float(scale), int(dec))
        await self._emit_status(f"SP4 스케일: UI {ui_value:.2f} → 장비 {hw_val:.{dec}f}")

        # 설정 전송 (no-reply)
        cmd = self._enqueue(self._mk_cmd("SP4_SET", value=hw_val), None,
                            allow_no_reply=True, tag="[SP4_SET]", owner=owner, track=True)

        # 검증 비활성화면 실제 전송된 뒤 확정(전송 전 폐기/대기 초과면 실패)
        if not self._verify_enabled:
            await self._confirm_sent_or_fail("SP4_SET", cmd, owner=owner)
            return

        # 검증 (READ_SP4_VALUE가 정의되어 있지 않으면 스킵하고 통과)
        ok = await self._verify_sp_set(4, hw_val, ui_value)
        if ok: await self._emit_confirmed("SP4_SET", owner=owner)
        else:  await self._emit_failed("SP4_SET", "SP4 설정 확인 실패", owner=owner)

    async def _confirm_sent_or_fail(self, key: str, cmd: Command, *, owner: Optional[str] = None) -> bool:
        """SPn_SET(검증 꺼짐) 공통: 실제 전송 + MFC_GAP_MS 뒤 확정, 아니면 실패."""
        ok, why = await self._await_sent(cmd, min_delay_s=self._cfg_int("MFC_GAP_MS", 1000) / 1000.0)
        if ok:
            await self._emit_confirmed(key, owner=owner)
        else:
            await self._emit_failed(key, f"[{key}] {why}", owner=owner)
        return ok

    # 🔹 추가: 장비에 현재 설정된 SP1~4 setpoint를 UI 단위로 읽기
    async def _read_sp_setpoint_ui(self, sp_idx: int) -> Optional[float]:
        """
        현재 SP{sp_idx}에 저장된 압력 setpoint를 읽어서
        UI 단위(예: mTorr)로 반환한다.

        - sp_idx: 1/2/3/4
        - 실패 시 None 반환
        """
        try:
            idx = int(sp_idx)
        except Exception:
            await self._emit_status(f"[READ_SP] 잘못된 sp_idx={sp_idx!r}")
            return None

        if idx not in (1, 2, 3, 4):
            await self._emit_status(f"[READ_SP] 지원하지 않는 SP index: {idx}")
            return None

        key_read = f"READ_SP{idx}_VALUE"
        cmds = self._cfg_get("MFC_COMMANDS", {})
        if not isinstance(cmds, dict):
            cmds = {}
        if key_read not in cmds:
            await self._emit_status(f"[READ_SP] '{key_read}' 명령이 MFC_COMMANDS에 정의되어 있지 않음")
            return None

        line = await self._send_and_wait_line(
            self._mk_cmd(key_read),
            tag=f"[READ_SP{idx}]",
            timeout_ms=self._cfg_int("MFC_TIMEOUT", 2000),
            expect_prefixes=(f"S{idx}",),
        )
        if not (line and line.strip()):
            await self._emit_status(f"[READ_SP{idx}] 응답 없음")
            return None

        val_hw = self._parse_pressure_value(line.strip())
        if val_hw is None:
            await self._emit_status(f"[READ_SP{idx}] 파싱 실패: {line!r}")
            return None

        # HW → UI 변환 (SPx_SET 때와 같은 스케일 사용)
        scale = self._cfg_float("MFC_PRESSURE_SCALE", 0.1)
        dec = self._cfg_int("MFC_PRESSURE_DECIMALS", 3)

        ui_val = float(val_hw) / float(scale)
        ui_val = round(ui_val, int(dec))

        await self._emit_status(
            f"[READ_SP{idx}] 현재 setpoint (UI) = "
            f"{ui_val:.{dec}f}"
        )
        return ui_val

    # ==============================
    #   압력 도달 판정 유틸 (NEW)
    # ==============================
    def pressure_within_tolerance(self, target_ui: float, actual_ui: float) -> bool:
        """
        채널 cfg(config_ch1/config_ch2) → common(config_common) 우선순위로 tolerance를 읽어서 판정.
        """
        if target_ui <= 0:
            return False

        tol_abs = float(self._cfg_get("MFC_PRESSURE_TOL_ABS", 0.02))
        tol_rel = float(self._cfg_get("MFC_PRESSURE_TOL_REL", 0.05))

        diff = abs(actual_ui - target_ui)
        if diff <= tol_abs:
            return True
        if diff <= abs(target_ui) * tol_rel:
            return True
        return False

    async def sp1_on(self, *, owner: Optional[str] = None):
        if not self._verify_enabled:
            await self._send_noreply_confirmed("SP1_ON", self._mk_cmd("SP1_ON"), tag="[SP1_ON]",
                                               owner=owner, min_delay_s=self._cfg_int("MFC_GAP_MS", 1000) / 1000.0)
            return
        ok = await self._verify_simple_flag("SP1_ON", expect_mask='1', owner=owner)
        if ok: await self._emit_confirmed("SP1_ON", owner=owner)
        else:  await self._emit_failed("SP1_ON", "SP1 상태 확인 실패", owner=owner)

    async def sp2_on(self, *, owner: Optional[str] = None):
        """SP2_ON: SP2 Set-Point 활성화."""
        if not self._verify_enabled:
            await self._send_noreply_confirmed("SP2_ON", self._mk_cmd("SP2_ON"), tag="[SP2_ON]",
                                               owner=owner, min_delay_s=self._cfg_int("MFC_GAP_MS", 1000) / 1000.0)
            return

        ok = await self._verify_simple_flag("SP2_ON", expect_mask='2', owner=owner)
        if ok:
            await self._emit_confirmed("SP2_ON", owner=owner)
        else:
            await self._emit_failed("SP2_ON", "SP2 상태 확인 실패", owner=owner)

    async def sp3_on(self, *, owner: Optional[str] = None):
        if not self._verify_enabled:
            await self._send_noreply_confirmed("SP3_ON", self._mk_cmd("SP3_ON"), tag="[SP3_ON]",
                                               owner=owner, min_delay_s=self._cfg_int("MFC_GAP_MS", 1000) / 1000.0)
            return
        ok = await self._verify_simple_flag("SP3_ON", expect_mask='3', owner=owner)
        if ok: await self._emit_confirmed("SP3_ON", owner=owner)
        else:  await self._emit_failed("SP3_ON", "SP3 상태 확인 실패", owner=owner)

    async def sp4_on(self, *, owner: Optional[str] = None):
        if not self._verify_enabled:
            await self._send_noreply_confirmed("SP4_ON", self._mk_cmd("SP4_ON"), tag="[SP4_ON]",
                                               owner=owner, min_delay_s=self._cfg_int("MFC_GAP_MS", 1000) / 1000.0)
            return
        ok = await self._verify_simple_flag("SP4_ON", expect_mask='4', owner=owner)
        if ok: await self._emit_confirmed("SP4_ON", owner=owner)
        else:  await self._emit_failed("SP4_ON", "SP4 상태 확인 실패", owner=owner)

    async def read_flow_all(self, *, owner: Optional[str] = None):
        """R60 한 번 읽고 이벤트로 각 채널 흐름을 방출."""
        vals = await self._read_r60_values()
        if not vals:
            await self._emit_failed("READ_FLOW", "R60 파싱 실패", owner=owner)
            return
        for ch, name in self.gas_map.items():
            idx = ch - 1
            if idx < len(vals):
                v_hw = float(vals[idx])                   # R60은 %FS(HW)로 옴
                v_ui = self._hw_to_ui(ch, v_hw)          # sccm로 변환해 UI에 표시
                await self._emit_flow(name, v_ui)        # UI(sccm) 이벤트
                self._monitor_flow(ch, v_hw)             # 비교는 HW(%FS)

    async def read_pressure(self, *, emit_fail: bool = True, tag: str = "[READ_PRESSURE]",
                            owner: Optional[str] = None) -> Optional[float]:
        """R5(예: READ_PRESSURE) 읽고 UI 문자열/숫자로 이벤트 + 현재 압력값 반환."""
        line = await self._send_and_wait_line(
            self._mk_cmd("READ_PRESSURE"),
            tag=tag, timeout_ms=self._cfg_int("MFC_TIMEOUT", 2000),
            expect_prefixes=("P",),
        )
        if not (line and line.strip()):
            if emit_fail:
                await self._emit_failed("READ_PRESSURE", "응답 없음", owner=owner)
            else:
                await self._emit_status("[READ_PRESSURE] 응답 없음 (non-fatal, will retry)")
            return None
        return self._emit_pressure_from_line_sync(line.strip())

    async def wait_for_pressure_reached(
        self,
        target_pressure: float,
        *,
        timeout_sec: float | None = None,
        check_interval_sec: float | None = None,
    ) -> tuple[bool, float]:
        """
        target_pressure(예: mTorr)에 실제 압력이 허용 오차 범위 안으로
        MFC_PRESSURE_STABLE_COUNT회 연속 들어올 때까지 대기한다.

        :return: (성공 여부, 마지막으로 읽은 압력 값)
        """
        if timeout_sec is None:
            timeout_sec = float(self._cfg_get("MFC_PRESSURE_TIMEOUT_SEC", 60.0))
        if check_interval_sec is None:
            check_interval_sec = float(self._cfg_get("MFC_PRESSURE_CHECK_INTERVAL_SEC", 1.0))

        tol_abs = float(self._cfg_get("MFC_PRESSURE_TOL_ABS", 0.02))
        tol_rel = float(self._cfg_get("MFC_PRESSURE_TOL_REL", 0.05))
        stable_need = int(self._cfg_get("MFC_PRESSURE_STABLE_COUNT", 3))
        fail_max = int(self._cfg_get("MFC_PRESSURE_READ_FAIL_STREAK_MAX", 3))

        if target_pressure <= 0:
            # 0 이하면 '압력 맞추기' 의미가 없으니 바로 실패 처리
            await self._emit_status(
                f"[PRESSURE] target <= 0 이라서 압력 대기를 건너뜁니다. target={target_pressure}"
            )
            return False, 0.0

        stable_count = 0
        elapsed = 0.0
        last_value = 0.0
        fail_streak = 0  # ✅ 루프 밖에서 누적해야 의미가 있음

        await self._emit_status(
            f"[PRESSURE] 목표압 {target_pressure:.3g} "
            f"(tol_abs={tol_abs}, tol_rel={tol_rel*100:.1f}%) "
            f"도달까지 대기 시작"
        )

        while elapsed < timeout_sec:
            current = await self.read_pressure(emit_fail=False)

            if current is None:
                fail_streak += 1
                stable_count = 0

                # ✅ “진짜 응답 없음”이면 여기서 최종 실패(공정 중단)로 보는 기준
                if fail_streak >= fail_max:   # 환경에 맞게 1~5 조정
                    await self._emit_status(
                        f"[PRESSURE] READ_PRESSURE 연속 {fail_streak}회 실패 → 통신불가로 중단"
                    )
                    return False, last_value

                await asyncio.sleep(check_interval_sec)
                elapsed += check_interval_sec
                continue

            # ✅ 유효값을 받으면 실패 누적 리셋
            fail_streak = 0
            last_value = current

            if self.pressure_within_tolerance(target_pressure, current):
                stable_count += 1
                await self._emit_status(
                    f"[PRESSURE] OK ({stable_count}/{stable_need}) "
                    f"target={target_pressure:.3g}, current={current:.3g}"
                )
                if stable_count >= stable_need:
                    await self._emit_status(
                        f"[PRESSURE] 목표압 도달 및 안정: "
                        f"target={target_pressure:.3g}, current={current:.3g}"
                    )
                    return True, current
            else:
                stable_count = 0
                await self._emit_status(
                    f"[PRESSURE] 아직 목표 미달: target={target_pressure:.3g}, current={current:.3g}"
                )

            await asyncio.sleep(check_interval_sec)
            elapsed += check_interval_sec

        await self._emit_status(
            f"[PRESSURE] 타임아웃({timeout_sec:.1f}s) - "
            f"target={target_pressure:.3g}, last={last_value:.3g}"
        )
        return False, last_value

    async def handle_command(self, cmd: str, args: dict | None = None, *, owner: Optional[str] = None) -> None:
        """
        main/process에서 넘어오는 문자열 명령을 고수준 메서드로 라우팅한다.
        - cmd: 'FLOW_SET', 'FLOW_ON', 'FLOW_OFF', 'VALVE_OPEN', 'VALVE_CLOSE',
            'PS_ZEROING', 'MFC_ZEROING',
            'SP1_ON', 'SP2_ON', 'SP3_ON', 'SP4_ON',
            'SP1_SET', 'SP2_SET', 'SP4_SET',
            'READ_FLOW_ALL', 'READ_PRESSURE',
            'WAIT_PRESSURE'   # 🔹 새로 추가되는 명령
        - args: 필요한 인자 (channel, value, target 등)
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
                await self.set_flow(ch, val_ui, owner=owner)

            elif key == "FLOW_ON":
                ch = _req("channel", int)
                await self.flow_on(ch, owner=owner)

            elif key == "FLOW_OFF":
                ch = _req("channel", int)
                await self.flow_off(ch, owner=owner)

            elif key == "VALVE_OPEN":
                await self.valve_open(owner=owner)

            elif key == "VALVE_CLOSE":
                await self.valve_close(owner=owner)

            elif key == "PS_ZEROING":
                # 실제 전송 + gap 뒤 확인(전송 전 폐기되면 실패 — 가짜 확인 금지)
                zgap = self._cfg_int("MFC_ZEROING_GAP_MS", self._cfg_int("MFC_GAP_MS", 1000))
                await self._emit_status("압력 센서 Zeroing 명령 전송")
                await self._send_noreply_confirmed("PS_ZEROING", self._mk_cmd("PS_ZEROING"), tag="[PS_ZEROING]",
                                                   owner=owner, gap_ms=zgap, after_send_s=zgap / 1000.0)

            elif key == "MFC_ZEROING":
                ch = _req("channel", int)
                zgap = self._cfg_int("MFC_ZEROING_GAP_MS", self._cfg_int("MFC_GAP_MS", 1000))
                await self._emit_status(f"Ch{ch} MFC Zeroing 명령 전송")
                await self._send_noreply_confirmed("MFC_ZEROING", self._mk_cmd("MFC_ZEROING", channel=ch),
                                                   tag=f"[MFC_ZEROING ch{ch}]", owner=owner, channel=ch,
                                                   gap_ms=zgap, after_send_s=zgap / 1000.0)

            elif key == "SP1_ON":
                await self.sp1_on(owner=owner)

            elif key == "SP2_ON":
                await self.sp2_on(owner=owner)

            elif key == "SP3_ON":
                await self.sp3_on(owner=owner)

            elif key == "SP4_ON":
                await self.sp4_on(owner=owner)

            elif key == "SP1_SET":
                val_ui = _req("value", float)
                await self.sp1_set(val_ui, owner=owner)

            elif key == "SP2_SET":
                val_ui = _req("value", float)
                await self.sp2_set(val_ui, owner=owner)

            elif key == "SP4_SET":
                val_ui = _req("value", float)
                await self.sp4_set(val_ui, owner=owner)

            # 🔹 압력 도달까지 대기 (옵션으로 SP setpoint 기준 사용 가능)
            elif key == "WAIT_PRESSURE":
                # 기본 target (UI에서 넘어온 값; SP3/4에서 읽기 실패 시 fallback 용이었음)
                target = _req("target", float)
                timeout_default = float(self._cfg_get("MFC_PRESSURE_TIMEOUT_SEC", 60.0))
                timeout = float(args.get("timeout_sec", timeout_default))

                # 새 옵션: 장비 SP setpoint를 먼저 읽어서 target으로 사용할지 여부
                use_sp_target = bool(args.get("use_sp_target", False))
                sp_index_raw = args.get("sp_index", None)

                if use_sp_target and sp_index_raw is not None:
                    try:
                        sp_idx = int(sp_index_raw)
                    except Exception:
                        sp_idx = None

                    if sp_idx in (1, 2, 3, 4):
                        # 🔹 변경: SP setpoint 읽기를 최대 5회 재시도
                        sp_target: Optional[float] = None
                        for attempt in range(1, 6):
                            sp_target = await self._read_sp_setpoint_ui(sp_idx)
                            if sp_target is not None and sp_target > 0:
                                break

                            # 읽기 실패한 경우 재시도 안내 로그
                            await self._emit_status(
                                f"[WAIT_PRESSURE] SP{sp_idx} setpoint 읽기 실패 "
                                f"(재시도 {attempt}/5)"
                            )
                            # 마지막 시도가 아니면 잠깐 대기 후 재시도
                            if attempt < 5:
                                await asyncio.sleep(0.5)

                        # 5회 시도 후에도 유효한 setpoint를 못 읽으면 → 전체 공정 실패 처리
                        if sp_target is None or sp_target <= 0:
                            await self._emit_failed(
                                "WAIT_PRESSURE",
                                f"SP{sp_idx} setpoint를 5회 시도 후에도 읽지 못함 → 압력 대기 불가",
                                owner=owner,
                            )
                            # 여기서 바로 리턴해서 wait_for_pressure_reached() 진입 자체를 막음
                            return

                        # 정상적으로 읽은 경우: 이 값을 기준 target으로 사용
                        await self._emit_status(
                            f"[WAIT_PRESSURE] SP{sp_idx} setpoint "
                            f"{sp_target:.3g} 기준으로 압력 도달 대기"
                        )
                        target = sp_target
                    else:
                        await self._emit_status(
                            f"[WAIT_PRESSURE] 잘못된 sp_index={sp_index_raw!r} → UI target 사용"
                        )

                check_default = float(self._cfg_get("MFC_PRESSURE_CHECK_INTERVAL_SEC", 1.0))
                ok, last = await self.wait_for_pressure_reached(
                    target_pressure=target,
                    timeout_sec=timeout,
                    check_interval_sec=check_default,
                )
                if ok:
                    # → ProcessController 쪽에서 ExpectToken("MFC", "WAIT_PRESSURE") 를 기다리게 할 것
                    await self._emit_confirmed("WAIT_PRESSURE", owner=owner)
                else:
                    await self._emit_failed(
                        "WAIT_PRESSURE",
                        f"압력 안정화 실패: target={target:.3g}, last={last:.3g}",
                        owner=owner,
                    )

            elif key in ("READ_FLOW_ALL", "READ_FLOW"):  # 호환용
                await self.read_flow_all(owner=owner)

            elif key in ("READ_PRESSURE",):
                await self.read_pressure(owner=owner)

            else:
                await self._emit_failed(key, "지원되지 않는 MFC 명령", owner=owner)
        except Exception as e:
            await self._emit_failed(key, f"예외: {e}", owner=owner)

    # ---- 폴링 on/off (Process와 연동) ----
    def _get_loop_safe(self) -> asyncio.AbstractEventLoop:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.get_event_loop_policy().get_event_loop()

    def set_process_status(self, should_poll: bool):
        loop = self._get_loop_safe()
        if should_poll:
            if self._poll_task is None or self._poll_task.done():
                self._ev_nowait(MFCEvent(kind="status", message="Polling read 시작"))
                self._poll_task = loop.create_task(self._poll_loop())
        else:
            if self._poll_task: 
                self._poll_task.cancel()
                self._poll_task = None
            self._poll_cycle_active = False
            purged = self._purge_poll_reads_only(cancel_inflight=True, reason="polling off")
            self._ev_nowait(MFCEvent(kind="status", message="Polling read 중지"))
            if purged:
                self._ev_nowait(MFCEvent(kind="status", message=f"[QUIESCE] Polling read {purged}건 제거 (polling off)"))

    def on_process_finished(self, success: bool, *, reason: Optional[str] = None):
        """공정 종료 시 내부 상태 '전체' 리셋(대기열 전체 폐기 포함). reason 이 있으면 폐기 로그 라벨만 그 값을 쓴다.
        ⚠ 공유 MFC 에서는 다른 사용자가 없을 때만 부른다 — 남아 있으면 release_owner() 를 쓴다."""
        label = reason or f"process finished ({'ok' if success else 'fail'})"
        self.set_process_status(False)
        # 안정화 중지 — 기다리던 주체가 멈춰 있지 않도록 실패로 알린다
        self._cancel_all_stab_jobs(reason=f"정리({label})로 안정화 중단", notify=True)
        if self._stab_task:
            self._stab_task.cancel()
            self._stab_task = None
        # 큐 정리 및 카운터 리셋
        self._purge_pending(label)
        self.last_setpoints = {1: 0.0, 2: 0.0, 3: 0.0}
        self.flow_error_counters = {1: 0, 2: 0, 3: 0}
        # ✅ 플래그도 초기화
        self._flow_on_flags = {1: False, 2: False, 3: False}
        self._ch_owner = {1: None, 2: None, 3: None}
        self._selected_ch = None
        self._selected_owner = None
        self._poll_cycle_active = False

    def release_owner(self, owner: Optional[str], *, reason: str = "") -> None:
        """공유 MFC 에 다른 사용자가 남아 있을 때 owner '몫만' 정리한다(대기열 전체 폐기·전체 초기화 없음).
        - owner 의 채널별 안정화 취소(본인이 떠나는 중이라 실패 통지 없음)
        - owner 가 쓰던 채널의 목표/ON 플래그/오차 카운터 초기화, 선택 채널 해제
        - 대기열의 owner 명령은 남겨 둔다(뒤에 실린 OFF 같은 안전 명령이 순서대로 전송되도록)"""
        if not owner:
            return
        n_stab = 0
        for ch, job in list(self._stab_jobs.items()):
            if job.owner == owner:
                if self._cancel_stab_job(ch, reason=f"{owner} 사용 종료", notify=False):
                    n_stab += 1
        chs = [ch for ch, o in self._ch_owner.items() if o == owner]
        for ch in chs:
            self.last_setpoints[ch] = 0.0
            self._flow_on_flags[ch] = False
            self.flow_error_counters[ch] = 0
            self._ch_owner[ch] = None
        if self._selected_owner == owner:
            self._selected_ch = None
            self._selected_owner = None
        tail = f", {reason}" if reason else ""
        self._ev_nowait(MFCEvent(
            kind="status", owner=owner,
            message=(f"[{owner}] 사용 종료 — 다른 사용자가 있어 대기열 유지, 이 주체 몫만 정리 "
                     f"(채널 {chs or '-'}, 안정화 취소 {n_stab}건{tail})")))

    def on_process_cleanup(self):
        """정리(cleanup) 경로에서의 상태 리셋 — 실패가 아니므로 폐기 로그를 'cleanup' 으로 남긴다."""
        self.on_process_finished(False, reason="cleanup")

    def set_endpoint(self, host: str, port: int, *, reconnect: bool = True) -> None:
        """런타임 엔드포인트 변경. reconnect=True면 즉시 재연결 루틴 트리거."""
        self._override_host = str(host)
        self._override_port = int(port)
        if reconnect:
            # 워치독만 잠깐 멈추고, 현재 연결은 정리
            loop = self._get_loop_safe()
            loop.create_task(self._bounce_connection())

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
        host = self._override_host if self._override_host else str(self._cfg_get("MFC_TCP_HOST", "127.0.0.1"))
        port = self._override_port if self._override_port else int(self._cfg_get("MFC_TCP_PORT", 4000))
        return str(host), int(port)

    # ---------- 내부: 워치독/연결 ----------
    async def _watchdog_loop(self):
        backoff = self._cfg_int("MFC_RECONNECT_BACKOFF_START_MS", 1000)
        while self._want_connected:
            if self._connected:
                await asyncio.sleep(self._cfg_int("MFC_WATCHDOG_INTERVAL_MS", 1500) / 1000.0)
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
                    timeout=max(0.5, float(self._cfg_float("MFC_CONNECT_TIMEOUT_S", 3.0)))
                )
                self._reader, self._writer = reader, writer
                self._connected = True
                self._ever_connected = True
                backoff = self._cfg_int("MFC_RECONNECT_BACKOFF_START_MS", 1000)

                # ★ Keepalive는 config에 따름(기본 False 권장)
                try:
                    sock = writer.get_extra_info("socket")
                    if sock is not None:
                        if self._cfg_bool("MFC_TCP_KEEPALIVE", False):
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
                backoff = min(backoff * 2, self._cfg_int("MFC_RECONNECT_BACKOFF_MAX_MS", 20000))

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
                self._resolve(cmd, False)
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
                remain = (self._last_connect_mono + (self._cfg_int("MFC_POST_OPEN_QUIET_MS", 800) / 1000.0)) - time.monotonic()
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
                drain_to = float(self._cfg_get("MFC_DRAIN_TIMEOUT_S", 2.0))
                await asyncio.wait_for(self._writer.drain(), timeout=drain_to)
            except Exception as e:
                self._dbg("MFC", f"{cmd.tag} {sent_txt} 전송 오류: {e}")
                self._inflight = None
                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    self._cmd_q.appendleft(cmd)
                else:
                    self._resolve(cmd, False)
                    self._safe_callback(cmd.callback, None)
                self._on_tcp_disconnected()
                continue

            # no-reply
            if cmd.allow_no_reply:
                self._inflight = None
                cmd.sent_mono = time.monotonic()
                self._resolve(cmd, True)          # ★ 실제 전송 완료(확정은 여기 이후에만)
                await asyncio.sleep(cmd.gap_ms / 1000.0)

                drain_ms = self._cfg_int("MFC_ALLOW_NO_REPLY_DRAIN_MS", 80)
                await self._absorb_late_lines(drain_ms)

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
                    await asyncio.sleep(self._cfg_int("MFC_POLLING_INTERVAL_MS", 3000) / 1000.0)
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

                # R60 → flow 이벤트 + 모니터링  (★ gas 마스크가 ON일 때만)
                if self._poll_gas_enabled:
                    vals = await self._read_r60_values(tag="[POLL R60]")
                    if vals:
                        for ch, name in self.gas_map.items():
                            idx = ch - 1
                            if idx < len(vals):
                                v_hw = float(vals[idx])               # %FS
                                v_ui = self._hw_to_ui(ch, v_hw)      # sccm
                                await self._emit_flow(name, v_ui)
                                self._monitor_flow(ch, v_hw)         # %FS

                # R5 → pressure 이벤트  (★ pressure 마스크가 ON일 때만)
                if self._poll_pressure_enabled:
                    line = await self._send_and_wait_line(self._mk_cmd("READ_PRESSURE"),
                                                          tag="[POLL PRESS]", timeout_ms=self._cfg_int("MFC_TIMEOUT", 2000),
                                                          expect_prefixes=("P",))
                    if line:
                        self._emit_pressure_from_line_sync(line.strip())

                self._poll_cycle_active = False
                await asyncio.sleep(self._cfg_int("MFC_POLLING_INTERVAL_MS", 3000) / 1000.0)
        except asyncio.CancelledError:
            self._poll_cycle_active = False

    # ---------- 내부: 안정화(채널별 작업, 감시 태스크 1개) ----------
    def _stab_owner(self, ch: int) -> Optional[str]:
        job = self._stab_jobs.get(ch)
        return job.owner if job is not None else None

    def _start_stab_job(self, ch: int, target_hw: float, owner: Optional[str]) -> None:
        """채널 ch 의 안정화 작업 등록(같은 채널 이전 작업은 교체). 감시 태스크가 없으면 올린다."""
        prev = self._stab_jobs.get(ch)
        if prev is not None and prev.owner != owner:
            self._ev_nowait(MFCEvent(
                kind="command_failed", cmd="FLOW_ON", owner=prev.owner, channel=ch,
                reason=f"ch{ch} 안정화 중단 — 다른 주체({owner or '-'})가 같은 채널을 다시 켬"))
        self._stab_jobs[ch] = _StabJob(channel=ch, target_hw=float(target_hw), owner=owner,
                                       created_mono=time.monotonic())
        if self._stab_task is None or self._stab_task.done():
            self._stab_task = asyncio.create_task(self._stabilization_loop(), name="MFCStabilization")

    def _cancel_stab_job(self, ch: int, *, reason: str, notify: bool) -> bool:
        """채널 ch 의 안정화 작업만 취소. notify=True 면 그 작업의 주체에게 FLOW_ON 실패로 알린다."""
        job = self._stab_jobs.pop(ch, None)
        if job is None:
            return False
        if notify:
            self._ev_nowait(MFCEvent(kind="command_failed", cmd="FLOW_ON", owner=job.owner, channel=ch,
                                     reason=f"ch{ch} 안정화 중단 ({reason})"))
        if not self._stab_jobs:
            t = self._stab_task
            if t is not None and t is not asyncio.current_task():
                t.cancel()
                self._stab_task = None
        return True

    def _cancel_all_stab_jobs(self, *, reason: str, notify: bool) -> int:
        n = 0
        for ch in list(self._stab_jobs.keys()):
            if self._cancel_stab_job(ch, reason=reason, notify=notify):
                n += 1
        return n

    async def _stabilization_loop(self):
        """등록된 채널별 안정화 작업을 R60 한 번 읽기로 함께 판정한다(채널마다 목표/주체/시도 횟수 별도)."""
        me = asyncio.current_task()
        try:
            while self._stab_jobs:
                chs = sorted(self._stab_jobs.keys())
                t_read = time.monotonic()
                vals = await self._read_r60_values(tag=f"[STAB R60 ch{','.join(str(c) for c in chs)}]")
                tol_ratio = self._cfg_float("FLOW_ERROR_TOLERANCE", 0.05)
                for ch in chs:
                    job = self._stab_jobs.get(ch)
                    # 읽는 동안 취소됐거나, 읽기 요청 뒤에 새로 등록된 작업이면 이번 값으로 판정하지 않는다
                    if job is None or job.created_mono > t_read:
                        continue
                    actual_hw = None
                    if vals and (ch - 1) < len(vals):
                        actual_hw = float(vals[ch - 1])              # %FS(HW)
                    actual_ui = None if actual_hw is None else self._hw_to_ui(ch, actual_hw)  # sccm
                    tol = job.target_hw * tol_ratio
                    await self._emit_status(
                        f"GAS 확인... ch{ch} (목표: {self._hw_to_ui(ch, job.target_hw):.2f}sccm, "
                        f"현재: {(-1 if actual_ui is None else actual_ui):.2f}sccm)",
                        owner=job.owner, channel=ch)
                    if self._stab_jobs.get(ch) is not job:
                        continue
                    if (actual_hw is not None) and (abs(actual_hw - job.target_hw) <= tol):
                        self._stab_jobs.pop(ch, None)
                        await self._emit_confirmed("FLOW_ON", owner=job.owner, channel=ch)
                        continue
                    # 목표 미도달이면 시도 횟수 증가
                    job.attempts += 1
                    if job.attempts >= 180:  # 가스 안정화 최대 3분 대기(채널별)
                        self._stab_jobs.pop(ch, None)
                        await self._emit_failed("FLOW_ON", "GAS 안정화 시간 초과", owner=job.owner, channel=ch)
                if not self._stab_jobs:
                    break
                await asyncio.sleep(self._cfg_int("MFC_STABILIZATION_INTERVAL_MS", 1000) / 1000.0)
        except asyncio.CancelledError:
            pass
        finally:
            if self._stab_task is me:
                self._stab_task = None

    # ---------- 내부: 고수준 시퀀스/검증 ----------
    async def _verify_flow_set(self, ch: int, scaled_value: float, *, owner: Optional[str] = None) -> bool:
        """READ_FLOW_SET(ch)으로 확인; 불일치면 재설정 후 재확인(최대 5회)."""
        for attempt in range(1, 6):
            line = await self._send_and_wait_line(
                self._mk_cmd("READ_FLOW_SET", channel=ch),
                tag=f"[VERIFY SET ch{ch}]", timeout_ms=self._cfg_int("MFC_TIMEOUT", 2000),
                expect_prefixes=(f"Q{4 + int(ch)}",)
            )

            val = self._parse_q_value_with_prefixes(line or "", prefixes=(f"Q{4 + int(ch)}",))
            ok = (val is not None) and (abs(val - scaled_value) < 0.1)
            if ok:
                await self._emit_status(f"Ch{ch} 목표 {scaled_value:.2f} 설정 확인")
                return True

            # 재전송 후 지연 → 재확인
            self._enqueue(self._mk_cmd("FLOW_SET", channel=ch, value=scaled_value), None,
                          allow_no_reply=True, tag=f"[RE-SET ch{ch}]", owner=owner)
            await self._emit_status(f"[FLOW_SET 검증 재시도] ch{ch}: 기대={scaled_value:.2f}, 응답={repr(line)} (시도 {attempt}/5)")
            await asyncio.sleep(self._cfg_int("MFC_DELAY_MS", 1000) / 1000.0)
        return False

    async def _set_onoff_mask_and_verify(self, bits_target: str) -> bool:
        """L0 적용 후 R69로 확인. 에코/반영 지연 고려해 재시도(최대 5회)."""
        for attempt in range(1, 6):
            # L0 적용 (no-reply)
            self._enqueue(self._mk_cmd("SET_ONOFF_MASK", bits_target), None,
                        allow_no_reply=True, tag=f"[L0 {bits_target}]")

            # 장비 반영 시간 대기 (최소 200ms 보장)
            await asyncio.sleep(max(self._cfg_int("MFC_DELAY_MS", 1000), 200) / 1000.0)

            # ★ 직전 L0 에코/배너가 섞이지 않도록 라인 큐만 짧게 드레인
            await self._absorb_late_lines(120)

            # 검증 (의미없는 빈 라인 방지용으로 최대 2회 읽기)
            now = ""
            for _ in range(2):
                line = await self._send_and_wait_line(self._mk_cmd("READ_MFC_ON_OFF_STATUS"),
                                                    tag="[VERIFY R69]", timeout_ms=self._cfg_int("MFC_TIMEOUT", 2000),
                                                    expect_prefixes=("L0","L"))
                now = self._parse_r69_bits(line or "")
                if now:
                    break

            if now == bits_target:
                await self._emit_status(f"L0 적용 확인: {bits_target}")
                return True

            await self._emit_status(f"[L0 검증 재시도] now={now or '∅'}, want={bits_target} (시도 {attempt}/5)")
            await asyncio.sleep(self._cfg_int("MFC_DELAY_MS", 1000) / 1000.0)
        return False

    async def _valve_move_and_verify(self, origin_cmd: str, *, owner: Optional[str] = None):
        """VALVE_OPEN/CLOSE → READ_VALVE_POSITION 확인(재시도 시 재전송 포함)."""
        # 명령 전송 (no-reply)
        self._enqueue(self._mk_cmd(origin_cmd), None, allow_no_reply=True, tag=f"[{origin_cmd}]", owner=owner)
        delay_valve_ms = self._cfg_int("MFC_DELAY_MS_VALVE", 5000)
        delay_cmd_ms = self._cfg_int("MFC_DELAY_MS", 1000)

        await self._emit_status(f"밸브 이동 대기 ({delay_valve_ms/1000:.0f}초)...")
        await asyncio.sleep(delay_valve_ms / 1000.0)

        for attempt in range(1, 6):
            line = await self._send_and_wait_line(
                self._mk_cmd("READ_VALVE_POSITION"),
                tag=f"[VERIFY VALVE {origin_cmd}]",
                timeout_ms=self._cfg_int("MFC_TIMEOUT", 2000), expect_prefixes=("V",)
            )

            pos_ok = self._parse_valve_ok(origin_cmd, line or "")
            if pos_ok:
                await self._emit_status(f"{origin_cmd} 완료")
                await self._emit_confirmed(origin_cmd, owner=owner)
                return
            # 일부 시점에서 재전송
            if attempt in (2, 4):
                self._enqueue(self._mk_cmd(origin_cmd), None, allow_no_reply=True, tag=f"[RE-{origin_cmd}]",
                              owner=owner)
                await self._emit_status(f"{origin_cmd} 재전송 (시도 {attempt}/5)")
                await asyncio.sleep(max(delay_cmd_ms, delay_valve_ms) / 1000.0)
            else:
                await self._emit_status(f"[{origin_cmd} 검증 재시도] 응답={repr(line)} (시도 {attempt}/5)")
                await asyncio.sleep(self._cfg_int("MFC_DELAY_MS", 1000) / 1000.0)

        await self._emit_failed(origin_cmd, "밸브 위치 확인 실패", owner=owner)

    async def _verify_sp1_set(self, hw_val: float, ui_val: float) -> bool:
        """READ_SP1_VALUE 로 HW값 비교(허용오차 MFC_SP1_VERIFY_TOL)."""
        tol = max(self._cfg_float("MFC_SP1_VERIFY_TOL", 0.02), 1e-9)
        dec = self._cfg_int("MFC_PRESSURE_DECIMALS", 3)

        for attempt in range(1, 6):
            line = await self._send_and_wait_line(
                self._mk_cmd("READ_SP1_VALUE"),
                tag="[VERIFY SP1_SET]", 
                timeout_ms=self._cfg_int("MFC_TIMEOUT", 2000),
                expect_prefixes=("S1",)
            )
            cur_hw = self._parse_pressure_value(line or "")
            if cur_hw is not None:
                cur_hw = round(cur_hw, int(dec))
            ok = (cur_hw is not None) and (abs(cur_hw - hw_val) <= tol)
            if ok:
                await self._emit_status(
                    f"SP1 설정 완료: UI {ui_val:.2f} (장비 {hw_val:.{dec}f})"
                )
                return True
            await self._emit_status(f"[SP1_SET 검증 재시도] 응답={repr(line)} (시도 {attempt}/5)")
            await asyncio.sleep(self._cfg_int("MFC_DELAY_MS", 1000) / 1000.0)
        return False
    
    async def _verify_sp_set(self, sp_idx: int, hw_val: float, ui_val: float) -> bool:
        """
        READ_SP{sp_idx}_VALUE 로 HW값 비교(허용오차 = MFC_SP1_VERIFY_TOL 재사용).
        - config에 READ_SP{sp_idx}_VALUE 키가 없으면 '검증 스킵'으로 간주하여 True 반환.
        - 장비 응답 접두사는 'S{sp_idx}'로 기대.
        """
        tol = max(self._cfg_float("MFC_SP1_VERIFY_TOL", 0.02), 1e-9)
        dec = self._cfg_int("MFC_PRESSURE_DECIMALS", 3)
        key_read = f"READ_SP{sp_idx}_VALUE"

        # 구성에 읽기 명령이 정의되지 않은 경우 검증 스킵
        cmds = self._cfg_get("MFC_COMMANDS", {})
        if not isinstance(cmds, dict):
            cmds = {}

        if key_read not in cmds:
            await self._emit_status(f"[VERIFY SP{sp_idx}_SET] 스킵: '{key_read}' 미정의 → 통과 처리")
            return True

        for attempt in range(1, 6):
            line = await self._send_and_wait_line(
                self._mk_cmd(key_read),
                tag=f"[VERIFY SP{sp_idx}_SET]",
                timeout_ms=self._cfg_int("MFC_TIMEOUT", 2000),
                expect_prefixes=(f"S{sp_idx}",)
            )
            cur_hw = self._parse_pressure_value(line or "")
            if cur_hw is not None:
                cur_hw = round(cur_hw, int(dec))

            ok = (cur_hw is not None) and (abs(cur_hw - hw_val) <= tol)
            if ok:
                await self._emit_status(
                    f"SP{sp_idx} 설정 완료: UI {ui_val:.2f} (장비 {hw_val:.{dec}f})"
                )
                return True

            await self._emit_status(
                f"[SP{sp_idx}_SET 검증 재시도] 응답={repr(line)} (시도 {attempt}/5)"
            )
            await asyncio.sleep(self._cfg_int("MFC_DELAY_MS", 1000) / 1000.0)

        return False

    async def _verify_simple_flag(self, cmd_key: str, expect_mask: str, *, owner: Optional[str] = None) -> bool:
        """SP1_ON/SP4_ON → READ_SYSTEM_STATUS 확인(Mn...)"""
        # 전송(no-reply)
        self._enqueue(self._mk_cmd(cmd_key), None, allow_no_reply=True, tag=f"[{cmd_key}]", owner=owner)
        for attempt in range(1, 6):
            line = await self._send_and_wait_line(
                self._mk_cmd("READ_SYSTEM_STATUS"),
                tag=f"[VERIFY {cmd_key}]", timeout_ms=self._cfg_int("MFC_TIMEOUT", 2000),
                expect_prefixes=("M",)
            )

            s = (line or "").strip().upper()
            ok = bool(s and s.startswith("M") and s[1:2] == expect_mask)
            if ok:
                await self._emit_status(f"{cmd_key} 활성화 확인")
                return True
            await self._emit_status(f"[{cmd_key} 검증 재시도] 응답={repr(line)} (시도 {attempt}/5)")
            await asyncio.sleep(self._cfg_int("MFC_DELAY_MS", 1000) / 1000.0)
        return False

    # ---------- 내부: 단위 파서/도우미 ----------
    async def _read_r60_values(self, tag: str = "[READ R60]") -> Optional[list[float]]:
        line = await self._send_and_wait_line(
            self._mk_cmd("READ_FLOW_ALL"),
            tag=tag, timeout_ms=self._cfg_int("MFC_TIMEOUT", 2000),
            expect_prefixes=("Q0",)
        )
        return self._parse_r60_values(line or "")

    async def _read_r69_bits(self) -> Optional[str]:
        line = await self._send_and_wait_line(
            self._mk_cmd("READ_MFC_ON_OFF_STATUS"),
            tag="[READ R69]", timeout_ms=self._cfg_int("MFC_TIMEOUT", 2000),
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

    def _emit_pressure_from_line_sync(self, line: str) -> Optional[float]:
        val_hw = self._parse_pressure_value(line)
        if val_hw is None:
            return None

        # HW → UI 변환
        scale = self._cfg_float("MFC_PRESSURE_SCALE", 0.1)
        dec = self._cfg_int("MFC_PRESSURE_DECIMALS", 3)

        ui_val = float(val_hw) / float(scale)
        fmt = "{:." + str(int(dec)) + "f}"
        text = fmt.format(ui_val)

        # 이벤트 두 형태를 하나로 통합해 전달
        self._ev_nowait(MFCEvent(kind="pressure", value=ui_val, text=text))

        return ui_val

    def _monitor_flow(self, channel: int, actual_flow_hw: float):
        """
        - 실제 ON 된 채널(self._flow_on_flags)만 감시 — 챔버 공정/PC 구분 없이 채널 기준
          (공유 MFC 동시 사용 시 양쪽 채널을 모두 감시. 선택 채널(_selected_ch)로 거르지 않는다)
        - setpoint(장비 단위)가 사실상 0이면 무시
        """
        if not self._flow_on_flags.get(channel, False):
            self.flow_error_counters[channel] = 0
            return

        target_flow = float(self.last_setpoints.get(channel, 0.0))
        if target_flow < 0.1:
            self.flow_error_counters[channel] = 0
            return

        # 기존 오차 판정 로직 유지
        tol_ratio = self._cfg_float("FLOW_ERROR_TOLERANCE", 0.05)
        if abs(actual_flow_hw - target_flow) > (target_flow * float(tol_ratio)):
            self.flow_error_counters[channel] += 1
            if self.flow_error_counters[channel] >= self._cfg_int("FLOW_ERROR_MAX_COUNT", 3):
                self._ev_nowait(MFCEvent(
                    kind="status",
                    message=f"Ch{channel} GAS 불안정! (목표: {target_flow:.2f}, 현재: {actual_flow_hw:.2f})"
                ))
                self.flow_error_counters[channel] = 0
        else:
            self.flow_error_counters[channel] = 0

    # ---------- 내부: 공통 송수신 ----------
    def _enqueue(self, cmd_str: str, on_reply: Optional[Callable[[Optional[str]], None]],
                *, timeout_ms: Optional[int] = None, gap_ms: Optional[int] = None,
                tag: str = "", retries_left: int = 5, allow_no_reply: bool = False,
                expect_prefixes: tuple[str, ...] = (), owner: Optional[str] = None,
                track: bool = False) -> Command:

        if timeout_ms is None:
            timeout_ms = self._cfg_int("MFC_TIMEOUT", 2000)
        if gap_ms is None:
            gap_ms = self._cfg_int("MFC_GAP_MS", 1000)

        if not cmd_str.endswith(self._tx_eol_str):
            cmd_str += self._tx_eol_str
        cmd = Command(
            cmd_str, on_reply, timeout_ms, gap_ms, tag, retries_left, allow_no_reply,
            expect_prefixes=expect_prefixes, owner=owner
        )
        cmd.enq_mono = time.monotonic()
        if track:
            with contextlib.suppress(RuntimeError):
                cmd.done = asyncio.get_running_loop().create_future()
        self._cmd_q.append(cmd)

        # ★ no-reply 명령의 '에코 라인'은 나중에 도착해도 스킵하도록 등록
        if allow_no_reply:
            no_eol = cmd_str[:-len(self._tx_eol_str)] if cmd_str.endswith(self._tx_eol_str) else cmd_str
            self._skip_echos.append(no_eol)
            if len(self._skip_echos) > 64:
                self._skip_echos.popleft()
            # 추적 로그를 UI/챗으로도 올림
            self._dbg("MFC", f"[ECHO] no-reply 등록: {no_eol}")
        return cmd

    # ---------- 내부: no-reply 실제 전송 확인 ----------
    @staticmethod
    def _resolve(cmd: Optional[Command], ok: bool) -> None:
        f = getattr(cmd, "done", None)
        if f is not None and not f.done():
            with contextlib.suppress(Exception):
                f.set_result(bool(ok))

    def _withdraw(self, cmd: Command, *, reason: str) -> bool:
        """아직 보내지 않은(대기열에 있는) cmd 만 철회. 철회했으면 True."""
        for i, c in enumerate(self._cmd_q):
            if c is cmd:
                del self._cmd_q[i]
                break
        else:
            return False
        if cmd.allow_no_reply:
            no_eol = cmd.cmd_str
            if self._tx_eol_str and no_eol.endswith(self._tx_eol_str):
                no_eol = no_eol[:-len(self._tx_eol_str)]
            with contextlib.suppress(ValueError):
                self._skip_echos.remove(no_eol)
        self._resolve(cmd, False)
        self._safe_callback(cmd.callback, None)
        self._ev_nowait(MFCEvent(kind="status", owner=cmd.owner,
                                 message=f"[WITHDRAW] {cmd.tag} 대기열에서 철회 ({reason})"))
        return True

    async def _await_sent(self, cmd: Command, *, min_delay_s: float = 0.0, after_send_s: float = 0.0,
                          withdraw_on_timeout: bool = True) -> tuple[bool, str]:
        """cmd 가 '실제로 전송'될 때까지 기다린다(최대 SEND_CONFIRM_TIMEOUT_S).
        확정 시점 = max(등록 + min_delay_s, 전송 + after_send_s) — 대기열이 비어 있으면 기존 고정 대기와 같다.
        (ok, 사유) 반환. 전송 전 폐기·전송 실패·시간 초과면 ok=False."""
        fut = cmd.done
        if fut is None:
            # 추적 불가(루프 밖 등록) → 기존 방식(고정 대기)
            await asyncio.sleep(max(min_delay_s, after_send_s, 0.0))
            return True, ""
        bound = float(SEND_CONFIRM_TIMEOUT_S)
        try:
            ok = await asyncio.wait_for(asyncio.shield(fut), timeout=bound)
        except asyncio.TimeoutError:
            if not withdraw_on_timeout:
                return False, f"{bound:.0f}초 안에 전송 확인 못 함(명령은 대기열에 남아 순서대로 전송)"
            if self._withdraw(cmd, reason=f"{bound:.0f}초 전송 대기 초과"):
                return False, f"{bound:.0f}초 안에 전송되지 못해 대기열에서 철회"
            # 대기열에 없음 = 지금 전송 중 → 전송 결과만 짧게 더 기다린다
            try:
                ok = await asyncio.wait_for(asyncio.shield(fut),
                                            timeout=self._cfg_float("MFC_DRAIN_TIMEOUT_S", 2.0) + 1.0)
            except asyncio.TimeoutError:
                return False, "전송 결과 확인 실패"
        if not ok:
            return False, "전송 전에 대기열에서 폐기됨(또는 전송 실패)"
        wait_until = max(cmd.enq_mono + max(0.0, float(min_delay_s)),
                         cmd.sent_mono + max(0.0, float(after_send_s)))
        remain = wait_until - time.monotonic()
        if remain > 0:
            await asyncio.sleep(remain)
        return True, ""

    async def _send_noreply_confirmed(self, key: str, cmd_str: str, *, tag: str, owner: Optional[str],
                                      channel: Optional[int] = None, gap_ms: Optional[int] = None,
                                      min_delay_s: float = 0.0, after_send_s: float = 0.0,
                                      withdraw_on_timeout: bool = True) -> bool:
        """no-reply 명령 1개: 등록 → 실제 전송 확인 → confirmed / failed(owner·channel 포함)."""
        cmd = self._enqueue(cmd_str, None, allow_no_reply=True, tag=tag, gap_ms=gap_ms,
                            owner=owner, track=True)
        ok, why = await self._await_sent(cmd, min_delay_s=min_delay_s, after_send_s=after_send_s,
                                         withdraw_on_timeout=withdraw_on_timeout)
        if ok:
            await self._emit_confirmed(key, owner=owner, channel=channel)
        else:
            await self._emit_failed(key, f"{tag} {why}", owner=owner, channel=channel)
        return ok

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
            timeout_ms=timeout_ms, gap_ms=None,
            tag=tag, retries_left=max(0, int(retries)), allow_no_reply=False,
            expect_prefixes=expect_prefixes # ★ 워커에게 전달
        )

        # 오픈 직후 첫 응답은 여유 부여
        extra = 0.0
        if self._last_connect_mono > 0.0 and (time.monotonic() - self._last_connect_mono) < 2.0:
            extra = self._cfg_int("MFC_FIRST_CMD_EXTRA_TIMEOUT_MS", 2000) / 1000.0
        try:
            return await asyncio.wait_for(fut, timeout=(timeout_ms / 1000.0) + 2.0 + extra)
        except asyncio.TimeoutError:
            return None

    def _mk_cmd(self, key: str, *args, **kwargs) -> str:
        """MFC_COMMANDS 값이 함수/문자열 모두 허용."""
        cmds = self._cfg_get("MFC_COMMANDS", {})
        if not isinstance(cmds, dict):
            cmds = {}
        v = cmds[key]
        if callable(v):
            return str(v(*args, **kwargs))
        return str(v)

    def _purge_pending(self, reason: str = "") -> int:
        purged = 0
        purged_tags: list[str] = []   # ★ 폐기되는 명령 tag 수집 (race condition 사후 분석용)

        # 1) ✅ inflight + cmd_q 비우기    
        if self._inflight is not None:
            cmd = self._inflight
            self._inflight = None
            purged += 1
            if cmd.tag:
                purged_tags.append(cmd.tag)
            self._resolve(cmd, False)
            self._safe_callback(cmd.callback, None)

        while self._cmd_q:
            c = self._cmd_q.popleft()
            purged += 1
            if c.tag:
                purged_tags.append(c.tag)
            self._resolve(c, False)
            self._safe_callback(c.callback, None)

        # 2) ✅ 라인 큐 비우기 (이전 응답/에코가 다음 명령과 섞이는 문제 방지)
        try:
            while True:
                self._line_q.get_nowait()
        except Exception:
            pass

        # 3) ✅ no-reply 에코 드레인 대기열 비우기
        try:
            self._skip_echos.clear()
        except Exception:
            pass

        # 4) ✅ 폴링 중첩 플래그도 리셋(다음 공정 시작 안정성)
        self._poll_cycle_active = False

        if reason:
            if purged_tags:
                self._ev_nowait(MFCEvent(kind="status",
                    message=f"대기 중 명령 {purged}개 폐기 ({reason}) tags={purged_tags!r}"))
            else:
                self._ev_nowait(MFCEvent(kind="status",
                    message=f"대기 중 명령 {purged}개 폐기 ({reason})"))
        return purged

    # ---------- 내부: 이벤트/로그 ----------
    async def _emit_status(self, msg: str, *, owner: Optional[str] = None, channel: Optional[int] = None):
        if self.debug_print:
            print(f"[MFC][status] {msg}")
        await self._event_q.put(MFCEvent(kind="status", message=msg, owner=owner, channel=channel))

    async def _emit_flow(self, gas: str, value_ui: float):
        if self.debug_print:
            print(f"[MFC][flow] {gas}: {value_ui:.2f} sccm")
        await self._event_q.put(MFCEvent(kind="flow", gas=gas, value=value_ui))

    async def _emit_confirmed(self, cmd: str, *, owner: Optional[str] = None, channel: Optional[int] = None):
        await self._event_q.put(MFCEvent(kind="command_confirmed", cmd=cmd, owner=owner, channel=channel))

    async def _emit_failed(self, cmd: str, why: str, *, owner: Optional[str] = None, channel: Optional[int] = None):
        await self._event_q.put(MFCEvent(kind="command_failed", cmd=cmd, reason=why, owner=owner, channel=channel))

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
            except asyncio.CancelledError:
                # ★ 취소된 자식 태스크를 기다리면 CancelledError(BaseException)가 올라온다.
                #   except Exception 만 있으면 여기서 cleanup() 이 중간에 끊긴다(워커 취소 뒤 TCP 종료/폐기 미실행).
                #   rf_pulse/dc_pulse 의 _cancel_task 와 같은 형태.
                pass
            except Exception:
                pass
            setattr(self, name, None)

    def _dbg(self, src: str, msg: str):
        if self.debug_print:
            print(f"[{src}] {msg}")

    # --- 유틸 ---
    def _ui_to_hw(self, ch: int, ui: float) -> float:
        sf_map = self._cfg_get("MFC_SCALE_FACTORS", None)
        if isinstance(sf_map, dict):
            sf = float(sf_map.get(ch, 1.0))
        else:
            sf = float(self.scale_factors.get(ch, 1.0))

        return float(ui) * sf  # sccm -> %FS

    def _hw_to_ui(self, ch: int, hw: float) -> float:
        sf_map = self._cfg_get("MFC_SCALE_FACTORS", None)
        if isinstance(sf_map, dict):
            sf = float(sf_map.get(ch, 1.0))
        else:
            sf = float(self.scale_factors.get(ch, 1.0))

        # sf==0 보호
        return float(hw) / (sf if sf != 0 else 1.0)  # %FS -> sccm

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
            self._resolve(self._inflight, False)
            self._safe_callback(self._inflight.callback, None)
            self._inflight = None
            purged += 1
            self._dbg("MFC", f"[QUIESCE] Polling inflight 취소: {reason}")
        kept = deque()
        while self._cmd_q:
            c = self._cmd_q.popleft()
            if self._is_poll_read_cmd(c.cmd_str, c.tag):
                self._resolve(c, False)
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
        inactivity_s = self._cfg_float("MFC_INACTIVITY_REOPEN_S", 0.0)
        if inactivity_s > 0:
            idle = time.monotonic() - (self._last_io_mono or 0.0)
            if idle >= inactivity_s:
                await self._emit_status(f"[MFC] idle {idle:.1f}s ≥ {inactivity_s:.1f}s → 세션 재시작")
                self._on_tcp_disconnected()



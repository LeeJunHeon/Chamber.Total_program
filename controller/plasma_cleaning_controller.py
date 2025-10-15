# -*- coding: utf-8 -*-
"""
plasma_cleaning_controller.py

플라즈마 클리닝 컨트롤러 (asyncio)
- 공정 순서: ① GV Open → ② IG On & Target Pressure 달성 대기 → ③ MFC ch3(N2) 유량 + SP4 Set/On → ④ RF On → 공정 유지
- IG는 '장비(AsyncIG)'를 직접 사용. PLC로 IG 토글하지 않음.
- Working Pressure는 SP4 Set → SP4 On으로 수행.
- Target Pressure는 IG가 읽어온 압력이 '이하'가 될 때까지(최대 180s, 10s 간격) 대기.
- MFC는 3번 가스(N2)만 사용.
- 남은 시간은 progress 이벤트로 초 단위 제공(로그로는 쓰지 않음 — UI에서 표시 업데이트).
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Callable, Awaitable, Any, AsyncGenerator
import asyncio
import contextlib
import time


# ─────────────────────────────────────────────────────────────
# 이벤트/파라미터 모델
# ─────────────────────────────────────────────────────────────
@dataclass
class PCleanEvent:
    kind: str                  # "status" | "log" | "progress" | "started" | "finished" | "failed"
    message: Optional[str] = None
    payload: Optional[dict] = None


@dataclass
class PlasmaCleaningParams:
    # 필수 입력
    target_pressure: float       # IG 기준: 이하가 되면 다음 단계로
    gas_flow_sccm: float         # N2@MFC ch3
    working_pressure: float      # SP4 Set 값(장치가 내부 변환)
    rf_power_w: float            # RF 목표 W (0보다 커야 함)
    duration_sec: float          # 공정 유지 시간(초)

    # 선택/정책
    target_chamber: Optional[int] = None
    stabilize_timeout: float = 40.0
    post_purge_sec: float = 2.0


# ─────────────────────────────────────────────────────────────
# 컨트롤러
# ─────────────────────────────────────────────────────────────
class PlasmaCleaningController:
    """
    의존성(콜백/핸들 기반):
      - mfc_handle(cmd: str, args: dict|None) -> awaitable[None]
          * 필수 명령: "FLOW_SET","FLOW_ON","FLOW_OFF","SP4_SET","SP4_ON"
      - ig: AsyncIG 인스턴스 (ensure_on(), ensure_off(), read_pressure() 등)
      - plc_write_switch(key: str, on: bool) -> awaitable[None] (GV 토글용, 선택)
      - rf_start(power_w: float) -> awaitable[None] (선택)
      - rf_stop() -> awaitable[None] (선택)
    """
    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        mfc_handle: Callable[[str, Optional[dict]], Awaitable[None]],
        ig: Any,
        plc_write_switch: Optional[Callable[[str, bool], Awaitable[None]]] = None,
        rf_start: Optional[Callable[[float], Awaitable[None]]] = None,
        rf_stop: Optional[Callable[[], Awaitable[None]]] = None,
        gv_switch_by_ch: Optional[dict[int, str]] = None,   # 예) {1: "GV1_OPEN_SW", 2: "GV2_OPEN_SW"}
    ):
        self.loop = loop
        self.mfc_handle = mfc_handle
        self.ig = ig
        self.plc_write_switch = plc_write_switch
        self._rf_start = rf_start
        self._rf_stop = rf_stop

        self.gv_switch_by_ch = dict(gv_switch_by_ch or {})

        self._ev_q: asyncio.Queue[PCleanEvent] = asyncio.Queue(maxsize=1024)
        self._task: Optional[asyncio.Task] = None
        self._stop_evt = asyncio.Event()
        self._running = False

    # ─────────────────────────────
    # 외부 API
    # ─────────────────────────────
    def start(self, params: PlasmaCleaningParams) -> None:
        if self._running:
            self._ev_nowait(PCleanEvent(kind="status", message="이미 진행 중입니다."))
            return
        self._stop_evt.clear()
        self._running = True
        self._task = self.loop.create_task(self._run(params), name="PClean.run")

    def request_stop(self) -> None:
        if not self._running:
            return
        self._ev_nowait(PCleanEvent(kind="status", message="정지 요청 수신"))
        self._stop_evt.set()

    async def events(self) -> AsyncGenerator[PCleanEvent, None]:
        while True:
            ev = await self._ev_q.get()
            yield ev

    # ─────────────────────────────
    # 메인 루틴
    # ─────────────────────────────
    async def _run(self, p: PlasmaCleaningParams):
        ch = int(p.target_chamber) if p.target_chamber else None
        try:
            self._ev_nowait(PCleanEvent(kind="started", message="Load-Lock 플라즈마 클리닝 시작"))

            # ① GV Open (옵션: 매핑이 있을 때만)
            await self._try_gv_open(ch)

            # ② IG On + Target Pressure 달성 대기
            await self._ig_ensure_on()
            ok = await self._wait_pressure_below_or_equal(target=p.target_pressure, timeout_s=180.0, interval_s=10.0)
            if not ok:
                raise RuntimeError(f"IG 압력이 {p.target_pressure:g} 이하로 떨어지지 않음(3분 제한 초과)")

            # ③ MFC: SP4 Set → SP4 On → N2(ch3) Flow Set/On
            await self._mfc_sp4_set(p.working_pressure)
            await self._mfc_sp4_on()
            await self._mfc_set_on(ch=3, sccm=p.gas_flow_sccm)

            # ④ RF: 옵션(0보다 크면 On)
            if self._rf_start and p.rf_power_w > 0:
                await self._rf_start(float(p.rf_power_w))
                self._ev_nowait(PCleanEvent(kind="status", message=f"RF 시작: {float(p.rf_power_w):.1f} W"))

            # 유지 구간: 남은 시간(초) 이벤트로만 통지(로그 X)
            deadline = time.monotonic() + float(p.duration_sec)
            while not self._stop_evt.is_set():
                left = max(0.0, deadline - time.monotonic())
                self._ev_nowait(PCleanEvent(kind="progress", payload={"seconds_left": int(left)}))
                if left <= 0:
                    break
                await asyncio.sleep(1.0)

            # 정리
            await self._safe_cleanup(stop_rf=True, stop_gas=True)
            self._ev_nowait(PCleanEvent(kind="finished", message="플라즈마 클리닝 정상 종료"))
        except asyncio.CancelledError:
            await self._safe_cleanup(stop_rf=True, stop_gas=True)
            self._ev_nowait(PCleanEvent(kind="failed", message="태스크 취소"))
        except Exception as e:
            await self._safe_cleanup(stop_rf=True, stop_gas=True)
            self._ev_nowait(PCleanEvent(kind="failed", message=f"예외: {e!r}"))
        finally:
            self._running = False
            self._task = None
            self._ev_nowait(PCleanEvent(kind="status", message="공정 종료됨"))

    # ─────────────────────────────
    # 내부 헬퍼
    # ─────────────────────────────
    def _ev_nowait(self, ev: PCleanEvent) -> None:
        try:
            self._ev_q.put_nowait(ev)
        except Exception:
            pass

    async def _mfc_set_on(self, *, ch: int, sccm: float) -> None:
        await self.mfc_handle("FLOW_SET", {"channel": int(ch), "value": float(sccm)})
        await self.mfc_handle("FLOW_ON",  {"channel": int(ch)})

    async def _mfc_off(self, *, ch: int) -> None:
        with contextlib.suppress(Exception):
            await self.mfc_handle("FLOW_OFF", {"channel": int(ch)})

    async def _mfc_sp4_set(self, working_pressure_ui: float) -> None:
        """
        Working Pressure를 SP4로 '설정' (장치가 내부 스케일/단위 변환 처리).
        ※ mfc.py에 'SP4_SET' 핸들러가 있어야 합니다.
           - (없다면) mfc.py에 handle_command('SP4_SET', {'value': <UI값>}) 분기와
             실제 S4 전송 로직을 추가하세요.
        """
        await self.mfc_handle("SP4_SET", {"value": float(working_pressure_ui)})

    async def _mfc_sp4_on(self) -> None:
        await self.mfc_handle("SP4_ON")

    async def _try_gv_open(self, ch: Optional[int]) -> None:
        if ch is None or not self.plc_write_switch:
            return
        key = self._key_gv_open(ch)
        if not key:
            return
        with contextlib.suppress(Exception):
            await self.plc_write_switch(key, True)
            self._ev_nowait(PCleanEvent(kind="status", message=f"GV{ch} 오픈 스위치 ON"))
            await asyncio.sleep(1.0)

    def _key_gv_open(self, ch: int) -> Optional[str]:
        return self.gv_switch_by_ch.get(ch)

    # ── IG 제어/대기 ─────────────────────────────────────
    async def _ig_ensure_on(self) -> None:
        """IG 장비 전원을 켬(ensure_on 존재 시) 또는 start만 보장."""
        try:
            fn = getattr(self.ig, "ensure_on", None)
            if callable(fn):
                await fn()
                return
        except Exception:
            pass
        # fallback: 연결만 보장
        try:
            st = getattr(self.ig, "start", None)
            if callable(st):
                res = st()
                if asyncio.iscoroutine(res):
                    await res
        except Exception:
            pass

    async def _ig_ensure_off(self) -> None:
        try:
            fn = getattr(self.ig, "ensure_off", None)
            if callable(fn):
                await fn()
        except Exception:
            pass

    async def _ig_read_pressure(self) -> Optional[float]:
        """IG 현재 압력 읽기(없으면 None)."""
        try:
            fn = getattr(self.ig, "read_pressure", None)
            if callable(fn):
                v = fn()
                return await v if asyncio.iscoroutine(v) else float(v)
        except Exception:
            return None
        # fallback: last_pressure 속성 시도
        try:
            v = getattr(self.ig, "last_pressure", None)
            if v is None:
                return None
            return float(v)
        except Exception:
            return None

    async def _wait_pressure_below_or_equal(self, *, target: float, timeout_s: float, interval_s: float) -> bool:
        """IG 압력이 target 이하가 될 때까지 대기(최대 timeout_s, interval_s 간격)."""
        deadline = time.monotonic() + float(timeout_s)
        while not self._stop_evt.is_set():
            cur = await self._ig_read_pressure()
            if (cur is not None) and (cur <= float(target)):
                self._ev_nowait(PCleanEvent(kind="status", message=f"IG OK: {cur:.3g} ≤ {target:g}"))
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(float(interval_s))
        return False

    async def _safe_cleanup(self, *, stop_rf: bool, stop_gas: bool) -> None:
        # 가스 → RF → IG Off 순
        if stop_gas:
            await self._mfc_off(ch=3)
            self._ev_nowait(PCleanEvent(kind="status", message="MFC ch3 OFF"))
        if stop_rf and self._rf_stop:
            with contextlib.suppress(Exception):
                await self._rf_stop()
                self._ev_nowait(PCleanEvent(kind="status", message="RF 정지"))
        await self._ig_ensure_off()

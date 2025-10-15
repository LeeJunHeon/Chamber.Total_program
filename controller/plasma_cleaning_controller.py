# controller/plasma_cleaning_controller.py
# -*- coding: utf-8 -*-
"""
plasma_cleaning_controller.py

플라즈마 클리닝 컨트롤러 (asyncio)
- 공정 순서(요청 사양 반영, 모두 필수):
  ① GV Open (필수, 실패 시 공정 시작 차단)
  ② IG 기준 Target Pressure(이하) 달성 대기  ← IG 자체 wait_for_base_pressure 활용
  ③ MFC1 ch3(N2) 유량 설정/ON                ← Gas Flow 먼저 (항상 ch=3)
  ④ SP4_SET → SP4_ON                         ← Working Pressure 제어(장비 내부 변환)
  ⑤ RF On → 공정 유지(duration_sec)
  종료 시(모두 실행):
    Gas OFF(ch3) → RF OFF → VALVE_OPEN(= SP4 해제 대용) → IG Off → GV Close

- MFC 명령은 device/mfc.AsyncMFC.handle_command에 맞춰 사용:
  'FLOW_SET','FLOW_ON','FLOW_OFF','SP4_SET','SP4_ON','VALVE_OPEN' 등.
- IG는 장비(AsyncIG)의 wait_for_base_pressure()를 사용(ON/재점등/폴링/정리 포함).
- 남은 시간은 progress 이벤트로 초 단위 제공.
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
    target_pressure: float       # IG 기준: 이하가 되면 다음 단계로 (Torr, ig.py 그대로)
    gas_flow_sccm: float         # N2@MFC ch3
    working_pressure: float      # SP4_SET 값(장치가 내부 변환)
    rf_power_w: float            # RF 목표 W (0보다 커야 함)
    duration_sec: float          # 공정 유지 시간(초)

    # 선택/정책
    target_chamber: Optional[int] = None
    # ig.wait_for_base_pressure 내부 제한을 따르므로 여기 timeout은 참조용
    stabilize_timeout: float = 40.0
    post_purge_sec: float = 2.0


# ─────────────────────────────────────────────────────────────
# 컨트롤러
# ─────────────────────────────────────────────────────────────
class PlasmaCleaningController:
    """
    의존성(콜백/핸들 기반):
      - mfc_handle(cmd: str, args: dict|None) -> awaitable[None]
          * 사용 명령: "FLOW_SET","FLOW_ON","FLOW_OFF","SP4_SET","SP4_ON","VALVE_OPEN"
      - ig: AsyncIG 인스턴스 (wait_for_base_pressure(), ensure_off() 등)
      - plc_write_switch(key: str, on: bool) -> awaitable[None] (GV 토글용, 필수)
      - rf_start(power_w: float) -> awaitable[None] (선택)
      - rf_stop() -> awaitable[None] (선택)
      - gv_switch_by_ch: {ch:int -> plc switch key:str} (필수)
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
        try:
            # ── 사전검증: GV 제어 필수 의존성 체크 ───────────────────
            ch = self._resolve_ch(p.target_chamber)
            key = self._key_gv_open(ch)  # 반드시 존재해야 함
            if self.plc_write_switch is None:
                raise RuntimeError("PLC GV 스위치 핸들이 없습니다(plc_write_switch=None).")
            if not key:
                raise RuntimeError(f"GV 스위치 키를 찾을 수 없습니다(ch={ch}). gv_switch_by_ch를 확인하세요.")

            self._ev_nowait(PCleanEvent(kind="started", message="Load-Lock 플라즈마 클리닝 시작"))

            # ① GV Open (필수, 실패 시 공정 시작 차단)
            await self._gv_open_required(key)

            # ② IG 기준 Target Pressure(이하) 달성 대기
            await self._ig_wait_base(target=p.target_pressure)

            # ③ N2 가스 유량 먼저: MFC1 ch3 FLOW_SET/ON
            await self._mfc_set_on(ch=3, sccm=p.gas_flow_sccm)
            self._ev_nowait(PCleanEvent(kind="status", message=f"MFC ch3 유량 설정/ON: {p.gas_flow_sccm:.3g} sccm"))

            # ④ Working Pressure: SP4_SET → SP4_ON
            await self._mfc_sp4_set(p.working_pressure)
            await self._mfc_sp4_on()
            self._ev_nowait(PCleanEvent(kind="status", message=f"SP4_SET / SP4_ON 완료 (Set={p.working_pressure:g})"))

            # ⑤ RF: 옵션(0보다 크면 On)
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
            await self._safe_cleanup(stop_rf=True, stop_gas=True, gv_key=key)
            self._ev_nowait(PCleanEvent(kind="finished", message="플라즈마 클리닝 정상 종료"))
        except asyncio.CancelledError:
            await self._safe_cleanup(stop_rf=True, stop_gas=True, gv_key=self._key_gv_open(self._resolve_ch(p.target_chamber)))
            self._ev_nowait(PCleanEvent(kind="failed", message="태스크 취소"))
        except Exception as e:
            await self._safe_cleanup(stop_rf=True, stop_gas=True, gv_key=self._key_gv_open(self._resolve_ch(p.target_chamber)))
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

    def _resolve_ch(self, ch_opt: Optional[int]) -> int:
        """필수 채널 결정: None이면 매핑이 하나뿐일 때 그 키를 사용, 여러 개면 에러."""
        if ch_opt is not None:
            return int(ch_opt)
        if not self.gv_switch_by_ch:
            raise RuntimeError("GV 채널 매핑이 비어 있습니다(gv_switch_by_ch).")
        keys = sorted(self.gv_switch_by_ch.keys())
        if len(keys) == 1:
            return int(keys[0])
        raise RuntimeError(f"target_chamber가 지정되지 않았고, GV 매핑이 다수({keys})입니다. 채널을 명시하세요.")

    async def _gv_open_required(self, key: str) -> None:
        """GV Open은 필수 — 실패 시 예외 발생."""
        assert self.plc_write_switch is not None
        await self.plc_write_switch(key, True)
        self._ev_nowait(PCleanEvent(kind="status", message=f"GV Open: {key} → ON"))
        await asyncio.sleep(1.0)

    async def _gv_close_required(self, key: str) -> None:
        """종료 시 GV Close도 필수 — 실패 시 이벤트로 남김(정리 경로이므로 예외는 삼킴)."""
        assert self.plc_write_switch is not None
        await self.plc_write_switch(key, False)
        self._ev_nowait(PCleanEvent(kind="status", message=f"GV Close: {key} → OFF"))

    # ── MFC 제어 ─────────────────────────────────────
    async def _mfc_set_on(self, *, ch: int, sccm: float) -> None:
        await self.mfc_handle("FLOW_SET", {"channel": int(ch), "value": float(sccm)})
        await self.mfc_handle("FLOW_ON",  {"channel": int(ch)})

    async def _mfc_off(self, *, ch: int) -> None:
        with contextlib.suppress(Exception):
            await self.mfc_handle("FLOW_OFF", {"channel": int(ch)})

    async def _mfc_sp4_set(self, working_pressure_ui: float) -> None:
        """
        Working Pressure를 SP4로 '설정' (장치가 내부 스케일/단위 변환 처리).
        ※ mfc.py에 'SP4_SET' 분기가 있어야 합니다.
        """
        await self.mfc_handle("SP4_SET", {"value": float(working_pressure_ui)})

    async def _mfc_sp4_on(self) -> None:
        await self.mfc_handle("SP4_ON")

    async def _mfc_sp4_release_by_valve_open(self) -> None:
        """
        SP4 OFF 대용: 장비 사양상 별도 OFF 명령이 없으므로, 'VALVE_OPEN'을 사용.
        """
        with contextlib.suppress(Exception):
            await self.mfc_handle("VALVE_OPEN")

    # ── IG 제어/대기 ─────────────────────────────────────
    async def _ig_wait_base(self, *, target: float) -> None:
        """
        IG가 target 이하에 도달할 때까지 ig.wait_for_base_pressure()에 위임.
        - ig.py가 ON/재점등/폴링/정리(SIG0)까지 수행.
        - 실패 시 예외 발생 처리.
        """
        interval_ms = 300  # 필요 시 ig config로 세부 튜닝 가능
        try:
            ok = await self.ig.wait_for_base_pressure(base_pressure=float(target), interval_ms=interval_ms)
        except Exception as e:
            raise RuntimeError(f"IG 대기 예외: {e!r}")
        if not ok:
            raise RuntimeError(f"IG 압력이 {target:g} 이하로 내려가지 않음")

    # ── 종료 시퀀스 ─────────────────────────────────────
    async def _safe_cleanup(self, *, stop_rf: bool, stop_gas: bool, gv_key: Optional[str]) -> None:
        # 종료 시퀀스: Gas OFF → RF OFF → VALVE_OPEN → IG OFF → GV Close(필수)
        if stop_gas:
            with contextlib.suppress(Exception):
                await self._mfc_off(ch=3)
                self._ev_nowait(PCleanEvent(kind="status", message="MFC ch3 OFF"))

        if stop_rf and self._rf_stop:
            with contextlib.suppress(Exception):
                await self._rf_stop()
                self._ev_nowait(PCleanEvent(kind="status", message="RF 정지"))

        with contextlib.suppress(Exception):
            await self._mfc_sp4_release_by_valve_open()
            self._ev_nowait(PCleanEvent(kind="status", message="VALVE_OPEN(=SP4 해제)"))

        # IG 정리(ig.wait_for_base_pressure가 cleanup을 했더라도 이 호출은 idempotent)
        try:
            fn = getattr(self.ig, "ensure_off", None)
            if callable(fn):
                await fn()
        except Exception:
            pass

        if gv_key and self.plc_write_switch:
            with contextlib.suppress(Exception):
                await self._gv_close_required(gv_key)

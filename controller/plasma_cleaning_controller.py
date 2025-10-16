# controller/plasma_cleaning_controller.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional


# ===== 파라미터 =====
@dataclass
class PCParams:
    # gas / mfc
    gas_idx: int = 3                 # Gas #3 (N2) 사용
    gas_flow_sccm: float = 0.0       # 유량 (FLOW_ON 시 적용)
    # 압력
    target_pressure: float = 5.0e-6  # IG 목표(이 값보다 낮아지면 통과)
    tol_mTorr: float = 0.2           # IG 허용 편차
    wait_timeout_s: float = 90.0     # IG 타임아웃
    settle_s: float = 5.0            # 안정화 대기
    # SP4 (Working Pressure)
    sp4_setpoint_mTorr: float = 2.0
    # RF
    rf_power_w: float = 100.0
    # 프로세스 시간
    process_time_min: float = 1.0        # 분 단위

# ===== 컨트롤러 =====
class PlasmaCleaningController:
    """
    공정 시퀀스는 여기에서 최대한 수행.
    런타임은 장비 I/O 함수와 로그/UI 콜백만 주입.
    """

    def __init__(
        self,
        *,
        # --- 필수 콜백(런타임이 주입) ---
        log: Callable[[str, str], None],
        # PLC(GV/램프/인터락)
        plc_check_gv_interlock: Callable[[], Awaitable[bool]],
        plc_gv_open: Callable[[], Awaitable[None]],
        plc_gv_close: Callable[[], Awaitable[None]],
        plc_read_gv_open_lamp: Callable[[], Awaitable[bool]],
        # IG
        ensure_ig_on: Callable[[], Awaitable[None]],
        read_ig_mTorr: Callable[[], Awaitable[float]],
        # IG (Torr 기준 대기: target_torr, interval_ms)
        ig_wait_for_base_torr: Optional[Callable[[float, int], Awaitable[bool]]] = None,  # ★ 추가/정형화
        # MFC (#1 고정, gas_idx 선택)
        mfc_gas_select: Callable[[int], Awaitable[None]],
        mfc_flow_set_on: Callable[[float], Awaitable[None]],
        mfc_flow_off: Callable[[], Awaitable[None]],
        mfc_sp4_set: Callable[[float], Awaitable[None]],
        mfc_sp4_on: Callable[[], Awaitable[None]],
        mfc_sp4_off: Callable[[], Awaitable[None]],
        # RF (PLC DAC 경유)
        rf_start: Callable[[float], Awaitable[None]],
        rf_stop: Callable[[], Awaitable[None]],
        # UI
        show_state: Callable[[str], None],
        show_countdown: Callable[[int], None],
    ) -> None:
        self._log = log
        self._plc_check_gv_interlock = plc_check_gv_interlock
        self._plc_gv_open = plc_gv_open
        self._plc_gv_close = plc_gv_close
        self._plc_read_gv_open_lamp = plc_read_gv_open_lamp

        self._ig_wait_for_base_torr = ig_wait_for_base_torr
        self._ensure_ig_on = ensure_ig_on
        self._read_ig_mTorr = read_ig_mTorr

        self._mfc_gas_select = mfc_gas_select
        self._mfc_flow_set_on = mfc_flow_set_on
        self._mfc_flow_off = mfc_flow_off
        self._mfc_sp4_set = mfc_sp4_set
        self._mfc_sp4_on = mfc_sp4_on
        self._mfc_sp4_off = mfc_sp4_off

        self._rf_start = rf_start
        self._rf_stop = rf_stop

        self._show_state = show_state
        self._show_countdown = show_countdown

        self.is_running: bool = False
        self._task: Optional[asyncio.Task] = None
        self._stop_evt = asyncio.Event()

    # ─────────────────────────────────────────────────────────────
    # 외부(UI/런타임)에서 쓰는 API
    # ─────────────────────────────────────────────────────────────
    def start(self, params: dict) -> None:
        if self.is_running:
            self._log("PC", "이미 실행 중입니다."); return
        p = PCParams(
            gas_idx               = int(params.get("pc_gas_mfc_idx", 3)),
            gas_flow_sccm         = float(params.get("pc_gas_flow_sccm", 0.0)),
            target_pressure       = float(params.get("pc_target_pressure", 5.0e-6)),
            tol_mTorr             = float(params.get("pc_tol_mTorr", 0.2)),
            wait_timeout_s        = float(params.get("pc_wait_timeout_s", 90.0)),
            settle_s              = float(params.get("pc_settle_s", 5.0)),
            sp4_setpoint_mTorr    = float(params.get("pc_sp4_setpoint_mTorr", 2.0)),
            rf_power_w            = float(params.get("pc_rf_power_w", 100.0)),
            process_time_min      = float(params.get("pc_process_time_min", 1.0)),
        )
        self._stop_evt = asyncio.Event()
        self._task = asyncio.create_task(self._run(p), name="PC_Run")

    def request_stop(self) -> None:
        if not self.is_running:
            return
        self._log("PC", "정지 요청 수신")
        self._stop_evt.set()

    # (MFC 이벤트용; 런타임에서 UI 반영에 사용)
    def on_mfc_confirmed(self, cmd: str) -> None:
        self._log("MFC", f"명령 확인: {cmd}")

    def on_mfc_failed(self, cmd: str, reason: str) -> None:
        self._log("MFC", f"명령 실패: {cmd} ({reason})")

    # ─────────────────────────────────────────────────────────────
    # 내부 실행 시퀀스
    # ─────────────────────────────────────────────────────────────
    async def _run(self, p: PCParams) -> None:
        self.is_running = True
        self._show_state("RUNNING")
        self._log("PC", "플라즈마 클리닝 시작")

        try:
            # 1) PLC - 게이트밸브 인터락 확인 → OPEN_SW → 5초 후 OPEN_LAMP TRUE?
            self._log("PLC", "GV 인터락 확인")
            ok = await self._plc_check_gv_interlock()
            if not ok:
                raise RuntimeError("게이트밸브 인터락 FALSE")

            self._log("PLC", "GV OPEN_SW 실행")
            await self._plc_gv_open()
            await asyncio.sleep(5.0)  # 램프 확인 지연
            lamp = await self._plc_read_gv_open_lamp()
            if not lamp:
                raise RuntimeError("GV OPEN_LAMP가 TRUE가 아님(오픈 실패?)")

            # 2) IG ON & 목표(보다 낮음) 대기 — IG 내부 API 사용
            await self._ensure_ig_on()
            if self._ig_wait_for_base_torr:
                self._log("IG", f"IG.wait_for_base_pressure: {p.target_pressure:.3e} Torr 대기")
                ok = await self._ig_wait_for_base_torr(p.target_pressure, interval_ms=1000)
                if not ok:
                    raise RuntimeError("Base pressure not reached (IG API)")
            else:
                # 콜백이 없으면 기존(mTorr 폴링) 경로로 폴백
                self._log("IG", f"IG 목표 미만 대기: {p.target_pressure:.3f} Torr")
                await self._wait_ig_below(p.target_pressure, p.tol_mTorr, p.wait_timeout_s, p.settle_s)

            # 3) MFC 가스 설정 (Gas #3 N2) + 4) SP4 세팅/ON
            await self._mfc_gas_select(p.gas_idx)            # ← Gas #3
            if p.gas_flow_sccm > 0.0:
                await self._mfc_flow_set_on(p.gas_flow_sccm) # 내부밸브 오픈 + 유량 설정
            await self._mfc_sp4_set(p.sp4_setpoint_mTorr)
            await self._mfc_sp4_on()

            # 6) RF POWER(PLC DAC) 설정
            self._log("RF", f"RF Power 적용: {p.rf_power_w:.1f} W")
            await self._rf_start(p.rf_power_w)

            # 7) PROCESS TIME 카운트다운
            total_sec = int(max(0, p.process_time_min * 60.0))
            for left in range(total_sec, -1, -1):
                if self._stop_evt.is_set():
                    break
                self._show_countdown(left)
                await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._log("PC", f"오류: {e!r}")
        finally:
            # 종료 시퀀스
            try:
                # 1) RF POWER OFF
                await self._rf_stop()
            except Exception as e:
                self._log("RF", f"OFF 실패: {e!r}")

            try:
                # 2) MFC GAS OFF
                await self._mfc_sp4_off()
                await self._mfc_flow_off()
                # 3) (요청사항) MFC VALVE OPEN = 유량 0으로 FLOW_ON 유지(내부 밸브만 open)
                await self._mfc_flow_set_on(0.0)
            except Exception as e:
                self._log("MFC", f"종료 동작 실패: {e!r}")

            try:
                # 4) GV 인터락 TRUE면 CLOSE_SW → 5초 후 OPEN_LAMP FALSE?
                self._log("PLC", "GV 인터락 재확인 후 CLOSE_SW")
                ok = await self._plc_check_gv_interlock()
                if ok:
                    await self._plc_gv_close()
                    await asyncio.sleep(5.0)
                    lamp = await self._plc_read_gv_open_lamp()
                    if lamp:
                        self._log("PLC", "경고: CLOSE 후에도 OPEN_LAMP가 TRUE")
                else:
                    self._log("PLC", "경고: 인터락 FALSE 상태 — CLOSE_SW 스킵")
            except Exception as e:
                self._log("PLC", f"GV 종료 실패: {e!r}")

            self.is_running = False
            self._show_state("IDLE")
            self._log("PC", "플라즈마 클리닝 종료")

    # ─────────────────────────────────────────────────────────────
    # 유틸
    # ─────────────────────────────────────────────────────────────
    async def _wait_ig_below(self, target_mTorr: float, tol: float, timeout_s: float, settle_s: float) -> None:
        """IG가 target 미만으로 떨어지고, tol 내에서 settle_s 유지될 때까지 대기."""
        deadline = asyncio.get_running_loop().time() + float(timeout_s)
        stable_start: Optional[float] = None

        while True:
            if self._stop_evt.is_set():
                raise asyncio.CancelledError()

            now = asyncio.get_running_loop().time()
            if now > deadline:
                raise TimeoutError("IG 목표 도달 타임아웃")

            try:
                p = float(await self._read_ig_mTorr())
            except Exception:
                p = float("inf")

            if p < target_mTorr + tol:
                if stable_start is None:
                    stable_start = now
                if (now - stable_start) >= float(settle_s):
                    return
            else:
                stable_start = None

            await asyncio.sleep(0.5)

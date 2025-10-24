# controller/plasma_cleaning_controller.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio, traceback, contextlib
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
        chat_notifier: Optional[Any] = None,   # ★ 추가
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

        self.last_result: str = "success"
        self.last_reason: str = ""

        self.chat = chat_notifier              # ★ 보관
        self._final_notified = False           # ★ 중복발송 가드(권장)

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
        c = (cmd or "").upper()
        if any(key in c for key in ("FLOW", "SP4", "PRESSURE")):
            # ▼ 결과/사유 남기기
            self.last_result = "fail"
            self.last_reason = f"MFC fail: {cmd} ({reason})"
            self._log("PC", "Gas/Pressure 안정화 실패 → 공정 중단 요청")
            with contextlib.suppress(Exception):
                self._show_state("Gas stabilize failed → STOP")
            self._stop_evt.set()

    # ─────────────────────────────────────────────────────────────
    # 내부 실행 시퀀스
    # ─────────────────────────────────────────────────────────────
    async def _run(self, p: PCParams) -> None:
        self.last_result = "success"
        self.last_reason = ""

        self._stop_evt = asyncio.Event()  # ★ 매 실행마다 초기화
        self.is_running = True
        self._show_state("Preparing…")           # ★ 시작 즉시 상태창에 표시
        self._log("PC", "플라즈마 클리닝 시작")

        # ★ LOG: 파라미터 스냅샷
        self._log(
            "STEP",
            (f"PARAMS gas_idx={p.gas_idx}, flow={p.gas_flow_sccm:.1f} sccm, "
            f"IG_target={p.target_pressure:.3e} Torr, SP4={p.sp4_setpoint_mTorr:.2f} mTorr, "
            f"RF={p.rf_power_w:.1f} W, time={p.process_time_min:.1f} min")
        )

        # 시작 카드 (컨트롤러가 직접)
        if self.chat:
            self.chat.notify_process_started({
                "process_note":  "Plasma Cleaning",
                "process_time":  float(p.process_time_min),
                "use_rf_power":  True,
                "rf_power":      float(p.rf_power_w),
            })

        try:
            # 1) PLC - 게이트밸브 인터락 확인 → OPEN_SW → 5초 후 OPEN_LAMP TRUE?
            self._log("PLC", "GV 인터락 확인")
            ok = await self._plc_check_gv_interlock()
            self._log("PLC", f"GV 인터락={ok}")  # ★ LOG
            if not ok:
                raise RuntimeError("게이트밸브 인터락 FALSE")

            self._log("PLC", "GV OPEN_SW 실행")
            await self._plc_gv_open()
            await asyncio.sleep(5.0)  # 램프 확인 지연
            lamp = await self._plc_read_gv_open_lamp()
            self._log("PLC", f"GV OPEN_LAMP={lamp}")  # ★ LOG (result)
            if not lamp:
                raise RuntimeError("GV OPEN_LAMP가 TRUE가 아님(오픈 실패?)")

            # 2) IG 목표(보다 낮음) 대기 — IG 내부 API만 사용(SIG 1 포함)
            if not self._ig_wait_for_base_torr:
                raise RuntimeError("IG API(ig_wait_for_base_torr)가 바인딩되지 않았습니다.")

            self._log("IG", f"IG.wait_for_base_pressure: {p.target_pressure:.3e} Torr 대기 (RDI=10s 간격, 외부 폴링 없음)")
            self._show_state("IG base wait…")       # ★ 상태창: IG 대기 시작

            wait_task = asyncio.create_task(
                self._ig_wait_for_base_torr(p.target_pressure, interval_ms=10_000),
                name="IGBaseWait",
            )

            stop_task = asyncio.create_task(self._stop_evt.wait(), name="PCStopWait")

            done, pending = await asyncio.wait(
                {wait_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            if stop_task in done and self._stop_evt.is_set():
                # STOP 우선: IG 대기 태스크를 정리하고 중단
                for t in pending:
                    t.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await wait_task
                raise asyncio.CancelledError()
            else:
                # IG 대기가 먼저 끝난 경우: 남은 stop_task 정리
                for t in pending:
                    t.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await stop_task

            try:
                ok = await wait_task
            except asyncio.CancelledError:
                self._log("IG", "IG base-wait이 장치 내부 사유로 취소됨 → 실패로 간주")
                ok = False

            if not ok:
                raise RuntimeError("Base pressure not reached (IG API)")
            self._show_state("Base pressure OK")    # ★ 상태창: IG 통과

            # 3) MFC 가스 설정 (Gas #3 N2) + 4) SP4 세팅/ON
            self._log("STEP", "3: Gas/Pressure 설정 시작")  # ★ LOG

            try:
                self._show_state(f"Gas select ch{p.gas_idx}")   # ★ 추가
                await self._mfc_gas_select(p.gas_idx)
                self._log("MFC", f"GAS SELECT ch={p.gas_idx} OK")  # ★ LOG
                if self._stop_evt.is_set():
                    raise asyncio.CancelledError()
            except Exception as e:
                self._log("MFC", f"GAS SELECT 실패: {e!r}")  # ★ LOG
                raise

            if p.gas_flow_sccm > 0.0:
                self._log("MFC", f"FLOW_SET_ON start ch={p.gas_idx} -> {p.gas_flow_sccm:.1f} sccm")  # ★ LOG
                try:
                    await self._mfc_flow_set_on(p.gas_flow_sccm)
                    self._log("MFC", "FLOW_SET_ON OK")  # ★ LOG
                    self._show_state(f"Gas Flow {p.gas_flow_sccm:.1f} sccm")   # ★ 추가
                    if self._stop_evt.is_set():
                        raise asyncio.CancelledError()
                except Exception as e:
                    self._log("MFC", f"FLOW_SET_ON 실패: {e!r}")  # ★ LOG
                    raise
            else:
                self._log("MFC", "FLOW_SET_ON 스킵(flow=0.0)")  # ★ LOG

            self._log("MFC", f"SP4_SET -> {p.sp4_setpoint_mTorr:.2f} mTorr")
            try:
                await self._mfc_sp4_set(p.sp4_setpoint_mTorr)
                self._log("MFC", "SP4_SET OK")  # ★ LOG
                self._show_state(f"SP4 Set {p.sp4_setpoint_mTorr:.2f} mTorr")   # ★ 추가
                if self._stop_evt.is_set():
                    raise asyncio.CancelledError()
            except Exception as e:
                self._log("MFC", f"SP4_SET 실패: {e!r}")  # ★ LOG
                raise

            self._log("MFC", "SP4_ON")
            try:
                await self._mfc_sp4_on()
                self._log("MFC", "SP4_ON OK")  # ★ LOG
                if self._stop_evt.is_set():
                    raise asyncio.CancelledError()
            except Exception as e:
                self._log("MFC", f"SP4_ON 실패: {e!r}")  # ★ LOG
                raise

            self._log("MFC", "[SOAK] SP4_ON → 60s 대기 시작")
            self._show_state("SP4 Soak")  # ★ 제목은 1회 고정
            for left in range(60, 0, -1):
                if self._stop_evt.is_set():
                    self._log("STEP", "STOP during SP4 soak → abort before RF")
                    raise asyncio.CancelledError()
                self._show_countdown(left)  # ★ 숫자만 갱신
                await asyncio.sleep(1.0)
            self._log("MFC", "[SOAK] 60s 완료")

            # 6) RF POWER(PLC DAC) 설정
            self._log("RF", f"RF Power 적용: {p.rf_power_w:.1f} W")
            self._show_state(f"RF Start {p.rf_power_w:.1f} W")   # ★ 추가
            try:
                await self._rf_start(p.rf_power_w)
                self._log("RF", "RF START OK")  # ★ LOG
            except Exception as e:
                self._log("RF", f"RF START 실패: {e!r}")  # ★ LOG
                raise

            # 7) PROCESS TIME 카운트다운
            self._show_state(f"PROCESS {p.process_time_min:.1f} min")   # ★ 추가
            total_sec = int(max(0, p.process_time_min * 60.0))
            for left in range(total_sec, -1, -1):
                if self._stop_evt.is_set():
                    self._log("STEP", f"STOP 이벤트 감지 → 남은 {left}s 시점에서 종료 진입")
                    self.last_result = "stop"
                    self.last_reason = "사용자 STOP"
                    raise asyncio.CancelledError()  # finally로 넘어가도록 명시 중단
                self._show_countdown(left)
                await asyncio.sleep(1.0)


        except asyncio.CancelledError:
            self.last_result = "stop"
            self.last_reason = "사용자 STOP"
            # STOP 이벤트로 중단이면 실패로 기록
            if self._stop_evt.is_set():
                self.last_result = "fail"
                if not getattr(self, "last_reason", ""):
                    self.last_reason = "Stopped by MFC/pressure failure"
            self._log("PC", "CancelledError: 외부 STOP 또는 경쟁 종료로 중단")
        except Exception as e:
            # ★ LOG: 예외 스택 트레이스까지 남김
            self.last_result = "fail"
            self.last_reason = f"{type(e).__name__}: {e!s}"
            self._log("PC", f"오류: {e!r}\n{traceback.format_exc()}")
        finally:
            self._log("STEP", "종료 시퀀스 진입")  # ★ LOG

            # 종료 시퀀스
            try:
                # 1) RF POWER OFF
                self._log("STEP", "종료: RF STOP 실행")  # ★ LOG
                await self._rf_stop()
            except Exception as e:
                self._log("RF", f"OFF 실패: {e!r}")

            try:
                # 2) Pressure 제어 OFF + Gas OFF
                await self._mfc_sp4_off()     # (밸브 OPEN: pressure MFC에서 진공 방출/안전 정리)
                await self._mfc_flow_off()    # (gas 유량 OFF)
            except Exception as e:
                self._log("MFC", f"종료 동작 실패: {e!r}")

            try:
                # 4) GV 인터락 TRUE면 CLOSE_SW → 5초 후 OPEN_LAMP FALSE?
                self._log("PLC", "GV 인터락 재확인 후 CLOSE_SW")
                ok = await self._plc_check_gv_interlock()
                self._log("PLC", f"종료시 인터락={ok}")
                if ok:
                    await self._plc_gv_close()
                    await asyncio.sleep(5.0)
                    lamp = await self._plc_read_gv_open_lamp()
                    if lamp:
                        self._log("PLC", "CLOSE 후에도 OPEN_LAMP=TRUE → GV CLOSE 실패 처리")
                        if self.last_result == "success":
                            self.last_result = "fail"
                            self.last_reason = "GV CLOSE 실패(OPEN_LAMP TRUE)"
                else:
                    self._log("PLC", "인터락 FALSE → CLOSE_SW 스킵")
                    if self.last_result == "success":
                        self.last_result = "fail"
                        self.last_reason = "GV 인터락 FALSE(닫힘 확인 불가)"

            except Exception as e:
                self._log("PLC", f"GV 종료 실패: {e!r}")

            self.is_running = False
            self._show_state("IDLE")
            self._log("PC", "플라즈마 클리닝 종료")

            # 최종 카드 (컨트롤러가 직접, 단 1회)
            if self.chat and not self._final_notified:
                ok = (self.last_result == "success")
                payload = {"process_name": "Plasma Cleaning"}
                if not ok:
                    payload["reason"] = (
                        self.last_reason or
                        ("사용자 STOP" if self.last_result == "stop" else "원인 미상")
                    )
                    # ★ 추가: fail이어도 STOP이면 뱃지 부여
                    if (self.last_result == "stop") or ("사용자 STOP" in (self.last_reason or "")):
                        payload["stopped"] = True
                        
                self.chat.notify_process_finished_detail(ok, payload)
                self._final_notified = True


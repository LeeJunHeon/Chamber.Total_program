# device/rf_power_async.py
# -*- coding: utf-8 -*-
"""
rf_power.py — asyncio 기반 RF Power 컨트롤러

핵심:
  - Qt 의존 제거(시그널/타이머 없음). asyncio 태스크로 폴링/램프다운/보정
  - start_process/cleanup는 await 기반 API
  - 측정 피드백(update_measurements; forward/reflected)을 외부(UI/브리지)가 전달
  - Ref.p(reflected) 과다 시 '대기 상태'로 전환하고, 최대 대기시간 초과 시 실패 처리
  - 유지 구간에서 '연속 N회 오차'로 노이즈/시리얼 스팸 억제
  - 전송은 콜백(AsyncFaduino.set_rf_power / set_rf_power_unverified) 주입
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Callable, Awaitable, AsyncGenerator, Literal, Any
import asyncio
import time

from lib import config_common as _cfg_common  # ✅ "모듈"로 import (값 고정 방지)

# ========= 이벤트 모델 =========
EventKind = Literal[
    "status",
    "display",              # forward/reflected 표시
    "state_changed",
    "target_reached",
    "target_failed",
    "power_off_finished",
]

@dataclass
class RFPowerEvent:
    kind: EventKind
    message: Optional[str] = None           # status/failed
    running: Optional[bool] = None          # state_changed
    forward: Optional[float] = None         # display
    reflected: Optional[float] = None       # display


class RFPowerAsync:
    def __init__(
        self,
        *,
        send_rf_power: Callable[[float], Awaitable[None]],
        send_rf_power_unverified: Callable[[float], Awaitable[None]],
        request_status_read: Optional[Callable[[], Awaitable[object]]] = None,
        toggle_enable: Optional[Callable[[bool], Awaitable[None]]] = None,
        poll_interval_ms: int = 1000,
        rampdown_interval_ms: int = 50,
        initial_step_w: float = 1.0,
        reflected_threshold_w: float = 20.0,
        reflected_wait_timeout_s: float = 60.0,
        maintain_need_consecutive: int = 2,
        direct_mode: bool = False,
        write_inv_a: float = 1.0,
        write_inv_b: float = 0.0,

        # ✅ 추가: 채널 cfg 주입(없으면 config_common 사용)
        cfg: Any | None = None,
    ):
        # ✅ cfg 모듈 보관 (config_ch2 같은 채널 모듈을 넣으면 거기 값 우선)
        self._cfg_mod = cfg if cfg is not None else _cfg_common

        # 주입 콜백
        self._send_rf_power_cb = send_rf_power
        self._send_rf_power_unverified_cb = send_rf_power_unverified
        self._request_status_read = request_status_read
        self._toggle_enable = toggle_enable
        self._enabled = False

        # ----------------------------
        # init 파라미터를 "기본값"으로 일단 세팅 (config가 있으면 reload에서 덮어씀)
        # ----------------------------
        self.debug_print = bool(getattr(self._cfg_mod, "DEBUG_PRINT", getattr(_cfg_common, "DEBUG_PRINT", False)))

        self._poll_interval_ms = int(poll_interval_ms)
        self._rampdown_interval_ms = int(rampdown_interval_ms)  # (현재 로직상 결국 poll과 맞출 예정)
        self._initial_step_w = float(initial_step_w)
        self._ref_th_w = float(reflected_threshold_w)
        self._ref_wait_to_s = float(reflected_wait_timeout_s)
        self._maintain_need_consecutive = int(maintain_need_consecutive)

        # ✅ RF 제어 파라미터 런타임 캐시(기본값은 config_common에서 읽고, 없으면 하드 폴백)
        self._rf_max_power = float(getattr(self._cfg_mod, "RF_MAX_POWER", getattr(_cfg_common, "RF_MAX_POWER", 600)))
        self._rf_ramp_step = float(getattr(self._cfg_mod, "RF_RAMP_STEP", getattr(_cfg_common, "RF_RAMP_STEP", 1.0)))
        self._rf_maintain_step = float(getattr(self._cfg_mod, "RF_MAINTAIN_STEP", getattr(_cfg_common, "RF_MAINTAIN_STEP", 0.1)))
        self._rf_tolerance_power = float(getattr(self._cfg_mod, "RF_TOLERANCE_POWER", getattr(_cfg_common, "RF_TOLERANCE_POWER", 1.0)))

        self._rf_low_power_thresh_w = float(getattr(self._cfg_mod, "RF_LOW_POWER_THRESH_W", getattr(_cfg_common, "RF_LOW_POWER_THRESH_W", 1.0)))
        self._rf_low_power_count_max_n = int(getattr(self._cfg_mod, "RF_LOW_POWER_COUNT_MAX_N", getattr(_cfg_common, "RF_LOW_POWER_COUNT_MAX_N", 3)))

        # ★ kick+ramp 파라미터 (direct_mode 전용, reload_runtime_cfg에서 덮어씀)
        self._rf_ramp_kick_threshold_w: float = 100.0
        self._rf_ramp_initial_kick_w: float = 50.0
        self._rf_ramp_down_step: float = 1.0
        self._rf_ramp_shutdown_cut_w: float = 50.0
        self._rf_ramp_fine_up_step: float = 3.0

        # ★ Blind ramp-up 파라미터 (FWD가 임계값 도달 전까지 REF.p 감시 OFF)
        #   - 0 또는 target_power 이하이면 기능 OFF (기존 동작과 동일)
        self._rf_blind_ramp_fwd_threshold_w: float = 70.0   # 이 FWD까지는 REF 무시하고 ramp-up
        self._rf_blind_ramp_settle_s: float        = 60.0  # 임계값 도달 후 안정화 대기(s)
        self._ref_check_armed: bool                = False  # REF.p 감시 ON 여부
        self._blind_reach_ts: Optional[float]      = None   # FWD 임계값 도달 시점

        # 상태/측정/목표
        self.state = "IDLE"
        self.previous_state = "IDLE"
        self._is_running = False
        self._is_ramping_down = False
        self._ref_wait_start_ts: Optional[float] = None

        self.target_power = 0.0
        self.current_power_step = 0.0
        self.forward_w = 0.0
        self.reflected_w = 0.0

        self._last_sent_w: Optional[float] = None
        self._rampdown_w: float = 0.0

        self._maintain_count = 0
        self._low_power_n: int = 0

        self._poll_task: Optional[asyncio.Task] = None
        self._rampdown_task: Optional[asyncio.Task] = None
        self._adjust_task: Optional[asyncio.Task] = None
        self._event_q: asyncio.Queue[RFPowerEvent] = asyncio.Queue(maxsize=512)

        self._power_off_evt = asyncio.Event()
        self._polling_enabled = True

        self._init_direct_mode = bool(direct_mode)
        self._direct_mode = bool(direct_mode)
        self._w_inv_a = float(write_inv_a)
        self._w_inv_b = float(write_inv_b)

        # ✅ 마지막에 config 재로딩(=UI apply 대비)
        self.reload_runtime_cfg()

    def reload_runtime_cfg(self) -> None:
        """
        UI에서 config 값을 바꾼 뒤, 이 메서드를 호출하면 즉시 반영되도록.
        - cfg(채널) → 없으면 config_common 폴백
        - 기존 RF 공정 로직(start/cleanup/poll/adjust)은 건드리지 않고
        '다음 동작부터 써야 하는 캐시성 값'만 다시 읽는다.
        """
        mod = self._cfg_mod if self._cfg_mod is not None else _cfg_common

        # 공통 디버그
        self.debug_print = bool(
            getattr(mod, "DEBUG_PRINT", getattr(_cfg_common, "DEBUG_PRINT", self.debug_print))
        )

        # RF 핵심 제어 파라미터
        self._rf_max_power = float(
            getattr(mod, "RF_MAX_POWER", getattr(_cfg_common, "RF_MAX_POWER", self._rf_max_power))
        )
        self._rf_ramp_step = float(
            getattr(mod, "RF_RAMP_STEP", getattr(_cfg_common, "RF_RAMP_STEP", self._rf_ramp_step))
        )
        self._rf_maintain_step = float(
            getattr(mod, "RF_MAINTAIN_STEP", getattr(_cfg_common, "RF_MAINTAIN_STEP", self._rf_maintain_step))
        )
        self._rf_tolerance_power = float(
            getattr(mod, "RF_TOLERANCE_POWER", getattr(_cfg_common, "RF_TOLERANCE_POWER", self._rf_tolerance_power))
        )

        self._rf_low_power_thresh_w = float(
            getattr(mod, "RF_LOW_POWER_THRESH_W", getattr(_cfg_common, "RF_LOW_POWER_THRESH_W", self._rf_low_power_thresh_w))
        )
        self._rf_low_power_count_max_n = int(
            getattr(mod, "RF_LOW_POWER_COUNT_MAX_N", getattr(_cfg_common, "RF_LOW_POWER_COUNT_MAX_N", self._rf_low_power_count_max_n))
        )

        # ✅ 추가 1) reflected 관련 기준도 reload 반영
        self._ref_th_w = float(
            getattr(mod, "RF_REFLECTED_THRESHOLD_W", getattr(_cfg_common, "RF_REFLECTED_THRESHOLD_W", self._ref_th_w))
        )
        self._ref_wait_to_s = float(
            getattr(mod, "RF_REFLECTED_WAIT_TIMEOUT_S", getattr(_cfg_common, "RF_REFLECTED_WAIT_TIMEOUT_S", self._ref_wait_to_s))
        )

        # ✅ 추가 1-2) Blind ramp-up 파라미터도 reload 반영
        self._rf_blind_ramp_fwd_threshold_w = float(
            getattr(mod, "RF_BLIND_RAMP_FWD_THRESHOLD_W", getattr(_cfg_common, "RF_BLIND_RAMP_FWD_THRESHOLD_W", self._rf_blind_ramp_fwd_threshold_w))
        )
        self._rf_blind_ramp_settle_s = float(
            getattr(mod, "RF_BLIND_RAMP_SETTLE_S", getattr(_cfg_common, "RF_BLIND_RAMP_SETTLE_S", self._rf_blind_ramp_settle_s))
        )

        # ✅ 추가 2) chamber runtime에서 생성자에 넣어주던 RF 연속파 운전값도 reload 반영
        self._poll_interval_ms = int(
            getattr(mod, "CHAMBER_RF_CONT_POLL_INTERVAL_MS", getattr(_cfg_common, "CHAMBER_RF_CONT_POLL_INTERVAL_MS", self._poll_interval_ms))
        )
        self._rampdown_interval_ms = int(
            getattr(mod, "CHAMBER_RF_CONT_RAMPDOWN_INTERVAL_MS", getattr(_cfg_common, "CHAMBER_RF_CONT_RAMPDOWN_INTERVAL_MS", self._rampdown_interval_ms))
        )
        if self._init_direct_mode:
            self._direct_mode = bool(
                getattr(mod, "PC_RF_DIRECT_MODE", getattr(_cfg_common, "PC_RF_DIRECT_MODE", self._direct_mode))
            )
        else:
            self._direct_mode = bool(
                getattr(mod, "CHAMBER_RF_CONT_DIRECT_MODE", getattr(_cfg_common, "CHAMBER_RF_CONT_DIRECT_MODE", self._direct_mode))
            )
        if self._init_direct_mode:
            self._w_inv_a = float(
                getattr(mod, "PC_RF_WRITE_INV_A", getattr(_cfg_common, "PC_RF_WRITE_INV_A", self._w_inv_a))
            )
            self._w_inv_b = float(
                getattr(mod, "PC_RF_WRITE_INV_B", getattr(_cfg_common, "PC_RF_WRITE_INV_B", self._w_inv_b))
            )
            # ★ kick+ramp 파라미터 읽기
            self._rf_ramp_kick_threshold_w = float(
                getattr(mod, "PC_RF_RAMP_KICK_THRESHOLD_W", getattr(_cfg_common, "PC_RF_RAMP_KICK_THRESHOLD_W", self._rf_ramp_kick_threshold_w))
            )
            self._rf_ramp_initial_kick_w = float(
                getattr(mod, "PC_RF_RAMP_INITIAL_KICK_W", getattr(_cfg_common, "PC_RF_RAMP_INITIAL_KICK_W", self._rf_ramp_initial_kick_w))
            )
            self._rf_ramp_step = float(
                getattr(mod, "PC_RF_RAMP_STEP", getattr(_cfg_common, "PC_RF_RAMP_STEP", self._rf_ramp_step))
            )
            self._rf_ramp_down_step = float(
                getattr(mod, "PC_RF_RAMP_DOWN_STEP", getattr(_cfg_common, "PC_RF_RAMP_DOWN_STEP", self._rf_ramp_down_step))
            )
            self._rf_ramp_shutdown_cut_w = float(
                getattr(mod, "PC_RF_RAMP_SHUTDOWN_CUT_W", getattr(_cfg_common, "PC_RF_RAMP_SHUTDOWN_CUT_W", self._rf_ramp_shutdown_cut_w))
            )
            self._rf_ramp_fine_up_step = float(
                getattr(mod, "PC_RF_RAMP_FINE_UP_STEP", getattr(_cfg_common, "PC_RF_RAMP_FINE_UP_STEP", self._rf_ramp_fine_up_step))
            )
        else:
            self._w_inv_a = float(
                getattr(mod, "CHAMBER_RF_CONT_WRITE_INV_A", getattr(_cfg_common, "CHAMBER_RF_CONT_WRITE_INV_A", self._w_inv_a))
            )
            self._w_inv_b = float(
                getattr(mod, "CHAMBER_RF_CONT_WRITE_INV_B", getattr(_cfg_common, "CHAMBER_RF_CONT_WRITE_INV_B", self._w_inv_b))
            )

    @property
    def reflected_threshold_w(self) -> float:
        return float(self._ref_th_w)

    @property
    def reflected_wait_timeout_s(self) -> float:
        return float(self._ref_wait_to_s)

    # ======= 이벤트 스트림 =======
    async def events(self) -> AsyncGenerator[RFPowerEvent, None]:
        while True:
            ev = await self._event_q.get()
            yield ev

    # ======= 공용 API =======
    async def start_process(self, target_power: float):
        if self._is_running:
            await self._emit_status("경고: RF 파워가 이미 동작 중입니다.")
            return

        self.target_power = float(max(0.0, min(self._rf_max_power, target_power)))
        self.current_power_step = float(self._initial_step_w)

        # ★ 새 런 시작 시 '첫 WRITE 보장'을 위해 중복 억제 캐시 초기화
        self._last_sent_w = None
        self._low_power_n = 0       # ★ 저출력 카운터 리셋

        # ★ Blind ramp-up 상태 초기화
        self._blind_reach_ts = None
        # Plasma Cleaning(direct_mode=kick+ramp)에는 blind ramp 적용 안 함 → 즉시 REF 감시 ON
        if getattr(self, "_direct_mode", False):
            self._ref_check_armed = True
        # 임계값 ≤ 0 이거나 target ≤ 임계값이면 의미가 없으므로 즉시 REF 감시 ON
        if (float(self._rf_blind_ramp_fwd_threshold_w) <= 0.0
            or float(self.target_power) <= float(self._rf_blind_ramp_fwd_threshold_w)):
            self._ref_check_armed = True
        else:
            self._ref_check_armed = False

        # ▼ RF 사용 전 SET 래치 ON (DCV_SET_1 = True)
        if self._toggle_enable:
            try:
                await self._toggle_enable(True)
                self._enabled = True
                await self._emit_status("RF SET ON")
            except Exception as e:
                await self._emit_status(f"RF SET ON 실패: {e!r}")
                return
        else:
            await self._emit_status("RF SET ON 생략(toggle_enable 미주입)")

        # ========= ★ direct_mode 분기 추가 (여기서 반환) =========
        if getattr(self, "_direct_mode", False):
            self._is_running = True
            await self._emit_state_changed(True)
            # ▶ 유지가 아니라 램프업으로 시작해야 도달 이벤트가 발생합니다.
            self.state = "RAMPING_UP"

            # ★ target > kick_threshold(100W) → kick+ramp 방식
            if float(self.target_power) > float(self._rf_ramp_kick_threshold_w):
                kick_w = min(float(self._rf_ramp_initial_kick_w), float(self.target_power))
                await self._emit_status(
                    f"Kick+Ramp 시작: {kick_w:.1f}W kick → {self.target_power:.1f}W까지 "
                    f"↑{self._rf_ramp_step:.1f}W/s, ↓{self._rf_ramp_down_step:.1f}W/s"
                )
                try:
                    await self._send_rf_power(kick_w)
                    self.current_power_step = kick_w
                    await self._emit_status(f"Kick {kick_w:.1f}W 전송 완료 — ramp 시작 대기")
                except Exception as e:
                    await self._emit_status(f"Kick 전송 실패: {e!r}")
                    return
            else:
                # target ≤ 100W → 기존 방식(목표값 직접 전송)
                await self._emit_status(f"Direct set: {self.target_power:.1f} W")
                try:
                    await self._send_rf_power(float(self.target_power))
                    self.current_power_step = float(self.target_power)
                    self._last_sent_w = float(self.target_power)
                    await self._emit_status(f"Direct set {self.target_power:.1f}W 전송 — 도달 판정 대기")
                except Exception as e:
                    await self._emit_status(f"Direct set 실패: {e!r}")
                    return

            # 폴링 활성화/재시작 (kick+ramp / direct 공통)
            self._polling_enabled = True
            if self._request_status_read is not None:
                if self._poll_task and not self._poll_task.done():
                    self._poll_task.cancel()
                    try:
                        await asyncio.wait_for(self._poll_task, timeout=1.0)
                    except Exception:
                        pass
                    self._poll_task = None
                self._poll_task = asyncio.create_task(self._poll_loop(), name="RF_Poll")
            else:
                await self._emit_status("상태읽기 콜백 없음 → 측정 없이 진행")

            return

        # ========= 기존 램프업 경로(그대로 유지) =========

        self._is_running = True
        await self._emit_state_changed(True)
        self.state = "RAMPING_UP"
        await self._emit_status(f"프로세스 시작. 목표: {self.target_power:.1f} W")

        # ★ 폴링 강제 활성화(이전에 False로 내려갔어도 시작 시 True로 복구)
        self._polling_enabled = True

        # 폴링 태스크 (선택) — 기존 태스크 있으면 정리 후 재시작 보장
        if self._request_status_read is not None:
            if self._poll_task and not self._poll_task.done():
                self._poll_task.cancel()
                try:
                    await asyncio.wait_for(self._poll_task, timeout=1.0)
                except Exception:
                    pass
                self._poll_task = None
            self._poll_task = asyncio.create_task(self._poll_loop(), name="RF_Poll")
        else:
            await self._emit_status("상태읽기 콜백 없음 → 측정 없이 진행")

        # ★ 첫 전송 kick: 초기 스텝을 즉시 1회 전송 (측정루프 시작 전에도 WRITE 보장)
        try:
            await self._send_rf_power(float(self.current_power_step))
            await self._emit_status(
                f"Ramp-Up 시작: 초기 {self.current_power_step:.1f}W 전송"
            )
        except Exception as e:
            await self._emit_status(f"초기 스텝 전송 실패: {e!r}")

    def set_process_status(self, active: bool) -> None:
        self._polling_enabled = bool(active)
        # 러닝중이고 request_status_read가 있을 때만 토글
        if not self._is_running or self._request_status_read is None:
            return
        if active:
            if self._poll_task is None or self._poll_task.done():
                self._poll_task = asyncio.create_task(self._poll_loop(), name="RF_Poll")
        else:
            if self._poll_task:
                self._poll_task.cancel()
                self._poll_task = None

    async def cleanup(self):
        if self._is_ramping_down and not self._is_running:
            return

        await self._emit_status("정지 신호 수신됨.")
        self._is_running = False
        await self._emit_state_changed(False)

        # ========= ★ direct_mode 분기 =========
        if getattr(self, "_direct_mode", False):
            self._power_off_evt.clear()

            # ★ kick+ramp로 켰던 경우 → 3W/s ramp-down 후 50W에서 즉시 OFF
            if float(self.target_power) > float(self._rf_ramp_kick_threshold_w):
                self.state = "IDLE"
                await self._cancel_task("_poll_task")
                await self._cancel_task("_adjust_task")
                await self._emit_status(
                    f"Kick+Ramp shutdown: {self._rf_ramp_step:.1f}W/s 하강 → "
                    f"{self._rf_ramp_shutdown_cut_w:.1f}W 도달 시 즉시 OFF"
                )
                self._is_ramping_down = True
                self._rampdown_w = self._last_sent_w if self._last_sent_w is not None else float(self.target_power)
                self._rampdown_task = asyncio.create_task(self._rampdown_loop_kick(), name="RF_RampDown_Kick")
                return

            # target ≤ 100W → 기존 방식: 즉시 OFF
            try:
                await self._set_rf_unverified(0.0)
                self._last_sent_w = 0.0
            finally:
                if self._toggle_enable and self._enabled:
                    try:
                        await self._toggle_enable(False)
                        await self._emit_status("RF SET OFF")
                    finally:
                        self._enabled = False

            self.state = "IDLE"
            self._ev_nowait(RFPowerEvent(kind="power_off_finished"))
            self._power_off_evt.set()
            return
        # ========= 기존 램프다운 경로(그대로 유지) =========

        self.state = "IDLE"

        # 폴링/보정 태스크 중지
        await self._cancel_task("_poll_task")
        await self._cancel_task("_adjust_task")

        # 램프다운 시작
        self._power_off_evt.clear()  # ★ 추가: 이번 종료 사이클의 완료 신호 초기화
        await self._emit_status("RF 파워 ramp-down 시작")
        self._is_ramping_down = True
        self._rampdown_w = self._last_sent_w if self._last_sent_w is not None else float(self.current_power_step)
        self._rampdown_task = asyncio.create_task(self._rampdown_loop(), name="RF_RampDown")

    # ======= 외부(브리지/UI)에서 전달하는 측정값 =======
    def update_measurements(self, forward_w: float, reflected_w: float):
        if not self._is_running:
            return

        self.forward_w = float(forward_w or 0.0)
        self.reflected_w = float(reflected_w or 0.0)

        # 디스플레이 이벤트 즉시 방출
        self._ev_nowait(RFPowerEvent(kind="display", forward=self.forward_w, reflected=self.reflected_w))
        
        # 1) Ref.p 과다 감시 (blind ramp 단계에서는 OFF)
        if self._ref_check_armed:
            if self.reflected_w > self._ref_th_w:
                if self.state != "REF_P_WAITING":
                    self.previous_state = self.state
                    self.state = "REF_P_WAITING"
                    self._ref_wait_start_ts = time.monotonic()
                    self._ev_nowait(RFPowerEvent(kind="status",
                                                 message=f"Ref.p({self.reflected_w:.1f}W) 안정화 대기 시작 (최대 {int(self._ref_wait_to_s)}초)"))
                else:
                    if (time.monotonic() - (self._ref_wait_start_ts or 0.0)) > self._ref_wait_to_s:
                        # 실패 처리
                        self._ev_nowait(RFPowerEvent(kind="status", message="Ref.p 안정화 시간 초과. 즉시 중단합니다."))
                        msg = (
                            f"Ref.p(REF) 안정화 시간({int(self._ref_wait_to_s)}s) 초과: "
                            f"REF={self.reflected_w:.1f}W > TH={self._ref_th_w:.1f}W"
                        )
                        self._ev_nowait(RFPowerEvent(kind="target_failed", message=msg))
                        asyncio.create_task(self.cleanup())
                return
            else:
                if self.state == "REF_P_WAITING":
                    self._ev_nowait(RFPowerEvent(kind="status",
                                                 message=f"Ref.p 안정화 완료({self.reflected_w:.1f}W). 공정 재개"))
                    self.state = self.previous_state
                    self._ref_wait_start_ts = None

        # 2) 저출력(forward power 너무 낮음) 감시
        #    - target_power > 0 인 런에서만 체크
        if self.target_power > 0.0:
            if self.forward_w <= self._rf_low_power_thresh_w:
                # 연속 저출력 카운트 증가
                self._low_power_n += 1
                self._ev_nowait(RFPowerEvent(
                    kind="status",
                    message=(
                        f"저출력 감지: Forward={self.forward_w:.1f}W "
                        f"({self._low_power_n}/{self._rf_low_power_count_max_n})"
                    ),
                ))

                if self._low_power_n >= self._rf_low_power_count_max_n:
                    # 3회(기본) 연속 저출력이면 실패 처리 + 정지
                    self._ev_nowait(RFPowerEvent(
                        kind="status",
                        message="Forward power가 너무 낮아 RF 공정을 중단합니다."
                    ))
                    self._ev_nowait(RFPowerEvent(
                        kind="target_failed",
                        message=(
                            f"Forward power <= {self._rf_low_power_thresh_w:.1f}W "
                            f"{self._low_power_n}회 연속"
                        ),
                    ))
                    asyncio.create_task(self.cleanup())
                    return
            else:
                # 정상 범위로 올라오면 카운터 리셋
                if self._low_power_n:
                    self._low_power_n = 0
        
        # 3) 램프업/유지 보정은 태스크로 비동기 실행(중복 호출 시 최신만 수행)
        if self._adjust_task and not self._adjust_task.done():
            self._adjust_task.cancel()
        self._adjust_task = asyncio.create_task(self._adjust_once(), name="RF_Adjust")

    # ======= 내부 루프/보정 =======
    def _ingest_status_result(self, res: object) -> None:
        """
        PLC의 power_read가 (P, V, I) 튜플을 리턴할 수 있으므로,
        튜플/리스트면 ref=0.0으로 고정해서 Ref.p 대기 오동작 방지.
        """
        try:
            fwd = ref = None
            if isinstance(res, (tuple, list)):
                if len(res) >= 1:
                    fwd = float(res[0])   # P
                ref = 0.0                 # V/I를 Ref.p로 간주하지 않음
            elif isinstance(res, dict):
                fwd = res.get("forward") or res.get("fwd") or res.get("power") or res.get("P")
                ref = res.get("reflected") or res.get("ref") or 0.0
                fwd = None if fwd is None else float(fwd)
                ref = None if ref is None else float(ref)
            if fwd is not None:
                self.update_measurements(fwd, float(ref or 0.0))
        except Exception:
            pass

    async def _poll_loop(self):
        try:
            while self._is_running and self._polling_enabled:
                try:
                    res = None
                    if self._request_status_read:
                        try:
                            # 폴링 주기(예: 1s)의 0.8배 + 최소 0.2s
                            to = max(0.2, (self._poll_interval_ms / 1000.0) * 0.8)
                            res = await asyncio.wait_for(self._request_status_read(), timeout=to)
                        except asyncio.TimeoutError:
                            await self._emit_status("상태 읽기 요청 timeout")
                        except Exception as e:
                            await self._emit_status(f"상태 읽기 요청 실패: {e}")
                    if res is not None:
                        self._ingest_status_result(res)
                except Exception as e:
                    await self._emit_status(f"상태 읽기 요청 실패: {e}")
                await asyncio.sleep(self._poll_interval_ms / 1000.0)
        except asyncio.CancelledError:
            pass

    async def _rampdown_loop(self):
        try:
            step_w = float(self._rf_ramp_step)
            while self._is_ramping_down:
                if self._rampdown_w <= 0.0:
                    # ★ 0W 전송 직전, 실제 전송값(보정 우회값)을 로그로 남김
                    scaled0 = self._xform_write(0.0)
                    await self._emit_status(f"Ramp-Down final: target=0.0W → write={scaled0:.3f}W")
                    await self._set_rf_unverified(0.0)

                    # ▼ RF 사용 종료 시 SET OFF (DCV_SET_1 = False)
                    if self._toggle_enable and self._enabled:
                        try:
                            await self._toggle_enable(False)
                            await self._emit_status("RF SET OFF")
                        finally:
                            self._enabled = False

                    await self._emit_status("RF 파워 ramp-down 완료")
                    self._is_ramping_down = False
                    self._ev_nowait(RFPowerEvent(kind="power_off_finished"))
                    self._power_off_evt.set()   # ★ 추가: 완료 신호 설정
                    return
                self._rampdown_w = max(0.0, self._rampdown_w - step_w)
                self._last_sent_w = self._rampdown_w
                # ★ 단계별 전송값도 상태 로그 남김
                scaled = self._xform_write(self._rampdown_w)
                await self._emit_status(
                    f"Ramp-Down step: target={self._rampdown_w:.1f}W → write={scaled:.3f}W"
                )
                await self._set_rf_unverified(self._rampdown_w)
                await asyncio.sleep(self._rampdown_interval_ms / 1000.0)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._emit_status(f"램프다운 오류: {e}")
        finally:
            # 혹시 위에서 return을 못타고 나온 예외 경로도 완료 신호 보증
            if not self._is_ramping_down:
                self._power_off_evt.set()

    async def _rampdown_loop_kick(self):
        """
        Kick+Ramp 종료 전용 루프.
        - _rf_ramp_step(3W)씩 1초 간격으로 하강
        - _rf_ramp_shutdown_cut_w(50W) 이하 도달 시 즉시 OFF
        """
        try:
            step_w = float(self._rf_ramp_step)
            cut_w  = float(self._rf_ramp_shutdown_cut_w)

            while self._is_ramping_down:
                if self._rampdown_w <= cut_w:
                    # 50W 이하 → 즉시 OFF
                    await self._emit_status(
                        f"Ramp-Down: {self._rampdown_w:.1f}W ≤ {cut_w:.1f}W → 즉시 OFF"
                    )
                    await self._set_rf_unverified(0.0)
                    self._last_sent_w = 0.0
                    self._ev_nowait(RFPowerEvent(kind="display", forward=0.0, reflected=0.0))
                    if self._toggle_enable and self._enabled:
                        try:
                            await self._toggle_enable(False)
                            await self._emit_status("RF SET OFF")
                        finally:
                            self._enabled = False
                    await self._emit_status("RF 파워 ramp-down 완료")
                    self._is_ramping_down = False
                    self._ev_nowait(RFPowerEvent(kind="power_off_finished"))
                    self._power_off_evt.set()
                    return

                self._rampdown_w = max(cut_w, self._rampdown_w - step_w)
                self._last_sent_w = self._rampdown_w
                await self._emit_status(f"Ramp-Down step: {self._rampdown_w:.1f}W")
                await self._set_rf_unverified(self._rampdown_w)
                await asyncio.sleep(1.0)  # 1초 간격 → 50W/s

        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._emit_status(f"Kick 램프다운 오류: {e}")
        finally:
            if self._is_ramping_down:
                self._is_ramping_down = False
                self._power_off_evt.set()

    async def _adjust_once(self):
        """
        목표 파워까지 램프업하고, 도달 후에는 유지 보정.
        - 장비 전송은 '와트(W)' 단위로 직접 보낸다고 가정(_send_rf_power 사용)
        """
        try:
            if not self._is_running or self.state == "REF_P_WAITING":
                return

            last_sent: Optional[float] = self._last_sent_w

            if self.state == "RAMPING_UP":
                # ★ Blind ramp 단계: FWD가 임계값 도달 → 안정화 대기 단계로 전환
                if (not self._ref_check_armed) and \
                   self.forward_w >= float(self._rf_blind_ramp_fwd_threshold_w):
                    self.state = "BLIND_SETTLE"
                    self._blind_reach_ts = time.monotonic()
                    await self._emit_status(
                        f"Blind ramp 완료: FWD={self.forward_w:.1f}W 도달 "
                        f"(임계 {self._rf_blind_ramp_fwd_threshold_w:.1f}W) → "
                        f"{int(self._rf_blind_ramp_settle_s)}초 안정화 대기 (REF.p 감시 OFF)"
                    )
                    return

                diff = float(self.target_power) - float(self.forward_w)
                send_needed = False

                # 허용 오차 내 → 유지 상태로 전환
                if abs(diff) <= float(self._rf_tolerance_power):
                    await self._emit_status(f"{self.target_power:.1f}W 도달. 파워 유지 시작")
                    self.state = "MAINTAINING"
                    # ⛔ 목표값 재전송하지 않음 — 직전에 forward를 만들어낸 setpoint를 그대로 유지
                    if self._last_sent_w is not None:
                        self.current_power_step = float(self._last_sent_w)
                    self._ev_nowait(RFPowerEvent(kind="target_reached"))
                    return

                # ▶ 스텝 계산 (상승/오버슈트 복귀)
                if diff > 0:
                    if self.current_power_step < float(self.target_power):
                        # setpoint가 target 미만 → 50W씩 빠르게 올림 (target 초과 방지)
                        new_power = min(
                            self.current_power_step + float(self._rf_ramp_step),
                            float(self.target_power),
                        )
                    else:
                        # setpoint가 이미 target인데 FWD 못 미침 → 3W fine-tuning
                        new_power = min(
                            self.current_power_step + float(self._rf_ramp_fine_up_step),
                            float(self._rf_max_power),
                        )
                else:
                    # ★ overshoot: _rf_ramp_down_step(1W/s)으로 하강
                    new_power = max(0.0, self.current_power_step - float(self._rf_ramp_down_step))
                    await self._emit_status(
                        f"목표 파워 초과. Ramp-Down 시도... "
                        f"(step→{new_power:.1f}W, FWD={self.forward_w:.1f}W)"
                    )

                # 범위 체크 + 실제 전송 여부 판단 (데드밴드 삭제, ε만 유지)
                new_power = max(0.0, min(float(self._rf_max_power), float(new_power)))

                if (last_sent is None) or (abs(new_power - last_sent) > 1e-6):
                    await self._send_rf_power(float(new_power))
                    send_needed = True

                self.current_power_step = float(new_power)

                # ★ 전송이 있었을 때만 램프업 로그 출력(중복/허수 로그 억제)
                if send_needed:
                    await self._emit_status(
                        f"Ramp-Up... 목표스텝:{self.current_power_step:.1f}W, 현재:{self.forward_w:.1f}W"
                    )

                return  # ★ 이번 호출은 램프업까지만. 유지 보정은 다음 측정 때.
            
            elif self.state == "BLIND_SETTLE":
                # FWD 임계값 도달 후 안정화 대기. setpoint는 그대로 두고 시간만 카운트.
                now = time.monotonic()
                elapsed = now - (self._blind_reach_ts or now)
                if elapsed >= float(self._rf_blind_ramp_settle_s):
                    self._ref_check_armed = True
                    self.state = "RAMPING_UP"
                    await self._emit_status(
                        f"안정화 대기 완료 ({elapsed:.1f}s 경과). REF.p 감시 ON → ramp-up 재개"
                    )
                # settle 중에는 _send_rf_power 호출 없음 (직전 setpoint 유지)
                return

            elif self.state == "MAINTAINING":
                error = float(self.target_power) - float(self.forward_w)

                # 허용 오차 내 → 보정 스킵
                if abs(error) <= float(self._rf_tolerance_power):
                    self._maintain_count = 0
                    return

                # 연속 N회 오차일 때만 보정
                self._maintain_count += 1
                if self._maintain_count < int(self._maintain_need_consecutive):
                    return
                self._maintain_count = 0

                step = float(self._rf_maintain_step) if error > 0 else -float(self._rf_maintain_step)

                # ✅ 누적 기준을 항상 current_power_step으로
                base = self.current_power_step
                new_power = max(0.0, min(float(self._rf_max_power), float(base) + step))

                # ✅ 다음 루프에서도 누적되도록 항상 갱신
                self.current_power_step = float(new_power)

                # ⛔ 데드밴드 삭제 — 동일값만 차단(ε)
                if (last_sent is None) or (abs(new_power - last_sent) > 1e-6):
                    await self._send_rf_power(float(new_power))
                    await self._emit_status(
                        f"유지 보정: meas={self.forward_w:.1f}W, target={self.target_power:.1f}W → set {new_power:.1f}W"
                    )

        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._emit_status(f"보정 루프 오류: {e}")

    # ======= 전송/보조 =======
    async def _send_rf_power(self, power_w: float):
        """
        장치에 W 단위로 전송(검증 응답 기대). 클램프/중복 억제 포함.
        """
        power_w = max(0.0, min(self._rf_max_power, float(power_w)))
        if self._last_sent_w is not None and abs(power_w - self._last_sent_w) < 1e-6:
            return
        
        # ▶ 전송 직전 보정(역변환) 적용
        scaled = self._xform_write(power_w)

        try:
            await self._send_rf_power_cb(scaled)  # ← 보정된 값으로 전송
            self._last_sent_w = power_w          # 내부 좌표계는 계속 ‘fwd W’
        except Exception as e:
            await self._emit_status(f"RF 설정 전송 실패(verified): {e}")

    async def _set_rf_unverified(self, power_w: float):
        """
        no-reply 전송 경로(램프다운 등). 실패는 status로만 보고.
        """
        power_w = max(0.0, min(self._rf_max_power, float(power_w)))
        scaled = self._xform_write(power_w)
        try:
            await self._send_rf_power_unverified_cb(scaled)  # ← 보정된 값으로 전송
        except Exception as e:
            await self._emit_status(f"RF 설정 전송 실패(unverified): {e}")

    # ======= 이벤트/유틸 =======
    def _xform_write(self, desired_forward_w: float) -> float:
        # ★ 0W는 보정 우회(절편 b 제거)해서 '진짜 0'을 쓰도록
        if desired_forward_w <= 0.01:
            return 0.0
        v = self._w_inv_a * float(desired_forward_w) + self._w_inv_b
        return max(0.0, min(float(self._rf_max_power), v))
    
    async def wait_power_off(self, timeout_s: float = 8.0) -> bool:
        try:
            await asyncio.wait_for(self._power_off_evt.wait(), timeout=timeout_s)
            return True
        except asyncio.TimeoutError:
            return False

    async def _emit_status(self, msg: str):
        if self.debug_print:
            print(f"[RFpower][status] {msg}")
        await self._event_q.put(RFPowerEvent(kind="status", message=msg))

    async def _emit_state_changed(self, running: bool):
        await self._event_q.put(RFPowerEvent(kind="state_changed", running=running))

    def _ev_nowait(self, ev: RFPowerEvent):
        try:
            self._event_q.put_nowait(ev)
        except Exception:
            pass

    async def _cancel_task(self, name: str):
        t: Optional[asyncio.Task] = getattr(self, name)
        if t:
            t.cancel()
            try:
                await asyncio.wait_for(t, timeout=1.0)
            except asyncio.TimeoutError:
                pass
            except Exception:
                pass
            setattr(self, name, None)

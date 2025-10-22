# device/rf_power_async.py
# -*- coding: utf-8 -*-
"""
rf_power.py — asyncio 기반 RF Power 컨트롤러

핵심:
  - Qt 의존 제거(시그널/타이머 없음). asyncio 태스크로 폴링/램프다운/보정
  - start_process/cleanup는 await 기반 API
  - 측정 피드백(update_measurements; forward/reflected)을 외부(UI/브리지)가 전달
  - 반사파(reflected) 과다 시 '대기 상태'로 전환하고, 최대 대기시간 초과 시 실패 처리
  - 유지 구간에서 '연속 N회 오차'와 '데드밴드'로 노이즈/시리얼 스팸 억제
  - 전송은 콜백(AsyncFaduino.set_rf_power / set_rf_power_unverified) 주입
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Callable, Awaitable, AsyncGenerator, Literal
import asyncio
import time

from lib.config_ch2 import (
    RF_MAX_POWER,
    RF_RAMP_STEP,
    RF_MAINTAIN_STEP,
    RF_TOLERANCE_POWER,
    DEBUG_PRINT,
)

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
        toggle_enable: Optional[Callable[[bool], Awaitable[None]]] = None,  # ← 추가 (DCV_SET_1 토글용)
        poll_interval_ms: int = 1000,
        rampdown_interval_ms: int = 50,
        initial_step_w: float = 6.0,
        reflected_threshold_w: float = 3.0,
        reflected_wait_timeout_s: float = 60.0,
        maintain_need_consecutive: int = 2,
        dac_deadband_counts: int = 2,   # 호환을 위해 남겨둠(>0이면 W 데드밴드로 사용)
    ):
        """
        send_rf_power:              검증 응답을 기대하는 전송 (예: AsyncFaduino.set_rf_power)
        send_rf_power_unverified:   no-reply 전송 (예: AsyncFaduino.set_rf_power_unverified)
        request_status_read:        (선택) 주기적 상태 읽기 트리거(예: AsyncFaduino.force_rf_read)
        """
        # 주입 콜백(필드명에 _cb를 붙여 메서드와 충돌 방지)
        self._send_rf_power_cb = send_rf_power
        self._send_rf_power_unverified_cb = send_rf_power_unverified
        self._request_status_read = request_status_read
        self._toggle_enable = toggle_enable           # ← 추가
        self._enabled = False                         # ← 추가 (SET 래치 상태 캐시)

        self.debug_print = DEBUG_PRINT

        # 파라미터
        self._poll_interval_ms = int(poll_interval_ms)
        self._rampdown_interval_ms = int(rampdown_interval_ms)
        self._initial_step_w = float(initial_step_w)
        self._ref_th_w = float(reflected_threshold_w)
        self._ref_wait_to_s = float(reflected_wait_timeout_s)
        self._maintain_need_consecutive = int(maintain_need_consecutive)
        
        # (after) 0도 허용. 음수면 0으로 클램프.
        self._deadband_w = max(0.0, float(dac_deadband_counts))

        # 상태
        self.state = "IDLE"  # "IDLE", "RAMPING_UP", "MAINTAINING", "REF_P_WAITING"
        self.previous_state = "IDLE"
        self._is_running = False
        self._is_ramping_down = False
        self._ref_wait_start_ts: Optional[float] = None

        # 측정/목표
        self.target_power = 0.0
        self.current_power_step = 0.0
        self.forward_w = 0.0
        self.reflected_w = 0.0

        # 전송 상태
        self._last_sent_w: Optional[float] = None
        self._rampdown_w: float = 0.0

        self._maintain_count = 0  # 유지 보정 시 연속 오차 카운터

        # 태스크/큐
        self._poll_task: Optional[asyncio.Task] = None
        self._rampdown_task: Optional[asyncio.Task] = None
        self._adjust_task: Optional[asyncio.Task] = None
        self._event_q: asyncio.Queue[RFPowerEvent] = asyncio.Queue(maxsize=512)

        self._polling_enabled = True

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

        self.target_power = float(max(0.0, min(RF_MAX_POWER, target_power)))
        self.current_power_step = float(self._initial_step_w)

        # ▼ RF 사용 전 SET 래치 ON (DCV_SET_1 = True)
        if self._toggle_enable:
            try:
                await self._toggle_enable(True)
                self._enabled = True
                await self._emit_status("RF SET ON")
            except Exception as e:
                await self._emit_status(f"RF SET ON 실패: {e!r}")
                return  # 실패 시 시작 중단 권장
        else:
            await self._emit_status("RF SET ON 생략(toggle_enable 미주입)")

        self._is_running = True
        await self._emit_state_changed(True)
        self.state = "RAMPING_UP"
        await self._emit_status(f"프로세스 시작. 목표: {self.target_power:.1f} W")

        # 폴링 태스크 (선택)
        if self._request_status_read is not None:
            self._poll_task = asyncio.create_task(self._poll_loop(), name="RF_Poll")

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

        self.state = "IDLE"

        # 폴링/보정 태스크 중지
        await self._cancel_task("_poll_task")
        await self._cancel_task("_adjust_task")

        # 램프다운 시작
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
        
        # 반사파 과다 → 대기/타임아웃
        if self.reflected_w > self._ref_th_w:
            if self.state != "REF_P_WAITING":
                self.previous_state = self.state
                self.state = "REF_P_WAITING"
                self._ref_wait_start_ts = time.monotonic()
                self._ev_nowait(RFPowerEvent(kind="status",
                                             message=f"반사파({self.reflected_w:.1f}W) 안정화 대기 시작 (최대 {int(self._ref_wait_to_s)}초)"))
            else:
                if (time.monotonic() - (self._ref_wait_start_ts or 0.0)) > self._ref_wait_to_s:
                    # 실패 처리
                    self._ev_nowait(RFPowerEvent(kind="status", message="반사파 안정화 시간 초과. 즉시 중단합니다."))
                    self._ev_nowait(RFPowerEvent(kind="target_failed", message="반사파 안정화 시간(60s) 초과"))
                    asyncio.create_task(self.cleanup())
            return
        else:
            if self.state == "REF_P_WAITING":
                self._ev_nowait(RFPowerEvent(kind="status",
                                             message=f"반사파 안정화 완료({self.reflected_w:.1f}W). 공정 재개"))
                self.state = self.previous_state
                self._ref_wait_start_ts = None
        
        # 램프업/유지 보정은 태스크로 비동기 실행(중복 호출 시 최신만 수행)
        if self._adjust_task and not self._adjust_task.done():
            self._adjust_task.cancel()
        self._adjust_task = asyncio.create_task(self._adjust_once(), name="RF_Adjust")

    # ======= 내부 루프/보정 =======
    def _ingest_status_result(self, res: object) -> None:
        """
        PLC의 power_read가 (P, V, I) 튜플을 리턴할 수 있으므로,
        튜플/리스트면 ref=0.0으로 고정해서 반사파 대기 오동작 방지.
        """
        try:
            fwd = ref = None
            if isinstance(res, (tuple, list)):
                if len(res) >= 1:
                    fwd = float(res[0])   # P
                ref = 0.0                 # V/I를 반사파로 간주하지 않음
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
                    res = await self._request_status_read() if self._request_status_read else None
                    if res is not None:
                        self._ingest_status_result(res)
                except Exception as e:
                    await self._emit_status(f"상태 읽기 요청 실패: {e}")
                await asyncio.sleep(self._poll_interval_ms / 1000.0)
        except asyncio.CancelledError:
            pass

    async def _rampdown_loop(self):
        try:
            step_w = float(RF_RAMP_STEP)
            while self._is_ramping_down:
                if self._rampdown_w <= 0.0:
                    await self._set_rf_unverified(0.0)
                    self._ev_nowait(RFPowerEvent(kind="display", forward=0.0, reflected=0.0))

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
                    return
                self._rampdown_w = max(0.0, self._rampdown_w - step_w)
                self._last_sent_w = self._rampdown_w
                await self._set_rf_unverified(self._rampdown_w)
                await asyncio.sleep(self._rampdown_interval_ms / 1000.0)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._emit_status(f"램프다운 오류: {e}")

    async def _adjust_once(self):
        """
        목표 파워까지 램프업하고, 도달 후에는 유지 보정.
        - 장비 전송은 '와트(W)' 단위로 직접 보낸다고 가정(_send_rf_power 사용)
        - 데드밴드는 self._deadband_w (W)
        """
        try:
            if not self._is_running or self.state == "REF_P_WAITING":
                return

            watt_deadband: float = float(self._deadband_w)
            last_sent: Optional[float] = self._last_sent_w

            if self.state == "RAMPING_UP":
                diff = float(self.target_power) - float(self.forward_w)
                send_needed = False

                # 허용 오차 내 → 유지 상태로 전환
                if abs(diff) <= float(RF_TOLERANCE_POWER):
                    await self._emit_status(f"{self.target_power:.1f}W 도달. 파워 유지 시작")
                    self.state = "MAINTAINING"
                    await self._send_rf_power(float(self.target_power))
                    self.current_power_step = float(self.target_power)
                    self._ev_nowait(RFPowerEvent(kind="target_reached"))
                    return

                # 스텝 계산 (상승/오버슈트 복귀)
                if diff > 0:
                    # ★ 타깃에 이미 붙었는데도 측정이 낮으면 → 타깃 초과 상향 보정 허용
                    if self.current_power_step >= float(self.target_power) - 1e-6:
                        base = last_sent if last_sent is not None else self.current_power_step
                        new_power = min(float(RF_MAX_POWER), float(base) + float(RF_MAINTAIN_STEP))
                    else:
                        new_power = min(self.current_power_step + float(RF_RAMP_STEP), float(self.target_power))
                else:
                    new_power = max(0.0, self.current_power_step - float(RF_MAINTAIN_STEP))
                    await self._emit_status("목표 파워 초과. 출력 하강 시도...")

                # 범위/데드밴드 체크 + 실제 전송 여부 판단
                new_power = max(0.0, min(float(RF_MAX_POWER), float(new_power)))

                # 데드밴드 0도 허용되므로, 차이가 '정말 0'이면 전송 생략되게 보정
                thr = max(watt_deadband, 1e-6)
                if (last_sent is None) or (abs(new_power - last_sent) >= thr):
                    await self._send_rf_power(float(new_power))
                    send_needed = True

                self.current_power_step = float(new_power)

                # ★ 전송이 있었을 때만 램프업 로그 출력(중복/허수 로그 억제)
                if send_needed:
                    await self._emit_status(
                        f"Ramp-Up... 목표스텝:{self.current_power_step:.1f}W, 현재:{self.forward_w:.1f}W"
                    )

            elif self.state == "MAINTAINING":
                error = float(self.target_power) - float(self.forward_w)

                # 허용 오차 내 → 보정 스킵
                if abs(error) <= float(RF_TOLERANCE_POWER):
                    self._maintain_count = 0
                    return

                # 연속 N회 오차일 때만 보정
                self._maintain_count += 1
                if self._maintain_count < int(self._maintain_need_consecutive):
                    return
                self._maintain_count = 0

                step = float(RF_MAINTAIN_STEP) if error > 0 else -float(RF_MAINTAIN_STEP)
                base = last_sent if last_sent is not None else self.current_power_step
                new_power = max(0.0, min(float(RF_MAX_POWER), float(base) + step))  # ← ✅ 추천

                if (last_sent is None) or (abs(new_power - last_sent) >= watt_deadband):
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
        power_w = max(0.0, min(RF_MAX_POWER, float(power_w)))
        if self._last_sent_w is not None and abs(power_w - self._last_sent_w) < 1e-6:
            return
        try:
            await self._send_rf_power_cb(power_w)
            self._last_sent_w = power_w
        except Exception as e:
            await self._emit_status(f"RF 설정 전송 실패(verified): {e}")

    async def _set_rf_unverified(self, power_w: float):
        """
        no-reply 전송 경로(램프다운 등). 실패는 status로만 보고.
        """
        power_w = max(0.0, min(RF_MAX_POWER, float(power_w)))
        try:
            await self._send_rf_power_unverified_cb(power_w)
        except Exception as e:
            await self._emit_status(f"RF 설정 전송 실패(unverified): {e}")

    # ======= 이벤트/유틸 =======
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
                await t
            except Exception:
                pass
            setattr(self, name, None)

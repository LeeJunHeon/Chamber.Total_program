# device/rf_power_async.py
# -*- coding: utf-8 -*-
"""
rf_power.py — asyncio 기반 RF Power 컨트롤러

핵심:
  - Qt 의존 제거(시그널/타이머 없음). asyncio 태스크로 폴링/램프다운/보정
  - start_process/stop_process는 await 기반 API
  - 측정 피드백(update_measurements; forward/reflected)을 외부(UI/브리지)가 전달
  - 반사파(reflected) 과다 시 '대기 상태'로 전환하고, 최대 대기시간 초과 시 실패 처리
  - 유지 구간에서 '연속 N회 오차'와 'DAC 데드밴드'로 노이즈/시리얼 스팸 억제
  - 전송은 콜백(AsyncFaduino.set_rf_power / set_rf_power_unverified 등) 주입

예시 연결:
    faduino = AsyncFaduino(...)
    rf = RFPowerAsync(
        send_rf_power=faduino.set_rf_power,
        send_rf_power_unverified=faduino.set_rf_power_unverified,
        request_status_read=faduino.force_rf_read,   # 선택: 내부 폴링 사용
        poll_interval_ms=1000
    )

    await rf.start_process(150.0)
    async for ev in rf.events():
        if ev.kind == "display":
            ui.update_rf(ev.forward, ev.reflected)
        elif ev.kind == "target_reached":
            process.next_step()
        elif ev.kind == "status":
            log.info(ev.message)
    ...
    await rf.stop_process()
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
    RF_PARAM_WATT_TO_DAC,
    RF_OFFSET_WATT_TO_DAC,
    DAC_FULL_SCALE,          # Faduino와 동일 스케일을 사용하는 것으로 가정
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
        send_rf_power: Callable[[int], Awaitable[None]],
        send_rf_power_unverified: Callable[[int], Awaitable[None]],
        request_status_read: Optional[Callable[[], Awaitable[None]]] = None,
        poll_interval_ms: int = 1000,
        rampdown_interval_ms: int = 50,
        initial_step_w: float = 6.0,
        reflected_threshold_w: float = 3.0,
        reflected_wait_timeout_s: float = 60.0,
        maintain_need_consecutive: int = 2,
        dac_deadband_counts: int = 2,
    ):
        """
        send_rf_power:              검증 응답을 기대하는 DAC 설정 (AsyncFaduino.set_rf_power)
        send_rf_power_unverified:   no-reply DAC 설정 (AsyncFaduino.set_rf_power_unverified)
        request_status_read:        (선택) 주기적 상태 읽기 트리거(예: AsyncFaduino.force_rf_read)
        poll_interval_ms:           상태 읽기 주기
        rampdown_interval_ms:       램프다운 스텝 주기
        initial_step_w:             램프업 시작 스텝(W)
        reflected_threshold_w:      반사파 대기 임계값(W)
        reflected_wait_timeout_s:   반사파 대기 최대 시간(s)
        maintain_need_consecutive:  유지구간 오차 연속 N회일 때만 보정
        dac_deadband_counts:        이 카운트 미만 변화는 전송 생략
        """
        self._send_rf_power = send_rf_power
        self._send_rf_power_unverified = send_rf_power_unverified
        self._request_status_read = request_status_read

        self.debug_print = DEBUG_PRINT

        # 파라미터
        self._poll_interval_ms = int(poll_interval_ms)
        self._rampdown_interval_ms = int(rampdown_interval_ms)
        self._initial_step_w = float(initial_step_w)
        self._ref_th_w = float(reflected_threshold_w)
        self._ref_wait_to_s = float(reflected_wait_timeout_s)
        self._maintain_need_consecutive = int(maintain_need_consecutive)
        self._dac_deadband = int(dac_deadband_counts)

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

        # DAC/보정 상태
        self._hold_dac = 0
        self._last_sent_dac: Optional[int] = None
        self._rampdown_dac = 0
        self._maintain_count = 0

        # 태스크/큐
        self._poll_task: Optional[asyncio.Task] = None
        self._rampdown_task: Optional[asyncio.Task] = None
        self._adjust_task: Optional[asyncio.Task] = None
        self._event_q: asyncio.Queue[RFPowerEvent] = asyncio.Queue(maxsize=512)

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

        self._is_running = True
        await self._emit_state_changed(True)
        self.state = "RAMPING_UP"
        await self._emit_status(f"프로세스 시작. 목표: {self.target_power:.1f} W")

        # 폴링 태스크 (선택)
        if self._request_status_read is not None:
            self._poll_task = asyncio.create_task(self._poll_loop(), name="RF_Poll")

    async def stop_process(self):
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
        self._rampdown_dac = self._last_sent_dac if self._last_sent_dac is not None else self._power_to_dac(self.current_power_step)
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
                    # stop_process는 await 이므로 태스크로 분리
                    asyncio.create_task(self.stop_process())
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

    # ======= 내부 루프 =======
    async def _poll_loop(self):
        try:
            while self._is_running:
                try:
                    await (self._request_status_read() if self._request_status_read else asyncio.sleep(0))
                except Exception as e:
                    await self._emit_status(f"상태 읽기 요청 실패: {e}")
                await asyncio.sleep(self._poll_interval_ms / 1000.0)
        except asyncio.CancelledError:
            pass

    async def _rampdown_loop(self):
        try:
            # Watt 스텝을 DAC 스텝으로 환산(최소 1카운트)
            dac_step = max(1, int(round(RF_PARAM_WATT_TO_DAC * float(RF_RAMP_STEP))))
            while self._is_ramping_down:
                if self._rampdown_dac <= 0:
                    await self._set_rf_unverified(0)
                    self._ev_nowait(RFPowerEvent(kind="display", forward=0.0, reflected=0.0))
                    await self._emit_status("RF 파워 ramp-down 완료")
                    self._is_ramping_down = False
                    self._ev_nowait(RFPowerEvent(kind="power_off_finished"))
                    return

                self._rampdown_dac = max(0, self._rampdown_dac - dac_step)
                self._last_sent_dac = self._rampdown_dac
                await self._set_rf_unverified(self._rampdown_dac)
                await asyncio.sleep(self._rampdown_interval_ms / 1000.0)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._emit_status(f"램프다운 오류: {e}")

    async def _adjust_once(self):
        try:
            if self.state == "RAMPING_UP":
                diff = self.target_power - self.forward_w
                if abs(diff) <= float(RF_TOLERANCE_POWER):
                    await self._emit_status(f"{self.target_power:.1f}W 도달. 파워 유지 시작")
                    self._hold_dac = self._last_sent_dac if self._last_sent_dac is not None else self._power_to_dac(self.target_power)
                    self.state = "MAINTAINING"
                    self._ev_nowait(RFPowerEvent(kind="target_reached"))
                    return

                if diff > 0:
                    # 목표보다 낮음 → 상승
                    self.current_power_step = min(self.current_power_step + float(RF_RAMP_STEP), self.target_power)
                else:
                    # 목표보다 높음 → 완만히 하강(오버슈트 복귀)
                    self.current_power_step = max(0.0, self.current_power_step - float(RF_MAINTAIN_STEP))
                    await self._emit_status("목표 파워 초과. 출력 하강 시도...")

                new_dac = self._power_to_dac(self.current_power_step)
                await self._maybe_send_dac(new_dac)
                await self._emit_status(
                    f"Ramp-Up... 목표스텝:{self.current_power_step:.1f}W, 현재:{self.forward_w:.1f}W (DAC:{new_dac})"
                )

            elif self.state == "MAINTAINING":
                error = self.target_power - self.forward_w
                if abs(error) <= float(RF_TOLERANCE_POWER):
                    self._maintain_count = 0
                    return

                self._maintain_count += 1
                if self._maintain_count < self._maintain_need_consecutive:
                    return
                self._maintain_count = 0

                step_w = float(RF_MAINTAIN_STEP) if error > 0 else -float(RF_MAINTAIN_STEP)
                new_dac = self._power_to_dac(self.target_power + step_w)

                if (self._last_sent_dac is None) or (abs(new_dac - self._last_sent_dac) >= self._dac_deadband):
                    self._hold_dac = new_dac
                    await self._maybe_send_dac(new_dac)
                    await self._emit_status(
                        f"유지 보정: meas={self.forward_w:.1f}W, target={self.target_power:.1f}W → DAC {new_dac}"
                    )
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._emit_status(f"보정 루프 오류: {e}")

    # ======= 전송/보조 =======
    async def _maybe_send_dac(self, dac: int):
        if self._last_sent_dac is not None and dac == self._last_sent_dac:
            return
        self._last_sent_dac = dac
        await self._set_rf_verified(dac)

    async def _set_rf_verified(self, dac: int):
        try:
            await self._send_rf_power(int(self._clamp_dac(dac)))
        except Exception as e:
            await self._emit_status(f"RF 설정 전송 실패(verified): {e}")

    async def _set_rf_unverified(self, dac: int):
        try:
            await self._send_rf_power_unverified(int(self._clamp_dac(dac)))
        except Exception as e:
            await self._emit_status(f"RF 설정 전송 실패(unverified): {e}")

    def _power_to_dac(self, power_watt: float) -> int:
        if power_watt <= 0:
            return 0
        dac_f = (float(RF_PARAM_WATT_TO_DAC) * float(power_watt)) + float(RF_OFFSET_WATT_TO_DAC)
        return self._clamp_dac(int(round(dac_f)))

    def _clamp_dac(self, v: int) -> int:
        if v < 0:
            return 0
        if v > int(DAC_FULL_SCALE):
            return int(DAC_FULL_SCALE)
        return v

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

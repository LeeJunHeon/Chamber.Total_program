# device/dc_power_async.py
# -*- coding: utf-8 -*-
"""
dc_power.py — asyncio 기반 DC Power 컨트롤러

핵심:
  - Qt 의존 제거(시그널/타이머 없음). asyncio 태스크/슬립 사용
  - start_process/stop_process는 await 기반 API
  - 측정 피드백(update_measurements)을 외부(UI 또는 브리지)가 호출해주면
    내부 로직이 DAC를 미세 조정(램프업/유지)하여 목표 파워를 맞춤
  - (선택) request_status_read 콜백을 주면 내부 주기(DC_INTERVAL_MS)로 읽기 요청 태스크가 동작
  - 상태/로그/디스플레이/완료 이벤트는 async 제너레이터 events()로 방출

사용:
  faduino = AsyncFaduino(...)
  dc = DCPowerAsync(
          send_dc_power=faduino.set_dc_power,
          send_dc_power_unverified=faduino.set_dc_power_unverified,
          request_status_read=faduino.force_dc_read   # 또는 faduino.force_status_read
      )
  await dc.start_process(120.0)
  async for ev in dc.events(): ...  # UI 갱신/로깅/다음 단계 트리거 등
  ...
  await dc.stop_process()

주의:
  - Faduino 측 폴링을 이미 켜두었다면(request_status_read가 없어도) UI/브리지에서
    Faduino 이벤트(kind='dc_power')를 수신할 때마다 dc.update_measurements(...)를 호출해주면 된다.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Callable, Awaitable, AsyncGenerator, Literal
import asyncio

from lib.config_ch2 import (
    DC_MAX_POWER,
    DC_TOLERANCE_POWER,
    DC_RAMP_STEP,
    DC_MAINTAIN_STEP,
    DC_INTERVAL_MS,
    DC_PARAM_WATT_TO_DAC,
    DC_OFFSET_WATT_TO_DAC,
    DAC_FULL_SCALE,
    DEBUG_PRINT,
)

# ========= 이벤트 모델 =========
EventKind = Literal["status", "display", "state_changed", "target_reached", "power_off_finished"]

@dataclass
class DCPowerEvent:
    kind: EventKind
    message: Optional[str] = None                 # status
    power: Optional[float] = None                 # display
    voltage: Optional[float] = None               # display
    current: Optional[float] = None               # display
    running: Optional[bool] = None                # state_changed


class DCPowerAsync:
    def __init__(
        self,
        *,
        send_dc_power: Callable[[int], Awaitable[None]],
        send_dc_power_unverified: Callable[[int], Awaitable[None]],
        request_status_read: Optional[Callable[[], Awaitable[None]]] = None,
    ):
        """
        send_dc_power:              검증 응답을 기대하는 DAC 설정 (AsyncFaduino.set_dc_power)
        send_dc_power_unverified:   no-reply DAC 설정 (AsyncFaduino.set_dc_power_unverified)
        request_status_read:        (선택) 주기적인 상태 읽기 트리거 (e.g., AsyncFaduino.force_dc_read)
        """
        self._send_dc_power = send_dc_power
        self._send_dc_power_unverified = send_dc_power_unverified
        self._request_status_read = request_status_read

        self.debug_print = DEBUG_PRINT

        # 상태
        self.state = "IDLE"  # "IDLE", "RAMPING_UP", "MAINTAINING"
        self._is_running = False

        self.target_power = 0.0
        self.current_power = 0.0
        self.current_voltage = 0.0
        self.current_current = 0.0
        self.current_dac_value = 0

        # 태스크/큐
        self._control_task: Optional[asyncio.Task] = None
        self._event_q: asyncio.Queue[DCPowerEvent] = asyncio.Queue(maxsize=256)

        # 내부 직전 보정 태스크(중복 방지용)
        self._pending_adjust_task: Optional[asyncio.Task] = None

    # ======= 퍼블릭 이벤트 스트림 =======
    async def events(self) -> AsyncGenerator[DCPowerEvent, None]:
        while True:
            ev = await self._event_q.get()
            yield ev

    # ======= 공용 API =======
    async def start_process(self, target_power: float):
        """목표 파워 설정 + 램프업 시작."""
        if self._is_running:
            await self._emit_status("경고: DC 파워가 이미 동작 중입니다.")
            return

        self.target_power = min(max(float(target_power), 0.0), float(DC_MAX_POWER))
        await self._emit_status(f"프로세스 시작. 목표 파워 {self.target_power:.2f} W로 설정 및 보정 시작.")

        # 초기 DAC 설정(보정식)
        self.current_dac_value = self._power_to_dac(self.target_power)
        await self._set_dc_verified(self.current_dac_value)

        # 상태 전환
        self._is_running = True
        await self._emit_state_changed(True)
        self.state = "RAMPING_UP"

        # 주기 읽기 태스크(선택)
        if self._request_status_read is not None:
            self._control_task = asyncio.create_task(self._control_loop(), name="DCPowerControl")

    async def cleanup(self):
        """제어 중지 및 출력 0."""
        # 루프 중지
        await self._cancel_task("_control_task")

        # 상태 리셋
        self._is_running = False
        await self._emit_state_changed(False)
        self.state = "IDLE"

        # 즉시 OFF (no-reply 허용)
        self.current_dac_value = 0
        await self._set_dc_unverified(0)

        await self._emit_status("출력 OFF. 대기 상태로 전환합니다.")
        await self._event_q.put(DCPowerEvent(kind="power_off_finished"))

    # ======= 외부(브리지/UI)에서 호출하는 측정 피드백 엔트리 =======
    def update_measurements(self, power: float, voltage: float, current: float):
        """
        Faduino 측 DC 측정 이벤트를 받을 때 호출.
        (UI/브리지에서 Faduino 이벤트를 구독하고, kind='dc_power'일 때 이 메서드에 전달.)
        """
        if not self._is_running:
            return

        # 최신 값 반영
        self.current_power = float(power or 0.0)
        self.current_voltage = float(voltage or 0.0)
        self.current_current = float(current or 0.0)

        # 디스플레이 이벤트 즉시 방출(동기)
        self._ev_nowait(DCPowerEvent(
            kind="display",
            power=self.current_power,
            voltage=self.current_voltage,
            current=self.current_current,
        ))

        # 보정 로직은 비동기로 실행(중복 호출 시 이전 조정 태스크 취소)
        if self._pending_adjust_task and not self._pending_adjust_task.done():
            self._pending_adjust_task.cancel()
        self._pending_adjust_task = asyncio.create_task(self._adjust_loop_once())

    # ======= 내부 로직 =======
    async def _control_loop(self):
        """(선택) 주기적으로 상태 읽기 요청을 보내는 루프."""
        try:
            while self._is_running:
                try:
                    await (self._request_status_read() if self._request_status_read else asyncio.sleep(0))
                except Exception as e:
                    await self._emit_status(f"상태 읽기 요청 실패: {e}")
                await asyncio.sleep(DC_INTERVAL_MS / 1000.0)
        except asyncio.CancelledError:
            pass

    async def _adjust_loop_once(self):
        """현재 측정값 기준으로 단일 스텝 보정."""
        try:
            diff = self.target_power - self.current_power

            # RAMPING_UP 단계
            if self.state == "RAMPING_UP":
                if abs(diff) <= float(DC_TOLERANCE_POWER):
                    await self._emit_status(f"목표 파워 {self.target_power:.2f} W 도달. 파워 유지를 시작합니다.")
                    self.state = "MAINTAINING"
                    await self._event_q.put(DCPowerEvent(kind="target_reached"))
                    return

                step = int(DC_RAMP_STEP) if diff > 0 else -int(DC_RAMP_STEP)
                new_dac = self._clamp_dac(self.current_dac_value + step)
                if new_dac != self.current_dac_value:
                    self.current_dac_value = new_dac
                    await self._set_dc_verified(self.current_dac_value)
                    await self._emit_status(f"파워 조정... Target DAC: {self.current_dac_value}")

            # MAINTAINING 단계
            elif self.state == "MAINTAINING":
                if abs(diff) > float(DC_TOLERANCE_POWER):
                    step = int(DC_MAINTAIN_STEP) if diff > 0 else -int(DC_MAINTAIN_STEP)
                    new_dac = self._clamp_dac(self.current_dac_value + step)
                    if new_dac != self.current_dac_value:
                        self.current_dac_value = new_dac
                        await self._set_dc_verified(self.current_dac_value)
                        await self._emit_status(
                            f"파워 유지 보정... Power: {self.current_power:.2f}W, DAC: {self.current_dac_value}"
                        )
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._emit_status(f"보정 루프 오류: {e}")

    # ======= 변환/보조 =======
    def _power_to_dac(self, power_watt: float) -> int:
        if power_watt <= 0:
            return 0
        dac_f = (float(DC_PARAM_WATT_TO_DAC) * float(power_watt)) + float(DC_OFFSET_WATT_TO_DAC)
        return self._clamp_dac(int(round(dac_f)))

    def _clamp_dac(self, v: int) -> int:
        if v < 0:
            return 0
        if v > int(DAC_FULL_SCALE):
            return int(DAC_FULL_SCALE)
        return v

    # ======= 실제 송신 =======
    async def _set_dc_verified(self, dac: int):
        try:
            await self._send_dc_power(int(dac))
        except Exception as e:
            await self._emit_status(f"DC 설정 전송 실패(verified): {e}")

    async def _set_dc_unverified(self, dac: int):
        try:
            await self._send_dc_power_unverified(int(dac))
        except Exception as e:
            await self._emit_status(f"DC 설정 전송 실패(unverified): {e}")

    # ======= 유틸 =======
    async def _emit_status(self, msg: str):
        if self.debug_print:
            print(f"[DCpower][status] {msg}")
        await self._event_q.put(DCPowerEvent(kind="status", message=msg))

    async def _emit_state_changed(self, running: bool):
        await self._event_q.put(DCPowerEvent(kind="state_changed", running=running))

    def _ev_nowait(self, ev: DCPowerEvent):
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

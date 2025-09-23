# device/dc_power_async.py
# -*- coding: utf-8 -*-
"""
dc_power_async.py — asyncio 기반 DC Power 컨트롤러 (W 단위 직접 전송)

핵심:
  - Qt 의존 제거(시그널/타이머 없음). asyncio 태스크로 폴링/램프다운/보정
  - start_process/cleanup는 await 기반 API
  - 측정 피드백(update_measurements; power/voltage/current)을 외부(UI/브리지)가 전달하거나,
    request_status_read 콜백이 값을 반환하면 내부에서 곧장 섭취(_ingest_status_result)
  - 목표 도달 후 유지 구간에서 연속 오차/데드밴드로 스팸 억제
  - 전송은 콜백(PLC 등)의 W 단위 API를 직접 호출(send_dc_power / send_dc_power_unverified)
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
    DEBUG_PRINT,
)

# ========= 이벤트 모델 =========
EventKind = Literal[
    "status",
    "display",          # power/voltage/current 표시
    "state_changed",
    "target_reached",
    "power_off_finished",
]

@dataclass
class DCPowerEvent:
    kind: EventKind
    message: Optional[str] = None
    power: Optional[float] = None
    voltage: Optional[float] = None
    current: Optional[float] = None
    running: Optional[bool] = None


class DCPowerAsync:
    def __init__(
        self,
        *,
        send_dc_power: Callable[[float], Awaitable[None]],
        send_dc_power_unverified: Callable[[float], Awaitable[None]],
        request_status_read: Optional[Callable[[], Awaitable[object]]] = None,
        rampdown_interval_ms: int = 50,
        initial_step_w: float = 5.0,
        watt_deadband: float = 0.5,
    ):
        """
        send_dc_power:              검증 응답을 기대하는 W 단위 설정 (예: PLC.power_apply(..., family="DCV"))
        send_dc_power_unverified:   no-reply W 단위 설정 (예: PLC.power_write(..., family="DCV"))
        request_status_read:        (선택) 주기적 상태 읽기 트리거. (반환값이 있으면 (P,V,I)로 간주하여 섭취)
        rampdown_interval_ms:       램프다운 스텝 주기
        initial_step_w:             램프업 시작 스텝(W)
        watt_deadband:              연속 전송 억제 데드밴드(W)
        """
        self._send_dc_power = send_dc_power
        self._send_dc_power_unverified = send_dc_power_unverified
        self._request_status_read = request_status_read

        self.debug_print = DEBUG_PRINT

        # 파라미터
        self._rampdown_interval_ms = int(rampdown_interval_ms)
        self._initial_step_w = float(initial_step_w)
        self._watt_deadband = float(watt_deadband)

        # 상태
        self.state = "IDLE"  # "IDLE", "RAMPING_UP", "MAINTAINING"
        self._is_running = False
        self._polling_enabled = True

        # 측정/목표
        self.target_power = 0.0
        self.current_power_step = 0.0
        self.power_w = 0.0
        self.voltage_v = 0.0
        self.current_a = 0.0

        # 전송 상태
        self._last_sent_power: Optional[float] = None
        self._rampdown_w: float = 0.0

        # 태스크/큐
        self._control_task: Optional[asyncio.Task] = None
        self._rampdown_task: Optional[asyncio.Task] = None
        self._adjust_task: Optional[asyncio.Task] = None
        self._event_q: asyncio.Queue[DCPowerEvent] = asyncio.Queue(maxsize=256)

    # ======= 퍼블릭 이벤트 스트림 =======
    async def events(self) -> AsyncGenerator[DCPowerEvent, None]:
        while True:
            ev = await self._event_q.get()
            yield ev

    # ======= 공용 API =======
    async def start_process(self, target_power: float):
        """목표 파워 설정 + 램프업 시작(W 기준)."""
        if self._is_running:
            await self._emit_status("경고: DC 파워가 이미 동작 중입니다.")
            return

        self.target_power = float(max(0.0, min(DC_MAX_POWER, target_power)))
        self.current_power_step = float(self._initial_step_w)

        self._is_running = True
        await self._emit_state_changed(True)
        self.state = "RAMPING_UP"
        await self._emit_status(f"프로세스 시작. 목표: {self.target_power:.1f} W")

        # 폴링 태스크 (선택)
        if self._request_status_read is not None:
            self._control_task = asyncio.create_task(self._control_loop(), name="DC_Poll")

    def set_process_status(self, active: bool) -> None:
        """외부에서 폴링 on/off(연결은 유지)."""
        self._polling_enabled = bool(active)
        if not self._is_running or self._request_status_read is None:
            return
        if active:
            if self._control_task is None or self._control_task.done():
                self._control_task = asyncio.create_task(self._control_loop(), name="DC_Poll")
        else:
            if self._control_task:
                self._control_task.cancel()
                self._control_task = None

    async def cleanup(self):
        """제어 중지 및 램프다운→OFF."""
        # 폴링/보정 태스크 중지
        await self._cancel_task("_control_task")
        await self._cancel_task("_adjust_task")

        # 상태 리셋(표시상 IDLE로 먼저 전환)
        self._is_running = False
        await self._emit_state_changed(False)
        self.state = "IDLE"

        # 램프다운 시작
        await self._emit_status("DC 파워 ramp-down 시작")
        self._rampdown_w = self._last_sent_power if self._last_sent_power is not None else float(self.current_power_step)
        self._rampdown_task = asyncio.create_task(self._rampdown_loop(), name="DC_RampDown")

    # ======= 외부(브리지/UI)에서 전달하는 측정값 =======
    def update_measurements(self, power: float, voltage: float, current: float):
        if not self._is_running:
            return

        self.power_w = float(power or 0.0)
        self.voltage_v = float(voltage or 0.0)
        self.current_a = float(current or 0.0)

        # 디스플레이 이벤트 즉시 방출
        self._ev_nowait(DCPowerEvent(kind="display", power=self.power_w, voltage=self.voltage_v, current=self.current_a))

        # 램프업/유지 보정은 태스크로 비동기 실행(중복 호출 시 최신만 수행)
        if self._adjust_task and not self._adjust_task.done():
            self._adjust_task.cancel()
        self._adjust_task = asyncio.create_task(self._adjust_once(), name="DC_Adjust")

    # ======= 내부 루프 =======
    def _ingest_status_result(self, res: object) -> None:
        """request_status_read()의 반환값을 (P,V,I)로 파싱해서 update_measurements 호출."""
        try:
            p = v = c = None
            if isinstance(res, (tuple, list)):
                if len(res) >= 1: p = float(res[0])
                if len(res) >= 2: v = float(res[1])
                if len(res) >= 3: c = float(res[2])
            elif isinstance(res, dict):
                # 다양한 키 폴백
                p = res.get("power")   or res.get("P")
                v = res.get("voltage") or res.get("V")
                c = res.get("current") or res.get("I")
                p = None if p is None else float(p)
                v = None if v is None else float(v)
                c = None if c is None else float(c)
            if p is not None:
                self.update_measurements(p, float(v or 0.0), float(c or 0.0))
        except Exception:
            pass

    async def _control_loop(self):
        """(선택) 주기적으로 상태 읽기 요청을 보내는 루프 + 결과 섭취."""
        try:
            while self._is_running and self._polling_enabled:
                try:
                    res = await self._request_status_read() if self._request_status_read else None
                    if res is not None:
                        self._ingest_status_result(res)
                except Exception as e:
                    await self._emit_status(f"상태 읽기 요청 실패: {e}")
                await asyncio.sleep(DC_INTERVAL_MS / 1000.0)
        except asyncio.CancelledError:
            pass

    async def _rampdown_loop(self):
        try:
            step_w = float(DC_RAMP_STEP)
            while True:
                if self._rampdown_w <= 0.0:
                    await self._set_dc_unverified(0.0)
                    self._ev_nowait(DCPowerEvent(kind="display", power=0.0, voltage=0.0, current=0.0))
                    await self._emit_status("DC 파워 ramp-down 완료")
                    self._ev_nowait(DCPowerEvent(kind="power_off_finished"))
                    return
                self._rampdown_w = max(0.0, self._rampdown_w - step_w)
                self._last_sent_power = self._rampdown_w
                await self._set_dc_unverified(self._rampdown_w)
                await asyncio.sleep(self._rampdown_interval_ms / 1000.0)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._emit_status(f"램프다운 오류: {e}")

    async def _adjust_once(self):
        """목표 파워까지 램프업하고, 도달 후에는 유지 보정(W 기준)."""
        try:
            if not self._is_running:
                return

            last_sent: Optional[float] = getattr(self, "_last_sent_power", None)
            deadband = float(self._watt_deadband)

            if self.state == "RAMPING_UP":
                diff = float(self.target_power) - float(self.power_w)

                # 허용 오차 내 → 유지 상태로 전환
                if abs(diff) <= float(DC_TOLERANCE_POWER):
                    await self._emit_status(f"{self.target_power:.1f}W 도달. 파워 유지 시작")
                    self.state = "MAINTAINING"
                    # 목표값으로 한 번 더 고정 전송(안전)
                    try:
                        await self._send_dc_power(float(self.target_power))
                        self._last_sent_power = float(self.target_power)
                    except Exception as e:
                        await self._emit_status(f"DC 설정 전송 실패: {e}")
                    self._ev_nowait(DCPowerEvent(kind="target_reached"))
                    return

                # 스텝 계산 (상승/과슈트 복귀)
                if diff > 0:
                    new_power = min(self.current_power_step + float(DC_RAMP_STEP), float(self.target_power))
                else:
                    new_power = max(0.0, self.current_power_step - float(DC_MAINTAIN_STEP))
                    await self._emit_status("목표 파워 초과. 출력 하강 시도...")

                # 범위 클램프
                new_power = max(0.0, min(float(DC_MAX_POWER), float(new_power)))

                # 데드밴드 적용 후 전송
                if (last_sent is None) or (abs(new_power - last_sent) >= deadband):
                    try:
                        await self._send_dc_power(float(new_power))
                        self._last_sent_power = float(new_power)
                    except Exception as e:
                        await self._emit_status(f"DC 설정 전송 실패: {e}")

                self.current_power_step = float(new_power)
                await self._emit_status(
                    f"Ramp-Up... 목표스텝:{self.current_power_step:.1f}W, 현재:{self.power_w:.1f}W"
                )

            elif self.state == "MAINTAINING":
                error = float(self.target_power) - float(self.power_w)

                # 허용 오차 내 → 보정 스킵
                if abs(error) <= float(DC_TOLERANCE_POWER):
                    return

                step = float(DC_MAINTAIN_STEP) if error > 0 else -float(DC_MAINTAIN_STEP)
                new_power = max(0.0, min(float(DC_MAX_POWER), float(self.target_power) + step))

                if (last_sent is None) or (abs(new_power - last_sent) >= deadband):
                    try:
                        await self._send_dc_power(float(new_power))
                        self._last_sent_power = float(new_power)
                    except Exception as e:
                        await self._emit_status(f"DC 설정 전송 실패: {e}")
                    await self._emit_status(
                        f"유지 보정: meas={self.power_w:.1f}W, target={self.target_power:.1f}W → set {new_power:.1f}W"
                    )

        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._emit_status(f"보정 루프 오류: {e}")

    # ======= 실제 송신 =======
    async def _set_dc_verified(self, power_w: float):
        try:
            await self._send_dc_power(float(power_w))
            self._last_sent_power = float(power_w)
        except Exception as e:
            await self._emit_status(f"DC 설정 전송 실패(verified): {e}")

    async def _set_dc_unverified(self, power_w: float):
        try:
            await self._send_dc_power_unverified(float(power_w))
            self._last_sent_power = float(power_w)
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

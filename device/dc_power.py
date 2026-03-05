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

import asyncio
from dataclasses import dataclass
from typing import Optional, Callable, Awaitable, AsyncGenerator, Literal, Any

from lib import config_common as cfgc  # ✅ "모듈"로 import (값 고정 방지)

# ========= 이벤트 모델 =========
EventKind = Literal[
    "status",
    "display",          # power/voltage/current 표시
    "state_changed",
    "target_reached",
    "target_failed",    # ★ 추가
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
        toggle_enable: Optional[Callable[[bool], Awaitable[None]]] = None,  # ← 추가
        watt_deadband: float = 0.5,

        # ✅ 추가: 채널 cfg 주입(없으면 config_common 사용)
        cfg: Any | None = None,
    ):
        self._send_dc_power = send_dc_power
        self._send_dc_power_unverified = send_dc_power_unverified
        self._request_status_read = request_status_read
        self._toggle_enable = toggle_enable

        # ✅ cfg 모듈 보관 (config_ch1/config_ch2 모듈을 넣으면 거기 값 우선)
        self._cfg = cfg

        # ---- 런타임 캐시(초기값) ----
        self.debug_print = False

        self._dc_max_power = 1000.0
        self._dc_tolerance_power = 1.0
        self._dc_maintain_step = 1.0
        self._dc_interval_ms = 5000

        self._dc_low_w_thresh = 1.0
        self._dc_low_streak_n = 3
        self._dc_low_current_thresh_a = 0.05
        self._dc_low_current_streak_n = 3

        # 파라미터
        self._watt_deadband = float(watt_deadband)

        # 상태
        self.state = "IDLE"  # "IDLE", "RAMPING_UP", "MAINTAINING"
        self._is_running = False
        self._polling_enabled = True

        # 측정/목표
        self.target_power = 0.0
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
        self._sent_target_reached = False

        self._enabled = False  # SET 래치

        self._low_power_streak = 0
        self._low_current_streak = 0  # 저전류 감시용 카운터

        # ✅ 마지막에 config 재로딩(=UI apply 대비)
        self.reload_runtime_cfg()

    def reload_runtime_cfg(self) -> None:
        mod = self._cfg if self._cfg is not None else cfgc

        # debug
        self.debug_print = bool(getattr(mod, "DEBUG_PRINT", getattr(cfgc, "DEBUG_PRINT", self.debug_print)))

        # 핵심 제어 파라미터
        self._dc_max_power = float(getattr(mod, "DC_MAX_POWER", getattr(cfgc, "DC_MAX_POWER", self._dc_max_power)))
        self._dc_tolerance_power = float(getattr(mod, "DC_TOLERANCE_POWER", getattr(cfgc, "DC_TOLERANCE_POWER", self._dc_tolerance_power)))
        self._dc_maintain_step = float(getattr(mod, "DC_MAINTAIN_STEP", getattr(cfgc, "DC_MAINTAIN_STEP", self._dc_maintain_step)))
        self._dc_interval_ms = int(getattr(mod, "DC_INTERVAL_MS", getattr(cfgc, "DC_INTERVAL_MS", self._dc_interval_ms)))

        # 저전력/저전류 감시
        self._dc_low_w_thresh = float(getattr(mod, "DC_LOW_W_THRESH", getattr(cfgc, "DC_LOW_W_THRESH", self._dc_low_w_thresh)))
        self._dc_low_streak_n = int(getattr(mod, "DC_LOW_STREAK_N", getattr(cfgc, "DC_LOW_STREAK_N", self._dc_low_streak_n)))
        self._dc_low_current_thresh_a = float(getattr(mod, "DC_LOW_CURRENT_THRESH_A", getattr(cfgc, "DC_LOW_CURRENT_THRESH_A", self._dc_low_current_thresh_a)))
        self._dc_low_current_streak_n = int(getattr(mod, "DC_LOW_CURRENT_STREAK_N", getattr(cfgc, "DC_LOW_CURRENT_STREAK_N", self._dc_low_current_streak_n)))

        # deadband도 config로 제어 가능하게(원하면)
        self._watt_deadband = float(getattr(mod, "DC_WATT_DEADBAND", getattr(cfgc, "DC_WATT_DEADBAND", self._watt_deadband)))

        # 방어
        if self._dc_max_power < 0:
            self._dc_max_power = 0.0
        if self._dc_interval_ms < 50:
            self._dc_interval_ms = 50
        if self._dc_low_streak_n < 1:
            self._dc_low_streak_n = 1
        if self._dc_low_current_streak_n < 1:
            self._dc_low_current_streak_n = 1

    # ======= 퍼블릭 이벤트 스트림 =======
    async def events(self) -> AsyncGenerator[DCPowerEvent, None]:
        while True:
            ev = await self._event_q.get()
            yield ev

    # ======= 공용 API =======
    async def start_process(self, target_power: float):
        if self._is_running:
            await self._emit_status("경고: DC 파워가 이미 동작 중입니다.")
            return

        self.target_power = float(max(0.0, min(self._dc_max_power, target_power)))
        self._low_power_streak = 0    # ★ 저전력 카운터 리셋
        self._low_current_streak = 0  # ★ 저전류 카운터 리셋

        if not self._toggle_enable:
            await self._emit_status("DCV SET ON 실패: toggle_enable 콜백이 없습니다.")
            return

        try:
            await self._toggle_enable(True)
            await self._emit_status("DCV SET ON")
        except Exception as e:
            await self._emit_status(f"DCV SET ON 실패: {e!r}")
            return  # 실패 시 시작 중단을 원하면 유지
        
        self._enabled = True

        self._is_running = True
        await self._emit_state_changed(True)
        self.state = "MAINTAINING"  # ← 바로 유지 모드
        self._sent_target_reached = False
        await self._emit_status(f"프로세스 시작. 목표: {self.target_power:.1f} W (직접 설정)")

        # 파워 ON 시 폴링 ON
        self.set_process_status(True)

        # ✅ 시작하자마자 목표 W로 전송 (SET 포함 경로)
        try:
            await self._send_dc_power(self.target_power)
            self._last_sent_power = float(self.target_power)
            await self._emit_status(f"목표 {self.target_power:.1f} W 즉시 전송")
        except Exception as e:
            await self._emit_status(f"초기 전송 실패: {e!r}")

        # 첫 보정 1회 태스크 기동
        if self._adjust_task and not self._adjust_task.done():
            self._adjust_task.cancel()
        self._adjust_task = asyncio.create_task(self._adjust_once(), name="DC_Adjust")

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
        # 스텝다운 루프 대신 '즉시 0 한 번'만 전송
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
            while self._is_running:
                if not self._polling_enabled:
                    await asyncio.sleep(0.2)  # OFF 동안 대기만
                    continue
                try:
                    res = await self._request_status_read() if self._request_status_read else None
                    if res is not None:
                        self._ingest_status_result(res)
                        
                        # === 연속 저전력 감시(최소 수정) ===
                        try:
                            p = float(self.power_w or 0.0)
                            if p <= self._dc_low_w_thresh:
                                self._low_power_streak += 1
                                await self._emit_status(
                                    f"저전력 감시: {self._low_power_streak}/{self._dc_low_streak_n} "
                                    f"(meas={p:.1f}W ≤ {self._dc_low_w_thresh:.1f}W)"
                                )
                                if self._low_power_streak >= self._dc_low_streak_n:
                                    await self._emit_status(
                                        f"DC 파워가 {self._dc_low_streak_n}회 연속 ≤ {self._dc_low_w_thresh:.1f}W → 공정 중단"
                                    )
                                    self._ev_nowait(DCPowerEvent(
                                        kind="target_failed",
                                        message=(f"저전력 연속 {self._low_power_streak}/{self._dc_low_streak_n} "
                                                f"(meas={float(self.power_w or 0.0):.1f}W ≤ {self._dc_low_w_thresh:.1f}W)")
                                    ))
                                    asyncio.create_task(self.cleanup())
                                    break
                            else:
                                if self._low_power_streak:
                                    self._low_power_streak = 0
                        except Exception:
                            pass

                        # === 램프업(목표 도달) 이후 저전류 감시 ===
                        try:
                            if self._sent_target_reached:
                                ia = float(self.current_a or 0.0)
                                if ia <= self._dc_low_current_thresh_a:
                                    self._low_current_streak += 1
                                    await self._emit_status(
                                        f"저전류 감시(목표 도달 후): "
                                        f"{self._low_current_streak}/{self._dc_low_current_streak_n} "
                                        f"(meas={ia:.3f}A ≤ {self._dc_low_current_thresh_a:.3f}A)"
                                    )
                                    if self._low_current_streak >= self._dc_low_current_streak_n:
                                        await self._emit_status(
                                            f"DC 전류가 목표 도달 후 "
                                            f"{self._dc_low_current_streak_n}회 연속 ≤ "
                                            f"{self._dc_low_current_thresh_a:.3f}A → 공정 중단"
                                        )
                                        self._ev_nowait(DCPowerEvent(
                                            kind="target_failed",
                                            message=(
                                                f"저전류 연속 {self._low_current_streak}/"
                                                f"{self._dc_low_current_streak_n} "
                                                f"(meas={ia:.3f}A ≤ {self._dc_low_current_thresh_a:.3f}A)"
                                            ),
                                        ))
                                        asyncio.create_task(self.cleanup())
                                        break
                                else:
                                    if self._low_current_streak:
                                        self._low_current_streak = 0
                        except Exception:
                            pass

                except Exception as e:
                    await self._emit_status(f"상태 읽기 요청 실패: {e}")
                await asyncio.sleep(self._dc_interval_ms / 1000.0)
        except asyncio.CancelledError:
            pass

    async def _rampdown_loop(self):
        """스텝다운 없이 '0 한 번'만 기록하고 종료."""
        try:
            # 이미 0을 보냈었다면(캐시) 실제 I/O 스킵
            if (self._last_sent_power or 0.0) != 0.0:
                await self._set_dc_unverified(0.0)

            self._last_sent_power = 0.0

            # 폴링도 OFF
            self.set_process_status(False)

            if self._toggle_enable and self._enabled:
                try:
                    await self._toggle_enable(False)  # ← SET OFF (종료 시)
                    await self._emit_status("DCV SET OFF")
                finally:
                    self._enabled = False

            # 표시/이벤트 정리
            self._ev_nowait(DCPowerEvent(kind="display", power=0.0, voltage=0.0, current=0.0))
            await self._emit_status("DC 파워 ramp-down 완료 (snap-to-zero)")
            self._ev_nowait(DCPowerEvent(kind="power_off_finished"))

        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._emit_status(f"램프다운 오류: {e}")

    async def _adjust_once(self):
        try:
            if not self._is_running:
                return

            last_sent: Optional[float] = getattr(self, "_last_sent_power", None)
            deadband = float(self._watt_deadband)

            # ▼ 유지 보정만 수행
            error = float(self.target_power) - float(self.power_w)

            # 허용 오차 내 → 보정 스킵
            if abs(error) <= float(self._dc_tolerance_power):
                if not self._sent_target_reached:
                    self._ev_nowait(DCPowerEvent(kind="target_reached"))
                    self._sent_target_reached = True
                return

            # 마지막 전송값 기준으로 한 스텝 보정 (측정이 낮으면 +, 높으면 -)
            base = float(last_sent if last_sent is not None else self.target_power)
            step = float(self._dc_maintain_step) if error > 0 else -float(self._dc_maintain_step)
            new_power = max(0.0, min(float(self._dc_max_power), base + step))

            # 데드밴드 적용 후 전송
            if (last_sent is None) or (abs(new_power - last_sent) >= deadband):
                try:
                    await self._send_dc_power(float(new_power))
                    self._last_sent_power = float(new_power)
                except Exception as e:
                    await self._emit_status(f"DC 설정 전송 실패: {e}")
                await self._emit_status(
                    f"오차 보정: meas={self.power_w:.1f}W, target={self.target_power:.1f}W → set {new_power:.1f}W"
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

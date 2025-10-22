# host/handlers.py
# -*- coding: utf-8 -*-
"""
실제 동작(장비 제어) 담당
- 상태 조회 / 공정 시작 / PLC 및 CHx 제어
- 충돌 방지를 위한 Lock(PLC/CH1/CH2) 관리
- 성공/실패 응답 포맷 통일
"""
from __future__ import annotations
from typing import Dict, Any
from .context import HostContext

Json = Dict[str, Any]


class HostHandlers:
    def __init__(self, ctx: HostContext) -> None:
        self.ctx = ctx

    # --------- 공통 응답 헬퍼 ---------
    def _ok(self, msg: str = "OK", **extra) -> Json:
        return {"result": "success", "message": msg, **extra}

    def _fail(self, e: Exception | str) -> Json:
        return {"result": "fail", "message": str(e)}

    # --------- 상태 조회 ---------
    async def get_sputter_status(self, _: Json) -> Json:
        try:
            running = bool(getattr(self.ctx.runtime_state, "any_running")())
            cleaning = bool(getattr(self.ctx.pc, "is_running", getattr(self.ctx.pc, "_running", False)))
            state = "cleaning" if cleaning else ("running" if running else "idle")

            # 진공 여부: GV OPEN 램프(1/2) 중 하나라도 켜져 있으면 True
            vacuum = False
            try:
                gv1 = await self.ctx.plc.read_bit("G_V_1_OPEN_LAMP")
                gv2 = await self.ctx.plc.read_bit("G_V_2_OPEN_LAMP")
                vacuum = bool(gv1 or gv2)
            except Exception:
                pass
            return self._ok(state=state, vacuum=vacuum)
        except Exception as e:
            return self._fail(e)

    # --------- 공정 시작 ---------
    async def start_sputter(self, data: Json) -> Json:
        recipe = str(data.get("recipe", ""))
        try:
            if hasattr(self.ctx.runtime_state, "any_running") and self.ctx.runtime_state.any_running():
                return self._fail("Another process is running")

            ch = int(data.get("ch", 1))  # 기본 CH1
            target = self.ctx.ch1 if ch == 1 else self.ctx.ch2

            if not hasattr(target, "start_with_recipe_string"):
                return self._fail("Runtime missing 'start_with_recipe_string' API")

            await target.start_with_recipe_string(recipe)
            return self._ok("Sputter started", ch=ch)
        except Exception as e:
            return self._fail(e)

    async def start_plasma_cleaning(self, data: Json) -> Json:
        recipe = str(data.get("recipe", ""))
        try:
            if hasattr(self.ctx.runtime_state, "any_running") and self.ctx.runtime_state.any_running():
                return self._fail("Another process is running")

            if not hasattr(self.ctx.pc, "start_with_recipe_string"):
                return self._fail("PlasmaCleaning runtime missing 'start_with_recipe_string' API")

            await self.ctx.pc.start_with_recipe_string(recipe)
            return self._ok("Plasma Cleaning started")
        except Exception as e:
            return self._fail(e)

    # --------- PLC 공용 계통 ---------
    async def vacuum_on(self, _: Json) -> Json:
        try:
            async with self.ctx.lock_plc:
                await self.ctx.plc.gv_open()
            return self._ok("VACUUM_ON")
        except Exception as e:
            return self._fail(e)

    async def vacuum_off(self, _: Json) -> Json:
        try:
            async with self.ctx.lock_plc:
                await self.ctx.plc.gv_close()
            return self._ok("VACUUM_OFF")
        except Exception as e:
            return self._fail(e)

    async def four_pin_up(self, _: Json) -> Json:
        try:
            async with self.ctx.lock_plc:
                await self.ctx.plc.lift_pin(up=True)   # ← 실제 API명
            return self._ok("4PIN_UP")
        except Exception as e:
            return self._fail(e)

    async def four_pin_down(self, _: Json) -> Json:
        try:
            async with self.ctx.lock_plc:
                await self.ctx.plc.lift_pin(up=False)
            return self._ok("4PIN_DOWN")
        except Exception as e:
            return self._fail(e)

    # --------- CHx 설비 제어 (게이트/척) ---------
    async def gate_open(self, data: Json) -> Json:
        ch = int(data.get("ch", 1))
        lock = self.ctx.lock_ch1 if ch == 1 else self.ctx.lock_ch2
        try:
            async with lock:
                # plc.py 실제 API: gate_valve(chamber=, open=)
                await self.ctx.plc.gate_valve(chamber=ch, open=True)
            return self._ok(f"CH{ch}_GATE_OPEN")
        except Exception as e:
            return self._fail(e)

    async def gate_close(self, data: Json) -> Json:
        ch = int(data.get("ch", 1))
        lock = self.ctx.lock_ch1 if ch == 1 else self.ctx.lock_ch2
        try:
            async with lock:
                await self.ctx.plc.gate_valve(chamber=ch, open=False)
            return self._ok(f"CH{ch}_GATE_CLOSE")
        except Exception as e:
            return self._fail(e)

    # CHUCK은 장비 API가 없으므로 일단 미지원 처리 (필요하면 lift_pin으로 매핑)
    async def chuck_up(self, data: Json) -> Json:
        return self._fail("CHx_CHUCK_UP is not supported by PLC API")

    async def chuck_down(self, data: Json) -> Json:
        return self._fail("CHx_CHUCK_DOWN is not supported by PLC API")
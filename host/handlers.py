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
import asyncio, time

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
    async def vacuum_on(self, data: Json) -> Json:
        """
        VACUUM ON 시퀀스:
        1) L_R_P_SW = True  (러핑펌프 ON)
        2) L_R_V_인터락 == True 확인
        3) L_R_V_SW = True  (러핑밸브 ON)
        4) L_VAC_READY_SW == True 까지 대기 (기본 600s)
        """
        timeout_s = float(data.get("timeout_s", 600.0))  # 기본 10분
        # 엑셀 맵: L_VAC_READY_SW = M00062 → 0x62 = 98
        L_VAC_READY_ADDR = int(data.get("vac_ready_addr", 98))

        try:
            async with self.ctx.lock_plc:
                # 1) 러핑펌프 ON
                await self.ctx.plc.write_switch("L_R_P_SW", True)

                # 2) 러핑밸브 인터락 확인
                interlock_ok = await self.ctx.plc.read_bit("L_R_V_인터락")
                if not interlock_ok:
                    return self._fail("L_R_V_인터락=FALSE → 러핑밸브 개방 불가")

                # 3) 러핑밸브 ON
                await self.ctx.plc.write_switch("L_R_V_SW", True)

                # 4) VAC_READY=True 대기 (10분)
                deadline = time.monotonic() + timeout_s

                async def _read_vac_ready() -> bool:
                    # 맵에 'L_VAC_READY_SW' 키가 생기면 이름으로 우선 읽고,
                    # 없으면 주소(98)로 폴백
                    try:
                        return await self.ctx.plc.read_bit("L_VAC_READY_SW")
                    except KeyError:
                        return await self.ctx.plc.read_bit(L_VAC_READY_ADDR)

                while time.monotonic() < deadline:
                    if await _read_vac_ready():
                        return self._ok("VACUUM_ON 완료 — L_VAC_READY_SW=TRUE")
                    await asyncio.sleep(0.5)  # 폴링 간격

                return self._fail(f"VACUUM_ON 타임아웃: {int(timeout_s)}s 내 L_VAC_READY_SW TRUE 미도달")

        except Exception as e:
            return self._fail(e)

    async def vacuum_off(self, data: Json) -> Json:
        """
        VACUUM OFF 시퀀스:
        1) L_VENT_인터락 True 확인
        2) L_VENT_SW = True (벤트 시작)
        3) L_ATM == True 까지 대기 (기본 240s)
        """
        timeout_s = float(data.get("timeout_s", 240.0))  # 기본 4분

        # 엑셀 맵 기준 L_ATM = M00063 → 주소 99
        # plc 맵에 등록하지 않고 직접 주소로 읽는다.
        L_ATM_ADDR = int(data.get("atm_addr", 99))

        try:
            async with self.ctx.lock_plc:
                # 1) 인터락 확인
                interlock_ok = await self.ctx.plc.read_bit("L_VENT_인터락")
                if not interlock_ok:
                    return self._fail("L_VENT_인터락=FALSE → 벤트 불가")

                # 2) L_VENT_SW ON
                await self.ctx.plc.write_switch("L_VENT_SW", True)

                # 3) L_ATM True까지 대기
                deadline = time.monotonic() + timeout_s

                # 우선 이름으로 읽어보고(맵에 있을 수도 있으므로),
                # 없으면 주소(99)로 폴백
                async def _read_L_ATM() -> bool:
                    try:
                        return await self.ctx.plc.read_bit("L_ATM")
                    except KeyError:
                        return await self.ctx.plc.read_bit(L_ATM_ADDR)

                while time.monotonic() < deadline:
                    if await _read_L_ATM():
                        return self._ok("VACUUM_OFF 완료 (L_ATM=TRUE)")
                    await asyncio.sleep(0.5)  # 0.5s 폴링

                return self._fail(f"VACUUM_OFF 타임아웃: {timeout_s:.0f}s 내 L_ATM TRUE 미도달")

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
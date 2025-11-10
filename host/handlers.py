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
        0) L_VENT_SW = False 선행 정지
        1) L_R_P_SW = True  (러핑펌프 ON)
        2) L_R_V_인터락 == True 확인
        3) L_R_V_SW = True  (러핑밸브 ON)
        4) L_VAC_READY_SW == True 까지 대기 (기본 600s)
        ※ 어떤 경로로든 종료 시 L_R_P_SW, L_R_V_SW를 False로 원복
        """
        timeout_s = float(data.get("timeout_s", 600.0))  # 기본 10분

        async with self.ctx.lock_plc:
            try:
                # 0) L_VENT_SW OFF
                await self.ctx.plc.write_switch("L_VENT_SW", False)
                await asyncio.sleep(0.5)  # 권장: 짧은 안정화

                # 1) 러핑펌프 ON
                await self.ctx.plc.write_switch("L_R_P_SW", True)

                # 2) 러핑밸브 인터락 확인
                interlock_ok = await self.ctx.plc.read_bit("L_R_V_인터락")
                if not interlock_ok:
                    return self._fail("L_R_V_인터락=FALSE → 러핑밸브 개방 불가")

                # 3) 러핑밸브 ON
                await self.ctx.plc.write_switch("L_R_V_SW", True)

                # 4) VAC_READY=True 대기
                deadline = time.monotonic() + timeout_s
                while time.monotonic() < deadline:
                    if await self.ctx.plc.read_bit("L_VAC_READY_SW"):
                        return self._ok("VACUUM_ON 완료 — L_VAC_READY_SW=TRUE")
                    await asyncio.sleep(0.5)

                # (타임아웃) 준비 신호 미도달 → L_VAC_NOT_READY 읽어서 메시지에 포함 + door 확인 문구
                not_ready = False
                try:
                    not_ready = await self.ctx.plc.read_bit("L_VAC_NOT_READY")
                except Exception:
                    # 읽기 실패는 메시지 구성에만 영향, 흐름엔 영향 없음
                    pass

                return self._fail(
                    f"VACUUM_ON 타임아웃: {int(timeout_s)}s 내 L_VAC_READY_SW TRUE 미도달 "
                    f"(L_VAC_NOT_READY={not_ready}) — door 확인"
                )

            except Exception as e:
                # 예외 사유는 message로 그대로 클라이언트 전달
                return self._fail(e)

            finally:
                # 어떤 경로로든 항상 OFF 시도 (래치/순간형 모두 무해)
                try:
                    await self.ctx.plc.write_switch("L_R_V_SW", False)
                except Exception:
                    pass
                try:
                    await self.ctx.plc.write_switch("L_R_P_SW", False)
                except Exception:
                    pass

    async def vacuum_off(self, data: Json) -> Json:
        """
        VACUUM OFF 시퀀스:
        0) L_R_V_SW=False → L_R_P_SW=False 선행 정지
        1) L_VENT_인터락 True 확인
        2) L_VENT_SW = True (벤트 시작)
        3) L_ATM == True 까지 대기 (기본 240s)
        4) L_VENT_SW = False
        """
        timeout_s = float(data.get("timeout_s", 240.0))  # 기본 4분

        async with self.ctx.lock_plc:
            try:
                # 0) L_R_V_SW=False → L_R_P_SW=False 선행 정지
                await self.ctx.plc.write_switch("L_R_V_SW", False)
                await asyncio.sleep(0.5)  # 짧은 안정화
                await self.ctx.plc.write_switch("L_R_P_SW", False)

                # 1) 인터락 확인
                interlock_ok = await self.ctx.plc.read_bit("L_VENT_인터락")
                if not interlock_ok:
                    return self._fail("L_VENT_인터락=FALSE → 벤트 불가")

                # 2) L_VENT_SW ON
                await self.ctx.plc.write_switch("L_VENT_SW", True)

                # 3) L_ATM True까지 대기
                deadline = time.monotonic() + timeout_s

                while time.monotonic() < deadline:
                    if await self.ctx.plc.read_bit("L_ATM"):
                        return self._ok("VACUUM_OFF 완료 (L_ATM=TRUE)")
                    await asyncio.sleep(0.5)  # 0.5s 폴링

                return self._fail(f"VACUUM_OFF 타임아웃: {timeout_s:.0f}s 내 L_ATM TRUE 미도달 (N2 gas 부족)")

            except Exception as e:
                # 예외 사유는 message에 그대로 담겨서 클라이언트로 전달됨
                return self._fail(e)
            
            finally:
                # 어떤 경로로든 반드시 OFF 시도(래치형/순간형 모두 무해)
                try:
                    await self.ctx.plc.write_switch("L_VENT_SW", False)
                except Exception:
                    pass

    async def four_pin_up(self, data: Json) -> Json:
        """
        4PIN_UP 시퀀스:
        1) L_PIN_인터락 == True 확인
        2) L_PIN_UP_SW = True
        3) 10초 후 L_PIN_UP_LAMP == True 확인
        """
        wait_s = float(data.get("wait_s", 10.0))  # 기본 10초

        try:
            async with self.ctx.lock_plc:
                # 1) 인터락 확인
                interlock_ok = await self.ctx.plc.read_bit("L_PIN_인터락")
                if not interlock_ok:
                    return self._fail("L_PIN_인터락=FALSE → 4PIN_UP 불가")

                # 2) SW = True
                await self.ctx.plc.write_switch("L_PIN_UP_SW", True)

                # 3) 10초 대기 후 램프 확인
                await asyncio.sleep(wait_s)
                lamp_ok = await self.ctx.plc.read_bit("L_PIN_UP_LAMP")
                if lamp_ok:
                    return self._ok(f"4PIN_UP 완료 — L_PIN_UP_LAMP=TRUE (대기 {int(wait_s)}s)")
                return self._fail(f"4PIN_UP 실패 — {int(wait_s)}s 후 L_PIN_UP_LAMP=FALSE")

        except Exception as e:
            return self._fail(e)

    async def four_pin_down(self, data: Json) -> Json:
        """
        4PIN_DOWN 시퀀스:
        1) L_PIN_인터락 == True 확인
        2) L_PIN_DOWN_SW = True
        3) 10초 후 L_PIN_DOWN_LAMP == True 확인
        """
        wait_s = float(data.get("wait_s", 10.0))  # 기본 10초

        try:
            async with self.ctx.lock_plc:
                # 1) 인터락 확인
                interlock_ok = await self.ctx.plc.read_bit("L_PIN_인터락")
                if not interlock_ok:
                    return self._fail("L_PIN_인터락=FALSE → 4PIN_DOWN 불가")

                # 2) SW = True
                await self.ctx.plc.write_switch("L_PIN_DOWN_SW", True)

                # 3) 10초 대기 후 램프 확인
                await asyncio.sleep(wait_s)
                lamp_ok = await self.ctx.plc.read_bit("L_PIN_DOWN_LAMP")
                if lamp_ok:
                    return self._ok(f"4PIN_DOWN 완료 — L_PIN_DOWN_LAMP=TRUE (대기 {int(wait_s)}s)")
                return self._fail(f"4PIN_DOWN 실패 — {int(wait_s)}s 후 L_PIN_DOWN_LAMP=FALSE")

        except Exception as e:
            return self._fail(e)

    # --------- CHx 설비 제어 (게이트/척) ---------
    async def gate_open(self, data: Json) -> Json:
        """
        CHx_GATE_OPEN 시퀀스:
        1) G_V_{ch}_인터락 == True 확인
        2) G_V_{ch}_OPEN_SW = True
        3) 5초 후 G_V_{ch}_OPEN_LAMP == True 확인
        """
        ch = int(data.get("ch", 1))
        wait_s = float(data.get("wait_s", 5.0))  # 기본 5초

        if ch == 1:
            interlock, sw, lamp = "G_V_1_인터락", "G_V_1_OPEN_SW", "G_V_1_OPEN_LAMP"
        elif ch == 2:
            interlock, sw, lamp = "G_V_2_인터락", "G_V_2_OPEN_SW", "G_V_2_OPEN_LAMP"
        else:
            return self._fail(f"지원하지 않는 CH: {ch}")

        lock = self.ctx.lock_ch1 if ch == 1 else self.ctx.lock_ch2
        try:
            async with lock:
                # 1) 인터락 확인
                il = await self.ctx.plc.read_bit(interlock)
                if not il:
                    return self._fail(f"{interlock}=FALSE → CH{ch}_GATE_OPEN 불가")

                # 2) 스위치 TRUE
                await self.ctx.plc.write_switch(sw, True)

                # 3) 대기 후 램프 확인
                await asyncio.sleep(wait_s)
                ok = await self.ctx.plc.read_bit(lamp)
                if ok:
                    return self._ok(f"CH{ch}_GATE_OPEN 완료 — {lamp}=TRUE (대기 {int(wait_s)}s)")
                return self._fail(f"CH{ch}_GATE_OPEN 실패 — {lamp}=FALSE (대기 {int(wait_s)}s)")

        except Exception as e:
            return self._fail(e)

    async def gate_close(self, data: Json) -> Json:
        """
        CHx_GATE_CLOSE 시퀀스:
        1) G_V_{ch}_인터락 == True 확인
        2) G_V_{ch}_CLOSE_SW = True
        3) 5초 후 G_V_{ch}_CLOSE_LAMP == True 확인
        """
        ch = int(data.get("ch", 1))
        wait_s = float(data.get("wait_s", 5.0))  # 기본 5초

        if ch == 1:
            interlock, sw, lamp = "G_V_1_인터락", "G_V_1_CLOSE_SW", "G_V_1_CLOSE_LAMP"
        elif ch == 2:
            interlock, sw, lamp = "G_V_2_인터락", "G_V_2_CLOSE_SW", "G_V_2_CLOSE_LAMP"
        else:
            return self._fail(f"지원하지 않는 CH: {ch}")

        lock = self.ctx.lock_ch1 if ch == 1 else self.ctx.lock_ch2
        try:
            async with lock:
                # 1) 인터락 확인
                il = await self.ctx.plc.read_bit(interlock)
                if not il:
                    return self._fail(f"{interlock}=FALSE → CH{ch}_GATE_CLOSE 불가")

                # 2) 스위치 TRUE
                await self.ctx.plc.write_switch(sw, True)

                # 3) 대기 후 램프 확인
                await asyncio.sleep(wait_s)
                ok = await self.ctx.plc.read_bit(lamp)
                if ok:
                    return self._ok(f"CH{ch}_GATE_CLOSE 완료 — {lamp}=TRUE (대기 {int(wait_s)}s)")
                return self._fail(f"CH{ch}_GATE_CLOSE 실패 — {lamp}=FALSE (대기 {int(wait_s)}s)")

        except Exception as e:
            return self._fail(e)

    async def chuck_up(self, data: Json) -> Json:
        """
        CHx_CHUCK_UP 시퀀스 (CH1/CH2 공통):
        - CH1: Z_M_P_1_MID_SW = True → 40초 후 Z1_MID == True 확인
        - CH2: Z_M_P_2_MID_SW = True → 40초 후 Z2_MID == True 확인
        """
        ch = int(data.get("ch", 1))
        wait_s = float(data.get("wait_s", 40.0))  # 기본 40초

        # CH별 스위치/램프 매핑
        if ch == 1:
            sw_name, lamp_name = "Z_M_P_1_MID_SW", "Z1_MID"
        elif ch == 2:
            sw_name, lamp_name = "Z_M_P_2_MID_SW", "Z2_MID"
        else:
            return self._fail(f"지원하지 않는 CH: {ch}")

        try:
            lock = self.ctx.lock_ch1 if ch == 1 else self.ctx.lock_ch2
            async with lock:
                # 1) 스위치 TRUE
                await self.ctx.plc.write_switch(sw_name, True)

                # 2) 대기 후 램프 확인
                await asyncio.sleep(wait_s)
                lamp_ok = await self.ctx.plc.read_bit(lamp_name)
                if lamp_ok:
                    return self._ok(f"CH{ch}_CHUCK_UP 완료 — {lamp_name}=TRUE (대기 {int(wait_s)}s)")
                return self._fail(f"CH{ch}_CHUCK_UP 실패 — {lamp_name}=FALSE (대기 {int(wait_s)}s)")

        except Exception as e:
            return self._fail(e)


    async def chuck_down(self, data: Json) -> Json:
        """
        CHx_CHUCK_DOWN 시퀀스 (CH1/CH2 공통):
        - CH1: Z_M_P_1_CCW_SW = True → 40초 후 Z1_DOWN == True 확인
        - CH2: Z_M_P_2_CCW_SW = True → 40초 후 Z2_DOWN == True 확인
        """
        ch = int(data.get("ch", 1))
        wait_s = float(data.get("wait_s", 40.0))  # 기본 40초

        # CH별 스위치/램프 매핑
        if ch == 1:
            sw_name, lamp_name = "Z_M_P_1_CCW_SW", "Z1_DOWN"
        elif ch == 2:
            sw_name, lamp_name = "Z_M_P_2_CCW_SW", "Z2_DOWN"
        else:
            return self._fail(f"지원하지 않는 CH: {ch}")

        try:
            lock = self.ctx.lock_ch1 if ch == 1 else self.ctx.lock_ch2
            async with lock:
                # 1) 스위치 TRUE
                await self.ctx.plc.write_switch(sw_name, True)

                # 2) 대기 후 램프 확인
                await asyncio.sleep(wait_s)
                lamp_ok = await self.ctx.plc.read_bit(lamp_name)
                if lamp_ok:
                    return self._ok(f"CH{ch}_CHUCK_DOWN 완료 — {lamp_name}=TRUE (대기 {int(wait_s)}s)")
                return self._fail(f"CH{ch}_CHUCK_DOWN 실패 — {lamp_name}=FALSE (대기 {int(wait_s)}s)")

        except Exception as e:
            return self._fail(e)

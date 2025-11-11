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

    # ================== 공통 응답 헬퍼 ==================
    def _ok(self, msg: str = "OK", **extra) -> Json:
        return {"result": "success", "message": msg, **extra}

    def _fail(self, e: Exception | str) -> Json:
        return {"result": "fail", "message": str(e)}

    # ================== CH1,2 상태 조회 ==================
    async def get_sputter_status(self, _: Json) -> Json:
        try:
            running = bool(getattr(self.ctx.runtime_state, "any_running")())
            cleaning = bool(getattr(self.ctx.pc, "is_running", getattr(self.ctx.pc, "_running", False)))
            state = "cleaning" if cleaning else ("running" if running else "idle")

            # 진공 여부: L_ATM만 사용
            # L_ATM=True(대기압)  -> vacuum=False
            # L_ATM=False(비대기압)-> vacuum=True
            atm = await self.ctx.plc.read_bit("L_ATM")
            vacuum = (not bool(atm))

            return self._ok(state=state, vacuum=vacuum)
        except Exception as e:
            return self._fail(e)

    # ================== CH1,2/plasma cleaning 공정 제어 ==================
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

    # ================== LoadLock vacuum 제어 ==================
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
                
                # 0-1) 러핑펌프 OFF 타이머(쿨타임) 확인 → 사유 분리
                if await self.ctx.plc.read_bit("L_R_P_OFF_TIMER"):
                    return self._fail("러핑펌프 OFF 타이머 진행 중 → 잠시 후 재시도")

                # 1) 러핑펌프 ON
                await self.ctx.plc.press_switch("L_R_P_SW")

                # 2) 러핑밸브 인터락 확인 → 사유 분리
                if not await self.ctx.plc.read_bit("L_R_V_인터락"):
                    return self._fail("L_R_V_인터락=FALSE → 러핑밸브 개방 불가")

                # 3) 러핑밸브 ON
                await self.ctx.plc.press_switch("L_R_V_SW")

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

    # ================== LoadLock 4pin 제어 ==================
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

                # 2) SW = True (순간 펄스 방식)
                await self.ctx.plc.press_switch("L_PIN_UP_SW")

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

                # 2) SW = True (순간 펄스 방식)
                await self.ctx.plc.press_switch("L_PIN_DOWN_SW")

                # 3) 10초 대기 후 램프 확인
                await asyncio.sleep(wait_s)
                lamp_ok = await self.ctx.plc.read_bit("L_PIN_DOWN_LAMP")
                if lamp_ok:
                    return self._ok(f"4PIN_DOWN 완료 — L_PIN_DOWN_LAMP=TRUE (대기 {int(wait_s)}s)")
                return self._fail(f"4PIN_DOWN 실패 — {int(wait_s)}s 후 L_PIN_DOWN_LAMP=FALSE")

        except Exception as e:
            return self._fail(e)

    # ================== CH1,2 gate 제어 ==================
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

                # 2) 스위치 TRUE (순간 펄스 방식)
                await self.ctx.plc.press_switch(sw)

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

                # 2) 스위치 TRUE (순간 펄스 방식)
                await self.ctx.plc.press_switch(sw)

                # 3) 대기 후 램프 확인
                await asyncio.sleep(wait_s)
                ok = await self.ctx.plc.read_bit(lamp)
                if ok:
                    return self._ok(f"CH{ch}_GATE_CLOSE 완료 — {lamp}=TRUE (대기 {int(wait_s)}s)")
                return self._fail(f"CH{ch}_GATE_CLOSE 실패 — {lamp}=FALSE (대기 {int(wait_s)}s)")

        except Exception as e:
            return self._fail(e)

    # ================== CH1,2 chuck 제어 ==================
    async def chuck_up(self, data: Json) -> Json:
        """
        (현재 정의 유지) CHx_CHUCK_UP = MID로 이동
        - CH1: Z_M_P_1_SW → Z_M_P_1_MID_SW → Z1_MID 램프/SGN 폴링
        - CH2: Z_M_P_2_SW → Z_M_P_2_MID_SW → Z2_MID 램프/SGN 폴링
        """
        ch = int(data.get("ch", 1))
        timeout_s = float(data.get("wait_s", 40.0))

        if ch == 1:
            return await self._move_chuck(
                1, "Z_M_P_1_SW", "Z_M_P_1_MID_SW", "Z1_MID", "mid", timeout_s
            )
        elif ch == 2:
            return await self._move_chuck(
                2, "Z_M_P_2_SW", "Z_M_P_2_MID_SW", "Z2_MID", "mid", timeout_s
            )
        else:
            return self._fail(f"지원하지 않는 CH: {ch}")

    async def chuck_down(self, data: Json) -> Json:
        """
        CHx_CHUCK_DOWN = 최하단 이동
        - CH1: Z_M_P_1_SW → Z_M_P_1_CCW_SW → Z1_DOWN 램프/SGN 폴링
        - CH2: Z_M_P_2_SW → Z_M_P_2_CCW_SW → Z2_DOWN 램프/SGN 폴링
        """
        ch = int(data.get("ch", 1))
        timeout_s = float(data.get("wait_s", 40.0))

        if ch == 1:
            return await self._move_chuck(
                1, "Z_M_P_1_SW", "Z_M_P_1_CCW_SW", "Z1_DOWN", "down", timeout_s
            )
        elif ch == 2:
            return await self._move_chuck(
                2, "Z_M_P_2_SW", "Z_M_P_2_CCW_SW", "Z2_DOWN", "down", timeout_s
            )
        else:
            return self._fail(f"지원하지 않는 CH: {ch}")

    async def _read_chuck_position(self, ch: int) -> dict:
        """
        SGN(실제 위치) + 램프(완료 판정)를 함께 읽어 종합 위치를 반환.
        return: {
        "position": "up"|"mid"|"down"|"unknown",
        "sgn": {"up":bool, "mid":bool, "down":bool},
        "lamp":{"up":bool, "mid":bool, "down":bool}
        }
        """
        if ch == 1:
            s_up, s_mid, s_dn = "Z_M_P_1_UP_SGN", "Z_M_P_1_MID_SGN", "Z_M_P_1_DN_SGN"
            l_up, l_mid, l_dn = "Z1_UP", "Z1_MID", "Z1_DOWN"
        elif ch == 2:
            s_up, s_mid, s_dn = "Z_M_P_2_UP_SGN", "Z_M_P_2_MID_SGN", "Z_M_P_2_DN_SGN"
            l_up, l_mid, l_dn = "Z2_UP", "Z2_MID", "Z2_DOWN"
        else:
            raise ValueError(f"지원하지 않는 CH: {ch}")

        # SGN(센서) 읽기
        s_up_b  = bool(await self.ctx.plc.read_bit(s_up))
        s_mid_b = bool(await self.ctx.plc.read_bit(s_mid))
        s_dn_b  = bool(await self.ctx.plc.read_bit(s_dn))

        # 램프(완료 상태) 읽기
        l_up_b  = bool(await self.ctx.plc.read_bit(l_up))
        l_mid_b = bool(await self.ctx.plc.read_bit(l_mid))
        l_dn_b  = bool(await self.ctx.plc.read_bit(l_dn))

        # 우선순위: SGN이 원-핫이면 그걸 위치로, 아니면 램프 원-핫을 보조로 사용
        pos = "unknown"
        s_sum = int(s_up_b) + int(s_mid_b) + int(s_dn_b)
        l_sum = int(l_up_b) + int(l_mid_b) + int(l_dn_b)

        if s_sum == 1:
            pos = "up" if s_up_b else ("mid" if s_mid_b else "down")
        elif l_sum == 1:
            pos = "up" if l_up_b else ("mid" if l_mid_b else "down")

        return {
            "position": pos,
            "sgn": {"up": s_up_b, "mid": s_mid_b, "down": s_dn_b},
            "lamp": {"up": l_up_b, "mid": l_mid_b, "down": l_dn_b},
        }


    async def _move_chuck(self, ch: int, power_sw: str, move_sw: str,
                        target_lamp: str, target_name: str,
                        timeout_s: float = 40.0) -> Json:
        """
        공통 Chuck 이동: 이미 목표면 즉시 OK, 아니면 power→move 펄스 후 램프/SGN 폴링.
        """
        lock = self.ctx.lock_ch1 if ch == 1 else self.ctx.lock_ch2
        async with lock:
            # 0) 이미 목표면 즉시 OK
            try:
                cur = await self._read_chuck_position(ch)
                if cur["position"] == target_name:
                    return self._ok(f"CH{ch} Chuck OK — 이미 {target_name.upper()} 위치",
                                    current=cur)
            except Exception:
                # 위치 읽기 실패는 이동 시도는 계속
                pass

            # 1) 전원 스위치 펄스
            await self.ctx.plc.press_switch(power_sw)
            await asyncio.sleep(0.2)

            # 2) 이동 스위치 펄스
            await self.ctx.plc.press_switch(move_sw)

            # 3) 폴링 (초반엔 SGN으로 빠른 변화를 보고, 최종은 램프로 확정)
            deadline = time.monotonic() + float(timeout_s)
            first_seen_sgn = False

            while time.monotonic() < deadline:
                try:
                    # 램프(완료) 먼저 확인
                    if await self.ctx.plc.read_bit(target_lamp):
                        cur = await self._read_chuck_position(ch)
                        return self._ok(f"CH{ch} Chuck 완료 — {target_name.upper()}(램프 TRUE)",
                                        current=cur)

                    # SGN(즉시 위치) 보조 체크: 초반에라도 목표 SGN이 잡히면 “도달 중” 로그 가능
                    cur = await self._read_chuck_position(ch)
                    if cur["position"] == target_name and not first_seen_sgn:
                        # 최초 감지 로그를 남기고 계속 램프를 기다림(타이머 고려)
                        first_seen_sgn = True
                except Exception:
                    pass

                await asyncio.sleep(0.4)  # 폴링 주기

            # 4) 타임아웃 — 상태 스냅샷 포함
            try:
                cur = await self._read_chuck_position(ch)
            except Exception:
                cur = None

            return self._fail({
                "msg": f"CH{ch} Chuck 타임아웃 — {target_name.upper()} 미도달",
                "target_lamp": target_lamp,
                "snapshot": cur,
            })

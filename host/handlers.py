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
import asyncio, time, contextlib
from pathlib import Path                      # ← 추가: 경로
from datetime import datetime                 # ← 추가: 파일명 타임스탬프
from contextlib import asynccontextmanager    # ← 추가: 비동기 컨텍스트
import contextlib                             # ← 추가: suppress 사용 (파일 쓰기 실패 무시용)

Json = Dict[str, Any]


class HostHandlers:
    def __init__(self, ctx: HostContext) -> None:
        self.ctx = ctx

    # ================== 로그 저장 헬퍼 ==================
        # NAS 우선, 실패 시 로컬 폴백 디렉터리 준비
        try:
            root = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs")
            d = root / "plc_remote"
            d.mkdir(parents=True, exist_ok=True)
            self._plc_log_dir = d              # 주 저장 폴더(NAS)
        except Exception:
            d = Path.cwd() / "Logs" / "plc_remote"
            d.mkdir(parents=True, exist_ok=True)
            self._plc_log_dir = d              # 폴백 폴더(로컬)

        self._plc_cmd_file = None              # 요청중 파일 경로(컨텍스트 내에서만 셋)

    def _write_line_sync(self, file_path: Path, line: str) -> None:
        """동기 파일 쓰기(예외는 호출부에서 처리)."""
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "a", encoding="utf-8") as fp:
            fp.write(line + "\n")

    def _append_line_nonblocking(self, file_path: Path, line: str) -> None:
        """
        이벤트루프를 막지 않도록 백그라운드 스레드에서 파일 append.
        실패 시 로컬 폴더로 자동 폴백.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # 이벤트루프가 없으면 동기로 시도하되, 실패는 억제
            with contextlib.suppress(Exception):
                self._write_line_sync(file_path, line)
            return

        async def _worker():
            # 1차: 지정 경로(NAS 우선)
            try:
                await asyncio.to_thread(self._write_line_sync, file_path, line)
                return
            except Exception:
                pass
            # 2차: 로컬 폴백(파일명은 동일 basename)
            local = (Path.cwd() / "Logs" / "plc_remote" / file_path.name)
            with contextlib.suppress(Exception):
                await asyncio.to_thread(self._write_line_sync, local, line)

        # 기다리지 않고 태스크만 걸어 둠 → 호출부가 절대 블로킹되지 않음
        loop.create_task(_worker())

    def _plc_file_logger(self, fmt, *args):
        """
        AsyncPLC가 호출하는 printf 스타일 로거 시그니처.
        현재 요청 컨텍스트에서 지정한 self._plc_cmd_file 에 비동기 append.
        """
        try:
            msg = (fmt % args) if args else str(fmt)
            ts  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fn  = self._plc_cmd_file or (self._plc_log_dir / f"plc_host_{datetime.now():%Y%m%d}.txt")
            self._append_line_nonblocking(fn, f"{ts} {msg}")
        except Exception:
            # 로깅 에러로 본체 흐름을 멈추지 않음
            pass

    @asynccontextmanager
    async def _plc_logs_to_file_only(self, tag: str):
        """
        이 블록 동안 발생하는 PLC 로그는:
          - 화면/챔버 로그로 보내지 않고
          - NAS(실패 시 로컬)에만 기록
          - '요청당 고유 파일'로 저장 (명령명이 파일명에 포함)
        """
        plc = self.ctx.plc
        prev = getattr(plc, "log", None)
        # 파일명: plc_host_YYYYmmdd_HHMMSS_<TAG>.txt
        safe_tag = "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in tag)
        self._plc_cmd_file = self._plc_log_dir / f"plc_host_{datetime.now():%Y%m%d_%H%M%S}_{safe_tag}.txt"
        plc.log = self._plc_file_logger
        try:
            yield
        finally:
            plc.log = prev
            self._plc_cmd_file = None

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
            async with self.ctx.lock_plc:
                async with self._plc_logs_to_file_only("GET_SPUTTER_STATUS"):
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
            async with self._plc_logs_to_file_only("VACUUM_ON"):
                try:
                    # 0) L_VENT_SW OFF
                    await self.ctx.plc.write_switch("L_VENT_SW", False)
                    await asyncio.sleep(0.3)  # 권장: 짧은 안정화
                    
                    # 0-1) 러핑펌프 OFF 타이머(쿨타임) 확인 → 사유 분리
                    if await self.ctx.plc.read_bit("L_R_P_OFF_TIMER"):
                        return self._fail("러핑펌프 OFF 타이머 진행 중 → 잠시 후 재시도")

                    # 1) 러핑펌프 ON
                    await self.ctx.plc.write_switch("L_R_P_SW", True)
                    await asyncio.sleep(0.3)  # 인터락 전파 여유 

                    # 2) 러핑밸브 인터락 확인 → 사유 분리
                    if not await self.ctx.plc.read_bit("L_R_V_인터락"):
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
            async with self._plc_logs_to_file_only("VACUUM_OFF"):
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
                async with self._plc_logs_to_file_only("4PIN_UP"):
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
                async with self._plc_logs_to_file_only("4PIN_DOWN"):
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
        async with self.ctx.lock_plc:                      # ← 추가(1)
            async with lock:                               # 기존 CH 락 유지
                async with self._plc_logs_to_file_only(f"GATE_OPEN_CH{ch}"):
                    try:
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
        async with self.ctx.lock_plc:                      # ← 추가(1)
            async with lock:                               # 기존 CH 락 유지
                async with self._plc_logs_to_file_only(f"GATE_CLOSE_CH{ch}"):
                    try:
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
        timeout_s = float(data.get("wait_s", 60.0))

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
        timeout_s = float(data.get("wait_s", 60.0))

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
        SGN(P-주소)는 읽지 않고 램프(M-주소)만으로 위치 판정 (단순/안정).
        'position'은 램프가 정확히 하나만 TRUE일 때만 확정, 아니면 'unknown'.
        """
        if ch == 1:
            l_up, l_mid, l_dn = "Z1_UP", "Z1_MID", "Z1_DOWN"
        elif ch == 2:
            l_up, l_mid, l_dn = "Z2_UP", "Z2_MID", "Z2_DOWN"
        else:
            raise ValueError(f"지원하지 않는 CH: {ch}")

        up  = bool(await self.ctx.plc.read_bit(l_up))
        mid = bool(await self.ctx.plc.read_bit(l_mid))
        dn  = bool(await self.ctx.plc.read_bit(l_dn))

        pos = "unknown"
        if int(up) + int(mid) + int(dn) == 1:
            pos = "up" if up else ("mid" if mid else "down")

        return {"position": pos, "lamp": {"up": up, "mid": mid, "down": dn}}

    async def _move_chuck(self, ch: int, power_sw: str, move_sw: str,
                        target_lamp: str, target_name: str,
                        timeout_s: float = 60.0) -> Json:
        """
        래치 유지 + 램프만 폴링(단순화):
        - Z-POWER ON 유지 → 방향 ON 유지 → target_lamp TRUE 시 둘 다 OFF
        - 타임아웃/예외 시에도 반드시 OFF
        """
        lock = self.ctx.lock_ch1 if ch == 1 else self.ctx.lock_ch2
        async with lock:
            async with self._plc_logs_to_file_only(f"CHUCK_{target_name.upper()}_CH{ch}"):
                # 이미 목표(확정 램프 TRUE)면 즉시 OK
                try:
                    cur = await self._read_chuck_position(ch)
                    if cur["position"] == target_name:
                        return self._ok(f"CH{ch} Chuck OK — 이미 {target_name.upper()} 위치", current=cur)
                except Exception:
                    pass

                try:
                    # 래치 ON 유지
                    await self.ctx.plc.write_switch(power_sw, True)
                    await asyncio.sleep(0.2)  # 전파 여유
                    await self.ctx.plc.write_switch(move_sw, True)

                    deadline = time.monotonic() + float(timeout_s)
                    while time.monotonic() < deadline:
                        if await self.ctx.plc.read_bit(target_lamp):
                            # 성공: 즉시 OFF 후 상태 반환
                            await self.ctx.plc.write_switch(move_sw, False)
                            await self.ctx.plc.write_switch(power_sw, False)
                            cur = await self._read_chuck_position(ch)
                            return self._ok(f"CH{ch} Chuck {target_name.upper()} 도달", current=cur)
                        await asyncio.sleep(0.3)

                    # 타임아웃: OFF 후 실패
                    await self.ctx.plc.write_switch(move_sw, False)
                    await self.ctx.plc.write_switch(power_sw, False)
                    cur = await self._read_chuck_position(ch)
                    return self._fail(
                        f"CH{ch} Chuck {target_name.upper()} 타임아웃({int(timeout_s)}s) — {target_lamp}=FALSE, snapshot={cur}"
                    )
                except Exception as e:
                    # 예외: OFF 보장
                    with contextlib.suppress(Exception):
                        await self.ctx.plc.write_switch(move_sw, False)
                        await self.ctx.plc.write_switch(power_sw, False)
                    return self._fail(e)

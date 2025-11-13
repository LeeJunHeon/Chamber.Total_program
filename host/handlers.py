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
    async def _plc_command(self, tag: str):
        """
        요청(명령) 1건의 로그 파일 이름만 준비한다.
        파일명: plc_host_YYYYmmdd_HHMMSS_<TAG>.txt
        """
        safe_tag = "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in tag)
        self._plc_cmd_file = self._plc_log_dir / f"plc_host_{datetime.now():%Y%m%d_%H%M%S}_{safe_tag}.txt"
        try:
            yield
        finally:
            self._plc_cmd_file = None

    @asynccontextmanager
    async def _plc_call(self):
        """
        '한 번의 PLC I/O 구간'만 아주 짧게 보호:
        - lock_plc 획득
        - plc.log 를 파일 로거로 임시 교체
        - I/O 수행
        - 원복
        """
        plc = self.ctx.plc
        prev = getattr(plc, "log", None)
        async with self.ctx.lock_plc:
            plc.log = self._plc_file_logger
            try:
                yield
            finally:
                plc.log = prev

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
            async with self._plc_command("GET_SPUTTER_STATUS"):
                async with self._plc_call():
                    atm = await self.ctx.plc.read_bit("L_ATM")
            vacuum = (not bool(atm))

            return self._ok(state=state, vacuum=vacuum)
        except Exception as e:
            return self._fail(e)

    # ================== CH1,2/plasma cleaning 공정 제어 ==================
    async def start_sputter(self, data: Json) -> Json:
        """
        START_SPUTTER 핸들러
        - data: {"ch": 1 or 2, "recipe": "csv 경로 또는 레시피 문자열"}
        - ChamberRuntime.start_with_recipe_string(...)을 호출해서
        프리플라이트/인터락/쿨다운 결과만 응답으로 돌려준다.
        """
        # 1) 파라미터 파싱
        ch = int(data.get("ch") or 0)
        recipe = str(data.get("recipe") or "").strip()

        if ch not in (1, 2):
            return self._fail("ch는 1 또는 2만 허용합니다.")

        if not recipe:
            return self._fail("recipe가 비어 있습니다. (CSV 경로 또는 레시피 문자열 필요)")

        # 2) 해당 챔버 런타임 가져오기
        #   - ctx.ch1 / ctx.ch2를 쓰고 있다면 그걸 사용
        #   - 예전 get_chamber_runtime(ch)를 계속 쓰고 싶으면 그걸 호출해도 됨
        chamber = getattr(self.ctx, "ch1", None) if ch == 1 else getattr(self.ctx, "ch2", None)
        # 만약 self.ctx.get_chamber_runtime(ch)를 이미 구현해놨다면 이렇게 바꿔도 됨:
        # chamber = self.ctx.get_chamber_runtime(ch)

        if not chamber:
            return self._fail(f"Chamber CH{ch} runtime not ready")

        try:
            # 챔버 런타임은 이미 host handshake가 구현되어 있어
            # 프리플라이트 통과/실패가 명확히 옴
            await chamber.start_with_recipe_string(recipe)

            # 여기까지 왔다는 것은:
            #  - 프리플라이트 OK
            #  - 교차실행/쿨다운 체크 OK
            #  - 실제 공정은 런타임 내부에서 비동기로 계속 진행 중
            return self._ok("SPUTTER START OK", ch=ch)
        except Exception as e:
            # start_with_recipe_string 안에서 _host_report_start(False, reason) 이 오면
            # RuntimeError(reason)이 올라오므로 그대로 문자열만 넘겨줌
            return self._fail(str(e))

    async def start_plasma_cleaning(self, data: Json) -> Json:
        """
        START_PLASMA_CLEANING 핸들러
        - data: {"recipe": "csv 경로 또는 레시피 문자열", ...}
        - PlasmaCleaningRuntime.start_with_recipe_string(...)을 호출해서
        프리플라이트/쿨다운/교차실행 체크 결과만 돌려준다.
        """
        recipe = str(data.get("recipe") or "").strip()

        if not recipe:
            return self._fail("recipe가 비어 있습니다. (CSV 경로 또는 레시피 문자열 필요)")

        pc = getattr(self.ctx, "pc", None)
        if not pc:
            return self._fail("Plasma Cleaning runtime not ready")

        try:
            # 런타임 내부에서:
            #  - runtime_state.check_can_start("pc", 선택된 CH) 호출
            #  - IG/MFC/PLC 상태 프리플라이트
            #  - 문제 있으면 _host_report_start(False, reason) → 여기서 예외로 전달
            await pc.start_with_recipe_string(recipe)
            return self._ok("PLASMA CLEANING START OK")
        except Exception as e:
            return self._fail(str(e))

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

        async with self._plc_command("VACUUM_ON"):
            try:
                # 0) 벤트 OFF
                async with self._plc_call():
                    await self.ctx.plc.write_switch("L_VENT_SW", False)
                await asyncio.sleep(0.3)

                # 0-1) 러핑펌프 OFF 타이머 체크
                async with self._plc_call():
                    if await self.ctx.plc.read_bit("L_R_P_OFF_TIMER"):
                        return self._fail("러핑펌프 OFF 타이머 진행 중 → 잠시 후 재시도")

                # 1) 러핑펌프 ON
                async with self._plc_call():
                    await self.ctx.plc.write_switch("L_R_P_SW", True)
                await asyncio.sleep(0.3)

                # 2) 러핑밸브 인터락
                async with self._plc_call():
                    if not await self.ctx.plc.read_bit("L_R_V_인터락"):
                        return self._fail("L_R_V_인터락=FALSE → 러핑밸브 개방 불가")

                # 3) 러핑밸브 ON
                async with self._plc_call():
                    await self.ctx.plc.write_switch("L_R_V_SW", True)

                # 4) VAC_READY 폴링 (대기 동안 락 없음, '읽을 때만' 짧게)
                deadline = time.monotonic() + float(timeout_s)
                while time.monotonic() < deadline:
                    async with self._plc_call():
                        if await self.ctx.plc.read_bit("L_VAC_READY_SW"):
                            return self._ok("VACUUM_ON 완료 — L_VAC_READY_SW=TRUE")
                    await asyncio.sleep(0.5)

                # (타임아웃 사유 보강: 읽을 때만 락)
                not_ready = False
                try:
                    async with self._plc_call():
                        not_ready = await self.ctx.plc.read_bit("L_VAC_NOT_READY")
                except Exception:
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
        - 긴 대기/폴링 동안에는 PLC 락을 잡지 않도록, I/O 구간만 _plc_call()으로 감싼다.
        - 예외/타임아웃이 나도 VENT_SW를 가능한 한 False로 되돌리도록 finally 보장.
        0) L_R_V_SW=False → L_R_P_SW=False 선행 정지
        1) L_VENT_인터락 True 확인
        2) L_VENT_SW = True (벤트 시작)
        3) L_ATM == True 까지 대기 (기본 240s)
        4) L_VENT_SW = False
        """
        timeout_s = float(data.get("timeout_s", 240.0))

        async with self._plc_command("VACUUM_OFF"):  # 요청별 로그 파일명 고정
            try:
                # 0) 러핑밸브/펌프 OFF (I/O 순간만 락)
                async with self._plc_call():
                    await self.ctx.plc.write_switch("L_R_V_SW", False)
                await asyncio.sleep(0.5)  # 짧은 안정화
                async with self._plc_call():
                    await self.ctx.plc.write_switch("L_R_P_SW", False)

                # 1) 벤트 인터락 확인 (읽기 순간만 락)
                async with self._plc_call():
                    if not await self.ctx.plc.read_bit("L_VENT_인터락"):
                        return self._fail("L_VENT_인터락=FALSE → 벤트 불가")

                # 2) 벤트 ON (쓰기 순간만 락)
                async with self._plc_call():
                    await self.ctx.plc.write_switch("L_VENT_SW", True)

                # 3) L_ATM TRUE 대기 (폴링 루프는 락 없이, 읽을 때만 짧게)
                deadline = time.monotonic() + timeout_s
                while time.monotonic() < deadline:
                    async with self._plc_call():
                        if await self.ctx.plc.read_bit("L_ATM"):
                            return self._ok("VACUUM_OFF 완료 (L_ATM=TRUE)")
                    await asyncio.sleep(0.5)

                # 4) 타임아웃
                return self._fail(f"VACUUM_OFF 타임아웃: {int(timeout_s)}s 내 L_ATM TRUE 미도달 (N2 gas 부족)")

            except Exception as e:
                # 예외는 message로 그대로 전달
                return self._fail(e)

            finally:
                # (가능하면) 벤트 OFF 시도 — 실패해도 본 플로우엔 영향 없음
                with contextlib.suppress(Exception):
                    async with self._plc_call():
                        await self.ctx.plc.write_switch("L_VENT_SW", False)

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
            async with self._plc_command("4PIN_UP"):
                async with self._plc_call():
                    if not await self.ctx.plc.read_bit("L_PIN_인터락"):
                        return self._fail("L_PIN_인터락=FALSE → 4PIN_UP 불가")
                async with self._plc_call():
                    await self.ctx.plc.press_switch("L_PIN_UP_SW")
                await asyncio.sleep(wait_s)
                async with self._plc_call():
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
        wait_s = float(data.get("wait_s", 10.0))
        try:
            async with self._plc_command("4PIN_DOWN"):
                # 1) 인터락 확인
                async with self._plc_call():
                    if not await self.ctx.plc.read_bit("L_PIN_인터락"):
                        return self._fail("L_PIN_인터락=FALSE → 4PIN_DOWN 불가")

                # 2) 펄스
                async with self._plc_call():
                    await self.ctx.plc.press_switch("L_PIN_DOWN_SW")

                # 3) 대기(락 없음) → 램프 확인(읽을 때만 락)
                await asyncio.sleep(wait_s)
                async with self._plc_call():
                    lamp_ok = await self.ctx.plc.read_bit("L_PIN_DOWN_LAMP")

                return self._ok(f"4PIN_DOWN 완료 — L_PIN_DOWN_LAMP=TRUE (대기 {int(wait_s)}s)") if lamp_ok \
                    else self._fail(f"4PIN_DOWN 실패 — {int(wait_s)}s 후 L_PIN_DOWN_LAMP=FALSE")

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
        async with lock:  # CH 절차 충돌 방지는 유지
            async with self._plc_command(f"GATE_OPEN_CH{ch}"):
                try:
                    # 1) 인터락 확인 — 읽는 순간만 락
                    async with self._plc_call():
                        il = await self.ctx.plc.read_bit(interlock)
                    if not il:
                        return self._fail(f"{interlock}=FALSE → CH{ch}_GATE_OPEN 불가")

                    # 2) 펄스 — 쓰는 순간만 락
                    async with self._plc_call():
                        await self.ctx.plc.press_switch(sw)

                    # 3) 대기(락 없음)
                    await asyncio.sleep(wait_s)

                    # 4) 램프 확인 — 읽는 순간만 락
                    async with self._plc_call():
                        ok = await self.ctx.plc.read_bit(lamp)
                    return self._ok(f"CH{ch}_GATE_OPEN 완료 — {lamp}=TRUE (대기 {int(wait_s)}s)") if ok \
                        else self._fail(f"CH{ch}_GATE_OPEN 실패 — {lamp}=FALSE (대기 {int(wait_s)}s)")
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
        async with lock:  # CH 절차 충돌 방지는 유지
            async with self._plc_command(f"GATE_CLOSE_CH{ch}"):
                try:
                    # 1) 인터락 확인 — 읽는 순간만 락
                    async with self._plc_call():
                        il = await self.ctx.plc.read_bit(interlock)
                    if not il:
                        return self._fail(f"{interlock}=FALSE → CH{ch}_GATE_CLOSE 불가")

                    # 2) 스위치 펄스 — 쓰는 순간만 락
                    async with self._plc_call():
                        await self.ctx.plc.press_switch(sw)

                    # 3) 대기(락 없음)
                    await asyncio.sleep(wait_s)

                    # 4) 램프 확인 — 읽는 순간만 락
                    async with self._plc_call():
                        ok = await self.ctx.plc.read_bit(lamp)
                    return self._ok(f"CH{ch}_GATE_CLOSE 완료 — {lamp}=TRUE (대기 {int(wait_s)}s)") if ok \
                        else self._fail(f"CH{ch}_GATE_CLOSE 실패 — {lamp}=FALSE (대기 {int(wait_s)}s)")
                except Exception as e:
                    return self._fail(e)

    # ================== CH1,2 chuck 제어 ==================
    async def chuck_up(self, data: Json) -> Json:
        """
        (현재 정의 유지) CHx_CHUCK_UP = MID로 이동
        - CH1: Z_M_P_1_SW → Z_M_P_1_MID_SW → Z1_MID_LOCATION 폴링
        - CH2: Z_M_P_2_SW → Z_M_P_2_MID_SW → Z2_MID_LOCATION 폴링
        """
        ch = int(data.get("ch", 1))
        timeout_s = float(data.get("wait_s", 60.0))

        if ch == 1:
            return await self._move_chuck(
                1, "Z_M_P_1_SW", "Z_M_P_1_MID_SW", "Z1_MID_LOCATION", "mid", timeout_s
            )
        elif ch == 2:
            return await self._move_chuck(
                2, "Z_M_P_2_SW", "Z_M_P_2_MID_SW", "Z2_MID_LOCATION", "mid", timeout_s
            )
        else:
            return self._fail(f"지원하지 않는 CH: {ch}")

    async def chuck_down(self, data: Json) -> Json:
        """
        CHx_CHUCK_DOWN = 최하단 이동
        - CH1: Z_M_P_1_SW → Z_M_P_1_CCW_SW → Z1_DOWN_LOCATION 폴링
        - CH2: Z_M_P_2_SW → Z_M_P_2_CCW_SW → Z2_DOWN_LOCATION 폴링
        """
        ch = int(data.get("ch", 1))
        timeout_s = float(data.get("wait_s", 60.0))

        if ch == 1:
            return await self._move_chuck(
                1, "Z_M_P_1_SW", "Z_M_P_1_CCW_SW", "Z1_DOWN_LOCATION", "down", timeout_s
            )
        elif ch == 2:
            return await self._move_chuck(
                2, "Z_M_P_2_SW", "Z_M_P_2_CCW_SW", "Z2_DOWN_LOCATION", "down", timeout_s
            )
        else:
            return self._fail(f"지원하지 않는 CH: {ch}")

    async def _read_chuck_position(self, ch: int) -> dict:
        """
        SGN(P-주소)는 읽지 않고 램프(M-주소)만으로 위치 판정 (단순/안정).
        'position'은 램프가 정확히 하나만 TRUE일 때만 확정, 아니면 'unknown'.
        """
        if ch == 1:
            l_up, l_mid, l_dn = "Z1_UP_LOCATION", "Z1_MID_LOCATION", "Z1_DOWN_LOCATION"
        elif ch == 2:
            l_up, l_mid, l_dn = "Z2_UP_LOCATION", "Z2_MID_LOCATION", "Z2_DOWN_LOCATION"
        else:
            raise ValueError(f"지원하지 않는 CH: {ch}")

        async with self._plc_call():
            up  = bool(await self.ctx.plc.read_bit(l_up))
        async with self._plc_call():
            mid = bool(await self.ctx.plc.read_bit(l_mid))
        async with self._plc_call():
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
            async with self._plc_command(f"CHUCK_{target_name.upper()}_CH{ch}"):
                # (A) 현재 위치 확인 — 내부 read는 _plc_call()로 보호됨
                try:
                    cur = await self._read_chuck_position(ch)
                    if cur["position"] == target_name:
                        return self._ok(f"CH{ch} Chuck OK — 이미 {target_name.upper()} 위치", current=cur)
                except Exception:
                    # 위치 조회 실패는 치명적이지 않으므로 계속 진행
                    pass

                try:
                    # (B) POWER ON → MOVE ON (각각 I/O 순간만 락)
                    async with self._plc_call():
                        await self.ctx.plc.write_switch(power_sw, True)
                    await asyncio.sleep(0.2)

                    async with self._plc_call():
                        await self.ctx.plc.write_switch(move_sw, True)

                    # (C) 타겟 램프 폴링: 읽을 때만 잠깐 락
                    deadline = time.monotonic() + float(timeout_s)
                    while time.monotonic() < deadline:
                        async with self._plc_call():
                            if await self.ctx.plc.read_bit(target_lamp):
                                # 성공: OFF 묶음도 한 블록에서 원자적으로 처리
                                await self.ctx.plc.write_switch(move_sw, False)
                                await self.ctx.plc.write_switch(power_sw, False)

                        # 성공 시 상태 스냅샷 리턴
                        async with self._plc_call():
                            if await self.ctx.plc.read_bit(target_lamp):
                                cur = await self._read_chuck_position(ch)
                                return self._ok(f"CH{ch} Chuck {target_name.upper()} 도달", current=cur)

                        await asyncio.sleep(0.3)

                    # (D) 타임아웃 → OFF 후 실패 반환
                    async with self._plc_call():
                        await self.ctx.plc.write_switch(move_sw, False)
                        await self.ctx.plc.write_switch(power_sw, False)
                    cur = await self._read_chuck_position(ch)
                    return self._fail(
                        f"CH{ch} Chuck {target_name.upper()} 타임아웃({int(timeout_s)}s) — "
                        f"{target_lamp}=FALSE, snapshot={cur}"
                    )

                except Exception as e:
                    # (E) 예외 시에도 OFF 보장(묶음으로)
                    with contextlib.suppress(Exception):
                        async with self._plc_call():
                            await self.ctx.plc.write_switch(move_sw, False)
                            await self.ctx.plc.write_switch(power_sw, False)
                    return self._fail(e)

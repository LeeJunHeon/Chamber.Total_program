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
            d = root / "PLC_Remote"
            d.mkdir(parents=True, exist_ok=True)
            self._plc_log_dir = d              # 주 저장 폴더(NAS)
        except Exception as e:
            # NAS 로그 폴더 생성 실패 사유를 로그창에 출력
            try:
                self.ctx.log(
                    "PLC_REMOTE",
                    f"[PLC_REMOTE_LOG_ERROR] NAS 로그 폴더 생성 실패: {e!r} → 로컬 Logs/PLC_Remote 사용",
                )
            except Exception:
                # log() 자체가 실패해도 공정은 멈추지 않음
                pass

            d = Path.cwd() / "Logs" / "PLC_Remote"
            d.mkdir(parents=True, exist_ok=True)
            self._plc_log_dir = d              # 폴백 폴더(로컬)

        self._plc_cmd_file = None              # 요청중 파일 경로(컨텍스트 내에서만 셋)
        self._current_cmd_tag: str | None = None  # 현재 처리 중인 명령 태그(VACUUM_OFF, 4PIN_DOWN 등)

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
            # 이벤트루프가 없으면 동기로 시도하되, 실패 사유는 로그로 남김
            try:
                self._write_line_sync(file_path, line)
            except Exception as e:
                try:
                    self.ctx.log(
                        "PLC_REMOTE",
                        f"[PLC_REMOTE_LOG_ERROR] _append_line_nonblocking(sync, {file_path}): {e!r}",
                    )
                except Exception:
                    pass
            return

        async def _worker():
            # 1차: 지정 경로(NAS 우선)
            try:
                await asyncio.to_thread(self._write_line_sync, file_path, line)
                return
            except Exception as e:
                try:
                    self.ctx.log(
                        "PLC_REMOTE",
                        f"[PLC_REMOTE_LOG_ERROR] _append_line_nonblocking(async, {file_path}): {e!r}",
                    )
                except Exception:
                    pass

            # 2차: 로컬 폴백(파일명은 동일 basename)
            local = (Path.cwd() / "Logs" / "PLC_Remote" / file_path.name)
            try:
                await asyncio.to_thread(self._write_line_sync, local, line)
            except Exception as e:
                try:
                    self.ctx.log(
                        "PLC_REMOTE",
                        f"[PLC_REMOTE_LOG_ERROR] _append_line_nonblocking(async_fallback, {local}): {e!r}",
                    )
                except Exception:
                    pass

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
        except Exception as e:
            # 로깅 에러로 본체 흐름을 멈추지 않되, 사유는 로그창에 출력
            try:
                self.ctx.log(
                    "PLC_REMOTE",
                    f"[PLC_REMOTE_LOG_ERROR] _plc_file_logger 실패: {e!r}",
                )
            except Exception:
                pass

    # ===== 클라이언트 REQ/RES 로그 헬퍼 =====
    def _log_client_request(self, data: Json) -> None:
        """
        현재 PLC 명령에 대해 클라이언트에서 어떤 data를 보냈는지
        plc_host_YYYYmmdd_HHMMSS_<TAG>.txt 에 한 줄 남긴다.
        (_plc_cmd_file 이 없으면 아무 것도 하지 않음)
        """
        if not self._plc_cmd_file:
            return
        try:
            tag = self._current_cmd_tag or ""
            # _plc_file_logger 가 타임스탬프는 붙여주므로 여기서는 메시지만 넘긴다.
            self._plc_file_logger("[CLIENT_REQ] %s data=%r", tag, data)
        except Exception as e:
            # 로깅 실패는 본 플로우에 영향 주지 않지만, 사유는 로그창에 출력
            try:
                self.ctx.log(
                    "PLC_REMOTE",
                    f"[PLC_REMOTE_LOG_ERROR] _log_client_request 실패: {e!r}",
                )
            except Exception:
                pass

    def _log_client_response(self, res: Json) -> None:
        """
        현재 PLC 명령에 대해 클라이언트로 어떤 응답(Json)을 보냈는지
        같은 파일에 한 줄 남긴다.
        """
        if not self._plc_cmd_file:
            return
        try:
            tag = self._current_cmd_tag or ""
            self._plc_file_logger(
                "[CLIENT_RES] %s result=%s message=%r data=%r",
                tag, res.get("result"), res.get("message"), res,
            )
        except Exception as e:
            # 로깅 실패는 본 플로우에 영향 주지 않지만, 사유는 로그창에 출력
            try:
                self.ctx.log(
                    "PLC_REMOTE",
                    f"[PLC_REMOTE_LOG_ERROR] _log_client_response 실패: {e!r}",
                )
            except Exception:
                pass

    @asynccontextmanager
    async def _plc_command(self, tag: str):
        """
        요청(명령) 1건의 로그 파일 이름만 준비한다.
        파일명: plc_host_YYYYmmdd_HHMMSS_<TAG>.txt
        """
        safe_tag = "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in tag)
        # 현재 처리 중인 명령 태그를 기억해 두고, 해당 명령 전용 로그 파일 경로를 만든다.
        self._current_cmd_tag = tag
        self._plc_cmd_file = self._plc_log_dir / f"plc_host_{datetime.now():%Y%m%d_%H%M%S}_{safe_tag}.txt"
        try:
            yield
        finally:
            # 명령이 끝나면 컨텍스트를 정리해 준다.
            self._plc_cmd_file = None
            self._current_cmd_tag = None

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
        """성공 응답(Json)을 만들면서, 현재 PLC 명령 컨텍스트라면 응답도 로그 파일에 남긴다."""
        res: Json = {"result": "success", "message": msg, **extra}
        self._log_client_response(res)
        return res

    def _fail(self, e: Exception | str) -> Json:
        """실패 응답(Json)을 만들면서, 현재 PLC 명령 컨텍스트라면 응답도 로그 파일에 남긴다."""
        res: Json = {"result": "fail", "message": str(e)}
        self._log_client_response(res)
        return res

    # ================== 공정 중 여부 체크 헬퍼 ==================
    def _fail_if_ch_busy(self, ch: int, action: str) -> Json | None:
        """
        runtime_state를 이용해서 해당 CH에서 공정이 실행 중이면
        명령을 차단하고, 실패 응답(Json)을 돌려준다.

        - '공정'으로 보는 것:
          · chamber(ch)  : 스퍼터 공정
          · pc(ch)       : Plasma Cleaning 공정
          · tsp(0, ch=1) : CH1과 연동된 TSP 공정
        - runtime_state가 없거나 예외가 나면 차단하지 않고 그대로 진행
        """
        rs = getattr(self.ctx, "runtime_state", None)
        if rs is None:
            return None

        try:
            reasons = []

            # CHx 스퍼터 공정
            if getattr(rs, "is_running", None) and rs.is_running("chamber", ch):
                reasons.append(f"CH{ch} 스퍼터 공정 실행 중")

            # CHx Plasma Cleaning 공정
            if getattr(rs, "is_running", None) and rs.is_running("pc", ch):
                reasons.append(f"CH{ch} Plasma Cleaning 실행 중")

            # TSP는 CH1과만 연관된 글로벌 공정으로 취급
            if int(ch) == 1 and getattr(rs, "is_running", None) and rs.is_running("tsp", 0):
                reasons.append("TSP 공정 실행 중")

            if reasons:
                # 예: "CH2_GATE_OPEN 불가 — CH2 스퍼터 공정 실행 중"
                return self._fail(f"{action} 불가 — " + " / ".join(reasons))

        except Exception:
            # runtime_state 문제로 장비 조작까지 막히지 않도록, 에러 시에는 통과
            return None

        return None
    
    # ================== 내부 유틸 ==================
    def _has_chamber_delay(self) -> bool:
        """
        CH1/CH2 중 하나라도 다음 공정이 _delay_main_task 로 예약되어 있으면 True.

        - chamber_runtime._start_next_process_from_queue() 에서
          self._set_task_later("_delay_main_task", ...) 로 설정되는 Task 를 본다.
        - Task 가 존재하고 아직 done() 이 아니라면, 리스트 자동 실행이 진행 중이며
          스텝 사이 대기 상태라고 판단한다.
        """
        for attr in ("ch1", "ch2"):
            rt = getattr(self.ctx, attr, None)
            if not rt:
                continue

            try:
                t = getattr(rt, "_delay_main_task", None)
            except Exception:
                t = None

            if t is not None:
                try:
                    if not t.done():
                        return True
                except Exception:
                    # done() 호출에서 예외가 나더라도 상태 판단에는 영향 없도록 무시
                    pass

        return False

    # ================== CH1,2 상태 조회 ==================
    async def get_sputter_status(self, payload: Json) -> Json:
        """
        CH1/CH2/LoadLock 각각의 상태(idle/running/error) + 진공 여부를 한 번에 조회.
        Chamber_1 / Chamber_2 / Loadlock_Chamber / vacuum 4개 키를 돌려준다.
        """
        try:
            rs = getattr(self.ctx, "runtime_state", None)

            def _ch_state(ch: int) -> str:
                """
                단일 CH 상태 계산:
                - runtime_state.is_running("chamber", ch)가 True면 running
                - 해당 CH의 리스트 공정 딜레이(_delay_main_task)가 살아 있어도 running
                - 그 외는 idle
                - 조회 중 예외가 나면 error
                """
                running_ch = False

                # 1) runtime_state 기반 실행 여부
                try:
                    if rs is not None and getattr(rs, "is_running", None):
                        if rs.is_running("chamber", ch):
                            running_ch = True
                except Exception:
                    # 상태 조회 자체에 문제가 있으면 error
                    return "error"

                # 2) 리스트 자동 실행의 스텝 사이 대기도 running 으로 간주
                attr = f"ch{ch}"
                try:
                    rt = getattr(self.ctx, attr, None)
                    if rt is not None:
                        t = getattr(rt, "_delay_main_task", None)
                        if t is not None:
                            try:
                                if not t.done():
                                    running_ch = True
                            except Exception:
                                return "error"
                except Exception:
                    return "error"

                return "running" if running_ch else "idle"

            def _loadlock_state() -> str:
                """
                Loadlock(Plasma Cleaning) 상태 계산:
                - runtime_state.is_running("pc", ch)가 1 또는 2 중 하나라도 True면 running
                - (fallback) plasma cleaning 런타임의 is_running / _running 플래그 사용
                - 조회 중 예외가 나면 error
                """
                # 1) runtime_state 기준 (pc kind)
                try:
                    if rs is not None and getattr(rs, "is_running", None):
                        for ch in (1, 2):
                            try:
                                if rs.is_running("pc", ch):
                                    return "running"
                            except Exception:
                                # 다른 채널도 계속 확인
                                continue
                except Exception:
                    return "error"

                # 2) pc 런타임 플래그(fallback)
                try:
                    pc = getattr(self.ctx, "pc", None)
                    if pc is not None:
                        cleaning = bool(
                            getattr(pc, "is_running", getattr(pc, "_running", False))
                        )
                        return "running" if cleaning else "idle"
                except Exception:
                    return "error"

                return "idle"

            # ── CH1 / CH2 / Loadlock 상태 계산 ─────────────────────────────
            chamber_1 = _ch_state(1)
            chamber_2 = _ch_state(2)
            loadlock  = _loadlock_state()

            # ── PLC에서 진공 상태(L_ATM=FALSE)를 읽어 vacuum 여부 확인 ─────
            async with self._plc_command("GET_SPUTTER_STATUS"):
                # ⇐ 여기서 클라이언트가 보낸 payload를 같이 남겨줌
                self._log_client_request(payload)

                async with self._plc_call():
                    atm = await self.ctx.plc.read_bit("L_ATM")

                # L_ATM 이 False면 진공 유지(True)
                vacuum = (not bool(atm))

                # 통신 명세서 v3 포맷에 맞춰 응답
                return self._ok(
                    Chamber_1=chamber_1,
                    Chamber_2=chamber_2,
                    Loadlock_Chamber=loadlock,
                    vacuum=vacuum,
                )

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
        
        # 🔹 여기부터 START_SPUTTER 전용 PLC_Remote 로그 파일 생성
        async with self._plc_command(f"START_SPUTTER_CH{ch}"):
            # 클라이언트에서 넘어온 전체 data 그대로 남김
            self._log_client_request(data)

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

        # 🔹 START_PLASMA_CLEANING 전용 로그 파일 생성
        async with self._plc_command("START_PLASMA_CLEANING"):
            # 클라이언트에서 넘어온 payload 그대로 남김
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
            self._log_client_request(data)
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
            self._log_client_request(data)
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
                self._log_client_request(data)
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
                self._log_client_request(data)
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
        1) (추가) runtime_state로 공정 실행 여부 확인
        2) G_V_{ch}_인터락 == True 확인
        3) G_V_{ch}_OPEN_SW = True
        4) 5초 후 G_V_{ch}_OPEN_LAMP == True 확인
        """
        ch = int(data.get("ch", 1))
        wait_s = float(data.get("wait_s", 5.0))  # 기본 5초

        # 🔹 공정 실행 중이면 게이트 조작 금지
        busy = self._fail_if_ch_busy(ch, f"CH{ch}_GATE_OPEN")
        if busy is not None:
            return busy

        if ch == 1:
            interlock, sw, lamp = "G_V_1_인터락", "G_V_1_OPEN_SW", "G_V_1_OPEN_LAMP"
        elif ch == 2:
            interlock, sw, lamp = "G_V_2_인터락", "G_V_2_OPEN_SW", "G_V_2_OPEN_LAMP"
        else:
            return self._fail(f"지원하지 않는 CH: {ch}")

        lock = self.ctx.lock_ch1 if ch == 1 else self.ctx.lock_ch2
        async with lock:  # CH 절차 충돌 방지는 유지
            async with self._plc_command(f"GATE_OPEN_CH{ch}"):
                self._log_client_request(data)
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
                self._log_client_request(data)
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

        # 🔹 공정 실행 중이면 Chuck 조작 금지
        busy = self._fail_if_ch_busy(ch, f"CH{ch}_CHUCK_UP")
        if busy is not None:
            return busy

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
                # 클라이언트 요청에 대응되는 Chuck 이동 파라미터를 남김
                self._log_client_request({"ch": ch, "target": target_name, "timeout_s": timeout_s})
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
                        lamp_on = False

                        # 1) 램프 상태 확인 + 스위치 OFF는 한 번의 _plc_call 안에서 처리
                        async with self._plc_call():
                            lamp_on = bool(await self.ctx.plc.read_bit(target_lamp))
                            if lamp_on:
                                # 성공: OFF 묶음도 한 블록에서 원자적으로 처리
                                await self.ctx.plc.write_switch(move_sw, False)
                                await self.ctx.plc.write_switch(power_sw, False)

                        # 2) 램프가 ON이면, 락 밖에서 위치 스냅샷을 읽는다
                        if lamp_on:
                            cur = await self._read_chuck_position(ch)
                            return self._ok(
                                f"CH{ch} Chuck {target_name.upper()} 도달",
                                current=cur,
                            )

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

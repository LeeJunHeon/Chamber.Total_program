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
import asyncio, time, contextlib, os
from pathlib import Path                      # ← 추가: 경로
from datetime import datetime                 # ← 추가: 파일명 타임스탬프
from contextlib import asynccontextmanager    # ← 추가: 비동기 컨텍스트
from util.error_reporter import notify_all
from lib import config_common as cfg

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
        AsyncPLC가 호출하는 printf 스타일 로거.
        ✅ 파일 저장은 하지 않음 (하루 1개 CSV는 server.py에서 처리)
        ✅ UI 로그는 남김
        """
        try:
            msg = (fmt % args) if args else str(fmt)
            ts  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # ✅ UI에 로그 남기기
            self.ctx.log("PLC_REMOTE", f"{ts} {msg}")

        except Exception as e:
            # 로깅 에러로 본체 흐름을 멈추지 않되, 사유는 UI에 출력
            try:
                self.ctx.log("PLC_REMOTE", f"[PLC_REMOTE_LOG_ERROR] _plc_file_logger 실패: {e!r}")
            except Exception:
                pass

    # ===== 클라이언트 REQ/RES 로그 헬퍼 =====
    def _log_client_request(self, data: Json) -> None:
        """
        현재는 per-command 파일 로깅을 사용하지 않는다.
        REQ/RES 저장은 host/server.py의 CSV에서 처리한다.
        - ProcessApp  : process_host_cmd_YYYYMMDD.csv
        - RobotServer : robot_server_cmd_YYYYMMDD.csv

        (_plc_cmd_file 은 항상 None이므로 여기서는 동작하지 않음)
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
        명령 컨텍스트: 파일을 만들지 않고, 현재 명령 TAG만 유지한다.
        (명령 req/res CSV 로깅은 host/server.py에서 하루 1개 파일로 처리)
        """
        self._current_cmd_tag = tag
        try:
            yield
        finally:
            self._current_cmd_tag = None
            self._plc_cmd_file = None  # 안전하게 항상 None 유지

    @asynccontextmanager
    async def _plc_call(self):
        """
        '한 번의 PLC I/O 구간'만 아주 짧게 보호:
        - lock_plc 획득 (handlers 차원에서 PLC 호출 직렬화)
        - plc.log 를 파일 로거로 임시 교체
        - ✅ PLC watchdog(heartbeat) 잠시 pause (락 경합/불필요 reconnect 방지)
        - I/O 수행
        - 원복
        """
        plc = self.ctx.plc
        prev = getattr(plc, "log", None)

        async with self.ctx.lock_plc:
            plc.log = self._plc_file_logger

            # ✅ watchdog가 있으면 잠시 멈춤 (pause/resume 메서드 이름은 plc.py에 맞춰 조정)
            paused = False
            try:
                if hasattr(plc, "pause_watchdog") and hasattr(plc, "resume_watchdog"):
                    await plc.pause_watchdog()
                    paused = True

                yield

            finally:
                # ✅ 반드시 resume (예외 발생해도)
                if paused:
                    try:
                        await plc.resume_watchdog()
                    except Exception:
                        pass

                plc.log = prev

    # ================== 공통 응답 헬퍼 ==================
    def _ok(self, msg: str = "OK", **extra) -> Json:
        """성공 응답(Json)을 만들면서, 현재 PLC 명령 컨텍스트라면 응답도 로그 파일에 남긴다."""
        res: Json = {"result": "success", "message": msg, **extra}
        self._log_client_response(res)
        return res

    def _fail(self, e, *, code: str | None = None, src: str = "HOST") -> Json:
        # ✅ code 미지정이면 예외 객체에서 자동 추출
        if code is None and isinstance(e, Exception):
            code = getattr(e, "code", None) or getattr(e, "error_code", None)
            # KeyError는 주소맵/키 누락으로 취급
            if code is None and isinstance(e, KeyError):
                code = "E411"

        res: Json = notify_all(
            log=self.ctx.log,
            chat=getattr(self.ctx, "chat", None),
            popup=getattr(self.ctx, "popup", None),
            src=src,
            code=code,
            message=e,
        )
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
                return self._fail(f"{action} 불가 — " + " / ".join(reasons), code="E205")

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
                - runtime_state.is_running("chamber", ch) 또는 is_running("pc", ch)가 True면 running
                - 해당 CH의 리스트 공정 딜레이(_delay_main_task)가 살아 있어도 running
                - 그 외는 idle
                - 조회 중 예외가 나면 error
                """
                running_ch = False

                # 1) runtime_state 기반 실행 여부
                try:
                    if rs is not None and getattr(rs, "is_running", None):
                        if rs.is_running("chamber", ch) or rs.is_running("pc", ch):
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

                # 3) 마지막 공정 실패 이력이 남아 있으면 error
                if not running_ch:
                    try:
                        if rs is not None and getattr(rs, "has_error", None) and rs.has_error("chamber", ch):
                            return "error"
                    except Exception:
                        return "error"

                return "running" if running_ch else "idle"
            
            def _ch1_is_waiting_ig() -> bool:
                """
                CH1 공정이 IG 대기(IG 단계)인지 판정.
                - CH1 process_controller가 running이고
                - current_step.action.value == "IG_CMD" 인 동안 True
                - start 직후 current_step이 아직 None인 짧은 구간도 True 처리(원하면 False로 변경 가능)
                """
                try:
                    rt = getattr(self.ctx, "ch1", None)
                    if rt is None:
                        return False

                    pc = getattr(rt, "process_controller", None)
                    if pc is None or not bool(getattr(pc, "is_running", False)):
                        return False

                    step = getattr(pc, "current_step", None)
                    if step is None:
                        # START 직후 스텝 진입 전 구간도 'IG 대기'로 간주하여 running 유지
                        return True

                    act = getattr(step, "action", None)
                    actv = getattr(act, "value", None)
                    if actv is None:
                        actv = str(act) if act is not None else ""

                    s = str(actv).upper()

                    return ("RGA" not in s)
                except Exception:
                    return False

            def _loadlock_state() -> str:
                """
                Loadlock(Plasma Cleaning) 상태 계산:
                - runtime_state.is_running("pc", ch)가 1 또는 2 중 하나라도 True면 running
                - 마지막 PC 실패 이력이 남아 있으면 error
                - ✅ (추가) CH1 공정이 IG 단계(IG_CMD)인 동안에는 Loadlock을 running으로 "보이게" 유지
                (IG 끝나고 RGA 시작하면 자동으로 idle로 돌아감)
                - (fallback) plasma cleaning 런타임의 is_running / _running 플래그 사용
                - 조회 중 예외가 나면 error
                """
                # 1) runtime_state 기준 (pc kind)
                try:
                    if rs is not None and getattr(rs, "is_running", None):
                        # 1-1) 하나라도 실행 중이면 running
                        for ch in (1, 2):
                            try:
                                if rs.is_running("pc", ch):
                                    return "running"
                            except Exception:
                                continue

                        # 1-2) 실행 중인 PC가 없으면, 마지막 실패 이력(PC) 있으면 error
                        if getattr(rs, "has_error", None):
                            for ch in (1, 2):
                                try:
                                    if rs.has_error("pc", ch):
                                        return "error"
                                except Exception:
                                    return "error"
                except Exception:
                    return "error"

                # ✅ 1.5) CH1이 IG 대기 단계면 Loadlock을 running으로 "보이게" 강제
                # (Plasma Cleaning이 끝났고 Gate가 닫힌 뒤 CH1 공정이 시작해도 IG 동안 계속 running 유지)
                if _ch1_is_waiting_ig():
                    return "running"

                # 2) pc 런타임 플래그(fallback)
                try:
                    pc = getattr(self.ctx, "pc", None)
                    if pc is not None:
                        fn = getattr(pc, "is_running", None)

                        if callable(fn):
                            # 메서드면 호출해서 True/False를 받아야 함
                            try:
                                cleaning = bool(fn())
                            except TypeError:
                                # 혹시 시그니처가 달라 호출이 안 되면 _running으로 폴백
                                cleaning = bool(getattr(pc, "_running", False))
                        else:
                            # 속성(bool)일 수도 있으니 그대로 사용
                            cleaning = bool(fn) if isinstance(fn, bool) else bool(getattr(pc, "_running", False))

                        return "running" if cleaning else "idle"
                except Exception:
                    return "error"

                return "idle"

            # ── CH1 / CH2 / Loadlock 상태 계산 ─────────────────────────────
            chamber_1 = _ch_state(1)
            chamber_2 = _ch_state(2)
            loadlock  = _loadlock_state()

            # ✅ 단순 인터락(표시용):
            # CH1이 공정 중(running)인 동안에는 CH2/Loadlock이 idle로 보이면 로봇이 움직이므로,
            # CH2/Loadlock이 running이 아니면 running으로 "보이게" 고정한다.l
            # CH1이 idle로 바뀌면 이 조건이 풀리면서 원래 상태(대개 idle)로 돌아간다.
            # if chamber_1 == "running":
            #     if chamber_2 != "running":
            #         chamber_2 = "running"
            #     if loadlock != "running":
            #         loadlock = "running"

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
            return self._fail(f"GET_SPUTTER_STATUS 실패: {type(e).__name__}: {e}", code=getattr(e, "code", None))
        
    # ================== 레시피 조회 ==========================
    async def get_recipe(self, data: Json) -> Json:
        """
        GET_RECIPE
        - data: {"folder": "CH1" | "CH2" | "ALD"}
        - 루트(ROBOT_RECIPE_ROOT_DIR) 아래의 해당 폴더만 스캔 (재귀 없음)
        - .csv 파일명 리스트 반환
        """
        try:
            folder = str(data.get("folder") or "").strip().upper()
            allowed = tuple(getattr(cfg, "ROBOT_RECIPE_FOLDERS", ("CH1", "CH2", "ALD")))
            if folder not in allowed:
                return self._fail(f"folder는 {allowed} 중 하나여야 합니다. (입력={folder!r})", code="E226")

            base_dir = Path(getattr(cfg, "ROBOT_RECIPE_ROOT_DIR"))
            target_dir = base_dir / folder
            timeout_s = float(getattr(cfg, "RECIPE_SCAN_TIMEOUT_S", 8.0))

            def _scan_sync() -> list[str]:
                if not target_dir.exists():
                    raise FileNotFoundError(f"Recipe folder not found: {target_dir}")
                if not target_dir.is_dir():
                    raise NotADirectoryError(f"Not a directory: {target_dir}")

                files: list[str] = []
                with os.scandir(target_dir) as it:
                    for ent in it:
                        if ent.is_file() and ent.name.lower().endswith(".csv"):
                            files.append(ent.name)
                files.sort(key=str.lower)
                return files

            files = await asyncio.wait_for(asyncio.to_thread(_scan_sync), timeout=timeout_s)

            return self._ok("OK", base_dir=str(base_dir), folder=folder, files=files, count=len(files))

        except asyncio.TimeoutError:
            return self._fail(f"GET_RECIPE timeout ({getattr(cfg, 'RECIPE_SCAN_TIMEOUT_S', 8.0)}s)", code="E227")
        except Exception as e:
            return self._fail(e)
        
    # ================== Loading Sensor 조회 ==================
    async def get_loading_1_sensor(self, payload: Json) -> Json:
        # GET_LOADING_1_SENSOR: LOADING_1_SENSOR_LAMP (M00300)
        return await self._get_loading_sensor(which=1, payload=payload)

    async def get_loading_2_sensor(self, payload: Json) -> Json:
        # GET_LOADING_2_SENSOR: LOADING_2_SENSOR_LAMP (M00301)
        return await self._get_loading_sensor(which=2, payload=payload)

    async def _get_loading_sensor(self, *, which: int, payload: Json | None = None) -> Json:
        try:
            which = int(which)
            key = "LOADING_1_SENSOR_LAMP" if which == 1 else "LOADING_2_SENSOR_LAMP"

            plc = self.ctx.plc

            try:
                async with self._plc_call():
                    v = await plc.read_bit(key)
            except KeyError as e:
                return self._fail(f"PLC 주소맵에 {key}가 없습니다: {e}", code="E411")
            except Exception as e:
                # ✅ PLC 통신 실패(E401/E402/E403)를 그대로 반영
                return self._fail(f"{key} 읽기 실패: {type(e).__name__}: {e}", code=getattr(e, "code", None) or "E412")

            async with self._plc_command(f"GET_LOADING_{which}_SENSOR"):
                # ✅ 클라이언트 요청 payload도 기록
                self._log_client_request(payload or {})

            return self._ok("OK", value=bool(v))

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
            return self._fail("ch는 1 또는 2만 허용합니다.", code="E201")

        if not recipe:
            return self._fail("recipe가 비어 있습니다. (CSV 경로 또는 레시피 문자열 필요)", code="E202")

        # 2) 해당 챔버 런타임 가져오기
        #   - ctx.ch1 / ctx.ch2를 쓰고 있다면 그걸 사용
        #   - 예전 get_chamber_runtime(ch)를 계속 쓰고 싶으면 그걸 호출해도 됨
        chamber = getattr(self.ctx, "ch1", None) if ch == 1 else getattr(self.ctx, "ch2", None)
        # 만약 self.ctx.get_chamber_runtime(ch)를 이미 구현해놨다면 이렇게 바꿔도 됨:
        # chamber = self.ctx.get_chamber_runtime(ch)

        if not chamber:
            return self._fail(f"Chamber CH{ch} runtime not ready", code="E203")
        
        busy = self._fail_if_ch_busy(ch, f"START_SPUTTER_CH{ch}")
        if busy is not None:
            return busy

        # ✅ CH별 절차 충돌 방지 락
        lock = self.ctx.lock_ch1 if ch == 1 else self.ctx.lock_ch2
        async with lock:
            async with self._plc_command(f"START_SPUTTER_CH{ch}"):
                self._log_client_request(data)

                try:
                    st = await self._read_gate_state(ch)
                except KeyError as e:
                    return self._fail(f"PLC 주소맵에 gate lamp 키가 없습니다: {e}", code="E411")
                except Exception as e:
                    # ✅ PLCError면 e.code(E401/E402/E403)가 자동으로 들어가게 됨(_fail 개선 덕분)
                    return self._fail(f"Gate 상태 조회 실패: {type(e).__name__}: {e}", code=getattr(e, "code", None) or "E412")

                if st["state"] != "closed":
                    return self._fail(f"START_SPUTTER 불가 — CH{ch} gate가 CLOSED가 아님({st['state']})", code="E301")

                try:
                    await chamber.start_with_recipe_string(recipe)
                    return self._ok("SPUTTER START OK", ch=ch)
                except Exception as e:
                    code = getattr(e, "code", None) or getattr(e, "error_code", None)
                    msg = getattr(e, "message", None) or str(e)
                    return self._fail(msg, code=code)

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
            self._log_client_request(data)

            try:
                # 런타임 내부에서:
                #  - runtime_state.check_can_start("pc", 선택된 CH) 호출
                #  - IG/MFC/PLC 상태 프리플라이트
                #  - 문제 있으면 _host_report_start(False, reason) → 여기서 예외로 전달
                await pc.start_with_recipe_string(recipe)
                return self._ok("PLASMA CLEANING START OK")
            except Exception as e:
                code = getattr(e, "code", None) or getattr(e, "error_code", None)
                msg = getattr(e, "message", None) or str(e)
                return self._fail(msg, code=code)

    # ================== LoadLock vacuum 제어 ==================
    async def _read_gate_state(self, ch: int) -> dict:
        """
        게이트 램프 기반 상태 판정.
        - closed: CLOSE_LAMP=True & OPEN_LAMP=False
        - open  : OPEN_LAMP=True  & CLOSE_LAMP=False
        - moving_or_unknown: 둘 다 False
        - invalid_both_true: 둘 다 True (배선/맵/PLC 로직 이상 가능)
        """
        if ch not in (1, 2):
            raise ValueError(f"지원하지 않는 CH: {ch}")

        open_key = f"G_V_{ch}_OPEN_LAMP"
        close_key = f"G_V_{ch}_CLOSE_LAMP"

        async with self._plc_call():
            open_lamp = bool(await self.ctx.plc.read_bit(open_key))
            close_lamp = bool(await self.ctx.plc.read_bit(close_key))

        if close_lamp and (not open_lamp):
            state = "closed"
        elif open_lamp and (not close_lamp):
            state = "open"
        elif (not open_lamp) and (not close_lamp):
            state = "moving_or_unknown"
        else:
            state = "invalid_both_true"

        return {"ch": ch, "state": state, "open_lamp": open_lamp, "close_lamp": close_lamp}
    
    async def _require_gates_closed(self) -> tuple[bool, str, str | None]:
        """
        CH1, CH2 모두 gate가 '닫힘' 상태인지 확인.
        - 하나라도 open / moving / unknown / invalid 이면 VACUUM_ON/OFF 진행 금지
        """
        for ch in (1, 2):
            try:
                st = await self._read_gate_state(ch)
            except KeyError as e:
                return False, f"PLC 주소맵에 gate lamp 키가 없습니다: {e}", "E411"
            except Exception as e:
                return False, f"Gate 상태 조회 실패: {type(e).__name__}: {e}", (getattr(e, "code", None) or "E412")

            if st.get("state") != "closed":
                return False, f"VACUUM_ON/OFF 불가 — CH{ch} gate가 CLOSED가 아님({st.get('state')})", "E301"

        return True, "CH1/CH2 gate 모두 CLOSED", None
    
    async def _read_loadlock_state_for_gate_open(self) -> dict:
        """
        Gate Open 전에 확인할 Loadlock 상태 스냅샷.
        """
        async with self._plc_call():
            return {
                "L_VENT_SW": bool(await self.ctx.plc.read_bit("L_VENT_SW")),
                "L_R_P_SW":  bool(await self.ctx.plc.read_bit("L_R_P_SW")),
                "L_R_V_SW":  bool(await self.ctx.plc.read_bit("L_R_V_SW")),
                "L_ATM":     bool(await self.ctx.plc.read_bit("L_ATM")),
            }

    async def _require_loadlock_safe_for_gate_open(self) -> tuple[bool, str]:
        """
        Gate Open 전에 Loadlock이 vacuum on/off 전환 상태가 아닌지 확인.
        조건(요구사항):
        - L_VENT_SW, L_R_P_SW, L_R_V_SW, L_ATM 중 하나라도 TRUE면 금지
        - L_VAC_READY_SW는 제외(검사하지 않음)
        """
        s = await self._read_loadlock_state_for_gate_open()
        bad = [k for k, v in s.items() if v]
        if bad:
            detail = ", ".join([f"{k}=TRUE" for k in bad])
            return False, f"Loadlock 상태로 인해 GATE_OPEN 불가 ({detail})"
        return True, "Loadlock 상태 OK"

    async def vacuum_on(self, data: Json) -> Json:
        """
        VACUUM ON 시퀀스:
        0) L_VENT_SW = False 선행 정지
        1) L_R_P_SW = True  (러핑펌프 ON)
        2) L_R_V_인터락 == True 확인
        3) L_R_V_SW = True  (러핑밸브 ON)
        4) L_VAC_READY_SW == True 까지 대기 (기본 600s)
        
        ✅ 추가:
        - 시작 전에 gate가 close인지 확인하고, 확인되면 다음 단계로 진행
        (옵션 없음: gate가 CLOSED가 아니면 즉시 실패 반환)
        - 실패/예외/타임아웃 포함 어떤 경로든 L_R_P_SW/L_R_V_SW OFF 원복 보장
        """
        timeout_s = float(data.get("timeout_s", 600.0))  # 기본 10분

        async with self._plc_command("VACUUM_ON"):
            self._log_client_request(data)

            success = False
            try:
                # ✅ gate_open 레이스 방지: loadlock 스위치 ON 전까지만 잠깐 락
                async with self.ctx.lock_ch1:
                    async with self.ctx.lock_ch2:
                        ok, msg, code = await self._require_gates_closed()
                        if not ok:
                            return self._fail(msg, code=code)

                        # 0) 벤트 OFF
                        async with self._plc_call():
                            await self.ctx.plc.write_switch("L_VENT_SW", False)
                        await asyncio.sleep(0.3)

                        # 0-1) 러핑펌프 OFF 타이머 체크
                        async with self._plc_call():
                            if await self.ctx.plc.read_bit("L_R_P_OFF_TIMER"):
                                return self._fail("러핑펌프 OFF 타이머 진행 중 → 잠시 후 재시도", code="E309")

                        # 1) 러핑펌프 ON  ← 여기까지 오면 gate_open이 이제 확실히 차단됨(L_R_P_SW TRUE)
                        async with self._plc_call():
                            await self.ctx.plc.write_switch("L_R_P_SW", True)

                # ✅ 펌프 기동 안정화 텀 (3초)
                await asyncio.sleep(3.0)

                # 2) 러핑밸브 인터락
                async with self._plc_call():
                    if not await self.ctx.plc.read_bit("L_R_V_인터락"):
                        return self._fail("L_R_V_인터락=FALSE → 러핑밸브 개방 불가", code="E310")

                # 3) 러핑밸브 ON
                async with self._plc_call():
                    await self.ctx.plc.write_switch("L_R_V_SW", True)

                # 4) VAC_READY + 러핑펌프/밸브 OFF 상태까지 폴링
                deadline = time.monotonic() + float(timeout_s)
                while time.monotonic() < deadline:
                    async with self._plc_call():
                        vac_ready = await self.ctx.plc.read_bit("L_VAC_READY_SW")
                        pump_sw  = await self.ctx.plc.read_bit("L_R_P_SW")
                        valve_sw = await self.ctx.plc.read_bit("L_R_V_SW")

                    # 조건:
                    # 1) L_VAC_READY_SW == TRUE
                    # 2) L_R_P_SW == FALSE  (러핑펌프 스위치 OFF)
                    # 3) L_R_V_SW == FALSE  (러핑밸브 스위치 OFF)
                    if vac_ready and (not pump_sw) and (not valve_sw):
                        success = True
                        return self._ok("VACUUM_ON 완료 — VAC_READY && L_R_P_SW/L_R_V_SW=FALSE 확인")

                    await asyncio.sleep(0.5)

                # (타임아웃 사유 보강: 읽을 때만 락)
                not_ready = False
                try:
                    async with self._plc_call():
                        not_ready = await self.ctx.plc.read_bit("L_VAC_NOT_READY")
                except Exception:
                    pass

                return self._fail(
                    f"VACUUM_ON 타임아웃: {int(timeout_s)}s 내 "
                    f"L_VAC_READY_SW && 펌프/밸브 OFF 상태 미도달 "
                    f"(L_VAC_NOT_READY={not_ready}) — door/밸브 상태 확인",
                    code="E312",
                )

            except Exception as e:
                # 예외 사유는 message로 그대로 클라이언트 전달
                return self._fail(e)
            
            finally:
                # ✅ 원복: 실패면 밸브 OFF → (락 밖에서) 3초 → 펌프 OFF
                # - gate가 열려있거나 인터락 실패/타임아웃 등으로 중간 종료돼도
                #   러핑펌프/밸브가 켜진 채로 남지 않게 함
                if not success:
                    with contextlib.suppress(Exception):
                        async with self._plc_call():
                            await self.ctx.plc.write_switch("L_R_V_SW", False)
                    await asyncio.sleep(3.0)  # ✅ 락 밖
                    with contextlib.suppress(Exception):
                        async with self._plc_call():
                            await self.ctx.plc.write_switch("L_R_P_SW", False)

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

        async with self._plc_command("VACUUM_OFF"):
            self._log_client_request(data)

            success = False
            try:
                # ✅ gate_open 레이스 방지: VENT_SW TRUE 쓰기 전까지만 잠깐 락
                async with self.ctx.lock_ch1:
                    async with self.ctx.lock_ch2:
                        ok, msg, code = await self._require_gates_closed()
                        if not ok:
                            return self._fail(msg, code=code)
                        
                        # ✅ (추가) 0) 이미 대기압이면(L_ATM=TRUE) 즉시 성공 응답 (인터락은 이미 확인함)
                        async with self._plc_call():
                            atm_now = bool(await self.ctx.plc.read_bit("L_ATM"))
                        if atm_now:
                            # ✅ 이미 대기압이어도 'VACUUM_OFF 종료 상태'를 맞춰주고 응답
                            async with self._plc_call():
                                await self.ctx.plc.write_switch("L_VENT_SW", False)
                                await self.ctx.plc.write_switch("L_R_V_SW", False)
                            await asyncio.sleep(3.0)
                            async with self._plc_call():
                                await self.ctx.plc.write_switch("L_R_P_SW", False)

                            success = True
                            return self._ok("VACUUM_OFF: 이미 대기압 상태 (L_ATM=TRUE)")

                        # 0) 러핑밸브/펌프 OFF
                        async with self._plc_call():
                            await self.ctx.plc.write_switch("L_R_V_SW", False)

                        await asyncio.sleep(3.0)

                        async with self._plc_call():
                            await self.ctx.plc.write_switch("L_R_P_SW", False)

                        # 1) 벤트 인터락 확인
                        async with self._plc_call():
                            if not await self.ctx.plc.read_bit("L_VENT_인터락"):
                                return self._fail("L_VENT_인터락=FALSE → 벤트 불가")

                        # 2) 벤트 ON  ← 여기까지 오면 gate_open이 이제 확실히 차단됨(L_VENT_SW TRUE)
                        async with self._plc_call():
                            await self.ctx.plc.write_switch("L_VENT_SW", True)

                # 3) L_ATM TRUE 대기 (폴링 루프는 락 없이, 읽을 때만 짧게)
                deadline = time.monotonic() + timeout_s
                while time.monotonic() < deadline:
                    async with self._plc_call():
                        atm = await self.ctx.plc.read_bit("L_ATM")

                    if atm:
                        # 3-1) 진공 해제 완료 → 벤트 밸브 닫기
                        async with self._plc_call():
                            await self.ctx.plc.write_switch("L_VENT_SW", False)
                        success = True
                        # 3-2) 벤트 OFF까지 처리된 후에 성공 응답
                        return self._ok("VACUUM_OFF 완료 (L_ATM=TRUE, L_VENT_SW=FALSE)")

                    await asyncio.sleep(0.5)

                # 4) 타임아웃 → 벤트 OFF 시도 후 실패 응답
                with contextlib.suppress(Exception):
                    async with self._plc_call():
                        await self.ctx.plc.write_switch("L_VENT_SW", False)

                return self._fail(
                    f"VACUUM_OFF 타임아웃: {int(timeout_s)}s 내 L_ATM TRUE 미도달 (N2 gas 부족)",
                    code="E313",
                )

            except Exception as e:
                return self._fail(e)
            
            finally:
                # ✅ 실패/예외면 벤트 밸브가 열려있는 채로 남지 않게 강제 OFF
                if not success:
                    with contextlib.suppress(Exception):
                        async with self._plc_call():
                            await self.ctx.plc.write_switch("L_VENT_SW", False)

    # ================== LoadLock 4pin 제어 ==================
    async def four_pin_up(self, data: Json) -> Json:
        """
        4PIN_UP 시퀀스:
        1) L_PIN_인터락 == True 확인
        2) L_PIN_UP_SW 펄스
        3) wait_s 동안 L_PIN_UP_LAMP 를 1초 간격으로 폴링 → TRUE 되면 즉시 성공
        """
        wait_s = float(data.get("wait_s", 20.0))  # 전체 타임아웃
        poll_s = float(data.get("poll_s", 1.0))   # ✅ 1초에 1번
        settle_s = float(data.get("settle_s", 5.0))  # ✅ 펄스 후 대기(기본 5초)

        try:
            async with self._plc_command("4PIN_UP"):
                self._log_client_request(data)

                # 1) 인터락 확인
                async with self._plc_call():
                    if not await self.ctx.plc.read_bit("L_PIN_인터락"):
                        return self._fail("L_PIN_인터락=FALSE → 4PIN_UP 불가", code="E314")
                    
                # 2) 펄스
                async with self._plc_call():
                    await self.ctx.plc.press_switch("L_PIN_UP_SW")

                # ✅ 펄스 후 바로 읽지 말고 5초 대기
                await asyncio.sleep(settle_s)

                # 3) ✅ 램프 폴링(1초마다)
                lamp_ok = await self._poll_bit_until_true(
                    "L_PIN_UP_LAMP",
                    timeout_s=wait_s,
                    interval_s=poll_s,
                )

                if lamp_ok:
                    return self._ok(f"4PIN_UP 완료 — L_PIN_UP_LAMP=TRUE (timeout {int(wait_s)}s, poll {poll_s:.1f}s)")
                return self._fail(
                    f"4PIN_UP 실패 — {int(wait_s)}s 내 L_PIN_UP_LAMP=TRUE 미도달 (poll {poll_s:.1f}s)",
                    code="E316",
                )

        except Exception as e:
            return self._fail(e)

    async def four_pin_down(self, data: Json) -> Json:
        """
        4PIN_DOWN 시퀀스:
        1) L_PIN_인터락 == True 확인
        2) L_PIN_DOWN_SW 펄스
        3) wait_s 동안 L_PIN_DOWN_LAMP 를 1초 간격으로 폴링 → TRUE 되면 즉시 성공
        """
        wait_s = float(data.get("wait_s", 20.0))
        poll_s = float(data.get("poll_s", 1.0))    # ✅ 1초에 1번
        settle_s = float(data.get("settle_s", 5.0))  # ✅ 기본 5초

        try:
            async with self._plc_command("4PIN_DOWN"):
                self._log_client_request(data)

                # 1) 인터락 확인
                async with self._plc_call():
                    if not await self.ctx.plc.read_bit("L_PIN_인터락"):
                        return self._fail("L_PIN_인터락=FALSE → 4PIN_DOWN 불가", code="E315")

                # 2) 펄스
                async with self._plc_call():
                    await self.ctx.plc.press_switch("L_PIN_DOWN_SW")

                # ✅ 펄스 후 바로 읽지 말고 5초(기본) 대기
                await asyncio.sleep(settle_s)

                # 3) ✅ 램프 폴링(1초마다)
                lamp_ok = await self._poll_bit_until_true(
                    "L_PIN_DOWN_LAMP",
                    timeout_s=wait_s,
                    interval_s=poll_s,
                )   

                return self._ok(f"4PIN_DOWN 완료 — L_PIN_DOWN_LAMP=TRUE (timeout {int(wait_s)}s, poll {poll_s:.1f}s)") if lamp_ok \
                    else self._fail(
                        f"4PIN_DOWN 실패 — {int(wait_s)}s 내 L_PIN_DOWN_LAMP=TRUE 미도달 (poll {poll_s:.1f}s)",
                        code="E317",
                    )

        except Exception as e:
            return self._fail(e)
        
    async def _poll_bit_until_true(self, bit_name: str, *, timeout_s: float, interval_s: float = 1.0) -> bool:
        """
        timeout_s 동안 interval_s 간격으로 bit_name을 폴링.
        - TRUE 되는 순간 즉시 True 반환
        - 끝까지 TRUE가 안 되면 False 반환
        - PLC 락은 '읽는 순간'에만 _plc_call()로 짧게 잡는다 (chuck과 동일한 철학)
        """
        deadline = time.monotonic() + float(timeout_s)

        while True:
            # 읽는 순간만 락
            async with self._plc_call():
                v = bool(await self.ctx.plc.read_bit(bit_name))

            if v:
                return True

            now = time.monotonic()
            if now >= deadline:
                return False

            # 남은 시간이 interval보다 짧으면 그만큼만 sleep (마지막 근접 샘플링 보장)
            await asyncio.sleep(min(float(interval_s), deadline - now))

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

        try:
            async with self.ctx.lock_ch1:
                async with self.ctx.lock_ch2:
                    async with self._plc_command(f"GATE_OPEN_CH{ch}"):
                        self._log_client_request(data)

                        # ✅ (추가) 0) gate lamp 먼저 확인: 이미 OPEN이면 즉시 OK (불필요 동작 방지)
                        cur_st = await self._read_gate_state(ch)
                        if cur_st["state"] == "open":
                            return self._ok(f"CH{ch}_GATE_OPEN: 이미 OPEN 상태", current=cur_st)
                        if cur_st["state"] == "invalid_both_true":
                            return self._fail(
                                f"CH{ch} gate lamp 이상(OPEN/CLOSE 모두 TRUE): {cur_st}",
                                code="E306",
                            )
                        
                        # ✅ (추가-1) Loadlock이 vacuum on/off 전환 상태인지 체크
                        ok_ll, msg_ll = await self._require_loadlock_safe_for_gate_open()
                        if not ok_ll:
                            return self._fail(msg_ll, code="E321") # ✅ Loadlock 상태로 Gate Open 차단

                        # ✅ (추가-2) 다른 챔버 gate가 열려있거나(또는 closed가 아니면) 금지
                        other = 2 if ch == 1 else 1
                        other_st = await self._read_gate_state(other)
                        if other_st["state"] != "closed":
                            return self._fail(
                                f"다른 챔버 Gate가 CLOSED가 아님: CH{other}={other_st['state']} → CH{ch}_GATE_OPEN 불가",
                                code="E303",
                            )

                        # 1) 인터락 확인 — 읽는 순간만 락
                        async with self._plc_call():
                            il = await self.ctx.plc.read_bit(interlock)
                        if not il:
                            return self._fail(f"{interlock}=FALSE → CH{ch}_GATE_OPEN 불가", code="E302")

                        # 2) 펄스 — 쓰는 순간만 락
                        async with self._plc_call():
                            await self.ctx.plc.press_switch(sw)

                        # 3) 대기(락 없음)
                        await asyncio.sleep(wait_s)

                        # 4) 램프 확인 — 읽는 순간만 락
                        async with self._plc_call():
                            ok = await self.ctx.plc.read_bit(lamp)
                        return self._ok(f"CH{ch}_GATE_OPEN 완료 — {lamp}=TRUE (대기 {int(wait_s)}s)") if ok \
                            else self._fail(f"CH{ch}_GATE_OPEN 실패 — {lamp}=FALSE (대기 {int(wait_s)}s)", code="E304")
        except Exception as e:
            # gate_open에서 예외는 대부분 PLC I/O/상태조회 계열 → E412로 정규화
            return self._fail(
                f"CH{ch}_GATE_OPEN 처리 중 예외: {type(e).__name__}: {e}",
                code=getattr(e, "code", None) or "E412",
            )

    async def gate_close(self, data: Json) -> Json:
        """
        CHx_GATE_CLOSE 시퀀스:
        1) G_V_{ch}_CLOSE_SW 펄스
        2) wait_s 후 G_V_{ch}_CLOSE_LAMP 확인
        (※ gate close는 interlock 체크를 하지 않도록 설계)
        """
        ch = int(data.get("ch", 1))
        wait_s = float(data.get("wait_s", 5.0))  # 기본 5초

        # ✅ (추가) 공정 실행 중이면 게이트 조작 금지 (gate_open과 동일 철학)
        busy = self._fail_if_ch_busy(ch, f"CH{ch}_GATE_CLOSE")
        if busy is not None:
            return busy

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
                    # ✅ (추가) 0) gate lamp 먼저 확인: 이미 CLOSED면 즉시 OK
                    cur_st = await self._read_gate_state(ch)
                    if cur_st["state"] == "closed":
                        return self._ok(f"CH{ch}_GATE_CLOSE: 이미 CLOSED 상태", current=cur_st)
                    if cur_st["state"] == "invalid_both_true":
                        return self._fail(
                            f"CH{ch} gate lamp 이상(OPEN/CLOSE 모두 TRUE): {cur_st}",
                            code="E306",
                        )

                    # 1) 스위치 펄스 — 쓰는 순간만 락
                    async with self._plc_call():
                        await self.ctx.plc.press_switch(sw)

                    # 2) 대기(락 없음)
                    await asyncio.sleep(wait_s)

                    # 3) 램프 확인 — 읽는 순간만 락
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
        timeout_s = float(data.get("wait_s", 90.0))

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
        timeout_s = float(data.get("wait_s", 90.0))

        # 🔹 공정 실행 중이면 Chuck 조작 금지 (chuck_up과 동일하게)
        busy = self._fail_if_ch_busy(ch, f"CH{ch}_CHUCK_DOWN")
        if busy is not None:
            return busy

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
            async with self._plc_command(f"CHUCK_{target_name.upper()}_CH{ch}"):
                # 클라이언트 요청에 대응되는 Chuck 이동 파라미터를 남김
                self._log_client_request({"ch": ch, "target": target_name, "timeout_s": timeout_s})
                # (A) 현재 위치 확인 — 내부 read는 _plc_call()로 보호됨
                try:
                    cur = await self._read_chuck_position(ch)
                except Exception as e:
                    return self._fail(
                        f"CH{ch} Chuck 위치 조회 실패: {type(e).__name__}: {e}",
                        code="E412",
                    )
                # chuck이 이미 목표 위치면 즉시 성공 응답
                if cur["position"] == target_name:
                    return self._ok(f"CH{ch} Chuck OK — 이미 {target_name.upper()} 위치", current=cur)
                
                # ✅ 핵심: 위치 불명(UP/MID/DOWN 모두 OFF 또는 2개 이상 ON) 상태에서 MID 자동은 실패 확률 높음
                #    → 오래 기다리지 말고 즉시 원인 명확하게 실패 처리
                if target_name == "mid" and cur["position"] == "unknown":
                    return self._fail(
                        f"CH{ch} Chuck 위치 불명(UP/MID/DOWN 모두 OFF 또는 중복 ON) → MID 이동 불가. "
                        f"먼저 CH{ch}_CHUCK_DOWN 등으로 위치를 확정한 뒤 재시도. snapshot={cur}",
                        code="E318",
                    )

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

                        await asyncio.sleep(1)

                    # (D) 타임아웃 → OFF 후 실패 반환
                    async with self._plc_call():
                        await self.ctx.plc.write_switch(move_sw, False)
                        await self.ctx.plc.write_switch(power_sw, False)
                    cur = await self._read_chuck_position(ch)
                    return self._fail(
                        f"CH{ch} Chuck {target_name.upper()} 타임아웃({int(timeout_s)}s) — "
                        f"{target_lamp}=FALSE, snapshot={cur}",
                        code="E318",
                    )

                except Exception as e:
                    # (E) 예외 시에도 OFF 보장(묶음으로)
                    with contextlib.suppress(Exception):
                        async with self._plc_call():
                            await self.ctx.plc.write_switch(move_sw, False)
                            await self.ctx.plc.write_switch(power_sw, False)
                    return self._fail(
                        f"CH{ch} Chuck {target_name.upper()} 처리 중 예외: {type(e).__name__}: {e}",
                        code="E412",
                    )

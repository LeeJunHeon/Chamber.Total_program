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
from contextvars import ContextVar             # ✅ 추가: 코루틴별 독립 태그 저장
from pathlib import Path                      # ← 추가: 경로
from datetime import datetime                 # ← 추가: 파일명 타임스탬프
from contextlib import asynccontextmanager    # ← 추가: 비동기 컨텍스트
from errors.error_reporter import (
    notify_error_code,
    notify_handler_error,
)
from lib import config_common as cfg

Json = Dict[str, Any]

# ✅ 코루틴별 독립 태그 저장소 (race 방지)
# 인스턴스 변수는 동시 요청 시 서로 덮어쓰는 문제가 있어,
# 비동기 코루틴마다 자기만의 슬롯을 제공하는 ContextVar로 변경.
_cmd_tag_var: ContextVar[str | None] = ContextVar("plc_cmd_tag", default=None)

class HostHandlers:
    def __init__(self, ctx: HostContext) -> None:
        self.ctx = ctx

        # ================== 로그 저장 헬퍼 ==================
        # NAS 우선, 실패 시 로컬 폴백 디렉터리 준비
        try:
            root = Path(r"\\VanaM_NAS\VanaM_Sputter\Sputter\Logs\CH1&2")
            d = root / "PLC_Remote"
            d.mkdir(parents=True, exist_ok=True)
            self._plc_log_dir = d              # 주 저장 폴더(NAS)
        except Exception as e:
            # NAS 로그 폴더 생성 실패 사유를 로그창에 출력
            try:
                self.ctx.log(
                    "PLC_REMOTE",
                    f"[PLC_REMOTE_LOG_ERROR] NAS 로그 폴더 생성 실패: {e!r} → 로컬 Logs/CH1&2/PLC_Remote 사용",
                )
            except Exception:
                # log() 자체가 실패해도 공정은 멈추지 않음
                pass

            d = Path.cwd() / "Logs" / "CH1&2" / "PLC_Remote"
            d.mkdir(parents=True, exist_ok=True)
            self._plc_log_dir = d              # 폴백 폴더(로컬)

        self._plc_cmd_file = None              # 호스트 명령 파일(파일을 만들지는 않음)
        # ⚠ _current_cmd_tag 는 인스턴스 변수가 아니라 property로 ContextVar 라우팅
        # (모듈 상단의 _cmd_tag_var 참조) — 동시 요청 시 태그 오염 방지

        # Loadlock 전환(VACUUM_ON / VACUUM_OFF) 동시 진입 방지
        self._loadlock_transition_lock = asyncio.Lock()
        self._loadlock_transition_tag: str | None = None

        # Gate OPEN/CLOSE 직렬화 락(채널 공용)
        self._loadlock_gate_lock = asyncio.Lock()

    # ✅ ContextVar property — 기존 코드(self._current_cmd_tag = X / tag = self._current_cmd_tag)가
    #    그대로 동작하지만, 내부적으로는 코루틴별 독립 슬롯에 저장/조회된다.
    @property
    def _current_cmd_tag(self) -> str | None:
        return _cmd_tag_var.get()

    @_current_cmd_tag.setter
    def _current_cmd_tag(self, v: str | None) -> None:
        _cmd_tag_var.set(v)

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
            local = (Path.cwd() / "Logs" / "CH1&2" / "PLC_Remote" / file_path.name)
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
            # - 현재 처리 중인 명령 태그가 있으면 같이 붙인다.
            # - 예: "2026-01-30 12:00:00 [GET_SPUTTER_STATUS] read L_ATM (addr=99) -> True"
            tag = (self._current_cmd_tag or "").strip()
            if tag:
                self.ctx.log("PLC_REMOTE", f"{ts} [{tag}] {msg}")
            else:
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
        prev_tag = self._current_cmd_tag  # 중첩 호출 시 이전 태그 보존
        self._current_cmd_tag = tag
        try:
            yield
        finally:
            self._current_cmd_tag = prev_tag  # None 대신 이전 값으로 복원
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
        """
        표준 fail payload를 만들고 log/chat/popup 및 응답 로그에 반영한다.

        규칙:
        - code가 명시되면 notify_error_code 사용
        - code가 없고 예외 객체면 notify_handler_error 사용
        - KeyError는 주소맵/키 누락 계열로 E411 우선 부여
        """
        # 이미 fail payload면 그대로 사용
        if isinstance(e, dict) and e.get("result") == "fail":
            self._log_client_response(e)
            return e

        # 1) code 자동 추출
        if code is None and isinstance(e, BaseException):
            code = getattr(e, "code", None) or getattr(e, "error_code", None)
            if code is None and isinstance(e, KeyError):
                code = "E411"

        # 2) detail 문자열 정리
        if isinstance(e, BaseException):
            detail = getattr(e, "detail", None) or str(e)
        else:
            detail = "" if e is None else str(e)

        # 3) meta 병합
        meta: dict[str, Any] = {}

        # 예외 객체가 이미 들고 있는 meta를 먼저 살린다
        if isinstance(e, BaseException):
            exc_meta = getattr(e, "meta", None)
            if isinstance(exc_meta, dict):
                meta.update(exc_meta)

        # host 명령 태그는 별도로 보존
        if self._current_cmd_tag:
            if "cmd" in meta and meta["cmd"] != self._current_cmd_tag:
                meta["host_cmd"] = self._current_cmd_tag
            else:
                meta["cmd"] = self._current_cmd_tag

        if self._plc_cmd_file is not None:
            meta["plc_log_file"] = str(self._plc_cmd_file)

        reporter_kwargs = dict(
            log=self.ctx.log,
            chat=getattr(self.ctx, "chat", None),
            popup=getattr(self.ctx, "popup", None),
            src=src,
            meta=(meta or None),
        )

        # 4) code가 명시된 경우
        if code:
            res: Json = notify_error_code(
                code,
                detail=detail,
                **reporter_kwargs,
            )
        # 5) 예외 객체면 handler 경계 처리
        elif isinstance(e, BaseException):
            res = notify_handler_error(
                e,
                **reporter_kwargs,
            )
        # 6) 문자열인데 code도 없으면 handler fallback
        else:
            res = notify_error_code(
                "E110",
                detail=detail or "Handler failure",
                **reporter_kwargs,
            )

        self._log_client_response(res)
        return res
    
    def _is_loadlock_transition_active(self) -> bool:
        return self._loadlock_transition_lock.locked()

    def _fail_if_loadlock_transition_busy(self, action: str) -> Json | None:
        if self._is_loadlock_transition_active():
            tag = self._loadlock_transition_tag or "LOADLOCK_TRANSITION"
            return self._fail(f"{action} 불가 — {tag} 진행 중", code="E321")
        return None

    # ================== 공정 중 여부 체크 헬퍼 ==================
    def _fail_if_ch_busy(self, ch: int, action: str) -> Json | None:
        """
        해당 CH에서 공정/정리/대기(Runner 포함)가 진행 중이면 명령을 차단하고 실패 응답(Json)을 돌려준다.

        - 'busy'로 보는 것:
          1) runtime_state 기준:
             · chamber(ch)  : 스퍼터 공정
             · pc(ch)       : Plasma Cleaning 공정
             · tsp(0, ch=1) : CH1과 연동된 TSP 공정
          2) ChamberRuntime(Runner) 기준:
             · rt._runner_state != "IDLE"  (PREFLIGHT/COOLDOWN/DELAY/CLEANUP/STOPPING 포함)
             · 또는 stage task가 살아있음

        - runtime_state 조회가 실패하더라도, Runner 상태가 busy면 차단한다.
        """
        rs = getattr(self.ctx, "runtime_state", None)

        try:
            reasons: list[str] = []

            # ------------------------------
            # 1) runtime_state 기반 실행 여부
            # ------------------------------
            if rs is not None and getattr(rs, "is_running", None):
                try:
                    if rs.is_running("chamber", ch):
                        reasons.append(f"CH{ch} 스퍼터 공정 실행 중")
                except Exception:
                    pass

                try:
                    if rs.is_running("pc", ch):
                        reasons.append(f"CH{ch} Plasma Cleaning 실행 중")
                except Exception:
                    pass

                try:
                    if int(ch) == 1 and rs.is_running("tsp", 0):
                        reasons.append("TSP 공정 실행 중")
                except Exception:
                    pass

            # ------------------------------
            # 2) Runner 상태(공정 종료 직후 cleanup / 다음 공정 대기 포함)
            # ------------------------------
            rt = getattr(self.ctx, f"ch{int(ch)}", None)
            if rt is not None:
                st = getattr(rt, "_runner_state", None)
                if isinstance(st, str) and st and st.upper() != "IDLE":
                    _STATE_MSG = {
                        "PREFLIGHT":     f"CH{ch} 공정 시작 준비 중",
                        "RUNNING":       f"CH{ch} 공정 실행 중",
                        "CLEANUP":       f"CH{ch} 공정 종료 처리 중",
                        "STOPPING":      f"CH{ch} 공정 정지 중",
                        "COOLDOWN":      f"CH{ch} 다음 공정 대기 중",
                        "DELAY":         f"CH{ch} 공정 간 대기 중",
                    }
                    reasons.append(_STATE_MSG.get(st.upper(), f"CH{ch} 처리 중 ({st})"))

                # stage task가 살아있는 동안도 busy로 간주
                t = getattr(rt, "_runner_stage_task", None)
                if isinstance(t, asyncio.Task):
                    try:
                        if not t.done():
                            k = getattr(rt, "_runner_stage_kind", None)
                            _STAGE_MSG = {
                                "ADVANCE_QUEUE": f"CH{ch} 리스트 공정 대기 중 (다음 공정 예약됨)",
                                "PROCESS":       f"CH{ch} 공정 실행 중",
                                "PREFLIGHT":     f"CH{ch} 공정 시작 준비 중",
                                "CLEANUP":       f"CH{ch} 공정 종료 처리 중",
                            }
                            if k:
                                reasons.append(_STAGE_MSG.get(k, f"CH{ch} 처리 중 ({k})"))
                            else:
                                reasons.append(f"CH{ch} 처리 중")
                    except Exception:
                        reasons.append(f"CH{ch} 처리 중")

            if reasons:
                return self._fail(f"{action} 불가 — " + " / ".join(reasons), code="E205")

        except Exception:
            # 상태 판단 예외가 장비 조작까지 막지 않도록, 예외 시에는 통과
            return None

        return None

    # ================== CH1,2 상태 조회 ==================
    async def get_sputter_status(self, payload: Json) -> Json:
        """
        CH1/CH2/LoadLock 각각의 상태(idle/running/error) + 진공 여부를 한 번에 조회.
        Chamber_1 / Chamber_2 / Loadlock_Chamber / vacuum 4개 키를 돌려준다.

        ✅ Runner 구조 반영:
        - runtime_state가 idle여도, Runner가 COOLDOWN/DELAY/CLEANUP/PREFLIGHT 등으로 바쁘면 running으로 표시한다.
        """
        try:
            rs = getattr(self.ctx, "runtime_state", None)

            def _ch_state(ch: int) -> str:
                """
                단일 CH 상태 계산:
                - runtime_state.is_running("chamber", ch) 또는 is_running("pc", ch)가 True면 running
                - Runner가 IDLE이 아니면(runner_state != "IDLE") running
                - (둘 다 아니면) idle
                - 조회 중 예외가 나면 error
                """
                running_ch = False

                # 1) runtime_state 기반 실행 여부
                try:
                    if rs is not None and getattr(rs, "is_running", None):
                        if rs.is_running("chamber", ch) or rs.is_running("pc", ch):
                            running_ch = True
                except Exception:
                    return "error"

                # 2) Runner 기반 실행/정리/대기 여부 (공정 종료 직후 next 대기 포함)
                try:
                    rt = getattr(self.ctx, f"ch{ch}", None)
                    if rt is not None:
                        st = getattr(rt, "_runner_state", None)
                        if isinstance(st, str) and st and st.upper() != "IDLE":
                            running_ch = True
                        else:
                            # stage task가 살아있는 동안도 running으로 간주
                            t = getattr(rt, "_runner_stage_task", None)
                            if isinstance(t, asyncio.Task) and (not t.done()):
                                running_ch = True
                except Exception:
                    return "error"

                # 3) 마지막 공정 실패 이력이 남아 있으면 error (단, running이 아닌 경우만)
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
                - start 직후 current_step이 아직 None인 짧은 구간도 True 처리
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
                        return True

                    act = getattr(step, "action", None)
                    actv = getattr(act, "value", None)
                    if actv is None:
                        actv = str(act) if act is not None else ""

                    s = str(actv).strip().upper()
                    if "." in s:
                        s = s.split(".")[-1].strip()

                    return (s == "IG_CMD")

                except Exception:
                    return False

            def _loadlock_state() -> str:
                """
                Loadlock(Plasma Cleaning) 상태 계산:
                - runtime_state.is_running("pc", ch)가 1 또는 2 중 하나라도 True면 running
                - 마지막 PC 실패 이력이 남아 있으면 error
                - ✅ CH1 공정이 IG 단계(IG_CMD)인 동안에는 Loadlock을 running으로 표시 유지
                - (fallback) plasma cleaning 런타임의 is_running / _running 플래그 사용
                - 조회 중 예외가 나면 error
                """
                try:
                    if rs is not None and getattr(rs, "is_running", None):
                        for ch in (1, 2):
                            try:
                                if rs.is_running("pc", ch):
                                    return "running"
                            except Exception:
                                continue

                        if getattr(rs, "has_error", None):
                            for ch in (1, 2):
                                try:
                                    if rs.has_error("pc", ch):
                                        return "error"
                                except Exception:
                                    return "error"
                except Exception:
                    return "error"

                if _ch1_is_waiting_ig():
                    return "running"

                try:
                    pc = getattr(self.ctx, "pc", None)
                    if pc is not None:
                        fn = getattr(pc, "is_running", None)

                        if callable(fn):
                            try:
                                cleaning = bool(fn())
                            except TypeError:
                                cleaning = bool(getattr(pc, "_running", False))
                        else:
                            cleaning = bool(fn) if isinstance(fn, bool) else bool(getattr(pc, "_running", False))

                        return "running" if cleaning else "idle"
                except Exception:
                    return "error"

                return "idle"

            chamber_1 = _ch_state(1)
            chamber_2 = _ch_state(2)
            loadlock  = _loadlock_state()

            async with self._plc_command("GET_SPUTTER_STATUS"):
                self._log_client_request(payload)

                async with self._plc_call():
                    atm = await self.ctx.plc.read_bit("L_ATM")

                vacuum = (not bool(atm))

                return self._ok(
                    Chamber_1=chamber_1,
                    Chamber_2=chamber_2,
                    Loadlock_Chamber=loadlock,
                    vacuum=vacuum,
                )

        except Exception as e:
            return self._fail(e, code=getattr(e, "code", None))
        
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
                return self._fail(e, code=getattr(e, "code", None) or "E412")

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
            # ✅ lock 획득 후 다시 한 번 확인
            busy = self._fail_if_ch_busy(ch, f"START_SPUTTER_CH{ch}")
            if busy is not None:
                return busy
    
            async with self._plc_command(f"START_SPUTTER_CH{ch}"):
                self._log_client_request(data)

                try:
                    st = await self._read_gate_state(ch)
                except KeyError as e:
                    return self._fail(f"PLC 주소맵에 gate lamp 키가 없습니다: {e}", code="E411")
                except Exception as e:
                    return self._fail(e, code=getattr(e, "code", None) or "E412")

                if st["state"] != "closed":
                    return self._fail(f"START_SPUTTER 불가 — CH{ch} gate가 CLOSED가 아님({st['state']})", code="E301")

                try:
                    await chamber.start_with_recipe_string(recipe)
                    return self._ok("SPUTTER START OK", ch=ch)
                except RuntimeError as e:
                    # 런타임 내부 프리플라이트/쿨다운/중복 실행 등 명시적 거절
                    _msg = str(e)
                    _code = getattr(e, "code", None)
                    if not _code and (
                        "1분 대기" in _msg 
                        or "cooldown" in _msg.lower()
                        or "preflight timeout" in _msg.lower()
                    ):
                        # ✅ 쿨다운은 BUSY 상태 — STOP이 아닌 RETRY_LATER로 분류
                        _code = "E700"
                    return self._fail(_msg, code=_code or "E410")
                except Exception as e:
                    return self._fail(e)

    async def start_plasma_cleaning(self, data: Json) -> Json:
        """
        START_PLASMA_CLEANING 핸들러
        - data: {"recipe": "csv 경로 또는 레시피 문자열", ...}
        - PlasmaCleaningRuntime.start_with_recipe_string(...)을 호출해서
        프리플라이트/쿨다운/교차실행 체크 결과만 돌려준다.
        """
        recipe = str(data.get("recipe") or "").strip()

        if not recipe:
            return self._fail(
                "recipe가 비어 있습니다. (CSV 경로 또는 레시피 문자열 필요)",
                code="E202",
            )

        pc = getattr(self.ctx, "pc", None)
        if not pc:
            return self._fail(
                "Plasma Cleaning runtime not ready",
                code="E204",
            )

        # 🔹 START_PLASMA_CLEANING 전용 로그 파일 생성
        async with self._plc_command("START_PLASMA_CLEANING"):
            # 클라이언트에서 넘어온 payload 그대로 남김
            self._log_client_request(data)

            try:
                await pc.start_with_recipe_string(recipe)
                return self._ok("PLASMA CLEANING START OK")
            except RuntimeError as e:
                # 런타임 내부 프리플라이트/쿨다운/중복 실행 등 명시적 거절
                _msg = str(e)
                _code = getattr(e, "code", None)
                if not _code and (
                    "1분 대기" in _msg 
                    or "cooldown" in _msg.lower()
                    or "preflight timeout" in _msg.lower()
                ):
                    # ✅ 쿨다운은 BUSY 상태 — STOP이 아닌 RETRY_LATER로 분류
                    _code = "E700"
                return self._fail(_msg, code=_code or "E420")
            except Exception as e:
                return self._fail(e)

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

    async def _read_loadlock_vacuum_transition_bits(self) -> dict[str, bool]:
        """
        VACUUM_ON/VACUUM_OFF 전환 판정용 Loadlock 상태 스냅샷.

        읽는 비트:
        - L_VAC_READY_SW
        - L_VAC_NOT_READY
        - LP_STEP1
        - LP_STEP2
        - L_R_P_SW
        - L_R_V_SW

        주의:
        - 현재는 개별 read_bit()를 순서대로 호출하는 스냅샷이며,
        완전한 atomic block read는 아니다.
        - 대신 polling grace(transition_grace_s / both_off_grace_s)로
        PLC 전이 시점의 관측 race를 흡수한다.
        """
        async with self._plc_call():
            return {
                "L_VAC_READY_SW": bool(await self.ctx.plc.read_bit("L_VAC_READY_SW")),
                "L_VAC_NOT_READY": bool(await self.ctx.plc.read_bit("L_VAC_NOT_READY")),
                "LP_STEP1": bool(await self.ctx.plc.read_bit("LP_STEP1")),
                "LP_STEP2": bool(await self.ctx.plc.read_bit("LP_STEP2")),
                "L_R_P_SW": bool(await self.ctx.plc.read_bit("L_R_P_SW")),
                "L_R_V_SW": bool(await self.ctx.plc.read_bit("L_R_V_SW")),
            }
        
    async def _read_loadlock_vacuum_diag(self) -> dict[str, bool | None]:
        """
        VACUUM_ON 실패/타임아웃 시점에 1회만 호출되는 진단 비트 스냅샷.

        - 폴링 루프에서는 호출되지 않으므로 race 윈도우에 영향 없음.
        - address map에 없을 수 있는 비트는 read 실패 시 None.

        수집 대상 (PLC 변수표 기준):
        - L_GAUGE_A           (P00003) : 로드락 진공게이지 setpoint A raw 신호
        - L_GAUGE_A_INTERLOCK (M01700) : 30 스캔 안정 후 SET. LP_STEP1 SET 조건
        - L_R_P_OUT           (P00030) : 러핑펌프 실제 출력 코일
        - L_R_V_OUT           (P00031) : 러핑밸브 실제 출력 코일
        - L_VENT_OUT          (P00034) : 벤트밸브 출력 (배기-벤트 충돌 진단용)
        - L_ATM_SENSOR        (P00009) : 로드락 ATM 센서
        """
        names = [
            "L_GAUGE_A",
            "L_GAUGE_A_INTERLOCK",
            "L_R_P_OUT",
            "L_R_V_OUT",
            "L_VENT_OUT",
            "L_ATM_SENSOR",
        ]
        result: dict[str, bool | None] = {}
        try:
            async with self._plc_call():
                for name in names:
                    try:
                        result[name] = bool(await self.ctx.plc.read_bit(name))
                    except Exception:
                        result[name] = None
        except Exception:
            for name in names:
                result.setdefault(name, None)
        return result

    async def vacuum_on(self, data: Json) -> Json:
        """
        VACUUM ON 시퀀스:
        1) gate close 확인
        2) 이미 L_VAC_READY_SW=True 이면 현재 러핑 상태까지 확인
        - 이미 L_R_P_SW=False, L_R_V_SW=False 이면 즉시 성공
        - READY는 TRUE인데 러핑이 아직 남아 있으면 러핑 OFF 정리 후 성공
        3) 아니면
        - L_VENT_SW=False
        - L_R_P_SW=True
        - 5초 대기
        - L_R_V_인터락 확인
        - L_R_V_SW=True
        4) timeout까지 Loadlock 전이 비트 스냅샷을 폴링:
        - L_VAC_READY_SW
        - L_VAC_NOT_READY
        - LP_STEP1
        - LP_STEP2
        - L_R_P_SW
        - L_R_V_SW
        5) 폴링 판정 원칙:
        - L_VAC_READY_SW=True 이면 성공 우선
        - L_VAC_NOT_READY=True 이면 실패 우선
        - PLC L_PUMPING stop sequence
            (LP_STEP1 -> 60s 후 L_R_V_SW OFF + LP_STEP2 ON
            -> 3s 후 L_R_P_SW OFF + L_VAC_READY_SW ON)를 정상 경로로 본다
        - 따라서 READY 전에 러핑 출력이 일부/전부 OFF로 보여도
            LP_STEP1/LP_STEP2 진행 중이거나 READY/NOT_READY 반영 race 구간이면
            짧은 grace 후 재확인한다
        - READY/NOT_READY 판정 없이 비정상 OFF 상태가 지속되면 실패한다
        6) READY 확인 후에는 PLC가 러핑 OFF를 스스로 정리하는지 먼저 기다리고,
        필요할 때만 fallback으로 L_R_V_SW -> delay -> L_R_P_SW 순서로 OFF 정리한다
        7) 실패/예외 시에는 러핑밸브/펌프 OFF 원복
        """
        timeout_s = float(data.get("timeout_s", 660.0))  # 기본 11분 (PLC T0052 600s + 60s 여유)

        async with self._plc_command("VACUUM_ON"):
            self._log_client_request(data)

            success = False
            cleanup_done = False

            async def _stop_roughing(delay_s: float = 5.0) -> None:
                """
                러핑밸브 OFF -> delay -> 러핑펌프 OFF
                실패 경로에서 중복 원복을 막기 위해 cleanup_done=True 처리
                """
                nonlocal cleanup_done

                with contextlib.suppress(Exception):
                    async with self._plc_call():
                        await self.ctx.plc.write_switch("L_R_V_SW", False)

                await asyncio.sleep(delay_s)

                with contextlib.suppress(Exception):
                    async with self._plc_call():
                        await self.ctx.plc.write_switch("L_R_P_SW", False)

                cleanup_done = True

            try:
                busy = self._fail_if_loadlock_transition_busy("VACUUM_ON")
                if busy is not None:
                    return busy

                async with self._loadlock_transition_lock:
                    self._loadlock_transition_tag = "VACUUM_ON"
                    try:
                        ok, msg, code = await self._require_gates_closed()
                        if not ok:
                            return self._fail(msg, code=code)

                        # 1) 이미 ready 상태면 현재 러핑 상태까지 같이 확인
                        snap0 = await self._read_loadlock_vacuum_transition_bits()
                        vac_ready_now = snap0["L_VAC_READY_SW"]
                        pump_sw_now = snap0["L_R_P_SW"]
                        valve_sw_now = snap0["L_R_V_SW"]

                        if vac_ready_now:
                            if (not pump_sw_now) and (not valve_sw_now):
                                success = True
                                return self._ok("VACUUM_ON: 이미 L_VAC_READY_SW=TRUE 상태")

                            await _stop_roughing(delay_s=5.0)

                            off_deadline = time.monotonic() + 10.0
                            while time.monotonic() < off_deadline:
                                async with self._plc_call():
                                    pump_sw2 = bool(await self.ctx.plc.read_bit("L_R_P_SW"))
                                    valve_sw2 = bool(await self.ctx.plc.read_bit("L_R_V_SW"))

                                if (not pump_sw2) and (not valve_sw2):
                                    success = True
                                    return self._ok(
                                        "VACUUM_ON: 이미 L_VAC_READY_SW=TRUE였고 "
                                        "L_R_V_SW OFF → 5초 → L_R_P_SW OFF 정리 완료"
                                    )

                                await asyncio.sleep(1.0)

                            return self._fail(
                                "VACUUM_ON 실패 — L_VAC_READY_SW=TRUE였지만 "
                                "L_R_P_SW/L_R_V_SW OFF 완료 확인 실패",
                                code="E312",
                            )

                        # 2) 벤트 OFF
                        async with self._plc_call():
                            await self.ctx.plc.write_switch("L_VENT_SW", False)
                        await asyncio.sleep(0.3)

                        # 3) 러핑펌프 OFF 타이머 체크
                        async with self._plc_call():
                            if await self.ctx.plc.read_bit("L_R_P_OFF_TIMER"):
                                return self._fail(
                                    "러핑펌프 OFF 타이머 진행 중 → 잠시 후 재시도",
                                    code="E309",
                                )

                        # 3-b) PLC L_PUMPING 시퀀스 진행 중 여부 확인
                        # LP_STEP1 또는 LP_STEP2가 True면 PLC가 이미 자동으로 시퀀스 진행 중.
                        # 이 상태에서 소프트웨어가 L_R_P_SW / L_R_V_SW를 쓰면
                        # PLC 타이머가 강제로 덮어써서 시퀀스가 꼬이므로 쓰기 전부 스킵.
                        async with self._plc_call():
                            lp_step1_active = bool(await self.ctx.plc.read_bit("LP_STEP1"))
                            lp_step2_active = bool(await self.ctx.plc.read_bit("LP_STEP2"))

                        if not (lp_step1_active or lp_step2_active):
                            # 4) 러핑펌프 ON
                            async with self._plc_call():
                                await self.ctx.plc.write_switch("L_R_P_SW", True)

                            await asyncio.sleep(5.0)

                            # 5) 러핑밸브 인터락 확인
                            async with self._plc_call():
                                rv_interlock = bool(await self.ctx.plc.read_bit("L_R_V_인터락"))

                            if not rv_interlock:
                                await _stop_roughing(delay_s=5.0)
                                return self._fail(
                                    "L_R_V_인터락=FALSE → 러핑밸브 개방 불가 (L_R_P_SW/L_R_V_SW OFF 처리)",
                                    code="E310",
                                )

                            # 6) 러핑밸브 ON
                            async with self._plc_call():
                                await self.ctx.plc.write_switch("L_R_V_SW", True)

                        # 7) timeout까지 폴링
                        deadline = time.monotonic() + timeout_s

                        # PLC L_PUMPING 문서 기준:
                        # - LP_STEP1 -> 60s 후 L_R_V_SW OFF + LP_STEP2 ON
                        # - LP_STEP2 -> 3s 후 L_R_P_SW OFF + L_VAC_READY_SW ON
                        # - 장시간 미도달 실패는 L_VAC_NOT_READY로 별도 판정
                        #
                        # 따라서 raw coil(L_R_P_SW / L_R_V_SW) 상태만으로 즉시 실패시키지 않고,
                        # LP_STEP1 / LP_STEP2 / L_VAC_NOT_READY와 함께 본다.
                        # 특히 READY 직전/직후에는 pump/valve OFF가 먼저 관측될 수 있으므로
                        # 짧은 grace 후 재확인한다.
                        transition_deadline: float | None = None
                        both_off_deadline: float | None = None
                        lp_step_off_deadline: float | None = None

                        transition_grace_s = 5.0   # LP_STEP2 3초 + 폴링 여유
                        both_off_grace_s = 5.0     # READY/NOT_READY 반영 race 흡수용

                        while time.monotonic() < deadline:
                            snap = await self._read_loadlock_vacuum_transition_bits()

                            vac_ready = snap["L_VAC_READY_SW"]
                            vac_not_ready = snap["L_VAC_NOT_READY"]
                            lp_step1 = snap["LP_STEP1"]
                            lp_step2 = snap["LP_STEP2"]
                            pump_sw = snap["L_R_P_SW"]
                            valve_sw = snap["L_R_V_SW"]

                            # 1) 성공 우선
                            if vac_ready:
                                transition_deadline = None
                                both_off_deadline = None

                                # 먼저 PLC가 스스로 OFF 정리하는지 짧게 기다린다.
                                off_deadline = time.monotonic() + 5.0
                                while time.monotonic() < off_deadline:
                                    snap2 = await self._read_loadlock_vacuum_transition_bits()
                                    if (not snap2["L_R_P_SW"]) and (not snap2["L_R_V_SW"]):
                                        success = True
                                        return self._ok(
                                            "VACUUM_ON 완료 — PLC의 L_VAC_READY_SW=TRUE 및 "
                                            "러핑 OFF 완료 확인"
                                        )
                                    await asyncio.sleep(0.2)

                                # PLC가 아직 정리하지 못했을 때만 fallback으로 OFF
                                await _stop_roughing(delay_s=5.0)

                                off_deadline = time.monotonic() + 10.0
                                while time.monotonic() < off_deadline:
                                    snap2 = await self._read_loadlock_vacuum_transition_bits()
                                    if (not snap2["L_R_P_SW"]) and (not snap2["L_R_V_SW"]):
                                        success = True
                                        return self._ok(
                                            "VACUUM_ON 완료 — L_VAC_READY_SW=TRUE 확인 후 "
                                            "fallback으로 L_R_V_SW/L_R_P_SW OFF 정리 완료"
                                        )
                                    await asyncio.sleep(1.0)

                                return self._fail(
                                    "VACUUM_ON 실패 — L_VAC_READY_SW=TRUE였지만 "
                                    "L_R_P_SW/L_R_V_SW OFF 완료 확인 실패",
                                    code="E312",
                                )

                            # 2) 실패 우선
                            if vac_not_ready:
                                return self._fail(
                                    "VACUUM_ON 실패 — PLC가 L_VAC_NOT_READY=TRUE로 판정 "
                                    f"(LP_STEP1={lp_step1}, LP_STEP2={lp_step2}, "
                                    f"L_R_P_SW={pump_sw}, L_R_V_SW={valve_sw})",
                                    code="E312",
                                )

                            # 3) 정상 러핑 진행 중
                            if pump_sw and valve_sw:
                                transition_deadline = None
                                both_off_deadline = None

                            # 4) stop-sequence 진입 또는 그 직전 관측
                            elif pump_sw and (not valve_sw):
                                now = time.monotonic()
                                both_off_deadline = None

                                if transition_deadline is None:
                                    transition_deadline = now + transition_grace_s
                                elif now >= transition_deadline:
                                    return self._fail(
                                        "VACUUM_ON 실패 — stop-sequence 진입 후 "
                                        "L_VAC_READY_SW가 유예시간 내 들어오지 않음 "
                                        f"(LP_STEP1={lp_step1}, LP_STEP2={lp_step2}, "
                                        f"L_R_P_SW={pump_sw}, L_R_V_SW={valve_sw})",
                                        code="E312",
                                    )

                            # 5) READY 직전/직후 폴링 race 흡수
                            elif (not pump_sw) and (not valve_sw):
                                now = time.monotonic()
                                transition_deadline = None

                                # ✅ LP_STEP1 또는 LP_STEP2가 True인 채 both_off가 읽히는 경우:
                                # 6개 비트를 순차로 읽는 동안 PLC 스캔이 진행되어 발생하는 race.
                                # PLC 실제 시퀀스: LP_STEP1(6초) → LP_STEP2(0.3초) → L_VAC_READY_SW SET.
                                # 5초 카운트를 시작하지 말고, 별도 15초 제한으로 다음 폴링을 기다린다.
                                if lp_step1 or lp_step2:
                                    if lp_step_off_deadline is None:
                                        lp_step_off_deadline = now + 15.0
                                    elif now >= lp_step_off_deadline:
                                        return self._fail(
                                            "VACUUM_ON 실패 — LP_STEP 진행 중 "
                                            "L_VAC_READY_SW 미도달 (15s 초과) "
                                            f"(LP_STEP1={lp_step1}, LP_STEP2={lp_step2})",
                                            code="E312",
                                        )
                                    both_off_deadline = None
                                    await asyncio.sleep(1.0)
                                    continue

                                # LP_STEP 모두 False → 정상 race 흡수 구간
                                lp_step_off_deadline = None

                                if both_off_deadline is None:
                                    both_off_deadline = now + both_off_grace_s
                                elif now >= both_off_deadline:
                                    snap2 = await self._read_loadlock_vacuum_transition_bits()

                                    if snap2["L_VAC_READY_SW"]:
                                        await asyncio.sleep(0.2)
                                        continue

                                    # ✅ 진단 비트 1회 수집 (실패해도 모두 None으로 채워 안전)
                                    #    L1247의 timeout 분기와 별도 경로이므로 여기서 별도 수집 필요.
                                    #    이 try-except가 없으면 아래 self.ctx.log()에서
                                    #    UnboundLocalError("cannot access local variable 'diag'") 발생.
                                    try:
                                        diag = await self._read_loadlock_vacuum_diag()
                                    except Exception:
                                        diag = {
                                            "L_GAUGE_A": None, "L_GAUGE_A_INTERLOCK": None,
                                            "L_R_P_OUT": None, "L_R_V_OUT": None,
                                            "L_VENT_OUT": None, "L_ATM_SENSOR": None,
                                        }

                                    # 전체 진단은 로그 파일에만 (Google Chat 알림 길이 절약)
                                    self.ctx.log(
                                        "PLC_REMOTE",
                                        f"[VACUUM_ON_DIAG/both_off] "
                                        f"L_GAUGE_A={diag['L_GAUGE_A']}, "
                                        f"L_GAUGE_A_INTERLOCK={diag['L_GAUGE_A_INTERLOCK']}, "
                                        f"L_R_P_OUT={diag['L_R_P_OUT']}, "
                                        f"L_R_V_OUT={diag['L_R_V_OUT']}, "
                                        f"L_VENT_OUT={diag['L_VENT_OUT']}, "
                                        f"L_ATM_SENSOR={diag['L_ATM_SENSOR']}, "
                                        f"snap2={snap2}",
                                    )

                                    if snap2["L_VAC_NOT_READY"]:
                                        return self._fail(
                                            "VACUUM_ON 실패 — 러핑 OFF 후 PLC가 "
                                            "L_VAC_NOT_READY=TRUE로 판정",
                                            code="E312",
                                        )

                                    return self._fail(
                                        "VACUUM_ON 실패 — PLC가 자체적으로 "
                                        "L_R_P_SW/L_R_V_SW를 OFF로 전환 "
                                        "(진공 게이지 인터락 미충족) "
                                        f"(L_GAUGE_A_INTERLOCK={diag['L_GAUGE_A_INTERLOCK']}, "
                                        f"L_R_P_OUT={diag['L_R_P_OUT']}, "
                                        f"L_R_V_OUT={diag['L_R_V_OUT']})",
                                        code="E312",
                                    )

                            # 6) 이 조합은 비정상
                            elif (not pump_sw) and valve_sw:
                                return self._fail(
                                    "VACUUM_ON 실패 — READY 전 러핑펌프가 먼저 OFF됨 "
                                    f"(LP_STEP1={lp_step1}, LP_STEP2={lp_step2}, "
                                    f"L_R_P_SW={pump_sw}, L_R_V_SW={valve_sw})",
                                    code="E312",
                                )

                            await asyncio.sleep(1.0)

                        # 8) timeout
                        try:
                            snap_timeout = await self._read_loadlock_vacuum_transition_bits()
                        except Exception:
                            snap_timeout = None

                        # 진단 비트 1회 수집 (실패해도 모두 None으로 채워 안전)
                        diag = await self._read_loadlock_vacuum_diag()

                        # 전체 진단은 로그 파일에만
                        self.ctx.log(
                            "PLC_REMOTE",
                            f"[VACUUM_ON_DIAG/timeout] "
                            f"snap={snap_timeout}, "
                            f"L_GAUGE_A={diag['L_GAUGE_A']}, "
                            f"L_GAUGE_A_INTERLOCK={diag['L_GAUGE_A_INTERLOCK']}, "
                            f"L_R_P_OUT={diag['L_R_P_OUT']}, "
                            f"L_R_V_OUT={diag['L_R_V_OUT']}, "
                            f"L_VENT_OUT={diag['L_VENT_OUT']}, "
                            f"L_ATM_SENSOR={diag['L_ATM_SENSOR']}",
                        )

                        if snap_timeout and snap_timeout["L_VAC_NOT_READY"]:
                            return self._fail(
                                f"VACUUM_ON 실패 — {int(timeout_s)}s 타임아웃, "
                                "PLC가 L_VAC_NOT_READY=TRUE로 판정 "
                                "(진공 게이지 인터락 미충족) "
                                f"(L_GAUGE_A_INTERLOCK={diag['L_GAUGE_A_INTERLOCK']})",
                                code="E312",
                            )

                        if snap_timeout:
                            return self._fail(
                                f"VACUUM_ON 타임아웃 — {int(timeout_s)}s 내 진공 미도달 "
                                "(진공 게이지 인터락/펌프/누설 점검 필요) "
                                f"(L_GAUGE_A_INTERLOCK={diag['L_GAUGE_A_INTERLOCK']}, "
                                f"L_R_P_SW={snap_timeout['L_R_P_SW']}, "
                                f"L_R_V_SW={snap_timeout['L_R_V_SW']})",
                                code="E312",
                            )

                        return self._fail(
                            f"VACUUM_ON 타임아웃 — {int(timeout_s)}s 내 진공 미도달 "
                            "(상태 스냅샷 읽기 실패)",
                            code="E312",
                        )

                    finally:
                        self._loadlock_transition_tag = None

            except Exception as e:
                return self._fail(e)

            finally:
                # ✅ 실패/예외 경로 원복
                # - 이미 _stop_roughing()을 수행한 경우(cleanup_done=True)는 중복 정리 안 함
                if (not success) and (not cleanup_done):
                    await _stop_roughing(delay_s=5.0)

    async def vacuum_off(self, data: Json) -> Json:
        """
        VACUUM OFF 시퀀스:
        0) L_R_V_SW=False → L_R_P_SW=False 선행 정지
        1) L_VENT_인터락 True 확인
        2) L_VENT_SW = True (벤트 시작)
        3) L_ATM == True 까지 대기
        4) L_VENT_SW = False
        """
        timeout_s = float(data.get("timeout_s", 240.0))

        async with self._plc_command("VACUUM_OFF"):
            self._log_client_request(data)

            success = False

            busy = self._fail_if_loadlock_transition_busy("VACUUM_OFF")
            if busy is not None:
                return busy

            try:
                async with self._loadlock_transition_lock:
                    self._loadlock_transition_tag = "VACUUM_OFF"
                    try:
                        ok, msg, code = await self._require_gates_closed()
                        if not ok:
                            return self._fail(msg, code=code)

                        # 이미 대기압이면 종료 상태만 맞추고 성공
                        async with self._plc_call():
                            atm_now = bool(await self.ctx.plc.read_bit("L_ATM"))
                        if atm_now:
                            async with self._plc_call():
                                await self.ctx.plc.write_switch("L_VENT_SW", False)
                                await self.ctx.plc.write_switch("L_R_V_SW", False)
                            await asyncio.sleep(5.0)
                            async with self._plc_call():
                                await self.ctx.plc.write_switch("L_R_P_SW", False)

                            success = True
                            return self._ok("VACUUM_OFF: 이미 대기압 상태 (L_ATM=TRUE)")

                        # 0) 러핑밸브/펌프 OFF
                        async with self._plc_call():
                            await self.ctx.plc.write_switch("L_R_V_SW", False)

                        await asyncio.sleep(5.0)

                        async with self._plc_call():
                            await self.ctx.plc.write_switch("L_R_P_SW", False)

                        # 1) 벤트 인터락 확인
                        async with self._plc_call():
                            if not await self.ctx.plc.read_bit("L_VENT_인터락"):
                                return self._fail(
                                    "L_VENT_인터락=FALSE → 벤트 불가",
                                    code="E311",
                                )

                        # 2) 벤트 ON
                        async with self._plc_call():
                            await self.ctx.plc.write_switch("L_VENT_SW", True)

                        # 3) L_ATM TRUE 대기
                        deadline = time.monotonic() + timeout_s
                        while time.monotonic() < deadline:
                            async with self._plc_call():
                                atm = await self.ctx.plc.read_bit("L_ATM")

                            if atm:
                                async with self._plc_call():
                                    await self.ctx.plc.write_switch("L_VENT_SW", False)
                                success = True
                                return self._ok("VACUUM_OFF 완료 (L_ATM=TRUE, L_VENT_SW=FALSE)")

                            await asyncio.sleep(0.5)

                        with contextlib.suppress(Exception):
                            async with self._plc_call():
                                await self.ctx.plc.write_switch("L_VENT_SW", False)

                        return self._fail(
                            f"VACUUM_OFF 타임아웃: {int(timeout_s)}s 내 L_ATM TRUE 미도달 (N2 gas 부족)",
                            code="E313",
                        )

                    finally:
                        self._loadlock_transition_tag = None

            except Exception as e:
                return self._fail(e)

            finally:
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
        
        # 🔹 Loadlock VACUUM_ON / VACUUM_OFF 진행 중이면 Gate Open 금지
        busy_ll = self._fail_if_loadlock_transition_busy(f"CH{ch}_GATE_OPEN")
        if busy_ll is not None:
            return busy_ll

        if ch == 1:
            interlock, sw, lamp = "G_V_1_인터락", "G_V_1_OPEN_SW", "G_V_1_OPEN_LAMP"
        elif ch == 2:
            interlock, sw, lamp = "G_V_2_인터락", "G_V_2_OPEN_SW", "G_V_2_OPEN_LAMP"
        else:
            return self._fail(f"지원하지 않는 CH: {ch}", code="E201")

        try:
            ch_lock = self.ctx.lock_ch1 if ch == 1 else self.ctx.lock_ch2

            async with ch_lock:
                # ✅ 같은 CH 공정 상태를 lock 안에서 다시 확인
                busy = self._fail_if_ch_busy(ch, f"CH{ch}_GATE_OPEN")
                if busy is not None:
                    return busy
                
                async with self._loadlock_gate_lock:
                    # ✅ 락 획득 후 다시 한 번 확인
                    busy_ll = self._fail_if_loadlock_transition_busy(f"CH{ch}_GATE_OPEN")
                    if busy_ll is not None:
                        return busy_ll

                    async with self._plc_command(f"GATE_OPEN_CH{ch}"):
                        self._log_client_request(data)

                        # 0) gate lamp 먼저 확인
                        cur_st = await self._read_gate_state(ch)

                        if cur_st["state"] == "invalid_both_true":
                            return self._fail(
                                f"CH{ch} gate lamp 이상(OPEN/CLOSE 모두 TRUE): {cur_st}",
                                code="E306",
                            )

                        # 1) 이미 OPEN이면 loadlock/other-gate 체크 없이 즉시 반환
                        if cur_st["state"] == "open":
                            if ch == 2:
                                async with self._plc_call():
                                    if not await self.ctx.plc.read_bit("MAIN_SHUTTER_2_인터락"):
                                        return self._fail("MAIN_SHUTTER_2_인터락=FALSE → MAIN_SHUTTER_OPEN 불가", code="E330")
                                    await self.ctx.plc.main_shutter(2, open=True)

                                return self._ok(f"CH2_GATE_OPEN: 이미 OPEN + MAIN_SHUTTER_OPEN", current=cur_st)

                            return self._ok(f"CH{ch}_GATE_OPEN: 이미 OPEN 상태", current=cur_st)

                        # 2) 실제로 gate를 열어야 할 때만 Loadlock 상태 확인
                        ok_ll, msg_ll = await self._require_loadlock_safe_for_gate_open()
                        if not ok_ll:
                            return self._fail(msg_ll, code="E321")

                        # 3) 다른 챔버 gate 확인
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

                        # ✅ gate open 실패면 여기서 끝 → MAIN_SHUTTER_OPEN 절대 안 함
                        if not ok:
                            return self._fail(f"CH{ch}_GATE_OPEN 실패 — {lamp}=FALSE (대기 {int(wait_s)}s)", code="E304")

                        # ✅ CH2: gate open 성공했을 때만 마지막에 main shutter open
                        if ch == 2:
                            async with self._plc_call():
                                if not await self.ctx.plc.read_bit("MAIN_SHUTTER_2_인터락"):
                                    return self._fail("MAIN_SHUTTER_2_인터락=FALSE → MAIN_SHUTTER_OPEN 불가", code="E330")
                                await self.ctx.plc.main_shutter(2, open=True)

                            return self._ok(f"CH2_GATE_OPEN 완료 — {lamp}=TRUE + MAIN_SHUTTER_OPEN (대기 {int(wait_s)}s)")

                        return self._ok(f"CH{ch}_GATE_OPEN 완료 — {lamp}=TRUE (대기 {int(wait_s)}s)")

        except Exception as e:
            return self._fail(
                e,
                code=getattr(e, "code", None) or "E412",
            )

    async def gate_close(self, data: Json) -> Json:
        """
        CHx_GATE_CLOSE 시퀀스:
        0) LOADING_{ch}_SENSOR_LAMP 먼저 확인
           - FALSE면 close 명령을 보내지 않고 즉시 실패
           - 실패 사유는 응답/로그/chat으로 그대로 전달
        1) G_V_{ch}_CLOSE_SW 펄스
        2) wait_s 후 G_V_{ch}_CLOSE_LAMP 확인
        (※ gate close 자체의 최종 permissive는 PLC 내부에서 다시 판단)
        """
        ch = int(data.get("ch", 1))
        wait_s = float(data.get("wait_s", 5.0))  # 기본 5초

        # ✅ (추가) 공정 실행 중이면 게이트 조작 금지 (gate_open과 동일 철학)
        busy = self._fail_if_ch_busy(ch, f"CH{ch}_GATE_CLOSE")
        if busy is not None:
            return busy

        if ch == 1:
            sw, lamp, sensor_lamp = (
                "G_V_1_CLOSE_SW",
                "G_V_1_CLOSE_LAMP",
                "LOADING_1_SENSOR_LAMP",
            )
        elif ch == 2:
            sw, lamp, sensor_lamp = (
                "G_V_2_CLOSE_SW",
                "G_V_2_CLOSE_LAMP",
                "LOADING_2_SENSOR_LAMP",
            )
        else:
            return self._fail(f"지원하지 않는 CH: {ch}", code="E201")

        ch_lock = self.ctx.lock_ch1 if ch == 1 else self.ctx.lock_ch2

        async with ch_lock:
            # ✅ lock 획득 후 다시 확인
            busy = self._fail_if_ch_busy(ch, f"CH{ch}_GATE_CLOSE")
            if busy is not None:
                return busy

            async with self._loadlock_gate_lock:
                async with self._plc_command(f"GATE_CLOSE_CH{ch}"):
                    self._log_client_request(data)
                    try:
                        # ✅ (추가) 0) gate lamp 먼저 확인: 이미 CLOSED면 즉시 OK
                        cur_st = await self._read_gate_state(ch)
                        if cur_st["state"] == "closed":
                            # ✅ CH2: gate가 이미 CLOSED여도 main shutter는 CLOSE로 맞춰준다.
                            if ch == 2:
                                async with self._plc_call():
                                    await self.ctx.plc.main_shutter(2, open=False)
                                return self._ok("CH2_GATE_CLOSE: 이미 CLOSED + MAIN_SHUTTER_CLOSE", current=cur_st)

                            return self._ok(f"CH{ch}_GATE_CLOSE: 이미 CLOSED 상태", current=cur_st)

                        if cur_st["state"] == "invalid_both_true":
                            return self._fail(
                                f"CH{ch} gate lamp 이상(OPEN/CLOSE 모두 TRUE): {cur_st}",
                                code="E306",
                            )
                        
                        # 0) gate close permissive용 loading sensor lamp 확인
                        try:
                            async with self._plc_call():
                                sensor_ok = bool(await self.ctx.plc.read_bit(sensor_lamp))
                        except KeyError as e:
                            return self._fail(
                                f"PLC 주소맵에 {sensor_lamp} 키가 없습니다: {e}",
                                code="E411",
                            )
                        except Exception as e:
                            return self._fail(
                                e,
                                code=getattr(e, "code", None) or "E412",
                            )

                        if not sensor_ok:
                            return self._fail(
                                f"CH{ch}_GATE_CLOSE 불가 — {sensor_lamp}=FALSE "
                                f"(arm/loading sensor 미감지, gate close 명령 미전송)",
                                code="E305",
                            )

                        # 1) 스위치 펄스 — 쓰는 순간만 락
                        async with self._plc_call():
                            await self.ctx.plc.press_switch(sw)

                        # 2) 대기(락 없음)
                        await asyncio.sleep(wait_s)

                        # 3) 램프 확인 — 읽는 순간만 락
                        async with self._plc_call():
                            ok = await self.ctx.plc.read_bit(lamp)
                            sensor_after = bool(await self.ctx.plc.read_bit(sensor_lamp))

                        # ✅ CH2: gate close 요청이면 마지막에 main shutter close는 best-effort로 시도(안전)
                        ms_err = None
                        if ch == 2:
                            try:
                                async with self._plc_call():
                                    await self.ctx.plc.main_shutter(2, open=False)
                            except Exception as e:
                                ms_err = e

                        if ok:
                            if ch == 2 and ms_err is not None:
                                return self._fail(f"CH2_GATE_CLOSE는 성공했지만 MAIN_SHUTTER_CLOSE 실패: {type(ms_err).__name__}: {ms_err}", code="E331")
                            if ch == 2:
                                return self._ok(f"CH2_GATE_CLOSE 완료 — {lamp}=TRUE + MAIN_SHUTTER_CLOSE (대기 {int(wait_s)}s)")
                            return self._ok(f"CH{ch}_GATE_CLOSE 완료 — {lamp}=TRUE (대기 {int(wait_s)}s)")

                        # gate close 실패
                        if ch == 2 and ms_err is None:
                            return self._fail(
                                f"CH2_GATE_CLOSE 실패 — {lamp}=FALSE, "
                                f"{sensor_lamp}={sensor_after} (대기 {int(wait_s)}s) "
                                f"+ MAIN_SHUTTER_CLOSE 시도 완료",
                                code="E305",
                            )
                        if ch == 2 and ms_err is not None:
                            return self._fail(
                                f"CH2_GATE_CLOSE 실패 — {lamp}=FALSE, "
                                f"{sensor_lamp}={sensor_after} (대기 {int(wait_s)}s) "
                                f"+ MAIN_SHUTTER_CLOSE도 실패: {type(ms_err).__name__}: {ms_err}",
                                code="E331",
                            )
                        return self._fail(
                            f"CH{ch}_GATE_CLOSE 실패 — {lamp}=FALSE, "
                            f"{sensor_lamp}={sensor_after} (대기 {int(wait_s)}s)",
                            code="E305",
                        )

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
            return self._fail(f"지원하지 않는 CH: {ch}", code="E201")

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
            return self._fail(f"지원하지 않는 CH: {ch}", code="E201")

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
            # ✅ lock 획득 후 다시 확인
            busy = self._fail_if_ch_busy(ch, f"CH{ch}_CHUCK_{target_name.upper()}")
            if busy is not None:
                return busy
    
            async with self._plc_command(f"CHUCK_{target_name.upper()}_CH{ch}"):
                # 클라이언트 요청에 대응되는 Chuck 이동 파라미터를 남김
                self._log_client_request({"ch": ch, "target": target_name, "timeout_s": timeout_s})
                # (A) 현재 위치 확인 — 내부 read는 _plc_call()로 보호됨
                try:
                    cur = await self._read_chuck_position(ch)
                except Exception as e:
                    return self._fail(
                        e,
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
                        e,
                        code="E412",
                    )
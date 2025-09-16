# process_ch2.py — pure asyncio ProcessController
#  - Qt 의존성 제거 (UI만 Qt, 로직은 asyncio)
#  - main.py와는 asyncio.Queue 기반 이벤트로 통신
#  - 장비 명령은 콜백 함수로 주입 (DI)

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from time import monotonic_ns
from typing import Optional, List, Tuple, Dict, Any, Callable


# =========================
# 이벤트/토큰 구조
# =========================

@dataclass
class PCEvent:
    """
    ProcessController → main.py 로 내보내는 이벤트
    kind:
      - 'log'                : {'src', 'msg'}
      - 'state'              : {'text'}
      - 'status'             : {'running': bool}
      - 'started'            : {'params': dict}
      - 'finished'           : {'ok': bool, 'detail': dict}
      - 'aborted'            : {}
      - 'polling'            : {'active': bool}
      - 'polling_targets'    : {'targets': {'mfc':bool,'faduino':bool,'rfpulse':bool}}
    """
    kind: str
    payload: Dict[str, Any] | None = None


@dataclass(frozen=True)
class ExpectToken:
    """해당 스텝 완료 판정을 위해 필요한 '확인 토큰'."""
    kind: str        # 'MFC', 'FADUINO', 'DC_TARGET', 'RF_TARGET', 'IG_OK', 'RGA_OK', 'RFPULSE_OFF', 'GENERIC_OK', ...
    spec: Any = None # 세부 식별자 (예: 명령 문자열)

    def matches(self, other: "ExpectToken") -> bool:
        return self.kind == other.kind and (self.spec is None or self.spec == other.spec)


class ExpectGroup:
    """여러 기대 토큰이 모두 충족되어야 완료되는 그룹."""
    def __init__(self, tokens: List[ExpectToken]) -> None:
        self._tokens: List[ExpectToken] = list(tokens)
        self._fut: asyncio.Future[bool] = asyncio.get_running_loop().create_future()

    @property
    def future(self) -> asyncio.Future[bool]:
        return self._fut

    def empty(self) -> bool:
        return not self._tokens

    def match(self, incoming: ExpectToken) -> bool:
        for i, t in enumerate(self._tokens):
            if t.matches(incoming):
                del self._tokens[i]
                if not self._tokens and not self._fut.done():
                    self._fut.set_result(True)
                return True
        return False

    def match_generic_ok(self) -> bool:
        """
        하위 호환:
        - 대기중인 토큰이 '정확히 1개'일 때, 장치에서 '일반 완료' 이벤트가 오면 완료로 간주.
        """
        if len(self._tokens) == 1 and not self._fut.done():
            self._tokens.clear()
            self._fut.set_result(True)
            return True
        return False

    def cancel(self, reason: str = "cancelled") -> None:
        if not self._fut.done():
            self._fut.set_exception(asyncio.CancelledError(reason))


# =========================
# 액션/스텝 정의
# =========================

class ActionType(str, Enum):
    IG_CMD = "IG_CMD"
    RGA_SCAN = "RGA_SCAN"
    MFC_CMD = "MFC_CMD"
    FADUINO_CMD = "FADUINO_CMD"
    DELAY = "DELAY"
    DC_POWER_SET = "DC_POWER_SET"
    RF_POWER_SET = "RF_POWER_SET"
    DC_POWER_STOP = "DC_POWER_STOP"
    RF_POWER_STOP = "RF_POWER_STOP"
    OES_RUN = "OES_RUN"
    RF_PULSE_START = "RF_PULSE_START"
    RF_PULSE_STOP = "RF_PULSE_STOP"


@dataclass
class ProcessStep:
    action: ActionType
    message: str
    value: Optional[float] = None
    params: Optional[Tuple] = None
    duration: Optional[int] = None  # ms
    parallel: bool = False
    polling: bool = False
    no_wait: bool = False  # 확인 응답 없이 즉시 다음 스텝으로

    def __post_init__(self):
        if self.action == ActionType.DELAY:
            if self.duration is None:
                raise ValueError("DELAY 액션은 duration이 필요합니다.")
            if self.parallel:
                raise ValueError("DELAY는 병렬 블록에 포함할 수 없습니다.")
        if self.action in (ActionType.DC_POWER_SET, ActionType.RF_POWER_SET, ActionType.IG_CMD):
            if self.value is None:
                raise ValueError(f"{self.action.name} 액션은 value가 필요합니다.")
        if self.action == ActionType.FADUINO_CMD:
            if not self.params or len(self.params) != 2:
                raise ValueError("FADUINO_CMD params는 (cmd:str, arg:any) 형태여야 합니다.")
        if self.action == ActionType.MFC_CMD:
            if not self.params or len(self.params) != 2 or not isinstance(self.params[1], dict):
                raise ValueError("MFC_CMD params는 (cmd:str, args:dict) 형태여야 합니다.")
        if self.action == ActionType.OES_RUN:
            if not self.params or len(self.params) != 2:
                raise ValueError("OES_RUN params는 (process_time_sec:float, integration_ms:int) 형태여야 합니다.")
        if self.action == ActionType.RF_PULSE_START:
            if self.value is None:
                raise ValueError("RF_PULSE_START에는 value(타깃 파워)가 필요합니다.")
            if not self.params or len(self.params) != 2:
                raise ValueError("RF_PULSE_START params는 (freqHz|None, duty%|None) 형태여야 합니다.")


# =========================
# 프로세스 컨트롤러 (asyncio)
# =========================

class ProcessController:
    """
    순수 asyncio 버전의 공정 컨트롤러.

    통신 방식
    --------
    - 컨트롤러 → main.py : self.event_q(PCEvent)로 상태/로그/폴링 설정 등을 push
    - main.py → 컨트롤러 : 장치 완료/실패 콜백(on_*)을 호출해 기대 토큰을 충족시킴

    명령 송신은 생성자에서 전달받은 콜백을 통해 실행:
      send_faduino(cmd:str, arg:Any) -> None
      send_mfc(cmd:str, args:dict) -> None
      send_dc_power(value:float) -> None
      stop_dc_power() -> None
      send_rf_power(value:float) -> None
      stop_rf_power() -> None
      start_rfpulse(power:float, freq:Optional[int], duty:Optional[int]) -> None
      stop_rfpulse() -> None
      ig_wait(base_pressure:float) -> None
      cancel_ig() -> None
      rga_scan() -> None
      oes_run(duration_sec:float, integration_ms:int) -> None
    """

    # ===== 생성/DI =====
    def __init__(
        self,
        *,
        send_faduino: Callable[[str, Any], None],
        send_mfc: Callable[[str, Dict[str, Any]], None],
        send_dc_power: Callable[[float], None],
        stop_dc_power: Callable[[], None],
        send_rf_power: Callable[[float], None],
        stop_rf_power: Callable[[], None],
        start_rfpulse: Callable[[float, Optional[int], Optional[int]], None],
        stop_rfpulse: Callable[[], None],
        ig_wait: Callable[[float], None],
        cancel_ig: Callable[[], None],
        rga_scan: Callable[[], None],
        oes_run: Callable[[float, int], None],
    ) -> None:
        self.event_q: asyncio.Queue[PCEvent] = asyncio.Queue()
        self._send_faduino = send_faduino
        self._send_mfc = send_mfc
        self._send_dc_power = send_dc_power
        self._stop_dc_power = stop_dc_power
        self._send_rf_power = send_rf_power
        self._stop_rf_power = stop_rf_power
        self._start_rfpulse = start_rfpulse
        self._stop_rfpulse = stop_rfpulse
        self._ig_wait = ig_wait
        self._cancel_ig = cancel_ig
        self._rga_scan = rga_scan
        self._oes_run = oes_run

        # 런타임 상태
        self.is_running: bool = False
        self.current_params: Dict[str, Any] = {}
        self.process_sequence: List[ProcessStep] = []
        self._current_step_idx: int = -1

        # 제어 플래그
        self._stop_requested: bool = False
        self._aborting: bool = False
        self._in_emergency: bool = False
        self._shutdown_in_progress: bool = False
        self._shutdown_error: bool = False
        self._shutdown_failures: List[str] = []

        # 대기/카운트다운
        self._countdown_task: Optional[asyncio.Task] = None
        self._countdown_total_ms: int = 0
        self._countdown_start_ns: int = 0
        self._countdown_base_msg: str = ""

        # 기대 토큰
        self._expect_group: Optional[ExpectGroup] = None

        # 메인 러너 태스크
        self._runner_task: Optional[asyncio.Task] = None
        
        # === 전체 공정 경과 타이머 ===
        self._proc_start_ns: int = 0
        self._elapsed_task: Optional[asyncio.Task] = None

    # ===== 공정 시작/중단 API =====

    def start_process(self, params: Dict[str, Any]) -> None:
        if self.is_running:
            self._emit_log("Process", "오류: 이미 다른 공정이 실행 중입니다.")
            return

        try:
            self.current_params = params or {}
            self.process_sequence = self._create_process_sequence(self.current_params)
            ok, errors = self.validate_process_sequence()
            if not ok:
                for m in errors:
                    self._emit_log("Process", f"[시퀀스 오류] {m}")
                raise ValueError("공정 시퀀스 검증 실패")

            # 상태 초기화
            self._current_step_idx = -1
            self._stop_requested = False
            self._aborting = False
            self._in_emergency = False
            self._shutdown_in_progress = False
            self._shutdown_error = False
            self._shutdown_failures.clear()
            self._expect_group = None

            self.is_running = True

            # 전체 공정 시작 시각 저장 + 기존 경과 타이머가 있으면 정리 후 재시작
            self._proc_start_ns = monotonic_ns()
            self._cancel_elapsed()       # (아래 4)항에서 추가하는 헬퍼)
            self._elapsed_task = asyncio.create_task(self._elapsed_loop())

            # 'status'는 호환성을 위해 running=True 유지, 추가 필드만 덧붙임
            self._emit(PCEvent("status", {
                "running": True,
                "elapsed_sec": 0,
                "elapsed_hms": "00:00:00",
            }))
            self._emit(PCEvent("started", {
                "params": dict(self.current_params),
                "t0_ns": self._proc_start_ns,           # 선택: 시작 시각 전달(메인이 안 써도 무해)
            }))

            pname = self.current_params.get("process_note", "Untitled")
            self._emit_log("Process", f"=== '{pname}' 공정 시작 (총 {len(self.process_sequence)}단계) ===")

            self._runner_task = asyncio.create_task(self._runner())

        except Exception as e:
            self._emit_log("Process", f"공정 시작 오류: {e}")
            self._finish(False)

    def request_stop(self) -> None:
        if self._aborting:
            self._emit_log("Process", "정지 요청: 이미 긴급 중단 처리 중입니다.")
            return
        if self._stop_requested or self._shutdown_in_progress:
            self._emit_log("Process", "정지 요청: 이미 정지 처리 중입니다.")
            return

        self._stop_requested = True
        self._emit_log("Process", "정지 요청을 받았습니다.")

        # ✅ 즉시 종료 절차로 진입 (러너의 '다음 틱'을 기다리지 않음)
        self._start_normal_shutdown()

        # 공정 미실행 상태일 수 있으니 러너가 없다면 기동
        if not self.is_running:
            self.is_running = True
            self._current_step_idx = -1
            self._runner_task = asyncio.create_task(self._runner())

    def emergency_stop(self) -> None:
        """비상정지: 즉시 차단 시퀀스로 전환"""
        if not self.is_running:
            return
        if self._aborting:
            self._emit_log("Process", "(중복) 긴급 중단 진행 중 - 추가 호출 무시")
            return

        self._emit_log("Process", "*** 비상 정지 활성화 ***")
        self._in_emergency = True
        self._aborting = True
        self._shutdown_in_progress = True

        # 진행 중 대기/기대 취소
        self._cancel_countdown()
        if self._expect_group:
            self._expect_group.cancel("emergency")
            self._expect_group = None

        # ✅ IG 즉시 중단
        try:
            self._cancel_ig()
        except Exception:
            pass

        # 시퀀스 교체
        self.process_sequence = self._create_emergency_shutdown_sequence()
        self._current_step_idx = -1

    def reset_controller(self) -> None:
        self._cancel_countdown()
        self._cancel_elapsed()
        if self._expect_group:
            self._expect_group.cancel("reset")
            self._expect_group = None

        self.is_running = False
        self._stop_requested = False
        self._aborting = False
        self._in_emergency = False
        self._shutdown_in_progress = False
        self._shutdown_error = False
        self._shutdown_failures.clear()
        self.current_params.clear()
        self.process_sequence.clear()
        self._current_step_idx = -1
        self._emit(PCEvent("status", {"running": False}))
        self._emit_state("대기 중")
        self._emit_log("Process", "프로세스 컨트롤러가 리셋되었습니다.")

    # ===== main.py → 컨트롤러 : 장치 이벤트 콜백 =====
    # (main에서 장치 이벤트를 받으면 아래 함수를 호출)

    def on_mfc_confirmed(self, cmd: str) -> None:
        self._match_token(ExpectToken("MFC", cmd))

    def on_mfc_failed(self, cmd: str, why: str) -> None:
        self._step_failed("MFC", f"{cmd}: {why}")

    def on_faduino_confirmed(self, cmd: str) -> None:
        self._match_token(ExpectToken("FADUINO", cmd))

    def on_faduino_failed(self, cmd: str, why: str) -> None:
        self._step_failed("Faduino", f"{cmd}: {why}")

    def on_ig_ok(self) -> None:
        self._match_token(ExpectToken("IG_OK"))

    def on_ig_failed(self, src: str, why: str) -> None:
        self._step_failed(src or "IG", why)

    def on_rga_finished(self) -> None:
        self._match_token(ExpectToken("RGA_OK"))

    def on_rga_failed(self, src: str, why: str) -> None:
        self._step_failed(src or "RGA", why)

    def on_dc_target_reached(self) -> None:
        self._match_token(ExpectToken("DC_TARGET"))

    def on_rf_target_reached(self) -> None:
        self._match_token(ExpectToken("RF_TARGET"))

    def on_rf_target_failed(self, why: str) -> None:
        self._step_failed("RF Power", why or "unknown")

    def on_rf_pulse_off_finished(self) -> None:
        self._match_token(ExpectToken("RFPULSE_OFF"))

    def on_rf_pulse_failed(self, why: str) -> None:
        self._step_failed("RFPulse", why or "unknown")

    def on_device_step_ok(self) -> None:
        # 하위 호환: "power_off_finished" 같은 일반 완료 이벤트
        if self._expect_group:
            self._expect_group.match_generic_ok()

    def on_oes_ok(self) -> None:
        # OES는 no_wait로 돌도록 구성(로그만)
        self._emit_log("OES", "OES 측정 종료(정상). 메인 공정은 계속 진행됩니다.")

    def on_oes_failed(self, src: str, why: str) -> None:
        self._step_failed(src or "OES", why)

    # =========================
    # 내부: 러너/스텝 실행
    # =========================

    async def _runner(self) -> None:
        try:
            while True:
                # 정지 요청 → 종료 절차로 전환
                if (self._stop_requested and
                    not (self._aborting or self._shutdown_in_progress) and
                    not self._in_emergency):
                    self._emit_log("Process", "정지 요청 감지 - 종료 절차를 시작합니다.")
                    self._start_normal_shutdown()

                self._current_step_idx += 1
                if self._current_step_idx >= len(self.process_sequence):
                    # 성공 판정
                    if self._shutdown_in_progress:
                        ok = not (self._aborting or self._in_emergency or self._stop_requested or self._shutdown_error)
                    else:
                        ok = not (self._aborting or self._in_emergency or self._stop_requested)
                    self._finish(ok)
                    return

                step = self.process_sequence[self._current_step_idx]
                self._emit_state(step.message)
                self._emit_log("Process",
                               f"[{'종료절차' if self._shutdown_in_progress else '공정'} "
                               f"{self._current_step_idx+1}/{len(self.process_sequence)}] {step.message}")

                # 병렬 블록 수집
                if step.parallel:
                    parallel_steps: List[ProcessStep] = []
                    t = self._current_step_idx
                    while t < len(self.process_sequence) and self.process_sequence[t].parallel:
                        parallel_steps.append(self.process_sequence[t])
                        t += 1
                    self._current_step_idx = t - 1

                    need_polling = any(s.polling for s in parallel_steps)
                    self._apply_polling(need_polling)

                    # 병렬 실행: 토큰 합쳐서 하나의 ExpectGroup으로 대기
                    tokens: List[ExpectToken] = []
                    for s in parallel_steps:
                        tokens.extend(self._send_and_collect_tokens(s))
                    fut = self._set_expect(tokens)
                    if fut is not None:
                        try:
                            await fut
                        except asyncio.CancelledError:
                            # ✅ 종료 절차 전환 등으로 '대기'가 취소됨
                            # 새 시퀀스(종료 스텝)으로 계속 진행
                            continue  # while 루프의 다음 반복으로
                else:
                    # 단일 스텝
                    self._apply_polling(step.polling)
                    await self._execute_step(step)
        except asyncio.CancelledError:
            self._finish(False)
        except Exception as e:
            self._emit_log("Process", f"러너 예외: {e}")
            self._finish(False)

    async def _execute_step(self, step: ProcessStep) -> None:
        if step.action == ActionType.DELAY:
            await self._sleep_with_countdown(step.duration or 100, step.message)
            return

        tokens = self._send_and_collect_tokens(step)
        if step.no_wait or not tokens:
            # 즉시 진행
            return
        fut = self._set_expect(tokens)
        if fut is not None:
            try:
                await fut
            except asyncio.CancelledError:
                # ✅ 종료 절차로 시퀀스가 교체됨
                return  # 이 스텝을 종료하고 러너 루프로 복귀

    def _send_and_collect_tokens(self, step: ProcessStep) -> List[ExpectToken]:
        a = step.action
        tokens: List[ExpectToken] = []

        if a == ActionType.DC_POWER_SET:
            self._send_dc_power(float(step.value))
            tokens.append(ExpectToken("DC_TARGET"))
        elif a == ActionType.DC_POWER_STOP:
            self._stop_dc_power()
            tokens.append(ExpectToken("GENERIC_OK"))  # 하위 호환
        elif a == ActionType.RF_POWER_SET:
            self._send_rf_power(float(step.value))
            tokens.append(ExpectToken("RF_TARGET"))
        elif a == ActionType.RF_POWER_STOP:
            self._stop_rf_power()
            tokens.append(ExpectToken("GENERIC_OK"))  # 하위 호환
        elif a == ActionType.RF_PULSE_START:
            power = float(step.value or 0.0)
            freq = step.params[0] if step.params else None
            duty = step.params[1] if step.params else None
            self._start_rfpulse(power, freq, duty)
            tokens.append(ExpectToken("RF_TARGET"))  # 타깃 도달 동일 처리
        elif a == ActionType.RF_PULSE_STOP:
            self._stop_rfpulse()
            tokens.append(ExpectToken("RFPULSE_OFF"))
        elif a == ActionType.IG_CMD:
            self._ig_wait(float(step.value))
            tokens.append(ExpectToken("IG_OK"))
        elif a == ActionType.RGA_SCAN:
            self._rga_scan()
            tokens.append(ExpectToken("RGA_OK"))
        elif a == ActionType.FADUINO_CMD:
            cmd, arg = step.params
            self._send_faduino(cmd, arg)
            tokens.append(ExpectToken("FADUINO", cmd))
        elif a == ActionType.MFC_CMD:
            cmd, args = step.params
            self._send_mfc(cmd, dict(args))
            tokens.append(ExpectToken("MFC", cmd))
        elif a == ActionType.OES_RUN:
            dur_sec, integ_ms = step.params
            self._oes_run(float(dur_sec), int(integ_ms))
            # OES는 no_wait로 운용(별도 토큰 없음)
        else:
            raise ValueError(f"알 수 없는 Action: {a}")

        return tokens

    def _set_expect(self, tokens: List[ExpectToken]) -> Optional[asyncio.Future[bool]]:
        # 기존 대기 취소
        if self._expect_group:
            self._expect_group.cancel("replaced")
            self._expect_group = None
        if not tokens:
            return None
        self._expect_group = ExpectGroup(tokens)
        return self._expect_group.future

    def _match_token(self, token: ExpectToken) -> None:
        if not self.is_running or not self._expect_group:
            return
        self._expect_group.match(token)

    # =========================
    # DELAY/카운트다운/폴링
    # =========================
    async def _sleep_with_countdown(self, duration_ms: int, base_message: str) -> None:
        # 1) 이전 카운트다운만 중지(상태는 아직 설정 전이므로 초기화 영향 없음)
        t = self._countdown_task
        if t and not t.done():
            t.cancel()
        self._countdown_task = None

        # 2) 이번 카운트다운 상태 설정
        self._countdown_total_ms = int(duration_ms)
        self._countdown_start_ns = monotonic_ns()
        self._countdown_base_msg = base_message

        # 3) 카운트다운 루프 시작
        self._countdown_task = asyncio.create_task(self._countdown_loop())

        try:
            await asyncio.sleep(duration_ms / 1000.0)
        finally:
            # 종료 시에만 상태까지 정리
            self._cancel_countdown()

    async def _countdown_loop(self) -> None:
        try:
            while True:
                elapsed_ms = (monotonic_ns() - self._countdown_start_ns) // 1_000_000
                remaining_ms = max(0, self._countdown_total_ms - int(elapsed_ms))
                # 표시 보정: 59999ms도 60초로 보이도록 천의 자리 올림
                rem_s = (remaining_ms + 999) // 1000
                m, s = divmod(rem_s, 60)
                tstr = f"{m}분 {s}초" if m > 0 else f"{s}초"
                self._emit_state(f"{self._countdown_base_msg} (남은 시간: {tstr})")
                if remaining_ms == 0:
                    return
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            return

    def _cancel_countdown(self) -> None:
        if self._countdown_task and not self._countdown_task.done():
            self._countdown_task.cancel()
        self._countdown_task = None
        self._countdown_total_ms = 0
        self._countdown_start_ns = 0
        self._countdown_base_msg = ""

    def _apply_polling(self, active: bool) -> None:
        self._emit(PCEvent("polling", {"active": bool(active)}))
        self._emit(PCEvent("polling_targets", {"targets": self._compute_polling_targets(active)}))

    def _compute_polling_targets(self, active: bool) -> Dict[str, bool]:
        if not active:
            return {'mfc': False, 'faduino': False, 'rfpulse': False}
        info = self._get_common_process_info(self.current_params or {})
        if info.get('use_rf_pulse', False):
            return {'mfc': True, 'faduino': False, 'rfpulse': True}
        return {'mfc': True, 'faduino': True, 'rfpulse': False}

    # =========================
    # 종료/실패 처리
    # =========================

    def _start_normal_shutdown(self) -> None:
        if self._aborting:
            self._emit_log("Process", "종료 절차 무시: 이미 긴급 중단 중입니다.")
            return
        if self._shutdown_in_progress:
            self._emit_log("Process", "종료 절차 무시: 이미 종료 절차 진행 중입니다.")
            return

        self._shutdown_in_progress = True
        self._emit_log("Process", "정지 요청 - 안전한 종료 절차를 시작합니다.")

        # ✅ IG 폴링/재점등을 즉시 중단 (SIG 0 전송은 IG 내부에서 응답 무시로 처리)
        try:
            self._cancel_ig()
        except Exception:
            pass

        self._cancel_countdown()
        if self._expect_group:
            self._expect_group.cancel("shutdown")
            self._expect_group = None

        try:
            shutdown_steps = self._create_shutdown_sequence(self.current_params or {})
            if shutdown_steps:
                self._emit_log("Process", f"종료 절차 생성 완료: {len(shutdown_steps)}단계")
                self.process_sequence = shutdown_steps
                self._current_step_idx = -1  # 러너가 다음 틱에 처음부터 실행
            else:
                self._emit_log("Process", "종료 절차가 없어서 즉시 완료합니다.")
                self._finish(False)
        except Exception as e:
            self._emit_log("Process", f"종료 절차 시작 오류: {e}")
            self._finish(False)

    def _step_failed(self, source: str, reason: str) -> None:
        if not self.is_running:
            return

        full = f"[{source} - {reason}]"
        cur = self.current_step

        if self._aborting or self._shutdown_in_progress:
            # 종료 중 실패는 기록 후 계속
            step_no = self._current_step_idx + 1
            act = cur.action.name if cur else "UNKNOWN"
            self._shutdown_error = True
            self._shutdown_failures.append(f"Step {step_no} {act}: {full}")
            self._emit_log("Process", f"경고: 종료 중 단계 실패 → 계속 진행 ({act}, 사유: {full})")
            # 종료 흐름에서의 진행은 러너가 계속 처리
            # (특별히 토큰 대기 중이었다면 취소)
            if self._expect_group:
                self._expect_group.cancel("failure-during-shutdown")
                self._expect_group = None
            return

        # 평시 실패 → 안전 종료로 전환
        self._emit_log("Process", f"오류 발생: {full}. 종료 절차를 시작합니다.")
        self._start_normal_shutdown()

    def _finish(self, ok: bool) -> None:
        if not self.is_running:
            return

        detail = {
            "process_name": self.current_params.get("process_note",
                                                    self.current_params.get("Process_name", "Untitled")),
            "stopped": self._stop_requested,
            "aborting": (self._aborting or self._in_emergency),
            "errors": list(self._shutdown_failures),
        }

        self.is_running = False
        self._cancel_countdown()
        self._cancel_elapsed()
        if self._expect_group:
            self._expect_group.cancel("finish")
            self._expect_group = None

        # 상태 리셋
        self._shutdown_in_progress = False
        self._stop_requested = False
        self._aborting = False
        self._in_emergency = False

        self._emit_log("Process", "=== 공정이 성공적으로 완료되었습니다 ===" if ok
                       else "=== 공정이 중단되었습니다 ===")
        if not ok and detail["errors"]:
            self._emit_log("Process", f"[종료 중 실패 요약] 총 {len(detail['errors'])}건")
            for item in detail["errors"]:
                self._emit_log("Process", f" - {item}")

        self._emit(PCEvent("status", {"running": False}))
        self._emit_state("공정 완료" if ok else "공정 중단됨")
        self._emit(PCEvent("finished", {"ok": ok, "detail": detail}))
        if (self._aborting or self._in_emergency) and not ok:
            self._emit(PCEvent("aborted", {}))

    # =========================
    # 시퀀스 생성/검증/요약
    # =========================

    def _get_common_process_info(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'use_ms': bool(params.get("use_ms", False)),
            'use_dc': bool(params.get("use_dc_power", False)) and float(params.get("dc_power", 0)) > 0,
            'use_rf': bool(params.get("use_rf_power", False)) and float(params.get("rf_power", 0)) > 0,
            'use_rf_pulse': bool(params.get("use_rf_pulse", False)) and float(params.get("rf_pulse_power", 0)) > 0,
            'gas_info': {"Ar": {"channel": 1}, "O2": {"channel": 2}, "N2": {"channel": 3}},
            'gun_shutters': ["G1", "G2", "G3"],
        }

    def _create_process_sequence(self, params: Dict[str, Any]) -> List[ProcessStep]:
        common_info = self._get_common_process_info(params)
        use_dc, use_rf, use_ms = common_info['use_dc'], common_info['use_rf'], common_info['use_ms']
        gas_info, gun_shutters = common_info['gas_info'], common_info['gun_shutters']

        base_pressure = float(params.get("base_pressure", 1e-5))
        working_pressure = float(params.get("working_pressure", 0))
        process_time_min = float(params.get("process_time", 0))
        shutter_delay_min = float(params.get("shutter_delay", 0))
        shutter_delay_sec = shutter_delay_min * 60.0
        process_time_sec = process_time_min * 60.0
        dc_power = float(params.get("dc_power", 0))
        rf_power = float(params.get("rf_power", 0))
        integration_ms = int(params.get("integration_time", 60))

        steps: List[ProcessStep] = []

        # --- 초기화 ---
        self._emit_log("Process", "공정 시작")
        steps.append(ProcessStep(
            action=ActionType.IG_CMD,
            value=base_pressure,
            message=f'베이스 압력({base_pressure:.1e}) 도달 대기'
        ))
        # 모든 채널 Flow OFF
        for gas, info in gas_info.items():
            steps.append(ProcessStep(
                action=ActionType.MFC_CMD,
                params=('FLOW_OFF', {'channel': info["channel"]}),
                message=f'Ch{info["channel"]}({gas}) Flow Off'
            ))
        steps.extend([
            ProcessStep(action=ActionType.MFC_CMD, params=('VALVE_OPEN', {}), message='MFC Valve Open'),
            ProcessStep(action=ActionType.MFC_CMD, params=('PS_ZEROING', {}), message='압력 센서 Zeroing'),
        ])
        for gas, info in gas_info.items():
            steps.append(ProcessStep(
                action=ActionType.MFC_CMD,
                params=('MFC_ZEROING', {'channel': info["channel"]}),
                message=f'Ch{info["channel"]}({gas}) Zeroing'
            ))

        # --- 가스 주입 ---
        steps.append(ProcessStep(
            action=ActionType.FADUINO_CMD, params=('MV', True), message='메인 밸브 열기'
        ))
        for gas, info in gas_info.items():
            if params.get(f"use_{gas.lower()}", False):
                flow_value = float(params.get(f"{gas.lower()}_flow", 0))
                steps.extend([
                    ProcessStep(
                        action=ActionType.FADUINO_CMD, params=(gas, True), message=f'{gas} 밸브 열기'
                    ),
                    ProcessStep(
                        action=ActionType.MFC_CMD,
                        params=('FLOW_SET', {'channel': info["channel"], 'value': flow_value}),
                        message=f'Ch{info["channel"]}({gas}) 유량 {flow_value}sccm 설정'
                    ),
                    ProcessStep(
                        action=ActionType.MFC_CMD,
                        params=('FLOW_ON', {'channel': info["channel"]}),
                        message=f'Ch{info["channel"]}({gas}) 유량 공급 시작'
                    )
                ])

        # --- 압력 제어 시작 ---
        steps.extend([
            ProcessStep(action=ActionType.MFC_CMD, params=('SP4_ON', {}), message='압력 제어(SP4) 시작'),
            ProcessStep(action=ActionType.MFC_CMD, params=('SP1_SET', {'value': working_pressure}),
                        message=f'목표 압력(SP1) {working_pressure:.2f} 설정'),
            ProcessStep(action=ActionType.DELAY, duration=60000, message='압력 안정화 대기 (60초)'),
        ])

        # --- 파워/셔터 ---
        # Gun Shutter 열기
        for shutter in gun_shutters:
            if params.get(f"use_{shutter.lower()}", False):
                steps.append(ProcessStep(
                    action=ActionType.FADUINO_CMD,
                    params=(shutter, True),
                    message=f'Gun Shutter {shutter} 열기'
                ))

        # Power_select: N2(채널3) 릴레이 ON
        if bool(params.get("use_power_select", False)):
            steps.append(ProcessStep(
                action=ActionType.FADUINO_CMD, params=("N2", True),
                message="Power_select: N2 가스 밸브(Ch3) ON"
            ))

        use_rf_pulse = bool(params.get("use_rf_pulse", False)) and float(params.get("rf_pulse_power", 0)) > 0.0
        rf_pulse_power = float(params.get("rf_pulse_power", 0))
        rf_pulse_freq = params.get("rf_pulse_freq", None)
        if rf_pulse_freq is not None:
            rf_pulse_freq = int(rf_pulse_freq)
        rf_pulse_duty = params.get("rf_pulse_duty", None)
        if rf_pulse_duty is not None:
            rf_pulse_duty = int(rf_pulse_duty)

        want_parallel = use_dc and (use_rf or use_rf_pulse)

        if use_dc:
            steps.append(ProcessStep(
                action=ActionType.DC_POWER_SET, value=dc_power,
                message=f'DC Power {dc_power}W 설정', parallel=want_parallel
            ))

        if use_rf_pulse:
            f_txt = f"{rf_pulse_freq}Hz" if rf_pulse_freq is not None else "keep"
            d_txt = f"{rf_pulse_duty}%" if rf_pulse_duty is not None else "keep"
            steps.append(ProcessStep(
                action=ActionType.RF_PULSE_START, value=rf_pulse_power,
                params=(rf_pulse_freq, rf_pulse_duty),
                message=f'RF Pulse 설정 및 ON (P={rf_pulse_power}W, f={f_txt}, duty={d_txt})',
                parallel=want_parallel
            ))
        elif use_rf:
            steps.append(ProcessStep(
                action=ActionType.RF_POWER_SET, value=rf_power,
                message=f'RF Power {rf_power}W 설정', parallel=want_parallel
            ))

        if use_rf_pulse:
            steps.append(ProcessStep(
                action=ActionType.DELAY, duration=20_000, message='Power Delay 20초'
            ))

        steps.append(ProcessStep(
            action=ActionType.MFC_CMD, params=('SP1_ON', {}), message='압력 제어(SP1) 시작'
        ))

        if shutter_delay_sec > 0:
            steps.append(ProcessStep(
                action=ActionType.DELAY,
                duration=int(round(shutter_delay_sec * 1000.0)),
                message=f'Shutter Delay {shutter_delay_min}분'
            ))

        if use_ms:
            steps.append(ProcessStep(
                action=ActionType.FADUINO_CMD, params=('MS', True), message='Main Shutter 열기'
            ))

        # --- 메인 공정 시간 ---
        if process_time_sec > 0:
            steps.append(ProcessStep(
                action=ActionType.OES_RUN,
                params=(process_time_sec, integration_ms),
                message=f'OES 측정 시작 ({process_time_min}분, {integration_ms}ms)',
                no_wait=True  # 백그라운드로 돌리고 즉시 다음 DELAY로
            ))
            steps.append(ProcessStep(
                action=ActionType.DELAY,
                duration=int(round(process_time_sec * 1000.0)),
                message=f'메인 공정 진행 ({process_time_min}분)',
                polling=True
            ))

        # --- 종료 시퀀스 ---
        steps.extend(self._create_shutdown_sequence(params))
        return steps

    def _create_shutdown_sequence(self, params: Dict[str, Any], *, force_all: bool = False) -> List[ProcessStep]:
        steps: List[ProcessStep] = []
        info = self._get_common_process_info(params)

        use_dc = force_all or info['use_dc']
        use_rf = force_all or info['use_rf']
        use_rf_pulse = force_all or info['use_rf_pulse']
        gas_info = info['gas_info']
        gun_shutters = info['gun_shutters']

        steps.append(ProcessStep(
            action=ActionType.FADUINO_CMD, params=('MS', False), message='Main Shutter 닫기 (항상)'
        ))

        if use_dc:
            steps.append(ProcessStep(action=ActionType.DC_POWER_STOP, message='DC Power Off'))
        if use_rf:
            steps.append(ProcessStep(action=ActionType.RF_POWER_STOP, message='RF Power Off'))
        if use_rf_pulse:
            steps.append(ProcessStep(action=ActionType.RF_PULSE_STOP, message='RF Pulse Off'))

        for gas, info_ch in gas_info.items():
            steps.append(ProcessStep(
                action=ActionType.MFC_CMD,
                params=('FLOW_OFF', {'channel': info_ch["channel"]}),
                message=f'Ch{info_ch["channel"]}({gas}) Flow Off'
            ))

        steps.append(ProcessStep(
            action=ActionType.MFC_CMD, params=('VALVE_OPEN', {}), message='전체 MFC Valve Open'
        ))

        for shutter in gun_shutters:
            if params.get(f"use_{shutter.lower()}", False) or force_all:
                steps.append(ProcessStep(
                    action=ActionType.FADUINO_CMD, params=(shutter, False), message=f'Gun Shutter {shutter} 닫기'
                ))

        if bool(params.get("use_power_select", False)) or force_all:
            steps.append(ProcessStep(
                action=ActionType.FADUINO_CMD, params=("N2", False),
                message="Power_select 종료: N2 가스 밸브(Ch3) OFF"
            ))

        for gas in info['gas_info']:
            steps.append(ProcessStep(
                action=ActionType.FADUINO_CMD, params=(gas, False), message=f'Faduino {gas} 밸브 닫기'
            ))

        steps.append(ProcessStep(
            action=ActionType.FADUINO_CMD, params=('MV', False), message='메인 밸브 닫기'
        ))

        self._emit_log("Process", "종료 절차가 생성되었습니다.")
        return steps

    def _create_emergency_shutdown_sequence(self) -> List[ProcessStep]:
        if not self.current_params:
            return []

        info = self._get_common_process_info(self.current_params)
        steps: List[ProcessStep] = []

        steps.append(ProcessStep(
            action=ActionType.FADUINO_CMD, params=('MS', False),
            message='[긴급] Main Shutter 즉시 닫기', no_wait=True
        ))

        both = info['use_dc'] and (info['use_rf'] or info['use_rf_pulse'])
        if info['use_dc']:
            steps.append(ProcessStep(
                action=ActionType.DC_POWER_STOP, message='[긴급] DC Power 즉시 차단',
                parallel=both, no_wait=True
            ))
        if info['use_rf']:
            steps.append(ProcessStep(
                action=ActionType.RF_POWER_STOP, message='[긴급] RF Power 즉시 차단',
                parallel=both, no_wait=True
            ))
        if info['use_rf_pulse']:
            steps.append(ProcessStep(
                action=ActionType.RF_PULSE_STOP, message='[긴급] RF Pulse 즉시 차단',
                parallel=both, no_wait=True
            ))

        for gas in ["Ar", "O2", "N2"]:
            if self.current_params.get(f"use_{gas.lower()}", False):
                steps.append(ProcessStep(
                    action=ActionType.FADUINO_CMD, params=(gas, False),
                    message=f'[긴급] {gas} 가스 즉시 차단', no_wait=True
                ))

        steps.append(ProcessStep(
            action=ActionType.FADUINO_CMD, params=('MV', False),
            message='[긴급] 메인 밸브 즉시 닫기', no_wait=True
        ))

        self._emit_log("Process", "긴급 종료 절차가 생성되었습니다.")
        return steps

    # ===== 조회/검증 =====

    @property
    def current_step(self) -> Optional[ProcessStep]:
        if 0 <= self._current_step_idx < len(self.process_sequence):
            return self.process_sequence[self._current_step_idx]
        return None

    @property
    def progress(self) -> float:
        if not self.process_sequence:
            return 0.0
        return (self._current_step_idx + 1) / len(self.process_sequence)

    def get_remaining_steps(self) -> List[ProcessStep]:
        if self._current_step_idx < 0:
            return self.process_sequence.copy()
        return self.process_sequence[self._current_step_idx + 1:]

    def get_process_summary(self) -> Dict[str, Any]:
        # 시작시각 기반 경과 계산 (is_running이 아닐 때는 0)
        if self.is_running and self._proc_start_ns:
            sec = int((monotonic_ns() - self._proc_start_ns) / 1_000_000_000)
        else:
            sec = 0
        h = sec // 3600
        m = (sec % 3600) // 60
        s = sec % 60
        hms = f"{h:02d}:{m:02d}:{s:02d}"

        return {
            'total_steps': len(self.process_sequence),
            'current_step': self._current_step_idx + 1,
            'progress': self.progress,
            'is_running': self.is_running,
            'is_parallel': False,
            'current_step_info': ({
                'action': self.current_step.action.name,
                'message': self.current_step.message,
                'parallel': self.current_step.parallel
            } if self.current_step else None),
            'process_name': self.current_params.get('process_note', 'Untitled'),
            'stop_requested': self._stop_requested,
            'aborting': self._aborting,
            # === 추가: 전체 공정 경과 ===
            'elapsed_sec': sec,
            'elapsed_hms': hms,
        }

    def validate_process_sequence(self) -> Tuple[bool, List[str]]:
        errors: List[str] = []
        try:
            in_parallel = False
            for i, step in enumerate(self.process_sequence):
                if step.parallel and not in_parallel:
                    in_parallel = True
                elif not step.parallel and in_parallel:
                    in_parallel = False

                n = i + 1
                if step.action == ActionType.DELAY and step.duration is None:
                    errors.append(f"Step {n}: DELAY 액션에 duration이 없습니다.")
                if step.action in [ActionType.DC_POWER_SET, ActionType.RF_POWER_SET, ActionType.IG_CMD]:
                    if step.value is None:
                        errors.append(f"Step {n}: {step.action.name} 액션에 value가 없습니다.")
                if step.action == ActionType.RF_PULSE_START:
                    if step.value is None:
                        errors.append(f"Step {n}: RF_PULSE_START에 value(파워)가 없습니다.")
                    if step.params is None or len(step.params) != 2:
                        errors.append(f"Step {n}: RF_PULSE_START params=(freq, duty) 필요.")
                if step.action in [ActionType.FADUINO_CMD, ActionType.MFC_CMD, ActionType.OES_RUN]:
                    if step.params is None:
                        errors.append(f"Step {n}: {step.action.name} 액션에 params가 없습니다.")
                if step.parallel and i > 0:
                    prev = self.process_sequence[i - 1]
                    if not prev.parallel:
                        errors.append(f"Step {n}: 병렬 스텝이 비병렬 스텝 뒤에 있습니다.")
        except Exception as e:
            errors.append(f"검증 중 오류 발생: {e}")
        return len(errors) == 0, errors

    def get_estimated_duration(self) -> int:
        return sum((s.duration or 0) for s in self.process_sequence if s.action == ActionType.DELAY)

    # =========================
    # 유틸: 이벤트 방출
    # =========================

    def _emit(self, ev: PCEvent) -> None:
        try:
            self.event_q.put_nowait(ev)
        except asyncio.QueueFull:
            # 이 케이스는 거의 없겠지만, 유실 방지를 위해 블록
            asyncio.create_task(self.event_q.put(ev))

    def _emit_log(self, src: str, msg: str) -> None:
        self._emit(PCEvent("log", {"src": src, "msg": msg}))

    def _emit_state(self, text: str) -> None:
        self._emit(PCEvent("state", {"text": text}))

    async def _elapsed_loop(self) -> None:
        """
        전체 공정 경과 타이머 (시작시각 기반, 1초 주기).
        - self.is_running이 False가 되면 자동 종료
        - 기존 'status' 이벤트에 elapsed 필드만 추가(호환성 유지)
        """
        last_sec = -1
        try:
            while self.is_running and self._proc_start_ns:
                now_ns = monotonic_ns()
                sec = int((now_ns - self._proc_start_ns) / 1_000_000_000)
                if sec != last_sec:
                    last_sec = sec
                    h = sec // 3600
                    m = (sec % 3600) // 60
                    s = sec % 60
                    hms = f"{h:02d}:{m:02d}:{s:02d}"
                    # running=True는 유지 + 추가 필드만 덧붙임
                    self._emit(PCEvent("status", {
                        "running": True,
                        "elapsed_sec": sec,
                        "elapsed_hms": hms,
                    }))
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            return

    def _cancel_elapsed(self) -> None:
        t = self._elapsed_task
        if t and not t.done():
            t.cancel()
        self._elapsed_task = None



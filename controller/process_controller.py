# process_controller.py
#  - Qt 의존성 제거 (UI만 Qt, 로직은 asyncio)
#  - main.py와는 asyncio.Queue 기반 이벤트로 통신
#  - 장비 명령은 콜백 함수로 주입 (DI)

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from time import monotonic_ns
from typing import Optional, List, Tuple, Dict, Any, Callable
from lib.config_common import SHUTDOWN_STEP_TIMEOUT_MS, SHUTDOWN_STEP_GAP_MS


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
      - 'polling_targets'    : {'targets': {'mfc':bool, 'dc':bool, 'rf':bool,
                                            'dc_pulse':bool, 'rf_pulse':bool}}
    """
    kind: str
    payload: Dict[str, Any] | None = None


@dataclass(frozen=True)
class ExpectToken:
    """해당 스텝 완료 판정을 위해 필요한 '확인 토큰'."""
    kind: str        # 'MFC','PLC','DC_TARGET','RF_TARGET','IG_OK','RGA_OK','DCPULSE_OFF','RFPULSE_OFF','GENERIC_OK', ...
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
        if (
            len(self._tokens) == 1
            and self._tokens[0].kind == "GENERIC_OK"
            and not self._fut.done()
        ):
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
    PLC_CMD = "PLC_CMD"
    DELAY = "DELAY"
    DC_POWER_SET = "DC_POWER_SET"
    RF_POWER_SET = "RF_POWER_SET"
    DC_POWER_STOP = "DC_POWER_STOP"
    RF_POWER_STOP = "RF_POWER_STOP"
    OES_RUN = "OES_RUN"
    # 펄스 완전 분리
    DC_PULSE_START = "DC_PULSE_START"
    DC_PULSE_STOP  = "DC_PULSE_STOP"
    RF_PULSE_START = "RF_PULSE_START"
    RF_PULSE_STOP  = "RF_PULSE_STOP"


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
        if self.action == ActionType.PLC_CMD:
            if not self.params or len(self.params) not in (2, 3):
                raise ValueError("PLC_CMD params는 (name:str, on:any[, ch:int]) 형태여야 합니다.")
        if self.action == ActionType.MFC_CMD:
            if not self.params or len(self.params) != 2 or not isinstance(self.params[1], dict):
                raise ValueError("MFC_CMD params는 (cmd:str, args:dict) 형태여야 합니다.")
        if self.action == ActionType.OES_RUN:
            if not self.params or len(self.params) != 2:
                raise ValueError("OES_RUN params는 (process_time_sec:float, integration_ms:int) 형태여야 합니다.")
        if self.action == ActionType.DC_PULSE_START:
            if self.value is None:
                raise ValueError("DC_PULSE_START에는 value(타깃 파워)가 필요합니다.")
            if not self.params or len(self.params) != 2:
                raise ValueError("DC_PULSE_START params는 (freqkHz|None, duty%|None) 형태여야 합니다.")
        if self.action == ActionType.RF_PULSE_START:
            if self.value is None:
                raise ValueError("RF_PULSE_START에는 value(타깃 파워)가 필요합니다.")
            if not self.params or len(self.params) != 2:
                raise ValueError("RF_PULSE_START params는 (freqkHz|None, duty%|None) 형태로 받고 내부에서 Hz로 변환합니다.")


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
      send_plc(cmd:str, arg:Any, ch:int) -> None
      send_mfc(cmd:str, args:dict) -> None
      # 연속
      send_dc_power(value:float), stop_dc_power()
      send_rf_power(value:float), stop_rf_power()
      # 펄스
      start_dc_pulse(power:float, freq:Optional[int], duty:Optional[int]), stop_dc_pulse()
      start_rf_pulse(power:float, freq:Optional[int], duty:Optional[int]), stop_rf_pulse()
      ig_wait(base_pressure:float) -> None
      cancel_ig() -> None
      rga_scan() -> None
      oes_run(duration_sec:float, integration_ms:int) -> None
    """

    # ===== 생성/DI =====
    def __init__(self, *,
        send_plc: Callable[[str, Any, int], None],
        send_mfc: Callable[[str, Dict[str, Any]], None],

        # 연속 파워
        send_dc_power: Callable[[float], None],
        stop_dc_power: Callable[[], None],
        send_rf_power: Callable[[float], None],
        stop_rf_power: Callable[[], None],

        # 펄스 파워 (완전 분리)
        start_dc_pulse: Callable[[float, Optional[int], Optional[int]], None],
        stop_dc_pulse: Callable[[], None],
        start_rf_pulse: Callable[[float, Optional[int], Optional[int]], None],
        stop_rf_pulse: Callable[[], None],

        ig_wait: Callable[[float], None],
        cancel_ig: Callable[[], None],
        rga_scan: Callable[[], None],
        oes_run: Callable[[float, int], None],

        ch: int,
        supports_dc_cont: bool,
        supports_rf_cont: bool,
        supports_dc_pulse: bool,
        supports_rf_pulse: bool,
    ) -> None:
        self.event_q: asyncio.Queue[PCEvent] = asyncio.Queue(maxsize=2000)
        self._send_plc = send_plc                   # 🔁 보관 멤버도 교체
        self._send_mfc = send_mfc
        self._send_dc_power = send_dc_power
        self._stop_dc_power = stop_dc_power
        self._send_rf_power = send_rf_power
        self._stop_rf_power = stop_rf_power
        self._start_dc_pulse = start_dc_pulse
        self._stop_dc_pulse  = stop_dc_pulse
        self._start_rf_pulse = start_rf_pulse
        self._stop_rf_pulse  = stop_rf_pulse
        self._ig_wait = ig_wait
        self._cancel_ig = cancel_ig
        self._rga_scan = rga_scan
        self._oes_run = oes_run

        # ⬇️ 추가: 챔버/지원능력
        self._ch = int(ch)
        self._supports_dc_cont = bool(supports_dc_cont)
        self._supports_rf_cont = bool(supports_rf_cont)
        self._supports_dc_pulse = bool(supports_dc_pulse)
        self._supports_rf_pulse = bool(supports_rf_pulse)

        # ✅ 항상 존재하는 가스 채널 맵(소스 오브 트루스)
        self._gas_info = {"AR": {"channel": 1}, "O2": {"channel": 2}, "N2": {"channel": 3}}

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
    
        # ✅ 추가: 런타임 실패 전파 플래그
        self._process_failed: bool = False

        # 대기/카운트다운
        self._countdown_task: Optional[asyncio.Task] = None
        self._countdown_total_ms: int = 0
        self._countdown_start_ns: int = 0
        self._countdown_base_msg: str = ""

        # 기대 토큰
        self._expect_group: Optional[ExpectGroup] = None

        # 메인 러너 태스크
        self._runner_task: Optional[asyncio.Task] = None

        # ✅ 즉시 중단 신호 (모든 대기에서 경쟁)
        self._abort_evt: asyncio.Event = asyncio.Event()
        
        # === 전체 공정 경과 타이머 ===
        self._proc_start_ns: int = 0
        self._elapsed_task: Optional[asyncio.Task] = None

        # === 폴링 상태 캐시(에지 트리거용) ===
        self._last_polling_active: Optional[bool] = None
        self._last_polling_targets: Optional[dict] = None

        # === 토큰 소유권 맵: (kind, spec) -> step_idx ===
        self._token_owner: Dict[Tuple[str, Any], int] = {}

    # ===== 공정 시작/중단 API =====

    def start_process(self, params: Dict[str, Any]) -> None:
        if self.is_running:
            self._emit_log("Process", "오류: 이미 다른 공정이 실행 중입니다.")
            return

        try:
            self._token_owner.clear()
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

            # ✅ 추가: 이번 런은 실패 아님으로 초기화
            self._process_failed = False

            self.is_running = True
            # ✅ 이전 런의 abort 상태 초기화
            self._abort_evt.clear()

            # 폴링 상태 캐시 초기화(처음 1회는 반드시 이벤트 발행되도록)
            self._last_polling_active = None
            self._last_polling_targets = None

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

        # ✅ 모든 대기 즉시 중단
        self._abort_evt.set()

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

        # ✅ 어떤 대기든 즉시 끊는다
        self._abort_evt.set()

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

        # ✅ 추가: 리셋 시에도 초기화
        self._process_failed = False
        
        # 폴링 캐시 초기화
        self._last_polling_active = None
        self._last_polling_targets = None

        self._abort_evt.clear()  # ✅ 리셋 시 abort 상태 초기화

        # 토큰 소유권 맵 초기화
        self._token_owner.clear()
        self._emit(PCEvent("status", {"running": False}))
        self._emit_state("대기 중")
        self._emit_log("Process", "프로세스 컨트롤러가 리셋되었습니다.")

    # ===== main.py → 컨트롤러 : 장치 이벤트 콜백 =====
    # (main에서 장치 이벤트를 받으면 아래 함수를 호출)

    def on_mfc_confirmed(self, cmd: str) -> None:
        self._match_token(ExpectToken("MFC", cmd))

    def on_mfc_failed(self, cmd: str, why: str) -> None:
        self._step_failed("MFC", f"{cmd}: {why}")

    def on_plc_confirmed(self, cmd: str) -> None:
        self._match_token(ExpectToken("PLC", cmd))

    def on_plc_failed(self, cmd: str, why: str) -> None:
        self._step_failed("PLC", f"{cmd}: {why}")

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

    def on_dc_pulse_target_reached(self) -> None:
        self._match_token(ExpectToken("DC_PULSE_TARGET"))

    def on_dc_pulse_off_finished(self) -> None:
        self._match_token(ExpectToken("DCPULSE_OFF"))

    def on_dc_pulse_failed(self, why: str) -> None:
        self._step_failed("DCPulse", why or "unknown")

    def on_rf_pulse_target_reached(self) -> None:
        # RF 펄스 타깃 도달은 연속 RF와 동일 판정으로 통일
        self._match_token(ExpectToken("RF_TARGET"))

    def on_rf_pulse_off_finished(self) -> None:
        self._match_token(ExpectToken("RFPULSE_OFF"))

    def on_rf_pulse_failed(self, why: str) -> None:
        self._step_failed("RFPulse", why or "unknown")

    def on_device_step_ok(self) -> None:
        # 일반 OK는 해당 스텝이 실제로 GENERIC_OK를 요구할 때만 인정
        self._match_token(ExpectToken("GENERIC_OK"))

    def on_oes_ok(self) -> None:
        # OES는 no_wait로 돌도록 구성(로그만)
        self._emit_log("OES", "OES 측정 종료(정상).")

    def on_oes_failed(self, src: str, why: str) -> None:
        # OES는 공정 비차단: 로그만 남기고 계속 진행
        self._emit_log(src or "OES", f"오류 무시하고 계속: {why}")
        # 혹시 현재 대기가 'GENERIC_OK' 하나만 기다리는 상황이면 통과시켜 준다(있어도/없어도 무해)
        if self._expect_group:
            self._expect_group.match_generic_ok()

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
                    # ✅ 변경: 실패 플래그를 일괄 반영
                    base_ok = not (self._aborting or self._in_emergency or self._stop_requested)
                    if self._shutdown_in_progress:
                        ok = base_ok and not self._shutdown_error and not self._process_failed
                    else:
                        ok = base_ok and not self._process_failed
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
                    owners: Dict[Tuple[str, Any], int] = {}

                    # 더 안전하게: 블록 시작 인덱스를 별도 계산
                    block_start = t - len(parallel_steps)  # t는 위에서 병렬 수집에 쓰던 인덱스

                    for j, s in enumerate(parallel_steps):
                        tks = self._send_and_collect_tokens(s)
                        tokens.extend(tks)
                        for tk in tks:
                            owners[self._tokey(tk)] = block_start + j  # 각 스텝의 실제 인덱스에 귀속

                    if tokens:
                        self._register_token_owners(owners)

                    fut = self._set_expect(tokens)

                    if fut is not None:
                        try:
                            if self._shutdown_in_progress:
                                # ✅ 종료 시퀀스: abort 무시 + 타임아웃
                                try:
                                    await asyncio.wait_for(fut, timeout=max(0.001, SHUTDOWN_STEP_TIMEOUT_MS) / 1000.0)
                                except asyncio.TimeoutError:
                                    self._emit_log("Process", "종료(병렬) 스텝 확인 시간 초과 → 다음으로")
                                # 병렬 블록 이후도 간격 보장
                                if SHUTDOWN_STEP_GAP_MS > 0:
                                    await asyncio.sleep(SHUTDOWN_STEP_GAP_MS / 1000.0)
                            else:
                                # 평시: abort와 경쟁
                                aborted = await self._wait_or_abort(fut, allow_abort=not self._in_emergency)
                                if aborted:
                                    if self._expect_group:
                                        self._expect_group.cancel("abort")
                                        self._expect_group = None
                                    continue
                        except asyncio.CancelledError:
                            continue
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
            # ✅ 메인 공정 DELAY처럼 polling=True인 경우, 스텝 진입 순간 다시 한 번 확실히 ON 적용
            if step.polling:
                # 러너에서 직전에 _apply_polling(True)를 호출하지만,
                # 병렬블록/즉시반환 스텝 뒤 신호 타이밍 문제로 유실될 수 있어 재보장
                self._apply_polling(True)
            await self._sleep_with_countdown(step.duration or 100, step.message)
            return

        tokens = self._send_and_collect_tokens(step)

        # ⬇️ 추가: 이 스텝이 만든 토큰은 현재 스텝 인덱스에 귀속
        if tokens:
            owners = { self._tokey(tk): self._current_step_idx for tk in tokens }
            self._register_token_owners(owners)

        if step.no_wait or not tokens:
            return

        fut = self._set_expect(tokens)
        if fut is not None:
            try:
                if self._shutdown_in_progress:
                    # ✅ 종료 시퀀스: abort 무시 + 타임아웃 대기
                    try:
                        await asyncio.wait_for(fut, timeout=max(0.001, SHUTDOWN_STEP_TIMEOUT_MS) / 1000.0)
                    except asyncio.TimeoutError:
                        self._emit_log("Process", "종료 스텝 확인 시간 초과 → 다음 스텝 진행")
                else:
                    # 평시: abort와 경쟁(비상 상황이면 abort 무시하지 않고 즉시 전환)
                    aborted = await self._wait_or_abort(fut, allow_abort=not self._in_emergency)
                    if aborted:
                        if self._expect_group:
                            self._expect_group.cancel("abort")
                            self._expect_group = None
                        return
            except asyncio.CancelledError:
                return

        # ✅ 종료 시퀀스일 때는 스텝 간 최소 간격 보장
        if self._shutdown_in_progress and SHUTDOWN_STEP_GAP_MS > 0:
            await asyncio.sleep(SHUTDOWN_STEP_GAP_MS / 1000.0)

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

        elif a == ActionType.DC_PULSE_START:
            power = float(step.value or 0.0)
            freq = step.params[0] if step.params else None
            duty = step.params[1] if step.params else None
            self._start_dc_pulse(power, freq, duty)
            tokens.append(ExpectToken("DC_PULSE_TARGET"))
        elif a == ActionType.DC_PULSE_STOP:
            self._stop_dc_pulse()
            tokens.append(ExpectToken("DCPULSE_OFF"))

        elif a == ActionType.RF_PULSE_START:
            power = float(step.value or 0.0)
            freq = step.params[0] if step.params else None
            duty = step.params[1] if step.params else None
            self._start_rf_pulse(power, freq, duty)
            tokens.append(ExpectToken("RF_TARGET"))
        elif a == ActionType.RF_PULSE_STOP:
            self._stop_rf_pulse()
            tokens.append(ExpectToken("RFPULSE_OFF"))

        elif a == ActionType.IG_CMD:
            self._ig_wait(float(step.value))
            tokens.append(ExpectToken("IG_OK"))
        elif a == ActionType.RGA_SCAN:
            self._rga_scan()
            tokens.append(ExpectToken("RGA_OK"))
        elif a == ActionType.PLC_CMD:
            name, on, *rest = step.params
            ch = int(rest[0]) if rest else 1
            nname = self._norm_plc_name(name)
            self._send_plc(nname, on, ch)
            tokens.append(ExpectToken("PLC", nname))
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

        # [추가] 카운트다운 시작 로그 1회
        self._emit_log("Process", f"{base_message} 시작 ({int((duration_ms + 999) // 1000)}초 대기)")

        # 3) 카운트다운 루프 시작
        self._countdown_task = asyncio.create_task(self._countdown_loop())

        try:
            allow_abort = not self._shutdown_in_progress and not self._in_emergency
            aborted = await self._sleep_or_abort(duration_ms / 1000.0, allow_abort=allow_abort)
            if aborted:
                self._emit_log("Process", f"{self._countdown_base_msg} 중단됨")
                return
            # [추가] 카운트다운 정상 완료 로그 1회
            self._emit_log("Process", f"{self._countdown_base_msg} 완료")
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
                if remaining_ms == 0 or self._abort_evt.is_set():
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
        active = bool(active)
        targets = self._compute_polling_targets(active)

        prev_active = self._last_polling_active
        prev_targets = self._last_polling_targets

        state_changed = (prev_active != active)
        targets_changed = (prev_targets != targets)

        # === 로그: '상태 변화'에만 1회 출력(타깃 변화는 로그 X) ===
        if state_changed:
            if active:
                # 처음 켜질 때도 로그 나오게(prev_active가 None이어도)
                self._emit_log("Process", "폴링 시작")
            else:
                # 초기 상태(None)→False로 들어오는 첫 호출은 로그 생략
                if prev_active is not None:
                    self._emit_log("Process", "폴링 중지")

        # === 이벤트: 상태/타깃 중 하나라도 바뀌면 UI로 알림 ===
        if state_changed or targets_changed:
            self._last_polling_active = active
            self._last_polling_targets = dict(targets)
            # ▼ 타깃을 먼저 내려보내 최신 타깃을 런타임이 저장/적용하게 함
            self._emit(PCEvent("polling_targets", {"targets": targets}))
            # ▼ 그 다음 active 신호를 보냄 → AND 계산 시 최신 타깃을 사용
            self._emit(PCEvent("polling", {"active": active}))

    def _compute_polling_targets(self, active: bool) -> Dict[str, bool]:
        """
        main.py가 기대하는 폴링 타깃 키를 반환:
        - mfc:     MFC 폴링 (활성 시 항상 True)
        - rf_pulse: RF 펄스 사용 시 True
        - dc:      DC 파워 사용 시 True (단, RF 펄스 사용 중이면 False)
        - rf:      RF 연속파 사용 시 True (단, RF 펄스 사용 중이면 False)

        active=False면 전부 False.
        """
        if not active:
            return {"mfc": False, "dc": False, "rf": False, "dc_pulse": False, "rf_pulse": False}

        info = self._get_common_process_info(self.current_params or {})
        use_dc_pulse = bool(info.get("use_dc_pulse", False))
        use_rf_pulse = bool(info.get("use_rf_pulse", False))
        use_dc       = bool(info.get("use_dc", False))
        use_rf       = bool(info.get("use_rf", False))

        # RF 펄스를 쓴다고 DC 연속 폴링까지 막을 필요는 없음
        return {
            "mfc": True,
            "dc_pulse": use_dc_pulse,
            "rf_pulse": use_rf_pulse,
            "dc": use_dc and not use_dc_pulse,   # DC 펄스를 쓸 때만 DC 연속 폴링 off
            "rf": use_rf and not use_rf_pulse,   # RF 펄스를 쓸 때만 RF 연속 폴링 off
        }


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
    
        # ⬇️ 폴링 즉시 OFF (로그는 1회만 출력됨)
        self._apply_polling(False)

        # ✅ 모든 대기 즉시 중단
        self._abort_evt.set()
        self._abort_evt = asyncio.Event()     # ✅ 종료 시퀀스용 새 abort 이벤트

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

        # ⬇️ 추가: 실패를 원 스텝에 귀속
        owner_idx = self._owner_step_for_source(source)
        if owner_idx is not None and 0 <= owner_idx < len(self.process_sequence):
            owner_step = self.process_sequence[owner_idx]
            owner_no   = owner_idx + 1
            owner_act  = owner_step.action.name
        else:
            cur = self.current_step
            owner_idx = self._current_step_idx
            owner_no  = self._current_step_idx + 1
            owner_act = cur.action.name if cur else "UNKNOWN"

        if self._aborting or self._shutdown_in_progress:
            self._shutdown_error = True
            self._shutdown_failures.append(f"Step {owner_no} {owner_act}: {full}")
            self._emit_log("Process", f"경고: 종료 중 단계 실패 → 계속 진행 ({owner_act}, 사유: {full})")
            if self._expect_group:
                self._expect_group.cancel("failure-during-shutdown")
                self._expect_group = None
            return

        # 평시 실패 → 이번 런 실패로 확정 + 원 스텝에 귀속
        self._process_failed = True
        self._shutdown_failures.append(f"Step {owner_no} {owner_act}: {full}")
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
        
        # ✅ 리셋 전에 현재 상태를 캐싱
        was_aborting = (self._aborting or self._in_emergency)

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

        # 다음 런 대비 토큰 소유권 맵 초기화
        self._token_owner.clear()
        self._emit(PCEvent("status", {"running": False}))
        self._emit_state("공정 완료" if ok else "공정 중단됨")
        self._emit(PCEvent("finished", {"ok": ok, "detail": detail}))

        # 다음 런을 위해 폴링 캐시 초기화
        self._last_polling_active = None
        self._last_polling_targets = None

        # 다음 런 대비
        self._abort_evt.clear()  # ✅ 다음 실행에 영향 없도록

        # ✅ 리셋 후에 캐시로 판단
        if was_aborting and not ok:
            self._emit(PCEvent("aborted", {}))

    # =========================
    # 시퀀스 생성/검증/요약
    # =========================

    def _get_common_process_info(self, params: Dict[str, Any]) -> Dict[str, Any]:
        req_dc_cont  = bool(params.get("use_dc_power", False)) and float(params.get("dc_power", 0)) > 0
        req_rf_cont  = bool(params.get("use_rf_power", False)) and float(params.get("rf_power", 0)) > 0
        req_dc_pulse = bool(params.get("use_dc_pulse", False)) and float(params.get("dc_pulse_power", 0)) > 0
        req_rf_pulse = bool(params.get("use_rf_pulse", False)) and float(params.get("rf_pulse_power", 0)) > 0

        use_dc       = req_dc_cont  and self._supports_dc_cont
        use_rf       = req_rf_cont  and self._supports_rf_cont
        use_dc_pulse = req_dc_pulse and self._supports_dc_pulse
        use_rf_pulse = req_rf_pulse and self._supports_rf_pulse

        gun_list = [] if self._ch == 1 else ["G1", "G2", "G3"]

        return {
            'use_ms': bool(params.get("use_ms", False)),
            'use_dc': use_dc,
            'use_rf': use_rf,
            'use_dc_pulse': use_dc_pulse,
            'use_rf_pulse': use_rf_pulse,
            'gas_info': {"AR": {"channel": 1}, "O2": {"channel": 2}, "N2": {"channel": 3}},
            'gun_shutters': gun_list,
            'req_dc': req_dc_cont, 'req_rf': req_rf_cont, 'req_dcp': req_dc_pulse, 'req_rfp': req_rf_pulse,
        }

    def _create_process_sequence(self, params: Dict[str, Any]) -> List[ProcessStep]:
        common_info = self._get_common_process_info(params)
        use_dc        = common_info['use_dc']
        use_rf        = common_info['use_rf']
        use_dc_pulse  = common_info['use_dc_pulse']
        use_rf_pulse  = common_info['use_rf_pulse']
        use_ms       = common_info['use_ms']

        # ✅ 키 누락 대비: 항상 존재하는 맵으로 폴백
        gas_info      = common_info.get('gas_info') or self._gas_info
        gun_shutters  = common_info.get('gun_shutters', [])

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

        # ✅ IG OK 후 RGA 스캔(그래프 그리기 완료까지 대기)
        steps.append(ProcessStep(
            action=ActionType.RGA_SCAN,
            message='RGA 스캔 및 그래프 출력 대기'
        ))

        use_any = any(params.get(k, False) for k in ("use_ar", "use_o2", "use_n2"))

        # 모든 채널 Flow OFF
        for gas, info in gas_info.items():
            if use_any and not params.get(f"use_{gas.lower()}", False):
                continue
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
            if use_any and not params.get(f"use_{gas.lower()}", False):
                continue
            steps.append(ProcessStep(
                action=ActionType.MFC_CMD,
                params=('MFC_ZEROING', {'channel': info["channel"]}),
                message=f'Ch{info["channel"]}({gas}) Zeroing'
            ))

        # --- 가스 주입 ---
        steps.append(ProcessStep(
            action=ActionType.PLC_CMD, params=('MV', True, self._ch), message='메인 밸브 열기'
        ))
        for gas, info in gas_info.items():
            if params.get(f"use_{gas.lower()}", False):
                flow_value = float(params.get(f"{gas.lower()}_flow", 0))
                steps.extend([
                    ProcessStep(
                        action=ActionType.PLC_CMD, params=(gas, True, self._ch), message=f'{gas} 밸브 열기'
                    ),
                    ProcessStep(
                        action=ActionType.MFC_CMD,
                        params=('FLOW_SET', {'channel': info["channel"], 'value': flow_value}),
                        message=f'Ch{info["channel"]}({gas}) GAS {flow_value}sccm 설정'
                    ),
                    ProcessStep(
                        action=ActionType.MFC_CMD,
                        params=('FLOW_ON', {'channel': info["channel"]}),
                        message=f'Ch{info["channel"]}({gas}) GAS 공급 시작'
                    )
                ])

        # --- 압력 제어 시작 (CH1은 SP3, 그 외는 SP4) ---
        sp_on_cmd   = 'SP3_ON' if self._ch == 1 else 'SP4_ON'
        sp_on_label = 'SP3'    if self._ch == 1 else 'SP4'
        steps.extend([
            ProcessStep(action=ActionType.MFC_CMD,
                        params=(sp_on_cmd, {}),
                        message=f'압력 제어({sp_on_label}) 시작'),
            ProcessStep(action=ActionType.MFC_CMD,
                        params=('SP1_SET', {'value': working_pressure}),
                        message=f'목표 압력(SP1) {working_pressure:.2f} 설정'),
            ProcessStep(action=ActionType.DELAY,
                        duration=60000,
                        message='압력 안정화 대기 (60초)'),
        ])

        # --- 파워/셔터 ---
        # Gun Shutter 열기 (CH2 전용: gun_shutters가 비어있지 않을 때만)
        if gun_shutters:
            for shutter in gun_shutters:
                if params.get(f"use_{shutter.lower()}", False):
                    steps.append(ProcessStep(
                        action=ActionType.PLC_CMD,
                        params=(shutter, True, self._ch),
                        message=f'Gun Shutter {shutter} 열기'
                    ))

        # 주: SW_RF_SELECT는 채널 독립 코일이라 ch 인자 없이 보냄
        if bool(params.get("use_power_select", False)) and self._ch == 2:
            steps.append(ProcessStep(
                action=ActionType.PLC_CMD, params=("SW_RF_SELECT", True),
                message="Power_select: Power Select ON (SW_RF_SELECT)"
            ))

        # 병렬: DC(연속) + (RF 연속/펄스)만 허용
        want_parallel = use_dc and (use_rf or use_rf_pulse)

        # (선택) 요청했지만 미지원인 경우 안내 로그
        ci = common_info
        if ci.get('req_dc')  and not use_dc:        self._emit_log("Process", "주의: 이 챔버는 DC 연속 파워 미지원 → DC 단계 스킵")
        if ci.get('req_rf')  and not use_rf:        self._emit_log("Process", "주의: 이 챔버는 RF 연속 파워 미지원 → RF 단계 스킵")
        if ci.get('req_dcp') and not use_dc_pulse:  self._emit_log("Process", "주의: 이 챔버는 DC Pulse 미지원 → Pulse 단계 스킵")
        if ci.get('req_rfp') and not use_rf_pulse:  self._emit_log("Process", "주의: 이 챔버는 RF Pulse 미지원 → Pulse 단계 스킵")

        # DC 연속
        if use_dc:
            steps.append(ProcessStep(
                action=ActionType.DC_POWER_SET, value=dc_power,
                message=f'DC Power {dc_power}W 설정',
                parallel=want_parallel, polling=False,
            ))

        # --- DC 펄스
        dc_pulse_power = float(params.get("dc_pulse_power", 0))
        dc_pulse_freq  = params.get("dc_pulse_freq", None)   # UI: kHz
        dc_pulse_duty  = params.get("dc_pulse_duty", None)   # UI: %

        if dc_pulse_freq is not None:
            dc_pulse_freq = int(dc_pulse_freq)               # 그대로 kHz 정수
        if dc_pulse_duty is not None:
            dc_pulse_duty = int(dc_pulse_duty)               # 그대로 %

        if use_dc_pulse:
            f_txt = f"{dc_pulse_freq}kHz" if dc_pulse_freq is not None else "keep"
            d_txt = f"{dc_pulse_duty}%" if dc_pulse_duty is not None else "keep"
            steps.append(ProcessStep(
                action=ActionType.DC_PULSE_START, value=dc_pulse_power,
                params=(dc_pulse_freq, dc_pulse_duty),  # kHz, %
                message=f'DC Pulse 설정 및 ON (P={dc_pulse_power}W, f={f_txt}, duty={d_txt})',
                parallel=False, polling=False,
            ))

        # --- RF
        rf_pulse_power = float(params.get("rf_pulse_power", 0))
        rf_pulse_freq_khz = params.get("rf_pulse_freq", None)    # UI: kHz
        rf_pulse_duty     = params.get("rf_pulse_duty", None)    # UI: %

        rf_pulse_freq_hz = None
        if rf_pulse_freq_khz is not None:
            # kHz(실수/정수 모두 허용) → Hz(int) 변환
            rf_pulse_freq_hz = int(round(float(rf_pulse_freq_khz) * 1000.0))
        if rf_pulse_duty is not None:
            rf_pulse_duty = int(rf_pulse_duty)

        if use_rf_pulse:
            # ✅ UI 옵션 없이: RF Pulse를 쓰면 자동으로 POWER_SELECT ON
            steps.append(ProcessStep(
                action=ActionType.PLC_CMD,
                params=("SW_POWER_SELECT", True),
                message="Power Select ON (SW_POWER_SELECT)"
            ))

            # 로그/메시지는 kHz로 보기 좋게 표시
            f_txt = f"{float(rf_pulse_freq_khz):.3f}kHz" if rf_pulse_freq_khz is not None else "keep"
            d_txt = f"{rf_pulse_duty}%" if rf_pulse_duty is not None else "keep"
            steps.append(ProcessStep(
                action=ActionType.RF_PULSE_START, value=rf_pulse_power,
                params=(rf_pulse_freq_hz, rf_pulse_duty),  # 장치에는 Hz, %
                message=f'RF Pulse 설정 및 ON (P={rf_pulse_power}W, f={f_txt}, duty={d_txt})',
                parallel=want_parallel, polling=False,
            ))

        elif use_rf:
            # RF 연속 사용 전에 POWER_SELECT = False
            steps.append(ProcessStep(
                action=ActionType.PLC_CMD,
                params=("SW_POWER_SELECT", False),
                message="Power Select OFF (SW_POWER_SELECT)",
            ))

            steps.append(ProcessStep(
                action=ActionType.RF_POWER_SET, value=rf_power,
                message=f'RF Power {rf_power}W 설정',
                parallel=want_parallel, polling=False,
            ))

        if use_rf_pulse:
            steps.append(ProcessStep(
                action=ActionType.DELAY, duration=20_000,
                message='Power Delay 20초', polling=False,
            ))

        steps.append(ProcessStep(
            action=ActionType.MFC_CMD, params=('SP1_ON', {}),
            message='압력 제어(SP1) 시작',
            polling=False,                         
        ))

        if shutter_delay_sec > 0:
            steps.append(ProcessStep(
                action=ActionType.DELAY,
                duration=int(round(shutter_delay_sec * 1000.0)),
                message=f'Shutter Delay {shutter_delay_min}분',
                polling=True,                  
            ))

        if use_ms:
            steps.append(ProcessStep(
                action=ActionType.PLC_CMD, params=('MS', True, self._ch), message='Main Shutter 열기'
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
        use_dc_pulse  = force_all or info['use_dc_pulse']   # ← 추가
        use_rf_pulse = force_all or info['use_rf_pulse']

        # ✅ 키 누락 대비
        gas_info     = info.get('gas_info') or self._gas_info
        gun_shutters = info.get('gun_shutters', [])

        steps.append(ProcessStep(
            action=ActionType.PLC_CMD, params=('MS', False, self._ch), message='Main Shutter 닫기 (항상)'
        ))

        if use_dc:        
            steps.append(ProcessStep(
                action=ActionType.DC_POWER_STOP, 
                message='DC Power Off'
            ))

        if use_rf:        
            steps.append(ProcessStep(
                action=ActionType.RF_POWER_STOP, 
                message='RF Power Off'
            ))

        if use_dc_pulse:  
            steps.append(ProcessStep(
                action=ActionType.DC_PULSE_STOP, 
                message='DC Pulse Off'
            ))

        if use_rf_pulse:  
            steps.append(ProcessStep(
                action=ActionType.RF_PULSE_STOP, 
                message='RF Pulse Off'
            ))

            # ✅ 경로 원복: POWER_SELECT OFF
            steps.append(ProcessStep(
                action=ActionType.PLC_CMD,
                params=("SW_POWER_SELECT", False),
                message="Power Select OFF (SW_POWER_SELECT)"
            ))

        use_any = any(params.get(k, False) for k in ("use_ar", "use_o2", "use_n2"))

        # MFC Flow OFF(선택된 가스만; 선택 없으면 전체)
        for gas, info in gas_info.items():
            if use_any and not params.get(f"use_{gas.lower()}", False):
                continue
            steps.append(ProcessStep(
                action=ActionType.MFC_CMD,
                params=('FLOW_OFF', {'channel': info["channel"]}),
                message=f'Ch{info["channel"]}({gas}) Flow Off'
            ))

        steps.append(ProcessStep(
            action=ActionType.MFC_CMD, params=('VALVE_OPEN', {}), message='전체 MFC Valve Open'
        ))

        if gun_shutters:
            for shutter in gun_shutters:
                if params.get(f"use_{shutter.lower()}", False) or force_all:
                    steps.append(ProcessStep(
                        action=ActionType.PLC_CMD, params=(shutter, False, self._ch), message=f'Gun Shutter {shutter} 닫기'
                    ))

        if (bool(params.get("use_power_select", False)) or force_all) and self._ch == 2:
            steps.append(ProcessStep(
                action=ActionType.PLC_CMD, params=("SW_RF_SELECT", False),
                message="Power_select 종료: Power Select OFF (SW_RF_SELECT)"
            ))

        for gas in gas_info.keys():  # ← 이미 위에서 gas_info = info.get('gas_info') or self._gas_info 해둠
            steps.append(ProcessStep(
                action=ActionType.PLC_CMD, params=(gas, False, self._ch), message=f'PLC {gas} 밸브 닫기'
            ))

        steps.append(ProcessStep(
            action=ActionType.PLC_CMD, params=('MV', False, self._ch), message='메인 밸브 닫기'
        ))

        self._emit_log("Process", "종료 절차가 생성되었습니다.")
        return steps

    def _create_emergency_shutdown_sequence(self) -> List[ProcessStep]:
        if not self.current_params:
            return []

        # ✅ 공통 정보는 항상 방어적으로 가져오기
        ci = self._get_common_process_info(self.current_params or {}) or {}
        steps: List[ProcessStep] = []

        steps.append(ProcessStep(
            action=ActionType.PLC_CMD, params=('MS', False, self._ch),
            message='[긴급] Main Shutter 즉시 닫기', no_wait=True
        ))

        # ✅ 키 누락 대비: .get() + 기본값
        use_dc       = bool(ci.get('use_dc', False))
        use_rf       = bool(ci.get('use_rf', False))
        use_dc_pulse = bool(ci.get('use_dc_pulse', False))
        use_rf_pulse = bool(ci.get('use_rf_pulse', False))

        # ✅ gas_info가 비어도 안전하게 폴백
        default_gas_info = {"AR": {"channel": 1}, "O2": {"channel": 2}, "N2": {"channel": 3}}
        gas_info = ci.get('gas_info') or default_gas_info

        both = (use_dc or use_dc_pulse) and (use_rf or use_rf_pulse)

        if use_dc:
            steps.append(ProcessStep(
                action=ActionType.DC_POWER_STOP, message='[긴급] DC Power 즉시 차단',
                parallel=both, no_wait=True
            ))

        if use_rf:
            steps.append(ProcessStep(
                action=ActionType.RF_POWER_STOP, message='[긴급] RF Power 즉시 차단',
                parallel=both, no_wait=True
            ))

        if use_rf_pulse:
            steps.append(ProcessStep(
                action=ActionType.RF_PULSE_STOP, message='[긴급] RF Pulse 즉시 차단',
                parallel=both, no_wait=True
            ))
            
            # ✅ 경로 원복: POWER_SELECT 즉시 OFF
            steps.append(ProcessStep(
                action=ActionType.PLC_CMD,
                params=("SW_POWER_SELECT", False),
                message='[긴급] Power Select 즉시 OFF',
                no_wait=True
            ))

        if use_dc_pulse:
            steps.append(ProcessStep(
                action=ActionType.DC_PULSE_STOP, message='[긴급] DC Pulse 즉시 차단',
                parallel=both, no_wait=True
            ))

        if bool(self.current_params.get("use_power_select", False)) and self._ch == 2:
            steps.append(ProcessStep(
                action=ActionType.PLC_CMD, params=("SW_RF_SELECT", False),
                message='[긴급] Power Select 즉시 OFF', no_wait=True
            ))

        # ✅ 선택된 가스만 MFC FLOW_OFF (no_wait) — 키 에러 방지 + 변수명 충돌 방지
        for gas, ginfo in gas_info.items():
            if self.current_params.get(f"use_{gas.lower()}", False):
                steps.append(ProcessStep(
                    action=ActionType.MFC_CMD,
                    params=('FLOW_OFF', {'channel': ginfo["channel"]}),
                    message=f'[긴급] Ch{ginfo["channel"]}({gas}) FLOW OFF',
                    no_wait=True
                ))

        # ✅ PLC 가스 차단도 선택된 가스만
        for gas in ("AR", "O2", "N2"):
            if self.current_params.get(f"use_{gas.lower()}", False):
                steps.append(ProcessStep(
                    action=ActionType.PLC_CMD, params=(gas, False, self._ch),
                    message=f'[긴급] {gas} 가스 즉시 차단', no_wait=True
                ))

        steps.append(ProcessStep(
            action=ActionType.PLC_CMD, params=('MV', False, self._ch),
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
                if step.action == ActionType.DC_PULSE_START:
                    if step.value is None:
                        errors.append(f"Step {n}: DC_PULSE_START에 value(파워)가 없습니다.")
                    if step.params is None or len(step.params) != 2:
                        errors.append(f"Step {n}: DC_PULSE_START params=(freq, duty) 필요.")
                if step.action == ActionType.RF_PULSE_START:
                    if step.value is None:
                        errors.append(f"Step {n}: RF_PULSE_START에 value(파워)가 없습니다.")
                    if step.params is None or len(step.params) != 2:
                        errors.append(f"Step {n}: RF_PULSE_START params=(freq, duty) 필요.")
                if step.action in [ActionType.PLC_CMD, ActionType.MFC_CMD, ActionType.OES_RUN]:
                    if step.params is None:
                        errors.append(f"Step {n}: {step.action.name} 액션에 params가 없습니다.")
        except Exception as e:
            errors.append(f"검증 중 오류 발생: {e}")
        return len(errors) == 0, errors

    def get_estimated_duration(self) -> int:
        return sum((s.duration or 0) for s in self.process_sequence if s.action == ActionType.DELAY)
    
    # =========================
    # 토큰 소유권 유틸
    # =========================
    def _tokey(self, t: ExpectToken) -> Tuple[str, Any]:
        return (t.kind, t.spec)

    def _register_token_owners(self, owners: Dict[Tuple[str, Any], int]) -> None:
        # 마지막 등록이 우선(파이썬 dict는 삽입 순서를 보존하므로, 뒤에서부터 찾으면 최신 소유자가 잡힘)
        self._token_owner.update(owners)

    def _device_token_kinds(self, source: str) -> Tuple[str, ...]:
        # on_* 실패 콜백에서 넘기는 source 문자열과, 그 스텝이 생성하는 토큰 kind를 매핑
        m = {
            "DCPulse":   ("DC_PULSE_TARGET", "DCPULSE_OFF"),
            "RFPulse":   ("RF_TARGET", "RFPULSE_OFF"),
            "RF Power":  ("RF_TARGET", "GENERIC_OK"),
            "DC Power":  ("DC_TARGET", "GENERIC_OK"),
            "MFC":       ("MFC",),
            "PLC":       ("PLC",),
            "IG":        ("IG_OK",),
            "RGA":       ("RGA_OK",),
            # 필요 시 OES 등 추가 가능
        }
        # 장치명이 조금 다르게 들어와도 대소문자/공백 차이 방어(선택)
        key = source.strip()
        return m.get(key, ())

    def _owner_step_for_source(self, source: str) -> Optional[int]:
        kinds = self._device_token_kinds(source)
        if not kinds:
            return None
        # 최신 등록부터 역순 탐색
        for (k, _spec), idx in reversed(list(self._token_owner.items())):
            if k in kinds:
                return idx
        return None

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

    async def _wait_or_abort(self, awaitable, *, allow_abort: bool = True) -> bool:
        """
        allow_abort=False면 abort 신호를 무시하고 awaitable이 끝날 때까지 기다린다.
        반환값: True면 'abort가 먼저 왔다'는 뜻.
        """
        if not allow_abort:
            await awaitable
            return False

        a = asyncio.create_task(awaitable) if not isinstance(awaitable, (asyncio.Task, asyncio.Future)) else awaitable
        b = asyncio.create_task(self._abort_evt.wait())
        done, pending = await asyncio.wait({a, b}, return_when=asyncio.FIRST_COMPLETED)
        for p in pending:
            p.cancel()
        if b in done:
            try:
                a.cancel()
            except Exception:
                pass
            return True
        return False

    async def _sleep_or_abort(self, seconds: float, *, allow_abort: bool = True) -> bool:
        return await self._wait_or_abort(asyncio.sleep(max(0.0, seconds)), allow_abort=allow_abort)


    def _norm_plc_name(self, name: str) -> str:
        nm = (name or "").strip().upper().replace(" ", "")
        aliases = {
            # 가스
            "ARGON": "AR", "AR2": "AR", "AR_2": "AR",
            # 메인밸브/셔터 별칭
            "MAINVALVE": "MV", "MAIN_VALVE": "MV",
            "MAINSHUTTER": "MS", "MAIN_SHUTTER": "MS",
            # 그대로 허용
            "AR": "AR", "O2": "O2", "N2": "N2", "MAIN": "MAIN",
            "MV": "MV", "MS": "MS",
            "G1": "G1", "G2": "G2", "G3": "G3",
        }
        return aliases.get(nm, nm)



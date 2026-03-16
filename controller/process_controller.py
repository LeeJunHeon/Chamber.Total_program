# process_controller.py
#  - Qt 의존성 제거 (UI만 Qt, 로직은 asyncio)
#  - main.py와는 asyncio.Queue 기반 이벤트로 통신
#  - 장비 명령은 콜백 함수로 주입 (DI)

from __future__ import annotations

import re
import asyncio
from dataclasses import dataclass
from enum import Enum
from time import monotonic_ns
from typing import Optional, List, Tuple, Dict, Any, Callable
from errors.app_error import AppError
from lib.config_common import SHUTDOWN_STEP_TIMEOUT_MS, SHUTDOWN_STEP_GAP_MS, RGA_STEP_TIMEOUT_MS
from lib import config_ch1, config_ch2


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

        # ✅ running loop가 없을 수 있으므로 생성 지연
        self._fut: Optional[asyncio.Future[bool]] = None

        # ✅ future 생성 전에도 '완료/취소' 상태를 기억
        self._completed: bool = not self._tokens
        self._cancel_exc: Optional[BaseException] = None

    def get_future(self) -> asyncio.Future[bool]:
        """async 컨텍스트(= running loop 존재)에서 처음 필요해질 때 Future 생성."""
        if self._fut is None:
            self._fut = asyncio.get_running_loop().create_future()

            # ✅ 이미 취소/완료 상태였다면 생성 즉시 반영
            if self._cancel_exc is not None and not self._fut.done():
                self._fut.set_exception(self._cancel_exc)
            elif self._completed and not self._fut.done():
                self._fut.set_result(True)

        return self._fut

    @property
    def future(self) -> asyncio.Future[bool]:
        return self.get_future()

    def empty(self) -> bool:
        return not self._tokens

    def match(self, incoming: ExpectToken) -> bool:
        for i, t in enumerate(self._tokens):
            if t.matches(incoming):
                del self._tokens[i]

                if not self._tokens:
                    self._completed = True
                    if self._fut is not None and not self._fut.done():
                        self._fut.set_result(True)

                return True
        return False

    # ✅ 추가: 토큰을 '소비'하지 않고, 현재 그룹이 이 토큰을 기다리는지 검사만
    def needs(self, incoming: ExpectToken) -> bool:
        return any(t.matches(incoming) for t in self._tokens)

    def match_generic_ok(self) -> bool:
        if len(self._tokens) == 1 and self._tokens[0].kind == "GENERIC_OK":
            self._tokens.clear()
            self._completed = True
            if self._fut is not None and not self._fut.done():
                self._fut.set_result(True)
            return True
        return False

    def cancel(self, reason: str = "cancelled") -> None:
        exc = asyncio.CancelledError(reason)
        self._cancel_exc = exc
        if self._fut is not None and not self._fut.done():
            self._fut.set_exception(exc)


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
    DC_PULSE_SET   = "DC_PULSE_SET"   # ✅ 추가: 출력 ON 유지 + setpoint 변경
    DC_PULSE_STOP  = "DC_PULSE_STOP"
    RF_PULSE_START = "RF_PULSE_START"
    RF_PULSE_SET   = "RF_PULSE_SET"   # ✅ 추가: 출력 ON 유지 + setpoint 변경
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
                raise AppError(code="E214", detail="DELAY 액션은 duration이 필요합니다.")
            if self.parallel:
                raise AppError(code="E215", detail="DELAY는 병렬 블록에 포함할 수 없습니다.")

        # ✅ value 필수 액션은 다시 복구
        if self.action in (
            ActionType.DC_POWER_SET,
            ActionType.RF_POWER_SET,
            ActionType.IG_CMD,
            ActionType.DC_PULSE_SET,
            ActionType.RF_PULSE_SET,
        ):
            if self.value is None:
                # 이 부분은 현재 전용 코드가 명확히 준비된 게 아니므로
                # 임의 코드 추정 없이 ValueError 유지하는 것이 안전
                raise ValueError(f"{self.action.name} 액션은 value가 필요합니다.")

        if self.action == ActionType.PLC_CMD:
            if not self.params or len(self.params) not in (2, 3):
                raise AppError(code="E216", detail="PLC_CMD params는 (name:str, on:any[, ch:int]) 형태여야 합니다.")

        if self.action == ActionType.MFC_CMD:
            if not self.params or len(self.params) != 2 or not isinstance(self.params[1], dict):
                raise AppError(code="E217", detail="MFC_CMD params는 (cmd:str, args:dict) 형태여야 합니다.")

        if self.action == ActionType.OES_RUN:
            if not self.params or len(self.params) != 2:
                raise AppError(code="E218", detail="OES_RUN params는 (process_time_sec:float, integration_ms:int) 형태여야 합니다.")

        if self.action == ActionType.DC_PULSE_START:
            if self.value is None:
                raise AppError(code="E219", detail="DC_PULSE_START에는 value(타깃 파워)가 필요합니다.")
            if not self.params or len(self.params) != 2:
                raise AppError(code="E221", detail="DC_PULSE_START params는 (freqkHz|None, duty%|None) 형태여야 합니다.")

        if self.action == ActionType.RF_PULSE_START:
            if self.value is None:
                raise AppError(code="E220", detail="RF_PULSE_START에는 value(타깃 파워)가 필요합니다.")
            if not self.params or len(self.params) != 2:
                raise AppError(code="E222", detail="RF_PULSE_START params는 (freqkHz|None, duty%|None) 형태로 받고 내부에서 Hz로 변환합니다.")


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
        set_dc_pulse_power: Optional[Callable[[float], None]] = None,   # ✅ 추가
        start_rf_pulse: Callable[[float, Optional[int], Optional[int]], None],
        stop_rf_pulse: Callable[[], None],
        set_rf_pulse_power: Optional[Callable[[float], None]] = None,   # ✅ 추가: RF setpoint 변경

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
        self._set_dc_pulse_power = set_dc_pulse_power   # ✅ 추가
        self._start_rf_pulse = start_rf_pulse
        self._stop_rf_pulse  = stop_rf_pulse
        self._set_rf_pulse_power = set_rf_pulse_power   # ✅ 추가
        self._ig_wait = ig_wait
        self._cancel_ig = cancel_ig
        self._rga_scan = rga_scan
        self._oes_run = oes_run

        # ⬇️ 추가: 챔버/지원능력
        self._ch = int(ch)
        self._cfg = config_ch1 if self._ch == 1 else config_ch2   # ✅ 채널별 설정 모듈
        self._supports_dc_cont = bool(supports_dc_cont)
        self._supports_rf_cont = bool(supports_rf_cont)
        self._supports_dc_pulse = bool(supports_dc_pulse)
        self._supports_rf_pulse = bool(supports_rf_pulse)

        # ✅ 항상 존재하는 가스 채널 맵(소스 오브 트루스)
        self._gas_info = dict(getattr(self._cfg, "PROCESS_GAS_INFO", {
            "AR": {"channel": 1},
            "O2": {"channel": 2},
            "N2": {"channel": 3},
        }))

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

        # ✅ 추가: 대표 실패 정보(첫 실패 원인 보존)
        self._last_error_code: str | None = None
        self._last_error_source: str | None = None
        self._last_error_detail: str = ""
        self._last_error_meta: Dict[str, Any] = {}

        # 대기/카운트다운
        self._countdown_task: Optional[asyncio.Task] = None
        self._countdown_total_ms: int = 0
        self._countdown_start_ns: int = 0
        self._countdown_base_msg: str = ""

        # ✅ 실제 진행 시간 누적(ms)
        # - Shutter Delay 구간에서 실제로 흐른 시간
        # - Main Process 구간에서 실제로 흐른 시간
        self._actual_shutter_delay_ms: int = 0
        self._actual_process_time_ms: int = 0

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

    # ===== 지원 플래그 property =====
    @property
    def supports_dc_cont(self) -> bool:
        return bool(self._supports_dc_cont)

    @supports_dc_cont.setter
    def supports_dc_cont(self, v: bool) -> None:
        self._supports_dc_cont = bool(v)

    @property
    def supports_rf_cont(self) -> bool:
        return bool(self._supports_rf_cont)

    @supports_rf_cont.setter
    def supports_rf_cont(self, v: bool) -> None:
        self._supports_rf_cont = bool(v)

    @property
    def supports_dc_pulse(self) -> bool:
        return bool(self._supports_dc_pulse)

    @supports_dc_pulse.setter
    def supports_dc_pulse(self, v: bool) -> None:
        self._supports_dc_pulse = bool(v)

    @property
    def supports_rf_pulse(self) -> bool:
        return bool(self._supports_rf_pulse)

    @supports_rf_pulse.setter
    def supports_rf_pulse(self, v: bool) -> None:
        self._supports_rf_pulse = bool(v)

    # ===== 공정 시작/중단 API =====
    def start_process(self, params: Dict[str, Any]) -> None:
        if self.is_running:
            self._emit_log("Process", "오류: 이미 다른 공정이 실행 중입니다.")
            return

        try:
            self._token_owner.clear()
            self.current_params = params or {}

            pname = (
                self.current_params.get("process_name")
                or self.current_params.get("process_note")
                or self.current_params.get("Process_name")
                or "Untitled"
            )
            pname = str(pname).strip() or "Untitled"

            self.current_params["process_name"] = pname
            # (호환 유지) 다른 파일들이 아직 process_note/Process_name을 볼 수 있어서 같이 맞춰둠
            self.current_params["process_note"] = pname
            self.current_params["Process_name"] = pname

            # ✅ 실제 시간 누적 초기화(이번 런 기준)
            self._actual_shutter_delay_ms = 0
            self._actual_process_time_ms = 0

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

            # ✅ 추가: 대표 실패 초기화
            self._last_error_code = None
            self._last_error_source = None
            self._last_error_detail = ""
            self._last_error_meta = {}

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
        # ✅ 핵심: 공정이 이미 종료된 상태에서 들어오는 STOP은 무시
        # (정상 종료 후 시간차로 들어온 stop 때문에 'stopped 종료 이벤트'가 다시 발생하는 걸 차단)
        if not self.is_running:
            self._emit_log("Process", "정지 요청: 실행 중 공정 없음(이미 종료됨) → 무시")
            return

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

        # ✅ TEST MODE면 장비 종료 시퀀스(Shutdown)로 들어가지 않음!
        if bool((self.current_params or {}).get("test_mode", False)):
            self._emit_log("Process", "[TEST MODE] STOP: 장비 종료 절차 스킵 (딜레이만 취소)")
            return

        # ✅ 즉시 종료 절차로 진입
        self._start_normal_shutdown()

        # ✅ _start_normal_shutdown()에서 '종료 절차 없음'이면 _finish(False)로 is_running=False가 될 수 있음
        if not self.is_running:
            return

        # ✅ (안전장치) 러너가 비정상적으로 없거나 종료된 상태면, 종료 시퀀스 수행을 위해서만 재기동
        t = self._runner_task
        if t is None or t.done():
            self._emit_log("Process", "정지 요청: runner가 없거나 종료됨 → 종료 시퀀스 수행 위해 runner 재기동")
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

        self._last_error_code = None
        self._last_error_source = None
        self._last_error_detail = ""
        self._last_error_meta = {}
        
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

    def on_mfc_failed(
        self,
        cmd: str,
        why: str | BaseException,
        *,
        code: str | None = None,
        meta: Dict[str, Any] | None = None,
    ) -> None:
        incoming = ExpectToken("MFC", cmd)

        merged_meta = {"cmd": cmd}
        if isinstance(meta, dict):
            merged_meta.update(meta)

        if self._expect_group and self._expect_group.needs(incoming):
            self._step_failed("MFC", why, code=code, meta=merged_meta)
            return

        err_code, err_detail, _ = self._normalize_error_info(why, code=code, meta=merged_meta)
        if err_code:
            self._emit_log("MFC", f"경고(무시): {cmd}: {err_code} - {err_detail} (현재 스텝과 무관)")
        else:
            self._emit_log("MFC", f"경고(무시): {cmd}: {err_detail} (현재 스텝과 무관)")

    def on_plc_confirmed(self, cmd: str) -> None:
        self._match_token(ExpectToken("PLC", cmd))

    def on_plc_failed(
        self,
        cmd: str,
        why: str | BaseException,
        *,
        code: str | None = None,
        meta: Dict[str, Any] | None = None,
    ) -> None:
        merged_meta = {"cmd": cmd}
        if isinstance(meta, dict):
            merged_meta.update(meta)

        self._step_failed("PLC", why, code=code, meta=merged_meta)

    def on_ig_ok(self) -> None:
        self._match_token(ExpectToken("IG_OK"))

    def on_ig_failed(
        self,
        src: str,
        why: str | BaseException,
        *,
        code: str | None = None,
        meta: Dict[str, Any] | None = None,
    ) -> None:
        self._step_failed(src or "IG", why, code=code, meta=meta)

    def on_rga_finished(self) -> None:
        self._match_token(ExpectToken("RGA_OK"))

    def on_rga_failed(
        self,
        src: str,
        why: str | BaseException,
        *,
        code: str | None = None,
        meta: Dict[str, Any] | None = None,
    ) -> None:
        self._step_failed(src or "RGA", why, code=code, meta=meta)

    def on_dc_target_reached(self) -> None:
        self._match_token(ExpectToken("DC_TARGET"))

    def on_dc_target_failed(
        self,
        why: str | BaseException,
        *,
        code: str | None = None,
        meta: Dict[str, Any] | None = None,
    ) -> None:
        self._step_failed("DC Power", why, code=code, meta=meta)

    def on_rf_target_reached(self) -> None:
        self._match_token(ExpectToken("RF_TARGET"))

    def on_rf_target_failed(
        self,
        why: str | BaseException,
        *,
        code: str | None = None,
        meta: Dict[str, Any] | None = None,
    ) -> None:
        self._step_failed("RF Power", why, code=code, meta=meta)

    def on_dc_pulse_target_reached(self) -> None:
        self._match_token(ExpectToken("DC_PULSE_TARGET"))

    def on_dc_pulse_set_confirmed(self) -> None:
        self._match_token(ExpectToken("DC_PULSE_SET"))

    def on_dc_pulse_off_finished(self) -> None:
        self._match_token(ExpectToken("DCPULSE_OFF"))

    def on_dc_pulse_failed(
        self,
        why: str | BaseException,
        *,
        code: str | None = None,
        meta: Dict[str, Any] | None = None,
    ) -> None:
        self._step_failed("DCPulse", why, code=code, meta=meta)

    def on_rf_pulse_target_reached(self) -> None:
        # RF 펄스 타깃 도달은 연속 RF와 동일 판정으로 통일
        self._match_token(ExpectToken("RF_TARGET"))

    def on_rf_pulse_off_finished(self) -> None:
        self._match_token(ExpectToken("RFPULSE_OFF"))

    def on_rf_pulse_failed(
        self,
        why: str | BaseException,
        *,
        code: str | None = None,
        meta: Dict[str, Any] | None = None,
    ) -> None:
        self._step_failed("RFPulse", why, code=code, meta=meta)

    def on_device_step_ok(self) -> None:
        # 일반 OK는 해당 스텝이 실제로 GENERIC_OK를 요구할 때만 인정
        self._match_token(ExpectToken("GENERIC_OK"))

    def on_oes_ok(self) -> None:
        # OES는 no_wait로 돌도록 구성(로그만)
        self._emit_log("OES", "OES 측정 종료(정상).")

    def on_oes_failed(
        self,
        src: str,
        why: str | BaseException,
        *,
        code: str | None = None,
        meta: Dict[str, Any] | None = None,
    ) -> None:
        err_code, err_detail, _ = self._normalize_error_info(why, code=code, meta=meta)

        if err_code:
            self._emit_log(src or "OES", f"오류 무시하고 계속: {err_code} - {err_detail}")
        else:
            self._emit_log(src or "OES", f"오류 무시하고 계속: {err_detail}")

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

                    if bool((self.current_params or {}).get("test_mode", False)):
                        # TEST MODE면 shutdown으로 전환하지 않고 그대로 끝까지 가서 finish(False) 됨
                        self._emit_log("Process", "[TEST MODE] 정지 요청 감지 - 종료 절차 스킵")
                    else:
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
                # 전원 OFF 계열 스텝은 중지(Shutdown) 시에도 반드시 완료 이벤트를 기다린다.
                hard_wait_actions = {
                    ActionType.RF_POWER_STOP,
                    ActionType.DC_POWER_STOP,
                    ActionType.RF_PULSE_STOP,
                    ActionType.DC_PULSE_STOP,
                }

                if self._shutdown_in_progress:
                    min_off_ms = int(getattr(self._cfg, "PC_POWER_OFF_TIMEOUT_MS", 240_000))
                    POWER_OFF_TIMEOUT_MS = max(min_off_ms, SHUTDOWN_STEP_TIMEOUT_MS)  # ✅ 전원 OFF는 더 길게

                    if step.action in hard_wait_actions:
                        try:
                            await asyncio.wait_for(fut, timeout=POWER_OFF_TIMEOUT_MS / 1000.0)
                        except asyncio.TimeoutError:
                            # ✅ shutdown 중 실패는 “기록하고 계속”이 네 설계와 일치
                            self._step_failed("Shutdown", f"{step.action.name} timeout ({POWER_OFF_TIMEOUT_MS/1000:.0f}s)")
                    else:
                        try:
                            await asyncio.wait_for(fut, timeout=max(0.001, SHUTDOWN_STEP_TIMEOUT_MS) / 1000.0)
                        except asyncio.TimeoutError:
                            self._emit_log("Process", "종료 스텝 확인 시간 초과 → 다음 스텝 진행")

                else:
                    # 평시: abort와 경쟁
                    if step.action == ActionType.RGA_SCAN:
                        try:
                            aborted = await asyncio.wait_for(
                                self._wait_or_abort(fut, allow_abort=not self._in_emergency),
                                timeout=max(0.001, RGA_STEP_TIMEOUT_MS) / 1000.0,
                            )
                        except asyncio.TimeoutError:
                            self._emit_log("Process", "RGA 스캔 대기 시간 초과 → 그래프 스킵, 다음 단계 진행")
                            if self._expect_group:
                                self._expect_group.cancel("rga-timeout")
                                self._expect_group = None
                            return
                    else:
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

        elif a == ActionType.DC_PULSE_SET:
            # Output ON 상태에서 setpoint(REF_POWER)만 변경
            power = float(step.value or 0.0)
            if not self._set_dc_pulse_power:
                raise AppError(
                    code="E715",
                    detail="DC_PULSE_SET을 사용하려면 set_dc_pulse_power 콜백이 주입되어야 합니다.",
                    meta={"action": "DC_PULSE_SET", "ch": self._ch},
                )
            self._set_dc_pulse_power(power)

            # ✅ 시작(OUTPUT_ON) 완료와, 중간 setpoint 변경(REF_POWER ACK)을 분리해서 기다린다.
            tokens.append(ExpectToken("DC_PULSE_SET"))

        elif a == ActionType.DC_PULSE_STOP:
            self._stop_dc_pulse()
            tokens.append(ExpectToken("DCPULSE_OFF"))

        elif a == ActionType.RF_PULSE_START:
            power = float(step.value or 0.0)
            freq = step.params[0] if step.params else None
            duty = step.params[1] if step.params else None
            self._start_rf_pulse(power, freq, duty)
            tokens.append(ExpectToken("RF_TARGET"))

        elif a == ActionType.RF_PULSE_SET:
            # Output ON 상태에서 setpoint(REF_POWER)만 변경
            power = float(step.value or 0.0)
            if not self._set_rf_pulse_power:
                raise AppError(
                    code="E716",
                    detail="RF_PULSE_SET을 사용하려면 set_rf_pulse_power 콜백이 주입되어야 합니다.",
                    meta={"action": "RF_PULSE_SET", "ch": self._ch},
                )
            self._set_rf_pulse_power(power)

            # 토큰은 '대기'가 아니라 실패 귀속을 위해 등록만 (스텝 생성 시 no_wait=True)
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
            ch = int(rest[0]) if rest else self._ch
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
            raise AppError(
                code="E213",
                detail=f"알 수 없는 Action: {a}",
                meta={"action": str(a)},
            )

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
            # 종료 시에만 상태까지 정리 (누적은 _cancel_countdown()에서 처리)
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
        """카운트다운 루프를 중단하고 상태를 정리한다.

        STOP/FAIL 시 _start_normal_shutdown()에서 먼저 호출되면서 start/total이 0으로 지워져
        실제 진행 시간이 0으로 남는 문제를 방지하기 위해,
        상태를 지우기 전에 여기서 1회 누적 후 초기화한다.
        (중복 호출되어도 start/total을 0으로 만들어 2중 누적 방지)
        """
        # ✅ 상태를 지우기 전에 실제로 흐른 시간 누적
        try:
            start_ns = int(self._countdown_start_ns or 0)
            total_ms = int(self._countdown_total_ms or 0)
            if start_ns > 0 and total_ms > 0:
                elapsed_ms = int((monotonic_ns() - start_ns) // 1_000_000)
                elapsed_ms = max(0, min(elapsed_ms, total_ms))

                msg = (self._countdown_base_msg or "").strip()
                if msg.startswith("Shutter Delay"):
                    self._actual_shutter_delay_ms += elapsed_ms
                elif msg.startswith("메인 공정 진행"):
                    self._actual_process_time_ms += elapsed_ms
        except Exception:
            pass

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
    

    def _normalize_error_info(
        self,
        reason: str | BaseException,
        *,
        code: str | None = None,
        meta: Dict[str, Any] | None = None,
    ) -> tuple[str | None, str, Dict[str, Any]]:
        err_code = code
        err_detail = ""
        err_meta: Dict[str, Any] = {}

        if isinstance(reason, BaseException):
            if err_code is None:
                err_code = getattr(reason, "code", None) or getattr(reason, "error_code", None)

            err_detail = getattr(reason, "detail", None) or str(reason)

            src_meta = getattr(reason, "meta", None)
            if isinstance(src_meta, dict):
                err_meta.update(src_meta)

            err_meta["cause_type"] = type(reason).__name__
        else:
            err_detail = "" if reason is None else str(reason)

        if isinstance(meta, dict):
            err_meta.update(meta)

        err_detail = str(err_detail).strip()
        if not err_detail:
            err_detail = "unknown"

        return err_code, err_detail, err_meta


    def _remember_primary_error(
        self,
        source: str,
        *,
        code: str | None,
        detail: str,
        meta: Dict[str, Any] | None = None,
    ) -> None:
        # 첫 번째 실패만 대표 원인으로 보존
        if self._last_error_source or self._last_error_detail:
            return

        self._last_error_code = (str(code).strip().upper() if code else None)
        self._last_error_source = str(source).strip() or "UNKNOWN"
        self._last_error_detail = str(detail).strip()
        self._last_error_meta = dict(meta or {})


    # =========================
    # 종료/실패 처리
    # =========================

    def _start_normal_shutdown(self) -> None:
        # ✅ 2중 방어: 이미 종료된 상태면 종료 절차 자체를 시작하지 않음
        if not self.is_running:
            self._emit_log("Process", "종료 절차 무시: 실행 중 공정 없음(이미 종료됨)")
            return

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

    def _step_failed(
        self,
        source: str,
        reason: str | BaseException,
        *,
        code: str | None = None,
        meta: Dict[str, Any] | None = None,
    ) -> None:
        if not self.is_running:
            return

        err_code, err_detail, err_meta = self._normalize_error_info(reason, code=code, meta=meta)

        if err_code:
            full = f"[{source} - {err_code}: {err_detail}]"
        else:
            full = f"[{source} - {err_detail}]"

        owner_idx = self._owner_step_for_source(source)
        if owner_idx is not None and 0 <= owner_idx < len(self.process_sequence):
            owner_step = self.process_sequence[owner_idx]
            owner_no   = owner_idx + 1
            owner_act  = owner_step.action.name
        else:
            cur = self.current_step
            owner_no  = self._current_step_idx + 1
            owner_act = cur.action.name if cur else "UNKNOWN"

        # ✅ 대표 실패 원인은 최초 1회만 저장
        self._remember_primary_error(
            source,
            code=err_code,
            detail=err_detail,
            meta=err_meta,
        )

        if self._aborting or self._shutdown_in_progress:
            self._shutdown_error = True
            self._shutdown_failures.append(f"Step {owner_no} {owner_act}: {full}")
            self._emit_log("Process", f"경고: 종료 중 단계 실패 → 계속 진행 ({owner_act}, 사유: {full})")
            if self._expect_group:
                self._expect_group.cancel("failure-during-shutdown")
                self._expect_group = None
            return

        self._process_failed = True
        self._shutdown_failures.append(f"Step {owner_no} {owner_act}: {full}")
        self._emit_log("Process", f"오류 발생: {full}. 종료 절차를 시작합니다.")
        self._start_normal_shutdown()

    def _finish(self, ok: bool) -> None:
        if not self.is_running:
            return

        proc_name = (
            self.current_params.get("process_name")
            or self.current_params.get("process_note")
            or self.current_params.get("Process_name")
            or "Untitled"
        )
        proc_name = str(proc_name).strip() or "Untitled"

        stopped = bool(self._stop_requested)

        # ✅ Result: SUCCESS / STOP / FAIL
        if bool(ok):
            result = "SUCCESS"
        else:
            result = "STOP" if stopped else "FAIL"

        detail = {
            "process_name": proc_name,
            "result": result,
            "stopped": stopped,
            "aborting": (self._aborting or self._in_emergency),
            "errors": list(self._shutdown_failures),

            # ✅ 추가: 대표 실패 정보
            "error_code": self._last_error_code,
            "error_source": self._last_error_source,
            "error_detail": self._last_error_detail,
            "error_meta": dict(self._last_error_meta or {}),

            "actual_shutter_delay_min": round(self._actual_shutter_delay_ms / 60000.0, 3),
            "actual_process_time_min": round(self._actual_process_time_ms / 60000.0, 3),
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

        gun_list = list(getattr(self._cfg, "PROCESS_GUN_SHUTTERS", []))

        return {
            'use_ms': bool(params.get("use_ms", False)),
            'use_dc': use_dc,
            'use_rf': use_rf,
            'use_dc_pulse': use_dc_pulse,
            'use_rf_pulse': use_rf_pulse,
            'gas_info': dict(getattr(self._cfg, "PROCESS_GAS_INFO", self._gas_info)),
            'gun_shutters': gun_list,
            'req_dc': req_dc_cont, 'req_rf': req_rf_cont, 'req_dcp': req_dc_pulse, 'req_rfp': req_rf_pulse,
        }

    def _create_process_sequence(self, params: Dict[str, Any]) -> List[ProcessStep]:
        # ------------------------------------------------------------
        # TEST MODE : 실제 장비 제어 없이 시간만 흘려보내기
        # ------------------------------------------------------------
        if bool(params.get("test_mode", False)):
            try:
                dur_sec = float(params.get("test_duration_sec") or 0.0)
            except Exception:
                dur_sec = 0.0

            # ✅ 추가: CSV의 time(60m/10s/1h30m)도 지원
            if dur_sec <= 0:
                time_str = str(params.get("time", "")).strip()
                if time_str:
                    dur_sec = self._parse_duration_seconds(time_str)

            if dur_sec <= 0:
                dur_sec = float(params.get("process_time") or 0.0) * 60.0

            dur_sec = max(1.0, float(dur_sec))

            self._emit_log("Process", f"[TEST MODE] 장비제어 스킵 / 시뮬레이션 {dur_sec:.1f}s 진행")
            return [ProcessStep(action=ActionType.DELAY, duration=int(dur_sec*1000), message=f"TEST MODE ({int(dur_sec)}s)", polling=False)]
        # ------------------------------------------------------------

        common_info = self._get_common_process_info(params)
        use_dc        = common_info['use_dc']
        use_rf        = common_info['use_rf']
        use_dc_pulse  = common_info['use_dc_pulse']
        use_rf_pulse  = common_info['use_rf_pulse']
        use_ms       = common_info['use_ms']

        # ✅ 키 누락 대비: 항상 존재하는 맵으로 폴백
        gas_info      = common_info.get('gas_info') or self._gas_info
        gun_shutters  = common_info.get('gun_shutters', [])

        default_base_pressure = float(getattr(self._cfg, "PROCESS_DEFAULT_BASE_PRESSURE", 1e-5))
        base_pressure = float(params.get("base_pressure", default_base_pressure))
        working_pressure = float(params.get("working_pressure", 0))
        process_time_min = float(params.get("process_time", 0))
        shutter_delay_min = float(params.get("shutter_delay", 0))
        shutter_delay_sec = shutter_delay_min * 60.0
        process_time_sec = process_time_min * 60.0

        # ✅ Config(채널별)에서 공정 파라미터 로드
        pressure_wait_timeout_s = float(getattr(self._cfg, "PC_PRESSURE_WAIT_TIMEOUT_S", 180.0))
        rf_pulse_post_on_delay_ms = int(getattr(self._cfg, "PC_RF_PULSE_POST_ON_DELAY_MS", 20_000))
        boost_target = float(getattr(self._cfg, "PC_WORKING_PRESSURE_BOOST_TARGET", 10.0))

        dc_power = float(params.get("dc_power", 0))
        rf_power = float(params.get("rf_power", 0))

        default_integration_ms = int(getattr(self._cfg, "PROCESS_DEFAULT_OES_INTEGRATION_MS", 60))
        try:
            integration_ms = int(float(params.get("integration_time", default_integration_ms)))
        except Exception:
            integration_ms = default_integration_ms

        # --- (옵션) DC Pulse 중간 Power 변경: CSV에 둘 다 있으면 1회 변경 ---
        #  - power_change_time: "5s", "5m", "0.5m", "1h30m" ...
        #  - change_power_value: 100 (W)
        raw_change_time = str(params.get("power_change_time", "") or "").strip()
        raw_change_power = str(params.get("change_power_value", "") or "").strip()

        change_time_sec = self._parse_duration_seconds(raw_change_time) if raw_change_time else 0.0
        try:
            change_power_value = float(raw_change_power) if raw_change_power else 0.0
        except Exception:
            change_power_value = 0.0

        total_window_sec = float(shutter_delay_sec + process_time_sec)

        # ✅ CSV에 값이 "둘 다" 있어야만 적용 + 범위 체크 + 콜백 주입 여부 체크
        do_mid_dc_pulse_change = (
            use_dc_pulse
            and raw_change_time != ""
            and raw_change_power != ""
            and change_power_value > 0.0
            and 0.0 < change_time_sec < total_window_sec
            and self._set_dc_pulse_power is not None
        )

        do_mid_rf_pulse_change = (
            use_rf_pulse
            and raw_change_time != ""
            and raw_change_power != ""
            and change_power_value > 0.0
            and 0.0 < change_time_sec < total_window_sec
            and self._set_rf_pulse_power is not None
        )

        # 둘 다 켜진 경우는 설계상 애매하니(동일 컬럼 공유) 우선순위 명시
        if do_mid_dc_pulse_change and do_mid_rf_pulse_change:
            self._emit_log("Process", "⚠ Pulse 중간 Power 변경: DC/RF Pulse가 동시에 활성 → DC 우선, RF는 무시")
            do_mid_rf_pulse_change = False

        do_mid_pulse_change = (do_mid_dc_pulse_change or do_mid_rf_pulse_change)
        pulse_set_action = (
            ActionType.DC_PULSE_SET if do_mid_dc_pulse_change else
            ActionType.RF_PULSE_SET if do_mid_rf_pulse_change else
            None
        )
        pulse_set_label = "DC Pulse" if do_mid_dc_pulse_change else ("RF Pulse" if do_mid_rf_pulse_change else "Pulse")

        # ✅ DC Pulse는 REF_POWER ACK까지 기다린 뒤 다음 Delay로 넘어간다.
        #    RF Pulse는 기존 동작 유지(no_wait).
        pulse_set_no_wait = (pulse_set_action != ActionType.DC_PULSE_SET)

        # 경고 로그(DC/RF 각각)
        if use_dc_pulse and (raw_change_time != "" or raw_change_power != "") and not do_mid_dc_pulse_change:
            self._emit_log(
                "Process",
                f"⚠ DC Pulse 중간 Power 변경 무시: power_change_time='{raw_change_time}', "
                f"change_power_value='{raw_change_power}', window={total_window_sec:.1f}s, "
                f"callback={'OK' if self._set_dc_pulse_power else 'MISSING'}"
            )

        if use_rf_pulse and (raw_change_time != "" or raw_change_power != "") and not do_mid_rf_pulse_change:
            self._emit_log(
                "Process",
                f"⚠ RF Pulse 중간 Power 변경 무시: power_change_time='{raw_change_time}', "
                f"change_power_value='{raw_change_power}', window={total_window_sec:.1f}s, "
                f"callback={'OK' if self._set_rf_pulse_power else 'MISSING'}"
            )

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

        # --- 압력 제어 시작 (채널별 config) ---
        sp_on_cmd = getattr(
            self._cfg,
            "PROCESS_PRESSURE_CONTROL_SP_ON_CMD",
            'SP3_ON' if self._ch == 1 else 'SP4_ON'
        )
        sp_on_label = getattr(
            self._cfg,
            "PROCESS_PRESSURE_CONTROL_SP_LABEL",
            'SP3' if self._ch == 1 else 'SP4'
        )
        sp_index = int(getattr(
            self._cfg,
            "PROCESS_PRESSURE_CONTROL_SP_INDEX",
            3 if self._ch == 1 else 4
        ))   # 🔹 현재 채널에서 사용하는 SP 번호
        
        # 1) 먼저 채널별 압력 제어 SP3 / SP4를 활성화
        steps.append(ProcessStep(
            action=ActionType.MFC_CMD,
            params=(sp_on_cmd, {}),
            message=f'압력 제어({sp_on_label}) 시작',
        ))

        # 2) MFC에 목표 압력 도달까지 대기 요청 (최대 180초)
        #    - target: UI working_pressure → 읽기 실패 시 fallback
        #    - use_sp_target=True & sp_index=3/4 → 실제 SP3/4 setpoint 기준
        steps.append(ProcessStep(
            action=ActionType.MFC_CMD,
            params=("WAIT_PRESSURE", {
                "target": working_pressure,
                "timeout_sec": pressure_wait_timeout_s,
                "source": "ps",
                "use_sp_target": True,
                "sp_index": sp_index,
            }),
            message=f'압력 도달 대기 (SP{sp_index} setpoint 기준, timeout={pressure_wait_timeout_s:.0f}s)',
        ))

        # steps.append(ProcessStep(
        #     action=ActionType.DELAY,
        #     duration=60000,
        #     message='압력 안정화 대기 (SP3/4, 60초)',
        # ))
        
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
            # ✅ CH2에서만 POWER_SELECT ON
            if self._ch == 2:
                steps.append(ProcessStep(
                    action=ActionType.PLC_CMD,
                    params=("SW_POWER_SELECT", True),
                    message="Power Select ON (SW_POWER_SELECT)"
                ))

            # ✅ CH1/CH2 공통으로 RF Pulse 시작
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
            delay_s = rf_pulse_post_on_delay_ms / 1000.0
            steps.append(ProcessStep(
                action=ActionType.DELAY, duration=rf_pulse_post_on_delay_ms,
                message=f'Power Delay {delay_s:.0f}초', polling=False,
            ))

        # 2) working_pressure < boost_target 인 경우: SP2로 먼저 제어 후 SP1 세팅
        if working_pressure < boost_target:
            steps.append(ProcessStep(
                action=ActionType.MFC_CMD,
                params=('SP2_SET', {'value': boost_target}),
                message=f'목표 압력(SP2) {boost_target:.2f} 설정',
            ))
            steps.append(ProcessStep(
                action=ActionType.MFC_CMD,
                params=('SP2_ON', {}),
                message='압력 제어(SP2) 시작',
            ))
            # SP2로 제어하면서 실제 압력이 boost_target 도달할 때까지 대기
            steps.append(ProcessStep(
                action=ActionType.MFC_CMD,
                params=("WAIT_PRESSURE", {
                    "target": boost_target,
                    "timeout_sec": pressure_wait_timeout_s,
                    "source": "ps",
                }),
                message=f'압력 도달 대기 (SP2, target={boost_target:.2f}, timeout={pressure_wait_timeout_s:.0f}s)',
            ))
            
        # SP2로 안정화 후 SP1 세팅
        # 3) working_pressure >= 5 인 경우: 기존처럼 SP1만 사용
        steps.append(ProcessStep(
            action=ActionType.MFC_CMD,
            params=('SP1_SET', {'value': working_pressure}),
            message=f'목표 압력(SP1) {working_pressure:.2f} 설정',
        ))

        steps.append(ProcessStep(
            action=ActionType.MFC_CMD, params=('SP1_ON', {}),
            message='압력 제어(SP1) 시작',
            polling=False,                         
        ))

        # =========================
        # Shutter Delay / Main Process
        # (옵션) do_mid_dc_pulse_change면 1회 DC_PULSE_SET 삽입
        # =========================

        # --- Shutter Delay ---
        if shutter_delay_sec > 0:
            # 변경 시점이 Shutter Delay 안이면: Delay를 2개로 쪼개고 가운데 Power 변경
            if do_mid_pulse_change  and change_time_sec < shutter_delay_sec:
                part1 = float(change_time_sec)
                part2 = float(shutter_delay_sec - change_time_sec)

                if part1 > 0:
                    steps.append(ProcessStep(
                        action=ActionType.DELAY,
                        duration=int(round(part1 * 1000.0)),
                        message=f'Shutter Delay (변경 전) {part1/60.0:.2f}분',
                        polling=False,
                    ))

                steps.append(ProcessStep(
                    action=pulse_set_action,
                    value=float(change_power_value),
                    message=f'{pulse_set_label} Power 변경 → {change_power_value}W (t={change_time_sec:.1f}s, ShutterDelay)',
                    polling=False,
                    no_wait=pulse_set_no_wait,
                ))

                if part2 > 0:
                    steps.append(ProcessStep(
                        action=ActionType.DELAY,
                        duration=int(round(part2 * 1000.0)),
                        message=f'Shutter Delay (변경 후) {part2/60.0:.2f}분',
                        polling=False,
                    ))
            else:
                steps.append(ProcessStep(
                    action=ActionType.DELAY,
                    duration=int(round(shutter_delay_sec * 1000.0)),
                    message=f'Shutter Delay {shutter_delay_min}분',
                    polling=False,
                ))

        # --- Main Shutter ---
        if use_ms:
            steps.append(ProcessStep(
                action=ActionType.PLC_CMD,
                params=('MS', True, self._ch),
                message='Main Shutter 열기'
            ))

        # --- Main Process Time ---
        if process_time_sec > 0:
            steps.append(ProcessStep(
                action=ActionType.OES_RUN,
                params=(process_time_sec, integration_ms),
                message=f'OES 측정 시작 ({process_time_min}분, {integration_ms}ms)',
                no_wait=True
            ))

            # 변경 시점이 Main 구간이면: Main Delay를 2개로 쪼개고 가운데 Power 변경
            # (change_time_sec == shutter_delay_sec이면 into_main=0 → 메인 시작하자마자 변경)
            if do_mid_pulse_change  and change_time_sec >= shutter_delay_sec and change_time_sec < (shutter_delay_sec + process_time_sec):
                into_main = float(change_time_sec - shutter_delay_sec)
                part1 = max(0.0, into_main)
                part2 = max(0.0, float(process_time_sec - part1))

                if part1 > 0:
                    steps.append(ProcessStep(
                        action=ActionType.DELAY,
                        duration=int(round(part1 * 1000.0)),
                        message=f'메인 공정 진행 (변경 전) {part1/60.0:.2f}분',
                        polling=True
                    ))

                steps.append(ProcessStep(
                    action=pulse_set_action,
                    value=float(change_power_value),
                    message=f'{pulse_set_label} Power 변경 → {change_power_value}W (t={change_time_sec:.1f}s, Main)',
                    polling=True,
                    no_wait=pulse_set_no_wait,
                ))

                if part2 > 0:
                    steps.append(ProcessStep(
                        action=ActionType.DELAY,
                        duration=int(round(part2 * 1000.0)),
                        message=f'메인 공정 진행 (변경 후) {part2/60.0:.2f}분',
                        polling=True
                    ))
            else:
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

            # ✅ CH2에서 RF Pulse를 쓴 경우에만 POWER_SELECT OFF
            if self._ch == 2:
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
        gas_info = ci.get('gas_info') or self._gas_info

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
            
            # ✅ CH2에서 RF Pulse를 쓴 경우에만 POWER_SELECT 즉시 OFF
            if self._ch == 2:
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
            'process_name': self.current_params.get('process_name',
               self.current_params.get('process_note', 'Untitled')),
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
                if step.action in [ActionType.DC_POWER_SET, ActionType.RF_POWER_SET, ActionType.IG_CMD, ActionType.DC_PULSE_SET, ActionType.RF_PULSE_SET]:
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
            "DCPulse":   ("DC_PULSE_TARGET", "DC_PULSE_SET", "DCPULSE_OFF"),
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
            return
        except asyncio.QueueFull:
            # ✅ 로그는 과부하 시 드랍 (파일 로그가 있으니 UI는 버려도 됨)
            if ev.kind == "log":
                return

            # ✅ 상태/중요 이벤트는 넣어야 하므로, 오래된 이벤트를 몇 개 버리고 공간 확보
            for _ in range(50):
                try:
                    _ = self.event_q.get_nowait()  # 가장 오래된 것 버림
                except asyncio.QueueEmpty:
                    break

                try:
                    self.event_q.put_nowait(ev)
                    return
                except asyncio.QueueFull:
                    continue

            # 끝까지 못 넣으면 마지막으로 그냥 드랍
            return

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

        # ✅ 항상 Task로 통일 (Future/Coroutine/Task 모두 안전하게 처리)
        a = asyncio.ensure_future(awaitable)
        b = asyncio.create_task(self._abort_evt.wait())

        done, pending = await asyncio.wait({a, b}, return_when=asyncio.FIRST_COMPLETED)

        for p in pending:
            p.cancel()

        if b in done:
            # abort가 먼저 왔으면 a도 취소
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

    def _parse_duration_seconds(self, s: str) -> float:
        """'5s', '5m', '1.5m', '0.5h', '1h30m', '1h30m10.5s' 등을 초(float)로 변환.
        형식이 맞지 않으면 0.0 반환.
        """
        s = (s or "").strip().lower()
        if not s:
            return 0.0

        # 숫자만 있으면 초로 취급
        if re.fullmatch(r"\d+(?:\.\d+)?", s):
            return float(s)

        # h/m/s 각각 소수 허용 (조합 허용)
        m = re.fullmatch(r"(?:(\d+(?:\.\d+)?)h)?(?:(\d+(?:\.\d+)?)m)?(?:(\d+(?:\.\d+)?)s)?", s)
        if not m:
            return 0.0

        h = float(m.group(1) or 0.0)
        mi = float(m.group(2) or 0.0)
        se = float(m.group(3) or 0.0)
        return float(h * 3600.0 + mi * 60.0 + se)

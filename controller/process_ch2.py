# process_ch2.py — asyncio 기반 

from dataclasses import dataclass
from enum import Enum
from time import monotonic_ns
from typing import Optional, List, Tuple, Dict, Any

from PyQt6.QtCore import Qt, QObject, QTimer, pyqtSignal as Signal, pyqtSlot as Slot


# -------------------- 액션/스텝 정의 --------------------

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
    duration: Optional[int] = None
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
                raise ValueError("OES_RUN params는 (process_time:float, integration_ms:int) 형태여야 합니다.")
        if self.action == ActionType.RF_PULSE_START:
            if self.value is None:
                raise ValueError("RF_PULSE_START에는 value(타깃 파워)가 필요합니다.")
            if not self.params or len(self.params) != 2:
                raise ValueError("RF_PULSE_START params는 (freqHz|None, duty%|None) 형태여야 합니다.")


class ParallelExecution:
    """병렬 블록 진행 상황 추적"""
    def __init__(self, steps: List[ProcessStep], end_index: int):
        self.steps = steps
        self.completed = 0
        self.end_index = end_index
        self.total = len(steps)

    def mark_completed(self) -> bool:
        self.completed += 1
        return self.completed >= self.total

    @property
    def progress(self) -> float:
        return self.completed / self.total if self.total > 0 else 1.0


# -------------------- 프로세스 컨트롤러 --------------------

class ProcessController(QObject):
    # 1) 장치로 보낼 신호
    update_faduino_port = Signal(str, bool)
    mfc_command_requested = Signal(str, dict)
    oes_command_requested = Signal(float, int)
    dc_power_command_requested = Signal(float)
    dc_power_stop_requested = Signal()
    rf_power_command_requested = Signal(float)
    rf_power_stop_requested = Signal()
    ig_command_requested = Signal(float)
    rga_external_scan_requested = Signal()
    rf_pulse_command_requested = Signal(float, object, object)  # (powerW, freqHz|None, duty%|None)
    rf_pulse_stop_requested = Signal()

    # 2) UI/상태
    log_message = Signal(str, str)
    process_status_changed = Signal(bool)
    process_started = Signal(dict)
    process_finished = Signal(bool)
    process_finished_detail = Signal(bool, dict)   # (ok, detail)
    update_process_state = Signal(str)

    # 3) 폴링 제어 (UI에서 대상별 on/off를 반영)
    set_polling = Signal(bool)
    set_polling_targets = Signal(object)  # {'mfc': bool, 'faduino': bool, 'rfpulse': bool}

    # 4) 비상정지 브로드캐스트
    process_aborted = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.process_sequence: List[ProcessStep] = []
        self._current_step_idx = -1
        self.is_running = False
        self.current_params: Dict[str, Any] = {}

        # 정지/종료 흐름 상태
        self._aborting = False               # 긴급 중단
        self._in_emergency = False
        self._stop_requested = False         # 일반 정지 요청
        self._shutdown_in_progress = False   # 종료 시퀀스 실행 중
        self._accept_completions = True      # 완료 신호 수락 여부

        # 종료 중 실패 기록
        self._shutdown_error = False
        self._shutdown_failures: List[str] = []

        # 스텝 타이머(정밀)
        self.step_timer = QTimer(self)
        self.step_timer.setSingleShot(True)
        self.step_timer.setTimerType(Qt.TimerType.PreciseTimer)
        self.step_timer.timeout.connect(self.on_step_completed)

        # 병렬 실행 관리자
        self._px: Optional[ParallelExecution] = None

        # 카운트다운 표시 타이머
        self._countdown_timer = QTimer(self)
        self._countdown_timer.setTimerType(Qt.TimerType.PreciseTimer)
        self._countdown_timer.setInterval(1000)
        self._countdown_timer.timeout.connect(self._on_countdown_tick)
        self._countdown_active = False
        self._countdown_start_ns = 0
        self._countdown_total_ms = 0
        self._countdown_base_message = ""

        # DELAY 타이머 가드
        self._delay_guard = QTimer(self)
        self._delay_guard.setSingleShot(True)
        self._delay_guard.timeout.connect(self._on_delay_guard)
        self._delay_start_ns = 0
        self._delay_total_ms = 0

    # ==================== 시퀀스 구성 ====================

    def start_process(self, params: Dict[str, Any]):
        """UI에서 호출. 시퀀스 생성/검증 후 실행."""
        if self.is_running:
            self.log_message.emit("Process", "오류: 이미 다른 공정이 실행 중입니다.")
            return
        try:
            self.current_params = params or {}
            self.process_sequence = self._create_process_sequence(self.current_params)

            ok, errors = self.validate_process_sequence()
            if not ok:
                for msg in errors:
                    self.log_message.emit("Process", f"[시퀀스 오류] {msg}")
                raise ValueError("공정 시퀀스 검증 실패")

            self._current_step_idx = -1
            self.is_running = True
            self._aborting = False
            self._in_emergency = False
            self._stop_requested = False
            self._shutdown_in_progress = False
            self._shutdown_error = False
            self._shutdown_failures.clear()
            self._accept_completions = True

            self.process_status_changed.emit(True)
            self.process_started.emit(self.current_params)

            process_name = self.current_params.get('process_note', 'Untitled')
            self.log_message.emit("Process", f"=== '{process_name}' 공정 시작 (총 {len(self.process_sequence)}단계) ===")

            self.on_step_completed()  # 첫 스텝 진입
        except Exception as e:
            self.log_message.emit("Process", f"공정 시작 오류: {e}")
            self._finish_process(False)

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
        self._add_initialization_steps(steps, base_pressure, gas_info)

        # --- 가스 주입 ---
        self._add_gas_injection_steps(steps, params, gas_info)

        # --- 압력 제어 시작 ---
        self._add_pressure_control_steps(steps, working_pressure)

        # --- 파워/셔터 ---
        self._add_power_and_shutter_steps(
            steps, params, gun_shutters, use_dc, use_rf, use_ms,
            dc_power, rf_power, shutter_delay_sec, shutter_delay_min
        )

        # --- 메인 공정 시간 ---
        if process_time_sec > 0:
            steps.append(ProcessStep(
                action=ActionType.OES_RUN,
                params=(process_time_sec, integration_ms),
                message=f'OES 측정 시작 ({process_time_min}분, {integration_ms}ms)',
                no_wait=True  # 백그라운드로 돌리고 즉시 다음 스텝(딜레이)로
            ))
            steps.append(ProcessStep(
                action=ActionType.DELAY,
                duration=self._ms_from_sec(process_time_sec),
                message=f'메인 공정 진행 ({process_time_min}분)',
                polling=True
            ))

        # --- 종료 시퀀스 ---
        steps.extend(self._create_shutdown_sequence(params))
        return steps

    def _add_initialization_steps(self, steps: List[ProcessStep], base_pressure: float, gas_info: Dict):
        self.log_message.emit("Process", "전체 공정 모드로 시작 (Base Pressure 대기 포함).")
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

    def _add_gas_injection_steps(self, steps: List[ProcessStep], params: Dict[str, Any], gas_info: Dict):
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

    def _add_pressure_control_steps(self, steps: List[ProcessStep], working_pressure: float):
        steps.extend([
            ProcessStep(action=ActionType.MFC_CMD, params=('SP4_ON', {}), message='압력 제어(SP4) 시작'),
            ProcessStep(action=ActionType.MFC_CMD, params=('SP1_SET', {'value': working_pressure}),
                        message=f'목표 압력(SP1) {working_pressure:.2f} 설정'),
            ProcessStep(action=ActionType.DELAY, duration=60000, message='압력 안정화 대기 (60초)'),
        ])

    def _add_power_and_shutter_steps(
        self, steps: List[ProcessStep], params: Dict[str, Any],
        gun_shutters: List[str], use_dc: bool, use_rf: bool, use_ms: bool,
        dc_power: float, rf_power: float, shutter_delay_sec: float, shutter_delay_min: float
    ):
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

        # RF Pulse 여부
        use_rf_pulse = bool(params.get("use_rf_pulse", False)) and float(params.get("rf_pulse_power", 0)) > 0.0
        rf_pulse_power = float(params.get("rf_pulse_power", 0))
        rf_pulse_freq = params.get("rf_pulse_freq", None)
        if rf_pulse_freq is not None:
            rf_pulse_freq = int(rf_pulse_freq)
        rf_pulse_duty = params.get("rf_pulse_duty", None)
        if rf_pulse_duty is not None:
            rf_pulse_duty = int(rf_pulse_duty)

        # DC/RF/RF Pulse 병렬 여부
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
                duration=self._ms_from_sec(shutter_delay_sec),
                message=f'Shutter Delay {shutter_delay_min}분'
            ))

        if use_ms:
            steps.append(ProcessStep(
                action=ActionType.FADUINO_CMD, params=('MS', True), message='Main Shutter 열기'
            ))

    def _create_shutdown_sequence(self, params: Dict[str, Any], *, force_all: bool = False) -> List[ProcessStep]:
        shutdown_steps: List[ProcessStep] = []
        info = self._get_common_process_info(params)

        use_dc = force_all or info['use_dc']
        use_rf = force_all or info['use_rf']
        use_rf_pulse = force_all or info['use_rf_pulse']
        gas_info = info['gas_info']
        gun_shutters = info['gun_shutters']

        shutdown_steps.append(ProcessStep(
            action=ActionType.FADUINO_CMD, params=('MS', False), message='Main Shutter 닫기 (항상)'
        ))

        if use_dc:
            shutdown_steps.append(ProcessStep(action=ActionType.DC_POWER_STOP, message='DC Power Off'))
        if use_rf:
            shutdown_steps.append(ProcessStep(action=ActionType.RF_POWER_STOP, message='RF Power Off'))
        if use_rf_pulse:
            shutdown_steps.append(ProcessStep(action=ActionType.RF_PULSE_STOP, message='RF Pulse Off'))

        for gas, info_ch in gas_info.items():
            shutdown_steps.append(ProcessStep(
                action=ActionType.MFC_CMD,
                params=('FLOW_OFF', {'channel': info_ch["channel"]}),
                message=f'Ch{info_ch["channel"]}({gas}) Flow Off'
            ))

        shutdown_steps.append(ProcessStep(
            action=ActionType.MFC_CMD, params=('VALVE_OPEN', {}), message='전체 MFC Valve Open'
        ))

        for shutter in gun_shutters:
            if params.get(f"use_{shutter.lower()}", False) or force_all:
                shutdown_steps.append(ProcessStep(
                    action=ActionType.FADUINO_CMD, params=(shutter, False), message=f'Gun Shutter {shutter} 닫기'
                ))

        # Power_select OFF
        if bool(params.get("use_power_select", False)) or force_all:
            shutdown_steps.append(ProcessStep(
                action=ActionType.FADUINO_CMD, params=("N2", False),
                message="Power_select 종료: N2 가스 밸브(Ch3) OFF"
            ))

        for gas in gas_info:
            shutdown_steps.append(ProcessStep(
                action=ActionType.FADUINO_CMD, params=(gas, False), message=f'Faduino {gas} 밸브 닫기'
            ))

        shutdown_steps.append(ProcessStep(
            action=ActionType.FADUINO_CMD, params=('MV', False), message='메인 밸브 닫기'
        ))

        self.log_message.emit("Process", "종료 절차가 생성되었습니다.")
        return shutdown_steps

    def _get_common_process_info(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'use_ms': bool(params.get("use_ms", False)),
            'use_dc': bool(params.get("use_dc_power", False)) and float(params.get("dc_power", 0)) > 0,
            'use_rf': bool(params.get("use_rf_power", False)) and float(params.get("rf_power", 0)) > 0,
            'use_rf_pulse': bool(params.get("use_rf_pulse", False)) and float(params.get("rf_pulse_power", 0)) > 0,
            'gas_info': {"Ar": {"channel": 1}, "O2": {"channel": 2}, "N2": {"channel": 3}},
            'gun_shutters': ["G1", "G2", "G3"],
        }

    # ==================== 실행/진행 ====================

    def _run_next_step(self, step: ProcessStep, step_index: int):
        # 일반 정지 요청이 들어왔고 아직 종료절차가 아니라면: 종료 시퀀스로 전환
        if (self._stop_requested and
            not (self._aborting or self._shutdown_in_progress) and
            not self._in_emergency):
            self.log_message.emit("Process", "정지 요청 감지 - 종료 절차를 시작합니다.")
            self._start_normal_shutdown()
            return

        if not self.is_running or self._aborting:
            self.log_message.emit("Process", f"스텝 실행 중단: running={self.is_running}, aborting={self._aborting}")
            return

        action = step.action
        message = step.message

        self.update_process_state.emit(message)
        self.log_message.emit("Process",
                              f"[{'종료절차' if self._shutdown_in_progress else '공정'} {step_index + 1}/{len(self.process_sequence)}] {message}")

        try:
            if action == ActionType.DELAY:
                duration = step.duration or 100
                if step.polling:
                    # 폴링 ON을 먼저 적용 후, 다음 틱에서 딜레이 시작
                    def _start_delay_with_polling():
                        self._apply_polling(True)
                        self._start_precise_delay(duration, message)
                    QTimer.singleShot(0, _start_delay_with_polling)
                else:
                    self._start_precise_delay(duration, message)
                return

            # 액션 → 핸들러
            handlers = {
                ActionType.DC_POWER_SET: lambda: self.dc_power_command_requested.emit(step.value),
                ActionType.DC_POWER_STOP: lambda: self.dc_power_stop_requested.emit(),
                ActionType.RF_POWER_SET: lambda: self.rf_power_command_requested.emit(step.value),
                ActionType.RF_POWER_STOP: lambda: self.rf_power_stop_requested.emit(),
                ActionType.RF_PULSE_START: lambda: self.rf_pulse_command_requested.emit(
                    float(step.value or 0.0),
                    step.params[0] if step.params else None,
                    step.params[1] if step.params else None
                ),
                ActionType.RF_PULSE_STOP:  lambda: self.rf_pulse_stop_requested.emit(),
                ActionType.IG_CMD:         lambda: self.ig_command_requested.emit(step.value),
                ActionType.RGA_SCAN:       lambda: self.rga_external_scan_requested.emit(),
                ActionType.FADUINO_CMD:    lambda: self.update_faduino_port.emit(*step.params),
                ActionType.MFC_CMD:        lambda: self.mfc_command_requested.emit(*step.params),
                ActionType.OES_RUN:        lambda: self.oes_command_requested.emit(*step.params),
            }

            handler = handlers.get(action)
            if handler:
                handler()
                if step.no_wait:
                    # 백그라운드 작업은 완료 신호를 기다리지 않고 다음으로
                    self._accept_completions = False
                    QTimer.singleShot(100, self._advance_after_nowait)
            else:
                raise ValueError(f"알 수 없는 Action: {action}")

        except Exception as e:
            self.log_message.emit("Process", f"스텝 실행 오류: {e}")
            self.abort_process()

    def on_step_completed(self):
        sender = self.sender()
        if sender is self.step_timer:
            self._stop_countdown()
            if self._delay_guard.isActive():
                self._delay_guard.stop()

        # 정지 요청 → 종료 절차
        if (self._stop_requested and
            not (self._aborting or self._shutdown_in_progress) and
            not self._in_emergency):
            self.log_message.emit("Process", "정지 요청 감지 - 종료 절차를 시작합니다.")
            self._start_normal_shutdown()
            return

        if not self._accept_completions or not self.is_running:
            self.log_message.emit("Process",
                                  f"스텝 완료 무시: accept={self._accept_completions}, running={self.is_running}")
            return

        # 병렬 처리 블록
        if self._px is not None:
            if not self._px.mark_completed():
                self.log_message.emit("Process", f"병렬 작업 진행 중: {self._px.completed}/{self._px.total}")
                return
            self.log_message.emit("Process", f"병렬 작업 {self._px.total}개 모두 완료")
            self._current_step_idx = self._px.end_index
            self._px = None

        # 폴링 OFF
        self._apply_polling(False)

        # 다음 스텝
        self._current_step_idx += 1
        self.log_message.emit("Process",
                              f"다음 스텝으로 진행: {self._current_step_idx + 1}/{len(self.process_sequence)}")

        if self._current_step_idx >= len(self.process_sequence):
            # 종료 절차 여부에 따라 성공 판정
            if self._shutdown_in_progress:
                success = not (self._aborting or self._in_emergency or self._stop_requested or self._shutdown_error)
            else:
                success = not (self._aborting or self._in_emergency or self._stop_requested)
            self._finish_process(success)
            return

        current_step = self.process_sequence[self._current_step_idx]

        if current_step.parallel:
            # 병렬 블록 수집
            parallel_steps: List[Tuple[ProcessStep, int]] = []
            t = self._current_step_idx
            while t < len(self.process_sequence) and self.process_sequence[t].parallel:
                parallel_steps.append((self.process_sequence[t], t))
                t += 1
            self._px = ParallelExecution([s for (s, _) in parallel_steps], end_index=t - 1)
            self._current_step_idx = self._px.end_index

            need_polling = any(s.polling for s, _ in parallel_steps)
            self._apply_polling(need_polling)

            self.log_message.emit("Process", f"병렬 작업 {len(parallel_steps)}개 동시 시작...")
            for step, idx in parallel_steps:
                self._run_next_step(step, idx)
        else:
            self._apply_polling(current_step.polling)
            self._run_next_step(current_step, self._current_step_idx)

    # ==================== 장치 완료/실패 슬롯 ====================

    @Slot()
    def on_device_step_ok(self):
        """어떤 장치에서든 '해당 스텝 완료' 신호가 들어왔을 때 호출."""
        if not self.is_running:
            return

        # 긴급 종료 중엔 특정 스텝만 카운트
        if self._aborting:
            cs = self.current_step
            if not cs or cs.action not in {
                ActionType.FADUINO_CMD, ActionType.MFC_CMD,
                ActionType.DC_POWER_STOP, ActionType.RF_POWER_STOP,
                ActionType.RF_PULSE_STOP
            }:
                return

        self.on_step_completed()

    @Slot(str)
    def on_mfc_confirmed(self, cmd: str):
        """MFCController.command_confirmed(cmd) 연결"""
        if not self.is_running:
            return

        if self._px is not None:
            if any(s.action == ActionType.MFC_CMD and s.params and s.params[0] == cmd for s in self._px.steps):
                self.on_device_step_ok()
            else:
                self.log_message.emit("MFC", f"확인 무시: '{cmd}' (현재 병렬 블록 기대와 불일치)")
            return

        step = self.current_step
        if not step or step.action != ActionType.MFC_CMD:
            self.log_message.emit("MFC",
                                  f"확인 무시: '{cmd}' (현재 스텝: {step.action.name if step else '없음'})")
            return
        expected = step.params[0] if (step.params and len(step.params) >= 1) else None
        if cmd == expected:
            self.on_device_step_ok()
        else:
            self.log_message.emit("MFC", f"확인 무시: '{cmd}', 기대: '{expected}'")

    @Slot(str, str)
    def on_mfc_failed(self, cmd: str, why: str):
        self.on_step_failed("MFC", f"{cmd}: {why}")

    @Slot(str)
    def on_faduino_confirmed(self, cmd: str):
        """FaduinoController.command_confirmed(cmd) 연결"""
        if not self.is_running:
            return

        if self._px is not None:
            if any(s.action == ActionType.FADUINO_CMD for s in self._px.steps):
                self.on_device_step_ok()
            else:
                self.log_message.emit("Faduino",
                                      f"확인 무시: '{cmd}' (현재 병렬 블록에 FADUINO_CMD 없음)")
            return

        if self.current_step and self.current_step.action == ActionType.FADUINO_CMD:
            self.on_device_step_ok()
        else:
            self.log_message.emit("Faduino",
                                  f"확인 무시: '{cmd}' (현재 스텝: {self.current_step.action.name if self.current_step else '없음'})")

    @Slot(str, str)
    def on_faduino_failed(self, cmd: str, why: str):
        self.on_step_failed("Faduino", f"{cmd}: {why}")

    @Slot()
    def on_ig_ok(self):
        """IGController의 '대기 완료' 연결"""
        self.on_device_step_ok()

    @Slot(str, str)
    def on_ig_failed(self, src: str, why: str):
        self.on_step_failed(src or "IG", why)

    @Slot()
    def on_oes_ok(self):
        """OES는 메인 DELAY와 병렬이므로 완료해도 시퀀스 진행 X"""
        self.log_message.emit("OES", "OES 측정 종료(정상). 메인 공정은 계속 진행됩니다.")

    @Slot(str, str)
    def on_oes_failed(self, src: str, why: str):
        self.on_step_failed(src or "OES", why)

    @Slot()
    def on_rga_finished(self):
        self.on_step_completed()

    @Slot(str, str)
    def on_rga_failed(self, src: str, why: str):
        self.on_step_failed(src or "RGA", why)

    @Slot()
    def on_dc_target_reached(self):
        """DCPowerController.target_reached → DC_POWER_SET 스텝 완료"""
        if not self.is_running:
            return
        if self._px is not None:
            if any(s.action == ActionType.DC_POWER_SET for s in self._px.steps):
                self.on_device_step_ok()
            return
        if self.current_step and self.current_step.action == ActionType.DC_POWER_SET:
            self.on_device_step_ok()

    @Slot()
    def on_rf_target_reached(self):
        """RF 일반 / RF Pulse 공용 완료 신호"""
        if not self.is_running:
            return
        if self._px is not None:
            if any(s.action in (ActionType.RF_POWER_SET, ActionType.RF_PULSE_START) for s in self._px.steps):
                self.on_device_step_ok()
            return
        if self.current_step and self.current_step.action in (ActionType.RF_POWER_SET, ActionType.RF_PULSE_START):
            self.on_device_step_ok()

    @Slot()
    def on_rf_pulse_off_finished(self):
        """RFPulse power_off_finished → RF_PULSE_STOP 스텝 완료"""
        if not self.is_running:
            return
        if self._px is not None:
            if any(s.action == ActionType.RF_PULSE_STOP for s in self._px.steps):
                self.on_device_step_ok()
            return
        if self.current_step and self.current_step.action == ActionType.RF_PULSE_STOP:
            self.on_device_step_ok()

    @Slot(str)
    def on_rf_pulse_failed(self, why: str):
        self.on_step_failed("RFPulse", why or "unknown")

    # ==================== 실패 처리 ====================

    def on_step_failed(self, source: str, reason: str):
        if not self.is_running:
            return

        full_reason = f"[{source} - {reason}]"
        cur = self.current_step

        # 종료(일반/긴급) 중엔 실패를 기록하고 계속 진행
        if self._aborting or self._shutdown_in_progress:
            step_no = self._current_step_idx + 1
            act_name = cur.action.name if cur else "UNKNOWN"
            self._shutdown_error = True
            self._shutdown_failures.append(f"Step {step_no} {act_name}: {full_reason}")
            self.log_message.emit("Process",
                                  f"경고: 종료 중 단계 실패 → 계속 진행 ({act_name}, 사유: {full_reason})")
            self.on_step_completed()
            return

        # 평시 실패: 안전 종료로 전환
        self.log_message.emit("Process", f"오류 발생: {full_reason}. 종료 절차를 시작합니다.")
        self._start_normal_shutdown()

    # ==================== 카운트다운/DALAY ====================

    def _start_precise_delay(self, duration_ms: int, message: str):
        self._delay_total_ms = int(duration_ms)
        self._delay_start_ns = monotonic_ns()

        # 메인 타이머 (정밀)
        self.step_timer.start(self._delay_total_ms)

        # 카운트다운 시작
        self._start_countdown(self._delay_total_ms, message)

        # 가드 타이머: 메인 타이머 + 2s 버퍼
        guard_ms = self._delay_total_ms + 2000
        if self._delay_guard.isActive():
            self._delay_guard.stop()
        self._delay_guard.start(guard_ms)

        self.log_message.emit("Process",
                              f"[DELAY start] {message} / {self._delay_total_ms}ms, guard={guard_ms}ms")

    def _on_delay_guard(self):
        if not self.is_running:
            return
        step = self.current_step
        if step and step.action == ActionType.DELAY:
            elapsed_ms = (monotonic_ns() - self._delay_start_ns) // 1_000_000
            if elapsed_ms >= self._delay_total_ms:
                if self.step_timer.isActive():
                    self.step_timer.stop()
                self.log_message.emit("Process", "[DELAY guard] 메인 타이머 지연/유실 감지 → 강제 완료")
                self.on_step_completed()

    def _start_countdown(self, duration_ms: int, base_message: str):
        self._countdown_active = True
        self._countdown_total_ms = int(duration_ms)
        self._countdown_start_ns = monotonic_ns()
        self._countdown_base_message = base_message
        if not self._countdown_timer.isActive():
            self._countdown_timer.start()
        self._on_countdown_tick()

    def _stop_countdown(self):
        self._countdown_timer.stop()
        self._countdown_total_ms = 0
        self._countdown_start_ns = 0
        self._countdown_base_message = ""
        self._countdown_active = False

    def _on_countdown_tick(self):
        if self._stop_requested and not (self._aborting or self._shutdown_in_progress):
            self.log_message.emit("Process", "정지 요청 감지 - 종료 절차를 시작합니다.")
            self._stop_countdown()
            self._start_normal_shutdown()
            return

        if not self._countdown_active or self._countdown_total_ms <= 0:
            self._stop_countdown()
            return

        elapsed_ms = (monotonic_ns() - self._countdown_start_ns) // 1_000_000
        remaining_ms = max(0, self._countdown_total_ms - int(elapsed_ms))

        remaining_sec = remaining_ms // 1000
        minutes = remaining_sec // 60
        seconds = remaining_sec % 60
        time_str = f"{minutes}분 {seconds}초" if minutes > 0 else f"{seconds}초"
        self.update_process_state.emit(f"{self._countdown_base_message} (남은 시간: {time_str})")

        if remaining_ms == 0:
            self._stop_countdown()

    # ==================== 종료/비상 ====================

    def _finish_process(self, success: bool):
        if not self.is_running:
            return

        # 리포트 컨텍스트(리셋 전에 캡처)
        finish_ctx = {
            "process_name": self.current_params.get("process_note",
                                                    self.current_params.get("Process_name", "Untitled")),
            "stopped": self._stop_requested,
            "aborting": (self._aborting or self._in_emergency),
            "errors": list(self._shutdown_failures),
        }
        try:
            self.process_finished_detail.emit(success, finish_ctx)
        except Exception:
            pass

        self.is_running = False
        self._px = None
        self.step_timer.stop()
        self._stop_countdown()
        self._apply_polling(False)

        # 리셋 플래그
        self._shutdown_in_progress = False
        self._stop_requested = False
        self._aborting = False
        self._in_emergency = False
        self._shutdown_error = False
        self._shutdown_failures.clear()

        if success:
            self.log_message.emit("Process", "=== 공정이 성공적으로 완료되었습니다 ===")
            self.update_process_state.emit("공정 완료")
        else:
            self.log_message.emit("Process", "=== 공정이 중단되었습니다 ===")
            self.update_process_state.emit("공정 중단됨")
            if finish_ctx["errors"]:
                self.log_message.emit("Process", f"[종료 중 실패 요약] 총 {len(finish_ctx['errors'])}건")
                for item in finish_ctx["errors"]:
                    self.log_message.emit("Process", f" - {item}")
            if self._aborting or self._in_emergency:
                self.process_aborted.emit()

        self.process_status_changed.emit(False)
        self.process_finished.emit(success)

    def abort_process(self):
        """긴급 중단(즉시 차단 시퀀스)"""
        if not self.is_running:
            return
        if self._aborting:
            self.log_message.emit("Process", "(중복) 긴급 중단 진행 중 - 추가 호출 무시")
            return

        self.step_timer.stop()
        self._stop_countdown()
        self._apply_polling(False)
        self._px = None

        self.log_message.emit("Process", "공정 긴급 중단을 시작합니다...")
        self._aborting = True
        self._accept_completions = False
        self._shutdown_in_progress = True

        emergency_steps = self._create_emergency_shutdown_sequence()
        if emergency_steps:
            self.process_sequence = emergency_steps
            self._current_step_idx = -1
            self._px = None
            self._accept_completions = True
            self.on_step_completed()
        else:
            self._finish_process(False)

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

        self.log_message.emit("Process", "긴급 종료 절차가 생성되었습니다.")
        return steps

    def _start_normal_shutdown(self):
        """일반 정지 요청 → 안전한 종료 절차 시작"""
        if self._aborting:
            self.log_message.emit("Process", "종료 절차 무시: 이미 긴급 중단 중입니다.")
            return
        if self._shutdown_in_progress:
            self.log_message.emit("Process", "종료 절차 무시: 이미 종료 절차 진행 중입니다.")
            return

        self._shutdown_in_progress = True
        self.log_message.emit("Process", "정지 요청 - 안전한 종료 절차를 시작합니다.")

        self.step_timer.stop()
        self._stop_countdown()
        self._apply_polling(False)
        self._px = None

        try:
            shutdown_steps = self._create_shutdown_sequence(self.current_params or {})
            if shutdown_steps:
                self.log_message.emit("Process", f"종료 절차 생성 완료: {len(shutdown_steps)}단계")
                self.process_sequence = shutdown_steps
                self._current_step_idx = -1
                self._px = None
                self._accept_completions = True
                QTimer.singleShot(100, self.on_step_completed)
            else:
                self.log_message.emit("Process", "종료 절차가 없어서 즉시 완료합니다.")
                self._finish_process(False)
        except Exception as e:
            self.log_message.emit("Process", f"종료 절차 시작 오류: {e}")
            self._finish_process(False)

    def request_stop(self):
        """UI가 호출하는 '일반 정지'"""
        if self._aborting:
            self.log_message.emit("Process", "정지 요청: 이미 긴급 중단 처리 중입니다.")
            return
        if self._stop_requested or self._shutdown_in_progress:
            self.log_message.emit("Process", "정지 요청: 이미 정지 처리 중입니다.")
            return

        self.log_message.emit("Process", "정지 요청을 받았습니다.")
        self._stop_requested = True

        # DELAY 중이면 타이머 중단 후 바로 종료 절차
        if self.step_timer.isActive():
            self.log_message.emit("Process", "현재 대기 타이머를 중단하고 종료 절차로 진입합니다.")
            self.step_timer.stop()
            self._stop_countdown()

        if not self.is_running:
            # 공정 미실행 → 강제 종료 시퀀스 수행
            self.log_message.emit("Process", "정지 요청: 공정 미실행 → 강제 종료 시퀀스 수행")
            self.process_sequence = self._create_shutdown_sequence(self.current_params or {}, force_all=True)
            self._shutdown_in_progress = True
            self.is_running = True
            self._current_step_idx = -1
            self._accept_completions = True
            self.on_step_completed()
            return

        self._start_normal_shutdown()

    # ==================== 조회/검증 ====================

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
        return {
            'total_steps': len(self.process_sequence),
            'current_step': self._current_step_idx + 1,
            'progress': self.progress,
            'is_running': self.is_running,
            'is_parallel': self._px is not None,
            'parallel_progress': self._px.progress if self._px else 0.0,
            'current_step_info': ({
                'action': self.current_step.action.name,
                'message': self.current_step.message,
                'parallel': self.current_step.parallel
            } if self.current_step else None),
            'process_name': self.current_params.get('process_note', 'Untitled'),
            'stop_requested': self._stop_requested,
            'aborting': self._aborting,
        }

    def validate_process_sequence(self) -> Tuple[bool, List[str]]:
        errors = []
        try:
            in_parallel = False
            for i, step in enumerate(self.process_sequence):
                if step.parallel and not in_parallel:
                    in_parallel = True
                elif not step.parallel and in_parallel:
                    in_parallel = False

                step_num = i + 1
                if step.action == ActionType.DELAY and step.duration is None:
                    errors.append(f"Step {step_num}: DELAY 액션에 duration이 없습니다.")
                if step.action in [ActionType.DC_POWER_SET, ActionType.RF_POWER_SET, ActionType.IG_CMD]:
                    if step.value is None:
                        errors.append(f"Step {step_num}: {step.action.name} 액션에 value가 없습니다.")
                if step.action == ActionType.RF_PULSE_START:
                    if step.value is None:
                        errors.append(f"Step {step_num}: RF_PULSE_START에 value(파워)가 없습니다.")
                    if step.params is None or len(step.params) != 2:
                        errors.append(f"Step {step_num}: RF_PULSE_START params=(freq, duty) 필요.")
                if step.action in [ActionType.FADUINO_CMD, ActionType.MFC_CMD, ActionType.OES_RUN]:
                    if step.params is None:
                        errors.append(f"Step {step_num}: {step.action.name} 액션에 params가 없습니다.")
                if step.parallel and i > 0:
                    prev = self.process_sequence[i - 1]
                    if not prev.parallel:
                        errors.append(f"Step {step_num}: 병렬 스텝이 비병렬 스텝 뒤에 있습니다.")
        except Exception as e:
            errors.append(f"검증 중 오류 발생: {e}")
        return len(errors) == 0, errors

    def get_estimated_duration(self) -> int:
        return sum((s.duration or 0) for s in self.process_sequence if s.action == ActionType.DELAY)

    def print_process_sequence(self):
        print(f"\n=== 공정 시퀀스 (총 {len(self.process_sequence)}단계) ===")
        for i, step in enumerate(self.process_sequence):
            parallel_mark = "[병렬]" if step.parallel else ""
            polling_mark = "[폴링]" if step.polling else ""
            print(f"{i+1:3d}. {step.action.name:15s} {parallel_mark}{polling_mark}: {step.message}")
            if step.value is not None:
                print(f"     └─ value: {step.value}")
            if step.params is not None:
                print(f"     └─ params: {step.params}")
            if step.duration is not None:
                print(f"     └─ duration: {step.duration}ms")
        est = self.get_estimated_duration()
        print(f"\n예상 소요 시간: {est/1000:.1f}초 ({est/60000:.1f}분)")
        print("=" * 50)

    # ==================== 안전/리셋 ====================

    def stop_now(self):
        """즉시 차단이 아닌 안전한 정지 요청으로 통일"""
        self.request_stop()

    def emergency_stop(self):
        self.log_message.emit("Process", "*** 비상 정지 활성화 ***")
        self._in_emergency = True
        # 즉시 차단 브로드캐스트(장치별 자체 안전 처리)
        self.update_faduino_port.emit('MS', False)
        self.dc_power_stop_requested.emit()
        self.rf_power_stop_requested.emit()
        self.rf_pulse_stop_requested.emit()
        self.abort_process()

    def reset_controller(self):
        self.step_timer.stop()
        self._stop_countdown()
        self.is_running = False
        self._aborting = False
        self._accept_completions = True
        self._in_emergency = False
        self._stop_requested = False
        self._shutdown_error = False
        self._shutdown_failures = []
        self._current_step_idx = -1
        self._px = None
        self.process_sequence.clear()
        self.current_params.clear()
        self._apply_polling(False)
        self.process_status_changed.emit(False)
        self.update_process_state.emit("대기 중")
        self.log_message.emit("Process", "프로세스 컨트롤러가 리셋되었습니다.")

    def get_debug_status(self) -> Dict[str, Any]:
        """디버깅용 상태 정보"""
        return {
            'is_running': self.is_running,
            'current_step_idx': self._current_step_idx,
            'total_steps': len(self.process_sequence),
            'accept_completions': self._accept_completions,
            'aborting': self._aborting,
            'stop_requested': self._stop_requested,
            'shutdown_in_progress': self._shutdown_in_progress,
            'in_emergency': self._in_emergency,
            'step_timer_active': self.step_timer.isActive(),
            'countdown_active': self._countdown_active,
            'parallel_execution': self._px is not None,
            'current_step_action': self.current_step.action.name if self.current_step else None,
            'current_step_message': self.current_step.message if self.current_step else None,
        }

    # ==================== 폴링 ====================

    def _compute_polling_targets(self, active: bool) -> Dict[str, bool]:
        """
        active=False → 전부 False
        active=True →
          - RF Pulse 사용:  MFC=True, Faduino=False, RFPulse=True
          - 그 외(DC/RF):  MFC=True, Faduino=True,  RFPulse=False
        """
        if not active:
            return {'mfc': False, 'faduino': False, 'rfpulse': False}
        info = self._get_common_process_info(self.current_params or {})
        if info.get('use_rf_pulse', False):
            return {'mfc': True, 'faduino': False, 'rfpulse': True}
        return {'mfc': True, 'faduino': True, 'rfpulse': False}

    def _apply_polling(self, active: bool):
        self.set_polling.emit(active)
        self.set_polling_targets.emit(self._compute_polling_targets(active))

    # ==================== 타임/유틸 ====================

    def _ms_from_sec(self, sec: float) -> int:
        return int(round(sec * 1000.0))

# -*- coding: utf-8 -*-
import sys
import re
import csv
import asyncio
from typing import Optional
from datetime import datetime

from PySide6.QtWidgets import QApplication, QWidget, QMessageBox, QFileDialog, QPlainTextEdit
from PySide6.QtCore import QCoreApplication, Qt, QTimer, Slot, Signal
from qasync import QEventLoop

# === imports ===
from ui import Ui_Form
from controller.graph_controller import GraphController
from controller.data_logger import DataLogger
from controller.chat_notifier import ChatNotifier

# ✅ 실제 장비 모듈(비동기)
from device.faduino import AsyncFaduino
from device.ig import AsyncIG
from device.rga import external_scan
from device.mfc import AsyncMFC
from device.oes import run_measurement as run_oes_measurement
from device.dc_power import RF_SAFE_FLOAT as _RF_SAFE_FLOAT  # (있으면) 사용, 없어도 무방
from device.dc_power import DCPowerAsync
from device.rf_power import RFPowerAsync
from device.rf_pulse import RFPulseAsync

# ✅ CH2 공정 컨트롤러
from controller.process_ch2 import ProcessController

from lib.config_ch2 import CHAT_WEBHOOK_URL, ENABLE_CHAT_NOTIFY


class MainWindow(QWidget):
    # UI 로그 배치 플러시용(내부에서만 사용)
    _log_flush_timer: Optional[QTimer] = None

    # 공정 완료 → 다음 공정으로
    process_finished_to_next = Signal(bool)

    def __init__(self):
        super().__init__()
        self.ui = Ui_Form()
        self.ui.setupUi(self)

        # --- Tab 키로 다음 필드로 이동 (QPlainTextEdit 전체 적용)
        for edit in self.findChildren(QPlainTextEdit):
            edit.setTabChangesFocus(True)

        self._set_default_ui_values()
        self.process_queue = []
        self.current_process_index = -1
        self._delay_timer: Optional[QTimer] = None
        self._shutdown_called = False

        # === 컨트롤러 (메인 스레드에서 생성) ===
        self.graph_controller = GraphController(self.ui.ch2_rgaGraph_widget, self.ui.ch2_oesGraph_widget)
        self.data_logger = DataLogger()
        self.process_controller = ProcessController()

        # === 비동기 장치 ===
        self.faduino = AsyncFaduino()
        self.mfc = AsyncMFC()
        self.ig = AsyncIG()
        self.dc_power = DCPowerAsync()
        self.rf_power = RFPowerAsync()
        self.rf_pulse = RFPulseAsync()

        # === Google Chat 알림(옵션) ===
        self.chat_notifier = ChatNotifier(CHAT_WEBHOOK_URL) if ENABLE_CHAT_NOTIFY else None
        if self.chat_notifier:
            self.chat_notifier.start()

        # === 신호 연결 ===
        self._connect_signals()

        # === 백그라운드 태스크 시작 ===
        loop = asyncio.get_running_loop()
        self._bg_tasks = [
            # 장치 내부 루프 (있다면)
            loop.create_task(self.faduino.start()),
            loop.create_task(self.mfc.start()),
            loop.create_task(self.ig.start()),
            loop.create_task(self.dc_power.start()),
            loop.create_task(self.rf_power.start()),
            loop.create_task(self.rf_pulse.start()),

            # 이벤트 펌프(장치 → UI/ProcessController 브릿지)
            loop.create_task(self._pump_faduino_events()),
            loop.create_task(self._pump_mfc_events()),
            loop.create_task(self._pump_ig_events()),
            loop.create_task(self._pump_dc_events()),
            loop.create_task(self._pump_rf_events()),
            loop.create_task(self._pump_rfpulse_events()),
        ]

        # 로그 배치 flush
        self.ui.ch2_logMessage_edit.setMaximumBlockCount(2000)
        self._log_ui_buf = []
        self._log_file_buf = []
        self._log_flush_timer = QTimer(self)
        self._log_flush_timer.setInterval(100)
        self._log_flush_timer.timeout.connect(self._flush_logs)
        self._log_flush_timer.start()

        # 앱 종료 훅
        app = QCoreApplication.instance()
        if app is not None:
            app.aboutToQuit.connect(lambda: self._shutdown_once("aboutToQuit"))

        # 공정 완료 → 다음 공정
        self.process_controller.process_finished.connect(self._start_next_process_from_queue)

    # ------------------------------------------------------------------
    # 신호 연결 (Qt → 어댑터/브릿지)
    # ------------------------------------------------------------------
    def _connect_signals(self):
        # === DataLogger ===
        self.process_controller.process_started.connect(self.data_logger.start_new_log_session,
                                                       type=Qt.ConnectionType.QueuedConnection)
        self.process_controller.process_finished.connect(self.data_logger.finalize_and_write_log,
                                                        type=Qt.ConnectionType.QueuedConnection)

        # 공정 시작 시 그래프 초기화
        self.process_controller.process_started.connect(
            lambda *args: self.graph_controller.reset(),
            type=Qt.ConnectionType.QueuedConnection
        )

        # === 로그/상태 ===
        self.process_controller.log_message.connect(self.append_log)
        self.process_controller.update_process_state.connect(self.on_update_process_state)

        # === ProcessController → 장치 (비동기 어댑터) ===
        self.process_controller.update_faduino_port.connect(self._on_faduino_named,
                                                            type=Qt.ConnectionType.QueuedConnection)
        self.process_controller.mfc_command_requested.connect(self._on_mfc_command,
                                                              type=Qt.ConnectionType.QueuedConnection)
        self.process_controller.oes_command_requested.connect(self._on_oes_run,
                                                              type=Qt.ConnectionType.QueuedConnection)
        self.process_controller.dc_power_command_requested.connect(self._on_dc_start,
                                                                   type=Qt.ConnectionType.QueuedConnection)
        self.process_controller.dc_power_stop_requested.connect(self._on_dc_stop,
                                                                type=Qt.ConnectionType.QueuedConnection)
        self.process_controller.rf_power_command_requested.connect(self._on_rf_start,
                                                                   type=Qt.ConnectionType.QueuedConnection)
        self.process_controller.rf_power_stop_requested.connect(self._on_rf_stop,
                                                                type=Qt.ConnectionType.QueuedConnection)
        self.process_controller.rf_pulse_command_requested.connect(self._on_rfpulse_start,
                                                                   type=Qt.ConnectionType.QueuedConnection)
        self.process_controller.rf_pulse_stop_requested.connect(self._on_rfpulse_stop,
                                                                type=Qt.ConnectionType.QueuedConnection)
        self.process_controller.rga_external_scan_requested.connect(self._on_rga_scan,
                                                                    type=Qt.ConnectionType.QueuedConnection)
        self.process_controller.ig_command_requested.connect(self._on_ig_wait,
                                                             type=Qt.ConnectionType.QueuedConnection)

        # 폴링 on/off
        self.process_controller.set_polling_targets.connect(self._apply_polling_targets,
                                                            type=Qt.ConnectionType.QueuedConnection)

        # === UI 버튼 (CH2) ===
        self.ui.ch2_Start_button.clicked.connect(self.on_start_button_clicked)
        self.ui.ch2_Stop_button.clicked.connect(self.on_stop_button_clicked)
        self.ui.ch2_processList_button.clicked.connect(self.on_process_list_button_clicked)

        self.process_controller.process_status_changed.connect(self._on_process_status_changed)
        self._on_process_status_changed(False)

        # === 그래프/UI 업데이트 (장치 이벤트 펌프에서 직접 호출도 병행) ===
        # RGA/OES는 아래 어댑터에서 그래프 컨트롤러로 직접 전달

        # === Google Chat 알림 (ProcessController의 신호만 연결) ===
        if self.chat_notifier is not None:
            self.process_controller.process_started.connect(
                self.chat_notifier.notify_process_started,
                type=Qt.ConnectionType.QueuedConnection
            )
            self.process_controller.process_finished_detail.connect(
                self.chat_notifier.notify_process_finished_detail,
                type=Qt.ConnectionType.QueuedConnection
            )
            self.process_controller.process_aborted.connect(
                lambda: self.chat_notifier.notify_text("🛑 공정이 중단되었습니다."),
                type=Qt.ConnectionType.QueuedConnection
            )

    # ------------------------------------------------------------------
    # 비동기 이벤트 펌프 (장치 → UI/ProcessController)
    # ------------------------------------------------------------------
    async def _pump_faduino_events(self):
        q = self.faduino.event_q
        while True:
            ev = await q.get()
            kind = getattr(ev, "kind", None)
            if kind == "status":
                self.append_log("Faduino", getattr(ev, "msg", ""))
            elif kind == "command_confirmed":
                self.process_controller.on_faduino_confirmed(getattr(ev, "cmd", ""))
            elif kind == "command_failed":
                why = getattr(ev, "why", "unknown")
                self.process_controller.on_faduino_failed(getattr(ev, "cmd", ""), why)
                if self.chat_notifier:
                    self.chat_notifier.notify_error_with_src("Faduino", why)
            elif kind == "dc_power":
                p = getattr(ev, "power", 0.0)
                v = getattr(ev, "voltage", 0.0)
                c = getattr(ev, "current", 0.0)
                # 데이터 로깅 & UI 갱신
                try:
                    self.data_logger.log_dc_power(p, v, c)
                except Exception:
                    pass
                self.handle_dc_power_display(p, v, c)
                # 제어기에 전달(목표 도달/유지 판단은 제어기 내부)
                self.dc_power.update_measurements(p, v, c)
            elif kind == "rf_power":
                f = getattr(ev, "forward", 0.0)
                r = getattr(ev, "reflected", 0.0)
                try:
                    self.data_logger.log_rf_power(f, r)
                except Exception:
                    pass
                self.handle_rf_power_display(f, r)
                self.rf_power.update_measurements(f, r)

    async def _pump_mfc_events(self):
        q = self.mfc.event_q
        while True:
            ev = await q.get()
            k = getattr(ev, "kind", None)
            if k == "status":
                self.append_log("MFC", getattr(ev, "msg", ""))
            elif k == "command_confirmed":
                self.process_controller.on_mfc_confirmed(getattr(ev, "cmd", ""))
            elif k == "command_failed":
                cmd = getattr(ev, "cmd", "")
                why = getattr(ev, "why", "unknown")
                self.process_controller.on_mfc_failed(cmd, why)
                if self.chat_notifier:
                    self.chat_notifier.notify_error_with_src("MFC", f"{cmd}: {why}")
            elif k == "update_flow":
                gas = getattr(ev, "gas", "")
                flow = float(getattr(ev, "flow", 0.0))
                try:
                    self.data_logger.log_mfc_flow(gas, flow)
                except Exception:
                    pass
                self.update_mfc_flow_ui(gas, flow)
            elif k == "update_pressure":
                pressure_str = getattr(ev, "pressure_str", "")
                try:
                    self.data_logger.log_mfc_pressure(pressure_str)
                except Exception:
                    pass
                self.update_mfc_pressure_ui(pressure_str)

    async def _pump_ig_events(self):
        q = self.ig.event_q
        while True:
            ev = await q.get()
            k = getattr(ev, "kind", None)
            if k == "status":
                self.append_log("IG", getattr(ev, "msg", ""))
            elif k == "ok":
                self.process_controller.on_ig_ok()
            elif k == "failed":
                why = getattr(ev, "why", "unknown")
                self.process_controller.on_ig_failed("IG", why)
                if self.chat_notifier:
                    self.chat_notifier.notify_error("IG", why)

    async def _pump_dc_events(self):
        q = self.dc_power.event_q
        while True:
            ev = await q.get()
            k = getattr(ev, "kind", None)
            if k == "status":
                self.append_log("DCpower", getattr(ev, "msg", ""))
            elif k == "state_changed":
                # Faduino에 상태 전달(인터락/폴링 스위치 등에 필요 시)
                try:
                    self.faduino.on_dc_state_changed(bool(getattr(ev, "running", False)))
                except Exception:
                    pass
            elif k == "target_reached":
                self.process_controller.on_dc_target_reached()
            elif k == "power_off_finished":
                self.process_controller.on_device_step_ok()

    async def _pump_rf_events(self):
        q = self.rf_power.event_q
        while True:
            ev = await q.get()
            k = getattr(ev, "kind", None)
            if k == "status":
                self.append_log("RFpower", getattr(ev, "msg", ""))
            elif k == "state_changed":
                try:
                    self.faduino.on_rf_state_changed(bool(getattr(ev, "running", False)))
                except Exception:
                    pass
            elif k == "target_reached":
                self.process_controller.on_rf_target_reached()
            elif k == "target_failed":
                why = getattr(ev, "why", "unknown")
                self.process_controller.on_step_failed("RF Power", why)
                if self.chat_notifier:
                    self.chat_notifier.notify_error_with_src("RF Power", why)
            elif k == "power_off_finished":
                self.process_controller.on_device_step_ok()

    async def _pump_rfpulse_events(self):
        q = self.rf_pulse.event_q
        while True:
            ev = await q.get()
            k = getattr(ev, "kind", None)
            if k == "status":
                self.append_log("RFPulse", getattr(ev, "msg", ""))
            elif k == "target_reached":
                self.process_controller.on_rf_target_reached()
            elif k == "target_failed":
                why = getattr(ev, "why", "unknown")
                self.process_controller.on_step_failed("RF Pulse", why)
                if self.chat_notifier:
                    self.chat_notifier.notify_error_with_src("RF Pulse", why)
            elif k == "power_off_finished":
                self.process_controller.on_rf_pulse_off_finished()

    # ------------------------------------------------------------------
    # ProcessController → 장치 (비동기 어댑터)
    # ------------------------------------------------------------------
    @Slot(str, bool)
    def _on_faduino_named(self, name: str, state: bool):
        asyncio.create_task(self.faduino.handle_named_command(name, state))

    @Slot(str, dict)
    def _on_mfc_command(self, cmd: str, args: dict):
        asyncio.create_task(self.mfc.handle_command(cmd, args))

    @Slot(float, int)
    def _on_oes_run(self, duration_sec: float, integration_ms: int):
        async def run():
            try:
                async for x_data, y_data in run_oes_measurement(duration_sec, integration_ms):
                    # 그래프 업데이트
                    self.graph_controller.update_oes_plot(x_data, y_data)
                # 완료
                self.process_controller.on_oes_ok()
            except Exception as e:
                self.process_controller.on_oes_failed("OES", str(e))
                if self.chat_notifier:
                    self.chat_notifier.notify_error("OES", str(e))
        asyncio.create_task(run())

    @Slot(float)
    def _on_dc_start(self, power_w: float):
        asyncio.create_task(self.dc_power.start_process(float(power_w)))

    @Slot()
    def _on_dc_stop(self):
        asyncio.create_task(self.dc_power.stop_process())

    @Slot(float)
    def _on_rf_start(self, power_w: float):
        asyncio.create_task(self.rf_power.start_process(float(power_w)))

    @Slot()
    def _on_rf_stop(self):
        asyncio.create_task(self.rf_power.stop_process())

    @Slot(float, object, object)
    def _on_rfpulse_start(self, power_w: float, freq, duty):
        asyncio.create_task(self.rf_pulse.start_pulse_process(float(power_w), freq, duty))

    @Slot()
    def _on_rfpulse_stop(self):
        asyncio.create_task(self.rf_pulse.stop_process())

    @Slot()
    def _on_rga_scan(self):
        async def run():
            try:
                mass_axis, pressures = await external_scan()
                self.graph_controller.update_rga_plot(mass_axis, pressures)
                self.process_controller.on_rga_finished()
            except Exception as e:
                self.process_controller.on_rga_failed("RGA", str(e))
                if self.chat_notifier:
                    self.chat_notifier.notify_error("RGA", str(e))
        asyncio.create_task(run())

    @Slot(float)
    def _on_ig_wait(self, base_pressure: float):
        asyncio.create_task(self.ig.wait_for_base_pressure(float(base_pressure)))

    # ------------------------------------------------------------------
    # 표시/입력 관련 슬롯
    # ------------------------------------------------------------------
    @Slot(float, float)
    def handle_rf_power_display(self, for_p, ref_p):
        if for_p is None or ref_p is None:
            self.append_log("MAIN", "for.p, ref.p 값이 비어있습니다.")
            return
        self.ui.ch2_forP_edit.setPlainText(f"{for_p:.2f}")
        self.ui.ch2_refP_edit.setPlainText(f"{ref_p:.2f}")

    @Slot(float, float, float)
    def handle_dc_power_display(self, power, voltage, current):
        if power is None or voltage is None or current is None:
            self.append_log("MAIN", "power, voltage, current값이 비어있습니다.")
            return
        self.ui.ch2_Power_edit.setPlainText(f"{power:.3f}")
        self.ui.ch2_Voltage_edit.setPlainText(f"{voltage:.3f}")
        self.ui.ch2_Current_edit.setPlainText(f"{current:.3f}")

    @Slot(str)
    def update_mfc_pressure_ui(self, pressure_value):
        self.ui.ch2_workingPressure_edit.setPlainText(pressure_value)

    @Slot(str, float)
    def update_mfc_flow_ui(self, gas_name, flow_value):
        if gas_name == "Ar":
            self.ui.ch2_arFlow_edit.setPlainText(f"{flow_value:.1f}")
        elif gas_name == "O2":
            self.ui.ch2_o2Flow_edit.setPlainText(f"{flow_value:.1f}")
        elif gas_name == "N2":
            self.ui.ch2_n2Flow_edit.setPlainText(f"{flow_value:.1f}")

    @Slot(bool)
    def _on_process_status_changed(self, running: bool):
        self.ui.ch2_Start_button.setEnabled(not running)
        self.ui.ch2_Stop_button.setEnabled(True)

    # ------------------------------------------------------------------
    # 파일 로딩 / 파라미터 UI 반영
    # ------------------------------------------------------------------
    @Slot()
    @Slot(bool)
    def on_process_list_button_clicked(self, _checked: bool=False):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "프로세스 리스트 파일 선택", "", "CSV Files (*.csv);;All Files (*)"
        )
        if not file_path:
            self.append_log("File", "파일 선택이 취소되었습니다.")
            return

        self.append_log("File", f"선택된 파일: {file_path}")
        try:
            with open(file_path, mode='r', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)
                self.process_queue = []
                self.current_process_index = -1
                for row in reader:
                    row['Process_name'] = row.get('#', f'공정 {len(self.process_queue) + 1}')
                    self.process_queue.append(row)

                if not self.process_queue:
                    self.append_log("File", "파일에 처리할 공정이 없습니다.")
                    return
                self.append_log("File", f"총 {len(self.process_queue)}개의 공정을 파일에서 읽었습니다.")
                self._update_ui_from_params(self.process_queue[0])
        except Exception as e:
            self.append_log("File", f"파일 처리 중 오류 발생: {e}")

    def _update_ui_from_params(self, params: dict):
        if self.process_queue:
            total = len(self.process_queue)
            current = self.current_process_index + 1
            progress_text = f"자동 공정 ({current}/{total}): '{params.get('Process_name', '이름없음')}' 준비 중..."
            self.append_log("UI", progress_text)
        else:
            self.append_log("UI", f"단일 공정 '{params.get('process_note', '이름없음')}'의 파라미터로 UI를 업데이트합니다.")

        self.append_log("UI", f"다음 공정 '{params.get('Process_name','')}'의 파라미터로 UI를 업데이트합니다.")

        # CH2 페이지 위젯 반영
        self.ui.ch2_dcPower_edit.setPlainText(str(params.get('dc_power', '0')))

        self.ui.ch2_rfPulsePower_checkbox.setChecked(params.get('use_rf_pulse_power', 'F') == 'T')
        self.ui.ch2_rfPulsePower_edit.setPlainText(str(params.get('rf_pulse_power', '0')))

        freq_raw = str(params.get('rf_pulse_freq', '')).strip()
        duty_raw = str(params.get('rf_pulse_duty_cycle', '')).strip()
        self.ui.ch2_rfPulseFreq_edit.setPlainText('' if freq_raw in ('', '0') else freq_raw)
        self.ui.ch2_rfPulseDutyCycle_edit.setPlainText('' if duty_raw in ('', '0') else duty_raw)

        # 시간/압력/가스
        self.ui.ch2_processTime_edit.setPlainText(str(params.get('process_time', '0')))
        self.ui.ch2_integrationTime_edit.setPlainText(str(params.get('integration_time', '60')))
        self.ui.ch2_arFlow_edit.setPlainText(str(params.get('Ar_flow', '0')))
        self.ui.ch2_o2Flow_edit.setPlainText(str(params.get('O2_flow', '0')))
        self.ui.ch2_n2Flow_edit.setPlainText(str(params.get('N2_flow', '0')))
        self.ui.ch2_workingPressure_edit.setPlainText(str(params.get('working_pressure', '0')))
        self.ui.ch2_basePressure_edit.setPlainText(str(params.get('base_pressure', '0')))
        self.ui.ch2_shutterDelay_edit.setPlainText(str(params.get('shutter_delay', '0')))

        # 체크박스
        self.ui.ch2_G1_checkbox.setChecked(params.get('gun1', 'F') == 'T')
        self.ui.ch2_G2_checkbox.setChecked(params.get('gun2', 'F') == 'T')
        self.ui.ch3_G3_checkbox.setChecked(params.get('gun3', 'F') == 'T')  # UI 이름 그대로 사용
        self.ui.ch2_Ar_checkbox.setChecked(params.get('Ar', 'F') == 'T')
        self.ui.ch2_O2_checkbox.setChecked(params.get('O2', 'F') == 'T')
        self.ui.ch2_N2_checkbox.setChecked(params.get('N2', 'F') == 'T')
        self.ui.ch2_mainShutter_checkbox.setChecked(params.get('main_shutter', 'F') == 'T')
        self.ui.ch2_dcPower_checkbox.setChecked(params.get('use_dc_power', 'F') == 'T')
        self.ui.ch2_powerSelect_checkbox.setChecked(params.get('power_select', 'F') == 'T')

        # 타겟명
        self.ui.ch2_g1Target_name.setPlainText(str(params.get('G1 Target', '')).strip())
        self.ui.ch2_g2Target_name.setPlainText(str(params.get('G2 Target', '')).strip())
        self.ui.ch2_g3Target_name.setPlainText(str(params.get('G3 Target', '')).strip())

    # ------------------------------------------------------------------
    # 자동 시퀀스 진행
    # ------------------------------------------------------------------
    @Slot(bool)
    def _start_next_process_from_queue(self, was_successful: bool):
        if self.process_controller.is_running and self.current_process_index > -1:
            self.append_log("MAIN", "경고: 다음 공정 자동 전환 시점에 이미 다른 공정이 실행 중입니다.")
            return

        if not was_successful:
            self.append_log("MAIN", "이전 공정이 실패하여 자동 시퀀스를 중단합니다.")
            self._clear_queue_and_reset_ui()
            return

        self.current_process_index += 1
        if self.current_process_index < len(self.process_queue):
            params = self.process_queue[self.current_process_index]
            self._update_ui_from_params(params)
            if self._try_handle_delay_step(params):   # delay 단계면 대기만 수행
                return
            QTimer.singleShot(100, lambda p=params: self._safe_start_process(self._normalize_params_for_process(p)))
        else:
            self.append_log("MAIN", "모든 공정이 완료되었습니다.")
            self._clear_queue_and_reset_ui()

    def _safe_start_process(self, params: dict):
        if self.process_controller.is_running:
            self.append_log("MAIN", "경고: 이미 다른 공정이 실행 중이므로 새 공정을 시작하지 않습니다.")
            return

        # (비동기 장치는 내부에서 자동 연결/재연결 → 별도 connect 체크 불필요)
        try:
            self.process_controller.start_process(params)
        except Exception as e:
            self.append_log("MAIN", f"오류: '{params.get('Process_name', '알 수 없는')} 공정' 시작에 실패했습니다. ({e})")
            self._start_next_process_from_queue(False)

    # ------------------------------------------------------------------
    # 단일 실행
    # ------------------------------------------------------------------
    @Slot()
    @Slot(bool)
    def on_start_button_clicked(self, _checked: bool=False):
        if self.process_controller.is_running:
            QMessageBox.warning(self, "실행 오류", "현재 다른 공정이 실행 중입니다.")
            return

        # 자동 시퀀스
        if self.process_queue:
            self.append_log("MAIN", "입력받은 파일로 자동 공정 시퀀스를 시작합니다.")
            self.current_process_index = -1
            self._start_next_process_from_queue(True)
            return

        # 단일 실행
        try:
            base_pressure = float(self.ui.ch2_basePressure_edit.toPlainText() or 1e-5)
            integration_time = int(self.ui.ch2_integrationTime_edit.toPlainText() or 60)
            working_pressure = float(self.ui.ch2_workingPressure_edit.toPlainText() or 0.0)
            shutter_delay = float(self.ui.ch2_shutterDelay_edit.toPlainText() or 0.0)
            process_time = float(self.ui.ch2_processTime_edit.toPlainText() or 0.0)
        except ValueError:
            self.append_log("UI", "오류: 값 입력란을 확인해주세요.")
            return

        vals = self._validate_single_run_inputs()
        if vals is None:
            return

        params = {
            "base_pressure": base_pressure,
            "integration_time": integration_time,
            "working_pressure": working_pressure,
            "shutter_delay": shutter_delay,
            "process_time": process_time,
            "process_note": "Single run",
            **vals,
        }
        # DataLogger 원본 헤더 동기화
        params["G1 Target"] = vals.get("G1_target_name", "")
        params["G2 Target"] = vals.get("G2_target_name", "")
        params["G3 Target"] = vals.get("G3_target_name", "")

        self.append_log("MAIN", "입력 검증 통과 → 공정 시작")
        self.process_controller.start_process(params)

    # ------------------------------------------------------------------
    # STOP/종료 (단일 경로)
    # ------------------------------------------------------------------
    @Slot()
    def on_stop_button_clicked(self, _checked: bool=False):
        self.request_stop_all(user_initiated=True)

    def request_stop_all(self, user_initiated: bool):
        """
        STOP 버튼/종료 경로에서 호출되는 단일 정지 경로.
        - 공정 정상 정지 요청
        - delay 타이머 취소
        - 자동 큐 초기화(사용자 중단인 경우 메시지)
        """
        # 1) 정상 정지 요청
        try:
            self.process_controller.request_stop()
        except Exception:
            pass

        # 2) 대기 타이머 취소
        self._cancel_delay_timer()

        # 3) 자동 큐 중단/초기화
        if self.process_queue:
            if user_initiated:
                self.append_log("MAIN", "자동 시퀀스가 사용자에 의해 중단되었습니다.")
            self._clear_queue_and_reset_ui()

    # ------------------------------------------------------------------
    # 로그
    # ------------------------------------------------------------------
    @Slot(str, str)
    def append_log(self, source: str, msg: str):
        now_ui   = datetime.now().strftime("%H:%M:%S")
        now_file = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._log_ui_buf.append(f"[{now_ui}] [{source}] {msg}")
        self._log_file_buf.append(f"[{now_file}] [{source}] {msg}\n")

    def _flush_logs(self):
        # UI
        if self._log_ui_buf:
            block = "\n".join(self._log_ui_buf) + "\n"
            self._log_ui_buf.clear()
            cursor = self.ui.ch2_logMessage_edit.textCursor()
            cursor.movePosition(cursor.End)
            self.ui.ch2_logMessage_edit.setTextCursor(cursor)
            self.ui.ch2_logMessage_edit.insertPlainText(block)
        # 파일
        if self._log_file_buf:
            try:
                with open("log.txt", "a", encoding="utf-8") as f:
                    f.writelines(self._log_file_buf)
            except Exception as e:
                self.ui.ch2_logMessage_edit.appendPlainText(f"[Logger] 파일 로그 실패: {e}")
            finally:
                self._log_file_buf.clear()

    # ------------------------------------------------------------------
    # 폴링/상태
    # ------------------------------------------------------------------
    @Slot(dict)
    def _apply_polling_targets(self, targets: dict):
        # 기존의 Qt 시그널 대신 비동기 컨트롤러 메서드를 직접 호출
        asyncio.create_task(self.mfc.set_process_status(bool(targets.get('mfc', False))))
        asyncio.create_task(self.faduino.set_process_status(bool(targets.get('faduino', False))))
        # RF-Pulse는 컨트롤러 내부에서 자체 관리

    @Slot(str)
    def on_update_process_state(self, message: str):
        self.ui.ch2_processState_edit.setPlainText(message)

    # ------------------------------------------------------------------
    # 기본 UI값/리셋
    # ------------------------------------------------------------------
    def _set_default_ui_values(self):
        self.ui.ch2_basePressure_edit.setPlainText("9e-6")
        self.ui.ch2_integrationTime_edit.setPlainText("60")
        self.ui.ch2_workingPressure_edit.setPlainText("2")
        self.ui.ch2_processTime_edit.setPlainText("1")
        self.ui.ch2_shutterDelay_edit.setPlainText("1")
        self.ui.ch2_arFlow_edit.setPlainText("20")
        self.ui.ch2_o2Flow_edit.setPlainText("0")
        self.ui.ch2_n2Flow_edit.setPlainText("0")
        self.ui.ch2_dcPower_edit.setPlainText("100")
        self.ui.ch2_rfPulsePower_checkbox.setChecked(False)
        self.ui.ch2_rfPulsePower_edit.setPlainText("100")
        self.ui.ch2_rfPulseFreq_edit.setPlainText("")
        self.ui.ch2_rfPulseDutyCycle_edit.setPlainText("")

    def _reset_ui_after_process(self):
        self._set_default_ui_values()
        for cb in [
            self.ui.ch2_G1_checkbox, self.ui.ch2_G2_checkbox, self.ui.ch3_G3_checkbox,
            self.ui.ch2_Ar_checkbox, self.ui.ch2_O2_checkbox, self.ui.ch2_N2_checkbox,
            self.ui.ch2_mainShutter_checkbox, self.ui.ch2_rfPulsePower_checkbox,
            self.ui.ch2_dcPower_checkbox, self.ui.ch2_powerSelect_checkbox
        ]:
            cb.setChecked(False)
        self.ui.ch2_processState_edit.setPlainText("대기 중")
        self.ui.ch2_Power_edit.setPlainText("0.00")
        self.ui.ch2_Voltage_edit.setPlainText("0.00")
        self.ui.ch2_Current_edit.setPlainText("0.00")
        self.ui.ch2_forP_edit.setPlainText("0.0")
        self.ui.ch2_refP_edit.setPlainText("0.0")
        self.ui.ch2_rfPulsePower_edit.setPlainText("0")
        self.ui.ch2_rfPulseFreq_edit.setPlainText("")
        self.ui.ch2_rfPulseDutyCycle_edit.setPlainText("")

    def _clear_queue_and_reset_ui(self):
        self.process_queue = []
        self.current_process_index = -1
        self._reset_ui_after_process()

    # ------------------------------------------------------------------
    # 종료/정리(단일 경로)
    # ------------------------------------------------------------------
    def closeEvent(self, event):
        self.append_log("MAIN", "프로그램 창 닫힘 → 종료 절차 시작...")
        self._shutdown_once("closeEvent")
        event.accept()
        super().closeEvent(event)

    def _shutdown_once(self, reason: str):
        if self._shutdown_called:
            return
        self._shutdown_called = True
        self.append_log("MAIN", f"종료 시퀀스({reason}) 시작")

        # 1) Stop 요청(큐/타이머 포함)
        self.request_stop_all(user_initiated=False)

        # 2) 장치 정리(가능하면 비동기 cleanup 호출)
        async def _cleanup():
            try:
                await asyncio.gather(
                    self.rf_pulse.cleanup(),
                    self.rf_power.cleanup(),
                    self.dc_power.cleanup(),
                    self.mfc.cleanup(),
                    self.ig.cleanup(),
                    self.faduino.cleanup(),
                    return_exceptions=True
                )
            except Exception:
                pass

        try:
            asyncio.create_task(_cleanup())
        except Exception:
            pass

        # 3) Chat Notifier 정지
        try:
            if self.chat_notifier:
                self.chat_notifier.shutdown()
        except Exception:
            pass

        # 4) 백그라운드 태스크 취소
        for t in getattr(self, "_bg_tasks", []):
            try:
                t.cancel()
            except Exception:
                pass

        # 5) Qt 앱 종료
        QTimer.singleShot(0, QCoreApplication.quit)
        self.append_log("MAIN", "종료 시퀀스 완료")

    # ------------------------------------------------------------------
    # 입력 검증 / 파라미터 정규화 / delay 처리
    # ------------------------------------------------------------------
    def _validate_single_run_inputs(self) -> dict | None:
        # 건 선택
        use_g1 = self.ui.ch2_G1_checkbox.isChecked()
        use_g2 = self.ui.ch2_G2_checkbox.isChecked()
        use_g3 = self.ui.ch3_G3_checkbox.isChecked()
        checked_count = int(use_g1) + int(use_g2) + int(use_g3)
        if checked_count == 0 or checked_count == 3:
            msg_box = QMessageBox(self)
            msg_box.setIcon(QMessageBox.Icon.Warning)
            msg_box.setWindowTitle("선택 오류")
            msg_box.setText("Gun 선택 개수를 확인해주세요.")
            msg_box.setInformativeText("G1, G2, G3 중 1개 또는 2개만 선택해야 합니다.")
            msg_box.setStandardButtons(QMessageBox.StandardButton.Ok)
            msg_box.exec()
            return None

        g1_name = self.ui.ch2_g1Target_name.toPlainText().strip()
        g2_name = self.ui.ch2_g2Target_name.toPlainText().strip()
        g3_name = self.ui.ch2_g3Target_name.toPlainText().strip()
        if use_g1 and not g1_name:
            QMessageBox.warning(self, "입력값 확인", "G1 타겟 이름이 비어 있습니다."); return None
        if use_g2 and not g2_name:
            QMessageBox.warning(self, "입력값 확인", "G2 타겟 이름이 비어 있습니다."); return None
        if use_g3 and not g3_name:
            QMessageBox.warning(self, "입력값 확인", "G3 타겟 이름이 비어 있습니다."); return None

        # 가스
        use_ar = self.ui.ch2_Ar_checkbox.isChecked()
        use_o2 = self.ui.ch2_O2_checkbox.isChecked()
        use_n2 = self.ui.ch2_N2_checkbox.isChecked()
        if not (use_ar or use_o2 or use_n2):
            QMessageBox.warning(self, "선택 오류", "가스를 하나 이상 선택해야 합니다."); return None

        if use_ar:
            txt = self.ui.ch2_arFlow_edit.toPlainText().strip()
            if not txt: QMessageBox.warning(self, "입력값 확인", "Ar 유량을 입력하세요."); return None
            try:
                ar_flow = float(txt)
                if ar_flow <= 0: QMessageBox.warning(self, "입력값 확인", "Ar 유량은 0보다 커야 합니다."); return None
            except ValueError:
                QMessageBox.warning(self, "입력값 확인", "Ar 유량이 올바른 수치가 아닙니다."); return None
        else:
            ar_flow = 0.0

        if use_o2:
            txt = self.ui.ch2_o2Flow_edit.toPlainText().strip()
            if not txt: QMessageBox.warning(self, "입력값 확인", "O2 유량을 입력하세요."); return None
            try:
                o2_flow = float(txt)
                if o2_flow <= 0: QMessageBox.warning(self, "입력값 확인", "O2 유량은 0보다 커야 합니다."); return None
            except ValueError:
                QMessageBox.warning(self, "입력값 확인", "O2 유량이 올바른 수치가 아닙니다."); return None
        else:
            o2_flow = 0.0

        if use_n2:
            txt = self.ui.ch2_n2Flow_edit.toPlainText().strip()
            if not txt: QMessageBox.warning(self, "입력값 확인", "N2 유량을 입력하세요."); return None
            try:
                n2_flow = float(txt)
                if n2_flow <= 0: QMessageBox.warning(self, "입력값 확인", "N2 유량은 0보다 커야 합니다."); return None
            except ValueError:
                QMessageBox.warning(self, "입력값 확인", "N2 유량이 올바른 수치가 아닙니다."); return None
        else:
            n2_flow = 0.0

        # 전원 선택 (CH2 페이지: RF Pulse, DC)
        use_rf_pulse = self.ui.ch2_rfPulsePower_checkbox.isChecked()
        use_dc = self.ui.ch2_dcPower_checkbox.isChecked()

        if not (use_rf_pulse or use_dc):
            QMessageBox.warning(self, "선택 오류", "RF Pulse, DC 파워 중 하나 이상을 반드시 선택해야 합니다.")
            return None

        rf_pulse_power = 0.0
        rf_pulse_freq = None
        rf_pulse_duty = None

        if use_rf_pulse:
            txtp = self.ui.ch2_rfPulsePower_edit.toPlainText().strip()
            if not txtp:
                QMessageBox.warning(self, "입력값 확인", "RF Pulse Target Power(W)를 입력하세요.")
                return None
            try:
                rf_pulse_power = float(txtp)
                if rf_pulse_power <= 0:
                    QMessageBox.warning(self, "입력값 확인", "RF Pulse Target Power(W)는 0보다 커야 합니다.")
                    return None
            except ValueError:
                QMessageBox.warning(self, "입력값 확인", "RF Pulse Target Power(W)가 올바른 수치가 아닙니다.")
                return None

            txtf = self.ui.ch2_rfPulseFreq_edit.toPlainText().strip()
            if txtf:
                try:
                    rf_pulse_freq = int(float(txtf))
                    if rf_pulse_freq < 1 or rf_pulse_freq > 100000:
                        QMessageBox.warning(self, "입력값 확인", "RF Pulse Freq(Hz)는 1..100000 범위로 입력하세요.")
                        return None
                except ValueError:
                    QMessageBox.warning(self, "입력값 확인", "RF Pulse Freq(Hz)가 올바른 수치가 아닙니다.")
                    return None

            txtd = self.ui.ch2_rfPulseDutyCycle_edit.toPlainText().strip()
            if txtd:
                try:
                    rf_pulse_duty = int(float(txtd))
                    if rf_pulse_duty < 1 or rf_pulse_duty > 99:
                        QMessageBox.warning(self, "입력값 확인", "RF Pulse Duty(%)는 1..99 범위로 입력하세요.")
                        return None
                except ValueError:
                    QMessageBox.warning(self, "입력값 확인", "RF Pulse Duty(%)가 올바른 수치가 아닙니다.")
                    return None

        if use_dc:
            txt = self.ui.ch2_dcPower_edit.toPlainText().strip()
            if not txt: QMessageBox.warning(self, "입력값 확인", "DC 파워(W)를 입력하세요."); return None
            try:
                dc_power = float(txt)
                if dc_power <= 0: QMessageBox.warning(self, "입력값 확인", "DC 파워(W)는 0보다 커야 합니다."); return None
            except ValueError:
                QMessageBox.warning(self, "입력값 확인", "DC 파워(W)가 올바른 수치가 아닙니다."); return None
        else:
            dc_power = 0.0

        return {
            "use_ms": self.ui.ch2_mainShutter_checkbox.isChecked(),
            "use_g1": use_g1, "use_g2": use_g2, "use_g3": use_g3,
            "use_ar": use_ar, "use_o2": use_o2, "use_n2": use_n2,
            "ar_flow": ar_flow, "o2_flow": o2_flow, "n2_flow": n2_flow,
            "use_rf_power": False,  # CH2 단일 실행 UI에는 RF 연속파 항목이 없음
            "use_rf_pulse": use_rf_pulse, "use_dc_power": use_dc,
            "rf_power": 0.0, "rf_pulse_power": rf_pulse_power, "dc_power": dc_power,
            "rf_pulse_freq": rf_pulse_freq, "rf_pulse_duty": rf_pulse_duty,
            "G1_target_name": g1_name, "G2_target_name": g2_name, "G3_target_name": g3_name,
            "use_power_select": self.ui.ch2_powerSelect_checkbox.isChecked(),
        }

    def _normalize_params_for_process(self, raw: dict) -> dict:
        def tf(v): return str(v).strip().upper() in ("T","TRUE","1","Y","YES")
        def fget(key, default="0"):
            try: return float(str(raw.get(key, default)).strip())
            except Exception: return float(default)
        def iget(key, default="0"):
            try: return int(float(str(raw.get(key, default)).strip()))
            except Exception: return int(default)
        def iget_opt(key):
            s = str(raw.get(key, '')).strip()
            return int(float(s)) if s != '' else None

        g1t = str(raw.get("G1 Target", "")).strip()
        g2t = str(raw.get("G2 Target", "")).strip()
        g3t = str(raw.get("G3 Target", "")).strip()

        return {
            "base_pressure":     fget("base_pressure", "1e-5"),
            "working_pressure":  fget("working_pressure", "0"),
            "process_time":      fget("process_time", "0"),
            "shutter_delay":     fget("shutter_delay", "0"),
            "integration_time":  iget("integration_time", "60"),
            "dc_power":          fget("dc_power", "0"),
            "rf_power":          fget("rf_power", "0"),

            "use_rf_pulse":      tf(raw.get("use_rf_pulse_power", raw.get("use_rf_pulse", "F"))),
            "rf_pulse_power":    fget("rf_pulse_power", "0"),
            "rf_pulse_freq":     iget_opt("rf_pulse_freq"),
            "rf_pulse_duty":     iget_opt("rf_pulse_duty_cycle"),

            "use_rf_power":      tf(raw.get("use_rf_power", "F")),
            "use_dc_power":      tf(raw.get("use_dc_power", "F")),

            "use_ar":            tf(raw.get("Ar", "F")),
            "use_o2":            tf(raw.get("O2", "F")),
            "use_n2":            tf(raw.get("N2", "F")),

            "ar_flow":           fget("Ar_flow", "0"),
            "o2_flow":           fget("O2_flow", "0"),
            "n2_flow":           fget("N2_flow", "0"),

            "use_g1":            tf(raw.get("gun1", "F")),
            "use_g2":            tf(raw.get("gun2", "F")),
            "use_g3":            tf(raw.get("gun3", "F")),
            "use_ms":            tf(raw.get("main_shutter", "F")),
            "process_note":      raw.get("Process_name", raw.get("process_note", "")),

            "G1_target_name":    g1t,
            "G2_target_name":    g2t,
            "G3_target_name":    g3t,

            "G1 Target":         g1t,
            "G2 Target":         g2t,
            "G3 Target":         g3t,

            "use_power_select":  tf(raw.get("power_select", "F")),
        }

    # --- delay 단계 처리 ------------------------------------------------
    def _cancel_delay_timer(self):
        if getattr(self, "_delay_timer", None):
            try:
                self._delay_timer.stop()
                self._delay_timer.deleteLater()
            except Exception:
                pass
            self._delay_timer = None

    def _on_delay_step_done(self, step_name: str):
        self._delay_timer = None
        self.append_log("Process", f"'{step_name}' 지연 완료 → 다음 공정으로 진행")
        self._start_next_process_from_queue(True)

    def _try_handle_delay_step(self, params: dict) -> bool:
        name = str(params.get("Process_name") or params.get("process_note", "")).strip()
        if not name:
            return False
        m = re.match(r"^\s*delay\s*(\d+)\s*([smhd]?)\s*$", name, re.IGNORECASE)
        if not m:
            return False

        amount = int(m.group(1))
        unit = (m.group(2) or "m").lower()
        factor = {"s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}[unit]
        duration_ms = amount * factor

        unit_txt = {"s": "초", "m": "분", "h": "시간", "d": "일"}[unit]
        self.append_log("Process", f"'{name}' 단계 감지: {amount}{unit_txt} 대기 시작")
        self.ui.ch2_processState_edit.setPlainText(f"지연 대기 중: {amount}{unit_txt}")

        self._cancel_delay_timer()
        self._delay_timer = QTimer(self)
        self._delay_timer.setSingleShot(True)
        self._delay_timer.timeout.connect(lambda: self._on_delay_step_done(name))
        self._delay_timer.start(duration_ms)
        return True


def _escape(s: str) -> str:
    try:
        return s.replace("\n", "\\n")
    except Exception:
        return s


if __name__ == "__main__":
    app = QApplication(sys.argv)
    loop = QEventLoop(app)          # Qt + asyncio 루프 통합
    asyncio.set_event_loop(loop)
    w = MainWindow()
    w.show()
    with loop:
        loop.run_forever()

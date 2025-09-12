# -*- coding: utf-8 -*-
import sys
import re
import csv
import asyncio
from typing import Optional
from datetime import datetime

from PySide6.QtWidgets import QApplication, QWidget, QMessageBox, QFileDialog, QPlainTextEdit
from PySide6.QtCore import QCoreApplication, Qt, QTimer, QThread, Slot, Signal
from qasync import QEventLoop

# === controller import ===
from ui import Ui_Form
from controller.graph_controller import GraphController

# ✅ 실제 장비 모듈은 루트에 고정된 파일명을 그대로 사용 (try 없이)
from device.faduino import FaduinoController
from device.ig import IGController
from device.rga import RGAController
from device.mfc import MFCController
from device.oes import OESController
from device.dc_power import DCPowerController
from device.rf_power import RFPowerController
from device.rf_pulse import RFPulseController

# ✅ 프로세스 컨트롤러는 process_ch2.py 사용
from process_ch2 import ProcessController

from controller.data_logger import DataLogger
from controller.chat_notifier import ChatNotifier
from lib.config_ch2 import CHAT_WEBHOOK_URL, ENABLE_CHAT_NOTIFY


class MainWindow(QWidget):
    # UI → 장치(워커) 요청 신호들
    request_faduino_connect = Signal()
    request_mfc_connect     = Signal()
    request_ig_connect      = Signal()
    request_rfpulse_connect = Signal()
    request_oes_initialize  = Signal()
    request_faduino_cleanup = Signal()
    request_mfc_cleanup     = Signal()
    request_ig_cleanup      = Signal()
    request_oes_cleanup     = Signal()
    request_rfpulse_cleanup = Signal()

    mfc_polling_request = Signal(bool)
    faduino_polling_request = Signal(bool)

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

        # === 1. 메인 스레드 컨트롤러 ===
        self.graph_controller = GraphController(self.ui.ch2_rgaGraph_widget, self.ui.ch2_oesGraph_widget)
        self.dc_power_controller = DCPowerController()
        self.rf_power_controller = RFPowerController()
        self.rf_pulse_controller = RFPulseController()

        # === 2. 워커 스레드 및 장치 컨트롤러 ===
        # Faduino
        self.faduino_thread = QThread(self); self.faduino_thread.setObjectName("FaduinoThread")
        self.faduino_controller = FaduinoController(); self.faduino_controller.moveToThread(self.faduino_thread)

        # MFC
        self.mfc_thread = QThread(self); self.mfc_thread.setObjectName("MFCThread")
        self.mfc_controller = MFCController(); self.mfc_controller.moveToThread(self.mfc_thread)

        # OES
        self.oes_thread = QThread(self); self.oes_thread.setObjectName("OESThread")
        self.oes_controller = OESController(); self.oes_controller.moveToThread(self.oes_thread)

        # IG
        self.ig_thread = QThread(self); self.ig_thread.setObjectName("IGThread")
        self.ig_controller = IGController(); self.ig_controller.moveToThread(self.ig_thread)

        # RGA
        self.rga_thread = QThread(self); self.rga_thread.setObjectName("RGAThread")
        self.rga_controller = RGAController(); self.rga_controller.moveToThread(self.rga_thread)

        # DataLogger
        self.data_logger_thread = QThread(self); self.data_logger_thread.setObjectName("DataLoggerThread")
        self.data_logger = DataLogger(); self.data_logger.moveToThread(self.data_logger_thread)

        # RF Pulse
        self.rf_pulse_thread = QThread(self); self.rf_pulse_thread.setObjectName("RFPulseThread")
        self.rf_pulse_controller.moveToThread(self.rf_pulse_thread)

        # === 3. 공정 감독관 ===
        self.process_controller = ProcessController()

        # === 4. Google Chat 알림(옵션) ===
        self.chat_notifier = ChatNotifier(CHAT_WEBHOOK_URL) if ENABLE_CHAT_NOTIFY else None
        if self.chat_notifier:
            self.chat_notifier.start()

        # === 5. 신호-슬롯 ===
        self._connect_signals()

        # === 6. 워커 스레드 시작 ===
        self.faduino_thread.start()
        self.mfc_thread.start()
        self.oes_thread.start()
        self.ig_thread.start()
        self.rga_thread.start()
        self.data_logger_thread.start()
        self.rf_pulse_thread.start()

        # 공정 완료 → 다음 공정
        self.process_controller.process_finished.connect(self._start_next_process_from_queue)

        # 로그 배치 flush
        self.ui.ch2_logMessage_edit.setMaximumBlockCount(2000)
        self._log_ui_buf = []
        self._log_file_buf = []
        self._log_flush_timer = QTimer(self)
        self._log_flush_timer.setInterval(100)
        self._log_flush_timer.timeout.connect(self._flush_logs)
        self._log_flush_timer.start()

        # 종료 훅
        app = QCoreApplication.instance()
        if app is not None:
            app.aboutToQuit.connect(lambda: self._shutdown_once("aboutToQuit"))

        # 내비게이션 버튼(통합 UI 대응: 없으면 무시)
        self._connect_optional_navigation()

        # 중복 실행 방지 플래그
        self._shutdown_called = False

    # ------------------------------------------------------------------
    # 연결 & 신호
    # ------------------------------------------------------------------
    def _connect_optional_navigation(self):
        """통합 UI에서 페이지 전환 버튼이 존재하는 경우에만 안전하게 연결"""
        def bind(name_btn, target_page_attr):
            btn = getattr(self.ui, name_btn, None)
            page = getattr(self.ui, target_page_attr, None)
            stacked = getattr(self.ui, "stackedWidget", None)
            if btn and page and stacked:
                btn.clicked.connect(lambda _=False, w=page: stacked.setCurrentWidget(w))
        # PlasmaCleaning <-> CH1/CH2
        bind("pc_btnGoCh1", "page")
        bind("pc_btnGoCh2", "page_2")
        bind("ch1_btnGoPC", "page_3")
        bind("ch1_btnGoCh2", "page_2")
        bind("ch2_btnGoPC", "page_3")
        bind("ch2_btnGoCh1", "page")

    def _connect_signals(self):
        # === DataLogger ===
        self.process_controller.process_started.connect(self.data_logger.start_new_log_session)
        self.process_controller.process_finished.connect(self.data_logger.finalize_and_write_log)
        self.ig_controller.pressure_update.connect(self.data_logger.log_ig_pressure)
        self.faduino_controller.dc_power_updated.connect(self.data_logger.log_dc_power)
        self.faduino_controller.rf_power_updated.connect(self.data_logger.log_rf_power)
        self.rf_pulse_controller.update_rf_status_display.connect(self.data_logger.log_rfpulse_power)
        self.mfc_controller.update_flow.connect(self.data_logger.log_mfc_flow)
        self.mfc_controller.update_pressure.connect(self.data_logger.log_mfc_pressure)

        # 공정 시작 시 그래프 초기화
        self.process_controller.process_started.connect(
            lambda *args: self.graph_controller.reset(),
            type=Qt.ConnectionType.QueuedConnection
        )

        # === 로그/상태 ===
        for src in (self.faduino_controller, self.mfc_controller,
                    self.oes_controller, self.ig_controller, self.rga_controller):
            src.status_message.connect(self.append_log)
        self.dc_power_controller.status_message.connect(self.append_log)
        self.rf_power_controller.status_message.connect(self.append_log)
        self.rf_pulse_controller.status_message.connect(self.append_log)
        self.process_controller.log_message.connect(self.append_log)
        self.process_controller.update_process_state.connect(self.on_update_process_state)

        # === ProcessController -> 장치 ===
        self.process_controller.update_faduino_port.connect(
            self.faduino_controller.handle_named_command,
            type=Qt.ConnectionType.QueuedConnection
        )
        self.process_controller.mfc_command_requested.connect(
            self.mfc_controller.handle_command,
            type=Qt.ConnectionType.QueuedConnection
        )
        self.process_controller.oes_command_requested.connect(
            self.oes_controller.run_measurement,
            type=Qt.ConnectionType.QueuedConnection
        )

        self.process_controller.dc_power_command_requested.connect(
            self.dc_power_controller.start_process,
            type=Qt.ConnectionType.QueuedConnection
        )
        self.process_controller.dc_power_stop_requested.connect(
            self.dc_power_controller.stop_process,
            type=Qt.ConnectionType.QueuedConnection
        )

        self.process_controller.rf_power_command_requested.connect(
            self.rf_power_controller.start_process,
            type=Qt.ConnectionType.QueuedConnection
        )
        self.process_controller.rf_power_stop_requested.connect(
            self.rf_power_controller.stop_process,
            type=Qt.ConnectionType.QueuedConnection
        )

        self.process_controller.rf_pulse_command_requested.connect(
            self.rf_pulse_controller.start_pulse_process,
            type=Qt.ConnectionType.QueuedConnection
        )
        self.process_controller.rf_pulse_stop_requested.connect(
            self.rf_pulse_controller.stop_process,
            type=Qt.ConnectionType.QueuedConnection
        )

        self.process_controller.rga_external_scan_requested.connect(
            self.rga_controller.execute_external_scan,
            type=Qt.ConnectionType.QueuedConnection
        )
        self.process_controller.ig_command_requested.connect(
            self.ig_controller.start_wait_for_pressure,
            type=Qt.ConnectionType.QueuedConnection
        )

        # === 명령 완료/실패 ===
        self.rga_controller.scan_finished.connect(self.process_controller.on_rga_finished,
            type=Qt.ConnectionType.QueuedConnection)
        self.rga_controller.scan_failed.connect(self.process_controller.on_rga_failed,
            type=Qt.ConnectionType.QueuedConnection)
        self.oes_controller.oes_finished.connect(self.process_controller.on_oes_ok,
            type=Qt.ConnectionType.QueuedConnection)
        self.oes_controller.oes_failed.connect(self.process_controller.on_oes_failed,
            type=Qt.ConnectionType.QueuedConnection)
        self.ig_controller.base_pressure_reached.connect(self.process_controller.on_ig_ok,
            type=Qt.ConnectionType.QueuedConnection)
        self.ig_controller.base_pressure_failed.connect(self.process_controller.on_ig_failed,
            type=Qt.ConnectionType.QueuedConnection)
        self.mfc_controller.command_confirmed.connect(self.process_controller.on_mfc_confirmed,
            type=Qt.ConnectionType.QueuedConnection)
        self.mfc_controller.command_failed.connect(self.process_controller.on_mfc_failed,
            type=Qt.ConnectionType.QueuedConnection)
        self.faduino_controller.command_confirmed.connect(self.process_controller.on_faduino_confirmed,
            type=Qt.ConnectionType.QueuedConnection)
        self.faduino_controller.command_failed.connect(self.process_controller.on_faduino_failed,
            type=Qt.ConnectionType.QueuedConnection)

        # DC
        self.dc_power_controller.target_reached.connect(self.process_controller.on_dc_target_reached,
            type=Qt.ConnectionType.QueuedConnection)
        self.dc_power_controller.power_off_finished.connect(self.process_controller.on_device_step_ok,
            type=Qt.ConnectionType.QueuedConnection)

        # RF Power
        self.rf_power_controller.target_reached.connect(self.process_controller.on_rf_target_reached,
            type=Qt.ConnectionType.QueuedConnection)
        self.rf_power_controller.target_failed.connect(
            lambda why: self.process_controller.on_step_failed("RF Power", why),
            type=Qt.ConnectionType.QueuedConnection
        )
        self.rf_power_controller.power_off_finished.connect(self.process_controller.on_device_step_ok,
            type=Qt.ConnectionType.QueuedConnection)

        # RF Pulse
        self.rf_pulse_controller.target_reached.connect(self.process_controller.on_rf_target_reached,
            type=Qt.ConnectionType.QueuedConnection)
        self.rf_pulse_controller.target_failed.connect(
            lambda why: self.process_controller.on_step_failed("RF Pulse", why),
            type=Qt.ConnectionType.QueuedConnection
        )
        self.rf_pulse_controller.power_off_finished.connect(self.process_controller.on_device_step_ok,
            type=Qt.ConnectionType.QueuedConnection)
        self.rf_pulse_controller.update_rf_status_display.connect(self.handle_rf_power_display,
            type=Qt.ConnectionType.QueuedConnection)

        # 폴링 on/off
        self.process_controller.set_polling_targets.connect(self._apply_polling_targets,
            type=Qt.ConnectionType.QueuedConnection)

        # 공정 종료 시 컨트롤러 내부 폴링 등 정리
        self.process_controller.process_finished.connect(self.faduino_controller.on_process_finished,
            type=Qt.ConnectionType.QueuedConnection)
        self.process_controller.process_finished.connect(self.mfc_controller.on_process_finished,
            type=Qt.ConnectionType.QueuedConnection)

        # === ✅ 장비 연결/정리 요청 신호들: 실제 슬롯에 연결 (루트 모듈 기준) ===
        self.request_faduino_connect.connect(self.faduino_controller.connect_faduino,
            type=Qt.ConnectionType.QueuedConnection)
        self.request_mfc_connect.connect(self.mfc_controller.connect_mfc,
            type=Qt.ConnectionType.QueuedConnection)
        self.request_ig_connect.connect(self.ig_controller.connect_ig,
            type=Qt.ConnectionType.QueuedConnection)
        self.request_oes_initialize.connect(self.oes_controller.initialize_device,
            type=Qt.ConnectionType.QueuedConnection)
        self.request_faduino_cleanup.connect(self.faduino_controller.cleanup,
            type=Qt.ConnectionType.QueuedConnection)
        self.request_mfc_cleanup.connect(self.mfc_controller.cleanup,
            type=Qt.ConnectionType.QueuedConnection)
        self.request_ig_cleanup.connect(self.ig_controller.cleanup,
            type=Qt.ConnectionType.QueuedConnection)
        self.request_oes_cleanup.connect(self.oes_controller.cleanup,
            type=Qt.ConnectionType.QueuedConnection)

        # === RFPulse 연결/정리 요청 신호 ===
        self.request_rfpulse_connect.connect(self.rf_pulse_controller.connect_rfpulse_device,
            type=Qt.ConnectionType.QueuedConnection)
        self.request_rfpulse_cleanup.connect(self.rf_pulse_controller.cleanup,
            type=Qt.ConnectionType.QueuedConnection)

        # === Google Chat 알림 ===
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
            self.rf_power_controller.target_failed.connect(
                lambda why: self.chat_notifier.notify_error_with_src("RF Power", why),
                type=Qt.ConnectionType.QueuedConnection
            )
            self.rf_pulse_controller.target_failed.connect(
                lambda why: self.chat_notifier.notify_error_with_src("RF Pulse", why),
                type=Qt.ConnectionType.QueuedConnection
            )
            self.mfc_controller.command_failed.connect(
                lambda cmd, why: self.chat_notifier.notify_error_with_src("MFC", f"{cmd}: {why}"),
                type=Qt.ConnectionType.QueuedConnection
            )
            self.faduino_controller.command_failed.connect(
                lambda cmd, why: self.chat_notifier.notify_error_with_src("Faduino", f"{cmd}: {why}"),
                type=Qt.ConnectionType.QueuedConnection
            )
            self.ig_controller.base_pressure_failed.connect(self.chat_notifier.notify_error,
                type=Qt.ConnectionType.QueuedConnection)
            self.oes_controller.oes_failed.connect(self.chat_notifier.notify_error,
                type=Qt.ConnectionType.QueuedConnection)
            self.rga_controller.scan_failed.connect(self.chat_notifier.notify_error,
                type=Qt.ConnectionType.QueuedConnection)

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

        if not self._check_and_connect_devices():
            self.append_log("MAIN", "장비 재확인 실패 → 자동 시퀀스를 중단합니다.")
            self._start_next_process_from_queue(False)
            return

        try:
            self.process_controller.start_process(params)
        except Exception as e:
            self.append_log("MAIN", f"오류: '{params.get('Process_name', '알 수 없는')} 공정' 시작에 실패했습니다. ({e})")
            self._start_next_process_from_queue(False)

    def _check_and_connect_devices(self):
        """워커 스레드 컨트롤러들이 지연 생성된 시리얼을 열도록 신호만 보낸다."""
        # Faduino
        if not getattr(self.faduino_controller, "serial_faduino", None) \
           or not self.faduino_controller.serial_faduino.isOpen():
            self.request_faduino_connect.emit()
        # MFC
        if not getattr(self.mfc_controller, "serial_mfc", None) \
           or not self.mfc_controller.serial_mfc.isOpen():
            self.request_mfc_connect.emit()
        # IG
        if not getattr(self.ig_controller, "serial_ig", None) \
           or not self.ig_controller.serial_ig.isOpen():
            self.request_ig_connect.emit()
        # OES (BlockingQueued 초기화)
        if getattr(self.oes_controller, "sChannel", -1) < 0:
            self.append_log("MAIN", "OES 초기화를 시도합니다...")
            self.request_oes_initialize.emit()
            if getattr(self.oes_controller, "sChannel", -1) < 0:
                self.append_log("MAIN", "OES 초기화 실패.")
                return False
        # RF Pulse (필요 시 지연 연결)
        if getattr(self.ui, "ch2_rfPulsePower_checkbox", None) and self.ui.ch2_rfPulsePower_checkbox.isChecked():
            self.append_log("MAIN", "RF Pulse 연결을 시도합니다…")
            self.request_rfpulse_connect.emit()

        self.append_log("MAIN", "모든 장비가 성공적으로 연결되었습니다.")
        return True

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

        if not self._check_and_connect_devices():
            QMessageBox.critical(self, "장비 연결 오류", "필수 장비 연결에 실패했습니다. 로그를 확인하세요.")
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
        STOP 버튼, 파일/프로그램 종료 경로에서 모두 호출되는 단일 정지 경로.
        - 공정 정상 정지 요청
        - delay 타이머 취소
        - 자동 큐 초기화(사용자 중단인 경우 메시지)
        """
        # 1) 정상 정지 요청 (장치 해제는 ProcessController/각 컨트롤러에서 수행됨)
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
        self.mfc_polling_request.emit(bool(targets.get('mfc', False)))
        self.faduino_polling_request.emit(bool(targets.get('faduino', False)))
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
        if getattr(self, "_shutdown_called", False):
            return
        self._shutdown_called = True
        self.append_log("MAIN", f"종료 시퀀스({_escape(reason)}) 시작")

        # 1) Stop 요청(큐/타이머 포함)
        self.request_stop_all(user_initiated=False)

        # 2) 장치 정리(각 워커 스레드에서 BlockingQueued로 실행됨)
        for sig in (self.request_faduino_cleanup, self.request_mfc_cleanup,
                    self.request_ig_cleanup, self.request_oes_cleanup,
                    self.request_rfpulse_cleanup):
            try:
                sig.emit()
            except Exception:
                pass

        # 3) Chat Notifier 정지
        try:
            if self.chat_notifier:
                self.chat_notifier.shutdown()
        except Exception:
            pass

        # 4) 스레드 종료 대기
        threads = [
            getattr(self, 'faduino_thread', None),
            getattr(self, 'mfc_thread', None),
            getattr(self, 'oes_thread', None),
            getattr(self, 'ig_thread', None),
            getattr(self, 'rga_thread', None),
            getattr(self, 'data_logger_thread', None),
            getattr(self, 'rf_pulse_thread', None),
        ]
        for th in threads:
            if th and th.isRunning():
                th.quit()
                th.wait()
                self.append_log("MAIN", f"{th.objectName() or type(th).__name__} 종료 완료.")

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

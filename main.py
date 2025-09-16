# -*- coding: utf-8 -*-
import sys
import re
import csv
import asyncio
from typing import Optional
from datetime import datetime
from pathlib import Path

from PySide6.QtWidgets import QApplication, QWidget, QMessageBox, QFileDialog, QPlainTextEdit, QStackedWidget
from PySide6.QtCore import QCoreApplication, Qt, QTimer, Slot
from PySide6.QtGui import QTextCursor
from qasync import QEventLoop

# === imports ===
from ui.main_window import Ui_Form
from controller.graph_controller import GraphController
from controller.data_logger import DataLogger
from controller.chat_notifier import ChatNotifier

# ✅ 실제 장비 모듈(비동기)
from device.faduino import AsyncFaduino
from device.ig import AsyncIG
from device.mfc import AsyncMFC
from device.rga import RGAAsync
from device.oes import OESAsync
from device.dc_power import DCPowerAsync
from device.rf_power import RFPowerAsync
from device.rf_pulse import RFPulseAsync

# ✅ CH2 공정 컨트롤러 (asyncio 순수 버전)
from controller.process_ch2 import ProcessController

from lib.config_ch2 import CHAT_WEBHOOK_URL, ENABLE_CHAT_NOTIFY, RGA_PROGRAM_PATH, RGA_CSV_PATH, BUTTON_TO_PIN


class MainWindow(QWidget):
    # UI 로그 배치 플러시용(내부에서만 사용)
    _log_flush_timer: Optional[QTimer] = None

    def __init__(self):
        super().__init__()
        self.ui = Ui_Form()
        self.ui.setupUi(self)

        # --- 스택 및 페이지 매핑 (UI 객체명 고정)
        self._stack = self.ui.stackedWidget
        self._pages = {
            "pc":  self.ui.page_3,  # Plasma Cleaning
            "ch1": self.ui.page,    # CH1
            "ch2": self.ui.page_2,  # CH2
        }

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

        # === 비동기 장치 ===
        self.faduino = AsyncFaduino()
        self.mfc = AsyncMFC()
        self.ig = AsyncIG()
        self.rf_pulse = RFPulseAsync()
        self.oes = OESAsync()
        self.rga = RGAAsync(
            program_path=RGA_PROGRAM_PATH, 
            csv_path=RGA_CSV_PATH,
            debug_print=True,
        )
        self.dc_power = DCPowerAsync(
            send_dc_power=self.faduino.set_dc_power,
            send_dc_power_unverified=self.faduino.set_dc_power_unverified,
            request_status_read=self.faduino.force_dc_read,
        )
        self.rf_power = RFPowerAsync(
            send_rf_power=self.faduino.set_rf_power,
            send_rf_power_unverified=self.faduino.set_rf_power_unverified,
            request_status_read=self.faduino.force_rf_read,
        )

        self._bg_started = False
        self._bg_tasks = []

        # === Fadunio 버튼 맵핑 ===
        self._pin_to_name = {pin: name for name, pin in BUTTON_TO_PIN.items()}

        # === Google Chat 알림(옵션) ===
        self.chat_notifier = ChatNotifier(CHAT_WEBHOOK_URL) if ENABLE_CHAT_NOTIFY else None
        if self.chat_notifier:
            self.chat_notifier.start()

        # === ProcessController 콜백 주입 (동기 함수 내부에서 코루틴 스케줄) ===
        def cb_faduino(cmd: str, arg):
            asyncio.create_task(self.faduino.handle_named_command(cmd, arg))

        def cb_mfc(cmd: str, args: dict):
            asyncio.create_task(self.mfc.handle_command(cmd, args))

        def cb_dc_power(value: float):
            asyncio.create_task(self.dc_power.start_process(float(value)))

        def cb_dc_stop():
            asyncio.create_task(self.dc_power.cleanup())

        def cb_rf_power(value: float):
            asyncio.create_task(self.rf_power.start_process(float(value)))

        def cb_rf_stop():
            asyncio.create_task(self.rf_power.cleanup())

        def cb_rfpulse_start(power: float, freq, duty):
            asyncio.create_task(self.rf_pulse.start_pulse_process(float(power), freq, duty))

        def cb_rfpulse_stop():
            # stop_process는 동기 함수이므로 그냥 호출
            self.rf_pulse.stop_process()

        def cb_ig_wait(base_pressure: float):
            asyncio.create_task(self.ig.wait_for_base_pressure(float(base_pressure)))

        def cb_ig_cancel():
            # IG 자동 재점등/폴링 즉시 중단 + SIG 0 (응답 무시)
            try:
                self.ig.cancel_wait()
            except Exception:
                pass

        def cb_rga_scan():
            asyncio.create_task(self._do_rga_scan())

        def cb_oes_run(duration_sec: float, integration_ms: int):
            async def run():
                try:
                    # 1) 채널이 열려 있지 않으면 초기화 시도 (안전망)
                    if getattr(self.oes, "sChannel", -1) < 0:
                        ok = await self.oes.initialize_device()
                        if not ok:
                            raise RuntimeError("OES 초기화 실패")

                    # 2) 측정 시작
                    await self.oes.run_measurement(duration_sec, integration_ms)
                except Exception as e:
                    self.process_controller.on_oes_failed("OES", str(e))
                    if self.chat_notifier:
                        self.chat_notifier.notify_error("OES", str(e))
            asyncio.create_task(run())

        self.process_controller = ProcessController(
            send_faduino=cb_faduino,
            send_mfc=cb_mfc,
            send_dc_power=cb_dc_power,
            stop_dc_power=cb_dc_stop,
            send_rf_power=cb_rf_power,
            stop_rf_power=cb_rf_stop,
            start_rfpulse=cb_rfpulse_start,
            stop_rfpulse=cb_rfpulse_stop,
            ig_wait=cb_ig_wait,
            cancel_ig=cb_ig_cancel,
            rga_scan=cb_rga_scan,
            oes_run=cb_oes_run,
        )

        # === UI 버튼 연결 ===
        self._connect_ui_signals()

        # 로그 배치 flush
        self.ui.ch2_logMessage_edit.setMaximumBlockCount(2000)

        # 고정 로그 폴더 (UNC)
        self._log_dir = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs")
        try:
            self._log_dir.mkdir(parents=True, exist_ok=True)  # 폴더 없으면 생성 시도
        except Exception:
            pass

        # Start 전에는 파일 미정
        self._log_file_path = None

        # 앱 종료 훅
        app = QCoreApplication.instance()
        if app is not None:
            app.aboutToQuit.connect(lambda: self._shutdown_once("aboutToQuit"))

        # 초기 상태
        self._on_process_status_changed(False)

    # ------------------------------------------------------------------
    # UI 버튼 연결만 유지 (컨트롤러 ↔ UI는 이벤트 큐로 처리)
    # ------------------------------------------------------------------
    def _connect_ui_signals(self):
        self.ui.ch2_Start_button.clicked.connect(self._handle_start_clicked)
        self.ui.ch2_Stop_button.clicked.connect(self._handle_stop_clicked)
        self.ui.ch2_processList_button.clicked.connect(self._handle_process_list_clicked)

        # --- 페이지 네비게이션 (UI에 존재하는 버튼들 직접 연결)
        # Plasma Cleaning 페이지의 버튼
        self.ui.pc_btnGoCh1.clicked.connect(lambda: self._switch_page("ch1"))
        self.ui.pc_btnGoCh2.clicked.connect(lambda: self._switch_page("ch2"))

        # CH1 페이지의 버튼
        self.ui.ch1_btnGoPC.clicked.connect(lambda: self._switch_page("pc"))
        self.ui.ch1_btnGoCh2.clicked.connect(lambda: self._switch_page("ch2"))

        # CH2 페이지의 버튼
        self.ui.ch2_btnGoPC.clicked.connect(lambda: self._switch_page("pc"))
        self.ui.ch2_btnGoCh1.clicked.connect(lambda: self._switch_page("ch1"))

    # ------------------------------------------------------------------
    # Page 전환 함수
    # ------------------------------------------------------------------
    def _switch_page(self, key: str):
        """'pc' | 'ch1' | 'ch2' 키로 스택 페이지 전환"""
        page = self._pages.get(key)
        if not page:
            self.append_log("UI", f"페이지 키 '{key}' 를 찾을 수 없습니다.")
            return

        # 스택 전환
        try:
            self._stack.setCurrentWidget(page)
        except Exception as e:
            self.append_log("UI", f"페이지 전환 실패({key}): {e}")
            return

    # ------------------------------------------------------------------
    # ProcessController 이벤트 펌프 (컨트롤러 → UI/로거/알림/다음 공정)
    # ------------------------------------------------------------------
    async def _pump_pc_events(self):
        q = self.process_controller.event_q
        while True:
            ev = await q.get()
            kind = ev.kind
            payload = ev.payload or {}

            try:
                if kind == "log":
                    self.append_log(payload.get("src", "PC"), payload.get("msg", ""))
                elif kind == "state":
                    self._apply_process_state_message(payload.get("text", ""))
                elif kind == "status":
                    self._on_process_status_changed(bool(payload.get("running", False)))
                elif kind == "started":
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    self._log_file_path = self._log_dir / f"{ts}.txt"
                    # 파일에 즉시 한 줄 남김
                    try:
                        with open(self._log_file_path, "a", encoding="utf-8") as f:
                            f.write(
                                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [Logger] "
                                f"새 로그 파일 시작: {self._log_file_path}\n"
                            )
                    except Exception:
                        pass

                    # DataLogger, 그래프, 알림
                    try:
                        self.data_logger.start_new_log_session(payload.get("params", {}))
                    except Exception:
                        pass
                    self.graph_controller.reset()
                    if self.chat_notifier:
                        try:
                            self.chat_notifier.notify_process_started(payload.get("params", {}))
                        except Exception:
                            pass
                elif kind == "finished":
                    ok = bool(payload.get("ok", False))
                    detail = payload.get("detail", {})
                    # DataLogger 마무리
                    try:
                        # finalize 함수 시그니처가 불명확하니 안전하게 호출
                        self.data_logger.finalize_and_write_log(ok)
                    except TypeError:
                        try:
                            self.data_logger.finalize_and_write_log()
                        except Exception:
                            pass
                    # 알림
                    if self.chat_notifier:
                        try:
                            self.chat_notifier.notify_process_finished_detail(ok, detail)
                        except Exception:
                            pass
                    try:
                        self.mfc.on_process_finished(ok)
                    except Exception:
                        pass
                    try:
                        self.faduino.on_process_finished(ok)
                    except Exception:
                        pass

                    # ✅ 공정 종료 이후에만 전체 cleanup 수행
                    if getattr(self, "_pending_device_cleanup", False):
                        try:
                            await self._stop_device_watchdogs(light=False)
                        except Exception:
                            pass
                        self._pending_device_cleanup = False

                    # 종료 가드 해제
                    self._pc_stopping = False

                    # 자동 큐 진행
                    self._start_next_process_from_queue(ok)

                elif kind == "aborted":
                    if self.chat_notifier:
                        try:
                            self.chat_notifier.notify_text("🛑 공정이 중단되었습니다.")
                        except Exception:
                            pass
                elif kind == "polling_targets":
                    targets = payload.get("targets", {})
                    self.append_log("Process", f"폴링 타깃 적용: {targets}")
                    self._apply_polling_targets(targets)

                elif kind == "polling":
                    active = bool(payload.get("active", False))
                    self.append_log("Process", f"폴링 {'ON' if active else 'OFF'}")
            finally:
                # ★ 핵심: 매 이벤트 처리 후 한 번 양보 → Qt 페인팅/타이머/다른 코루틴이 돌 기회 제공
                await asyncio.sleep(0)

    # ------------------------------------------------------------------
    # 비동기 이벤트 펌프 (장치 → ProcessController)
    # ------------------------------------------------------------------
    async def _pump_faduino_events(self):
        async for ev in self.faduino.events():
            k = ev.kind
            if k == "status":
                self.append_log("Faduino", ev.message or "")
            elif k == "command_confirmed":
                name = ev.cmd or ""
                try:
                    # "R,<pin>,<state>" → "MV"/"Ar"/"O2"/"N2"/"MS"/"G1"/"G2"/"G3"
                    if name.startswith("R,"):
                        _, pin_str, _ = name.split(",", 2)
                        pin = int(pin_str)
                        name = self._pin_to_name.get(pin, name)
                except Exception:
                    pass
                self.process_controller.on_faduino_confirmed(name)

            elif k == "command_failed":
                why = ev.reason or "unknown"
                name = ev.cmd or ""
                try:
                    if name == "R" or name.startswith("R,"):
                        # 실패 시 cmd가 "R"만 올 수도 있어 핀 파싱이 안될 수 있음
                        if "," in name:
                            _, pin_str, _ = name.split(",", 2)
                            pin = int(pin_str)
                            name = self._pin_to_name.get(pin, name)
                except Exception:
                    pass
                self.process_controller.on_faduino_failed(name, why)

            elif k == "dc_power":
                p, v, c = ev.dc_p or 0.0, ev.dc_v or 0.0, ev.dc_c or 0.0
                try:
                    self.data_logger.log_dc_power(p, v, c)
                except Exception:
                    pass
                self.handle_dc_power_display(p, v, c)
                self.dc_power.update_measurements(p, v, c)

            elif k == "rf_power":
                f, r = ev.rf_forward or 0.0, ev.rf_reflected or 0.0
                try:
                    self.data_logger.log_rf_power(f, r)
                except Exception:
                    pass
                self.handle_rf_power_display(f, r)
                self.rf_power.update_measurements(f, r)

    async def _pump_mfc_events(self):
        async for ev in self.mfc.events():
            k = ev.kind
            if k == "status":
                self.append_log("MFC", ev.message or "")
            elif k == "command_confirmed":
                self.process_controller.on_mfc_confirmed(ev.cmd or "")
            elif k == "command_failed":
                why = ev.reason or "unknown"
                self.process_controller.on_mfc_failed(ev.cmd or "", why)
                if self.chat_notifier:
                    self.chat_notifier.notify_error_with_src("MFC", f"{ev.cmd or ''}: {why}")
            elif k == "flow":
                gas = ev.gas or ""
                flow = float(ev.value or 0.0)
                try:
                    self.data_logger.log_mfc_flow(gas, flow)
                except Exception:
                    pass
                self.update_mfc_flow_ui(gas, flow)
            elif k == "pressure":
                txt = ev.text or (f"{ev.value:.3g}" if ev.value is not None else "")
                try:
                    self.data_logger.log_mfc_pressure(txt)
                except Exception:
                    pass
                self.update_mfc_pressure_ui(txt)

    async def _pump_ig_events(self):
        async for ev in self.ig.events():
            k = ev.kind
            if k == "status":
                self.append_log("IG", ev.message or "")
            elif k == "pressure":
                pass  # 필요 시 UI 반영
            elif k == "base_reached":
                self.process_controller.on_ig_ok()
                asyncio.create_task(self.ig.cleanup())  # ✅ 안전망
            elif k == "base_failed":
                why = ev.message or "unknown"
                self.process_controller.on_ig_failed("IG", why)
                if self.chat_notifier:
                    self.chat_notifier.notify_error("IG", why)
                asyncio.create_task(self.ig.cleanup())  # ✅ 안전망

    async def _pump_rga_events(self):
        async for ev in self.rga.events():
            if ev.kind == "status":
                self.append_log("RGA", ev.message or "")
            elif ev.kind == "data":
                # 그래프 업데이트
                self.graph_controller.update_rga_plot(ev.mass_axis, ev.pressures)
            elif ev.kind == "finished":
                self.process_controller.on_rga_finished()
            elif ev.kind == "failed":
                why = ev.message or "RGA failed"
                self.process_controller.on_rga_failed("RGA", why)
                if self.chat_notifier:
                    self.chat_notifier.notify_error("RGA", why)

    async def _pump_dc_events(self):
        async for ev in self.dc_power.events():
            k = ev.kind
            if k == "status":
                self.append_log("DCpower", ev.message or "")
            elif k == "state_changed":
                try:
                    self.faduino.on_dc_state_changed(bool(ev.running))
                except Exception:
                    pass
            elif k == "target_reached":
                self.process_controller.on_dc_target_reached()
            elif k == "power_off_finished":
                self.process_controller.on_device_step_ok()

    async def _pump_rf_events(self):
        async for ev in self.rf_power.events():
            k = ev.kind
            if k == "status":
                self.append_log("RFpower", ev.message or "")
            elif k == "state_changed":
                try:
                    self.faduino.on_rf_state_changed(bool(ev.running))
                except Exception:
                    pass
            elif k == "target_reached":
                self.process_controller.on_rf_target_reached()
            elif k == "target_failed":
                why = ev.message or "unknown"
                self.process_controller.on_rf_target_failed(why)
                if self.chat_notifier:
                    self.chat_notifier.notify_error_with_src("RF Power", why)
            elif k == "power_off_finished":
                self.process_controller.on_device_step_ok()

    async def _pump_rfpulse_events(self):
        async for ev in self.rf_pulse.events():
            k = ev.kind
            if k == "status":
                self.append_log("RFPulse", ev.message or "")
            elif k == "target_reached":
                self.process_controller.on_rf_target_reached()
            elif k == "command_failed":
                why = ev.reason or "unknown"
                self.process_controller.on_rf_pulse_failed(why)
                if self.chat_notifier:
                    self.chat_notifier.notify_error_with_src("RF Pulse", why)
            elif k == "power_off_finished":
                self.process_controller.on_rf_pulse_off_finished()

    async def _pump_oes_events(self):
        async for ev in self.oes.events():
            if ev.kind == "status":
                self.append_log("OES", ev.message or "")
            elif ev.kind == "data":
                self.graph_controller.update_oes_plot(ev.x, ev.y)
            elif ev.kind == "finished":
                if ev.success:
                    self.process_controller.on_oes_ok()
                else:
                    self.process_controller.on_oes_failed("OES", "measure failed")
                    if self.chat_notifier:
                        self.chat_notifier.notify_error("OES", "measure failed")

    # ------------------------------------------------------------------
    # RGA dummy
    # ------------------------------------------------------------------
    async def _do_rga_scan(self):
        await self.rga.scan_once() 

    # ------------------------------------------------------------------
    # 백그라운 태스크 시작 함수
    # ------------------------------------------------------------------
    def _ensure_background_started(self):
        if self._bg_started:
            return
        # 혹시 남아있는 태스크가 있으면(이상 상태) 정리
        try:
            self._bg_tasks = [t for t in getattr(self, "_bg_tasks", []) if t and not t.done()]
            if self._bg_tasks:
                for t in self._bg_tasks:
                    t.cancel()
        except Exception:
            pass
        self._bg_tasks = []

        self._bg_started = True
        loop = asyncio.get_running_loop()
        self._bg_tasks = [
            # 장치 내부 루프
            loop.create_task(self.faduino.start()),
            loop.create_task(self.mfc.start()),
            loop.create_task(self.ig.start()),
            loop.create_task(self.rf_pulse.start()),
            # 이벤트 펌프(장치 → PC/UI)
            loop.create_task(self._pump_faduino_events()),
            loop.create_task(self._pump_mfc_events()),
            loop.create_task(self._pump_ig_events()),
            loop.create_task(self._pump_rga_events()),
            loop.create_task(self._pump_dc_events()),
            loop.create_task(self._pump_rf_events()),
            loop.create_task(self._pump_rfpulse_events()),
            loop.create_task(self._pump_oes_events()),
            # PC 이벤트 펌프
            loop.create_task(self._pump_pc_events()),
        ]

    # ------------------------------------------------------------------
    # 표시/입력 관련
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

    def _on_process_status_changed(self, running: bool):
        self.ui.ch2_Start_button.setEnabled(not running)
        self.ui.ch2_Stop_button.setEnabled(True)

    # ------------------------------------------------------------------
    # 파일 로딩 / 파라미터 UI 반영
    # ------------------------------------------------------------------
    @Slot()
    @Slot(bool)
    def _handle_process_list_clicked(self, _checked: bool = False):
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
            norm = self._normalize_params_for_process(params)
            self._prepare_log_file(norm)  # [추가] 다음 공정도 장비 연결부터 동일 파일에 기록
            QTimer.singleShot(100, lambda p=params: self._safe_start_process(self._normalize_params_for_process(p)))
        else:
            self.append_log("MAIN", "모든 공정이 완료되었습니다.")
            self._clear_queue_and_reset_ui()

    def _safe_start_process(self, params: dict):
        if self.process_controller.is_running:
            self.append_log("MAIN", "경고: 이미 다른 공정이 실행 중이므로 새 공정을 시작하지 않습니다.")
            return
        # 프리플라이트(연결 확인) → 완료 후 공정 시작
        asyncio.create_task(self._start_after_preflight(params))

    # (MainWindow 클래스 내부)
    # 1) 재진입 안전한 비모달 표출 유틸을 "메서드"로 추가
    def _post_critical(self, title: str, text: str) -> None:
        QTimer.singleShot(0, lambda: QMessageBox.critical(self, title, text))

    # 2) async 함수 안의 모달 호출을 유틸로 교체
    async def _start_after_preflight(self, params: dict):
        try:
            self._ensure_background_started()
            self.append_log("MAIN", "장비 연결 확인 중...")

            ok, failed = await self._preflight_connect(params, timeout_s=5.0)
            if not ok:
                fail_list = ", ".join(failed) if failed else "알 수 없음"
                self.append_log("MAIN", f"필수 장비 연결 실패: {fail_list} → 공정 시작 중단")

                # ⬇️ 모달 직접 호출 금지 → 이벤트 루프 턴 넘긴 뒤 띄우기
                self._post_critical(
                    "장비 연결 실패",
                    f"다음 장비 연결을 확인하지 못했습니다:\n - {fail_list}\n\n"
                    "케이블/전원/포트 설정을 확인한 뒤 다시 시도하세요."
                )
                self._start_next_process_from_queue(False)
                return

            self.append_log("MAIN", "장비 연결 확인 완료 → 공정 시작")
            self.process_controller.start_process(params)

        except Exception as e:
            msg = f"오류: '{params.get('Process_name', '알 수 없는')} 공정' 시작에 실패했습니다. ({e})"
            self.append_log("MAIN", msg)

            # ⬇️ 예외 케이스도 동일하게 비모달로
            self._post_critical("오류", msg)

            self._start_next_process_from_queue(False)

    async def _wait_device_connected(self, dev, name: str, timeout_s: float) -> bool:
        """장비 워치독이 포트를 실제로 열어 `_connected=True` 될 때까지 기다린다."""
        try:
            t0 = asyncio.get_running_loop().time()
        except RuntimeError:
            t0 = 0.0  # fallback
        self.append_log(name, "연결 대기...")
        while True:
            try:
                if bool(getattr(dev, "_connected", False)):
                    self.append_log(name, "연결 확인")
                    return True
            except Exception:
                pass
            try:
                now = asyncio.get_running_loop().time()
            except RuntimeError:
                now = t0 + timeout_s + 1.0
            if now - t0 >= timeout_s:
                self.append_log(name, "연결 확인 실패(타임아웃)")
                return False
            await asyncio.sleep(0.05)

    async def _preflight_connect(self, params: dict, timeout_s: float = 5.0) -> tuple[bool, list[str]]:
        """
        필수 장비가 `_connected=True`가 될 때까지 대기.
        - 기본 필수: Faduino, MFC, IG
        - 선택 필수: RF Pulse를 사용할 때만 RFPulse 포함
        반환: (모두연결OK, 실패장비이름목록)
        """
        need: list[tuple[str, object]] = [
            ("Faduino", self.faduino),
            ("MFC", self.mfc),
            ("IG", self.ig),
        ]
        try:
            use_rf_pulse = bool(params.get("use_rf_pulse", False) or params.get("use_rf_pulse_power", False))
        except Exception:
            use_rf_pulse = False
        if use_rf_pulse:
            need.append(("RFPulse", self.rf_pulse))

        # 병렬 대기
        results = await asyncio.gather(
            *[self._wait_device_connected(dev, name, timeout_s) for name, dev in need],
            return_exceptions=False
        )
        failed = [name for (name, _), ok in zip(need, results) if not ok]
        return (len(failed) == 0, failed)

    # ------------------------------------------------------------------
    # 단일 실행
    # ------------------------------------------------------------------
    @Slot()
    @Slot(bool)
    def _handle_start_clicked(self, _checked: bool = False):
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

        self._prepare_log_file(params)  # [추가] 장비 연결 전에 새 로그 파일 준비
        self.append_log("MAIN", "입력 검증 통과 → 장비 연결 확인 시작")
        self._safe_start_process(params)

    # ------------------------------------------------------------------
    # STOP/종료 (단일 경로)
    # ------------------------------------------------------------------
    @Slot()
    def _handle_stop_clicked(self, _checked: bool = False):
        self.request_stop_all(user_initiated=True)

    def request_stop_all(self, user_initiated: bool):
        # 이미 종료 절차가 진행 중이면 중복 요청 차단
        if getattr(self, "_pc_stopping", False):
            self.append_log("MAIN", "정지 요청 무시: 이미 종료 절차 진행 중")
            return

        # (선택) 만약 워치독 워커가 죽어있다면 살려서 포트 재연결 가능 상태 보장
        try:
            self._ensure_background_started()
        except Exception:
            pass

        # 0) ⚡ 라이트 정지: 폴링만 끄고(재연결은 장치별 pause로 일시중지), 포트는 열어둠
        asyncio.create_task(self._stop_device_watchdogs(light=True))

        # 이번에 종료 절차에 진입
        self._pc_stopping = True
        self._pending_device_cleanup = True  # 풀 cleanup은 공정 종료 후로 미룸

        # 1) 공정 종료만 지시 (11단계)
        self.process_controller.request_stop()

    async def _stop_device_watchdogs(self, *, light: bool = False):
        """
        light=True : 폴링만 즉시 중지(연결은 유지, 포트 닫지 않음)
        light=False: 전체 정리(이벤트 펌프/워커/워치독 취소 + cleanup)
        """
        if light:
            # 폴링만 중지 (연결/워커 유지)
            try:
                self.mfc.set_process_status(False)
            except Exception:
                pass
            try:
                self.faduino.set_process_status(False)
            except Exception:
                pass
            try:
                self.rf_pulse.set_process_status(False)
            except Exception:
                pass
            return

        # ===== 기존 전체 정리 경로 =====
        # 0) 이벤트 펌프/백그라운드 태스크 먼저 취소 → 중복 소비/중복 로깅 방지
        try:
            for t in getattr(self, "_bg_tasks", []):
                try:
                    if t and not t.done():
                        t.cancel()
                except Exception:
                    pass
            if self._bg_tasks:
                await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        finally:
            self._bg_tasks = []

        # 1) 장치 워치독/워커 정리(재연결 억제)
        tasks = []
        for dev in (self.ig, self.mfc, self.faduino, self.rf_pulse, self.dc_power, self.rf_power, self.oes):
            if dev and hasattr(dev, "cleanup"):
                try:
                    tasks.append(dev.cleanup())
                except Exception:
                    pass
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # 2) 다음 Start에서만 다시 올리도록 플래그 리셋
        self._bg_started = False

    # ------------------------------------------------------------------
    # 로그
    # ------------------------------------------------------------------
    def append_log(self, source: str, msg: str):
        now_ui = datetime.now().strftime("%H:%M:%S")
        now_file = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line_ui = f"[{now_ui}] [{source}] {msg}"
        line_file = f"[{now_file}] [{source}] {msg}\n"

        # 1) UI: 즉시(그러나 GUI 스레드 보장 위해 Qt 큐로 위임)
        QTimer.singleShot(0, lambda s=line_ui: self._append_log_to_ui(s))

        # 2) 파일: 즉시 append
        path = self._log_file_path or (self._log_dir / "log.txt")
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line_file)
        except Exception as e:
            # 파일 실패도 UI에 바로 알림
            QTimer.singleShot(0, lambda: self.ui.ch2_logMessage_edit.appendPlainText(f"[Logger] 파일 기록 실패: {e}"))

    def _append_log_to_ui(self, line: str):
        self.ui.ch2_logMessage_edit.moveCursor(QTextCursor.MoveOperation.End)
        self.ui.ch2_logMessage_edit.insertPlainText(line + "\n")

    # === [추가] 새 로그 파일을 프리플라이트 전에 준비 ===
    def _prepare_log_file(self, params: dict):
        """프리플라이트(장비 연결) 시작 직전에 호출: 새 로그 파일 경로 생성 + 헤더 남김."""
        from datetime import datetime
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._log_file_path = self._log_dir / f"{ts}.txt"
        # 한 줄 알림은 append_log로 남겨 경로 세팅 직후 동일 파일에 기록되게 함
        self.append_log("Logger", f"새 로그 파일 시작: {self._log_file_path}")
        note = str(params.get("process_note", "") or params.get("Process_name", "") or "Run")
        self.append_log("MAIN", f"=== '{note}' 공정 준비 (장비 연결부터 기록) ===")

    # ------------------------------------------------------------------
    # 폴링/상태
    # ------------------------------------------------------------------
    def _apply_polling_targets(self, targets: dict):
        """
        ProcessController가 내려보낸 폴링 타깃을 그대로 각 장치에 적용한다.
        - DC 경로:   {'mfc':True,  'faduino':True,  'rfpulse':False}
        - RF-Pulse: {'mfc':True,  'faduino':False, 'rfpulse':True}
        - 비활성:    모두 False
        """
        try:
            self.mfc.set_process_status(bool(targets.get('mfc', False)))
        except Exception:
            pass
        try:
            self.faduino.set_process_status(bool(targets.get('faduino', False)))
        except Exception:
            pass
        try:
            # ★ 누락되었던 부분: RF-Pulse 폴링도 명시적으로 토글
            self.rf_pulse.set_process_status(bool(targets.get('rfpulse', False)))
        except Exception:
            pass

    def _apply_process_state_message(self, message: str):
        # 같은 텍스트면 스킵(불필요한 repaint 방지)
        if getattr(self, "_last_state_text", None) == message:
            return
        self._last_state_text = message

        # Qt 이벤트 큐로 위임하여 항상 GUI 스레드에서 안전하게 반영
        QTimer.singleShot(0, lambda m=message: self.ui.ch2_processState_edit.setPlainText(m))

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

        # 1) Stop 요청(라이트 정지 + 종료 11단계)
        self.request_stop_all(user_initiated=False)

        # 2) 즉시 cleanup()은 호출하지 않음.
        #    finished/aborted 이벤트에서 _stop_device_watchdogs(light=False)로 한 번에 정리.
        #    (안전장치) 그래도 10초 내에 종료 이벤트가 안 오면 강제 정리
        def _force_cleanup():
            if getattr(self, "_pending_device_cleanup", False):
                self.append_log("MAIN", "강제 정리 타임아웃 → 풀 cleanup 강제 수행")
                asyncio.create_task(self._stop_device_watchdogs(light=False))
                self._pending_device_cleanup = False
                self._pc_stopping = False
                QTimer.singleShot(0, QCoreApplication.quit)

        QTimer.singleShot(10_000, _force_cleanup)

        # 3) Chat Notifier 정지 등 부가 정리는 유지
        try:
            if self.chat_notifier:
                self.chat_notifier.shutdown()
        except Exception:
            pass
        # 4) Qt 앱 종료는 cleanup 이후에 최종적으로 수행되므로 여기선 스케줄만
        self.append_log("MAIN", "종료 시퀀스 진행 중 (종료 11단계 대기)")


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
        def tf(v): return str(v).strip().upper() in ("T", "TRUE", "1", "Y", "YES")
        def fget(key, default="0"):
            try:
                return float(str(raw.get(key, default)).strip())
            except Exception:
                return float(default)
        def iget(key, default="0"):
            try:
                return int(float(str(raw.get(key, default)).strip()))
            except Exception:
                return int(default)
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


if __name__ == "__main__":
    app = QApplication(sys.argv)
    loop = QEventLoop(app)          # Qt + asyncio 루프 통합
    asyncio.set_event_loop(loop)
    w = MainWindow()
    w.show()
    with loop:
        loop.run_forever()

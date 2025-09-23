# main.py
# -*- coding: utf-8 -*-
import re, csv, sys, traceback, asyncio
from typing import Optional, TypedDict, Mapping, Any, Coroutine, Callable, Literal, Sequence, Deque, cast
from datetime import datetime
from pathlib import Path
from collections import deque
import contextlib

from PySide6.QtWidgets import QApplication, QWidget, QMessageBox, QFileDialog, QPlainTextEdit, QStackedWidget
from PySide6.QtCore import QCoreApplication, QTimer
from PySide6.QtGui import QTextCursor, QCloseEvent
from qasync import QEventLoop

# =============== debug ==============
import time
# =============== debug ==============

# === imports ===
from ui.main_window import Ui_Form
from main_tsp import TSPPageController
from controller.graph_controller import GraphController
from controller.data_logger import DataLogger
from controller.chat_notifier import ChatNotifier

# ✅ 실제 장비 모듈(비동기)
from device.ig import AsyncIG
from device.mfc import AsyncMFC
from device.plc import AsyncFaduinoPLC
from device.oes import OESAsync
from device.rga import RGA100AsyncAdapter
from device.dc_power import DCPowerAsync
from device.rf_power import RFPowerAsync
from device.rf_pulse import RFPulseAsync

# ✅ CH2 공정 컨트롤러 (asyncio 순수 버전)
from controller.process_ch2 import ProcessController

from lib.config_ch2 import (
    CHAT_WEBHOOK_URL, ENABLE_CHAT_NOTIFY, IG_POLLING_INTERVAL_MS,
    RGA_NET, RGA_CSV_PATH, TSP_PORT, TSP_BAUD
)

RawParams = TypedDict('RawParams', {
    # 파일/CSV 및 내부 공용 키 (대부분 Optional 취급; total=False 덕분에 부분 딕셔너리 허용)
    'Process_name': str,
    'process_note': str,

    'base_pressure': float | str,
    'working_pressure': float | str,
    'process_time': float | str,
    'shutter_delay': float | str,
    'integration_time': int | str,

    'Ar': Literal['T','F'] | bool,
    'O2': Literal['T','F'] | bool,
    'N2': Literal['T','F'] | bool,
    'Ar_flow': float | str,
    'O2_flow': float | str,
    'N2_flow': float | str,

    'use_dc_power': Literal['T','F'] | bool,
    'use_rf_power': Literal['T','F'] | bool,
    'use_rf_pulse': Literal['T','F'] | bool,
    'use_rf_pulse_power': Literal['T','F'] | bool,
    'dc_power': float | str,
    'rf_power': float | str,
    'rf_pulse_power': float | str,
    'rf_pulse_freq': int | str | None,
    'rf_pulse_duty_cycle': int | str | None,

    'gun1': Literal['T','F'] | bool,
    'gun2': Literal['T','F'] | bool,
    'gun3': Literal['T','F'] | bool,
    'main_shutter': Literal['T','F'] | bool,

    # ★ CSV/로그에 쓰이는 공백 포함 키(그대로 둬야 함)
    'G1 Target': str,
    'G2 Target': str,
    'G3 Target': str,

    'power_select': Literal['T','F'] | bool,
}, total=False)


NormParams = TypedDict('NormParams', {
    # 실행에 필요한 정규화 파라미터(숫자/불리언 확정)
    'base_pressure': float,
    'working_pressure': float,
    'process_time': float,
    'shutter_delay': float,
    'integration_time': int,

    'use_ar': bool,
    'use_o2': bool,
    'use_n2': bool,
    'ar_flow': float,
    'o2_flow': float,
    'n2_flow': float,

    'use_dc_power': bool,
    'dc_power': float,
    'use_rf_power': bool,
    'rf_power': float,
    'use_rf_pulse': bool,
    'rf_pulse_power': float,
    'rf_pulse_freq': int | None,
    'rf_pulse_duty': int | None,

    'use_g1': bool,
    'use_g2': bool,
    'use_g3': bool,
    'use_ms': bool,

    'process_note': str,

    # 내부에서 쓰는 식별자 키(언더스코어)와
    'G1_target_name': str,
    'G2_target_name': str,
    'G3_target_name': str,

    # ★ DataLogger/CSV 호환을 위한 공백 포함 별칭도 함께 둠
    'G1 Target': str,
    'G2 Target': str,
    'G3 Target': str,

    'use_power_select': bool,
}, total=False)

# 자주 쓰는 맵/타깃 타입 별칭
ParamsMap = Mapping[str, Any]
TargetsMap = Mapping[Literal["mfc", "rfpulse", "dc", "rf"], bool]


class MainWindow(QWidget):
    def __init__(self, loop: asyncio.AbstractEventLoop | None = None):
        super().__init__()
        self.ui = Ui_Form()
        self.ui.setupUi(self)
        self._loop = loop or asyncio.get_event_loop()

        # --- 스택 및 페이지 매핑 (UI 객체명 고정)
        self._stack: QStackedWidget = self.ui.stackedWidget
        self._pages: dict[str, QWidget] = {
            "pc":  self.ui.page_3,  # Plasma Cleaning
            "ch1": self.ui.page,    # CH1
            "ch2": self.ui.page_2,  # CH2
        }

        # --- Tab 키로 다음 필드로 이동 (QPlainTextEdit 전체 적용)
        for edit in self.findChildren(QPlainTextEdit):
            edit.setTabChangesFocus(True)

        self._set_default_ui_values()
        self.process_queue: list[RawParams] = []
        self.current_process_index: int = -1
        self._shutdown_called: bool = False
        self._delay_task: Optional[asyncio.Task] = None
        self._force_cleanup_task: Optional[asyncio.Task] = None

        # === 컨트롤러 (메인 스레드에서 생성) ===
        self.graph_controller: GraphController = GraphController(self.ui.ch2_rgaGraph_widget, self.ui.ch2_oesGraph_widget)
        # QtCharts 축/마진/틱 세팅을 즉시 반영(처음 화면에서 틱 100 단위, plotArea 정렬 보장)
        self.graph_controller.reset()

        self.data_logger: DataLogger = DataLogger()

        # ✅ PLC 로그 어댑터 (printf 스타일 → append_log)
        def _plc_log(fmt, *args):
            try:
                msg = (fmt % args) if args else str(fmt)
            except Exception:
                msg = str(fmt)
            self.append_log("PLC", msg)

        # ✅ PLC 인스턴스
        self.plc: AsyncFaduinoPLC = AsyncFaduinoPLC(logger=_plc_log)

        # === 비동기 장치 ===
        self.mfc: AsyncMFC = AsyncMFC()
        self.ig: AsyncIG = AsyncIG()
        self.rf_pulse: RFPulseAsync = RFPulseAsync()
        self.oes: OESAsync = OESAsync()
        # ✅ LAN RGA 두 대
        self.rga_ch1 = RGA100AsyncAdapter(RGA_NET["ch1"]["ip"],
                                        user=RGA_NET["ch1"]["user"],
                                        password=RGA_NET["ch1"]["password"],
                                        name="CH1")
        self.rga_ch2 = RGA100AsyncAdapter(RGA_NET["ch2"]["ip"],
                                        user=RGA_NET["ch2"]["user"],
                                        password=RGA_NET["ch2"]["password"],
                                        name="CH2")
        
        # ✅ 동시 스캔 동기화용 상태
        self._rga_channels: list[int] = [ch for ch, ad in ((1, self.rga_ch1), (2, self.rga_ch2)) if ad]
        self._rga_scan_pending: bool = False
        self._rga_done_signaled: bool = False
        self._rga_plotted: dict[int, bool] = {ch: False for ch in self._rga_channels}
        
        # DC는 family="DCV"
        async def _dc_send(power: float) -> None:
            await self.plc.power_apply(power, family="DCV", ensure_set=True)

        async def _dc_send_unverified(power: float) -> None:
            await self.plc.power_write(power, family="DCV")

        async def _dc_request_read():
            try:
                P, V, I = await self.plc.power_read(family="DCV")
                self.handle_dc_power_display(P, V, I)
                return (P, V, I)
            except Exception as e:
                self.append_log("DCpower", f"read failed: {e!r}")

        self.dc_power: DCPowerAsync = DCPowerAsync(
            send_dc_power=_dc_send,
            send_dc_power_unverified=_dc_send_unverified,
            request_status_read=_dc_request_read,
        )

        async def _rf_send(power: float) -> None:
            # RF는 DCV 네임스페이스의 채널 1번을 사용 → DCV_SET_1, DCV_WRITE_1 로 갑니다.
            await self.plc.power_apply(power, family="DCV", ensure_set=True, channel=1)

        async def _rf_send_unverified(power: float) -> None:
            # 쓰기 레지스터도 DCV_WRITE_1
            await self.plc.power_write(power, family="DCV", write_idx=1)

        async def _rf_request_read():
            # 이 부분은 이미 표와 일치(READ_2=for.p, READ_3=ref.p)
            try:
                fwd_raw = await self.plc.read_reg_name("DCV_READ_2")
                ref_raw = await self.plc.read_reg_name("DCV_READ_3")

                # 필요하면 스케일 적용(지금은 1.0 가정)
                fwd = float(fwd_raw)  # * getattr(self.plc.cfg, "rf_fwd_scale", 1.0)
                ref = float(ref_raw)  # * getattr(self.plc.cfg, "rf_ref_scale", 1.0)

                # RFPowerAsync._ingest_status_result 가 dict의 forward/reflected를 정확히 파싱함
                return {"forward": fwd, "reflected": ref}
            except Exception as e:
                self.append_log("RFpower", f"read failed: {e!r}")
                return None

        self.rf_power: RFPowerAsync = RFPowerAsync(
            send_rf_power=_rf_send,
            send_rf_power_unverified=_rf_send_unverified,
            request_status_read=_rf_request_read,
        )

        # TSP는 별도 컨트롤러에 위임 (버튼 훅/연결/명령 포함)
        self.tsp_ctrl = TSPPageController(
            ui=self.ui,              # UI 전체를 넘겨서 컨트롤러가 Start/Stop 버튼에 자체로 연결
            port=TSP_PORT,
            baud=TSP_BAUD,
            loop=self._loop,         # (선택) 컨트롤러가 asyncio 태스크 만들 때 사용
        )

        self._advancing: bool = False
        self._bg_started: bool = False
        self._bg_tasks: list[asyncio.Task[Any]] = []

        # === Google Chat 알림(옵션) ===
        self.chat_notifier: ChatNotifier | None = ChatNotifier(CHAT_WEBHOOK_URL) if ENABLE_CHAT_NOTIFY else None
        if self.chat_notifier:
            self.chat_notifier.start()

        # === 폴링 데이터 로그창 출력 여부 ===
        self._verbose_polling_log: bool = True  # 필요시 UI 토글로 바꿔도 됨

        # === ProcessController 콜백 주입 (동기 함수 내부에서 코루틴 스케줄) ===
        def cb_plc(cmd: str, on: Any, ch: int) -> None:
            async def run():
                raw = str(cmd)
                nname = raw.upper()
                onb = bool(on)
                try:
                    if nname == "MV":
                        # ✅ 올바른 매핑: 메인 밸브 코일 (채널별)
                        await self.plc.write_switch(f"MAIN_{int(ch)}_GAS_SW", onb)

                    elif nname in ("AR", "O2", "N2", "MAIN"):
                        # ✅ 가스는 plc.gas(ch, gas)
                        await self.plc.gas(int(ch), nname, on=onb)

                    elif nname == "MS":
                        # ✅ 메인 셔터 (채널별)
                        await self.plc.main_shutter(int(ch), open=onb)

                    elif nname in ("G1", "G2", "G3"):
                        # 건 셔터는 채널 독립
                        idx = int(nname[1])
                        await self.plc.write_switch(f"SHUTTER_{idx}_SW", onb)

                    else:
                        # 예: SW_RF_SELECT 등 채널 독립 키는 그대로
                        await self.plc.write_switch(raw, onb)

                    self.process_controller.on_plc_confirmed(nname)
                except Exception as e:
                    self.process_controller.on_plc_failed(nname, str(e))
                    if self.chat_notifier:
                        self.chat_notifier.notify_error_with_src("PLC", f"{nname}: {e}")
                    self.append_log("PLC", f"명령 실패: {raw} -> {onb}: {e!r}")
            self._spawn_detached(run())

        def cb_mfc(cmd: str, args: Mapping[str, Any]) -> None:
            self._spawn_detached(self.mfc.handle_command(cmd, args))

        def cb_dc_power(value: float):
            self._spawn_detached(self.dc_power.start_process(float(value)))

        def cb_dc_stop():
            self._spawn_detached(self.dc_power.cleanup())

        def cb_rf_power(value: float):
            self._spawn_detached(self.rf_power.start_process(float(value)))

        def cb_rf_stop():
            self._spawn_detached(self.rf_power.cleanup())

        def cb_rfpulse_start(power: float, freq: int | None, duty: int | None) -> None:
            self._spawn_detached(self.rf_pulse.start_pulse_process(float(power), freq, duty))

        def cb_rfpulse_stop():
            # stop_process는 동기 함수이므로 그대로 호출
            self.rf_pulse.stop_process()

        def cb_ig_wait(base_pressure: float) -> None:      # ← self 제거
            async def _run():
                # 동기 함수라 await 금지 (사이드이펙트로 펌프/워치독 띄움)
                self._ensure_background_started()
                ok = await self.ig.wait_for_base_pressure(  # 반환값을 변수에 담아주면 회색 경고도 사라짐
                    float(base_pressure),
                    interval_ms=IG_POLLING_INTERVAL_MS
                )
                self.append_log("IG", f"wait_for_base_pressure returned: {ok}")
            self._spawn_detached(_run())

        def cb_ig_cancel():
            self._spawn_detached(self.ig.cancel_wait())

        def cb_oes_run(duration_sec: float, integration_ms: int):
            async def run():
                try:
                    self._ensure_background_started()
                    if getattr(self.oes, "sChannel", -1) < 0:
                        ok = await self.oes.initialize_device()
                        if not ok:
                            raise RuntimeError("OES 초기화 실패")
                    targets = getattr(self, "_last_polling_targets", None)
                    if not targets:
                        params = getattr(self.process_controller, "current_params", {}) or {}
                        use_rf_pulse = bool(params.get("use_rf_pulse", False))
                        targets = {"mfc": True, "rfpulse": use_rf_pulse}
                    self._apply_polling_targets(targets)

                    self._soon(self.graph_controller.clear_oes_plot)  # Qt GUI 스레드에서 안전 호출
                    self.append_log("DBG", "OES: run_measurement() call")
                    await self.oes.run_measurement(duration_sec, integration_ms)
                    self.append_log("DBG", "OES: run_measurement() returned")

                except Exception as e:
                    self.process_controller.on_oes_failed("OES", str(e))
                    if self.chat_notifier:
                        self.chat_notifier.notify_error_with_src("OES", str(e))
            self._spawn_detached(run())

        self.process_controller = ProcessController(
            send_plc=cb_plc,
            send_mfc=cb_mfc,
            send_dc_power=cb_dc_power,
            stop_dc_power=cb_dc_stop,
            send_rf_power=cb_rf_power,
            stop_rf_power=cb_rf_stop,
            start_rfpulse=cb_rfpulse_start,
            stop_rfpulse=cb_rfpulse_stop,
            ig_wait=cb_ig_wait,
            cancel_ig=cb_ig_cancel,
            rga_scan=self.cb_rga_scan,   # ✔️ 바운드 메서드로 넘기기
            oes_run=cb_oes_run,
        )

        # === UI 버튼 연결 ===
        self._connect_ui_signals()

        # 로그 배치 flush
        self.ui.ch2_logMessage_edit.setMaximumBlockCount(2000)

        # 기본은 NAS 경로, 실패 시 로컬 폴백
        nas_path = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs")
        local_fallback = Path.cwd() / "_Logs_local"
        try:
            nas_path.mkdir(parents=True, exist_ok=True)
            self._log_dir = nas_path
        except Exception:
            local_fallback.mkdir(parents=True, exist_ok=True)
            self._log_dir = local_fallback
            self._soon(self.ui.ch2_logMessage_edit.appendPlainText, f"[Logger] NAS 폴더 접근 실패 → 로컬로 폴백: {self._log_dir}")


        # Start 전에는 파일 미정
        self._log_file_path: Path | None = None

        # ✅ Start 전 로그를 임시로 쌓아둘 버퍼(최근 1000줄)
        self._prestart_buf: Deque[str] = deque(maxlen=1000)

        # 선택적으로 쓰는 내부 상태 캐시들 초기화
        self._last_polling_targets: TargetsMap | None = None
        self._last_state_text: str | None = None
        self._pc_stopping: bool = False
        self._pending_device_cleanup: bool = False

        # # 앱 종료 훅
        # app = QCoreApplication.instance()
        # if app is not None:
        #     app.aboutToQuit.connect(lambda: self._shutdown_once("aboutToQuit"))

        # 초기 상태
        self._on_process_status_changed(False)

        # ✅ 콘솔로 안 찍고 로그로만 받게 훅 설치
        self._install_exception_hooks()

        # =============== debug ==============
        # __init__ 끝부분, self._install_exception_hooks() 다음쯤
        from PySide6.QtCore import qInstallMessageHandler, QtMsgType
        def _qt_msg_handler(mode, context, message):
            tag = {QtMsgType.QtDebugMsg: "QtDebug", QtMsgType.QtInfoMsg: "QtInfo",
                QtMsgType.QtWarningMsg: "QtWarn", QtMsgType.QtCriticalMsg: "QtCrit",
                QtMsgType.QtFatalMsg: "QtFatal"}.get(mode, "Qt")
            self.append_log(tag, message)
        qInstallMessageHandler(_qt_msg_handler)

        # 🔧 (A) UI 하트비트: 메인 스레드 지연 감시
        self._last_ui_tick = time.perf_counter()
        self._ui_heartbeat = QTimer(self)
        self._ui_heartbeat.setInterval(1000)  # 1s
        self._ui_heartbeat.timeout.connect(self._on_ui_heartbeat)
        self._ui_heartbeat.start()

        # 🔧 (B) asyncio 이벤트루프 지연 감시
        self._spawn_detached(self._loop_health_watchdog(), store=True, name="WD.eventloop")
        
    def _on_ui_heartbeat(self):
        now = time.perf_counter()
        dt_ms = (now - getattr(self, "_last_ui_tick", now)) * 1000.0
        self._last_ui_tick = now
        if dt_ms > 1500:  # 1.5초 이상 늦으면 UI가 막혔을 가능성
            self.append_log("WD.UI", f"UI 타이머 지연 {dt_ms:.0f} ms (메인 스레드 정지 가능성)")

    async def _loop_health_watchdog(self, interval: float = 1.0, warn_slack_ms: float = 800.0):
        last = time.perf_counter()
        while True:
            await asyncio.sleep(interval)
            now = time.perf_counter()
            dt_ms = (now - last) * 1000.0
            last = now
            expected = interval * 1000.0
            if dt_ms > expected + warn_slack_ms:
                live = [t for t in getattr(self, "_bg_tasks", []) if t and not t.done()]
                self.append_log("WD.Asyncio", f"이벤트루프 지연 {dt_ms:.0f} ms (bg_tasks={len(live)})")

    def _mark_pump_event(self, tag: str):
        if not hasattr(self, "_last_pump_evt"):
            self._last_pump_evt = {}
        self._last_pump_evt[tag] = time.perf_counter()

    async def _pump_idle_watchdog(self, tag: str, threshold_s: float = 5.0):
        self._mark_pump_event(tag)
        while True:
            await asyncio.sleep(1.0)
            last = self._last_pump_evt.get(tag, 0.0)
            if time.perf_counter() - last > threshold_s:
                self.append_log("WD.Pump", f"{tag} no events for >{threshold_s:.0f}s")
                # 경고 후 리셋(로그 스팸 방지)
                self._last_pump_evt[tag] = time.perf_counter()
    # =============== debug ==============

    # ------------------------------------------------------------------
    # UI 버튼 연결만 유지 (컨트롤러 ↔ UI는 이벤트 큐로 처리)
    # ------------------------------------------------------------------
    def _connect_ui_signals(self) -> None:
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
    def _switch_page(self, key: Literal["pc", "ch1", "ch2"]) -> None:
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
    async def _pump_pc_events(self) -> None:
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
                    # 세션 파일이 없을 때만 생성. 이미 있으면 그대로 사용.
                    if not getattr(self, "_log_file_path", None):
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        self._log_file_path = self._log_dir / f"{ts}.txt"
                        if self._prestart_buf:
                            buf_copy = list(self._prestart_buf)
                            self._prestart_buf.clear()
                            try:
                                loop = asyncio.get_running_loop()
                            except RuntimeError:
                                loop = self._loop
                            self._spawn_detached(self._dump_prestart_buf_async(self._log_file_path, buf_copy))

                    # DataLogger/그래프/알림 등 나머지 로직은 그대로
                    try:
                        self.data_logger.start_new_log_session(payload.get("params", {}))
                    except Exception:
                        pass
                    self._soon(self.graph_controller.reset)

                    if self.chat_notifier:
                        try:
                            self.chat_notifier.notify_process_started(payload.get("params", {}))
                        except Exception:
                            pass
                    self._last_polling_targets = None
                elif kind == "finished":
                    ok = bool(payload.get("ok", False))
                    detail = payload.get("detail", {}) or {}

                    # ✅ (선택) process_ch2.py가 ok_for_log를 줄 수도 있으니 우선 사용, 없으면 ok
                    ok_for_log = bool(detail.get("ok_for_log", ok))

                    # 1) DataLogger를 먼저 마무리 (성공 시만 기록하는 정책은 내부에서 ok로 필터링)
                    try:
                        self.data_logger.finalize_and_write_log(ok_for_log)
                    except TypeError:
                        try:
                            self.data_logger.finalize_and_write_log()
                        except Exception as e:
                            self.append_log("Logger", f"DataLogger finalize 예외: {e!r}")

                    # 2) ✨ I/O/NAS 환경 고려: 아주 짧게 이벤트 루프에 양보
                    await asyncio.sleep(0.20)  # 0.2~0.3 권장 (NAS면 0.3까지 고려)

                    # 3) 그 다음에 후처리/다음 공정 진행
                    if self.chat_notifier:
                        try:
                            self.chat_notifier.notify_process_finished_detail(ok, detail)
                        except Exception:
                            pass
                    try:
                        self.mfc.on_process_finished(ok)
                    except Exception:
                        pass

                    # ✅ Stop 경로(사용자 중단 포함)에서는 UI를 즉시 초기화
                    if getattr(self, "_pc_stopping", False):
                        try:
                            self._clear_queue_and_reset_ui()
                        except Exception:
                            pass
                        self._last_polling_targets = None

                    if getattr(self, "_pending_device_cleanup", False):
                        try:
                            # ✅ 여기서 기다리지 말고 예약만
                            self._spawn_detached(self._stop_device_watchdogs(light=False), name="FullCleanup")
                        except Exception:
                            pass
                        self._pending_device_cleanup = False
                        # 종료 플로우이면 다음 공정 안 돌리고 펌프 루프 종료
                        self._pc_stopping = False
                        break  # ← while True 탈출(이 펌프 태스크는 정상 종료)

                    self._pc_stopping = False

                    # 자동 큐 진행
                    self._start_next_process_from_queue(ok)
                    self._last_polling_targets = None
                elif kind == "aborted":
                    if self.chat_notifier:
                        try:
                            self.chat_notifier.notify_text("🛑 공정이 중단되었습니다.")
                        except Exception:
                            pass
                    # ✅ 공정 중단 시에도 큐/상태 정리 + UI 초기화
                    try:
                        self._clear_queue_and_reset_ui()
                    except Exception:
                        pass

                    # (선택) 종료 대기 중이던 워치독 정리까지
                    if getattr(self, "_pending_device_cleanup", False):
                        try:
                            # 기다리지 말고 예약만
                            self._spawn_detached(self._stop_device_watchdogs(light=False), name="FullCleanup")
                        except Exception:
                            pass
                        self._pending_device_cleanup = False
                        self._pc_stopping = False
                        break  # ← 이벤트 펌프 종료(“finished”와 동일한 패턴)

                elif kind == "polling_targets":
                    targets = dict(payload.get("targets") or {})   # ← 올바른 접근
                    self._last_polling_targets = targets
                    self._apply_polling_targets(targets)
                elif kind == "polling":
                    active = bool(payload.get("active", False))
                    self._ensure_background_started()

                    targets = getattr(self, "_last_polling_targets", None)
                    if not targets:
                        params = getattr(self.process_controller, "current_params", {}) or {}
                        use_rf_pulse = bool(params.get("use_rf_pulse", False))
                        use_dc       = bool(params.get("use_dc_power", False))
                        use_rf       = bool(params.get("use_rf_power", False))
                        targets = {
                            "mfc":     True,
                            "rfpulse": use_rf_pulse,
                            "dc":      use_dc and not use_rf_pulse,  # Pulse만 쓰면 DC 폴링 OFF
                            "rf":      use_rf and not use_rf_pulse,  # Pulse만 쓰면 RF 폴링 OFF
                        }
                    else:
                        targets = {
                            "mfc":     (active and bool(targets.get("mfc", False))),
                            "rfpulse": (active and bool(targets.get("rfpulse", False))),
                            "dc":      (active and bool(targets.get("dc", False))),
                            "rf":      (active and bool(targets.get("rf", False))),
                        }

                    self._apply_polling_targets(targets)
                else:
                    # 미지정 이벤트도 안전하게 무시/로그
                    self.append_log("MAIN", f"알 수 없는 PC 이벤트 수신: {kind} {payload}")
            except Exception as e:
                # 💡 핵심: 여기서 잡고 계속 돈다(펌프가 죽지 않음)
                self.append_log("MAIN", f"PC 이벤트 처리 예외: {e!r} (kind={kind})")
            finally:
                # Qt 페인팅/타이머/다른 코루틴에 양보
                await asyncio.sleep(0)

    # ------------------------------------------------------------------
    # 비동기 이벤트 펌프 (장치 → ProcessController)
    # ------------------------------------------------------------------
    async def _pump_mfc_events(self) -> None:
        # =============== debug ==============
        self.append_log("DBG", "Pump.MFC started")
        # =============== debug ==============
        async for ev in self.mfc.events():
            k = ev.kind
            # =============== debug ==============
            self._mark_pump_event("MFC")
            # =============== debug ==============
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
                if self._verbose_polling_log:
                    self.append_log("MFC", f"[poll] {gas}: {flow:.2f} sccm")

            elif k == "pressure":
                txt = ev.text or (f"{ev.value:.3g}" if ev.value is not None else "")
                try:
                    self.data_logger.log_mfc_pressure(txt)
                except Exception:
                    pass
                if self._verbose_polling_log:
                    self.append_log("MFC", f"[poll] ChamberP: {txt}")

    async def _pump_ig_events(self) -> None:
        # =============== debug ==============
        self.append_log("DBG", "Pump.IG started")
        # =============== debug ==============
        async for ev in self.ig.events():
            # =============== debug ==============
            self._mark_pump_event("IG")
            # =============== debug ==============
            k = ev.kind
            if k == "status":
                self.append_log("IG", ev.message or "")
            elif k == "pressure":
                try:
                    if ev.pressure is not None:
                        self.data_logger.log_ig_pressure(float(ev.pressure))
                    else:
                        # message로만 온 경우 간단히 로깅
                        if ev.message:
                            self.data_logger.log_ig_pressure(ev.message)
                except Exception:
                    pass
            elif k == "base_reached":
                self.process_controller.on_ig_ok()
            elif k == "base_failed":
                why = ev.message or "unknown"
                self.process_controller.on_ig_failed("IG", why)
                if self.chat_notifier:
                    self.chat_notifier.notify_error_with_src("IG", why)

    async def _pump_rga_events_ch(self, adapter: RGA100AsyncAdapter, ch: int) -> None:
        # =============== debug ==============
        self.append_log("DBG", f"Pump.RGA{ch} started")
        # =============== debug ==============
        tag = f"RGA{ch}"
        async for ev in adapter.events():
            # =============== debug ==============
            self._mark_pump_event(tag)
            # =============== debug ==============
            if ev.kind == "status":
                self.append_log(tag, ev.message or "")

            elif ev.kind == "data":
                # ✅ CH2만 그리기 + 그린 '직후' 완료 토큰(1회)
                if ch == 2:
                    def _draw_then_finish(x=ev.mass_axis, y=ev.pressures):
                        try:
                            self.graph_controller.update_rga_plot(x, y)
                        finally:
                            if not self._rga_done_signaled:
                                self._rga_done_signaled = True
                                self.process_controller.on_rga_finished()
                    self._soon(_draw_then_finish)
                else:
                    # CH1 데이터는 무시(현재 CH1 미구현)
                    pass

            elif ev.kind == "finished":
                # 기존 로그 유지
                self.append_log(tag, ev.message or "scan finished")
                # 🔧 안전망: data 없이 finished만 온 경우에도 다음 단계로 진행
                if ch == 2 and not self._rga_done_signaled:
                    self._rga_done_signaled = True
                    self.process_controller.on_rga_finished()

            elif ev.kind == "failed":
                # ✅ CH2 스캔 실패 → 로그 남기고 '완료' 토큰으로 다음 단계 진행 (소프트 실패)
                why = ev.message or "RGA failed"
                if ch == 2:
                    self.append_log(tag, f"측정 실패: {why} → 다음 단계로 건너뜀")
                    if self.chat_notifier:
                        # 원하면 경고성 알림만 남기고 '에러'로 치진 않도록 메시지 톤만 조정
                        try:
                            self.chat_notifier.notify_text(f"[{tag}] 측정 실패: {why} → 다음 단계로 건너뜀")
                        except Exception:
                            pass
                    # 중복 시그널 방지
                    if not self._rga_done_signaled:
                        self._rga_done_signaled = True
                        self.process_controller.on_rga_finished()
                else:
                    self.append_log(tag, f"CH1 이벤트 무시(실패): {why}")

    async def _pump_dc_events(self) -> None:
        # =============== debug ==============
        self.append_log("DBG", "Pump.DC started")
        # =============== debug ==============
        async for ev in self.dc_power.events():
            # =============== debug ==============
            self._mark_pump_event("DC")
            # =============== debug ==============
            k = ev.kind
            if k == "status":
                self.append_log("DCpower", ev.message or "")
            elif k == "target_reached":
                self.process_controller.on_dc_target_reached()
            elif k == "power_off_finished":
                self.process_controller.on_device_step_ok()

    async def _pump_rf_events(self) -> None:
        # =============== debug ==============
        self.append_log("DBG", "Pump.RF started")
        # =============== debug ==============
        async for ev in self.rf_power.events():
            # =============== debug ==============
            self._mark_pump_event("RF")
            # =============== debug ==============
            k = ev.kind
            if k == "status":
                self.append_log("RFpower", ev.message or "")
            elif k == "display":
                # ← forward/reflected 즉시 UI 반영
                self.handle_rf_power_display(ev.forward, ev.reflected)
            elif k == "target_reached":
                self.process_controller.on_rf_target_reached()
            elif k == "target_failed":
                why = ev.message or "unknown"
                self.process_controller.on_rf_target_failed(why)
                if self.chat_notifier:
                    self.chat_notifier.notify_error_with_src("RF Power", why)
            elif k == "power_off_finished":
                self.process_controller.on_device_step_ok()

    async def _pump_rfpulse_events(self) -> None:
        # =============== debug ==============
        self.append_log("DBG", "Pump.RFPulse started")
        # =============== debug ==============
        async for ev in self.rf_pulse.events():
            # =============== debug ==============
            self._mark_pump_event("RFPulse")
            # =============== debug ==============
            k = ev.kind
            if k == "status":
                self.append_log("RFPulse", ev.message or "")
            elif k == "power":
                # 드라이버가 주기적으로 뿌리는 forward/reflected 값
                try:
                    fwd = float(ev.forward or 0.0)
                    ref = float(ev.reflected or 0.0)
                    self.data_logger.log_rfpulse_power(fwd, ref)
                except Exception:
                    pass
                # 필요하면 UI에도 반영:
                # self.handle_rfpulse_power_display(fwd, ref)
            elif k == "target_reached":
                self.process_controller.on_rf_target_reached()
            elif k == "command_failed":
                why = ev.reason or "unknown"
                self.process_controller.on_rf_pulse_failed(why)
                if self.chat_notifier:
                    self.chat_notifier.notify_error_with_src("RF Pulse", why)
            elif k == "power_off_finished":
                self.process_controller.on_rf_pulse_off_finished()
            elif k == "rf_status":
                st = ev.rfstatus
                # 필요하면 상태 텍스트로 로그/표시
                if st is not None:
                    self.append_log("RFPulse", f"STATUS on={int(st.rf_output_on)} req={int(st.rf_on_requested)} ...")

    async def _pump_oes_events(self) -> None:
        # =============== debug ==============
        self.append_log("DBG", "Pump.OES started")
        # =============== debug ==============
        async for ev in self.oes.events():
            # =============== debug ==============
            self._mark_pump_event("OES")
            # =============== debug ==============
            try:
                k = getattr(ev, "kind", None)
                if k == "status":
                    self.append_log("OES", ev.message or "")
                    continue

                if k in ("data", "spectrum", "frame"):
                    # 필드 호환 (x/y → wavelengths/intensities → lambda/counts 등)
                    x = getattr(ev, "x", None)
                    y = getattr(ev, "y", None)
                    if x is None or y is None:
                        x = getattr(ev, "wavelengths", getattr(ev, "lambda_axis", None))
                        y = getattr(ev, "intensities", getattr(ev, "counts", None))

                    if x is not None and y is not None:
                        # 1) 그래프 업데이트 (그대로 유지)
                        self._post_update_oes_plot(x, y)
                    else:
                        self.append_log("OES", f"경고: 데이터 필드 없음: {ev!r}")
                    continue

                if k == "finished":
                    if bool(getattr(ev, "success", False)):
                        self.process_controller.on_oes_ok()
                    else:
                        why = getattr(ev, "message", "measure failed")
                        self.process_controller.on_oes_failed("OES", why)
                        if self.chat_notifier:
                            self.chat_notifier.notify_error_with_src("OES", why)
                    continue

                self.append_log("OES", f"알 수 없는 이벤트: {ev!r}")

            except Exception as e:
                # 💡 핵심: 예외가 나도 펌프 태스크가 죽지 않도록
                self.append_log("OES", f"이벤트 처리 예외: {e!r}")
                continue

    # ------------------------------------------------------------------
    # RGA dummy
    # ------------------------------------------------------------------
    def cb_rga_scan(self):
        async def _run():
            try:
                self._ensure_background_started()
                self._rga_done_signaled = False
                self._soon(self.graph_controller.clear_rga_plot)
                self.append_log("DBG", "RGA: scan_histogram_to_csv() call")
                await self.rga_ch2.scan_histogram_to_csv(RGA_CSV_PATH["ch2"])
                self.append_log("DBG", "RGA: scan_histogram_to_csv() returned")
                # 완료 토큰은 data/finished 쪽에서 처리
            except Exception as e:
                # 🔧 여기서도 '실패'를 '소프트 실패'로 처리: 로그 + 다음 단계 진행
                msg = f"예외로 RGA 스캔 실패: {e!r} → 다음 단계로 건너뜀"
                self.append_log("RGA", msg)
                if self.chat_notifier:
                    try:
                        self.chat_notifier.notify_text(f"[RGA] {msg}")
                    except Exception:
                        pass
                if not self._rga_done_signaled:
                    self._rga_done_signaled = True
                    self.process_controller.on_rga_finished()
        self._spawn_detached(_run())

    # ------------------------------------------------------------------
    # 백그라운 태스크 시작 함수
    # ------------------------------------------------------------------
    def _ensure_task_alive(self, name: str, coro_factory: Callable[[], Coroutine[Any, Any, Any]]) -> None:
        """이름으로 태스크가 살아있는지 확인하고 없으면 새로 띄움(중복 방지)."""
        # 죽은 태스크는 리스트에서 제거
        self._bg_tasks = [t for t in getattr(self, "_bg_tasks", []) if t and not t.done()]
        for t in self._bg_tasks:
            try:
                if t.get_name() == name and not t.done():
                    return  # 이미 살아있음
            except Exception:
                pass
        # 없으면 새로 생성
        self._spawn_detached(coro_factory(), store=True, name=name)

    def _ensure_background_started(self) -> None:
        # 언제 불려도 안전하게 "필수 태스크가 살아있음"을 보장
        self._ensure_task_alive("MFC.start",      self.mfc.start)
        self._ensure_task_alive("IG.start",       self.ig.start)
        self._ensure_task_alive("RFPulse.start",  self.rf_pulse.start)
        # ✅ PLC 연결
        self._ensure_task_alive("PLC.connect",     self.plc.connect)

        self._ensure_task_alive("Pump.MFC",       self._pump_mfc_events)
        self._ensure_task_alive("Pump.IG",        self._pump_ig_events)
        # ✅ RGA 두 대 펌프 기동
        self._ensure_task_alive("Pump.RGA1",      lambda: self._pump_rga_events_ch(self.rga_ch1, 1))
        self._ensure_task_alive("Pump.RGA2",      lambda: self._pump_rga_events_ch(self.rga_ch2, 2))
        self._ensure_task_alive("Pump.DC",        self._pump_dc_events)
        self._ensure_task_alive("Pump.RF",        self._pump_rf_events)
        self._ensure_task_alive("Pump.RFPulse",   self._pump_rfpulse_events)
        self._ensure_task_alive("Pump.OES",       self._pump_oes_events)
        self._ensure_task_alive("Pump.PC",        self._pump_pc_events)

        # =============== debug ==============
        # 🔧 펌프 idle 워치독(필요한 채널만 선택)
        self._ensure_task_alive("WD.Pump.RGA2", lambda: self._pump_idle_watchdog("RGA2"))
        self._ensure_task_alive("WD.Pump.MFC",  lambda: self._pump_idle_watchdog("MFC"))
        self._ensure_task_alive("WD.Pump.IG",   lambda: self._pump_idle_watchdog("IG"))
        # 필요 시 추가:
        self._ensure_task_alive("WD.Pump.OES",  lambda: self._pump_idle_watchdog("OES"))
        self._ensure_task_alive("WD.Pump.RF",   lambda: self._pump_idle_watchdog("RF"))
        self._ensure_task_alive("WD.Pump.DC",   lambda: self._pump_idle_watchdog("DC"))
        self._ensure_task_alive("WD.Pump.RFPulse", lambda: self._pump_idle_watchdog("RFPulse"))
        # =============== debug ==============

        self._bg_started = True  # 플래그는 호환을 위해 유지

    # ------------------------------------------------------------------
    # 표시/입력 관련
    # ------------------------------------------------------------------
    def handle_rf_power_display(self, for_p: Optional[float], ref_p: Optional[float]) -> None:
        if for_p is None or ref_p is None:
            self.append_log("MAIN", "for.p, ref.p 값이 비어있습니다.")
            return
        self._soon(self.ui.ch2_forP_edit.setPlainText, f"{for_p:.2f}")
        self._soon(self.ui.ch2_refP_edit.setPlainText, f"{ref_p:.2f}")

    def handle_dc_power_display(self, power: Optional[float], voltage: Optional[float], current: Optional[float]) -> None:
        if power is None or voltage is None or current is None:
            self.append_log("MAIN", "power, voltage, current값이 비어있습니다.")
            return
        self._soon(self.ui.ch2_Power_edit.setPlainText,   f"{power:.3f}")
        self._soon(self.ui.ch2_Voltage_edit.setPlainText, f"{voltage:.3f}")
        self._soon(self.ui.ch2_Current_edit.setPlainText, f"{current:.3f}")

    def _on_process_status_changed(self, running: bool) -> None:
        self.ui.ch2_Start_button.setEnabled(not running)
        self.ui.ch2_Stop_button.setEnabled(True)

    # ------------------------------------------------------------------
    # 파일 로딩 / 파라미터 UI 반영
    # ------------------------------------------------------------------
    def _handle_process_list_clicked(self, _checked: bool = False) -> None:
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
                    self.process_queue.append(cast(RawParams, row))

                if not self.process_queue:
                    self.append_log("File", "파일에 처리할 공정이 없습니다.")
                    return
                self.append_log("File", f"총 {len(self.process_queue)}개의 공정을 파일에서 읽었습니다.")
                self._update_ui_from_params(self.process_queue[0])
        except Exception as e:
            self.append_log("File", f"파일 처리 중 오류 발생: {e}")

    def _update_ui_from_params(self, params: RawParams) -> None:
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
        self.ui.ch2_G3_checkbox.setChecked(params.get('gun3', 'F') == 'T')  # UI 이름 그대로 사용
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
    def _start_next_process_from_queue(self, was_successful: bool) -> None:
        if getattr(self, "_advancing", False):
            self.append_log("MAIN", "자동 진행 중복 호출 무시")
            return
        self._advancing = True
        try:
            # === 기존 본문 그대로 ===
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
                if self._try_handle_delay_step(params):
                    return
                norm = self._normalize_params_for_process(params)

                if not getattr(self, "_log_file_path", None):
                    self._prepare_log_file(norm)
                else:
                    self.append_log("Logger", f"같은 세션 파일 계속 사용: {self._log_file_path.name}")

                self._spawn_detached(self._start_process_later(params, 0.25))
            else:
                self.append_log("MAIN", "모든 공정이 완료되었습니다.")
                self._clear_queue_and_reset_ui()
        finally:
            self._advancing = False

    async def _start_process_later(self, params: RawParams, delay_s: float = 0.1) -> None:
        await asyncio.sleep(delay_s)
        self._safe_start_process(self._normalize_params_for_process(params))

    def _safe_start_process(self, params: NormParams) -> None:
        if self.process_controller.is_running:
            self.append_log("MAIN", "경고: 이미 다른 공정이 실행 중이므로 새 공정을 시작하지 않습니다.")
            return
        # 프리플라이트(연결 확인) → 완료 후 공정 시작
        self._spawn_detached(self._start_after_preflight(params))

    # (MainWindow 클래스 내부)
    # 1) 재진입 안전한 비모달 표출 유틸을 "메서드"로 추가
    def _post_critical(self, title: str, text: str) -> None:
        self._soon(QMessageBox.critical, self, title, text)

    # 2) async 함수 안의 모달 호출을 유틸로 교체
    async def _start_after_preflight(self, params: NormParams) -> None:
        try:
            self.append_log("DBG", "PF: entering start_after_preflight")
            self._ensure_background_started()
            self.append_log("DBG", "PF: background started ensured")

            # 프리플라이트 동안 Start 비활성화(중복 클릭 방지)
            self._on_process_status_changed(True)

            # 타임아웃은 짧게 고정: RF Pulse 사용 시 10초, 아니면 8초
            use_rf_pulse: bool = bool(params.get("use_rf_pulse", False))
            timeout = 10.0 if use_rf_pulse else 8.0

            # =============== debug ==============
            self.append_log("DBG", "PF: preflight_connect() about to await")
            ok, failed = await self._preflight_connect(params, timeout_s=timeout)
            self.append_log("DBG", f"PF: preflight_connect() returned ok={ok} failed={failed}")
            # =============== debug ==============

            if not ok:
                fail_list = ", ".join(failed) if failed else "알 수 없음"
                self.append_log("MAIN", f"필수 장비 연결 실패: {fail_list} → 공정 시작 중단")

                self._post_critical(
                    "장비 연결 실패",
                    f"다음 장비 연결을 확인하지 못했습니다:\n - {fail_list}\n\n"
                    "케이블/전원/포트 설정을 확인한 뒤 다시 시도하세요."
                )
                # 실패 시 버튼 즉시 복구
                self._on_process_status_changed(False)
                self._start_next_process_from_queue(False)
                return
            
            # ✅ 전환 전 캐시 초기화
            self._last_polling_targets = None

            self.append_log("MAIN", "장비 연결 확인 완료 → 공정 시작")
            self.process_controller.start_process(params)

        except Exception as e:
            note = params.get("process_note", "알 수 없는")
            msg = f"오류: '{note}' 공정 시작에 실패했습니다. ({e})"
            self.append_log("MAIN", msg)
            self._post_critical("오류", msg)
            self._start_next_process_from_queue(False)
            # 예외 시 버튼 복구
            self._on_process_status_changed(False)


    async def _wait_device_connected(self, dev: object, name: str, timeout_s: float) -> bool:
        """장비 워치독이 실제로 붙을 때까지(공개 API 우선) 대기"""
        try:
            t0 = asyncio.get_running_loop().time()
        except RuntimeError:
            t0 = 0.0

        # =============== debug ==============
        self.append_log("DBG", f"WAIT:{name} start (timeout={timeout_s}s)")
        # =============== debug ==============

        while True:
            if self._is_dev_connected(dev):
                self.append_log(name, "연결 성공")
                return True
            try:
                now = asyncio.get_running_loop().time()
            except RuntimeError:
                now = t0 + timeout_s + 1.0
            if now - t0 >= timeout_s:
                self.append_log(name, "연결 확인 실패(타임아웃)")
                return False
            await asyncio.sleep(0.2)

    async def _preflight_connect(self, params: ParamsMap, timeout_s: float = 8.0) -> tuple[bool, list[str]]:
        """
        필수 장비 연결 대기.
        - 기본 필수: MFC, IG, PLC
        - 선택 필수: RF Pulse 사용 시 RFPulse 포함
        """
        need: list[tuple[str, object]] = [
            ("PLC", self.plc),          # ✅ 릴레이 제어용(가스/셔터/밸브)
            ("MFC", self.mfc),
            ("IG", self.ig),
        ]
        try:
            use_rf_pulse = bool(params.get("use_rf_pulse", False) or params.get("use_rf_pulse_power", False))
        except Exception:
            use_rf_pulse = False
        if use_rf_pulse:
            need.append(("RFPulse", self.rf_pulse))

        # 진행 로그 태스크 시작
        stop_evt = asyncio.Event()
        prog_task = asyncio.create_task(self._preflight_progress_log(need, stop_evt))

        try:
            results = await asyncio.gather(
                *[self._wait_device_connected(dev, name, timeout_s) for name, dev in need],
                return_exceptions=False
            )
        finally:
            stop_evt.set()
            with contextlib.suppress(Exception):
                await prog_task

        failed = [name for (name, _), ok in zip(need, results) if not ok]
        return (len(failed) == 0, failed)


    # ------------------------------------------------------------------
    # 단일 실행
    # ------------------------------------------------------------------
    def _handle_start_clicked(self, _checked: bool = False):
        if self.process_controller.is_running:
            QMessageBox.warning(self, "실행 오류", "현재 다른 공정이 실행 중입니다.")
            return
        
        # 자동 시퀀스
        if self.process_queue:
            # 세션 파일이 아직 없으면 첫 공정 이름으로 노트 달아서 선생성
            if not getattr(self, "_log_file_path", None):
                first = self.process_queue[0] if self.process_queue else {}
                note = f"AutoRun: {first.get('Process_name', 'Run')}"
                self._prepare_log_file({"process_note": note})

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
        params_norm = cast(NormParams, params)
        self._safe_start_process(params_norm)

    # ------------------------------------------------------------------
    # STOP/종료 (단일 경로)
    # ------------------------------------------------------------------
    def _handle_stop_clicked(self, _checked: bool = False):
        self.request_stop_all(user_initiated=True)

    def request_stop_all(self, user_initiated: bool):
        # 지연(step: delay N s/m/h) 예약 취소
        self._cancel_delay_task()

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
        self._spawn_detached(self._stop_device_watchdogs(light=True))

        # 이번에 종료 절차에 진입
        self._pc_stopping = True
        self._pending_device_cleanup = True  # 풀 cleanup은 공정 종료 후로 미룸

        # 1) 공정 종료만 지시 (11단계)
        self.process_controller.request_stop()

    async def _stop_device_watchdogs(self, *, light: bool = False) -> None:
        """
        light=True : 폴링만 즉시 중지(연결은 유지, 포트 닫지 않음)
        light=False: 전체 정리(이벤트 펌프/워커/워치독 취소 + cleanup)
        """
        self.append_log("DBG", f"STOP WD: light={light} entering")

        if light:
            # 폴링만 중지 (연결/워커 유지)
            try:
                self.mfc.set_process_status(False)
            except Exception:
                pass
            try:
                self.rf_pulse.set_process_status(False)
            except Exception:
                pass
            try:
                if hasattr(self.dc_power, "set_process_status"):
                    self.dc_power.set_process_status(False)
            except Exception:
                pass
            try:
                if hasattr(self.rf_power, "set_process_status"):
                    self.rf_power.set_process_status(False)
            except Exception:
                pass
            return

        # ===== 기존 전체 정리 경로 =====
        # 0) 이벤트 펌프/백그라운드 태스크 먼저 취소 → 같은 틱 재귀 취소 방지
        loop = self._loop_from_anywhere()
        try:
            current = asyncio.current_task()
            live = [t for t in getattr(self, "_bg_tasks", []) if t and not t.done() and t is not current]
            for t in live:
                loop.call_soon(t.cancel)
            if live:
                await asyncio.gather(*live, return_exceptions=True)
        finally:
            self._bg_tasks = []

        # ▶ IG는 OFF 보장을 먼저 '대기'해서 끝내 둠(중복 OFF는 무해)
        try:
            if self.ig and hasattr(self.ig, "cancel_wait"):
                try:
                    await asyncio.wait_for(self.ig.cancel_wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
        except Exception:
            pass

        # 1) 장치 워치독/워커 정리(재연결 억제)
        tasks = []
        for dev in (self.ig, self.mfc, self.rf_pulse, self.dc_power, self.rf_power, self.oes,
                    getattr(self, "rga_ch1", None), getattr(self, "rga_ch2", None)):
            if dev and hasattr(dev, "cleanup"):
                try:
                    tasks.append(dev.cleanup())
                except Exception:
                    pass

        # ✅ PLC는 cleanup이 아니라 close()
        try:
            if self.plc and hasattr(self.plc, "close"):
                tasks.append(self.plc.close())
        except Exception:
            pass

        if tasks:
            self.append_log("DBG", f"STOP WD: gather {len(tasks)} tasks")
            await asyncio.gather(*tasks, return_exceptions=True)
            self.append_log("DBG", "STOP WD: gather returned")

        try:
            if getattr(self, "tsp_ctrl", None) and hasattr(self.tsp_ctrl, "aclose"):
                await self.tsp_ctrl.aclose()
                self.append_log("TSP", "포트 닫힘")
        except Exception:
            pass
        
        # 2) 다음 Start에서만 다시 올리도록 플래그 리셋
        self._bg_started = False

    # ------------------------------------------------------------------
    # 로그
    # ------------------------------------------------------------------
    def append_log(self, source: str, msg: str) -> None:
        now_ui = datetime.now().strftime("%H:%M:%S")
        now_file = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line_ui = f"[{now_ui}] [{source}] {msg}"
        line_file = f"[{now_file}] [{source}] {msg}\n"

        # 1) UI는 항상 즉시, 비블로킹으로
        self._soon(self._append_log_to_ui, line_ui)

        # 2) 파일 경로가 아직 없으면 버퍼에만 쌓고 끝
        if not getattr(self, "_log_file_path", None):
            try:
                self._prestart_buf.append(line_file)
            except Exception:
                pass
            return

        # 3) 파일 쓰기는 절대 동기로 하지 말 것 → 전용 스레드로 오프로딩
        self._spawn_detached(self._write_log_line_async(self._log_file_path, line_file))

    async def _write_log_line_async(self, path: Path, text: str) -> None:
        await asyncio.to_thread(self._write_log_line_sync, path, text)

    def _write_log_line_sync(self, path: Path, text: str) -> None:
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(text)
        except Exception as e:
            self._soon(self.ui.ch2_logMessage_edit.appendPlainText, f"[Logger] 파일 기록 실패: {e}")

    def _append_log_to_ui(self, line: str) -> None:
        self.ui.ch2_logMessage_edit.moveCursor(QTextCursor.MoveOperation.End)
        self.ui.ch2_logMessage_edit.insertPlainText(line + "\n")

    # === [추가] 새 로그 파일을 프리플라이트 전에 준비 ===
    def _prepare_log_file(self, params: ParamsMap) -> None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._log_file_path = self._log_dir / f"{ts}.txt"

        # Start 이전 버퍼는 전용 스레드에서 한 번에 기록
        if self._prestart_buf:
            buf_copy = list(self._prestart_buf)
            self._prestart_buf.clear()
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = self._loop
            self._spawn_detached(self._dump_prestart_buf_async(self._log_file_path, buf_copy))

        self.append_log("Logger", f"새 로그 파일 시작: {self._log_file_path}")
        note = str(params.get("process_note", "") or params.get("Process_name", "") or "Run")
        self.append_log("MAIN", f"=== '{note}' 공정 준비 (장비 연결부터 기록) ===")

    async def _dump_prestart_buf_async(self, path: Path, lines: list[str]) -> None:
        def _write_many():
            with open(path, "a", encoding="utf-8") as f:
                f.writelines(lines)
        try:
            await asyncio.to_thread(_write_many)
        except Exception:
            self._soon(self.ui.ch2_logMessage_edit.appendPlainText, "[Logger] 버퍼 덤프 실패")


    # ------------------------------------------------------------------
    # 폴링/상태
    # ------------------------------------------------------------------
    def _apply_polling_targets(self, targets: TargetsMap) -> None:
        self._ensure_background_started()

        mfc_on = bool(targets.get('mfc', False))
        rfp_on = bool(targets.get('rfpulse', False))
        dc_on  = bool(targets.get('dc', False))
        rf_on  = bool(targets.get('rf', False))

        try:
            self.mfc.set_process_status(mfc_on)
        except Exception as e:
            self.append_log("MFC", f"폴링 토글 실패: {e}")

        try:
            self.rf_pulse.set_process_status(rfp_on)
        except Exception as e:
            self.append_log("RFPulse", f"폴링 토글 실패: {e}")

        try:
            if hasattr(self.dc_power, "set_process_status"):
                self.dc_power.set_process_status(dc_on)
        except Exception as e:
            self.append_log("DCpower", f"폴링 토글 실패: {e}")

        try:
            if hasattr(self.rf_power, "set_process_status"):
                self.rf_power.set_process_status(rf_on)
        except Exception as e:
            self.append_log("RFpower", f"폴링 토글 실패: {e}")

    def _apply_process_state_message(self, message: str) -> None:
        # 같은 텍스트면 스킵(불필요한 repaint 방지)
        if getattr(self, "_last_state_text", None) == message:
            return
        self._last_state_text = message

        self._soon(self.ui.ch2_processState_edit.setPlainText, message)

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
        # 1) 입력 위젯 기본값 적용
        self._set_default_ui_values()

        # 2) 체크박스 일괄 OFF 
        checkbox_names = (
            "ch2_G1_checkbox", "ch2_G2_checkbox", "ch2_G3_checkbox",
            "ch2_Ar_checkbox", "ch2_O2_checkbox", "ch2_N2_checkbox",
            "ch2_mainShutter_checkbox", "ch2_rfPulsePower_checkbox",
            "ch2_dcPower_checkbox", "ch2_powerSelect_checkbox",
        )
        for name in checkbox_names:
            cb = getattr(self.ui, name, None)
            if cb is not None:
                cb.setChecked(False)

        # 3) 상태 라벨/표시값 초기화
        self.ui.ch2_processState_edit.setPlainText("대기 중")
        self.ui.ch2_Power_edit.setPlainText("")
        self.ui.ch2_Voltage_edit.setPlainText("")
        self.ui.ch2_Current_edit.setPlainText("")
        self.ui.ch2_forP_edit.setPlainText("")
        self.ui.ch2_refP_edit.setPlainText("")

        # 4) 버튼 상태: 공정 미실행 상태로(Start=활성, Stop=비활성)
        self._on_process_status_changed(False)

        # 5) 그래프도 초기화(다음 런을 위해 깨끗하게)
        if hasattr(self, "graph_controller") and self.graph_controller:
            try:
                self.graph_controller.reset()
            except Exception:
                pass

    def _clear_queue_and_reset_ui(self) -> None:
        self.process_queue = []
        self.current_process_index = -1
        self._reset_ui_after_process()
        # 다음 실행부터는 새 파일로 시작
        self._log_file_path = None
        # ✅ Start 전 로그 버퍼도 초기화(세션 간 혼입 방지)
        try:
            self._prestart_buf.clear()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 종료/정리(단일 경로)
    # ------------------------------------------------------------------
    def closeEvent(self, event: QCloseEvent) -> None:
        self.append_log("MAIN", "프로그램 창 닫힘 → 빠른 종료 경로 진입(장비 명령 전송 없음).")
        self._spawn_detached(self._fast_quit())
        event.accept()
        super().closeEvent(event)

    def _shutdown_once(self, reason: str) -> None:
        if self._shutdown_called:
            return
        self._shutdown_called = True
        self.append_log("MAIN", f"종료 시퀀스({reason}) 시작")

        # 종료 중 예약된 delay 타이머가 뒤늦게 시작되는 것 방지
        self._cancel_delay_task()

        # 1) Stop 요청(라이트 정지 + 종료 11단계)
        self.request_stop_all(user_initiated=False)

        # 2) 즉시 cleanup()은 호출하지 않음.
        #    finished/aborted 이벤트에서 _stop_device_watchdogs(light=False)로 한 번에 정리.
        #    (안전장치) 그래도 10초 내에 종료 이벤트가 안 오면 강제 정리
        async def _force_cleanup_after(sec: float):
            await asyncio.sleep(sec)
            if getattr(self, "_pending_device_cleanup", False):
                self.append_log("MAIN", "강제 정리 타임아웃 → 풀 cleanup 강제 수행")
                await self._stop_device_watchdogs(light=False)
                self._pending_device_cleanup = False
                self._pc_stopping = False
                asyncio.get_running_loop().call_soon(QCoreApplication.quit)

        # 이전 태스크 취소 후 새로 스케줄
        if self._force_cleanup_task and not self._force_cleanup_task.done():
            self._force_cleanup_task.cancel()
        # ✅ 종료 스텝 확인 타임아웃(예: 2.5s) × 여러 스텝 고려 → 약 30s 권장
        self._set_task_later("_force_cleanup_task", _force_cleanup_after(30.0), name="ForceCleanup")

        # 3) Chat Notifier 정지 등 부가 정리는 유지
        try:
            if self.chat_notifier:
                self.chat_notifier.shutdown()
        except Exception:
            pass
        # 4) Qt 앱 종료는 cleanup 이후에 최종적으로 수행되므로 여기선 스케줄만
        self.append_log("MAIN", "종료 시퀀스 진행 중 (종료 11단계 대기)")

    async def _fast_quit(self) -> None:
        # 0) 지연(step delay) 등 예약 취소 + 채팅 노티 중지
        self._cancel_delay_task()
        try:
            if self.chat_notifier:
                self.chat_notifier.shutdown()
        except Exception:
            pass

        # ▶ IG 대기 태스크가 떠있을 수 있으므로 빠르게 취소 (명령은 아니고 내부 상태 해제)
        try:
            if self.ig and hasattr(self.ig, "cancel_wait"):
                await asyncio.wait_for(self.ig.cancel_wait(), timeout=1.0)
        except Exception:
            pass

        # 1) 백그라운드 태스크 취소는 같은 틱 이후로 미룸 → 재귀 취소 폭주 방지
        loop = asyncio.get_running_loop()
        current = asyncio.current_task()
        live = [t for t in getattr(self, "_bg_tasks", []) if t and not t.done() and t is not current]
        for t in live:
            loop.call_soon(t.cancel)
        if live:
            await asyncio.gather(*live, return_exceptions=True)
        self._bg_tasks = []
        self._bg_started = False

        # 2) 장치: 재연결 시도 금지 + 포트만 닫는 빠른 정리
        tasks = []
        for dev in (self.ig, self.mfc, self.rf_pulse, self.dc_power, self.rf_power, self.oes,
                    getattr(self, "rga_ch1", None), getattr(self, "rga_ch2", None)):
            if not dev:
                continue
            try:
                if hasattr(dev, "cleanup_quick"):
                    tasks.append(dev.cleanup_quick())
                elif hasattr(dev, "cleanup"):
                    tasks.append(dev.cleanup())
            except Exception:
                pass

        # ✅ PLC는 cleanup이 아니라 close()
        try:
            if self.plc and hasattr(self.plc, "close"):
                tasks.append(self.plc.close())
        except Exception:
            pass

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        try:
            if getattr(self, "tsp_ctrl", None) and hasattr(self.tsp_ctrl, "aclose"):
                await self.tsp_ctrl.aclose()
        except Exception:
            pass


        await asyncio.sleep(0.3)

        # 3) 앱 종료
        QCoreApplication.quit()

    # ------------------------------------------------------------------
    # 입력 검증 / 파라미터 정규화 / delay 처리
    # ------------------------------------------------------------------
    def _validate_single_run_inputs(self) -> dict[str, Any] | None:
        # 건 선택
        use_g1 = self.ui.ch2_G1_checkbox.isChecked()
        use_g2 = self.ui.ch2_G2_checkbox.isChecked()
        use_g3 = self.ui.ch2_G3_checkbox.isChecked()
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

    def _normalize_params_for_process(self, raw: RawParams) -> NormParams:
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
    def _cancel_delay_task(self):
        t = getattr(self, "_delay_task", None)
        if t and not t.done():
            t.cancel()
        self._delay_task = None

    def _on_delay_step_done(self, step_name: str):
        self._delay_task = None
        # ✅ 추가: 다음 상태 텍스트는 무조건 반영되도록 캐시 리셋
        self._last_state_text = None
        self.append_log("Process", f"'{step_name}' 지연 완료 → 다음 공정으로 진행")
        self._start_next_process_from_queue(True)

    async def _delay_sleep_then_continue(self, name: str, sec: float):
        try:
            await asyncio.sleep(sec)
            self._on_delay_step_done(name)
        except asyncio.CancelledError:
            # 종료/정지 시 취소되는 정상 경로
            pass

    def _try_handle_delay_step(self, params: ParamsMap) -> bool:
        name = str(params.get("Process_name") or params.get("process_note", "")).strip()
        if not name:
            return False
        m = re.match(r"^\s*delay\s*(\d+)\s*([smhd]?)\s*$", name, re.IGNORECASE)
        if not m:
            return False

        amount = int(m.group(1))
        unit = (m.group(2) or "m").lower()
        factor = {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}[unit]
        duration_s = amount * factor

        unit_txt = {"s": "초", "m": "분", "h": "시간", "d": "일"}[unit]
        self.append_log("Process", f"'{name}' 단계 감지: {amount}{unit_txt} 대기 시작")

        # 폴링 OFF로 전환(잔여 측정/명령 혼입 방지)
        self._apply_polling_targets({"mfc": False, "rfpulse": False, "dc": False, "rf": False})
        self._last_polling_targets = None

        # 상태 문구는 단일 경로로만
        self._soon(self.ui.ch2_processState_edit.setPlainText, f"지연 대기 중: {amount}{unit_txt}")

        self._cancel_delay_task()
        self._set_task_later("_delay_task", self._delay_sleep_then_continue(name, duration_s), name=f"Delay:{name}")
        return True
        
    def _post_update_oes_plot(self, x: Sequence[float], y: Sequence[float]) -> None:
        # Qt 이벤트 루프에서 안전하게 실행(렌더 스레드 보장)
        self._soon(lambda: self.graph_controller.update_oes_plot(x, y))

    # === 유틸 ===
    # 현재 태스크의 자식이 아닌 '루트 태스크'로 분리 생성
    def _spawn_detached(self, coro, *, store: bool=False, name: str|None=None) -> None:
        loop = self._loop
        def _create():
            t = loop.create_task(coro, name=name)
            if store:
                self._bg_tasks.append(t)

            # =========== debug ==========
            tn = t.get_name() if hasattr(t, "get_name") else (name or "task")
            def _on_done(fut: asyncio.Task):
                try:
                    exc = fut.exception()
                except Exception:
                    exc = None
                if exc:
                    self.append_log("Task", f"{tn} finished with ERROR: {exc!r}")
                else:
                    self.append_log("Task", f"{tn} finished OK")
            t.add_done_callback(_on_done)
            self.append_log("Task", f"spawned: {tn}")
            # =========== debug ==========

        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        if running is loop:
            loop.call_soon(_create)
        else:
            loop.call_soon_threadsafe(_create)

    # 나중에 생성될 태스크 핸들을 self.<attr>에 기록(지연 생성)
    def _set_task_later(self, attr_name: str, coro: Coroutine[Any, Any, Any], *, name: str | None = None) -> None:
        loop = self._loop
        def _create_and_set():
            t = loop.create_task(coro, name=name)
            setattr(self, attr_name, t)
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            loop.call_soon(_create_and_set)
        else:
            loop.call_soon_threadsafe(_create_and_set)

    # 콘솔 traceback 가로채서 append_log로만 남기기
    def _install_exception_hooks(self) -> None:
        # 실행 중 루프가 있으면 그걸, 아니면 주입된 루프(self._loop) 사용
        try:
            target_loop = asyncio.get_running_loop()
        except RuntimeError:
            target_loop = self._loop  # __init__ 초반에 이미 채워둠

        def excepthook(exctype, value, tb):
            txt = ''.join(traceback.format_exception(exctype, value, tb)).rstrip()
            self.append_log("EXC", txt)

        def loop_exception_handler(loop_, context):
            exc = context.get("exception")
            msg = context.get("message", "")
            if exc:
                try:
                    txt = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__)).rstrip()
                except Exception:
                    txt = f"{exc!r}"
                self.append_log("Asyncio", txt)
            elif msg:
                self.append_log("Asyncio", msg)

        sys.excepthook = excepthook
        target_loop.set_exception_handler(loop_exception_handler)

    def _loop_from_anywhere(self) -> asyncio.AbstractEventLoop:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return self._loop

    def _soon(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        """
        어떤 스레드에서 호출해도 Qt 메인 루프로 안전하게 넘긴다.
        - 현재 실행 중 루프가 self._loop와 같으면 call_soon
        - 아니면 call_soon_threadsafe
        """
        loop = self._loop
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        if running is loop:
            loop.call_soon(fn, *args, **kwargs)
        else:
            loop.call_soon_threadsafe(fn, *args, **kwargs)


    # === 연결 상태 판단: 공개 API 우선 ===
    def _is_dev_connected(self, dev: object) -> bool:
        try:
            v = getattr(dev, "is_connected", None)
            if callable(v):   # is_connected()
                return bool(v())
            if isinstance(v, bool):  # is_connected 프로퍼티
                return v
        except Exception:
            pass
        # fallback: 내부 플래그
        try:
            return bool(getattr(dev, "_connected", False))
        except Exception:
            return False

    # === 프리플라이트 진행상황을 1초마다 로그로 출력 ===
    async def _preflight_progress_log(self, need: list[tuple[str, object]], stop_evt: asyncio.Event) -> None:
        try:
            while not stop_evt.is_set():
                missing = [name for name, dev in need if not self._is_dev_connected(dev)]
                txt = ", ".join(missing) if missing else "모두 연결됨"
                self.append_log("MAIN", f"연결 대기 중: {txt}")
                await asyncio.wait_for(stop_evt.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass  # 주기적 로그 지속
        except Exception as e:
            self.append_log("MAIN", f"프리플라이트 진행 로그 예외: {e!r}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    loop = QEventLoop(app)          # Qt + asyncio 루프 통합
    asyncio.set_event_loop(loop)
    w = MainWindow(loop)
    w.show()

    with loop:
        loop.run_forever()

# -*- coding: utf-8 -*-
import sys, asyncio
from typing import Optional, Literal
from pathlib import Path

from PySide6.QtWidgets import QApplication, QWidget, QStackedWidget
from PySide6.QtGui import QCloseEvent
from qasync import QEventLoop

# UI / Controller
from ui.main_window import Ui_Form
from main_tsp import TSPPageController
from controller.chat_notifier import ChatNotifier

# 공유 장비(PLC만 공용)
from device.plc import AsyncPLC

# ▶ 새로 추가될 런타임 래퍼 (각 챔버 독립 실행단위)
from controller.chamber_runtime import ChamberRuntime  # 다음 단계에서 파일 제공

# 챔버별 설정
from lib import config_ch1, config_ch2
from lib import config_common as cfgc


class MainWindow(QWidget):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
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

        # === 공용(공유) 리소스만 이곳에서 생성 ===
        # Chat Notifier (공통 config로 이동)
        self.chat_notifier: ChatNotifier | None = (
            ChatNotifier(cfgc.CHAT_WEBHOOK_URL) if getattr(cfgc, "ENABLE_CHAT_NOTIFY", False) else None
        )
        if self.chat_notifier:
            self.chat_notifier.start()

        # PLC (공유) : 로그는 양쪽 런타임에 방송
        def _plc_log(fmt, *args):
            msg = (fmt % args) if args else str(fmt)
            self._broadcast_log("PLC", msg)

        self.plc: AsyncPLC = AsyncPLC(logger=_plc_log)

        # 로그 루트 (NAS 실패 시 런타임 내부에서 폴백 처리)
        self._log_root = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs")

        # === 챔버 런타임 2개 생성 (각각 자기 장치/그래프/로거/버튼 바인딩 포함) ===
        # CH1: DC-Pulse, IG/MFC/OES/RGA(1), PLC 공유
        self.ch1 = ChamberRuntime(
            ui=self.ui,
            chamber_no=1,
            prefix="ch1_",                 # UI 위젯 접두사
            loop=self._loop,
            plc=self.plc,                  # 공유
            chat=self.chat_notifier,       # 공유(옵션)
            cfg=config_ch1,                # CH1 설정
            log_dir=self._log_root,        # 루트만 넘기면 런타임이 ch1/ch2 파일명 분리
        )

        # CH2: DC + RF-Pulse, IG/MFC/OES/RGA(2), PLC 공유
        self.ch2 = ChamberRuntime(
            ui=self.ui,
            chamber_no=2,
            prefix="ch2_",
            loop=self._loop,
            plc=self.plc,
            chat=self.chat_notifier,
            cfg=config_ch2,
            log_dir=self._log_root,
        )

        # === TSP 페이지 컨트롤러 ===  (공통 config로 이동)
        self.tsp_ctrl = TSPPageController(
            ui=self.ui,
            host=cfgc.TSP_TCP_HOST,
            tcp_port=cfgc.TSP_TCP_PORT,
            addr=cfgc.TSP_ADDR,
            loop=self._loop,
        )

        # --- 페이지 네비 버튼만 여기서 연결 (Start/Stop은 각 런타임이 자기 버튼에 바인딩)
        self._connect_page_buttons()

    # 공용 PLC 로그를 두 런타임 로그창에 뿌림
    def _broadcast_log(self, source: str, msg: str) -> None:
        try:
            if hasattr(self, "ch1") and self.ch1:
                self.ch1.append_log(source, msg)
        except Exception:
            pass
        try:
            if hasattr(self, "ch2") and self.ch2:
                self.ch2.append_log(source, msg)
        except Exception:
            pass

    # --- 페이지 전환 (UI에 있던 네비 버튼만 연결)
    def _connect_page_buttons(self) -> None:
        self.ui.pc_btnGoCh1.clicked.connect(lambda: self._switch_page("ch1"))
        self.ui.pc_btnGoCh2.clicked.connect(lambda: self._switch_page("ch2"))
        self.ui.ch1_btnGoPC.clicked.connect(lambda: self._switch_page("pc"))
        self.ui.ch1_btnGoCh2.clicked.connect(lambda: self._switch_page("ch2"))
        self.ui.ch2_btnGoPC.clicked.connect(lambda: self._switch_page("pc"))
        self.ui.ch2_btnGoCh1.clicked.connect(lambda: self._switch_page("ch1"))

    def _switch_page(self, key: Literal["pc", "ch1", "ch2"]) -> None:
        page = self._pages.get(key)
        if page:
            self._stack.setCurrentWidget(page)

    # --- 앱 종료: 각 런타임의 빠른 정리 API 호출 + Chat 종료
    def closeEvent(self, event: QCloseEvent) -> None:
        try:
            if self.ch1:
                self.ch1.shutdown_fast()
        except Exception:
            pass
        try:
            if self.ch2:
                self.ch2.shutdown_fast()
        except Exception:
            pass
        try:
            if self.chat_notifier:
                self.chat_notifier.shutdown()
        except Exception:
            pass
        event.accept()
        super().closeEvent(event)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)
    w = MainWindow(loop)
    w.show()
    with loop:
        loop.run_forever()

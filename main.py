# -*- coding: utf-8 -*-
import sys, asyncio, re
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

# ▶ 런타임 래퍼
from controller.chamber_runtime import ChamberRuntime           # ← 경로 정리
from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime   # ★ 필요 시 사용

# 챔버별 설정
from lib import config_ch1, config_ch2
from lib import config_common as cfgc
from lib import config_local as cfgl  # CHAT_WEBHOOK_URL 로드

# ───────────────────────────────────────────────────────────
CH_HINT_RE = re.compile(r"\[CH\s*(\d)\]|\bCH(?:=|:)?\s*(\d)\b", re.IGNORECASE)
# ───────────────────────────────────────────────────────────

class MainWindow(QWidget):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        super().__init__()
        self.ui = Ui_Form()
        self.ui.setupUi(self)

        # ▼ TSP 기본값(UI에 채워 넣기)
        self.ui.TSP_targetPressure_edit.setPlainText("2.5e-7")
        self.ui.TSP_setCycle_edit.setPlainText("10")

        self._loop = loop or asyncio.get_event_loop()

        # --- 스택 및 페이지 매핑 (UI 객체명 고정)
        self._stack: QStackedWidget = self.ui.stackedWidget
        self._pages: dict[str, QWidget] = {
            "pc":  self.ui.page_3,  # Plasma Cleaning
            "ch1": self.ui.page,    # CH1
            "ch2": self.ui.page_2,  # CH2
        }

        # === 공용(공유) 리소스만 이곳에서 생성 ===
        # 단일 Chat Notifier (config_local.CHAT_WEBHOOK_URL 사용)
        url = getattr(cfgl, "CHAT_WEBHOOK_URL", None)
        self.chat: Optional[ChatNotifier] = ChatNotifier(url) if url else None
        if self.chat:
            self.chat.start()

        # ── 현재 PLC 로그의 소유 챔버 (1/2). 없으면 None → 방송 모드
        self._plc_owner: Optional[int] = None

        # PLC (공유) : CH 힌트 > 소유자 > 방송 순으로 라우팅
        self.plc: AsyncPLC = AsyncPLC(logger=self._plc_log)

        # 로그 루트 (NAS 실패 시 런타임 내부에서 폴백 처리)
        self._log_root = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs")

        # === Plasma Cleaning가 필요하면 사용
        # self.pc = PlasmaCleaningRuntime(
        #     ui=self.ui,
        #     prefix="pc_",
        #     loop=self._loop,
        #     cfg=config_ch1,
        #     log_dir=self._log_root,
        #     plc=self.plc,
        #     chat=self.chat,  # PC 알림도 같은 방으로
        # )

        # === 챔버 런타임 2개 생성 ===
        self.ch1 = ChamberRuntime(
            ui=self.ui,
            chamber_no=1,
            prefix="ch1_",
            loop=self._loop,
            plc=self.plc,
            chat=self.chat,     # ★ 단일 Notifier 공유
            cfg=config_ch1,
            log_dir=self._log_root,
            on_plc_owner=self._set_plc_owner,
        )

        self.ch2 = ChamberRuntime(
            ui=self.ui,
            chamber_no=2,
            prefix="ch2_",
            loop=self._loop,
            plc=self.plc,
            chat=self.chat,     # ★ 단일 Notifier 공유
            cfg=config_ch2,
            log_dir=self._log_root,
            on_plc_owner=self._set_plc_owner,
        )

        # === TSP 페이지 컨트롤러 ===
        self.tsp_ctrl = TSPPageController(
            ui=self.ui,
            host=cfgc.TSP_TCP_HOST,
            tcp_port=cfgc.TSP_TCP_PORT,
            addr=cfgc.TSP_ADDR,
            loop=self._loop,
        )

        # --- 페이지 네비 버튼 연결
        self._connect_page_buttons()

    # ───────────────────────────────────────────────────────────
    def _set_plc_owner(self, ch: Optional[int]) -> None:
        self._plc_owner = ch if ch in (1, 2) else None
        if hasattr(self, "ch1"): self.ch1.set_plc_log_owner(self._plc_owner == 1)
        if hasattr(self, "ch2"): self.ch2.set_plc_log_owner(self._plc_owner == 2)

    def _route_log_to(self, ch: int, src: str, msg: str) -> None:
        if ch == 1 and getattr(self, "ch1", None):
            self.ch1.append_log(src, msg)
        elif ch == 2 and getattr(self, "ch2", None):
            self.ch2.append_log(src, msg)

    def _plc_log(self, fmt, *args):
        msg = (fmt % args) if args else str(fmt)
        m = CH_HINT_RE.search(msg)
        if m:
            hinted = next((g for g in m.groups() if g), None)
            hinted_ch = int(hinted) if hinted else 0
            if hinted_ch in (1, 2):
                self._route_log_to(hinted_ch, "PLC", msg)
                return
        if self._plc_owner in (1, 2):
            self._route_log_to(self._plc_owner, "PLC", msg)
            return
        if getattr(self, "ch1", None):
            self.ch1.append_log("PLC(Global)", msg)
        if getattr(self, "ch2", None):
            self.ch2.append_log("PLC(Global)", msg)
        if getattr(self, "pc", None):
            self.pc.append_log("PLC(Global)", msg)

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
        try:
            if hasattr(self, "pc") and self.pc:
                self.pc.append_log(source, msg)
        except Exception:
            pass

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

    def closeEvent(self, event: QCloseEvent) -> None:
        try:
            if self.pc:
                self.pc.shutdown_fast()
        except Exception:
            pass
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
            if self.chat:
                self.chat.shutdown()   # ★ 단일 Notifier만 종료
        except Exception:
            pass
        event.accept()
        super().closeEvent(event)


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    app = QApplication(sys.argv)
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)

    w = MainWindow(loop)
    w.show()
    with loop:
        loop.run_forever()

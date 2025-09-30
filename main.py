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
from runtime.chamber_runtime import ChamberRuntime
from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime   # ★ 추가

# 챔버별 설정
from lib import config_ch1, config_ch2
from lib import config_common as cfgc

# ───────────────────────────────────────────────────────────
# 로그 메시지 내 CH 힌트 정규식: [CH1], [CH 2], CH=1, CH:2, ch 1 등
CH_HINT_RE = re.compile(r"\[CH\s*(\d)\]|\bCH(?:=|:)?\s*(\d)\b", re.IGNORECASE)
# ───────────────────────────────────────────────────────────


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

        # ── 현재 PLC 로그의 소유 챔버 (1/2). 없으면 None → 방송 모드
        self._plc_owner: Optional[int] = None

        # PLC (공유) : CH 힌트 > 소유자 > 방송 순으로 라우팅
        self.plc: AsyncPLC = AsyncPLC(logger=self._plc_log)

        # 로그 루트 (NAS 실패 시 런타임 내부에서 폴백 처리)
        self._log_root = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs")

        # === Plasma Cleaning 런타임 (PC 전용) 생성 ★ 추가 ===
        # - RF는 PLC 경유(연속만), 가스밸브/IG 없음, CH1 MFC TCP 사용
        # self.pc = PlasmaCleaningRuntime(
        #     ui=self.ui,
        #     prefix="pc_",            # PC 페이지 접두사
        #     loop=self._loop,
        #     cfg=config_ch1,          # CH1의 MFC TCP를 사용
        #     log_dir=self._log_root,
        #     plc=self.plc,            # ★ PLC 전달 (RF 파워 제어용)
        #     chat=self.chat_notifier,
        # )

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
            on_plc_owner=self._set_plc_owner,   # ★ 공정 시작/종료 때 소유자 갱신
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
            on_plc_owner=self._set_plc_owner,   # ★ 공정 시작/종료 때 소유자 갱신
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

    # ───────────────────────────────────────────────────────────
    # ▼ 여기부터는 클래스 "메서드"로 유지해야 합니다
    # ───────────────────────────────────────────────────────────
    def _set_plc_owner(self, ch: Optional[int]) -> None:
        """ChamberRuntime에서 공정 시작/종료 때 호출해 소유자 갱신"""
        self._plc_owner = ch if ch in (1, 2) else None
        if hasattr(self, "ch1"): self.ch1.set_plc_log_owner(self._plc_owner == 1)
        if hasattr(self, "ch2"): self.ch2.set_plc_log_owner(self._plc_owner == 2)

    def _route_log_to(self, ch: int, src: str, msg: str) -> None:
        """지정 챔버로만 로그 싱글캐스트"""
        if ch == 1 and getattr(self, "ch1", None):
            self.ch1.append_log(src, msg)
        elif ch == 2 and getattr(self, "ch2", None):
            self.ch2.append_log(src, msg)

    def _plc_log(self, fmt, *args):
        """PLC 드라이버에서 호출하는 로거 콜백"""
        msg = (fmt % args) if args else str(fmt)

        # 1) 메시지 자체에 CH 힌트가 있으면 최우선 라우팅
        m = CH_HINT_RE.search(msg)
        if m:
            hinted = next((g for g in m.groups() if g), None)
            hinted_ch = int(hinted) if hinted else 0
            if hinted_ch in (1, 2):
                self._route_log_to(hinted_ch, "PLC", msg)
                return

        # 2) 힌트가 없으면, 현재 PLC 소유자에게만 보냄
        if self._plc_owner in (1, 2):
            self._route_log_to(self._plc_owner, "PLC", msg)
            return

        # 3) 소유자도 없으면 '전역'으로 방송 (★ PC에도 뿌려줌)
        if getattr(self, "ch1", None):
            self.ch1.append_log("PLC(Global)", msg)
        if getattr(self, "ch2", None):
            self.ch2.append_log("PLC(Global)", msg)
        if getattr(self, "pc", None):                          # ★ 추가
            self.pc.append_log("PLC(Global)", msg)

    # 공용 로그 방송 (필요시 사용)
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
        try:                                                   # ★ 추가(옵션)
            if hasattr(self, "pc") and self.pc:
                self.pc.append_log(source, msg)
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
            if self.pc:                                       # ★ 추가
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
            if self.chat_notifier:
                self.chat_notifier.shutdown()
        except Exception:
            pass
        event.accept()
        super().closeEvent(event)


if __name__ == "__main__":
    # ▶ Windows + Python 3.13 재진입 예외 회피 (Overlapped → Selector)
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    app = QApplication(sys.argv)
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)

    w = MainWindow(loop)
    w.show()
    with loop:
        loop.run_forever()

# main.py
# -*- coding: utf-8 -*-
import sys, asyncio, re
from typing import Optional, Literal
from pathlib import Path

from PySide6.QtWidgets import QApplication, QWidget, QStackedWidget
from PySide6.QtGui import QCloseEvent
from qasync import QEventLoop

# UI / Controller
from ui.main_window import Ui_Form
from runtime.tsp_runtime import TSPPageController
from controller.chat_notifier import ChatNotifier

# 공유 장비(PLC, IG, MFC)
from device.plc import AsyncPLC
from device.mfc import AsyncMFC
from device.ig import AsyncIG

# ▶ 런타임 래퍼
from runtime.chamber_runtime import ChamberRuntime
from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime

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

        # ▼ Plasma Cleaning 기본값(UI에 채워 넣기)
        self.ui.PC_targetPressure_edit.setPlainText("5e-6")  # Target Pressure
        self.ui.PC_gasFlow_edit.setPlainText("30")           # Gas Flow (sccm)
        self.ui.PC_workingPressure_edit.setPlainText("30")   # Working Pressure
        self.ui.PC_rfPower_edit.setPlainText("55")           # RF Power (W)
        self.ui.PC_ProcessTime_edit.setPlainText("0.25")     # Process Time (분)

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

        # ─────────────────────────────────────────────────────
        # CH1/CH2용 IG/MFC 모두 생성해 보관
        #   - IG: 각 챔버 전용 포트
        #   - MFC: 각 챔버 전용 포트
        #   - Gas Flow는 항상 MFC1의 ch=3 사용(Plasma Cleaning용 정책)
        # ─────────────────────────────────────────────────────
        self.mfc1 = AsyncMFC(
            host=getattr(config_ch1, "MFC_TCP_HOST", getattr(cfgc, "MFC_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch1, "MFC_TCP_PORT", 4003),
            enable_verify=False,
            enable_stabilization=True,
        )
        self.mfc2 = AsyncMFC(
            host=getattr(config_ch2, "MFC_TCP_HOST", getattr(cfgc, "MFC_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch2, "MFC_TCP_PORT", 4006),
            enable_verify=False,
            enable_stabilization=True,
        )

        self.ig1 = AsyncIG(
            host=getattr(config_ch1, "IG_TCP_HOST", getattr(cfgc, "IG_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch1, "IG_TCP_PORT", 4001),
        )
        self.ig2 = AsyncIG(
            host=getattr(config_ch2, "IG_TCP_HOST", getattr(cfgc, "IG_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch2, "IG_TCP_PORT", 4002),
        )

        # Plasma Cleaning 선택 상태(초기 CH1), Gas Flow 정책(항상 MFC1 ch3)
        self._pc_use_ch: int = 1
        self._pc_gas_mfc = self.mfc1
        self._pc_gas_channel = 3

        # === 챔버 런타임 2개 생성 ===
        self.ch1 = ChamberRuntime(
            ui=self.ui,
            chamber_no=1,
            prefix="ch1_",
            loop=self._loop,
            plc=self.plc,
            chat=self.chat,     # 단일 Notifier 공유 가능
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
            chat=self.chat,     # 단일 Notifier 공유 가능
            cfg=config_ch2,
            log_dir=self._log_root,
            on_plc_owner=self._set_plc_owner,
        )

        # === Plasma Cleaning 런타임 생성 ===
        # mfc 인자는 하위 호환용으로 CH1의 MFC를 초기값으로 전달하되,
        # 곧바로 라디오 선택 상태를 반영하여 set_devices 또는 속성 주입으로 덮어씁니다.
        self.pc = PlasmaCleaningRuntime(
            ui=self.ui,
            prefix="",          # UI 위젯 접두사: PC_Start_button 등과 매칭
            loop=self._loop,
            plc=self.plc,          # 공유 PLC
            mfc=self.mfc1,         # 초기값: CH1 MFC
            chat=None,
        )

        # === TSP 런타임 생성 ===
        self.tsp_ctrl = TSPPageController(
            ui=self.ui,
            host=cfgc.TSP_TCP_HOST,
            tcp_port=cfgc.TSP_TCP_PORT,
            addr=cfgc.TSP_ADDR,
            loop=self._loop,
        )

        # --- 페이지 네비 버튼 및 라디오 연결
        self._connect_page_buttons()

        # --- Plasma Cleaning에 현재 라디오 선택 반영(초기 바인딩)
        self._apply_pc_ch_selection()

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

        # Plasma Cleaning 챔버 선택 라디오: 기본 CH1 체크 + 핸들러 연결
        try:
            if hasattr(self.ui, "PC_useChamber1_radio"):
                self.ui.PC_useChamber1_radio.setChecked(True)
            # 두 라디오 모두 토글 시 _apply_pc_ch_selection 호출
            for rb in (getattr(self.ui, "PC_useChamber1_radio", None),
                       getattr(self.ui, "PC_useChamber2_radio", None)):
                if rb:
                    rb.toggled.connect(self._on_pc_radio_toggled)
        except Exception:
            pass

    def _on_pc_radio_toggled(self, _checked: bool) -> None:
        # 현재 라디오 상태 읽어서 내부 플래그 갱신
        ch = 1
        try:
            if hasattr(self.ui, "PC_useChamber2_radio") and self.ui.PC_useChamber2_radio.isChecked():
                ch = 2
        except Exception:
            pass

        if ch != getattr(self, "_pc_use_ch", None):
            self._pc_use_ch = ch

        # IG/MFC 바인딩 재적용
        self._apply_pc_ch_selection()

    def _apply_pc_ch_selection(self) -> None:
        """라디오 버튼 상태를 Plasma Cleaning 런타임에 반영.
        - IG: 선택된 챔버(1→ig1, 2→ig2)
        - MFC(SP4 전용): 선택된 챔버(1→mfc1, 2→mfc2)
        - Gas Flow: 항상 MFC1 채널 3
        """
        if not getattr(self, "pc", None):
            return

        # 안전하게 기본값 보정
        ch = int(getattr(self, "_pc_use_ch", 1))
        ig  = self.ig1 if ch == 1 else self.ig2
        mfc_sp4 = self.mfc1 if ch == 1 else self.mfc2
        gas_mfc = self._pc_gas_mfc       # 항상 MFC1
        gas_ch  = self._pc_gas_channel   # 항상 3

        # 런타임 메서드가 있으면 사용, 없으면 속성 주입
        try:
            if hasattr(self.pc, "set_devices"):  # set_devices(ig=..., mfc_sp4=..., gas_mfc=..., gas_channel=...)
                self.pc.set_devices(ig=ig, mfc_sp4=mfc_sp4, gas_mfc=gas_mfc, gas_channel=gas_ch)
            else:
                self.pc.ig = ig
                self.pc.mfc_sp4 = mfc_sp4        # 선택된 챔버의 MFC (SP4_SET/SP4_ON 전용)
                self.pc.gas_mfc = gas_mfc        # 항상 MFC1
                self.pc.gas_channel = gas_ch     # 항상 3
        except Exception as e:
            self._broadcast_log("PC", f"device bind failed: {e!r}")

        # 로그 안내
        sel = f"CH{ch}"
        self._broadcast_log(
            "PC",
            f"[Plasma Cleaning] Use {sel} IG/MFC(SP4 전용), GasFlow → MFC1 ch{gas_ch}"
        )

    def _switch_page(self, key: Literal["pc", "ch1", "ch2"]) -> None:
        page = self._pages.get(key)
        if page:
            self._stack.setCurrentWidget(page)

    def closeEvent(self, event: QCloseEvent) -> None:
        # 요청: 해당 장비를 챔버에서 사용하고 있을 수 있으니 정리(클린업)는 추가하지 않음
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
                self.chat.shutdown()   # 단일 Notifier만 종료
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

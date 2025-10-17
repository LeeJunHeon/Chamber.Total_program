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
from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime  # type: ignore
from controller.plasma_cleaning_controller import PlasmaCleaningController  # type: ignore

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
        self.ui.PC_workingPressure_edit.setPlainText("30")   # Working Pressure (mTorr)
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

        # === 공용(공유) 리소스 생성 ===
        # 단일 Chat Notifier (config_local.CHAT_WEBHOOK_URL 사용)
        url = getattr(cfgl, "CHAT_WEBHOOK_URL", None)
        self.chat: Optional[ChatNotifier] = ChatNotifier(url) if url else None
        if self.chat:
            self.chat.start()

        # ── 현재 PLC 로그의 소유 챔버 (1/2). 없으면 None → 방송 모드
        self._plc_owner: Optional[int] = None
        self.pc = None  # Plasma Cleaning 런타임 핸들

        # PLC (공유) : CH 힌트 > 소유자 > 방송 순으로 라우팅
        self.plc: AsyncPLC = AsyncPLC(logger=self._plc_log)

        # 로그 루트 (NAS 실패 시 런타임 내부에서 폴백 처리)
        self._log_root = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs")

        # ─────────────────────────────────────────────────────
        # CH1/CH2용 IG/MFC 모두 생성해 보관
        #   - IG: 각 챔버 전용 포트
        #   - MFC: 각 챔버 전용 포트
        #   - Plasma Cleaning에서 Gas Flow는 '항상 MFC1의 ch=3' 정책
        self.mfc1: AsyncMFC = AsyncMFC(
            host=getattr(config_ch1, "MFC_TCP_HOST", getattr(cfgc, "MFC_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch1, "MFC_TCP_PORT", 4003),
            enable_verify=False,
            enable_stabilization=True,
        )
        self.mfc2: AsyncMFC = AsyncMFC(
            host=getattr(config_ch2, "MFC_TCP_HOST", getattr(cfgc, "MFC_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch2, "MFC_TCP_PORT", 4006),
            enable_verify=False,
            enable_stabilization=True,
        )

        self.ig1: AsyncIG = AsyncIG(
            host=getattr(config_ch1, "IG_TCP_HOST", getattr(cfgc, "IG_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch1, "IG_TCP_PORT", 4001),
        )
        self.ig2: AsyncIG = AsyncIG(
            host=getattr(config_ch2, "IG_TCP_HOST", getattr(cfgc, "IG_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch2, "IG_TCP_PORT", 4002),
        )

        # Plasma Cleaning에서 선택된 챔버 추적(기본 CH1)
        self._pc_use_ch: int = 1

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
            mfc=self.mfc1,
            ig=self.ig1,
            on_plc_owner=self._set_plc_owner,
        )

        self.ch2 = ChamberRuntime(
            ui=self.ui,
            chamber_no=2,
            prefix="ch2_",
            loop=self._loop,
            plc=self.plc,
            chat=self.chat,
            cfg=config_ch2,
            log_dir=self._log_root,
            mfc=self.mfc2,
            ig=self.ig2,
            on_plc_owner=self._set_plc_owner,
        )

        # === Plasma Cleaning 런타임 생성 (공유 장치 주입) ===
        try:
            self.pc = PlasmaCleaningRuntime(
                ui=self.ui,
                prefix="PC_",               # PC_* 네이밍 사용
                loop=self._loop,
                log_dir=self._log_root,
                plc=self.plc,              # 공유 PLC
                mfc_gas=self.mfc1,         # Gas Flow는 정책상 항상 MFC1 사용
                mfc_sp4=self.mfc1,         # 초기엔 CH1 기준
                ig=self.ig1,               # IG도 초기엔 CH1 기준
                chat=self.chat,
            )
        except Exception as e:
            self.pc = None
            try:
                self._broadcast_log("PC", f"Failed to initialize PlasmaCleaningRuntime: {e!r}")
            except Exception:
                pass

        # === TSP 런타임 생성 ===
        self.tsp_ctrl = TSPPageController(
            ui=self.ui,
            host=cfgc.TSP_TCP_HOST,
            tcp_port=cfgc.TSP_TCP_PORT,
            addr=cfgc.TSP_ADDR,
            loop=self._loop,
            chat=self.chat, # ← CH1/CH2와 동일하게 Notifier 주입
            log_dir=self._log_root, # ★ NAS 로그 루트 전달 (CH/PC와 동일)
        )

        # --- 페이지 네비 버튼 및 라디오 연결
        self._connect_page_buttons()

        # IG 콜백 + SP4용 MFC 전환을 함께 반영
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
            for rb in (getattr(self.ui, "PC_useChamber1_radio", None),
                       getattr(self.ui, "PC_useChamber2_radio", None)):
                if rb:
                    rb.toggled.connect(self._on_pc_radio_toggled)
        except Exception:
            pass

    def _on_pc_radio_toggled(self, checked: bool) -> None:
        # 라디오가 꺼지는 이벤트(False)는 무시해서 중복 호출 방지
        if not checked:
            return

        # 현재 라디오 상태 읽기
        try:
            ch2_on = hasattr(self.ui, "PC_useChamber2_radio") and self.ui.PC_useChamber2_radio.isChecked()
        except Exception:
            ch2_on = False
        ch = 2 if ch2_on else 1

        # 선택이 이전과 동일하면 아무 것도 하지 않음
        if ch == getattr(self, "_pc_use_ch", None):
            return

        self._pc_use_ch = ch
        self._apply_pc_ch_selection()

    def _apply_pc_ch_selection(self) -> None:
        pc = getattr(self, "pc", None)
        if not pc:
            return

        # 라디오 상태 → 선택 챔버
        ch = 1
        try:
            if hasattr(self.ui, "PC_useChamber2_radio") and self.ui.PC_useChamber2_radio.isChecked():
                ch = 2
        except Exception:
            pass
        self._pc_use_ch = ch

        # 선택된 챔버의 IG
        ig = self.ig1 if ch == 1 else self.ig2

        # IG 콜백 주입
        async def _ensure_on():
            fn = getattr(ig, "ensure_on", None) or getattr(ig, "turn_on", None)
            if callable(fn):
                await fn()

        async def _read_mTorr() -> float:
            # 프로젝트의 AsyncIG 구현에서 mTorr 직접 반환 함수가 있으면 사용
            for name in ("read_mTorr", "read_pressure_mTorr", "get_mTorr"):
                fn = getattr(ig, name, None)
                if callable(fn):
                    v = await fn()
                    return float(v)
            # 없으면 Torr 읽고 변환
            read_fn = getattr(ig, "read_pressure", None)
            if not callable(read_fn):
                raise RuntimeError("IG read API not found")
            torr = float(await read_fn())
            return torr * 1000.0

        try:
            pc.set_ig_callbacks(_ensure_on, _read_mTorr)
        except Exception as e:
            self._broadcast_log("PC", f"IG 콜백 바인딩 실패: {e!r}")

        # 선택 챔버에 맞춰 MFC 주입: Gas Flow는 항상 mfc1, SP4는 해당 챔버 MFC
        try:
            mfc_sp4 = self.mfc1 if ch == 1 else self.mfc2
            pc.set_mfcs(mfc_gas=self.mfc1, mfc_sp4=mfc_sp4)
        except Exception as e:
            self._broadcast_log("PC", f"MFC 주입 실패: {e!r}")

        # 선택 챔버 번호 런타임에 통지
        try:
            pc.set_selected_ch(ch)
        except Exception:
            pass

        # 안내 로그
        try:
            pc.append_log("PC", f"[Plasma Cleaning] Use CH{ch} IG, SP4 → MFC{ch}, GasFlow → MFC1 ch3")
        except Exception:
            pass

    def _switch_page(self, key: Literal["pc", "ch1", "ch2"]) -> None:
        page = self._pages.get(key)
        if page:
            self._stack.setCurrentWidget(page)

    def closeEvent(self, event: QCloseEvent) -> None:
        # 공유 장치이므로 여기서는 빠른 종료만
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
                self.chat.shutdown()
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

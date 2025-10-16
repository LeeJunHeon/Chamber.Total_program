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
        self.pc = None  # Plasma Cleaning 비활성화 표시

        # PLC (공유) : CH 힌트 > 소유자 > 방송 순으로 라우팅
        self.plc: AsyncPLC = AsyncPLC(logger=self._plc_log)

        # 로그 루트 (NAS 실패 시 런타임 내부에서 폴백 처리)
        self._log_root = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs")

        # ─────────────────────────────────────────────────────
        # CH1/CH2용 IG/MFC 모두 생성해 보관
        #   - IG: 각 챔버 전용 포트
        #   - MFC: 각 챔버 전용 포트
        #   - Gas Flow는 항상 MFC1의 ch=3 사용(Plasma Cleaning용 정책)
        #
        # IG와 MFC는 PLC와 마찬가지로 공통 자원으로 관리해야 한다.  
        # 이전 구현에서는 각 챔버 런타임 내부에서 IG/MFC 인스턴스를 생성했으나  
        # Plasma Cleaning과 CH 공정이 동일한 장치를 사용하면서 상태를 공유할 수 있도록  
        # 여기에서 챔버별로 한 번씩만 생성해 두고 런타임/컨트롤러에 주입한다.

        # CH1용 MFC (Gas #1–4), CH2용 MFC
        # 설정 모듈에 값이 없을 때는 config_common의 값을 사용하고,
        # 최종적으로 기본값으로 192.168.1.50 IP와 포트를 사용한다.
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

        # CH1용 IG, CH2용 IG
        self.ig1: AsyncIG = AsyncIG(
            host=getattr(config_ch1, "IG_TCP_HOST", getattr(cfgc, "IG_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch1, "IG_TCP_PORT", 4001),
        )
        self.ig2: AsyncIG = AsyncIG(
            host=getattr(config_ch2, "IG_TCP_HOST", getattr(cfgc, "IG_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch2, "IG_TCP_PORT", 4002),
        )

        # Plasma Cleaning에서 선택된 챔버(MFC/IG) 추적용 변수  
        # 기본적으로 CH1을 사용하며, Gas Flow는 정책상 항상 MFC1의 3번 가스 채널을 이용한다.
        self._pc_use_ch: int = 1
        self._pc_gas_mfc = self.mfc1
        self._pc_gas_channel: int = 3

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
            chat=self.chat,     # 단일 Notifier 공유 가능
            cfg=config_ch2,
            log_dir=self._log_root,
            mfc=self.mfc2,
            ig=self.ig2,
            on_plc_owner=self._set_plc_owner,
        )

        # === Plasma Cleaning 런타임과 컨트롤러 생성 ===
        # Plasma Cleaning 공정은 IG/MFC/PLC의 인스턴스를 공유하므로 메인에서
        # Runtime을 한 번만 생성한 뒤 각 챔버 선택 시 디바이스를 바꿔 끼웁니다.
        try:
            # 초기에는 CH1의 IG/MFC를 바인딩한다. 이후 라디오 버튼
            # 상태에 따라 set_devices()로 변경된다.
            self.pc = PlasmaCleaningRuntime(
                ui=self.ui,
                prefix="PC_",                 # UI objectName이 PC_Start_button 형식이면 반드시 "PC"
                loop=self._loop,
                #cfg=config_ch1,              # CH1 TCP 정보로 fallback 생성 가능하게
                log_dir=self._log_root,
                plc=self.plc,                # 공유 PLC
                # Plasma Cleaning은 Gas Flow를 항상 MFC1의 3번 채널로 사용하고,  
                # 스퍼터링 압력 제어용 SP4 역시 현재 선택된 챔버의 MFC 인스턴스를 사용한다.  
                # 초기에 CH1을 기본으로 사용하도록 주입한다.  
                mfc_gas=self.mfc1,
                mfc_sp4=self.mfc1,
                ig=self.ig1,
                chat=self.chat,
            )

        except Exception as e:
            # 초기화 실패 시 None으로 설정하고 로그를 남긴다.
            self.pc = None
            self.pc_ctrl = None
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
        )

        # --- 페이지 네비 버튼 및 라디오 연결
        self._connect_page_buttons()

        # IG 콜백 + SP4용 MFC 전환을 함께 반영하도록 수정
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
        pc = getattr(self, "pc", None)
        if not pc:
            return

        # 라디오 상태 반영
        ch = 1
        try:
            if hasattr(self.ui, "PC_useChamber2_radio") and self.ui.PC_useChamber2_radio.isChecked():
                ch = 2
        except Exception:
            pass
        self._pc_use_ch = ch

        # 선택된 챔버의 IG 객체
        ig = self.ig1 if ch == 1 else self.ig2

        # IG 콜백 주입 (런타임은 이 콜백만 사용)
        async def _ensure_on():
            fn = getattr(ig, "ensure_on", None) or getattr(ig, "turn_on", None)
            if callable(fn):
                await fn()

        async def _read_mTorr() -> float:
            # 프로젝트의 AsyncIG에 맞는 읽기 API로 교체하세요.
            # 아래는 흔한 네이밍을 우선 시도하고, 실패 시 예외를 일으켜 상위에서 처리.
            for name in ("read_mTorr", "read_pressure_mTorr", "get_mTorr"):
                fn = getattr(ig, name, None)
                if callable(fn):
                    v = await fn()
                    return float(v)
            raise RuntimeError("IG read API not found")

        try:
            pc.set_ig_callbacks(_ensure_on, _read_mTorr)
        except Exception as e:
            self._broadcast_log("PC", f"IG 콜백 바인딩 실패: {e!r}")

        # 선택된 챔버에 따라 MFC 주입: Gas Flow는 항상 self.mfc1, SP4는 현재 챔버 MFC
        try:
            mfc_sp4 = self.mfc1 if ch == 1 else self.mfc2
            if hasattr(pc, 'set_mfcs'):
                pc.set_mfcs(mfc_gas=self.mfc1, mfc_sp4=mfc_sp4)
            elif hasattr(pc, 'set_mfc_sp4'):
                pc.set_mfc_sp4(mfc_sp4)
        except Exception as e:
            self._broadcast_log("PC", f"MFC 주입 실패: {e!r}")

        # (선택) 현재 선택된 챔버 번호도 런타임에 전달
        if hasattr(pc, 'set_selected_ch'):
            pc.set_selected_ch(ch)

        # 안내 로그 (한 번만 출력하도록 중복 제거)
        try:
            msg = f"[Plasma Cleaning] Use CH{ch} IG, SP4 → MFC{ch}, GasFlow → MFC1 ch3"
            if hasattr(self, "pc") and self.pc:
                self.pc.append_log("PC", msg)  # PC 로그창에만 출력
            else:
                self._broadcast_log("PC", msg)
        except Exception:
            pass
    
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

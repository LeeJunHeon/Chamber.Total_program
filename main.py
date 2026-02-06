# main.py
# -*- coding: utf-8 -*-

# 추가: host 설치 유틸
from host.setup import install_host
from controller.runtime_state import runtime_state

import sys, asyncio, re, atexit, logging
from typing import Optional, Literal
from pathlib import Path
from contextvars import ContextVar

from PySide6.QtWidgets import QApplication, QWidget, QStackedWidget, QPlainTextEdit, QTextEdit
from PySide6.QtCore import Qt, QTimer
from itertools import chain  # ← 추가
from PySide6.QtGui import QCloseEvent
from qasync import QEventLoop

# UI / Controller
from ui.main_window import Ui_Form
from runtime.tsp_runtime import TSPPageController
from runtime.pre_sputter_runtime import PreSputterRuntime
from controller.chat_notifier import ChatNotifier

# 공유 장비(PLC, IG, MFC)
from device.plc import AsyncPLC
from device.mfc import AsyncMFC
from device.ig import AsyncIG

# ▶ 런타임 래퍼
from runtime.chamber_runtime import ChamberRuntime
from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime  # type: ignore
from runtime.server_page import ServerPage  # ✅ NEW (server_page.py 위치에 맞게 경로 조정)

# 챔버별 설정
from lib import config_ch1, config_ch2
from lib import config_common as cfgc
from lib import config_local as cfgl  # CHAT_WEBHOOK_URL 로드

# 에러코드 팝업
from PySide6.QtWidgets import QMessageBox
from util.timed_popup import attach_autoclose

# 시스템 로그 저장
from util.app_logging import (
    setup_app_logging,
    install_global_exception_hooks,
    install_asyncio_exception_logging,
    install_warnings_logging,
    install_qt_message_logging,
    uninstall_qt_message_logging,   # ✅ 추가
    get_app_logger,
)

# ───────────────────────────────────────────────────────────
# PLC 로그 출처를 담는 컨텍스트 (CH1 / CH2 / PC 등)
PLC_ORIGIN: ContextVar[Optional[str]] = ContextVar("PLC_ORIGIN", default=None)

class _PLCProxy:
    """
    AsyncPLC에 대한 얇은 래퍼.
    - 메서드 호출 시 ContextVar(PLC_ORIGIN)에 origin을 세팅한 뒤 실제 AsyncPLC 메서드를 호출.
    - 호출이 끝나면 컨텍스트를 원래대로 돌려놓는다.
    """

    def __init__(self, plc: AsyncPLC, origin: str):
        self._plc = plc
        self._origin = origin

    def __getattr__(self, name):
        attr = getattr(self._plc, name)

        # 속성/필드는 그대로 전달
        if not callable(attr):
            return attr

        # ★ is_connected 는 동기 bool 리턴 메서드라서
        #   프리플라이트(_is_dev_connected)에서 동기 방식으로 바로 호출해야 함.
        #   따라서 Proxy에서 비동기 wrapper 로 감싸지 않고 그대로 돌려준다.
        if name == "is_connected":
            return attr

        async def _wrapper(*args, **kwargs):
            token = PLC_ORIGIN.set(self._origin)
            try:
                result = attr(*args, **kwargs)
                if asyncio.iscoroutine(result):
                    return await result
                return result
            finally:
                PLC_ORIGIN.reset(token)

        return _wrapper

# ───────────────────────────────────────────────────────────
CH_HINT_RE = re.compile(r"\[CH\s*(\d)\]|\bCH(?:=|:)?\s*(\d)\b", re.IGNORECASE)
# ───────────────────────────────────────────────────────────

class MainWindow(QWidget):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        super().__init__()
        self.ui = Ui_Form()
        self.ui.setupUi(self)

        # # ✅ CH2 공정 페이지 P.W Select 체크박스 항상 비활성화
        # self.ui.ch2_powerSelect_checkbox.setEnabled(False)

        # # ✅ RF Select(=Power_Select_button) 항상 비활성화
        # if hasattr(self.ui, "Power_Select_button"):
        #     self.ui.Power_Select_button.setEnabled(False)
        #     self.ui.Power_Select_button.setToolTip("비활성화(고정)")

        # ✅ Integration Time 입력칸을 'Process Name' 입력으로 재활용 (CH1/CH2)
        #   - CSV 자동공정: Process_name 표시
        #   - UI 수동공정: 사용자가 입력 → 로그/구글챗/CSV(Process Note)에 기록
        try:
            self.ui.ch1_integrationTime_label.setText("Process Name")
            self.ui.ch2_integrationTime_label.setText("Process Name")
            # CH1 위젯은 오브젝트명이 오타(intergration)라서 그대로 사용
            tip = "공정명(로그/구글챗/CSV Process Note에 기록). Integration Time은 기본 60ms로 고정됩니다."
            self.ui.ch1_intergrationTime_edit.setToolTip(tip)
            self.ui.ch2_integrationTime_edit.setToolTip(tip)
        except Exception:
            pass

        # Power_Select 버튼 토글 → PLC 코일 쓰기 (비동기)
        self.ui.Power_Select_button.toggled.connect(
            lambda on: self._loop.create_task(self._on_power_select_toggled(on))
        )
        
        # ▼ 추가: 텍스트 에디트에서 Tab을 '다음 칸 이동'으로 동작시키기
        self._enable_tab_moves_focus()

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
        self._logger = get_app_logger()

        # --- 스택 및 페이지 매핑 (UI 객체명 고정)
        self._stack: QStackedWidget = self.ui.stackedWidget
        self._pages: dict[str, QWidget] = {
            "pc":  self.ui.page_3,  # Plasma Cleaning
            "ch1": self.ui.page,    # CH1
            "ch2": self.ui.page_2,  # CH2
        }

        # === 공용(공유) 리소스 생성 ===
        # ✅ Chat Notifier를 용도별로 분리 (CH1/CH2/PC/TSP/HOST)
        url = getattr(cfgl, "CHAT_WEBHOOK_URL", None)

        def _new_chat(label: str) -> Optional[ChatNotifier]:
            if not url:
                return None
            c = ChatNotifier(url)
            try:
                c.setObjectName(f"ChatNotifier_{label}")
            except Exception:
                pass
            c.start()
            return c

        self.chat_host = _new_chat("HOST")
        self.chat_ch1  = _new_chat("CH1")
        self.chat_ch2  = _new_chat("CH2")
        self.chat_pc   = _new_chat("PC")
        self.chat_tsp  = _new_chat("TSP")

        # ── 현재 PLC 로그의 소유 챔버 (1/2). 없으면 None → 방송 모드
        self._plc_owner: Optional[int] = None
        self.pc = None  # Plasma Cleaning 런타임 핸들

        # PLC (공유) : 실제 AsyncPLC 인스턴스는 하나만 생성
        self.plc: AsyncPLC = AsyncPLC(logger=self._plc_log)

        # ★ 챔버/플라즈마별 PLC Proxy 생성 (로그 출처 구분용)
        self._plc_ch1 = _PLCProxy(self.plc, "CH1")
        self._plc_ch2 = _PLCProxy(self.plc, "CH2")
        self._plc_pc  = _PLCProxy(self.plc, "PC")

        # ← 옵션: 부팅 직후 PLC 자동 연결 (공용 PLC로 직접 연결)
        async def _boot_plc():
            try:
                await self.plc.connect()
                self._broadcast_log("PLC", "부팅 시 자동 연결 성공")
            except Exception as e:
                self._broadcast_log("PLC", f"부팅 시 자동 연결 실패: {e}")
        self._loop.create_task(_boot_plc())

        # 로그 루트 (NAS 실패 시 런타임 내부에서 폴백 처리)
        self._log_root = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs")

        # ✅ Server 페이지 생성/등록 (단 1회만)
        self.server_page: Optional[QWidget] = None
        try:
            self.server_page = ServerPage(log_root=self._log_root)
            self._stack.addWidget(self.server_page)
            self._pages["server"] = self.server_page

            if hasattr(self.server_page, "set_host_info"):
                self.server_page.set_host_info(cfgc.HOST_SERVER_HOST, int(cfgc.HOST_SERVER_PORT))
            if hasattr(self.server_page, "set_running"):
                self.server_page.set_running(False)

            self._broadcast_log("NET", "Server page added to stackedWidget")
        except Exception as e:
            self.server_page = None
            self._broadcast_log("NET", f"ServerPage init failed: {e!r}")

        if hasattr(self.server_page, "sigHostStart"):
            self.server_page.sigHostStart.connect(self.request_host_start)
            self.server_page.sigHostStop.connect(self.request_host_stop)
            self.server_page.sigHostRestart.connect(self.request_host_restart)

        # ─────────────────────────────────────────────────────
        # CH1/CH2용 IG/MFC 모두 생성해 보관
        #   - IG: 각 챔버 전용 포트
        #   - MFC: 각 챔버 전용 포트
        #   - Plasma Cleaning에서 Gas Flow는 '항상 MFC1의 ch=3' 정책
        # 채널별 스케일을 명시 주입 (CH1/CH2 각각 자신의 설정 사용)
        scale1 = getattr(config_ch1, "MFC_SCALE_FACTORS", getattr(cfgc, "MFC_SCALE_FACTORS", {1: 1.0, 2: 1.0, 3: 10.0}))
        scale2 = getattr(config_ch2, "MFC_SCALE_FACTORS", getattr(cfgc, "MFC_SCALE_FACTORS", {1: 1.0, 2: 10.0, 3: 2.0}))

        self.mfc1: AsyncMFC = AsyncMFC(
            host=getattr(config_ch1, "MFC_TCP_HOST", getattr(cfgc, "MFC_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch1, "MFC_TCP_PORT", 4003),
            enable_verify=False,
            enable_stabilization=True,
            scale_factors=scale1,
        )
        self.mfc2: AsyncMFC = AsyncMFC(
            host=getattr(config_ch2, "MFC_TCP_HOST", getattr(cfgc, "MFC_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch2, "MFC_TCP_PORT", 4006),
            enable_verify=False,
            enable_stabilization=True,
            scale_factors=scale2,
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

        self._pc_log_autoscroll_pending = False

        # === 챔버 런타임 2개 생성 ===
        self.ch1 = ChamberRuntime(
            ui=self.ui,
            chamber_no=1,
            prefix="ch1_",
            loop=self._loop,
            plc=self._plc_ch1,   # ★ CH1 전용 Proxy
            chat=self.chat_ch1,  # CH1 전용 Notifier
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
            plc=self._plc_ch2,   # ★ CH2 전용 Proxy
            chat=self.chat_ch2,  # CH2 전용 Notifier
            cfg=config_ch2,
            supports_rf_cont=True,   # ✅ CH2 RF 연속파워 강제 사용
            log_dir=self._log_root,
            mfc=self.mfc2,
            ig=self.ig2,
            on_plc_owner=self._set_plc_owner,
        )

        try:
            # ── Pre-Sputter: 챔버 전용 런타임 2개로 분리 ───────────────────────────
            self.pre_ch1 = PreSputterRuntime(
                ch1=self.ch1, ch2=None, chat=None, hh=6, mm=0, parallel=False, ui=self.ui
            )
            self.pre_ch1.set_pc_logger(self._append_pc_log_autoscroll)
            self.pre_ch1.start_daily()   # ← 프로그램 시작 시 CH1 자동 예약

            self.pre_ch2 = PreSputterRuntime(
                ch1=None, ch2=self.ch2, chat=None, hh=6, mm=0, parallel=False, ui=self.ui
            )
            self.pre_ch2.set_pc_logger(self._append_pc_log_autoscroll)
            self.pre_ch2.start_daily()   # ← 프로그램 시작 시 CH2 자동 예약
            # ─────────────────────────────────────────────────────────────

            # ★ 라디오 기본값: CH1 선택
            try:
                if hasattr(self.ui, "preSputter_useChamber1_radio"):
                    self.ui.preSputter_useChamber1_radio.setChecked(True)
            except Exception:
                pass

            # ★ UI 버튼을 메인에서 직접 분기 연결(선택된 챔버만 제어)
            try:
                self.ui.preSputter_Start_button.clicked.connect(self._on_presputter_start_clicked)
                self.ui.preSputter_Stop_button.clicked.connect(self._on_presputter_stop_clicked)
            except Exception:
                pass
        except Exception as e:
            self._broadcast_log("Auto", f"PreSputter 예약 초기화 실패: {e!r}")

        # === Plasma Cleaning 런타임 생성 (공유 장치 주입) ===
        try:
            self.pc = PlasmaCleaningRuntime(
                ui=self.ui,
                prefix="PC_",               # PC_* 네이밍 사용
                loop=self._loop,
                log_dir=self._log_root,
                plc=self._plc_pc,          # ★ PC 전용 Proxy
                mfc_gas=self.mfc1,         # Gas Flow는 정책상 항상 MFC1 사용
                mfc_pressure=self.mfc1,         # 초기엔 CH1 기준
                ig=self.ig1,               # IG도 초기엔 CH1 기준
                chat=self.chat_pc,         # PC 전용 Notifier
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
            chat=self.chat_tsp,     # TSP 전용 Notifier 주입
            log_dir=self._log_root, # ★ NAS 로그 루트 전달 (CH/PC와 동일)
        )

        # --- 페이지 네비 버튼 및 라디오 연결
        self._connect_page_buttons()

        # IG 콜백 + SP4용 MFC 전환을 함께 반영
        self._apply_pc_ch_selection()

        # ★ 외부 제어 서버 기동
        self._host_handle = None

        # host → log(tag, text) 콜백
        def _netlog(tag: str, text: str) -> None:
            msg = str(text)
            try:
                sp = getattr(self, "server_page", None)

                t = str(tag or "")
                tu = t.upper()

                self._log_global(tu or t, msg)

                # 1) ServerPage에 먼저 기록(표시는 대문자 통일 권장)
                if sp and hasattr(sp, "append_log"):
                    sp.append_log(tu or t, msg)

               # 2) ✅ 통신/원격 PLC/Host 관련 로그는 ServerPage에만 남기고, 다른 페이지로는 방송 금지
                #    - notify_all(src="HOST") → tag가 "ERROR/HOST" 로 들어옴
                #    - 혹시 다른 HOST 계열 태그가 생겨도 "/HOST" 로 끝나면 같이 차단
                if tu in ("PLC_HOST", "PLC_REMOTE", "NET", "HOST") or tu.endswith("/HOST"):
                    return

                # 3) 그 외 로그는 기존처럼 방송
                self._broadcast_log(tag, msg)

            except Exception as e:
                try:
                    print(f"[netlog] error: {e!r}", file=sys.stderr)
                except Exception:
                    pass

        self._netlog = _netlog
        self._loop.create_task(self._boot_host())

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

        src = "PLC"
        m = msg.lstrip()
        if m.startswith("[") and "]" in m:
            src = m[1:m.index("]")].strip() or "PLC"

        self._log_global(src, msg)

        # 0) ContextVar에 출처가 명시되어 있으면 그쪽으로만 라우팅
        origin = None
        try:
            origin = PLC_ORIGIN.get()
        except LookupError:
            origin = None

        if origin == "CH1" and getattr(self, "ch1", None):
            self.ch1.append_log("PLC", msg)
            return
        if origin == "CH2" and getattr(self, "ch2", None):
            self.ch2.append_log("PLC", msg)
            return
        if origin == "PC" and getattr(self, "pc", None):
            # Plasma Cleaning 전용 로그
            self.pc.append_log("PLC", msg)
            return

        # 1) 메시지 안에 [CH1], [CH2] 힌트가 있으면 → 해당 챔버로
        m = CH_HINT_RE.search(msg)
        if m:
            hinted = next((g for g in m.groups() if g), None)
            hinted_ch = int(hinted) if hinted else 0
            if hinted_ch in (1, 2):
                self._route_log_to(hinted_ch, "PLC", msg)
                return
            
        # 2) 힌트가 없고, 현재 PLC 소유 챔버(_plc_owner)가 있으면 그쪽으로
        if self._plc_owner in (1, 2):
            self._route_log_to(self._plc_owner, "PLC", msg)
            return
        
        # 3) (fallback) Plasma Cleaning 실행 중이면 PC 로그로만 보내기
        try:
            pc = getattr(self, "pc", None)
            if pc:
                running = False
                # is_running이 property인 경우
                ir = getattr(pc, "is_running", None)
                if isinstance(ir, bool):
                    running = ir
                elif callable(ir):
                    # 혹시 메서드 형태로 구현된 다른 런타임과도 호환
                    running = bool(ir())
                else:
                    running = bool(getattr(pc, "_running", False))

                if running:
                    pc.append_log("PLC(Global)", msg)
                    return
        except Exception:
            pass

        # 4) 기본: 방송 모드(CH1/CH2)
        if getattr(self, "ch1", None):
            self.ch1.append_log("PLC(Global)", msg)
        if getattr(self, "ch2", None):
            self.ch2.append_log("PLC(Global)", msg)

    def _broadcast_log(self, source: str, msg: str) -> None:
        try:
            self._log_global(source, msg)
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

    def _log_global(self, source: str, msg: str) -> None:
        try:
            src = (source or "")
            src_u = src.upper()
            level = logging.INFO

            if (
                src_u.startswith("ERROR") or src_u.startswith("ERR") or
                "ERROR" in src_u or msg.strip().startswith("❌")
            ):
                level = logging.ERROR
            elif (
                src_u.startswith("WARN") or "WARN" in src_u or
                msg.strip().startswith("⚠")
            ):
                level = logging.WARNING

            # ✅ 시스템 로그는 "중복 최소화"가 목적이므로 WARNING+만 저장
            if level < logging.WARNING:
                return
            self._logger.log(level, "[%s] %s", source, msg)

        except Exception:
            pass

    def _connect_page_buttons(self) -> None:
        if getattr(self, "_page_buttons_bound", False):
            return
        self._page_buttons_bound = True

        self.ui.pc_btnGoCh1.clicked.connect(lambda: self._switch_page("ch1"))
        self.ui.pc_btnGoCh2.clicked.connect(lambda: self._switch_page("ch2"))
        self.ui.ch1_btnGoPC.clicked.connect(lambda: self._switch_page("pc"))
        self.ui.ch1_btnGoCh2.clicked.connect(lambda: self._switch_page("ch2"))
        self.ui.ch2_btnGoPC.clicked.connect(lambda: self._switch_page("pc"))
        self.ui.ch2_btnGoCh1.clicked.connect(lambda: self._switch_page("ch1"))

        # 라디오 그룹 ‘그룹 단위 배타’ 보강 (엣지케이스 방지)
        for gname in ("buttonGroup", "buttonGroup_2"):
            grp = getattr(self.ui, gname, None)
            if grp:
                grp.setExclusive(True)  # ← 한번만 못 박아 둠

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

        # ✅ PC 페이지의 Server 버튼 → server 페이지로 전환
        btn_server = getattr(self.ui, "Server_button", None)
        if btn_server is not None:
            btn_server.clicked.connect(lambda: self._switch_page("server"))
            
        # ✅ Server 페이지 우상단 네비 버튼 연결
        sp = getattr(self, "server_page", None)
        if sp is not None:
            if hasattr(sp, "btnGoPC"):
                sp.btnGoPC.clicked.connect(lambda: self._switch_page("pc"))
            if hasattr(sp, "btnGoCh1"):
                sp.btnGoCh1.clicked.connect(lambda: self._switch_page("ch1"))
            if hasattr(sp, "btnGoCh2"):
                sp.btnGoCh2.clicked.connect(lambda: self._switch_page("ch2"))

    def _on_pc_radio_toggled(self, checked: bool) -> None:
        if not checked:
            return

        # 실행 중 전환 금지 (property/메서드/내부플래그 모두 안전 처리)
        pc = getattr(self, "pc", None)
        if pc:
            running = False
            ir = getattr(pc, "is_running", None)
            if callable(ir):
                running = bool(ir())
            elif isinstance(ir, bool):
                running = ir
            else:
                running = bool(getattr(pc, "_running", False))

            if running:
                try:
                    pc.append_log("PC", "플라즈마 클리닝 실행 중에는 챔버 전환이 불가합니다.")
                except Exception:
                    pass
                # 라디오를 이전 선택으로 되돌림
                try:
                    prev = getattr(self, "_pc_use_ch", 1)
                    if prev == 1 and hasattr(self.ui, "PC_useChamber1_radio"):
                        self.ui.PC_useChamber1_radio.setChecked(True)
                    elif hasattr(self.ui, "PC_useChamber2_radio"):
                        self.ui.PC_useChamber2_radio.setChecked(True)
                except Exception:
                    pass
                return

        # 현재 라디오 상태 읽기
        try:
            ch2_on = hasattr(self.ui, "PC_useChamber2_radio") and self.ui.PC_useChamber2_radio.isChecked()
        except Exception:
            ch2_on = False
        ch = 2 if ch2_on else 1

        # 동일 선택이면 무시
        if ch == getattr(self, "_pc_use_ch", None):
            return

        # 상태 저장 후 선택 반영
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

        # ★ IG 디바이스 자체도 교체 (wait_for_base_pressure가 self.ig를 사용)
        try:
            pc.set_ig_device(ig)
        except Exception as e:
            self._broadcast_log("PC", f"IG 디바이스 주입 실패: {e!r}")

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
            mfc_pressure = self.mfc1 if ch == 1 else self.mfc2
            pc.set_mfcs(mfc_gas=self.mfc1, mfc_pressure=mfc_pressure)
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

    def _switch_page(self, key: Literal["pc", "ch1", "ch2", "server"]) -> None:
        page = self._pages.get(key)
        if page:
            self._stack.setCurrentWidget(page)

    async def _on_power_select_toggled(self, on: bool) -> None:
        try:
            await self.plc.power_select(on=on)
        except Exception as e:
            self._broadcast_log("PLC", f"Power Select 실패(PLC 연결 확인): {e!r}")
            # 버튼 상태 원복(무한 재호출 방지)
            try:
                btn = self.ui.Power_Select_button
                btn.blockSignals(True)
                btn.setChecked(not on)
            finally:
                try:
                    btn.blockSignals(False)
                except Exception:
                    pass

    def closeEvent(self, event: QCloseEvent) -> None:
        # 1) 외부 제어 서버 먼저 종료 요청
        try:
            self._loop.create_task(self._stop_host())
        except Exception:
            pass

        # 2) 공유 장치/런타임 정리
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
        for c in (getattr(self, "chat_host", None),
                getattr(self, "chat_ch1", None),
                getattr(self, "chat_ch2", None),
                getattr(self, "chat_pc", None),
                getattr(self, "chat_tsp", None)):
            try:
                if c:
                    c.shutdown()
            except Exception:
                pass
        try:
            uninstall_qt_message_logging(get_app_logger())
        except Exception:
            pass
        event.accept()
        super().closeEvent(event)

    def _selected_presputter_ch(self) -> int:
        """Pre-Sputter 라디오 상태로 선택 챔버 반환(기본 1)."""
        try:
            rb2 = getattr(self.ui, "preSputter_useChamber2_radio", None)
            return 2 if (rb2 and rb2.isChecked()) else 1
        except Exception:
            return 1

    def _on_presputter_start_clicked(self) -> None:
        ch = self._selected_presputter_ch()
        rt = self.pre_ch1 if ch == 1 else self.pre_ch2
        if rt:
            rt.schedule_from_ui()  # ← 해당 챔버만 예약 갱신/시작

    def _on_presputter_stop_clicked(self) -> None:
        ch = self._selected_presputter_ch()
        rt = self.pre_ch1 if ch == 1 else self.pre_ch2
        if rt:
            rt.stop(silent=False)  # ← 해당 챔버만 예약 취소

    def _enable_tab_moves_focus(self) -> None:
        # ✔ 각각 찾아서 합치기
        edits = chain(
            self.findChildren(QPlainTextEdit),
            self.findChildren(QTextEdit),
        )
        for w in edits:
            try:
                w.setTabChangesFocus(True)   # Tab/Shift+Tab → 다음/이전 위젯 포커스
                w.setFocusPolicy(Qt.StrongFocus)  # (안전) Tab 포커스 허용
            except Exception:
                pass

    async def _boot_host(self) -> None:
        # 이미 떠있으면 중복 실행 방지
        if getattr(self, "_host_handle", None):
            self._netlog("NET", "Host already running")
            try:
                sp = getattr(self, "server_page", None)
                if sp and hasattr(sp, "set_running"):
                    sp.set_running(True)
            except Exception:
                pass
            return

        try:
            self._host_handle = await install_host(
                host=cfgc.HOST_SERVER_HOST,
                port=int(cfgc.HOST_SERVER_PORT),
                log=self._netlog,
                plc=self.plc,
                ch1=self.ch1,
                ch2=self.ch2,
                pc=self.pc,
                runtime_state=runtime_state,
                chat=self.chat_host,
                popup=self._host_popup,
            )

            # ✅ server 페이지 상태 RUNNING 갱신
            sp = getattr(self, "server_page", None)
            if sp and hasattr(sp, "set_running"):
                sp.set_running(True)
            if sp and hasattr(sp, "set_host_info"):
                sp.set_host_info(cfgc.HOST_SERVER_HOST, int(cfgc.HOST_SERVER_PORT))

            # ✅ NET 로그도 server 페이지로
            self._netlog("NET", f"Host started on {cfgc.HOST_SERVER_HOST}:{cfgc.HOST_SERVER_PORT}")

        except Exception as e:
            sp = getattr(self, "server_page", None)
            if sp and hasattr(sp, "set_running"):
                sp.set_running(False)
            self._netlog("NET", f"Host start failed: {e!r}")

    async def _stop_host(self) -> None:
        try:
            hh = getattr(self, "_host_handle", None)
            if not hh:
                self._netlog("NET", "Host already stopped")
            else:
                await hh.aclose()
                self._host_handle = None
                self._netlog("NET", "Host stopped")
        finally:
            sp = getattr(self, "server_page", None)
            if sp and hasattr(sp, "set_running"):
                sp.set_running(False)
    
    def _host_popup(self, title: str, text: str) -> None:
        box = QMessageBox(self)
        box.setWindowTitle(title)
        box.setText(text)
        box.setIcon(QMessageBox.Critical)
        box.setStandardButtons(QMessageBox.Ok)
        box.open()
        attach_autoclose(box, ms=0)  # 자동닫힘 원치 않으면 0

    async def _restart_host(self) -> None:
        await self._stop_host()
        await self._boot_host()

    def request_host_start(self) -> None:
        self._loop.create_task(self._boot_host())

    def request_host_stop(self) -> None:
        self._loop.create_task(self._stop_host())

    def request_host_restart(self) -> None:
        self._loop.create_task(self._restart_host())

    def _append_pc_log_autoscroll(self, msg: str) -> None:
        w = getattr(self.ui, "pc_logMessage_edit", None)
        if not w:
            return

        line = str(msg)
        sb = w.verticalScrollBar()

        # 사용자가 최하단을 보고 있을 때만 바닥 고정
        stick_to_bottom = True
        try:
            stick_to_bottom = (sb.value() >= (sb.maximum() - 2))
        except Exception:
            pass

        w.appendPlainText(line)

        if stick_to_bottom:
            if getattr(self, "_pc_log_autoscroll_pending", False):
                return
            self._pc_log_autoscroll_pending = True

            def _scroll_bottom():
                self._pc_log_autoscroll_pending = False
                sbb = w.verticalScrollBar()
                sbb.setValue(sbb.maximum())
                w.ensureCursorVisible()

            # 레이아웃 계산 이후에 최하단 재보정
            QTimer.singleShot(0, _scroll_bottom)

def main() -> int:
    _logger = setup_app_logging(
        app_name="CH_1_2_program",
        file_level=logging.INFO,      # ✅ INFO까지 파일에 저장
        enable_console=False          # 콘솔은 꺼버림 (GUI exe라서)
    )
    install_global_exception_hooks(_logger)
    install_warnings_logging(_logger)

    app = QApplication(sys.argv)

    install_qt_message_logging(_logger)

    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)

    install_asyncio_exception_logging(loop, _logger)

    w = MainWindow(loop)
    w.show()
    
    try:
        with loop:
            loop.run_forever()
    finally:
        # ✅ closeEvent를 안 타고 빠져나와도 Qt handler 원복
        try:
            uninstall_qt_message_logging(_logger)
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        from multiprocessing import freeze_support
        freeze_support()
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    raise SystemExit(main())


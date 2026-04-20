# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import asyncio
import re
import atexit
import logging
import contextlib
from typing import Optional, Literal
from pathlib import Path
from contextvars import ContextVar

# (선택) 개발 실행 시 import 깨짐 방지
_BASE_DIR = Path(__file__).resolve().parent
if str(_BASE_DIR) not in sys.path:
    sys.path.insert(0, str(_BASE_DIR))

# ✅ Windows 이벤트루프 정책은 Qt/qasync/import 전에 먼저 고정
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 추가: host 설치 유틸
# ✅ 내부 모듈 import는 그 다음
from host.setup import install_host
from controller.runtime_state import runtime_state

from PySide6.QtWidgets import QApplication, QWidget, QStackedWidget, QPlainTextEdit, QTextEdit
from PySide6.QtCore import Qt, QTimer
from itertools import chain  # ← 추가
from PySide6.QtGui import QCloseEvent
from qasync import QEventLoop

# ✅ Qt(C++) 객체가 이미 delete된 상태인지 체크 (Access Violation 방어)
try:
    from shiboken6 import isValid as _qt_is_valid
except Exception:
    def _qt_is_valid(obj) -> bool:
        return obj is not None

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

# ✅ Runtime Dump(수동 상태 스냅샷) - PyInstaller 누락 방지용(없어도 크래시 안 나게)
try:
    from util.runtime_dump import request_dump
except Exception:
    request_dump = None

# 시스템 로그 저장
from util.app_logging import (
    setup_app_logging,
    install_global_exception_hooks,
    install_asyncio_exception_logging,
    install_warnings_logging,
    install_qt_message_logging,
    uninstall_qt_message_logging,
    install_signal_logging,
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

        # ✅ settings.json → config 모듈에 안전하게 덮어쓰기
        try:
            from lib._config_loader import load_settings
            load_settings()
        except Exception as e:
            try:
                print(f"[Config] startup apply failed: {e!r}", file=sys.stderr)
            except Exception:
                pass

        # ✅ Config 팝업 인스턴스 보관(가비지컬렉션/중복창 방지)
        self._boot_plc_task: Optional[asyncio.Task] = None
        self._host_task: Optional[asyncio.Task] = None

        # ✅ 종료 시퀀스 상태 플래그
        self._closing_started: bool = False
        self._close_ready: bool = False
        self._close_task: Optional[asyncio.Task] = None

        # ✅ 공정 중 종료 차단 팝업 중복 방지
        self._close_block_popup_open: bool = False

        # ✅ 종료 확인 팝업 중복 방지
        self._close_confirm_popup_open: bool = False

        # ✅ 공정 중 종료 시도 dump 중복 방지
        self._close_block_dumped_once: bool = False

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

        # ✅ loop/logger를 먼저 준비(시그널 핸들러에서 사용하므로)
        self._loop = loop or asyncio.get_event_loop()
        self._logger = get_app_logger()

        # Power_Select 버튼 토글 → PLC 코일 쓰기 (비동기)
        self.ui.Power_Select_button.toggled.connect(
            lambda on: self._loop.create_task(self._on_power_select_toggled(on))
        )
        
        # ▼ 추가: 텍스트 에디트에서 Tab을 '다음 칸 이동'으로 동작시키기
        self._enable_tab_moves_focus()

        self._apply_ui_defaults_from_config(overwrite=False)

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
            except asyncio.CancelledError:
                return
            except Exception as e:
                self._broadcast_log("PLC", f"부팅 시 자동 연결 실패: {e}")

            # 종료 시작 후에는 logger를 새로 띄우지 않음
            if self._closing_started:
                return

            try:
                await self.plc.start_plc_coil_csv_logger(
                    interval_s=5.0,
                    nas_dir=r"\\VanaM_NAS\VanaM_Sputter\Sputter\Logs\CH1&2\CH1&2_PLC",
                    local_dir=None,
                    keys=None,
                )
                self._broadcast_log("PLC", "PLC COIL CSV 로깅 시작(5s)")
            except asyncio.CancelledError:
                return
            except Exception as e:
                self._broadcast_log("PLC", f"PLC COIL CSV 로깅 시작 실패: {e!r}")

        self._boot_plc_task = self._loop.create_task(_boot_plc())

        # 로그 루트 (NAS 실패 시 런타임 내부에서 폴백 처리)
        self._log_root = Path(r"\\VanaM_NAS\VanaM_Sputter\Sputter\Logs\CH1&2")

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

        def _new_mfc(**kwargs):
            try:
                return AsyncMFC(**kwargs)
            except TypeError:
                # cfg를 아직 지원 안 하는 버전이어도 기존처럼 동작하게 폴백
                kwargs.pop("cfg", None)
                return AsyncMFC(**kwargs)

        def _new_ig(**kwargs):
            try:
                return AsyncIG(**kwargs)
            except TypeError:
                kwargs.pop("cfg", None)
                return AsyncIG(**kwargs)

        self.mfc1: AsyncMFC = _new_mfc(
            host=getattr(config_ch1, "MFC_TCP_HOST", getattr(cfgc, "MFC_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch1, "MFC_TCP_PORT", 4003),
            enable_verify=False,
            enable_stabilization=True,
            scale_factors=scale1,
            cfg=config_ch1,   # ✅ 추가
        )

        self.mfc2: AsyncMFC = _new_mfc(
            host=getattr(config_ch2, "MFC_TCP_HOST", getattr(cfgc, "MFC_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch2, "MFC_TCP_PORT", 4006),
            enable_verify=False,
            enable_stabilization=True,
            scale_factors=scale2,
            cfg=config_ch2,   # ✅ 추가
        )

        self.ig1: AsyncIG = _new_ig(
            host=getattr(config_ch1, "IG_TCP_HOST", getattr(cfgc, "IG_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch1, "IG_TCP_PORT", 4001),
            cfg=config_ch1,   # ✅ 추가
        )

        self.ig2: AsyncIG = _new_ig(
            host=getattr(config_ch2, "IG_TCP_HOST", getattr(cfgc, "IG_TCP_HOST", "192.168.1.50")),
            port=getattr(config_ch2, "IG_TCP_PORT", 4002),
            cfg=config_ch2,   # ✅ 추가
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

        # ✅ PC ↔ Main Process 로그 연동 콜백 등록
        self.ch1.set_main_done_callback(self._on_main_done)
        self.ch2.set_main_done_callback(self._on_main_done)

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
            # CH2는 기본 자동 예약하지 않음
            # ─────────────────────────────────────────────────────────────

            # ★ 라디오 기본값: CH1 선택
            try:
                if hasattr(self.ui, "preSputter_useChamber1_radio"):
                    self.ui.preSputter_useChamber1_radio.setChecked(True)
            except Exception:
                pass

            # ★ UI 버튼을 메인에서 직접 분기 연결(선택된 챔버만 제어)
            try:
                self.ui.preSputter_Start_button.clicked.connect(self._on_presputter_reserve_clicked)
                self.ui.preSputter_Stop_button.clicked.connect(self._on_presputter_cancel_clicked)
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

            # ✅ 카메라 레코더 생성 및 각 런타임에 주입
            try:
                from util.camera_recorder import CameraRecorder
                _recorder = CameraRecorder(camera_index=1, interval=1.0)
                self.ch1.camera_recorder = _recorder
                self.ch2.camera_recorder = _recorder
                self.pc.camera_recorder  = _recorder
                self._broadcast_log("CAM", "CameraRecorder 초기화 성공")
            except Exception as e:
                self._broadcast_log("CAM", f"CameraRecorder 초기화 실패: {e!r}")

            # ✅ PC GDrive 저장 경로 주입 + 콜백 등록
            self._pending_log: dict = {}
            self.pc.set_gdrive_log_dir(
                Path(getattr(cfgc, "GDRIVE_LOG_DIR",
                             "G:/공유 드라이브/VanaM_Sputter/Process_log"))
            )
            self.pc.set_pc_done_callback(self._on_pc_done)

        except Exception as e:
            self.pc = None
            try:
                self._broadcast_log("PC", f"Failed to initialize PlasmaCleaningRuntime: {e!r}")
            except Exception:
                pass

        # === TSP 런타임 생성 ===
        tsp_addr = getattr(cfgc, "TSP_RS232_ADDR", getattr(cfgc, "TSP_ADDR", 0x80))
        self.tsp_ctrl = TSPPageController(
            ui=self.ui,
            host=cfgc.TSP_TCP_HOST,
            tcp_port=cfgc.TSP_TCP_PORT,
            addr=int(tsp_addr),
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

        # ✅ 현재 host가 어떤 주소/포트로 떠 있어야 하는지 추적
        self._host_bound = (
            str(getattr(cfgc, "HOST_SERVER_HOST", "0.0.0.0")),
            int(getattr(cfgc, "HOST_SERVER_PORT", 0)),
        )

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
        self._host_task = self._loop.create_task(self._boot_host())

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

        # ✅ (추가) PC 페이지의 State Dump 버튼 → runtime dump 저장
        btn_dump = getattr(self.ui, "RuntimeDump_button", None)
        if btn_dump is not None:
            btn_dump.clicked.connect(self._on_runtime_dump_clicked)

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

    # ── Plasma Cleaning ↔ Main Process 로그 연동 ─────────────────────
    def _on_pc_done(self, ch: int, process_name: str) -> None:
        """
        PC 완료 시 plasma_cleaning_runtime.py가 호출하는 콜백.
        저장은 plasma_cleaning_runtime이 직접 처리하므로 여기서는 하지 않음.
        pending_log 정리만 담당.
        """
        if not hasattr(self, "_pending_log"):
            self._pending_log = {}

        ch_log = self._pending_log.setdefault(ch, {})

        # 이전 공정 잔존 데이터 정리 (stale 중복 저장 원인이었던 부분)
        if process_name:
            ch_log.pop(process_name, None)
        
    def _on_main_done(self, ch, process_name, data_logger, pc_params_override=None):
        """
        Main Process 완료 시 chamber_runtime.py가 호출하는 콜백.
        - pending_log에 PC 데이터가 있으면 merge 저장
        - 없으면 Main 단독 행으로 저장 (기존 동작 유지)
        반환값: PC params dict (chamber_runtime이 gdrive_save에 전달할 값), 없으면 None
        """
        if not hasattr(self, "_pending_log"):
            self._pending_log = {}

        ch_log = self._pending_log.setdefault(ch, {})

        if not process_name:
            return None

        entry = ch_log.get(process_name, {})

        if entry.get("pc") is not None:
            # PC 데이터 있음 → 꺼내서 반환 (병합 저장)
            pc_params = entry["pc"]
            del ch_log[process_name]
            return pc_params

        # ✅ PC 없음 → 즉시 None 반환 (PENDING 반환하지 않음)
        # 마커도 남기지 않음 — 다음 공정에 영향 주지 않도록
        with contextlib.suppress(Exception):
            ch_log.pop(process_name, None)
            ch_log.pop(f"_done_{process_name}", None)
        return None

    async def _save_pc_only_row(self, ch: int, pc_params: dict) -> None:
        """PC 데이터만으로 xlsx에 단독 행 저장."""
        import contextlib
        try:
            from util.gdrive_logger import save_pc_only
            from lib import config_common as _cfgc
            from pathlib import Path
            log_dir = Path(getattr(_cfgc, "GDRIVE_LOG_DIR",
                                   "G:/공유 드라이브/VanaM_Sputter/Process_log"))
            arc_thresh  = getattr(_cfgc, "GDRIVE_ARC_ALERT_THRESH", 5)
            refp_warn   = getattr(_cfgc, "GDRIVE_REF_P_WARN_W", 20.0)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: save_pc_only(ch, pc_params, log_dir, arc_thresh, refp_warn),
            )
        except Exception as e:
            self._broadcast_log("GDrive", f"PC 단독 행 저장 실패: {e!r}")

    async def _save_merged_row(self, ch: int, process_name: str,
                                pc_params: dict, data_logger) -> None:
        try:
            from util.gdrive_logger import save_process_log 
            from lib import config_common as _cfgc
            await save_process_log(   
                ch=ch,
                data_logger=data_logger,
                operator=getattr(data_logger, "process_params", {}).get("operator", ""),
                substrate=getattr(data_logger, "process_params", {}).get("substrate", ""),
                note=getattr(data_logger, "process_params", {}).get("note", ""),
                pc_params=pc_params,
                log_dir=None,
                arc_thresh=getattr(_cfgc, "GDRIVE_ARC_ALERT_THRESH", 5),
                refp_warn=getattr(_cfgc, "GDRIVE_REF_P_WARN_W", 20.0),
                webhook_url=getattr(_cfgc, "CHAT_WEBHOOK_MONITOR_URL", ""),
                arc_alert_sent=False,
            )
        except Exception as e:
            self._broadcast_log("GDrive", f"PC+Main merge 저장 실패: {e!r}")

    def _switch_page(self, key: Literal["pc", "ch1", "ch2", "server"]) -> None:
        page = self._pages.get(key)
        if page:
            self._stack.setCurrentWidget(page)

    def _apply_ui_defaults_from_config(self, *, overwrite: bool) -> None:
        """
        overwrite=False: 사용자가 UI에 이미 뭔가 입력해 둔 경우 덮어쓰지 않음(안전)
        overwrite=True : config 값으로 강제로 UI를 갱신
        """
        # TSP: config_common 기반
        def _set_plain(w, value: str) -> None:
            if not w:
                return
            if (not overwrite) and w.toPlainText().strip():
                return
            w.setPlainText(str(value))

        _set_plain(getattr(self.ui, "TSP_targetPressure_edit", None), getattr(cfgc, "TSP_UI_DEFAULT_TARGET", "2.5e-7"))
        _set_plain(getattr(self.ui, "TSP_setCycle_edit", None), getattr(cfgc, "TSP_UI_DEFAULT_CYCLES", "10"))

        # Plasma Cleaning: config_common 기본값 기반
        _set_plain(
            getattr(self.ui, "PC_targetPressure_edit", None),
            str(getattr(cfgc, "PC_DEFAULT_TARGET_PRESSURE_TORR", 5.0e-6))
        )
        _set_plain(
            getattr(self.ui, "PC_gasFlow_edit", None),
            str(getattr(cfgc, "PC_DEFAULT_GAS_FLOW_SCCM", 0.0))
        )
        _set_plain(
            getattr(self.ui, "PC_workingPressure_edit", None),
            str(getattr(cfgc, "PC_DEFAULT_SP4_SETPOINT_MTORR", 2.0))
        )
        _set_plain(
            getattr(self.ui, "PC_rfPower_edit", None),
            str(getattr(cfgc, "PC_DEFAULT_RF_POWER_W", 100.0))
        )
        _set_plain(
            getattr(self.ui, "PC_ProcessTime_edit", None),
            str(getattr(cfgc, "PC_DEFAULT_PROCESS_TIME_MIN", 1.0))
        )

        # CH1/CH2: 현재 config로 기본값이 확정된 항목만 UI 반영
        _set_plain(
            getattr(self.ui, "ch1_basePressure_edit", None),
            str(getattr(config_ch1, "PROCESS_DEFAULT_BASE_PRESSURE",
                        getattr(cfgc, "PROCESS_DEFAULT_BASE_PRESSURE", 1e-5)))
        )
        _set_plain(
            getattr(self.ui, "ch2_basePressure_edit", None),
            str(getattr(config_ch2, "PROCESS_DEFAULT_BASE_PRESSURE",
                        getattr(cfgc, "PROCESS_DEFAULT_BASE_PRESSURE", 1e-5)))
        )

    def _runtime_is_running(self, obj) -> bool:
        if not obj:
            return False

        try:
            ir = getattr(obj, "is_running", None)

            if callable(ir):
                return bool(ir())

            if isinstance(ir, bool):
                return ir

            return bool(getattr(obj, "_running", False))
        except Exception:
            return False

    def _active_run_labels(self) -> list[str]:
        active: list[str] = []

        try:
            if self._runtime_is_running(getattr(self, "ch1", None)):
                active.append("CH1 Sputter")
        except Exception:
            pass

        try:
            if self._runtime_is_running(getattr(self, "ch2", None)):
                active.append("CH2 Sputter")
        except Exception:
            pass

        try:
            if self._runtime_is_running(getattr(self, "pc", None)):
                ch = getattr(self, "_pc_use_ch", None)
                if ch in (1, 2):
                    active.append(f"Plasma Cleaning (CH{ch})")
                else:
                    active.append("Plasma Cleaning")
        except Exception:
            pass

        try:
            if runtime_state.is_running("tsp"):
                active.append("TSP")
        except Exception:
            pass

        return list(dict.fromkeys(active))

    def _build_runtime_dump_extra(self) -> dict:
        extra = {
            "runtime_state": runtime_state.snapshot(),
            "pc_use_ch": getattr(self, "_pc_use_ch", None),
            "plc_owner": getattr(self, "_plc_owner", None),
            "closing_started": getattr(self, "_closing_started", False),
            "close_ready": getattr(self, "_close_ready", False),
        }

        for name in ("plc", "mfc1", "mfc2", "ig1", "ig2", "ch1", "ch2", "pc", "tsp_ctrl", "server_page"):
            try:
                extra[name] = getattr(self, name, None)
            except Exception:
                extra[name] = None

        return extra

    def _request_runtime_dump_internal(self, reason: str, *, log_tag: str = "WARN/DUMP") -> Optional[str]:
        if request_dump is None:
            self._broadcast_log("ERROR/DUMP", "util.runtime_dump import 실패(request_dump=None)")
            return None

        try:
            path = request_dump(
                ui=self.ui,
                loop=self._loop,
                log_root=self._log_root,
                extra_objects=self._build_runtime_dump_extra(),
                reason=reason,
            )
            self._broadcast_log(log_tag, f"Runtime dump saved({reason}): {path}")
            return str(path)
        except RuntimeError as e:
            self._broadcast_log("WARN/DUMP", f"Runtime dump skipped({reason}): {e}")
            return None
        except Exception as e:
            self._broadcast_log("ERROR/DUMP", f"Runtime dump failed({reason}): {e!r}")
            return None

    def _on_runtime_dump_clicked(self) -> None:
        btn = getattr(self.ui, "RuntimeDump_button", None)

        try:
            try:
                if btn is not None:
                    btn.setEnabled(False)
            except Exception:
                pass

            path = self._request_runtime_dump_internal("manual_button", log_tag="WARN/DUMP")

            if path is None and request_dump is None:
                try:
                    QMessageBox.warning(self, "Dump", "util/runtime_dump.py(request_dump) 로드 실패")
                except Exception:
                    pass

        except Exception as e:
            self._broadcast_log("ERROR/DUMP", f"Runtime dump failed: {e!r}")
            try:
                QMessageBox.warning(self, "Dump", f"Runtime dump failed: {e!r}")
            except Exception:
                pass

        finally:
            try:
                if btn is not None:
                    QTimer.singleShot(500, lambda: btn.setEnabled(True))
            except Exception:
                pass

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

    async def _shutdown_app_async(self) -> None:
        with contextlib.suppress(Exception):
            self._logger.warning(
                "_shutdown_app_async entered. runtime_state=%s",
                runtime_state.snapshot(),
            )

        self._broadcast_log("WARN/EXIT", "앱 종료 정리 시작")

        # 1) 시작/적용 task 먼저 정리
        for attr in ("_boot_plc_task", "_host_task"):
            t = getattr(self, attr, None)
            if t and not t.done():
                t.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await t
            setattr(self, attr, None)

        # 2) 외부 제어 서버 종료
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(self._stop_host(), timeout=3.0)

        # 3) PLC COIL CSV logger 종료
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(self.plc.stop_plc_coil_csv_logger(), timeout=3.0)

        # 4) 런타임 종료
        for rt in (
            getattr(self, "pc", None),
            getattr(self, "ch1", None),
            getattr(self, "ch2", None),
        ):
            if not rt:
                continue

            fn_async = getattr(rt, "shutdown_fast_async", None)
            if callable(fn_async):
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await asyncio.wait_for(fn_async(), timeout=8.0)
            else:
                fn = getattr(rt, "shutdown_fast", None)
                if callable(fn):
                    with contextlib.suppress(Exception):
                        fn()

        # 5) 공유 PLC 종료 (heartbeat / socket / to_thread 정리)
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(self.plc.close(), timeout=3.0)

        # 6) Chat notifier 종료
        for c in (
            getattr(self, "chat_host", None),
            getattr(self, "chat_ch1", None),
            getattr(self, "chat_ch2", None),
            getattr(self, "chat_pc", None),
            getattr(self, "chat_tsp", None),
        ):
            with contextlib.suppress(Exception):
                if c:
                    c.shutdown()

        # 7) Qt 로그 핸들러 원복
        with contextlib.suppress(Exception):
            uninstall_qt_message_logging(get_app_logger())

        with contextlib.suppress(Exception):
            self._logger.warning("_shutdown_app_async completed")

        self._broadcast_log("WARN/EXIT", "앱 종료 정리 완료")

    def closeEvent(self, event: QCloseEvent) -> None:
        # ✅ cleanup 완료 후 다시 들어온 close면 실제 종료
        if self._close_ready:
            event.accept()
            return super().closeEvent(event)

        # ✅ 이미 종료 시퀀스 시작됨 → 중복 close 무시
        if self._closing_started:
            event.ignore()
            return

        # ✅ 공정 실행 중이면 종료 차단
        active = self._active_run_labels()
        if active:
            detail = ", ".join(active)

            with contextlib.suppress(Exception):
                self._logger.warning(
                    "closeEvent blocked while process running. active=%s, runtime_state=%s",
                    detail,
                    runtime_state.snapshot(),
                )

            self._broadcast_log("WARN/EXIT", f"프로그램 종료 차단: 실행 중 공정 = {detail}")

            # 종료 시도 당시 상태를 1회만 dump 저장
            if not self._close_block_dumped_once:
                self._close_block_dumped_once = True
                with contextlib.suppress(Exception):
                    self._request_runtime_dump_internal("close_blocked_while_running", log_tag="WARN/DUMP")

            if not self._close_block_popup_open:
                self._close_block_popup_open = True
                try:
                    box = QMessageBox(self)
                    box.setIcon(QMessageBox.Warning)
                    box.setWindowTitle("프로그램 종료 차단")
                    box.setText(
                        "현재 공정이 진행 중이므로 프로그램을 종료할 수 없습니다.\n\n"
                        f"실행 중: {detail}\n\n"
                        "공정을 먼저 정상 종료 또는 정지한 뒤 프로그램을 닫아주세요."
                    )
                    box.setStandardButtons(QMessageBox.Ok)
                    box.finished.connect(lambda *_: setattr(self, "_close_block_popup_open", False))
                    attach_autoclose(box, ms=5000)
                    box.show()
                except Exception:
                    self._close_block_popup_open = False

            event.ignore()
            return

        # ✅ 공정이 없을 때도 종료 전 사용자 확인
        if self._close_confirm_popup_open:
            event.ignore()
            return

        self._close_confirm_popup_open = True
        try:
            reply = QMessageBox.question(
                self,
                "프로그램 종료 확인",
                "프로그램을 종료하시겠습니까?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
        finally:
            self._close_confirm_popup_open = False

        if reply != QMessageBox.Yes:
            with contextlib.suppress(Exception):
                self._logger.warning("closeEvent canceled by user")
            self._broadcast_log("WARN/EXIT", "사용자가 프로그램 종료를 취소했습니다.")
            event.ignore()
            return

        # 종료 확인 완료 → 실제 종료 시작
        self._close_block_dumped_once = False
        self._closing_started = True
        event.ignore()

        try:
            spontaneous = event.spontaneous() if hasattr(event, "spontaneous") else None
        except Exception:
            spontaneous = None

        with contextlib.suppress(Exception):
            self._logger.warning(
                "closeEvent accepted. spontaneous=%s, runtime_state=%s",
                spontaneous,
                runtime_state.snapshot(),
            )

        self._broadcast_log("WARN/EXIT", "프로그램 종료 시퀀스 시작")

        with contextlib.suppress(Exception):
            self.setEnabled(False)

        async def _run_close():
            try:
                await self._shutdown_app_async()
            finally:
                self._close_ready = True
                QTimer.singleShot(0, self.close)

        try:
            self._close_task = self._loop.create_task(_run_close())
        except Exception:
            self._close_ready = True
            event.accept()
            super().closeEvent(event)

    def _selected_presputter_ch(self) -> int:
        """Pre-Sputter 라디오 상태로 선택 챔버 반환(기본 1)."""
        try:
            rb2 = getattr(self.ui, "preSputter_useChamber2_radio", None)
            return 2 if (rb2 and rb2.isChecked()) else 1
        except Exception:
            return 1

    def _on_presputter_reserve_clicked(self) -> None:
        ch = self._selected_presputter_ch()
        rt = self.pre_ch1 if ch == 1 else self.pre_ch2
        if rt:
            rt.schedule_from_ui()  # ← 해당 챔버만 예약 등록/갱신

    def _on_presputter_cancel_clicked(self) -> None:
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

            self._host_bound = (
                str(cfgc.HOST_SERVER_HOST),
                int(cfgc.HOST_SERVER_PORT),
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
        prev = getattr(self, "_host_task", None)
        if prev and not prev.done():
            prev.cancel()
        self._host_task = self._loop.create_task(self._boot_host())

    def request_host_stop(self) -> None:
        prev = getattr(self, "_host_task", None)
        if prev and not prev.done():
            prev.cancel()
        self._host_task = self._loop.create_task(self._stop_host())

    def request_host_restart(self) -> None:
        prev = getattr(self, "_host_task", None)
        if prev and not prev.done():
            prev.cancel()
        self._host_task = self._loop.create_task(self._restart_host())

    def _append_pc_log_autoscroll(self, msg: str) -> None:
        line = str(msg)

        def _ui() -> None:
            w = getattr(self.ui, "pc_logMessage_edit", None)
            if not w or not _qt_is_valid(w):
                return

            try:
                sb = w.verticalScrollBar()
            except Exception:
                return

            # 사용자가 최하단을 보고 있을 때만 바닥 고정
            stick_to_bottom = True
            try:
                stick_to_bottom = (sb.value() >= (sb.maximum() - 2))
            except Exception:
                pass

            # ✅ UI 스레드에서만 append
            try:
                w.appendPlainText(line)
            except Exception:
                return

            if not stick_to_bottom:
                return

            if getattr(self, "_pc_log_autoscroll_pending", False):
                return
            self._pc_log_autoscroll_pending = True

            def _scroll_bottom():
                self._pc_log_autoscroll_pending = False

                ww = getattr(self.ui, "pc_logMessage_edit", None)
                if not ww or not _qt_is_valid(ww):
                    return

                with contextlib.suppress(Exception):
                    sbb = ww.verticalScrollBar()
                    sbb.setValue(sbb.maximum())
                    ww.ensureCursorVisible()

            # 레이아웃 계산 이후에 최하단 재보정
            QTimer.singleShot(0, _scroll_bottom)

        loop = getattr(self, "_loop", None)
        if loop is None:
            _ui()
            return

        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        if running is loop:
            _ui()
        else:
            # ✅ 다른 스레드/다른 루프에서 호출되어도 UI 루프에 안전하게 넘김
            try:
                loop.call_soon_threadsafe(_ui)
            except Exception:
                # 종료 중/loop 닫힘이면 무시
                pass

def main() -> int:
    _logger = setup_app_logging(
        app_name="CH_1_2_program",
        file_level=logging.INFO,      # ✅ INFO까지 파일에 저장
        enable_console=False          # 콘솔은 꺼버림 (GUI exe라서)
    )
    install_global_exception_hooks(_logger)
    install_warnings_logging(_logger)
    install_signal_logging(_logger)

    app = QApplication(sys.argv)

    install_qt_message_logging(_logger)

    try:
        app.aboutToQuit.connect(lambda: _logger.warning("QApplication.aboutToQuit emitted"))
    except Exception:
        pass

    try:
        app.aboutToQuit.connect(lambda: uninstall_qt_message_logging(_logger))
    except Exception:
        pass

    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)

    install_asyncio_exception_logging(loop, _logger)

    w = MainWindow(loop)
    w.show()

    try:
        with loop:
            loop.run_forever()
    except KeyboardInterrupt:
        pass
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

    raise SystemExit(main())


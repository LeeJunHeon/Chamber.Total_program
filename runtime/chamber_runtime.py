# controller/chamber_runtime.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import csv, asyncio, contextlib, inspect, re, traceback, os, time
import json
import uuid
from datetime import datetime
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Deque, Literal, Mapping, Optional, Sequence, TypedDict, cast, Union
from pathlib import Path
from datetime import datetime, timedelta
from collections import deque

from PySide6.QtWidgets import QMessageBox, QFileDialog, QPlainTextEdit, QDialog, QApplication
from PySide6.QtGui import QTextCursor
from PySide6.QtCore import Qt, QTimer  # ← 추가: 모달리티/속성 지정용

# ✅ Qt(C++) 객체가 이미 delete된 상태인지 체크 (Access Violation 방어)
try:
    from shiboken6 import isValid as _qt_is_valid
except Exception:
    def _qt_is_valid(obj: Any) -> bool:
        return obj is not None

# 팝업 자동 닫기(5초) 유틸
from util.timed_popup import attach_autoclose

# 장비
from device.ig import AsyncIG
from device.mfc import AsyncMFC
from device.oes import OESAsync
from device.rga import RGAWorkerClient
from device.dc_power import DCPowerAsync
from device.rf_power import RFPowerAsync
from device.rf_pulse import RFPulseAsync
from device.dc_pulse import AsyncDCPulse

# 그래프/로거/알림
from controller.graph_controller import GraphController
from controller.data_logger import DataLogger
from controller.chat_notifier import ChatNotifier

# ⬇️ 추가: 전역 런타임 상태 레지스트리
from controller.runtime_state import runtime_state

# ✅ RF-Pulse는 1대 공유(동시 사용 금지): CH1/CH2 중 1개만 점유 가능
_RFPULSE_OWNER_LOCK = threading.Lock()
_RFPULSE_OWNER_CH: int | None = None

# 공정 컨트롤러(기존 CH2) + CH1은 별도 모듈이 있으면 사용, 없으면 CH2를 공용으로
from controller.process_controller import ProcessController

# ---- 타입 (main.py의 정의를 최소 필요만 가져와 복제) -------------------------
RawParams = TypedDict('RawParams', {
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
    'dc_power': float | str,
    'rf_power': float | str,

    # 🔥 펄스 완전 분리(레거시 키 전부 제거)
    'use_dc_pulse': Literal['T','F'] | bool,
    'dc_pulse_power': float | str,
    'dc_pulse_freq': int | str | None,
    'dc_pulse_duty_cycle': int | str | None,

    'use_rf_pulse': Literal['T','F'] | bool,
    'rf_pulse_power': float | str,
    'rf_pulse_freq': int | str | None,
    'rf_pulse_duty_cycle': int | str | None,

    'gun1': Literal['T','F'] | bool,
    'gun2': Literal['T','F'] | bool,
    'gun3': Literal['T','F'] | bool,
    'main_shutter': Literal['T','F'] | bool,
    'G1 Target': str,
    'G2 Target': str,
    'G3 Target': str,
    'power_select': Literal['T','F'] | bool,
}, total=False)

NormParams = TypedDict('NormParams', {
    'base_pressure': float,
    'working_pressure': float,
    'process_time': float,
    'shutter_delay': float,
    'integration_time': int,
    'use_ar': bool, 'use_o2': bool, 'use_n2': bool,
    'ar_flow': float, 'o2_flow': float, 'n2_flow': float,
    'use_dc_power': bool, 'dc_power': float,
    'use_rf_power': bool, 'rf_power': float,

    'use_dc_pulse': bool, 'dc_pulse_power': float,
    'dc_pulse_freq': int | None, 'dc_pulse_duty': int | None,

    'use_rf_pulse': bool, 'rf_pulse_power': float,
    'rf_pulse_freq': int | None, 'rf_pulse_duty': int | None,

    'use_g1': bool, 'use_g2': bool, 'use_g3': bool, 'use_ms': bool,
    'process_note': str,
    'G1_target_name': str, 'G2_target_name': str, 'G3_target_name': str,
    'G1 Target': str, 'G2 Target': str, 'G3 Target': str,
    'use_power_select': bool,
    'chuck_position': str, # ★ CSV의 up/mid/down (또는 "")
}, total=False)

# 폴링 타깃도 명확히 분리
TargetsMap = Mapping[Literal["mfc", "dc", "rf", "dc_pulse", "rf_pulse"], bool]

# -----------------------------------------------------------------------------


@dataclass
class _CfgAdapter:
    """config_ch1 / config_ch2 모듈을 추상화해서 접근(필수 키만)."""
    mod: Any
    ch: int  # ← 채널 번호 저장(기본 파일명 등에 사용)

    def _get(self, name: str, default=None):
        """채널별 모듈 값 우선, 없으면 config_common으로 폴백."""
        v = getattr(self.mod, name, None)
        if v is not None:
            return v
        try:
            from lib import config_common as _cc
            return getattr(_cc, name, default)
        except Exception:
            return default

    @property
    def IG_POLLING_INTERVAL_MS(self) -> int:
        return int(self._get("IG_POLLING_INTERVAL_MS", 500))

    @property
    def RGA_CSV_PATH(self) -> Path:
        p = self._get("RGA_CSV_PATH", None)

        # ✅ dict 형태면 채널 키로 선택
        if isinstance(p, dict):
            p = p.get(f"ch{self.ch}") or p.get(str(self.ch)) or p.get(self.ch)

        # ✅ 혹시 RGA_CSV_PATH가 없고, 레거시로 RGA_XLSX_PATH만 있는 경우도 처리
        if not p:
            legacy = self._get("RGA_XLSX_PATH", None)
            if isinstance(legacy, dict):
                p = legacy.get(f"ch{self.ch}")
            elif isinstance(legacy, str):
                p = legacy

        if p:
            return Path(p)
        return Path.cwd() / f"RGA_CH{self.ch}.csv"

    @property
    def RGA_NET(self) -> Mapping[str, Any]:
        # 기존 코드 호환(필요시 여전히 접근 가능)
        return self._get("RGA_NET", {}) or {}

    def rga_creds(self) -> tuple[str, str, str]:
        """
        RGA 연결 정보 반환.
        - 단일 dict: {"ip","user","password"}
        - 채널별 dict: {"ch1":{...},"ch2":{...}}
        """
        rnet = self._get("RGA_NET", {}) or {}
        if isinstance(rnet, dict) and "ip" in rnet:
            # 단일 dict 형태
            return (
                rnet.get("ip", ""),
                rnet.get("user", "admin"),
                rnet.get("password", "admin"),
            )
        # 채널별 dict 형태
        block = rnet.get(f"ch{self.ch}", {}) if isinstance(rnet, dict) else {}
        return (
            block.get("ip", ""),
            block.get("user", "admin"),
            block.get("password", "admin"),
        )
    
    @property
    def IG_TCP(self) -> tuple[str, int]:
        return (
            str(self._get("IG_TCP_HOST", "192.168.1.50")),
            int(self._get("IG_TCP_PORT", 4001 if self.ch == 1 else 4002)),
        )

    @property
    def MFC_TCP(self) -> tuple[str, int]:
        return (
            str(self._get("MFC_TCP_HOST", "192.168.1.50")),
            int(self._get("MFC_TCP_PORT", 4003 if self.ch == 1 else 4006)),
        )
    
    @property
    def DCPULSE_TCP(self) -> tuple[str, int]:
        return (
            str(self._get("DCPULSE_TCP_HOST", "192.168.1.50")),
            int(self._get("DCPULSE_TCP_PORT", 4007)),
        )

    @property
    def RFPULSE_TCP(self) -> Optional[tuple[str, int]]:
        """
        (우선) RFPULSE_TCP_HOST/RFPULSE_TCP_PORT
        (폴백) RFPULSE_PORT="ip:port" 형태
        없으면 None
        """
        host = self._get("RFPULSE_TCP_HOST", None)
        port = self._get("RFPULSE_TCP_PORT", None)
        if host is not None and port is not None:
            try:
                return (str(host), int(port))
            except Exception:
                return None

        s = self._get("RFPULSE_PORT", None)
        if not s:
            return None

        try:
            # tuple/list (host, port)도 허용
            if isinstance(s, (tuple, list)) and len(s) == 2:
                return (str(s[0]), int(s[1]))

            s = str(s).strip()
            if ":" in s:
                h, p = s.split(":", 1)
                return (h.strip(), int(p.strip()))
        except Exception:
            return None

        return None
    
class ChamberRuntime:
    """
    한 챔버 실행 단위(장치/이벤트펌프/그래프/로그/버튼 바인딩).
    - PLC는 외부에서 공유 주입
    - CH1은 건셔터 없음: PLC 콜백에서 MS/G1~G3는 무시(즉시 confirmed)
    - 파워 구성:
        * CH1: DC-Pulse
        * CH2: DC(연속) + RF-Pulse (필요 시 RF 연속도 옵션)
    """

    def __init__(
        self,
        ui: Any,
        chamber_no: int,
        prefix: str,
        loop: asyncio.AbstractEventLoop,
        plc: Any,
        chat: Optional[ChatNotifier],
        cfg: Any,
        log_dir: Path,
        *,
        mfc: Optional[AsyncMFC] = None,
        ig: Optional[AsyncIG] = None,
        supports_dc_cont: Optional[bool] = None,   # DC 연속
        supports_rf_cont: Optional[bool] = None,   # RF 연속
        supports_dc_pulse: Optional[bool] = None,  # DC-Pulse
        supports_rf_pulse: Optional[bool] = None,  # RF-Pulse
        owns_plc: Optional[bool] = None,   # ← 추가: PLC 로그 소유자
        on_plc_owner: Optional[Callable[[Optional[int]], None]] = None,   # ★ 추가
    ) -> None:
        self.ui = ui
        self.ch = int(chamber_no)
        self.prefix = str(prefix)
        self._loop = loop
        self.plc = plc
        self.chat = chat
        self.cfg = _CfgAdapter(cfg, self.ch)
        self._bg_tasks: list[asyncio.Task[Any]] = []
        self._mfc_seq_lock = asyncio.Lock()
        self._starter_threads: dict[str, asyncio.Task] = {}
        self._bg_started = False
        self._pc_stopping = False
        self._pending_device_cleanup = False
        self._cleanup_timed_out = False  # ✅ cleanup 중 timeout 발생 여부(재시작 안전장치)
        self._last_polling_targets: TargetsMap | None = None
        self._last_state_text: str | None = None
        # 지연(다음 공정 예약)과 카운트다운을 분리
        self._delay_main_task: Optional[asyncio.Task] = None
        self._delay_countdown_task: Optional[asyncio.Task] = None
        self._dc_failed_flag: bool = False     # ★ 추가
        self._auto_connect_enabled = True  # ← 실패시 False로 내려 자동 재연결 차단
        self._run_select: dict[str, bool] | None = None  # ← 이번 런에서 펄스 선택 상태
        self._owns_plc = bool(owns_plc if owns_plc is not None else (int(chamber_no) == 1))  # 기본 CH1
        self._notify_plc_owner = on_plc_owner 
        self._last_running_state: Optional[bool] = None  
        self._rf_pulse_reserved: bool = False
        
        # ✅ 런(시작) 세대 번호: 프리플라이트/정리 레이스 방지 + watchdog 식별
        self._run_gen: int = 0
        self._active_run_gen: int = 0
    
        # ✅ Host 응답용 Future (프리플라이트가 끝나면 결과를 세팅)
        self._host_start_future: Optional[asyncio.Future] = None

        # QMessageBox 참조 저장소(비모달 유지용)
        self._msg_boxes: list[QMessageBox] = []  # ← 추가

        # ✅ 기본 전략: config의 SUPPORTS_*를 최우선으로, 없으면 기존 CH 기본값으로 폴백
        def _cfg_bool(*names: str):
            for n in names:
                v = self.cfg._get(n, None)
                if v is not None:
                    return bool(v)
            return None

        if supports_dc_cont is None:
            v = _cfg_bool("SUPPORTS_DC_CONT", "SUPPORTS_DC")  # 레거시 키도 허용
            supports_dc_cont = v if v is not None else (self.ch == 2)

        if supports_rf_cont is None:
            v = _cfg_bool("SUPPORTS_RF_CONT")
            
            if self.ch == 2:
                supports_rf_cont = True   # 🔥 CH2 RF 연속 파워 강제 허용
            else:
                supports_rf_cont = v if v is not None else False

        if supports_dc_pulse is None:
            v = _cfg_bool("SUPPORTS_DC_PULSE", "SUPPORTS_DCPULSE")
            supports_dc_pulse = v if v is not None else (self.ch == 1)

        if supports_rf_pulse is None:
            v = _cfg_bool("SUPPORTS_RFPULSE", "SUPPORTS_RF_PULSE")
            supports_rf_pulse = v if v is not None else (self.ch == 2)

        self.supports_dc_cont  = bool(supports_dc_cont)
        self.supports_rf_cont  = bool(supports_rf_cont)
        self.supports_dc_pulse = bool(supports_dc_pulse)
        self.supports_rf_pulse = bool(supports_rf_pulse)

        # UI 포인터
        self._w_log: QPlainTextEdit | None = self._u("logMessage_edit")
        self._w_state: QPlainTextEdit | None = self._u("processState_edit")

        # 그래프 컨트롤러
        self.graph = GraphController(self._u("rgaGraph_widget"), self._u("oesGraph_widget"))
        try:
            self.graph.reset()
        except Exception:
            self.append_log("Graph", "reset skipped (headless)")

        # 로그 파일 경로 관리(세션 단위) + 사전 버퍼
        self._log_root = Path(log_dir)
        # ✅ CH 로그를 루트 바로 아래 CH1/CH2에 저장
        self._log_dir = self._ensure_log_dir(self._log_root / f"CH{self.ch}")
        self._log_file_path: Path | None = None
        self._prestart_buf: Deque[str] = deque(maxlen=1000)
        self._log_fp = None
        self._log_q: asyncio.Queue[str] = asyncio.Queue(maxsize=4096)
        self._log_writer_task: asyncio.Task | None = None

        # ✅ 로그 파일 I/O는 이벤트루프 밖(전용 1-thread)에서만 수행
        self._log_io_exec = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix=f"LogIO.CH{self.ch}"
        )

        # ✅ UI 로그 무한 누적 방지(프리징 완화)
        self._ui_log_buf = deque(maxlen=5000)   # UI에 쌓을 임시 버퍼(메모리 보호)
        self._ui_log_timer = None

        if self._w_log:
            # 1) UI 문서 줄 수 제한(이미 있다면 유지/조정)
            self._w_log.setMaximumBlockCount(2000)   # 또는 5000

            # 2) Undo/Redo 끄면 QPlainTextEdit 비용이 줄어듦
            with contextlib.suppress(Exception):
                self._w_log.setUndoRedoEnabled(False)

            # 3) 배치로 찍기 위한 타이머
            self._ui_log_timer = QTimer(self._w_log)
            self._ui_log_timer.setInterval(100)      # 100ms마다 한번만 UI 업데이트
            self._ui_log_timer.timeout.connect(self._flush_ui_log_to_ui)
            self._ui_log_timer.start()

        # 데이터 로거 (Sputter Calib CSV) - CH 로그로 로그를 흘려보내도록 콜백 전달
        self.data_logger = DataLogger(
            ch=self.ch,
            csv_dir=Path(r"\\VanaM_NAS\VanaM_Sputter\Sputter\Calib\Database"),
            log_func=lambda msg: self.append_log("CSV", msg),
        )

        # 장치 인스턴스(각 챔버 독립)
        mfc_host, mfc_port = self.cfg.MFC_TCP
        ig_host,  ig_port  = self.cfg.IG_TCP

        # 설정에서 채널별 스케일 정보를 불러와 주입
        try:
            scale_map = getattr(self.cfg, "MFC_SCALE_FACTORS", {1: 1.0, 2: 1.0, 3: 1.0})
        except Exception:
            scale_map = {1: 1.0, 2: 1.0, 3: 1.0}

        # MFC/IG를 외부에서 주입하면 그대로 사용하고, 없으면 기존 방식대로 생성
        self.mfc = mfc or AsyncMFC(
            host=mfc_host, port=mfc_port, enable_verify=False, enable_stabilization=True,
            # ★ 챔버별 스케일을 드라이버에 주입
            scale_factors=scale_map,  # ✅ CH별 MFC 스케일 전달
        )
        self.ig  = ig or AsyncIG(host=ig_host, port=ig_port)

        # OES 인스턴스 생성 시 현재 챔버 번호에 따라 USB 채널을 명시적으로 매핑한다.
        # CH1 → USB0, CH2 → USB1. OESAsync 내부 기본 동작도 동일하지만 명확성을 위해 전달한다.
        _usb_index = 0 if self.ch == 1 else 1
        self.oes = OESAsync(chamber=self.ch, usb_index=_usb_index)
        self._oes_initialized = False  # ✅ 워커 init(장치 스캔) 1회만 수행하도록 캐시

        # RGA: worker client (메인에서 srsinst import 안함)
        self.rga = None
        try:
            # logger는 선택사항인데 ChamberRuntime에는 self.logger가 없으니 전달하지 않는다.
            timeout_s = float(getattr(self.cfg, "RGA_WORKER_TIMEOUT_S", 60.0))
            self.rga = RGAWorkerClient(ch=self.ch, logger=None, default_timeout_s=timeout_s)
        except Exception as e:
            # 기존 로그 시스템(append_log)로만 남긴다.
            self.append_log(f"RGA{self.ch}", f"RGAWorkerClient init failed: {e!r} (RGA disabled)")
            self.rga = None

        # 펄스 파워(완전 분리)
        # - on_telemetry를 DataLogger로 직결(있으면 log_dcpulse_power, 없으면 log_dc_power 폴백)
        # - 생성 시점에 host/port도 지정
        if self.supports_dc_pulse:
            _cb = getattr(self.data_logger, "log_dcpulse_power", None)
            if not callable(_cb):
                def _cb(p, v, i):
                    try:
                        self.data_logger.log_dc_power(float(p), float(v), float(i))
                    except Exception:
                        pass
            host, port = self.cfg.DCPULSE_TCP
            self.dc_pulse = AsyncDCPulse(host=host, port=port, on_telemetry=_cb)
        else:
            self.dc_pulse = None

        self.rf_pulse = RFPulseAsync() if self.supports_rf_pulse else None

        # 연속 파워
        self.dc_power = None
        if self.supports_dc_cont:
            async def _dc_send(power: float):
                # 연속 제어 루프에서는 SET을 건드리지 않는다 → WRITE만 수행
                await self.plc.power_write(power, family="DCV", write_idx=0)

            async def _dc_send_unverified(power: float):
                # no-reply: WRITE만
                await self.plc.power_write(power, family="DCV", write_idx=0)

            async def _dc_read():
                try:
                    P, V, I = await self.plc.power_read(family="DCV", v_idx=0, i_idx=1)
                    return (P, V, I)
                except Exception as e:
                    self.append_log("DCpower", f"read failed: {e!r}")

            # ⬇️ 추가: SET 코일 ON/OFF 콜백
            async def _dc_toggle_enable(on: bool):
                await self.plc.power_enable(on, family="DCV", set_idx=0)

            self.dc_power = DCPowerAsync(
                send_dc_power=_dc_send,
                send_dc_power_unverified=_dc_send_unverified,
                request_status_read=_dc_read,
                toggle_enable=_dc_toggle_enable,   # ← 추가
            )

        self.rf_power = None
        if self.supports_rf_cont and self.plc:
            # (CH2 전용) RF 연속 제어 — RF channel 2 사용
            # - SET: DCV_SET_2, WRITE: DCV ch2, READ: DCV_READ_4/5 (FWD/REF)
            async def _rf_send(power: float):
                # SET 래치는 RFPowerAsync의 toggle_enable(True)에서 한 번만 걸어도 됨
                # 여기서는 중복 SET 방지하려면 ensure_set=False로 호출
                await self.plc.rf_apply(float(power), ensure_set=False, rf_ch=2)

            async def _rf_send_unverified(power: float):
                await self.plc.rf_write_w(float(power), rf_ch=2)

            async def _rf_request_read():
                try:
                    # ★ CH2는 제로잉 미적용
                    return await self.plc.rf_read_fwd_ref(rf_ch=2, zeroing=False)
                except Exception as e:
                    self.append_log("RF", f"read failed: {e!r}")
                    return None

            async def _rf_toggle_enable(on: bool):
                await self.plc.rf_enable(bool(on), rf_ch=2)

            self.rf_power = RFPowerAsync(
                send_rf_power=_rf_send,
                send_rf_power_unverified=_rf_send_unverified,
                request_status_read=_rf_request_read,
                toggle_enable=_rf_toggle_enable,
                poll_interval_ms=1000,
                rampdown_interval_ms=50,
                direct_mode=False,
                # 필요 시 CH2 전용 역변환 계수로 조정. 없으면 Plasma Cleaning과 동일값 사용 가능.
                write_inv_a=1.6546,   # ← 엑셀 기반 역보정
                write_inv_b=2.6323,   # ← (입력W = 1.6546*목표W + 2.6323)
            )

        # === ProcessController 바인딩 ===
        self._bind_process_controller()

        # === UI 버튼 바인딩 (자기 챔버 것만) ===
        self._connect_my_buttons()

        # === 백그라운드 워치독/이벤트펌프 준비는 최초 Start 때 올림 ===
        self._on_process_status_changed(False)

    # ------------------------------------------------------------------
    # 공정 컨트롤러 바인딩

    # 클래스 내부 어딘가(예: _bind_process_controller 위/아래)
    async def mfc_dispatch(self, cmd: str, args: Mapping[str, Any] | None = None, *, atomic: bool = False):
        """같은 AsyncMFC 내부 큐로 안전하게 보냄. atomic=True면 짧은 시퀀스 원자 실행."""
        if atomic:
            async with self._mfc_seq_lock:
                await self.mfc.handle_command(cmd, args or {})
        else:
            await self.mfc.handle_command(cmd, args or {})

    def _bind_process_controller(self) -> None:
        # === 콜백 정의(PLC/MFC/파워/OES/RGA/IG) ===

        def cb_plc(cmd: str, on: Any, ch: int | None = None) -> None:
            async def run():
                raw = str(cmd)
                nname = raw.upper()
                onb = bool(on)

                # ⬇️ 추가: 요청 로그 + 총 소요시간 계측 시작
                t0 = 0.0
                try:
                    t0 = asyncio.get_running_loop().time()
                except RuntimeError:
                    pass
                self.append_log("PLC", f"[CH{self.ch}] 요청: {nname} -> {onb} (raw='{raw}', ch={self.ch})")

                try:
                    # CH1: 셔터 + N2 가스 무시
                    if self.ch == 1 and nname in ("G1", "G2", "G3", "N2"):
                        reason = "건 셔터 없음" if nname in ("G1", "G2", "G3") else "N2 라인 없음"
                        self.append_log("PLC", f"[CH1] '{nname}' 명령은 무시({reason}).")
                        self.process_controller.on_plc_confirmed(nname)
                        return

                    if nname == "MV":
                        await self.plc.write_switch(f"MAIN_{int(self.ch)}_GAS_SW", onb)
                    elif nname in ("AR", "O2", "N2", "MAIN"):
                        await self.plc.gas(int(self.ch), nname, on=onb)
                    elif nname == "MS":
                        await self.plc.main_shutter(int(self.ch), open=onb)
                    elif nname in ("G1", "G2", "G3"):
                        idx = int(nname[1])
                        await self.plc.write_switch(f"SHUTTER_{idx}_SW", onb)
                    else:
                        await self.plc.write_switch(raw, onb)

                    self.process_controller.on_plc_confirmed(nname)

                    # ⬇️ 추가: 완료 로그(+ 소요시간)
                    dt_ms = 0.0
                    try:
                        if t0:
                            dt_ms = (asyncio.get_running_loop().time() - t0) * 1000.0
                    except RuntimeError:
                        pass
                    self.append_log("PLC", f"[CH{self.ch}] 완료: {nname} -> {onb} ({dt_ms:.0f} ms)")

                except Exception as e:
                    # ⬇️ 추가: 실패 로그(+ 소요시간)
                    dt_ms = 0.0
                    try:
                        if t0:
                            dt_ms = (asyncio.get_running_loop().time() - t0) * 1000.0
                    except RuntimeError:
                        pass

                    self.process_controller.on_plc_failed(nname, str(e))

                    self.append_log("PLC", f"명령 실패: {raw} -> {onb}: {e!r}")
            self._spawn_detached(run())

        def cb_mfc(cmd: str, args: Mapping[str, Any]) -> None:
            # 🔒 CH1에선 N2 가스를 완전히 무시
            gas = str(args.get("gas", "")).upper() if isinstance(args, Mapping) else ""
            if self.ch == 1 and gas == "N2":
                self.append_log("MFC", "[CH1] N2 요청 무시 (라인 없음)")
                # 프로세스 진행이 끊기지 않도록 '확인' 신호만 넘겨줌
                self.process_controller.on_mfc_confirmed(cmd)
                return
            
            self._spawn_detached(self.mfc.handle_command(cmd, args))

        def cb_dc_power(value: float):
            if not self.dc_power:
                self.append_log("DCpower", "이 챔버는 DC 연속 파워를 지원하지 않습니다.")
                return
            self._spawn_detached(self.dc_power.start_process(float(value)))

        def cb_dc_stop():
            if self.dc_power:
                self._spawn_detached(self.dc_power.cleanup())

        def cb_rf_power(value: float):
            if not self.rf_power:
                self.append_log("RFpower", "이 챔버는 RF 연속 파워를 지원하지 않습니다.")
                return
            self._spawn_detached(self.rf_power.start_process(float(value)))

        def cb_rf_stop():
            if self.rf_power:
                self._spawn_detached(self.rf_power.cleanup())

        def cb_dc_pulse_start(
            power: float,
            freq: Union[int, float, str, None],
            duty: Union[int, float, str, None],
        ) -> None:
            async def run():
                if not self.dc_pulse:
                    self.append_log("DCPulse", "DC-Pulse 미지원 챔버입니다."); return
                try:
                    self._ensure_background_started()
                    # (선행 단계에서 이미 연결/워치독이 올라와 있으므로 start()는 생략해도 무방)
                    ok = await self.dc_pulse.prepare_and_start(power_w=float(power), freq=freq, duty=duty)
                    if not ok:
                        self.process_controller.on_dc_pulse_failed("prepare_and_start failed")
                        return
                except Exception as e:
                    why = f"DC-Pulse start failed: {e!r}"
                    self.append_log("DCPulse", why)
                    self.process_controller.on_dc_pulse_failed(why)
            self._spawn_detached(run())

        # ✅ 추가: Output ON 상태에서 Power setpoint만 변경
        def cb_dc_pulse_set_power(power: float) -> None:
            async def run():
                if not self.dc_pulse:
                    self.append_log("DCPulse", "DC-Pulse 미지원 챔버입니다."); return
                try:
                    # 공정 중이면 이미 켜져있겠지만, 방어적으로 보장
                    self._ensure_background_started()

                    ok = await self.dc_pulse.set_reference_power(float(power))
                    if not ok:
                        self.process_controller.on_dc_pulse_failed("set_reference_power failed")
                except Exception as e:
                    why = f"DC-Pulse set_reference_power failed: {e!r}"
                    self.append_log("DCPulse", why)
                    self.process_controller.on_dc_pulse_failed(why)
            self._spawn_detached(run())

        def cb_dc_pulse_stop():
            async def run():
                if self.dc_pulse:
                    try:
                        await self.dc_pulse.output_off()
                    except Exception:
                        self.process_controller.on_dc_pulse_failed("output_off failed")
            self._spawn_detached(run())

        def cb_rf_pulse_start(power: float, freq: int | None, duty: int | None) -> None:
            async def run():
                if not self.rf_pulse:
                    self.append_log("RFPulse", "RF-Pulse 미지원 챔버입니다."); return
                self._ensure_background_started()
                await self.rf_pulse.start_pulse_process(float(power), freq, duty)
            self._spawn_detached(run())

        def cb_rf_pulse_stop():
            async def run():
                if not self.rf_pulse:
                    return
                try:
                    # stop_process가 동기/blocking이어도 UI 안 멈추게
                    res = await asyncio.to_thread(self.rf_pulse.stop_process)
                    if inspect.isawaitable(res):
                        await res
                except Exception as e:
                    self.append_log("RFPulse", f"stop_process failed: {e!r}")
            self._spawn_detached(run())

        def cb_ig_wait(base_pressure: float) -> None:
            async def _run():
                self._ensure_background_started()
                ok = await self.ig.wait_for_base_pressure(
                    float(base_pressure),
                    interval_ms=self.cfg.IG_POLLING_INTERVAL_MS
                )
                self.append_log("IG", f"wait_for_base_pressure returned: {ok}")
            self._spawn_detached(_run())

        def cb_ig_cancel():
            self._spawn_detached(self.ig.cancel_wait())

        def cb_oes_run(duration_sec: float, integration_ms: int):
            async def run():
                # ✅ OES는 외부 워커(oes_worker.exe)로 측정한다.
                #    - 메인 프로세스에서 DLL 로드/장비 연결을 하지 않음
                #    - 워커가 로컬 CSV를 append+flush로 기록
                #    - 메인(OESAsync)이 CSV를 tail 해서 실시간 스펙트럼 이벤트를 발행
                #    - 종료 시 워커가 NAS 복사 후 finished(JSON)로 결과 통지
                try:
                    # ✅ 공정이 멈추지 않도록 OES 이벤트 펌프는 항상 살아 있어야 함
                    # (auto_connect 차단 상태에서도 OES step은 돌 수 있으므로 별도로 보장)
                    self._ensure_task_alive(f"Pump.OES.{self.ch}", self._pump_oes_events)

                    # (기존 동작 유지) 나머지 백그라운드 장치/펌프도 필요 시 기동
                    self._ensure_background_started()

                    # ✅ 이전 런 잔여 이벤트(특히 finished)를 제거
                    if hasattr(self.oes, "drain_events"):
                        with contextlib.suppress(Exception):
                            await self.oes.drain_events()

                    # ✅ 워커 init(장치 스캔)은 1회만 수행 (실패 시 캐시 무효화)
                    if not getattr(self, "_oes_initialized", False):
                        ok = await self.oes.initialize_device()
                        if not ok:
                            raise RuntimeError("OES 초기화 실패")
                        self._oes_initialized = True

                    self._soon(self._safe_clear_oes_plot)

                    # 이번 런에서만 finished 이벤트를 처리하도록 플래그 ON
                    self._oes_active = True

                    # 측정 시작(완료/실패는 _pump_oes_events의 'finished'에서 단일 경로로 처리)
                    await self.oes.run_measurement(duration_sec, integration_ms)

                    # ⚠️ 여기서 _oes_active를 끄지 않는다.
                    # finished 이벤트가 큐에 들어온 뒤 pump가 처리할 때 끄는 게 레이스가 없다.

                except Exception as e:
                    # 예외/초기화 실패는 즉시 실패 처리
                    self._oes_active = False
                    self._oes_initialized = False  # 다음 런에서 재초기화 시도
                    self.append_log("OES", f"OES 실패: {e!r}")
                    if self.chat:
                        with contextlib.suppress(Exception):
                            self.chat.notify_text(f"[OES] 실패: {e!r}")
                            if hasattr(self.chat, "flush"):
                                self.chat.flush()
                    self.process_controller.on_oes_failed("OES", str(e))

            self._spawn_detached(run())

        def cb_rga_scan():
            async def _run():
                timeout_s = float(getattr(self.cfg, "RGA_WORKER_TIMEOUT_S", 60.0))
                self._soon(self._graph_clear_rga_plot_safe)

                try:
                    # ✅ auto_connect 차단 상태여도 Pump.RGA는 올려야 finished/data를 소비함
                    if self.rga:
                        self._ensure_task_alive(f"Pump.RGA.{self.ch}", self._pump_rga_events)

                    if self.rga:
                        await self.rga.scan_histogram_to_csv(timeout_s=timeout_s)
                    else:
                        raise RuntimeError("RGA 어댑터 없음")

                except Exception as e:
                    self.append_log("RGA", f"예외로 RGA 스캔 실패: {e!r} → 다음 단계")

                finally:
                    # ✅ 핵심: 펌프/이벤트 누락이 있어도 공정이 여기서 영원히 멈추지 않게 한다
                    try:
                        self.process_controller.on_rga_finished()
                    except Exception:
                        pass

            self._spawn_detached(_run())

        # 컨트롤러 생성
        self.process_controller = ProcessController(
            send_plc=cb_plc,
            send_mfc=cb_mfc,

            # 연속 파워
            send_dc_power=cb_dc_power, 
            stop_dc_power=cb_dc_stop,
            send_rf_power=cb_rf_power, 
            stop_rf_power=cb_rf_stop,

            # 펄스 파워(완전 분리)
            start_dc_pulse=cb_dc_pulse_start, 
            stop_dc_pulse=cb_dc_pulse_stop,
            set_dc_pulse_power=cb_dc_pulse_set_power,   # ✅ 추가
            start_rf_pulse=cb_rf_pulse_start, 
            stop_rf_pulse=cb_rf_pulse_stop,

            ig_wait=cb_ig_wait, 
            cancel_ig=cb_ig_cancel,
            rga_scan=cb_rga_scan, 
            oes_run=cb_oes_run,

            ch=self.ch,
            supports_dc_cont=self.supports_dc_cont,
            supports_rf_cont=self.supports_rf_cont,
            supports_dc_pulse=self.supports_dc_pulse,
            supports_rf_pulse=self.supports_rf_pulse,
        )

        # 이벤트 펌프 루프(컨트롤러 → UI/로거/다음공정)
        self._ensure_task_alive("Pump.PC", self._pump_pc_events)

    # ------------------------------------------------------------------
    # 이벤트 펌프들
    async def _pump_pc_events(self) -> None:
        q = self.process_controller.event_q
        while True:
            ev = await q.get()
            kind = ev.kind
            payload = ev.payload or {}
            try:
                if kind == "log":
                    self.append_log(payload.get("src", f"PC{self.ch}"), payload.get("msg", ""))

                elif kind == "state":
                    self._apply_process_state_message(payload.get("text", ""))

                elif kind == "status":
                    self._on_process_status_changed(bool(payload.get("running", False)))

                elif kind == "started":
                    params = payload.get("params", {}) or {}

                    # ✅ 공정명 키 통일: process_name을 정식 키로 사용
                    #    (레거시 키 Process_name / process_note는 동일값으로 유지)
                    pname = str(params.get("process_name") or params.get("Process_name") or params.get("process_note") or "").strip()
                    if not pname:
                        pname = f"Run CH{self.ch}"
                    params["process_name"] = pname
                    params["Process_name"] = pname
                    params["process_note"] = pname

                    # ✅ 시작 카드 전송(성공 시 로그 X, 실패만 로그)
                    # AFTER: 시작 카드 전송 후 즉시 flush
                    if self.chat:
                        p = dict(params)
                        p.setdefault("ch", self.ch)
                        p["prefix"] = f"CH{self.ch} Sputter"

                        # ➋ 리스트 공정이면 공정명에 " (i/n)"을 덧붙이고 인덱스도 함께 넘김
                        try:
                            total = len(getattr(self, "process_queue", []) or [])
                            cur   = int(getattr(self, "current_process_index", -1)) + 1
                            if total > 0 and cur > 0:
                                name_key = "Process_name" if "Process_name" in p else ("process_name" if "process_name" in p else None)
                                if name_key:
                                    base = (str(p.get(name_key, "")) or f"Run CH{self.ch}").strip()
                                    p[name_key] = f"{base} ({cur}/{total})"
                                p["process_index"] = cur
                                p["process_total"] = total
                        except Exception:
                            pass

                        p = self._format_card_payload_for_chat(p)
                        try:
                            ret = self.chat.notify_process_started(p)
                            if inspect.iscoroutine(ret):
                                await ret
                            # ★ 추가: 버퍼링 드롭 방지(즉시 밀어내기)
                            if hasattr(self.chat, "flush"):
                                self.chat.flush()
                        except Exception as e:
                            self.append_log("CHAT", f"구글챗 시작 카드 전송 실패: {e!r}")

                    # ✅ 시작시각 확정: 버튼-누른-시각 우선, 없으면 지금 시각 (둘 다 tz 없음)
                    from datetime import datetime
                    params = dict(params)
                    t0 = params.get("t0_pressed_wall") or datetime.now().isoformat(timespec="seconds")
                    params["t0_wall"]   = t0
                    params["started_at"] = t0  # 하위호환 키 동일값

                    # 런 시작 시각/세션 정보 저장
                    self._run_started_wall = datetime.now()
                    self._oes_active = False  # OES는 별도 cb에서 True로 바꿈

                    # Plasma Cleaning 스타일 헤더 포함한 오픈 (중복 방지)
                    if not getattr(self, "_log_file_path", None):
                        self._open_run_log(params)
                    else:
                        self.append_log("Logger", f"이미 열린 로그 파일 사용: {self._log_file_path.name}")

                    try:
                        self.data_logger.start_new_log_session(params)
                        # 성공 시에도 명시적으로 남겨 두면 나중에 추적이 쉬움
                        self.append_log("CSV", "Sputter Calib 로그 세션 시작")
                    except Exception as e:
                        # 시작 자체가 실패한 경우도 CH 로그에 남김
                        self.append_log("CSV", f"Sputter Calib 로그 세션 시작 실패: {e!r}")

                    self._soon(self._graph_reset_safe)

                    # ✅ 텍스트 알림은 기존 그대로 유지
                    name = (params.get("process_name")
                            or params.get("Process_name")
                            or params.get("process_note")
                            or f"Run CH{self.ch}")
                    t = params.get("process_time", 0) or 0
                    line = f"▶️ CH{self.ch} '{name}' 시작 (t={float(t):.1f}s)"
                    self.append_log("MAIN", line)

                    # 폴링 타깃 초기화
                    self._last_polling_targets = None

                elif kind == "finished":
                    ok = False
                    detail = {}
                    try:
                        ok = bool(payload.get("ok", False))
                        detail = payload.get("detail", {}) or {}

                        # ✅ test/stop 판별 (기존 로직 영향 없음)
                        is_test = bool(detail.get("test_mode", False))
                        is_stopped = bool(detail.get("stopped", False))
                        is_test_cancel = is_test and is_stopped

                        # ============================
                        # ✅ CSV용 Result/실제 시간 반영
                        # ============================

                        # 1) Result 매핑: (process_controller는 SUCCESS/FAIL/STOP을 보냄)
                        raw_result = str(detail.get("result") or "").strip().upper()
                        if raw_result in ("SUCCESS", "OK", "TRUE", "1"):
                            result = "성공"
                        elif raw_result in ("STOP", "STOPPED", "CANCEL", "CANCELED", "CANCELLED"):
                            result = "stop"
                        elif raw_result in ("FAIL", "FAILED", "ERROR", "FALSE", "0"):
                            result = "실패"
                        else:
                            # detail에 result가 없을 때만 최소 폴백(추정 X: ok/stopped로만 결정)
                            result = "stop" if is_stopped else ("성공" if ok else "실패")

                        # 2) DataLogger의 process_params에 덮어쓰기(여기가 핵심)
                        try:
                            pp = getattr(self.data_logger, "process_params", None)
                            if isinstance(pp, dict):
                                # 공정명도 확실히 통일
                                pname = str(detail.get("process_name") or detail.get("Process_name") or detail.get("process_note") or "").strip()
                                if pname:
                                    pp["process_name"] = pname
                                    pp["Process_name"] = pname
                                    pp["process_note"] = pname

                                # Result 컬럼용
                                pp["result"] = result
                                pp["stopped"] = bool(is_stopped)

                                # 실제 진행 시간(분) → 기존 컬럼(shutter_delay / process_time)에 그대로 넣기
                                v = detail.get("actual_shutter_delay_min", None)
                                if v is not None:
                                    pp["shutter_delay"] = float(v)

                                v = detail.get("actual_process_time_min", None)
                                if v is not None:
                                    pp["process_time"] = float(v)

                        except Exception as e:
                            self.append_log("CSV", f"Sputter Calib CSV 메타 갱신 실패(무시): {e!r}")

                        # 3) 이제 기록(성공/실패/stop 모두 기록)
                        self.append_log("CSV", f"Sputter Calib CSV 기록 요청 (ok={ok}, result={result})")
                        
                        # - data_logger가 'ok=False면 return'인 현재 버전이면, 아래 인자를 ok로 두면 실패/stop이 기록되지 않습니다.
                        # - data_logger를 실패/stop도 기록하도록 수정한 뒤에는 ok를 그대로 넘겨도 됩니다.
                        self.data_logger.finalize_and_write_log(ok)

                        # ➊ 카드 헤더용 prefix: "CHx Sputter"
                        detail.setdefault("ch", self.ch)
                        detail.setdefault("prefix", f"CH{self.ch} Sputter")

                        # ➋ 리스트 공정 표기 (i/n) 동일하게 적용
                        try:
                            total = len(getattr(self, "process_queue", []) or [])
                            cur   = int(getattr(self, "current_process_index", -1)) + 1
                            if total > 0 and cur > 0:
                                name_key = "Process_name" if "Process_name" in detail else ("process_name" if "process_name" in detail else None)
                                if name_key:
                                    base = (str(detail.get(name_key, "")) or f"Run CH{self.ch}").strip()
                                    detail[name_key] = f"{base} ({cur}/{total})"
                                detail["process_index"] = cur
                                detail["process_total"] = total
                        except Exception:
                            pass

                        # ✅ 종료 카드 전송(성공 시 로그 X, 실패만 로그)
                        if self.chat:
                            # 라우팅/표시용 힌트 보강: CH2 누락으로 전송이 드롭/오경로 되는 문제 방지
                            payload = dict(detail)
                            payload.setdefault("ch", self.ch)           # ← 필수(라우팅)
                            payload.setdefault("prefix", self.prefix)   # ← 표시/구분용
                            # 시작 카드와 키를 맞춰 카드 템플릿이 동일하게 먹히도록 보정
                            if "process_note" not in payload and "process_name" in payload:
                                payload["process_note"] = payload["process_name"]

                            try:
                                # ✅ chuck 경고/목표 위치를 종료 카드로 전달
                                pos = str(getattr(self, "_run_chuck_position", "") or "").strip()
                                if pos:
                                    payload.setdefault("chuck_position", pos)

                                warns = list(getattr(self, "_run_warnings", []) or [])
                                if warns:
                                    payload.setdefault("warnings", warns)

                                ret = self.chat.notify_process_finished_detail(ok, payload)
                                if inspect.iscoroutine(ret):
                                    await ret
                                # Plasma cleaning과 동일하게 즉시 밀어내기(버퍼링 드롭 방지)
                                if hasattr(self.chat, "flush"):
                                    self.chat.flush()
                            except Exception as e:
                                self.append_log("CHAT", f"구글챗 종료 카드 전송 실패: {e!r}")

                            # 👇 추가: 카드가 잘려 보일 때를 대비해 '실패 이유'만 텍스트로 별도 전송
                            # ✅ 실패 이유 텍스트는 "진짜 실패"에만
                            if (not ok) and (not detail.get("stopped", False)):
                                reason = (str(detail.get("reason") or "")).strip()
                                if not reason:
                                    errs = detail.get("errors", [])
                                    if isinstance(errs, (list, tuple)) and errs:
                                        reason = str(errs[0])
                                    elif isinstance(errs, str):
                                        reason = errs
                                if reason:
                                    try:
                                        r = self.chat.notify_text(f"❌ CH{self.ch} 공정 실패 이유: {reason}")
                                        if inspect.iscoroutine(r):
                                            await r
                                        # ★ 추가: 실패 텍스트도 카드 직후에 바로 나가도록 즉시 flush
                                        if hasattr(self.chat, "flush"):
                                            self.chat.flush()
                                    except Exception as _e:
                                        self.append_log("CHAT", f"실패 이유 텍스트 알림 실패: {_e!r}")

                        try:
                            self.mfc.on_process_finished(ok)
                        except Exception:
                            pass

                        # 0) 재연결 선차단 + 폴링 완전 OFF
                        self._auto_connect_enabled = False
                        self._run_select = None
                        self._last_polling_targets = None
                        # 남아 있을 수 있는 폴링 스위치를 즉시 모두 내림(장치 내부 워치독 종료 유도)
                        self._apply_polling_targets({"mfc": False, "dc_pulse": False, "rf_pulse": False, "dc": False, "rf": False})

                        # 1) 이제 실제로 장치/워치독을 내려서 RS-232/TCP 점유 해제
                        if not is_test_cancel:
                            self.append_log("MAIN", "공정 종료 → 모든 장치 연결 해제 및 워치독 중지")
                            try:
                                await self._stop_device_watchdogs(light=False)
                            except Exception as e:
                                self.append_log("MAIN", f"종료 정리 중 예외(무시): {e!r}")
                        else:
                            self.append_log("MAIN", "[TEST] STOP 종료 → 장치 정리 생략")

                        # ★ 추가: 혹시 남아 있을 수 있는 카운트다운/지연 태스크 누수 방지
                        self._cancel_delay_task()

                        # 2) 다음 공정 새 로그 파일을 위해 세션 리셋
                        # (중요) 여기서는 파일을 건드리지 않음.
                        # - 다음 공정이 있으면, 다음 공정 진입 직전에 닫고(None) 돌리고
                        # - 마지막 공정이면, '모든 공정 완료'까지 기록한 뒤 닫는다.

                        if getattr(self, "_pc_stopping", False):
                            with contextlib.suppress(Exception):
                                self._clear_queue_and_reset_ui()
                            self._last_polling_targets = None
                            self._pc_stopping = False
                            continue

                        self._pc_stopping = False
                        self._start_next_process_from_queue(ok)
                        self._last_polling_targets = None
                    except Exception as e:
                        self.append_log("MAIN", f"예외 발생 (finished 처리): {e}")
                        # 예외 시 안전하게 UI를 '대기 중'으로 복귀
                        with contextlib.suppress(Exception):
                            self._clear_queue_and_reset_ui()

                    finally:
                        try:
                            stopped = bool(detail.get("stopped"))

                            if ok or stopped:
                                # ✅ 정상 종료(ok=True) 또는 사용자 STOP(stopped=True)은 idle로 표시
                                runtime_state.clear_error("chamber", self.ch)
                            else:
                                # ✅ 실패만 error
                                _reason = (str(detail.get("reason") or "")).strip()
                                if not _reason:
                                    _errs = detail.get("errors", None)
                                    if isinstance(_errs, (list, tuple)) and _errs:
                                        _reason = str(_errs[0])
                                    elif isinstance(_errs, str):
                                        _reason = _errs
                                if not _reason:
                                    _reason = "process failed"

                                runtime_state.set_error("chamber", self.ch, _reason)

                                # ✅ 실패 알림창: 사용자가 OK를 누르면 status 표시가 idle로 돌아가도록
                                try:
                                    _pname = (str(detail.get("process_name") or "").strip() or
                                            str(detail.get("Process_name") or "").strip() or
                                            "(process)")
                                    self._post_critical(
                                        f"CH{self.ch} 공정 실패",
                                        f"{_pname}\n\n사유: {_reason}\n\n확인을 누르면 상태 표시가 Idle로 변경됩니다.",
                                        clear_status_to_idle=True,
                                    )
                                except Exception:
                                    pass

                            runtime_state.mark_finished("chamber", self.ch)

                        except Exception:
                            pass

                elif kind == "aborted":
                    try:
                        if self.chat:
                            try:
                                ret = self.chat.notify_text(f"🛑 CH{self.ch} 공정 중단")
                                if inspect.iscoroutine(ret):
                                    await ret
                            except Exception as e:
                                self.append_log("CHAT", f"구글챗 중단 알림 전송 실패: {e!r}")
                        with contextlib.suppress(Exception):
                            self._clear_queue_and_reset_ui()

                        # ✅ 전역: CH 공정 '종료' 시각 마킹 (중단도 종료로 취급)
                        try:
                            # finished에서 이미 error reason을 남겼을 수 있으니 덮어쓰지 않게 방어
                            if not runtime_state.has_error("chamber", self.ch):
                                runtime_state.set_error("chamber", self.ch, "aborted")
                            runtime_state.mark_finished("chamber", self.ch)
                        except Exception:
                            pass

                        # ★ 추가: 혹시 남아 있을 수 있는 카운트다운/지연 태스크 누수 방지
                        self._cancel_delay_task()
                        
                        # MFC 내부 상태 완전 초기화 (실패 종료)
                        try:
                            if self.mfc and hasattr(self.mfc, "on_process_finished"):
                                self.mfc.on_process_finished(False)
                        except Exception:
                            pass

                    except Exception as e:
                        self.append_log("MAIN", f"예외 발생 (aborted 처리): {e}")
                        # 예외 시 안전하게 UI를 '대기 중'으로 복귀
                        with contextlib.suppress(Exception):
                            self._clear_queue_and_reset_ui()

                elif kind == "polling_targets":
                    targets = dict(payload.get("targets") or {})
                    self._last_polling_targets = targets
                    self._apply_polling_targets(targets)

                elif kind == "polling":
                    active = bool(payload.get("active", False))

                    # ✅ 공정이 실제 실행 중일 때만 자동 기동
                    if active and self._auto_connect_enabled and self.process_controller.is_running:
                        self._ensure_background_started()

                    # (선택 안전망) active=False면 폴링 타깃을 모두 내리도록 명시
                    if not active:
                        self._apply_polling_targets({
                            "mfc": False, "dc_pulse": False, "rf_pulse": False, "dc": False, "rf": False
                        })

                    params = getattr(self.process_controller, "current_params", {}) or {}
                    use_dc_pulse = bool(params.get("use_dc_pulse", False))
                    use_rf_pulse = bool(params.get("use_rf_pulse", False))
                    use_dc_cont  = bool(params.get("use_dc_power", False))
                    use_rf_cont  = bool(params.get("use_rf_power", False))

                    # 핵심 변경:
                    # - 같은 "계열"만 상호배타
                    #   · DC 연속 ⭕ + RF Pulse ⭕  → 허용
                    #   · DC 연속 ❌ + DC Pulse ⭕  → 금지 (동시 X)
                    #   · RF 연속 ❌ + RF Pulse ⭕  → 금지 (동시 X)
                    base_targets = {
                        "mfc":      active,
                        "dc_pulse": active and self.supports_dc_pulse and use_dc_pulse and not use_dc_cont,
                        "rf_pulse": active and self.supports_rf_pulse and use_rf_pulse and not use_rf_cont,
                        "dc":       active and self.supports_dc_cont  and use_dc_cont  and not use_dc_pulse,
                        "rf":       active and self.supports_rf_cont  and use_rf_cont  and not use_rf_pulse,
                    }

                    # 이전 'polling_targets'로 특정 장치만 허용했으면 그 범위 내에서만 켜기(AND)
                    if self._last_polling_targets:
                        lt = self._last_polling_targets
                        targets = {
                            "mfc":      base_targets["mfc"]      and bool(lt.get("mfc", False)),
                            "dc_pulse": base_targets["dc_pulse"] and bool(lt.get("dc_pulse", False)),
                            "rf_pulse": base_targets["rf_pulse"] and bool(lt.get("rf_pulse", False)),
                            "dc":       base_targets["dc"]       and bool(lt.get("dc", False)),
                            "rf":       base_targets["rf"]       and bool(lt.get("rf", False)),
                        }
                    else:
                        targets = base_targets

                    self._apply_polling_targets(targets)

                else:
                    self.append_log(f"MAIN{self.ch}", f"알 수 없는 PC 이벤트: {kind} {payload}")

            except Exception as e:
                self.append_log(f"MAIN{self.ch}", f"PC 이벤트 처리 예외: {e!r} (kind={kind})")
            finally:
                await asyncio.sleep(0)

    async def _pump_mfc_events(self) -> None:
        async for ev in self.mfc.events():
            k = ev.kind
            if k == "status":
                self.append_log(f"MFC{self.ch}", ev.message or "")
            elif k == "command_confirmed":
                self.process_controller.on_mfc_confirmed(ev.cmd or "")
            elif k == "command_failed":
                why = ev.reason or "unknown"
                self.process_controller.on_mfc_failed(ev.cmd or "", why)
                # 중복 방지: 런타임에서 MFC 장비오류 카드는 전송하지 않음
            elif k == "flow":
                gas = ev.gas or ""
                flow = float(ev.value or 0.0)
                with contextlib.suppress(Exception):
                    self._dl_fire_and_forget(self.data_logger.log_mfc_flow, gas, flow)
                self.append_log(f"MFC{self.ch}", f"[poll] {gas}: {flow:.2f} sccm")
            elif k == "pressure":
                txt = ev.text or (f"{ev.value:.3g}" if ev.value is not None else "")

                # ✅ Working Pressure는 메인 공정(process time) 폴링 구간에서만 수집
                #    - process_controller 에서 polling=True 인 DELAY(step) 동안만
                #      _last_polling_targets["mfc"] 가 True 가 됨
                targets = getattr(self, "_last_polling_targets", None) or {}
                if targets.get("mfc"):
                    with contextlib.suppress(Exception):
                        self.data_logger.log_mfc_pressure(txt)

                # UI / 로그에는 기존처럼 항상 표시
                self.append_log(f"MFC{self.ch}", f"[poll] ChamberP: {txt}")

    async def _pump_ig_events(self) -> None:
        async for ev in self.ig.events():
            k = ev.kind
            if k == "status":
                self.append_log(f"IG{self.ch}", ev.message or "")
            elif k == "pressure":
                try:
                    if ev.pressure is not None:
                        self._dl_fire_and_forget(self.data_logger.log_ig_pressure, float(ev.pressure))
                    elif ev.message:
                        self.data_logger.log_ig_pressure(ev.message)
                except Exception:
                    pass
            elif k == "base_reached":
                self.process_controller.on_ig_ok()
            elif k == "base_failed":
                why = ev.message or "unknown"
                self.process_controller.on_ig_failed("IG", why)
                # 중복 방지: 런타임에서 IG 오류 카드는 전송하지 않음

    async def _pump_rga_events(self) -> None:
        adapter = self.rga
        if not adapter:
            return

        tag = f"RGA{self.ch}"
        finished_called = False  # ✅ 중복 방지(혹시 failed/finished 둘 다 들어오거나 예외 발생 시)

        try:
            async for ev in adapter.events():
                if ev.kind == "status":
                    self.append_log(tag, ev.message or "")

                elif ev.kind == "data":
                    self._graph_update_rga_safe(ev.mass_axis, ev.pressures)
                    # finish는 finished 이벤트에서만 처리(중복 방지)

                elif ev.kind == "finished":
                    if not finished_called:
                        finished_called = True
                        try:
                            self.process_controller.on_rga_finished()
                        except Exception as e:
                            self.append_log(tag, f"on_rga_finished() error: {e!r}")

                elif ev.kind == "failed":
                    why = ev.message or "RGA failed"
                    self.append_log(tag, f"측정 실패: {why} → 다음 단계")

                    # ✅ 워커가 준 stdout/stderr 있으면 같이 남겨서 원인 추적 가능하게
                    with contextlib.suppress(Exception):
                        payload = getattr(ev, "payload", None) or {}
                        stderr = (payload.get("stderr") or "").strip()
                        stdout = (payload.get("stdout") or "").strip()
                        if stderr:
                            self.append_log(tag, f"stderr: {stderr[-800:]}")
                        if stdout:
                            self.append_log(tag, f"stdout: {stdout[-800:]}")

                    if self.chat:
                        with contextlib.suppress(Exception):
                            self.chat.notify_text(f"[{tag}] 측정 실패: {why} → 건너뜀")
                            if hasattr(self.chat, "flush"):
                                self.chat.flush()

                    if not finished_called:
                        finished_called = True
                        self.process_controller.on_rga_finished()

        except Exception as e:
            # ✅ adapter.events() 자체가 예외로 끊겨도 공정은 계속 진행되게
            self.append_log(tag, f"RGA event pump crashed: {e!r} → 다음 단계")
            if self.chat:
                with contextlib.suppress(Exception):
                    self.chat.notify_text(f"[{tag}] 이벤트 루프 예외: {e!r} → 건너뜀")
                    if hasattr(self.chat, "flush"):
                        self.chat.flush()

            if not finished_called:
                self.process_controller.on_rga_finished()

    async def _pump_dc_events(self) -> None:
        if not self.dc_power:
            return
        async for ev in self.dc_power.events():
            k = ev.kind
            if k == "status":
                self.append_log(f"DC{self.ch}", ev.message or "")
            elif k == "display":
                with contextlib.suppress(Exception):
                    self.data_logger.log_dc_power(
                        float(ev.power  or 0.0),
                        float(ev.voltage or 0.0),
                        float(ev.current or 0.0),
                    )
                self._display_dc(ev.power, ev.voltage, ev.current)
                self.append_log(f"DC{self.ch}", f"측정: {float(ev.power or 0.0):.1f} W, {float(ev.voltage or 0.0):.1f} V, {float(ev.current or 0.0):.3f} A")
            elif k == "target_reached":
                self.process_controller.on_dc_target_reached()
            elif k == "target_failed":                      # ★ 추가: 실패 통지 받으면
                self._dc_failed_flag = True                 #    실패 플래그 세우고
                self.process_controller._step_failed("DC Power", ev.message or "low-power")  
            elif k == "power_off_finished":
                if not self._dc_failed_flag:                # ★ 추가: 실패 시에는 OK 토큰(다음 스텝 진행) 차단
                    self.process_controller.on_device_step_ok()
                else:
                    self._dc_failed_flag = False            #    1회성 플래그 해제

    async def _pump_rf_events(self) -> None:
        if not self.rf_power:
            return
        async for ev in self.rf_power.events():
            k = ev.kind
            if k == "status":
                self.append_log(f"RF{self.ch}", ev.message or "")
            elif k == "display":
                fwd = float(ev.forward or 0.0)
                ref = float(ev.reflected or 0.0)
                # 데이터 로거 저장 + UI 갱신 + 텍스트 로그
                with contextlib.suppress(Exception):
                    self._dl_fire_and_forget(self.data_logger.log_rf_power, fwd, ref)
                self._display_rf(fwd, ref)
                self.append_log(f"RF{self.ch}", f"[poll] fwd={fwd:.1f}W, ref={ref:.1f}W")
            elif k == "target_reached":
                self.process_controller.on_rf_target_reached()
            elif k == "target_failed":
                why = ev.message or "unknown"
                self.process_controller.on_rf_target_failed(why)
            elif k == "power_off_finished":
                self.process_controller.on_device_step_ok()

    async def _pump_rfpulse_events(self) -> None:
        if not self.rf_pulse:
            return
        async for ev in self.rf_pulse.events():
            k = ev.kind
            if k == "status":
                self.append_log(f"RFPulse{self.ch}", ev.message or "")
            elif k == "power":
                with contextlib.suppress(Exception):
                    fwd = float(ev.forward or 0.0)
                    ref = float(ev.reflected or 0.0)
                    self._dl_fire_and_forget(self.data_logger.log_rfpulse_power, fwd, ref)
                    self._display_rf(fwd, ref)   # ← 추가: 화면 갱신
            elif k == "target_reached":
                self.process_controller.on_rf_pulse_target_reached()
            elif k == "command_failed":
                why = ev.reason or "unknown"
                self.process_controller.on_rf_pulse_failed(why)
            elif k == "power_off_finished":
                self.process_controller.on_rf_pulse_off_finished()

    async def _pump_dcpulse_events(self) -> None:
        if not self.dc_pulse:
            return
        async for ev in self.dc_pulse.events():
            try:
                k = ev.kind
                if k == "status":
                    self.append_log(f"DCPulse{self.ch}", ev.message or "")
                
                elif k == "telemetry":
                    # 장비 내부 폴링 결과(P/V/I)를 화면/로거에 반영
                    P = getattr(ev, "power",   None)
                    V = getattr(ev, "voltage", None)
                    I = getattr(ev, "current", None)

                    # 혹시 dict 형태로 올 수도 있으니 보강
                    if (P is None or V is None or I is None) and hasattr(ev, "eng"):
                        eng = getattr(ev, "eng") or {}
                        P = P if P is not None else float(eng.get("P_W", 0.0))
                        V = V if V is not None else float(eng.get("V_V", 0.0))
                        I = I if I is not None else float(eng.get("I_A", 0.0))

                    # on_telemetry가 이미 DataLogger에 기록했다면 중복 방지
                    if not callable(getattr(self.data_logger, "log_dcpulse_power", None)):
                        try:
                            self.data_logger.log_dc_power(float(P or 0.0), float(V or 0.0), float(I or 0.0))
                        except Exception:
                            pass

                    self._display_dc(P, V, I)
                    self.append_log(
                        f"DCPulse{self.ch}",
                        f"[telemetry] P={float(P or 0):.1f} W, V={float(V or 0):.2f} V, I={float(I or 0):.3f} A"
                    )

                elif k == "command_confirmed":
                    cmd = (ev.cmd or "").upper()
                    # VERIFIED 포함 처리
                    if cmd.startswith("OUTPUT_ON"):
                        self.process_controller.on_dc_pulse_target_reached()
                    elif cmd.startswith("OUTPUT_OFF"):
                        self.process_controller.on_dc_pulse_off_finished()

                elif k == "command_failed":
                    why_raw = ev.reason or "unknown"
                    why = str(why_raw).lower()
                    cmd = (ev.cmd or "").upper()

                    self.append_log(f"DCPulse{self.ch}", f"CMD FAIL: {cmd} ({why_raw})")

                    # ★ 세트포인트 5회 연속 이탈 또는 P=0W로 인해 드라이버가 AUTO_STOP을 올리면
                    #    → 공정 실패 처리 + 명확한 챗 알림
                    if cmd == "AUTO_STOP" or "target_failed" in why:
                        if self.chat:
                            with contextlib.suppress(Exception):
                                self.chat.notify_error_with_src(
                                    "DCPulse",
                                    "세트포인트 이탈(연속) 또는 P=0W 감지 → 전체 공정 중단"
                                )

                    self.process_controller.on_dc_pulse_failed(why_raw)

            except Exception as e:
                # 펌프 루프 자체가 죽지 않도록 방어
                self.append_log(f"DCPulse{self.ch}", f"[pump] 예외 발생: {e!r}")

    async def _pump_oes_events(self) -> None:
        async for ev in self.oes.events():
            try:
                k = getattr(ev, "kind", None)
                if k == "status":
                    self.append_log(f"OES{self.ch}", ev.message or ""); continue
                if k in ("data", "spectrum", "frame"):
                    x = getattr(ev, "x", None)
                    if x is None: x = getattr(ev, "wavelengths", None)
                    if x is None: x = getattr(ev, "lambda_axis", None)

                    y = getattr(ev, "y", None)
                    if y is None: y = getattr(ev, "intensities", None)
                    if y is None: y = getattr(ev, "counts", None)

                    if x is not None and y is not None:
                        x_list = x.tolist() if hasattr(x, "tolist") else list(x)
                        y_list = y.tolist() if hasattr(y, "tolist") else list(y)
                        self._post_update_oes_plot(x_list, y_list)
                    else:
                        self.append_log(f"OES{self.ch}", f"경고: 데이터 필드 없음: kind={k}")
                    continue

                elif k == "finished":
                    # ✅ cb_oes_run()에서 _oes_active=True로 올린 '이번 런'만 처리
                    if not getattr(self, "_oes_active", False):
                        self.append_log(f"OES{self.ch}", "이전 런 잔여 'finished' 이벤트 무시")
                        continue

                    ok = bool(getattr(ev, "success", False))
                    out_csv = getattr(ev, "out_csv", None) or getattr(ev, "csv_path", None)
                    msg = getattr(ev, "message", None) or ("측정 완료" if ok else "measure failed")

                    if ok:
                        if out_csv:
                            self.append_log(f"OES{self.ch}", f"{msg} (csv={out_csv})")
                        else:
                            self.append_log(f"OES{self.ch}", msg)
                        self._oes_active = False
                        self.process_controller.on_oes_ok()
                    else:
                        # 실패면 stderr tail도 남겨서 원인 추적이 가능하게
                        err_tail = getattr(ev, "error", None)
                        if out_csv:
                            self.append_log(f"OES{self.ch}", f"측정 실패: {msg} (csv={out_csv}) → 종료 절차로 전환")
                        else:
                            self.append_log(f"OES{self.ch}", f"측정 실패: {msg} → 종료 절차로 전환")

                        if err_tail:
                            _t = str(err_tail)
                            if len(_t) > 2000:
                                _t = _t[:2000] + "..."
                            self.append_log(f"OES{self.ch}", f"워커 stderr tail:\n{_t}")

                        self._oes_active = False
                        # 실패 시 다음 런에서 init을 다시 시도하도록 캐시 무효화
                        self._oes_initialized = False
                        self.process_controller.on_oes_failed("OES", msg)
                    continue

                self.append_log(f"OES{self.ch}", f"알 수 없는 이벤트: {ev!r}")
            except Exception as e:
                self.append_log(f"OES{self.ch}", f"이벤트 처리 예외: {e!r}")
                continue

    # ------------------------------------------------------------------
    # 백그라운드 시작/보장
    def _ensure_task_alive(self, name: str, coro_factory: Callable[[], Coroutine[Any, Any, Any]]) -> None:
        self._bg_tasks = [t for t in getattr(self, "_bg_tasks", []) if t and not t.done()]
        for t in self._bg_tasks:
            try:
                if t.get_name() == name and not t.done():
                    return
            except Exception:
                pass
        self._spawn_detached(coro_factory(), store=True, name=name)

    def _ensure_background_started(self) -> None:
        # 🔒 실패 등으로 자동 연결 차단 중이면 아무 것도 올리지 않음
        if not getattr(self, "_auto_connect_enabled", True):
            return
        if getattr(self, "_ensuring_bg", False):
            return
        self._ensuring_bg = True
        try:
            self._ensure_devices_started()
            sel = getattr(self, "_run_select", None) or {}

            self._ensure_task_alive("Pump.PC", self._pump_pc_events)
            self._ensure_task_alive(f"Pump.MFC.{self.ch}", self._pump_mfc_events)  # 항상
            self._ensure_task_alive(f"Pump.IG.{self.ch}",  self._pump_ig_events)   # 항상

            if self.rga:
                self._ensure_task_alive(f"Pump.RGA.{self.ch}", self._pump_rga_events)

            # 연속 DC/RF는 PLC 경유 제어라 기존 그대로(변경 없음)
            if self.dc_power:
                self._ensure_task_alive(f"Pump.DC.{self.ch}", self._pump_dc_events)
            if self.rf_power:
                self._ensure_task_alive(f"Pump.RF.{self.ch}", self._pump_rf_events)

            # 펄스 펌프는 선택된 경우에만
            if self.dc_pulse and sel.get("dc_pulse", False):
                self._ensure_task_alive(f"Pump.DCPulse.{self.ch}", self._pump_dcpulse_events)
            if self.rf_pulse and sel.get("rf_pulse", False):
                self._ensure_task_alive(f"Pump.RFPulse.{self.ch}", self._pump_rfpulse_events)

            self._ensure_task_alive(f"Pump.OES.{self.ch}", self._pump_oes_events)

            self._bg_started = True
        finally:
            self._ensuring_bg = False

    # ──────────────────────────────────────────────────────────────
    # 디바이스 start/connect 보장(중복 호출 안전)
    # ──────────────────────────────────────────────────────────────
    def _ensure_devices_started(self) -> None:
        """MFC/IG는 start(), PLC는 connect()로 워치독/하트비트까지 기동."""
        if getattr(self, "_devices_started", False):
            return
        self._devices_started = True
        self._spawn_detached(self._start_devices_task(), store=True, name=f"DevStart.CH{self.ch}")

    async def _start_devices_task(self) -> None:
        async def _maybe_start_or_connect(obj, label: str, *, log: bool = True):
            if not obj:
                return
            try:
                if self._is_dev_connected(obj):        # ★ 이미 연결됨
                    if log:
                        self.append_log(label, "already connected → skip")
                    return
                
                meth = getattr(obj, "start", None) or getattr(obj, "connect", None)
                if not callable(meth):
                    if log:
                        self.append_log(label, "start/connect 메서드 없음 → skip")
                    return
                
                # ⬇️ start/connect가 동기(blocking)여도 이벤트루프를 막지 않게: to_thread + timeout
                timeout_s = float(getattr(self.cfg.mod, "DEVICE_START_TIMEOUT_S", 20.0))

                try:
                    # meth() 호출 자체를 백그라운드 스레드에서 수행
                    res = await asyncio.wait_for(asyncio.to_thread(meth), timeout=timeout_s)
                except asyncio.TimeoutError:
                    if log:
                        self.append_log(label, f"{getattr(meth, '__name__', 'start/connect')} timeout({timeout_s}s) → skip")
                    return
                except Exception as e:
                    if log:
                        self.append_log(label, f"{getattr(meth, '__name__', 'start/connect')} 호출 실패: {e!r}")
                    return

                # meth가 coroutine을 “반환”하는 타입이면 여기서 await
                if inspect.isawaitable(res):
                    try:
                        await asyncio.wait_for(res, timeout=timeout_s)
                    except asyncio.TimeoutError:
                        if log:
                            self.append_log(label, f"{getattr(meth, '__name__', 'start/connect')} await timeout({timeout_s}s) → skip")
                        return

                if log:
                    self.append_log(label, f"{getattr(meth, '__name__', 'start/connect')} 호출 완료")

            except Exception as e:
                try:
                    name = meth.__name__  # type: ignore[attr-defined]
                except Exception:
                    name = "start/connect"
                if log:
                    self.append_log(label, f"{name} 실패: {e!r}")

        sel = getattr(self, "_run_select", None) or {}

        # PLC는 공유 → 소유자만 로그 출력 (비소유자는 연결 시도하되 로그 무음)
        await _maybe_start_or_connect(self.plc, "PLC", log=self._owns_plc)

        # 나머지는 기존대로 각 챔버에서 로그 출력
        await _maybe_start_or_connect(self.mfc, "MFC")
        await _maybe_start_or_connect(self.ig,  "IG")

        # 펄스 장비는 '이번 런에서 선택된 경우에만' 연결 시도
        if self.dc_pulse and sel.get("dc_pulse", False):
            await _maybe_start_or_connect(self.dc_pulse, "DCPulse")
        if self.rf_pulse and sel.get("rf_pulse", False):
            await _maybe_start_or_connect(self.rf_pulse, "RFPulse")

    # ------------------------------------------------------------------
    # 표시/입력/상태
    def _display_rf(self, for_p: Optional[float], ref_p: Optional[float]) -> None:
        if for_p is None or ref_p is None:
            self.append_log("MAIN", "for.p/ref.p 비어있음"); return
        self._set("forP_edit", f"{for_p:.2f}")
        self._set("refP_edit", f"{ref_p:.2f}")

    def _display_dc(self, power: Optional[float], voltage: Optional[float], current: Optional[float]) -> None:
        if power is None or voltage is None or current is None:
            self.append_log("MAIN", "P/V/I 비어있음"); return
        self._set("Power_edit",   f"{power:.1f}")
        self._set("Voltage_edit", f"{voltage:.1f}")
        self._set("Current_edit", f"{current:.3f}")

    def _on_process_status_changed(self, running: bool) -> None:
        # ✅ Start/Stop 버튼은 '상태와 무관하게 항상 활성화' (사용자 요구)
        b_start = self._u("Start_button"); b_stop = self._u("Stop_button")
        if b_start:
            with contextlib.suppress(Exception):
                b_start.setEnabled(True)
        if b_stop:
            with contextlib.suppress(Exception):
                b_stop.setEnabled(True)

        # ★ PLC 소유권 콜백은 running 값이 실제로 바뀐 경우에만 호출
        prev = getattr(self, "_last_running_state", None)
        if prev is None or prev != running:
            cb = getattr(self, "_notify_plc_owner", None)
            if callable(cb):
                try:
                    cb(self.ch if running else None)
                except Exception:
                    pass

        self._last_running_state = running

    # === 외부 공개: 현재 챔버 공정 실행 여부 ===
    @property
    def is_running(self) -> bool:
        try:
            return bool(self.process_controller.is_running)
        except Exception:
            return False

    # (추가) RF-Pulse 전역 점유(동시 사용 금지) 유틸
    def _try_reserve_rf_pulse(self) -> tuple[bool, int | None]:
        """RF-Pulse 전역 점유 시도. 성공하면 (True, None), 실패하면 (False, owner_ch)."""
        global _RFPULSE_OWNER_CH
        with _RFPULSE_OWNER_LOCK:
            if _RFPULSE_OWNER_CH is None or _RFPULSE_OWNER_CH == self.ch:
                _RFPULSE_OWNER_CH = self.ch
                self._rf_pulse_reserved = True
                return True, None
            return False, _RFPULSE_OWNER_CH

    def _release_rf_pulse_reservation(self) -> None:
        """내가 점유자라면 RF-Pulse 전역 점유 해제."""
        global _RFPULSE_OWNER_CH
        with _RFPULSE_OWNER_LOCK:
            if _RFPULSE_OWNER_CH == self.ch:
                _RFPULSE_OWNER_CH = None
        self._rf_pulse_reserved = False

    def _apply_process_state_message(self, message: str) -> None:
        if getattr(self, "_last_state_text", None) == message:
            return
        self._last_state_text = message
        if self._w_state:
            self._w_state.setPlainText(message)

    def _fmt_hms(self, seconds: float) -> str:
        if seconds < 0:
            seconds = 0
        s = int(seconds)
        h, m, sec = s // 3600, (s % 3600) // 60, s % 60
        return f"{h:02d}:{m:02d}:{sec:02d}"

    def _set_state_text(self, text: str) -> None:
        self._last_state_text = str(text)
        if self._w_state:
            try:
                self._w_state.setPlainText(self._last_state_text)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # 파일 로딩 / UI 반영
    def _connect_my_buttons(self) -> None:
        if not self._has_ui():
            self._set_default_ui_values()  # 필요 없으면 생략 가능
            return

        btn = self._u("Start_button")
        if btn: btn.clicked.connect(self._handle_start_clicked)

        btn = self._u("Stop_button")
        if btn: btn.clicked.connect(self._handle_stop_clicked)

        btn = self._u("processList_button")
        if btn:
            btn.clicked.connect(lambda: self._spawn_detached(self._handle_process_list_clicked_async()))

        if self._w_log:
            self._w_log.setMaximumBlockCount(2000)
        self._set_default_ui_values()

    async def _handle_process_list_clicked_async(self) -> None:
        start_dir = getattr(self, "_last_process_list_dir", "") or r"\\VanaM_NAS\VanaM_toShare"

        file_path = await self._aopen_file(
            caption=f"CH{self.ch} 프로세스 리스트 파일 선택",
            start_dir=start_dir,
            name_filter="CSV Files (*.csv);;All Files (*)"
        )

        if file_path:
            # 다음번에 다시 열 때 마지막 폴더부터 열리게
            with contextlib.suppress(Exception):
                self._last_process_list_dir = str(Path(file_path).parent)

        if not file_path:
            self.append_log("File", "파일 선택 취소")
            return

        self.append_log("File", f"선택된 파일: {file_path}")
        try:
            with open(file_path, mode='r', encoding='utf-8-sig', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                self.process_queue: list[RawParams] = []
                self.current_process_index: int = -1
                for row in reader:
                    name = (row.get('Process_name') or row.get('#') or f"공정 {len(self.process_queue)+1}").strip()
                    row['Process_name'] = name
                    self.process_queue.append(cast(RawParams, row))
                if not self.process_queue:
                    self.append_log("File", "파일에 공정이 없습니다.")
                    return
                self.append_log("File", f"총 {len(self.process_queue)}개 공정 읽음.")
                self._update_ui_from_params(self.process_queue[0])
        except Exception as e:
            self.append_log("File", f"파일 처리 오류: {e}")

    def _update_ui_from_params(self, params: RawParams) -> None:
        if self._w_log:
            if getattr(self, "process_queue", None):
                total = len(self.process_queue); current = getattr(self, "current_process_index", -1) + 1
                self.append_log("UI", f"[CH{self.ch}] 자동 공정 ({current}/{total}) 준비: '{params.get('Process_name','')}'")
            else:
                self.append_log("UI", f"[CH{self.ch}] 단일 공정 UI 업데이트: '{params.get('process_note','')}'")

        _set = self._set

        # --- Pulse UI ---
        if self.ch == 1:
            # ✅ CH1: Pulse 입력(Edit/Freq/Duty)은 dcPulse* 1세트를 공용으로 사용한다.
            #    RF/DC 선택은 라디오(rfPulsePower_checkbox / dcPulsePower_checkbox)로 표시한다.
            def _is_true(v) -> bool:
                return str(v).strip().upper() in ("T", "TRUE", "1", "Y", "YES")

            def _pos_num(v) -> bool:
                try:
                    s = str(v).strip()
                    if s == "" or s.lower() == "nan":
                        return False
                    return float(s) > 0.0
                except Exception:
                    return False

            use_rf_flag = _is_true(params.get("use_rf_pulse", "F"))
            use_dc_flag = _is_true(params.get("use_dc_pulse", "F"))

            rf_power = params.get("rf_pulse_power", "0")
            rf_freq  = str(params.get("rf_pulse_freq", "")).strip()
            rf_duty  = str(params.get("rf_pulse_duty_cycle", "")).strip()

            dc_power = params.get("dc_pulse_power", "0")
            dc_freq  = str(params.get("dc_pulse_freq", "")).strip()
            dc_duty  = str(params.get("dc_pulse_duty_cycle", "")).strip()

            rf_requested = use_rf_flag or _pos_num(rf_power) or (rf_freq not in ("", "0")) or (rf_duty not in ("", "0"))
            dc_requested = use_dc_flag or _pos_num(dc_power) or (dc_freq not in ("", "0")) or (dc_duty not in ("", "0"))

            sel = None
            if use_rf_flag and not use_dc_flag:
                sel = "rf"
            elif use_dc_flag and not use_rf_flag:
                sel = "dc"
            elif rf_requested and not dc_requested:
                sel = "rf"
            elif dc_requested and not rf_requested:
                sel = "dc"
            elif rf_requested and dc_requested:
                sel = "rf"
                self.append_log("UI", "[CH1] CSV에 RF/DC Pulse가 동시에 설정됨 → UI는 RF로 표시(실행은 validate에서 차단)")
            else:
                sel = None  # pulse 미사용

            _set("rfPulsePower_checkbox", sel == "rf")
            _set("dcPulsePower_checkbox", sel == "dc")

            if sel == "rf":
                power, freq, duty = rf_power, rf_freq, rf_duty
            elif sel == "dc":
                power, freq, duty = dc_power, dc_freq, dc_duty
            else:
                power, freq, duty = "0", "", ""

            _set("dcPulsePower_edit", power)
            _set("dcPulseFreq_edit",      "" if str(freq).strip() in ("", "0", "nan") else str(freq).strip())
            _set("dcPulseDutyCycle_edit", "" if str(duty).strip() in ("", "0", "nan") else str(duty).strip())

        else:
            # CH2: DC-Pulse(옵션) / RF-Pulse 사용 가능 (supports + validate + 전역 점유로 안전 제어)
            _set("dcPulsePower_checkbox", params.get('use_dc_pulse', 'F') == 'T')
            _set("dcPulsePower_edit",     params.get('dc_pulse_power', '0'))
            dcf = str(params.get('dc_pulse_freq', '')).strip()
            dcd = str(params.get('dc_pulse_duty_cycle', '')).strip()
            _set("dcPulseFreq_edit",      '' if dcf in ('', '0') else dcf)
            _set("dcPulseDutyCycle_edit", '' if dcd in ('', '0') else dcd)

            _set("rfPulsePower_checkbox", params.get('use_rf_pulse', 'F') == 'T')
            _set("rfPulsePower_edit",     params.get('rf_pulse_power', '0'))
            rff = str(params.get('rf_pulse_freq', '')).strip()
            rfd = str(params.get('rf_pulse_duty_cycle', '')).strip()
            _set("rfPulseFreq_edit",      '' if rff in ('', '0') else rff)
            _set("rfPulseDutyCycle_edit", '' if rfd in ('', '0') else rfd)
        
        # DC-Pulse
        # _set("dcPulsePower_checkbox", params.get('use_dc_pulse', 'F') == 'T')
        # _set("dcPulsePower_edit",     params.get('dc_pulse_power', '0'))
        # dcf = str(params.get('dc_pulse_freq', '')).strip()
        # dcd = str(params.get('dc_pulse_duty_cycle', '')).strip()
        # _set("dcPulseFreq_edit",       '' if dcf in ('', '0') else dcf)
        # _set("dcPulseDutyCycle_edit",  '' if dcd in ('', '0') else dcd)

        # DC-Power
        _set("dcPower_checkbox", params.get('use_dc_power', 'F') == 'T')
        _set("dcPower_edit", params.get('dc_power', '0'))

        # RF-Pulse
        # _set("rfPulsePower_checkbox", params.get('use_rf_pulse', 'F') == 'T')
        # _set("rfPulsePower_edit",     params.get('rf_pulse_power', '0'))
        # rff = str(params.get('rf_pulse_freq', '')).strip()
        # rfd = str(params.get('rf_pulse_duty_cycle', '')).strip()
        # _set("rfPulseFreq_edit",       '' if rff in ('', '0') else rff)
        # _set("rfPulseDutyCycle_edit",  '' if rfd in ('', '0') else rfd)

        # RF-Power
        _set("rfPower_checkbox", params.get('use_rf_power', 'F') == 'T')
        _set("rfPower_edit",     params.get('rf_power', '0'))

        _set("processTime_edit", params.get('process_time', '0'))
        # ✅ Integration Time 입력칸을 'Process Name' 입력으로 재활용
        #    (CSV 자동 공정: Process_name 표시 / UI 수동 공정: 사용자가 입력)
        _set("integrationTime_edit", params.get('Process_name', params.get('process_note', '')))
        _set("arFlow_edit", params.get('Ar_flow', '0'))
        _set("o2Flow_edit", params.get('O2_flow', '0'))
        _set("n2Flow_edit", params.get('N2_flow', '0'))
        _set("workingPressure_edit", params.get('working_pressure', '0'))
        _set("basePressure_edit", params.get('base_pressure', '0'))
        _set("shutterDelay_edit", params.get('shutter_delay', '0'))

        _set("G1_checkbox", params.get('gun1', 'F') == 'T')
        _set("G2_checkbox", params.get('gun2', 'F') == 'T')
        _set("G3_checkbox", params.get('gun3', 'F') == 'T')
        _set("Ar_checkbox", params.get('Ar', 'F') == 'T')
        _set("O2_checkbox", params.get('O2', 'F') == 'T')
        _set("N2_checkbox", params.get('N2', 'F') == 'T')
        _set("mainShutter_checkbox", params.get('main_shutter', 'F') == 'T')
        _set("powerSelect_checkbox", params.get('power_select', 'F') == 'T')

        # ---- CH1: 단일 타겟 위젯에 한 번만 세팅 ----
        if self.ch == 1:
            name = (str(params.get('G1 Target', '')).strip()
                    or str(params.get('G2 Target', '')).strip()
                    or str(params.get('G3 Target', '')).strip())
            _set("g1Target_name", name)
        else:
            _set("g1Target_name", str(params.get('G1 Target', '')).strip())
            _set("g2Target_name", str(params.get('G2 Target', '')).strip())
            _set("g3Target_name", str(params.get('G3 Target', '')).strip())

    def _set(self, leaf: str, v: Any) -> None:
        w = self._u(leaf)
        if w is None:
            return
        try:
            if hasattr(w, "setChecked"):
                w.setChecked(bool(v))
                return

            if hasattr(w, "setValue"):
                try:
                    w.setValue(v if isinstance(v, (int, float)) else float(str(v)))
                except Exception:
                    pass
                else:
                    return

            s = str(v)
            if hasattr(w, "setPlainText"):
                w.setPlainText(s)
                return
            if hasattr(w, "setText"):
                w.setText(s)
                return
        except Exception as e:
            self.append_log("UI", f"_set('{leaf}') 실패: {e!r}")

    # ------------------------------------------------------------------
    # 자동 시퀀스
    def _start_next_process_from_queue(self, was_successful: bool) -> None:
        if getattr(self, "_advancing", False):
            self.append_log("MAIN", "자동 진행 중복 호출 무시"); return
        self._advancing = True
        try:
            if self.process_controller.is_running and getattr(self, "current_process_index", -1) > -1:
                self.append_log("MAIN", "경고: 전환 시점에 이미 실행 중"); return

            if not was_successful:
                self.append_log("MAIN", "이전 공정 실패 → 자동 중단")
                self._clear_queue_and_reset_ui(); return

            self.current_process_index = getattr(self, "current_process_index", -1) + 1
            if self.current_process_index < len(getattr(self, "process_queue", [])):
                params = self.process_queue[self.current_process_index]
                self._update_ui_from_params(params)
                if self._try_handle_delay_step(params):
                    return
                
                # ------------------------------------------------------------
                # TEST MODE : CSV의 #열이 test 이면 장비제어 없이 실행
                # ------------------------------------------------------------
                marker = str(params.get("#") or "").strip().lower()
                if marker == "test":
                    time_str = str(params.get("time") or "").strip()
                    test_duration_sec = self._parse_duration_seconds(time_str.lower())

                    params["test_mode"] = True
                    params["test_duration_sec"] = test_duration_sec

                    # ✅ 구글챗/카드에서 분 단위 값을 쓰는 경우 대비
                    if test_duration_sec > 0:
                        params.setdefault("process_time", round(test_duration_sec / 60.0, 3))

                    # ✅ 카드/로그에 표시될 이름 보강
                    params.setdefault("process_note", params.get("Process_name") or "TEST")

                    self.append_log("MAIN", f"[TEST MODE] {test_duration_sec:.1f}s 동안 시뮬레이션 실행")
                    self._safe_start_process(params)
                    return
                # ------------------------------------------------------------

                norm = self._normalize_params_for_process(params)
                
                # 입력값 검증
                errs = self._validate_norm_params(norm)
                if errs:
                    # ✅ 레시피 오류도 파일 로그로 남기기
                    with contextlib.suppress(Exception):
                        if not getattr(self, "_log_file_path", None):
                            self._open_run_log(norm)

                    self.append_log("Validate", "CSV 공정 파라미터 오류:\n - " + "\n - ".join(errs))
                    self._clear_queue_and_reset_ui()
                    return
                
                # ✅ 중요:
                # - 이전 공정의 finished 처리에서 이미 _stop_device_watchdogs(light=False)가
                #   _shutdown_log_writer()까지 await로 끝내는 구조다.
                # - 여기서 _shutdown_log_writer()를 detached로 또 돌리면,
                #   "새 공정 시작"과 경합(race)하면서 새 로그/프리플라이트가 꼬일 수 있다.
                #
                # 따라서 "다음 공정 시작" 단계에서는 writer shutdown을 재호출하지 않는다.
                # with contextlib.suppress(Exception):
                #     self._spawn_detached(self._shutdown_log_writer())

                self._log_file_path = None

                # (NEW) 최근 'chamber' 종료 시각 기준 쿨다운을 반영해서 다음 스텝 대기
                try:
                    remain = float(runtime_state.remaining_cooldown("chamber", self.ch, 60.0))
                except Exception:
                    remain = 0.0

                # 🚫 첫 번째 스텝(인덱스 0)은 강제 60초 대기 없이 즉시 시작
                first_step = (self.current_process_index == 0)

                # remain 자체가 "남은 시간"이므로 그대로 쓰는 게 맞음
                delay_s = max(0.0, float(remain))

                reason = ("최근 종료로 인한 대기" if first_step else "쿨다운 대기")

                # ✅ Start를 눌러 “시작 시도”되는 순간 바로 로그 파일을 연다
                with contextlib.suppress(Exception):
                    if not getattr(self, "_log_file_path", None):
                        self._open_run_log(norm)

                # 지연이 없으면: 태스크 예약을 거치지 말고 즉시 시작(간헐적 예약 실패/정지 방지)
                if delay_s <= 0.0:
                    self.append_log("MAIN", f"다음 공정 즉시 시작(쿨다운 remain={remain:.3f}s)")
                    self._set_state_text("다음 공정 시작 준비 중…")
                    self._cancel_delay_task()
                    self._safe_start_process(norm)   # ✅ 검증된 norm으로 바로 진입
                    return

                # 지연 필요 시: 첫 스텝이면 '최근 종료로 인한 대기', 이후 스텝은 '쿨다운 대기'
                reason = ("최근 종료로 인한 대기" if first_step else "쿨다운 대기")
                self._set_state_text(f"다음 공정 대기중 ({reason}) · 남은 시간 {self._fmt_hms(delay_s)}")

                # 파일 로그에도 “왜/얼마나 기다리는지” 남김
                self.append_log("MAIN", f"다음 공정 대기 시작: delay_s={delay_s:.1f}s, reason={reason}, remain={remain:.3f}s")

                self._cancel_delay_task()
                self._set_task_later(
                    "_delay_main_task",
                    self._start_process_later(params, delay_s, reason=reason),
                    name=f"NextProcDelay.CH{self.ch}"
                )

            else:
                self._clear_queue_and_reset_ui()
                # (주의) 장치 연결 해제는 finished 분기에서 이미 수행함
                # ★ 추가: 정상 종료 + 더 이상 다음 공정이 없으면 장치 연결 해제(PLC 제외)
                #self._spawn_detached(self._stop_device_watchdogs(light=False), name="FullCleanup.EndRun")
        finally:
            self._advancing = False

    async def _start_process_later(self, params: RawParams, delay_s: float = 0.1, *, reason: str = "") -> None:
        if delay_s <= 0.5:
            self._safe_start_process(self._normalize_params_for_process(params))
            return

        # ETA 로그
        rtxt = f" ({reason})" if reason else ""
        try:
            eta = datetime.now() + timedelta(seconds=delay_s)
            self.append_log("MAIN", f"다음 공정 예약: {delay_s:.0f}s 후 {eta.strftime('%H:%M:%S')}{rtxt}")
        except Exception:
            pass

        # 카운트다운 태스크만 관리(메인 태스크는 절대 자기 자신을 취소하지 않음)
        async def _countdown_loop():
            try:
                remain = int(delay_s)
                self._set_state_text(f"다음 공정 대기중{rtxt} · 남은 시간 {self._fmt_hms(remain)}")
                while remain > 0:
                    await asyncio.sleep(1)
                    remain -= 1
                    if remain <= 60 or (remain % 5 == 0):
                        self._set_state_text(f"다음 공정 대기중{rtxt} · 남은 시간 {self._fmt_hms(remain)}")
            except asyncio.CancelledError:
                raise

        loop = asyncio.get_running_loop()
        # 기존 카운트다운이 있으면 취소
        if self._delay_countdown_task and not self._delay_countdown_task.done():
            self._delay_countdown_task.cancel()
        # 새 카운트다운 등록
        self._delay_countdown_task = loop.create_task(
            _countdown_loop(), name=f"CH{self.ch}-NextProcCountdown"
        )

        try:
            # 메인 태스크는 단순 대기만 수행(자기-취소 금지)
            await asyncio.sleep(delay_s)
        except asyncio.CancelledError:
            self._set_state_text("다음 공정 대기 취소됨")
            # 카운트다운도 함께 정리
            if self._delay_countdown_task and not self._delay_countdown_task.done():
                self._delay_countdown_task.cancel()
            self._delay_countdown_task = None
            raise
        finally:
            # 정상 시작/취소 직전 카운트다운 정리
            if self._delay_countdown_task:
                try:
                    self._delay_countdown_task.cancel()
                except Exception:
                    pass
                self._delay_countdown_task = None

        # 이제 진짜 시작
        self._set_state_text("다음 공정 시작 준비 중…")
        self._safe_start_process(self._normalize_params_for_process(params))

    # ✅ PLC read_bit에 timeout 강제 (무한대기 방지)
    async def _plc_read_bit_safe(self, key: str, *, timeout_s: float = 0.6) -> bool | None:
        if not getattr(self, "plc", None):
            self.append_log("PLC", f"PLC 없음: read_bit({key})")
            return None
        try:
            v = await asyncio.wait_for(self.plc.read_bit(key), timeout=timeout_s)
            return bool(v)
        except asyncio.TimeoutError:
            self.append_log("PLC", f"read_bit timeout: {key} ({timeout_s:.1f}s)")
            return None
        except Exception as e:
            self.append_log("PLC", f"read_bit failed: {key}: {e!r}")
            return None
        
    async def _preflight_watchdog(self, run_gen: int, timeout_s: float = 25.0) -> None:
        """
        프리플라이트가 특정 시간 안에 실제 공정(start_process)로 넘어가지 못하면
        자동으로 stop/정리 시퀀스를 태워서 '프리플라이트에서 영구 멈춤'을 복구한다.
        """
        try:
            await asyncio.sleep(float(timeout_s))

            # 더 최신 Start가 있으면 무시
            if int(getattr(self, "_active_run_gen", 0)) != int(run_gen):
                return

            # 이미 공정이 시작됐으면 정상
            if bool(getattr(self.process_controller, "is_running", False)):
                return

            # 아직 프리플라이트 상태라면 "멈춤"으로 판단
            state_txt = str(getattr(self, "_last_state_text", "") or "")
            if "프리플라이트" not in state_txt:
                return

            self.append_log("MAIN", f"⚠ preflight watchdog timeout({timeout_s:.0f}s) → 자동 정리/상태 복구")
            # Stop이 “공정중 아님”으로 막히던 케이스까지 복구해야 하므로 request_stop_all을 탄다.
            self.request_stop_all(user_initiated=False)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.append_log("MAIN", f"preflight watchdog exception: {e!r}")

    def _safe_start_process(self, params: NormParams) -> None:
        # 0) 이미 실행 중이면 즉시 실패 처리
        if self.process_controller.is_running:
            msg = "이미 다른 공정 실행 중"
            self.append_log("MAIN", msg)

            with contextlib.suppress(Exception):
                runtime_state.set_error("chamber", self.ch, msg)
                runtime_state.mark_finished("chamber", self.ch)

            with contextlib.suppress(Exception):
                self._host_report_start(False, msg)

            self._set_state_text(msg)
            self._on_process_status_changed(False)
            return

        # 1) ✅ Start 누르는 즉시 로그 파일 “생성” 보장 (멈춰도 파일이 남게)
        try:
            if not getattr(self, "_log_file_path", None):
                self._open_run_log(params)  # 내부에서 writer 큐/헤더 기록
        except Exception as e:
            self.append_log("Logger", f"_open_run_log failed: {e!r}")

        # 2) ✅ preflight 진입 순간부터 UI는 running (Stop 활성화)
        self._set_state_text("프리플라이트(장비 확인) 중…")
        self._on_process_status_changed(True)

        # 3) ✅ 전역 running 마킹(큐/자동시작 포함) - 여기서 1회만
        with contextlib.suppress(Exception):
            runtime_state.mark_started("chamber", self.ch)

        # 4) ✅ preflight 실행(예외는 _spawn_detached done_callback에서 로그로 남음)
        # ✅ 이번 Start 시도 세대 번호 증가
        self._run_gen = int(getattr(self, "_run_gen", 0)) + 1
        gen = self._run_gen
        self._active_run_gen = gen

        self._spawn_detached(
            self._start_after_preflight(params, gen),
            store=True,
            name=f"StartAfterPreflight.CH{self.ch}.g{gen}",
        )

        # ✅ 프리플라이트 watchdog: 일정 시간 내 진행 없으면 자동 정리/복귀
        self._spawn_detached(
            self._preflight_watchdog(gen, timeout_s=25.0),
            store=True,
            name=f"PreflightWD.CH{self.ch}.g{gen}",
        )

    # ✅ Gate(밸브) 인터락: 시작하려는 챔버의 Gate가 CLOSED인지 확인
    async def _check_gate_closed_before_start(self) -> bool:
        """
        Gate lamp 기준 상태 판정.
        - closed: CLOSE_LAMP=True & OPEN_LAMP=False
        - open  : OPEN_LAMP=True  & CLOSE_LAMP=False
        - moving_or_unknown: 둘 다 False
        - invalid_both_true: 둘 다 True (배선/맵/PLC 로직 이상 가능)

        시작 조건:
        - 'closed'일 때만 True
        - 그 외는 모두 False(시작 차단)
        """
        if not getattr(self, "plc", None):
            self.append_log("MAIN", f"[CH{self.ch}] PLC 없음 → Gate 상태 확인 불가 → 시작 차단")
            return False

        ch = int(getattr(self, "ch", 0) or 0)
        if ch not in (1, 2):
            self.append_log("MAIN", f"[CH{self.ch}] 잘못된 CH={ch} → 시작 차단")
            return False

        open_key = f"G_V_{ch}_OPEN_LAMP"
        close_key = f"G_V_{ch}_CLOSE_LAMP"

        # (선택) 전이 상태(moving)일 때 잠깐만 재확인(짧게)
        for _ in range(5):  # 5회 * 0.2s = 최대 1초
            try:
                open_lamp = await asyncio.wait_for(self.plc.read_bit(open_key),  timeout=0.6)
                close_lamp = await asyncio.wait_for(self.plc.read_bit(close_key), timeout=0.6)
                open_lamp  = bool(open_lamp)
                close_lamp = bool(close_lamp)

                # timeout/예외로 값을 못 읽으면 → “상태 확인 불가”로 시작 차단 + 사용자에게 알림
                if open_lamp is None or close_lamp is None:
                    self.append_log("MAIN", f"[CH{self.ch}] Gate 상태 읽기 실패/timeout → 시작 차단")
                    return False
            except KeyError as e:
                self.append_log("MAIN", f"[CH{self.ch}] PLC 주소맵에 gate lamp 키 없음: {e} → 시작 차단")
                return False
            except Exception as e:
                self.append_log("MAIN", f"[CH{self.ch}] Gate lamp 읽기 실패: {type(e).__name__}: {e} → 시작 차단")
                return False

            if close_lamp and (not open_lamp):
                # ✅ 정상: CLOSED
                return True

            if open_lamp and (not close_lamp):
                # ❌ OPEN
                self.append_log("MAIN", f"[CH{self.ch}] Gate 상태=OPEN (open={open_lamp}, close={close_lamp})")
                return False

            if open_lamp and close_lamp:
                # ❌ 비정상(둘 다 TRUE)
                self.append_log("MAIN", f"[CH{self.ch}] Gate lamp 이상(OPEN/CLOSE 모두 TRUE) (open={open_lamp}, close={close_lamp})")
                return False

            # 둘 다 False면 moving/unknown → 잠깐 기다렸다가 재확인
            await asyncio.sleep(0.2)

        self.append_log("MAIN", f"[CH{self.ch}] Gate 상태=moving_or_unknown (OPEN/CLOSE 모두 FALSE) → 시작 차단")
        return False

    async def _start_after_preflight(self, params: NormParams, run_gen: int) -> None:
        try:
            # ✅ 더 최신 Start가 들어오면(세대 불일치) 이 태스크는 조용히 종료
            if int(getattr(self, "_active_run_gen", 0)) != int(run_gen):
                return

            # ⬇️ 추가: 이전 런의 잔여 종료 플래그를 명시적으로 클리어
            self._pc_stopping = False
            self._pending_device_cleanup = False

            # ------------------------------------------------------------
            # TEST MODE : preflight/인터락/chuck/장비연결 전부 스킵
            # ------------------------------------------------------------
            if bool(params.get("test_mode", False)):
                # ✅ TEST MODE에서는 장비 자동연결/워치독을 절대 올리지 않음
                self._auto_connect_enabled = False
                self._run_select = None

                time_str = str(params.get("time", "")).strip()
                dur_s = float(params.get("test_duration_sec", 0.0) or 0.0)

                if dur_s <= 0 and time_str:
                    dur_s = self._parse_duration_seconds(time_str.lower())
                    params["test_duration_sec"] = dur_s

                if dur_s <= 0:
                    try:
                        dur_s = float(params.get("process_time", 0.0)) * 60.0
                    except Exception:
                        dur_s = 0.0

                dur_s = max(1.0, float(dur_s))

                # ✅ 카드/로그용 분 단위
                params.setdefault("process_time", round(dur_s / 60.0, 3))
                params.setdefault("process_note", params.get("Process_name") or "TEST")

                note = params.get("process_note") or "TEST"
                self.append_log("MAIN", f"[TEST MODE] '{note}' 장비 제어 스킵 / {dur_s:.1f}s 시뮬레이션")
                self._host_report_start(True, f"TEST MODE: {time_str or f'{dur_s:.0f}s'}")

                # ✅ 상태 RUNNING (UI/상태/구글챗 흐름은 정상 공정과 동일)
                self._on_process_status_changed(True)

                # ✅ 핵심: ProcessController가 TEST MODE(DELAY) 시퀀스로 실행
                self.process_controller.start_process(params)
                return
            # ------------------------------------------------------------
            
            # ✅ REAL MODE부터 여기서 장비 연결/백그라운드 허용
            self._auto_connect_enabled = True

            # ✅ 이번 런에서 실제로 사용할 펄스만 표시(IG/MFC는 항상 연결이므로 제외)
            def _pos(v) -> bool:
                try:
                    return float(v) > 0.0
                except Exception:
                    return False

            # ✅ "요청" 판정(체크박스 + 값)
            rf_requested = (
                bool(params.get("use_rf_pulse", False))
                or _pos(params.get("rf_pulse_power", 0.0))
                or (params.get("rf_pulse_freq") is not None)
                or (params.get("rf_pulse_duty") is not None)
            )

            dc_requested = (
                bool(params.get("use_dc_pulse", False))
                or _pos(params.get("dc_pulse_power", 0.0))
                or (params.get("dc_pulse_freq") is not None)
                or (params.get("dc_pulse_duty") is not None)
            )

            use_dc_pulse = bool(dc_requested) and self.supports_dc_pulse
            use_rf_pulse = bool(rf_requested) and self.supports_rf_pulse

            # ✅ RF-Pulse 동시 사용 금지(전역 점유)
            if use_rf_pulse:
                ok_res, owner = self._try_reserve_rf_pulse()
                if not ok_res:
                    msg = f"RF-Pulse는 동시에 1개 챔버만 사용할 수 있습니다. 현재 CH{owner}에서 사용 중입니다."
                    self.append_log("MAIN", msg)

                    # Host start 요청도 즉시 실패로 응답(대기/타임아웃 방지)
                    self._host_report_start(False, msg)

                    self._auto_connect_enabled = False
                    self._run_select = None
                    self._on_process_status_changed(False)

                    with contextlib.suppress(Exception):
                        runtime_state.set_error("chamber", self.ch, msg)
                        runtime_state.mark_finished("chamber", self.ch)

                    self._start_next_process_from_queue(False)
                    return

            self._run_select = {
                "dc_pulse": use_dc_pulse,
                "rf_pulse": use_rf_pulse,
            }

            # ✅ Pulse는 각 장비의 "자기 포트" 설정을 그대로 사용한다. (덮어쓰기 금지)
            # - DC Pulse  : cfg.DCPULSE_TCP
            # - RF Pulse  : RFPulseAsync 내부 설정(또는 cfg.RFPULSE_TCP가 있으면 그 값)

            # (선택) DC는 필요시 재연결만 수행 (원래도 DC 포트를 써야 하니까 OK)
            if use_dc_pulse and self.dc_pulse and hasattr(self.dc_pulse, "set_endpoint_reconnect"):
                host, port = self.cfg.DCPULSE_TCP
                try:
                    await asyncio.wait_for(self.dc_pulse.set_endpoint_reconnect(host, port), timeout=2.0)
                except asyncio.TimeoutError:
                    raise RuntimeError("DC-Pulse reconnect timeout(2s)")

            # ✅ RF는 DCPULSE_TCP로 절대 덮어쓰지 않는다.
            #    - RFPulseAsync가 내부적으로 RF 포트를 알고 있으면: 아무 것도 안 해도 됨
            #    - cfg.RFPULSE_TCP가 준비되어 있으면: 그 값으로만 reconnect
            if use_rf_pulse and self.rf_pulse and hasattr(self.rf_pulse, "set_endpoint_reconnect"):
                rf_tcp = getattr(self.cfg, "RFPULSE_TCP", None)
                if rf_tcp:
                    host, port = rf_tcp
                    try:
                        await asyncio.wait_for(self.rf_pulse.set_endpoint_reconnect(host, port), timeout=2.0)
                    except asyncio.TimeoutError:
                        raise RuntimeError("RF-Pulse reconnect timeout(2s)")
                # else: RFPulseAsync 내부 설정을 그대로 사용

            self._ensure_background_started()

            # ✅ Start 클릭 시점에 OES init을 백그라운드로 '미리' 수행
            self._kick_oes_init_background(force=False)

            self._on_process_status_changed(True)

            timeout = 10.0 if (use_dc_pulse or use_rf_pulse) else 8.0
            ok, failed = await self._preflight_connect(params, timeout_s=timeout)

            if not ok:
                fail_list = ", ".join(failed) if failed else "알 수 없음"
                self.append_log("MAIN", f"필수 장비 연결 실패: {fail_list} → 시작 중단")
                self._post_critical(
                    "장비 연결 실패",
                    "다음 장비 연결을 확인하지 못했습니다:\n"
                    f" - {fail_list}\n\n케이블/전원/포트 설정 확인 후 재시도"
                )

                # ✅ 자동 재연결 자체 차단 (이후 _ensure_background_started 가 장치 start 못 올리도록)
                self._auto_connect_enabled = False

                # ✅ 이미 올라가 있던 워치독/연결 태스크 완전 정지
                try:
                    await self._stop_device_watchdogs(light=False)
                except Exception:
                    pass

                # (선택) 폴링 상태도 명시적으로 내려줌 — 없어도 무방
                with contextlib.suppress(Exception): self.mfc.set_process_status(False)
                with contextlib.suppress(Exception):
                    if hasattr(self.ig, "set_process_status"): self.ig.set_process_status(False)
                with contextlib.suppress(Exception):
                    if self.dc_pulse and hasattr(self.dc_pulse, "set_process_status"):
                        self.dc_pulse.set_process_status(False)

                self._on_process_status_changed(False)

                # ✅ 전역 점유/쿨다운을 ‘실패 종료’로 명확히 정리
                try:
                    runtime_state.set_error("chamber", self.ch, f"preflight connect failed: {fail_list}")
                    runtime_state.mark_finished("chamber", self.ch)
                except Exception:
                    pass

                self._start_next_process_from_queue(False)
                return
            
            # ✅ Gate(밸브) 인터락: 시작하려는 챔버의 Gate가 열려 있으면 공정 시작을 차단
            # - Plasma Cleaning은 Gate를 열고 진행하므로, Gate Open 상태면 Sputter Start 금지
            ok_gate = await self._check_gate_closed_before_start()
            if not ok_gate:
                self.append_log("MAIN", f"[CH{self.ch}] Gate Open → 공정 시작 차단")

                # ✅ 이미 올라간 연결/펌프/점유를 정리(특히 RF-Pulse 점유 해제 목적)
                self._auto_connect_enabled = False
                with contextlib.suppress(Exception):
                    await self._stop_device_watchdogs(light=False)

                self._on_process_status_changed(False)
                with contextlib.suppress(Exception):
                    runtime_state.set_error("chamber", self.ch, "gate open")
                    runtime_state.mark_finished("chamber", self.ch)

                self._start_next_process_from_queue(False)
                return
            
            # ★ 추가: 공정 시작 직전 Chuck 위치 선행 설정
            self._run_chuck_position = str(params.get("chuck_position") or "").strip().lower()
            self._run_warnings = []

            ok_chuck = await self._set_chuck_position_if_needed(params)
            if not ok_chuck:
                pos = self._run_chuck_position
                warn = f"Chuck 위치 이동 실패 (target='{pos}')" if pos else "Chuck 위치 이동 실패"
                self.append_log("MAIN", f"⚠️ {warn} → 공정은 계속 진행")
                self._run_warnings.append(warn)
                # ✅ 여기서 실패처리/return/큐 fail 처리/critical/error 기록 전부 하지 않음

            self._last_polling_targets = None
            self.append_log("MAIN", "장비 연결 확인 완료 → 공정 시작")
            self.process_controller.start_process(params)

        except Exception as e:
            note = params.get("process_note", "알 수 없는")
            msg = f"오류: '{note}' 시작 실패. ({e})"
            self.append_log("MAIN", msg)
            self._post_critical("오류", msg)

            # ✅ Host start 요청이 걸려있으면 즉시 실패 응답(대기/타임아웃 방지)
            with contextlib.suppress(Exception):
                self._host_report_start(False, msg)

            # ✅ 자동 재연결 차단 + 워치독/점유 정리(RF-Pulse 점유 해제 포함)
            self._auto_connect_enabled = False
            with contextlib.suppress(Exception):
                await self._stop_device_watchdogs(light=False)

            # ✅ 예외로 비정상 종료 → error 표시(정상 종료만 idle)
            with contextlib.suppress(Exception):
                runtime_state.set_error("chamber", self.ch, msg)
                runtime_state.mark_finished("chamber", self.ch)

            self._start_next_process_from_queue(False)
            self._on_process_status_changed(False)

    def _kick_oes_init_background(self, *, force: bool = True) -> None:
        """Start 버튼을 눌렀을 때 OES init을 '백그라운드'로 미리 돌려둔다."""
        if not getattr(self, "oes", None):
            return

        t = getattr(self, "_oes_init_task", None)
        if t and not t.done():
            return

        async def _run():
            try:
                self.append_log(f"OES{self.ch}", "[OES] init (background) begin")
                self._ensure_background_started()

                ok = await self.oes.initialize_device(timeout_s=20.0, force=force)
                self._oes_initialized = bool(ok)

                if ok:
                    self.append_log(f"OES{self.ch}", "[OES] init (background) OK")
                else:
                    err = getattr(self.oes, "_init_error", None) or "unknown"
                    self.append_log(f"OES{self.ch}", f"[OES] init (background) FAIL: {err}")

            except Exception as e:
                self._oes_initialized = False
                self.append_log(f"OES{self.ch}", f"[OES] init (background) EXC: {type(e).__name__}: {e}")

        self._oes_init_task = self._spawn_detached(_run(), store=True, name=f"OES.init.{self.ch}")

    async def _wait_device_connected(self, dev: object, name: str, timeout_s: float) -> bool:
        """장비 연결 여부가 True가 될 때까지 대기(타임아웃).
        ⚠️ qasync(QEventLoop) 환경에서 loop.time()이 간헐적으로 정지/비정상 값을 반환하는 케이스가 있어,
        프리플라이트가 영원히 끝나지 않는 현상을 막기 위해 time.monotonic() 기반으로 구현한다.
        """
        t0 = time.monotonic()
        while True:
            if self._is_dev_connected(dev):
                return True

            if (time.monotonic() - t0) >= float(timeout_s):
                self.append_log(name, f"연결 확인 실패(타임아웃 {timeout_s:.1f}s)")
                return False

            await asyncio.sleep(0.2)

    async def _preflight_connect(self, params: Mapping[str, Any], timeout_s: float = 8.0) -> tuple[bool, list[str]]:
        need: list[tuple[str, object]] = [("PLC", self.plc), ("MFC", self.mfc), ("IG", self.ig)]

        use_dc_pulse = bool(params.get("use_dc_pulse", False))
        use_rf_pulse = bool(params.get("use_rf_pulse", False))

        if use_dc_pulse and self.dc_pulse:
            need.append(("DC-Pulse", self.dc_pulse))
        if use_rf_pulse and self.rf_pulse:
            need.append(("RF-Pulse", self.rf_pulse))

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
        ok = len(failed) == 0
    
        # ✅ 여기서만 Host로 성공/실패 신호를 보낸다
        if ok:
            self._host_report_start(True, "preflight OK")
        else:
            self._host_report_start(False, "장비 연결 실패: " + ", ".join(failed))

        return ok, failed
    
    async def _set_chuck_position_if_needed(self, params: Mapping[str, Any]) -> bool:
        """
        레시피에 chuck_position 값이 있으면(공란 제외) 공정 시작 전에 1회만 Chuck 위치를 조정.

        handlers.py 의 chuck_up/chuck_down 과 동일한 구조:
        - Z_M_P_{CH}_SW (Z-POWER) ON 유지
        - 방향 스위치(Z_M_P_{CH}_CW/MID/CCW_SW) ON 유지
        - Z{CH}_*_LOCATION 램프를 폴링해서 목표 위치 도달 여부 확인
        - 타임아웃/예외 시에도 스위치는 반드시 OFF
        """
        pos = str(params.get("chuck_position") or "").strip().lower()
        if not pos:
            # 공란이면 스킵
            return True

        ch = 1 if int(getattr(self, "ch", 1)) != 2 else 2

        # 허용 값 체크
        if pos not in ("up", "mid", "down"):
            self.append_log("PLC", f"[CH{self.ch}] 알 수 없는 chuck_position='{pos}' → 스킵")
            return True

        # POWER / 방향 스위치 / 위치 램프 매핑 (handlers.py와 동일한 구조)
        power_sw = f"Z_M_P_{ch}_SW"
        if pos == "up":
            move_sw = f"Z_M_P_{ch}_CW_SW"
            lamp_bit = f"Z{ch}_UP_LOCATION"
        elif pos == "mid":
            move_sw = f"Z_M_P_{ch}_MID_SW"
            lamp_bit = f"Z{ch}_MID_LOCATION"
        else:  # "down"
            move_sw = f"Z_M_P_{ch}_CCW_SW"
            lamp_bit = f"Z{ch}_DOWN_LOCATION"

        if not self.plc:
            self.append_log("PLC", f"[CH{self.ch}] PLC 미연결 상태 → Chuck 제어 불가")
            return False

        timeout_s = 60.0

        try:
            # (A) 이미 목표 위치인지 먼저 한 번 확인
            try:
                already = bool(await self.plc.read_bit(lamp_bit))
            except Exception:
                already = False

            if already:
                self.append_log(
                    "PLC",
                    f"[CH{self.ch}] Chuck '{pos}' 이미 목표 위치 ({lamp_bit}=True) → 이동 생략",
                )
                return True

            # (B) POWER ON → MOVE ON
            self.append_log(
                "PLC",
                f"[CH{self.ch}] Chuck '{pos}' 이동 시작: {power_sw} → {move_sw} → {lamp_bit} 폴링",
            )

            await self.plc.write_switch(power_sw, True)
            await asyncio.sleep(0.2)
            await self.plc.write_switch(move_sw, True)

            # (C) 램프 폴링 (최대 timeout_s)
            deadline = time.monotonic() + timeout_s
            while time.monotonic() < deadline:
                try:
                    ok = bool(await self.plc.read_bit(lamp_bit))
                except Exception:
                    ok = False

                if ok:
                    # 성공: 스위치 OFF
                    with contextlib.suppress(Exception):
                        await self.plc.write_switch(move_sw, False)
                        await self.plc.write_switch(power_sw, False)
                    self.append_log(
                        "PLC",
                        f"[CH{self.ch}] Chuck '{pos}' 이동 성공 ({lamp_bit}=True)",
                    )
                    return True

                await asyncio.sleep(0.3)

            # (D) 타임아웃: 스위치 OFF 후 실패 반환
            with contextlib.suppress(Exception):
                await self.plc.write_switch(move_sw, False)
                await self.plc.write_switch(power_sw, False)

            self.append_log(
                "PLC",
                f"[CH{self.ch}] Chuck '{pos}' 타임아웃({int(timeout_s)}s) — {lamp_bit}=False",
            )
            return False

        except Exception as e:
            # (E) 예외 시에도 스위치 OFF 보장
            with contextlib.suppress(Exception):
                try:
                    await self.plc.write_switch(move_sw, False)
                    await self.plc.write_switch(power_sw, False)
                except Exception:
                    pass

            self.append_log(
                "PLC",
                f"[CH{self.ch}] Chuck '{pos}' 이동 중 예외: {e!r}",
            )
            return False

    # ------------------------------------------------------------------
    # Start/Stop (개별 챔버)
    # ------------------------------------------------------------------
    def _handle_start_clicked(self, _checked: bool = False):
        """
        Start 버튼 / Host Start 요청 공통 진입점.
        ★ 어떤 이유로든 예외가 나더라도 조용히 죽지 않고,
        최소한 로그 + 알림창을 남기도록 전체를 보호한다.
        """
        try:
            # ✅ 전역 runtime_state 기준 60초 쿨다운
            remain = runtime_state.remaining_cooldown("chamber", self.ch, cooldown_s=60.0)
            if remain > 0.0:
                secs = int(remain + 0.999)
                self._host_report_start(False, f"cooldown {remain:.0f}s remaining")
                self._post_warning("대기 필요", f"이전 공정 종료 후 1분 대기 필요합니다.\n{secs}초 후에 시작하십시오.")
                return
            
            # ★ 장치 정리가 백그라운드에서 진행 중이면 대기 안내
            if getattr(self, "_pending_device_cleanup", False):
                # ✅ cleanup이 아직 끝나지 않았으면 Start는 막는 게 안전
                self._host_report_start(False, "previous run cleanup incomplete")
                self._post_warning(
                    "정리 미완료",
                    "이전 공정 장치 정리가 아직 끝나지 않았습니다.\n"
                    "잠시 후 다시 시도하세요.\n"
                    "오래 지속되면 프로그램 재시작 또는 정리 실패 장치(RGA 등) 상태를 확인하세요."
                )
                return
            
            # ★ 추가(권장): 이미 다음 공정이 예약되어 있으면 Start 재클릭은 무시하고 안내
            t = getattr(self, "_delay_main_task", None)
            if t is not None and not t.done():
                self._host_report_start(False, "main task delayed")
                self._post_warning("대기 중", "다음 공정이 예약되어 있습니다. 카운트다운 종료 후 자동 시작합니다.")
                return

            # ✅ 교차 실행 차단: 해당 챔버가 이미 다른 런타임(CH/PC/TSP)에서 점유 중이면 시작 금지
            if runtime_state.is_running("chamber", self.ch):
                self._host_report_start(False, "this chamber already running")
                self._post_warning("실행 오류", f"CH{self.ch}는 이미 다른 공정이 실행 중입니다.")
                return

            if self.process_controller.is_running:
                self._host_report_start(False, "process controller busy")
                self._post_warning("실행 오류", "다른 공정이 실행 중입니다.")
                return  
            
            # 재시도: 사용자가 Start를 누른 시점부터 자동 연결 허용
            self._auto_connect_enabled = True

            if getattr(self, "process_queue", None):
                # 파일은 'started' 이벤트에서 _open_run_log()로 한 번만 생성
                self.append_log("MAIN", f"[CH{self.ch}] 파일 기반 자동 공정 시작")
                self.current_process_index = -1
                self._start_next_process_from_queue(True)
                return

            vals = self._validate_single_run_inputs()
            if vals is None:
                self._host_report_start(False, "invalid inputs")
                return

            try:
                base_pressure = float(self._get_text("basePressure_edit") or 1e-5)
                working_pressure = float(self._get_text("workingPressure_edit") or 0.0)
                shutter_delay = float(self._get_text("shutterDelay_edit") or 0.0)
                process_time = float(self._get_text("processTime_edit") or 0.0)

                # ✅ (추가) UI 수동 공정에서 공정명(Process Name)을 입력받아 로그/카드에 반영
                # - integrationTime_edit 칸을 Process Name 입력칸으로 재활용
                # - 비어 있으면 기존과 동일하게 기본값 사용
                process_name = (self._get_text("integrationTime_edit") or '').strip()
                process_note = process_name if process_name else f"Single CH{self.ch}"
            except ValueError:
                self.append_log("UI", "오류: 값 입력란을 확인해주세요.")
                self._host_report_start(False, "invalid number input")  # ★ 추가
                return

            params: dict[str, Any] = {
                "base_pressure": base_pressure,
                "integration_time": 60,
                "working_pressure": working_pressure,
                "shutter_delay": shutter_delay,
                "process_time": process_time,
                "process_note": process_note,   # ✅ 공정명(사용자 입력)이 로그/구글챗/CSV에 반영됨
                "Process_name": process_note,   # ✅ 호환 위해 같이 유지
                **vals,

                # ✅ Start 버튼 "누른" 시각 (tz 없이, 초 단위)
                "t0_pressed_wall": datetime.now().isoformat(timespec="seconds"),
                "t0_pressed_ns":   time.monotonic_ns(),
            }

            # ✅ Start 클릭 즉시 로그 파일 생성 (프리플라이트 실패/입력오류도 파일에 남김)
            with contextlib.suppress(Exception):
                if not getattr(self, "_log_file_path", None):
                    self._open_run_log(params)

            errs = self._validate_norm_params(cast(NormParams, params))
            if errs:
                self._host_report_start(False, "; ".join(errs))
                self._post_warning("입력값 확인", "\n".join(f"- {e}" for e in errs))
                return  

            params["G1 Target"] = vals.get("G1_target_name", "")
            params["G2 Target"] = vals.get("G2_target_name", "")
            params["G3 Target"] = vals.get("G3_target_name", "")

            # ❌ 여기서는 running 마킹하지 않음 (공통 진입점 _safe_start_process에서 1회만 수행)
            self.append_log("MAIN", "입력 검증 통과 → 장비 연결 확인 시작")
            self._safe_start_process(cast(NormParams, params))
        except Exception as e:
            tb = "".join(traceback.format_exception(type(e), e, e.__traceback__)).rstrip()
            self.append_log("MAIN", f"_handle_start_clicked 예외 발생:\n{tb}")

            # ✅ 예외는 비정상 종료로 간주 → error latch + running 해제
            with contextlib.suppress(Exception):
                runtime_state.set_error("chamber", self.ch, f"exception: {e!r}")
                runtime_state.mark_finished("chamber", self.ch)

            # Host쪽에서도 실패 통보 받도록
            self._host_report_start(False, f"exception: {e!r}")

            try:
                self._post_critical(
                    "실행 오류",
                    "공정 시작 준비 중 내부 오류가 발생했습니다.\n"
                    "자세한 내용은 로그 파일을 확인해주세요.",
                )
            except Exception:
                pass


    def _handle_stop_clicked(self, _checked: bool = False):
        self.request_stop_all(user_initiated=True)

    def start_presputter_from_ui(self) -> bool:
        """
        Pre-Sputter 자동 실행 진입점.
        'Start' 버튼을 누른 것과 동일한 경로로, 현재 UI 값(기본값/마지막값)으로 1회 실행한다.
        """
        if self.is_running:
            self.append_log("MAIN", f"[CH{self.ch}] PreSputter: 이미 공정 중입니다.")
            return False
        try:
            # 버튼 클릭과 동일 경로(쿨다운·검증·프리플라이트·로깅 모두 재사용)
            self._handle_start_clicked(False)
            self.append_log("MAIN", f"[CH{self.ch}] PreSputter 자동 시작 (UI 현재값)")
            return True
        except Exception as e:
            self.append_log("MAIN", f"[CH{self.ch}] PreSputter 시작 실패: {e!r}")
            return False

    def request_stop_all(self, user_initiated: bool):
        self._cancel_delay_task()
        
        # ✅ 중복 STOP 가드
        if getattr(self, "_pc_stopping", False):
            self.append_log("MAIN", "정지 요청 무시: 이미 종료 절차 진행 중")
            return

        # ✅ 현재 런이 TEST 모드인지 판정
        is_test_mode = False
        try:
            is_test_mode = bool((getattr(self.process_controller, "current_params", {}) or {}).get("test_mode", False))
        except Exception:
            is_test_mode = False

        # Stop 이후엔 자동 재연결 차단(사용자가 Start로 다시 올릴 때까지)
        self._auto_connect_enabled = False
        self._run_select = None

        # 라이트 정리: 출력/폴링 OFF (통신/cleanup 없음)
        self._spawn_detached(self._stop_device_watchdogs(light=True))

        # ✅ [핵심] Preflight 중에는 ProcessController.is_running=False일 수 있다.
        #         기존 코드는 여기서 "실행 중 공정 없음"으로 return 해서,
        #         UI는 Stop이 켜져 있어도 실제 STOP/정리/상태복구가 전부 막혔다.
        if not getattr(self.process_controller, "is_running", False):
            self.append_log("MAIN", "STOP 요청: 공정 시작 전(Preflight/Idle) → 즉시 정리 시퀀스 수행")

            # Start 재진입 방지
            self._pc_stopping = True
            self._pending_device_cleanup = True

            async def _stop_preflight_or_idle():
                cancelled = False
                try:
                    # 1) StartAfterPreflight/DeviceStart 태스크가 살아 있으면 취소
                    for t in list(getattr(self, "_bg_tasks", []) or []):
                        if not t or t.done():
                            continue
                        try:
                            nm = t.get_name()
                        except Exception:
                            nm = ""
                        if nm.startswith(f"StartAfterPreflight.CH{self.ch}") or nm.startswith(f"DevStart.CH{self.ch}"):
                            with contextlib.suppress(Exception):
                                t.cancel()
                                cancelled = True

                    if cancelled:
                        self.append_log("MAIN", "정지 요청: Preflight/DeviceStart 태스크 취소")

                    # 2) RF 점유 해제(있다면)
                    with contextlib.suppress(Exception):
                        self._release_rf_pulse_reservation()

                    # 3) 컨트롤러 리셋(혹시 남아있는 상태 제거)
                    with contextlib.suppress(Exception):
                        self.process_controller.reset_controller()

                    # 4) heavy cleanup: bg task cancel + device cleanup (timeout 포함)
                    await self._stop_device_watchdogs(light=False)

                    # 5) runtime_state 정리 (preflight에서도 mark_started가 찍혀 있을 수 있음)
                    with contextlib.suppress(Exception):
                        if runtime_state.is_running("chamber", self.ch):
                            reason = "user cancelled during preflight" if cancelled else "user stop (idle)"
                            runtime_state.set_error("chamber", self.ch, reason)
                            runtime_state.mark_finished("chamber", self.ch)

                finally:
                    # 6) UI/로그 리셋
                    with contextlib.suppress(Exception):
                        self._clear_queue_and_reset_ui()
                    with contextlib.suppress(Exception):
                        self._set_state_text("대기 중")

            self._spawn_detached(_stop_preflight_or_idle(), store=True, name=f"StopImmediate.CH{self.ch}")
            return

        # ✅ 여기부터는 "진짜 공정 실행 중"인 경우(기존 STOP 시퀀스 유지)
        self._pc_stopping = True

        # ✅ TEST 모드면 장비 정리/폴백 자체를 타면 안 됨
        if is_test_mode:
            self._pending_device_cleanup = False
            self.append_log("MAIN", "[TEST MODE] STOP → 시뮬레이션(딜레이)만 취소, 장비 정리/폴백 스킵")
            self.process_controller.request_stop()
            return

        # ✅ REAL MODE: 기존 동작 유지
        self._pending_device_cleanup = True
        self.process_controller.request_stop()

        # ✅ 백업 타이머(고정 10분) - (기존 코드 그대로)
        self._stop_fallback_gen = int(getattr(self, "_stop_fallback_gen", 0)) + 1
        _gen = self._stop_fallback_gen

        timeout_s = 600.0
        self.append_log("MAIN", f"STOP fallback timer set: {timeout_s:.0f}s")

        async def _fallback():
            try:
                await asyncio.sleep(timeout_s)
                if _gen != int(getattr(self, "_stop_fallback_gen", 0)):
                    return
                if not (self._pc_stopping and self._pending_device_cleanup):
                    return

                self.append_log("MAIN", f"STOP fallback({timeout_s:.0f}s) → emergency shutdown")
                with contextlib.suppress(Exception):
                    self.process_controller.emergency_stop()

                grace_s = 25.0
                t0 = time.monotonic()
                while (time.monotonic() - t0) < grace_s:
                    if not self.process_controller.is_running:
                        # ✅ Preflight 중이면 StartAfterPreflight 태스크를 찾아 취소
                        cancelled = False
                        for t in list(getattr(self, "_bg_tasks", []) or []):
                            if not t or t.done():
                                continue
                            try:
                                nm = t.get_name()
                            except Exception:
                                nm = ""
                            if nm.startswith(f"StartAfterPreflight.CH{self.ch}"):
                                t.cancel()
                                cancelled = True

                        if cancelled:
                            self.append_log("MAIN", "정지 요청: Preflight 취소")
                            self._auto_connect_enabled = False
                            self._run_select = None

                            # RF 점유 중이면 해제
                            with contextlib.suppress(Exception):
                                self._release_rf_pulse_reservation()

                            with contextlib.suppress(Exception):
                                runtime_state.set_error("chamber", self.ch, "user cancelled during preflight")
                                runtime_state.mark_finished("chamber", self.ch)

                            self._on_process_status_changed(False)
                            self._set_state_text("대기 중")
                            return

                        self.append_log("MAIN", "정지 요청 무시: 실행 중 공정 없음(이미 종료됨)")
                        return
                    await asyncio.sleep(0.5)

                self.append_log("MAIN", "STOP fallback → heavy cleanup + controller reset")

                with contextlib.suppress(Exception):
                    self.process_controller.reset_controller()

                await self._stop_device_watchdogs(light=False)

                with contextlib.suppress(Exception):
                    runtime_state.mark_finished("chamber", self.ch)

                self._pending_device_cleanup = False
                self._pc_stopping = False
                self._clear_queue_and_reset_ui()

            except asyncio.CancelledError:
                pass

        self._spawn_detached(_fallback(), store=True, name=f"StopFallback.CH{self.ch}")

    async def _stop_device_watchdogs(self, *, light: bool = False) -> None:
        if light:
            with contextlib.suppress(Exception): self.mfc.set_process_status(False)
            if self.dc_pulse:
                with contextlib.suppress(Exception): self.dc_pulse.set_process_status(False)
            if self.rf_pulse:
                with contextlib.suppress(Exception): self.rf_pulse.set_process_status(False)
            if self.dc_power and hasattr(self.dc_power, "set_process_status"):
                with contextlib.suppress(Exception): self.dc_power.set_process_status(False)
            if self.rf_power and hasattr(self.rf_power, "set_process_status"):
                with contextlib.suppress(Exception): self.rf_power.set_process_status(False)
            return

        # ✅ 이번 cleanup이 “완전히 끝났는지” 표시 (Start 재진입 안전장치)
        self._cleanup_timed_out = False

        pending_cleanup_names: list[str] = []

        # ✅ heavy 시작 직후도 한 번 더 OFF
        with contextlib.suppress(Exception):
            if self.mfc and hasattr(self.mfc, "on_process_finished"):
                self.mfc.on_process_finished(False)
            elif self.mfc and hasattr(self.mfc, "set_process_status"):
                self.mfc.set_process_status(False)

        if self.dc_pulse:
            with contextlib.suppress(Exception): self.dc_pulse.set_process_status(False)
        if self.rf_pulse:
            with contextlib.suppress(Exception): self.rf_pulse.set_process_status(False)
        if self.dc_power and hasattr(self.dc_power, "set_process_status"):
            with contextlib.suppress(Exception): self.dc_power.set_process_status(False)
        if self.rf_power and hasattr(self.rf_power, "set_process_status"):
            with contextlib.suppress(Exception): self.rf_power.set_process_status(False)

        loop = self._loop_from_anywhere()

        # 0) bg task cancel (timeout)
        try:
            current = asyncio.current_task()
            live = [t for t in getattr(self, "_bg_tasks", []) if t and not t.done() and t is not current]
            for t in live:
                with contextlib.suppress(Exception):
                    t.cancel()

            if live:
                try:
                    await asyncio.wait_for(asyncio.gather(*live, return_exceptions=True), timeout=5.0)
                except asyncio.TimeoutError:
                    self._cleanup_timed_out = True
                    with contextlib.suppress(Exception):
                        names = [getattr(t, "get_name", lambda: repr(t))() for t in live]
                    self.append_log("MAIN", f"⚠ bg task cancel timeout: {names!r}")
        finally:
            self._bg_tasks = []

        # 1) IG cancel (timeout 유지)
        try:
            if self.ig and hasattr(self.ig, "cancel_wait"):
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.ig.cancel_wait(), timeout=2.0)
        except Exception:
            pass

        # 2) device cleanup (timeout)
        # 2-A) ✅ OES는 stop 요청만 던지고 기다리지 않는다.
        #      - one-shot 모드: 다음 공정에서 init을 다시 타도록 캐시 리셋
        #      - daemon 모드: OES daemon을 계속 유지하므로 캐시 유지
        if self.oes:
            with contextlib.suppress(Exception):
                if hasattr(self.oes, "stop_measurement"):
                    self._spawn_detached(
                        self.oes.stop_measurement(wait=False),
                        store=False,
                        name=f"Cleanup.OESAsync.CH{self.ch}.DETACHED",
                    )
                else:
                    self._spawn_detached(
                        self.oes.cleanup(),
                        store=False,
                        name=f"Cleanup.OESAsync.CH{self.ch}.DETACHED",
                    )

            # ✅ 핵심: daemon 모드에서는 init 캐시를 깨지 않는다(상주 유지)
            if not bool(getattr(self.oes, "_daemon_enabled", False)):
                self._oes_initialized = False

        # 2-B) device cleanup (timeout) - ✅ OES는 제외하고 나머지만 기다림
        cleanup_tasks: list[asyncio.Task] = []
        for dev in (self.ig, self.mfc, self.dc_pulse, self.rf_pulse, self.dc_power, self.rf_power, self.rga):
            if dev and hasattr(dev, "cleanup"):
                try:
                    coro = dev.cleanup()
                except Exception:
                    continue
                try:
                    nm = getattr(dev, "NAME", None) or dev.__class__.__name__
                    cleanup_tasks.append(loop.create_task(coro, name=f"Cleanup.{nm}.CH{self.ch}"))
                except Exception:
                    cleanup_tasks.append(loop.create_task(coro))

        if cleanup_tasks:
            cleanup_timeout_s = 15.0  # ✅ OES 때문에 75초로 늘리지 않음
            done, pending = await asyncio.wait(cleanup_tasks, timeout=cleanup_timeout_s)
            if pending:
                self._cleanup_timed_out = True

                pn: list[str] = []
                with contextlib.suppress(Exception):
                    pn = [t.get_name() for t in pending]
                if not pn:
                    pn = [repr(t) for t in pending]
                pending_cleanup_names = list(pn)

                self.append_log("MAIN", f"⚠ device cleanup timeout: {pending_cleanup_names!r}")

                for t in pending:
                    with contextlib.suppress(Exception):
                        t.cancel()

                # ✅ cancel 했는데도 안 죽는 cleanup이 있으면 여기서 무한 대기 가능 → 2초로 끊음(기존 유지)
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*pending, return_exceptions=True),
                        timeout=2.0
                    )
                except asyncio.TimeoutError:
                    self._cleanup_timed_out = True
                    with contextlib.suppress(Exception):
                        pn2 = [getattr(t, "get_name", lambda: repr(t))() for t in pending]
                    self.append_log("MAIN", f"⚠ cleanup cancel timeout(2s): {pn2!r} (detached/leaked)")

        # 3) footer 먼저
        with contextlib.suppress(Exception):
            self._close_run_log()

        # 4) writer 완전 종료 (timeout)
        try:
            t = loop.create_task(self._shutdown_log_writer(), name=f"ShutdownLogWriter.CH{self.ch}")
            try:
                await asyncio.wait_for(t, timeout=6.0)
            except asyncio.TimeoutError:
                self._cleanup_timed_out = True
                self.append_log("MAIN", "⚠ log writer shutdown timeout")
                with contextlib.suppress(Exception):
                    t.cancel()
                    try:
                        await asyncio.wait_for(asyncio.gather(t, return_exceptions=True), timeout=2.0)
                    except asyncio.TimeoutError:
                        self._cleanup_timed_out = True
                        self.append_log("MAIN", "⚠ log writer cancel timeout(2s): leaked")
        except Exception:
            self._cleanup_timed_out = True
            with contextlib.suppress(Exception):
                self.append_log("MAIN", "⚠ log writer shutdown exception")

        # 5) 파일 경로/버퍼 초기화
        self._log_file_path = None
        with contextlib.suppress(Exception):
            self._prestart_buf.clear()

        self._bg_started = False
        self._devices_started = False
        self._run_select = None

        # ✅ RF-Pulse 전역 점유 해제(정리된 경우에만)
        if getattr(self, "_rf_pulse_reserved", False):
            # cleanup timeout 목록에 RFPulse가 남아있으면 점유 유지(동시 사용 방지)
            rfpulse_pending = any("RFPulse" in n for n in (pending_cleanup_names or []))
            if not rfpulse_pending:
                self._release_rf_pulse_reservation()
            else:
                self.append_log("MAIN", "⚠ RF-Pulse cleanup 미완료 → 점유 유지(동시 사용 방지)")

        # ✅ cleanup이 완전히 끝난 경우에만 “정리 완료”로 간주
        if not getattr(self, "_cleanup_timed_out", False):
            self._pending_device_cleanup = False
            self._pc_stopping = False
        else:
            self._pending_device_cleanup = True
            self.append_log("MAIN", "⚠ cleanup 미완료(타임아웃) → Start는 정리 완료 전까지 제한")

    def shutdown_fast(self) -> None:
        async def run():
            self._cancel_delay_task()
            try:
                if self.ig and hasattr(self.ig, "cancel_wait"):
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(self.ig.cancel_wait(), timeout=1.0)
            except Exception:
                pass

            loop = asyncio.get_running_loop()
            current = asyncio.current_task()
            live = [t for t in getattr(self, "_bg_tasks", []) if t and not t.done() and t is not current]
            for t in live: loop.call_soon(t.cancel)
            if live: await asyncio.gather(*live, return_exceptions=True)
            self._bg_tasks = []
            self._bg_started = False
            self._devices_started = False

            tasks = []
            for dev in (self.ig, self.mfc, self.dc_pulse, self.rf_pulse, self.dc_power, self.rf_power, self.oes, self.rga):
                if not dev: continue
                try:
                    if hasattr(dev, "cleanup_quick"):
                        tasks.append(dev.cleanup_quick())
                    elif hasattr(dev, "cleanup"):
                        tasks.append(dev.cleanup())
                except Exception:
                    pass
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            # 1) footer 먼저 (파일이 열려 있으면 "# ==== END ====" 남김)
            with contextlib.suppress(Exception):
                self._close_run_log()

            # 2) writer 완전 종료 + 큐 리셋
            with contextlib.suppress(Exception):
                await self._shutdown_log_writer()

            # 3) 파일 경로/버퍼 초기화 (다음 런은 새 파일명으로 시작)
            self._log_file_path = None
            with contextlib.suppress(Exception):
                self._prestart_buf.clear()

        self._spawn_detached(run())

    # ------------------------------------------------------------------
    # 입력 검증 / 정규화 / delay 처리
    def _get_text(self, leaf: str) -> str:
        w = self._u(leaf)
        return w.toPlainText().strip() if w else ""

    def _validate_single_run_inputs(self) -> dict[str, Any] | None:
        if self.ch == 1:
            use_ar = bool(getattr(self._u("Ar_checkbox"), "isChecked", lambda: False)())
            use_o2 = bool(getattr(self._u("O2_checkbox"), "isChecked", lambda: False)())
            use_n2 = bool(getattr(self._u("N2_checkbox"), "isChecked", lambda: False)())
            if not (use_ar or use_o2 or use_n2):
                self._post_warning("선택 오류", "가스를 하나 이상 선택해야 합니다.")
                return None

            def _read_flow(name: str) -> float:
                txt = self._get_text(name) or "0"
                v = float(txt)
                if v < 0:
                    raise ValueError()
                return v

            try:
                ar_flow = _read_flow("arFlow_edit") if use_ar else 0.0
                o2_flow = _read_flow("o2Flow_edit") if use_o2 else 0.0
                n2_flow = _read_flow("n2Flow_edit") if use_n2 else 0.0
            except Exception:
                self._post_warning("입력값 확인", "가스 유량 입력을 확인하세요.")
                return None

            # ✅ CH1에서 RF-Pulse 임시 사용: 지원 여부(supports) 반영
            use_rf_pulse = self.supports_rf_pulse and bool(getattr(self._u("rfPulsePower_checkbox"), "isChecked", lambda: False)())
            use_dc_pulse = self.supports_dc_pulse and bool(getattr(self._u("dcPulsePower_checkbox"), "isChecked", lambda: False)())

            # 둘 다 선택 금지
            if use_rf_pulse and use_dc_pulse:
                self._post_warning("선택 오류", "RF-Pulse와 DC-Pulse는 동시에 선택할 수 없습니다.")
                return None

            # 최소 1개 선택
            if not (use_rf_pulse or use_dc_pulse):
                if self.supports_rf_pulse:
                    self._post_warning("선택 오류", "CH1은 RF-Pulse(또는 DC-Pulse) 중 하나를 선택해야 합니다.")
                elif self.supports_dc_pulse:
                    self._post_warning("선택 오류", "CH1은 DC-Pulse를 선택해야 합니다.")
                else:
                    self._post_warning("선택 오류", "CH1에서 사용 가능한 파워 장치가 없습니다.")
                return None

            # 기본값
            dc_pulse_power = 0.0; dc_pulse_freq = None; dc_pulse_duty = None
            rf_pulse_power = 0.0; rf_pulse_freq = None; rf_pulse_duty = None

            if use_dc_pulse:
                # ---- 기존 DC-Pulse 검증 유지 ----
                try:
                    dc_pulse_power = float(self._get_text("dcPulsePower_edit") or "0")
                    if dc_pulse_power <= 0:
                        raise ValueError()
                except ValueError:
                    self._post_warning("입력값 확인", "DC-Pulse Target Power(W)를 확인하세요.")
                    return None

                txtf = self._get_text("dcPulseFreq_edit")
                if txtf:
                    try:
                        dc_pulse_freq = int(float(txtf))  # kHz
                        if dc_pulse_freq < 20 or dc_pulse_freq > 150:
                            raise ValueError()
                    except ValueError:
                        self._post_warning("입력값 확인", "DC-Pulse Freq(kHz)는 20..150 범위입니다.")
                        return None

                txtd = self._get_text("dcPulseDutyCycle_edit")
                if txtd:
                    try:
                        dc_pulse_duty = int(float(txtd))
                        if dc_pulse_duty < 1 or dc_pulse_duty > 99:
                            raise ValueError()
                    except ValueError:
                        self._post_warning("입력값 확인", "DC-Pulse Duty(%)는 1..99 범위")
                        return None

            if use_rf_pulse:
                # ---- CH2에서 쓰는 RF-Pulse 검증 로직을 CH1에도 동일 적용 ----
                try:
                    rf_pulse_power = float(self._get_text("dcPulsePower_edit") or "0")
                    if rf_pulse_power <= 0:
                        raise ValueError()
                except ValueError:
                    self._post_warning("입력값 확인", "RF Pulse Target Power(W)를 확인하세요.")
                    return None

                txtf = self._get_text("dcPulseFreq_edit")
                if txtf:
                    try:
                        rf_pulse_freq = int(float(txtf))  # kHz
                        if rf_pulse_freq < 1 or rf_pulse_freq > 100:
                            raise ValueError()
                    except ValueError:
                        self._post_warning("입력값 확인", "RF Pulse Freq(kHz)는 1..100 범위입니다.")
                        return None

                txtd = self._get_text("dcPulseDutyCycle_edit")
                if txtd:
                    try:
                        rf_pulse_duty = int(float(txtd))
                        if rf_pulse_duty < 1 or rf_pulse_duty > 99:
                            raise ValueError()
                    except ValueError:
                        self._post_warning("입력값 확인", "RF Pulse Duty(%) 1..99")
                        return None

            g1n = self._get_text("g1Target_name")
            g2n = self._get_text("g2Target_name")
            g3n = self._get_text("g3Target_name")

            return {
                "use_ms": bool(getattr(self._u("mainShutter_checkbox"), "isChecked", lambda: False)()),
                "use_g1": False, "use_g2": False, "use_g3": False,
                "use_ar": use_ar, "use_o2": use_o2, "use_n2": use_n2,
                "ar_flow": ar_flow, "o2_flow": o2_flow, "n2_flow": n2_flow,

                # CH1 단일공정에서는 연속파는 일단 미사용(필요하면 확장)
                "use_rf_power": False, "use_dc_power": False,

                "use_dc_pulse": use_dc_pulse,
                "dc_pulse_power": dc_pulse_power,
                "dc_pulse_freq": dc_pulse_freq,
                "dc_pulse_duty": dc_pulse_duty,

                "use_rf_pulse": use_rf_pulse,
                "rf_pulse_power": rf_pulse_power,
                "rf_pulse_freq": rf_pulse_freq,
                "rf_pulse_duty": rf_pulse_duty,

                "G1_target_name": g1n, "G2_target_name": g2n, "G3_target_name": g3n,
                "use_power_select": bool(getattr(self._u("powerSelect_checkbox"), "isChecked", lambda: False)()),
            }

        elif self.ch == 2:
            # -----------------------------
            # ✅ CH2 단일 실행(UI) 검증/파라미터 생성
            # -----------------------------
            use_ar = bool(getattr(self._u("Ar_checkbox"), "isChecked", lambda: False)())
            use_o2 = bool(getattr(self._u("O2_checkbox"), "isChecked", lambda: False)())
            use_n2 = bool(getattr(self._u("N2_checkbox"), "isChecked", lambda: False)())

            if not (use_ar or use_o2 or use_n2):
                self._post_warning("선택 오류", "가스를 하나 이상 선택해야 합니다.")
                return None

            def _read_flow(name: str) -> float:
                txt = self._get_text(name) or "0"
                v = float(txt)
                if v < 0:
                    raise ValueError()
                return v

            try:
                ar_flow = _read_flow("arFlow_edit") if use_ar else 0.0
                o2_flow = _read_flow("o2Flow_edit") if use_o2 else 0.0
                n2_flow = _read_flow("n2Flow_edit") if use_n2 else 0.0
            except Exception:
                self._post_warning("입력값 확인", "가스 유량 입력을 확인하세요.")
                return None

            # ✅ CH2: 연속 파워(DC/RF)
            use_dc_power = bool(getattr(self._u("dcPower_checkbox"), "isChecked", lambda: False)())
            use_rf_power = bool(getattr(self._u("rfPower_checkbox"), "isChecked", lambda: False)())

            dc_power = 0.0
            rf_power = 0.0

            if use_dc_power:
                try:
                    dc_power = float(self._get_text("dcPower_edit") or "0")
                    if dc_power <= 0:
                        raise ValueError()
                except Exception:
                    self._post_warning("입력값 확인", "DC Power(W)를 확인하세요.")
                    return None

            if use_rf_power:
                try:
                    rf_power = float(self._get_text("rfPower_edit") or "0")
                    if rf_power <= 0:
                        raise ValueError()
                except Exception:
                    self._post_warning("입력값 확인", "RF Power(W)를 확인하세요.")
                    return None

            # ✅ CH2: Pulse 선택(DC-Pulse / RF-Pulse)
            use_dc_pulse = self.supports_dc_pulse and bool(getattr(self._u("dcPulsePower_checkbox"), "isChecked", lambda: False)())
            use_rf_pulse = self.supports_rf_pulse and bool(getattr(self._u("rfPulsePower_checkbox"), "isChecked", lambda: False)())

            # 둘 다 선택 금지(장비/포트 충돌 방지)
            if use_dc_pulse and use_rf_pulse:
                self._post_warning("선택 오류", "DC-Pulse와 RF-Pulse는 동시에 선택할 수 없습니다.")
                return None

            # 기본값
            dc_pulse_power = 0.0
            dc_pulse_freq = None
            dc_pulse_duty = None

            rf_pulse_power = 0.0
            rf_pulse_freq = None
            rf_pulse_duty = None

            if use_dc_pulse:
                # ---- 기존 DC-Pulse 검증 유지 ----
                try:
                    dc_pulse_power = float(self._get_text("dcPulsePower_edit") or "0")
                    if dc_pulse_power <= 0:
                        raise ValueError()
                except Exception:
                    self._post_warning("입력값 확인", "DC-Pulse Target Power(W)를 확인하세요.")
                    return None

                txtf = self._get_text("dcPulseFreq_edit")
                if txtf:
                    try:
                        dc_pulse_freq = int(float(txtf))
                        if dc_pulse_freq < 20 or dc_pulse_freq > 150:
                            raise ValueError()
                    except Exception:
                        self._post_warning("입력값 확인", "DC-Pulse Freq(kHz)는 20..150 범위입니다.")
                        return None

                txtd = self._get_text("dcPulseDutyCycle_edit")
                if txtd:
                    try:
                        dc_pulse_duty = int(float(txtd))
                        if dc_pulse_duty < 1 or dc_pulse_duty > 99:
                            raise ValueError()
                    except Exception:
                        self._post_warning("입력값 확인", "DC-Pulse Duty(%)는 1..99 범위")
                        return None

            if use_rf_pulse:
                # ---- RF-Pulse 검증 ----
                try:
                    rf_pulse_power = float(self._get_text("rfPulsePower_edit") or "0")
                    if rf_pulse_power <= 0:
                        raise ValueError()
                except Exception:
                    self._post_warning("입력값 확인", "RF-Pulse Target Power(W)를 확인하세요.")
                    return None

                txtf = self._get_text("rfPulseFreq_edit")
                if txtf:
                    try:
                        rf_pulse_freq = int(float(txtf))  # kHz
                        if rf_pulse_freq < 1 or rf_pulse_freq > 100:
                            raise ValueError()
                    except Exception:
                        self._post_warning("입력값 확인", "RF-Pulse Freq(kHz)는 1..100 범위입니다.")
                        return None

                txtd = self._get_text("rfPulseDutyCycle_edit")
                if txtd:
                    try:
                        rf_pulse_duty = int(float(txtd))
                        if rf_pulse_duty < 1 or rf_pulse_duty > 99:
                            raise ValueError()
                    except Exception:
                        self._post_warning("입력값 확인", "RF-Pulse Duty(%)는 1..99 범위입니다.")
                        return None

            # ✅ 최소 1개 파워 동작 선택 확인(정책)
            if not (use_dc_power or use_rf_power or use_dc_pulse or use_rf_pulse):
                self._post_warning("선택 오류", "DC Power / RF Power / DC-Pulse / RF-Pulse 중 하나 이상 선택해야 합니다.")
                return None

            g1n = self._get_text("g1Target_name")
            g2n = self._get_text("g2Target_name")
            g3n = self._get_text("g3Target_name")

            return {
                "use_ms": bool(getattr(self._u("mainShutter_checkbox"), "isChecked", lambda: False)()),
                "use_g1": bool(getattr(self._u("G1_checkbox"), "isChecked", lambda: False)()),
                "use_g2": bool(getattr(self._u("G2_checkbox"), "isChecked", lambda: False)()),
                "use_g3": bool(getattr(self._u("G3_checkbox"), "isChecked", lambda: False)()),
                "use_ar": use_ar, "use_o2": use_o2, "use_n2": use_n2,
                "ar_flow": ar_flow, "o2_flow": o2_flow, "n2_flow": n2_flow,

                "use_dc_power": use_dc_power,
                "dc_power": dc_power,
                "use_rf_power": use_rf_power,
                "rf_power": rf_power,

                "use_dc_pulse": use_dc_pulse,
                "dc_pulse_power": dc_pulse_power,
                "dc_pulse_freq": dc_pulse_freq,
                "dc_pulse_duty": dc_pulse_duty,

                # CH2도 RF-Pulse 사용 가능
                "use_rf_pulse": use_rf_pulse,
                "rf_pulse_power": rf_pulse_power,
                "rf_pulse_freq": rf_pulse_freq,
                "rf_pulse_duty": rf_pulse_duty,

                # ✅ P.W select는 Start를 막지 않음: 값만 전달
                "use_power_select": bool(getattr(self._u("powerSelect_checkbox"), "isChecked", lambda: False)()),

                "G1_target_name": g1n, "G2_target_name": g2n, "G3_target_name": g3n,
            }

        # (방어) 혹시 모르는 값
        return None

    def _normalize_params_for_process(self, raw: RawParams) -> NormParams:
        def tf(v): return str(v).strip().upper() in ("T","TRUE","1","Y","YES")
        def fget(key, default="0"):
            try: return float(str(raw.get(key, default)).strip())
            except Exception: return float(default)
        def iget(key, default="0"):
            try: return int(float(str(raw.get(key, default)).strip()))
            except Exception: return int(default)
        def iget_opt(key):
            s = str(raw.get(key, '')).strip()
            return int(float(s)) if s != '' else None

        g1t = str(raw.get("G1 Target", "")).strip()
        g2t = str(raw.get("G2 Target", "")).strip()
        g3t = str(raw.get("G3 Target", "")).strip()

        # ▼ 추가: chuck_position(up/mid/down, 공란이면 스킵)
        _pos = str(raw.get("chuck_position", "")).strip().lower()
        if _pos not in ("up", "mid", "down"):
            _pos = ""

        def _opt_int_from_keys(*keys):
            for k in keys:
                s = str(raw.get(k, "")).strip()
                if s != "":
                    try:
                        return int(float(s))
                    except Exception:
                        return None
            return None

        def _float_from_keys(default: str, *keys):
            for k in keys:
                s = str(raw.get(k, "")).strip()
                if s != "":
                    try:
                        return float(s)
                    except Exception:
                        break
            return float(default)

        if self.ch == 1:
            # ✅ CH1: RF/DC Pulse 둘 다 CSV에서 독립적으로 사용 (두 컬럼 유지)
            use_dc_pulse = tf(raw.get("use_dc_pulse", "F"))
            dc_pulse_power = _float_from_keys("0", "dc_pulse_power")
            dc_pulse_freq  = _opt_int_from_keys("dc_pulse_freq")
            dc_pulse_duty  = _opt_int_from_keys("dc_pulse_duty_cycle")

            use_rf_pulse = tf(raw.get("use_rf_pulse", "F"))
            rf_pulse_power = _float_from_keys("0", "rf_pulse_power")
            rf_pulse_freq  = _opt_int_from_keys("rf_pulse_freq")
            rf_pulse_duty  = _opt_int_from_keys("rf_pulse_duty_cycle")

        else:
            # ✅ CH2: 기존 키 그대로 사용
            use_dc_pulse = tf(raw.get("use_dc_pulse", "F"))
            dc_pulse_power = fget("dc_pulse_power", "0")
            dc_pulse_freq  = iget_opt("dc_pulse_freq")
            dc_pulse_duty  = iget_opt("dc_pulse_duty_cycle")

            use_rf_pulse = tf(raw.get("use_rf_pulse", "F"))
            rf_pulse_power = fget("rf_pulse_power", "0")
            rf_pulse_freq  = iget_opt("rf_pulse_freq")
            rf_pulse_duty  = iget_opt("rf_pulse_duty_cycle")

        res: NormParams = {
            "base_pressure":     fget("base_pressure", "1e-5"),
            "working_pressure":  fget("working_pressure", "0"),
            "process_time":      fget("process_time", "0"),
            "shutter_delay":     fget("shutter_delay", "0"),
            "integration_time":  iget("integration_time", "60"),
            "dc_power":          fget("dc_power", "0"),
            "rf_power":          fget("rf_power", "0"),

            "use_dc_pulse":      use_dc_pulse,
            "dc_pulse_power":    dc_pulse_power,
            "dc_pulse_freq":     dc_pulse_freq,
            "dc_pulse_duty":     dc_pulse_duty,

            "use_rf_pulse":      use_rf_pulse,
            "rf_pulse_power":    rf_pulse_power,
            "rf_pulse_freq":     rf_pulse_freq,
            "rf_pulse_duty":     rf_pulse_duty,

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
            "G1_target_name":    g1t, "G2_target_name": g2t, "G3_target_name": g3t,
            "G1 Target":         g1t, "G2 Target": g2t, "G3 Target": g3t,
            "use_power_select":  tf(raw.get("power_select", "F")),

            # ★ 추가
            "chuck_position":    _pos,
        }

        # ✅ (옵션) DC Pulse 중간 power 변경용 컬럼 전달
        # - 비어있으면 ProcessController가 기존처럼 무시함
        res["power_change_time"] = str(raw.get("power_change_time", "") or "").strip()
        res["change_power_value"] = str(raw.get("change_power_value", "") or "").strip()

        # ------------------------------------------------------------
        # ✅ Pulse 파라미터 정규화
        #  - 체크(use_*) 뿐 아니라 값(power/freq/duty)로도 "요청"을 판단
        #  - CH1/CH2 모두 RF/DC Pulse 사용 가능
        #  - 단, RF-Pulse는 전역 점유(락)로 CH1/CH2 동시 사용 금지
        # ------------------------------------------------------------
        def _pos(v) -> bool:
            try:
                return float(v) > 0.0
            except Exception:
                return False

        rf_requested = (
            bool(res.get("use_rf_pulse"))
            or _pos(res.get("rf_pulse_power", 0.0))
            or (res.get("rf_pulse_freq") is not None)
            or (res.get("rf_pulse_duty") is not None)
        )

        dc_requested = (
            bool(res.get("use_dc_pulse"))
            or _pos(res.get("dc_pulse_power", 0.0))
            or (res.get("dc_pulse_freq") is not None)
            or (res.get("dc_pulse_duty") is not None)
        )

        if self.ch == 1:
            # ✅ CH1: RF/DC Pulse 둘 다 허용 (동시 선택은 validate에서 차단)
            #    - 체크(use_*) 뿐 아니라 값(power/freq/duty)로도 "요청"을 판단
            #    - 둘 다 요청이면 그대로 두고 validate에서 막는다.
            if rf_requested:
                res["use_rf_pulse"] = True
            if dc_requested:
                res["use_dc_pulse"] = True

            # ✅ (선택) Pulse와 연속전력 동시 요청 방지: Pulse가 켜지면 연속 전력은 끔
            if rf_requested or dc_requested:
                res["use_rf_power"] = False
                res["rf_power"] = 0.0
                res["use_dc_power"] = False
                res["dc_power"] = 0.0

        elif self.ch == 2:
            # ✅ CH2: 값(power/freq/duty)이 들어오면 "요청"으로 간주해 use_rf_pulse를 True로 정규화
            if rf_requested:
                if self.supports_rf_pulse:
                    res["use_rf_pulse"] = True
                else:
                    self.append_log("Params", "CH2: RF-Pulse 미지원 설정 → RF-Pulse 요청을 무시하고 OFF 처리합니다.")
                    res["use_rf_pulse"] = False
                    res["rf_pulse_power"] = 0.0
                    res["rf_pulse_freq"] = None
                    res["rf_pulse_duty"] = None

        # 🔒 CH1은 N2 라인이 없으므로 강제 무시
        if self.ch == 1:
            if res.get("use_n2") or (res.get("n2_flow", 0.0) or 0.0) > 0.0:
                self.append_log("Params", "CH1은 N2 미지원 → N2 설정을 무시합니다.")
            res["use_n2"] = False
            res["n2_flow"] = 0.0

        return res

    # --- delay 단계 ---
    def _cancel_delay_task(self):
        # 메인/카운트다운 모두 취소
        for name in ("_delay_main_task", "_delay_countdown_task"):
            t = getattr(self, name, None)
            if t and not t.done():
                t.cancel()
            setattr(self, name, None)

    def _on_delay_step_done(self, step_name: str):
        self._delay_countdown_task = None
        self._last_state_text = None
        self.append_log("Process", f"'{step_name}' 지연 완료 → 다음 공정")

        # ✉ delay 완료 챗 알림
        if self.chat:
            try:
                total = len(getattr(self, "process_queue", []) or [])
                cur   = int(getattr(self, "current_process_index", -1)) + 1
                idx_txt = f" ({cur}/{total})" if total > 0 and cur > 0 else ""
                msg = f"✅ CH{self.ch} delay 단계 완료{idx_txt}: '{step_name}'"

                ret = self.chat.notify_text(msg)
                if inspect.iscoroutine(ret):
                    self._spawn_detached(ret, name=f"Chat.DelayDone.CH{self.ch}")
                if hasattr(self.chat, "flush"):
                    self.chat.flush()
            except Exception as e:
                self.append_log("CHAT", f"delay 완료 알림 실패: {e!r}")

        self._start_next_process_from_queue(True)

    async def _delay_sleep_then_continue(self, name: str, sec: float):
        try:
            await asyncio.sleep(sec)
            self._on_delay_step_done(name)
        except asyncio.CancelledError:
            pass

    async def _delay_countdown_then_continue(self, step_name: str, sec: float, amount: int, unit_txt: str):
        """
        지연(delay) 단계 동안 상태창에 카운트다운을 표시하고,
        완료되면 다음 공정으로 이어간다. Stop 등으로 취소되면 즉시 종료.
        """
        def _fmt_hms(x: float) -> str:
            if x < 0:
                x = 0
            s = int(x)
            h, m = divmod(s, 3600)
            m, s = divmod(m, 60)
            return f"{h:02d}:{m:02d}:{s:02d}"

        try:
            remain = int(sec)
            # 최초 1회 출력은 호출부에서 이미 했지만, 안전하게 한 번 더 보정 가능
            if self._w_state:
                self._w_state.setPlainText(f"지연 대기 중: {amount}{unit_txt} · 남은 시간 {_fmt_hms(remain)}")

            # 1초 단위 감소, 1분 초과 구간은 5초마다 갱신하여 부하 감소
            while remain > 0:
                await asyncio.sleep(1)
                remain -= 1
                if remain <= 60 or (remain % 5 == 0):
                    if self._w_state:
                        self._w_state.setPlainText(f"지연 대기 중: {amount}{unit_txt} · 남은 시간 {_fmt_hms(remain)}")

            # 지연 완료 → 다음 공정
            self._on_delay_step_done(step_name)

        except asyncio.CancelledError:
            # Stop/Abort 등으로 취소된 경우
            if self._w_state:
                self._w_state.setPlainText("지연 대기 취소됨")
            # 상위에서 _cancel_delay_task()로 핸들 정리됨
            pass

    def _try_handle_delay_step(self, params: Mapping[str, Any]) -> bool:
        name = str(params.get("Process_name") or params.get("process_note", "")).strip()
        if not name: 
            return False
        m = re.match(r"^\s*delay\s*(\d+)\s*([smhd]?)\s*$", name, re.IGNORECASE)
        if not m: 
            return False

        amount = int(m.group(1))
        unit = (m.group(2) or "m").lower()
        factor = {"s":1.0, "m":60.0, "h":3600.0, "d":86400.0}[unit]
        duration_s = amount * factor
        unit_txt = {"s":"초","m":"분","h":"시간","d":"일"}[unit]

        self.append_log("Process", f"'{name}' 단계 감지: {amount}{unit_txt} 대기 시작")

        # ✉ delay 시작 챗 알림
        if self.chat:
            try:
                total = len(getattr(self, "process_queue", []) or [])
                cur   = int(getattr(self, "current_process_index", -1)) + 1
                idx_txt = f" ({cur}/{total})" if total > 0 and cur > 0 else ""
                msg = f"⏱️ CH{self.ch} delay 단계 시작{idx_txt}: {amount}{unit_txt} 대기"

                ret = self.chat.notify_text(msg)
                if inspect.iscoroutine(ret):
                    self._spawn_detached(ret, name=f"Chat.DelayStart.CH{self.ch}")
                if hasattr(self.chat, "flush"):
                    self.chat.flush()
            except Exception as e:
                self.append_log("CHAT", f"delay 시작 알림 실패: {e!r}")

        # 폴링 모두 정지(원래 로직 유지)
        self._apply_polling_targets({"mfc": False, "dc_pulse": False, "rf_pulse": False, "dc": False, "rf": False})
        self._last_polling_targets = None

        # 상태창 초기 표시(남은 시간까지 같이)
        if self._w_state:
            # 첫 화면을 '남은 시간' 포함해 바로 표시
            h = int(duration_s) // 3600
            m_ = (int(duration_s) % 3600) // 60
            s_ = int(duration_s) % 60
            self._w_state.setPlainText(f"지연 대기 중: {amount}{unit_txt} · 남은 시간 {h:02d}:{m_:02d}:{s_:02d}")

        # 기존 지연 태스크 취소 후, 카운트다운 코루틴 등록
        self._cancel_delay_task()
        self._set_task_later(
            "_delay_countdown_task",
            self._delay_countdown_then_continue(name, duration_s, amount, unit_txt),
            name=f"Delay:{name}"
        )

        return True
    
    def _graph_reset_safe(self) -> None:
        try:
            self.graph.reset()
        except Exception:
            self.append_log("Graph", "reset skipped (headless)")

    def _graph_clear_rga_plot_safe(self) -> None:
        try:
            self.graph.clear_rga_plot()
        except Exception:
            self.append_log("Graph", "clear_rga_plot skipped (headless)")

    def _graph_update_rga_safe(self, x, y) -> None:
        try:
            x_list = x.tolist() if hasattr(x, "tolist") else x
            y_list = y.tolist() if hasattr(y, "tolist") else y
            self.graph.update_rga_plot(x_list, y_list)
        except Exception as e:
            self.append_log("Graph", f"update_rga_plot skipped: {e!r}")

    def _safe_clear_oes_plot(self) -> None:
        try: self.graph.clear_oes_plot()
        except Exception as e:
            self.append_log("OES", f"그래프 초기화 실패(무시): {e!r}")

    def _post_update_oes_plot(self, x: Sequence[float], y: Sequence[float]) -> None:
        def _safe_draw():
            try:
                xx = x.tolist() if hasattr(x, "tolist") else list(x)
                yy = y.tolist() if hasattr(y, "tolist") else list(y)
                self.graph.update_oes_plot(xx, yy)
            except Exception as e:
                self.append_log("OES", f"그래프 업데이트 실패(무시): {e!r}")
        self._soon(_safe_draw)

    # ------------------------------------------------------------------
    # 폴링/상태
    def _apply_polling_targets(self, targets: TargetsMap) -> None:
        mfc_on = bool(targets.get('mfc', False))
        dcpl_on = bool(targets.get('dc_pulse', False))
        rfpl_on = bool(targets.get('rf_pulse', False))
        dc_on   = bool(targets.get('dc', False))
        rf_on   = bool(targets.get('rf', False))

        # ✅ 어떤 폴링이라도 실제로 켜야 할 때 + 자동연결 허용 + 공정 실행 중일 때만 자동 기동
        if (mfc_on or dcpl_on or rfpl_on or dc_on or rf_on) \
                and self._auto_connect_enabled \
                and self.process_controller.is_running:
            self._ensure_background_started()

        with contextlib.suppress(Exception):
            self.mfc.set_process_status(mfc_on)

        if self.dc_pulse:
            with contextlib.suppress(Exception):
                # ✅ True/False 모두 직접 전달(다른 장치들과 일관)
                self.dc_pulse.set_process_status(dcpl_on)

        if self.rf_pulse:
            with contextlib.suppress(Exception):
                self.rf_pulse.set_process_status(rfpl_on)

        if self.dc_power and hasattr(self.dc_power, "set_process_status"):
            with contextlib.suppress(Exception):
                self.dc_power.set_process_status(dc_on)

        if self.rf_power and hasattr(self.rf_power, "set_process_status"):
            with contextlib.suppress(Exception):
                self.rf_power.set_process_status(rf_on)

    # ------------------------------------------------------------------
    # 로그
    def append_log(self, source: str, msg: str) -> None:
        now_ui = datetime.now().strftime("%H:%M:%S")
        now_file = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line_ui = f"[{now_ui}] [CH{self.ch}:{source}] {msg}"
        line_file = f"[{now_file}] [CH{self.ch}:{source}] {msg}\n"

        self._soon(self._enqueue_ui_log, line_ui)

        if not getattr(self, "_log_file_path", None):
            self._soon(self._prestart_buf.append, line_file)
            return
        self._soon(self._log_enqueue_nowait, line_file)

    def _dl_fire_and_forget(self, fn, *args, **kwargs) -> None:
        """
        DataLogger처럼 NAS/파일 I/O 가능성이 있는 동기 함수를
        이벤트루프(=UI)에서 직접 돌리지 않기 위한 안전 래퍼.
        """
        async def _run():
            try:
                # ✅ blocking I/O는 thread로
                await asyncio.to_thread(fn, *args, **kwargs)
            except Exception:
                # DataLogger 실패는 공정을 죽이면 안 되므로 조용히 무시(필요 시 rate-limit 로그만)
                pass

        self._spawn_detached(_run(), name=f"DL.{getattr(fn, '__name__', 'call')}.CH{self.ch}")

    def _enqueue_ui_log(self, line: str) -> None:
        # UI 스레드에서 호출되도록 _soon을 통해 들어온다고 가정
        self._ui_log_buf.append(line)

    def _flush_ui_log_to_ui(self) -> None:
        w = getattr(self, "_w_log", None)
        if not w or not _qt_is_valid(w):
            self._w_log = None
            return
        if not self._ui_log_buf:
            return

        try:
            sb = w.verticalScrollBar()
        except Exception:
            return

        # ✅ 사용자가 이미 최하단을 보고 있을 때만 '바닥에 붙이는' 오토 스크롤 유지
        stick_to_bottom = True
        try:
            stick_to_bottom = (sb.value() >= (sb.maximum() - 2))
        except Exception:
            stick_to_bottom = True

        # 한 번에 몰아서 출력 (UI 작업 최소화)
        lines = []
        max_lines = 200  # 100~300 사이 추천
        while self._ui_log_buf and len(lines) < max_lines:
            s = self._ui_log_buf.popleft()
            if s is None:
                continue
            try:
                s = str(s)
            except Exception:
                continue
            s = s.rstrip("\r\n")
            if not s:
                continue
            lines.append(s)

        if not lines:
            return

        prefix = ""
        try:
            prefix = "" if w.document().isEmpty() else "\n"
        except Exception:
            prefix = "\n"

        text = prefix + "\n".join(lines)

        old_sb_val = None
        if not stick_to_bottom:
            with contextlib.suppress(Exception):
                old_sb_val = sb.value()

        # ✅ 여기서도 invalid/타이밍 이슈를 최대한 방어
        try:
            w.moveCursor(QTextCursor.MoveOperation.End)
            w.insertPlainText(text)
        except Exception:
            return

        if old_sb_val is not None:
            with contextlib.suppress(Exception):
                sb.setValue(old_sb_val)
            return

        if not getattr(self, "_log_autoscroll_pending", False):
            self._log_autoscroll_pending = True

            def _scroll_bottom():
                self._log_autoscroll_pending = False
                ww = getattr(self, "_w_log", None)
                if not ww or not _qt_is_valid(ww):
                    self._w_log = None
                    return
                with contextlib.suppress(Exception):
                    sbb = ww.verticalScrollBar()
                    sbb.setValue(sbb.maximum())
                    ww.ensureCursorVisible()

            QTimer.singleShot(0, _scroll_bottom)

    def _ensure_log_dir(self, root: Path) -> Path:
        nas_path = Path(root)
        local_fallback = Path.cwd() / f"_Logs_local_CH{self.ch}"
        try:
            nas_path.mkdir(parents=True, exist_ok=True)
            return nas_path
        except Exception:
            local_fallback.mkdir(parents=True, exist_ok=True)

            w = getattr(self, "_w_log", None)
            if w and _qt_is_valid(w):
                with contextlib.suppress(Exception):
                    w.appendPlainText(f"[Logger] NAS 폴더 접근 실패 → 로컬 폴백: {local_fallback}")

            return local_fallback

    def _prepare_log_file(self, params: Mapping[str, Any]) -> None:
        now_local = datetime.now()
        ts = now_local.strftime("%Y%m%d_%H%M%S")

        # 1) 공정명 가져오기 (UI / CSV 공통)
        raw_name = str(params.get("process_note") or params.get("Process_name") or "").strip()

        # 2) 공정명 비어있으면 기본값 (UI 단일공정은 이미 Single CHx로 들어오는 편이지만, 안전장치)
        if not raw_name:
            raw_name = "Untitled"

        # 3) 파일명에 못 쓰는 문자 제거 (Windows/SMB/NAS 호환)
        safe_name = re.sub(r'[\\/:*?"<>|]+', "_", raw_name)   # 금지문자 치환
        safe_name = re.sub(r"\s+", " ", safe_name).strip()    # 공백 정리
        safe_name = safe_name.replace(" ", "_")               # 공백 → _
        safe_name = safe_name.strip(" .")                     # 끝점/끝공백 방지
        safe_name = safe_name[:60] if safe_name else "Untitled"  # 너무 길면 잘라내기

        # 4) 최종 파일명: CH2_공정명_날짜_시간.txt
        base = self._log_dir / f"CH{self.ch}_{safe_name}_{ts}"
        path = base.with_suffix(".txt")

        i = 1
        while path.exists():
            path = (self._log_dir / f"CH{self.ch}_{safe_name}_{ts}_{i}").with_suffix(".txt")
            i += 1

        self._log_file_path = path
        if self._log_fp is None:
            self._log_fp = open(self._log_file_path, "a", encoding="utf-8", newline="")
        if not self._log_writer_task or self._log_writer_task.done():
            self._set_task_later("_log_writer_task", self._log_writer_loop(), name=f"LogWriter.CH{self.ch}")

        # (삭제) prestart_buf는 _open_run_log에서 헤더 뒤로 밀어 넣는다.

        note = str(params.get("process_note", "") or params.get("Process_name", "") or f"Run CH{self.ch}")
        self.append_log("MAIN", f"=== '{note}' 공정 준비 (장비 연결부터 기록) ===")

    def _open_run_log(self, params: Mapping[str, Any]) -> None:
        now_local = datetime.now()
        ts = now_local.strftime("%Y%m%d_%H%M%S")

        raw_name = str(params.get("process_note") or params.get("Process_name") or "").strip()
        if not raw_name:
            raw_name = "Untitled"

        safe_name = re.sub(r'[\\/:*?"<>|]+', "_", raw_name)
        safe_name = re.sub(r"\s+", " ", safe_name).strip()
        safe_name = safe_name.replace(" ", "_")
        safe_name = safe_name.strip(" .")
        safe_name = safe_name[:60] if safe_name else "Untitled"

        base = (self._log_dir / f"CH{self.ch}_{safe_name}_{ts}").with_suffix(".txt")
        path = base
        i = 1
        while path.exists():
            path = (self._log_dir / f"CH{self.ch}_{safe_name}_{ts}_{i}").with_suffix(".txt")
            i += 1

        # ✅ 여기서부터 writer가 이 경로로만 쓴다 (직접 open 금지)
        self._log_file_path = path
        self._log_fp = None  # 더 이상 사용하지 않음

        if not self._log_writer_task or self._log_writer_task.done():
            self._set_task_later("_log_writer_task", self._log_writer_loop(), name=f"LogWriter.CH{self.ch}")

        name = (params.get("process_note") or params.get("Process_name") or f"Run CH{self.ch}")

        # ✅ 헤더도 큐로 기록(순서 보장)
        self._log_enqueue_nowait("# ==== Sputter Run ====\n")
        self._log_enqueue_nowait(f"# started_at = {datetime.now().isoformat()}\n")
        self._log_enqueue_nowait(f"# chamber = CH{self.ch}\n")
        self._log_enqueue_nowait(f"# process_name = {name}\n")
        if "process_time" in params:
            self._log_enqueue_nowait(f"# time_min = {float(params.get('process_time', 0) or 0):.2f}\n")
        self._log_enqueue_nowait("# ============================\n")

        # ✅ pre-start 버퍼도 헤더 뒤로 밀어 넣기
        with contextlib.suppress(Exception):
            for line in list(self._prestart_buf):
                self._log_enqueue_nowait(line)
            self._prestart_buf.clear()

        self.append_log("Logger", f"새 로그 파일 시작: {path.name}")

    def _close_run_log(self) -> None:
        """종료 마커만 큐에 넣고, 실제 flush/close는 _shutdown_log_writer()에서 처리."""
        with contextlib.suppress(Exception):
            self._log_enqueue_nowait("# ==== END ====\n")

    def _log_enqueue_nowait(self, line: str) -> None:
        try:
            self._log_q.put_nowait(line)
        except asyncio.QueueFull:
            with contextlib.suppress(Exception):
                _ = self._log_q.get_nowait()
                self._log_q.put_nowait(line)

    def _log_write_sync(self, path: Path, text: str) -> None:
        """⚠️ 반드시 to_thread로만 호출. (UI/이벤트루프에서 직접 호출 금지)"""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8", newline="") as fp:
            fp.write(text)
            fp.flush()

    async def _log_writer_loop(self):
        try:
            while True:
                # ✅ 폴링(get_nowait+sleep) 대신 “대기”로 CPU 절약 + 안정성↑
                line = await self._log_q.get()

                # ✅ 한 번에 배치로 모아서 write 횟수/flush 횟수 줄이기
                batch = [line]
                for _ in range(300):  # 배치 크기(원하면 조절)
                    try:
                        batch.append(self._log_q.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                text = "".join(batch)

                # ✅ 파일 경로가 없으면 버림(또는 prestart_buf로 보내도 됨)
                if not self._log_file_path:
                    continue

                # ✅ open/write/flush는 무조건 스레드로
                try:
                    loop = asyncio.get_running_loop()
                    await asyncio.wait_for(
                        loop.run_in_executor(self._log_io_exec, self._log_write_sync, self._log_file_path, text),
                        timeout=5.0,
                    )
                except Exception as e:
                    # NAS 쓰기/열기 실패 → 로컬로 전환 후 다시 시도
                    try:
                        local_dir = Path.cwd() / f"_Logs_local_CH{self.ch}"
                        local_dir.mkdir(parents=True, exist_ok=True)
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        self._log_file_path = (local_dir / f"CH{self.ch}_{ts}_recovered.txt")

                        loop = asyncio.get_running_loop()
                        await asyncio.wait_for(
                            loop.run_in_executor(self._log_io_exec, self._log_write_sync, self._log_file_path, text),
                            timeout=5.0,
                        )
                        self.append_log("Logger", f"NAS 로그 쓰기 실패({e!r}) → 로컬 폴백: {self._log_file_path}")
                    except Exception:
                        # ✅ 최악: 로컬도 실패하면, 유실을 최소화하려고 재큐잉(단, 무한루프 주의)
                        # 필요하면 여기서 batch를 파일 대신 메모리 버퍼로 보관하는 쪽이 더 안전
                        await asyncio.sleep(0.2)
                        for s in batch[:50]:  # 너무 많이 재큐잉하면 또 폭주할 수 있으니 제한
                            self._soon(self._log_enqueue_nowait, s)

        except asyncio.CancelledError:
            pass

    async def _shutdown_log_writer(self):
        # ✅ 어떤 상황에도 상태 리셋은 보장 (취소/예외에도 finally 실행)
        path = self._log_file_path
        fp = self._log_fp
        loop = asyncio.get_running_loop()

        try:
            # 1) writer 중지
            t = self._log_writer_task
            self._log_writer_task = None
            if t:
                t.cancel()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(t, timeout=2.0)

            # 2) 큐 드레인 (이벤트루프에서 빠르게 메모리로만)
            drained: list[str] = []
            while True:
                try:
                    drained.append(self._log_q.get_nowait())
                except asyncio.QueueEmpty:
                    break
            text = "".join(drained)

            # 3) (남아있을 수 있는) fp는 executor로 닫기
            self._log_fp = None
            if fp:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(
                        loop.run_in_executor(self._log_io_exec, fp.close),
                        timeout=2.0,
                    )

            # 4) 남은 로그를 executor에서 “한 번에” 쓰기
            if path and text:
                try:
                    await asyncio.wait_for(
                        loop.run_in_executor(self._log_io_exec, self._log_write_sync, path, text),
                        timeout=6.0,
                    )
                except Exception:
                    # 최후 폴백: 로컬로라도 남김
                    with contextlib.suppress(Exception):
                        local_dir = Path.cwd() / f"_Logs_local_CH{self.ch}"
                        local_dir.mkdir(parents=True, exist_ok=True)
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        rec = local_dir / f"CH{self.ch}_{ts}_shutdown_drain.txt"
                        await asyncio.wait_for(
                            loop.run_in_executor(self._log_io_exec, self._log_write_sync, rec, text),
                            timeout=6.0,
                        )

        finally:
            # 5) 정리 (다음 런 보장)
            self._log_fp = None
            self._log_file_path = None

            # ✅ 중요: Queue 객체 자체를 교체하지 않는다.
            # - 교체하면 "기존 queue를 await 중인 writer/task"와 레이스가 생길 수 있고,
            #   다음 공정에서 로그가 안 써지는 문제가 발생할 수 있다.
            # - 이미 위에서 drain을 했으니, 안전하게 한번 더 비우기만 한다.
            while True:
                try:
                    self._log_q.get_nowait()
                except asyncio.QueueEmpty:
                    break

    def _clear_queue_and_reset_ui(self) -> None:
        # 전역 runtime_state로 종료 시각을 기록하므로 로컬 타임스탬프는 불필요
        # ★ 추가: 남아 있을 수 있는 카운트다운 태스크 정리
        self._cancel_delay_task()

        # 1) 리스트 공정 인덱스/큐 초기화
        self.current_process_index = -1
        
        # ★ 핵심: 리스트 공정 큐까지 비워서 다음 Start는 단일 공정(UI 기반)으로만 동작하게
        try:
            if hasattr(self, "process_queue"):
                # 남아 있는 CSV 공정 리스트 제거
                self.process_queue.clear()
        except Exception:
            # 혹시 구조가 꼬여 있어도 다음 런에 영향 없도록 빈 리스트로 강제 재설정
            self.process_queue = []
    
        # 2) UI 리셋
        self._reset_ui_after_process()

        # 3) 로그 파일 / 로그 writer 정리
        with contextlib.suppress(Exception):
            self._close_run_log()

        with contextlib.suppress(Exception):
            self._spawn_detached(self._shutdown_log_writer())

        self._log_file_path = None

        # 4) 프리스타트 버퍼 정리 (한 번만 호출해도 충분)
        with contextlib.suppress(Exception):
            self._prestart_buf.clear()

        # 5) 종료 관련 내부 플래그
        # - cleanup 타임아웃이면 Start를 막아야 안전(“정리 덜 끝났는데 idle” 방지)
        if not getattr(self, "_cleanup_timed_out", False):
            self._pending_device_cleanup = False
            self._pc_stopping = False
        else:
            self.append_log("MAIN", "⚠ cleanup timeout 상태 유지: Start 제한 유지")

    # ------------------------------------------------------------------
    # 기본 UI값/리셋
    def _set_default_ui_values(self) -> None:
        _set = self._set
        
        _set("integrationTime_edit", "")
        _set("workingPressure_edit", "2")
        _set("arFlow_edit", "20")
        _set("o2Flow_edit", "0")
        _set("n2Flow_edit", "0")
        _set("dcPower_edit", "130")

        # DC-Pulse
        _set("dcPulsePower_checkbox", False)
        _set("dcPulsePower_edit", "200")
        _set("dcPulseFreq_edit", "")
        _set("dcPulseDutyCycle_edit", "")

        # RF-Pulse
        _set("rfPulsePower_checkbox", False)
        _set("rfPulsePower_edit", "100")
        _set("rfPulseFreq_edit", "")
        _set("rfPulseDutyCycle_edit", "")

        # RF-Power
        _set("rfPower_checkbox", False)
        _set("rfPower_edit", "0")

        # ← 추가: 챔버별 기본 체크
        try:
            if self.ch == 1:
                _set("basePressure_edit", "1e-6")
                _set("Ar_checkbox", True)
                _set("dcPulsePower_checkbox", True)   # CH1: DC Pulse 사용
                _set("dcPower_checkbox", False)
                _set("shutterDelay_edit", "0")
                _set("processTime_edit", "30")
            elif self.ch == 2:
                _set("basePressure_edit", "9e-6")
                _set("G2_checkbox", True)             # CH2: G2 사용
                _set("Ar_checkbox", True)             # CH2: Ar 가스
                _set("dcPower_checkbox", True)        # CH2: DC Power 사용
                _set("dcPulsePower_checkbox", False)
                _set("shutterDelay_edit", "5")
                _set("processTime_edit", "25")
        except Exception:
            pass

        self._apply_ui_lockouts()

    def _apply_ui_lockouts(self) -> None:
        """챔버별로 선택/입력을 막아야 하는 UI를 비활성화한다."""
        if not getattr(self, "ui", None):
            return

        def _raw(leaf: str):
            return getattr(self.ui, f"{self.prefix}{leaf}", None)

        def _disable(leaf: str):
            w = _raw(leaf)
            if w is None:
                return
            try:
                w.setEnabled(False)
            except Exception:
                pass

        # ✅ CH2에서도 RF-Pulse 가능
        #    단, config/생성자 설정상 supports_rf_pulse=False면 UI를 비활성화
        if (self.ch == 2) and (not self.supports_rf_pulse):
            for leaf in ("rfPulsePower_checkbox", "rfPulsePower_edit", "rfPulseFreq_edit", "rfPulseDutyCycle_edit"):
                _disable(leaf)

    def _reset_ui_after_process(self):
        self._set_default_ui_values()

        # ✅ 타겟명 초기화 (공통 leaf 사용 → CH1은 단일 위젯으로 alias 매핑됨)
        for leaf in ("g1Target_name", "g2Target_name", "g3Target_name"):
            # CH1에선 세 leaf가 모두 같은 'gunTarget_name'으로 alias 되지만, 같은 위젯을 여러 번 비워도 무해
            self._set(leaf, "")

        for name in (
            "G1_checkbox","G2_checkbox","G3_checkbox","Ar_checkbox","O2_checkbox","N2_checkbox",
            "mainShutter_checkbox","dcPulsePower_checkbox","rfPulsePower_checkbox","dcPower_checkbox","powerSelect_checkbox",
        ):
            w = self._u(name)
            if w is not None:
                with contextlib.suppress(Exception):
                    w.setChecked(False)
        
        # ← 추가: 챔버별 기본 체크 복원
        try:
            if self.ch == 1:
                self._u("Ar_checkbox") and self._u("Ar_checkbox").setChecked(True)
                self._u("dcPulsePower_checkbox") and self._u("dcPulsePower_checkbox").setChecked(True)
            elif self.ch == 2:
                self._u("G2_checkbox") and self._u("G2_checkbox").setChecked(True)
                self._u("Ar_checkbox") and self._u("Ar_checkbox").setChecked(True)
                self._u("dcPower_checkbox") and self._u("dcPower_checkbox").setChecked(True)
        except Exception:
            pass

        _s = self._u("processState_edit")
        if _s: _s.setPlainText("대기 중")

        for leaf in ("Power_edit","Voltage_edit","Current_edit","forP_edit","refP_edit"):
            w = self._u(leaf)
            if w: w.setPlainText("")

        self._on_process_status_changed(False)
        with contextlib.suppress(Exception):
            self.graph.reset()

    # ======= 서버 통신 api =======
    def _host_report_start(self, ok: bool, reason: str = "") -> None:
        fut = getattr(self, "_host_start_future", None)
        if fut is not None and not fut.done():
            fut.set_result((bool(ok), str(reason)))

    async def start_with_recipe_string(self, recipe: str) -> None:
        """
        Host 진입점:
        - 프리플라이트를 새로 하지 않는다
        - 기존 시작 경로(_handle_start_clicked)만 호출
        - 프리플라이트가 보내줄 결과 신호(Future)만 잠깐 대기해 핸들러에 반환
        """
        loop = asyncio.get_running_loop()
        self._host_start_future = loop.create_future()

        s = (recipe or "").strip()
        if not s:
            # 현재 UI 값으로 단발 시작 (버튼과 동일 경로)
            self._handle_start_clicked(False)
        elif s.lower().endswith(".csv"):
            if not os.path.exists(s):
                raise RuntimeError(f"CSV 파일을 찾을 수 없습니다: {s}")
            # CSV 로드 + 큐 구성 + 첫 행 UI 반영 (네 코드 그대로)
            with open(s, mode='r', encoding='utf-8-sig', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                self.process_queue = []
                self.current_process_index = -1
                for row in reader:
                    name = (row.get('Process_name') or row.get('#') or f"공정 {len(self.process_queue)+1}").strip()
                    row['Process_name'] = name
                    self.process_queue.append(cast(RawParams, row))
            if not self.process_queue:
                raise RuntimeError("CSV에 공정 데이터가 없습니다.")
            self._update_ui_from_params(self.process_queue[0])
            self.append_log("File", f"CSV 로드 완료: {s} (총 {len(self.process_queue)}개)")

            # 버튼과 동일 경로로 시작 (프리플라이트는 내부에서 호출됨)
            self._handle_start_clicked(False)
        else:
            raise RuntimeError("지원하지 않는 레시피 형식입니다. CSV 경로만 허용됩니다.")

        # ✅ 프리플라이트가 보내는 신호만 잠깐 대기 (타임아웃은 10초 권장)
        try:
            ok, reason = await asyncio.wait_for(self._host_start_future, timeout=10.0)
        except asyncio.TimeoutError:
            # 프리플라이트가 호출되지 않은 경우(초기 가드에서 막힘) 대비
            raise RuntimeError("preflight timeout (start guard 또는 내부 대기로 인해 프리플라이트 미도달)")
        finally:
            self._host_start_future = None

        if not ok:
            raise RuntimeError(reason)
        # ok면 그대로 반환 (공정은 내부에서 계속 진행)

    # ------------------------------------------------------------------
    # 유틸
    # ------------------------------------------------------------------
    import re

    def _parse_duration_seconds(self, s: str) -> float:
        """
        '10s', '1m', '1h30m', '2h' 형태 문자열을 초 단위로 변환.
        """
        if not s:
            return 0.0
        s = s.replace(" ", "").lower()
        pattern = r"(?:(\d+(?:\.\d+)?)h)?(?:(\d+(?:\.\d+)?)m)?(?:(\d+(?:\.\d+)?)s)?"
        m = re.match(pattern, s)
        if not m:
            try:
                return float(s) * 60.0  # 단위 없으면 분으로 처리
            except Exception:
                return 0.0
        h = float(m.group(1) or 0)
        m_ = float(m.group(2) or 0)
        s_ = float(m.group(3) or 0)
        return h * 3600 + m_ * 60 + s_

    def _spawn_detached(self, coro, *, store: bool=False, name: str|None=None) -> None:
        loop = self._loop
        def _create():
            t = loop.create_task(coro, name=name)
            def _done(task: asyncio.Task):
                if task.cancelled():
                    return
                try:
                    exc = task.exception()
                except Exception as e:
                    self.append_log(f"Task{self.ch}", f"exception() failed: {e!r}")
                    return
                if exc:
                    import traceback
                    tb = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__)).rstrip()
                    self.append_log(f"Task{self.ch}", f"[{name or 'task'}] crashed:\n{tb}")

            t.add_done_callback(_done)
            if store:
                self._bg_tasks.append(t)

        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            loop.call_soon(_create)
        else:
            loop.call_soon_threadsafe(_create)

    def _set_task_later(self, attr_name: str, coro: Coroutine[Any, Any, Any], *, name: str | None = None) -> None:
        """UI/다른 스레드 어디서든 안전하게 task를 만들고, 예외를 조용히 삼키지 않게 한다."""
        loop = self._loop

        def _create_and_set():
            try:
                t = loop.create_task(coro, name=name)
            except Exception as e:
                with contextlib.suppress(Exception):
                    coro.close()
                tb = "".join(traceback.format_exception(type(e), e, e.__traceback__)).rstrip()
                self.append_log("Task", f"[{name or attr_name}] create_task failed:\n{tb}")

                if attr_name in ("_delay_main_task", "_log_writer_task"):
                    with contextlib.suppress(Exception):
                        self._set_state_text("내부 오류: 태스크 생성 실패(로그 확인)")
                    with contextlib.suppress(Exception):
                        self._post_critical(
                            "내부 오류",
                            "백그라운드 작업 생성에 실패했습니다.\n"
                            "프로그램이 멈춘 것처럼 보일 수 있습니다.\n\n"
                            "자세한 내용은 로그 파일(또는 터미널)을 확인해주세요.",
                        )
                setattr(self, attr_name, None)
                return

            setattr(self, attr_name, t)

            def _done(task: asyncio.Task):
                if task.cancelled():
                    return
                try:
                    exc = task.exception()
                except Exception as e2:
                    self.append_log("Task", f"[{name or attr_name}] exception() failed: {e2!r}")
                    return
                if exc:
                    tb2 = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)).rstrip()
                    self.append_log("Task", f"[{name or attr_name}] crashed:\n{tb2}")

                    if attr_name == "_delay_main_task":
                        with contextlib.suppress(Exception):
                            self._set_state_text("다음 공정 예약 실패(로그 확인)")
                        with contextlib.suppress(Exception):
                            self._post_critical(
                                "다음 공정 예약 실패",
                                "다음 공정을 시작하기 위한 내부 작업이 중단되었습니다.\n"
                                "자세한 내용은 로그 파일(또는 터미널)을 확인해주세요.",
                            )

                with contextlib.suppress(Exception):
                    if getattr(self, attr_name, None) is task:
                        setattr(self, attr_name, None)

            t.add_done_callback(_done)

        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            loop.call_soon(_create_and_set)
        else:
            loop.call_soon_threadsafe(_create_and_set)

    def _loop_from_anywhere(self) -> asyncio.AbstractEventLoop:
        try: return asyncio.get_running_loop()
        except RuntimeError: return self._loop

    def _soon(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        def _safe():
            try:
                fn(*args, **kwargs)
            except Exception as e:
                tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__)).rstrip()
                self.append_log(f"CB{self.ch}", f"callback failed:\n{tb}")
        loop = self._loop
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            loop.call_soon(_safe)
        else:
            loop.call_soon_threadsafe(_safe)

    def _is_dev_connected(self, dev: object) -> bool:
        try:
            v = getattr(dev, "is_connected", None)
            if callable(v): return bool(v())
            if isinstance(v, bool): return v
        except Exception:
            pass
        try: return bool(getattr(dev, "_connected", False))
        except Exception: return False

    async def _preflight_progress_log(self, need: list[tuple[str, object]], stop_evt: asyncio.Event) -> None:
        """프리플라이트 진행 상황을 1초 간격으로 계속 로그에 남긴다.

        기존 구현은 wait_for(timeout=1.0)에서 TimeoutError가 한 번 발생하면 함수가 종료되어
        '연결 대기 중' 로그가 1회만 남고 이후 진행 상황이 보이지 않는 문제가 있었다.
        """
        try:
            while not stop_evt.is_set():
                missing = [name for name, dev in need if not self._is_dev_connected(dev)]
                txt = ", ".join(missing) if missing else "모두 연결됨"
                self.append_log("MAIN", f"연결 대기 중: {txt}")

                try:
                    await asyncio.wait_for(stop_evt.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.append_log("MAIN", f"프리플라이트 진행 로그 예외: {e!r}")

    # --- UI 위젯 접근/부모/다이얼로그 관리 -----------------------------------
    def _alias_leaf(self, leaf: str) -> str:
        """CH1의 UI 위젯 이름과 공통 이름을 매핑.
        주의: 실제 속성 접근은 getattr(self.ui, f"{self.prefix}{name}") 이므로,
        여기서는 prefix(예: 'ch1_')를 절대 포함하지 않는다.
        """
        if self.ch != 1:
            return leaf
        return {
            "integrationTime_edit": "intergrationTime_edit",

            # CH1은 단일 타겟 위젯: ch1_gunTarget_name
            "g1Target_name": "gunTarget_name",
            "g2Target_name": "gunTarget_name",
            "g3Target_name": "gunTarget_name",

            # ✅ CH1: Pulse 입력(Edit/Freq/Duty)은 1세트를 공용(dcPulse*)으로 유지한다.
            #    - RF/DC 선택은 라디오(rfPulsePower_checkbox / dcPulsePower_checkbox)로 처리
            #    - 따라서 edit/freq/duty만 rfPulse* → dcPulse*로 alias 한다.
            "rfPulsePower_edit": "dcPulsePower_edit",
            "rfPulseFreq_edit": "dcPulseFreq_edit",
            "rfPulseDutyCycle_edit": "dcPulseDutyCycle_edit",

        }.get(leaf, leaf)

    def _u(self, name: str) -> Any | None:
        """prefix+name 위젯을 가져온다. 없으면 None."""
        name = self._alias_leaf(name)
        if not getattr(self, "ui", None):
            return None

        w = getattr(self.ui, f"{self.prefix}{name}", None)
        if w is None:
            return None
        if not _qt_is_valid(w):
            return None
        return w

    def _parent_widget(self) -> Any | None:
        """메시지/파일 다이얼로그의 합리적 부모 위젯을 찾는다."""
        for leaf in ("Start_button", "Stop_button", "processState_edit", "logMessage_edit"):
            w = self._u(leaf)
            if w is not None:
                try:
                    return w.window()
                except Exception:
                    return w
        return None

    async def _aopen_file(self, caption="CSV 선택", start_dir="", 
                        name_filter="CSV Files (*.csv);;All Files (*.*)") -> str:
        if not self._has_ui():
            self.append_log("File", "headless: 파일 선택 UI 생략"); return ""

        dlg = QFileDialog(self._parent_widget() or None, caption, start_dir, name_filter)
        dlg.setFileMode(QFileDialog.ExistingFile)

        # ✅ Windows 쉘/COM(네이티브 파일 다이얼로그) 우회
        with contextlib.suppress(Exception):
            dlg.setOption(QFileDialog.Option.DontUseNativeDialog, True)
        with contextlib.suppress(Exception):
            dlg.setOption(QFileDialog.DontUseNativeDialog, True)
        with contextlib.suppress(Exception):
            dlg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
            dlg.setWindowModality(Qt.WindowModality.WindowModal)

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[str] = loop.create_future()

        def _done(result: int):
            # ✅ finished가 중복으로 들어와도 Future를 두 번 set_result하지 않게
            if fut.done():
                return
            try:
                if result == QDialog.Accepted and dlg.selectedFiles():
                    fut.set_result(dlg.selectedFiles()[0])
                else:
                    fut.set_result("")  # 취소
            finally:
                with contextlib.suppress(Exception):
                    dlg.finished.disconnect(_done)
                dlg.deleteLater()

        dlg.finished.connect(_done)
        dlg.open()

        try:
            return await fut
        finally:
            # await 중 취소/예외에도 다이얼로그 정리
            if _qt_is_valid(dlg):
                with contextlib.suppress(Exception):
                    dlg.close()
                with contextlib.suppress(Exception):
                    dlg.deleteLater()

    def _ensure_msgbox_store(self):
        if not hasattr(self, "_msg_boxes"):
            self._msg_boxes = []

    def _post_warning(self, title: str, text: str, auto_close_ms: int = 5000) -> None:
        if not self._has_ui():
            self.append_log("WARN", f"{title}: {text}"); return

        self._ensure_msgbox_store()
        box = QMessageBox(self._parent_widget() or None)
        box.setWindowTitle(title)
        box.setText(text)
        box.setIcon(QMessageBox.Warning)
        box.setStandardButtons(QMessageBox.Ok)
        box.setWindowModality(Qt.WindowModality.WindowModal)
        box.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)

        self._msg_boxes.append(box)
        def _cleanup(_res: int):
            with contextlib.suppress(ValueError):
                self._msg_boxes.remove(box)
            box.deleteLater()
        box.finished.connect(_cleanup)

        # ✅ 기본 5초 자동 닫힘
        attach_autoclose(box, ms=auto_close_ms)

        box.open()

    def _post_critical(self, title: str, text: str, *, clear_status_to_idle: bool = False) -> None:
        if not self._has_ui():
            self.append_log("ERROR", f"{title}: {text}"); return

        self._ensure_msgbox_store()
        box = QMessageBox(self._parent_widget() or None)
        box.setWindowTitle(title)
        box.setText(text)
        box.setIcon(QMessageBox.Critical)
        box.setStandardButtons(QMessageBox.Ok)
        box.setWindowModality(Qt.WindowModality.WindowModal)
        box.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)

        self._msg_boxes.append(box)
        def _cleanup(_res: int):
            with contextlib.suppress(ValueError):
                self._msg_boxes.remove(box)
            box.deleteLater()
        box.finished.connect(_cleanup)

        if clear_status_to_idle:
            def _ack_to_idle(_res: int):
                # OK 클릭 시만 idle로 (X로 닫으면 0인 경우가 많음)
                if int(_res) == int(QMessageBox.Ok):
                    with contextlib.suppress(Exception):
                        runtime_state.clear_error("chamber", self.ch)
            box.finished.connect(_ack_to_idle)

        box.open()

    def _has_ui(self) -> bool:
        try:
            return QApplication.instance() is not None and self._parent_widget() is not None
        except Exception:
            return False
        
    def _format_card_payload_for_chat(self, p: dict) -> dict:
        """
        구글챗 카드에 보내기 전에 보기 좋게 정리:
        - CH1: 단일 타겟 위젯(ch1_gunTarget_name) 반영, G2/G3 제거
        - 파워: 사용하지 않는 종류는 키 자체를 제거(카드에 안 보이게)
        """
        q = dict(p)

        # 기본 헤더 타이틀이 없으면 CHx Sputter로 보정
        q.setdefault("prefix", f"CH{self.ch} Sputter")

        # ── 1) CH1은 건 1개만 노출 ─────────────────────────────────────────────
        if self.ch == 1:
            # NormParams 쪽(G1_target_name/ G1 Target)과 UI 위젯(ch1_gunTarget_name) 모두 커버
            name = (q.get("G1_target_name")
                    or q.get("G1 Target")
                    or q.get("ch1_gunTarget_name")  # ← 보강: 실제 UI 필드명
                    or "").strip()
            if name:
                q["use_g1"] = True
                q["G1_target_name"] = name
            # G2/G3 관련 키 제거
            for key in ("use_g2", "use_g3",
                        "G2_target_name", "G3_target_name",
                        "G2 Target", "G3 Target"):
                q.pop(key, None)

        # ── 2) 파워는 '사용 중'인 것만 노출 ─────────────────────────────────────
        def _drop(keys: tuple[str, ...]):
            for k in keys:
                q.pop(k, None)

        if not bool(q.get("use_dc_pulse", False)):
            _drop(("dc_pulse_power", "dc_pulse_freq", "dc_pulse_duty", "dc_pulse_duty_cycle"))
        if not bool(q.get("use_rf_pulse", False)):
            _drop(("rf_pulse_power", "rf_pulse_freq", "rf_pulse_duty", "rf_pulse_duty_cycle"))
        if not bool(q.get("use_dc_power", False)):
            _drop(("dc_power",))
        if not bool(q.get("use_rf_power", False)):
            _drop(("rf_power",))

        return q
        
    # ============================= PLC 로그 소유 관리 =============================
    def set_plc_log_owner(self, owns: bool) -> None:
        """이 런타임이 PLC 로그의 현재 소유자인지 토글"""
        prev = getattr(self, "_owns_plc", False)
        self._owns_plc = bool(owns)
        # 필요하면 디버깅용 로그(선택)
        # if prev != self._owns_plc:
        #     self.append_log("MAIN", f"PLC log owner -> {self._owns_plc}")
    # ============================= PLC 로그 소유 관리 =============================

    # ============================= 입력값 검증 헬퍼 =============================
    def _validate_norm_params(self, p: NormParams) -> list[str]:
        errs: list[str] = []

        # 공통: 가스/유량
        if not (p.get("use_ar") or p.get("use_o2") or p.get("use_n2")):
            errs.append("가스를 하나 이상 선택해야 합니다.")

        # 🔧 None / "None" / 빈 문자열 등을 0으로 처리
        for k in ("ar_flow", "o2_flow", "n2_flow"):
            raw = p.get(k, 0)

            # None, "", "None" → 0 으로 간주
            if raw is None:
                v = 0.0
            else:
                s = str(raw).strip()
                if s == "" or s.upper() == "NONE":
                    v = 0.0
                else:
                    try:
                        v = float(s)
                    except (TypeError, ValueError):
                        # 이상한 값이면 0으로 처리하고, 에러 리스트에만 남김 (선택)
                        v = 0.0
                        errs.append(f"{k} 값이 숫자가 아니라 0으로 처리(raw={raw!r}).")

            if v < 0:
                errs.append(f"{k}는 음수 불가")

            # 이후에서 확실히 float 로 쓰도록 p에 다시 넣어줌
            p[k] = v

        if self.ch == 1:
            use_rf = bool(p.get("use_rf_pulse"))
            use_dc = bool(p.get("use_dc_pulse"))

            if use_rf and use_dc:
                errs.append("CH1에서는 RF Pulse와 DC Pulse를 동시에 선택할 수 없습니다.")

            if not (use_rf or use_dc):
                errs.append("CH1은 RF Pulse 또는 DC Pulse 중 하나를 반드시 선택해야 합니다.")

            if use_rf:
                if p.get("rf_pulse_power", 0) <= 0:
                    errs.append("RF Pulse Target Power(W)는 0보다 커야 합니다.")
                f = p.get("rf_pulse_freq")
                d = p.get("rf_pulse_duty")
                if f is not None and not (1 <= f <= 100):
                    errs.append("RF Pulse Freq(kHz)는 1..100")
                if d is not None and not (1 <= d <= 99):
                    errs.append("RF Pulse Duty(%)는 1..99")

            if use_dc:
                if p.get("dc_pulse_power", 0) <= 0:
                    errs.append("DC Pulse Target Power(W)는 0보다 커야 합니다.")
                f = p.get("dc_pulse_freq")
                d = p.get("dc_pulse_duty")
                if f is not None and not (20 <= f <= 150):
                    errs.append("DC Pulse Freq(kHz)는 20..150")
                if d is not None and not (1 <= d <= 99):
                    errs.append("DC Pulse Duty(%)는 1..99")

        else:
            checked = int(p.get("use_g1", False)) + int(p.get("use_g2", False)) + int(p.get("use_g3", False))
            if checked == 0 or checked == 3:
                errs.append("G1~G3 중 1개 또는 2개만 선택")

            use_rf_pulse = bool(p.get("use_rf_pulse"))
            use_dc_pulse = bool(p.get("use_dc_pulse"))

            # DC/RF Pulse 동시 선택 금지(안전)
            if use_rf_pulse and use_dc_pulse:
                errs.append("CH2에서는 DC Pulse와 RF Pulse를 동시에 선택할 수 없습니다.")

            if use_rf_pulse:
                if not self.supports_rf_pulse:
                    errs.append("이 챔버는 RF-Pulse를 지원하지 않습니다.")
                else:
                    if p.get("rf_pulse_power", 0) <= 0:
                        errs.append("RF Pulse Target Power(W)는 0보다 커야 합니다.")
                    f = p.get("rf_pulse_freq")
                    d = p.get("rf_pulse_duty")
                    if f is not None and not (1 <= f <= 100):
                        errs.append("RF Pulse Freq(kHz)는 1..100")
                    if d is not None and not (1 <= d <= 99):
                        errs.append("RF Pulse Duty(%)는 1..99")

            # CH2는 (연속 DC/RF) 또는 (Pulse DC/RF) 중 하나 이상은 필요
            if not (p.get("use_dc_power") or p.get("use_rf_power") or use_dc_pulse or use_rf_pulse):
                errs.append("CH2는 DC/RF Power 또는 DC/RF Pulse 중 하나 이상 선택 필요")

            # (방어) Pulse와 RF 연속 동시 금지 규칙 유지(원하면 삭제 가능)
            if use_rf_pulse and p.get("use_rf_power"):
                errs.append("RF Pulse와 RF Power는 동시에 선택할 수 없습니다.")

            if p.get("use_dc_power") and p.get("dc_power", 0) < 0:
                errs.append("DC Target Power(W)는 0 이상이어야 합니다.")

        return errs
    # ============================= 입력값 검증 헬퍼 =============================  

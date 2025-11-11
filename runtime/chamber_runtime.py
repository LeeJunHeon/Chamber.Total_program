# controller/chamber_runtime.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import csv, asyncio, contextlib, inspect, re, traceback, os, time
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Deque, Literal, Mapping, Optional, Sequence, TypedDict, cast, Union
from pathlib import Path
from datetime import datetime, timedelta
from collections import deque

from PySide6.QtWidgets import QMessageBox, QFileDialog, QPlainTextEdit, QDialog, QApplication
from PySide6.QtGui import QTextCursor
from PySide6.QtCore import Qt  # ← 추가: 모달리티/속성 지정용

# 장비
from device.ig import AsyncIG
from device.mfc import AsyncMFC
from device.oes import OESAsync
from device.rga import RGA100AsyncAdapter
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
        if p:
            return Path(p)
        # 설정이 없으면 채널별 기본 파일로 분리
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

        # QMessageBox 참조 저장소(비모달 유지용)
        self._msg_boxes: list[QMessageBox] = []  # ← 추가

        # 기본 전략: CH1=DC-Pulse 전용, CH2=RF-Pulse(+DC 연속 +RF 연속)
        if supports_dc_cont  is None: supports_dc_cont  = (self.ch == 2)
        if supports_rf_cont  is None: supports_rf_cont  = (self.ch == 2)  # CH2에서 RF 연속 허용
        if supports_dc_pulse is None: supports_dc_pulse = (self.ch == 1)
        if supports_rf_pulse is None: supports_rf_pulse = (self.ch == 2)

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

        # 로거
        self.data_logger = DataLogger(ch=self.ch, csv_dir=Path(r"\\VanaM_NAS\VanaM_Sputter\Sputter\Calib\Database"))

        # 로그 파일 경로 관리(세션 단위) + 사전 버퍼
        self._log_root = Path(log_dir)
        # ✅ CH 로그를 루트 바로 아래 CH1/CH2에 저장
        self._log_dir = self._ensure_log_dir(self._log_root / f"CH{self.ch}")
        self._log_file_path: Path | None = None
        self._prestart_buf: Deque[str] = deque(maxlen=1000)
        self._log_fp = None
        self._log_q: asyncio.Queue[str] = asyncio.Queue(maxsize=4096)
        self._log_writer_task: asyncio.Task | None = None

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

        # RGA: config에서 연결 정보 꺼내 생성(단일/채널별 모두 지원)
        self.rga = None  # type: ignore
        try:
            ip, user, pwd = self.cfg.rga_creds()
            if ip:
                self.rga = RGA100AsyncAdapter(ip, user=user, password=pwd, name=f"CH{self.ch}")
        except Exception:
            self.rga = None  # 안전

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
            if self.rf_pulse:
                self.rf_pulse.stop_process()

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
                try:
                    self._ensure_background_started()

                    # 초기화
                    try:
                        if getattr(self.oes, "sChannel", -1) < 0:
                            ok = await self.oes.initialize_device()
                            if not ok:
                                raise RuntimeError("OES 초기화 실패")
                    except Exception as e:
                        self.append_log("OES", f"초기화 실패: {e!r} → 종료 절차로 전환")
                        self.process_controller.on_oes_failed("OES", f"init: {e}")
                        return

                    self._soon(self._safe_clear_oes_plot)

                    # 측정
                    # 이번 런에서만 finished 이벤트를 받도록 플래그 ON
                    self._oes_active = True
                    try:
                        # 가능하면 잔여 이벤트 드레인 (드라이버가 지원하면)
                        if hasattr(self.oes, "drain_events"):
                            with contextlib.suppress(Exception):
                                await self.oes.drain_events()

                        await self.oes.run_measurement(duration_sec, integration_ms)
                    except Exception as e:
                        self.append_log("OES", f"측정 예외: {e!r} → 종료 절차로 전환")
                        if self.chat:
                            with contextlib.suppress(Exception):
                                self.chat.notify_text(f"[OES] 측정 실패: {e!r}")
                        self.process_controller.on_oes_failed("OES", f"measure: {e}")
                        return
                    finally:
                        # finished 수신 여부와 관계없이 플래그 OFF
                        self._oes_active = False

                    # ✅ 정상 완료 시에는 여기서 아무 것도 호출하지 않음
                    # (success 처리는 OES 이벤트 pump의 'finished'에서 단일 경로로)

                except Exception as e:
                    self.append_log("OES", f"예상치 못한 예외: {e!r} → 종료 절차로 전환")
                    if self.chat:
                        with contextlib.suppress(Exception):
                            self.chat.notify_text(f"[OES] 예외: {e!r}")
                    self.process_controller.on_oes_failed("OES", f"unexpected: {e}")

            self._spawn_detached(run())

        def cb_rga_scan():
            async def _run():
                try:
                    self._ensure_background_started()
                    self._soon(self._graph_clear_rga_plot_safe)
                    if self.rga:
                        await self.rga.scan_histogram_to_csv(self.cfg.RGA_CSV_PATH)
                    else:
                        raise RuntimeError("RGA 어댑터 없음")
                except Exception as e:
                    msg = f"예외로 RGA 스캔 실패: {e!r} → 다음 단계"
                    self.append_log("RGA", msg)
                    self.process_controller.on_rga_finished()
            self._spawn_detached(_run())

        # 컨트롤러 생성
        self.process_controller = ProcessController(
            send_plc=cb_plc,
            send_mfc=cb_mfc,

            # 연속 파워
            send_dc_power=cb_dc_power, stop_dc_power=cb_dc_stop,
            send_rf_power=cb_rf_power, stop_rf_power=cb_rf_stop,

            # 펄스 파워(완전 분리)
            start_dc_pulse=cb_dc_pulse_start, stop_dc_pulse=cb_dc_pulse_stop,
            start_rf_pulse=cb_rf_pulse_start, stop_rf_pulse=cb_rf_pulse_stop,

            ig_wait=cb_ig_wait, cancel_ig=cb_ig_cancel,
            rga_scan=cb_rga_scan, oes_run=cb_oes_run,

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

                    # ✅ 전역: CH 공정 '시작' 시각 마킹
                    try:
                        runtime_state.mark_started("chamber", self.ch)
                    except Exception:
                        pass

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
                    except Exception:
                        pass
                    self._soon(self._graph_reset_safe)

                    # ✅ 텍스트 알림은 기존 그대로 유지
                    name = (params.get("process_note")
                            or params.get("Process_name")
                            or f"Run CH{self.ch}")
                    t = params.get("process_time", 0) or 0
                    line = f"▶️ CH{self.ch} '{name}' 시작 (t={float(t):.1f}s)"
                    self.append_log("MAIN", line)

                    # 폴링 타깃 초기화
                    self._last_polling_targets = None

                elif kind == "finished":
                    try:
                        ok = bool(payload.get("ok", False))
                        detail = payload.get("detail", {}) or {}
                        ok_for_log = bool(detail.get("ok_for_log", ok))
                        self.data_logger.finalize_and_write_log(ok_for_log)
                        await asyncio.sleep(0.20)

                        # ✅ 종료 카드 전송(성공 시 로그 X, 실패만 로그)
                        ok = bool(payload.get("ok", False))
                        detail = dict(payload.get("detail", {}) or {})

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
                                ret = self.chat.notify_process_finished_detail(ok, payload)
                                if inspect.iscoroutine(ret):
                                    await ret
                                # Plasma cleaning과 동일하게 즉시 밀어내기(버퍼링 드롭 방지)
                                if hasattr(self.chat, "flush"):
                                    self.chat.flush()
                            except Exception as e:
                                self.append_log("CHAT", f"구글챗 종료 카드 전송 실패: {e!r}")

                        try:
                            self.mfc.on_process_finished(ok)
                        except Exception:
                            pass

                        # ✅ 전역: CH 공정 '종료' 시각 마킹
                        try:
                            runtime_state.mark_finished("chamber", self.ch)
                        except Exception:
                            pass

                        # 0) 재연결 선차단 + 폴링 완전 OFF
                        self._auto_connect_enabled = False
                        self._run_select = None
                        self._last_polling_targets = None
                        # 남아 있을 수 있는 폴링 스위치를 즉시 모두 내림(장치 내부 워치독 종료 유도)
                        self._apply_polling_targets({"mfc": False, "dc_pulse": False, "rf_pulse": False, "dc": False, "rf": False})

                        # 1) 이제 실제로 장치/워치독을 내려서 RS-232/TCP 점유 해제
                        self.append_log("MAIN", "공정 종료 → 모든 장치 연결 해제 및 워치독 중지")
                        try:
                            await self._stop_device_watchdogs(light=False)
                        except Exception as e:
                            self.append_log("MAIN", f"종료 정리 중 예외(무시): {e!r}")

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

                        if getattr(self, "_pending_device_cleanup", False):
                            with contextlib.suppress(Exception):
                                self._spawn_detached(self._stop_device_watchdogs(light=False), name="FullCleanup")
                            self._pending_device_cleanup = False
                            self._pc_stopping = False

                        self._pc_stopping = False
                        self._start_next_process_from_queue(ok)
                        self._last_polling_targets = None
                    except Exception as e:
                        self.append_log("MAIN", f"예외 발생 (finished 처리): {e}")
                        # 예외 시 안전하게 UI를 '대기 중'으로 복귀
                        with contextlib.suppress(Exception):
                            self._clear_queue_and_reset_ui()

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

                        if getattr(self, "_pending_device_cleanup", False):
                            with contextlib.suppress(Exception):
                                self._spawn_detached(self._stop_device_watchdogs(light=False), name="FullCleanup")
                            self._pending_device_cleanup = False
                            self._pc_stopping = False
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
                    self.data_logger.log_mfc_flow(gas, flow)
                self.append_log(f"MFC{self.ch}", f"[poll] {gas}: {flow:.2f} sccm")
            elif k == "pressure":
                txt = ev.text or (f"{ev.value:.3g}" if ev.value is not None else "")
                with contextlib.suppress(Exception):
                    self.data_logger.log_mfc_pressure(txt)
                self.append_log(f"MFC{self.ch}", f"[poll] ChamberP: {txt}")

    async def _pump_ig_events(self) -> None:
        async for ev in self.ig.events():
            k = ev.kind
            if k == "status":
                self.append_log(f"IG{self.ch}", ev.message or "")
            elif k == "pressure":
                try:
                    if ev.pressure is not None:
                        self.data_logger.log_ig_pressure(float(ev.pressure))
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
        async for ev in adapter.events():
            if ev.kind == "status":
                self.append_log(tag, ev.message or "")
            elif ev.kind == "data":
                def _draw_then_finish(x=ev.mass_axis, y=ev.pressures):
                    try:
                        self._graph_update_rga_safe(x, y)
                    finally:
                        self.process_controller.on_rga_finished()
                self._soon(_draw_then_finish)
            elif ev.kind == "finished":
                self.append_log(tag, ev.message or "scan finished")
                self.process_controller.on_rga_finished()
            elif ev.kind == "failed":
                why = ev.message or "RGA failed"
                self.append_log(tag, f"측정 실패: {why} → 다음 단계")
                if self.chat:
                    with contextlib.suppress(Exception):
                        self.chat.notify_text(f"[{tag}] 측정 실패: {why} → 건너뜀")
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
                self.append_log(f"DC{self.ch}", f"측정: {float(ev.power or 0.0):.1f} W, {float(ev.voltage or 0.0):.1f} V, {float(ev.current or 0.0):.2f} A")
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
                    self.data_logger.log_rf_power(fwd, ref)
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
                    self.data_logger.log_rfpulse_power(fwd, ref)
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
                    if not getattr(self, "_oes_active", False):
                        # 이전 런의 잔여 finished가 튀는 케이스 무시
                        self.append_log(f"OES{self.ch}", "이전 런 잔여 'finished' 이벤트 무시")
                        continue

                    ok = bool(getattr(ev, "success", False))
                    if ok:
                        self.append_log(f"OES{self.ch}", ev.message or "측정 완료")
                        self._oes_active = False
                        self.process_controller.on_oes_ok()
                    else:
                        why = getattr(ev, "message", "measure failed")
                        self.append_log(f"OES{self.ch}", f"측정 실패: {why} → 종료 절차로 전환")
                        self._oes_active = False
                        self.process_controller.on_oes_failed("OES", why)
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
                res = meth()
                if inspect.isawaitable(res):
                    await res
                if log:
                    self.append_log(label, f"{meth.__name__} 호출 완료")
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
        self._set("Power_edit",   f"{power:.3f}")
        self._set("Voltage_edit", f"{voltage:.3f}")
        self._set("Current_edit", f"{current:.3f}")

    def _on_process_status_changed(self, running: bool) -> None:
        b_start = self._u("Start_button"); b_stop = self._u("Stop_button")
        if b_start: b_start.setEnabled(not running)
        if b_stop: b_stop.setEnabled(bool(running))

        # ★ 변경점: running 값이 실제로 바뀐 경우에만 소유권 콜백 호출
        prev = getattr(self, "_last_running_state", None)
        if prev is None or prev != running:
            cb = getattr(self, "_notify_plc_owner", None)
            if callable(cb):
                try:
                    cb(self.ch if running else None)
                except Exception:
                    pass

        # True로 "전환"될 때만 점유 표시
        try:
            if running and prev is not True:
                runtime_state.set_running("chamber", True, self.ch)
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
        file_path = await self._aopen_file(
            caption=f"CH{self.ch} 프로세스 리스트 파일 선택",
            start_dir="",
            name_filter="CSV Files (*.csv);;All Files (*)"
        )
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
        
        # DC-Pulse
        _set("dcPulsePower_checkbox", params.get('use_dc_pulse', 'F') == 'T')
        _set("dcPulsePower_edit",     params.get('dc_pulse_power', '0'))
        dcf = str(params.get('dc_pulse_freq', '')).strip()
        dcd = str(params.get('dc_pulse_duty_cycle', '')).strip()
        _set("dcPulseFreq_edit",       '' if dcf in ('', '0') else dcf)
        _set("dcPulseDutyCycle_edit",  '' if dcd in ('', '0') else dcd)

        # DC-Power
        _set("dcPower_checkbox", params.get('use_dc_power', 'F') == 'T')
        _set("dcPower_edit", params.get('dc_power', '0'))

        # RF-Pulse
        _set("rfPulsePower_checkbox", params.get('use_rf_pulse', 'F') == 'T')
        _set("rfPulsePower_edit",     params.get('rf_pulse_power', '0'))
        rff = str(params.get('rf_pulse_freq', '')).strip()
        rfd = str(params.get('rf_pulse_duty_cycle', '')).strip()
        _set("rfPulseFreq_edit",       '' if rff in ('', '0') else rff)
        _set("rfPulseDutyCycle_edit",  '' if rfd in ('', '0') else rfd)

        # RF-Power
        _set("rfPower_checkbox", params.get('use_rf_power', 'F') == 'T')
        _set("rfPower_edit",     params.get('rf_power', '0'))

        _set("processTime_edit", params.get('process_time', '0'))
        _set("integrationTime_edit", params.get('integration_time', '60'))
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
                norm = self._normalize_params_for_process(params)
                
                # 입력값 검증
                errs = self._validate_norm_params(norm)
                if errs:
                    self.append_log("Validate", "CSV 공정 파라미터 오류:\n - " + "\n - ".join(errs))
                    # 전체 자동 실행 중단
                    self._clear_queue_and_reset_ui()
                    return

                # 새 스텝마다 이전 파일을 정리하고, 항상 새로운 파일로 시작
                try:
                    self._spawn_detached(self._shutdown_log_writer())
                except Exception:
                    pass
                self._log_file_path = None

                # (NEW) 최근 'chamber' 종료 시각 기준 쿨다운을 반영해서 다음 스텝 대기
                try:
                    remain = float(runtime_state.remaining_cooldown("chamber", self.ch, 60.0))
                except Exception:
                    remain = 0.0

                # 🚫 첫 번째 스텝(인덱스 0)은 강제 60초 대기 없이 즉시 시작
                first_step = (self.current_process_index == 0)
                delay_s = (remain if first_step else max(60.0, remain))

                # 지연이 없으면 바로 시작 예약
                if delay_s <= 0.0:
                    self._set_state_text("다음 공정 즉시 시작")
                    self._cancel_delay_task()
                    self._set_task_later(
                        "_delay_main_task",
                        self._start_process_later(params, 0.0, reason="즉시 시작"),
                        name=f"NextProcDelay.CH{self.ch}"
                    )
                    return

                # 지연 필요 시: 첫 스텝이면 '최근 종료로 인한 대기', 이후 스텝은 '쿨다운 대기'
                reason = ("최근 종료로 인한 대기" if first_step else "쿨다운 대기")
                self._set_state_text(f"다음 공정 대기중 ({reason}) · 남은 시간 {self._fmt_hms(delay_s)}")

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

    def _safe_start_process(self, params: NormParams) -> None:
        if self.process_controller.is_running:
            self.append_log("MAIN", "이미 다른 공정 실행 중"); return
        self._spawn_detached(self._start_after_preflight(params), store=True, name=f"StartAfterPreflight.CH{self.ch}")

    async def _start_after_preflight(self, params: NormParams) -> None:
        try:
            # 시작 시도 직전에만 허용
            self._auto_connect_enabled = True

            # ⬇️ 추가: 이전 런의 잔여 종료 플래그를 명시적으로 클리어
            self._pc_stopping = False
            self._pending_device_cleanup = False
            
            # ✅ 이번 런에서 실제로 사용할 펄스만 표시(IG/MFC는 항상 연결이므로 제외)
            use_dc_pulse = bool(params.get("use_dc_pulse", False)) and self.supports_dc_pulse
            use_rf_pulse = bool(params.get("use_rf_pulse", False)) and self.supports_rf_pulse
            self._run_select = {
                "dc_pulse": use_dc_pulse,
                "rf_pulse": use_rf_pulse,
            }

            # ✅ 이번 런에서 DC-Pulse를 쓸 거면: 엔드포인트 지정 + 즉시 재연결
            if use_dc_pulse and self.dc_pulse:
                host, port = self.cfg.DCPULSE_TCP
                await self.dc_pulse.set_endpoint_reconnect(host, port)

            self._ensure_background_started()
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
                    runtime_state.mark_finished("chamber", self.ch)
                except Exception:
                    pass

                self._start_next_process_from_queue(False)
                return
            
            # ★ 추가: 공정 시작 직전 Chuck 위치 선행 설정
            ok_chuck = await self._set_chuck_position_if_needed(params)
            if not ok_chuck:
                note = params.get("process_note", "알 수 없는")
                self.append_log("MAIN", f"Chuck 위치 설정 실패 → '{note}' 시작 중단")
                self._post_critical("Chuck 이동 실패", f"'{note}' 시작 중단 (chuck_position='{params.get('chuck_position')}')")
                # 자동/단일 모두 동일 경로로 중단 처리(기존 시작 실패 처리와 동일하게)
                self._start_next_process_from_queue(False)
                self._on_process_status_changed(False)
                return

            self._last_polling_targets = None
            self.append_log("MAIN", "장비 연결 확인 완료 → 공정 시작")
            self.process_controller.start_process(params)

        except Exception as e:
            note = params.get("process_note", "알 수 없는")
            msg = f"오류: '{note}' 시작 실패. ({e})"
            self.append_log("MAIN", msg)
            self._post_critical("오류", msg)
            self._start_next_process_from_queue(False)
            self._on_process_status_changed(False)

    async def _wait_device_connected(self, dev: object, name: str, timeout_s: float) -> bool:
        try: t0 = asyncio.get_running_loop().time()
        except RuntimeError: t0 = 0.0
        while True:
            if self._is_dev_connected(dev):
                return True
            try: now = asyncio.get_running_loop().time()
            except RuntimeError: now = t0 + timeout_s + 1.0
            if now - t0 >= timeout_s:
                self.append_log(name, "연결 확인 실패(타임아웃)")
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
        return (len(failed) == 0, failed)
    
    async def _set_chuck_position_if_needed(self, params: Mapping[str, Any]) -> bool:
        """
        레시피에 chuck_position 값이 있으면(공란 제외) 공정 시작 전에 1회만 Chuck 위치를 조정.
        - up   : Z_M_P_{CH}_CW_SW  펄스 → 60초 대기 → Z{CH}_UP   읽어 True 확인
        - mid  : Z_M_P_{CH}_MID_SW 펄스 → 60초 대기 → Z{CH}_MID  읽어 True 확인
        - down : Z_M_P_{CH}_CCW_SW 펄스 → 60초 대기 → Z{CH}_DOWN 읽어 True 확인
        실패 시 False 반환(공정 시작 중단).
        """
        pos = str(params.get("chuck_position") or "").strip().lower()
        if not pos:
            # 공란이면 스킵
            return True

        ch = 1 if int(getattr(self, "ch", 1)) != 2 else 2
        # 스위치/확인 비트 명칭 매핑
        if pos == "up":
            sw_name  = f"Z_M_P_{ch}_CW_SW"
            lamp_bit = f"Z{ch}_UP"
        elif pos == "mid":
            sw_name  = f"Z_M_P_{ch}_MID_SW"
            lamp_bit = f"Z{ch}_MID"
        elif pos == "down":
            sw_name  = f"Z_M_P_{ch}_CCW_SW"
            lamp_bit = f"Z{ch}_DOWN"
        else:
            self.append_log("PLC", f"[CH{self.ch}] 알 수 없는 chuck_position='{pos}' → 스킵")
            return True

        try:
            if not self.plc:
                self.append_log("PLC", f"[CH{self.ch}] PLC 미연결 상태 → Chuck 제어 불가")
                return False

            self.append_log("PLC", f"[CH{self.ch}] Chuck '{pos}' 설정: {sw_name} 펄스 전송")
            # 순간 스위치(펄스) 사용 — 내부에서 momentary로 처리
            if hasattr(self.plc, "press_switch"):
                await self.plc.press_switch(sw_name)
            else:
                # 구버전 호환(모멘터리 지원 안 되면 강제로 True→pulse_ms→False)
                await self.plc.write_switch(sw_name, True, momentary=True)

            # 규격: 60초 고정 대기 후 램프 확인(요청사항 준수)
            await asyncio.sleep(60.0)

            ok = await self.plc.read_bit(lamp_bit)
            if ok:
                self.append_log("PLC", f"[CH{self.ch}] Chuck '{pos}' 확인 성공 ({lamp_bit}=True)")
                return True
            else:
                self.append_log("PLC", f"[CH{self.ch}] Chuck '{pos}' 확인 실패: {lamp_bit}=False")
                return False

        except Exception as e:
            self.append_log("PLC", f"[CH{self.ch}] Chuck 위치 설정/확인 중 예외: {e!r}")
            return False

    # ------------------------------------------------------------------
    # Start/Stop (개별 챔버)
    def _handle_start_clicked(self, _checked: bool = False):
        # ✅ 전역 runtime_state 기준 60초 쿨다운
        remain = runtime_state.remaining_cooldown("chamber", self.ch, cooldown_s=60.0)
        if remain > 0.0:
            secs = int(remain + 0.999)
            self._post_warning("대기 필요", f"이전 공정 종료 후 1분 대기 필요합니다.\n{secs}초 후에 시작하십시오.")
            return
        
        # ★ 추가: 장치 정리가 백그라운드에서 진행 중이면 대기 안내
        if getattr(self, "_pending_device_cleanup", False):
            self._post_warning("정리 중", "이전 공정 정리 중입니다. 잠시 후 다시 시작하세요.")
            return
        
        # ★ 추가(권장): 이미 다음 공정이 예약되어 있으면 Start 재클릭은 무시하고 안내
        t = getattr(self, "_delay_main_task", None)
        if t is not None and not t.done():
            self._post_warning("대기 중", "다음 공정이 예약되어 있습니다. 카운트다운 종료 후 자동 시작합니다.")
            return

        # ✅ 교차 실행 차단: 해당 챔버가 이미 다른 런타임(CH/PC/TSP)에서 점유 중이면 시작 금지
        if runtime_state.is_running("chamber", self.ch):
            self._post_warning("실행 오류", f"CH{self.ch}는 이미 다른 공정이 실행 중입니다.")
            return

        if self.process_controller.is_running:
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
            return

        try:
            base_pressure = float(self._get_text("basePressure_edit") or 1e-5)
            integration_time = int(self._get_text("integrationTime_edit") or 60)
            working_pressure = float(self._get_text("workingPressure_edit") or 0.0)
            shutter_delay = float(self._get_text("shutterDelay_edit") or 0.0)
            process_time = float(self._get_text("processTime_edit") or 0.0)
        except ValueError:
            self.append_log("UI", "오류: 값 입력란을 확인해주세요."); return

        params: dict[str, Any] = {
            "base_pressure": base_pressure,
            "integration_time": integration_time,
            "working_pressure": working_pressure,
            "shutter_delay": shutter_delay,
            "process_time": process_time,
            "process_note": f"Single CH{self.ch}",
            **vals,

            # ✅ Start 버튼 "누른" 시각 (tz 없이, 초 단위)
            "t0_pressed_wall": datetime.now().isoformat(timespec="seconds"),
            "t0_pressed_ns":   time.monotonic_ns(),
        }
        errs = self._validate_norm_params(cast(NormParams, params))
        if errs:
            self._post_warning("입력값 확인", "\n".join(f"- {e}" for e in errs))
            return  

        params["G1 Target"] = vals.get("G1_target_name", "")
        params["G2 Target"] = vals.get("G2_target_name", "")
        params["G3 Target"] = vals.get("G3_target_name", "")

        # ❌ 여기서는 파일을 열지 않습니다. (started 이벤트에서 1회 오픈)
        self.append_log("MAIN", "입력 검증 통과 → 장비 연결 확인 시작")
        self._safe_start_process(cast(NormParams, params))

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
        if getattr(self, "_pc_stopping", False):
            self.append_log("MAIN", "정지 요청 무시: 이미 종료 절차 진행 중"); return
        
        # Stop 이후엔 자동 재연결 차단(사용자가 Start로 다시 올릴 때까지)
        self._auto_connect_enabled = False
        self._run_select = None

        # 라이트 정리: 출력/폴링 OFF
        self._spawn_detached(self._stop_device_watchdogs(light=True))

        self._pc_stopping = True
        self._pending_device_cleanup = True
        self.process_controller.request_stop()

        # ✅ 백업 타이머: 30초 내 미종료 시 헤비 강제
        async def _fallback():
            try:
                await asyncio.sleep(30)
                if self._pc_stopping and self._pending_device_cleanup:
                    self.append_log("MAIN", "STOP fallback → heavy cleanup")
                    await self._stop_device_watchdogs(light=False)

                    # ✅ 전역 종료 시각 마킹(이벤트가 오지 않은 강제 경로 보완)
                    try:
                        runtime_state.mark_finished("chamber", self.ch)
                    except Exception:
                        pass

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
        
        # ✅ heavy 시작 직후도 한 번 더 OFF
        with contextlib.suppress(Exception):
            if self.mfc and hasattr(self.mfc, "on_process_finished"):
                # 호출 시 폴링과 내부 플래그를 초기화
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
        try:
            current = asyncio.current_task()
            live = [t for t in getattr(self, "_bg_tasks", []) if t and not t.done() and t is not current]
            for t in live: loop.call_soon(t.cancel)
            if live: await asyncio.gather(*live, return_exceptions=True)
        finally:
            self._bg_tasks = []

        try:
            if self.ig and hasattr(self.ig, "cancel_wait"):
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.ig.cancel_wait(), timeout=2.0)
        except Exception:
            pass

        tasks = []
        for dev in (self.ig, self.mfc, self.dc_pulse, self.rf_pulse, self.dc_power, self.rf_power, self.oes, self.rga):
            if dev and hasattr(dev, "cleanup"):
                try: tasks.append(dev.cleanup())
                except Exception: pass
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

        self._bg_started = False
        self._devices_started = False  # ✅ 다음 시작 때 장치 start() 다시 보장
        self._run_select = None

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
                self._post_warning("선택 오류", "가스를 하나 이상 선택해야 합니다."); return None

            def _read_flow(name: str) -> float:
                txt = self._get_text(name) or "0"
                try:
                    v = float(txt)
                    if v < 0: raise ValueError()
                    return v
                except ValueError:
                    raise

            try:
                ar_flow = _read_flow("arFlow_edit") if use_ar else 0.0
                o2_flow = _read_flow("o2Flow_edit") if use_o2 else 0.0
                n2_flow = _read_flow("n2Flow_edit") if use_n2 else 0.0
            except Exception:
                self._post_warning("입력값 확인", "가스 유량 입력을 확인하세요.")
                return None

            use_dc_pulse = bool(getattr(self._u("dcPulsePower_checkbox"), "isChecked", lambda: False)())
            if not use_dc_pulse:
                self._post_warning("선택 오류", "CH1은 DC-Pulse를 반드시 선택해야 합니다.")
                return None

            try:
                dc_pulse_power = float(self._get_text("dcPulsePower_edit") or "0")
                if dc_pulse_power <= 0: raise ValueError()
            except ValueError:
                self._post_warning("입력값 확인", "DC-Pulse Target Power(W)를 확인하세요.")
                return None

            dc_pulse_freq = None
            dc_pulse_duty = None
            # kHz 입력
            txtf = self._get_text("dcPulseFreq_edit")
            if txtf:
                try:
                    dc_pulse_freq = int(float(txtf))  # kHz
                    if dc_pulse_freq < 20 or dc_pulse_freq > 150:  # EnerPulse: 20~150 kHz
                        raise ValueError()
                except ValueError:
                    self._post_warning("입력값 확인", "DC-Pulse Freq(kHz)는 20..150 범위입니다.")
                    return None
            txtd = self._get_text("dcPulseDutyCycle_edit")
            if txtd:
                try:
                    dc_pulse_duty = int(float(txtd))
                    if dc_pulse_duty < 1 or dc_pulse_duty > 99: raise ValueError()
                except ValueError:
                    self._post_warning("입력값 확인", "DC-Pulse Duty(%)는 1..99 범위")
                    return None

            g1n = self._get_text("g1Target_name")
            g2n = self._get_text("g2Target_name")
            g3n = self._get_text("g3Target_name")

            return {
                "use_ms": bool(getattr(self._u("mainShutter_checkbox"), "isChecked", lambda: False)()),
                "use_g1": False, "use_g2": False, "use_g3": False,
                "use_ar": use_ar, "use_o2": use_o2, "use_n2": use_n2,
                "ar_flow": ar_flow, "o2_flow": o2_flow, "n2_flow": n2_flow,
                "use_rf_power": False,
                "use_dc_power": False,
                "use_dc_pulse": True,  "dc_pulse_power": dc_pulse_power,
                "dc_pulse_freq": dc_pulse_freq, "dc_pulse_duty": dc_pulse_duty,
                "use_rf_pulse": False, "rf_pulse_power": 0.0,
                "G1_target_name": g1n, "G2_target_name": g2n, "G3_target_name": g3n,
                "use_power_select": bool(getattr(self._u("powerSelect_checkbox"), "isChecked", lambda: False)()),
            }

        use_g1 = bool(getattr(self._u("G1_checkbox"), "isChecked", lambda: False)())
        use_g2 = bool(getattr(self._u("G2_checkbox"), "isChecked", lambda: False)())
        use_g3 = bool(getattr(self._u("G3_checkbox"), "isChecked", lambda: False)())
        checked = int(use_g1) + int(use_g2) + int(use_g3)
        if checked == 0 or checked == 3:
            self._post_warning("선택 오류", "G1~G3 중 1개 또는 2개만 선택")
            return None

        g1_name = self._get_text("g1Target_name")
        g2_name = self._get_text("g2Target_name")
        g3_name = self._get_text("g3Target_name")
        
        use_ar = bool(getattr(self._u("Ar_checkbox"), "isChecked", lambda: False)())
        use_o2 = bool(getattr(self._u("O2_checkbox"), "isChecked", lambda: False)())
        use_n2 = bool(getattr(self._u("N2_checkbox"), "isChecked", lambda: False)())
        if not (use_ar or use_o2 or use_n2):
            self._post_warning("선택 오류", "가스를 하나 이상 선택"); return None

        def _flow(name: str) -> float:
            txt = self._get_text(name); 
            if not txt: return 0.0
            v = float(txt); 
            if v < 0: raise ValueError()
            return v

        try:
            ar_flow = _flow("arFlow_edit") if use_ar else 0.0
            o2_flow = _flow("o2Flow_edit") if use_o2 else 0.0
            n2_flow = _flow("n2Flow_edit") if use_n2 else 0.0
        except Exception:
            self._post_warning("입력값 확인", "가스 유량을 확인하세요."); return None

        use_rf_pulse = bool(getattr(self._u("rfPulsePower_checkbox"), "isChecked", lambda: False)())
        use_dc       = bool(getattr(self._u("dcPower_checkbox"), "isChecked", lambda: False)())
        use_rf_power = bool(getattr(self._u("rfPower_checkbox"), "isChecked", lambda: False)())

        # 최소 한 가지 파워는 선택되어야 함 (RF Pulse, RF Power, DC)
        if not (use_rf_pulse or use_rf_power or use_dc):
            self._post_warning("선택 오류", "RF Pulse, RF Power, DC 중 하나 이상 선택"); 
            return None

        rf_pulse_power = 0.0; rf_pulse_freq = None; rf_pulse_duty = None
        if use_rf_pulse:
            try:
                rf_pulse_power = float(self._get_text("rfPulsePower_edit") or "0")
                if rf_pulse_power <= 0: raise ValueError()
            except ValueError:
                self._post_warning("입력값 확인", "RF Pulse Target Power(W)를 확인하세요."); return None
            # kHz 입력
            txtf = self._get_text("rfPulseFreq_edit")
            if txtf:
                try:
                    rf_pulse_freq = int(float(txtf))  # kHz
                    if rf_pulse_freq < 1 or rf_pulse_freq > 100:
                        raise ValueError()
                except ValueError:
                    self._post_warning("입력값 확인", "RF Pulse Freq(kHz)는 1..100 범위입니다.")
                    return None
            txtd = self._get_text("rfPulseDutyCycle_edit")
            if txtd:
                try:
                    rf_pulse_duty = int(float(txtd))
                    if rf_pulse_duty < 1 or rf_pulse_duty > 99: raise ValueError()
                except ValueError:
                    self._post_warning("입력값 확인", "RF Pulse Duty(%) 1..99"); return None

        if use_dc:
            try:
                dc_power = float(self._get_text("dcPower_edit") or "0")
                if dc_power <= 0: raise ValueError()
            except ValueError:
                self._post_warning("입력값 확인", "DC 파워(W)를 확인하세요."); return None
        else:
            dc_power = 0.0

        use_rf_power = bool(getattr(self._u("rfPower_checkbox"), "isChecked", lambda: False)())
        rf_power_val = 0.0
        if use_rf_power:
            try:
                rf_power_val = float(self._get_text("rfPower_edit") or "0")
                if rf_power_val <= 0: raise ValueError()
            except ValueError:
                self._post_warning("입력값 확인", "RF Power(W)를 확인하세요.")
                return None

        # 허용/금지 조합 체크
        if not (use_rf_pulse or use_dc or use_rf_power):
            self._post_warning("선택 오류", "RF Pulse, RF Power, DC 중 하나 이상 선택")
            return None

        # RF Pulse와 RF Power 동시 금지
        if use_rf_pulse and use_rf_power:
            self._post_warning("선택 오류", "RF Pulse와 RF Power는 동시에 선택할 수 없습니다.")
            return None

        return {
            "use_ms": bool(getattr(self._u("mainShutter_checkbox"), "isChecked", lambda: False)()),
            "use_g1": use_g1, "use_g2": use_g2, "use_g3": use_g3,
            "use_ar": use_ar, "use_o2": use_o2, "use_n2": use_n2,
            "ar_flow": ar_flow, "o2_flow": o2_flow, "n2_flow": n2_flow,
            "use_rf_power": use_rf_power,"rf_power": rf_power_val, 
            "use_rf_pulse": use_rf_pulse, "use_dc_power": use_dc,
            "rf_pulse_power": rf_pulse_power, "dc_power": dc_power,
            "rf_pulse_freq": rf_pulse_freq, "rf_pulse_duty": rf_pulse_duty,
            "G1_target_name": g1_name, "G2_target_name": g2_name, "G3_target_name": g3_name,
            "use_power_select": bool(getattr(self._u("powerSelect_checkbox"), "isChecked", lambda: False)()),
        }

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

        res: NormParams = {
            "base_pressure":     fget("base_pressure", "1e-5"),
            "working_pressure":  fget("working_pressure", "0"),
            "process_time":      fget("process_time", "0"),
            "shutter_delay":     fget("shutter_delay", "0"),
            "integration_time":  iget("integration_time", "60"),
            "dc_power":          fget("dc_power", "0"),
            "rf_power":          fget("rf_power", "0"),

            "use_dc_pulse":      tf(raw.get("use_dc_pulse", "F")),
            "dc_pulse_power":    fget("dc_pulse_power", "0"),
            "dc_pulse_freq":     iget_opt("dc_pulse_freq"),
            "dc_pulse_duty":     iget_opt("dc_pulse_duty_cycle"),

            "use_rf_pulse":      tf(raw.get("use_rf_pulse", "F")),
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
            "G1_target_name":    g1t, "G2_target_name": g2t, "G3_target_name": g3t,
            "G1 Target":         g1t, "G2 Target": g2t, "G3 Target": g3t,
            "use_power_select":  tf(raw.get("power_select", "F")),

            # ★ 추가
            "chuck_position":    _pos,
        }

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

        self._soon(self._append_log_to_ui, line_ui)

        if not getattr(self, "_log_file_path", None):
            self._soon(self._prestart_buf.append, line_file)
            return
        self._soon(self._log_enqueue_nowait, line_file)

    def _append_log_to_ui(self, line: str) -> None:
        if not self._w_log: return
        self._w_log.moveCursor(QTextCursor.MoveOperation.End)
        self._w_log.insertPlainText(line + "\n")

    def _ensure_log_dir(self, root: Path) -> Path:
        nas_path = Path(root)
        local_fallback = Path.cwd() / f"_Logs_local_CH{self.ch}"
        try:
            nas_path.mkdir(parents=True, exist_ok=True)
            return nas_path
        except Exception:
            local_fallback.mkdir(parents=True, exist_ok=True)
            if self._w_log:
                self._w_log.appendPlainText(f"[Logger] NAS 폴더 접근 실패 → 로컬 폴백: {local_fallback}")
            return local_fallback

    def _prepare_log_file(self, params: Mapping[str, Any]) -> None:
        now_local = datetime.now()
        ts = now_local.strftime("%Y%m%d_%H%M%S")

        base = self._log_dir / f"CH{self.ch}_{ts}"
        path = base.with_suffix(".txt")
        i = 1
        while path.exists():
            path = (self._log_dir / f"CH{self.ch}_{ts}_{i}").with_suffix(".txt")
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
        # 1) 고유 파일경로 계산 (아직 self._log_file_path 노출 X)
        now_local = datetime.now()
        ts = now_local.strftime("%Y%m%d_%H%M%S")

        base = (self._log_dir / f"CH{self.ch}_{ts}").with_suffix(".txt")
        path = base
        i = 1
        while path.exists():
            path = (self._log_dir / f"CH{self.ch}_{ts}_{i}").with_suffix(".txt")
            i += 1

        # 2) 우선 파일을 열어서 헤더를 '먼저' 기록 (line-buffering 권장)
        fp = open(path, "a", encoding="utf-8", newline="", buffering=1)
        try:
            name = (params.get("process_note")
                    or params.get("Process_name")
                    or f"Run CH{self.ch}")
            fp.write("# ==== Sputter Run ====\n")
            fp.write(f"# started_at = {datetime.now().isoformat()}\n")
            fp.write(f"# chamber = CH{self.ch}\n")
            fp.write(f"# process_name = {name}\n")
            if "process_time" in params:
                fp.write(f"# time_min = {float(params.get('process_time', 0) or 0):.2f}\n")
            fp.write("# ============================\n")
            fp.flush()
        finally:
            # 3) 이제야 경로/핸들을 '노출' → 이 시점부터 writer가 파일에 씀
            self._log_file_path = path
            self._log_fp = fp
            if not self._log_writer_task or self._log_writer_task.done():
                self._set_task_later("_log_writer_task", self._log_writer_loop(), name=f"LogWriter.CH{self.ch}")

            # 4) pre-start 버퍼를 파일에 옮긴 뒤 비운다(초반 상황도 기록 보존)
            with contextlib.suppress(Exception):
                for line in list(self._prestart_buf):
                    self._log_enqueue_nowait(line)
                self._prestart_buf.clear()

        self.append_log("Logger", f"새 로그 파일 시작: {path.name}")

    def _close_run_log(self) -> None:
        """현재 열려 있는 로그 파일을 명확히 종료(END 기록 + 핸들 닫기 + 경로 차단)."""
        fp = getattr(self, "_log_fp", None)
        # 이후 append_log가 파일을 다시 열지 못하도록 경로/핸들을 먼저 끊는다
        self._log_fp = None
        self._log_file_path = None
        if fp:
            with contextlib.suppress(Exception):
                fp.write("# ==== END ====\n")
                fp.flush()
                fp.close()

    def _log_enqueue_nowait(self, line: str) -> None:
        try:
            self._log_q.put_nowait(line)
        except asyncio.QueueFull:
            with contextlib.suppress(Exception):
                _ = self._log_q.get_nowait()
                self._log_q.put_nowait(line)

    async def _log_writer_loop(self):
        try:
            while True:
                try:
                    line = self._log_q.get_nowait()
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.05)
                    continue

                if self._log_fp is None:
                    if self._log_file_path:
                        try:
                            # 부모 디렉터리 보장
                            self._log_file_path.parent.mkdir(parents=True, exist_ok=True)
                            self._log_fp = open(self._log_file_path, "a", encoding="utf-8", newline="")
                        except Exception as e:
                            # NAS 열기 실패 → 로컬 폴백으로 즉시 전환
                            try:
                                local_dir = Path.cwd() / f"_Logs_local_CH{self.ch}"
                                local_dir.mkdir(parents=True, exist_ok=True)
                                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                                self._log_file_path = (local_dir / f"CH{self.ch}_{ts}_recovered.txt")
                                self._log_fp = open(self._log_file_path, "a", encoding="utf-8", newline="")
                                self.append_log("Logger", f"NAS 로그 열기 실패({e!r}) → 로컬 폴백: {self._log_file_path}")
                            except Exception:
                                await asyncio.sleep(0.2)
                                self._soon(self._log_enqueue_nowait, line)
                                continue
                    else:
                        continue
                try:
                    self._log_fp.write(line); self._log_fp.flush()
                except Exception as e:
                    # 쓰기 도중 핸들이 깨진 경우 → 즉시 로컬로 전환 시도
                    with contextlib.suppress(Exception):
                        self._log_fp.close()
                    self._log_fp = None
                    try:
                        local_dir = Path.cwd() / f"_Logs_local_CH{self.ch}"
                        local_dir.mkdir(parents=True, exist_ok=True)
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        self._log_file_path = (local_dir / f"CH{self.ch}_{ts}_recovered.txt")
                        self._log_fp = open(self._log_file_path, "a", encoding="utf-8", newline="")
                        self.append_log("Logger", f"NAS 쓰기 실패({e!r}) → 로컬 폴백 전환: {self._log_file_path}")
                    except Exception:
                        await asyncio.sleep(0.2)
                        self._soon(self._log_enqueue_nowait, line)
        except asyncio.CancelledError:
            pass
        finally:
            # TextIOWrapper 소멸자에서 noisy 에러가 나지 않도록
            fp, self._log_fp = self._log_fp, None
            if fp:
                with contextlib.suppress(Exception):
                    fp.flush()
                with contextlib.suppress(Exception):
                    fp.close()

    async def _shutdown_log_writer(self):
        if self._log_writer_task:
            self._log_writer_task.cancel()
            with contextlib.suppress(Exception):
                await self._log_writer_task
            self._log_writer_task = None

        if self._log_fp:
            with contextlib.suppress(Exception): self._log_fp.flush()
            with contextlib.suppress(Exception): self._log_fp.close()
        self._log_fp = None

        # ★ 다음 런으로 누수되는 줄 차단
        self._log_q = asyncio.Queue(maxsize=4096)

    def _clear_queue_and_reset_ui(self) -> None:
        # 전역 runtime_state로 종료 시각을 기록하므로 로컬 타임스탬프는 불필요
        # ★ 추가: 남아 있을 수 있는 카운트다운 태스크 정리
        self._cancel_delay_task()

        self.current_process_index = -1
        self._reset_ui_after_process()

        with contextlib.suppress(Exception):
            self._close_run_log()

        with contextlib.suppress(Exception):
            self._spawn_detached(self._shutdown_log_writer())

        self._log_file_path = None
        with contextlib.suppress(Exception):
            self._prestart_buf.clear()

        try: 
            self._prestart_buf.clear()
        except Exception: 
            pass

    # ------------------------------------------------------------------
    # 기본 UI값/리셋
    def _set_default_ui_values(self) -> None:
        _set = self._set
        
        _set("integrationTime_edit", "60")
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

    # ======= 서버 통신을 통한 실행 api =======
    async def start_with_recipe_string(self, recipe: str) -> None:
        """
        외부 제어(Host) 진입점.
        - recipe == "" 또는 None: 현재 UI 값으로 단발 실행(버튼 클릭과 동일)
        - recipe 가 .csv 경로: 파일을 읽어 process_queue를 구성하고 자동공정 시작
        """
        s = (recipe or "").strip()
        if not s:
            # 현재 UI 값으로 단일 공정 실행 (Start 버튼 경로 재사용)
            self._handle_start_clicked(False)
            return

        # CSV 경로 케이스
        if s.lower().endswith(".csv"):
            if not os.path.exists(s):
                raise RuntimeError(f"CSV 파일을 찾을 수 없습니다: {s}")

            try:
                # ▼ _handle_process_list_clicked_async() 내부 로직을 그대로 재사용
                with open(s, mode='r', encoding='utf-8-sig', newline='') as csvfile:
                    reader = csv.DictReader(csvfile)
                    self.process_queue: list[RawParams] = []
                    self.current_process_index: int = -1
                    for row in reader:
                        name = (row.get('Process_name') or row.get('#') or f"공정 {len(self.process_queue)+1}").strip()
                        row['Process_name'] = name
                        self.process_queue.append(cast(RawParams, row))

                if not self.process_queue:
                    raise RuntimeError("CSV에 공정 데이터가 없습니다.")

                # 첫 행을 UI에 적용(기존 함수 재사용)
                self._update_ui_from_params(self.process_queue[0])
                self.append_log("File", f"CSV 로드 완료: {s} (총 {len(self.process_queue)}개)")

                # 버튼 클릭과 동일 경로로 시작
                self._handle_start_clicked(False)
                return
            except Exception as e:
                # Host 응답에 그대로 전달되므로 예외 메시지를 명확하게
                raise RuntimeError(f"CSV 로드 실패: {e!s}")

        # 지원 포맷은 CSV 뿐
        raise RuntimeError("지원하지 않는 레시피 형식입니다. CSV 경로만 허용됩니다.")

    # ------------------------------------------------------------------
    # 유틸
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
        loop = self._loop
        def _create_and_set():
            t = loop.create_task(coro, name=name)
            setattr(self, attr_name, t)
        try: running = asyncio.get_running_loop()
        except RuntimeError: running = None
        if running is loop: loop.call_soon(_create_and_set)
        else: loop.call_soon_threadsafe(_create_and_set)

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
        try:
            while not stop_evt.is_set():
                missing = [name for name, dev in need if not self._is_dev_connected(dev)]
                txt = ", ".join(missing) if missing else "모두 연결됨"
                self.append_log("MAIN", f"연결 대기 중: {txt}")
                await asyncio.wait_for(stop_evt.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass
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
            # => prefix('ch1_') + 'gunTarget_name' == 'ch1_gunTarget_name'
            "g1Target_name": "gunTarget_name",
            "g2Target_name": "gunTarget_name",
            "g3Target_name": "gunTarget_name",
        }.get(leaf, leaf)

    def _u(self, name: str) -> Any | None:
        """prefix+name 위젯을 가져온다. 없으면 None."""
        name = self._alias_leaf(name)
        if not getattr(self, "ui", None):
            return None
        return getattr(self.ui, f"{self.prefix}{name}", None)

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

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[str] = loop.create_future()

        def _done(result: int):
            try:
                if result == QDialog.Accepted and dlg.selectedFiles():
                    fut.set_result(dlg.selectedFiles()[0])
                else:
                    fut.set_result("")  # 취소
            finally:
                dlg.deleteLater()

        dlg.finished.connect(_done)
        dlg.open()
        return await fut

    def _ensure_msgbox_store(self):
        if not hasattr(self, "_msg_boxes"):
            self._msg_boxes = []

    def _post_warning(self, title: str, text: str) -> None:
        if not self._has_ui():
            self.append_log("WARN", f"{title}: {text}"); return

        self._ensure_msgbox_store()
        box = QMessageBox(self._parent_widget() or None)
        box.setWindowTitle(title)
        box.setText(text)
        box.setIcon(QMessageBox.Warning)
        box.setStandardButtons(QMessageBox.Ok)
        box.setWindowModality(Qt.WindowModality.WindowModal)  # 부모 창 기준 모달
        box.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)

        # 참조 유지 & 종료 시 정리
        self._msg_boxes.append(box)
        def _cleanup(_res: int):
            with contextlib.suppress(ValueError):
                self._msg_boxes.remove(box)
            box.deleteLater()
        box.finished.connect(_cleanup)

        box.open()  # 비모달(이벤트 루프 방해 없음)

    def _post_critical(self, title: str, text: str) -> None:
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
        for k in ("ar_flow","o2_flow","n2_flow"):
            v = float(p.get(k, 0) or 0)
            if v < 0:
                errs.append(f"{k}는 음수 불가")

        if self.ch == 1:
            # CH1 규칙
            if not p.get("use_dc_pulse"):
                errs.append("CH1은 DC-Pulse를 반드시 선택해야 합니다.")
            if p.get("dc_pulse_power", 0) <= 0:
                errs.append("DC-Pulse Target Power(W) > 0 필요")
            f = p.get("dc_pulse_freq")
            d = p.get("dc_pulse_duty")
            if f is not None and not (20 <= f <= 150):
                errs.append("DC-Pulse Freq(kHz)는 20..150")
            if d is not None and not (1 <= d <= 99):
                errs.append("DC-Pulse Duty(%)는 1..99")
        else:
            # CH2 규칙(기존 싱글런 로직과 동일)
            checked = int(p.get("use_g1", False)) + int(p.get("use_g2", False)) + int(p.get("use_g3", False))
            if checked == 0 or checked == 3:
                errs.append("G1~G3 중 1개 또는 2개만 선택")

            # 타겟 이름이 비어있어도 허용
            # if p.get("use_g1") and not p.get("G1_target_name"):
            #     errs.append("G1 타겟 이름이 비어있음")
            # if p.get("use_g2") and not p.get("G2_target_name"):
            #     errs.append("G2 타겟 이름이 비어있음")
            # if p.get("use_g3") and not p.get("G3_target_name"):
            #     errs.append("G3 타겟 이름이 비어있음")

            if not (p.get("use_rf_pulse") or p.get("use_dc_power") or p.get("use_rf_power")):
                errs.append("RF Pulse, RF Power, DC Power 중 하나 이상 선택 필요")

            # RF Pulse와 RF Power 동시 금지
            if p.get("use_rf_pulse") and p.get("use_rf_power"):
                errs.append("RF Pulse와 RF Power는 동시에 선택할 수 없습니다.")

            if p.get("use_rf_pulse"):
                if p.get("rf_pulse_power", 0) <= 0:
                    errs.append("RF Pulse Target Power(W) > 0 필요")
                f = p.get("rf_pulse_freq")
                d = p.get("rf_pulse_duty")
                if f is not None and not (1 <= f <= 100):
                    errs.append("RF Pulse Freq(kHz)는 1..100")
                if d is not None and not (1 <= d <= 99):
                    errs.append("RF Pulse Duty(%)는 1..99")
            if p.get("use_dc_power") and p.get("dc_power", 0) <= 0:
                errs.append("DC 파워(W) > 0 필요")

        return errs
    # ============================= 입력값 검증 헬퍼 =============================  

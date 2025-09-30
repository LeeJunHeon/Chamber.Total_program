# controller/chamber_runtime.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import csv, asyncio, contextlib, inspect, re, traceback
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Deque, Literal, Mapping, Optional, Sequence, TypedDict, cast, Union
from pathlib import Path
from datetime import datetime
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

# 공정 컨트롤러(기존 CH2) + CH1은 별도 모듈이 있으면 사용, 없으면 CH2를 공용으로
from controller.process_controller import ProcessController
from controller.plasma_cleaning_controller import PlasmaCleaningController

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
            int(self._get("IG_TCP_PORT", 4002 if self.ch == 1 else 4003)),
        )

    @property
    def MFC_TCP(self) -> tuple[str, int]:
        return (
            str(self._get("MFC_TCP_HOST", "192.168.1.50")),
            int(self._get("MFC_TCP_PORT", 4006 if self.ch == 1 else 4007)),
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
        supports_dc_cont: Optional[bool] = None,   # DC 연속
        supports_rf_cont: Optional[bool] = None,   # RF 연속
        supports_dc_pulse: Optional[bool] = None,  # DC-Pulse
        supports_rf_pulse: Optional[bool] = None,  # RF-Pulse
        owns_plc: Optional[bool] = None,   # ← 추가: PLC 로그 소유자
    ) -> None:
        self.ui = ui
        self.ch = int(chamber_no)
        self.prefix = str(prefix)
        self._loop = loop
        self.plc = plc
        self.chat = chat
        self.cfg = _CfgAdapter(cfg, self.ch)
        self._bg_tasks: list[asyncio.Task[Any]] = []
        self._starter_threads: dict[str, asyncio.Task] = {}
        self._bg_started = False
        self._pc_stopping = False
        self._pending_device_cleanup = False
        self._last_polling_targets: TargetsMap | None = None
        self._last_state_text: str | None = None
        self._delay_task: Optional[asyncio.Task] = None
        self._auto_connect_enabled = True  # ← 실패시 False로 내려 자동 재연결 차단
        self._run_select: dict[str, bool] | None = None  # ← 이번 런에서 펄스 선택 상태
        self._owns_plc = bool(owns_plc if owns_plc is not None else (int(chamber_no) == 1))  # 기본 CH1

        # QMessageBox 참조 저장소(비모달 유지용)
        self._msg_boxes: list[QMessageBox] = []  # ← 추가

        # 기본 전략: CH1=DC-Pulse 전용, CH2=RF-Pulse(+DC 연속)
        if supports_dc_cont  is None: supports_dc_cont  = (self.ch == 2)
        if supports_rf_cont  is None: supports_rf_cont  = False
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
        self._log_dir = self._ensure_log_dir(self._log_root)
        self._log_file_path: Path | None = None
        self._prestart_buf: Deque[str] = deque(maxlen=1000)
        self._log_fp = None
        self._log_q: asyncio.Queue[str] = asyncio.Queue(maxsize=4096)
        self._log_writer_task: asyncio.Task | None = None

        # 장치 인스턴스(각 챔버 독립)
        mfc_host, mfc_port = self.cfg.MFC_TCP
        ig_host,  ig_port  = self.cfg.IG_TCP

        self.mfc = AsyncMFC(host=mfc_host, port=mfc_port, enable_verify=False)
        self.ig  = AsyncIG(host=ig_host,  port=ig_port)

        self.oes = OESAsync(chamber=self.ch)

        # RGA: config에서 연결 정보 꺼내 생성(단일/채널별 모두 지원)
        self.rga = None  # type: ignore
        try:
            ip, user, pwd = self.cfg.rga_creds()
            if ip:
                self.rga = RGA100AsyncAdapter(ip, user=user, password=pwd, name=f"CH{self.ch}")
        except Exception:
            self.rga = None  # 안전

        # 펄스 파워(완전 분리)
        self.dc_pulse = AsyncDCPulse() if self.supports_dc_pulse else None
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
        if self.supports_rf_cont:
            async def _rf_send(power: float):
                await self.plc.power_apply(power, family="DCV", ensure_set=True, channel=1)

            async def _rf_send_unverified(power: float):
                await self.plc.power_write(power, family="DCV", write_idx=1)

            async def _rf_request_read():
                try:
                    fwd_raw = await self.plc.read_reg_name("DCV_READ_2")
                    ref_raw = await self.plc.read_reg_name("DCV_READ_3")
                    return {"forward": float(fwd_raw), "reflected": float(ref_raw)}
                except Exception as e:
                    self.append_log("RFpower", f"read failed: {e!r}")
                    return None

            self.rf_power = RFPowerAsync(
                send_rf_power=_rf_send,
                send_rf_power_unverified=_rf_send_unverified,
                request_status_read=_rf_request_read,
            )

        # === ProcessController 바인딩 ===
        self._bind_process_controller()

        # === UI 버튼 바인딩 (자기 챔버 것만) ===
        self._connect_my_buttons()

        # === 백그라운드 워치독/이벤트펌프 준비는 최초 Start 때 올림 ===
        self._on_process_status_changed(False)

    # ------------------------------------------------------------------
    # 공정 컨트롤러 바인딩
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
                    if self.chat:
                        with contextlib.suppress(Exception):
                            self.chat.notify_error_with_src("PLC", f"{nname}: {e}")
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
                    await self.dc_pulse.start()
                    await self.dc_pulse.prepare_and_start(power_w=float(power), freq=freq, duty=duty)
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
                    try:
                        if getattr(self.oes, "sChannel", -1) < 0:
                            ok = await self.oes.initialize_device()
                            if not ok:
                                raise RuntimeError("OES 초기화 실패")
                    except Exception as e:
                        self.append_log("OES", f"초기화 실패: {e!r} → 그래프 없이 다음 단계")
                        if self.chat:
                            with contextlib.suppress(Exception):
                                self.chat.notify_text(f"[OES] 초기화 실패: {e!r} → 건너뜀")
                        self.process_controller.on_oes_ok()
                        return

                    self._soon(self._safe_clear_oes_plot)

                    try:
                        await self.oes.run_measurement(duration_sec, integration_ms)
                    except Exception as e:
                        self.append_log("OES", f"측정 예외: {e!r} → 다음 단계")
                        if self.chat:
                            with contextlib.suppress(Exception):
                                self.chat.notify_text(f"[OES] 측정 실패: {e!r} → 건너뜀")
                        self.process_controller.on_oes_ok()

                except Exception as e:
                    self.append_log("OES", f"예상치 못한 예외: {e!r} → 다음 단계")
                    if self.chat:
                        with contextlib.suppress(Exception):
                            self.chat.notify_text(f"[OES] 예외: {e!r} → 건너뜀")
                    self.process_controller.on_oes_ok()
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
                    if self.chat:
                        with contextlib.suppress(Exception):
                            self.chat.notify_text(f"[RGA] {msg}")
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
                    if not getattr(self, "_log_file_path", None):
                        self._prepare_log_file(payload.get("params", {}))
                    try:
                        self.data_logger.start_new_log_session(payload.get("params", {}))
                    except Exception:
                        pass
                    self._soon(self._graph_reset_safe)
                    if self.chat:
                        with contextlib.suppress(Exception):
                            self.chat.notify_process_started(payload.get("params", {}))
                    self._last_polling_targets = None

                elif kind == "finished":
                    ok = bool(payload.get("ok", False))
                    detail = payload.get("detail", {}) or {}
                    ok_for_log = bool(detail.get("ok_for_log", ok))
                    self.data_logger.finalize_and_write_log(ok_for_log)
                    await asyncio.sleep(0.20)
                    if self.chat:
                        with contextlib.suppress(Exception):
                            self.chat.notify_process_finished_detail(ok, detail)
                    try:
                        self.mfc.on_process_finished(ok)
                    except Exception:
                        pass

                    if getattr(self, "_pc_stopping", False):
                        with contextlib.suppress(Exception):
                            self._clear_queue_and_reset_ui()
                        self._last_polling_targets = None

                    if getattr(self, "_pending_device_cleanup", False):
                        with contextlib.suppress(Exception):
                            self._spawn_detached(self._stop_device_watchdogs(light=False), name="FullCleanup")
                        self._pending_device_cleanup = False
                        self._pc_stopping = False
                        continue

                    self._pc_stopping = False
                    self._start_next_process_from_queue(ok)
                    self._last_polling_targets = None

                elif kind == "aborted":
                    if self.chat:
                        with contextlib.suppress(Exception):
                            self.chat.notify_text(f"🛑 CH{self.ch} 공정 중단")
                    with contextlib.suppress(Exception):
                        self._clear_queue_and_reset_ui()

                    if getattr(self, "_pending_device_cleanup", False):
                        with contextlib.suppress(Exception):
                            self._spawn_detached(self._stop_device_watchdogs(light=False), name="FullCleanup")
                        self._pending_device_cleanup = False
                        self._pc_stopping = False

                elif kind == "polling_targets":
                    targets = dict(payload.get("targets") or {})
                    self._last_polling_targets = targets
                    self._apply_polling_targets(targets)

                elif kind == "polling":
                    active = bool(payload.get("active", False))
                    self._ensure_background_started()
                    targets = getattr(self, "_last_polling_targets", None)
                    if not targets:
                        params = getattr(self.process_controller, "current_params", {}) or {}
                        # 계산(펄스 독립, 펄스 사용 시 연속은 안전상 꺼둠)
                        use_dc_pulse = bool(params.get("use_dc_pulse", False))
                        use_rf_pulse = bool(params.get("use_rf_pulse", False))
                        use_dc_cont  = bool(params.get("use_dc_power", False))
                        use_rf_cont  = bool(params.get("use_rf_power", False))
                        any_pulse    = use_dc_pulse or use_rf_pulse

                        targets = {
                            "mfc":      active,
                            "dc_pulse": active and use_dc_pulse and self.supports_dc_pulse,
                            "rf_pulse": active and use_rf_pulse and self.supports_rf_pulse,
                            "dc":       active and use_dc_cont and self.supports_dc_cont and not any_pulse,
                            "rf":       active and use_rf_cont and self.supports_rf_cont and not any_pulse,
                        }
                    else:
                        targets = {
                            "mfc":      (active and bool(targets.get("mfc", False))),
                            "dc_pulse": (active and bool(targets.get("dc_pulse", False)) and self.supports_dc_pulse),
                            "rf_pulse": (active and bool(targets.get("rf_pulse", False)) and self.supports_rf_pulse),
                            "dc":       (active and bool(targets.get("dc", False)) and self.supports_dc_cont),
                            "rf":       (active and bool(targets.get("rf", False)) and self.supports_rf_cont),
                        }
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
                if self.chat:
                    with contextlib.suppress(Exception):
                        self.chat.notify_error_with_src(f"MFC{self.ch}", f"{ev.cmd or ''}: {why}")
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
                if self.chat:
                    with contextlib.suppress(Exception):
                        self.chat.notify_error_with_src(f"IG{self.ch}", why)

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
            elif k == "target_reached":
                self.process_controller.on_dc_target_reached()
            elif k == "power_off_finished":
                self.process_controller.on_device_step_ok()

    async def _pump_rf_events(self) -> None:
        if not self.rf_power:
            return
        async for ev in self.rf_power.events():
            k = ev.kind
            if k == "status":
                self.append_log(f"RF{self.ch}", ev.message or "")
            elif k == "display":
                with contextlib.suppress(Exception):
                    self.data_logger.log_rf_power(
                        float(ev.forward   or 0.0),
                        float(ev.reflected or 0.0),
                    )
                self._display_rf(ev.forward, ev.reflected)
            elif k == "target_reached":
                self.process_controller.on_rf_target_reached()
            elif k == "target_failed":
                why = ev.message or "unknown"
                self.process_controller.on_rf_target_failed(why)
                if self.chat:
                    with contextlib.suppress(Exception):
                        self.chat.notify_error_with_src("RF Power", why)
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
                if self.chat:
                    with contextlib.suppress(Exception):
                        self.chat.notify_error_with_src("RFPulse", why)
            elif k == "power_off_finished":
                self.process_controller.on_rf_pulse_off_finished()

    async def _pump_dcpulse_events(self) -> None:
        if not self.dc_pulse:
            return
        async for ev in self.dc_pulse.events():
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

                try:
                    self.data_logger.log_dc_power(float(P or 0.0), float(V or 0.0), float(I or 0.0))
                except Exception:
                    pass
                self._display_dc(P, V, I)
                self.append_log(f"DCPulse{self.ch}", f"[telemetry] P={float(P or 0):.1f} W, V={float(V or 0):.2f} V, I={float(I or 0):.3f} A")

            elif k == "command_confirmed":
                cmd = (ev.cmd or "").upper()
                if cmd == "OUTPUT_ON":
                    # 폴링은 장비 내부에서 자동 시작됨
                    self.process_controller.on_dc_pulse_target_reached()
                elif cmd == "OUTPUT_OFF":
                    # 폴링은 장비 내부에서 자동 중지됨
                    self.process_controller.on_dc_pulse_off_finished()

            elif k == "command_failed":
                why = ev.reason or "unknown"
                self.append_log(f"DCPulse{self.ch}", f"CMD FAIL: {ev.cmd or ''} ({why})")
                self.process_controller.on_dc_pulse_failed(why)

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
                if k == "finished":
                    if bool(getattr(ev, "success", False)):
                        self.process_controller.on_oes_ok()
                    else:
                        why = getattr(ev, "message", "measure failed")
                        self.append_log(f"OES{self.ch}", f"측정 실패: {why} → 다음 단계")
                        if self.chat:
                            with contextlib.suppress(Exception):
                                self.chat.notify_text(f"[OES{self.ch}] 측정 실패: {why} → 건너뜀")
                        self.process_controller.on_oes_ok()
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
        self._spawn_detached(self._start_devices_task(), name=f"DevStart.CH{self.ch}")

    async def _start_devices_task(self) -> None:
        async def _maybe_start_or_connect(obj, label: str, *, log: bool = True):
            if not obj:
                return
            try:
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
        if b_stop:  b_stop.setEnabled(True)

    def _apply_process_state_message(self, message: str) -> None:
        if getattr(self, "_last_state_text", None) == message:
            return
        self._last_state_text = message
        if self._w_state:
            self._w_state.setPlainText(message)

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
        _set("dcPower_edit", params.get('dc_power', '0'))

        # DC-Pulse
        _set("dcPulsePower_checkbox", params.get('use_dc_pulse', 'F') == 'T')
        _set("dcPulsePower_edit",     params.get('dc_pulse_power', '0'))
        dcf = str(params.get('dc_pulse_freq', '')).strip()
        dcd = str(params.get('dc_pulse_duty_cycle', '')).strip()
        _set("dcPulseFreq_edit",       '' if dcf in ('', '0') else dcf)
        _set("dcPulseDutyCycle_edit",  '' if dcd in ('', '0') else dcd)

        # RF-Pulse
        _set("rfPulsePower_checkbox", params.get('use_rf_pulse', 'F') == 'T')
        _set("rfPulsePower_edit",     params.get('rf_pulse_power', '0'))
        rff = str(params.get('rf_pulse_freq', '')).strip()
        rfd = str(params.get('rf_pulse_duty_cycle', '')).strip()
        _set("rfPulseFreq_edit",       '' if rff in ('', '0') else rff)
        _set("rfPulseDutyCycle_edit",  '' if rfd in ('', '0') else rfd)

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
        _set("dcPower_checkbox", params.get('use_dc_power', 'F') == 'T')
        _set("powerSelect_checkbox", params.get('power_select', 'F') == 'T')

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
                if not getattr(self, "_log_file_path", None):
                    self._prepare_log_file(norm)
                else:
                    self.append_log("Logger", f"같은 세션 파일 계속 사용: {self._log_file_path.name}")
                self._spawn_detached(self._start_process_later(params, 0.25))
            else:
                self.append_log("MAIN", "모든 공정 완료")
                self._clear_queue_and_reset_ui()
        finally:
            self._advancing = False

    async def _start_process_later(self, params: RawParams, delay_s: float = 0.1) -> None:
        await asyncio.sleep(delay_s)
        self._safe_start_process(self._normalize_params_for_process(params))

    def _safe_start_process(self, params: NormParams) -> None:
        if self.process_controller.is_running:
            self.append_log("MAIN", "이미 다른 공정 실행 중"); return
        self._spawn_detached(self._start_after_preflight(params))

    async def _start_after_preflight(self, params: NormParams) -> None:
        try:
            # 시작 시도 직전에만 허용
            self._auto_connect_enabled = True
            
            # ✅ 이번 런에서 실제로 사용할 펄스만 표시(IG/MFC는 항상 연결이므로 제외)
            use_dc_pulse = bool(params.get("use_dc_pulse", False)) and self.supports_dc_pulse
            use_rf_pulse = bool(params.get("use_rf_pulse", False)) and self.supports_rf_pulse
            self._run_select = {
                "dc_pulse": use_dc_pulse,
                "rf_pulse": use_rf_pulse,
            }

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
                self._start_next_process_from_queue(False)
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
        need: list[tuple[str, object]] = [("MFC", self.mfc), ("IG", self.ig)]

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

    # ------------------------------------------------------------------
    # Start/Stop (개별 챔버)
    def _handle_start_clicked(self, _checked: bool = False):
        if self.process_controller.is_running:
            self._post_warning("실행 오류", "다른 공정이 실행 중입니다."); 
            return
        
        if not self._has_ui() and not getattr(self, "process_queue", None):
            self.append_log("MAIN", "headless: 수동 시작 비활성화 (CSV 자동 실행만 지원)")
            return
        
        # 재시도: 사용자가 Start를 누른 시점부터 자동 연결 허용
        self._auto_connect_enabled = True

        if getattr(self, "process_queue", None):
            if not getattr(self, "_log_file_path", None):
                first = self.process_queue[0] if self.process_queue else {}
                note = f"AutoRun CH{self.ch}: {first.get('Process_name', 'Run')}"
                self._prepare_log_file({"process_note": note})
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
        }
        params["G1 Target"] = vals.get("G1_target_name", "")
        params["G2 Target"] = vals.get("G2_target_name", "")
        params["G3 Target"] = vals.get("G3_target_name", "")

        self._prepare_log_file(params)
        self.append_log("MAIN", "입력 검증 통과 → 장비 연결 확인 시작")
        self._safe_start_process(cast(NormParams, params))

    def _handle_stop_clicked(self, _checked: bool = False):
        self.request_stop_all(user_initiated=True)

    def request_stop_all(self, user_initiated: bool):
        self._cancel_delay_task()
        if getattr(self, "_pc_stopping", False):
            self.append_log("MAIN", "정지 요청 무시: 이미 종료 절차 진행 중"); return
        
        # Stop 이후엔 자동 재연결 차단(사용자가 Start로 다시 올릴 때까지)
        self._auto_connect_enabled = False
        try: self._ensure_background_started()
        except Exception: pass

        self._spawn_detached(self._stop_device_watchdogs(light=True))
        self._pc_stopping = True
        self._pending_device_cleanup = True
        self.process_controller.request_stop()

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

        try:
            await self._shutdown_log_writer()
        except Exception:
            pass

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

            try: await self._shutdown_log_writer()
            except Exception: pass

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
            txtf = self._get_text("dcPulseFreq_edit")
            if txtf:
                try:
                    dc_pulse_freq = int(float(txtf))
                    if dc_pulse_freq < 1 or dc_pulse_freq > 100000: raise ValueError()
                except ValueError:
                    self._post_warning("입력값 확인", "DC-Pulse Freq(Hz)는 1..100000 범위")
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
        if use_g1 and not g1_name:
            self._post_warning("입력값 확인", "G1 타겟 이름이 비어있습니다.")
            return None
        if use_g2 and not g2_name:
            self._post_warning("입력값 확인", "G2 타겟 이름이 비어있습니다.")
            return None
        if use_g3 and not g3_name:
            self._post_warning("입력값 확인", "G3 타겟 이름이 비어있습니다.")
            return None
        
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
        use_dc = bool(getattr(self._u("dcPower_checkbox"), "isChecked", lambda: False)())
        if not (use_rf_pulse or use_dc):
            self._post_warning("선택 오류", "RF Pulse 또는 DC 중 하나 이상 선택"); return None

        rf_pulse_power = 0.0; rf_pulse_freq = None; rf_pulse_duty = None
        if use_rf_pulse:
            try:
                rf_pulse_power = float(self._get_text("rfPulsePower_edit") or "0")
                if rf_pulse_power <= 0: raise ValueError()
            except ValueError:
                self._post_warning("입력값 확인", "RF Pulse Target Power(W)를 확인하세요."); return None
            txtf = self._get_text("rfPulseFreq_edit")
            if txtf:
                try:
                    rf_pulse_freq = int(float(txtf))
                    if rf_pulse_freq < 1 or rf_pulse_freq > 100000: raise ValueError()
                except ValueError:
                    self._post_warning("입력값 확인", "RF Pulse Freq(Hz) 1..100000"); return None
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

        return {
            "use_ms": bool(getattr(self._u("mainShutter_checkbox"), "isChecked", lambda: False)()),
            "use_g1": use_g1, "use_g2": use_g2, "use_g3": use_g3,
            "use_ar": use_ar, "use_o2": use_o2, "use_n2": use_n2,
            "ar_flow": ar_flow, "o2_flow": o2_flow, "n2_flow": n2_flow,
            "use_rf_power": False,
            "use_rf_pulse": use_rf_pulse, "use_dc_power": use_dc,
            "rf_power": 0.0, "rf_pulse_power": rf_pulse_power, "dc_power": dc_power,
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
        t = getattr(self, "_delay_task", None)
        if t and not t.done(): t.cancel()
        self._delay_task = None

    def _on_delay_step_done(self, step_name: str):
        self._delay_task = None
        self._last_state_text = None
        self.append_log("Process", f"'{step_name}' 지연 완료 → 다음 공정")
        self._start_next_process_from_queue(True)

    async def _delay_sleep_then_continue(self, name: str, sec: float):
        try:
            await asyncio.sleep(sec)
            self._on_delay_step_done(name)
        except asyncio.CancelledError:
            pass

    def _try_handle_delay_step(self, params: Mapping[str, Any]) -> bool:
        name = str(params.get("Process_name") or params.get("process_note", "")).strip()
        if not name: return False
        m = re.match(r"^\s*delay\s*(\d+)\s*([smhd]?)\s*$", name, re.IGNORECASE)
        if not m: return False
        amount = int(m.group(1))
        unit = (m.group(2) or "m").lower()
        factor = {"s":1.0, "m":60.0, "h":3600.0, "d":86400.0}[unit]
        duration_s = amount * factor
        unit_txt = {"s":"초","m":"분","h":"시간","d":"일"}[unit]
        self.append_log("Process", f"'{name}' 단계 감지: {amount}{unit_txt} 대기 시작")

        self._apply_polling_targets({"mfc": False, "dc_pulse": False, "rf_pulse": False, "dc": False, "rf": False})
        self._last_polling_targets = None

        if self._w_state:
            self._w_state.setPlainText(f"지연 대기 중: {amount}{unit_txt}")

        self._cancel_delay_task()
        self._set_task_later("_delay_task", self._delay_sleep_then_continue(name, duration_s), name=f"Delay:{name}")
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

        # ✅ 어떤 폴링이라도 실제로 켜야 할 때 + 자동연결 허용 상태일 때만 장치/워치독을 올림
        if (mfc_on or dcpl_on or rfpl_on or dc_on or rf_on) and self._auto_connect_enabled:
            self._ensure_background_started()

        with contextlib.suppress(Exception):
            self.mfc.set_process_status(mfc_on)

        if self.dc_pulse:
            with contextlib.suppress(Exception):
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
        now_local = datetime.now().astimezone()
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

        if self._prestart_buf:
            for line in list(self._prestart_buf):
                self._log_enqueue_nowait(line)
            self._prestart_buf.clear()
        self.append_log("Logger", f"새 로그 파일 시작: {self._log_file_path.name}")
        note = str(params.get("process_note", "") or params.get("Process_name", "") or f"Run CH{self.ch}")
        self.append_log("MAIN", f"=== '{note}' 공정 준비 (장비 연결부터 기록) ===")

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
                line = await self._log_q.get()
                if self._log_fp is None:
                    if self._log_file_path:
                        try:
                            self._log_fp = open(self._log_file_path, "a", encoding="utf-8", newline="")
                        except Exception:
                            await asyncio.sleep(0.2)
                            self._soon(self._log_enqueue_nowait, line)
                            continue
                    else:
                        continue
                try:
                    self._log_fp.write(line); self._log_fp.flush()
                except Exception:
                    await asyncio.sleep(0.2)
                    self._soon(self._log_enqueue_nowait, line)
        except asyncio.CancelledError:
            pass
        finally:
            if self._log_fp:
                with contextlib.suppress(Exception):
                    self._log_fp.flush(); self._log_fp.close()
                self._log_fp = None

    async def _shutdown_log_writer(self):
        if self._log_writer_task:
            self._log_writer_task.cancel()
            with contextlib.suppress(Exception):
                await self._log_writer_task
            self._log_writer_task = None
        if self._log_fp:
            with contextlib.suppress(Exception):
                self._log_fp.flush(); self._log_fp.close()
            self._log_fp = None

    def _clear_queue_and_reset_ui(self) -> None:
        self.process_queue = []
        self.current_process_index = -1
        self._reset_ui_after_process()
        try:
            self._spawn_detached(self._shutdown_log_writer())
        except Exception:
            pass
        self._log_file_path = None
        try: self._prestart_buf.clear()
        except Exception: pass

    # ------------------------------------------------------------------
    # 기본 UI값/리셋
    def _set_default_ui_values(self) -> None:
        _set = self._set
        _set("basePressure_edit", "9e-6")
        _set("integrationTime_edit", "60")
        _set("workingPressure_edit", "2")
        _set("processTime_edit", "1")
        _set("shutterDelay_edit", "1")
        _set("arFlow_edit", "20")
        _set("o2Flow_edit", "0")
        _set("n2Flow_edit", "0")
        _set("dcPower_edit", "100")
        # DC-Pulse
        _set("dcPulsePower_checkbox", False)
        _set("dcPulsePower_edit", "100")
        _set("dcPulseFreq_edit", "")
        _set("dcPulseDutyCycle_edit", "")
        # RF-Pulse
        _set("rfPulsePower_checkbox", False)
        _set("rfPulsePower_edit", "100")
        _set("rfPulseFreq_edit", "")
        _set("rfPulseDutyCycle_edit", "")

    def _reset_ui_after_process(self):
        self._set_default_ui_values()
        for name in (
            "G1_checkbox","G2_checkbox","G3_checkbox","Ar_checkbox","O2_checkbox","N2_checkbox",
            "mainShutter_checkbox","dcPulsePower_checkbox","rfPulsePower_checkbox","dcPower_checkbox","powerSelect_checkbox",
        ):
            w = self._u(name)
            if w is not None:
                with contextlib.suppress(Exception):
                    w.setChecked(False)
        _s = self._u("processState_edit")
        if _s: _s.setPlainText("대기 중")
        for leaf in ("Power_edit","Voltage_edit","Current_edit","forP_edit","refP_edit"):
            w = self._u(leaf)
            if w: w.setPlainText("")
        self._on_process_status_changed(False)
        with contextlib.suppress(Exception):
            self.graph.reset()

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
        """CH1의 UI 위젯 이름과 공통 이름을 매핑."""
        if self.ch != 1:
            return leaf
        return {
            "integrationTime_edit": "intergrationTime_edit",
            "rfPulsePower_checkbox":    "dcPulsePower_checkbox",
            "rfPulsePower_edit":        "dcPulsePower_edit",
            "rfPulseFreq_edit":         "dcPulseFreq_edit",
            "rfPulseDutyCycle_edit":    "dcPulseDutyCycle_edit",
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


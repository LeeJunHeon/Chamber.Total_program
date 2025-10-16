# runtime/plasma_cleaning_runtime.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio, contextlib, inspect, traceback, time, logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Coroutine, Mapping, Optional, Awaitable

from PySide6.QtGui import QTextCursor
from PySide6.QtWidgets import QMessageBox, QPlainTextEdit, QApplication
from PySide6.QtCore import Qt

# 장비/컨트롤러
from device.mfc import AsyncMFC
from device.plc import AsyncPLC
from controller.plasma_cleaning_controller import PlasmaCleaningController
from controller.graph_controller import GraphController
from controller.data_logger import DataLogger
from device.rf_power import RFPowerAsync, RFPowerEvent


# --- 최소 설정 어댑터: CH1 MFC TCP만 필요 ---
@dataclass
class _CfgAdapter:
    mod: Any
    def MFC_TCP_CH1(self) -> tuple[str, int]:
        host = str(getattr(self.mod, "MFC_TCP_HOST", "192.168.1.50"))
        port = int(getattr(self.mod, "MFC_TCP_PORT", 4006))  # CH1 기본 포트
        return (host, port)


class PlasmaCleaningRuntime:
    """
    Plasma Cleaning 전용 런타임.

    - MFC 컨트롤러 **#1** 사용, 가스 라인 **#3** 고정 (on/set/off는 유량으로 처리: 0 = off)
    - **Working Pressure**: MFC의 **SP4 set/on/off**로 목표 압력 유지
    - **Target Pressure**: **IG**를 읽어서 목표 도달(±tol, settle 유지)까지 **대기**
    - RF 파워는 펄스 없이 '연속'만, **PLC DAC**로 적용/읽기/끄기
    - Plasma Cleaning은 SP4 사용 및 MFC1 #3 GAS 사용
    """

    # ─────────────────────────────────────────────────────────────
    # 생성/초기화
    # ─────────────────────────────────────────────────────────────
    def __init__(
        self,
        ui: Any,
        prefix: str,
        loop: asyncio.AbstractEventLoop,
        cfg: Any,
        log_dir: Path,
        *,
        plc: AsyncPLC | None = None,   # PLC는 RF DAC 및 GV
        chat: Optional[Any] = None,
    ) -> None:
        self.ui = ui
        self.prefix = str(prefix)
        self._loop = loop
        self.chat = chat
        self.cfg = _CfgAdapter(cfg)
        self.plc = plc
        self._bg_tasks: list[asyncio.Task[Any]] = []
        self._bg_started = False
        self._devices_started = False
        self._auto_connect_enabled = True

        # 로그/상태 위젯
        self._w_log: QPlainTextEdit | None = self._u("logMessage_edit")
        self._w_state: QPlainTextEdit | None = self._u("processState_edit")

        # 그래프
        self.graph = GraphController(self._u("rgaGraph_widget"), self._u("oesGraph_widget"))
        with contextlib.suppress(Exception):
            self.graph.reset()

        # 데이터 로거
        self.data_logger = DataLogger(
            ch=0,
            csv_dir=Path(r"\\VanaM_NAS\VanaM_Sputter\Sputter\Calib\Database"),
        )

        # 로그 파일 상태
        self._log_root = Path(log_dir)
        self._log_dir = self._ensure_log_dir(self._log_root)
        self._log_file_path: Path | None = None
        self._log_fp = None

        # 장치: CH1 MFC (컨트롤러 #1)
        host, port = self.cfg.MFC_TCP_CH1()
        self.mfc = AsyncMFC(host=host, port=port, enable_verify=False, enable_stabilization=True)

        # PLC가 없다면 RF/밸브 기능 제한됨
        self.rf = RFPowerAsync(
            send_rf_power=self._rf_apply_via_plc,
            send_rf_power_unverified=self._rf_write_via_plc,
            request_status_read=self._rf_read_status_via_plc,
            toggle_enable=self._rf_set_latch_via_plc,           # SET 래치
            watt_deadband=5.0,
        )

        # 최근 IG 압력(mTorr) 캐시
        self._last_ig_mTorr: float | None = None
        # 외부(ChamberRuntime)에서 주입 가능한 IG 콜백
        self._ensure_ig_on_cb: Optional[Callable[[], Awaitable[None]]] = None
        self._read_ig_cb: Optional[Callable[[], Awaitable[float]]] = None

        # UI 바인딩
        self._connect_my_buttons()

        # (중요) 외부 로그를 UI로 리다이렉트 — 터미널 출력 방지
        self._install_logger_bridge()

        # 컨트롤러 바인딩
        self._bind_plasma_controller()

        # MFC 이벤트 구독 → UI/로깅
        self._ensure_task_alive("MFC_Events", self._mfc_event_loop)

    # ─────────────────────────────────────────────────────────────
    # 외부 주입: IG/PLC 연결 브리지
    # ─────────────────────────────────────────────────────────────
    def set_ig_callbacks(
        self,
        ensure_on: Callable[[], Awaitable[None]] | None,
        read_mTorr: Callable[[], Awaitable[float]] | None,
    ) -> None:
        self._ensure_ig_on_cb = ensure_on
        self._read_ig_cb = read_mTorr

    # ─────────────────────────────────────────────────────────────
    # UI 유틸
    # ─────────────────────────────────────────────────────────────
    def _u(self, leaf: str) -> Any | None:
        try:
            return getattr(self.ui, f"{self.prefix}_{leaf}")
        except Exception:
            return None

    def _has_ui(self) -> bool:
        try:
            return QApplication.instance() is not None
        except Exception:
            return False

    def append_log(self, src: str, msg: str) -> None:
        s = f"[{src}] {msg}"
        try:
            if self._w_log is not None:
                self._w_log.appendPlainText(s)
                self._w_log.moveCursor(QTextCursor.End)
        except Exception:
            pass
        # 파일에도 기록
        try:
            if self._log_fp:
                self._log_fp.write(f"{datetime.now():%H:%M:%S} {s}\n")
                self._log_fp.flush()
        except Exception:
            pass

    def _set(self, leaf: str, v: Any) -> None:
        w = self._u(leaf)
        if not w:
            return
        try:
            if hasattr(w, "setValue"):
                try:
                    w.setValue(v if isinstance(v, (int, float)) else float(str(v)))
                except Exception:
                    pass
                else:
                    return
            s = str(v)
            if hasattr(w, "setPlainText"):
                w.setPlainText(s); return
            if hasattr(w, "setText"):
                w.setText(s); return
        except Exception as e:
            self.append_log("UI", f"_set('{leaf}') 실패: {e!r}")

    def _show_state(self, st: str) -> None:
        self._set("processState_edit", st)

    # ─────────────────────────────────────────────────────────────
    # 메시지박스(경고/치명)
    # ─────────────────────────────────────────────────────────────
    def _parent_widget(self) -> Any | None:
        for leaf in ("Start_button", "Stop_button", "processState_edit", "logMessage_edit"):
            w = self._u(leaf)
            if w is not None:
                try:
                    return w.window()
                except Exception:
                    return w
        return None

    def _post_warning(self, title: str, text: str) -> None:
        if not self._has_ui():
            self.append_log("WARN", f"{title}: {text}"); return
        box = QMessageBox(self._parent_widget() or None)
        box.setWindowTitle(title); box.setText(text)
        box.setIcon(QMessageBox.Warning)
        box.setStandardButtons(QMessageBox.Ok)
        box.setWindowModality(Qt.WindowModality.WindowModal)
        box.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
        box.open()

    def _post_critical(self, title: str, text: str) -> None:
        if not self._has_ui():
            self.append_log("ERROR", f"{title}: {text}"); return
        box = QMessageBox(self._parent_widget() or None)
        box.setWindowTitle(title); box.setText(text)
        box.setIcon(QMessageBox.Critical)
        box.setStandardButtons(QMessageBox.Ok)
        box.setWindowModality(Qt.WindowModality.WindowModal)
        box.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
        box.open()

    # ─────────────────────────────────────────────────────────────
    # 태스크 유틸
    # ─────────────────────────────────────────────────────────────
    def _spawn_detached(self, coro, *, store: bool=False, name: str|None=None) -> None:
        loop = self._loop
        def _create():
            t = loop.create_task(coro, name=name)
            def _done(task: asyncio.Task):
                if task.cancelled(): return
                try: exc = task.exception()
                except Exception: return
                if exc:
                    tb = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__)).rstrip()
                    self.append_log("Task", f"[{name or 'task'}] crashed:\n{tb}")
            t.add_done_callback(_done)
            if store: self._bg_tasks.append(t)
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop: loop.call_soon(_create)
        else: loop.call_soon_threadsafe(_create)

    def _ensure_task_alive(self, name: str, coro_factory: Callable[[], Coroutine[Any, Any, Any]]) -> None:
        self._bg_tasks = [t for t in self._bg_tasks if t and not t.done()]
        for t in self._bg_tasks:
            try:
                if t.get_name() == name and not t.done(): return
            except Exception:
                pass
        self._spawn_detached(coro_factory(), store=True, name=name)

    def shutdown_fast(self) -> None:
        async def run():
            try:
                if getattr(self.plasma, "is_running", False):
                    self.plasma.request_stop()
            except Exception:
                pass
            with contextlib.suppress(Exception):
                await self.rf.cleanup()
            with contextlib.suppress(Exception):
                await self.mfc.cleanup()
            with contextlib.suppress(Exception):
                if self._log_fp: self._log_fp.flush(); self._log_fp.close()
        self._spawn_detached(run())

    # ─────────────────────────────────────────────────────────────
    # 외부 로그 → UI(Log 창) 브릿지
    # ─────────────────────────────────────────────────────────────
    def _install_logger_bridge(self) -> None:
        class _UIHandler(logging.Handler):
            def __init__(self, sink: Callable[[str, str], None]):
                super().__init__()
                self._sink = sink
            def emit(self, record: logging.LogRecord) -> None:
                try:
                    msg = self.format(record)
                except Exception:
                    msg = record.getMessage()
                self._sink(record.name, msg)

        h = _UIHandler(lambda src, m: self.append_log(src, m))
        h.setLevel(logging.INFO)

        # pymodbus/asyncio 계열 로그를 UI로만 보냄(터미널 출력 억제)
        for name in ("pymodbus", "pymodbus.client", "asyncio"):
            lg = logging.getLogger(name)
            lg.setLevel(logging.INFO)
            lg.handlers[:] = [h]
            lg.propagate = False

    # ─────────────────────────────────────────────────────────────
    # RF (PLC DAC) 브릿지
    # ─────────────────────────────────────────────────────────────
    async def _rf_apply_via_plc(self, watt: float) -> None:
        if not self.plc:
            raise RuntimeError("PLC 미주입 — RF 설정 불가")
        await self.plc.power_apply(watt, family="RFV")  # PLC의 DAC 적용(검증형)

    async def _rf_write_via_plc(self, watt: float) -> None:
        if not self.plc:
            return
        await self.plc.power_write(watt, family="RFV")  # 무응답형

    async def _rf_read_status_via_plc(self) -> object:
        # 필요시 PLC에서 FWD/REF 읽어와 dict로 반환
        try:
            if not self.plc:
                return {}
            fw, rf = await self.plc.read_rf_forward_reflected()
            return {"forward": fw, "reflected": rf}
        except Exception:
            return {}

    async def _rf_set_latch_via_plc(self, enable: bool) -> None:
        if not self.plc:
            return
        await self.plc.toggle_set_latch(enable, family="RFV")  # RF SET ON/OFF 래치

    # ─────────────────────────────────────────────────────────────
    # MFC 이벤트 루프
    # ─────────────────────────────────────────────────────────────
    async def _mfc_event_loop(self):
        async for ev in self.mfc.events():
            try:
                k = getattr(ev, "kind", None)
                if k == "status":
                    self.append_log("MFC", ev.message or "")
                elif k == "command_confirmed":
                    self.plasma.on_mfc_confirmed(ev.cmd or "")
                elif k == "command_failed":
                    self.plasma.on_mfc_failed(ev.cmd or "", ev.reason or "unknown")
                elif k == "flow":
                    with contextlib.suppress(Exception):
                        self.data_logger.log_mfc_flow(ev.gas or "", float(ev.value or 0.0))
                elif k == "pressure":  # IG/챔버 압력 이벤트
                    try:
                        if ev.value is not None:
                            self._last_ig_mTorr = float(ev.value)
                            self._set("basePressure_edit", f"{self._last_ig_mTorr:.4f}")
                            with contextlib.suppress(Exception):
                                self.data_logger.log_mfc_pressure(f"{self._last_ig_mTorr:.4f}")
                        elif ev.text:
                            self.data_logger.log_mfc_pressure(ev.text)
                    except Exception:
                        pass
            except Exception:
                pass

    # ─────────────────────────────────────────────────────────────
    # UI 바인딩
    # ─────────────────────────────────────────────────────────────
    def _connect_my_buttons(self) -> None:
        if not self._has_ui():
            return
        b = self._u("Start_button")
        if b: b.clicked.connect(self._handle_start_clicked)
        b = self._u("Stop_button")
        if b: b.clicked.connect(self._handle_stop_clicked)

    def _handle_start_clicked(self, _checked: bool = False):
        if getattr(self.plasma, "is_running", False):
            self._post_warning("실행 중", "이미 플라즈마 클리닝이 실행 중입니다.")
            return

        def _f(leaf: str, default: float) -> float:
            t = (self._get_text(leaf) or "").strip()
            try:
                return float(t) if t else float(default)
            except Exception:
                return float(default)

        gas_flow = _f("gasFlow_edit", 0.0)                 # Gas Flow (sccm)
        rf_w     = _f("rfPower_edit", 100.0)               # RF Power (W)
        if rf_w <= 0:
            self._post_warning("입력값 확인", "RF Power(W)를 확인하세요.")
            return

        workingP = _f("workingPressure_edit", 2.0)         # SP4 setpoint (mTorr)
        ig_target = _f("targetPressure_edit", workingP)    # IG 타겟 (mTorr)

        tolP      = 0.2
        timeout_s = 90.0
        settle_s  = 5.0
        hold_min  = max(0.0, _f("ProcessTime_edit", 1.0))

        params = {
            "process_note": "PlasmaCleaning",
            "pc_gas_mfc_idx": 3,                   # Gas #3 (N2) 사용【turn19file10†L70-L71】
            "pc_gas_flow_sccm": gas_flow,
            "pc_target_pressure_mTorr": ig_target,
            "pc_tol_mTorr": tolP,
            "pc_wait_timeout_s": timeout_s,
            "pc_settle_s": settle_s,
            "pc_sp4_setpoint_mTorr": workingP,
            "pc_rf_power_w": rf_w,
            "pc_process_time_min": hold_min,
        }

        self._prepare_log_file(params)
        self.plasma.start(params)

    def _handle_stop_clicked(self, _checked: bool = False):
        try:
            self.plasma.request_stop()
        except Exception as e:
            self.append_log("PC", f"정지 요청 실패: {e!r}")

    # ─────────────────────────────────────────────────────────────
    # 컨트롤러 바인딩(콜백 주입)
    # ─────────────────────────────────────────────────────────────
    def _bind_plasma_controller(self) -> None:
        # ── PLC(GV) Key 결정자: 기본 CH1 사용 ─────────────────────
        # 필요시 외부에서 setter로 CH2 키를 주입할 수 있게 람다 사용
        def gv_key(name: str) -> str:
            # CH1 기준: G_V_1_*
            # CH2 사용 시 외부에서 이 함수 교체(set_..._callbacks 등으로) 권장
            base = "1"
            return f"G_V_{base}_{name}"

        async def plc_check_gv_interlock() -> bool:
            if not self.plc: return False
            try:
                return bool(await self.plc.read_bit(gv_key("인터락")))
            except Exception as e:
                self.append_log("PLC", f"인터락 읽기 실패: {e!r}")
                return False

        async def plc_gv_open() -> None:
            if not self.plc: return
            # 모멘터리 스위치로 취급
            await self.plc.write_switch(gv_key("OPEN_SW"), True, momentary=True)

        async def plc_gv_close() -> None:
            if not self.plc: return
            await self.plc.write_switch(gv_key("CLOSE_SW"), True, momentary=True)

        async def plc_read_gv_open_lamp() -> bool:
            if not self.plc: return False
            try:
                return bool(await self.plc.read_bit(gv_key("OPEN_LAMP")))
            except Exception as e:
                self.append_log("PLC", f"OPEN_LAMP 읽기 실패: {e!r}")
                return False

        # ── IG 브리지 ─────────────────────────────────────────────
        async def ensure_ig_on() -> None:
            if callable(self._ensure_ig_on_cb):
                await self._ensure_ig_on_cb()
            else:
                # 외부 미주입 시 스킵(로그만)
                self.append_log("IG", "ensure_ig_on 콜백 미주입 — 스킵")

        async def read_ig_mTorr() -> float:
            if callable(self._read_ig_cb):
                return float(await self._read_ig_cb())
            # 미주입 시, MFC 이벤트에서 캐시한 값 사용
            if self._last_ig_mTorr is None:
                raise RuntimeError("IG 읽기 콜백 없음")
            return float(self._last_ig_mTorr)

        # ── MFC 브리지 (#1 + Gas idx) ───────────────────────────
        async def mfc_gas_select(gas_idx: int) -> None:
            await self.mfc.handle_command("GAS_SELECT", {"mfc_idx": 1, "gas_idx": int(gas_idx)})

        async def mfc_flow_set_on(flow_sccm: float) -> None:
            await self.mfc.handle_command("FLOW_SET_ON", {"mfc_idx": 1, "gas_idx": 3, "flow": float(flow_sccm)})

        async def mfc_flow_off() -> None:
            await self.mfc.handle_command("FLOW_OFF", {"mfc_idx": 1})

        async def mfc_sp4_set(setpoint_mTorr: float) -> None:
            await self.mfc.handle_command("SP4_SET", {"mfc_idx": 1, "value": float(setpoint_mTorr)})

        async def mfc_sp4_on() -> None:
            await self.mfc.handle_command("SP4_ON", {"mfc_idx": 1})

        async def mfc_sp4_off() -> None:
            await self.mfc.handle_command("SP4_OFF", {"mfc_idx": 1})

        # ── RF(PLC DAC) 브리지 ───────────────────────────────────
        async def rf_start(watt: float) -> None:
            await self.rf.start_process(float(watt))
            self.rf.set_process_status(True)

        async def rf_stop() -> None:
            await self.rf.cleanup()

        # ── UI 콜백 ───────────────────────────────────────────────
        def show_state(st: str) -> None:
            self._show_state(st)

        def show_countdown(left_sec: int) -> None:
            m = left_sec // 60
            s = left_sec % 60
            self._set("ProcessTime_edit", f"{m:02d}:{s:02d}")

        # 컨트롤러 생성
        self.plasma = PlasmaCleaningController(
            log=self.append_log,
            plc_check_gv_interlock=plc_check_gv_interlock,
            plc_gv_open=plc_gv_open,
            plc_gv_close=plc_gv_close,
            plc_read_gv_open_lamp=plc_read_gv_open_lamp,
            ensure_ig_on=ensure_ig_on,
            read_ig_mTorr=read_ig_mTorr,
            mfc_gas_select=mfc_gas_select,
            mfc_flow_set_on=mfc_flow_set_on,
            mfc_flow_off=mfc_flow_off,
            mfc_sp4_set=mfc_sp4_set,
            mfc_sp4_on=mfc_sp4_on,
            mfc_sp4_off=mfc_sp4_off,
            rf_start=rf_start,
            rf_stop=rf_stop,
            show_state=show_state,
            show_countdown=show_countdown,
        )

    # ─────────────────────────────────────────────────────────────
    # 시작/정지 UI 값 읽기 도우미
    # ─────────────────────────────────────────────────────────────
    def _get_text(self, leaf: str) -> str:
        w = self._u(leaf)
        try:
            if not w: return ""
            if hasattr(w, "text"): return str(w.text())
            if hasattr(w, "toPlainText"): return str(w.toPlainText())
            return ""
        except Exception:
            return ""

    # ─────────────────────────────────────────────────────────────
    # 파일 로깅
    # ─────────────────────────────────────────────────────────────
    def _ensure_log_dir(self, root: Path) -> Path:
        d = root / f"plasma_cleaning"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _prepare_log_file(self, params: Mapping[str, Any]) -> None:
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fn = f"{ts}_plasma_cleaning.csv"
            self._log_file_path = self._log_dir / fn
            self._log_fp = open(self._log_file_path, "a", encoding="utf-8", newline="")
            self.append_log("Logger", f"로그 파일 오픈: {self._log_file_path.name}")
        except Exception as e:
            self.append_log("Logger", f"로그 파일 준비 실패: {e!r}")


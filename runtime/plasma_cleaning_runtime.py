# runtime/plasma_cleaning_runtime.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio, contextlib, inspect, traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Coroutine, Mapping, Optional

from PySide6.QtGui import QTextCursor
from PySide6.QtWidgets import QMessageBox, QPlainTextEdit, QApplication
from PySide6.QtCore import Qt

# 장비/컨트롤러
from device.mfc import AsyncMFC
from device.plc import AsyncPLC
from controller.plasma_cleaning_controller import PlasmaCleaningController
from controller.graph_controller import GraphController
from controller.data_logger import DataLogger


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
    - 가스밸브(PLC) 제어 없음: 밸브는 항상 Open으로 가정, MFC#3으로 on/set/off(유량 0=off)
    - SP4 set, on/off는 PLC로 제어 (컨트롤러의 sp1_* 콜백은 내부에서 SP4로 매핑)
    - RF 파워는 펄스 없이 '연속'만, PLC의 DAC로 적용/읽기/끄기
    - IG 압력을 주기적으로 읽어 target까지 '대기'
    """

    def __init__(
        self,
        ui: Any,
        prefix: str,
        loop: asyncio.AbstractEventLoop,
        cfg: Any,
        log_dir: Path,
        *,
        plc: AsyncPLC | None = None,   # ★ 공유 PLC (필수)
        chat: Optional[Any] = None,
    ) -> None:
        self.ui = ui
        self.prefix = str(prefix)
        self._loop = loop
        self.chat = chat
        self.cfg = _CfgAdapter(cfg)
        self.plc = plc                          # ★ PLC 보관
        self._bg_tasks: list[asyncio.Task[Any]] = []
        self._bg_started = False
        self._devices_started = False
        self._auto_connect_enabled = True

        # 로그/상태 위젯 포인터
        self._w_log: QPlainTextEdit | None = self._u("logMessage_edit")
        self._w_state: QPlainTextEdit | None = self._u("processState_edit")

        # 그래프(필요 없으면 내부에서 알아서 스킵)
        self.graph = GraphController(self._u("rgaGraph_widget"), self._u("oesGraph_widget"))
        with contextlib.suppress(Exception):
            self.graph.reset()

        # 데이터 로거
        self.data_logger = DataLogger(ch=0, csv_dir=Path(r"\\VanaM_NAS\VanaM_Sputter\Sputter\Calib\Database"))

        # 로그 파일 상태
        self._log_root = Path(log_dir)
        self._log_dir = self._ensure_log_dir(self._log_root)
        self._log_file_path: Path | None = None
        self._log_fp = None

        # 장치: CH1 MFC (MFC1 사용)
        host, port = self.cfg.MFC_TCP_CH1()
        self.mfc = AsyncMFC(host=host, port=port, enable_verify=False, enable_stabilization=True)

        # 컨트롤러 바인딩
        self._bind_plasma_controller()

        # 버튼 연결/초기값
        self._connect_my_buttons()
        self._set_default_ui_values()

    # ─────────────────────────────────────────────────────────────
    # PLC 헬퍼 (SP1, RF) — 실제 드라이버에 따라 메서드명이 다를 수 있어 다중 시도
    # ─────────────────────────────────────────────────────────────
    async def _plc_sp1_setpoint(self, mTorr: float) -> None:
        if not self.plc:
            raise RuntimeError("PLC 없음(SP1 set 실패)")
        # 1) 전용 메서드가 있으면 사용
        if hasattr(self.plc, "sp1_setpoint"):
            await self.plc.sp1_setpoint(float(mTorr)); return
        if hasattr(self.plc, "set_sp1"):
            await self.plc.set_sp1(float(mTorr)); return
        # 2) 레지스터 이름으로 쓰는 방식(프로젝트 규약에 맞게 바꿔도 됨)
        if hasattr(self.plc, "write_reg_name"):
            await self.plc.write_reg_name("SP1_SETPOINT", float(mTorr)); return
        raise RuntimeError("SP1 set를 지원하는 PLC API를 찾지 못함")

    async def _plc_sp1_on(self, on: bool) -> None:
        if not self.plc:
            raise RuntimeError("PLC 없음(SP1 on/off 실패)")
        if hasattr(self.plc, "sp1_on"):
            await self.plc.sp1_on(bool(on)); return
        if hasattr(self.plc, "sp1_enable"):
            await self.plc.sp1_enable(bool(on)); return
        if hasattr(self.plc, "write_switch"):
            await self.plc.write_switch("SP1_SW", bool(on)); return
        raise RuntimeError("SP1 on/off를 지원하는 PLC API를 찾지 못함")

    async def _plc_rf_apply(self, power_w: float) -> None:
        if not self.plc:
            raise RuntimeError("PLC 없음(RF 적용 실패)")
        # CH2에서 쓰던 규약: family="DCV", channel=1 → RF 채널
        await self.plc.power_apply(float(power_w), family="DCV", ensure_set=True, channel=1)

    async def _plc_rf_off(self) -> None:
        if not self.plc:
            return
        # RF 채널 write_idx=1에 0W 쓰는 규약(프로젝트 기준 유지)
        await self.plc.power_write(0.0, family="DCV", write_idx=1)

    async def _plc_rf_read(self) -> tuple[float, float]:
        """forward/reflected 읽기 (가능할 때만). 실패 시 (0,0)"""
        if not self.plc or not hasattr(self.plc, "read_reg_name"):
            return (0.0, 0.0)
        try:
            fwd_raw = await self.plc.read_reg_name("DCV_READ_2")
            ref_raw = await self.plc.read_reg_name("DCV_READ_3")
            return (float(fwd_raw), float(ref_raw))
        except Exception:
            return (0.0, 0.0)

    # ─────────────────────────────────────────────────────────────
    # 컨트롤러 바인딩 (MFC1 + SP1 + RF 연속)
    # ─────────────────────────────────────────────────────────────
    def _bind_plasma_controller(self) -> None:
        # MFC: on/set/off 모두 "유량"으로 처리(유량>0 = on, 0 = off)
        def cb_mfc(cmd: str, args: Mapping[str, Any]) -> None:
            self._spawn_detached(self.mfc.handle_command(cmd, args))

        # SP1 set (동기 콜백 → 내부에서 비동기로 실행)
        def cb_sp1_set(mTorr: float) -> None:
            async def run():
                try:
                    await self._plc_sp1_setpoint(float(mTorr))
                except Exception as e:
                    self.append_log("SP1", f"set 실패: {e!r}")
            self._spawn_detached(run())

        # SP1 on/off
        def cb_sp1_on(on: bool) -> None:
            async def run():
                try:
                    await self._plc_sp1_on(bool(on))
                except Exception as e:
                    self.append_log("SP1", f"on/off 실패: {e!r}")
            self._spawn_detached(run())

        # RF 파워 적용
        def cb_rf_power(value: float) -> None:
            async def run():
                try:
                    await self._plc_rf_apply(float(value))
                    # 상태 표시(가능 시)
                    fwd, ref = await self._plc_rf_read()
                    if fwd or ref:
                        with contextlib.suppress(Exception):
                            self.data_logger.log_rf_power(fwd, ref)
                        self._set("forP_edit", f"{fwd:.2f}")
                        self._set("refP_edit", f"{ref:.2f}")
                except Exception as e:
                    self.append_log("RF", f"파워 적용 실패: {e!r}")
            self._spawn_detached(run())

        # RF off
        def cb_rf_off() -> None:
            async def run():
                with contextlib.suppress(Exception):
                    await self._plc_rf_off()
            self._spawn_detached(run())

        # IG/RGA/OES 없음
        self.plasma = PlasmaCleaningController(
            ch=1,                       # PC는 CH1 리소스 사용
            send_mfc=cb_mfc,
            sp1_set=cb_sp1_set,
            sp1_on=cb_sp1_on,
            send_rf_power=cb_rf_power,
            stop_rf_power=cb_rf_off,
            # wait_pressure_stable / await_rf_target는 없으면 컨트롤러가 안전 폴백
        )

        # 이벤트 펌프
        self._ensure_task_alive("Pump.PC", self._pump_pc_events)

    async def _pump_pc_events(self) -> None:
        q = self.plasma.event_q
        while True:
            ev = await q.get()
            k = ev.kind; p = ev.payload or {}
            try:
                if k == "log":
                    self.append_log(p.get("src", "PC"), p.get("msg", ""))
                elif k == "state":
                    self._apply_process_state_message(p.get("text", ""))
                elif k == "status":
                    self._on_process_status_changed(bool(p.get("running", False)))
                elif k == "started":
                    if not getattr(self, "_log_file_path", None):
                        self._prepare_log_file(p.get("params", {}))
                    with contextlib.suppress(Exception):
                        self.data_logger.start_new_log_session(p.get("params", {}))
                elif k in ("finished", "aborted"):
                    ok = bool(p.get("ok", False))
                    detail = p.get("detail", {}) or {}
                    ok_for_log = bool(detail.get("ok_for_log", ok))
                    with contextlib.suppress(Exception):
                        self.data_logger.finalize_and_write_log(ok_for_log)
                    self._on_process_status_changed(False)
            except Exception as e:
                self.append_log("PC", f"event pump error: {e!r}")

    # ─────────────────────────────────────────────────────────────
    # 장치 시작/펌프
    # ─────────────────────────────────────────────────────────────
    def _ensure_background_started(self) -> None:
        if not self._auto_connect_enabled or getattr(self, "_ensuring_bg", False):
            return
        self._ensuring_bg = True
        try:
            self._ensure_devices_started()
            self._ensure_task_alive("Pump.MFC.CH1", self._pump_mfc_events)
            self._bg_started = True
        finally:
            self._ensuring_bg = False

    def _ensure_devices_started(self) -> None:
        if getattr(self, "_devices_started", False):
            return
        self._devices_started = True
        self._spawn_detached(self._start_devices_task(), name="DevStart.PC")

    async def _start_devices_task(self) -> None:
        async def _maybe_start_or_connect(obj, label: str):
            if not obj:
                return
            try:
                if self._is_connected(obj):
                    self.append_log(label, "already connected → skip")
                    return
                meth = getattr(obj, "start", None) or getattr(obj, "connect", None)
                if not callable(meth):
                    self.append_log(label, "start/connect 없음 → skip")
                    return
                res = meth()
                if inspect.isawaitable(res):
                    await res
                self.append_log(label, f"{meth.__name__} 호출 완료")
            except Exception as e:
                self.append_log(label, f"start/connect 실패: {e!r}")

        # PLC(공유) + CH1 MFC
        await _maybe_start_or_connect(self.plc, "PLC")
        await _maybe_start_or_connect(self.mfc, "MFC-CH1")

    async def _pump_mfc_events(self) -> None:
        async for ev in self.mfc.events():
            k = ev.kind
            if k == "status":
                self.append_log("MFC", ev.message or "")
            elif k == "command_confirmed":
                self.plasma.on_mfc_confirmed(ev.cmd or "")
            elif k == "command_failed":
                self.plasma.on_mfc_failed(ev.cmd or "", ev.reason or "unknown")
            elif k == "flow":
                with contextlib.suppress(Exception):
                    self.data_logger.log_mfc_flow(ev.gas or "", float(ev.value or 0.0))
            elif k == "pressure":
                with contextlib.suppress(Exception):
                    txt = ev.text or (f"{ev.value:.3g}" if ev.value is not None else "")
                    self.data_logger.log_mfc_pressure(txt)

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

        # --- 입력 읽기 ---
        # MFC1만 사용: on/set/off는 유량으로 처리
        def _f(leaf: str, default: float) -> float:
            t = (self._get_text(leaf) or "").strip()
            try:
                return float(t) if t else float(default)
            except Exception:
                return float(default)

        # 가스: Ar/O2 중 하나 선택 → 해당 flow를 초기 유량으로 사용
        use_ar = bool(getattr(self._u("Ar_checkbox"), "isChecked", lambda: False)())
        use_o2 = bool(getattr(self._u("O2_checkbox"), "isChecked", lambda: False)())
        if not (use_ar or use_o2):
            self._post_warning("선택 오류", "Ar 또는 O2를 하나 이상 선택하세요.")
            return

        ar_flow = _f("arFlow_edit", 0.0) if use_ar else 0.0
        o2_flow = _f("o2Flow_edit", 0.0) if use_o2 else 0.0
        init_flow = ar_flow if use_ar else o2_flow   # MFC1 초기 유량

        rf_w      = _f("rfPower_edit", 100.0) or _f("dcPower_edit", 100.0)
        if rf_w <= 0:
            self._post_warning("입력값 확인", "RF Power(W)를 확인하세요.")
            return

        targetP   = _f("workingPressure_edit", 2.0)     # mTorr
        tolP      = _f("pressureTolerance_edit", 0.2)   # mTorr (없으면 기본)
        timeout_s = _f("pressureTimeout_edit", 90.0)    # sec
        settle_s  = _f("pressureSettle_edit", 5.0)      # sec

        # UI가 초 단위일 가능성 높아 변환: processTime_edit(초) → 분
        t_proc_s  = _f("processTime_edit", 60.0)
        hold_min  = max(0.0, t_proc_s / 60.0)

        params = {
            "process_note": "PlasmaCleaning",
            "pc_gas_mfc_idx": 1,                    # ★ MFC1 고정
            "pc_gas_flow_sccm": init_flow,         # on/set flow
            "pc_target_pressure_mTorr": targetP,
            "pc_pressure_tolerance": tolP,
            "pc_pressure_timeout_s": timeout_s,
            "pc_pressure_settle_s":  settle_s,
            "pc_rf_power_w": rf_w,
            "pc_rf_reach_timeout_s": 0.0,          # 목표 도달 확인 생략(필요 시 UI 추가)
            "pc_process_time_min":   hold_min,     # 분
        }

        # 로그 파일 준비
        if not getattr(self, "_log_file_path", None):
            self._prepare_log_file(params)

        # 백그라운드 보장 + 시작
        self._auto_connect_enabled = True
        self._ensure_background_started()
        self._on_process_status_changed(True)

        # maybe_start: 이미 켜져 있어도 안전(멱등)
        self._spawn_detached(self.mfc.start())

        try:
            self.plasma.start(params)  # ★ 컨트롤러의 공식 시작 API
        except Exception as e:
            self._post_critical("시작 실패", f"플라즈마 클리닝 시작 실패: {e!r}")
            self._on_process_status_changed(False)

    def _handle_stop_clicked(self, _checked: bool = False):
        self.request_stop_all(user_initiated=True)

    def request_stop_all(self, user_initiated: bool):
        self._auto_connect_enabled = False
        self._spawn_detached(self._stop_all())

    async def _stop_all(self):
        try:
            if getattr(self.plasma, "is_running", False):
                self.plasma.request_stop()
        except Exception:
            pass
        # RF 끄기: PLC 0 W
        with contextlib.suppress(Exception):
            await self._plc_rf_off()
        # 장치 정리
        with contextlib.suppress(Exception):
            await self.mfc.cleanup()
        self._bg_started = False
        self._devices_started = False
        self._on_process_status_changed(False)

    # ─────────────────────────────────────────────────────────────
    # 표시/상태/UI 유틸
    # ─────────────────────────────────────────────────────────────
    def append_log(self, source: str, msg: str) -> None:
        now = datetime.now()
        ui_line = f"[{now:%H:%M:%S}] [PC:{source}] {msg}"
        if self._w_log:
            self._w_log.moveCursor(QTextCursor.MoveOperation.End)
            self._w_log.insertPlainText(ui_line + "\n")

        if not getattr(self, "_log_file_path", None):
            return
        try:
            if self._log_fp is None:
                self._log_fp = open(self._log_file_path, "a", encoding="utf-8", newline="")
            self._log_fp.write(f"[{now:%Y-%m-%d %H:%M:%S}] [PC:{source}] {msg}\n")
            self._log_fp.flush()
        except Exception:
            pass

    def _apply_process_state_message(self, text: str) -> None:
        if self._w_state:
            self._w_state.setPlainText(text)

    def _on_process_status_changed(self, running: bool) -> None:
        s = self._u("Start_button"); t = self._u("Stop_button")
        if s: s.setEnabled(not running)
        if t: t.setEnabled(True)

    def _ensure_log_dir(self, root: Path) -> Path:
        root.mkdir(parents=True, exist_ok=True)
        return root

    def _prepare_log_file(self, params: Mapping[str, Any]) -> None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = self._log_dir / f"PC_{ts}.txt"
        self._log_file_path = base
        self.append_log("Logger", f"새 로그 파일 시작: {base.name}")
        self.append_log("MAIN", "=== Plasma Cleaning 준비 ===")

    def _set_default_ui_values(self) -> None:
        self._set("processState_edit", "대기 중")

    # ---- 작은 유틸들 ----
    def _u(self, name: str) -> Any | None:
        name = self._alias_leaf(name)
        return getattr(self.ui, f"{self.prefix}{name}", None) if getattr(self, "ui", None) else None

    def _alias_leaf(self, leaf: str) -> str:
        # 필요 시 PC 페이지 위젯명 매핑
        mapping = {
            # "rfPower_edit": "dcPower_edit",
        }
        return mapping.get(leaf, leaf)

    def _set(self, leaf: str, v: Any) -> None:
        w = self._u(leaf)
        if not w: return
        try:
            if hasattr(w, "setChecked"):
                w.setChecked(bool(v)); return
            if hasattr(w, "setValue"):
                with contextlib.suppress(Exception):
                    w.setValue(float(v)); return
            s = str(v)
            if hasattr(w, "setPlainText"):
                w.setPlainText(s); return
            if hasattr(w, "setText"):
                w.setText(s); return
        except Exception:
            pass

    def _get_text(self, leaf: str) -> str:
        w = self._u(leaf)
        return w.toPlainText().strip() if w else ""

    def _has_ui(self) -> bool:
        try:
            return QApplication.instance() is not None
        except Exception:
            return False

    def _is_connected(self, dev: object) -> bool:
        try:
            v = getattr(dev, "is_connected", None)
            if callable(v): return bool(v())
            if isinstance(v, bool): return v
        except Exception:
            pass
        try:
            return bool(getattr(dev, "_connected", False))
        except Exception:
            return False

    # --- 메시지박스(경고/치명) ---
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

    # --- 태스크 유틸 ---
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
                await self._plc_rf_off()
            with contextlib.suppress(Exception):
                await self.mfc.cleanup()
            with contextlib.suppress(Exception):
                if self._log_fp: self._log_fp.flush(); self._log_fp.close()
        self._spawn_detached(run())

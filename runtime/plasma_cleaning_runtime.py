# runtime/plasma_cleaning_runtime.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio, contextlib, inspect, traceback, time
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

    - 가스밸브(PLC) 제어 없음: 밸브는 항상 Open으로 가정
    - MFC 컨트롤러 **#1** 사용, 가스 라인 **#3** 고정 (on/set/off는 유량으로 처리: 0 = off)
    - **Working Pressure**: MFC의 **SP4 set/on/off**로 목표 압력 유지
    - **Target Pressure**: **IG**를 읽어서 목표 도달(±tol, settle 유지)까지 **대기**
    - RF 파워는 펄스 없이 '연속'만, **PLC DAC**로 적용/읽기/끄기
    """

    def __init__(
        self,
        ui: Any,
        prefix: str,
        loop: asyncio.AbstractEventLoop,
        cfg: Any,
        log_dir: Path,
        *,
        plc: AsyncPLC | None = None,   # PLC는 RF DAC 용
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

        # 로그/상태 위젯 포인터
        self._w_log: QPlainTextEdit | None = self._u("logMessage_edit")
        self._w_state: QPlainTextEdit | None = self._u("processState_edit")

        # 그래프(PC 페이지에 없으면 내부에서 무시)
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

        # 최근 IG(mTorr) 캐시
        self._last_ig_mTorr: float | None = None
        # IG 대기용 타깃 저장 (컨트롤러가 넘겨주는 target과 별도로 유지)
        self._ig_wait_target: float | None = None

        # 컨트롤러 바인딩
        self._bind_plasma_controller()

        # 버튼 연결/초기값
        self._connect_my_buttons()
        self._set_default_ui_values()

    # ─────────────────────────────────────────────────────────────
    # RF(PLC) & SP4(MFC) & IG 헬퍼
    # ─────────────────────────────────────────────────────────────
    async def _plc_rf_apply(self, power_w: float) -> None:
        if not self.plc:
            raise RuntimeError("PLC 없음(RF 적용 실패)")
        # 규약: family="DCV", channel=1 → RF 채널
        await self.plc.power_apply(float(power_w), family="DCV", ensure_set=True, channel=1)

    async def _plc_rf_off(self) -> None:
        if not self.plc:
            return
        await self.plc.power_write(0.0, family="DCV", write_idx=1)

    async def _plc_rf_read(self) -> tuple[float, float]:
        """forward/reflected 읽기 (가능 시), 실패 시 (0,0)"""
        if not self.plc or not hasattr(self.plc, "read_reg_name"):
            return (0.0, 0.0)
        try:
            fwd_raw = await self.plc.read_reg_name("DCV_READ_2")
            ref_raw = await self.plc.read_reg_name("DCV_READ_3")
            return (float(fwd_raw), float(ref_raw))
        except Exception:
            return (0.0, 0.0)

    async def _mfc_sp4_setpoint(self, mTorr: float) -> None:
        """MFC의 SP4 setpoint 설정 (드라이버가 다르면 다중 시도)"""
        # 1) 전용 메서드가 있으면 사용
        if hasattr(self.mfc, "sp_set"):
            # 일반적으로 sp_set(sp_idx, value_mTorr, ch=?)
            try:
                res = self.mfc.sp_set(4, float(mTorr), ch=1)
            except TypeError:
                res = self.mfc.sp_set(4, float(mTorr))
            if inspect.isawaitable(res):
                await res
            return
        # 2) 커맨드 라우터 경유
        args = {"sp_idx": 4, "value": float(mTorr), "mfc_idx": 1, "ch": 1}
        await self.mfc.handle_command("sp_set", args)

    async def _mfc_sp4_on(self, on: bool) -> None:
        """MFC의 SP4 on/off"""
        if hasattr(self.mfc, "sp_on"):
            try:
                res = self.mfc.sp_on(4, bool(on), ch=1)
            except TypeError:
                res = self.mfc.sp_on(4, bool(on))
            if inspect.isawaitable(res):
                await res
            return
        args = {"sp_idx": 4, "on": bool(on), "mfc_idx": 1, "ch": 1}
        await self.mfc.handle_command("sp_on", args)

    async def _read_ig_mTorr(self) -> float | None:
        """
        IG(mTorr) 읽기:
        - mfc.read_ig() / mfc.read_pressure() / 최근 이벤트 캐시 순으로 시도
        """
        # 1) 전용 메서드
        for meth_name in ("read_ig", "read_pressure", "get_ig", "get_pressure"):
            meth = getattr(self.mfc, meth_name, None)
            if callable(meth):
                try:
                    v = meth()
                    v = await v if inspect.isawaitable(v) else v
                    return float(v)
                except Exception:
                    pass
        # 2) 이벤트 캐시
        return self._last_ig_mTorr

    async def _wait_ig_stable(self, _target_arg: float, tol: float, timeout_s: float, settle_s: float) -> bool:
        """
        컨트롤러에서 넘기는 target은 'SP setpoint'이므로, 여기서는
        런타임이 저장해둔 **IG 타깃(self._ig_wait_target)** 으로 판정한다.
        """
        target = float(self._ig_wait_target) if self._ig_wait_target is not None else float(_target_arg)
        tol = abs(float(tol))
        deadline = time.monotonic() + float(timeout_s)
        settle_until: float | None = None

        self.append_log("IG", f"대기 시작: {target:.3f}±{tol:.3f} mTorr, timeout={timeout_s:.0f}s, settle={settle_s:.0f}s")
        while time.monotonic() < deadline:
            val = await self._read_ig_mTorr()
            if val is not None:
                # UI/로그 업데이트(너무 자주 찍지 않도록 간단히)
                self._set("basePressure_edit", f"{val:.4f}")
                if abs(val - target) <= tol:
                    if settle_until is None:
                        settle_until = time.monotonic() + float(settle_s)
                    if time.monotonic() >= settle_until:
                        self.append_log("IG", f"목표 도달: {val:.3f} mTorr")
                        return True
                else:
                    settle_until = None
            await asyncio.sleep(0.2)
        self.append_log("IG", "타임아웃(목표 미달)")
        return False

    # ─────────────────────────────────────────────────────────────
    # 컨트롤러 바인딩 (MFC1+Gas#3, SP4, RF 연속)
    # ─────────────────────────────────────────────────────────────
    def _bind_plasma_controller(self) -> None:
        # MFC: on/set/off 모두 "유량"으로 처리. mfc_idx=1, gas_idx=3 강제
        def cb_mfc(cmd: str, args: Mapping[str, Any]) -> None:
            a = dict(args or {})
            a["mfc_idx"] = 1
            a["gas_idx"] = 3
            self._spawn_detached(self.mfc.handle_command(cmd, a))

        # SP4 set (컨트롤러의 sp1_set 콜백 자리에 매핑)
        def cb_sp1_set(mTorr: float) -> None:
            async def run():
                try:
                    await self._mfc_sp4_setpoint(float(mTorr))
                except Exception as e:
                    self.append_log("SP4", f"set 실패: {e!r}")
            self._spawn_detached(run())

        # SP4 on/off (컨트롤러의 sp1_on 콜백 자리에 매핑)
        def cb_sp1_on(on: bool) -> None:
            async def run():
                try:
                    await self._mfc_sp4_on(bool(on))
                except Exception as e:
                    self.append_log("SP4", f"on/off 실패: {e!r}")
            self._spawn_detached(run())

        # RF 파워 적용 (PLC DAC)
        def cb_rf_power(value: float) -> None:
            async def run():
                try:
                    await self._plc_rf_apply(float(value))
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

        # 바인딩
        self.plasma = PlasmaCleaningController(
            ch=1,  # PC는 CH1 리소스 사용
            send_mfc=cb_mfc,
            sp1_set=cb_sp1_set,   # ← SP4 set
            sp1_on=cb_sp1_on,     # ← SP4 on/off
            send_rf_power=cb_rf_power,
            stop_rf_power=cb_rf_off,
            wait_pressure_stable=self._wait_ig_stable,  # ← IG 기반 대기
            await_rf_target=None,
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

        await _maybe_start_or_connect(self.plc, "PLC")       # RF용
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
            elif k == "pressure":  # IG 또는 챔버 압력 이벤트라고 가정
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
        def _f(leaf: str, default: float) -> float:
            t = (self._get_text(leaf) or "").strip()
            try:
                return float(t) if t else float(default)
            except Exception:
                return float(default)

        # Gas Flow (sccm) → MFC1 + gas_idx=3로 on/set/off
        gas_flow = _f("gasFlow_edit", 0.0)

        # RF Power (W) → PLC DAC
        rf_w = _f("rfPower_edit", 100.0)
        if rf_w <= 0:
            self._post_warning("입력값 확인", "RF Power(W)를 확인하세요.")
            return

        # Working Pressure → SP4 setpoint
        workingP = _f("workingPressure_edit", 2.0)  # mTorr

        # Target Pressure → IG 대기용
        ig_target = _f("targetPressure_edit", workingP)  # mTorr
        self._ig_wait_target = ig_target

        # 대기 파라미터(기본값)
        tolP      = 0.2   # mTorr
        timeout_s = 90.0  # sec
        settle_s  = 5.0   # sec

        # Process Time [min]
        hold_min = max(0.0, _f("ProcessTime_edit", 1.0))

        params = {
            "process_note": "PlasmaCleaning",
            # 주의: 컨트롤러는 pc_gas_mfc_idx를 gas_idx로 사용하므로 '3(가스라인)'을 넣는다.
            "pc_gas_mfc_idx": 3,                   # ← gas_idx=3 (N2 라인)
            "pc_gas_flow_sccm": gas_flow,          # on/set flow
            # 컨트롤러는 sp1_set(target)을 먼저 호출하므로 여기에 'Working P'를 전달
            "pc_target_pressure_mTorr": workingP,  # ← SP4 setpoint로 사용됨
            "pc_pressure_tolerance": tolP,
            "pc_pressure_timeout_s": timeout_s,
            "pc_pressure_settle_s":  settle_s,
            "pc_rf_power_w": rf_w,
            "pc_rf_reach_timeout_s": 0.0,
            "pc_process_time_min":   hold_min,
        }

        # 로그 파일 준비
        if not getattr(self, "_log_file_path", None):
            self._prepare_log_file(params)

        # 백그라운드 보장 + 시작
        self._auto_connect_enabled = True
        self._ensure_background_started()
        self._on_process_status_changed(True)

        # 멱등 start
        self._spawn_detached(self.mfc.start())

        try:
            self.plasma.start(params)
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
        # RF 끄기
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
        """
        PC 페이지는 대소문자/접두사 혼재(PC_*, pc_*). 다음 순서로 탐색:
        - f"{self.prefix}{name}"  (예: 'pc_' + 'Start_button' -> pc_Start_button)
        - "PC_" + name            (예: PC_Start_button, PC_rfPower_edit)
        - "pc_" + name            (예: pc_logMessage_edit)
        - name 그대로
        """
        if not getattr(self, "ui", None):
            return None
        candidates = []
        if self.prefix:
            candidates.append(f"{self.prefix}{name}")
        candidates.extend((f"PC_{name}", f"pc_{name}", name))
        for cand in candidates:
            w = getattr(self.ui, cand, None)
            if w is not None:
                return w
        return None

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

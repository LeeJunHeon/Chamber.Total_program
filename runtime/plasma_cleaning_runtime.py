# runtime/plasma_cleaning_runtime.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import contextlib
import inspect
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from PySide6.QtGui import QTextCursor
from PySide6.QtWidgets import QMessageBox, QPlainTextEdit, QApplication

# 장비/컨트롤러
from device.mfc import AsyncMFC
from device.plc import AsyncPLC
from device.ig import AsyncIG  # IG 직접 주입 지원
from controller.plasma_cleaning_controller import PlasmaCleaningController, PCParams
from device.rf_power import RFPowerAsync, RFPowerEvent


class PlasmaCleaningRuntime:
    """
    Plasma Cleaning 전용 런타임 (그래프/데이터로거 미사용 버전)

    - 장치 인스턴스는 main.py에서 생성 후 주입
      * MFC 가스 유량:  mfc_gas
      * MFC SP4(Working Pressure): mfc_sp4
      * IG: set_ig_callbacks(ensure_on, read_mTorr) 또는 ig 직접 주입
      * PLC: RF 연속 출력(DCV ch=1)용
    - start/stop 버튼 → PlasmaCleaningController로 연결
    - RF는 RFPowerAsync 상태머신 사용
    """

    # =========================
    # 생성/초기화
    # =========================
    def __init__(
        self,
        ui: Any,
        prefix: str,
        loop: asyncio.AbstractEventLoop,
        *,
        plc: Optional[AsyncPLC],
        mfc_gas: Optional[AsyncMFC],
        mfc_sp4: Optional[AsyncMFC],
        log_dir: Path,                     # 시그니처 유지(내부 미사용)
        chat: Optional[Any] = None,
        ig: Optional[AsyncIG] = None,      # IG 객체 직접 주입 가능
    ) -> None:
        self.ui = ui
        self.prefix = str(prefix)
        self._loop = loop
        self.chat = chat
        self._log_dir = log_dir            # (미사용) 호환용 보관만

        # 주입 장치
        self.plc: Optional[AsyncPLC] = plc
        self.mfc_gas: Optional[AsyncMFC] = mfc_gas
        self.mfc_sp4: Optional[AsyncMFC] = mfc_sp4
        self.ig: Optional[AsyncIG] = ig

        # IG 콜백 (라디오 토글 시 main에서 갱신 가능)
        self._ig_ensure_on_cb: Optional[Callable[[], Awaitable[None]]] = None
        self._ig_read_mTorr_cb: Optional[Callable[[], Awaitable[float]]] = None

        # 상태/태스크
        self._bg_tasks: list[asyncio.Task] = []
        self._running: bool = False
        self._selected_ch: int = 1  # 라디오에 맞춰 set_selected_ch로 갱신

        # 로그/상태 위젯
        self._w_log = (
            _safe_get(ui, f"{self.prefix.lower()}logMessage_edit")
            or _safe_get(ui, f"{self.prefix}logMessage_edit")
            or _safe_get(ui, "pc_logMessage_edit")
        )
        self._w_state = (
            _safe_get(ui, f"{self.prefix.lower()}processState_edit")
            or _safe_get(ui, f"{self.prefix}processState_edit")
            or _safe_get(ui, "pc_processState_edit")
        )

        # RF 파워(연속) 바인딩
        self.rf = self._make_rf_async()

        # PlasmaCleaningController 바인딩
        self.pc = self._bind_pc_controller()

        # UI 버튼 연결
        self._connect_ui_buttons()

        # IG 객체가 넘어온 경우, 기본 콜백 자동 바인딩
        if self.ig is not None:
            self._bind_ig_device(self.ig)

        # RF 이벤트 펌프 시작
        self._ensure_task("PC.RFEvents", self._pump_rf_events)

    # =========================
    # 퍼블릭: 바인딩/설정 갱신
    # =========================
    def set_selected_ch(self, ch: int) -> None:
        """main.py 라디오 토글 시 호출 (로그 편의/PLC용)"""
        self._selected_ch = 1 if int(ch) != 2 else 2
        self.append_log("PC", f"Selected Chamber → CH{self._selected_ch}")

    def set_mfcs(self, *, mfc_gas: Optional[AsyncMFC], mfc_sp4: Optional[AsyncMFC]) -> None:
        """
        라디오 선택에 맞춰 가스/SP4용 MFC를 교체.
        (같은 인스턴스를 둘 다에 써도 무방)
        """
        self.mfc_gas = mfc_gas
        self.mfc_sp4 = mfc_sp4
        self.append_log("PC", f"Bind MFC: GAS={_mfc_name(mfc_gas)}, SP4={_mfc_name(mfc_sp4)}")

    def set_ig_callbacks(
        self,
        ensure_on: Callable[[], Awaitable[None]],
        read_mTorr: Callable[[], Awaitable[float]],
    ) -> None:
        """IG 콜백 주입 (라디오 전환 시마다 업데이트)"""
        self._ig_ensure_on_cb = ensure_on
        self._ig_read_mTorr_cb = read_mTorr
        self.append_log("PC", "IG callbacks bound")

    # =========================
    # 내부: 컨트롤러/콜백 바인딩
    # =========================
    def _bind_pc_controller(self) -> PlasmaCleaningController:
        # ---- 로그 콜백
        def _log(src: str, msg: str) -> None:
            self.append_log(src, msg)

        # ---- PLC: GV 인터락/오픈/램프
        async def _plc_check_gv_interlock() -> bool:
            if not self.plc:
                return True
            lamp_key = f"G_V_{self._selected_ch}_OPEN_LAMP"
            return await self.plc.read_bit(lamp_key)

        async def _plc_gv_open() -> None:
            if self.plc:
                await self.plc.gate_valve(self._selected_ch, open=True, momentary=True)

        async def _plc_gv_close() -> None:
            if self.plc:
                await self.plc.gate_valve(self._selected_ch, open=False, momentary=True)

        async def _plc_read_gv_open_lamp() -> bool:
            if not self.plc:
                return True
            return await self.plc.read_bit(f"G_V_{self._selected_ch}_OPEN_LAMP")

        # ---- IG
        async def _ensure_ig_on() -> None:
            if self._ig_ensure_on_cb:
                await self._ig_ensure_on_cb()

        async def _read_ig_mTorr() -> float:
            if not self._ig_read_mTorr_cb:
                raise RuntimeError("IG read callback is not bound")
            v = await self._ig_read_mTorr_cb()
            return float(v)

        # ---- MFC (GAS/SP4)
        async def _mfc_gas_select(gas_idx: int) -> None:
            mfc = self.mfc_gas
            if not mfc:
                raise RuntimeError("mfc_gas not bound")
            # 1) 전용 메서드 우선
            for name in ("gas_select", "select_gas", "set_gas"):
                fn = getattr(mfc, name, None)
                if callable(fn):
                    res = fn(int(gas_idx))
                    await res if inspect.isawaitable(res) else None
                    return
            # 2) 커맨드 디스패치
            await self._mfc_dispatch(mfc, "gas_select", {"gas_idx": int(gas_idx)})

        async def _mfc_flow_set_on(flow_sccm: float) -> None:
            mfc = self.mfc_gas
            if not mfc:
                raise RuntimeError("mfc_gas not bound")
            flow = float(max(0.0, flow_sccm))
            # 전용 API 우선: flow_set + flow_on
            for name in ("flow_set_on",):
                fn = getattr(mfc, name, None)
                if callable(fn):
                    res = fn(flow)
                    await res if inspect.isawaitable(res) else None
                    return
            # 대체: flow_set → flow_on
            fn_set = getattr(mfc, "flow_set", None)
            fn_on = getattr(mfc, "flow_on", None)
            if callable(fn_set):
                res = fn_set(flow)
                await res if inspect.isawaitable(res) else None
            if callable(fn_on):
                res = fn_on(True)
                await res if inspect.isawaitable(res) else None
                return
            # 커맨드 디스패치
            await self._mfc_dispatch(mfc, "flow_set", {"value": flow})
            await self._mfc_dispatch(mfc, "flow_on", {"on": True})

        async def _mfc_flow_off() -> None:
            mfc = self.mfc_gas
            if not mfc:
                return
            for name in ("flow_on",):
                fn = getattr(mfc, name, None)
                if callable(fn):
                    res = fn(False)
                    await res if inspect.isawaitable(res) else None
                    return
            await self._mfc_dispatch(mfc, "flow_on", {"on": False})

        async def _mfc_sp4_set(mTorr: float) -> None:
            mfc = self.mfc_sp4
            if not mfc:
                raise RuntimeError("mfc_sp4 not bound")
            # 전용 API(sp_set) 우선
            fn = getattr(mfc, "sp_set", None)
            if callable(fn):
                try:
                    res = fn(4, float(mTorr))
                except TypeError:
                    res = fn(4, float(mTorr))
                await res if inspect.isawaitable(res) else None
                return
            # 커맨드 디스패치
            await self._mfc_dispatch(mfc, "sp_set", {"sp_idx": 4, "value": float(mTorr)})

        async def _mfc_sp4_on() -> None:
            mfc = self.mfc_sp4
            if not mfc:
                raise RuntimeError("mfc_sp4 not bound")
            fn = getattr(mfc, "sp_on", None)
            if callable(fn):
                res = fn(4, True)
                await res if inspect.isawaitable(res) else None
                return
            await self._mfc_dispatch(mfc, "sp_on", {"sp_idx": 4, "on": True})

        async def _mfc_sp4_off() -> None:
            mfc = self.mfc_sp4
            if not mfc:
                return
            fn = getattr(mfc, "sp_on", None)
            if callable(fn):
                res = fn(4, False)
                await res if inspect.isawaitable(res) else None
                return
            await self._mfc_dispatch(mfc, "sp_on", {"sp_idx": 4, "on": False})

        # ---- RF (PLC DCV ch=1 사용 — enable/write/read)
        async def _rf_start(power_w: float) -> None:
            if not self.rf:
                return
            await self.rf.start_process(float(power_w))

        async def _rf_stop() -> None:
            if self.rf:
                await self.rf.cleanup()

        # ---- UI
        def _show_state(text: str) -> None:
            self._set_state_text(text)

        def _show_countdown(sec: int) -> None:
            self._set_state_text(f"Remaining {int(sec)} s")

        # 컨트롤러 생성
        return PlasmaCleaningController(
            log=_log,
            plc_check_gv_interlock=_plc_check_gv_interlock,
            plc_gv_open=_plc_gv_open,
            plc_gv_close=_plc_gv_close,
            plc_read_gv_open_lamp=_plc_read_gv_open_lamp,
            ensure_ig_on=_ensure_ig_on,
            read_ig_mTorr=_read_ig_mTorr,
            mfc_gas_select=_mfc_gas_select,
            mfc_flow_set_on=_mfc_flow_set_on,
            mfc_flow_off=_mfc_flow_off,
            mfc_sp4_set=_mfc_sp4_set,
            mfc_sp4_on=_mfc_sp4_on,
            mfc_sp4_off=_mfc_sp4_off,
            rf_start=_rf_start,
            rf_stop=_rf_stop,
            show_state=_show_state,
            show_countdown=_show_countdown,
        )

    def _make_rf_async(self) -> Optional[RFPowerAsync]:
        if not self.plc:
            return None

        async def _rf_send(power: float):
            # SET(DCV_SET_1) 보장 + WRITE_1 (ch=1)
            await self.plc.power_apply(float(power), family="DCV", ensure_set=True, channel=1)

        async def _rf_send_unverified(power: float):
            await self.plc.power_write(float(power), family="DCV", write_idx=1)

        async def _rf_request_read():
            try:
                # DCV_READ_2/3을 forward/reflected로 사용
                fwd = await self.plc.read_reg_name("DCV_READ_2")
                ref = await self.plc.read_reg_name("DCV_READ_3")
                return {"forward": float(fwd), "reflected": float(ref)}
            except Exception as e:
                self.append_log("RF", f"read failed: {e!r}")
                return None

        async def _rf_toggle_enable(on: bool):
            # RF SET 래치(DCV_SET_1)
            await self.plc.power_enable(bool(on), family="DCV", set_idx=1)

        return RFPowerAsync(
            send_rf_power=_rf_send,
            send_rf_power_unverified=_rf_send_unverified,
            request_status_read=_rf_request_read,
            toggle_enable=_rf_toggle_enable,
            poll_interval_ms=1000,
            rampdown_interval_ms=50,
        )

    # =========================
    # UI/이벤트
    # =========================
    def _connect_ui_buttons(self) -> None:
        """
        버튼 objectName 변형(예: PC_Start_button / pcStart_button 등)을 모두 수용.
        """
        w_start = _find_first(self.ui, [
            f"{self.prefix}Start_button",
            f"{self.prefix}StartButton",
            f"{self.prefix.lower()}Start_button",
            f"{self.prefix.lower()}StartButton",
            "PC_Start_button",
            "pcStart_button",
        ])
        w_stop = _find_first(self.ui, [
            f"{self.prefix}Stop_button",
            f"{self.prefix}StopButton",
            f"{self.prefix.lower()}Stop_button",
            f"{self.prefix.lower()}StopButton",
            "PC_Stop_button",
            "pcStop_button",
        ])

        if w_start:
            w_start.clicked.connect(lambda: asyncio.ensure_future(self._on_click_start()))
        if w_stop:
            w_stop.clicked.connect(lambda: asyncio.ensure_future(self._on_click_stop()))

    async def _on_click_start(self) -> None:
        if self._running:
            self.append_log("PC", "이미 실행 중입니다.")
            return

        # 파라미터 수집 (UI에서 못 읽으면 기본값; '1e-5' 과학표기 허용)
        p = self._read_params_from_ui()

        # (삭제됨) 세션 디렉터리 생성/로깅 — DataLogger 미사용

        # 컨트롤러 시퀀스 시작
        self._running = True
        try:
            await self.pc._run(p)  # PlasmaCleaningController가 공정 전체 수행
        except Exception as e:
            self.append_log("PC", f"오류: {e!r}")
        finally:
            self._running = False
            # (삭제됨) self._logger.end_session()
            await self._safe_rf_stop()
            self._set_state_text("IDLE")

    async def _on_click_stop(self) -> None:
        # 컨트롤러 안전 정지 (RF 정지/가스 off/SP4 off는 컨트롤러 내부에서도 호출)
        await self._safe_rf_stop()
        self._running = False
        self._set_state_text("STOPPED")
        self.append_log("PC", "사용자에 의해 중지")

    async def _safe_rf_stop(self) -> None:
        with contextlib.suppress(Exception):
            if self.rf:
                await self.rf.cleanup()

    async def _pump_rf_events(self) -> None:
        """RFPowerAsync 이벤트를 UI/로그로 중계"""
        if not self.rf:
            return
        async for ev in self.rf.events():
            if ev.kind == "display":
                self._set_state_text(f"RF FWD={ev.forward:.1f}, REF={ev.reflected:.1f} (W)")
            elif ev.kind == "status":
                self.append_log("RF", ev.message or "")
            elif ev.kind == "target_reached":
                self.append_log("RF", "목표 파워 도달")

    # =========================
    # 내부 헬퍼들
    # =========================
    async def _mfc_dispatch(self, mfc: AsyncMFC, cmd: str, args: Optional[dict] = None) -> None:
        """
        드라이버가 메서드를 직접 제공하지 않을 때, 공용 커맨드 라우터로 보냅니다.
        프로젝트의 AsyncMFC는 내부에 단일 큐를 두는 구조입니다.
        """
        fn = getattr(mfc, "handle_command", None)
        if not callable(fn):
            raise RuntimeError("AsyncMFC.handle_command 가 없습니다.")
        await fn(cmd, args or {})

    def _read_params_from_ui(self) -> PCParams:
        def _read_plain_number(obj_name: str, default: float) -> float:
            w = _safe_get(self.ui, obj_name)
            if not w:
                return default
            try:
                if hasattr(w, "toPlainText"):
                    txt = w.toPlainText().strip()
                elif hasattr(w, "text"):
                    txt = w.text().strip()
                elif hasattr(w, "value"):
                    return float(w.value())
                else:
                    txt = str(w)
                return float(txt) if txt else default  # 과학표기(1e-5) 허용
            except Exception:
                return default

        gas_flow        = _read_plain_number("PC_gasFlow_edit",        0.0)
        target_pressure = _read_plain_number("PC_targetPressure_edit", 2.0)
        sp4_setpoint    = _read_plain_number("PC_workingPressure_edit", 2.0)
        rf_power        = _read_plain_number("PC_rfPower_edit",        100.0)
        process_time    = _read_plain_number("PC_ProcessTime_edit",    1.0)

        return PCParams(
            gas_idx               = 3,             # Gas #3 (N₂) 고정
            gas_flow_sccm         = gas_flow,
            target_pressure_mTorr = target_pressure,
            tol_mTorr             = 0.2,
            wait_timeout_s        = 90.0,
            settle_s              = 5.0,
            sp4_setpoint_mTorr    = sp4_setpoint,
            rf_power_w            = rf_power,
            process_time_min      = process_time,
        )

    def _set_state_text(self, text: str) -> None:
        if not self._w_state:
            return
        try:
            self._w_state.setPlainText(str(text))
        except Exception:
            pass

    def append_log(self, src: str, msg: str) -> None:
        line = f"[{datetime.now().strftime('%H:%M:%S')}] {src}: {msg}"
        if self._w_log:
            try:
                self._w_log.appendPlainText(line)
                self._w_log.moveCursor(QTextCursor.End)
            except Exception:
                pass

    def shutdown_fast(self) -> None:
        # 공유 장치이므로 close() 등은 호출하지 않습니다.
        for t in self._bg_tasks:
            t.cancel()

    def _ensure_task(self, name: str, coro_fn: Callable[[], Awaitable[None]]) -> None:
        t = asyncio.ensure_future(coro_fn(), loop=self._loop)
        t.set_name(name)
        self._bg_tasks.append(t)

    # IG 객체로부터 기본 콜백 구성
    def _bind_ig_device(self, ig: AsyncIG) -> None:
        async def _ensure_on():
            fn = getattr(ig, "ensure_on", None) or getattr(ig, "turn_on", None)
            if callable(fn):
                await fn()

        async def _read_mTorr() -> float:
            read_fn = getattr(ig, "read_pressure", None)
            if not callable(read_fn):
                raise RuntimeError("IG object does not provide read_pressure()")
            torr = float(await read_fn())
            return torr * 1000.0

        self.set_ig_callbacks(_ensure_on, _read_mTorr)


# ─────────────────────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────────────────────
def _safe_get(ui: Any, name: str) -> Any:
    with contextlib.suppress(Exception):
        return getattr(ui, name)
    return None

def _find_first(ui: Any, names: list[str]) -> Any:
    for n in names:
        w = _safe_get(ui, n)
        if w is not None:
            return w
    return None

def _mfc_name(m: Optional[AsyncMFC]) -> str:
    if not m:
        return "None"
    host = getattr(m, "host", None) or getattr(m, "_override_host", None) or "?"
    port = getattr(m, "port", None) or getattr(m, "_override_port", None) or "?"
    return f"{host}:{port}"

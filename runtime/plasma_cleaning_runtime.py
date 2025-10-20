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
      * MFC SP4(Working Pressure): mfc_pressure
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
        mfc_pressure: Optional[AsyncMFC],
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
        self.mfc_pressure: Optional[AsyncMFC] = mfc_pressure
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

    # =========================
    # MFC/IG 이벤트 펌프
    # =========================
    async def _pump_mfc_events(self, mfc, label: str) -> None:
        if not mfc:
            return
        async for ev in mfc.events():
            k = getattr(ev, "kind", None)
            if k == "status":
                self.append_log(label, ev.message or "")
            elif k == "command_confirmed":
                self.append_log(label, f"OK: {ev.cmd or ''}")
            elif k == "command_failed":
                self.append_log(label, f"FAIL: {ev.cmd or ''} ({ev.reason or 'unknown'})")
            elif k == "flow":
                gas  = getattr(ev, "gas", "") or ""
                flow = float(getattr(ev, "value", 0.0) or 0.0)
                self.append_log(label, f"[poll] {gas}: {flow:.2f} sccm")
            elif k == "pressure":
                txt = ev.text or (f"{ev.value:.3g}" if ev.value is not None else "")
                self.append_log(label, f"[poll] ChamberP: {txt}")

    async def _pump_ig_events(self, label: str) -> None:
        if not self.ig:
            return
        async for ev in self.ig.events():
            k = getattr(ev, "kind", None)
            if k == "status":
                self.append_log(label, ev.message or "")
            elif k == "pressure":
                p = getattr(ev, "pressure", None)
                txt = f"{p:.3e} Torr" if isinstance(p, (int, float)) else (ev.message or "")
                self.append_log(label, f"[poll] {txt}")
            elif k == "base_reached":
                self.append_log(label, "Base pressure reached")
            elif k == "base_failed":
                self.append_log(label, f"Base pressure failed: {ev.message or ''}")

    # =========================
    # 장비 연결
    # =========================
    # 클래스 내부에 추가
    def _is_dev_connected(self, dev) -> bool:
        try:
            fn = getattr(dev, "is_connected", None)
            if callable(fn):
                return bool(fn())
            # 일부 디바이스는 내부 플래그만 있을 수 있음
            return bool(getattr(dev, "_connected", False))
        except Exception:
            return False

    async def _preflight_connect(self, timeout_s: float = 10.0) -> None:
        """공정 시작 전 장비 연결 보장. 모두 연결되면 리턴, 아니면 예외."""
        self.append_log("PC", "프리플라이트: 장비 연결 확인/시작")
        need: list[tuple[str, object]] = []
        if self.plc:      need.append(("PLC", self.plc))
        if self.mfc_gas:  need.append(("MFC(GAS)", self.mfc_gas))
        if self.mfc_pressure:  need.append(("MFC(SP4)", self.mfc_pressure))
        if self.ig:       need.append(("IG", self.ig))

        # 1) 미연결이면 start/connect(or PLC 핸드셰이크) 시도
        for name, dev in need:
            if self._is_dev_connected(dev):
                self.append_log("PC", f"{name} 이미 연결됨")
                continue
            try:
                if name == "PLC":
                    # PLC는 첫 I/O에서 연결 → 무해한 coil 읽기로 핸드셰이크
                    await dev.read_coil(0)
                    self.append_log("PC", "PLC 핸드셰이크(read_coil 0)")
                else:
                    for m in ("start", "connect"):
                        fn = getattr(dev, m, None)
                        if callable(fn):
                            res = fn()
                            if inspect.isawaitable(res):
                                await res
                            self.append_log("PC", f"{name} {m} 호출")
                            break
            except Exception as e:
                raise RuntimeError(f"{name} 연결 실패: {e!r}")

        # 2) 타임아웃 내 모두 연결되었는지 대기
        deadline = asyncio.get_running_loop().time() + float(timeout_s)
        while True:
            missing = [n for n, d in need if not self._is_dev_connected(d)]
            if not missing:
                break
            if asyncio.get_running_loop().time() >= deadline:
                raise RuntimeError(f"장비 연결 타임아웃: {', '.join(missing)}")
            await asyncio.sleep(0.5)

        # 3) 이벤트 펌프 기동 (중복 방지)
        if not hasattr(self, "_event_tasks"):
            self._event_tasks = []

        def _has_task(name: str) -> bool:
            return any(getattr(t, "get_name", lambda: "")() == name for t in self._event_tasks)

        if self.rf and not _has_task("PC.Pump.RF"):
            self._event_tasks.append(asyncio.create_task(self._pump_rf_events(), name="PC.Pump.RF"))

        if self.mfc_gas and not _has_task("PC.Pump.MFC.GAS"):
            self._event_tasks.append(asyncio.create_task(self._pump_mfc_events(self.mfc_gas, "MFC(GAS)"), name="PC.Pump.MFC.GAS"))

        if self.mfc_pressure and not _has_task("PC.Pump.MFC.SP4"):
            sel = f"CH{self._selected_ch}"
            self._event_tasks.append(asyncio.create_task(self._pump_mfc_events(self.mfc_pressure, f"MFC(SP4-{sel})"), name="PC.Pump.MFC.SP4"))

        if self.ig and not _has_task("PC.Pump.IG"):
            sel = f"CH{self._selected_ch}"
            self._event_tasks.append(asyncio.create_task(self._pump_ig_events(f"IG-{sel}"), name="PC.Pump.IG"))

    # =========================
    # 퍼블릭: 바인딩/설정 갱신
    # =========================
    def set_selected_ch(self, ch: int) -> None:
        """main.py 라디오 토글 시 호출 (로그 편의/PLC용)"""
        self._selected_ch = 1 if int(ch) != 2 else 2
        self.append_log("PC", f"Selected Chamber → CH{self._selected_ch}")

    def set_mfcs(self, *, mfc_gas: Optional[AsyncMFC], mfc_pressure: Optional[AsyncMFC]) -> None:
        """
        라디오 선택에 맞춰 가스/SP4용 MFC를 교체.
        (같은 인스턴스를 둘 다에 써도 무방)
        """
        self.mfc_gas = mfc_gas
        self.mfc_pressure = mfc_pressure
        self.append_log("PC", f"Bind MFC: GAS={_mfc_name(mfc_gas)}, SP4={_mfc_name(mfc_pressure)}")

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
            key = f"G_V_{self._selected_ch}_인터락"  # CH1→G_V_1_인터락, CH2→G_V_2_인터락
            return await self.plc.read_bit(key)

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
                self.append_log("IG", "ensure ON")
                await self._ig_ensure_on_cb()

        async def _read_ig_mTorr() -> float:
            if not self._ig_read_mTorr_cb:
                raise RuntimeError("IG read callback is not bound")
            v = await self._ig_read_mTorr_cb()
            self.append_log("IG", f"read {float(v):.3e} Torr")
            return float(v)

        async def _ig_wait_for_base_torr(target_torr: float, interval_ms: int = 1000) -> bool:
            if not self.ig:
                raise RuntimeError("IG not bound")
            return await self.ig.wait_for_base_pressure(float(target_torr), interval_ms=interval_ms)

        # ---- MFC (GAS/SP4)
        # 기존 _mfc_gas_select(gas_idx) 바디를 아래로 교체
        async def _mfc_gas_select(gas_idx: int) -> None:
            # GasFlow는 정책상 항상 MFC1의 ch=3이므로, 전달된 값도 ch로 저장만
            self._gas_channel = int(gas_idx)
            self.append_log("PC", f"GasFlow → MFC1 ch{self._gas_channel}")

        async def _mfc_flow_set_on(flow_sccm: float) -> None:
            mfc = self.mfc_gas
            if not mfc:
                raise RuntimeError("mfc_gas not bound")
            ch   = getattr(self, "_gas_channel", 3)
            flow = float(max(0.0, flow_sccm))

            # ✔ mfc.py 정식 API만 사용
            await mfc.set_flow(ch, flow)
            await mfc.flow_on(ch)

        async def _mfc_flow_off() -> None:
            mfc = self.mfc_gas
            if not mfc:
                return
            ch = getattr(self, "_gas_channel", 3)
            await mfc.flow_off(ch)  # ✔ 정식 API

        async def _mfc_sp4_set(mTorr: float) -> None:
            mfc = self.mfc_pressure
            if not mfc:
                raise RuntimeError("mfc_pressure not bound")
            await mfc.sp4_set(float(mTorr))      # ✔ 정식 API

        async def _mfc_sp4_on() -> None:
            mfc = self.mfc_pressure
            if not mfc:
                raise RuntimeError("mfc_pressure not bound")
            #await mfc.valve_open()               # ✔ 밸브는 pressure MFC에서만
            await mfc.sp4_on()                   # ✔ 정식 API

        async def _mfc_sp4_off() -> None:
            mfc = self.mfc_pressure
            if not mfc:
                return
            await mfc.valve_open()              # ✔ 정식 API

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
            ig_wait_for_base_torr=_ig_wait_for_base_torr,
        )

    def _make_rf_async(self) -> Optional[RFPowerAsync]:
        if not self.plc:
            return None

        async def _rf_send(power: float):
            p = float(power)
            self.append_log("PLC", f"DCV WRITE ch1 <- {p:.1f} W (SET+WRITE)")
            await self.plc.power_apply(p, family="DCV", ensure_set=True, channel=1)
            try:
                fwd = await self.plc.read_reg_name("DCV_READ_2")
                ref = await self.plc.read_reg_name("DCV_READ_3")
                self.append_log("PLC", f"ADC READ FWD={float(fwd):.1f} W, REF={float(ref):.1f} W")
            except Exception as e:
                self.append_log("PLC", f"ADC READ 실패: {e!r}")

        async def _rf_send_unverified(power: float):
            p = float(power)
            self.append_log("PLC", f"DCV WRITE ch1(unverified) <- {p:.1f} W")
            await self.plc.power_write(p, family="DCV", write_idx=1)
            try:
                fwd = await self.plc.read_reg_name("DCV_READ_2")
                ref = await self.plc.read_reg_name("DCV_READ_3")
                self.append_log("PLC", f"ADC READ FWD={float(fwd):.1f} W, REF={float(ref):.1f} W")
            except Exception as e:
                self.append_log("PLC", f"ADC READ 실패: {e!r}")

        async def _rf_request_read():
            try:
                fwd = await self.plc.read_reg_name("DCV_READ_2")
                ref = await self.plc.read_reg_name("DCV_READ_3")
                # (선택) 원시 ADC를 로그로도 남기고 싶으면 한 줄 추가
                # self.append_log("PLC", f"ADC READ FWD={float(fwd):.1f} W, REF={float(ref):.1f} W")
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

        # 1) 파라미터 수집
        p = self._read_params_from_ui()

        # 2) 프리플라이트(연결/이벤트펌프) — 실패 시 바로 종료
        try:
            await self._preflight_connect(timeout_s=10.0)
        except Exception as e:
            self.append_log("PC", f"오류: {e!r}")
            return

        # 3) 프로세스 시작 알림(폴링/이벤트 등 ON)
        for dev in (self.mfc_gas, self.mfc_pressure, self.ig):
            try:
                if dev and hasattr(dev, "set_process_status"):
                    await dev.set_process_status(True)
            except Exception as e:
                self.append_log("PC", f"경고: set_process_status 실패: {e!r}")

        # 4) 컨트롤러 실행
        success = False
        self._running = True
        try:
            await self.pc._run(p)   # 공정 전체 수행(Stop 누르면 내부에서 안전 종료)
            success = True
        except Exception as e:
            self.append_log("PC", f"오류: {e!r}")
        finally:
            self._running = False
            self._set_state_text("IDLE")

            # 5) 프로세스 종료 알림(폴링/이벤트 등 OFF + 결과 반영)
            for dev in (self.mfc_gas, self.mfc_pressure, self.ig):
                try:
                    if dev and hasattr(dev, "set_process_status"):
                        await dev.set_process_status(False)
                    if dev and hasattr(dev, "on_process_finished"):
                        await dev.on_process_finished(success=success)
                except Exception as e:
                    self.append_log("PC", f"경고: on_process_finished 실패: {e!r}")

    async def _on_click_stop(self) -> None:
        # 1) 컨트롤러 루프 중단 요청
        with contextlib.suppress(Exception):
            if hasattr(self, "pc") and self.pc:
                self.pc.request_stop()

        # 2) RF 정리(즉시 감압 필요 시)
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
        target_pressure = _read_plain_number("PC_targetPressure_edit", 5.0e-6)
        sp4_setpoint    = _read_plain_number("PC_workingPressure_edit", 2.0)
        rf_power        = _read_plain_number("PC_rfPower_edit",        100.0)
        process_time    = _read_plain_number("PC_ProcessTime_edit",    1.0)

        return PCParams(
            gas_idx               = 3,             # Gas #3 (N₂) 고정
            gas_flow_sccm         = gas_flow,
            target_pressure       = target_pressure,
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
        for t in getattr(self, "_bg_tasks", []):
            t.cancel()
        for t in getattr(self, "_event_tasks", []):
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

# runtime/plasma_cleaning_runtime.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio, contextlib, inspect, csv, os, time
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, Mapping
from collections import deque

from PySide6.QtGui import QTextCursor
from PySide6.QtWidgets import QMessageBox, QPlainTextEdit, QApplication, QWidget
from PySide6.QtCore import Qt, QTimer   # ⬅ 챔버와 동일한 모달리티/속성 적용용

try:
    from shiboken6 import isValid as _qt_is_valid
except Exception:
    def _qt_is_valid(obj: Any) -> bool:
        return obj is not None

from util.timed_popup import attach_autoclose

# 장비/컨트롤러
from device.mfc import AsyncMFC
from device.plc import AsyncPLC
from device.ig import AsyncIG  # IG 직접 주입 지원
from controller.plasma_cleaning_controller import PlasmaCleaningController, PCParams
from device.rf_power import RFPowerAsync, RFPowerEvent
from controller.runtime_state import runtime_state  # ★ 추가: 전역 쿨다운/이력

from lib import user_config
from lib import config_common as cfgc

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
        disconnect_on_finish: bool = False,
    ) -> None:
        self.ui = ui
        self.prefix = str(prefix)
        self._loop = loop
        self.chat = chat
        self._log_dir = log_dir            # 기본 로그 루트
        self._pc_log_dir = (log_dir / "Plasma_Cleaning")
        self._pc_log_dir.mkdir(parents=True, exist_ok=True)

        self._log_fp = None                # 현재 런 세션 로그 파일 핸들
        self._log_session_id = None        # 파일명에 들어갈 세션 ID (timestamp)

        # 🔒 종료 챗 exactly-once 보장용 플래그
        self._final_notified: bool = False
        self._stop_requested: bool = False

        # ★ TEST 레시피 실행 중(장비 제어/cleanup 스킵용)
        self._test_mode_active: bool = False

        # ★ 추가: 비모달 경고창 보관(가비지 컬렉션 방지)
        self._msg_boxes: list[QMessageBox] = []

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
        self._event_tasks: list[asyncio.Task] = []   # ★ 추가
        self._running: bool = False
        self._selected_ch: int = 1  # 라디오에 맞춰 set_selected_ch로 갱신
        self._pc_gas_idx: Optional[int] = None  # ← PC에서 선택된 gas_idx 저장(스케일 계산용)

        self._rf_target_evt = asyncio.Event()   # ★ 목표 도달 이벤트 대기용
        
        # ★ 추가: RF 목표 대기 결과(성공/실패)와 사유를 명확히 저장
        self._rf_target_ok: Optional[bool] = None
        self._rf_target_reason: str = ""

        # ★ 추가(권장): RF 실패가 "stop"으로 오염되는 것 방지용 최종 보정 플래그
        self._forced_fail: bool = False
        self._forced_fail_reason: Optional[str] = None

        self._state_header: str = ""            # ★ 현재 단계 제목 보관

        # ▶ 공정(Process) 타이머 활성화 여부 (SP4/IG 대기는 False)
        self._process_timer_active: bool = False

        # ▶ 종료/정지 후 UI 복원용 시작 시 분 값 저장소
        self._last_process_time_min: Optional[float] = None

        # 호스트에게 프리플라이트 결과를 전달하기 위한 Future 추가
        self._host_start_future: Optional[asyncio.Future] = None
        
        # ✅ 마지막으로 열었던 CSV 폴더 기억
        self._last_process_list_dir: str = ""

        self._runlog_buf = deque()

        self._cfg_mod = cfgc

        parent = self._parent_widget() or QApplication.instance()  # QWidget 우선
        self._runlog_timer = QTimer(parent)
        self._runlog_timer.setInterval(1000)
        self._runlog_timer.timeout.connect(self._flush_run_log)
        self._runlog_timer.start()

        # 로그/상태 위젯
        self._w_log = (
            _safe_get(ui, f"{self.prefix.lower()}logMessage_edit")
            or _safe_get(ui, f"{self.prefix}logMessage_edit")
            or _safe_get(ui, "pc_logMessage_edit")
        )

        # ✅ server_page와 동일한 방식: UI 로그 무한 누적 방지
        self._pc_ui_log_max_lines = 5000  # 2000~10000 사이 추천
        try:
            if isinstance(self._w_log, QPlainTextEdit):
                self._w_log.setMaximumBlockCount(self._pc_ui_log_max_lines)
                self._w_log.setUndoRedoEnabled(False)
        except Exception:
            pass

        self._w_state = (
            _safe_get(ui, f"{self.prefix.lower()}processState_edit")
            or _safe_get(ui, f"{self.prefix}processState_edit")
            or _safe_get(ui, "pc_processState_edit")
        )

        # ✅ Start/Stop 버튼 캐싱(한번만 찾고 계속 사용)
        self._w_start = _find_first(self.ui, [
            f"{self.prefix}Start_button", f"{self.prefix}StartButton",
            f"{self.prefix.lower()}Start_button", f"{self.prefix.lower()}StartButton",
            "PC_Start_button", "pcStart_button",
        ])
        self._w_stop = _find_first(self.ui, [
            f"{self.prefix}Stop_button", f"{self.prefix}StopButton",
            f"{self.prefix.lower()}Stop_button", f"{self.prefix.lower()}StopButton",
            "PC_Stop_button", "pcStop_button",
        ])

        # ✅ 버튼 토글 ‘세대(Generation)’ 카운터 — 마지막 지시만 유효
        self._ui_toggle_gen = 0

        # RF 파워(연속) 바인딩
        self.rf = self._make_rf_async()

        # PlasmaCleaningController 바인딩
        self.pc = self._bind_pc_controller()

        # UI 버튼 연결
        self._connect_ui_buttons()

        self._disconnect_on_finish = bool(disconnect_on_finish)

        # IG 객체가 넘어온 경우, 기본 콜백 자동 바인딩
        if self.ig is not None:
            self._bind_ig_device(self.ig)

    @property
    def is_running(self) -> bool:
        return bool(getattr(self, "_running", False))

    # =========================
    # MFC/IG/RFpower 이벤트 펌프
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

                # ▶ 컨트롤러에도 통지
                try:
                    if getattr(self, "pc", None):
                        self.pc.on_mfc_confirmed(getattr(ev, "cmd", "") or "")
                except Exception:
                    pass

            elif k == "command_failed":
                self.append_log(label, f"FAIL: {ev.cmd or ''} ({ev.reason or 'unknown'})")

                try:
                    if getattr(self, "pc", None):
                        self.pc.on_mfc_failed(
                            getattr(ev, "cmd", "") or "",
                            getattr(ev, "reason", "") or "unknown",
                            code=getattr(ev, "code", None) or getattr(ev, "error_code", None),
                            meta={
                                "kind": k,
                                "gas": getattr(ev, "gas", None),
                                "label": label,
                                "ch": self._selected_ch,
                            },
                        )
                except Exception:
                    pass

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

    async def _pump_rf_events(self) -> None:
        """RFPowerAsync 이벤트를 UI/로그로 중계"""
        if not self.rf:
            return
        try:
            async for ev in self.rf.events():
                if ev.kind == "display":
                    # 1) 전용 칸(FWD/REF) 갱신
                    for_w = getattr(self.ui, "PC_forP_edit", None)
                    ref_w = getattr(self.ui, "PC_refP_edit", None)
                    with contextlib.suppress(Exception):
                        if for_w and hasattr(for_w, "setPlainText"):
                            for_w.setPlainText(f"{float(ev.forward):.1f}")
                        if ref_w and hasattr(ref_w, "setPlainText"):
                            ref_w.setPlainText(f"{float(ev.reflected):.1f}")

                    # 2) 상태창은 카운트다운 유지 → 덮어쓰지 않고 로그만 남김
                    self.append_log("RF", f"FWD={ev.forward:.1f}, REF={ev.reflected:.1f} (W)")
                    continue

                elif ev.kind == "status":
                    self.append_log("RF", ev.message or "")

                elif ev.kind == "target_reached":  
                    # RF 목표 파워 도달 (FWD가 setpoint 근처)
                    self.append_log("RF", "목표 파워 도달")
                    
                    # ★ 성공 결과 저장
                    self._rf_target_ok = True
                    self._rf_target_reason = ""

                    self._rf_target_evt.set()

                elif ev.kind == "target_failed":
                    # RFPowerAsync가 공정을 실패로 판정한 경우(REF.P 60초 초과, 저출력 등)
                    reason = ev.message or "RF 목표 파워 실패 (REF.P 과다 또는 저출력)"
                    self.append_log("RF", f"목표 파워 실패: {reason}")

                    # ★ 실패 결과 저장 (이게 _rf_start()의 판단 근거가 됨)
                    self._rf_target_ok = False
                    self._rf_target_reason = reason

                    # ★ (선택/권장) 최종 결과 보정을 위한 런타임 플래그도 저장
                    self._forced_fail = True
                    self._forced_fail_reason = reason

                    # 컨트롤러에도 실패 결과 기록(사유 보존)
                    pc = getattr(self, "pc", None)
                    if pc is not None:
                        with contextlib.suppress(Exception):
                            pc.on_rf_failed(
                                reason,
                                code=getattr(ev, "code", None) or getattr(ev, "error_code", None),
                                meta={
                                    "kind": ev.kind,
                                    "forward": getattr(ev, "forward", None),
                                    "reflected": getattr(ev, "reflected", None),
                                    "ch": self._selected_ch,
                                },
                            )
                            self.append_log("PC", "RF 실패 감지 → PC 컨트롤러에 FAIL 전달")

                    # 목표 대기 중인 _rf_start()를 깨움(성공/실패 구분은 _rf_target_ok로)
                    self._rf_target_evt.set()

                elif ev.kind == "power_off_finished":
                    self.append_log("RF", "Power OFF finished")

        except asyncio.CancelledError:
            # 정상 취소 경로
            pass
        except Exception as e:
            # ★ 펌프가 죽더라도 로그 남기고 종료
            self.append_log("RF", f"이벤트 펌프 오류: {e!r}")

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

    async def _preflight_connect(self, timeout_s: float | None = None) -> None:
        cfgm = getattr(self, "_cfg_mod", cfgc)

        if timeout_s is None:
            timeout_s = float(getattr(cfgm, "PC_PREFLIGHT_TIMEOUT_S", 10.0))

        plc_handshake_timeout_s = float(getattr(cfgm, "PC_PREFLIGHT_PLC_HANDSHAKE_TIMEOUT_S", 1.0))
        device_connect_timeout_s = float(getattr(cfgm, "PC_PREFLIGHT_DEVICE_CONNECT_TIMEOUT_S", 3.0))
        connect_poll_interval_s = float(getattr(cfgm, "PC_PREFLIGHT_CONNECT_POLL_INTERVAL_S", 0.5))

        self.append_log("PC", "프리플라이트: 장비 연결 확인/시작")
        need: list[tuple[str, object]] = []
        if self.plc:
            need.append(("PLC", self.plc))
        if self.mfc_gas:
            need.append(("MFC(GAS)", self.mfc_gas))
        if self.mfc_pressure:
            need.append(("MFC(SP4)", self.mfc_pressure))
        if self.ig:
            need.append(("IG", self.ig))

        for name, dev in need:
            if self._is_dev_connected(dev):
                self.append_log("PC", f"{name} 이미 연결됨")
                continue
            try:
                if name == "PLC":
                    await asyncio.wait_for(dev.read_coil(0), timeout=plc_handshake_timeout_s)
                    self.append_log("PC", "PLC 핸드셰이크(read_coil 0)")
                else:
                    fn = getattr(dev, "connect", None)

                    if callable(fn):
                        try:
                            if inspect.iscoroutinefunction(fn):
                                await asyncio.wait_for(fn(), timeout=device_connect_timeout_s)
                            else:
                                await asyncio.wait_for(asyncio.to_thread(fn), timeout=device_connect_timeout_s)
                        except asyncio.TimeoutError:
                            raise RuntimeError(f"{name} connect timeout({device_connect_timeout_s:.1f}s)")
                        self.append_log("PC", f"{name} connect 호출")
                    else:
                        st = getattr(dev, "start", None)
                        if not callable(st):
                            raise RuntimeError(f"{name}는 connect() 또는 start() 중 하나를 제공해야 합니다")
                        try:
                            if inspect.iscoroutinefunction(st):
                                await asyncio.wait_for(st(), timeout=device_connect_timeout_s)
                            else:
                                await asyncio.wait_for(asyncio.to_thread(st), timeout=device_connect_timeout_s)
                        except asyncio.TimeoutError:
                            raise RuntimeError(f"{name} start timeout({device_connect_timeout_s:.1f}s)")
                        self.append_log("PC", f"{name} start 호출")
            except Exception as e:
                raise RuntimeError(f"{name} 연결 실패: {e!r}")

        t0 = time.monotonic()
        while True:
            missing = [n for n, d in need if not self._is_dev_connected(d)]
            if not missing:
                break
            if (time.monotonic() - t0) >= float(timeout_s):
                raise RuntimeError(f"장비 연결 타임아웃: {', '.join(missing)}")
            await asyncio.sleep(connect_poll_interval_s)

        # 3) 이벤트 펌프 기동 (중복 방지)
        if not hasattr(self, "_event_tasks"):
            self._event_tasks = []

        # ★ 취소/종료된 태스크는 리스트에서 제거 (좀비 방지)
        _alive = []
        for t in self._event_tasks:
            try:
                if t and (not t.cancelled()) and (not t.done()):
                    _alive.append(t)
            except Exception:
                pass
        self._event_tasks = _alive

        def _has_task(name: str) -> bool:
            return any((getattr(t, "get_name", lambda: "")() == name) for t in self._event_tasks)

        if self.rf and not _has_task("PC.Pump.RF"):
            self._event_tasks.append(asyncio.create_task(self._pump_rf_events(), name="PC.Pump.RF"))

        # 같은 MFC 인스턴스를 가리키면 펌프는 하나만 띄운다
        if self.mfc_gas is self.mfc_pressure:
            if self.mfc_gas and not _has_task("PC.Pump.MFC.COMBINED"):
                sel = f"CH{self._selected_ch}"
                self._event_tasks.append(asyncio.create_task(
                    self._pump_mfc_events(self.mfc_gas, f"MFC(SP4/GAS-{sel})"),
                    name="PC.Pump.MFC.COMBINED"))
        else:
            if self.mfc_gas and not _has_task("PC.Pump.MFC.GAS"):
                self._event_tasks.append(asyncio.create_task(
                    self._pump_mfc_events(self.mfc_gas, "MFC(GAS)"),
                    name="PC.Pump.MFC.GAS"))
            if self.mfc_pressure and not _has_task("PC.Pump.MFC.SP4"):
                sel = f"CH{self._selected_ch}"
                self._event_tasks.append(asyncio.create_task(
                    self._pump_mfc_events(self.mfc_pressure, f"MFC(SP4-{sel})"),
                    name="PC.Pump.MFC.SP4"))

        if self.ig and not _has_task("PC.Pump.IG"):
            sel = f"CH{self._selected_ch}"
            self._event_tasks.append(asyncio.create_task(
                self._pump_ig_events(f"IG-{sel}"),
                name="PC.Pump.IG"))

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

    def set_ig_device(self, ig: Optional[AsyncIG]) -> None:
        """IG.wait_for_base_pressure에서 사용할 실제 IG 인스턴스 교체"""
        self.ig = ig

    def set_ig_callbacks(
        self,
        ensure_on: Callable[[], Awaitable[None]],
        read_mTorr: Callable[[], Awaitable[float]],
    ) -> None:
        """IG 콜백 주입 (라디오 전환 시마다 업데이트)"""
        self._ig_ensure_on_cb = ensure_on
        self._ig_read_mTorr_cb = read_mTorr
        #self.append_log("PC", "IG callbacks bound")

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

        async def _ig_wait_for_base_torr(target_torr: float, interval_ms: int = 10_000) -> bool:
            if not self.ig:
                raise RuntimeError("IG not bound")
            return await self.ig.wait_for_base_pressure(float(target_torr), interval_ms=interval_ms)

        # ---- MFC (GAS/SP4)
        async def _mfc_gas_select(gas_idx: int) -> None:
            if not self.mfc_gas:
                raise RuntimeError("mfc_gas not bound")
            gi = int(gas_idx)
            self._pc_gas_idx = gi  # ← 런타임에 보관해서 이후 스케일에 사용
            self.append_log("PC", f"GasFlow → {_mfc_name(self.mfc_gas)} ch{gi}")
            await self.mfc_gas.gas_select(gi)  # MFC 내부 '선택 채널' 갱신

        async def _mfc_flow_set_on(flow_sccm: float) -> None:
            mfc = self.mfc_gas
            if not mfc:
                raise RuntimeError("mfc_gas not bound")
            # ← gas_idx는 직전에 _mfc_gas_select에서 self._pc_gas_idx로 저장됨
            ch   = getattr(self, "_pc_gas_idx", 3)
            ui   = float(max(0.0, flow_sccm))   # 이중 스케일 제거

            self.append_log("MFC", f"FLOW_SET_ON(sel ch={ch}) -> {ui:.1f} sccm")
            await mfc.flow_set_on(ui)  # 선택 채널 기준의 개별 ON/안정화

        async def _mfc_flow_off() -> None:
            mfc = self.mfc_gas
            if not mfc:
                return
            self.append_log("MFC", "FLOW_OFF(sel)")
            await mfc.flow_off_selected()   # ✔ 선택 채널만 OFF(개별 L{ch}0)
            self.append_log("MFC", "FLOW_OFF OK")

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

        async def _mfc_wait_sp4_pressure(target_mTorr: float, timeout_s: float) -> bool:
            """
            Plasma Cleaning용 SP4 압력 안정화 대기.
            - AsyncMFC.wait_for_pressure_reached() 에 위임해서
              실제 압력이 target_mTorr 근처에 도달할 때까지 기다린다.
            - timeout_s 안에 도달 못 하면 False 리턴.
            """
            mfc = self.mfc_pressure
            if not mfc:
                self.append_log("MFC", "[SP4 WAIT] mfc_pressure not bound")
                return False

            target = float(target_mTorr)
            timeout = float(timeout_s)

            self.append_log(
                "MFC",
                f"[SP4 WAIT] target={target:.3f} mTorr, timeout={timeout:.1f}s",
            )
            try:
                # AsyncMFC 유틸 사용 (스레드/로직 공용)
                # timeout 인자는 keyword-only 이므로 timeout_sec= 으로 넘겨야 함
                res = await mfc.wait_for_pressure_reached(
                    target,
                    timeout_sec=timeout,
                )
            except Exception as e:
                self.append_log("MFC", f"[SP4 WAIT] 예외: {e!r}")
                return False

            ok: bool
            last_p = None
            if isinstance(res, tuple) and res:
                ok = bool(res[0])
                if len(res) > 1:
                    with contextlib.suppress(Exception):
                        last_p = float(res[1])
            else:
                ok = bool(res)

            if last_p is not None:
                self.append_log(
                    "MFC",
                    f"[SP4 WAIT] 결과 ok={ok}, last={last_p:.3f} mTorr",
                )
            else:
                self.append_log("MFC", f"[SP4 WAIT] 결과 ok={ok}")
            return ok

        # ---- RF (PLC DCV ch=1 사용 — enable/write/read)
        async def _rf_start(power_w: float) -> None:
            if not self.rf:
                return

            # ★ 새 시작마다 결과/사유 초기화
            self._rf_target_ok = None
            self._rf_target_reason = ""
            self._rf_target_evt.clear()

            # ✅ 추가: 어떤 경로로 가도 항상 정의되도록 초기화
            last_fwd = None
            last_ref = None

            await self.rf.start_process(float(power_w))

            try:
                # RFPowerAsync의 REF 대기(기본 60s)보다 약간 길게 기다려서 레이스 방지
                cfgm = getattr(self, "_cfg_mod", cfgc)

                rf_target_wait_timeout_s = float(getattr(cfgm, "PC_RF_TARGET_WAIT_TIMEOUT_S", 60.0))
                rf_target_wait_extra_s = float(getattr(cfgm, "PC_RF_TARGET_WAIT_EXTRA_S", 5.0))
                rf_fail_ref_threshold_w = float(getattr(cfgm, "PC_RF_FAIL_REF_THRESHOLD_W", 20.0))

                _wait_s = rf_target_wait_timeout_s
                try:
                    if self.rf:
                        _wait_s = max(
                            _wait_s,
                            float(getattr(self.rf, "reflected_wait_timeout_s", rf_target_wait_timeout_s)) + rf_target_wait_extra_s
                        )
                except Exception:
                    _wait_s = rf_target_wait_timeout_s

                await asyncio.wait_for(self._rf_target_evt.wait(), timeout=_wait_s)

                # ★ 이벤트가 왔으면 성공/실패를 반드시 구분한다
                if self._rf_target_ok is False:
                    reason = self._rf_target_reason or "RF 목표 파워 실패"
                    self.append_log("RF", f"RF START 실패 확정: {reason}")
                    
                    # ★ stop 오염 방지용 강제 실패 플래그도 같이 세팅
                    self._forced_fail = True
                    self._forced_fail_reason = reason

                    # 안전 정지(램프다운)
                    self._process_timer_active = False
                    with contextlib.suppress(Exception):
                        await self._safe_rf_stop()

                    # 컨트롤러 쪽에는 “정상 중단” 형태로만 전달(스택트레이스/불필요 오류 방지)
                    raise asyncio.CancelledError()

                if self._rf_target_ok is True:
                    self.append_log("RF", "목표 파워 안정 → 프로세스 타이머 시작 가능")
                    self._process_timer_active = True
                    return

                # ★ 이벤트는 왔는데 플래그가 None이면(이상 케이스) 실패로 정리
                reason = "RF START 실패: target 이벤트 수신했지만 결과 플래그 없음"
                self.append_log("RF", reason)

                if getattr(self, "pc", None):
                    self.pc.on_rf_failed(
                        reason,
                        meta={
                            "stage": "_rf_start",
                            "power_w": float(power_w),
                            "selected_ch": self._selected_ch,
                            "last_fwd": last_fwd,
                            "last_ref": last_ref,
                        },
                    )

                self._process_timer_active = False
                with contextlib.suppress(Exception):
                    await self._safe_rf_stop()
                raise asyncio.CancelledError()

            except asyncio.TimeoutError:
                last_fwd = None
                last_ref = None
                try:
                    meas = await self.plc.rf_read_fwd_ref(rf_ch=1) if self.plc else None
                    if isinstance(meas, dict):
                        last_fwd = float(meas.get("forward", 0.0) or 0.0)
                        last_ref = float(meas.get("reflected", 0.0) or 0.0)
                except Exception:
                    pass

                # 기준값(REF 임계치) — RFPowerAsync 쪽 값을 우선 사용
                ref_th = rf_fail_ref_threshold_w
                try:
                    if self.rf:
                        ref_th = float(getattr(self.rf, "reflected_threshold_w", rf_fail_ref_threshold_w))
                except Exception:
                    ref_th = rf_fail_ref_threshold_w

                # ★ timeout 문구를 "REF 과다"로 분류해서 reason을 만든다
                req = float(power_w)

                if (last_ref is not None) and (last_ref > ref_th):
                    reason = (
                        f"RF Ref.p(REF) 과다로 파워 도달 실패: "
                        f"REF={last_ref:.1f}W > TH={ref_th:.1f}W "
                        f"(FWD={0.0 if last_fwd is None else last_fwd:.1f}W, SET={req:.1f}W)"
                    )
                elif (last_fwd is not None) and (last_fwd <= 0.5):
                    reason = "RF Power 도달 실패: 장비 OFF/SET 미설정(0W 지속/응답 없음)"
                else:
                    # 일반적인 “목표 미도달”
                    if last_fwd is not None and last_ref is not None:
                        reason = f"RF Power 도달 실패: 목표({req:.1f}W) 미도달, timeout 60s (FWD={last_fwd:.1f}W, REF={last_ref:.1f}W)"
                    elif last_fwd is not None:
                        reason = f"RF Power 도달 실패: 목표({req:.1f}W) 미도달, timeout 60s (FWD={last_fwd:.1f}W)"
                    else:
                        reason = f"RF Power 도달 실패: 목표({req:.1f}W) 미도달, timeout 60s"

                self.append_log("RF", reason)

                # ★ stop 오염 방지용 강제 실패 플래그
                self._forced_fail = True
                self._forced_fail_reason = reason

                if getattr(self, "pc", None):
                    self.pc.on_rf_failed(
                        reason,
                        meta={
                            "stage": "_rf_start",
                            "power_w": float(power_w),
                            "selected_ch": self._selected_ch,
                            "last_fwd": last_fwd,
                            "last_ref": last_ref,
                        },
                    )

                self._process_timer_active = False
                with contextlib.suppress(Exception):
                    await self._safe_rf_stop()
                raise asyncio.CancelledError()

            except Exception as e:
                reason = f"RF Power 도달 실패: {type(e).__name__}: {e!s}"
                self.append_log("RF", reason)

                # ★ stop 오염 방지용 강제 실패 플래그
                self._forced_fail = True
                self._forced_fail_reason = reason

                if getattr(self, "pc", None):
                    self.pc.on_rf_failed(
                        reason,
                        meta={
                            "stage": "_rf_start",
                            "power_w": float(power_w),
                            "selected_ch": self._selected_ch,
                            "last_fwd": last_fwd,
                            "last_ref": last_ref,
                        },
                    )

                self._process_timer_active = False
                with contextlib.suppress(Exception):
                    await self._safe_rf_stop()
                raise asyncio.CancelledError()

        # (통일) 모든 종료 경로는 안전 정지 루틴 사용 → 램프다운 완료까지 대기
        async def _rf_stop() -> None:
            # ▶ 공정 카운트다운 표시 비활성화 (정상/비정상 종료 모두 커버)
            self._process_timer_active = False
            await self._safe_rf_stop()

        # ---- UI
        def _show_state(text: str) -> None:
            # 단계 제목을 고정해 두고 상태창에 반영
            self._state_header = text or ""
            self._set_state_text(self._state_header)

        def _show_countdown(sec: int) -> None:
            # mm:ss 형식 + (분 단위 2자리 소수) 함께 표시
            try:
                sec = int(sec)
            except Exception:
                sec = 0
            mm, ss = divmod(max(sec, 0), 60)
            tail = f"{mm:02d}:{ss:02d}" if mm else f"{ss:02d}s"
            mins = max(sec, 0) / 60.0
            tail_with_min = f"{tail} ({mins:.2f} min)"

            # 1) 상단 상태창은 언제나 덮어서 표시 (SP4 대기/공정 카운트다운 모두 노출)
            if self._state_header:
                self._set_state_text(f"{self._state_header} · {tail_with_min}")
            else:
                self._set_state_text(tail_with_min)

            # 2) Process Time 칸 표기는 '공정 카운트다운'일 때만 mm:ss 유지 (기존 동작 유지)
            if getattr(self, "_process_timer_active", False):
                w = getattr(self.ui, "PC_ProcessTime_edit", None)
                if w and hasattr(w, "setPlainText"):
                    with contextlib.suppress(Exception):
                        w.setPlainText(tail)

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
            mfc_wait_sp4_pressure=_mfc_wait_sp4_pressure,  # ★ 추가
        )

    def _make_rf_async(self) -> Optional[RFPowerAsync]:
        if not self.plc:
            return None

        async def _rf_send(power_w: float) -> None:
            # ✅ SET 래치는 RFPowerAsync → _rf_toggle_enable(True)에서 1회만 수행
            #    여기서는 WRITE만 (중복 SET 제거)
            await self.plc.rf_apply(float(power_w), ensure_set=False, rf_ch=1)
            try:
                meas = await self.plc.rf_read_fwd_ref(rf_ch=1)  # ← 보정 적용
                self.append_log("PLC", f"RF READ FWD={meas['forward']:.1f} W, REF={meas['reflected']:.1f} W")
            except Exception as e:
                self.append_log("PLC", f"RF READ 실패: {e!r}")

        # _rf_send_unverified — WRITE는 ch1로 고정, READ도 ch1 명시
        async def _rf_send_unverified(power_w: float) -> None:
            await self.plc.power_write(power_w, family="DCV", write_idx=1)  # no-reply 경로
            try:
                meas = await self.plc.rf_read_fwd_ref(rf_ch=1)
                self.append_log("PLC", f"RF READ(FB) FWD={meas['forward']:.1f} W, REF={meas['reflected']:.1f} W")
            except Exception:
                pass

        # _rf_request_read — 주기 읽기도 ch1 명시
        async def _rf_request_read():
            try:
                return await self.plc.rf_read_fwd_ref(rf_ch=1)  # ← 핵심 수정
            except Exception as e:
                self.append_log("RF", f"read failed: {e!r}")
                return None

        async def _rf_toggle_enable(on: bool):
            # RF SET 래치(DCV_SET_1)는 여기가 유일한 경로
            await self.plc.power_enable(bool(on), family="DCV", set_idx=1)

        cfgm = getattr(self, "_cfg_mod", cfgc)

        rf_poll_interval_ms = int(getattr(cfgm, "PC_RF_POLL_INTERVAL_MS", 1000))
        rf_rampdown_interval_ms = int(getattr(cfgm, "PC_RF_RAMPDOWN_INTERVAL_MS", 50))
        rf_direct_mode = bool(getattr(cfgm, "PC_RF_DIRECT_MODE", True))
        rf_write_inv_a = float(getattr(cfgm, "PC_RF_WRITE_INV_A", 1.74))
        rf_write_inv_b = float(getattr(cfgm, "PC_RF_WRITE_INV_B", 0.0))

        return RFPowerAsync(
            send_rf_power=_rf_send,
            send_rf_power_unverified=_rf_send_unverified,
            request_status_read=_rf_request_read,
            toggle_enable=_rf_toggle_enable,
            poll_interval_ms=rf_poll_interval_ms,
            rampdown_interval_ms=rf_rampdown_interval_ms,
            direct_mode=rf_direct_mode,   # ★ Plasma Cleaning용 기본값 유지
            write_inv_a=rf_write_inv_a,
            write_inv_b=rf_write_inv_b,
            cfg=getattr(self, "_cfg_mod", None),
        )

    # =========================
    # UI/이벤트
    # =========================
    def _connect_ui_buttons(self) -> None:
        """
        버튼 연결을 한 번만(중복 방지) + __init__에서 캐싱된 버튼을 그대로 사용.
        """
        # 0) 이미 연결되었으면 재연결하지 않음(중복 실행 방지)
        if getattr(self, "_buttons_connected", False):
            return

        # 1) __init__에서 캐싱한 버튼 사용
        w_start = getattr(self, "_w_start", None)
        w_stop  = getattr(self, "_w_stop",  None)

        # 2) CSV/레시피 버튼은 필요 시 한 번만 찾음
        w_proc = _find_first(self.ui, [
            "PC_processList_button", "pcProcessList_button",
            "processList_button", "PC_LoadCSV_button", "pcLoadCSV_button",
        ])

        # 3) 비동기 핸들러 연결 (최신 권장 create_task 사용)
        if w_start:
            w_start.clicked.connect(lambda: self._spawn_detached(self._on_click_start(), name="PC.ClickStart"))
        if w_stop:
            w_stop.clicked.connect(lambda: self._spawn_detached(self._on_click_stop(), name="PC.ClickStop"))
        if w_proc:
            w_proc.clicked.connect(lambda: self._spawn_detached(self._handle_process_list_clicked_async(), name="PC.ClickOpenCSV"))

        # 4) 중복 연결 방지 플래그
        self._buttons_connected = True

    def _spawn_detached(
        self,
        coro,
        *,
        name: str = "PC.Detached",
        store: bool = True,
    ):
        """
        Qt clicked signal 등 '현재 running loop가 보장되지 않는' 곳에서도
        self._loop 에 안전하게 코루틴을 태운다.
        """
        loop = getattr(self, "_loop", None)
        if loop is None:
            self.append_log("TASK", f"{name}: loop 없음")
            return None

        created: dict[str, asyncio.Task] = {}

        def _create() -> None:
            try:
                task = loop.create_task(coro, name=name)
                created["task"] = task

                if store:
                    self._bg_tasks.append(task)

                def _done(tsk: asyncio.Task) -> None:
                    if store:
                        with contextlib.suppress(ValueError):
                            self._bg_tasks.remove(tsk)

                    if tsk.cancelled():
                        return

                    with contextlib.suppress(Exception):
                        exc = tsk.exception()
                        if exc:
                            self.append_log("TASK", f"{name} failed: {exc!r}")

                task.add_done_callback(_done)

            except Exception as e:
                self.append_log("TASK", f"{name} create_task 실패: {e!r}")

        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        try:
            if running is loop:
                _create()
            else:
                loop.call_soon_threadsafe(_create)
        except Exception as e:
            self.append_log("TASK", f"{name} schedule 실패: {e!r}")
            return None

        return created.get("task")

    # Host 프리플라이트 결과 전달용 헬퍼
    def _host_report_start(self, ok: bool, reason: str = "") -> None:
        """
        Host에서 start를 걸었을 때 프리플라이트 결과(ok / fail 이유)를
        start_with_recipe_string 쪽 Future로 전달하기 위한 헬퍼.

        - UI에서 버튼을 눌렀을 때는 _host_start_future가 None이라 그냥 무시된다.
        - Host에서 start_with_recipe_string을 호출한 경우에만 실제로 의미가 있다.
        """
        fut = getattr(self, "_host_start_future", None)
        try:
            if fut is not None and not fut.done():
                fut.set_result((bool(ok), str(reason)))
        except Exception:
            # 여기서 예외 터져도 공정은 그대로 진행되게 조용히 무시
            pass

    async def _on_click_start(self) -> None:
        # ------------------------------------------------------------
        # TEST MODE 판정 (# == test)
        # ------------------------------------------------------------
        test_mode = False
        test_dur_sec = 0.0
        try:
            row = getattr(self, "_loaded_recipe_row", None) or {}
            marker = str(row.get("#", "")).strip().lower()
            time_str = str(row.get("time", "") or row.get("Time", "") or row.get("TIME", "")).strip()

            if marker == "test":
                test_mode = True
                test_dur_sec = self._parse_duration_seconds(time_str.lower())
                if test_dur_sec <= 0:
                    test_dur_sec = 60.0
        except Exception:
            pass
        # ------------------------------------------------------------

        self._cleanup_started = False  # ★ 추가: 새 런마다 정리 가드 초기화

        # ★ 추가(권장): RF 실패 보정 플래그 초기화
        self._forced_fail = False
        self._forced_fail_reason = None

        # start 버튼 중복 클릭 방지
        if getattr(self, "_running", False):
            msg = "이미 Plasma Cleaning이 실행 중입니다."
            self._post_warning("실행 중", msg)
            self._host_report_start(False, msg) # ★ Host에도 실패 사유 전달
            return

        ch = int(getattr(self, "_selected_ch", 1))

        # 1) 쿨다운 교차 검사 (PC 전역 + 해당 챔버)
        ok_cool, remain, _ = runtime_state.pc_block_reason(ch, cooldown_s=60.0)
        if not ok_cool:
            secs = int(float(remain) + 0.999)
            msg = f"이전 공정 종료 후 1분 대기 필요합니다. 약 {secs}초 후에 다시 시도하십시오."
            # ✔ 챔버 공정과 동일한 문구/형식
            self._post_warning("대기 필요",
                               f"이전 공정 종료 후 1분 대기 필요합니다.\n{secs}초 후에 시작하십시오.")
            self._host_report_start(False, msg)   # ★ Host 실패
            return

        # 2) 교차 실행 차단
        # 2-1) 같은 CH의 PC가 이미 실행 중이면 금지
        if runtime_state.is_running("pc", ch):
            msg = f"CH{ch} Plasma Cleaning이 이미 실행 중입니다."
            self._post_warning("실행 오류", msg)
            self._host_report_start(False, msg)   # ★ Host 실패
            return

        # 2-2) 같은 CH의 Chamber 공정도 실행 중이면 금지
        if runtime_state.is_running("chamber", ch):
            msg = f"CH{ch}는 이미 다른 공정이 실행 중입니다."
            self._post_warning("실행 오류", msg)
            self._host_report_start(False, msg)   # ★ Host 실패
            return
        
        # ✅ start 수락 즉시 RUNNING + error clear
        with contextlib.suppress(Exception):
            runtime_state.mark_started("pc", ch)
        
        # =============================================================
        # 테스트 모드
        # =============================================================
        if test_mode:
            # TEST는 장비를 절대 건드리지 않는다.
            # (이전 실행에서 남은 이벤트 펌프/태스크가 PLC를 건드릴 수 있으니 먼저 정리)
            with contextlib.suppress(Exception):
                await self._shutdown_all_tasks()

            self.append_log("MAIN", f"[TEST MODE] {test_dur_sec:.1f}s 동안 시뮬레이션 실행 (장비 제어 없음)")
            self._host_report_start(True, "TEST MODE (skip preflight)")

            # UI의 ProcessTime(min) 칸도 TEST 시간으로 맞춰서 로그/챗에 반영
            with contextlib.suppress(Exception):
                self._set_plaintext("PC_ProcessTime_edit", f"{(test_dur_sec / 60.0):.2f}")

            # 런 시작 준비(실공정과 동일하게)
            ch = int(getattr(self, "_selected_ch", 1))

            # running 마킹(PC만)
            with contextlib.suppress(Exception):
                runtime_state.set_running("pc", True, ch)

            p = self._read_params_from_ui()
            self._last_process_time_min = float(p.process_time_min)
            self._running = True
            self._test_mode_active = True
            self._stop_requested = False
            self._final_notified = False

            self._set_running_ui_state()
            self._set_state_text("TEST MODE · Preparing…")
            self._open_run_log(p)
            self.append_log("PC", "파일 로그 시작 (TEST MODE)")

            ok_final: bool = True
            stopped_final: bool = False
            final_reason: Optional[str] = None

            try:
                # 시작 카드(선택) — TEST임을 명확히
                with contextlib.suppress(Exception):
                    if self.chat:
                        self.chat.notify_process_started({
                            "process_note":  "Plasma Cleaning (TEST)",
                            "process_time":  float(p.process_time_min),
                            "use_rf_power":  False,
                            "rf_power":      0.0,
                            "prefix":        f"CH{self._selected_ch} Plasma Cleaning (TEST)",
                            "ch":            self._selected_ch,
                        })
                        if hasattr(self.chat, "flush"):
                            self.chat.flush()

                # STOP 가능 카운트다운(0.2s 폴링, 표시는 1s 단위로)
                loop = asyncio.get_running_loop()
                end_t = loop.time() + float(test_dur_sec)
                last_sec = None

                while True:
                    if self._stop_requested:
                        stopped_final = True
                        ok_final = False  # STOP은 success는 아니지만, stopped_final=True로 처리되어 error 상태는 아님(idle)
                        final_reason = "사용자 STOP"
                        break

                    remain = int(max(0.0, end_t - loop.time()) + 0.999)  # ceil
                    if last_sec != remain:
                        last_sec = remain
                        mm, ss = divmod(remain, 60)
                        mins = remain / 60.0
                        self._set_state_text(f"TEST MODE · {mm:02d}:{ss:02d} ({mins:.2f} min)")

                        # ProcessTime_edit는 실행 중에는 mm:ss로 보여주기(실공정과 유사)
                        w = getattr(self.ui, "PC_ProcessTime_edit", None)
                        if w and hasattr(w, "setPlainText"):
                            with contextlib.suppress(Exception):
                                w.setPlainText(f"{mm:02d}:{ss:02d}")

                    if remain <= 0:
                        break

                    await asyncio.sleep(0.2)

            except Exception as e:
                ok_final = False
                stopped_final = False
                final_reason = f"{type(e).__name__}: {e!s}"
                self.append_log("PC", f"[TEST MODE] 오류: {e!r}")

            finally:
                # TEST 정리(장비 정리 스킵하도록 _final_cleanup가 가드함)
                await self._final_cleanup()

                # UI 복구
                self._running = False
                self._process_timer_active = False
                self._test_mode_active = False
                self._reset_ui_state(restore_time_min=self._last_process_time_min)
                self._set_state_text("대기 중")
                self.append_log("MAIN", "[TEST MODE] 종료 및 UI 복구 완료")

                # 종료 통지 + runtime_state 해제(구글챗/상태 반영)
                with contextlib.suppress(Exception):
                    await self._notify_finish_once(ok=ok_final, reason=final_reason, stopped=stopped_final)

            return
        # =============================================================
        # 테스트 모드
        # =============================================================
        
        # 3) 프리플라이트 (성공하면 계속)
        # ✅ START 요청 수락 시점에 즉시 RUNNING 표시 (프리플라이트 전)
        #    - 정상 종료(ok=True)만 idle
        #    - 프리플라이트/오류/STOP 등 비정상은 error
        self._running = True
        self._stop_requested = False
        self._final_notified = False
        with contextlib.suppress(Exception):
            runtime_state.set_running("pc", True, ch)
        with contextlib.suppress(Exception):
            self._set_running_ui_state()
            self._set_state_text("프리플라이트 중…")

        try:
            # 1) 사전 연결 점검
            await self._preflight_connect()
        except Exception as e:
            msg = f"장치 연결에 실패했습니다: {e}"
            self._post_critical(
                "연결 실패",
                f"장치 연결에 실패했습니다.\n\n{e}\n\n확인을 누르면 상태 표시가 Idle로 변경됩니다.",
                clear_status_to_idle=True,
                ch=ch,
            )
            self._host_report_start(False, msg)    # ★ Host 실패

            # ✅ 프리플라이트 실패 → error (RUNNING 해제 + 쿨다운 마킹)
            with contextlib.suppress(Exception):
                runtime_state.set_error("pc", ch, msg)
                runtime_state.mark_finished("pc", ch)

            with contextlib.suppress(Exception):
                await self._cleanup_tasks_only("preflight_connect failed")

            self._running = False
            with contextlib.suppress(Exception):
                self._reset_ui_state()
            return

        # 3-1) GV 인터락 체크
        try:
            gv_ok = True
            if self.plc:
                key = f"G_V_{ch}_인터락"
                self.append_log("PLC", "GV 인터락 확인(프리플라이트)")
                try:
                    gv_ok = bool(await asyncio.wait_for(self.plc.read_bit(key), timeout=0.6))
                except asyncio.TimeoutError:
                    raise RuntimeError(f"GV 인터락 read timeout(0.6s): {key}")
        except Exception as e:
            msg = f"게이트밸브 인터락 상태 확인 실패: {e}"
            self._post_critical(
                "GV 인터락 오류",
                f"{msg}\n\n확인을 누르면 상태 표시가 Idle로 변경됩니다.",
                clear_status_to_idle=True,
                ch=ch,
            )
            self._host_report_start(False, msg)

            # ✅ 추가: 프리플라이트 이후 단계에서 실패하면 이벤트펌프/태스크 정리
            with contextlib.suppress(Exception):
                await self._final_cleanup()

            with contextlib.suppress(Exception):
                runtime_state.set_error("pc", ch, msg)
                runtime_state.mark_finished("pc", ch)

            self._running = False
            with contextlib.suppress(Exception):
                self._reset_ui_state()
            return

        if not gv_ok:
            msg = f"게이트밸브 인터락 FALSE (G_V_{ch}_인터락=FALSE) → LoadLock의 압력을 확인하십시오."
            self.append_log("PLC", msg)
            self._post_critical("게이트밸브 인터락", msg, clear_status_to_idle=True, ch=ch)
            self._host_report_start(False, msg)

            # ✅ 추가: 이벤트펌프/태스크 정리
            with contextlib.suppress(Exception):
                await self._final_cleanup()

            with contextlib.suppress(Exception):
                runtime_state.set_error("pc", ch, msg)
                runtime_state.mark_finished("pc", ch)

            self._running = False
            with contextlib.suppress(Exception):
                self._reset_ui_state()
            return

        # ★ 여기까지 왔으면 Host 프리플라이트 성공
        self._host_report_start(True, "preflight OK")

        # 5) UI/로그 준비
        p = self._read_params_from_ui()
        self._last_process_time_min = float(p.process_time_min)
        self._running = True
        self._set_running_ui_state()
        self._set_state_text("Preparing…")
        self._open_run_log(p)
        self.append_log("PC", "파일 로그 시작")
        self._stop_requested = False
        self._final_notified = False

        # ★ 최종 결과 (finally에서 사용할 컨테이너)
        ok_final: bool = False
        stopped_final: bool = False
        final_reason: Optional[str] = None

        try:
            # 6) 시작 카드 전송 — 여기만 suppress
            with contextlib.suppress(Exception):
                if self.chat:
                    self.chat.notify_process_started({
                        "process_note":  "Plasma Cleaning",
                        "process_time":  float(p.process_time_min),
                        "use_rf_power":  True,
                        "rf_power":      float(p.rf_power_w),
                        "prefix":        f"CH{self._selected_ch} Plasma Cleaning",  # ← 헤더용
                        "ch":            self._selected_ch,
                    })
                    if hasattr(self.chat, "flush"):
                        self.chat.flush()

            # 7) 컨트롤러 실행
            exc_reason = None
            try:
                await self.pc._run(p)
            except asyncio.CancelledError:
                # ✅ 진짜 사용자 STOP일 때만 STOP으로 본다
                if self._stop_requested:
                    exc_reason = "사용자 STOP"
                else:
                    # 컨트롤러가 정상 종료에 내부 cancel을 사용한 케이스 → 실패로 보지 않음
                    exc_reason = None
            except Exception as e:
                exc_reason = f"{type(e).__name__}: {e!s}"

            lr = str(getattr(self.pc, "last_result", "") or "").strip().lower()
            ls = str(getattr(self.pc, "last_reason", "") or "").strip()
            led = str(getattr(self.pc, "last_error_detail", "") or "").strip()

            # 사용자가 STOP을 눌렀거나 컨트롤러가 'stop'을 준 경우를 모두 STOP으로 간주
            stopped_final = bool(self._stop_requested or lr == "stop")

            if exc_reason:
                ok_final = False
                final_reason = exc_reason or led or ls or "runtime/controller error"
            else:
                ok_final = (lr == "success")
                final_reason = (None if ok_final else (led or ls or "runtime/controller error"))

            # ★ 추가: RF쪽에서 강제 실패가 확정된 경우(stop으로 오염돼도 failed로 고정)
            if getattr(self, "_forced_fail", False):
                ok_final = False
                stopped_final = False
                ff = getattr(self, "_forced_fail_reason", None)
                if ff:
                    final_reason = str(ff)

            # ✅ 최종 보정: 컨트롤러가 success라고 했으면 사용자 STOP/실패로 뒤집히지 않게 보정
            #    단, RF 강제 실패가 찍힌 경우(_forced_fail)는 절대 success로 덮어쓰지 않음
            if (lr == "success") and (not self._stop_requested) and (not getattr(self, "_forced_fail", False)):
                ok_final = True
                stopped_final = False
                final_reason = None

            self.append_log("PC", f"Final notify ok={ok_final}, stopped={stopped_final}, lr={lr!r}, reason={final_reason!r}")

        except Exception as e:
            # 프리플라이트/초기 오류 등
            ok_final = False
            stopped_final = False
            final_reason = f"{type(e).__name__}: {e!s}"
            self.append_log("PC", f"오류: {e!r}")

        finally:
            # [A] 먼저 실제 정리 (장치/태스크)
            await self._final_cleanup()

            # [B] UI 복구/플래그 정리
            self._running = False
            self._process_timer_active = False
            self._reset_ui_state(restore_time_min=self._last_process_time_min)
            self._set_state_text("대기 중")
            self.append_log("MAIN", "[FINALLY] idle UI 복구 완료")

            # [C] 마지막에 종료 통지 + runtime_state 해제
            try:
                await self._notify_finish_once(ok=ok_final, reason=final_reason, stopped=stopped_final)
            except Exception as e:
                self.append_log("PC", f"notify_finish_once error: {e!r}")

            # # [A] 🔁 순서 변경: 종료 통지 먼저 (runtime_state 즉시 해제 + 종료 챗 선송)
            # try:
            #     await self._notify_finish_once(ok=ok_final, reason=final_reason, stopped=stopped_final)  # ← 순서 ↑
            # except Exception as e:
            #     self.append_log("PC", f"notify_finish_once error: {e!r}")

            # # [B] 그 다음 장치/태스크 정리 (오래 걸려도 상관없음)
            # await self._final_cleanup()

            # # [C] 마지막으로 UI 복구
            # self._running = False
            # self._process_timer_active = False
            # self._reset_ui_state(restore_time_min=self._last_process_time_min)
            # self._set_state_text("대기 중")
            # self.append_log("MAIN", "[FINALLY] idle UI 복구 완료")

    async def _on_click_stop(self) -> None:
        # 0) 실행/중복 가드
        if not getattr(self, "_running", False):
            self.append_log("UI", "[STOP] 실행 중이 아님 → 무시")
            return

        if getattr(self, "_final_notified", False):
            self.append_log("UI", "[STOP] 이미 종료 통지됨 → 무시")
            return

        if getattr(self, "_stop_requested", False):
            self.append_log("UI", "[STOP] 이미 정지 요청됨 → 무시")
            return

        # 1) 정지 요청 플래그
        self._stop_requested = True
        self.append_log("MAIN", "[STOP] 사용자 정지 요청 수신")

        # 2) 즉시 UI 반영(중복 클릭 방지)
        self._set_state_text("정지 중…")
        self._apply_button_state(start_enabled=False, stop_enabled=False)

        # 3) 컨트롤러에 소프트 스톱 신호
        with contextlib.suppress(Exception):
            if getattr(self, "pc", None) and hasattr(self.pc, "request_stop"):
                self.pc.request_stop()
                self.append_log("CTRL", "[STOP] pc.request_stop() 전달")

        # 4) 여기서는 끝. (정리/종료 통지는 _on_click_start()의 finally에서 '단일' 수행)
        return
    
    async def _force_finalize_rf_stop(self) -> None:
        """
        RF를 외부에서 강제로 0W/SET OFF 한 뒤,
        RFPowerAsync 내부에 남아 있을 수 있는 task/상태를 정리한다.
        - ramp-down 로그가 계속 찍히는 현상 방지
        - 다음 run 시작 시 stale state 방지
        """
        if not self.rf:
            return

        # 1) 내부 task 정리
        for attr in ("_rampdown_task", "_adjust_task", "_poll_task"):
            with contextlib.suppress(Exception):
                t = getattr(self.rf, attr, None)
                if t and not t.done():
                    t.cancel()
                    await asyncio.gather(t, return_exceptions=True)
                setattr(self.rf, attr, None)

        # 2) 내부 상태 초기화
        with contextlib.suppress(Exception):
            self.rf._is_ramping_down = False
        with contextlib.suppress(Exception):
            self.rf._is_running = False
        with contextlib.suppress(Exception):
            self.rf._enabled = False
        with contextlib.suppress(Exception):
            self.rf.state = "IDLE"
        with contextlib.suppress(Exception):
            self.rf.target_power = 0.0
            self.rf.current_power_step = 0.0
            self.rf._last_sent_w = 0.0

        # 3) 상위 wait가 더 이상 막히지 않도록 완료 이벤트 세팅
        with contextlib.suppress(Exception):
            self.rf._power_off_evt.set()

    async def _safe_rf_stop(self) -> None:
        # ▶ 어떤 경로로 불려도 카운트다운 표시는 종료
        self._process_timer_active = False

        # RF 객체가 없더라도 나머지 장치는 정리해야 함
        if not self.rf:
            await self._shutdown_rest_devices()
            return

        cfgm = getattr(self, "_cfg_mod", cfgc)
        rf_cleanup_timeout_s = float(getattr(cfgm, "PC_RF_CLEANUP_TIMEOUT_S", 5.0))
        rf_wait_power_off_timeout_s = float(getattr(cfgm, "PC_RF_WAIT_POWER_OFF_TIMEOUT_S", 15.0))

        # 1) RF cleanup 요청
        try:
            await asyncio.wait_for(self.rf.cleanup(), timeout=rf_cleanup_timeout_s)
        except asyncio.TimeoutError:
            self.append_log("RF", f"cleanup timeout({rf_cleanup_timeout_s:.1f}s) → 계속 진행")
        except Exception as e:
            self.append_log("RF", f"cleanup error: {e!r}")

        # 2) RF OFF 완료 신호 대기
        ok = False
        try:
            ok = await self.rf.wait_power_off(timeout_s=rf_wait_power_off_timeout_s)
        except Exception as e:
            self.append_log("RF", f"wait_power_off error: {e!r}")

        # 3) timeout/실패 시 PLC 강제 OFF + RF 내부 stale task/state 정리
        if not ok:
            if self.plc:
                self.append_log("RF", "ramp-down 완료 신호 timeout → 강제 0W/SET OFF")
                with contextlib.suppress(Exception):
                    await self.plc.power_write(0.0, family="DCV", write_idx=1)
                with contextlib.suppress(Exception):
                    await self.plc.power_enable(False, family="DCV", set_idx=1)

            await self._force_finalize_rf_stop()

        # 4) RF 종료 후 나머지 장치 정리
        await self._shutdown_rest_devices()

    # =========================
    # 내부 헬퍼들
    # =========================
    async def _final_cleanup(self) -> None:
        # (A) 재진입 가드
        if getattr(self, "_cleanup_started", False):
            return
        self._cleanup_started = True

        # ★ (A2) 시작/끝 로그(선택) — 추적성 강화
        self.append_log("MAIN", "[CLEANUP] begin")

        try:
            # ★ TEST MODE면 장비(RF/MFC/PLC) 정리 절대 하지 않음
            if getattr(self, "_test_mode_active", False):
                self.append_log("MAIN", "[CLEANUP] TEST MODE: 장비 정리(RF/MFC/PLC) 스킵, 태스크/로그만 정리")
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(self._shutdown_all_tasks(), timeout=3.0)
                with contextlib.suppress(Exception):
                    self._close_run_log()
                return

            # ✅ (추가) base-wait 잔존 시 다음 run이 즉시 실패하므로 종료 시 항상 정리
            with contextlib.suppress(Exception):
                if self.ig and hasattr(self.ig, "cancel_wait"):
                    await asyncio.wait_for(self.ig.cancel_wait(), timeout=2.0)

            # (B) MFC 폴링/자동재연결 명시 중단
            with contextlib.suppress(Exception):
                skip_finalize = False
                try:
                    # CH2에서 PC 중이고 + CH1 Chamber가 실행 중이면 True
                    skip_finalize = self._skip_mfc_finalize_due_to_ch1()
                except Exception:
                    pass

                if skip_finalize:
                    self.append_log("MFC", "CH1 공정 중 → MFC 폴링/상태 리셋 생략")
                else:
                    if self.mfc_gas:
                        if hasattr(self.mfc_gas, "on_process_finished"):
                            self.mfc_gas.on_process_finished(False)
                        elif hasattr(self.mfc_gas, "set_process_status"):
                            self.mfc_gas.set_process_status(False)
                    if self.mfc_pressure:
                        if hasattr(self.mfc_pressure, "on_process_finished"):
                            self.mfc_pressure.on_process_finished(False)
                        elif hasattr(self.mfc_pressure, "set_process_status"):
                            self.mfc_pressure.set_process_status(False)

            # (C) RF/가스/SP4 안전 정지
            with contextlib.suppress(Exception):
                await self._safe_rf_stop()

            # (D) 내부 태스크 취소/대기 (유한 시간)
            try:
                await asyncio.wait_for(self._shutdown_all_tasks(), timeout=3.0)
            except asyncio.TimeoutError:
                self.append_log("PC", "태스크 종료 지연(timeout) → 계속 진행")

            # (E) 선택 장치 해제 — 정책에 맞춰 '필요할 때만'
            if getattr(self, "_disconnect_on_finish", False):
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(self._disconnect_selected_devices(), timeout=5.0)

            # (F) 로그 파일 닫기
            with contextlib.suppress(Exception):
                self._close_run_log()

        finally:
            # ★★★ 가장 중요: 플래그 복구(예외 발생해도 다음 런에서 cleanup 동작)
            #self._cleanup_started = False
            self.append_log("MAIN", "[CLEANUP] end")  # (선택)

    def _apply_button_state(self, *, start_enabled: bool, stop_enabled: bool) -> None:
        """
        Start/Stop 버튼 상태를 중앙 경로로만 변경한다.
        - 즉시 반영 + 다음 틱 보정
        - 세대(gen) 카운터로 '마지막 지시'만 적용되게 보장
        """
        self.append_log("UI", f"[buttons] start={start_enabled}, stop={stop_enabled}")

        self._ui_toggle_gen += 1
        gen = self._ui_toggle_gen

        # 1) 즉시 반영
        with contextlib.suppress(Exception):
            if self._w_start and hasattr(self._w_start, "setEnabled"):
                self._w_start.setEnabled(bool(start_enabled))
            if self._w_stop and hasattr(self._w_stop, "setEnabled"):
                self._w_stop.setEnabled(bool(stop_enabled))

        # 2) 다음 틱 보정(레이스 방지) — 최신 gen만 실행
        def _force():
            if gen != getattr(self, "_ui_toggle_gen", 0):
                return  # 뒤늦게 도착한 구세대 요청은 무시
            with contextlib.suppress(Exception):
                if self._w_start and hasattr(self._w_start, "setEnabled"):
                    self._w_start.setEnabled(bool(start_enabled))
                if self._w_stop and hasattr(self._w_stop, "setEnabled"):
                    self._w_stop.setEnabled(bool(stop_enabled))
        try:
            QTimer.singleShot(0, _force)
        except Exception:
            pass

    def _set_running_ui_state(self) -> None:
        """공정 실행 중 UI 상태 (Start 비활성, Stop 활성)"""
        self._apply_button_state(start_enabled=False, stop_enabled=True)
        
    # ★ 추가: running 상태 변경 공통 처리 (TEST/실공정 모두 재사용 가능)
    def _on_process_status_changed(self, is_running: bool) -> None:
        self._running = bool(is_running)
        ch = int(getattr(self, "_selected_ch", 1) or 1)

        # runtime_state 반영(PC만) - ✅ 값이 바뀔 때만 set (불필요한 중복 제거)
        with contextlib.suppress(Exception):
            cur = bool(runtime_state.is_running("pc", ch))
            if cur != self._running:
                runtime_state.set_running("pc", self._running, ch)

        # 버튼/UI 반영
        if self._running:
            self._set_running_ui_state()
        else:
            self._apply_button_state(start_enabled=True, stop_enabled=False)

    def _skip_mfc_finalize_due_to_ch1(self) -> bool:
        """
        CH2에서 Plasma Cleaning을 하는 동안 CH1 'chamber' 공정이 돌고 있다면,
        MFC 폴링/상태 리셋은 CH1의 소유권을 침범하므로 생략한다.
        """
        try:
            # PC가 CH2 선택 + CH1 chamber 실행 중일 때만 스킵
            return int(getattr(self, "_selected_ch", 1)) == 2 and \
                   bool(runtime_state.is_running("chamber", 1))
        except Exception:
            return False


    async def _shutdown_rest_devices(self) -> None:
        """
        RF가 완전히 내려간 뒤 실행할 공용 ‘종료 시퀀스’.
        - 선택 가스만 OFF
        - SP4 측 MFC 밸브 open(네 코드 주석 기준)
        - 게이트밸브 닫기 시도(있으면)
        - 이벤트 펌프 정리(선택)
        실패해도 공정 정지를 막지 않도록 예외는 삼킨다.
        """
        # 1) GAS off (선택 채널만)
        with contextlib.suppress(Exception):
            if self.mfc_gas:
                self.append_log("STEP", "종료: MFC GAS OFF(sel)")
                await self.mfc_gas.flow_off_selected()
                self.append_log("STEP", "종료: MFC GAS OFF OK")

        # 2) SP4 밸브 open(네 런타임 주석 기준 종료 시엔 open)
        with contextlib.suppress(Exception):
            if self.mfc_pressure:
                self.append_log("STEP", "종료: MFC(SP4) VALVE OPEN")
                await self.mfc_pressure.valve_open()
                self.append_log("STEP", "종료: MFC(SP4) VALVE OPEN OK")

        # 3) 폴링 완전 종료 및 내부 상태 리셋 (gas/pressure MFC)
        with contextlib.suppress(Exception):
            skip_finalize = self._skip_mfc_finalize_due_to_ch1()
            if skip_finalize:
                self.append_log("MFC", "CH1 공정 중 → MFC 폴링/상태 리셋 생략")
            else:
                if self.mfc_gas:
                    if hasattr(self.mfc_gas, "on_process_finished"):
                        self.mfc_gas.on_process_finished(False)
                    elif hasattr(self.mfc_gas, "set_process_status"):
                        self.mfc_gas.set_process_status(False)
                if self.mfc_pressure:
                    if hasattr(self.mfc_pressure, "on_process_finished"):
                        self.mfc_pressure.on_process_finished(False)
                    elif hasattr(self.mfc_pressure, "set_process_status"):
                        self.mfc_pressure.set_process_status(False)

        # 3) 게이트밸브 OPEN 유지(정책) — 닫기 생략
        self.append_log("STEP", f"종료: GateValve CH{self._selected_ch} 유지(OPEN)")

    async def _disconnect_selected_devices(self) -> None:
        """
        Plasma Cleaning 종료 시 선택 장치만 연결 해제:
        - CH1:  MFC1(gas/pressure 동일 가능) + IG1
        - CH2:  MFC2(pressure) + IG2    (MFC1(gas)와 PLC는 유지)
        """
        try:
            use_ch = int(getattr(self, "_selected_ch", 1))
            # 장치-기반으로 '진짜 공유되는 경우'만 생략한다.
            # - CH1: mfc1을 끊으므로, CH2의 PC(= mfc1 gas 공유)만 위험
            # - CH2: mfc2만 끊으므로, 다른 쪽이 무엇을 하든 안전 → 생략 가드 불필요
            skip_disconnect = False
            with contextlib.suppress(Exception):
                if use_ch == 1:
                    # CH2 PC가 실행 중이면 mfc1 gas를 공유하므로 끊지 않는다
                    skip_disconnect = bool(runtime_state.is_running("pc", 2))
                else:
                    # CH2 → mfc2만 정리하므로 언제나 끊어도 안전
                    skip_disconnect = False

            if skip_disconnect:
                self.append_log("MFC", "CH2 PC 실행 중 → mfc1 공유 → MFC disconnect 생략")
                return
            else:            
                # ← 여기부터는 ‘안전할 때만’ 끊음
                if int(getattr(self, "_selected_ch", 0)) == 1:
                    mfc_set = {m for m in (self.mfc_gas, self.mfc_pressure) if m}
                    for m in mfc_set:
                        self.append_log("MFC", "CH1 종료: MFC 연결 해제")
                        with contextlib.suppress(Exception):
                            await asyncio.wait_for(m.cleanup(), timeout=3.0)
                else:
                    if self.mfc_pressure:
                        self.append_log("MFC", "CH2 종료: Pressure MFC 연결 해제")
                        with contextlib.suppress(Exception):
                            await asyncio.wait_for(self.mfc_pressure.cleanup(), timeout=3.0)

            # 2) IG — 항상 정리
            if self.ig:
                self.append_log("IG", f"CH{self._selected_ch} 종료: IG 연결 해제")
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(self.ig.cleanup(), timeout=3.0)

            # PLC는 공유자원 → 절대 끊지 않음
        except Exception as e:
            self.append_log("PC", f"장치 연결 해제 중 예외: {e!r}")

    def _read_params_from_ui(self) -> PCParams:
        # ✅ Config(Plasma cleaning 탭) 기본값 로드
        pc_cfg: dict = {}
        try:
            pc_cfg = (user_config.load() or {}).get("plasma_cleaning", {}) or {}
            if not isinstance(pc_cfg, dict):
                pc_cfg = {}
        except Exception:
            pc_cfg = {}

        cfgm = getattr(self, "_cfg_mod", cfgc)

        # ✅ 공통 기본값은 config_common.py에서 가져온다.
        default_gas_idx = int(getattr(cfgm, "PC_DEFAULT_GAS_IDX", 3))
        default_gas_flow = float(getattr(cfgm, "PC_DEFAULT_GAS_FLOW_SCCM", 0.0))
        default_target_pressure = float(getattr(cfgm, "PC_DEFAULT_TARGET_PRESSURE_TORR", 5.0e-6))
        default_tol_mtorr = float(getattr(cfgm, "PC_DEFAULT_TOL_MTORR", 0.2))
        default_wait_timeout_s = float(getattr(cfgm, "PC_DEFAULT_WAIT_TIMEOUT_S", 90.0))
        default_sp4_setpoint = float(getattr(cfgm, "PC_DEFAULT_SP4_SETPOINT_MTORR", 2.0))
        default_rf_power = float(getattr(cfgm, "PC_DEFAULT_RF_POWER_W", 100.0))
        default_process_time = float(getattr(cfgm, "PC_DEFAULT_PROCESS_TIME_MIN", 1.0))
        default_gv_open_lamp_delay_s = float(getattr(cfgm, "PC_DEFAULT_GV_OPEN_LAMP_DELAY_S", 5.0))
        default_ig_interval_ms = int(getattr(cfgm, "PC_DEFAULT_IG_INTERVAL_MS", 10_000))

        def _cfg_float(key: str, default: float) -> float:
            try:
                v = pc_cfg.get(key, default)
                return float(v)
            except Exception:
                return float(default)

        def _cfg_int(key: str, default: int) -> int:
            try:
                v = pc_cfg.get(key, default)
                return int(v)
            except Exception:
                return int(default)

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

                # ✅ 비어 있으면 default(=config 값) 사용
                return float(txt) if txt else default
            except Exception:
                return default

        # ✅ UI 입력이 비어 있으면 Config 기본값을 사용
        gas_flow        = _read_plain_number("PC_gasFlow_edit",         _cfg_float("gas_flow_sccm", default_gas_flow))
        target_pressure = _read_plain_number("PC_targetPressure_edit",  _cfg_float("target_pressure", default_target_pressure))
        sp4_setpoint    = _read_plain_number("PC_workingPressure_edit", _cfg_float("sp4_setpoint_mTorr", default_sp4_setpoint))
        rf_power        = _read_plain_number("PC_rfPower_edit",         _cfg_float("rf_power_w", default_rf_power))
        process_time    = _read_plain_number("PC_ProcessTime_edit",     _cfg_float("process_time_min", default_process_time))

        return PCParams(
            gas_idx            = _cfg_int("gas_idx", default_gas_idx),
            gas_flow_sccm      = gas_flow,
            target_pressure    = target_pressure,
            tol_mTorr          = _cfg_float("tol_mTorr", default_tol_mtorr),
            wait_timeout_s     = _cfg_float("wait_timeout_s", default_wait_timeout_s),
            sp4_setpoint_mTorr = sp4_setpoint,
            rf_power_w         = rf_power,
            process_time_min   = process_time,
            gv_open_lamp_delay_s = _cfg_float("gv_open_lamp_delay_s", default_gv_open_lamp_delay_s),
            ig_interval_ms       = _cfg_int("ig_interval_ms", default_ig_interval_ms),
        )

    def _set_state_text(self, text: str) -> None:
        if not self._w_state:
            return
        try:
            self._w_state.setPlainText(str(text))
        except Exception:
            pass

    def append_log(self, src: str, msg: str) -> None:
        # 🔇 Plasma Cleaning 화면에서 MFC/IG의 [poll] 라인 숨김(표시만 억제)
        try:
            if isinstance(msg, str) and msg.lstrip().startswith("[poll]"):
                if isinstance(src, str) and (src.startswith("MFC") or src.startswith("IG")):
                    return
        except Exception:
            pass

        line = f"[{datetime.now().strftime('%H:%M:%S')}] {src}: {msg}"
        # UI
        if self._w_log:
            try:
                w = self._w_log
                sb = w.verticalScrollBar()

                # ✅ 사용자가 이미 최하단을 보고 있을 때만 '바닥에 붙이는' 오토 스크롤 유지
                stick_to_bottom = True
                try:
                    stick_to_bottom = (sb.value() >= (sb.maximum() - 2))
                except Exception:
                    stick_to_bottom = True

                if hasattr(w, "appendPlainText"):
                    w.appendPlainText(line)
                elif hasattr(w, "append"):
                    w.append(line)          # QTextEdit 폴백
                else:
                    return

                if stick_to_bottom:
                    # 워드랩/레이아웃 계산 이후(다음 이벤트 루프 틱)에 scrollbar maximum이 갱신되므로
                    # 즉시 이동하면 '최하단 바로 위'에서 멈출 수 있음 → singleShot(0)로 보정
                    if not getattr(self, "_log_autoscroll_pending", False):
                        self._log_autoscroll_pending = True

                        def _scroll_bottom():
                            self._log_autoscroll_pending = False
                            ww = getattr(self, "_w_log", None)
                            if not ww:
                                return
                            sbb = ww.verticalScrollBar()
                            sbb.setValue(sbb.maximum())

                        QTimer.singleShot(0, _scroll_bottom)

            except Exception:
                pass

        # 파일
        try:
            self._queue_run_log_line(line)
        except Exception:
            # 파일 오류가 난다고 공정을 멈출 필요는 없음 — 조용히 무시
            pass

    def _queue_run_log_line(self, line: str) -> None:
        # ✅ 파일이 열려있을 때만 버퍼링/저장 (안 열려 있으면 쌓지 않음)
        if not getattr(self, "_log_fp", None):
            return

        self._runlog_buf.append(line)

        # 폭주 보호
        if len(self._runlog_buf) >= 2000:
            self._flush_run_log()

    def _flush_run_log(self) -> None:
        """버퍼에 쌓인 파일 로그를 한 번에 파일로 flush."""
        fp = getattr(self, "_log_fp", None)
        if not fp:
            return
        if not self._runlog_buf:
            return

        lines = []
        while self._runlog_buf and len(lines) < 2000:
            lines.append(self._runlog_buf.popleft())

        try:
            fp.write("\n".join(lines) + "\n")
            fp.flush()
        except Exception:
            # 파일 오류는 공정을 죽이지 않게
            pass

    async def _cancel_and_wait(self, tasks: list[asyncio.Task]) -> None:
        curr = asyncio.current_task()
        safe_tasks: list[asyncio.Task] = []
        for t in list(tasks):
            try:
                if t is curr:    # ★ 자기 자신은 제외 (재귀 취소 방지)
                    continue
                if t and not t.done():
                    t.cancel()
                safe_tasks.append(t)
            except Exception:
                pass
        if safe_tasks:
            # ★ wait_for 제거: 타임아웃 취소의 2중 전파로 인한 재귀 가능성 차단
            with contextlib.suppress(Exception):
                await asyncio.gather(*safe_tasks, return_exceptions=True)

    async def _shutdown_all_tasks(self) -> None:
        """PC 런타임이 만든 태스크만 전부 종료(취소+완료 대기)."""
        bg = list(getattr(self, "_bg_tasks", []))
        ev = list(getattr(self, "_event_tasks", []))

        await self._cancel_and_wait(bg)
        await self._cancel_and_wait(ev)

        if hasattr(self, "_bg_tasks") and self._bg_tasks is not None:
            self._bg_tasks.clear()
        if hasattr(self, "_event_tasks") and self._event_tasks is not None:
            self._event_tasks.clear()

    async def _cleanup_tasks_only(self, reason: str = "") -> None:
        """
        프리플라이트 실패 등 '공정이 실제로 시작되기 전' 경로에서
        장비(RF/MFC/PLC)에는 손대지 않고,
        PC 런타임이 만든 태스크/로그만 정리한다.
        """
        if reason:
            self.append_log("MAIN", f"[CLEANUP_TASKS_ONLY] {reason}")

        # PC 런타임이 만든 태스크만 정리
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self._shutdown_all_tasks(), timeout=3.0)

        # run log가 열려있으면 닫기(열려있지 않으면 no-op)
        with contextlib.suppress(Exception):
            self._close_run_log()

    async def shutdown_fast_async(self) -> None:
        """
        앱 종료 시 빠른 정리 (await 가능):
        - PC 런타임이 만든 asyncio task 취소/대기
        - PC run log 파일 닫기
        - 최소 상태 플래그 정리
        """
        # ✅ 종료 의도 명시
        with contextlib.suppress(Exception):
            self._stop_requested = True

        with contextlib.suppress(Exception):
            self._process_timer_active = False

        with contextlib.suppress(Exception):
            self._state_header = ""

        # 1) 태스크 정리
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self._shutdown_all_tasks(), timeout=3.0)

        # 2) 로그 파일 닫기
        with contextlib.suppress(Exception):
            self._close_run_log()

        # 3) 상태 플래그 정리
        with contextlib.suppress(Exception):
            self._running = False


    def shutdown_fast(self) -> None:
        """
        기존 호출부 호환용 wrapper.
        """
        loop = getattr(self, "_loop", None)
        if loop is None:
            return

        def _create_task():
            try:
                loop.create_task(self.shutdown_fast_async(), name="PC.shutdown_fast")
            except Exception:
                pass

        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        try:
            if running is loop:
                loop.call_soon(_create_task)
            else:
                loop.call_soon_threadsafe(_create_task)
        except Exception:
            pass

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

    def _open_run_log(self, p: PCParams) -> None:
        # ★ 이전 세션 타이머가 멈춰 있었다면 다시 시작
        try:
            if getattr(self, "_runlog_timer", None) and not self._runlog_timer.isActive():
                self._runlog_timer.start()
        except Exception:
            pass

        # 세션 ID = 시작 시각
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._log_session_id = ts
        fname = f"PC_{ts}_CH{self._selected_ch}.log"
        path = self._pc_log_dir / fname
        self._log_fp = open(path, "a", encoding="utf-8", buffering=1)  # line-buffered

        # 헤더 기록
        self._log_fp.write("# ==== Plasma Cleaning Run ====\n")
        self._log_fp.write(f"# started_at = {datetime.now().isoformat()}\n")
        self._log_fp.write(f"# chamber = CH{self._selected_ch}\n")
        self._log_fp.write(f"# mfc_gas = {_mfc_name(self.mfc_gas)}\n")
        self._log_fp.write(f"# mfc_pressure = {_mfc_name(self.mfc_pressure)}\n")
        self._log_fp.write(f"# params: gas_idx={p.gas_idx}, flow={p.gas_flow_sccm:.1f} sccm, "
                        f"IG_target={p.target_pressure:.3e} Torr, "
                        f"SP4={p.sp4_setpoint_mTorr:.2f} mTorr, RF={p.rf_power_w:.1f} W, "
                        f"time={p.process_time_min:.2f} min\n")
        self._log_fp.write("# ============================\n")
        self._log_fp.flush()

    def _close_run_log(self) -> None:
        # ✅ 마지막 남은 버퍼를 먼저 flush
        try:
            self._flush_run_log()
        except Exception:
            pass

        # ✅ 타이머 중지
        try:
            if getattr(self, "_runlog_timer", None):
                self._runlog_timer.stop()
        except Exception:
            pass

        try:
            if self._log_fp:
                self._log_fp.close()
        except Exception:
            pass
        self._log_fp = None

    def _reset_ui_state(self, *, restore_time_min: Optional[float] = None) -> None:
        """종료 또는 STOP 후 UI를 초기 상태로 되돌림(챔버 공정과 동일한 체감).
        - 상태표시: IDLE
        - ProcessTime_edit: 시작 시 분 값으로 복원(있으면), 없으면 건드리지 않음
        - FWD/REF 표시칸: 공백
        - Start/Stop 버튼: Start=enabled, Stop=disabled
        - 내부 플래그: 카운트다운 비활성, 상태 헤더 제거, 목표 도달 이벤트 클리어
        """
        # 내부 플래그/이벤트 정리
        self._process_timer_active = False
        self._state_header = ""
        with contextlib.suppress(Exception):
            self._rf_target_evt.clear()

        # ★ 추가: RF 목표 결과/사유 리셋(다음 런에서 stale 값 방지)
        self._rf_target_ok = None
        self._rf_target_reason = ""

        # ★ 추가(권장): 최종 보정 플래그도 리셋
        self._forced_fail = False
        self._forced_fail_reason = None

        # 상태 텍스트: Chamber와 동일하게 '대기 중'으로 표시
        self._set_state_text("대기 중")

        # ProcessTime 복원
        if restore_time_min is not None:
            with contextlib.suppress(Exception):
                w = getattr(self.ui, "PC_ProcessTime_edit", None)
                if w and hasattr(w, "setPlainText"):
                    w.setPlainText(f"{float(restore_time_min):.2f}")

        # FWD/REF 표시칸 초기화
        with contextlib.suppress(Exception):
            for_w = getattr(self.ui, "PC_forP_edit", None)
            ref_w = getattr(self.ui, "PC_refP_edit", None)
            if for_w and hasattr(for_w, "setPlainText"):
                for_w.setPlainText("")
            if ref_w and hasattr(ref_w, "setPlainText"):
                ref_w.setPlainText("")

        # 버튼은 중앙 헬퍼로만 토글
        self._apply_button_state(start_enabled=True, stop_enabled=False)

    async def _notify_finish_once(self, *, ok: bool, reason: str | None = None, stopped: bool = False) -> None:
        # 0) 재진입 차단
        if self._final_notified:
            return
        self._final_notified = True

        # 1) 전역 종료/해제 — 챗이 실패해도 반드시 풀림
        try:
            ch = int(getattr(self, "_selected_ch", 1))

            if ok or stopped:
                runtime_state.clear_error("pc", ch)
            else:
                _reason = (reason or "error")
                runtime_state.set_error("pc", ch, _reason)

                try:
                    self._post_critical(
                        f"CH{ch} Plasma Cleaning 실패",
                        f"사유: {_reason}\n\n확인을 누르면 상태 표시가 Idle로 변경됩니다.",
                        clear_status_to_idle=True,
                        ch=ch,
                    )
                except Exception:
                    pass

            runtime_state.mark_finished("pc", ch)

        except Exception as e:
            self.append_log("STATE", f"runtime_state finalize mark failed: {e!r}")

        # 2) 챗이 없으면 여기서 종료 (상태 해제는 이미 완료)
        if not getattr(self, "chat", None):
            self.append_log("CHAT", "chat=None → 종료 카드 생략")
            return

        # 3) 페이로드 구성(상태/시간/단계 정보 보강: 있으면 넣고, 없으면 생략)
        status = "stopped" if stopped else ("success" if ok else "failed")
        payload = {
            "process_name": "Plasma Cleaning",
            "prefix": f"CH{getattr(self, '_selected_ch', '?')} Plasma Cleaning",
            "ch": int(getattr(self, "_selected_ch", 0) or 0),
            "status": status,
            "stopped": bool(stopped),
        }

        pc = getattr(self, "pc", None)
        if pc is not None:
            if getattr(pc, "last_error_code", None):
                payload["error_code"] = pc.last_error_code
            if getattr(pc, "last_error_source", ""):
                payload["error_source"] = pc.last_error_source
            if getattr(pc, "last_error_detail", ""):
                payload["error_detail"] = pc.last_error_detail
            if getattr(pc, "last_error_meta", None):
                payload["error_meta"] = dict(pc.last_error_meta)

        if reason:
            payload["reason"] = str(reason)
            payload["errors"] = [str(reason)]

        # 4) 종료 카드 전송 — 코루틴/동기 모두 지원 + 타임아웃으로 행거 방지
        try:
            ret = self.chat.notify_process_finished_detail(ok, payload)
            if inspect.iscoroutine(ret):
                await asyncio.wait_for(ret, timeout=3.0)   # ← 행거 방지
            # flush도 동기/비동기 모두 대응
            if hasattr(self.chat, "flush"):
                f = self.chat.flush()
                if inspect.iscoroutine(f):
                    await asyncio.wait_for(f, timeout=2.0)
        except asyncio.TimeoutError:
            self.append_log("CHAT", "finish notify timeout (3s)")
        except Exception as e:
            self.append_log("CHAT", f"finish notify failed: {e!r}")

        # 👇 추가: 실패 이유만 텍스트로 별도 전송(카드 잘림 방지)
        if (not ok):
            _reason = (str(payload.get("reason") or "")).strip()
            if not _reason:
                errs = payload.get("errors", [])
                if isinstance(errs, (list, tuple)) and errs:
                    _reason = str(errs[0])
                elif isinstance(errs, str):
                    _reason = errs
            if _reason:
                try:
                    r = getattr(self.chat, "notify_text", None)
                    if callable(r):
                        out = r(f"❌ Plasma Cleaning 실패 이유: {_reason}")
                        if inspect.iscoroutine(out):
                            await asyncio.wait_for(out, timeout=2.0)
                    elif hasattr(self.chat, "notify_error_with_src"):
                        # notify_text가 없다면 예비 경로
                        out = self.chat.notify_error_with_src("PC", f"실패 이유: {_reason}")
                        if inspect.iscoroutine(out):
                            await asyncio.wait_for(out, timeout=2.0)
                except Exception as e:
                    self.append_log("CHAT", f"reason text notify failed: {e!r}")
                else:
                    # ★ 추가: 실패 텍스트도 카드 직후에 바로 나가도록 즉시 flush
                    if hasattr(self.chat, "flush"):
                        f2 = self.chat.flush()
                        if inspect.iscoroutine(f2):
                            await asyncio.wait_for(f2, timeout=2.0)

    def _post_warning(self, title: str, text: str, auto_close_ms: int = 5000) -> None:
        try:
            if not self._has_ui():
                raise RuntimeError("UI/parent not ready")

            self._ensure_msgbox_store()
            parent = self._parent_widget()
            if not isinstance(parent, QWidget):
                raise RuntimeError("parent widget not found")

            box = QMessageBox(parent)
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
        except Exception as e:
            self.append_log("PC", f"[경고] {title}: {text} ({e!s})")

    def _post_critical(
        self,
        title: str,
        text: str,
        *,
        clear_status_to_idle: bool = False,
        ch: Optional[int] = None,
    ) -> None:
        """
        공정 실패/인터락 등 '에러 상태'를 사용자에게 확실히 알리고,
        사용자가 OK를 눌렀을 때만 runtime_state error를 해제해서 idle로 복귀시키는 팝업.
        - auto-close 없음 (계속 떠있음)
        - 비모달 + 참조 보관(_msg_boxes)으로 GC 방지
        """
        try:
            box = QMessageBox(self.ui)
            box.setIcon(QMessageBox.Critical)
            box.setWindowTitle(title)
            box.setText(str(text))
            box.setStandardButtons(QMessageBox.Ok)
            box.setModal(False)

            # ✅ OK 눌렀을 때만 idle로 복귀
            def _on_closed(result: int) -> None:
                try:
                    if clear_status_to_idle and result == int(QMessageBox.Ok):
                        # ch 미지정이면 현재 선택 채널 사용
                        _ch = int(ch) if ch is not None else int(getattr(self, "_selected_ch", 1))
                        runtime_state.clear_error("pc", _ch)
                except Exception:
                    pass

            self._msg_boxes.append(box)
            box.open(_on_closed)   # show()가 아니라 open(callback)
        except Exception:
            pass

    # ===== 알림창 유틸: 챔버와 동일한 방식 =====
    def _has_ui(self) -> bool:
        """QApplication과 부모 위젯이 준비되었는지 확인"""
        try:
            app = QApplication.instance()
            if not app:
                return False
            w = self._parent_widget()
            return isinstance(w, QWidget)
        except Exception:
            return False

    def _ensure_msgbox_store(self) -> None:
        """메시지박스 참조를 보관해서 GC로 사라지지 않게 유지"""
        if not hasattr(self, "_msg_boxes"):
            self._msg_boxes = []

    def _parent_widget(self) -> Optional[QWidget]:
        """
        알림창의 부모로 쓸 QWidget을 안전하게 찾아준다.
        - ui가 QWidget이면 그대로 사용
        - 아니면 PC 페이지의 대표 위젯(window())을 부모로 사용
        - 마지막으로 activeWindow()로 폴백
        """
        try:
            if isinstance(self.ui, QWidget):
                return self.ui

            # PC 페이지에서 확실히 존재하는 후보들(main_window.py 기준)
            candidates = [
                "pc_logMessage_edit", "pc_processState_edit",
                "PC_Start_button", "pcStart_button",
                "PC_Stop_button",  "pcStop_button",
            ]
            for name in candidates:
                w = _safe_get(self.ui, name)
                if isinstance(w, QWidget):
                    # 탭/스택 위젯 내부라도 window()를 부모로 쓰면 잘 뜬다
                    try:
                        win = w.window()
                        if isinstance(win, QWidget):
                            return win
                    except Exception:
                        return w

            # 최후의 수단
            aw = QApplication.activeWindow()
            if isinstance(aw, QWidget):
                return aw
            app = QApplication.instance()
            if app:
                aw = app.activeWindow()
                if isinstance(aw, QWidget):
                    return aw
        except Exception:
            pass
        return None
    
    # ===== CSV 단발 로더: 첫 데이터 행만 읽어 UI에 반영 =====
    def _set_plaintext(self, name: str, value: Any) -> None:
        """QPlainTextEdit/QLineEdit/QSpinBox 등에 값 세팅 (있으면만)."""
        w = _safe_get(self.ui, name)
        if not w:
            return
        try:
            if hasattr(w, "setPlainText"):
                w.setPlainText(str(value))
            elif hasattr(w, "setText"):
                w.setText(str(value))
            elif hasattr(w, "setValue"):
                # 숫자 위젯
                w.setValue(float(value))
        except Exception:
            pass

    def _apply_recipe_row_to_ui(self, row: Mapping[str, Any]) -> None:
        """
        CSV의 1개 행(딕셔너리)을 받아 PC UI 위젯에 값만 채운다.
        - 공정 실행은 하지 않음 (요청사항: 단발 세팅만)
        """
        # 1) 채널 선택(옵션)
        use_ch = str(row.get("use_ch", "")).strip()
        if use_ch:
            # "1", "2", "1.0" 같은 값들을 float → int로 정규화
            try:
                ch_val = int(float(use_ch))
            except Exception:
                ch_val = 1  # 이상한 값이면 안전하게 CH1로

            # (1) UI 라디오 버튼 상태를 먼저 바꿔서
            #     main.py 쪽의 라디오 토글 슬롯(_apply_pc_ch_selection)을 타게 한다.
            try:
                # CH2 선택
                if ch_val == 2 and hasattr(self.ui, "PC_useChamber2_radio"):
                    # setChecked(True)를 쓰면 상태가 바뀔 때 toggled 시그널이 나감
                    self.ui.PC_useChamber2_radio.setChecked(True)
                # CH1 선택
                elif ch_val == 1 and hasattr(self.ui, "PC_useChamber1_radio"):
                    self.ui.PC_useChamber1_radio.setChecked(True)
            except Exception:
                # UI가 없거나, 테스트 코드에서만 돌릴 때를 대비해 조용히 무시
                pass

            # (2) 런타임 내부 선택 채널도 맞춰줌(로그/PLC 키용)
            try:
                self.set_selected_ch(ch_val)
            except Exception:
                pass

        # 2) 숫자 파싱 헬퍼
        def _f(key: str, default: float = 0.0) -> str:
            s = str(row.get(key, "")).strip()
            if s == "":
                return str(default)
            # 과학표기(5.00E-06 등) 포함 → 그대로 텍스트로 유지
            return s

        # 3) UI에 값 세팅
        self._set_plaintext("PC_targetPressure_edit",   _f("base_pressure"))     # Torr
        self._set_plaintext("PC_gasFlow_edit",          _f("gas_flow"))          # sccm
        self._set_plaintext("PC_workingPressure_edit",  _f("working_pressure"))  # mTorr
        self._set_plaintext("PC_rfPower_edit",          _f("rf_power"))          # W
        self._set_plaintext("PC_ProcessTime_edit",      _f("process_time"))      # min

    def _read_first_row_from_csv(self, file_path: str) -> Optional[dict]:
        """CSV의 첫 데이터 행(헤더 제외)을 dict로 반환. 없으면 None."""
        try:
            with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # 첫 non-empty 행만 반환
                    if any((v or "").strip() for v in row.values()):
                        return { (k or "").strip(): (v or "").strip() for k, v in row.items() }
        except Exception as e:
            self.append_log("File", f"CSV 읽기 실패: {e!r}")
        return None

    async def _handle_process_list_clicked_async(self) -> None:
        """PC_processList_button → 파일 선택 → 첫 행으로 UI 세팅(단발)."""
        start_dir = (
            getattr(self, "_last_process_list_dir", "")
            or str(getattr(getattr(self, "_cfg_mod", cfgc), "PROCESS_LIST_START_DIR", r"\\VanaM_NAS\VanaM_toShare"))
        )

        try:
            if start_dir and not Path(start_dir).exists():
                self.append_log("File", f"시작 폴더 없음 → 기본 위치로 열기: {start_dir}")
                start_dir = ""
        except Exception:
            start_dir = ""

        file_path = await self._aopen_file(
            caption="Plasma Cleaning 레시피(CSV) 선택",
            start_dir=start_dir,
            name_filter="CSV Files (*.csv);;All Files (*)"
        )

        if file_path:
            with contextlib.suppress(Exception):
                self._last_process_list_dir = str(Path(file_path).parent)

        if not file_path:
            self.append_log("File", "파일 선택 취소")
            return

        row = self._read_first_row_from_csv(file_path)
        if not row:
            self._post_warning("CSV 오류", "데이터 행이 없습니다.")
            return

        self._loaded_recipe_row = dict(row)
        self._apply_recipe_row_to_ui(row)
        self.append_log("File", f"CSV 로드 완료: {file_path}\n→ UI에 값 세팅")

    async def _aopen_file(self, caption="파일 선택", start_dir="", name_filter="All Files (*)") -> str:
        """Qt 비동기 파일 열기 다이얼로그 (QFileDialog). 취소 시 빈 문자열."""
        if not self._has_ui():
            return ""

        from PySide6.QtWidgets import QFileDialog, QDialog

        dlg = QFileDialog(self._parent_widget() or None, caption, start_dir, name_filter)
        dlg.setFileMode(QFileDialog.ExistingFile)

        # ✅ Windows 네이티브 파일 다이얼로그 우회
        with contextlib.suppress(Exception):
            dlg.setOption(QFileDialog.Option.DontUseNativeDialog, True)
        with contextlib.suppress(Exception):
            dlg.setOption(QFileDialog.DontUseNativeDialog, True)

        # ✅ 챔버 런타임과 동일하게 수명/모달리티 고정
        with contextlib.suppress(Exception):
            dlg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
            dlg.setWindowModality(Qt.WindowModality.WindowModal)

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[str] = loop.create_future()

        def _done(result: int):
            # ✅ finished 중복 호출 방지
            if fut.done():
                return
            try:
                if result == QDialog.Accepted and dlg.selectedFiles():
                    fut.set_result(dlg.selectedFiles()[0])
                else:
                    fut.set_result("")
            finally:
                with contextlib.suppress(Exception):
                    dlg.finished.disconnect(_done)
                with contextlib.suppress(Exception):
                    dlg.deleteLater()

        dlg.finished.connect(_done)
        dlg.open()

        try:
            return await fut
        finally:
            if _qt_is_valid(dlg):
                with contextlib.suppress(Exception):
                    dlg.close()
                with contextlib.suppress(Exception):
                    dlg.deleteLater()
    
    def reload_runtime_cfg(self) -> None:
        """
        ConfigDialog Apply(Runtime) 이후 main에서 호출 가능.
        - PCParams는 _read_params_from_ui에서 매번 읽으므로 여기서는 장비 캐시만 갱신
        """
        # RFPowerAsync 캐시 갱신
        with contextlib.suppress(Exception):
            if self.rf and hasattr(self.rf, "reload_runtime_cfg"):
                self.rf.reload_runtime_cfg()

        # PLC/MFC/IG 캐시 갱신
        for dev in (self.plc, self.mfc_gas, self.mfc_pressure, self.ig):
            with contextlib.suppress(Exception):
                if dev and hasattr(dev, "reload_runtime_cfg"):
                    dev.reload_runtime_cfg()
    
    # ======= 서버 통신을 통한 실행 api =======
    async def start_with_recipe_string(self, recipe: str) -> None:
        """
        외부 제어(Host) 진입점.

        - recipe == "" 또는 None:
            현재 UI 값으로 단발 실행(버튼 클릭과 동일)하되,
            프리플라이트(쿨다운/교차실행/장비 연결)가 통과했는지만 Host에 반환한다.
        - recipe 가 .csv 경로:
            첫 데이터 행으로 UI 갱신 후 기존 Start 경로 실행(위와 동일하게 프리플라이트 결과만 반환).

        공정 본체(PCController._run)는 _on_click_start()에서 비동기로 끝까지 실행되고,
        이 함수는 프리플라이트 성공/실패에 따른 OK/FAIL만 짧게 응답한다.
        """
        loop = asyncio.get_running_loop()

        # 이미 다른 Host start가 대기 중이면 차단(동시 중복 호출 방지)
        old_fut = getattr(self, "_host_start_future", None)
        if old_fut is not None and not old_fut.done():
            raise RuntimeError("다른 Plasma Cleaning start 요청이 이미 처리 중입니다.")

        # 새 프리플라이트 Future 준비
        self._host_start_future = loop.create_future()

        try:
            s = (recipe or "").strip()
            if not s:
                # 현재 UI 값으로 버튼 클릭과 동일하게 실행 (비동기)
                asyncio.create_task(self._on_click_start())
            elif s.lower().endswith(".csv"):
                if not os.path.exists(s):
                    raise RuntimeError(f"CSV 파일을 찾을 수 없습니다: {s}")

                row = self._read_first_row_from_csv(s)
                if not row:
                    raise RuntimeError("CSV에 데이터 행이 없습니다.")

                self._loaded_recipe_row = dict(row)  # ✅ 추가
                # CSV → UI 세팅 (use_ch 있으면 set_selected_ch까지 내부 적용)
                self._apply_recipe_row_to_ui(row)
                self.append_log("File", f"CSV 로드 완료: {s} → UI에 값 세팅")

                # 기존 Start 경로로 실행 (쿨다운/프리플라이트/로깅/종료 처리 모두 기존대로, 비동기)
                asyncio.create_task(self._on_click_start())
            else:
                raise RuntimeError("지원하지 않는 레시피 형식입니다. CSV 경로만 허용됩니다.")

            # 🔎 여기서 프리플라이트 결과 신호만 대기 (예: 최대 10초)
            host_start_wait_timeout_s = float(
                getattr(self._cfg_mod, "PC_HOST_START_WAIT_TIMEOUT_S", 10.0)
            )

            try:
                ok, reason = await asyncio.wait_for(self._host_start_future, timeout=host_start_wait_timeout_s)
            except asyncio.TimeoutError:
                raise RuntimeError(
                    f"preflight timeout({host_start_wait_timeout_s:.1f}s) "
                    "(쿨다운/가드 등으로 프리플라이트에 도달하지 못했습니다)"
                )

            if not ok:
                # 실패 사유를 그대로 Host에 전달
                raise RuntimeError(reason or "Plasma Cleaning start failed")
            # ok=True면 그대로 리턴 → handlers.start_plasma_cleaning에서 OK 응답
        finally:
            # 어떤 경우에도 Future는 정리해서 다음 런에 영향이 없게
            self._host_start_future = None

# ─────────────────────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────────────────────
    def _parse_duration_seconds(self, s: str) -> float:
        import re
        if not s:
            return 0.0
        s = s.replace(" ", "").lower()
        pattern = r"(?:(\d+(?:\.\d+)?)h)?(?:(\d+(?:\.\d+)?)m)?(?:(\d+(?:\.\d+)?)s)?"
        m = re.match(pattern, s)
        if not m:
            try:
                return float(s) * 60.0
            except Exception:
                return 0.0
        h = float(m.group(1) or 0)
        m_ = float(m.group(2) or 0)
        s_ = float(m.group(3) or 0)
        return h * 3600 + m_ * 60 + s_

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



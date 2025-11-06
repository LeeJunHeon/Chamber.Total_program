# runtime/plasma_cleaning_runtime.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio, contextlib, inspect, csv, os
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, Mapping

from PySide6.QtGui import QTextCursor
from PySide6.QtWidgets import QMessageBox, QPlainTextEdit, QApplication, QWidget
from PySide6.QtCore import Qt, QTimer   # ⬅ 챔버와 동일한 모달리티/속성 적용용

# 장비/컨트롤러
from device.mfc import AsyncMFC
from device.plc import AsyncPLC
from device.ig import AsyncIG  # IG 직접 주입 지원
from controller.plasma_cleaning_controller import PlasmaCleaningController, PCParams
from device.rf_power import RFPowerAsync, RFPowerEvent
from controller.runtime_state import runtime_state  # ★ 추가: 전역 쿨다운/이력

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
        self._pc_log_dir = (log_dir / "plasma_cleaning")
        self._pc_log_dir.mkdir(parents=True, exist_ok=True)

        self._log_fp = None                # 현재 런 세션 로그 파일 핸들
        self._log_session_id = None        # 파일명에 들어갈 세션 ID (timestamp)

        # 🔒 종료 챗 exactly-once 보장용 플래그
        self._final_notified: bool = False
        self._stop_requested: bool = False

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
        self._running: bool = False
        self._selected_ch: int = 1  # 라디오에 맞춰 set_selected_ch로 갱신
        self._pc_gas_idx: Optional[int] = None  # ← PC에서 선택된 gas_idx 저장(스케일 계산용)

        self._rf_target_evt = asyncio.Event()   # ★ 목표 도달 이벤트 대기용
        self._state_header: str = ""            # ★ 현재 단계 제목 보관

        # ▶ 공정(Process) 타이머 활성화 여부 (SP4/IG 대기는 False)
        self._process_timer_active: bool = False

        # ▶ 종료/정지 후 UI 복원용 시작 시 분 값 저장소
        self._last_process_time_min: Optional[float] = None

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

                # ▶ 컨트롤러에도 실패 통지 → 컨트롤러가 STOP 플래그 세팅
                try:
                    if getattr(self, "pc", None):
                        self.pc.on_mfc_failed(getattr(ev, "cmd", "") or "", getattr(ev, "reason", "") or "unknown")
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
                elif ev.kind == "target_reached":   # ★ 추가
                    self.append_log("RF", "목표 파워 도달")
                    self._rf_target_evt.set()
                elif ev.kind == "power_off_finished":   # ★ 추가
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
                    fn = getattr(dev, "connect", None)
                    if not callable(fn):
                        raise RuntimeError(f"{name}는 connect()를 제공해야 합니다 (start() 금지)")
                    res = fn()
                    if inspect.isawaitable(res):
                        await res
                    self.append_log("PC", f"{name} connect 호출")
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
            # ★ 살아있는 태스크만 대상으로 이름 비교
            return any((getattr(t, "get_name", lambda: "")() == name) for t in self._event_tasks)

        if self.rf and not _has_task("PC.Pump.RF"):
            self._event_tasks.append(asyncio.create_task(self._pump_rf_events(), name="PC.Pump.RF"))

        # 같은 MFC 인스턴스를 가리키면 펌프는 '하나만' 띄운다
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

        # ---- RF (PLC DCV ch=1 사용 — enable/write/read)
        async def _rf_start(power_w: float) -> None:
            if not self.rf:
                return
            # ★ 목표 도달 이벤트 기다림 (타임아웃은 취향껏: 60s 예시)
            self._rf_target_evt.clear()

            await self.rf.start_process(float(power_w))

            try:
                await asyncio.wait_for(self._rf_target_evt.wait(), timeout=60.0)
                self.append_log("RF", "목표 파워 안정 → 프로세스 타이머 시작 가능")
                # ▶ 이제부터만 ProcessTime_edit에 카운트다운을 표시
                self._process_timer_active = True
            except asyncio.TimeoutError:
                self.append_log("RF", "목표 파워 미도달(60s timeout) → 중단")
                self._process_timer_active = False
                raise
            except Exception:
                self._process_timer_active = False
                raise

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

        return RFPowerAsync(
            send_rf_power=_rf_send,
            send_rf_power_unverified=_rf_send_unverified,
            request_status_read=_rf_request_read,
            toggle_enable=_rf_toggle_enable,
            poll_interval_ms=1000,
            rampdown_interval_ms=50,
            direct_mode=True, # ★ Plasma Cleaning에서는 DC처럼 즉시 ON/OFF
            write_inv_a=1.74,      # ← 보정 스케일 적용
            write_inv_b=0.0,      # ← 오프셋(기본 0)
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
            w_start.clicked.connect(lambda: asyncio.create_task(self._on_click_start()))
        if w_stop:
            w_stop.clicked.connect(lambda: asyncio.create_task(self._on_click_stop()))
        if w_proc:
            w_proc.clicked.connect(lambda: asyncio.create_task(self._handle_process_list_clicked_async()))

        # 4) 중복 연결 방지 플래그
        self._buttons_connected = True

    async def _on_click_start(self) -> None:
        self._cleanup_started = False  # ★ 추가: 새 런마다 정리 가드 초기화

        # start 버튼 중복 클릭 방지
        if getattr(self, "_running", False):
            self._post_warning("실행 중", "이미 Plasma Cleaning이 실행 중입니다.")
            return

        ch = int(getattr(self, "_selected_ch", 1))

        # 1) 쿨다운 교차 검사 (PC 전역 + 해당 챔버)
        ok_cool, remain, _ = runtime_state.pc_block_reason(ch, cooldown_s=60.0)
        if not ok_cool:
            secs = int(float(remain) + 0.999)
            # ✔ 챔버 공정과 동일한 문구/형식
            self._post_warning("대기 필요", f"이전 공정 종료 후 1분 대기 필요합니다.\n{secs}초 후에 시작하십시오.")
            # ✔ 로그 출력 제거(요청사항)
            return

        # 2) 교차 실행 차단
        # 2-1) 같은 CH의 PC가 이미 실행 중이면 금지
        if runtime_state.is_running("pc", ch):
            self._post_warning("실행 오류", f"CH{ch} Plasma Cleaning이 이미 실행 중입니다.")
            return

        # 2-2) 같은 CH의 Chamber 공정도 실행 중이면 금지
        if runtime_state.is_running("chamber", ch):
            self._post_warning("실행 오류", f"CH{ch}는 이미 다른 공정이 실행 중입니다.")
            return
        
        # 3) 프리플라이트 (성공하면 계속)
        try:
            # 1) 사전 연결 점검
            await self._preflight_connect(timeout_s=10.0)
        except Exception as e:
            # PLC, MFC, IG 등 연결 실패 시 사용자 알림만 표시하고 중단
            self._post_warning("연결 실패", f"장치 연결에 실패했습니다.\n\n{e}")
            return

        # 4) 실행/시작 마킹 + 동시실행 가드(대칭성 보장)
        runtime_state.mark_started("pc", ch)
        runtime_state.set_running("pc", True, ch)
        runtime_state.mark_started("chamber", ch)
        runtime_state.set_running("chamber", True, ch)

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


            lr = str(getattr(self.pc, "last_result", "") or "").strip().lower()   # "success" | "fail" | "stop"
            ls = str(getattr(self.pc, "last_reason", "") or "").strip()

            # 사용자가 STOP을 눌렀거나 컨트롤러가 'stop'을 준 경우를 모두 STOP으로 간주
            stopped_final = bool(self._stop_requested or lr == "stop")

            if exc_reason:
                ok_final = False
                final_reason = exc_reason or ls or "runtime/controller error"
            else:
                ok_final = (lr == "success")
                final_reason = (None if ok_final else (ls or "runtime/controller error"))

            # ✅ 최종 보정: 컨트롤러가 success라고 했으면 사용자 STOP/실패로 뒤집히지 않게 보정
            if (lr == "success") and (not self._stop_requested):
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
            # [A] 🔁 순서 변경: 종료 통지 먼저 (runtime_state 즉시 해제 + 종료 챗 선송)
            self.append_log("MAIN", "[FINALLY] notify_finish_once 진입")
            try:
                await self._notify_finish_once(ok=ok_final, reason=final_reason, stopped=stopped_final)  # ← 순서 ↑
            except Exception as e:
                self.append_log("PC", f"notify_finish_once error: {e!r}")

            # [B] 그 다음 장치/태스크 정리 (오래 걸려도 상관없음)
            self.append_log("MAIN", "[FINALLY] final_cleanup 진입")
            await self._final_cleanup()

            # [C] 마지막으로 UI 복구
            self._running = False
            self._process_timer_active = False
            self._reset_ui_state(restore_time_min=self._last_process_time_min)
            self._set_state_text("대기 중")
            self.append_log("MAIN", "[FINALLY] idle UI 복구 완료")

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

    async def _safe_rf_stop(self) -> None:
        # ▶ 방어: 어떤 경로로 불려도 카운트다운 표시는 종료
        self._process_timer_active = False

        if not self.rf:
            return
        # 1) ramp-down 시작
        try:
            await asyncio.wait_for(self.rf.cleanup(), timeout=5.0)
        except asyncio.TimeoutError:
            self.append_log("RF", "cleanup timeout → 계속 진행")

        # 2) ramp-down 완료 신호 대기
        ok = False
        try:
            ok = await self.rf.wait_power_off(timeout_s=15.0)
        except Exception as e:
            self.append_log("RF", f"wait_power_off error: {e!r}")

        # 3) 실패 시 강제 종료(failsafe)
        if not ok and self.plc:
            self.append_log("RF", "ramp-down 완료 신호 timeout → 강제 0W/SET OFF")
            with contextlib.suppress(Exception):
                await self.plc.power_write(0.0, family="DCV", write_idx=1)
            with contextlib.suppress(Exception):
                await self.plc.power_enable(False, family="DCV", set_idx=1)

        # 4) RF 완전 종료 → 나머지 중단 공정 실행
        await self._shutdown_rest_devices()        # ← 이제 정의 추가(아래)

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
            # (B) MFC 폴링/자동재연결 명시 중단
            with contextlib.suppress(Exception):
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
            self._cleanup_started = False
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
                self._w_log.appendPlainText(line)
                self._w_log.moveCursor(QTextCursor.End)
            except Exception:
                pass
        # 파일
        try:
            if getattr(self, "_log_fp", None):
                self._log_fp.write(line + "\n")
                self._log_fp.flush()
        except Exception:
            # 파일 오류가 난다고 공정을 멈출 필요는 없음 — 조용히 무시
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
        self._bg_tasks.clear()
        self._event_tasks.clear()

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
        fp = getattr(self, "_log_fp", None)
        self._log_fp = None
        if fp:
            try:
                fp.write("# ==== END ====\n")
                fp.flush()
                fp.close()
            except Exception:
                pass

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
            runtime_state.mark_finished("pc", ch)
            runtime_state.mark_finished("chamber", ch)
            runtime_state.set_running("pc", False, ch)
            runtime_state.set_running("chamber", False, ch)
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

    def _post_warning(self, title: str, text: str) -> None:
        try:
            # UI/부모 준비 확인
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
            box.setWindowModality(Qt.WindowModality.WindowModal)        # 챔버와 동일
            box.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True) # 챔버와 동일

            # 참조 유지 & 닫힐 때 정리
            self._msg_boxes.append(box)
            def _cleanup(_res: int):
                with contextlib.suppress(ValueError):
                    self._msg_boxes.remove(box)
                box.deleteLater()
            box.finished.connect(_cleanup)

            box.open()  # 비모달로 열어도 WindowModal이라 UX 동일
        except Exception as e:
            # 실패 시에도 기존처럼 로그는 남김 (원인 추적용으로 예외메시지 덧붙임)
            self.append_log("PC", f"[경고] {title}: {text} ({e!s})")

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
            try:
                self.set_selected_ch(int(float(use_ch)))
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
        file_path = await self._aopen_file(
            caption="Plasma Cleaning 레시피(CSV) 선택",
            name_filter="CSV Files (*.csv);;All Files (*)"
        )
        if not file_path:
            self.append_log("File", "파일 선택 취소")
            return

        row = self._read_first_row_from_csv(file_path)
        if not row:
            self._post_warning("CSV 오류", "데이터 행이 없습니다.")
            return

        self._apply_recipe_row_to_ui(row)
        self.append_log("File", f"CSV 로드 완료: {file_path}\n→ UI에 값 세팅")

    async def _aopen_file(self, caption="파일 선택", start_dir="", name_filter="All Files (*)") -> str:
        """Qt 비동기 파일 열기 다이얼로그 (QFileDialog). 취소 시 빈 문자열."""
        if not self._has_ui():
            return ""
        from PySide6.QtWidgets import QFileDialog, QDialog
        dlg = QFileDialog(self._parent_widget() or None, caption, start_dir, name_filter)
        dlg.setFileMode(QFileDialog.ExistingFile)

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[str] = loop.create_future()

        def _done(result: int):
            try:
                if result == QDialog.Accepted and dlg.selectedFiles():
                    fut.set_result(dlg.selectedFiles()[0])
                else:
                    fut.set_result("")
            finally:
                dlg.deleteLater()

        dlg.finished.connect(_done)
        dlg.open()
        return await fut
    
    # ======= 서버 통신을 통한 실행 api =======
    async def start_with_recipe_string(self, recipe: str) -> None:
        """
        외부 제어(Host) 진입점.
        - recipe == "" 또는 None: 현재 UI 값으로 단발 실행(버튼 클릭과 동일)
        - recipe 가 .csv 경로: 첫 데이터 행으로 UI 갱신 후 기존 Start 경로 실행
        """
        s = (recipe or "").strip()
        if not s:
            # 현재 UI 값으로 버튼 클릭과 동일하게 실행
            await self._on_click_start()
            return

        if s.lower().endswith(".csv"):
            if not os.path.exists(s):
                raise RuntimeError(f"CSV 파일을 찾을 수 없습니다: {s}")

            row = self._read_first_row_from_csv(s)
            if not row:
                raise RuntimeError("CSV에 데이터 행이 없습니다.")

            # CSV → UI 세팅 (use_ch 있으면 set_selected_ch까지 내부 적용)
            self._apply_recipe_row_to_ui(row)
            self.append_log("File", f"CSV 로드 완료: {s} → UI에 값 세팅")

            # 기존 Start 경로로 실행 (쿨다운/프리플라이트/로깅/종료 처리 모두 기존대로)
            await self._on_click_start()
            return

        raise RuntimeError("지원하지 않는 레시피 형식입니다. CSV 경로만 허용됩니다.")

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



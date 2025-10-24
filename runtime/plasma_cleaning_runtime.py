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

# 플라즈마 클리닝 ‘전용’ UI→장치 전달 보정(채널별)
_PC_FLOW_UI_SCALE = {1: 1.0, 2: 0.1, 3: 0.1}  # ← PC에서만 적용

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
        self._log_dir = log_dir            # 기본 로그 루트
        self._pc_log_dir = (log_dir / "plasma_cleaning")
        self._pc_log_dir.mkdir(parents=True, exist_ok=True)

        self._log_fp = None                # 현재 런 세션 로그 파일 핸들
        self._log_session_id = None        # 파일명에 들어갈 세션 ID (timestamp)

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

        # RF 파워(연속) 바인딩
        self.rf = self._make_rf_async()

        # PlasmaCleaningController 바인딩
        self.pc = self._bind_pc_controller()

        # UI 버튼 연결
        self._connect_ui_buttons()

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

        def _has_task(name: str) -> bool:
            return any(getattr(t, "get_name", lambda: "")() == name for t in self._event_tasks)

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
            scale = _PC_FLOW_UI_SCALE.get(gi, 1.0)
            self.append_log("PC", f"GasFlow → MFC1 ch{gi} (PC scale x{scale:g})")
            await self.mfc_gas.gas_select(gi)  # MFC 내부 '선택 채널' 갱신

        async def _mfc_flow_set_on(flow_sccm: float) -> None:
            mfc = self.mfc_gas
            if not mfc:
                raise RuntimeError("mfc_gas not bound")
            # ← gas_idx는 직전에 _mfc_gas_select에서 self._pc_gas_idx로 저장됨
            ch   = getattr(self, "_pc_gas_idx", 3)
            req  = float(max(0.0, flow_sccm))
            ui   = req * float(_PC_FLOW_UI_SCALE.get(ch, 1.0))  # PC 전용 보정

            self.append_log("MFC", f"FLOW_SET_ON(sel ch={ch}) -> {ui:.1f} sccm (PC only, req {req:.1f})")
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
            await self.rf.start_process(float(power_w))

            # ★ 목표 도달 이벤트 기다림 (타임아웃은 취향껏: 60s 예시)
            self._rf_target_evt.clear()
            try:
                await asyncio.wait_for(self._rf_target_evt.wait(), timeout=180.0)
                self.append_log("RF", "목표 파워 안정 → 프로세스 타이머 시작 가능")
                # ▶ 이제부터만 ProcessTime_edit에 카운트다운을 표시
                self._process_timer_active = True
            except asyncio.TimeoutError:
                self.append_log("RF", "목표 파워 미도달(180s timeout) → 중단")
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
            # mm:ss 형식(1분 미만이면 ss만)으로 tail 구성
            try:
                sec = int(sec)
            except Exception:
                sec = 0
            mm, ss = divmod(max(sec, 0), 60)
            tail = f"{mm:02d}:{ss:02d}" if mm else f"{ss:02d}s"

            # 1) 상단 상태창: "제목 · mm:ss"
            if self._state_header:
                self._set_state_text(f"{self._state_header} · {tail}")
            else:
                self._set_state_text(tail)

            # 2) Process Time 칸 표기는 '공정 카운트다운'일 때만
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
            chat_notifier=self.chat,
        )

    def _make_rf_async(self) -> Optional[RFPowerAsync]:
        if not self.plc:
            return None

        async def _rf_send(power_w: float) -> None:
            # SET 보장 + 목표 W 쓰기 (DCV_SET_1, DCV_WRITE_1)
            await self.plc.rf_apply(float(power_w), ensure_set=True)
            try:
                meas = await self.plc.rf_read_fwd_ref()  # ← 보정 적용
                self.append_log("PLC", f"RF READ FWD={meas['forward']:.1f} W, REF={meas['reflected']:.1f} W")
            except Exception as e:
                self.append_log("PLC", f"RF READ 실패: {e!r}")

        async def _rf_send_unverified(power_w: float) -> None:
            await self.plc.power_write(power_w, family="DCV", write_idx=1)  # no-reply 경로
            try:
                meas = await self.plc.rf_read_fwd_ref()
                self.append_log("PLC", f"RF READ(FB) FWD={meas['forward']:.1f} W, REF={meas['reflected']:.1f} W")
            except Exception:
                pass

        async def _rf_request_read():
            try:
                return await self.plc.rf_read_fwd_ref()  # ← 핵심 수정
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
        # 1) 파라미터 수집
        p = self._read_params_from_ui()
        self._last_process_time_min = float(p.process_time_min)

        # 1.2) 실행 상태로 버튼 전환 + 상태 텍스트 (중복 클릭 방지)
        self._running = True
        self._set_running_ui_state()
        self._set_state_text("Preparing…")  # 선택

        # 1.5) 파일 로그 오픈
        self._open_run_log(p)
        self.append_log("PC", "파일 로그 시작")

        # 2) 프리플라이트 — 실패 시 UI/파일 복구 후 종료
        try:
            await self._preflight_connect(timeout_s=10.0)
        except Exception as e:
            self.append_log("PC", f"오류: {e!r}")
            self._running = False
            try:
                self._close_run_log()
            except Exception:
                pass
            # 시작 시 분 값으로 복원
            self._reset_ui_state(restore_time_min=self._last_process_time_min)
            return

        # 4) 컨트롤러 실행
        success = False
        try:
            await self.pc._run(p)   # Stop을 누르면 내부에서 안전 종료
            success = True
        except Exception as e:
            self.append_log("PC", f"오류: {e!r}")
        finally:
            self._running = False
            # ▶ 공정 종료 후 초기 UI 복귀
            self._reset_ui_state(restore_time_min=self._last_process_time_min)
            self._set_state_text("IDLE")
            self.append_log("PC", "파일 로그 종료")
            self._close_run_log()

    async def _on_click_stop(self) -> None:
        # 1) 컨트롤러 루프 중단 요청
        with contextlib.suppress(Exception):
            if hasattr(self, "pc") and self.pc:
                self.pc.request_stop()

        # 2) RF 정리(즉시 감압 필요 시)
        await self._safe_rf_stop()
            
        # ← 이벤트 펌프도 끄기 (옵션)
        for t in getattr(self, "_event_tasks", []):
            t.cancel()

        self._running = False
        self._process_timer_active = False     # ▶ 공정 타이머 모드 해제
        self._set_state_text("STOPPED")
        self.append_log("PC", "사용자에 의해 중지")

        # ★ 선택: 즉시 파일 닫기(컨트롤러가 이미 끝나지 않아도 파일은 닫혀서 flush됨)
        with contextlib.suppress(Exception):
            self._close_run_log()

        # ▶ STOP 후에도 챔버 공정처럼 UI를 초깃값으로 복구
        self._reset_ui_state(restore_time_min=self._last_process_time_min)

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
    def _set_running_ui_state(self) -> None:
        """공정 실행 중 UI 상태 (Start 비활성, Stop 활성)"""
        with contextlib.suppress(Exception):
            w_start = _find_first(self.ui, [
                f"{self.prefix}Start_button", f"{self.prefix}StartButton",
                f"{self.prefix.lower()}Start_button", f"{self.prefix.lower()}StartButton",
                "PC_Start_button", "pcStart_button",
            ])
            if w_start and hasattr(w_start, "setEnabled"):
                w_start.setEnabled(False)

            w_stop = _find_first(self.ui, [
                f"{self.prefix}Stop_button", f"{self.prefix}StopButton",
                f"{self.prefix.lower()}Stop_button", f"{self.prefix.lower()}StopButton",
                "PC_Stop_button", "pcStop_button",
            ])
            if w_stop and hasattr(w_stop, "setEnabled"):
                w_stop.setEnabled(True)

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

        # 3) 게이트밸브 닫기(있으면)
        with contextlib.suppress(Exception):
            if self.plc:
                self.append_log("STEP", f"종료: GateValve CH{self._selected_ch} CLOSE")
                await self.plc.gate_valve(self._selected_ch, open=False, momentary=True)
                # (선택) 표시램프가 꺼질 때까지 짧게 대기
                with contextlib.suppress(Exception):
                    for _ in range(10):  # 최대 ~2초
                        lamp = await self.plc.read_bit(f"G_V_{self._selected_ch}_OPEN_LAMP")
                        if not lamp:
                            break
                        await asyncio.sleep(0.2)

        # 4) (선택) 이벤트 펌프 태스크 정리
        for t in getattr(self, "_event_tasks", []):
            t.cancel()

        # 5) (신규) 선택 장치만 연결 해제 — PLC 제외
        await self._disconnect_selected_devices()

    async def _disconnect_selected_devices(self) -> None:
        """
        Plasma Cleaning 종료 시 선택 장치만 연결 해제:
        - CH1:  MFC1(gas/pressure 동일 가능) + IG1
        - CH2:  MFC2(pressure) + IG2    (MFC1(gas)와 PLC는 유지)
        """
        try:
            # 1) 먼저 MFC
            if int(getattr(self, "_selected_ch", 0)) == 1:
                # gas/pressure가 동일 인스턴스일 수 있어 중복 제거
                mfc_set = {m for m in (self.mfc_gas, self.mfc_pressure) if m}
                for m in mfc_set:
                    self.append_log("MFC", "CH1 종료: MFC 연결 해제")
                    # 방어적 타임아웃
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(m.cleanup(), timeout=3.0)
            else:
                # CH2: pressure=MFC2만 해제, gas=MFC1은 유지
                if self.mfc_pressure:
                    self.append_log("MFC", "CH2 종료: Pressure MFC 연결 해제")
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(self.mfc_pressure.cleanup(), timeout=3.0)

            # 2) IG (각 CH에 해당하는 IG 인스턴스가 self.ig로 바인딩되어 있음)
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
                        f"time={p.process_time_min:.1f} min\n")
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

        # 상태 텍스트
        self._set_state_text("IDLE")

        # ProcessTime 복원
        if restore_time_min is not None:
            with contextlib.suppress(Exception):
                w = getattr(self.ui, "PC_ProcessTime_edit", None)
                if w and hasattr(w, "setPlainText"):
                    w.setPlainText(f"{float(restore_time_min):.1f}")

        # FWD/REF 표시칸 초기화
        with contextlib.suppress(Exception):
            for_w = getattr(self.ui, "PC_forP_edit", None)
            ref_w = getattr(self.ui, "PC_refP_edit", None)
            if for_w and hasattr(for_w, "setPlainText"):
                for_w.setPlainText("")
            if ref_w and hasattr(ref_w, "setPlainText"):
                ref_w.setPlainText("")

        # 버튼 상태 복원 (Start 가능, Stop 불가)
        with contextlib.suppress(Exception):
            w_start = _find_first(self.ui, [
                f"{self.prefix}Start_button", f"{self.prefix}StartButton",
                f"{self.prefix.lower()}Start_button", f"{self.prefix.lower()}StartButton",
                "PC_Start_button", "pcStart_button",
            ])
            if w_start and hasattr(w_start, "setEnabled"):
                w_start.setEnabled(True)

            w_stop = _find_first(self.ui, [
                f"{self.prefix}Stop_button", f"{self.prefix}StopButton",
                f"{self.prefix.lower()}Stop_button", f"{self.prefix.lower()}StopButton",
                "PC_Stop_button", "pcStop_button",
            ])
            if w_stop and hasattr(w_stop, "setEnabled"):
                w_stop.setEnabled(False)

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



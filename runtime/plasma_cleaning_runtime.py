# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
from typing import Any, Optional, Mapping, TYPE_CHECKING, TypeAlias

from PySide6.QtWidgets import QPlainTextEdit, QMessageBox
from PySide6.QtGui import QTextCursor

from controller.plasma_cleaning_controller import (
    PlasmaCleaningController,
    PlasmaCleaningParams,
    PCleanEvent,
)

# ===== 타입 힌트(선택, 런타임 안전) =====
if TYPE_CHECKING:
    from device.mfc import AsyncMFC
    from device.ig import AsyncIG
    from device.plc import AsyncPLC
    from device.rf_power import RFPowerAsync
else:
    AsyncMFC: TypeAlias = Any
    AsyncIG: TypeAlias = Any
    AsyncPLC: TypeAlias = Any
    RFPowerAsync: TypeAlias = Any


class PlasmaCleaningRuntime:
    """
    Plasma Cleaning 런타임

    - 라디오 선택(CH1/CH2)에 따라 IG/MFC를 스위칭.
    - 가스 유량(FLOW_*)은 항상 '공용 MFC1 ch=3'만 사용.
    - 선택된 챔버의 MFC에는 SP4_SET/ SP4_ON/ VALVE_OPEN만 보냄.
    - 컨트롤러는 mfc_handle 라우터를 통해 올바른 장비로 명령을 전송.
    """

    def __init__(
        self,
        *,
        ui: Any,
        prefix: str,
        loop: asyncio.AbstractEventLoop,
        plc: Any,
        # --- 호환성: 예전 main.py가 mfc=만 넘겨도 동작하도록 ---
        mfc: Optional[Any] = None,           # (legacy) 단일 MFC
        # --- 권장: 명확한 라우팅용 핸들들 ---
        mfc_common: Optional[AsyncMFC] = None,   # 공용 MFC1 (FLOW_* 전용, ch=3 사용)
        mfc_ch1: Optional[AsyncMFC] = None,      # CH1용 MFC (SP4_* / VALVE_OPEN)
        mfc_ch2: Optional[AsyncMFC] = None,      # CH2용 MFC (SP4_* / VALVE_OPEN)
        ig_ch1: Optional[AsyncIG] = None,        # CH1용 IG
        ig_ch2: Optional[AsyncIG] = None,        # CH2용 IG
        rf_clean: Optional[RFPowerAsync] = None,
        chat: Optional[Any] = None,
        gv_switch_by_ch: Mapping[int, str] | None = None,  # 예) {1: "GV1_OPEN_SW", 2: "GV2_OPEN_SW"}
    ) -> None:
        self.ui = ui
        self.prefix = prefix or ""  # UI 위젯 접두사
        self._loop = loop

        # PLC
        self.plc: AsyncPLC = plc

        # --- MFC 라우팅 소스 보관 ---
        # 공용(MFC1) — FLOW_* (ch=3) 전용
        self.mfc_common: Optional[AsyncMFC] = mfc_common or mfc

        # 챔버별 MFC — SP4_* / VALVE_OPEN 전용
        self.mfc_by_ch: dict[int, Optional[AsyncMFC]] = {
            1: mfc_ch1 or mfc,  # legacy 환경: mfc 하나만 들어오면 CH1에 매핑
            2: mfc_ch2,         # 없으면 None → CH2 선택 시 오류 안내
        }

        # 챔버별 IG
        self.ig_by_ch: dict[int, Optional[AsyncIG]] = {
            1: ig_ch1,
            2: ig_ch2,
        }

        self.chat = chat

        # RF 파워 핸들: 주입 없으면 PLC 콜백으로 구성 시도
        self.rf: Optional[RFPowerAsync] = rf_clean or self._build_rf_with_plc_callbacks(self.plc)

        # GV 키 매핑(기본값 제공)
        self.gv_switch_by_ch: dict[int, str] = dict(gv_switch_by_ch or {}) or {
            1: "GV1_OPEN_SW",
            2: "GV2_OPEN_SW",
        }

        # 컨트롤러 구성: mfc_handle 라우터 + IG/PLC/RF 핸들
        self.controller = PlasmaCleaningController(
            loop=self._loop,
            mfc_handle=self._mfc_router_handle,  # ← 라우터
            ig=self._current_ig(),               # ← 시작 시점 IG (Start 직전에 다시 주입)
            plc_write_switch=getattr(self.plc, "write_switch", None),
            rf_start=(self.rf.start_process if (self.rf and hasattr(self.rf, "start_process")) else None),
            rf_stop=(self.rf.cleanup if (self.rf and hasattr(self.rf, "cleanup")) else None),
            gv_switch_by_ch=self.gv_switch_by_ch,
        )

        # 상태
        self._bg_tasks: list[asyncio.Task] = []
        self._pump_started = False

        # UI 포인터
        self._w_log: Optional[QPlainTextEdit] = self._u("pc_logMessage_edit")

        # 버튼 바인딩
        self._connect_buttons()

        # 이벤트 펌프 시작
        self._ensure_event_pump()

    # ──────────────────────────────────────────────
    # UI / 버튼
    # ──────────────────────────────────────────────
    def _u(self, leaf: str) -> Any | None:
        if not getattr(self, "ui", None):
            return None
        return getattr(self.ui, f"{self.prefix}{leaf}", None)

    def _connect_buttons(self) -> None:
        btn = self._u("PC_Start_button")
        if btn:
            btn.clicked.connect(self._handle_start_clicked)

        btn = self._u("PC_Stop_button")
        if btn:
            btn.clicked.connect(self._handle_stop_clicked)

        if self._w_log:
            self._w_log.setMaximumBlockCount(2000)

    def _ensure_event_pump(self) -> None:
        if self._pump_started:
            return
        self._pump_started = True
        t = self._loop.create_task(self._pump_events(), name="PlasmaClean.Pump")
        self._bg_tasks.append(t)

    async def _pump_events(self) -> None:
        async for ev in self.controller.events():
            try:
                if ev.kind in ("status", "log"):
                    if ev.message:
                        self._append_log(ev.message)
                elif ev.kind == "progress":
                    left = int((ev.payload or {}).get("seconds_left", 0))
                    mm, ss = divmod(max(0, left), 60)
                    w = self._u("PC_ProcessTime_edit")
                    if w and hasattr(w, "setPlainText"):
                        w.setPlainText(f"{mm:02d}:{ss:02d}")
                elif ev.kind == "started":
                    self._append_log("▶ Load-Lock 플라즈마 클리닝 시작")
                elif ev.kind == "finished":
                    self._append_log("✅ Load-Lock 플라즈마 클리닝 정상 종료")
                elif ev.kind == "failed":
                    self._append_log(f"❌ 실패: {ev.message}")
            except Exception as e:
                self._append_log(f"[Pump] 예외: {e!r}")

    # ──────────────────────────────────────────────
    # 라디오 선택/장비 선택
    # ──────────────────────────────────────────────
    def _selected_ch(self) -> int:
        r1 = self._u("PC_useChamber1_radio")
        r2 = self._u("PC_useChamber2_radio")
        # 기본 CH1
        if r2 and getattr(r2, "isChecked", None) and r2.isChecked():
            return 2
        return 1

    def _current_ig(self) -> Optional[AsyncIG]:
        ch = self._selected_ch()
        ig = self.ig_by_ch.get(ch)
        if ig is not None:
            return ig
        # 둘 다 없으면(호환) — 단일 IG를 내부 생성해도 되지만, 여기선 None 그대로 반환
        return None

    # ──────────────────────────────────────────────
    # 버튼 핸들러 / 입력 검증
    # ──────────────────────────────────────────────
    def _handle_start_clicked(self, _checked: bool = False):
        params = self._read_params_from_ui_and_validate()
        if not params:
            return

        # 선택된 챔버 반영
        params.target_chamber = self._selected_ch()
        # 컨트롤러에 현재 IG 주입(선택 라디오 기준)
        self.controller.ig = self._current_ig()

        async def run():
            # 필요한 장비 연결 시도(있을 때만)
            await self._connect_required_devices(timeout_s=4.0)

            # 필수 장비 체크
            if not self._devices_ready_or_alert():
                return

            # 시작
            self.controller.start(params)

        self._bg_tasks.append(self._loop.create_task(run(), name="PC.Start"))

    def _handle_stop_clicked(self, _checked: bool = False):
        self.controller.request_stop()

    def _read_params_from_ui_and_validate(self) -> Optional[PlasmaCleaningParams]:
        def gtxt(name: str) -> str:
            w = self._u(name)
            return (w.toPlainText().strip() if w and hasattr(w, "toPlainText") else "")

        errs: list[str] = []

        # Target Pressure (지수표기 1e-6 형태 허용)
        tp_s = gtxt("PC_targetPressure_edit")
        target_pressure = None
        if not tp_s:
            errs.append("Target Pressure가 비어 있습니다.")
        else:
            try:
                target_pressure = float(tp_s.strip())  # '1e-6' 그대로 OK
                if target_pressure <= 0:
                    errs.append("Target Pressure는 0보다 커야 합니다.")
            except Exception:
                errs.append("Target Pressure 형식이 올바르지 않습니다(예: 1e-6).")

        # Gas Flow (N2 sccm, MFC1 ch3)
        gf_s = gtxt("PC_gasFlow_edit")
        gas_flow = None
        if not gf_s:
            errs.append("Gas Flow(N2 sccm)이 비어 있습니다.")
        else:
            try:
                gas_flow = float(gf_s)
                if gas_flow <= 0:
                    errs.append("Gas Flow(N2)는 0보다 커야 합니다.")
            except Exception:
                errs.append("Gas Flow(N2) 형식이 올바르지 않습니다(숫자).")

        # Working Pressure (SP4_SET 값)
        wp_s = gtxt("PC_workingPressure_edit")
        working_pressure = None
        if not wp_s:
            errs.append("Working Pressure(SP4 Set)가 비어 있습니다.")
        else:
            try:
                working_pressure = float(wp_s)
                if working_pressure <= 0:
                    errs.append("Working Pressure는 0보다 커야 합니다.")
            except Exception:
                errs.append("Working Pressure 형식이 올바르지 않습니다(숫자).")

        # RF Power
        rf_s = gtxt("PC_rfPower_edit")
        rf_power = None
        if not rf_s:
            errs.append("RF Power가 비어 있습니다.")
        else:
            try:
                rf_power = float(rf_s)
                if rf_power <= 0:
                    errs.append("RF Power는 0보다 커야 합니다.")
            except Exception:
                errs.append("RF Power 형식이 올바르지 않습니다(숫자).")

        # Process Time (분 → 초)
        pt_s = gtxt("PC_ProcessTime_edit")
        duration_sec = None
        if not pt_s:
            errs.append("Process Time(분)이 비어 있습니다.")
        else:
            try:
                minutes = float(pt_s)
                if minutes <= 0:
                    errs.append("Process Time(분)은 0보다 커야 합니다.")
                else:
                    duration_sec = minutes * 60.0
            except Exception:
                errs.append("Process Time 형식이 올바르지 않습니다(숫자).")

        if errs:
            try:
                QMessageBox.warning(None, "입력 오류", "\n".join(f"• {e}" for e in errs))
            except Exception:
                pass
            return None

        return PlasmaCleaningParams(
            target_pressure=float(target_pressure),
            gas_flow_sccm=float(gas_flow),
            working_pressure=float(working_pressure),
            rf_power_w=float(rf_power),
            duration_sec=float(duration_sec),
            target_chamber=None,  # 선택은 Start 시 주입
        )

    # ──────────────────────────────────────────────
    # 장치 연결/점검
    # ──────────────────────────────────────────────
    async def _connect_required_devices(self, *, timeout_s: float = 3.0) -> None:
        tasks = []

        # 공용 MFC (FLOW_* 전용)
        if self.mfc_common and not self._is_dev_connected(self.mfc_common):
            tasks.append(self._try_connect_device("MFC(Common)", self.mfc_common, timeout_s))

        # 선택 챔버 MFC (SP4_* 전용)
        sel_mfc = self.mfc_by_ch.get(self._selected_ch())
        if sel_mfc and not self._is_dev_connected(sel_mfc):
            tasks.append(self._try_connect_device(f"MFC(CH{self._selected_ch()})", sel_mfc, timeout_s))

        # 선택 챔버 IG
        ig = self._current_ig()
        if ig and not self._is_dev_connected(ig):
            tasks.append(self._try_connect_device(f"IG(CH{self._selected_ch()})", ig, timeout_s))

        # PLC
        if self.plc and not self._is_dev_connected(self.plc):
            tasks.append(self._try_connect_device("PLC", self.plc, timeout_s))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _try_connect_device(self, name: str, dev: object, timeout_s: float) -> None:
        try:
            if self._is_dev_connected(dev):
                return
            started = await self._dev_start_or_connect(dev)
            if not started:
                self._append_log(f"[{name}] start/connect 진입 불가(메서드 없음)")
                return
            deadline = asyncio.get_event_loop().time() + float(timeout_s)
            while asyncio.get_event_loop().time() < deadline:
                if self._is_dev_connected(dev):
                    self._append_log(f"[{name}] 연결 완료")
                    return
                await asyncio.sleep(0.2)
            self._append_log(f"[{name}] 연결 대기 시간 초과({timeout_s:.1f}s)")
        except Exception as e:
            self._append_log(f"[{name}] 연결 시도 실패: {e!r}")

    async def _dev_start_or_connect(self, dev: object) -> bool:
        for method_name in ("start", "connect"):
            fn = getattr(dev, method_name, None)
            if callable(fn):
                res = fn()
                if asyncio.iscoroutine(res):
                    await res
                return True
        return False

    def _devices_ready_or_alert(self) -> bool:
        missing: list[str] = []

        # IG
        ig = self._current_ig()
        if not (ig and self._is_dev_connected(ig)):
            missing.append("IG")

        # 공용 MFC
        if not (self.mfc_common and self._is_dev_connected(self.mfc_common)):
            missing.append("MFC(Common ch3)")

        # 선택 챔버 MFC
        sel_mfc = self.mfc_by_ch.get(self._selected_ch())
        if not (sel_mfc and self._is_dev_connected(sel_mfc)):
            missing.append(f"MFC(CH{self._selected_ch()})")

        # PLC
        if not (self.plc and self._is_dev_connected(self.plc)):
            missing.append("PLC")

        if missing:
            msg = "다음 장비가 연결되지 않았습니다. 연결 후 다시 시작해 주세요.\n\n" + \
                  "\n".join(f"• {name}" for name in missing)
            try:
                QMessageBox.critical(None, "장비 연결 오류", msg)
            except Exception:
                pass
            self._append_log(f"❌ 공정 시작 취소 — 미연결: {', '.join(missing)}")
            return False
        return True

    # ──────────────────────────────────────────────
    # RFPowerAsync 구성(PLC 콜백)
    # ──────────────────────────────────────────────
    def _build_rf_with_plc_callbacks(self, plc: AsyncPLC) -> Optional[RFPowerAsync]:
        if plc is None:
            return None

        async def send_rf_power(power_w: float):
            await plc.power_apply(float(power_w), ensure_set=True)

        async def send_rf_power_unverified(power_w: float):
            await plc.power_write(float(power_w), ensure_set=False)

        async def request_status_read():
            return await plc.power_read()

        async def toggle_enable(on: bool):
            await plc.power_enable(bool(on), set_idx=1)

        try:
            from device.rf_power import RFPowerAsync as _RF
            return _RF(
                send_rf_power=send_rf_power,
                send_rf_power_unverified=send_rf_power_unverified,
                request_status_read=request_status_read,
                toggle_enable=toggle_enable,
            )
        except Exception:
            return None

    # ──────────────────────────────────────────────
    # MFC 라우터
    # ──────────────────────────────────────────────
    async def _invoke_mfc(self, mfc: Any, cmd: str, args: Optional[dict]) -> None:
        """
        mfc 인스턴스의 핸들러를 찾아 호출:
        - handle_command / apply_command / command 중 하나
        """
        if mfc is None:
            raise RuntimeError("MFC 핸들이 없습니다.")
        for name in ("handle_command", "apply_command", "command"):
            fn = getattr(mfc, name, None)
            if callable(fn):
                res = fn(cmd, *((),), **(args or {})) if name == "command" else fn(cmd, args)
                if asyncio.iscoroutine(res):
                    await res
                return
        raise RuntimeError("MFC에 호출 가능한 명령 핸들러가 없습니다.")

    async def _mfc_router_handle(self, cmd: str, args: Optional[dict]) -> None:
        """
        컨트롤러 → 이 라우터 → 알맞은 MFC로 분기
        - FLOW_*: 공용 MFC1 로 전달 (ch=3 사용)
        - SP4_SET / SP4_ON / VALVE_OPEN: 선택 챔버의 MFC로 전달
        """
        key = (cmd or "").strip().upper()
        if key in ("FLOW_SET", "FLOW_ON", "FLOW_OFF"):
            # 공용 MFC1
            if key == "FLOW_SET":
                # channel 인자는 무시하고 ch=3으로 강제
                v = float((args or {}).get("value", 0.0))
                await self._invoke_mfc(self.mfc_common, "FLOW_SET", {"channel": 3, "value": v})
                return
            else:
                await self._invoke_mfc(self.mfc_common, key, {"channel": 3})
                return

        elif key in ("SP4_SET", "SP4_ON", "VALVE_OPEN"):
            ch = self._selected_ch()
            mfc_sel = self.mfc_by_ch.get(ch)
            if mfc_sel is None:
                raise RuntimeError(f"선택된 챔버 CH{ch}의 MFC 핸들이 없습니다.")
            if key == "SP4_SET":
                v = float((args or {}).get("value", 0.0))
                await self._invoke_mfc(mfc_sel, "SP4_SET", {"value": v})
                return
            else:
                await self._invoke_mfc(mfc_sel, key, None)
                return

        else:
            raise RuntimeError(f"지원하지 않는 MFC 명령: {cmd!r}")

    # ──────────────────────────────────────────────
    # 종료/로그/유틸
    # ──────────────────────────────────────────────
    def shutdown_fast(self) -> None:
        for t in list(self._bg_tasks):
            if t and not t.done():
                t.cancel()
        self._bg_tasks.clear()

    def _append_log(self, line: str) -> None:
        w = self._w_log
        if not w:
            return
        try:
            w.moveCursor(QTextCursor.MoveOperation.End)
            w.insertPlainText(line + "\n")
        except Exception:
            pass

    # 메인에서 호출하는 퍼블릭 로그 API(소스 태그 포함)
    def append_log(self, source: str, msg: str) -> None:
        self._append_log(f"[{source}] {msg}")

    def _is_dev_connected(self, dev: object) -> bool:
        try:
            v = getattr(dev, "is_connected", None)
            if callable(v):
                return bool(v())
            if isinstance(v, bool):
                return v
        except Exception:
            pass
        try:
            return bool(getattr(dev, "_connected", False))
        except Exception:
            return False

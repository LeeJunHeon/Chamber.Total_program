# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio, contextlib
from typing import Any, Optional, Mapping

from PySide6.QtWidgets import QPlainTextEdit, QMessageBox
from PySide6.QtGui import QTextCursor

from controller.plasma_cleaning_controller import (
    PlasmaCleaningController,
    PlasmaCleaningParams,
    PCleanEvent,
)

# 타입 힌트(선택)
try:
    from device.mfc import AsyncMFC  # noqa
    from device.ig import AsyncIG    # noqa
except Exception:
    AsyncMFC = Any  # type: ignore
    AsyncIG = Any   # type: ignore


class PlasmaCleaningRuntime:
    """
    Load-Lock 플라즈마 클리닝 런타임 (CH1의 MFC 공유 사용)
    - 폴링은 사용하지 않음(이벤트 펌프만).
    - 입력값 5개(Target Pressure, Gas Flow, Working Pressure, RF Power, Process Time) 검증 실패 시 알림창으로 막음.
    - 남은 시간은 로그 대신 UI의 Process Time 입력칸 값을 1초마다 갱신(mm:ss).
    - IG 장비 인스턴스는 Runtime에서 생성/관리.
    """

    def __init__(
        self,
        *,
        ui: Any,
        prefix: str,
        loop: asyncio.AbstractEventLoop,
        plc: Any,
        mfc: Optional[Any],
        rf_clean: Optional[Any] = None,
        chat: Optional[Any] = None,
        gv_switch_by_ch: Mapping[int, str] | None = None,
    ) -> None:
        self.ui = ui
        self.prefix = prefix
        self._loop = loop
        self.plc = plc
        self.mfc = mfc
        self.rf_clean = rf_clean
        self.chat = chat

        # IG 장비는 런타임에서 생성
        try:
            self.ig = AsyncIG()  # 설정값이 모듈 내부/구성에서 로드되도록 가정
        except Exception:
            self.ig = None

        # 컨트롤러
        self.controller = PlasmaCleaningController(
            loop=loop,
            mfc_handle=(self.mfc.handle_command if self.mfc else self._noop_mfc),
            ig=self.ig,
            plc_write_switch=getattr(self.plc, "write_switch", None),
            rf_start=(rf_clean.start_process if rf_clean and hasattr(rf_clean, "start_process") else None),
            rf_stop=(rf_clean.cleanup      if rf_clean and hasattr(rf_clean, "cleanup")      else None),
            gv_switch_by_ch=dict(gv_switch_by_ch or {}),
        )

        self._bg_tasks: list[asyncio.Task] = []
        self._pump_started = False

        # UI 포인터
        self._w_log: QPlainTextEdit | None = self._u("pc_logMessage_edit")
        self._w_state: QPlainTextEdit | None = None

        # 버튼 바인딩
        self._connect_buttons()

        # 이벤트 펌프 시작
        self._ensure_event_pump()

    # ──────────────────────────────────────────────────────────────
    # UI / 이벤트 바인딩

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
                    # 남은 시간을 UI(Process Time) 입력칸에 mm:ss로 표기
                    left = int((ev.payload or {}).get("seconds_left", 0))
                    mm = left // 60
                    ss = left % 60
                    w = self._u("PC_ProcessTime_edit")
                    if w and hasattr(w, "setPlainText"):
                        w.setPlainText(f"{mm:02d}:{ss:02d}")

                elif ev.kind == "started":
                    self._append_log("▶ Load-Lock 플라즈마 클리닝 시작")
                    await self._ensure_mfc_connected()
                    await self._ensure_ig_connected()

                elif ev.kind == "finished":
                    self._append_log("✅ Load-Lock 플라즈마 클리닝 정상 종료")

                elif ev.kind == "failed":
                    self._append_log(f"❌ 실패: {ev.message}")

            except Exception as e:
                self._append_log(f"[Pump] 예외: {e!r}")

    # ──────────────────────────────────────────────────────────────
    # 버튼 핸들러 / 입력 검증

    def _handle_start_clicked(self, _checked: bool = False):
        params = self._read_params_from_ui_and_validate()
        if not params:
            return

        async def run():
            # 장치 연결 보장
            await self._ensure_mfc_connected()
            await self._ensure_ig_connected()
            # 공정 시작
            self.controller.start(params)

        self._bg_tasks.append(self._loop.create_task(run(), name="PC.Start"))

    def _handle_stop_clicked(self, _checked: bool = False):
        self.controller.request_stop()

    def _read_params_from_ui_and_validate(self) -> Optional[PlasmaCleaningParams]:
        """
        필수 입력(빈칸/0/음수 금지) → 모두 통과 시 컨트롤러 파라미터 생성
          - Target Pressure (PC_targetPressure_edit)
          - Gas Flow (PC_gasFlow_edit)          # N2 sccm (MFC ch3)
          - Working Pressure (PC_workingPressure_edit)  # SP4 Set 값
          - RF Power (PC_rfPower_edit)
          - Process Time (PC_ProcessTime_edit, 분 단위 입력)
        """
        def gtxt(name: str) -> str:
            w = self._u(name)
            return (w.toPlainText().strip() if w and hasattr(w, "toPlainText") else "")

        errs: list[str] = []

        # Target Pressure
        tp_s = gtxt("PC_targetPressure_edit")
        target_pressure = None
        if not tp_s:
            errs.append("Target Pressure가 비어 있습니다.")
        else:
            try:
                target_pressure = float(tp_s)
                if target_pressure <= 0:
                    errs.append("Target Pressure는 0보다 커야 합니다.")
            except Exception:
                errs.append("Target Pressure 형식이 올바르지 않습니다(숫자).")

        # Gas Flow (N2@ch3)
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

        # Working Pressure (SP4 Set)
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

        # Process Time (분 입력 → 초 변환)
        pt_s = gtxt("PC_ProcessTime_edit")
        duration_sec = None
        if not pt_s:
            errs.append("Process Time(분)이 비어 있습니다.")
        else:
            try:
                # 사용자가 mm:ss로 보게 될 필드지만, '입력'은 분 단위로 받는다
                minutes = float(pt_s)
                if minutes <= 0:
                    errs.append("Process Time(분)은 0보다 커야 합니다.")
                else:
                    duration_sec = minutes * 60.0
            except Exception:
                errs.append("Process Time 형식이 올바르지 않습니다(숫자).")

        if errs:
            QMessageBox.warning(None, "입력 오류", "\n".join(f"• {e}" for e in errs))
            return None

        return PlasmaCleaningParams(
            target_pressure=float(target_pressure),     # type: ignore[arg-type]
            gas_flow_sccm=float(gas_flow),             # type: ignore[arg-type]
            working_pressure=float(working_pressure),   # type: ignore[arg-type]
            rf_power_w=float(rf_power),                 # type: ignore[arg-type]
            duration_sec=float(duration_sec),           # type: ignore[arg-type]
            target_chamber=None,                        # 필요 시 채널 지정
        )

    # ──────────────────────────────────────────────────────────────
    # 장치 연결 보장/종료

    async def _ensure_mfc_connected(self) -> None:
        if not self.mfc:
            return
        if self._is_dev_connected(self.mfc):
            return
        try:
            res = self.mfc.start()
            if asyncio.iscoroutine(res):
                await res
            self._append_log("[MFC] start() 호출 완료")
        except Exception as e:
            self._append_log(f"[MFC] start 실패: {e!r}")

    async def _ensure_ig_connected(self) -> None:
        if not self.ig:
            self._append_log("[IG] 인스턴스 없음(생성 실패)")
            return
        if self._is_dev_connected(self.ig):
            return
        try:
            res = self.ig.start()
            if asyncio.iscoroutine(res):
                await res
            self._append_log("[IG] start() 호출 완료")
        except Exception as e:
            self._append_log(f"[IG] start 실패: {e!r}")

    def shutdown_fast(self) -> None:
        for t in list(self._bg_tasks):
            if t and not t.done():
                t.cancel()
        self._bg_tasks.clear()

    # ──────────────────────────────────────────────────────────────
    # 표시/상태

    def _append_log(self, line: str) -> None:
        w = self._w_log
        if not w:
            return
        try:
            w.moveCursor(QTextCursor.MoveOperation.End)
            w.insertPlainText(line + "\n")
        except Exception:
            pass

    def _set_state(self, text: str) -> None:
        w = self._w_state
        if not w:
            return
        try:
            w.setPlainText(text)
        except Exception:
            pass

    # ──────────────────────────────────────────────────────────────
    # 유틸

    def _u(self, leaf: str) -> Any | None:
        if not getattr(self, "ui", None):
            return None
        return getattr(self.ui, f"{self.prefix}{leaf}", None)

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

    async def _noop_mfc(self, *_args, **_kwargs):
        raise RuntimeError("MFC 핸들러가 주입되지 않았습니다.")

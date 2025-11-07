# runtime/pre_sputter_runtime.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Optional, Callable

from controller.runtime_state import runtime_state  # CH1/CH2 실행 상태 조회용
# ch1, ch2는 runtime.chamber_runtime.ChamberRuntime 인스턴스여야 합니다.
# (start_presputter_from_ui(), is_running, append_log 등을 가정)

def _next_time_at(hh: int, mm: int) -> datetime:
    """
    오늘 hh:mm, 이미 지났으면 내일 같은 시각을 반환(로컬 시간 기준).
    """
    now = datetime.now()
    cand = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if cand <= now:
        cand = cand + timedelta(days=1)
    return cand


class PreSputterRuntime:
    """
    매일 지정 시각에 Pre-Sputter를 자동 실행하는 런타임.

    특징
    - CH1/CH2 동시 공정 허용(기본 parallel=True). 순차 실행도 선택 가능.
    - 시작 직전 다른 공정이 돌고 있으면, 빌 때까지 대기(1분에 한 번만 대기 로그).
    - 각 챔버는 ChamberRuntime.start_presputter_from_ui()를 통해
      'Start 버튼과 동일한 경로'로 현재 UI 값(기본값/마지막값)으로 1회 실행.
    - 실행 결과/상태는 기존 ChamberRuntime 로깅(append_log) 및 선택적 ChatNotifier로 브로드캐스트.

    사용
    ----
        runtime = PreSputterRuntime(ch1, ch2, chat=chat, hh=6, mm=0, parallel=True)
        runtime.start_daily()
        # 중단: runtime.stop()
    """

    def __init__(
        self,
        ch1,
        ch2,
        *,
        chat: Optional[object] = None,
        hh: int = 6,
        mm: int = 0,
        parallel: bool = True,
        wait_log_interval_s: float = 60.0,
        ui=None,
    ) -> None:
        self.ch1 = ch1
        self.ch2 = ch2
        self.chat = chat

        self.hh = int(hh)
        self.mm = int(mm)
        self.parallel = bool(parallel)
        self.wait_log_interval_s = float(wait_log_interval_s)

        self._task: Optional[asyncio.Task] = None
        self._repeat_daily: bool = True

        self._ui = ui # ★ UI 참조 (없으면 None)
        self._ui_bound: bool = False            # ★ 추가: 중복 바인딩 방지
        self._base_pressure_text: Optional[str] = None  # ★ 추가: Start시 저장해 둘 Base Pressure

        # 어떤 챔버용 런타임인지 로그에 표기하려고 라벨 보유
        if ch1 and not ch2:
            self._label = "CH1"
        elif ch2 and not ch1:
            self._label = "CH2"
        else:
            self._label = "CH1+CH2"

    # ─────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────
    def _fmt_hms(self, seconds: float) -> str:
        if seconds < 0: seconds = 0
        s = int(seconds)
        h, m, sec = s // 3600, (s % 3600) // 60, s % 60
        return f"{h:02d}:{m:02d}:{sec:02d}"

    def _set_text(self, w, s: str) -> None:
        if not w: return
        try:
            if hasattr(w, "setPlainText"): w.setPlainText(s)
            elif hasattr(w, "setText"): w.setText(s)
        except Exception:
            pass

    def start_daily(self) -> None:
        self.stop(silent=False)
        self._repeat_daily = True   # ★ 매일 반복
        when = _next_time_at(self.hh, self.mm)

        # ★ UI 초기 표기
        if self._ui:
            self._set_text(self._ui.preSputter_SetTime_edit, when.strftime("%H:%M"))
            self._set_text(
                self._ui.preSputter_LeftTime_edit,
                self._fmt_hms((when - datetime.now()).total_seconds()),
            )
            self._set_text(self._ui.preSputter_remainigTime_edit, "—")

        loop = asyncio.get_event_loop_policy().get_event_loop()
        self._task = loop.create_task(self._loop(when), name="PreSputterRuntime")
        self._log(f"[PreSputter] 예약 등록: {when.strftime('%Y-%m-%d %H:%M:%S')} (매일 반복)")

    def bind_ui(self, ui) -> None:
        """Pre-Sputter Start/Stop 버튼을 런타임에 연결(1회)."""
        if not ui or self._ui_bound:
            return
        self._ui = ui

        # Start → UI의 시간/베이스프레셔로 매일 예약 파라미터 갱신
        try:
            if hasattr(ui, "preSputter_Start_button"):
                ui.preSputter_Start_button.clicked.connect(self._on_start_clicked)
        except Exception:
            pass

        # Stop → 예약만 취소
        try:
            if hasattr(ui, "preSputter_Stop_button"):
                ui.preSputter_Stop_button.clicked.connect(self.stop)
        except Exception:
            pass

        self._ui_bound = True
        self._log("[PreSputter] UI 버튼 바인딩 완료(Start=예약 파라미터 갱신, Stop=예약 취소)")

    def stop(self, *, silent: bool = False) -> None:
        """예약 취소(진행 중 공정은 건드리지 않음)."""
        had_task = bool(self._task and not self._task.done())
        if had_task:
            self._task.cancel()
        self._task = None
        self._repeat_daily = False
        # 실제로 취소한 경우에만, 그리고 silent가 아닐 때만 로그 출력
        if (not silent) and had_task:
            self._log("예약 취소됨")

    def _on_start_clicked(self) -> None:
        """UI의 예약시각, Base Pressure로 매일 예약 파라미터 갱신 후 즉시 재예약."""
        hh, mm = self._read_time_from_ui()
        if hh is None:
            self._log("[PreSputter] 잘못된 시간 형식입니다. 예) 08:30")
            lab = getattr(self._ui, "preSputter_LeftTime_label", None)
            if lab:
                try: lab.setText("Invalid time (HH:MM)")
                except Exception: pass
            return

        # 1) 파라미터 갱신
        self.hh, self.mm = int(hh), int(mm)

        # 2) 매일 예약을 새 파라미터로 재시작
        self.start_daily()

    def _read_time_from_ui(self):
        """preSputter_SetTime_edit에서 HH:MM 또는 '8시30분' 류를 파싱."""
        ui = self._ui
        if not ui: return (None, None)
        edit = getattr(ui, "preSputter_SetTime_edit", None)
        if not edit: return (None, None)
        try:
            raw = edit.toPlainText().strip()
        except Exception:
            return (None, None)

        import re
        m = re.match(r"^\s*(\d{1,2})\s*(?::|시)\s*(\d{1,2})", raw)
        if not m: return (None, None)
        hh, mm = int(m.group(1)), int(m.group(2))
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            return (None, None)
        return (hh, mm)

    async def start_once_now(self, *, parallel: Optional[bool] = None) -> None:
        """
        지금 즉시 한 번 실행(디버그/수동 트리거용).
        """
        par = self.parallel if parallel is None else bool(parallel)
        await self._run(parallel=par)

    # ─────────────────────────────────────────────────────
    # Internal
    # ─────────────────────────────────────────────────────
    async def _loop(self, when: datetime) -> None:
        try:
            while True:
                # 1) 지정 시각까지 대기
                # 교체:
                while True:
                    remain_s = (when - datetime.now()).total_seconds()
                    if remain_s <= 0:
                        break
                    if self._ui:
                        self._set_text(self._ui.preSputter_LeftTime_edit, self._fmt_hms(remain_s))
                    await asyncio.sleep(1.0)
                if self._ui:
                    self._set_text(self._ui.preSputter_LeftTime_edit, "00:00:00")

                # 2) 내 챔버가 바쁘면 이번 예약은 PASS (대기하지 않음)
                def _my_ch_busy() -> bool:
                    return ((self.ch1 and self.ch1.is_running) or
                            (self.ch2 and self.ch2.is_running))

                if _my_ch_busy():
                    self._log("[PreSputter] 해당 챔버가 이미 공정 중 → 이번 예약 PASS")
                    if not self._repeat_daily:
                        self._log("[PreSputter] 1회 예약 실행 완료(반복 없음).")
                        break
                    # 다음날 재예약
                    when = when + timedelta(days=1)
                    self._log(f"[PreSputter] 다음 반복 예약: {when.strftime('%Y-%m-%d %H:%M:%S')}")
                    continue

                # 3) 실행(병렬 or 순차)
                await self._run(parallel=self.parallel)

                # 4) 다음날 재예약
                if not self._repeat_daily:
                    self._log("[PreSputter] 1회 예약 실행 완료(반복 없음).")
                    break
                when = when + timedelta(days=1)
                self._log(f"[PreSputter] 다음 반복 예약: {when.strftime('%Y-%m-%d %H:%M:%S')}")
        except asyncio.CancelledError:
            pass
        finally:
            if not self._repeat_daily:
                self._task = None

    async def _run(self, *, parallel: bool) -> None:
        if parallel:
            await self._run_parallel()
        else:
            await self._run_sequential()

    async def _run_parallel(self) -> None:
        # 같은 챔버의 중복 실행만 막고(CH간은 허용), 둘 다 트리거
        started = []
        if self.ch1 and not self.ch1.is_running:
            ok = self.ch1.start_presputter_from_ui()
            started.append(("CH1", ok))
        if self.ch2 and not self.ch2.is_running:
            ok = self.ch2.start_presputter_from_ui()
            started.append(("CH2", ok))

        # 종료까지 감시(둘 다 False가 될 때까지)
        # _run_parallel()의 감시 루프 대체
        while (self.ch1 and self.ch1.is_running) or (self.ch2 and self.ch2.is_running):
            if self._ui:
                self._set_text(self._ui.preSputter_remainigTime_edit, "—")
            await asyncio.sleep(1.0)
        if self._ui:
            self._set_text(self._ui.preSputter_remainigTime_edit, "00:00:00")


        pretty = ", ".join([f"{label}:{'OK' if ok else 'FAIL'}" for label, ok in started]) or "None"
        self._log(f"[PreSputter] 병렬 실행 완료 ({pretty})")

    async def _run_sequential(self) -> None:
        await self._run_one(self.ch1, "CH1")
        await asyncio.sleep(5.0)  # 버스 안정화/로그 여유
        await self._run_one(self.ch2, "CH2")

    async def _run_one(self, ch, label: str) -> None:
        if not ch:
            return
        if ch.is_running:
            self._log(f"[PreSputter] {label} 이미 실행 중 → 건너뜀"); return
        ok = ch.start_presputter_from_ui()
        if not ok:
            self._log(f"[PreSputter] {label} 시작 실패"); return
        # _run_one()의 감시 루프 대체
        while ch.is_running:
            if self._ui:
                self._set_text(self._ui.preSputter_remainigTime_edit, "—")
            await asyncio.sleep(1.0)
        if self._ui:
            self._set_text(self._ui.preSputter_remainigTime_edit, "00:00:00")

        self._log(f"[PreSputter] {label} 완료")

    # ★ 공개 API: 메인에서 바로 호출할 수 있도록 이름 변경
    def schedule_from_ui(self) -> None:
        """UI의 예약시각/베이스프레셔를 읽어 매일 예약을 갱신."""
        hh, mm = self._read_time_from_ui()
        if hh is None:
            self._log("[설정] 잘못된 시간 형식(HH:MM)")
            lab = getattr(self._ui, "preSputter_LeftTime_label", None)
            if lab:
                try: lab.setText("Invalid time (HH:MM)")
                except Exception: pass
            return

        bp_txt = self._read_base_pressure_from_ui()
        if bp_txt and bp_txt.strip():
            self._base_pressure_text = bp_txt.strip()
            self._log(f"[설정] Base Pressure 고정: {self._base_pressure_text}")
        else:
            self._base_pressure_text = None
            self._log("[설정] Base Pressure 미입력 → 각 챔버 UI값 사용")

        self.hh, self.mm = int(hh), int(mm)
        self.start_daily()

    def _log(self, msg: str) -> None:
        line = f"[{datetime.now():%H:%M:%S}] [PreSputter] {self._label} {msg}"
        sink = self._log_sink
        if sink:
            try: sink(line)
            except Exception: pass
        if self.chat:
            try: self.chat.notify_text(f"[PreSputter] {self._label} {msg}")
            except Exception: pass

    def set_pc_logger(self, sink: Callable[[str], None]) -> None:
        """한 줄 문자열을 받아 출력하는 콜백(예: pc_logMessage_edit.appendPlainText)을 주입."""
        self._log_sink = sink

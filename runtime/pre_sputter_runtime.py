# runtime/pre_sputter_runtime.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Optional

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
        self.stop()
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


    def stop(self) -> None:
        """
        예약 중지.
        """
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = None

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

                # 2) 다른 공정이 돌고 있으면 빌 때까지 대기(스팸 방지: 1분에 한번 로그)
                last_log = 0.0
                loop_time = asyncio.get_running_loop().time
                while runtime_state.any_running():
                    t = loop_time()
                    if t - last_log >= self.wait_log_interval_s:
                        self._log("[PreSputter] 다른 공정 진행 중… 예약 실행 대기")
                        last_log = t
                    await asyncio.sleep(5.0)

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

    def _log(self, msg: str) -> None:
        """
        두 챔버 로그로 브로드캐스트 + 선택적 ChatNotifier 전송.
        """
        try:
            if self.ch1: self.ch1.append_log("Auto", msg)
        except Exception:
            pass
        try:
            if self.ch2: self.ch2.append_log("Auto", msg)
        except Exception:
            pass
        if self.chat:
            try:
                # ChatNotifier에 일반 텍스트 보내는 메서드명은 프로젝트에 맞게 수정
                self.chat.notify_text(msg)
            except Exception:
                pass

# controller/heater_saturation.py
# -*- coding: utf-8 -*-
"""DAC 포화 감시 — 유지 모드와 독립된 안전망. 챔버K controller/heater_saturation.py 이식(Qt 없음, TC2 없음).

배경(챔버K 2026-09-22): 유지 모드 진입 실패 상태에서 셔터가 열려 PV 급락 → PID 와인드업 → DAC 상한 포화 25분.
포화를 보는 코드가 없어 늦게 발견했다.

동작(HeaterService 가 hold.tick 직전에 tick(st, hold_active, hold_kind) 를 부른다):
  · RUN 중 유지 모드가 아니고 MV ≥ 0.98×실제 상한(D118 되읽기)이 SAT_SEC 연속이면 포화 1회:
    경고 + saturated(dict) + D118 ← 포화 직전 60초 평균 MV(없으면 상한×0.9), [MV_MIN+20, MV_LIMIT] 클램프.
  · 해제(RUN OFF / FAULT / 목표 변경) → MV_LIMIT 원복.
  · dac 유지 중이면 클램프하지 않고(유지 모드가 D118 소유) 경고만, 에피소드당 1회.
  · 목표(SV)가 바뀌면 다시 잰다 — 파이썬 램프 중에는 SV 가 계속 바뀌어 포화 판정이 안 된다(챔버K 와 같다).
PLC 를 직접 쓰지 않는다 — request_mv_limit 콜백으로 요청만 한다.
"""
from __future__ import annotations

import time
from collections import deque
from typing import Callable

MV_SAT_REL = 0.98          # 상한의 98% 이상이면 "상한에 물려 있다"
MV_SAT_FALLBACK_REL = 0.9  # 포화 직전 이력이 없을 때 클램프 값(상한의 90%)
PRE_WINDOW_SEC = 60.0      # 클램프 값을 잡는 "포화 직전" 창


def _noop(*_a, **_k):
    return None


class HeaterSaturationGuard:
    def __init__(self, *, mv_limit: int, mv_min: int, sat_sec: float,
                 clock: Callable[[], float] = time.monotonic,
                 request_mv_limit: Callable[[int], None] = _noop,
                 message: Callable[[str, str], None] = _noop,
                 saturated: Callable[[dict], None] = _noop):
        self.mv_limit = int(mv_limit)
        self.mv_min = int(mv_min)
        self.sat_sec = float(sat_sec)
        self._now = clock
        self._req = request_mv_limit
        self._message = message
        self._saturated = saturated
        self._hist: deque = deque()
        self._sat_since = 0.0
        self._clamped = None
        self._warned_hold = False
        self._sv = None

    @property
    def clamped(self):
        return self._clamped

    def _msg(self, level: str, text: str) -> None:
        try:
            self._message(level, text)
        except Exception:
            pass

    def _release(self, why: str, restore: bool = True) -> None:
        if self._clamped is not None:
            if restore:
                try:
                    self._req(int(self.mv_limit))
                except Exception:
                    pass
                self._msg("정보", f"DAC 포화 클램프 해제 → {int(self.mv_limit)} 원복 ({why})")
            else:
                self._msg("정보", f"DAC 포화 클램프 종료 — 유지 모드가 D118 을 이어받음 ({why})")
        self._clamped = None
        self._sat_since = 0.0
        self._warned_hold = False
        self._hist.clear()

    def tick(self, st: dict, hold_active: bool, hold_kind=None) -> None:
        try:
            self._tick(st, bool(hold_active), hold_kind)
        except Exception as e:
            self._msg("경고", f"DAC 포화 감시 예외: {e!r}")

    def _tick(self, st: dict, hold_active: bool, hold_kind) -> None:
        now = self._now()
        run = bool(st.get('run')); fault = bool(st.get('fault'))
        sv = st.get('sv'); mv = st.get('mv')
        if not run:
            self._release("운전 OFF"); self._sv = None
            return
        if fault:
            self._release("히터 이상")
            return
        if hold_active:
            if self._clamped is not None:
                self._release("유지 모드 진입", restore=(hold_kind != 'dac'))
            if hold_kind != 'dac':
                self._sat_since = 0.0; self._warned_hold = False
                return
            if mv is None or self._warned_hold:
                return
            if float(mv) >= MV_SAT_REL * self._actual_limit(st):
                if not self._sat_since:
                    self._sat_since = now
                elif (now - self._sat_since) >= self.sat_sec:
                    self._warned_hold = True
                    self._announce(st, now, clamp=None, owner="hold_dac")
            else:
                self._sat_since = 0.0
            return
        if sv is not None:
            if self._sv is not None and abs(float(sv) - float(self._sv)) > 0.05:
                self._release(f"목표 변경 {self._sv} → {sv}")
            self._sv = float(sv)
        if mv is None:
            return
        mv = float(mv)
        self._hist.append((now, mv))
        keep_from = (self._sat_since if self._sat_since else now) - PRE_WINDOW_SEC
        while self._hist and self._hist[0][0] < keep_from:
            self._hist.popleft()
        if self._clamped is not None:
            return
        if mv >= MV_SAT_REL * self._actual_limit(st):
            if not self._sat_since:
                self._sat_since = now
            elif (now - self._sat_since) >= self.sat_sec:
                self._saturate(st, now)
        else:
            self._sat_since = 0.0

    def _actual_limit(self, st: dict) -> float:
        try:
            v = st.get('mv_limit')
            return float(v) if v is not None and float(v) > 0 else float(self.mv_limit)
        except Exception:
            return float(self.mv_limit)

    def _saturate(self, st: dict, now: float) -> None:
        pre = [m for t, m in self._hist if (self._sat_since - PRE_WINDOW_SEC) <= t < self._sat_since]
        if pre:
            raw = sum(pre) / len(pre); src = f"포화 직전 {PRE_WINDOW_SEC:.0f}초 평균"
        else:
            raw = MV_SAT_FALLBACK_REL * float(self.mv_limit); src = f"이력 없음 → 상한×{MV_SAT_FALLBACK_REL:g}"
        value = max(self.mv_min + 20, min(self.mv_limit, int(round(raw))))
        self._clamped = value
        self._announce(st, now, clamp=value, owner="guard", src=src)
        try:
            self._req(value)
        except Exception:
            pass

    def _announce(self, st: dict, now: float, clamp, owner: str, src: str = "") -> None:
        sec = now - self._sat_since
        pv = st.get('pv'); mv = st.get('mv')
        limit = int(self._actual_limit(st))
        t = "--.-" if pv is None else f"{float(pv):.1f}"
        tail = (f" → D118 을 {clamp}({src})로 클램프" if clamp is not None
                else f" — 클램프 없음(유지 모드가 D118 {limit} 을 소유)")
        self._msg("경고", f"DAC 출력 포화 — {sec:.0f}초째 상한({limit})에 물려 있음: PV {t} / MV {mv}{tail}")
        try:
            self._saturated({"pv": pv, "mv": mv, "sec": sec, "clamp": clamp, "src": src,
                             "owner": owner, "limit": limit})
        except Exception:
            pass

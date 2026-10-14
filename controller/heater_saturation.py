# controller/heater_saturation.py
# -*- coding: utf-8 -*-
"""DAC 포화 감시 — 유지 모드와 독립된 안전망. 챔버K controller/heater_saturation.py 이식(Qt 없음, TC2 없음).

배경(챔버K 2026-09-22): 목표에 도달한 뒤 유지 모드 진입에 실패한 상태에서 셔터가 열려 PV 급락 → PID 와인드업 →
DAC 상한 포화 25분. 포화를 보는 코드가 없어 늦게 발견했다. 이 감시가 막으려는 것은 '도달 후 새로 시작된 포화'다.

CH2 이식 후 에뮬레이터로 확인한 문제(2026-10): '도달' 개념 없이 "상한에 120초"만 보면
  · 승온 중(SV 가 홀드백으로 멈춘 채 MV 상한)에도 D118 을 줄여 온도가 내려가고(300°C 목표: 248→189°C),
  · dac 유지(D118 = 창 평균 MV)에서 MV 가 유지값에 붙는 정상 상태를 포화로 경고했다.
→ 판정 규칙(HeaterService 가 hold.tick 직전에 tick(st, hold_active, hold_kind, final_target, ramp_active) 를 부른다):
  a) RUN OFF / FAULT: 해제(클램프 중이면 MV_LIMIT 원복) + 이력·도달·무장·정체·유지 정보 상태 전부 초기화.
  b) 도달 래치(목표당 1회): 파이썬 램프 끝, st['ok'], |D119−SV| ≤ 0.05, |SV−final| ≤ 0.05, |PV−final| ≤ arrive_tol.
     목표(final)가 0.05°C 넘게 바뀌면 도달·무장·정체를 초기화한다.
  c) 유지 중: 포화 경고·카드·클램프를 하지 않는다(dac 유지에서 MV 가 유지값에 붙는 것은 정상).
     dac 유지 중 SV−PV ≥ HOLD_LOW_INFO_C 가 sat_sec 이어지면 정보 로그 1줄(유지 에피소드당 1회).
  d) MV 이력: 유지 중이 아니면 매 tick 적재, (포화 시작 또는 지금) − 60초보다 오래된 것은 버린다.
     RUN OFF·FAULT 에서만 지운다 — 램프 끝 직후 포화에도 직전 이력을 쓴다.
  e) 도달 전: 절대 클램프하지 않는다. MV 상한·PV 미도달이 STALL_WINDOW_SEC 이어지는 동안 상승 속도를 재
     '승온 정체'(출력 상한이 모자랄 수 있음)만 목표당 1회 알린다.
  f) 도달 후: 도달 뒤 MV 가 상한 아래로 한 번 내려와야 '무장'된다(도달 시점부터 상한이면 포화로 세지 않는다 —
     출력 여유 없음은 유지 모드 포기 경고가 이미 알린다). 무장 뒤 상한에 sat_sec 연속이면 포화 1회:
     직전 60초 이력이 PRE_MIN_COVER_SEC 이상이고 평균 < 상한×0.98 이면 D118 ← 평균(클램프, 챔버K 보호),
     아니면 줄일 근거가 없으므로 경고만(MV 가 상한 아래로 내려올 때까지 다시 알리지 않음).
  g) 해제 조건(RUN OFF / FAULT / SV 0.05°C 초과 변경)과 '유지 모드 진입' 처리는 챔버K 와 같다.
'상한' = 실제 상한(st['mv_limit'] = D118 되읽기). PLC 를 직접 쓰지 않는다 — request_mv_limit 콜백으로 요청만 한다.
"""
from __future__ import annotations

import time
from collections import deque
from typing import Callable, Optional

MV_SAT_REL = 0.98            # 상한의 98% 이상이면 "상한에 물려 있다"
PRE_WINDOW_SEC = 60.0        # 클램프 값을 잡는 "포화 직전" 창
PRE_MIN_COVER_SEC = 30.0     # 직전 이력이 이만큼은 거슬러 올라가야 클램프한다
STALL_WINDOW_SEC = 300.0     # 승온 정체 판정 창
STALL_EDGE_SEC = 30.0        # 창 앞·뒤 평균 구간
STALL_RATE_C_PER_MIN = 0.5   # 이보다 느리면 정체 후보
STALL_ETA_MIN = 60.0         # 현재 속도로 남은 시간이 이보다 길면 정체
HOLD_LOW_INFO_C = 10.0       # dac 유지 중 PV 가 목표보다 이만큼 낮으면 정보 로그


def _noop(*_a, **_k):
    return None


class HeaterSaturationGuard:
    def __init__(self, *, mv_limit: int, mv_min: int, sat_sec: float,
                 arrive_tol_c: float = 1.0,
                 clock: Callable[[], float] = time.monotonic,
                 request_mv_limit: Callable[[int], None] = _noop,
                 message: Callable[[str, str], None] = _noop,
                 saturated: Callable[[dict], None] = _noop,
                 stalled: Callable[[dict], None] = _noop):
        self.mv_limit = int(mv_limit)
        self.mv_min = int(mv_min)
        self.sat_sec = float(sat_sec)
        self.arrive_tol = float(arrive_tol_c)
        self._now = clock
        self._req = request_mv_limit
        self._message = message
        self._saturated = saturated
        self._stalled = stalled
        self._hist: deque = deque()          # (t, mv)
        self._sat_since = 0.0
        self._clamped: Optional[int] = None
        self._sat_noted = False              # 이번 에피소드에 '클램프 없는 포화' 경고를 냈는가
        self._sv: Optional[float] = None     # SV 변경 감지(해제 조건)
        self._final: Optional[float] = None  # 목표 변경 감지(도달·정체 초기화)
        self._arrived = False
        self._armed = False
        self._stall_win: deque = deque()     # (t, pv)
        self._stall_done = False
        self._low_since = 0.0
        self._low_done = False
        self.clamp_enabled = True            # False: 공정이 작업 조건에서 유지 모드를 다시 잡기 전 — 포화는 경고만

    # ───────── 조회 ─────────
    @property
    def clamped(self):
        return self._clamped

    @property
    def arrived(self) -> bool:
        return bool(self._arrived)

    def recent_mv_avg(self, window_s: float = PRE_WINDOW_SEC) -> Optional[float]:
        """최근 window_s 초 MV 평균(유지 중이 아닐 때 쌓인 이력). 없으면 None."""
        now = self._now()
        xs = [m for t, m in self._hist if t >= now - float(window_s)]
        return (sum(xs) / len(xs)) if xs else None

    def _msg(self, level: str, text: str) -> None:
        try:
            self._message(level, text)
        except Exception:
            pass

    def _release(self, why: str, restore: bool = True) -> None:
        """클램프 해제(중이면) + 포화 진행 상태 초기화. 이력은 지우지 않는다(지우는 것은 _reset_all)."""
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
        self._sat_noted = False

    def _reset_target(self) -> None:
        self._arrived = False
        self._armed = False
        self._stall_win.clear()
        self._stall_done = False

    def set_clamp_enabled(self, on: bool, why: str = "") -> None:
        """공정이 유지 모드를 작업 조건(작업 압력·플라즈마)에서 다시 잡게 할 때 쓴다(HeaterService.set_hold_armable).
        끌 때: 클램프 중이면 MV_LIMIT 원복. 켤 때: 꺼져 있던 동안의 포화 진행·이력·무장을 버린다 —
        가스로 늘어난 부하에 MV 가 올라간 것은 '새 포화'가 아니므로, 켠 뒤 상한 아래로 한 번 내려와야 다시 무장된다."""
        on = bool(on)
        if not on and self._clamped is not None:
            self._release(why or "공정이 유지 재측정 요청")
        if on and not self.clamp_enabled:
            self._hist.clear()
            self._armed = False
            self._sat_since = 0.0
            self._sat_noted = False
        self.clamp_enabled = on

    def _reset_all(self) -> None:
        self._hist.clear()
        self._sv = None
        self._final = None
        self._reset_target()
        self._low_since = 0.0
        self._low_done = False

    # ───────── tick ─────────
    def tick(self, st: dict, hold_active: bool, hold_kind=None, final_target=None, ramp_active: bool = False) -> None:
        try:
            self._tick(st, bool(hold_active), hold_kind, final_target, bool(ramp_active))
        except Exception as e:
            self._msg("경고", f"DAC 포화 감시 예외: {e!r}")

    def _tick(self, st: dict, hold_active: bool, hold_kind, final_target, ramp_active: bool) -> None:
        now = self._now()
        # a) RUN OFF / FAULT
        if not st.get('run'):
            self._release("운전 OFF")
            self._reset_all()
            return
        if st.get('fault'):
            self._release("히터 이상")
            self._reset_all()
            return
        sv = st.get('sv'); mv = st.get('mv'); pv = st.get('pv'); svr = st.get('sv_ramp')
        try:
            final = float(final_target) if final_target is not None else (float(sv) if sv is not None else None)
        except Exception:
            final = float(sv) if sv is not None else None

        # b) 도달 래치(목표당 1회)
        if final is not None:
            if self._final is not None and abs(final - self._final) > 0.05:
                self._reset_target()
            self._final = final
        if (not self._arrived and not ramp_active and st.get('ok') and None not in (pv, sv, svr, final)
                and abs(float(svr) - float(sv)) <= 0.05 and abs(float(sv) - final) <= 0.05
                and abs(float(pv) - final) <= self.arrive_tol):
            self._arrived = True
            self._armed = False

        # c) 유지 중 — 경고·카드·클램프 없음
        if hold_active:
            if self._clamped is not None:
                self._release("유지 모드 진입", restore=(hold_kind != 'dac'))
            self._sat_since = 0.0
            self._sat_noted = False
            if hold_kind == 'dac' and sv is not None and pv is not None:
                low = float(sv) - float(pv)
                if low >= HOLD_LOW_INFO_C:
                    if not self._low_since:
                        self._low_since = now
                    elif (now - self._low_since) >= self.sat_sec and not self._low_done:
                        self._low_done = True
                        self._msg("정보",
                                  f"유지 중 PV 가 목표보다 {low:.1f}°C 낮은 상태 {now - self._low_since:.0f}초 — "
                                  f"출력은 유지값(D118 {st.get('mv_limit')})으로 제한 중. 셔터·가스 등으로 TC 표시만 "
                                  f"바뀐 경우면 정상, 히터 자체가 식는 경우면 확인 필요")
                else:
                    self._low_since = 0.0
            return
        self._low_since = 0.0
        self._low_done = False

        # g) SV 변경 해제(이력은 유지)
        if sv is not None:
            if self._sv is not None and abs(float(sv) - float(self._sv)) > 0.05:
                self._release(f"목표 변경 {self._sv} → {sv}")
            self._sv = float(sv)
        if mv is None:
            return
        mv = float(mv)

        # d) MV 이력
        self._hist.append((now, mv))
        keep_from = (self._sat_since if self._sat_since else now) - PRE_WINDOW_SEC
        while self._hist and self._hist[0][0] < keep_from:
            self._hist.popleft()

        limit = self._actual_limit(st)
        at_lim = mv >= MV_SAT_REL * limit

        # e) 도달 전 — 클램프 없음, 승온 정체만
        if not self._arrived:
            self._sat_since = 0.0
            self._stall_tick(now, pv, final, mv, limit, at_lim)
            return
        self._stall_win.clear()

        # f) 도달 후 — 무장 뒤 새로 시작된 포화만
        if not self._armed:
            if not at_lim:
                self._armed = True
            return
        if self._clamped is not None:
            return
        if not at_lim:
            self._sat_since = 0.0
            self._sat_noted = False
            return
        if not self._sat_since:
            self._sat_since = now
            return
        if (now - self._sat_since) >= self.sat_sec and not self._sat_noted:
            self._saturate(st, now, limit)

    def _stall_tick(self, now: float, pv, final, mv: float, limit: float, at_lim: bool) -> None:
        if not at_lim or pv is None or final is None or float(pv) >= final - self.arrive_tol:
            self._stall_win.clear()
            return
        self._stall_win.append((now, float(pv)))
        while self._stall_win and self._stall_win[0][0] < now - STALL_WINDOW_SEC:
            self._stall_win.popleft()
        if self._stall_done or (now - self._stall_win[0][0]) < STALL_WINDOW_SEC:
            return
        t0 = self._stall_win[0][0]
        head = [p for t, p in self._stall_win if t <= t0 + STALL_EDGE_SEC]
        tail = [p for t, p in self._stall_win if t >= now - STALL_EDGE_SEC]
        if not head or not tail:
            return
        rate = (sum(tail) / len(tail) - sum(head) / len(head)) / ((STALL_WINDOW_SEC - STALL_EDGE_SEC) / 60.0)
        dist = final - float(pv)
        eta = (dist / rate) if rate > 0 else float("inf")
        if rate >= STALL_RATE_C_PER_MIN or eta <= STALL_ETA_MIN:
            return
        self._stall_done = True
        eta_txt = "도달 안 됨" if eta == float("inf") else f"약 {eta:.0f}분"
        self._msg("경고",
                  f"승온 정체 — 출력 상한({int(limit)})에 붙은 채 최근 {STALL_WINDOW_SEC / 60:.0f}분 PV 상승 "
                  f"{rate:.2f}°C/min (PV {float(pv):.1f} / 목표 {final:.1f}°C, 현재 속도로 {eta_txt}) — "
                  f"출력 상한(HEATER_MV_LIMIT)이 모자랄 수 있음. 클램프하지 않음")
        try:
            self._stalled({"pv": float(pv), "target": final, "rate": rate,
                           "eta_min": None if eta == float("inf") else eta,
                           "mv": mv, "limit": int(limit), "window_s": STALL_WINDOW_SEC})
        except Exception:
            pass

    def _actual_limit(self, st: dict) -> float:
        try:
            v = st.get('mv_limit')
            return float(v) if v is not None and float(v) > 0 else float(self.mv_limit)
        except Exception:
            return float(self.mv_limit)

    def _saturate(self, st: dict, now: float, limit: float) -> None:
        sec = now - self._sat_since
        pre = [(t, m) for t, m in self._hist if (self._sat_since - PRE_WINDOW_SEC) <= t < self._sat_since]
        cover = (self._sat_since - pre[0][0]) if pre else 0.0
        avg = (sum(m for _t, m in pre) / len(pre)) if pre else None
        pv = st.get('pv'); mv = st.get('mv')
        t = "--.-" if pv is None else f"{float(pv):.1f}"
        if not self.clamp_enabled:
            self._sat_noted = True
            self._msg("경고", f"도달 후 {sec:.0f}초째 DAC 출력 상한({int(limit)})에 물려 있음: PV {t} / MV {mv} — "
                              f"공정이 작업 조건에서 유지 모드를 잡기 전(가스·압력·전원 변화 구간)이라 클램프하지 않음 — 출력 상한 여유 확인")
            return
        if avg is not None and cover >= PRE_MIN_COVER_SEC and avg < MV_SAT_REL * limit:
            value = max(self.mv_min + 20, min(self.mv_limit, int(round(avg))))
            self._clamped = value
            src = f"포화 직전 {PRE_WINDOW_SEC:.0f}초 평균"
            self._msg("경고", f"도달 후 {sec:.0f}초째 DAC 출력 상한({int(limit)})에 물려 있음: PV {t} / MV {mv} "
                              f"→ D118 을 {value}({src})로 클램프")
            try:
                self._saturated({"pv": pv, "mv": mv, "sec": sec, "clamp": value, "src": src,
                                 "owner": "guard", "limit": int(limit)})
            except Exception:
                pass
            try:
                self._req(value)
            except Exception:
                pass
            return
        self._sat_noted = True
        why = ("직전 이력 부족" if (avg is None or cover < PRE_MIN_COVER_SEC)
               else f"직전 평균 {avg:.0f} 도 상한")
        self._msg("경고", f"도달 후 {sec:.0f}초째 DAC 출력 상한({int(limit)})에 물려 있음: PV {t} / MV {mv} — "
                          f"{why}, 줄일 근거가 없어 클램프하지 않음")

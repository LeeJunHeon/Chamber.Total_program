"""CH2 히터 출력 무응답 감시 [EL-092] — v1 controller/heater_response.py(HeaterResponseGuard) 그대로 옮김.

판정(RUN·TC 정상·응답 있음일 때만, 폴링 1초 기준) — 어느 쪽이든 운전당 1번만 돌려준다:
  A) 출력 무응답 — 이번 운전에서 아직 데워진 적이 없고(운전 시작 뒤 최저 PV 보다 rise_c 이상 오른 적 없음)
     목표보다 DEMAND_C 이상 낮아 가열이 필요한 상태로, MV ≥ min(mv_high, 실제 상한×0.98) 이고
     PV 가 그 구간 시작 때와 rise_c 안(평탄)인 채 sec 초 연속.
  B) 출력 끊김 — MV ≥ 실제 상한(D118 되읽기)×0.98 이고 PV 가 목표보다 loss_below_c 이상 낮은 상태가 loss_sec 초 연속이며,
     그 사이 PV 가 loss_fall_c 이상 떨어짐(창 앞·뒤 EDGE_SEC 평균). dac 유지 중에는 보지 않는다.
     DAC 포화 클램프 중에는 클램프가 걸릴 때의 PV 추세로 정한다(그때 이미 B 기준 속도로 떨어지고 있었으면 계속 본다,
     아니면 그 클램프 동안 보지 않는다 — 남는 한계: 의심 없이 걸린 클램프 뒤 새 끊김은 여기서 끄지 않는다, HEAT-074).
     B 구간이 막 시작돼(2×EDGE_SEC 미만) 속도를 못 재면 포화 감시가 잴 때까지 클램프를 미룬다(loss_wait).
표본 사이가 GAP_RESET_SEC 넘게 벌어지거나 TC 이상·FAULT 면 연속 판정을 처음부터 다시 센다.
PLC 에 쓰지 않는다 — 끄기·카드·공정 중단은 엔진 몫.
"""
from __future__ import annotations

import time
from collections import deque
from typing import Callable, Optional

MV_SAT_REL = 0.98      # 상한의 98% 이상이면 "상한에 붙음"(heater_saturation 과 같은 기준)
DEMAND_C = 5.0         # 목표보다 이만큼 이상 낮으면 '가열이 필요한 상태'
EDGE_SEC = 30.0        # B: 창 앞·뒤 평균 구간
GAP_RESET_SEC = 10.0   # 표본 사이가 이보다 벌어지면 연속 판정을 처음부터
HINT = "D/A 출력 배선(DV04A CH3 → VARITAP)·VARITAP 전원/차단기·히터 단선·TC 위치 확인"


class HeaterResponseGuard:
    def __init__(self, *, enabled: bool = True, mv_high: float = 600.0, sec: float = 300.0,
                 rise_c: float = 5.0, loss_sec: float = 180.0, loss_below_c: float = 10.0,
                 loss_fall_c: float = 10.0, mv_limit: float = 1200.0,
                 clock: Callable[[], float] = time.monotonic):
        self.enabled = bool(enabled)
        self.mv_high = float(mv_high)
        self.sec = float(sec)
        self.rise_c = float(rise_c)
        self.loss_sec = float(loss_sec)
        self.loss_below_c = float(loss_below_c)
        self.loss_fall_c = float(loss_fall_c)
        self.mv_limit = float(mv_limit)
        self._now = clock
        self._reset_run()

    # ───────── 상태 ─────────
    def _reset_run(self) -> None:
        """RUN 이 꺼지면(또는 처음) — 이번 운전의 기준·연속 판정·판정 래치를 모두 비운다."""
        self._pv0: Optional[float] = None     # 이번 운전 첫 PV(문구용)
        self._pv_min: Optional[float] = None  # 이번 운전 최저 PV
        self._heated = False                  # 최저 PV 보다 rise_c 이상 오른 적이 있음(= 히터가 데운다)
        self._tripped = False
        self._clamp_prev = False              # 직전 tick 의 포화 클램프 여부(클램프 시작 감지)
        self._clamp_watch = False             # 이번 클램프가 출력 끊김 의심(B 하강 추세) 중에 걸렸음 → 클램프 중에도 B
        self._reset_streaks()

    def _reset_streaks(self) -> None:
        self._a_since = 0.0                   # A 조건 연속 시작
        self._a_pv: Optional[float] = None    # A 조건 시작 PV(평탄 판정)
        self._b_since = 0.0                   # B 조건 연속 시작
        self._b_win: deque = deque()          # (t, pv) — B 조건 구간, 최근 loss_sec 초
        self._last_t: Optional[float] = None

    @property
    def tripped(self) -> bool:
        return self._tripped

    def _trend(self) -> Optional[str]:
        """B 구간의 PV 하강 추세 — None: 구간 없음 / "wait": 2×EDGE_SEC 미만이라 아직 못 잼 /
        "fall": B 기준 속도(창이 다 차면 히터를 끄는 그 기준) 이상으로 떨어지는 중 / "slow": 그보다 느림."""
        if not self._b_since or not self._b_win:
            return None
        t0, t1 = self._b_win[0][0], self._b_win[-1][0]
        span = t1 - t0
        if span < 2.0 * EDGE_SEC:
            return "wait"
        head = [p for t, p in self._b_win if t <= t0 + EDGE_SEC]
        tail = [p for t, p in self._b_win if t >= t1 - EDGE_SEC]
        fall = sum(head) / len(head) - sum(tail) / len(tail)
        return "fall" if fall >= self.loss_fall_c * (span - EDGE_SEC) / (self.loss_sec - EDGE_SEC) else "slow"

    def loss_wait(self) -> bool:
        """B 구간이 막 시작돼 PV 하강 속도를 아직 못 잼 — 포화 감시가 클램프를 미룬다(B 구간 시작 뒤 최대 2×EDGE_SEC)."""
        return self.enabled and not self._tripped and self._trend() == "wait"

    def _limit(self, st: dict) -> float:
        try:
            v = st.get("mv_limit")
            return float(v) if v is not None and float(v) > 0 else self.mv_limit
        except Exception:
            return self.mv_limit

    # ───────── tick ─────────
    def tick(self, st: dict, final_target, holding: bool = False, clamped: bool = False) -> Optional[dict]:
        """폴링마다 부른다. 판정되면 사유 dict 를 1번 돌려준다(같은 운전에서 다시 내지 않음). 예외를 던지지 않는다.
        holding: dac 유지 중(B 를 보지 않음). clamped: DAC 포화 클램프 중(B 는 클램프가 걸릴 때의 추세로 — 모듈 설명)."""
        try:
            return self._tick(st, final_target, bool(holding), bool(clamped))
        except Exception:
            return None

    def _tick(self, st: dict, final_target, holding: bool, clamped: bool = False) -> Optional[dict]:
        if not st.get("run"):
            self._reset_run()
            return None
        if not self.enabled or self._tripped:
            return None
        if clamped and not self._clamp_prev:
            # 클램프가 막 걸림 — 그때 PV 가 이미 B 기준 속도로 떨어지고 있었으면(출력 끊김 의심) 이 클램프 동안에도 B 를 본다
            self._clamp_watch = (self._trend() == "fall")
        self._clamp_prev = clamped
        if st.get("stale") or st.get("fault") or not st.get("ok"):
            self._reset_streaks()
            return None
        pv, mv = st.get("pv"), st.get("mv")
        if pv is None or mv is None or final_target is None:
            self._reset_streaks()
            return None
        now = self._now()
        if self._last_t is not None and (now - self._last_t) > GAP_RESET_SEC:
            self._reset_streaks()
        self._last_t = now
        pv, mv, tgt = float(pv), float(mv), float(final_target)
        if self._pv0 is None:
            self._pv0 = self._pv_min = pv
        elif pv < self._pv_min:
            self._pv_min = pv
        if (pv - self._pv_min) >= self.rise_c:
            self._heated = True
        limit = self._limit(st)
        below = tgt - pv

        # A) 출력 무응답 — 아직 데워진 적 없이, 가열이 필요한데 MV 가 높고 PV 가 평탄
        thr = min(self.mv_high, MV_SAT_REL * limit)
        if (not self._heated) and mv >= thr and below >= DEMAND_C:
            if not self._a_since or abs(pv - float(self._a_pv)) >= self.rise_c:
                self._a_since, self._a_pv = now, pv          # 시작(또는 평탄이 깨져 다시 시작)
            elif (now - self._a_since) >= self.sec:
                return self._trip({
                    "kind": "noresp", "label": "출력 무응답", "pv": pv, "mv": mv, "limit": limit, "target": tgt,
                    "sec": now - self._a_since,
                    "text": (f"MV {mv:.0f}(≥{thr:.0f}) {(now - self._a_since) / 60:.0f}분 동안 PV {self._a_pv:.1f}→{pv:.1f}°C "
                             f"(운전 시작 PV {self._pv0:.1f}°C, 목표 {tgt:.1f}°C)"),
                })
        else:
            self._a_since, self._a_pv = 0.0, None

        # B) 출력 끊김 — 상한에서 목표보다 크게 낮은 채 빠르게 떨어짐
        #    (dac 유지 중, 출력 끊김 의심 없이 걸린 포화 클램프 중 제외)
        at_lim = mv >= MV_SAT_REL * limit
        rest = holding or (clamped and not self._clamp_watch)
        if (not rest) and at_lim and below >= self.loss_below_c:
            if not self._b_since:
                self._b_since = now
            self._b_win.append((now, pv))
            while self._b_win and self._b_win[0][0] < now - self.loss_sec:
                self._b_win.popleft()
            if (now - self._b_since) >= self.loss_sec and self._b_win:
                t0 = self._b_win[0][0]
                head = [p for t, p in self._b_win if t <= t0 + EDGE_SEC]
                tail = [p for t, p in self._b_win if t >= now - EDGE_SEC]
                if head and tail:
                    h, tl = sum(head) / len(head), sum(tail) / len(tail)
                    if (h - tl) >= self.loss_fall_c:
                        return self._trip({
                            "kind": "loss", "label": "출력 끊김", "pv": pv, "mv": mv, "limit": limit, "target": tgt,
                            "sec": now - self._b_since,
                            "text": (f"출력 상한(MV {mv:.0f}/{limit:.0f})인데 {(now - t0) / 60:.0f}분 동안 "
                                     f"PV {h:.1f}→{tl:.1f}°C 하강(목표 {tgt:.1f}°C)"),
                        })
        else:
            self._b_since = 0.0
            self._b_win.clear()
        return None

    def _trip(self, info: dict) -> dict:
        self._tripped = True
        info["hint"] = HINT + (" — 출력이 모자란 채 부하(가스 등)가 늘어도 같은 증상" if info.get("kind") == "loss" else "")
        return info

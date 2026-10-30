r"""OES 스펙트럼 → 화면(포털·노트북 화면 OES 그래프) — v1 controller/graph_controller.py update_oes_plot 의 거르기·쓰로틀을 장비 쪽에서.

v1 그래프 규칙(graph_controller.py:57-58·260-346, 그대로):
- 직전 그리기로부터 OES_PLOT_MIN_INTERVAL_S(0.5 s) 안 연속 호출은 건너뛴다(마지막 데이터만 의미 있음). 시각은 거르기 '전'에 적는다
  (v1 은 쓰로틀을 통과하면 바로 _oes_last_draw_ts 를 바꾸고, 그 뒤 점이 모자라 그리지 않아도 시각은 바뀐 채).
- 두 목록 중 짧은 길이까지, 2개 미만이면 그리지 않음. 숫자 아님·비유한값·x 범위(100~1200 nm) 밖은 버림, 남은 점 2개 미만이면 그리지
  않음, x 로 정렬. y 는 자르지 않는다(축 0~16000 은 화면이 고정).
v2 에서 더한 것 — 점 솎기(웹으로 1초마다 2048점을 보내지 않게):
- 상한 MAX_POINTS = 512 점(구간 256 개 × 그 구간의 최소·최대 점 — x 순서대로). 근거: 화면 그래프의 그리는 폭이 약 450~520 px
  (포털 SVG viewBox 520, v1 위젯 폭도 비슷) — 구간 하나가 약 2 px 라 선 모양이 2048점 그대로 그린 것과 같고, 구간마다 최대값을 남겨
  좁은 방출선 꼭대기가 사라지지 않는다(단순 n 번째 점 고르기는 꼭대기를 놓친다). 크기는 x 소수 2자리·y 소수 1자리로 JSON 약 6 KB.
- 상한 이하면 솎지 않는다(v1 과 같은 점). 원래 점 수는 n 으로 함께 보낸다.
이 결과(dict)는 링크가 이벤트 kind='spectrum' 으로 내고(oes_link._sample), 허브는 사건으로 보내지 않고 state.spectra[챔버] 에 마지막
것만 둔다(hub.py — 기록·사건 링 버퍼에 넣지 않음, system.TRANSIENT_KINDS). 측정 시작 때는 clear(v1 _safe_clear_oes_plot).
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

X_RANGE: Tuple[float, float] = (100.0, 1200.0)      # graph_controller.py:57 OES_X_RANGE
Y_RANGE: Tuple[float, float] = (0.0, 16000.0)       # :58 OES_Y_RANGE(화면 축 — 값은 자르지 않음)
DEFAULT_MIN_INTERVAL_S = 0.5                        # lib/config_common.py:248 OES_PLOT_MIN_INTERVAL_S
MAX_POINTS = 512                                    # 웹으로 보내는 점 상한(구간 256 × 최소·최대)


def clean(x: Sequence[Any], y: Sequence[Any]) -> Optional[List[Tuple[float, float]]]:
    """v1 update_oes_plot 의 거르기 — (x, y) 점 목록(x 정렬) 또는 None(그리지 않음)."""
    n = min(len(x or ()), len(y or ()))
    if n < 2:
        return None
    lo, hi = X_RANGE
    pts: List[Tuple[float, float]] = []
    for a, b in zip(list(x)[:n], list(y)[:n]):
        try:
            xf, yf = float(a), float(b)
        except (TypeError, ValueError):
            continue
        if not (math.isfinite(xf) and math.isfinite(yf)):
            continue
        if xf < lo or xf > hi:
            continue
        pts.append((xf, yf))
    if len(pts) < 2:
        return None
    pts.sort(key=lambda p: p[0])
    return pts


def thin(pts: Sequence[Tuple[float, float]], max_points: int = MAX_POINTS) -> List[Tuple[float, float]]:
    """상한 이하면 그대로, 넘으면 구간(max_points // 2 개)마다 최소·최대 점을 x 순서로(같은 점이면 하나)."""
    if len(pts) <= max_points:
        return list(pts)
    buckets = max(1, max_points // 2)
    out: List[Tuple[float, float]] = []
    n = len(pts)
    for b in range(buckets):
        seg = pts[b * n // buckets:(b + 1) * n // buckets]
        if not seg:
            continue
        i_lo = min(range(len(seg)), key=lambda i: seg[i][1])
        i_hi = max(range(len(seg)), key=lambda i: seg[i][1])
        for i in sorted({i_lo, i_hi}):
            out.append(seg[i])
    return out


def payload(pts: Sequence[Tuple[float, float]], chamber: str, t_wall: float, n_raw: int) -> Dict[str, Any]:
    """이벤트·state 에 싣는 모양 — x(nm, 소수 2자리)·y(세기, 소수 1자리)·n(거르기 전 점 수)·t(장비 시각 epoch)."""
    return {"chamber": chamber, "x": [round(p[0], 2) for p in pts], "y": [round(p[1], 1) for p in pts],
            "n": int(n_raw), "t": round(float(t_wall), 3)}


class SpectrumThrottle:
    """v1 쓰로틀(_oes_last_draw_ts) — offer(now, x, y) → 보낼 dict 또는 None. reset() 은 측정 시작 때(clear 와 함께)."""

    def __init__(self, min_interval_s: float = DEFAULT_MIN_INTERVAL_S, max_points: int = MAX_POINTS) -> None:
        self.min_interval_s = max(0.0, float(min_interval_s))
        self.max_points = int(max_points)
        self.last: float = 0.0
        self.sent = 0
        self.skipped = 0

    def reset(self) -> None:
        self.last = 0.0

    def offer(self, now: float, x: Sequence[Any], y: Sequence[Any], chamber: str, t_wall: float,
              min_interval_s: Optional[float] = None) -> Optional[Dict[str, Any]]:
        gap = self.min_interval_s if min_interval_s is None else max(0.0, float(min_interval_s))
        if self.last > 0.0 and (now - self.last) < gap:
            self.skipped += 1
            return None
        self.last = now if now > 0.0 else 1e-9          # v1: 0 은 '아직 안 그림' — 시각 0 에서도 다음 호출을 거르게
        pts = clean(x, y)
        if pts is None:
            return None
        self.sent += 1
        return payload(thin(pts, self.max_points), chamber, t_wall, min(len(x or ()), len(y or ())))

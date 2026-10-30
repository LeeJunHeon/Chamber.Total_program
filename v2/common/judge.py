"""시간 기반 지속 판정 (장비 공통) — "조건이 X 초 이어지면".

v1 은 장비마다 다른 주기로 횟수를 셌다(2 s 폴링 5회 = 10 s, 5 s 폴링 3회 = 15 s …). v2 는 1초 tick 이지만 횟수가 아니라
표본 시각으로 판정한다 — 판정 주기(1 s, 250 ms)가 바뀌어도 걸리는 시간은 같다.

표본 = (값을 만든 시각, 조건 참/거짓/모름)
- 참   : 처음 참이 된 시각(since)을 잡는다. 마지막 참 표본 시각 − since ≥ hold_s 면 성립.
- 거짓 : 초기화(since 지움).
- 모름(None): stale·fail·suppressed 표본 — 판정을 바꾸지 않는다(since 를 지우지도, 성립을 확정하지도 않음).
  통신이 끊겨 표본이 오지 않으면 성립하지 않는다 — 두절은 따로 두절 규칙이 잡는다.
- 늦게 온(late) 값은 자기 시각으로 쓴다. 이미 본 시각보다 오래된 표본은 버린다.
hold_s = 0 이면 첫 참 표본에서 바로 성립.
"""
from __future__ import annotations

from typing import Optional

from v2.common.snapshot import Quality

FRESH = (Quality.OK.value, Quality.LATE.value)


class Persist:
    def __init__(self, hold_s: float) -> None:
        self.hold_s = float(hold_s)
        self.since: Optional[float] = None
        self.last_true: Optional[float] = None
        self._last_t: Optional[float] = None

    def reset(self) -> None:
        self.since = self.last_true = self._last_t = None

    def update(self, t: float, cond: Optional[bool]) -> bool:
        if cond is None:
            return self.active
        if self._last_t is not None and t < self._last_t:
            return self.active                     # 오래된 표본(늦게 도착) — 순서를 거스르지 않는다
        self._last_t = t
        if cond:
            if self.since is None:
                self.since = t
            self.last_true = t
        else:
            self.since = self.last_true = None
        return self.active

    @property
    def active(self) -> bool:
        return self.since is not None and self.last_true is not None and self.last_true - self.since >= self.hold_s - 1e-9

    @property
    def held_s(self) -> float:
        return 0.0 if self.since is None else self.last_true - self.since


def sample(row: dict, block: str, field: Optional[str] = None):
    """스냅샷 행에서 (표본 시각, 값) — 품질이 ok·late 가 아니면 값 None(모름). 시각 = 행 시각 − 나이."""
    from datetime import datetime
    if field is not None and f"{block}.{field}.q" in row:
        q, age = row[f"{block}.{field}.q"], row[f"{block}.{field}.age_s"]
    else:
        q, age = row[f"{block}.q"], row[f"{block}.age_s"]
    t = datetime.fromisoformat(row["ts"]).timestamp() - (age or 0.0)
    if q not in FRESH:
        return t, None
    return t, (row.get(f"{block}.{field}") if field else None)

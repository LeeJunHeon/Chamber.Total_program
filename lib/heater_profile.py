# lib/heater_profile.py
# -*- coding: utf-8 -*-
"""히터 램프의 '감속 접근(approach)' 프로파일 — 순수 함수만 모아 둔다(챔버K lib/heater_profile.py 이식).

[왜]
  목표에 정속으로 도착하면 그 순간 히터에 잉여 전력이 남아 그대로 넘어간다(챔버K 2026-09-10 실측:
  6°C/min 도착 → +5°C 오버슈트). 도착 속도를 0 에 가깝게 줄이면 이 잉여가 사라진다.

[모델] 남은 거리에 선형 비례해 속도를 줄인다.
  남은 거리 d, 본 구간 속도 rate, 접근 구간 zone, 도착 속도 rmin:
      r(d) = rmin + (rate − rmin) · d / zone      (d < zone)
      r(d) = rate                                 (d ≥ zone)

[시간 적분] b ≡ (rate − rmin) / zone 이면 접근 구간 전체(d: Z→0)는 ln((rmin + b·Z) / rmin) / b 분.
  (12°C/min · zone 20 · rmin 1 이면 접근 구간에만 4.52분)
  거리 span 전체: (span − d₁)/rate + ln((rmin + b·d₁)/rmin)/b, d₁ = min(span, zone).
  rate ≤ rmin 이거나 zone ≤ 0 이면 span/rate.

[역산] ramp_minutes 는 rate 에 대해 단조 감소 → 이분법으로 본 구간 속도를 구한다.

이 모듈은 config·PLC·Qt 를 import 하지 않는다. zone·rmin 은 호출부(HeaterConfig)가 넘긴다.
"""
from __future__ import annotations

import math


def approach_rate(dist: float, rate: float, zone: float, rmin: float) -> float:
    """남은 거리 dist(°C)에서 지금 내야 할 속도(°C/min). 접근 구간 밖이면 rate 그대로."""
    z, m = float(zone), float(rmin)
    rate = float(rate)
    dist = float(dist)
    if rate <= m or z <= 0 or dist >= z:
        return rate
    return m + (rate - m) * max(0.0, dist) / z


def ramp_minutes(span: float, rate: float, zone: float, rmin: float) -> float:
    """거리 span(°C)을 rate(°C/min)로 올리는 데 걸리는 시간(분). 접근 구간 감속을 적분으로 반영한다."""
    z, m = float(zone), float(rmin)
    span = abs(float(span))
    rate = float(rate)
    if rate <= 0 or span <= 0:
        return 0.0
    if z <= 0 or rate <= m or m <= 0:
        return span / rate
    d1 = min(span, z)
    b = (rate - m) / z
    return (span - d1) / rate + math.log((m + b * d1) / m) / b


def solve_rate_for_minutes(span: float, minutes: float, zone: float, rmin: float) -> float:
    """span(°C)을 minutes(분)에 올리는 '본 구간' 속도(°C/min). 답은 항상 산술값(span/minutes) 이상이다."""
    z, m = float(zone), float(rmin)
    span = abs(float(span))
    minutes = float(minutes)
    if minutes <= 0 or span <= 0:
        return 0.0
    r0 = span / minutes
    if z <= 0 or r0 <= m:
        return r0                      # 감속이 걸리지 않는 영역 — 산술값이 곧 답
    lo = r0                            # 여기서는 ramp_minutes >= minutes
    hi = r0 * 10.0
    while ramp_minutes(span, hi, z, m) > minutes and hi < 600.0:
        hi *= 2.0
    hi = min(hi, 600.0)
    for _ in range(80):
        mid = (lo + hi) / 2.0
        if ramp_minutes(span, mid, z, m) > minutes:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0

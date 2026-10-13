# -*- coding: utf-8 -*-
"""lib/heater_profile.py — 감속 접근 프로파일(순수 함수). pytest / 직접 실행 모두 가능."""
import os
import sys
import math

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import pytest                                                   # noqa: E402
from lib.heater_profile import approach_rate, ramp_minutes, solve_rate_for_minutes   # noqa: E402

Z, M = 20.0, 1.0


def test_approach_rate_linear_in_zone():
    assert approach_rate(30.0, 12.0, Z, M) == 12.0              # 접근 구간 밖
    assert approach_rate(20.0, 12.0, Z, M) == 12.0              # 경계
    assert abs(approach_rate(10.0, 12.0, Z, M) - 6.5) < 1e-9    # 1 + 11×0.5
    assert approach_rate(0.0, 12.0, Z, M) == 1.0                # 도착 속도
    assert approach_rate(-3.0, 12.0, Z, M) == 1.0               # 음수 거리 → 0
    assert approach_rate(5.0, 0.8, Z, M) == 0.8                 # rate ≤ rmin → 감속 없음
    assert approach_rate(5.0, 12.0, 0.0, M) == 12.0             # zone 0 → 감속 없음


def test_ramp_minutes_approach_integral():
    # 12°C/min · zone 20 · rmin 1 → 접근 구간에만 약 4.52분 (ln(12)/0.55)
    t = ramp_minutes(20.0, 12.0, Z, M)
    assert abs(t - math.log(12.0) / 0.55) < 1e-9
    assert abs(t - 4.52) < 0.01
    # 접근 구간 밖은 정속: 100°C 면 80/12 + 4.52
    assert abs(ramp_minutes(100.0, 12.0, Z, M) - (80.0 / 12.0 + t)) < 1e-9
    assert ramp_minutes(10.0, 12.0, 0.0, M) == pytest.approx(10.0 / 12.0)
    assert ramp_minutes(0.0, 12.0, Z, M) == 0.0
    assert ramp_minutes(10.0, 0.0, Z, M) == 0.0


@pytest.mark.parametrize("span,rate", [(100.0, 12.0), (300.0, 6.0), (15.0, 30.0), (500.0, 60.0)])
def test_solve_rate_roundtrip(span, rate):
    minutes = ramp_minutes(span, rate, Z, M)
    r = solve_rate_for_minutes(span, minutes, Z, M)
    assert abs(r - rate) < 1e-6 * max(1.0, rate)
    assert r >= span / minutes                                 # 감속이 시간을 먹으므로 산술값 이상


def test_solve_rate_edges():
    assert solve_rate_for_minutes(0.0, 10.0, Z, M) == 0.0
    assert solve_rate_for_minutes(10.0, 0.0, Z, M) == 0.0
    assert solve_rate_for_minutes(10.0, 20.0, Z, M) == 0.5     # 산술값 ≤ rmin → 그대로


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

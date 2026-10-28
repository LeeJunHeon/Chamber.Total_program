# -*- coding: utf-8 -*-
"""ThicknessEndJudge 잡음 시뮬레이션 — 측정 = round((실제 + N(0,σ)) / 0.1) × 0.1 (장비 분해능 0.1 nm),
간격 0.5초 ± 0.1초, 실제 증착률 0.02~10 nm/s × σ 0~0.3 nm × 시드 여러 개, 목표 100 nm.
합격: 상한 도달 0건, 종료 시 실제 두께 − 목표 ≤ max(실제 증착률 × 1초, 0.3 nm) + 4σ, 목표 − 실제 두께 ≤ 4σ + 0.3 nm
(종료 = 목표 도달 즉시, 기준점 0 = 메인 셔터 열기 전 영점 확인).
실제 두께 = 영점(시각 0) 뒤 쌓인 두께. 직접 실행하면 σ별 최악 초과·일찍 끝남 표를 출력한다."""
import contextlib
import os
import random
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

from controller.thickness_process import ThicknessEndJudge, expected_seconds, max_time_s  # noqa: E402
from lib.thickness_config import ThicknessJudgeParams            # noqa: E402

P = ThicknessJudgeParams()
TARGET = 100.0
RATES = (0.02, 0.04, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0)
SIGMAS = (0.0, 0.1, 0.2, 0.3)
SEEDS = range(6)


def run(actual: float, sigma: float, seed: int, recipe=None):
    """(종료했나, 종료 시 실제 두께, 큰 튐 횟수)."""
    rng = random.Random(seed * 7919 + int(actual * 1000) + int(sigma * 100))
    recipe = actual if recipe is None else recipe
    j = ThicknessEndJudge(TARGET, recipe, P, start_t=0.0, base=0.0, seq0=0)
    limit = max_time_s(expected_seconds(TARGET, recipe), P)
    t, seq = 0.0, 0
    while t < limit:
        t += 0.5 + rng.uniform(-0.1, 0.1)
        seq += 1
        true_nm = actual * t
        meas = round(round((true_nm + rng.gauss(0.0, sigma)) / 0.1) * 0.1, 6)
        ev = j.tick({"seq": seq, "thickness_nm": meas, "rate_nm_s": actual, "crystal_ok": True,
                     "reset_seen": 0, "stale": False}, t)
        if ev is not None and ev.kind == "done":
            return True, true_nm, j.jump_count
        assert ev is None, ev
    return False, actual * t, j.jump_count


def _table():
    out = {}
    for sigma in SIGMAS:
        worst_over = (-1e9, None)
        worst_early = (-1e9, None)
        worst_sec = (-1e9, None)
        limits = 0
        for actual in RATES:
            for seed in SEEDS:
                done, real, _jumps = run(actual, sigma, seed)
                if not done:
                    limits += 1
                    continue
                over = real - TARGET
                if over > worst_over[0]:
                    worst_over = (over, actual)
                if -over > worst_early[0]:
                    worst_early = (-over, actual)
                if over / actual > worst_sec[0]:
                    worst_sec = (over / actual, actual)
        out[sigma] = (limits, worst_over, worst_early, worst_sec)
    return out


@pytest.mark.parametrize("sigma", SIGMAS)
@pytest.mark.parametrize("actual", RATES)
def test_noise_ends_near_target(actual, sigma):
    for seed in SEEDS:
        done, real, _jumps = run(actual, sigma, seed)
        assert done, (actual, sigma, seed, "상한 도달")
        assert real - TARGET <= max(actual * 1.0, 0.3) + 4 * sigma, (actual, sigma, seed, real)
        assert TARGET - real <= 4 * sigma + 0.3, (actual, sigma, seed, real)


@pytest.mark.parametrize("ratio", [1, 5, 20])
def test_recipe_rate_mismatch_with_noise(ratio):
    """레시피 dep_rate 가 실제의 1/ratio — 판정은 dep_rate 를 쓰지 않으므로 그대로 목표에서 끝난다(상한은 레시피 기준이라 넉넉)."""
    for actual in (0.1, 1.0):
        done, real, _j = run(actual, 0.2, 3, recipe=actual / ratio)
        assert done and -1.1 <= real - TARGET <= max(actual * 1.0, 0.3) + 0.8


if __name__ == "__main__":
    tab = _table()
    print(f"목표 {TARGET:g} nm, 증착률 {RATES} nm/s, 시드 {len(SEEDS)}개, 간격 0.5±0.1초, 분해능 0.1 nm")
    for sigma, (limits, (over, ov_r), (early, ea_r), (sec, sec_r)) in tab.items():
        print(f"σ={sigma:.1f} nm: 상한 도달 {limits}건 · 최악 초과 {over:+.2f} nm (증착률 {ov_r} nm/s) · "
              f"초과 최악 {sec:.1f}초 분량 (증착률 {sec_r} nm/s) · 최악 일찍 끝남 {max(0.0, early):.2f} nm (증착률 {ea_r} nm/s)")

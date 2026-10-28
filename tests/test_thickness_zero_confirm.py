# -*- coding: utf-8 -*-
"""ThicknessEndJudge 5차 — 영점 확인 후 기준점(영점 지연 '하락 재설정' 규칙 대체).
- 셔터 열 충격(영점 뒤 1·3·5 nm 하락 → 30초에 걸쳐 회복): 예전엔 하락을 영점 지연으로 오인해 떨어진 값을 기준점으로 잡고
  회복분을 증착으로 세어 일찍 끝났다(5 nm → 96.8 nm). 지금은 기준점 0(영점 시각) — 하락·회복은 장비 두께 그대로.
- 영점이 늦게 반영(1~3초 동안 이전 두께 50 nm) → 반영 확인 뒤 기준점 0. 영점이 끝내 안 먹음 → 5초 뒤 현재 값 기준점 + 경고.
- 영점 실패(zero_ok=False) → 첫 값 기준점. 기준점 전 통신 끊김·복귀. 빠른 증착률(10 nm/s)에서도 첫 값으로 영점 확인.
직접 실행하면 열 충격 시뮬레이션 표를 출력한다."""
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


def sim(rate=0.05, *, sigma=0.0, seed=0, dip=0.0, dip_at=1.0, recover_s=30.0, before=None, before_s=0.0,
        zero_ok=True, stuck=None, gap=None):
    """영점(시각 0) 뒤 장비 표시값을 만들어 판정기에 넣는다. (종료 이벤트, 종료 시 실제 두께, 판정기)
    dip: dip_at 초에 dip nm 떨어졌다가 recover_s 초에 걸쳐 회복(열 충격).
    before: 영점 반영 전 표시값(before_s 초 동안 — 영점 늦게 반영). stuck: 영점이 끝내 안 먹은 오프셋.
    gap: (시작 초, 끝 초) 동안 새 값 없음(통신 끊김)."""
    rng = random.Random(seed)
    j = ThicknessEndJudge(TARGET, rate, P, start_t=0.0, seq0=0, zero_ok=zero_ok)
    limit = max_time_s(expected_seconds(TARGET, rate), P)
    t, seq = 0.0, 0
    while t < limit:
        t = round(t + 0.5, 6)
        true_nm = rate * t
        if gap and gap[0] <= t < gap[1]:
            ev = j.tick({"seq": seq, "thickness_nm": None, "crystal_ok": True, "reset_seen": 0}, t)
            if ev is not None and ev.kind == "done":
                return ev, true_nm, j
            continue
        v = true_nm
        if dip and t >= dip_at:
            v -= dip * max(0.0, 1.0 - (t - dip_at) / recover_s)
        if before is not None and t < before_s:
            v = before
        if stuck is not None:
            v += stuck
        v = round(round((v + rng.gauss(0.0, sigma)) / 0.1) * 0.1, 6)
        seq += 1
        ev = j.tick({"seq": seq, "thickness_nm": v, "rate_nm_s": rate, "crystal_ok": True, "reset_seen": 0}, t)
        if ev is not None and ev.kind == "done":
            return ev, true_nm, j
    return None, rate * t, j


@pytest.mark.parametrize("dip", [1.0, 3.0, 5.0])
@pytest.mark.parametrize("sigma", [0.0, 0.1, 0.2])
@pytest.mark.parametrize("dip_at", [0.2, 1.0, 3.0])       # 0.2 = 첫 값부터 이미 하락 중
def test_thermal_shock_does_not_end_early(dip, sigma, dip_at):
    for seed in range(4):
        ev, real, j = sim(0.05, sigma=sigma, seed=seed, dip=dip, dip_at=dip_at)
        assert ev is not None and ev.kind == "done"
        assert real >= TARGET - (4 * sigma + 0.3), (dip, sigma, dip_at, seed, real)
        assert real - TARGET <= max(0.05 * 12.0, 1.0)
        assert j.base == 0.0 and not any("영점 확인 안 됨" in e for e in j.events)


@pytest.mark.parametrize("before_s", [1.0, 2.0, 3.0])
def test_late_zero_waits_then_base_zero(before_s):
    """영점 뒤 1~3초 동안 이전 두께 50 nm 가 읽히다 0 으로 — 그 값으로 판정하지 않고 반영 확인 뒤 기준점 0."""
    ev, real, j = sim(0.2, before=50.0, before_s=before_s)
    assert ev.kind == "done" and TARGET - 0.3 <= real <= TARGET + 0.2 * 12
    assert j.base == 0.0 and any("영점 확인" in e for e in j.events)


def test_late_zero_old_value_at_target_does_not_end_immediately():
    """이전 공정 두께가 목표와 같아도(100 nm) 영점 반영 전 값으로 끝내지 않는다."""
    ev, real, j = sim(0.2, before=100.0, before_s=3.0)
    assert ev.kind == "done" and real >= TARGET - 0.3


def test_zero_never_applied_uses_current_value_after_5s():
    ev, real, j = sim(0.2, stuck=50.0)
    assert ev.kind == "done"
    assert any("영점 확인 안 됨" in e for e in j.events)
    assert j.base == pytest.approx(50.0 + 0.2 * 5.0, abs=0.15)        # 5초 시점 값
    assert TARGET - 0.3 <= real - 0.2 * 5.0 <= TARGET + 0.2 * 12      # 기준점 이전 5초 분량은 빠진다(경고 상황)


def test_zero_failed_takes_first_value():
    ev, real, j = sim(0.2, stuck=50.0, zero_ok=False)
    assert ev.kind == "done" and j.base == pytest.approx(50.1, abs=0.05)
    assert not any("영점 확인" in e for e in j.events)


def test_values_before_base_are_not_judged():
    j = ThicknessEndJudge(10.0, 0.1, P, start_t=0.0, seq0=0)
    for i in range(1, 9):                                              # 4초 동안 이전 두께 20 nm(목표 이상)
        assert j.tick({"seq": i, "thickness_nm": 20.0, "crystal_ok": True, "reset_seen": 0}, i * 0.5) is None
    assert j.base is None and j.done_n == 0


def test_comm_lost_before_base_then_resume():
    """영점 뒤 기준점 전에 끊김 → 시간 대체 → 복귀: 영점 성공이면 기준점 0, 실패면 영구 시간 대체(기존 규칙)."""
    ev, real, j = sim(0.2, gap=(0.0, 30.0))
    assert ev.kind == "done" and j.base == 0.0 and TARGET - 0.3 <= real <= TARGET + 0.2 * 12
    ev, real, j = sim(0.2, gap=(0.0, 30.0), zero_ok=False)
    assert ev is None and j.permanent


@pytest.mark.parametrize("rate", [2.0, 5.0, 10.0])
def test_fast_rate_first_value_confirms_zero(rate):
    """빠른 증착률 — 첫 값이 이미 1 nm 를 넘어도 영점 뒤 증착 분량 안이면 영점 확인(5초 기다리다 그만큼 넘게 증착하지 않게)."""
    ev, real, j = sim(rate, sigma=0.1)
    assert ev.kind == "done" and j.base == 0.0
    assert not any("영점 확인 안 됨" in e for e in j.events)
    assert real - TARGET <= rate * 12.0


def test_negative_warning_uses_neg_warn_nm():
    ev, real, j = sim(0.05, dip=5.0, dip_at=1.0)
    assert any("음수" in e for e in j.events)                           # 기록만
    ev, real, j = sim(0.05, dip=1.0, dip_at=1.0)
    assert not any("음수" in e for e in j.events)


if __name__ == "__main__":
    print("열 충격(0.05 nm/s, 목표 100 nm, 30초 회복, 시드 4개) — 종료 시 실제 두께 최소/최대")
    for dip in (1.0, 3.0, 5.0):
        for sigma in (0.0, 0.1, 0.2):
            for dip_at in (0.2, 1.0, 3.0):
                reals = [sim(0.05, sigma=sigma, seed=s, dip=dip, dip_at=dip_at)[1] for s in range(4)]
                print(f"하락 {dip:g} nm · σ {sigma:g} · 하락 시작 {dip_at:g}초: {min(reals):.2f} ~ {max(reals):.2f} nm")

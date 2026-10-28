# -*- coding: utf-8 -*-
"""controller/thickness_process.py — ThicknessEndJudge 순수 판정(장비·Qt·asyncio 없음) + 모드 판정·상한 계산.
pytest / 직접 실행 모두 가능."""
import contextlib
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

from controller.thickness_process import (ThicknessEndJudge, expected_seconds, fmt_mmss,  # noqa: E402
                                          max_time_s, thickness_mode_of)
from lib.thickness_config import ThicknessJudgeParams            # noqa: E402

P = ThicknessJudgeParams()


class Feed:
    """0.5초마다 새 S 값을 넣는 도우미. s = 장비 두께(nm), 기준점 = 첫 값."""

    def __init__(self, target=1.0, dep=0.1, p=P, base=5.0):
        self.j = ThicknessEndJudge(target, dep, p, start_t=0.0, seq0=0, reset0=0)
        self.t = 0.0
        self.seq = 0
        self.s = base
        self.evs = []

    def st(self, **kw):
        d = {"seq": self.seq, "thickness_nm": self.s, "rate_nm_s": None, "crystal_ok": True,
             "reset_seen": 0, "stale": False}
        d.update(kw)
        return d

    def new(self, ds=0.0, dt=0.5, **kw):
        self.t += dt
        self.seq += 1
        self.s += ds
        ev = self.j.tick(self.st(**kw), self.t)
        self.evs.append(ev)
        return ev

    def same(self, dt=0.5, **kw):
        """새 값 없음(seq 그대로)."""
        self.t += dt
        ev = self.j.tick(self.st(**kw), self.t)
        self.evs.append(ev)
        return ev


def test_mode_and_limits():
    assert thickness_mode_of("100", True) and thickness_mode_of(50.0, True)
    assert not thickness_mode_of("100", False)          # 채널이 두께 모니터를 안 씀(CH1 등)
    for v in (None, "", "0", 0, -5, "abc"):
        assert not thickness_mode_of(v, True)
    assert expected_seconds(100, 0.05) == pytest.approx(2000)
    assert max_time_s(2000, P) == 4000                  # max(×2, +300)
    assert max_time_s(100, P) == 400                    # 짧은 공정은 +300초
    assert fmt_mmss(65.4) == "01:05" and fmt_mmss(None) == "--:--"


def test_normal_done_needs_two_consecutive():
    f = Feed(target=1.0)
    assert f.new(0.0) is None                           # 기준점
    for _ in range(19):                                 # 0.05 nm × 19 = 0.95
        assert f.new(0.05) is None
    ev = f.new(0.05)                                    # 1.00 — 첫 번째
    assert ev is None and f.j.done_n == 1
    ev = f.new(0.05)                                    # 두 번째 → 종료
    assert ev is not None and ev.kind == "done" and ev.eff_nm == pytest.approx(1.05)


def test_same_seq_is_ignored():
    f = Feed(target=0.2)
    f.new(0.0)
    f.new(0.25)                                         # 목표 이상 1회
    assert f.j.done_n == 1
    f.s += 1.0
    for _ in range(5):                                  # 값이 바뀌어도 seq 가 같으면 판정하지 않음
        assert f.same() is None
    assert f.j.done_n == 1
    assert f.new(0.0).kind == "done"


def test_step_jump_over_target_then_falls_back_continues():
    f = Feed(target=3.0)
    f.new(0.0)
    for _ in range(20):
        f.new(0.05)                                     # 1.0 nm, 기준 증착률 0.1 nm/s
    assert f.new(5.0) is None and f.j.suspect is not None and f.j.suspect_count == 1   # 튐으로 목표 초과
    assert f.new(0.05) is None                          # 아직 의심 — 끝내지 않음
    assert f.new(-5.0) is None and f.j.suspect is None  # 다시 떨어짐 → 의심 취소, 계속
    assert f.j.eff == pytest.approx(1.05)
    assert any("계단 튐 의심" in e for e in f.j.events) and any("취소" in e for e in f.j.events)
    ev = None
    while ev is None:
        ev = f.new(0.05)
    assert ev.kind == "done" and f.j.eff >= 3.0


def test_step_jump_then_three_normals_is_excluded_not_ended():
    """한 번 튀고 그대로 유지되는 측정 오류(오프셋) — 예전엔 정상 증가 3회 뒤 바로 종료(일찍 끝남). 지금은 초과분을 뺀다."""
    f = Feed(target=3.0)
    f.new(0.0)
    for _ in range(20):
        f.new(0.05)                                     # 유효 1.0
    assert f.new(5.0) is None                           # 튐 — 표시상 목표 초과
    assert f.new(0.05) is None and f.new(0.05) is None
    assert f.new(0.05) is None                          # 정상 3회 → 제외, 종료하지 않음
    assert f.j.suspect is None and f.j.step_excluded_nm == pytest.approx(5.0 - 0.1 * 0.5)
    assert f.j.eff == pytest.approx(1.0 + 0.05 + 0.15) and any("계단 튐 제외" in e for e in f.j.events)
    ev = None
    while ev is None:
        ev = f.new(0.05)
    true_nm = f.s - 5.0 - 5.0                           # 표시값 − 기준점 − 튐
    assert ev.kind == "done" and f.j.eff >= 3.0 and f.j.eff == pytest.approx(true_nm + 0.05)   # 튄 구간 0.5초 증착 0.05 만 남김


def test_step_detection_skipped_in_first_seconds():
    f = Feed(target=50.0)
    f.new(0.0)
    f.new(1.0)                                          # 기준점 0.5초 뒤 큰 증가 — 처음 3초는 판정 제외
    assert f.j.suspect is None and f.j.suspect_count == 0


def test_late_zero_resets_base():
    f = Feed(target=1.0, base=50.0)                     # 영점이 아직 반영 안 된 값이 기준점이 됨
    f.new(0.0)
    f.new(0.05)
    assert f.new(-49.0) is None                         # 10초 안 2 nm 이상 하락 → 기준점 재설정
    assert f.j.base == pytest.approx(1.05) and f.j.eff == 0.0
    assert any("영점 지연" in e for e in f.j.events)
    f.new(0.05)
    assert f.j.eff == pytest.approx(0.05)
    # 10초가 지난 뒤의 하락은 재설정하지 않는다
    g = Feed(target=100.0, base=10.0)
    g.new(0.0)
    for _ in range(25):
        g.new(0.0)
    g.new(-5.0)
    assert g.j.base == 10.0


def test_stale_fallback_then_resume_and_done_on_resume():
    f = Feed(target=2.0, dep=0.1)
    f.new(0.0)
    for _ in range(10):
        f.new(0.05)                                     # 유효 0.5 nm, 5초
    ev = None
    n = 0
    while ev is None:
        ev = f.same(dt=1.0)
        n += 1
    assert ev.kind == "fallback" and not ev.permanent and n == 11      # 10초 넘게 새 값 없음
    assert ev.remaining_s == pytest.approx((2.0 - 0.5) / 0.1)            # 30초 전 → 레시피 dep_rate
    assert f.same() is None                             # 이미 시간 대체
    ev = f.new(0.3)                                     # 통신 복귀(리셋 없음)
    assert ev.kind == "resume" and f.j.mode == "thickness" and ev.eff_nm == pytest.approx(0.8)
    # 끊긴 동안 목표를 넘었으면 복귀 순간 종료
    for _ in range(11):
        f.same(dt=1.0)
    assert f.j.mode == "fallback"
    ev = f.new(5.0)
    assert ev.kind == "done" and ev.reason == "복귀 시 이미 목표 이상"


@pytest.mark.parametrize("kw,word", [({"crystal_ok": False}, "XTAL"), ({"reset_seen": 1}, "리셋")])
def test_xtal_fail_or_reset_is_permanent(kw, word):
    f = Feed(target=2.0)
    f.new(0.0)
    f.new(0.05)
    ev = f.new(0.05, **kw)
    assert ev.kind == "fallback" and ev.permanent and word in ev.reason
    for _ in range(5):
        assert f.new(0.05) is None                      # 복귀하지 않음
    assert f.j.mode == "fallback" and f.j.permanent


def test_stale_then_xtal_fail_becomes_permanent_without_new_event():
    f = Feed(target=2.0)
    f.new(0.0)
    for _ in range(11):
        f.same(dt=1.0)
    assert f.j.mode == "fallback" and not f.j.permanent
    assert f.same(crystal_ok=False) is None and f.j.permanent
    assert f.new(0.5) is None and f.j.mode == "fallback"


def test_fallback_rate_measured_after_30s():
    f = Feed(target=20.0, dep=0.1)
    f.new(0.0)
    for _ in range(80):                                 # 40초, 실측 0.2 nm/s
        f.new(0.1)
    ev = None
    while ev is None:
        ev = f.same(dt=1.0)
    assert f.j.use_rate()[1] == "실측" and f.j.use_rate()[0] == pytest.approx(0.2)
    assert ev.remaining_s == pytest.approx((20.0 - 8.0) / 0.2)
    g = Feed(target=20.0, dep=0.1)                      # 20초만 측정 → 레시피 값
    g.new(0.0)
    for _ in range(40):
        g.new(0.1)
    assert g.j.use_rate() == (0.1, "레시피")


def test_judge_exception_falls_back():
    f = Feed(target=2.0)
    f.new(0.0)
    f.seq += 1
    f.t += 0.5
    ev = f.j.tick(f.st(thickness_nm="not-a-number"), f.t)
    assert ev.kind == "fallback" and ev.permanent and "판정 코드 예외" in ev.reason
    assert "Traceback" in ev.detail
    assert f.new(0.05) is None


def test_warnings_only():
    f = Feed(target=100.0, dep=0.1)
    f.new(0.0)
    for _ in range(130):                                # 65초 동안 늘지 않음
        assert f.new(0.0) is None
    assert any("늘지 않음" in e for e in f.j.events)
    assert f.new(0.05, rate_nm_s=1.0) is None           # 레시피 × 5 초과 증착률
    assert any("급등" in e for e in f.j.events)


def test_view_text():
    f = Feed(target=10.0, dep=0.1)
    f.new(0.0)
    for _ in range(10):
        f.new(0.05)
    text, pct = f.j.view({"rate_nm_s": 0.1}, f.t)
    assert text == "두께 0.5 / 10.0 nm (5%) · 증착률 0.100 nm/s · 예상 남은 시간 01:35" and pct == pytest.approx(5.0)


# ═══════════════════════ 3차: 증착률 비율·계단 튐 제외·기준점 없는 복귀·분해능 ═══════════════════════
def simulate(target, recipe, actual, *, dt=0.5, res=None, jumps=None, p=P, stop_after_s=None):
    """장비 시뮬레이션 — 영점(0) 뒤 actual nm/s 로 쌓이고, jumps={시각(s): +nm} 로 측정값이 튐(그대로 유지).
    res 면 장비 분해능(0.1 nm 등)으로 반올림. (종료 이벤트, 종료 시각, 실제 증착 두께, 판정기)."""
    j = ThicknessEndJudge(target, recipe, p, start_t=0.0, seq0=0)
    limit = max_time_s(expected_seconds(target, recipe), p)
    offset, t, seq = 0.0, 0.0, 0
    jumps = dict(jumps or {})
    while t < limit:
        t = round(t + dt, 6)
        seq += 1
        for jt in [k for k in jumps if k <= t]:
            offset += jumps.pop(jt)
        true_nm = actual * t
        s = true_nm + offset
        if res:
            s = round(round(s / res) * res, 6)
        ev = j.tick({"seq": seq, "thickness_nm": s, "rate_nm_s": actual, "crystal_ok": True,
                     "reset_seen": 0, "stale": False}, t)
        if ev is not None and ev.kind == "done":
            return ev, t, true_nm, j
        if stop_after_s and t >= stop_after_s:
            break
    return None, t, actual * t, j


@pytest.mark.parametrize("recipe", [0.05, 0.2, 0.5])
@pytest.mark.parametrize("ratio", [1, 3, 5, 10, 15])
@pytest.mark.parametrize("res", [None, 0.1])
def test_rate_ratio_ends_near_target(recipe, ratio, res):
    """실제/레시피 증착률 비 1~15× — 예전엔 5× 이상에서 의심 상태를 못 빠져나와 상한까지 갔다(목표 100 nm → ~1000 nm)."""
    target = 100.0
    actual = recipe * ratio
    ev, t, true_nm, j = simulate(target, recipe, actual, res=res)
    assert ev is not None, (recipe, ratio, j.events[-3:])
    assert target - 0.2 <= true_nm <= target + 2.0 * actual + 0.2, (true_nm, j.events[-3:])
    assert j.step_excluded_nm == 0.0                      # 실제 증착률 변화는 제외하지 않는다


def test_reproduced_cases_1_0_and_3_0():
    for recipe, actual in ((0.2, 1.0), (0.5, 3.0)):
        ev, t, true_nm, j = simulate(100.0, recipe, actual)
        assert ev.kind == "done" and true_nm <= 100.0 + 2 * actual


@pytest.mark.parametrize("recipe,before,after", [(0.2, 0.2, 1.0), (0.5, 0.5, 3.0), (0.05, 0.05, 0.75)])
def test_mid_run_rate_increase_is_rate_change_not_glitch(recipe, before, after):
    """공정 중 실제 증착률이 갑자기 4배 넘게 오름 — 의심 → 한계 초과가 6초 지속 → 증착률 변화(제외 없음), 목표에서 종료."""
    j = ThicknessEndJudge(100.0, recipe, P, start_t=0.0, seq0=0)
    t, s, seq, ev = 0.0, 0.0, 0, None
    while ev is None and t < 2000:
        t += 0.5
        seq += 1
        s += (before if s < 30.0 else after) * 0.5
        ev = j.tick({"seq": seq, "thickness_nm": round(s, 1), "crystal_ok": True, "reset_seen": 0}, t)
    assert ev is not None and ev.kind == "done" and s <= 100.0 + 2 * after + 0.2
    assert any("증착률 변화로 판정" in e for e in j.events) and j.step_excluded_nm == 0.0


def test_single_glitch_is_excluded_and_ends_at_real_target():
    ev, t, true_nm, j = simulate(100.0, 0.2, 0.2, jumps={200.0: 5.0})
    assert ev.kind == "done" and 99.8 <= true_nm <= 100.0 + 0.4 + 0.2
    assert j.step_excluded_nm == pytest.approx(5.0, abs=0.05) and j.suspect_count == 1   # 초과분 = d − 기준×Δt = 튐 크기


def test_glitch_over_target_does_not_end_early():
    """97 nm 근처에서 +5 nm 튀어 표시가 목표를 넘고 그대로 유지 — 실제 두께가 목표에 닿을 때 끝난다."""
    ev, t, true_nm, j = simulate(100.0, 0.2, 0.2, jumps={485.0: 5.0})
    assert ev.kind == "done" and true_nm >= 99.8 and j.step_excluded_nm > 4.8


def test_glitch_that_falls_back_is_not_excluded():
    ev, t, true_nm, j = simulate(100.0, 0.2, 0.2, jumps={200.0: 5.0, 201.0: -5.0})
    assert ev.kind == "done" and j.step_excluded_nm == 0.0 and 99.8 <= true_nm <= 100.6
    assert any("다시 떨어짐" in e for e in j.events)


def test_exclusion_over_5pct_warns_once():
    ev, t, true_nm, j = simulate(100.0, 0.2, 0.2, jumps={100.0: 3.0, 200.0: 3.0, 300.0: 3.0})
    assert ev.kind == "done" and j.step_excluded_nm > 5.0
    notes = j.pop_notices()
    assert len(notes) == 1 and "확인 필요" in notes[0] and j.pop_notices() == []


def test_resolution_and_slow_rate():
    """0.1 nm 분해능 + 0.04 nm/s 저속 — 계단 튐 오판정 없이 목표에서 끝난다."""
    ev, t, true_nm, j = simulate(10.0, 0.04, 0.04, res=0.1)
    assert ev.kind == "done" and 9.9 <= true_nm <= 10.0 + 2 * 0.04 + 0.1
    assert j.suspect_count == 0 and j.step_excluded_nm == 0.0


@pytest.mark.parametrize("zero_ok", [True, False])
def test_resume_without_base(zero_ok):
    """영점 직후 첫 값 전에 끊김 → 30 nm 쌓인 뒤 복귀. 예전: 기준점 30 nm → 목표 + 30 nm 증착."""
    j = ThicknessEndJudge(100.0, 1.0, P, start_t=0.0, seq0=0, zero_ok=zero_ok)
    t = 0.0
    ev = None
    while ev is None:                                   # 새 값 없음 → 시간 대체
        t += 1.0
        ev = j.tick({"seq": 0, "thickness_nm": None, "crystal_ok": True, "reset_seen": 0}, t)
    assert ev.kind == "fallback" and not ev.permanent
    ev = j.tick({"seq": 1, "thickness_nm": 30.0, "crystal_ok": True, "reset_seen": 0}, 30.0)
    if zero_ok:
        assert ev.kind == "resume" and j.base == 0.0 and j.eff == pytest.approx(30.0)
        assert j.use_rate()[0] == pytest.approx(1.0)     # 기준점 시각 = 영점 시각(30 nm / 30 s)
        # 끊긴 동안 이미 목표를 넘었으면 복귀 순간 종료
        k = ThicknessEndJudge(100.0, 1.0, P, start_t=0.0, seq0=0, zero_ok=True)
        for tt in range(1, 13):
            k.tick({"seq": 0, "thickness_nm": None, "crystal_ok": True, "reset_seen": 0}, float(tt))
        assert k.mode == "fallback"
        ev = k.tick({"seq": 1, "thickness_nm": 100.2, "crystal_ok": True, "reset_seen": 0}, 101.0)
        assert ev.kind == "done"
    else:
        assert ev is None and j.permanent and j.mode == "fallback" and "기준점" in j.fallback_reason
        assert j.tick({"seq": 2, "thickness_nm": 31.0, "crystal_ok": True, "reset_seen": 0}, 31.0) is None


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

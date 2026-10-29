# -*- coding: utf-8 -*-
"""controller/thickness_process.py — ThicknessEndJudge 순수 판정(장비·Qt·asyncio 없음) + 모드 판정·상한 계산.
종료 규칙: 유효두께(장비 두께 − 기준점) ≥ 목표 인 새 값이 THICKNESS_DONE_CONSECUTIVE(기본 1 = 도달 즉시) 번 연속. 큰 튐은 기록만.
기준점은 메인 셔터 열기 전 THICKNESS_ZERO 에서 정해 판정기에 넘긴다(ZeroConfirm — tests/test_thickness_zero_confirm.py).
잡음 시뮬레이션은 tests/test_thickness_judge_noise.py. pytest / 직접 실행 모두 가능."""
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
import dataclasses                                               # noqa: E402

from lib.thickness_config import ThicknessJudgeParams            # noqa: E402

P = ThicknessJudgeParams()
P3 = dataclasses.replace(P, done_consecutive=3)                  # 연속 N 회 구조(설정으로 되돌릴 수 있게 남긴 것) 시험용


class Feed:
    """0.5초마다 새 S 값을 넣는 도우미. s = 장비 두께(nm), 기준점 0(영점 확인됨)."""

    def __init__(self, target=1.0, dep=0.1, p=P, s0=0.0):
        self.j = ThicknessEndJudge(target, dep, p, start_t=0.0, base=0.0, seq0=0)
        self.t = 0.0
        self.seq = 0
        self.s = s0
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


def test_defaults():
    assert P.done_consecutive == 1 and P.jump_min_nm == 1.0 and P.jump_rate_k == 10.0
    assert P.zero_confirm_nm == 2.0 and P.zero_confirm_s == 5.0
    for gone in ("step_skip_s", "step_rate_k", "step_min_nm", "ref_rate_n", "step_normal_n", "step_elev_s",
                 "step_exclude_warn_frac"):
        assert not hasattr(P, gone)


def test_mode_and_limits():
    assert thickness_mode_of("100", True) and thickness_mode_of(50.0, True)
    assert not thickness_mode_of("100", False)          # 채널이 두께 모니터를 안 씀(CH1 등)
    for v in (None, "", "0", 0, -5, "abc"):
        assert not thickness_mode_of(v, True)
    assert expected_seconds(100, 0.05) == pytest.approx(2000)
    assert max_time_s(2000, P) == 4000                  # max(×2, +300)
    assert max_time_s(100, P) == 400                    # 짧은 공정은 +300초
    assert fmt_mmss(65.4) == "01:05" and fmt_mmss(None) == "--:--"


def test_default_ends_at_first_value_at_target():
    f = Feed(target=1.0)
    f.new(0.0)
    for _ in range(19):
        assert f.new(0.05) is None
    ev = f.new(0.05)                                    # 1.00 — 목표 도달 즉시
    assert ev.kind == "done" and ev.eff_nm == pytest.approx(1.0)


def test_done_needs_three_consecutive():
    f = Feed(target=1.0, p=P3)
    assert f.new(0.0) is None                           # 기준점
    for _ in range(19):                                 # 0.05 nm × 19 = 0.95
        assert f.new(0.05) is None
    assert f.new(0.05) is None and f.j.done_n == 1      # 1.00 — 첫 번째(부동소수점 오차 허용)
    assert f.new(0.05) is None and f.j.done_n == 2
    ev = f.new(0.05)                                    # 세 번째 → 종료
    assert ev is not None and ev.kind == "done" and ev.eff_nm == pytest.approx(1.10)


def test_below_target_resets_count():
    f = Feed(target=1.0, p=P3)
    f.new(0.0)
    f.new(1.0)
    f.new(0.0)
    assert f.j.done_n == 2
    assert f.new(-0.1) is None and f.j.done_n == 0      # 하나라도 목표 미만이면 다시 센다
    for _ in range(2):
        assert f.new(0.1) is None
    assert f.new(0.0).kind == "done"


def test_same_seq_is_ignored():
    f = Feed(target=0.2, p=P3)
    f.new(0.0)
    f.new(0.25)                                         # 목표 이상 1회
    assert f.j.done_n == 1
    f.s += 1.0
    for _ in range(5):                                  # 값이 바뀌어도 seq 가 같으면 판정하지 않음
        assert f.same() is None
    assert f.j.done_n == 1
    assert f.new(0.0) is None
    assert f.new(0.0).kind == "done"


def test_single_spike_near_target_does_not_end_early():
    """(연속 3회 설정) 목표 근처 한 표본만 +5 nm — 다음 값이 목표 미만이면 다시 세어 일찍 끝나지 않는다. 큰 튐은 기록만.
    기본(1회)은 도달 즉시 끝나는 쪽을 택했다(사용자 결정) — 스파이크 대비가 필요하면 THICKNESS_DONE_CONSECUTIVE 를 올린다."""
    f = Feed(target=100.0, dep=0.2, p=P3)
    f.new(0.0)
    while f.j.eff < 99.0:
        assert f.new(0.1) is None
    assert f.new(5.0) is None and f.j.done_n == 1 and f.j.jump_count == 1
    assert f.new(-5.0 + 0.1) is None and f.j.done_n == 0
    assert any("큰 튐" in e and "기록만" in e for e in f.j.events)
    ev = None
    while ev is None:
        ev = f.new(0.1)
    assert ev.kind == "done" and 100.0 <= f.j.eff <= 100.35


def test_step_offset_that_stays_ends_on_device_value():
    """튄 값이 그대로 유지되면 장비 두께 기준으로 그대로 끝낸다(프로그램은 장비 계산 두께를 믿는다) — 큰 튐은 기록."""
    f = Feed(target=100.0, dep=0.2, p=P3)
    f.new(0.0)
    while f.j.eff < 97.0:
        f.new(0.1)
    f.new(5.0)
    assert f.new(0.1) is None
    ev = f.new(0.1)
    assert ev.kind == "done" and f.j.jump_count == 1


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
    assert ev.remaining_s == pytest.approx((2.0 - 0.5) / 0.1 - 11.0)     # 30초 전 → 레시피 dep_rate, 마지막 값 뒤 11초 뺌
    assert f.same() is None                             # 이미 시간 대체
    ev = f.new(0.3)                                     # 통신 복귀(리셋 없음)
    assert ev.kind == "resume" and f.j.mode == "thickness" and ev.eff_nm == pytest.approx(0.8)
    for _ in range(11):
        f.same(dt=1.0)
    assert f.j.mode == "fallback"
    ev = f.new(5.0)                                     # 끊긴 동안 목표를 넘었으면 복귀 순간 종료
    assert ev.kind == "done" and ev.reason == "복귀 시 이미 목표 이상"


@pytest.mark.parametrize("kw,word", [({"crystal_ok": False}, "XTAL"), ({"reset_seen": 1}, "리셋")])
def test_xtal_fail_or_reset_is_permanent(kw, word):
    f = Feed(target=2.0)
    f.new(0.0)
    f.new(0.05)
    ev = f.new(0.05, **kw)
    assert ev.kind == "fallback" and ev.permanent and word in ev.reason
    for _ in range(5):
        assert f.new(0.5) is None                       # 복귀하지 않음(목표를 넘어도)
    assert f.j.mode == "fallback" and f.j.permanent


def test_xtal_fail_seen_while_values_missing_if_link_ok():
    """XTAL FAIL 이면 장비가 두께 대신 빈칸 — stale 이어도 통신(link_ok)이 되면 XTAL FAIL 영구 시간 대체."""
    f = Feed(target=2.0)
    f.new(0.0)
    ev = f.same(stale=True, link_ok=True, crystal_ok=False)
    assert ev.kind == "fallback" and ev.permanent and "XTAL" in ev.reason
    g = Feed(target=2.0)                                # 통신이 안 되면 마지막 M 값은 믿지 않는다
    g.new(0.0)
    assert g.same(stale=True, link_ok=False, crystal_ok=False) is None and not g.j.permanent


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
    rate = 8.0 / 40.5                                   # 기준점 시각 = 영점 시각(0) — 영점 뒤 40.5초에 8.0 nm
    assert f.j.use_rate()[1] == "실측" and f.j.use_rate()[0] == pytest.approx(rate)
    assert ev.remaining_s == pytest.approx((20.0 - 8.0) / rate - (f.t - 40.5))    # 마지막 값 뒤 경과를 뺀다
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
    assert f.new(5.0) is None


def test_no_base_is_permanent_fallback_from_start():
    """영점 확인 중 두께 값이 없어 기준점이 없으면 처음부터 영구 시간 대체(예상시간) — 셔터 연 뒤 값이 와도 판정하지 않는다."""
    j = ThicknessEndJudge(100.0, 0.5, P, start_t=0.0, base=None, seq0=0)
    ev = j.start_event()
    assert ev.kind == "fallback" and ev.permanent and ev.remaining_s == pytest.approx(200.0) and "기준점" in ev.reason
    assert j.tick({"seq": 1, "thickness_nm": 500.0, "crystal_ok": True, "reset_seen": 0}, 1.0) is None
    assert ThicknessEndJudge(100.0, 0.5, P, start_t=0.0, base=0.0).start_event() is None


def test_base_from_zero_step_is_subtracted():
    """영점 확인 안 됨(그때 값 50 nm 가 기준점) — 늘어난 양으로 판정."""
    j = ThicknessEndJudge(10.0, 0.5, P, start_t=0.0, base=50.0, seq0=0)
    assert j.tick({"seq": 1, "thickness_nm": 55.0, "crystal_ok": True, "reset_seen": 0}, 10.0) is None
    assert j.eff == pytest.approx(5.0)
    assert j.tick({"seq": 2, "thickness_nm": 60.0, "crystal_ok": True, "reset_seen": 0}, 20.0).kind == "done"


def test_warnings_only_do_not_change_judgement():
    f = Feed(target=100.0, dep=0.1)
    f.new(0.0)
    for _ in range(130):                                # 65초 동안 늘지 않음
        assert f.new(0.0) is None
    assert any("늘지 않음" in e for e in f.j.events)
    assert f.new(0.05, rate_nm_s=1.0) is None           # 레시피 × 5 초과 증착률
    assert any("급등" in e for e in f.j.events)
    assert f.new(2.0) is None and f.j.jump_count == 1   # 큰 튐: > max(1.0, 0.1 × 10 × 0.5)
    assert f.new(0.9) is None and f.j.jump_count == 1   # 1.0 이하는 큰 튐 아님
    assert f.j.eff == pytest.approx(2.95)               # 판정 값은 장비 두께 그대로
    g = Feed(target=100.0, dep=0.1)
    g.new(0.0)
    g.new(-1.5)
    assert not any("음수" in e for e in g.j.events)    # −THICKNESS_NEG_WARN_NM(2.0) 까지는 경고 없음
    g.new(-1.0)
    assert any("음수" in e for e in g.j.events)


@pytest.mark.parametrize("recipe,actual", [(0.05, 0.05), (0.05, 1.0), (0.2, 4.0), (0.5, 0.025), (1.0, 20.0), (0.1, 2.0)])
def test_recipe_rate_does_not_affect_end(recipe, actual):
    """판정은 dep_rate 를 쓰지 않는다 — 실제가 레시피의 1/20~20배여도 목표에서 끝난다(상한 안)."""
    target = 100.0
    j = ThicknessEndJudge(target, recipe, P, start_t=0.0, base=0.0, seq0=0)
    limit = max_time_s(expected_seconds(target, recipe), P)
    t, seq, ev = 0.0, 0, None
    while ev is None and t < limit:
        t += 0.5
        seq += 1
        ev = j.tick({"seq": seq, "thickness_nm": round(actual * t, 1), "rate_nm_s": actual, "crystal_ok": True,
                     "reset_seen": 0}, t)
    if actual * limit < target + actual:                 # 레시피가 실제보다 훨씬 빠르면 상한이 먼저 온다(정상 종료 절차)
        assert ev is None
        return
    assert ev is not None and ev.kind == "done"
    assert target - 0.05 <= actual * t <= target + actual * 0.5 + 0.1     # 0.1 nm 반올림 + 도달 즉시 종료


def test_view_text():
    f = Feed(target=10.0, dep=0.1)
    f.new(0.0)
    for _ in range(10):
        f.new(0.05)
    text, pct = f.j.view({"rate_nm_s": 0.1}, f.t)
    assert text == "두께 0.5 / 10.0 nm (5%) · 증착률 0.100 nm/s · 예상 남은 시간 01:35" and pct == pytest.approx(5.0)
    assert "증착률 0.091 nm/s" in f.j.view({"rate_nm_s": None}, f.t)[0]           # 장비 값 없으면 실측 평균(0.5 nm / 영점 뒤 5.5초)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

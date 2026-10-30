"""두께 판정(순수) — v1 tests/test_thickness_judge.py·test_thickness_judge_noise.py·test_thickness_zero_confirm.py(순수 부분)·
test_thickness_round14.py(증착률 상식 범위·남은 시간) 를 v2 로 옮긴 것(v1 import 없음).

기대값은 v1 시험의 값 그대로(근거: v1 tests/test_thickness_judge.py 줄 번호를 각 시험 독스트링에). 판정 코드는
v2/ch12/thickness_judge.py — v1 controller/thickness_process.py 를 다시 쓴 것.
"""
import dataclasses
import random

import pytest

from v2.common.paramdef import defaults
from v2.ch12.params import core_thickness, stm100
from v2.ch12.thickness_judge import (RATE_SANE_MAX, RATE_SANE_MIN, JudgeParams, ThicknessEndJudge, ZeroConfirm,
                                     expected_seconds, fmt_mmss, judge_params, max_time_s, thickness_mode_of)

P = JudgeParams()
P3 = dataclasses.replace(P, done_consecutive=3)
P2 = dataclasses.replace(P, done_consecutive=2)


class Feed:
    """0.5초마다 새 S 값을 넣는 도우미(v1 test_thickness_judge.py:28-56). s = 장비 두께(nm), 기준점 0."""

    def __init__(self, target=1.0, dep=0.1, p=P, s0=0.0):
        self.j = ThicknessEndJudge(target, dep, p, start_t=0.0, base=0.0, seq0=0)
        self.t = 0.0
        self.seq = 0
        self.s = s0

    def st(self, **kw):
        d = {"seq": self.seq, "thickness_nm": self.s, "rate_nm_s": None, "crystal_ok": True, "reset_seen": 0, "stale": False}
        d.update(kw)
        return d

    def new(self, ds=0.0, dt=0.5, **kw):
        self.t += dt
        self.seq += 1
        self.s += ds
        return self.j.tick(self.st(**kw), self.t)

    def same(self, dt=0.5, **kw):
        self.t += dt
        return self.j.tick(self.st(**kw), self.t)


# ═══════════════════════ 설정·보조(v1 :59-75, :347-357) ═══════════════════════
def test_defaults():
    assert P.done_consecutive == 1 and P.jump_min_nm == 1.0 and P.jump_rate_k == 10.0
    assert P.zero_confirm_nm == 2.0 and P.zero_confirm_s == 5.0
    for gone in ("step_skip_s", "step_rate_k", "step_min_nm", "ref_rate_n", "step_normal_n"):
        assert not hasattr(P, gone)


def test_mode_and_limits():
    assert thickness_mode_of("100", True) and thickness_mode_of(50.0, True)
    assert not thickness_mode_of("100", False)
    for v in (None, "", "0", 0, -5, "abc"):
        assert not thickness_mode_of(v, True)
    assert expected_seconds(100, 0.05) == pytest.approx(2000)
    assert max_time_s(2000, P) == 4000 and max_time_s(100, P) == 400
    assert fmt_mmss(65.4) == "01:05" and fmt_mmss(None) == "--:--"


def test_params_from_cfg_ch2_done_consecutive_is_2_and_ch1_is_1():
    """15차 커밋 88455cf — CH2 settings.json 2, CH1 키 없음 → 공통 기본 1(v1 test_settings_json_ch2_done_consecutive_is_2)."""
    cfg = defaults(core_thickness.PARAMS + stm100.PARAMS)
    p2, w2 = judge_params(cfg, "ch2")
    p1, w1 = judge_params(cfg, 1)
    assert p2.done_consecutive == 2 and p1.done_consecutive == 1 and w1 == w2 == []
    assert (p2.stale_s, p2.ready_fresh_s, p2.zero_confirm_s, p2.max_time_margin_s) == (10.0, 5.0, 5.0, 300.0)


def test_params_clamp_and_format_like_v1():
    """lib/thickness_config.py:69-82 — bool·글자는 형식 오류(기본값), 범위 밖은 클램프, done_consecutive 정수."""
    p, w = judge_params({"CH2_THICKNESS_DONE_CONSECUTIVE": 20, "CH2_THICKNESS_STALE_S": True,
                         "CH2_THICKNESS_ZERO_CONFIRM_S": "x", "THICKNESS_JUMP_MIN_NM": 0.01}, "ch2")
    assert p.done_consecutive == 10 and isinstance(p.done_consecutive, int)
    assert p.stale_s == 10.0 and p.zero_confirm_s == 5.0 and p.jump_min_nm == 0.1     # 공통 키도 읽는다
    assert len(w) == 4 and any("형식 오류" in x for x in w) and any("범위(1~10) 밖 → 10" in x for x in w)


# ═══════════════════════ 종료 규칙(v1 :78-148) ═══════════════════════
def test_default_ends_at_first_value_at_target():
    f = Feed(target=1.0)
    f.new(0.0)
    for _ in range(19):
        assert f.new(0.05) is None
    ev = f.new(0.05)
    assert ev.kind == "done" and ev.eff_nm == pytest.approx(1.0)


def test_done_needs_three_consecutive():
    f = Feed(target=1.0, p=P3)
    assert f.new(0.0) is None
    for _ in range(19):
        assert f.new(0.05) is None
    assert f.new(0.05) is None and f.j.done_n == 1
    assert f.new(0.05) is None and f.j.done_n == 2
    ev = f.new(0.05)
    assert ev is not None and ev.kind == "done" and ev.eff_nm == pytest.approx(1.10)


def test_below_target_resets_count():
    f = Feed(target=1.0, p=P3)
    f.new(0.0)
    f.new(1.0)
    f.new(0.0)
    assert f.j.done_n == 2
    assert f.new(-0.1) is None and f.j.done_n == 0
    for _ in range(2):
        assert f.new(0.1) is None
    assert f.new(0.0).kind == "done"


def test_same_seq_is_ignored():
    f = Feed(target=0.2, p=P3)
    f.new(0.0)
    f.new(0.25)
    assert f.j.done_n == 1
    f.s += 1.0
    for _ in range(5):
        assert f.same() is None
    assert f.j.done_n == 1
    assert f.new(0.0) is None
    assert f.new(0.0).kind == "done"


def test_single_spike_near_target_does_not_end_early():
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
    f = Feed(target=100.0, dep=0.2, p=P3)
    f.new(0.0)
    while f.j.eff < 97.0:
        f.new(0.1)
    f.new(5.0)
    assert f.new(0.1) is None
    ev = f.new(0.1)
    assert ev.kind == "done" and f.j.jump_count == 1


# ═══════════════════════ 시간 대체·복귀(v1 :151-244) ═══════════════════════
def test_stale_fallback_then_resume_and_done_on_resume():
    f = Feed(target=2.0, dep=0.1)
    f.new(0.0)
    for _ in range(10):
        f.new(0.05)
    ev, n = None, 0
    while ev is None:
        ev = f.same(dt=1.0)
        n += 1
    assert ev.kind == "fallback" and not ev.permanent and n == 11
    assert ev.remaining_s == pytest.approx((2.0 - 0.5) / 0.1 - 11.0)
    assert f.same() is None
    ev = f.new(0.3)
    assert ev.kind == "resume" and f.j.mode == "thickness" and ev.eff_nm == pytest.approx(0.8)
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
        assert f.new(0.5) is None
    assert f.j.mode == "fallback" and f.j.permanent


def test_xtal_fail_seen_while_values_missing_if_link_ok():
    f = Feed(target=2.0)
    f.new(0.0)
    ev = f.same(stale=True, link_ok=True, crystal_ok=False)
    assert ev.kind == "fallback" and ev.permanent and "XTAL" in ev.reason
    g = Feed(target=2.0)
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
    for _ in range(80):
        f.new(0.1)
    ev = None
    while ev is None:
        ev = f.same(dt=1.0)
    rate = 8.0 / 40.5
    assert f.j.use_rate()[1] == "실측" and f.j.use_rate()[0] == pytest.approx(rate)
    assert ev.remaining_s == pytest.approx((20.0 - 8.0) / rate - (f.t - 40.5))
    g = Feed(target=20.0, dep=0.1)
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
    assert ev.kind == "fallback" and ev.permanent and "판정 코드 예외" in ev.reason and "Traceback" in ev.detail
    assert f.new(5.0) is None


def test_no_base_is_permanent_fallback_from_start():
    j = ThicknessEndJudge(100.0, 0.5, P, start_t=0.0, base=None, seq0=0)
    ev = j.start_event()
    assert ev.kind == "fallback" and ev.permanent and ev.remaining_s == pytest.approx(200.0) and "기준점" in ev.reason
    assert j.tick({"seq": 1, "thickness_nm": 500.0, "crystal_ok": True, "reset_seen": 0}, 1.0) is None
    assert ThicknessEndJudge(100.0, 0.5, P, start_t=0.0, base=0.0).start_event() is None


def test_base_from_zero_step_is_subtracted():
    j = ThicknessEndJudge(10.0, 0.5, P, start_t=0.0, base=50.0, seq0=0)
    assert j.tick({"seq": 1, "thickness_nm": 55.0, "crystal_ok": True, "reset_seen": 0}, 10.0) is None
    assert j.eff == pytest.approx(5.0)
    assert j.tick({"seq": 2, "thickness_nm": 60.0, "crystal_ok": True, "reset_seen": 0}, 20.0).kind == "done"


# ═══════════════════════ 기록만 하는 경고·dep_rate·문구(v1 :252-302) ═══════════════════════
def test_warnings_only_do_not_change_judgement():
    f = Feed(target=100.0, dep=0.1)
    f.new(0.0)
    for _ in range(130):
        assert f.new(0.0) is None
    assert any("늘지 않음" in e for e in f.j.events)
    assert f.new(0.05, rate_nm_s=1.0) is None
    assert any("급등" in e for e in f.j.events)
    assert f.new(2.0) is None and f.j.jump_count == 1
    assert f.new(0.9) is None and f.j.jump_count == 1
    assert f.j.eff == pytest.approx(2.95)
    g = Feed(target=100.0, dep=0.1)
    g.new(0.0)
    g.new(-1.5)
    assert not any("음수" in e for e in g.j.events)
    g.new(-1.0)
    assert any("음수" in e for e in g.j.events)


@pytest.mark.parametrize("recipe,actual", [(0.05, 0.05), (0.05, 1.0), (0.2, 4.0), (0.5, 0.025), (1.0, 20.0), (0.1, 2.0)])
def test_recipe_rate_does_not_affect_end(recipe, actual):
    target = 100.0
    j = ThicknessEndJudge(target, recipe, P, start_t=0.0, base=0.0, seq0=0)
    limit = max_time_s(expected_seconds(target, recipe), P)
    t, seq, ev = 0.0, 0, None
    while ev is None and t < limit:
        t += 0.5
        seq += 1
        ev = j.tick({"seq": seq, "thickness_nm": round(actual * t, 1), "rate_nm_s": actual, "crystal_ok": True,
                     "reset_seen": 0}, t)
    if actual * limit < target + actual:
        assert ev is None
        return
    assert ev is not None and ev.kind == "done"
    assert target - 0.05 <= actual * t <= target + actual * 0.5 + 0.1


def test_view_text():
    f = Feed(target=10.0, dep=0.1)
    f.new(0.0)
    for _ in range(10):
        f.new(0.05)
    text, pct = f.j.view({"rate_nm_s": 0.1}, f.t)
    assert text == "두께 0.5 / 10.0 nm (5%) · 증착률 0.100 nm/s · 예상 남은 시간 01:35" and pct == pytest.approx(5.0)
    assert "증착률 0.091 nm/s" in f.j.view({"rate_nm_s": None}, f.t)[0]


# ═══════════════════════ 15차 — 2번 연속(v1 :305-344) ═══════════════════════
def test_consecutive2_single_spike_then_two_in_a_row():
    f = Feed(target=100.0, dep=0.2, p=P2)
    f.new(0.0)
    f.new(50.0)
    assert f.new(592207.9 - 50.0) is None and f.j.done_n == 1          # 주파수 응답이 두께로 읽힘 — 종료 안 함
    assert f.new(50.5 - 592207.9) is None and f.j.done_n == 0
    while f.j.eff < 99.0 - 1e-9:
        assert f.new(0.5) is None
    assert f.new(1.2) is None and f.j.done_n == 1
    ev = f.new(0.2)
    assert ev.kind == "done" and f.j.done_n == 2


def _fallback_feed(p):
    f = Feed(target=2.0, dep=0.1, p=p)
    f.new(0.0)
    f.new(0.5)
    for _ in range(11):
        f.same(dt=1.0)
    assert f.j.mode == "fallback"
    return f


@pytest.mark.parametrize("nxt,end", [(0.0, True), (-3.0, False)])
def test_consecutive2_resume_first_value_over_target_needs_next(nxt, end):
    f = _fallback_feed(P2)
    ev = f.new(2.0)
    assert ev.kind == "resume" and f.j.mode == "thickness" and f.j.done_n == 1
    assert "첫 값이 목표 이상" in f.j.events[-1] and "다음 값으로 확인" in f.j.events[-1]
    ev2 = f.new(nxt)
    if end:
        assert ev2.kind == "done"
    else:
        assert ev2 is None and f.j.done_n == 0 and f.j.mode == "thickness"


def test_consecutive1_resume_over_target_ends_at_once():
    f = _fallback_feed(P)
    ev = f.new(2.0)
    assert ev.kind == "done" and ev.reason == "복귀 시 이미 목표 이상"


# ═══════════════════════ 14차 — 증착률 상식 범위·남은 시간(v1 test_thickness_round14.py:146-180) ═══════════════════════
def _judge_with_rate(k, dep=0.1, span=40.0):
    j = ThicknessEndJudge(100.0, dep, P, start_t=0.0, base=0.0, seq0=0)
    j._set_eff(dep * k * span, span)
    return j


@pytest.mark.parametrize("k,src", [(0.3, "레시피"), (1.0, "실측"), (3.0, "레시피")])
def test_use_rate_sane_range(k, src):
    rate, s = _judge_with_rate(k).use_rate()
    assert s == src and rate == pytest.approx(0.1 * (k if src == "실측" else 1.0))
    assert (RATE_SANE_MIN, RATE_SANE_MAX) == (0.5, 2.0)


def test_out_of_range_rate_is_logged_on_fallback():
    j = _judge_with_rate(3.0)
    ev = j._to_fallback(40.0, "새 두께 값 없음", permanent=False)
    msg = j.events[-1]
    assert ev.kind == "fallback" and "레시피 0.1000 nm/s" in msg and "실측 0.3000 nm/s" in msg and "0.5~2배 밖" in msg


def test_fallback_remaining_subtracts_time_since_last_value():
    j = _judge_with_rate(1.0)
    ev = j._to_fallback(52.0, "새 두께 값 없음 12초", permanent=False)
    assert ev.remaining_s == pytest.approx((100.0 - 4.0) / 0.1 - 12.0) and "마지막 값 뒤 12초 반영" in j.events[-1]
    j2 = ThicknessEndJudge(5.0, 0.1, P, start_t=0.0, base=0.0, seq0=0)
    j2._set_eff(4.5, 10.0)
    assert j2._to_fallback(22.0, "x", permanent=False).remaining_s == 0.0
    j3 = ThicknessEndJudge(10.0, 0.1, P, start_t=0.0, base=None, seq0=0)
    ev3 = j3.start_event()
    assert ev3.remaining_s == pytest.approx(100.0) and "마지막 값 뒤" not in j3.events[-1]


# ═══════════════════════ 잡음 시뮬(v1 test_thickness_judge_noise.py) ═══════════════════════
TARGET = 100.0
RATES = (0.02, 0.04, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0)
SIGMAS = (0.0, 0.1, 0.2, 0.3)


def _noise_run(actual, sigma, seed, recipe=None):
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
        ev = j.tick({"seq": seq, "thickness_nm": meas, "rate_nm_s": actual, "crystal_ok": True, "reset_seen": 0,
                     "stale": False}, t)
        if ev is not None and ev.kind == "done":
            return True, true_nm
        assert ev is None, ev
    return False, actual * t


@pytest.mark.parametrize("sigma", SIGMAS)
@pytest.mark.parametrize("actual", RATES)
def test_noise_ends_near_target(actual, sigma):
    for seed in range(6):
        done, real = _noise_run(actual, sigma, seed)
        assert done, (actual, sigma, seed, "상한 도달")
        assert real - TARGET <= max(actual * 1.0, 0.3) + 4 * sigma
        assert TARGET - real <= 4 * sigma + 0.3


@pytest.mark.parametrize("ratio", [1, 5, 20])
def test_recipe_rate_mismatch_with_noise(ratio):
    for actual in (0.1, 1.0):
        done, real = _noise_run(actual, 0.2, 3, recipe=actual / ratio)
        assert done and -1.1 <= real - TARGET <= max(actual * 1.0, 0.3) + 0.8


# ═══════════════════════ 영점 확인 ZeroConfirm(v1 test_thickness_zero_confirm.py:33-96) ═══════════════════════
def test_zero_confirm_xtal_fail_is_immediate():
    zc = ZeroConfirm(P, start_t=0.0, seq0=0, zero_ok=True)
    r = zc.tick({"seq": 0, "thickness_nm": None, "crystal_ok": False, "link_ok": True, "stale": True}, 0.1)
    assert r is not None and r.base is None and "XTAL FAIL" in r.log and "XTAL FAIL" in r.warning
    zc = ZeroConfirm(P, start_t=0.0, seq0=0, zero_ok=True)
    assert zc.tick({"seq": 0, "crystal_ok": False, "link_ok": False, "stale": True}, 0.1) is None


def _confirm(values, *, zero_ok=True, dt=0.5, gap_after=None):
    zc = ZeroConfirm(P, start_t=0.0, seq0=0, zero_ok=zero_ok)
    t, seq = 0.0, 0
    while t < 20:
        t = round(t + dt, 6)
        if gap_after is not None and seq >= gap_after:
            r = zc.tick({"seq": seq, "thickness_nm": None}, t)
        else:
            seq += 1
            r = zc.tick({"seq": seq, "thickness_nm": values[min(seq - 1, len(values) - 1)]}, t)
        if r is not None:
            return r, t
    raise AssertionError("결과 없음")


def test_immediate_zero():
    r, t = _confirm([0.0])
    assert r.base == 0.0 and t == 0.5 and r.warning == "" and "영점 확인" in r.log


@pytest.mark.parametrize("n_old", [1, 2, 3])
def test_old_thickness_then_zero(n_old):
    r, t = _confirm([350.0] * n_old + [0.1])
    assert r.base == 0.0 and t == 0.5 * (n_old + 1) and r.warning == ""


@pytest.mark.parametrize("v", [-3.0, 1.5, 2.0])
def test_negative_and_small_positive_confirm(v):
    assert _confirm([v])[0].base == 0.0


def test_not_confirmed_in_5s_uses_last_value_with_warning():
    r, t = _confirm([50.0, 50.1, 49.9, 50.2])
    assert t == pytest.approx(5.0) and r.base == pytest.approx(50.2)
    assert r.warning.startswith("영점 확인 안 됨 — 50.20 nm 를 기준점으로")


def test_zero_command_failed_takes_first_value_with_warning():
    r, t = _confirm([350.0, 0.0], zero_ok=False)
    assert r.base == 350.0 and t == 0.5 and r.warning.startswith("영점 명령 실패 — 첫 값 350.00 nm")


def test_no_values_means_no_base():
    r, t = _confirm([0.0], gap_after=0)
    assert r.base is None and t == pytest.approx(5.0) and "기준점 없음" in r.warning
    assert _confirm([0.0], zero_ok=False, gap_after=0)[0].base is None


def test_values_lost_after_old_reading_uses_last():
    r, _t = _confirm([350.0], gap_after=1)
    assert r.base == 350.0 and "영점 확인 안 됨" in r.warning


# ═══════════════════════ 셔터 연 뒤 열 충격(v1 test_thickness_zero_confirm.py:102-138) ═══════════════════════
def _shock(rate=0.05, *, sigma=0.0, seed=0, dip=0.0, dip_at=1.0, recover_s=30.0):
    rng = random.Random(seed)
    j = ThicknessEndJudge(TARGET, rate, P, start_t=0.0, base=0.0, seq0=0)
    limit = max_time_s(expected_seconds(TARGET, rate), P)
    t, seq = 0.0, 0
    while t < limit:
        t = round(t + 0.5, 6)
        true_nm = rate * t
        v = true_nm
        if dip and t >= dip_at:
            v -= dip * max(0.0, 1.0 - (t - dip_at) / recover_s)
        v = round(round((v + rng.gauss(0.0, sigma)) / 0.1) * 0.1, 6)
        seq += 1
        ev = j.tick({"seq": seq, "thickness_nm": v, "rate_nm_s": rate, "crystal_ok": True, "reset_seen": 0}, t)
        if ev is not None and ev.kind == "done":
            return ev, true_nm, j
    return None, rate * t, j


@pytest.mark.parametrize("dip", [1.0, 3.0, 5.0])
@pytest.mark.parametrize("sigma", [0.0, 0.1, 0.2])
@pytest.mark.parametrize("dip_at", [0.2, 1.0, 3.0])
def test_thermal_shock_does_not_end_early(dip, sigma, dip_at):
    for seed in range(4):
        ev, real, _j = _shock(0.05, sigma=sigma, seed=seed, dip=dip, dip_at=dip_at)
        assert ev is not None and ev.kind == "done"
        assert real >= TARGET - (4 * sigma + 0.3)
        assert real - TARGET <= max(0.05 * 1.0, 0.3) + 4 * sigma


def test_negative_warning_uses_neg_warn_nm():
    assert any("음수" in e for e in _shock(0.05, dip=5.0, dip_at=1.0)[2].events)
    assert not any("음수" in e for e in _shock(0.05, dip=1.0, dip_at=1.0)[2].events)


# ═══════════════════════ v2: 저널 상태 되살리기 ═══════════════════════
def test_judge_state_roundtrip_with_new_clock():
    """to_state/from_state — 메인 셔터 연 시각 기준 상대 시각. 되살린 판정기는 같은 값에 같은 판정을 한다."""
    f = Feed(target=10.0, dep=0.1, p=P2)
    f.new(0.0)
    for _ in range(40):
        f.new(0.2)                                                   # 8.0 nm, 20.5 s
    st = f.j.to_state()
    j2 = ThicknessEndJudge.from_state(st, t0=1000.0, seq0=-1, reset0=0)
    assert j2.eff == pytest.approx(8.0) and j2.eff == f.j.eff
    assert (j2.done_n, j2.mode, j2.dep_rate, j2.base, j2.p.done_consecutive) == (0, "thickness", 0.1, 0.0, 2)
    assert j2.eff_t == pytest.approx(1000.0 + f.j.eff_t) and j2.last_new_t == pytest.approx(1000.0 + f.j.last_new_t)
    assert j2.tick({"seq": 1, "thickness_nm": 10.0, "crystal_ok": True, "reset_seen": 0}, 1021.0) is None
    assert j2.tick({"seq": 2, "thickness_nm": 10.1, "crystal_ok": True, "reset_seen": 0}, 1021.5).kind == "done"


# ═══════════════════════ v1 실행 골든(v2/tools/v1golden_thickness.py → golden/thickness.json) ═══════════════════════
import json                                                          # noqa: E402
from pathlib import Path                                             # noqa: E402

G = json.loads((Path(__file__).parent / "golden" / "thickness.json").read_text(encoding="utf-8"))


def _ev(e):
    if e is None:
        return None
    return {"kind": e.kind, "reason": e.reason, "remaining_s": e.remaining_s, "permanent": e.permanent,
            "eff_nm": e.eff_nm, "has_detail": bool(e.detail)}


@pytest.mark.parametrize("sc", G["judge"], ids=[s["name"] for s in G["judge"]])
def test_judge_same_events_and_logs_as_v1(sc):
    """v1 ThicknessEndJudge 를 같은 입력으로 실제로 돌린 결과(이벤트·로그·끝 상태)와 같다."""
    p = dataclasses.replace(P, **sc["p"])
    j = ThicknessEndJudge(sc["target"], sc["dep"], p, start_t=0.0, base=sc["base"], seq0=0, reset0=0)
    assert _ev(j.start_event()) == sc["start"]
    assert [_ev(j.tick(st, t)) for t, st in sc["ticks"]] == sc["events"]
    assert j.events == sc["logs"]
    fin = {"eff": j.eff, "done_n": j.done_n, "jump_count": j.jump_count, "mode": j.mode, "permanent": j.permanent,
           "fallback_count": j.fallback_count, "avg_rate": j.avg_rate(), "use_rate": list(j.use_rate())}
    assert fin == sc["final"]


@pytest.mark.parametrize("sc", G["zero"], ids=[s["name"] for s in G["zero"]])
def test_zero_confirm_same_as_v1(sc):
    zc = ZeroConfirm(P, start_t=0.0, seq0=0, zero_ok=sc["zero_ok"])
    res = None
    for i, (t, st) in enumerate(sc["ticks"]):
        r = zc.tick(st, t)
        if r is not None:
            res = {"index": i, "base": r.base, "log": r.log, "warning": r.warning}
            break
    assert res == sc["result"]

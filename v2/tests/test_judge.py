"""시간 기반 지속 판정 시험 — 판정 주기가 바뀌어도 걸리는 시간은 같다, stale·fail 표본 처리."""
import pytest

from v2.common.judge import Persist, sample


@pytest.mark.parametrize("period", [0.25, 1.0, 2.0, 5.0])
def test_same_time_regardless_of_period(period):
    p = Persist(15.0)
    t, fired = 0.0, None
    while t <= 40.0:
        if p.update(t, True) and fired is None:
            fired = t
        t += period
    assert 15.0 - 1e-9 <= fired < 15.0 + period             # 15 s 를 넘는 첫 표본(주기 하나 안) — 횟수가 아니라 시각


def test_false_resets_unknown_keeps():
    p = Persist(5.0)
    for t in range(0, 3):
        p.update(t, True)
    p.update(3, None)                                       # stale·fail — 지우지도 확정하지도 않음
    p.update(4, None)
    assert not p.active and p.since == 0
    assert p.update(5, True)                                # 0 ~ 5 s 참 → 성립
    p.update(6, False)
    assert not p.active and p.since is None
    for t in range(7, 20):
        p.update(t, None)                                   # 표본이 계속 없으면 성립하지 않는다(두절은 따로)
    assert not p.active


def test_zero_hold_and_old_samples():
    p = Persist(0.0)
    assert p.update(10.0, True)
    q = Persist(3.0)
    q.update(10.0, True)
    q.update(13.0, True)
    assert q.active
    q.update(12.0, False)                                   # 늦게 온 오래된 표본 — 무시
    assert q.active


def test_sample_from_row():
    row = {"ts": "2026-10-16T10:00:05", "a.q": "late", "a.age_s": 1.0, "a.x": 3.0,
           "b.q": "stale", "b.age_s": 4.0, "b.y": 9.0, "c.q": "ok", "c.age_s": 0.0, "c.z": 1.0,
           "c.z.q": "stale", "c.z.age_s": 1.0}
    from datetime import datetime
    t0 = datetime.fromisoformat(row["ts"]).timestamp()
    assert sample(row, "a", "x") == (t0 - 1.0, 3.0)        # late 는 자기 시각으로 쓴다
    assert sample(row, "b", "y") == (t0 - 4.0, None)       # stale 은 모름
    assert sample(row, "c", "z") == (t0 - 1.0, None)       # 필드별 품질 우선

"""가스 도달 판정 연속 N 회(MFC_FLOW_STABLE_COUNT, v1 main 27decba) — v2/ch12/drivers/mfc_ops.stab_loop.

v1 tests/test_mfc_flow_stable_count.py 와 같은 경우를 v2 로 옮겼다.
- 앞부분(각본): stab_loop 생성기를 직접 돌리고 R60 응답을 각본으로 준다(v1 이 _read_r60_values 를 각본으로 바꾼 것과 같은 자리).
- 뒷부분(시험대): 실제 드라이버 + MFC 시뮬레이터 + 가상 시계(test_mfc.Rig) — FLOW_ON·재 FLOW_ON·플라즈마 클리닝 flow_set_on·설정 오류.
예전: 범위 안 한 번이면 확정. 지금: 연속 N(기본 3)회. 범위 밖·읽기 실패면 0 부터, 확정 안 된 회차가 180 이면 'GAS 안정화 시간 초과'.
"""
from __future__ import annotations

import re
from types import SimpleNamespace

import pytest

from v2.ch12.drivers import mfc_ops as ops
from v2.ch12.drivers.line_ops import Sleep
from v2.ch12.params import mfc as P

IN, OUT = 20.0, 15.0          # 목표 20(±5% = 19~21) 안 / 밖


class FakeD:
    """stab_loop 이 쓰는 것만: c()·status()·emit()·hw_to_ui()·query()·clock·stab_jobs."""

    def __init__(self, **cfg):
        self.cfg = dict(FLOW_ERROR_TOLERANCE=0.05, MFC_STABILIZATION_INTERVAL_MS=1000, **cfg)
        self.stab_jobs: dict = {}
        self.statuses: list = []
        self.events: list = []
        self.t = 0.0
        self.clock = SimpleNamespace(monotonic=lambda: self.t)

    def c(self, key, default=None):
        return self.cfg[key] if key in self.cfg else default

    def hw_to_ui(self, ch, v):
        return v

    def status(self, msg, owner=None, channel=None):
        self.statuses.append((channel, msg))

    def emit(self, kind, detail="", **kw):
        self.events.append((kind, detail, kw))

    def query(self, cmd, prefixes, tag):
        return ("query", cmd, tag)

    def add(self, ch, target, owner):
        self.stab_jobs[ch] = ops.StabJob(ch, float(target), owner, self.t)

    def results(self, channel=None):
        return [(k, d, kw) for k, d, kw in self.events
                if k in ("command_confirmed", "command_failed") and kw.get("cmd") == "FLOW_ON"
                and (channel is None or kw.get("channel") == channel)]

    def gas(self, ch):
        return [m for c, m in self.statuses if c == ch and m.startswith(f"GAS 확인... ch{ch} ")]


def _line(vals):
    """각본 항목 → R60 응답 줄. None = 읽기 실패(응답 없음), [] = 'Q0' 뿐(채널 값 없음)."""
    if vals is None:
        return None
    return "Q0" + "".join(f" {v:+.2f}" for v in vals)


def drive(d: FakeD, script, on_read=None, max_reads=1000) -> int:
    """stab_loop 을 끝까지 돌린다. 각본이 끝나면 마지막 항목 반복. 돌려준 값 = R60 읽은 횟수."""
    gen = ops.stab_loop(d)
    reads = 0
    try:
        item = next(gen)
        while reads <= max_reads:
            if isinstance(item, Sleep):
                d.t += item.seconds
                item = gen.send(None)
                continue
            if on_read:
                on_read(reads)
            vals = script[min(reads, len(script) - 1)]
            reads += 1
            item = gen.send(_line(vals))
    except StopIteration:
        pass
    return reads


def _streak(msgs, need=3):
    return [re.search(rf"\[연속 (\d+)/{need}\]$", m).group(1) for m in msgs]


# ───────────────────────── 기본 규칙: 연속 3회, 밖이면 끊김 ─────────────────────────
def test_in_in_out_in_in_in_confirms_on_6th_read():
    d = FakeD(MFC_FLOW_STABLE_COUNT=3)
    d.add(1, 20.0, "chamber1")
    n = drive(d, [[v, 0, 0] for v in (IN, IN, OUT, IN, IN, IN, OUT)])
    res = d.results()
    assert [r[0] for r in res] == ["command_confirmed"] and res[0][2]["channel"] == 1 and res[0][2]["owner"] == "chamber1"
    assert n == 6                                                  # 6번째 읽기에서 확정(7번째는 읽지 않음)
    msgs = d.gas(1)
    assert _streak(msgs) == ["1", "2", "0", "1", "2", "3"]
    assert msgs[0] == "GAS 확인... ch1 (목표: 20.00sccm, 현재: 20.00sccm) [연속 1/3]"    # 앞부분 문구는 예전 그대로 + [연속 n/N]
    assert msgs[2] == "GAS 확인... ch1 (목표: 20.00sccm, 현재: 15.00sccm) [연속 0/3]"
    assert 1 not in d.stab_jobs


def test_three_in_a_row_needed_not_one():
    d = FakeD(MFC_FLOW_STABLE_COUNT=3)
    d.add(1, 20.0, "chamber1")
    n = drive(d, [[IN, 0, 0], [IN, 0, 0], [OUT, 0, 0]])           # 두 번 들어오고 계속 밖
    assert [(r[0], r[1], r[2]["reason"]) for r in d.results()] == [("command_failed", "GAS 안정화 시간 초과", "STAB_TIMEOUT")]
    assert n == 180


def test_read_failure_breaks_streak():
    d = FakeD(MFC_FLOW_STABLE_COUNT=3)
    d.add(1, 20.0, "chamber1")
    # 안·안·읽기 실패(None)·안·안·채널 값 없음([])·안·안·안 → 9번째에서 확정
    n = drive(d, [[IN, 0, 0], [IN, 0, 0], None, [IN, 0, 0], [IN, 0, 0], [], [IN, 0, 0], [IN, 0, 0], [IN, 0, 0]])
    assert [r[0] for r in d.results()] == ["command_confirmed"] and n == 9
    msgs = d.gas(1)
    assert _streak(msgs) == ["1", "2", "0", "1", "2", "0", "1", "2", "3"]
    assert "현재: -1.00sccm" in msgs[2]                             # 읽기 실패 표기는 예전 그대로


def test_always_out_times_out_at_180():
    d = FakeD(MFC_FLOW_STABLE_COUNT=3)
    d.add(1, 20.0, "chamber1")
    assert drive(d, [[OUT, 0, 0]]) == 180
    res = d.results()
    assert [r[0] for r in res] == ["command_failed"] and res[0][1] == "GAS 안정화 시간 초과"


def test_in_but_short_of_streak_counts_as_attempt():
    """범위 안이지만 연속 횟수 미달인 회차도 attempts 에 든다 → 안·안·밖 반복이면 180회째 시간 초과(무한 대기 없음)."""
    d = FakeD(MFC_FLOW_STABLE_COUNT=3)
    d.add(1, 20.0, "chamber1")
    assert drive(d, [[IN, 0, 0], [IN, 0, 0], [OUT, 0, 0]] * 70) == 180
    assert [(r[0], r[1]) for r in d.results()] == [("command_failed", "GAS 안정화 시간 초과")]


def test_two_channels_count_separately():
    d = FakeD(MFC_FLOW_STABLE_COUNT=3)
    # ch1(목표 20): 안·안·안 → 3번째 확정 / ch3(목표 30, ±1.5): 안·밖·안·안·안 → 5번째 확정
    d.add(1, 20.0, "chamber1")
    d.add(3, 30.0, "pc2")
    n = drive(d, [[IN, 0, 30.0], [IN, 0, 10.0], [IN, 0, 30.0], [OUT, 0, 30.0], [OUT, 0, 30.0], [OUT, 0, 10.0]])
    assert [(r[0], r[2]["owner"]) for r in d.results(1)] == [("command_confirmed", "chamber1")]
    assert [(r[0], r[2]["owner"]) for r in d.results(3)] == [("command_confirmed", "pc2")]
    assert len(d.gas(1)) == 3 and len(d.gas(3)) == 5
    assert n == 5                                                  # 한 번 읽기로 두 채널을 함께 판정
    assert _streak(d.gas(3)) == ["1", "0", "1", "2", "3"]


def test_job_created_during_read_is_not_judged_that_round():
    """읽는 동안 새로 생긴 작업(created > t_read)은 그 회차에 판정하지 않는 기존 규칙 유지."""
    d = FakeD(MFC_FLOW_STABLE_COUNT=3)
    d.add(1, 20.0, "chamber1")

    def on_read(i):
        if i == 0:
            d.t += 0.001                                           # 읽기 도중 등록(읽기 시작 시각보다 뒤)
            d.add(3, 30.0, "pc2")
    n = drive(d, [[IN, 0, 30.0]], on_read=on_read)
    assert len(d.gas(1)) == 3
    assert len(d.gas(3)) == 3                                      # 1회차는 건너뜀 → 2~4회차로 3회
    assert n == 4


# ───────────────────────── 설정 오류: 고쳐 쓰지 않고 FLOW_ON 실패 ─────────────────────────
@pytest.mark.parametrize("bad", [0, -1, 2.5, 3.0, "3", True, None], ids=repr)
def test_bad_stable_count_fails_flow_on(bad):
    d = FakeD(MFC_FLOW_STABLE_COUNT=bad)
    d.add(1, 20.0, "chamber1")
    n = drive(d, [[IN, 0, 0]])
    res = d.results()
    assert [r[0] for r in res] == ["command_failed"]
    assert res[0][1] == f"설정 오류(MFC_FLOW_STABLE_COUNT={bad!r}) — 1 이상의 정수여야 합니다" and res[0][2]["owner"] == "chamber1"
    assert res[0][2]["reason"] == "STAB_CONFIG"
    assert n == 0 and d.stab_jobs == {}                            # 읽지도 않고 바로 실패


def test_stable_count_from_cfg_is_used():
    d = FakeD(MFC_FLOW_STABLE_COUNT=1)
    d.add(1, 20.0, "chamber1")
    assert drive(d, [[IN, 0, 0]]) == 1 and d.gas(1)[-1].endswith("[연속 1/1]")


def test_default_stable_count_is_three_and_channel_keys():
    """키가 없으면 3(v1 config_common). 파라미터 표는 채널 키 CH1_·CH2_(v1 은 settings.json ch1·ch2 구역) — 기본 3, 1~180."""
    d = FakeD()
    assert ops.flow_stable_need(d) == 3
    by = {p.key: p for p in P.PARAMS}
    for k in ("CH1_MFC_FLOW_STABLE_COUNT", "CH2_MFC_FLOW_STABLE_COUNT"):
        assert by[k].default == 3 and (by[k].min, by[k].max) == (1, 180)
    assert "MFC_FLOW_STABLE_COUNT" not in by                      # 공통 키는 없음(채널 모듈 값만 쓰였다)


# ───────────────────────── 시험대(실제 드라이버 + 시뮬레이터) ─────────────────────────
def _gas_st(r, ch):
    return [e.detail for e in r.kinds("status") if e.detail.startswith(f"GAS 확인... ch{ch} ")]


def test_chamber_flow_on_needs_three_and_reflow_on_restarts():
    """CH 공정 FLOW_ON: 3회 연속 뒤 확정. 같은 채널에 새 목표 FLOW_SET → FLOW_ON(L21 재전송)이면 새 목표로 처음부터 다시 센다
    (가스 단계 설정 9.0 → 9.5 와 같은 흐름)."""
    from v2.tests.test_mfc import Rig
    r = Rig("ch2")
    t0 = r.c.monotonic()
    assert r.call(r.d.set_flow, 2, 9.0, owner="chamber2")[1] is None
    assert r.call(r.d.flow_on, 2, owner="chamber2") == ("FLOW_ON", None, "")
    assert r.sent(t0).count("R60") == 3
    assert r.call(r.d.set_flow, 2, 9.5, owner="chamber2")[1] is None
    assert r.call(r.d.flow_on, 2, owner="chamber2") == ("FLOW_ON", None, "")
    sent = r.sent(t0)
    assert sent.count("R60") == 6
    assert [s for s in sent if s.startswith(("L2", "Q2"))] == ["Q2 90.0", "L21", "Q2 95.0", "L21"]
    st = _gas_st(r, 2)
    assert _streak(st) == ["1", "2", "3", "1", "2", "3"]
    assert st[-1] == "GAS 확인... ch2 (목표: 9.50sccm, 현재: 9.50sccm) [연속 3/3]"
    r.done()


def test_plasma_cleaning_flow_set_on_needs_three():
    """플라즈마 클리닝 선택 채널(gas_select) FLOW_SET → FLOW_ON 도 같은 판정 — 3회 연속 뒤 확정."""
    from v2.tests.test_mfc import Rig
    r = Rig("ch2")
    r.d.gas_select(3, owner="pc2")
    t0 = r.c.monotonic()
    res = r.call(r.d.flow_set_on, 30.0, owner="pc2")
    assert res == ("FLOW_ON", None, "")
    assert r.sent(t0).count("R60") == 3
    assert _streak(_gas_st(r, 3)) == ["1", "2", "3"]
    assert _gas_st(r, 3)[-1] == "GAS 확인... ch3 (목표: 30.00sccm, 현재: 30.00sccm) [연속 3/3]"
    r.done()


def test_flow_on_bad_count_fails_without_reading():
    from v2.tests.test_mfc import Rig
    r = Rig("ch1", cfg={"CH1_MFC_FLOW_STABLE_COUNT": 0})
    t0 = r.c.monotonic()
    r.call(r.d.set_flow, 1, 20.0, owner="chamber1")
    res = r.call(r.d.flow_on, 1, owner="chamber1")
    assert res == ("FLOW_ON", "STAB_CONFIG", "설정 오류(MFC_FLOW_STABLE_COUNT=0) — 1 이상의 정수여야 합니다")
    assert r.sent(t0).count("R60") == 0
    r.done()

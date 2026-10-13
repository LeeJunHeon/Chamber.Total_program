# -*- coding: utf-8 -*-
"""DC Pulse 아크/점화 로그 = 장비 원값 기준 검증 (2026-10-12 CH2 런 4개 실측 근거, 장치·네트워크 접근 없음).

(a) 0x8C 리셋 판정 3값(예/아니오/확인불가) — 예전 판정은 '후값 0' 이면 무조건 예
(b) 0xAE/0xAF(AE=0, AF=1 고정)는 아크 rate 판정에 넣지 않는다 — 누계 증가분/경과시간(calc_rate)만
(c) 주기 로그에 장비 원값(SAT/ANT/AE/AF)이 먼저 나온다
(d) 활성화 대기 구간 PIV → [telemetry·점화] 로그(이벤트·콜백·CSV 경로로는 안 보냄) + ON 직후 SAT/ANT 1회
(e) 카운터 감소 문구가 새 형식으로 런당 1회

실제 device/dc_pulse.py 코드를 그대로 돌린다(읽기 함수만 가짜). pytest 로도 직접 실행으로도 돈다.
"""
import os
import sys
import time
import asyncio
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402
import device.dc_pulse as DCP                                    # noqa: E402
from device.dc_pulse import AsyncDCPulse, _arc_reset_state       # noqa: E402


# ───────────────────────── 공용 ─────────────────────────
def _cfg(**kw):
    base = dict(DEBUG_PRINT=False, DCP_POLL_INTERVAL_S=0.05, DCP_ARC_IGN_WINDOW_S=0.0,
                DCP_ARC_ALERT_RATE_PER_S=1.0, DCP_ARC_ALERT_RATE_N=2,
                DCP_ARC_IGN_WARN=0, DCP_ARC_DEPO_WARN=0)
    base.update(kw)
    return type("_Cfg", (), base)()


def _mk(**kw):
    """실제 AsyncDCPulse(통신 안 함). 출력 ON·연결 상태로 만든다."""
    d = AsyncDCPulse(host="127.0.0.1", port=9, cfg=_cfg(**kw))
    d._connected = True
    d._out_on = True
    d._last_ref_power_w = 500.0
    d._arc_run_start_ts = time.monotonic() - 100.0
    d._arc_ign_soft = 0
    d._arc_ign_hard = 0
    return d


def _events(d):
    evs = []
    while True:
        try:
            evs.append(d._event_q.get_nowait())
        except asyncio.QueueEmpty:
            return evs


def _status(evs):
    return [e.message or "" for e in evs if e.kind == "status"]


def _feed(d, *, soft, hard, ae=0, af=1, piv=(500.0, 1.0, 500.0)):
    """폴링 읽기 함수를 가짜로. soft/hard 는 호출마다 꺼내는 리스트(마지막 값 반복)."""
    soft, hard = list(soft), list(hard)
    seen = {"hit_max": 0}

    async def _piv():
        p, i, v = piv
        return {"raw": {"P": int(p * 10), "I": int(i * 100), "V": int(v)},
                "eng": {"P_W": p, "I_A": i, "V_V": v}}

    async def _s():
        seen["hit_max"] = max(seen["hit_max"], int(getattr(d, "_arc_rate_hit_n", 0)))
        return soft.pop(0) if len(soft) > 1 else soft[0]

    async def _h():
        return hard.pop(0) if len(hard) > 1 else hard[0]

    async def _ae():
        return ae

    async def _af():
        return af
    d.read_output_piv = _piv
    d.read_soft_arc_total = _s
    d.read_hard_arc_total = _h
    d.read_soft_arc_per_sec = _ae
    d.read_hard_arc_per_sec = _af
    return seen


def _run_poll(d, seconds=0.5):
    async def _main():
        t = asyncio.ensure_future(d._poll_loop())
        await asyncio.sleep(seconds)
        t.cancel()
        with contextlib.suppress(BaseException):
            await t
        return _events(d)
    return asyncio.run(_main())


# ───────────────────────── (a) ─────────────────────────
@pytest.mark.parametrize("pre,post,expect", [
    ((3, 0), (3, 0), "no"),          # 10/12 실측: 리셋 ACK 인데 그대로
    ((0, 0), (0, 0), "unknown"),     # 둘 다 0 → 먹었는지 알 수 없음(예전엔 무조건 '예')
    ((3, 0), (0, 0), "yes"),
    ((2, 1), (2, 0), "yes"),
    ((None, None), (None, None), "unknown"),   # 읽기 실패
    ((5, None), (None, None), "unknown"),
    ((5, 0), (7, 0), "no"),          # 커짐
])
def test_a_reset_state(pre, post, expect):
    assert _arc_reset_state(pre[0], pre[1], post[0], post[1]) == expect


def test_a2_reset_ok_only_when_yes_and_summary_keys():
    src = open(DCP.__file__, encoding="utf-8").read()
    i = src.index("self._arc_reset_state = _arc_reset_state(_pre_s, _pre_h, _post_s, _post_h)")
    blk = src[i:i + 900]
    assert 'self._arc_reset_ok = (self._arc_reset_state == "yes")' in blk
    assert "[arc] 0x8C 리셋: 전 SAT=" in blk and "(동작=" in blk
    d = AsyncDCPulse(host="127.0.0.1", port=9, cfg=_cfg())
    for st, ok in (("yes", True), ("no", False), ("unknown", False)):
        d._arc_reset_state = st
        d._arc_reset_ok = (st == "yes")
        s = d.arc_summary
        assert s["reset_state"] == st and s["reset_ok"] is ok
    d.reset_arc_counts()
    assert d._arc_reset_state == "unknown" and d._arc_reset_ok is False


# ───────────────────────── (b) ─────────────────────────
def test_b1_device_rate_not_used_when_counts_flat():
    """AE=0, AF=1 고정 + 누계 변화 없음 → rate 판정 카운터가 오르지 않고 경고도 없다(예전엔 1/s 가 섞였다)."""
    d = _mk(DCP_ARC_ALERT_RATE_PER_S=1.0)
    seen = _feed(d, soft=[3], hard=[0], ae=0, af=1)
    evs = _run_poll(d, 0.5)
    assert seen["hit_max"] == 0 and d._arc_rate_hit_n == 0
    assert [e for e in evs if e.kind == "arc_threshold_reached"] == []


def test_b2_huge_device_rate_ignored():
    d = _mk(DCP_ARC_ALERT_RATE_PER_S=20.0)
    _feed(d, soft=[3], hard=[0], ae=5000, af=5000)
    evs = _run_poll(d, 0.5)
    assert d._arc_rate_hit_n == 0
    assert [e for e in evs if e.kind == "arc_threshold_reached"] == []


def test_b3_real_count_increase_judged_by_calc_rate():
    """누계가 실제로 늘면 calc_rate 로 판정 → 경고(장비 rate 는 0 이어도)."""
    d = _mk(DCP_ARC_ALERT_RATE_PER_S=20.0, DCP_ARC_ALERT_RATE_N=2)
    _feed(d, soft=[100 * k for k in range(1, 60)], hard=[0], ae=0, af=0)
    evs = _run_poll(d, 0.6)
    alerts = [e for e in evs if e.kind == "arc_threshold_reached"]
    assert len(alerts) == 1 and alerts[0].data["by_rate"] is True
    assert alerts[0].data["calc_rate"] >= 20.0


# ───────────────────────── (c) ─────────────────────────
def test_c_periodic_log_shows_raw_first():
    d = _mk()
    d._arc_baseline_soft = 2
    d._arc_baseline_hard = 0
    _feed(d, soft=[3], hard=[0], ae=0, af=1)
    evs = _run_poll(d, 0.3)
    lines = [m for m in _status(evs) if m.startswith("[arc] 장비 SAT=")]
    assert lines, _status(evs)
    m = lines[-1]
    assert m.startswith("[arc] 장비 SAT=3 ANT=0 AE=0 AF=1 | 런 soft=1 hard=0 (기준 SAT=2 ANT=0) | "), m
    assert "점화=" in m and "증착=" in m and "| 계산 " in m
    assert "rate soft/s" not in m and "calc/s" not in m
    # 첫 표본은 계산값이 없다 → "-"
    assert lines[0].endswith("| 계산 -"), lines[0]


def test_c2_rate_read_failed_shows_dash():
    d = _mk()
    _feed(d, soft=[3], hard=[0])

    async def _fail():
        return None
    d.read_soft_arc_per_sec = _fail
    d.read_hard_arc_per_sec = _fail
    evs = _run_poll(d, 0.2)
    lines = [m for m in _status(evs) if m.startswith("[arc] 장비 SAT=")]
    assert lines and "AE=- AF=-" in lines[0], lines


# ───────────────────────── (d) ─────────────────────────
class _Clock:
    def __init__(self):
        self.t = 1000.0

    def monotonic(self):
        return self.t

    async def sleep(self, s):
        self.t += max(0.0, float(s))


def _mk_arm(monkeypatch, *, armed=True, samples=None, delay=1.5, interval=0.5, sat=(0, 0)):
    clk = _Clock()
    monkeypatch.setattr(DCP.time, "monotonic", clk.monotonic)
    monkeypatch.setattr(DCP.asyncio, "sleep", clk.sleep)
    from lib import config_common as cfgc
    monkeypatch.setattr(cfgc, "DC_OVERCURRENT_ARM_ON_START", armed, raising=False)
    monkeypatch.setattr(cfgc, "DC_OVERCURRENT_ARM_INTERVAL_S", interval, raising=False)
    monkeypatch.setattr(cfgc, "DC_OVERCURRENT_ENABLE", True, raising=False)
    d = AsyncDCPulse(host="127.0.0.1", port=9, cfg=_cfg())
    d._activation_check_delay_s = delay
    d._last_ref_power_w = 500.0
    d._arc_on_ref = (2, 0)
    d.piv_reads, d.arc_reads, d.tele = [], [], []
    d._on_telemetry = lambda p, v, i: d.tele.append((p, v, i))
    samples = list(samples or [(1250.0, 1.0, 170.0), (1010.0, 1.1, 172.0), (780.0, 1.2, 175.0)])

    async def _piv():
        d.piv_reads.append(clk.t)
        p, i, v = samples[min(len(d.piv_reads) - 1, len(samples) - 1)]
        return {"raw": {"P": int(p / 10), "I": int(i * 100), "V": int(v)},
                "eng": {"P_W": p, "I_A": i, "V_V": v}}

    async def _s():
        d.arc_reads.append("S")
        return sat[0]

    async def _h():
        d.arc_reads.append("H")
        return sat[1]
    d.read_output_piv = _piv
    d.read_soft_arc_total = _s
    d.read_hard_arc_total = _h
    return d, clk


def test_d1_activation_piv_logged_not_sent_as_telemetry(monkeypatch):
    d, clk = _mk_arm(monkeypatch, sat=(0, 0))
    t0 = clk.t
    assert asyncio.run(d._watch_overcurrent_during_activation()) is False
    evs = _events(d)
    msgs = _status(evs)
    tele_lines = [m for m in msgs if m.startswith("[telemetry·점화]")]
    assert len(tele_lines) == 3 == len(d.piv_reads), msgs
    assert tele_lines[0] == "[telemetry·점화] P=1250.0 W, V=170.00 V, I=1.000 A (raw P/I/V=125/100/170) +0.0 s"
    assert tele_lines[1].endswith("+0.5 s") and tele_lines[2].endswith("+1.0 s")
    assert [e for e in evs if e.kind == "telemetry"] == [] and d.tele == []      # CSV·평균 경로 아님
    on_lines = [m for m in msgs if m.startswith("[arc] ON 직후")]
    assert on_lines == ["[arc] ON 직후 장비 SAT=0 ANT=0 (ON 전 기준 SAT=2 ANT=0)"], msgs
    assert d.arc_reads == ["S", "H"]                                              # 1회만
    assert abs((clk.t - t0) - 1.5) < 1e-9                                         # delay 를 넘지 않음


def test_d2_arm_off_still_logs_counters_within_delay(monkeypatch):
    d, clk = _mk_arm(monkeypatch, armed=False, sat=(3, 0))
    t0 = clk.t
    assert asyncio.run(d._watch_overcurrent_during_activation()) is False
    msgs = _status(_events(d))
    assert d.piv_reads == []                                                      # 감시(PIV)는 안 함
    assert msgs == ["[arc] ON 직후 장비 SAT=3 ANT=0 (ON 전 기준 SAT=2 ANT=0)",
                    "[arc] 런 기준값: SAT=0 ANT=0 (ON 직후 초기화 확인)"], msgs
    assert abs((clk.t - t0) - 1.5) < 1e-9


def test_d3_counter_read_failure_shows_dash(monkeypatch):
    d, clk = _mk_arm(monkeypatch)

    async def _boom():
        raise RuntimeError("no reply")
    d.read_soft_arc_total = _boom
    d.read_hard_arc_total = _boom
    d._arc_on_ref = (None, None)
    asyncio.run(d._watch_overcurrent_during_activation())
    msgs = _status(_events(d))
    assert "[arc] ON 직후 장비 SAT=- ANT=- (ON 전 기준 SAT=- ANT=-)" in msgs, msgs


def test_d4_overcurrent_still_checked_first_and_trips(monkeypatch):
    """과전류 판정 위치·순서는 그대로 — 첫 샘플에서 차단되면 True(점화 로그 1줄은 남는다)."""
    d, clk = _mk_arm(monkeypatch, samples=[(500.0, 7.2, 170.0)])
    offs = []

    async def _off():
        offs.append(1)
    d.output_off = _off

    async def _fault():
        return 0
    d.read_fault_code = _fault
    assert asyncio.run(d._watch_overcurrent_during_activation()) is True
    msgs = _status(_events(d))
    assert offs == [1] and len(d.piv_reads) == 1
    assert sum(1 for m in msgs if m.startswith("[telemetry·점화]")) == 1


# ───────────────────────── (e) ─────────────────────────
def test_e_counter_decrease_message_once():
    d = _mk()
    d._arc_baseline_soft = 3
    d._arc_baseline_hard = 1
    _feed(d, soft=[0], hard=[0])
    evs = _run_poll(d, 0.4)
    msgs = _status(evs)
    dec = [m for m in msgs if m.startswith("[arc] 장비 카운터 감소")]
    assert dec == ["[arc] 장비 카운터 감소 SAT 3→0, ANT 1→0 — 장비가 카운터를 다시 셈 → 기준값을 0 으로 변경"], msgs
    assert not any("장비 자체 리셋 감지" in m for m in msgs)
    assert d._arc_baseline_soft == 0 and d._arc_baseline_hard == 0               # 보정 동작은 그대로


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

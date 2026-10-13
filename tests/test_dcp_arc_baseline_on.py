# -*- coding: utf-8 -*-
"""DC Pulse 런 아크 기준값 = OUTPUT_ON 직후 장비값 검증 (2026-10-12~13 CH2 실측 7/7: ON 직후 SAT/ANT 0/0).

(a) ON 전 2/0, ON 직후 0/0, 폴링 3/0 → 런 soft=3, 기준 0/0, '초기화 확인'
(b) ON 전 2/0, ON 직후 0/0, 첫 폴링 4/0 → 런 soft=4 (예전 코드는 2 — 첫 폴링 전 아크 누락 회귀 방지)
(c) ON 전 3/0, ON 직후 3/0, 폴링 5/0 → 기준 3 유지, 런 soft=2, 경고 1줄
(d) ON 전 0/0 → 기준 0, 런 = 장비값
(e) ON 직후 읽기 실패 → 기존 방식(기준=ON 전 값, 첫 폴링 감소 시 0 으로 보정)
(f) 첫 PIV 에서 과전류 차단 → 예외 없이 기존 동작(기준 결정 안 함)
(g) 활성화 감시 경로(ARM on)로 불러도 같은 결정

실제 device/dc_pulse.py 코드를 그대로 돌린다(읽기 함수만 가짜). 장치·네트워크 접근 없음.
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
from device.dc_pulse import AsyncDCPulse                         # noqa: E402


def _cfg(**kw):
    base = dict(DEBUG_PRINT=False, DCP_POLL_INTERVAL_S=0.05, DCP_ARC_IGN_WINDOW_S=0.0,
                DCP_ARC_ALERT_RATE_PER_S=0.0, DCP_ARC_ALERT_RATE_N=2,
                DCP_ARC_IGN_WARN=0, DCP_ARC_DEPO_WARN=0)
    base.update(kw)
    return type("_Cfg", (), base)()


def _mk(pre=(2, 0)):
    """ON 전 기준값(리셋 후 읽은 값)이 pre 인 상태 — _prepare_and_start_impl 4-c) 직후와 같다."""
    d = AsyncDCPulse(host="127.0.0.1", port=9, cfg=_cfg())
    d._connected = True
    d._out_on = True
    d._last_ref_power_w = 500.0
    d._arc_run_start_ts = time.monotonic() - 100.0
    d._arc_ign_soft = 0
    d._arc_ign_hard = 0
    d._arc_baseline_soft = int(pre[0])
    d._arc_baseline_hard = int(pre[1])
    d._arc_on_ref = pre
    return d


def _feed(d, soft, hard):
    """0x96/0x99 읽기 가짜 — 호출마다 리스트에서 꺼낸다(마지막 값 반복). 값이 Exception 이면 실패."""
    soft, hard = list(soft), list(hard)

    def _take(lst):
        v = lst.pop(0) if len(lst) > 1 else lst[0]
        if isinstance(v, Exception):
            raise v
        return v

    async def _s():
        return _take(soft)

    async def _h():
        return _take(hard)

    async def _piv():
        return {"raw": {"P": 50, "I": 100, "V": 500}, "eng": {"P_W": 500.0, "I_A": 1.0, "V_V": 500.0}}

    async def _rate():
        return 0
    d.read_soft_arc_total = _s
    d.read_hard_arc_total = _h
    d.read_output_piv = _piv
    d.read_soft_arc_per_sec = _rate
    d.read_hard_arc_per_sec = _rate


def _events(d):
    evs = []
    while True:
        try:
            evs.append(d._event_q.get_nowait())
        except asyncio.QueueEmpty:
            return evs


def _status(d):
    return [e.message or "" for e in _events(d) if e.kind == "status"]


def _after_on_then_poll(d, poll_s=0.3):
    """ON 직후 1회 읽기(기준 결정) → 폴링 루프 잠깐."""
    async def _main():
        await d._log_arc_after_on()
        t = asyncio.ensure_future(d._poll_loop())
        await asyncio.sleep(poll_s)
        t.cancel()
        with contextlib.suppress(BaseException):
            await t
    asyncio.run(_main())
    return _status(d)


def test_a_reset_confirmed_baseline_zero():
    d = _mk(pre=(2, 0))
    _feed(d, soft=[0, 3], hard=[0])
    msgs = _after_on_then_poll(d)
    assert (d._arc_baseline_soft, d._arc_baseline_hard) == (0, 0)
    assert d._soft_arc_total == 3 and d._hard_arc_total == 0
    assert "[arc] 런 기준값: SAT=0 ANT=0 (ON 직후 초기화 확인)" in msgs, msgs
    assert not any("카운터 감소" in m for m in msgs)            # 안전망은 발동하지 않음
    assert not any("초기화되지 않음" in m for m in msgs)


def test_b_arcs_before_first_poll_not_lost():
    """ON 후 첫 폴링 전 아크가 ON 전 값(2) 이상 쌓여도(4) 빠지지 않는다. 예전 코드: 4-2=2."""
    d = _mk(pre=(2, 0))
    _feed(d, soft=[0, 4], hard=[0])
    _after_on_then_poll(d)
    assert d._soft_arc_total == 4
    assert d.arc_counts == (4, 0)


def test_c_not_reset_keeps_baseline_and_warns_once():
    d = _mk(pre=(3, 0))
    _feed(d, soft=[3, 5], hard=[0])
    msgs = _after_on_then_poll(d)
    assert d._arc_baseline_soft == 3 and d._arc_baseline_hard == 0
    assert d._soft_arc_total == 2
    warns = [m for m in msgs if m.startswith("[arc] ⚠ ON 직후 카운터가 초기화되지 않음")]
    assert warns == ["[arc] ⚠ ON 직후 카운터가 초기화되지 않음 SAT 3→3, ANT 0→0 — 기준값 유지(기존 방식)"], msgs
    assert "[arc] 런 기준값: SAT=3 ANT=0 (초기화 안 됨)" in msgs


def test_d_zero_before_on():
    d = _mk(pre=(0, 0))
    _feed(d, soft=[0, 2], hard=[0, 1])
    msgs = _after_on_then_poll(d)
    assert (d._arc_baseline_soft, d._arc_baseline_hard) == (0, 0)
    assert d.arc_counts == (2, 1)
    assert "[arc] 런 기준값: SAT=0 ANT=0 (ON 직후 초기화 확인)" in msgs


def test_e_read_failure_falls_back_to_old_behavior():
    d = _mk(pre=(2, 0))
    _feed(d, soft=[RuntimeError("no reply"), 1], hard=[RuntimeError("no reply"), 0])
    async def _main():
        await d._log_arc_after_on()
    asyncio.run(_main())
    msgs1 = _status(d)
    assert d._arc_baseline_soft == 2                              # 기준 유지(기존 방식)
    assert "[arc] 런 기준값: SAT=2 ANT=0 (확인 실패 → 기존 방식)" in msgs1, msgs1
    # 첫 폴링에서 감소 감지 → 기존 안전망이 0 으로 보정
    d2 = d
    async def _poll():
        t = asyncio.ensure_future(d2._poll_loop())
        await asyncio.sleep(0.3)
        t.cancel()
        with contextlib.suppress(BaseException):
            await t
    asyncio.run(_poll())
    msgs2 = _status(d)
    assert d._arc_baseline_soft == 0 and d._soft_arc_total == 1
    assert any(m.startswith("[arc] 장비 카운터 감소 SAT 2→1") for m in msgs2), msgs2


# ───────────── 활성화 감시 경로 (가짜 시계) ─────────────
class _Clock:
    def __init__(self):
        self.t = 1000.0

    def monotonic(self):
        return self.t

    async def sleep(self, s):
        self.t += max(0.0, float(s))


def _arm(monkeypatch, d, *, armed=True):
    clk = _Clock()
    monkeypatch.setattr(DCP.time, "monotonic", clk.monotonic)
    monkeypatch.setattr(DCP.asyncio, "sleep", clk.sleep)
    from lib import config_common as cfgc
    monkeypatch.setattr(cfgc, "DC_OVERCURRENT_ARM_ON_START", armed, raising=False)
    monkeypatch.setattr(cfgc, "DC_OVERCURRENT_ARM_INTERVAL_S", 0.5, raising=False)
    monkeypatch.setattr(cfgc, "DC_OVERCURRENT_ENABLE", True, raising=False)
    d._activation_check_delay_s = 1.5
    return clk


def test_f_overcurrent_trip_on_first_piv_keeps_old_behavior(monkeypatch):
    d = _mk(pre=(2, 0))
    _feed(d, soft=[0], hard=[0])

    async def _piv_hi():
        return {"raw": {"P": 50, "I": 720, "V": 170}, "eng": {"P_W": 500.0, "I_A": 7.2, "V_V": 170.0}}
    d.read_output_piv = _piv_hi
    offs = []

    async def _off():
        offs.append(1)
    d.output_off = _off

    async def _fault():
        return 0
    d.read_fault_code = _fault
    _arm(monkeypatch, d)
    assert asyncio.run(d._watch_overcurrent_during_activation()) is True
    msgs = _status(d)
    assert offs == [1]
    assert d._arc_baseline_soft == 2                              # 기준 결정 안 함(기존 동작)
    assert not any(m.startswith("[arc] 런 기준값") for m in msgs), msgs


def test_g_decision_through_activation_watcher(monkeypatch):
    for armed in (True, False):
        d = _mk(pre=(10, 2))
        _feed(d, soft=[0], hard=[0])
        clk = _arm(monkeypatch, d, armed=armed)
        t0 = clk.t
        assert asyncio.run(d._watch_overcurrent_during_activation()) is False
        msgs = _status(d)
        assert (d._arc_baseline_soft, d._arc_baseline_hard) == (0, 0), armed
        assert "[arc] 런 기준값: SAT=0 ANT=0 (ON 직후 초기화 확인)" in msgs, msgs
        assert abs((clk.t - t0) - 1.5) < 1e-9                    # 활성화 대기 시간은 그대로


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

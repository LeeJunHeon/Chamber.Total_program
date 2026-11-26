# -*- coding: utf-8 -*-
"""CH2 파워 조합 규칙 — 건 3개 허용 / RF 연속 + RF Pulse 동시 금지 / RF 연속을 가장 먼저 켠다.

시퀀스는 test_process_heater_steps 의 _pc / _rows, 검증은 ChamberRuntime.__new__ 스텁을 쓴다.
"""
import os
import sys
import json
import asyncio
import contextlib
import itertools

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "tests"))
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                   # noqa: E402
from test_process_heater_steps import _pc, _rows, CASES, GOLDEN  # noqa: E402

_POWER = ("RF_POWER_SET", "DC_POWER_SET", "DC2_POWER_SET", "DC_PULSE_START", "RF_PULSE_START")
_MSG_GUN = "G1~G3 중 1개 이상 선택"
_MSG_RF_BOTH = "RF Power와 RF Pulse는 동시에 사용할 수 없습니다"
_MSG_PULSE_BOTH = "RF Pulse와 DC Pulse는 동시에 사용할 수 없습니다 (같은 시리얼 포트)"

_BASE = dict(base_pressure=7e-6, working_pressure=2.0, process_time=10, shutter_delay=5,
             use_ar=True, ar_flow=20, use_ms=True, use_g1=True)
_RF = dict(use_rf_power=True, rf_power=150)
_DC1 = dict(use_dc_power=True, dc_power=100)
_DCP = dict(use_dc_pulse=True, dc_pulse_power=80, dc_pulse_freq=20, dc_pulse_off_time=10.0)
_RFP = dict(use_rf_pulse=True, rf_pulse_power=100, rf_pulse_freq=1.0, rf_pulse_duty=50)


def _seq(ch, **p):
    async def go():
        return _pc(ch)._create_process_sequence(dict(p))
    return asyncio.run(go())


def _power_order(seq):
    return [s.action.name for s in seq if s.action.name in _POWER]


# ───────────────────────── 시퀀스: 켜는 순서 ─────────────────────────
def test_three_guns_three_powers_order():
    seq = _seq(2, **_BASE, **_RF, **_DC1, **_DCP, use_g2=True, use_g3=True)
    msgs = [s.message for s in seq]
    names = [s.action.name for s in seq]
    i = msgs.index("Gun Shutter G1 열기")
    assert msgs[i:i + 4] == ["Gun Shutter G1 열기", "Gun Shutter G2 열기", "Gun Shutter G3 열기",
                             "Power Select OFF (SW_POWER_SELECT)"]
    assert names[i + 4:i + 7] == ["RF_POWER_SET", "DC_POWER_SET", "DC_PULSE_START"]
    assert msgs[i + 7].startswith("목표 압력(SP2)")
    assert seq[i + 4].parallel is False


@pytest.mark.parametrize("parts, expect", [
    ((_RF, _DC1), ["RF_POWER_SET", "DC_POWER_SET"]),
    ((_RF, _DCP), ["RF_POWER_SET", "DC_PULSE_START"]),
    ((_RF, _DC1, _DCP), ["RF_POWER_SET", "DC_POWER_SET", "DC_PULSE_START"]),
    ((_DC1, _DCP), ["DC_POWER_SET", "DC_PULSE_START"]),
    ((_DC1, _RFP), ["DC_POWER_SET", "RF_PULSE_START"]),
    ((_RF,), ["RF_POWER_SET"]),
    ((_DC1,), ["DC_POWER_SET"]),
    ((_DCP,), ["DC_PULSE_START"]),
    ((_RFP,), ["RF_PULSE_START"]),
])
def test_ch2_power_on_order(parts, expect):
    p = dict(_BASE)
    for d in parts:
        p.update(d)
    seq = _seq(2, **p)
    assert _power_order(seq) == expect
    for s in seq:
        if s.action.name == "RF_POWER_SET":
            assert s.parallel is False


def test_no_adjacent_parallel_in_allowed_combos():
    """검증을 통과하는 조합 전부(RF 연속+RF Pulse, RF Pulse+DC Pulse 제외)에서 parallel 단계가 붙지 않는다."""
    n = 0
    for use in itertools.product((False, True), repeat=4):
        rf, dc1, dcp, rfp = use
        if not any(use) or (rf and rfp) or (rfp and dcp):
            continue
        p = dict(_BASE)
        for on, d in zip(use, (_RF, _DC1, _DCP, _RFP)):
            if on:
                p.update(d)
        for extra in ({}, {"use_power_select": True}, {"working_pressure": 60}):
            seq = _seq(2, **dict(p, **extra))
            flags = [bool(s.parallel) for s in seq]
            assert not any(a and b for a, b in zip(flags, flags[1:])), (use, extra)
            n += 1
    assert n == 27


def test_ch1_power_order_unchanged():
    with open(GOLDEN, encoding="utf-8") as f:
        golden = json.load(f)
    for name, expect in (("ch1_rfp", ["RF_PULSE_START"]), ("ch1_dcp_change_sd", ["DC_PULSE_START"])):
        ch, p = CASES[name]
        seq = _seq(ch, **p)
        assert _power_order(seq) == expect
        assert json.loads(json.dumps(_rows(seq), ensure_ascii=False)) == golden[name]
        # CH1 은 RF 연속 미지원 — use_rf_power 가 남아 있어도 시퀀스가 같다
        assert _rows(_seq(ch, **dict(p, use_rf_power=True, rf_power=150))) == _rows(seq)


# ───────────────────────── 검증: _validate_norm_params ─────────────────────────
def _chamber(ch=2):
    from runtime.chamber_runtime import ChamberRuntime
    c = ChamberRuntime.__new__(ChamberRuntime)
    c.ch = ch
    c.prefix = f"ch{ch}_"
    c.supports_dc_cont = True
    c.supports_dc_cont2 = False
    c.supports_rf_cont = (ch == 2)
    c.supports_dc_pulse = True
    c.supports_rf_pulse = True
    return c


def _params(**kw):
    p = {"use_ar": True, "ar_flow": 10.0, "use_o2": False, "o2_flow": 0, "use_n2": False, "n2_flow": 0,
         "use_g1": False, "use_g2": False, "use_g3": False,
         "use_dc_power": False, "dc_power": 0.0, "use_dc_power2": False, "dc_power2": 0.0,
         "use_rf_power": False, "rf_power": 0.0, "use_dc_pulse": False, "use_rf_pulse": False}
    p.update(kw)
    return p


def _errs(ch=2, **kw):
    return _chamber(ch)._validate_norm_params(_params(**kw))


def test_ch2_three_guns_three_powers_ok():
    assert _errs(use_g1=True, use_g2=True, use_g3=True, **_RF, **_DC1, **_DCP) == []


def test_ch2_gun_count():
    assert _errs(**_DC1) == [_MSG_GUN]
    assert _errs(use_g2=True, **_DC1) == []
    assert _errs(use_g1=True, use_g3=True, **_DC1) == []
    assert _errs(use_g1=True, use_g2=True, use_g3=True, **_DC1) == []


def test_ch2_rf_cont_and_rf_pulse_rejected():
    assert _errs(use_g1=True, **_RF, **_RFP) == [_MSG_RF_BOTH]
    # use_rf_power=T 여도 0 W 면 RF 연속을 쓰지 않는 것으로 본다(지금처럼 통과)
    assert _errs(use_g1=True, use_rf_power=True, rf_power=0, **_RFP) == []
    assert _errs(use_g1=True, **_RF) == []
    assert _errs(use_g1=True, **_RFP) == []


def test_ch2_rf_pulse_and_dc_pulse_message_kept():
    errs = _errs(use_g1=True, **_RFP, **_DCP)
    assert _MSG_PULSE_BOTH in errs and _MSG_RF_BOTH not in errs


def test_ch1_rf_power_flag_not_rejected():
    assert _errs(ch=1, **_RF, **_RFP) == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

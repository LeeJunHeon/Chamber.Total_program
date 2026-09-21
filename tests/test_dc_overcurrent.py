# -*- coding: utf-8 -*-
"""DC 과전류 인터락 검증 (장치·PLC·시리얼 접근 없음).

A. lib/power_limits.dc_overcurrent_limit_a 순수 함수
B. DC Pulse — _check_overcurrent (폴링 루프에서 _fast 가드보다 앞에서 호출됨)
C. DC Power — _control_loop 에 상태 읽기 스텁을 주입해 update_measurements 경로로 검증
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

import pytest                                              # noqa: E402
from lib import config_common as cfgc                      # noqa: E402
from lib.power_limits import dc_overcurrent_limit_a        # noqa: E402
import device.dc_pulse as DCP                              # noqa: E402
from device.dc_pulse import AsyncDCPulse, DCPEvent         # noqa: E402
from device.dc_power import DCPowerAsync, DCPowerEvent     # noqa: E402


@pytest.fixture(autouse=True)
def _defaults(monkeypatch):
    for k, v in dict(DC_OVERCURRENT_ENABLE=True, DC_OVERCURRENT_ENABLE_CONT=True,
                     DC_OVERCURRENT_P1_W=300.0, DC_OVERCURRENT_I1_A=1.0,
                     DC_OVERCURRENT_P2_W=600.0, DC_OVERCURRENT_I2_A=2.0,
                     DC_OVERCURRENT_STREAK_N=1, DC_OVERCURRENT_EXTRAPOLATE=True).items():
        monkeypatch.setattr(cfgc, k, v, raising=False)
    yield monkeypatch


# ───────────────────────── A ─────────────────────────
def test_a1_below_p1_is_i1():
    for p in (0, 100, 300):
        assert dc_overcurrent_limit_a(p) == 1.0


def test_a2_a3_linear():
    assert abs(dc_overcurrent_limit_a(450) - 1.5) < 1e-9
    assert abs(dc_overcurrent_limit_a(600) - 2.0) < 1e-9


def test_a4_extrapolate_or_clamp(_defaults):
    assert abs(dc_overcurrent_limit_a(900) - 3.0) < 1e-9
    assert abs(dc_overcurrent_limit_a(900, extrapolate=False) - 2.0) < 1e-9
    _defaults.setattr(cfgc, "DC_OVERCURRENT_EXTRAPOLATE", False, raising=False)
    assert abs(dc_overcurrent_limit_a(900) - 2.0) < 1e-9


def test_a5_bad_args_fallback_i1():
    assert dc_overcurrent_limit_a(450, p1=600, p2=300) == 1.0
    assert dc_overcurrent_limit_a(450, p1=300, p2=300) == 1.0
    assert dc_overcurrent_limit_a(450, i2=float("nan")) == 1.0
    assert dc_overcurrent_limit_a(450, i1=-5, i2=-1) == 1.0     # 0 이하 → 1.0 보정
    assert dc_overcurrent_limit_a("abc") == 1.0


def test_a6_none_or_negative_setpoint():
    assert dc_overcurrent_limit_a(None) == 1.0
    assert dc_overcurrent_limit_a(-50) == 1.0
    assert dc_overcurrent_limit_a(None, i1=0.7) == 0.7


# ───────────────────────── B: DC Pulse ─────────────────────────
def _mk_dcp(**cfg):
    d = AsyncDCPulse.__new__(AsyncDCPulse)
    d._cfg = dict(cfg)
    d._overcurr_n = 0
    d._last_ref_power_w = None
    d._arc_run_start_ts = 0.0
    d._poll_period_s = 5.0
    d.events_out = []
    d.status = []
    d.offs = []

    def _cb(key, default):
        return bool(cfg.get(key, getattr(cfgc, key, default)))

    def _ci(key, default):
        return int(cfg.get(key, getattr(cfgc, key, default)))

    def _cf(key, default):
        return float(cfg.get(key, getattr(cfgc, key, default)))

    async def _emit(msg):
        d.status.append(msg)

    async def _off():
        d.offs.append(1)

    async def _fault():
        return 0

    d._cfg_bool = _cb
    d._cfg_int = _ci
    d._cfg_float = _cf
    d._emit_status = _emit
    d.output_off = _off
    d.read_fault_code = _fault
    d._ev_nowait = lambda ev: d.events_out.append(ev)
    return d


def _run_check(d, ref_w, i, raw=None):
    d._last_ref_power_w = ref_w
    p = float(ref_w)
    v = p / i if i else 0.0
    eng = {"P_W": p, "V_V": v, "I_A": i}
    return asyncio.run(d._check_overcurrent(p, v, i, eng, raw))


def _stops(d):
    return [e for e in d.events_out if e.kind == "command_failed" and e.cmd == "AUTO_STOP"]


def test_b1_measured_case_190w_2p71a_trips():
    d = _mk_dcp()
    assert _run_check(d, 190.0, 2.71, raw={"P": 19, "I": 271, "V": 70}) is True
    st = _stops(d)
    assert len(st) == 1 and "overcurrent" in st[0].reason, st
    assert "설정 190W" in st[0].reason and abs(st[0].current - 2.71) < 1e-9
    assert d.offs == [1]
    assert any(m.startswith("[overcurrent]") and "raw P/I/V=19/271/70" in m for m in d.status), d.status
    assert any("[AUTO-STOP] 과전류" in m for m in d.status)


def test_b2_190w_0p9a_no_trip():
    d = _mk_dcp()
    assert _run_check(d, 190.0, 0.9) is False
    assert _stops(d) == [] and d.offs == []


def test_b3_600w_boundary():
    d = _mk_dcp()
    assert _run_check(d, 600.0, 1.9) is False
    assert _stops(d) == []
    assert _run_check(d, 600.0, 2.1) is True
    assert len(_stops(d)) == 1


def test_b4_trips_inside_ignition_fast_window():
    """★ 점화 고속창(_in_arc_fast_window()==True)에서도 그대로 차단된다."""
    d = _mk_dcp(DCP_ARC_IGN_WINDOW_S=30.0)
    d._arc_run_start_ts = time.monotonic()          # 방금 OUTPUT ON
    assert d._in_arc_fast_window() is True
    assert _run_check(d, 190.0, 2.71) is True
    assert len(_stops(d)) == 1

    # 소스 순서: 과전류 판정이 `_fast = self._in_arc_fast_window()` 보다 앞에 있다
    src = open(DCP.__file__, encoding="utf-8").read()
    i_chk = src.index("if await self._check_overcurrent(p, v, i, eng, res.get(\"raw\")):")
    i_fast = src.index("_fast = self._in_arc_fast_window()")
    i_low = src.index("# ① 저전류 감시")
    assert i_chk < i_fast < i_low


def test_b5_disabled_no_trip(_defaults):
    _defaults.setattr(cfgc, "DC_OVERCURRENT_ENABLE", False, raising=False)
    d = _mk_dcp()
    assert _run_check(d, 190.0, 2.71) is False
    assert _stops(d) == [] and d._overcurr_n == 0


def test_b6_streak_two(_defaults):
    _defaults.setattr(cfgc, "DC_OVERCURRENT_STREAK_N", 2, raising=False)
    d = _mk_dcp()
    assert _run_check(d, 190.0, 2.71) is False
    assert d._overcurr_n == 1 and _stops(d) == []
    assert _run_check(d, 190.0, 2.71) is True
    assert d._overcurr_n == 2 and len(_stops(d)) == 1 and "2회 연속" in _stops(d)[0].reason


def test_b7_streak_resets_on_normal(_defaults):
    _defaults.setattr(cfgc, "DC_OVERCURRENT_STREAK_N", 2, raising=False)
    d = _mk_dcp()
    _run_check(d, 190.0, 2.71)
    assert d._overcurr_n == 1
    _run_check(d, 190.0, 0.5)
    assert d._overcurr_n == 0
    # NaN/None 은 카운터를 건드리지 않는다
    d._overcurr_n = 1
    _run_check(d, 190.0, float("nan"))
    assert d._overcurr_n == 1
    assert asyncio.run(d._check_overcurrent(190.0, 0.0, None, {}, None)) is False
    assert d._overcurr_n == 1


# ───────────────────────── C: DC Power ─────────────────────────
def _mk_dcpower(target_w, current_a, *, reached=True):
    """폴링 루프에 상태 읽기 스텁을 넣어 update_measurements 경로로 측정값을 주입."""
    async def _read():
        return (float(target_w), 300.0, float(current_a))

    async def _toggle(on):
        return None

    async def _send(w):
        return None

    d = DCPowerAsync(send_dc_power=_send, send_dc_power_unverified=_send,
                     request_status_read=_read, toggle_enable=_toggle, name="DCTEST")
    d._dc_interval_ms = 50
    d.target_power = float(target_w)
    d._is_running = True
    d._polling_enabled = True
    d._sent_target_reached = reached
    d.cleaned = []

    async def _cleanup():
        d.cleaned.append(1)
        d._is_running = False

    async def _adjust_once():
        return None

    d.cleanup = _cleanup
    d._adjust_once = _adjust_once
    return d


def _run_loop(d, seconds=0.35):
    async def _main():
        t = asyncio.create_task(d._control_loop())
        await asyncio.sleep(seconds)
        d._is_running = False
        with contextlib.suppress(Exception):
            await asyncio.wait_for(t, timeout=1.0)
        await asyncio.sleep(0.05)
    asyncio.run(_main())
    evs = []
    while not d._event_q.empty():
        evs.append(d._event_q.get_nowait())
    return evs


def test_c1_300w_1p5a_trips():
    d = _mk_dcpower(300.0, 1.5)
    evs = _run_loop(d)
    tf = [e for e in evs if e.kind == "target_failed"]
    assert len(tf) == 1 and "DC 과전류" in tf[0].message and "meas=1.500A > 한계 1.000A" in tf[0].message, tf
    assert "I_scale=" in tf[0].message and "raw≈" in tf[0].message
    assert d.cleaned == [1]
    assert abs(d.current_a - 1.5) < 1e-9        # update_measurements 경로를 탔다


def test_c2_300w_0p8a_no_trip():
    d = _mk_dcpower(300.0, 0.8)
    evs = _run_loop(d)
    assert [e for e in evs if e.kind == "target_failed"] == []
    assert d.cleaned == []


def test_c3_trips_during_rampup():
    """★ _sent_target_reached=False(램프업 중)에서도 차단된다."""
    d = _mk_dcpower(300.0, 1.5, reached=False)
    evs = _run_loop(d)
    assert len([e for e in evs if e.kind == "target_failed"]) == 1
    assert d.cleaned == [1]


def test_c4_disabled_cont(_defaults):
    _defaults.setattr(cfgc, "DC_OVERCURRENT_ENABLE_CONT", False, raising=False)
    d = _mk_dcpower(300.0, 1.5)
    evs = _run_loop(d)
    assert [e for e in evs if e.kind == "target_failed"] == []
    assert d.cleaned == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

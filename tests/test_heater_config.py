# -*- coding: utf-8 -*-
"""lib/heater_config.py — 클램프 규칙(래더와 같게)·REAL 2워드 변환. pytest / 직접 실행 모두 가능."""
import os
import sys
import struct
from types import SimpleNamespace

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import pytest                                                   # noqa: E402
from lib.heater_config import HeaterConfig, load_heater_config, float_to_words, words_to_float   # noqa: E402


def _load(**kw):
    return load_heater_config(SimpleNamespace(**kw))


def test_defaults_without_keys():
    cfg, warns = _load()
    assert warns == []
    assert cfg.enabled is False and cfg == HeaterConfig()


def test_enabled_flag():
    cfg, _ = _load(SUPPORTS_HEATER=True)
    assert cfg.enabled is True


@pytest.mark.parametrize("sv,ot,exp_sv,exp_ot", [
    (0, 550, 500.0, 550.0),          # SV ≤0 → 500
    (900, 750, 700.0, 750.0),        # SV >700 → 700
    (500, 0, 500.0, 550.0),          # OT ≤0 → 550
    (500, 900, 500.0, 750.0),        # OT >750 → 750
    (600, 550, 600.0, 630.0),        # OT ≤ SV → SV+30
    (500, 500, 500.0, 530.0),
])
def test_temp_limits(sv, ot, exp_sv, exp_ot):
    cfg, warns = _load(HEATER_SV_LIMIT_C=sv, HEATER_OT_LIMIT_C=ot)
    assert (cfg.sv_limit_c, cfg.ot_limit_c) == (exp_sv, exp_ot)
    assert warns


def test_mv_rules():
    cfg, w = _load(HEATER_MV_ABS_MAX=2000, HEATER_MV_LIMIT=1800)
    assert cfg.mv_abs_max == 1600 and cfg.mv_limit == 1600 and w
    cfg, w = _load(HEATER_MV_LIMIT=300)
    assert cfg.mv_limit == 420 and w                          # [MV_MIN+20, ABS_MAX]
    cfg, w = _load(HEATER_MV_MIN=350)
    assert any("래더 고정값 400" in s for s in w)


def test_ramp_holdback_rules():
    cfg, w = _load(HEATER_RAMP_RATE_C_PER_MIN=3, HEATER_SLOW_RATE_C_PER_MIN=1,
                   HEATER_SLOW_ZONE_C=-5, HEATER_HOLDBACK_C=0)
    assert (cfg.ramp_rate, cfg.slow_rate, cfg.slow_zone_c, cfg.holdback_c) == (6.0, 6.0, 0.0, 10.0)
    assert len(w) == 4


def test_pid_out_of_range_marked_unused():
    cfg, w = _load(HEATER_KP=60, HEATER_TI_S=5)
    assert cfg.kp_valid is False and cfg.ti_valid is False
    assert any("쓰지 않음" in s for s in w)
    cfg, w = _load(HEATER_KP=0.1, HEATER_TI_S=3000)
    assert cfg.kp_valid and cfg.ti_valid and w == []


def test_max_temp_and_approach():
    cfg, _ = _load(HEATER_MAX_TEMP=900)
    assert cfg.max_temp == 500.0
    cfg, _ = _load(HEATER_MAX_TEMP=0)
    assert cfg.max_temp == 500.0
    cfg, _ = _load(HEATER_APPROACH_ZONE_C=-1, HEATER_APPROACH_MIN_RATE_C_PER_MIN=0, HEATER_APPROACH_LEAD_C=0.05)
    assert (cfg.approach_zone, cfg.approach_min_rate, cfg.approach_lead) == (0.0, 1.0, 0.2)
    cfg, _ = _load(HEATER_APPROACH_MIN_RATE_C_PER_MIN=9)
    assert cfg.approach_min_rate == 6.0


def test_hold_mode_tc2_becomes_dac():
    cfg, w = _load(HEATER_HOLD_MODE="tc2")
    assert cfg.hold_mode == "dac" and any("tc2" in s for s in w)
    cfg, w = _load(HEATER_HOLD_MODE="off")
    assert cfg.hold_mode == "off" and w == []


def test_hold_and_period_clamps():
    cfg, _ = _load(HEATER_HOLD_MV_ENTER_TOL_C=0.1, HEATER_HOLD_MV_ENTER_SEC=5, HEATER_HOLD_MV_ARRIVE_TOL_C=50,
                   HEATER_HOLD_MV_DRIFT_PV_C=0, HEATER_HOLD_MV_DRIFT_MV=500, HEATER_MV_SAT_SEC=1)
    assert (cfg.hold_enter_tol, cfg.hold_enter_sec, cfg.hold_arrive_tol, cfg.hold_drift_pv,
            cfg.hold_drift_mv, cfg.sat_sec) == (0.5, 10.0, 10.0, 0.1, 200.0, 10.0)
    cfg, _ = _load(HEATER_POLL_INTERVAL_S=0.1, HEATER_POLL_IDLE_S=30, HEATER_WD_PERIOD_S=20,
                   HEATER_STALE_SEC=1, HEATER_LOG_PERIOD_S=0)
    assert (cfg.poll_interval_s, cfg.poll_idle_s, cfg.wd_period_s, cfg.stale_sec, cfg.log_period_s) == \
        (0.5, 10.0, 9.0, 20.0, 1.0)
    cfg, _ = _load(HEATER_POLL_INTERVAL_S=2, HEATER_POLL_IDLE_S=1)
    assert cfg.poll_idle_s == 2.0                             # [POLL_INTERVAL, 10]


@pytest.mark.parametrize("reg,expect", [(120, 0), (149, 0), (110, 0), (160, 160), (155, 155), (0, 0), (300, 0)])
def test_marker_register_overlap(reg, expect):
    cfg, w = _load(HEATER_SESSION_MARK_REG=reg)
    assert cfg.session_mark_reg == expect
    if reg not in (0, 155, 160):
        assert w


def test_real_words():
    assert float_to_words(3.0) == (0x0000, 0x4040)            # D142=0x0000, D143=0x4040
    assert float_to_words(300.0) == (0x0000, 0x4396)
    for v, bits in ((0.1, 0x3DCCCCCD), (50.0, 0x42480000), (10.0, 0x41200000), (3000.0, 0x453B8000)):
        lo, hi = float_to_words(v)
        assert (hi << 16) | lo == bits, hex((hi << 16) | lo)
        assert words_to_float(lo, hi) == struct.unpack(">f", struct.pack(">f", v))[0]


def test_helpers():
    c = HeaterConfig()
    assert c.c_to_raw(500.0) == 5000 and c.c_to_raw(-1) == 0
    assert c.rate_counts(12.0) == 2 and c.rate_counts(60.0) == 10 and c.rate_counts(1.0) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

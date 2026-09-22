# -*- coding: utf-8 -*-
"""START_SPUTTER min_total_s 하한 계산이 런타임 규칙(delay 행, thickness÷dep_rate 역산)과 같은지 검증.

2026-09-22 실측: 150nm ÷ 0.042972222 nm/s(58.177분) 레시피에서 min_total_s=390 이 나가던 문제(실제 ~68분).
"""
import os
import sys
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402
from controller.process_controller import (                      # noqa: E402
    ProcessController, eta_delay_seconds, eta_row_process_time_min,
    eta_row_min_seconds, eta_min_total_seconds,
)
from lib import config_common as cfgc                            # noqa: E402


def _pc():
    pc = ProcessController.__new__(ProcessController)
    pc._cfg = cfgc
    return pc


def test_1_thickness_recipe_matches_runtime_inversion():
    """CSV 원본 행(process_time 비어 있음) — 2026-09-22 CH1 레시피와 동일 값."""
    row = {"Process_name": "20260921_#1 1mtorr_5sccm", "shutter_delay": "5", "process_time": "",
           "thickness": "150", "dep_rate": "0.042972222"}
    pt = eta_row_process_time_min(row)
    assert abs(pt - 150 / 0.042972222 / 60) < 1e-9            # 58.177분
    expect = (5 + pt) * 60 + 90
    assert abs(eta_min_total_seconds([row], 90.0) - expect) < 1e-6
    v = _pc().estimate_min_total_s([row])
    assert v == round(expect) == 3881                           # 기존 구현은 390
    assert v <= 68 * 60                                         # 실제 소요(~68분)를 넘지 않는 하한


def test_2_dep_rate_key_variants_and_invalid():
    base = {"shutter_delay": "5", "process_time": "", "thickness": "150"}
    assert eta_row_process_time_min({**base, "dep.rate": "0.05"}) == pytest.approx(150 / 0.05 / 60)
    assert eta_row_process_time_min({**base, "dep_rate": "0.05", "dep.rate": "0.01"}) == pytest.approx(150 / 0.05 / 60)  # dep_rate 우선
    assert eta_row_process_time_min({**base, "dep_rate": "0"}) == 0.0          # 0 이하 무효
    assert eta_row_process_time_min({**base, "dep_rate": "abc"}) == 0.0
    assert eta_row_process_time_min({**base, "dep_rate": "0.05", "thickness": "0"}) == 0.0
    assert eta_row_process_time_min({**base, "dep_rate": "0.05", "process_time": "12"}) == 12.0   # 명시값 우선


def test_3_delay_rows():
    assert eta_delay_seconds({"Process_name": "heating", "delay": "10m"}) == 600.0
    assert eta_delay_seconds({"Process_name": "heating", "delay": "20"}) == 1200.0          # 숫자만 → 분
    assert eta_delay_seconds({"Process_name": "x", "delay": "1h30m"}) == 5400.0
    assert eta_delay_seconds({"Process_name": "x", "delay": "90s"}) == 90.0
    assert eta_delay_seconds({"Process_name": "delay 10"}) == 600.0                          # 이름 패턴, 기본 단위 m
    assert eta_delay_seconds({"Process_name": "Delay 2h"}) == 7200.0
    assert eta_delay_seconds({"process_note": "delay 30s"}) == 30.0
    assert eta_delay_seconds({"Process_name": "STO_#14-4", "shutter_delay": "5", "process_time": "300"}) == 0.0
    # delay 행은 대기 초만 더하고 shutter_delay/process_time 은 더하지 않는다(런타임은 continue)
    assert eta_row_min_seconds({"Process_name": "heating", "delay": "10m", "shutter_delay": "5", "process_time": "300"}) == 600.0


def test_4_regression_numeric_process_time_equals_legacy():
    rows = [{"shutter_delay": "5", "process_time": "300"}, {"shutter_delay": "3", "process_time": "12.5"}]
    legacy = ((5 + 300) + (3 + 12.5)) * 60 + 90
    assert _pc().estimate_min_total_s(rows) == round(legacy)
    # STO #14-4 형태: delay 10m + (5 + 300)분
    rows2 = [{"Process_name": "heating", "delay": "10m"}, {"Process_name": "STO_#14-4", "shutter_delay": "5", "process_time": "300"}]
    assert _pc().estimate_min_total_s(rows2) == 600 + (5 + 300) * 60 + 90 == 18990
    assert _pc().estimate_min_total_s(rows2[1:]) == (5 + 300) * 60 + 90         # _eta_min_total_s 의 rows[idx:] 슬라이스


def test_5_edge_cases(monkeypatch):
    pc = _pc()
    assert pc.estimate_min_total_s([]) is None
    assert pc.estimate_min_total_s(None) is None
    assert pc.estimate_min_total_s([None, {}]) == 90                             # 빈/None 행 → 0 + tail
    assert pc.estimate_min_total_s([{"shutter_delay": "x", "process_time": None, "delay": "??"}]) == 90
    monkeypatch.setattr(cfgc, "ETA_ENABLED", False, raising=False)
    assert pc.estimate_min_total_s([{"process_time": "10"}]) is None
    monkeypatch.setattr(cfgc, "ETA_ENABLED", True, raising=False)
    monkeypatch.setattr(cfgc, "ETA_TAIL_MIN_S", 30.0, raising=False)
    assert pc.estimate_min_total_s([{"process_time": "1"}]) == 90


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

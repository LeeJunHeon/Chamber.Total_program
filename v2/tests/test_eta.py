"""ETA(eta.py) — v1 시험 tests/test_eta_min_total.py 의 기대값과 골든 eta_rows, 확정·무효 규칙, 엔진 응답(min_total_s·remaining_s).

v1: min_total_s = Σ 행 하한 + ETA_TAIL_MIN_S 90(파일 레시피일 때만, 단일 시작이면 null), remaining_s = 'Shutter Delay'(없으면 '메인 공정
진행') DELAY 진입 때 1회 확정 — 그 단계부터 DELAY 합 + ETA_TAIL_S 120, THICKNESS_WAIT 가 있으면 확정 안 함, 정지·실패·종료 절차면 null.
"""
import json
from pathlib import Path

import pytest

from v2.ch12 import eta as E
from v2.ch12.process_steps import ProcessStep as S

ROOT = Path(__file__).resolve().parents[2]


def test_1_thickness_recipe_matches_runtime_inversion():
    row = {"Process_name": "20260921_#1 1mtorr_5sccm", "shutter_delay": "5", "process_time": "",
           "thickness": "150", "dep_rate": "0.042972222"}
    pt = E.eta_row_process_time_min(row)
    assert abs(pt - 150 / 0.042972222 / 60) < 1e-9
    expect = (5 + pt) * 60 + 90
    assert abs(E.eta_min_total_seconds([row], 90.0) - expect) < 1e-6
    assert E.estimate_min_total_s([row], {}, 1) == round(expect) == 3881


def test_2_dep_rate_key_variants_and_invalid():
    base = {"shutter_delay": "5", "process_time": "", "thickness": "150"}
    assert E.eta_row_process_time_min({**base, "dep.rate": "0.05"}) == pytest.approx(150 / 0.05 / 60)
    assert E.eta_row_process_time_min({**base, "dep_rate": "0.05", "dep.rate": "0.01"}) == pytest.approx(150 / 0.05 / 60)
    assert E.eta_row_process_time_min({**base, "dep_rate": "0"}) == 0.0
    assert E.eta_row_process_time_min({**base, "dep_rate": "abc"}) == 0.0
    assert E.eta_row_process_time_min({**base, "dep_rate": "0.05", "thickness": "0"}) == 0.0
    assert E.eta_row_process_time_min({**base, "dep_rate": "0.05", "process_time": "12"}) == 12.0


def test_3_delay_rows():
    assert E.eta_delay_seconds({"Process_name": "heating", "delay": "10m"}) == 600.0
    assert E.eta_delay_seconds({"Process_name": "heating", "delay": "20"}) == 1200.0
    assert E.eta_delay_seconds({"Process_name": "x", "delay": "1h30m"}) == 5400.0
    assert E.eta_delay_seconds({"Process_name": "x", "delay": "90s"}) == 90.0
    assert E.eta_delay_seconds({"Process_name": "delay 10"}) == 600.0
    assert E.eta_delay_seconds({"Process_name": "Delay 2h"}) == 7200.0
    assert E.eta_delay_seconds({"process_note": "delay 30s"}) == 30.0
    assert E.eta_delay_seconds({"Process_name": "STO_#14-4", "shutter_delay": "5", "process_time": "300"}) == 0.0
    assert E.eta_row_min_seconds({"Process_name": "heating", "delay": "10m", "shutter_delay": "5",
                                  "process_time": "300"}) == 600.0


def test_4_regression_numeric_process_time_equals_legacy():
    rows = [{"shutter_delay": "5", "process_time": "300"}, {"shutter_delay": "3", "process_time": "12.5"}]
    assert E.estimate_min_total_s(rows, {}, 1) == round(((5 + 300) + (3 + 12.5)) * 60 + 90)
    rows2 = [{"Process_name": "heating", "delay": "10m"}, {"Process_name": "STO_#14-4", "shutter_delay": "5",
                                                           "process_time": "300"}]
    assert E.estimate_min_total_s(rows2, {}, 1) == 600 + (5 + 300) * 60 + 90 == 18990
    assert E.estimate_min_total_s(rows2[1:], {}, 1) == (5 + 300) * 60 + 90


def test_5_edge_cases():
    assert E.estimate_min_total_s([], {}, 1) is None and E.estimate_min_total_s(None, {}, 1) is None
    assert E.estimate_min_total_s([None, {}], {}, 1) == 90
    assert E.estimate_min_total_s([{"shutter_delay": "x", "process_time": None, "delay": "??"}], {}, 1) == 90
    assert E.estimate_min_total_s([{"process_time": "10"}], {"CH1_ETA_ENABLED": False}, 1) is None
    assert E.estimate_min_total_s([{"process_time": "1"}], {"CH1_ETA_TAIL_MIN_S": 30.0}, 1) == 90


def test_thickness_rows_count_shutter_only_when_supported():
    row = {"shutter_delay": "2", "process_time": "", "thickness": "100", "dep_rate": "0.05"}
    assert E.eta_row_min_seconds(row, thickness_supported=True) == 120.0
    assert E.eta_row_min_seconds(row, thickness_supported=False) == pytest.approx((2 + 100 / 0.05 / 60) * 60)


def test_golden_eta_rows_4250():
    gold = json.loads((ROOT / "tests" / "data" / "process_sequences_golden.json").read_text(encoding="utf-8"))
    rows = [{"Process_name": "a", "shutter_delay": "5", "process_time": "10"}, {"Process_name": "w", "delay": "20m"},
            {"Process_name": "b", "shutter_delay": "1", "process_time": "", "thickness": "100", "dep_rate": "0.05"}]
    assert E.estimate_min_total_s(rows, {}, 2) == gold["eta_rows"] == 4250


class _Clk:
    def __init__(self):
        self.t = 0.0

    def monotonic(self):
        return self.t


def _seq():
    return [S("IG_CMD", "베이스", value=1e-5), S("DELAY", "Shutter Delay 5.0분", duration=300_000),
            S("OES_RUN", "OES", params=(600.0, 60), no_wait=True), S("DELAY", "메인 공정 진행 (10분)", duration=600_000)]


def test_confirm_once_at_shutter_delay_and_invalidate():
    c, logs = _Clk(), []
    ec = E.EtaClock({}, 1, c.monotonic)
    st = _seq()
    log = lambda s, m: logs.append(m)                                        # noqa: E731
    ec.try_confirm(st, 0, blocked=False, log=log)
    assert ec.end_t is None and ec.remaining_s(running=True, invalid=False) is None   # 확정 전 null
    c.t = 100.0
    ec.try_confirm(st, 1, blocked=False, log=log)
    assert logs == ["[ETA] 종료 예정 확정: 1020s (타이머 900s + tail 120s)"]
    c.t = 400.0
    assert ec.remaining_s(running=True, invalid=False) == 720
    ec.try_confirm(st, 3, blocked=False, log=log)                              # 두 번째는 무시
    assert len(logs) == 1
    assert ec.remaining_s(running=True, invalid=True) is None                  # 정지·실패·종료 절차
    ec.shift(30.0)
    assert ec.remaining_s(running=True, invalid=False) == 750                  # 일시정지 30 s


def test_no_confirm_with_thickness_wait_or_disabled():
    c = _Clk()
    st = _seq() + [S("THICKNESS_WAIT", "메인 공정 진행 (두께 100nm)", value=100.0, params=(100.0, 400.0))]
    ec = E.EtaClock({}, 2, c.monotonic)
    ec.try_confirm(st, 1, blocked=False, log=lambda s, m: None)
    assert ec.end_t is None
    ec2 = E.EtaClock({"CH2_ETA_ENABLED": False}, 2, c.monotonic)
    ec2.try_confirm(_seq(), 1, blocked=False, log=lambda s, m: None)
    assert ec2.end_t is None


def test_host_eta_block_shape():
    class F:
        def __init__(self, v):
            self.v = v

        def eta_remaining_s(self):
            return self.v
    assert E.host_eta({"ch1": F(None), "ch2": F(42)}) == {"CH1": {"remaining_s": None}, "CH2": {"remaining_s": 42}}
    assert E.host_eta({}) == {"CH1": {"remaining_s": None}, "CH2": {"remaining_s": None}}

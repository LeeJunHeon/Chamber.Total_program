# -*- coding: utf-8 -*-
"""공정 모니터 편차 알림 — 사건 단위(첫 알림 즉시 · 계속 요약 · 정상 복귀 · 셔터 닫힘/공정 종료 요약).

예전: 범위 밖이면 폴링(3초)마다 1건 → 한 공정에 수백 건. 웹훅 대신 log_func 로 받은 문구를 센다
(웹훅은 URL 이 비어 있으면 보내지 않는다). 시각은 now_func 로 넣는다. 장치·네트워크 접근 없음.
"""
import os
import re
import sys
from pathlib import Path

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from controller.process_monitor import ProcessMonitor          # noqa: E402
from lib import config_common, config_ch1, config_ch2          # noqa: E402
from lib._config_loader import load_settings                   # noqa: E402

PARAMS = {"use_ms": True, "use_ar": True, "ar_flow": 20.0, "use_o2": True, "o2_flow": 5.0,
          "working_pressure": 3.0}


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _mon(repeat_s=300.0, recover_s=30.0):
    clk = Clock()
    msgs = []
    m = ProcessMonitor(ch=2, webhook_url="", log_func=lambda src, msg: msgs.append((src, msg)),
                       repeat_s=repeat_s, recover_s=recover_s, now_func=clk)
    m.activate(PARAMS)
    m.notify_shutter_open()
    return m, clk, msgs


def _feed(m, clk, gas, values, step=3.0):
    for v in values:
        m.check_flow(gas, v)
        clk.t += step


def _texts(msgs):
    assert all(src == "Monitor" for src, _ in msgs)
    return [t for _, t in msgs]


def test_first_alert_immediate_with_old_text_then_quiet():
    m, clk, msgs = _mon()
    _feed(m, clk, "Ar", [20.5] + [20.6] * 90)          # 약 273초 동안 계속 범위 밖
    t = _texts(msgs)
    assert t == ["⚠️ CH2 Ar flow 편차\n세팅: 20.00 sccm → 실측: 20.50 sccm (편차: +0.50)"]


def test_pressure_first_alert_old_text():
    m, clk, msgs = _mon()
    m.check_pressure(3.4)
    m.check_pressure(3.5)
    assert _texts(msgs) == ["⚠️ CH2 Pressure 편차\n세팅: 3.000 mTorr → 실측: 3.400 mTorr (편차: +0.400)"]


def test_repeat_summary_every_interval():
    m, clk, msgs = _mon(repeat_s=300.0)
    _feed(m, clk, "Ar", [20.5] * 201)                  # 0 ~ 600초
    t = _texts(msgs)
    assert len(t) == 3
    assert t[1].startswith("⚠️ CH2 Ar flow 편차 계속 (벗어난 지 5분 0초)\n")
    assert t[2].startswith("⚠️ CH2 Ar flow 편차 계속 (벗어난 지 10분 0초)\n")
    assert "최대 편차: +0.50" in t[1] and "범위 밖 101회" in t[1]


def test_repeat_zero_disables_summary():
    m, clk, msgs = _mon(repeat_s=0)
    _feed(m, clk, "Ar", [20.5] * 400)
    assert len(msgs) == 1


def test_recovery_after_hold_time():
    m, clk, msgs = _mon(recover_s=30.0)
    _feed(m, clk, "Ar", [20.5, 20.7, 20.4])            # 0, 3, 6초 범위 밖 (최대 +0.70)
    _feed(m, clk, "Ar", [20.1] * 10)                   # 9 ~ 36초 안 → 아직 30초 안 됨
    assert len(msgs) == 1
    _feed(m, clk, "Ar", [20.1])                        # 39초 — 돌아온 지 30초
    t = _texts(msgs)
    assert len(t) == 2
    assert t[1] == ("✅ CH2 Ar flow 정상 복귀 (벗어나 있던 시간 9초)\n"
                    "세팅: 20.00 sccm → 실측: 20.10 sccm · 최대 편차: +0.70 · 범위 밖 3회")
    _feed(m, clk, "Ar", [20.1] * 20)                   # 복귀 뒤 범위 안 → 조용
    _feed(m, clk, "Ar", [19.6])                        # 새 사건 → 다시 즉시 1건
    t = _texts(msgs)
    assert len(t) == 3 and t[2].startswith("⚠️ CH2 Ar flow 편차\n") and "(편차: -0.40)" in t[2]


def test_brief_return_is_same_excursion():
    m, clk, msgs = _mon(recover_s=30.0)
    _feed(m, clk, "Ar", [20.5])
    _feed(m, clk, "Ar", [20.0] * 5)                    # 15초만 안 → 복귀 아님
    _feed(m, clk, "Ar", [20.6])                        # 다시 밖 → 같은 사건(새 알림 없음)
    _feed(m, clk, "Ar", [20.0] * 11)                   # 30초 이상 안 → 복귀 1건
    t = _texts(msgs)
    assert len(t) == 2
    assert t[1].startswith("✅ CH2 Ar flow 정상 복귀 (벗어나 있던 시간 21초)\n")
    assert "최대 편차: +0.60 · 범위 밖 2회" in t[1]


def test_recover_zero_means_first_in_range_sample():
    m, clk, msgs = _mon(recover_s=0)
    _feed(m, clk, "Ar", [20.5, 20.0])
    t = _texts(msgs)
    assert len(t) == 2 and t[1].startswith("✅ CH2 Ar flow 정상 복귀 (벗어나 있던 시간 3초)\n")


def test_shutter_close_summary_once_then_deactivate_silent():
    m, clk, msgs = _mon()
    _feed(m, clk, "Ar", [20.5] * 40)                   # 0 ~ 117초 밖
    m.check_pressure(3.0)                              # 압력은 정상 — 사건 없음
    m.notify_shutter_close()
    t = _texts(msgs)
    assert len(t) == 2
    assert t[1] == ("⚠️ CH2 Ar flow 편차 — 셔터 닫힐 때까지 계속 (벗어난 지 2분 0초)\n"
                    "세팅: 20.00 sccm → 마지막 실측: 20.50 sccm (편차: +0.50) · 최대 편차: +0.50 · 범위 밖 40회")
    assert not m.is_active
    m.check_flow("Ar", 25.0)                           # 셔터 닫힌 뒤 → 무시
    m.deactivate()
    assert len(msgs) == 2


def test_deactivate_summarizes_open_excursions():
    m, clk, msgs = _mon()
    _feed(m, clk, "O2", [5.5])
    m.check_pressure(2.5)
    clk.t += 60
    m.check_pressure(2.5)
    m.deactivate()
    t = _texts(msgs)
    assert len(t) == 4
    assert t[2].startswith("⚠️ CH2 O2 flow 편차 — 공정 끝날 때까지 계속 (벗어난 지 1분 3초)\n")
    assert t[3].startswith("⚠️ CH2 Pressure 편차 — 공정 끝날 때까지 계속 (벗어난 지 1분 0초)\n")
    assert "마지막 실측: 2.500 mTorr (편차: -0.500)" in t[3]


def test_close_while_returning_reports_recovery():
    m, clk, msgs = _mon(recover_s=30.0)
    _feed(m, clk, "Ar", [20.5, 20.5])
    _feed(m, clk, "Ar", [20.0] * 3)                    # 돌아왔지만 30초 전
    m.notify_shutter_close()
    t = _texts(msgs)
    assert len(t) == 2 and t[1].startswith("✅ CH2 Ar flow 정상 복귀 (벗어나 있던 시간 6초)\n")


def test_keys_are_independent():
    m, clk, msgs = _mon()
    m.check_flow("Ar", 20.5)
    m.check_flow("O2", 5.5)
    m.check_pressure(3.5)
    clk.t += 3
    m.check_flow("Ar", 20.5)
    m.check_flow("O2", 5.5)
    m.check_pressure(3.5)
    t = _texts(msgs)
    assert [x.split("\n")[0] for x in t] == [
        "⚠️ CH2 Ar flow 편차", "⚠️ CH2 O2 flow 편차", "⚠️ CH2 Pressure 편차"]


def test_inactive_and_unknown_gas_ignored():
    m, clk, msgs = _mon()
    m.check_flow("N2", 99.0)                           # 레시피에 없는 가스
    m.notify_shutter_close()
    m.check_flow("Ar", 99.0)                           # 셔터 닫힘
    m.activate({"use_ms": False})
    m.notify_shutter_open()                            # 대상 없음 → 활성화 안 됨
    m.check_pressure(99.0)
    assert msgs == [] and not m.is_active


def test_activate_resets_open_excursion():
    m, clk, msgs = _mon()
    m.check_flow("Ar", 20.5)
    m.activate(PARAMS)                                 # 새 공정(이전 사건은 잊음)
    m.notify_shutter_open()
    m.check_flow("Ar", 20.5)
    assert len(msgs) == 2 and msgs[1][1].startswith("⚠️ CH2 Ar flow 편차\n")


def test_arc_and_refp_stay_immediate():
    m, clk, msgs = _mon()
    for _ in range(3):
        m.notify_arc_warning(3, 2, process_name="P")
        m.notify_refp_warning(25.0, 20.0, process_name="P")
    assert len(msgs) == 6


@pytest.mark.parametrize("bad", ["abc", -5, None, True, float("nan"), float("inf")])
def test_invalid_interval_falls_back_to_default(bad):
    m = ProcessMonitor(ch=1, repeat_s=bad, recover_s=bad)
    assert m._repeat_s() == 300.0 and m._recover_s() == 30.0


@pytest.fixture
def restore_cfg():
    mods = (config_common, config_ch1, config_ch2)
    snap = [dict(vars(md)) for md in mods]
    yield
    for md, d in zip(mods, snap):
        for k in [k for k in vars(md) if k not in d]:
            delattr(md, k)
        for k, v in d.items():
            setattr(md, k, v)


def test_settings_json_values_read_at_runtime(tmp_path, restore_cfg):
    m = ProcessMonitor(ch=2)                           # 생성 뒤 settings 로드해도 반영(실행 시점 읽기)
    assert m._repeat_s() == 300.0 and m._recover_s() == 30.0
    txt = (Path(ROOT) / "config" / "settings.json").read_text(encoding="utf-8")
    for key, val in (("PROCESS_MONITOR_REPEAT_S", "120"), ("PROCESS_MONITOR_RECOVER_S", "15")):
        txt, n = re.subn(rf'("{key}"\s*:\s*)[^,\n]+', lambda mt: mt.group(1) + val, txt, count=1)
        assert n == 1
    p = tmp_path / "settings.json"
    p.write_text(txt, encoding="utf-8")
    load_settings(p)
    assert config_common.PROCESS_MONITOR_REPEAT_S == 120
    assert m._repeat_s() == 120.0 and m._recover_s() == 15.0


def test_repo_settings_json_has_keys():
    load = (Path(ROOT) / "config" / "settings.json").read_text(encoding="utf-8")
    assert re.search(r'"PROCESS_MONITOR_REPEAT_S"\s*:\s*300\b', load)
    assert re.search(r'"PROCESS_MONITOR_RECOVER_S"\s*:\s*30\b', load)
    assert config_common.PROCESS_MONITOR_REPEAT_S == 300.0
    assert config_common.PROCESS_MONITOR_RECOVER_S == 30.0

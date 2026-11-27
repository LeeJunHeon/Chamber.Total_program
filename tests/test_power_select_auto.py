# -*- coding: utf-8 -*-
"""PROCESS_POWER_SELECT_AUTO_ENABLED — 공정 시퀀스의 SW_POWER_SELECT 자동 제어 on/off.

true(기본): 지금과 같다(골든 시퀀스 그대로 — test_process_heater_steps 가 확인).
false: 시작(RF 연속 전 OFF · CH2 RF Pulse 전 ON)·종료(CH2 RF Pulse 뒤 OFF)·긴급 정지(즉시 OFF)에서 SW_POWER_SELECT 단계를
만들지 않는다. SW_RF_SELECT(레시피 Power Select 칸)와 7초 안정화 대기는 그대로. 꺼짐 로그는 시퀀스마다 한 번.
process_controller 는 채널 설정(config_ch2)을 읽으므로 settings.json 은 "ch2" 구역 값이 적용된다.
"""
import asyncio
import contextlib
import os
import re
import sys
import types
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

import test_process_heater_steps as hs                           # noqa: E402
from lib import config_common, config_ch1, config_ch2            # noqa: E402
from lib import _config_loader as L                              # noqa: E402

REPO_JSON = Path(_ROOT) / "config" / "settings.json"
OFF_LOG = "Power Select 자동 제어 꺼짐(설정) — SW_POWER_SELECT 를 바꾸지 않음"


def _pc(ch, auto=None):
    pc = hs._pc(ch)
    if auto is not None:
        d = dict(vars(pc._cfg))
        d["PROCESS_POWER_SELECT_AUTO_ENABLED"] = auto
        pc._cfg = types.SimpleNamespace(**d)
    pc.logs = []
    pc._emit_log = lambda src, msg: pc.logs.append(msg)
    return pc


def _seq(ch, params, auto=None):
    async def go():
        pc = _pc(ch, auto)
        return hs._rows(pc._create_process_sequence(dict(params))), pc.logs
    return asyncio.run(go())


def _is_ps(row):
    return "SW_POWER_SELECT" in row[3]


def _is_rf_sel(row):
    return "SW_RF_SELECT" in row[3]


@pytest.mark.parametrize("name", list(hs.CASES))
def test_true_is_same_as_without_key(name):
    ch, p = hs.CASES[name]
    base, base_logs = _seq(ch, p)
    on, on_logs = _seq(ch, p, auto=True)
    assert on == base and OFF_LOG not in on_logs and OFF_LOG not in base_logs


@pytest.mark.parametrize("name", list(hs.CASES))
def test_false_only_removes_power_select_steps(name):
    ch, p = hs.CASES[name]
    base, _ = _seq(ch, p)
    off, logs = _seq(ch, p, auto=False)
    assert off == [r for r in base if not _is_ps(r)]                 # 다른 단계·순서·문구는 그대로
    assert not any(_is_ps(r) for r in off)
    assert [r for r in off if _is_rf_sel(r)] == [r for r in base if _is_rf_sel(r)]
    assert logs.count(OFF_LOG) == (1 if any(_is_ps(r) for r in base) else 0)


def test_rf_cont_recipe_false():
    base, _ = _seq(*hs.CASES["ch2_dc_rf_boost"])
    off, logs = _seq(*hs.CASES["ch2_dc_rf_boost"], auto=False)
    assert ["PLC_CMD", "Power Select OFF (SW_POWER_SELECT)"] in [r[:2] for r in base]
    assert not any(_is_ps(r) for r in off) and logs.count(OFF_LOG) == 1


def test_rf_pulse_recipe_ch2_false_keeps_rf_select_and_settle():
    base, _ = _seq(*hs.CASES["ch2_rfp_ps"])
    off, _ = _seq(*hs.CASES["ch2_rfp_ps"], auto=False)
    msgs_base = [r[1] for r in base]
    assert "Power Select ON (SW_POWER_SELECT)" in msgs_base and "Power Select OFF (SW_POWER_SELECT)" in msgs_base
    msgs = [r[1] for r in off]
    assert "Power_select: Power Select ON (SW_RF_SELECT)" in msgs and "Power Select 안정화 대기 (7초)" in msgs
    assert not any("SW_POWER_SELECT" in m for m in msgs)


def test_shutdown_force_all_and_emergency_false():
    async def go(auto):
        pc = _pc(2, auto)
        pc._ps_off_logged = False
        shut = hs._rows(pc._create_shutdown_sequence({}, force_all=True))
        pc.current_params = dict(hs.CASES["ch2_rfp_ps"][1])
        emer = hs._rows(pc._create_emergency_shutdown_sequence())
        return shut, emer, pc.logs
    shut, emer, _ = asyncio.run(go(None))
    assert any(_is_ps(r) for r in shut) and ["PLC_CMD", "[긴급] Power Select 즉시 OFF"] in [r[:2] for r in emer]
    shut_off, emer_off, logs = asyncio.run(go(False))
    assert shut_off == [r for r in shut if not _is_ps(r)] and emer_off == [r for r in emer if not _is_ps(r)]
    assert logs.count(OFF_LOG) == 2                                   # 시퀀스마다 한 번


# ═══════════════════════ settings.json → config_ch2 → 시퀀스 ═══════════════════════
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


def _ch2_seq_with(json_path):
    res = L.check_and_load_settings(json_path)
    assert res.ok, res.problems

    async def go():
        pc = hs._pc(2)
        pc._cfg = config_ch2                                           # 실제 런타임과 같은 채널 설정 모듈
        pc._emit_log = lambda *a: None
        return hs._rows(pc._create_process_sequence(dict(hs.CASES["ch2_rfp_ps"][1])))
    return asyncio.run(go())


def _ch2_section_sub(txt, val):
    i = txt.index('"ch2": {')
    head, tail = txt[:i], txt[i:]
    tail, n = re.subn(r'("PROCESS_POWER_SELECT_AUTO_ENABLED"\s*:\s*)[^,\n]+', lambda m: m.group(1) + val, tail, count=1)
    assert n == 1
    return head + tail


def test_repo_settings_default_true(restore_cfg):
    rows = _ch2_seq_with(REPO_JSON)
    assert config_ch2.PROCESS_POWER_SELECT_AUTO_ENABLED is True and any(_is_ps(r) for r in rows)


def test_settings_ch2_false_changes_ch2_sequence(restore_cfg, tmp_path):
    p = tmp_path / "settings.json"
    p.write_text(_ch2_section_sub(REPO_JSON.read_text(encoding="utf-8"), "false"), encoding="utf-8")
    rows = _ch2_seq_with(p)
    assert config_ch2.PROCESS_POWER_SELECT_AUTO_ENABLED is False
    assert not any(_is_ps(r) for r in rows) and any(_is_rf_sel(r) for r in rows)


def test_settings_in_common_only_does_not_reach_ch2(restore_cfg, tmp_path):
    """common 구역에만 넣으면 CH2 공정에는 반영되지 않는다(채널 모듈은 import 때 common 값을 복사) — 설정 위치 안내의 근거."""
    txt = REPO_JSON.read_text(encoding="utf-8")
    txt = txt.replace('"PROCESS_POWER_SELECT_AUTO_ENABLED": true,', "", 1)        # ch2 줄 삭제
    txt = txt.replace('"common": {', '"common": {\n    "PROCESS_POWER_SELECT_AUTO_ENABLED": false,', 1)
    p = tmp_path / "settings.json"
    p.write_text(txt, encoding="utf-8")
    rows = _ch2_seq_with(p)
    assert config_common.PROCESS_POWER_SELECT_AUTO_ENABLED is False and config_ch2.PROCESS_POWER_SELECT_AUTO_ENABLED is True
    assert any(_is_ps(r) for r in rows)


def test_old_exe_rejects_and_workers_ignore(restore_cfg):
    """배포 주의 — 옛 exe(이름 없음) + 새 settings.json: 메인은 시작을 막고, 워커는 ch2 구역을 보지 않아 무시."""
    for md in (config_common, config_ch1, config_ch2):
        if hasattr(md, "PROCESS_POWER_SELECT_AUTO_ENABLED"):
            delattr(md, "PROCESS_POWER_SELECT_AUTO_ENABLED")
    res = L.check_and_load_settings(REPO_JSON)
    assert not res.ok and any("ch2.PROCESS_POWER_SELECT_AUTO_ENABLED" in p for p in res.problems)
    for rel in (("apps", "log_sync_service", "log_sync.py"), ("apps", "rf_service", "rf_worker.py")):
        src = open(os.path.join(_ROOT, *rel), encoding="utf-8").read()
        keys = re.findall(r'"([A-Z0-9_]+)"', re.search(r"SETTINGS_KEYS = \(([^)]*)\)", src, re.S).group(1))
        res = L.check_and_load_settings(REPO_JSON, worker_keys=keys)
        assert res.ok, (rel, res.problems)

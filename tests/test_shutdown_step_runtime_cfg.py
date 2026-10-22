# -*- coding: utf-8 -*-
"""종료 단계 대기값 SHUTDOWN_STEP_* — process_controller 를 먼저 import 한 뒤 load_settings 로 넣은
settings.json 값이 실행 시점 헬퍼에 반영되는지, 잘못된 값이면 기본값인지, 모듈 이름 덮어쓰기(테스트)가 우선인지."""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

import controller.process_controller as m                 # noqa: E402  (load_settings 보다 먼저)
from lib import config_common, config_ch1, config_ch2     # noqa: E402
from lib._config_loader import load_settings              # noqa: E402


@pytest.fixture
def restore_cfg():
    """load_settings 가 바꾼 config 모듈 값을 원래대로 돌린다."""
    mods = (config_common, config_ch1, config_ch2)
    snap = [dict(vars(md)) for md in mods]
    yield
    for md, d in zip(mods, snap):
        for k in [k for k in vars(md) if k not in d]:
            delattr(md, k)
        for k, v in d.items():
            setattr(md, k, v)


def _settings_copy(tmp_path: Path, gap: str, timeout: str | None = None) -> Path:
    txt = (Path(ROOT) / "config" / "settings.json").read_text(encoding="utf-8")
    txt, n = re.subn(r'("SHUTDOWN_STEP_GAP_MS"\s*:\s*)[^,\n]+', lambda mt: mt.group(1) + gap, txt, count=1)
    assert n == 1
    if timeout is not None:
        txt, n = re.subn(r'("SHUTDOWN_STEP_TIMEOUT_MS"\s*:\s*)[^,\n]+', lambda mt: mt.group(1) + timeout, txt, count=1)
        assert n == 1
    p = tmp_path / "settings.json"
    p.write_text(txt, encoding="utf-8")
    return p


def test_settings_value_used_at_runtime(tmp_path, restore_cfg):
    load_settings(_settings_copy(tmp_path, "777", "3333"))
    assert config_common.SHUTDOWN_STEP_GAP_MS == 777
    assert m.SHUTDOWN_STEP_GAP_MS == 500                       # 값 단위 import 는 그대로(옛 값)
    assert m._shutdown_gap_ms() == 777
    assert m._shutdown_timeout_ms() == 3333


@pytest.mark.parametrize("bad", ['"abc"', "-5", "null", "true"])
def test_invalid_value_falls_back_to_default(tmp_path, restore_cfg, bad):
    load_settings(_settings_copy(tmp_path, bad, bad))
    assert m._shutdown_gap_ms() == 500
    assert m._shutdown_timeout_ms() == 2500


def test_module_name_override_wins(tmp_path, restore_cfg, monkeypatch):
    load_settings(_settings_copy(tmp_path, "777"))
    monkeypatch.setattr(m, "SHUTDOWN_STEP_GAP_MS", 0)          # test_process_heater_steps 방식
    assert m._shutdown_gap_ms() == 0
    monkeypatch.setattr(m, "SHUTDOWN_STEP_TIMEOUT_MS", 10)
    assert m._shutdown_timeout_ms() == 10
    monkeypatch.undo()
    assert m._shutdown_gap_ms() == 777                         # 되돌리면 다시 settings 값


def test_defaults_without_settings():
    assert m._shutdown_gap_ms() == float(config_common.SHUTDOWN_STEP_GAP_MS)
    assert m._shutdown_timeout_ms() == float(config_common.SHUTDOWN_STEP_TIMEOUT_MS)

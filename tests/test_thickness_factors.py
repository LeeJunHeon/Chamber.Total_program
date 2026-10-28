# -*- coding: utf-8 -*-
"""lib/thickness_factors.py · lib/thickness_config.py — Factor 파일 읽기·잘못된 값·원자적 저장(한글 경로), 설정 검증.
pytest / 직접 실행 모두 가능."""
import contextlib
import json
import os
import sys
import tempfile
import types
from pathlib import Path

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

import lib.thickness_factors as tf                               # noqa: E402
from lib.thickness_config import DEFAULT_LOG_DIR, base_dir, load_thickness_config  # noqa: E402
from lib.thickness_factors import CHUCKS, GUNS, ThicknessFactors, load_factors, save_factors  # noqa: E402


def test_missing_file_is_blank_table():
    fac, warns = load_factors(Path(tempfile.gettempdir()) / "없는_파일_thickness.json")
    assert warns == []
    assert all(fac.tooling[c][g] is None for c in CHUCKS for g in GUNS)
    assert all(fac.material[g] is None for g in GUNS)


def test_shipped_ch2_file_is_blank_and_valid():
    fac, warns = load_factors(base_dir() / "config" / "thickness_factors_ch2.json")
    assert warns == []
    assert all(fac.tooling[c][g] is None for c in CHUCKS for g in GUNS)


def test_invalid_values_only_that_cell_unset():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "f.json"
        p.write_text(json.dumps({
            "tooling": {"up": {"G1": 120.5, "G2": 5, "G3": "abc"}, "mid": {"G1": 400}, "down": "x"},
            "material": {"G1": {"material": "Aluminum Oxide"},
                         "G2": {"density": 2.7, "z_factor": 1.08, "memo": "Al"},     # 이전 형식 — 경고 후 무시
                         "G3": "x"},
        }), encoding="utf-8")
        fac, warns = load_factors(p)
        assert fac.tooling["up"]["G1"] == 120.5
        assert fac.tooling["up"]["G2"] is None and fac.tooling["up"]["G3"] is None
        assert fac.tooling["mid"]["G1"] is None and fac.tooling["mid"]["G2"] is None
        assert fac.material == {"G1": "Aluminum Oxide", "G2": None, "G3": None}
        assert any("이전 형식" in w for w in warns)
        assert len(warns) == 6, warns
        assert fac.tooling_for("UP", "g1") == 120.5 and fac.material_name("g1") == "Aluminum Oxide"

        p.write_text("{ broken", encoding="utf-8")
        fac, warns = load_factors(p)
        assert len(warns) == 1 and fac.tooling["up"]["G1"] is None


def test_atomic_save_roundtrip_korean_path():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "한글 폴더 & 설정" / "thickness_factors_ch2.json"
        fac = ThicknessFactors()
        fac.tooling["mid"]["G2"] = 98.5
        fac.material["G2"] = "Titanium"
        save_factors(p, fac)
        back, warns = load_factors(p)
        assert warns == [] and back.tooling["mid"]["G2"] == 98.5 and back.material == {"G1": None, "G2": "Titanium", "G3": None}
        assert json.loads(p.read_text(encoding="utf-8"))["material"]["G2"] == {"material": "Titanium"}   # 이름만 저장
        assert [x.name for x in p.parent.iterdir()] == [p.name]           # 임시 파일이 남지 않는다


def test_atomic_save_failure_keeps_original(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "f.json"
        fac = ThicknessFactors()
        fac.tooling["up"]["G1"] = 100.0
        save_factors(p, fac)
        before = p.read_bytes()

        def boom(*_a, **_k):
            raise OSError("disk full")
        monkeypatch.setattr(tf.os, "replace", boom)
        fac.tooling["up"]["G1"] = 200.0
        with pytest.raises(OSError):
            save_factors(p, fac)
        assert p.read_bytes() == before
        assert [x.name for x in Path(tmp).iterdir()] == ["f.json"]


def _mod(**kw):
    return types.SimpleNamespace(**kw)


def test_config_defaults_and_validation():
    cfg, warns = load_thickness_config(_mod(), 2)
    assert not cfg.enabled and warns == []
    assert cfg.factor_file.endswith(str(Path("config") / "thickness_factors_ch2.json"))

    cfg, warns = load_thickness_config(_mod(SUPPORTS_THICKNESS=True, THICKNESS_TCP_PORT=0), 2)
    assert not cfg.enabled and any("PORT" in w for w in warns)                  # 포트 미설정이면 끔

    cfg, warns = load_thickness_config(_mod(
        SUPPORTS_THICKNESS=True, THICKNESS_TCP_HOST="192.168.1.50", THICKNESS_TCP_PORT=4008,
        THICKNESS_POLL_INTERVAL_S=0.01, THICKNESS_SLOW_POLL_S=0.1, THICKNESS_STALE_S=0.5,
        THICKNESS_CMD_TIMEOUT_S=99, THICKNESS_LOG_DIR=r"G:\공유 드라이브\x",
        THICKNESS_FACTOR_FILE="config/thickness_factors_ch2.json"), 2)
    assert cfg.enabled and cfg.port == 4008
    assert cfg.poll_interval_s == 0.25 and cfg.slow_poll_s == 0.25 and cfg.stale_s == 2.0
    assert cfg.cmd_timeout_s == 5.0 and cfg.log_dir == DEFAULT_LOG_DIR
    assert Path(cfg.factor_file) == base_dir() / "config" / "thickness_factors_ch2.json"
    assert len(warns) == 5, warns

    cfg, _ = load_thickness_config(_mod(SUPPORTS_THICKNESS="yes", THICKNESS_TCP_PORT="abc"), 1)
    assert not cfg.enabled                                                        # 형식 오류 → 기본 0 → 끔


def test_settings_json_channels():
    """settings.json 의 ch1/ch2 가 채널 모듈로 들어간다(CH1 끔, CH2 켬)."""
    from lib._config_loader import check_and_load_settings
    res = check_and_load_settings()
    assert res.problems == []
    from lib import config_ch1, config_ch2
    c1, _ = load_thickness_config(config_ch1, 1)
    c2, w2 = load_thickness_config(config_ch2, 2)
    assert not c1.enabled
    assert c2.enabled and c2.port == 4008 and w2 == []
    assert not c2.log_dir.upper().startswith("G:")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

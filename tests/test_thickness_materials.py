# -*- coding: utf-8 -*-
"""lib/thickness_materials.py — 재료 목록: 기본 생성(STM-100 매뉴얼 4.3, 99개, Z '-' → 1.000), 범위·중복 검사,
원자적 저장(한글 경로), 잘못된 항목만 제외, 배포 파일(config/thickness_materials.json) 유효. pytest / 직접 실행 모두 가능."""
import contextlib
import json
import os
import sys
import tempfile
from pathlib import Path

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

import lib.thickness_materials as tm                             # noqa: E402
from lib.thickness_config import base_dir                        # noqa: E402
from lib.thickness_materials import (Z_MISSING_NOTE, Material, default_materials, find_material,  # noqa: E402
                                     load_materials, save_materials)


def test_default_list_from_manual():
    m = default_materials()
    assert len(m) == 99
    z_missing = [x for x in m if x.note == Z_MISSING_NOTE]
    assert len(z_missing) == 18 and all(x.z_factor == 1.0 for x in z_missing)
    al = find_material(m, "aluminum")
    assert (al.symbol, al.density, al.z_factor, al.label) == ("Al", 2.73, 1.08, "Al — Aluminum")
    assert find_material(m, "Aluminum Oxide").note == Z_MISSING_NOTE
    assert find_material(m, "Neodymium Fluoride").density == 6.506
    assert find_material(m, "Carbon (Graphite)").z_factor == 3.26 and find_material(m, "Gold").density == 19.3
    assert len({x.name.casefold() for x in m}) == 99


def test_missing_file_is_created_with_defaults():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "한글 폴더" / "thickness_materials.json"
        mats, warns = load_materials(p)
        assert warns == [] and len(mats) == 99 and p.exists()
        back, w2 = load_materials(p)
        assert w2 == [] and [x.name for x in back] == [x.name for x in mats]
        assert [x.name for x in p.parent.iterdir()] == [p.name]           # 임시 파일 없음


def test_invalid_entries_excluded_with_warning():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "m.json"
        p.write_text(json.dumps({"materials": [
            {"name": "Good", "symbol": "G", "density": 2.0, "z_factor": 1.0, "note": "ok"},
            {"name": "", "symbol": "X", "density": 2.0, "z_factor": 1.0},
            {"name": "LowDensity", "symbol": "L", "density": 0.1, "z_factor": 1.0},
            {"name": "BigZ", "symbol": "Z", "density": 2.0, "z_factor": 12.0},
            {"name": "NotNum", "symbol": "N", "density": "abc", "z_factor": 1.0},
            {"name": "good", "symbol": "g", "density": 3.0, "z_factor": 1.0},      # 대소문자 무시 중복
            "string",
        ]}), encoding="utf-8")
        mats, warns = load_materials(p)
        assert [x.name for x in mats] == ["Good"] and mats[0].note == "ok"
        assert len(warns) == 6 and any("중복" in w for w in warns)
        p.write_text("{ broken", encoding="utf-8")
        mats, warns = load_materials(p)
        assert len(mats) == 99 and len(warns) == 1                         # 읽을 수 없으면 기본 목록(파일은 그대로)
        assert p.read_text(encoding="utf-8") == "{ broken"


def test_save_rejects_duplicates_and_is_atomic(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "m.json"
        save_materials(p, [Material("A", "a", 1.0, 1.0)])
        before = p.read_bytes()
        with pytest.raises(ValueError):
            save_materials(p, [Material("A", "a", 1.0, 1.0), Material("a", "b", 2.0, 1.0)])
        assert p.read_bytes() == before

        def boom(*_a, **_k):
            raise OSError("disk full")
        monkeypatch.setattr(tm.os, "replace", boom)
        with pytest.raises(OSError):
            save_materials(p, [Material("B", "b", 1.0, 1.0)])
        assert p.read_bytes() == before and [x.name for x in Path(tmp).iterdir()] == ["m.json"]


def test_shipped_file_is_valid_default():
    mats, warns = load_materials(base_dir() / "config" / "thickness_materials.json", create=False)
    assert warns == [] and len(mats) == 99
    assert [(x.name, x.density, x.z_factor) for x in mats] == [(x.name, x.density, x.z_factor) for x in default_materials()]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

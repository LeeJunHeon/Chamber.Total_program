# -*- coding: utf-8 -*-
"""apps/rf_service/rf_worker.py 의 MODES 에 "ALL" 이 포함되는지 (텍스트 검사 — 워커 import 는 무겁다)."""
import os
import re
import sys
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest   # noqa: E402


def test_rf_worker_modes_include_all():
    p = os.path.join(_ROOT, "apps", "rf_service", "rf_worker.py")
    with open(p, encoding="utf-8") as f:
        src = f.read()
    m = re.search(r'^MODES\s*=\s*\[([^\]]*)\]', src, re.M)
    assert m, "MODES 정의를 찾지 못했다"
    modes = [s.strip().strip('"\'') for s in m.group(1).split(",") if s.strip()]
    assert modes == ["CH1", "CH2", "CLEANING", "ALL"], modes


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

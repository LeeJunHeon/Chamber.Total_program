# -*- coding: utf-8 -*-
"""DC Pulse 전압 환산 상수 정정(1.468815 → 1.0) 검증.

t1  lib.config_common 의 PIV 상수값 (V/LSB=1.0, A/LSB=0.01, W/LSB=10.0, V_SET_STEP=1.0)
t2  config/settings.json 의 DCP_V_MEAS_V_PER_LSB 가 모든 위치에서 1.0
t3  device/dc_pulse.py 에 `_cfg_float("DCP_V_MEAS_V_PER_LSB", 1.468815)` 폴백이 남아 있지 않다
t4  환산 검산: 실측 (P_raw,I_raw,V_raw) 에서 P_W ≈ V_V x I_A (오차 8% 이내)
t5  _piv_scale_check 의 오차 계산·임계 판정·건너뛰기
"""
import os
import re
import sys
import json
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import lib.config_common as cc                                  # noqa: E402
from device.dc_pulse import _piv_scale, _piv_scale_check, PIV_SCALE_ERR_MAX_PCT   # noqa: E402

_SAMPLES = [(8, 54, 147), (8, 58, 138), (5, 25, 200)]   # 2026-09-17~19 실측 (P_raw, I_raw, V_raw)


def _strip_jsonc(text: str) -> str:
    """// 및 /* */ 주석 제거(문자열 내부 보존) + 트레일링 콤마 제거."""
    out, i, n, in_str = [], 0, len(text), False
    while i < n:
        c = text[i]
        if in_str:
            out.append(c)
            if c == "\\" and i + 1 < n:
                out.append(text[i + 1]); i += 1
            elif c == '"':
                in_str = False
        elif c == '"':
            in_str = True; out.append(c)
        elif text.startswith("//", i):
            while i < n and text[i] != "\n":
                i += 1
            continue
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            i = n if j < 0 else j + 2
            continue
        else:
            out.append(c)
        i += 1
    return re.sub(r",(\s*[}\]])", r"\1", "".join(out))


def _walk(obj, key):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key:
                yield v
            yield from _walk(v, key)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v, key)


def test_t1_config_common_constants():
    assert cc.DCP_V_MEAS_V_PER_LSB == 1.0
    assert cc.DCP_I_MEAS_A_PER_LSB == 0.01
    assert cc.DCP_P_MEAS_W_PER_LSB == 10.0
    assert cc.DCP_V_SET_STEP_V == 1.0


def test_t2_settings_json_all_positions():
    p = os.path.join(_ROOT, "config", "settings.json")
    with open(p, encoding="utf-8") as f:
        data = json.loads(_strip_jsonc(f.read()))
    vals = list(_walk(data, "DCP_V_MEAS_V_PER_LSB"))
    assert vals, "settings.json 에 DCP_V_MEAS_V_PER_LSB 가 없다"
    assert all(float(v) == 1.0 for v in vals), vals


def test_t3_no_legacy_fallback_in_source():
    p = os.path.join(_ROOT, "device", "dc_pulse.py")
    with open(p, encoding="utf-8") as f:
        src = f.read()
    # 주석 속 이력 설명은 허용 — 코드 폴백/대입 형태만 금지
    assert not re.search(r'_cfg_float\(\s*"DCP_V_MEAS_V_PER_LSB"\s*,\s*1\.468815', src)
    assert not re.search(r'_v_meas_v_per_lsb\s*=\s*1\.468815', src)


def test_t4_scale_consistency_on_measured_samples():
    for P_raw, I_raw, V_raw in _SAMPLES:
        r = _piv_scale({"P": P_raw, "I": I_raw, "V": V_raw},
                       cc.DCP_P_MEAS_W_PER_LSB, cc.DCP_I_MEAS_A_PER_LSB, cc.DCP_V_MEAS_V_PER_LSB)
        eng = r["eng"]
        assert r["raw"] == {"P": P_raw, "I": I_raw, "V": V_raw}
        err = abs(eng["P_W"] - eng["V_V"] * eng["I_A"]) / eng["P_W"] * 100.0
        assert err <= 8.0, (P_raw, I_raw, V_raw, eng, err)
    # 구 상수로는 어긋난다는 것도 고정(회귀 방지)
    bad = _piv_scale({"P": 8, "I": 54, "V": 147}, 10.0, 0.01, 1.468815)["eng"]
    assert abs(bad["P_W"] - bad["V_V"] * bad["I_A"]) / bad["P_W"] * 100.0 > 30.0


def test_t5_scale_check_judgement():
    assert PIV_SCALE_ERR_MAX_PCT == 15.0
    ok, err = _piv_scale_check(80.0, 0.54, 147.0)
    assert ok and abs(err - 0.775) < 0.01
    ok, err = _piv_scale_check(80.0, 0.54, 147.0 * 1.468815)
    assert (not ok) and err > 40.0
    # 임계 경계
    assert _piv_scale_check(100.0, 1.0, 115.0)[0] is True
    assert _piv_scale_check(100.0, 1.0, 115.01)[0] is False
    # P 또는 I 가 0 → 판정 불가(None)
    assert _piv_scale_check(0.0, 0.5, 100.0) is None
    assert _piv_scale_check(50.0, 0.0, 100.0) is None
    # 분모 하한 max(1, P)
    _, err = _piv_scale_check(0.5, 0.1, 10.0)
    assert abs(err - 50.0) < 1e-9


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"{len(fns)} tests passed")

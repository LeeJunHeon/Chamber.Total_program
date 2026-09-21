# -*- coding: utf-8 -*-
"""DC 과전류 인터락의 전류 상한 — 세트포인트 전력에 대한 선형 기준.

lib 는 device/runtime 을 import 하지 않으므로 DC Pulse / DC Power 가 안전하게 공유한다.

    P_set <= P1        → I1
    P1 < P_set < P2    → I1 + (P_set - P1) * (I2 - I1) / (P2 - P1)
    P_set >= P2        → extrapolate=True : 같은 기울기로 연장
                         extrapolate=False: I2 로 클램프
기본값 300W/1.0A ~ 600W/2.0A → 300W 이하 1.0A, 450W 1.5A, 600W 2.0A, 900W 3.0A
"""
from __future__ import annotations

import math
from typing import Optional

from lib import config_common as cfgc


def dc_overcurrent_limit_a(p_set_w, *, p1: Optional[float] = None, i1: Optional[float] = None,
                           p2: Optional[float] = None, i2: Optional[float] = None,
                           extrapolate: Optional[bool] = None) -> float:
    """세트포인트 전력 p_set_w(W) 에 대한 전류 상한(A). 순수 함수 — 예외를 내지 않는다.

    인자가 None 이면 config_common 의 DC_OVERCURRENT_* 를 읽는다(런타임 변경 반영).
    비정상 인자(p2 <= p1, NaN 등)나 None/음수 세트포인트는 i1(보수적)로 폴백한다.
    """
    try:
        _i1 = float(getattr(cfgc, "DC_OVERCURRENT_I1_A", 1.0) if i1 is None else i1)
    except Exception:
        _i1 = 1.0
    if not math.isfinite(_i1) or _i1 <= 0.0:
        _i1 = 1.0
    try:
        _p1 = float(getattr(cfgc, "DC_OVERCURRENT_P1_W", 300.0) if p1 is None else p1)
        _p2 = float(getattr(cfgc, "DC_OVERCURRENT_P2_W", 600.0) if p2 is None else p2)
        _i2 = float(getattr(cfgc, "DC_OVERCURRENT_I2_A", 2.0) if i2 is None else i2)
        _ext = bool(getattr(cfgc, "DC_OVERCURRENT_EXTRAPOLATE", True) if extrapolate is None else extrapolate)
        p = float(p_set_w) if p_set_w is not None else 0.0
    except Exception:
        return _i1
    if not all(math.isfinite(x) for x in (_p1, _p2, _i2, p)):
        return _i1
    if _p2 <= _p1 or _i2 <= 0.0:
        return _i1
    if p <= _p1:
        return _i1
    slope = (_i2 - _i1) / (_p2 - _p1)
    if p >= _p2:
        lim = _i2 + (p - _p2) * slope if _ext else _i2
    else:
        lim = _i1 + (p - _p1) * slope
    if not math.isfinite(lim) or lim <= 0.0:
        return _i1
    return lim

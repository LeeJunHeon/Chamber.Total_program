# rf_reader_v3/preprocess.py
# -*- coding: utf-8 -*-
"""
숫자 크롭 전처리 (학습/추론 공통).

- align_com: 밝은 획의 무게중심을 중앙으로 평행이동 → 슬롯 위치 흔들림 제거.
              클러스터 평균이 선명해지고 분류 일관성↑. 결정적/저비용이라 추론에도 동일 적용.
- norm_feat: 클러스터링/분류 피처용 다운스케일 + per-image 표준화.
"""
from __future__ import annotations

import cv2
import numpy as np

from rf_config_defs import DIGIT_W, DIGIT_H


def align_com(crop):
    """밝은 획 무게중심을 중앙으로 이동(번짐 영향 줄이려 상대임계 후 COM)."""
    g = crop.astype(np.float32)
    lo = np.percentile(g, 40); hi = np.percentile(g, 98)
    if hi - lo < 12:
        return crop.copy()
    m = (g > lo + 0.5 * (hi - lo)).astype(np.float32)
    if m.sum() < 5:
        return crop.copy()
    ys, xs = np.nonzero(m)
    cy, cx = ys.mean(), xs.mean()
    H, W = crop.shape
    dx = int(round(W / 2.0 - cx)); dy = int(round(H / 2.0 - cy))
    M = np.float32([[1, 0, dx], [0, 1, dy]])
    return cv2.warpAffine(crop, M, (W, H), flags=cv2.INTER_LINEAR, borderValue=int(g.min()))


def norm_feat(crop, fw=14, fh=20):
    """피처 벡터: 다운스케일 + per-image 표준화."""
    s = cv2.resize(crop, (fw, fh), interpolation=cv2.INTER_AREA).astype(np.float32)
    s -= s.mean(); s /= (s.std() + 1e-6)
    return s.ravel()

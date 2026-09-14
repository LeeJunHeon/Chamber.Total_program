# rf_reader_v3/geometry.py
# -*- coding: utf-8 -*-
"""
원근보정 + 칸 크롭 + 단일 숫자 분할.

흐름:  세로원본 → 패널별 warp → 칸 ROI 크롭(R채널) → 숫자블록 위치탐지
       → 3등분 슬롯 → 정규화 단일 숫자 크롭(+부호).

7-세그먼트는 문자 피치가 물리적으로 고정 → '숫자블록을 찾아 3등분'이 매우 견고.
'1'은 슬롯 안에서 가늘게 들어오지만 분류기가 슬롯 단위로 보므로 문제 없음.
"""
from __future__ import annotations

import cv2
import numpy as np

from rf_config_defs import PANELS, ROIS, PANEL_OF, DIGIT_W, DIGIT_H


def build_homographies():
    mats = {}
    for name, p in PANELS.items():
        w, h = p["size"]
        mats[name] = cv2.getPerspectiveTransform(
            np.float32(p["src"]), np.float32([[0, 0], [w, 0], [w, h], [0, h]]))
    return mats


def warp_panels(img, mats):
    return {n: cv2.warpPerspective(img, mats[n], PANELS[n]["size"]) for n in PANELS}


def map_roi(roi, M):
    """원본 좌표 ROI [y0,y1,x0,x1] → warp 좌표 bbox [x0,y0,x1,y1]."""
    y0, y1, x0, x1 = roi
    pts = np.float32([[x0, y0], [x1, y0], [x1, y1], [x0, y1]]).reshape(-1, 1, 2)
    w = cv2.perspectiveTransform(pts, M).reshape(-1, 2)
    return [int(round(w[:, 0].min())), int(round(w[:, 1].min())),
            int(round(w[:, 0].max())), int(round(w[:, 1].max()))]


# 칸 크롭 시 ROI 박스 주변 여유 (warp 픽셀).
PAD_T, PAD_B = 6, 6        # 숫자 높이 여유
PAD_X = 0.10               # 슬롯 폭 대비 좌우 여유 비율
SIGN_W = 46               # 부호 검출용 박스 왼쪽 영역 폭(warp px)


def crop_block_red(warp_panel, box):
    """warp 패널에서 '숫자블록'(보정된 ROI 박스) R채널 크롭. (red, sign_red) 반환.
    sign_red: 블록 왼쪽 부호 영역 R채널 크롭."""
    x0, y0, x1, y1 = box
    H, W = warp_panel.shape[:2]
    Y0 = max(0, y0 - PAD_T); Y1 = min(H, y1 + PAD_B)
    X0 = max(0, x0); X1 = min(W, x1)
    red = warp_panel[Y0:Y1, X0:X1, 2]
    sx0 = max(0, x0 - SIGN_W)
    sign_red = warp_panel[Y0:Y1, sx0:max(sx0 + 1, x0), 2]
    return red, sign_red


def detect_sign(sign_red):
    """부호 영역 R채널에서 가로 막대(-) 존재 여부.
    상대임계(영역 내 밝은 픽셀)로 중앙밴드 vs 외곽 비교."""
    if sign_red is None or sign_red.size == 0:
        return False
    g = sign_red.astype(np.float32)
    hi = np.percentile(g, 92)
    lo = np.percentile(g, 30)
    if hi - lo < 18:               # 부호 영역에 밝은 획 자체가 없음
        return False
    thr = lo + 0.55 * (hi - lo)
    m = g > thr
    H, W = m.shape
    band = m[int(0.36 * H):int(0.64 * H), :]
    outer = np.concatenate([m[:int(0.28 * H), :].ravel(),
                            m[int(0.74 * H):, :].ravel()])
    cen = band.sum(); out = outer.sum()
    return bool(band.size and cen > 0.12 * band.size and cen > 1.8 * out + 2)


def split_digits(red, n_digits=3):
    """
    숫자블록 R채널 → 단일 숫자 크롭 리스트 (그레이, 번짐 포함 그대로).
    블록을 n등분(7-seg 피치 고정 가정). 각 크롭 (DIGIT_H, DIGIT_W) uint8.
    """
    H, W = red.shape
    if W < n_digits * 4 or H < 4:
        return []
    slot = W / n_digits
    pad_x = int(PAD_X * slot)
    digits = []
    for i in range(n_digits):
        sx0 = max(0, int(i * slot) - pad_x)
        sx1 = min(W, int((i + 1) * slot) + pad_x)
        d = red[:, sx0:sx1]
        if d.size == 0:
            return []
        d = cv2.resize(d, (DIGIT_W, DIGIT_H), interpolation=cv2.INTER_AREA)
        digits.append(d)
    return digits

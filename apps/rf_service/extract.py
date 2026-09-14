# rf_reader_v3/extract.py
# -*- coding: utf-8 -*-
"""
보정(slots.json) 기반 단일 숫자/부호 추출.

프레임 → 패널 warp → (칸별) 보정된 3슬롯 R채널 크롭 + 부호박스 → 정규화 숫자.
분류기에는 그레이스케일(번짐 포함)을 그대로 먹인다.
"""
from __future__ import annotations

import json
import os
import sys

import cv2
import numpy as np

from rf_config_defs import PANELS, DIGIT_W, DIGIT_H
from geometry import build_homographies, warp_panels

def _asset_base():
    """에셋(slots.json) 기준 폴더. frozen: sys._MEIPASS(없으면 exe 폴더) / dev: 이 파일 폴더."""
    if getattr(sys, "frozen", False):
        return getattr(sys, "_MEIPASS", os.path.dirname(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


SLOTS_PATH = os.path.join(_asset_base(), "calibration", "slots.json")

SLOT_PAD_X = 2     # 슬롯 좌우 여유(px)
SLOT_PAD_Y = 2


def load_slots(path=SLOTS_PATH):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _norm_digit(crop):
    """그레이 숫자 크롭 → (DIGIT_H, DIGIT_W) uint8."""
    if crop.size == 0:
        return None
    return cv2.resize(crop, (DIGIT_W, DIGIT_H), interpolation=cv2.INTER_AREA)


def detect_sign(panel_img, sign_box):
    """부호 영역 R채널에서 가로 막대(-) 검출.
    마이너스 = 세로 중앙에만 밝은 가로획, 위/아래는 어두움.
    숫자 세로획 bleed를 피하려 박스 좌측 75%만 사용하고, 행 밝기 프로파일로 판정.
    반환: (is_neg, score)."""
    sx0, sy0, sx1, sy1 = sign_box
    reg = panel_img[sy0:sy1, sx0:sx1, 2].astype(np.float32)
    if reg.size == 0 or reg.shape[0] < 6:
        return False, 0.0
    # 우측(숫자 인접) 제외
    W = reg.shape[1]
    reg = reg[:, :max(3, int(0.78 * W))]
    H = reg.shape[0]
    row = reg.mean(axis=1)
    lo, hi = np.percentile(reg, 20), np.percentile(reg, 98)
    if hi - lo < 16:                      # 박스에 밝은 획 자체가 없음 → 부호 없음
        return False, 0.0
    rown = (row - lo) / (hi - lo + 1e-6)
    mid = rown[int(0.32 * H):int(0.68 * H)]
    edge = np.concatenate([rown[:int(0.24 * H)], rown[int(0.80 * H):]])
    mid_peak = float(mid.max()) if mid.size else 0.0
    edge_lvl = float(edge.mean()) if edge.size else 0.0
    score = mid_peak - edge_lvl
    is_neg = bool(mid_peak > 0.55 and score > 0.30)
    return is_neg, float(score)


def extract_cell(panels, cell_calib):
    """한 칸 → (digit_crops[3], sign_bool, sign_score)."""
    panel_img = panels[cell_calib["panel"]]
    H, W = panel_img.shape[:2]
    y0 = max(0, cell_calib["y0"] - SLOT_PAD_Y)
    y1 = min(H, cell_calib["y1"] + SLOT_PAD_Y)
    crops = []
    for (sx0, sx1) in cell_calib["slots"]:
        a = max(0, sx0 - SLOT_PAD_X); b = min(W, sx1 + SLOT_PAD_X)
        d = panel_img[y0:y1, a:b, 2]
        crops.append(_norm_digit(d))
    is_neg, score = detect_sign(panel_img, cell_calib["sign"])
    return crops, is_neg, score


def extract_frame(img, mats=None, slots=None):
    """전체 프레임 → {label: (digit_crops[3], sign_bool, sign_score)}."""
    if mats is None:
        mats = build_homographies()
    if slots is None:
        slots = load_slots()
    panels = warp_panels(img, mats)
    out = {}
    for lbl, cc in slots.items():
        out[lbl] = extract_cell(panels, cc)
    return out

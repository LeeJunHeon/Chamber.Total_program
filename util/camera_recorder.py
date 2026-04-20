# util/camera_recorder.py
# -*- coding: utf-8 -*-
"""
CameraRecorder  (v2.0 — 인식율 최대화 중심 개편)
==============================================
공정 중 RF 매칭 컨트롤 패널 디스플레이를 카메라로 읽어 CSV + 이미지로 저장.

v1 대비 주요 개선 (동작 / 결과만 개선, Public API 불변)
--------------------------------------------------------
1. 디스플레이 ON/OFF 감지 (R-G 차분 기반) — OFF인 ROI는 OCR 자체를 건너뛴다.
   → CH2 공정 중 CH1 OFF ROI의 허위 인식(약 30~60%) 제거.
2. 디짓-단위 분리 후 개별 OCR — 수평 연결요소로 각 자리를 분리,
   단일-문자 Tesseract → 자릿수 누락/공백오인식 대폭 감소.
3. 음수 부호(-) ROI 인접 감지 — ROI 좌측에 가로선형 요소 존재 시
   '-' 접두, 자릿수 판정도 signed에 맞게 보정.
4. 결과 정규화 — 기대 자릿수보다 1개 부족하면 leading-zero 패딩 허용.
5. 시계열 정합성 검증 — 최근 N개 중앙값 대비 급변 시 의심값 마스킹(기본 기록은 유지, 별도 플래그).
6. 캐스케이드 확장 — CLAHE 강조, 적응형 임계, per-digit fallback 추가.
7. ROI별 누적 성공/실패 카운터 — 공정 종료 시 요약 로그.

저장 구조 (v1과 동일)
----------------------
//VanaM_NAS/VanaM_Sputter/Sputter/Logs/CH1&2/Camera_Logs/
├── CH1/
│   ├── raw/
│   │   └── 20260326_143022/
│   │       ├── 143022_0001.jpg
│   │       └── ...
│   └── CH1_20260326_143022.csv
├── CH2/
└── CLEANING/

설계 원칙 (v1과 동일)
----------------------
- threading.Thread(daemon=True) → asyncio/Qt 루프에 영향 없음
- start() / stop() 논블로킹, 즉시 반환
- OCR 실패 / 카메라 오류 → 내부에서 처리, 메인 공정에 예외 전파 없음
- NAS 접근 불가 시 로컬 경로(rf_logs/)로 자동 폴백
- Public API 완전 호환: CameraRecorder(camera_index, interval), start(mode), stop(), is_running
"""

from __future__ import annotations

import csv
import sys
import json
import logging
import os
import platform
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Optional

import cv2
import numpy as np

try:
    import pytesseract
    if platform.system() == "Windows":
        # PyInstaller 빌드 시 exe 옆 Tesseract-OCR 폴더 우선 탐색
        if getattr(sys, "frozen", False):
            _base = Path(sys.executable).parent
        else:
            _base = Path(__file__).resolve().parent.parent

        _candidates = [
            _base / "Tesseract-OCR" / "tesseract.exe",
            Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe"),
            Path(r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe"),
        ]
        _found = next((str(p) for p in _candidates if p.exists()), None)
        if _found:
            pytesseract.pytesseract.tesseract_cmd = _found
    _TESSERACT_OK = True
except ImportError:
    _TESSERACT_OK = False

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────
# 저장 루트
# ──────────────────────────────────────────────────────────
NAS_LOG_ROOT   = Path(r"\\VanaM_NAS\VanaM_Sputter\Sputter\Logs\CH1&2\Camera_Logs")
LOCAL_FALLBACK = Path("rf_logs")   # NAS 접근 불가 시 폴백

# ──────────────────────────────────────────────────────────
# 기본 ROI
# (주: ROI는 1920x1080 → 세로 회전 후 기준이므로 y가 세로축, x가 가로축)
# ──────────────────────────────────────────────────────────
_DEFAULT_ROIS = [
    [571, 610,  191, 291],   # CH1_FWD
    [586, 628,  405, 496],   # CH1_REF
    [604, 642,  630, 715],   # CH1_LOAD
    [611, 650,  794, 886],   # CH1_TUNE
    [997,1042,  277, 413],   # RF3_LOAD  (음수 부호 포함)
    [994,1038,  465, 605],   # RF3_TUNE  (음수 부호 포함)
    [1372,1417, 203, 295],   # CH2_FWD
    [1348,1394, 384, 499],   # CH2_REF
    [1332,1375, 598, 714],   # CH2_LOAD
    [1312,1350, 775, 878],   # CH2_TUNE
]

_ALL_LABELS = [
    "CH1_FWD", "CH1_REF", "CH1_LOAD", "CH1_TUNE",
    "RF3_LOAD", "RF3_TUNE",
    "CH2_FWD", "CH2_REF", "CH2_LOAD", "CH2_TUNE",
]

# RF3와 CH2_* 일부 디스플레이는 음수 표시 발생 → signed=True로 관리
_SIGNED_LABELS = {"RF3_LOAD", "RF3_TUNE", "CH2_REF", "CH2_LOAD", "CH2_TUNE"}

# ──────────────────────────────────────────────────────────
# ON/OFF 판별 임계값 (실측 기반)
# 측정: ON  → r_max=255,   rg_max=150~227,  rg_p95=132~167
#       OFF → r_max<170,   rg_max=0~14,     rg_p95=0~4
# 넉넉한 안전 마진으로 분리
# ──────────────────────────────────────────────────────────
ONOFF_R_MAX_MIN    = 200    # r_max ≥ 200 (ON은 보통 255)
ONOFF_RG_MAX_MIN   = 40     # rg_max ≥ 40 (ON 150+, OFF ≤15)
ONOFF_RG_P95_MIN   = 25     # rg_p95 ≥ 25 (ON 130+, OFF ≤5)

# ──────────────────────────────────────────────────────────
# 시계열 검증 파라미터
# ──────────────────────────────────────────────────────────
TEMPORAL_HISTORY_N = 5      # 최근 몇 개 값 추적
TEMPORAL_MAD_THRESH = 3.0   # 중앙값 대비 MAD(절대편차 중앙값)의 n배 초과 시 의심
TEMPORAL_MIN_SAMPLES = 3    # 검증 활성화 최소 샘플수

# ──────────────────────────────────────────────────────────
# 진단 이미지 저장 임계값
# ──────────────────────────────────────────────────────────
SPIKE_THRESHOLD_PCT: float = 20.0  # 이전 값 대비 이 % 이상 변하면 diag 저장

# ──────────────────────────────────────────────────────────
# ROI별 캐스케이드 OCR 설정
# method 종류:
#   "norm_otsu"   = R채널 → 확대 → 정규화(0~255) → OTSU
#   "norm_fixed"  = R채널 → 확대 → 정규화(0~255) → 고정 tv
#   "rg_otsu"     = (R-G)차분 → 확대 → 정규화 → OTSU
#   "rg_fixed"    = (R-G)차분 → 확대 → 정규화 → 고정 tv
#   "fixed"       = R채널 → 확대 → 고정 tv 이진화 (정규화 없음)
#   "clahe_otsu"  = R채널 → 확대 → CLAHE → OTSU  (신규)
#   "rg_adaptive" = (R-G)차분 → 확대 → 정규화 → 적응형 임계 (신규)
#
# 추가 옵션:
#   morph_k: CLOSE 커널 크기 (기본 2)
#   border:  Tesseract 입력 패딩 (기본 15)
# ──────────────────────────────────────────────────────────
_DEFAULT_PARAMS = [
    # CH1_FWD — 기대 자릿수 3, 음수 없음. 와이드 간격 → 개별 디짓 fallback 유효.
    {"scale": 6, "digits": 3, "signed": False, "methods": [
        {"method": "norm_otsu",   "psm": 7},
        {"method": "rg_otsu",     "psm": 7},
        {"method": "fixed",       "tv": 110, "psm": 7},
        {"method": "norm_fixed",  "tv": 128, "psm": 7},
        {"method": "clahe_otsu",  "psm": 7},
        {"method": "norm_otsu",   "psm": 8},
    ]},
    # CH1_REF — 자릿수 3
    {"scale": 6, "digits": 3, "signed": False, "methods": [
        {"method": "norm_fixed",  "tv": 128, "psm": 7},
        {"method": "norm_otsu",   "psm": 7},
        {"method": "rg_otsu",     "psm": 7},
        {"method": "fixed",       "tv": 110, "psm": 7},
        {"method": "clahe_otsu",  "psm": 7},
    ]},
    # CH1_LOAD — 안정값(123~126), 자릿수 3
    {"scale": 6, "digits": 3, "signed": False, "methods": [
        {"method": "fixed",       "tv": 140, "psm": 7},
        {"method": "norm_otsu",   "psm": 7},
        {"method": "rg_otsu",     "psm": 7},
    ]},
    # CH1_TUNE — 변화가 큼(0~999), 자릿수 3
    {"scale": 6, "digits": 3, "signed": False, "methods": [
        {"method": "fixed",       "tv": 120, "psm": 7},
        {"method": "norm_otsu",   "psm": 7},
        {"method": "rg_otsu",     "psm": 7},
        {"method": "fixed",       "tv": 100, "psm": 8},
        {"method": "clahe_otsu",  "psm": 7},
        {"method": "norm_fixed",  "tv": 128, "psm": 8},
    ]},
    # RF3_LOAD — signed, 자릿수 4(부호 포함)
    {"scale": 6, "digits": 4, "signed": True, "methods": [
        {"method": "fixed",       "tv": 150, "psm": 7},
        {"method": "norm_otsu",   "psm": 7},
        {"method": "rg_otsu",     "psm": 7},
        {"method": "fixed",       "tv": 150, "psm": 8},
        {"method": "clahe_otsu",  "psm": 7},
    ]},
    # RF3_TUNE — signed, 자릿수 4(부호 포함). 기존 V1에서 가장 말썽 많았음.
    {"scale": 6, "digits": 4, "signed": True, "methods": [
        {"method": "norm_fixed",  "tv": 170, "psm": 8},
        {"method": "rg_otsu",     "psm": 7},
        {"method": "fixed",       "tv": 160, "psm": 7},
        {"method": "norm_fixed",  "tv": 180, "psm": 8},
        {"method": "norm_otsu",   "psm": 8},
        {"method": "clahe_otsu",  "psm": 8},
        {"method": "rg_fixed",    "tv": 120, "psm": 7},
    ]},
    # CH2_FWD — 자릿수 3
    {"scale": 6, "digits": 3, "signed": False, "methods": [
        {"method": "norm_otsu",   "psm": 7},
        {"method": "rg_otsu",     "psm": 7},
        {"method": "fixed",       "tv": 110, "psm": 7},
        {"method": "norm_fixed",  "tv": 128, "psm": 7},
        {"method": "clahe_otsu",  "psm": 7},
    ]},
    # CH2_REF — signed 허용 (디스플레이가 -000 표시 가능), 자릿수 4 (부호 포함) 또는 3
    # 관찰: med=0, 대부분 "000" 또는 "-000". 자릿수는 4로 두고 부호 필수는 아님.
    {"scale": 6, "digits": 4, "signed": True, "methods": [
        {"method": "norm_otsu",   "psm": 7},
        {"method": "rg_otsu",     "psm": 7},
        {"method": "norm_fixed",  "tv": 128, "psm": 7},
        {"method": "clahe_otsu",  "psm": 7},
    ]},
    # CH2_LOAD — signed (실제 -100~-250 범위 관찰), 자릿수 4
    {"scale": 6, "digits": 4, "signed": True, "methods": [
        {"method": "rg_otsu",     "psm": 7},
        {"method": "norm_otsu",   "psm": 7},
        {"method": "fixed",       "tv": 140, "psm": 7},
        {"method": "norm_fixed",  "tv": 128, "psm": 7},
        {"method": "clahe_otsu",  "psm": 7},
        {"method": "norm_otsu",   "psm": 8},
    ]},
    # CH2_TUNE — v1 기준 57%. 가장 공격적 캐스케이드.
    {"scale": 7, "digits": 3, "signed": False, "methods": [
        {"method": "rg_otsu",     "psm": 7},
        {"method": "clahe_otsu",  "psm": 7},
        {"method": "fixed",       "tv": 140, "psm": 7},
        {"method": "norm_fixed",  "tv": 128, "psm": 8, "morph_k": 3, "border": 25},
        {"method": "norm_fixed",  "tv": 128, "psm": 7, "morph_k": 3, "border": 25},
        {"method": "norm_otsu",   "psm": 8},
        {"method": "rg_fixed",    "tv": 120, "psm": 7},
        {"method": "rg_adaptive", "psm": 7},
    ]},
]

# 모드별 기록 레이블 + 챔버 서브폴더명
_MODE_CONFIG: dict[str, dict] = {
    "CH1":      {"labels": _ALL_LABELS, "active": ["CH1_FWD", "CH1_REF", "CH1_LOAD", "CH1_TUNE"], "folder": "CH1"},
    "CH2":      {"labels": _ALL_LABELS, "active": ["CH2_FWD", "CH2_REF", "CH2_LOAD", "CH2_TUNE"], "folder": "CH2"},
    "CLEANING": {"labels": _ALL_LABELS, "active": ["CH1_FWD", "CH1_REF", "CH1_LOAD", "CH1_TUNE"], "folder": "CLEANING"},
    "ALL":      {"labels": _ALL_LABELS, "active": _ALL_LABELS,                                    "folder": "ALL"},
}

CONFIG_FILE = "rf_config.json"


# ══════════════════════════════════════════════════════════
# ON/OFF 디스플레이 판별
# ══════════════════════════════════════════════════════════
def _is_display_on(crop: np.ndarray) -> bool:
    """
    R-G 차분 통계로 디스플레이 ON/OFF 판정.
    실측 기반 임계값 — ON/OFF는 값이 명확히 분리됨.
    """
    if crop is None or crop.size == 0:
        return False
    r = crop[:, :, 2].astype(np.float32)
    g = crop[:, :, 1].astype(np.float32)
    rg = np.clip(r - g, 0, 255)

    r_max  = float(r.max())
    rg_max = float(rg.max())
    rg_p95 = float(np.percentile(rg, 95))

    # 세 조건 중 2개 이상 만족 시 ON (안정성 ↑)
    votes = 0
    if r_max  >= ONOFF_R_MAX_MIN:  votes += 1
    if rg_max >= ONOFF_RG_MAX_MIN: votes += 1
    if rg_p95 >= ONOFF_RG_P95_MIN: votes += 1
    return votes >= 2


# ══════════════════════════════════════════════════════════
# OCR 전처리 방식
# ══════════════════════════════════════════════════════════
def _apply_method(crop: np.ndarray, scale: int, method: str, tv: int = 0) -> Optional[np.ndarray]:
    """전처리 방식 적용 → 이진화 이미지 반환"""
    try:
        if method == "norm_otsu":
            big = cv2.resize(crop[:, :, 2], None, fx=scale, fy=scale,
                             interpolation=cv2.INTER_LANCZOS4)
            vmin, vmax = float(big.min()), float(big.max())
            if vmax - vmin < 1:
                return None
            big = ((big.astype(float) - vmin) / (vmax - vmin) * 255).astype(np.uint8)
            _, th = cv2.threshold(big, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            return th

        elif method == "norm_fixed":
            big = cv2.resize(crop[:, :, 2], None, fx=scale, fy=scale,
                             interpolation=cv2.INTER_LANCZOS4)
            vmin, vmax = float(big.min()), float(big.max())
            if vmax - vmin < 1:
                return None
            big = ((big.astype(float) - vmin) / (vmax - vmin) * 255).astype(np.uint8)
            _, th = cv2.threshold(big, tv, 255, cv2.THRESH_BINARY)
            return th

        elif method == "rg_otsu":
            r = crop[:, :, 2].astype(float)
            g = crop[:, :, 1].astype(float)
            diff = np.clip(r - g, 0, 255).astype(np.uint8)
            big = cv2.resize(diff, None, fx=scale, fy=scale,
                             interpolation=cv2.INTER_LANCZOS4)
            vmin, vmax = float(big.min()), float(big.max())
            if vmax - vmin < 1:
                return None
            big = ((big.astype(float) - vmin) / (vmax - vmin) * 255).astype(np.uint8)
            _, th = cv2.threshold(big, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            return th

        elif method == "rg_fixed":
            r = crop[:, :, 2].astype(float)
            g = crop[:, :, 1].astype(float)
            diff = np.clip(r - g, 0, 255).astype(np.uint8)
            big = cv2.resize(diff, None, fx=scale, fy=scale,
                             interpolation=cv2.INTER_LANCZOS4)
            vmin, vmax = float(big.min()), float(big.max())
            if vmax - vmin < 1:
                return None
            big = ((big.astype(float) - vmin) / (vmax - vmin) * 255).astype(np.uint8)
            _, th = cv2.threshold(big, tv, 255, cv2.THRESH_BINARY)
            return th

        elif method == "fixed":
            big = cv2.resize(crop[:, :, 2], None, fx=scale, fy=scale,
                             interpolation=cv2.INTER_LANCZOS4)
            _, th = cv2.threshold(big, tv, 255, cv2.THRESH_BINARY)
            return th

        # ─── 신규 전처리 ─────────────────────────────────
        elif method == "clahe_otsu":
            # CLAHE: 국소 대비 증폭 → 흐릿한 digit 강조에 유리
            chan = crop[:, :, 2]
            clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(4, 4))
            enh = clahe.apply(chan)
            big = cv2.resize(enh, None, fx=scale, fy=scale,
                             interpolation=cv2.INTER_LANCZOS4)
            _, th = cv2.threshold(big, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            return th

        elif method == "rg_adaptive":
            r = crop[:, :, 2].astype(float)
            g = crop[:, :, 1].astype(float)
            diff = np.clip(r - g, 0, 255).astype(np.uint8)
            big = cv2.resize(diff, None, fx=scale, fy=scale,
                             interpolation=cv2.INTER_LANCZOS4)
            # 적응형 임계 — 국소 배경차이에 강건
            bs = max(21, (min(big.shape) // 8) | 1)  # 홀수 윈도우
            th = cv2.adaptiveThreshold(big, 255,
                                       cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                       cv2.THRESH_BINARY, bs, -2)
            return th

    except Exception as e:
        logger.warning("[OCR] _apply_method 오류 (%s): %s", method, e)
    return None


def _post_process(th: np.ndarray, morph_k: int = 2) -> np.ndarray:
    """이진화 후 공통 후처리: 반전체크 + CLOSE + medianBlur"""
    if th.sum() / (255 * th.size) > 0.5:
        th = cv2.bitwise_not(th)
    k = np.ones((morph_k, morph_k), np.uint8)
    th = cv2.morphologyEx(th, cv2.MORPH_CLOSE, k)
    th = cv2.medianBlur(th, 3)
    return th


def _run_tesseract(th: np.ndarray, psm: int, border: int = 15) -> str:
    """Tesseract 실행 → 숫자+마이너스 문자열 반환"""
    bordered = cv2.copyMakeBorder(th, border, border, border, border,
                                  cv2.BORDER_CONSTANT, value=0)
    cfg = f'--psm {psm} --oem 3 -c tessedit_char_whitelist=0123456789-'
    raw = pytesseract.image_to_string(bordered, config=cfg).strip()
    return ''.join(c for c in raw if c.isdigit() or c == '-')


# ══════════════════════════════════════════════════════════
# 디짓 개별 분할 & 인식 (wide-gap 디스플레이 대응)
# ══════════════════════════════════════════════════════════
def _split_digit_columns(th: np.ndarray,
                         min_w_ratio: float = 0.02,
                         min_h_ratio: float = 0.30) -> list:
    """
    이진화 이미지에서 digit 후보 연결요소들을 x 순서로 반환.
    각 튜플은 (x, y, w, h, kind) — kind는 "digit" 또는 "dash".
    """
    if th.dtype != np.uint8:
        th = th.astype(np.uint8)
    num, labels, stats, _ = cv2.connectedComponentsWithStats(th, connectivity=8)
    H, W = th.shape
    min_w = max(3, int(W * min_w_ratio))
    min_h = max(8, int(H * min_h_ratio))
    boxes = []
    for i in range(1, num):
        x, y, w, h, area = stats[i]
        if w < min_w or h < min_h:
            continue
        if w > W * 0.9 or h > H * 0.98:  # 테두리 같은 과대 요소 제외
            continue
        aspect = h / max(w, 1)
        if aspect < 0.5:
            # 가로선(마이너스) 후보
            boxes.append((x, y, w, h, "dash"))
            continue
        boxes.append((x, y, w, h, "digit"))
    boxes.sort(key=lambda b: b[0])
    return boxes


def _ocr_per_digit(th: np.ndarray,
                   expected_digits: int,
                   signed: bool) -> Optional[str]:
    """
    이진화 이미지에서 digit 영역을 각각 분리해 개별 Tesseract로 인식.
    wide-gap 디스플레이 (ex. CH1_FWD '0  12')에서 greatly 강함.
    """
    boxes = _split_digit_columns(th)
    if not boxes:
        return None

    # 마이너스 선 검출 (좌측에 있는 가로선 요소)
    dash_boxes = [b for b in boxes if b[4] == "dash"]
    digit_boxes = [b for b in boxes if b[4] == "digit"]
    if not digit_boxes:
        return None

    # 마이너스 기호: 가장 왼쪽 digit 보다 앞쪽에 있는 dash
    first_digit_x = digit_boxes[0][0]
    has_minus = any(b[0] < first_digit_x for b in dash_boxes) if signed else False

    # 기대 자리수(부호 제외) 산출
    expect_n = expected_digits - (1 if signed else 0)
    if not (expect_n - 1 <= len(digit_boxes) <= expect_n + 1):
        return None

    pad = 3
    result_chars = []
    for (x, y, w, h, _kind) in digit_boxes:
        x0 = max(0, x - pad)
        y0 = max(0, y - pad)
        x1 = min(th.shape[1], x + w + pad)
        y1 = min(th.shape[0], y + h + pad)
        sub = th[y0:y1, x0:x1]
        # 개별 digit Tesseract (psm 10 = single char)
        try:
            bordered = cv2.copyMakeBorder(sub, 20, 20, 20, 20,
                                          cv2.BORDER_CONSTANT, value=0)
            cfg = '--psm 10 --oem 3 -c tessedit_char_whitelist=0123456789'
            raw = pytesseract.image_to_string(bordered, config=cfg).strip()
            raw = ''.join(c for c in raw if c.isdigit())
            if len(raw) == 1:
                result_chars.append(raw)
            else:
                return None
        except Exception:
            return None

    s = ''.join(result_chars)
    # 자리수 보정 — 하나 부족하면 leading zero 패딩
    if len(s) == expect_n - 1:
        s = "0" + s
    elif len(s) != expect_n:
        return None

    if has_minus:
        s = "-" + s
    return s


def _normalize_result(s: Optional[str],
                      expected_digits: int,
                      signed: bool) -> Optional[str]:
    """
    OCR 원문을 기대 자릿수에 맞춰 보정.
    - signed=False: expected_digits 자리 숫자만 허용. 1자리 부족 시 leading-zero 패딩.
    - signed=True:  부호 포함 expected_digits. 부호 없을 수도 있음(실측: '-000' / '000' 혼재).
                    부호 있으면 부호+(expected_digits-1)자리, 없으면 expected_digits 또는 expected_digits-1자리 허용.
    """
    if not s:
        return None

    # 정리 — 공백/유효치 않은 문자 제거
    s = ''.join(c for c in s if c.isdigit() or c == '-')
    if not s:
        return None

    # 부호는 맨 앞에만 허용
    if s.count('-') > 1:
        return None
    if '-' in s and not s.startswith('-'):
        return None

    digits_only = s.lstrip('-')
    if not digits_only.isdigit():
        return None

    if not signed:
        # 부호 없어야 함
        if s.startswith('-'):
            return None
        if len(digits_only) == expected_digits:
            return digits_only
        if len(digits_only) == expected_digits - 1:
            return "0" + digits_only
        return None

    # signed case
    sign = '-' if s.startswith('-') else ''
    # 기대 자릿수(부호 포함)
    target_digits = expected_digits - 1  # 부호 자리 제외한 숫자 자릿수

    if len(digits_only) == target_digits:
        return sign + digits_only
    if len(digits_only) == target_digits - 1:
        return sign + "0" + digits_only
    if len(digits_only) == target_digits + 1 and sign == '':
        # 부호 없는 expected_digits 자리 (예: expected=4, '0000')
        return digits_only
    return None


def _ocr_cascade(crop: np.ndarray, params: dict) -> Optional[str]:
    """
    ROI별 캐스케이드 OCR.
    단계:
      1) 전체 이미지 Tesseract 시도 (psm 7/8)
      2) 실패 시 per-digit 분할 인식 시도
    """
    if not _TESSERACT_OK or crop is None or crop.size == 0:
        return None

    scale    = params["scale"]
    digits   = params["digits"]
    signed   = bool(params.get("signed", False))

    for m in params.get("methods", []):
        try:
            method  = m["method"]
            psm     = m["psm"]
            morph_k = m.get("morph_k", 2)
            border  = m.get("border", 15)

            th = _apply_method(crop, scale, method, m.get("tv", 0))
            if th is None:
                continue

            th = _post_process(th, morph_k)

            # 1차: 전체 Tesseract
            raw = _run_tesseract(th, psm, border)
            result = _normalize_result(raw, digits, signed)
            if result:
                return result

            # 2차: per-digit fallback
            result = _ocr_per_digit(th, digits, signed)
            if result:
                return result

        except Exception as e:
            logger.warning("[OCR] cascade 오류 (%s): %s", m.get("method", "?"), e)

    return None


# ══════════════════════════════════════════════════════════
# 시계열 정합성 검증
# ══════════════════════════════════════════════════════════
def _temporal_is_outlier(cur: float, history: deque) -> bool:
    """
    최근 N개 히스토리 대비 cur 가 이상치인지.
    MAD(절대편차 중앙값) 기반 robust test.
    """
    if len(history) < TEMPORAL_MIN_SAMPLES:
        return False
    arr = np.array(list(history), dtype=float)
    med = np.median(arr)
    mad = np.median(np.abs(arr - med)) + 1e-6
    # 중앙값으로부터 TEMPORAL_MAD_THRESH * MAD 이상 벗어나면 outlier
    return abs(cur - med) > TEMPORAL_MAD_THRESH * mad * 3.5  # 3.5 ≈ 정규분포 sigma 환산


# ══════════════════════════════════════════════════════════
# 경로 헬퍼
# ══════════════════════════════════════════════════════════
def _resolve_root() -> Path:
    """NAS 접근 가능하면 NAS, 아니면 로컬 폴백 반환."""
    try:
        NAS_LOG_ROOT.mkdir(parents=True, exist_ok=True)
        return NAS_LOG_ROOT
    except Exception as e:
        logger.warning("[CameraRecorder] NAS 접근 실패 → 로컬 저장: %s", e)
        LOCAL_FALLBACK.mkdir(parents=True, exist_ok=True)
        return LOCAL_FALLBACK


# ══════════════════════════════════════════════════════════
# CameraRecorder 클래스
# ══════════════════════════════════════════════════════════
class CameraRecorder:
    """
    백그라운드 스레드로 카메라를 캡처하고 OCR 결과를 CSV + 원본 이미지로 저장.

    Public API (v1과 완전 호환)
    ---------------------------
    CameraRecorder(camera_index=1, interval=1.0, config_file=CONFIG_FILE)
    .start(mode="ALL" | "CH1" | "CH2" | "CLEANING")
    .stop()
    .is_running → bool
    """

    def __init__(
        self,
        camera_index: int = 1,
        interval: float = 1.0,
        config_file: str | Path = CONFIG_FILE,
    ) -> None:
        self._cam_idx     = camera_index
        self._interval    = max(0.2, float(interval))
        self._config_file = Path(config_file)

        self._rois:   list = list(_DEFAULT_ROIS)
        self._params: list = list(_DEFAULT_PARAMS)

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        self._mode          = "ALL"
        self._active_labels = list(_ALL_LABELS)
        self._check_labels  = list(_ALL_LABELS)
        self._mode_folder   = "ALL"

        self._load_config()

    # ── 설정 로드 ──────────────────────────────────────────
    def _load_config(self) -> None:
        if not self._config_file.exists():
            logger.debug("[CameraRecorder] config 없음, 기본 좌표 사용")
            return
        try:
            with open(self._config_file) as f:
                cfg = json.load(f)
            self._rois = cfg.get("rois", _DEFAULT_ROIS)
            # params는 dict 구조가 변경되었으므로 signed 키 보충
            user_params = cfg.get("params")
            if user_params and isinstance(user_params, list) and len(user_params) == len(_DEFAULT_PARAMS):
                merged = []
                for i, p in enumerate(user_params):
                    d = dict(_DEFAULT_PARAMS[i])
                    d.update(p)
                    merged.append(d)
                self._params = merged
            self._cam_idx = int(cfg.get("camera_index", self._cam_idx))
            logger.info("[CameraRecorder] config 로드: %s", self._config_file)
        except Exception as e:
            logger.warning("[CameraRecorder] config 로드 실패: %s", e)

    # ── Public API ─────────────────────────────────────────
    def start(self, mode: str = "ALL") -> None:
        """
        백그라운드 녹화 시작. 즉시 반환(논블로킹).
        """
        with self._lock:
            if self._thread and self._thread.is_alive():
                logger.debug("[CameraRecorder] 이전 스레드 정리")
                self._stop_event.set()
                self._thread.join(timeout=3.0)

            self._mode = mode.upper()
            mc = _MODE_CONFIG.get(self._mode, _MODE_CONFIG["ALL"])
            self._active_labels  = mc["labels"]   # CSV 컬럼 (항상 10개)
            self._check_labels   = mc["active"]   # 에러 판단 대상
            self._mode_folder    = mc["folder"]
            self._stop_event.clear()

            self._thread = threading.Thread(
                target=self._record_loop,
                daemon=True,
                name=f"CameraRecorder-{self._mode}",
            )
            self._thread.start()
            logger.info("[CameraRecorder] 시작 mode=%s", self._mode)

    def stop(self) -> None:
        """녹화 정지 요청. 즉시 반환(논블로킹)."""
        self._stop_event.set()
        logger.info("[CameraRecorder] 정지 요청")

    @property
    def is_running(self) -> bool:
        """현재 녹화 중이면 True."""
        return bool(
            self._thread and self._thread.is_alive()
            and not self._stop_event.is_set()
        )

    # ── 내부: 녹화 루프 ────────────────────────────────────
    def _record_loop(self) -> None:
        """백그라운드 스레드 본체. 예외가 나도 메인에 전파하지 않는다."""

        # ── 1) 경로 결정 ──────────────────────────────────
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        root      = _resolve_root()
        mode_dir  = root / self._mode_folder
        raw_dir   = mode_dir / "raw" / ts
        csv_path  = mode_dir / f"{self._mode_folder}_{ts}.csv"

        try:
            mode_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error("[CameraRecorder] 폴더 생성 실패: %s", e)
            return

        # ── 2) 카메라 오픈 ────────────────────────────────
        cap = cv2.VideoCapture(self._cam_idx)
        if not cap.isOpened():
            logger.error("[CameraRecorder] 카메라 열기 실패 (index=%d)", self._cam_idx)
            return

        cap.set(cv2.CAP_PROP_FRAME_WIDTH,  1920)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 1080)
        actual_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        actual_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        logger.info("[CameraRecorder] 카메라 해상도: %dx%d", actual_w, actual_h)

        fieldnames = ["timestamp"] + self._active_labels
        err_count  = 0
        img_count  = 0
        saved_count = 0

        # ── 누적 카운터 / 히스토리 ────────────────────────
        success_count = {lbl: 0 for lbl in self._active_labels}
        fail_count    = {lbl: 0 for lbl in self._active_labels}
        off_count     = {lbl: 0 for lbl in self._active_labels}
        outlier_count = {lbl: 0 for lbl in self._active_labels}
        prev_values: dict = {lbl: None for lbl in self._active_labels}
        history: dict = {
            lbl: deque(maxlen=TEMPORAL_HISTORY_N) for lbl in self._active_labels
        }

        logger.info("[CameraRecorder] CSV  → %s", csv_path)
        logger.info("[CameraRecorder] 이미지 → %s (조건부 저장)", raw_dir)

        try:
            with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                f.flush()

                while not self._stop_event.is_set():
                    t0 = time.time()

                    # ── 프레임 캡처 ──────────────────────
                    ret, frame = cap.read()
                    if not ret:
                        err_count += 1
                        if err_count > 10:
                            logger.error("[CameraRecorder] 카메라 읽기 반복 실패")
                            break
                        time.sleep(0.3)
                        continue
                    err_count = 0
                    img_count += 1

                    now_dt  = datetime.now()
                    now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
                    now_hms = now_dt.strftime("%H%M%S")

                    # ── 회전 보정 ────────────────────────
                    frame = cv2.rotate(frame, cv2.ROTATE_90_CLOCKWISE)

                    # ── OCR (ON/OFF 선체크 + 캐스케이드) ──
                    row: dict = {"timestamp": now_str}
                    ocr_failed   = False
                    spike_detect = False

                    for label, roi, p in zip(self._active_labels, self._rois, self._params):
                        y1, y2, x1, x2 = roi
                        fh, fw = frame.shape[:2]
                        pad = 5
                        y1p, y2p = max(0, y1 - pad), min(fh, y2 + pad)
                        x1p, x2p = max(0, x1 - pad), min(fw, x2 + pad)
                        crop = frame[y1p:y2p, x1p:x2p]

                        # ① ON/OFF 판정 — OFF면 OCR 건너뛰고 None 기록
                        if not _is_display_on(crop):
                            row[label] = None
                            off_count[label] += 1
                            # 활성 채널이 OFF면 failed 로 간주하지 않음 (램프 전/후 정상)
                            continue

                        # ② 캐스케이드 OCR
                        result = _ocr_cascade(crop, p)

                        # ③ 시계열 검증 — 히스토리 기반 outlier 식별
                        if result is not None:
                            try:
                                cur_val = float(result)
                                if _temporal_is_outlier(cur_val, history[label]):
                                    outlier_count[label] += 1
                                    # 기록은 유지하되, diag 이미지 저장 트리거
                                    if label in self._check_labels:
                                        spike_detect = True
                                history[label].append(cur_val)
                            except (ValueError, TypeError):
                                pass

                        row[label] = result

                        # 에러/급변 판단은 해당 공정 관련 레이블만
                        if label in self._check_labels:
                            if result is None:
                                ocr_failed = True
                                fail_count[label] += 1
                            else:
                                success_count[label] += 1
                                try:
                                    cur_val  = float(result)
                                    prev_val = prev_values.get(label)
                                    if prev_val is not None and prev_val != 0.0:
                                        change_pct = abs(cur_val - prev_val) / abs(prev_val) * 100.0
                                        if change_pct >= SPIKE_THRESHOLD_PCT:
                                            spike_detect = True
                                            logger.info(
                                                "[CameraRecorder] 급변 감지 %s: %.0f → %.0f (%.1f%%)",
                                                label, prev_val, cur_val, change_pct,
                                            )
                                    prev_values[label] = cur_val
                                except (ValueError, TypeError):
                                    pass
                        else:
                            # 비활성 채널도 success/fail은 집계 (ON 상태였을 때)
                            if result is None:
                                fail_count[label] += 1
                            else:
                                success_count[label] += 1

                    # ── CSV 기록 ─────────────────────────
                    writer.writerow(row)
                    f.flush()

                    # ── 조건부 이미지 저장 (진단 이미지) ──
                    if ocr_failed or spike_detect:
                        saved_count += 1
                        reason = []
                        if ocr_failed:   reason.append("ocr_fail")
                        if spike_detect: reason.append("spike")
                        reason_str = "_".join(reason)

                        try:
                            diag = frame.copy()
                            for lbl, roi_d, pd in zip(_ALL_LABELS, self._rois, self._params):
                                y1d, y2d, x1d, x2d = roi_d
                                color = (0, 255, 80) if lbl in self._check_labels else (120, 120, 120)
                                cv2.rectangle(diag, (x1d, y1d), (x2d, y2d), color, 2)
                                val = row.get(lbl, "")
                                cv2.putText(diag, f"{lbl.split('_',1)[-1]}:{val or '?'}",
                                            (x1d, max(y1d - 4, 12)),
                                            cv2.FONT_HERSHEY_SIMPLEX, 0.38, color, 1)
                            img_name = raw_dir / f"{now_hms}_{img_count:04d}_{reason_str}.jpg"
                            cv2.imwrite(str(img_name), diag)
                        except Exception as e:
                            logger.warning("[CameraRecorder] 이미지 저장 실패: %s", e)

                    # ── 인터벌 대기 (stop_event 감지 포함) ──
                    elapsed  = time.time() - t0
                    deadline = time.time() + max(0.0, self._interval - elapsed)
                    while time.time() < deadline:
                        if self._stop_event.is_set():
                            break
                        time.sleep(0.05)

        except Exception as e:
            logger.error("[CameraRecorder] 루프 오류: %s", e)
        finally:
            cap.release()
            # ROI별 누적 성공률 요약
            try:
                summary_lines = []
                for lbl in self._active_labels:
                    succ = success_count[lbl]
                    fail = fail_count[lbl]
                    off  = off_count[lbl]
                    out  = outlier_count[lbl]
                    total = succ + fail + off
                    if total == 0:
                        continue
                    on_total = succ + fail
                    rate = (succ / on_total * 100.0) if on_total > 0 else 0.0
                    marker = "*" if lbl in self._check_labels else " "
                    summary_lines.append(
                        f"  {marker} {lbl:10s}  ON={on_total:4d} 성공={succ:4d} "
                        f"실패={fail:4d}  OFF={off:4d}  의심={out:3d}  인식율={rate:5.1f}%"
                    )
                logger.info("[CameraRecorder] ROI별 인식 요약:\n" + "\n".join(summary_lines))
            except Exception:
                pass
            logger.info(
                "[CameraRecorder] 완료 — 촬영 %d장, 저장 %d장 | CSV: %s",
                img_count, saved_count, csv_path,
            )

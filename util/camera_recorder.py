# util/camera_recorder.py
# -*- coding: utf-8 -*-
"""
CameraRecorder
==============
공정 중 RF 매칭 컨트롤 패널 디스플레이를 카메라로 읽어 CSV + 이미지로 저장.

저장 구조
---------
//VanaM_NAS/VanaM_Sputter/Sputter/Logs/CH1&2/Camera_Logs/
├── CH1/
│   ├── raw/
│   │   └── 20260326_143022/        ← 공정 시작 시각 폴더
│   │       ├── 143022_0001.jpg
│   │       ├── 143023_0002.jpg
│   │       └── ...
│   └── CH1_20260326_143022.csv     ← 공정 1회분 CSV
├── CH2/
│   ├── raw/
│   └── CH2_20260326_152010.csv
└── CLEANING/
    ├── raw/
    └── CLEANING_20260326_170033.csv

설계 원칙
---------
- threading.Thread(daemon=True) → asyncio / Qt 루프에 영향 없음
- start() / stop() 논블로킹, 즉시 반환
- OCR 실패 / 카메라 오류 → 내부에서 처리, 메인 공정에 예외 전파 없음
- NAS 접근 불가 시 로컬 경로(rf_logs/)로 자동 폴백
- ROI별 캐스케이드 OCR: 1순위 실패 시 2순위, 3순위 자동 시도
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
            _base / "Tesseract-OCR" / "tesseract.exe",          # 빌드 포함
            Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe"),   # 시스템 설치
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
# 기본 ROI (카메라 위치가 바뀌면 rf_config.json 으로 재설정)
# ──────────────────────────────────────────────────────────
_DEFAULT_ROIS = [
    [571, 610,  191, 291],   # CH1_FWD
    [586, 628,  405, 496],   # CH1_REF
    [604, 642,  630, 715],   # CH1_LOAD
    [611, 650,  794, 886],   # CH1_TUNE
    [997,1042,  277, 413],   # RF3_LOAD
    [994,1038,  465, 605],   # RF3_TUNE   ← 마이너스 부호 포함 (x1=465)
    [1372,1417, 203, 295],   # CH2_FWD
    [1348,1394, 384, 499],   # CH2_REF
    [1332,1375, 598, 714],   # CH2_LOAD
    [1312,1350, 775, 878],   # CH2_TUNE
]

# ──────────────────────────────────────────────────────────
# ROI별 캐스케이드 OCR 설정
#
# method 종류:
#   "norm_otsu"  = R채널 → 확대 → 정규화(0~255) → OTSU 자동 이진화
#   "norm_fixed" = R채널 → 확대 → 정규화(0~255) → 고정 tv 이진화
#   "rg_otsu"    = (R-G)차분 → 확대 → 정규화 → OTSU
#   "rg_fixed"   = (R-G)차분 → 확대 → 정규화 → 고정 tv
#   "fixed"      = R채널 → 확대 → 고정 tv 이진화 (정규화 없음)
#
# 추가 옵션:
#   morph_k: CLOSE 커널 크기 (기본 2)
#   border:  Tesseract 입력 패딩 (기본 15)
# ──────────────────────────────────────────────────────────
_DEFAULT_PARAMS = [
    # CH1_FWD — 92%
    {"scale": 6, "digits": 3, "methods": [
        {"method": "norm_otsu",  "psm": 7},
        {"method": "fixed",      "tv": 110, "psm": 7},
        {"method": "norm_fixed", "tv": 128, "psm": 7},
        {"method": "norm_otsu",  "psm": 8},
    ]},
    # CH1_REF — 98%
    {"scale": 6, "digits": 3, "methods": [
        {"method": "norm_fixed", "tv": 128, "psm": 7},
        {"method": "fixed",      "tv": 110, "psm": 7},
        {"method": "norm_otsu",  "psm": 7},
    ]},
    # CH1_LOAD — 100%
    {"scale": 6, "digits": 3, "methods": [
        {"method": "fixed",      "tv": 140, "psm": 7},
        {"method": "norm_otsu",  "psm": 7},
    ]},
    # CH1_TUNE — 100%
    {"scale": 6, "digits": 3, "methods": [
        {"method": "fixed",      "tv": 120, "psm": 7},
        {"method": "fixed",      "tv": 100, "psm": 8},
        {"method": "norm_otsu",  "psm": 8},
        {"method": "norm_fixed", "tv": 128, "psm": 8},
    ]},
    # RF3_LOAD — 95%
    {"scale": 6, "digits": 4, "methods": [
        {"method": "fixed",      "tv": 150, "psm": 7},
        {"method": "norm_otsu",  "psm": 7},
        {"method": "fixed",      "tv": 150, "psm": 8},
    ]},
    # RF3_TUNE — 90%
    {"scale": 6, "digits": 4, "methods": [
        {"method": "norm_fixed", "tv": 170, "psm": 8},
        {"method": "fixed",      "tv": 160, "psm": 8},
        {"method": "norm_fixed", "tv": 180, "psm": 8},
        {"method": "norm_fixed", "tv": 160, "psm": 8},
        {"method": "norm_otsu",  "psm": 8},
    ]},
    # CH2_FWD — 100%
    {"scale": 6, "digits": 3, "methods": [
        {"method": "norm_otsu",  "psm": 7},
        {"method": "fixed",      "tv": 110, "psm": 7},
        {"method": "norm_fixed", "tv": 128, "psm": 7},
        {"method": "norm_otsu",  "psm": 8},
    ]},
    # CH2_REF — 100%
    {"scale": 6, "digits": 4, "methods": [
        {"method": "norm_otsu",  "psm": 7},
        {"method": "norm_fixed", "tv": 128, "psm": 7},
    ]},
    # CH2_LOAD — 100%
    {"scale": 6, "digits": 4, "methods": [
        {"method": "norm_otsu",  "psm": 7},
        {"method": "fixed",      "tv": 140, "psm": 7},
        {"method": "norm_otsu",  "psm": 8},
    ]},
    # CH2_TUNE — 57%
    {"scale": 7, "digits": 3, "methods": [
        {"method": "rg_otsu",    "psm": 7},
        {"method": "fixed",      "tv": 140, "psm": 7},
        {"method": "norm_fixed", "tv": 128, "psm": 8, "morph_k": 3, "border": 25},
        {"method": "norm_fixed", "tv": 128, "psm": 7, "morph_k": 3, "border": 25},
        {"method": "norm_otsu",  "psm": 8},
        {"method": "rg_fixed",   "tv": 120, "psm": 7},
    ]},
]

_ALL_LABELS = [
    "CH1_FWD", "CH1_REF", "CH1_LOAD", "CH1_TUNE",
    "RF3_LOAD", "RF3_TUNE",
    "CH2_FWD", "CH2_REF", "CH2_LOAD", "CH2_TUNE",
]

# 모드별 기록 레이블 + 챔버 서브폴더명
_MODE_CONFIG: dict[str, dict] = {
    "CH1":      {"labels": _ALL_LABELS, "active": ["CH1_FWD", "CH1_REF", "CH1_LOAD", "CH1_TUNE"], "folder": "CH1"},
    "CH2":      {"labels": _ALL_LABELS, "active": ["CH2_FWD", "CH2_REF", "CH2_LOAD", "CH2_TUNE"], "folder": "CH2"},
    "CLEANING": {"labels": _ALL_LABELS, "active": ["CH1_FWD", "CH1_REF", "CH1_LOAD", "CH1_TUNE"], "folder": "CLEANING"},
    "ALL":      {"labels": _ALL_LABELS, "active": _ALL_LABELS,                                    "folder": "ALL"},
}

CONFIG_FILE = "rf_config.json"

# 급변 감지 임계값 (이전 값 대비 이 % 이상 변하면 이미지 저장)
SPIKE_THRESHOLD_PCT: float = 20.0


# ──────────────────────────────────────────────────────────
# OCR 전처리 방식
# ──────────────────────────────────────────────────────────
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


def _ocr_cascade(crop: np.ndarray, params: dict) -> Optional[str]:
    """ROI별 캐스케이드 OCR — 1순위 실패 시 2순위, 3순위 자동 시도."""
    if not _TESSERACT_OK or crop is None or crop.size == 0:
        return None

    scale  = params["scale"]
    digits = params["digits"]

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
            result = _run_tesseract(th, psm, border)

            if result and len(result) == digits:
                return result

        except Exception as e:
            logger.warning("[OCR] cascade 오류 (%s): %s", m.get("method", "?"), e)

    return None


# ──────────────────────────────────────────────────────────
# 경로 헬퍼
# ──────────────────────────────────────────────────────────
def _resolve_root() -> Path:
    """NAS 접근 가능하면 NAS, 아니면 로컬 폴백 반환."""
    try:
        NAS_LOG_ROOT.mkdir(parents=True, exist_ok=True)
        return NAS_LOG_ROOT
    except Exception as e:
        logger.warning("[CameraRecorder] NAS 접근 실패 → 로컬 저장: %s", e)
        LOCAL_FALLBACK.mkdir(parents=True, exist_ok=True)
        return LOCAL_FALLBACK


# ──────────────────────────────────────────────────────────
# CameraRecorder 클래스
# ──────────────────────────────────────────────────────────
class CameraRecorder:
    """
    백그라운드 스레드로 카메라를 캡처하고 OCR 결과를 CSV + 원본 이미지로 저장.

    Parameters
    ----------
    camera_index : int
        OpenCV 카메라 인덱스 (기본 0)
    interval : float
        캡처 간격(초). 기본 1.0
    config_file : str | Path
        캘리브레이션 JSON 경로
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
            self._rois    = cfg.get("rois",   _DEFAULT_ROIS)
            self._params  = cfg.get("params", _DEFAULT_PARAMS)
            self._cam_idx = int(cfg.get("camera_index", self._cam_idx))
            logger.info("[CameraRecorder] config 로드: %s", self._config_file)
        except Exception as e:
            logger.warning("[CameraRecorder] config 로드 실패: %s", e)

    # ── Public API ─────────────────────────────────────────
    def start(self, mode: str = "ALL") -> None:
        """
        백그라운드 녹화 시작. 즉시 반환(논블로킹).

        Parameters
        ----------
        mode : "CH1" | "CH2" | "CLEANING" | "ALL"
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

        # ── 이전 값 캐시 (급변 감지용) ────────────────────
        prev_values: dict[str, float | None] = {lbl: None for lbl in self._active_labels}

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

                    # ── OCR (캐스케이드) ──────────────────
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

                        result = _ocr_cascade(crop, p)
                        row[label] = result

                        # 에러/급변 판단은 해당 공정 관련 레이블만
                        if label not in self._check_labels:
                            continue

                        if result is None:
                            ocr_failed = True
                        else:
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
            logger.info(
                "[CameraRecorder] 완료 — 촬영 %d장, 저장 %d장 | CSV: %s",
                img_count, saved_count, csv_path,
            )
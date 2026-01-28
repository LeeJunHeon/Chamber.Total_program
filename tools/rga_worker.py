# tools/rga_worker.py
# -*- coding: utf-8 -*-
"""
RGA Worker (standalone exe)
- 목적: RGA 측정(srsinst) + CSV append + JSON(1줄) stdout 응답
- 메인 프로그램이 subprocess로 실행해서 stdout JSON을 읽는다.
- 메인→워커 파라미터: --ch, --timeout 만 사용
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import traceback
from typing import Tuple
from pathlib import Path
from datetime import datetime

import numpy as np


# === Worker 내장 기본값(배포/독립 실행 안정성을 위해 config import 안 함) ===
DEFAULTS = {
    1: {
        "ip": "192.168.1.20",  # ✅ CH1 RGA IP로 변경
        "user": "admin",
        "password": "admin",
        "csv": r"\\VanaM_NAS\VanaM_Sputter\RGA\Ch.1\RGA_spectrums.csv",  # ✅ 실제 경로로 변경
    },
    2: {
        "ip": "192.168.1.21",  # ✅ CH2 RGA IP로 변경
        "user": "admin",
        "password": "admin",
        "csv": r"\\VanaM_NAS\VanaM_Sputter\RGA\Ch.2\RGA_spectrums.csv",  # ✅ 실제 경로로 변경
    },
}

DEFAULT_SUFFIX = "e-12"
DEFAULT_TIMEOUT = 30.0


# ---- srsinst import (너의 device/rga.py 방식을 그대로 따라감) ----
try:
    from srsinst.rga import RGA100
except ModuleNotFoundError as e:
    if e.name == "matplotlib":
        err = (
            "RGA 드라이버 import 실패: matplotlib 누락\n"
            "해결: pip install matplotlib\n"
            "PyInstaller 빌드 시 --collect-all matplotlib 권장"
        )
        print(json.dumps({"ok": False, "stage": "import", "error": err}, ensure_ascii=False), flush=True)
        sys.exit(10)
    err = f"RGA 드라이버 import 실패: 누락 모듈={e.name} (pip install srsinst 권장)"
    print(json.dumps({"ok": False, "stage": "import", "error": err}, ensure_ascii=False), flush=True)
    sys.exit(11)
except Exception as e:
    err = f"RGA 드라이버 import 실패: {type(e).__name__}: {e}"
    print(json.dumps({"ok": False, "stage": "import", "error": err}, ensure_ascii=False), flush=True)
    sys.exit(12)


def now_str() -> str:
    # 너의 device/rga.py와 동일 포맷
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_csv_header(path: Path, n: int, encoding: str = "utf-8-sig") -> None:
    """
    너가 올린 RGA_spectrums.csv처럼
    Time,Mass 1,Mass 2, ... 형식의 헤더가 없으면 1회 생성.
    (이미 헤더가 있으면 건드리지 않음)
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    if not path.exists() or path.stat().st_size == 0:
        with open(path, "w", newline="", encoding=encoding) as f:
            w = csv.writer(f)
            header = ["Time"] + [f"Mass {i}" for i in range(1, n + 1)]
            w.writerow(header)


def append_row(path: Path, ts: str, hist_raw: np.ndarray, suffix: str = "e-12", encoding: str = "utf-8-sig") -> None:
    """
    ✅ 현재 너의 device/rga.py와 동일한 저장 규칙(구조 유지):
      row = [timestamp] + [f"{v}{suffix}" ...]
    """
    with open(path, "a", newline="", encoding=encoding) as f:
        w = csv.writer(f)
        row = [ts] + [f"{v}{suffix}" for v in hist_raw.tolist()]
        w.writerow(row)


def rga_measure_once(ip: str, user: str, password: str) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    device/rga.py의 _blocking_histogram_once() 그대로(비동기 제거 버전)
    """
    rga = RGA100("tcpip", ip, user, password)
    try:
        rga.filament.turn_on()
        try:
            histogram = np.asarray(rga.scan.get_histogram_scan(), dtype=float)
        finally:
            try:
                rga.filament.turn_off()
            except Exception:
                pass

        pressures = np.asarray(
            rga.scan.get_partial_pressure_corrected_spectrum(histogram),
            dtype=float
        )
        mass_axis = np.asarray(
            rga.scan.get_mass_axis(for_analog_scan=False),
            dtype=float
        )
        return mass_axis, histogram, pressures
    finally:
        try:
            rga.disconnect()
        except Exception:
            pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ch", type=int, required=True, choices=[1, 2])
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    args = ap.parse_args()

    defaults = DEFAULTS.get(args.ch)
    if not defaults:
        print(json.dumps({"ok": False, "stage": "args", "error": f"invalid ch={args.ch}"}, ensure_ascii=False), flush=True)
        return 2

    # ✅ argparse에 ip/user/password/csv를 안 둘 거면, args.*에 넣지 말고 로컬 변수로 꺼내서 쓰자
    ip = defaults["ip"]
    user = defaults["user"]
    password = defaults["password"]
    csv_path = Path(defaults["csv"])

    suffix = DEFAULT_SUFFIX

    t0 = time.time()
    try:
        # --- timeout은 “간단한 소프트 타임아웃”으로 처리(네이티브 블럭 시 kill은 메인에서) ---
        mass_axis, hist_raw, pressures = rga_measure_once(ip, user, password)

        ts = now_str()

        # 헤더 보장(이미 있으면 변화 없음)
        ensure_csv_header(csv_path, n=len(hist_raw))

        # CSV append (구조 유지)
        append_row(csv_path, ts, hist_raw, suffix=suffix)

        dt_ms = int((time.time() - t0) * 1000)

        # ✅ 메인 프로그램이 즉시 그래프 그릴 수 있도록 JSON으로 결과 전달
        payload = {
            "ok": True,
            "ch": args.ch,
            "ip": ip,
            "timestamp": ts,
            "duration_ms": dt_ms,
            "csv_path": str(csv_path),
            "mass_axis": mass_axis.tolist(),
            "pressures": pressures.tolist(),
            # 필요하면 아래도 켜서 보내면 됨(용량 증가)
            # "histogram_raw": hist_raw.tolist(),
        }
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        return 0

    except Exception as e:
        dt_ms = int((time.time() - t0) * 1000)
        payload = {
            "ok": False,
            "stage": "measure",
            "ip": ip,
            "ch": args.ch,
            "duration_ms": dt_ms,
            "error": f"{type(e).__name__}: {e}",
            "traceback": traceback.format_exc(limit=50),
        }
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        return 30


if __name__ == "__main__":
    raise SystemExit(main())

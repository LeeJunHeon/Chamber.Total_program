# tools/rga_worker.py
# -*- coding: utf-8 -*-
"""
RGA Worker (standalone exe)
- 목적
  1) RGA 측정(srsinst) 수행
  2) NAS CSV에 "한 줄 append" (기존 구조 유지)
  3) 메인 프로그램이 즉시 그래프를 그릴 수 있도록 JSON(1줄)을 stdout으로 반환

- 메인 프로그램 → worker 전달 파라미터: --ch, --timeout 만 사용
  (IP/계정/CSV 경로는 이 파일 상단의 CH_CONFIG에 고정)
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple, Optional

import numpy as np

# ──────────────────────────────────────────────────────────────────────
# ✅ 여기만 수정하면 됨 (worker가 스스로 들고 있을 값들)
# ──────────────────────────────────────────────────────────────────────
CH_CONFIG: Dict[int, Dict[str, str]] = {
    1: {  # CH1
        "ip": "192.168.1.20",
        "user": "admin",
        "password": "admin",
        "csv": r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\CH1&2_Log\Ch.1\RGA\RGA_spectrums.csv",
    },
    2: {  # CH2
        "ip": "192.168.1.21",
        "user": "admin",
        "password": "admin",
        "csv": r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\CH1&2_Log\Ch.2\RGA\RGA_spectrums.csv",
    },
}

CSV_ENCODING = "utf-8-sig"
CSV_HAS_HEADER_IF_EMPTY = True  # 파일이 없거나 0바이트일 때만 헤더 생성
CSV_FLOAT_FMT = "{:.2E}"        # 업로드된 CSV처럼 'E' 표기 + 소수 2자리

# ──────────────────────────────────────────────────────────────────────
# srsinst import는 worker 내부에서만 수행 (메인 프로세스 안정성 확보)
# ──────────────────────────────────────────────────────────────────────
try:
    from srsinst.rga import RGA100
except ModuleNotFoundError as e:
    err = f"RGA 드라이버 import 실패: 누락 모듈={e.name}"
    print(json.dumps({"ok": False, "stage": "import", "error": err}, ensure_ascii=False), flush=True)
    raise SystemExit(11)
except Exception as e:
    err = f"RGA 드라이버 import 실패: {type(e).__name__}: {e}"
    print(json.dumps({"ok": False, "stage": "import", "error": err}, ensure_ascii=False), flush=True)
    raise SystemExit(12)


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _detect_csv_columns(path: Path) -> Optional[int]:
    """기존 CSV의 mass 컬럼 개수(첫 줄 기준). 없으면 None."""
    if (not path.exists()) or path.stat().st_size == 0:
        return None
    try:
        with open(path, "r", newline="", encoding=CSV_ENCODING) as f:
            r = csv.reader(f)
            first = next(r, None)
        if not first:
            return None
        return max(0, len(first) - 1)  # Time + N masses
    except Exception:
        return None


def _ensure_header_if_needed(path: Path, n: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not CSV_HAS_HEADER_IF_EMPTY:
        return
    if path.exists() and path.stat().st_size > 0:
        return
    with open(path, "w", newline="", encoding=CSV_ENCODING) as f:
        w = csv.writer(f)
        header = ["Time"] + [f"Mass {i}" for i in range(1, n + 1)]
        w.writerow(header)


def _append_row(path: Path, ts: str, pressures: np.ndarray) -> None:
    """✅ CSV 구조 유지: Time + N개 값, 값은 '%.2E' 형태"""
    with open(path, "a", newline="", encoding=CSV_ENCODING) as f:
        w = csv.writer(f)
        row = [ts] + [CSV_FLOAT_FMT.format(float(v)) for v in pressures.tolist()]
        w.writerow(row)


def rga_measure_once(ip: str, user: str, password: str) -> Tuple[np.ndarray, np.ndarray]:
    """
    1회 측정
    - mass_axis: x축
    - pressures : y축 (보정된 partial pressure spectrum)
    """
    rga = RGA100("tcpip", ip, user, password)
    try:
        rga.filament.turn_on()
        histogram = np.asarray(rga.scan.get_histogram_scan(), dtype=float)
        rga.filament.turn_off()

        pressures = np.asarray(
            rga.scan.get_partial_pressure_corrected_spectrum(histogram),
            dtype=float,
        )
        mass_axis = np.asarray(
            rga.scan.get_mass_axis(for_analog_scan=False),
            dtype=float,
        )
        return mass_axis, pressures
    finally:
        try:
            rga.disconnect()
        except Exception:
            pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ch", type=int, required=True, help="1 or 2")
    ap.add_argument("--timeout", type=float, default=30.0, help="soft timeout hint (main side will enforce kill)")
    args = ap.parse_args()

    if args.ch not in CH_CONFIG:
        print(json.dumps({"ok": False, "stage": "args", "error": f"unknown ch={args.ch}"}, ensure_ascii=False), flush=True)
        return 2

    cfg = CH_CONFIG[args.ch]
    ip = cfg["ip"]
    user = cfg.get("user", "admin")
    password = cfg.get("password", "admin")
    csv_path = Path(cfg["csv"])

    t0 = time.time()
    try:
        mass_axis, pressures = rga_measure_once(ip, user, password)

        n_existing = _detect_csv_columns(csv_path)
        n_new = int(len(pressures))

        if n_existing is None:
            _ensure_header_if_needed(csv_path, n_new)
        else:
            if n_existing != n_new:
                raise RuntimeError(f"CSV column mismatch: existing={n_existing}, new={n_new} (file={csv_path})")

        ts = now_str()
        _append_row(csv_path, ts, pressures)

        dt_ms = int((time.time() - t0) * 1000)
        payload = {
            "ok": True,
            "ch": args.ch,
            "ip": ip,
            "timestamp": ts,
            "duration_ms": dt_ms,
            "csv_path": str(csv_path),
            "mass_axis": mass_axis.tolist(),
            "pressures": pressures.tolist(),
        }
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        return 0

    except Exception as e:
        dt_ms = int((time.time() - t0) * 1000)
        payload = {
            "ok": False,
            "stage": "measure",
            "ch": args.ch,
            "ip": ip,
            "duration_ms": dt_ms,
            "error": f"{type(e).__name__}: {e}",
            "traceback": traceback.format_exc(limit=50),
        }
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        return 30


if __name__ == "__main__":
    raise SystemExit(main())

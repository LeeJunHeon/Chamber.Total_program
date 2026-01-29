# tools/rga_worker.py
# -*- coding: utf-8 -*-
"""
RGA Worker (standalone exe or script)
- 목적: RGA 측정(srsinst) + CSV append + JSON(1줄) stdout 응답
- 메인 프로그램이 subprocess로 실행해서 stdout JSON을 읽는다.
- 메인 → worker 전달 파라미터는 --ch, --timeout 만 사용한다.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List


# ====== 여기만 보면 됨(고정 파라미터) ======
CH_CONFIG: Dict[int, Dict[str, str]] = {
    1: {
        "ip": "192.168.1.20",
        "user": "admin",
        "password": "admin",
        "csv": r"\\VanaM_NAS\VanaM_Sputter\RGA\Ch.1\RGA_spectrums.csv",
    },
    2: {
        "ip": "192.168.1.21",
        "user": "admin",
        "password": "admin",
        "csv": r"\\VanaM_NAS\VanaM_Sputter\RGA\Ch.2\RGA_spectrums.csv",
    },
}
# =======================================


def now_str() -> str:
    # 기존 CSV Time 컬럼 포맷 유지
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_csv_header(path: Path, n: int, encoding: str = "utf-8-sig") -> None:
    """
    기존 RGA_spectrums.csv 구조 유지:
    Time,Mass 1,Mass 2,... 헤더가 없으면 1회 생성.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    if not path.exists() or path.stat().st_size == 0:
        with open(path, "w", newline="", encoding=encoding) as f:
            w = csv.writer(f)
            header = ["Time"] + [f"Mass {i}" for i in range(1, n + 1)]
            w.writerow(header)


def _read_existing_header_cols(path: Path, encoding: str = "utf-8-sig") -> int:
    """
    기존 파일이 있을 때 Mass 컬럼 개수 체크(구조 유지용)
    - Time 포함 컬럼 수를 읽어 Mass 개수로 환산
    """
    if not path.exists() or path.stat().st_size == 0:
        return 0
    with open(path, "r", encoding=encoding) as f:
        first = f.readline().strip()
    if not first:
        return 0
    cols = [c.strip() for c in first.split(",")]
    # Time + MassN...
    return max(0, len(cols) - 1)


def append_row(path: Path, ts: str, pressures: List[float], encoding: str = "utf-8-sig") -> None:
    """
    CSV 구조 유지:
    row = [timestamp] + [1.23E-09, ...] (문자열로 저장)
    """
    with open(path, "a", newline="", encoding=encoding) as f:
        w = csv.writer(f)
        row = [ts] + [f"{v:.2E}" for v in pressures]
        w.writerow(row)


def rga_measure_once(ip: str, user: str, password: str):
    """
    실제 측정은 여기서만 수행 (srsinst import도 worker 내부에서만)
    """
    try:
        from srsinst.rga import RGA100
    except Exception as e:
        raise RuntimeError(f"srsinst import failed: {type(e).__name__}: {e}")

    rga = RGA100("tcpip", ip, user, password)
    try:
        rga.filament.turn_on()
        histogram = rga.scan.get_histogram_scan()
        rga.filament.turn_off()

        pressures = rga.scan.get_partial_pressure_corrected_spectrum(histogram)
        mass_axis = rga.scan.get_mass_axis(for_analog_scan=False)

        # list[float]로 강제(직렬화 안정)
        mass_axis = [float(x) for x in mass_axis]
        pressures = [float(x) for x in pressures]
        return mass_axis, pressures
    finally:
        try:
            rga.disconnect()
        except Exception:
            pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ch", type=int, required=True, choices=[1, 2])
    ap.add_argument("--timeout", type=float, default=30.0)  # 메인에서 subprocess timeout으로 사용
    args = ap.parse_args()

    t0 = time.time()
    try:
        cfg = CH_CONFIG[int(args.ch)]
        ip = cfg["ip"]
        user = cfg["user"]
        password = cfg["password"]
        csv_path = Path(cfg["csv"])

        mass_axis, pressures = rga_measure_once(ip, user, password)

        # CSV 구조(컬럼 수) 유지 체크
        existing_n = _read_existing_header_cols(csv_path)
        if existing_n and existing_n != len(pressures):
            raise RuntimeError(
                f"CSV column mismatch: existing Mass={existing_n}, new Mass={len(pressures)} "
                f"(구조 변경 방지로 중단)"
            )

        ensure_csv_header(csv_path, n=len(pressures))
        ts = now_str()
        append_row(csv_path, ts, pressures)

        dt_ms = int((time.time() - t0) * 1000)

        payload = {
            "ok": True,
            "ch": int(args.ch),
            "ip": ip,
            "timestamp": ts,
            "duration_ms": dt_ms,
            "csv_path": str(csv_path),
            "mass_axis": mass_axis,
            "pressures": pressures,
        }
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        return 0

    except Exception as e:
        dt_ms = int((time.time() - t0) * 1000)
        payload = {
            "ok": False,
            "stage": "worker",
            "ch": int(getattr(args, "ch", 0) or 0),
            "duration_ms": dt_ms,
            "error": f"{type(e).__name__}: {e}",
            "traceback": traceback.format_exc(limit=50),
        }
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        return 30


if __name__ == "__main__":
    raise SystemExit(main())

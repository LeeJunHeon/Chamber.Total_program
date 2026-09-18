# tools/rga_worker.py
# -*- coding: utf-8 -*-
"""
RGA Worker (standalone exe or script)
- 목적: RGA 측정(srsinst) + CSV append + JSON(1줄) stdout 응답
- 메인 프로그램이 subprocess로 실행해서 stdout JSON을 읽는다.
- 메인 → worker 전달 파라미터: --ch, --timeout, --csv, --csv_fallback
  (--csv / --csv_fallback 이 없으면 CH_CONFIG 기본값과 exe 폴더 아래 로컬 폴백을 쓴다)
- 측정과 CSV 저장을 분리한다: 측정이 성공하면 CSV 저장이 실패해도 ok:true 로 응답하고,
  csv_ok / csv_path / csv_error / csv_fallback_used 로 저장 결과를 알린다.
  (CSV 드라이브가 없다고 해서 측정 결과를 버리지 않는다)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import socket
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
        "csv": r"G:\공유 드라이브\VanaM_Sputter\RGA\Ch.1\RGA_spectrums.csv",
    },
    2: {
        "ip": "192.168.1.21",
        "user": "admin",
        "password": "admin",
        "csv": r"G:\공유 드라이브\VanaM_Sputter\RGA\Ch.2\RGA_spectrums.csv",
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
        # ✅ 예외가 어디서 나든 filament는 끈다
        try:
            rga.filament.turn_off()
        except Exception:
            pass
        try:
            rga.disconnect()
        except Exception:
            pass

def _default_fallback_csv(ch: int) -> Path:
    """exe(또는 스크립트) 폴더 아래 로컬 폴백 경로."""
    if getattr(sys, "frozen", False):
        base = Path(sys.executable).resolve().parent
    else:
        base = Path(__file__).resolve().parent
    return base / "Logs_LocalFallback" / "RGA" / f"Ch.{int(ch)}" / "RGA_spectrums.csv"


def _save_csv(path: Path, pressures: List[float], ts: str) -> None:
    """한 파일에 저장. 컬럼 수 불일치는 구조 변경 방지를 위해 예외."""
    existing_n = _read_existing_header_cols(path)
    if existing_n and existing_n != len(pressures):
        raise RuntimeError(
            f"CSV column mismatch: existing Mass={existing_n}, new Mass={len(pressures)} "
            f"(구조 변경 방지로 중단)"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_csv_header(path, n=len(pressures))
    append_row(path, ts, pressures)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ch", type=int, required=True, choices=[1, 2])
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--csv", type=str, default="")           # 정본 CSV 경로
    ap.add_argument("--csv_fallback", type=str, default="")  # 정본 실패 시 로컬 폴백
    # 모르는 인자는 무시한다(구/신 메인 혼용 대비)
    args, _unknown = ap.parse_known_args()

    t0 = time.time()
    try:
        cfg = CH_CONFIG[int(args.ch)]
        ip = cfg["ip"]
        user = cfg["user"]
        password = cfg["password"]

        primary = Path(args.csv) if args.csv else Path(cfg["csv"])
        fallback = Path(args.csv_fallback) if args.csv_fallback else _default_fallback_csv(args.ch)

        # ✅ --timeout 을 실제로 쓴다: 장비가 응답하지 않을 때 부모가 죽이기 전에
        #    워커 안에서 socket timeout 예외로 끝나 원인을 JSON 으로 남기게 한다.
        try:
            socket.setdefaulttimeout(max(5.0, float(args.timeout)))
        except Exception:
            pass

        # ① 측정 (여기 실패만 '측정 실패')
        mass_axis, pressures = rga_measure_once(ip, user, password)
        ts = now_str()

        # ② CSV 저장 (실패해도 측정 결과는 살린다)
        csv_ok = False
        csv_used = ""
        csv_fallback_used = False
        csv_errors: List[str] = []
        try:
            _save_csv(primary, pressures, ts)
            csv_ok = True
            csv_used = str(primary)
        except Exception as e1:
            csv_errors.append(f"primary({primary}): {type(e1).__name__}: {e1}")
            try:
                _save_csv(fallback, pressures, ts)
                csv_ok = True
                csv_used = str(fallback)
                csv_fallback_used = True
            except Exception as e2:
                csv_errors.append(f"fallback({fallback}): {type(e2).__name__}: {e2}")

        dt_ms = int((time.time() - t0) * 1000)

        payload = {
            "ok": True,
            "worker_version": 2,
            "ch": int(args.ch),
            "ip": ip,
            "timestamp": ts,
            "duration_ms": dt_ms,
            "csv_ok": csv_ok,
            "csv_path": csv_used,
            "csv_error": " | ".join(csv_errors),
            "csv_fallback_used": csv_fallback_used,
            "mass_axis": mass_axis,
            "pressures": pressures,
        }
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        return 0

    except Exception as e:
        dt_ms = int((time.time() - t0) * 1000)
        payload = {
            "ok": False,
            "worker_version": 2,
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

r"""RGA 워커 프로세스 — `python -m v2.ch12.workers.rga_worker` 또는 onedir exe. v1 apps/rga_service/rga_api.py 를 옮긴 것.

장비 호출 순서·인자(v1 그대로, rga_measure_once): RGA100('tcpip', ip, user, password) → filament.turn_on() →
  scan.get_histogram_scan() → filament.turn_off() → scan.get_partial_pressure_corrected_spectrum(histogram) →
  scan.get_mass_axis(for_analog_scan=False), finally filament.turn_off()(두 번째)·disconnect(). 소켓 기본 타임아웃 max(5, timeout).
CSV(v1 그대로): utf-8-sig, 머리글 'Time,Mass 1..N'(없거나 0바이트일 때), 행 [YYYY-mm-dd HH:MM:SS] + '%.2E',
  머리글 Mass 수가 다르면 예외 → 폴백 CSV. 측정 성공이면 CSV 실패여도 ok:true.
응답 필드(v1 그대로): 성공 {ok, worker_version:2, ch, ip, timestamp, duration_ms, csv_ok, csv_path, csv_error,
  csv_fallback_used, mass_axis, pressures}, 실패 {ok:false, worker_version:2, stage:'worker', ch, duration_ms, error, traceback}.

v2 연결부(바뀐 것):
- 상주 프로세스: stdin JSONL 명령 {"v":2, "id":n, "cmd":"scan", "ch", "timeout", "csv", "csv_fallback", "user", "password"} →
  stdout {"v":2, "id":n, "kind":"result", ...v1 응답 필드...}. v1 은 스캔마다 '--ch --timeout --csv --csv_fallback' 로 새로 실행했다
  (같은 인자 이름 — 메인 rga_link 가 v1 재시도·kill 규칙대로 부른다). ping → pong, close·EOF → 끝(os._exit).
- 하트비트 {"kind":"hb", "state": idle|scanning} 를 --hb_s 마다(별도 스레드).
- 장비 계정: v1 은 코드에 고정(admin/admin, rga_api.py:30-44). v2 는 메인 설정(비밀값 RGA_NET)에서 명령으로 받는다 —
  코드·명령줄·로그에 남기지 않는다. 계정이 없으면 측정하지 않고 실패 응답('RGA 계정 설정 없음').
- 출력 JSON 은 v1 처럼 ensure_ascii=True(한국어 Windows 파이프 cp949 에서 한글이 깨지지 않게).
- --fake: srsinst 없이 가짜 RGA100(rga_fake) — 시험용.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import socket
import sys
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# ====== 장비 표(v1 rga_api.py:30-44 — 계정은 v2 에서 뺌: 메인 설정에서 받는다) ======
CH_CONFIG: Dict[int, Dict[str, str]] = {
    1: {"ip": "192.168.1.20", "csv": r"G:\공유 드라이브\VanaM_Sputter\RGA\Ch.1\RGA_spectrums.csv"},
    2: {"ip": "192.168.1.21", "csv": r"G:\공유 드라이브\VanaM_Sputter\RGA\Ch.2\RGA_spectrums.csv"},
}

RGA100_FACTORY = None      # --fake 일 때 가짜 RGA100 클래스
_OUT_LOCK = threading.Lock()
_STATE = {"state": "idle"}


def now_str() -> str:
    # 기존 CSV Time 컬럼 포맷 유지
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_csv_header(path: Path, n: int, encoding: str = "utf-8-sig") -> None:
    """기존 RGA_spectrums.csv 구조 유지: Time,Mass 1,Mass 2,... 헤더가 없으면 1회 생성."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.stat().st_size == 0:
        with open(path, "w", newline="", encoding=encoding) as f:
            w = csv.writer(f)
            header = ["Time"] + [f"Mass {i}" for i in range(1, n + 1)]
            w.writerow(header)


def _read_existing_header_cols(path: Path, encoding: str = "utf-8-sig") -> int:
    """기존 파일이 있을 때 Mass 컬럼 개수 체크(구조 유지용) — Time 포함 컬럼 수를 Mass 개수로 환산."""
    if not path.exists() or path.stat().st_size == 0:
        return 0
    with open(path, "r", encoding=encoding) as f:
        first = f.readline().strip()
    if not first:
        return 0
    cols = [c.strip() for c in first.split(",")]
    return max(0, len(cols) - 1)


def append_row(path: Path, ts: str, pressures: List[float], encoding: str = "utf-8-sig") -> None:
    """CSV 구조 유지: row = [timestamp] + [1.23E-09, ...] (문자열로 저장)."""
    with open(path, "a", newline="", encoding=encoding) as f:
        w = csv.writer(f)
        row = [ts] + [f"{v:.2E}" for v in pressures]
        w.writerow(row)


def _rga100_class():
    if RGA100_FACTORY is not None:
        return RGA100_FACTORY
    try:
        from srsinst.rga import RGA100      # 업체 라이브러리 — 워커 안에서만(v1 rga_api.py:98-101)
    except Exception as e:
        raise RuntimeError(f"srsinst import failed: {type(e).__name__}: {e}")
    return RGA100


def rga_measure_once(ip: str, user: str, password: str):
    """실제 측정은 여기서만 수행(v1 rga_api.py:94-125 그대로)."""
    RGA100 = _rga100_class()
    rga = RGA100("tcpip", ip, user, password)
    try:
        rga.filament.turn_on()
        histogram = rga.scan.get_histogram_scan()
        rga.filament.turn_off()

        pressures = rga.scan.get_partial_pressure_corrected_spectrum(histogram)
        mass_axis = rga.scan.get_mass_axis(for_analog_scan=False)

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


def scan_once(cmd: Dict[str, Any]) -> Dict[str, Any]:
    """스캔 1회 — v1 rga_api.py main() 본문(149-231). 인자는 명령 줄(v1 의 --ch·--timeout·--csv·--csv_fallback 과 같은 이름)."""
    t0 = time.time()
    ch = cmd.get("ch")
    try:
        ch = int(ch)
        if ch not in (1, 2):
            raise ValueError(f"ch must be 1 or 2 (got {ch})")
        cfg = CH_CONFIG[ch]
        ip = cfg["ip"]
        user = str(cmd.get("user") or "")
        password = str(cmd.get("password") or "")
        if not user:
            raise RuntimeError("RGA 계정 설정 없음(RGA_NET) — 측정하지 않음")
        timeout = float(cmd.get("timeout", 30.0) or 30.0)

        primary = Path(cmd["csv"]) if cmd.get("csv") else Path(cfg["csv"])
        fallback = Path(cmd["csv_fallback"]) if cmd.get("csv_fallback") else _default_fallback_csv(ch)

        # ✅ --timeout 을 실제로 쓴다: 장비가 응답하지 않을 때 부모가 죽이기 전에 socket timeout 예외로 끝낸다
        try:
            socket.setdefaulttimeout(max(5.0, float(timeout)))
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
        return {
            "ok": True, "worker_version": 2, "ch": int(ch), "ip": ip, "timestamp": ts, "duration_ms": dt_ms,
            "csv_ok": csv_ok, "csv_path": csv_used, "csv_error": " | ".join(csv_errors),
            "csv_fallback_used": csv_fallback_used, "mass_axis": mass_axis, "pressures": pressures,
        }
    except Exception as e:
        dt_ms = int((time.time() - t0) * 1000)
        return {
            "ok": False, "worker_version": 2, "stage": "worker", "ch": int(ch or 0) if str(ch or 0).isdigit() else 0,
            "duration_ms": dt_ms, "error": f"{type(e).__name__}: {e}", "traceback": traceback.format_exc(limit=50),
        }


# ───────── v2 연결부 ─────────
def _emit(obj: Dict[str, Any]) -> None:
    """한 줄 JSON — v1 처럼 ASCII 로만(ensure_ascii=True). 파이프가 깨져도 죽지 않는다."""
    s = json.dumps({**obj, "v": 2}, ensure_ascii=True)
    with _OUT_LOCK:
        try:
            sys.stdout.write(s + "\n")
            sys.stdout.flush()
        except Exception:
            pass


def _heartbeat(period_s: float) -> None:
    while True:
        _emit({"kind": "hb", "id": None, "state": _STATE["state"], "pid": os.getpid(), "ts": time.time()})
        time.sleep(period_s)


def serve(hb_s: float) -> int:
    if hb_s > 0:
        threading.Thread(target=_heartbeat, args=(hb_s,), name="rga-hb", daemon=True).start()
    _emit({"kind": "ready", "id": None, "worker_version": 2, "pid": os.getpid()})
    for line in sys.stdin:
        s = line.strip()
        if not s:
            continue
        try:
            cmd = json.loads(s)
            if not isinstance(cmd, dict):
                raise ValueError("not an object")
        except Exception as e:
            _emit({"kind": "status", "id": None, "message": f"[rga] invalid json: {type(e).__name__}: {e} raw={s[:200]}"})
            continue
        cid = cmd.get("id")
        name = str(cmd.get("cmd") or "").strip().lower()
        if name in ("ping", "health"):
            _emit({"kind": "pong", "id": cid, "ok": True, "ts": time.time()})
        elif name in ("close", "exit", "quit"):
            break
        elif name == "scan":
            _STATE["state"] = "scanning"
            try:
                _emit({"kind": "result", "id": cid, **scan_once(cmd)})
            finally:
                _STATE["state"] = "idle"
        else:
            _emit({"kind": "status", "id": cid, "message": f"[rga] unknown cmd={name}"})
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hb_s", type=float, default=1.0)
    ap.add_argument("--fake", action="store_true")
    args, _unknown = ap.parse_known_args(argv)     # 모르는 인자는 무시(v1 과 같은 성질)
    if args.fake:
        global RGA100_FACTORY
        from v2.ch12.workers import rga_fake
        RGA100_FACTORY = rga_fake.FakeRGA100
    return serve(float(args.hb_s))


if __name__ == "__main__":
    _rc = main()
    try:
        sys.stdout.flush()
    except Exception:
        pass
    os._exit(int(_rc))      # 스캔 스레드가 장비 호출에 걸려 있어도 남지 않게

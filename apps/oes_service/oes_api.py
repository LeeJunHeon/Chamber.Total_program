# apps/oes_service/oes_api.py
# -*- coding: utf-8 -*-
"""
OES worker process

- device/oes.py 의 OESAsync(=DLL 호출/실측 로직)를 '워커 프로세스'에서만 사용한다.
- 측정 중에는 로컬 CSV를 "행 단위 append + flush"로 계속 기록한다.
  (메인 프로그램은 이 CSV를 tail 하여 실시간 그래프 업데이트 가능)
- 메인 프로세스가 죽는 원인(0xc0000005 등)을 워커로 격리하기 위한 목적.

stdout: JSON 1줄씩 출력(메인에서 파싱 가능)
  - 시작: {"kind":"started", "ok":true, "out_csv":"...", "cols":1024}
  - 종료: {"kind":"finished", "ok":true, "out_csv":"...", "rows":1234, "elapsed_s":33.2}
  - 실패: {"kind":"finished", "ok":false, "error":"...", "trace":"..."}
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple


def _print_json(obj) -> None:
    sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def _ensure_project_import_path() -> None:
    """
    개발 실행 시( python oes_api.py )에도 project import가 되게 sys.path 보정.
    구조:
      CH_1_2_program/
        device/
        apps/oes_service/oes_api.py  <-- 현재 파일
    """
    try:
        root = Path(__file__).resolve().parents[2]  # CH_1_2_program/
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
    except Exception:
        pass


def _default_out_dir(ch: int) -> Path:
    # 사용자가 요청한 기본 로컬 폴더(메인에서 out_dir을 넘기면 그걸 우선 사용)
    # CH1 => C:\Users\vanam\Desktop\oes\CH1
    # CH2 => C:\Users\vanam\Desktop\oes\CH2
    base = Path(r"C:\Users\vanam\Desktop\oes")
    return base / f"CH{int(ch)}"


def _make_default_filename() -> str:
    # device/oes.py 와 동일한 파일명 패턴: OES_Data_YYYYMMDD_HHMMSS.csv
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"OES_Data_{ts}.csv"


async def _acquire_first_frame(oes, retries: int = 20, delay_s: float = 0.2):
    """
    첫 프레임(x,y) 확보 (간헐적으로 초기 몇 번 None이 나올 수 있어 방어)
    """
    last_err = None
    for _ in range(max(1, retries)):
        try:
            x, y = await oes._call(oes._acquire_one_slice_avg)
            if x is not None and y is not None:
                return x, y
        except Exception as e:
            last_err = e
        await asyncio.sleep(delay_s)
    if last_err:
        raise RuntimeError(f"first frame failed: {last_err}")
    raise RuntimeError("first frame failed: no data")


async def cmd_init(ch: int, usb: int, dll_path: Optional[str]) -> int:
    _ensure_project_import_path()
    try:
        from device.oes import OESAsync

        # save_directory는 init 시에는 큰 의미 없지만, 생성자 요구 파라미터라 임시로 둠
        temp_dir = _default_out_dir(ch)
        oes = OESAsync(chamber=int(ch), usb_index=int(usb), dll_path=dll_path, save_directory=str(temp_dir))

        ok = await oes.initialize_device()
        _print_json({
            "kind": "init",
            "ok": bool(ok),
            "ch": int(ch),
            "usb": int(usb),
            "resolved_usb": int(getattr(oes, "sChannel", -1)),
            "model": str(getattr(oes, "_model_name", "UNKNOWN")),
            "pixels": int(getattr(oes, "_npix", 0)),
        })

        # cleanup (채널 닫기)
        try:
            await oes.cleanup()
        except Exception:
            pass

        return 0 if ok else 2

    except Exception as e:
        _print_json({
            "kind": "init",
            "ok": False,
            "ch": int(ch),
            "usb": int(usb),
            "error": f"{type(e).__name__}: {e}",
            "trace": traceback.format_exc(),
        })
        return 3


async def cmd_measure(
    ch: int,
    usb: int,
    duration_s: float,
    integration_ms: int,
    sample_interval_s: float,
    avg_count: int,
    out_dir: Optional[Path],
    out_csv: Optional[Path],
    dll_path: Optional[str],
) -> int:
    _ensure_project_import_path()

    t0 = time.time()
    rows = 0

    # 출력 경로 결정
    if out_csv:
        out_csv = Path(out_csv).expanduser()
        out_dir_final = out_csv.parent
        filename = out_csv.name
    else:
        out_dir_final = Path(out_dir).expanduser() if out_dir else _default_out_dir(ch)
        filename = _make_default_filename()
        out_csv = out_dir_final / filename

    out_dir_final.mkdir(parents=True, exist_ok=True)

    # device/oes.py 는 save_directory를 넣으면 내부에서 CH1/CH2 폴더 정규화 로직이 있음
    # (p.name이 CH1/CH2면 그대로 유지, 아니면 /CH{n}을 붙임):contentReference[oaicite:6]{index=6}
    # -> out_dir_final이 ...\CH1 형태면 중복 없이 그대로 사용됨.
    save_directory = str(out_dir_final)

    f = None
    w = None

    try:
        from device.oes import OESAsync

        oes = OESAsync(
            chamber=int(ch),
            usb_index=int(usb),
            dll_path=dll_path,
            save_directory=save_directory,
            sample_interval_s=float(sample_interval_s),
            avg_count=int(avg_count),
            debug_print=False,  # 워커는 조용히(필요하면 True로)
        )

        # 1) 초기화
        ok = await oes.initialize_device()
        if not ok or getattr(oes, "sChannel", -1) < 0:
            raise RuntimeError("OES initialize_device() failed")

        # 2) 설정 적용(실패해도 진행):contentReference[oaicite:7]{index=7}
        try:
            await oes._call(oes._apply_device_settings_blocking, int(oes.sChannel), int(integration_ms))
        except Exception:
            pass

        # 3) CSV open + header 작성(첫 프레임 x를 헤더로 사용)
        f = open(out_csv, "w", newline="", encoding="utf-8")
        w = csv.writer(f)

        x, y = await _acquire_first_frame(oes)
        x_list = x.tolist() if hasattr(x, "tolist") else list(x)
        y_list = y.tolist() if hasattr(y, "tolist") else list(y)

        # 헤더: ["Time"] + x축(WL 또는 pixel index)  :contentReference[oaicite:8]{index=8}
        w.writerow(["Time"] + [float(v) for v in x_list])
        f.flush()

        _print_json({
            "kind": "started",
            "ok": True,
            "ch": int(ch),
            "usb": int(usb),
            "resolved_usb": int(getattr(oes, "sChannel", -1)),
            "out_csv": str(out_csv),
            "cols": int(len(x_list)),
            "sample_interval_s": float(sample_interval_s),
            "avg_count": int(avg_count),
            "integration_ms": int(integration_ms),
        })

        # 첫 행 기록
        now_s = datetime.now().strftime("%H:%M:%S")
        w.writerow([now_s] + [float(v) for v in y_list])
        rows += 1
        f.flush()

        # 4) duration 동안 반복 측정 (행 단위 append+flush)
        deadline = time.time() + max(0.0, float(duration_s))
        while time.time() < deadline:
            await asyncio.sleep(float(sample_interval_s))

            # 한 프레임 획득
            x2, y2 = await oes._call(oes._acquire_one_slice_avg)
            if x2 is None or y2 is None:
                continue

            y2_list = y2.tolist() if hasattr(y2, "tolist") else list(y2)
            if len(y2_list) != len(x_list):
                # 길이 불일치면 스킵(ROI/장비상태 일시 이상 방어)
                continue

            now_s = datetime.now().strftime("%H:%M:%S")
            w.writerow([now_s] + [float(v) for v in y2_list])
            rows += 1
            f.flush()

        elapsed = time.time() - t0

        # 5) 채널 닫기/정리
        try:
            await oes.cleanup()  # 내부적으로 close 수행:contentReference[oaicite:9]{index=9}
        except Exception:
            pass

        _print_json({
            "kind": "finished",
            "ok": True,
            "ch": int(ch),
            "usb": int(usb),
            "out_csv": str(out_csv),
            "rows": int(rows),
            "elapsed_s": float(elapsed),
        })
        return 0

    except Exception as e:
        elapsed = time.time() - t0
        _print_json({
            "kind": "finished",
            "ok": False,
            "ch": int(ch),
            "usb": int(usb),
            "out_csv": str(out_csv) if out_csv else None,
            "rows": int(rows),
            "elapsed_s": float(elapsed),
            "error": f"{type(e).__name__}: {e}",
            "trace": traceback.format_exc(),
        })
        return 10

    finally:
        try:
            if f:
                f.close()
        except Exception:
            pass


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--cmd", required=True, choices=["init", "measure"])
    p.add_argument("--ch", required=True, type=int)
    p.add_argument("--usb", required=True, type=int)

    p.add_argument("--dll_path", type=str, default=None)

    # measure 옵션
    p.add_argument("--duration", dest="duration_s", type=float, default=0.0)
    p.add_argument("--integration_ms", type=int, default=0)
    p.add_argument("--sample_interval_s", type=float, default=1.0)
    p.add_argument("--avg_count", type=int, default=3)

    # 출력 경로: out_csv가 있으면 그걸 우선 사용
    p.add_argument("--out_dir", type=str, default=None)
    p.add_argument("--out_csv", type=str, default=None)
    return p


async def _amain(argv=None) -> int:
    args = build_parser().parse_args(argv)

    if args.cmd == "init":
        return await cmd_init(args.ch, args.usb, args.dll_path)

    out_dir = Path(args.out_dir) if args.out_dir else None
    out_csv = Path(args.out_csv) if args.out_csv else None

    return await cmd_measure(
        ch=int(args.ch),
        usb=int(args.usb),
        duration_s=float(args.duration_s),
        integration_ms=int(args.integration_ms),
        sample_interval_s=float(args.sample_interval_s),
        avg_count=int(args.avg_count),
        out_dir=out_dir,
        out_csv=out_csv,
        dll_path=args.dll_path,
    )


def main(argv=None) -> int:
    try:
        return asyncio.run(_amain(argv))
    except KeyboardInterrupt:
        _print_json({"kind": "finished", "ok": False, "error": "KeyboardInterrupt"})
        return 130
    except Exception as e:
        _print_json({"kind": "fatal", "ok": False, "error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc()})
        return 99


if __name__ == "__main__":
    raise SystemExit(main())

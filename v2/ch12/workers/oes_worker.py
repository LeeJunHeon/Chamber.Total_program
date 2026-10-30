r"""OES 워커 프로세스 입구 — `python -m v2.ch12.workers.oes_worker ...` 또는 PyInstaller onedir exe.

v1 apps/oes_service/oes_api.py 를 v2/ch12/workers 로 옮긴 것(2026-09-30 OES 드라이버 BSOD 이력 — 메인과 다른 프로세스).
업체 DLL(SPdbUSBm.dll) 호출 순서·인자는 v1 그대로이고(oes_device·oes_guard·oes_measure·oes_daemon), 메인과의 연결부만 v2:

명령줄(v1 그대로): --cmd {init|measure|daemon} --ch N --usb U [--dll_path P] [--duration 초] [--integration_ms ms]
                  [--sample_interval_s 1.0] [--avg_count 3] [--out_dir D] [--out_csv F]
v2 가 더한 명령줄: --home D   워커 기준 폴더(log\·stop\·oes_config.json — 메인은 v1 배치 apps\oes_service 를 준다)
                  --id X     단발(init·measure) 명령 id — 출력 줄마다 실린다
                  --hb_s S   하트비트 간격(초, 기본 1.0, 0 이면 끔)
                  --fake     DLL 없이 가짜 분광기(oes_fake.FakeDll) — 시험용. 그 밖은 실제와 같은 코드가 돈다
JSONL(stdout, 한 줄 = JSON 하나, utf-8): v1 kind(status·init·daemon·started·finished·fatal·pong·reset) 그대로 +
  v2 봉투 "v": 2, "id": <명령 id 또는 null>, 그리고 하트비트 {"kind": "hb", "state": init|idle|measuring, "pid", "ts"}.
stdin(daemon): v2 메인은 {"v": 2, "id": n, "cmd": ...} — v1 의 {"cmd": ...} 도 그대로 받는다.
끝: daemon 은 close 명령이나 stdin EOF(메인이 죽음)에 정리하고 끝낸다. 항상 os._exit(rc) — DLL 스레드가 걸려 있어도 좀비가 안 남게.
종료 코드(v1): init 0/2/3/4, measure 0/10/4, daemon 0/4/99/2, 시간 초과 124·125·126, KeyboardInterrupt 130, 예외 99.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import sys
import time
import traceback
from pathlib import Path

from v2.ch12.workers import oes_common as C
from v2.ch12.workers.oes_common import _errlog_exc, _print_json, _status, _worker_base_dir
from v2.ch12.workers.oes_daemon import STATE, cmd_daemon
from v2.ch12.workers.oes_measure import cmd_init, cmd_measure


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--cmd", required=True, choices=["init", "measure", "daemon"])
    p.add_argument("--ch", required=True, type=int)
    p.add_argument("--usb", required=True, type=int)
    p.add_argument("--dll_path", type=str, default=None)

    p.add_argument("--duration", dest="duration_s", type=float, default=0.0)
    p.add_argument("--integration_ms", type=int, default=0)
    p.add_argument("--sample_interval_s", type=float, default=1.0)
    p.add_argument("--avg_count", type=int, default=3)
    p.add_argument("--out_dir", type=str, default=None)
    p.add_argument("--out_csv", type=str, default=None)
    # ── v2 연결부 ──
    p.add_argument("--home", type=str, default=None)
    p.add_argument("--id", dest="cmd_id", type=str, default=None)
    p.add_argument("--hb_s", type=float, default=1.0)
    p.add_argument("--fake", action="store_true")
    return p


async def _heartbeat(period_s: float) -> None:
    """v2 하트비트 — 이벤트 루프가 살아 있다는 표시(DLL 호출은 실행자 스레드라 측정 중에도 나온다). id 는 싣지 않는다."""
    while True:
        _print_json({"kind": "hb", "id": None, "state": STATE.get("state"), "pid": os.getpid(), "ts": time.time()})
        await asyncio.sleep(period_s)


async def _amain(argv=None) -> int:
    args = build_parser().parse_args(argv)
    C.set_home(args.home)
    C.set_command_id(args.cmd_id)
    if args.fake:
        from v2.ch12.workers import oes_fake
        oes_fake.install()
    C._load_anomaly_config()
    _status(f"[worker] START argv={sys.argv} frozen={getattr(sys,'frozen',False)} base={_worker_base_dir()} anomaly={C._ANOMALY_ENABLED}")

    hb = asyncio.create_task(_heartbeat(float(args.hb_s))) if float(args.hb_s) > 0 else None
    try:
        if args.cmd == "init":
            STATE["state"] = "init"
            out_dir = Path(args.out_dir) if args.out_dir else None
            out_csv = Path(args.out_csv) if args.out_csv else None
            return await cmd_init(args.ch, args.usb, args.dll_path, out_dir, out_csv)

        if args.cmd == "daemon":
            out_dir = Path(args.out_dir) if args.out_dir else None
            return await cmd_daemon(args.ch, args.usb, args.dll_path, out_dir)

        STATE["state"] = "measuring"
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
    finally:
        if hb is not None:
            hb.cancel()


def main(argv=None) -> int:
    try:
        return asyncio.run(_amain(argv))
    except KeyboardInterrupt:
        _print_json({"kind": "finished", "ok": False, "error": "KeyboardInterrupt"})
        return 130
    except Exception as e:
        _errlog_exc("fatal exception in main()")
        _print_json({"kind": "fatal", "ok": False, "error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc()})
        return 99


if __name__ == "__main__":
    # ✅ 워커는 절대 '정상 인터프리터 종료'를 하지 않는다(v1 oes_api.py:2436-2445) — DLL 호출에 갇힌 실행자 스레드가
    #    있으면 join 에서 영구 대기(좀비)하므로 asyncio.run 반환 뒤 하드 종료. _print_json 은 매 줄 flush.
    _rc = main()
    with contextlib.suppress(Exception):
        sys.stdout.flush()
        sys.stderr.flush()
    os._exit(int(_rc))

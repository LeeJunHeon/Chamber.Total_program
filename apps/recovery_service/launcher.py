# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from .paths import project_root, recovery_state_base_dir, recovery_state_dir, recovery_logs_dir

def _default_main_py() -> Path:
    return (project_root() / "main.py").resolve()

def main() -> int:
    ap = argparse.ArgumentParser(description="CH1/CH2 program minimal launcher (dev)")
    ap.add_argument("--main-py", default="", help="실행할 main.py 경로(기본: 프로젝트루트/main.py)")
    ap.add_argument("--workdir", default="", help="작업 디렉토리(기본: 프로젝트 루트)")
    ap.add_argument("--restart-delay", type=float, default=2.0, help="비정상 종료 후 재시작 대기(초)")
    ap.add_argument("--max-restarts", type=int, default=0, help="재시작 최대 횟수(0이면 무제한)")
    ap.add_argument("--pass-recovery-env", action="store_true", help="recovery_state 경로 env 전달")
    ap.add_argument("args", nargs=argparse.REMAINDER, help="main.py에 전달할 인자(앞에 -- 사용 권장)")
    args = ap.parse_args()

    main_py = Path(args.main_py).expanduser().resolve() if args.main_py.strip() else _default_main_py()
    if not main_py.exists():
        print(f"[launcher] main.py not found: {main_py}", file=sys.stderr)
        return 2

    wd = Path(args.workdir).expanduser().resolve() if args.workdir.strip() else project_root()

    # ✅ 디렉토리 자동 생성 금지: 없으면 명확히 실패
    if args.pass_recovery_env:
        base = recovery_state_base_dir()
        sdir = recovery_state_dir()
        ldir = recovery_logs_dir()
        if not base.exists():
            print(f"[launcher] recovery base dir not found: {base}", file=sys.stderr)
            return 3
        if not sdir.exists():
            print(f"[launcher] recovery state dir not found: {sdir}", file=sys.stderr)
            return 3
        if not ldir.exists():
            print(f"[launcher] recovery logs dir not found: {ldir}", file=sys.stderr)
            return 3

    restart_count = 0
    passthrough = [a for a in args.args if a != "--"]

    while True:
        env = os.environ.copy()
        if args.pass_recovery_env:
            env["CH12_RECOVERY_BASE_DIR"] = str(recovery_state_base_dir())
            env["CH12_RECOVERY_STATE_DIR"] = str(recovery_state_dir())
            env["CH12_RECOVERY_LOG_DIR"] = str(recovery_logs_dir())
            env["CH12_LAUNCHED_BY"] = "apps.recovery_service.launcher"

        cmd = [sys.executable, "-u", str(main_py)] + passthrough
        print(f"[launcher] start: {cmd} (cwd={wd})", flush=True)

        p = subprocess.Popen(cmd, cwd=str(wd), env=env)
        code = p.wait()
        print(f"[launcher] exited: code={code}", flush=True)

        # ✅ 0이면 정상 종료로 간주 → launcher도 종료
        if code == 0:
            return 0

        restart_count += 1
        if args.max_restarts > 0 and restart_count > args.max_restarts:
            print(f"[launcher] reached max restarts ({args.max_restarts}) -> stop", flush=True)
            return 4

        time.sleep(max(0.1, float(args.restart_delay)))

if __name__ == "__main__":
    raise SystemExit(main())

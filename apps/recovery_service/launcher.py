# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from .paths import project_root, recovery_state_base_dir, recovery_state_dir, recovery_logs_dir


def _resolve_main_exe(user_arg: str) -> Path:
    """
    main.exe 기본 탐색 규칙:
      1) --main-exe로 지정되면 그걸 사용 (상대경로면 프로젝트 루트 기준)
      2) 미지정이면 아래 후보들 중 존재하는 첫 번째 사용
    """
    root = project_root()

    if user_arg.strip():
        p = Path(user_arg).expanduser()
        if not p.is_absolute():
            p = (root / p).resolve()
        return p.resolve()

    candidates = [
        root / "dist" / "main.exe",
        root / "dist" / "CH_1_2_program.exe",
        root / "main.exe",
        root / "CH_1_2_program.exe",
    ]
    for c in candidates:
        if c.exists():
            return c.resolve()

    # 아무것도 못 찾으면 기본값(첫 후보)을 반환하되, main()에서 exists 검사로 에러 처리
    return candidates[0].resolve()


def _clean_state_files(state_dir: Path) -> None:
    """
    깔끔하게 처음부터 시작: state/*.json 삭제
    """
    if not state_dir.exists():
        return
    for p in state_dir.glob("*.json"):
        try:
            p.unlink()
        except Exception:
            pass
    for p in state_dir.glob("*.tmp"):
        try:
            p.unlink()
        except Exception:
            pass


def main() -> int:
    ap = argparse.ArgumentParser(description="CH1/CH2 program minimal launcher (exe)")
    ap.add_argument("--main-exe", default="", help="실행할 main.exe 경로(미지정 시 dist/main.exe 등 자동 탐색)")
    ap.add_argument("--workdir", default="", help="작업 디렉토리(미지정 시 main.exe 폴더)")
    ap.add_argument("--restart-delay", type=float, default=2.0, help="비정상 종료 후 재시작 대기(초)")
    ap.add_argument("--max-restarts", type=int, default=0, help="재시작 최대 횟수(0이면 무제한)")
    ap.add_argument("--pass-recovery-env", action="store_true", help="recovery_state 경로 env 전달 + 폴더 자동 생성")
    ap.add_argument("--clean-state", action="store_true", help="시작 전에 recovery_state/state/*.json 삭제(처음부터 깔끔히 시작)")
    ap.add_argument("args", nargs=argparse.REMAINDER, help="main.exe에 전달할 인자(앞에 -- 사용 권장)")
    args = ap.parse_args()

    main_exe = _resolve_main_exe(args.main_exe)
    if not main_exe.exists():
        print(f"[launcher] main.exe not found: {main_exe}", file=sys.stderr)
        print("[launcher] Tip: --main-exe <path-to-exe> 로 정확한 경로를 지정하세요.", file=sys.stderr)
        return 2

    wd = Path(args.workdir).expanduser().resolve() if args.workdir.strip() else main_exe.parent

    # ✅ 요청사항: 폴더 없으면 생성(현재 구조 유지)
    if args.pass_recovery_env:
        base = recovery_state_base_dir()
        sdir = recovery_state_dir()
        ldir = recovery_logs_dir()

        # base 자체도 생성
        base.mkdir(parents=True, exist_ok=True)
        sdir.mkdir(parents=True, exist_ok=True)
        ldir.mkdir(parents=True, exist_ok=True)

        if args.clean_state:
            _clean_state_files(sdir)

    restart_count = 0
    passthrough = [a for a in args.args if a != "--"]

    while True:
        env = os.environ.copy()
        if args.pass_recovery_env:
            env["CH12_RECOVERY_BASE_DIR"] = str(recovery_state_base_dir())
            env["CH12_RECOVERY_STATE_DIR"] = str(recovery_state_dir())
            env["CH12_RECOVERY_LOG_DIR"] = str(recovery_logs_dir())
            env["CH12_LAUNCHED_BY"] = "apps.recovery_service.launcher(exe)"

        cmd = [str(main_exe)] + passthrough
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

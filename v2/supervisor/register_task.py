"""로그온 자동 시작 등록 도구(윈도우 전용) — 실제 등록은 사람이 한다(사용자 지시 3장 (6)).

    python -m v2.supervisor.register_task --print          등록 명령만 보여 줌(기본)
    python -m v2.supervisor.register_task --apply          schtasks 로 등록(사람이 직접 실행)
    python -m v2.supervisor.register_task --remove         등록 해제

v1 처럼 schtasks /create /sc onlogon /it(사용자가 로그온해 있을 때만, 그 사용자의 화면에서) 으로 등록한다
(근거: v1 apps/recovery_service/launcher.py 의 schtasks 사용). 작업 이름 TASK_NAME.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

TASK_NAME = "Ch12_v2_Supervisor"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def create_cmd(python: str, root: Path, user: Optional[str] = None) -> List[str]:
    run = f'cmd /c cd /d "{root}" && "{python}" -m v2.supervisor'
    cmd = ["schtasks", "/create", "/tn", TASK_NAME, "/sc", "onlogon", "/it", "/rl", "highest", "/f", "/tr", run]
    if user:
        cmd += ["/ru", user]
    return cmd


def delete_cmd() -> List[str]:
    return ["schtasks", "/delete", "/tn", TASK_NAME, "/f"]


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m v2.supervisor.register_task")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--apply", action="store_true")
    g.add_argument("--remove", action="store_true")
    g.add_argument("--print", action="store_true")
    ap.add_argument("--user")
    a = ap.parse_args(argv)
    cmd = delete_cmd() if a.remove else create_cmd(sys.executable, repo_root(), a.user)
    print(subprocess.list2cmdline(cmd))
    if not (a.apply or a.remove):
        return 0
    if sys.platform != "win32":
        print("윈도우에서만 등록할 수 있음")
        return 1
    return subprocess.call(cmd)


if __name__ == "__main__":
    sys.exit(main())

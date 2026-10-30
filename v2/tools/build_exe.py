"""PyInstaller onedir 묶음 도구(결정 D01 — 기본 배포는 소스 실행, 이 도구는 v1 처럼 exe 로 묶을 때).

    python -m v2.tools.build_exe [--out dist_v2] [--dry-run]

만드는 것(모두 onedir — onefile 은 kill 이 부트로더만 죽인다, 사용자 지시 4장):
  ch12_v2/ch12_v2.exe            제어 프로그램(python -m v2.ch12 와 같음). 감시 프로그램(v2.supervisor)은 python -m 으로 자식을 켜므로
                                 exe 묶음에서는 자동 재시작이 없다 — 자동 재시작이 필요하면 소스 실행(run_ch12_v2.cmd)을 쓴다(결정 D321)
  ch12_robot_server/…exe          로봇 서버
  workers/oes_worker/…exe         OES 워커(업체 DLL — v1 apps/oes_service/SPdbUSBm.dll 경로는 설정 OES_DLL_PATH)
  workers/rga_worker/…exe         RGA 워커
노트북 화면(v2/hmi)은 데이터 파일로 함께 넣는다. 실제 빌드는 윈도우 장비 노트북에서 사람이 한다(pyinstaller 가 PATH 에 있어야).
--dry-run 이면 실행할 명령만 출력한다(리눅스 시험은 이것만 확인).
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

ROOT = Path(__file__).resolve().parents[2]
SEP = ";" if sys.platform == "win32" else ":"

TARGETS = (
    ("ch12_v2", "v2/ch12/__main__.py", ["--add-data", f"v2/hmi{SEP}v2/hmi"]),
    ("ch12_robot_server", "v2/robot_server/__main__.py", []),
    ("oes_worker", "v2/ch12/workers/oes_worker.py", []),
    ("rga_worker", "v2/ch12/workers/rga_worker.py", []),
)


def commands(out: Path) -> List[List[str]]:
    cmds = []
    for name, entry, extra in TARGETS:
        cmds.append([sys.executable, "-m", "PyInstaller", "--noconfirm", "--onedir", "--name", name,
                     "--distpath", str(out), "--workpath", str(out / "_build"), "--paths", str(ROOT)]
                    + extra + [str(ROOT / entry)])
    return cmds


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m v2.tools.build_exe")
    ap.add_argument("--out", default=str(ROOT / "dist_v2"))
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    for c in commands(Path(a.out)):
        print(subprocess.list2cmdline(c))
        if not a.dry_run:
            rc = subprocess.call(c, cwd=str(ROOT))
            if rc != 0:
                return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())

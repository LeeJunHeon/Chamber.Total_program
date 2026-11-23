"""저장소 경로가 윈도우에서 받을 수 있는가(2026-11-23 D427).

장비 노트북(윈도우)의 git 은 이름에 윈도우가 못 쓰는 글자가 든 파일이 하나라도 있으면 clone·checkout·reset 이 'invalid path' 로
멈춘다 — rewrite/v2 맨 위에 'C:\\VanaM_Logs\\CH1&2\\CH1&2_MFC/CH1_20251009.csv' 같은 시험 찌꺼기 4개가 커밋돼 있었다(9ca2649).
git 이 추적하는 모든 경로의 각 조각에 \\ : * ? " < > | 와 제어 글자(0~31)가 없고, 끝이 점·공백이 아니고, 예약 이름
(CON·PRN·AUX·NUL·COM1~9·LPT1~9 — 확장자가 붙어도)이 아니어야 한다(규칙: v2/common/winnames — 꾸러미 pack 도 같은 규칙).
저장소 안에 쓰는 시험은 conftest 가 실패로 잡는다.
"""
import subprocess
from pathlib import Path

import pytest

from v2.common.winnames import problems as windows_problems

REPO = Path(__file__).resolve().parents[2]


def _tracked() -> list:
    try:
        r = subprocess.run(["git", "ls-files", "-z"], cwd=REPO, capture_output=True, timeout=60)
    except Exception:
        return []
    if r.returncode != 0:
        return []
    return [p.decode("utf-8", "surrogateescape") for p in r.stdout.split(b"\0") if p]


def test_rule_catches_known_bad_names():
    assert windows_problems("C:\\VanaM_Logs\\CH1&2\\CH1&2_MFC/CH1_20251009.csv")
    assert windows_problems("a/b./c") and windows_problems("a/b /c") and windows_problems("x/CON") and windows_problems("x/nul.txt")
    assert windows_problems('a/b"c') and windows_problems("a/b|c") and windows_problems("a/b\tc")
    assert windows_problems("v2/tests/golden/outputs/calib_pending/files/G/막힘/Database/Ch1_log.csv") == []
    assert windows_problems("v2/hmi/assets/app-Y3V2W7JL.js") == [] and windows_problems("CONSOLE.md/conx") == []


def test_every_tracked_path_is_windows_safe():
    paths = _tracked()
    if not paths:
        pytest.skip("git 저장소가 아님(꾸러미 zip 등)")
    bad = {p: windows_problems(p) for p in paths if windows_problems(p)}
    assert bad == {}, f"윈도우에서 받을 수 없는 경로 {len(bad)}개: {list(bad.items())[:10]}"

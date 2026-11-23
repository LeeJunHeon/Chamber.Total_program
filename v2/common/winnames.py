"""윈도우에서 만들 수 있는 경로 이름인가(2026-11-23 D427·D429).

장비 노트북(윈도우)의 git 은 이름에 윈도우가 못 쓰는 글자가 든 파일이 하나라도 있으면 clone·checkout 이 'invalid path' 로 멈추고,
zip 도 그 파일을 풀지 못한다. '/' 로 나뉜 경로의 각 조각에 \\ : * ? " < > | 와 제어 글자(0~31)가 없고, 끝이 점·공백이 아니고,
예약 이름(CON·PRN·AUX·NUL·COM1~9·LPT1~9 — 확장자가 붙어도)이 아니어야 한다.
쓰는 곳: 시험 test_repo_paths(git 이 추적하는 모든 경로), 꾸러미 v2/tools/pack(zip 에 넣는 모든 경로).
"""
from __future__ import annotations

import re
from typing import List

BAD_CHARS = frozenset('\\:*?"<>|') | frozenset(chr(c) for c in range(32))
RESERVED = re.compile(r"^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\..*)?$", re.IGNORECASE)


def problems(path: str) -> List[str]:
    """경로 하나('/' 로 나뉜 git·zip 경로)의 윈도우 문제 — 빈 목록이면 윈도우에서 만들 수 있음."""
    out = []
    for part in path.split("/"):
        bad = sorted({c for c in part if c in BAD_CHARS})
        if bad:
            out.append(f"{part!r}: 못 쓰는 글자 {''.join(bad)!r}")
        if part.endswith((".", " ")):
            out.append(f"{part!r}: 끝이 점·공백")
        if RESERVED.match(part):
            out.append(f"{part!r}: 예약 이름")
    return out

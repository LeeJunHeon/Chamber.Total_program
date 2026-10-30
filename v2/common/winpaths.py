"""윈도우 경로 기본값을 리눅스·맥에서 쓸 수 있는 곳으로 바꿔 쓰기(실행 값만 — 설정 값 자체는 그대로).

왜: 설정 기본값(LOCAL_DATA_DIR 'C:\\CH12_v2\\data'·v1 기록 'C:\\VanaM_Logs\\…'·NAS·G: 공유 드라이브·감시 프로그램
--log-dir 'C:\\Ch12_v2\\logs' 등)은 장비 노트북(윈도우) 값이다. 리눅스·맥에서 그대로 쓰면 'C:\\CH12_v2\\data' 가 '이름에
역슬래시가 든 폴더 하나'로 현재 폴더 아래에 생긴다(저장소 안이 더러워지고 실행 위치마다 기록이 흩어진다).

규칙(윈도우가 아닐 때만 — 윈도우에서는 아무것도 바꾸지 않는다)
- 드라이브 문자 경로 'X:\\…' 또는 'X:/…' → <BASE>/X/…        (예: C:\\VanaM_Logs\\CH1&2 → <BASE>/C/VanaM_Logs/CH1&2)
- UNC '\\\\server\\share\\…'              → <BASE>/UNC/server/share/…
- 구분자는 \\ 와 / 둘 다로 나누고, 빈 칸·'.'·'..' 조각은 버린다(<BASE> 밖으로 나가지 않게).
<BASE> = 홈 아래 ~/.ch12_v2/winpaths 하나로 정함. 근거:
  ① 감시 프로그램(--log-dir)은 설정 폴더를 모른다 — 두 프로그램이 서로 값을 주고받지 않아도 같은 곳으로 바뀌어야 한다.
  ② --config-dir 기본값 'C:\\CH12_v2\\config' 자체가 윈도우 경로라 '<config-dir 상위>' 는 기준이 될 수 없다(먼저 바꿔야 함).
  ③ 실행 위치(현재 폴더)·저장소 위치와 무관하게 늘 같은 곳 — 시험은 HOME 을 임시 폴더로 바꿔 격리한다(Path.home 은 HOME 을 읽음).
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, List, Optional, Tuple

BASE_PARTS = (".ch12_v2", "winpaths")        # 홈 아래
_DRIVE = re.compile(r"^([A-Za-z]):[\\/]")
_UNC = re.compile(r"^\\\\[^\\/]")


def on_windows() -> bool:
    return os.name == "nt"


def base_dir() -> Path:
    return Path.home().joinpath(*BASE_PARTS)


def is_windows_path(v: Any) -> bool:
    """드라이브 문자('X:\\…'·'X:/…') 또는 UNC('\\\\server\\…') 모양의 글자인가."""
    return isinstance(v, str) and bool(_DRIVE.match(v) or _UNC.match(v))


def _parts(s: str) -> List[str]:
    return [p for p in re.split(r"[\\/]+", s) if p not in ("", ".", "..")]


def local_path(v: str, base: Optional[Path] = None) -> str:
    """윈도우 경로 하나를 <BASE> 아래 경로로(윈도우 경로 모양이 아니면 그대로). 윈도우 여부는 부르는 쪽이 본다."""
    if not is_windows_path(v):
        return v
    b = Path(base) if base is not None else base_dir()
    m = _DRIVE.match(v)
    if m:
        return str(b.joinpath(m.group(1).upper(), *_parts(v[2:])))
    return str(b.joinpath("UNC", *_parts(v[2:])))


def _map(v: Any, base: Path) -> Tuple[Any, int]:
    if isinstance(v, str):
        n = local_path(v, base)
        return n, int(n != v)
    if isinstance(v, list):
        out = [_map(x, base) for x in v]
        return [x for x, _ in out], sum(c for _, c in out)
    if isinstance(v, tuple):
        out = [_map(x, base) for x in v]
        return tuple(x for x, _ in out), sum(c for _, c in out)
    if isinstance(v, dict):
        out = {k: _map(x, base) for k, x in v.items()}
        return {k: x for k, (x, _) in out.items()}, sum(c for _, c in out.values())
    return v, 0


def map_values(cfg: dict, base: Optional[Path] = None, windows: Optional[bool] = None) -> List[str]:
    """실행 설정 사전(cfg — 복사본)의 윈도우 경로 값을 제자리에서 바꾼다(목록·사전 안의 글자까지). 바꾼 키 목록을 돌려준다.
    윈도우면 아무것도 안 함(빈 목록)."""
    if on_windows() if windows is None else windows:
        return []
    b = Path(base) if base is not None else base_dir()
    keys = []
    for k in list(cfg):
        v, n = _map(cfg[k], b)
        if n:
            cfg[k] = v
            keys.append(k)
    return keys


def runtime_path(v: str, windows: Optional[bool] = None) -> str:
    """경로 하나(명령줄 인자 등) — 윈도우가 아니면 바꾼 값, 윈도우면 그대로."""
    if on_windows() if windows is None else windows:
        return v
    return local_path(v)


def note(keys: List[str], base: Optional[Path] = None) -> str:
    """시작 로그 한 줄."""
    b = Path(base) if base is not None else base_dir()
    head = ", ".join(keys[:6]) + (f" 외 {len(keys) - 6}개" if len(keys) > 6 else "")
    return f"윈도우 경로 {len(keys)}개를 {b} 아래로 바꿔 씀(이 OS 는 윈도우가 아님 — 설정 값은 그대로): {head}"

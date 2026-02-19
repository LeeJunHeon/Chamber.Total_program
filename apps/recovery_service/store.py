# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Optional

from .paths import recovery_state_dir

def _state_file(kind: str, ch: int) -> Path:
    k = str(kind).strip().lower()
    if k not in ("chamber", "pc", "tsp"):
        raise ValueError(f"invalid kind: {kind!r}")
    if k == "tsp":
        return recovery_state_dir() / "tsp.json"
    if int(ch) not in (1, 2):
        raise ValueError(f"invalid ch: {ch}")
    return recovery_state_dir() / f"{k}_ch{int(ch)}.json"

def read(kind: str, ch: int = 0) -> Optional[dict[str, Any]]:
    path = _state_file(kind, ch)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None

def write_atomic(kind: str, ch: int, data: Mapping[str, Any]) -> None:
    """
    원자적 저장(tmp -> replace).
    ✅ 폴더가 없으면 자동 생성
    """
    path = _state_file(kind, ch)
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp = path.with_suffix(path.suffix + ".tmp")
    txt = json.dumps(dict(data), ensure_ascii=False, indent=2)
    tmp.write_text(txt, encoding="utf-8")
    tmp.replace(path)

def patch(kind: str, ch: int, fields: Mapping[str, Any]) -> None:
    cur = read(kind, ch) or {}
    if not isinstance(cur, dict):
        cur = {}
    cur.update(dict(fields))
    write_atomic(kind, ch, cur)

def delete(kind: str, ch: int = 0) -> None:
    path = _state_file(kind, ch)
    if path.exists():
        path.unlink()

# lib/thickness_factors.py
# -*- coding: utf-8 -*-
"""두께 모니터 Factor 파일(config/thickness_factors_ch{N}.json) — 읽기·검증·원자적 저장. Qt 없음.

구조:
  tooling[chuck][gun]  — chuck ∈ up/mid/down, gun ∈ G1/G2/G3, 단위 %(10.0~399). 비어 있으면(null) 미설정.
  material[gun]        — {"material": 재료 이름} — 재료 목록(lib/thickness_materials, config/thickness_materials.json)의 name.
                         density·Z 는 공정 시작 때 이 이름으로 재료 목록에서 찾는다. 비어 있으면 미선택.
exe 옆 config 폴더에 두어 재빌드 없이 고친다. 파일이 없으면 빈 표. 잘못된 값은 경고 후 그 칸만 미설정.
이전 형식(건별 density·z_factor 숫자)은 경고 후 무시한다(재료를 다시 고른다).
저장은 같은 폴더의 임시 파일에 쓴 뒤 os.replace(원자적 교체). 경로는 pathlib 그대로(한글 경로 안전).
"""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from device.thickness_monitor import TOOLING_MAX, TOOLING_MIN

CHUCKS = ("up", "mid", "down")
GUNS = ("G1", "G2", "G3")
FILE_VERSION = 2


def _blank_tooling() -> Dict[str, Dict[str, Optional[float]]]:
    return {c: {g: None for g in GUNS} for c in CHUCKS}


def _blank_material() -> Dict[str, Optional[str]]:
    return {g: None for g in GUNS}


@dataclass
class ThicknessFactors:
    tooling: Dict[str, Dict[str, Optional[float]]] = field(default_factory=_blank_tooling)
    material: Dict[str, Optional[str]] = field(default_factory=_blank_material)   # 건 → 재료 이름(None = 미선택)

    def tooling_for(self, chuck: str, gun: str) -> Optional[float]:
        return (self.tooling.get(str(chuck).lower()) or {}).get(str(gun).upper())

    def material_name(self, gun: str) -> Optional[str]:
        return self.material.get(str(gun).upper())

    def to_json(self) -> dict:
        return {
            "version": FILE_VERSION,
            "tooling": {c: {g: self.tooling[c][g] for g in GUNS} for c in CHUCKS},
            "material": {g: {"material": self.material[g] or ""} for g in GUNS},
        }


def check_tooling(v) -> Tuple[Optional[float], str]:
    """tooling 한 칸 — (값, 오류문구). 빈값/None 은 (None, '')."""
    if v is None or (isinstance(v, str) and not v.strip()):
        return None, ""
    if isinstance(v, bool):
        return None, f"{v!r} 숫자가 아님"
    try:
        f = float(v)
    except Exception:
        return None, f"{v!r} 숫자가 아님"
    if f != f or not (TOOLING_MIN <= f <= TOOLING_MAX):
        return None, f"{f:g} 범위({TOOLING_MIN:g}~{TOOLING_MAX:g}%) 밖"
    return f, ""


def load_factors(path) -> Tuple[ThicknessFactors, List[str]]:
    """(ThicknessFactors, 경고 목록) — 예외를 던지지 않는다. 파일이 없으면 빈 표(경고 없음)."""
    fac = ThicknessFactors()
    warns: List[str] = []
    p = Path(path)
    if not p.exists():
        return fac, warns
    try:
        data = json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception as e:
        warns.append(f"Factor 파일을 읽을 수 없음({p.name}) — 빈 표로 시작: {e!r}")
        return fac, warns
    if not isinstance(data, dict):
        warns.append(f"Factor 파일 형식 오류({p.name}) — 빈 표로 시작")
        return fac, warns

    tool = data.get("tooling") or {}
    if not isinstance(tool, dict):
        warns.append("tooling 이 표(객체)가 아님 — 전부 미설정")
        tool = {}
    for c in CHUCKS:
        row = tool.get(c) or {}
        if not isinstance(row, dict):
            warns.append(f"tooling.{c} 가 표(객체)가 아님 — 그 줄 미설정")
            continue
        for g in GUNS:
            v, err = check_tooling(row.get(g))
            if err:
                warns.append(f"tooling[{c}][{g}] {err} — 미설정")
            fac.tooling[c][g] = v

    mat = data.get("material") or {}
    if not isinstance(mat, dict):
        warns.append("material 이 표(객체)가 아님 — 전부 미선택")
        mat = {}
    for g in GUNS:
        m = mat.get(g) or {}
        if not isinstance(m, dict):
            warns.append(f"material.{g} 가 표(객체)가 아님 — 미선택")
            continue
        if any(m.get(k) not in (None, "") for k in ("density", "z_factor")) and not m.get("material"):
            warns.append(f"material.{g}: 이전 형식(density·Z 숫자)은 쓰지 않음 — [Factor 설정]에서 재료를 다시 고르십시오")
        name = m.get("material")
        fac.material[g] = str(name).strip() if isinstance(name, str) and name.strip() else None
    return fac, warns


def save_factors(path, fac: ThicknessFactors) -> Path:
    """같은 폴더 임시 파일에 쓰고 os.replace 로 교체한다. 실패하면 예외(원본은 그대로)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(fac.to_json(), ensure_ascii=False, indent=2) + "\n"
    fd, tmp = tempfile.mkstemp(prefix=p.name + ".", suffix=".tmp", dir=str(p.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fp:
            fp.write(text)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp, str(p))
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return p

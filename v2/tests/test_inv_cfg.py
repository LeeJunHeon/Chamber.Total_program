"""전수 목록 cfg(v2/ch12/spec/inv_cfg.py) 항목 시험 — v1 설정 키마다 v2 키·기본값·쓰는 곳을 실제로 확인한다.

test_key[<v1 경로>] (경로 = settings_v1facts KEYS 의 "common.X"·"ch1.X"·"ch2.X"·"구역.X.하위"·"(code).X")
  1) v2 대응: 변환 도구(v2/tools/convert_v1_config.Converter)와 같은 색인으로 그 v1 키를 옮기는 v2 파라미터(이름 규칙
     common.X → X, chN.X → CHN_X, 또는 ParamDef.v1_key)가 있거나, 조각 DROPPED 에 근거가 있다.
  2) 기본값: v2 기본값 = v1 실효값 — json 값, 단 ⚠ 'common 값은 효과 없음'(채널 모듈이 import 때 복사한 .py 값을 씀 — D133)
     과 settings.json 에 없는 선언 상수는 .py 값, '이 채널 값은 읽히지 않음'은 common 값(D134).
  3) 쓰는 곳: v2 코드(v2/ch12·v2/common·v2/robot_server·v2/supervisor — params·spec 조각 제외, 독스트링 제외)의 글자 상수나
     f-글자 틀에 그 키 이름(CH1_/CH2_ 접두어·f"{prefix}X" 포함)이 있다. 이름을 동적으로 만드는 곳·포털이 읽는 키는 USE 표에
     (파일, 글자) 로 적고 그 파일에 그 글자가 있는지 본다(포털 vanam-process 가 같은 상위 폴더에 없으면 포털 쪽만 건너뜀).
     버린 키(DROPPED)는 대신하는 키(replaced_by)가 같은 방식으로 쓰이는지, 또는 FIXED 표(v2 코드 상수 = v1 값)로 본다.
  na 항목(v1 에서도 읽지 않던 키): v1 사실표에 '읽는 곳 없음'이고 v2 코드에서도 쓰지 않음을 확인한다(test_unread_key).
"""
from __future__ import annotations

import ast
import json
import re
from functools import lru_cache
from pathlib import Path

import pytest

from v2.ch12.params import ownership as own
from v2.ch12.params import table as params_table
from v2.ch12.spec import inv_cfg
from v2.ch12.spec import settings_v1facts as facts
from v2.tools import convert_v1_config as conv

ROOT = Path(__file__).resolve().parents[2]
PORTAL = ROOT.parent / "vanam-process"
SCAN_DIRS = ("v2/ch12", "v2/common", "v2/robot_server", "v2/supervisor")
SKIP_DIRS = ("v2/ch12/params/", "v2/ch12/spec/")
FACT = {k[0]: k for k in facts.KEYS}
T = params_table()
CONV = conv.Converter(T)
_ID = re.compile(r"[A-Za-z0-9_]+")


# ───────────── v2 코드 색인(글자 상수·f-글자 틀) ─────────────
@lru_cache(maxsize=1)
def _index():
    tokens: dict = {}             # 글자 상수 안의 이름 조각 → 파일들
    templates: list = []          # (정규식, 파일) — f"{pre}_TCP_HOST" 처럼 이름을 만드는 틀
    for d in SCAN_DIRS:
        for f in sorted((ROOT / d).rglob("*.py")):
            rel = f.relative_to(ROOT).as_posix()
            if rel.startswith(SKIP_DIRS):
                continue
            tree = ast.parse(f.read_text(encoding="utf-8"))
            skip = set()
            for n in ast.walk(tree):
                if isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant) and isinstance(n.value.value, str):
                    skip.add(id(n.value))                       # 독스트링·떠 있는 글자는 쓰는 곳이 아니다
                if isinstance(n, ast.JoinedStr):
                    parts, lits = [], []
                    for v in n.values:
                        if isinstance(v, ast.Constant):
                            skip.add(id(v))
                            parts.append(re.escape(str(v.value)))
                            lits.append(str(v.value))
                        else:
                            parts.append("[A-Za-z0-9_]*")
                    if (len(n.values) > 1 and lits and all(_ID.fullmatch(x) for x in lits)
                            and max(len(x.strip("_")) for x in lits) >= 6):     # 이름 조각이 있는 틀만(CH{n}_{x} 같은 것 제외)
                        templates.append((re.compile("".join(parts)), rel))
            for n in ast.walk(tree):
                if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in skip:
                    for tok in _ID.findall(n.value):
                        tokens.setdefault(tok, set()).add(rel)
    return tokens, templates


def uses(name: str) -> set:
    """v2 코드에서 이름 name(v1 이름 — CH1_/CH2_ 붙은 꼴·f-글자 틀 포함)을 쓰는 파일."""
    tokens, templates = _index()
    cands = (name, "_" + name, "CH1_" + name, "CH2_" + name)
    out = set()
    for c in cands:
        out |= tokens.get(c, set())
    for rx, rel in templates:
        if any(rx.fullmatch(c) for c in cands):
            out.add(rel)
    return out


def _where_ok(where) -> list:
    """[(파일, 글자)] 중 확인 못 한 것(포털 없으면 포털 항목은 건너뜀)."""
    bad = []
    for path, snippet in where:
        if path.startswith("vanam-process/"):
            p = PORTAL / path[len("vanam-process/"):]
            if not PORTAL.exists():
                continue
        else:
            p = ROOT / path
        if not p.is_file() or snippet not in p.read_text(encoding="utf-8", errors="replace"):
            bad.append((path, snippet))
    return bad


# ───────────── v1 실효값 ─────────────
def v1_effective(path: str):
    f = FACT.get(path)
    if f is None:                                    # ch1 구역에 없는 채널 키 — 채널 모듈 복사값(.py)
        sec, rest = path.split(".", 1)
        f = FACT.get("ch2." + rest) or FACT.get("common." + rest)
        return f[2]
    eff = conv._effect(path)
    if eff == "dead_channel":                        # 채널 값은 읽히지 않음 — common 값
        sec, name, sub = own.split_path(path)
        return v1_effective(f"common.{name}" + (f".{sub}" if sub else ""))
    if path.startswith("(code).") or eff == "dead_common" or f[1] is None:
        return f[2]
    return f[1]


def _default_of(p):
    if p.pattern == r"\[.*\]" and isinstance(p.default, str):
        return json.loads(p.default)
    return p.default

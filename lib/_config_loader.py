# lib/_config_loader.py
# -*- coding: utf-8 -*-
"""
설정 JSON 로더
==============
config/settings.json을 읽어서 기존 config_common / config_ch1 / config_ch2
모듈 변수에 안전하게 덮어쓴다.

원칙
----
- .py 파일의 하드코딩 값이 **항상 먼저 로드**된다 (안전망).
- 먼저 파일 전체를 검사하고, 고칠 곳이 **하나도 없을 때만** 값을 덮어쓴다(전부 아니면 전무).
  고칠 곳: 파일 없음 · JSON 문법 오류 · 알 수 없는 구역/키(오타) · 같은 키 두 번 · 타입 불일치 ·
  settings.json 으로 바꿀 수 없는 항목(Path, bytes, 함수).
  일부만 무시하고 나머지를 적용하면 사용자가 바꾼 값이 조용히 빠지므로 그렇게 하지 않는다.
- UTF-8 BOM(메모장 저장)은 그대로 읽는다.
- 섹션(common/ch1/ch2)을 통째로 빼면 그 섹션은 .py 값을 쓴다(오타 구역 이름은 오류).

사용법
------
    # main.py: 고칠 곳이 있으면 알림창으로 목록을 보여 주고 시작하지 않는다.
    from lib._config_loader import check_and_load_settings, format_problems
    res = check_and_load_settings()
    if res.problems: ... format_problems(res) ...

    # 예전 방식(테스트 등): 고칠 곳이 있으면 아무것도 적용하지 않고 {} 반환
    from lib._config_loader import load_settings
    load_settings()
"""

from __future__ import annotations

import difflib
import json
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────
# JSON 경로 결정
# ──────────────────────────────────────────────────────────

def _settings_path() -> Path:
    """
    settings.json 위치:
      - PyInstaller exe: exe가 있는 폴더/config/settings.json
      - 소스 실행:       프로젝트 루트/config/settings.json
    """
    if getattr(sys, "frozen", False):
        base = Path(sys.executable).resolve().parent
    else:
        # lib/_config_loader.py → parents[1] = 프로젝트 루트
        base = Path(__file__).resolve().parents[1]
    return base / "config" / "settings.json"


# ──────────────────────────────────────────────────────────
# 타입 호환 검사
# ──────────────────────────────────────────────────────────

# int와 float은 서로 호환 (JSON에서 1.0 → int로 쓰거나 그 반대)
_NUMERIC = (int, float)


def _type_compatible(old_val: Any, new_val: Any) -> bool:
    """기존 값과 새 값의 타입이 호환되는지 확인."""

    # 기존 값이 None이면 어떤 JSON 값이든 허용
    if old_val is None:
        return True

    # bool 은 숫자와 섞지 않는다(true ↔ 1 은 오타일 가능성이 크다; Python 에서 bool 은 int 의 하위 타입)
    if isinstance(old_val, bool) or isinstance(new_val, bool):
        return isinstance(old_val, bool) and isinstance(new_val, bool)

    # int ↔ float 호환
    if isinstance(old_val, _NUMERIC) and isinstance(new_val, _NUMERIC):
        return True

    # 같은 타입
    if type(old_val) is type(new_val):
        return True

    # list ↔ tuple 호환
    if isinstance(old_val, (list, tuple)) and isinstance(new_val, (list, tuple)):
        return True

    return False


# ──────────────────────────────────────────────────────────
# dict 키 변환 (MFC_SCALE_FACTORS 등 int-key dict 처리)
# ──────────────────────────────────────────────────────────

def _convert_dict_keys(old_val: Any, new_val: Any) -> Any:
    """
    기존 값이 {int: ...} dict이고 JSON 값이 {"str": ...} dict이면
    키를 int로 변환해서 반환한다.
    예: 기존 {1: 1.0, 2: 10.0} / JSON {"1": 1.0, "2": 10.0}
        → {1: 1.0, 2: 10.0}
    """
    if not isinstance(old_val, dict) or not isinstance(new_val, dict):
        return new_val

    if not old_val:
        return new_val

    # 기존 키가 전부 int이고, 새 키가 전부 str(숫자)이면 변환
    old_keys_are_int = all(isinstance(k, int) for k in old_val)
    if old_keys_are_int:
        converted = {}
        for k, v in new_val.items():
            try:
                converted[int(k)] = v
            except (ValueError, TypeError):
                converted[k] = v
        return converted

    return new_val


# ──────────────────────────────────────────────────────────
# 검사 결과
# ──────────────────────────────────────────────────────────

_SECTIONS = ("common", "ch1", "ch2")


@dataclass
class SettingsResult:
    path: Path
    data: Dict[str, Any] = field(default_factory=dict)   # 적용한 JSON(고칠 곳이 있으면 {})
    problems: List[str] = field(default_factory=list)    # 사람이 읽는 "어디: 무엇 — 어떻게 고칠지"
    applied: int = 0                                      # 적용한 값 개수
    missing: bool = False                                 # 파일 없음

    @property
    def ok(self) -> bool:
        return not self.problems


def _section_modules() -> Dict[str, Any]:
    from lib import config_common
    from lib import config_ch1
    from lib import config_ch2
    return {"common": config_common, "ch1": config_ch1, "ch2": config_ch2}


def _kind(v: Any) -> str:
    """값 종류를 settings.json 에 쓰는 모양으로 설명."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true/false"
    if isinstance(v, (int, float)):
        return "숫자"
    if isinstance(v, str):
        return '따옴표로 감싼 글자'
    if isinstance(v, (list, tuple)):
        return "목록([ ... ])"
    if isinstance(v, dict):
        return "묶음({ ... })"
    return type(v).__name__


def _short(v: Any, limit: int = 60) -> str:
    try:
        t = json.dumps(v, ensure_ascii=False)
    except Exception:
        t = repr(v)
    return t if len(t) <= limit else t[: limit - 3] + "..."


class _PairsDict(dict):
    """json object_pairs_hook 결과 — 같은 키가 두 번 나온 것을 기억한다."""
    dups: List[str]


def _pairs_hook(pairs):
    d = _PairsDict()
    d.dups = []
    for k, v in pairs:
        if k in d and k not in d.dups:
            d.dups.append(k)
        d[k] = v
    return d


def _plain(v: Any) -> Any:
    """_PairsDict → 보통 dict (모듈에 넣는 값은 보통 dict/list 여야 한다)."""
    if isinstance(v, dict):
        return {k: _plain(vv) for k, vv in v.items()}
    if isinstance(v, list):
        return [_plain(vv) for vv in v]
    return v


def _nested_dups(v: Any, where: str, out: List[str]) -> None:
    """값 안쪽 묶음({ })의 중복 키."""
    if isinstance(v, dict):
        for k in getattr(v, "dups", []):
            out.append(f'{where} 안의 "{k}": 같은 이름이 두 번 있습니다 — 하나를 지우세요(지금은 뒤의 값만 쓰입니다)')
        for k, vv in v.items():
            _nested_dups(vv, f"{where}.{k}", out)
    elif isinstance(v, list):
        for vv in v:
            _nested_dups(vv, where, out)


_JSON_HINTS = (
    ("Expecting ',' delimiter",
     "쉼표(,)가 빠졌습니다 — 바로 앞 항목(보통 앞 줄) 끝에 쉼표를 넣으세요"),
    ("Expecting property name enclosed in double quotes",
     "이름이 와야 할 곳입니다 — 마지막 항목 뒤의 쉼표(,)를 지우거나, 이름을 큰따옴표(\"\")로 감싸세요"),
    ("Illegal trailing comma",
     "마지막 항목 뒤의 쉼표(,)를 지우세요"),
    ("Expecting ':' delimiter",
     "이름 뒤에 콜론(:)이 빠졌습니다 — \"이름\": 값 모양으로 쓰세요"),
    ("Expecting value",
     "값이 없거나 잘못됐습니다 — 글자는 큰따옴표로 감싸고(\"abc\"), true/false/null 은 소문자로, 마지막 쉼표는 지우세요"),
    ("Unterminated string",
     "큰따옴표(\")가 닫히지 않았습니다 — 글자 끝에 \" 를 넣으세요"),
    ("Invalid \\escape",
     "역슬래시(\\)는 두 번 써야 합니다 — 예: \"C:\\\\VanaM_Logs\" (경로에 자주 생김)"),
    ("Invalid \\uXXXX escape",
     "역슬래시(\\)는 두 번 써야 합니다 — 예: \"C:\\\\Users\" (경로에 자주 생김)"),
    ("Invalid control character",
     "글자(\"...\") 안에 줄바꿈·탭이 들어 있습니다 — 한 줄로 쓰세요"),
    ("Extra data",
     "파일 끝 } 뒤에 글자가 더 있습니다 — 중괄호 짝을 확인하세요"),
)


def _json_problem(e: json.JSONDecodeError, lines: List[str]) -> str:
    hint = next((h for k, h in _JSON_HINTS if e.msg.startswith(k)), f"JSON 문법 오류({e.msg})")
    msg = f"줄 {e.lineno} 칸 {e.colno}: {hint}"
    if 1 <= e.lineno <= len(lines):
        msg += f"\n      {e.lineno}| {lines[e.lineno - 1].rstrip()}"
    return msg


def _check_section(module: Any, sec: str, section_data: Dict[str, Any], problems: List[str]) -> Dict[str, Any]:
    """섹션 검사 → 적용할 (key → 값) 반환. 고칠 곳은 problems 에 추가."""
    plan: Dict[str, Any] = {}
    for k in getattr(section_data, "dups", []):
        problems.append(f'{sec}.{k}: 같은 이름이 두 번 있습니다 — 하나를 지우세요(지금은 뒤의 값만 쓰입니다)')
    for key, new_val in section_data.items():
        # __section__으로 시작하는 키는 JSON 주석용 → 무시
        if key.startswith("__"):
            continue
        where = f"{sec}.{key}"
        if not hasattr(module, key):
            names = [n for n in vars(module) if n.isupper()]
            near = difflib.get_close_matches(key, names, n=1, cutoff=0.75)
            tip = f" (혹시 {near[0]} ?)" if near else ""
            problems.append(f"{where}: 프로그램에 없는 설정 이름입니다 — 이름 오타를 확인하세요{tip}")
            continue
        old_val = getattr(module, key)
        _nested_dups(new_val, where, problems)
        new_val = _plain(new_val)
        # JSON으로 관리 불가능한 타입(Path, bytes, callable)
        if isinstance(old_val, (Path, bytes)) or callable(old_val):
            problems.append(f"{where}: settings.json 으로 바꿀 수 없는 항목입니다 — 이 줄을 지우세요")
            continue
        if not _type_compatible(old_val, new_val):
            problems.append(
                f"{where}: {_kind(new_val)} {_short(new_val)} 가 들어 있습니다"
                f" — {_kind(old_val)}로 쓰세요 (기본값 예: {_short(old_val)})"
            )
            continue
        if isinstance(old_val, dict) and isinstance(new_val, dict):
            new_val = _convert_dict_keys(old_val, new_val)
        plan[key] = new_val
    return plan


# ──────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────

def _strip_jsonc_comments(text: str) -> str:
    """
    // 라인 주석과 /* */ 블록 주석 제거. 단, 문자열 내부의 //(예: https://)는 보존.
    주석 글자는 공백으로 바꾸고 줄바꿈은 남긴다 → JSON 오류의 줄·칸 번호가 원본 파일과 같다.
    """
    out = []
    i = 0
    n = len(text)
    in_str = False
    esc = False
    while i < n:
        c = text[i]
        if in_str:
            out.append(c)
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            i += 1
        else:
            if c == '"':
                in_str = True
                out.append(c)
                i += 1
            elif c == "/" and i + 1 < n and text[i + 1] == "/":
                while i < n and text[i] != "\n":
                    out.append(" ")
                    i += 1
            elif c == "/" and i + 1 < n and text[i + 1] == "*":
                end = text.find("*/", i + 2)
                end = n if end < 0 else end + 2
                out.extend("\n" if ch == "\n" else " " for ch in text[i:end])
                i = end
            else:
                out.append(c)
                i += 1
    return "".join(out)


def check_and_load_settings(path: Optional[Path] = None, *, require_file: bool = True) -> SettingsResult:
    """
    settings.json 을 검사하고, 고칠 곳이 없을 때만 config 모듈들에 적용한다.

    - config_common ← JSON["common"]
    - config_ch1    ← JSON["ch1"]
    - config_ch2    ← JSON["ch2"]

    require_file=False 이면 파일이 없을 때 오류 대신 .py 값으로 진행한다(missing=True).
    """
    json_path = Path(path) if path is not None else _settings_path()
    res = SettingsResult(path=json_path)

    if not json_path.is_file():
        res.missing = True
        if require_file:
            res.problems.append(
                f"설정 파일이 없습니다: {json_path} — 프로그램 폴더의 config 폴더에 settings.json 을 넣으세요"
            )
        else:
            logger.info("[ConfigLoader] %s 없음 → 기존 .py 값 사용", json_path)
        return res

    try:
        raw = json_path.read_text(encoding="utf-8-sig")       # BOM(메모장 저장) 허용
    except UnicodeDecodeError:
        res.problems.append(
            "UTF-8 로 저장된 파일이 아닙니다 — 메모장에서 '다른 이름으로 저장' → 인코딩 'UTF-8' 로 저장하세요"
        )
        return res
    except Exception as e:
        res.problems.append(f"파일을 읽을 수 없습니다({e}) — 파일이 다른 프로그램에 열려 있거나 권한이 없는지 확인하세요")
        return res

    try:
        data = json.loads(_strip_jsonc_comments(raw), object_pairs_hook=_pairs_hook)
    except json.JSONDecodeError as e:
        res.problems.append(_json_problem(e, raw.splitlines()))
        return res

    if not isinstance(data, dict):
        res.problems.append('파일 전체가 { } 로 감싸져 있지 않습니다 — 첫 글자가 { , 마지막 글자가 } 여야 합니다')
        return res

    for k in getattr(data, "dups", []):
        res.problems.append(f'"{k}" 구역이 두 번 있습니다 — 하나로 합치세요')

    mods = _section_modules()
    plans: Dict[str, Dict[str, Any]] = {}
    for sec, sec_data in data.items():
        if sec.startswith("__"):
            continue
        if sec not in mods:
            near = difflib.get_close_matches(sec, _SECTIONS, n=1, cutoff=0.6)
            tip = f" (혹시 {near[0]} ?)" if near else ""
            res.problems.append(
                f'"{sec}": 알 수 없는 구역입니다 — "common", "ch1", "ch2" 중 하나여야 합니다{tip}'
            )
            continue
        if not isinstance(sec_data, dict):
            res.problems.append(f'"{sec}": {{ ... }} 묶음이어야 하는데 {_kind(sec_data)} 입니다 — {{ }} 로 감싸세요')
            continue
        plans[sec] = _check_section(mods[sec], sec, sec_data, res.problems)

    if res.problems:
        return res                                                # 하나라도 있으면 아무것도 적용하지 않음

    for sec, plan in plans.items():
        module = mods[sec]
        for key, val in plan.items():
            setattr(module, key, val)
            res.applied += 1
    res.data = _plain(data)
    logger.info("[ConfigLoader] 설정 로드: %s (%d개 값 적용)", json_path, res.applied)
    return res


def format_problems(res: SettingsResult, limit: Optional[int] = None) -> str:
    """알림창/로그용 목록. limit 이 있으면 앞의 limit 개만 + '외 N건'."""
    items = res.problems if limit is None else res.problems[:limit]
    lines = [f"{i}. {p}" for i, p in enumerate(items, 1)]
    rest = len(res.problems) - len(items)
    if rest > 0:
        lines.append(f"... 외 {rest}건 (자세히 보기)")
    return "\n".join(lines)


def load_settings(path: Optional[Path] = None) -> Dict[str, Any]:
    """
    예전 호출 방식. 고칠 곳이 있으면 아무것도 적용하지 않고 {} 를 반환한다(로그 ERROR).
    파일이 없으면 .py 값으로 진행하고 {} 를 반환한다.
    """
    res = check_and_load_settings(path, require_file=False)
    for p in res.problems:
        logger.error("[ConfigLoader] %s: %s", res.path, p)
    if res.problems:
        logger.error("[ConfigLoader] 고칠 곳 %d건 → settings.json 값을 하나도 적용하지 않음", len(res.problems))
    return res.data

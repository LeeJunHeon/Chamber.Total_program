# lib/user_config.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import fields as dc_fields, is_dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Iterable


# ============================================================
# 저장 경로
#   - 기본: <프로젝트루트>/config/user_config.json
#   - 환경변수: CH12_USER_CONFIG 로 강제 지정 가능
# ============================================================

def _project_root() -> Path:
    # .../CH_1_2_program/lib/user_config.py -> parents[1] == .../CH_1_2_program
    return Path(__file__).resolve().parents[1]


def get_config_path() -> Path:
    env = (os.environ.get("CH12_USER_CONFIG") or "").strip()
    if env:
        return Path(env)
    return _project_root() / "config" / "user_config.json"


# ============================================================
# JSON 가능 타입(현 시점: bytes/함수/클래스 등은 제외)
# ============================================================

_JSONABLE_BASE = (str, int, float, bool, type(None))


def is_jsonable(v: Any) -> bool:
    if isinstance(v, _JSONABLE_BASE):
        return True
    if isinstance(v, (list, tuple)):
        return all(is_jsonable(x) for x in v)
    if isinstance(v, dict):
        return all(isinstance(k, str) and is_jsonable(val) for k, val in v.items())
    return False


def _deep_copy_jsonable(v: Any) -> Any:
    if isinstance(v, dict):
        return {str(k): _deep_copy_jsonable(val) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        return [_deep_copy_jsonable(x) for x in v]
    return v


# ============================================================
# 소스에서 "대문자 변수 할당" 목록만 추출 (config_ch1/ch2의 *import 중복 방지)
# ============================================================

_ASSIGN_RE = re.compile(r'^\s*([A-Z][A-Z0-9_]*)\s*=\s*', re.M)


def _extract_assigned_upper_names(py_text: str) -> Iterable[str]:
    seen = set()
    for m in _ASSIGN_RE.finditer(py_text or ""):
        name = m.group(1)
        if name not in seen:
            seen.add(name)
            yield name


def extract_module_params_defined_in_file(mod: Any) -> Dict[str, Any]:
    """
    mod.__file__의 소스를 기준으로 "그 파일에서 직접 할당한" 대문자 변수만 추출한다.
    (config_ch1/ch2가 config_common을 import * 해도 중복 노출 방지)
    """
    out: Dict[str, Any] = {}

    py_path = getattr(mod, "__file__", None)
    if not py_path:
        return out

    try:
        src = Path(py_path).read_text(encoding="utf-8", errors="replace")
    except Exception:
        return out

    for name in _extract_assigned_upper_names(src):
        try:
            v = getattr(mod, name)
        except Exception:
            continue

        if callable(v):
            continue

        if is_jsonable(v):
            out[name] = _deep_copy_jsonable(v)

    return out


# ============================================================
# Plasma Cleaning PCParams 기본값 추출
# ============================================================

def extract_pcparams_defaults() -> Dict[str, Any]:
    try:
        from controller.plasma_cleaning_controller import PCParams
    except Exception:
        return {}

    if not is_dataclass(PCParams):
        return {}

    out: Dict[str, Any] = {}
    for f in dc_fields(PCParams):
        try:
            dv = f.default
        except Exception:
            continue

        if dv is None or isinstance(dv, _JSONABLE_BASE):
            out[f.name] = dv
        elif is_jsonable(dv):
            out[f.name] = _deep_copy_jsonable(dv)

    return out


# ============================================================
# 기본값 빌드 + 카테고리 분리
# ============================================================

def build_defaults() -> Dict[str, Any]:
    from lib import config_common as cfgc
    from lib import config_ch1 as cfg1
    from lib import config_ch2 as cfg2

    common_all = extract_module_params_defined_in_file(cfgc)

    tsp = {k: v for k, v in common_all.items() if k.startswith("TSP_")}
    communication = {k: v for k, v in common_all.items() if not k.startswith("TSP_")}

    return {
        "__meta__": {
            "version": 2,
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        },
        "communication": communication,
        "tsp": tsp,
        "plasma_cleaning": extract_pcparams_defaults(),
        "ch1": extract_module_params_defined_in_file(cfg1),
        "ch2": extract_module_params_defined_in_file(cfg2),
    }


# ============================================================
# 로드/저장 (파일이 없으면 기본값으로 생성)
# ============================================================

def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def ensure_exists(path: Optional[Path] = None) -> Path:
    p = path or get_config_path()
    if p.exists():
        return p
    defaults = build_defaults()
    _atomic_write_text(p, json.dumps(defaults, ensure_ascii=False, indent=2, sort_keys=True))
    return p


def _migrate_legacy(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    구버전(common/ch1/ch2) 포맷 → 새 포맷(communication/tsp/plasma_cleaning/ch1/ch2)
    """
    if not isinstance(data, dict):
        return {}

    if "common" in data and ("communication" not in data and "tsp" not in data):
        common = data.get("common") if isinstance(data.get("common"), dict) else {}
        tsp = {k: v for k, v in common.items() if isinstance(k, str) and k.startswith("TSP_")}
        communication = {k: v for k, v in common.items() if isinstance(k, str) and not k.startswith("TSP_")}
        data = dict(data)
        data.pop("common", None)
        data["communication"] = communication
        data["tsp"] = tsp

    meta = data.get("__meta__")
    if not isinstance(meta, dict):
        meta = {}
    meta.setdefault("version", 2)
    data["__meta__"] = meta
    return data


def _merge_defaults(defaults: Dict[str, Any], loaded: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(defaults)

    for top in ("communication", "tsp", "plasma_cleaning", "ch1", "ch2"):
        d0 = defaults.get(top, {}) if isinstance(defaults.get(top), dict) else {}
        d1 = loaded.get(top, {}) if isinstance(loaded.get(top), dict) else {}
        merged = dict(d0)
        merged.update(d1)  # 파일 값 우선
        out[top] = merged

    meta0 = defaults.get("__meta__", {}) if isinstance(defaults.get("__meta__"), dict) else {}
    meta1 = loaded.get("__meta__", {}) if isinstance(loaded.get("__meta__"), dict) else {}
    out["__meta__"] = {**meta0, **meta1}
    return out


def load(path: Optional[Path] = None) -> Dict[str, Any]:
    p = ensure_exists(path)
    defaults = build_defaults()

    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return defaults
        raw = _migrate_legacy(raw)
        return _merge_defaults(defaults, raw)
    except Exception:
        return defaults


def save(data: Dict[str, Any], path: Optional[Path] = None) -> Tuple[bool, Path, str]:
    p = path or get_config_path()

    if not isinstance(data, dict):
        return False, p, "data is not dict"

    meta = data.get("__meta__", {})
    if not isinstance(meta, dict):
        meta = {}
    meta["saved_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    meta.setdefault("version", 2)
    data["__meta__"] = meta

    try:
        _atomic_write_text(p, json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True))
        return True, p, "saved"
    except Exception as e:
        return False, p, f"save failed: {e!r}"


# ============================================================
# 런타임 적용
# ============================================================

def _set_if_exists(mod: Any, key: str, val: Any) -> bool:
    if not hasattr(mod, key):
        return False
    try:
        setattr(mod, key, val)
        return True
    except Exception:
        return False


def _defined_keys_in_module_file(mod: Any) -> set[str]:
    py_path = getattr(mod, "__file__", None)
    if not py_path:
        return set()
    try:
        src = Path(py_path).read_text(encoding="utf-8", errors="replace")
    except Exception:
        return set()
    return set(_extract_assigned_upper_names(src))


def apply_overrides(cfg: Dict[str, Any]) -> Dict[str, Tuple[int, int]]:
    """
    cfg(load() 결과)를 config 모듈 변수에 반영한다.
    """
    from lib import config_common as cfgc
    from lib import config_ch1 as cfg1
    from lib import config_ch2 as cfg2

    res: Dict[str, Tuple[int, int]] = {}

    comm = cfg.get("communication", {}) if isinstance(cfg.get("communication"), dict) else {}
    tsp = cfg.get("tsp", {}) if isinstance(cfg.get("tsp"), dict) else {}
    common_all = dict(comm)
    common_all.update(tsp)

    # 1) config_common 갱신
    applied = skipped = 0
    for k, v in common_all.items():
        if not isinstance(k, str) or not k.isupper():
            skipped += 1
            continue
        if _set_if_exists(cfgc, k, v):
            applied += 1
        else:
            skipped += 1
    res["communication+tsp->config_common"] = (applied, skipped)

    # 2) config_ch1/ch2: common 값 동기화 (단, 채널 파일에서 직접 정의한 키는 유지)
    ch1_overrides = _defined_keys_in_module_file(cfg1)
    ch2_overrides = _defined_keys_in_module_file(cfg2)

    applied1 = skipped1 = 0
    for k, v in common_all.items():
        if k in ch1_overrides:
            continue
        if _set_if_exists(cfg1, k, v):
            applied1 += 1
        else:
            skipped1 += 1
    res["sync_common->ch1"] = (applied1, skipped1)

    applied2 = skipped2 = 0
    for k, v in common_all.items():
        if k in ch2_overrides:
            continue
        if _set_if_exists(cfg2, k, v):
            applied2 += 1
        else:
            skipped2 += 1
    res["sync_common->ch2"] = (applied2, skipped2)

    # 3) 채널 오버라이드 적용
    def _apply_group(mod: Any, group_name: str) -> Tuple[int, int]:
        d = cfg.get(group_name, {})
        if not isinstance(d, dict):
            return (0, 0)
        a = s = 0
        for k, v in d.items():
            if not isinstance(k, str) or not k.isupper():
                s += 1
                continue
            if _set_if_exists(mod, k, v):
                a += 1
            else:
                s += 1
        return (a, s)

    res["apply_ch1"] = _apply_group(cfg1, "ch1")
    res["apply_ch2"] = _apply_group(cfg2, "ch2")

    return res


# ============================================================
# 툴팁(설명) 생성: 소스 주석 기반
# ============================================================

def _parse_tooltips_from_pyfile(py_path: Path) -> Dict[str, str]:
    try:
        text = py_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return {}

    tips: Dict[str, str] = {}
    current_section = ""

    for line in text.splitlines():
        raw = line.rstrip("\n")
        s = raw.strip()

        if s.startswith("#"):
            c = s.lstrip("#").strip()
            if c and not set(c) <= {"="}:
                if len(c) <= 80:
                    current_section = c
            continue

        m = re.match(r'^\s*([A-Z][A-Z0-9_]*)\s*=\s*.*?(?:#\s*(.*))?$', raw)
        if not m:
            continue
        name = m.group(1)
        trail = (m.group(2) or "").strip()

        parts = []
        if current_section:
            parts.append(current_section)
        if trail:
            parts.append(trail)

        tip = " / ".join([p for p in parts if p])
        if tip:
            tips[name] = tip

    return tips


def _parse_pcparams_tooltips() -> Dict[str, str]:
    try:
        import controller.plasma_cleaning_controller as pcmod
        py = Path(pcmod.__file__)
        text = py.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return {}

    tips: Dict[str, str] = {}
    in_pcparams = False
    for line in text.splitlines():
        if "class PCParams" in line:
            in_pcparams = True
            continue
        if in_pcparams and line.startswith("class "):
            break
        if not in_pcparams:
            continue

        m = re.match(r'^\s*([a-zA-Z_]\w*)\s*:\s*[^=]+=\s*.*?(?:#\s*(.*))?$', line)
        if not m:
            continue
        name = m.group(1)
        comm = (m.group(2) or "").strip()
        if comm:
            tips[name] = comm
    return tips


def build_tooltips() -> Dict[str, Dict[str, str]]:
    from lib import config_common as cfgc
    from lib import config_ch1 as cfg1
    from lib import config_ch2 as cfg2

    tips_common = _parse_tooltips_from_pyfile(Path(cfgc.__file__))
    tips_ch1 = _parse_tooltips_from_pyfile(Path(cfg1.__file__))
    tips_ch2 = _parse_tooltips_from_pyfile(Path(cfg2.__file__))

    tsp = {k: v for k, v in tips_common.items() if k.startswith("TSP_")}
    communication = {k: v for k, v in tips_common.items() if not k.startswith("TSP_")}

    return {
        "communication": communication,
        "tsp": tsp,
        "plasma_cleaning": _parse_pcparams_tooltips(),
        "ch1": tips_ch1,
        "ch2": tips_ch2,
    }
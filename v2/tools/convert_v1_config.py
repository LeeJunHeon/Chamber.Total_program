"""v1 설정(config/settings.json + 두께 json) → v2 설정(챔버K ConfigStore 형식 config_active.json + 두께 데이터 파일).

실행: cd <저장소 루트> && python3 -m v2.tools.convert_v1_config --out <v2 설정 폴더> [--v1-root <v1 폴더>] [--allow-pending]
      (v1 폴더 기본 = 이 저장소 루트 — config/settings.json·config/thickness_*.json 을 읽는다. 읽기만 한다.)

읽기 — v1 lib/_config_loader.py 와 같게
- utf-8-sig(메모장 BOM 허용), // 줄 주석·/* */ 블록 주석 제거(문자열 안 // 는 보존, _strip_jsonc_comments 와 같은 규칙).
- 최상위 { } 아님·같은 구역/키 두 번(값 안쪽 묶음 포함)·알 수 없는 구역(common/ch1/ch2 외)·구역이 { } 아님 → 오류.
- "__" 로 시작하는 구역·키는 주석용(v1 도 무시) — '버림'으로 적는다.
- 형식: v1 _type_compatible 그대로(bool 은 bool 끼리 — true ↔ 1 금지, int↔float 호환, list↔tuple 호환, null 은 기존 값이
  None 일 때만). 기존 값의 형식은 v2/ch12/spec/settings_v1facts.py KEYS(형식 칸)에서. Path·bytes 항목은 '바꿀 수 없는 항목'.

키마다 결과(셋 중 하나 — 조용히 버리지 않는다)
- 변환됨   : 그 v1 키를 옮기는 v2 파라미터가 있다(이름 규칙 common.X → X, chN.X → CHN_X, 또는 ParamDef.v1_key).
             값 = v1 이 실제로 쓰던 값: inv_cfg ⚠ 'common 값은 효과 없음' 키는 .py 기본값, '이 chN 값은 읽히지 않음' 키는
             common 값 — json 과 다르면 '사용자 확인 필요'에 적는다. 그다음 v2 형식·범위 검사(정수 키에 소수 → 오류,
             v1 은 클램프해 쓰던 범위 밖 값 → 오류). 목록·묶음 값은 v2 STR 키에 JSON 글자로.
             채널 구역 값은 CH1_/CH2_ 키로만 — 공통 값을 채널 키에 복사하지 않는다(v1 'import 시 복사' 문제를 구조로 없앰).
- 버림     : 조각의 DROPPED 에 있는 v1 키(근거 출력). v1 이 쓰던 키인데 값이 v1 기본과 다르면 옮길 곳이 없으므로 오류.
- 오류     : v1 에 선언되지 않은 이름(v1 로더도 거부), 형식 불일치, v2 범위 밖, 담당 조각이 정해졌지만 아직 정의 안 됨
             (--allow-pending 이면 '보류'로 적고 계속 — 장비 조각을 만드는 중일 때만), 담당도 없는 v1 이름.
  오류가 하나라도 있으면 아무 파일도 쓰지 않는다(v1 로더의 '전부 아니면 전무'와 같은 원칙).

두께 json — v2 데이터 파일로 정함(파라미터가 아님)
- 근거: Factor 는 척(up·mid·down) × 건(G1~G3) 표 + 건별 재료 이름, 재료 목록은 99 개 레코드 — ParamDef 는 스칼라만 담고,
  v1 은 화면 [Factor 설정]·[재료] 창에서 이 파일들을 원자적으로 저장했다(lib/thickness_factors.py:118·thickness_materials.py:122).
  같은 형식(version 2 / version 1)으로 v2 설정 폴더에 둔다 — thickness_factors_ch{N}.json·thickness_materials.json.
- v1 읽기 규칙 그대로(lib/thickness_factors.load_factors·thickness_materials.load_materials): 잘못된 칸·항목은 v1 도 미설정·제외로
  읽던 것 — '버림'에 적고 정규화한 파일을 쓴다. 파일을 읽을 수 없으면 오류.
- CHn_THICKNESS_FACTOR_FILE 은 "" (= 설정 폴더의 thickness_factors_ch{N}.json). v1 값이 다른 경로를 가리키면 그 파일을 옮기고
  '사용자 확인 필요'에 적는다.

쓰기
- v2/common/config_store.py ConfigStore(같은 정의·규칙·스키마 해시)로 버전 1 을 적용 — 전체 검사(형식·범위·키 사이 규칙) 통과해야 한다.
  출력 폴더에 이미 config_active.json 이 있으면 멈춘다(--force 가 아니면).
- 변환 기록 convert_report.json(키마다 결과·근거) — 화면 출력과 같은 내용.
비밀값(웹 토큰·챗 웹훅 — v1 lib/config_local.py)은 settings.json 에 없다. 노트북의 local_override.json 에 손으로 넣는다(커밋 금지).
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

from v2.ch12.params import ownership as own
from v2.ch12.params import table as params_table
from v2.ch12.spec import settings_v1facts as facts
from v2.common.config_store import ConfigStore
from v2.common.paramdef import PType, check_value

ROOT = Path(__file__).resolve().parents[2]
REPORT = "convert_report.json"

# ───────────── v1 읽기(lib/_config_loader.py 와 같게) ─────────────


def strip_jsonc_comments(text: str) -> str:
    """// 줄 주석과 /* */ 블록 주석을 공백으로(줄바꿈은 남김) — 문자열 안의 // 는 보존. v1 _strip_jsonc_comments 와 같다."""
    out, i, n, in_str, esc = [], 0, len(text), False, False
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
        elif c == '"':
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


class _Pairs(dict):
    dups: list


def _pairs_hook(pairs):
    d = _Pairs()
    d.dups = []
    for k, v in pairs:
        if k in d and k not in d.dups:
            d.dups.append(k)
        d[k] = v
    return d


def _nested_dups(v: Any, where: str, out: list) -> None:
    if isinstance(v, dict):
        for k in getattr(v, "dups", []):
            out.append(f'{where} 안의 "{k}": 같은 이름이 두 번 있다(v1 로더도 거부)')
        for k, x in v.items():
            _nested_dups(x, f"{where}.{k}", out)
    elif isinstance(v, list):
        for x in v:
            _nested_dups(x, where, out)


def _plain(v: Any) -> Any:
    if isinstance(v, dict):
        return {k: _plain(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_plain(x) for x in v]
    return v


def read_json_v1(path: Path) -> Any:
    """v1 과 같은 방법으로 읽는다(utf-8-sig·JSONC·중복 키 기억). 실패하면 ValueError(사람이 읽는 문구)."""
    try:
        raw = path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError as e:
        raise ValueError(f"{path}: UTF-8 로 저장된 파일이 아니다") from e
    try:
        return json.loads(strip_jsonc_comments(raw), object_pairs_hook=_pairs_hook)
    except json.JSONDecodeError as e:
        raise ValueError(f"{path}: 줄 {e.lineno} 칸 {e.colno}: JSON 문법 오류({e.msg})") from e


# ───────────── v1 사실(형식·기본값·실효 출처) ─────────────
_FACTS = {k[0]: k for k in facts.KEYS}


def _fact(path: str):
    """그 경로의 사실 행. 없으면 같은 이름의 다른 구역 행(채널 모듈은 config_common 을 복사하므로 형식·기본값이 같다)."""
    if path in _FACTS:
        return _FACTS[path]
    sec, name, sub = own.split_path(path)
    tail = name if sub is None else f"{name}.{sub}"
    for s in ("common", own.CODE, "ch2", "ch1"):
        if f"{s}.{tail}" in _FACTS:
            return _FACTS[f"{s}.{tail}"]
    return None


def _kind_of(fmt: str) -> str:
    if fmt == "true/false":
        return "bool"
    if fmt.startswith("숫자"):
        return "number"
    if fmt == "글자":
        return "str"
    if fmt == "목록":
        return "list"
    if fmt == "묶음":
        return "dict"
    return "fixed"            # Path·bytes — settings.json 으로 바꿀 수 없음


def _v1_compatible(kind: str, v: Any) -> bool:
    """v1 _type_compatible(기존 값 종류, 새 값)."""
    if isinstance(v, bool) or kind == "bool":
        return kind == "bool" and isinstance(v, bool)
    if kind == "number":
        return isinstance(v, (int, float))
    if kind == "str":
        return isinstance(v, str)
    if kind == "list":
        return isinstance(v, (list, tuple))
    if kind == "dict":
        return isinstance(v, dict)
    return False


def _same(a: Any, b: Any) -> bool:
    """v1 값 비교 — bool 엄격, 숫자 1 == 1.0, list == tuple, 묶음 키는 글자로."""
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return len(a) == len(b) and all(_same(x, y) for x, y in zip(a, b))
    if isinstance(a, dict) and isinstance(b, dict):
        aa, bb = {str(k): v for k, v in a.items()}, {str(k): v for k, v in b.items()}
        return aa.keys() == bb.keys() and all(_same(aa[k], bb[k]) for k in aa)
    return type(a) is type(b) and a == b


def _effect(path: str) -> str:
    """'used' / 'dead_common'(⚠ common 값 효과 없음) / 'dead_channel'(⚠ 이 채널 값 안 읽힘) / 'unread'(읽는 곳 없음)."""
    f = _FACTS.get(path)
    if f is None:
        return "used"
    m = f[5]
    if "⚠ common 값은 효과 없음" in m:
        return "dead_common"
    if "값은 읽히지 않음" in m:
        return "dead_channel"
    if "읽는 곳 없음" in m:
        return "unread"
    return "used"


# ───────────── 결과 ─────────────
@dataclass
class Result:
    values: dict = field(default_factory=dict)        # v2 키 → 변환된 값(v1 에서 온 것만)
    converted: list = field(default_factory=list)     # {path, key, v1, v2, note}
    dropped: list = field(default_factory=list)       # {path, reason, v1}
    pending: list = field(default_factory=list)       # {path, owner}
    errors: list = field(default_factory=list)
    confirm: list = field(default_factory=list)       # 사용자 확인 필요
    files: dict = field(default_factory=dict)         # 출력 데이터 파일 이름 → 내용(dict)
    store_status: str = ""

    @property
    def ok(self) -> bool:
        return not self.errors

    def summary(self) -> dict:
        return {"converted": len(self.converted), "dropped": len(self.dropped), "pending": len(self.pending),
                "errors": len(self.errors), "confirm": len(self.confirm), "files": sorted(self.files)}


class Converter:
    def __init__(self, table=None):
        self.t = table or params_table()
        self.by_key = self.t.by_key()
        self.param_of: dict = {}           # v1 경로 → ParamDef
        for p in self.t.params:
            for ref in own.v1_refs(p.key, p.v1_key):
                self.param_of.setdefault(ref, p)
        for path in _FACTS:                # 이름 규칙(묶음 하위 키 "구역.X.k" → X_K 처럼 거꾸로 풀 수 없는 것 포함)
            k = own.v2_key_for(path)
            if k in self.by_key:
                self.param_of.setdefault(path, self.by_key[k])
        self.dropped_of: dict = {}
        for d in self.t.dropped:
            for ref in own.v1_refs(d.key, d.key):
                self.dropped_of.setdefault(ref, d)

    # ── 한 값 ──
    def _to_v2(self, p, v: Any) -> tuple:
        """(v2 값, 오류 문구|None)."""
        if p.type is PType.STR and isinstance(v, (list, tuple, dict)):
            v = json.dumps(list(v) if isinstance(v, tuple) else v, ensure_ascii=False)
        elif p.type is PType.INT and isinstance(v, float) and not isinstance(v, bool):
            if not (math.isfinite(v) and v == int(v)):
                return None, f"{p.key}: 정수 키에 소수 {v!r} (v1 은 받았지만 v2 는 정수만)"
            v = int(v)
        elif p.type is PType.FLOAT and isinstance(v, int) and not isinstance(v, bool):
            v = float(v)
        err = check_value(p, v)
        return v, err

    def _entry(self, res: Result, path: str, v: Any, data: dict) -> None:
        sec, name, sub = own.split_path(path)
        f = _fact(path)
        kind = _kind_of(f[3]) if f is not None else None
        if kind == "fixed":
            res.errors.append(f"{path}: settings.json 으로 바꿀 수 없는 항목(v1 로더도 거부 — Path·bytes)")
            return
        if kind is not None and not _v1_compatible(kind, v):
            res.errors.append(f"{path}: 형식 불일치 {json.dumps(v, ensure_ascii=False)[:60]} — v1 형식 {f[3]} (v1 로더도 거부)")
            return
        subs = [f"{path}.{k}" for k in v] if kind == "dict" and sub is None else []
        whole = path in self.param_of
        if subs and not whole and (path not in self.dropped_of
                                   or any(sp in self.param_of or sp in self.dropped_of for sp in subs)):
            if path in self.dropped_of:                   # 묶음 자체는 버리고 하위 키로 옮기는 경우(예: MFC_SCALE_FACTORS)
                d = self.dropped_of[path]
                res.dropped.append({"path": path, "reason": d.reason + " (하위 키는 따로 처리)", "v1": v,
                                    "replaced_by": d.replaced_by})
            for k, x in v.items():                        # 묶음은 하위 키로(예: MFC_SCALE_FACTORS.1)
                sp = f"{path}.{k}"
                if _fact(sp) is None and sp not in self.param_of and sp not in self.dropped_of:
                    res.errors.append(f"{sp}: 모르는 묶음 하위 키")
                else:
                    self._entry(res, sp, x, data)
            return
        if path in self.dropped_of:
            d = self.dropped_of[path]
            eff = _effect(path)
            if eff == "used" and f is not None and not _same(v, f[2]):
                res.errors.append(f"{path}: v2 에서 뺀 키({d.reason})인데 값 {v!r} 이 v1 기본 {f[2]!r} 과 달라 옮길 곳이 없다")
                return
            res.dropped.append({"path": path, "reason": d.reason, "v1": v, "replaced_by": d.replaced_by})
            return
        if path in self.param_of:
            p = self.param_of[path]
            eff, note, val = _effect(path), "", v
            if eff == "dead_common" and f is not None:
                val = f[2]
                if not _same(v, val):
                    res.confirm.append(f"{path}: v1 에서 효과 없던 common 값 {v!r} — v1 이 실제로 쓰던 .py 값 {val!r} 로 옮김")
                note = "v1 효과 없던 common 값 — v1 실효값(.py)"
            elif eff == "dead_channel":
                cpath = f"common.{name}"
                val = data.get("common", {}).get(name, _FACTS[cpath][2] if cpath in _FACTS else v)
                if not _same(v, val):
                    res.confirm.append(f"{path}: v1 에서 읽히지 않던 채널 값 {v!r} — v1 이 실제로 쓰던 common 값 {val!r} 로 옮김")
                note = "v1 에서 읽히지 않던 채널 값 — v1 실효값(common)"
            if name == "THICKNESS_FACTOR_FILE":
                val, note = "", "설정 폴더의 thickness_factors_chN.json(두께 파일 변환 참고)"
            out, err = self._to_v2(p, val)
            if err:
                res.errors.append(f"{path} → {err}")
                return
            if p.key in res.values:
                res.errors.append(f"{path}: v2 키 {p.key} 를 다른 v1 키도 채운다")
                return
            res.values[p.key] = out
            res.converted.append({"path": path, "key": p.key, "v1": v, "v2": out, "note": note})
            return
        o = own.owner_of(path)
        if o is not None:
            res.pending.append({"path": path, "owner": o})
            return
        res.errors.append(f"{path}: v1 에 선언된 이름이지만 v2 에 대응 키도 버림 근거도 없다(담당 조각 없음)")

    # ── settings.json 전체 ──
    def convert_settings(self, data: Any, res: Result) -> None:
        if not isinstance(data, dict):
            res.errors.append("파일 전체가 { } 로 감싸져 있지 않다")
            return
        for k in getattr(data, "dups", []):
            res.errors.append(f'"{k}" 구역이 두 번 있다(v1 로더도 거부)')
        for sec, sd in data.items():
            if sec.startswith("__"):
                res.dropped.append({"path": sec, "reason": "주석용 구역(v1 도 무시)", "v1": _plain(sd), "replaced_by": None})
                continue
            if sec not in own.SECTIONS:
                res.errors.append(f'"{sec}": 알 수 없는 구역 — common·ch1·ch2 중 하나(v1 로더도 거부)')
                continue
            if not isinstance(sd, dict):
                res.errors.append(f'"{sec}": {{ }} 묶음이어야 한다(v1 로더도 거부)')
                continue
            for k in getattr(sd, "dups", []):
                res.errors.append(f"{sec}.{k}: 같은 이름이 두 번 있다(v1 로더도 거부)")
            for key, v in sd.items():
                path = f"{sec}.{key}"
                if key.startswith("__"):
                    res.dropped.append({"path": path, "reason": "주석용 키(v1 도 무시)", "v1": _plain(v), "replaced_by": None})
                    continue
                if not own.is_declared(sec, key):
                    res.errors.append(f"{path}: 모르는 키 — v1 에도 없는 이름(v1 로더도 거부, 오타 확인)")
                    continue
                dups: list = []
                _nested_dups(v, path, dups)
                if dups:
                    res.errors.extend(dups)
                    continue
                self._entry(res, path, _plain(v), data)

    # ── 두께 json ──
    def convert_thickness(self, data: dict, v1_root: Path, res: Result) -> None:
        for n in (1, 2):
            sec = data.get(f"ch{n}", {}) if isinstance(data, dict) else {}
            raw = sec.get("THICKNESS_FACTOR_FILE", "") if isinstance(sec, dict) else ""
            default = v1_root / "config" / f"thickness_factors_ch{n}.json"
            src = default if not str(raw).strip() else (Path(raw) if Path(raw).is_absolute() else v1_root / raw)
            if src.resolve() != default.resolve():
                res.confirm.append(f"ch{n}.THICKNESS_FACTOR_FILE: v1 이 {src} 를 썼다 — v2 설정 폴더 thickness_factors_ch{n}.json 으로 옮김")
            if not src.exists():
                res.dropped.append({"path": f"file:{src.name}", "reason": f"CH{n} Factor 파일 없음 — v1 도 빈 표(v2 도 빈 표)",
                                    "v1": None, "replaced_by": None})
                continue
            try:
                fac, warns = load_factors_v1(src)
            except ValueError as e:
                res.errors.append(str(e))
                continue
            for w in warns:
                res.dropped.append({"path": f"file:{src.name}", "reason": f"v1 도 미설정으로 읽던 값: {w}", "v1": None,
                                    "replaced_by": None})
            res.files[f"thickness_factors_ch{n}.json"] = fac
        msrc = v1_root / "config" / "thickness_materials.json"
        if not msrc.exists():
            res.confirm.append("thickness_materials.json 없음 — v1 은 처음 읽을 때 STM-100 기본 목록(99 종)을 만들었다. "
                               "v2 두께 모듈이 같은 기본 목록을 만든다")
            return
        try:
            mats, warns = load_materials_v1(msrc)
        except ValueError as e:
            res.errors.append(str(e))
            return
        for w in warns:
            res.dropped.append({"path": "file:thickness_materials.json", "reason": f"v1 도 제외하던 항목: {w}", "v1": None,
                                "replaced_by": None})
        res.files["thickness_materials.json"] = {"version": 1, "materials": mats}


# ───────────── 두께 파일(v1 lib/thickness_factors.py·thickness_materials.py 와 같은 규칙) ─────────────
CHUCKS, GUNS = ("up", "mid", "down"), ("G1", "G2", "G3")
TOOLING_MIN, TOOLING_MAX = 10.0, 399.0          # device/thickness_monitor.py:53
DENSITY_MIN, DENSITY_MAX = 0.500, 99.99          # device/thickness_monitor.py:51
Z_MIN, Z_MAX = 0.100, 9.999                      # device/thickness_monitor.py:52


def _num(v, lo, hi):
    """(값, 오류) — bool·빈값·숫자 아님·NaN·범위 밖은 오류."""
    if isinstance(v, bool) or v is None or (isinstance(v, str) and not v.strip()):
        return None, f"{v!r} 숫자가 아님"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None, f"{v!r} 숫자가 아님"
    if f != f or not (lo <= f <= hi):
        return None, f"{f:g} 범위({lo:g}~{hi:g}) 밖"
    return f, ""


def load_factors_v1(path: Path) -> tuple:
    """(v1 to_json 모양 dict, 경고). 못 읽음·{ } 아님 → ValueError(v1 은 빈 표로 시작 — 변환은 멈춘다)."""
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"{path}: Factor 파일을 읽을 수 없음 — {e!r}") from e
    if not isinstance(data, dict):
        raise ValueError(f"{path}: Factor 파일 형식 오류({{ }} 아님)")
    warns = []
    tooling = {c: {g: None for g in GUNS} for c in CHUCKS}
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
            v = row.get(g)
            if v is None or (isinstance(v, str) and not v.strip()):
                continue
            f, err = _num(v, TOOLING_MIN, TOOLING_MAX)
            if err:
                warns.append(f"tooling[{c}][{g}] {err} — 미설정")
            tooling[c][g] = f
    material = {g: None for g in GUNS}
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
            warns.append(f"material.{g}: 이전 형식(density·Z 숫자)은 쓰지 않음 — 재료를 다시 고른다")
        name = m.get("material")
        material[g] = name.strip() if isinstance(name, str) and name.strip() else None
    out = {"version": 2, "tooling": tooling, "material": {g: {"material": material[g] or ""} for g in GUNS}}
    return out, warns


def load_materials_v1(path: Path) -> tuple:
    """([재료 dict], 경고). 못 읽음·materials 목록 없음 → ValueError."""
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
        items = data.get("materials") if isinstance(data, dict) else None
        if not isinstance(items, list):
            raise ValueError("materials 목록이 없음")
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"{path}: 재료 목록 파일을 읽을 수 없음 — {e!r}") from e
    out, warns, seen = [], [], set()
    for i, it in enumerate(items, 1):
        if not isinstance(it, dict):
            warns.append(f"{i}번째 항목이 표(객체)가 아님 — 제외")
            continue
        nm = str(it.get("name") or "").strip()
        errs = [] if nm else ["이름이 비어 있음"]
        dens, e1 = _num(it.get("density"), DENSITY_MIN, DENSITY_MAX)
        z, e2 = _num(it.get("z_factor"), Z_MIN, Z_MAX)
        errs += [f"density {e1}"] if e1 else []
        errs += [f"z_factor {e2}"] if e2 else []
        if errs:
            warns.append(f"{i}번째 항목 '{it.get('name')}': {', '.join(errs)} — 제외")
            continue
        if nm.casefold() in seen:
            warns.append(f"{i}번째 항목 '{nm}': 이름 중복 — 제외")
            continue
        seen.add(nm.casefold())
        d = {"name": nm, "symbol": str(it.get("symbol") or "").strip(), "density": dens, "z_factor": z}
        note = str(it.get("note") or "").strip()
        if note:
            d["note"] = note
        out.append(d)
    return out, warns


# ───────────── 전체 ─────────────
def convert(settings_path: Path, v1_root: Path, *, allow_pending: bool = False, table=None) -> Result:
    res = Result()
    conv = Converter(table)
    try:
        data = read_json_v1(Path(settings_path))
    except (OSError, ValueError) as e:
        res.errors.append(str(e))
        return res
    conv.convert_settings(data, res)
    conv.convert_thickness(data if isinstance(data, dict) else {}, Path(v1_root), res)
    if res.pending and not allow_pending:
        res.errors.extend(f"{x['path']}: 담당 조각 {x['owner']} 에 아직 정의 없음(보류 — --allow-pending)" for x in res.pending)
    return res


def write(res: Result, out_dir: Path, *, table=None, force: bool = False) -> Result:
    """오류가 없을 때만 쓴다. ConfigStore 로 버전 1 적용(전체 검사) + 두께 데이터 파일 + 변환 기록."""
    if not res.ok:
        return res
    t = table or params_table()
    out_dir = Path(out_dir)
    store = ConfigStore(out_dir, t.params, t.rules, t.schema_hash())
    if (store.active_path.exists() or store.prev_path.exists()) and not force:
        res.errors.append(f"{out_dir}: 이미 v2 설정이 있다(config_active.json) — 덮어쓰려면 --force")
        return res
    if force:
        for pth in (store.active_path, store.prev_path):
            if pth.exists():
                pth.unlink()
    store.load()
    values = dict(store.stored())
    values.update({k: v for k, v in res.values.items() if k not in store.secret_keys})
    ar = store.apply(1, values, t.schema_hash(), busy=False, by="convert_v1_config")
    res.store_status = ar.status
    if ar.status == "rejected":
        res.errors.append(f"v2 설정 검사 실패({ar.reason}): {ar.errors[:10]}")
        return res
    for name, obj in res.files.items():
        tmp = out_dir / (name + ".tmp")
        tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
        tmp.replace(out_dir / name)
    (out_dir / REPORT).write_text(json.dumps(asdict(res) | {"summary": res.summary()}, ensure_ascii=False, indent=1,
                                             default=str) + "\n", encoding="utf-8", newline="\n")
    return res


def _print(res: Result, out=sys.stdout) -> None:
    s = res.summary()
    print(f"변환 {s['converted']} · 버림 {s['dropped']} · 보류 {s['pending']} · 오류 {s['errors']} · "
          f"사용자 확인 {s['confirm']} · 데이터 파일 {s['files']} · 저장 {res.store_status or '-'}", file=out)
    for d in res.dropped:
        print(f"  버림 {d['path']}: {d['reason']}", file=out)
    for x in res.pending:
        print(f"  보류 {x['path']} → 조각 {x['owner']}", file=out)
    for c in res.confirm:
        print(f"  확인 {c}", file=out)
    for e in res.errors:
        print(f"  오류 {e}", file=out)


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(description="v1 config/settings.json(+두께 json) → v2 설정 폴더")
    ap.add_argument("--v1-root", default=str(ROOT), help="v1 프로그램 폴더(config/ 가 있는 곳)")
    ap.add_argument("--settings", default=None, help="settings.json 경로(기본 <v1-root>/config/settings.json)")
    ap.add_argument("--out", required=True, help="v2 설정 폴더(config_active.json 을 쓸 곳)")
    ap.add_argument("--allow-pending", action="store_true", help="담당 조각이 아직 정의 안 한 키를 보류로 두고 계속")
    ap.add_argument("--force", action="store_true", help="출력 폴더의 기존 v2 설정을 덮어쓴다")
    a = ap.parse_args(argv)
    v1_root = Path(a.v1_root)
    res = convert(Path(a.settings) if a.settings else v1_root / "config" / "settings.json", v1_root,
                  allow_pending=a.allow_pending)
    if res.ok:
        write(res, Path(a.out), force=a.force)
    _print(res)
    return 0 if res.ok else 1


if __name__ == "__main__":
    sys.exit(main())

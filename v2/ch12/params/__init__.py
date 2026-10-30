"""챔버12 파라미터 표 전체 — 조각(params/<이름>.py 의 PARAMS·CROSS_RULES·DROPPED)을 모아 하나로 내놓는다.

조각
- 장비마다 하나(plc·dc·rf·mfc·ig·dcpulse·rfpulse·tsp·stm100·camera·oes·rga — 그 장비 드라이버 작성자가 만든다)와
  core·core_heater·core_thickness(장비 밖 v1 설정 + v2 새 키). 어떤 v1 키가 어느 조각인지는 ownership.py.
- 이 폴더의 모든 모듈 중 '_' 로 시작하지 않고 ownership 이 아닌 것을 조각으로 본다(새 조각은 파일만 두면 들어온다).

모으기(collect) — 조용히 넘어가지 않는다
- 조각 import 가 실패하면(문법 오류·없는 이름 등) ParamsError 로 멈춘다 — 조각 하나를 빼고 나머지로 도는 일은 없다.
- PARAMS 가 없거나 ParamDef 가 아닌 항목이 있으면 ParamsError.
- 키 중복(조각 안·조각 사이), 두 키가 같은 v1 키(경로 — ownership.v1_refs)를 옮김, 키 사이 규칙 id 중복, DROPPED 중복, 같은 키가 PARAMS 와 DROPPED 에 함께,
  같은 v1 키(경로)를 한 조각은 옮기고 다른 조각은 버림 → ParamsError.

쓰는 법
    from v2.ch12.params import PARAMS, CROSS_RULES, DROPPED      # 처음 읽을 때 모은다(지연 — PEP 562 __getattr__)
    from v2.ch12.params import table; t = table(); t.origin["DCP_GAP_MS"]  → "dcpulse"
조각 하나만 import(예: 드라이버 시험의 from v2.ch12.params import dc)는 다른 조각을 import 하지 않는다 — 남의 조각이 깨져도
내 드라이버 시험은 돈다. 전체 표를 쓰는 곳(설정 저장소·웹 묶음·변환 도구·test_params)은 여기서 크게 실패한다.

등급 규칙(챔버K v2/chk/params.py 와 같음 — 시험 v2/tests/test_params.py)
- H 등급은 admin 전용이고 공정 중 변경 금지(apply 가 live 가 아님).
- install/secret 은 재시작 필요(apply=restart)·admin.
- 모든 default 는 자기 범위와 키 사이 규칙(CROSS_RULES)을 통과한다.
"""
from __future__ import annotations

import importlib
import pkgutil
import threading
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Iterable, Optional, Sequence

from v2.common.paramdef import CrossRule, Dropped, ParamDef, schema_hash as _schema_hash

from .ownership import EXPECTED_FRAGMENTS, v1_refs

_NOT_FRAGMENTS = {"ownership"}
_TABLE_NAMES = ("PARAMS", "CROSS_RULES", "DROPPED", "ORIGIN")


class ParamsError(RuntimeError):
    """파라미터 표를 만들 수 없음 — 시작 거부(조각 import 실패·키 중복 등)."""


@dataclass(frozen=True)
class Table:
    params: tuple
    rules: tuple
    dropped: tuple
    origin: dict = field(default_factory=dict)        # 키·규칙 id·버린 키 → 조각 이름
    fragments: tuple = ()                             # 모은 조각 이름(순서대로)

    def by_key(self) -> dict:
        return {p.key: p for p in self.params}

    def schema_hash(self) -> str:
        return _schema_hash(self.params, self.rules)


def fragment_names() -> list:
    """이 폴더의 조각 모듈 이름 — EXPECTED_FRAGMENTS 순서 먼저, 나머지는 이름순."""
    here = Path(__file__).resolve().parent
    found = {m.name for m in pkgutil.iter_modules([str(here)])
             if not m.name.startswith("_") and m.name not in _NOT_FRAGMENTS}
    ordered = [n for n in EXPECTED_FRAGMENTS if n in found]
    return ordered + sorted(found - set(ordered))


def missing_fragments() -> list:
    """아직 파일이 없는 조각(장비 드라이버 작성 중) — 시험이 출력만 한다."""
    have = set(fragment_names())
    return [n for n in EXPECTED_FRAGMENTS if n not in have]


def load_fragments(names: Optional[Iterable[str]] = None) -> list:
    """[(이름, 모듈)]. 하나라도 import 가 실패하면 ParamsError(원래 예외를 __cause__ 로)."""
    out = []
    for name in (fragment_names() if names is None else list(names)):
        try:
            mod = importlib.import_module(f"{__name__}.{name}")
        except Exception as e:  # noqa: BLE001 — 어떤 실패든 이름을 붙여 다시 던진다
            raise ParamsError(f"파라미터 조각 {name} 을 import 할 수 없음: {e!r}") from e
        out.append((name, mod))
    return out


def _seq(mod: ModuleType, name: str, attr: str, kind: type, required: bool) -> tuple:
    if not hasattr(mod, attr):
        if required:
            raise ParamsError(f"파라미터 조각 {name}: {attr} 가 없음")
        return ()
    items = getattr(mod, attr)
    if not isinstance(items, (tuple, list)):
        raise ParamsError(f"파라미터 조각 {name}: {attr} 는 tuple 이어야 함 (받은 {type(items).__name__})")
    bad = [repr(x)[:60] for x in items if not isinstance(x, kind)]
    if bad:
        raise ParamsError(f"파라미터 조각 {name}: {attr} 에 {kind.__name__} 가 아닌 항목 {bad[:3]}")
    return tuple(items)


def collect(modules: Optional[Sequence[tuple]] = None) -> Table:
    """조각을 모아 Table. modules=[(이름, 모듈)] 를 주면 그것만(시험용), 없으면 이 폴더의 모든 조각."""
    mods = load_fragments() if modules is None else list(modules)
    params, rules, dropped, origin, errs = [], [], [], {}, []
    v1paths: dict = {}                                 # v1 경로 → "조각.키"(두 키가 같은 v1 키를 옮기면 오류)
    for name, mod in mods:
        for p in _seq(mod, name, "PARAMS", ParamDef, True):
            if p.key in origin:
                errs.append(f"키 중복 {p.key}: {origin[p.key]} / {name}")
                continue
            origin[p.key] = name
            params.append(p)
            for ref in v1_refs(p.key, p.v1_key):
                if ref in v1paths:
                    errs.append(f"같은 v1 키 {ref} 를 두 키가 옮김: {v1paths[ref]} / {name}.{p.key}")
                v1paths[ref] = f"{name}.{p.key}"
        for r in _seq(mod, name, "CROSS_RULES", CrossRule, False):
            rid = f"rule:{r.id}"
            if rid in origin:
                errs.append(f"키 사이 규칙 id 중복 {r.id}: {origin[rid]} / {name}")
                continue
            origin[rid] = name
            rules.append(r)
        for d in _seq(mod, name, "DROPPED", Dropped, False):
            did = f"dropped:{d.key}"
            if did in origin:
                errs.append(f"DROPPED 중복 {d.key}: {origin[did]} / {name}")
                continue
            origin[did] = name
            dropped.append(d)
    both = sorted({p.key for p in params} & {d.key for d in dropped})
    if both:
        errs.append(f"PARAMS 와 DROPPED 에 함께 있는 키: {both}")
    used = {ref: p.key for p in params for ref in v1_refs(p.key, p.v1_key)}
    clash = sorted(f"{ref}(PARAMS {used[ref]} · DROPPED {origin['dropped:' + d.key]})"
                   for d in dropped for ref in v1_refs(d.key, d.key) if ref in used)
    if clash:
        errs.append(f"같은 v1 키를 옮기면서 버림: {clash}")
    if errs:
        raise ParamsError("파라미터 표 오류 — " + "; ".join(errs))
    return Table(tuple(params), tuple(rules), tuple(dropped), origin, tuple(n for n, _ in mods))


_lock = threading.Lock()
_cached: Optional[Table] = None


def table(reload: bool = False) -> Table:
    """모은 표(한 번 모으면 캐시). reload=True 면 다시 모은다(이미 import 된 조각은 그대로)."""
    global _cached
    with _lock:
        if _cached is None or reload:
            _cached = collect()
        return _cached


def by_key() -> dict:
    return table().by_key()


def schema_hash() -> str:
    return table().schema_hash()


def __getattr__(name: str):
    """PARAMS·CROSS_RULES·DROPPED·ORIGIN 은 처음 읽을 때 모은다. 그 밖의 이름은 AttributeError(조각 하위 모듈 import 는 그대로)."""
    if name in _TABLE_NAMES:
        t = table()
        return {"PARAMS": t.params, "CROSS_RULES": t.rules, "DROPPED": t.dropped, "ORIGIN": t.origin}[name]
    raise AttributeError(name)


__all__ = ["ParamsError", "Table", "collect", "table", "by_key", "schema_hash", "fragment_names", "missing_fragments",
           "load_fragments", "PARAMS", "CROSS_RULES", "DROPPED", "ORIGIN"]

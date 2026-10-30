"""파라미터 정의 체계 (장비 공통).

원칙
- 정의(이름·단위·범위·등급)의 원본은 장비 코드(이 표), 값의 원본은 웹 DB 다.
- 장비는 hello 때 schema_dict()·schema_hash() 를 올리고, 웹은 같은 정의로 사전 검사한다.
  장비가 최종 검사하며 하나라도 실패하면 그 버전 전체를 거부한다(부분 적용 없음).
- scope=secret 은 값·기본값을 웹에 보내지 않는다(장비 로컬 파일에만 둔다).
- 타입은 엄격하다: bool 자리에 "false" 문자열, int 자리에 bool 은 거부한다(v1 은 "false" 를 참으로 읽었다).
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence


class PType(str, Enum):
    INT = "int"
    FLOAT = "float"
    BOOL = "bool"
    STR = "str"
    ENUM = "enum"


class Safety(str, Enum):
    """잘못 바꿨을 때의 위험. H 는 공정 중 변경 금지·admin 전용."""
    H = "H"
    M = "M"
    L = "L"


class Apply(str, Enum):
    """값이 반영되는 시점."""
    LIVE = "live"        # 즉시(공정 중에도)
    IDLE = "idle"        # 공정·히터가 멈춰 있을 때만 적용, 그 전까지 '대기 중'
    RESTART = "restart"  # 프로그램 재시작 뒤


class Scope(str, Enum):
    PROCESS = "process"  # 운전 파라미터 — 웹에서 편집
    INSTALL = "install"  # 설치값(포트·장비 ID·경로) — 웹(admin, 재시작) + 노트북 비상 덮어쓰기 파일
    SECRET = "secret"    # 비밀값 — 웹에 보내지 않는다


class Role(str, Enum):
    ADMIN = "admin"
    OPERATOR = "operator"


@dataclass(frozen=True)
class ParamDef:
    key: str
    group: str
    label: str                      # 한글 이름(웹 표시)
    desc: str
    unit: str
    type: PType
    default: Any                    # 지금 운영값(v1 config_user.json) — 없으면 v1 코드값
    min: Optional[float] = None
    max: Optional[float] = None
    enum: Optional[tuple] = None
    pattern: Optional[str] = None   # STR 전용 정규식(전체 일치)
    safety: Safety = Safety.M
    apply: Apply = Apply.IDLE
    scope: Scope = Scope.PROCESS
    role: Role = Role.ADMIN
    v1_key: Optional[str] = None    # v1 json 키 이름이 다르면(없으면 key 와 같거나 v1 에 없음)
    v1_default: Any = None          # v1 lib/config.py 기본값(참고) — json 값과 다를 때만 의미
    v1_source: str = ""             # v1 근거 위치 ("config_user.json", "controller/process_controller.py:35" …)
    pending_confirm: bool = False   # 사용자 확인 대기 — 권장안으로 넣어 둠
    note: str = ""
    v1_period_s: Optional[float] = None   # v1 판정 주기 — v1 은 횟수, v2 기본값 = v1 횟수 × 이 주기(초, 시간 기반 판정)
    web_locked: bool = False        # 웹에서 바꾸지 않는 키(CONFIG_APPLY·ROLLBACK 에 지금 값과 다른 값 → CONFIG_LOCKED_KEY)

    def public_dict(self) -> dict:
        """웹으로 보내는 정의. secret 은 기본값을 싣지 않는다."""
        d = {
            "key": self.key, "group": self.group, "label": self.label, "desc": self.desc,
            "unit": self.unit, "type": self.type.value,
            "default": None if self.scope is Scope.SECRET else self.default,
            "min": self.min, "max": self.max,
            "enum": list(self.enum) if self.enum else None,
            "pattern": self.pattern,
            "safety": self.safety.value, "apply": self.apply.value,
            "scope": self.scope.value, "role": self.role.value,
            "pendingConfirm": self.pending_confirm, "webLocked": self.web_locked,
        }
        return d


@dataclass(frozen=True)
class CrossRule:
    """키 사이 규칙. check(values) 가 None 이면 통과, 문자열이면 위반 사유."""
    id: str
    keys: tuple
    desc: str
    check: Callable[[Mapping[str, Any]], Optional[str]]


@dataclass(frozen=True)
class Dropped:
    """v2 에서 뺀 v1 키·상수와 이유."""
    key: str
    reason: str
    v1_source: str = ""
    replaced_by: Optional[str] = None


def check_value(p: ParamDef, v: Any) -> Optional[str]:
    """한 값 검사. 통과하면 None."""
    t = p.type
    if t is PType.BOOL:
        if not isinstance(v, bool):
            return f"{p.key}: bool 이어야 함 (받은 값 {v!r})"
        return None
    if t is PType.INT:
        if isinstance(v, bool) or not isinstance(v, int):
            return f"{p.key}: 정수여야 함 (받은 값 {v!r})"
    elif t is PType.FLOAT:
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return f"{p.key}: 숫자여야 함 (받은 값 {v!r})"
        if v != v:  # NaN
            return f"{p.key}: NaN 불가"
    elif t is PType.STR:
        if not isinstance(v, str):
            return f"{p.key}: 문자열이어야 함 (받은 값 {v!r})"
        if p.pattern is not None and not re.fullmatch(p.pattern, v):
            return f"{p.key}: 형식 불일치 {v!r} (형식 {p.pattern})"
        return None
    elif t is PType.ENUM:
        if v not in (p.enum or ()):
            return f"{p.key}: 허용값 {list(p.enum or ())} 중 하나여야 함 (받은 값 {v!r})"
        return None
    if p.min is not None and v < p.min:
        return f"{p.key}: {v} < 최소 {p.min}"
    if p.max is not None and v > p.max:
        return f"{p.key}: {v} > 최대 {p.max}"
    return None


def check_config(defs: Sequence[ParamDef], rules: Sequence[CrossRule],
                 values: Mapping[str, Any]) -> list:
    """설정 한 벌 전체 검사 — 오류 문구 목록(빈 목록이면 통과). 모르는 키·빠진 키도 오류."""
    errs = []
    by_key = {p.key: p for p in defs}
    for k in values:
        if k not in by_key:
            errs.append(f"{k}: 정의되지 않은 키")
    for p in defs:
        if p.key not in values:
            errs.append(f"{p.key}: 값 없음")
            continue
        e = check_value(p, values[p.key])
        if e:
            errs.append(e)
    if errs:
        return errs          # 타입이 틀리면 키 사이 규칙은 돌리지 않는다
    for r in rules:
        try:
            msg = r.check(values)
        except Exception as ex:  # 규칙 자체 오류도 거부 사유
            msg = f"규칙 평가 오류: {ex!r}"
        if msg:
            errs.append(f"[{r.id}] {msg}")
    return errs


def as_float(defs: Iterable[ParamDef], values: Mapping[str, Any]) -> dict:
    """float 형식 키의 정수 값을 float 로(bool 은 숫자로 치지 않아 그대로 — 검사가 거부). 문구·JSON 이 늘 700.0 모양 —
    웹 TS 는 묶음 형식대로 쓰고, 웹 JSON 은 700.0 을 700 으로 보낸다."""
    fl = {p.key for p in defs if p.type is PType.FLOAT}
    return {k: (float(v) if k in fl and isinstance(v, int) and not isinstance(v, bool) else v) for k, v in values.items()}


def defaults(defs: Iterable[ParamDef]) -> dict:
    defs = tuple(defs)
    return as_float(defs, {p.key: p.default for p in defs})


def schema_dict(defs: Sequence[ParamDef], rules: Sequence[CrossRule]) -> dict:
    """hello 에 싣는 정의(비밀값 기본값 제외)."""
    return {
        "params": [p.public_dict() for p in defs],
        "rules": [{"id": r.id, "keys": list(r.keys), "desc": r.desc} for r in rules],
    }


def schema_hash(defs: Sequence[ParamDef], rules: Sequence[CrossRule]) -> str:
    raw = json.dumps(schema_dict(defs, rules), ensure_ascii=False, sort_keys=True,
                     separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

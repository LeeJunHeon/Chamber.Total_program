"""파라미터 조각 공통 도우미 — 조각(params/<이름>.py)이 ParamDef 를 짧게 쓰도록(챔버K v2/chk/params.py 의 _p·_inst 와 같은 모양).

조각 규칙(params/__init__.py 가 모으고 v2/tests/test_params.py 가 지킨다)
- 모듈마다 PARAMS = (ParamDef, ...) — 필수. CROSS_RULES·DROPPED 는 있으면 모은다.
- 키 이름: v1 settings.json 공통 키는 이름 그대로, ch1/ch2 구역 키는 "CH1_"/"CH2_" 를 앞에 붙인다.
  v1 이름과 다르게 지었으면 v1_key 에 v1 경로("ch2.HEATER_MAX_TEMP" 처럼 구역까지)를 적는다.
- default 는 v1 이 실제로 쓰던 값(v2/ch12/spec/inv_cfg.py 의 ⚠ — 채널 모듈을 먼저 읽는 곳은 .py 기본값).
- Dropped.key 는 v1 경로("common.X"·"ch1.X"·"(code).X") — 변환 도구가 이 키를 '버림'으로 처리한다.

이 모듈 이름은 '_' 로 시작해 조각으로 세지 않는다.
"""
from __future__ import annotations

from v2.common.paramdef import Apply, CrossRule, Dropped, ParamDef, PType, Role, Safety, Scope

F, I, B, S, E = PType.FLOAT, PType.INT, PType.BOOL, PType.STR, PType.ENUM
H, M, L = Safety.H, Safety.M, Safety.L
LIVE, IDLE, RESTART = Apply.LIVE, Apply.IDLE, Apply.RESTART

V2 = "v2"
JSON_LIST = r"\[.*\]"          # v1 목록 값(list)을 JSON 글자로 담는 STR 키의 형식(변환 도구가 json.dumps 로 넣는다)
ANY = r".*"
IPV4 = r"(25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])(\.(25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])){3}"


def p(key, group, label, unit, type_, default, lo=None, hi=None, *, safety=M, apply=IDLE,
      scope=Scope.PROCESS, role=None, desc="", enum=None, pattern=None, v1_key=None,
      v1_default=None, src=V2, pending=False, note="", v1_period=None, locked=False) -> ParamDef:
    """운전 파라미터 하나. role 을 안 주면 H 등급·install·secret 은 admin, 나머지는 operator."""
    if role is None:
        role = Role.ADMIN if (safety is H or scope is not Scope.PROCESS) else Role.OPERATOR
    return ParamDef(key=key, group=group, label=label, desc=desc or label, unit=unit, type=type_,
                    default=default, min=lo, max=hi, enum=enum, pattern=pattern, safety=safety,
                    apply=apply, scope=scope, role=role, v1_key=v1_key, v1_default=v1_default,
                    v1_source=src, pending_confirm=pending, note=note, v1_period_s=v1_period, web_locked=locked)


def inst(key, group, label, type_, default, lo=None, hi=None, *, unit="", safety=M, desc="", enum=None,
         pattern=None, v1_key=None, v1_default=None, src=V2, pending=False, note="", locked=False) -> ParamDef:
    """설치값(scope=install) — 웹 admin + 노트북 비상 덮어쓰기, 재시작 뒤 적용."""
    return p(key, group, label, unit, type_, default, lo, hi, safety=safety, apply=RESTART, scope=Scope.INSTALL,
             desc=desc, enum=enum, pattern=pattern, v1_key=v1_key, v1_default=v1_default, src=src,
             pending=pending, note=note, locked=locked)


def secret(key, group, label, *, desc="", pattern=None, src=V2, note="") -> ParamDef:
    """비밀값(scope=secret) — 장비 로컬 파일(local_override.json)에만, 웹에 보내지 않는다. 기본 "" — 값은 커밋하지 않는다."""
    return p(key, group, label, "", S, "", safety=M, apply=RESTART, scope=Scope.SECRET, desc=desc,
             pattern=pattern, src=src, note=note)


def rule(id_, keys, desc, fn) -> CrossRule:
    return CrossRule(id=id_, keys=tuple(keys), desc=desc, check=fn)


def need(cond: bool, msg: str):
    return None if cond else msg


__all__ = ["Apply", "CrossRule", "Dropped", "ParamDef", "PType", "Role", "Safety", "Scope",
           "F", "I", "B", "S", "E", "H", "M", "L", "LIVE", "IDLE", "RESTART", "V2", "JSON_LIST", "ANY", "IPV4",
           "p", "inst", "secret", "rule", "need"]

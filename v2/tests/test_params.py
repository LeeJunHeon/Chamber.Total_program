"""챔버12 파라미터 표 시험 — 조각 모으기(크게 실패), 등급 규칙, 범위·키 사이 규칙, v1 키 담당 표, core 기본값 = v1 실효값.

장비 조각(다른 작성자)이 아직 정의하지 않은 v1 키는 실패로 보지 않고 '아직 없는 키'로 출력만 한다(-s 로 보인다).
"""
import json
import re
import types

import pytest

import v2.ch12.params as P
from v2.ch12.params import ownership as own
from v2.ch12.spec import settings_v1facts as facts
from v2.common.paramdef import (
    Apply, CrossRule, Dropped, ParamDef, PType, Role, Safety, Scope, check_config, check_value, defaults, schema_dict,
)
from v2.tools import convert_v1_config as conv

T = P.table(reload=True)
BY_KEY = T.by_key()
MINE = ("core", "core_heater", "core_thickness")
FACT = {k[0]: k for k in facts.KEYS}


def _pd(key, **kw):
    base = dict(key=key, group="g", label="l", desc="d", unit="", type=PType.INT, default=1, min=0, max=9, v1_source="v2")
    base.update(kw)
    return ParamDef(**base)


def _mod(**attrs):
    return types.SimpleNamespace(**attrs)


# ───────────── 모으기 ─────────────
def test_collect_all_fragments():
    t = P.table(reload=True)
    assert set(MINE) <= set(t.fragments)
    print("\n조각:", t.fragments, "· 아직 파일 없는 조각:", P.missing_fragments(), "· 키", len(t.params),
          "· 규칙", len(t.rules), "· 버림", len(t.dropped))
    from v2.ch12.params import PARAMS, CROSS_RULES, DROPPED     # 지연 이름(PEP 562)
    assert PARAMS == t.params and CROSS_RULES == t.rules and DROPPED == t.dropped


def test_import_failure_is_loud(monkeypatch):
    with pytest.raises(P.ParamsError, match="__없는_조각__"):
        P.load_fragments(["__없는_조각__"])

    def boom(name, *a, **k):
        raise SyntaxError("조각 문법 오류")
    monkeypatch.setattr(P.importlib, "import_module", boom)
    with pytest.raises(P.ParamsError) as ei:
        P.collect()
    assert isinstance(ei.value.__cause__, SyntaxError)


@pytest.mark.parametrize("mods,msg", [
    ([("a", _mod(PARAMS=(_pd("X"),))), ("b", _mod(PARAMS=(_pd("X"),)))], "키 중복 X"),
    ([("a", _mod(PARAMS=(_pd("X"), _pd("X"))))], "키 중복 X"),
    ([("a", _mod(PARAMS=(_pd("X"),))), ("b", _mod(PARAMS=(_pd("Y", v1_key="common.X"),)))], "같은 v1 키 common.X"),
    ([("a", _mod(PARAMS=(_pd("X"),), DROPPED=(Dropped("common.X", "r"),)))], "옮기면서 버림"),
    ([("a", _mod(PARAMS=(_pd("X"),), DROPPED=(Dropped("X", "r"),)))], "PARAMS 와 DROPPED"),
    ([("a", _mod(PARAMS=(), DROPPED=(Dropped("common.Q", "r"), Dropped("common.Q", "r"))))], "DROPPED 중복"),
    ([("a", _mod(PARAMS=(), CROSS_RULES=(CrossRule("R", (), "", lambda v: None),) * 2))], "규칙 id 중복"),
    ([("a", _mod(DROPPED=()))], "PARAMS 가 없음"),
    ([("a", _mod(PARAMS=("X",)))], "ParamDef 가 아닌"),
    ([("a", _mod(PARAMS=[_pd("X")], CROSS_RULES="R"))], "tuple 이어야"),
])
def test_structure_errors_are_loud(mods, msg):
    with pytest.raises(P.ParamsError, match=msg):
        P.collect(mods)


# ───────────── 정의마다(챔버K test_params 와 같은 등급 규칙) ─────────────
@pytest.mark.parametrize("p", T.params, ids=lambda p: f"{T.origin[p.key]}:{p.key}")
def test_def_is_consistent(p):
    assert re.fullmatch(r"[A-Z][A-Z0-9_]*", p.key)
    assert p.label and p.desc and p.group and p.v1_source
    assert check_value(p, p.default) is None, check_value(p, p.default)
    if p.type is PType.ENUM:
        assert p.enum and p.default in p.enum
    if p.type in (PType.INT, PType.FLOAT) and p.min is not None and p.max is not None:
        assert p.min <= p.max
    if p.type in (PType.BOOL, PType.ENUM, PType.STR):
        assert p.min is None and p.max is None
    if p.safety is Safety.H:                       # H 등급: admin 전용, 공정 중 변경 금지
        assert p.role is Role.ADMIN and p.apply is not Apply.LIVE
    if p.scope is not Scope.PROCESS:               # 설치값·비밀값: 재시작, admin
        assert p.apply is Apply.RESTART and p.role is Role.ADMIN
    if p.scope is Scope.SECRET:
        assert p.default == "" and p.public_dict()["default"] is None


def test_all_defaults_pass_config_check():
    assert check_config(T.params, T.rules, defaults(T.params)) == []


def test_cross_rule_keys_exist():
    keys = set(BY_KEY)
    for r in T.rules:
        assert set(r.keys) <= keys, (r.id, set(r.keys) - keys)


def test_schema_hash_and_secrets():
    h = T.schema_hash()
    assert re.fullmatch(r"[0-9a-f]{64}", h) and h == P.schema_hash()
    sd = schema_dict(T.params, T.rules)
    secrets = [d for d in sd["params"] if d["scope"] == "secret"]
    assert secrets and all(d["default"] is None for d in secrets)


# ───────────── v1 키 담당 표 ─────────────
def test_ownership_covers_all_530_v1_keys():
    assert len(facts.KEYS) == 530
    assert set(own.V1_KEY_OWNER) == set(FACT)
    assert set(own.V1_KEY_OWNER.values()) | set(own.V1_CODE_ONLY_OWNER.values()) <= set(own.EXPECTED_FRAGMENTS)


def test_name_rule():
    assert own.v2_key_for("common.DCP_GAP_MS") == "DCP_GAP_MS"
    assert own.v2_key_for("ch2.MFC_TCP_PORT") == "CH2_MFC_TCP_PORT"
    assert own.v2_key_for("ch1.MFC_SCALE_FACTORS.2") == "CH1_MFC_SCALE_FACTORS_2"
    assert own.v2_key_for("(code).PC_POWER_SELECT_SETTLE_MS") == "PC_POWER_SELECT_SETTLE_MS"
    assert own.v1_refs("CH2_HEATER_KP") == ("ch2.HEATER_KP",)
    assert own.v1_refs("X", "ch1.Y") == ("ch1.Y",) and own.v1_refs("CH1_X", "Y") == ("ch1.Y",)
    assert own.is_declared("ch1", "DCP_GAP_MS") and own.is_declared("ch2", "PROCESS_GUN_SHUTTERS")
    assert not own.is_declared("common", "PROCESS_GUN_SHUTTERS") and not own.is_declared("common", "NOPE")


def _status():
    """v1 경로 → ('param', 키) / ('dropped', 이유) / None — 변환 도구와 같은 색인."""
    c = conv.Converter(T)
    out = {}
    for path in own.V1_KEY_OWNER:
        if path in c.param_of:
            out[path] = ("param", c.param_of[path].key)
        elif path in c.dropped_of:
            out[path] = ("dropped", c.dropped_of[path].reason)
        else:
            out[path] = None
    return out


def test_my_fragments_cover_their_v1_keys():
    st = _status()
    missing = [p for p, o in own.V1_KEY_OWNER.items() if o in MINE and st[p] is None]
    assert not missing, missing
    elsewhere = [(p, T.origin[st[p][1]]) for p, o in own.V1_KEY_OWNER.items()
                 if o in MINE and st[p] and st[p][0] == "param" and T.origin[st[p][1]] != o]
    assert not elsewhere, elsewhere


def test_report_v1_keys_not_yet_defined():
    """장비 조각 작성자가 아직 정의하지 않은 v1 키 — 출력만(마지막에 채운다)."""
    st = _status()
    todo: dict = {}
    for path, o in list(own.V1_KEY_OWNER.items()) + list(own.V1_CODE_ONLY_OWNER.items()):
        if st.get(path) is None and not (path in own.V1_CODE_ONLY_OWNER and own.v2_key_for(path) in BY_KEY):
            todo.setdefault(o, []).append(path)
    moved = sorted((p, o, T.origin[s[1]]) for p, s in st.items() if s and s[0] == "param"
                   for o in [own.V1_KEY_OWNER[p]] if T.origin[s[1]] != o)
    print(f"\n아직 없는 v1 키 {sum(map(len, todo.values()))}개:")
    for o, paths in todo.items():
        print(f"  {o} ({len(paths)}): {paths}")
    if moved:
        print(f"담당과 다른 조각이 정의한 v1 키 {len(moved)}개: {moved[:20]}{' …' if len(moved) > 20 else ''}")


# ───────────── core 기본값 = v1 실효값 ─────────────
def _v1_effective(path: str):
    f = FACT.get(path)
    if f is None:                                    # ch1 구역에 없는 채널 키 — 채널 모듈 복사값(.py)
        return FACT["ch2." + path.split(".", 1)[1]][2]
    if path.startswith("(code).") or conv._effect(path) == "dead_common" or f[1] is None:
        return f[2]
    return f[1]


def _my_params():
    for p in T.params:
        if T.origin[p.key] in MINE and p.v1_source != "v2" and not p.v1_source.startswith(("v2 ", "lib/config_local")):
            yield p


@pytest.mark.parametrize("p", list(_my_params()), ids=lambda p: p.key)
def test_my_defaults_equal_v1_effective(p):
    path = own.v1_refs(p.key, p.v1_key)[0]
    if path not in FACT and f"(code).{p.key}" in FACT:
        path = f"(code).{p.key}"
    want = _v1_effective(path)
    if p.key.endswith("THICKNESS_FACTOR_FILE"):      # "" = 설정 폴더의 thickness_factors_chN.json(v1 기본 ""와 같은 파일)
        assert p.default == "" and FACT.get(path, (None, None, ""))[2] == ""
        return
    got = json.loads(p.default) if p.pattern == r"\[.*\]" else p.default
    assert conv._same(got, want), (path, got, want)
    if conv._effect(path) == "dead_common":           # ⚠ v1 이상 동작 — 사용자 확인 필요로 표시
        assert p.pending_confirm and "사용자 확인 필요" in p.note


def test_v2_keys():
    d = defaults(T.params)
    assert d["EQUIPMENT_ID"] == "CH12" and d["RUN_MODE"] == "sim" and BY_KEY["EQUIPMENT_ID"].web_locked
    assert d["RESUME_ENABLED"] is True and d["RESUME_MAX_GAP_S"] == 30.0 and BY_KEY["RESUME_MAX_GAP_S"].apply is Apply.LIVE
    assert d["COMMAND_SOURCES_LOCAL_ALL"] is True
    assert d["ROBOT_API_PORT"] == 50071 and d["HOST_SERVER_PORT"] == 50070
    for k in ("TICK_DEADLINE_MS", "LOCAL_HMI_PORT", "UPLINK_INTERVAL_MS", "LOCAL_DATA_DIR", "NAS_COPY_DIR",
              "SNAPSHOT_RETENTION_DAYS"):
        assert k in d
    for k in ("UPLINK_URL", "UPLINK_TOKEN", "CHAT_WEBHOOK_URL", "CHAT_WEBHOOK_MONITOR_URL"):
        assert BY_KEY[k].scope is Scope.SECRET and BY_KEY[k].default == ""


@pytest.mark.parametrize("key,bad", [
    ("CH2_HEATER_MAX_TEMP", 950.0),           # > SV 상한(900) — CH01, 범위
    ("CH2_HEATER_OT_LIMIT_C", 900.0),         # ≤ SV 상한 — CH02
    ("CH2_HEATER_MV_LIMIT", 1400),            # > ABS_MAX — CH03
    ("CH2_HEATER_SESSION_MARK_REG", 140),     # D110~D149 — CH06
    ("CH2_HEATER_WD_PERIOD_S", 20.0),         # v1 클램프 1~9
    ("CH2_HEATER_STALE_SEC", 5.0),            # < 2 × POLL_IDLE — CH05
    ("LOCAL_HMI_PORT", 50070),                # 로봇 서버와 같은 포트 — C01
    ("CH1_ETA_TAIL_MIN_S", 200.0),            # > TAIL — C03
    ("TSP_DAILY_HH", 24),
])
def test_bad_values_rejected(key, bad):
    vals = defaults(T.params)
    vals[key] = bad
    assert check_config(T.params, T.rules, vals)


@pytest.mark.parametrize("key,bad", [
    ("RESUME_ENABLED", "false"),              # v1 은 문자열을 참으로 읽던 곳이 있었다
    ("PC_DEFAULT_GAS_IDX", True),             # bool 은 int 로 받지 않는다
    ("CH2_HEATER_HOLD_MODE", "DAC"),          # v1 은 소문자로 고쳤다 — v2 는 정확히
    ("CH2_HEATER_HOLD_FAIL_ACTION", "stop"),
    ("ROBOT_RECIPE_FOLDERS", "CH1,CH2"),      # 목록은 JSON 글자
    ("CH1_THICKNESS_DONE_CONSECUTIVE", 1.5),
])
def test_strict_types(key, bad):
    assert check_value(BY_KEY[key], bad)

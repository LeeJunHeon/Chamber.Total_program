"""범위 밖 설정값은 적용 자체를 거부한다 — 어디에도 조용히 고쳐 쓰는(clamp) 경로가 남지 않았음을 확인(사용자 결정 2026-11-02, D11).

세 겹으로 본다.
 1) 문(gate): 파라미터 표의 모든 숫자·열거 키에 범위 밖 값을 넣으면 ConfigStore/ConfigApply 가 버전 전체를 거부하고 저장 값·실행 사전이 그대로.
    v1 변환 도구도 범위 밖 값을 고쳐 옮기지 않고 오류로 낸다.
 2) 코드 안의 클램프: 소스에서 max(상수, 설정값)·min(상수, 설정값) 모양을 AST 로 찾아, 그 상수가 파라미터 표 범위 밖이 아님을 본다
    (표가 받은 값이 코드에서 조용히 바뀌는 길이 없음). 예외(허용 목록)는 이유를 적는다.
 3) 읽기 쪽 문: 표가 받는 값(경계값)은 히터·두께 판정·두께 모니터 읽기에서 그대로 읽히고, 범위 밖을 직접 넣으면 ConfigRangeError.
"""
from __future__ import annotations

import ast
import json
import math
from pathlib import Path

import pytest

from v2.ch12.params import table
from v2.common.config_store import ConfigStore
from v2.common.paramdef import ConfigRangeError, PType, check_config, defaults

ROOT = Path(__file__).resolve().parents[2]
T = table()
BY = T.by_key()
DEF = defaults(T.params)
WEB = {p.key: DEF[p.key] for p in T.params if p.scope.value != "secret"}      # 웹이 보내는 값(비밀값 제외)


def _out_of_range(p):
    """(낮은 쪽, 높은 쪽) 범위 밖 값 — 없는 쪽은 None."""
    lo = hi = None
    if p.type in (PType.INT, PType.FLOAT):
        step = 1 if p.type is PType.INT else max(abs(p.min if p.min is not None else 0), 1.0) * 1e-3 + 1e-3
        if p.min is not None:
            lo = p.min - step if p.type is PType.FLOAT else int(p.min) - 1
        if p.max is not None:
            hi = p.max + step if p.type is PType.FLOAT else int(p.max) + 1
    return lo, hi


# ───────────────────────── 1) 문 ─────────────────────────
def test_gate_rejects_every_out_of_range_value(tmp_path):
    """모든 숫자 키: 최소 미만·최대 초과 값이 들어간 버전은 CONFIG_INVALID 로 통째 거부 — 저장 파일·값이 그대로."""
    st = ConfigStore(tmp_path / "cfg", T.params, T.rules, T.schema_hash())
    st.load()
    before = dict(st.values)
    checked = 0
    ver = 1
    for p in T.params:
        if p.scope.value == "secret":
            continue
        lo, hi = _out_of_range(p)
        for bad in (lo, hi):
            if bad is None:
                continue
            ver += 1
            res = st.apply(ver, {**WEB, p.key: bad}, T.schema_hash(), False, by="test")
            assert res.status == "rejected" and res.reason in ("CONFIG_INVALID", "CONFIG_LOCKED_KEY"), (p.key, bad, res)
            if res.reason == "CONFIG_INVALID":
                assert any(p.key in e for e in res.errors), (p.key, res.errors)
            checked += 1
    assert checked > 500
    assert st.values == before and not st.active_path.exists()                      # 부분 적용 없음


def test_gate_rejects_wrong_type_and_enum(tmp_path):
    st = ConfigStore(tmp_path / "cfg", T.params, T.rules, T.schema_hash())
    st.load()
    ver = 1
    for p in T.params:
        if p.scope.value == "secret":
            continue
        bad = {PType.INT: "7", PType.FLOAT: "7.5", PType.BOOL: "false", PType.STR: 5, PType.ENUM: "__없는값__"}[p.type]
        ver += 1
        res = st.apply(ver, {**WEB, p.key: bad}, T.schema_hash(), False, by="test")
        assert res.status == "rejected", (p.key, bad)
    assert not st.active_path.exists()


def test_config_apply_rejects_without_touching_running_cfg(tmp_path):
    """웹 설정 화면 경로(ConfigApply): 범위 밖이 하나라도 있으면 실행 사전은 한 키도 바뀌지 않는다."""
    from v2.ch12.config_apply import ConfigApply
    st = ConfigStore(tmp_path / "cfg", T.params, T.rules, T.schema_hash())
    cfg = dict(st.load())
    snap = dict(cfg)
    ca = ConfigApply(st, cfg, lambda: False)
    vals = dict(WEB)
    vals["CH2_HEATER_KP"] = 60.0                       # 범위 밖(v1 은 쓰지 않고 래더 기본으로 두었다)
    vals["CH2_HEATER_POLL_INTERVAL_S"] = 4.0           # 바른 값
    reason = ca.apply({"version": 5, "values": vals, "schemaHash": T.schema_hash()}, "web")
    assert reason == "CONFIG_INVALID" and cfg == snap


def test_cross_rule_violations_are_rejected_not_fixed(tmp_path):
    """키 사이 규칙 위반(v1 은 고쳐 썼다): 과온 ≤ 목표 상한, MV_LIMIT 밖, 두께 stale < 4×주기 등은 거부."""
    st = ConfigStore(tmp_path / "cfg", T.params, T.rules, T.schema_hash())
    st.load()
    cases = [{"CH2_HEATER_SV_LIMIT_C": 600.0, "CH2_HEATER_OT_LIMIT_C": 550.0},
             {"CH2_HEATER_MV_LIMIT": 300},
             {"CH2_HEATER_MV_ABS_MAX": 410},
             {"CH2_HEATER_RUN_LOG_PERIOD_S": 1.0},
             {"CH2_HEATER_SESSION_MARK_REG": 120},
             {"CH2_HEATER_STALE_SEC": 3.0},
             {"CH2_THICKNESS_POLL_INTERVAL_S": 1.0, "CH2_THICKNESS_STALE_S": 3.0},
             {"CH2_THICKNESS_POLL_INTERVAL_S": 2.0, "CH2_THICKNESS_SLOW_POLL_S": 1.0}]
    for i, over in enumerate(cases):
        res = st.apply(10 + i, {**WEB, **over}, T.schema_hash(), False, by="test")
        assert res.status == "rejected" and res.reason == "CONFIG_INVALID", (over, res)


def test_converter_errors_instead_of_clamping(tmp_path):
    """v1 settings.json 값이 v2 범위 밖이면 변환 도구가 고쳐 옮기지 않고 오류를 낸다(전부 아니면 전무)."""
    from v2.tools import convert_v1_config as C
    root = tmp_path / "v1"
    (root / "config").mkdir(parents=True)
    (root / "config" / "settings.json").write_text(json.dumps({"ch2": {"HEATER_KP": 60.0, "HEATER_RUN_LOG_PERIOD_S": 1}}),
                                                   encoding="utf-8")
    res = C.convert(root / "config" / "settings.json", root)
    assert not res.ok and any("CH2_HEATER_KP" in e for e in res.errors)
    assert "CH2_HEATER_KP" not in res.values


# ───────────────────────── 2) 코드 안의 클램프 ─────────────────────────
# 설정값을 상수와 max/min 하는 곳 중 '표 범위 밖 값을 바꿀 수 있는' 곳의 허용 목록 — (파일, 키): 이유
ALLOWED = {
    ("ch12/drivers/mfc.py", "MFC_GAP_MS"): "재시도 쉼 = max(0.15 s, 간격)(v1 device/mfc.py:1425) — 설정 값을 바꾸는 것이 아니라 한 번의 쉼 길이를 정하는 규칙",
    ("ch12/drivers/rfpulse.py", "ACK_TIMEOUT_MS"): "RF ON/OFF 응답 대기는 2500 ms 이상(v1 device/rf_pulse.py) — 명령 하나의 최소 대기, 설정 값은 그대로",
    ("ch12/drivers/rfpulse_poll.py", "ACK_TIMEOUT_MS"): "위와 같음",
}


def _keys_in(node, by):
    out = []
    for n in ast.walk(node):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            for pre in ("", "CH1_", "CH2_"):
                if pre + n.value in by:
                    out.append(pre + n.value)
    return out


def _const(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)) and not isinstance(node.value, bool):
        return node.value
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub) and isinstance(node.operand, ast.Constant):
        return -node.operand.value
    return None


def _clamp_sites():
    v2 = ROOT / "v2"
    files = sorted((v2 / "ch12").rglob("*.py")) + sorted((v2 / "common").glob("*.py"))
    for f in files:
        rel = f.relative_to(v2).as_posix()
        if rel.startswith(("ch12/spec/", "ch12/sim/", "ch12/params/")):
            continue
        for n in ast.walk(ast.parse(f.read_text(encoding="utf-8"))):
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id in ("max", "min") and len(n.args) == 2:
                a, b = n.args
                ca, cb = _const(a), _const(b)
                if (ca is None) == (cb is None):
                    continue
                c, other = (ca, b) if ca is not None else (cb, a)
                for k in dict.fromkeys(_keys_in(other, BY)):
                    yield rel, n.lineno, k, n.func.id, c


def test_code_clamps_never_change_a_value_the_table_accepts():
    sites = list(_clamp_sites())
    assert len(sites) > 50                              # 찾는 방법이 제대로 도는지(너무 적으면 AST 규칙이 깨진 것)
    bad = []
    for rel, line, key, kind, c in sites:
        if (rel, key) in ALLOWED or (rel, key[4:] if key[:4] in ("CH1_", "CH2_") else key) in ALLOWED:
            continue
        p = BY[key]
        ok = (p.min is not None and p.min >= c) if kind == "max" else (p.max is not None and p.max <= c)
        if not ok:
            bad.append(f"{rel}:{line} {kind}({c}, {key}) — 표 범위 {p.min}~{p.max}")
    assert not bad, "표가 받는 값을 코드가 조용히 바꾼다:\n" + "\n".join(bad)


def test_allowed_exceptions_still_exist():
    """허용 목록이 낡지 않게 — 목록의 (파일, 키)가 실제 클램프 모양으로 남아 있어야 한다."""
    found = {(rel, key) for rel, _l, key, _k, _c in _clamp_sites()}
    for k in ALLOWED:
        assert k in found, f"{k}: 코드에서 사라짐 — 허용 목록에서 지울 것"


def test_no_silent_clamp_helpers_left():
    """히터·두께·두께 모니터의 '고쳐 쓰고 경고' 도우미는 거부(ConfigRangeError)로 바뀌었다."""
    for rel in ("ch12/heater_profile.py", "ch12/thickness_judge.py", "ch12/process_steps.py", "ch12/drivers/stm100.py"):
        src = (ROOT / "v2" / rel).read_text(encoding="utf-8")
        assert "ConfigRangeError" in src, rel
        assert "→ 기본값" not in src.replace("→ 기본값/", ""), rel                      # 형식 오류를 기본값으로 바꿔 쓰던 문구
        assert "min(max(v, lo), hi)" not in src and "min(max(float(v), lo), hi)" not in src, rel


# ───────────────────────── 3) 읽기 쪽 문 ─────────────────────────
def _gate_ok(cfg) -> bool:
    return not check_config(T.params, T.rules, cfg)


def _boundary_values(p):
    vs = []
    if p.type in (PType.INT, PType.FLOAT):
        for v in (p.min, p.max):
            if v is not None:
                vs.append(p.type is PType.INT and int(v) or v)
    elif p.type is PType.ENUM:
        vs += list(p.enum or ())
    return vs


def test_heater_loader_accepts_everything_the_table_accepts():
    """표(범위 + 키 사이 규칙)가 받은 히터 값은 load_heater_cfg 가 그대로 읽는다 — 표와 읽기가 어긋나 값이 바뀌는 길이 없다."""
    from v2.ch12.heater_profile import CFG_KEYS, load_heater_cfg
    key_of = {f"CH2_{v}": f for f, v in CFG_KEYS.items()}
    n = 0
    for p in T.params:
        if p.key not in key_of or p.type is PType.BOOL or p.type is PType.STR:
            continue
        for v in _boundary_values(p):
            cfg = {**DEF, p.key: v}
            if not _gate_ok(cfg):
                continue                                                  # 표가 거부하는 조합은 읽기까지 오지 않는다
            c, _w = load_heater_cfg(cfg)
            got = getattr(c, key_of[p.key])
            assert math.isclose(float(got), float(v)) if not isinstance(v, str) else got == v, (p.key, v, got)
            n += 1
    assert n > 40


def test_heater_loader_never_rewrites_a_value():
    """표가 거부하는 값(경계 밖)을 읽기에 직접 넣으면 ConfigRangeError 이거나 값 그대로(읽기 쪽이 더 느슨한 곳) —
    다른 값으로 고쳐 쓰는 길은 없다."""
    from v2.ch12.heater_profile import CFG_KEYS, load_heater_cfg
    key_of = {f"CH2_{v}": f for f, v in CFG_KEYS.items()}
    raised = same = 0
    for p in T.params:
        if p.key not in key_of or p.type not in (PType.INT, PType.FLOAT):
            continue
        for bad in _out_of_range(p):
            if bad is None:
                continue
            try:
                c, _w = load_heater_cfg({**DEF, p.key: bad})
            except ConfigRangeError:
                raised += 1
                continue
            assert math.isclose(float(getattr(c, key_of[p.key])), float(bad)), (p.key, bad, getattr(c, key_of[p.key]))
            same += 1
    assert raised > 25 and same >= 0


@pytest.mark.parametrize("ch", [1, 2])
def test_thickness_readers_accept_everything_the_table_accepts_and_reject_the_rest(ch):
    from v2.ch12.drivers import stm100
    from v2.ch12.thickness_judge import _JUDGE_KEYS, judge_params
    P = f"CH{ch}_"
    for attr, key, lo, hi in _JUDGE_KEYS:
        p = BY[P + key] if (P + key) in BY else None
        if p is None:
            continue
        for v in (p.min, p.max):
            cfg = {**DEF, p.key: v}
            if _gate_ok(cfg):
                got = getattr(judge_params(cfg, ch)[0], attr)
                assert math.isclose(float(got), float(v)), (p.key, v, got)
        for bad in _out_of_range(p):
            if bad is not None:
                try:
                    got = getattr(judge_params({**DEF, p.key: bad}, ch)[0], attr)
                except ConfigRangeError:
                    continue
                assert math.isclose(float(got), float(bad)), (p.key, bad, got)               # 거부 아니면 값 그대로
    chn = f"ch{ch}"
    for key in ("THICKNESS_POLL_INTERVAL_S", "THICKNESS_SLOW_POLL_S", "THICKNESS_STALE_S", "THICKNESS_CMD_TIMEOUT_S"):
        p = BY[P + key]
        for v in (p.min, p.max):
            cfg = {**DEF, p.key: v}
            if _gate_ok(cfg):
                s = stm100.settings(cfg, chn)
                got = {"THICKNESS_POLL_INTERVAL_S": s.poll_s, "THICKNESS_SLOW_POLL_S": s.slow_s, "THICKNESS_STALE_S": s.stale_s,
                       "THICKNESS_CMD_TIMEOUT_S": s.cmd_timeout_s}[key]
                assert math.isclose(got, float(v)), (p.key, v, got)
        for bad in _out_of_range(p):
            if bad is not None:
                try:
                    s = stm100.settings({**DEF, p.key: bad}, chn)
                except ConfigRangeError:
                    continue
                got = {"THICKNESS_POLL_INTERVAL_S": s.poll_s, "THICKNESS_SLOW_POLL_S": s.slow_s, "THICKNESS_STALE_S": s.stale_s,
                       "THICKNESS_CMD_TIMEOUT_S": s.cmd_timeout_s}[key]
                assert math.isclose(got, float(bad)), (p.key, bad, got)


def test_heater_limit_rejects_instead_of_fixing():
    """히터 목표 상한: v1 은 SV ≤ 0 → 500, MAX_TEMP 가 0 이하거나 SV 보다 크면 SV 로 고쳤다 → v2 는 거부."""
    from v2.ch12.recipe_check import heater_limit_c
    assert heater_limit_c({}) == 900.0 and heater_limit_c({"CH2_HEATER_SV_LIMIT_C": 600.0, "CH2_HEATER_MAX_TEMP": 550.0}) == 550.0
    for cfg in ({"CH2_HEATER_SV_LIMIT_C": 0.0}, {"CH2_HEATER_MAX_TEMP": 0.0},
                {"CH2_HEATER_SV_LIMIT_C": 500.0, "CH2_HEATER_MAX_TEMP": 900.0}):
        with pytest.raises(ConfigRangeError):
            heater_limit_c(cfg)

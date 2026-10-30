"""챔버12 웹 설정 내보내기 묶음(web_schema.py) — 웹이 이것 하나로 설정·명령·레시피·블록·스냅샷·챔버 화면을 그린다.

정의 내용이 바뀌면 PINNED 해시가 달라져 이 시험이 실패한다 — web_schema.EXPORT_VERSION 을 올리고 독스트링 히스토리에 적고
PINNED 를 (새 버전, 새 해시)로(챔버K 규칙). 파라미터 조각·4단계 commands·safety_rules·3단계 process_states 가 바뀌어도 바뀐다.
"""
import json

from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.ch12 import hub as H
from v2.ch12 import snapshot_fields as SF
from v2.ch12 import web_blocks as WB
from v2.ch12 import web_schema as W
from v2.ch12.params import PARAMS, table
from v2.ch12.sim.rig import Ch12Rig

PINNED = (1, "47b7e38996e5d78d3a5008916cf35a3004bbf41960e096b073c99e0d54c00ece")
TOP = ["version", "hash", "paramSchemaHash", "configVersion", "overriddenKeys", "params", "crossRules", "dropped", "commands",
       "preconditions", "reasons", "plcButtons", "recipe", "blocks", "adjust", "stopSequence", "states", "events",
       "eventKinds", "paramGroups", "snapshot", "chambers"]


def test_version_and_hash_pinned():
    assert (W.EXPORT_VERSION, W.content_hash()) == PINNED, (
        "웹 내보내기 정의가 바뀌었다 — EXPORT_VERSION 을 올리고(히스토리에 적고) PINNED 를 (새 버전, 새 해시)로: "
        f"{W.content_hash()}")


def test_bundle_shape_and_values():
    vals = dict(defaults(PARAMS), UPLINK_CLOCK_SKEW_SEC=9.0, UPLINK_TOKEN="tok-xyz", UPLINK_URL="https://h.example/p")
    b = W.bundle(vals, {"SHUTDOWN_STEP_GAP_MS": 700}, 7, stored=dict(vals, UPLINK_CLOCK_SKEW_SEC=5.0),
                 overridden=["UPLINK_CLOCK_SKEW_SEC"])
    raw = W.to_json(b)
    assert json.loads(raw)["hash"] == b["hash"] == W.content_hash()          # 현재값은 해시에 들어가지 않는다
    assert list(b) == TOP and b["configVersion"] == 7 and b["overriddenKeys"] == ["UPLINK_CLOCK_SKEW_SEC"]
    assert b["paramSchemaHash"] == table().schema_hash() == H.param_hash()
    by = {p["key"]: p for p in b["params"]}
    assert set(by) == {p.key for p in PARAMS}
    s = by["UPLINK_CLOCK_SKEW_SEC"]
    assert (s["current"], s["stored"], s["overridden"], s["default"]) == (9.0, 5.0, True, 5.0)
    assert by["SHUTDOWN_STEP_GAP_MS"]["pending"] == 700 and by["SHUTDOWN_STEP_GAP_MS"]["overridden"] is False
    assert {k for k, d in by.items() if d["webLocked"]} == {"RUN_MODE", "EQUIPMENT_ID", "UPLINK_IN_SIM"}
    for p in PARAMS:
        d = by[p.key]
        assert {"label", "group", "unit", "type", "min", "max", "default", "current", "stored", "safety", "apply",
                "scope", "role", "v1Source", "desc", "webLocked"} <= set(d)
        if p.scope.value == "secret":
            assert d["default"] is None and d["current"] is None and d["stored"] is None and d["pending"] is None
    assert "tok-xyz" not in raw and "h.example" not in raw
    assert {g["key"] for g in b["paramGroups"]} >= {p.group for p in PARAMS}         # 모든 묶음에 화면 이름
    assert len({g["key"] for g in b["paramGroups"]}) == len(b["paramGroups"])
    assert b["crossRules"] and b["dropped"] and all(set(d) == {"key", "reason", "v1Source", "replacedBy"} for d in b["dropped"])


def test_blocks_recipe_adjust_from_web_blocks():
    b = W.bundle(defaults(PARAMS))
    wb = WB.definitions()
    assert b["blocks"]["version"] == WB.WEB_BLOCKS_VERSION and b["blocks"]["cfg"] == wb["cfg"]
    assert b["blocks"]["defs"] == wb["blocks"]["defs"] and b["recipe"] == wb["recipe"]
    assert b["adjust"]["command"] == "PROCESS_ADJUST" and b["adjust"]["items"] == wb["blocks"]["adjust"]["items"]
    assert {e["kind"] for e in b["events"]} == {"run_start", "run_end"}
    kinds = [(e["source"], e["kind"]) for e in b["eventKinds"]]
    assert len(kinds) == len(set(kinds)) and {e["level"] for e in b["eventKinds"]} == {"main", "detail"}
    assert ("uplink", "uplink_down") in kinds and ("process", "run_start") in kinds and ("mfc", "disconnected") in kinds


def test_optional_parts_follow_modules():
    """4단계 commands·safety_rules, 3단계 process_states — 있으면 그 모듈의 정의, 없으면 빈 것(묶음 모양은 같다)."""
    b = W.bundle(defaults(PARAMS))
    C = H.optional_module("v2.ch12.commands")
    if C is None:
        assert b["commands"] == b["preconditions"] == b["reasons"] == b["plcButtons"] == []
    else:
        assert b["commands"]
    SR = H.optional_module("v2.ch12.safety_rules")
    assert isinstance(b["stopSequence"], dict) and (SR is not None or b["stopSequence"] == {"fixed": True, "steps": []})
    assert isinstance(b["states"], list)


def test_chambers_definitions_and_supports():
    d = W.definitions()["chambers"]
    assert [c["id"] for c in d] == ["ch1", "ch2", "pc", "tsp"] and all(c["supports"] is None for c in d)
    b = W.bundle(dict(defaults(PARAMS), CH2_SUPPORTS_HEATER=False))
    ch = {c["id"]: c for c in b["chambers"]}
    assert ch["ch1"]["gases"] == ["ar", "o2"] and "n2" in ch["ch2"]["gases"] and ch["ch2"]["heater"]
    assert ch["ch1"]["supports"]["dcp"] is True and ch["ch1"]["supports"]["dc"] is False
    assert ch["ch2"]["supports"]["heater"] is False and ch["ch2"]["supports"]["rf"] is True
    assert ch["pc"]["supports"] is None and ch["tsp"]["blocks"] == ["tsp", "tsp.ig"]
    assert ch["ch2"]["procPrefix"] == "ch2.proc." and "ch2.set.heater_c" in ch["ch2"]["targets"]
    blocks = {x for c in b["chambers"] for x in c["blocks"]}
    assert blocks <= set(SF.BLOCK_ORDER) and set(SF.BLOCK_ORDER) - blocks == {"plc", "camera"}
    cols = set(b["snapshot"]["columns"])
    assert all(t in cols for c in b["chambers"] for t in c["targets"])


def test_snapshot_definitions_match_hello_columns(tmp_path):
    """묶음 snapshot 의 열 집합 = hello.snapshotColumns(같은 순서) — 웹이 라벨·단위·형식·그래프를 열 이름으로 찾는다."""
    sn = W.bundle(defaults(PARAMS))["snapshot"]
    cols = [f["column"] for f in sn["tick"]] + [f["column"] for f in sn["process"]]
    for b in sn["blocks"]:
        cols += b["quality"]
        for f in b["fields"]:
            cols.append(f["column"])
            if f["ownQuality"]:
                cols += [f["column"] + ".q", f["column"] + ".age_s"]
    rig = Ch12Rig(VirtualClock(start_wall=1_790_000_000.0), tmp_path)
    try:
        hub = H.MessageHub(rig)
        assert cols == sn["columns"] == hub.hello_data()["snapshotColumns"] == list(rig.bus.columns)
    finally:
        rig.outputs.stop()
    assert [b["name"] for b in sn["blocks"]] == list(SF.BLOCK_ORDER)
    assert {b["name"]: b["worker"] for b in sn["blocks"]}["ch2.dc"] == "plc"
    by = {f["column"]: f for b in sn["blocks"] for f in b["fields"]}
    assert all("source" not in f for f in by.values()) and {f["type"] for f in by.values()} <= {"float", "int", "bool", "str"}
    proc = {f["column"]: f for f in sn["process"]}
    assert proc["ch1.proc.owner"]["enumLabels"] == {"robot": "로봇", "web": "웹", "local": "로컬"}
    assert all(f["stateColumn"] is None or f["stateColumn"] in sn["columns"] for f in by.values())


def test_every_param_reference_resolves_from_bundle_alone():
    """웹이 묶음만으로 범위를 계산할 수 있다 — 블록·수정·명령 인자의 paramMin·paramMax·cfg 키는 묶음 params 의 키."""
    b = W.bundle(defaults(PARAMS))
    cur = {p["key"] for p in b["params"]}
    refs = []
    for d in b["blocks"]["defs"]:
        refs += [(p, d["type"]) for p in d["params"]]
    refs += [(a["param"], a["key"]) for a in b["adjust"]["items"]]
    for c in b["commands"]:
        refs += [(a, c.get("kind")) for a in c.get("args", [])]
    for p, where in refs:
        for k in (p.get("paramMin"), p.get("paramMax")):
            assert k is None or k in cur, (where, k)
        dflt = p.get("default")
        if isinstance(dflt, str) and dflt.startswith("cfg:"):
            key = dflt[4:]
            assert key in cur or f"CH1_{key}" in cur or f"CH2_{key}" in cur, (where, dflt)

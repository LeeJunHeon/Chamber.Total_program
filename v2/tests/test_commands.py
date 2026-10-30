"""챔버12 명령 표(commands.py) — 모든 명령에 출처·상태·인자·대상, 설정만 바꿔 '공정 시작·레시피·설정은 포털에서만', 사유 표, 웹 묶음 모양.

v1 근거: 화면 버튼(ui/main_window.py·heater_panel·thickness_panel·factor/material 창)과 로봇 18개(host/setup.py) — 명령 목록이
그 조작을 빠짐없이 덮는지(이름으로) 확인한다.
"""
from v2.common.commands import ArgDef, Source
from v2.common.paramdef import defaults
from v2.ch12 import commands as C
from v2.ch12 import process_states as PS
from v2.ch12.drivers import plc_map as PM
from v2.ch12.params import PARAMS

CFG = defaults(PARAMS)
TYPES = {"float", "int", "bool", "str", "enum", "obj", "rows", "any"}
TARGETS = {"chamber", "pc", "tsp", "heater", "thickness", "shared", "system", "robot"}
STATE_NAMES = {s.name for s in PS.STATES} | {C.ANY}

# v1 화면 조작(버튼·입력) + v2 명령 — 하나라도 빠지면 실패
V1_UI = {
    "PROCESS_STOP": "ch1/ch2_Stop_button", "PROCESS_START": "ch1/ch2_Start_button", "RECIPE_LOAD": "processList(Select File)",
    "PC_START": "PC_Start_button", "PC_STOP": "PC_Stop_button", "PC_RECIPE_LOAD": "PC_processList_button",
    "PC_SELECT_CH": "PC 채널 라디오", "POWER_SELECT": "Power_Select_button", "RUNTIME_DUMP": "RuntimeDump_button",
    "TSP_START": "TSP_Start_button", "TSP_STOP": "TSP_Stop_button", "TSP_SCHEDULE": "TSP 예약",
    "PRESPUTTER_RESERVE": "preSputter Reserve", "PRESPUTTER_CANCEL": "preSputter Cancel", "PRESPUTTER_RECIPE": "Recipe",
    "HEATER_ONOFF": "히터 ON/OFF", "HEATER_APPLY": "히터 적용", "HEATER_RESET": "히터 리셋", "THICKNESS_ZERO": "두께 영점",
    "THICKNESS_FACTOR_SAVE": "Factor 저장", "THICKNESS_FACTOR_WRITE": "Factor 장비에 쓰기",
    "THICKNESS_MATERIALS_SAVE": "재료 창 저장", "THICKNESS_RELOAD": "Factor 다시 읽기", "LOG_CLEAR": "로그 Clear",
    "CHAMBER_ERROR_CLEAR": "오류 팝업 닫기", "PC_ERROR_CLEAR": "PC 오류 팝업 닫기", "PLC_BUTTON": "PLC 스위치",
}
V2 = ("ALL_STOP", "PROCESS_ADJUST", "BLOCK_SKIP", "PROCESS_PAUSE", "PROCESS_RESUME", "SEQUENCE_EDIT", "CONFIG_APPLY",
      "CONFIG_ROLLBACK", "RECIPE_CLEAR")


def test_every_command_has_sources_states_args_target():
    kinds = [c.kind for c in C.COMMANDS]
    assert len(kinds) == len(set(kinds))
    for c in C.COMMANDS:
        assert c.sources and all(isinstance(s, Source) for s in c.sources), c.kind
        assert c.states and set(c.states) <= STATE_NAMES, c.kind
        assert c.target in TARGETS, c.kind
        assert c.label and (c.v1 or c.role == "system"), c.kind
        for a in c.args:
            assert isinstance(a, ArgDef) and a.type in TYPES, (c.kind, a.name)
            assert a.type != "enum" or a.enum, (c.kind, a.name)
            for k in (a.param_min, a.param_max):
                assert k is None or k in CFG, (c.kind, k)
        if c.target == "chamber" and c.role != "system" or c.kind == "PROCESS_ABORT":
            assert c.args and c.args[0].name == "ch" and c.args[0].required, c.kind
    assert set(V1_UI) | set(V2) <= set(kinds)


def test_robot_commands_are_robot_only_and_match_v1_registration():
    from v2.ch12.robot_ops import COMMANDS as ROBOT
    assert C.ROBOT_COMMANDS == ROBOT and len(ROBOT) == 18
    robot = [c for c in C.COMMANDS if c.target == "robot"]
    assert [c.kind for c in robot] == [f"ROBOT_{x}" for x in ROBOT]
    assert all(c.sources == (Source.ROBOT,) for c in robot)
    assert all(Source.ROBOT not in c.sources for c in C.COMMANDS if c.target != "robot")
    for k in ("PROCESS_ADJUST", "BLOCK_SKIP", "PROCESS_PAUSE", "PROCESS_RESUME", "SEQUENCE_EDIT"):  # 지시 3장 (3) 로봇 제외
        assert set(C.get(k).sources) == {Source.WEB, Source.HMI}
    assert {c.kind for c in robot if c.bench_ok} == {f"ROBOT_{x}" for x in C.ROBOT_READS}


def test_local_all_default_and_portal_only_by_config():
    """기본(전환 기간) — 로컬 화면에서도 전부. COMMAND_SOURCES_LOCAL_ALL=false 만 바꾸면 공정 시작·레시피·설정은 포털에서만."""
    assert CFG[C.LOCAL_ALL_KEY] is True
    portal = {c.kind for c in C.COMMANDS if c.portal_only}
    assert {"PROCESS_START", "RECIPE_LOAD", "RECIPE_CLEAR", "SEQUENCE_EDIT", "PC_START", "TSP_START",
            "PRESPUTTER_RECIPE", "THICKNESS_FACTOR_SAVE", "THICKNESS_MATERIALS_SAVE"} <= portal
    on = C.kinds_for(Source.HMI, CFG)
    off = C.kinds_for(Source.HMI, dict(CFG, COMMAND_SOURCES_LOCAL_ALL=False))
    assert portal <= set(on) and not (portal & set(off))
    assert set(on) - set(off) == portal
    for k in ("PROCESS_STOP", "ALL_STOP", "PC_STOP", "TSP_STOP", "PLC_BUTTON", "PROCESS_ADJUST", "HEATER_ONOFF"):
        assert k in off                                   # 정지·조작은 늘 로컬에서도
    assert C.check_command("PROCESS_START", Source.HMI, "IDLE", dict(CFG, COMMAND_SOURCES_LOCAL_ALL=False)) == \
        "SOURCE_NOT_ALLOWED"
    assert C.check_command("PROCESS_START", Source.WEB, "IDLE", dict(CFG, COMMAND_SOURCES_LOCAL_ALL=False)) is None
    for k in ("CONFIG_APPLY", "CONFIG_ROLLBACK"):                  # 설정 판은 웹 묶음 기준 — 설정과 상관없이 웹만
        assert C.get(k).web_only and C.allowed_sources(C.get(k), CFG) == (Source.WEB,)


def test_check_command_and_args():
    assert C.check_command("NOPE", Source.WEB, "IDLE") == "UNKNOWN_COMMAND"
    assert C.check_command("ROBOT_4PIN_UP", Source.WEB, "IDLE") == "SOURCE_NOT_ALLOWED"
    assert C.check_command("PROCESS_START", Source.WEB, "RUNNING", CFG) == "STATE_NOT_ALLOWED"
    assert C.check_command("PROCESS_ADJUST", Source.HMI, "IDLE", CFG) == "STATE_NOT_ALLOWED"
    assert C.check_command("PROCESS_STOP", Source.HMI, "STOPPING", CFG) is None
    ck = lambda kind, a: C.check_args(C.get(kind), a, CFG)
    assert ck("PROCESS_STOP", {}) == "ch — 값 필요"
    assert ck("PROCESS_STOP", {"ch": 3}) == "ch 3 — 범위 1 ~ 2"
    assert ck("PROCESS_STOP", {"ch": 1.5}).endswith("정수여야 함")
    assert ck("PROCESS_STOP", {"ch": True}).endswith("숫자여야 함")
    assert ck("PROCESS_STOP", {"ch": 2}) is None
    assert ck("PLC_BUTTON", {"target": "nope", "on": True}).startswith("target 'nope'")
    assert ck("PLC_BUTTON", {"target": "vent_1_sw", "on": 1}).endswith("true/false 여야 함")
    assert ck("PLC_BUTTON", {"target": "vent_1_sw", "on": True}) is None
    assert ck("SEQUENCE_EDIT", {"ch": 1, "op": []}) == "op — 객체(dict)여야 함"
    assert ck("RECIPE_LOAD", {"ch": 1, "rows": {}}) == "rows — 목록이어야 함"
    assert ck("PROCESS_ADJUST", {"ch": 1, "device": "x", "item": "a", "value": 1}).startswith("device 'x'")
    assert ck("CONFIG_APPLY", {"version": 0, "values": {}, "schemaHash": "h"}) == "version 0 — 1 이상"


def test_plc_buttons_cover_every_v1_switch_coil():
    want = {n for n in PM.COIL_MAP if n.endswith("_SW") or n.startswith("SW_")}
    assert {b.name for b in C.PLC_BUTTONS} == want and len(want) > 40
    assert {b.target for b in C.PLC_BUTTONS} == {n.lower() for n in want}
    by = {b.name: b for b in C.PLC_BUTTONS}
    assert by["SW_POWER_SELECT"].chamber == "shared" and by["VENT_2_SW"].chamber == "ch2"
    assert by["L_PIN_UP_SW"].chamber == "loadlock" and "게이트 밸브" in by["G_V_1_OPEN_SW"].label


def test_reason_table_covers_engine_reasons():
    codes = C.reason_codes()
    assert len(codes) == len(C.ALL_REASONS)
    engine = {"COOLDOWN", "RUNNER_BUSY", "NO_RECIPE", "BUSY", "RECIPE_EMPTY", "RECIPE_INVALID", "E230", "PROCESS_RUNNING",
              "PROCESS_NOT_RUNNING", "CH1_PROCESS_RUNNING", "SKIP_NOT_ALLOWED", "PAUSE_NOT_ALLOWED", "PAUSE_NOT_AVAILABLE",
              "NOT_PAUSED", "ADJUST_NOT_USED", "ADJUST_OUT_OF_RANGE", "ADJUST_NOT_APPLICABLE", "EDIT_BAD_OP",
              "EDIT_STARTED_BLOCK", "EDIT_INVALID", "PULSE_PORT_BUSY", "PROCESS_FAILED", "PREFLIGHT_FAILED",
              "GATE_CHECK_FAILED", "MAIN_VALVE_CHECK_FAILED", "RECIPE_PARAM_ERROR", "CORE_NOT_READY", "INTERNAL_ERROR",
              "BENCH_READ_ONLY", "SOURCE_NOT_ALLOWED", "STATE_NOT_ALLOWED", "BAD_ARGS", "UNKNOWN_COMMAND", "LINK_DOWN",
              "USER_STOP", "ALL_STOP", "PROGRAM_EXIT", "PROGRAM_RESTART"}
    assert engine <= codes, engine - codes
    assert all(p.reason in codes for p in C.PRECONDITIONS)
    assert all(r.category in ("reject", "abort", "fail", "info") for r in C.ALL_REASONS)
    import re
    from v2.ch12 import errors as ER
    e_codes = [r.code for r in C.CH12_REASONS if re.fullmatch(r"E\d{3}", r.code)]
    assert e_codes == ["E230"] and ER.get_error_definition("E230").code == "E230"   # v1 E### 는 errors.py 그대로
    req = {p.id for p in C.PRECONDITIONS}
    assert all(set(c.requires) <= req for c in C.COMMANDS)


def test_public_bundle_contract_and_bench_ok():
    p = C.public()
    assert set(p) >= {"commands", "preconditions", "reasons", "plcButtons"}
    cmd = {c["kind"]: c for c in p["commands"]}
    assert set(cmd) == {c.kind for c in C.COMMANDS}
    x = cmd["PROCESS_START"]
    assert set(x) == {"kind", "label", "target", "sources", "portalOnly", "webOnly", "role", "states", "requires", "args",
                      "desc", "v1", "benchOk"}
    assert x["sources"] == ["web", "hmi"] and x["portalOnly"] is True and x["benchOk"] is False
    assert cmd["RECIPE_LOAD"]["benchOk"] and cmd["CONFIG_APPLY"]["benchOk"] and not cmd["PROCESS_STOP"]["benchOk"]
    assert cmd["ROBOT_GET_SPUTTER_STATUS"]["sources"] == ["robot"]
    assert {b["target"] for b in p["plcButtons"]} == {b.target for b in C.PLC_BUTTONS}
    assert all(isinstance(b["coil"], int) for b in p["plcButtons"])
    from v2.ch12 import web_schema as W
    part = W.commands_part()
    assert part["commands"] == p["commands"] and part["plcButtons"] == p["plcButtons"]

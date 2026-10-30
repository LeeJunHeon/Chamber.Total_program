"""챔버12 웹 검사 골든 — v2/tests/golden/web_ch12_*.json 이 지금 코드로 만든 것과 같은지(포털 lib/ops2/ch12 TS 확인용).

다르면 python -m v2.tools.make_web_golden_ch12 로 다시 만들고 --copy <포털> 로 포털 사본도 바꾼다(npm run ops2:golden).
또 꼬리표·값 사이 규칙(web_blocks.VALUE_RULES)·블록 종류·행 종류가 골든에 모두 나오는지, 정의(web_blocks.definitions)가
JSON 으로 그대로 나가고 코드 상수와 같은지 본다.
"""
import json

from v2.ch12 import blocks as BK
from v2.ch12 import process_steps as PS
from v2.ch12 import recipe_check as CK
from v2.ch12 import web_blocks as WB
from v2.tools import make_web_golden_ch12 as G

DATA = G.build()


def test_golden_files_match_current_code():
    for name in G.FILES:
        have = (G.GOLD / name).read_text(encoding="utf-8")
        assert have == G.to_json(DATA[name]), f"{name} 이 지금 코드와 다름 — python -m v2.tools.make_web_golden_ch12 로 다시 만들 것"


def test_build_is_deterministic():
    assert all(G.to_json(G.build()[n]) == G.to_json(DATA[n]) for n in G.FILES)


def test_cfg_head_is_what_checks_read():
    track: dict = {}
    G.build(track)
    full = G._defaults()
    for name in G.FILES:
        assert set(DATA[name]["cfg"]) == {k for k in track[name] if k in full}
    keys = {k["key"] for k in WB.CFG_KEYS} | {k["key"] for k in WB.CFG_PLAIN_KEYS}
    for k in keys:                                                        # 정의 cfg 표의 키는 v2 파라미터 표에 있는 이름
        assert k in full or f"CH1_{k}" in full or f"CH2_{k}" in full or k.startswith("SUPPORTS_"), k


def test_block_cases_shape_and_cover():
    d = DATA[G.FILES[0]]
    cases = d["blockCases"] + d["editCases"]
    assert len({c["name"] for c in cases}) == len(cases)
    for c in d["blockCases"]:
        assert (c["clean"] is None) == bool(c["errors"]), c["name"]
        assert (c["autoEndStop"] is None) == (c["clean"] is None), c["name"]
    for c in d["editCases"]:
        assert c["ok"] == (c["result"] is not None) == (c["reason"] == ""), c["name"]
        assert c["rev2"] == (c["rev"] + 1 if c["ok"] else c["rev"]), c["name"]
    assert {t for c in cases for t in c["covers"]} == set(G.TAGS) | {r["id"] for r in WB.VALUE_RULES}
    seen = {b["type"] for c in d["blockCases"] if c["clean"] for b in c["clean"]}
    assert seen == {x.type for x in BK.BLOCK_DEFS}                       # 통과 경우만으로 블록 종류 전부
    assert any(c["warnings"] and not c["errors"] for c in d["blockCases"])
    assert any(c["warnings"] and c["errors"] for c in d["blockCases"])
    assert {c["reason"] for c in d["editCases"]} == {"", *WB.definitions()["blocks"]["edit"]["reasons"]}


def test_value_rules_each_fire():
    """규칙 id 마다 그 꼬리표 경우에서 실제로 그 문구가 나온다(오류 또는 경고)."""
    d = DATA[G.FILES[0]]
    marks = {"chamber_only": "에서는 쓸 수 없음", "needs_cap": "꺼져 있음(SUPPORTS_", "gas_flow_blank": "비움 → 0 sccm",
             "ch1_no_n2": "CH1 은 N2 라인 없음", "shutter_gun_exists": "없는 건 셔터", "power_cap": "미지원",
             "pulse_exclusive": "동시에 사용할 수 없습니다", "rfp_freq_range": "rfp_freq_khz: 허용 범위",
             "dcp_off_range": "DC Pulse Off Time", "heater_limit": "°C", "main_change_together": "함께 넣어야 함",
             "heater_wait_target": "펼치기 실패: 히터 도달 대기"}
    assert set(marks) == {r["id"] for r in WB.VALUE_RULES}
    for rid, m in marks.items():
        pool = [x for c in d["blockCases"] + d["editCases"] if rid in c["covers"] for x in c["errors"] + c["warnings"]]
        assert any(m in x for x in pool), rid


def test_template_cases_kinds():
    cases = DATA[G.FILES[1]]["cases"]
    assert len({c["name"] for c in cases}) == len(cases) >= 435
    kinds = {c["kind"] for c in cases}
    assert kinds == set(G.TEMPLATE_KINDS) | {None}
    assert any(c["exception"] == "OverflowError" for c in cases)
    for c in cases:
        if c["kind"] in ("process", "test"):
            assert c["blocks"] and all(b["row"] == 1 for b in c["blocks"]) and c["normErrors"] is not None, c["name"]
        if c["kind"] == "process":
            assert c["blocks"][-1]["type"] == "end_stop", c["name"]
        if c["kind"] == "error":
            assert c["error"].startswith("E844: ") and c["blocks"] == [], c["name"]
    # v1 이 통과시킨 행(시작 전 값 검사 오류 없음)의 템플릿 블록은 블록 검사도 통과
    for c in cases:
        if c["kind"] in ("process", "test") and not c["normErrors"]:
            cfg = G._cfg(c["cfgOver"])
            try:
                BK.expand(c["blocks"], cfg, c["ch"])
            except ZeroDivisionError:                                     # v1 도 예외(Thickness 모드 dep_rate 없음 — 골든 combos)
                continue
            assert not BK.check_blocks(c["blocks"], cfg, c["ch"])[1], c["name"]


def test_recipe_check_cases():
    d = DATA[G.FILES[2]]
    assert len(d["rawCases"]) >= 20 and len(d["normCases"]) >= 51
    assert any(c["errors"] and c["errors"][-1].startswith("... 외 ") for c in d["rawCases"])
    assert any(len(c["errors"]) == CK.MAX_SHOWN for c in d["rawCases"])
    for c in d["rawCases"]:
        assert (c["message"] == "") == (not c["errors"]) and c["message"].startswith(("", CK.RECIPE_ERROR_PREFIX))


def test_definitions_json_and_constants():
    defs = WB.definitions()
    assert json.loads(json.dumps(defs, ensure_ascii=False, allow_nan=False)) == defs
    assert WB.content_hash(defs) == WB.content_hash()
    b = defs["blocks"]
    assert [x["type"] for x in b["defs"]] == [x.type for x in BK.BLOCK_DEFS]
    assert b["blockKeys"] == list(BK.BLOCK_KEYS) and b["limits"] == {"labelMax": BK.LABEL_MAX, "maxBlocks": BK.MAX_BLOCKS}
    end = next(x for x in b["defs"] if x["type"] == "end_stop")
    assert end["params"][0]["enum"] == [p["id"] for p in b["shutdownParts"] if p["skippable"]]
    assert b["endStopMustKeep"] == [p["id"] for p in b["shutdownParts"] if not p["skippable"]]
    sd = defs["cfg"]["stepCfgDefaults"]
    assert sd["1"]["gun_shutters"] == [] and sd["2"]["gun_shutters"] == ["G1", "G2", "G3"]
    for k in WB.CFG_KEYS:
        if k["field"]:
            assert k["field"] in sd["1"], k
    assert PS.step_cfg({}, 2).power_select_settle_ms == sd["2"]["power_select_settle_ms"]
    raw = defs["recipe"]["raw"]
    assert raw["floatKeys"] == list(CK.RAW_FLOAT_KEYS) and raw["maxShown"] == CK.MAX_SHOWN
    for name in G.FILES:
        assert DATA[name]["defs"] == defs and DATA[name]["defsHash"] == WB.content_hash()

"""v1 설정 변환 도구 시험(v2/tools/convert_v1_config.py) — 실제 v1 config/settings.json 전부 처리, 모르는 키 거부,
v1 과 같은 형식 규칙(bool·숫자 섞임 거부), 채널 값은 CH1_/CH2_ 로만(공통 값 복사 없음), 두께 json → v2 데이터 파일.

v1 파일은 읽기만 한다. 장비 조각이 아직 정의하지 않은 키는 '보류'로 출력만(-s).
"""
import json
from pathlib import Path

import pytest

from v2.ch12.params import table
from v2.ch12.params import ownership as own
from v2.common.config_store import ConfigStore
from v2.tools import convert_v1_config as C

ROOT = Path(__file__).resolve().parents[2]
V1_SETTINGS = ROOT / "config" / "settings.json"


def _leaves(data: dict, conv: C.Converter) -> set:
    """settings.json 의 키 경로(묶음은 하위 키까지 — 변환 도구와 같은 펼침)."""
    out = set()
    for sec, sd in data.items():
        for k, v in sd.items():
            path = f"{sec}.{k}"
            subs = [f"{path}.{s}" for s in v] if isinstance(v, dict) else []
            if subs and path not in conv.param_of and (
                    path not in conv.dropped_of or any(s in conv.param_of or s in conv.dropped_of for s in subs)):
                if path in conv.dropped_of:
                    out.add(path)
                out |= set(subs)
            else:
                out.add(path)
    return out


def _v1(tmp_path: Path, settings: dict, *, raw: str = None) -> Path:
    root = tmp_path / "v1"
    (root / "config").mkdir(parents=True, exist_ok=True)
    (root / "config" / "settings.json").write_text(raw if raw is not None else json.dumps(settings, ensure_ascii=False),
                                                   encoding="utf-8")
    return root


def _conv(tmp_path, settings, **kw):
    root = _v1(tmp_path, settings, raw=kw.pop("raw", None))
    return C.convert(root / "config" / "settings.json", root, **kw)


# ───────────── 실제 v1 settings.json ─────────────
def test_real_v1_settings_every_key_handled(tmp_path):
    res = C.convert(V1_SETTINGS, ROOT, allow_pending=True)
    assert res.errors == []
    conv = C.Converter()
    data = C._plain(C.read_json_v1(V1_SETTINGS))
    leaves = _leaves(data, conv)
    done = ({c["path"] for c in res.converted} | {d["path"] for d in res.dropped} | {p["path"] for p in res.pending})
    assert leaves <= done, sorted(leaves - done)
    assert leaves <= set(own.V1_KEY_OWNER)                    # 실제 파일의 키는 전부 담당 표에 있다
    print(f"\n실제 settings.json: 키 {len(leaves)} · " + " · ".join(f"{k} {v}" for k, v in res.summary().items()))
    if res.pending:
        print("보류(담당 조각이 아직 정의 안 함):", sorted(p["path"] for p in res.pending))
    for d in res.dropped:
        print("  버림", d["path"], "—", d["reason"])
    for c in res.confirm:
        print("  확인", c)
    # 변환된 값은 모두 그 키의 형식·범위를 통과하고, 채널 값은 CH1_/CH2_ 키로만 갔다
    by_key = table().by_key()
    for c in res.converted:
        sec = c["path"].split(".", 1)[0]
        assert c["key"].startswith(f"{sec.upper()}_") == (sec in ("ch1", "ch2")), c
        assert C.check_value(by_key[c["key"]], c["v2"]) is None

    out = tmp_path / "v2cfg"
    C.write(res, out)
    assert res.errors == [] and res.store_status in ("applied", "pending_restart", "pending_idle")
    t = table()
    st = ConfigStore(out, t.params, t.rules, t.schema_hash())
    vals = st.load()
    assert st.version == 1 and st.source == "active"
    for c in res.converted:
        assert vals[c["key"]] == c["v2"]
    assert (out / "thickness_factors_ch2.json").exists() and (out / "thickness_materials.json").exists()
    mats = json.loads((out / "thickness_materials.json").read_text(encoding="utf-8"))["materials"]
    assert len(mats) == 99 and mats[0]["name"] == "Aluminum"
    rep = json.loads((out / C.REPORT).read_text(encoding="utf-8"))
    assert rep["summary"]["converted"] == len(res.converted)
    again = C.write(C.convert(V1_SETTINGS, ROOT, allow_pending=True), out)     # 덮어쓰기는 --force 로만
    assert any("이미 v2 설정" in e for e in again.errors)


def test_real_v1_settings_without_pending_flag():
    """보류가 있으면 --allow-pending 없이는 오류(조용히 버리지 않음). 보류가 없으면 그대로 통과."""
    res = C.convert(V1_SETTINGS, ROOT)
    pend = C.convert(V1_SETTINGS, ROOT, allow_pending=True).pending
    assert bool(res.errors) == bool(pend)


# ───────────── 모르는 키·구역·형식 ─────────────
@pytest.mark.parametrize("settings,msg", [
    ({"common": {"NOPE_KEY": 1}}, "모르는 키"),
    ({"common": {"PROCESS_GUN_SHUTTERS": []}}, "모르는 키"),          # 채널 모듈에만 있는 이름(v1 로더도 거부)
    ({"commn": {"DEBUG_PRINT": True}}, "알 수 없는 구역"),
    ({"common": [1, 2]}, "묶음이어야"),
    ({"common": {"DEBUG_PRINT": 1}}, "형식 불일치"),                    # bool 자리에 숫자
    ({"common": {"SHUTDOWN_STEP_TIMEOUT_MS": True}}, "형식 불일치"),     # 숫자 자리에 bool
    ({"common": {"SHUTDOWN_STEP_TIMEOUT_MS": "2500"}}, "형식 불일치"),
    ({"common": {"SHUTDOWN_STEP_TIMEOUT_MS": None}}, "형식 불일치"),
    ({"common": {"SHUTDOWN_STEP_TIMEOUT_MS": 2500.5}}, "정수 키에 소수"),
    ({"ch2": {"HEATER_WD_PERIOD_S": 20.0}}, "> 최대"),                    # v1 은 1~9 로 클램프 — v2 는 거부
    ({"ch2": {"HEATER_HOLD_MODE": "DAC"}}, "허용값"),
    ({"common": {"LOCAL_FALLBACK_ROOT": "C:\\x"}}, "바꿀 수 없는 항목"),
    ({"common": {"DEV_MODE": True}}, "옮길 곳이 없다"),                  # 뺀 키인데 v1 기본과 다른 값
    ({"common": {"ROBOT_RECIPE_FOLDERS": "CH1"}}, "형식 불일치"),
])
def test_rejected(tmp_path, settings, msg):
    res = _conv(tmp_path, settings)
    assert not res.ok and any(msg in e for e in res.errors), res.errors
    out = tmp_path / "out"
    C.write(res, out)
    assert not out.exists()                                           # 오류가 있으면 아무것도 쓰지 않는다


@pytest.mark.parametrize("raw,msg", [
    ('{"common": {"DEBUG_PRINT": true, "DEBUG_PRINT": false}}', "두 번"),
    ('{"common": {}, "common": {}}', "두 번"),
    ('[1, 2]', "{ } 로 감싸져"),
    ('{"common": {"DEBUG_PRINT": true,}}', "줄 1 칸 32: "),                  # v1 힌트 문구(_JSON_HINTS) + 원본 줄
    ('{"common": {"LOG_ROOT_DIR": "C:\\x"}}', "역슬래시(\\)는 두 번 써야 합니다"),
])
def test_rejected_raw(tmp_path, raw, msg):
    res = _conv(tmp_path, None, raw=raw)
    assert any(msg in e for e in res.errors), res.errors


def test_jsonc_bom_comments(tmp_path):
    raw = ("\ufeff{\n  // 줄 주석 \"따옴표\"\n  /* 블록\n 주석 */\n  \"__about\": \"설명\",\n  \"common\": {\n"
           "    \"__note\": 1,\n    \"INVENTORY_API_URL\": \"https://x.example/api\", // 문자열 안 // 는 보존\n"
           "    \"SHUTDOWN_STEP_GAP_MS\": 500.0\n  }\n}\n")
    res = _conv(tmp_path, None, raw=raw)
    assert res.ok, res.errors
    assert res.values["INVENTORY_API_URL"] == "https://x.example/api"
    assert res.values["SHUTDOWN_STEP_GAP_MS"] == 500 and isinstance(res.values["SHUTDOWN_STEP_GAP_MS"], int)
    assert {"__about", "common.__note"} <= {d["path"] for d in res.dropped}


# ───────────── 채널 값·v1 실효값 ─────────────
def test_channel_values_go_to_channel_keys_only(tmp_path):
    res = _conv(tmp_path, {"common": {"PC_RF_PULSE_POST_ON_DELAY_MS": 20000, "SUPPORTS_DC": True},
                           "ch1": {"SUPPORTS_DC": False}, "ch2": {"PC_RF_PULSE_POST_ON_DELAY_MS": 30000}})
    assert res.ok, res.errors
    v = res.values
    assert v["PC_RF_PULSE_POST_ON_DELAY_MS"] == 20000 and v["CH2_PC_RF_PULSE_POST_ON_DELAY_MS"] == 30000
    assert v["SUPPORTS_DC"] is True and v["CH1_SUPPORTS_DC"] is False
    assert "CH1_PC_RF_PULSE_POST_ON_DELAY_MS" not in v and "CH2_SUPPORTS_DC" not in v       # 공통 값을 복사하지 않는다


def test_dead_common_value_becomes_v1_effective_value(tmp_path):
    """⚠ v1 은 common 구역 값을 채널에 전달하지 않았다 — v1 이 실제로 쓰던 .py 값으로 옮기고 사용자 확인에 적는다."""
    res = _conv(tmp_path, {"common": {"PC_POWER_OFF_TIMEOUT_MS": 100000, "ETA_ENABLED": False}})
    assert res.ok, res.errors
    assert res.values["PC_POWER_OFF_TIMEOUT_MS"] == 240000
    assert any("PC_POWER_OFF_TIMEOUT_MS" in c and "240000" in c for c in res.confirm)
    assert "common.ETA_ENABLED" in {d["path"] for d in res.dropped}            # 두 채널 모두 자기 키 — 버림


def test_list_value_to_json_text(tmp_path):
    res = _conv(tmp_path, {"common": {"ROBOT_RECIPE_FOLDERS": ["CH1", "ALD"]}, "ch2": {"PROCESS_GUN_SHUTTERS": ["G1"]}})
    assert res.ok, res.errors
    assert json.loads(res.values["ROBOT_RECIPE_FOLDERS"]) == ["CH1", "ALD"]
    assert json.loads(res.values["CH2_PROCESS_GUN_SHUTTERS"]) == ["G1"]


def test_pending_without_definition_is_error(tmp_path):
    """담당 조각이 정해졌지만 정의가 없는 키 — 보류(--allow-pending) 아니면 오류."""
    t = table()
    core_only = type(t)(tuple(p for p in t.params if t.origin[p.key] == "core"), (), t.dropped, t.origin)
    root = _v1(tmp_path, {"common": {"DCP_GAP_MS": 150}})
    res = C.convert(root / "config" / "settings.json", root, table=core_only)
    assert any("DCP_GAP_MS" in e and "아직 정의 없음" in e for e in res.errors)
    res2 = C.convert(root / "config" / "settings.json", root, table=core_only, allow_pending=True)
    assert res2.ok and res2.pending == [{"path": "common.DCP_GAP_MS", "owner": own.owner_of("common.DCP_GAP_MS")}]


# ───────────── 두께 json → v2 데이터 파일 ─────────────
def test_thickness_files(tmp_path):
    root = _v1(tmp_path, {"ch2": {"THICKNESS_FACTOR_FILE": "config/thickness_factors_ch2.json"}})
    (root / "config" / "thickness_factors_ch2.json").write_text(json.dumps({
        "version": 2,
        "tooling": {"up": {"G1": 105.5, "G2": 500, "G3": True}, "mid": {"G1": "", "G2": None, "G3": "88"}, "down": [1]},
        "material": {"G1": {"material": " Gold "}, "G2": {"density": 2.0, "z_factor": 1.0}, "G3": {"material": ""}},
    }), encoding="utf-8-sig")
    (root / "config" / "thickness_materials.json").write_text(json.dumps({"version": 1, "materials": [
        {"name": "Gold", "symbol": "Au", "density": 19.3, "z_factor": 0.381},
        {"name": "gold", "symbol": "Au", "density": 19.3, "z_factor": 0.381},
        {"name": "Bad", "symbol": "B", "density": 200, "z_factor": 1.0},
        {"name": "Note", "symbol": "N", "density": "2.5", "z_factor": 1, "note": " 메모 "}]}), encoding="utf-8")
    res = C.convert(root / "config" / "settings.json", root)
    assert res.ok, res.errors
    assert res.values["CH2_THICKNESS_FACTOR_FILE"] == ""
    fac = res.files["thickness_factors_ch2.json"]
    assert fac["tooling"]["up"] == {"G1": 105.5, "G2": None, "G3": None}
    assert fac["tooling"]["mid"] == {"G1": None, "G2": None, "G3": 88.0}
    assert fac["material"] == {"G1": {"material": "Gold"}, "G2": {"material": ""}, "G3": {"material": ""}}
    mats = res.files["thickness_materials.json"]["materials"]
    assert [m["name"] for m in mats] == ["Gold", "Note"] and mats[1] == {"name": "Note", "symbol": "N", "density": 2.5,
                                                                          "z_factor": 1.0, "note": "메모"}
    reasons = " ".join(d["reason"] for d in res.dropped)
    for w in ("tooling[up][G2]", "tooling[up][G3]", "tooling.down", "이전 형식", "이름 중복", "density 200 범위"):
        assert w in reasons, w
    assert "CH1 Factor 파일 없음" in reasons
    out = tmp_path / "out"
    C.write(res, out)
    assert json.loads((out / "thickness_factors_ch2.json").read_text(encoding="utf-8")) == fac


def test_thickness_custom_path_and_unreadable(tmp_path):
    other = tmp_path / "elsewhere.json"
    other.write_text("{깨진", encoding="utf-8")
    res = _conv(tmp_path, {"ch1": {"THICKNESS_FACTOR_FILE": str(other)}})
    assert any("Factor 파일을 읽을 수 없음" in e for e in res.errors)
    assert any("ch1.THICKNESS_FACTOR_FILE" in c for c in res.confirm)


def test_cli(tmp_path, capsys):
    root = _v1(tmp_path, {"common": {"SHUTDOWN_STEP_GAP_MS": 400}})
    out = tmp_path / "out"
    assert C.main(["--v1-root", str(root), "--out", str(out)]) == 0
    assert "변환 1" in capsys.readouterr().out and (out / "config_active.json").exists()
    assert C.main(["--v1-root", str(root), "--out", str(out)]) == 1                 # 이미 있음
    assert C.main(["--v1-root", str(root), "--out", str(out), "--force"]) == 0
    bad = _v1(tmp_path / "b", {"common": {"OOPS": 1}})
    assert C.main(["--v1-root", str(bad), "--out", str(tmp_path / "o2")]) == 1
    assert not (tmp_path / "o2").exists()

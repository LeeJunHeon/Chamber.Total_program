"""두께 Factor·재료 파일(설정 폴더 데이터 파일, 결정 D12)과 Factor·재료 창 명령의 검사·문구 —
v1 tests/test_thickness_factors.py·test_thickness_materials.py·test_thickness_panel.py(창 부분)를 v2 로 옮긴 것(v1 import 없음).

파일 읽기 규칙·기본 99종·배포 파일 내용은 v1 을 실제로 돌린 골든(v2/tools/v1golden_thickness.py → golden/thickness.json),
창 검사 기대값은 v1 시험 값 그대로(근거 v1 파일:줄을 각 시험에).
"""
import json
from pathlib import Path

import pytest

from v2.ch12 import thickness_factors as TF
from v2.ch12 import thickness_materials as TM
from v2.ch12.thickness_factors import CHUCKS, GUNS, ThicknessFactors, load_factors, save_factors
from v2.ch12.thickness_materials import Material, default_materials, find_material, load_materials, save_materials

G = json.loads((Path(__file__).parent / "golden" / "thickness.json").read_text(encoding="utf-8"))["files"]


def _names(p: Path):
    return sorted(x.name for x in p.iterdir())


# ═══════════════════════ Factor 파일(v1 test_thickness_factors.py) ═══════════════════════
def test_missing_file_is_blank_table(tmp_path):
    fac, warns = load_factors(tmp_path / "없는_파일_thickness.json")
    assert warns == [] and all(fac.tooling[c][g] is None for c in CHUCKS for g in GUNS)
    assert all(fac.material[g] is None for g in GUNS)


def test_shipped_v1_ch2_file_is_blank_and_valid(tmp_path):
    """v1 배포 파일 config/thickness_factors_ch2.json(골든에 내용 그대로) — 빈 표·경고 없음. v2 저장 형식도 같은 글자."""
    p = tmp_path / "thickness_factors_ch2.json"
    p.write_text(json.dumps(G["factors_ch2_shipped"]), encoding="utf-8")
    fac, warns = load_factors(p)
    assert warns == [] and all(fac.tooling[c][g] is None for c in CHUCKS for g in GUNS)
    assert fac.to_json() == G["factors_ch2_shipped"]


@pytest.mark.parametrize("case", sorted(G["load_factors"]))
def test_load_factors_same_as_v1(tmp_path, case):
    g = G["load_factors"][case]
    p = tmp_path / f"{case}.json"
    p.write_text(g["text"], encoding="utf-8")
    fac, warns = load_factors(p)
    assert fac.tooling == g["tooling"] and fac.material == g["material"]
    assert [w.replace(str(tmp_path), "<tmp>") for w in warns] == g["warns"]


def test_invalid_values_only_that_cell_unset(tmp_path):
    """v1 :37-57 — 칸만 미설정, 경고 6건, 대소문자 무시 조회."""
    p = tmp_path / "f.json"
    p.write_text(G["load_factors"]["invalid"]["text"], encoding="utf-8")
    fac, warns = load_factors(p)
    assert fac.tooling["up"]["G1"] == 120.5 and fac.material == {"G1": "Aluminum Oxide", "G2": None, "G3": None}
    assert len(warns) == 6 and any("이전 형식" in w for w in warns)
    assert fac.tooling_for("UP", "g1") == 120.5 and fac.material_name("g1") == "Aluminum Oxide"


def test_check_tooling_same_as_v1():
    for v_repr, want in G["check_tooling"]:
        v = eval(v_repr, {"nan": float("nan")})                  # 골든 입력은 repr 글자(None·'abc'·True·nan …)
        got = list(TF.check_tooling(v))
        assert got == want or (want[0] is None and got[0] is None and got[1] == want[1]), (v_repr, got, want)


def test_atomic_save_roundtrip_korean_path(tmp_path):
    p = tmp_path / "한글 폴더 & 설정" / "thickness_factors_ch2.json"
    fac = ThicknessFactors()
    fac.tooling["mid"]["G2"] = 98.5
    fac.material["G2"] = "Titanium"
    save_factors(p, fac)
    back, warns = load_factors(p)
    assert warns == [] and back.tooling["mid"]["G2"] == 98.5 and back.material == {"G1": None, "G2": "Titanium", "G3": None}
    assert json.loads(p.read_text(encoding="utf-8"))["material"]["G2"] == {"material": "Titanium"}
    assert _names(p.parent) == [p.name]
    assert p.read_bytes().endswith(b"}\n") and b"\r\n" not in p.read_bytes()      # LF, 끝 줄바꿈(v1 newline="\n")


def test_atomic_save_failure_keeps_original(tmp_path, monkeypatch):
    p = tmp_path / "f.json"
    fac = ThicknessFactors()
    fac.tooling["up"]["G1"] = 100.0
    save_factors(p, fac)
    before = p.read_bytes()

    def boom(*_a, **_k):
        raise OSError("disk full")
    monkeypatch.setattr(TM.os, "replace", boom)
    fac.tooling["up"]["G1"] = 200.0
    with pytest.raises(OSError):
        save_factors(p, fac)
    assert p.read_bytes() == before and _names(tmp_path) == ["f.json"]


def test_factor_file_location_in_settings_dir(tmp_path):
    """CHn_THICKNESS_FACTOR_FILE "" = 설정 폴더/thickness_factors_chN.json, 상대 = 설정 폴더 기준, 절대 = 그대로."""
    assert TF.factor_file({}, "ch2", tmp_path) == tmp_path / "thickness_factors_ch2.json"
    assert TF.factor_file({"CH1_THICKNESS_FACTOR_FILE": ""}, "ch1", tmp_path) == tmp_path / "thickness_factors_ch1.json"
    assert TF.factor_file({"CH2_THICKNESS_FACTOR_FILE": "sub/f.json"}, "ch2", tmp_path) == tmp_path / "sub" / "f.json"
    a = tmp_path / "abs.json"
    assert TF.factor_file({"CH2_THICKNESS_FACTOR_FILE": str(a)}, "ch2", "/elsewhere") == a
    assert TM.materials_file(tmp_path) == tmp_path / "thickness_materials.json"


# ═══════════════════════ 재료 목록(v1 test_thickness_materials.py) ═══════════════════════
def test_default_list_same_as_v1_manual_table():
    m = default_materials()
    assert [vars(x) for x in m] == G["materials_default"]
    assert len(m) == 99 and len({x.name.casefold() for x in m}) == 99
    z_missing = [x for x in m if x.note == TM.Z_MISSING_NOTE]
    assert len(z_missing) == 18 and all(x.z_factor == 1.0 for x in z_missing)
    al = find_material(m, "aluminum")
    assert (al.symbol, al.density, al.z_factor, al.label) == ("Al", 2.73, 1.08, "Al — Aluminum")
    assert find_material(m, "Neodymium Fluoride").density == 6.506
    assert find_material(m, "Carbon (Graphite)").z_factor == 3.26 and find_material(m, "Gold").density == 19.3


def test_default_file_same_as_v1_shipped_file(tmp_path):
    """없으면 만드는 파일 = v1 배포 config/thickness_materials.json 과 같은 내용(골든)."""
    p = tmp_path / "한글 폴더" / "thickness_materials.json"
    mats, warns = load_materials(p)
    assert warns == [] and len(mats) == 99 and p.exists() and _names(p.parent) == [p.name]
    assert json.loads(p.read_text(encoding="utf-8")) == G["materials_shipped"]
    back, w2 = load_materials(p)
    assert w2 == [] and [x.name for x in back] == [x.name for x in mats]


@pytest.mark.parametrize("case", sorted(G["load_materials"]))
def test_load_materials_same_as_v1(tmp_path, case):
    g = G["load_materials"][case]
    p = tmp_path / f"m_{case}.json"
    p.write_text(g["text"], encoding="utf-8")
    mats, warns = load_materials(p)
    assert [m.name for m in mats] == g["names"] and [m.note for m in mats] == g["notes"]
    assert [w.replace(str(tmp_path), "<tmp>") for w in warns] == g["warns"]
    if case == "broken":
        assert p.read_text(encoding="utf-8") == "{ broken"          # 읽을 수 없으면 파일은 그대로


def test_check_material_fields_same_as_v1():
    for args, want_m, want_e in G["check_material"]:
        a = [eval(x) for x in args]
        m, e = TM.check_material_fields(*a)
        assert (None if m is None else vars(m)) == want_m and e == want_e, args


def test_save_rejects_duplicates_and_is_atomic(tmp_path, monkeypatch):
    p = tmp_path / "m.json"
    save_materials(p, [Material("A", "a", 1.0, 1.0)])
    before = p.read_bytes()
    with pytest.raises(ValueError):
        save_materials(p, [Material("A", "a", 1.0, 1.0), Material("a", "b", 2.0, 1.0)])
    assert p.read_bytes() == before

    def boom(*_a, **_k):
        raise OSError("disk full")
    monkeypatch.setattr(TM.os, "replace", boom)
    with pytest.raises(OSError):
        save_materials(p, [Material("B", "b", 1.0, 1.0)])
    assert p.read_bytes() == before and _names(tmp_path) == ["m.json"]


# ═══════════════════════ 창 명령(v1 test_thickness_panel.py:160-345) ═══════════════════════
def _materials(tmp):
    mp = Path(tmp) / "materials.json"
    save_materials(mp, [Material("Aluminum", "Al", 2.73, 1.08), Material("Aluminum Oxide", "Al2O3", 3.97, 1.0, "매뉴얼 Z 미기재"),
                        Material("Titanium", "Ti", 4.5, 0.628)])
    return mp


def test_factor_window_dropdown_values_and_save(tmp_path):
    """v1 test_factor_dialog_material_dropdown_and_save — 드롭다운·ρ/Z 표시·틀린 칸 2건·저장 내용·다시 열기."""
    mats, _w = load_materials(_materials(tmp_path))
    path = tmp_path / "한글" / "thickness_factors_ch2.json"
    win = TF.factor_window(path, mats, ch=2)
    ch = win["choices"]
    assert ch[0] == ("(선택 안 함)", "") and len(ch) == 4 and ch[2][0] == "Al2O3 — Aluminum Oxide"
    assert win["values"]["G1"] == "—" and win["title"] == "CH2 Thickness Monitor — Factor 설정"
    assert TF.value_text("Aluminum Oxide", mats, "G1") == "ρ 3.97 · Z 1.000"
    assert TF.value_text("Titanium", mats, "G2") == "ρ 4.5 · Z 0.628" and TF.value_text("titanium", mats, "G2") == "ρ 4.5 · Z 0.628"
    assert TF.value_text("Ti — Titanium", mats, "G2") == "ρ 4.5 · Z 0.628"
    assert TF.value_text("Unobtainium", mats, "G3") == "목록에 없음"
    cells = {c: {g: "" for g in GUNS} for c in CHUCKS}
    cells["up"]["G1"], cells["mid"]["G2"] = "120", "5"
    texts = {"G1": "Al2O3 — Aluminum Oxide", "G2": "Titanium", "G3": "Unobtainium"}
    fac, errs, bad = TF.collect(cells, texts, mats)
    assert len(errs) == 2 and any("Unobtainium" in e for e in errs) and "tooling.mid.G2" in bad
    assert errs[0] == "tooling Mid/G2: 5 범위(10~399%) 밖"
    ok, text, _bad = TF.save_window(path, cells, texts, mats)
    assert not ok and text.startswith("고칠 칸이 있어 저장하지 않았습니다:\n\n") and not path.exists()
    cells["mid"]["G2"], texts["G3"] = "", "(선택 안 함)"
    assert TF.save_window(path, cells, texts, mats) == (True, "", [])
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["tooling"]["up"]["G1"] == 120.0 and data["tooling"]["mid"]["G2"] is None
    assert data["material"] == {"G1": {"material": "Aluminum Oxide"}, "G2": {"material": "Titanium"}, "G3": {"material": ""}}
    win2 = TF.factor_window(path, mats, ch=2)
    assert win2["tooling"]["up"]["G1"] == "120" and win2["material"]["G1"] == "Aluminum Oxide"
    assert win2["values"]["G2"] == "ρ 4.5 · Z 0.628" and "다음 공정 시작 때 적용" in win2["info"]


def test_factor_window_warns_material_not_in_list(tmp_path):
    """목록에 없는 저장 이름은 그대로 두고 경고(v1 :97-106, 최대 8줄 + '… 외 n건')."""
    mats, _w = load_materials(_materials(tmp_path))
    path = tmp_path / "f.json"
    fac = ThicknessFactors()
    fac.material["G3"] = "Unobtainium"
    save_factors(path, fac)
    win = TF.factor_window(path, mats, [f"w{i}" for i in range(9)])
    assert win["material"]["G3"] == "Unobtainium" and win["values"]["G3"] == "목록에 없음"
    assert win["warnings"][-1] == "G3 재료 'Unobtainium' 가 재료 목록에 없음 — 다시 고르십시오"
    assert win["warning_text"].startswith("⚠ 읽기 경고:\n재료 목록: w0") and win["warning_text"].endswith("… 외 2건")


def test_material_window_validate_delete_confirm_and_save(tmp_path):
    """v1 test_material_dialog_edit_validate_and_refresh_factor_dialog — 범위·이름 중복(대소문자 무시) 빨간 칸, 사용 중 재료 삭제 확인."""
    mp = _materials(tmp_path)
    win = TM.materials_window(mp)
    assert len(win["rows"]) == 3 and win["rows"][1] == ["Aluminum Oxide", "Al2O3", "3.97", "1", "매뉴얼 Z 미기재"]
    rows = [list(r) for r in win["rows"]] + [["Copper", "Cu", "8.93", "0.437", "새 재료"], ["titanium", "Ti2", "4.5", "0.6", ""],
                                            ["", "", "", "", ""]]
    rows[0][2] = "200"
    mats, errs, bad = TM.collect_rows(rows)
    assert len(errs) == 2 and (0, 2) in bad and (4, 0) in bad
    assert errs[1] == "5행 'titanium': 이름 중복(3행)"
    rows[0][2] = "2.70"
    del rows[4]
    ask = TM.delete_confirm_text(["Titanium"], ["Titanium"])
    assert "Titanium" in ask and "E845" in ask and ask.endswith("지울까요?")
    assert TM.delete_confirm_text(["Aluminum Oxide"], ["Titanium"]) == ""
    del rows[1]                                                            # 안 쓰는 재료 — 확인 없이 지움
    ok, text, _b = TM.save_rows(mp, rows)
    assert ok and text == ""
    back, w = load_materials(mp)
    assert w == [] and [m.name for m in back] == ["Aluminum", "Titanium", "Copper"] and back[0].density == 2.7


def test_write_values_rules(tmp_path):
    """[장비에 쓰기] 값 검사(v1 test_write_to_device_rejects_bad_cells) — 빈 칸·범위 밖 tooling, 재료 미선택."""
    mats, _w = load_materials(_materials(tmp_path))
    t, m, errs, bad = TF.write_values("mid", "G2", "120", "Titanium", mats)
    assert (t, m.name, errs) == (120.0, "Titanium", [])
    for tool, mat, red in (("", "Titanium", "tooling.mid.G2"), ("5", "Titanium", "tooling.mid.G2"), ("120", "", "material.G2")):
        _t, _m, errs, bad = TF.write_values("mid", "G2", tool, mat, mats)
        assert errs and bad == [red]
    assert TF.write_values("mid", "G2", "", "", mats)[2] == ["tooling Mid/G2: 빈 칸", "G2: 재료 미선택"]


def test_device_write_block_shared_by_zero_and_write():
    ok = {"connected": True, "stale": False}
    assert TF.device_write_block(ok) == ""
    assert TF.device_write_block(dict(ok, process_running=True)) == "공정 중"
    assert TF.device_write_block(ok, process_running=True) == "공정 중"
    assert TF.device_write_block(dict(ok, connected=False)) == "두께 모니터 미연결"
    assert TF.device_write_block(dict(ok, stale=True)) == "두께 모니터 응답 없음"

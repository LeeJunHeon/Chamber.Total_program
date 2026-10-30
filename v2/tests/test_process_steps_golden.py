"""v1 골든과 v2 공정 스텝 비교 — v1 을 import 하지 않고 골든 파일만 읽는다.

(a) v1 저장소의 tests/data/process_sequences_golden.json(6 시나리오, v1 tests/test_process_heater_steps.py 가 만든 것)과
    tests/data/thickness_time_mode_golden.json(4 시나리오)을 v2 로 다시 만든다 — process_sequence.build_sequence(블록 없이)와
    blocks.template_blocks → blocks.expand(기본 템플릿 블록) 둘 다 같아야 한다. eta_rows(ETA 하한)는 eta.py 몫이라 여기서 보지 않는다.
(b) v2/tests/golden/process_combos.json — v2/tools/v1golden_process_steps.py 가 v1 코드를 실제로 돌려 만든 열 조합(400+ 경우):
    행 종류(delay·TEST)·정규화 params·시작 전 검사 문구·스텝 8개 필드(종료 절차 포함)·긴급 종료 절차·순서 검사·v1 로그.
(c) recipe_check.json(E230 행 검사·시작 전 검사)·recipe_files.json(CSV·XLSX 읽기) — v1 함수 결과와 같은지.
"""
from __future__ import annotations

import copy
import dataclasses
import json
from pathlib import Path

import pytest

from v2.ch12 import process_steps as ps
from v2.ch12.blocks import check_blocks, expand, template_blocks
from v2.ch12.blocks_expand import segment_uses, template_from_row
from v2.ch12.process_sequence import build_sequence, validate_sequence, estimated_duration_ms
from v2.ch12.recipe import normalize_row, read_rows
from v2.ch12.recipe_check import check_norm_params, check_raw_rows, e230_message

ROOT = Path(__file__).resolve().parents[2]
GOLD = Path(__file__).resolve().parent / "golden"


def _j(x):
    return json.loads(json.dumps(x, ensure_ascii=False))


# ───────────── (a) v1 저장소 골든 ─────────────
V1_CASES = {
    "ch2_dc_rf_boost": (2, dict(base_pressure=7e-6, working_pressure=2.0, process_time=10, shutter_delay=5, use_ar=True,
                                ar_flow=20, use_dc_power=True, dc_power=100, use_rf_power=True, rf_power=150, use_ms=True,
                                use_g1=True, use_g2=True)),
    "ch2_dcp_change": (2, dict(base_pressure=7e-6, working_pressure=60, process_time=20, shutter_delay=5, use_ar=True,
                               ar_flow=50, use_dc_pulse=True, dc_pulse_power=80, dc_pulse_freq=20, dc_pulse_off_time=10.0,
                               use_ms=True, use_g1=True, power_change_time="10m", change_power_value="100")),
    "ch2_rfp_ps": (2, dict(base_pressure=1e-5, working_pressure=5, process_time=3, shutter_delay=0, use_ar=True, use_o2=True,
                           ar_flow=20, o2_flow=2, use_rf_pulse=True, rf_pulse_power=100, rf_pulse_freq=1.0, rf_pulse_duty=50,
                           use_power_select=True, use_g3=True)),
    "ch1_rfp": (1, dict(base_pressure=1e-5, working_pressure=3, process_time=5, shutter_delay=2, use_ar=True, use_o2=True,
                        ar_flow=20, o2_flow=1, use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40,
                        use_ms=True)),
    "ch1_dcp_change_sd": (1, dict(base_pressure=1e-5, working_pressure=12, process_time=5, shutter_delay=3, use_ar=True,
                                  ar_flow=30, use_dc_pulse=True, dc_pulse_power=60, dc_pulse_freq=50, dc_pulse_off_time="DC",
                                  use_ms=True, power_change_time="1m", change_power_value="90")),
    "ch2_test": (2, dict(test_mode=True, test_duration_sec=30)),
}
THK_CASES = {
    "ch2_thk_dcp": (2, dict(base_pressure=7e-6, working_pressure=5, process_time=100 / 0.05 / 60, shutter_delay=5,
                            thickness=100.0, dep_rate=0.05, use_ar=True, ar_flow=20, use_dc_pulse=True, dc_pulse_power=80,
                            dc_pulse_freq=20, dc_pulse_off_time=10.0, use_ms=True, use_g1=True)),
    "ch2_thk_change_main": (2, dict(base_pressure=7e-6, working_pressure=60, process_time=20, shutter_delay=5, thickness=60.0,
                                    dep_rate=0.05, use_ar=True, ar_flow=50, use_dc_pulse=True, dc_pulse_power=80,
                                    dc_pulse_freq=20, dc_pulse_off_time=10.0, use_ms=True, use_g1=True,
                                    power_change_time="10m", change_power_value="100")),
    "ch1_thk_rfp": (1, dict(base_pressure=1e-5, working_pressure=3, process_time=150 / 0.042972222 / 60, shutter_delay=2,
                            thickness=150.0, dep_rate=0.042972222, use_ar=True, ar_flow=20, use_rf_pulse=True,
                            rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True)),
    "ch2_thk_heater_rf": (2, dict(base_pressure=7e-6, working_pressure=2.0, process_time=10, shutter_delay=0, thickness=30.0,
                                  dep_rate=0.05, use_ar=True, ar_flow=20, use_rf_power=True, rf_power=150, use_ms=True,
                                  use_g2=True, use_heater=True, heater_temp=300.0)),
}


def _golden_caps(ch: int) -> ps.Caps:
    """v1 골든 생성 조건(tests/test_process_heater_steps.py:_pc) — DC·RF 연속·DC2 는 CH2, 펄스 둘 다."""
    return ps.Caps(dc=ch == 2, dc2=ch == 2, rf=ch == 2, dcp=True, rfp=True, heater=ch == 2)


@pytest.mark.parametrize("fname,cases", [("process_sequences_golden.json", V1_CASES),
                                         ("thickness_time_mode_golden.json", THK_CASES)])
def test_v1_repo_golden(fname, cases):
    golden = json.loads((ROOT / "tests" / "data" / fname).read_text(encoding="utf-8"))
    assert set(cases) <= set(golden)
    for name, (ch, p) in cases.items():
        caps = _golden_caps(ch)
        direct = _j(ps.to_golden(build_sequence(dict(p), ch, {}, caps=caps)))
        assert direct == golden[name], name
        blocks, _ = template_blocks(dict(p), ch, {}, caps=caps)
        steps, insts = expand(blocks, {}, ch, caps=caps)
        assert _j(ps.to_golden(steps)) == golden[name], name
        assert not any(i.auto for i in insts)                     # 템플릿은 끝 정지를 직접 갖는다
        clean, errs, _w = check_blocks(blocks, {}, ch, caps=caps)
        assert not errs, (name, errs)


def test_v1_heater_steps_only_added():
    """v1 test_heater_steps_only_added_at_start_and_after_sp1 와 같은 성질(히터 = 앞 2단계 + SP1 뒤 HEATER_HOLD)."""
    for name in ("ch2_dc_rf_boost", "ch2_dcp_change", "ch2_rfp_ps"):
        ch, p = V1_CASES[name]
        caps = _golden_caps(ch)
        base = ps.to_golden(build_sequence(dict(p), ch, {}, caps=caps))
        blocks, _ = template_blocks(dict(p, use_heater=True, heater_temp=200.0, heater_ramp=6.0), ch, {}, caps=caps)
        steps, _ = expand(blocks, {}, ch, caps=caps)
        names = [s.action for s in steps]
        assert names[:3] == ["HEATER_SET", "HEATER_WAIT", "IG_CMD"]
        assert steps[0].value == 200.0 and steps[0].params == (6.0,) and steps[1].value == 200.0
        i = names.index("HEATER_HOLD")
        assert steps[i - 1].message.startswith("압력 제어(SP1) 시작") and names.count("HEATER_HOLD") == 1
        assert [r for r in ps.to_golden(steps) if not r[0].startswith("HEATER")] == base


# ───────────── (b) 열 조합 골든 ─────────────
COMBO = json.loads((GOLD / "process_combos.json").read_text(encoding="utf-8"))
CFG = COMBO["cfg"]


def _caps(case) -> ps.Caps:
    c = ps.caps_from_cfg(CFG, case["ch"])
    c = dataclasses.replace(c, **case["caps_override"])
    return dataclasses.replace(c, thickness=bool(case["thk"]))


def test_combo_count_and_caps():
    cases = COMBO["cases"]
    assert len(cases) >= 300
    for ch in (1, 2):
        v1 = next(c["caps"] for c in cases if c["ch"] == ch and not c["caps_override"])
        assert dataclasses.asdict(ps.caps_from_cfg(CFG, ch)) == {**v1, "set_dcp": True, "set_rfp": True}


@pytest.mark.parametrize("case", COMBO["cases"], ids=[c["name"] for c in COMBO["cases"]])
def test_combo(case):
    ch, v1 = case["ch"], case["v1"]
    caps = _caps(case)
    rt = template_from_row(dict(case["raw"]), ch, CFG, caps=caps)
    if v1["kind"] == "delay":
        assert rt.kind == "delay"
        b = rt.blocks[0]
        assert (b["params"]["sec"], b["params"]["label"], b["params"]["name"]) == (v1["sec"], v1["label"], v1["disp"])
        steps, _ = expand(rt.blocks, CFG, ch, caps=caps)
        assert [s.action for s in steps] == ["ROW_DELAY"] and steps[0].duration == v1["ticks"] * 1000
        return
    assert _j(rt.params) == v1["norm"]
    p_chk = copy.deepcopy(rt.params)
    p_chk.pop("thickness_mode", None)
    assert check_norm_params(p_chk, ch, caps, CFG, heater_available=caps.heater) == v1["norm_errors"]
    if "error" in v1:
        assert rt.kind == "error" and rt.error.startswith(v1["error"] + ":")
        assert _j(rt.logs) == v1["logs"]
        return
    assert rt.kind == v1["kind"]
    if "exception" in v1:
        with pytest.raises(Exception) as ei:
            expand(rt.blocks, CFG, ch, caps=caps)
        assert type(ei.value).__name__ == v1["exception"]
        return
    steps, insts = expand(rt.blocks, CFG, ch, caps=caps)
    assert _j(ps.to_golden(steps)) == v1["steps"]
    assert _j(ps.to_golden(build_sequence(dict(rt.params), ch, CFG, caps=caps))) == v1["steps"]
    assert _j(rt.logs) == v1["logs"]
    assert list(validate_sequence(steps)) == v1["validate"]
    assert estimated_duration_ms(steps) == v1["estimated_ms"]
    sc = ps.step_cfg(CFG, ch)
    assert _j(ps.to_golden(ps.emergency_steps(sc, ps.uses_from_params(rt.params, caps, sc)))) == v1["emergency"]
    if v1["kind"] == "process":
        seg = [b for b in rt.blocks if b["type"] != "end_stop"]
        assert _j(ps.to_golden(ps.emergency_steps(sc, segment_uses(seg, sc)))) == v1["emergency"]
        assert all(s.block >= 0 for s in steps) and not any(i.auto for i in insts)
    if not v1["norm_errors"]:                                  # v1 이 통과시킨 행의 템플릿 블록은 블록 검사도 통과
        clean, errs, _w = check_blocks(rt.blocks, CFG, ch, caps=caps)
        assert not errs, errs


# ───────────── (c) 레시피 검사·파일 읽기 ─────────────
CHECK = json.loads((GOLD / "recipe_check.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("name", sorted(CHECK["raw"]))
def test_raw_rows_e230(name):
    g = CHECK["raw"][name]
    errs = check_raw_rows([dict(r) for r in g["rows"]], row_numbers=g["row_numbers"])
    assert errs == g["errors"]
    assert (e230_message(errs) if errs else "") == g["host_message"]


def test_norm_params_check():
    cfg = CHECK["cfg"]
    for c in CHECK["norm"]:
        caps = ps.Caps(**c["caps"])
        assert check_norm_params(copy.deepcopy(c["params"]), c["ch"], caps, cfg, heater_available=caps.heater) == c["errors"], c


FILES = json.loads((GOLD / "recipe_files.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("name", sorted(FILES["files"]))
def test_recipe_file_read(name):
    g = FILES["files"][name]
    rows, nos, blank = read_rows(str(GOLD / "recipe_files" / name))
    assert [[[k, v] for k, v in r.items()] for r in rows] == g["rows"]
    assert nos == g["row_nos"] and blank == g["blank"]


def test_v2_param_defaults_give_v1_settings():
    """v2 파라미터 표 기본값(다른 담당의 params/*.py)으로 읽은 공정 설정·지원 플래그가 골든을 만든 v1 설정(settings.json)과 같다."""
    from v2.ch12.params import table
    from v2.ch12.recipe_check import heater_limit_c, rfp_freq_max_khz
    cfg = {p.key: p.default for p in table().params}
    for ch in (1, 2):
        assert ps.step_cfg(cfg, ch) == ps.step_cfg(CFG, ch)
        assert ps.caps_from_cfg(cfg, ch) == ps.caps_from_cfg(CFG, ch)
        assert rfp_freq_max_khz(cfg, ch) == rfp_freq_max_khz(CFG, ch)
    assert heater_limit_c(cfg) == heater_limit_c(CFG)

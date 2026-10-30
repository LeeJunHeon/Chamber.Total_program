"""v2/ch12/recipe.py·recipe_check.py·recipe_xlsx.py — 열 정의·파일 읽기·기간 해석(v1 이상 동작 포함)·행 종류·정규화·검사.

v1 과 같은지(전수)는 test_process_steps_golden.py(골든)가 본다. 여기는 규칙 하나하나를 이름 붙여 확인한다.
"""
from __future__ import annotations

import csv
import zipfile
from pathlib import Path

import pytest

from v2.ch12 import process_steps as ps
from v2.ch12.process_sequence import row_test_duration_sec
from v2.ch12.recipe import (COLUMN_DOC, TEMPLATE_COLUMNS, delay_of, is_blank_row, load_recipe_file, mark_test_row,
                            normalize_row, parse_duration_runtime, plan_row, read_rows, thickness_mode_of)
from v2.ch12.recipe_check import (E230, check_dc_pulse, check_norm_params, check_raw_rows, e230_message, e230_popup,
                                  host_error_code, parse_duration_for_validate, queue_param_error_log)
from v2.ch12.recipe_xlsx import is_date_format, iter_first_sheet

GOLD = Path(__file__).resolve().parent / "golden" / "recipe_files"
CAPS1 = ps.Caps(dc=False, dc2=False, rf=False, dcp=True, rfp=True)
CAPS2 = ps.Caps(dc=True, dc2=False, rf=True, dcp=True, rfp=True, heater=True, thickness=True)


def _row(**kw):
    r = {c: "" for c in TEMPLATE_COLUMNS}
    r.update(kw)
    return r


def test_template_columns_match_v1_template_file():
    hdr = (GOLD / "Recipe_Template.csv").read_text(encoding="utf-8-sig").splitlines()[0].split(",")
    assert tuple(h.strip() for h in hdr) == TEMPLATE_COLUMNS and len(TEMPLATE_COLUMNS) == 48
    assert set(TEMPLATE_COLUMNS) <= set(COLUMN_DOC)


def test_duration_parsers_v1_quirks():
    # 러너 규칙(delay·time 열): 단위 없는 숫자는 0 초, 뒤에 붙은 글자는 무시
    assert parse_duration_runtime("30") == 0.0 and parse_duration_runtime("abc") == 0.0
    assert parse_duration_runtime("5m") == 300 and parse_duration_runtime("1h30") == 3600
    assert parse_duration_runtime("1.5M") == 90 and parse_duration_runtime("1h 30m") == 5400 and parse_duration_runtime("") == 0
    # 컨트롤러 규칙(power_change_time·TEST time 두 번째): 숫자만 = 초, 전체 일치
    assert ps.parse_duration_pc("30") == 30 and ps.parse_duration_pc("1h30") == 0.0 and ps.parse_duration_pc("1h0m30s") == 3630
    assert parse_duration_for_validate(" 5M ") == 300 and parse_duration_for_validate("0s") == 0


def test_delay_rows():
    assert delay_of({"delay": "5", "Process_name": "x"}) == (300.0, "5분", "x")
    assert delay_of({"delay": "20m", "Process_name": "x"})[:2] == (1200.0, "20m")
    assert delay_of({"delay": "abc", "Process_name": "x"})[0] == 0.0              # 대기 0 → 공정 행으로 진행
    assert delay_of({"delay": "90", "Process_name": ""}) == (5400.0, "90분", "delay 90분")
    assert delay_of({"Process_name": "delay 10s"})[:2] == (10.0, "10초")
    assert delay_of({"Process_name": "Delay 1H"})[:2] == (3600.0, "1시간")
    assert delay_of({"Process_name": "delay5m"})[:2] == (300.0, "5분")
    assert delay_of({"Process_name": "delay 1.5m"})[0] == 0.0                     # 정수만
    assert plan_row({"delay": "1m", "#": "test"}).kind == "delay"                  # delay 판정이 먼저


def test_test_row_time_two_parsers():
    r = {"#": "Test", "time": "30", "Process_name": "t", "process_time": ""}
    assert mark_test_row(r) and r["test_duration_sec"] == 0.0 and r["process_time"] == ""
    p = normalize_row(r, 2, CAPS2)
    assert p["test_mode"] and p["time"] == "30" and row_test_duration_sec(p) == 30.0      # 컨트롤러가 다시 읽어 30 초
    r = {"#": "test", "time": "1m"}
    mark_test_row(r)
    assert r["test_duration_sec"] == 60.0 and r["process_time"] == 1.0 and r["process_note"] == "TEST"
    assert row_test_duration_sec({"test_mode": True}) == 1.0                              # 최소 1 초


def test_normalize_pulse_use_column_only_D04():
    log = []
    p = normalize_row(_row(use_dc_pulse="F", dc_pulse_power="80", dc_pulse_freq="20", dc_pulse_off_time="10"), 2, CAPS2, log=log)
    assert not p["use_dc_pulse"] and p["dc_pulse_power"] == 0.0 and p["dc_pulse_freq"] is None and p["dc_pulse_off_time"] is None
    assert any("DC-Pulse: use_dc_pulse=F 이므로 사용하지 않습니다" in m for _, m in log)
    log = []
    p = normalize_row(_row(use_rf_pulse="T", rf_pulse_power="10"), 2, ps.Caps(dc=True, dc2=False, rf=True, dcp=True, rfp=False),
                      log=log)
    assert not p["use_rf_pulse"] and ("Params", "CH2: RF-Pulse 미지원 설정 → OFF 처리합니다.") in log


def test_normalize_misc():
    log = []
    p = normalize_row(_row(N2="T", N2_flow="5", use_dc_pulse="T", dc_pulse_power="80", dc_pulse_freq="40",
                           dc_pulse_duty_cycle="70", thickness="100", dep_rate="0.05", chuck_position="UP",
                           use_heater="F", heater_temp="200"), 1, CAPS1, log=log)
    assert p["use_n2"] is False and p["n2_flow"] == 0.0 and p["chuck_position"] == "up"
    assert p["dc_pulse_off_time"] == 7.5 and p["dc_pulse_duty"] == 70.0
    assert p["process_time"] == pytest.approx(100 / 0.05 / 60) and p["heater_temp"] is None
    msgs = [m for _, m in log]
    assert msgs[0].startswith("[호환] dc_pulse_duty_cycle 70% @40kHz") and "CH1은 N2 미지원 → N2 설정을 무시합니다." in msgs
    assert any(m.startswith("process_time 자동 계산: 100.0nm ÷ 0.05nm/s = 33.333분") for m in msgs)
    p = normalize_row(_row(dc_pulse_off_time="dc", integration_time="abc", base_pressure=""), 2, CAPS2)
    assert p["integration_time"] == 60 and p["base_pressure"] == 1e-5
    assert thickness_mode_of(100.0, True) and not thickness_mode_of(100.0, False) and not thickness_mode_of("", True)


def test_raw_check_messages_and_overflow():
    rows = [_row(Process_name=f"p{i}", base_pressure="x", working_pressure="y", Ar="Q") for i in range(8)]
    errs = check_raw_rows(rows)
    assert len(errs) == 21 and errs[-1] == "... 외 4건"
    assert errs[0] == "2행('p0') base_pressure='x' → 숫자가 아닙니다"
    assert e230_message(errs).startswith("레시피 오류 21건:\n - 2행") and e230_popup(errs[:1]).endswith("\n- " + errs[0])
    assert host_error_code(e230_message(errs)) == E230 and host_error_code("cooldown 30s remaining") == "E700"
    assert host_error_code("runner busy") == "E511" and host_error_code("x", "E207") == "E207"
    e = check_raw_rows([_row(Process_name="", power_change_time="1m")], row_numbers=[7])
    assert e == ["7행 change_power_value 와 power_change_time 은 함께 입력해야 합니다 (현재: change_power_value='', "
                 "power_change_time='1m')"]
    assert check_raw_rows([_row(tooling="10", density="99.99", z_factor="9.999", dc_pulse_off_time="DC", chuck_position="Mid")]) == []
    assert queue_param_error_log(["a", "b"]) == "CSV 공정 파라미터 오류:\n - a\n - b"


def test_norm_check_rules():
    base = dict(use_ar=True, ar_flow=20.0, o2_flow="abc", n2_flow=None)
    p = dict(base)
    errs = check_norm_params(p, 1, CAPS1, {})
    assert errs == ["o2_flow 값이 숫자가 아니라 0으로 처리(raw='abc').",
                    "CH1은 RF Pulse 또는 DC Pulse 중 하나를 반드시 선택해야 합니다."]
    assert p["o2_flow"] == 0.0 and p["n2_flow"] == 0.0                           # v1 처럼 p 를 고친다
    assert check_norm_params(dict(base, test_mode=True, use_ar=False), 1, CAPS1, {}) == []
    assert check_dc_pulse({"dc_pulse_freq": 100, "dc_pulse_off_time": 4.5}) == [
        "DC Pulse Off Time 4.5µs 는 100kHz 상한 4.0µs 초과(매뉴얼 p.13 그림3)"]
    e = check_norm_params(dict(base, use_g1=True, use_rf_power=True, use_heater=True, heater_temp=950.0), 2, CAPS2,
                          {"CH2_HEATER_MAX_TEMP": 900.0, "CH2_HEATER_SV_LIMIT_C": 900.0})
    assert e[-1] == "heater_temp 는 0 초과 900°C 이하여야 합니다 (현재 950.0)"
    e = check_norm_params(dict(base, use_g1=True, use_rf_power=True, use_heater=True, heater_temp=100.0), 2, CAPS2, {},
                          heater_available=False)
    assert e[-1].startswith("히터 기능이 꺼져 있어 use_heater=T 공정을 실행할 수 없습니다")


def _write_csv(path, rows, enc="utf-8-sig"):
    with open(path, "w", encoding=enc, newline="") as f:
        w = csv.writer(f)
        for r in rows:
            w.writerow(r)


def test_load_recipe_file_codes(tmp_path):
    hdr = list(TEMPLATE_COLUMNS)
    r = load_recipe_file(str(tmp_path / "none.csv"))
    assert (r.code, r.message) == ("E207", f"레시피 파일을 찾을 수 없습니다: {tmp_path / 'none.csv'}")
    assert load_recipe_file("abc.txt").code == "E209"
    p = tmp_path / "empty.csv"
    _write_csv(p, [hdr, [""] * len(hdr)])
    r = load_recipe_file(str(p))
    assert r.code == "E208" and r.blank == 1 and r.logs == [("File", "빈 행 1개 건너뜀 (모든 칸이 빈 행)")]
    p = tmp_path / "bad.csv"
    _write_csv(p, [hdr, ["1", "a"] + [""] * 10 + ["x"]])
    r = load_recipe_file(str(p))
    assert r.code == "E230" and r.message == "레시피 오류 1건:\n - 2행('a') base_pressure='x' → 숫자가 아닙니다"
    ui = load_recipe_file(str(p), origin="ui")
    assert ui.message == "레시피 오류 1건:\n- 2행('a') base_pressure='x' → 숫자가 아닙니다" and ui.logs[-1][1] == r.message
    p = tmp_path / "cp949.csv"
    _write_csv(p, [hdr, ["1", "한글"]], enc="cp949")
    r = load_recipe_file(str(p))
    assert r.code == "E209" and "UnicodeDecodeError" in r.message and "CSV UTF-8(쉼표로 분리)" in r.message
    p = tmp_path / "ok.csv"
    _write_csv(p, [hdr, ["", "", "20m"], ["3"]])
    r = load_recipe_file(str(p))
    assert r.ok and [x["Process_name"] for x in r.rows] == ["공정 1", "3"] and r.row_nos == [2, 3]
    assert r.logs[-1] == ("File", f"CSV 로드 완료: {p} (총 2개)")


def test_blank_row_rules():
    assert is_blank_row({"a": "", "b": None, None: ["", " "]}) and not is_blank_row({"a": "0"})
    assert is_blank_row(["", None]) and not is_blank_row("x")


def test_xlsx_stdlib_reader_matches_openpyxl_rules():
    rows, nos, blank = read_rows(str(GOLD / "mixed.xlsx"))
    assert nos[0] == 2 and blank == 0
    assert rows[0]["base_pressure"] == "7e-06" and rows[0]["main_shutter"] == "True" and rows[0]["#"] == "1"
    assert "" not in rows[0] and rows[0]["heater_temp"] == "200"                   # 헤더 strip·빈 헤더 열 무시
    t = [r for r in rows if r["#"] == "test"][0]
    assert t["Process_name"] == "공백" and nos[rows.index(t)] == 5
    assert any(r["process_time"] == "2026-10-30 12:00:00" for r in rows)            # 날짜 서식 숫자 → datetime 글자
    assert list(iter_first_sheet(str(GOLD / "empty.xlsx"))) == []
    assert is_date_format("yyyy-mm-dd h:mm:ss") and not is_date_format("0.00") and not is_date_format('"d"0')


def test_xlsx_without_dimension_and_shared_strings(tmp_path):
    """dimension 이 없는 시트(행마다 마지막 칸까지) · 공유 문자열 · r 속성 없는 칸 · 빠진 행."""
    p = tmp_path / "raw.xlsx"
    ns = 'xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
    rns = 'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"'
    with zipfile.ZipFile(p, "w") as z:
        z.writestr("_rels/.rels", '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                   '<Relationship Id="r1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/'
                   'officeDocument" Target="xl/workbook.xml"/></Relationships>')
        z.writestr("xl/workbook.xml", f'<workbook {ns} {rns}><sheets><sheet name="A" sheetId="1" r:id="rId1"/></sheets></workbook>')
        z.writestr("xl/_rels/workbook.xml.rels",
                   '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                   '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
                   'Target="worksheets/s.xml"/><Relationship Id="rId2" '
                   'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" '
                   'Target="sharedStrings.xml"/></Relationships>')
        z.writestr("xl/sharedStrings.xml", f'<sst {ns}><si><t>Process_name</t></si><si><r><t>de</t></r><r><t>lay</t></r>'
                   f'<rPh><t>x</t></rPh></si><si><t>공정</t></si></sst>')
        z.writestr("xl/worksheets/s.xml", f'<worksheet {ns}><sheetData><row r="1"><c t="s"><v>0</v></c><c t="s"><v>1</v></c>'
                   '</row><row r="3"><c r="A3" t="s"><v>2</v></c><c r="B3"><v>5</v></c></row></sheetData></worksheet>')
    assert list(iter_first_sheet(str(p))) == [("Process_name", "delay"), (), ("공정", 5)]
    rows, nos, _ = read_rows(str(p))
    assert rows == [{"Process_name": "공정", "delay": "5"}] and nos == [3]

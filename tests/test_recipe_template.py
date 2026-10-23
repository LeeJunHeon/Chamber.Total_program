# -*- coding: utf-8 -*-
"""레시피 템플릿(recipes/Recipe_Template.csv·.xlsx) — 두 파일의 열이 같고, 엑셀 입력 제한·설명 시트가 열과 맞는지,
머리글 메모 상자가 줄지 않았는지. 열을 하나 추가할 때 한쪽만 고치는 실수를 막는다."""
import os
import sys
import csv
import zipfile

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import pytest                                                     # noqa: E402
from openpyxl import load_workbook                                # noqa: E402

CSV_PATH = os.path.join(_ROOT, "recipes", "Recipe_Template.csv")
XLSX_PATH = os.path.join(_ROOT, "recipes", "Recipe_Template.xlsx")


def _csv_header():
    with open(CSV_PATH, encoding="utf-8-sig", newline="") as fp:
        return next(csv.reader(fp))


@pytest.fixture(scope="module")
def wb():
    return load_workbook(XLSX_PATH)


def _dvs(ws):
    return {str(dv.sqref): dv for dv in ws.data_validations.dataValidation}


def test_csv_and_xlsx_columns_match(wb):
    ws = wb["Recipe"]
    xlsx = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
    assert xlsx == _csv_header()
    assert xlsx[-4:] == ["use_heater", "heater_temp", "heater_ramp", "heater_soak"]


def test_every_column_has_comment_and_validation(wb):
    ws = wb["Recipe"]
    dvs = _dvs(ws)
    for c in range(1, ws.max_column + 1):
        col = ws.cell(1, c).column_letter
        assert ws.cell(1, c).comment is not None, ws.cell(1, c).value
        assert f"{col}2:{col}61" in dvs, ws.cell(1, c).value


def test_heater_and_special_validations(wb):
    dvs = _dvs(wb["Recipe"])
    assert (dvs["AL2:AL61"].type, dvs["AL2:AL61"].formula1) == ("list", '"T,F"')
    assert (dvs["AM2:AM61"].type, dvs["AM2:AM61"].formula1, dvs["AM2:AM61"].formula2) == ("decimal", "1", "900")
    assert (dvs["AN2:AN61"].type, dvs["AN2:AN61"].formula1, dvs["AN2:AN61"].formula2) == ("decimal", "0.1", "60")
    assert dvs["A2:A61"].type == "custom" and 'LOWER(A2)="test"' in dvs["A2:A61"].formula1    # TEST 행 허용
    assert dvs["Z2:Z61"].formula1 == '"F"'                                                      # DC2 폐지


def test_description_sheet_lists_every_column(wb):
    ws, desc = wb["Recipe"], wb["설명"]
    cols = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
    listed = [desc.cell(r, 1).value for r in range(5, desc.max_row + 1)]
    assert listed == cols


def test_comment_boxes_not_shrunk():
    """머리글 메모 상자가 openpyxl 기본 크기(144×79)로 줄지 않았는지 — 줄면 설명 글이 잘린다.
    openpyxl 은 기존 메모 상자 크기를 읽지 못해 다시 저장하면 기본값으로 바꾸므로, 템플릿을 고치는 스크립트가 크기를 다시 넣어야 한다."""
    with zipfile.ZipFile(XLSX_PATH) as z:
        vml = "".join(z.read(n).decode("utf-8", "replace") for n in z.namelist() if n.endswith(".vml"))
    assert vml and "width:144px;height:79px" not in vml


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

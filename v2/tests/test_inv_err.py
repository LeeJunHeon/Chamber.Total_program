"""전수 목록 err(v2/ch12/spec/inv_err.py) 항목 시험 — 골든(test_errors.py)이 덮지 않는 상수·개수·순서·문구 규칙.

오류 코드 하나하나의 필드·payload 는 v1 을 실제로 돌려 만든 골든 v2/tests/golden/errors.json 으로
test_errors.py::test_registry_same_as_v1_golden·test_case_same_as_v1 이 본다(여기서는 그 골든이 정말 그 코드를 담는지 확인).
"""
from __future__ import annotations

import collections
import json
import re
from pathlib import Path

import pytest

from v2.ch12 import errors as E
from v2.ch12.spec import inv_err

GOLDEN = json.loads((Path(__file__).resolve().parent / "golden" / "errors.json").read_text(encoding="utf-8"))
CODE_ITEMS = {m.group(1): it for it in inv_err.ITEMS for m in [re.match(r"오류 (E\d{3}) '", it["desc"])] if m}


def payload_case_index(code: str) -> int:
    """골든에서 그 코드의 build_error_payload_from_code 첫 경우 번호(test_errors.py::test_case_same_as_v1 의 id 끝 숫자)."""
    for n, c in enumerate(GOLDEN["cases"]):
        if c["fn"] == "build_error_payload_from_code" and c["args"].get("code") == code:
            return n
    raise KeyError(code)


def test_constants_and_counts_same_as_v1():
    assert (E.ACTION_RETRY, E.ACTION_STOP, E.ACTION_IGNORE, E.ACTION_RETRY_LATER) == ("RETRY", "STOP", "IGNORE",
                                                                                      "RETRY_LATER")
    assert (E.SEV_WARN, E.SEV_ERROR, E.SEV_FATAL) == ("WARN", "ERROR", "FATAL")
    defs = list(E.iter_error_definitions())
    assert len(defs) == 192
    assert collections.Counter(d.client_action for d in defs) == {"STOP": 142, "RETRY": 38, "IGNORE": 11, "RETRY_LATER": 1}
    assert [d.code for d in defs if d.client_action == "RETRY_LATER"] == ["E700"]
    assert collections.Counter(d.severity for d in defs) == {"ERROR": 137, "WARN": 50, "FATAL": 5}
    assert [d.code for d in defs if d.is_fallback] == ["E110", "E999"]           # fallback 은 둘뿐(ERR-019)


def test_registry_order_and_keys():
    codes = E.list_error_codes()
    assert codes == [d["code"] for d in GOLDEN["registry"]] and all(E.ERROR_REGISTRY[c].code == c for c in codes)
    i = codes.index
    assert i("E209") + 1 == i("E230") and i("E230") + 1 == i("E210")              # E230 은 E209 뒤
    assert i("E326") + 1 == i("E330") and codes[-2:] == ["E700", "E999"]
    assert i("E848") < i("E847") and codes[i("E901"):i("E911") + 1] == [f"E9{n:02d}" for n in range(1, 12)]


def test_golden_covers_every_inventory_code():
    """inv_err 의 오류 코드 항목(192)마다 골든에 정의와 payload 경우가 있다 — 그 시험 id 가 inv_err test 칸이다."""
    assert set(CODE_ITEMS) == set(E.ERROR_REGISTRY)
    for code, it in CODE_ITEMS.items():
        d = E.ERROR_REGISTRY[code]
        assert f"'{d.title}'" in it["desc"] and f"category {d.category}" in it["desc"], code
        n = payload_case_index(code)
        assert f"test_case_same_as_v1[build_error_payload_from_code-{n}]" in it["test"] or it["status"] != "done", code


def test_one_line_truncation_and_popup_title():
    long = "가" * 2100
    p = {"error_code": "E302", "client_action": "STOP", "message": "제목\r\n둘째 줄", "detail": long}
    t = E.format_user_text(p)
    assert t.startswith("[E302] 제목  둘째 줄 | 상세: ")      # \r→\n 뒤 줄마다 공백(v1 _one_line)
    assert t.endswith("…") and len(t.split("상세: ")[1]) == 2001
    assert E.format_log_text({"message": "m", "detail": "m"}) == "m"                  # detail==message 면 '상세' 생략
    assert E.format_log_text({"error_code": "E1", "message": "m", "detail": ""}) == "[E1] m"
    assert E.popup_title("HOST") == "오류(HOST)" and E.popup_title("HOST", "E302") == "오류(HOST, E302)"


@pytest.mark.parametrize("raw,want", [("", "E999"), (None, "E999"), (" e302 ", "E302"), ("E000", "E999")])
def test_definition_lookup_falls_back_to_e999(raw, want):
    assert E.get_error_definition(raw).code == want

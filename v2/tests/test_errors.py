"""오류 코드 카탈로그 시험 — v1 errors/ 를 실제로 돌려 만든 골든(v2/tools/v1golden_errors.py → golden/errors.json)과 비교.

v1 을 import 하지 않는다. 결과 dict 는 키 순서까지 같아야 한다(로봇 실패 응답의 필드 순서).
"""
import json
from pathlib import Path

import pytest

from v2.ch12 import errors as E
from v2.ch12.spec import settings_v1facts as facts

GOLDEN = json.loads((Path(__file__).resolve().parent / "golden" / "errors.json").read_text(encoding="utf-8"))
_BUILTIN = {"ValueError": ValueError, "RuntimeError": RuntimeError, "TimeoutError": TimeoutError,
            "KeyError": KeyError, "OSError": OSError, "Exception": Exception}


def make_exc(spec: dict) -> BaseException:
    t = spec["type"]
    if t == "AppError":
        kw = {"code": spec["code"], "cause": make_exc(spec["cause"]) if spec.get("cause") else None}
        if "detail" in spec:
            kw["detail"] = spec["detail"]
        if "meta" in spec:
            kw["meta"] = dict(spec["meta"])
        return E.AppError(**kw)
    if t == "CodedRuntimeError":
        return E.CodedRuntimeError(spec["msg"], spec["code"])
    return _BUILTIN[t](spec["msg"])


def _jsonable(v):
    if isinstance(v, BaseException):
        return {"__exc__": type(v).__name__, "str": str(v)}
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    return v


def _ordered(v):
    """dict 를 (키, 값) 목록으로 — 순서까지 비교."""
    if isinstance(v, dict):
        return [(k, _ordered(x)) for k, x in v.items()]
    if isinstance(v, list):
        return [_ordered(x) for x in v]
    return v


def run_case(fn: str, a: dict):
    exc = (lambda: make_exc(GOLDEN["exceptions"][a["exc"]])) if "exc" in a else None
    kw = {k: v for k, v in a.items() if k not in ("exc", "code", "text", "payload")}
    if fn == "build_error_payload_from_code":
        return E.build_error_payload_from_code(a["code"], **kw)
    if fn in ("build_error_payload", "build_unknown_error_payload", "build_handler_error_payload"):
        return getattr(E, fn)(exc(), **kw)
    if fn == "ensure_app_error":
        e2 = E.ensure_app_error(exc(), default_code=a["default_code"], detail=None, meta=a["meta"])
        return {"code": e2.code, "detail": e2.detail, "meta": e2.meta,
                "cause_type": type(e2.cause).__name__ if e2.cause is not None else None, "str": str(e2)}
    if fn == "AppError":
        x = exc()
        return {"code": x.code, "error_code": x.error_code, "detail": x.detail, "meta": x.meta, "str": str(x),
                "repr": repr(x), "to_dict": x.to_dict()}
    if fn == "AppError_invalid":
        try:
            E.AppError(code=a["code"])
            return {"raised": None}
        except Exception as ex:  # noqa: BLE001
            return {"raised": type(ex).__name__, "msg": str(ex)}
    if fn in ("normalize_error_code", "has_error_code", "catalog_is_fallback", "is_fallback_error",
              "get_error_title", "get_error_category", "get_client_action", "get_error_severity", "get_error_cause",
              "get_error_fix", "get_legacy_error_dict"):
        return getattr(E, fn)(a["code"], **kw)
    if fn in ("get_error_definition", "get_error_info"):
        return getattr(E, fn)(a["code"]).to_dict()
    if fn == "extract_error_code":
        return E.extract_error_code(a["text"])
    if fn == "list_error_codes":
        return E.list_error_codes()
    if fn in ("format_log_text", "format_user_text", "is_fail_payload"):
        return getattr(E, fn)(a["payload"])
    raise AssertionError(f"골든의 모르는 함수 {fn}")


def test_registry_same_as_v1_golden():
    assert [_ordered(d.to_dict()) for d in E.ERROR_REGISTRY.values()] == [_ordered(d) for d in GOLDEN["registry"]]
    assert len(E.ERROR_REGISTRY) == 192


def test_registry_same_as_spec_facts():
    """settings_v1facts.ERRORS(v1 표를 기계적으로 뽑은 사실표)와도 순서·필드가 같다."""
    assert list(E.ERROR_REGISTRY) == [row[0] for row in facts.ERRORS]
    for code, title, fields, _src in facts.ERRORS:
        d = E.ERROR_REGISTRY[code].to_dict()
        assert d["title"] == title and all(d[k] == v for k, v in fields.items()), code


@pytest.mark.parametrize("i", range(len(GOLDEN["cases"])),
                         ids=[f"{c['fn']}-{n}" for n, c in enumerate(GOLDEN["cases"])])
def test_case_same_as_v1(i):
    case = GOLDEN["cases"][i]
    got = _jsonable(run_case(case["fn"], case["args"]))
    assert _ordered(got) == _ordered(case["out"]), (case["fn"], case["args"])


def test_every_code_has_payload_case():
    covered = {c["args"]["code"] for c in GOLDEN["cases"] if c["fn"] == "build_error_payload_from_code"}
    assert set(E.ERROR_REGISTRY) <= covered


def test_payload_field_order_and_rules():
    p = E.build_error_payload(E.AppError("E302", "", {"ch": 2}, cause=TimeoutError("t/o")), meta={"x": 1})
    assert list(p) == ["result", "error_code", "client_action", "category", "severity", "message", "detail", "guide",
                       "meta", "cause_type"]
    assert p["message"] == E.get_error_title("E302") and p["guide"] == E.get_error_fix("E302")
    assert p["detail"] == "t/o"                                   # AppError.detail 이 비면 원인 예외 문구
    assert p["meta"] == {"ch": 2, "x": 1}
    q = E.build_error_payload_from_code("E777")                  # 미등록 → E999
    assert q["error_code"] == "E999" and q["detail"] == E.get_error_cause("E999")
    assert E.build_handler_error_payload(ValueError("v"))["error_code"] == "E110"
    assert E.is_fail_payload(q) and not E.is_fail_payload({"result": "ok"})

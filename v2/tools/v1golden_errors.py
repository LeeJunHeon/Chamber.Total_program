"""v1 오류 모듈(errors/)을 실제로 실행해 골든을 만든다 → v2/tests/golden/errors.json.

실행: cd <저장소 루트> && python3 v2/tools/v1golden_errors.py

- v1 모듈은 importlib 로 동적으로 부른다(v2 안에서 v1 을 import 문으로 쓰지 않는다 — test_isolation).
- registry : v1 build_registry_snapshot() 를 표 순서 그대로(list) — 192 개 코드의 title·category·client_action·severity·cause·fix·is_fallback.
- cases    : 함수 이름·인자(JSON 으로 다시 만들 수 있는 모양)·결과. 결과 dict 는 키 순서까지 비교한다(로봇 응답 필드 순서).
             예외 인자 모양: {"type": "AppError", "code", "detail", "meta", "cause": 예외} / {"type": "ValueError", "msg"}.
v2 시험(test_errors.py)은 v1 을 import 하지 않고 이 골든만 읽어 v2/ch12/errors.py 가 같은 결과를 내는지 본다.
"""
from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "v2" / "tests" / "golden" / "errors.json"

_BUILTIN_EXC = {"ValueError": ValueError, "RuntimeError": RuntimeError, "TimeoutError": TimeoutError,
                "KeyError": KeyError, "OSError": OSError, "Exception": Exception}

# 예외 인자 모양(골든에 그대로 들어가고, v2 시험이 같은 모양으로 v2 예외를 만든다)
EXCS = {
    "value": {"type": "ValueError", "msg": "bad value 3"},
    "value_blank": {"type": "ValueError", "msg": "   "},
    "runtime_empty": {"type": "RuntimeError", "msg": ""},
    "timeout": {"type": "TimeoutError", "msg": "PLC read timeout"},
    "key": {"type": "KeyError", "msg": "G_V_1"},
    "coded": {"type": "CodedRuntimeError", "msg": "레시피 파일 없음", "code": " e207 "},
    "app": {"type": "AppError", "code": "E302", "detail": "G_V_1_인터락=FALSE", "meta": {"ch": 1, "signal": "G_V_1"}},
    "app_lower": {"type": "AppError", "code": " e711 ", "detail": "  공백 detail  "},
    "app_nodetail": {"type": "AppError", "code": "E401"},
    "app_unknown": {"type": "AppError", "code": "E000", "detail": "없는 코드"},
    "app_cause": {"type": "AppError", "code": "E610", "cause": {"type": "TimeoutError", "msg": "MFC 응답 없음"}},
    "app_cause_detail": {"type": "AppError", "code": "E610", "detail": "R5 무응답",
                         "cause": {"type": "OSError", "msg": "socket closed"}},
    "app_none_detail": {"type": "AppError", "code": "E999", "detail": None, "meta": {"b": 2, "a": 1}},
}


def _load_v1():
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    names = ("errors.error_registry", "errors.error_catalog", "errors.app_error", "errors.error_payload",
             "errors.error_reporter")
    return {n.split(".")[-1]: importlib.import_module(n) for n in names}


def make_exc(spec: dict, app_error_cls, coded_cls) -> BaseException:
    t = spec["type"]
    if t == "AppError":
        cause = make_exc(spec["cause"], app_error_cls, coded_cls) if spec.get("cause") else None
        kw = {"code": spec["code"], "cause": cause}
        if "detail" in spec:
            kw["detail"] = spec["detail"]
        if "meta" in spec:
            kw["meta"] = dict(spec["meta"])
        return app_error_cls(**kw)
    if t == "CodedRuntimeError":
        return coded_cls(spec["msg"], spec["code"])
    return _BUILTIN_EXC[t](spec["msg"])


def _jsonable(v):
    """결과를 JSON 으로 — 예외 객체는 이름·문구로."""
    if isinstance(v, BaseException):
        return {"__exc__": type(v).__name__, "str": str(v)}
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    return v


def build_cases(m) -> list:
    reg, cat, app, pay, rep = m["error_registry"], m["error_catalog"], m["app_error"], m["error_payload"], m["error_reporter"]
    E = lambda name: make_exc(EXCS[name], app.AppError, app.CodedRuntimeError)  # noqa: E731
    cases = []

    def add(fn, args, out):
        cases.append({"fn": fn, "args": args, "out": _jsonable(out)})

    codes = list(reg.ERROR_REGISTRY.keys())
    for c in codes:                                   # 모든 코드: 코드만으로 만든 payload
        add("build_error_payload_from_code", {"code": c}, pay.build_error_payload_from_code(c))
    for code, detail, meta in (("E302", "G_V_1_인터락=FALSE", {"ch": 1}), ("e302", "  ", None), (" E302 ", None, {}),
                               ("E000", "", None), ("", "x", None), ("302", "", {"z": 1, "a": 2}), ("E999", "d", None)):
        args = {"code": code}
        kw = {}
        if detail is not None:
            args["detail"] = kw["detail"] = detail
        if meta is not None:
            args["meta"] = kw["meta"] = meta
        add("build_error_payload_from_code", args, pay.build_error_payload_from_code(code, **kw))
    for name in EXCS:
        add("build_error_payload", {"exc": name}, pay.build_error_payload(E(name)))
        add("build_error_payload", {"exc": name, "default_code": "E110", "detail": "인자 detail", "meta": {"m": 1}},
            pay.build_error_payload(E(name), default_code="E110", detail="인자 detail", meta={"m": 1}))
        add("build_error_payload", {"exc": name, "detail": "   "}, pay.build_error_payload(E(name), detail="   "))
        add("build_unknown_error_payload", {"exc": name}, pay.build_unknown_error_payload(E(name)))
        add("build_handler_error_payload", {"exc": name, "meta": {"cmd": "VACUUM_ON"}},
            pay.build_handler_error_payload(E(name), meta={"cmd": "VACUUM_ON"}))
        e2 = app.ensure_app_error(E(name), default_code="E205", detail=None, meta={"k": "v"})
        add("ensure_app_error", {"exc": name, "default_code": "E205", "meta": {"k": "v"}},
            {"code": e2.code, "detail": e2.detail, "meta": e2.meta,
             "cause_type": type(e2.cause).__name__ if e2.cause is not None else None, "str": str(e2)})
        if EXCS[name]["type"] == "AppError":
            a = E(name)
            add("AppError", {"exc": name}, {"code": a.code, "error_code": a.error_code, "detail": a.detail,
                                            "meta": a.meta, "str": str(a), "repr": repr(a), "to_dict": a.to_dict()})
    for bad in ("", "   ", None, 302):
        try:
            app.AppError(code=bad)
            add("AppError_invalid", {"code": bad}, {"raised": None})
        except Exception as ex:  # noqa: BLE001
            add("AppError_invalid", {"code": bad}, {"raised": type(ex).__name__, "msg": str(ex)})
    for c in ("E302", "e302", " e302 ", "302", "E000", "", None, "999", "1234", "E3021"):
        add("normalize_error_code", {"code": c}, cat.normalize_error_code(c))
        add("normalize_error_code", {"code": c, "fallback": "E110"}, cat.normalize_error_code(c, fallback="E110"))
        add("has_error_code", {"code": c}, cat.has_error_code(c))
        add("catalog_is_fallback", {"code": c}, cat.is_fallback_error(c))
        add("is_fallback_error", {"code": c}, reg.is_fallback_error(c))
        add("get_error_definition", {"code": c}, reg.get_error_definition(c).to_dict())
        add("get_error_info", {"code": c}, cat.get_error_info(c).to_dict())
    for t in ("[E302] 인터락", "응답 e711 오류", "E000 없음", "코드 없음", "", None, "AE3021 E401 E402", "xE302"):
        add("extract_error_code", {"text": t}, cat.extract_error_code(t))
    for fn in ("get_error_title", "get_error_category", "get_client_action", "get_error_severity", "get_error_cause",
               "get_error_fix"):
        for c in ("E302", "e999", "E000"):
            add(fn, {"code": c}, getattr(reg, fn)(c))
    add("get_legacy_error_dict", {"code": "E302"}, reg.get_legacy_error_dict("E302"))
    add("list_error_codes", {}, reg.list_error_codes())
    samples = [pay.build_error_payload_from_code("E302", detail="G_V_1_인터락=FALSE"),
               pay.build_error_payload_from_code("E302"),
               {"error_code": "", "client_action": "", "message": "제목", "detail": "제목"},
               {"error_code": "E1", "message": "줄\r\n바꿈", "detail": "a\nb" + "x" * 2100}, {}]
    for i, s in enumerate(samples):
        add("format_log_text", {"payload": s}, rep._build_log_text(s))
        add("format_user_text", {"payload": s}, rep._build_user_text(s))
    add("is_fail_payload", {"payload": samples[0]}, pay.is_fail_payload(samples[0]))
    add("is_fail_payload", {"payload": {"result": "ok"}}, pay.is_fail_payload({"result": "ok"}))
    add("is_fail_payload", {"payload": None}, pay.is_fail_payload(None))
    return cases


def main() -> int:
    m = _load_v1()
    reg = m["error_registry"]
    data = {
        "_source": "v1 errors/ (error_registry·error_catalog·app_error·error_payload·error_reporter) 를 importlib 로 실행",
        "exceptions": EXCS,
        "registry": [d.to_dict() for d in reg.ERROR_REGISTRY.values()],
        "cases": build_cases(m),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(data, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"{OUT}: 코드 {len(data['registry'])}개, 경우 {len(data['cases'])}개")
    return 0


if __name__ == "__main__":
    sys.exit(main())

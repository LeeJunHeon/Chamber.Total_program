"""v1 로봇(호스트) 서버 골든 만들기 — v1 host/server.py 를 실제로 띄우고 요청을 보내 받은 바이트·로그·명령 CSV 를 적는다.

실행: cd Chamber.Total_program && python v2/tools/v1golden_robot_server.py  →  v2/tests/golden/robot_server_frames.json

방법
- sys.path 에 저장소 루트를 넣고 importlib 로 v1 host.protocol·host.server·host.router·errors.* 를 불러온다
  (v2 패키지는 v1 을 import 하지 않는다 — 이 도구만 v1 을 실행한다).
- lib.config_common 의 LOG_ROOT_DIR·LOCAL_FALLBACK_SERVER_DIR 을 임시 폴더로 바꿔 remote_cmd CSV 를 거기에 쓰게 한다.
- host.protocol 의 time 을 고정 시각(FIXED_TS)으로 바꿔 응답 헤더 timestamp 를 고정한다.
- v1 Router 에 18개 명령 이름으로 가짜 핸들러를 등록한다(장비 없이 v1 handlers 와 같은 모양의 dict — 실패는 v1
  errors.error_reporter.notify_error_code 로 만든다). 핸들러 예외·error_code 없는 fail·dict 아닌 반환도 만든다.
- HostServer 를 127.0.0.1 임의 포트로 띄우고 시험 요청(정상·잘못된 헤더·부분 수신·두 요청 붙여 보내기·JSON 아님·객체 아님·
  미등록·공백·빈 명령·핸들러 예외 …)을 보낸다. 사례마다 새 연결.
- 사례마다 적는 것: 보낸 바이트(조각·간격), 접속 peer 문자열, 받은 바이트 전체, 서버가 끊었는지, 서버 로그 (tag, text) 전부,
  remote_cmd CSV 행(server_time·duration_ms 제외), 그리고 v2 가짜 내부 API 가 돌려줄 값(api — 등록 명령마다 v1 이
  dispatch(또는 server except 의 notify_unknown_error)에서 만든 응답 명령·data 와 그 사이 로그 줄).
- 꺼짐 응답에 쓰는 v1 실패 data(E203·E205·E227·E412, v2 문구)와 v2 protocol.ERROR_DEFS 코드의 v1 레지스트리 정의도 적는다.
"""
from __future__ import annotations

import asyncio
import csv
import importlib
import json
import os
import struct
import sys
import tempfile
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "v2" / "tests" / "golden" / "robot_server_frames.json"
FIXED_TS = 1793318400          # 2026-10-30 00:00:00 UTC
HDR = "!BBHIQ"


def req(command, data=None, request_id="r1", *, version=1, flags=0, ts=0, body=None, cmd_len=None) -> bytes:
    if body is None:
        obj = {"request_id": request_id, "command": command, "data": {} if data is None else data}
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    cl = len(str(command).encode("utf-8")) if cmd_len is None else cmd_len
    return struct.pack(HDR, version, flags, cl & 0xFFFF, len(body), ts) + body


ALL18 = [
    ("GET_SPUTTER_STATUS", {}), ("GET_LOADING_1_SENSOR", {}), ("GET_LOADING_2_SENSOR", {}),
    ("GET_RECIPE", {"folder": "ch1"}), ("START_SPUTTER", {"ch": 1, "recipe": "C:\\Recipes\\CH1\\레시피 A.csv"}),
    ("START_PLASMA_CLEANING", {"recipe": "C:\\Recipes\\PC\\pc.csv"}), ("VACUUM_ON", {"timeout_s": 660}),
    ("VACUUM_OFF", {}), ("4PIN_UP", {}), ("4PIN_DOWN", {"wait_s": 20, "poll_s": 1, "settle_s": 5}),
    ("CH1_GATE_OPEN", {}), ("CH2_GATE_OPEN", {"ch": 1}), ("CH1_GATE_CLOSE", {}), ("CH2_GATE_CLOSE", {}),
    ("CH1_CHUCK_UP", {}), ("CH2_CHUCK_UP", {"wait_s": 90}), ("CH1_CHUCK_DOWN", {}), ("CH2_CHUCK_DOWN", {}),
]


def _cases() -> list:
    status = req("GET_SPUTTER_STATUS", {}, "s-1")
    c = []

    def add(name, desc, sends, expect, *, half_close=False, behave=None):
        c.append(dict(name=name, desc=desc, sends=[(b, d) for b, d in sends], expect=expect,
                      half_close=half_close, behave=behave or {}))

    add("status_ok", "정상 — GET_SPUTTER_STATUS(핸들러 PLC_REMOTE 로그 1줄)", [(status, 0)], 1)
    add("lowercase_spaces", "명령 정규화 ' get_loading_1_sensor ' → GET_LOADING_1_SENSOR_RESULT",
        [(req(" get_loading_1_sensor ", {}, "lc"), 0)], 1)
    add("korean_fail", "START_SPUTTER 한글 인자 — 핸들러가 E205 fail(ERROR/HOST 보고 + meta)",
        [(req("START_SPUTTER", {"ch": 2, "recipe": "C:\\레시피\\b.csv"}, "ko-1"), 0)], 1)
    add("data_not_dict", "data 가 목록 → {} 로, request_id 없음 → ''",
        [(req("GET_RECIPE", body=json.dumps({"command": "GET_RECIPE", "data": [1, 2]}).encode()), 0)], 1)
    add("numeric_ids", "request_id·command 가 숫자 → str() (command 123 → 미등록 E106 '123_RESULT')",
        [(req(123, body=json.dumps({"request_id": 7, "command": 123}).encode()), 0)], 1)
    add("bad_version", "version 2 → E102 'VERSION_ERROR_RESULT' 후 서버가 끊음(뒤 요청은 처리 안 함)",
        [(req("GET_SPUTTER_STATUS", {}, "v2", version=2) + status, 0)], 1)
    add("header_junk_ignored", "flags·cmd_len·timestamp 엉뚱한 값 — 무시하고 처리",
        [(req("GET_LOADING_2_SENSOR", {}, "junk", flags=0xAB, ts=123456789, cmd_len=999), 0)], 1)
    add("partial", "부분 수신 — 헤더·바디를 1~7바이트 조각으로 30 ms 간격",
        [(status[i:i + 7], 0.03) for i in range(0, len(status), 7)], 1)
    add("two_in_one", "두 요청을 한 번에 — 순서대로 두 응답",
        [(req("GET_LOADING_1_SENSOR", {}, "a") + req("GET_LOADING_2_SENSOR", {}, "b"), 0)], 2)
    add("invalid_json_then_ok", "JSON 아님 → E104 'PARSE_ERROR_RESULT', 연결 유지 → 다음 요청 정상",
        [(req("x", body=b"{bad json"), 0), (status, 0.05)], 2)
    add("invalid_utf8", "UTF-8 아님 → E104", [(req("x", body=b"\xff\xfe\x00"), 0)], 1)
    add("empty_body", "body_len 0 → E104", [(req("x", body=b""), 0)], 1)
    add("not_object", "JSON 이지만 객체 아님([1,2]) → 응답 없이 서버가 끊음(v1 이상 동작)",
        [(req("x", body=b"[1, 2]"), 0)], 0)
    add("not_object_string", "JSON 문자열 → 응답 없이 끊김", [(req("x", body=b'"GET_SPUTTER_STATUS"'), 0)], 0)
    add("unknown_cmd", "미등록 명령 → E106 'FOO_BAR_RESULT'(보고 없음)", [(req("foo_bar", {"x": 1}, "u1"), 0)], 1)
    add("empty_cmd", "command '' → 서버 E105 'UNKNOWN_RESULT'(보고 있음)", [(req("", {}, "e1"), 0)], 1)
    add("missing_cmd", "command 키 없음 → 서버 E105",
        [(req("", body=json.dumps({"request_id": "m1", "data": {}}).encode()), 0)], 1)
    add("blank_cmd", "공백만 → 라우터 E105 'UNKNOWN_RESULT'(보고 없음)", [(req("   ", {}, "b1"), 0)], 1)
    add("handler_exception", "핸들러 예외(소문자 명령) → 'vacuum_on_RESULT' E999(v1 이상 동작)",
        [(req("vacuum_on", {"timeout_s": 5}, "x1"), 0)], 1, behave={"VACUUM_ON": "raise"})
    add("fail_without_code", "핸들러가 error_code 없는 fail → NET 경고 + E110 으로 교체",
        [(req("4PIN_UP", {}, "f1"), 0)], 1, behave={"4PIN_UP": "fail_no_code"})
    add("non_dict_return", "핸들러가 dict 아닌 값 → 라우터 AppError E111 → server except 경로",
        [(req("Ch1_Gate_Open", {}, "n1"), 0)], 1, behave={"CH1_GATE_OPEN": "non_dict"})
    add("eof_mid_body", "바디 중간에 클라이언트가 보내기를 끝냄 → 응답 없이 'Client disconnected'",
        [(status[:20], 0)], 0, half_close=True)
    add("eof_mid_header", "헤더 중간에 끝냄", [(status[:5], 0)], 0, half_close=True)
    add("all18", "등록 명령 18개를 한 번에 붙여 보냄 — 순서대로 18 응답",
        [(b"".join(req(n, d, f"all-{i}") for i, (n, d) in enumerate(ALL18)), 0)], 18)
    return c


# ───────────────────────── v1 가짜 핸들러 ─────────────────────────

def _payloads(err_reporter, LOG):
    def fail(code, detail, meta=None):
        return err_reporter.notify_error_code(code, log=LOG, src="HOST", detail=detail, meta=meta)

    def plc_line(tag, msg):
        LOG("PLC_REMOTE", f"2026-10-30 12:00:00 [{tag}] {msg}")

    def h_status(d):
        plc_line("GET_SPUTTER_STATUS", "read L_ATM (addr=99) -> False")
        return {"result": "success", "message": "OK", "Chamber_1": "idle", "Chamber_2": "running",
                "Loadlock_Chamber": "idle", "vacuum": True,
                "eta": {"CH1": {"remaining_s": None}, "CH2": {"remaining_s": 1234}}}

    t = {
        "GET_SPUTTER_STATUS": h_status,
        "GET_LOADING_1_SENSOR": lambda d: {"result": "success", "message": "OK", "value": True},
        "GET_LOADING_2_SENSOR": lambda d: {"result": "success", "message": "OK", "value": False},
        "GET_RECIPE": lambda d: {"result": "success", "message": "OK", "base_dir": "C:\\Recipes", "folder": "CH1",
                                 "files": ["a.csv", "레시피 A.csv", "b.xlsx"], "count": 3},
        "START_SPUTTER": lambda d: (fail("E205", f"START_SPUTTER_CH{d.get('ch')} 불가 — 공정 실행 중",
                                         {"cmd": f"START_SPUTTER_CH{d.get('ch')}"})
                                    if d.get("ch") == 2 else
                                    {"result": "success", "message": "SPUTTER START OK", "ch": 1, "min_total_s": 1890}),
        "START_PLASMA_CLEANING": lambda d: {"result": "success", "message": "PLASMA CLEANING START OK"},
        "VACUUM_ON": lambda d: {"result": "success", "message": "VACUUM_ON 완료(L_VAC_READY_SW=TRUE)"},
        "VACUUM_OFF": lambda d: {"result": "success", "message": "VACUUM_OFF 완료", "elapsed_s": 12.5},
        "4PIN_UP": lambda d: {"result": "success", "message": "4PIN_UP 완료"},
        "4PIN_DOWN": lambda d: {"result": "success", "message": "4PIN_DOWN 완료"},
    }
    for ch in (1, 2):
        t[f"CH{ch}_GATE_OPEN"] = (lambda ch: lambda d: {"result": "success", "message": f"CH{ch} GATE OPEN 완료"})(ch)
        t[f"CH{ch}_GATE_CLOSE"] = (lambda ch: lambda d: fail("E305", f"CH{ch} 게이트 닫힘 램프 확인 실패",
                                                               {"cmd": f"GATE_CLOSE_CH{ch}"}) if ch == 2 else
                                   {"result": "success", "message": "이미 CLOSED", "current": "closed"})(ch)
        t[f"CH{ch}_CHUCK_UP"] = (lambda ch: lambda d: {
            "result": "success", "message": f"CH{ch} CHUCK MID 완료",
            "current": {"position": "MID", "lamp": {"up": False, "mid": True, "down": False}}})(ch)
        t[f"CH{ch}_CHUCK_DOWN"] = (lambda ch: lambda d: {
            "result": "success", "message": f"CH{ch} CHUCK DOWN 완료",
            "current": {"position": "DOWN", "lamp": {"up": False, "mid": False, "down": True}}})(ch)
    return t


async def _main() -> dict:
    sys.path.insert(0, str(ROOT))
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    tmp = Path(tempfile.mkdtemp(prefix="v1golden_robot_"))
    cfgc = importlib.import_module("lib.config_common")
    cfgc.LOG_ROOT_DIR = str(tmp / "logroot")
    cfgc.LOCAL_FALLBACK_SERVER_DIR = tmp / "fallback"
    proto = importlib.import_module("host.protocol")
    proto.time = types.SimpleNamespace(time=lambda: FIXED_TS)
    server_mod = importlib.import_module("host.server")
    router_mod = importlib.import_module("host.router")
    err_reporter = importlib.import_module("errors.error_reporter")
    err_payload = importlib.import_module("errors.error_payload")
    registry = importlib.import_module("errors.error_registry")

    LOG: list = []

    def log(tag, text):     # 트레이스백의 저장소 경로는 '<v1>' 로(골든이 컴퓨터마다 같게)
        LOG.append([str(tag), str(text).replace(str(ROOT), "<v1>")])

    behave: dict = {}
    table = _payloads(err_reporter, log)
    records: list = []

    def make(name):
        async def h(d):
            kind = behave.get(name)
            if kind == "raise":
                raise RuntimeError("가짜 핸들러 예외 boom")
            if kind == "fail_no_code":
                return {"result": "fail", "message": "4PIN_UP 실패(코드 없음)"}
            if kind == "non_dict":
                return ["not", "a", "dict"]
            return table[name](d)
        return h

    router = router_mod.Router()
    registered = [n for n, _ in ALL18]
    for n in registered:
        router.register(n, make(n))

    orig_dispatch = router.dispatch

    async def rec_dispatch(command, data):
        c = str(command or "").upper().strip()
        r = {"cmd": c, "raw_cmd": command, "data": data, "forwarded": c in registered, "log_start": len(LOG)}
        records.append(r)
        res = await orig_dispatch(command, data)
        r["reply_cmd"], r["body"], r["log_end"] = res[0], res[1], len(LOG)
        return res
    router.dispatch = rec_dispatch

    orig_unknown = server_mod.notify_unknown_error

    def rec_unknown(e, **kw):
        p = orig_unknown(e, **kw)
        r = records[-1]
        r["reply_cmd"], r["body"], r["log_end"] = f"{r['raw_cmd']}_RESULT", p, len(LOG)
        return p
    server_mod.notify_unknown_error = rec_unknown

    srv = server_mod.HostServer("127.0.0.1", 0, router, log)
    await srv.start()
    port = srv._server.sockets[0].getsockname()[1]
    LOG.clear()

    out_cases = []
    for case in _cases():
        behave.clear()
        behave.update(case["behave"])
        records.clear()
        start = len(LOG)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        peer = str(writer.get_extra_info("sockname")[:2])
        for b, delay in case["sends"]:
            writer.write(b)
            await writer.drain()
            if delay:
                await asyncio.sleep(delay)
        if case["half_close"]:
            writer.write_eof()
        rx, closed = await read_frames(reader, case["expect"], wait_eof=case["half_close"] or case["expect"] == 0)
        writer.close()
        for _ in range(300):
            if any(t == "NET" and x == f"Client closed: {peer}" for t, x in LOG[start:]):
                break
            await asyncio.sleep(0.01)
        api = [dict(cmd=r["cmd"], data=r["data"], reply_cmd=r["reply_cmd"], body=r["body"],
                    log=LOG[r["log_start"]:r["log_end"]]) for r in records if r["forwarded"]]
        out_cases.append(dict(name=case["name"], desc=case["desc"],
                              sends=[dict(hex=b.hex(), delay_s=d) for b, d in case["sends"]],
                              expect=case["expect"], half_close=case["half_close"], peer=peer,
                              rx_hex=rx.hex(), server_closed=closed, log=LOG[start:], api=api, csv=[]))
        await asyncio.sleep(0.05)
    await srv.aclose()

    # CSV 행을 사례(peer)별로
    for p in sorted((tmp / "logroot").rglob("remote_cmd_*.csv")):
        with open(p, encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                row.pop("server_time", None)
                row.pop("duration_ms", None)
                for oc in out_cases:
                    if row["peer"] == oc["peer"]:
                        oc["csv"].append(row)

    from v2.robot_server.cache import DOWN_DETAIL, NO_CACHE_CODE, BUSY_CODE
    from v2.robot_server.protocol import ERROR_DEFS
    down = {}
    for cmd, code in list(NO_CACHE_CODE.items()) + [("START_SPUTTER", BUSY_CODE), ("VACUUM_ON", BUSY_CODE)]:
        down[cmd] = err_payload.build_error_payload_from_code(code, detail=DOWN_DETAIL.format(cmd=cmd))
    defs = {}
    for code in ERROR_DEFS:
        d = registry.get_error_definition(code)
        defs[code] = [d.title, d.category, d.client_action, d.severity, d.cause, d.fix]
    return dict(
        meta=dict(tool="v2/tools/v1golden_robot_server.py", fixed_ts=FIXED_TS, python=sys.version.split()[0],
                  v1=["host/protocol.py", "host/server.py", "host/router.py", "errors/error_payload.py",
                      "errors/error_reporter.py", "errors/error_registry.py", "util/log_hub.py"],
                  note="v1 HostServer 를 127.0.0.1 임의 포트로 실제 실행. 핸들러만 가짜(v1 Router 에 등록)."),
        error_defs=defs, down_payloads=down, cases=out_cases)


async def read_frames(reader, expect: int, wait_eof: bool) -> tuple:
    buf = b""
    n = 0
    closed = False

    def count(b):
        k, i = 0, 0
        while len(b) - i >= 16:
            bl = struct.unpack(HDR, b[i:i + 16])[3]
            if len(b) - i - 16 < bl:
                break
            i += 16 + bl
            k += 1
        return k
    while n < expect:
        try:
            ch = await asyncio.wait_for(reader.read(65536), timeout=5.0)
        except asyncio.TimeoutError:
            break
        if not ch:
            closed = True
            break
        buf += ch
        n = count(buf)
    if not closed:
        try:
            ch = await asyncio.wait_for(reader.read(65536), timeout=3.0 if wait_eof else 0.2)
            if ch:
                buf += ch
            else:
                closed = True
        except asyncio.TimeoutError:
            pass
        except ConnectionError:
            closed = True
    return buf, closed


def main() -> int:
    g = asyncio.run(_main())
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(g, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"{OUT} — 사례 {len(g['cases'])}개")
    return 0


if __name__ == "__main__":
    sys.exit(main())

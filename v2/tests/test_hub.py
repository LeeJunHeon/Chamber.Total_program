"""챔버12 장비 메시지 허브(hub.py) — 조립 시험대(Ch12Rig, tick 없이)에 붙여 내부 이벤트·1초 행 → envelope.

- 챔버·출처 정하기(ch1.mfc → ch1·mfc, process:ch2 → ch2·process, heater → ch2·heater, plc → shared …).
- 이벤트·run_start·run_end(챔버별)·안전 알람·명령 결과(run_command — core 없음·예외·정상).
- state: 챔버별 공정 맥락(<c>.proc.*)·장비 링크·엔진 web_state·설정 대기·messages.py 의 필수 필드.
- 소비자: 연결 때 hello·state·최근, Last-Event-ID 이어 받기·지난 실행 번호·gap·느린 소비자만 끊기.
"""
import json
from dataclasses import dataclass, field

import pytest

from v2.common.clock import VirtualClock
from v2.common.commands import Source
from v2.common.messages import MESSAGES
from v2.ch12 import hub as H
from v2.ch12.hub import CLOSED, CmdResult, chamber_of, origin_of
from v2.ch12.sim.rig import Ch12Rig


@pytest.fixture
def rig(tmp_path):
    r = Ch12Rig(VirtualClock(start_wall=1_790_000_000.0), tmp_path)
    H.attach(r, recent_keep=50)
    yield r
    r.outputs.stop()                                          # 조립이 만든 v1 기록 출력 스레드


def _events(r, code=None):
    return [e for e in r.hub.recent if e["type"] == "event" and (code is None or e["data"]["code"] == code)]


def _required(type_):
    m = next(m for m in MESSAGES if m.type == type_)
    return {f.name for f in m.fields if f.required}


@dataclass
class DrvEvent:
    kind: str
    detail: str = ""
    reason: str = None
    data: dict = field(default_factory=dict)


@dataclass
class ProcEvent:
    kind: str
    detail: str = ""
    data: dict = field(default_factory=dict)
    chamber: int = None


@dataclass
class SafetyEvent:
    kind: str
    rule: str
    detail: str = ""
    code: str = None


@pytest.mark.parametrize("source,ev,ch,org", [
    ("ch1.mfc", None, "ch1", "mfc"), ("ch2.ig", None, "ch2", "ig"), ("ch2.dc", None, "ch2", "dc"),
    ("pc.rf", None, "pc", "rf"), ("tsp", None, "tsp", "tsp"), ("tsp.ig", None, "tsp", "ig"),
    ("plc", None, "shared", "plc"), ("camera", None, "shared", "camera"), ("pulse", None, "shared", "pulse"),
    ("process:ch2", None, "ch2", "process"), ("engine:pc", None, "pc", "process"), ("ch1", None, "ch1", "process"),
    ("engine:heater", None, "ch2", "process"), ("heater", None, "ch2", "heater"), ("safety", None, "shared", "safety"),
    ("safety:ch1", None, "ch1", "safety"), ("uplink", None, "shared", "uplink"), ("", None, "shared", "unknown"),
    ("engine:x", ProcEvent("block", chamber=2), "ch2", "process"), ("x", {"chamber": "pc"}, "pc", "x"),
    ("mfc", DrvEvent("flow", data={"chamber": "ch1"}), "ch1", "mfc"),
])
def test_chamber_and_origin(source, ev, ch, org):
    assert (chamber_of(source, ev), origin_of(source, ev)) == (ch, org)


def test_driver_and_engine_events_become_event_messages(rig):
    rig._ev_for("ch2.mfc")(DrvEvent("command_failed", "FLOW_SET 확인 실패", reason="MFC_VERIFY", data={"ch": 3}))
    rig.emit("process:ch1", "block", "블록 2 시작", event={"no": 2, "type": "wait"})
    rig.emit("uplink", "uplink_gap", "놓침", level="warn", code="UPLINK_GAP", event={"fromSeq": 3, "toSeq": 9})
    a, b, c = _events(rig)[-3:]
    assert a["data"] == {"level": "warn", "code": "MFC_VERIFY", "ruleId": None, "message": "FLOW_SET 확인 실패",
                         "context": {"source": "mfc", "chamber": "ch2", "kind": "command_failed", "origin": "ch2.mfc",
                                     "data": {"ch": 3}}}
    assert b["data"]["context"] == {"source": "process", "chamber": "ch1", "kind": "block", "origin": "process:ch1",
                                    "no": 2, "type": "wait"}
    assert b["data"]["code"] == "BLOCK" and b["data"]["level"] == "info"
    assert c["data"]["context"]["fromSeq"] == 3 and c["data"]["context"]["source"] == "uplink"
    assert all(set(e["data"]) == {"level", "code", "ruleId", "message", "context"} for e in (a, b, c))
    assert [e["seq"] for e in (a, b, c)] == sorted(e["seq"] for e in (a, b, c))
    assert {a["instance"], b["instance"]} == {rig.hub.instance} and a["ts"].endswith("Z")
    rig.emit("x", "long", "가" * 900)
    assert len(_events(rig)[-1]["data"]["message"]) == 500


def test_run_start_and_run_end_per_chamber(rig):
    rig.emit("process:ch2", "run_start", "시작", event={"run_id": "r2", "recipe_name": "A.csv", "by": "op@web",
                                                       "name": "row1", "step_no": 3, "params": {"ar": 10}, "blocks": []})
    rig.emit("process:ch1", "run_start", "시작", event=ProcEvent("run_start", data={"run_id": "r1", "params": {}}))
    rig.emit("process:ch2", "run_end", "끝", event={"run_id": "r2", "status": "done", "name": "row1"})
    rs = [e for e in rig.hub.recent if e["type"] == "run_start"]
    re_ = [e for e in rig.hub.recent if e["type"] == "run_end"]
    assert [(e["data"]["chamber"], e["data"]["runId"]) for e in rs] == [("ch2", "r2"), ("ch1", "r1")]
    d = rs[0]["data"]
    assert (d["recipeName"], d["by"], d["stepNo"], d["params"], d["configVersion"]) == ("A.csv", "op@web", 3, {"ar": 10}, 0)
    assert rs[1]["data"]["stepNo"] == 1 and _required("run_start") <= set(d)
    e = re_[0]["data"]
    assert (e["chamber"], e["runId"], e["result"], e["summary"]) == ("ch2", "r2", "done", {"name": "row1"})
    assert _required("run_end") <= set(e)
    assert not _events(rig)                                   # run_* 는 event 로 나가지 않는다


def test_safety_alarm_codes_and_rule_id(rig):
    rig.emit("safety:ch1", "fired", "게이트 열림", level="error", code="GATE_OPEN", event=SafetyEvent("fired", "S12"))
    rig.emit("safety", "seq_start", "정지", event=SafetyEvent("seq_start", "stop"))
    a, b = _events(rig)
    assert a["data"]["ruleId"] == "S12" and b["data"]["ruleId"] is None
    assert rig.hub.alarm_codes["S12"]["code"] == "GATE_OPEN" and rig.hub.alarm_codes["S12"]["chamber"] == "ch1"
    st = rig.hub.state_data({})
    assert st["alarms"][0]["ruleId"] == "S12"
    rig.emit("safety:ch1", "released", "해제", event=SafetyEvent("released", "S12"))
    assert rig.hub.state_data({})["alarms"] == []


def test_run_command_publishes_cmd_result(rig):
    r = rig.hub.run_command("PROCESS_STOP", {}, Source.HMI, cid="c1", by="local")
    assert (r.ok, r.reason) == (False, "CORE_NOT_READY")
    calls = []

    def command(kind, args, source=None, cid=None, by=None):
        calls.append((kind, args, source, cid, by))
        if kind == "BOOM":
            raise RuntimeError("x")
        return CmdResult(kind != "BAD", None if kind != "BAD" else "BAD_ARGS", "" if kind != "BAD" else "value 범위 밖")
    rig.command = command
    assert rig.hub.run_command("PROCESS_STOP", {"a": 1}, Source.WEB, cid="c2", by="a@web").ok
    assert calls[-1] == ("PROCESS_STOP", {"a": 1}, Source.WEB, "c2", "a@web")
    assert rig.hub.run_command("BAD", None, Source.HMI).reason == "BAD_ARGS"
    assert rig.hub.run_command("BOOM", {}, Source.HMI, cid=7).reason == "INTERNAL_ERROR"
    res = [e["data"] for e in rig.hub.recent if e["type"] == "cmd_result"]
    assert [(d["cmdId"], d["kind"], d["status"], d["reasonCode"], d["source"], d["by"]) for d in res] == [
        ("c1", "PROCESS_STOP", "rejected", "CORE_NOT_READY", "hmi", "local"),
        ("c2", "PROCESS_STOP", "accepted", None, "web", "a@web"),
        (None, "BAD", "rejected", "BAD_ARGS", "hmi", None), ("7", "BOOM", "rejected", "INTERNAL_ERROR", "hmi", None)]
    assert res[2]["detail"] == "value 범위 밖" and all(_required("cmd_result") <= set(d) for d in res)
    assert _events(rig, "INTERNAL_ERROR")                     # 예외도 기록에


def test_hello_and_state_fields(rig):
    h = rig.hub.hello()["data"]
    assert _required("hello") <= set(h) and "webSchema" not in h
    assert h["equipment"] == "CH12" and h["runMode"] == "sim" and h["writesAllowed"] is True
    assert h["snapshotColumns"] == list(rig.bus.columns) and h["chambers"] == ["ch1", "ch2", "pc", "tsp"]
    assert h["schemaHash"] == H.param_hash() and len(h["webSchemaHash"]) == 64
    row = {"ts": "t", "seq": 1, "plc.q": "ok", "plc.age_s": 0.2, "ch2.proc.state": "RUNNING", "ch2.proc.run_id": "r9",
           "ch2.proc.block_no": 2, "ch2.proc.block_count": 5, "ch2.proc.block_type": "main", "ch2.proc.remain_s": 30.0,
           "ch2.proc.alarms": "S3,S4", "ch2.proc.owner": "web"}

    class Eng:
        busy = True

        def web_state(self):
            return {"owner": "process", "run": True}
    rig.register_engine("heater", Eng())
    rig.hub.on_row(row)
    st = rig.hub.last_state["data"]
    assert _required("state") <= set(st) and st["procState"] == "BUSY"
    c2 = st["chambers"]["ch2"]
    assert (c2["procState"], c2["runId"], c2["owner"], c2["step"]["no"], c2["step"]["total"], c2["remainS"]) == (
        "RUNNING", "r9", "web", 2, 5, 30.0)
    assert c2["alarms"] == ["S3", "S4"] and st["chambers"]["ch1"]["step"] is None
    assert {"ruleId": "S3", "chamber": "ch2"} in st["alarms"]
    assert st["links"]["plc"] == {"q": "ok", "age": 0.2} and set(st["links"]) == {b.name for b in rig.bus.blocks}
    assert st["heater"] == {"owner": "process", "run": True} and st["engines"]["heater"]["run"]
    assert st["uplink"] == {"enabled": False} and st["snapshot"] is row
    assert rig.hub.last_state not in rig.hub.recent             # state 는 링 버퍼에 넣지 않는다


def test_row_listener_publishes_state_after_engines(rig):
    order = []

    class Eng:
        def on_row(self, view):
            order.append("engine")
    rig.register_engine("x", Eng())
    rig.row_listeners.insert(0, lambda row: order.append("first"))
    rig.row_listeners.append(lambda row: order.append(rig.hub.last_state["data"]["snapshot"]["seq"]))
    rig._on_row({"seq": 5})
    assert order == ["engine", "first", 5]


def test_subscribe_backlog_resume_gap_and_slow(rig):
    hub = rig.hub
    hello = hub.hello()
    hub.on_row({"seq": 1})
    for i in range(10):
        rig.emit("t", "k", f"m{i}")
    sub = hub.subscribe()
    got = [sub.get(0.01) for _ in range(12)]
    assert got[0] is hello and got[1]["type"] == "state" and [g["data"]["message"] for g in got[2:]] == [f"m{i}" for i in range(10)]
    assert sub.get(0.01) is None
    rig.emit("t", "k", "live")
    assert sub.get(0.01)["data"]["message"] == "live"
    mid = got[6]["seq"]
    s2 = hub.subscribe(mid)                                      # Last-Event-ID — 그 뒤부터(hello 없이)
    g2 = [s2.get(0.01) for _ in range(7)]
    assert g2[0]["type"] == "state" and [g["data"]["message"] for g in g2[1:]] == ["m5", "m6", "m7", "m8", "m9", "live"]
    assert g2[1]["seq"] == mid + 1 and s2.get(0.01) is None and s2.gap is None
    s3 = hub.subscribe(10 ** 9)                                  # 지난 실행의 번호 — 처음부터
    assert s3.get(0.01) is hello
    for i in range(60):                                          # 링 버퍼(50)보다 많이
        rig.emit("t", "k", f"x{i}")
    s4 = hub.subscribe(mid)
    assert s4.gap is not None and s4.gap[0] == mid + 1 and s4.gap[1] >= mid + 1
    slow = hub.subscribe(maxsize=3, name="slow")
    while slow.backlog:
        slow.backlog.popleft()
    for i in range(5):
        rig.emit("t", "k", f"y{i}")
    assert slow.closed and slow.reason == "slow" and hub.dropped_subs == 1 and slow not in hub.subs
    assert sub.get(0.01) is not None and not sub.closed            # 다른 소비자는 그대로
    hub.close()
    assert sub.closed and sub.reason == "shutdown"
    left = [sub.get(0.0) for _ in range(200)]
    assert CLOSED in left


def test_listener_errors_do_not_reach_control(rig):
    rig.hub.publish = None                                       # 메시지 만들기가 깨져도
    rig.emit("ch1.mfc", "flow", "x")                             # 예외가 조립 밖으로 나가지 않는다
    assert rig.hub.errors >= 1
    del rig.hub.publish


def test_nan_in_event_is_null_on_the_wire(rig):
    from v2.common.messages import dumps_json
    rig.emit("ch1.ig", "pressure", "x", event={"v": float("nan")})
    raw = dumps_json(_events(rig)[-1])
    assert json.loads(raw)["data"]["context"]["v"] is None and b"NaN" not in raw


def test_optional_module():
    assert H.optional_module("v2.ch12.no_such_module_x") is None
    assert H.optional_module("v2.ch12.hub") is H

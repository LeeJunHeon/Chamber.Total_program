"""포털 이벤트 계약(vanam-process lib/ops2/ch12/chatCards.ts — 포털 서버가 정한 모양)을 장비 쪽이 지키는지 — 허브 envelope 로 확인.

1) run_end summary.result_detail(엔진 run_end 상세)   2) run_start processTotal(리스트 '(i/n)')
3) 플라즈마 클리닝·TSP 도 run_start·run_end(챔버 pc·tsp)   4) TSP 이벤트 챔버 tsp
5) error_event {src, code, message}(로봇·로봇 서버 실패 보고) · monitor_card {text}(공정 모니터·엑셀 Arc) ·
   source oes·kind anomaly {text}(D28) · client_process {kind, info}(Robot CSV 기록 이벤트)
"""
from __future__ import annotations

from datetime import datetime

import pytest

from v2.common.clock import VirtualClock
from v2.ch12 import hub as H
from v2.ch12.sim.rig import Ch12Rig


@pytest.fixture
def rig(tmp_path):
    r = Ch12Rig(VirtualClock(start_wall=1_790_000_000.0), tmp_path)
    H.attach(r, recent_keep=200)
    yield r
    r.outputs.stop()


def _msgs(r, type_):
    return [e["data"] for e in r.hub.recent if e["type"] == type_]


def _ctx(r, kind):
    return [e["data"] for e in r.hub.recent if e["type"] == "event" and e["data"]["context"]["kind"] == kind]


def _data(ev):
    c = ev["context"]
    return c["data"] if isinstance(c.get("data"), dict) else c        # 포털과 같은 읽기(chatCards.ts:583)


def test_engine_run_start_total_and_run_end_result_detail(rig):
    from v2.ch12.process_engine import ProcEvent
    rig.emit("engine:ch1", "run_start", "A", event=ProcEvent("run_start", "A", data={
        "run_id": "R1", "owner": "web", "params": {"process_name": "A"}, "recipe_rev": 0, "process_index": 2,
        "process_total": 3}))
    det = {"process_name": "A", "result": "FAIL", "errors": ["E302 압력"], "warnings": ["Chuck 위치 이동 실패"],
           "chuck_position": "up"}
    rig.emit("engine:ch1", "run_end", "A", code="E302", level="warn", event=ProcEvent("run_end", "A", "E302", data={
        "result": "error", "run_id": "R1", "result_detail": det}))
    rs, re_ = _msgs(rig, "run_start")[-1], _msgs(rig, "run_end")[-1]
    assert (rs["stepNo"], rs["processTotal"]) == (2, 3)
    assert re_["reasonCode"] == "E302" and re_["summary"] == {"name": "A", "result_detail": det}


def test_single_run_has_no_total(rig):
    from v2.ch12.process_engine import ProcEvent
    rig.emit("engine:ch2", "run_start", "B", event=ProcEvent("run_start", "B", data={"run_id": "R2", "params": {}}))
    rs = _msgs(rig, "run_start")[-1]
    assert rs["stepNo"] == 1 and rs["processTotal"] is None


def test_pc_run_start_and_run_end(rig):
    from v2.ch12 import plasma_cleaning as PCM
    pc = PCM.attach(rig)
    pc.run_id, pc.owner_src, pc.by = "pc-1", "robot", "robot"
    pc.card("notify_process_started", payload={"process_note": "Plasma Cleaning", "process_time": 1.0,
                                               "prefix": "CH1 Plasma Cleaning", "ch": 1}, flush=True)
    pc.card("notify_process_finished_detail", level="warn", ok=False, flush=True,
            payload={"process_name": "Plasma Cleaning", "prefix": "CH1 Plasma Cleaning", "ch": 1, "status": "failed",
                     "stopped": False, "error_code": "E511", "reason": "x", "errors": ["x"]})
    rs, re_ = _msgs(rig, "run_start")[-1], _msgs(rig, "run_end")[-1]
    assert (rs["chamber"], rs["runId"], rs["name"], rs["owner"], rs["params"]["process_note"]) == (
        "pc", "pc-1", "CH1 Plasma Cleaning", "robot", "Plasma Cleaning")
    assert (re_["chamber"], re_["runId"], re_["result"], re_["reasonCode"]) == ("pc", "pc-1", "error", "E511")
    assert re_["summary"]["result_detail"]["errors"] == ["x"]
    assert [e["context"]["chamber"] for e in _ctx(rig, "card")][-1] == "pc"          # 카드 이벤트는 그대로
    pc.card("notify_process_started", payload={"prefix": "CH1 Plasma Cleaning"})
    pc.card("notify_process_finished_detail", ok=False, payload={"status": "stopped", "stopped": True})
    assert (_msgs(rig, "run_end")[-1]["result"], _msgs(rig, "run_end")[-1]["reasonCode"]) == ("aborted", "USER_STOP")
    pc.card("notify_process_started", payload={})
    pc.card("notify_process_finished_detail", ok=True, payload={"status": "success"})
    assert (_msgs(rig, "run_end")[-1]["result"], _msgs(rig, "run_end")[-1]["reasonCode"]) == ("done", None)


def test_tsp_events_chamber_tsp_and_runs(rig):
    from v2.ch12 import tsp_engine as TE
    from v2.ch12.run_state import RunState
    t = TE.attach(rig, RunState(rig.clock), schedule=False)
    t.run_id, t.owner = "tsp-1", "web"
    t._log("[STATE] x")
    t._card("started", {"params": {"process_note": "TSP run: target=1.000e-06, cycles=2"}, "run_id": "tsp-1"})
    t._card("finished", {"ok": False, "detail": {"process_name": "TSP", "errors": ["timeout"]}, "run_id": "tsp-1",
                         "code": "E808"}, level="error")
    logs = [e for e in _ctx(rig, "log") if e["context"]["origin"] == "engine:tsp"]
    assert logs and all(e["context"]["chamber"] == "tsp" for e in logs)
    cards = [e for e in _ctx(rig, "card") if e["context"]["chamber"] == "tsp"]
    assert [_data(c)["card"] for c in cards] == ["started", "finished"]
    rs, re_ = _msgs(rig, "run_start")[-1], _msgs(rig, "run_end")[-1]
    assert (rs["chamber"], rs["runId"], rs["name"], rs["owner"]) == ("tsp", "tsp-1", "TSP", "web")
    assert (re_["chamber"], re_["result"], re_["reasonCode"], re_["summary"]["result_detail"]["errors"]) == (
        "tsp", "error", "E808", ["timeout"])
    t._card("started", {"params": {}, "run_id": "tsp-2"})
    t._card("finished", {"ok": False, "detail": {"process_name": "TSP", "stopped": True}, "run_id": "tsp-2"})
    assert (_msgs(rig, "run_end")[-1]["result"], _msgs(rig, "run_end")[-1]["reasonCode"]) == ("aborted", "USER_STOP")


def test_error_event_from_robot_ops_and_robot_server(rig):
    from v2.ch12 import robot_api as RA

    class Ops:
        on_report = None
    RA.report_to_emit(rig.emit)({"error_code": "E301", "message": "게이트 닫힘 미확인", "detail": "x"}, "HOST")
    with RA.RobotApi(Ops(), port=0, emit=rig.emit) as api:
        api._event({"kind": "error_event", "code": "E104", "detail": "[E104] 잘못된 JSON 본문 | 상세: y", "src": "HOST"})
    evs = _ctx(rig, "error_event")
    assert [(_data(e)["src"], _data(e)["code"], _data(e)["message"]) for e in evs] == [
        ("HOST", "E301", "[E301] 게이트 닫힘 미확인 | 상세: x"), ("HOST", "E104", "[E104] 잘못된 JSON 본문 | 상세: y")]
    assert [e["code"] for e in evs] == ["E301", "E104"] and evs[0]["level"] == "warn"


def test_monitor_card_text_from_engine_monitor_and_excel_arc(tmp_path):
    from v2.ch12 import process_monitor as PM
    from v2.tests import proc_rig as R
    rig = R.make(tmp_path, chambers=("ch2",))
    H.attach(rig, recent_keep=200)
    try:
        e = rig.eng["ch2"]
        m = PM.attach(rig, e)
        assert e.excel.on_arc_alert == m.excel_arc_alert
        m.mon.notify_refp_warning(6.0, 5.0, process_name="P")
        e.excel.on_arc_alert(2, "P2", 4000, 1500)
        cards = _ctx(rig, "monitor_card")
        assert [_data(c)["text"] for c in cards] == [
            "⚠️ CH2 REFP 경고 (공정 계속 진행)\n공정: P\nREFP: 6.0W  ≥  기준 5.0W",
            "⚠️ *CH2 Arc 경고*\n공정: `P2`\nSoft Arc: 4000회 / Hard Arc: 1500회 (합계 5500회)"]
        assert all(c["context"]["chamber"] == "ch2" and c["context"]["source"] == "process" for c in cards)
    finally:
        rig.outputs.stop()
        rig.clock.close()


def test_oes_anomaly_event(rig, monkeypatch):
    from v2.ch12.drivers.oes_link import OesLink
    link = rig.oes.get("ch1") or OesLink(rig.clock, None, rig.cfg, True, on_event=rig._ev_for("ch1.oes"), chamber="ch1")
    text = "⚠️ OES 이상 감지 (CH1)\nAr 811.5nm 급등: 1200.0 (평균 1000.0, +20.0%)\n측정 행: 33"
    link._on_daemon_msg({"kind": "anomaly", "text": text, "v": 2, "id": "c1"})
    link._on_oneshot_msg({"kind": "anomaly", "text": text})
    evs = _ctx(rig, "anomaly")
    assert len(evs) == 2
    assert all((e["context"]["source"], e["context"]["chamber"], _data(e)["text"]) == ("oes", "ch1", text) for e in evs)
    from v2.ch12.workers import oes_common as C                       # 워커 쪽: 웹훅 대신 JSONL 한 줄
    got = []
    monkeypatch.setattr(C, "_print_json", got.append)
    monkeypatch.setattr(C, "_post_anomaly_webhook", lambda m: got.append(("POSTED", m)))
    C._report_anomaly(text)
    assert got == [{"kind": "anomaly", "text": text}]


def test_client_process_event_kind_and_info(rig):
    from v2.ch12 import robot_api as RA
    sink = RA.client_process_to_emit(rig.emit)
    info = {"key": "k1", "target": "CH1", "request_id": "r-9", "received_at": datetime(2026, 10, 30, 9, 0, 1),
            "started_at": None, "recipe_name": "A.csv", "row_count": "2", "process_names": "A / B", "log_files": []}
    sink("started", info)
    sink("finished", dict(info, result="거절", reason="E205", finished_at=datetime(2026, 10, 30, 9, 0, 2)))
    evs = _ctx(rig, "client_process")
    assert [_data(e)["kind"] for e in evs] == ["started", "finished"]
    i0, i1 = _data(evs[0])["info"], _data(evs[1])["info"]
    assert i0["received_at"] == "2026-10-30T09:00:01" and i0["target"] == "CH1" and i0["started_at"] is None
    assert (i1["result"], i1["reason"], i1["finished_at"]) == ("거절", "E205", "2026-10-30T09:00:02")


def test_robot_csv_sink_wiring(tmp_path):
    """outputs.robot_csv.RobotCsv.set_event_sink 에 그대로 꽂힌다(started 1회·finished 1회)."""
    from v2.ch12 import robot_api as RA
    from v2.ch12.outputs.robot_csv import RobotCsv
    got = []
    r = RobotCsv({"LOG_ROOT_DIR": str(tmp_path / "logs"), "LOCAL_FALLBACK_ROOT": str(tmp_path / "fb"),
                  "HOST_LOG_NAS_DIR": str(tmp_path / "nas"), "HOST_LOG_LOCAL_DIR": str(tmp_path / "hostlocal")})
    try:
        r.set_event_sink(RA.client_process_to_emit(lambda *a, **k: got.append((a, k))))
        key = r.request(target="CH1", request_id="q1", peer="p", received_at=datetime(2026, 10, 30, 9, 0, 0))
        r.mark_started(key, started_at=datetime(2026, 10, 30, 9, 0, 5))
        r.finalize(key, "성공", "", datetime(2026, 10, 30, 9, 30, 0))
    finally:
        r.close()
    kinds = [k["event"].data["kind"] for _a, k in got]
    assert kinds == ["started", "finished"] and all(a[:2] == ("robot", "client_process") for a, _k in got)
    assert got[1][1]["event"].data["info"]["result"] == "성공"

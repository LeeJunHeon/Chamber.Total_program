"""웹 로그 평소 보기(2026-11-18 D414) — 공정 진행 줄(공정 단계 시작·끝 등)이 묶음 eventKinds 의 main 이어서 공정 중에도
기본 로그가 비지 않는다. 공정 단계(block) 사건 data 에 total(전체 블록 수)·elapsed_s(끝 — 걸린 시간)가 있어 화면이
'1/1 TEST 시작'·'1 TEST 끝 — 건너뜀, 0:05' 처럼 쓴다(끝 줄은 다음 블록으로 넘어갈 때·건너뛸 때 — 마지막 블록의 끝은 run_end 가 대신한다). 화면을 연 직후의 최근 줄은 허브가 연결 때 최근 메시지(링 버퍼)를 다시 보낸다."""
from pathlib import Path

from v2.common.clock import VirtualClock
from v2.common.commands import Source
from v2.ch12 import web_schema as WS
from v2.ch12.sim.rig import Ch12FullRig

MAIN = {("process", k) for k in ("block", "skip", "pause", "resume", "edit", "adjust", "recipe_loaded", "popup", "chat", "state",
                                 "result", "run_start", "run_end")}


def test_process_progress_kinds_are_main_and_chat_cards_are_detail():
    lv = {(s, k): l for s, k, _lab, l in WS.EVENT_KINDS}
    assert all(lv.get(x) == "main" for x in MAIN), {x: lv.get(x) for x in MAIN}
    assert lv[("process", "card")] == "detail" and lv[("process", "log")] == "detail"
    assert len({(s, k) for s, k, _l, _v in WS.EVENT_KINDS}) == len(WS.EVENT_KINDS)          # 같은 짝 두 번 없음


def test_block_events_carry_total_and_elapsed_and_are_replayed_to_a_new_viewer(tmp_path):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = Ch12FullRig(c, Path(tmp_path), cfg={"CAMERA_MIN_FREE_GB": 0.0}, robot_api=False)
    try:
        assert rig.start()
        rig.loop.run(2)
        assert rig.command("RECIPE_LOAD", {"ch": 1, "rows": [{"#": "test", "Process_name": "T1", "time": "30s"}], "name": "t"},
                           source=Source.HMI).ok
        assert rig.command("PROCESS_START", {"ch": 1}, source=Source.HMI).ok
        rig.loop.run(6)
        assert rig.command("BLOCK_SKIP", {"ch": 1}, source=Source.HMI).ok          # 블록 끝 줄(건너뜀)
        rig.loop.run(3)
        lv = {(k["source"], k["kind"]): k["level"] for k in WS.bundle()["eventKinds"]}
        blocks = [e for e in rig.hub.recent if e["type"] == "event" and e["data"]["context"].get("kind") == "block"
                  and e["data"]["context"].get("chamber") == "ch1"]
        start = [e["data"]["context"]["data"] for e in blocks if e["data"]["context"]["data"].get("phase") == "start"]
        end = [e["data"]["context"]["data"] for e in blocks if e["data"]["context"]["data"].get("phase") == "end"]
        assert start and start[0]["total"] == 1 and start[0]["no"] == 1 and start[0]["type"] == "test"
        assert end and end[0]["total"] == 1 and end[0]["result"] == "skipped" and 4.0 <= end[0]["elapsed_s"] <= 8.0, end
        assert all(lv[(e["data"]["context"]["source"], "block")] == "main" for e in blocks)
        # 새로 붙은 화면(연결 때 첫 묶음) — 최근 메시지에 공정 단계 줄이 들어 있다
        first = rig.hub.first_messages(None) if hasattr(rig.hub, "first_messages") else list(rig.hub.recent)
        assert any(m.get("type") == "event" and m["data"]["context"].get("kind") == "block" for m in first)
    finally:
        rig.stop()
        c.close()

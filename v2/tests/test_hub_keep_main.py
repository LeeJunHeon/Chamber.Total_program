"""화면을 다시 열어도 앞선 공정 진행 줄이 보인다(2026-11-19 D418) — 허브의 기본 보기 줄 챔버별 보관(HUB_KEEP_MAIN).

현상: sim 에서 CH1·CH2 동시 공정 중 화면을 몇 분 뒤 처음 열면 기록 탭 로그가 2줄뿐, CH2 알림·로그 칸이 '최근 사건이 없습니다'.
원인: 연결 때 다시 보내는 최근 메시지 링(500)을 CH2 RF 램프의 상세 줄(DCV 쓰기 로그·Ramp-Up 상태·DISPLAY·MFC IO)이 몇 초 만에 밀어냄.
고침: 기본 보기 줄(묶음 eventKinds main·경고·오류·run_*·cmd_result·config_ack)은 챔버별 링에 따로 보관하고 연결 때 먼저(시간 순서) 다시 보냄.
"""
from types import SimpleNamespace

import pytest

from v2.common.paramdef import ConfigRangeError
from v2.ch12 import hub as H


class _Sys:
    def __init__(self, cfg=None):
        self.cfg = cfg or {}
        self.event_listeners, self.row_listeners = [], []
        self.clock = SimpleNamespace(wall=lambda: 1_760_000_000.0)


def _ev(hub, source, kind, chamber, level="info", msg="", **data):
    return hub.publish("event", {"level": level, "code": kind.upper(), "ruleId": None, "message": msg or f"{source} {kind}",
                                 "context": {"source": source, "chamber": chamber, "kind": kind, "data": data}})


def _progress(hub):
    """레시피 적재·공정 시작·블록 1~8 시작/끝(CH1·CH2) + 경고 하나 — 화면이 처음 열릴 때 보여야 할 줄."""
    want = []
    for ch in ("ch1", "ch2"):
        want.append(_ev(hub, "process", "recipe_loaded", ch, msg=f"{ch} 레시피"))
        want.append(hub.publish("run_start", {"runId": f"r-{ch}", "chamber": ch, "recipeName": "t"}))
    for no in range(1, 9):
        for ch in ("ch1", "ch2"):
            want.append(_ev(hub, "process", "block", ch, no=no, phase="start", type="wait", total=12))
            want.append(_ev(hub, "process", "block", ch, no=no, phase="end", type="wait", result="done", total=12))
    want.append(_ev(hub, "mfc", "status", "ch2", level="warn", msg="Ch2 GAS 불안정!"))
    want.append(hub.cmd_result("c1", "PROCESS_START", H.CmdResult(True), "web", "u"))
    return want


def _flood(hub, n):
    """CH2 RF 램프의 상세 줄(초당 약 120 개) — DCV 쓰기 로그 2줄·Ramp-Up 상태·DISPLAY·MFC IO."""
    for i in range(n):
        k = i % 5
        if k in (0, 1):
            _ev(hub, "plc", "log", "shared", msg=f"[DCV] write ch2 rf {i}")
        elif k == 2:
            _ev(hub, "rf", "status", "ch2", msg=f"Ramp-Up {i} W")
        elif k == 3:
            _ev(hub, "rf", "display", "ch2", msg=f"DISPLAY {i}")
        else:
            _ev(hub, "mfc", "io", "ch2", msg=f"[SEND] R60 {i}")


def test_main_lines_survive_thousands_of_detail_lines_and_come_first_in_order():
    hub = H.MessageHub(_Sys())
    hub.publish("hello", {"x": 1})
    want = _progress(hub)
    _flood(hub, 5000)                                           # 최근 링(500)을 열 번 넘게 밀어냄
    assert all(e not in hub.recent for e in want)              # 예전 방식이면 화면에 하나도 안 감
    sub = hub.subscribe(None)
    first = list(sub.backlog)
    assert first[0]["type"] == "hello"
    got = [e for e in first if e["type"] not in ("hello", "state")]
    assert [e["seq"] for e in got if H.main_context(e)] == [e["seq"] for e in want]     # 빠짐없이, 시간 순서
    assert [e["seq"] for e in got] == sorted(e["seq"] for e in got)                      # 전체도 seq 순서(오래된 것부터)
    assert len(got) == len(want) + H.RECENT_KEEP                                          # 기본 보기 줄 + 최근 링 그대로
    assert len({e["id"] for e in got}) == len(got)                                       # 같은 것 두 번 없음
    assert first.index(want[-1]) < first.index(hub.recent[0])                            # 최근 링보다 먼저


def test_issue_lines_and_per_chamber_rings_are_separate_and_bounded():
    hub = H.MessageHub(_Sys({"HUB_KEEP_MAIN": 50}))
    for i in range(80):
        _ev(hub, "process", "block", "ch1", no=i, phase="start")
    pc = _ev(hub, "process", "state", "pc", msg="PC 공정 결과")
    err = _ev(hub, "dc", "log", "ch2", level="error", msg="DC 과전류")            # detail 종류라도 오류면 보관
    tsp = _ev(hub, "tsp", "status", "tsp", level="warn", msg="TSP 경고")
    _flood(hub, 3000)
    assert len(hub.keep["ch1"]) == 50                                           # 챔버마다 개수 상한(설정)
    assert pc in hub.keep["pc"] and err in hub.keep["ch2"] and tsp in hub.keep["tsp"]
    first = hub.first_messages(None)
    assert pc in first and err in first and tsp in first
    ch1 = [e for e in first if e["type"] == "event" and e["data"]["context"]["chamber"] == "ch1" and e["data"]["context"]["kind"] == "block"]
    assert [e["data"]["context"]["data"]["no"] for e in ch1] == list(range(30, 80))   # CH1 이 많아도 다른 챔버 줄은 밀지 않음


def test_detail_lines_are_not_kept_and_resume_from_last_seq():
    hub = H.MessageHub(_Sys())
    want = _progress(hub)
    _flood(hub, 2000)
    assert not any(H.main_context(e) for dq in hub.keep.values() for e in dq if e["type"] == "event"
                   and e["data"]["context"]["kind"] in ("log", "display", "io") and e["data"]["level"] == "info")
    mid = want[10]["seq"]                                                       # 이어 받기 — last_seq 뒤의 것만
    again = [e for e in hub.subscribe(mid).backlog if e["type"] not in ("hello", "state")]
    assert again[0]["seq"] == want[11]["seq"] and all(e["seq"] > mid for e in again)


@pytest.mark.parametrize("bad", [0, 49, 5001, "300", 300.0, True, None])
def test_keep_count_setting_range_rejected_not_fixed(bad):
    with pytest.raises(ConfigRangeError):
        H.MessageHub(_Sys({"HUB_KEEP_MAIN": bad}))
    from v2.ch12.params import table
    p = next(x for x in table().params if x.key == "HUB_KEEP_MAIN")
    assert (p.default, p.min, p.max) == (H.KEEP_MAIN_DEFAULT, *H.KEEP_MAIN_RANGE)


def test_main_definition_is_the_web_bundle_main_set():
    """보관 기준 = 웹 묶음 eventKinds 의 main(D414 정의) + 경고·오류 + run_*·cmd_result·config_ack — 같은 표 한 곳."""
    from v2.ch12 import web_schema as WS
    main = {(k["source"], k["kind"]) for k in WS.bundle()["eventKinds"] if k["level"] == "main"}
    assert H.main_kinds() == frozenset(main) and ("process", "block") in main and ("plc", "log") not in main
    hub = H.MessageHub(_Sys())
    assert H.main_context(_ev(hub, "rf", "display", "ch2")) is None
    assert H.main_context(_ev(hub, "rf", "display", "ch2", level="warn")) == "ch2"
    assert H.main_context(hub.publish("config_ack", {"version": 1})) == "shared"
    assert H.main_context(hub.publish("run_end", {"runId": "r", "chamber": "pc"})) == "pc"
    assert H.main_context(_ev(hub, "process", "block", "nowhere")) == "shared"


def test_full_rig_new_viewer_sees_earlier_block_lines_after_a_detail_flood(tmp_path):
    """시험대(sim 장비 + 실제 엔진): 공정 진행 뒤 상세 줄 수천 개가 몰려도 새로 붙은 화면 첫 묶음에 앞선 블록 줄·공정 시작이 있다."""
    from v2.common.clock import VirtualClock
    from v2.common.commands import Source
    from v2.ch12.sim.rig import Ch12FullRig
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = Ch12FullRig(c, tmp_path, cfg={"CAMERA_MIN_FREE_GB": 0.0}, robot_api=False)
    try:
        assert rig.start()
        rig.loop.run(2)
        assert rig.command("RECIPE_LOAD", {"ch": 1, "rows": [{"#": "test", "Process_name": "T1", "time": "30s"}], "name": "t"},
                           source=Source.HMI).ok
        assert rig.command("PROCESS_START", {"ch": 1}, source=Source.HMI).ok
        rig.loop.run(6)
        before = [e for e in list(rig.hub.recent) if H.main_context(e)]
        assert any(e["type"] == "run_start" for e in before)
        for i in range(3000):                                   # RF 램프 같은 상세 줄 폭주(장비 이벤트 경로 그대로)
            rig.emit("ch2.rf", "display", f"DISPLAY {i}")
        assert all(e not in rig.hub.recent for e in before)
        first = rig.hub.first_messages(None)
        assert all(e in first for e in before)                  # 앞선 공정 진행 줄이 그대로 다시 감
    finally:
        rig.stop()
        c.close()

"""PLC 버튼 확인창·기록 강화(2026-11-20 D425 — 사용자 결정: 공정 중에도 누를 수 있게 둔다, 잠그지 않음).

- 버튼 정의(묶음 plcButtons): 위험 분류(사람 — 벤트·도어·게이트·로드락 핀·척 모터 / 장비 — 터보·로터리 끄기, 포어라인·러핑·메인 밸브,
  Power Select), 공정 챔버(공유 건 셔터·Power Select 는 CH2), 짧은 이름, 위험 문구(plcHazardText — RF 전용 릴레이 문구 포함).
- 누를 때마다 사건 plc_button 하나(받음·쓰기 실패·거부 모두): 버튼·켬/끔·누가(웹 이름 + 계정, 노트북)·결과·그 챔버 공정 상태,
  그 챔버 탭(context.chamber), SYSTEM 일자 로그에 늘, 공정 중이면 그 챔버 공정 로그(화면 로그·txt)에도 한 줄.
시험대: sim.rig.Ch12FullRig(실행 프로그램과 같은 조립 — 시뮬레이터·실제 드라이버·실제 엔진, 가상 시계). 공정 = TEST 행(20 s).
"""
import glob
from pathlib import Path
from types import SimpleNamespace

import pytest

from v2.common.clock import VirtualClock
from v2.common.commands import Source
from v2.ch12 import commands as C
from v2.ch12.core import Ch12Core
from v2.ch12.plc_button_log import line, who_of
from v2.ch12.sim.rig import Ch12FullRig

WEB, HMI = Source.WEB, Source.HMI
HONG = "hong@vanam.co.kr"
TEST_ROWS = [{"#": "test", "Process_name": "T1", "time": "20s"}]


# ───────────────────────── 버튼 정의 ─────────────────────────
def _b(t):
    return C.PLC_BY_TARGET[t]


def test_hazard_classes_by_direction():
    person = ["vent_1_sw", "vent_2_sw", "l_vent_sw", "door_1_open_sw", "door_2_close_sw", "g_v_1_open_sw", "g_v_2_close_sw",
              "l_pin_up_sw", "l_pin_down_sw", "z_m_p_1_sw", "z_m_p_1_cw_sw", "z_m_p_2_ccw_sw", "z_m_p_2_mid_sw"]
    for t in person:
        assert (_b(t).hazard(True), _b(t).hazard(False)) == ("person", "person"), t
    for t in ("turbo_1_sw", "turbo_2_sw", "r_p_1_sw", "r_p_2_sw", "l_r_p_sw"):        # 끄기만 장비 위험
        assert (_b(t).hazard(True), _b(t).hazard(False)) == (None, "equipment"), t
    for t in ("f_v_1_sw", "r_v_2_sw", "m_v_1_sw", "m_v_2_sw", "l_r_v_sw", "sw_rf_select", "sw_power_select"):
        assert (_b(t).hazard(True), _b(t).hazard(False)) == ("equipment", "equipment"), t
    for t in ("ar_1_gas_sw", "main_2_gas_sw", "main_shutter_1_sw", "shutter_1_sw", "z_m_p_1_stop_sw", "buzzer_stop_sw",
              "l_vac_ready_sw"):
        assert (_b(t).hazard(True), _b(t).hazard(False)) == (None, None), t
    assert {b.target for b in C.PLC_BUTTONS if b.hazard(True) or b.hazard(False)} >= set(person)


def test_proc_chamber_short_name_and_bundle():
    assert (_b("vent_1_sw").proc_chamber, _b("door_2_open_sw").proc_chamber) == ("ch1", "ch2")
    for t in ("shutter_1_sw", "shutter_2_sw", "shutter_3_sw", "sw_rf_select", "sw_power_select"):
        assert (_b(t).chamber, _b(t).proc_chamber) == ("shared", "ch2"), t                 # 공정이 쓰는 공유 버튼 → 그 챔버(CH2)
    assert _b("buzzer_stop_sw").proc_chamber is None and _b("l_pin_up_sw").proc_chamber is None
    assert [_b(t).short for t in ("vent_1_sw", "door_2_open_sw", "shutter_1_sw", "l_pin_up_sw", "sw_rf_select")] == \
        ["CH1 벤트", "CH2 도어 열기", "건 셔터 G1", "로드락 4핀 올림", "Power Select 릴레이(SW_RF_SELECT)"]
    pub = C.public()
    v = next(b for b in pub["plcButtons"] if b["target"] == "turbo_1_sw")
    assert v["short"] == "CH1 터보 펌프" and v["procChamber"] == "ch1" and v["hazard"] == {"on": None, "off": "equipment"}
    t = pub["plcHazardText"]
    assert t["person"].startswith("사람 위험 — ") and t["equipment"].startswith("장비 위험 — ")
    assert "RF 전용 릴레이" in t["rf_select_dcp"] and "DC Pulse" in t["rf_select_dcp"]
    arg = next(a for a in C.get("PLC_BUTTON").args if a.name == "by_name")
    assert arg.type == "str" and not arg.required


def test_who_and_line_text():
    assert who_of("web", HONG, "홍길동") == "홍길동(웹)" and who_of("web", HONG, "홍길동", account=True) == f"홍길동(웹, {HONG})"
    assert who_of("web", HONG, None) == "hong(웹)" and who_of("local", "local", None) == "노트북"
    d = dict(short="CH1 벤트", on=True, origin="web", by=HONG, by_name="홍길동", result="done",
             process={"chamber": "ch1", "busy": True, "block": "메인 공정(시간)", "recipe": "R1"})
    assert line(d, account=False) == "PLC 버튼 · CH1 벤트 켬 · 홍길동(웹) (CH1 공정 중 · 메인 공정(시간))"
    assert line(d, account=True) == f"PLC 버튼 · CH1 벤트 켬 · 홍길동(웹, {HONG}) (CH1 공정 중 · 메인 공정(시간) · 레시피 R1)"
    r = dict(d, on=False, origin="local", by="local", result="rejected", reason="LINK_DOWN", detail="PLC 연결 없음", process=None)
    assert line(r, account=True) == "PLC 버튼 · CH1 벤트 끔 · 노트북 — 거부: PLC 연결 없음"


# ───────────────────────── 시험대(실제 조립) ─────────────────────────
@pytest.fixture
def rig(tmp_path):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    r = Ch12FullRig(c, Path(tmp_path), cfg={"CAMERA_MIN_FREE_GB": 0.0}, robot_api=False)
    assert r.start()
    r.loop.run(2)
    r.tmp = Path(tmp_path)
    yield r
    r.stop()
    c.close()


def _evs(rig):
    return [e for e in rig.events if e.kind == "plc_button"]


def _hub(rig):
    return [m["data"] for m in rig.hub.recent if m["type"] == "event" and m["data"]["context"].get("kind") == "plc_button"]


def _syslog(rig):
    for e in rig.core.proc.values():
        e.writer.flush(5)
    out = []
    for f in sorted(glob.glob(str(rig.tmp / "**" / "SYSTEM" / "*.log"), recursive=True)):
        out += [l.rstrip("\n") for l in open(f, encoding="utf-8") if "] [PLC] PLC 버튼" in l]
    return out


def _proc_lines(rig, ch):
    return [l for l in rig.core.proc[ch].log_lines if ":PLC] PLC 버튼" in l]


def _press(rig, target, on, src=WEB, by=HONG, by_name="홍길동", run=1):
    a = {"target": target, "on": on}
    if by_name is not None:
        a["by_name"] = by_name
    r = rig.command("PLC_BUTTON", a, source=src, by=by if src == WEB else None)
    rig.loop.run(run)
    return r


def test_web_press_idle_one_event_system_log_no_process_line(rig):
    assert _press(rig, "vent_1_sw", True).ok
    assert rig.sims["plc"].coil("VENT_1_SW")
    ev = _evs(rig)
    assert len(ev) == 1 and (ev[0].source, ev[0].level, ev[0].code) == ("command:ch1", "info", "PLC_BUTTON")
    assert ev[0].detail == f"PLC 버튼 · CH1 벤트 켬 · 홍길동(웹, {HONG})"
    h = _hub(rig)[-1]
    cx, d = h["context"], h["context"]["data"]
    assert (cx["chamber"], cx["source"], h["code"]) == ("ch1", "command", "PLC_BUTTON")
    assert (d["target"], d["name"], d["on"], d["by"], d["by_name"], d["origin"], d["who"], d["result"], d["hazard"]) == \
        ("vent_1_sw", "VENT_1_SW", True, HONG, "홍길동", "web", "홍길동(웹)", "done", "person")
    assert d["process"]["busy"] is False and d["process"]["chamber"] == "ch1"
    assert _syslog(rig)[-1].endswith(f"[PLC] PLC 버튼 · CH1 벤트 켬 · 홍길동(웹, {HONG})")
    assert _proc_lines(rig, "ch1") == [] and _proc_lines(rig, "ch2") == []          # 공정 중이 아니면 공정 로그에는 없음


def test_notebook_press_and_tabs_for_shared_and_loadlock(rig):
    assert _press(rig, "shutter_1_sw", True, src=HMI, by_name=None).ok
    assert _press(rig, "l_pin_up_sw", False, src=HMI, by_name=None).ok
    assert _press(rig, "buzzer_stop_sw", True, src=HMI, by_name=None).ok
    h = _hub(rig)
    assert [(x["context"]["chamber"], x["context"]["data"]["who"], x["context"]["data"]["origin"]) for x in h] == \
        [("ch2", "노트북", "local"), ("shared", "노트북", "local"), ("shared", "노트북", "local")]
    assert [e.detail for e in _evs(rig)] == ["PLC 버튼 · 건 셔터 G1 켬 · 노트북", "PLC 버튼 · 로드락 4핀 올림 끔 · 노트북",
                                             "PLC 버튼 · 부저 정지 켬 · 노트북"]
    assert [e.source for e in _evs(rig)] == ["command:ch2", "command", "command"]
    assert len(_syslog(rig)) == 3


def test_in_process_press_goes_to_that_chambers_process_log(rig):
    assert rig.command("RECIPE_LOAD", {"ch": 1, "rows": TEST_ROWS}, source=HMI).ok
    assert rig.command("PROCESS_START", {"ch": 1}, source=HMI).ok
    rig.loop.run(3)
    assert rig.core.proc["ch1"].busy
    assert _press(rig, "turbo_1_sw", False).ok
    assert _press(rig, "vent_2_sw", True, src=HMI, by_name=None).ok              # CH2 는 공정 중 아님 — CH1 공정 로그에 없음
    ev = _evs(rig)
    assert [(e.code, e.detail) for e in ev] == [
        ("PLC_MANUAL_IN_PROCESS", f"PLC 버튼 · CH1 터보 펌프 끔 · 홍길동(웹, {HONG}) (CH1 공정 중 · TEST MODE · 레시피 T1)"),
        ("PLC_BUTTON", "PLC 버튼 · CH2 벤트 켬 · 노트북")]
    p = _hub(rig)[0]["context"]["data"]["process"]
    assert (p["busy"], p["block"], p["recipe"], p["block_no"]) == (True, "TEST MODE", "T1", 1)
    assert _hub(rig)[0]["context"]["data"]["hazard"] == "equipment"
    pl = _proc_lines(rig, "ch1")
    assert len(pl) == 1 and pl[0].endswith(f"[CH1:PLC] PLC 버튼 · CH1 터보 펌프 끔 · 홍길동(웹, {HONG}) (CH1 공정 중 · TEST MODE · 레시피 T1)")
    assert _proc_lines(rig, "ch2") == []
    e1 = rig.core.proc["ch1"]
    e1.writer.flush(5)
    txt = Path(e1.txt._path).read_text(encoding="utf-8")
    assert "[CH1:PLC] PLC 버튼 · CH1 터보 펌프 끔 · 홍길동(웹" in txt                    # 공정 txt 파일에도
    assert len(_syslog(rig)) == 2                                                    # 장비 로그는 늘
    # 화면 확인창 'CH1 공정 중 — 블록 · 공정 이름' 의 이름 = 엔진 화면 상태 process_name(장비 기록과 같은 값), 공정 중일 때만
    assert rig.core.proc["ch1"].state_dict()["process_name"] == "T1" and rig.core.proc["ch2"].state_dict()["process_name"] is None


def test_rejections_are_one_event_each_with_reason(rig, monkeypatch):
    assert _press(rig, "nope", True).reason == "BAD_ARGS"
    monkeypatch.setattr(Ch12Core, "_plc_up", lambda self: False)
    assert _press(rig, "door_1_open_sw", True, src=HMI, by_name=None).reason == "LINK_DOWN"
    monkeypatch.undo()
    monkeypatch.setattr(Ch12Core, "writes_allowed", property(lambda self: False))
    assert _press(rig, "m_v_2_sw", False).reason == "BENCH_READ_ONLY"
    ev = _evs(rig)
    assert [(e.level, e.code) for e in ev] == [("warn", "BAD_ARGS"), ("warn", "LINK_DOWN"), ("warn", "BENCH_READ_ONLY")]
    assert ev[0].detail.startswith(f"PLC 버튼 · nope 켬 · 홍길동(웹, {HONG}) — 거부: target 'nope'")
    assert ev[1].detail == "PLC 버튼 · CH1 도어 열기 켬 · 노트북 — 거부: PLC 연결 없음"
    assert ev[2].detail.startswith(f"PLC 버튼 · CH2 메인 밸브 끔 · 홍길동(웹, {HONG}) — 거부: bench 모드")
    d = [x["context"]["data"] for x in _hub(rig)]
    assert [(x["result"], x["reason"]) for x in d] == [("rejected", "BAD_ARGS"), ("rejected", "LINK_DOWN"),
                                                        ("rejected", "BENCH_READ_ONLY")]
    assert [x["context"]["chamber"] for x in _hub(rig)] == ["shared", "ch1", "ch2"]
    assert not rig.sims["plc"].coil("DOOR_1_OPEN_SW")
    assert len(_syslog(rig)) == 3


def test_write_failure_is_failed_event(rig, monkeypatch):
    drv = type(rig.core.sys.shared.plc.proxy("WEB")._drv)

    def fail(self, name, on, *, on_done=None, **kw):
        on_done(SimpleNamespace(status="FAILED", reason="TIMEOUT", error="PLC 응답 없음"))
    monkeypatch.setattr(drv, "write_switch", fail)
    assert _press(rig, "r_v_1_sw", True).ok                                         # 명령은 받음 — 쓰기가 실패
    ev = _evs(rig)
    assert len(ev) == 1 and (ev[0].level, ev[0].code) == ("warn", "PLC_BUTTON_FAILED")
    assert ev[0].detail == f"PLC 버튼 · CH1 러핑 밸브 켬 · 홍길동(웹, {HONG}) — 실패: PLC 응답 없음"
    d = _hub(rig)[-1]["context"]["data"]
    assert (d["result"], d["reason"], d["hazard"]) == ("failed", "TIMEOUT", "equipment")


def test_one_event_per_press_and_name_fallback(rig):
    for on in (True, False, True):
        assert _press(rig, "ar_1_gas_sw", on, by_name=None).ok
    ev = _evs(rig)
    assert len(ev) == 3 and rig.core.plc_log.count == 3
    assert [e.detail for e in ev] == ["PLC 버튼 · CH1 Ar 가스 켬 · hong(웹, hong@vanam.co.kr)",
                                      "PLC 버튼 · CH1 Ar 가스 끔 · hong(웹, hong@vanam.co.kr)",
                                      "PLC 버튼 · CH1 Ar 가스 켬 · hong(웹, hong@vanam.co.kr)"]
    assert _hub(rig)[-1]["context"]["data"]["who"] == "hong(웹)"
    long = "가" * 100
    assert _press(rig, "ar_1_gas_sw", False, by_name="  " + long + "  ").ok
    assert _hub(rig)[-1]["context"]["data"]["by_name"] == long[:60]

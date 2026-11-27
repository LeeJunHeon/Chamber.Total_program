"""W — 윈도우 전원 설정 경고(사용자 결정 2026-11-27 · 챔버K W-25 A5 와 같은 이름·글).

절전 막기(SetThreadExecutionState)는 덮개 닫기·전원 버튼 절전을 막지 못한다. 시작할 때 전원 설정을 읽기만 하고(powrprof·
GetSystemPowerStatus), 덮개 닫기가 '아무것도 안 함'이 아니거나 AC 절전 시간이 '없음'이 아니거나 배터리로 돌고 있으면 시작 줄 한 줄 +
경고 사건 1번(selfwatch power_settings, 사유 POWER_SETTINGS). 설정은 바꾸지 않는다. 윈도우가 아니면 조용히. 가짜 읽기로 시험.
U(2026-11-27, 챔버K W-25 와 같게): 읽지 못하면 시작 줄 없이 상세 사건 1번(selfwatch power_settings_unread, 같은 이름·글 — 평소 보기에 안 나옴).
"""
from v2.ch12 import commands as C
from v2.ch12 import keep_awake as K
from v2.ch12 import web_schema as W

OK = K.PowerInfo(lid_ac=0, lid_dc=0, sleep_ac_s=0, on_ac=True)


def run(info=None, raises=None, reader=True, unread=None):
    lines, events = [], []

    def rd():
        if raises:
            raise raises
        return info
    msg = K.check_power(rd if reader else None, lines.append, lambda m, d: events.append((m, d)),
                        None if unread is None else unread.append)
    return msg, lines, events


def test_safe_settings_say_nothing():
    assert run(OK) == (None, [], [])


def test_lid_sleep_ac_sleep_and_battery_each_warn_once():
    msg, lines, events = run(K.PowerInfo(lid_ac=1, lid_dc=1, sleep_ac_s=1800, on_ac=False))
    assert lines == [f"[주의] {msg}"] and len(events) == 1 and events[0][0] == msg
    assert msg == ("장비 노트북 전원 설정: 덮개를 닫으면 절전됩니다(전원 연결 시). 전원 연결 시 30분 동안 쓰지 않으면 절전됩니다. "
                   "지금 배터리로 돌고 있습니다. 윈도우 전원 설정에서 덮개 닫기는 '아무것도 안 함', 절전은 '없음'으로 바꾸고 전원을 연결하세요")
    assert events[0][1] == {"lidAc": 1, "lidDc": 1, "sleepAcS": 1800, "onAc": False}
    assert "덮개를 닫으면 꺼집니다(배터리 사용 시)" in run(K.PowerInfo(lid_ac=0, lid_dc=3, sleep_ac_s=0, on_ac=True))[0]
    assert "최대 절전 모드로" in run(K.PowerInfo(lid_ac=2, lid_dc=0, sleep_ac_s=0, on_ac=True))[0]
    only_sleep = run(K.PowerInfo(lid_ac=0, lid_dc=0, sleep_ac_s=600, on_ac=True))[0]
    assert "10분 동안" in only_sleep and "덮개" not in only_sleep.split(":")[1].split(".")[0]
    only_batt = run(K.PowerInfo(lid_ac=0, lid_dc=0, sleep_ac_s=0, on_ac=False))[0]
    assert "지금 배터리로 돌고 있습니다" in only_batt


def test_not_windows_is_silent_unreadable_is_one_detail_event():
    un = []
    assert run(reader=False, unread=un) == (None, [], []) and un == []                  # 윈도우 아님 — 아무것도
    assert run(raises=OSError("PowerGetActiveScheme"), unread=un) == (None, [], [])     # 못 읽음 — 시작 줄·경고 없음
    assert un == ["윈도우 전원 설정을 읽지 못함(OSError)"]                             # 상세 사건 1번(챔버K 와 같은 글)
    assert run(raises=OSError("x")) == (None, [], [])                                    # 받는 곳이 없어도 조용히
    bad = run(raises=OSError("x"), unread=type("U", (), {"append": lambda self, m: 1 / 0})())   # 받는 곳 오류도 시작을 막지 않음
    assert bad == (None, [], [])
    assert run(K.PowerInfo(), unread=un) == (None, [], []) and len(un) == 1             # 값을 하나도 못 읽음 — 경고 없음
    import sys
    if not sys.platform.startswith("win"):
        assert K.windows_power() is None


def test_reason_and_event_kind_in_bundle():
    r = {x["code"]: x for x in W.commands_part()["reasons"]}
    assert r["POWER_SETTINGS"]["label"] == "장비 노트북 전원 설정 확인" and r["POWER_SETTINGS"]["category"] == "info"
    assert ("selfwatch", "power_settings", "장비 노트북 전원 설정", "main") in W.EVENT_KINDS
    assert ("selfwatch", "power_settings_unread", "전원 설정 읽기 실패", "detail") in W.EVENT_KINDS   # 평소 보기에 안 나옴
    assert "POWER_SETTINGS" in C.reason_codes()


def test_app_start_line_and_warn_event(tmp_path, monkeypatch):
    from v2.ch12 import app as A
    from v2.tests.test_app_web import FakeWeb, _in_process
    monkeypatch.setattr(A.App, "power_reader", lambda self: (lambda: K.PowerInfo(lid_ac=1, lid_dc=0, sleep_ac_s=0, on_ac=True)))
    app, lines = _in_process(tmp_path, FakeWeb(), UPLINK_IN_SIM=False, EQUIPMENT_ID="CH12")
    try:
        assert app.start() == A.EXIT_OK
        warn = [ln for ln in lines if ln.startswith("[주의] 장비 노트북 전원 설정")]
        assert len(warn) == 1 and "덮개를 닫으면 절전됩니다(전원 연결 시)" in warn[0]
        ev = [e for e in app.system.events if e.source == "selfwatch" and e.kind == "power_settings"]
        assert len(ev) == 1 and ev[0].level == "warn" and ev[0].code == "POWER_SETTINGS"
    finally:
        app.shutdown()


def test_app_unreadable_power_settings_is_one_detail_event_only(tmp_path, monkeypatch):
    from v2.ch12 import app as A
    from v2.tests.test_app_web import FakeWeb, _in_process

    def broken():
        raise OSError("PowerGetActiveScheme")
    monkeypatch.setattr(A.App, "power_reader", lambda self: broken)
    app, lines = _in_process(tmp_path, FakeWeb(), UPLINK_IN_SIM=False, EQUIPMENT_ID="CH12")
    try:
        assert app.start() == A.EXIT_OK
        assert not [ln for ln in lines if "전원 설정" in ln]                            # 시작 줄 없음
        ev = [e for e in app.system.events if e.source == "selfwatch" and e.kind.startswith("power_settings")]
        assert [(e.kind, e.detail, e.level, e.code) for e in ev] == [
            ("power_settings_unread", "윈도우 전원 설정을 읽지 못함(OSError)", "info", None)]
    finally:
        app.shutdown()

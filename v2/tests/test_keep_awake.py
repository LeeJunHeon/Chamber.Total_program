"""절전 막기(G — keep_awake.py) — 가짜 SetThreadExecutionState 로 KEEP_AWAKE 세 값·걸고 풀기·사건·윈도우가 아닐 때."""
import threading
import time

from v2.ch12 import keep_awake as KA


class Api:
    def __init__(self):
        self.calls = []

    def __call__(self, flags):
        self.calls.append((flags, threading.get_ident()))
        return 1


ON = KA.ES_CONTINUOUS | KA.ES_SYSTEM_REQUIRED


def _ka(mode, busy=lambda: False):
    api, ev = Api(), []
    k = KA.KeepAwake({"KEEP_AWAKE": mode}, busy, lambda kind, detail: ev.append((kind, detail)), api=api)
    return k, api, ev


def test_always_holds_from_start_and_releases_on_exit_same_thread():
    k, api, ev = _ka("always")
    k.start()
    for _ in range(100):
        if api.calls:
            break
        time.sleep(0.01)
    k.stop()
    flags = [f for f, _t in api.calls]
    assert flags == [ON, KA.ES_CONTINUOUS]                             # 걸고 → 종료 때 풀기(화면 꺼짐은 막지 않음)
    assert len({t for _f, t in api.calls}) == 1 and api.calls[0][1] != threading.get_ident()   # 살아 있는 한 스레드에서
    assert [x[0] for x in ev] == ["keep_awake_on", "keep_awake_off"]
    assert "프로그램이 켜져 있는 동안" in ev[0][1] and "프로그램 종료" in ev[1][1]


def test_run_follows_busy():
    busy = [False]
    k, api, ev = _ka("run", lambda: busy[0])
    k.tick()
    assert api.calls == [] and not k.held
    busy[0] = True
    k.tick()
    k.tick()
    assert [f for f, _ in api.calls] == [ON] and k.held
    busy[0] = False
    k.tick()
    assert [f for f, _ in api.calls] == [ON, KA.ES_CONTINUOUS] and not k.held
    assert [x for x, _ in ev] == ["keep_awake_on", "keep_awake_off"] and "운전 중" in ev[0][1]


def test_off_never_holds_and_default_is_always():
    k, api, ev = _ka("off", lambda: True)
    k.tick()
    assert api.calls == [] and ev == []
    assert KA.KeepAwake({}, lambda: False, api=Api()).want() is True             # 기본 always — 로봇 요청을 늘 받는다


def test_not_windows_does_nothing(monkeypatch):
    monkeypatch.setattr(KA.sys, "platform", "linux")
    assert KA.windows_api() is None
    k = KA.KeepAwake({"KEEP_AWAKE": "always"}, lambda: True, api=None)
    k.start()
    k.tick()
    k.stop()
    assert k._thread is None and not k.held


def test_param_and_event_kinds_in_bundle():
    from v2.ch12.params import by_key
    from v2.ch12 import web_schema as W
    p = by_key()["KEEP_AWAKE"]
    assert p.default == "always" and tuple(p.enum) == ("always", "run", "off")
    kinds = {(s, k): lvl for s, k, _n, lvl in W.EVENT_KINDS}
    assert kinds[("selfwatch", "keep_awake_on")] == "detail" and kinds[("selfwatch", "keep_awake_off")] == "detail"

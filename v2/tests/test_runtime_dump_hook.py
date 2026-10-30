"""런타임 덤프 부르기(v2/ch12/runtime_dump_hook.py) — v1 main.py 1170-1235·1500-1505 의 사유·로그 문구·1회 규칙, v2 상태 사전."""
from __future__ import annotations

import json
from types import SimpleNamespace

from v2.ch12 import runtime_dump_hook as H
from v2.ch12.outputs import runtime_dump as RD


class Sys:
    def __init__(self, tmp):
        self.cfg = {"LOG_ROOT_DIR": str(tmp / "logs"), "LOCAL_FALLBACK_ROOT": str(tmp / "fb")}
        self.said = []
        self.engines = {"proc.ch1": SimpleNamespace(state_dict=lambda: {"state": "IDLE", "run_id": None}),
                        "bad": SimpleNamespace(state=lambda: 1 / 0), "plain": object()}
        self.run_state = SimpleNamespace(snapshot=lambda: {"running": {}})
        self.events = []
        self.mode = "sim"

    def emit(self, source, kind, detail="", level="info", code=None, reason=None, event=None):
        self.said.append((source, kind, reason, detail, level))


def _snap(path):
    text = open(path, encoding="utf-8").read()
    return json.loads(text.split("=== SNAPSHOT(JSON) ===\n", 1)[1].split("\n\n=== THREAD TRACEBACK", 1)[0])


def test_manual_dump_saved_with_v2_state(tmp_path):
    s = Sys(tmp_path)
    p = H.dump_runtime(s, "manual_button", wait=True)
    assert p and p.startswith(str(tmp_path / "logs" / "RUNTIME_DUMP"))
    assert "_manual_button_pid" in p
    assert s.said == [("dump", "log", "WARN/DUMP", f"Runtime dump saved(manual_button): {p}", "warn")]
    snap = _snap(p)
    obj = snap["objects"]
    assert obj["runtime_state"] == {"running": "{}"}
    assert obj["engine:proc.ch1"] == {"state": "'IDLE'", "run_id": "None"}
    assert "ZeroDivisionError" in obj["engine:bad"]["error"]
    assert obj["engine:plain"]["type"] == "builtins.object"
    assert snap["asyncio"] == {"available": False, "reason": "loop is None"}


def test_skipped_when_in_progress_and_failed_logged(tmp_path, monkeypatch):
    s = Sys(tmp_path)
    RD._DUMP_IN_PROGRESS = True
    try:
        assert H.dump_runtime(s, "manual_button") is None
    finally:
        RD._DUMP_IN_PROGRESS = False
    assert s.said[-1] == ("dump", "log", "WARN/DUMP", "Runtime dump skipped(manual_button): runtime dump already in progress",
                          "warn")
    monkeypatch.setattr(RD, "request_dump", lambda *a, **k: (_ for _ in ()).throw(OSError("disk")))
    assert H.dump_runtime(s, "x") is None
    assert s.said[-1] == ("dump", "log", "ERROR/DUMP", "Runtime dump failed(x): OSError('disk')", "error")


def test_close_blocked_dumps_once(tmp_path):
    s = Sys(tmp_path)
    p = H.dump_close_blocked(s, wait=True)
    assert p and "_close_blocked_while_running_pid" in p
    assert H.dump_close_blocked(s, wait=True) is None
    assert len([x for x in s.said if "saved" in x[3]]) == 1


def test_real_rig_state(tmp_path):
    from v2.tests import proc_rig as R
    rig = R.make(tmp_path, chambers=("ch1",))
    try:
        st = H.collect_state(rig)
        assert "engine:proc.ch1" in st and st["meta"]["engines"] == ["proc.ch1"]
        assert st["devices"]["plc"] == "on" and "recorder" in st
        p = H.dump_runtime(rig, "manual_button", wait=True)
        assert p and str(tmp_path) in p
    finally:
        rig.clock.close()

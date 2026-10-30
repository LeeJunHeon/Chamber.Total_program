"""화면 빠진 기능의 장비 쪽(전수 목록 UI-098·099·104·118~127·080·086·087) — 포털·노트북 화면이 그리는 값.

- OES 스펙트럼(drivers/oes_spectrum.py): v1 graph_controller.update_oes_plot 거르기(비유한·x 100~1200 밖 버림, 2점 미만 생략, x 정렬)·
  OES_PLOT_MIN_INTERVAL_S 쓰로틀(시각은 거르기 전에)·512점 솎기(구간 최소·최대 — 꼭대기 유지).
- 조립·허브: 링크 이벤트 kind='spectrum' 은 링 버퍼·기록에 넣지 않고(system.TRANSIENT_KINDS) state.spectra 에 마지막 것만 —
  clear·그 챔버 run_start/run_end 에 비움(v1 graph.reset), 사건으로는 나가지 않음.
- THICKNESS_WINDOW: 결과 value(factor_window·materials_window) + 사건 thickness/window(req 그대로), bench 에서도, 저장 뒤 새 값.
- 히터 패널 글(heater_panel_text): v1 우선순위 문구·상태 줄·배지·소유자.
- 공정 실패 팝업: clear_on_close(v1 clear_status_to_idle).
- 로컬 서버 /api/v1/files: 시작 폴더 아래만·폴더+csv/xlsx·이름순·'..'·절대·링크 탈출 403·없는 폴더 404·응답 지연 504.
"""
import json
import math
import os
import time
import http.client
from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.common.commands import Source
from v2.ch12 import hub as H
from v2.ch12 import local_server as LS
from v2.ch12.drivers import oes_spectrum as OS
from v2.ch12.drivers.oes_link import OesEvent
from v2.ch12.heater_panel_text import panel_texts
from v2.ch12.sim.rig import Ch12FullRig, Ch12Rig


# ───────── OES 스펙트럼 규칙 ─────────
def test_clean_same_rules_as_v1_graph():
    assert OS.clean([], []) is None and OS.clean([500.0], [1.0]) is None                 # 2점 미만
    pts = OS.clean([300, "x", 99.9, 1200.0, float("nan"), 150, 1200.1, 400],
                   [1, 2, 3, 4, 5, float("inf"), 7, -8])
    assert pts == [(300.0, 1.0), (400.0, -8.0), (1200.0, 4.0)]                          # 정렬, y 는 자르지 않음
    assert OS.clean([300, 400, 500], [1, 2]) == [(300.0, 1.0), (400.0, 2.0)]            # 짧은 쪽까지
    assert OS.clean([50, 2000, 300], [1, 2, 3]) is None                                  # 남은 점 1개


def test_thin_keeps_peaks_and_cap():
    xs = [100 + i * 0.5 for i in range(2048)]
    ys = [100.0] * 2048
    ys[777] = 15000.0                                    # 좁은 방출선 하나
    ys[1500] = -5.0
    pts = OS.clean(xs, ys)
    out = OS.thin(pts)
    assert len(out) <= OS.MAX_POINTS and len(out) >= OS.MAX_POINTS // 2
    assert (xs[777], 15000.0) in out and (xs[1500], -5.0) in out
    assert [p[0] for p in out] == sorted(p[0] for p in out)
    small = pts[:300]
    assert OS.thin(small) == small                       # 상한 이하는 그대로
    d = OS.payload(out, "ch1", 1_790_000_000.123456, 2048)
    assert d["chamber"] == "ch1" and d["n"] == 2048 and len(d["x"]) == len(d["y"]) == len(out) and d["t"] == 1_790_000_000.123
    assert len(json.dumps(d)) < 12_000                   # 웹으로 1초마다 — 크기 상한(약 6 KB)


def test_throttle_v1_interval_and_time_set_before_filter():
    th = OS.SpectrumThrottle(0.5)
    xs, ys = [200.0, 300.0, 400.0], [1.0, 2.0, 3.0]
    assert th.offer(10.0, xs, ys, "ch2", 1.0)["x"] == [200.0, 300.0, 400.0]
    assert th.offer(10.4, xs, ys, "ch2", 1.0) is None and th.skipped == 1               # 0.5 s 안 → 건너뜀
    assert th.offer(10.5, [50.0, 60.0], [1.0, 2.0], "ch2", 1.0) is None                 # 통과했지만 점 없음
    assert th.offer(10.7, xs, ys, "ch2", 1.0) is None                                   # 시각은 10.5 로 바뀌어 있음(v1)
    assert th.offer(11.0, xs, ys, "ch2", 1.0) is not None
    th.reset()
    assert th.offer(11.1, xs, ys, "ch2", 1.0) is not None                               # 측정 시작 뒤 첫 표본은 바로
    assert OS.SpectrumThrottle(0.0).offer(0.0, xs, ys, "ch1", 0.0) is not None


# ───────── 조립·허브 ─────────
@pytest.fixture
def rig(tmp_path):
    r = Ch12Rig(VirtualClock(start_wall=1_790_000_000.0), tmp_path)
    H.attach(r, recent_keep=50)
    yield r
    r.outputs.stop()


def test_spectrum_goes_to_state_only_and_resets(rig):
    on = rig._ev_for("ch1.oes")
    n_events = len(rig.events)
    d = OS.payload(OS.clean([300.0, 400.0], [10.0, 20.0]), "ch1", 1.0, 2)
    on(OesEvent("spectrum", "", data=d))
    assert len(rig.events) == n_events                                    # 링 버퍼·기록에 넣지 않음
    assert not [e for e in rig.hub.recent if e["type"] == "event"]       # 사건으로 나가지 않음
    rig.hub.on_row({"ts": "t", "seq": 1})
    st = rig.hub.last_state["data"]
    assert st["spectra"]["ch1"]["x"] == [300.0, 400.0] and st["spectra"]["ch2"] is None
    on(OesEvent("spectrum", "", data={"chamber": "ch1", "clear": True}))   # 측정 시작(v1 clear_oes_plot)
    assert rig.hub.spectra["ch1"] is None
    on(OesEvent("spectrum", "", data=d))
    rig.emit("engine:ch1", "run_start", "P1", event={"run_id": "r1"})     # 공정 시작(v1 graph.reset)
    assert rig.hub.spectra["ch1"] is None
    on(OesEvent("spectrum", "", data=d))
    rig.emit("engine:ch1", "run_end", "P1", event={"run_id": "r1", "result": "done"})
    assert rig.hub.spectra["ch1"] is None
    on(OesEvent("status", "[OES] x"))                                     # 다른 OES 이벤트는 그대로
    assert len(rig.events) == n_events + 3


@pytest.fixture
def full(tmp_path):
    c = VirtualClock(start_wall=1_790_000_000.0)
    c.register()
    r = Ch12FullRig(c, tmp_path, cfg={"CAMERA_MIN_FREE_GB": 0.0, "CH2_SUPPORTS_THICKNESS": True}, robot_api=False)
    assert r.start()
    r.loop.run(2)
    yield r
    r.stop()
    c.close()


def test_thickness_window_value_and_event(full):
    res = full.hub.run_command("THICKNESS_WINDOW", {"ch": 2, "req": "q1"}, Source.HMI, cid="c9", by="local")
    assert res.ok, res
    v = res.value
    assert v["req"] == "q1" and v["factor"]["title"] == "CH2 Thickness Monitor — Factor 설정"
    assert set(v["factor"]["tooling"]) == {"up", "mid", "down"} and list(v["factor"]["choices"][0]) == ["(선택 안 함)", ""]
    assert v["materials"]["columns"] == ["이름", "기호", "density (g/cm3)", "Z-factor", "note"] and v["materials"]["rows"]
    ev = [e["data"] for e in full.hub.recent if e["type"] == "event" and e["data"]["context"].get("kind") == "window"]
    assert ev and ev[-1]["context"]["source"] == "thickness" and ev[-1]["context"]["chamber"] == "ch2"
    assert ev[-1]["context"]["data"]["req"] == "q1" and ev[-1]["context"]["cmdId"] == "c9"
    # 저장 → 다시 읽으면 새 값
    name = v["materials"]["rows"][0][0]
    tooling = {c: {g: "" for g in ("G1", "G2", "G3")} for c in ("up", "mid", "down")}
    tooling["mid"]["G2"] = "120"
    r2 = full.command("THICKNESS_FACTOR_SAVE", {"ch": 2, "tooling": tooling, "materials": {"G1": name, "G2": "", "G3": ""}},
                      source=Source.HMI)
    assert r2.ok, r2
    v2 = full.command("THICKNESS_WINDOW", {"ch": 2}, source=Source.HMI).value
    assert v2["factor"]["tooling"]["mid"]["G2"] == "120" and v2["factor"]["material"]["G1"] == name
    bad = full.command("THICKNESS_FACTOR_SAVE", {"ch": 2, "tooling": dict(tooling, up={"G1": "5", "G2": "", "G3": ""}),
                                                 "materials": {"G1": "", "G2": "", "G3": ""}}, source=Source.HMI)
    assert (bad.ok, bad.reason) == (False, "BAD_ARGS") and bad.detail.startswith("고칠 칸이 있어 저장하지 않았습니다:")
    from v2.ch12 import commands as C
    assert C.get("THICKNESS_WINDOW").bench_ok


def test_process_failed_popup_clears_on_close():
    from v2.ch12.process_engine import ProcessEngine
    got = []

    class Fake:
        def emit(self, kind, detail="", code=None, level="info", data=None, **kw):
            got.append((kind, detail, level, dict(data or {}, **kw)))
    ProcessEngine.popup(Fake(), "CH1 공정 실패", "x", "error", clear_on_close=True)
    ProcessEngine.popup(Fake(), "대기 필요", "y")
    assert got[0] == ("popup", "x", "error", {"title": "CH1 공정 실패", "clear_on_close": True})
    assert got[1] == ("popup", "y", "warn", {"title": "대기 필요"})


# ───────── 히터 패널 글 ─────────
NO_RAMP = (False, None, 0.0, None)


def test_heater_panel_texts_v1_priority():
    ok = {"ok": True, "itl": True, "tc_ok": True, "da_ok": True, "water_ok": True, "vac_ok": True, "stale": False}
    p = panel_texts(dict(ok, fault=True, ot=True, itl=False), NO_RAMP, {}, "manual")
    assert (p["status_text"], p["status_tone"], p["badge"], p["owner_text"]) == ("과온 트립", "red", "FAULT", "수동")
    assert panel_texts(dict(ok, fault=True, wd_err=True), NO_RAMP, {}, None)["status_text"] == "워치독 트립"
    assert panel_texts(dict(ok, fault=True), NO_RAMP, {}, None)["status_text"] == "이상 발생"
    p = panel_texts(dict(ok, itl=False, water_ok=False), NO_RAMP, {}, "process")
    assert (p["status_text"], p["status_tone"], p["badge"], p["owner_text"]) == ("인터락 — 냉각수 꺼짐", "orange", "ITL", "공정")
    p = panel_texts(dict(ok, itl=False, stale=True, age_s=12.4), NO_RAMP, {}, None)
    assert (p["status_text"], p["status_tone"], p["owner_text"]) == ("PLC 응답 없음 · 12초 전 값", "red", "—")
    assert panel_texts(dict(ok, stale=True, age_s=None), NO_RAMP, {}, None)["status_text"] == "PLC 응답 없음"
    p = panel_texts(dict(ok, run=True, pv=101.0, cur_sv=100.0, sv=300.0), (True, 300.0, 5.0, 40.0), {}, "manual")
    assert (p["status_text"], p["state_line"], p["sv_show"], p["dev"]) == ("운전 중 · 램프", "램프 5°C/min → 300.0°C · 약 40분", 100.0, 1.0)
    p = panel_texts(dict(ok, run=True), NO_RAMP, {"holding": True, "value": 900}, None)
    assert (p["status_text"], p["state_line"], p["badge"]) == ("운전 중 · 유지", "유지 · DAC 상한 900 고정", "RUN")
    assert panel_texts(dict(ok, run=True), NO_RAMP, {}, None)["state_line"] == "운전 중"
    p = panel_texts(dict(ok, run=False, sv=50.0), NO_RAMP, {}, None)
    assert (p["status_text"], p["status_tone"], p["state_line"], p["badge"], p["sv_show"]) == ("정지", "gray", "정지", "STOP", 50.0)


def test_heater_web_state_has_panel(full):
    he = full.hub.last_state["data"]["heater"]
    assert he["panel"]["status_text"] in ("정지", "운전 중") and he["panel"]["badge"] in ("STOP", "ITL", "RUN")


# ───────── 로컬 서버 파일 고르기 ─────────
@pytest.fixture
def files_env(tmp_path):
    root = tmp_path / "share"
    (root / "B폴더").mkdir(parents=True)
    (root / "a").mkdir()
    (root / "a" / "deep.csv").write_text("x", encoding="utf-8")
    (root / "r1.csv").write_text("#,Process_name\n", encoding="utf-8")
    (root / "R2.xlsx").write_bytes(b"pk")
    (root / "note.txt").write_text("x", encoding="utf-8")
    out = tmp_path / "secret"
    out.mkdir()
    (out / "s.csv").write_text("x", encoding="utf-8")
    try:
        os.symlink(out, root / "link")
    except OSError:
        pass
    r = Ch12Rig(VirtualClock(start_wall=1_790_000_000.0), tmp_path / "rig")
    H.attach(r)
    r.cfg["PROCESS_LIST_START_DIR"] = str(root)
    sock = LS.bind_socket(0)
    srv = LS.LocalServer(0, None, sock=sock)
    srv.attach(r)
    srv.start()
    yield r, srv, root
    srv.stop()
    r.hub.close()
    r.outputs.stop()


def _get(srv, path):
    c = http.client.HTTPConnection("127.0.0.1", srv.port, timeout=5)
    try:
        c.request("GET", path)
        resp = c.getresponse()
        return resp.status, json.loads(resp.read())
    finally:
        c.close()


def test_files_list_under_start_dir(files_env):
    r, srv, root = files_env
    code, b = _get(srv, "/api/v1/files")
    assert code == 200 and b["dir"] == "" and b["parent"] is None and b["root"] == str(root)
    names = [(e["name"], e["kind"]) for e in b["entries"] if e["name"] != "link"]
    assert names == [("a", "dir"), ("B폴더", "dir"), ("r1.csv", "file"), ("R2.xlsx", "file")]   # 폴더 먼저·이름순·csv/xlsx 만
    f = next(e for e in b["entries"] if e["name"] == "r1.csv")
    assert f["path"] == "r1.csv" and Path(f["full"]) == (root / "r1.csv").resolve() and f["size"] == len("#,Process_name\n")
    code, b = _get(srv, "/api/v1/files?dir=a")
    assert code == 200 and b["dir"] == "a" and b["parent"] == "" and [e["name"] for e in b["entries"]] == ["deep.csv"]
    assert _get(srv, "/api/v1/files?dir=%EB%AA%A8%EB%A6%84")[0] == 404                         # 없는 폴더
    for bad in ("..", "a/../..", "/etc", "C:/Windows"):
        assert _get(srv, f"/api/v1/files?dir={bad}")[0] == 403, bad
    if (root / "link").exists():
        assert _get(srv, "/api/v1/files?dir=link")[0] == 403                                    # 링크로 밖으로
    r.cfg["PROCESS_LIST_START_DIR"] = str(root / "없음")
    assert _get(srv, "/api/v1/files") == (404, {"error": "no_root", "detail": f"시작 폴더 없음: {root / '없음'}"})


def test_files_timeout_504(files_env, monkeypatch):
    r, srv, root = files_env
    real = os.scandir

    def slow(p):
        time.sleep(0.5)
        return real(p)
    monkeypatch.setattr(LS.os, "scandir", slow)
    code, b = srv.files("", timeout_s=0.1)
    assert code == 504 and b["error"] == "timeout" and "응답지연" in b["detail"]
    time.sleep(0.6)


def test_spectrum_payload_json_finite():
    pts = OS.clean([300.0, 301.0, 302.0], [1.0, 2.0, 3.0])
    d = OS.payload(pts, "ch2", 5.0, 3)
    assert all(math.isfinite(v) for v in d["x"] + d["y"])


# ───────── OES 링크(실제 워커 --fake) → spectrum 이벤트 ─────────
def test_oes_link_spectrum_events_throttled_thinned_cleared(tmp_path, monkeypatch):
    import threading
    from v2.common.clock import RealClock
    from v2.ch12.drivers import oes_link as ol
    monkeypatch.delenv("OES_LOCAL_BASE", raising=False)
    monkeypatch.delenv("OES_DAEMON", raising=False)
    monkeypatch.setenv("OES_FAKE", "")
    monkeypatch.setenv("OES_FAKE_LOG", str(tmp_path / "calls.jsonl"))
    cfg = {"OES_LOCAL_DIR": str(tmp_path / "oes"), "OES_WORKER_FAKE": True, "OES_WORKER_HOME": str(tmp_path / "home"),
           "CHAMBER_OES_INIT_TIMEOUT_S": 20.0, "OES_PLOT_MIN_INTERVAL_S": 0.5}
    events, raw = [], []
    link = ol.OesLink(RealClock(), None, cfg, True, on_event=events.append, chamber="ch2",
                      on_data=lambda x, y: raw.append(len(x)))
    try:
        done = threading.Event()
        link.measure(3.5, 50, on_done=lambda res: done.set())
        assert done.wait(60)
        sp = [e for e in events if e.kind == "spectrum"]
        assert sp and sp[0].data == {"chamber": "ch2", "clear": True}                # 측정 시작 = 지움
        got = [e.data for e in sp[1:]]
        assert got and raw and raw[0] == 1024                                           # on_data 는 모두(솎지 않음)
        for d in got:
            assert d["chamber"] == "ch2" and d["n"] == 1024 and len(d["x"]) <= OS.MAX_POINTS
            assert all(100.0 <= v <= 1200.0 for v in d["x"]) and d["x"] == sorted(d["x"])
        ts = [d["t"] for d in got]
        assert all(b - a >= 0.5 - 1e-3 for a, b in zip(ts, ts[1:]))                    # 0.5 s 쓰로틀
        assert len(got) <= len(raw)
    finally:
        link.close()

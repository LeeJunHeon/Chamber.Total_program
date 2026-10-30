"""전수 목록 워커 앱 영역(inv_app.py)·빠진 항목 C(inv_gap_c.py) 중 다른 시험이 덮지 않던 것 — 실제 실행 시험.

1) 독립 도구(apps/rf_service rf_worker·apps/log_sync_service log_sync — v1 그대로 쓰는 도구): v2 가 만든 사진·기록·설정 파일을
   v1 도구가 실제로 받아들이는지. v2/tools/v1golden_independent.py 가 v1 도구를 별도 프로세스로 v2 출력에 돌린 결과
   (golden/independent_tools.json)를 읽고, 지금 v2 가 같은 출력·같은 settings.json(export_worker_settings — 결정 D13)을
   만드는지 비교한다(v1 import 없음).
2) OES 워커(v2/ch12/workers — 별도 프로세스, --fake 가짜 DLL)를 명령줄로 직접 실행: JSON 필드·종료 코드·로그 파일·일련번호 확인·
   전역 정지 플래그·첫 프레임 실패·연속 읽기 실패 리셋.
3) OES 링크 설정 규칙(USB 매핑·저장 폴더 권한 폴백·콘솔 옵션·one-shot 중복·cleanup)과 공정 엔진의 OES·RGA 결과 처리.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from v2.tools import export_worker_settings as EXP
from v2.tools import independent_scen as S

ROOT = Path(__file__).resolve().parents[2]
GOLD = json.loads((Path(__file__).parent / "golden" / "independent_tools.json").read_text(encoding="utf-8"))


# ───────────────────────── 1) 독립 도구 ─────────────────────────
@pytest.fixture(scope="module")
def made(tmp_path_factory):
    pytest.importorskip("cv2")
    pytest.importorskip("numpy")
    root = tmp_path_factory.mktemp("ind") / "기록 뿌리 & 시험"
    return root, S.make_all(root)


def test_v2_outputs_are_what_v1_tools_consumed(made):
    """v2 가 지금 만드는 사진·기록 파일 목록 = 골든 때 v1 도구가 받아 처리한 목록(경로·파일명 규칙이 바뀌면 깨진다)."""
    _root, m = made
    assert m["camera"] == GOLD["v2"]["camera"]
    assert m["log_root"] == GOLD["v2"]["log_root"]
    assert m["oes"] == GOLD["v2"]["oes"]


def test_camera_photos_match_rf_worker_rules(made):
    """APP-060·061: 세션 폴더 {CH1|CH2|CLEANING|ALL}/YYYYmmdd_HHMMSS(정규식 ^\\d{8}_\\d{6}$), 파일 HHMMSS_NNNN.jpg,
    세로 1920 x 가로 1080(90° 시계 방향 회전) — v1 rf_worker 가 판독(status done, frames = 장 수)했고 시각 열은 세션 날짜 +
    파일 시각(자정 넘으면 +1일). GAPC-006: 판독 알고리즘은 v1 도구 그대로(모델 cnn.onnx 로 실제 판독됨)."""
    cv2 = pytest.importorskip("cv2")
    np = pytest.importorskip("numpy")
    root, m = made
    cam = Path(m["cfg"]["CAMERA_LOG_ROOT"])
    for rel in m["camera"]:
        mode, sess, name = rel.split("/")
        assert mode in ("CH1", "CH2", "CLEANING", "ALL") and re.fullmatch(r"\d{8}_\d{6}", sess)
        assert re.fullmatch(r"\d{6}_\d{4}\.jpg", name)
        img = cv2.imdecode(np.frombuffer((cam / rel).read_bytes(), np.uint8), cv2.IMREAD_COLOR)
        assert img.shape[:2] == (1920, 1080)
    rf = GOLD["rf_worker"]
    assert rf["exit"] == 0 and rf["settings_line"] and not rf["problems"]
    by_sess = {}
    for rel in m["camera"]:
        mode, sess, name = rel.split("/")
        by_sess.setdefault(f"{mode}/{sess}", []).append(name)
    assert rf["manifest"] == {k: {"status": "done", "frames": len(v)} for k, v in by_sess.items()}
    for key, names in by_sess.items():
        mode, sess = key.split("/")
        c = rf["csv"][f"{mode}/{mode}_{sess}.csv"]
        assert c["header"][0] == "timestamp" and len(c["header"]) == 11
        base = datetime.strptime(sess, "%Y%m%d_%H%M%S")
        want = []
        for n in sorted(names):
            t = base.replace(hour=int(n[:2]), minute=int(n[2:4]), second=int(n[4:6]))
            want.append((t + timedelta(days=1) if t < base else t).strftime("%Y-%m-%d %H:%M:%S"))
        assert c["ts"] == want
    assert rf["nas"] == sorted(f"{k.split('/')[0]}/{k.split('/')[0]}_{k.split('/')[1]}.csv" for k in by_sess)


def test_log_sync_copies_v2_records(made):
    """APP-066·067: v1 log_sync 가 LOG_SYNC_PAIRS 의 src(v2 LOG_ROOT_DIR·OES_LOCAL_DIR)를 dst 로 복사 — v2 기록이 모두 NAS 에
    반영되고 제어 파일(.stop·.flag)은 복사되지 않는다. GAPC-009: 실패 없음(state consecutive_fails 0)."""
    _root, m = made
    ls = GOLD["log_sync"]
    assert ls["exit"] == 0 and not ls["problems"] and ls["state"]["consecutive_fails"] == 0
    logs = set(ls["copied"]["NAS/Sputter/Logs/CH1&2"])
    assert set(m["log_root"]) <= logs
    oes = ls["copied"]["NAS/Sputter/OES"]
    assert oes == [x for x in m["oes"] if not x.endswith((".stop", ".flag"))] and len(oes) < len(m["oes"])


def test_export_worker_settings_matches_tools(made, tmp_path):
    """D13·APP-064: v2 설정 → v1 형식 settings.json({"common": {...}}) — 키 = 두 도구의 SETTINGS_KEYS, 값은 v1 형식(목록은 목록).
    골든 때 두 도구가 이 파일을 문제 없이 읽었다(rf_worker settings 줄·problems 없음)."""
    root, m = made
    want_keys = GOLD["v1_settings_keys"]["log_sync"] + GOLD["v1_settings_keys"]["rf_worker"]
    assert list(EXP.ALL_KEYS) == want_keys
    obj = EXP.build(EXP.load_values(None) | {k: m["cfg"][k] for k in EXP.ALL_KEYS if k in m["cfg"]})
    assert S.placeholder(obj, root) == GOLD["settings_json"]
    assert isinstance(obj["common"]["LOG_SYNC_PAIRS"], list) and isinstance(obj["common"]["RF_READER_RETENTION_DAYS"], int)
    out = tmp_path / "config" / "settings.json"
    assert EXP.main(["--out", str(out)]) == 0
    d = json.loads(out.read_text(encoding="utf-8"))
    assert list(d) == ["common"] and d["common"]["LOG_SYNC_PAIRS"][0][0] == r"C:\VanaM_Logs\CH1&2"
    assert EXP.main(["--out", str(out)]) == 1                       # 이미 있으면 --force 없이는 멈춤
    with pytest.raises(ValueError, match="v1 형식과 맞지 않는"):
        EXP.build({"RF_READER_RETENTION_DAYS": "14"})


def test_v2_default_paths_fit_tools():
    """APP-063·066·067(D14·D29): v2 기본 LOG_ROOT_DIR·OES_LOCAL_DIR 가 log_sync 기본 쌍의 src, 판독 워커의 _csv·_state 는 사진
    모드 폴더와 겹치지 않고, 1초 기록(LOCAL_DATA_DIR — SQLite)은 log_sync 가 복사하는 폴더 밖."""
    from v2.ch12.params import table
    d = {p.key: p.default for p in table().params}
    pairs = json.loads(d["LOG_SYNC_PAIRS"])
    assert [p[0] for p in pairs] == [d["LOG_ROOT_DIR"], d["OES_LOCAL_DIR"]]
    assert d["CAMERA_LOG_ROOT"] == r"C:\VanaM_Logs\Camera_Logs"
    for k in ("RF_READER_CSV_DIR", "RF_READER_STATE_DIR"):
        name = d[k].rsplit("\\", 1)[1]
        assert d[k].startswith(d["CAMERA_LOG_ROOT"] + "\\") and name.startswith("_") and name not in ("CH1", "CH2", "CLEANING", "ALL")
    srcs = [p[0].lower() for p in pairs]
    assert not any(d["LOCAL_DATA_DIR"].lower().startswith(s) for s in srcs)


# ───────────────────────── 2) OES 워커(별도 프로세스) ─────────────────────────
def _worker(tmp: Path, *args: str, fake: str = "", timeout: float = 90.0):
    env = dict(os.environ, OES_FAKE=fake, PYTHONPATH=str(ROOT), PYTHONIOENCODING="utf-8")
    env.pop("OES_LOCAL_BASE", None)
    p = subprocess.run([sys.executable, "-m", "v2.ch12.workers.oes_worker", *args, "--home", str(tmp / "home"),
                        "--fake", "--hb_s", "0"], cwd=str(tmp), env=env, capture_output=True, text=True,
                       encoding="utf-8", timeout=timeout)
    lines = [json.loads(x) for x in p.stdout.splitlines() if x.strip()]
    return p.returncode, lines


def _kind(lines, k):
    return [x for x in lines if x.get("kind") == k]


def test_oes_worker_init_json_logs_and_serial_check(tmp_path):
    """APP-020·021·022·023·043·030·042, GAPC-013: init 응답 JSON 필드, status 문구(뮤텍스·init begin·START argv),
    run.log, oes_config.json expected_serial 불일치면 ok=False '분광기 일련번호 불일치 …'."""
    (tmp_path / "home").mkdir()
    rc, lines = _worker(tmp_path, "--cmd", "init", "--ch", "1", "--usb", "0")
    assert rc == 0
    (ini,) = _kind(lines, "init")
    assert list(ini)[:10] == ["kind", "ok", "ch", "usb", "resolved_usb", "pixels", "model", "dll_resolved", "dll_exists", "serial"]
    assert ini["ok"] is True and ini["pixels"] == 1056 and ini["model"] == "PDA"
    st = [x["message"] for x in _kind(lines, "status")]
    assert st[0].startswith("[worker] START argv=") and " frozen=False base=" in st[0]
    assert r"[worker] init: acquiring mutex name=Local\VanaM_OES_USB0 timeout_ms=5000" in st
    assert any(s.startswith("[worker] init begin ch=1 usb=0 ") for s in st)
    assert any(s.startswith("[init] 분광기 확인 USB0: ") and "확인=꺼짐" in s for s in st)
    runlog = list((tmp_path / "home" / "log").glob("*.run.log"))
    assert runlog and re.fullmatch(r"\d{8}\.run\.log", runlog[0].name)
    assert re.search(r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d \[pid=\d+\] ", runlog[0].read_text(encoding="utf-8"), re.M)
    (tmp_path / "home" / "oes_config.json").write_text(json.dumps({"expected_serial": {"1": "OTHER"}}), encoding="utf-8")
    rc, lines = _worker(tmp_path, "--cmd", "init", "--ch", "1", "--usb", "0")
    (ini,) = _kind(lines, "init")
    assert ini["ok"] is False and ini["error"] == "분광기 일련번호 불일치: CH1 기대=OTHER, 실제=FAKE0"
    assert ini["scan"]["detected_count"] == 2 and ini["resolved_usb"] == -1


def test_oes_worker_measure_json_csv_and_flags(tmp_path):
    """APP-037·038·039·040·042: started/finished JSON 필드, CSV(Time+파장, 'HH:MM:SS'+세기), 종료 코드 0,
    전역 정지 플래그가 미리 있으면 측정 없이 ok/stopped/pre_stop."""
    out = tmp_path / "oes" / "CH1" / "OES_Data_20261030_091520.csv"
    out.parent.mkdir(parents=True)
    rc, lines = _worker(tmp_path, "--cmd", "measure", "--ch", "1", "--usb", "0", "--duration", "2", "--integration_ms", "50",
                        "--out_csv", str(out))
    assert rc == 0
    (stt,) = _kind(lines, "started")
    assert [k for k in stt if k not in ("v", "id")] == ["kind", "ok", "ch", "usb", "resolved_usb", "out_csv", "cols", "model",
                                                         "sample_interval_s", "avg_count", "integration_ms"]
    assert stt["cols"] == 1024 and stt["avg_count"] == 3 and stt["sample_interval_s"] == 1.0
    (fin,) = _kind(lines, "finished")
    assert [k for k in fin if k not in ("v", "id")] == ["kind", "ok", "stopped", "stop_reason", "ch", "usb", "out_csv", "nas_ok",
                                                         "nas_csv", "nas_error", "local_deleted", "rows", "elapsed_s",
                                                         "link_pause_count", "link_pause_s", "reinit_count", "reset_count"]
    assert fin["ok"] and fin["nas_ok"] is False and fin["local_deleted"] is False and fin["rows"] >= 2
    rows = out.read_text(encoding="utf-8").splitlines()
    assert rows[0].startswith("Time,") and len(rows) == fin["rows"] + 1
    assert all(re.fullmatch(r"\d\d:\d\d:\d\d", r.split(",")[0]) for r in rows[1:])
    (tmp_path / "home" / "stop").mkdir(parents=True, exist_ok=True)
    (tmp_path / "home" / "stop" / ".stop_usb0.flag").write_text("stop\n", encoding="utf-8")
    rc, lines = _worker(tmp_path, "--cmd", "measure", "--ch", "1", "--usb", "0", "--duration", "2", "--integration_ms", "50",
                        "--out_csv", str(out.with_name("b.csv")))
    (fin,) = _kind(lines, "finished")
    assert rc == 0 and fin["ok"] and fin["stopped"] and fin["stop_reason"] == "pre_stop" and fin["rows"] == 0


def test_oes_worker_first_frame_and_read_recovery(tmp_path):
    """APP-033: 첫 프레임 20회(0.2 s) 실패면 'first frame failed' — 종료 코드 10. APP-036: 연속 읽기 실패 3번째에 포트 리셋
    (finished reset_count) 뒤 측정 계속."""
    out = tmp_path / "o"
    out.mkdir()
    rc, lines = _worker(tmp_path, "--cmd", "measure", "--ch", "1", "--usb", "0", "--duration", "2", "--integration_ms", "50",
                        "--out_csv", str(out / "a.csv"), fake="fail_read_after=0")
    (fin,) = _kind(lines, "finished")
    assert rc == 10 and fin["ok"] is False and fin["error"].startswith("RuntimeError: first frame failed")
    assert 3.0 <= fin["elapsed_s"] < 8.0
    rc, lines = _worker(tmp_path, "--cmd", "measure", "--ch", "1", "--usb", "0", "--duration", "6", "--integration_ms", "50",
                        "--out_csv", str(out / "b.csv"), fake="fail_read_after=9")
    (fin,) = _kind(lines, "finished")
    assert rc == 0 and fin["ok"] and fin["reset_count"] >= 1 and fin["rows"] >= 2


def test_oes_async_default_and_channel_folder(tmp_path):
    """APP-047: 워커 OESAsync 기본 저장 폴더 C:\\Users\\vanam\\Desktop\\OES, 이름이 CH1/CH2 면 부모\\CH{n}, 아니면 \\CH{n} — 만든다."""
    import inspect
    pytest.importorskip("numpy")
    from v2.ch12.workers.oes_device import OESAsync
    assert inspect.signature(OESAsync).parameters["save_directory"].default == r"C:\Users\vanam\Desktop\OES"
    a = OESAsync(save_directory=str(tmp_path / "x" / "CH1"), chamber=2)
    b = OESAsync(save_directory=str(tmp_path / "y"), chamber=1)
    assert (tmp_path / "x" / "CH2").is_dir() and (tmp_path / "y" / "CH1").is_dir()
    assert a._save_dir == tmp_path / "x" / "CH2" and b._save_dir == tmp_path / "y" / "CH1"


# ───────────────────────── 3) OES 링크 규칙·공정 엔진 결과 처리 ─────────────────────────
def test_oes_link_usb_folder_and_console_options(tmp_path, monkeypatch):
    """APP-004: CH1 USB 0·CH2 USB 1(CHAMBER_OES_USB_INDEX_CHn). APP-003: OES_LOCAL_BASE → OES_LOCAL_DIR \\CH{n}, 권한 오류면
    %LOCALAPPDATA%\\VanaM\\OES\\CH{n}. APP-002: 콘솔 옵션 기본 0, OES_WORKER_HIDE_CONSOLE·SHOW_CONSOLE(윈도우)."""
    import subprocess as sp
    from v2.common.clock import RealClock
    from v2.ch12.drivers import oes_link as ol
    monkeypatch.delenv("OES_LOCAL_BASE", raising=False)
    c = RealClock()
    l1 = ol.OesLink(c, None, {"OES_LOCAL_DIR": str(tmp_path / "oes")}, True, chamber="ch1")
    l2 = ol.OesLink(c, None, {"OES_LOCAL_DIR": str(tmp_path / "oes"), "CHAMBER_OES_USB_INDEX_CH2": 3}, True, chamber="ch2")
    assert (l1.usb, l2.usb, ol.OesLink(c, None, {}, True, chamber="ch2").usb) == (0, 3, 1)
    assert l1.local_dir() == tmp_path / "oes" / "CH1" and l1.local_dir().is_dir()
    monkeypatch.setenv("OES_LOCAL_BASE", str(tmp_path / "env"))
    assert ol.OesLink(c, None, {"OES_LOCAL_DIR": "x"}, True, chamber="ch2").local_dir() == tmp_path / "env" / "CH2"
    monkeypatch.delenv("OES_LOCAL_BASE")
    deny = tmp_path / "deny" / "CH1"
    orig = Path.mkdir

    def mk(self, *a, **kw):
        if self == deny:
            raise PermissionError("권한 없음")
        return orig(self, *a, **kw)
    monkeypatch.setattr(Path, "mkdir", mk)
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "lad"))
    assert ol.OesLink(c, None, {"OES_LOCAL_DIR": str(tmp_path / "deny")}, True, chamber="ch1").local_dir() == \
        tmp_path / "lad" / "VanaM" / "OES" / "CH1"
    monkeypatch.setattr(sp, "CREATE_NO_WINDOW", 0x08000000, raising=False)
    monkeypatch.setattr(sp, "CREATE_NEW_CONSOLE", 0x10, raising=False)
    mods = {}
    for k in ({}, {"OES_WORKER_HIDE_CONSOLE": True}, {"OES_WORKER_SHOW_CONSOLE": True}):
        link = ol.OesLink(c, None, dict(k), True, chamber="ch1")
        saved = ol.os.name
        ol.os.name = "nt"
        try:
            mods[tuple(k)] = link._creationflags()
        finally:
            ol.os.name = saved
    assert mods == {(): 0, ("OES_WORKER_HIDE_CONSOLE",): 0x08000000, ("OES_WORKER_SHOW_CONSOLE",): 0x10}


def test_oes_link_oneshot_busy_and_cleanup(tmp_path, monkeypatch):
    """APP-012: one-shot 워커가 진짜 측정 중이면 새 요청은 무시('[OES] 이미 측정 중 → 요청 무시', BUSY). APP-017: cleanup —
    OES_CLEANUP_DETACH 면 플래그만(기다리지 않음), 아니면 끝날 때까지(OES_STOP_WAIT_S)."""
    from v2.common.clock import RealClock
    from v2.ch12.drivers import oes_link as ol
    monkeypatch.delenv("OES_LOCAL_BASE", raising=False)
    monkeypatch.setenv("OES_FAKE", "")
    ev = []
    cfg = {"OES_LOCAL_DIR": str(tmp_path / "oes"), "OES_WORKER_FAKE": True, "OES_WORKER_HOME": str(tmp_path / "home"),
           "OES_DAEMON": False, "OES_CLEANUP_DETACH": True}
    link = ol.OesLink(RealClock(), None, cfg, True, on_event=ev.append, chamber="ch1")
    try:
        done1, got2, cl = threading.Event(), [], threading.Event()
        link.measure(30.0, 50, on_done=lambda r: done1.set())
        end = time.monotonic() + 20
        while not (link.is_running and link._proc is not None and link._proc.alive()) and time.monotonic() < end:
            time.sleep(0.05)
        link.measure(2.0, 50, on_done=got2.append)
        assert got2 and got2[0].reason == "BUSY" and not got2[0].emitted
        assert "[OES] 이미 측정 중 → 요청 무시" in [e.detail for e in ev if e.kind == "status"]
        while link.status()["rows"] < 1 and time.monotonic() < end:     # 워커가 시작 때 로컬 플래그를 지우므로 측정이 돈 뒤에
            time.sleep(0.05)
        t0 = time.monotonic()
        link.cleanup(on_done=cl.set)
        assert cl.wait(1.0) and time.monotonic() - t0 < 1.0              # detach — 플래그만
        assert done1.wait(20)
    finally:
        link.close()
    link2 = ol.OesLink(RealClock(), None, {"OES_STOP_WAIT_S": 7.0, "OES_LOCAL_DIR": str(tmp_path / "oes2")}, True, chamber="ch1")
    seen = []
    link2._stop_impl = lambda wait, timeout_s: seen.append((wait, timeout_s))
    ok = threading.Event()
    link2.cleanup(on_done=ok.set)
    assert ok.wait(5) and seen == [(True, 7.0)]
    link2.close()


class _Runner:
    def __init__(self):
        self.calls = []

    def rga_finished(self):
        self.calls.append(("rga_finished",))

    def oes_failed(self, src, e, meta=None):
        self.calls.append(("oes_failed", src, str(e), meta))


def _actions(**kw):
    from v2.ch12.process_actions import ProcessActions as PA
    logs, chats = [], []
    me = SimpleNamespace(n=1, runner=_Runner(), oes_active=False, oes_initialized=False,
                         log=lambda src, msg: logs.append((src, msg)),
                         e=SimpleNamespace(chat=chats.append, cget=lambda k, d=None: d), **kw)
    me._rga_done = lambda: PA._rga_done(me)
    return PA, me, logs, chats


def test_engine_rga_result_handling():
    """APP-054: RGA status·결과 → 'RGA{n}' 로그, 실패면 '측정 실패: .. → 다음 단계' + stderr/stdout 끝 800자 + 챗, 어느 경우든
    rga_finished(공정이 멈추지 않게). 링크 없음·예외도 다음 단계."""
    class Rga:
        def __init__(self, res):
            self.res = res

        def scan(self, timeout_s, on_done):
            self.timeout_s = timeout_s
            on_done(self.res)
    bad = SimpleNamespace(ok=False, message="RGA worker stdout JSON parse failed", fail={"stderr": "E" * 900, "stdout": ""})
    PA, me, logs, chats = _actions(rga=Rga(bad))
    PA._a_rga_scan(me, SimpleNamespace())
    assert me.rga.timeout_s == 60.0
    assert logs[0] == ("RGA1", "측정 실패: RGA worker stdout JSON parse failed → 다음 단계") and logs[1] == ("RGA1", "stderr: " + "E" * 800)
    assert chats == ["[RGA1] 측정 실패: RGA worker stdout JSON parse failed → 건너뜀"] and me.runner.calls == [("rga_finished",)]
    PA, me, logs, chats = _actions(rga=Rga(SimpleNamespace(ok=True, message="", fail=None)))
    PA._a_rga_scan(me, SimpleNamespace())
    assert me.runner.calls == [("rga_finished",)] and not chats
    PA, me, logs, chats = _actions(rga=None)
    PA._a_rga_scan(me, SimpleNamespace())
    assert me.runner.calls == [("rga_finished",)] and "RGA 어댑터 없음" in logs[0][1]


def test_engine_oes_result_handling():
    """APP-005·019: OES 스텝 — init 안 됐으면 CHAMBER_OES_INIT_TIMEOUT_S 로 init(실패면 'OES 초기화 실패: ..' → oes_failed),
    성공이면 '{msg} (csv=..)' 로그, 실패(emitted)면 init 캐시 무효화 + oes_failed → 종료 절차."""
    class Oes:
        def __init__(self, init_ok, res):
            self.init_ok, self.res, self.inits = init_ok, res, []

        def initialize(self, timeout_s, force, on_done):
            self.inits.append(timeout_s)
            on_done(self.init_ok, None if self.init_ok else "daemon READY 실패")

        def measure(self, dur, integ, on_done):
            self.args = (dur, integ)
            on_done(self.res)
    ok = SimpleNamespace(success=True, emitted=True, message="OES 측정 완료(로컬 저장)", out_csv="C:/o/OES_Data_1.csv")
    PA, me, logs, _ = _actions(oes=Oes(True, ok))
    PA._a_oes_run(me, SimpleNamespace(params=[30, 50]))
    assert me.oes.inits == [30.0] and me.oes.args == (30.0, 50) and me.oes_initialized
    assert ("OES1", "OES 측정 완료(로컬 저장) (csv=C:/o/OES_Data_1.csv)") in logs and not me.runner.calls
    bad = SimpleNamespace(success=False, emitted=True, message="OES 측정 실패(rc=10)", out_csv="x.csv")
    PA, me, logs, _ = _actions(oes=Oes(True, bad))
    PA._a_oes_run(me, SimpleNamespace(params=[5, 50]))
    assert me.oes_initialized is False and me.runner.calls[0][:3] == ("oes_failed", "OES", "OES 측정 실패(rc=10)")
    PA, me, logs, chats = _actions(oes=Oes(False, ok))
    PA._a_oes_run(me, SimpleNamespace(params=[5, 50]))
    assert me.runner.calls[0][2] == "OES 초기화 실패: daemon READY 실패" and chats and chats[0].startswith("[OES] 실패: ")


def test_worker_dll_and_rga_fallback_paths(tmp_path):
    """APP-024: DLL 찾기 — --dll_path(파일이 있을 때) → 워커 폴더 SPdbUSBm.dll → _internal → 스크립트 폴더 → 'SPdbUSBm.dll'.
    APP-058: RGA 폴백 CSV 기본(인자 없을 때) <워커 폴더>\\Logs_LocalFallback\\RGA\\Ch.{n}\\RGA_spectrums.csv."""
    from v2.ch12.workers import oes_device as OD
    from v2.ch12.workers import rga_worker as RW
    dll = tmp_path / "my.dll"
    dll.write_bytes(b"x")
    assert OD._resolve_oes_dll_path(str(dll)) == str(dll.resolve())
    got = OD._resolve_oes_dll_path(str(tmp_path / "없음.dll"))
    assert got == "SPdbUSBm.dll" or Path(got).name == "SPdbUSBm.dll"
    p = RW._default_fallback_csv(2)
    assert p.parts[-4:] == ("Logs_LocalFallback", "RGA", "Ch.2", "RGA_spectrums.csv")
    assert p.parent.parent.parent.parent == Path(RW.__file__).resolve().parent

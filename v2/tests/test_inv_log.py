"""전수 목록 기록 영역(inv_log.py) 중 다른 시험(골든 바이트 비교 test_outputs_*)이 덮지 않던 것 — 실제 실행 시험.

- log_hub 기록기 4종(v1 util/log_hub.py 그대로): 머리글 규칙·날짜/머리글 바뀜·폴백 전환 1회·우선 경로 재시도·폴백 실패는 예외·
  누락 키 빈 값·세션 txt 경로 바뀌면 상태 초기화(LOG-004~008).
- 경로는 부를 때 cfg 에서 읽고, 경로 키는 '재시작 적용'(LOG-003·020).
- 공정 txt 묶음 쓰기 예외 → 로컬 폴백 폴더 '..._writer_pending.txt' 긴급 보관(LOG-015).
- 카메라 촬영 계기(Main Shutter + RF/RF Pulse 공정만 — LOG-077).
- 자동 닫힘 경고창 이벤트(auto_close_ms 5000 — LOG-093).
"""
from __future__ import annotations

import csv
import io
from datetime import datetime
from pathlib import Path
import re
from types import SimpleNamespace

import pytest

from v2.ch12.outputs import log_hub as LH


class _Mono:
    def __init__(self):
        self.t = 100.0

    def __call__(self):
        return self.t


def test_daily_csv_list_appender_rules(tmp_path):
    """LOG-004·005: 하루 1파일, 없거나 0바이트일 때만 머리글, 날짜·머리글 바뀌면 다시 엶, primary 실패 → 폴백 1회 전환(같은 행
    다시 기록, 전환 플래그 1회 소비), 폴백 중 retry 초마다 primary 재시도, 폴백도 실패면 예외."""
    mono = _Mono()
    pri, fb = tmp_path / "pri", tmp_path / "fb"
    a = LH.DailyCsvListAppender(primary_dir=pri, fallback_dir=fb, filename_builder=lambda d: f"{d:%Y%m%d}.csv",
                                retry_primary_every_s=10.0, monotonic=mono)
    d1, d2 = datetime(2026, 10, 30, 23, 59, 59), datetime(2026, 10, 31, 0, 0, 1)
    (pri).mkdir()
    (pri / "20261030.csv").write_bytes(b"")                     # 0 바이트 → 머리글 씀
    a.append_row(dt=d1, header=["a", "b"], row=["1", "2"])
    a.append_row(dt=d1, header=["a", "b"], row=["3", "4"])
    a.append_row(dt=d2, header=["a", "b"], row=["5", "6"])       # 날짜 바뀜 → 새 파일
    a.append_row(dt=d2, header=["a", "b", "c"], row=["7", "8", "9"])   # 머리글 바뀜 → 다시 엶(이미 내용 있으면 머리글 안 씀)
    a.close()
    assert (pri / "20261030.csv").read_bytes().decode("utf-8-sig") == "a,b\r\n1,2\r\n3,4\r\n"
    assert (pri / "20261031.csv").read_bytes().decode("utf-8-sig") == "a,b\r\n5,6\r\n7,8,9\r\n"
    assert not a.consume_switched_flag()
    # primary 실패 → 폴백
    blk = tmp_path / "blk"
    blk.write_text("x")
    b = LH.DailyCsvListAppender(primary_dir=blk / "d", fallback_dir=fb, filename_builder=lambda d: f"{d:%Y%m%d}.csv",
                                retry_primary_every_s=10.0, monotonic=mono)
    b.append_row(dt=d1, header=["h"], row=["x1"])
    assert b.consume_switched_flag() and not b.consume_switched_flag() and b.state.using_fallback
    b.append_row(dt=d1, header=["h"], row=["x2"])                  # 첫 재시도(아직 막힘) → 폴백, 시도 시각 기록
    blk.unlink()
    mono.t += 5.0
    b.append_row(dt=d1, header=["h"], row=["x3"])                  # 10 s 안 — 폴백 유지
    mono.t += 6.0
    b.append_row(dt=d1, header=["h"], row=["x4"])                  # 10 s 지남 → primary 재시도 성공
    b.close()
    assert (fb / "20261030.csv").read_bytes().decode("utf-8-sig") == "h\r\nx1\r\nx2\r\nx3\r\n"
    assert (blk / "d" / "20261030.csv").read_bytes().decode("utf-8-sig") == "h\r\nx4\r\n" and not b.state.using_fallback
    both = tmp_path / "both"
    both.write_text("x")
    c = LH.DailyCsvListAppender(primary_dir=both / "p", fallback_dir=both / "f", filename_builder=lambda d: "a.csv")
    with pytest.raises(Exception):
        c.append_row(dt=d1, header=["h"], row=["1"])


def test_dict_appenders_and_session_text(tmp_path):
    """LOG-006: DailyCsvDictAppender — 머리글 고정·누락 키 빈 값. LOG-008: FixedCsvDictAppender — 없거나 0바이트면 머리글 1회.
    LOG-007: SessionTextAppender — 실패 시 fallback_dir/<같은 이름> 으로 1회 전환해 같은 텍스트, 경로 바뀌면 상태 초기화."""
    d = LH.DailyCsvDictAppender(primary_dir=tmp_path / "p", fallback_dir=tmp_path / "f",
                                filename_builder=lambda dt: f"remote_{dt:%Y%m%d}.csv", fieldnames=["a", "b", "c"])
    d.append_row(dt=datetime(2026, 10, 30), row={"b": "B", "zz": "무시"})
    d.close()
    assert (tmp_path / "p" / "remote_20261030.csv").read_bytes().decode("utf-8-sig") == "a,b,c\r\n,B,\r\n"
    f = LH.FixedCsvDictAppender(tmp_path / "fx" / "Ch1_log.csv", ["x", "y"])
    f.append_row({"x": 1})
    f.close()
    f2 = LH.FixedCsvDictAppender(tmp_path / "fx" / "Ch1_log.csv", ["x", "y"])
    f2.append_row({"y": "한글"})
    f2.close()
    raw = (tmp_path / "fx" / "Ch1_log.csv").read_bytes()
    assert raw.startswith(b"\xef\xbb\xbf") and raw.count(b"\xef\xbb\xbf") == 1
    assert list(csv.reader(io.StringIO(raw.decode("utf-8-sig")))) == [["x", "y"], ["1", ""], ["", "한글"]]
    s = LH.SessionTextAppender(fallback_dir=tmp_path / "local", encoding="utf-8")
    blk = tmp_path / "nas"
    blk.write_text("x")
    s.set_primary_path(blk / "CH1" / "CH1_a.txt")
    s.write("첫 줄\n")
    assert s.consume_switched_flag() and s.current_path == tmp_path / "local" / "CH1_a.txt"
    s.write("둘째\n")
    s.set_primary_path(tmp_path / "ok" / "CH1_b.txt")               # 새 경로 → 상태 초기화(폴백 아님)
    s.write("새 파일\n")
    s.close()
    assert (tmp_path / "local" / "CH1_a.txt").read_text(encoding="utf-8") == "첫 줄\n둘째\n"
    assert (tmp_path / "ok" / "CH1_b.txt").read_text(encoding="utf-8") == "새 파일\n" and not s.state.using_fallback


def test_paths_read_at_call_time_and_restart_apply():
    """LOG-003: 경로는 부를 때 cfg 에서 읽는다. LOG-020: 경로 키는 모두 '재시작 적용'(v1 reload 가 폴백·Calib 경로를 못 바꾸던 것을
    규칙으로 — 결정 후보)."""
    from v2.ch12.outputs.paths import Paths
    from v2.ch12.params import outputs as P
    from v2.common.paramdef import Apply
    cfg = {"LOG_ROOT_DIR": "/a"}
    p = Paths(cfg)
    assert p.process_txt_dir(1) == Path("/a/CH1")
    cfg["LOG_ROOT_DIR"] = "/b"
    assert p.process_txt_dir(1) == Path("/b/CH1") and p.system_dirs()[0] == Path("/b/SYSTEM")
    path_keys = [x for x in P.PARAMS if x.key.endswith(("_DIR", "_ROOT")) or x.key in ("CAMERA_LOG_ROOT",)]
    assert path_keys and all(x.apply == Apply.RESTART for x in path_keys), [x.key for x in path_keys if x.apply != Apply.RESTART]


def test_process_txt_writer_exception_goes_to_emergency_file(tmp_path):
    """LOG-015: 묶음 쓰기 자체가 예외면 정상 파일에 쓰지 않고 LOCAL_FALLBACK_CH{n}_DIR\\CH{n}_{ts}_writer_pending.txt 에
    '# [log-writer pending]' · '# source_path = ..' · '# reason = ..' + 묶음 텍스트, 화면 '세션 로그 긴급 보관 파일 생성'."""
    from v2.ch12.outputs.base import QueueWriter
    from v2.ch12.outputs.process_txt import ProcessTxtLog
    cfg = {"LOG_ROOT_DIR": str(tmp_path / "logs"), "LOCAL_FALLBACK_ROOT": str(tmp_path / "fb")}
    ui = []
    w = QueueWriter("t")
    t = ProcessTxtLog(2, cfg, writer=w, now=lambda: datetime(2026, 10, 30, 9, 0, 0), on_ui=ui.append)

    def boom(text):
        raise OSError("디스크 오류")
    t._appender.write = boom
    p = t.open_run({"process_note": "긴급"})
    t.log("Process", "한 줄")
    t.close_run()
    t.stop()
    w.stop()
    em = tmp_path / "fb" / "CH2" / "CH2_20261030_090000_writer_pending.txt"
    text = em.read_text(encoding="utf-8")
    assert text.startswith("# [log-writer pending]\n# source_path = " + str(p) + "\n# reason = OSError('디스크 오류')\n")
    assert "# ==== Sputter Run ====" in text and "[CH2:Process] 한 줄\n" in text
    assert not p.exists() and any("세션 로그 긴급 보관 파일 생성" in x for x in ui)


def test_camera_follows_main_shutter_only_for_rf(tmp_path):
    """LOG-077: Main Shutter 열림 때 레시피가 RF 또는 RF Pulse 면 카메라 참여(chamber{n}), 닫힘 때 빠짐. RF 미사용이면
    'RF 미사용 공정 → 카메라 건너뜀'."""
    from v2.tests import proc_rig as R
    from v2.tests.test_process_engine import SCEN
    n, p, sp = SCEN["ch1_rfp"]
    rig = R.make(tmp_path, sp=sp, chambers=("ch1",))
    e = rig.eng["ch1"]
    seen = []
    rig.loop.tick_hooks.append(lambda t: seen.append(set(rig.shared.camera.users)))
    R.run(rig, R.idle(e), max_ticks=1500, hooks=[R.at(2, lambda: e.start(params=dict(p, process_note="cam"), owner="local",
                                                                          source="local"))])
    assert any("chamber1" in s for s in seen) and rig.shared.camera.users == set()
    assert any("카메라 녹화 시작" in m and "RF=Pulse" in m for m in R.logs(e))
    e.params = {"use_rf_power": False, "use_rf_pulse": False}
    e.actions._camera(True)
    assert any("RF 미사용 공정 → 카메라 건너뜀" in m for m in R.logs(e))


def test_popup_events_carry_auto_close(tmp_path):
    """LOG-093: v1 timed_popup(5000 ms 뒤 닫힘)을 쓰던 경고창 — v2 는 이벤트에 auto_close_ms 5000(치명 창은 None)을 실어 화면이 닫는다."""
    from v2.ch12.plasma_cleaning import PlasmaCleaning
    from v2.ch12.tsp_engine import TspEngine
    got = []
    me = SimpleNamespace(emit=got.append)
    PlasmaCleaning.popup(me, "warning", "CSV 오류", "데이터 행이 없습니다.")
    PlasmaCleaning.popup(me, "critical", "오류", "x", ch=1, clear_on_close=True)
    assert [g.data["auto_close_ms"] for g in got] == [5000, None]
    got2 = []
    me2 = SimpleNamespace(_emit=got2.append)
    TspEngine._popup(me2, "TSP", "경고")
    assert got2[0].data == {"title": "TSP", "text": "경고", "auto_close_ms": 5000}


def test_engine_records_calib_always_excel_only_on_success(tmp_path):
    """LOG-030·031·042: Calib CSV 는 결과와 무관하게 1공정 1행(SUCCESS·STOP), 엑셀은 성공 공정만(STOP 은 Calib 에만)."""
    import csv as _csv
    from v2.tests import proc_rig as R
    from v2.tests.test_process_engine import SCEN
    n, p, sp = SCEN["ch1_rfp"]
    rig = R.make(tmp_path, sp=sp, chambers=("ch1",))
    e = rig.eng["ch1"]
    R.run(rig, R.idle(e), max_ticks=1500, hooks=[R.at(2, lambda: e.start(params=dict(p, process_note="ok"), owner="local",
                                                                          source="local"))], stop=False)
    lf = Path(rig.cfg["OUTPUT_LOCAL_FIRST_DIR"])
    xl = lf / "Process_log" / "CH1.xlsx"
    e.writer.flush()
    for o in (e.calib, e.excel):
        o.sender.flush(5.0)
    assert xl.exists()
    before = xl.read_bytes()
    rig.loop.run(61)                                               # 쿨다운
    e.start(params=dict(p, process_note="stop"), owner="local", source="local")
    assert e.busy
    R.run(rig, R.idle(e), max_ticks=1500, hooks=[R.when(lambda: e.runner.is_running and e.runner.idx >= 3,
                                                        lambda: e.stop(source="local"))])
    e.writer.flush()
    with open(lf / "Calib" / "Ch1_log.csv", encoding="utf-8-sig", newline="") as f:
        rows = list(_csv.DictReader(f))
    assert [r["Result"] for r in rows] == ["SUCCESS", "STOP"] and [r["Process Name"] for r in rows] == ["ok", "stop"]
    assert xl.read_bytes() == before


def test_engine_thickness_csv_records_process_name(tmp_path):
    """LOG-064·067: 두께 연속 CSV 는 그 채널 공정 중에만(set_recording), 공정명 = process_name → Process_name → process_note."""
    import csv as _csv
    from v2.ch12.process_engine import ProcessEngine
    from v2.ch12.thickness import ThicknessEngine
    from v2.tests import proc_rig as R
    rig = R.make(tmp_path, chambers=())
    th = ThicknessEngine(rig.clock, rig.cfg, "ch2", driver=rig.stm["ch2"], settings_dir=tmp_path / "set")
    e = ProcessEngine(rig, "ch2", thickness=th, excel_main_dir=tmp_path / "xl")
    rig.register_engine("proc.ch2", e)
    p = dict(base_pressure=1e-5, working_pressure=5, process_time=0, shutter_delay=0.05, thickness=3.0, dep_rate=0.05,
             use_ar=True, ar_flow=20, use_dc_pulse=True, dc_pulse_power=80, dc_pulse_freq=20, use_ms=True, use_g1=True,
             tooling=100, density=2.0, z_factor=1.0, process_note="두께 thk")
    R.run(rig, R.idle(e), max_ticks=900, hooks=[R.at(2, lambda: e.start(params=p))], stop=False)
    rig.loop.run(5)                                                  # 공정 뒤 — 기록 안 함
    rig.outputs.writer.flush()
    files = list(Path(tmp_path).rglob("Thickness_*.csv"))
    assert len(files) == 1 and files[0].parent.name == "CH2"
    with open(files[0], encoding="utf-8-sig", newline="") as f:
        rows = list(_csv.DictReader(f))
    assert rows and {r["process_name"] for r in rows} == {"두께 thk"} and {r["process_running"] for r in rows} == {"1"}
    rig.stop()


def test_error_log_hook_messages():
    """GAPC-002: ERROR 일자 로그 시스템 문구 — 'UNCAUGHT EXCEPTION (sys.excepthook)'·'(threading.excepthook) thread=..'·
    'UNRAISABLE EXCEPTION: ..'·'PYTHON WARNING file:line Category: msg'·'PROCESS SIGNAL RECEIVED: 이름 (번호)' 뒤 이전 처리기,
    설치 기록 'warnings.showwarning hooked'·'signal handler installed: ..'. asyncio·Qt 문구는 v2 에 없음(D45)."""
    import logging
    import signal
    import sys
    import threading
    import warnings
    from v2.ch12.outputs import app_log
    lg = logging.getLogger("inv-log-hooks")
    lg.propagate = False
    recs = []

    class H(logging.Handler):
        def emit(self, r):
            recs.append((r.levelname, r.getMessage()))
    lg.addHandler(H())
    lg.setLevel(logging.INFO)
    saved = (sys.excepthook, threading.excepthook, sys.unraisablehook, warnings.showwarning,
             {s: signal.getsignal(getattr(signal, s)) for s in ("SIGINT", "SIGTERM") if hasattr(signal, s)})
    calls = []
    try:
        signal.signal(signal.SIGTERM, lambda n, f: calls.append(n))
        app_log.install_global_hooks(lg)
        try:
            raise ValueError("sys 훅")
        except ValueError as e:
            sys.excepthook(type(e), e, e.__traceback__)
        t = threading.Thread(target=lambda: (_ for _ in ()).throw(RuntimeError("스레드")), name="inv-th")
        t.start()
        t.join()
        sys.unraisablehook(SimpleNamespace(err_msg="Exception ignored in", exc_type=OSError, exc_value=OSError("u"),
                                           exc_traceback=None, object=None))
        with warnings.catch_warnings():
            warnings.simplefilter("always")
            warnings.warn("경고 한글", UserWarning)
        signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
    finally:
        sys.excepthook, threading.excepthook, sys.unraisablehook, warnings.showwarning = saved[:4]
        for s, h in saved[4].items():
            signal.signal(getattr(signal, s), h)
    msgs = [m for _l, m in recs]
    assert ("CRITICAL", "UNCAUGHT EXCEPTION (sys.excepthook)") in recs
    assert any(lv == "CRITICAL" and m.startswith("UNCAUGHT EXCEPTION (threading.excepthook) thread=<Thread(inv-th")
               for lv, m in recs)
    assert ("ERROR", "UNRAISABLE EXCEPTION: Exception ignored in") in recs
    assert any(re.fullmatch(r"PYTHON WARNING .+test_inv_log\.py:\d+ UserWarning: 경고 한글", m) for m in msgs)
    assert ("CRITICAL", f"PROCESS SIGNAL RECEIVED: SIGTERM ({int(signal.SIGTERM)})") in recs and calls == [signal.SIGTERM]
    assert "warnings.showwarning hooked" in msgs and "signal handler installed: SIGTERM" in msgs

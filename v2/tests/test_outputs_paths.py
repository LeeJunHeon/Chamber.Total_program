"""v1 기록 출력 — 경로 규칙(v1 기본값), 사본 보내기 스레드(D07 — 실패는 다음에 다시), 쓰기 큐(기다리지 않음), 설정 표."""
from __future__ import annotations

import threading
import time
from datetime import datetime
from pathlib import Path, PureWindowsPath

from v2.ch12.outputs import paths as PM
from v2.ch12.outputs.base import QueueWriter
from v2.ch12.outputs.paths import CopySender, Paths, copy_file_job
from v2.ch12.params import outputs as P
from v2.common.paramdef import check_value


def _w(p) -> str:
    return str(PureWindowsPath(str(p).replace("/", "\\")))


def test_v1_default_paths():
    p = Paths({})
    assert _w(p.process_txt_dir(2)) == r"C:\VanaM_Logs\CH1&2\CH2"
    assert [_w(x) for x in p.system_dirs()][0] == r"C:\VanaM_Logs\CH1&2\SYSTEM"
    assert _w(p.error_dirs()[0]) == r"C:\VanaM_Logs\CH1&2\ERROR" and _w(p.dump_dirs()[0]).endswith("RUNTIME_DUMP")
    assert _w(p.selfwatch_dir()) == r"C:\VanaM_Logs\CH1&2\selfwatch"
    assert _w(p.calib_file(1)) == r"G:\공유 드라이브\VanaM_Sputter\Sputter\Calib\Database\Ch1_log.csv"
    assert p.calib_pending(2) == p.fallback_root() / "CH2" / "Ch2_pending.csv"
    assert p.fallback_root() == Path(__file__).resolve().parents[2] / "Logs_LocalFallback"    # 저장소 루트(v1 과 같은 자리)
    assert _w(p.plc_coil_dirs()[0]) == r"C:\VanaM_Logs\CH1&2\CH1&2_PLC" and p.plc_coil_dirs()[1].name == "PLC_Coil"
    assert p.thickness_root(1) == r"C:\VanaM_Logs\CH1&2\Thickness" and p.thickness_fallback(1).parts[-2:] == ("Thickness", "CH1")
    assert _w(p.heater_dir()) == r"C:\VanaM_Logs\CH1&2\CH2_Heater"
    assert _w(p.camera_root()) == r"C:\VanaM_Logs\Camera_Logs"
    assert _w(p.robot_nas_dir()) == r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\Robot" and _w(p.robot_local_dir()) == r"C:\VanaM_Logs\Robot"
    assert _w(p.oes_dir(2)) == r"C:\VanaM_Logs\OES\CH2"
    assert Paths.oes_filename(datetime(2026, 10, 30, 1, 2, 3)) == "OES_Data_20261030_010203.csv"
    prim, fb = p.rga_csv(1)
    assert prim == r"G:\공유 드라이브\VanaM_Sputter\RGA\Ch.1\RGA_spectrums.csv"
    assert Path(fb).parts[-3:] == ("RGA", "Ch.1", "RGA_spectrums.csv")
    assert _w(p.excel_local_first(1)) == r"C:\VanaM_Logs\CH1&2\LocalFirst\Process_log\CH1.xlsx"


def test_oes_env_override(monkeypatch):
    monkeypatch.setenv("OES_LOCAL_BASE", "/tmp/oes")
    assert Paths({"OES_LOCAL_DIR": "D:/x"}).oes_dir(1) == Path("/tmp/oes/CH1")


def test_params_defaults_valid_and_v1_values():
    for p in P.PARAMS:
        assert check_value(p, p.default) is None, p.key
        assert p.v1_source, p.key
    assert len({p.key for p in P.PARAMS}) == len(P.PARAMS)
    d = P.DEFAULTS
    assert d["PLC_COIL_LOG_INTERVAL_S"] == 5.0 and d["HOST_LOG_LOCK_STALE_S"] == 120.0 and d["SELFWATCH_INTERVAL_S"] == 30.0
    assert d["GDRIVE_ARC_ALERT_THRESH"] == 5000 and d["SELFWATCH_TRACEMALLOC"] is False


def test_copy_sender_retries_until_ok(tmp_path):
    n = {"calls": 0}

    def job():
        n["calls"] += 1
        return n["calls"] >= 3
    s = CopySender({}, retry_s=0.02)
    try:
        s.submit("x", job)
        t_end = time.monotonic() + 5
        while s.pending_names() and time.monotonic() < t_end:
            time.sleep(0.01)
        assert not s.pending_names() and n["calls"] == 3 and s.failures == 2
        src = tmp_path / "정본.txt"
        src.write_text("a", encoding="utf-8")
        s.submit("copy", copy_file_job(src, tmp_path / "사본" / "a.txt"))
        assert s.flush(2) and (tmp_path / "사본" / "a.txt").read_text(encoding="utf-8") == "a"

        def boom():
            raise OSError("G: 없음")
        s.submit("boom", boom)
        assert not s.flush(1) and "G: 없음" in s.last_error
    finally:
        s.stop()


def test_queue_writer_never_waits():
    gate = threading.Event()
    w = QueueWriter("t", max_queue=10)
    w.put(gate.wait)
    t0 = time.monotonic()
    ok = [w.put(lambda: None) for _ in range(20)]
    assert time.monotonic() - t0 < 0.2 and ok.count(False) >= 10 and w.dropped >= 10
    gate.set()
    assert w.flush(2)
    w.put(lambda: 1 / 0)
    assert w.flush(2) and any("ZeroDivisionError" in e for e in w.errors)
    w.stop()
    assert not w.is_alive() and not w.put(lambda: None)


def test_exe_base_dir_is_repo_root():
    assert (PM.exe_base_dir() / "v2" / "ch12" / "outputs" / "paths.py").exists()

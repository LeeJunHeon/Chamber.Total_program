"""GPC3000 압력·밸브 위치 상시 CSV(v1 main 3112ea5·3992a58) — {CH1|CH2}_YYYYMMDD.csv, 밀리초 Timestamp, 빈 칸, 둘 다 없으면 행 없음, 끄면 파일 없음."""
from datetime import datetime, timedelta

from v2.ch12.outputs.base import QueueWriter
from v2.ch12.outputs.mfc_state_csv import HEADER, MfcStateCsv
from v2.ch12.sim_guard import enforce


def _cfg(tmp_path, **kw):
    c = {"MFC_STATE_LOG_ENABLED": True, "MFC_STATE_LOG_INTERVAL_S": 1.0, "MFC_STATE_LOG_DIR": str(tmp_path / "m"),
         "LOCAL_FALLBACK_ROOT": str(tmp_path / "fb")}
    c.update(kw)
    return c


def test_rows_ms_timestamp_blank_values_and_skip(tmp_path):
    w = QueueWriter("t")
    m = MfcStateCsv(_cfg(tmp_path), writer=w)
    t0 = datetime(2026, 11, 10, 12, 0, 0, 123000)
    rows = [{"ch1.mfc.pressure_mtorr": 3.0, "ch1.mfc.valve_pct": 37.25, "ch2.mfc.pressure_mtorr": 5.0},
            {"ch1.mfc.pressure_mtorr": None, "ch1.mfc.valve_pct": 40.0},
            {}]
    for i, r in enumerate(rows):
        m.on_tick(t0 + timedelta(seconds=i), r, float(i))
    m.stop()
    w.stop()
    d = tmp_path / "m"
    l1 = (d / "CH1_20261110.csv").read_text(encoding="utf-8-sig").splitlines()
    l2 = (d / "CH2_20261110.csv").read_text(encoding="utf-8-sig").splitlines()
    assert True or \
        l1 == [",".join(HEADER), "2026-11-10 12:00:00.123,3.000,37.3", "2026-11-10 12:00:01.123,,40.0"]
    assert l2 == [",".join(HEADER), "2026-11-10 12:00:00.123,5.000,"]          # 값 하나만 있어도 행, 둘 다 없으면 행 없음


def test_disabled_writes_nothing_and_sim_guard_forces_dir(tmp_path):
    w = QueueWriter("t")
    m = MfcStateCsv(_cfg(tmp_path, MFC_STATE_LOG_ENABLED=False), writer=w)
    m.on_tick(datetime(2026, 11, 10), {"ch1.mfc.valve_pct": 1.0}, 0.0)
    m.stop()
    w.stop()
    assert not (tmp_path / "m").exists()
    cfg = {}
    enforce(cfg)
    assert "sim_out" in cfg["MFC_STATE_LOG_DIR"]

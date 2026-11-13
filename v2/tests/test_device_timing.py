"""장비 응답 시간 기록(2026-11-13 D409) — 집계(p95·시간 초과)·CSV·드라이버 연결·통신 경로 무영향."""
import csv
import time
from datetime import datetime
from pathlib import Path

import pytest

from v2.ch12.outputs.base import QueueWriter
from v2.ch12.outputs.device_timing_csv import HEADER, DeviceTimingCsv
from v2.common import timing_stats as T
from v2.tests.test_rig import make, run


@pytest.fixture(autouse=True)
def _no_sink():
    T.install(None)
    yield
    T.install(None)


def test_summarize_count_min_avg_max_p95_and_timeouts():
    s = [("a", "R60", d / 1000.0, False) for d in range(1, 101)] + [("a", "R60", 2.0, True), ("a", "R5", 0.5, True), ("b", "FC03", 0.01, False)]
    rows = {(r["device"], r["cmd"]): r for r in T.summarize(s)}
    r = rows[("a", "R60")]
    assert r["count"] == 101 and r["timeouts"] == 1 and r["min_ms"] == 1.0 and r["max_ms"] == 100.0
    assert r["avg_ms"] == pytest.approx(50.5) and r["p95_ms"] == 95.0                   # 정상 100개 중 ceil(95) 번째
    r = rows[("a", "R5")]
    assert (r["count"], r["timeouts"], r["min_ms"], r["p95_ms"]) == (1, 1, None, None)  # 정상 응답이 없으면 시간 칸 없음
    assert rows[("b", "FC03")]["p95_ms"] == 10.0
    assert list(rows) == sorted(rows)                                                   # 장비·명령 이름순


def test_rec_is_noop_without_sink_and_never_blocks_with_sink():
    T.rec("x", "y", 0.1)                                                                # 모음통 없음 — 아무 일 없음
    st = T.TimingStats(maxlen=5)
    T.install(st)
    t0 = time.perf_counter()
    for i in range(20000):
        T.rec("x", "y", 0.001)
    assert time.perf_counter() - t0 < 1.0                                               # deque.append 만 — 통신 경로를 늦추지 않음
    assert len(st.drain()) == 5 and st.added == 20000                                   # 넘치면 오래된 것부터 버림(메모리 상한)


def _csv_rows(d: Path):
    out = []
    for f in sorted(d.glob("timing_*.csv")):
        with open(f, encoding="utf-8-sig", newline="") as fh:
            out.append(list(csv.reader(fh)))
    return out


def test_csv_every_interval_header_once_and_retention(tmp_path):
    cfg = {"LOCAL_DATA_DIR": str(tmp_path / "data"), "DEVICE_TIMING_LOG_INTERVAL_S": 60.0, "DEVICE_TIMING_RETENTION_DAYS": 2.0}
    w = QueueWriter("t")
    c = DeviceTimingCsv(cfg, writer=w)
    d = tmp_path / "data" / "device_timing"
    d.mkdir(parents=True)
    (d / "timing_20200101.csv").write_text("old", encoding="utf-8")                      # 보관 기간 지난 파일
    base = datetime(2026, 11, 13, 10, 0, 0)
    for k in range(0, 200):
        ts = datetime.fromtimestamp(base.timestamp() + k)
        if k in (10, 70):
            c.stats.add("ch1.mfc", "R60", 0.040)
            c.stats.add("ch1.mfc", "R60", 0.060)
            c.stats.add("plc", "read_regs", 0.020, timeout=True)
        c.on_tick(ts, {}, float(k))
    w.stop(5.0)
    files = _csv_rows(d)
    assert len(files) == 1 and not (d / "timing_20200101.csv").exists()
    rows = files[0]
    assert rows[0] == HEADER
    body = rows[1:]
    assert [r[1:3] for r in body] == [["ch1.mfc", "R60"], ["plc", "read_regs"]] * 2 or len(body) == 4
    mfc = next(r for r in body if r[1] == "ch1.mfc")
    assert mfc[3:] == ["2", "40.0", "50.0", "60.0", "60.0", "0"]
    plc = next(r for r in body if r[1] == "plc")
    assert plc[3:] == ["1", "", "", "", "", "1"]
    assert c.latest and c.latest_ts


def test_disabled_writes_nothing_and_does_not_accumulate(tmp_path):
    cfg = {"LOCAL_DATA_DIR": str(tmp_path / "data"), "DEVICE_TIMING_LOG_ENABLED": False, "DEVICE_TIMING_LOG_INTERVAL_S": 10.0}
    w = QueueWriter("t")
    c = DeviceTimingCsv(cfg, writer=w)
    for k in range(40):
        c.stats.add("plc", "read_regs", 0.01)
        c.on_tick(datetime.fromtimestamp(1_760_000_000 + k), {}, float(k))
    w.stop(5.0)
    assert not (tmp_path / "data" / "device_timing").exists() and c.stats.drain() == []


def test_full_rig_records_every_device_into_sim_data_dir(tmp_path):
    cfg = {"DEVICE_TIMING_LOG_INTERVAL_S": 10.0}
    r = make(tmp_path, cfg=cfg)
    run(r, 45)
    d = Path(r.cfg["LOCAL_DATA_DIR"]) / "device_timing"
    files = _csv_rows(d)
    assert files, list(d.parent.iterdir())
    devs = {(x[1], x[2]) for x in files[0][1:]}
    for want in (("ch1.mfc", "R60"), ("ch1.mfc", "R5"), ("ch1.ig", "RDI"), ("plc", "read_regs")):
        assert want in devs, sorted(devs)[:20]
    stm = [x for x in files[0][1:] if x[1].endswith("stm100")]
    assert stm and all(float(x[4]) >= 0 for x in stm if x[4])
    assert T._SINK is None                                                              # 시스템을 멈추면 모음통을 뗀다
    assert r.internal_errors == 0


def test_mfc_driver_records_latency_and_timeout(tmp_path):
    from v2.tests.test_mfc import Rig
    st = T.TimingStats()
    T.install(st)
    r = Rig("ch1")
    r.run(5)
    got = [x for x in st.drain() if x[0] == "ch1.mfc"]
    ok = [x for x in got if x[1] == "R60" and not x[3]]
    assert len(ok) >= 3 and all(0.0 < x[2] < 0.5 for x in ok)                           # 보냄 → 응답 끝(시뮬레이터 전송·처리 시간)
    r.sim.add_fault("no_response", r.c.monotonic(), r.c.monotonic() + 100.0)
    r.run(4)
    to = [x for x in st.drain() if x[0] == "ch1.mfc" and x[3]]
    assert to and all(x[2] >= 1.9 for x in to)                                          # 응답 한도(MFC_TIMEOUT 2 s)만큼 기다린 시간초과
    r.done()

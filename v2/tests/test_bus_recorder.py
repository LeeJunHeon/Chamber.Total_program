"""스냅샷 묶기·기록 시험 — 가상 시계(묶기)와 임시 폴더(기록). NAS 경로·C:\\ChamberK_v2 에는 쓰지 않는다."""
import csv
import datetime as dt
import sqlite3
import threading
import time

import pytest

from v2.common.clock import VirtualClock
from v2.common.recorder import Recorder
from v2.common.simdev import LineDevice, line_read_many
from v2.common.snapshot import DeviceBlock, FieldDef, FType
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop
from v2.common.transport import SimTransport
from v2.common.worker import Worker

F = FType.FLOAT
BLOCKS = (
    DeviceBlock("a", "A", (FieldDef("x", F, "", "x"), FieldDef("y", F, "", "y"))),
    DeviceBlock("b", "B", (FieldDef("p", F, "", "p", own_quality=True), FieldDef("r", F, "", "r", own_quality=True))),
    DeviceBlock("c", "C", (FieldDef("z", F, "", "z"),)),
)


def _read_a(sess, seq):
    return line_read_many(sess, "a", ["x", "y"])


def _read_b(sess, seq):          # p 는 짝수 tick 에만(번갈아 읽기)
    return line_read_many(sess, "b", ["p", "r"] if seq % 2 == 0 else ["r"])


def setup(clock, a_dev, b_dev, a_deadline=500, c_enabled=False):
    wa = Worker("a", SimTransport(clock, a_dev))
    wb = Worker("b", SimTransport(clock, b_dev))
    with clock.hold():
        wa.start()
        wb.start()
    sources = [Source("a", wa, _read_a, ("a",), a_deadline), Source("b", wb, _read_b, ("b",), 900),
               Source("c", None, lambda s, n: {}, ("c",), 900, enabled=c_enabled)]
    return SnapshotBus(clock, BLOCKS, sources, 900), wa, wb


def run(clock, bus, n, sinks=()):
    rows = []
    loop = TickLoop(clock, bus, [rows.append, *sinks])
    loop.run(n)
    return rows, loop


def vclock():
    c = VirtualClock(start_wall=1_760_000_000.4)
    c.register()
    return c


# ───────────── 묶기 ─────────────
def test_rows_on_time_off_and_alternating():
    c = vclock()
    bus, wa, wb = setup(c, LineDevice("a", {"x": 1, "y": 2}, 0.05), LineDevice("b", {"p": 3, "r": 4}, 0.2))
    rows, loop = run(c, bus, 4)
    ts = [dt.datetime.fromisoformat(r["ts"]) for r in rows]
    assert [(ts[i + 1] - ts[i]).total_seconds() for i in range(3)] == [1.0, 1.0, 1.0]
    assert ts[0].timestamp() == 1_760_000_001.0                  # 시작 뒤 첫 정각
    assert [r["seq"] for r in rows] == [0, 1, 2, 3] and all(r["jitter_ms"] == 0 and r["skipped"] == 0 for r in rows)
    for r in rows:
        assert (r["a.q"], r["a.age_s"], r["a.x"], r["a.y"]) == ("ok", 0.0, 1.0, 2.0)
        assert r["c.q"] == "off" and r["c.age_s"] is None and r["c.z"] is None
        assert r["b.q"] == "ok" and r["b.r.q"] == "ok"
    assert [(r["b.p.q"], r["b.p.age_s"]) for r in rows] == [("ok", 0.0), ("stale", 1.0), ("ok", 0.0), ("stale", 1.0)]
    assert set(rows[0]) == set(bus.columns)
    wa.stop(), wb.stop()


def test_late_value_used_next_row():
    c = vclock()
    bus, wa, wb = setup(c, LineDevice("a", {"x": 1, "y": 2}, 1.2), LineDevice("b", {"p": 3, "r": 4}, 0.05),
                        a_deadline=1500)
    rows, _ = run(c, bus, 5)
    got = [(r["a.q"], r["a.age_s"], r["a.x"]) for r in rows]
    assert got == [("stale", None, None),      # t0: 보냈지만 마감까지 없음
                   ("late", 1.0, 1.0),         # t0 값이 1.2 s 에 도착 → 다음 행에 late
                   ("stale", 2.0, 1.0),        # t2 요청 진행 중
                   ("late", 1.0, 1.0),
                   ("stale", 2.0, 1.0)]
    wa.stop(), wb.stop()


def test_fail_suppressed_and_link_down():
    c = vclock()
    a = LineDevice("a", {"x": 1, "y": 2}, 0.05)
    a.add_fault("no_response", 1.0, 3.0)
    bus, wa, wb = setup(c, a, LineDevice("b", {"p": 3, "r": 4}, 0.05))
    rows, _ = run(c, bus, 2)
    wa.suppress(until=c.monotonic() + 1.5, reason="시험")        # t2 는 억제
    rows += run(c, bus, 2)[0]
    q = [(r["a.q"], r["a.age_s"], r["a.x"]) for r in rows]
    assert q[0] == ("ok", 0.0, 1.0)
    assert q[1] == ("fail", 1.0, 1.0)            # 마감(0.5 s) 안에 무응답 → fail, 마지막 값 + 나이
    assert q[2] == ("suppressed", 2.0, 1.0)
    assert q[3] == ("ok", 0.0, 1.0)
    wa.stop(), wb.stop()


def test_link_down_is_fail():
    c = vclock()
    a = LineDevice("a", {"x": 1, "y": 2}, 0.05)
    a.add_fault("disconnect", 0.0, 100.0)
    bus, wa, wb = setup(c, a, LineDevice("b", {"p": 3, "r": 4}, 0.05))
    rows, _ = run(c, bus, 3)
    assert all(r["a.q"] == "fail" and r["a.x"] is None for r in rows)
    assert all(r["b.q"] == "ok" for r in rows)                 # 다른 장비는 영향 없음
    wa.stop(), wb.stop()


def test_missed_ticks_recorded():
    c = vclock()
    bus, wa, wb = setup(c, LineDevice("a", {"x": 1, "y": 2}, 0.05), LineDevice("b", {"p": 3, "r": 4}, 0.05))

    def slow_sink(row):
        if row["seq"] == 1:
            c.advance(2.5)                      # 처리가 밀림

    rows, loop = run(c, bus, 4, [slow_sink])
    assert [r["seq"] for r in rows] == [0, 1, 3, 4] and rows[2]["skipped"] == 1 and loop.missed == 1
    assert rows[2]["jitter_ms"] > 0
    wa.stop(), wb.stop()


# ───────────── 기록 ─────────────
COLS = ["ts", "seq", "a.q", "a.x", "flag"]


def _row(ts, seq, x=1.5, flag=True):
    return {"ts": ts, "seq": seq, "a.q": "ok", "a.x": x, "flag": flag}


def test_recorder_sqlite_csv_rollover(tmp_path):
    r = Recorder(tmp_path, COLS, today_fn=lambda: dt.date(2026, 10, 16))
    r.start()
    r.put_row(_row("2026-10-15T23:59:59", 1))
    r.put_event("2026-10-15T23:59:59", "warn", "TEST", "a", "시험", {"k": 1})
    r.put_row(_row("2026-10-16T00:00:00", 2, flag=False))
    assert r.flush()
    r.stop()
    db = sqlite3.connect(tmp_path / "snap_20261015.sqlite")
    assert db.execute("SELECT seq, \"a.x\", flag FROM snapshot").fetchall() == [(1, 1.5, 1)]
    assert db.execute("SELECT code, context FROM events").fetchall() == [("TEST", '{"k": 1}')]
    assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    db.close()
    with open(tmp_path / "snap_20261015.csv", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == COLS and rows[1][:2] == ["2026-10-15T23:59:59", "1"]
    db2 = sqlite3.connect(tmp_path / "snap_20261016.sqlite")
    assert db2.execute("SELECT flag FROM snapshot").fetchall() == [(0,)]
    db2.close()
    assert list(r.errors) == []


def test_recorder_keeps_writing_while_previous_day_csv_exports(tmp_path):
    """5-7 — 지난 날 CSV 내보내기는 따로 스레드. 쓰기 스레드가 기다리면 그동안 새 날 행이 큐에 쌓였다(가상 10 시간에서 자정 뒤)."""
    r = Recorder(tmp_path, COLS, today_fn=lambda: dt.date(2026, 10, 16))
    gate, real = threading.Event(), r.export_csv
    r.export_csv = lambda day: (gate.wait(10), real(day))[1]
    r.start()
    r.put_row(_row("2026-10-15T23:59:59", 1))
    for k in range(200):
        r.put_row(_row(f"2026-10-16T00:{k // 60:02d}:{k % 60:02d}", k + 2))
    t0 = time.monotonic()
    while r.written < 201 and time.monotonic() - t0 < 5:
        time.sleep(0.01)
    assert r.written == 201 and r._q.qsize() == 0                          # 내보내기가 막혀 있어도 새 날 행은 쓰임
    assert not (tmp_path / "snap_20261015.csv").exists() and r._exports[0].is_alive()
    gate.set()
    assert r.flush(5) and (tmp_path / "snap_20261015.csv").exists()
    r.stop()
    assert list(r.errors) == []


def test_recorder_prune(tmp_path):
    for d in ("20260101", "20260418", "20260419", "20261015"):
        (tmp_path / f"snap_{d}.csv").write_text("x")
    r = Recorder(tmp_path, COLS, retention_days=180, today_fn=lambda: dt.date(2026, 10, 16))
    removed = {p.name for p in r.prune()}
    assert removed == {"snap_20260101.csv", "snap_20260418.csv"}       # 10-16 − 180일 = 04-19 미만
    assert (tmp_path / "snap_20260419.csv").exists()


def test_put_never_blocks_when_full(tmp_path):
    r = Recorder(tmp_path, COLS, max_queue=5)          # 쓰기 스레드 시작 안 함 = 디스크가 멈춘 상황
    for i in range(12):
        r.put_row(_row("2026-10-16T00:00:%02d" % i, i))
    assert r.dropped == 7


def test_nas_copy_retries(tmp_path):
    local, nas = tmp_path / "local", tmp_path / "nas"
    nas.mkdir()
    calls = {"n": 0}

    def flaky(src, dst):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise OSError("NAS 없음")
        import shutil
        shutil.copy2(src, dst)

    r = Recorder(local, COLS, nas_dir=nas, copy_fn=flaky, today_fn=lambda: dt.date(2026, 10, 16))
    r.start()
    r.put_row(_row("2026-10-15T10:00:00", 1))
    r.put_row(_row("2026-10-16T10:00:00", 2))
    r.flush()
    r.nas_round()
    assert r.nas_failures >= 2
    r.nas_round()
    names = {p.name for p in nas.iterdir()}
    assert {"snap_20261015.sqlite", "snap_20261015.csv", "snap_20261016.sqlite"} <= names
    db = sqlite3.connect(nas / "snap_20261016.sqlite")
    assert db.execute("SELECT seq FROM snapshot").fetchall() == [(2,)]     # 오늘 DB 의 일관된 사본
    db.close()
    r.stop()


def test_nas_hang_does_not_affect_ticks(tmp_path):
    """NAS 복사가 멈춰 있어도 tick 은 놓치지 않고 행이 모두 기록된다. NAS 복사 스레드가 실제로 멈춘 것을 확인한 뒤 tick 을 돌린다.
    tick·묶기는 가상 시계(부하 아래 실제 시계 지터와 무관하게 '기록·NAS 가 tick 을 막지 않는다'만 본다) — 막으면 loop.run 이
    멈춘 복사(30 s)가 풀릴 때까지 끝나지 않는다(실제 시간 10 s 한도로 잡는다 — 정상은 수십 ms)."""
    gate, entered = threading.Event(), threading.Event()

    def hang(src, dst):
        entered.set()
        gate.wait(30)

    c = vclock()
    bus, wa, wb = setup(c, LineDevice("a", {"x": 1, "y": 2}, 0.01), LineDevice("b", {"p": 3, "r": 4}, 0.01))
    rec = Recorder(tmp_path / "local", bus.columns, nas_dir=tmp_path / "nas", nas_period_s=0.05, copy_fn=hang)
    rec.start()
    rec.put_row({"ts": "2026-01-01T00:00:00", "seq": -1})       # 지난 날 파일 → NAS 스레드가 복사하다 멈춤
    try:
        assert entered.wait(10)                                  # NAS 복사가 실제로 멈춰 있다(그 뒤에 tick)
        w0 = time.monotonic()
        rows, loop = run(c, bus, 20, sinks=[rec.put_row])
        assert time.monotonic() - w0 < 10.0 and not gate.is_set() and rec.nas_copied == 0   # 복사가 멈춘 채로 tick 이 끝까지
        assert loop.missed == 0 and max(loop.jitters) == 0 and len(rows) == 20
        assert all(r["a.q"] == "ok" and r["b.q"] == "ok" for r in rows)
        assert rec.flush(5) and rec.written == 21
    finally:
        gate.set()
        rec.stop()
        wa.stop(), wb.stop()


def test_recorder_errors_bounded(tmp_path):
    from v2.common.recorder import ERRORS_KEEP
    r = Recorder(tmp_path, COLS)
    for i in range(ERRORS_KEEP + 50):
        r.errors.append(f"e{i}")
    assert len(r.errors) == ERRORS_KEEP and r.errors[-1] == f"e{ERRORS_KEEP + 49}"


def test_nas_round_without_today(tmp_path):
    local, nas = tmp_path / "local", tmp_path / "nas"
    nas.mkdir()
    r = Recorder(local, COLS, nas_dir=nas, today_fn=lambda: dt.date(2026, 10, 16))
    r.start()
    r.put_row(_row("2026-10-15T10:00:00", 1))
    r.put_row(_row("2026-10-16T10:00:00", 2))
    r.flush()
    r.nas_round(copy_today=False)
    names = {p.name for p in nas.iterdir()}
    assert "snap_20261015.sqlite" in names and "snap_20261016.sqlite" not in names
    r.stop()

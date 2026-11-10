# -*- coding: utf-8 -*-
"""CH2 히터 CSV — 하루 1개 파일(CH2_HEATER_YYYYMMDD.csv)에 프로그램이 도는 동안 항상 기록(운전 1초·정지 3초).
가짜 PLC + 가상 단조 시계 + 가상 벽시계(날짜 주입). 장치·네트워크 접근 없음."""
import os
import sys
import csv
import asyncio
import datetime
import contextlib
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402
from controller.heater_service import HeaterService              # noqa: E402
from lib.heater_config import HeaterConfig, load_heater_config   # noqa: E402
from lib.heater_logger import COLUMNS                            # noqa: E402
from test_heater_service import FakePLC, FakeChat                # noqa: E402

VALUE_COLS = [c for c in COLUMNS if c not in ("timestamp", "note")]


class _Wall:
    """단조 시계와 벽시계를 함께 흘린다."""

    def __init__(self, start: datetime.datetime):
        self.t = 1000.0
        self.dt = start

    def __call__(self) -> float:
        return self.t

    def now(self) -> datetime.datetime:
        return self.dt

    def advance(self, s: float) -> None:
        self.t += s
        self.dt += datetime.timedelta(seconds=s)


def _mk(tmp_path, start=datetime.datetime(2026, 11, 10, 10, 0, 0), **over):
    plc = FakePLC()
    chat = FakeChat()
    clk = _Wall(start)
    logs = []
    cfg = HeaterConfig(**{"enabled": True, "log_enabled": True, "log_dir": str(tmp_path),
                          "log_period_s": 1.0, "poll_interval_s": 1.0, "poll_idle_s": 3.0, **over})
    svc = HeaterService(plc, cfg, log=lambda s, m: logs.append(m), chat=chat, clock=clk)
    svc.csv._wall = clk.now
    return svc, plc, clk, logs


def _poll(svc, clk, dt=None):
    """폴링 1회 후 시계를 서비스가 정한 다음 폴링 주기(또는 dt)만큼 흘린다."""
    asyncio.run(svc._poll_once())
    clk.advance(svc._next_interval() if dt is None else dt)


def _files(tmp_path):
    return sorted(p for p in Path(tmp_path).iterdir() if p.name.startswith("CH2_HEATER_"))


def _rows(path):
    with open(path, encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _lines(path):
    with open(path, encoding="utf-8-sig") as fp:
        return fp.read().splitlines()


def _ts(r):
    return datetime.datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S")


def _gaps(rows):
    return [(_ts(b) - _ts(a)).total_seconds() for a, b in zip(rows, rows[1:])]


# ───────────────────────── a) 주기·파일 이름 ─────────────────────────
def test_idle_3s_run_1s_daily_filename(tmp_path):
    svc, plc, clk, logs = _mk(tmp_path)
    for _ in range(10):                                    # 정지(3초 폴링) 30초
        _poll(svc, clk)
    plc.bits[640] = True
    for _ in range(10):                                    # 운전(1초 폴링) 10초
        _poll(svc, clk)
    files = _files(tmp_path)
    assert [f.name for f in files] == ["CH2_HEATER_20261110.csv"]
    rows = _rows(files[0])
    idle = [r for r in rows if r["run"] == "0"]
    run = [r for r in rows if r["run"] == "1"]
    assert len(idle) == 10 and set(_gaps(idle)) == {3.0}
    assert len(run) == 10 and set(_gaps(run)) == {1.0}
    assert all(r["elapsed_sec"] == "" and r["owner"] == "" for r in idle)
    assert all(r["elapsed_sec"] != "" and r["owner"] != "" for r in run)
    assert svc._next_interval() == 1.0                     # 폴링 주기 자체는 그대로(운전 1초)


# ───────────────────────── b) 자정 ─────────────────────────
def test_midnight_splits_into_two_files_each_with_header(tmp_path):
    svc, plc, clk, logs = _mk(tmp_path, start=datetime.datetime(2026, 11, 10, 23, 59, 59))
    _poll(svc, clk, 2.0)                                   # 23:59:59
    _poll(svc, clk, 3.0)                                   # 00:00:01
    _poll(svc, clk, 3.0)                                   # 00:00:04
    files = _files(tmp_path)
    assert [f.name for f in files] == ["CH2_HEATER_20261110.csv", "CH2_HEATER_20261111.csv"]
    for f, n in zip(files, (1, 2)):
        lines = _lines(f)
        assert lines[0] == ",".join(COLUMNS) and lines.count(lines[0]) == 1
        assert len(lines) == 1 + n
    assert _rows(files[1])[0]["timestamp"] == "2026-11-11 00:00:01"


# ───────────────────────── c) 같은 날 재시작 → 이어 쓰기 ─────────────────────────
def test_restart_same_day_appends_without_duplicate_header(tmp_path):
    svc, plc, clk, logs = _mk(tmp_path)
    _poll(svc, clk)
    _poll(svc, clk)
    asyncio.run(svc.shutdown())                            # "프로그램 종료" 행 + 닫기
    svc2, plc2, clk2, _ = _mk(tmp_path, start=datetime.datetime(2026, 11, 10, 12, 0, 0))
    _poll(svc2, clk2)
    _poll(svc2, clk2)
    files = _files(tmp_path)
    assert len(files) == 1
    lines = _lines(files[0])
    assert lines.count(",".join(COLUMNS)) == 1 and lines[0] == ",".join(COLUMNS)
    rows = _rows(files[0])
    assert [r["note"] for r in rows] == ["", "", "프로그램 종료", "", ""]
    assert rows[3]["timestamp"].startswith("2026-11-10 12:00")


# ───────────────────────── d) RUN 엣지 ─────────────────────────
def test_run_edges_single_rows_and_no_per_run_files(tmp_path):
    svc, plc, clk, logs = _mk(tmp_path)
    _poll(svc, clk)
    _poll(svc, clk)
    plc.bits[640] = True
    for _ in range(4):
        _poll(svc, clk)
    plc.bits[640] = False
    for _ in range(3):
        _poll(svc, clk)
    files = _files(tmp_path)
    assert [f.name for f in files] == ["CH2_HEATER_20261110.csv"]  # 운전별 파일 없음
    rows = _rows(files[0])
    notes = [r["note"] for r in rows]
    assert notes.count("운전 시작") == 1 and notes.count("운전 종료") == 1
    i0, i1 = notes.index("운전 시작"), notes.index("운전 종료")
    assert rows[i0]["run"] == "1" and rows[i0]["owner"] == "외부" and rows[i0]["elapsed_sec"] == "0.0"
    assert rows[i1]["run"] == "0" and rows[i1]["elapsed_sec"] == "4.0" and rows[i1]["owner"] == "외부"
    assert rows[i1 + 1]["note"] == "" and rows[i1 + 1]["owner"] == "" and rows[i1 + 1]["elapsed_sec"] == ""
    # 각 폴링에 정확히 1행(시각이 모두 다르다)
    stamps = [r["timestamp"] for r in rows]
    assert len(stamps) == len(set(stamps)) == 2 + 4 + 3
    assert svc.owner is None
    assert any("CSV " in m and "CH2_HEATER_20261110.csv" in m for m in logs)   # 운전 시작 로그에 하루 파일 경로


def test_run_edge_rows_written_even_if_period_not_due(tmp_path):
    """상승·하강 행은 주기와 상관없이 기록(기록 주기를 길게 둬도 빠지지 않는다)."""
    svc, plc, clk, logs = _mk(tmp_path, log_period_s=10.0, poll_idle_s=10.0)
    _poll(svc, clk, 1.0)
    plc.bits[640] = True
    _poll(svc, clk, 1.0)
    plc.bits[640] = False
    _poll(svc, clk, 1.0)
    rows = _rows(_files(tmp_path)[0])
    assert [r["note"] for r in rows] == ["", "운전 시작", "운전 종료"]


# ───────────────────────── e) PLC 미연결 ─────────────────────────
def test_disconnected_rows_blank_values_and_period_follows_last_run(tmp_path):
    svc, plc, clk, logs = _mk(tmp_path)
    # 프로그램 시작 직후 아직 연결 전(quiet) — 3초마다 "PLC 미연결"
    plc.connected = False
    for _ in range(3):
        _poll(svc, clk)
    plc.connected = True
    _poll(svc, clk)
    plc.bits[640] = True
    for _ in range(3):
        _poll(svc, clk)
    # 운전 중 끊김 — 1초 간격으로 "PLC 미연결", 값 열 전부 빈칸(옛 값 반복 없음)
    plc.connected = False
    for _ in range(5):
        _poll(svc, clk)
    rows = _rows(_files(tmp_path)[0])
    first3 = rows[:3]
    assert all(r["note"] == "PLC 미연결" for r in first3) and set(_gaps(first3)) == {3.0}
    assert all(all(r[c] == "" for c in VALUE_COLS) for r in first3)
    tail = rows[-5:]
    assert all(r["note"] == "PLC 미연결" for r in tail) and set(_gaps(tail)) == {1.0}
    assert all(all(r[c] == "" for c in VALUE_COLS) for r in tail)
    assert rows[-6]["run"] == "1" and rows[-6]["pv_c"] != ""           # 끊기기 직전 행은 실제 값


def test_idle_disconnect_3s_and_read_failure_note(tmp_path):
    svc, plc, clk, logs = _mk(tmp_path)
    for _ in range(2):
        _poll(svc, clk)
    plc.connected = False                                   # 정지 중 끊김 → 3초
    for _ in range(4):
        _poll(svc, clk)
    plc.connected = True

    async def boom(*a, **k):
        raise TimeoutError("timeout")
    plc.read_regs_block = boom                              # 링크는 있는데 읽기 실패
    for _ in range(2):
        _poll(svc, clk)
    rows = _rows(_files(tmp_path)[0])
    mid = rows[2:6]
    assert all(r["note"] == "PLC 미연결" for r in mid) and set(_gaps(mid)) == {3.0}
    assert all(r["note"] == "PLC 미연결(읽기 실패)" for r in rows[6:8])
    assert all(all(r[c] == "" for c in VALUE_COLS) for r in rows[2:8])


# ───────────────────────── f) 폴링 간격 흔들림 ─────────────────────────
def test_jittered_polling_never_skips_a_row(tmp_path):
    svc, plc, clk, logs = _mk(tmp_path)
    jit = [0.9, 1.1, 1.0, 0.9, 1.1, 1.1, 0.9]
    n_idle = 12
    for i in range(n_idle):                                 # 정지: 2.9~3.1초
        _poll(svc, clk, 2.0 + jit[i % len(jit)])
    plc.bits[640] = True
    n_run = 15
    for i in range(n_run):                                  # 운전: 0.9~1.1초
        _poll(svc, clk, jit[i % len(jit)])
    rows = _rows(_files(tmp_path)[0])
    assert len(rows) == n_idle + n_run                      # 매 폴링 1행, 빠짐·중복 없음(timestamp 는 초 단위라 겹칠 수 있다)
    assert sum(r["run"] == "1" for r in rows) == n_run


def test_idle_with_1s_polling_logs_about_every_3s(tmp_path):
    """정지 중인데 다른 이유로 1초 폴링(설정 쓰기 재시도) — 대략 3초마다 1행."""
    svc, plc, clk, logs = _mk(tmp_path)
    _poll(svc, clk)
    svc._pending_off = True                                 # 보류 OFF 가 남아 있으면 busy → 1초 폴링
    plc.fail_writes = True                                  # 계속 실패해 보류가 유지된다
    for _ in range(30):
        assert svc._next_interval() == 1.0
        _poll(svc, clk)
    rows = _rows(_files(tmp_path)[0])
    assert 10 <= len(rows) <= 12 and all(g == 3.0 for g in _gaps(rows))


# ───────────────────────── g) 끔 ─────────────────────────
def test_log_disabled_creates_no_file(tmp_path):
    svc, plc, clk, logs = _mk(tmp_path, log_enabled=False)
    for _ in range(3):
        _poll(svc, clk)
    plc.bits[640] = True
    for _ in range(3):
        _poll(svc, clk)
    plc.connected = False
    _poll(svc, clk)
    asyncio.run(svc.shutdown())
    assert _files(tmp_path) == []


# ───────────────────────── 종료 행·기록 실패 ─────────────────────────
def test_shutdown_writes_one_row_and_closes(tmp_path):
    svc, plc, clk, logs = _mk(tmp_path)
    _poll(svc, clk)
    asyncio.run(svc.shutdown())
    rows = _rows(_files(tmp_path)[0])
    assert [r["note"] for r in rows] == ["", "프로그램 종료"] and rows[-1]["pv_c"] != ""
    assert not svc.csv.is_open()


def test_shutdown_before_any_read_leaves_blank_row(tmp_path):
    svc, plc, clk, logs = _mk(tmp_path)
    plc.connected = False
    _poll(svc, clk)
    asyncio.run(svc.shutdown())
    rows = _rows(_files(tmp_path)[0])
    assert [r["note"] for r in rows] == ["PLC 미연결", "프로그램 종료"]
    assert all(rows[-1][c] == "" for c in VALUE_COLS)


def _last_row_blank(tmp_path):
    rows = _rows(_files(tmp_path)[0])
    last = rows[-1]
    assert last["note"] == "프로그램 종료" and sum(r["note"] == "프로그램 종료" for r in rows) == 1
    return all(last[c] == "" for c in VALUE_COLS), last


def test_shutdown_after_disconnected_poll_blank_values(tmp_path):
    """a) 마지막 폴링이 PLC 미연결 — 읽은 적은 있어도 옛 값을 찍지 않는다."""
    svc, plc, clk, logs = _mk(tmp_path)
    _poll(svc, clk)
    _poll(svc, clk)
    plc.connected = False
    _poll(svc, clk)
    asyncio.run(svc.shutdown())
    blank, _ = _last_row_blank(tmp_path)
    assert blank and not svc.csv.is_open()


def test_shutdown_after_read_failure_blank_values(tmp_path):
    """b) 마지막 폴링이 읽기 실패(링크는 있음)."""
    svc, plc, clk, logs = _mk(tmp_path)
    _poll(svc, clk)

    async def boom(*a, **k):
        raise TimeoutError("timeout")
    plc.read_regs_block = boom
    _poll(svc, clk)
    rows = _rows(_files(tmp_path)[0])
    assert rows[-1]["note"] == "PLC 미연결(읽기 실패)"
    asyncio.run(svc.shutdown())
    blank, _ = _last_row_blank(tmp_path)
    assert blank


def test_shutdown_link_lost_after_good_poll_blank_values(tmp_path):
    """c) 정상 읽기 직후 링크만 끊긴 상태(아직 실패 폴링 없음)."""
    svc, plc, clk, logs = _mk(tmp_path)
    _poll(svc, clk)
    _poll(svc, clk)
    plc.connected = False
    asyncio.run(svc.shutdown())
    blank, _ = _last_row_blank(tmp_path)
    assert blank


def test_shutdown_while_running_fills_owner_and_elapsed(tmp_path):
    """d) 운전 중 정상 읽기 직후 shutdown — 값·owner·elapsed_sec 채움(RUN OFF 쓰기 순서도 그대로)."""
    svc, plc, clk, logs = _mk(tmp_path)
    _poll(svc, clk)
    plc.bits[640] = True
    for _ in range(4):
        _poll(svc, clk)
    asyncio.run(svc.shutdown())
    blank, last = _last_row_blank(tmp_path)
    assert not blank
    assert last["owner"] == "외부" and last["elapsed_sec"] == "4.0" and last["pv_c"] != "" and last["run"] == "1"
    assert ("wc", 640, False) in plc.writes()


def test_shutdown_while_idle_fills_values_not_owner(tmp_path):
    """e) 정지 중 정상 읽기 직후 shutdown — 값은 채우고 owner·elapsed 빈칸."""
    svc, plc, clk, logs = _mk(tmp_path)
    _poll(svc, clk)
    _poll(svc, clk)
    asyncio.run(svc.shutdown())
    blank, last = _last_row_blank(tmp_path)
    assert not blank
    assert last["owner"] == "" and last["elapsed_sec"] == "" and last["pv_c"] != "" and last["run"] == "0"


def test_write_failure_warns_once_and_keeps_polling(tmp_path):
    svc, plc, clk, logs = _mk(tmp_path)
    _poll(svc, clk)
    svc.csv._app.append_row = lambda **k: (_ for _ in ()).throw(OSError("disk"))
    for _ in range(3):
        _poll(svc, clk)
    assert sum("히터 CSV 기록 실패" in m for m in logs) == 1
    assert svc.status()["run"] is False                      # 폴링은 계속된다


# ───────────────────────── h) settings.json 실제 로드 ─────────────────────────
def test_repo_settings_json_heater_values_and_strict_check():
    from lib import _config_loader as L
    from lib import config_common, config_ch1, config_ch2
    mods = (config_common, config_ch1, config_ch2)
    snap = [dict(vars(md)) for md in mods]
    try:
        res = L.check_and_load_settings(Path(_ROOT) / "config" / "settings.json")
        assert not res.problems, L.format_problems(res)
        cfg, warns = load_heater_config(config_ch2)
        assert warns == []
        assert cfg.log_enabled is True and cfg.log_period_s == 1.0
        assert cfg.poll_interval_s == 1.0 and cfg.poll_idle_s == 3.0 and cfg.stale_sec == 10.0
    finally:
        for md, d in zip(mods, snap):
            for k in [k for k in vars(md) if k not in d]:
                delattr(md, k)
            for k, v in d.items():
                setattr(md, k, v)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

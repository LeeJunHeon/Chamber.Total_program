"""기록 (장비 공통) — 제어 흐름을 막지 않는다.

- put_row / put_event 는 큐에 넣기만 한다(가득 차면 버리고 dropped 를 센다 — tick 은 절대 기다리지 않음).
- 쓰기 스레드: 일별 SQLite(WAL) snap_YYYYMMDD.sqlite 의 snapshot·events 표에 쓴다. 1초마다 commit.
  날이 바뀌면 지난 날을 CSV(snap_YYYYMMDD.csv, utf-8-sig)로 내보내고 보관 기간이 지난 로컬 파일을 지운다 — 따로 스레드에서
  (recorder-csv). 쓰기 스레드가 내보내기를 기다리면 하루치(8만 행)를 내보내는 동안 새 행이 큐에 쌓인다(5-7 오래 켜 두기에서 찾음 —
  가상 시계 10 시간 시험에서 자정 뒤 쓰기가 멈추고 큐가 시간당 수천 행씩 늘었다).
  D319(사용자 결정 2026-11-02): 하루치 CSV(약 130 MB) 내보내기는 공정·운전(CH1·CH2·플라즈마 클리닝·TSP — busy_fn)이 없을 때만, 낮은
  우선순위(스레드 우선순위를 낮춤)로 EXPORT_CHUNK_ROWS 행씩 나눠 쓰고 조각 사이에 EXPORT_PAUSE_S 쉰다(tick 이 CPU 를 기다리지 않게).
  공정 중이면 시작도 미루고(조각 사이에 공정이 시작돼도 멈춤), 끝난 뒤 멈춘 자리에서 이어서 한다. 프로그램이 꺼졌다 켜지면 CSV 가
  없는 지난 날 DB 를 start() 가 다시 찾아 내보낸다(.tmp 는 처음부터).
- NAS 사본 스레드: nas_period_s 마다 지난 날 파일(.sqlite·.csv)을 복사하고, nas_today_period_s 마다 오늘 DB 를 SQLite backup 으로
  만든 일관된 사본(staging)을 복사한다. 실패하면 다음 회차에 다시. 복사가 멈춰도 쓰기·tick 과 무관.
  NAS 쪽은 지우지 않는다(사본 전체 보관).
"""
from __future__ import annotations

import collections
import csv
import datetime as _dt
import json
import os
import queue
import shutil
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

PREFIX = "snap_"
# 설정으로 두지 않는 상수(이유)
COMMIT_EVERY_S = 1.0     # SQLite commit 간격 — 정전 때 잃는 양(≤1 s)과 디스크 쓰기 횟수의 균형. 운영자가 바꿀 일 없음
MAX_QUEUE = 20000        # 기록 큐 — 1초 1행 기준 5시간 이상 디스크가 멈춰도 버리지 않는 양(메모리 수십 MB 이하)
ERRORS_KEEP = 200        # 오류 목록은 최근 이만큼만(무한히 늘지 않게)
EXPORT_CHUNK_ROWS = 500  # CSV 내보내기 한 조각의 행 수 — 한 조각이 GIL 을 쥐는 시간을 수십 ms 안으로(1초 tick 에 지연을 주지 않게, D319)
EXPORT_PAUSE_S = 0.05    # 조각 사이 쉬는 시간 — tick·기록 스레드에 CPU 를 양보
EXPORT_BUSY_POLL_S = 1.0  # 공정·운전 중이라 미룰 때 끝났는지 보는 간격


class ExportStopped(Exception):
    """프로그램이 끝나는 중이라 CSV 내보내기를 멈춤(.tmp 는 남고 다음 시작 때 처음부터)."""


def _lower_thread_priority() -> None:
    """이 스레드의 우선순위를 낮춘다(할 수 있는 OS 에서만 — 못 해도 조용히 지나감: 조각 쉼이 주된 양보)."""
    try:
        if os.name == "nt":
            import ctypes
            k = ctypes.WinDLL("kernel32")
            k.GetCurrentThread.restype = ctypes.c_void_p
            k.SetThreadPriority(ctypes.c_void_p(k.GetCurrentThread()), -2)       # THREAD_PRIORITY_LOWEST
        elif hasattr(os, "setpriority") and hasattr(threading, "get_native_id") and os.uname().sysname == "Linux":
            os.setpriority(os.PRIO_PROCESS, threading.get_native_id(), 10)       # 리눅스는 스레드마다 nice 를 가진다
    except Exception:
        pass


def _day_of(ts_iso: str) -> str:
    return ts_iso[:10].replace("-", "")


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class Recorder:
    def __init__(self, directory: Path, columns: Sequence[str], retention_days: int = 180,
                 max_queue: int = MAX_QUEUE, commit_every_s: float = COMMIT_EVERY_S,
                 nas_dir: Optional[Path] = None, nas_period_s: float = 60.0, nas_today_period_s: float = 600.0,
                 copy_fn: Callable[[str, str], Any] = shutil.copy2,
                 today_fn: Callable[[], _dt.date] = _dt.date.today,
                 wall_fn: Callable[[], float] = time.time,
                 busy_fn: Optional[Callable[[], bool]] = None,
                 export_chunk_rows: int = EXPORT_CHUNK_ROWS, export_pause_s: float = EXPORT_PAUSE_S,
                 export_busy_poll_s: float = EXPORT_BUSY_POLL_S) -> None:
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.columns = list(columns)
        if self.columns[0] != "ts":
            raise ValueError("첫 열은 ts")
        self.retention_days = retention_days
        self.commit_every_s = commit_every_s
        self.nas_dir = Path(nas_dir) if nas_dir else None
        self.nas_period_s = nas_period_s
        self.nas_today_period_s = nas_today_period_s
        self._last_today_copy: Optional[float] = None
        self.copy_fn = copy_fn
        self.today_fn = today_fn
        self.wall_fn = wall_fn
        self.busy_fn = busy_fn or (lambda: False)         # 공정·운전 중이면 True(CSV 내보내기를 미룬다 — D319)
        self.export_chunk_rows = max(1, int(export_chunk_rows))
        self.export_pause_s = max(0.0, float(export_pause_s))
        self.export_busy_poll_s = max(0.01, float(export_busy_poll_s))
        self._deferred = threading.Event()                # 공정·운전 중이라 내보내기가 기다리는 중
        self._bg = threading.local()                       # 내보내기 스레드 안에서만 '끝나는 중이면 멈춤'을 따른다(직접 부르는 export_csv 는 끝까지)
        self.export_deferrals = 0                         # 미룬 횟수(진단·시험)
        self.exported_days: "collections.deque[str]" = collections.deque(maxlen=ERRORS_KEEP)
        self.blackbox_files: "collections.deque[Path]" = collections.deque(maxlen=100)
        self._q: "queue.Queue" = queue.Queue(maxsize=max_queue)
        self.dropped = 0
        self.written = 0
        self.nas_copied = 0
        self.nas_failures = 0
        self.errors: "collections.deque[str]" = collections.deque(maxlen=ERRORS_KEEP)
        self._db: Optional[sqlite3.Connection] = None
        self._day: Optional[str] = None
        self._stop = threading.Event()
        self._writer: Optional[threading.Thread] = None
        self._nas: Optional[threading.Thread] = None
        self._db_lock = threading.Lock()   # 오늘 DB 경로·열기 상태 보호(NAS 스레드가 backup 할 때)
        self._exports: List[threading.Thread] = []

    # ───────── 넣기(제어 쪽 — 기다리지 않음) ─────────
    def put_row(self, row: Dict[str, Any]) -> None:
        try:
            self._q.put_nowait(("row", row))
        except queue.Full:
            self.dropped += 1

    def put_blackbox(self, name: str, rows: List[Dict[str, Any]]) -> None:
        """사고 때 원시 값 링버퍼를 blackbox/<name>_<YYYYmmdd_HHMMSS>.csv 로(쓰기 스레드에서)."""
        try:
            self._q.put_nowait(("blackbox", (name, self.wall_fn(), [dict(r) for r in rows])))
        except queue.Full:
            self.dropped += 1

    def put_event(self, ts: str, level: str, code: str, source: str, message: str,
                  context: Optional[dict] = None) -> None:
        try:
            self._q.put_nowait(("event", (ts, level, code, source, message,
                                          json.dumps(context or {}, ensure_ascii=False))))
        except queue.Full:
            self.dropped += 1

    # ───────── 생명주기 ─────────
    def start(self) -> None:
        self.prune()
        for day in self._undone_days():                    # 꺼지기 전에 못 끝낸 지난 날 CSV 를 이어서
            self._spawn_export(day)
        self._writer = threading.Thread(target=self._write_loop, name="recorder", daemon=True)
        self._writer.start()
        if self.nas_dir is not None:
            self._nas = threading.Thread(target=self._nas_loop, name="nas-copy", daemon=True)
            self._nas.start()

    def flush(self, timeout_s: float = 10.0) -> bool:
        """큐가 다 쓰이고 지난 날 CSV 내보내기가 끝날 때까지(시험·종료용)."""
        t0 = time.monotonic()
        while (self._q.unfinished_tasks and self._writer is not None and self._writer.is_alive()   # 시작 전(쓰기 스레드 없음)·
               and time.monotonic() - t0 < timeout_s):                                              # 멈춘 뒤면 기다리지 않음
            time.sleep(0.005)
        for t in list(self._exports):                      # 공정·운전 중이라 미뤄진 내보내기는 기다리지 않는다(끝난 뒤에 이어서)
            while t.is_alive() and not self.export_deferred() and time.monotonic() - t0 < timeout_s:
                t.join(0.02)
        self._exports = [t for t in self._exports if t.is_alive()]
        return not self._q.unfinished_tasks and not self._exports

    def export_deferred(self) -> bool:
        """지난 날 CSV 내보내기가 공정·운전 중이라 기다리는 중인가(시험·소크가 '끝날 때까지 기다리기'에서 뺄 때)."""
        return self._deferred.is_set() and bool(self.busy_fn())     # 공정이 끝났으면 곧 이어서 하므로 더는 미뤄진 것이 아니다

    def stop(self, timeout_s: float = 10.0) -> None:
        self.flush(timeout_s)
        self._stop.set()
        if self._writer:
            self._writer.join(timeout_s)
        # NAS 스레드는 데몬 — 복사가 멈춰 있어도 기다리지 않는다

    # ───────── 쓰기 스레드 ─────────
    def db_path(self, day: str) -> Path:
        return self.dir / f"{PREFIX}{day}.sqlite"

    def csv_path(self, day: str) -> Path:
        return self.dir / f"{PREFIX}{day}.csv"

    def _open_day(self, day: str) -> None:
        if self._day == day:
            return
        old = self._day
        with self._db_lock:
            if self._db is not None:
                self._db.commit()
                self._db.close()
            db = sqlite3.connect(self.db_path(day), check_same_thread=False)
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=NORMAL")
            cols = ", ".join(_q(c) + (" TEXT PRIMARY KEY" if c == "ts" else "") for c in self.columns)
            db.execute(f"CREATE TABLE IF NOT EXISTS snapshot ({cols})")
            have = {r[1] for r in db.execute("PRAGMA table_info(snapshot)")}
            for c in self.columns:
                if c not in have:          # 열이 늘어난 버전으로 같은 날 다시 시작
                    db.execute(f"ALTER TABLE snapshot ADD COLUMN {_q(c)}")
            db.execute("CREATE TABLE IF NOT EXISTS events (ts TEXT, level TEXT, code TEXT, source TEXT, "
                       "message TEXT, context TEXT)")
            db.commit()
            self._db, self._day = db, day
        if old is not None:                # 지난 날 CSV·정리는 따로 스레드(쓰기는 새 날 DB 로 계속)
            self._spawn_export(old)

    def _spawn_export(self, day: str) -> None:
        self._exports = [t for t in self._exports if t.is_alive()]
        t = threading.Thread(target=self._export_old, args=(day,), name="recorder-csv", daemon=True)
        t.start()
        self._exports.append(t)

    def _undone_days(self) -> List[str]:
        """오늘 이전 날 DB 는 있는데 CSV 가 없는 날(꺼지기 전에 내보내기를 못 끝냄)."""
        today = self.today_fn().strftime("%Y%m%d")
        out = []
        for p in sorted(self.dir.glob(f"{PREFIX}*.sqlite")):
            d = p.name[len(PREFIX):len(PREFIX) + 8]
            if len(d) == 8 and d.isdigit() and d < today and not self.csv_path(d).exists():
                out.append(d)
        return out

    def _export_old(self, day: str) -> None:
        _lower_thread_priority()                           # 낮은 우선순위(D319)
        self._bg.on = True
        try:
            self.export_csv(day)
        except ExportStopped:
            return                                         # 끝나는 중 — 다음 시작 때 처음부터
        except Exception as e:
            self.errors.append(f"CSV 내보내기 {day}: {e!r}")
        self.prune()

    def _write_loop(self) -> None:
        sql_row = (f"INSERT OR REPLACE INTO snapshot ({', '.join(_q(c) for c in self.columns)}) "
                   f"VALUES ({', '.join('?' for _ in self.columns)})")
        last_commit = time.monotonic()
        while not (self._stop.is_set() and self._q.empty()):
            try:
                kind, item = self._q.get(timeout=0.2)
            except queue.Empty:
                kind, item = None, None
            try:
                if kind == "row":
                    self._open_day(_day_of(item["ts"]))
                    self._db.execute(sql_row, [_cell(item.get(c)) for c in self.columns])
                    self.written += 1
                elif kind == "blackbox":
                    self._write_blackbox(*item)
                elif kind == "event":
                    self._open_day(_day_of(item[0]))
                    self._db.execute("INSERT INTO events VALUES (?,?,?,?,?,?)", item)
                if self._db is not None and (time.monotonic() - last_commit >= self.commit_every_s
                                             or self._q.empty()):
                    self._db.commit()
                    last_commit = time.monotonic()
            except Exception as e:
                self.errors.append(repr(e))
            finally:
                if kind is not None:
                    self._q.task_done()
        with self._db_lock:
            if self._db is not None:
                self._db.commit()
                self._db.close()
                self._db = None

    def _write_blackbox(self, name: str, t_wall: float, rows: List[Dict[str, Any]]) -> None:
        d = self.dir / "blackbox"
        d.mkdir(exist_ok=True)
        stamp = _dt.datetime.fromtimestamp(t_wall).strftime("%Y%m%d_%H%M%S")
        out = d / f"{name}_{stamp}.csv"
        cols: List[str] = []
        for r in rows:
            for k in r:
                if k not in cols:
                    cols.append(k)
        with open(out, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for r in rows:
                w.writerow(["" if r.get(c) is None else _cell(r.get(c)) for c in cols])
        self.blackbox_files.append(out)

    # ───────── CSV·보관 ─────────
    def _wait_idle(self) -> None:
        """공정·운전(busy_fn)이 없을 때까지 기다린다 — 끝나는 중이면 ExportStopped. 기다린 만큼 export_deferrals 에 센다."""
        first = True
        while self.busy_fn():
            if first:
                self.export_deferrals += 1
                first = False
            self._deferred.set()
            if self._stop.wait(self.export_busy_poll_s) and getattr(self._bg, "on", False):
                self._deferred.clear()
                raise ExportStopped()
            if self._stop.is_set() and not getattr(self._bg, "on", False):
                time.sleep(self.export_busy_poll_s)                       # 직접 부른 내보내기는 끝까지(끝나는 중에도) — 공정 중이면 계속 기다림
        self._deferred.clear()
        if self._stop.is_set() and getattr(self._bg, "on", False):
            raise ExportStopped()

    def export_csv(self, day: str) -> Path:
        """하루치 CSV — EXPORT_CHUNK_ROWS 행씩(ts 순서, ts 는 기본 키라 색인으로 이어 읽는다). 조각마다 공정·운전 중이 아닌지 보고
        (맞으면 끝날 때까지 기다린 뒤 멈춘 자리에서 이어서), 조각 사이에 쉰다. 다 쓰면 .tmp 를 바꿔치기."""
        cols = ", ".join(_q(c) for c in self.columns)
        out = self.csv_path(day)
        tmp = out.with_name(out.name + ".tmp")
        n = self.export_chunk_rows
        self._wait_idle()                                  # 공정·운전 중이면 시작도 미룬다
        src = sqlite3.connect(self.db_path(day))
        try:
            last = None
            with open(tmp, "w", encoding="utf-8-sig", newline="") as f:
                w = csv.writer(f)
                w.writerow(self.columns)
                while True:
                    if last is None:
                        rows = src.execute(f"SELECT {cols} FROM snapshot ORDER BY ts LIMIT ?", (n,)).fetchall()
                    else:
                        rows = src.execute(f"SELECT {cols} FROM snapshot WHERE ts > ? ORDER BY ts LIMIT ?", (last, n)).fetchall()
                    for r in rows:
                        w.writerow(["" if v is None else v for v in r])
                    if len(rows) < n:
                        break
                    f.flush()                                                 # 미뤄진 동안 .tmp 에 지금까지가 보이게
                    last = rows[-1][0]
                    if self.export_pause_s:
                        if self._stop.wait(self.export_pause_s) and getattr(self._bg, "on", False):    # 조각 사이 양보(끝나는 중이면 바로 깨어남)
                            raise ExportStopped()
                    self._wait_idle()                                      # 다음 조각 전에 공정·운전이 시작됐으면 끝날 때까지 기다림
            os.replace(tmp, out)
            self.exported_days.append(day)
            return out
        finally:
            src.close()

    def prune(self) -> List[Path]:
        cutoff = self.today_fn() - _dt.timedelta(days=self.retention_days)
        removed = []
        for p in self.dir.glob(f"{PREFIX}*"):
            d = p.name[len(PREFIX):len(PREFIX) + 8]
            try:
                day = _dt.datetime.strptime(d, "%Y%m%d").date()
            except ValueError:
                continue
            if day < cutoff and d != self._day:
                try:
                    p.unlink()
                    removed.append(p)
                except OSError as e:
                    self.errors.append(f"정리 {p.name}: {e!r}")
        return removed

    # ───────── NAS 사본 ─────────
    def nas_round(self, copy_today: bool = True) -> None:
        """한 회차: 지난 날 파일 복사(크기가 다르면). copy_today 면 오늘 DB 의 backup 사본도 복사."""
        assert self.nas_dir is not None
        today = self._day
        for p in sorted(self.dir.glob(f"{PREFIX}*")):
            if p.suffix not in (".sqlite", ".csv") or p.name.endswith(".tmp"):
                continue
            day = p.name[len(PREFIX):len(PREFIX) + 8]
            if day == today:
                continue
            dst = self.nas_dir / p.name
            try:
                if dst.exists() and dst.stat().st_size == p.stat().st_size:
                    continue
                self._copy(p, dst)
            except Exception as e:
                self.nas_failures += 1
                self.errors.append(f"NAS {p.name}: {e!r}")
        if today is not None and copy_today:
            staging = self.dir / "staging"
            staging.mkdir(exist_ok=True)
            snap = staging / f"{PREFIX}{today}.sqlite"
            try:
                src = sqlite3.connect(self.db_path(today))
                dst_db = sqlite3.connect(snap)
                try:
                    src.backup(dst_db)
                finally:
                    dst_db.close()
                    src.close()
                self._copy(snap, self.nas_dir / snap.name)
            except Exception as e:
                self.nas_failures += 1
                self.errors.append(f"NAS 오늘 사본: {e!r}")

    def _copy(self, src: Path, dst: Path) -> None:
        tmp = dst.with_name(dst.name + ".part")
        self.copy_fn(str(src), str(tmp))
        os.replace(tmp, dst)
        self.nas_copied += 1

    def _nas_loop(self) -> None:
        while not self._stop.wait(self.nas_period_s):
            try:
                self.nas_dir.mkdir(parents=True, exist_ok=True)
                now = time.monotonic()
                today = self._last_today_copy is None or now - self._last_today_copy >= self.nas_today_period_s
                self.nas_round(copy_today=today)
                if today:
                    self._last_today_copy = now
            except Exception as e:
                self.nas_failures += 1
                self.errors.append(f"NAS 회차: {e!r}")


def _cell(v: Any) -> Any:
    if isinstance(v, bool):
        return int(v)
    if v is None or isinstance(v, (int, float, str)):
        return v
    return str(v)

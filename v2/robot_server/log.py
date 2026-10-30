"""로봇 서버 기록 — 서버 로그(server_page_YYYYMMDD.log)와 명령 CSV(remote_cmd_YYYYMMDD.csv).

서버 로그(v1 runtime/server_page.py 의 '전부 저장' 경로 + main.py _netlog)
- 줄 형식: '{YYYY-mm-dd HH:MM:SS} [{TAG}] {본문}'. 본문이 이미 'YYYY-mm-dd HH:MM:SS ' 로 시작하면(PLC_REMOTE 줄) 그 시각을
  앞으로 뺀다(server_page.py:237-248). 태그는 대문자로(main.py:658-662).
- 파일: <LOG_ROOT_DIR>/CH1&2_Server/server_page_YYYYMMDD.log, 그 폴더를 못 만들면 LOCAL_FALLBACK_SERVER_DIR
  (server_page.py:355-388). utf-8 추가 쓰기, 핸들 유지, 날짜가 바뀌면 새 파일, 1초마다 백그라운드 스레드가 flush
  (server_page.py:96-99·410-463). 버퍼 하드 상한 50000줄 — 넘으면 오래된 절반을 버리고 1회 기록(server_page.py:390-408).
- v1 화면(서버 페이지)의 표시·숨김·접속 목록은 v2 노트북 화면/포털 몫 — 여기서는 콘솔 출력(ROBOT_LOG_ECHO)만.

명령 CSV(v1 host/server.py:42-131 DailyCommandCsvLogger + util/log_hub.py:184-305 DailyCsvDictAppender)
- <LOG_ROOT_DIR>/CH1&2_Server/remote_cmd_YYYYMMDD.csv 우선, 쓰기 실패 시 LOCAL_FALLBACK_SERVER_DIR 로 1회 전환,
  10 s 마다 우선 경로 재시도, utf-8-sig, 핸들 유지, 빈 파일이면 헤더. 전환이 처음 일어난 쓰기 뒤 '__NAS_MISSING__' 마커 1줄.
- 기록 실패는 모두 무시(응답·연결에 영향 없음).
"""
from __future__ import annotations

import asyncio
import csv
import re
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

_DT_PREFIX_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+(?P<rest>.*)$")   # v1 과 같게 DOTALL 없음
SERVER_DIR_NAME = "CH1&2_Server"


def default_fallback_dir() -> Path:
    """v1 LOCAL_FALLBACK_SERVER_DIR = <exe 폴더>/Logs_LocalFallback/Server — v2 소스 실행은 저장소 루트 기준."""
    if getattr(sys, "frozen", False):
        base = Path(sys.executable).resolve().parent
    else:
        base = Path(__file__).resolve().parents[2]
    return base / "Logs_LocalFallback" / "Server"


def format_line(tag: str, msg: str, now: Callable[[], datetime] = datetime.now) -> str:
    """server_page._format_with_ts 와 같은 줄."""
    msg = str(msg)
    m = _DT_PREFIX_RE.match(msg)
    if m:
        ts, rest = m.group("ts"), m.group("rest")
    else:
        ts, rest = now().strftime("%Y-%m-%d %H:%M:%S"), msg
    return f"{ts} [{tag}] {rest}"


class ServerLog:
    """log(tag, text) — v1 HostServer 에 넘기던 log 콜백과 같은 모양."""

    HARD_MAX = 50000
    FLUSH_EVERY_S = 1.0

    def __init__(self, log_root: Optional[Path], fallback_dir: Optional[Path] = None, *, echo: bool = False,
                 now: Callable[[], datetime] = datetime.now,
                 on_line: Optional[Callable[[str, str, str], None]] = None) -> None:
        self._root = Path(log_root) if log_root else None
        self._fallback = Path(fallback_dir) if fallback_dir else default_fallback_dir()
        self._echo = echo
        self._now = now
        self._on_line = on_line
        self._buf: List[str] = []
        self._lock = threading.Lock()
        self._fp = None
        self._fp_path: Optional[Path] = None
        self._dir_ready: Optional[Path] = None
        self._overflow_logged = False
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ── 쓰기 ──
    def __call__(self, tag: str, text: str) -> None:
        self.log(tag, text)

    def log(self, tag: str, text: str) -> None:
        try:
            t = str(tag or "")
            tu = t.upper() or t
            msg = str(text)
            line = format_line(tu, msg, self._now)
            if self._on_line is not None:
                try:
                    self._on_line(tu, msg, line)
                except Exception:
                    pass
            if self._echo:
                try:
                    print(line, flush=True)
                except Exception:
                    pass
            with self._lock:
                self._buf.append(line)
                if len(self._buf) > self.HARD_MAX:
                    n_drop = len(self._buf) // 2
                    del self._buf[:n_drop]
                    if not self._overflow_logged:
                        self._overflow_logged = True
                        self._buf.append(format_line("SERVER", f"server_page 일별 로그 버퍼 상한 초과 — {n_drop}줄 폐기",
                                                     self._now))
        except Exception:
            pass

    # ── flush 스레드 ──
    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="RobotServerLogIO", daemon=True)
            self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self.FLUSH_EVERY_S):
            self.flush()

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        self.flush()
        if self._fp is not None:
            try:
                self._fp.close()
            except Exception:
                pass
            self._fp = None
            self._fp_path = None

    def _dir(self) -> Path:
        if self._root is not None:
            d = self._root / SERVER_DIR_NAME
            if self._dir_ready == d:
                return d
            try:
                d.mkdir(parents=True, exist_ok=True)
                self._dir_ready = d
                return d
            except Exception:
                pass
        d = self._fallback
        if self._dir_ready != d:
            d.mkdir(parents=True, exist_ok=True)
            self._dir_ready = d
        return d

    def path_today(self) -> Path:
        return self._dir() / f"server_page_{self._now():%Y%m%d}.log"

    def flush(self) -> None:
        with self._lock:
            lines, self._buf = self._buf, []
        if not lines:
            return
        try:
            p = self.path_today()
            if self._fp is None or self._fp_path != p:
                if self._fp is not None:
                    try:
                        self._fp.close()
                    except Exception:
                        pass
                p.parent.mkdir(parents=True, exist_ok=True)
                self._fp = open(p, "a", encoding="utf-8")
                self._fp_path = p
            self._fp.write("\n".join(lines) + "\n")
            self._fp.flush()
            self._overflow_logged = False
        except Exception:
            # v1: 핸들 오류면 리셋하고 다음 주기에 다시 연다(그 줄들은 v1 처럼 잃는다)
            try:
                if self._fp is not None:
                    self._fp.close()
            except Exception:
                pass
            self._fp = None
            self._fp_path = None


CSV_HEADER = ["server_time", "peer", "request_id", "req_command", "req_data_json", "res_command", "res_result",
              "res_message", "res_data_json", "duration_ms"]


class _DailyCsvDictAppender:
    """util/log_hub.DailyCsvDictAppender 와 같은 동작(우선/대체 폴더·10 s 재시도·핸들 유지·빈 파일이면 헤더)."""

    def __init__(self, primary_dir: Path, fallback_dir: Path, filename_builder: Callable[[datetime], str],
                 fieldnames: Sequence[str], encoding: str = "utf-8-sig", retry_primary_every_s: float = 10.0) -> None:
        self._primary, self._fallback = Path(primary_dir), Path(fallback_dir)
        self._fname = filename_builder
        self._fields = list(fieldnames)
        self._enc = encoding
        self._retry_s = float(retry_primary_every_s)
        self._lock = threading.RLock()
        self._fp = None
        self._writer: Optional[csv.DictWriter] = None
        self._cur: Optional[Path] = None
        self.using_fallback = False
        self.switched_once = False
        self.last_error: Optional[BaseException] = None
        self._last_try = 0.0

    @property
    def current_path(self) -> Optional[Path]:
        return self._cur

    def consume_switched_flag(self) -> bool:
        with self._lock:
            f, self.switched_once = self.switched_once, False
            return f

    def close(self) -> None:
        with self._lock:
            fp, self._fp, self._writer, self._cur = self._fp, None, None, None
            try:
                if fp is not None:
                    fp.flush()
                    fp.close()
            except Exception:
                pass

    def _path(self, dt: datetime, fallback: bool) -> Path:
        return (self._fallback if fallback else self._primary) / self._fname(dt)

    def _open(self, path: Path) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        try:
            need_header = (not path.exists()) or path.stat().st_size == 0
        except Exception:
            need_header = True
        self._fp = open(path, "a", encoding=self._enc, newline="")
        self._writer = csv.DictWriter(self._fp, fieldnames=self._fields)
        self._cur = path
        if need_header:
            self._writer.writeheader()
            self._fp.flush()

    def _ensure_open(self, dt: datetime) -> None:
        want = self._path(dt, self.using_fallback)
        if self._fp is None or self._writer is None or self._cur != want:
            self.close()
            self._open(want)

    def _maybe_retry_primary(self, dt: datetime) -> None:
        if not self.using_fallback or self._retry_s <= 0:
            return
        now = time.monotonic()
        if now - self._last_try < self._retry_s:
            return
        self._last_try = now
        try:
            self.close()
            self._open(self._path(dt, False))
            self.using_fallback = False
            self.last_error = None
        except Exception as e:
            self.last_error = e
            self.using_fallback = True
            self.close()
            self._open(self._path(dt, True))

    def append_row(self, *, dt: datetime, row: Dict[str, Any]) -> None:
        with self._lock:
            self._maybe_retry_primary(dt)
            try:
                self._ensure_open(dt)
                self._writer.writerow({k: row.get(k, "") for k in self._fields})
                self._fp.flush()
            except Exception as e:
                self.last_error = e
                if not self.using_fallback:
                    self.using_fallback = True
                    self.switched_once = True
                    self.close()
                    self._open(self._path(dt, True))
                    self._writer.writerow({k: row.get(k, "") for k in self._fields})
                    self._fp.flush()
                else:
                    raise


class CommandCsv:
    """remote_cmd_YYYYMMDD.csv (v1 host/server.py DailyCommandCsvLogger)."""

    def __init__(self, log_root: Optional[Path], fallback_dir: Optional[Path] = None,
                 now: Callable[[], datetime] = datetime.now) -> None:
        primary = (Path(log_root) if log_root else default_fallback_dir().parent) / SERVER_DIR_NAME
        self._now = now
        self._lock: Optional[asyncio.Lock] = None
        self._writer = _DailyCsvDictAppender(primary, Path(fallback_dir) if fallback_dir else default_fallback_dir(),
                                             lambda dt: f"remote_cmd_{dt:%Y%m%d}.csv", CSV_HEADER)

    @property
    def current_path(self) -> Optional[Path]:
        return self._writer.current_path

    def _ensure_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def append(self, row: dict) -> None:
        lock = self._ensure_lock()
        dt = self._now()
        async with lock:
            try:
                await asyncio.to_thread(self._writer.append_row, dt=dt, row=row)
                if self._writer.consume_switched_flag():
                    msg = f"NAS write failed -> switched to LOCAL (keep-handle). reason={self._writer.last_error!r}"
                    if len(msg) > 500:
                        msg = msg[:500] + "...(truncated)"
                    marker = {
                        "server_time": dt.strftime("%Y-%m-%d %H:%M:%S"), "peer": str(row.get("peer", "")),
                        "request_id": str(row.get("request_id", "")), "req_command": "__NAS_MISSING__",
                        "req_data_json": "", "res_command": "", "res_result": "WARN", "res_message": msg,
                        "res_data_json": "", "duration_ms": str(row.get("duration_ms", "")),
                    }
                    await asyncio.to_thread(self._writer.append_row, dt=dt, row=marker)
            except Exception:
                pass

    async def aclose(self) -> None:
        async with self._ensure_lock():
            try:
                await asyncio.to_thread(self._writer.close)
            except Exception:
                pass

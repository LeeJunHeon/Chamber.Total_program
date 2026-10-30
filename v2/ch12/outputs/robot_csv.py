"""Robot_YYYYMMDD.csv — ALD 프로그램과 같이 쓰는 하루 단위 공유 CSV(v1 util/host_process_log.py 규약 v3/v3.1 그대로).

규약(ALD 와 공통 — 바꾸지 않는다, ROB-111~137)
- NAS <HOST_LOG_NAS_DIR>\\Robot_YYYYMMDD.csv(공용·append 전용), 잠금 <NAS>\\_lock\\Robot.lock.
  로컬 <HOST_LOG_LOCAL_DIR>\\Robot_YYYYMMDD_sputter.csv(내 줄 사본 15칸)·pending_sputter.csv(NAS 미전송, 요청날짜+15칸)·
  open_sputter.json(미종료 요청, indent=1·ensure_ascii=False, .tmp→fsync→os.replace).
- 15열 HEADER, 프로그램 'sputter', 결과 '성공'·'실패'·'STOP'·'거절'·'중단(재시작)'·'미확인'. 파일 날짜 = 요청 수신 날짜.
- 줄: csv.writer RFC4180, 줄 끝 CRLF. 시각 HH:MM:SS(없으면 빈칸), 시작 안 한 요청은 종료·소요 빈칸, 소요 (종료-시작)/60
  소수 1자리, 사유는 성공이면 빈칸 아니면 CR/LF → ' | ' 한 줄, 로그파일 ' / ' 연결, 마지막 열 기록키.
- 기록키 'sputter-YYYYMMDDHHMMSS{ms3}-{request_id 정제}'([A-Za-z0-9_-] 외 '_', 64자). open·pending·직전 발급 키와 겹치면 -2, -3 …
- 끊긴 줄 보정: 이어쓰기 전에 파일 끝이 '\\n' 이 아니면 '\\r\\n' 먼저(없는·빈·BOM 만 있는 파일은 그대로).
- 새 파일(NAS·사본)은 utf-8-sig(BOM)+머리글, 있으면 utf-8 로 이어 씀.
- NAS 한 줄 보내기: 폴더 준비 → 잠금 파일 O_CREAT|O_EXCL|O_WRONLY('sputter {pid} {YYYY-mm-dd HH:MM:SS}'), 이미 있으면 mtime 이
  HOST_LOG_LOCK_STALE_S(120 s — 설정 읽기 예외면 30, v1 이상 동작 그대로) 넘었으면 지우고 바로 다시, 아니면 0.2 s, 최대 25회.
  NAS 파일에 줄 끝이 ','+기록키 인 줄이 있으면 이미 기록된 것(성공). 잠금은 늘 close+unlink.
- pending 을 앞에서부터 한 줄씩, 실패하면 그 회차 중단(순서 유지), 보낸 만큼 앞부분을 지우고 다시 씀. 메모리 큐 상한 10000줄.
- startup_recover(시작 때, 로컬만): open 중 pending 에 이미 있는 키는 open 에서만 지우고, 나머지는 '중단(재시작)'
  ('프로그램 재시작 (공정 중)'/'(시작 전)', 종료·소요 빈칸) 줄을 pending·사본에.
- 'started'(mark_started 첫 회)·'finished'(finalize·reject·startup_recover, 기록키당 1회·최근 512개) 이벤트 → sink.
- HOST_LOG_ENABLED=False 면 모든 공개 API no-op(request 는 ''). 공개 API 는 예외를 내지 않는다. 디버그는 SYSTEM 로그 'HOSTLOG'.

v2 구조(호출이 tick·제어를 막지 않게)
- 공개 API 는 메모리(open·pending 키 거울)만 바꾸고 파일 쓰기는 쓰기 스레드(QueueWriter)에, NAS 보내기는 보내기 스레드
  (v1 HostProcessLogWorker 와 같음, HOST_LOG_RETRY_S)에서 한다. 키 중복 검사는 메모리 거울로(같은 파일을 이 프로그램만 씀 —
  ALD 는 NAS 파일만 같이 씀). finalize 는 pending 에 실제로 쓴 뒤에 open 에서 지운다(v1 순서 그대로).

부르는 법(로봇 명령 처리·공정 엔진)
  r = RobotCsv(cfg, writer=w, system_log=sl); r.startup_recover()
  key = r.request(target="CH1", request_id=.., peer=.., received_at=dt, recipe_name=.., row_count=.., process_names=..)
  r.update(key, recipe_name=.., row_count=.., process_names=.., log_files=[..]); r.mark_owned(key); r.is_owned(key)
  r.mark_started(key, started_at=dt, log_file=name); r.finalize(key, "성공"|.., reason, finished_at=dt); r.reject(key, 사유)
  r.set_event_sink(fn(kind, info)); r.close()
"""
from __future__ import annotations

import csv
import io
import json
import os
import re
import threading
import time
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional

from v2.ch12.outputs.base import QueueWriter, wall_now
from v2.ch12.params import outputs as P

PROGRAM_NAME = "sputter"
HEADER: List[str] = [
    "대상", "요청시각", "시작시각", "종료시각", "소요(분)", "결과", "사유",
    "공정명", "레시피", "행개수", "request_id", "peer", "로그파일", "프로그램", "기록키",
]
N_COLS = len(HEADER)
LOCK_DIR_NAME = "_lock"
LOCK_FILE_NAME = "Robot.lock"
RESULT_SUCCESS, RESULT_FAIL, RESULT_STOP = "성공", "실패", "STOP"
RESULT_REJECT, RESULT_RESTART, RESULT_UNKNOWN = "거절", "중단(재시작)", "미확인"
MEM_QUEUE_MAX = 10000
_KEY_SAFE_RE = re.compile(r"[^A-Za-z0-9_-]")
_MAX_KEY_ID_LEN = 64


def sanitize_request_id(rid: Any) -> str:
    return _KEY_SAFE_RE.sub("_", str(rid or ""))[:_MAX_KEY_ID_LEN]


def _hms(dt: Optional[datetime]) -> str:
    return dt.strftime("%H:%M:%S") if isinstance(dt, datetime) else ""


def one_line(s: Any) -> str:
    t = str(s or "").replace("\r\n", "\n").replace("\r", "\n")
    return " | ".join(x.strip() for x in t.split("\n") if x.strip())


def _iso(dt: Optional[datetime]) -> str:
    return dt.isoformat(timespec="milliseconds") if isinstance(dt, datetime) else ""


def _parse_iso(s: Any) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(str(s)) if s else None
    except Exception:
        return None


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding=encoding, newline="") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def csv_line(fields: List[str]) -> str:
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\r\n").writerow(fields)
    return buf.getvalue()


def build_row(it: Mapping[str, Any], *, result: str, reason: str, started: Optional[datetime],
              finished: Optional[datetime]) -> List[str]:
    rx = _parse_iso(it.get("received_at"))
    if not isinstance(started, datetime):
        finished = None
    dur = ""
    if isinstance(started, datetime) and isinstance(finished, datetime):
        dur = f"{(finished - started).total_seconds() / 60.0:.1f}"
    lf = it.get("log_files") or []
    return [
        str(it.get("target", "")), _hms(rx), _hms(started), _hms(finished), dur, str(result or ""),
        "" if result == RESULT_SUCCESS else one_line(reason),
        str(it.get("process_names", "")), str(it.get("recipe_name", "")), str(it.get("row_count", "")),
        str(it.get("request_id", "")), str(it.get("peer", "")), " / ".join(str(x) for x in lf),
        PROGRAM_NAME, str(it.get("key", "")),
    ]


class RobotCsv:
    def __init__(self, cfg: Optional[Mapping[str, Any]] = None, *, writer: Optional[QueueWriter] = None,
                 system_log: Any = None, now: Callable[[], datetime] = wall_now,
                 time_fn: Callable[[], float] = time.time, sleep_fn: Callable[[float], None] = time.sleep,
                 pid: Optional[int] = None, start_worker: bool = True) -> None:
        self.cfg = cfg if cfg is not None else {}
        self._own_writer = writer is None
        self.writer = writer if writer is not None else QueueWriter("robot-csv")
        self._syslog = system_log
        self.now, self.time_fn, self.sleep_fn = now, time_fn, sleep_fn
        self.pid = os.getpid() if pid is None else int(pid)
        self._lk = threading.RLock()
        self._flush_lk = threading.Lock()
        self._owned: set = set()
        self._last_key = ""
        self._finalizing: set = set()
        self._mem_queue: List[str] = []
        self._mem_queue_dropped = 0
        self._mem_queue_drop_warned = False
        self._event_sink: Optional[Callable[[str, Dict[str, Any]], None]] = None
        self._final_emitted: "OrderedDict[str, None]" = OrderedDict()
        self._kick = threading.Event()
        self._stop = threading.Event()
        self._worker: Optional[threading.Thread] = None
        self._start_worker = bool(start_worker)
        self._open: Dict[str, dict] = self._load_open() if self.enabled() else {}
        self._pend_keys: set = {r[-1] for r in self._read_pending() if r} if self.enabled() else set()

    # ── 설정(호출 시점에 읽음) ──
    def _cfg(self, key: str):
        return P.get(self.cfg, key)

    def enabled(self) -> bool:
        try:
            return bool(self._cfg("HOST_LOG_ENABLED"))
        except Exception:
            return True

    def _nas_dir(self) -> Path:
        return Path(str(self._cfg("HOST_LOG_NAS_DIR")))

    def _local_dir(self) -> Path:
        return Path(str(self._cfg("HOST_LOG_LOCAL_DIR")))

    def _retry_s(self) -> float:
        try:
            return max(1.0, float(self._cfg("HOST_LOG_RETRY_S")))
        except Exception:
            return 30.0

    def _lock_stale_s(self) -> float:
        try:
            return max(1.0, float(self._cfg("HOST_LOG_LOCK_STALE_S")))
        except Exception:
            return 30.0          # v1 이상 동작 그대로(기본 120 과 다름 — util/host_process_log.py:189-196)

    def _p_open(self) -> Path:
        return self._local_dir() / f"open_{PROGRAM_NAME}.json"

    def _p_pending(self) -> Path:
        return self._local_dir() / f"pending_{PROGRAM_NAME}.csv"

    def _p_mine(self, ymd: str) -> Path:
        return self._local_dir() / f"Robot_{ymd}_{PROGRAM_NAME}.csv"

    def _p_nas(self, ymd: str) -> Path:
        return self._nas_dir() / f"Robot_{ymd}.csv"

    def _dbg(self, msg: str) -> None:
        try:
            if self._syslog is not None:
                self._syslog.append("HOSTLOG", msg)
        except Exception:
            pass

    # ── 파일(쓰기·보내기 스레드) ──
    def _ensure_line_end(self, path: Path) -> None:
        try:
            p = Path(path)
            if not p.exists():
                return
            size = p.stat().st_size
            if size <= 0:
                return
            with open(p, "rb") as f:
                if size <= 3 and f.read() == b"\xef\xbb\xbf"[:size]:
                    return
                f.seek(size - 1)
                last = f.read(1)
            if last != b"\n":
                with open(p, "ab") as f:
                    f.write(b"\r\n")
                    f.flush()
                self._dbg(f"끊긴 줄 보정: {p.name}")
        except Exception as e:
            self._dbg(f"끊긴 줄 보정 실패({path}): {e!r}")

    def _load_open(self) -> Dict[str, dict]:
        try:
            p = self._p_open()
            if not p.exists():
                return {}
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
            return d if isinstance(d, dict) else {}
        except Exception as e:
            self._dbg(f"open 로드 실패: {e!r}")
            return {}

    def _save_open(self) -> None:
        try:
            with self._lk:
                text = json.dumps(self._open, ensure_ascii=False, indent=1)
            self._local_dir().mkdir(parents=True, exist_ok=True)
            atomic_write_text(self._p_open(), text)
        except Exception as e:
            self._dbg(f"open 저장 실패: {e!r}")

    def _append_pending_line(self, line: str) -> bool:
        try:
            self._local_dir().mkdir(parents=True, exist_ok=True)
            self._ensure_line_end(self._p_pending())
            with open(self._p_pending(), "a", encoding="utf-8", newline="") as f:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
            return True
        except Exception as e:
            self._dbg(f"pending append 실패: {e!r}")
            return False

    def _append_mine(self, ymd: str, row: List[str]) -> None:
        p = self._p_mine(ymd)
        self._local_dir().mkdir(parents=True, exist_ok=True)
        new = not p.exists()
        if not new:
            self._ensure_line_end(p)
        with open(p, "a", encoding="utf-8-sig" if new else "utf-8", newline="") as f:
            if new:
                f.write(csv_line(HEADER))
            f.write(csv_line(row))
            f.flush()

    def _read_pending(self) -> List[List[str]]:
        try:
            p = self._p_pending()
            if not p.exists():
                return []
            with open(p, "r", encoding="utf-8", newline="") as f:
                return [r for r in csv.reader(f) if r and len(r) >= N_COLS + 1]
        except Exception as e:
            self._dbg(f"pending 읽기 실패: {e!r}")
            return []

    def _write_pending(self, rows: List[List[str]]) -> None:
        try:
            self._local_dir().mkdir(parents=True, exist_ok=True)
            atomic_write_text(self._p_pending(), "".join(csv_line(r) for r in rows))
        except Exception as e:
            self._dbg(f"pending 쓰기 실패: {e!r}")

    # ── 이벤트 ──
    def set_event_sink(self, fn: Optional[Callable[[str, Dict[str, Any]], None]]) -> None:
        with self._lk:
            self._event_sink = fn

    @staticmethod
    def _event_info(it: Mapping[str, Any], **extra) -> Dict[str, Any]:
        info: Dict[str, Any] = {
            "key": str(it.get("key", "") or ""), "target": str(it.get("target", "") or ""),
            "request_id": str(it.get("request_id", "") or ""), "peer": str(it.get("peer", "") or ""),
            "received_at": _parse_iso(it.get("received_at")), "started_at": _parse_iso(it.get("started_at")),
            "recipe_name": str(it.get("recipe_name", "") or ""), "row_count": str(it.get("row_count", "") or ""),
            "process_names": str(it.get("process_names", "") or ""),
            "log_files": [str(x) for x in (it.get("log_files") or [])],
        }
        info.update(extra)
        return info

    def _final_event_once(self, it, *, result, reason, finished_at) -> Optional[Dict[str, Any]]:
        if self._event_sink is None:
            return None
        key = str(it.get("key", "") or "")
        if not key or key in self._final_emitted:
            return None
        self._final_emitted[key] = None
        while len(self._final_emitted) > 512:
            self._final_emitted.popitem(last=False)
        return self._event_info(it, result=str(result or ""), reason=one_line(reason),
                                finished_at=finished_at if isinstance(finished_at, datetime) else None)

    def _emit(self, kind: str, info: Optional[Dict[str, Any]]) -> None:
        fn = self._event_sink
        if fn is None or not info:
            return
        try:
            fn(kind, info)
        except Exception as e:
            self._dbg(f"이벤트 통지 실패({kind}): {e!r}")

    # ── 공개 API(메모리만 + 큐) ──
    def request(self, *, target: str, request_id: Any, peer: str, received_at: Optional[datetime] = None,
                recipe_name: str = "", row_count: Any = "", process_names: str = "") -> str:
        if not self.enabled():
            return ""
        try:
            with self._lk:
                rx = received_at if isinstance(received_at, datetime) else self.now()
                base = (f"{PROGRAM_NAME}-{rx.strftime('%Y%m%d%H%M%S')}{rx.microsecond // 1000:03d}-"
                        f"{sanitize_request_id(request_id)}")
                key, n = base, 1
                while (key in self._open) or (key in self._pend_keys) or (key == self._last_key):
                    n += 1
                    key = f"{base}-{n}"
                self._last_key = key
                self._open[key] = {
                    "key": key, "target": str(target or ""), "request_id": str(request_id or ""),
                    "peer": str(peer or ""), "received_at": _iso(rx), "started_at": "",
                    "recipe_name": str(recipe_name or ""),
                    "row_count": "" if row_count in (None, "") else str(row_count),
                    "process_names": str(process_names or ""), "log_files": [],
                }
            self.writer.put(self._save_open)
            return key
        except Exception as e:
            self._dbg(f"request 실패: {e!r}")
            return ""

    def update(self, key: str, **fields) -> None:
        if not self.enabled() or not key:
            return
        try:
            with self._lk:
                it = self._open.get(key)
                if not it:
                    return
                for k in ("recipe_name", "row_count", "process_names", "log_files"):
                    if k in fields and fields[k] not in (None,):
                        v = fields[k]
                        it[k] = list(v) if k == "log_files" else ("" if v == "" else str(v))
            self.writer.put(self._save_open)
        except Exception as e:
            self._dbg(f"update 실패: {e!r}")

    def mark_started(self, key: str, started_at: Optional[datetime] = None, log_file: Optional[str] = None) -> None:
        if not self.enabled() or not key:
            return
        ev = None
        try:
            with self._lk:
                it = self._open.get(key)
                if not it:
                    return
                first = False
                if not it.get("started_at"):
                    it["started_at"] = _iso(started_at if isinstance(started_at, datetime) else self.now())
                    first = True
                if log_file:
                    lf = list(it.get("log_files") or [])
                    if log_file not in lf:
                        lf.append(str(log_file))
                    it["log_files"] = lf
                if first and self._event_sink is not None:
                    ev = self._event_info(it)
            self.writer.put(self._save_open)
        except Exception as e:
            self._dbg(f"mark_started 실패: {e!r}")
        self._emit("started", ev)

    def mark_owned(self, key: str) -> None:
        if key:
            with self._lk:
                self._owned.add(key)

    def is_owned(self, key: str) -> bool:
        if not key:
            return False
        with self._lk:
            return key in self._owned

    def finalize(self, key: str, result: str, reason: str = "", finished_at: Optional[datetime] = None) -> bool:
        """종료 줄 1개(멱등 — open 에 없거나 이미 마감 중이면 False)."""
        if not self.enabled() or not key:
            return False
        ev = None
        try:
            with self._lk:
                it = self._open.get(key)
                if not it or key in self._finalizing:
                    return False
                rx = _parse_iso(it.get("received_at"))
                st = _parse_iso(it.get("started_at"))
                fin = finished_at if isinstance(finished_at, datetime) else self.now()
                ymd = (rx or fin).strftime("%Y%m%d")
                row = build_row(it, result=result, reason=reason, started=st, finished=fin)
                ev = self._final_event_once(it, result=result, reason=reason, finished_at=fin)
                self._finalizing.add(key)
            self.writer.put(self._finalize_write, key, ymd, row)
            return True
        except Exception as e:
            self._dbg(f"finalize 실패: {e!r}")
            return False
        finally:
            self._emit("finished", ev)

    def reject(self, key: str, reason: str) -> bool:
        return self.finalize(key, RESULT_REJECT, reason)

    def _finalize_write(self, key: str, ymd: str, row: List[str]) -> None:
        with self._lk:
            self._finalizing.discard(key)
            if not self._append_pending_line(csv_line([ymd] + row)):
                self._mem_queue.append(csv_line([ymd] + row))
                self._trim_mem_queue()
                self._dbg(f"pending 기록 실패 → 메모리 큐 보관 key={key}")
                self._kick.set()
                return
            self._pend_keys.add(row[-1])
            try:
                self._append_mine(ymd, row)
            except Exception:
                pass
            self._open.pop(key, None)
            self._owned.discard(key)
        self._save_open()
        self._ensure_worker()
        self._kick.set()

    def startup_recover(self) -> None:
        """시작 때(쓰기 스레드에서): pending 에 이미 있는 open 은 버리고, 나머지는 '중단(재시작)' 줄.
        대상은 부른 순간 open 에 있던 것(지난 실행이 남긴 것)만 — 쓰기 스레드가 늦게 돌 때(부하·앞선 기록) 그 사이 새로 받은 요청을
        '재시작' 으로 닫지 않게(v1 은 시작 때 바로 처리)."""
        if not self.enabled():
            return
        with self._lk:
            keys = set(self._open)
        self.writer.put(self._recover, keys)

    def _recover(self, keys: Optional[set] = None) -> None:
        evs: List[Dict[str, Any]] = []
        try:
            with self._lk:
                pend = {r[-1] for r in self._read_pending() if r}
                self._pend_keys |= pend
                for key, it in list(self._open.items()):
                    if keys is not None and key not in keys:
                        continue                 # 시작 뒤 새로 받은 요청 — 지난 실행 것이 아님
                    if key in pend:
                        self._open.pop(key, None)
                        continue
                    st = _parse_iso(it.get("started_at"))
                    reason = "프로그램 재시작 (공정 중)" if st else "프로그램 재시작 (시작 전)"
                    rx = _parse_iso(it.get("received_at"))
                    ymd = (rx or self.now()).strftime("%Y%m%d")
                    row = build_row(it, result=RESULT_RESTART, reason=reason, started=st, finished=None)
                    if self._append_pending_line(csv_line([ymd] + row)):
                        self._pend_keys.add(row[-1])
                        try:
                            self._append_mine(ymd, row)
                        except Exception:
                            pass
                        self._open.pop(key, None)
                        ev = self._final_event_once(it, result=RESULT_RESTART, reason=reason, finished_at=None)
                        if ev is not None:
                            evs.append(ev)
            self._save_open()
        except Exception as e:
            self._dbg(f"startup_recover 실패: {e!r}")
        for ev in evs:
            self._emit("finished", ev)
        self._ensure_worker()
        self._kick.set()

    # ── 보내기 스레드(v1 HostProcessLogWorker) ──
    def _ensure_worker(self) -> None:
        if not self.enabled() or not self._start_worker:
            return
        with self._lk:
            if self._worker is not None and self._worker.is_alive():
                return
            self._stop.clear()
            self._worker = threading.Thread(target=self._worker_loop, name="HostProcessLogWorker", daemon=True)
            self._worker.start()

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.flush_once()
            except Exception as e:
                self._dbg(f"워커 예외(무시): {e!r}")
            self._kick.wait(timeout=self._retry_s())
            self._kick.clear()

    def _trim_mem_queue(self) -> None:
        over = len(self._mem_queue) - MEM_QUEUE_MAX
        if over > 0:
            del self._mem_queue[:over]
            self._mem_queue_dropped += over
            if not self._mem_queue_drop_warned:
                self._mem_queue_drop_warned = True
                self._dbg(f"WARN 메모리 큐 상한({MEM_QUEUE_MAX}줄) 초과 — 가장 오래된 {over}줄 폐기 "
                          f"(파일 기록 실패가 계속되고 있음)")

    def flush_once(self) -> None:
        with self._flush_lk:
            self._flush_once_locked()

    def _flush_once_locked(self) -> None:
        with self._lk:
            if self._mem_queue:
                for ln in list(self._mem_queue):
                    if not self._append_pending_line(ln):
                        break
                    self._mem_queue.remove(ln)
                    try:
                        k = next(csv.reader(io.StringIO(ln)))[-1]
                        self._pend_keys.add(k)
                        if self._open.pop(k, None) is not None:
                            self._save_open()
                        self._owned.discard(k)
                    except Exception:
                        pass
            rows = self._read_pending()
        if not rows:
            return
        remaining = list(rows)
        for r in rows:
            if not self._send_one(r[0], r[1:1 + N_COLS]):
                break
            remaining.pop(0)
        if len(remaining) != len(rows):
            with self._lk:
                now_rows = self._read_pending()
                n_sent = len(rows) - len(remaining)
                self._write_pending(now_rows[n_sent:])
                self._pend_keys = {r[-1] for r in now_rows[n_sent:] if r}

    def _send_one(self, ymd: str, row: List[str]) -> bool:
        nas = self._nas_dir()
        lock_path = nas / LOCK_DIR_NAME / LOCK_FILE_NAME
        fd = None
        try:
            nas.mkdir(parents=True, exist_ok=True)
            lock_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self._dbg(f"NAS 폴더 준비 실패: {e!r}")
            return False
        acquired = False
        for _attempt in range(25):
            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"{PROGRAM_NAME} {self.pid} {self.now():%Y-%m-%d %H:%M:%S}".encode("utf-8"))
                acquired = True
                break
            except FileExistsError:
                try:
                    age = self.time_fn() - lock_path.stat().st_mtime
                    if age > self._lock_stale_s():
                        lock_path.unlink(missing_ok=True)
                        self._dbg(f"오래된 잠금 제거(age={age:.0f}s)")
                        continue
                except Exception:
                    pass
                self.sleep_fn(0.2)
            except Exception as e:
                self._dbg(f"잠금 오류: {e!r}")
                return False
        if not acquired:
            return False
        try:
            target = self._p_nas(ymd)
            key = row[-1]
            try:
                if target.exists():
                    with open(target, "r", encoding="utf-8-sig", newline="") as f:
                        for ln in f:
                            if ln.rstrip("\r\n").endswith("," + key):
                                return True
            except Exception as e:
                self._dbg(f"NAS 읽기 실패: {e!r}")
                return False
            try:
                new = not target.exists()
                if not new:
                    self._ensure_line_end(target)
                with open(target, "a", encoding="utf-8-sig" if new else "utf-8", newline="") as f:
                    if new:
                        f.write(csv_line(HEADER))
                    f.write(csv_line(row))
                    f.flush()
                return True
            except Exception as e:
                self._dbg(f"NAS 쓰기 실패: {e!r}")
                return False
        finally:
            try:
                if fd is not None:
                    os.close(fd)
            except Exception:
                pass
            try:
                lock_path.unlink(missing_ok=True)
            except Exception:
                pass

    def flush(self, timeout_s: float = 10.0) -> bool:
        return self.writer.flush(timeout_s)

    def close(self, timeout_s: float = 2.0) -> None:
        self.writer.flush(timeout_s)
        self._stop.set()
        self._kick.set()
        t = self._worker
        if t is not None and t.is_alive():
            t.join(timeout=max(0.0, float(timeout_s)))
        if self._own_writer:
            self.writer.stop(timeout_s)

# util/host_process_log.py
# -*- coding: utf-8 -*-
"""
호스트(로봇/스케줄러) 요청 공정의 하루 단위 공유 CSV 로그 — 공통 사양 v3 / v3.1

- 요청 1건 = CSV 1줄. NAS 공유 파일에 append 하며, 같은 파일을 다른 PC 의 ALD(C#)
  프로그램도 함께 쓴다. 규약(컬럼/잠금/인코딩)은 양쪽이 동일해야 하므로 바꾸지 말 것.
- UI 버튼 시작 / UI 파일 시작 / Pre-Sputter 예약은 기록하지 않는다(origin 으로 구분).
- 모든 공개 API 는 동기이며 예외를 밖으로 내보내지 않는다. 공정/응답을 지연시키지 않도록
  NAS 전송은 데몬 워커 스레드가 담당하고, 호출자는 로컬 pending 기록까지만 기다린다.

파일 배치
  NAS   : <HOST_LOG_NAS_DIR>/Robot_YYYYMMDD.csv      (공용, append 전용)
          <HOST_LOG_NAS_DIR>/_lock/Robot.lock        (잠금)
  로컬  : <HOST_LOG_LOCAL_DIR>/Robot_YYYYMMDD_sputter.csv   (내 줄 사본, 15칸)
          <HOST_LOG_LOCAL_DIR>/pending_sputter.csv          (NAS 미전송, 16칸)
          <HOST_LOG_LOCAL_DIR>/open_sputter.json            (미종료 요청)

파일 날짜는 '요청 수신 날짜'다(자정을 넘겨 끝나도 요청 날짜 파일에 기록).
"""
from __future__ import annotations

import csv
import io
import json
import os
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# ─────────────────────────────────────────────────────────────
# 규약 상수 (ALD 와 공통 — 변경 금지)
# ─────────────────────────────────────────────────────────────
PROGRAM_NAME = "sputter"

HEADER: List[str] = [
    "대상", "요청시각", "시작시각", "종료시각", "소요(분)", "결과", "사유",
    "공정명", "레시피", "행개수", "request_id", "peer", "로그파일", "프로그램", "기록키",
]
N_COLS = len(HEADER)          # 15

LOCK_DIR_NAME = "_lock"
LOCK_FILE_NAME = "Robot.lock"

RESULT_SUCCESS = "성공"
RESULT_FAIL = "실패"
RESULT_STOP = "STOP"
RESULT_REJECT = "거절"
RESULT_RESTART = "중단(재시작)"
RESULT_UNKNOWN = "미확인"

_KEY_SAFE_RE = re.compile(r"[^A-Za-z0-9_-]")
_MAX_KEY_ID_LEN = 64


def _sanitize_request_id(rid: Any) -> str:
    """기록키에 쓸 수 있도록 request_id 를 정제한다([A-Za-z0-9_-] 외는 '_', 64자 절단)."""
    s = _KEY_SAFE_RE.sub("_", str(rid or ""))
    return s[:_MAX_KEY_ID_LEN]


def _hms(dt: Optional[datetime]) -> str:
    """CSV 에 쓸 시각 표기. 없으면 빈칸."""
    return dt.strftime("%H:%M:%S") if isinstance(dt, datetime) else ""


def _one_line(s: Any) -> str:
    """사유는 한 줄로 (CR/LF → ' | ')."""
    t = str(s or "")
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    return " | ".join(x.strip() for x in t.split("\n") if x.strip())


def _dbg(msg: str) -> None:
    """내부 디버그 — SYSTEM 로그로만 남긴다(공정 로그를 건드리지 않는다)."""
    try:
        from util.system_log import system_log_append
        system_log_append("HOSTLOG", msg)
    except Exception:
        pass


def _iso(dt: Optional[datetime]) -> str:
    return dt.isoformat(timespec="milliseconds") if isinstance(dt, datetime) else ""


def _parse_iso(s: Any) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(str(s)) if s else None
    except Exception:
        return None


def _atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    """임시파일 → os.replace 로 원자적 교체 (flush + fsync)."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding=encoding, newline="") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _csv_line(fields: List[str]) -> str:
    """RFC4180 한 줄(CRLF 종결)."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\r\n")
    w.writerow(fields)
    return buf.getvalue()


class HostProcessLog:
    """v3.1 규약 구현체. 모듈 싱글턴은 get_host_log() 로 얻는다."""

    def __init__(self) -> None:
        self._lk = threading.RLock()
        self._owned: set[str] = set()          # 러너/컨트롤러가 수락한 기록키
        self._last_key: str = ""               # 직전 발급 키(충돌 접미사 판정용)
        self._kick = threading.Event()
        self._stop = threading.Event()
        self._worker: Optional[threading.Thread] = None
        self._mem_queue: List[str] = []        # pending 파일 기록 실패 시 임시 보관
        self._mem_queue_max: int = 10000       # 상한(줄). 파일 기록이 계속 실패해도 무한 누적하지 않는다
        self._mem_queue_dropped: int = 0
        self._mem_queue_drop_warned: bool = False
        # NAS 전송은 프로세스 안에서 한 번에 하나만 (워커 스레드 ↔ 명시 호출 경쟁 방지)
        self._flush_lk = threading.Lock()

    # ── 설정 (DEC-033: 호출 시점에 읽는다) ───────────────────
    @staticmethod
    def _cfg(key: str, default):
        try:
            from lib import config_common as cfgc
            return getattr(cfgc, key, default)
        except Exception:
            return default

    def enabled(self) -> bool:
        try:
            return bool(self._cfg("HOST_LOG_ENABLED", True))
        except Exception:
            return True

    def _nas_dir(self) -> Path:
        return Path(str(self._cfg("HOST_LOG_NAS_DIR", r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\Robot")))

    def _local_dir(self) -> Path:
        return Path(str(self._cfg("HOST_LOG_LOCAL_DIR", r"C:\VanaM_Logs\Robot")))

    def _retry_s(self) -> float:
        try:
            return max(1.0, float(self._cfg("HOST_LOG_RETRY_S", 30)))
        except Exception:
            return 30.0

    def _lock_stale_s(self) -> float:
        """오래된 잠금 판정 기준(초).
        age = 내 PC 시각 − NAS mtime 이라 시계 오차에 취약하다.
        잠금 보유 시간은 수십 ms 수준이므로 120초로 여유를 둔다."""
        try:
            return max(1.0, float(self._cfg("HOST_LOG_LOCK_STALE_S", 120)))
        except Exception:
            return 30.0

    # ── 로컬 경로 ────────────────────────────────────────────
    def _p_open(self) -> Path:
        return self._local_dir() / f"open_{PROGRAM_NAME}.json"

    def _p_pending(self) -> Path:
        return self._local_dir() / f"pending_{PROGRAM_NAME}.csv"

    def _p_mine(self, ymd: str) -> Path:
        return self._local_dir() / f"Robot_{ymd}_{PROGRAM_NAME}.csv"

    def _p_nas(self, ymd: str) -> Path:
        return self._nas_dir() / f"Robot_{ymd}.csv"

    # ── open_sputter.json ────────────────────────────────────
    def _load_open(self) -> Dict[str, dict]:
        try:
            p = self._p_open()
            if not p.exists():
                return {}
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
            return d if isinstance(d, dict) else {}
        except Exception as e:
            _dbg(f"open 로드 실패: {e!r}")
            return {}

    def _save_open(self, d: Dict[str, dict]) -> None:
        try:
            self._local_dir().mkdir(parents=True, exist_ok=True)
            _atomic_write_text(self._p_open(), json.dumps(d, ensure_ascii=False, indent=1))
        except Exception as e:
            _dbg(f"open 저장 실패: {e!r}")

    # ── 공개 API ─────────────────────────────────────────────
    def request(self, *, target: str, request_id: Any, peer: str,
                received_at: Optional[datetime] = None,
                recipe_name: str = "", row_count: Any = "",
                process_names: str = "") -> str:
        """요청 수신 시점에 기록키를 발급하고 open 에 등록한다. 반환: 기록키."""
        if not self.enabled():
            return ""
        try:
            with self._lk:
                rx = received_at if isinstance(received_at, datetime) else datetime.now()
                base = f"{PROGRAM_NAME}-{rx.strftime('%Y%m%d%H%M%S')}{rx.microsecond // 1000:03d}-{_sanitize_request_id(request_id)}"
                cur = self._load_open()
                pend = self._pending_keys()
                key = base
                n = 1
                while (key in cur) or (key in pend) or (key == self._last_key):
                    n += 1
                    key = f"{base}-{n}"
                self._last_key = key

                cur[key] = {
                    "key": key,
                    "target": str(target or ""),
                    "request_id": str(request_id or ""),
                    "peer": str(peer or ""),
                    "received_at": _iso(rx),
                    "started_at": "",
                    "recipe_name": str(recipe_name or ""),
                    "row_count": "" if row_count in (None, "") else str(row_count),
                    "process_names": str(process_names or ""),
                    "log_files": [],
                }
                self._save_open(cur)
                return key
        except Exception as e:
            _dbg(f"request 실패: {e!r}")
            return ""

    def update(self, key: str, **fields) -> None:
        """open 항목의 recipe_name / row_count / process_names / log_files 갱신."""
        if not self.enabled() or not key:
            return
        try:
            with self._lk:
                cur = self._load_open()
                it = cur.get(key)
                if not it:
                    return
                for k in ("recipe_name", "row_count", "process_names", "log_files"):
                    if k in fields and fields[k] not in (None,):
                        v = fields[k]
                        it[k] = list(v) if k == "log_files" else ("" if v == "" else str(v))
                cur[key] = it
                self._save_open(cur)
        except Exception as e:
            _dbg(f"update 실패: {e!r}")

    def mark_started(self, key: str, started_at: Optional[datetime] = None,
                     log_file: Optional[str] = None) -> None:
        """시작일시(최초 1회만) + 로그파일명 추가."""
        if not self.enabled() or not key:
            return
        try:
            with self._lk:
                cur = self._load_open()
                it = cur.get(key)
                if not it:
                    return
                if not it.get("started_at"):
                    it["started_at"] = _iso(started_at if isinstance(started_at, datetime) else datetime.now())
                if log_file:
                    lf = list(it.get("log_files") or [])
                    if log_file not in lf:
                        lf.append(str(log_file))
                    it["log_files"] = lf
                cur[key] = it
                self._save_open(cur)
        except Exception as e:
            _dbg(f"mark_started 실패: {e!r}")

    def mark_owned(self, key: str) -> None:
        """러너/컨트롤러가 요청을 수락했다는 표시(메모리)."""
        if not key:
            return
        with self._lk:
            self._owned.add(key)

    def is_owned(self, key: str) -> bool:
        if not key:
            return False
        with self._lk:
            return key in self._owned

    def finalize(self, key: str, result: str, reason: str = "",
                 finished_at: Optional[datetime] = None) -> bool:
        """종료 줄 1개를 기록한다. 멱등 — open 에 key 가 없으면 False."""
        if not self.enabled() or not key:
            return False
        try:
            with self._lk:
                cur = self._load_open()
                it = cur.get(key)
                if not it:
                    return False          # 이미 기록됨(멱등)

                rx = _parse_iso(it.get("received_at"))
                st = _parse_iso(it.get("started_at"))
                fin = finished_at if isinstance(finished_at, datetime) else datetime.now()
                ymd = (rx or fin).strftime("%Y%m%d")

                row = self._build_row(it, result=result, reason=reason,
                                      started=st, finished=fin)
                line = _csv_line(row)

                # ① pending 에 먼저 (요청날짜 + 15칸)
                if not self._append_pending(ymd, row):
                    # 로컬 기록 실패 → open 유지, 메모리 큐로 워커에 위임
                    self._mem_queue.append(_csv_line([ymd] + row))
                    self._trim_mem_queue()
                    self._kick.set()
                    _dbg(f"pending 기록 실패 → 메모리 큐 보관 key={key}")
                    return True

                # ② 내 줄 사본 (실패 무시)
                try:
                    self._append_mine(ymd, row)
                except Exception:
                    pass

                # ③ open 에서 제거
                cur.pop(key, None)
                self._save_open(cur)
                self._owned.discard(key)

            # ④ 워커 kick (락 밖)
            self._ensure_worker()
            self._kick.set()
            return True
        except Exception as e:
            _dbg(f"finalize 실패: {e!r}")
            return False

    def reject(self, key: str, reason: str) -> bool:
        return self.finalize(key, RESULT_REJECT, reason)

    # ── 줄 생성 ──────────────────────────────────────────────
    def _build_row(self, it: dict, *, result: str, reason: str,
                   started: Optional[datetime], finished: Optional[datetime]) -> List[str]:
        rx = _parse_iso(it.get("received_at"))
        # ⚠ 시작하지 않은 요청(거절 / 대기 행만 있는 레시피)은 종료시각도 빈칸이다.
        if not isinstance(started, datetime):
            finished = None
        dur = ""
        if isinstance(started, datetime) and isinstance(finished, datetime):
            dur = f"{(finished - started).total_seconds() / 60.0:.1f}"
        lf = it.get("log_files") or []
        return [
            str(it.get("target", "")),
            _hms(rx),
            _hms(started),
            _hms(finished),
            dur,
            str(result or ""),
            "" if result == RESULT_SUCCESS else _one_line(reason),
            str(it.get("process_names", "")),
            str(it.get("recipe_name", "")),
            str(it.get("row_count", "")),
            str(it.get("request_id", "")),
            str(it.get("peer", "")),
            " / ".join(str(x) for x in lf),
            PROGRAM_NAME,
            str(it.get("key", "")),
        ]

    # ── pending / 내 사본 ────────────────────────────────────
    def _append_pending(self, ymd: str, row: List[str]) -> bool:
        try:
            self._local_dir().mkdir(parents=True, exist_ok=True)
            with open(self._p_pending(), "a", encoding="utf-8", newline="") as f:
                f.write(_csv_line([ymd] + row))
                f.flush()
                os.fsync(f.fileno())
            return True
        except Exception as e:
            _dbg(f"pending append 실패: {e!r}")
            return False

    def _append_mine(self, ymd: str, row: List[str]) -> None:
        p = self._p_mine(ymd)
        self._local_dir().mkdir(parents=True, exist_ok=True)
        new = not p.exists()
        with open(p, "a", encoding="utf-8-sig" if new else "utf-8", newline="") as f:
            if new:
                f.write(_csv_line(HEADER))
            f.write(_csv_line(row))
            f.flush()

    def _read_pending(self) -> List[List[str]]:
        """pending 을 파일 순서 그대로 읽는다(각 행 = 16칸)."""
        try:
            p = self._p_pending()
            if not p.exists():
                return []
            with open(p, "r", encoding="utf-8", newline="") as f:
                return [r for r in csv.reader(f) if r and len(r) >= N_COLS + 1]
        except Exception as e:
            _dbg(f"pending 읽기 실패: {e!r}")
            return []

    def _write_pending(self, rows: List[List[str]]) -> None:
        try:
            self._local_dir().mkdir(parents=True, exist_ok=True)
            text = "".join(_csv_line(r) for r in rows)
            _atomic_write_text(self._p_pending(), text)
        except Exception as e:
            _dbg(f"pending 쓰기 실패: {e!r}")

    def _pending_keys(self) -> set[str]:
        return {r[-1] for r in self._read_pending() if r}

    # ── startup / shutdown ───────────────────────────────────
    def startup_recover(self) -> None:
        """프로그램 시작 시: pending 에 이미 있는 open 은 버리고, 나머지는 중단 줄로 남긴다.
        로컬 파일만 본다 — NAS 를 기다리지 않는다."""
        if not self.enabled():
            return
        try:
            with self._lk:
                P = self._pending_keys()
                cur = self._load_open()
                if cur:
                    for key, it in list(cur.items()):
                        if key in P:
                            cur.pop(key, None)          # 이미 기록됨 → open 만 정리
                            continue
                        st = _parse_iso(it.get("started_at"))
                        reason = ("프로그램 재시작 (공정 중)" if st else "프로그램 재시작 (시작 전)")
                        rx = _parse_iso(it.get("received_at"))
                        ymd = (rx or datetime.now()).strftime("%Y%m%d")
                        row = self._build_row(it, result=RESULT_RESTART, reason=reason,
                                              started=st, finished=None)   # 종료/소요 빈칸
                        if self._append_pending(ymd, row):
                            try:
                                self._append_mine(ymd, row)
                            except Exception:
                                pass
                            cur.pop(key, None)
                    self._save_open(cur)
        except Exception as e:
            _dbg(f"startup_recover 실패: {e!r}")

        self._ensure_worker()
        self._kick.set()

    def close(self, timeout_s: float = 2.0) -> None:
        try:
            self._stop.set()
            self._kick.set()
            t = self._worker
            if t is not None and t.is_alive():
                t.join(timeout=max(0.0, float(timeout_s)))
        except Exception:
            pass

    # ── 워커 ─────────────────────────────────────────────────
    def _ensure_worker(self) -> None:
        if not self.enabled():
            return
        with self._lk:
            if self._worker is not None and self._worker.is_alive():
                return
            self._stop.clear()
            t = threading.Thread(target=self._worker_loop, name="HostProcessLogWorker", daemon=True)
            self._worker = t
            t.start()

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._flush_once()
            except Exception as e:
                _dbg(f"워커 예외(무시): {e!r}")
            self._kick.wait(timeout=self._retry_s())
            self._kick.clear()

    def _flush_once(self) -> None:
        """pending 을 파일 순서대로 한 줄씩 NAS 에 보낸다. 앞 줄이 실패하면 중단(순서 유지).
        프로세스 내 동시 실행을 막아 pending 재작성이 서로를 덮어쓰지 않게 한다."""
        with self._flush_lk:
            self._flush_once_locked()

    def _trim_mem_queue(self) -> None:
        """_lk 안에서 호출. 상한 초과분은 가장 오래된 것부터 버리고 1회만 경고."""
        try:
            over = len(self._mem_queue) - int(self._mem_queue_max)
            if over > 0:
                del self._mem_queue[:over]
                self._mem_queue_dropped += over
                if not self._mem_queue_drop_warned:
                    self._mem_queue_drop_warned = True
                    _dbg(f"WARN 메모리 큐 상한({self._mem_queue_max}줄) 초과 — 가장 오래된 {over}줄 폐기 "
                         f"(파일 기록 실패가 계속되고 있음)")
        except Exception:
            pass

    def _flush_once_locked(self) -> None:
        with self._lk:
            if self._mem_queue:
                # finalize 에서 pending 기록이 실패했던 줄들 — 이제 다시 시도
                for ln in list(self._mem_queue):
                    try:
                        self._local_dir().mkdir(parents=True, exist_ok=True)
                        with open(self._p_pending(), "a", encoding="utf-8", newline="") as f:
                            f.write(ln)
                            f.flush()
                            os.fsync(f.fileno())
                        self._mem_queue.remove(ln)
                        # pending 에 들어갔으니 open 에서도 제거
                        try:
                            k = next(csv.reader(io.StringIO(ln)))[-1]
                            cur = self._load_open()
                            if cur.pop(k, None) is not None:
                                self._save_open(cur)
                            self._owned.discard(k)
                        except Exception:
                            pass
                    except Exception:
                        break

            rows = self._read_pending()

        if not rows:
            return

        remaining = list(rows)
        for r in rows:
            ymd, line_fields = r[0], r[1:1 + N_COLS]
            ok = self._send_one(ymd, line_fields)
            if not ok:
                break                      # 순서 유지 — 뒤 줄도 이번 라운드에서 보내지 않는다
            remaining.pop(0)

        if len(remaining) != len(rows):
            with self._lk:
                # 그 사이 새로 append 된 줄이 있을 수 있으므로 다시 읽어 앞부분만 제거
                now_rows = self._read_pending()
                n_sent = len(rows) - len(remaining)
                self._write_pending(now_rows[n_sent:])

    def _send_one(self, ymd: str, row: List[str]) -> bool:
        """잠금을 잡고 NAS 파일에 한 줄 append. 이미 기록돼 있으면 성공으로 본다."""
        nas = self._nas_dir()
        lock_path = nas / LOCK_DIR_NAME / LOCK_FILE_NAME
        fd = None
        try:
            nas.mkdir(parents=True, exist_ok=True)
            lock_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            _dbg(f"NAS 폴더 준비 실패: {e!r}")
            return False

        # ── 잠금 획득 ──
        acquired = False
        for attempt in range(25):
            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"{PROGRAM_NAME} {os.getpid()} {datetime.now():%Y-%m-%d %H:%M:%S}".encode("utf-8"))
                acquired = True
                break
            except FileExistsError:
                # 오래된 잠금이면 1회 삭제 후 재시도
                try:
                    age = time.time() - lock_path.stat().st_mtime
                    if age > self._lock_stale_s():
                        lock_path.unlink(missing_ok=True)
                        _dbg(f"오래된 잠금 제거(age={age:.0f}s)")
                        continue
                except Exception:
                    pass
                time.sleep(0.2)
            except Exception as e:
                _dbg(f"잠금 오류: {e!r}")
                return False

        if not acquired:
            return False

        try:
            target = self._p_nas(ymd)
            key = row[-1]

            # 이미 기록돼 있는지 (마지막 열이 기록키)
            try:
                if target.exists():
                    with open(target, "r", encoding="utf-8-sig", newline="") as f:
                        for ln in f:
                            if ln.rstrip("\r\n").endswith("," + key):
                                return True          # 이미 기록됨
            except Exception as e:
                _dbg(f"NAS 읽기 실패: {e!r}")
                return False

            try:
                new = not target.exists()
                with open(target, "a", encoding="utf-8-sig" if new else "utf-8", newline="") as f:
                    if new:
                        f.write(_csv_line(HEADER))
                    f.write(_csv_line(row))
                    f.flush()
                return True
            except Exception as e:
                _dbg(f"NAS 쓰기 실패: {e!r}")
                return False
        finally:
            # 잠금은 항상 해제
            try:
                if fd is not None:
                    os.close(fd)
            except Exception:
                pass
            try:
                lock_path.unlink(missing_ok=True)
            except Exception:
                pass


_INSTANCE: Optional[HostProcessLog] = None
_INSTANCE_LK = threading.Lock()


def get_host_log() -> HostProcessLog:
    """모듈 싱글턴."""
    global _INSTANCE
    if _INSTANCE is None:
        with _INSTANCE_LK:
            if _INSTANCE is None:
                _INSTANCE = HostProcessLog()
    return _INSTANCE


__all__ = ["HostProcessLog", "get_host_log", "HEADER", "N_COLS", "PROGRAM_NAME",
           "RESULT_SUCCESS", "RESULT_FAIL", "RESULT_STOP", "RESULT_REJECT",
           "RESULT_RESTART", "RESULT_UNKNOWN"]

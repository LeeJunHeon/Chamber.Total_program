"""selfwatch 기록 — v1 util/self_watch.py 의 파일 출력을 같은 형식으로(LOG-080~083). 값은 1초 행의 프로그램 상태에서.

v1 그대로
- {SELFWATCH_DIR 또는 LOG_ROOT_DIR\\selfwatch}\\selfwatch_YYYYMMDD.csv, 열 ts, working_set_mb, private_mb, handles, gdi, user,
  threads, tasks, gc0, gc1, gc2(구할 수 없으면 -1), utf-8(BOM 없음), ','.join + '\\n'(newline="" — 변환 없음), 머리글은
  '이 실행에서 그 파일에 처음이고' 파일이 없거나 0바이트일 때만.
- 주기 SELFWATCH_INTERVAL_S(30 s, 최소 5 s), SELFWATCH_ENABLE, 시작 때 폴더에서 7일 넘은 파일 삭제,
  SELFWATCH_TRACEMALLOC 이면 tracemalloc.start(1).
- 임계: private_mb > SELFWATCH_PRIVATE_MB, tasks > SELFWATCH_TASKS_MAX, handles > SELFWATCH_HANDLES_MAX, user >
  SELFWATCH_USER_OBJ_MAX(0 이하면 끔) → 항목별 SELFWATCH_DUMP_COOLDOWN_S 안에 1회 dump_YYYYmmdd_HHMMSS.txt + on_event(챗은
  포털 몫 — v1 notify_error_event("SELFWATCH", "", text) 와 같은 문구를 넘김).
- 덤프 형식: '# selfwatch dump <ts>', '# 초과 항목: ..', '', '[sample]' k=v …, '[asyncio tasks …]'(v2 는 asyncio 가 없어
  '(loop 없음)'), '[tracemalloc — lineno 상위 30]', '[threads — 이름별 개수]', utf-8.
v2: tasks 값은 v2 에 asyncio 가 없어 -1(1초 행이 값을 주면 그 값). 샘플은 부르는 쪽(1초 행의 프로그램 상태).

부르는 법
  sw = SelfWatchLog(cfg, writer=w, log=fn, on_event=fn, now=clock_dt, monotonic=clock.monotonic); sw.start()
  sw.on_tick(state)   # 1초마다 — {working_set_mb, private_mb, handles, gdi, user, threads, tasks, gc0, gc1, gc2}
"""
from __future__ import annotations

import threading
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional

from v2.ch12.outputs.base import Owned, QueueWriter, wall_now
from v2.ch12.outputs.paths import Paths
from v2.ch12.params import outputs as P

CSV_FIELDS = ["ts", "working_set_mb", "private_mb", "handles", "gdi", "user", "threads",
              "tasks", "gc0", "gc1", "gc2"]


def make_sample(state: Mapping[str, Any], ts: datetime) -> Dict[str, Any]:
    s: Dict[str, Any] = {k: -1 for k in CSV_FIELDS}
    s["ts"] = ts.strftime("%Y-%m-%d %H:%M:%S")
    for k in CSV_FIELDS[1:]:
        v = state.get(k)
        if v is not None:
            s[k] = v
    return s


def check_thresholds(cfg: Mapping[str, Any], s: Mapping[str, Any]) -> List[str]:
    out: List[str] = []
    try:
        lim = float(P.get(cfg, "SELFWATCH_PRIVATE_MB"))
        if lim > 0 and float(s.get("private_mb", -1)) > lim:
            out.append(f"private_mb={s.get('private_mb')}>{lim:g}")
        lim = int(P.get(cfg, "SELFWATCH_TASKS_MAX"))
        if lim > 0 and int(s.get("tasks", -1)) > lim:
            out.append(f"tasks={s.get('tasks')}>{lim}")
        lim = int(P.get(cfg, "SELFWATCH_HANDLES_MAX"))
        if lim > 0 and int(s.get("handles", -1)) > lim:
            out.append(f"handles={s.get('handles')}>{lim}")
        lim = int(P.get(cfg, "SELFWATCH_USER_OBJ_MAX"))
        if lim > 0 and int(s.get("user", -1)) > lim:
            out.append(f"user_objects={s.get('user')}>{lim}")
    except Exception:
        pass
    return out


def dump_text(reasons: List[str], s: Mapping[str, Any], *, include_tracemalloc: bool = True) -> str:
    lines = [f"# selfwatch dump {s.get('ts')}", f"# 초과 항목: {', '.join(reasons)}", "", "[sample]"]
    for k in CSV_FIELDS:
        lines.append(f"{k}={s.get(k)}")
    lines += ["", "[asyncio tasks — 이름|코루틴 별 개수 상위 30]", "(loop 없음)"]
    lines += ["", "[tracemalloc — lineno 상위 30]"]
    try:
        import tracemalloc
        if not include_tracemalloc:
            lines.append("(동기 폴백 — 생략)")
        elif tracemalloc.is_tracing():
            cur, peak = tracemalloc.get_traced_memory()
            lines.append(f"traced={cur/1048576:.1f}MB peak={peak/1048576:.1f}MB")
            for st in tracemalloc.take_snapshot().statistics("lineno")[:30]:
                lines.append(str(st))
        else:
            lines.append("(tracemalloc 꺼짐)")
    except Exception as e:
        lines.append(f"(tracemalloc 실패: {e!r})")
    lines += ["", "[threads — 이름별 개수]"]
    try:
        for name, n in Counter(t.name for t in threading.enumerate()).most_common():
            lines.append(f"{n:6d}  {name}")
    except Exception:
        pass
    return "\n".join(lines) + "\n"


class SelfWatchLog(Owned):
    def __init__(self, cfg: Optional[Mapping[str, Any]] = None, *, writer: Optional[QueueWriter] = None,
                 log: Optional[Callable[[str], None]] = None, on_event: Optional[Callable[[str, str, str], None]] = None,
                 now: Callable[[], datetime] = wall_now, monotonic: Callable[[], float] = time.monotonic,
                 wall_time: Callable[[], float] = time.time) -> None:
        super().__init__(writer, "selfwatch")
        self.cfg = cfg if cfg is not None else {}
        self.paths = Paths(self.cfg)
        self._log_fn = log
        self.on_event = on_event
        self.now = now
        self.monotonic = monotonic
        self.wall_time = wall_time
        self._next_due: Optional[float] = None
        self._last_dump_ts: Dict[str, float] = {}
        self._header_written: set = set()

    def enabled(self) -> bool:
        return bool(P.get(self.cfg, "SELFWATCH_ENABLE"))

    def start(self) -> None:
        if not self.enabled():
            return
        try:
            if bool(P.get(self.cfg, "SELFWATCH_TRACEMALLOC")):
                import tracemalloc
                if not tracemalloc.is_tracing():
                    tracemalloc.start(1)
        except Exception:
            pass
        self.writer.put(self._purge_old)

    def _purge_old(self) -> None:
        d = self.paths.selfwatch_dir()
        if not d.is_dir():
            return
        cut = self.wall_time() - 7 * 86400
        for p in d.glob("*"):
            try:
                if p.is_file() and p.stat().st_mtime < cut:
                    p.unlink()
            except Exception:
                pass

    def interval(self) -> float:
        try:
            return max(5.0, float(P.get(self.cfg, "SELFWATCH_INTERVAL_S")))
        except Exception:
            return 30.0

    def on_tick(self, state: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        """1초 tick — 주기가 되면 1행 기록 + 임계 판정. 기록했으면 샘플을 돌려준다."""
        if not self.enabled():
            return None
        now = self.monotonic()
        if self._next_due is None:
            self._next_due = now + self.interval()      # v1: 첫 기록은 시작 뒤 한 주기(asyncio.sleep 먼저)
            return None
        if now < self._next_due - 1e-6:
            return None
        self._next_due = now + self.interval()
        return self.tick(state)

    def tick(self, state: Mapping[str, Any]) -> Dict[str, Any]:
        s = make_sample(state, self.now())
        day = self.now().strftime("%Y%m%d")
        self.writer.put(self._append_csv, dict(s), day)
        reasons = check_thresholds(self.cfg, s)
        if reasons:
            self._handle_violation(reasons, s)
        return s

    def _append_csv(self, s: Dict[str, Any], day: str) -> None:
        d = self.paths.selfwatch_dir()
        d.mkdir(parents=True, exist_ok=True)
        p = d / f"selfwatch_{day}.csv"
        need_header = (str(p) not in self._header_written) and (not p.exists() or p.stat().st_size == 0)
        with open(p, "a", encoding="utf-8", newline="") as f:
            if need_header:
                f.write(",".join(CSV_FIELDS) + "\n")
            f.write(",".join(str(s.get(k, -1)) for k in CSV_FIELDS) + "\n")
        self._header_written.add(str(p))

    def _handle_violation(self, reasons: List[str], s: Dict[str, Any]) -> None:
        cooldown = max(0.0, float(P.get(self.cfg, "SELFWATCH_DUMP_COOLDOWN_S")))
        now = self.monotonic()
        keys = [r.split("=", 1)[0] for r in reasons]
        fresh = [k for k in keys if now - self._last_dump_ts.get(k, -1e9) >= cooldown]
        if not fresh:
            return
        for k in fresh:
            self._last_dump_ts[k] = now
        stamp = self.now().strftime("%Y%m%d_%H%M%S")
        self.writer.put(self._dump_and_notify, list(reasons), dict(s), stamp)

    def _dump_and_notify(self, reasons: List[str], s: Dict[str, Any], stamp: str) -> None:
        p: Optional[Path] = None
        try:
            d = self.paths.selfwatch_dir()
            d.mkdir(parents=True, exist_ok=True)
            p = d / f"dump_{stamp}.txt"
            p.write_text(dump_text(reasons, s), encoding="utf-8")
        except Exception:
            p = None
        name = p.name if p else "(덤프 실패)"
        if self._log_fn:
            try:
                self._log_fn(f"[selfwatch] 자원 임계 초과: {', '.join(reasons)} → {name}")
            except Exception:
                pass
        if self.on_event is not None:
            try:
                self.on_event("SELFWATCH", "", f"자원 임계 초과: {', '.join(reasons)} | 덤프 {name}")
            except Exception:
                pass

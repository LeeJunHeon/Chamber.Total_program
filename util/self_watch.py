# -*- coding: utf-8 -*-
"""프로세스 자기 감시(self-watch) — 메모리/핸들/태스크 수를 주기 샘플링하고 임계 초과 시 덤프.

배경: 2026-09-20 04:41:36 CH1_2_Program.exe 가 0xc0000409 로 죽었고 직전에 "가상 메모리 부족"
팝업이 있었으나 덤프가 없어 원인 미상. 다음 재발 시 원인을 확정할 수 있도록 남긴다.

- 30초(SELFWATCH_INTERVAL_S)마다 WorkingSet/PrivateBytes/핸들/GDI/USER/스레드/asyncio 태스크/gc 카운트를
  {SELFWATCH_DIR}/selfwatch_YYYYMMDD.csv 에 append (파일 I/O 는 데몬 스레드).
- 임계(SELFWATCH_PRIVATE_MB / TASKS_MAX / HANDLES_MAX / USER_OBJ_MAX) 초과 시 dump_YYYYMMDD_HHMMSS.txt
  (샘플 + asyncio 태스크 인벤토리 상위 30 + tracemalloc 상위 30 + 스레드 이름별 개수) 를 쓰고 챗 카드 1장.
  같은 위반은 SELFWATCH_DUMP_COOLDOWN_S 동안 1회.
- 모든 진입점은 예외를 삼킨다. 공정/호스트 응답에 절대 전파되지 않는다. 이벤트 루프를 블로킹하지 않는다.
"""
from __future__ import annotations

import asyncio
import ctypes
import gc
import os
import sys
import threading
import time
import traceback
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from lib import config_common as cfgc

_CSV_FIELDS = ["ts", "working_set_mb", "private_mb", "handles", "gdi", "user", "threads",
               "tasks", "gc0", "gc1", "gc2"]


# ───────────────────────── 설정 ─────────────────────────
def _cfg(key: str, default):
    try:
        return getattr(cfgc, key, default)
    except Exception:
        return default


def selfwatch_dir() -> Path:
    d = _cfg("SELFWATCH_DIR", "")
    if d:
        return Path(str(d))
    return Path(str(_cfg("LOG_ROOT_DIR", r"C:\VanaM_Logs\CH1&2"))) / "selfwatch"


# ───────────────────────── 샘플링 ─────────────────────────
def _win_process_counters() -> Dict[str, float]:
    """psutil 없이 ctypes 로 WorkingSet/PrivateBytes(커밋)/핸들/GDI/USER. 실패 항목은 -1."""
    out = {"working_set_mb": -1.0, "private_mb": -1.0, "handles": -1, "gdi": -1, "user": -1}
    if os.name != "nt":
        return out
    try:
        k32 = ctypes.windll.kernel32
        psapi = ctypes.windll.psapi
        k32.GetCurrentProcess.restype = ctypes.c_void_p          # 64-bit HANDLE
        h = k32.GetCurrentProcess()
        psapi.GetProcessMemoryInfo.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_ulong]

        class PMC_EX(ctypes.Structure):
            _fields_ = [("cb", ctypes.c_ulong), ("PageFaultCount", ctypes.c_ulong),
                        ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                        ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t),
                        ("PrivateUsage", ctypes.c_size_t)]
        pmc = PMC_EX()
        pmc.cb = ctypes.sizeof(PMC_EX)
        with_ex = getattr(psapi, "GetProcessMemoryInfo", None)
        if with_ex and with_ex(h, ctypes.byref(pmc), pmc.cb):
            out["working_set_mb"] = pmc.WorkingSetSize / (1024 * 1024)
            out["private_mb"] = pmc.PrivateUsage / (1024 * 1024)
    except Exception:
        pass
    try:
        k32 = ctypes.windll.kernel32
        k32.GetCurrentProcess.restype = ctypes.c_void_p
        k32.GetProcessHandleCount.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_ulong)]
        cnt = ctypes.c_ulong(0)
        if k32.GetProcessHandleCount(k32.GetCurrentProcess(), ctypes.byref(cnt)):
            out["handles"] = int(cnt.value)
    except Exception:
        pass
    try:
        u32 = ctypes.windll.user32
        k32 = ctypes.windll.kernel32
        k32.GetCurrentProcess.restype = ctypes.c_void_p
        u32.GetGuiResources.argtypes = [ctypes.c_void_p, ctypes.c_uint]
        h = k32.GetCurrentProcess()
        out["gdi"] = int(u32.GetGuiResources(h, 0))    # GR_GDIOBJECTS
        out["user"] = int(u32.GetGuiResources(h, 1))   # GR_USEROBJECTS
    except Exception:
        pass
    return out


def sample(loop: Optional[asyncio.AbstractEventLoop] = None) -> Dict[str, Any]:
    """현재 프로세스 자원 샘플. 구할 수 없는 값은 -1. 예외를 내지 않는다."""
    s: Dict[str, Any] = {k: -1 for k in _CSV_FIELDS}
    s["ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        try:
            import psutil  # type: ignore
            mi = psutil.Process().memory_info()
            s["working_set_mb"] = round(mi.rss / (1024 * 1024), 1)
            priv = getattr(mi, "private", None)
            s["private_mb"] = round((priv if priv is not None else mi.vms) / (1024 * 1024), 1)
        except Exception:
            pass
        w = _win_process_counters()
        if s["working_set_mb"] == -1 and w["working_set_mb"] >= 0:
            s["working_set_mb"] = round(w["working_set_mb"], 1)
        if s["private_mb"] == -1 and w["private_mb"] >= 0:
            s["private_mb"] = round(w["private_mb"], 1)
        s["handles"], s["gdi"], s["user"] = w["handles"], w["gdi"], w["user"]
    except Exception:
        pass
    try:
        s["threads"] = threading.active_count()
    except Exception:
        pass
    try:
        if loop is not None:
            s["tasks"] = len(asyncio.all_tasks(loop))
    except Exception:
        pass
    try:
        c = gc.get_count()
        s["gc0"], s["gc1"], s["gc2"] = int(c[0]), int(c[1]), int(c[2])
    except Exception:
        pass
    return s


# ───────────────────────── 덤프 ─────────────────────────
def task_inventory(loop: asyncio.AbstractEventLoop, top: int = 30) -> list[tuple[str, int]]:
    """asyncio 태스크를 (이름, 코루틴 repr 의 함수 부분) 으로 묶어 개수 내림차순 상위 top."""
    c: Counter = Counter()
    try:
        for t in asyncio.all_tasks(loop):
            try:
                name = t.get_name()
            except Exception:
                name = "?"
            try:
                cr = repr(t.get_coro())
                # "<coroutine object X.y at 0x...>" → "X.y"
                key_c = cr.split(" object ", 1)[1].split(" at ", 1)[0] if " object " in cr else cr[:60]
            except Exception:
                key_c = "?"
            # Task-N 처럼 자동 이름은 코루틴 기준으로 묶는다
            key_n = "" if name.startswith("Task-") else name
            c[f"{key_n}|{key_c}" if key_n else key_c] += 1
    except Exception:
        pass
    return c.most_common(top)


def write_dump(reasons: list[str], s: Dict[str, Any], loop: Optional[asyncio.AbstractEventLoop],
               out_dir: Optional[Path] = None, *, include_tracemalloc: bool = True) -> Optional[Path]:
    """상세 덤프 파일을 쓴다. 실패하면 None. include_tracemalloc=False 면 그 항목만 생략(동기 폴백용)."""
    try:
        d = out_dir or selfwatch_dir()
        d.mkdir(parents=True, exist_ok=True)
        p = d / f"dump_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        lines = [f"# selfwatch dump {s.get('ts')}", f"# 초과 항목: {', '.join(reasons)}", "", "[sample]"]
        for k in _CSV_FIELDS:
            lines.append(f"{k}={s.get(k)}")
        lines += ["", "[asyncio tasks — 이름|코루틴 별 개수 상위 30]"]
        if loop is not None:
            for key, n in task_inventory(loop, 30):
                lines.append(f"{n:6d}  {key}")
        else:
            lines.append("(loop 없음)")
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
        p.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return p
    except Exception:
        return None


# ───────────────────────── 감시 루프 ─────────────────────────
class SelfWatch:
    """이벤트 루프에서 주기적으로 sample() 을 뜨고(가벼움), 파일 I/O 는 데몬 스레드로 넘긴다."""

    def __init__(self, loop: asyncio.AbstractEventLoop, *,
                 chat: Any = None, log: Optional[Callable[[str], None]] = None):
        self._loop = loop
        self._chat = chat
        self._log = log
        self._task: Optional[asyncio.Task] = None
        self._last_dump_ts: Dict[str, float] = {}
        self._header_written: set[str] = set()
        self._io_lock = threading.Lock()
        # CSV 기록용 워커 1개를 재사용(매 tick 스레드 생성 금지). submit 실패 시 동기 폴백.
        self._io_exec = None
        try:
            from concurrent.futures import ThreadPoolExecutor
            self._io_exec = ThreadPoolExecutor(max_workers=1, thread_name_prefix="SelfWatchIO")
        except Exception:
            self._io_exec = None

    # -- 공개 --
    def start(self) -> None:
        try:
            if bool(_cfg("SELFWATCH_TRACEMALLOC", True)):
                import tracemalloc
                if not tracemalloc.is_tracing():
                    tracemalloc.start(1)      # frame depth 1: 오버헤드 최소
            threading.Thread(target=self._purge_old, name="SelfWatchPurge", daemon=True).start()
            self._task = self._loop.create_task(self._run(), name="SelfWatch")
        except Exception:
            pass

    def stop(self) -> None:
        try:
            if self._task and not self._task.done():
                self._task.cancel()
        except Exception:
            pass
        try:
            if self._io_exec is not None:
                self._io_exec.shutdown(wait=False)
        except Exception:
            pass

    # -- 내부 --
    def _emit(self, msg: str) -> None:
        try:
            if self._log:
                self._log(msg)
        except Exception:
            pass

    def _purge_old(self) -> None:
        try:
            d = selfwatch_dir()
            if not d.is_dir():
                return
            cut = time.time() - 7 * 86400
            for p in d.glob("*"):
                try:
                    if p.is_file() and p.stat().st_mtime < cut:
                        p.unlink()
                except Exception:
                    pass
        except Exception:
            pass

    async def _run(self) -> None:
        while True:
            try:
                interval = max(5.0, float(_cfg("SELFWATCH_INTERVAL_S", 30.0)))
            except Exception:
                interval = 30.0
            try:
                await asyncio.sleep(interval)
                self.tick()
            except asyncio.CancelledError:
                return
            except Exception:
                pass

    def tick(self) -> Optional[Dict[str, Any]]:
        """1회 샘플링 + CSV 기록 + 임계 판정. 예외를 내지 않는다.

        ⚠ 메모리 고갈로 스레드 생성/submit 이 실패하는 상황이 바로 이 계측이 필요한 순간이다.
          (a) sample 실패 시에만 조기 return, (b) CSV 는 별도 try(실패 시 동기 폴백),
          (c) 임계 판정·덤프는 CSV 성공 여부와 무관하게 반드시 실행한다."""
        try:
            s = sample(self._loop)
        except Exception:
            return None
        # (b) CSV 기록 — 워커 재사용, 실패하면 같은 스레드에서 동기 1회
        try:
            submitted = False
            try:
                if self._io_exec is not None:
                    self._io_exec.submit(self._append_csv, dict(s))
                    submitted = True
            except Exception:
                submitted = False
            if not submitted:
                self._append_csv(dict(s))
        except Exception:
            pass
        # (c) 임계 판정 — CSV 와 무관하게 반드시
        reasons: list[str] = []
        try:
            reasons = self.check_thresholds(s)
        except Exception:
            reasons = []
        try:
            if reasons:
                self._handle_violation(reasons, s)
        except Exception:
            pass
        return s

    @staticmethod
    def check_thresholds(s: Dict[str, Any]) -> list[str]:
        out = []
        try:
            lim = float(_cfg("SELFWATCH_PRIVATE_MB", 2000.0))
            if lim > 0 and float(s.get("private_mb", -1)) > lim:
                out.append(f"private_mb={s.get('private_mb')}>{lim:g}")
            lim = int(_cfg("SELFWATCH_TASKS_MAX", 500))
            if lim > 0 and int(s.get("tasks", -1)) > lim:
                out.append(f"tasks={s.get('tasks')}>{lim}")
            lim = int(_cfg("SELFWATCH_HANDLES_MAX", 5000))
            if lim > 0 and int(s.get("handles", -1)) > lim:
                out.append(f"handles={s.get('handles')}>{lim}")
            lim = int(_cfg("SELFWATCH_USER_OBJ_MAX", 5000))
            if lim > 0 and int(s.get("user", -1)) > lim:
                out.append(f"user_objects={s.get('user')}>{lim}")
        except Exception:
            pass
        return out

    def _handle_violation(self, reasons: list[str], s: Dict[str, Any]) -> None:
        try:
            cooldown = max(0.0, float(_cfg("SELFWATCH_DUMP_COOLDOWN_S", 600.0)))
            now = time.monotonic()
            keys = [r.split("=", 1)[0] for r in reasons]
            fresh = [k for k in keys if now - self._last_dump_ts.get(k, -1e9) >= cooldown]
            if not fresh:
                return
            for k in fresh:
                self._last_dump_ts[k] = now
            # 덤프(파일 I/O)와 챗은 스레드에서 — 루프를 막지 않는다.
            # 스레드 생성이 실패하면(메모리 고갈) 동기 폴백: tracemalloc 상위 30 만 생략하고 나머지는 기록한다.
            snap_loop = self._loop
            try:
                threading.Thread(target=self._dump_and_notify, args=(list(reasons), dict(s), snap_loop),
                                 name="SelfWatchDump", daemon=True).start()
            except Exception:
                self._dump_and_notify(list(reasons), dict(s), snap_loop, sync_fallback=True)
        except Exception:
            pass

    def _dump_and_notify(self, reasons: list[str], s: Dict[str, Any], loop, *, sync_fallback: bool = False) -> None:
        try:
            p = write_dump(reasons, s, loop, include_tracemalloc=not sync_fallback)
            name = p.name if p else "(덤프 실패)"
            self._emit(f"[selfwatch] 자원 임계 초과: {', '.join(reasons)} → {name}")
            chat = self._chat
            if chat is not None:
                fn = getattr(chat, "notify_error_event", None)
                if callable(fn):
                    text = f"자원 임계 초과: {', '.join(reasons)} | 덤프 {name}"
                    # 이벤트 루프 스레드에서 호출(내부에서 create_task 를 쓴다)
                    try:
                        loop.call_soon_threadsafe(lambda: fn("SELFWATCH", "", text))
                    except Exception:
                        pass
        except Exception:
            pass

    def _append_csv(self, s: Dict[str, Any]) -> None:
        try:
            d = selfwatch_dir()
            d.mkdir(parents=True, exist_ok=True)
            p = d / f"selfwatch_{datetime.now().strftime('%Y%m%d')}.csv"
            with self._io_lock:
                need_header = (str(p) not in self._header_written) and (not p.exists() or p.stat().st_size == 0)
                with open(p, "a", encoding="utf-8", newline="") as f:
                    if need_header:
                        f.write(",".join(_CSV_FIELDS) + "\n")
                    f.write(",".join(str(s.get(k, -1)) for k in _CSV_FIELDS) + "\n")
                self._header_written.add(str(p))
        except Exception:
            pass


# ───────────────────────── main.py 진입점 ─────────────────────────
_INSTANCE: Optional[SelfWatch] = None


def start_self_watch(loop: asyncio.AbstractEventLoop, *, chat: Any = None,
                     log: Optional[Callable[[str], None]] = None) -> Optional[SelfWatch]:
    """SELFWATCH_ENABLE 이 참이면 감시를 시작한다. 어떤 예외도 밖으로 내지 않는다."""
    global _INSTANCE
    try:
        if not bool(_cfg("SELFWATCH_ENABLE", True)):
            return None
        sw = SelfWatch(loop, chat=chat, log=log)
        sw.start()
        _INSTANCE = sw
        return sw
    except Exception:
        return None


def stop_self_watch() -> None:
    global _INSTANCE
    try:
        if _INSTANCE:
            _INSTANCE.stop()
    except Exception:
        pass
    _INSTANCE = None

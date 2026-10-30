r"""OES 링크(메인 쪽) — 별도 프로세스 OES 워커(v2/ch12/workers/oes_worker)를 시작·재시작·kill 하고 명령을 주고받는다.

v1 device/oes.py(OESAsync 클라이언트)의 동작을 스레드로 다시 쓴 것(v1 import 없음, 메인은 DLL 을 부르지 않는다):
- daemon 모드 기본(OES_DAEMON). 공정 Start 때 initialize(=daemon 기동, CHAMBER_OES_INIT_TIMEOUT_S), 측정은 stdin 명령 1줄.
  daemon 측정 실패 → (STOP 요청 때문이면 끝) daemon kill → 강제 재기동 → 1회 재시도 → 그래도 실패면 one-shot 워커 1회.
- one-shot: '--cmd measure ...' 프로세스 1개, 결과는 finished JSON 우선, 없으면 rc==0·CSV 크기 > 0.
- STOP: 로컬 플래그 파일 2개('<CSV 폴더>\.stop_usb{U}.flag', '<CSV>.stop')에 'stop\n'. 공정은 wait=False(플래그만).
- CSV tail 0.1 s(워커가 쓰는 로컬 CSV — 머리글 2번째 칸부터 파장, 각 줄 2번째 칸부터 세기) → on_data(x, y).
  60 s 안에 CSV 가 안 생기면 워커/daemon kill. 표본 간격 1 s·평균 3 회(v1 생성자 기본값 — OES_AVG_COUNT 는 v1 도 안 읽음, D177).
- 저장 폴더: 환경변수 OES_LOCAL_BASE → 설정 OES_LOCAL_DIR(C:\VanaM_Logs\OES) \CH{n}. USB: CH1 0, CH2 1.
v2 연결부: 명령 id(응답 매칭은 id — v1 은 out_csv 문자열), 하트비트(hb — 나이를 status() 에), 종료 순서는 v1 그대로
  (close → 2 s → terminate → 2 s → kill → 2 s). 실행은 onedir exe(OES_WORKER_EXE) 또는 python -m(기본, decisions D01).
1초 행(블록 "<ch>.oes")에는 상태만: running(워커 프로세스 살아 있음)·measuring(측정 중)·sample_age_s(마지막 표본 나이).
이 링크는 실제 하위 프로세스를 다루므로 RealClock 에서 쓴다(가상 시계 sim 은 워커 --fake 를 실제 시각으로 띄운다).
"""
from __future__ import annotations

import contextlib
import itertools
import os
import subprocess
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from v2.common.snapshot import DeviceBlock, FieldDef, FType
from v2.ch12.drivers import worker_proc as wp
from v2.ch12.drivers.oes_oneshot import OneShotMixin
from v2.ch12.drivers.oes_tail import TailMixin, _Tail

MODULE = "v2.ch12.workers.oes_worker"
SAMPLE_INTERVAL_S = 1.0       # v1 OESAsync 생성자 기본값(device/oes.py:155)
AVG_COUNT = 3                 # v1 생성자 기본값(device/oes.py:156)
DEFAULT_LOCAL_BASE = r"C:\VanaM_Logs\OES"

BUDGET = dict(queries_per_tick=0, bytes_tx=0, bytes_rx=0, min_gap_ms=0, fits_1s=True, alternate=None,
              note="장비 통신 없음 — 1초 행은 링크 상태(프로세스·측정 중·마지막 표본 나이)만 메모리에서. "
                   "분광기 읽기는 워커가 표본 1 s(avg 3회 spReadDataEx)로, 메인은 CSV 를 0.1 s 마다 tail")


def blocks(chamber: str) -> List[DeviceBlock]:
    return [DeviceBlock(f"{chamber}.oes", f"{chamber.upper()} OES", (
        FieldDef("running", FType.BOOL, "", "OES 워커 실행 중", source="링크(프로세스 살아 있음)"),
        FieldDef("measuring", FType.BOOL, "", "OES 측정 중", source="링크(is_running)"),
        FieldDef("sample_age_s", FType.FLOAT, "s", "마지막 표본 나이", fmt=".1f", source="CSV tail 마지막 행"),
    ), desc="OES 분광기(별도 프로세스 워커 — 값은 OES CSV 에)")]


@dataclass(frozen=True)
class OesEvent:
    kind: str                     # status / finished / worker_exit / rejected
    detail: str
    reason: Optional[str] = None
    data: Optional[dict] = None


@dataclass
class OesResult:
    """v1 OESEvent(kind="finished") 와 같은 내용. emitted=False 는 v1 이 finished 없이 끝난 경우(STOP 중 실패·중복 요청 무시·bench)."""
    emitted: bool
    success: bool = False
    message: str = ""
    out_csv: Optional[str] = None
    rows: int = 0
    elapsed_s: float = 0.0
    error: Optional[str] = None
    reason: Optional[str] = None


@dataclass
class _Waiter:
    cid: int
    result: Optional[dict] = None


def _flag(v: Any) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def make_filename(now: Optional[datetime] = None) -> str:
    return f"OES_Data_{(now or datetime.now()).strftime('%Y%m%d_%H%M%S')}.csv"


class OesLink(OneShotMixin, TailMixin):
    NAME = "oes"

    def __init__(self, clock, worker, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[OesEvent], None]] = None, chamber: str = "ch1",
                 on_data: Optional[Callable[[list, list], None]] = None) -> None:
        self.clock, self.worker, self.cfg = clock, worker, cfg
        self.writes_allowed = writes_allowed
        self.on_event, self.on_data = on_event, on_data
        self.chamber = chamber
        self.ch = 1 if chamber == "ch1" else 2
        self.block = f"{chamber}.oes"
        self._ids = itertools.count(1)
        self.sent: List[dict] = []                 # 워커로 보낸 줄(골든 비교·진단, 최근 200)
        self.spawned: List[dict] = []              # 띄운 명령줄·환경(골든 비교·진단, 최근 50)
        self._local_dir: Optional[Path] = None
        # 측정 상태(v1 is_running·_stop_requested·_out_csv_local·_rows_seen)
        self.is_running = False
        self._stop_requested = False
        self._out_csv: Optional[Path] = None
        self._rows_seen = 0
        self._last_sample: Optional[float] = None
        self._tail: Optional[_Tail] = None
        self._worker_finished: Optional[dict] = None
        # daemon
        self._daemon: Optional[wp.ProcHandle] = None
        self._daemon_ready = False
        self._daemon_info: Optional[dict] = None
        self._waiter: Optional[_Waiter] = None
        self._daemon_lock = threading.Lock()
        # one-shot·init(OneShotMixin)
        self._proc: Optional[wp.ProcHandle] = None
        self._init_proc: Optional[wp.ProcHandle] = None
        self._init_done = self._init_ok = False
        self._init_obj: Optional[dict] = None
        self.init_error: Optional[str] = None
        # 작업 스레드(초기화·측정을 v1 처럼 한 번에 하나씩)
        self._jobs: List[Callable[[], None]] = []
        self._job_thread: Optional[threading.Thread] = None
        self._closing = False
        self._threads: List[threading.Thread] = []

    # ───────── 설정 ─────────
    def _c(self, key: str, default: Any) -> Any:
        """설정 값(없으면 v1 의 같은 이름 환경변수, 그것도 없으면 v1 기본값)."""
        if key in self.cfg:
            return self.cfg[key]
        env = os.environ.get(key)
        if env is not None and env.strip() != "":
            return env
        return default

    @property
    def daemon_enabled(self) -> bool:
        return _flag(self._c("OES_DAEMON", True))

    @property
    def usb(self) -> int:
        return int(self._c(f"CHAMBER_OES_USB_INDEX_CH{self.ch}", 0 if self.ch == 1 else 1))

    def local_dir(self) -> Path:
        """v1 device/oes.py:35-50·171-183 — 처음 쓸 때 만든다(v1 은 생성자에서)."""
        if self._local_dir is not None:
            return self._local_dir
        env = (os.environ.get("OES_LOCAL_BASE") or "").strip()
        base = Path(env) if env else Path(str(self.cfg.get("OES_LOCAL_DIR") or DEFAULT_LOCAL_BASE))
        d = base / f"CH{self.ch}"
        try:
            d.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            d = Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) / "VanaM" / "OES" / f"CH{self.ch}"
            d.mkdir(parents=True, exist_ok=True)
        except Exception:
            d = wp.REPO_ROOT / "_OES" / f"CH{self.ch}"
            d.mkdir(parents=True, exist_ok=True)
        self._local_dir = d
        return d

    def _dll_path(self) -> str:
        return str(wp.resolve_path(str(self._c("OES_DLL_PATH", "apps/oes_service/SPdbUSBm.dll"))))

    def _home(self) -> str:
        h = str(self._c("OES_WORKER_HOME", "") or "")
        return str(wp.resolve_path(h)) if h else str(Path(self._dll_path()).parent)

    def _worker_argv(self) -> Optional[List[str]]:
        """None = 설정한 exe 가 없음(v1 '워커 exe 없음')."""
        exe = str(self._c("OES_WORKER_EXE", "") or "")
        if exe and not wp.resolve_path(exe).is_file():
            return None
        return wp.worker_command(exe, MODULE)

    def _v2_args(self, cid: Optional[int] = None) -> List[str]:
        out = ["--home", self._home(), "--hb_s", str(float(self._c("OES_HEARTBEAT_S", 1.0)))]
        if cid is not None:
            out += ["--id", str(cid)]
        if _flag(self._c("OES_WORKER_FAKE", False)):
            out.append("--fake")
        return out

    def _creationflags(self) -> int:
        """v1 _worker_creationflags — 기본 0(콘솔 상속), HIDE 면 CREATE_NO_WINDOW, SHOW 면 CREATE_NEW_CONSOLE."""
        if os.name != "nt":
            return 0
        if _flag(self._c("OES_WORKER_HIDE_CONSOLE", False)):
            return int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
        if _flag(self._c("OES_WORKER_SHOW_CONSOLE", False)):
            return int(getattr(subprocess, "CREATE_NEW_CONSOLE", 0))
        return 0

    def _f(self, key: str, default: float) -> float:
        try:
            return float(self._c(key, default))
        except (TypeError, ValueError):
            return float(default)

    # ───────── 이벤트 ─────────
    def _emit(self, kind: str, detail: str, reason: Optional[str] = None, data: Optional[dict] = None) -> None:
        if self.on_event:
            try:
                self.on_event(OesEvent(kind, detail, reason, data))
            except Exception:
                pass

    def _status(self, msg: str) -> None:
        self._emit("status", msg)

    def _spawn(self, argv: List[str], env_extra: Dict[str, str], on_message, tag: str) -> wp.ProcHandle:
        env = wp.worker_env(env_extra)
        with contextlib.suppress(OSError):
            Path(self._home()).mkdir(parents=True, exist_ok=True)     # cwd(v1 = 워커 exe 폴더 — 늘 있음)
        h = wp.ProcHandle(self.clock, f"oes{self.ch}-{tag}", argv, cwd=self._home(), env=env,
                          creationflags=self._creationflags(), on_message=on_message, on_exit=self._on_exit)
        self.spawned = (self.spawned + [{"tag": tag, "argv": list(argv), "env": dict(env_extra)}])[-50:]
        h.start()
        return h

    def _send(self, h: Optional[wp.ProcHandle], obj: Dict[str, Any]) -> bool:
        if h is None:
            return False
        msg = {"v": 2, **obj}
        ok = h.send(msg)
        if ok:
            self.sent = (self.sent + [msg])[-200:]
        return ok

    def _on_exit(self, h: wp.ProcHandle) -> None:
        with self.clock.cond:
            if h is self._daemon and self._waiter is not None and self._waiter.result is None:
                # v1 _watch_daemon_stdout: stdout 닫힘 → 대기 중 측정 즉시 실패(빠른 폴백)
                self._waiter.result = {"kind": "fatal", "ok": False, "error": "daemon stdout closed"}
            unexpected = h is self._daemon
            self.clock.cond.notify_all()
        if unexpected:
            self._emit("worker_exit", f"[OES] daemon 종료됨 rc={h.returncode}", "WORKER_EXIT")

    # ───────── 작업 스레드 ─────────
    def _submit(self, job: Callable[[], None]) -> None:
        with self.clock.cond:
            self._jobs.append(job)
            if self._job_thread is None or not self._job_thread.is_alive():
                self._job_thread = self.clock.start_thread(self._job_loop, f"oes{self.ch}-jobs")
            self.clock.cond.notify_all()

    def _job_loop(self) -> None:
        while True:
            with self.clock.cond:
                if self._closing or not self._jobs:
                    self._job_thread = None
                    return
                job = self._jobs.pop(0)
            try:
                job()
            except Exception as e:          # 작업 하나의 오류로 스레드가 죽지 않게
                self._status(f"[OES] 작업 예외: {type(e).__name__}: {e}")

    def _reject_bench(self, what: str, on_done) -> bool:
        if self.writes_allowed:
            return False
        self._emit("rejected", f"[OES] bench(읽기 전용) — {what} 거부", "BENCH_READ_ONLY")
        if on_done:
            on_done(OesResult(emitted=False, reason="BENCH_READ_ONLY"))
        return True

    # ───────── 공개 API(공정 엔진이 부름) ─────────
    def initialize(self, timeout_s: Optional[float] = None, force: bool = False,
                   on_done: Optional[Callable[[bool, Optional[str]], None]] = None) -> None:
        """v1 initialize_device — daemon 모드면 daemon 기동(READY 까지), 아니면 init 워커 1회. on_done(ok, 오류)."""
        if not self.writes_allowed:
            self._emit("rejected", "[OES] bench(읽기 전용) — 초기화 거부", "BENCH_READ_ONLY")
            if on_done:
                on_done(False, "BENCH_READ_ONLY")
            return
        t = float(timeout_s if timeout_s is not None else self._f("CHAMBER_OES_INIT_TIMEOUT_S", 20.0))

        def job() -> None:
            if self.daemon_enabled:
                ok = self._ensure_daemon(t, force)
            else:
                ok = self._init_oneshot(t, force)
            if on_done:
                on_done(ok, None if ok else (self.init_error or "unknown"))
        self._submit(job)

    def measure(self, duration_s: float, integration_ms: int,
                on_done: Optional[Callable[[OesResult], None]] = None) -> None:
        """v1 run_measurement — daemon(+재기동·one-shot 폴백) 또는 one-shot. 끝나면 on_done(OesResult)."""
        if self._reject_bench("측정", on_done):
            return
        if not self.daemon_enabled and self._proc is not None and self._proc.alive():
            if self.is_running and not self._stop_requested:
                self._status("[OES] 이미 측정 중 → 요청 무시")
                if on_done:
                    on_done(OesResult(emitted=False, reason="BUSY"))
                return
            self._preempt_oneshot()

        def job() -> None:
            res = self._run_measurement(float(duration_s), int(integration_ms))
            if res.emitted:
                self._emit("finished", res.message, None if res.success else "OES_FAILED", data=res.__dict__)
            if on_done:
                on_done(res)
        self._submit(job)

    def stop(self, wait: bool = False, timeout_s: float = 60.0, on_done: Optional[Callable[[], None]] = None) -> None:
        """v1 stop_measurement — 플래그 파일만(wait=False, 공정 경로) 또는 끝날 때까지 기다림(wait=True, 별도 스레드)."""
        if not wait:
            self._stop_impl(False, timeout_s)
            if on_done:
                on_done()
            return

        def run() -> None:
            self._stop_impl(True, timeout_s)
            if on_done:
                on_done()
        t = self.clock.start_thread(run, f"oes{self.ch}-stop")
        self._threads.append(t)

    def cleanup(self, on_done: Optional[Callable[[], None]] = None) -> None:
        """v1 cleanup — OES_CLEANUP_DETACH 면 stop(wait=False), 아니면 stop(wait=True, OES_STOP_WAIT_S 60 s)."""
        if _flag(self._c("OES_CLEANUP_DETACH", False)):
            self.stop(wait=False, on_done=on_done)
        else:
            self.stop(wait=True, timeout_s=self._f("OES_STOP_WAIT_S", 60.0), on_done=on_done)

    def close(self) -> None:
        """프로그램 종료 — 작업 스레드를 끝내고 daemon 은 close 명령으로, 나머지는 kill. 스레드까지 정리."""
        with self.clock.cond:
            self._closing = True
            self._jobs.clear()
            self.clock.cond.notify_all()
        self._stop_tail()
        with self._daemon_lock:
            self._shutdown_daemon(graceful=True)
        self.cleanup_quick()
        for t in [self._job_thread] + self._threads:
            if t is not None and t is not threading.current_thread():
                t.join(5.0)
        self._threads = []

    def status(self) -> Dict[str, Any]:
        d = self._daemon
        now = self.clock.monotonic()
        return {"daemon_alive": bool(d and d.alive()), "daemon_ready": self._daemon_ready,
                "daemon_pid": d.pid if d else None,
                "hb_age_s": (None if not d or d.last_hb is None else round(now - d.last_hb, 1)),
                "oneshot_alive": bool(self._proc and self._proc.alive()), "measuring": self.is_running,
                "stop_requested": self._stop_requested, "rows": self._rows_seen,
                "out_csv": str(self._out_csv) if self._out_csv else None, "init_error": self.init_error}

    def source_read(self, session, seq: int) -> Dict[str, Any]:
        b = self.block
        procs = (self._daemon, self._proc, self._init_proc)
        age = None if self._last_sample is None else round(self.clock.monotonic() - self._last_sample, 1)
        return {f"{b}.running": any(p is not None and p.alive() for p in procs),
                f"{b}.measuring": bool(self.is_running), f"{b}.sample_age_s": age}

    # ───────── daemon (v1 device/oes.py:445-646) ─────────
    def _on_daemon_msg(self, obj: Dict[str, Any]) -> None:
        k = obj.get("kind")
        if k == "hb":
            return
        if k == "_raw":
            self._status(f"[OES][daemon] {obj.get('text')}")
            return
        if k == "status":
            msg = obj.get("message", "")
            if msg:
                self._status(f"[OES] {msg}")
            return
        if k == "daemon":
            with self.clock.cond:
                self._daemon_info = obj
                self._daemon_ready = True           # ok 와 관계없이 '응답 받음'
                self.clock.cond.notify_all()
            if not bool(obj.get("ok", False)):
                self._status(f"[OES] daemon FAIL: {obj.get('error')}")
            return
        if k == "started":
            self._status(f"[OES] daemon started out_csv={obj.get('out_csv')} cols={obj.get('cols')} usb={obj.get('resolved_usb')}")
            return
        if k in ("finished", "fatal"):
            with self.clock.cond:
                w = self._waiter
                if w is not None and w.result is None and obj.get("id") == w.cid:
                    w.result = obj
                    self.clock.cond.notify_all()
                    return
                self._worker_finished = obj          # 매칭 안 되면 참고용
            return
        self._status(f"[OES][daemon] {obj}")

    def _ensure_daemon(self, timeout_s: float, force: bool) -> bool:
        with self._daemon_lock:
            d = self._daemon
            if not force and d is not None and d.alive() and self._daemon_ready:
                return True
            self._shutdown_daemon(graceful=True)
            if self._closing:                 # 프로그램 종료 중에는 새로 띄우지 않는다
                return False
            base = self._worker_argv()
            if base is None:
                self.init_error = "daemon 워커 exe 없음"
                self._status(f"[OES] daemon 워커 exe 없음: {self._c('OES_WORKER_EXE', '')}")
                return False
            argv = [*base, "--cmd", "daemon", "--ch", str(self.ch), "--usb", str(self.usb),
                    "--out_dir", str(self.local_dir()), "--dll_path", self._dll_path(), *self._v2_args()]
            wt = max(5.0, float(timeout_s) - 2.0)
            self._status(f"[OES] daemon spawn ch={self.ch} usb={self.usb} parent_timeout={timeout_s}s "
                         f"worker_timeout={wt}s cmd={argv} cwd={self._home()}")
            with self.clock.cond:
                self._daemon_ready, self._daemon_info = False, None
            try:
                h = self._spawn(argv, {"OES_INIT_TIMEOUT_S": str(wt)}, self._on_daemon_msg, "daemon")
            except OSError as e:
                self.init_error = f"daemon spawn 실패: {e}"
                self._status(f"[OES] daemon spawn 실패: {type(e).__name__}: {e}")
                return False
            self._daemon = h
            got = self.clock.wait_until(self.clock.monotonic() + float(timeout_s), lambda: self._daemon_ready)
            if not got:
                self.init_error = f"daemon READY timeout after {timeout_s}s"
                self._status(f"[OES] daemon READY timeout after {timeout_s}s → kill")
                self._shutdown_daemon(graceful=False)
                return False
            info = self._daemon_info or {}
            if not bool(info.get("ok", False)):
                self.init_error = str(info.get("error"))
                self._status(f"[OES] daemon READY 실패: {info.get('error')}")
                self._shutdown_daemon(graceful=False)
                return False
            self.init_error = None
            self._status(f"[OES] daemon READY: {info}")
            return True

    def _shutdown_daemon(self, graceful: bool = True) -> None:
        with self.clock.cond:
            h, self._daemon = self._daemon, None
            self._daemon_ready, self._daemon_info = False, None
            w, self._waiter = self._waiter, None
            if w is not None and w.result is None:
                w.result = {"kind": "fatal", "ok": False, "error": "daemon stopped"}
            self.clock.cond.notify_all()
        if h is None:
            return
        step = self._f("OES_CLOSE_STEP_S", 2.0)
        if graceful and h.alive() and self._send(h, {"id": next(self._ids), "cmd": "close"}):
            h.wait(step)                     # v1: close → 2 s → terminate → 2 s → kill → 2 s
        h.shutdown(False, None, step)
        h.join()

    def _local_stop_flags(self) -> tuple:
        d = self.local_dir()
        usb_flag = d / f".stop_usb{int(self.usb)}.flag"
        csv_flag = None
        if self._out_csv is not None:
            usb_flag = self._out_csv.parent / f".stop_usb{int(self.usb)}.flag"
            csv_flag = self._out_csv.with_suffix(self._out_csv.suffix + ".stop")
        return usb_flag, csv_flag

    def _clear_local_flags(self) -> None:
        d = self.local_dir()
        with contextlib.suppress(OSError):
            (d / f".stop_usb{int(self.usb)}.flag").unlink()
        with contextlib.suppress(OSError):
            for p in d.glob("*.stop"):
                with contextlib.suppress(OSError):
                    p.unlink()

    def _measure_timeout(self, duration_s: float) -> float:
        return max(self._f("OES_MEASURE_TIMEOUT_MIN_S", 10.0), float(duration_s) + self._f("OES_MEASURE_TIMEOUT_MARGIN_S", 60.0))

    def _new_measure(self) -> Path:
        self._stop_tail()
        out_csv = self.local_dir() / make_filename()
        self._out_csv = out_csv
        self._rows_seen = 0
        self._stop_requested = False
        self._worker_finished = None
        return out_csv

    def _run_daemon(self, duration_s: float, integration_ms: int) -> OesResult:
        """v1 _run_measurement_daemon — ok=False·시간 초과·전송 실패는 예외(폴백 유도)."""
        if not self._ensure_daemon(self._f("CHAMBER_OES_INIT_TIMEOUT_S", 20.0), False):
            raise RuntimeError("daemon not ready")
        out_csv = self._new_measure()
        self._clear_local_flags()
        cid = next(self._ids)
        with self.clock.cond:
            self._waiter = _Waiter(cid)
        self._start_tail(out_csv)
        t0 = self.clock.monotonic()
        self.is_running = True
        try:
            safe_integ = int(integration_ms)
            if safe_integ <= 0:
                safe_integ = int(self._c("OES_DEFAULT_INTEGRATION_MS", 50) or 50)
            sent = self._send(self._daemon, {
                "id": cid, "cmd": "measure", "duration_s": float(duration_s), "integration_ms": safe_integ,
                "sample_interval_s": float(SAMPLE_INTERVAL_S), "avg_count": int(AVG_COUNT),
                "out_csv": str(out_csv), "out_dir": str(self.local_dir()),
            })
            if not sent:
                raise RuntimeError("daemon send failed")
            self._status(f"[OES] daemon measure start: {duration_s / 60:.1f}분, out={out_csv}")
            w = self._waiter
            if not self.clock.wait_until(t0 + self._measure_timeout(duration_s), lambda: w is not None and w.result is not None):
                raise TimeoutError(f"daemon measure timeout ({self._measure_timeout(duration_s):g}s)")
            obj = w.result or {}
            if not bool(obj.get("ok", False)):
                raise RuntimeError(str(obj.get("error") or "daemon measure failed"))
            elapsed = float(obj.get("elapsed_s", self.clock.monotonic() - t0) or (self.clock.monotonic() - t0))
            rows = int(obj.get("rows", self._rows_seen) or self._rows_seen)
            nas_ok, nas_csv, nas_error = bool(obj.get("nas_ok", False)), obj.get("nas_csv"), obj.get("nas_error")
            final_path, msg = out_csv, "OES 측정 완료"
            if nas_ok and nas_csv:
                final_path, msg = Path(str(nas_csv)), "OES 측정 완료(NAS 저장)"
            else:
                msg = f"OES 측정 완료(로컬 저장, NAS 실패: {nas_error})" if nas_error else "OES 측정 완료(로컬 저장)"
            self._stop_tail()
            if nas_ok and out_csv.exists() and not bool(obj.get("local_deleted", False)):
                with contextlib.suppress(OSError):
                    out_csv.unlink()
            return OesResult(True, True, msg, str(final_path), rows, elapsed, None)
        finally:
            self._stop_tail()
            with self.clock.cond:
                self._waiter = None
            self.is_running = False

    def _run_measurement(self, duration_s: float, integration_ms: int) -> OesResult:
        if not self.daemon_enabled:
            return self._run_oneshot(duration_s, integration_ms)
        try:
            return self._run_daemon(duration_s, integration_ms)
        except Exception as e1:
            if self._closing:
                return OesResult(emitted=False, reason="CLOSING")
            if self._stop_requested:
                self._status(f"[OES] stop 요청으로 종료됨 → fallback 재기동 skip ({type(e1).__name__})")
                return OesResult(emitted=False, reason="STOPPED")
            self._status(f"[OES] daemon 실패 → 재기동 후 재시도: {type(e1).__name__}: {e1}")
            with self._daemon_lock:
                self._shutdown_daemon(graceful=False)
            if self._ensure_daemon(self._f("CHAMBER_OES_INIT_TIMEOUT_S", 20.0), True):
                try:
                    return self._run_daemon(duration_s, integration_ms)
                except Exception as e2:
                    self._status(f"[OES] daemon 재시도 실패 → one-shot fallback: {type(e2).__name__}: {e2}")
            if self._closing:
                return OesResult(emitted=False, reason="CLOSING")
            return self._run_oneshot(duration_s, integration_ms)

    def _stop_impl(self, wait: bool, timeout_s: float) -> None:
        p = self._proc
        if p is not None and p.alive():
            return self._stop_oneshot(wait, timeout_s)
        if self.daemon_enabled:
            return self._stop_daemon(wait, timeout_s)
        return self._stop_oneshot(wait, timeout_s)

    def _stop_daemon(self, wait: bool, timeout_s: float) -> None:
        """v1 _stop_measurement_daemon(device/oes.py:784-826)."""
        d = self._daemon
        if d is None or not d.alive():
            self._stop_tail()
            self._stop_requested = False
            return
        self._stop_requested = True
        self._status("[OES] daemon stop → request stop flag")
        self._stop_tail()
        usb_flag, csv_flag = self._local_stop_flags()
        self._write_flags(usb_flag, csv_flag)
        if not wait:
            self._status("[OES] daemon stop flag set(detach)")
            return
        w = self._waiter
        if w is not None and w.result is None:
            if not self.clock.wait_until(self.clock.monotonic() + float(timeout_s), lambda: w.result is not None):
                self._status("[OES] daemon stop wait timeout → daemon kill")
                with self._daemon_lock:
                    self._shutdown_daemon(graceful=False)
        for f in (usb_flag, csv_flag):
            if f is not None:
                with contextlib.suppress(OSError):
                    f.unlink()

    @staticmethod
    def _write_flags(usb_flag: Path, csv_flag: Optional[Path]) -> None:
        with contextlib.suppress(OSError):
            usb_flag.write_text("stop\n", encoding="utf-8")
        if csv_flag is not None:
            with contextlib.suppress(OSError):
                csv_flag.write_text("stop\n", encoding="utf-8")

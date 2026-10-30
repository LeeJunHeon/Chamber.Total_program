r"""별도 프로세스 워커(OES·RGA) 공용 연결 — 시작·JSONL 주고받기·종료·kill. oes_link·rga_link 가 쓴다.

실행 방법 두 가지(설정으로 고름, decisions D01):
- onedir exe: 설정 키(OES_WORKER_EXE·RGA_WORKER_EXE)에 exe 경로가 있고 파일이 있으면 [exe, 인자...]
  (onefile 은 kill 이 부트로더만 죽인다 — plan 4장. 그래서 onedir 만)
- 소스 실행: 없으면 [sys.executable, "-m", "v2.ch12.workers.<이름>", 인자...], PYTHONPATH 앞에 저장소 루트.
줄 형식(v2): 한 줄 = JSON 하나(utf-8). 메인 → 워커 {"v": 2, "id": n, "cmd": ...}(ensure_ascii — 파이프 인코딩과 무관),
  워커 → 메인 {"v": 2, "id": n|null, "kind": ...}. JSON 이 아닌 줄은 {"kind": "_raw", "text": ...} 로 넘긴다.
스레드: 프로세스마다 stdout·stderr 읽기, 종료 대기 — 일반 스레드(가상 시계 참가자 아님), 프로세스가 끝나면 끝난다.
  상태가 바뀌면 clock.notify() — 링크는 clock.wait_until 로 기다린다(이 링크는 실제 하위 프로세스라 RealClock 에서 쓴다).
종료(v1 device/oes.py:530-577 _shutdown_daemon): graceful 이면 close 명령 → step_s → terminate → step_s → kill → step_s.
"""
from __future__ import annotations

import collections
import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from v2.common.transport import Transport
from v2.common.worker import Worker

REPO_ROOT = Path(__file__).resolve().parents[3]        # .../Chamber.Total_program (v2 의 부모)


class NullTransport(Transport):
    """입출력 없는 전송 — 링크 상태만 돌려주는 1초 읽기 소스용(SnapshotBus 는 worker 를 거쳐 읽기를 부른다)."""

    def open(self) -> None:
        self._open = True

    def close(self) -> None:
        self._open = False

    def is_open(self) -> bool:
        return getattr(self, "_open", False)

    def reset_input(self) -> None:
        pass


def status_worker(clock, name: str) -> Worker:
    """1초 행에 링크 상태(실행 중·측정 중·마지막 표본 나이)를 싣는 읽기 전용 worker(포트 없음)."""
    return Worker(name, NullTransport(clock), writes_allowed=True)


def resolve_path(p: str) -> Path:
    """설정 경로 — 상대 경로면 저장소 루트 기준(v1 파일 apps\\… 를 가리킬 때)."""
    q = Path(str(p)).expanduser()
    return q if q.is_absolute() else (REPO_ROOT / q)


def worker_command(exe: str, module: str) -> List[str]:
    if exe:
        p = resolve_path(exe)
        if p.is_file():
            return [str(p)]
    return [sys.executable, "-m", module]


def worker_env(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    pp = env.get("PYTHONPATH", "")
    root = str(REPO_ROOT)
    if root not in pp.split(os.pathsep):
        env["PYTHONPATH"] = root + (os.pathsep + pp if pp else "")
    for k, v in (extra or {}).items():
        env[k] = str(v)
    return env


class ProcHandle:
    """워커 프로세스 하나. on_message(dict) 는 stdout 읽기 스레드에서 부른다(짧게)."""

    def __init__(self, clock, name: str, argv: List[str], *, cwd: Optional[str] = None,
                 env: Optional[Dict[str, str]] = None, creationflags: int = 0,
                 on_message: Optional[Callable[[Dict[str, Any]], None]] = None,
                 on_exit: Optional[Callable[["ProcHandle"], None]] = None, stderr_keep: int = 80) -> None:
        self.clock, self.name, self.argv = clock, name, list(argv)
        self.cwd, self.env, self.creationflags = cwd, env, creationflags
        self.on_message, self.on_exit = on_message, on_exit
        self.stderr_tail: "collections.deque[str]" = collections.deque(maxlen=stderr_keep)
        self.proc: Optional[subprocess.Popen] = None
        self.exited = False
        self.eof = False
        self.last_hb: Optional[float] = None           # 마지막 하트비트(monotonic)
        self.last_line: Optional[float] = None
        self._wlock = threading.Lock()
        self._threads: List[threading.Thread] = []

    # ───────── 시작·상태 ─────────
    def start(self) -> None:
        kw: Dict[str, Any] = {}
        if os.name == "nt":
            kw["creationflags"] = int(self.creationflags)
        self.proc = subprocess.Popen(self.argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     cwd=self.cwd, env=self.env, bufsize=0, **kw)
        for target, nm in ((self._read_stdout, "out"), (self._read_stderr, "err"), (self._wait_exit, "wait")):
            t = threading.Thread(target=target, name=f"{self.name}-{nm}-{self.proc.pid}", daemon=True)
            t.start()
            self._threads.append(t)

    @property
    def pid(self) -> Optional[int]:
        return None if self.proc is None else self.proc.pid

    @property
    def returncode(self) -> Optional[int]:
        return None if self.proc is None else self.proc.poll()

    def alive(self) -> bool:
        return self.proc is not None and not self.exited and self.proc.poll() is None

    # ───────── 주고받기 ─────────
    def send(self, obj: Dict[str, Any]) -> bool:
        if not self.alive() or self.proc is None or self.proc.stdin is None:
            return False
        data = (json.dumps(obj, ensure_ascii=True) + "\n").encode("utf-8")
        try:
            with self._wlock:
                self.proc.stdin.write(data)
                self.proc.stdin.flush()
            return True
        except (OSError, ValueError):
            return False

    def close_stdin(self) -> None:
        try:
            if self.proc is not None and self.proc.stdin is not None:
                self.proc.stdin.close()
        except (OSError, ValueError):
            pass

    def _read_stdout(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        for raw in iter(self.proc.stdout.readline, b""):
            s = raw.decode("utf-8", errors="ignore").strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                if not isinstance(obj, dict):
                    obj = {"kind": "_raw", "text": s}
            except ValueError:
                obj = {"kind": "_raw", "text": s}
            now = self.clock.monotonic()
            self.last_line = now
            if obj.get("kind") == "hb":
                self.last_hb = now
            if self.on_message is not None:
                try:
                    self.on_message(obj)
                except Exception:      # 받는 쪽 오류로 읽기 스레드가 죽지 않게
                    pass
        self.eof = True
        self.clock.notify()

    def _read_stderr(self) -> None:
        assert self.proc is not None and self.proc.stderr is not None
        for raw in iter(self.proc.stderr.readline, b""):
            s = raw.decode("utf-8", errors="ignore").rstrip()
            if s:
                self.stderr_tail.append(s)

    def _wait_exit(self) -> None:
        assert self.proc is not None
        self.proc.wait()
        for t in self._threads[:2]:
            t.join(2.0)          # 남은 출력까지 읽은 뒤 '끝남'을 알린다
        self.exited = True
        self.clock.notify()
        if self.on_exit is not None:
            try:
                self.on_exit(self)
            except Exception:
                pass

    # ───────── 끝내기 ─────────
    def wait(self, timeout_s: float) -> bool:
        """끝날 때까지(최대 timeout_s) 기다림. 끝났으면 True."""
        if self.proc is None:
            return True
        return self.clock.wait_until(self.clock.monotonic() + max(0.0, timeout_s), lambda: self.exited)

    def terminate(self) -> None:
        try:
            if self.proc is not None and self.proc.poll() is None:
                self.proc.terminate()
        except OSError:
            pass

    def kill(self) -> None:
        try:
            if self.proc is not None and self.proc.poll() is None:
                self.proc.kill()
        except OSError:
            pass

    def shutdown(self, graceful: bool, close_cmd: Optional[Dict[str, Any]] = None, step_s: float = 2.0) -> None:
        """v1 순서: (graceful) close → step → terminate → step → kill → step."""
        if self.proc is None:
            return
        if self.alive() and graceful and close_cmd is not None:
            self.send(close_cmd)
            self.wait(step_s)
        if not self.exited:
            self.terminate()
            self.wait(step_s)
        if not self.exited:
            self.kill()
            self.wait(step_s)
        self.close_stdin()

    def join(self, timeout_s: float = 3.0) -> None:
        for t in self._threads:
            t.join(timeout_s)
        for f in (self.proc.stdout, self.proc.stderr) if self.proc is not None else ():
            try:
                if f is not None:
                    f.close()
            except OSError:
                pass

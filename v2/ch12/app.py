"""챔버12 v2 실행 프로그램 — python -m v2.ch12 [--config-dir C:\\CH12_v2\\config] (챔버K v2/chk/app.py 와 같은 순서).

시작 순서
  1 설정 읽기(system.new_store = ConfigStore — config_active·config_prev·local_override·config_history 가 --config-dir 에).
    RUN_MODE 는 설정에서만(기본 sim, prod 는 local_override.json 에 명시). 설정을 쓸 수 없으면 시작 거부(종료 코드 2).
    파라미터 표를 만들 수 없을 때(params.ParamsError — 조각 import 실패·키 중복)도 2.
  2 한 번만 실행: 127.0.0.1:LOCAL_HMI_PORT 를 바인드해 둔다 — 실패 = 이미 실행 중 → 알리고 끝(종료 코드 3). 장비 포트를 열기 전에.
    (7단계 로컬 서버가 이 소켓을 이어받는다 — attach_server 자리.)
  3 로그(LOCAL_DATA_DIR\\logs\\ch12_v2.log 회전 로그 — 콘솔에는 시작 줄·경고·오류만).
  4 조립(system.Ch12System — sim: 시뮬레이터(sim.rig.make_sims·sim_transports, OES·RGA 워커는 --fake), bench·prod: TCP
    (system.tcp_transports)) → worker 시작 → tick 스레드. bench·prod 에서 장비가 없으면 죽지 않고 worker 재연결로 계속(링크 끊김).
    조립·시작 중 예외 = 종료 코드 4(감시 프로그램 v2/supervisor 가 다시 켠다).
  다음 단계 자리(지금은 하지 않음): 저널·이어가기(5단계 — start_journal), 웹 연결(7단계 — start_uplink),
  로컬 서버(7단계 — attach_server), 윈도우 콘솔 닫기·로그오프(winshell — install_console_handlers).
버전은 .git 의 HEAD·ref 파일을 읽어서(git 명령을 실행하지 않음), 못 읽으면 unknown.

종료(종료 코드 0)
- Ctrl+C(SIGINT·SIGBREAK): 엔진이 busy(공정·정지 순서 등 — v1 main.py closeEvent 조건)면 거부하고 계속 실행(콘솔에 이유).
  아니면 정상 종료: tick 멈춤 → 포트 닫기 → 기록·출력 flush.
- 종료 신호(SIGTERM — 감시 프로그램·서비스 정지): 거부하지 않는다(정상 종료와 같은 순서). 공정 중 파워만 끄는 처리는
  4·5단계(챔버K 결정 4 — force_exit 자리).
"""
from __future__ import annotations

import argparse
import logging
import logging.handlers
import signal
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from v2.common.clock import RealClock
from v2.common.config_store import ConfigError
from v2.common.mode import RunMode, policy, resolve_mode
from v2.ch12.system import APP_RETENTION, Ch12System, new_store, tcp_transports

PROGRAM = "ch12-v2"
DEFAULT_CONFIG_DIR = r"C:\CH12_v2\config"
LOG_MAX_BYTES = 5_000_000
LOG_BACKUPS = 10
EXIT_OK, EXIT_CONFIG, EXIT_RUNNING, EXIT_START = 0, 2, 3, 4
REFUSE_BUSY = "공정 중 — 로컬 화면·웹에서 STOP 먼저"
RUNNING_LINE = "실행 중 — 멈추려면 Ctrl+C"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def git_version(root: Optional[Path] = None) -> str:
    """.git 의 HEAD·ref 파일만 읽는다(git 명령 없음). 브랜치@SHA7, 분리 HEAD 면 SHA7, 못 읽으면 unknown."""
    try:
        g = (root or repo_root()) / ".git"
        if g.is_file():
            g = (g.parent / g.read_text(encoding="utf-8").split(":", 1)[1].strip()).resolve()
        head = (g / "HEAD").read_text(encoding="utf-8").strip()
        if not head.startswith("ref:"):
            return head[:7]
        ref = head.split(":", 1)[1].strip()
        f = g / ref
        sha = f.read_text(encoding="utf-8").strip() if f.is_file() else None
        if sha is None and (g / "packed-refs").is_file():
            for line in (g / "packed-refs").read_text(encoding="utf-8").splitlines():
                if line.endswith(" " + ref):
                    sha = line.split(" ", 1)[0]
        branch = ref[len("refs/heads/"):] if ref.startswith("refs/heads/") else ref
        return f"{branch}@{sha[:7]}" if sha else f"{branch}@unknown"
    except (OSError, IndexError, ValueError):
        return "unknown"


class App:
    def __init__(self, config_dir: str = DEFAULT_CONFIG_DIR, print_fn: Callable[[str], None] = print, clock=None) -> None:
        self.config_dir = Path(config_dir)
        self.say = print_fn
        self.clock = clock or RealClock()
        self.store = None
        self.cfg: Dict[str, Any] = {}
        self.system: Optional[Ch12System] = None
        self.sims = None
        self.instance_sock: Optional[socket.socket] = None
        self.log = logging.getLogger("ch12")
        self._log_handlers: List[logging.Handler] = []
        self._tick_thread: Optional[threading.Thread] = None
        self._done = threading.Event()
        self._signals: List[str] = []
        self.version = git_version()

    # ───────── 1 설정 ─────────
    def load_config(self) -> bool:
        from v2.ch12.params import ParamsError
        try:
            self.store = new_store(self.config_dir)
            self.cfg = dict(self.store.load())
            resolve_mode(self.cfg.get("RUN_MODE"))
        except (ConfigError, ValueError, ParamsError) as e:
            self.say(f"[중지] 설정을 쓸 수 없음 — 시작 거부: {e}")
            return False
        return True

    # ───────── 2 한 번만 실행 ─────────
    def bind_instance(self) -> bool:
        port = int(self.cfg["LOCAL_HMI_PORT"])
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sys.platform == "win32" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            s.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        try:
            s.bind(("127.0.0.1", port))
            s.listen(16)
        except OSError as e:
            s.close()
            self.say(f"[중지] 127.0.0.1:{port} 를 열 수 없음 — 이미 실행 중일 수 있습니다(장비 포트는 열지 않고 끝냅니다): {e}")
            return False
        self.instance_sock = s
        return True

    # ───────── 3 로그 ─────────
    @property
    def data_dir(self) -> Path:
        return Path(self.cfg["LOCAL_DATA_DIR"])

    def setup_logging(self) -> None:
        logs = self.data_dir / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        self.log.setLevel(logging.INFO)
        self.log.propagate = False
        fh = logging.handlers.RotatingFileHandler(logs / "ch12_v2.log", maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUPS,
                                                  encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        say = self.say

        class _Console(logging.Handler):                  # 콘솔은 경고·오류만(시작 줄은 따로)
            def emit(self, rec):
                try:
                    say(f"[{rec.levelname}] {rec.getMessage()}")
                except Exception:
                    pass
        for h in (fh, _Console(logging.WARNING)):
            self.log.addHandler(h)
            self._log_handlers.append(h)

    def _start_lines(self) -> List[str]:
        c, st = self.cfg, self.store
        mode = RunMode(c["RUN_MODE"])
        nas = c.get("NAS_COPY_DIR") if policy(mode).nas_copy and c.get("NAS_COPY_DIR") else "끔"
        return [f"챔버12 v2 — 모드 {mode.value} · 장비 {c['EQUIPMENT_ID']} · 버전 {self.version}",
                f"설정 버전 {st.version}({st.source}) · 설정 폴더 {self.config_dir}",
                f"1초 기록 {self.data_dir} · 로그 {self.data_dir / 'logs'} · v1 기록 {c['LOG_ROOT_DIR']} · NAS 사본 {nas}"]

    # ───────── 4 조립·시작 ─────────
    def build(self) -> Ch12System:
        mode = RunMode(self.cfg["RUN_MODE"])
        cam = None
        if mode is RunMode.SIM:
            from v2.ch12.sim.camera_sim import SimCamera
            from v2.ch12.sim.rig import make_sims, sim_transports
            _, self.sims = make_sims(self.clock.monotonic(), self.cfg, log_keep=APP_RETENTION.sim_log)
            tr = sim_transports(self.clock, self.sims)
            cam = SimCamera()
        else:
            tr = tcp_transports(self.clock, self.cfg)
        self.system = Ch12System(self.clock, self.cfg, self.data_dir, tr, mode=mode.value, store=self.store,
                                 retention=APP_RETENTION, nas_dir=self.cfg.get("NAS_COPY_DIR") or None, links=True,
                                 camera_backend=cam, program=PROGRAM, version=self.version)
        self.system.event_listeners.append(self._log_event)
        return self.system

    def _log_event(self, e) -> None:
        lv = {"error": logging.ERROR, "warn": logging.WARNING}.get(e.level, logging.INFO)
        self.log.log(lv, "%s %s %s", e.source, (e.code or e.reason or e.kind), e.detail)

    def start(self) -> int:
        """1~4 단계. 0 이면 실행 중(run 으로 기다림), 아니면 종료 코드."""
        if not self.load_config():
            return EXIT_CONFIG
        if not self.bind_instance():
            return EXIT_RUNNING
        try:
            self.setup_logging()
            for line in self._start_lines():
                self.say(line)
                self.log.info(line)
            for n in self.store.notes:
                self.say(f"설정: {n}")
                self.log.info("설정: %s", n)
            s = self.build()
            self.start_journal(s)
            self.start_uplink(s)
            ok = s.start(ready_timeout_s=10.0)
            msg = "장비 연결: " + ", ".join(f"{k} {'준비' if w.ready else '연결 안 됨(재시도 중)'}"
                                         for k, w in s.workers.items() if s.enabled[k])
            self.say(msg)
            (self.log.info if ok else self.log.warning)(msg)
            self._tick_thread = self.clock.start_thread(s.loop.run, "tick")
            self.attach_server(s)
            self.say(RUNNING_LINE)
        except Exception as e:
            self.say(f"[중지] 시작 실패: {e!r}")
            self.log.exception("시작 실패")
            self._teardown()
            return EXIT_START
        return EXIT_OK

    # 다음 단계 자리 — 지금은 아무것도 하지 않는다
    def start_journal(self, s: Ch12System) -> None:       # 5단계: 저널·이어가기(resume)
        return None

    def start_uplink(self, s: Ch12System) -> None:        # 7단계: 웹 연결(uplink) — prod·bench 만
        return None

    def attach_server(self, s: Ch12System) -> None:       # 7단계: 로컬 서버가 instance_sock 을 이어받는다
        return None

    def install_console_handlers(self) -> None:           # winshell(윈도우 콘솔 닫기·로그오프) 자리
        return None

    # ───────── 실행 ─────────
    def run(self) -> int:
        code = self.start()
        if code != EXIT_OK:
            return code
        self.install_handlers()
        while not self._done.is_set():
            try:
                time.sleep(0.2)
            except KeyboardInterrupt:                      # 처리기 설치 전에 온 Ctrl+C
                self._signals.append("SIGINT")
            while self._signals:
                sig = self._signals.pop(0)
                if sig == "SIGTERM" or self.on_ctrl_c() == "exit":
                    self.shutdown(sig)
                    return EXIT_OK
        return EXIT_OK

    def install_handlers(self) -> None:
        def on_int(num, frame):
            self._signals.append("SIGINT")

        def on_term(num, frame):
            self._signals.append("SIGTERM")
        signal.signal(signal.SIGINT, on_int)
        if hasattr(signal, "SIGBREAK"):
            signal.signal(signal.SIGBREAK, on_int)
        signal.signal(signal.SIGTERM, on_term)
        self.install_console_handlers()

    def exit_check(self) -> tuple:
        """("refuse", 이유) / ("ok", "")."""
        s = self.system
        if s is not None and s.busy():
            return "refuse", REFUSE_BUSY
        return "ok", ""

    def on_ctrl_c(self) -> str:
        kind, msg = self.exit_check()
        if kind == "refuse":
            self.say(f"[종료 거부] {msg}")
            self.log.warning("종료 거부: %s", msg)
            return "refused"
        return "exit"

    def shutdown(self, why: str = "SIGINT") -> None:
        self.say(f"종료 중({why}) — 포트 닫기·기록 정리")
        self.log.info("정상 종료 시작(%s)", why)
        self._teardown()
        self.say("종료")

    def _teardown(self) -> None:
        self._done.set()
        s = self.system
        if s is not None and s.started:
            s.stop()                                       # tick 멈춤 → worker 정지(포트 닫기) → 출력·기록 flush
        if self._tick_thread is not None:
            self._tick_thread.join(3.0)
        if self.instance_sock is not None:
            try:
                self.instance_sock.close()
            except OSError:
                pass
            self.instance_sock = None
        for h in self._log_handlers:
            h.flush()
            self.log.removeHandler(h)
            h.close()
        self._log_handlers = []


def main(argv=None) -> int:
    for st in (sys.stdout, sys.stderr):
        try:
            st.reconfigure(errors="replace")
        except Exception:
            pass
    ap = argparse.ArgumentParser(prog="python -m v2.ch12", description="챔버12 v2 실행 프로그램")
    ap.add_argument("--config-dir", default=DEFAULT_CONFIG_DIR,
                    help="config_active.json·config_prev.json·local_override.json·config_history.jsonl 폴더")
    a = ap.parse_args(argv)
    return App(a.config_dir, print_fn=lambda s: print(s, flush=True)).run()

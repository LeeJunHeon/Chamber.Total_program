"""챔버12 v2 실행 프로그램 — python -m v2.ch12 [--config-dir C:\\CH12_v2\\config] (챔버K v2/chk/app.py 와 같은 순서).

시작 순서
  1 설정 읽기(system.new_store = ConfigStore — config_active·config_prev·local_override·config_history 가 --config-dir 에).
    RUN_MODE 는 설정에서만(기본 sim, prod 는 local_override.json 에 명시). 설정을 쓸 수 없으면 시작 거부(종료 코드 2).
    파라미터 표를 만들 수 없을 때(params.ParamsError — 조각 import 실패·키 중복)도 2.
    웹 연결 정책(uplink_wanted): prod·bench 는 연결, sim 은 UPLINK_IN_SIM 일 때만 — 이때 EQUIPMENT_ID 가 -SIM 으로 끝나야 한다
    (CH12-SIM — 아니면 시작 거부, 종료 코드 2: 진짜 장비 기록과 섞이지 않게).
  2 한 번만 실행: 127.0.0.1:LOCAL_HMI_PORT 를 바인드해 둔다(local_server.bind_socket) — 실패 = 이미 실행 중 → 알리고 끝(종료 코드 3).
    장비 포트를 열기 전에. 로컬 서버(attach_server)가 이 소켓을 이어받는다(두 번 바인드하지 않음).
  3 로그(LOCAL_DATA_DIR\\logs\\ch12_v2.log 회전 로그 — 콘솔에는 시작 줄·경고·오류만).
  4 조립(system.Ch12System — sim: 시뮬레이터(sim.rig.make_sims·sim_transports, OES·RGA 워커는 --fake), bench·prod: TCP
    (system.tcp_transports)) → 장비 메시지 허브(hub.attach — 이벤트·1초 행 구독, 첫 메시지 hello)·설정 적용(config_apply.attach)
    → 엔진 조립(assembly.build — 공정·PC·TSP·히터·두께·프리스퍼터·안전·명령 처리 core·로봇 RobotOps·내부 명령 API)
    → 웹 연결(start_uplink) → worker 시작 → tick 스레드 → 로컬 서버(attach_server — /api/v1·v2/hmi).
    bench·prod 에서 장비가 없으면 죽지 않고 worker 재연결로 계속(링크 끊김). 조립·시작 중 예외 = 종료 코드 4(감시 프로그램이 다시 켠다).
  5 웹 연결(uplink — 계약은 common.messages): 정책이 연결이면 UPLINK_URL·UPLINK_TOKEN(local_override.json 의 비밀값)으로.
    UPLINK_URL 이 비면 경고만(웹 연결 없이 운전). 주소는 호스트만 콘솔·로그에, 토큰은 어디에도. http 주소면 경고 한 번.
    worker 시작 전에 붙는다(hello·장비 연결 이벤트부터 받게). 웹이 끊겨도 공정·안전은 노트북에서 그대로.
  다음 단계 자리(지금은 하지 않음): 저널·이어가기(5단계 — start_journal, 웹 명령 id 기억은 uplink.to_state/from_state),
  윈도우 콘솔 닫기·로그오프(winshell — install_console_handlers).
버전은 .git 의 HEAD·ref 파일을 읽어서(git 명령을 실행하지 않음), 못 읽으면 unknown.

종료(종료 코드 0)
- Ctrl+C(SIGINT·SIGBREAK): 엔진이 busy(공정·정지 순서 등 — v1 main.py closeEvent 조건)면 거부하고 계속 실행(콘솔에 이유).
  아니면 정상 종료: tick 멈춤 → 포트 닫기 → 기록·출력 flush → 허브 닫기 → 웹 연결 정지(남은 메시지는 보관함 — 다음 시작 때
  올림) → 로컬 서버 정지.
- 종료 신호(SIGTERM — 감시 프로그램·서비스 정지): 거부하지 않는다(정상 종료와 같은 순서). 공정 중이면 먼저 파워 출력만 끈다
  (safety.power_off_only — 챔버K 결정 4, S32). 콘솔 닫기·로그오프(winshell)도 같은 함수를 부를 자리(5단계).
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
from urllib.parse import urlsplit

from v2.common.clock import RealClock
from v2.common.config_store import ConfigError
from v2.common.mode import RunMode, policy, resolve_mode
from v2.ch12.local_server import LocalServer, bind_socket
from v2.ch12.system import APP_RETENTION, Ch12System, new_store, tcp_transports

PROGRAM = "ch12-v2"
DEFAULT_CONFIG_DIR = r"C:\CH12_v2\config"
LOG_MAX_BYTES = 5_000_000
LOG_BACKUPS = 10
EXIT_OK, EXIT_CONFIG, EXIT_RUNNING, EXIT_START = 0, 2, 3, 4
REFUSE_BUSY = "공정 중 — 로컬 화면·웹에서 STOP 먼저"
RUNNING_LINE = "실행 중 — 멈추려면 Ctrl+C"
SIM_ID_SUFFIX = "-SIM"               # UPLINK_IN_SIM 일 때 장비 ID 끝
REPO_HMI_DIR = Path(__file__).resolve().parents[1] / "hmi"   # LOCAL_HMI_STATIC_DIR 이 비면 — 이 저장소의 v2/hmi


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
        self.hub = None
        self.server: Optional[LocalServer] = None
        self.uplink = None
        self.assembly = None

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
        if (self.uplink_wanted() and RunMode(self.cfg["RUN_MODE"]) is RunMode.SIM
                and not str(self.cfg["EQUIPMENT_ID"]).endswith(SIM_ID_SUFFIX)):
            self.say(f"[중지] UPLINK_IN_SIM(sim 웹 연결)인데 EQUIPMENT_ID {self.cfg['EQUIPMENT_ID']} 가 {SIM_ID_SUFFIX} 으로 끝나지 않음 "
                     "— 진짜 장비 기록과 섞이지 않게 시작 거부")
            return False
        return True

    def uplink_wanted(self) -> bool:
        """웹 연결 정책 — prod·bench 는 연결, sim 은 UPLINK_IN_SIM 일 때만."""
        mode = RunMode(self.cfg["RUN_MODE"])
        return bool(policy(mode).uplink or (mode is RunMode.SIM and self.cfg.get("UPLINK_IN_SIM")))

    @property
    def hmi_dir(self) -> Path:
        """로컬 화면 파일 폴더 — LOCAL_HMI_STATIC_DIR, 비면 이 저장소의 v2/hmi."""
        v = str(self.cfg.get("LOCAL_HMI_STATIC_DIR") or "")
        return Path(v) if v else REPO_HMI_DIR

    # ───────── 2 한 번만 실행 ─────────
    def bind_instance(self) -> bool:
        port = int(self.cfg["LOCAL_HMI_PORT"])
        try:
            s = bind_socket(port)
        except OSError as e:
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
                f"1초 기록 {self.data_dir} · 로그 {self.data_dir / 'logs'} · v1 기록 {c['LOG_ROOT_DIR']} · NAS 사본 {nas}",
                f"로컬 화면 http://127.0.0.1:{c['LOCAL_HMI_PORT']}/ · 화면 파일 {self.hmi_dir}",
                f"웹 연결 {self._uplink_line()}"]

    def _uplink_line(self) -> str:
        if not self.uplink_wanted():
            return "끔(sim)"
        url = str(self.cfg.get("UPLINK_URL") or "")
        if not url:
            return "주소 없음 — 연결 안 함"
        u = urlsplit(url)
        return f"{u.hostname}({u.scheme})"                   # 호스트만 — 경로·토큰은 쓰지 않는다

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
        from v2.ch12 import config_apply, hub
        from v2.ch12.assembly import build as build_engines
        self.hub = hub.attach(self.system)                 # 이벤트·1초 행 → 장비 메시지(로컬 화면·웹)
        config_apply.attach(self.system)                   # CONFIG_APPLY·CONFIG_ROLLBACK·idle 키 적용
        # 엔진 전부·안전·명령 처리(core — system.command)·로봇(RobotOps·내부 명령 API 127.0.0.1:ROBOT_API_PORT) — 시험대
        # (sim.rig.Ch12FullRig)와 같은 조립. sim 은 v1 엑셀 메인 사본(G: 공유 드라이브)을 기록 폴더 아래로.
        xl = self.data_dir / "gdrive_main" if mode is RunMode.SIM else None
        self.assembly = build_engines(self.system, excel_main_dir=xl)
        self.hub.hello()                                   # 첫 메시지(장비 연결 이벤트보다 먼저)
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
    def start_journal(self, s: Ch12System) -> None:
        """5단계 저널·이어가기(journal.py·resume.py) — 조립 뒤·worker 시작 전: 옛 저널을 읽고(공정 중이었던 부분은 판단 전까지
        덮어쓰지 않음·재시작 횟수 +1·히터 넘겨받기·명령 id 기억 되살림) 1초 저널을 켠다. 판단은 시작 뒤 별도 스레드
        (1초 행 몇 개를 다시 읽은 뒤 — 이어가기 또는 PROGRAM_RESTART 안전 정지). 웹 연결을 만든 직후 명령 기억을 넣는다."""
        from v2.ch12 import resume as RS
        say = lambda m: (self.say(f"[재시작] {m}"), self.log.warning("재시작: %s", m))   # noqa: E731
        self.journal, self.resumer = RS.attach(s, log=say)
        for part, d in self.resumer.active.items():
            say(f"지난 실행의 {part} 공정(run {d.get('run_id')})이 저널에 '공정 중' — 장비를 다시 읽고 이어가기/안전 정지 판단")
        orig_uplink = self.start_uplink

        def start_uplink(sys_) -> None:
            orig_uplink(sys_)
            self.journal.bind_commands()                   # 웹 명령 id 기억 — 연결이 명령을 받기 전에
        self.start_uplink = start_uplink
        th = threading.Thread(target=lambda: (s.clock.wait_until(None, lambda: s.started or self._done.is_set()),
                                              None if self._done.is_set() else self.resumer.run()),
                              name="resume", daemon=True)
        th.start()

    def start_uplink(self, s: Ch12System) -> None:
        """웹 연결 — 정책이 연결일 때만(prod·bench, sim 은 UPLINK_IN_SIM). 실패해도 웹 없이 운전(공정·안전은 그대로)."""
        if not self.uplink_wanted():
            return
        url = str(self.cfg.get("UPLINK_URL") or "")
        if not url:
            self.log.warning("웹 연결: UPLINK_URL 이 비어 있음 — 웹 연결 없이 운전(local_override.json 에 넣고 다시 시작)")
            return
        if urlsplit(url).scheme != "https":
            self.log.warning("웹 연결: 주소가 https 가 아님(%s) — 암호화·인증서 확인 없음, 시험용으로만", urlsplit(url).hostname)
        from v2.ch12.uplink import Uplink
        try:
            up = Uplink(s, url, str(self.cfg.get("UPLINK_TOKEN") or ""), self.data_dir)
        except Exception as e:                             # 보관함 폴더를 못 쓰는 등 — 웹 없이 운전
            self.log.warning("웹 연결 준비 실패 — 웹 연결 없이 운전: %s", type(e).__name__)
            return
        self.uplink = s.uplink = up
        up.start()

    def attach_server(self, s: Ch12System) -> None:
        """로컬 서버 — 2단계에서 잡아 둔 소켓을 이어받아 /api/v1·v2/hmi 를 연다."""
        sock, self.instance_sock = self.instance_sock, None
        self.server = LocalServer(int(self.cfg["LOCAL_HMI_PORT"]), str(self.hmi_dir), sock=sock)
        self.server.attach(s)
        self.server.start()

    def install_console_handlers(self) -> None:
        """의도적 종료(5단계 — resume.intentional_exit: 저널 표시 먼저 → 공정 중단 PROGRAM_EXIT → 파워만 끔 → 저널 마지막).
        - 정상 종료 경로(Ctrl+C 를 받아들였을 때·SIGTERM — 리눅스·윈도우 같음): shutdown 앞에 끼운다(종료 코드 0).
        - 윈도우 콘솔 창 닫기·로그오프·윈도우 종료(winshell — SetConsoleCtrlHandler·숨은 창 WM_ENDSESSION): 처리기 스레드에서
          같은 처리 → 기록 flush → os._exit(0)(감시 프로그램이 다시 켜지 않음 — 창 닫기는 약 5초 안에 끝나야 한다)."""
        import os
        from v2.ch12 import resume as RS
        from v2.ch12 import winshell
        say = lambda m: (self.say(m), self.log.warning("%s", m))                # noqa: E731

        def exit_now(why: str):
            s = self.system
            if s is None:
                return None
            return RS.intentional_exit(s, getattr(self, "journal", None), why, log=say)
        orig_shutdown = self.shutdown

        def shutdown(why: str = "SIGINT") -> None:
            exit_now(why)
            orig_shutdown(why)
            j = getattr(self, "journal", None)
            if j is not None:
                j.stop()
            if getattr(self, "shell", None) is not None:
                self.shell.stop()
        self.shutdown = shutdown

        def on_console(why: str) -> None:
            exit_now(why)
            s = self.system
            try:
                if s is not None and s.recorder is not None:
                    s.recorder.flush(0.5)
                for h in self._log_handlers:
                    h.flush()
            finally:
                os._exit(EXIT_OK)
        self.shell = winshell.install(on_console, say)

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
            if self.system is not None and (getattr(self.system, "cfg", None) or {}).get("LOG_ROOT_DIR"):
                # v1 종료 차단 때 덤프 1회(close_blocked_while_running — main.py:1500-1505). 기록 폴더가 정해진 조립에서만
                from v2.ch12.runtime_dump_hook import dump_close_blocked
                dump_close_blocked(self.system)
            return "refused"
        return "exit"

    def shutdown(self, why: str = "SIGINT") -> None:
        self.say(f"종료 중({why}) — 포트 닫기·기록 정리")
        self.log.info("정상 종료 시작(%s)", why)
        s = self.system
        if why == "SIGTERM" and s is not None and s.busy() and getattr(s, "safety", None) is not None:
            res = s.safety.power_off_only("PROGRAM_EXIT")      # 의도적 종료 — 파워 출력만(챔버K 결정 4, safety S32)
            self.log.warning("의도적 종료(%s) — 파워만 끔: %s", why, res)
        self._teardown()
        self.say("종료")

    def _teardown(self) -> None:
        self._done.set()
        s = self.system
        if s is not None and s.started:
            s.stop()                                       # tick 멈춤 → worker 정지(포트 닫기) → 출력·기록 flush
        if self._tick_thread is not None:
            self._tick_thread.join(3.0)
        if self.hub is not None:
            self.hub.close()                               # 소비자(로컬 화면 SSE·웹 연결) 끝
        if self.uplink is not None:
            self.uplink.stop()                             # 남은 메시지는 보관함에(다음 시작 때 올림)
        if self.server is not None:
            self.server.stop()
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

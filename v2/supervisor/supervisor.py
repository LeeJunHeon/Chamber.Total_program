"""감시·재시작 프로그램(사용자 지시 3장 (6)) — 제어 프로그램(python -m v2.ch12)을 자식 프로세스로 켜고 지킨다.

규칙(사용자와 합의)
- 자식이 비정상 종료(종료 코드 ≠ 0)하면 RESTART_DELAY_S(기본 3 s) 뒤 다시 켠다.
- 최근 CRASH_WINDOW_S(600 s = 10분) 안에 비정상 종료가 CRASH_LIMIT(3)번을 넘으면(4번째) 재시작을 멈추고 기록한 뒤
  종료 코드 EXIT_GAVE_UP 으로 끝난다(장비 끊김 알림은 포털 몫 — 여기서는 알림을 보내지 않는다).
- 정상 종료(종료 코드 0 — 사람이 끔·의도적 종료)면 다시 켜지 않고 같이 끝난다.
  설정 거부(2)·이미 실행 중(3)도 다시 켜도 같으므로 재시작하지 않는다(NO_RESTART_CODES).
- 감시 프로그램 자신이 Ctrl+C·콘솔 닫기를 받으면(윈도우) 넘기지 않고(같은 콘솔 그룹이므로 자식도 같은 신호를 받는다 — 중복 방지)
  자식이 끝나기를 기다린다 — 자식이 의도적 종료를 저널에 먼저 적는다(이어가기 금지). CTRL_BREAK 도 그대로(처리기를 달지 않음).
- 리눅스·맥: 감시 프로그램이 SIGTERM·SIGINT·SIGHUP 을 받으면(서비스 정지·kill <pid> — 자식은 그 신호를 받지 못한다) 자식에게
  SIGTERM 을 넘긴다(자식은 SIGTERM 을 의도적 종료로 처리 — 거부하지 않음, 공정 중이면 파워만 끔). 터미널 Ctrl+C 처럼 자식도
  SIGINT 를 같이 받은 경우에도 넘긴다(자식이 공정 중이라 Ctrl+C 를 거부해도 감시 프로그램을 끄라는 뜻이므로 의도적 종료로 —
  리눅스는 개발·sim 용). 자식이 STOP_WAIT_S 안에 끝나지 않으면 kill. 자식 종료 코드로 끝나며 다시 켜지 않는다(stop_requested).
  STOP_WAIT_S = 30 s 근거: 자식의 의도적 종료 한도(파워 끄기 결과 기다림 safety FORCE_DEADLINE_S 4 s) + 정리 대기 상한
  (PLC 남은 요청 2 s·카메라 3 s·tick 3 s·재시작 판단 3 s = 11 s) + worker 정지 대기(하나 5 s — 보통 바로 끝남) 몇 개 ≈ 20 s 보다
  길게, systemd 기본 정지 한도(TimeoutStopSec 90 s)보다는 짧게(감시 프로그램이 먼저 자식을 정리하고 끝나게).
- 윈도우가 아닌 곳에서 --log-dir 이 윈도우 경로(기본 'C:\\Ch12_v2\\logs')면 홈 아래로 바꿔 쓴다(v2.common.winpaths — 시작 로그에 한 줄).
- 기록: 로그 파일 supervisor.log(LOG_DIR) 에 시작·종료 코드·재시작·포기를 한 줄씩, 상태 파일 supervisor_state.json 에
  {pid, child_pid, starts, crashes[시각], gave_up, last_exit} — 제어 프로그램이 다시 켜졌을 때 '몇 번째 재시작인지'를 읽는다
  (저널의 이어가기 조건 ② '이 공정에서 첫 재시작'은 저널이 직접 센다 — 이 파일은 참고·화면용).
- 자식에게 환경 변수 CH12_SUPERVISED=1, CH12_RESTART_COUNT=<n>, CH12_HEARTBEAT_FILE(<log_dir>/heartbeat.json)·CH12_HANG_DUMP_FILE(<log_dir>/hang_dump.log)을 준다.
- 멈춤 감지(2026-11-13 D408) — 꺼지지 않고 멈춘 자식(로봇 서버는 끝까지 기다리므로 로봇도 멈춘다)을 되살린다.
  자식(v2/ch12/health_beacon)이 1 s 마다 건강 확인 파일에 1초 tick 번호(tick)와 명령 코어 확인 횟수(cmd)·설정을 쓴다.
  감시 프로그램이 HEALTH_INTERVAL_S(설정 SUPERVISOR_HEALTH_INTERVAL_S, 기본 10 s)마다 파일을 읽어 tick 또는 cmd 가 SUPERVISOR_HANG_TIMEOUT_S(기본 60 s)
  동안 움직이지 않으면(HTTP 스레드가 응답해도 핵심 루프가 멈춘 것) 멈춤: 자식이 남긴 스레드 덤프(hang_dump.log 끝)를 supervisor.log 에 옮기고 →
  강제 종료(kill) → 비정상 종료와 같게 다시 켠다(재시작 한도 계산에 포함, 사유 '멈춤'). 시작 직후 SUPERVISOR_START_GRACE_S(기본 300 s) 동안은
  '준비됨' 표시가 나올 때까지 판단하지 않고, 그 시간이 지나도 준비 표시가 없으면 멈춤으로 본다. SUPERVISOR_HANG_ENABLED=false 면 감지 안 함.
  종료 신호를 받은 뒤(의도적 종료 중)에는 감지하지 않는다. 설정 값은 파일(자식이 cfg 에서 읽어 씀)에서 읽으므로 웹에서 바꾸면 다음 확인부터 적용.
v1 apps/recovery_service 는 메인이 쓰지 않아 동작하지 않던 것이다 — 참고만 했다(launcher.py 의 schtasks 등록 방식).
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from typing import Callable, Deque, List, Optional

RESTART_DELAY_S = 3.0
CRASH_WINDOW_S = 600.0
CRASH_LIMIT = 3
EXIT_OK, EXIT_GAVE_UP = 0, 10
NO_RESTART_CODES = frozenset({0, 2, 3})       # 정상·설정 거부·이미 실행 중(v2.ch12.app 종료 코드)
DEFAULT_LOG_DIR = r"C:\Ch12_v2\logs"
STOP_WAIT_S = 30.0                            # 신호를 넘긴 뒤 자식 종료를 기다리는 상한(넘으면 kill — 근거는 머리글)
HEALTH_INTERVAL_S, HANG_TIMEOUT_S, START_GRACE_S, HANG_ENABLED = 10.0, 60.0, 300.0, True   # 건강 확인 파일에 설정이 오기 전의 기본값
POLL_S = 0.2                                  # 자식 기다림 간격(신호를 받았는지 보는 주기)
FORWARD_SIGNALS = ("SIGTERM", "SIGINT", "SIGHUP")   # 리눅스·맥에서 받으면 자식에게 SIGTERM 을 넘기는 신호


class Supervisor:
    def __init__(self, cmd: List[str], log_dir: Path, restart_delay_s: float = RESTART_DELAY_S,
                 crash_window_s: float = CRASH_WINDOW_S, crash_limit: int = CRASH_LIMIT,
                 now: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
                 popen: Callable = subprocess.Popen, say: Callable[[str], None] = print,
                 stop_wait_s: float = STOP_WAIT_S, health_interval_s: Optional[float] = None, hang_timeout_s: Optional[float] = None,
                 start_grace_s: Optional[float] = None, hang_enabled: Optional[bool] = None, hb_now: Callable[[], float] = time.monotonic) -> None:
        self.cmd = list(cmd)
        self.log_dir = Path(log_dir)
        self.restart_delay_s = restart_delay_s
        self.crash_window_s = crash_window_s
        self.crash_limit = crash_limit
        self.now, self.sleep, self.popen, self.say = now, sleep, popen, say
        self.crashes: Deque[float] = deque()
        self.starts = 0
        self.gave_up = False
        self.last_exit: Optional[int] = None
        self.child = None
        self.stop_requested = False
        self.stop_wait_s = stop_wait_s
        self.stop_signal: Optional[str] = None       # 받은 종료 신호 이름(신호 처리기가 적고 기다림 고리가 자식에게 넘긴다)
        self.notes: List[str] = []                   # 시작 로그에 남길 줄(윈도우 경로 바꿔 씀 등)
        # 멈춤 감지 — None 이면 건강 확인 파일의 설정(웹에서 바꾼 값)을 쓰고, 값이 있으면(명령줄·시험) 그 값 고정
        self._ov = {"interval": health_interval_s, "timeout": hang_timeout_s, "grace": start_grace_s, "enabled": hang_enabled}
        self.hb_now = hb_now
        self.hb_path = self.log_dir / "heartbeat.json"
        self.dump_path = self.log_dir / "hang_dump.log"
        self.hangs = 0
        self.hang_killed = False
        self._hb_reset()

    # ── 기록 ──
    def log(self, msg: str) -> None:
        line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
        self.say(f"[감시] {msg}")
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            with open(self.log_dir / "supervisor.log", "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            pass

    def save_state(self) -> None:
        d = {"pid": os.getpid(), "child_pid": getattr(self.child, "pid", None), "starts": self.starts,
             "crashes": [round(t, 3) for t in self.crashes], "gave_up": self.gave_up, "last_exit": self.last_exit, "hangs": self.hangs,
             "saved_at": time.strftime("%Y-%m-%dT%H:%M:%S")}
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            tmp = self.log_dir / "supervisor_state.json.tmp"
            tmp.write_text(json.dumps(d, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp, self.log_dir / "supervisor_state.json")
        except OSError:
            pass

    # ── 멈춤 감지 ──
    def _hb_reset(self) -> None:
        t = self.hb_now()
        self._hb_start = t
        self._hb_pid: Optional[int] = None
        self._hb_ready = False
        self._hb_tick: Optional[int] = None
        self._hb_cmd: Optional[int] = None
        self._hb_tick_t = self._hb_cmd_t = t
        self._hb_next = t
        self._hb_cfg: dict = {}
        self._dump_off = self.dump_path.stat().st_size if self.dump_path.exists() else 0

    def _hv(self, key: str, default):
        v = self._ov.get(key)
        return v if v is not None else self._hb_cfg.get(key, default)

    def health_interval(self) -> float:
        return max(0.05, float(self._hv("interval", HEALTH_INTERVAL_S)))

    def _read_hb(self) -> Optional[dict]:
        try:
            d = json.loads(self.hb_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return d if isinstance(d, dict) and d.get("pid") == getattr(self.child, "pid", None) else None

    def check_health(self) -> Optional[str]:
        """건강 확인 한 번 — 멈춤이면 사유 글자, 아니면 None. (시험에서 직접 부른다)"""
        now = self.hb_now()
        d = self._read_hb()
        if d is not None:
            self._hb_cfg = {k: d[k] for k in ("enabled", "interval", "timeout", "grace") if k in d}
            if d.get("ready") and not self._hb_ready:
                self._hb_ready = True
                self._hb_tick_t = self._hb_cmd_t = now
                self._hb_tick, self._hb_cmd = d.get("tick"), d.get("cmd")
            elif self._hb_ready:
                if d.get("tick") != self._hb_tick:
                    self._hb_tick, self._hb_tick_t = d.get("tick"), now
                if d.get("cmd") != self._hb_cmd:
                    self._hb_cmd, self._hb_cmd_t = d.get("cmd"), now
        if not bool(self._hv("enabled", HANG_ENABLED)):
            self._hb_tick_t = self._hb_cmd_t = now              # 끈 동안은 기준을 지금으로(켜자마자 오래된 기준으로 죽이지 않게)
            return None
        timeout, grace = float(self._hv("timeout", HANG_TIMEOUT_S)), float(self._hv("grace", START_GRACE_S))
        if not self._hb_ready:
            if now - self._hb_start >= grace:
                return f"시작 후 {grace:g} s 안에 준비 표시가 없음"
            return None
        if now - self._hb_tick_t >= timeout:
            return f"1초 tick 번호가 {timeout:g} s 동안 움직이지 않음(tick={self._hb_tick})"
        if now - self._hb_cmd_t >= timeout:
            return f"명령 처리(코어 잠금)가 {timeout:g} s 동안 진행 없음(cmd={self._hb_cmd})"
        return None

    def _dump_tail(self, max_lines: int = 120) -> List[str]:
        try:
            with open(self.dump_path, "rb") as f:
                f.seek(self._dump_off)
                text = f.read().decode("utf-8", "replace")
        except OSError:
            return []
        lines = text.splitlines()
        return lines[-max_lines:]

    def kill_hung(self, why: str) -> None:
        self.hangs += 1
        self.hang_killed = True
        self.log(f"[멈춤] 제어 프로그램 pid={self.child.pid} — {why} → 스레드 덤프를 남기고 강제 종료")
        tail = self._dump_tail()
        if tail:
            for ln in tail:
                self.log("  | " + ln)
        else:
            self.log("  (덤프 파일에 내용 없음 — 자식이 덤프를 쓰기 전에 멈췄거나 덤프가 꺼져 있음)")
        try:
            self.child.kill()
        except OSError:
            pass

    # ── 판단 ──
    def record_crash(self, t: float) -> bool:
        """비정상 종료 하나 — 창 안의 횟수가 한도를 넘으면 True(포기)."""
        self.crashes.append(t)
        while self.crashes and t - self.crashes[0] > self.crash_window_s:
            self.crashes.popleft()
        return len(self.crashes) > self.crash_limit

    # ── 종료 신호(리눅스·맥) ──
    def install_signals(self) -> bool:
        """SIGTERM·SIGINT·SIGHUP 처리기(메인 스레드에서만 — main 이 부른다). 윈도우는 달지 않는다(콘솔 그룹 Ctrl+C·CTRL_BREAK 를
        자식도 같이 받으므로 지금처럼 KeyboardInterrupt 로 기다리기만 — 넘기면 중복). 달았으면 True."""
        if os.name == "nt":
            return False
        for name in FORWARD_SIGNALS:
            num = getattr(signal, name, None)
            if num is not None:
                signal.signal(num, self._on_signal)
        return True

    def _on_signal(self, num, frame) -> None:
        # 처리기에서는 표시만(로그·자식 신호는 기다림 고리에서 — 처리기 안에서 파일·잠금을 건드리지 않게)
        self.stop_requested = True
        if self.stop_signal is None:
            try:
                self.stop_signal = signal.Signals(num).name
            except ValueError:
                self.stop_signal = str(num)

    def _wait_child(self) -> int:
        """자식이 끝나기를 기다린다. 종료 신호를 받았으면 자식에게 SIGTERM 을 한 번 넘기고 stop_wait_s 안에 안 끝나면 kill."""
        deadline: Optional[float] = None
        while True:
            if self.stop_signal is not None and deadline is None:
                deadline = time.monotonic() + self.stop_wait_s
                self.log(f"{self.stop_signal} 받음 — 제어 프로그램 pid={self.child.pid} 에 SIGTERM 을 넘기고 "
                         f"종료를 기다림(최대 {self.stop_wait_s:g} s)")
                try:
                    self.child.send_signal(signal.SIGTERM)
                except OSError:                      # 이미 끝남
                    pass
            if deadline is not None and time.monotonic() > deadline:
                self.log(f"제어 프로그램이 {self.stop_wait_s:g} s 안에 끝나지 않음 — 강제 종료(kill)")
                try:
                    self.child.kill()
                except OSError:
                    pass
                return self.child.wait()
            if (self.stop_signal is None and not self.stop_requested and not self.hang_killed
                    and self.hb_now() >= self._hb_next):
                self._hb_next = self.hb_now() + self.health_interval()
                why = self.check_health()
                if why:
                    self.kill_hung(why)
            try:
                return self.child.wait(timeout=POLL_S)
            except subprocess.TimeoutExpired:
                continue
            except KeyboardInterrupt:                # (윈도우) 자식도 같은 Ctrl+C 를 받는다 — 넘기지 않고 끝나기를 기다린다
                self.stop_requested = True
                self.log("Ctrl+C — 제어 프로그램 종료를 기다림")

    def run_once(self) -> int:
        env = dict(os.environ, CH12_SUPERVISED="1", CH12_RESTART_COUNT=str(self.starts),
                   CH12_HEARTBEAT_FILE=str(self.hb_path), CH12_HANG_DUMP_FILE=str(self.dump_path))
        self.starts += 1
        self.hang_killed = False
        try:
            self.hb_path.unlink()                    # 이전 자식의 확인 파일이 남아 새 자식이 준비된 것으로 보이지 않게
        except OSError:
            pass
        self.child = self.popen(self.cmd, env=env)
        self._hb_reset()
        self.log(f"시작 #{self.starts} pid={self.child.pid} cmd={' '.join(self.cmd)}")
        self.save_state()
        code = self._wait_child()
        self.last_exit = code
        if self.hang_killed:
            self.log(f"멈춤으로 강제 종료됨(종료 코드 {code}) — 멈춤 {self.hangs}번째")
        return code

    def run(self) -> int:
        for n in self.notes:
            self.log(n)
        while True:
            if self.stop_requested and self.starts:  # 재시작 대기 중에 종료 신호 — 다시 켜지 않는다
                self.log(f"{self.stop_signal or '종료 요청'} — 다시 켜지 않음")
                self.save_state()
                return _exit_code(self.last_exit)
            code = self.run_once()
            if code in NO_RESTART_CODES or self.stop_requested:
                self.log(f"종료 코드 {code} — 다시 켜지 않음")
                self.save_state()
                return _exit_code(code)
            if self.record_crash(self.now()):
                self.gave_up = True
                self.log(f"비정상 종료 코드 {code} — 최근 {self.crash_window_s / 60:g}분에 {len(self.crashes)}번"
                         f"(한도 {self.crash_limit}번 초과) — 재시작을 멈춤")
                self.save_state()
                return EXIT_GAVE_UP
            self.log(f"{'멈춤 강제 종료' if self.hang_killed else '비정상 종료'} 코드 {code} — {self.restart_delay_s:g} s 뒤 다시 켬"
                     f"(최근 {self.crash_window_s / 60:g}분 {len(self.crashes)}번)")
            self.save_state()
            self.sleep(self.restart_delay_s)


def _exit_code(code: Optional[int]) -> int:
    """감시 프로그램 자신의 종료 코드 — 자식 코드 그대로(0 = 정상). 신호로 죽은 자식(음수 -N)은 셸 관례 128+N."""
    if code is None or code == 0:
        return EXIT_OK
    return 128 - code if code < 0 else code


def child_cmd(extra: List[str]) -> List[str]:
    return [sys.executable, "-m", "v2.ch12"] + list(extra)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m v2.supervisor", description="챔버12 제어 프로그램 감시·재시작")
    ap.add_argument("--log-dir", default=DEFAULT_LOG_DIR)
    ap.add_argument("--restart-delay", type=float, default=RESTART_DELAY_S)
    ap.add_argument("--hang-timeout", type=float, default=None, help="멈춤 판단 한도(s) — 주면 설정 SUPERVISOR_HANG_TIMEOUT_S 대신(시험용)")
    ap.add_argument("--health-interval", type=float, default=None, help="건강 확인 주기(s) — 주면 설정 대신(시험용)")
    ap.add_argument("--no-hang-check", action="store_true", help="멈춤 감지 끔(설정 SUPERVISOR_HANG_ENABLED=false 와 같다)")
    ap.add_argument("rest", nargs=argparse.REMAINDER, help="-- 뒤는 제어 프로그램 인자")
    a = ap.parse_args(argv)
    rest = a.rest[1:] if a.rest[:1] == ["--"] else a.rest
    from v2.common import winpaths                 # 윈도우가 아니면 윈도우 경로 --log-dir 를 홈 아래로(윈도우는 그대로)
    log_dir = winpaths.runtime_path(a.log_dir)
    sup = Supervisor(child_cmd(rest), Path(log_dir), restart_delay_s=a.restart_delay, hang_timeout_s=a.hang_timeout,
                     health_interval_s=a.health_interval, hang_enabled=(False if a.no_hang_check else None))
    if log_dir != a.log_dir:
        sup.notes.append(winpaths.note(["--log-dir"]) + f" — {a.log_dir} → {log_dir}")
    sup.install_signals()
    return sup.run()

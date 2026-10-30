"""감시·재시작 프로그램(사용자 지시 3장 (6)) — 제어 프로그램(python -m v2.ch12)을 자식 프로세스로 켜고 지킨다.

규칙(사용자와 합의)
- 자식이 비정상 종료(종료 코드 ≠ 0)하면 RESTART_DELAY_S(기본 3 s) 뒤 다시 켠다.
- 최근 CRASH_WINDOW_S(600 s = 10분) 안에 비정상 종료가 CRASH_LIMIT(3)번을 넘으면(4번째) 재시작을 멈추고 기록한 뒤
  종료 코드 EXIT_GAVE_UP 으로 끝난다(장비 끊김 알림은 포털 몫 — 여기서는 알림을 보내지 않는다).
- 정상 종료(종료 코드 0 — 사람이 끔·의도적 종료)면 다시 켜지 않고 같이 끝난다.
  설정 거부(2)·이미 실행 중(3)도 다시 켜도 같으므로 재시작하지 않는다(NO_RESTART_CODES).
- 감시 프로그램 자신이 Ctrl+C·콘솔 닫기를 받으면 자식에게 그대로 넘기고(같은 콘솔 그룹이므로 자식도 같은 신호를 받는다)
  자식이 끝나기를 기다린다 — 자식이 의도적 종료를 저널에 먼저 적는다(이어가기 금지).
- 기록: 로그 파일 supervisor.log(LOG_DIR) 에 시작·종료 코드·재시작·포기를 한 줄씩, 상태 파일 supervisor_state.json 에
  {pid, child_pid, starts, crashes[시각], gave_up, last_exit} — 제어 프로그램이 다시 켜졌을 때 '몇 번째 재시작인지'를 읽는다
  (저널의 이어가기 조건 ② '이 공정에서 첫 재시작'은 저널이 직접 센다 — 이 파일은 참고·화면용).
- 자식에게 환경 변수 CH12_SUPERVISED=1, CH12_RESTART_COUNT=<n> 을 준다.
v1 apps/recovery_service 는 메인이 쓰지 않아 동작하지 않던 것이다 — 참고만 했다(launcher.py 의 schtasks 등록 방식).
"""
from __future__ import annotations

import argparse
import json
import os
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


class Supervisor:
    def __init__(self, cmd: List[str], log_dir: Path, restart_delay_s: float = RESTART_DELAY_S,
                 crash_window_s: float = CRASH_WINDOW_S, crash_limit: int = CRASH_LIMIT,
                 now: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
                 popen: Callable = subprocess.Popen, say: Callable[[str], None] = print) -> None:
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
             "crashes": [round(t, 3) for t in self.crashes], "gave_up": self.gave_up, "last_exit": self.last_exit,
             "saved_at": time.strftime("%Y-%m-%dT%H:%M:%S")}
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            tmp = self.log_dir / "supervisor_state.json.tmp"
            tmp.write_text(json.dumps(d, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp, self.log_dir / "supervisor_state.json")
        except OSError:
            pass

    # ── 판단 ──
    def record_crash(self, t: float) -> bool:
        """비정상 종료 하나 — 창 안의 횟수가 한도를 넘으면 True(포기)."""
        self.crashes.append(t)
        while self.crashes and t - self.crashes[0] > self.crash_window_s:
            self.crashes.popleft()
        return len(self.crashes) > self.crash_limit

    def run_once(self) -> int:
        env = dict(os.environ, CH12_SUPERVISED="1", CH12_RESTART_COUNT=str(self.starts))
        self.starts += 1
        self.child = self.popen(self.cmd, env=env)
        self.log(f"시작 #{self.starts} pid={self.child.pid} cmd={' '.join(self.cmd)}")
        self.save_state()
        while True:
            try:
                code = self.child.wait()
                break
            except KeyboardInterrupt:                # 자식도 같은 Ctrl+C 를 받는다 — 끝나기를 기다린다
                self.stop_requested = True
                self.log("Ctrl+C — 제어 프로그램 종료를 기다림")
        self.last_exit = code
        return code

    def run(self) -> int:
        while True:
            code = self.run_once()
            if code in NO_RESTART_CODES or self.stop_requested:
                self.log(f"종료 코드 {code} — 다시 켜지 않음")
                self.save_state()
                return EXIT_OK if code == 0 else code
            if self.record_crash(self.now()):
                self.gave_up = True
                self.log(f"비정상 종료 코드 {code} — 최근 {self.crash_window_s / 60:g}분에 {len(self.crashes)}번"
                         f"(한도 {self.crash_limit}번 초과) — 재시작을 멈춤")
                self.save_state()
                return EXIT_GAVE_UP
            self.log(f"비정상 종료 코드 {code} — {self.restart_delay_s:g} s 뒤 다시 켬"
                     f"(최근 {self.crash_window_s / 60:g}분 {len(self.crashes)}번)")
            self.save_state()
            self.sleep(self.restart_delay_s)


def child_cmd(extra: List[str]) -> List[str]:
    return [sys.executable, "-m", "v2.ch12"] + list(extra)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m v2.supervisor", description="챔버12 제어 프로그램 감시·재시작")
    ap.add_argument("--log-dir", default=DEFAULT_LOG_DIR)
    ap.add_argument("--restart-delay", type=float, default=RESTART_DELAY_S)
    ap.add_argument("rest", nargs=argparse.REMAINDER, help="-- 뒤는 제어 프로그램 인자")
    a = ap.parse_args(argv)
    rest = a.rest[1:] if a.rest[:1] == ["--"] else a.rest
    return Supervisor(child_cmd(rest), Path(a.log_dir), restart_delay_s=a.restart_delay).run()

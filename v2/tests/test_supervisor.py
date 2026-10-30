"""감시 프로그램 — 비정상 종료 재시작, 10분에 3번 넘으면 포기, 정상 종료는 다시 켜지 않음, 실제 자식 프로세스.
종료 신호(리눅스): 감시 프로그램 pid 에만 SIGTERM·SIGINT 를 보내도 자식에게 SIGTERM 을 넘겨 같이 끝난다(고아 없음),
안 끝나는 자식은 기다림 한도 뒤 kill.
"""
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from v2.supervisor import register_task
from v2.supervisor.supervisor import EXIT_GAVE_UP, Supervisor

ROOT = Path(__file__).resolve().parents[2]
posix_only = pytest.mark.skipif(sys.platform == "win32", reason="POSIX 신호로 시험 — 윈도우는 콘솔 그룹 Ctrl+C(넘기지 않음)")


class FakeChild:
    def __init__(self, code):
        self.code, self.pid = code, 1234

    def wait(self, timeout=None):
        return self.code


def _sup(tmp_path, codes, times):
    it, tt = iter(codes), iter(times)
    sleeps = []
    s = Supervisor(["x"], tmp_path, now=lambda: next(tt), sleep=sleeps.append,
                   popen=lambda cmd, env: FakeChild(next(it)), say=lambda m: None)
    return s, sleeps


def test_restart_then_normal_exit(tmp_path):
    s, sleeps = _sup(tmp_path, [1, 0xC0000409, 0], [0.0, 10.0])
    assert s.run() == 0
    assert s.starts == 3 and sleeps == [3.0, 3.0]
    assert "다시 켜지 않음" in (tmp_path / "supervisor.log").read_text(encoding="utf-8")


def test_gives_up_after_4th_crash_in_10min(tmp_path):
    s, _ = _sup(tmp_path, [1, 1, 1, 1], [0.0, 100.0, 200.0, 300.0])
    assert s.run() == EXIT_GAVE_UP and s.gave_up and s.starts == 4


def test_crashes_spread_over_window_keep_restarting(tmp_path):
    s, _ = _sup(tmp_path, [1, 1, 1, 1, 1, 0], [0.0, 250.0, 500.0, 750.0, 1000.0])
    assert s.run() == 0 and s.starts == 6


def test_config_reject_not_restarted(tmp_path):
    s, _ = _sup(tmp_path, [2], [])
    assert s.run() == 2 and s.starts == 1


def test_real_child_process(tmp_path):
    flag = tmp_path / "n"
    code = ("import os,sys,pathlib;p=pathlib.Path(sys.argv[1]);n=int(p.read_text()) if p.exists() else 0;"
            "p.write_text(str(n+1));sys.exit(0 if n>=1 else 3221226505 % 256)")
    s = Supervisor([sys.executable, "-c", code, str(flag)], tmp_path, restart_delay_s=0.01, say=lambda m: None)
    assert s.run() == 0 and s.starts == 2


def test_register_cmd_uses_onlogon_it():
    c = register_task.create_cmd("C:\\py\\python.exe", register_task.repo_root())
    assert "/sc" in c and "onlogon" in c and "/it" in c and register_task.TASK_NAME in c


# ───────────── 종료 신호를 자식에게 넘김(실제 하위 프로세스) ─────────────
def _free_port() -> int:
    import socket
    k = socket.socket()
    k.bind(("127.0.0.1", 0))
    p = k.getsockname()[1]
    k.close()
    return p


class _Run:
    """하위 프로세스 하나 — 출력 줄을 모으고 특정 줄을 기다린다."""

    def __init__(self, cmd, env=None) -> None:
        self.p = subprocess.Popen(cmd, cwd=str(ROOT), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                  text=True, encoding="utf-8", errors="replace")
        self.lines = []
        self.t = threading.Thread(target=lambda: [self.lines.append(x.rstrip("\n")) for x in self.p.stdout], daemon=True)
        self.t.start()

    def wait_line(self, head: str, timeout: float) -> bool:
        end = time.time() + timeout
        while time.time() < end:
            if any(x.startswith(head) for x in list(self.lines)):
                return True
            if self.p.poll() is not None:
                return False
            time.sleep(0.1)
        return False

    @property
    def text(self) -> str:
        return "\n".join(self.lines)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    try:                                            # 좀비(끝났는데 거두지 않음)도 끝난 것으로
        return Path(f"/proc/{pid}/stat").read_text().split(") ")[-1][:1] != "Z"
    except OSError:
        return True


def _child_pid(log_dir: Path) -> int:
    end = time.time() + 10.0                        # 상태 파일은 자식을 켠 바로 뒤에 쓴다 — 아주 짧은 틈을 기다림
    while True:
        try:
            pid = json.loads((log_dir / "supervisor_state.json").read_text(encoding="utf-8"))["child_pid"]
            if pid:
                return pid
        except (OSError, ValueError, KeyError):
            pass
        if time.time() > end:
            raise AssertionError("supervisor_state.json 에 child_pid 없음")
        time.sleep(0.05)


def _cleanup(run: _Run, pids) -> None:
    if run.p.poll() is None:
        run.p.kill()
        run.p.wait(10)
    for pid in pids:
        if pid and _alive(pid):
            os.kill(pid, signal.SIGKILL)


@posix_only
def test_sigterm_to_supervisor_only_stops_real_ch12_child(tmp_path):
    """python -m v2.supervisor -- --config-dir … (자식 = 실제 v2.ch12 sim). 감시 프로그램 pid 에만 SIGTERM →
    자식이 의도적 종료로 끝나고(코드 0) 감시 프로그램도 0, 다시 켜지 않음, 자식 프로세스가 남지 않음."""
    from v2.ch12 import app as A
    from v2.ch12.sim.rig import local_paths
    cfg = tmp_path / "config"
    cfg.mkdir()
    vals = dict(local_paths(tmp_path / "root"), LOCAL_HMI_PORT=_free_port(), ROBOT_API_PORT=_free_port())
    (cfg / "local_override.json").write_text(json.dumps({"values": vals}), encoding="utf-8")
    logs = tmp_path / "sup"
    run = _Run([sys.executable, "-m", "v2.supervisor", "--log-dir", str(logs), "--restart-delay", "0.2",
                "--", "--config-dir", str(cfg)], env=dict(os.environ, HOME=str(tmp_path / "home")))
    pid = None
    try:
        assert run.wait_line(A.RUNNING_LINE, 60.0), run.text
        pid = _child_pid(logs)
        assert _alive(pid)
        run.p.send_signal(signal.SIGTERM)                       # 감시 프로그램에게만(자식 pid 에는 보내지 않음)
        assert run.p.wait(60) == 0, run.text
        run.t.join(5)
        assert not _alive(pid), run.text                         # 고아 없음
        log = (logs / "supervisor.log").read_text(encoding="utf-8")
        assert "SIGTERM 받음" in log and "다시 켜지 않음" in log and "강제 종료" not in log, log
        assert log.count("시작 #") == 1
        assert "의도적 종료(SIGTERM)" in run.text                 # 자식이 SIGTERM 을 의도적 종료로 처리
    finally:
        _cleanup(run, [pid])


_SUP = ("import sys; sys.path.insert(0, sys.argv[1]);"
        "from v2.supervisor.supervisor import Supervisor;"
        "s = Supervisor([sys.executable, '-c', sys.argv[3]], sys.argv[2], restart_delay_s=0.2, stop_wait_s=1.0,"
        " say=lambda m: print(m, flush=True));"
        "s.install_signals(); sys.exit(s.run())")


@posix_only
@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
def test_signal_to_supervisor_forwards_sigterm(tmp_path, sig):
    """감시 프로그램 pid 에만 SIGTERM·SIGINT → 자식은 SIGTERM 을 받아 0 으로 끝남 → 감시 프로그램 0, 다시 켜지 않음."""
    child = ("import signal, sys, time\n"
             "signal.signal(signal.SIGINT, signal.SIG_IGN)\n"                       # SIGINT 로는 끝나지 않음 — SIGTERM 을 받아야
             "signal.signal(signal.SIGTERM, lambda *a: sys.exit(0))\n"
             "print('ready', flush=True)\n"
             "time.sleep(60)\n"
             "sys.exit(5)\n")
    run = _Run([sys.executable, "-c", _SUP, str(ROOT), str(tmp_path), child])
    pid = None
    try:
        assert run.wait_line("ready", 30.0), run.text
        pid = _child_pid(tmp_path)
        run.p.send_signal(sig)
        assert run.p.wait(30) == 0, run.text
        assert not _alive(pid)
        log = (tmp_path / "supervisor.log").read_text(encoding="utf-8")
        assert f"{sig.name} 받음" in log and log.count("시작 #") == 1, log
    finally:
        _cleanup(run, [pid])


@posix_only
def test_stubborn_child_killed_after_stop_wait(tmp_path):
    """SIGTERM 을 무시하는 자식 — 기다림 한도(시험 1 s) 뒤 kill, 감시 프로그램은 128+9 로 끝남(다시 켜지 않음)."""
    child = ("import signal, time\n"
             "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
             "print('ready', flush=True)\n"
             "time.sleep(60)\n")
    run = _Run([sys.executable, "-c", _SUP, str(ROOT), str(tmp_path), child])
    pid = None
    try:
        assert run.wait_line("ready", 30.0), run.text
        pid = _child_pid(tmp_path)
        t0 = time.monotonic()
        run.p.send_signal(signal.SIGTERM)
        assert run.p.wait(30) == 128 + signal.SIGKILL, run.text
        assert time.monotonic() - t0 >= 0.9 and not _alive(pid)
        log = (tmp_path / "supervisor.log").read_text(encoding="utf-8")
        assert "강제 종료(kill)" in log and log.count("시작 #") == 1, log
    finally:
        _cleanup(run, [pid])

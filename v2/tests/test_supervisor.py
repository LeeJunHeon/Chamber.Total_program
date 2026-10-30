"""감시 프로그램 — 비정상 종료 재시작, 10분에 3번 넘으면 포기, 정상 종료는 다시 켜지 않음, 실제 자식 프로세스."""
import sys

from v2.supervisor import register_task
from v2.supervisor.supervisor import EXIT_GAVE_UP, Supervisor


class FakeChild:
    def __init__(self, code):
        self.code, self.pid = code, 1234

    def wait(self):
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

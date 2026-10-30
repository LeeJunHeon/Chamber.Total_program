"""챔버12 실행 프로그램(python -m v2.ch12) — 실제 하위 프로세스로 sim 실행·종료 코드.

- sim 으로 켜서 몇 초 돌리고 Ctrl+C(SIGINT)·종료 신호(SIGTERM) → 종료 코드 0, 1초 기록 SQLite·v1 기록(PLC 코일 CSV)·로그가 생김.
- 종료 코드(감시 프로그램 v2/supervisor 가 본다): 2 설정 거부, 3 이미 실행 중(포트 사용 중 — 장비 포트는 열지 않음), 4 시작 실패.
- Ctrl+C 는 엔진이 busy 면 거부(v1 closeEvent 조건).
기록 경로는 모두 tmp 아래(local_override.json — install 키). 윈도우 콘솔 Ctrl+C(CTRL_C_EVENT)는 리눅스에서 못 돌림 — 윈도우 확인 필요.
"""
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from v2.ch12 import app as A
from v2.ch12.sim.rig import local_paths

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX 신호로 시험 — 윈도우는 콘솔 이벤트(윈도우 확인 필요)")


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def config(tmp: Path, **extra) -> Path:
    d = tmp / "config"
    d.mkdir(parents=True, exist_ok=True)
    vals = dict(local_paths(tmp / "root"), LOCAL_HMI_PORT=free_port(), **extra)
    (d / "local_override.json").write_text(json.dumps({"values": vals}), encoding="utf-8")
    return d


class Proc:
    def __init__(self, cfg_dir, cwd=None, env=None) -> None:
        args = ["--config-dir", str(cfg_dir)] if cfg_dir is not None else []      # None = 기본 --config-dir
        self.p = subprocess.Popen([sys.executable, "-m", "v2.ch12"] + args, cwd=str(cwd or ROOT), env=env,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8")
        self.lines = []
        self.running = threading.Event()
        self.t = threading.Thread(target=self._read, daemon=True)
        self.t.start()

    def _read(self) -> None:
        for line in self.p.stdout:
            self.lines.append(line.rstrip("\n"))
            if line.startswith(A.RUNNING_LINE):
                self.running.set()

    def wait(self, timeout: float = 30.0) -> int:
        code = self.p.wait(timeout)
        self.t.join(5.0)
        return code

    @property
    def text(self) -> str:
        return "\n".join(self.lines)


def rows_in(root: Path) -> int:
    dbs = list((root / "data").glob("snap_*.sqlite"))
    if not dbs:
        return 0
    db = sqlite3.connect(dbs[0])
    try:
        return db.execute("SELECT COUNT(*) FROM snapshot").fetchone()[0]
    finally:
        db.close()


@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM])
def test_sim_runs_and_exits_0_on_signal(tmp_path, sig):
    cfg = config(tmp_path)
    pr = Proc(cfg)
    assert pr.running.wait(60.0), pr.text
    time.sleep(3.5)
    pr.p.send_signal(sig)
    code = pr.wait(30.0)
    assert code == A.EXIT_OK, pr.text
    out = pr.text
    assert "모드 sim" in out and "장비 연결: plc 준비" in out and "종료" in out.splitlines()[-1]
    root = tmp_path / "root"
    assert rows_in(root) >= 2                                                 # 1초 기록
    assert list((root / "logs" / "CH1&2_PLC").glob("*.csv"))                  # v1 PLC 코일 CSV
    log = (root / "data" / "logs" / "ch12_v2.log").read_text(encoding="utf-8")
    assert "정상 종료 시작" in log and "PLC 연결 끊김" not in log              # 종료가 하트비트 실패로 보이지 않음


def test_bad_config_exit_2(tmp_path):
    cfg = config(tmp_path, NO_SUCH_KEY=1)
    pr = Proc(cfg)
    assert pr.wait(60.0) == A.EXIT_CONFIG
    assert "시작 거부" in pr.text and rows_in(tmp_path / "root") == 0


def test_already_running_exit_3_without_opening_devices(tmp_path):
    cfg = config(tmp_path)
    port = json.loads((cfg / "local_override.json").read_text(encoding="utf-8"))["values"]["LOCAL_HMI_PORT"]
    hold = socket.socket()
    hold.bind(("127.0.0.1", port))
    hold.listen(1)
    try:
        pr = Proc(cfg)
        assert pr.wait(60.0) == A.EXIT_RUNNING
    finally:
        hold.close()
    assert "이미 실행 중" in pr.text and not (tmp_path / "root" / "data").exists()


def test_start_failure_exit_4(tmp_path):
    bad = tmp_path / "root" / "data_is_a_file"
    bad.parent.mkdir(parents=True)
    bad.write_text("x", encoding="utf-8")
    cfg = config(tmp_path, LOCAL_DATA_DIR=str(bad))
    pr = Proc(cfg)
    assert pr.wait(60.0) == A.EXIT_START
    assert "시작 실패" in pr.text


def test_ctrl_c_refused_while_engine_busy():
    app = A.App("unused", print_fn=lambda s: None)

    class Sys:
        busy_now = True

        def busy(self):
            return self.busy_now
    app.system = Sys()
    assert app.on_ctrl_c() == "refused"
    app.system.busy_now = False
    assert app.on_ctrl_c() == "exit"
    assert A.git_version(Path("/nonexistent")) == "unknown"


def test_default_windows_paths_not_created_in_cwd(tmp_path):
    """리눅스에서 기본 설정(--config-dir 도 기본 'C:\\CH12_v2\\config')으로 실제로 띄워도 작업 폴더에 'C:…' 이름의 폴더가
    생기지 않는다 — 윈도우 경로는 홈 아래 ~/.ch12_v2/winpaths/<드라이브>/… 로 바꿔 쓴다(v2.common.winpaths, 실행 값만).
    HOME 은 임시 폴더로(격리). 포트만 local_override 로(시험이 동시에 돌아도 겹치지 않게) — 경로는 하나도 넣지 않는다."""
    home, work = tmp_path / "home", tmp_path / "work"
    work.mkdir()
    base = home / ".ch12_v2" / "winpaths"
    cfg = base / "C" / "CH12_v2" / "config"                                  # 기본 --config-dir 이 바뀐 곳
    cfg.mkdir(parents=True)
    vals = {"LOCAL_HMI_PORT": free_port(), "ROBOT_API_PORT": free_port()}
    (cfg / "local_override.json").write_text(json.dumps({"values": vals}), encoding="utf-8")
    env = dict(os.environ, HOME=str(home), PYTHONPATH=os.pathsep.join(p for p in (str(ROOT), os.environ.get("PYTHONPATH")) if p))
    pr = Proc(None, cwd=work, env=env)
    try:
        assert pr.running.wait(60.0), pr.text
        time.sleep(1.5)
        pr.p.send_signal(signal.SIGTERM)
        assert pr.wait(30.0) == A.EXIT_OK, pr.text
    finally:
        if pr.p.poll() is None:
            pr.p.kill()
    bad = [p.name for p in work.iterdir() if p.name[:2].endswith(":") or p.name.startswith("\\")]
    assert not bad, (bad, pr.text)                                           # 작업 폴더에 'C:…'·'G:…'·'\\…' 없음
    assert (base / "C" / "CH12_v2" / "data" / "logs" / "ch12_v2.log").is_file()   # LOCAL_DATA_DIR 'C:\\CH12_v2\\data'
    assert "윈도우 경로" in pr.text and str(base) in pr.text                 # 시작 로그 한 줄

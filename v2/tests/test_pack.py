"""장비 노트북에서 띄우기 쉽게(2026-11-23 D429) — 꾸러미 zip·VERSION 버전 표시·sim 낮은 우선순위·실행 cmd·콘솔 안내 한 번.

- python -m v2.tools.pack: 그 커밋의 v2 만(시험·골든 도구 제외) + 맨 위 VERSION('브랜치@커밋7'), 경로는 윈도우에서 풀 수 있는 이름.
- .git 이 없는 폴더(꾸러미를 푼 곳)는 VERSION 첫 줄이 버전(예전에는 'unknown').
- sim 은 윈도우에서 BelowNormal(자식 워커도 물려받음) — kernel32 를 가짜로 바꿔 호출을 본다.
- run_ch12_sim.cmd: C:\\CH12_v2\\venv → py -3.13 → py -3.11 → python 순서, CRLF.
- 콘솔: 감시 창 준비·user32 같은 안내는 INFO 한 번([WARNING] 없음), 실패만 '[WARNING]' 한 번, 경고 줄을 두 번 찍지 않음.
"""
from __future__ import annotations

import subprocess
import zipfile
from pathlib import Path

import pytest

from v2.ch12 import app as A
from v2.common import winproc
from v2.common.winnames import problems
from v2.tools import pack

ROOT = Path(__file__).resolve().parents[2]
V2 = ROOT / "v2"


def _git_ok() -> bool:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, timeout=30).returncode == 0
    except Exception:
        return False


needs_git = pytest.mark.skipif(not _git_ok(), reason="git 저장소가 아님(꾸러미 zip 을 푼 곳)")


def test_included_rules():
    assert pack.included("v2/ch12/app.py") and pack.included("v2/hmi/index.html") and pack.included("v2/run_ch12_sim.cmd")
    assert pack.included("v2/tools/pack.py") and pack.included("v2/requirements.txt")
    assert not pack.included("v2/tests/test_app.py") and not pack.included("v2/tests/golden/dev_plc.json")
    assert not pack.included("v2/tools/v1golden_plc.py") and not pack.included("v2/tools/robot_golden_cases.py")
    assert not pack.included("v2/tools/make_web_golden_ch12.py")
    assert not pack.included("main.py") and not pack.included("v2/ch12/__pycache__/app.cpython-313.pyc")


@needs_git
def test_pack_zip_has_v2_only_with_version(tmp_path):
    dest, ver, n = pack.build(ROOT, tmp_path / "out")
    assert dest.parent == tmp_path / "out" and dest.name == f"ch12_v2_{ver.replace('/', '-').replace('@', '_')}.zip"
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    assert ver.endswith("@" + sha[:7])
    with zipfile.ZipFile(dest) as z:
        names = z.namelist()
        lines = z.read("VERSION").decode("utf-8").splitlines()
    assert lines[0] == ver and lines[1] == sha and lines[2].startswith("만든 시각 ")
    files = [x for x in names if x != "VERSION"]
    assert len(files) == n and all(x.startswith("v2/") for x in files)
    assert not [x for x in files if x.startswith("v2/tests/") or "/v1golden_" in x or "__pycache__" in x]
    assert {"v2/ch12/app.py", "v2/ch12/__main__.py", "v2/hmi/index.html", "v2/run_ch12_sim.cmd",
            "v2/requirements.txt"} <= set(files)
    assert [x for x in names if problems(x)] == []
    tracked = subprocess.run(["git", "ls-tree", "-r", "-z", "--name-only", "HEAD", "--", "v2"], cwd=ROOT,
                             capture_output=True).stdout.decode("utf-8").split("\0")
    assert sorted(files) == sorted(p for p in tracked if p and pack.included(p))   # 그 커밋 그대로(작업 폴더 변경은 담지 않음)
    # 빈 폴더에 풀면 .git 없이 VERSION 으로 버전
    out = tmp_path / "app"
    with zipfile.ZipFile(dest) as z:
        z.extractall(out)
    assert not (out / ".git").exists() and A.git_version(out) == ver


def test_pack_refuses_outside_git(tmp_path):
    (tmp_path / "v2").mkdir()
    with pytest.raises(pack.PackError, match="git 저장소가 아님"):
        pack.build(tmp_path, tmp_path / "out")


@needs_git
def test_pack_main_prints_path_and_version(tmp_path, capsys):
    assert pack.main(["--out", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "꾸러미 " in out and " — 버전 " in out and "VERSION" in out
    assert len(list(tmp_path.glob("ch12_v2_*.zip"))) == 1 and not list(tmp_path.glob("*.part"))


def test_version_from_version_file_when_no_git(tmp_path):
    assert A.git_version(tmp_path) == "unknown"
    (tmp_path / "VERSION").write_text("rewrite/v2@abc1234\nabc1234ffff\n만든 시각 x\n", encoding="utf-8")
    assert A.git_version(tmp_path) == "rewrite/v2@abc1234"
    g = tmp_path / ".git"                                  # .git 이 있으면 git 이 먼저
    (g / "refs" / "heads").mkdir(parents=True)
    (g / "HEAD").write_text("ref: refs/heads/main\n", encoding="utf-8")
    (g / "refs" / "heads" / "main").write_text("1234567890abcdef\n", encoding="utf-8")
    assert A.git_version(tmp_path) == "main@1234567"


# ───────── 낮은 우선순위 ─────────
class _Fn:
    def __init__(self, f):
        self.f = f

    def __call__(self, *a):
        return self.f(*a)


class FakeKernel32:
    def __init__(self, ok=True, report=None):
        self.calls, self.cls = [], 0x20
        self.ok, self.report = ok, report
        self.GetCurrentProcess = _Fn(lambda: -1)
        self.SetPriorityClass = _Fn(self._set)
        self.GetPriorityClass = _Fn(lambda h: self.report if self.report is not None else self.cls)

    def _set(self, h, c):
        self.calls.append((h, c))
        if self.ok:
            self.cls = c
        return self.ok


def test_lower_priority_sets_below_normal():
    k = FakeKernel32()
    assert winproc.lower_priority(k) == winproc.LOW_LINE and k.calls == [(-1, 0x4000)] and k.cls == 0x4000
    assert "낮추지 못함" in winproc.lower_priority(FakeKernel32(ok=False))
    assert "0x20" in winproc.lower_priority(FakeKernel32(report=0x20))


def test_lower_priority_off_windows_does_nothing(monkeypatch):
    monkeypatch.setattr(winproc.sys, "platform", "linux")
    assert winproc.lower_priority() is None


def test_sim_app_lowers_priority_before_workers():
    """sim 모드에서만, 시작 줄 뒤·조립(워커를 띄우기) 전에 한 번 — 자식이 낮은 우선순위를 물려받게."""
    src = Path(A.__file__).read_text(encoding="utf-8")
    i_low, i_build = src.index("winproc.lower_priority()"), src.index("s = self.build()")
    assert src.count("winproc.lower_priority()") == 1
    assert src.index("for line in self._start_lines():") < i_low < i_build
    assert "RunMode.SIM" in src[i_low - 200:i_low]


# ───────── 실행 cmd ─────────
@pytest.mark.parametrize("name", ["run_ch12_sim.cmd", "run_ch12_sim_robot_server.cmd"])
def test_sim_cmd_python_order_and_crlf(name):
    raw = (V2 / name).read_bytes()
    assert b"\r\n" in raw and b"\n" not in raw.replace(b"\r\n", b"")
    t = raw.decode("utf-8")
    i_venv = t.index(r'if exist "C:\CH12_v2\venv\Scripts\python.exe"')
    i313, i311, ipy = t.index("py -3.13 -c"), t.index("py -3.11 -c"), t.index('python -c ""')
    assert i_venv < i313 < i311 < ipy
    cmds = [ln.strip().lower() for ln in t.splitlines() if ln.strip() and not ln.strip().lower().startswith("rem")]
    assert not [c for c in cmds if c.startswith("start ")]     # Ctrl+C 를 끄는 'start /belownormal /b' 는 쓰지 않음(프로그램이 낮춤)
    assert ("-m v2.ch12 --sim" in t) if name == "run_ch12_sim.cmd" else ("-m v2.robot_server --sim" in t)


# ───────── 콘솔 안내 한 번 ─────────
def _app_with_console(tmp_path, monkeypatch):
    out = []
    app = A.App(str(tmp_path / "cfg"), print_fn=out.append)
    app.cfg = {"LOCAL_DATA_DIR": str(tmp_path / "data")}
    app.setup_logging()
    got = {}
    from v2.ch12 import winshell
    monkeypatch.setattr(winshell, "install", lambda on_exit, log=None, warn=None: got.update(log=log, warn=warn) or None)
    app.install_console_handlers()
    return app, out, got


def test_shell_notices_are_info_once_and_failures_warning_once(tmp_path, monkeypatch):
    app, out, got = _app_with_console(tmp_path, monkeypatch)
    try:
        got["log"]("로그오프·윈도우 종료 감시 창(WM_ENDSESSION) 준비")
        got["log"]("user32 로드됨: True (참이면 로그오프·종료는 숨은 창으로 받음)")
        assert out == ["로그오프·윈도우 종료 감시 창(WM_ENDSESSION) 준비", "user32 로드됨: True (참이면 로그오프·종료는 숨은 창으로 받음)"]
        out.clear()
        got["warn"]("로그오프·윈도우 종료 감시 실패: OSError()")
        assert out == ["[WARNING] 로그오프·윈도우 종료 감시 실패: OSError()"]
        out.clear()
        app.warn_once("[재시작] 지난 실행의 ch1 공정", "재시작: %s", "지난 실행의 ch1 공정")
        assert out == ["[재시작] 지난 실행의 ch1 공정"]
        for h in app._log_handlers:
            h.flush()
        text = (tmp_path / "data" / "logs" / "ch12_v2.log").read_text(encoding="utf-8")
        assert "INFO 로그오프·윈도우 종료 감시 창(WM_ENDSESSION) 준비" in text and "INFO user32 로드됨" in text
        assert "WARNING 로그오프·윈도우 종료 감시 실패" in text and "WARNING 재시작: 지난 실행의 ch1 공정" in text
    finally:
        for h in app._log_handlers:
            app.log.removeHandler(h)
            h.close()


def test_winshell_failures_go_to_warn():
    from v2.ch12 import winshell
    info, warn = [], []
    sh = winshell.WinShell(lambda why: (_ for _ in ()).throw(RuntimeError("x")), info.append, warn.append)
    sh.fire("CTRL_CLOSE")
    assert info == [] and warn and warn[0].startswith("종료 처리 오류")
    sh2 = winshell.WinShell(lambda why: None, info.append)      # warn 없으면 log 로(예전과 같음)
    assert sh2.warn == sh2.log

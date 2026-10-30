"""배포 도구(결정 D01) — 소스 실행 cmd 와 onedir 묶음 도구."""
from pathlib import Path

from v2.tools import build_exe

V2 = Path(__file__).resolve().parents[1]


def test_build_exe_dry_run_is_onedir_for_every_target(capsys):
    assert build_exe.main(["--dry-run", "--out", "x"]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == len(build_exe.TARGETS) == 4
    assert all("--onedir" in ln and "--onefile" not in ln for ln in lines)
    for _, entry, _ in build_exe.TARGETS:
        assert (V2.parent / entry).is_file()


def test_run_cmd_uses_supervisor_and_crlf():
    raw = (V2 / "run_ch12_v2.cmd").read_bytes()
    assert b"\r\n" in raw and b"-m v2.supervisor" in raw and b"--config-dir" in raw
    assert b"-m v2.robot_server" in (V2 / "run_ch12_robot_server.cmd").read_bytes()

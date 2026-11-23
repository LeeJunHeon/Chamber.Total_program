"""listen 소켓 공통(v2/common/netbind — 2026-11-23 D428)과 sim OES 정지 플래그 폴더.

- 같은 포트를 두 번 열면 PortBusy(OSError) — 문구에 이름·주소·포트.
- 윈도우 경로: SO_REUSEADDR 를 켜지 않고 SO_EXCLUSIVEADDRUSE(리눅스에서는 windows()·소켓 상수를 바꿔 기록으로 확인).
- v2 소스의 모든 listen 이 netbind 를 지난다(ThreadingHTTPServer 직접 생성·asyncio.start_server(host, port)·raw listen·SO_REUSEADDR 없음).
- 로봇 내부 명령 API 포트가 이미 쓰이면 조립이 bind_failed(ROBOT_NOT_READY)로 어느 포트인지 알리고, 공정 쪽은 그대로 뜬다.
- 로봇 서버(따로 도는 프로그램)는 로그 줄은 v1 그대로('Host start failed: OSError(..)'), 콘솔에는 분명한 문구.
- sim(OES_MUTEX_PREFIX 있음) OES 정지 폴더를 못 만들면 %TEMP%\\VanaM_OES_STOP 로 가지 않고 오류(운영은 v1 과 같게 예비).
"""
from __future__ import annotations

import ast
import asyncio
import socket
from http.server import BaseHTTPRequestHandler
from pathlib import Path

import pytest

from v2.common import netbind as NB

V2 = Path(__file__).resolve().parents[1]


class _H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass


def test_listen_socket_twice_is_port_busy_with_clear_message():
    a = NB.listen_socket("127.0.0.1", 0, 4, name="시험 서버")
    try:
        port = a.getsockname()[1]
        with pytest.raises(NB.PortBusy) as ei:
            NB.listen_socket("127.0.0.1", port, 4, name="시험 서버")
        e = ei.value
        assert isinstance(e, OSError) and e.port == port and e.name == "시험 서버" and isinstance(e.cause, OSError)
        assert f"시험 서버 포트 127.0.0.1:{port} 를 열 수 없음" in str(e) and "이미 다른 프로그램" in str(e)
    finally:
        a.close()


def test_http_server_on_busy_port_is_port_busy():
    hold = NB.listen_socket("127.0.0.1", 0, 4, name="먼저")
    try:
        port = hold.getsockname()[1]
        with pytest.raises(NB.PortBusy) as ei:
            NB.ExclusiveHTTPServer(("127.0.0.1", port), _H, name="두 번째 HTTP")
        assert f"두 번째 HTTP 포트 127.0.0.1:{port}" in str(ei.value)
    finally:
        hold.close()
    srv = NB.ExclusiveHTTPServer(("127.0.0.1", 0), _H, name="빈 포트")   # 비면 그대로 열림
    try:
        assert srv.server_address[1] > 0 and srv.daemon_threads
    finally:
        srv.server_close()


class _RecSock:
    """setsockopt·bind 기록(윈도우 경로를 리눅스에서 보기)."""

    def __init__(self, *a, **k):
        self.opts, self.bound, self.closed = [], None, False

    def setsockopt(self, level, opt, val):
        self.opts.append((level, opt, val))

    def bind(self, addr):
        self.bound = addr

    def listen(self, n):
        self.backlog = n

    def getsockname(self):
        return ("127.0.0.1", 50123)

    def close(self):
        self.closed = True


FAKE_EXCL = 0x7FFF_FFFB   # 리눅스에 없는 상수 — 기록만


def test_windows_uses_exclusiveaddruse_not_reuseaddr(monkeypatch):
    monkeypatch.setattr(NB, "windows", lambda: True)
    monkeypatch.setattr(socket, "SO_EXCLUSIVEADDRUSE", FAKE_EXCL, raising=False)
    s = _RecSock()
    NB.prepare(s)
    assert s.opts == [(socket.SOL_SOCKET, FAKE_EXCL, 1)]
    # listen_socket 도 같은 준비
    monkeypatch.setattr(NB.socket, "socket", _RecSock)
    ls = NB.listen_socket("127.0.0.1", 50123, 7, name="윈도우")
    assert ls.opts == [(socket.SOL_SOCKET, FAKE_EXCL, 1)] and ls.bound == ("127.0.0.1", 50123) and ls.backlog == 7


def test_windows_http_server_bind_sets_exclusive_and_no_reuse(monkeypatch):
    monkeypatch.setattr(NB, "windows", lambda: True)
    monkeypatch.setattr(socket, "SO_EXCLUSIVEADDRUSE", FAKE_EXCL, raising=False)
    srv = NB.ExclusiveHTTPServer(("127.0.0.1", 0), _H, bind_and_activate=False, name="윈도우 HTTP")
    real = srv.socket
    try:
        srv.allow_reuse_address = False          # 윈도우에서의 클래스 값(sys.platform == "win32")
        srv.allow_reuse_port = False
        srv.socket = _RecSock()
        srv.server_bind()
        assert srv.socket.opts == [(socket.SOL_SOCKET, FAKE_EXCL, 1)], "윈도우: SO_REUSEADDR 없이 SO_EXCLUSIVEADDRUSE 하나"
        assert srv.socket.bound == ("127.0.0.1", 0)
    finally:
        real.close()


def test_non_windows_keeps_reuseaddr_for_time_wait():
    s = _RecSock()
    NB.prepare(s)
    assert s.opts == [(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)]
    assert NB.ExclusiveHTTPServer.allow_reuse_address is True


# ───────── 소스 검사: 모든 listen 이 netbind 를 지난다 ─────────
HTTP_SERVERS = {"HTTPServer", "ThreadingHTTPServer", "TCPServer", "ThreadingTCPServer", "UDPServer"}


def _listen_problems(src: str, rel: str) -> list:
    out = []
    for n in ast.walk(ast.parse(src)):
        if isinstance(n, ast.Call):
            f = n.func
            name = f.attr if isinstance(f, ast.Attribute) else (f.id if isinstance(f, ast.Name) else "")
            if name in HTTP_SERVERS:
                out.append(f"{rel}:{n.lineno} {name}( 직접 생성")
            if name in ("start_server", "create_server") and not any(k.arg == "sock" for k in n.keywords):
                out.append(f"{rel}:{n.lineno} {name}(host, port) — sock=listen_socket(..) 로")
            if name == "listen" and isinstance(f, ast.Attribute):
                out.append(f"{rel}:{n.lineno} .listen( 직접")
        if isinstance(n, ast.Attribute) and n.attr in ("SO_REUSEADDR", "SO_EXCLUSIVEADDRUSE"):
            out.append(f"{rel}:{n.lineno} {n.attr} 직접")
        if isinstance(n, ast.ClassDef):
            for b in n.bases:
                bn = b.attr if isinstance(b, ast.Attribute) else (b.id if isinstance(b, ast.Name) else "")
                if bn in HTTP_SERVERS:
                    out.append(f"{rel}:{n.lineno} class {n.name}({bn}) — ExclusiveHTTPServer 로")
    return out


def test_scan_catches_old_listen_shapes():
    old = ("from http.server import ThreadingHTTPServer\nimport asyncio, socket\nclass S(ThreadingHTTPServer): pass\n"
           "h = ThreadingHTTPServer(('127.0.0.1', 1), None)\nasync def f():\n    await asyncio.start_server(None, '127.0.0.1', 0)\n"
           "s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.listen(4)\n")
    assert len(_listen_problems(old, "x.py")) == 5
    ok = "import asyncio\nfrom v2.common.netbind import listen_socket\nasync def f(h):\n" \
         "    await asyncio.start_server(h, sock=listen_socket('127.0.0.1', 0))\n"
    assert _listen_problems(ok, "y.py") == []


def test_every_listen_in_v2_goes_through_netbind():
    files = [p for p in V2.rglob("*.py") if "tests" not in p.relative_to(V2).parts and p.name != "netbind.py"
             and "__pycache__" not in p.parts]
    assert len(files) > 100
    bad = [x for p in files for x in _listen_problems(p.read_text(encoding="utf-8"), p.relative_to(V2.parent).as_posix())]
    assert bad == [], "netbind 를 지나지 않는 listen:\n" + "\n".join(bad)


# ───────── 실제 사용처 ─────────
def test_robot_api_second_instance_is_port_busy():
    from v2.ch12.robot_api import RobotApi

    class Ops:
        def handle(self, *a, **k):
            return {}

    with RobotApi(Ops(), port=0) as a:
        with pytest.raises(NB.PortBusy) as ei:
            RobotApi(Ops(), port=a.port)
        assert f"로봇 내부 명령 API 포트 127.0.0.1:{a.port}" in str(ei.value)


def test_assembly_reports_busy_robot_api_port_and_keeps_running(tmp_path, monkeypatch):
    """이미 쓰는 ROBOT_API_PORT — 조립은 bind_failed(ROBOT_NOT_READY, 어느 포트인지) 하나를 남기고 나머지는 그대로 뜬다."""
    from v2.ch12 import robot_api as RA
    from v2.tests import restart_rig as R

    hold = NB.listen_socket("127.0.0.1", 0, 4, name="다른 프로그램")
    busy = hold.getsockname()[1]
    real = RA.RobotApi

    def forced(*a, **k):
        k["port"] = busy
        return real(*a, **k)

    monkeypatch.setattr(RA, "RobotApi", forced)
    rig = R.boot(tmp_path, robot_api=True, start=False)
    try:
        assert getattr(rig.asm, "robot_api", None) is None
        ev = [e for e in rig.events if e.source == "robot_api" and e.kind == "bind_failed"]
        assert len(ev) == 1 and ev[0].level == "error" and ev[0].code == "ROBOT_NOT_READY"
        assert f"로봇 내부 명령 API 포트 127.0.0.1:{busy} 를 열 수 없음" in ev[0].detail
        assert "로봇 명령을 받지 못함(공정·안전은 그대로)" in ev[0].detail
        assert rig.engine("proc.ch1") is not None
    finally:
        hold.close()
        R.cleanup(rig)
        rig.clock.close()


def test_robot_server_busy_port_keeps_v1_log_line_and_prints_clear_message(tmp_path, capsys):
    from v2.robot_server import app as RA

    hold = NB.listen_socket("127.0.0.1", 0, 4, name="v1 로봇 서버")
    port = hold.getsockname()[1]
    try:
        cfg = {"LOG_ROOT_DIR": str(tmp_path / "logs"), "ROBOT_CACHE_PATH": "", "HOST_SERVER_HOST": "127.0.0.1",
               "HOST_SERVER_PORT": port}
        assert asyncio.run(RA._run(cfg, False)) == 1
    finally:
        hold.close()
    err = capsys.readouterr().err
    assert f"[중지] 로봇 서버 포트 127.0.0.1:{port} 를 열 수 없음" in err
    text = "".join(p.read_text(encoding="utf-8") for p in (tmp_path / "logs").rglob("server_page_*.log"))
    assert "[NET] Host start failed: OSError(" in text and "PortBusy" not in text


def test_sim_tcp_and_inventory_servers_refuse_busy_port():
    from v2.ch12.sim.inventory_sim import FakeInventoryServer
    from v2.ch12.sim.tcp_adapter import SimTcpServer

    class Dev:
        name = "가짜"

    hold = NB.listen_socket("127.0.0.1", 0, 4, name="먼저")
    port = hold.getsockname()[1]
    try:
        with pytest.raises(NB.PortBusy, match="sim 가짜 TCP"):
            SimTcpServer(Dev(), port=port)
        with pytest.raises(NB.PortBusy, match="sim 재고 서버"):
            FakeInventoryServer(port=port)
    finally:
        hold.close()


# ───────── sim OES 정지 플래그 폴더 ─────────
def _block_mkdir(monkeypatch, blocked: Path):
    real = Path.mkdir

    def mk(self, *a, **k):
        if Path(self) == blocked:
            raise PermissionError(13, "막힘", str(self))
        return real(self, *a, **k)

    monkeypatch.setattr(Path, "mkdir", mk)


def test_sim_oes_stop_dir_does_not_fall_back_to_temp(tmp_path, monkeypatch):
    from v2.ch12.workers import oes_common as OC

    stop = tmp_path / "sim_out" / "oes_worker" / "stop"
    temp = tmp_path / "TEMP"
    monkeypatch.setenv("OES_STOP_DIR", str(stop))
    monkeypatch.setenv("TEMP", str(temp))
    monkeypatch.setenv("OES_MUTEX_PREFIX", "CH12SIM_")
    assert OC._stop_dir() == stop and stop.is_dir()                 # 만들 수 있으면 그대로
    blocked = tmp_path / "막힌" / "stop"
    monkeypatch.setenv("OES_STOP_DIR", str(blocked))
    _block_mkdir(monkeypatch, blocked)
    with pytest.raises(OC.StopDirError) as ei:
        OC._stop_dir()
    assert isinstance(ei.value, OSError) and str(blocked) in str(ei.value) and "VanaM_OES_STOP(v1 운영 폴더)로 가지 않습니다" in str(ei.value)
    assert not (temp / "VanaM_OES_STOP").exists()


def test_production_oes_stop_dir_keeps_v1_temp_fallback(tmp_path, monkeypatch):
    from v2.ch12.workers import oes_common as OC

    blocked = tmp_path / "막힌" / "stop"
    temp = tmp_path / "TEMP"
    monkeypatch.setenv("OES_STOP_DIR", str(blocked))
    monkeypatch.setenv("TEMP", str(temp))
    monkeypatch.delenv("OES_MUTEX_PREFIX", raising=False)
    _block_mkdir(monkeypatch, blocked)
    assert OC._stop_dir() == temp / "VanaM_OES_STOP" and (temp / "VanaM_OES_STOP").is_dir()

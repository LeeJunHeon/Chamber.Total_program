"""listen 소켓 공통 — 같은 포트를 두 프로그램이 같이 열지 못하게(2026-11-23 D428).

윈도우에서 SO_REUSEADDR 는 이미 LISTEN 중인 포트에도 같이 바인드를 허용한다 — v1 이 돌고 있는 장비 노트북에서 v2 가 같은 포트를
조용히 같이 열면 요청이 어느 쪽으로 갈지 모른다(http.server 의 HTTPServer 는 allow_reuse_address = 1 이 기본). 그래서:
- 윈도우: SO_REUSEADDR 를 켜지 않고 SO_EXCLUSIVEADDRUSE(이 소켓이 잡은 동안 다른 소켓은 그 포트를 못 연다).
- 그 밖(리눅스·맥): SO_REUSEADDR(TIME_WAIT 중에도 바로 다시 — 다른 소켓이 LISTEN 중이면 여전히 실패).
- 열지 못하면 PortBusy(OSError 의 하위 — 기존 'except OSError' 처리 그대로) — 문구에 어느 이름·주소·포트인지.
v2 의 모든 listen(로컬 화면 서버·로봇 내부 명령 API·로봇 서버·가짜 내부 API·sim 장비 TCP·sim 재고 서버·v1 골든 PLC 도구)이 이것을 쓴다
(시험 test_netbind 이 소스에서 확인).
"""
from __future__ import annotations

import socket
import sys
from http.server import ThreadingHTTPServer
from typing import Optional


def windows() -> bool:
    return sys.platform == "win32"


class PortBusy(OSError):
    """포트를 열 수 없음(이미 다른 프로그램이 쓰는 중일 수 있음)."""

    def __init__(self, name: str, host: str, port: int, cause: Optional[BaseException] = None) -> None:
        self.name, self.host, self.port, self.cause = name, host, int(port), cause
        super().__init__(f"{name} 포트 {host}:{int(port)} 를 열 수 없음 — 이미 다른 프로그램이 쓰는 중일 수 있음({cause})")


def prepare(sock: socket.socket) -> None:
    """bind 앞 — 윈도우 SO_EXCLUSIVEADDRUSE, 그 밖 SO_REUSEADDR."""
    excl = getattr(socket, "SO_EXCLUSIVEADDRUSE", None)
    if windows() and excl is not None:
        sock.setsockopt(socket.SOL_SOCKET, excl, 1)
    else:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)


def listen_socket(host: str, port: int, backlog: int = 16, name: str = "listen") -> socket.socket:
    """host:port 를 잡아 listen 한 TCP 소켓(못 열면 PortBusy)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        prepare(s)
        s.bind((host, int(port)))
        s.listen(backlog)
    except OSError as e:
        s.close()
        raise PortBusy(name, host, port, e) from e
    return s


class ExclusiveHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer — 윈도우는 SO_REUSEADDR 끔·SO_EXCLUSIVEADDRUSE, 못 열면 PortBusy(name 은 문구용)."""
    daemon_threads = True
    allow_reuse_address = sys.platform != "win32"
    name = "HTTP"

    def __init__(self, server_address, handler, bind_and_activate: bool = True, name: Optional[str] = None) -> None:
        if name:
            self.name = name
        try:
            super().__init__(server_address, handler, bind_and_activate)
        except OSError as e:
            if isinstance(e, PortBusy):
                raise
            raise PortBusy(self.name, str(server_address[0]), int(server_address[1]), e) from e

    def server_bind(self) -> None:
        excl = getattr(socket, "SO_EXCLUSIVEADDRUSE", None)
        if windows() and excl is not None:
            self.socket.setsockopt(socket.SOL_SOCKET, excl, 1)
        super().server_bind()

"""시뮬레이터를 127.0.0.1 TCP 서버로 띄우는 어댑터 — 실제 소켓(TcpTransport·RealClock) 경로를 실제 장비 없이 시험할 때.

SimDevice(respond(data, now) → [(도착 시각, bytes)], is_connected(now))를 그대로 쓴다. 시뮬레이터 시각 = 서버 시작 뒤 실제 경과 초.
연결 하나에 스레드 하나: 받은 바이트를 그대로 respond 에 넘기고 예정 시각에 보낸다. disconnect 고장 구간이면 연결을 끊고
그 동안 새 연결을 바로 닫는다. 주소는 늘 127.0.0.1(포트 0 = 비어 있는 포트) — 장비망(192.168.1.x)에 붙지 않는다.
written 에 받은 바이트(쓰기 0 검사용), connections 에 받은 연결 수를 남긴다.
"""
from __future__ import annotations

import socket
import threading
import time
from typing import List, Optional, Tuple


class SimTcpServer:
    def __init__(self, device, origin: Optional[float] = None, port: int = 0) -> None:
        self.dev = device
        self.origin = time.monotonic() if origin is None else origin
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(("127.0.0.1", port))
        self._srv.listen(4)
        self._srv.settimeout(0.05)
        self.port = self._srv.getsockname()[1]
        self.written: List[bytes] = []
        self.connections = 0
        self._stop = False
        self._lock = threading.Lock()
        self._threads: List[threading.Thread] = []
        t = threading.Thread(target=self._accept, name=f"simtcp-{getattr(device, 'name', '?')}", daemon=True)
        t.start()
        self._threads.append(t)

    @property
    def addr(self) -> Tuple[str, int]:
        return "127.0.0.1", self.port

    def now(self) -> float:
        return time.monotonic() - self.origin

    def _accept(self) -> None:
        while not self._stop:
            try:
                c, _ = self._srv.accept()
            except (socket.timeout, OSError):
                continue
            if not self.dev.is_connected(self.now()):
                c.close()
                continue
            with self._lock:
                self.connections += 1
            t = threading.Thread(target=self._serve, args=(c,), daemon=True)
            t.start()
            self._threads.append(t)

    def _serve(self, c: socket.socket) -> None:
        c.settimeout(0.005)
        sched: List[Tuple[float, bytes]] = []
        try:
            while not self._stop:
                now = self.now()
                if not self.dev.is_connected(now):
                    break
                try:
                    b = c.recv(4096)
                    if not b:
                        break
                    with self._lock:
                        self.written.append(bytes(b))
                    sched += self.dev.respond(bytes(b), self.now())
                    sched.sort(key=lambda x: x[0])
                except socket.timeout:
                    pass
                now = self.now()
                while sched and sched[0][0] <= now:
                    c.sendall(sched.pop(0)[1])
        except OSError:
            pass
        finally:
            try:
                c.close()
            except OSError:
                pass

    def close(self) -> None:
        self._stop = True
        try:
            self._srv.close()
        except OSError:
            pass
        for t in self._threads:
            t.join(1.0)

# apps/robot_server/main_server.py
from __future__ import annotations

import asyncio
import contextlib
import sys
import socket
from pathlib import Path
from typing import Optional, Tuple

from PySide6.QtWidgets import QApplication
from qasync import QEventLoop

# ✅ 개발(py 실행)일 때만 sys.path 보정
if not getattr(sys, "frozen", False):
    ROOT = Path(__file__).resolve().parents[2]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

import lib.config_common as cfgc
from apps.robot_server.runtime.server_page import ServerPage


def _pick_upstream_endpoint() -> Tuple[str, int]:
    """
    업스트림(ProcessApp host) 엔드포인트 추정.
    기존 코드의 후보 키들을 그대로 유지.
    """
    host_candidates = [
        "PROCESS_SERVER_HOST",
        "PROCESS_HOST",
        "PROCESS_HOST_HOST",
        "MAIN_SERVER_HOST",
        "PLC_HOST_HOST",
        "UPSTREAM_HOST",
    ]
    port_candidates = [
        "PROCESS_SERVER_PORT",
        "PROCESS_PORT",
        "PROCESS_HOST_PORT",
        "MAIN_SERVER_PORT",
        "PLC_HOST_PORT",
        "UPSTREAM_PORT",
    ]

    host = None
    for k in host_candidates:
        if hasattr(cfgc, k):
            v = getattr(cfgc, k)
            if v:
                host = str(v)
                break

    port = None
    for k in port_candidates:
        if hasattr(cfgc, k):
            v = getattr(cfgc, k)
            if v:
                try:
                    port = int(v)
                    break
                except Exception:
                    pass

    # 기본값: 로컬의 ProcessApp host 포트(예: 50071)
    return host or "127.0.0.1", port or 50071


def _set_tcp_keepalive(sock_obj: object) -> None:
    """
    끊긴 TCP를 OS 레벨에서 빨리 감지하도록 keepalive 활성화.
    (Windows에서 상세 튜닝은 생략해도 기본 keepalive는 도움됨)
    """
    try:
        if isinstance(sock_obj, socket.socket):
            sock_obj.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    except Exception:
        pass


class RawTcpProxy:
    """
    "중계만" 하는 RAW TCP Proxy.
    - 메시지 파싱/JSON/Job/폴링/취소 같은 정책 없음
    - client <-> upstream 사이 바이트를 그대로 전달
    """

    def __init__(self, listen_host: str, listen_port: int, upstream_host: str, upstream_port: int, log) -> None:
        self.listen_host = str(listen_host)
        self.listen_port = int(listen_port)
        self.upstream_host = str(upstream_host)
        self.upstream_port = int(upstream_port)
        self.log = log

        self._server: Optional[asyncio.base_events.Server] = None
        self._conn_tasks: set[asyncio.Task] = set()
        self._closing = False

    async def start(self) -> None:
        if self._server is not None:
            return

        self._closing = False
        self._server = await asyncio.start_server(self._handle_client, self.listen_host, self.listen_port)
        sock = self._server.sockets[0] if self._server.sockets else None
        addr = sock.getsockname() if sock else (self.listen_host, self.listen_port)
        self.log("NET", f"RobotHost started on {addr} (RAW proxy -> {self.upstream_host}:{self.upstream_port})")

    async def stop(self) -> None:
        self._closing = True

        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()
            self._server = None

        # 현재 연결들 정리
        tasks = list(self._conn_tasks)
        self._conn_tasks.clear()
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(Exception):
                await t

        self.log("NET", "RobotHost stopped (RAW proxy)")

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, tag: str) -> None:
        """
        reader -> writer로 바이트 스트림 복사
        """
        try:
            while True:
                data = await reader.read(64 * 1024)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except asyncio.CancelledError:
            # stop() 시 정상 취소
            raise
        except Exception as e:
            self.log("NET", f"[PIPE_ERR] {tag} {e!r}")
        finally:
            with contextlib.suppress(Exception):
                writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _handle_client(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter) -> None:
        peer = client_writer.get_extra_info("peername")
        cid = id(client_writer) & 0xFFFF  # 간단한 connection id
        self.log("NET", f"[CONN_OPEN] cid={cid} peer={peer}")

        upstream_writer: Optional[asyncio.StreamWriter] = None
        task: Optional[asyncio.Task] = None

        try:
            # upstream 연결
            upstream_reader, upstream_writer = await asyncio.open_connection(self.upstream_host, self.upstream_port)

            # keepalive
            _set_tcp_keepalive(client_writer.get_extra_info("socket"))
            _set_tcp_keepalive(upstream_writer.get_extra_info("socket"))

            # 양방향 파이프
            t1 = asyncio.create_task(self._pipe(client_reader, upstream_writer, f"cid={cid} C->U"))
            t2 = asyncio.create_task(self._pipe(upstream_reader, client_writer, f"cid={cid} U->C"))

            # 서버 stop() 때 같이 죽이기 위해 task set에 묶음
            task = asyncio.current_task()
            if task:
                self._conn_tasks.add(task)

            done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)

            # 하나라도 끝나면 반대쪽도 정리
            for p in pending:
                p.cancel()
            for p in pending:
                with contextlib.suppress(Exception):
                    await p

        except asyncio.CancelledError:
            raise
        except Exception as e:
            if not self._closing:
                self.log("NET", f"[CONN_ERR] cid={cid} peer={peer} err={e!r}")
        finally:
            if task and task in self._conn_tasks:
                self._conn_tasks.discard(task)

            # 안전 close
            with contextlib.suppress(Exception):
                client_writer.close()
            with contextlib.suppress(Exception):
                await client_writer.wait_closed()

            if upstream_writer is not None:
                with contextlib.suppress(Exception):
                    upstream_writer.close()
                with contextlib.suppress(Exception):
                    await upstream_writer.wait_closed()

            self.log("NET", f"[CONN_CLOSE] cid={cid} peer={peer}")


class RobotServerApp:
    """
    기존 UI(ServerPage)의 Start/Stop/Restart 버튼은 그대로 유지하되,
    서버 로직만 'RAW TCP Proxy'로 교체한 버전.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop

        log_root = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs")
        self.page = ServerPage(log_root=log_root)
        self.page.set_host_info(cfgc.HOST_SERVER_HOST, int(cfgc.HOST_SERVER_PORT))
        self.page.set_running(False)
        self.page.show()

        up_host, up_port = _pick_upstream_endpoint()
        self.page.append_log("HOST_PROXY", f"Upstream endpoint = {up_host}:{up_port}")

        self.proxy = RawTcpProxy(
            cfgc.HOST_SERVER_HOST,
            int(cfgc.HOST_SERVER_PORT),
            up_host,
            up_port,
            self.page.append_log,
        )

        self.page.sigHostStart.connect(self.request_start)
        self.page.sigHostStop.connect(self.request_stop)
        self.page.sigHostRestart.connect(self.request_restart)

        self.loop.create_task(self._start())

    def request_start(self) -> None:
        self.loop.create_task(self._start())

    def request_stop(self) -> None:
        self.loop.create_task(self._stop())

    def request_restart(self) -> None:
        self.loop.create_task(self._restart())

    async def _start(self) -> None:
        # 이미 떠 있으면 무시
        await self.proxy.start()
        self.page.set_running(True)

    async def _stop(self) -> None:
        await self.proxy.stop()
        self.page.set_running(False)

    async def _restart(self) -> None:
        await self._stop()
        await self._start()


def run() -> int:
    if sys.platform.startswith("win"):
        from multiprocessing import freeze_support

        freeze_support()
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    app = QApplication(sys.argv)
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)

    _ = RobotServerApp(loop)

    with loop:
        loop.run_forever()

    return 0


if __name__ == "__main__":
    raise SystemExit(run())

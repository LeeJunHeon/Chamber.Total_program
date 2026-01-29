# apps/robot_server/main_server.py
from __future__ import annotations

import asyncio
import contextlib
import json
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from PySide6.QtWidgets import QApplication
from qasync import QEventLoop

from lib import config_common as cfgc

# ✅ 서버 프로그램 전용 ServerPage (네가 새로 추가한 파일)
# 예: robot_server/runtime/server_page.py
from robot_server.runtime.server_page import ServerPage

# ✅ 기존 Host 서버/프로토콜 재사용
from host.server import HostServer
from host.router import Router
from host.protocol import HEADER_SIZE, pack_message, unpack_header


Json = Dict[str, Any]


class UpstreamClient:
    """RobotServer -> ProcessApp(브릿지)로 요청을 전달하는 클라이언트(요청마다 1회 연결)."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        connect_timeout_s: float = 2.0,
        request_timeout_s: float = 10.0,
        log=None,
    ) -> None:
        self.host = host
        self.port = int(port)
        self.connect_timeout_s = float(connect_timeout_s)
        self.request_timeout_s = float(request_timeout_s)
        self.log = log

    async def _read_exact(self, reader: asyncio.StreamReader, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = await reader.read(n - len(buf))
            if not chunk:
                raise ConnectionError("EOF while reading from upstream")
            buf += chunk
        return buf

    async def request(self, command: str, data: Json, request_id: Optional[str] = None) -> Json:
        rid = request_id or str(uuid.uuid4())
        pkt = pack_message(command, {"request_id": rid, "data": data or {}})

        # 1) connect
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=self.connect_timeout_s,
            )
        except Exception as e:
            if self.log:
                self.log("HOST_PROXY", f"Upstream connect failed: {e!r}")
            return {"result": "fail", "message": f"ProcessApp offline(connect): {e}"}

        try:
            # 2) send
            writer.write(pkt)
            await asyncio.wait_for(writer.drain(), timeout=self.request_timeout_s)

            # 3) recv header + body
            header = await asyncio.wait_for(self._read_exact(reader, HEADER_SIZE), timeout=self.request_timeout_s)
            # unpack_header 반환형은 프로젝트 구현에 따름(일반적으로 body_len 포함)
            # 보통: (version, flags, cmd_len, body_len, ts)
            parts = unpack_header(header)
            body_len = parts[3]  # body_len 위치가 다르면 여기 조정 필요

            body = await asyncio.wait_for(self._read_exact(reader, body_len), timeout=self.request_timeout_s)
            obj = json.loads(body.decode("utf-8", errors="replace"))

            res_data = obj.get("data", {})
            if not isinstance(res_data, dict):
                return {"result": "fail", "message": f"Bad upstream response data: {type(res_data).__name__}"}
            return res_data

        except Exception as e:
            if self.log:
                self.log("HOST_PROXY", f"Upstream request failed: {e!r}")
            return {"result": "fail", "message": f"ProcessApp offline(io): {e}"}

        finally:
            with contextlib.suppress(Exception):
                writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()


def build_proxy_router(up: UpstreamClient) -> Router:
    """
    외부 로봇 명령을 ProcessApp 브릿지로 그대로 전달하는 Router.
    ✅ 여기에 등록하는 커맨드 목록은 ProcessApp(host/setup.py의 install_host 등록 목록)과 맞춰야 함.
    """
    r = Router()

    PROXY_COMMANDS = [
        "GET_SPUTTER_STATUS",
        "GET_LOADING_1_SENSOR",
        "GET_LOADING_2_SENSOR",
        "GET_RECIPE",
        "START_SPUTTER",
        "START_PLASMA_CLEANING",
        "VACUUM_ON",
        "VACUUM_OFF",
        "4PIN_UP",
        "4PIN_DOWN",
        "CH1_GATE_OPEN",
        "CH2_GATE_OPEN",
        "CH1_GATE_CLOSE",
        "CH2_GATE_CLOSE",
        "CH1_CHUCK_UP",
        "CH2_CHUCK_UP",
        "CH1_CHUCK_DOWN",
        "CH2_CHUCK_DOWN",
    ]

    async def _proxy(cmd: str, data: Json) -> Json:
        # request_id를 유지하고 싶으면(선택):
        rid = None
        if isinstance(data, dict):
            rid = data.pop("_request_id", None) or None
        return await up.request(cmd, data or {}, request_id=rid)

    for cmd in PROXY_COMMANDS:
        r.register(cmd, (lambda d, c=cmd: _proxy(c, d)))

    return r


class RobotServerApp:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop

        # 로그 루트는 메인과 동일하게 맞추고 싶으면 여기 Path를 지정(선택)
        log_root = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs")

        self.page = ServerPage(log_root=log_root)
        self.page.set_host_info(cfgc.HOST_SERVER_HOST, int(cfgc.HOST_SERVER_PORT))
        self.page.set_running(False)
        self.page.show()

        self.up = UpstreamClient(
            cfgc.PROCESS_HOST_HOST,
            int(cfgc.PROCESS_HOST_PORT),
            connect_timeout_s=getattr(cfgc, "PROCESS_HOST_CONNECT_TIMEOUT_S", 2.0),
            request_timeout_s=getattr(cfgc, "PROCESS_HOST_REQUEST_TIMEOUT_S", 10.0),
            log=self.page.append_log,
        )

        self.server: Optional[HostServer] = None

        # 버튼 연결
        self.page.sigHostStart.connect(self.request_start)
        self.page.sigHostStop.connect(self.request_stop)
        self.page.sigHostRestart.connect(self.request_restart)

        # 자동 시작(원하면 주석 처리)
        self.loop.create_task(self._start())

    def request_start(self) -> None:
        self.loop.create_task(self._start())

    def request_stop(self) -> None:
        self.loop.create_task(self._stop())

    def request_restart(self) -> None:
        self.loop.create_task(self._restart())

    async def _start(self) -> None:
        if self.server is not None:
            self.page.append_log("NET", "Robot host already running")
            return

        router = build_proxy_router(self.up)

        # HostServer 생성/시작(외부 로봇이 붙는 포트)
        self.server = HostServer(
            cfgc.HOST_SERVER_HOST,
            int(cfgc.HOST_SERVER_PORT),
            router,
            self.page.append_log,
        )
        await self.server.start()

        self.page.set_running(True)
        self.page.append_log(
            "NET",
            f"RobotHost started on {cfgc.HOST_SERVER_HOST}:{cfgc.HOST_SERVER_PORT} "
            f"-> upstream {cfgc.PROCESS_HOST_HOST}:{cfgc.PROCESS_HOST_PORT}",
        )

    async def _stop(self) -> None:
        if self.server is None:
            return

        srv = self.server
        self.server = None

        # HostServer 종료 메서드 이름이 다를 수 있어 안전 처리
        if hasattr(srv, "aclose"):
            await srv.aclose()
        elif hasattr(srv, "stop"):
            await srv.stop()

        self.page.set_running(False)
        self.page.append_log("NET", "RobotHost stopped")

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

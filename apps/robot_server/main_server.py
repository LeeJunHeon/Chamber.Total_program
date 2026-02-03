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

# ✅ 개발(py 실행)일 때만 sys.path 보정
# ✅ PyInstaller(EXE, frozen)에서는 번들 내부 모듈만 사용해야 "코드 섞임"이 안 생김
if not getattr(sys, "frozen", False):
    ROOT = Path(__file__).resolve().parents[2]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

# 이제부터 프로젝트 모듈 import가 안정적임
import lib.config_common as cfgc

# ✅ 서버 프로그램 전용 ServerPage (네가 새로 추가한 파일)
from apps.robot_server.runtime.server_page import ServerPage

# ✅ 기존 Host 서버/프로토콜 재사용
from host.server import HostServer
from host.router import Router
from host.protocol import HEADER_SIZE, pack_message, unpack_header


Json = Dict[str, Any]


class UpstreamClient:
    """RobotServer -> ProcessApp(브릿지)로 요청을 전달하는 클라이언트(✅ 지속 연결)."""

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

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

        # ✅ 한 연결에서 동시에 여러 request가 섞이면 프로토콜이 깨질 수 있으니 직렬화
        self._lock = asyncio.Lock()

    async def aclose(self) -> None:
        if self._writer is not None:
            with contextlib.suppress(Exception):
                self._writer.close()
            with contextlib.suppress(Exception):
                await self._writer.wait_closed()
        self._reader = None
        self._writer = None

    async def _ensure_connected(self) -> None:
        if self._writer is not None and not self._writer.is_closing():
            return

        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=self.connect_timeout_s,
        )
        if self.log:
            self.log("HOST_PROXY", f"Upstream connected: {self.host}:{self.port}")

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
        pkt = pack_message(command, data or {}, request_id=rid)

        async with self._lock:
            # ✅ 1회 실패 시 연결 닫고 재연결 후 1회 재시도
            for attempt in range(2):
                try:
                    await self._ensure_connected()

                    assert self._writer is not None
                    assert self._reader is not None

                    # send
                    self._writer.write(pkt)
                    await asyncio.wait_for(self._writer.drain(), timeout=min(5.0, self.request_timeout_s))

                    # recv header + body
                    header = await asyncio.wait_for(
                        self._read_exact(self._reader, HEADER_SIZE),
                        timeout=self.request_timeout_s,
                    )
                    parts = unpack_header(header)
                    body_len = parts[3]

                    body = await asyncio.wait_for(
                        self._read_exact(self._reader, body_len),
                        timeout=self.request_timeout_s,
                    )

                    obj = json.loads(body.decode("utf-8", errors="replace"))
                    res_data = obj.get("data", {})
                    if not isinstance(res_data, dict):
                        return {"result": "fail", "message": f"Bad upstream response data: {type(res_data).__name__}"}
                    return res_data

                except Exception as e:
                    if self.log:
                        self.log("HOST_PROXY", f"Upstream error cmd={command} rid={rid} attempt={attempt+1}: {e!r}")

                    # ✅ timeout/끊김/파싱오류 등 어떤 예외든 연결 상태가 불명확해질 수 있으니 닫고 재연결
                    await self.aclose()

            return {"result": "fail", "message": f"ProcessApp offline/timeout cmd={command} rid={rid}"}


def build_proxy_router(up_status: UpstreamClient, up_cmd: UpstreamClient) -> Router:
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
        # ✅ data 원본을 pop으로 변형하지 않게 복사
        payload = dict(data) if isinstance(data, dict) else {}
        rid = payload.pop("_request_id", None) or None

        # ✅ 너 요구대로: GET_SPUTTER_STATUS만 status 채널로
        up = up_status if cmd == "GET_SPUTTER_STATUS" else up_cmd
        return await up.request(cmd, payload, request_id=rid)

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

        # ✅ 상태 전용 연결
        self.up_status = UpstreamClient(
            cfgc.PROCESS_HOST_HOST,
            int(cfgc.PROCESS_HOST_PORT),
            connect_timeout_s=getattr(cfgc, "PROCESS_HOST_CONNECT_TIMEOUT_S", 2.0),
            request_timeout_s=getattr(cfgc, "PROCESS_HOST_STATUS_REQUEST_TIMEOUT_S", 5.0),
            log=self.page.append_log,
        )

        # ✅ 명령 전용 연결(오래 걸리는 동작 대비)
        self.up_cmd = UpstreamClient(
            cfgc.PROCESS_HOST_HOST,
            int(cfgc.PROCESS_HOST_PORT),
            connect_timeout_s=getattr(cfgc, "PROCESS_HOST_CONNECT_TIMEOUT_S", 2.0),
            request_timeout_s=getattr(cfgc, "PROCESS_HOST_CMD_REQUEST_TIMEOUT_S", 180.0),
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
        
        # ✅ 업스트림 2개를 미리 붙여서 “항상 연결 유지” 상태로 시작
        await self.up_status._ensure_connected()
        await self.up_cmd._ensure_connected()

        router = build_proxy_router(self.up_status, self.up_cmd)

        # HostServer 생성/시작(외부 로봇이 붙는 포트)
        self.server = HostServer(
            cfgc.HOST_SERVER_HOST,
            int(cfgc.HOST_SERVER_PORT),
            router,
            self.page.append_log,
            csv_prefix="robot_server_cmd",
            csv_subdir="RobotServer",
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

        # ✅ upstream 지속 연결도 닫기(정지 시 유령 연결 방지)
        with contextlib.suppress(Exception):
            await self.up_status.aclose()
        with contextlib.suppress(Exception):
            await self.up_cmd.aclose()

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

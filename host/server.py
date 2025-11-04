# host/server.py
# -*- coding: utf-8 -*-
"""
TCP 서버(I/O 전용)
- 연결 수락, 정확히 N바이트 읽기, JSON 디코드, 라우터 호출, 응답 write
- 비즈니스 로직/장비 제어는 호출하지 않는다 (router/handlers가 담당)
"""
from __future__ import annotations
import asyncio, json, contextlib, traceback
from typing import Optional, Callable, Dict, Any
from .protocol import HEADER_SIZE, unpack_header, pack_message, PROTOCOL_VERSION
from .router import Router

Json = Dict[str, Any]
LogFn = Callable[[str, str], None]


class HostServer:
    def __init__(self, host: str, port: int, router: Router, log: LogFn) -> None:
        self.host = host
        self.port = port
        self.router = router
        self.log = log
        self._server: Optional[asyncio.AbstractServer] = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        sock = next(iter(self._server.sockets or []), None)
        self.log("NET", f"Host started on {sock.getsockname() if sock else (self.host, self.port)}")

    async def aclose(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self.log("NET", "Host closed")

    async def _read_exact(self, r: asyncio.StreamReader, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = await r.read(n - len(buf))
            if not chunk:
                raise ConnectionError("EOF while reading")
            buf += chunk
        return buf

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        self.log("NET", f"Client connected: {peer}")
        try:
            while True:
                # 1) 헤더
                header = await self._read_exact(reader, HEADER_SIZE)
                version, flags, cmd_len, body_len, ts = unpack_header(header)
                if version != PROTOCOL_VERSION:
                    raise ValueError(f"Unsupported protocol version: {version}")

                # 2) 바디(JSON)
                body = await self._read_exact(reader, body_len)

                # (A) JSON 파싱 실패도 표준 실패 응답
                try:
                    obj: Json = json.loads(body.decode("utf-8"))
                except Exception as e:
                    # 요청 ID 추출 불가 시 빈 문자열로 회신
                    packet = pack_message("PARSE_ERROR_RESULT", {
                        "request_id": "",
                        "data": {"result": "fail", "message": f"Invalid JSON: {e}"}
                    })
                    writer.write(packet)
                    await writer.drain()
                    continue  # 다음 요청 대기

                req_id = str(obj.get("request_id", ""))
                cmd = str(obj.get("command", ""))
                data = obj.get("data", {}) or {}

                # (B) command 미지정/공백도 표준 실패 응답
                if not cmd:
                    packet = pack_message("UNKNOWN_RESULT", {
                        "request_id": req_id,
                        "data": {"result": "fail", "message": "Missing 'command' in request"}
                    })
                    writer.write(packet)
                    await writer.drain()
                    continue

                # 3) 라우팅/실행
                try:
                    res_cmd, res_data = await self.router.dispatch(cmd, data)
                except Exception as e:
                    self.log("NET", f"Handler error for {cmd}: {e}\n{traceback.format_exc()}")
                    res_cmd, res_data = f"{cmd}_RESULT", {"result": "fail", "message": str(e)}

                # 4) 응답
                packet = pack_message(res_cmd, {"request_id": req_id, "data": res_data})
                writer.write(packet)
                await writer.drain()
        except Exception as e:
            self.log("NET", f"Client error/disconnect {peer}: {e}")
        finally:
            with contextlib.suppress(Exception):
                writer.close()
                await writer.wait_closed()
            self.log("NET", f"Client closed: {peer}")

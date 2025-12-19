# host/server.py
# -*- coding: utf-8 -*-
"""
TCP 서버(I/O 전용)
- 연결 수락, 정확히 N바이트 읽기, JSON 디코드, 라우터 호출, 응답 write
- 비즈니스 로직/장비 제어는 호출하지 않는다 (router/handlers가 담당)
"""
from __future__ import annotations
import asyncio, json, contextlib, traceback, csv, time
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable, Dict, Any
from .protocol import HEADER_SIZE, unpack_header, pack_message, PROTOCOL_VERSION
from .router import Router

Json = Dict[str, Any]
LogFn = Callable[[str, str], None]

class DailyCommandCsvLogger:
    """
    하루에 파일 1개(remote_cmd_YYYYMMDD.csv)만 만들고,
    그날 들어온 모든 요청/응답을 한 파일에 append.
    NAS 실패 시 로컬 Logs/PLC_Remote 로 자동 폴백.
    """
    HEADER = [
        "server_time",
        "peer",
        "request_id",
        "req_command",
        "req_data_json",
        "res_command",
        "res_result",
        "res_message",
        "res_data_json",
        "duration_ms",
    ]

    def __init__(self) -> None:
        self._lock: asyncio.Lock | None = None
        self._dir = self._init_dir()

    def _init_dir(self) -> Path:
        # NAS 우선
        try:
            root = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs")
            d = root / "PLC_Remote"
            d.mkdir(parents=True, exist_ok=True)
            return d
        except Exception:
            d = Path.cwd() / "Logs" / "PLC_Remote"
            d.mkdir(parents=True, exist_ok=True)
            return d

    def _file_path(self, now: datetime | None = None) -> Path:
        now = now or datetime.now()
        return self._dir / f"remote_cmd_{now:%Y%m%d}.csv"

    def _ensure_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _write_row_sync(self, file_path: Path, row: dict) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        new_file = (not file_path.exists()) or (file_path.stat().st_size == 0)

        with open(file_path, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=self.HEADER)
            if new_file:
                w.writeheader()
            w.writerow(row)

    async def append(self, row: dict) -> None:
        lock = self._ensure_lock()
        fn = self._file_path()

        async with lock:
            try:
                await asyncio.to_thread(self._write_row_sync, fn, row)
            except Exception:
                # NAS 실패 → 로컬 폴백
                local = (Path.cwd() / "Logs" / "PLC_Remote" / fn.name)
                local.parent.mkdir(parents=True, exist_ok=True)
                await asyncio.to_thread(self._write_row_sync, local, row)

class HostServer:
    def __init__(self, host: str, port: int, router: Router, log: LogFn) -> None:
        self.host = host
        self.port = port
        self.router = router
        self.log = log
        self._server: Optional[asyncio.AbstractServer] = None
        self._cmd_csv = DailyCommandCsvLogger()

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
                    # === JSON 깨진 요청 로그 ===
                    # body가 뭔지, 어떤 에러였는지 Plasma Cleaning 로그창 + NET 로그에 남김
                    try:
                        # Plasma Cleaning 로그창용
                        self.log(
                            "PLC_HOST",
                            f"[REQ] peer={peer} INVALID_JSON body={body!r} error={e}"
                        )
                    except Exception as log_err:
                        # 로그 자체가 또 실패하면 NET 태그로 한 번 더 남김
                        try:
                            self.log(
                                "NET",
                                f"[LOG_ERROR] failed to log INVALID_JSON (peer={peer}): {log_err}"
                            )
                        except Exception:
                            pass

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

                # === 클라이언트 → 서버 요청 로그 ===
                # Plasma Cleaning 로그창에 어떤 명령이 들어왔는지 남김
                try:
                    self.log(
                        "PLC_HOST",  # 필요시 "NET" 으로 바꿔도 됨
                        f"[REQ] peer={peer} id={req_id} cmd={cmd} data={data}"
                    )
                except Exception as e:
                    # PLC_HOST 로그 자체가 실패했을 때, 어떤 에러였는지 NET 태그로 한 번 더 남김
                    try:
                        self.log(
                            "NET",
                            f"[LOG_ERROR] failed to log REQ (peer={peer}, cmd={cmd}, id={req_id}): {e}"
                        )
                    except Exception:
                        # 여기까지도 실패하면 어쩔 수 없이 무시 (통신은 계속)
                        pass

                # (B) command 미지정/공백도 표준 실패 응답
                if not cmd:
                    packet = pack_message("UNKNOWN_RESULT", {
                        "request_id": req_id,
                        "data": {"result": "fail", "message": "Missing 'command' in request"}
                    })
                    writer.write(packet)
                    await writer.drain()
                    continue

                t0 = time.perf_counter()
                try:
                    res_cmd, res_data = await self.router.dispatch(cmd, data)
                except Exception as e:
                    self.log("NET", f"Handler error for {cmd}: {e}\n{traceback.format_exc()}")
                    res_cmd, res_data = f"{cmd}_RESULT", {"result": "fail", "message": str(e)}
                dt_ms = int((time.perf_counter() - t0) * 1000)

                packet = pack_message(res_cmd, {"request_id": req_id, "data": res_data})
                writer.write(packet)
                await writer.drain()

                # ✅ 하루 1개 CSV에 누적 기록 (요청+응답 한 줄)
                row = {
                    "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "peer": str(peer),
                    "request_id": req_id,
                    "req_command": cmd,
                    "req_data_json": json.dumps(data, ensure_ascii=False),
                    "res_command": res_cmd,
                    "res_result": str(res_data.get("result", "")),
                    "res_message": str(res_data.get("message", "")),
                    "res_data_json": json.dumps(res_data, ensure_ascii=False),
                    "duration_ms": dt_ms,
                }
                asyncio.create_task(self._cmd_csv.append(row))

                # === 서버 → 클라이언트 응답 로그 ===
                # 내가 어떤 응답을 보냈는지 Plasma Cleaning 로그창에 남김
                try:
                    self.log(
                        "PLC_HOST",  # 필요시 "NET" 으로 변경 가능
                        f"[RES] peer={peer} id={req_id} cmd={res_cmd} data={res_data}"
                    )
                except Exception as e:
                    try:
                        self.log(
                            "NET",
                            f"[LOG_ERROR] failed to log RES (peer={peer}, cmd={res_cmd}, id={req_id}): {e}"
                        )
                    except Exception:
                        pass
        except Exception as e:
            # 네트워크 끊김(정상 종료)에 대해서는 깔끔한 메시지만 남기고,
            # 그 외 에러만 자세히 찍어준다.
            msg = str(e) if e else ""
            normal_disconnect = False

            # 우리 쪽에서 직접 raise 하는 ConnectionError("EOF while reading")
            # + 일반적인 소켓 끊김 예외들
            if isinstance(e, (ConnectionError,
                               ConnectionResetError,
                               BrokenPipeError,
                               asyncio.IncompleteReadError)):
                normal_disconnect = True
            # Windows에서 자주 나오는 WinError 64 메시지(지정된 네트워크 이름을 더 이상 사용할 수 없습니다)
            elif isinstance(e, OSError) and "지정된 네트워크 이름을 더 이상 사용할 수 없습니다" in msg:
                normal_disconnect = True

            if normal_disconnect:
                self.log("NET", f"Client disconnected: {peer}")
            else:
                # 진짜 이상한 예외는 기존처럼 상세 메시지 유지
                self.log("NET", f"Client error from {peer}: {e!r}")
        finally:
            with contextlib.suppress(Exception):
                writer.close()
                await writer.wait_closed()
            self.log("NET", f"Client closed: {peer}")

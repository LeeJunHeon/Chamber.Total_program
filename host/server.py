# host/server.py
# -*- coding: utf-8 -*-
"""
TCP 서버(I/O 전용)
- 연결 수락, 정확히 N바이트 읽기, JSON 디코드, 라우터 호출, 응답 write
- 비즈니스 로직/장비 제어는 호출하지 않는다 (router/handlers가 담당)
"""
from __future__ import annotations
import asyncio, json, contextlib, traceback, time
from util.log_hub import DailyCsvDictAppender
from lib import config_common as cfgc
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable, Dict, Any
from .protocol import HEADER_SIZE, unpack_header, pack_message, PROTOCOL_VERSION
from errors.error_reporter import (
    notify_error_code,
    notify_unknown_error,
)
from .router import Router

Json = Dict[str, Any]
LogFn = Callable[[str, str], None]

class DailyCommandCsvLogger:
    """
    하루에 파일 1개(remote_cmd_YYYYMMDD.csv)만 만들고,
    그날 들어온 모든 요청/응답을 한 파일에 append.

    ✅ keep-handle(파일 핸들 유지) 방식
    ✅ NAS(UNC) 우선 → 실패 시 로컬 폴백 → 이후 주기적으로 NAS 재시도
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

        primary = Path(getattr(cfgc, "LOG_ROOT_DIR",
                       r"G:\공유 드라이브\VanaM_Sputter\Sputter\Logs\CH1&2")) / "CH1&2_Server"
        fallback = Path(
            getattr(
                cfgc,
                "LOCAL_FALLBACK_SERVER_DIR",
                Path.cwd() / "Logs_LocalFallback" / "Server",
            )
        )

        self._writer = DailyCsvDictAppender(
            primary_dir=primary,
            fallback_dir=fallback,
            filename_builder=lambda dt: f"remote_cmd_{dt:%Y%m%d}.csv",
            fieldnames=self.HEADER,
            encoding="utf-8-sig",
            retry_primary_every_s=10.0,  # NAS 복구되면 10초마다 다시 붙기 시도
        )

    def _ensure_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def append(self, row: dict) -> None:
        lock = self._ensure_lock()
        dt = datetime.now()

        async with lock:
            try:
                # ✅ keep-handle append
                await asyncio.to_thread(self._writer.append_row, dt=dt, row=row)

                # ✅ 이번 write에서 NAS→LOCAL 전환이 발생했으면 marker 1줄만 추가(원하면 유지)
                if self._writer.consume_switched_flag():
                    err = self._writer.state.last_error
                    msg = f"NAS write failed -> switched to LOCAL (keep-handle). reason={err!r}"
                    if len(msg) > 500:
                        msg = msg[:500] + "...(truncated)"

                    marker = {
                        "server_time": dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "peer": str(row.get("peer", "")),
                        "request_id": str(row.get("request_id", "")),
                        "req_command": "__NAS_MISSING__",
                        "req_data_json": "",
                        "res_command": "",
                        "res_result": "WARN",
                        "res_message": msg,
                        "res_data_json": "",
                        "duration_ms": str(row.get("duration_ms", "")),
                    }
                    await asyncio.to_thread(self._writer.append_row, dt=dt, row=marker)

            except Exception:
                # ✅ 서버 기능에 영향 주지 않도록: CSV 로깅 실패는 무시
                pass

    async def aclose(self) -> None:
        lock = self._ensure_lock()
        async with lock:
            try:
                await asyncio.to_thread(self._writer.close)
            except Exception:
                pass


class HostServer:
    def __init__(self, host: str, port: int, router: Router, log: LogFn, chat=None, popup=None) -> None:
        self.host = host
        self.port = port
        self.router = router
        self.log = log
        self.chat = chat
        self.popup = popup
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

        # ✅ keep-handle CSV logger close
        try:
            await self._cmd_csv.aclose()
        except Exception:
            pass

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
            def _safe_json(obj: Any) -> str:
                try:
                    return json.dumps(obj, ensure_ascii=False, default=str)
                except Exception as je:
                    return f"<<JSON_DUMP_ERROR {je!r}>> {repr(obj)[:300]}"

            def _cut(s: str, n: int = 500) -> str:
                return s if len(s) <= n else s[:n] + "...(truncated)"

            while True:
                # 1) 헤더
                header = await self._read_exact(reader, HEADER_SIZE)
                version, flags, cmd_len, body_len, ts = unpack_header(header)
                if version != PROTOCOL_VERSION:
                    fail = notify_error_code(
                        "E102",
                        log=self.log,
                        chat=self.chat,
                        popup=self.popup,
                        src="HOST",
                        detail=f"Unsupported protocol version: {version}",
                        meta={"peer": str(peer)},
                    )

                    packet = pack_message("VERSION_ERROR_RESULT", {
                        "request_id": "",
                        "data": fail,
                    })

                    writer.write(packet)
                    await writer.drain()
                    break

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

                    # ✅ CSV에 1줄 남기기 (continue 전에)
                    try:
                        bad_txt = body.decode("utf-8", errors="replace")
                        row = {
                            "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "peer": str(peer),
                            "request_id": "",
                            "req_command": "__INVALID_JSON__",
                            "req_data_json": _cut(bad_txt, 500),
                            "res_command": "PARSE_ERROR_RESULT",
                            "res_result": "fail",
                            "res_message": _cut(f"Invalid JSON: {e!r}", 500),
                            "res_data_json": "",
                            "duration_ms": 0,
                        }
                        await self._cmd_csv.append(row)
                    except Exception:
                        pass

                    fail = notify_error_code(
                        "E104",
                        log=self.log,
                        chat=self.chat,
                        popup=self.popup,
                        src="HOST",
                        detail=f"Invalid JSON: {e}",
                        meta={"peer": str(peer)},
                    )

                    packet = pack_message("PARSE_ERROR_RESULT", {
                        "request_id": "",
                        "data": fail
                    })
                    writer.write(packet)
                    await writer.drain()
                    continue

                req_id = str(obj.get("request_id", ""))
                cmd = str(obj.get("command", ""))
                raw_data = obj.get("data", {})
                data = raw_data if isinstance(raw_data, dict) else {}

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
                    # ✅ CSV에 1줄 남기기 (continue 전에)
                    try:
                        row = {
                            "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "peer": str(peer),
                            "request_id": req_id,
                            "req_command": "__MISSING_COMMAND__",
                            "req_data_json": _safe_json(obj),  # 요청 전문을 남겨두면 원인추적 쉬움
                            "res_command": "UNKNOWN_RESULT",
                            "res_result": "fail",
                            "res_message": "Missing 'command' in request",
                            "res_data_json": "",
                            "duration_ms": 0,
                        }
                        await self._cmd_csv.append(row)
                    except Exception:
                        pass

                    fail = notify_error_code(
                        "E105",
                        log=self.log,
                        chat=self.chat,
                        popup=self.popup,
                        src="HOST",
                        detail="Missing 'command' in request",
                        meta={"peer": str(peer), "request_id": req_id},
                    )

                    packet = pack_message("UNKNOWN_RESULT", {
                        "request_id": req_id,
                        "data": fail
                    })
                    writer.write(packet)
                    await writer.drain()
                    continue

                t0 = time.perf_counter()
                try:
                    res_cmd, res_data = await self.router.dispatch(cmd, data)
                except Exception as e:
                    tb = traceback.format_exc()
                    self.log("NET", f"Handler error for {cmd}: {e}\n{tb}")

                    fail = notify_unknown_error(
                        e,
                        log=self.log,
                        chat=self.chat,
                        popup=self.popup,
                        src="HOST",
                        meta={
                            "peer": str(peer),
                            "request_id": req_id,
                            "cmd": cmd,
                        },
                    )
                    res_cmd, res_data = f"{cmd}_RESULT", fail

                dt_ms = int((time.perf_counter() - t0) * 1000)

                try:
                    # res_data가 dict가 아니면 방어
                    if not isinstance(res_data, dict):
                        res_data = notify_error_code(
                            "E111",
                            log=self.log,
                            chat=self.chat,
                            popup=self.popup,
                            src="HOST",
                            detail=f"Handler returned non-dict: {type(res_data).__name__} raw={repr(res_data)}",
                            meta={
                                "peer": str(peer),
                                "request_id": req_id,
                                "cmd": cmd,
                            },
                        )

                    # ✅ fail인데 error_code 없으면: server는 '결정'하지 않고 기본값만 넣는다
                    if isinstance(res_data, dict) and res_data.get("result") == "fail" and "error_code" not in res_data:
                        self.log("NET", f"[WARN] fail without error_code: cmd={cmd} id={req_id} res={res_data!r}")
                        res_data = notify_error_code(
                            "E110",
                            log=self.log,
                            chat=self.chat,
                            popup=self.popup,
                            src="HOST",
                            detail=f"Fail payload missing error_code: {res_data!r}",
                            meta={
                                "peer": str(peer),
                                "request_id": req_id,
                                "cmd": cmd,
                            },
                        )

                    row = {
                        "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "peer": str(peer),
                        "request_id": req_id,
                        "req_command": cmd,
                        "req_data_json": _safe_json(data),
                        "res_command": res_cmd,
                        "res_result": str(res_data.get("result", "")),
                        "res_message": str(res_data.get("message", "")),
                        "res_data_json": _safe_json(res_data),
                        "duration_ms": dt_ms,
                    }
                    await self._cmd_csv.append(row)

                except Exception as log_e:
                    # ✅ 로깅 자체가 실패했을 때도 CSV에 1줄 남김
                    try:
                        err_row = {
                            "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "peer": str(peer),
                            "request_id": req_id,
                            "req_command": cmd or "__UNKNOWN__",
                            "req_data_json": _safe_json(data),
                            "res_command": "__LOGGING_EXCEPTION__",
                            "res_result": "fail",
                            "res_message": _cut(f"Failed to build/append csv row: {log_e!r}", 500),
                            "res_data_json": _safe_json({"res_cmd": res_cmd, "res_data_repr": repr(res_data)}),
                            "duration_ms": dt_ms if isinstance(dt_ms, int) else 0,
                        }
                        await self._cmd_csv.append(err_row)
                    except Exception:
                        pass

                try:
                    packet = pack_message(res_cmd, {"request_id": req_id, "data": res_data})
                except Exception as pe:
                    fail = notify_error_code(
                        "E107",
                        log=self.log,
                        chat=self.chat,
                        popup=self.popup,
                        src="HOST",
                        detail=f"Response serialization failed: {type(pe).__name__}: {pe}",
                        meta={
                            "peer": str(peer),
                            "request_id": req_id,
                            "cmd": cmd,
                            "res_cmd": res_cmd,
                        },
                    )
                    packet = pack_message(res_cmd, {"request_id": req_id, "data": fail})

                writer.write(packet)
                await writer.drain()

                # === 서버 → 클라이언트 응답 로그 ===
                # 내가 어떤 응답을 보냈는지 server 로그창에 남김
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
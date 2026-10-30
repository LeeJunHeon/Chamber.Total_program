"""로봇(호스트) TCP 서버 — 연결·프레이밍·로그는 v1 host/server.py·host/router.py 그대로, 등록 명령은 내부 명령 API 로 넘긴다.

v1 과 같게 하는 것(바이트·로그 줄·순서)
- 연결마다 태스크 하나(asyncio.start_server), 한 연결 안 요청은 순차(응답을 보낸 뒤 다음 헤더를 읽음), 연결끼리는 동시.
- 헤더 16바이트·바디 body_len 바이트를 정확히 읽는다(EOF → ConnectionError('EOF while reading') → 'Client disconnected').
- version != 1 → E102 보고 + 'VERSION_ERROR_RESULT'(request_id '') 응답 후 연결 종료(server.py:188-206).
- 바디 JSON 파싱 실패(UTF-8 아님 포함) → PLC_HOST '[REQ] .. INVALID_JSON ..' + CSV + E104 보고 + 'PARSE_ERROR_RESULT', 연결 유지.
- 바디가 JSON 이지만 객체가 아니면 obj.get 에서 AttributeError → 응답 없이 'Client error from ..' 후 연결 종료
  (server.py:270 — v1 이상 동작 그대로, decisions D1xx).
- command 빈 문자열(없음 포함) → CSV + E105 보고 + 'UNKNOWN_RESULT'(request_id 유지).
- 라우터(router.py): upper().strip() 정규화, 공백만이면 'UNKNOWN_RESULT' E105(보고 없음), 미등록이면 '{C}_RESULT' E106(보고 없음).
- 등록 명령 → 내부 명령 API(forward.py). 응답 명령·data·로그 줄은 제어 프로그램이 v1 handlers·server except 경로와 같게 만든다
  (핸들러 예외 때 원문 명령 + '_RESULT' 도 제어 프로그램 쪽에서 — v1 이상 동작 그대로).
- 서버 방어(server.py:369-400): data 가 dict 가 아니면 E111, result 'fail' 인데 error_code 가 없으면 NET 경고 + E110.
- 응답 직렬화 실패 → E107 로 다시 pack(server.py:435-452).
- 응답 write/drain 뒤 remote_cmd CSV 1줄, 그 뒤 PLC_HOST '[RES] ..' 로그.
- 연결 로그 NET 'Client connected/disconnected/closed: {peer}', 'Client error from {peer}: {e!r}'. peer = str(peername 튜플).

v1 과 다른 것
- 실패 보고(notify_* — E102·E104·E105·E107·E110·E111, 서버 자체 예외 E999)는 서버 로그('ERROR/HOST' 줄) 뒤 제어 프로그램의
  내부 명령 API POST /robot/v1/event 로 넘긴다(결정 D09 — 구글챗 카드는 포털, 팝업은 노트북 화면이 같은 문구로). 보내기는 별도
  태스크라 응답 바이트·순서·로그 줄은 v1 그대로이고, 제어 프로그램이 꺼져 있으면 보내지 못한 채 로그만 남는다(다시 보내지 않음).
- 제어 프로그램이 꺼져 있으면 cache.py 규칙대로 응답(연결 유지).
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from v2.robot_server.cache import ReplyCache
from v2.robot_server.forward import COMMANDS, Forwarder, ForwardDown
from v2.robot_server.log import CommandCsv
from v2.robot_server.protocol import HEADER_SIZE, PROTOCOL_VERSION, error_payload, pack_message, report_event, \
    report_log_text, unpack_header

Json = Dict[str, Any]
LogFn = Callable[[str, str], None]
_REGISTERED = frozenset(COMMANDS)


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception as je:
        return f"<<JSON_DUMP_ERROR {je!r}>> {repr(obj)[:300]}"


def _cut(s: str, n: int = 500) -> str:
    return s if len(s) <= n else s[:n] + "...(truncated)"


class RobotServer:
    def __init__(self, host: str, port: int, forwarder: Forwarder, log: LogFn, *,
                 csv: Optional[CommandCsv] = None, cache: Optional[ReplyCache] = None,
                 now: Callable[[], float] = time.time) -> None:
        self.host, self.port = host, port
        self.forwarder = forwarder
        self.log = log
        self.csv = csv
        self.cache = cache if cache is not None else ReplyCache(None)
        self.now = now
        self._server: Optional[asyncio.AbstractServer] = None
        self.down_count = 0          # 제어 프로그램 꺼짐 응답 수(진단·시험용)
        self._writers: set = set()   # 열린 연결(종료 때 닫기)
        self._event_tasks: set = set()   # 보내는 중인 실패 보고(D09)
        self.events_sent = 0         # 제어 프로그램이 받은 실패 보고 수(진단·시험)
        self.events_failed = 0       # 못 보낸 실패 보고 수(제어 프로그램 꺼짐 등 — 로그만 남음)

    # ── 시작·종료 ──
    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        sock = next(iter(self._server.sockets or []), None)
        self.log("NET", f"Host started on {sock.getsockname() if sock else (self.host, self.port)}")

    @property
    def bound_port(self) -> int:
        sock = next(iter(self._server.sockets or []), None) if self._server else None
        return int(sock.getsockname()[1]) if sock else int(self.port)

    async def aclose(self) -> None:
        if self._server:
            self._server.close()
            # 열린 로봇 연결도 닫는다 — Python 3.12+ 의 wait_closed 는 연결이 모두 끝날 때까지 기다린다
            for w in list(self._writers):
                with contextlib.suppress(Exception):
                    w.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._server.wait_closed(), timeout=5.0)
            self.log("NET", "Host closed")
        await self.drain_events()
        if self.csv is not None:
            with contextlib.suppress(Exception):
                await self.csv.aclose()

    async def drain_events(self, timeout_s: float = 5.0) -> None:
        """보내는 중인 실패 보고를 기다린다(종료·시험)."""
        tasks = [t for t in self._event_tasks if not t.done()]
        if tasks:
            with contextlib.suppress(Exception):
                await asyncio.wait(tasks, timeout=timeout_s)

    # ── 보고·기록 ──
    def _notify(self, code: str, detail: str, meta: Json) -> Json:
        """v1 notify_error_code — payload 만들고 'ERROR/HOST' 로그."""
        p = error_payload(code, detail=detail, meta=meta)
        with contextlib.suppress(Exception):
            self.log("ERROR/HOST", report_log_text(p))
        self._send_event(p)
        return p

    def _send_event(self, payload: Json) -> None:
        """실패 보고를 제어 프로그램으로(D09) — 기다리지 않는다(응답·로그 순서를 바꾸지 않게)."""
        try:
            ev = report_event(payload, "HOST")
            task = asyncio.get_running_loop().create_task(self._post_event(ev))
        except Exception:
            self.events_failed += 1
            return
        self._event_tasks.add(task)
        task.add_done_callback(self._event_tasks.discard)

    async def _post_event(self, ev: Json) -> None:
        send = getattr(self.forwarder, "send_event", None)
        ok = False
        if send is not None:
            with contextlib.suppress(Exception):
                ok = bool(await send(ev))
        if ok:
            self.events_sent += 1
        else:
            self.events_failed += 1

    async def _csv(self, row: dict) -> None:
        if self.csv is not None:
            with contextlib.suppress(Exception):
                await self.csv.append(row)

    def _pack(self, command: str, payload: Json) -> bytes:
        return pack_message(command, payload, now=self.now)

    @staticmethod
    async def _read_exact(r: asyncio.StreamReader, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = await r.read(n - len(buf))
            if not chunk:
                raise ConnectionError("EOF while reading")
            buf += chunk
        return buf

    # ── 등록 명령 처리(v1 router.dispatch 자리) ──
    async def _dispatch(self, cmd: str, data: Json, raw: Json, peer: Any) -> tuple:
        c = str(cmd or "").upper().strip()
        if not c:
            return "UNKNOWN_RESULT", error_payload("E105", detail="Missing command", meta={"command": cmd})
        if c not in _REGISTERED:
            return f"{c}_RESULT", error_payload("E106", detail=f"Unknown command: {cmd}", meta={"command": cmd})
        peer_s = f"{peer[0]}:{peer[1]}" if isinstance(peer, tuple) and len(peer) >= 2 else str(peer)
        try:
            reply_cmd, body, lines = await self.forwarder.forward(
                c, data, raw, peer_s, datetime.now().isoformat(timespec="milliseconds"))
        except ForwardDown as e:
            self.down_count += 1
            reply_cmd, body, used = self.cache.down_reply(c, data)
            with contextlib.suppress(Exception):
                self.log("NET", f"[WARN] control program unreachable: cmd={c} reason={e} -> "
                                f"{'last reply (busy)' if used else body.get('error_code')}")
            if body.get("result") == "fail":
                with contextlib.suppress(Exception):
                    self.log("ERROR/HOST", report_log_text(body))
            return reply_cmd, body
        for tag, text in lines:
            with contextlib.suppress(Exception):
                self.log(tag, text)
        with contextlib.suppress(Exception):
            self.cache.put(c, data, body)
        return reply_cmd, body

    # ── 연결 ──
    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        self._writers.add(writer)
        self.log("NET", f"Client connected: {peer}")
        try:
            while True:
                # 1) 헤더
                header = await self._read_exact(reader, HEADER_SIZE)
                version, flags, cmd_len, body_len, ts = unpack_header(header)
                if version != PROTOCOL_VERSION:
                    fail = self._notify("E102", f"Unsupported protocol version: {version}", {"peer": str(peer)})
                    writer.write(self._pack("VERSION_ERROR_RESULT", {"request_id": "", "data": fail}))
                    await writer.drain()
                    break

                # 2) 바디(JSON)
                body = await self._read_exact(reader, body_len)
                try:
                    obj: Json = json.loads(body.decode("utf-8"))
                except Exception as e:
                    try:
                        self.log("PLC_HOST", f"[REQ] peer={peer} INVALID_JSON body={body!r} error={e}")
                    except Exception as log_err:
                        with contextlib.suppress(Exception):
                            self.log("NET", f"[LOG_ERROR] failed to log INVALID_JSON (peer={peer}): {log_err}")
                    try:
                        bad_txt = body.decode("utf-8", errors="replace")
                        await self._csv({
                            "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "peer": str(peer),
                            "request_id": "", "req_command": "__INVALID_JSON__", "req_data_json": _cut(bad_txt, 500),
                            "res_command": "PARSE_ERROR_RESULT", "res_result": "fail",
                            "res_message": _cut(f"Invalid JSON: {e!r}", 500), "res_data_json": "", "duration_ms": 0,
                        })
                    except Exception:
                        pass
                    fail = self._notify("E104", f"Invalid JSON: {e}", {"peer": str(peer)})
                    writer.write(self._pack("PARSE_ERROR_RESULT", {"request_id": "", "data": fail}))
                    await writer.drain()
                    continue

                # 객체가 아니면 여기서 AttributeError → 바깥 except(응답 없이 끊김) — v1 이상 동작 그대로
                req_id = str(obj.get("request_id", ""))
                cmd = str(obj.get("command", ""))
                raw_data = obj.get("data", {})
                data = raw_data if isinstance(raw_data, dict) else {}

                try:
                    self.log("PLC_HOST", f"[REQ] peer={peer} id={req_id} cmd={cmd} data={data}")
                except Exception as e:
                    with contextlib.suppress(Exception):
                        self.log("NET", f"[LOG_ERROR] failed to log REQ (peer={peer}, cmd={cmd}, id={req_id}): {e}")

                if not cmd:
                    await self._csv({
                        "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "peer": str(peer),
                        "request_id": req_id, "req_command": "__MISSING_COMMAND__", "req_data_json": _safe_json(obj),
                        "res_command": "UNKNOWN_RESULT", "res_result": "fail",
                        "res_message": "Missing 'command' in request", "res_data_json": "", "duration_ms": 0,
                    })
                    fail = self._notify("E105", "Missing 'command' in request", {"peer": str(peer), "request_id": req_id})
                    writer.write(self._pack("UNKNOWN_RESULT", {"request_id": req_id, "data": fail}))
                    await writer.drain()
                    continue

                t0 = time.perf_counter()
                try:
                    res_cmd, res_data = await self._dispatch(cmd, data, obj, peer)
                except Exception as e:      # 로봇 서버 자체 오류(정상이면 오지 않음) — v1 server except 경로 모양
                    self.log("NET", f"Handler error for {cmd}: {e}")
                    res_cmd = f"{cmd}_RESULT"
                    res_data = self._notify("E999", str(e).strip() or "명시적으로 분류되지 않은 내부 예외가 발생했습니다.",
                                            {"peer": str(peer), "request_id": req_id, "cmd": cmd})
                    res_data["cause_type"] = type(e).__name__
                dt_ms = int((time.perf_counter() - t0) * 1000)

                pending_row = None
                meta = {"peer": str(peer), "request_id": req_id, "cmd": cmd}
                try:
                    if not isinstance(res_data, dict):
                        res_data = self._notify(
                            "E111", f"Handler returned non-dict: {type(res_data).__name__} raw={repr(res_data)}", meta)
                    if res_data.get("result") == "fail" and "error_code" not in res_data:
                        self.log("NET", f"[WARN] fail without error_code: cmd={cmd} id={req_id} res={res_data!r}")
                        res_data = self._notify("E110", f"Fail payload missing error_code: {res_data!r}", meta)
                    pending_row = {
                        "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "peer": str(peer),
                        "request_id": req_id, "req_command": cmd, "req_data_json": _safe_json(data),
                        "res_command": res_cmd, "res_result": str(res_data.get("result", "")),
                        "res_message": str(res_data.get("message", "")), "res_data_json": _safe_json(res_data),
                        "duration_ms": dt_ms,
                    }
                except Exception as log_e:
                    try:
                        pending_row = {
                            "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "peer": str(peer),
                            "request_id": req_id, "req_command": cmd or "__UNKNOWN__", "req_data_json": _safe_json(data),
                            "res_command": "__LOGGING_EXCEPTION__", "res_result": "fail",
                            "res_message": _cut(f"Failed to build/append csv row: {log_e!r}", 500),
                            "res_data_json": _safe_json({"res_cmd": res_cmd, "res_data_repr": repr(res_data)}),
                            "duration_ms": dt_ms,
                        }
                    except Exception:
                        pending_row = None

                try:
                    packet = self._pack(res_cmd, {"request_id": req_id, "data": res_data})
                except Exception as pe:
                    fail = self._notify("E107", f"Response serialization failed: {type(pe).__name__}: {pe}",
                                        {**meta, "res_cmd": res_cmd})
                    packet = self._pack(res_cmd, {"request_id": req_id, "data": fail})

                writer.write(packet)
                await writer.drain()

                if pending_row is not None:
                    await self._csv(pending_row)

                try:
                    self.log("PLC_HOST", f"[RES] peer={peer} id={req_id} cmd={res_cmd} data={res_data}")
                except Exception as e:
                    with contextlib.suppress(Exception):
                        self.log("NET", f"[LOG_ERROR] failed to log RES (peer={peer}, cmd={res_cmd}, id={req_id}): {e}")
        except Exception as e:
            msg = str(e) if e else ""
            normal = isinstance(e, (ConnectionError, ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError)) \
                or (isinstance(e, OSError) and "지정된 네트워크 이름을 더 이상 사용할 수 없습니다" in msg)
            if normal:
                self.log("NET", f"Client disconnected: {peer}")
            else:
                self.log("NET", f"Client error from {peer}: {e!r}")
        finally:
            self._writers.discard(writer)
            with contextlib.suppress(Exception):
                writer.close()
                await writer.wait_closed()
            self.log("NET", f"Client closed: {peer}")

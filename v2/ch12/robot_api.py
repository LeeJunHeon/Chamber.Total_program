"""로봇 서버가 부르는 내부 명령 API(제어 프로그램 쪽) — 127.0.0.1:ROBOT_API_PORT(기본 50071), 표준 라이브러리 HTTP/1.1.

계약(v2/robot_server/forward.py 독스트링 그대로)
- POST /robot/v1/command  본문 {"id", "cmd"(정규화된 명령), "data", "raw"(요청 JSON 전체), "peer"("ip:port"), "received_at"(ISO)}
  → 200 {"reply_cmd", "body", "log": [[태그, 본문], ...]}. 핸들러 예외의 원문 명령 + '_RESULT'·E999·'Handler error for ..' 줄도
  여기(robot_ops.handle)가 만든다. 본문이 JSON 객체가 아니면 400(로봇 서버는 '제어 프로그램 응답 없음'과 같게 다룬다).
- POST /robot/v1/event    본문 {"kind", "code", "detail", "level"?} — 로봇 서버 쪽 실패 보고(E102·E104·E105·E107·E110·E111 —
  결정 D09). emit(source='robot_server', kind, detail, level, code) 로 넘긴다(→ 기록·포털 카드). → 200 {"ok": true}.
- GET /robot/v1/health    → 200 {"ok": true, "inflight": n} (진단용).
- 127.0.0.1 에만 묶는다(밖에서 못 부른다).

명령 처리 한 곳(사용자 지시 3장 (4)): 모든 로봇 명령은 dispatch_robot(...) 하나를 지난다. 지금은 robot_ops.handle 을 바로
부르는 얇은 함수이고, core 단계에서 core.command(kind=f"ROBOT_{cmd}", args=data, source=Source.ROBOT) 로 바꿀 자리다(표시: CORE).

같은 명령 id 두 번(로봇 서버 재전송·재시작 뒤 중복): 처리한 id 와 결과를 기억해(최근 DEDUP_KEEP 개) 다시 실행하지 않고 같은 결과
(+ "duplicate": true)를 돌려준다. 처리 중인 id 가 또 오면 끝날 때까지 기다렸다가 같은 결과. to_state()/from_state() 로 5단계 저널이
이 기억을 저장·복원한다(재시작 뒤에도 같은 id 는 다시 실행하지 않는다).
실패 보고(robot_ops on_report — v1 chat_host·팝업 자리): emit(source='robot', kind='host_error', 사용자 문구, level='warn', code).
가상 시계(시험)에서는 요청 스레드를 시계에 등록한다.
"""
from __future__ import annotations

import collections
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Optional, Tuple

from v2.ch12 import errors as ER

Json = Dict[str, Any]
API_HOST = "127.0.0.1"
CMD_PATH = "/robot/v1/command"
EVENT_PATH = "/robot/v1/event"
HEALTH_PATH = "/robot/v1/health"
DEFAULT_PORT = 50071              # params/robot.py ROBOT_API_PORT
DEDUP_KEEP = 1000                 # 기억할 명령 id 수(오래된 것부터 버림)
EmitFn = Callable[..., None]      # emit(source, kind, detail="", level="info", code=None)


def dispatch_robot(ops, cmd: str, data: Any, raw: Any = None, peer: str = "", received_at: Any = None,
                   request_id: Optional[str] = None) -> Tuple[str, Any, List[List[str]]]:
    """로봇 명령 처리 한 곳. CORE: core 단계에서 core.command(kind=f"ROBOT_{cmd}", args=data, source=Source.ROBOT, …) 로 바꾼다
    — 그때도 결과 모양(reply_cmd, body, log)은 이대로."""
    return ops.handle(cmd, data, raw, peer, received_at, request_id)


def report_to_emit(emit: EmitFn) -> Callable[[Json, str], None]:
    """robot_ops on_report → 내부 이벤트(포털이 v1 chat_host 카드와 같은 계기로 보낸다 — D09)."""
    def on_report(payload: Json, src: str) -> None:
        emit("robot", "host_error", ER.format_user_text(payload), level="warn",
             code=str(payload.get("error_code") or "") or None)
    return on_report


class RobotApi:
    def __init__(self, ops, clock=None, cfg: Optional[Dict[str, Any]] = None, emit: Optional[EmitFn] = None,
                 port: Optional[int] = None, dispatch: Callable[..., Tuple[str, Any, list]] = dispatch_robot,
                 keep: int = DEDUP_KEEP) -> None:
        self.ops, self.clock, self.emit = ops, clock, emit
        self.cfg = cfg or {}
        self.dispatch = dispatch
        self.keep = int(keep)
        self._lk = threading.Lock()
        self._done: "collections.OrderedDict[str, Json]" = collections.OrderedDict()
        self._inflight: Dict[str, threading.Event] = {}
        self.events: List[Json] = []                # 받은 로봇 서버 이벤트(시험·진단)
        self.executed = 0                           # 실제로 실행한 명령 수(중복 제외)
        if emit is not None and getattr(ops, "on_report", None) is None:
            ops.on_report = report_to_emit(emit)
        p = int(self.cfg.get("ROBOT_API_PORT", DEFAULT_PORT)) if port is None else int(port)
        api = self

        class _H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a: Any) -> None:
                pass

            def do_GET(self) -> None:
                if self.path != HEALTH_PATH:
                    return self._send(404, {"error": "not found"})
                with api._lk:
                    n = len(api._inflight)
                self._send(200, {"ok": True, "inflight": n})

            def do_POST(self) -> None:
                n = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(n) if n > 0 else b""
                try:
                    obj = json.loads(raw.decode("utf-8"))
                except Exception as e:
                    return self._send(400, {"error": f"invalid json: {e!r}"})
                if not isinstance(obj, dict):
                    return self._send(400, {"error": "body must be an object"})
                try:
                    if self.path == CMD_PATH:
                        code, out = api._command(obj)
                    elif self.path == EVENT_PATH:
                        code, out = api._event(obj)
                    else:
                        code, out = 404, {"error": "not found"}
                except Exception as e:          # 처리 경계 밖 오류(정상이면 오지 않음) — 로봇 서버는 '응답 없음'으로 다룬다
                    code, out = 500, {"error": repr(e)}
                self._send(code, out)

            def _send(self, code: int, obj: Any) -> None:
                body = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
                try:
                    self.send_response(code)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.wfile.write(body)
                except Exception:
                    pass

        self._httpd = ThreadingHTTPServer((API_HOST, p), _H)
        self._httpd.daemon_threads = True
        self.port = int(self._httpd.server_address[1])
        self._thread: Optional[threading.Thread] = None

    # ───────── 명령 ─────────
    def _command(self, obj: Json) -> Tuple[int, Json]:
        cmd = obj.get("cmd")
        if not isinstance(cmd, str):
            return 400, {"error": "cmd must be a string"}
        rid = obj.get("id")
        rid = str(rid) if rid not in (None, "") else ""
        if rid:
            while True:
                with self._lk:
                    if rid in self._done:
                        return 200, dict(self._done[rid], duplicate=True)
                    ev = self._inflight.get(rid)
                    if ev is None:
                        self._inflight[rid] = threading.Event()
                        break
                ev.wait()
        try:
            out = self._run(cmd, obj)
        finally:
            if rid:
                with self._lk:
                    ev = self._inflight.pop(rid, None)
                if ev is not None:
                    ev.set()
        return 200, out

    def _run(self, cmd: str, obj: Json) -> Json:
        clk = self.clock
        reg = bool(getattr(clk, "virtual", False))
        if reg:
            clk.register()
        try:
            data = obj.get("data")
            reply_cmd, body, log = self.dispatch(self.ops, cmd, data if isinstance(data, dict) else {}, obj.get("raw"),
                                                 str(obj.get("peer") or ""), obj.get("received_at"))
        finally:
            if reg:
                clk.unregister()
        out = {"reply_cmd": reply_cmd, "body": body, "log": [list(x) for x in log]}
        rid = obj.get("id")
        with self._lk:
            self.executed += 1
            if rid not in (None, ""):
                self._done[str(rid)] = out
                while len(self._done) > self.keep:
                    self._done.popitem(last=False)
        return out

    # ───────── 로봇 서버 이벤트(D09) ─────────
    def _event(self, obj: Json) -> Tuple[int, Json]:
        kind = str(obj.get("kind") or "robot_server_error")
        code = obj.get("code")
        detail = str(obj.get("detail") or obj.get("text") or "")
        level = str(obj.get("level") or "warn")
        with self._lk:
            self.events.append(dict(obj))
            del self.events[:-200]
        if self.emit is not None:
            try:
                self.emit("robot_server", kind, detail, level=level, code=str(code) if code else None)
            except Exception:
                pass
        return 200, {"ok": True}

    # ───────── 저널(5단계) ─────────
    def to_state(self) -> Json:
        with self._lk:
            return {"version": 1, "done": [[k, v] for k, v in self._done.items()]}

    def from_state(self, state: Optional[Json]) -> int:
        """저장한 기억을 되살린다(지금 기억 뒤에 붙임). 되살린 수."""
        if not isinstance(state, dict):
            return 0
        n = 0
        with self._lk:
            for item in state.get("done") or []:
                if isinstance(item, (list, tuple)) and len(item) == 2 and isinstance(item[1], dict):
                    self._done[str(item[0])] = item[1]
                    n += 1
            while len(self._done) > self.keep:
                self._done.popitem(last=False)
        return n

    def seen(self, rid: str) -> bool:
        with self._lk:
            return str(rid) in self._done

    # ───────── 시작·종료 ─────────
    def start(self) -> "RobotApi":
        self._thread = threading.Thread(target=self._httpd.serve_forever, kwargs={"poll_interval": 0.05},
                                        name="RobotApi", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._thread is not None:
            self._httpd.shutdown()
            self._thread.join(timeout=5.0)
            self._thread = None
        self._httpd.server_close()

    def __enter__(self) -> "RobotApi":
        return self.start()

    def __exit__(self, *a: Any) -> None:
        self.stop()

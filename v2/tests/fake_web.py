"""가짜 웹(시험용, 127.0.0.1 만 — 챔버K v2/tests/fake_web.py 를 옮김, 경로만 /dev/ch12) — uplink 계약(v2.common.messages)의 웹 쪽을 흉내 내고 실패를 조절한다.

- POST /ingest: 토큰 확인 → 표준 JSON 만(NaN·Infinity 는 웹 JSON.parse 처럼 400) → envelope 을 id 로 한 번만 저장 → {acked, commands?, needWebSchema?}
- GET /stream: SSE — push() 한 명령을 event: command 로, keepalive_s 마다 ": ka"
- 조절: status(다음 응답 코드 — 함수 또는 값), delay_s(응답 지연), max_messages(넘으면 413), need_schema(한 번), post_commands,
  down()/up()(같은 포트로 내렸다 올림 — 열린 SSE 도 끊음)
"""
from __future__ import annotations

import collections
import json
import queue
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import parse_qs, urlsplit


class _Srv(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = sys.platform != "win32"


class FakeWeb:
    def __init__(self, token: str = "tok-secret-123", keepalive_s: float = 15.0, port: int = 0) -> None:
        self.token = token
        self.keepalive_s = keepalive_s
        self.port = port
        self.lock = threading.Lock()
        self.stored: "collections.OrderedDict[str, dict]" = collections.OrderedDict()
        self.dups = 0
        self.posts: List[dict] = []                    # {t, n, types, code}
        self.states = 0
        self.status: Union[None, int, Callable[[dict], Optional[int]]] = None
        self.sse_status: Union[None, int] = None
        self.delay_s = 0.0
        self.max_messages: Optional[int] = None
        self.need_schema = False
        self.post_commands: List[dict] = []
        self.sse_clients: List["queue.Queue"] = []
        self.sse_connects = 0
        self.sse_queries: List[dict] = []
        self.auth_seen: List[str] = []
        self.httpd: Optional[_Srv] = None
        self._socks: List[socket.socket] = []
        self.alive = False

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/dev/ch12"

    # ── 생명주기 ──
    def up(self) -> "FakeWeb":
        self.httpd = _Srv(("127.0.0.1", self.port), _handler(self))
        self.port = self.httpd.server_address[1]
        self.alive = True
        threading.Thread(target=self.httpd.serve_forever, kwargs={"poll_interval": 0.1}, name="fake-web",
                         daemon=True).start()
        return self

    def down(self) -> None:
        self.alive = False
        if self.httpd is not None:
            self.httpd.shutdown()
            self.httpd.server_close()
            self.httpd = None
        with self.lock:
            socks, self._socks = self._socks, []
            for q in self.sse_clients:
                q.put(None)
        for s in socks:
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def push(self, cmd: dict) -> int:
        """열린 SSE 모두에 명령 — 보낸 연결 수."""
        with self.lock:
            for q in self.sse_clients:
                q.put(cmd)
            return len(self.sse_clients)

    # ── 조회 ──
    def messages(self, type_: Optional[str] = None) -> List[dict]:
        with self.lock:
            return [e for e in self.stored.values() if type_ is None or e["type"] == type_]

    def wait(self, pred: Callable[[], bool], limit_s: float) -> bool:
        t0 = time.monotonic()
        while time.monotonic() - t0 < limit_s:
            if pred():
                return True
            time.sleep(0.05)
        return pred()

    # ── 처리 ──
    def ingest(self, body: dict) -> tuple:
        msgs = body.get("messages") or []
        if self.max_messages is not None and len(msgs) > self.max_messages:
            return 413, {"error": "too_large"}
        code = self.status(body) if callable(self.status) else self.status
        if code:
            return code, {"error": "x"}
        acked = []
        with self.lock:
            for e in msgs:
                if e["type"] == "state":
                    self.states += 1
                    continue
                if e["id"] in self.stored:
                    self.dups += 1
                else:
                    self.stored[e["id"]] = e
                acked.append(e["id"])
            out: Dict[str, Any] = {"acked": acked}
            if self.post_commands:
                out["commands"], self.post_commands = self.post_commands, []
            if self.need_schema:
                out["needWebSchema"], self.need_schema = True, False
        return 200, out


def _no_nan(name: str):
    raise ValueError(f"표준 JSON 아님: {name}")


def _handler(web: FakeWeb):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass

        def _json(self, code, obj):
            b = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            try:
                self.wfile.write(b)
            except OSError:
                pass                                   # 장비가 시간 초과로 먼저 끊음(지연 시험)

        def _authed(self) -> bool:
            a = self.headers.get("Authorization") or ""
            web.auth_seen.append(a)
            return a == f"Bearer {web.token}"

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(n)
            if web.delay_s:
                time.sleep(web.delay_s)
            if urlsplit(self.path).path != "/dev/ch12/ingest":
                return self._json(404, {})
            if not self._authed():
                web.posts.append({"t": time.monotonic(), "code": 401})
                return self._json(401, {"error": "auth"})
            try:
                body = json.loads(raw, parse_constant=_no_nan)
            except ValueError:
                web.posts.append({"t": time.monotonic(), "code": 400})
                return self._json(400, {"error": "bad_json"})
            code, out = web.ingest(body)
            web.posts.append({"t": time.monotonic(), "code": code, "n": len(body.get("messages") or []),
                              "types": [e["type"] for e in body.get("messages") or []], "bytes": len(raw),
                              "instance": body.get("instance")})
            self._json(code, out)

        def do_GET(self):
            u = urlsplit(self.path)
            if u.path != "/dev/ch12/stream":
                return self._json(404, {})
            if not self._authed():
                return self._json(401, {"error": "auth"})
            if web.sse_status:
                return self._json(web.sse_status, {"error": "x"})
            web.sse_queries.append({k: v[0] for k, v in parse_qs(u.query).items()})
            q: "queue.Queue" = queue.Queue()
            with web.lock:
                web.sse_clients.append(q)
                web._socks.append(self.connection)
                web.sse_connects += 1
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            try:
                while web.alive:
                    try:
                        cmd = q.get(timeout=web.keepalive_s)
                    except queue.Empty:
                        self.wfile.write(b": ka\n\n")
                        self.wfile.flush()
                        continue
                    if cmd is None:
                        break
                    self.wfile.write(b"event: command\ndata: " + json.dumps(cmd).encode() + b"\n\n")
                    self.wfile.flush()
            except OSError:
                pass
            finally:
                with web.lock:
                    if q in web.sse_clients:
                        web.sse_clients.remove(q)
    return H

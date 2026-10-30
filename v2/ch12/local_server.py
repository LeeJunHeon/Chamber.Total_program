"""챔버12 로컬 서버(브라우저 API v1, 사용자 지시 3장 (7)) — 노트북 화면(포털 화면을 local 모드로 빌드해 v2/hmi 에 넣은 것)이 쓰는 곳.
챔버K v2/chk/local_server.py 와 같은 경로·형식(웹 화면이 두 장비에서 같은 코드로 붙게).

127.0.0.1:LOCAL_HMI_PORT 에만 연다(로그인이 없으므로 다른 PC 에서 못 들어오게). ThreadingHTTPServer, 표준 라이브러리만.
같은 포트에 두 번째로 열 수 없게(Windows SO_EXCLUSIVEADDRUSE — 포트 바인드 실패 = 이미 실행 중). 실행 프로그램(app)은 시작
2단계에서 포트를 먼저 잡아 두고(한 번만 실행 — 장비 포트를 열기 전에) 그 소켓을 여기 넘긴다(sock=) — 바인드를 두 번 하지 않는다.

  GET  /api/v1/hello    장비 ID·프로그램·버전·runMode·설정 버전·webSchemaHash·스냅샷 열·이 경로(HMI)에서 받는 명령 목록·챔버
  GET  /api/v1/schema   web_schema.bundle_for(현재값·대기값·저장값·설정 버전)
  GET  /api/v1/stream   text/event-stream — 메시지 하나 = "id: seq / event: type / data: envelope JSON".
                        연결 때 최신 state 하나 + 최근 메시지(state 제외 — Last-Event-ID 가 있으면 그 뒤부터).
                        15 s 마다 keep-alive 주석. 동시 연결 상한 SSE_MAX(넘으면 503).
  POST /api/v1/command  {id?, kind, args} → hub.run_command → system.command(kind, args, source=HMI, cid=id, by="local")
                        → {id, ok, reason, detail(거부 이유 문구, 없으면 "")}. 결과는 cmd_result 메시지로도.
  GET  /api/v1/files?dir=<상대 경로>   레시피 파일 고르기(v1 'Select File'·PC 'CSV 불러오기' 대화상자 자리 — UI-080·PC-087):
                        시작 폴더 PROCESS_LIST_START_DIR 아래만(밖·'..'·절대 경로는 403), 127.0.0.1 에서 온 요청만(403).
                        → {root, dir(시작 폴더 기준 상대 — '' = 시작 폴더), parent, abs, entries[{name, path(상대), full(절대),
                        kind dir|file, size, mtime}]} — 폴더와 *.csv·*.xlsx 만(v1 필터 'Recipe Files (*.csv *.xlsx)'), 이름순(폴더 먼저),
                        FILES_MAX 개까지(truncated). NAS 가 걸리면 FILES_TIMEOUT_S(3 s — v1 시작 폴더 확인 3 s) 뒤 504,
                        시작 폴더가 없으면 404 {error: no_root}. 고른 파일은 화면이 RECIPE_LOAD·PC_RECIPE_LOAD·PRESPUTTER_RECIPE path 로.
  GET  / · 정적 파일    LOCAL_HMI_STATIC_DIR — 비면 이 저장소의 v2/hmi(폴더 밖 경로 404). 폴더가 비면(점 파일만) 안내 페이지
                        (화면 파일은 8단계에서 포털 hmi-build·hmi-copy 로 넣는다).
bench 에서는 쓰기 명령이 BENCH_READ_ONLY — hello 의 runMode·writesAllowed 로 알 수 있다.
"""
from __future__ import annotations

import json
import mimetypes
import os
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, unquote, urlsplit

from v2.common.commands import Source
from v2.common.messages import dumps_json
from v2.ch12.hub import CLOSED

HOST = "127.0.0.1"
SSE_MAX = 8                        # 동시 stream 연결 상한
KEEPALIVE_S = 15.0
BODY_MAX = 1_000_000               # POST 본문 상한(바이트)
LOCAL_BY = "local"                 # 로컬 화면 명령의 요청자
FILE_EXTS = (".csv", ".xlsx")      # v1 'Recipe Files (*.csv *.xlsx)'(chamber_runtime.py:3366)
FILES_TIMEOUT_S = 3.0              # 폴더 읽기 제한(v1 plasma_cleaning_runtime 시작 폴더 확인 3 s — NAS 응답 지연)
FILES_MAX = 2000                   # 한 폴더에서 돌려줄 항목 상한
LOOPBACK = ("127.0.0.1", "::1", "::ffff:127.0.0.1")
EMPTY_PAGE = """<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>챔버12 v2</title></head>
<body style="font-family:sans-serif;margin:2rem">
<h1>로컬 화면 파일 없음 — 웹 단계에서 넣음</h1>
<p><a href="/api/v1/hello">/api/v1/hello</a></p>
</body></html>
"""


def _dumps(obj: Any) -> bytes:
    return dumps_json(obj)


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = sys.platform != "win32"   # Windows SO_REUSEADDR 는 같은 포트 두 번 바인드를 허용한다 — 끔

    def server_bind(self) -> None:
        if sys.platform == "win32" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()


def bind_socket(port: int) -> socket.socket:
    """한 번만 실행 — 127.0.0.1:port 를 잡아 listen 한 소켓(실패 = OSError, 이미 실행 중). LocalServer(sock=) 가 이어받는다."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if sys.platform == "win32" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            s.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        else:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)   # TIME_WAIT 중에도 바로 다시(LISTEN 중이면 여전히 실패)
        s.bind((HOST, int(port)))
        s.listen(16)
    except OSError:
        s.close()
        raise
    return s


class LocalServer:
    """bind() 로 포트를 먼저 잡거나(실패 = 이미 실행 중) 잡아 둔 소켓(sock)을 받고, attach(system) 뒤 start() 로 요청을 받는다."""

    def __init__(self, port: int, static_dir: Optional[str] = None, sse_max: int = SSE_MAX,
                 keepalive_s: float = KEEPALIVE_S, sock: Optional[socket.socket] = None) -> None:
        self.port, self.static_dir = int(port), Path(static_dir) if static_dir else None
        self.sse_max, self.keepalive_s = sse_max, keepalive_s
        self.system = None
        self.httpd: Optional[_Server] = None
        self._thread: Optional[threading.Thread] = None
        self._sse_lock = threading.Lock()
        self.sse_open = 0
        self._subs: set = set()                      # 열린 stream 의 허브 소비자(멈출 때 닫아 SSE 스레드를 바로 끝낸다)
        self.stopping = False
        if sock is not None:
            self.httpd = _Server((HOST, self.port), _handler(self), bind_and_activate=False)
            self.httpd.socket.close()
            self.httpd.socket = sock
            self.httpd.server_address = sock.getsockname()
            self.port = self.httpd.server_address[1]

    def bind(self) -> None:
        """포트 바인드 — 이미 쓰이면 OSError(그대로 올린다)."""
        self.httpd = _Server((HOST, self.port), _handler(self))
        self.port = self.httpd.server_address[1]

    def attach(self, system) -> None:
        self.system = system

    def start(self) -> None:
        assert self.httpd is not None
        self._thread = threading.Thread(target=self.httpd.serve_forever, kwargs={"poll_interval": 0.2},
                                        name="local-server", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self.stopping = True
        with self._sse_lock:
            subs = list(self._subs)
        for sub in subs:
            sub.close("shutdown")
        if self.httpd is None:
            return
        if self._thread is not None:
            self.httpd.shutdown()
            self._thread.join(5.0)
        self.httpd.server_close()

    # ── 처리 ──
    def hello(self) -> dict:
        return self.system.hub.hello_data()

    def schema(self) -> dict:
        from v2.ch12 import web_schema as WS
        s = self.system
        return WS.bundle_for(dict(s.cfg), getattr(s, "store", None), s.hub.hello_data()["configVersion"])

    def command(self, body: dict) -> dict:
        cid = body.get("id")
        kind, args = body.get("kind"), body.get("args") or {}
        if not isinstance(kind, str) or not isinstance(args, dict):
            return {"id": cid, "ok": False, "reason": "BAD_ARGS", "detail": "kind 는 문자열, args 는 객체"}
        r = self.system.hub.run_command(kind, args, Source.HMI, cid=cid, by=LOCAL_BY)
        return {"id": cid, "ok": bool(r.ok), "reason": r.reason, "detail": getattr(r, "detail", "") or ""}

    def files(self, rel: str, timeout_s: float = FILES_TIMEOUT_S) -> tuple:
        """레시피 파일 고르기 — (HTTP 코드, 본문). 시작 폴더(PROCESS_LIST_START_DIR) 아래만."""
        root_s = str((self.system.cfg.get("PROCESS_LIST_START_DIR") if self.system is not None else "") or "")
        rel = str(rel or "").replace("\\", "/").rstrip("/")
        if not root_s:
            return 404, {"error": "no_root", "detail": "PROCESS_LIST_START_DIR 이 비었음"}
        if rel and (rel.startswith("/") or ":" in rel or ".." in rel.split("/")):
            return 403, {"error": "outside", "detail": "시작 폴더 밖"}
        box: dict = {}

        def work() -> None:
            try:
                root = Path(root_s)
                if not root.is_dir():
                    box["r"] = (404, {"error": "no_root", "detail": f"시작 폴더 없음: {root_s}"})
                    return
                base = root.resolve()
                d = (base / rel).resolve() if rel else base
                try:
                    d.relative_to(base)
                except ValueError:
                    box["r"] = (403, {"error": "outside", "detail": "시작 폴더 밖"})
                    return
                if not d.is_dir():
                    box["r"] = (404, {"error": "not_found", "detail": f"폴더 없음: {rel}"})
                    return
                items = []
                with os.scandir(d) as it:
                    for e in it:
                        try:
                            is_dir = e.is_dir()
                            if not is_dir and not e.name.lower().endswith(FILE_EXTS):
                                continue
                            stt = e.stat()
                        except OSError:
                            continue
                        p = (d / e.name)
                        r = p.relative_to(base).as_posix()
                        items.append({"name": e.name, "path": r, "full": str(p), "kind": "dir" if is_dir else "file",
                                      "size": None if is_dir else int(stt.st_size), "mtime": round(stt.st_mtime, 3)})
                items.sort(key=lambda x: (x["kind"] != "dir", x["name"].lower()))
                cur = d.relative_to(base).as_posix()
                cur = "" if cur == "." else cur
                parent = None if not cur else ("/".join(cur.split("/")[:-1]))
                box["r"] = (200, {"root": root_s, "dir": cur, "parent": parent, "abs": str(d),
                                  "entries": items[:FILES_MAX], "truncated": len(items) > FILES_MAX})
            except OSError as e:
                box["r"] = (404, {"error": "unreadable", "detail": f"{type(e).__name__}: {e}"})
        t = threading.Thread(target=work, name="local-files", daemon=True)
        t.start()
        t.join(timeout_s)
        if "r" not in box:
            return 504, {"error": "timeout", "detail": f"시작 폴더 없음/응답지연({timeout_s:g}s)"}
        return box["r"]

    def static_file(self, path: str) -> Optional[Path]:
        """요청 경로 → 폴더 안 파일(폴더 밖·없음 = None)."""
        root = self.static_dir
        if root is None or not root.is_dir():
            return None
        rel = unquote(path).lstrip("/") or "index.html"
        try:
            base = root.resolve()
            f = (base / rel).resolve()
            f.relative_to(base)
        except (ValueError, OSError):
            return None
        if f.is_dir():
            f = f / "index.html"
        return f if f.is_file() else None

    def static_empty(self) -> bool:
        """화면 파일 없음 — 점으로 시작하는 파일(저장소의 v2/hmi/.gitkeep)만 있어도 비었다고 본다."""
        root = self.static_dir
        return root is None or not root.is_dir() or not any(not p.name.startswith(".") for p in root.iterdir())


def _handler(srv: LocalServer):
    class H(BaseHTTPRequestHandler):
        server_version = "ch12-v2"
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):            # 콘솔에 요청 줄을 찍지 않는다(로그 파일은 app 이)
            pass

        def _send(self, code: int, body: bytes, ctype: str = "application/json; charset=utf-8") -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _json(self, code: int, obj: Any) -> None:
            self._send(code, _dumps(obj))

        def do_GET(self):
            path = urlsplit(self.path).path
            if srv.system is None:
                return self._json(503, {"error": "starting"})
            if path == "/api/v1/hello":
                return self._json(200, srv.hello())
            if path == "/api/v1/schema":
                return self._json(200, srv.schema())
            if path == "/api/v1/stream":
                return self._stream()
            if path == "/api/v1/files":
                if str(self.client_address[0]) not in LOOPBACK:
                    return self._json(403, {"error": "loopback_only"})
                q = parse_qs(urlsplit(self.path).query)
                code, body = srv.files((q.get("dir") or [""])[0])
                return self._json(code, body)
            if path.startswith("/api/"):
                return self._json(404, {"error": "not_found"})
            f = srv.static_file(path)
            if f is not None:
                ctype = mimetypes.guess_type(f.name)[0] or "application/octet-stream"
                if ctype.startswith("text/") or ctype in ("application/javascript", "application/json"):
                    ctype += "; charset=utf-8"
                return self._send(200, f.read_bytes(), ctype)
            if path in ("/", "/index.html") and srv.static_empty():
                return self._send(200, EMPTY_PAGE.encode("utf-8"), "text/html; charset=utf-8")
            return self._json(404, {"error": "not_found"})

        def do_POST(self):
            path = urlsplit(self.path).path
            if srv.system is None:
                return self._json(503, {"error": "starting"})
            if path != "/api/v1/command":
                return self._json(404, {"error": "not_found"})
            try:
                n = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                n = -1
            if n < 0 or n > BODY_MAX:
                self.close_connection = True                 # 본문을 읽지 않았다 — 이 연결은 다시 쓰지 않는다
                return self._json(413, {"error": "body_too_large"})
            try:
                body = json.loads(self.rfile.read(n).decode("utf-8") or "{}")
                if not isinstance(body, dict):
                    raise ValueError
            except ValueError:
                return self._json(400, {"id": None, "ok": False, "reason": "BAD_ARGS", "detail": "본문은 JSON 객체"})
            return self._json(200, srv.command(body))

        def _stream(self):
            with srv._sse_lock:
                if srv.sse_open >= srv.sse_max:
                    return self._json(503, {"error": "too_many_streams", "max": srv.sse_max})
                srv.sse_open += 1
            last = self.headers.get("Last-Event-ID")
            try:
                last_seq = int(last) if last not in (None, "") else None
            except ValueError:
                last_seq = None
            sub = srv.system.hub.subscribe(last_seq, name=f"sse:{self.client_address[1]}")
            with srv._sse_lock:
                srv._subs.add(sub)
            try:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True
                while not srv.stopping:
                    env = sub.get(srv.keepalive_s)
                    if env is CLOSED:
                        break
                    if env is None:
                        self.wfile.write(b": keep-alive\n\n")
                    else:
                        self.wfile.write(b"id: %d\nevent: %s\ndata: " % (env["seq"], env["type"].encode()) + _dumps(env) + b"\n\n")
                    self.wfile.flush()
            except (OSError, ValueError):
                pass                                     # 브라우저가 끊음
            finally:
                sub.close("disconnected")
                with srv._sse_lock:
                    srv.sse_open -= 1
                    srv._subs.discard(sub)

    return H

"""가짜 내부 명령 API 서버(시험·sim 용) — 제어 프로그램의 POST /robot/v1/command 자리에 선다(표준 라이브러리만).

FakeApi(respond) — respond(req: dict) -> dict 가 {"reply_cmd", "body", "log"} 를 돌려준다(forward.py 계약).
respond 가 예외를 내면 500. delay_s 만큼 늦게 답할 수 있다(시간 초과 시험). requests 에 받은 요청이 쌓인다.
127.0.0.1 에만 묶는다.
"""
from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Optional

from v2.robot_server.forward import API_PATH

Json = Dict[str, Any]


def default_respond(req: Json) -> Json:
    """기본 응답 — 모든 명령에 '{cmd}_RESULT' + success."""
    return {"reply_cmd": f"{req.get('cmd')}_RESULT", "body": {"result": "success", "message": "OK"}, "log": []}


class FakeApi:
    def __init__(self, respond: Optional[Callable[[Json], Json]] = None, port: int = 0) -> None:
        self.respond = respond or default_respond
        self.delay_s = 0.0
        self.requests: List[Json] = []
        api = self

        class _H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a: Any) -> None:     # 조용히
                pass

            def do_POST(self) -> None:
                n = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(n)
                if self.path != API_PATH:
                    self._send(404, b"{}")
                    return
                try:
                    req = json.loads(raw.decode("utf-8"))
                    api.requests.append(req)
                    if api.delay_s > 0:
                        time.sleep(api.delay_s)
                    out = api.respond(req)
                    self._send(200, json.dumps(out, ensure_ascii=False, default=str).encode("utf-8"))
                except Exception as e:
                    self._send(500, json.dumps({"error": repr(e)}).encode("utf-8"))

            def _send(self, code: int, body: bytes) -> None:
                try:
                    self.send_response(code)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.wfile.write(body)
                except Exception:
                    pass

        self._httpd = ThreadingHTTPServer(("127.0.0.1", port), _H)
        self._httpd.daemon_threads = True
        self.port = int(self._httpd.server_address[1])
        self._thread: Optional[threading.Thread] = None

    def start(self) -> "FakeApi":
        self._thread = threading.Thread(target=self._httpd.serve_forever, kwargs={"poll_interval": 0.05},
                                        name="FakeRobotApi", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        """멈춤(여러 번 불러도 됨 — 돌고 있을 때만 shutdown: 안 돌 때 부르면 영원히 기다린다)."""
        if self._thread is not None:
            self._httpd.shutdown()
            self._thread.join(timeout=5.0)
            self._thread = None
        self._httpd.server_close()

    def __enter__(self) -> "FakeApi":
        return self.start()

    def __exit__(self, *a: Any) -> None:
        self.stop()

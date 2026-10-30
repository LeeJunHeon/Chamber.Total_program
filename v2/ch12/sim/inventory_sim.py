"""재고관리 웹앱 API 가짜 서버(시험·sim 용) — 127.0.0.1 에만 묶는다(실제 https://vanam.synology.me/inventory/api/chamber-slots
대신). 표준 라이브러리 HTTP. GET {PATH} → slots(JSON). 응답 모양은 v1 util/inventory_client.py 가 읽는 것(슬롯 목록 —
locationName 'Chamber N - Gun M', itemName, targetUnitId)이다. 실제 서버의 다른 필드는 미확인(v1 이 읽지 않음).

FakeInventoryServer(slots=None, delay_s=0.0, status=200, body=None)
- slots: 돌려줄 목록(None 이면 SIM_SLOTS). body(bytes)를 주면 그것을 그대로(깨진 응답 시험).
- delay_s: 응답 전 실제 대기(3 초 초과 시험). status: HTTP 상태(500 등 — v1 은 네트워크 실패와 같게 빈 값).
- requests: 받은 경로 목록. url: 'http://127.0.0.1:{port}{PATH}'.
"""
from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, List, Optional

PATH = "/inventory/api/chamber-slots"
SIM_SLOTS: List[dict] = [
    {"locationName": "Chamber 1 - Gun 1", "itemName": "SIM-Ti", "targetUnitId": 101},
    {"locationName": "Chamber 2 - Gun 1", "itemName": "SIM-Al", "targetUnitId": 201},
    {"locationName": "Chamber 2 - Gun 2", "itemName": "SIM-Cu", "targetUnitId": 202},
    {"locationName": "Chamber 2 - Gun 3", "itemName": "SIM-ITO", "targetUnitId": None},
]


class FakeInventoryServer:
    def __init__(self, slots: Optional[List[dict]] = None, delay_s: float = 0.0, status: int = 200,
                 body: Optional[bytes] = None, port: int = 0) -> None:
        self.slots = list(SIM_SLOTS if slots is None else slots)
        self.delay_s, self.status, self.body = float(delay_s), int(status), body
        self.requests: List[str] = []
        srv = self

        class _H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a: Any) -> None:
                pass

            def do_GET(self) -> None:
                srv.requests.append(self.path)
                if srv.delay_s > 0:
                    time.sleep(srv.delay_s)
                if self.path.split("?")[0] != PATH:
                    return self._send(404, b'{"error":"not found"}')
                data = srv.body if srv.body is not None else json.dumps(srv.slots, ensure_ascii=False).encode("utf-8")
                self._send(srv.status, data)

            def _send(self, code: int, data: bytes) -> None:
                try:
                    self.send_response(code)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(data)))
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.wfile.write(data)
                except Exception:
                    pass

        self._httpd = ThreadingHTTPServer(("127.0.0.1", int(port)), _H)
        self._httpd.daemon_threads = True
        self.port = int(self._httpd.server_address[1])
        self.url = f"http://127.0.0.1:{self.port}{PATH}"
        self._thread: Optional[threading.Thread] = None

    def start(self) -> "FakeInventoryServer":
        self._thread = threading.Thread(target=self._httpd.serve_forever, kwargs={"poll_interval": 0.05},
                                        name="FakeInventory", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._thread is not None:
            self._httpd.shutdown()
            self._thread.join(timeout=5.0)
            self._thread = None
        self._httpd.server_close()

    def __enter__(self) -> "FakeInventoryServer":
        return self.start()

    def __exit__(self, *a: Any) -> None:
        self.stop()

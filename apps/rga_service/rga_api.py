# rga_service/rga_api.py
from __future__ import annotations

import json
import time
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9020


class RGAState:
    def __init__(self):
        self._lock = threading.Lock()
        self._by_ch: dict[str, dict] = {}  # ch_id -> status dict

    def _ensure(self, ch_id: str):
        if ch_id not in self._by_ch:
            self._by_ch[ch_id] = {
                "connected": False,
                "scanning": False,
                "last_error": "",
                "updated_ts": 0.0,
                "recipe": ""
            }

    def start_scan(self, ch_id: str, recipe: str):
        with self._lock:
            self._ensure(ch_id)
            st = self._by_ch[ch_id]
            # ---- 여기에 실제 RGA connect + scan start 넣기 ----
            st["connected"] = True
            st["scanning"] = True
            st["recipe"] = recipe
            st["last_error"] = ""
            st["updated_ts"] = time.time()

    def stop_scan(self, ch_id: str):
        with self._lock:
            self._ensure(ch_id)
            st = self._by_ch[ch_id]
            # ---- 여기에 실제 RGA scan stop 넣기 ----
            st["scanning"] = False
            st["updated_ts"] = time.time()

    def get_status(self, ch_id: str) -> dict:
        with self._lock:
            self._ensure(ch_id)
            return dict(self._by_ch[ch_id])

    def get_all(self) -> dict:
        with self._lock:
            return {ch: dict(st) for ch, st in self._by_ch.items()}


STATE = RGAState()


class RGAAPIHandler(BaseHTTPRequestHandler):
    server_version = "RGAService/1.0"

    def _send_json(self, code: int, obj: dict) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        n = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(n) if n > 0 else b"{}"
        return json.loads(raw.decode("utf-8"))

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            return self._send_json(200, {"ok": True})

        if parsed.path == "/status":
            # /status?ch_id=1 같은 쿼리까지 구현하고 싶으면 parse_qs로 확장 가능
            return self._send_json(200, {"ok": True, "states": STATE.get_all()})

        return self._send_json(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/scan/start":
                payload = self._read_json()
                ch_id = str(payload.get("ch_id", "")).strip()
                recipe = str(payload.get("recipe", "")).strip()
                if not ch_id:
                    raise ValueError("ch_id required")
                STATE.start_scan(ch_id, recipe)
                return self._send_json(200, {"ok": True})

            if parsed.path == "/scan/stop":
                payload = self._read_json()
                ch_id = str(payload.get("ch_id", "")).strip()
                if not ch_id:
                    raise ValueError("ch_id required")
                STATE.stop_scan(ch_id)
                return self._send_json(200, {"ok": True})

            return self._send_json(404, {"ok": False, "error": "not found"})
        except Exception as e:
            return self._send_json(400, {"ok": False, "error": str(e)})


def run_rga_api(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
    httpd = ThreadingHTTPServer((host, port), RGAAPIHandler)
    print(f"[RGA_SERVICE] http://{host}:{port}")
    httpd.serve_forever()

# robot_server/state_api.py
from __future__ import annotations

import json
import time
import threading
from dataclasses import dataclass, asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# ==============================
# 설정값(필요하면 config로 분리)
# ==============================
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9010
DEFAULT_TOKEN = "CHANGE_ME"
HEARTBEAT_TIMEOUT_S = 10.0   # 이 시간 이상 heartbeat 없으면 OFFLINE 처리


@dataclass
class ProcState:
    ch_id: str
    run_id: str
    process_name: str = ""
    recipe: str = ""
    state: str = "IDLE"   # IDLE/RUNNING/SUCCESS/FAIL/ABORT/OFFLINE
    step: str = ""
    detail: dict | None = None
    updated_ts: float = 0.0
    last_heartbeat_ts: float = 0.0


class StateStore:
    """
    서버가 기억하는 '현재 공정 상태' 저장소.
    ch_id 별로 마지막 상태를 유지한다.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._by_ch: dict[str, ProcState] = {}

    def upsert_state(self, payload: dict) -> None:
        now = time.time()
        ch_id = str(payload.get("ch_id", "")).strip()
        run_id = str(payload.get("run_id", "")).strip()
        if not ch_id or not run_id:
            raise ValueError("ch_id/run_id required")

        with self._lock:
            st = self._by_ch.get(ch_id)
            if st is None or st.run_id != run_id:
                st = ProcState(ch_id=ch_id, run_id=run_id)
                self._by_ch[ch_id] = st

            st.process_name = str(payload.get("process_name", st.process_name) or "")
            st.recipe = str(payload.get("recipe", st.recipe) or "")
            st.state = str(payload.get("state", st.state) or "")
            st.step = str(payload.get("step", st.step) or "")
            st.detail = payload.get("detail", st.detail)
            st.updated_ts = now
            # 상태 업데이트가 왔다는 건 "살아있음" 신호로 간주 → heartbeat도 갱신
            st.last_heartbeat_ts = now

    def heartbeat(self, payload: dict) -> None:
        now = time.time()
        ch_id = str(payload.get("ch_id", "")).strip()
        run_id = str(payload.get("run_id", "")).strip()
        if not ch_id:
            raise ValueError("ch_id required")

        with self._lock:
            st = self._by_ch.get(ch_id)
            if st is None:
                st = ProcState(ch_id=ch_id, run_id=run_id or "")
                self._by_ch[ch_id] = st
            if run_id and st.run_id != run_id:
                st.run_id = run_id

            st.last_heartbeat_ts = now
            st.updated_ts = now

    def _apply_offline_locked(self) -> None:
        now = time.time()
        for st in self._by_ch.values():
            if st.state == "RUNNING":
                if (now - st.last_heartbeat_ts) > HEARTBEAT_TIMEOUT_S:
                    st.state = "OFFLINE"
                    st.step = "HEARTBEAT_TIMEOUT"
                    st.updated_ts = now

    def get_one(self, ch_id: str) -> dict | None:
        with self._lock:
            self._apply_offline_locked()
            st = self._by_ch.get(ch_id)
            return asdict(st) if st else None

    def get_all(self) -> dict:
        with self._lock:
            self._apply_offline_locked()
            return {ch: asdict(st) for ch, st in self._by_ch.items()}


STORE = StateStore()


class StateAPIHandler(BaseHTTPRequestHandler):
    server_version = "StateAPI/1.0"

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

    def _auth_ok(self, token: str) -> bool:
        got = self.headers.get("X-Api-Token", "")
        return got == token

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/healthz":
            return self._send_json(200, {"ok": True})

        if parsed.path == "/api/state":
            token = getattr(self.server, "api_token", DEFAULT_TOKEN)
            if not self._auth_ok(token):
                return self._send_json(401, {"ok": False, "error": "unauthorized"})

            qs = parse_qs(parsed.query)
            ch_id = (qs.get("ch_id") or [""])[0].strip()
            if ch_id:
                return self._send_json(200, {"ok": True, "state": STORE.get_one(ch_id)})
            return self._send_json(200, {"ok": True, "states": STORE.get_all()})

        return self._send_json(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        token = getattr(self.server, "api_token", DEFAULT_TOKEN)
        if not self._auth_ok(token):
            return self._send_json(401, {"ok": False, "error": "unauthorized"})

        try:
            if parsed.path == "/api/state":
                payload = self._read_json()
                STORE.upsert_state(payload)
                return self._send_json(200, {"ok": True})

            if parsed.path == "/api/heartbeat":
                payload = self._read_json()
                STORE.heartbeat(payload)
                return self._send_json(200, {"ok": True})

            return self._send_json(404, {"ok": False, "error": "not found"})
        except Exception as e:
            return self._send_json(400, {"ok": False, "error": str(e)})


def run_state_api(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT, api_token: str = DEFAULT_TOKEN) -> None:
    httpd = ThreadingHTTPServer((host, port), StateAPIHandler)
    httpd.api_token = api_token
    print(f"[STATE_API] http://{host}:{port}  token={api_token}")
    httpd.serve_forever()

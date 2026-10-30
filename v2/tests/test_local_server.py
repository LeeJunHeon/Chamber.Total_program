"""챔버12 로컬 서버(local_server.py) — 127.0.0.1 실제 HTTP·SSE·명령(조립 시험대 Ch12Rig + 허브, tick 없이 1초 행을 손으로).

- 경로: /api/v1 hello·schema·stream·command(챔버K 와 같은 경로·형식), 그 밖 404, 시작 전 503.
- 명령: system.command(kind, args, source=HMI, cid, by="local") — 결과 {id, ok, reason, detail} + cmd_result 메시지,
  본문 오류 400·413·BAD_ARGS, core 없음 CORE_NOT_READY.
- SSE: 연결 때 hello·state·최근, 이어서 실시간, Last-Event-ID 이어 받기, keep-alive, 동시 연결 상한 503, 멈추면 스트림 끝.
- 정적 파일: 폴더 안만(밖 경로 404), 빈 폴더(점 파일만) 안내 페이지. 한 번만 실행: 같은 포트 두 번째 바인드 실패.
"""
import http.client
import json
import threading
import time

import pytest

from v2.common.clock import VirtualClock
from v2.common.commands import Source
from v2.ch12 import hub as H
from v2.ch12 import local_server as LS
from v2.ch12 import web_schema as WS
from v2.ch12.hub import CmdResult
from v2.ch12.sim.rig import Ch12Rig


@pytest.fixture
def env(tmp_path):
    r = Ch12Rig(VirtualClock(start_wall=1_790_000_000.0), tmp_path)
    H.attach(r)
    calls = []

    def command(kind, args, source=None, cid=None, by=None):
        calls.append((kind, args, source, cid, by))
        if kind == "PLC_BUTTON" and args.get("target") == "nope":
            return CmdResult(False, "BAD_ARGS", "target nope — 없는 버튼")
        return CmdResult(True)
    r.command = command
    r.calls = calls
    static = tmp_path / "hmi"
    static.mkdir()
    sock = LS.bind_socket(0)
    srv = LS.LocalServer(0, str(static), keepalive_s=0.3, sse_max=2, sock=sock)
    srv.attach(r)
    srv.start()
    yield r, srv, static
    srv.stop()
    r.hub.close()
    r.outputs.stop()


def _req(srv, method, path, body=None, headers=None):
    c = http.client.HTTPConnection("127.0.0.1", srv.port, timeout=5)
    try:
        c.request(method, path, body=body, headers=headers or {})
        r = c.getresponse()
        return r.status, r.getheader("Content-Type"), r.read()
    finally:
        c.close()


def _json(srv, method, path, body=None, headers=None):
    code, ctype, raw = _req(srv, method, path, body, headers)
    return code, json.loads(raw)


class Sse:
    """SSE 한 연결 — 별도 스레드가 메시지를 모은다."""

    def __init__(self, srv, last=None):
        self.c = http.client.HTTPConnection("127.0.0.1", srv.port, timeout=5)
        self.c.request("GET", "/api/v1/stream", headers={"Last-Event-ID": str(last)} if last is not None else {})
        self.r = self.c.getresponse()
        self.msgs, self.comments, self.ended = [], 0, threading.Event()
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        ev = {}
        try:
            while True:
                line = self.r.fp.readline()
                if not line:
                    break
                line = line.decode().rstrip("\n")
                if line.startswith(":"):
                    self.comments += 1
                elif line == "":
                    if ev:
                        self.msgs.append(ev)
                    ev = {}
                else:
                    k, v = line.split(": ", 1)
                    ev[k] = v
        except OSError:
            pass
        finally:
            self.ended.set()

    def wait(self, pred, limit=5.0):
        t0 = time.monotonic()
        while time.monotonic() - t0 < limit:
            if pred():
                return True
            time.sleep(0.02)
        return pred()

    def types(self):
        return [m["event"] for m in self.msgs]

    def close(self):
        self.c.close()


def test_hello_schema_and_404(env):
    r, srv, _ = env
    code, h = _json(srv, "GET", "/api/v1/hello")
    assert code == 200 and h["equipment"] == "CH12" and h["runMode"] == "sim" and h["instance"] == r.hub.instance
    assert h["snapshotColumns"] == list(r.bus.columns) and h["webSchemaHash"] == WS.content_hash()
    code, b = _json(srv, "GET", "/api/v1/schema")
    assert code == 200 and b["hash"] == h["webSchemaHash"] and b["version"] == WS.EXPORT_VERSION
    by = {p["key"]: p for p in b["params"]}
    assert by["EQUIPMENT_ID"]["current"] == "CH12" and by["UPLINK_TOKEN"]["current"] is None
    assert _json(srv, "GET", "/api/v1/nope")[0] == 404 and _json(srv, "POST", "/api/v1/hello", b"{}")[0] == 404
    code, ctype, _ = _req(srv, "GET", "/api/v1/hello")
    assert ctype == "application/json; charset=utf-8"


def test_command_round_trip_and_errors(env):
    r, srv, _ = env
    code, out = _json(srv, "POST", "/api/v1/command", json.dumps({"id": "h1", "kind": "PROCESS_STOP", "args": {"ch": 1}}))
    assert (code, out) == (200, {"id": "h1", "ok": True, "reason": None, "detail": ""})
    assert r.calls[-1] == ("PROCESS_STOP", {"ch": 1}, Source.HMI, "h1", "local")
    code, out = _json(srv, "POST", "/api/v1/command", json.dumps({"kind": "PLC_BUTTON", "args": {"target": "nope"}}))
    assert out == {"id": None, "ok": False, "reason": "BAD_ARGS", "detail": "target nope — 없는 버튼"}
    res = [e["data"] for e in r.hub.recent if e["type"] == "cmd_result"]
    assert [(d["cmdId"], d["status"], d["source"], d["by"]) for d in res] == [("h1", "accepted", "hmi", "local"),
                                                                              (None, "rejected", "hmi", "local")]
    n = len(r.calls)
    assert _json(srv, "POST", "/api/v1/command", b"[1,2]")[1]["reason"] == "BAD_ARGS"            # 객체 아님 → 400
    assert _json(srv, "POST", "/api/v1/command", b"{bad")[0] == 400
    assert _json(srv, "POST", "/api/v1/command", json.dumps({"kind": 3}))[1]["reason"] == "BAD_ARGS"
    assert _json(srv, "POST", "/api/v1/command", json.dumps({"kind": "X", "args": [1]}))[1]["reason"] == "BAD_ARGS"
    code, _, _ = _req(srv, "POST", "/api/v1/command", b"{}", {"Content-Length": str(LS.BODY_MAX + 1)})
    assert code == 413 and len(r.calls) == n                                             # 실행하지 않음
    del r.command                                                                        # core 없음(4단계 전)
    assert _json(srv, "POST", "/api/v1/command", json.dumps({"kind": "PROCESS_STOP"}))[1]["reason"] == "CORE_NOT_READY"


def test_stream_backlog_live_resume_keepalive_and_limit(env):
    r, srv, _ = env
    r.hub.hello()
    r.hub.on_row({"seq": 1, "ts": "x"})
    r.emit("ch1.mfc", "flow", "첫")
    a = Sse(srv)
    assert a.r.status == 200 and a.r.getheader("Content-Type") == "text/event-stream; charset=utf-8"
    assert a.wait(lambda: len(a.msgs) >= 3) and a.types()[:3] == ["hello", "state", "event"]
    env0 = json.loads(a.msgs[2]["data"])
    assert int(a.msgs[2]["id"]) == env0["seq"] and env0["data"]["context"]["chamber"] == "ch1"
    r.emit("process:ch2", "run_start", "시작", event={"run_id": "r1", "params": {}})
    _json(srv, "POST", "/api/v1/command", json.dumps({"id": "s1", "kind": "PROCESS_STOP"}))
    assert a.wait(lambda: "cmd_result" in a.types() and "run_start" in a.types())
    assert a.wait(lambda: a.comments >= 1, 3)                                            # keep-alive(0.3 s)
    b = Sse(srv, last=env0["seq"])                                                       # 그 뒤부터(hello 없이)
    assert b.wait(lambda: len(b.msgs) >= 3) and b.types()[:3] == ["state", "run_start", "cmd_result"]
    code, out = _json(srv, "GET", "/api/v1/stream")                                     # 상한 2 → 503
    assert code == 503 and out["error"] == "too_many_streams"
    srv.stop()                                                                           # 멈추면 스트림이 끝난다
    assert a.ended.wait(3) and b.ended.wait(3)
    a.close()


def test_static_files_and_empty_page(env):
    r, srv, static = env
    code, ctype, raw = _req(srv, "GET", "/")
    assert code == 200 and "로컬 화면 파일 없음" in raw.decode() and ctype.startswith("text/html")
    (static / ".gitkeep").write_text("")
    assert "로컬 화면 파일 없음" in _req(srv, "GET", "/index.html")[2].decode()
    (static / "index.html").write_text("<h1>CH12</h1>", encoding="utf-8")
    (static / "app").mkdir()
    (static / "app" / "x.js").write_text("let a=1", encoding="utf-8")
    (static.parent / "secret.txt").write_text("no", encoding="utf-8")
    assert _req(srv, "GET", "/")[2] == b"<h1>CH12</h1>"
    code, ctype, raw = _req(srv, "GET", "/app/x.js")
    assert code == 200 and raw == b"let a=1" and "charset=utf-8" in ctype
    assert _req(srv, "GET", "/app/")[0] == 404                                            # 폴더에 index 없음
    for bad in ("/../secret.txt", "/%2e%2e/secret.txt", "/app/../../secret.txt", "/nope.png"):
        assert _req(srv, "GET", bad)[0] == 404, bad


def test_not_attached_503_and_single_instance_port(tmp_path):
    s = LS.LocalServer(0, None)
    s.bind()
    s.start()
    try:
        assert _json(s, "GET", "/api/v1/hello")[0] == 503 and _json(s, "POST", "/api/v1/command", b"{}")[0] == 503
        with pytest.raises(OSError):
            LS.bind_socket(s.port)                                                         # 이미 실행 중
        with pytest.raises(OSError):
            LS.LocalServer(s.port, None).bind()
    finally:
        s.stop()
    sock = LS.bind_socket(0)                                                               # 잡아 둔 소켓을 이어받음
    srv = LS.LocalServer(0, None, sock=sock)
    assert srv.port == sock.getsockname()[1] and srv.httpd.socket is sock
    srv.stop()
    assert sock.fileno() == -1                                                             # 서버가 닫는다

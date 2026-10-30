"""챔버12 실행 프로그램의 웹 연결 — 실제 하위 프로세스(python -m v2.ch12, sim + UPLINK_IN_SIM + CH12-SIM) + 가짜 웹(127.0.0.1).

- 웹: hello 맨 앞(runMode sim·equipment CH12-SIM) → state 가 POST 마다 → SSE 명령 → cmd_result 가 웹에(by = requestedBy).
  명령 처리(core, 4단계)가 아직 없으면 rejected CORE_NOT_READY — 왕복(웹 → 장비 → 웹)은 그대로 확인한다.
- 로컬 서버: 같은 프로세스의 127.0.0.1:LOCAL_HMI_PORT — /api/v1/hello·schema·stream·command, 빈 화면 폴더면 안내 페이지.
- 비밀값: 토큰·주소 경로가 콘솔·로그·웹 메시지·보관함 어디에도 없다(주소는 호스트만).
- 같은 프로세스(App): sim 기본은 웹 연결 없음, UPLINK_IN_SIM 인데 -SIM 이 아니면 종료 코드 2, 주소가 비면 경고만.
"""
import http.client
import json
import signal
import socket
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from v2.ch12 import app as A
from v2.ch12.sim.rig import local_paths
from v2.tests.fake_web import FakeWeb

ROOT = Path(__file__).resolve().parents[2]
posix_only = pytest.mark.skipif(sys.platform == "win32", reason="POSIX 신호로 끝냄 — 윈도우는 콘솔 이벤트(윈도우 확인 필요)")


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def config(tmp: Path, web: FakeWeb, **extra) -> Path:
    d = tmp / "config"
    d.mkdir(parents=True, exist_ok=True)
    vals = dict(local_paths(tmp / "root"), LOCAL_HMI_PORT=free_port(), LOCAL_HMI_STATIC_DIR=str(tmp / "hmi"),
                UPLINK_IN_SIM=True, EQUIPMENT_ID="CH12-SIM", UPLINK_URL=web.url, UPLINK_TOKEN=web.token)
    vals.update(extra)
    (d / "local_override.json").write_text(json.dumps({"values": vals}), encoding="utf-8")
    return d


def _get(port, path):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        c.request("GET", path)
        r = c.getresponse()
        return r.status, r.read()
    finally:
        c.close()


def _post(port, path, obj):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        c.request("POST", path, body=json.dumps(obj), headers={"Content-Type": "application/json"})
        r = c.getresponse()
        return r.status, json.loads(r.read())
    finally:
        c.close()


def _cmd(cid, kind, by="op@web", args=None):
    import datetime as dt
    now = dt.datetime.now(dt.timezone.utc)
    iso = lambda d: d.isoformat().replace("+00:00", "Z")
    return {"id": cid, "kind": kind, "args": args or {}, "requestedBy": by, "requestedAt": iso(now),
            "expiresAt": iso(now + dt.timedelta(seconds=60))}


@posix_only
def test_process_sim_uplink_hello_state_command_round_trip(tmp_path):
    web = FakeWeb(keepalive_s=1.0).up()
    cfg = config(tmp_path, web)
    port = json.loads((cfg / "local_override.json").read_text(encoding="utf-8"))["values"]["LOCAL_HMI_PORT"]
    p = subprocess.Popen([sys.executable, "-m", "v2.ch12", "--config-dir", str(cfg)], cwd=str(ROOT),
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8")
    lines, running = [], threading.Event()

    def read():
        for line in p.stdout:
            lines.append(line.rstrip("\n"))
            if line.startswith(A.RUNNING_LINE):
                running.set()
    t = threading.Thread(target=read, daemon=True)
    t.start()
    try:
        assert running.wait(60), "\n".join(lines)
        # 웹: hello 맨 앞 → state
        assert web.wait(lambda: web.states >= 2 and web.sse_connects >= 1, 20), "\n".join(lines)
        first = next(x for x in web.posts if x["code"] == 200)
        assert first["types"][0] == "hello"
        h = web.messages("hello")[0]["data"]
        assert (h["equipment"], h["runMode"], h["program"]) == ("CH12-SIM", "sim", "ch12-v2")
        assert h["instance"] == first["instance"] and len(h["snapshotColumns"]) > 500
        assert web.sse_queries[0] == {"equipment": "CH12-SIM", "instance": h["instance"]}
        evs = web.messages("event")
        assert any(e["data"]["context"]["source"] == "plc" and e["data"]["context"]["chamber"] == "shared" for e in evs)
        assert any(e["data"]["context"]["chamber"] == "ch1" for e in evs)
        # 웹 → 장비 → 웹(SSE 명령 → cmd_result)
        assert web.push(_cmd("w-1", "PROCESS_STOP", by="op@web"))
        assert web.wait(lambda: [m for m in web.messages("cmd_result") if m["data"]["cmdId"] == "w-1"], 10)
        r = [m["data"] for m in web.messages("cmd_result") if m["data"]["cmdId"] == "w-1"][0]
        assert (r["kind"], r["source"], r["by"]) == ("PROCESS_STOP", "web", "op@web") and r["status"] in ("accepted", "rejected")
        # 로컬 서버(같은 프로세스)
        code, raw = _get(port, "/api/v1/hello")
        lh = json.loads(raw)
        assert code == 200 and lh["instance"] == h["instance"] and lh["webSchemaHash"] == h["webSchemaHash"]
        code, raw = _get(port, "/api/v1/schema")
        assert code == 200 and json.loads(raw)["hash"] == h["webSchemaHash"]
        assert b"\xeb\xa1\x9c\xec\xbb\xac" in _get(port, "/")[1]                     # "로컬" — 빈 화면 폴더 안내 페이지
        code, out = _post(port, "/api/v1/command", {"id": "l-1", "kind": "PROCESS_STOP"})
        assert code == 200 and out["id"] == "l-1" and set(out) == {"id", "ok", "reason", "detail"}
        assert web.wait(lambda: [m for m in web.messages("cmd_result") if m["data"]["cmdId"] == "l-1"], 10)
        lr = [m["data"] for m in web.messages("cmd_result") if m["data"]["cmdId"] == "l-1"][0]
        assert (lr["source"], lr["by"]) == ("hmi", "local")
        st = web.posts[-1]
        assert st["code"] == 200
    finally:
        p.send_signal(signal.SIGINT)
        try:
            code = p.wait(30)
        except subprocess.TimeoutExpired:
            p.kill()
            code = p.wait(10)
        t.join(5)
        web.down()
    assert code == A.EXIT_OK, "\n".join(lines)
    out = "\n".join(lines)
    assert "웹 연결 127.0.0.1(http)" in out and "https 가 아님" in out
    log = (tmp_path / "root" / "data" / "logs" / "ch12_v2.log").read_text(encoding="utf-8")
    msgs = json.dumps(web.messages(), ensure_ascii=False)
    spool = "".join(f.read_text(encoding="utf-8") for f in (tmp_path / "root" / "data" / "uplink").glob("*"))
    for text in (out, log, msgs, spool):
        assert web.token not in text and "/dev/ch12" not in text


def _in_process(tmp_path, web, **extra):
    lines = []
    app = A.App(str(config(tmp_path, web, **extra)), print_fn=lines.append)
    return app, lines


def test_sim_default_no_uplink(tmp_path):
    web = FakeWeb()
    app, lines = _in_process(tmp_path, web, UPLINK_IN_SIM=False, EQUIPMENT_ID="CH12")
    assert app.start() == A.EXIT_OK
    try:
        assert app.uplink is None and "웹 연결 끔(sim)" in lines
        assert app.system.hub.state_data({})["uplink"] == {"enabled": False}
        assert app.server is not None and app.instance_sock is None                     # 서버가 소켓을 이어받음
        code, raw = _get(app.server.port, "/api/v1/hello")
        assert code == 200 and json.loads(raw)["equipment"] == "CH12"
        assert app.system.config is not None and app.system.hub.last_hello is not None
    finally:
        app.shutdown()
    assert app.system.hub.subs == []


def test_uplink_in_sim_requires_sim_id_and_empty_url_warns(tmp_path):
    web = FakeWeb()
    bad, out = _in_process(tmp_path / "a", web, EQUIPMENT_ID="CH12")
    assert bad.start() == A.EXIT_CONFIG and bad.system is None and bad.instance_sock is None
    assert any("-SIM 으로 끝나지 않음" in s for s in out)
    app, lines = _in_process(tmp_path / "b", web, UPLINK_URL="")
    assert app.start() == A.EXIT_OK
    try:
        assert app.uplink is None and "웹 연결 주소 없음 — 연결 안 함" in lines
        assert any("UPLINK_URL 이 비어 있음" in s for s in lines)
    finally:
        app.shutdown()

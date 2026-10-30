"""챔버12 웹 상시 연결(uplink.py·uplink_spool.py) — 127.0.0.1 의 가짜 웹(fake_web)으로, 실패를 조절하며(챔버K test_uplink 를 옮김).

- 단위: 다시 시도 간격, 보관함(조각 지움·다시 읽기·다시 쓰기·상한), snapshot_summary(챔버별 run id).
- 시험대(Ch12Rig, tick 없이) + 허브 + 설정 적용 + 가짜 명령 처리(core 계약 — system.command(kind, args, source, cid, by)) + Uplink:
  hello 맨 앞·state·메시지 빠짐없이 순서대로·acked 뒤 보관함 비움, SSE 명령·같은 id 두 번(SSE·POST 응답)·만료·시계 차·
  CONFIG_APPLY(config_ack by)·needWebSchema, 401·409·413·500·응답 지연, 보관함 상한·느린 소비자·UPLINK_GAP, bench,
  HIGH 메시지 바로 POST(백오프 중엔 아님), NaN, 웹 끊김 → UPLINK_DOWN → 다시 올라옴(보관함 유지·hello 다시)·다시 켜도 이어 올림,
  명령 id 기억(to_state·from_state — 재시작 뒤 같은 명령 실행 안 함).
"""
import datetime as dt
import json
import socket
import threading
import time

import pytest

from v2.common.clock import VirtualClock
from v2.ch12 import config_apply as CA
from v2.ch12 import hub as H
from v2.ch12 import uplink as U
from v2.ch12.hub import CmdResult
from v2.ch12.params import PARAMS
from v2.ch12.sim.rig import Ch12Rig
from v2.ch12.system import param_schema_hash
from v2.tests.fake_web import FakeWeb

SECRETS = {p.key for p in PARAMS if p.scope.value == "secret"}
BENCH_OK = {"RECIPE_LOAD", "CONFIG_APPLY", "CONFIG_ROLLBACK"}


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _iso(sec_from_now: float) -> str:
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=sec_from_now)).isoformat().replace("+00:00", "Z")


def _cmd(cid, kind, args=None, by="op@test", exp=60.0):
    return {"id": cid, "kind": kind, "args": args or {}, "requestedBy": by, "requestedAt": _iso(0), "expiresAt": _iso(exp)}


def _web_cmd_results(web, cid):
    return [m["data"] for m in web.messages("cmd_result") if m["data"]["cmdId"] == cid]


def _env(i, type_="event", level="info", size=0):
    return {"id": f"m{i:05d}", "instance": "x", "seq": i, "ts": "2026-10-21T00:00:00.000Z", "type": type_,
            "data": {"level": level, "code": "C", "message": "x" * size}}


class Rigs:
    """시험대 정리(v1 기록 출력 스레드·웹 연결 스레드)."""

    def __init__(self):
        self.made = []

    def make(self, tmp_path, web, token=None, mode="sim", **cfg):
        rig = Ch12Rig(VirtualClock(start_wall=time.time()), tmp_path, mode=mode,
                      cfg=dict({"UPLINK_INTERVAL_MS": 500, "UPLINK_TIMEOUT_SEC": 2.0, "UPLINK_DOWN_SEC": 5}, **cfg))
        H.attach(rig)
        CA.attach(rig)
        rig.calls = []

        def command(kind, args, source=None, cid=None, by=None):      # core 계약의 본보기(4단계가 같은 모양으로)
            rig.calls.append((kind, cid, by))
            if kind == "CONFIG_APPLY":
                why = rig.config.apply(args, by)
                return CmdResult(why is None, why)
            if not rig.policy.device_writes and kind not in BENCH_OK:
                return CmdResult(False, "BENCH_READ_ONLY", "bench — 장비 쓰기 금지")
            return CmdResult(True)
        rig.command = command
        up = U.Uplink(rig, web.url, web.token if token is None else token, tmp_path / "data")
        rig.uplink = up
        self.made.append((rig, up))
        return rig, up

    def close(self):
        for rig, up in self.made:
            up.stop()
            rig.hub.close()
            rig.outputs.stop()


@pytest.fixture
def rigs():
    r = Rigs()
    yield r
    r.close()


def _codes(rig, code):
    return [e for e in list(rig.hub.recent) if e["type"] == "event" and e["data"]["code"] == code]


def _info(rig, i):
    rig.emit("test", "t", f"m{i}", code="T")


def _hub_up(hub):
    return [e for e in list(hub.recent) if e["type"] != "hello"]


# ───────────── 단위 ─────────────
def test_retry_delay_policy():
    assert U.retry_delay(401, 1) == 60.0 and U.retry_delay(401, 9) == 60.0
    assert U.retry_delay(409, 3) == 10.0
    mid = lambda: 0.5
    assert [U.retry_delay(500, n, mid) for n in range(1, 8)] == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]
    assert U.retry_delay(None, 1, lambda: 0.0) == pytest.approx(0.8) and U.retry_delay(None, 1, lambda: 1.0) == pytest.approx(1.2)
    assert U.retry_delay(None, 20, lambda: 1.0) == 30.0


def test_spool_ack_deletes_files_and_survives_restart(tmp_path):
    sp = U.Spool(tmp_path / "up", 10_000_000)
    for i in range(1, 2001):
        sp.put(_env(i, size=200))
    assert sp.count == 2000 and sp.files() >= 2
    b = sp.batch(500, 10_000_000)
    assert [k for k, _ in b] == [f"m{i:05d}" for i in range(1, 501)]
    assert sp.batch(10, 1000)[0][0] == "m00001" and len(sp.batch(10, 1000)) <= 4
    sp.ack([k for k, _ in b] + ["없는 id"])
    assert sp.count == 1500
    sp.close()
    sp2 = U.Spool(tmp_path / "up", 10_000_000)
    assert sp2.loaded == 1500 and sp2.batch(1, 10_000_000)[0][0] == "m00501"
    sp2.ack([k for k, _ in sp2.batch(5000, 100_000_000)])
    assert sp2.count == 0 and sp2.files() == 0 and sp2.live_bytes == 0
    sp2.put(_env(9999))
    assert sp2.files() == 1
    sp2.close()


def test_spool_partial_acks_are_compacted(tmp_path):
    sp = U.Spool(tmp_path / "up", 400_000)
    keep = []
    for i in range(1, 20001):
        sp.put(_env(i, level="warn", size=150))
        if i % 300 == 0:
            keep.append(f"m{i:05d}")
        else:
            sp.ack([f"m{i:05d}"])
    assert sp.count == len(keep) and sp.disk_bytes() <= 2 * 400_000 + U.SEG_BYTES + 400
    assert [k for k, _ in sp.batch(10_000, 10_000_000)] == keep
    sp.close()
    sp3 = U.Spool(tmp_path / "up", 400_000)
    assert sp3.loaded == len(keep)
    sp3.close()


def test_spool_full_drops_low_priority_first(tmp_path):
    sp = U.Spool(tmp_path / "up", 60_000)
    high = []
    for i in range(1, 401):
        t = ("event", "info") if i % 4 else (("run_end", None) if i % 8 else ("event", "warn"))
        if i % 10 == 0:
            t = ("snapshot_summary", None)
        sp.put(_env(i, t[0], t[1] or "info", size=300))
        if U.priority(_env(i, t[0], t[1] or "info")) == U.HIGH:
            high.append(f"m{i:05d}")
    left = [k for k, _ in sp.batch(10_000, 10_000_000)]
    assert sp.dropped > 0 and sp.live_bytes <= 60_000
    assert set(high) <= set(left)
    assert sp.dropped_by_type["event"] + sp.dropped_by_type["snapshot_summary"] == sp.dropped
    for i in range(401, 800):
        sp.put(_env(i, "run_end", size=300))
    assert sp.live_bytes <= 60_000 and sp.batch(1, 10**7)[0][0] != "m00004"
    sp.close()


def test_snapshot_summary_per_chamber_run_ids():
    """공정 중 구간만 — 구간 안 어느 챔버든 run id 가 있으면 내고 runId = 마지막으로 본 것, runIds = 챔버별."""
    s = U.Summary()
    t0 = 1_760_000_040.0
    plan = ([(None, None)] * 60 + [(None, None)] * 20 + [("a1", None)] * 30 + [("a1", "b1")] * 10
            + [(None, None)] * 60 + [(None, None)])
    out = []
    for k, (r1, r2) in enumerate(plan):
        row = {"seq": k, "ch1.mfc.ar_sccm": float(k), "ch1.mfc.q": "ok" if k % 4 else "stale", "ch1.mfc.age_s": 0.1 * (k % 5),
               "ch1.proc.run_id": r1, "ch2.proc.run_id": r2, "ch1.proc.state": "RUNNING", "plc.q": "ok", "plc.age_s": 0.0}
        r = s.add(t0 + k, row, 60)
        if r:
            out.append(r)
    assert len(out) == 1
    o = out[0]
    assert (o["runId"], o["runIds"], o["rows"], o["from"]) == ("b1", {"ch1": "a1", "ch2": "b1"}, 60, U.utc_iso(t0 + 60))
    assert o["values"]["ch1.mfc.ar_sccm"]["last"] == 119.0 and "seq" not in o["values"]
    assert "ch1.proc.state" not in o["values"] and o["quality"]["ch1.mfc"]["okRatio"] == 0.75
    assert o["quality"]["plc"] == {"okRatio": 1.0, "maxAge": 0.0}


# ───────────── 시험대 + 가짜 웹 ─────────────
def test_normal_flow_commands_schema_and_secrets(tmp_path, rigs):
    from v2.ch12 import web_schema as WS
    web = FakeWeb(keepalive_s=0.5).up()
    rig, up = rigs.make(tmp_path, web, EQUIPMENT_ID="CH12-SIM", UPLINK_IN_SIM=True)
    hub = rig.hub
    hub.hello()
    stop = threading.Event()

    def rows():                                       # 1초 행 대신(빨리) — state
        k = 0
        while not stop.is_set():
            hub.on_row({"seq": k, "ts": "x", "ch1.proc.run_id": None})
            k += 1
            time.sleep(0.2)
    threading.Thread(target=rows, daemon=True).start()
    up.start()
    try:
        assert web.wait(lambda: web.states >= 2 and web.sse_connects >= 1, 8)
        first = next(p for p in web.posts if p["code"] == 200)
        assert first["types"][0] == "hello" and first["instance"] == hub.instance
        h = web.messages("hello")[0]["data"]
        assert h["runMode"] == "sim" and h["equipment"] == "CH12-SIM" and "webSchema" not in h
        assert abs(U.parse_iso(h["deviceUtc"]) - time.time()) < 5
        assert web.sse_queries[0] == {"equipment": "CH12-SIM", "instance": hub.instance}
        assert all(p["types"].count("state") <= 1 for p in web.posts if p.get("types"))
        # SSE 명령 → cmd_result(by = requestedBy)
        web.push(_cmd("c-stop", "PROCESS_STOP", by="a@test"))
        assert web.wait(lambda: _web_cmd_results(web, "c-stop"), 5)
        r = _web_cmd_results(web, "c-stop")[0]
        assert (r["status"], r["source"], r["by"], r["kind"]) == ("accepted", "web", "a@test", "PROCESS_STOP")
        # 같은 id 를 SSE·POST 응답으로 두 번 → 한 번만 실행, 결과는 두 번 같게
        dup = _cmd("c-dup", "PROCESS_STOP")
        web.push(dup)
        assert web.wait(lambda: _web_cmd_results(web, "c-dup"), 5)
        web.post_commands = [dup]
        assert web.wait(lambda: len(_web_cmd_results(web, "c-dup")) == 2, 5)
        a, b = _web_cmd_results(web, "c-dup")
        assert a == b and rig.calls.count(("PROCESS_STOP", "c-dup", "op@test")) == 1 and up.dup_cmds == 1
        # 만료 → COMMAND_EXPIRED(실행 안 함), 허용 시계 차(5 s) 안은 실행
        web.push(_cmd("c-old", "PROCESS_STOP", exp=-30))
        web.push(_cmd("c-skew", "PROCESS_STOP", exp=-2))
        assert web.wait(lambda: _web_cmd_results(web, "c-old") and _web_cmd_results(web, "c-skew"), 5)
        r = _web_cmd_results(web, "c-old")[0]
        assert (r["status"], r["reasonCode"]) == ("rejected", "COMMAND_EXPIRED") and not [c for c in rig.calls if c[1] == "c-old"]
        assert _web_cmd_results(web, "c-skew")[0]["status"] == "accepted"
        # 형식 오류 → BAD_ARGS, id 없음 → 버림(UPLINK_BAD_COMMAND)
        web.push({"id": "c-bad", "kind": "X", "args": [], "expiresAt": _iso(60)})
        web.push({"kind": "X"})
        assert web.wait(lambda: _web_cmd_results(web, "c-bad") and _codes(rig, "UPLINK_BAD_COMMAND"), 5)
        assert _web_cmd_results(web, "c-bad")[0]["reasonCode"] == "BAD_ARGS"
        # CONFIG_APPLY → config_ack(by = requestedBy), live 키는 바로 같은 사전에
        vals = {k: x for k, x in rig.store.stored().items() if k not in SECRETS}
        vals["UPLINK_CLOCK_SKEW_SEC"] = 7.0
        web.push(_cmd("c-cfg", "CONFIG_APPLY", {"version": 1, "values": vals, "schemaHash": param_schema_hash()},
                      by="admin@test"))
        assert web.wait(lambda: web.messages("config_ack"), 5)
        ack = web.messages("config_ack")[0]["data"]
        assert (ack["status"], ack["by"], ack["appliedKeys"]) == ("applied", "admin@test", ["UPLINK_CLOCK_SKEW_SEC"])
        assert rig.cfg["UPLINK_CLOCK_SKEW_SEC"] == 7.0 and _web_cmd_results(web, "c-cfg")[0]["by"] == "admin@test"
        # needWebSchema → webSchema 를 실은 hello 한 번 더
        web.need_schema = True
        assert web.wait(lambda: any("webSchema" in m["data"] for m in web.messages("hello")), 5)
        ws = [m["data"]["webSchema"] for m in web.messages("hello") if "webSchema" in m["data"]]
        assert len(ws) == 1 and ws[0]["hash"] == WS.content_hash() == h["webSchemaHash"]
        assert ws[0]["configVersion"] == 1 and {p["key"]: p for p in ws[0]["params"]}["UPLINK_TOKEN"]["current"] is None
        # run_start·run_end(챔버별)
        rig.emit("process:ch1", "run_start", "시작", event={"run_id": "r1", "params": {}, "by": "run@test"})
        rig.emit("process:ch1", "run_end", "끝", event={"run_id": "r1", "status": "done"})
        assert web.wait(lambda: web.messages("run_end"), 5)
        assert web.messages("run_start")[0]["data"]["chamber"] == "ch1" and web.messages("run_end")[0]["data"]["result"] == "done"
        # 빠짐없이·순서대로·한 번
        time.sleep(1.0)
        assert web.wait(lambda: up.spool.count == 0, 5)
        want = [e["id"] for e in _hub_up(hub) if e["type"] != "state"]
        got = {e["id"] for e in web.messages()}
        assert want and set(want) <= got
        seqs = [e["seq"] for e in web.messages() if e["type"] != "hello" and e["instance"] == hub.instance]
        assert seqs == sorted(seqs) and web.dups == 0 and up.spool.files() == 0
        st = up.status()
        assert st["enabled"] and st["connected"] and st["lastError"] is None and st["host"] == "127.0.0.1"
        assert hub.last_state["data"]["uplink"]["enabled"]
    finally:
        stop.set()
        up.stop()
        web.down()
    text = json.dumps(web.messages(), ensure_ascii=False)
    for t in (text, "".join(p.read_text(encoding="utf-8") for p in (tmp_path / "data" / "uplink").glob("*"))):
        assert web.token not in t and "/dev/ch12" not in t
    assert web.auth_seen and all(a == f"Bearer {web.token}" for a in web.auth_seen)


@pytest.mark.slow                                  # 실제 시간 다시 시도 간격(1→2→4 s)·응답 지연을 그대로 재므로 약 25 s
def test_http_errors(tmp_path, rigs, monkeypatch):
    monkeypatch.setattr(U, "AUTH_RETRY_S", 0.5)
    monkeypatch.setattr(U, "CONFLICT_RETRY_S", 0.3)
    web = FakeWeb(keepalive_s=0.5).up()
    rig, up = rigs.make(tmp_path / "a", web, token="wrong")
    up.start()
    try:
        time.sleep(2.2)
        n401 = sum(1 for p in web.posts if p["code"] == 401)
        assert 3 <= n401 <= 6 and len(_codes(rig, "UPLINK_AUTH_FAILED")) == 2     # POST·SSE 각각 한 번
        assert up.status()["lastError"] == "HTTP 401" and not up.status()["connected"]
    finally:
        up.stop()
    web.posts.clear()
    rig, up = rigs.make(tmp_path / "b", web)
    up.start()
    try:
        web.status, web.sse_status = 409, 409
        time.sleep(1.5)
        assert len(_codes(rig, "UPLINK_CONFLICT")) == 2 and sum(1 for p in web.posts if p["code"] == 409) >= 3
        web.status = web.sse_status = None
        assert web.wait(lambda: up.status()["lastError"] is None and up.status()["connected"], 3)
        web.posts.clear()
        web.status = 500
        time.sleep(7.5)
        ts = [p["t"] for p in web.posts if p["code"] == 500]
        gaps = [b - a for a, b in zip(ts, ts[1:])]
        assert len(gaps) >= 2 and 0.75 <= gaps[0] <= 1.35 and 1.55 <= gaps[1] <= 2.5, gaps
        assert len(_codes(rig, "UPLINK_DOWN")) == 1 and up.status()["lastError"] == "HTTP 500"
        down = _codes(rig, "UPLINK_DOWN")[0]["data"]
        assert down["level"] == "warn" and down["context"]["source"] == "uplink" and "/dev/ch12" not in down["message"]
        web.status = None
        assert web.wait(lambda: _codes(rig, "UPLINK_UP"), 12)
        assert [p for p in web.posts if p["code"] == 200][0]["types"][0] == "hello"   # 다시 연결 → hello 맨 앞
        web.delay_s = 3.0
        assert web.wait(lambda: up.status()["lastError"] == "timeout", 6)
        web.delay_s = 0.0
        assert web.wait(lambda: up.status()["lastError"] is None, 8)
        web.max_messages = 4
        ids = []
        for i in range(30):
            _info(rig, i)
            ids.append(rig.hub.recent[-1]["id"])
        assert web.wait(lambda: set(ids) <= {m["id"] for m in web.messages()}, 10)
        assert any(p["code"] == 413 for p in web.posts) and up.batch_n <= 3
        assert all(p["n"] <= 4 for p in web.posts if p["code"] == 200)
    finally:
        up.stop()
        web.down()


def test_spool_full_and_slow_consumer_gap(tmp_path, rigs):
    web = FakeWeb(keepalive_s=0.5)                                          # 웹 내려 둔 채
    web.port = _free_port()
    rig, up = rigs.make(tmp_path, web)
    up.spool.max_bytes = 40_000
    up.start()
    try:
        for i in range(300):
            _info(rig, i)
            if i % 50 == 0:
                rig.emit("test", "w", f"w{i}", level="warn", code="W")
        time.sleep(1.0)
        assert _codes(rig, "UPLINK_SPOOL_FULL") and up.spool.dropped > 0 and up.spool.live_bytes <= 40_000
        kept = {json.loads(ln)["data"]["message"] for _, ln in up.spool.batch(10_000, 10**8)}
        assert {f"w{i}" for i in range(0, 300, 50)} <= kept
        assert up.status()["dropped"] == up.spool.dropped
    finally:
        up.stop()
    rig, up = rigs.make(tmp_path / "g", web)
    up.sub_queue = 5
    gate = threading.Event()
    real_put = up.spool.put

    def slow_put(env):
        gate.wait(5)
        return real_put(env)
    up.spool.put = slow_put
    up.start()
    try:
        want = []
        for i in range(40):
            _info(rig, i)
            want.append(rig.hub.recent[-1]["id"])
        gate.set()
        assert web.wait(lambda: up.spool.count >= 40, 5)
        assert up.resubscribes >= 1 and rig.hub.dropped_subs >= 1 and not _codes(rig, "UPLINK_GAP")
        got = [k for k, _ in up.spool.batch(10_000, 10**8)]
        assert [k for k in got if k in want] == want
        gate.clear()
        for i in range(700):
            _info(rig, i)
        gate.set()
        assert web.wait(lambda: _codes(rig, "UPLINK_GAP"), 5)
        g = _codes(rig, "UPLINK_GAP")[0]["data"]["context"]
        assert g["toSeq"] >= g["fromSeq"]
    finally:
        gate.set()
        up.stop()


def test_bench_uploads_but_write_commands_read_only(tmp_path, rigs):
    web = FakeWeb(keepalive_s=0.5).up()
    rig, up = rigs.make(tmp_path, web, mode="bench")
    rig.hub.hello()
    up.start()
    try:
        _info(rig, 1)
        ev = rig.hub.recent[-1]["id"]
        assert web.wait(lambda: ev in {m["id"] for m in web.messages()} and web.sse_connects, 5)
        h = web.messages("hello")[0]["data"]
        assert (h["runMode"], h["writesAllowed"]) == ("bench", False)
        web.push(_cmd("b-stop", "PROCESS_STOP"))
        web.push(_cmd("b-load", "RECIPE_LOAD", {"name": "x"}))
        assert web.wait(lambda: _web_cmd_results(web, "b-stop") and _web_cmd_results(web, "b-load"), 5)
        assert _web_cmd_results(web, "b-stop")[0]["reasonCode"] == "BENCH_READ_ONLY"
        assert _web_cmd_results(web, "b-load")[0]["status"] == "accepted"
    finally:
        up.stop()
        web.down()


def _result_delays(web, n, prefix):
    out = []
    for i in range(n):
        time.sleep(0.13 * (i % 7) + 0.05)
        cid = f"{prefix}{i}"
        t0 = time.monotonic()
        assert web.push(_cmd(cid, "PROCESS_STOP"))
        assert web.wait(lambda: _web_cmd_results(web, cid), 3), cid
        out.append(time.monotonic() - t0)
    return out


def test_high_message_posts_right_away_but_not_during_backoff(tmp_path, rigs):
    web = FakeWeb(keepalive_s=0.5).up()
    rig, up = rigs.make(tmp_path, web, UPLINK_INTERVAL_MS=1000)
    up.start()
    try:
        assert web.wait(lambda: web.sse_connects and any(p["code"] == 200 for p in web.posts), 5)
        e0 = up.early_posts
        d = _result_delays(web, 8, "fast-")
        assert max(d) < 0.6 and sum(d) / len(d) < 0.4, d
        assert up.early_posts - e0 >= 6
        web.status = 500
        assert web.wait(lambda: sum(1 for p in web.posts if p["code"] == 500) >= 2, 5)
        t_fail = [p["t"] for p in web.posts if p["code"] == 500][-1]
        for i in range(3):
            assert web.push(_cmd(f"bo-{i}", "PROCESS_STOP"))
        time.sleep(1.2)
        assert not [p for p in web.posts if p["t"] > t_fail], "백오프 중에 POST 함"
        web.status = None
        assert web.wait(lambda: all(_web_cmd_results(web, f"bo-{i}") for i in range(3)), 8)
    finally:
        up.stop()
        web.down()
    t0 = time.monotonic()
    rig2, up2 = rigs.make(tmp_path / "s", web)                         # 멈출 때는 바로 끝난다(웹 없음)
    up2.start()
    rig2.emit("test", "w", "w", level="warn")
    up2.stop()
    assert time.monotonic() - t0 < 3.0 and not any(t.is_alive() for t in up2._threads)


def test_nan_and_inf_become_null(tmp_path, rigs):
    from v2.ch12 import local_server as LS
    strict = lambda b: json.loads(b, parse_constant=lambda c: (_ for _ in ()).throw(ValueError(c)))
    bad = {"a": float("nan"), "b": [float("inf"), -float("inf"), 1.5], "c": {"d": float("nan")}, "e": "NaN"}
    for raw in (LS._dumps(bad), U._dumps(bad)):
        assert strict(raw) == {"a": None, "b": [None, None, 1.5], "c": {"d": None}, "e": "NaN"}
    web = FakeWeb(keepalive_s=0.5).up()
    rig, up = rigs.make(tmp_path / "w", web)
    up.start()
    try:
        rig.emit("ch1.ig", "pressure", "nan", level="warn", event={"v": float("nan"), "w": float("inf")})
        eid = rig.hub.recent[-1]["id"]
        rig.hub.publish("state", {"snapshot": {"ch1.ig.ion_torr": float("inf"), "ch1.proc.run_id": None}})
        assert web.wait(lambda: eid in {m["id"] for m in web.messages()} and web.states >= 1, 5)
        ctx = web.messages("event")[-1]["data"]["context"]
        assert ctx["v"] is None and ctx["w"] is None
        assert web.posts and all(p["code"] == 200 for p in web.posts)
    finally:
        up.stop()
        web.down()


def test_outage_reconnect_and_restart_keep_messages(tmp_path, rigs):
    """웹 끊김 → 공정·명령은 그대로(보관함에 쌓임)·UPLINK_DOWN → 다시 올라오면 다 올라감·UPLINK_UP·hello 다시.
    웹이 받지 않는 동안 멈춘 뒤 다시 켜면(새 실행) 지난 실행 메시지가 그 instance 그대로 올라간다."""
    web = FakeWeb(keepalive_s=0.5).up()
    rig, up = rigs.make(tmp_path, web)
    hub = rig.hub
    up.start()
    try:
        assert web.wait(lambda: web.posts and web.sse_connects, 5)
        n_hello = len([p for p in web.posts if p.get("types") and p["types"][0] == "hello"])
        web.down()
        t0 = time.monotonic()
        k = 0
        while time.monotonic() - t0 < 6.5:
            hub.run_command("PROCESS_STOP", {}, U.Source.HMI, cid=f"o-{k}")            # 노트북 명령은 그대로 된다
            k += 1
            time.sleep(0.5)
        st = up.status()
        assert not st["connected"] and st["lastError"] and st["queued"] >= k
        assert _codes(rig, "UPLINK_DOWN") and not _codes(rig, "UPLINK_UP") and len(rig.calls) == k
        web.up()
        want = [e["id"] for e in _hub_up(hub) if e["type"] != "state"]
        assert web.wait(lambda: set(want) <= {m["id"] for m in web.messages()}, 15), "끊긴 동안 쌓인 메시지가 다 올라가지 않음"
        assert len([d for d in (m["data"] for m in web.messages("cmd_result")) if d["cmdId"].startswith("o-")]) == k
        assert web.wait(lambda: any(e["data"]["code"] == "UPLINK_UP" for e in web.messages("event")), 5)
        assert len([p for p in web.posts if p.get("types") and p["types"][0] == "hello"]) > n_hello
        assert web.wait(lambda: up.spool.count == 0 and up.status()["connected"], 10)
        web.status = 503
        time.sleep(1.2)
        mark = hub.seq
        for i in range(3):
            hub.run_command("PROCESS_STOP", {}, U.Source.HMI, cid=f"r-{i}")
        time.sleep(0.8)
    finally:
        up.stop()
    old_inst = hub.instance
    pending = [e["id"] for e in _hub_up(hub) if e["seq"] > mark and e["type"] != "state"]
    assert len(pending) >= 3
    web.status = None
    rig2, up2 = rigs.make(tmp_path, web)
    up2.start()
    try:
        assert up2.spool.loaded >= len(pending) and _codes(rig2, "UPLINK_SPOOL_LOADED")
        assert web.wait(lambda: set(pending) <= {m["id"] for m in web.messages()}, 10)
        olds = [m for m in web.messages() if m["id"] in pending]
        assert {m["instance"] for m in olds} == {old_inst} != {rig2.hub.instance}
        assert web.wait(lambda: up2.spool.count == 0, 10)
    finally:
        up2.stop()
        web.down()


def test_command_memory_survives_restart_via_state(tmp_path, rigs):
    """to_state()/from_state() — 저널이 저장한 명령 기억을 넣으면 재시작 전에 실행한 명령이 다시 와도 실행하지 않는다."""
    web = FakeWeb(keepalive_s=0.5).up()
    rig, up = rigs.make(tmp_path / "a", web)
    data = up.handle_command(_cmd("j-1", "PROCESS_STOP", by="a@test"))
    assert data["status"] == "accepted" and len(rig.calls) == 1
    up.handle_command(_cmd("j-exp", "PROCESS_STOP", exp=-60))
    saved = json.loads(json.dumps(up.to_state()))                           # 디스크(JSON) 왕복
    assert saved["v"] == 1 and [k for k, _ in saved["cmds"]] == ["j-1", "j-exp"]
    rig2, up2 = rigs.make(tmp_path / "b", web)
    up2.handle_command(_cmd("j-new", "PROCESS_STOP"))
    assert up2.from_state(saved) == 2 and up2.from_state({"cmds": [["", {}], [1, {}], "x", ["j-new", {"status": "x"}]]}) == 0
    assert list(up2.done_cmds) == ["j-1", "j-exp", "j-new"]                  # 지금 것이 뒤(최근)
    again = up2.handle_command(_cmd("j-1", "PROCESS_STOP", by="a@test"))
    assert again == data and len(rig2.calls) == 1 and up2.dup_cmds == 1     # j-new 만 실행됨
    res = [e["data"] for e in rig2.hub.recent if e["type"] == "cmd_result" and e["data"]["cmdId"] == "j-1"]
    assert res == [data]                                                     # 같은 cmd_result 를 다시
    assert up2.from_state(None) == 0 and up2.from_state({"cmds": "x"}) == 0
    big = {"cmds": [[f"k{i}", {"status": "accepted"}] for i in range(U.CMD_KEEP + 50)]}
    up2.from_state(big)
    assert len(up2.done_cmds) == U.CMD_KEEP and "j-new" in up2.done_cmds
    web.down()

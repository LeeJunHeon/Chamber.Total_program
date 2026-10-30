"""TSP 드라이버 + 시뮬레이터 시험(가상 시계) — v1 골든(dev_tsp.json, v1 device/tsp.py 를 실제로 실행)과 바이트 비교.

골든은 v2/tools/v1golden_tsp.py 가 만든다. 이 시험은 v1 을 import 하지 않는다.
"""
import json
from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import check_value, defaults
from v2.common.snapshot_bus import SnapshotBus, TickLoop
from v2.common.worker import Worker
from v2.ch12.drivers import tsp as T
from v2.ch12.params import tsp as P
from v2.ch12.sim.tsp_sim import TspSim, code_frame, status_frame

GOLDEN = json.loads((Path(__file__).parent / "golden" / "dev_tsp.json").read_text(encoding="utf-8"))
CFG = defaults(P.PARAMS)

# v1 예외 → v2 결과(사유, v1 오류 코드)
V1_EXC = {
    "TSPNackError": ("DEVICE_REJECTED", "E806"), "TSPWindowError": ("DEVICE_REJECTED", "E806"),
    "TSPProtocolError": ("DEVICE_REJECTED", "E805"), "TimeoutError": ("DEVICE_NO_RESPONSE", None),
    "IncompleteReadError": ("LINK_DOWN", "E804"), "TSPError": ("VERIFY_MISMATCH", "E808"),
}


class Rig:
    def __init__(self, writes=True, sim=None, cfg=None, script=None):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.cfg = dict(CFG, **(cfg or {}))
        self.sim = sim or TspSim(script=script)
        if script is not None:
            self.sim.latency_s = 0.001                    # 골든은 127.0.0.1 가짜 서버(지연 거의 0)
        self.events = []
        self.tr = T.make_sim_transport(self.c, self.sim)
        self.w = Worker("tsp", self.tr, writes_allowed=writes)
        self.d = T.TspDriver(self.c, self.w, self.cfg, writes, on_event=self.events.append)
        self.w.start()
        self.c.wait_until(0.5)

    def call(self, op, **kw):
        box = []
        getattr(self.d, op)(on_done=box.append, **kw)
        assert self.c.wait_until(self.c.monotonic() + 30.0, lambda: bool(box)), op
        return box[0]

    def frames(self):
        return [f.hex() for f in self.sim.frames]

    def run(self, n):
        rows = []
        bus = SnapshotBus(self.c, T.blocks(), [self.d.source()], 900)
        TickLoop(self.c, bus, [rows.append]).run(n)
        return rows

    def done(self):
        self.w.stop()


# ───────── 순수 함수: v1 골든 ─────────
def test_build_frame_same_as_v1():
    for g in GOLDEN["frames"]:
        if "hex" in g:
            assert T.build_frame(g["win"], g["write"], g["data"], addr=g["addr"]).hex() == g["hex"], g
        else:
            with pytest.raises(ValueError) as e:
                T.build_frame(g["win"], g["write"], g["data"], addr=g["addr"])
            assert g["exc"] == "ValueError" and str(e.value) == g["msg"], g


def test_parse_response_same_as_v1():
    for g in GOLDEN["parse"]:
        raw = bytes.fromhex(g["raw"])
        if "kind" in g:
            assert T.parse_response(raw) == (g["kind"], g["data"]), g
        else:
            with pytest.raises(T.TspProtocolError) as e:
                T.parse_response(raw)
            assert g["exc"] == "TSPProtocolError" and str(e.value) == g["msg"]


def test_fact_table_bytes():
    assert T.build_frame("011", True, "1").hex(" ") == "02 80 30 31 31 31 31 03 42 33"
    assert T.build_frame("011", True, "0").hex(" ") == "02 80 30 31 31 31 30 03 42 32"
    assert T.build_frame("205", False, None).hex(" ") == "02 80 32 30 35 30 03 38 34"


def test_params_defaults_valid():
    for p in P.PARAMS:
        assert check_value(p, CFG[p.key]) is None, p.key
        assert p.v1_source and p.group == "tsp"


# ───────── v1 골든 대본을 v2 드라이버로 ─────────
def _script(case):
    return [bytes.fromhex(x) if (x is not None and x != "close") else x for x in case["script"]]


@pytest.mark.parametrize("case", GOLDEN["cases"], ids=[c["name"] for c in GOLDEN["cases"]])
def test_same_bytes_and_results_as_v1(case):
    ov = {k: v for k, v in case["cfg"].items() if v is not None}
    if case["cfg"].get("TSP_RS232_ADDR", 0) is None:
        ov["TSP_RS232_ADDR"] = None
    r = Rig(cfg=ov, script=_script(case))
    opmap = {"on": "on", "off": "off", "get_status": "read_status"}
    got = []
    for op in case["ops"]:
        got.append(r.call(opmap[op["op"]], **op["kw"]))
        r.c.wait_until(r.c.monotonic() + 0.05)
    # 보낸 바이트 — 연결 번호까지(통신 예외 뒤 다음 호출이 새 연결)
    want = [t["hex"] for t in case["tx"]]
    if case["name"].startswith("verify_timeout"):
        # 폴링 횟수는 v1 실제 시간(가짜 서버 지연)에 따라 ±1 — 앞(쓰기 + 205)과 횟수 범위만 비교
        assert r.frames()[:2] == want[:2] and set(r.frames()[1:]) == set(want[1:])
        assert abs(len(r.frames()) - len(want)) <= 1, (r.frames(), want)
    else:
        assert r.frames() == want, case["name"]
    conns = [t["conn"] for t in case["tx"]]
    assert r.tr.opens == max(conns), case["name"]
    for g, d in zip(case["results"], got):
        assert d.ok == g["ok"], (case["name"], g, d)
        if not g["ok"]:
            reason, code = V1_EXC[g["exc"]]
            if "예상치 못한 응답" in g.get("msg", ""):
                code = "E807"
            assert (d.reason, d.code) == (reason, code), (case["name"], g, d)
        elif g["op"] == "get_status":
            assert d.status == g["value"], (case["name"], g, d)
    r.done()


# ───────── 모델 시뮬레이터 ─────────
def test_on_off_with_model_and_connect_only_when_needed():
    r = Rig()
    assert r.tr.opens == 0 and not r.tr.link_open                      # 연결은 첫 명령 때
    on = r.call("on")
    assert on.ok and on.status in T.WANT_ON
    off = r.call("off")
    assert off.ok and off.status == 0
    assert r.tr.opens == 1                                              # 같은 연결로 계속
    rows = r.run(3)
    assert all(x["tsp.q"] == "off" for x in rows)                       # 쓰지 않을 때는 1초 읽기 없음
    r.done()


def test_tick_reads_only_while_in_use():
    r = Rig()
    r.d.begin_use()
    rows = r.run(3)
    assert all(x["tsp.q"] == "ok" and x["tsp.status"] == 0 and x["tsp.state"] == "STOP" for x in rows)
    n = len(r.sim.frames)
    r.d.end_use()
    r.c.wait_until(r.c.monotonic() + 0.2)
    assert not r.tr.link_open                                           # 공정 끝에 닫는다(v1 aclose)
    rows = r.run(3)
    assert all(x["tsp.q"] == "off" for x in rows) and len(r.sim.frames) == n
    r.done()


def test_tick_during_verify_uses_poll_value():
    """on 확인(1.5 s 창)이 tick 을 걸쳐도 tick 이 205 를 따로 보내지 않는다 — 확인 폴링의 마지막 값을 싣는다."""
    sim = TspSim()
    sim.interlock = True                                                # 2 에 머묾 → 창 1.5 s 끝까지 폴링
    r = Rig(sim=sim)
    r.d.begin_use()
    r.run(1)
    box = []
    r.c.wait_until(r.c.monotonic() + 0.3)
    t_on = r.c.monotonic()
    r.d.on(on_done=box.append)
    rows = r.run(3)
    assert box and box[0].code == "E808"
    ts = [t for t, d in sim.rx_log if t > t_on and d[2:5] == b"205" and t < t_on + T.VERIFY_WINDOW_S + 0.1]
    gaps = {round(b - a, 6) for a, b in zip(ts, ts[1:])}
    assert gaps == {round(sim.latency_s + CFG["TSP_STATUS_POLL_INTERVAL_S"], 6)}   # 확인 폴링만 — 사이에 낀 tick 읽기 없음
    assert all(x["tsp.q"] == "ok" and x["tsp.status"] == 2 for x in rows)          # tick 은 확인 폴링 값(WAIT_INTERLOCK)
    r.done()


def test_interlock_verify_timeout_reports_e808():
    sim = TspSim()
    sim.interlock = True
    r = Rig(sim=sim)
    d = r.call("on")
    assert not d.ok and d.reason == "VERIFY_MISMATCH" and d.code == "E808" and d.status == 2
    n205 = sum(1 for f in r.sim.frames if f[2:5] == b"205")
    assert 10 <= n205 <= 32                                             # 1.5 s 창, 0.05 s 간격
    assert any(e.kind == "command_result" and e.code == "E808" for e in r.events)
    assert r.tr.link_open                                               # 확인 시간초과는 닫지 않는다(v1 _wait_status)
    assert r.call("read_status").ok and r.tr.opens == 1
    r.done()


def test_disconnect_then_next_call_reconnects():
    sim = TspSim()
    sim.add_fault("disconnect", 0.0, 3.0)
    r = Rig(sim=sim)
    d = r.call("read_status")
    assert not d.ok and d.reason == "LINK_DOWN" and d.code == "E804"
    assert r.w.connected                                                # worker 는 백오프하지 않는다(v1: 다음 호출이 바로 재연결)
    r.c.wait_until(3.5)
    assert r.call("read_status").ok
    r.done()


def test_no_response_and_garbage():
    sim = TspSim()
    sim.add_fault("no_response", 0.0, 1.0)
    r = Rig(sim=sim)
    d = r.call("read_status")
    assert not d.ok and d.reason == "DEVICE_NO_RESPONSE"
    sim.add_fault("garbage", r.c.monotonic(), r.c.monotonic() + 1.0)
    d = r.call("read_status")
    assert not d.ok and d.reason == "DEVICE_REJECTED"                  # STX 아님 → 프로토콜 오류
    r.c.wait_until(r.c.monotonic() + 2.0)
    assert r.call("read_status").ok
    r.done()


def test_bench_rejects_on_off_allows_status():
    r = Rig(writes=False)
    assert r.call("on").reason == "BENCH_READ_ONLY"
    assert r.call("off").reason == "BENCH_READ_ONLY"
    assert r.call("read_status").ok
    assert [f[2:5] for f in r.sim.frames] == [b"205"]
    r.done()


def test_safety_off_purges_and_confirms():
    r = Rig()
    r.call("on")
    box = []
    r.d.read_status(on_done=box.append)
    r.d.off(on_done=box.append, safety=True)
    assert r.c.wait_until(r.c.monotonic() + 10, lambda: len(box) == 2)
    off = [b for b in box if b.action == "off"][0]
    assert off.ok and off.status == 0
    r.done()


def test_nack_sim_and_short_codes():
    sim = TspSim(short_codes=True)
    r = Rig(sim=sim)
    assert r.call("on").ok
    sim.nack = True
    d = r.call("off")
    assert not d.ok and d.code == "E806"
    r.done()


def test_script_helpers_match_golden_frames():
    assert code_frame(0x06).hex() == "028006033835"                    # CRC 는 ASCII 2글자('85')
    assert status_frame(3) == bytes.fromhex(GOLDEN["cases"][0]["script"][1])

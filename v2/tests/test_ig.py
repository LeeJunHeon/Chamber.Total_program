"""챔버12 이온 게이지(IG) 드라이버 + 시뮬레이터 시험(가상 시계·SimTransport — 실제 포트·장비망 접속 없음).

바이트 동일: v2/tests/golden/dev_ig.json(v2/tools/v1golden_ig.py 가 v1 device/ig.py 를 127.0.0.1 가짜 장비에 붙여 실제로 실행)과
같은 응답에서 v2 가 같은 명령을 같은 순서로 보내는지, base_reached·base_failed 문구·pressure 값이 같은지 본다.
골든은 간격·대기를 줄여 만들었으므로(meta.timing_overrides) 같은 값으로 돌린다 — tsp 사례만 v1 기본값 그대로.
"""
import json
from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop
from v2.common.transport import SimTransport
from v2.ch12.drivers import ig as G
from v2.ch12.params import ig as P
from v2.ch12.sim.chamber import Chamber
from v2.ch12.sim.ig_sim import IgSim, fmt_rdi

GOLDEN = json.loads((Path(__file__).parent / "golden" / "dev_ig.json").read_text(encoding="utf-8"))
CFG = dict(defaults(P.PARAMS), TSP_IG_TCP_PORT=4001)


def _overrides(d: dict) -> dict:
    return {k: (json.dumps(v) if isinstance(v, list) else v) for k, v in d.items()}


class Rig:
    def __init__(self, chamber="ch1", writes=True, cfg=None, torr=5e-6, start=1.0):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.ch = Chamber("ch2" if chamber == "tsp" else chamber)
        self.ch.pressure = torr
        self.sim = IgSim(self.ch)
        self.cfg = dict(CFG, **(cfg or {}))
        self.events, self.wev = [], []
        self.w = G.make_worker(self.c, SimTransport(self.c, self.sim), self.cfg, chamber, writes, on_event=self.wev.append)
        self.d = G.IgDriver(self.c, self.w, self.cfg, writes, on_event=self.events.append, chamber=chamber)
        self.w.start()
        self.c.wait_until(start)

    def call(self, fn, *a, **kw):
        box = []
        fn(*a, on_done=lambda k, r, t: (box.append((k, r, t)), self.c.notify()), **kw)
        self.c.wait_until(None, lambda: bool(box))
        return box[0]

    def kinds(self, kind):
        return [e for e in self.events if e.kind == kind]

    def run(self, n):
        b = G.blocks(self.d.chamber)
        bus = SnapshotBus(self.c, b, [Source("ig", self.w, self.d.source_read, (b[0].name,), G.source_deadline_ms(self.cfg))], 900)
        rows = []
        TickLoop(self.c, bus, [rows.append]).run(n)
        return rows

    def done(self):
        self.w.stop()


def test_parse_rdi():
    assert G.parse_rdi("1.2x10e-5") == (1.2e-5, "ok") and G.parse_rdi("IG OFF") == (None, "off")
    assert G.parse_rdi("abc") == (None, "invalid") and G.parse_rdi("") == (None, "none")
    assert fmt_rdi(1.2e-5) == "1.2x10e-5" and fmt_rdi(9.96e-6) == "1.0x10e-5"


@pytest.mark.parametrize("case", GOLDEN["cases"], ids=[c["name"] for c in GOLDEN["cases"]])
def test_golden_bytes_and_events_match_v1(case):
    sec = "CH2" if case["chamber"] == "tsp" else case["chamber"].upper()
    per_ch = {"IG_WAIT_TIMEOUT"}                                           # v2 는 채널 구역 키(CH1_/CH2_)
    over = {(f"{sec}_{k}" if k in per_ch else k): v for k, v in case["cfg"].items()}
    cfg = {} if case["default_cfg"] else _overrides(dict(GOLDEN["meta"]["timing_overrides"], **over))
    r = Rig(case["chamber"], cfg=cfg)
    r.sim.latency_s = 0.001                                                 # 골든의 가짜 장비(127.0.0.1)처럼 바로 응답
    r.sim.rdi_script = list(case["rdi"]) or ["IG OFF"]
    r.sim.sig1_reply = case["sig1"]
    result = None
    for st in case["steps"]:
        if st == "wait":
            result = r.call(r.d.wait_for_base_pressure, case["target"], case["interval_ms"] or 50)
        elif st == "wait_then_cancel":
            box = []
            r.d.wait_for_base_pressure(case["target"], case["interval_ms"], on_done=lambda *a: box.append(a))
            r.c.wait_until(r.c.monotonic() + 0.4)
            r.call(r.d.cancel_wait)
            r.c.wait_until(None, lambda: bool(box))
            result = box[0]
        elif st == "ensure_on":
            r.call(r.d.ensure_on)
        elif st == "read_pressure":
            result = r.call(r.d.read_pressure)
        elif st == "ensure_off":
            r.call(r.d.ensure_off)
        elif st == "bg_poll":
            r.d.set_process_status(True)
            r.c.wait_until(None, lambda: "RDI" in r.sim.commands_seen())
            r.d.set_process_status(False)
        r.c.wait_until(r.c.monotonic() + 0.3)
    assert r.sim.commands_seen() == case["sent_text"]
    assert [bytes.fromhex(h) for h in case["sent"]] == [(x + "\r").encode() for x, _ in
                                                         zip(case["sent_text"], r.sim.received)]
    ev = [{"kind": e.kind, "message": (e.detail or None) if e.kind == "base_failed" else None, "pressure": e.pressure}
          for e in r.events if e.kind in ("base_reached", "base_failed", "pressure")]
    assert ev == case["events"]
    v1 = case["result"]
    if case["steps"][-1] in ("wait", "wait_then_cancel"):
        assert (result[1] is None) == (v1 is True)
    elif "read_pressure" in case["steps"]:
        assert float(result[2]) == v1
    r.done()


def test_wait_timeout_message_has_no_tail():
    """D181 — '목표 압력 도달 실패' 에는 꼬리(최종·목표 Torr)가 붙지 않는다(v1 조건은 '목표 도달 실패')."""
    r = Rig("ch1", torr=5e-5, cfg={"CH1_IG_WAIT_TIMEOUT": 30.0})
    r.ch.pressure_override = 5e-5
    res = r.call(r.d.wait_for_base_pressure, 1e-5, 10000)
    assert res == ("WAIT_FOR_BASE", "BASE_TIMEOUT", "목표 압력 도달 실패")
    assert [e.detail for e in r.kinds("base_failed")] == ["목표 압력 도달 실패"]
    assert any(e.detail == "시간 초과(30.0s): 목표 압력 미도달" for e in r.kinds("status"))
    assert r.sim.commands_seen().count("SIG 0") == 2                       # 시간 초과 OFF + 정리 OFF(v1 골든과 같음)
    r.done()


def test_hard_timeout_when_interval_longer_than_limit():
    r = Rig("ch1", torr=5e-5, cfg={"CH1_IG_WAIT_TIMEOUT": 10.0})
    r.ch.pressure_override = 5e-5
    t0 = r.c.monotonic()
    res = r.call(r.d.wait_for_base_pressure, 1e-5, 60000)
    assert res == ("WAIT_FOR_BASE", "HARD_TIMEOUT", "HardTimeout")
    assert r.c.monotonic() - t0 == pytest.approx(10.0 + 5.0 + 5.0, abs=2.5)  # 시작 + 한도 + 첫 지연 + 5 s(+ SIG 0·정리)
    assert any(e.detail == "하드 타임아웃: 상위 가드에 의해 종료" for e in r.kinds("status"))
    r.done()


def test_wait_uses_v1_poll_interval_and_reaches_base():
    r = Rig("ch2", torr=5e-5)
    r.ch.valves.update(mv=True, turbo=True)
    res = r.call(r.d.wait_for_base_pressure, 2e-6)                         # 기본 interval = IG_POLLING_INTERVAL_MS 10 s
    assert res[1] is None and r.kinds("base_reached")
    rdi_t = [t for t, d in r.sim.received if d == b"RDI\r"]
    assert rdi_t[0] - [t for t, d in r.sim.received if d == b"SIG 1\r"][0] == pytest.approx(0.05 + 5.0 + 0.03, abs=0.02)   # 'OK' 받은 뒤 첫 지연 5 s + 흡수 30 ms
    assert all(b - a >= 10.0 for a, b in zip(rdi_t[1:], rdi_t[2:]))
    r.done()


def test_one_second_reads_rows_and_first_read_suppression():
    r = Rig("ch1", torr=3e-6)
    r.ch.pressure_override = 3e-6
    rows = r.run(6)
    assert rows[-1]["ch1.ig.q"] == "ok" and rows[-1]["ch1.ig.state"] == "off"          # 꺼져 있으면 'IG OFF'
    r.call(r.d.ensure_on)
    r.c.wait_until(r.c.monotonic() + 1.0)
    rows = r.run(8)
    ok = [x for x in rows if x["ch1.ig.pressure_torr.q"] == "ok"]
    assert ok and ok[-1]["ch1.ig.pressure_torr"] == pytest.approx(3e-6, rel=0.05) and ok[-1]["ch1.ig.state"] == "ok"
    assert r.sim.commands_seen().count("SIG 1") == 1                          # 읽기는 상태를 바꾸지 않는다
    box = []
    r.d.wait_for_base_pressure(1e-7, 10000, on_done=lambda *a: box.append(a))
    rows = r.run(6)
    assert "suppressed" in [x["ch1.ig.q"] for x in rows]                       # 점등 뒤 첫 RDI 지연 동안 안 읽음
    r.d.cancel_wait()
    r.c.wait_until(None, lambda: bool(box))
    r.done()


def test_bg_poll_pressure_events_every_10s_with_tick_reads():
    r = Rig("ch1", torr=4e-6)
    r.ch.pressure_override = 4e-6
    r.call(r.d.ensure_on)
    r.d.set_process_status(True)
    rows = r.run(35)
    pe = r.kinds("pressure")
    assert 3 <= len(pe) <= 4 and all(e.pressure == pytest.approx(4e-6, rel=0.05) for e in pe)
    assert sum(1 for x in rows if x["ch1.ig.pressure_torr.q"] == "ok") >= 25
    r.d.set_process_status(False)
    n = len(r.kinds("pressure"))
    r.run(15)
    assert len(r.kinds("pressure")) == n
    r.done()


def test_reignite_on_ig_off_in_sim():
    r = Rig("ch1", torr=5e-5)
    r.sim.trip_at = [8.0]
    box = []
    r.d.wait_for_base_pressure(1e-6, 2000, on_done=lambda *a: box.append(a))
    r.c.wait_until(20.0)
    r.ch.pressure = 5e-7
    r.c.wait_until(None, lambda: bool(box))
    assert box[0][1] is None
    st = [e.detail for e in r.kinds("status")]
    assert "IG OFF 응답 감지 → 자동 재점등 시도(1/3)" in st and "재점등 성공. 첫 RDI 후 폴링 재개" in st
    assert r.sim.commands_seen().count("SIG 1") == 2
    r.done()


def test_bench_read_only():
    r = Rig("ch1", writes=False)
    assert r.call(r.d.wait_for_base_pressure, 1e-6)[1] == "BENCH_READ_ONLY"
    assert r.call(r.d.ensure_on)[1] == "BENCH_READ_ONLY" and r.call(r.d.ensure_off)[1] == "BENCH_READ_ONLY"
    assert r.call(r.d.read_pressure)[1:] == ("DEVICE_REJECTED", "parse error: 'IG OFF'")    # 읽기는 bench 에서도(꺼져 있음)
    rows = r.run(3)
    assert all(c == "RDI" for c in r.sim.commands_seen()) and rows[-1]["ch1.ig.q"] == "ok"
    r.done()


def test_no_reply_keeps_link_and_disconnect_reconnects():
    r = Rig("ch1")
    r.call(r.d.ensure_on)
    t = r.c.monotonic()
    r.sim.add_fault("no_response", t, t + 6.0)
    rows = r.run(6)
    assert "fail" in [x["ch1.ig.q"] for x in rows] and "disconnected" not in [e.kind for e in r.wev]   # v1: 무응답으로 끊지 않음
    r.c.wait_until(t + 7.0)
    res = r.call(r.d.read_pressure)
    assert res[1] is None
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t, t + 3.0)
    r.c.wait_until(t + 8.0)
    assert r.call(r.d.read_pressure)[1] is None
    r.done()


def test_garbage_reply_parse_fail_status():
    r = Rig("ch1", torr=5e-5)
    r.sim.rdi_script = ["abc", "1.0x10e-7"]
    res = r.call(r.d.wait_for_base_pressure, 1e-6, 2000)
    assert res[1] is None and "압력 읽기 실패(파싱): 'abc'" in [e.detail for e in r.kinds("status")]
    r.done()


def test_tsp_ig_uses_ch2_values_and_tsp_port():
    """D189 — TSP 런타임은 cfg 없이 AsyncIG → 기본 cfg = config_ch2, 포트 TSP_IG_TCP_PORT 4001(골든 meta 로 확인)."""
    assert GOLDEN["meta"]["default_cfg_module"] == "lib.config_ch2"
    r = Rig("tsp", cfg={"CH2_IG_WAIT_TIMEOUT": 123.0, "CH1_IG_WAIT_TIMEOUT": 900.0})
    assert G.endpoint(r.cfg, "tsp") == ("192.168.1.50", 4001) and r.d.c("IG_WAIT_TIMEOUT") == 123.0
    assert G.endpoint(r.cfg, "ch2") == ("192.168.1.50", 4002)
    assert r.call(r.d.ensure_on)[1] is None
    r.done()


def test_params_v1_values():
    assert CFG["CH1_IG_TCP_PORT"] == 4001 and CFG["CH2_IG_TCP_PORT"] == 4002
    assert CFG["CH1_IG_WAIT_TIMEOUT"] == 900.0 == GOLDEN["meta"]["ch1_IG_WAIT_TIMEOUT"]
    assert CFG["CH2_IG_WAIT_TIMEOUT"] == 900.0 == GOLDEN["meta"]["ch2_IG_WAIT_TIMEOUT"]
    assert json.loads(CFG["IG_REIGNITE_BACKOFF_MS"]) == [2000, 5000, 10000]
    assert G.BUDGET["queries_per_tick"] == 1 and G.source_deadline_ms(CFG) == 3630

"""두께 모니터 STM-100 드라이버 + 시뮬레이터 시험(가상 시계) — v1 골든(dev_stm100.json)과 바이트·순서 비교.

골든은 v2/tools/v1golden_stm100.py 가 v1 device/thickness_monitor.py·controller/thickness_service.py 를 실제로 돌려 만든다.
이 시험은 v1 을 import 하지 않는다.
"""
import json
from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import check_value, defaults
from v2.common.snapshot_bus import SnapshotBus, TickLoop
from v2.common.worker import Request, Worker
from v2.ch12.drivers import stm100 as D
from v2.ch12.drivers import stm100_proto as SP
from v2.ch12.params import stm100 as P
from v2.ch12.sim.chamber import Chamber
from v2.ch12.sim.stm100_sim import Stm100Sim, frame
from v2.common.transport import SimTransport

G = json.loads((Path(__file__).parent / "golden" / "dev_stm100.json").read_text(encoding="utf-8"))
CFG = defaults(P.PARAMS)
EXC = {"ThicknessValueUnavailable": SP.StmValueUnavailable, "ThicknessProtocolError": SP.StmProtocolError,
       "ThicknessChecksumError": SP.StmChecksumError, "ValueError": ValueError,
       "ThicknessTimeout": SP.StmTimeout, "ThicknessFrameError": SP.StmFrameError,
       "ThicknessCommandRejected": SP.StmCommandRejected, "ThicknessVerifyError": SP.StmVerifyError}
DYADIC_LATENCY = 1 / 64          # 시각 합이 정확히 떨어지게(5 s 경계 비교가 부동소수 잡음에 흔들리지 않게)


def normal_reply(cmd):
    return frame({"S": "A1234", "T": "A+001.2", "U": "A5319234", "V": "A87.5", "M": "A@", "E?": "A8.900",
                  "F?": "A1.000", "J?": "A100.0", "L": "A"}.get(cmd, "A"))


class Rig:
    def __init__(self, writes=True, sim=None, cfg=None, script=None, start=True):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.cfg = dict(CFG, **(cfg or {}))
        self.ch = Chamber("ch2")
        self.sim = sim or Stm100Sim(self.ch, script=script)
        if script is not None:
            self.sim.latency_s = DYADIC_LATENCY
        self.events = []
        self.w = Worker("stm100", SimTransport(self.c, self.sim), writes_allowed=writes, **D.worker_kwargs())
        self.d = D.Stm100Driver(self.c, self.w, self.cfg, writes, on_event=self.events.append, chamber="ch2")
        self.groups = []
        self._n = 0
        self.d.add_listener(self._mark)
        self.w.start()
        self.c.wait_until(0.5)
        if start:
            self.d.start()

    def _mark(self, st):
        n = len(self.sim.cmds)
        self.groups.append(self.sim.cmds[self._n:n])
        self._n = n

    def query(self, cmd, parser=None):
        box = []
        fn = (lambda s: self.d._read_value(s, cmd, parser)) if parser else (lambda s: self.d._query(s, cmd))
        self.w.submit(Request("read", fn, on_done=box.append, exec_timeout_s=30.0))
        assert self.c.wait_until(self.c.monotonic() + 30, lambda: bool(box))
        return box[0]

    def job(self, name, *a, **kw):
        box = []
        getattr(self.d, name)(*a, on_done=box.append, **kw)
        assert self.c.wait_until(self.c.monotonic() + 40, lambda: bool(box))
        return box[0]

    def rows(self, n):
        rows = []
        bus = SnapshotBus(self.c, D.blocks("ch2"), [self.d.source()], 900)
        TickLoop(self.c, bus, [rows.append]).run(n)
        return rows

    def kinds(self):
        return [e.kind for e in self.events]

    def done(self):
        self.d.stop()
        self.w.stop()


# ───────── 순수 함수: v1 골든 ─────────
def _check_call(g, fn, *a):
    if "value" in g:
        assert fn(*a) == g["value"], g
    else:
        with pytest.raises(EXC[g["exc"]]) as e:
            fn(*a)
        assert str(e.value) == g["msg"], g


def test_pure_functions_same_as_v1():
    pu = G["pure"]
    for g in pu["build"]:
        if "hex" in g:
            assert SP.build_frame(g["cmd"]).hex() == g["hex"]
        else:
            _check_call(g, SP.build_frame, g["cmd"])
    for g in pu["extract"]:
        buf = bytearray.fromhex(g["buf"])
        _check_call(g, SP.extract_frame, buf)
        assert bytes(buf).hex() == g["rest"], g
    for g in pu["parse"]:
        if g["fn"] == "parse_number":
            _check_call(g, SP.parse_number, g["body"], g["token"])
        else:
            _check_call(g, getattr(SP, g["fn"]), g["body"])
    for g in pu["material"]:
        if "value" in g:
            assert list(SP.check_material(*g["args"])) == g["value"], g
        else:
            _check_call(g, SP.check_material, *g["args"])
    for g in pu["compact"]:
        assert SP.fmt_compact(g["x"], g["d"]) == g["value"]
    for g in pu["echo"]:
        assert SP.strip_echo(g["body"], g["token"]) == g["value"]


def test_error_text_labels_same_as_v1():
    for v1name, label in G["pure"]["error_text_labels"].items():
        cls = {"ThicknessCommError": SP.StmCommError, "ThicknessError": SP.StmError}.get(v1name) or EXC[v1name]
        e = cls("c", "x") if cls is SP.StmCommandRejected else cls("x")
        assert SP.error_text(e).startswith(label + " — "), v1name


def test_fact_table_bytes_and_params():
    assert SP.build_frame("S").hex(" ") == "02 01 53 53" and SP.build_frame("E?").hex(" ") == "02 02 45 3f 84"
    assert SP.build_frame("E=1.23").hex(" ") == "02 06 45 3d 31 2e 32 33 46"
    for p in P.PARAMS:
        assert check_value(p, CFG[p.key]) is None and p.group == "stm100" and p.v1_source
    assert CFG["CH2_THICKNESS_TCP_PORT"] == 4008 and CFG["CH2_SUPPORTS_THICKNESS"] is True
    assert CFG["CH1_SUPPORTS_THICKNESS"] is False and CFG["CH1_THICKNESS_TCP_PORT"] == 0


def test_settings_clamps_like_v1():
    c = dict(CFG, CH2_THICKNESS_POLL_INTERVAL_S=0.1, CH2_THICKNESS_STALE_S=0.5, CH2_THICKNESS_CMD_TIMEOUT_S=9.0)
    s = D.settings(c, "ch2")
    assert (s.poll_s, s.stale_s, s.cmd_timeout_s) == (0.25, 2.0, 5.0) and s.warnings
    s = D.settings(dict(CFG, CH1_SUPPORTS_THICKNESS=True), "ch1")
    assert not s.enabled and "끔" in s.warnings[0]                     # 포트 0 이면 사용으로 켜도 끈다


# ───────── 장치 명령: v1 골든 대본 ─────────
OPS = {"read_thickness_a": ("S", SP.parse_thickness_a), "read_rate_a_s": ("T", SP.parse_rate_a_s),
       "read_frequency_hz": ("U", SP.parse_frequency_hz), "read_crystal_life": ("V", SP.parse_life_pct),
       "read_crystal_ok": ("M", SP.parse_crystal_ok), "read_density": ("E?", lambda b: SP.parse_number(b, "E")),
       "read_z_factor": ("F?", lambda b: SP.parse_number(b, "F")), "read_tooling": ("J?", lambda b: SP.parse_number(b, "J")),
       "read_version": ("@", lambda b: b.strip()), "read_reset_status": ("a", lambda b: SP.strip_echo(b, "a"))}
JOB_REASON = {"ThicknessCommandRejected": "DEVICE_REJECTED", "ValueError": "BAD_ARGS",
              "ThicknessVerifyError": "VERIFY_MISMATCH", "ThicknessTimeout": "DEVICE_NO_RESPONSE"}


@pytest.mark.parametrize("case", G["device"], ids=[c["name"] for c in G["device"]])
def test_device_commands_same_bytes_as_v1(case):
    script = [None if x is None else bytes.fromhex(x) for x in case["script"]]
    r = Rig(script=script, start=False, cfg={"CH2_THICKNESS_CMD_TIMEOUT_S": case["timeout_s"]})
    for g in case["results"]:
        op = g["op"]
        if op in OPS:
            cmd, parser = OPS[op]
            res = r.query(cmd, parser)
            if g["ok"]:
                assert res.status == "ok" and res.value == g["value"], (case["name"], g, res)
            else:
                assert res.status != "ok" and g["exc"] in EXC and EXC[g["exc"]].__name__ in res.error, (g, res)
                assert g["msg"] in res.error or g["msg"].replace("'", "\\'") in res.error, (g, res.error)
        else:
            d = r.job(op, *g["args"])
            assert d.ok == g["ok"], (case["name"], g, d)
            if g["ok"] and g.get("value") is not None:
                assert d.value == g["value"]
            if not g["ok"]:
                assert d.reason == JOB_REASON[g["exc"]] and g["msg"] in d.detail, (g, d)
        r.c.wait_until(r.c.monotonic() + 0.05)
    assert r.sim.cmds == [t["cmd"] for t in case["tx"]], case["name"]
    sent = [d.hex() for _, d in r.sim.rx_log]
    assert sent == [t["hex"] for t in case["tx"]]
    assert r.d.reset_seen == case["reset_seen"]
    r.done()


# ───────── 상시 읽기: v1 ThicknessService 골든 ─────────
def _service(case_name):
    return next(c for c in G["service"] if c["name"] == case_name)


def test_service_cadence_like_v1():
    case = _service("normal_cadence")
    r = Rig(script=[], start=False)
    r.sim.script_default = normal_reply
    r.d.start()
    r.c.wait_until(0.5 + 10.75)
    want = [p["cmds"] for p in case["polls"]]                           # v1: 0, 0.5, 1.0, 5.25, 5.75, 10.5 s
    got = [r.groups[i] for i in (0, 1, 2, 10, 11, 20)]                  # v2: 0.5 s 마다 — 5 s 지난 첫 poll 에서 U·V·M
    assert got == want
    assert all(g == ["S", "T"] for i, g in enumerate(r.groups[:21]) if i not in (0, 10, 20))
    st = r.d.status()
    assert st["seq"] == len(r.groups) and st["thickness_nm"] == pytest.approx(123.4) and st["rate_nm_s"] == pytest.approx(0.12)
    assert st["freq_hz"] == 5319234 and st["life_pct"] == 87.5 and st["crystal_ok"] is True
    assert (st["density"], st["z_factor"], st["tooling"]) == (8.9, 1.0, 100.0)
    r.c.wait_until(0.5 + 60.25)
    assert r.groups[120][:2] == ["S", "T"] and r.groups[120][-3:] == ["E?", "F?", "J?"]   # 60 s 마다 적용값
    r.done()


@pytest.mark.parametrize("name", ["xtal_fail_reads_m_now", "format_error_no_reconnect", "dead_after_ok",
                                  "dead_device_first_poll"])
def test_service_sequences_like_v1(name):
    case = _service(name)
    polls = case["polls"]
    r = Rig(script=[], start=False, cfg={"CH2_THICKNESS_CMD_TIMEOUT_S": case["timeout_s"]})
    r.sim.script_default = normal_reply
    # v1 골든 생성 때 poll 마다 넣은 응답(도구의 plan) — 같은 응답을 같은 poll 에
    plan = {"xtal_fail_reads_m_now": [[], ["A--------", "A--------", "A    . ", "A    . ", "A!"], ["A--------", "A--------"], []],
            "format_error_no_reconnect": [[], ["A12 .4"], ["F"], []],
            "dead_after_ok": [[], [None] * 3, [None] * 3, []],
            "dead_device_first_poll": [[None] * 18, []]}[name]
    assert len(plan) == len(polls)
    feed = [[None if x is None else frame(x) for x in p] for p in plan]
    r.sim.script = list(feed[0])
    orig = r._mark

    def mark(st):
        orig(st)
        i = len(r.groups)
        if i < len(feed):
            r.sim.script += feed[i]
    r.d.remove_listener(r._mark)
    r.d.add_listener(mark)
    r.d.start()
    r.c.wait_until(r.c.monotonic() + 30.0, lambda: len(r.groups) >= len(polls))
    for i, p in enumerate(polls):
        assert r.groups[i] == p["cmds"], (name, i, r.groups[:len(polls)])
    if name == "dead_device_first_poll":
        assert not r.w.connected or r.groups[1] == []
        assert r.kinds().count("unresponsive") == 1
    if name == "format_error_no_reconnect":
        assert r.w.connected and "format_error" in r.kinds()
    if name == "xtal_fail_reads_m_now":
        assert r.d.status()["crystal_ok"] is False and "unavailable" in r.kinds()
    r.done()


# ───────── 모델 시뮬레이터 ─────────
def test_thickness_follows_chamber_and_seq_listener():
    r = Rig()
    r.ch.set_valves(ms=True)
    r.ch.gas_sccm["ar"] = 20.0
    r.ch.pressure_override = 5e-3
    r.ch.dcp_w = 500.0                                                  # 5 Å/s
    seqs = []
    r.d.add_listener(lambda st: seqs.append(st["seq"]))
    r.c.wait_until(r.c.monotonic() + 4.0)
    st = r.d.status()
    assert st["rate_nm_s"] == pytest.approx(0.5) and st["thickness_nm"] > 1.0
    assert seqs == sorted(seqs) and seqs[-1] >= 8                       # 새 S 마다 seq +1(0.5 s)
    # 영점 — 장비 두께가 0 근처로
    z = r.job("zero", "B")
    assert z.ok
    r.c.wait_until(r.c.monotonic() + 0.3)
    assert r.d.status()["thickness_nm"] < 0.3
    assert "B" in r.sim.cmds and r.sim.cmds[r.sim.cmds.index("B") + 1] == "S"   # 요청 뒤 바로 poll
    r.done()


def test_tick_rows_carry_last_value_and_age():
    r = Rig()
    rows = r.rows(4)
    assert all(x["ch2.stm100.q"] == "ok" for x in rows)
    assert all(x["ch2.stm100.value_age_s"] is not None and x["ch2.stm100.value_age_s"] < 0.6 for x in rows)
    assert rows[-1]["ch2.stm100.crystal_ok"] is True and rows[-1]["ch2.stm100.link_ok"] is True
    r.sim.add_fault("no_response", r.c.monotonic(), r.c.monotonic() + 100.0)
    rows = r.rows(3)
    assert rows[-1]["ch2.stm100.value_age_s"] > 1.5                     # 1초 행은 마지막 값 + 나이
    r.done()


def test_xtal_fail_model_and_recovery():
    r = Rig()
    r.c.wait_until(r.c.monotonic() + 1.0)
    r.ch.xtal_fail = True
    r.c.wait_until(r.c.monotonic() + 1.0)
    st = r.d.status()
    assert st["crystal_ok"] is False and r.w.connected and st["link_ok"]   # 값 없음은 재연결 사유 아님
    assert "M" in r.sim.cmds[-12:]
    r.ch.xtal_fail = False
    r.c.wait_until(r.c.monotonic() + 6.0)
    assert r.d.status()["crystal_ok"] is True
    r.done()


def test_six_failures_reconnect_with_backoff_and_recover():
    r = Rig()
    r.c.wait_until(r.c.monotonic() + 1.0)
    t0 = r.c.monotonic()
    r.sim.add_fault("no_response", t0, t0 + 25.0)
    r.c.wait_until(t0 + 30.0)
    k = r.kinds()
    assert k.count("unresponsive") == 1 and k.count("recovered") == 1
    assert r.w.connected and r.d.status()["link_ok"]
    r.done()


def test_reconnect_backoff_1_2_4_8_16_30():
    c = VirtualClock()
    c.register()
    sim = Stm100Sim()
    sim.add_fault("disconnect", 0.0, 200.0)
    ev = []
    w = Worker("stm100", SimTransport(c, sim), on_event=ev.append, **D.worker_kwargs())
    d = D.Stm100Driver(c, w, CFG, True, chamber="ch2")
    w.start()
    d.start()
    c.wait_until(150.0)
    t = [e.t for e in ev if e.kind == "connect_failed"]
    assert [round(b - a, 6) for a, b in zip(t, t[1:])][:8] == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]
    assert not [e for e in ev if e.kind == "long_outage"]               # v1 은 장기 두절 구분 없음
    d.stop()
    w.stop()


def test_reset_flag_counts_and_acks():
    r = Rig()
    r.c.wait_until(r.c.monotonic() + 1.0)
    r.sim.power_cycle()
    r.c.wait_until(r.c.monotonic() + 1.0)
    assert r.d.reset_seen == 1 and r.d.status()["reset_seen"] == 1 and "L" in r.sim.cmds
    assert not r.sim.reset_flag and "reset_flag" in r.kinds()
    r.done()


def test_set_material_with_model_and_verify_failure():
    r = Rig()
    d = r.job("set_material", 19.3, 0.381, 87.4, material="Au")
    assert d.ok and d.value == {"density": 19.3, "z_factor": 0.381, "tooling": 87.4}
    st = r.d.status()
    assert st["material"] == "Au" and st["tooling"] == 87.4
    r.sim.sticky_tooling = True
    d = r.job("set_material", 8.9, 1.0, 100.0)
    assert not d.ok and d.reason == "VERIFY_MISMATCH" and d.code == "E846"
    assert r.sim.cmds.count("J=100") == 2                               # 1회 다시 쓴 뒤 실패
    d = r.job("set_material", 0.1, 1.0, 100.0)
    assert not d.ok and d.reason == "BAD_ARGS" and "E=0.1" not in r.sim.cmds
    r.done()


def test_bench_rejects_writes_allows_reads():
    r = Rig(writes=False)
    assert r.job("zero", "B").reason == "BENCH_READ_ONLY"
    assert r.job("set_material", 8.9, 1.0, 100.0).reason == "BENCH_READ_ONLY"
    r.c.wait_until(r.c.monotonic() + 1.0)
    assert "B" not in r.sim.cmds and not [c for c in r.sim.cmds if "=" in c]
    assert r.d.status()["seq"] >= 2
    r.done()


def test_disconnect_and_reconnect():
    r = Rig()
    t0 = r.c.monotonic()
    r.sim.add_fault("disconnect", t0 + 1.0, t0 + 4.0)
    r.c.wait_until(t0 + 2.0)
    assert not r.w.connected
    rows = r.rows(1)
    assert rows[0]["ch2.stm100.q"] == "fail"
    r.c.wait_until(t0 + 12.0)
    assert r.w.connected and r.d.status()["link_ok"]
    r.done()


def test_garbage_is_frame_error_retry():
    r = Rig()
    t0 = r.c.monotonic()
    r.sim.add_fault("garbage", t0, t0 + 0.4)
    r.c.wait_until(t0 + 2.0)
    assert r.w.connected and r.d.status()["link_ok"]
    r.done()


def test_disabled_channel_is_off_and_rejects_jobs():
    c = VirtualClock()
    c.register()
    w = Worker("stm100", SimTransport(c, Stm100Sim()), **D.worker_kwargs())
    d = D.Stm100Driver(c, w, CFG, True, chamber="ch1")
    assert not d.enabled and not d.source().enabled
    box = []
    d.zero("B", on_done=box.append)
    assert box[0].reason == "DEVICE_DISABLED"

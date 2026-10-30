"""RF Pulse(AE CESAR) 드라이버 + 시뮬레이터 시험(가상 시계 + Worker + SimTransport). 실제 포트는 열지 않는다.

v1 비교: v2/tests/golden/dev_rfpulse.json(v2/tools/v1golden_rfpulse.py 가 v1 device/rf_pulse.py 를 127.0.0.1 가짜 서버에 붙여
실제로 실행해 만든 것)과 같은 시뮬레이터 상태에서 v2 가 같은 바이트를 같은 순서로 보내고 같은 결과·이벤트를 내는지 본다.
"""
import json
from pathlib import Path

import pytest

from v2.ch12.drivers import rfpulse as R
from v2.ch12.params import rfpulse as PR
from v2.ch12.sim.chamber import Chamber
from v2.ch12.sim.rfpulse_sim import RfPulseSim
from v2.common import aebus
from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop
from v2.common.transport import SimTransport
from v2.common.worker import Worker

GOLD = json.loads((Path(__file__).parent / "golden" / "dev_rfpulse.json").read_text(encoding="utf-8"))
CFG = defaults(PR.PARAMS)


class Rig:
    def __init__(self, writes=True, cfg=None, control=2, chamber="ch1"):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.ch = Chamber(chamber)
        self.sim = RfPulseSim(self.ch, control=control)
        self.cfg = dict(CFG, **(cfg or {}))
        self.events = []
        self.d = R.RfPulseDriver(self.c, None, self.cfg, writes, on_event=self.events.append, chamber=chamber)
        self.w = Worker("rfp", SimTransport(self.c, self.sim), writes_allowed=writes, **self.d.worker_params())
        self.d.bind(self.w)
        self.w.start()
        self.c.wait_until(0.5)

    def call(self, fn, *a, **kw):
        box = []
        fn(*a, on_done=lambda *r: (box.append(r), self.c.notify()), **kw)
        self.c.wait_until(None, lambda: bool(box))
        return box[0]

    def rows(self, n):
        bus = SnapshotBus(self.c, R.blocks("ch1"), [Source("ch1.rfpulse", self.w, self.d.source_read, ("ch1.rfpulse",))])
        out = []
        TickLoop(self.c, bus, [out.append]).run(n)
        return out

    def ev(self, kind):
        return [e for e in self.events if e.kind == kind]

    def sent(self, since=0):
        return [f.hex(" ") for _, f in list(self.sim.received)[since:]]

    def times(self, since=0):
        return [t for t, _ in list(self.sim.received)[since:]]

    def done(self):
        self.w.stop()


def test_frames_match_v1():
    for g in GOLD["frames"]:
        assert aebus.build(g["addr"], g["cmd"], bytes.fromhex(g["data"])).hex(" ") == g["hex"], g["name"]


def test_status_and_duty_limits_match_v1():
    f = GOLD["functions"]
    for k, lim in f["duty_limits"]:
        assert list(R.duty_limits_pct(k)) == lim
    keys = ("on", "req", "sp_miss", "ilock", "ot", "limI", "xflt", "cex")
    for raw, want in f["status"]:
        st = R.parse_status(bytes.fromhex(raw))
        assert (None if st is None else [st[k] for k in keys]) == want


@pytest.mark.parametrize("case", GOLD["cases"], ids=[c["name"] for c in GOLD["cases"]])
def test_case_bytes_match_v1(case):
    r = Rig()
    if case["pre_on"]:
        assert r.call(r.d.start_pulse_process, 100.0) == (None, True)
        r.d.polling = False
        r.c.wait_until(r.c.monotonic() + 2.0)
    for k, v in case["sim"].items():
        setattr(r.sim, k, {int(a): (list(b) if isinstance(b, list) else b) for a, b in v} if k == "csr_once" else v)
    n0 = len(r.sim.received)
    r.events.clear()
    a = dict(case["args"])
    call = case["call"]
    if call == "start_pulse_process":
        reason, ok = r.call(r.d.start_pulse_process, a.pop("target_w"), a.get("freq_hz"), a.get("duty_percent"))
        assert ok is case["result"]
    elif call == "set_reference_power":
        assert r.call(r.d.set_reference_power, a["target_w"])[1] is case["result"]
    elif call == "read_actual_pulse_params":
        assert r.call(r.d.read_actual_pulse_params)[1] == case["result"]
    else:
        r.call(getattr(r.d, call))
    assert r.sent(n0) == case["sent"]
    mine = [[e.kind, e.cmd, e.detail if e.kind == "command_failed" else None] for e in r.events
            if e.kind in ("command_failed", "target_reached", "power_off_finished")]
    theirs = [[k, c, why if k == "command_failed" else None] for k, c, why in case["events"]]
    assert mine == theirs
    r.done()


def test_command_gap_is_v1_1500ms():
    r = Rig(control=6)
    assert r.call(r.d.start_pulse_process, 100.0, 1000, 50) == (None, True)
    ts = r.times()
    assert all(b - a >= 1.5 for a, b in zip(ts, ts[1:]))
    r.done()


def test_poll_cycle_v1_order_and_quality():
    r = Rig()
    assert r.call(r.d.start_pulse_process, 100.0) == (None, True)
    n0 = len(r.sim.received)
    rows = r.rows(20)
    cmds = [bytes.fromhex(f)[1] for f in r.sent(n0)]
    assert cmds[:6] == [162, 165, 166, 162, 165, 166]                               # v1 순서 162 → 165 → 166
    ts = r.times(n0)
    assert ts[1] - ts[0] >= 1.5 and ts[3] - ts[2] >= 5.0                            # 간격 1.5 s, 바퀴 사이 5 s
    assert all(x["ch1.rfpulse.q"] == "ok" for x in rows)
    assert {x["ch1.rfpulse.fwd_w.q"] for x in rows} == {"ok", "stale"}
    assert rows[-1]["ch1.rfpulse.fwd_w"] == pytest.approx(100.0, abs=1) and rows[-1]["ch1.rfpulse.set_w"] == 100.0
    assert rows[-1]["ch1.rfpulse.ref_w"] == pytest.approx(60.0, abs=2)              # 점화 전(가스 없음) 반사 큼
    r.done()


def test_refp_monitor_repeats_every_3_cycles():
    r = Rig()
    assert r.call(r.d.start_pulse_process, 100.0) == (None, True)
    r.rows(60)                                                                     # 반사 60 W ≥ 20 W, 바퀴 ≈ 8 s → 약 7바퀴
    fails = [e for e in r.ev("command_failed") if e.cmd == "REFP_MONITOR"]
    assert len(fails) == 2 and fails[0].detail == "REFP가 20.0W 이상 (3회 연속)"     # 3회마다 다시(v1 — 카운터만 0)
    assert not [e for e in r.ev("command_failed") if e.cmd == "FORP_MONITOR"]
    r.done()


def test_forp_monitor_and_ch2_refp_warn():
    r = Rig(chamber="ch2")
    r.ch.gas_sccm["ar"] = 20.0
    r.ch.pressure_override = 0.005
    r.sim.fwd_scale = 0.5                                                          # 순방향 50 W(설정 100 W)
    r.sim.ref_inject_w = 6.0
    assert r.call(r.d.start_pulse_process, 100.0) == (None, True)
    bus = SnapshotBus(r.c, R.blocks("ch2"), [Source("ch2.rfpulse", r.w, r.d.source_read, ("ch2.rfpulse",))])
    TickLoop(r.c, bus, []).run(30)
    assert [e for e in r.ev("command_failed") if e.cmd == "FORP_MONITOR"]
    warn = [e for e in r.ev("status") if e.detail.startswith("⚠ REFP_WARN")]
    assert len(warn) == 1 and "≥ 5.0W" in warn[0].detail                            # CH2 5 W, 한 번만
    r.done()


def test_stop_guard_blocks_settings_after_stop():
    r = Rig(control=6)
    box = []
    r.d.start_pulse_process(100.0, 1000, 50, on_done=lambda *x: box.append(x))
    r.c.wait_until(r.c.monotonic() + 5.0)
    assert r.call(r.d.stop_process) == (None, True)
    r.c.wait_until(r.c.monotonic() + 20.0)
    assert box and box[0][1] is False
    cmds = [bytes.fromhex(f)[1] for f in r.sent()]
    i = len(cmds) - 1 - cmds[::-1].index(1)
    assert all(c in (1, 14) or c >= 128 for c in cmds[i:]) and 2 not in cmds        # 정지 뒤 RF ON 은 나가지 않음
    assert r.ev("power_off_finished")
    r.done()


def test_off_unconfirmed_when_device_silent():
    r = Rig()
    assert r.call(r.d.start_pulse_process, 100.0) == (None, True)
    r.sim.add_fault("no_response", r.c.monotonic(), r.c.monotonic() + 100.0)
    t0 = r.c.monotonic()
    reason, ok = r.call(r.d.stop_process)
    assert ok is False and reason == "RFP_OFF_UNCONFIRMED" and r.d.output_off_unconfirmed
    assert r.c.monotonic() - t0 <= 20.5
    fail = [e for e in r.ev("command_failed") if e.cmd == "RF_OFF"]
    assert len(fail) == 1 and "출력 상태 미확인" in fail[0].detail
    r.done()


def test_unused_off_once_without_warning():
    r = Rig()
    r.sim.add_fault("no_response", 0.0, 100.0)
    t0 = r.c.monotonic()
    assert r.call(r.d.stop_unused) == (None, True)
    assert r.c.monotonic() - t0 <= 3.05 and r.sent() == ["08 01 09"]
    assert r.ev("power_off_finished") and not r.ev("command_failed")
    r.done()


def test_nak_and_bad_checksum():
    r = Rig()
    assert r.call(r.d.start_pulse_process, 100.0) == (None, True)
    t = r.c.monotonic()
    r.sim.add_fault("garbage", t, t + 20.0)
    rows = r.rows(10)
    assert any(x["ch1.rfpulse.q"] == "fail" for x in rows)
    assert any("FRAME(cs_bad)" in e.detail for e in r.ev("status"))
    assert r.w.connected                                                           # 무응답·형식 오류로는 끊지 않음(v1)
    r.done()


def test_bench_rejects_writes():
    r = Rig(writes=False)
    assert r.call(r.d.start_pulse_process, 100.0) == ("BENCH_READ_ONLY", False)
    assert r.call(r.d.stop_process) == ("BENCH_READ_ONLY", False)
    assert r.sent() == []
    r.done()


def test_reconnect_after_disconnect():
    r = Rig()
    assert r.call(r.d.start_pulse_process, 100.0) == (None, True)
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t + 1.0, t + 4.0)
    rows = r.rows(25)
    assert any(x["ch1.rfpulse.q"] == "fail" for x in rows) and rows[-1]["ch1.rfpulse.q"] == "ok"
    assert any(x["ch1.rfpulse.fwd_w.q"] == "ok" for x in rows[-10:])
    r.done()


def test_budget():
    assert R.BUDGET["min_gap_ms"] == 1500 and R.BUDGET["fits_1s"] is False and R.BUDGET["queries_per_tick"] == 1


def test_cleanup_confirms_off_when_maybe_on():
    r = Rig()
    assert r.call(r.d.start_pulse_process, 100.0) == (None, True)
    box = []
    r.d.cleanup(on_done=lambda u: (box.append(u), r.c.notify()))
    r.c.wait_until(None, lambda: bool(box))
    assert box == [False] and not r.sim.out and r.ev("power_off_finished") and not r.d.output_off_unconfirmed
    r.done()


def test_poll_once_order():
    r = Rig()
    n0 = len(r.sim.received)
    assert r.call(r.d.poll_once) == (None, True)
    assert [bytes.fromhex(f)[1] for f in r.sent(n0)] == [162, 165, 166]
    assert any(e.detail.startswith("[ONCE] STATUS") for e in r.ev("status")) and r.ev("power")
    r.done()

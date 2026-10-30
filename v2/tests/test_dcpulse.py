"""DC Pulse(EnerPulse EP5) 드라이버 + 시뮬레이터 시험(가상 시계 + Worker + SimTransport). 실제 포트는 열지 않는다.

v1 비교: v2/tests/golden/dev_dcpulse.json(v2/tools/v1golden_dcpulse.py 가 v1 device/dc_pulse.py 를 127.0.0.1 가짜 서버에 붙여
실제로 실행해 만든 것)과 같은 시뮬레이터 상태에서 v2 가 같은 바이트를 같은 순서로 보내는지 본다.
"""
import json
from pathlib import Path

import pytest

from v2.ch12.drivers import dcpulse as D
from v2.ch12.drivers import dcpulse_proto as P
from v2.ch12.params import dcpulse as PD
from v2.ch12.sim.chamber import Chamber
from v2.ch12.sim.dcpulse_sim import DcPulseSim
from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop
from v2.common.transport import SimTransport
from v2.common.worker import Worker

GOLD = json.loads((Path(__file__).parent / "golden" / "dev_dcpulse.json").read_text(encoding="utf-8"))
CFG = defaults(PD.PARAMS)
POLL_CMDS = {0x9A, 0x96, 0x99, 0xAE, 0xAF}


class Rig:
    def __init__(self, writes=True, cfg=None, lit=True, chamber="ch1"):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.ch = Chamber(chamber)
        if lit:
            self.ch.gas_sccm["ar"] = 20.0
            self.ch.pressure_override = 0.005
        self.sim = DcPulseSim(self.ch)
        self.cfg = dict(CFG, **(cfg or {}))
        self.events = []
        self.d = D.DcPulseDriver(self.c, None, self.cfg, writes, on_event=self.events.append, chamber=chamber)
        self.w = Worker("dcp", SimTransport(self.c, self.sim), writes_allowed=writes, **self.d.worker_params())
        self.d.bind(self.w)
        self.w.start()
        self.c.wait_until(0.5)

    def call(self, fn, *a, **kw):
        box = []
        fn(*a, on_done=lambda *r: (box.append(r), self.c.notify()), **kw)
        self.c.wait_until(None, lambda: bool(box))
        return box[0]

    def start(self, power=100.0, **kw):
        return self.call(self.d.prepare_and_start, power, **kw)

    def rows(self, n):
        bus = SnapshotBus(self.c, D.blocks("ch1"), [Source("ch1.dcpulse", self.w, self.d.source_read, ("ch1.dcpulse",))])
        out = []
        TickLoop(self.c, bus, [out.append]).run(n)
        return out

    def ev(self, kind):
        return [e for e in self.events if e.kind == kind]

    def sent(self, since=0):
        return [f.hex(" ") for _, f in list(self.sim.received)[since:]]

    def done(self):
        self.w.stop()


def norm(frames):
    """연속 중복을 하나로(활성화 대기 PIV 횟수는 실제 시간 지터), 출력 ON 뒤 0x90 다음의 폴링 읽기는 뺀다(v1 폴링 태스크가 끼어듦)."""
    out, seen_on, seen_st = [], False, False
    for f in frames:
        b = bytes.fromhex(f)
        if seen_st and b[1] in POLL_CMDS:
            continue
        if b[1] == 0x80 and len(b) == 6 and b[3] == 1:
            seen_on = True
        if seen_on and b[1] == 0x90:
            seen_st = True
        if not out or out[-1] != f:
            out.append(f)
    return out


# ───────── 프레임·순수 함수(v1 골든) ─────────
def test_frames_match_v1():
    for g in GOLD["frames"]:
        got = P.pack_write(g["code"], g["value"], 2) if g["kind"] == "write" else P.pack_read(g["code"])
        assert got.hex(" ") == g["hex"], g["name"]
    assert P.pack_write(P.OFF_TIME, P.off_time_to_raw(5.0), 2).hex(" ") == "02 67 00 32 03 9e"       # 매뉴얼 p.50 예


def test_pure_functions_match_v1():
    f = GOLD["functions"]
    assert [[k, P.max_off_time_us(k)] for k, _ in f["max_off_time_us"]] == f["max_off_time_us"]
    assert [[u, P.off_time_to_raw(u)] for u, _ in f["off_time_to_raw"]] == f["off_time_to_raw"]
    assert [[r, P.raw_to_off_time(r)] for r, _ in f["raw_to_off_time"]] == f["raw_to_off_time"]
    assert [[k, d, P.duty_to_off_time_us(k, d)] for k, d, _ in f["duty_to_off"]] == f["duty_to_off"]
    assert [[k, o, P.off_time_to_duty_pct(k, o)] for k, o, _ in f["off_to_duty"]] == f["off_to_duty"]
    assert [[a, b, c, d, P.arc_reset_state(a, b, c, d)] for a, b, c, d, _ in f["arc_reset_state"]] == f["arc_reset_state"]
    assert [[a, b, c, d, P.arc_split(a, b, c, d)] for a, b, c, d, _ in f["arc_split"]] == f["arc_split"]
    assert [[p, i, v, list(r) if (r := P.piv_scale_check(p, i, v)) else None] for p, i, v, _ in f["piv_scale_check"]] == f["piv_scale_check"]
    for w, lim in f["overcurrent_limit"]:
        assert P.overcurrent_limit_a(w, 300.0, 1.0, 600.0, 2.0, True) == pytest.approx(lim)


def test_parser_matches_v1():
    for g in GOLD["parser"]:
        fp = P.FrameParser()
        assert [t.hex(" ") for t in fp.feed(bytes.fromhex(g["raw"]))] == g["tokens"], g["raw"]


# ───────── v1 실제 실행과 같은 바이트(골든 경우 전부) ─────────
@pytest.mark.parametrize("case", GOLD["cases"], ids=[c["name"] for c in GOLD["cases"]])
def test_case_bytes_match_v1(case):
    r = Rig()
    if case["pre_on"]:
        assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
        r.d.polling = False
        r.c.wait_until(r.c.monotonic() + 0.5)
    for k, v in case["sim"].items():
        setattr(r.sim, k, v)
    n0 = len(r.sim.received)
    r.events.clear()
    a = case["args"]
    if case["call"] == "prepare_and_start":
        reason, ok = r.call(r.d.prepare_and_start, a.pop("power_w"), **a)
        assert ok is case["result"]
        assert r.d.last_failure == case["last_failure"]
    elif case["call"] == "set_reference_power":
        assert r.call(r.d.set_reference_power, a["value_w"])[1] is case["result"]
    elif case["call"] == "read_actual_pulse_params":
        assert r.call(r.d.read_actual_pulse_params)[1] == case["result"]
    else:
        assert r.call(getattr(r.d, case["call"]))[1] is case["result"]
    assert norm(r.sent(n0)) == norm(case["sent"])
    mine = [[e.kind, e.cmd, e.detail if e.kind == "command_failed" else None] for e in r.events
            if e.kind in ("command_confirmed", "command_failed")]
    theirs = [e for e in case["events"] if e[0] in ("command_confirmed", "command_failed")]
    assert mine == theirs
    r.done()


# ───────── 1초 읽기 ─────────
def test_reads_only_while_on_then_each_tick_with_ae_af_alternate():
    r = Rig()
    rows = r.rows(2)
    assert all(x["ch1.dcpulse.q"] == "ok" and x["ch1.dcpulse.p_w"] is None for x in rows)      # 출력 OFF — 읽지 않음
    assert len(r.sim.received) == 0
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    rows = r.rows(40)
    on = rows[1:]
    assert all(x["ch1.dcpulse.p_w.q"] == "ok" and x["ch1.dcpulse.p_w"] == 100.0 for x in on)
    assert on[0]["ch1.dcpulse.i_a"] == pytest.approx(0.25) and on[0]["ch1.dcpulse.v_v"] == 400.0
    late = [x for x in rows if x["ch1.dcpulse.arc_soft_rate.q"] == "ok" or x["ch1.dcpulse.arc_hard_rate.q"] == "ok"]
    assert late                                                                                 # 점화 창 밖: AE/AF 읽음
    early = rows[2:20]                                                                          # 점화 창(30 s) 안: AE/AF 안 읽음
    assert all(x["ch1.dcpulse.arc_soft_rate"] is None for x in early)
    assert any(x["ch1.dcpulse.arc_soft_rate.q"] == "stale" and x["ch1.dcpulse.arc_hard_rate.q"] == "ok" for x in rows[-6:])
    assert rows[-1]["ch1.dcpulse.out_on"] is True and rows[-1]["ch1.dcpulse.set_w"] == 100.0
    assert rows[-1]["ch1.dcpulse.freq_khz"] == 50 and rows[-1]["ch1.dcpulse.off_raw"] == 50
    r.done()


def test_low_current_auto_stop_at_v1_cadence():
    r = Rig(lit=False)                                              # 점화 안 됨 → P = 0, I = 0
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    t_on = r.d._arc_run_start_ts
    r.rows(60)
    warns = [e for e in r.ev("status") if "저전류 감지" in e.detail]
    stop = [e for e in r.ev("command_failed") if e.cmd == "AUTO_STOP"]
    assert len(warns) == 3 and len(stop) == 1 and stop[0].detail.startswith("low_current: I <= 0.050A (3회 연속)")
    assert r.d.output_off_unconfirmed is False and r.sim.out is False and r.d.polling is False
    assert any(e.cmd == "OUTPUT_OFF" for e in r.ev("command_confirmed"))
    # 점화 창 30 s 안에서는 세지 않고, 창 밖은 5 s 마다(1초 읽기로 세면 5배 빨라짐 — v1 금지)
    assert r.c.monotonic() - t_on > 30.0 + 2 * 5.0
    r.done()


def test_setpoint_deviation_auto_stop():
    r = Rig()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    r.sim.ref_raw = 5                                               # 장비 출력이 50 W 로(설정 100 W, 허용 ±15 W)
    r.rows(60)
    stop = [e for e in r.ev("command_failed") if e.cmd == "AUTO_STOP"]
    assert len(stop) == 1 and stop[0].detail == "target_failed"
    r.done()


def test_overcurrent_during_activation_blocks_start():
    r = Rig(cfg={"DC_OVERCURRENT_ENABLE": True})
    r.sim.i_override_a = 2.71                                       # 2026-09-19 CH2 사례: 190 W 설정에 2.71 A
    reason, ok = r.start(190.0, freq=50, off_time_us=5.0)
    assert ok is False
    stop = [e for e in r.ev("command_failed") if e.cmd == "AUTO_STOP"]
    assert stop and stop[0].detail.startswith("overcurrent: I=2.710A > 1.000A")
    assert r.sim.out is False and r.d.polling is False
    r.done()


def test_overcurrent_disabled_by_default_v1_deployed():
    r = Rig()
    r.sim.i_override_a = 2.71
    assert r.start(190.0, freq=50, off_time_us=5.0) == (None, True)
    assert any("인터락 OFF" in e.detail for e in r.ev("status"))
    r.done()


def test_arc_alert_once_and_counts():
    r = Rig()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    r.sim.arc_rate_soft = 50.0
    r.rows(50)
    alerts = r.ev("arc_threshold_reached")
    assert len(alerts) == 1 and alerts[0].data["by_rate"] and alerts[0].data["calc_rate"] >= 20
    assert r.d.arc_summary["ign_closed"] and r.d.arc_summary["ran"]
    assert r.call(r.d.output_off) == (None, True)
    s, h = r.d.arc_counts
    assert s > 1000 and h == 0 and r.d.arc_summary["final_read"]
    r.done()


# ───────── 쓰기 실패·복구·고장 ─────────
def test_write_no_response_recovers_with_fault_check():
    r = Rig()
    r.sim.add_fault("no_response", 0.75, 0.85)                       # 첫 MASTER_ONOFF(연결 + 0.8 s) 하나만 무응답
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    st = [e.detail for e in r.ev("status")]
    assert any("[TIMEOUT] MASTER_ONOFF[1/5]" in s for s in st)
    assert any("실패 후 fault=0 → 1초 대기 후 재전송" in s for s in st)
    assert sum(1 for f in r.sent() if f == "02 7b 00 03 03 83") == 2
    r.done()


def test_write_err_retries_after_fault_check():
    r = Rig()
    r.sim.write_err = {P.REGULATION: 1}                             # 0x81 에 ERR 1회
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    assert any("[REG_POWER] 실패 후 fault=0 → 1초 대기 후 재전송" in e.detail for e in r.ev("status"))
    assert sum(1 for f in r.sent() if f == "02 81 00 03 03 89") == 2
    r.done()


def test_stop_during_start_cancels_and_guards_writes():
    r = Rig()
    box = []
    r.d.prepare_and_start(100.0, freq=50, off_time_us=5.0, on_done=lambda *x: box.append(x))
    r.c.wait_until(r.c.monotonic() + 1.6)                           # 마스터 쓰는 중
    assert r.call(r.d.output_off) == (None, True)
    r.c.wait_until(r.c.monotonic() + 3.0)
    assert box and box[0][1] is False
    after = [bytes.fromhex(f) for f in r.sent()]
    i_off = max(i for i, b in enumerate(after) if b[1] == 0x80)
    assert all(b[1] not in (0x81, 0x83, 0x66, 0x67) or b[1] == 0x80 for b in after[i_off + 1:])
    assert not any(b[1] == 0x80 and b[3] == 1 for b in after)        # OUTPUT_ON 은 끝내 나가지 않음
    r.done()


def test_off_unused_single_frame_no_reply_still_completes():
    r = Rig()
    r.sim.add_fault("no_response", 0.0, 100.0)
    t0 = r.c.monotonic()
    assert r.call(r.d.output_off_unused) == (None, True)
    assert r.c.monotonic() - t0 == pytest.approx(3.0, abs=0.01)
    assert r.sent() == ["02 80 00 02 03 87"]
    assert any("켠 적 없음" in e.detail and "응답 없음" in e.detail for e in r.ev("status"))
    r.done()


def test_off_unconfirmed_after_all_attempts():
    r = Rig()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    r.sim.off_err_n = 99
    reason, ok = r.call(r.d.output_off)
    assert ok is False and reason == "DCP_OFF_UNCONFIRMED" and r.d.output_off_unconfirmed and r.d.off_needs_confirm()
    fail = [e for e in r.ev("command_failed") if e.cmd == "OUTPUT_OFF"]
    assert fail and "출력 상태 미확인" in fail[-1].detail
    r.done()


def test_disconnect_and_reconnect_during_polling():
    r = Rig()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t + 2.2, t + 5.0)
    rows = r.rows(12)
    assert any(x["ch1.dcpulse.q"] == "fail" for x in rows)
    assert rows[-1]["ch1.dcpulse.q"] == "ok" and rows[-1]["ch1.dcpulse.p_w.q"] == "ok"
    r.done()


def test_garbage_reply_logged_as_chkfail():
    r = Rig()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    t = r.c.monotonic()
    r.sim.add_fault("garbage", t, t + 1.5)
    r.rows(3)
    assert any(e.detail.startswith("[CHKFAIL]") for e in r.ev("status"))
    r.done()


def test_bench_rejects_all_writes():
    r = Rig(writes=False)
    assert r.start(100.0) == ("BENCH_READ_ONLY", False)
    assert r.call(r.d.output_off) == ("BENCH_READ_ONLY", False)
    assert r.call(r.d.output_off_unused) == ("BENCH_READ_ONLY", False)
    assert r.sent() == []
    r.done()


def test_budget_and_blocks():
    assert D.BUDGET["min_gap_ms"] == 150 and D.BUDGET["fits_1s"]
    cols = D.blocks("ch2")[0].columns()
    assert "ch2.dcpulse.p_w.q" in cols and "ch2.dcpulse.set_w" in cols


def test_cleanup_turns_off_and_keeps_final_arc():
    r = Rig()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    r.sim.arc_rate_hard = 10.0
    r.rows(8)
    box = []
    r.d.cleanup(on_done=lambda u: (box.append(u), r.c.notify()))
    r.c.wait_until(None, lambda: bool(box))
    assert box == [False] and r.sim.out is False
    assert r.d._hard_arc_total == 0 and r.d.arc_counts[1] > 0          # v1 도 정리 때 누적만 0 — OFF 직전 확정값은 남는다
    assert any(e.cmd == "OUTPUT_OFF" for e in r.ev("command_confirmed"))
    r.done()


def test_cleanup_wait_limit_marks_unconfirmed():
    r = Rig()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    r.sim.add_fault("no_response", r.c.monotonic(), r.c.monotonic() + 1000.0)
    t0 = r.c.monotonic()
    box = []
    r.d.cleanup(on_done=lambda u: (box.append(u), r.c.notify()))
    r.c.wait_until(None, lambda: bool(box))
    assert box == [True] and r.c.monotonic() - t0 == pytest.approx(8.0, abs=0.2)
    r.done()


def test_unused_v1_setting_apis_bytes():
    r = Rig()
    assert r.call(r.d.set_limits, 500.0, 2.0, 600.0) == (None, True)
    assert r.call(r.d.set_ramp_and_ignition, 100, 800.0) == (None, True)
    want = [P.pack_write(0x0C, 50, 2), P.pack_write(0x0D, 20, 2), P.pack_write(0x0E, 600, 2),
            P.pack_write(0x0F, 100, 2), P.pack_write(0x10, 800, 2)]
    assert r.sent() == [w.hex(" ") for w in want]
    r.done()

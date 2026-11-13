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
    # v1 값으로 비교: 명령 간격 1500 ms·되읽기 불일치는 경고만(새 기본값은 아래 전용 시험 — 사용자 결정 2026-11-02 D50)
    r = Rig(cfg={"CMD_GAP_MS": 1500, "RFPULSE_READBACK_ENABLED": False})
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
    """설정 키 CMD_GAP_MS 를 v1 값(1500)으로 두면 명령 사이가 v1 처럼 벌어진다(기본값은 0 — 아래 시험)."""
    r = Rig(control=6, cfg={"CMD_GAP_MS": 1500})
    assert r.call(r.d.start_pulse_process, 100.0, 1000, 50) == (None, True)
    ts = r.times()
    assert all(b - a >= 1.5 for a, b in zip(ts, ts[1:]))
    r.done()


def test_poll_reads_all_three_every_tick_no_stale():
    """사용자 결정 2026-11-02 D50 — 매 tick 162 → 165 → 166 전부, 응답을 받는 대로 다음 질의(CMD_GAP_MS 기본 0), stale 없음."""
    assert CFG["CMD_GAP_MS"] == 0 and R.BUDGET["fits_1s"] is True and R.BUDGET["queries_per_tick"] == 3
    r = Rig()
    assert r.call(r.d.start_pulse_process, 100.0) == (None, True)
    n0 = len(r.sim.received)
    rows = r.rows(20)
    cmds = [bytes.fromhex(f)[1] for f in r.sent(n0)]
    assert cmds[:6] == [162, 165, 166, 162, 165, 166]                               # 순서는 v1 과 같다
    assert len(cmds) >= 3 * 19
    ts = r.times(n0)
    assert ts[1] - ts[0] < 0.2 and ts[2] - ts[1] < 0.2                              # 응답 받는 대로 다음 질의
    assert all(x["ch1.rfpulse.q"] == "ok" for x in rows)
    for k in ("fwd_w", "ref_w", "out_on"):
        assert all(x[f"ch1.rfpulse.{k}.q"] == "ok" for x in rows[1:]), k
    assert rows[-1]["ch1.rfpulse.fwd_w"] == pytest.approx(100.0, abs=1) and rows[-1]["ch1.rfpulse.set_w"] == 100.0
    assert rows[-1]["ch1.rfpulse.ref_w"] == pytest.approx(60.0, abs=2)              # 점화 전(가스 없음) 반사 큼
    r.done()


def test_readback_of_freq_duty_rewrites_then_fails_with_reason():
    r = Rig(control=6, cfg={"RFPULSE_REWRITE_MAX": 2})
    r.sim.readback_offset = 5                                                     # 장비가 값을 못 바꾼 것처럼 틀리게 되읽힘
    reason, ok = r.call(r.d.start_pulse_process, 100.0, 1000, 50)
    assert ok is False
    fails = [e for e in r.ev("command_failed") if e.cmd == "START_SEQUENCE"]
    assert fails and fails[-1].reason == "VERIFY_MISMATCH" and "다시 쓰기 2회" in fails[-1].detail
    cmds = [bytes.fromhex(f)[1] for f in r.sent()]
    assert cmds.count(96) == 3 and cmds.count(93) == 3                            # DUTY·FREQ: 쓰기 1 + 다시 쓰기 2
    assert 2 not in cmds                                                          # RF ON 은 나가지 않음
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
    r = Rig(control=6, cfg={"CMD_GAP_MS": 1500})                                  # v1 간격 — 시작 순서가 5 s 보다 오래 걸려야 중간에 멈춘다
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
    assert R.BUDGET["min_gap_ms"] == 0 and R.BUDGET["fits_1s"] is True and R.BUDGET["queries_per_tick"] == 3


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


def test_setpoint_and_pulsing_are_read_back_after_write_164_177():
    """2026-11-09 E2 — SET_SETPOINT(8)→164·SET_PULSING(27)→177 쓰기 뒤 되읽기. 맞으면 RF ON 까지 간다."""
    r = Rig(control=6)
    reason, ok = r.call(r.d.start_pulse_process, 100.0, 1000, 50)
    assert ok is True, reason
    cmds = [bytes.fromhex(f)[1] for f in r.sent()]
    assert 164 in cmds and 177 in cmds and cmds.index(164) > cmds.index(8) and cmds.index(177) > cmds.index(27)
    assert cmds.index(2) > cmds.index(177)                                          # RF ON 은 되읽기 확인 뒤
    r.done()


def test_setpoint_readback_mismatch_rewrites_then_verify_mismatch_and_no_rf_on():
    r = Rig(control=6, cfg={"RFPULSE_REWRITE_MAX": 2})
    r.sim.setpoint_readback_offset = 3
    reason, ok = r.call(r.d.start_pulse_process, 100.0, 1000, 50)
    assert ok is False
    fails = [e for e in r.ev("command_failed") if e.reason == "VERIFY_MISMATCH"]
    assert fails and "SETP" in fails[-1].detail and "다시 쓰기 2회" in fails[-1].detail
    cmds = [bytes.fromhex(f)[1] for f in r.sent()]
    assert cmds.count(8) == 3 and 2 not in cmds                                     # 쓰기 1 + 다시 쓰기 2, RF ON 안 나감
    r.done()


def test_run_setpoint_change_is_verified_too_and_readback_can_be_turned_off():
    r = Rig(control=6)
    reason, ok = r.call(r.d.start_pulse_process, 100.0, 1000, 50)
    assert ok is True
    n0 = len(r.sim.received)
    reason, ok = r.call(r.d.set_reference_power, 120.0)
    assert ok is True and [bytes.fromhex(f)[1] for f in r.sent(n0)][:2] == [8, 164]
    r.d.cfg["RFPULSE_READBACK_ENABLED"] = False
    n1 = len(r.sim.received)
    r.call(r.d.set_reference_power, 130.0)
    assert 164 not in [bytes.fromhex(f)[1] for f in r.sent(n1)]                       # 끄면 v1 처럼 CSR 만
    r.done()


def test_mode_readback_154_by_default_and_off_with_zero_155_is_not_regulation_mode():
    r = Rig(control=6)                                                              # 기본 154(Table 4-16 조절 모드 보고)
    reason, ok = r.call(r.d.start_pulse_process, 100.0, 1000, 50)
    assert ok is True
    cmds = [bytes.fromhex(f)[1] for f in r.sent()]
    assert 154 in cmds and cmds.count(155) == 1 and cmds.index(3) < cmds.index(154)   # 모드 쓰기(3) 뒤 154, 155 는 시작 때 HOST 확인 한 번뿐
    r.done()
    r2 = Rig(control=6, cfg={"RFPULSE_Q_MODE": 0})                                  # 0 = 끔
    r2.call(r2.d.start_pulse_process, 100.0, 1000, 50)
    assert 154 not in [bytes.fromhex(f)[1] for f in r2.sent()]
    r2.done()


def test_mode_readback_mismatch_retries_then_verify_mismatch():
    r = Rig(control=6)
    r.sim.mode_query_value = 8                                                      # 장비가 External 로 보고(쓴 6 FWD 와 다름)
    reason, ok = r.call(r.d.start_pulse_process, 100.0, 1000, 50)
    assert ok is False
    cmds = [bytes.fromhex(f)[1] for f in r.sent()]
    assert cmds.count(154) == 4 and cmds.count(3) == 4                              # 처음 + 다시 쓰기 3회(RFPULSE_REWRITE_MAX)
    assert any(getattr(e, "reason", "") == "VERIFY_MISMATCH" for e in r.events if e.kind == "command_failed")
    r.done()


def test_mode_readback_comm_failure_skips_verification():
    r = Rig(control=6)
    r.sim.silent_cmds.add(154)                                                      # 154 에 응답 없음 → 통신 실패로 검증 생략
    reason, ok = r.call(r.d.start_pulse_process, 100.0, 1000, 50)
    assert ok is True
    r.done()

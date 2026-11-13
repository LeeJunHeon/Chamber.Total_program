"""챔버12 MFC 드라이버 + 시뮬레이터 시험(가상 시계·SimTransport — 실제 포트·장비망 접속 없음).

바이트 동일: v2/tests/golden/dev_mfc.json(v2/tools/v1golden_mfc.py 가 v1 device/mfc.py 를 127.0.0.1 가짜 장비에 붙여 실제로 실행해 받은 바이트)과
같은 명령·인자에서 v2 가 같은 바이트를 같은 순서로 보내는지, 확인/실패 이벤트(v1 문구)가 같은지 본다.
"""
import json
from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop
from v2.common.transport import SimTransport
from v2.ch12.drivers import mfc as M
from v2.ch12.params import mfc as P
from v2.ch12.sim.chamber import Chamber
from v2.ch12.sim.mfc_sim import MfcSim

GOLDEN = json.loads((Path(__file__).parent / "golden" / "dev_mfc.json").read_text(encoding="utf-8"))
CFG = defaults(P.PARAMS)
SCALE = {"ch1": (1.0, 1.0, 1.0), "ch2": (1.0, 10.0, 2.0)}


class Rig:
    def __init__(self, chamber="ch1", writes=True, cfg=None, flow_tau_s=0.0, start=1.5, readback=False):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.ch = Chamber(chamber)
        self.sim = MfcSim(self.ch, scale=SCALE[chamber], flow_tau_s=flow_tau_s)
        # 되읽기는 기본 끔 — v1 바이트·순서 비교 시험은 v1 그대로 보내는 명령만 본다(되읽기는 아래 전용 시험)
        self.cfg = dict(CFG, MFC_READBACK_ENABLED=readback, **(cfg or {}))
        self.events, self.wev = [], []
        self.w = M.make_worker(self.c, SimTransport(self.c, self.sim), self.cfg, chamber, writes, on_event=self.wev.append)
        self.d = M.MfcDriver(self.c, self.w, self.cfg, writes, on_event=self.events.append, chamber=chamber)
        self.w.start()
        self.c.wait_until(start)

    def call(self, fn, *a, **kw):
        box = []
        fn(*a, on_done=lambda k, r, t: (box.append((k, r, t)), self.c.notify()), **kw)
        self.c.wait_until(None, lambda: bool(box))
        return box[0]

    def sent(self, t0=0.0):
        return [d.decode("ascii").strip() for t, d in self.sim.received if t >= t0]

    def kinds(self, kind):
        return [e for e in self.events if e.kind == kind]

    def bus(self):
        b = M.blocks(self.d.chamber)
        return SnapshotBus(self.c, b, [Source("mfc", self.w, self.d.source_read, (b[0].name,),
                                              M.source_deadline_ms(self.cfg))], 900)

    def run(self, n, bus=None):
        rows = []
        TickLoop(self.c, bus or self.bus(), [rows.append]).run(n)
        return rows

    def done(self):
        self.w.stop()


def test_parsers_and_command_table():
    assert M.parse_r60("Q0+10.0+0.0+2.3+0.0") == [10.0, 0.0, 2.3, 0.0] and M.parse_r60("P+0.20") is None
    assert M.parse_pressure("P+0.20") == 0.2 and M.parse_pressure("S4+000.30") == 0.3 and M.parse_pressure("") is None
    assert M.mk_cmd("FLOW_SET", channel=2, value=0.07 * 10.0) == "Q2 0.7000000000000001"      # str(float) 그대로(D183)
    assert M.mk_cmd("SP4_SET", value=round(1.234 * 0.1, 3)) == "S4 0.123"
    assert [M.mk_cmd(k, channel=1) for k in ("FLOW_ON", "FLOW_OFF", "MFC_ZEROING")] == ["L11", "L10", "L51"]


@pytest.mark.parametrize("chamber", ["ch1", "ch2"])
def test_golden_bytes_and_events_match_v1(chamber):
    """v1 을 실제로 돌려 받은 바이트·이벤트와 같은지 — 같은 순서로 같은 명령(한 인스턴스에 이어서, v1 골든과 같게)."""
    cases = [c for c in GOLDEN["cases"] if c["chamber"] == chamber]
    assert cases
    r = Rig(chamber)
    owner = f"chamber{chamber[-1]}"
    for case in cases:
        st = case["setup"]
        if "pressure_hw" in st:
            r.ch.pressure_override = r.ch.pressure = st["pressure_hw"] / 0.1 / 1000.0
        for n, v in (st.get("sp") or {}).items():
            r.sim.sp[int(n) - 1] = float(v)
        t0 = r.c.monotonic()
        n_ev = len(r.events)
        if case["cmd"] == "POLL_CYCLE":
            r.d.acquire_poll(owner)
            r.run(3)
            assert r.d.release_poll(owner) == 0
            got = r.sent(t0)[:2]
        else:
            res = r.call(r.d.command, case["cmd"], case["args"], owner)
            r.c.wait_until(r.c.monotonic() + 0.5)
            got = r.sent(t0)
            assert res[0] in (case["cmd"], "READ_FLOW", "READ_PRESSURE")
        assert got == case["sent_text"], case["name"]
        assert [bytes.fromhex(h) for h in case["sent"]] == [(x + "\r").encode() for x in got]
        ev = [{"kind": e.kind, "cmd": e.cmd, "reason": e.detail or None, "channel": e.channel}
              for e in r.events[n_ev:] if e.kind in ("command_confirmed", "command_failed")]
        assert ev == case["events"], case["name"]
    r.done()


def test_timing_matches_v1_gaps():
    """v1 값(MFC_GAP_MS 1000)으로 둔 경우: FLOW_SET 확정은 보낸 즉시, FLOW_ON 은 등록 + max(MFC_DELAY_MS,200) 와 보낸 시각 중 늦은 쪽,
    명령 사이는 gap 1 s + 흡수 80 ms. (기본 간격은 50 ms — 사용자 결정 2026-11-02 D36, 아래 기본 간격 시험)"""
    r = Rig("ch1", cfg={"MFC_GAP_MS": 1000})
    t0 = r.c.monotonic()
    r.call(r.d.set_flow, 1, 10.0)
    t1 = r.c.monotonic()
    r.call(r.d.flow_on, 1)
    times = [t for t, d in r.sim.received if t >= t0]
    assert times[1] - times[0] == pytest.approx(1.08, abs=0.01)           # Q1 → L11: gap 1000 + drain 80
    assert times[2] - times[1] == pytest.approx(1.08, abs=0.01)           # L11 → R60(안정화)
    assert t1 - t0 < 0.01
    t2 = r.c.monotonic()
    r.call(r.d.valve_open)
    assert r.c.monotonic() - t2 == pytest.approx(5.0, abs=0.01)            # 등록 + MFC_DELAY_MS_VALVE
    r.done()


def test_default_gap_is_50ms_and_commands_follow_it():
    r = Rig("ch1")
    assert CFG["MFC_GAP_MS"] == 50
    t0 = r.c.monotonic()
    r.call(r.d.set_flow, 1, 10.0)
    r.call(r.d.flow_on, 1)
    times = [t for t, d in r.sim.received if t >= t0]
    assert times[1] - times[0] == pytest.approx(0.13, abs=0.01)           # Q1 → L11: gap 50 + drain 80
    r.done()


def test_one_second_reads_both_every_tick_no_stale():
    """사용자 결정 2026-11-02 D36 — 매 tick R60·R5 를 모두 읽는다(번갈아 읽기·stale 없음)."""
    r = Rig("ch2")
    r.call(r.d.set_flow, 2, 1.0)
    r.call(r.d.flow_on, 2)
    r.c.wait_until(r.c.monotonic() + 2.0)
    t0 = r.c.monotonic()
    rows = r.run(14)
    q = [(x["ch2.mfc.o2_sccm.q"], x["ch2.mfc.pressure_mtorr.q"]) for x in rows[2:]]
    assert all(row["ch2.mfc.q"] == "ok" for row in rows[2:])
    assert all(x == ("ok", "ok") for x in q)                                # 둘 다 매 tick 새 값
    flows = [x["ch2.mfc.o2_sccm"] for x in rows[2:]]
    assert flows and all(v == pytest.approx(1.0) for v in flows)            # %FS 10 ÷ 스케일 10
    assert rows[-1]["ch2.mfc.o2_set_sccm"] == pytest.approx(1.0) and rows[-1]["ch2.mfc.o2_on"] is True
    seq = r.sent(t0)
    assert seq.count("R60") >= 13 and seq.count("R5") >= 13
    gaps = [b - a for (a, x), (b, y) in zip(r.sim.received, list(r.sim.received)[1:])]
    assert min(gaps) >= 0.05 - 1e-9                                         # 명령 간격 MFC_GAP_MS(50 ms)를 지킴
    r.done()


def test_tick_reads_yield_while_control_runs():
    """v1 간격(1000 ms)에서는 제어 동작 중·직후 1초 읽기가 끼어들지 않는다 — FLOW_SET → FLOW_ON → 안정화 R60 순서가 v1 과 같다."""
    r = Rig("ch1", cfg={"MFC_GAP_MS": 1000})
    bus = r.bus()
    out = []
    loop = TickLoop(r.c, bus, [out.append])
    r.c.start_thread(lambda: loop.run(30), "ticks")
    r.c.wait_until(r.c.monotonic() + 5.3)
    t0 = r.c.monotonic()
    r.call(r.d.set_flow, 1, 10.0)
    r.call(r.d.flow_on, 1)
    seq = r.sent(t0)
    i = seq.index("Q1 10.0")
    assert seq[i:i + 3] == ["Q1 10.0", "L11", "R60"]
    r.c.wait_until(None, lambda: len(out) >= 30)
    r.done()


def test_readback_confirms_each_write_kind():
    """쓰기 → 되읽기 확인(FLOW_SET R65·FLOW_ON/OFF R69·VALVE R6·SPn_SET R{n}·SPn_ON R7) — 한 번에 맞으면 다시 쓰지 않는다."""
    r = Rig("ch1", readback=True)
    t0 = r.c.monotonic()
    assert r.call(r.d.set_flow, 1, 10.0)[1] is None
    assert r.sent(t0) == ["Q1 10.0", "R65"]
    t1 = r.c.monotonic()
    assert r.call(r.d.flow_on, 1)[0] == "FLOW_ON"
    assert r.sent(t1)[:2] == ["L11", "R69"]
    t2 = r.c.monotonic()
    assert r.call(r.d.flow_off, 1)[1] is None
    assert r.sent(t2) == ["L10", "R69"]
    t3 = r.c.monotonic()
    assert r.call(r.d.valve_close)[1] is None
    assert r.sent(t3) == ["C", "R6"]
    t4 = r.c.monotonic()
    assert r.call(r.d.sp_set, 1, 2.5)[1] is None
    assert r.sent(t4) == ["S1 0.25", "R1"]
    t5 = r.c.monotonic()
    assert r.call(r.d.sp_on, 1)[1] is None
    assert r.sent(t5) == ["D1", "R7"]
    t6 = r.c.monotonic()
    assert r.call(r.d.ps_zeroing)[1] is None
    assert r.sent(t6) == ["Z1"]                                             # 되읽을 질의 없음 — v1 과 같게
    assert any("되읽기 확인" in e.detail for e in r.kinds("status"))
    r.done()


def test_readback_mismatch_rewrites_then_fails_with_reason_code():
    r = Rig("ch1", readback=True, cfg={"MFC_REWRITE_MAX": 2, "MFC_REWRITE_GAP_MS": 100})
    r.sim.ignore_writes = {"Q1"}                                            # 장비가 값을 받지 않음
    t0 = r.c.monotonic()
    res = r.call(r.d.set_flow, 1, 10.0)
    assert res[0] == "FLOW_SET" and res[1] == "VERIFY_MISMATCH"
    assert res[2].startswith("FLOW_SET ch1 되읽기 확인 실패 — 기대 10.00 %FS")
    assert r.sent(t0) == ["Q1 10.0", "R65", "Q1 10.0", "R65", "Q1 10.0", "R65"]   # 쓰기 1 + 다시 쓰기 2
    assert r.kinds("command_failed")[-1].reason == "VERIFY_MISMATCH"
    assert sum("되읽기 불일치" in e.detail for e in r.kinds("status")) == 3
    assert r.d.last_setpoints[1] == 0.0                                     # 확정하지 않음
    r.done()


def test_readback_recovers_when_first_write_is_lost():
    r = Rig("ch1", readback=True)
    r.sim.ignore_writes = {"Q1"}
    box = []
    r.d.set_flow(1, 10.0, on_done=lambda *a: (box.append(a), r.c.notify()))
    r.c.wait_until(r.c.monotonic() + 0.2)
    r.sim.ignore_writes = set()                                             # 두 번째 쓰기부터 받음
    r.c.wait_until(None, lambda: bool(box))
    assert box[0][1] is None and r.sent().count("Q1 10.0") == 2
    assert r.d.last_setpoints[1] == 10.0
    r.done()


def test_readback_disabled_confirms_without_query():
    r = Rig("ch1")
    t0 = r.c.monotonic()
    assert r.call(r.d.set_flow, 1, 10.0)[1] is None
    assert r.sent(t0) == ["Q1 10.0"]
    r.done()


def test_poll_ownership_and_mask():
    r = Rig("ch1")
    r.call(r.d.set_flow, 1, 10.0)
    r.call(r.d.flow_on, 1)
    r.d.acquire_poll("chamber1")
    r.d.acquire_poll("pc1")
    rows = r.run(10)
    flows = [e for e in r.events if e.kind == "flow"]
    press = [e for e in r.events if e.kind == "pressure"]
    assert len(flows) >= 6 and len(press) >= 2                                # 3 s 주기 — Ar·O2·N2 각 1 이벤트
    assert r.d.release_poll("chamber1") == 1 and r.d.polling                  # 남은 사용자 있음 → 유지
    assert any("폴링 유지 — 사용 중: pc1" in e.detail for e in r.kinds("status"))
    r.d.set_poll_mask(gas=False, pressure=True)
    n_f, n_p = len(r.kinds("flow")), len(r.kinds("pressure"))
    r.run(8, r.bus())
    assert len(r.kinds("flow")) == n_f and len(r.kinds("pressure")) > n_p     # pressure-only
    assert r.d.release_poll("pc1") == 0 and not r.d.polling
    n_p = len(r.kinds("pressure"))
    r.run(5, r.bus())
    assert len(r.kinds("pressure")) == n_p and rows[-1]["ch1.mfc.q"] == "ok"  # 폴링 멈춤(1초 읽기는 계속)
    r.done()


def test_flow_deviation_warning_only():
    r = Rig("ch1")
    r.call(r.d.set_flow, 1, 10.0)
    r.call(r.d.flow_on, 1)
    r.sim.set_pct[0] = 5.0                                                    # 장비 유량이 목표에서 벗어남
    r.d.acquire_poll("chamber1")
    r.run(14)
    warn = [e for e in r.kinds("status") if "GAS 불안정" in e.detail]
    assert warn and warn[0].detail == "Ch1 GAS 불안정! (목표: 10.00, 현재: 5.00)"
    assert not [e for e in r.kinds("command_failed")]                        # 경고만(공정 계속)
    r.done()


def test_stabilization_timeout_180_attempts():
    r = Rig("ch1")
    r.sim.ignore_writes = {"L1"}                                              # ON 이 안 됨 → 유량 0
    r.call(r.d.set_flow, 1, 10.0)
    res = r.call(r.d.flow_on, 1)
    assert res == ("FLOW_ON", "STAB_TIMEOUT", "GAS 안정화 시간 초과")
    assert r.sent().count("R60") == 180
    assert r.kinds("command_failed")[-1].detail == "GAS 안정화 시간 초과"
    r.done()


def test_send_confirm_30s_withdraw_on_but_keep_off():
    r = Rig("ch1")
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t, t + 70.0)
    r.d.set_flow(1, 10.0)
    r.c.wait_until(t + 0.5)
    res_on = r.call(r.d.flow_on, 1)
    assert res_on == ("FLOW_ON", "SEND_TIMEOUT", "ch1 FLOW_ON 30초 안에 전송되지 못해 대기열에서 철회")
    res_off = r.call(r.d.flow_off, 2)
    assert res_off == ("FLOW_OFF", "SEND_TIMEOUT", "ch2 FLOW_OFF 30초 안에 전송 확인 못 함(명령은 대기열에 남아 순서대로 전송)")
    r.c.wait_until(t + 75.0)
    seen = r.sent()
    assert "L20" in seen and "L11" not in seen                                # OFF 는 남아 나중에 보냄, ON 은 철회
    assert any(e.detail.startswith("[WITHDRAW] [FLOW_ON ch1]") for e in r.kinds("status"))
    r.done()


def test_one_late_reply_reconnects_after_2s_window():
    r = Rig("ch1", start=5.0)
    t = r.c.monotonic()
    r.sim.add_fault("no_response", t, t + 2.5)
    line = r.call(r.d.read_pressure)
    assert [e.kind for e in r.wev].count("disconnected") == 1                  # 응답 1회 늦어도 끊고 재연결(D182)
    assert "connected" in [e.kind for e in r.wev[1:]]
    assert line[1] is None and r.kinds("pressure")                            # 재연결 뒤 다시 보낸 R5 로 값
    r.done()
    r = Rig("ch1", start=0.0, cfg={"MFC_TIMEOUT": 400})                      # 연결 직후 쉼 0.8 + 한도 0.4 < 2 s
    r.sim.add_fault("no_response", 0.0, 1.3)
    r.c.wait_until(0.05)
    r.call(r.d.read_pressure)
    assert "disconnected" not in [e.kind for e in r.wev]                      # 연결 뒤 2 s 안이면 연결 유지 재시도
    assert r.sent().count("R5") >= 2
    r.done()


def test_bench_read_only():
    r = Rig("ch1", writes=False)
    out = [r.call(r.d.set_flow, 1, 10.0), r.call(r.d.flow_on, 1), r.call(r.d.valve_open),
           r.call(r.d.sp_set, 1, 2.5), r.call(r.d.ps_zeroing)]
    assert [x[1] for x in out] == ["BENCH_READ_ONLY"] * 5
    rows = r.run(4)
    assert r.sim.set_commands_seen() == [] and all(x["ch1.mfc.q"] == "ok" for x in rows[1:])
    assert r.call(r.d.read_pressure)[1] is None
    r.done()


def test_echo_and_prefix_noise_are_skipped():
    r = Rig("ch1")
    r.sim.echo = True
    r.sim.reply_prefix_noise = "Q9+1.0"
    r.call(r.d.set_flow, 1, 10.0)
    r.call(r.d.flow_on, 1)
    r.call(r.d.read_pressure)
    assert float(r.kinds("pressure")[-1].text) == pytest.approx(r.sim.pressure_hw() / 0.1, abs=0.2)   # 'Q9…'·에코는 건너뜀
    assert r.kinds("command_confirmed")[-1].cmd == "FLOW_ON"
    r.done()


def test_garbage_reply_wait_pressure_fail_streak():
    r = Rig("ch1", start=5.0)
    t = r.c.monotonic()
    r.sim.add_fault("garbage", t, t + 1000.0)
    res = r.call(r.d.wait_pressure, 2.0)
    assert res[1] == "PRESSURE_NOT_STABLE" and res[2].startswith("압력 안정화 실패: target=2")
    assert any("READ_PRESSURE 연속 3회 실패" in e.detail for e in r.kinds("status"))
    r.done()


def test_process_finished_purges_and_cancels_stab():
    r = Rig("ch1")
    r.sim.ignore_writes = {"L1"}
    r.call(r.d.set_flow, 1, 10.0)
    box = []
    r.d.flow_on(1, owner="chamber1", on_done=lambda *a: box.append(a))
    r.c.wait_until(r.c.monotonic() + 6.0)
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t, t + 20.0)
    r.d.valve_close(owner="chamber1", on_done=lambda *a: box.append(a))
    r.c.wait_until(t + 2.0)
    r.d.on_process_finished(False)
    r.c.wait_until(None, lambda: len(box) >= 2)
    assert ("FLOW_ON", "STAB_CANCELLED", "ch1 안정화 중단 (정리(process finished (fail))로 안정화 중단)") in box
    assert ("VALVE_CLOSE", "PURGED", "[VALVE_CLOSE] 전송 전에 대기열에서 폐기됨(또는 전송 실패)") in box
    assert r.d.last_setpoints == {1: 0.0, 2: 0.0, 3: 0.0} and not r.d.flow_on_flags[1]
    r.c.wait_until(t + 25.0)
    assert "C" not in r.sent(t)
    r.done()


def test_release_owner_keeps_queue_and_other_channels():
    r = Rig("ch2")
    r.call(r.d.set_flow, 1, 5.0, owner="chamber2")
    r.call(r.d.set_flow, 2, 1.0, owner="pc2")
    r.d.release_owner("pc2", reason="PC 종료")
    r.c.wait_until(r.c.monotonic() + 0.1)
    assert r.d.last_setpoints[2] == 0.0 and r.d.last_setpoints[1] == 5.0
    assert any(e.detail.startswith("[pc2] 사용 종료 — 다른 사용자가 있어 대기열 유지") for e in r.kinds("status"))
    r.done()


def test_cleanup_marks_shutdown():
    r = Rig("ch1")
    r.d.cleanup()
    res = r.call(r.d.flow_off, 1)
    assert res == ("FLOW_OFF", "SHUTDOWN", "MFC not ready (shutdown)")
    assert r.call(r.d.valve_open)[2] == "MFC not ready (shutdown)"
    r.d.start()
    assert r.call(r.d.valve_open)[1] is None
    r.done()


def test_params_and_budget():
    assert CFG["CH1_MFC_TCP_PORT"] == 4003 and CFG["CH2_MFC_TCP_PORT"] == 4006
    assert (CFG["CH2_MFC_SCALE_FACTORS_2"], CFG["CH2_MFC_SCALE_FACTORS_3"]) == (10.0, 2.0)
    assert M.endpoint(CFG, "ch2") == ("192.168.1.50", 4006)
    assert M.BUDGET["queries_per_tick"] == 4 and M.BUDGET["min_gap_ms"] == CFG["MFC_GAP_MS"] and M.BUDGET["fits_1s"]
    assert M.source_deadline_ms(CFG) == 2600


def test_valve_position_onoff_sp_values_and_active_sp_are_read_every_tick_as_actual_values():
    """2026-11-09 E1 — GPC3000 R6(밸브 위치 %, 매뉴얼 p.73)·R69(ON/OFF 비트)·R1~R4(SP 값)·R7(선택 SP)을 매 tick 읽어 기억값 대신 실제값으로.
    (2026-11-13 D407 — 이 동작은 MFC_READ_MODE=all. 기본 split 은 아래 시험)"""
    r = Rig("ch1", cfg={"MFC_READ_MODE": "all"})
    r.sim.valve_mode, r.sim.valve = "hold", 0.372                         # 장비 쪽 밸브가 37.2 % 에 있음(기억값이 아니라 장비 값)
    r.sim.sp[1] = 4.5 * 0.1                                              # SP2(UI 4.5 mTorr → 장비 0.45)
    r.call(r.d.set_flow, 1, 5.0)
    r.call(r.d.flow_on, 1)
    r.c.wait_until(r.c.monotonic() + 2.0)
    t0 = r.c.monotonic()
    rows = r.run(8)
    last = rows[-1]
    assert all(rows[k]["ch1.mfc.q"] == "ok" for k in range(2, 8))
    assert last["ch1.mfc.valve_pct"] == pytest.approx(37.2, abs=0.2) and last["ch1.mfc.valve_pct.q"] == "ok"
    assert last["ch1.mfc.sp2_mtorr"] == pytest.approx(4.5, abs=0.01) and last["ch1.mfc.ar_on"] is True
    assert "ch1.mfc.sp_active" in last and last["ch1.mfc.sp_active"] in (0, 1, 2, 3, 4)
    seq = r.sent(t0)
    for cmd in ("R60", "R5", "R6", "R69", "R1", "R2", "R3", "R4", "R7"):
        assert seq.count(cmd) >= 7, (cmd, seq[:40])                      # 9개 질의가 매 tick(번갈아 읽기 없음)
    r.done()


def test_parse_valve_and_active_sp():
    assert M.parse_valve_pct("V+037.2") == 37.2 and M.parse_valve_pct("V-0.1") == -0.1 and M.parse_valve_pct("V+100.0") == 100.0
    assert M.parse_valve_pct("37.2") == 37.2 and M.parse_valve_pct("garbage") is None and M.parse_valve_pct(None) is None
    assert M.parse_active_sp("M3000") == 3 and M.parse_active_sp("M0") == 0 and M.parse_active_sp("X3") is None


# ───────── 2026-11-13 D407 — MFC 나눠 읽기(split) ─────────
def _split_rig(readback=False, **cfg):
    r = Rig("ch1", cfg=cfg or None, readback=readback)
    r.sim.valve_mode, r.sim.valve = "hold", 0.372
    r.sim.sp[1] = 4.5 * 0.1
    return r


def test_split_default_reads_three_every_tick_and_rotates_the_other_six_one_per_tick():
    assert CFG["MFC_READ_MODE"] == "split" and CFG["MFC_TICK_EVERY"] == "R60,R5,R6"
    assert CFG["MFC_TICK_ROTATE"] == "R69,R1,R2,R3,R4,R7"
    r = _split_rig()
    t0 = r.c.monotonic()
    rows = r.run(14)
    seq = r.sent(t0)
    for cmd in ("R60", "R5", "R6"):
        assert seq.count(cmd) >= 13, (cmd, seq)
    rot = [c for c in seq if c in ("R69", "R1", "R2", "R3", "R4", "R7")]
    assert rot[:12] == ["R69", "R1", "R2", "R3", "R4", "R7"] * 2 or rot[:6] == ["R69", "R1", "R2", "R3", "R4", "R7"], rot    # 차례대로 1개씩
    assert all(len(rot) >= 12 for _ in [0]) and max(rot.count(c) for c in set(rot)) - min(rot.count(c) for c in set(rot)) <= 1
    last = rows[-1]
    assert last["ch1.mfc.q"] == "ok" and last["ch1.mfc.valve_pct"] == pytest.approx(37.2, abs=0.2)
    # 돌아가며 읽는 값도 유효 시간((6 + 1) s) 안이라 매 tick 값이 있다 + 나이 칸
    assert last["ch1.mfc.sp2_mtorr"] == pytest.approx(4.5, abs=0.01) and "ch1.mfc.sp_active" in last
    ages = [last[f"ch1.mfc.{k}"] for k in ("on_age_s", "sp1_age_s", "sp2_age_s", "sp3_age_s", "sp4_age_s", "sp_active_age_s")]
    assert all(0.0 <= a <= 6.0 for a in ages) and len(set(ages)) > 1            # 읽은 시점이 서로 다름(돌아가기)
    r.done()


def test_rotated_value_expires_after_rotate_count_plus_one_seconds_and_is_never_filled_with_remembered_value():
    r = _split_rig()
    r.run(10)
    r.d.cfg["MFC_TICK_ROTATE"] = "R69"                                            # R1 읽기를 멈춤(설정으로 — 재시작 없이)
    assert r.d.valid_s("R69") == 2.0 and r.d.valid_s("R60") == 1.0
    rows = r.run(6)
    last = rows[-1]
    assert last["ch1.mfc.sp1_mtorr"] is None and last["ch1.mfc.sp1_mtorr.q"] == "stale"   # 유효 시간 안에 못 읽어 빈 값 — 기억값 없음
    assert last["ch1.mfc.sp1_age_s"] > 2.0 and last["ch1.mfc.valve_pct"] == pytest.approx(37.2, abs=0.2)
    r.done()


def test_all_mode_reads_nine_every_tick():
    r = _split_rig(MFC_READ_MODE="all")
    every, rot = M.tick_plan(r.cfg)
    assert len(every) == 9 and rot == ()
    t0 = r.c.monotonic()
    r.run(8)
    seq = r.sent(t0)
    assert all(seq.count(c) >= 7 for c in ("R60", "R5", "R6", "R69", "R1", "R2", "R3", "R4", "R7"))
    r.done()


def test_write_readback_reads_the_register_immediately_regardless_of_rotation():
    r = _split_rig(readback=True)
    r.run(3)
    t0 = r.c.monotonic()
    k, reason, _ = r.call(r.d.sp_set, 2, 4.5)
    seq = r.sent(t0)
    assert "R2" in seq                                                                # 되읽기 질의 R2 가 차례와 무관하게 바로 나감
    r.done()


def test_tick_lists_reject_out_of_allowlist_and_require_r60_r5():
    from v2.common.paramdef import check_config, check_value
    by = {x.key: x for x in P.PARAMS}
    assert check_value(by["MFC_TICK_EVERY"], "R60,R5,R6,R99") is not None            # 허용 목록 밖(D11 — 고치지 않고 거부)
    assert check_value(by["MFC_TICK_ROTATE"], "R69,Rx") is not None
    assert check_value(by["MFC_TICK_ROTATE"], "") is None                              # 비우면 돌아가기 없음
    assert check_value(by["MFC_READ_MODE"], "fast") is not None
    bad = dict(CFG, MFC_TICK_EVERY="R60,R6")
    assert any("R60" in str(e) for e in check_config(P.PARAMS, P.CROSS_RULES, bad) or [])
    dup = dict(CFG, MFC_TICK_EVERY="R60,R5,R6,R7", MFC_TICK_ROTATE="R69,R7")
    assert check_config(P.PARAMS, P.CROSS_RULES, dup)
    assert M.parse_tick_list("R60, R5,R5,Rx,R7") == ("R60", "R5", "R7")
    every, rot = M.tick_plan(dict(MFC_TICK_EVERY="R6", MFC_TICK_ROTATE="R69,R6"))     # 방어: R60·R5 는 늘 매 tick, 중복은 매 tick 쪽
    assert every == ("R60", "R5", "R6") and rot == ("R69",)

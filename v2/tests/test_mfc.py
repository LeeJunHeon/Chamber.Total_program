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
    def __init__(self, chamber="ch1", writes=True, cfg=None, flow_tau_s=0.0, start=1.5):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.ch = Chamber(chamber)
        self.sim = MfcSim(self.ch, scale=SCALE[chamber], flow_tau_s=flow_tau_s)
        self.cfg = dict(CFG, **(cfg or {}))
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
    """FLOW_SET 확정은 보낸 즉시, FLOW_ON 은 등록 + max(MFC_DELAY_MS,200) 와 보낸 시각 중 늦은 쪽, 명령 사이는 gap 1 s + 흡수 80 ms."""
    r = Rig("ch1")
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


def test_one_second_reads_alternate_with_quality():
    r = Rig("ch2")
    r.call(r.d.set_flow, 2, 1.0)
    r.call(r.d.flow_on, 2)
    r.c.wait_until(r.c.monotonic() + 2.0)
    rows = r.run(14)
    q = [(x["ch2.mfc.o2_sccm.q"], x["ch2.mfc.pressure_mtorr.q"]) for x in rows[2:]]
    assert all(row["ch2.mfc.q"] == "ok" for row in rows[2:])
    assert ("ok", "stale") in q and ("stale", "ok") in q                    # 질의 하나씩 번갈아
    flows = [x["ch2.mfc.o2_sccm"] for x in rows if x["ch2.mfc.o2_sccm.q"] == "ok"]
    assert flows and all(v == pytest.approx(1.0) for v in flows)            # %FS 10 ÷ 스케일 10
    assert rows[-1]["ch2.mfc.o2_set_sccm"] == pytest.approx(1.0) and rows[-1]["ch2.mfc.o2_on"] is True
    gaps = [b - a for (a, x), (b, y) in zip(r.sim.received, list(r.sim.received)[1:])]
    assert min(gaps) >= 1.0 - 1e-9                                          # v1 명령 간격 1 s 를 지킴
    r.done()


def test_tick_reads_yield_while_control_runs():
    """제어 동작 중·직후 1.5 s 에는 1초 읽기가 끼어들지 않는다 — FLOW_SET → FLOW_ON → 안정화 R60 순서가 v1 과 같다."""
    r = Rig("ch1")
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
    assert M.BUDGET["queries_per_tick"] == 1 and M.BUDGET["min_gap_ms"] == CFG["MFC_GAP_MS"]
    assert M.source_deadline_ms(CFG) == 2600

"""CH2 히터 엔진 시험 — v1 HeaterService·런타임 히터 연동 시험의 입력·기대값을 옮김(v1 import 없음, 근거 = v1 시험 파일:줄).

A) 가짜 PLC(_DumbPlc — v1 tests/test_heater_service.py:21-72 FakePLC 와 같은 동작: 쓴 값 반영·PID 를 ACT 로 복사)로
   폴링·명령(tests/test_heater_service.py 23개).
B) 래더 시험대(FastHeaterRig — plc_ladder.HeaterLadder)로 쓰기-읽기 경합(tests/test_heater_poll_write_race.py 3개),
   쓰기 뒤 읽기 실패(tests/test_heater_read_fail_after_write.py 4개). 카드는 test_heater_cards.py.
"""
from __future__ import annotations

from typing import List

import pytest

from v2.common.worker import Request, Result, Status
from v2.ch12.drivers import heater as H
from v2.ch12.drivers.heater import float_to_words
from v2.ch12.drivers.plc import PlcPoll
from v2.ch12.heater_engine import HeaterEngine
from v2.ch12.heater_profile import HeaterCfg, cfg_to_dict
from v2.ch12.sim.heater_rig import FakeClock, FastHeaterRig

ALLOWED_REGS = {112, 113, 114, 118, 120, 121, 126, 127, 128, 142, 143, 144, 145, 160}
ALLOWED_COILS = {640, 643}
T = "[CH2] 히터 "


class _DumbPlc:
    """v1 FakePLC 와 같은 가짜(쓴 값 반영, D142~D145 → D146~D149 복사, RST 는 반영 안 함) + PlcDriver API."""

    def __init__(self, clock, events=None):
        self.clock = clock
        self.regs = {a: 0 for a in range(110, 161)}
        self.bits = {a: False for a in range(640, 659)}
        self.connected = True
        self.fail_writes = False
        self.calls: List[tuple] = []
        self.events = events if events is not None else self.calls
        for a in (653, 656, 657, 658, 641):
            self.bits[a] = True
        self.regs[110] = 1000
        self.decoders = []
        self.allowed = (set(), set())
        self.write_reg_hook = None
        self.write_coil_effect = True

    def allow_writes(self, coils=(), regs=()):
        self.allowed[0].update(coils)
        self.allowed[1].update(regs)

    def register_decoder(self, blocks, decode, on_fail=None):
        self.decoders.append((decode, on_fail))

    def submit(self, name, fn, *, kind="write", source="", on_done=None, **_k):
        req = Request(kind, fn, name=name)
        if not self.connected:
            res = Result(req, Status.LINK_DOWN, reason="LINK_DOWN")
        else:
            res = Result(req, Status.FAIL)
            try:
                res.value, res.status = fn(self), Status.OK
            except Exception as e:
                res.error = repr(e)
        if on_done:
            on_done(res)
        return req

    def io_write_reg(self, s, addr, value):
        self.calls.append(("wr", addr, int(value)))
        if self.events is not self.calls:
            self.events.append(("wr", addr, int(value)))
        if self.fail_writes:
            raise RuntimeError("write failed")
        self.regs[addr] = int(value)
        for a, d in ((142, 146), (143, 147), (144, 148), (145, 149)):
            if addr == a:
                self.regs[d] = int(value)
        if self.write_reg_hook:
            self.write_reg_hook(addr, value)

    def io_write_coil(self, s, addr, on):
        self.calls.append(("wc", addr, bool(on)))
        if self.events is not self.calls:
            self.events.append(("wc", addr, bool(on)))
        if self.fail_writes:
            raise RuntimeError("write failed")
        if addr != 643 and self.write_coil_effect:
            self.bits[addr] = bool(on)

    def writes(self):
        return [c for c in self.calls if c[0] in ("wr", "wc")]

    def read(self):
        if not self.connected:
            return
        self.calls.append(("rr", 110, 51, "low"))
        self.calls.append(("rc", 640, 19, "low"))
        poll = PlcPoll(0.0, self.clock.monotonic(), dict(self.bits), dict(self.regs), {})
        for d, _f in self.decoders:
            d(poll)


def _mk(cfg: HeaterCfg = None, events=None, csv_sink=None):
    clk = FakeClock()
    plc = _DumbPlc(clk, events)
    c = cfg or HeaterCfg(enabled=True, log_enabled=False)
    d = cfg_to_dict(c)
    evs = []
    drv = H.HeaterDriver(clk, plc, d, True, on_event=evs.append)
    eng = HeaterEngine(clk, drv, d, on_event=evs.append, csv=csv_sink)
    return eng, plc, evs, clk


def _poll(eng, plc, clk, dt=1.0):
    plc.read()
    eng.poll_once()
    clk.t += dt


def _logs(evs, word=""):
    return [e.detail for e in evs if e.kind == "log" and word in e.detail]


def _cards(evs, word=""):
    return [(e.data["title"], e.data["subtitle"], e.data["status"]) for e in evs if e.kind == "card" and word in e.data["title"]]


def _call(fn, *a, **k):
    box = []
    fn(*a, on_done=lambda ok, why: box.append((ok, why)), **k)
    return box[0]


# ═════════════════════ A. tests/test_heater_service.py ═════════════════════
def test_residual_run_cleared_before_first_publish():
    events = []
    eng, plc, evs, clk = _mk(events=events)
    plc.bits[640] = True
    plc.regs[110] = 1234
    eng.add_listener(lambda st: events.append(("pub", st["run"])))
    _poll(eng, plc, clk)
    i_off, i_sv = events.index(("wc", 640, False)), events.index(("wr", 112, 1234))
    i_pub = next(i for i, e in enumerate(events) if e[0] == "pub")
    assert i_off < i_sv < i_pub and events[i_pub] == ("pub", False)
    assert _cards(evs, "잔존") and _logs(evs, "잔존")


def test_residual_failure_not_published_then_retried():
    events = []
    eng, plc, evs, clk = _mk(events=events)
    plc.bits[640] = True
    plc.fail_writes = True
    eng.add_listener(lambda st: events.append(("pub", st["run"])))
    _poll(eng, plc, clk)
    assert not any(e[0] == "pub" for e in events)
    plc.fail_writes = False
    _poll(eng, plc, clk)
    assert ("pub", False) in events


def test_push_order_values_high_word_first_and_marker():
    eng, plc, evs, clk = _mk()
    _poll(eng, plc, clk)
    w = [c for c in plc.writes() if c[0] == "wr"]
    assert [c[1] for c in w] == [113, 126, 118, 120, 121, 127, 128, 143, 142, 145, 144, 160]
    vals = {c[1]: c[2] for c in w}
    kp_lo, kp_hi = float_to_words(3.0)
    ti_lo, ti_hi = float_to_words(300.0)
    assert (vals[113], vals[126], vals[118], vals[120], vals[121], vals[127], vals[128]) == (5000, 5500, 520, 2, 100, 0, 10)
    assert (vals[143], vals[142], vals[145], vals[144]) == (kp_hi, kp_lo, ti_hi, ti_lo)
    assert 0x1000 <= vals[160] <= 0xFFFE and eng._marker == vals[160]
    n = len(plc.writes())
    _poll(eng, plc, clk)
    assert len(plc.writes()) == n and not _logs(evs, "되읽기")


def test_pid_not_rewritten_when_act_matches_and_invalid_not_written():
    eng, plc, evs, clk = _mk(cfg=HeaterCfg(enabled=True, log_enabled=False, kp=60.0))
    lo, hi = float_to_words(300.0)
    plc.regs[148], plc.regs[149] = lo, hi
    _poll(eng, plc, clk)
    assert not ({142, 143, 144, 145} & {c[1] for c in plc.writes() if c[0] == "wr"})


def test_readback_mismatch_warns_once():
    eng, plc, evs, clk = _mk()

    def forced(addr, value):
        if addr == 118:
            plc.regs[118] = 600
    plc.write_reg_hook = forced
    for _ in range(3):
        _poll(eng, plc, clk)
    assert sum("래더가 강제 중일 수 있음" in m for m in _logs(evs)) == 1 and _cards(evs, "되읽기 불일치")


def test_marker_single_mismatch_ignored_double_repushes():
    eng, plc, evs, clk = _mk()
    _poll(eng, plc, clk)
    m1 = eng._marker
    n = len(plc.writes())
    plc.regs[160] = 0x0001
    _poll(eng, plc, clk)
    plc.regs[160] = m1
    _poll(eng, plc, clk)
    assert len(plc.writes()) == n and eng._marker == m1
    plc.regs[160] = 0
    _poll(eng, plc, clk)
    assert len(plc.writes()) == n
    _poll(eng, plc, clk)
    w = plc.writes()[n:]
    assert [c[1] for c in w][:7] == [113, 126, 118, 120, 121, 127, 128]
    assert w[-1][1] == 160 and eng._marker == w[-1][2] and _cards(evs, "재기동")


def test_fault_rise_sends_run_off_once():
    eng, plc, evs, clk = _mk()
    _poll(eng, plc, clk)
    plc.bits[640] = True
    _poll(eng, plc, clk)
    plc.bits[642] = plc.bits[644] = True
    plc.bits[641] = False
    plc.write_coil_effect = False
    for _ in range(4):
        _poll(eng, plc, clk)
    assert plc.calls.count(("wc", 640, False)) == 1
    assert sum(1 for c in _cards(evs) if c[0] == T + "이상") == 1 and _logs(evs, "과온")
    plc.write_coil_effect = True
    plc.bits[642] = plc.bits[644] = False
    _poll(eng, plc, clk)
    assert _logs(evs, "이상 해제")


def test_first_poll_fault_marked_existing_latch():
    eng, plc, evs, clk = _mk()
    plc.bits[642] = plc.bits[645] = True
    _poll(eng, plc, clk)
    assert _logs(evs, "기존 래치")


def test_off_deferred_while_link_down_on_rejected():
    eng, plc, evs, clk = _mk()
    _poll(eng, plc, clk)
    plc.bits[640] = True
    _poll(eng, plc, clk)
    plc.connected = False
    n = len(plc.calls)
    ok, why = _call(eng.turn_off, "테스트")
    assert ok is True and "보류" in why
    ok2, why2 = _call(eng.turn_on, 200.0)
    assert ok2 is False and "링크" in why2
    _poll(eng, plc, clk)
    assert len(plc.calls) == n
    plc.connected = True
    _poll(eng, plc, clk)
    assert plc.calls.count(("wc", 640, False)) == 1
    _poll(eng, plc, clk)
    assert plc.calls.count(("wc", 640, False)) == 1


def test_output_dead_grace_and_three_in_a_row():
    eng, plc, evs, clk = _mk()
    _poll(eng, plc, clk)
    plc.bits[640] = True
    plc.regs[141] = 0
    for _ in range(4):
        _poll(eng, plc, clk)
    assert not _cards(evs, "출력 죽음")
    for _ in range(3):
        _poll(eng, plc, clk)
    assert len(_cards(evs, "출력 죽음")) == 1
    plc.regs[141] = 500
    _poll(eng, plc, clk)
    assert eng._out_dead_notified is False


def test_watchdog_only_while_run():
    eng, plc, evs, clk = _mk(cfg=HeaterCfg(enabled=True, log_enabled=False, wd_period_s=1.0))

    def tick(n):
        for _ in range(n):
            clk.t += 1.0
            eng.on_tick_start()
    tick(5)
    none = [c for c in plc.calls if c[:2] == ("wr", 114)]
    eng._st["run"] = True
    tick(5)
    plc.connected = False
    nb = len([c for c in plc.calls if c[:2] == ("wr", 114)])
    tick(3)
    na = len([c for c in plc.calls if c[:2] == ("wr", 114)])
    vals = [c[2] for c in plc.calls if c[:2] == ("wr", 114)]
    assert none == [] and len(vals) >= 2 and vals == list(range(1, len(vals) + 1)) and nb == na


def test_turn_on_kicks_watchdog_once_and_sets_owner():
    eng, plc, evs, clk = _mk()
    _poll(eng, plc, clk)
    ok, why = _call(eng.turn_on, 150.0)
    assert ok, why
    w = plc.writes()[-4:]
    assert ("wr", 120, 3) in w and ("wc", 640, True) in w and w[-1][:2] == ("wr", 114)
    assert eng.owner == "manual"
    assert not _call(eng.turn_on, 600.0)[0]


def test_turn_on_rejections():
    eng, plc, evs, clk = _mk()
    assert not _call(eng.turn_on, 100.0)[0]
    _poll(eng, plc, clk)
    plc.bits[641] = plc.bits[653] = False
    _poll(eng, plc, clk)
    ok, why = _call(eng.turn_on, 100.0)
    assert not ok and "TC" in why
    plc.bits[641] = plc.bits[653] = True
    _poll(eng, plc, clk)
    for bad in (0.0, -5.0, 501.0):
        ok, why = _call(eng.turn_on, bad)
        assert not ok and "범위" in why
    assert eng.claim("process")
    ok, why = _call(eng.turn_on, 100.0, owner="manual")
    assert not ok and "충돌" in why


def test_reset_refused_while_run():
    eng, plc, evs, clk = _mk()
    _poll(eng, plc, clk)
    plc.bits[640] = True
    _poll(eng, plc, clk)
    ok, _ = _call(eng.reset_fault)
    assert not ok and ("wc", 643, True) not in plc.calls
    plc.bits[640] = False
    _poll(eng, plc, clk)
    ok, _ = _call(eng.reset_fault)
    assert ok and ("wc", 643, True) in plc.calls


def test_poll_intervals():
    eng, plc, evs, clk = _mk()
    _poll(eng, plc, clk)
    assert eng._next_interval() == eng.cfg.poll_idle_s
    plc.bits[640] = True
    _poll(eng, plc, clk)
    assert eng._next_interval() == eng.cfg.poll_interval_s
    plc.bits[640] = False
    _poll(eng, plc, clk)
    eng._pending_off = True
    assert eng._next_interval() == eng.cfg.poll_interval_s


def test_poll_cadence_on_rows():
    """on_row 는 v1 주기대로 — 정지 중 3 s, RUN 중 1 s 마다 폴링(controller/heater_service.py:710-725)."""
    eng, plc, evs, clk = _mk()
    got = []
    eng.add_listener(lambda st: got.append(clk.t))
    for _ in range(10):
        plc.read()
        eng.on_row(None)
        clk.t += 1.0
    assert [b - a for a, b in zip(got, got[1:])] == [3.0, 3.0, 3.0]
    plc.bits[640] = True
    got.clear()
    for _ in range(6):
        plc.read()
        eng.on_row(None)
        clk.t += 1.0
    assert len(got) == 4 and [b - a for a, b in zip(got, got[1:])] == [1.0, 1.0, 1.0]   # 남은 정지 주기 뒤 1 s 마다


def test_not_connected_no_io_and_stale():
    eng, plc, evs, clk = _mk()
    plc.connected = False
    got = []
    eng.add_listener(got.append)
    _poll(eng, plc, clk)
    assert plc.calls == [] and got and got[-1]["stale"] is True
    plc.connected = True
    _poll(eng, plc, clk)
    assert got[-1]["stale"] is False
    plc.connected = False
    clk.t += 5.0
    _poll(eng, plc, clk)
    assert got[-1]["stale"] is False
    clk.t += 10.0
    _poll(eng, plc, clk)
    assert got[-1]["stale"] is True and got[-1]["age_s"] > 10


def test_write_addresses_whitelist_and_low_priority_reads():
    eng, plc, evs, clk = _mk()
    plc.bits[640] = True
    _poll(eng, plc, clk)
    _poll(eng, plc, clk)
    _call(eng.turn_on, 150.0)
    for _ in range(5):
        plc.regs[119] = plc.regs[112]
        _poll(eng, plc, clk)
    _call(eng.apply_target, 120.0)
    _call(eng.turn_off)
    _poll(eng, plc, clk)
    _call(eng.reset_fault)
    eng.shutdown()
    for c in plc.writes():
        assert c[1] in (ALLOWED_REGS if c[0] == "wr" else ALLOWED_COILS), c
    reads = [c for c in plc.calls if c[0] in ("rr", "rc")]
    assert reads and all(c[3] == "low" for c in reads) and {(c[1], c[2]) for c in reads} == {(110, 51), (640, 19)}
    assert plc.allowed[0] == ALLOWED_COILS and plc.allowed[1] == ALLOWED_REGS


def test_driver_rejects_outside_whitelist():
    eng, plc, evs, clk = _mk()
    with pytest.raises(ValueError, match="허용 밖 레지스터 D115"):
        eng.drv.w_reg(plc, 115, 1)
    with pytest.raises(ValueError, match="허용 밖 코일 641"):
        eng.drv.w_coil(plc, 641, True)
    assert plc.calls == []


def test_run_edges_csv_and_chat(tmp_path):
    from v2.ch12.outputs.heater_csv import HeaterCsv
    c = HeaterCfg(enabled=True, log_enabled=True, log_dir=str(tmp_path), log_period_s=1.0)
    clk = FakeClock()
    sink = HeaterCsv(cfg_to_dict(c), monotonic=clk.monotonic)
    plc = _DumbPlc(clk)
    d = cfg_to_dict(c)
    evs = []
    eng = HeaterEngine(clk, H.HeaterDriver(clk, plc, d, True), d, on_event=evs.append, csv=sink)
    _poll(eng, plc, clk)
    plc.bits[640] = True
    _poll(eng, plc, clk)
    assert eng.owner == "external" and any(x[0] == T + "시작" for x in _cards(evs))
    for _ in range(3):
        _poll(eng, plc, clk)
    plc.bits[640] = False
    _poll(eng, plc, clk)
    assert eng.owner is None and any(x[0] == T + "종료" for x in _cards(evs))
    sink.shutdown()
    files = [f for f in tmp_path.iterdir() if f.name.startswith("CH2_HEATER_")]
    assert len(files) == 1
    lines = files[0].read_text(encoding="utf-8-sig").strip().splitlines()
    assert lines[0].startswith("timestamp,elapsed_sec,owner,pv_c") and len(lines) >= 3 and "운전 종료" in lines[-1]


def test_push_stops_on_first_failure_backoff_and_recovery():
    eng, plc, evs, clk = _mk()
    plc.fail_writes = True
    _poll(eng, plc, clk)
    assert plc.writes() == [("wr", 113, 5000)]
    ok, why = _call(eng.turn_on, 100.0)
    assert not ok and "설정" in why
    for _ in range(28):
        _poll(eng, plc, clk)
    assert len(plc.writes()) == 1 and sum("히터 설정 쓰기 실패" in m for m in _logs(evs)) == 1
    plc.fail_writes = False
    _poll(eng, plc, clk)
    _poll(eng, plc, clk)
    assert any(c[:2] == ("wr", 160) for c in plc.writes()) and _logs(evs, "히터 설정 쓰기 복구 (1회 실패 뒤)")
    ok, why = _call(eng.turn_on, 100.0)
    assert ok, why


def test_marker_mismatch_run_fall_reason_restart():
    eng, plc, evs, clk = _mk()
    _poll(eng, plc, clk)
    plc.bits[640] = True
    _poll(eng, plc, clk)
    plc.bits[640] = False
    plc.regs[160] = 0
    _poll(eng, plc, clk)
    ends = [c for c in _cards(evs) if c[0] == T + "종료"]
    assert len(ends) == 1 and "재기동" in ends[0][1]


def test_quiet_before_first_success_then_logged():
    eng, plc, evs, clk = _mk()
    plc.connected = False
    for _ in range(3):
        _poll(eng, plc, clk)
    plc.connected = True
    _poll(eng, plc, clk)
    assert not _logs(evs, "읽기 실패") and not _logs(evs, "읽기 복구")
    plc.connected = False
    _poll(eng, plc, clk)
    assert _logs(evs, "히터 상태 읽기 실패")
    plc.connected = True
    _poll(eng, plc, clk)
    assert _logs(evs, "히터 상태 읽기 복구")


def test_rst_stuck_cleared_after_two_polls():
    eng, plc, evs, clk = _mk()
    _poll(eng, plc, clk)
    plc.bits[643] = True
    _poll(eng, plc, clk)
    assert ("wc", 643, False) not in plc.calls
    _poll(eng, plc, clk)
    assert ("wc", 643, False) in plc.calls and _logs(evs, "M00403")


def test_reset_with_cause_remaining_logged():
    eng, plc, evs, clk = _mk()
    plc.bits[642] = plc.bits[645] = True
    plc.bits[641] = False
    _poll(eng, plc, clk)
    ok, why = _call(eng.reset_fault)
    assert ok, why
    _poll(eng, plc, clk)
    _poll(eng, plc, clk)
    assert sum("리셋 후에도" in m for m in _logs(evs)) == 1


def test_bench_no_writes_still_publishes():
    """bench(writes_allowed=False) — 해독·발행은 하고 PLC 에 쓰지 않으며 명령은 BENCH_READ_ONLY(v2 이중 보호)."""
    clk = FakeClock()
    plc = _DumbPlc(clk)
    d = cfg_to_dict(HeaterCfg(enabled=True, log_enabled=False))
    eng = HeaterEngine(clk, H.HeaterDriver(clk, plc, d, False), d)
    plc.bits[640] = True
    got = []
    eng.add_listener(got.append)
    _poll(eng, plc, clk)
    assert got and got[-1]["run"] is True and plc.writes() == []
    ok, why = _call(eng.turn_off)
    assert not ok and "BENCH_READ_ONLY" in why and plc.writes() == []


# ═════════════════════ B. 경합(tests/test_heater_poll_write_race.py:87-149) ═════════════════════
def _rig(**kw):
    r = FastHeaterRig(**kw)
    r.run(5)
    return r


def test_process_turn_on_during_poll_read_no_false_e832():
    r = _rig(mv_limit=700)
    pub = []
    r.eng.add_listener(lambda st: pub.append(bool(st.get("run"))))
    r.plc.read()                                         # 폴링이 RUN=0 을 읽음(처리 전)
    r.eng.heater_set(200.0, None)                        # 공정이 히터를 켬 → 감시 시작
    assert [k for k, *_ in r.eng.take_process_results()] == ["set_ok"] and r.eng._watch and r.plc.M[640]
    r.eng.poll_once()                                    # 쓰기 전 읽기 — 버린다
    assert pub == [] and not r.eng.take_process_results()
    r.run(3)
    assert r.eng.status()["run"] and pub[-1] is True and not r.eng.take_process_results()


def test_panel_turn_off_during_poll_read_no_stale_run():
    r = _rig(mv_limit=700)
    assert r.call(r.eng.turn_on, 150.0, owner="manual")[0]
    r.run(5)
    pub = []
    r.eng.add_listener(lambda st: pub.append(bool(st.get("run"))))
    r.plc.read()                                         # RUN=1 을 읽은 채
    assert r.call(r.eng.turn_off, "정지(패널)", owner="manual")[0]
    r.eng.poll_once()
    assert pub == []
    r.run(3)
    assert not r.eng.status()["run"] and pub and not any(pub)


def test_watchdog_write_during_read_does_not_drop_poll():
    r = _rig(mv_limit=700)
    assert r.call(r.eng.turn_on, 150.0, owner="manual")[0]
    r.run(5)
    pub = []
    r.eng.add_listener(lambda st: pub.append(bool(st.get("run"))))
    r.plc.read()
    r.eng._wd_next = 0.0
    r.eng.on_tick_start()                                # 워치독(D114) — 상태를 바꾸지 않는다
    r.eng.poll_once()
    assert pub == [True]


# ═════════════════════ B. 쓰기 뒤 읽기 실패(tests/test_heater_read_fail_after_write.py:92-164) ═════════════════════
@pytest.mark.parametrize("wait", [True, False])
def test_process_turn_on_then_one_read_fail_no_false_stop(wait):
    r = _rig(mv_limit=700)
    pub = []
    r.eng.add_listener(lambda st: pub.append((r.clk.t, bool(st.get("run")), bool(st.get("stale")), st.get("age_s"))))
    r.eng.heater_set(200.0, None)
    if wait:
        r.eng.heater_wait(200.0)
    assert [k for k, *_ in r.eng.take_process_results()] == ["set_ok"] and r.eng._watch and r.plc.M[640]
    r.plc.fail_reads = 1
    r.run(3)
    assert not r.eng.take_process_results()
    assert not [p for p in pub if not p[1] and not p[2]], pub
    assert pub and pub[-1][1] and not pub[-1][2] and r.log_texts("히터 상태 읽기 실패")


def test_panel_turn_off_then_read_fail_no_old_run():
    r = _rig(mv_limit=700)
    assert r.call(r.eng.turn_on, 150.0, owner="manual")[0]
    r.run(5)
    pub = []
    r.eng.add_listener(lambda st: pub.append(bool(st.get("run"))))
    assert r.call(r.eng.turn_off, "정지(패널)", owner="manual")[0]
    r.plc.fail_reads = 1
    r.run(3)
    assert pub and not any(pub)


def test_read_fail_without_write_still_republishes():
    """쓰기가 없었으면 읽기 실패 때 마지막 상태를 stale 아님(age)으로 다시 발행 — v1 은 age 1.0(폴링 1 s), v2 정지 중 3 s 주기."""
    r = _rig(mv_limit=700)
    assert r.call(r.eng.turn_on, 150.0, owner="manual")[0]
    r.run(3)
    pub = []
    r.eng.add_listener(lambda st: pub.append((bool(st.get("run")), bool(st.get("stale")), st.get("age_s"))))
    r.plc.fail_reads = 1
    r.run(2)
    assert pub[0] == (True, False, 1.0) and len(pub) == 2, pub


def test_long_read_fail_after_turn_on_publishes_stale_then_waiter_fails_as_before():
    r = _rig(mv_limit=700)
    pub = []
    r.eng.add_listener(lambda st: pub.append((r.clk.t, bool(st.get("run")), bool(st.get("stale")))))
    r.eng.heater_set(200.0, None)
    r.eng.heater_wait(200.0)
    t0 = r.clk.t
    r.eng.take_process_results()
    r.plc.fail_reads = 10 ** 6
    r.run(120)
    assert not [p for p in pub if not p[2]], pub[:5]
    assert min(p[0] for p in pub) - t0 <= r.eng.cfg.stale_sec + 2
    fails = [x for x in r.eng.take_process_results() if x[0] == "failed"]
    assert len(fails) == 1 and "PLC 응답 없음" in fails[0][2]

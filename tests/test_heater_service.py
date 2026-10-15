# -*- coding: utf-8 -*-
"""controller/heater_service.py — 가짜 PLC(호출 기록)로 폴링·명령 동작 검증. 장치·네트워크 접근 없음.
pytest / 직접 실행 모두 가능."""
import os
import sys
import asyncio
import tempfile
from dataclasses import replace

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import pytest                                                   # noqa: E402
from controller import heater_service as HS                     # noqa: E402
from controller.heater_service import HeaterService             # noqa: E402
from lib.heater_config import HeaterConfig, float_to_words      # noqa: E402

ALLOWED_REGS = {112, 113, 114, 118, 120, 121, 126, 127, 128, 142, 143, 144, 145, 160}
ALLOWED_COILS = {640, 643}


class FakePLC:
    """AsyncPLC 공개 메서드만 흉내낸다. 쓴 값은 레지스터에 반영(래더가 PID 값을 ACT 로 복사하는 것도 흉내)."""

    def __init__(self, events=None):
        self.regs = {a: 0 for a in range(110, 161)}
        self.bits = {a: False for a in range(640, 659)}
        self.connected = True
        self.fail_writes = False
        self.calls = []
        self.events = events if events is not None else self.calls
        self.bits[653] = True          # TC1_OK
        self.bits[656] = True          # DA_OK
        self.bits[657] = True          # WATER_OK
        self.bits[658] = True          # VAC_OK
        self.bits[641] = True          # ITL
        self.regs[110] = 1000          # PV 100.0

    def is_connected(self):
        return self.connected

    async def read_regs_block(self, start, count, *, priority="high"):
        self.calls.append(("rr", start, count, priority))
        return [self.regs[a] for a in range(start, start + count)]

    async def read_coils_block(self, start, count, *, priority="high"):
        self.calls.append(("rc", start, count, priority))
        return [self.bits[a] for a in range(start, start + count)]

    async def write_reg(self, addr, value):
        self.calls.append(("wr", addr, int(value)))
        if self.events is not self.calls:
            self.events.append(("wr", addr, int(value)))
        if self.fail_writes:
            raise RuntimeError("write failed")
        self.regs[addr] = int(value)
        for s, d in ((142, 146), (143, 147), (144, 148), (145, 149)):
            if addr == s:
                self.regs[d] = int(value)

    async def write_coil(self, addr, on):
        self.calls.append(("wc", addr, bool(on)))
        if self.events is not self.calls:
            self.events.append(("wc", addr, bool(on)))
        if self.fail_writes:
            raise RuntimeError("write failed")
        if addr != 643:
            self.bits[addr] = bool(on)

    def writes(self):
        return [c for c in self.calls if c[0] in ("wr", "wc")]


class FakeChat:
    def __init__(self):
        self.cards = []

    def _post_card(self, title, subtitle="", status="INFO", fields=None, urgent=False):
        self.cards.append((title, subtitle, status, urgent))


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _mk(cfg=None, events=None):
    plc = FakePLC(events)
    chat = FakeChat()
    clk = _Clock()
    logs = []
    c = cfg or HeaterConfig(enabled=True, log_enabled=False)
    svc = HeaterService(plc, c, log=lambda s, m: logs.append(m), chat=chat, clock=clk)
    return svc, plc, chat, clk, logs


def _poll(svc, clk, dt=1.0):
    asyncio.run(svc._poll_once())
    clk.t += dt


# ───────────────────────── 잔존 RUN ─────────────────────────
def test_residual_run_cleared_before_first_publish():
    events = []
    svc, plc, chat, clk, logs = _mk(events=events)
    plc.bits[640] = True                                          # 이전 세션 RUN
    plc.regs[110] = 1234
    svc.add_listener(lambda st: events.append(("pub", st["run"])))
    _poll(svc, clk)
    i_off = events.index(("wc", 640, False))
    i_sv = events.index(("wr", 112, 1234))
    i_pub = next(i for i, e in enumerate(events) if e[0] == "pub")
    assert i_off < i_sv < i_pub
    assert events[i_pub] == ("pub", False)
    assert any("잔존" in c[0] for c in chat.cards)
    assert any("잔존" in m for m in logs)


def test_residual_failure_not_published_then_retried():
    events = []
    svc, plc, chat, clk, logs = _mk(events=events)
    plc.bits[640] = True
    plc.fail_writes = True
    svc.add_listener(lambda st: events.append(("pub", st["run"])))
    _poll(svc, clk)
    assert not any(e[0] == "pub" for e in events)
    plc.fail_writes = False
    _poll(svc, clk)
    assert ("pub", False) in events


# ───────────────────────── push·마커 ─────────────────────────
def test_push_order_values_high_word_first_and_marker():
    svc, plc, chat, clk, logs = _mk()
    _poll(svc, clk)
    w = [c for c in plc.writes() if c[0] == "wr"]
    addrs = [c[1] for c in w]
    assert addrs == [113, 126, 118, 120, 121, 127, 128, 143, 142, 145, 144, 160], addrs
    vals = {c[1]: c[2] for c in w}
    kp_lo, kp_hi = float_to_words(3.0)
    ti_lo, ti_hi = float_to_words(300.0)
    assert (vals[113], vals[126], vals[118], vals[120], vals[121], vals[127], vals[128]) == \
        (5000, 5500, 520, 2, 100, 0, 10)
    assert (vals[143], vals[142], vals[145], vals[144]) == (kp_hi, kp_lo, ti_hi, ti_lo)
    assert 0x1000 <= vals[160] <= 0xFFFE and svc._marker == vals[160]
    # 다음 폴링: 되읽기 일치 → 경고 없음, 다시 쓰지 않음
    n = len(plc.writes())
    _poll(svc, clk)
    assert len(plc.writes()) == n
    assert not any("되읽기" in m for m in logs)


def test_pid_not_rewritten_when_act_matches_and_invalid_not_written():
    svc, plc, chat, clk, logs = _mk(cfg=HeaterConfig(enabled=True, log_enabled=False, kp=60.0, kp_valid=False))
    lo, hi = float_to_words(300.0)
    plc.regs[148], plc.regs[149] = lo, hi                          # Ti ACT 이미 300
    _poll(svc, clk)
    addrs = [c[1] for c in plc.writes() if c[0] == "wr"]
    assert not ({142, 143, 144, 145} & set(addrs)), addrs


def test_readback_mismatch_warns_once():
    svc, plc, chat, clk, logs = _mk()
    orig = plc.write_reg

    async def _forced(addr, value):                               # 래더가 D118 을 강제
        await orig(addr, value)
        if addr == 118:
            plc.regs[118] = 600
    plc.write_reg = _forced
    _poll(svc, clk)
    _poll(svc, clk)
    _poll(svc, clk)
    assert sum("래더가 강제 중일 수 있음" in m for m in logs) == 1
    assert any("되읽기 불일치" in c[0] for c in chat.cards)


def test_marker_single_mismatch_ignored_double_repushes():
    svc, plc, chat, clk, logs = _mk()
    _poll(svc, clk)
    m1 = svc._marker
    n = len(plc.writes())
    plc.regs[160] = 0x0001                                        # 1회 불일치
    _poll(svc, clk)
    plc.regs[160] = m1
    _poll(svc, clk)
    assert len(plc.writes()) == n and svc._marker == m1
    plc.regs[160] = 0                                             # PLC 재기동(래치 없음 → 0)
    _poll(svc, clk)
    assert len(plc.writes()) == n
    _poll(svc, clk)                                               # 2회 연속 → 재push + 새 마커
    w = plc.writes()[n:]
    assert [c[1] for c in w][:7] == [113, 126, 118, 120, 121, 127, 128]
    assert w[-1][1] == 160 and svc._marker == w[-1][2]
    assert any("재기동" in c[0] for c in chat.cards)


# ───────────────────────── FAULT ─────────────────────────
def test_fault_rise_sends_run_off_once():
    svc, plc, chat, clk, logs = _mk()
    _poll(svc, clk)
    plc.bits[640] = True
    _poll(svc, clk)
    plc.bits[642] = True                                          # FAULT, 래더가 RUN 을 아직 안 끔(가정)
    plc.bits[644] = True                                          # OT
    plc.bits[641] = False
    orig = plc.write_coil

    async def _no_effect(addr, on):                               # 쓰기는 기록만 — RUN 이 그대로 보이게
        plc.calls.append(("wc", addr, bool(on)))
    plc.write_coil = _no_effect
    for _ in range(4):
        _poll(svc, clk)
    offs = [c for c in plc.calls if c == ("wc", 640, False)]
    assert len(offs) == 1
    assert sum(1 for c in chat.cards if "이상" == c[0].replace("[CH2] 히터 ", "")) == 1
    assert any("과온" in m for m in logs)
    plc.write_coil = orig
    plc.bits[642] = plc.bits[644] = False
    _poll(svc, clk)
    assert any("이상 해제" in m for m in logs)


def test_first_poll_fault_marked_existing_latch():
    svc, plc, chat, clk, logs = _mk()
    plc.bits[642] = True
    plc.bits[645] = True
    _poll(svc, clk)
    assert any("기존 래치" in m for m in logs)


# ───────────────────────── 링크 다운 OFF/ON ─────────────────────────
def test_off_deferred_while_link_down_on_rejected():
    svc, plc, chat, clk, logs = _mk()
    _poll(svc, clk)
    plc.bits[640] = True
    _poll(svc, clk)
    plc.connected = False
    n = len(plc.calls)
    ok, why = asyncio.run(svc.turn_off("테스트"))
    assert ok is True and "보류" in why
    ok2, why2 = asyncio.run(svc.turn_on(200.0))
    assert ok2 is False and "링크" in why2
    _poll(svc, clk)
    assert len(plc.calls) == n                                    # 미연결: I/O 0건
    plc.connected = True
    _poll(svc, clk)
    assert plc.calls.count(("wc", 640, False)) == 1
    _poll(svc, clk)
    assert plc.calls.count(("wc", 640, False)) == 1               # 1회만


# ───────────────────────── 출력 죽음 ─────────────────────────
def test_output_dead_grace_and_three_in_a_row():
    svc, plc, chat, clk, logs = _mk()
    _poll(svc, clk)
    plc.bits[640] = True
    plc.regs[141] = 0                                             # MV < MV_MIN
    for _ in range(4):                                            # RUN 상승 후 0~3초 — 유예 중
        _poll(svc, clk)
    assert not any("출력 죽음" in c[0] for c in chat.cards)
    for _ in range(3):
        _poll(svc, clk)
    assert sum("출력 죽음" in c[0] for c in chat.cards) == 1
    plc.regs[141] = 500                                           # 회복 → 다음 에피소드에 다시 알릴 수 있음
    _poll(svc, clk)
    assert svc._out_dead_notified is False


# ───────────────────────── 워치독 ─────────────────────────
def test_watchdog_only_while_run():
    cfg = replace(HeaterConfig(enabled=True, log_enabled=False), wd_period_s=0.02)

    async def _main():
        svc, plc, chat, clk, logs = _mk(cfg=cfg)
        svc._st["run"] = False
        t = asyncio.get_running_loop().create_task(svc._wd_loop())
        await asyncio.sleep(0.15)
        none = [c for c in plc.calls if c[:2] == ("wr", 114)]
        svc._st["run"] = True
        await asyncio.sleep(0.15)
        plc.connected = False
        n_before = len([c for c in plc.calls if c[:2] == ("wr", 114)])
        await asyncio.sleep(0.1)
        n_after = len([c for c in plc.calls if c[:2] == ("wr", 114)])
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        vals = [c[2] for c in plc.calls if c[:2] == ("wr", 114)]
        return none, vals, n_before, n_after

    none, vals, nb, na = asyncio.run(_main())
    assert none == []
    assert len(vals) >= 2 and vals == list(range(1, len(vals) + 1))
    assert nb == na                                               # 링크 없으면 쓰지 않음


def test_turn_on_kicks_watchdog_once_and_sets_owner():
    svc, plc, chat, clk, logs = _mk()
    _poll(svc, clk)
    ok, why = asyncio.run(svc.turn_on(150.0))
    assert ok, why
    w = [c for c in plc.writes()][-4:]
    assert ("wr", 120, 3) in w and ("wc", 640, True) in w and w[-1][:2] == ("wr", 114)
    assert svc.owner == "manual"
    ok, why = asyncio.run(svc.turn_on(600.0))
    assert not ok


def test_turn_on_rejections():
    svc, plc, chat, clk, logs = _mk()
    ok, why = asyncio.run(svc.turn_on(100.0))                     # 아직 한 번도 못 읽음 → stale
    assert not ok
    _poll(svc, clk)
    plc.bits[641] = False
    plc.bits[653] = False
    _poll(svc, clk)
    ok, why = asyncio.run(svc.turn_on(100.0))
    assert not ok and "TC" in why
    plc.bits[641] = plc.bits[653] = True
    _poll(svc, clk)
    for bad in (0.0, -5.0, 501.0):
        ok, why = asyncio.run(svc.turn_on(bad))
        assert not ok and "범위" in why
    assert svc.claim("process")
    ok, why = asyncio.run(svc.turn_on(100.0, owner="manual"))
    assert not ok and "충돌" in why


def test_reset_refused_while_run():
    svc, plc, chat, clk, logs = _mk()
    _poll(svc, clk)
    plc.bits[640] = True
    _poll(svc, clk)
    ok, why = asyncio.run(svc.reset_fault())
    assert not ok and ("wc", 643, True) not in plc.calls
    plc.bits[640] = False
    _poll(svc, clk)
    ok, why = asyncio.run(svc.reset_fault())
    assert ok and ("wc", 643, True) in plc.calls


# ───────────────────────── 주기·미연결 ─────────────────────────
def test_poll_intervals():
    svc, plc, chat, clk, logs = _mk()
    _poll(svc, clk)
    assert svc._next_interval() == svc.cfg.poll_idle_s
    plc.bits[640] = True
    _poll(svc, clk)
    assert svc._next_interval() == svc.cfg.poll_interval_s
    plc.bits[640] = False
    _poll(svc, clk)
    svc._pending_off = True
    assert svc._next_interval() == svc.cfg.poll_interval_s


def test_not_connected_no_io_and_stale():
    svc, plc, chat, clk, logs = _mk()
    plc.connected = False
    got = []
    svc.add_listener(got.append)
    _poll(svc, clk)
    assert plc.calls == []
    assert got and got[-1]["stale"] is True
    plc.connected = True
    _poll(svc, clk)
    assert got[-1]["stale"] is False
    plc.connected = False
    clk.t += 5.0
    _poll(svc, clk)
    assert got[-1]["stale"] is False                              # 아직 STALE_SEC(10) 안
    clk.t += 10.0
    _poll(svc, clk)
    assert got[-1]["stale"] is True and got[-1]["age_s"] > 10


# ───────────────────────── 주소·우선순위 ─────────────────────────
def test_write_addresses_whitelist_and_low_priority_reads():
    tmp = tempfile.mkdtemp()
    svc, plc, chat, clk, logs = _mk(cfg=HeaterConfig(enabled=True, log_enabled=True, log_dir=tmp))
    plc.bits[640] = True
    _poll(svc, clk)                                               # 잔존 RUN
    _poll(svc, clk)
    asyncio.run(svc.turn_on(150.0))
    for _ in range(5):
        plc.regs[119] = plc.regs[112]
        _poll(svc, clk)
    asyncio.run(svc.apply_target(120.0))
    asyncio.run(svc.turn_off())
    _poll(svc, clk)
    asyncio.run(svc.reset_fault())
    asyncio.run(svc.shutdown())
    for c in plc.writes():
        if c[0] == "wr":
            assert c[1] in ALLOWED_REGS, c
        else:
            assert c[1] in ALLOWED_COILS, c
    reads = [c for c in plc.calls if c[0] in ("rr", "rc")]
    assert reads and all(c[3] == "low" for c in reads)
    assert {(c[1], c[2]) for c in reads} == {(110, 51), (640, 19)}


def test_run_edges_csv_and_chat(tmp_path=None):
    tmp = tempfile.mkdtemp()
    svc, plc, chat, clk, logs = _mk(cfg=HeaterConfig(enabled=True, log_enabled=True, log_dir=tmp, log_period_s=1.0))
    _poll(svc, clk)
    plc.bits[640] = True                                          # 외부에서 켬
    _poll(svc, clk)
    assert svc.owner == "external"
    assert any(c[0] == "[CH2] 히터 시작" for c in chat.cards)
    for _ in range(3):
        _poll(svc, clk)
    plc.bits[640] = False
    _poll(svc, clk)
    assert svc.owner is None
    assert any(c[0] == "[CH2] 히터 종료" for c in chat.cards)
    files = [f for f in os.listdir(tmp) if f.startswith("CH2_HEATER_")]
    assert len(files) == 1
    with open(os.path.join(tmp, files[0]), encoding="utf-8-sig") as fp:
        lines = fp.read().strip().splitlines()
    assert lines[0].startswith("timestamp,elapsed_sec,owner,pv_c")
    assert len(lines) >= 3 and "운전 종료" in lines[-1]



# ───────────────────────── 1단계 보완 ─────────────────────────
def test_push_stops_on_first_failure_backoff_and_recovery():
    svc, plc, chat, clk, logs = _mk()
    plc.fail_writes = True
    _poll(svc, clk)
    assert [c for c in plc.writes()] == [("wr", 113, 5000)]       # 첫 실패에서 멈춤(쓰기 1건)
    ok, why = asyncio.run(svc.turn_on(100.0))
    assert not ok and "설정" in why                               # 설정 전 ON 거부
    for _ in range(28):                                           # 30초 안 재시도 없음
        _poll(svc, clk)
    assert len(plc.writes()) == 1
    assert sum("히터 설정 쓰기 실패" in m for m in logs) == 1
    plc.fail_writes = False
    _poll(svc, clk)
    _poll(svc, clk)
    assert any(c[:2] == ("wr", 160) for c in plc.writes())
    assert any("히터 설정 쓰기 복구 (1회 실패 뒤)" in m for m in logs)
    ok, why = asyncio.run(svc.turn_on(100.0))
    assert ok, why


def test_marker_mismatch_run_fall_reason_restart():
    svc, plc, chat, clk, logs = _mk()
    _poll(svc, clk)
    plc.bits[640] = True
    _poll(svc, clk)
    plc.bits[640] = False
    plc.regs[160] = 0                                             # 메모리 초기화
    _poll(svc, clk)
    ends = [c for c in chat.cards if c[0] == "[CH2] 히터 종료"]
    assert len(ends) == 1 and "재기동" in ends[0][1], ends


def test_quiet_before_first_success_then_logged():
    svc, plc, chat, clk, logs = _mk()
    plc.connected = False
    for _ in range(3):
        _poll(svc, clk)
    plc.connected = True
    _poll(svc, clk)
    assert not any("읽기 실패" in m or "읽기 복구" in m for m in logs)
    plc.connected = False
    _poll(svc, clk)
    assert any("히터 상태 읽기 실패" in m for m in logs)
    plc.connected = True
    _poll(svc, clk)
    assert any("히터 상태 읽기 복구" in m for m in logs)


def test_rst_stuck_cleared_after_two_polls():
    svc, plc, chat, clk, logs = _mk()
    _poll(svc, clk)
    plc.bits[643] = True                                          # 래더가 RST 를 0 으로 되돌리지 않음
    _poll(svc, clk)
    assert ("wc", 643, False) not in plc.calls
    _poll(svc, clk)
    assert ("wc", 643, False) in plc.calls
    assert any("M00403" in m for m in logs)


def test_reset_with_cause_remaining_logged():
    svc, plc, chat, clk, logs = _mk()
    plc.bits[642] = plc.bits[645] = True
    plc.bits[641] = False
    _poll(svc, clk)
    ok, why = asyncio.run(svc.reset_fault())
    assert ok, why
    _poll(svc, clk)
    assert sum("리셋 후에도" in m for m in logs) == 1
    _poll(svc, clk)
    assert sum("리셋 후에도" in m for m in logs) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

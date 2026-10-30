"""CH2 히터 드라이버 — 해독·허용 목록·쓰기 횟수 + 실제 PLC 스택(PlcSim Modbus TCP·worker·1초 묶음 읽기·TickLoop) 시나리오.

실제 스택(HeaterRig): 승온·유지(파이썬 램프·RUN 전에 SV 출발점), 워치독(D114 하트비트 — 끊기면 래더 WD_ERR → FAULT → RUN 리셋),
PLC 재기동(세션 마커 2회 불일치 → 재 push·마커), 출력 무응답 A·끊김 B, 포화 클램프, 쓰기-읽기 경합, bench, 1초 행 필드.
바이트 근거: 설정 push 순서·값(v1 tests/test_heater_service.py:114-130 와 같은 주소·값 — PlcSim 이 받은 FC6 PDU 그대로).
"""
from __future__ import annotations


import pytest

from v2.ch12.drivers import heater as H
from v2.ch12.drivers.heater import decode_block, float_to_words
from v2.ch12.heater_profile import HeaterCfg
from v2.ch12.sim.heater_rig import HeaterRig

FAST = dict(CH2_HEATER_RAMP_RATE_C_PER_MIN=60.0, CH2_HEATER_KP=20.0)


def _fast(r):
    """시험 시간을 줄이는 열 모델(가정값 — 시정수 200 s, 이득 700 °C: MV 1200 정상상태 ≈ 492 °C)."""
    r.sim.heater.tau = 200.0
    r.sim.heater.gain = 700.0
    return r


def _quiet(r):
    """1초 읽기가 끝난 시각(정각 + 0.95 s)으로 — 시험 코드가 장비 메모리를 바꿀 때 읽기 도중에 끼지 않게."""
    import math
    r.p.until(math.floor(r.c.monotonic()) + 0.95)


def _fc6(pdus):
    return [(int.from_bytes(p[1:3], "big"), int.from_bytes(p[3:5], "big")) for p in pdus if p and p[0] == 6]


def _fc5(pdus):
    return [(int.from_bytes(p[1:3], "big"), p[3:5] == b"\xff\x00") for p in pdus if p and p[0] == 5]


# ═════════════════════ 해독(controller/heater_service.py:656-700) ═════════════════════
def _regs(**kv):
    r = [0] * H.REG_BLOCK_COUNT
    for a, v in kv.items():
        r[int(a[1:]) - H.REG_BLOCK_START] = v
    return r


def test_decode_block_v1_rules():
    c = HeaterCfg(mv_abs_max=1300)
    lo, hi = float_to_words(3.0)
    regs = _regs(D110=2005, D112=2100, D113=9000, D116=0xFFF6, D119=2050, D120=2, D121=100, D126=9500, D127=50,
                 D128=10, D141=850, D118=1200, D146=lo, D147=hi, D160=0x1234)
    bits = [False] * H.COIL_COUNT
    bits[H.COIL_RUN - H.COIL_BASE] = bits[H.COIL_WATER_OK - H.COIL_BASE] = True
    st = decode_block(regs, bits, c, 5.0)
    assert st["ok"] and st["pv"] == pytest.approx(200.5) and st["sv"] == pytest.approx(210.0)
    assert st["cur_sv"] == pytest.approx(-1.0) and st["ramp_rate"] == 12 and st["slow_rate"] == 60
    assert st["holdback"] == pytest.approx(10.0) and st["mv_pct"] == pytest.approx((850 - 400) / 900 * 100)
    assert st["kp_act"] == 3.0 and st["marker"] == 0x1234 and st["run"] and st["water_ok"] and not st["vac_ok"]
    st = decode_block(_regs(D110=0xFFFF, D141=300), bits, c, 5.0)
    assert st["ok"] is False and st["pv"] is None and st["mv_pct"] == 0.0           # PV −1 = TC 이상, MV<MIN → 0%
    assert decode_block(_regs(), bits, HeaterCfg(session_mark_reg=0), 0.0)["marker"] is None


def test_write_seq_excludes_watchdog_and_marker():
    from v2.ch12.sim.heater_rig import FakeClock, LadderPlc
    clk = FakeClock()
    plc = LadderPlc(clk)
    drv = H.HeaterDriver(clk, plc, {"CH2_HEATER_SESSION_MARK_REG": 160}, True)
    assert plc.allowed_coils == {640, 643} and 160 in plc.allowed_regs and 115 not in plc.allowed_regs
    drv.w_reg(plc, H.REG_WD, 7)
    drv.w_marker(plc, 160, 0x2222)
    assert drv.write_seq == 0
    drv.w_reg(plc, H.REG_SV, 1000)
    drv.w_coil(plc, H.COIL_RUN, True)
    assert drv.write_seq == 2
    with pytest.raises(RuntimeError, match="링크"):
        drv.w_reg(None, H.REG_SV, 1)
    bench = H.HeaterDriver(clk, plc, {}, False)
    with pytest.raises(RuntimeError, match="BENCH_READ_ONLY"):
        bench.w_coil(plc, H.COIL_RUN, True)


def test_blocks_fields_and_budget():
    b = H.blocks()[0]
    names = [f.name for f in b.fields]
    assert b.name == "ch2.heater" and {"pv", "sv", "mv", "run", "itl", "wd", "owner", "target", "hold"} <= set(names)
    assert H.BUDGET["queries_per_tick"] == 0 and H.BUDGET["fits_1s"]


# ═════════════════════ 실제 PLC 스택 ═════════════════════
@pytest.fixture
def rig():
    rs = []

    def make(**kw):
        r = HeaterRig(**kw)
        rs.append(r)
        return r
    yield make
    for r in rs:
        r.done()


@pytest.mark.slow
def test_stack_push_bytes_ramp_and_rows(rig):
    r = _fast(rig(cfg=FAST))
    r.after(5)
    w = _fc6(r.p.pdus())
    # 래더 적용값(D146~D149)이 이미 Ti 300 이라 Ti 는 다시 쓰지 않음, Kp 20 은 상위 워드 먼저(heater_service.py:979-996)
    assert [a for a, _v in w[:10]] == [113, 126, 118, 120, 121, 127, 128, 143, 142, 160]
    vals = dict(w[:10])
    kp_lo, kp_hi = float_to_words(20.0)
    assert (vals[113], vals[126], vals[118], vals[120], vals[121], vals[127], vals[128]) == (9000, 9500, 1200, 10, 100, 0, 10)
    assert (vals[143], vals[142]) == (kp_hi, kp_lo) and vals[160] == r.eng._marker and r.sim.regs[160] == r.eng._marker
    assert r.eng.status()["kp_act"] == 20.0
    assert r.eng.is_ready() == (True, "")
    t_on = r.c.monotonic()
    ok, why = r.call(r.eng.turn_on, 150.0)
    assert ok, why
    after = [(t, p) for t, p in r.p.frames(t_on)]
    seq = [(p[0], int.from_bytes(p[1:3], "big")) for _t, p in after if p[0] in (5, 6)]
    i_ramp, i_sv, i_run = seq.index((6, 120)), seq.index((6, 112)), seq.index((5, 640))
    assert i_ramp < i_sv < i_run and seq[i_run + 1] == (6, 114)        # D120 → SV 출발점 → RUN → 워치독 1회
    d120 = [v for a, v in _fc6([p for _t, p in after]) if a == 120][0]
    assert d120 == max(2, -(-60 // 6) + 1)                                # max(2, ceil(속도/6)+1) = 11
    sv0 = [v for a, v in _fc6([p for _t, p in after]) if a == 112][0]
    assert abs(sv0 / 10.0 - 25.0) <= 0.2                                  # RUN 전에 SV = 현재 PV
    assert r.wait(lambda: r.eng.status()["pv"] is not None and r.eng.status()["pv"] >= 148.0, 1200)
    r.after(200)
    st = r.eng.status()
    assert st["run"] and abs(st["pv"] - 150.0) <= 2.0 and not r.eng.profiler.is_active()
    row = r.rows[-1]
    assert row["ch2.heater.q"] == "ok" and row["ch2.heater.run"] is True and abs(row["ch2.heater.pv"] - 150.0) <= 2.5
    assert row["ch2.heater.owner"] == "manual" and row["ch2.heater.target"] == 150.0 and row["ch2.heater.ready"] is True
    sv_writes = [v for a, v in _fc6(r.p.pdus(t_on)) if a == 112]
    assert all(b >= a for a, b in zip(sv_writes, sv_writes[1:]))           # 파이썬 램프 — SV 는 뒤로 가지 않는다
    assert all(v <= 1500 for v in sv_writes)


@pytest.mark.slow
def test_stack_watchdog_heartbeat_and_ladder_wd_err(rig):
    r = _fast(rig(cfg=FAST))
    r.after(5)
    assert r.call(r.eng.turn_on, 120.0)[0]
    t0 = r.c.monotonic()
    r.after(120)
    wd = [v for a, v in _fc6(r.p.pdus(t0)) if a == 114]
    assert len(wd) >= 20 and wd == list(range(wd[0], wd[0] + len(wd)))      # 5 s 마다 +1
    assert not r.sim.coils[646] and r.eng.status()["run"]
    r.eng._wd_fn = lambda s: None                                             # 프로그램이 멈춘 것처럼 하트비트를 끊음
    t1 = r.c.monotonic()
    assert r.wait(lambda: bool(r.cards("이상")), 60)
    assert 25.0 <= r.c.monotonic() - t1 <= 40.0                               # 래더 H2: 30 s 무변화 → WD_ERR
    assert r.sim.coils[646] and not r.sim.coils[640]
    assert r.cards("이상")[0][1] == "수동 · 이상 — 통신 워치독"
    assert r.wait(lambda: bool(r.cards("종료")), 10)
    assert r.cards("종료")[0][1] == "수동 · 이상 — 통신 워치독" and r.cards("종료")[0][2] == "FAIL"


@pytest.mark.slow
def test_stack_plc_restart_repush_marker(rig):
    r = _fast(rig(cfg=FAST))
    r.after(5)
    assert r.call(r.eng.turn_on, 120.0)[0]
    r.after(60)
    _quiet(r)
    assert r.eng.status()["run"]
    m0 = r.eng._marker
    t = r.sim.heater.T
    r.sim.restart()
    r.sim.heater.T = t
    assert r.wait(lambda: bool(r.cards("재기동 감지")) and bool(r.cards("종료")), 30)
    end = r.cards("종료")[0]
    assert "재기동" in end[1] and end[2] == "FAIL"
    r.after(3)
    assert r.eng._marker != m0 and r.sim.regs[160] == r.eng._marker and r.sim.regs[113] == 9000
    assert r.eng.owner is None and r.eng.is_ready()[0]


@pytest.mark.slow
def test_stack_noresp_a_turns_off(rig):
    r = rig()
    r.sim.heater.gain = 0.0                                                    # VARITAP 입력 0 V 흉내
    r.after(5)
    pub = []
    r.eng.add_listener(lambda st: pub.append(st.get("noresp")))
    assert r.call(r.eng.turn_on, 200.0)[0]
    assert r.wait(lambda: bool(r.cards("출력 무응답")), 1200)
    r.after(3)
    assert not r.sim.coils[640] and not r.eng.status()["run"] and r.eng.owner is None
    assert [x for x in pub if x][0].startswith("출력 무응답 — MV ")
    assert r.cards("종료")[0][1].endswith("· 출력 무응답")


@pytest.mark.slow
def test_stack_loss_b_turns_off_and_process_e832(rig):
    r = _fast(rig(cfg=FAST))
    r.after(5)
    r.eng.heater_set(150.0, None)
    assert r.wait(lambda: any(k == "set_ok" for k, *_ in r.eng.process_results), 10)
    assert r.wait(lambda: r.eng.sat.arrived, 1500)
    r.after(60)
    _quiet(r)
    r.sim.heater.gain = 0.0                                                    # 차단기·배선 끊김
    assert r.wait(lambda: bool(r.cards("출력 끊김")), 900)
    r.after(3)
    fails = [x for x in r.eng.process_results if x[0] == "failed"]
    assert len(fails) == 1 and fails[0][3] == "E832" and fails[0][2].startswith("공정 중 히터 출력 끊김 — ")
    assert not r.sim.coils[640]


@pytest.mark.slow
def test_stack_saturation_clamp_after_arrival(rig):
    r = _fast(rig(cfg=dict(FAST, CH2_HEATER_MV_LIMIT=600)))                  # 상한 600 = 정상상태 ≈ 142 °C
    r.after(5)
    assert r.call(r.eng.turn_on, 120.0)[0]
    assert r.wait(lambda: r.eng.sat.arrived, 1500)
    r.after(120)
    _quiet(r)
    r.sim.heater.tc_offset = -30.0                                             # TC 가 낮게 읽힘 → 상한에 물림(도달 후 새 포화)
    t0 = r.c.monotonic()
    assert r.wait(lambda: r.eng.sat.clamped is not None, 900)
    assert 115.0 <= r.c.monotonic() - t0 <= 135.0                             # HEATER_MV_SAT_SEC 120 s
    r.after(3)
    assert r.sim.regs[118] == r.eng.sat.clamped < 600 and r.cards("DAC 포화")[0][2] == "FAIL"
    assert r.sim.coils[640]


@pytest.mark.slow
def test_stack_write_during_row_discards_old_read(rig):
    """1초 읽기(RUN=0) 뒤 행 처리 전에 공정이 켬 → 그 읽기는 버리고 RUN=0 을 발행하지 않는다(E832 오판 없음)."""
    r = rig(cfg=FAST)
    r.after(5)
    pub = []
    r.eng.add_listener(lambda st: pub.append(bool(st.get("run"))))
    fired = []

    def pre(row):
        if not fired:
            fired.append(1)
            r.eng.heater_set(150.0, None)
    r.pre_row.append(pre)
    assert r.wait(lambda: bool(fired) and any(k == "set_ok" for k, *_ in r.eng.process_results), 10)
    pub_at = len(pub)
    r.after(10)
    assert all(pub[pub_at:]) and pub[-1] is True
    assert not [x for x in r.eng.process_results if x[0] == "failed"]


@pytest.mark.slow
def test_stack_bench_reads_without_writes(rig):
    r = rig(writes=False)
    r.after(5)
    assert not _fc6(r.p.pdus()) and not _fc5(r.p.pdus())
    ok, why = r.call(r.eng.turn_on, 100.0)
    assert not ok and "BENCH_READ_ONLY" in why
    r.after(5)
    assert not _fc6(r.p.pdus()) and not _fc5(r.p.pdus())
    row = r.rows[-1]
    assert row["ch2.heater.q"] == "ok" and row["ch2.heater.pv"] == pytest.approx(25.0, abs=0.2)


@pytest.mark.slow
def test_stack_resume_checks_and_journal(rig):
    r = _fast(rig(cfg=FAST))
    r.after(5)
    r.eng.heater_set(120.0, None)
    r.after(30)
    j = r.eng.state_for_journal()
    assert j["owner"] == "process" and j["run"] and j["watch"] and j["target"] == 120.0
    checks = {k: ok for k, ok, _t in r.eng.resume_checks(j)}
    assert checks == {"heater_run": True, "watchdog": True, "heater_target": True}
    bad = dict(j, run=False)
    assert {k: ok for k, ok, _t in r.eng.resume_checks(bad)}["heater_run"] is False

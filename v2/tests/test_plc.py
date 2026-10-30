"""PLC 드라이버 + 시뮬레이터 시험(가상 시계·SimTransport). 바이트는 v1 골든(v2/tools/v1golden_plc.py — v1 AsyncPLC 를
pymodbus 로 실제 실행해 받은 MBAP 뒤 PDU)과 비교한다(트랜잭션 id 는 비교에서 뺀다 — 결정 D02)."""
import json
from pathlib import Path

import pytest

from v2.common.worker import Status
from v2.ch12.drivers import plc as P
from v2.ch12.drivers import plc_map as PM
from v2.ch12.params import plc as PPLC
from v2.ch12.sim.plc_rig import PlcRig

GOLDEN = json.loads((Path(__file__).parent / "golden" / "dev_plc.json").read_text(encoding="utf-8"))
CASES = GOLDEN["plc"]["cases"]
FORCED = {0: 1000, 1: 500, 8: 700, 9: 40, 2: 600, 3: 100, 10: 2000, 11: 1027}


def _call(r: PlcRig, name, a):
    d = r.d
    table = {
        "read_coil": lambda: r.call(d.read_coil, a[0]),
        "write_coil": lambda: r.call(d.write_coil, a[0], a[1]),
        "read_reg": lambda: r.call(d.read_reg, a[0]),
        "write_reg": lambda: r.call(d.write_reg, a[0], a[1]),
        "read_coils_block": lambda: r.call(d.read_coils_block, a[0], a[1]),
        "read_regs_block": lambda: r.call(d.read_regs_block, a[0], a[1]),
        "write_switch": lambda: r.call(d.write_switch, a[0], a[1]),
        "press_switch": lambda: r.call(d.press_switch, a[0]),
        "pulse": lambda: r.call(d.pulse, a[0], a[1]),
        "read_bit": lambda: r.call(d.read_bit, a[0]),
        "write_coil_p_mirror": lambda: r.call(d.write_coil, a[0], a[1]),
        "gas": lambda: r.call(d.gas, a[0], a[1], on=a[2]),
        "main_shutter": lambda: r.call(d.main_shutter, a[0], open=a[1]),
        "gate_valve": lambda: r.call(d.gate_valve, a[0], open=a[1], momentary=a[2]),
        "lift_pin": lambda: r.call(d.lift_pin, up=a[0], momentary=a[1]),
        "rf_select": lambda: r.call(d.rf_select, rf_mode=a[0]),
        "power_select": lambda: r.call(d.power_select, on=a[0]),
        "buzzer_stop": lambda: r.call(d.buzzer_stop, momentary=a[0]),
        "set": lambda: r.call(d.set, a[0], a[1], ch=a[2]),
        "power_enable": lambda: r.call(d.power_enable, a[0], set_idx=a[1]),
        "power_write": lambda: r.call(d.power_write, a[0], write_idx=a[1]),
        "power_read": lambda: r.call(d.power_read, v_idx=a[0], i_idx=a[1]),
        "rf_enable": lambda: r.call(d.rf_enable, a[0], rf_ch=a[1]),
        "rf_write_w": lambda: r.call(d.rf_write_w, a[0], rf_ch=a[1]),
        "rf_apply": lambda: r.call(d.rf_apply, a[0], ensure_set=a[1], rf_ch=a[2]),
        "rf_read_fwd_ref": lambda: r.call(d.rf_read_fwd_ref, rf_ch=a[0], zeroing=a[1]),
    }
    return table[name]


SIMPLE = [c for c in CASES if c["name"] in {
    "read_coil", "write_coil", "read_reg", "write_reg", "read_coils_block", "read_regs_block", "write_switch",
    "press_switch", "pulse", "read_bit", "write_coil_p_mirror", "gas", "main_shutter", "gate_valve", "lift_pin",
    "rf_select", "power_select", "buzzer_stop", "set", "power_enable", "power_write", "power_read", "rf_enable",
    "rf_write_w", "rf_apply", "rf_read_fwd_ref"}]


# ───────── 주소 표 ─────────
def test_address_tables_match_v1():
    assert len(PM.COILS) == 124 and len(PM.REG_MAP) == 12 and len(PM.TIMER_MAP) == 25
    assert PM.COIL_MAP["SW_RF_SELECT"] == 214 and PM.COIL_MAP["SW_POWER_SELECT"] == 215
    assert PM.COIL_M_ADDR["SW_POWER_SELECT"] == "M00137"
    assert PM.P_MIRROR_MAP == {"L_GAUGE_A": 803, "L_ATM_SENSOR": 809, "L_R_P_OUT": 848, "L_R_V_OUT": 849,
                               "L_VENT_OUT": 852}
    assert PM.resolve("AR_2_GAS_SW") == 235 and PM.resolve("DCV_WRITE_2") == 6 and PM.resolve("L_ATM_SENSOR") == 809
    with pytest.raises(KeyError):
        PM.p_mirror_coil("P00050")
    assert len({PM.field_name(n) for n in PM.COIL_MAP}) == 124
    # v1 골든의 블록 계획(json 1) = (1,481)(1600,1)(2400,481)(3040,1)
    snap = next(c for c in CASES if c["name"] == "snapshot_all_coils_fast")
    _, ranges = PM.coil_ranges_for_plan(PM.COIL_MAP.values(), GOLDEN["plc"]["plan_idx"])
    assert [P.MT.pdu_read_coils(s, n).hex() for s, n in ranges] == snap["pdus"]
    assert GOLDEN["plc"]["cfg"]["PLC_COIL_LOG_BLOCK_PLAN"] == PPLC.PARAMS[[p.key for p in PPLC.PARAMS].index(
        "PLC_COIL_LOG_BLOCK_PLAN")].default


# ───────── v1 골든과 같은 PDU ─────────
@pytest.mark.parametrize("case", SIMPLE, ids=[f"{c['name']}{c['args']}" for c in SIMPLE])
def test_pdus_match_v1_golden(case):
    r = PlcRig()
    r.sim.forced_regs.update(FORCED)
    r.sim.coils[203] = True
    t0 = r.c.monotonic()
    fn = _call(r, case["name"], case["args"])
    if not case["ok"] and case["code"] == "ValueError":
        with pytest.raises(ValueError):
            fn()
        assert r.pdus(t0) == []
        r.done()
        return
    res = fn()
    assert [p.hex() for p in r.pdus(t0)] == case["pdus"]
    if case["ok"]:
        assert res.status == Status.OK, res.error
        if case["name"] in ("power_write", "rf_write_w", "rf_apply", "read_reg", "read_coil", "rf_read_fwd_ref"):
            assert res.value == case["value"]
        if case["name"] == "power_read":
            assert list(res.value) == pytest.approx(case["value"], abs=1e-5)
    else:
        assert res.status == Status.FAIL and "P 미러" in res.error          # E403, 보내지 않음
    if len(case["pdus"]) == 2 and case["pdus"][0][:2] == "05" and case["pdus"][0][:6] == case["pdus"][1][:6]:
        ts = [t for t, p in r.frames(t0)]
        gap_v1 = case["t"][1] - case["t"][0]
        assert ts[1] - ts[0] == pytest.approx(gap_v1, abs=0.03)              # 순간 스위치 폭(180 ms·지정 ms)
    r.done()


def test_short_block_response_padded_like_v1():
    r = PlcRig()
    for name, kind, a in (("read_coils_block_short", "coils", (1, 481)), ("read_regs_block_short", "regs", (0, 12))):
        g = next(c for c in CASES if c["name"] == name)
        t = r.c.monotonic()
        r.sim.add_fault("short", t, t + 0.4)
        r.sim.forced_regs.update(FORCED)
        res = r.call(r.d.read_coils_block if kind == "coils" else r.d.read_regs_block, *a)
        assert res.status == Status.OK
        v = res.value
        assert len(v) == a[1] and [p.hex() for p in r.pdus(t)] == g["pdus"]
        half = a[1] // 2
        assert all(x in (False, 0) for x in v[half:]) and all(x in (False, 0) for x in g["value"][half:])
        if kind == "regs":
            assert v[:4] == g["value"][:4]                                   # D0..D3 같은 값, D4 는 메모리 차이
        r.after(0.5)
    r.done()


# ───────── 타임아웃 정책(device/plc.py:1252-1367) ─────────
def test_timeout_policy_matches_v1():
    r = PlcRig()
    r.sim.forced_regs[8] = 700
    gold = {c["name"]: c for c in CASES if c["name"].startswith(("timeout_", "low_priority"))}
    opens0 = r.inner.opens

    def run(drop, fn):
        r.sim.drop_next = drop
        t = r.c.monotonic()
        res = fn()
        ts = [tt for tt, _ in r.frames(t)]
        return res, [p.hex() for p in r.pdus(t)], (ts[1] - ts[0]) if len(ts) > 1 else None
    res, pd, el = run(1, lambda: r.call(r.d.read_reg, 8))
    g = gold["timeout_once_retry_same_socket"]
    assert res.status == Status.OK and res.value == 700 and pd == g["pdus"] and r.inner.opens == opens0
    assert el == pytest.approx(g["t"][1] - g["t"][0], abs=0.05)              # 2.0 s 무응답 → 같은 소켓 재시도
    res, pd, _ = run(2, lambda: r.call(r.d.read_reg, 8))
    g = gold["timeout_two_fail_same_socket"]
    assert res.status == Status.TIMEOUT and res.reason == "DEVICE_NO_RESPONSE" and pd == g["pdus"]
    assert r.inner.opens == opens0 and r.d.io.consec == 2 and r.w.connected   # 무응답만으로는 안 닫는다
    res, pd, _ = run(1, lambda: r.call(r.d.read_reg, 8))
    g = gold["timeout_third_recreates_socket"]
    assert res.status == Status.OK and pd == g["pdus"] and g["conns"][0] != g["conns"][1]
    assert r.inner.opens == opens0 + 1 and r.d.io.reconnect_count == 1         # 연속 3회째 재생성
    assert "link_down" in r.kinds() and "link_up" in r.kinds()
    assert any("소켓 재생성 #1" in e.detail for e in r.events)
    res, pd, _ = run(1, lambda: r.call(r.d.read_regs_block, 0, 12, priority="low"))
    g = gold["low_priority_no_retry"]
    assert res.status == Status.TIMEOUT and pd == g["pdus"] and r.d.io.consec == 0 and r.d.io.consec_low == 1
    r.done()


def test_reconnect_backoff_5s_and_link_cards():
    r = PlcRig()
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t + 0.1, t + 21.0)
    r.after(0.2)
    res = r.call(r.d.write_coil, "MAIN_SHUTTER_1_SW", True, source="CH1")
    assert res.status in (Status.FAIL, Status.LINK_DOWN)
    r.after(25.0)
    conn = [e.t for e in r.wevents if e.kind in ("connect_failed", "connected")]
    gaps = [b - a for a, b in zip(conn, conn[1:])]
    assert gaps and all(g == pytest.approx(5.0 + 1.0, abs=0.2) for g in gaps[:-1])  # 백오프 5 s + 시도 3회(0.5 s 사이)
    assert r.kinds().count("link_down") == 1 and r.kinds().count("link_up") == 1
    up = next(e for e in r.events if e.kind == "link_up")
    assert up.data["card"]["title"] == "PLC 재연결" and "끊김" in up.data["card"]["subtitle"]
    down = next(e for e in r.events if e.kind == "link_down")
    assert down.data["card"]["status"] == "FAIL" and "192.168.1.2:502" in down.data["card"]["subtitle"]
    assert any(e.kind == "connect_failed" and e.code == "E401" for e in r.events)
    assert r.w.connected
    r.done()


# ───────── 1초 묶음 읽기 ─────────
def test_one_second_read_contents_rotation_and_budget():
    r = PlcRig()
    seen = set()
    for i in range(4):
        t = r.c.monotonic()
        res = r.read()
        assert res.status == Status.OK, res.error
        assert res.finished - res.started < 0.9 * 0.8
        seen |= {p.hex() for p in r.pdus(t)}
        v = res.value
        assert v["plc.m_v_1_sw"] is True and v["plc.dcv_read_0"] is not None and v["plc.coil_plan"] == 1
        r.after(1.0)
    snap = next(c for c in CASES if c["name"] == "snapshot_all_coils_fast")
    assert set(snap["pdus"]) <= seen                                          # v1 코일 로그 블록 전부(4 tick)
    assert {"030000000c", "03006e0033", "0102800013", "0103200050"} <= seen   # D0..D11·히터·P 미러
    assert P.BUDGET["queries_per_tick"] == 5 and P.BUDGET["fits_1s"]
    r.done()


def test_tick_rows_have_all_columns_and_own_quality():
    r = PlcRig(tick=True)
    r.after(6.0)
    rows = r.rows
    assert len(rows) >= 5
    last = rows[-1]
    assert last["plc.q"] == "ok" and last["plc.m_v_2_sw"] is True
    assert {last["plc.l_atm_sensor.q"], last["plc.gauge_1_a_interlock.q"]} <= {"ok", "stale"}
    assert all(f"plc.{PM.field_name(n)}" in last for n in PM.COIL_MAP)
    r.done()


def test_coil_log_backoff_skips_coils_but_reads_power():
    r = PlcRig()
    r.read()
    r.d.io.consec_low = int(r.cfg["PLC_COIL_LOG_BACKOFF_AFTER"])
    t = r.c.monotonic()
    v = r.read().value
    assert v["plc.m_v_1_sw"] is None and v["plc.dcv_read_0"] is not None
    assert all(p[0] == 3 or p[1:3] == bytes([2, 0x80]) for p in r.pdus(t))   # 코일 표 블록 없음(히터 코일만)
    r.done()


def test_coil_plan_demote_on_e403_and_promote():
    r = PlcRig()
    r.sim.coil_limit = 2401                     # (2400, 481) 블록은 범위 밖 → 예외 2(E403)
    fails = 0
    for _ in range(8):
        res = r.read()
        fails += res.status != Status.OK
        r.after(1.0)
    assert r.d.plan_idx == 2 and any(e.kind == "coil_plan" for e in r.events) and fails >= 1
    r.sim.coil_limit = None
    for _ in range(14):
        r.read()
        r.after(1.0)
    assert r.d.plan_idx == 1                     # 연속 성공 12회 → 기본 계획으로 승격
    assert any("promote after 12" in e.detail for e in r.events if e.kind == "coil_plan")
    r.done()


# ───────── 하트비트 ─────────
def test_heartbeat_every_6s_paused_for_robot():
    r = PlcRig()
    hb = bytes.fromhex("0100000001")
    t0 = r.c.monotonic()
    r.second(13)
    n = [p for p in r.pdus(t0) if p == hb]
    assert len(n) == 2                                                        # 6 s·12 s
    r.d.pause_watchdog()
    t1 = r.c.monotonic()
    r.second(13)
    assert [p for p in r.pdus(t1) if p == hb] == []
    r.d.resume_watchdog()
    t2 = r.c.monotonic()
    r.second(7)
    assert [p for p in r.pdus(t2) if p == hb]
    assert hb.hex() == next(c for c in CASES if c["name"] == "heartbeat_ping")["pdus"][0]
    r.done()


def test_heartbeat_failure_conn_change_messages():
    r = PlcRig()
    t = r.c.monotonic()
    r.sim.add_fault("no_response", t + 5.0, t + 40.0)
    r.second(20)
    msgs = [e.detail for e in r.events if e.kind == "conn_change"]
    assert msgs and msgs[0].startswith("[CH1&2] PLC 연결 끊김 0초 경과, 재연결 실패 (192.168.1.2:502)")
    r.second(30)
    msgs = [e.detail for e in r.events if e.kind == "conn_change"]
    assert msgs[-1] == "[CH1&2] PLC 재연결 성공 (192.168.1.2:502)"
    assert any("PLC 링크 복구" in e.detail for e in r.events)
    r.done()


# ───────── 쓰기 보호·출처 ─────────
def test_write_allowlist_and_p_mirror():
    r = PlcRig()
    t = r.c.monotonic()
    res = r.call(r.d.write_reg, 110, 5)
    assert res.status == Status.FAIL and res.reason == "COMMAND_BLOCKED"
    res = r.call(r.d.write_coil, "L_ATM_SENSOR", True)
    assert res.status == Status.FAIL and "읽기 전용" in res.error
    assert r.pdus(t) == []
    r.d.allow_writes(regs=[112], coils=[640])
    assert r.call(r.d.write_reg, 112, 1000).status == Status.OK and r.sim.regs[112] == 1000
    assert r.call(r.d.write_coil, 640, True).status == Status.OK
    r.done()


def test_bench_sends_no_writes():
    r = PlcRig(writes=False)
    r.second(7)
    for res in (r.call(r.d.write_coil, 234, True), r.call(r.d.power_write, 100.0, write_idx=0),
                r.call(r.d.rf_enable, True, rf_ch=2), r.call(r.d.press_switch, "G_V_1_OPEN_SW")):
        assert res.status == Status.REJECTED and res.reason == "BENCH_READ_ONLY"
    assert r.sim.write_pdus() == []
    assert r.read().status == Status.OK
    r.done()


def test_source_tags_and_proxy():
    r = PlcRig()
    ch2 = r.d.proxy("CH2")
    r.call(ch2.main_shutter, 2, open=True)
    r.call(r.d.proxy("ROBOT").press_switch, "L_PIN_UP_SW")
    logs = [e for e in r.events if e.kind == "log"]
    assert any(e.detail == "[CH2] set MAIN_SHUTTER_2_SW <- True (addr=234)" for e in logs)
    assert any(e.detail.startswith("[ROBOT] pulse L_PIN_UP_SW (addr=210, 180ms)") for e in logs)
    assert all(e.data.get("source") in ("CH2", "ROBOT") for e in logs if e.detail.startswith("["))
    r.done()


def test_command_gap_150ms_between_ops():
    r = PlcRig()
    t = r.c.monotonic()
    for i in range(4):
        r.d.write_coil(234, i % 2 == 0)
    r.after(2.0)
    ts = [tt for tt, _ in r.frames(t)]
    assert len(ts) == 4 and all(b - a >= 0.15 - 1e-6 for a, b in zip(ts, ts[1:]))
    r.done()


# ───────── 시뮬레이터 래더 모델 ─────────
def test_sim_gate_chuck_pin_loadlock_valves():
    r = PlcRig()
    r.call(r.d.gate_valve, 2, open=True, momentary=True)
    r.after(3.5)
    assert r.sim.coil("G_V_2_OPEN_LAMP") and not r.sim.coil("G_V_2_CLOSE_LAMP") and r.sim.ch[2].valves["gate"]
    r.call(r.d.write_coil, "Z_M_P_1_SW", True)
    r.call(r.d.write_coil, "Z_M_P_1_CW_SW", True)
    r.after(5.0)
    assert not any(r.sim.coil(f"Z1_{x}_LOCATION") for x in ("UP", "MID", "DOWN"))   # 이동 중엔 모두 꺼짐
    r.after(9.0)
    assert r.sim.coil("Z1_UP_LOCATION")
    r.call(r.d.press_switch, "L_PIN_UP_SW")
    r.after(2.5)
    assert r.sim.coil("L_PIN_UP_LAMP")
    r.call(r.d.write_coil, "L_R_P_SW", True)
    r.call(r.d.write_coil, "L_R_V_SW", True)
    r.after(100.0)
    assert r.sim.coil("L_VAC_READY_SW") and not r.sim.coil("L_R_P_SW") and not r.sim.coil("L_ATM")
    r.call(r.d.main_shutter, 1, open=True)
    assert r.sim.ch[1].valves["ms"] is True
    r.call(r.d.gas, 2, "AR", on=True)
    assert r.sim.ch[2].valves["ar"] is True
    r.done()


def test_sim_heater_ladder_watchdog_30s():
    r = PlcRig()
    r.d.allow_writes(regs=[112, 114], coils=[640])
    r.call(r.d.write_reg, 112, 1000)
    r.call(r.d.write_coil, 640, True)
    for i in range(10):
        r.call(r.d.write_reg, 114, i + 1)                                      # 5 s 마다 워치독 갱신
        r.after(5.0)
    assert r.sim.coils[640] and not r.sim.coils[646]
    r.after(31.0)
    assert r.sim.coils[646] and r.sim.coils[642] and not r.sim.coils[640]     # D114 30 s 무변화 → WD_ERR·RUN OFF
    v = r.read().value
    assert v is not None and r.d.last_poll.coils[646] is True and r.d.last_poll.regs[110] > 250
    r.done()

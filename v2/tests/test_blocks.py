"""v2/ch12/blocks.py·blocks_expand.py — 블록 검사·펼치기·건너뛰기/일시정지/이어가기 정의 표·끝 정지 강제·시퀀스 편집·
PROCESS_ADJUST 항목·스텝 형식 검사(process_steps.ProcessStep)."""
from __future__ import annotations

import pytest

from v2.ch12 import process_steps as ps
from v2.ch12.blocks import (ADJUST_ITEMS, AUTO_END_STOP_LABEL, BLOCK_DEFS, RESUME_CHECKS, SequenceState, by_type,
                            check_blocks, definition_table, edit_sequence, expand, template_blocks)
from v2.ch12.blocks_expand import segment_uses, template_from_rows
from v2.ch12.process_steps import SHUTDOWN_MUST_KEEP, SHUTDOWN_SKIPPABLE, StepError

CFG: dict = {}
CAPS2 = ps.Caps(dc=True, dc2=False, rf=True, dcp=True, rfp=True, heater=True, thickness=True)
CAPS1 = ps.Caps(dc=False, dc2=False, rf=False, dcp=True, rfp=True)


def B(t, label="", **p):
    return {"type": t, "params": p, "label": label}


def _ok(blocks, ch=2, caps=CAPS2):
    clean, errs, warns = check_blocks(blocks, CFG, ch, caps=caps)
    assert not errs, errs
    return clean


def _errs(blocks, ch=2, caps=CAPS2):
    return check_blocks(blocks, CFG, ch, caps=caps)[1]


ROW = dict(base_pressure=7e-6, working_pressure=2.0, process_time=10, shutter_delay=5, use_ar=True, ar_flow=20,
           use_dc_pulse=True, dc_pulse_power=80, dc_pulse_freq=20, dc_pulse_off_time=10.0, use_ms=True, use_g1=True,
           use_heater=True, heater_temp=200.0, power_change_time="10m", change_power_value="100")


# ───────────── 정의 표 ─────────────
RESUMABLE = {"heater_wait", "pressure_ctrl", "pressure_boost", "wait", "main", "thickness_main", "heater_hold", "test",
             "row_delay"}
PAUSABLE = {"heater_wait", "power_on", "wait", "main", "test", "row_delay"}


def test_definition_table():
    t = {d["type"]: d for d in definition_table()}
    assert set(t) == {d.type for d in BLOCK_DEFS}
    assert {k for k, d in t.items() if d["resumable"]} == RESUMABLE
    assert {k for k, d in t.items() if d["pausable"]} == PAUSABLE
    for d in BLOCK_DEFS:
        assert bool(d.skip_cleanup) == d.skippable, d.type          # 건너뛸 수 있으면 정리할 것도 적는다
        assert bool(d.pause) == d.pausable, d.type
        assert set(d.resume_checks) <= set(RESUME_CHECKS), d.type
        if d.resumable and d.touches:
            assert d.resume_checks, d.type
    # 장비 설정 쓰기·정지 순서는 이어가기·건너뛰기 없음
    for k in ("heater_set", "gas_prep", "gas_intro", "pressure_set", "shutter", "power_select", "power_on", "pulse_set",
              "end_stop"):
        assert not t[k]["resumable"] and not t[k]["skippable"], k
    assert not t["end_stop"]["pausable"]
    assert "현재" in t["power_on"]["pause"] or "지금 출력값 유지" in t["power_on"]["pause"]
    assert t["wait"]["pause"].startswith("타이머 정지")
    assert set(SHUTDOWN_MUST_KEEP) == {"power", "relay"} and "power" not in SHUTDOWN_SKIPPABLE


def test_resume_only_on_holding_steps():
    blocks, _ = template_blocks(ROW, 2, CFG, caps=CAPS2)
    steps, insts = expand(blocks, CFG, 2, caps=CAPS2)
    defs = by_type()
    held = {s.action for s in steps if s.holding}
    assert held <= {"DELAY", "MFC_CMD", "HEATER_WAIT", "HEATER_HOLD", "THICKNESS_WAIT", "ROW_DELAY"}
    assert all(s.params[0] == "WAIT_PRESSURE" for s in steps if s.holding and s.action == "MFC_CMD")
    for s in steps:                                             # holding 스텝은 늘 이어가기 가능 블록 안
        if s.holding:
            assert defs[insts[s.block].type].resumable, insts[s.block].type


# ───────────── 검사 ─────────────
def test_check_shape_errors():
    assert check_blocks([], CFG, 2)[1] == ["블록 목록이 비었다"]
    e = _errs([{"type": "nope"}, {"type": "wait", "params": {"sec": "x", "zz": 1}, "foo": 1}, B("main")])
    assert e[0] == "블록 1: 모르는 종류 'nope'"
    assert "블록 2(wait): 모르는 키 ['foo']" in e and "블록 2(wait): zz: 모르는 값" in e and "블록 2(wait): sec: 숫자여야 함" in e
    assert "블록 3(main): min: 값 필요" in e


def test_check_device_and_value_rules():
    e = _errs([B("heater_set", target_c=100.0), B("shutter", action="open", targets=["g1"]),
               B("gas_intro", n2=True, n2_flow=1.0), B("power_on", dc_power=10.0, rf_power=5.0)], ch=1, caps=CAPS1)
    assert "블록 1(heater_set): CH1 에서는 쓸 수 없음(CH2 전용)" in e
    assert "블록 2(shutter): targets: 이 챔버에 없는 건 셔터 ['g1'](PROCESS_GUN_SHUTTERS)" in e
    assert "블록 3(gas_intro): n2: CH1 은 N2 라인 없음" in e
    assert "블록 4(power_on): dc_power: 이 챔버는 DC 연속 미지원" in e and "블록 4(power_on): rf_power: 이 챔버는 RF 연속 미지원" in e
    e = _errs([B("power_on", dcp_power=80.0, rfp_power=50.0, dcp_freq_khz=100, dcp_off_us=4.5, rfp_freq_khz=40.0),
               B("heater_set", target_c=901.0), B("main", min=1.0, change_after_sec=10.0)])
    assert "블록 1(power_on): RF Pulse와 DC Pulse는 동시에 사용할 수 없습니다 (같은 시리얼 포트)" in e
    assert "블록 1(power_on): DC Pulse Off Time 4.5µs 는 100kHz 상한 4.0µs 초과(매뉴얼 p.13 그림3)" in e
    assert "블록 1(power_on): rfp_freq_khz: 허용 범위 0.001~30kHz 밖" in e
    assert "블록 2(heater_set): target_c: 901 > 900°C" in e
    assert "블록 3(main): change_after_sec·change_kind·change_power 는 함께 넣어야 함" in e
    assert _errs([B("heater_wait")]) == ["펼치기 실패: 히터 도달 대기: 목표 없음(앞에 히터 설정 블록이 없고 target_c 도 비었다)"]
    assert _errs([B("thickness_zero")], caps=CAPS1, ch=1) == ["블록 1(thickness_zero): 이 챔버 설정에서 꺼져 있음(SUPPORTS_THICKNESS)"]


def test_end_stop_cannot_skip_power_off():
    e = _errs([B("power_on", dc_power=10.0), B("end_stop", skip=["power", "gas_close"])])
    assert e == ["블록 2(end_stop): skip: 뺄 수 없는 단계 ['power'] — 뺄 수 있는 것 "
                 "['ms_close', 'flow_off', 'valve_open', 'gun_close', 'gas_close', 'mv_close']"]
    assert _errs([B("power_on", dc_power=10.0), B("end_stop", skip=["relay"])])[0].startswith("블록 2(end_stop): skip: 뺄 수 없는")
    clean = _ok([B("power_on", dc_power=10.0, rfp_power=None), B("end_stop", skip=["gas_close", "mv_close", "ms_close"])])
    steps, _ = expand(clean, CFG, 2, caps=CAPS2)
    acts = [(s.action, s.params) for s in steps]
    assert ("DC_POWER_STOP", None) in acts and not any(p and p[0] in ("AR", "O2", "N2", "MV") for a, p in acts if a == "PLC_CMD")
    # 실행 함수에 직접 넘겨도 파워 끄기는 빠지지 않는다
    u = ps.Uses(rfp=True)
    sd = ps.shutdown_steps(ps.step_cfg(CFG, 2), u, skip=("power", "relay", "ms_close"))
    assert [s.action for s in sd][:2] == ["RF_PULSE_STOP", "PLC_CMD"] and sd[1].params == ("SW_POWER_SELECT", False)


# ───────────── 펼치기 ─────────────
def test_auto_end_stop():
    steps, insts = expand(_ok([B("gas_intro", ar=True, ar_flow=10.0), B("power_on", rf_power=50.0), B("wait", sec=1.0)]), CFG, 2)
    assert insts[-1].auto and insts[-1].label == AUTO_END_STOP_LABEL and insts[-1].type == "end_stop"
    tail = [s.message for s in steps[insts[-1].first_step:]]
    assert tail[:2] == ["Main Shutter 닫기 (항상)", "RF Power Off"] and tail[-1] == "메인 밸브 닫기"
    assert "Ch1(AR) Flow Off" in tail and "Ch2(O2) Flow Off" not in tail        # 이 구간이 쓴 가스만
    steps, insts = expand(_ok([B("test", sec=5.0)]), CFG, 2)                     # 장비 안 쓰는 목록 — v1 TEST 행
    assert [s.action for s in steps] == ["DELAY"] and not any(i.auto for i in insts)
    steps, insts = expand(_ok([B("power_on", dc_power=5.0), B("end_stop"), B("wait", sec=2.0)]), CFG, 2)
    assert not insts[-1].auto and insts[-1].type == "wait"                      # 끝 정지 뒤 대기뿐이면 덧붙이지 않음


def test_segment_uses_per_row():
    rows = [{"Process_name": "a", "Ar": "T", "Ar_flow": "10", "use_dc_power": "T", "dc_power": "50", "gun1": "T",
             "process_time": "1", "main_shutter": "T"},
            {"Process_name": "w", "delay": "1"},
            {"Process_name": "b", "O2": "T", "O2_flow": "2", "use_rf_pulse": "T", "rf_pulse_power": "30", "gun2": "T",
             "process_time": "1"}]
    blocks, rts = template_from_rows(rows, 2, CFG, caps=CAPS2)
    assert [rt.kind for rt in rts] == ["process", "delay", "process"] and {b["row"] for b in blocks} == {1, 2, 3}
    steps, insts = expand(blocks, CFG, 2, caps=CAPS2)
    ends = [i for i in insts if i.type == "end_stop"]
    first = [s.message for s in steps[ends[0].first_step:ends[0].first_step + ends[0].n_steps]]
    second = [s.message for s in steps[ends[1].first_step:ends[1].first_step + ends[1].n_steps]]
    assert "DC1 Power Off" in first and "RF Pulse Off" not in first and "Gun Shutter G1 닫기" in first
    assert "RF Pulse Off" in second and "DC1 Power Off" not in second and "Gun Shutter G2 닫기" in second
    assert [s.action for s in steps if s.block == insts[[i.type for i in insts].index("row_delay")].index] == ["ROW_DELAY"]
    u = segment_uses([b for b in blocks if b.get("row") == 3], ps.step_cfg(CFG, 2))
    assert u.rfp and not u.dc and u.gases == {"O2": True} and u.guns == {"G2": True}


def test_template_e844_and_refs():
    with pytest.raises(StepError) as ei:
        template_blocks(dict(ROW, thickness_mode=True, thickness=100.0, dep_rate=0.05), 2, CFG, caps=CAPS2)
    assert ei.value.code == "E844"
    blocks, logs = template_blocks(ROW, 2, CFG, caps=CAPS2)
    assert logs[0] == ("Process", "공정 시작") and logs[1][1].startswith("히터 사용 — 목표 200°C") and logs[-1][1] == ps.SHUTDOWN_LOG
    steps, _ = expand(_ok(blocks), CFG, 2, caps=CAPS2)
    refs = {s.ref for s in steps if s.ref}
    keys = {a.key for a in ADJUST_ITEMS}
    assert refs <= keys
    assert {"mfc.ar_flow", "mfc.wp", "dcp.power", "heater.target", "process.main_remaining"} <= refs
    split = [s for s in steps if s.action == "DC_PULSE_SET"]
    assert len(split) == 1 and split[0].message == "DC Pulse Power 변경 → 100.0W (t=600.0s, Main)" and split[0].polling


def test_adjust_items_shape():
    assert len({(a.device, a.item) for a in ADJUST_ITEMS}) == len(ADJUST_ITEMS)
    for a in ADJUST_ITEMS:
        assert a.param.required and a.apply and a.when_live and a.public()["key"] == a.key


# ───────────── 시퀀스 편집 ─────────────
def _seq():
    return _ok([B("gas_intro", ar=True, ar_flow=10.0), B("power_on", dc_power=20.0), B("wait", "Shutter Delay", sec=60.0),
                B("main", min=1.0), B("end_stop")])


def test_edit_only_unstarted_blocks():
    seq = _seq()
    r = edit_sequence(seq, 2, {"op": "insert", "at": 1, "block": B("wait", sec=1.0)}, CFG, 2)
    assert not r.ok and r.reason == "EDIT_STARTED_BLOCK"
    assert edit_sequence(seq, 2, {"op": "delete", "at": 0}, CFG, 2).reason == "EDIT_STARTED_BLOCK"
    assert edit_sequence(seq, 2, {"op": "move", "from": 3, "to": 1}, CFG, 2).reason == "EDIT_STARTED_BLOCK"
    assert edit_sequence(seq, 2, {"op": "swap"}, CFG, 2).reason == "EDIT_BAD_OP"
    assert edit_sequence(seq, 2, {"op": "delete", "at": 9}, CFG, 2).reason == "EDIT_BAD_OP"
    r = edit_sequence(seq, 2, {"op": "insert", "at": 2, "block": B("wait", sec=-1.0)}, CFG, 2)
    assert r.reason == "EDIT_INVALID" and r.errors == ["블록 3(wait): sec: -1.0 < 0"]
    r = edit_sequence(seq, 2, {"op": "insert", "at": 2, "block": B("wait", "추가 대기", sec=30.0)}, CFG, 2, rev=4)
    assert r.ok and r.rev == 5 and r.blocks[2]["label"] == "추가 대기" and r.blocks[:2] == seq[:2]
    r = edit_sequence(seq, 2, {"op": "move", "from": 3, "to": 2}, CFG, 2)
    assert r.ok and [b["type"] for b in r.blocks] == ["gas_intro", "power_on", "main", "wait", "end_stop"]


def test_edit_keeps_power_off():
    seq = _seq()
    r = edit_sequence(seq, 1, {"op": "delete", "at": 4}, CFG, 2)                 # 끝 정지를 지워도
    assert r.ok
    steps, insts = expand(r.blocks, CFG, 2)
    assert insts[-1].auto and "DC1 Power Off" in [s.message for s in steps[insts[-1].first_step:]]
    r = edit_sequence(seq, 1, {"op": "insert", "at": 5, "block": B("power_on", rf_power=10.0)}, CFG, 2)
    steps, insts = expand(r.blocks, CFG, 2)                                     # 끝 정지 뒤에 켠 파워도 끈다
    assert insts[-1].auto and [s.action for s in steps[insts[-1].first_step:]][1] == "RF_POWER_STOP"


def test_sequence_state_records():
    st = SequenceState.start(_seq())
    assert st.edit({"op": "delete", "at": 0}, 1, CFG, 2, origin="web").reason == "EDIT_STARTED_BLOCK"
    assert st.edit({"op": "delete", "at": 2}, 1, CFG, 2, origin="local").ok
    rec = st.record()
    assert rec["rev"] == 1 and len(rec["original"]) == 5 and len(rec["blocks"]) == 4
    assert [(h["ok"], h["origin"]) for h in rec["history"]] == [(False, "web"), (True, "local")]


# ───────────── 스텝 형식 ─────────────
def test_process_step_validation_codes():
    with pytest.raises(StepError) as e:
        ps.ProcessStep("DELAY", "x")
    assert e.value.code == "E214"
    with pytest.raises(StepError) as e:
        ps.ProcessStep("DELAY", "x", duration=1, parallel=True)
    assert e.value.code == "E215"
    for act, kw, code in (("PLC_CMD", {"params": ("MS",)}, "E216"), ("MFC_CMD", {"params": ("X", [])}, "E217"),
                          ("OES_RUN", {"params": (1,)}, "E218"), ("DC_PULSE_START", {}, "E219"),
                          ("DC_PULSE_START", {"value": 1.0, "params": (1,)}, "E221"), ("RF_PULSE_START", {}, "E220"),
                          ("RF_PULSE_START", {"value": 1.0}, "E222"), ("THICKNESS_ZERO", {}, "E847"),
                          ("THICKNESS_WAIT", {"value": 1.0}, "E847")):
        with pytest.raises(StepError) as e:
            ps.ProcessStep(act, "x", **kw)
        assert e.value.code == code, act
    with pytest.raises(ValueError):
        ps.ProcessStep("IG_CMD", "x")
    assert set(ps.ACTIONS) >= {s.action for s in expand(_seq(), CFG, 2)[0]}
    assert ps.norm_plc_name(" main_valve ") == "MV" and ps.norm_plc_name("argon") == "AR"


def test_emergency_and_force_all():
    sc = ps.step_cfg(CFG, 2)
    u = ps.Uses(dc=True, rfp=True, gases={"AR": True}, power_select=True)
    em = ps.emergency_steps(sc, u)
    assert all(s.no_wait for s in em) and [s.parallel for s in em if s.action.endswith("STOP")] == [True, True]
    assert [s.message for s in em][-3:] == ["[긴급] Ch1(AR) FLOW OFF", "[긴급] AR 가스 즉시 차단", "[긴급] 메인 밸브 즉시 닫기"]
    full = ps.shutdown_steps(sc, ps.Uses(), force_all=True)
    assert [s.action for s in full][1:6] == ["DC_POWER_STOP", "DC2_POWER_STOP", "RF_POWER_STOP", "DC_PULSE_STOP",
                                              "RF_PULSE_STOP"]
    assert [s.message for s in full if s.message.startswith("Gun Shutter")] == [f"Gun Shutter G{i} 닫기" for i in (1, 2, 3)]


def test_check_is_idempotent_and_json_safe():
    import json
    blocks, _ = template_blocks(ROW, 2, CFG, caps=CAPS2)
    clean = _ok(blocks)
    again = _ok(json.loads(json.dumps(clean)))
    assert again == clean
    assert expand(again, CFG, 2, caps=CAPS2)[0] == expand(blocks, CFG, 2, caps=CAPS2)[0]

"""가스 단계 설정(PROCESS_GAS_STEP_LEVELS, v1 main 27decba) — 가스 주입 블록 펼치기·설정 검사·시작 거부(E211)·실제 실행.

v1 tests/test_gas_step_levels.py 와 같은 경우를 v2 로 옮겼다.
- 펼치기: 목표보다 작은 중간값만 작은 순서대로 FLOW_SET → FLOW_ON(문구 '(단계 i/n)'·'(… 도달 대기)'), 중간값이 없으면 예전과 글자 하나까지 같음.
- 설정 검사: v1 _gas_step_levels 를 실제로 부른 골든(v2/tests/golden/gas_step_levels.json)과 결과·문구가 같음, 파라미터 표가 저장 때 거부(D11).
- 시작 거부: 잘못된 설정이면 Chuck 이동 전에 E211(로그·챗·결과), O2 를 안 쓰는 레시피도 막음(설정 자체가 잘못됨).
- 실행(시험대): CH2 O2 10 sccm → MFC 에 'Q2 90.0'·L21·'Q2 95.0'·L21·'Q2 100.0'·L21 순서, 중간 단계 도달 실패면 다음 단계 없이 중단.
- 공정 중 값 바꾸기: 아직 안 한 가스 주입은 중간값 구성이 같은 범위에서만(D417).
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from v2.ch12 import blocks as BK
from v2.ch12 import process_steps as ps
from v2.ch12.process_adjust import AdjustMixin
from v2.ch12.process_sequence import build_sequence
from v2.tests import proc_rig as R

GOLD = json.loads((Path(__file__).parent / "golden" / "gas_step_levels.json").read_text(encoding="utf-8"))
O2_LV = {"O2": [9.0, 9.5]}
CAPS2 = ps.Caps(dc=True, dc2=False, rf=True, dcp=True, rfp=True, heater=True, thickness=True)
CAPS1 = ps.Caps(dc=False, dc2=False, rf=False, dcp=True, rfp=True)


def _cfg(ch: int, levels=None) -> dict:
    return {} if levels is None else {f"CH{ch}_PROCESS_GAS_STEP_LEVELS": json.dumps(levels)}


def _recipe(**kw) -> dict:
    p = dict(base_pressure=1e-5, working_pressure=3, process_time=1, shutter_delay=0, use_ar=True, ar_flow=20,
             use_dc_power=True, dc_power=100, use_g1=True)
    p.update(kw)
    return p


def _seq(ch, cfg, params):
    return build_sequence(params, ch, cfg, caps=CAPS2 if ch == 2 else CAPS1)


def _gas_part(seq, ch):
    """메인 밸브 열기 다음부터 압력 제어 시작 전까지 중 채널 ch 의 FLOW_SET/FLOW_ON (메시지, 명령, 값) — v1 시험과 같은 자르기."""
    out, started = [], False
    for s in seq:
        if s.message == "메인 밸브 열기":
            started = True
            continue
        if started and s.message.startswith("압력 제어("):
            break
        if started and s.action == "MFC_CMD" and s.params[0] in ("FLOW_SET", "FLOW_ON") and s.params[1].get("channel") == ch:
            out.append((s.message, s.params[0], s.params[1].get("value")))
    return out


def _rows(seq):
    return ps.to_golden(seq)


TABLE = {
    8.0: [("Ch2(O2) GAS 8.0sccm 설정", "FLOW_SET", 8.0),
          ("Ch2(O2) GAS 공급 시작", "FLOW_ON", None)],
    9.0: [("Ch2(O2) GAS 9.0sccm 설정", "FLOW_SET", 9.0),
          ("Ch2(O2) GAS 공급 시작", "FLOW_ON", None)],
    9.2: [("Ch2(O2) GAS 9.0sccm 설정 (단계 1/2)", "FLOW_SET", 9.0),
          ("Ch2(O2) GAS 공급 시작 (9.0sccm 도달 대기)", "FLOW_ON", None),
          ("Ch2(O2) GAS 9.2sccm 설정 (단계 2/2)", "FLOW_SET", 9.2),
          ("Ch2(O2) GAS 9.2sccm 도달 대기", "FLOW_ON", None)],
    9.5: [("Ch2(O2) GAS 9.0sccm 설정 (단계 1/2)", "FLOW_SET", 9.0),
          ("Ch2(O2) GAS 공급 시작 (9.0sccm 도달 대기)", "FLOW_ON", None),
          ("Ch2(O2) GAS 9.5sccm 설정 (단계 2/2)", "FLOW_SET", 9.5),
          ("Ch2(O2) GAS 9.5sccm 도달 대기", "FLOW_ON", None)],
    10.0: [("Ch2(O2) GAS 9.0sccm 설정 (단계 1/3)", "FLOW_SET", 9.0),
           ("Ch2(O2) GAS 공급 시작 (9.0sccm 도달 대기)", "FLOW_ON", None),
           ("Ch2(O2) GAS 9.5sccm 설정 (단계 2/3)", "FLOW_SET", 9.5),
           ("Ch2(O2) GAS 9.5sccm 도달 대기", "FLOW_ON", None),
           ("Ch2(O2) GAS 10.0sccm 설정 (단계 3/3)", "FLOW_SET", 10.0),
           ("Ch2(O2) GAS 10.0sccm 도달 대기", "FLOW_ON", None)],
}


# ───────────────────────── 펼치기: 표대로 ─────────────────────────
@pytest.mark.parametrize("o2", sorted(TABLE))
def test_ch2_o2_steps_follow_table(o2):
    seq = _seq(2, _cfg(2, O2_LV), _recipe(use_o2=True, o2_flow=o2))
    assert _gas_part(seq, 2) == TABLE[o2]
    names = [s.message for s in seq]
    i = names.index("O2 밸브 열기")                                # O2 밸브 열기 바로 뒤에 단계들이 붙는다
    assert names[i + 1:i + 1 + len(TABLE[o2])] == [t[0] for t in TABLE[o2]]
    sets = [s for s in seq if s.action == "MFC_CMD" and s.params[0] == "FLOW_SET" and s.params[1]["channel"] == 2]
    assert [s.ref for s in sets] == [None] * (len(sets) - 1) + ["mfc.o2_flow"]   # 공정 중 값 바꾸기는 목표 FLOW_SET 에만


@pytest.mark.parametrize("o2", [8.0, 9.0])
def test_no_mid_levels_identical_to_before(o2):
    """중간값이 없으면 단계 개수·문구·값이 설정이 아예 없을 때와 글자 하나까지 같다."""
    p = _recipe(use_o2=True, o2_flow=o2)
    assert _rows(_seq(2, _cfg(2, O2_LV), p)) == _rows(_seq(2, _cfg(2, {}), p))


def test_o2_unused_no_steps():
    p = _recipe()
    seq = _seq(2, _cfg(2, O2_LV), p)
    assert _gas_part(seq, 2) == []
    assert _rows(seq) == _rows(_seq(2, _cfg(2, {}), p))
    assert not any("(단계 " in s.message or "sccm 도달 대기" in s.message for s in seq)


def test_ar_unchanged():
    p = _recipe(use_o2=True, o2_flow=10.0)
    assert _gas_part(_seq(2, _cfg(2, O2_LV), p), 1) == [("Ch1(AR) GAS 20.0sccm 설정", "FLOW_SET", 20.0),
                                                        ("Ch1(AR) GAS 공급 시작", "FLOW_ON", None)]
    a = [r for r in _rows(_seq(2, _cfg(2, O2_LV), p)) if "Ch2(O2) GAS" not in r[1]]
    b = [r for r in _rows(_seq(2, _cfg(2, {}), p)) if "Ch2(O2) GAS" not in r[1]]
    assert a == b


def test_ch1_default_is_unchanged_even_for_o2_10():
    """CH1 기본값({} — v1 config_ch1 이 config_common 을 복사) → O2 10.0 이어도 그대로. 키가 없을 때·표 기본값 모두."""
    from v2.ch12.params import table
    defaults = {p.key: p.default for p in table().params}
    assert defaults["CH1_PROCESS_GAS_STEP_LEVELS"] == "{}"
    p = _recipe(use_o2=True, o2_flow=10.0, use_dc_power=False, use_g1=False, use_dc_pulse=True, dc_pulse_power=80)
    for cfg in ({}, defaults):
        assert _gas_part(_seq(1, cfg, p), 2) == [("Ch2(O2) GAS 10.0sccm 설정", "FLOW_SET", 10.0),
                                                 ("Ch2(O2) GAS 공급 시작", "FLOW_ON", None)]


def test_ch2_default_10_goes_9_9p5_10():
    """CH2 기본값(키가 없을 때 = v1 config_ch2 {"O2": [9.0, 9.5]}, 표 기본값도 같음) → 10.0 → 9.0 → 9.5 → 10.0."""
    from v2.ch12.params import table
    defaults = {p.key: p.default for p in table().params}
    assert json.loads(defaults["CH2_PROCESS_GAS_STEP_LEVELS"]) == O2_LV
    for cfg in ({}, defaults):
        assert _gas_part(_seq(2, cfg, _recipe(use_o2=True, o2_flow=10.0)), 2) == TABLE[10.0]


def test_int_levels_are_floats():
    assert _gas_part(_seq(2, _cfg(2, {"O2": [9, 9.5]}), _recipe(use_o2=True, o2_flow=10.0)), 2) == TABLE[10.0]


def test_block_expand_same_as_sequence():
    """블록 펼치기(blocks.expand — 엔진이 쓰는 것)도 같은 단계(같은 함수)."""
    from v2.ch12.blocks_expand import template_from_row
    from v2.ch12.recipe import TEMPLATE_COLUMNS
    row = {c: "" for c in TEMPLATE_COLUMNS}
    row.update({"Process_name": "o2", "base_pressure": "1e-5", "working_pressure": "3", "process_time": "1",
                "shutter_delay": "0", "Ar": "T", "Ar_flow": "20", "O2": "T", "O2_flow": "10", "use_dc_power": "T",
                "dc_power": "100", "gun1": "T", "main_shutter": "T"})
    cfg = _cfg(2, O2_LV)
    rt = template_from_row(row, 2, cfg, caps=CAPS2)
    steps, _ = BK.expand(rt.blocks, cfg, 2, caps=CAPS2)
    assert _gas_part(steps, 2) == TABLE[10.0]


# ───────────────────────── 설정 검사 — v1 골든과 같음 ─────────────────────────
@pytest.mark.parametrize("case", GOLD["cases"], ids=[f"ch{c['ch']}-{c['raw_repr']}" for c in GOLD["cases"]])
def test_levels_check_matches_v1(case):
    raw = case["raw"]
    if isinstance(raw, dict) and "__tuple__" in raw:
        raw = {"O2": tuple(raw["__tuple__"])}
    gas_info = ps.GAS_INFO_DEFAULT
    levels, err = ps.gas_step_levels(raw, gas_info)
    if case["code"] is None:
        assert err == "" and {k: list(v) for k, v in levels.items()} == case["levels"]
    else:
        assert case["code"] == "E211" and err == case["detail"] == case["error_text"] and levels == {}
    # v2 설정은 JSON 글자로 온다 — 글자로 줘도 같은 판정(문구의 '현재 값' 은 읽은 값의 repr — v1 과 같음)
    if not isinstance(case["raw"], str) and case["raw"] is not None and "__tuple__" not in str(case["raw"]):
        lv2, err2 = ps.gas_step_levels(json.dumps(case["raw"]), gas_info)
        assert (lv2, err2) == (levels, err)


BAD = [{"O2": [9.5, 9.0]}, {"O2": [9.0, 9.0]}, {"O2": [0, 9.0]}, {"O2": [-1.0]}, {"O2": [9.0, "9.5"]}, {"O2": [True]},
       {"O2": 9.0}, {"o2": [9.0]}, {"H2": [9.0]}, [9.0, 9.5]]


@pytest.mark.parametrize("bad", BAD, ids=[repr(b) for b in BAD])
def test_bad_levels_rejected_by_param_table_and_ignored_by_expand(bad):
    """잘못된 값: 파라미터 표가 저장 때 거부(D11 — 키 사이 규칙 C06), 표를 거치지 않은 값은 펼치기가 단계 없이(시작 전 검사가 E211)."""
    from v2.ch12.params import table
    t = table()
    vals = {p.key: p.default for p in t.params}
    vals["CH2_PROCESS_GAS_STEP_LEVELS"] = json.dumps(bad)
    rule = next(r for r in t.rules if r.id == "C06")
    msg = rule.check(vals)
    assert msg and msg.startswith("설정 오류(PROCESS_GAS_STEP_LEVELS): ")
    sc = ps.step_cfg(_cfg(2, bad), 2)
    assert sc.gas_steps == {} and sc.gas_steps_error == msg
    assert _gas_part(_seq(2, _cfg(2, bad), _recipe(use_o2=True, o2_flow=10.0)), 2) == TABLE[8.0][:0] + [
        ("Ch2(O2) GAS 10.0sccm 설정", "FLOW_SET", 10.0), ("Ch2(O2) GAS 공급 시작", "FLOW_ON", None)]


def test_good_levels_no_error():
    for lv in (O2_LV, {}, None):
        assert ps.gas_step_levels(lv, ps.GAS_INFO_DEFAULT)[1] == ""
    assert ps.step_cfg({}, 2).gas_steps == {"O2": (9.0, 9.5)} and ps.step_cfg({}, 1).gas_steps == {}


# ───────────────────────── 시험대: 시작 거부(E211)·실제 실행·중간 단계 실패 ─────────────────────────
CH2_O2 = dict(base_pressure=1e-5, working_pressure=3, process_time=0.05, shutter_delay=0.05, use_ar=True, ar_flow=20,
              use_o2=True, o2_flow=10, use_dc_pulse=True, dc_pulse_power=60, dc_pulse_freq=50, dc_pulse_off_time=5,
              use_ms=True, use_g1=True)


def _run2(tmp, cfg=None, params=None, extra=(), max_ticks=1500, prep=None):
    rig = R.make(tmp, chambers=("ch2",), cfg=cfg)
    e = rig.eng["ch2"]
    host = []
    e.on_host_start = lambda ok, msg: host.append((ok, msg))
    if prep:
        prep(rig, e)
    p = dict(CH2_O2, **(params or {}))
    R.run(rig, lambda: not e.busy and any(x.kind == "run_end" for x in e.events), max_ticks=max_ticks,
          hooks=[R.at(2, lambda: e.start(params=p, owner="local", source="local"))] + list(extra))
    return rig, e, host


def _mfc_sent(rig):
    return [d.decode("ascii").strip() for _t, d in rig.sims["ch2.mfc"].received]


def test_bad_levels_block_start_before_chuck_e211(tmp_path):
    """잘못된 설정 → Chuck 이동 전 시작 거부(E211): 로그 '[CH2] 가스 단계 설정 오류 [E211] … → 공정 시작 차단'·챗 장비 오류(E211)·
    결과 run_end error E211·호스트 실패 응답, 가스 명령은 하나도 나가지 않음. O2 를 안 쓰는 레시피도 막는다."""
    bad = '{"O2": [9.5, 9.0]}'
    for params in ({}, {"use_o2": False, "o2_flow": 0}):
        rig, e, host = _run2(tmp_path / str(len(params)), cfg={"CH2_PROCESS_GAS_STEP_LEVELS": bad}, params=params)
        try:
            msg = ps.gas_step_levels(bad, ps.GAS_INFO_DEFAULT)[1]
            assert msg == "설정 오류(PROCESS_GAS_STEP_LEVELS): O2 값은 작은 순서(오름차순)·중복 없이 적어야 합니다 — 현재 값 {'O2': [9.5, 9.0]}"
            assert R.has_log(e, f"[CH2] 가스 단계 설정 오류 [E211] {msg} → 공정 시작 차단")
            chat = [x for x in e.events if x.kind == "chat" and x.code == "E211"]
            assert chat and chat[0].detail.startswith(msg + " → 공정 시작 차단 (공정: ")
            end = [x for x in e.events if x.kind == "run_end"][-1]
            assert (end.code, end.detail) == ("E211", f"gas step config error [E211]: {msg}")
            assert host and host[-1][0] is False
            assert not R.has_log(e, "Chuck") and not R.has_log(e, "장비 연결 확인 완료")
            assert not [s for s in _mfc_sent(rig) if s.startswith(("Q1 ", "Q2 ", "L11", "L21"))]
        finally:
            pass


def test_runner_sends_steps_in_order(tmp_path):
    """CH2 O2 10 sccm(×10 → %FS): 'Q2 90.0'·L21(3회 연속 도달)·'Q2 95.0'·L21·'Q2 100.0'·L21 순서로 보내고 공정이 끝까지 간다."""
    rig, e, _host = _run2(tmp_path)
    try:
        end = [x for x in e.events if x.kind == "run_end"][-1]
        assert end.data.get("result") == "done", (end.code, end.detail)
        o2 = [s for s in _mfc_sent(rig) if s.startswith(("Q2 ", "L21"))]
        assert o2[:6] == ["Q2 90.0", "L21", "Q2 95.0", "L21", "Q2 100.0", "L21"]
        msgs = [x[3] for x in e.traces[-1] if "Ch2(O2) GAS" in x[3]]
        assert msgs == [t[0] for t in TABLE[10.0]]                               # 실제로 돈 스텝 = 표 그대로
    finally:
        pass


def test_mid_step_failure_aborts_before_next_step(tmp_path):
    """9.0 단계 FLOW_ON 이 도달하지 못하면(장비가 L21 을 무시 → 유량 0 → 180회 시간 초과) 다음 단계(9.5·10)로 가지 않고 공정 중단,
    파워도 켜지 않음. 종료 절차는 예전 그대로(O2 FLOW_OFF 'L20' 이 나간다)."""
    def prep(rig, e):
        rig.sims["ch2.mfc"].ignore_writes = {"L2"}
    rig, e, _host = _run2(tmp_path, prep=prep, max_ticks=1500)
    try:
        end = [x for x in e.events if x.kind == "run_end"][-1]
        assert end.data.get("result") in ("error", "aborted", "failed"), (end.code, end.detail)
        sent = _mfc_sent(rig)
        assert "Q2 90.0" in sent and "Q2 95.0" not in sent and "Q2 100.0" not in sent
        assert "L20" in sent[sent.index("Q2 90.0"):]
        ran = [x[2] for x in e.traces[-1]]
        assert "MFC_CMD" in ran and "DC_PULSE_START" not in ran                   # 파워 켜기 스텝까지 가지 않음
    finally:
        pass


# ───────────────────────── 공정 중 값 바꾸기(D417) ─────────────────────────
def test_future_flow_adjust_only_within_same_mid_levels():
    """아직 안 한 가스 주입의 O2 목표를 바꿀 때 중간값 구성이 바뀌면 거부(10 → 9.2 = [9.0, 9.5] → [9.0]), 같으면 허용(10 → 12)."""
    ai = BK.adjust_item("mfc", "o2_flow")
    steps = _seq(2, _cfg(2, O2_LV), _recipe(use_o2=True, o2_flow=10.0))
    fut = [s for s in steps if s.ref == "mfc.o2_flow"]
    assert len(fut) == 1 and fut[0].params[1]["value"] == 10.0
    me = SimpleNamespace(cfg=_cfg(2, O2_LV), n=2, _step_value=AdjustMixin._step_value)
    assert AdjustMixin._gas_step_conflict(me, ai, fut, 12.0) == ""
    assert AdjustMixin._gas_step_conflict(me, ai, fut, 9.6) == ""
    why = AdjustMixin._gas_step_conflict(me, ai, fut, 9.2)
    assert "O2 10 → 9.2 sccm" in why and "[9.0, 9.5] → [9.0]" in why
    assert AdjustMixin._gas_step_conflict(me, ai, fut, 8.0)
    assert AdjustMixin._gas_step_conflict(me, ai, [], 8.0) == ""                    # 이미 지난 가스 주입은 해당 없음(live)
    assert AdjustMixin._gas_step_conflict(me, BK.adjust_item("mfc", "ar_flow"), fut, 5.0) == ""   # AR 은 단계 없음

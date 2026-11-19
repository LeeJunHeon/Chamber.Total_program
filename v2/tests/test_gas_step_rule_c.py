"""가스 단계 규칙 C(사용자 결정 2026-11-19, D420) — 단계 설정이 있는 가스는 레시피 시작이든 공정 중 변경이든 '올라갈 때 중간값을 작은
순서대로 거친다, 내릴 때는 바로'. 시험대(sim 장비 + 실제 엔진 — proc_rig) CH2 O2(단계 9.0·9.5)로:
- 아직 안 한 가스 주입의 목표를 바꾸면 거부하지 않고 다시 펼친다: 10 → 9.2 = [9.0] → 9.2, 10 → 12 = 9.0 → 9.5 → 12, 10 → 8 = 8.
- 흐르는 중 올리면 지금 설정값 < 중간값 < 새 값 을 작은 순서대로(각 도달 판정) 뒤 새 값: 8 → 10 = 9.0 → 9.5 → 10, 9.2 → 10 = 9.5 → 10,
  10 → 12 = 바로 12. 내리면(10 → 8) 바로.
- 중간 단계 도달 실패 → 공정 중단('[MFC - …]', 가스 주입 실패와 같은 규칙), 단계 진행 중 같은 가스 재요청 → 거부(ADJUST_GAS_STEP).
- 단계 없는 가스(Ar)는 예전과 같음. 시퀀스 편집·재시작 뒤 다시 펼치기는 그때 다시 펼친 모양 그대로, 단계 진행 중 재시작이면 이어가지 않음.
MFC 장비값은 CH2 O2 스케일 ×10(9.0 sccm → 'Q2 90.0').
"""
from __future__ import annotations

from v2.ch12 import blocks as BK
from v2.ch12 import process_steps as PS
from v2.tests import proc_rig as R

BASE = dict(base_pressure=1e-5, working_pressure=3, process_time=2.0, shutter_delay=0.05, use_ar=True, ar_flow=20,
            use_o2=True, o2_flow=10, use_dc_pulse=True, dc_pulse_power=60, dc_pulse_freq=50, dc_pulse_off_time=5,
            use_ms=True, use_g1=True)


def _sent(rig, t0=0.0):
    return [d.decode("ascii").strip() for t, d in rig.sims["ch2.mfc"].received if t >= t0]


def _o2(seq):
    return [s for s in seq if s.startswith(("Q2 ", "L21"))]


def _cur(e):
    r = e.runner
    return r.current if r.is_running and not r.shutdown else None


def _in_block(e, typ):
    return e.cur_inst is not None and e.cur_inst.type == typ and e.runner.is_running and not e.runner.shutdown


def _run(tmp, params=None, hooks=(), max_ticks=1200, prep=None, until=None):
    rig = R.make(tmp, chambers=("ch2",))
    e = rig.eng["ch2"]
    if prep:
        prep(rig, e)
    p = dict(BASE, **(params or {}))
    R.run(rig, until or (lambda: not e.busy and any(x.kind == "run_end" for x in e.events)), max_ticks=max_ticks,
          hooks=[R.at(2, lambda: e.start(params=p, owner="local", source="local"))] + list(hooks))
    return rig, e


def _adj_events(e):
    return [x for x in e.events if x.kind == "adjust"]


def _end(e):
    return [x for x in e.events if x.kind == "run_end"][-1]


# ───────────────────────── 아직 안 한 가스 주입: 다시 펼치기 ─────────────────────────
def _future(tmp, new):
    box = {}

    def adj():
        box["t"] = rig_box["rig"].clock.monotonic()
        box["r"] = rig_box["e"].adjust("mfc", "o2_flow", new, source="web", by="t")
    rig_box = {}

    def prep(rig, e):
        rig_box.update(rig=rig, e=e)
    hook = R.when(lambda: rig_box and _cur(rig_box["e"]) is not None and _cur(rig_box["e"]).action == "IG_CMD", adj)
    rig, e = _run(tmp, hooks=[hook], prep=prep)
    return rig, e, box


def test_future_10_to_9p2_restep_9_then_9p2(tmp_path):
    rig, e, box = _future(tmp_path, 9.2)
    assert box["r"] is None
    assert _o2(_sent(rig, box["t"]))[:4] == ["Q2 90.0", "L21", "Q2 92.0", "L21"]
    msgs = [x[3] for x in e.traces[-1] if "Ch2(O2) GAS" in x[3]]
    assert msgs == ["Ch2(O2) GAS 9.0sccm 설정 (단계 1/2)", "Ch2(O2) GAS 공급 시작 (9.0sccm 도달 대기)",
                    "Ch2(O2) GAS 9.2sccm 설정 (단계 2/2)", "Ch2(O2) GAS 9.2sccm 도달 대기"]
    assert _end(e).data.get("result") == "done"
    a = _adj_events(e)
    assert a and a[0].data["result"] == "scheduled" and a[0].data["old"] == 10.0 and a[0].data["new"] == 9.2
    assert e.gas_plan and list(e.gas_plan.values()) == [9.2]


def test_future_10_to_12_restep_9_9p5_12(tmp_path):
    rig, e, box = _future(tmp_path, 12.0)
    assert box["r"] is None
    assert _o2(_sent(rig, box["t"]))[:6] == ["Q2 90.0", "L21", "Q2 95.0", "L21", "Q2 120.0", "L21"]
    assert _end(e).data.get("result") == "done"


def test_future_10_to_8_no_mid_levels(tmp_path):
    rig, e, box = _future(tmp_path, 8.0)
    assert box["r"] is None
    assert _o2(_sent(rig, box["t"]))[:2] == ["Q2 80.0", "L21"] and "Q2 90.0" not in _sent(rig, box["t"])
    msgs = [x[3] for x in e.traces[-1] if "Ch2(O2) GAS" in x[3]]
    assert msgs == ["Ch2(O2) GAS 8.0sccm 설정", "Ch2(O2) GAS 공급 시작"]          # 레시피에 8 이 있었던 것과 같음


def test_future_restep_equals_recipe_with_that_value():
    """다시 펼친 묶음 = 그 값을 레시피에 넣고 펼친 묶음(문구·값·ref) — 블록 순번과 BlockInst 범위도 맞음."""
    cfg = {}
    for new in (9.2, 12.0, 8.0, 9.0, 9.5):
        bl = [{"type": "gas_intro", "params": {"ar": True, "ar_flow": 20.0, "o2": True, "o2_flow": 10.0}},
              {"type": "wait", "params": {"sec": 5}}]
        steps, insts = BK.expand(bl, cfg, 2)
        sc = PS.step_cfg(cfg, 2)
        new_steps, n = PS.restep_gas(steps, insts, sc, "mfc.o2_flow", new, 0)
        bl2 = [dict(bl[0], params=dict(bl[0]["params"], o2_flow=new)), bl[1]]
        want, winsts = BK.expand(bl2, cfg, 2)
        assert n == 1 and PS.to_golden(new_steps) == PS.to_golden(want), new
        assert [(i.first_step, i.n_steps) for i in insts] == [(i.first_step, i.n_steps) for i in winsts], new
        assert [s.block for s in new_steps] == [s.block for s in want]


# ───────────────────────── 이미 흐르는 중: 올리면 단계, 내리면 바로 ─────────────────────────
def _live(tmp, recipe_o2, new, prep2=None, extra=None):
    box = {}
    rig_box = {}

    def prep(rig, e):
        rig_box.update(rig=rig, e=e)
        if prep2:
            prep2(rig, e)

    def adj():
        box["t"] = rig_box["rig"].clock.monotonic()
        box["r"] = rig_box["e"].adjust("mfc", "o2_flow", new, source="web", by="t")
        if extra:
            extra(rig_box, box)
    hook = R.when(lambda: rig_box and _in_block(rig_box["e"], "main"), adj)
    rig, e = _run(tmp, params={"o2_flow": recipe_o2}, hooks=[hook], prep=prep, max_ticks=1500)
    return rig, e, box


def test_live_8_to_10_goes_9_9p5_10_each_reached(tmp_path):
    rig, e, box = _live(tmp_path, 8.0, 10.0)
    assert box["r"] is None
    assert _o2(_sent(rig, box["t"])) == ["Q2 90.0", "L21", "Q2 95.0", "L21", "Q2 100.0", "L21"]
    a = _adj_events(e)
    texts = [x.data.get("text") for x in a if x.data["result"] == "stepping"]
    assert texts == ["O2 9.0 → 9.5 → 10 진행 중 1/3", "O2 9.0 → 9.5 → 10 진행 중 2/3", "O2 9.0 → 9.5 → 10 진행 중 3/3"]
    assert [x.data["result"] for x in a] == ["applied", "stepping", "stepping", "stepping", "confirmed"]
    gas = [l for l in R.logs(e) if "GAS 확인... ch2" in l]
    assert any("(목표: 9.00sccm" in l and "[연속 3/3]" in l for l in gas)          # 단계마다 가스 주입과 같은 도달 판정
    assert any("(목표: 9.50sccm" in l and "[연속 3/3]" in l for l in gas)
    assert _end(e).data.get("result") == "done" and not e.gas_steps


def test_live_9p2_to_10_goes_9p5_then_10(tmp_path):
    rig, e, box = _live(tmp_path, 9.2, 10.0)
    assert box["r"] is None
    assert _o2(_sent(rig, box["t"])) == ["Q2 95.0", "L21", "Q2 100.0", "L21"]
    texts = [x.data.get("text") for x in _adj_events(e) if x.data["result"] == "stepping"]
    assert texts == ["O2 9.5 → 10 진행 중 1/2", "O2 9.5 → 10 진행 중 2/2"]


def test_live_10_to_12_direct(tmp_path):
    rig, e, box = _live(tmp_path, 10.0, 12.0)
    assert box["r"] is None
    assert _o2(_sent(rig, box["t"])) == ["Q2 120.0"]                              # 중간값 없음 — 바로(예전과 같은 FLOW_SET)
    assert [x.data["result"] for x in _adj_events(e)] == ["applied", "confirmed"]


def test_live_lowering_10_to_8_is_direct(tmp_path):
    rig, e, box = _live(tmp_path, 10.0, 8.0)
    assert box["r"] is None
    assert _o2(_sent(rig, box["t"])) == ["Q2 80.0"]
    assert [x.data["result"] for x in _adj_events(e)] == ["applied", "confirmed"]


def _live_fail(tmp, readback):
    box, rig_box = {}, {}

    def adj():
        rig_box["rig"].sims["ch2.mfc"].ignore_writes = {"Q2"}                     # 유량이 8 에서 움직이지 않음
        box["t"] = rig_box["rig"].clock.monotonic()
        box["r"] = rig_box["e"].adjust("mfc", "o2_flow", 10.0, source="web", by="t")
    hook = R.when(lambda: rig_box and _in_block(rig_box["e"], "main"), adj)
    rig = R.make(tmp, chambers=("ch2",), cfg={"MFC_READBACK_ENABLED": readback})
    e = rig.eng["ch2"]
    rig_box.update(rig=rig, e=e)
    p = dict(BASE, o2_flow=8.0, process_time=10.0)
    R.run(rig, lambda: not e.busy and any(x.kind == "run_end" for x in e.events), max_ticks=1500,
          hooks=[R.at(2, lambda: e.start(params=p, owner="local", source="local")), hook])
    return rig, e, box


def _aborted_like_gas_injection(e, rig, box, reason):
    end = _end(e)
    assert end.data.get("result") == "error", (end.data, end.code, end.detail)
    sent = _o2(_sent(rig, box["t"]))
    assert "Q2 95.0" not in sent and "Q2 100.0" not in sent                      # 다음 단계로 가지 않음
    assert any(": [MFC - " in f for f in e.runner.failures) or "[MFC - " in (end.detail or ""), (e.runner.failures, end.detail)
    a = _adj_events(e)
    assert a[-1].data["result"] == "failed" and a[-1].data["reason"] == reason
    assert not e.gas_steps


def test_live_mid_step_not_reached_aborts_like_gas_injection(tmp_path):
    """9.0 단계 도달 못 함(장비가 Q2 를 받지 않아 유량이 8 에 머묾, 되읽기 끔) → 180 회 시간 초과 'GAS 안정화 시간 초과' → 다음 단계 없이
    공정 중단(가스 주입 FLOW_ON 실패와 같은 '[MFC - GAS 안정화 시간 초과]')."""
    rig, e, box = _live_fail(tmp_path, False)
    assert box["r"] is None
    assert _o2(_sent(rig, box["t"]))[:2] == ["Q2 90.0", "L21"]
    _aborted_like_gas_injection(e, rig, box, "STAB_TIMEOUT")
    assert any("[MFC - GAS 안정화 시간 초과]" in f for f in e.runner.failures) or "GAS 안정화 시간 초과" in (_end(e).detail or "")


def test_live_mid_step_flow_set_verify_fails_aborts(tmp_path):
    """되읽기를 켜 두면 FLOW_SET 되읽기가 끝내 다름(VERIFY_MISMATCH) → 그 단계 실패로 같은 중단(FLOW_ON 은 보내지 않음)."""
    rig, e, box = _live_fail(tmp_path, True)
    assert box["r"] is None
    sent = _o2(_sent(rig, box["t"]))
    assert sent and set(sent) == {"Q2 90.0"} and "L21" not in sent
    _aborted_like_gas_injection(e, rig, box, "VERIFY_MISMATCH")


def test_rerequest_during_stepping_is_rejected_with_reason(tmp_path):
    def extra(rb, box):
        e = rb["e"]
        box["r2"] = e.adjust("mfc", "o2_flow", 11.0, source="web", by="t")
        box["detail"] = e.reject_detail
        box["ar"] = e.adjust("mfc", "ar_flow", 22.0, source="web", by="t")    # 다른 가스(Ar)는 받음
    rig, e, box = _live(tmp_path, 8.0, 10.0, extra=extra)
    assert box["r"] is None and box["r2"] == "ADJUST_GAS_STEP" and box["ar"] is None
    assert box["detail"].startswith("O2 단계 진행 중(O2 9.0 → 9.5 → 10 진행 중 1/3)") and box["detail"].endswith("끝난 뒤 다시")
    assert _o2(_sent(rig, box["t"])) == ["Q2 90.0", "L21", "Q2 95.0", "L21", "Q2 100.0", "L21"]   # 첫 요청대로 끝까지


def test_ar_without_levels_is_unchanged_direct(tmp_path):
    box, rig_box = {}, {}

    def prep(rig, e):
        rig_box.update(rig=rig, e=e)

    def adj():
        box["t"] = rig_box["rig"].clock.monotonic()
        box["r"] = rig_box["e"].adjust("mfc", "ar_flow", 25.0, source="web", by="t")
    hook = R.when(lambda: rig_box and _in_block(rig_box["e"], "main"), adj)
    rig, e = _run(tmp_path, hooks=[hook], prep=prep)
    assert box["r"] is None
    q1 = [s for s in _sent(rig, box["t"]) if s.startswith(("Q1 ", "L11"))]
    assert q1 == ["Q1 25.0"] and [x.data["result"] for x in _adj_events(e)] == ["applied", "confirmed"]
    st = next(x for x in e.events if x.kind == "run_end")
    assert st.data.get("result") == "done"


# ───────────────────────── 겹칠 때: 시퀀스 편집·재시작·화면 표시 ─────────────────────────
def test_edit_sequence_after_future_restep_keeps_shape_and_steps_match(tmp_path):
    """다시 펼친 뒤 시퀀스 편집(아직 안 한 블록 추가) → 지금 런을 다시 펼쳐도 다시 펼친 가스 묶음(9.0 → 9.5 → 12) 그대로, 스텝 수 일치."""
    box, rig_box = {}, {}

    def prep(rig, e):
        rig_box.update(rig=rig, e=e)

    def adj():
        e = rig_box["e"]
        box["r"] = e.adjust("mfc", "o2_flow", 12.0, source="web", by="t")
        box["n_after_adjust"] = len(e.runner.steps)

    def edit():
        e = rig_box["e"]
        box["before"] = [(s.action, s.message) for s in e.runner.steps]
        k = len(e.blocks) - 1
        box["edit"] = e.edit_sequence({"op": "insert", "at": k, "block": {"type": "wait", "params": {"sec": 1}}}, source="web", by="t")
        box["after"] = [(s.action, s.message) for s in e.runner.steps]
    h1 = R.when(lambda: rig_box and _cur(rig_box["e"]) is not None and _cur(rig_box["e"]).action == "IG_CMD", adj)
    h2 = R.when(lambda: rig_box and _in_block(rig_box["e"], "main"), edit)
    rig, e = _run(tmp_path, hooks=[h1, h2], prep=prep)
    assert box["r"] is None and box.get("edit") is None, box.get("edit")
    g = [m for a, m in box["after"] if "Ch2(O2) GAS" in m]
    assert g == ["Ch2(O2) GAS 9.0sccm 설정 (단계 1/3)", "Ch2(O2) GAS 공급 시작 (9.0sccm 도달 대기)", "Ch2(O2) GAS 9.5sccm 설정 (단계 2/3)",
                 "Ch2(O2) GAS 9.5sccm 도달 대기", "Ch2(O2) GAS 12.0sccm 설정 (단계 3/3)", "Ch2(O2) GAS 12.0sccm 도달 대기"]
    i = next(k for k, (a, m) in enumerate(box["before"]) if "메인 공정" in m or a == "DELAY")
    assert box["after"][:i] == box["before"][:i]                                  # 지난 스텝 모양 그대로
    assert _end(e).data.get("result") == "done"


def test_journal_restores_restepped_shape_and_stepping_blocks_resume(tmp_path):
    """저널 → 다시 펼치기: gas_plan 으로 다시 펼친 묶음 모양을 그대로 만들어 스텝 수·지금 스텝이 맞는다. 단계 진행 중이었다는 표시가 있으면
    재시작 확인 gas_step 이 실패(이어가지 않음)."""
    from v2.ch12.resume import ResumeManager
    box, rig_box = {}, {}

    def prep(rig, e):
        rig_box.update(rig=rig, e=e)

    def adj():
        rig_box["e"].adjust("mfc", "o2_flow", 12.0, source="web", by="t")

    def snap():
        e = rig_box["e"]
        box["state"] = e.journal_state() if hasattr(e, "journal_state") else e.state_for_journal()
        box["n"] = len(e.runner.steps)
    h1 = R.when(lambda: rig_box and _cur(rig_box["e"]) is not None and _cur(rig_box["e"]).action == "IG_CMD", adj)
    h2 = R.when(lambda: rig_box and _in_block(rig_box["e"], "main"), snap)
    rig, e = _run(tmp_path, hooks=[h1, h2], prep=prep)
    st = box["state"]
    assert st["gas_plan"] and st["gas_plan"][0]["value"] == 12.0 and st["step_count"] == box["n"]
    plan = e.gas_plan_from(st["gas_plan"])
    run = e.runs[0]
    steps, insts = BK.expand(run.blocks(e.blocks), e.cfg, e.n, caps=e.caps)
    steps = e.apply_gas_plan(steps, insts, plan)
    assert len(steps) == st["step_count"] and steps[st["step_index"]].message == st["step_message"]
    # 단계 진행 중이었으면 이어가지 않음(재시작 확인 gas_step) — 같은 부팅·짧은 공백이라 다른 확인은 통과하는 문서로
    res = ResumeManager.__new__(ResumeManager)
    res.cfg, res.clock = e.cfg, e.clock
    now_w, now_m = e.clock.wall(), e.clock.monotonic()
    base = {"exit": {}, "restarts": 0, "saved_wall": now_w, "saved_mono": now_m, "boot": now_w - now_m}
    _gap, ok_checks = res.conditions("ch2", dict(base, state=dict(st, resume_mode="hold")))
    assert not [c for c in ok_checks if c[0] == "gas_step"]
    _gap, checks = res.conditions("ch2", dict(base, state=dict(st, resume_mode="hold",
                                                                gas_steps=[{"text": "O2 9.0 → 9.5 → 10 진행 중 2/3", "gas": "O2"}])))
    gs = [c for c in checks if c[0] == "gas_step"]
    assert gs and gs[0][1] is False and "O2 9.0 → 9.5 → 10 진행 중 2/3" in gs[0][2]


def test_adjust_state_shows_stepping_progress(tmp_path):
    def extra(rb, box):
        box["state"] = [x for x in rb["e"].adjust_state() if x["item"] == "o2_flow"][0]
    rig, e, box = _live(tmp_path, 8.0, 10.0, extra=extra)
    s = box["state"]["stepping"]
    assert s == {"levels": [9.0, 9.5, 10.0], "i": 1, "n": 3, "text": "O2 9.0 → 9.5 → 10 진행 중 1/3"}
    assert box["state"]["value"] == 10.0


def test_pause_does_not_stop_stepping_and_gas_intro_cannot_be_skipped(tmp_path):
    """겹칠 때(D420 ⑥): 일시정지 중에도 단계는 계속(사람이 준 값 바꾸기 — 일시정지는 블록 타이머·파워 명령만), 가스 주입 블록은 건너뛸 수 없는
    블록이라 묶음 도중에 끊기지 않는다."""
    assert BK.by_type()["gas_intro"].skippable is False
    def extra(rb, box):
        e = rb["e"]
        box["pause"] = e.pause(source="web", by="t")
        box["paused_at_start"] = bool(e.paused)
    rig, e, box = _live(tmp_path, 8.0, 10.0, extra=extra)
    assert box["r"] is None and box["pause"] is None and box["paused_at_start"]
    assert _o2(_sent(rig, box["t"])) == ["Q2 90.0", "L21", "Q2 95.0", "L21", "Q2 100.0", "L21"]   # 일시정지 중에도 끝까지
    assert [x.data["result"] for x in _adj_events(e)][-1] == "confirmed"

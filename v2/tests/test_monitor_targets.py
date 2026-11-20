"""공정 모니터 기준 = 지금 장비에 확정된 목표(2026-11-19 D423 — process_monitor·monitor_targets).

공정 중 값 바꾸기 뒤 거짓 편차 카드(MONITOR)가 나가던 문제: 모니터 기준이 run_start 의 레시피 값에 고정돼 있었다.
- 모니터 단위: hold(판정 쉼, 여럿이 쉬게 할 수 있음)·set_target(기준 바꿈 — 열린 편차 사건은 카드 없이 로그 한 줄로 닫음)·
  arm(새 기준에 처음 들어올 때까지 미룸, 한도 뒤엔 판정)·release(실패 — 기준 그대로)·rearm(가스 바꾸기가 쉬게 한 압력 다시).
- 시험대(sim 장비 + 실제 엔진 — proc_rig, CH2 O2 단계 9.0·9.5)에 공정 모니터를 붙여, 셔터 열린 메인 공정에서:
  가스 내림(10 → 8, 바로)·올림(8 → 10, 단계 9.0 → 9.5 → 10)·작업 압력(3 → 4) 바꾸기 뒤 거짓 카드 0, 기준은 새 값.
  바꾼 뒤 진짜 편차(실제 유량을 틀어 놓음) → 카드 1장('세팅' = 새 값). 바로 바꾸기 실패·단계 중간 실패 → 기준 그대로.
  뒤 블록 예약 값(아직 안 한 가스 주입) → 그 블록에서 기준. 시퀀스 편집(작업 압력 블록 끼워 넣기) → 그 값이 기준.
- 재시작 뒤 이어가기(restart_rig — 실제 조립): 공정 중 바꾼 Ar 값이 재시작 뒤에도 기준, 셔터 열린 채 감시 다시, 거짓 카드 0.
"""
from __future__ import annotations

from v2.ch12 import process_monitor as PM
from v2.ch12.process_monitor import ProcessMonitor
from v2.tests import proc_rig as R

BASE = dict(base_pressure=1e-5, working_pressure=3, process_time=1.5, shutter_delay=0.5, use_ar=True, ar_flow=20,
            use_o2=True, o2_flow=10, use_dc_pulse=True, dc_pulse_power=60, dc_pulse_freq=50, dc_pulse_off_time=5,
            use_ms=True, use_g1=True)


# ───────────────────────── 모니터 단위 ─────────────────────────
class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _mon():
    clk = Clock()
    msgs, cards = [], []
    m = ProcessMonitor(ch=2, cfg={}, log_func=lambda src, msg: msgs.append(msg), on_card=lambda text, info: cards.append(text),
                       repeat_s=300.0, recover_s=30.0, now_func=clk)
    m.activate({"use_ms": True, "use_ar": True, "ar_flow": 20.0, "use_o2": True, "o2_flow": 10.0, "working_pressure": 3.0})
    m.notify_shutter_open()
    return m, clk, msgs, cards


def test_hold_skips_and_set_target_closes_open_excursion_with_one_log_line():
    m, clk, msgs, cards = _mon()
    m.check_flow("O2", 8.0)                                   # 기준 10 — 편차(진짜로 보고)
    assert len(cards) == 1
    m.hold("O2", "공정 중 값 바꾸기")
    for _ in range(50):
        m.check_flow("O2", 8.0)
        clk.t += 3
    assert len(cards) == 1                                    # 쉬는 동안 요약·복귀 없음
    m.set_target("O2", 8.0, "공정 중 값 바꾸기")
    assert msgs[-1] == "O2 flow 기준 10 → 8 — 공정 중 값 바꾸기, 편차 사건 닫음" and len(cards) == 1
    m.check_flow("O2", 8.1)
    m.check_flow("O2", 8.5)                                   # 새 값 기준 진짜 편차 → 카드('세팅' = 새 값)
    assert len(cards) == 2 and cards[1] == "⚠️ CH2 O2 flow 편차\n세팅: 8.00 sccm → 실측: 8.50 sccm (편차: +0.50)"


def test_arm_waits_for_first_in_range_then_judges_or_judges_after_deadline():
    m, clk, msgs, cards = _mon()
    m.set_target("pressure", 4.0, "공정 중 값 바꾸기", arm_s=60.0)
    assert msgs == ["Pressure 기준 3 → 4 — 공정 중 값 바꾸기"]
    for v in (3.0, 3.3, 3.6):                                 # 옮겨 가는 중 — 미룸
        m.check_pressure(v)
        clk.t += 5
    assert cards == [] and m.holding("pressure")
    m.check_pressure(3.9)                                     # 처음 기준 안 — 이제부터 판정
    assert not m.holding("pressure")
    m.check_pressure(4.5)
    assert cards == ["⚠️ CH2 Pressure 편차\n세팅: 4.000 mTorr → 실측: 4.500 mTorr (편차: +0.500)"]
    m.set_target("Ar", 25.0, "공정 중 값 바꾸기", arm_s=10.0)
    m.check_flow("Ar", 20.0)
    clk.t += 11                                               # 한도까지 못 들어옴 → 편차로 판정
    m.check_flow("Ar", 20.0)
    assert len(cards) == 2 and cards[1].startswith("⚠️ CH2 Ar flow 편차\n세팅: 25.00 sccm → 실측: 20.00 sccm")


def test_release_keeps_baseline_rearm_and_several_holders():
    m, clk, msgs, cards = _mon()
    m.hold("O2", "공정 중 값 바꾸기")
    m.hold("pressure", "O2 유량 바꾸는 중", by="gas:O2")
    m.hold("pressure", "작업 압력 설정 중")
    m.release("O2")                                           # 실패 — 기준 10 그대로, 로그 없음
    assert msgs == [] and m.targets["O2"] == 10.0
    base = lambda: [x for x in msgs if "기준" in x]           # noqa: E731 — 카드 문구도 같은 로그로 나간다
    m.check_flow("O2", 8.0)
    assert len(cards) == 1
    m.rearm("pressure", 60.0, by="gas:O2")
    m.check_pressure(5.0)                                     # 작업 압력 쪽이 아직 쉬게 함
    assert len(cards) == 1
    m.set_target("pressure", 3.0, "공정 진행", arm_s=60.0)     # 같은 값 — 로그 없음, 처음 기준 안에 들어올 때까지 미룸
    assert base() == []
    m.check_pressure(5.0)
    assert len(cards) == 1
    m.check_pressure(3.1)
    m.check_pressure(3.5)
    assert len(cards) == 2 and "Pressure 편차" in cards[1]


def test_only_active_hold_and_arm_before_shutter_open_is_v1_immediate():
    m, clk, msgs, cards = _mon()
    m.notify_shutter_close()
    m.hold("pressure", "Ar 유량 바꾸는 중", by="gas:Ar", only_active=True)   # 셔터 전 러너 가스 스텝 — 압력은 쉬게 하지 않음
    m.set_target("pressure", 4.0, "공정 진행", arm_s=60.0)                   # 셔터 전 레시피 길 — 미루지 않음(v1 처럼 바로)
    m.notify_shutter_open()
    assert not m.holding("pressure")
    m.check_pressure(3.0)
    assert len(cards) == 1
    m.set_target("Ar", 22.0, "공정 중 값 바꾸기", arm_s=60.0, always=True)  # 공정 중 바로 바꾼 값 — 셔터 전이라도 미룸
    assert m.holding("Ar")
    m.activate({"use_ms": True, "use_ar": True, "ar_flow": 20.0})
    assert not m.holding("Ar") and m.targets == {"Ar": 20.0}               # 새 공정 — 쉬는 것·미루는 것 모두 지움


def test_untracked_item_is_ignored():
    m, clk, msgs, cards = _mon()
    m.set_target("N2", 5.0, "공정 중 값 바꾸기", arm_s=60.0)
    assert "N2" not in m.targets and msgs == []


# ───────────────────────── 시험대(CH2 — sim 장비 + 실제 엔진 + 공정 모니터) ─────────────────────────
def _in_main(e):
    return e.cur_inst is not None and e.cur_inst.type == "main" and e.runner.is_running and not e.runner.shutdown


def _cur(e):
    r = e.runner
    return r.current if r.is_running and not r.shutdown else None


class Box(dict):
    pass


def _run(tmp, params=None, hooks=(), cfg=None, max_ticks=2500, prep=None):
    rig = R.make(tmp, chambers=("ch2",), cfg=cfg)
    e = rig.eng["ch2"]
    mon = PM.attach(rig, e)
    box = Box(rig=rig, e=e, mon=mon, logs=[])
    rig.event_listeners.append(lambda lg: box["logs"].append(lg))
    if prep:
        prep(box)
    p = dict(BASE, **(params or {}))
    R.run(rig, lambda: not e.busy and any(x.kind == "run_end" for x in e.events), max_ticks=max_ticks,
          hooks=[R.at(2, lambda: e.start(params=p, owner="local", source="local"))] + [h(box) for h in hooks])
    return box


def _cards(box, phase=None):
    return [lg for lg in box["rig"].events if lg.kind == "monitor_card" and (phase is None or lg.event["phase"] == phase)]


def _end(box):
    return [x for x in box["e"].events if x.kind == "run_end"][-1]


def _adj(box):
    return [x.data for x in box["e"].events if x.kind == "adjust"]


def _when_active_main(fn, delay_s=10.0):
    """셔터 열린 메인 공정에 들어가 판정 중이 된 뒤 delay_s 지나면 한 번."""
    def make(box):
        st = {}

        def hook(t):
            e, mon = box["e"], box["mon"].mon
            if "done" in st or not (_in_main(e) and mon.is_active and e.actions.mfc_polling):
                return
            now = box["rig"].clock.monotonic()
            st.setdefault("t0", now)
            if now - st["t0"] >= delay_s:
                st["done"] = True
                box["t_adj"] = now
                fn(box)
        return hook
    return make


def _bump_o2_when(pred, d_sccm=0.6):
    """pred(box) 가 참이 된 첫 tick 에 sim O2 실제 유량을 d_sccm 만큼 틀어 놓는다(진짜 편차)."""
    def make(box):
        def hook(t):
            if "bumped" in box or not pred(box):
                return
            sim = box["rig"].sims["ch2.mfc"]
            sim.set_pct[1] += d_sccm * (sim.scale[1] or 1.0)
            box["bumped"] = box["rig"].clock.monotonic()
        return hook
    return make


def _settled(key, value):
    def pred(box):
        m = box["mon"].mon
        return m.is_active and m.targets.get(key) == value and not m.holding(key) and not m.holding("pressure") \
            and _in_main(box["e"]) and box["rig"].clock.monotonic() - box.get("t_adj", 0) > 20
    return pred


def _assert_no_cards(box):
    c = _cards(box)
    assert c == [], [x.detail for x in c]


def test_baseline_run_without_change_has_no_cards(tmp_path):
    box = _run(tmp_path)
    assert _end(box).data.get("result") == "done"
    _assert_no_cards(box)


def test_live_lowering_o2_10_to_8_no_false_card_baseline_follows(tmp_path):
    seen = {}

    def adj(box):
        box["r"] = box["e"].adjust("mfc", "o2_flow", 8.0, source="web", by="t")
    box = _run(tmp_path, hooks=[_when_active_main(adj),
                                lambda b: (lambda t: seen.update(o2=b["mon"].mon.targets.get("O2")) if _in_main(b["e"]) else None)])
    assert box["r"] is None and [a["result"] for a in _adj(box)] == ["applied", "confirmed"]
    assert seen["o2"] == 8.0
    assert any("O2 flow 기준 10 → 8 — 공정 중 값 바꾸기" in l for l in R.logs(box["e"]))
    assert _end(box).data.get("result") == "done"
    _assert_no_cards(box)


def test_live_raising_o2_8_to_10_with_steps_no_false_card(tmp_path):
    seen = {}

    def adj(box):
        box["r"] = box["e"].adjust("mfc", "o2_flow", 10.0, source="web", by="t")

    def watch(b):
        def hook(t):
            if b["e"].gas_steps:
                seen["held"] = seen.get("held", True) and b["mon"].mon.holding("O2") and b["mon"].mon.holding("pressure")
            if _in_main(b["e"]):
                seen["o2"] = b["mon"].mon.targets.get("O2")
        return hook
    box = _run(tmp_path, params={"o2_flow": 8.0}, hooks=[_when_active_main(adj), watch])
    assert box["r"] is None
    assert [a["result"] for a in _adj(box)] == ["applied", "stepping", "stepping", "stepping", "confirmed"]
    assert seen["held"] is True and seen["o2"] == 10.0                    # 단계 진행 중 내내 O2·압력 판정 쉼, 끝나면 기준 10
    assert any("O2 flow 기준 8 → 10 — 공정 중 값 바꾸기(가스 단계)" in l for l in R.logs(box["e"]))
    assert _end(box).data.get("result") == "done"
    _assert_no_cards(box)


def test_live_working_pressure_3_to_4_no_false_card(tmp_path):
    seen = {}

    def adj(box):
        box["r"] = box["e"].adjust("mfc", "wp", 4.0, source="web", by="t")
    box = _run(tmp_path, hooks=[_when_active_main(adj),
                                lambda b: (lambda t: seen.update(p=b["mon"].mon.targets.get("pressure")) if _in_main(b["e"]) else None)])
    assert box["r"] is None and [a["result"] for a in _adj(box)] == ["applied", "confirmed"]
    assert seen["p"] == 4.0
    assert any("Pressure 기준 3 → 4 — 공정 중 값 바꾸기" in l for l in R.logs(box["e"]))
    _assert_no_cards(box)


def test_real_deviation_after_change_one_card_with_new_setpoint(tmp_path):
    def adj(box):
        box["r"] = box["e"].adjust("mfc", "o2_flow", 8.0, source="web", by="t")
    box = _run(tmp_path, hooks=[_when_active_main(adj), _bump_o2_when(_settled("O2", 8.0))])
    assert box["r"] is None and "bumped" in box
    first = _cards(box, "first")
    assert len(first) == 1, [x.detail for x in _cards(box)]
    assert first[0].event["item"] == "O2"
    assert first[0].detail.startswith("⚠️ CH2 O2 flow 편차\n세팅: 8.00 sccm → 실측: 8.")
    assert all(c.event["item"] == "O2" for c in _cards(box))               # 압력 등 다른 거짓 카드 없음


def test_live_change_failure_keeps_baseline(tmp_path):
    """되읽기 켬 + 장비가 Q2 를 받지 않음 → 내림(10 → 8, 바로) 실패(VERIFY_MISMATCH) → 기준 10 그대로·판정 다시, 공정은 계속."""
    seen = {}

    def adj(box):
        box["rig"].sims["ch2.mfc"].ignore_writes = {"Q2"}
        box["r"] = box["e"].adjust("mfc", "o2_flow", 8.0, source="web", by="t")

    def watch(b):
        def hook(t):
            a = _adj(b)
            if a and a[-1]["result"] == "failed" and "o2" not in seen:
                m = b["mon"].mon
                seen.update(o2=m.targets.get("O2"), held=m.holding("O2"))
        return hook
    box = _run(tmp_path, cfg={"MFC_READBACK_ENABLED": True}, hooks=[_when_active_main(adj), watch])
    assert box["r"] is None and _adj(box)[-1]["result"] == "failed" and _adj(box)[-1]["reason"] == "VERIFY_MISMATCH"
    assert seen == {"o2": 10.0, "held": False}
    assert not any("O2 flow 기준" in l for l in R.logs(box["e"]))
    assert _end(box).data.get("result") == "done"
    _assert_no_cards(box)


def test_mid_step_failure_keeps_baseline_then_aborts(tmp_path):
    """올림(8 → 10) 9.0 단계에서 도달 못 함(장비가 Q2 를 받지 않음) → 공정 중단, 그 순간 기준 8 그대로·쉬는 것 풀림, 거짓 카드 0."""
    seen = {}

    def adj(box):
        box["rig"].sims["ch2.mfc"].ignore_writes = {"Q2"}
        box["r"] = box["e"].adjust("mfc", "o2_flow", 10.0, source="web", by="t")

    def prep(box):
        def on(lg):
            ev = getattr(lg, "event", None)
            if lg.kind == "adjust" and getattr(ev, "data", {}).get("result") == "failed" and "o2" not in seen:
                m = box["mon"].mon
                seen.update(o2=m.targets.get("O2"), held=m.holding("O2"), p_held=bool(m._hold.get("pressure")))
        box["rig"].event_listeners.append(on)
    box = _run(tmp_path, params={"o2_flow": 8.0, "process_time": 10.0}, cfg={"MFC_READBACK_ENABLED": False},
               hooks=[_when_active_main(adj, 3.0)], prep=prep, max_ticks=4000)
    assert box["r"] is None and _adj(box)[-1]["reason"] == "STAB_TIMEOUT"
    assert seen == {"o2": 8.0, "held": False, "p_held": False}
    assert not any("O2 flow 기준" in l for l in R.logs(box["e"]))
    assert _end(box).data.get("result") == "error"
    _assert_no_cards(box)


def test_scheduled_value_for_later_block_is_baseline_there(tmp_path):
    """아직 안 한 가스 주입(10 → 8 예약) → 그 블록 FLOW_ON 도달 뒤 기준 8('공정 중 값 바꾸기(예약)'), 메인 공정에서 진짜 편차는
    '세팅: 8.00' 으로 카드 1장."""
    def adj(t, box):
        if "r" not in box and _cur(box["e"]) is not None and _cur(box["e"]).action == "IG_CMD":
            box["r"] = box["e"].adjust("mfc", "o2_flow", 8.0, source="web", by="t")
    box = _run(tmp_path, hooks=[lambda b: (lambda t: adj(t, b)), _bump_o2_when(_settled("O2", 8.0))])
    assert box["r"] is None and [a["result"] for a in _adj(box)] == ["scheduled"]
    assert any("O2 flow 기준 10 → 8 — 공정 중 값 바꾸기(예약)" in l for l in R.logs(box["e"]))
    first = _cards(box, "first")
    assert len(first) == 1 and first[0].detail.startswith("⚠️ CH2 O2 flow 편차\n세팅: 8.00 sccm → 실측: 8."), \
        [x.detail for x in _cards(box)]


def test_sequence_edit_inserted_pressure_block_is_baseline(tmp_path):
    """시퀀스 편집 — 메인 공정 앞에 작업 압력 4 블록과 대기를 끼워 넣음 → 그 SP1 이 기준('공정 진행'), 메인 공정 거짓 카드 0."""
    def edit(t, box):
        e = box["e"]
        if "r" in box or _cur(e) is None or _cur(e).action != "IG_CMD":
            return
        k = next(i for i, b in enumerate(e.blocks) if b["type"] == "shutter" and "ms" in (b["params"].get("targets") or []))
        r1 = e.edit_sequence({"op": "insert", "at": k, "block": {"type": "pressure_set", "params": {"value": 4}}})
        r2 = e.edit_sequence({"op": "insert", "at": k + 1, "block": {"type": "wait", "params": {"sec": 30}}})
        box["r"] = (r1, r2)
    seen = {}
    box = _run(tmp_path, hooks=[lambda b: (lambda t: edit(t, b)),
                                lambda b: (lambda t: seen.update(p=b["mon"].mon.targets.get("pressure")) if _in_main(b["e"]) else None)])
    assert box["r"] == (None, None), box["e"].reject_detail
    assert seen["p"] == 4.0
    assert any("Pressure 기준 3 → 4 — 공정 진행" in l for l in R.logs(box["e"]))
    assert _end(box).data.get("result") == "done"
    _assert_no_cards(box)


# ───────────────────────── 재시작 뒤 이어가기(실제 조립) ─────────────────────────
def test_resume_keeps_changed_baseline_and_watch(tmp_path):
    from v2.tests import restart_rig as RR
    from v2.tests.test_resume import kill_at_block, revive
    rig = RR.boot(tmp_path)
    e = rig.eng["ch1"]
    RR.drive(rig, lambda: rig.resumer.done, 30)
    p = dict(base_pressure=1e-5, working_pressure=3, process_time=1.5, shutter_delay=0.5, use_ar=True, use_o2=True, ar_flow=20,
             o2_flow=1, use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True)
    assert e.start(params=dict(p, process_note="mon"), owner="local", source="local") is None
    box = {}

    def adj():
        if "r" not in box and e.monitor.mon.is_active:
            box["r"] = e.adjust("mfc", "ar_flow", 22.0, source="web", by="t")
    RR.drive(rig, lambda: "r" in box and e.monitor.mon.targets.get("Ar") == 22.0 and not e.monitor.mon.holding("Ar"), 1500,
             hooks=[RR.block_hook(e, "main", adj, 5.0)])
    assert box["r"] is None
    kill_at_block(rig, e, "main", 20.0)
    rig2, e2, dec = revive(tmp_path, rig, 5.0, ch="ch1", to_end=False)
    try:
        RR.drive(rig2, lambda: e2.runner.cd is not None, 60)
        assert dec["result"] == "resumed"
        m = e2.monitor.mon
        assert m.is_active and m.targets == {"Ar": 22.0, "O2": 1.0, "pressure": 3.0}
        assert any("Ar flow 기준 20 → 22 — 재시작 뒤 이어가기" in x for x in e2.log_lines)
        RR.drive(rig2, lambda: not e2.busy, 400)
        assert RR.ends(e2) == [("done", None)]
        cards = [lg for lg in rig2.events if lg.kind == "monitor_card"]
        assert cards == [], [c.detail for c in cards]
    finally:
        RR.finish(rig2)

"""진행 단계의 화면용 이름(2026-11-24 묶음 16 ④ — step_labels).

- 실제 공정(가상 시계 + 시뮬레이터 — proc_rig)에서 스텝마다 사건 process/step 하나(이름 = 화면용 이름, data.message = v1 스텝 문구),
  1초 행 <c>.proc.step_label·엔진 화면 상태 step_label 이 그 이름이다.
- 스텝 문구·공정 로그('[공정 i/N] 문구')·엔진 text 는 그대로(v1 골든 문구) — 이름은 따로 실린다.
- 골든의 모든 스텝 문구(181개 — process_combos.json)에 이름이 생긴다: test_web_labels.test_step_screen_names_for_every_golden_step.
"""
from v2.ch12.step_labels import step_label
from v2.tests import proc_rig as R

P = dict(base_pressure=1e-5, working_pressure=3, process_time=0.1, shutter_delay=0.1, use_ar=True, use_o2=True, ar_flow=20,
         o2_flow=1, use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True,
         process_note="step-label")


def test_step_events_snapshot_and_state_carry_screen_names(tmp_path):
    rig = R.make(tmp_path, sp=(3.0, 5.0), chambers=("ch1",))
    e = rig.eng["ch1"]
    seen = {}

    def watch(*_a):
        if e.runner.is_running and e.step_label:
            row = e.snapshot_fields()
            seen.setdefault(e.step_label, (row.get("ch1.proc.step_label"), e.state_dict()["step_label"], e.state_text))
    R.run(rig, R.idle(e), max_ticks=1500, hooks=[R.at(2, lambda: e.start(params=P, owner="local", source="local")), watch])
    assert R.ends(e) == [("done", None)], R.logs(e)[-20:]
    ev = [x for x in e.events if x.kind == "step"]
    steps = e.runner.steps
    assert len(ev) >= len([s for s in steps]) - 1 and ev                      # 공정 스텝 + 정상 끝 정지 스텝
    by_msg = {x.data["message"]: x for x in ev}
    for s in steps:
        x = by_msg.get(s.message)
        if x is None:
            continue
        assert x.detail == x.data["label"] == step_label(s) and x.data["action"] == s.action
        assert x.data["message"] == s.message                                     # v1 스텝 문구 그대로(data 에)
    labels = [x.data["label"] for x in ev]
    assert "Main 가스 열기" in labels and "Main 셔터 열기" in labels and "RF Pulse 켜기 120 W" in labels and "Main 가스 닫기" in labels
    assert not any(x.startswith("MV ") for x in labels)          # 대상 'MV' 는 가스 Main 밸브(MAIN_n_GAS_SW) — 진공 MV 이름을 쓰지 않음
    assert all(1 <= x.data["index"] <= x.data["total"] for x in ev) and ev[0].data["run_id"] == e.run_id
    # 1초 행·엔진 화면 상태에 같은 이름, 엔진 text 는 v1 스텝 문구(또는 카운트다운 문구)
    assert seen and all(snap == lab and st == lab for lab, (snap, st, _t) in seen.items())
    assert any(t == "메인 밸브 열기" for _s, _st, t in seen.values()) or "메인 밸브 열기" in [s.message for s in steps]
    # 공정 로그는 v1 문구 그대로 — 화면 이름은 로그에 들어가지 않는다
    logs = "\n".join(R.logs(e))
    assert "[공정 " in logs and "메인 밸브 열기" in logs and "MV 열기" not in logs and "Main 가스 열기" not in logs   # 공정 txt 는 v1 문구 그대로
    # 끝나면 비운다
    assert e.state_dict()["step_label"] is None and e.snapshot_fields().get("ch1.proc.step_label") is None


def test_main_gas_step_name_w21():
    """W-21·22 대조 E — 공정 단계 대상 'MV'(v1 '메인 밸브 열기/닫기')는 MAIN_n_GAS_SW 를 여닫는다: 화면 이름 'Main 가스', 정지 단계 12 도 같음.
    v1 스텝 문구·진공 MV(M_V_n_SW) 버튼 이름은 그대로."""
    import inspect
    from v2.ch12 import process_actions as PA, safety_rules as SR, step_labels as SL
    from v2.ch12.process_steps import S
    src = inspect.getsource(PA)
    assert 'if nname == "MV":' in src and 'write_switch(f"MAIN_{self.n}_GAS_SW"' in src
    assert SL.step_label(S("PLC_CMD", "메인 밸브 열기", params=("MV", True, 1))) == "Main 가스 열기"
    assert SL.step_label(S("PLC_CMD", "[긴급] 메인 밸브 즉시 닫기", params=("MV", False, 2), no_wait=True)) == "긴급 Main 가스 닫기"
    st12 = [s for s in SR.STOP_SEQUENCE if s.part == "mv_close"][0]
    assert st12.label == "Main 가스 닫기" and "MAIN_n_GAS_SW" in st12.act
    assert S("PLC_CMD", "메인 밸브 열기", params=("MV", True, 1)).message == "메인 밸브 열기"
    from v2.ch12.web_schema import bundle
    b = bundle({})
    names = {x["target"].lower(): x["label"] for x in b["plcButtons"]}
    assert names["m_v_1_sw"] == "CH1 MV" and names["main_1_gas_sw"] == "CH1 Main 가스"

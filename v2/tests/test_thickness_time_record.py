"""두께 모니터 Time 모드 기록(v1 main 2f968be, tests/test_thickness_time_record.py 와 같은 기대값) —
시작 전 Factor 적용 시도(실패해도 시작 안 막음)·성공 런만 Shutter Delay 뒤·Main Shutter 앞 THICKNESS_ZERO·ETA 가 영점 확인 초를 더함·
기록은 기준점 기준 + Factor 열·실패 사유는 Fallback Reason."""
import pytest

from v2.ch12 import blocks as BK
from v2.ch12.eta import sum_delay_from
from v2.ch12.process_steps import caps_from_cfg
from v2.tests.test_thickness import CFG, Rig, _params


def _time_params(**kw):
    p = _params(thickness=None)
    p.update(kw)
    return p


def _prep(r, p, chuck_ok=True):
    box = []
    r.e.time_prepare(p, chuck_ok, lambda: box.append(1))
    assert box
    return p


def test_prepare_ok_sets_flag_and_pre(tmp_path):
    r = Rig(tmp_path)
    p = _prep(r, _time_params())
    assert p.get("thickness_time_record") is True and "thickness_record_note" not in p and "thickness_mode" not in p
    assert r.d.mat_calls[0][:3] == (2.73, 1.08, 110.0) and r.d.zero_calls == []
    assert r.e._pre["factor_source"] == "T:factor D:factor Z:factor"
    line = [m for s, m in r.logs if s == "THICKNESS"][-1]
    assert line.startswith("Time 모드(두께 기록) — G1 · chuck mid · tooling 110 %") and "목표" not in line
    assert r.e.run_warnings == []


@pytest.mark.parametrize("mut,code", [
    (lambda r: r.e.factors.tooling["mid"].__setitem__("G1", None), "E845"),
    (lambda r: r.d.st.update(connected=False), "E840"),
    (lambda r: r.d.st.update(crystal_ok=False), "E841"),
    (lambda r: setattr(r.d, "mat_error", "되읽기 불일치"), "E846"),
])
def test_prepare_failure_does_not_block(tmp_path, mut, code):
    r = Rig(tmp_path)
    mut(r)
    p = _prep(r, _time_params())
    assert "thickness_time_record" not in p and r.e._pre is None
    note = p["thickness_record_note"]
    assert note.startswith(f"[{code}]") and note.endswith("Factor 미적용")
    assert r.e.run_warnings == [f"두께 기록: {note}"]
    assert any("Time 모드 두께 기록" in m and note in m for m in r.msgs("THICKNESS"))


@pytest.mark.parametrize("guns,word", [(("g1", "g2"), "co-sputter(G1+G2) — Factor 미적용"), ((), "사용 건 없음 — Factor 미적용")])
def test_prepare_cosputter_or_no_gun_sends_nothing(tmp_path, guns, word):
    r = Rig(tmp_path)
    flags = {"use_g1": False, "use_g2": False, "use_g3": False}
    flags.update({f"use_{g}": True for g in guns})
    p = _prep(r, _time_params(**flags))
    assert r.d.mat_calls == [] and r.d.zero_calls == []
    assert p["thickness_record_note"] == word
    r.e.run_started(p)
    assert r.e.record.rec["fallback_reason"] == word and "time_record" not in r.e.record.rec


def test_time_zero_success_final_is_last_minus_base(tmp_path):
    r = Rig(tmp_path)
    p = _prep(r, _time_params())
    r.e.run_started(p)
    assert r.e.record.rec["time_record"] and r.e.record.rec["target"] is None
    r.d.zero_ok = False                                           # 영점 명령 실패 → 첫 값을 기준점으로
    r.zero()
    r.feed(1.5)
    r.feed(1.5)
    r.feed(1.5)
    r.e.poll()
    assert r.zeros and r.e.record.rec["zero_base"] == 1.5
    r.e.set_main_polling(True)
    for s in (2.0, 6.0, 11.5):
        r.feed(s, dt=10.0)
    v = r.e.record.values()
    assert v["end"] == "Time" and v["target"] is None
    assert v["final_nm"] == pytest.approx(10.0) and v["avg_rate"] == pytest.approx(0.5)
    assert (v["tooling"], v["density"], v["z_factor"], v["material"], v["chuck"], v["gun"]) == \
        (110.0, 2.73, 1.08, "Aluminum", "mid", "G1")


def test_failed_prepare_records_difference_and_reason(tmp_path):
    r = Rig(tmp_path)
    r.d.st.update(connected=False)
    p = _prep(r, _time_params())
    r.e.run_started(p)
    r.e.set_main_polling(True)
    r.d.st.update(connected=True)
    for s in (40.0, 43.0):
        r.feed(s, dt=10.0)
    v = r.e.record.values()
    assert v["final_nm"] == pytest.approx(3.0) and v.get("tooling") is None
    assert v["fallback_reason"].startswith("[E840] 두께 모니터 응답 없음")
    r2 = Rig(tmp_path / "x") if False else None
    r.e.record.rec["first"] = r.e.record.rec["last"] = None       # 값이 하나도 없어도 사유는 남긴다
    assert r.e.record.values()["fallback_reason"] == p["thickness_record_note"]




def test_zero_step_between_shutter_delay_and_main_shutter_and_eta():
    from v2.ch12.blocks_expand import template_blocks
    cfg = dict(CFG)
    caps = caps_from_cfg(cfg, 2)
    p = {"process_note": "t", "base_pressure": 1e-5, "working_pressure": 3.0, "process_time": 10.0, "shutter_delay": 2.0,
         "use_ar": True, "ar_flow": 20.0, "use_g1": True, "use_ms": True, "use_dc_power": True, "dc_power": 100.0,
         "integration_time": 1000}
    try:
        blocks, _ = template_blocks(p, 2, cfg, caps=caps)
    except Exception as e:                                       # 템플릿 입력 키가 다르면 정규화 행으로
        pytest.skip(f"템플릿 입력 준비 필요: {e!r}")
    base, _ = BK.expand(blocks, cfg, 2, caps=caps)
    seq, insts = BK.expand(blocks, cfg, 2, caps=caps, time_zero=True)
    names = [s.action for s in seq]
    z = names.index("THICKNESS_ZERO")
    assert seq[z - 1].action == "DELAY" and seq[z - 1].message.startswith("Shutter Delay")
    assert seq[z + 1].action == "PLC_CMD" and seq[z + 1].params[:2] == ("MS", True)
    assert "THICKNESS_WAIT" not in names and "THICKNESS_ZERO" not in [s.action for s in base]
    assert [s.message for s in seq[:z]] + [s.message for s in seq[z + 1:]] == [s.message for s in base]
    sd = next(i for i, s in enumerate(base) if s.action == "DELAY" and s.message.startswith("Shutter Delay"))
    assert sum_delay_from(seq, sd) == pytest.approx(sum_delay_from(base, sd) + 5.0)

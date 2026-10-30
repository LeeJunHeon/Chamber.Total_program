"""CH2 히터 구글챗 카드 — v1 tests/test_heater_cards.py(11)의 입력·기대값(v1 import 없음). 시험대 FastHeaterRig(래더 모델).

카드는 v2 에서 이벤트(HeaterEvent kind='card', data: title·subtitle·status·fields·urgent)로 나가고 포털이 같은 내용으로 보낸다.
"""
from __future__ import annotations

import re

import pytest

from v2.ch12.heater_engine import HeaterEngine
from v2.ch12.sim.heater_rig import FastHeaterRig

T = "[CH2] 히터 "

# ═════════════════════ B. 카드(tests/test_heater_cards.py:217-334) ═════════════════════
def _crig(**over):
    return FastHeaterRig(**{"hold_mode": "off", **over})


def _only(r, name):
    return [c for c in r.cards_full() if c[0] == T + name]


def test_manual_ok_start_and_end_success():
    r = _crig()
    r.run(5)
    assert r.call(r.eng.turn_on, 150.0, owner="manual")[0]
    r.run(600)
    r.call(r.eng.turn_off, "정지(패널)", owner="manual")
    r.run(3)
    cards = r.cards_full()
    assert [c[0] for c in cards] == [T + "시작", T + "종료"]
    s, e = cards
    assert s[1] == "수동" and s[2] == "INFO" and list(s[3]) == ["목표", "램프", "현재 PV"]
    assert s[3]["목표"] == "150.0°C" and s[3]["램프"] == "12°C/min"
    assert e[1] == "수동 · 정지(패널)" and e[2] == "SUCCESS" and e[3]["사유"] == "정지(패널)" and e[3]["특이사항"] == "없음"
    assert e[3]["운전 시간"] == "10분"
    assert r.log_texts("운전 종료 — 사유 정지(패널)") and "특이사항 없음" in r.log_texts("운전 종료")[0]


def test_start_card_shows_hold_mode_when_dac():
    r = _crig(hold_mode="dac")
    r.run(5)
    r.call(r.eng.turn_on, 150.0)
    r.run(3)
    assert _only(r, "시작")[0][3]["유지 모드"] == "dac"


def test_duration_format():
    f = HeaterEngine._fmt_duration
    assert (f(59), f(59 * 60 + 59), f(3600), f(2 * 3600 + 5 * 60 + 30)) == ("0분", "59분", "1시간 0분", "2시간 5분")


def test_manual_stall_fail_card_and_end_fail():
    r = _crig()
    r.run(5)
    r.call(r.eng.turn_on, 300.0, owner="manual")
    r.run(5400)
    r.call(r.eng.turn_off, "정지(패널)", owner="manual")
    r.run(3)
    stall = _only(r, "승온 정체")
    assert len(stall) == 1 and stall[0][2] == "FAIL" and stall[0][1].startswith("수동 · 목표 300.0°C")
    e = _only(r, "종료")
    assert len(e) == 1 and e[0][2] == "FAIL" and e[0][1] == "수동 · 정지(패널)" and "승온 정체" in e[0][3]["특이사항"]


def test_overtemp_during_run_fault_card_and_end_fail():
    r = _crig()
    r.run(5)
    r.call(r.eng.turn_on, 150.0, owner="manual")
    r.run(120)
    r.plc.tc_offset = 600.0
    r.run(5)
    f = _only(r, "이상")
    assert len(f) == 1 and f[0][2] == "FAIL" and f[0][1] == "수동 · 이상 — 과온"
    e = _only(r, "종료")
    assert len(e) == 1 and e[0][2] == "FAIL" and e[0][1] == "수동 · 이상 — 과온"
    assert e[0][3]["사유"] == "이상 — 과온" and e[0][3]["특이사항"] == "이상 — 과온"


def _process_run(r, name, target, nxt, index=0, total=1):
    from v2.ch12.heater_link import process_context
    r.eng.next_step_fn = lambda: nxt
    r.eng.take_process_results()
    r.eng.heater_set(float(target), None, context=process_context(name, index, total))
    r.run(2)
    assert [k for k, *_ in r.eng.take_process_results()] == ["set_ok"]
    r.eng.heater_wait(float(target))
    got = []
    r.until(lambda: got.extend(r.eng.take_process_results()) or any(k == "ready" for k, *_ in got))


@pytest.mark.parametrize("detail,stop_user,reach,sub,status", [
    ({"result": "SUCCESS"}, True, True, "공정 종료", "SUCCESS"),
    ({"result": "STOP", "stopped": True}, True, False, "공정 중단 — 사용자 STOP", "SUCCESS"),
    ({"result": "STOP", "stopped": True}, False, False, "공정 중단 — 원격 STOP", "SUCCESS"),
    ({"result": "FAIL", "error_code": "E832", "error_detail": "x"}, True, False, "공정 중단 — 오류 (E832 공정 중 히터 이상)", "FAIL"),
])
def test_process_end_cards(detail, stop_user, reach, sub, status):
    from v2.ch12.heater_link import process_context
    r = _crig(mv_limit=700)
    r.run(5)
    if reach:
        _process_run(r, "STO_A", 200.0, "Main Shutter Open")
    else:
        r.eng.heater_set(200.0, None, context=process_context("STO_A", 0, 1))
        r.run(60)
    r.eng.stop_user = stop_user
    r.eng.after_row(ok=detail.get("result") == "SUCCESS", stopped=bool(detail.get("stopped")), detail=detail)
    r.run(3)
    assert not r.eng.status()["run"] and r.eng.owner is None
    e = _only(r, "종료")
    assert len(e) == 1 and e[0][1] == f'공정 "STO_A" · {sub}' and e[0][2] == status and e[0][3]["사유"] == sub
    assert e[0][3]["특이사항"] == ("없음" if status == "SUCCESS" else sub)
    assert r.log_texts(f"운전 종료 — 사유 {sub} ·")
    if reach:
        s, rr, _e = r.cards_full()
        assert s[1] == '공정 "STO_A"' and s[2] == "INFO" and s[3]["목표"] == "200.0°C"
        assert rr[1] == '공정 "STO_A"' and rr[2] == "SUCCESS" and list(rr[3]) == ["목표", "도달 PV", "승온 소요", "다음 단계"]
        assert rr[3]["다음 단계"] == "Main Shutter Open" and re.fullmatch(r"\d+분 \d+초", rr[3]["승온 소요"])
    else:
        assert not _only(r, "도달")


def test_process_fail_without_code_uses_detail_40():
    r = FastHeaterRig()
    assert r.eng.end_reason(False, False, {"result": "FAIL", "error_detail": "가" * 50}) == f"공정 중단 — 오류 ({'가' * 40})"
    assert r.eng.end_reason(False, False, {"result": "FAIL"}) == "공정 중단 — 오류"
    assert r.eng.end_reason(True, False, None) == "공정 종료"


def test_queue_two_processes_one_start_two_reached_one_end():
    r = _crig(mv_limit=700)
    r.run(5)
    _process_run(r, "STO_A", 200.0, "Main Shutter Open", 0, 2)
    r.eng.after_row(ok=True, stopped=False, detail={"result": "SUCCESS"}, queue_mode=True, next_uses_heater=True)
    r.run(30)
    assert r.eng.status()["run"] and r.eng.owner == "process"
    _process_run(r, "STO_B", 220.0, "Gun Shutter Open", 1, 2)
    r.eng.after_row(ok=True, stopped=False, detail={"result": "SUCCESS"}, queue_mode=True, next_uses_heater=False)
    r.run(3)
    cards = r.cards_full()
    assert [c[0] for c in cards] == [T + "시작", T + "도달", T + "도달", T + "종료"]
    s, r1, r2, e = cards
    assert s[1] == '공정 "STO_A" (1/2)'
    assert r1[1] == '공정 "STO_A" (1/2)' and r1[3]["목표"] == "200.0°C" and r1[3]["다음 단계"] == "Main Shutter Open"
    assert r2[1] == '공정 "STO_B" (2/2)' and r2[3]["목표"] == "220.0°C" and r2[3]["다음 단계"] == "Gun Shutter Open"
    assert e[1] == '공정 "STO_B" (2/2) · 공정 종료' and e[2] == "SUCCESS"


def test_many_events_capped_at_four():
    r = _crig()
    r.run(5)
    r.call(r.eng.turn_on, 150.0, owner="manual")
    r.run(30)
    for lb in ("승온 정체", "DAC 포화", "출력 죽음", "유지 모드 포기", "유지 모드 강제 진입", "승온 정체"):
        r.eng._event(lb)
    r.call(r.eng.turn_off, "정지(패널)", owner="manual")
    r.run(3)
    e = _only(r, "종료")[0]
    assert e[2] == "FAIL" and e[3]["특이사항"] == "승온 정체 · DAC 포화 · 출력 죽음 · 유지 모드 포기 외 1건"


def test_card_events_carry_v1_shape():
    """카드 이벤트 = v1 _post_card('[CH2] 히터 {제목}', 부제, 상태, 필드, urgent=True)(controller/heater_service.py:530-536)."""
    r = _crig()
    r.run(5)
    r.call(r.eng.turn_on, 150.0)
    r.run(2)
    e = [x for x in r.events if x.kind == "card"][0]
    assert e.data["title"] == T + "시작" and e.data["urgent"] is True and e.detail == e.data["subtitle"] == "수동"

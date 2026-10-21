# -*- coding: utf-8 -*-
"""CH2 히터 구글챗 카드(챔버K 방식) — 시작 ℹ️ · 도달 ✅/❌ · 종료 ✅/❌ 로 한 번의 운전(RUN 상승~하강)을 요약한다.
수동 운전은 HeaterService + 래더 에뮬레이터, 레시피 공정은 ChamberRuntime 콜백(cb_heater_set·승온 대기·공정 종료 처리)으로 돈다.
시나리오 함수(_sc_*)는 (제목, 부제, 상태, 필드) 목록을 돌려준다 — 검증 출력에도 그대로 쓴다."""
import os
import sys
import asyncio
import contextlib
import re

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest                                                    # noqa: E402
import heater_ladder_sim as sim                                  # noqa: E402
from controller.heater_service import HeaterService             # noqa: E402
from runtime.chamber_runtime import ChamberRuntime               # noqa: E402

T = "[CH2] 히터 "


class FieldChat(sim.FakeChat):
    """에뮬레이터 가짜 챗 + 필드까지 남긴다."""

    def __init__(self, clk) -> None:
        super().__init__(clk)
        self.full = []

    def _post_card(self, title, subtitle="", status="INFO", fields=None, urgent=False):
        super()._post_card(title, subtitle, status, fields, urgent)
        self.full.append((title, subtitle, status, dict(fields or {})))


def _make(**over):
    svc, plc, clk, logs, chat = sim.make(**{"hold_mode": "off", **over})
    chat = FieldChat(clk)
    svc._chat = chat
    return svc, plc, clk, logs, chat


def _only(cards, name):
    return [c for c in cards if c[0] == T + name]


class _PCRec:
    """공정 컨트롤러 대신 on_heater_* 호출만 기록(공정 이름·다음 스텝은 속성으로 넣는다)."""

    def __init__(self):
        self.calls = []
        self.current_params = {}
        self.next_msg = ""

    def __getattr__(self, name):
        if name.startswith("on_heater"):
            return lambda *a, **k: self.calls.append(name)
        raise AttributeError(name)

    def next_step_message(self):
        return self.next_msg


def _rt():
    """런타임 최소 인스턴스 + 실제 _bind_process_controller 콜백 (test_runtime_heater 와 같은 방식)."""
    rt = ChamberRuntime.__new__(ChamberRuntime)
    rt.ch = 2
    rt.logs = []
    rt.append_log = lambda s, m: rt.logs.append((s, m))
    rt._get_text = lambda name: ""
    rt.supports_rf_pulse = rt.supports_dc_pulse = rt.supports_dc_cont = rt.supports_rf_cont = True
    rt.supports_dc_cont2 = False
    rt._loop = asyncio.get_running_loop()
    rt._ensure_task_alive = lambda *a, **k: None
    rt._heater = None
    rt._heater_gen = 0
    rt._heater_job = None
    rt._heater_watch = False
    rt._heater_rate = None
    rt._runner_queue_mode = True
    rt.process_queue = []
    rt.current_process_index = -1
    rt._bind_process_controller()
    pc = rt.process_controller
    rec = _PCRec()
    rt.process_controller = rec
    return rt, pc, rec


async def _tick(n=5):
    for _ in range(n):
        await asyncio.sleep(0)


async def _until(svc, plc, clk, cond, limit_s=20000, step=5):
    t0 = clk.t
    while not cond():
        await sim.run(svc, plc, clk, step)
        await _tick()
        assert clk.t - t0 < limit_s, "시간 초과"


# ───────────────────────── 시나리오 ─────────────────────────
async def _sc_manual_ok():
    svc, plc, clk, logs, chat = _make()
    await sim.run(svc, plc, clk, 5)
    ok, why = await svc.turn_on(150.0, owner="manual")
    assert ok, why
    await sim.run(svc, plc, clk, 600)
    await svc.turn_off("정지(패널)", owner="manual")
    await sim.run(svc, plc, clk, 3)
    return chat.full, logs


async def _sc_manual_stall():
    svc, plc, clk, logs, chat = _make()
    await sim.run(svc, plc, clk, 5)
    await svc.turn_on(300.0, owner="manual")             # MV 520 으로 도달 불가 → 승온 정체
    await sim.run(svc, plc, clk, 5400)
    await svc.turn_off("정지(패널)", owner="manual")
    await sim.run(svc, plc, clk, 3)
    return chat.full, logs


async def _sc_manual_ot():
    svc, plc, clk, logs, chat = _make()
    await sim.run(svc, plc, clk, 5)
    await svc.turn_on(150.0, owner="manual")
    await sim.run(svc, plc, clk, 120)
    plc.tc_offset = 600.0                                # TC 가 OT(550°C) 넘게 읽힘 → 래더 과온 트립·RUN 리셋
    await sim.run(svc, plc, clk, 5)
    return chat.full, logs


async def _process_run(rt, pc, rec, svc, plc, clk, name, target, nxt):
    """공정 1행: 이름·다음 스텝을 넣고 HEATER_SET → 승온 대기 통과까지."""
    rec.current_params = {"process_note": name}
    rec.next_msg = nxt
    rec.calls.clear()
    pc._heater_set(float(target), None)
    await _tick()
    await sim.run(svc, plc, clk, 2)
    assert rec.calls == ["on_heater_set_ok"], rec.calls
    pc._heater_wait(float(target))
    await _until(svc, plc, clk, lambda: "on_heater_ready" in rec.calls)


async def _sc_process(detail, *, user_stop=True, reach=True):
    rt, pc, rec = _rt()
    svc, plc, clk, logs, chat = _make(mv_limit=700)
    rt.attach_heater(svc)
    rt.process_queue = [{"use_heater": "T", "Process_name": "STO_A"}]
    rt.current_process_index = 0
    await sim.run(svc, plc, clk, 5)
    if reach:
        await _process_run(rt, pc, rec, svc, plc, clk, "STO_A", 200.0, "Main Shutter Open")
    else:
        rec.current_params = {"process_note": "STO_A"}
        pc._heater_set(200.0, None)
        await _tick()
        await sim.run(svc, plc, clk, 60)
    rt._heater_stop_user = user_stop
    await rt._heater_after_row(ok=detail.get("result") == "SUCCESS", stopped=bool(detail.get("stopped")), detail=detail)
    await sim.run(svc, plc, clk, 3)
    assert not svc.status()["run"] and svc.owner is None
    return chat.full, logs


async def _sc_queue_two():
    rt, pc, rec = _rt()
    svc, plc, clk, logs, chat = _make(mv_limit=700)
    rt.attach_heater(svc)
    rt.process_queue = [{"use_heater": "T", "Process_name": "STO_A"}, {"use_heater": "T", "Process_name": "STO_B"}]
    rt.current_process_index = 0
    await sim.run(svc, plc, clk, 5)
    await _process_run(rt, pc, rec, svc, plc, clk, "STO_A", 200.0, "Main Shutter Open")
    await rt._heater_after_row(ok=True, stopped=False, detail={"result": "SUCCESS"})
    await sim.run(svc, plc, clk, 30)
    assert svc.status()["run"] and svc.owner == "process"           # 다음 공정도 히터 → 유지
    rt.current_process_index = 1
    await _process_run(rt, pc, rec, svc, plc, clk, "STO_B", 220.0, "Gun Shutter Open")
    await rt._heater_after_row(ok=True, stopped=False, detail={"result": "SUCCESS"})
    await sim.run(svc, plc, clk, 3)
    return chat.full, logs


async def _sc_many_events():
    svc, plc, clk, logs, chat = _make()
    await sim.run(svc, plc, clk, 5)
    await svc.turn_on(150.0, owner="manual")
    await sim.run(svc, plc, clk, 30)
    for lb in ("승온 정체", "DAC 포화", "출력 죽음", "유지 모드 포기", "유지 모드 강제 진입", "승온 정체"):
        svc._event(lb)
    await svc.turn_off("정지(패널)", owner="manual")
    await sim.run(svc, plc, clk, 3)
    return chat.full, logs


SCENARIOS = {
    "1 수동 정상": lambda: _sc_manual_ok(),
    "2 수동 + 승온 정체": lambda: _sc_manual_stall(),
    "3 운전 중 과온": lambda: _sc_manual_ot(),
    "4 공정 성공": lambda: _sc_process({"result": "SUCCESS"}),
    "5a 공정 사용자 STOP": lambda: _sc_process({"result": "STOP", "stopped": True}, user_stop=True, reach=False),
    "5b 공정 원격 STOP": lambda: _sc_process({"result": "STOP", "stopped": True}, user_stop=False, reach=False),
    "5c 공정 실패": lambda: _sc_process({"result": "FAIL", "error_code": "E832", "error_detail": "x"}, reach=False),
    "6 연속 실행 2개": lambda: _sc_queue_two(),
    "7 특이사항 5개 이상": lambda: _sc_many_events(),
}


def _run(key):
    return asyncio.run(SCENARIOS[key]())


# ───────────────────────── 테스트 ─────────────────────────
def test_manual_ok_start_and_end_success():
    cards, logs = _run("1 수동 정상")
    assert [c[0] for c in cards] == [T + "시작", T + "종료"]
    s, e = cards
    assert s[1] == "수동" and s[2] == "INFO"
    assert list(s[3]) == ["목표", "램프", "현재 PV"]                   # 유지 모드 off → 필드 없음
    assert s[3]["목표"] == "150.0°C" and s[3]["램프"] == "12°C/min"
    assert e[1] == "수동 · 정지(패널)" and e[2] == "SUCCESS"
    assert e[3]["사유"] == "정지(패널)" and e[3]["특이사항"] == "없음"
    assert re.fullmatch(r"\d+분", e[3]["운전 시간"]) and e[3]["운전 시간"] == "10분"
    assert any("운전 종료 — 사유 정지(패널)" in m and "특이사항 없음" in m for _t, m in logs)


def test_start_card_shows_hold_mode_when_dac():
    async def go():
        svc, plc, clk, logs, chat = _make(hold_mode="dac")
        await sim.run(svc, plc, clk, 5)
        await svc.turn_on(150.0)
        await sim.run(svc, plc, clk, 3)
        return chat.full
    s = _only(asyncio.run(go()), "시작")[0]
    assert s[3]["유지 모드"] == "dac"


def test_duration_format():
    assert HeaterService._fmt_duration(59) == "0분"
    assert HeaterService._fmt_duration(59 * 60 + 59) == "59분"
    assert HeaterService._fmt_duration(3600) == "1시간 0분"
    assert HeaterService._fmt_duration(2 * 3600 + 5 * 60 + 30) == "2시간 5분"


def test_manual_stall_fail_card_and_end_fail():
    cards, _ = _run("2 수동 + 승온 정체")
    stall = _only(cards, "승온 정체")
    assert len(stall) == 1 and stall[0][2] == "FAIL" and stall[0][1].startswith("수동 · 목표 300.0°C")
    e = _only(cards, "종료")
    assert len(e) == 1 and e[0][2] == "FAIL" and e[0][1] == "수동 · 정지(패널)"
    assert "승온 정체" in e[0][3]["특이사항"]


def test_overtemp_during_run_fault_card_and_end_fail():
    cards, _ = _run("3 운전 중 과온")
    f = _only(cards, "이상")
    assert len(f) == 1 and f[0][2] == "FAIL" and f[0][1] == "수동 · 이상 — 과온"
    e = _only(cards, "종료")
    assert len(e) == 1 and e[0][2] == "FAIL" and e[0][1] == "수동 · 이상 — 과온"
    assert e[0][3]["사유"] == "이상 — 과온" and e[0][3]["특이사항"] == "이상 — 과온"


def test_process_success_start_reached_end():
    cards, _ = _run("4 공정 성공")
    assert [c[0] for c in cards] == [T + "시작", T + "도달", T + "종료"]
    s, r, e = cards
    assert s[1] == '공정 "STO_A"' and s[2] == "INFO" and s[3]["목표"] == "200.0°C"
    assert r[1] == '공정 "STO_A"' and r[2] == "SUCCESS"
    assert list(r[3]) == ["목표", "도달 PV", "승온 소요", "다음 단계"]
    assert r[3]["목표"] == "200.0°C" and r[3]["다음 단계"] == "Main Shutter Open"
    assert re.fullmatch(r"\d+분 \d+초", r[3]["승온 소요"])
    assert e[1] == '공정 "STO_A" · 공정 종료' and e[2] == "SUCCESS" and e[3]["특이사항"] == "없음"


@pytest.mark.parametrize("key,sub,status", [
    ("5a 공정 사용자 STOP", "공정 중단 — 사용자 STOP", "SUCCESS"),
    ("5b 공정 원격 STOP", "공정 중단 — 원격 STOP", "SUCCESS"),
    ("5c 공정 실패", "공정 중단 — 오류 (E832 공정 중 히터 이상)", "FAIL"),
])
def test_process_stop_and_fail_end_reason(key, sub, status):
    cards, logs = _run(key)
    e = _only(cards, "종료")
    assert len(e) == 1 and e[0][1] == f'공정 "STO_A" · {sub}' and e[0][2] == status
    assert e[0][3]["사유"] == sub
    assert e[0][3]["특이사항"] == ("없음" if status == "SUCCESS" else sub)
    assert not _only(cards, "도달")
    assert any(f"운전 종료 — 사유 {sub} ·" in m for _t, m in logs)


def test_process_fail_without_code_uses_detail_40():
    rt = ChamberRuntime.__new__(ChamberRuntime)
    long = "가" * 50
    assert rt._heater_process_end_reason(False, False, {"result": "FAIL", "error_detail": long}) == f"공정 중단 — 오류 ({'가' * 40})"
    assert rt._heater_process_end_reason(False, False, {"result": "FAIL"}) == "공정 중단 — 오류"
    assert rt._heater_process_end_reason(True, False, None) == "공정 종료"


def test_queue_two_processes_one_start_two_reached_one_end():
    cards, _ = _run("6 연속 실행 2개")
    assert [c[0] for c in cards] == [T + "시작", T + "도달", T + "도달", T + "종료"]
    s, r1, r2, e = cards
    assert s[1] == '공정 "STO_A" (1/2)'
    assert r1[1] == '공정 "STO_A" (1/2)' and r1[3]["목표"] == "200.0°C" and r1[3]["다음 단계"] == "Main Shutter Open"
    assert r2[1] == '공정 "STO_B" (2/2)' and r2[3]["목표"] == "220.0°C" and r2[3]["다음 단계"] == "Gun Shutter Open"
    assert e[1] == '공정 "STO_B" (2/2) · 공정 종료' and e[2] == "SUCCESS"


def test_many_events_capped_at_four():
    cards, _ = _run("7 특이사항 5개 이상")
    e = _only(cards, "종료")[0]
    assert e[2] == "FAIL"
    assert e[3]["특이사항"] == "승온 정체 · DAC 포화 · 출력 죽음 · 유지 모드 포기 외 1건"


def test_next_step_message_read_only():
    async def go():
        rt, pc, rec = _rt()
        from controller.process_controller import ProcessStep, ActionType
        pc.process_sequence = [ProcessStep(action=ActionType.HEATER_WAIT, message="히터 승온 대기", value=200.0),
                               ProcessStep(action=ActionType.DELAY, message="다음 스텝", duration=1000)]
        pc._current_step_idx = 0
        assert pc.next_step_message() == "다음 스텝" and pc._current_step_idx == 0
        pc._current_step_idx = 1
        assert pc.next_step_message() == ""
    asyncio.run(go())


if __name__ == "__main__":
    with contextlib.suppress(Exception):
        sys.stdout.reconfigure(encoding="utf-8")
    sys.exit(pytest.main([__file__, "-q"]))

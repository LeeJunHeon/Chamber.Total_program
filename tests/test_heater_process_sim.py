# -*- coding: utf-8 -*-
"""2단계(공정 연동) 히터 판정 — 래더·열 모델 에뮬레이터(tests/heater_ladder_sim.py) 시나리오.

공정 순서 모델: 진공 승온·도달 → IG 대기(진공 5분) → SP4 가스 충전(열손실 +5%) → 전원 ON + SP1 작업 압력
(열손실 +25%, 플라즈마 +20°C 환산) → [유지 모드 측정] → 셔터 딜레이 5분 → 메인 셔터 개방(TC 표시 흔들림) → 증착 30분.
열 모델 값은 실제 CH2 값이 아니다 — 유지값이 조건에 따라 달라지는 것을 재현하려고 고른 값이다.
"""
import os
import sys
import asyncio

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)

import pytest                                   # noqa: E402
import heater_ladder_sim as sim                 # noqa: E402
from controller.heater_process import (         # noqa: E402
    HeaterSoakWaiter, HeaterHoldWaiter, heater_wait_timeout_s,
)

GAS_FILL_LOSS, WORK_LOSS, PLASMA_C = 0.05, 0.25, 20.0


def _logs(logs, word):
    return [m for t, m in logs if word in m]


def _cards(chat, word):
    return [c for c in chat.cards if word in c[1]]


async def _heat(svc, plc, clk, target=200.0, process=True):
    """공정처럼 승온(유지 금지) — HeaterSoakWaiter 통과까지."""
    await sim.run(svc, plc, clk, 5)
    if process:
        svc.take_over("process")
        await svc.set_hold_armable(False, "공정 승온")
    ok, why = await svc.turn_on(target, rate=12.0, owner="process" if process else "manual")
    assert ok, why
    w = HeaterSoakWaiter(target, tol=2.0, soak_sec=60, timeout_s=7200, clock=clk)
    r = None
    while r is None:
        await sim.run(svc, plc, clk, 1)
        r = w.tick(svc.status(), svc.profiler.is_active())
    assert r[0], r[1]


async def _to_work_condition(svc, plc, clk):
    await sim.run(svc, plc, clk, 300)                 # IG 베이스 압력 대기(진공)
    plc.gas_loss = GAS_FILL_LOSS
    await sim.run(svc, plc, clk, 60)                  # SP4 가스 충전
    plc.gas_loss, plc.plasma_heat = WORK_LOSS, PLASMA_C
    await sim.run(svc, plc, clk, 30)                  # 전원 ON + SP1 작업 압력


async def _hold_wait(svc, plc, clk, timeout_s=300):
    hw = HeaterHoldWaiter(timeout_s=timeout_s, give_up_seq0=svc.hold_snapshot()["give_up_seq"], clock=clk)
    r = None
    while r is None:
        await sim.run(svc, plc, clk, 1)
        r = hw.tick(svc.status(), svc.hold_snapshot())
    return r + (hw.fail_kind,)


async def _deposit(svc, plc, clk, tc_offset):
    tmax = [0.0]

    def step(_s, p):
        tmax[0] = max(tmax[0], p.T)
    await sim.run(svc, plc, clk, 300, on_step=step)   # 셔터 딜레이
    plc.tc_offset = tc_offset                         # 메인 셔터 개방
    await sim.run(svc, plc, clk, 1800, on_step=step)  # 증착
    return tmax[0]


@pytest.mark.parametrize("tc_offset", [0.0, -100.0])
def test_capture_in_work_condition_keeps_temperature(tc_offset):
    """작업 압력·플라즈마 조건에서 잡은 유지값은 셔터 개방 때 TC 가 100°C 낮게 읽혀도 실제 온도를 지킨다."""
    async def go():
        svc, plc, clk, logs, chat = sim.make(mv_limit=700)
        await _heat(svc, plc, clk)
        assert not svc.hold.is_holding()                     # 진공에서는 잡지 않는다
        await _to_work_condition(svc, plc, clk)
        await svc.set_hold_armable(True, "")
        r = await _hold_wait(svc, plc, clk)
        assert r[0], r[1]
        assert 500 <= svc.hold.value <= 512 and plc.D[118] == svc.hold.value
        tmax = await _deposit(svc, plc, clk, tc_offset)
        assert abs(plc.T - 200.0) <= 1.5 and tmax <= 202.0
    asyncio.run(go())


def test_vacuum_capture_starves_under_gas():
    """대조: 진공에서 잡힌 유지값(챔버K식)은 가스·플라즈마 조건에서 모자라 실제 온도가 10°C 넘게 내려간다."""
    async def go():
        svc, plc, clk, logs, chat = sim.make(mv_limit=700)
        await _heat(svc, plc, clk, process=False)
        await sim.run(svc, plc, clk, 300)
        assert svc.hold.is_holding() and svc.hold.value <= 498
        await _to_work_condition(svc, plc, clk)
        await _deposit(svc, plc, clk, 0.0)
        assert plc.T <= 190.0
    asyncio.run(go())


@pytest.mark.parametrize("gas_sec", [100, 200])
def test_gas_load_rise_not_clamped_and_guard_rearms(gas_sec):
    """출력 상한이 빠듯할 때(520) 가스 부하로 MV 가 상한에 붙어도 클램프하지 않고(경고 1회),
    유지 측정을 켠 직후에도 진공 이력으로 클램프하지 않는다(가스 전 평균 494 로 줄이면 온도가 내려간다).
    gas_sec=100: 포화 120초가 차기 전에 유지 측정을 켠다 — 켤 때 포화 진행을 버리지 않으면 여기서 494 로 클램프된다."""
    async def go():
        svc, plc, clk, logs, chat = sim.make(mv_limit=520)
        await _heat(svc, plc, clk)
        await sim.run(svc, plc, clk, 300)
        plc.gas_loss, plc.plasma_heat = WORK_LOSS, 0.0       # 플라즈마 없이 가스만 — MV 필요 ≈ 517
        d118 = []
        await sim.run(svc, plc, clk, gas_sec, on_step=lambda s, p: d118.append(p.D[118]))
        await svc.set_hold_armable(True, "")
        await sim.run(svc, plc, clk, 600, on_step=lambda s, p: d118.append(p.D[118]))
        assert min(d118) == 520
        assert len(_logs(logs, "클램프하지 않음")) <= 1
        assert not _logs(logs, "로 클램프")
        assert abs(plc.T - 200.0) <= 1.5
    asyncio.run(go())


def test_hold_give_up_is_reported_once_after_start():
    """상한에 붙은 채면 유지 모드가 포기 — 포기 직후 다음 측정이 시작돼도 대기가 놓치지 않는다(E833)."""
    async def go():
        svc, plc, clk, logs, chat = sim.make(mv_limit=520)
        await _heat(svc, plc, clk)
        await sim.run(svc, plc, clk, 300)
        plc.gas_loss = WORK_LOSS
        await sim.run(svc, plc, clk, 200)
        await svc.set_hold_armable(True, "")
        r = await _hold_wait(svc, plc, clk)
        assert r[0] is False and r[2] == "E833" and r[3] == "give_up" and "포기" in r[1]
        assert len(_cards(chat, "유지 모드 포기")) == 1
        assert plc.D[118] == 520                              # PID 가 상한까지 그대로 쓴다
    asyncio.run(go())


def test_hold_timeout_then_force_uses_recent_average():
    """시간 안에 못 잡으면(진입 창 600초 > 대기 120초) 강제 진입 — 최근 평균값으로 고정해 셔터 개방에도 온도를 지킨다."""
    async def go():
        svc, plc, clk, logs, chat = sim.make(mv_limit=700, hold_enter_sec=600.0)
        await _heat(svc, plc, clk)
        await _to_work_condition(svc, plc, clk)
        await sim.run(svc, plc, clk, 90)
        await svc.set_hold_armable(True, "")
        r = await _hold_wait(svc, plc, clk, timeout_s=120)
        assert r[0] is False and r[2] == "E833" and r[3] == "timeout" and "시간 초과" in r[1]
        ok, why = await svc.force_hold("유지 모드 진입 시간 초과")
        assert ok, why
        assert svc.hold.is_holding() and 500 <= svc.hold.value <= 512 and plc.D[118] == svc.hold.value
        assert len(_cards(chat, "강제 진입")) == 1
        await _deposit(svc, plc, clk, -100.0)
        assert abs(plc.T - 200.0) <= 2.0
    asyncio.run(go())


def test_disarm_releases_hold_and_run_off_restores():
    """수동 운전에서 잡힌 유지를 공정이 넘겨받아 금지하면 D118 이 바로 상한으로 돌아가고, 금지 동안은 다시 잡지 않는다.
    RUN OFF 뒤에는 수동 운전처럼 다시 허용된다."""
    async def go():
        svc, plc, clk, logs, chat = sim.make(mv_limit=700)
        await _heat(svc, plc, clk, process=False)
        await sim.run(svc, plc, clk, 300)
        assert svc.hold.is_holding()
        svc.take_over("process")
        assert _logs(logs, "넘겨받음")
        await svc.set_hold_armable(False, "공정이 작업 조건에서 다시 측정")
        assert not svc.hold.is_holding() and plc.D[118] == 700
        assert svc.hold.last_window_mv is None               # 진공 측정값을 강제 진입에 쓰지 않는다
        await sim.run(svc, plc, clk, 600)
        assert not svc.hold.is_holding() and plc.D[118] == 700
        ok, why = await svc.turn_off("정지", owner="manual")
        assert not ok and "공정" in why
        ok, why = await svc.apply_target(150.0, owner="manual")
        assert not ok
        ok, why = await svc.turn_off("공정 종료", owner="process")
        assert ok, why
        await sim.run(svc, plc, clk, 5)
        assert svc.owner is None and svc.hold.armable and svc.sat.clamp_enabled
    asyncio.run(go())


def test_panel_can_still_stop_external_run():
    """외부(프로그램 밖)에서 켠 RUN 은 지금처럼 패널(수동)로 끌 수 있어야 한다 — 소유 검사는 '공정'만 보호한다."""
    async def go():
        svc, plc, clk, logs, chat = sim.make()
        await sim.run(svc, plc, clk, 5)
        await plc.write_reg(112, 1500)                     # SV 150.0°C
        await plc.write_coil(640, True)                    # 외부에서 RUN
        await sim.run(svc, plc, clk, 3)
        assert svc.status()["run"] and svc.owner == "external"
        ok, why = await svc.apply_target(140.0)            # 패널 [적용] 은 owner 없이 부른다
        assert ok, why
        ok, why = await svc.turn_off("정지(패널)", owner="manual")
        assert ok, why
        await sim.run(svc, plc, clk, 3)
        assert not svc.status()["run"] and svc.owner is None
    asyncio.run(go())


def test_soak_waiter_failures_and_timeout_estimate():
    clk = sim.Clock()
    base = {"run": True, "fault": False, "stale": False, "pv": 150.0, "sv": 200.0, "sv_ramp": 200.0}
    w = HeaterSoakWaiter(200.0, tol=2.0, soak_sec=60, timeout_s=600, clock=clk)
    assert w.tick(dict(base), False) is None
    clk.t += 601
    r = w.tick(dict(base), False)
    assert r[0] is False and r[2] == "E831"
    w = HeaterSoakWaiter(200.0, tol=2.0, soak_sec=60, timeout_s=600, clock=clk)
    assert w.tick({**base, "fault": True}, False)[2] == "E832"
    assert w.tick({**base, "run": False}, False)[2] == "E830"
    assert w.tick({**base, "stale": True}, False) is None
    clk.t += 61
    assert w.tick({**base, "stale": True}, False)[2] == "E830"
    w = HeaterSoakWaiter(200.0, tol=2.0, soak_sec=60, timeout_s=600, clock=clk)
    st = {**base, "pv": 199.0}
    assert w.tick(st, True) is None                      # 램프 중이면 도달 아님
    assert w.tick(st, False) is None
    clk.t += 60
    assert w.tick(st, False)[0] is True
    t, why = heater_wait_timeout_s(25.0, 200.0, 12.0, base_timeout_s=3600, approach_zone=20.0,
                                   approach_min_rate=1.0, soak_sec=60)
    assert t == 3600 and "기본값" in why
    t, why = heater_wait_timeout_s(25.0, 450.0, 6.0, base_timeout_s=3600, approach_zone=20.0,
                                   approach_min_rate=1.0, soak_sec=60)
    assert t > 3600 and "예상 승온" in why
    t, why = heater_wait_timeout_s(400.0, 200.0, 12.0, base_timeout_s=3600, approach_zone=20.0,
                                   approach_min_rate=1.0, soak_sec=60)
    assert t >= 200 * 60 + 1800 and "냉각" in why
    t, why = heater_wait_timeout_s(None, 200.0, 12.0, base_timeout_s=3600, approach_zone=20.0,
                                   approach_min_rate=1.0, soak_sec=60)
    assert t == 3600

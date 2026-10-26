# -*- coding: utf-8 -*-
"""DAC 포화 클램프와 출력 끊김(B) 감시의 경계 — controller/heater_response.py · heater_saturation.py (2026-10-26 검토).

예전: 출력 한계 근처(850°C, MV 약 1107)에서 부하가 늘어 MV 가 상한(1200)에 120초 붙으면 포화 감시가 D118 을
포화 직전 평균(약 1117)으로 클램프 → 줄인 출력 때문에 PV 가 내려감 → 출력 끊김 감시가 '상한인데 PV 하강'으로 보고
히터를 끄고(❌ 출력 끊김) 공정이 E832 로 멈췄다.
지금: 클램프가 걸릴 때 PV 가 이미 출력 끊김 기준 속도로 떨어지고 있었으면(출력 끊김 의심) 클램프 중에도 그대로 보고,
아니면 그 클램프 동안은 보지 않는다. 도달 뒤 출력 끊김·TC 분리·통신 끊김이 섞인 경우의 결과는 예전과 같다.
래더·열 에뮬레이터(tests/heater_ladder_sim.py)와 가짜 시계, 장치 접근 없음.
"""
import os
import re
import sys
import asyncio
import contextlib

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402
import heater_ladder_sim as sim                                  # noqa: E402
from controller.heater_process import HeaterSoakWaiter           # noqa: E402
from controller.heater_response import HeaterResponseGuard       # noqa: E402
from controller.heater_saturation import HeaterSaturationGuard   # noqa: E402


# ───────────────────────── 에뮬레이터: 공정처럼 도달 → HEATER_HOLD(클램프 켬) → 사건 ─────────────────────────
def _mk(gain):
    """장비 설정값(settings.json ch2)에 맞춘 히터 — 상한 1200, 절대 상한 1300, 램프 6°C/min, 유지 모드 off."""
    return sim.make(gain=gain, mv_limit=1200, mv_abs_max=1300, ramp_rate=6.0, hold_mode="off",
                    max_temp=900.0, sv_limit_c=900.0, ot_limit_c=950.0)


async def _reach(gain, target):
    """공정처럼: 히터 설정(클램프 꺼짐) → 도달 확인 → 300초 → HEATER_HOLD(유지 off → 포화 클램프 켬) → 120초."""
    svc, plc, clk, logs, chat = _mk(gain)
    pub = []
    svc.add_listener(lambda st: pub.append(st.get("noresp")))
    await sim.run(svc, plc, clk, 5)
    svc.take_over("process")
    await svc.set_hold_armable(False, "공정이 작업 조건에서 유지값을 다시 잰다")
    ok, why = await svc.turn_on(target, rate=12.0, owner="process")
    assert ok, why
    w = HeaterSoakWaiter(target, tol=2.0, soak_sec=60, timeout_s=6 * 3600, clock=clk)
    r = None
    while r is None:
        await sim.run(svc, plc, clk, 1)
        r = w.tick(svc.status(), svc.profiler.is_active())
    assert r[0], r[1]
    await sim.run(svc, plc, clk, 300)
    await svc.set_hold_armable(True, "")
    await sim.run(svc, plc, clk, 120)
    return svc, plc, clk, logs, chat, pub


def _trips(logs, t0=0.0):
    return [(t - t0, m) for t, m in logs if ("히터 출력 끊김:" in m or "히터 출력 무응답:" in m) and t >= t0]


def _clamps(logs, t0=0.0):
    return [(t - t0, m) for t, m in logs if "로 클램프" in m and t >= t0]


@pytest.mark.parametrize("gain,target,load", [(1400.0, 850.0, 0.2), (1400.0, 800.0, 0.2), (1400.0, 900.0, 0.1)])
def test_overload_near_capability_clamp_keeps_heater_on(gain, target, load):
    """출력 한계 근처에서 부하 증가 → 포화 클램프 → PV 하강: 예전엔 '출력 끊김'으로 히터 OFF·E832, 지금은 계속 운전."""
    async def go():
        svc, plc, clk, logs, chat, pub = await _reach(gain, target)
        t0 = clk.t
        plc.gas_loss = load                                       # 증착 중 부하 증가(가스 등)
        await sim.run(svc, plc, clk, 1800)
        assert _clamps(logs, t0), [m for _t, m in logs][-10:]     # 포화 클램프가 걸린 시나리오다
        assert svc.sat.clamped is not None and plc.D[118] == svc.sat.clamped
        assert not _trips(logs, t0), _trips(logs, t0)             # 예전: 3~7분 뒤 '히터 출력 끊김' → OFF
        assert plc.M[640] and svc.status()["run"]
        assert not [x for x in pub if x]                          # 공정 E832 로 이어지는 noresp 없음
        sat_cards = [c for c in chat.cards if "포화" in c[1]]
        assert sat_cards and sat_cards[0][3] == "FAIL"            # 클램프는 ❌ 'DAC 포화' 카드로 알린다
    asyncio.run(go())


@pytest.mark.parametrize("gain,target", [(1400.0, 850.0), (1400.0, 700.0), (700.0, 200.0)])
def test_output_loss_after_arrival_still_turns_off(gain, target):
    """도달 뒤 출력 끊김: MV 가 상한에 붙어 포화 클램프(120초)가 B(180초)보다 먼저 걸려도 B 가 끝까지 보고 끈다."""
    async def go():
        svc, plc, clk, logs, chat, pub = await _reach(gain, target)
        t0 = clk.t
        plc.gain = 0.0                                            # 차단기·배선 끊김 흉내
        await sim.run(svc, plc, clk, 600)
        tr = [(t, m) for t, m in _trips(logs, t0) if "히터 출력 끊김:" in m]
        assert len(tr) == 1 and tr[0][0] <= 300, _trips(logs, t0)    # 예전과 같은 시점(193~257초)
        cl = _clamps(logs, t0)
        assert cl and cl[0][0] < tr[0][0]                          # 클램프가 먼저 걸렸는데도 잡는다
        assert not plc.M[640]
        assert [x for x in pub if x][0].startswith("출력 끊김 — 출력 상한(MV ")
    asyncio.run(go())


def test_output_loss_with_comm_gap_before_clamp_still_turns_off():
    """출력 끊김 직후 통신이 15초 끊겨 B 구간이 처음부터 다시 시작돼도 — 포화 감시가 속도를 잴 때까지 클램프를 미루고 B 가 끈다."""
    async def go():
        svc, plc, clk, logs, chat, pub = await _reach(1400.0, 850.0)
        t0 = clk.t
        plc.gain = 0.0

        def gap(_s, p):
            p.connected = not (60.0 <= clk.t - t0 < 75.0)
        await sim.run(svc, plc, clk, 600, on_step=gap)
        tr = [t for t, m in _trips(logs, t0) if "히터 출력 끊김:" in m]
        assert len(tr) == 1 and tr[0] <= 300 and not plc.M[640]
        assert [m for t, m in logs if "하강 속도를 재는 중" in m and t >= t0]   # 클램프를 잠시 미룸(로그 1줄)
    asyncio.run(go())


def test_tc_step_detach_clamp_timing_unchanged():
    """TC 가 떨어져 실제의 절반으로 읽힘(PV 급락 뒤 평탄) — 클램프는 예전처럼 상한 120초에 걸리고, 히터는 끄지 않는다."""
    async def go():
        svc, plc, clk, logs, chat, pub = await _reach(1400.0, 400.0)
        t0 = clk.t
        top = [plc.T]

        def detach(_s, p):
            p.tc_offset = -(p.T - 25.0) * 0.5
            top[0] = max(top[0], p.T)
        detach(svc, plc)
        await sim.run(svc, plc, clk, 1800, on_step=detach)
        cl = _clamps(logs, t0)
        assert cl, [m for _t, m in logs][-10:]
        sec = int(re.search(r"도달 후 (\d+)초째", cl[0][1]).group(1))
        assert sec <= svc.cfg.sat_sec + 2                         # 미루지 않음(속도를 잴 수 있고 하강이 아님)
        assert not _trips(logs, t0) and plc.M[640]
        assert top[0] < 480.0 and plc.T < 420.0                   # 클램프가 실제 온도를 묶는다(예전과 같음)
    asyncio.run(go())


def test_fast_cooling_at_limit_before_clamp_still_trips():
    """상한에서 이미 출력 끊김 기준 속도로 식는 중에 클램프가 걸리면(부하 +40%) 예전처럼 '출력 끊김'으로 끈다 — 설계상 같은 증상."""
    async def go():
        svc, plc, clk, logs, chat, pub = await _reach(1400.0, 850.0)
        t0 = clk.t
        plc.gas_loss = 0.4
        await sim.run(svc, plc, clk, 900)
        assert [t for t, m in _trips(logs, t0) if "히터 출력 끊김:" in m] and not plc.M[640]
    asyncio.run(go())


def test_output_loss_after_clamp_released_still_trips():
    """클램프가 풀리면(목표 변경) 감시가 다시 판정한다 — 그 뒤 실제 출력 끊김은 잡는다."""
    async def go():
        svc, plc, clk, logs, chat, pub = await _reach(1400.0, 850.0)
        plc.gas_loss = 0.2
        await sim.run(svc, plc, clk, 600)
        assert svc.sat.clamped is not None
        plc.gas_loss = 0.0
        ok, why = await svc.apply_target(700.0, owner="process")   # 목표 변경 → 클램프 해제(D118 원복)
        assert ok, why
        await sim.run(svc, plc, clk, 30)
        assert svc.sat.clamped is None and plc.D[118] == 1200
        await sim.run(svc, plc, clk, 3600)                          # 700°C 에 자리 잡음
        assert plc.M[640] and not _trips(logs)
        plc.gain = 0.0
        t1 = clk.t
        await sim.run(svc, plc, clk, 600)
        assert [t for t, m in _trips(logs, t1) if "히터 출력 끊김:" in m] and not plc.M[640]
    asyncio.run(go())


# ───────────────────────── 판정 단위(가짜 시계) ─────────────────────────
class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _st(pv, mv, limit=1200):
    return {"run": True, "ok": True, "fault": False, "stale": False, "pv": pv, "mv": mv, "mv_limit": limit}


def _drive(g, clk, secs, pv_fn, mv_fn, limit_fn, clamped_fn, target=850.0):
    """1초마다 tick. 판정되면 (경과초, info) — 없으면 None."""
    for i in range(int(secs)):
        r = g.tick(_st(pv_fn(i), mv_fn(i), limit_fn(i)), target, clamped=clamped_fn(i))
        clk.t += 1.0
        if r is not None:
            return i, r
    return None


def test_unit_clamp_during_fast_fall_keeps_judging():
    """상한에서 0.2°C/s 로 떨어지는 중(목표보다 10°C 아래부터) 120초에 클램프 → 클램프 중에도 B 가 보고 180초에 판정."""
    clk = _Clock()
    g = HeaterResponseGuard(clock=clk)
    r = _drive(g, clk, 600, lambda i: 840.0 - 0.2 * i, lambda i: 1200 if i < 120 else 1117,
               lambda i: 1200 if i < 120 else 1117, lambda i: i >= 120)
    assert r is not None and r[1]["kind"] == "loss" and r[0] == 180


def test_unit_clamp_without_fall_rests_until_released():
    """PV 가 목표 근처에서 상한(클램프 전 하강 없음) → 클램프 뒤 PV 가 빨리 떨어져도 그 클램프 동안은 판정하지 않는다.
    클램프가 풀리면(D118 원복) 다시 본다."""
    clk = _Clock()
    g = HeaterResponseGuard(clock=clk)
    pv = lambda i: 849.0 if i < 120 else 849.0 - 0.2 * (i - 120)                     # noqa: E731
    r = _drive(g, clk, 900, pv, lambda i: 1200 if i < 120 else 1117,
               lambda i: 1200 if i < 120 else 1117, lambda i: i >= 120)
    assert r is None and not g.tripped
    r = _drive(g, clk, 600, lambda i: 669.0 - 0.2 * i, lambda i: 1200, lambda i: 1200, lambda i: False)
    assert r is not None and r[1]["kind"] == "loss"


def test_unit_loss_wait_only_while_window_short():
    clk = _Clock()
    g = HeaterResponseGuard(clock=clk)
    assert not g.loss_wait()
    _drive(g, clk, 30, lambda i: 840.0, lambda i: 1200, lambda i: 1200, lambda i: False)
    assert g.loss_wait()                                       # B 구간 30초 — 아직 속도를 못 잼
    _drive(g, clk, 40, lambda i: 840.0, lambda i: 1200, lambda i: 1200, lambda i: False)
    assert not g.loss_wait()                                   # 60초 넘음 — 잴 수 있음
    g2 = HeaterResponseGuard(enabled=False, clock=_Clock())
    assert not g2.loss_wait()


def _sat():
    clk = _Clock()
    ev = {"req": [], "msg": []}
    g = HeaterSaturationGuard(mv_limit=1200, mv_min=400, sat_sec=120, arrive_tol_c=1.0, clock=clk,
                              request_mv_limit=ev["req"].append,
                              message=lambda lv, t: ev["msg"].append((lv, t)))
    return g, clk, ev


def _feed(g, clk, secs, mv, defer, pv=850.0):
    for _ in range(int(secs)):
        g.tick({"ok": True, "run": True, "fault": False, "pv": pv, "sv": 850.0, "sv_ramp": 850.0,
                "mv": mv, "mv_limit": 1200}, False, "dac", final_target=850.0, defer_clamp=defer)
        clk.t += 1.0


def test_unit_saturation_defers_clamp_then_uses_same_pre_average():
    g, clk, ev = _sat()
    _feed(g, clk, 60, 1100, False)                             # 도달·무장(상한 아래)
    _feed(g, clk, 125, 1200, True)                             # 상한 125초 — 미룸
    assert g.clamped is None and not ev["req"]
    assert len([t for _lv, t in ev["msg"] if "하강 속도를 재는 중" in t]) == 1
    _feed(g, clk, 30, 1200, True)
    assert g.clamped is None and len([t for _lv, t in ev["msg"] if "하강 속도를 재는 중" in t]) == 1
    _feed(g, clk, 1, 1200, False)                              # 잴 수 있게 됨 → 클램프
    assert ev["req"] == [1100] and g.clamped == 1100           # 미루지 않았을 때와 같은 '포화 직전 평균'


def test_unit_defer_ignored_when_clamp_disabled():
    g, clk, ev = _sat()
    g.set_clamp_enabled(False)
    _feed(g, clk, 60, 1100, False)
    _feed(g, clk, 125, 1200, True)
    assert g.clamped is None and not ev["req"]
    assert [t for _lv, t in ev["msg"] if "클램프하지 않음" in t]      # 예전 그대로 경고만
    assert not [t for _lv, t in ev["msg"] if "하강 속도를 재는 중" in t]

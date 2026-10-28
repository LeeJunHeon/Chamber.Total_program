# -*- coding: utf-8 -*-
"""두께 모니터 영점·기준점 — 메인 셔터를 열기 전 THICKNESS_ZERO(영점 B + ZeroConfirm) 에서 기준점을 정한다.
- ZeroConfirm: 즉시 확인(0 nm), 이전 두께 350 nm 가 1~3번 읽힌 뒤 0, 첫 값 −3·+1.5 nm 확인, 5초 미확인 → 마지막 값 기준 + 경고,
  영점 실패 → 첫 값 기준 + 경고, 새 값 없음 → 기준점 없음.
- 셔터 연 뒤 열 충격(1·3·5 nm 하락 → 30초 회복): 기준점 0 이라 하락·회복은 장비 두께 그대로 — 일찍 끝나지 않는다.
- 런타임 THICKNESS_ZERO 콜백(가짜 서비스): 기준점·경고 목록(_run_warnings)·끝 알림, 기준점 없음 → 판정 시작 즉시 영구 시간 대체.
직접 실행하면 열 충격 시뮬레이션 표를 출력한다."""
import asyncio
import contextlib
import os
import random
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

from controller.thickness_process import (ThicknessEndJudge, ZeroConfirm, expected_seconds,  # noqa: E402
                                          max_time_s)
from lib.thickness_config import ThicknessJudgeParams            # noqa: E402

P = ThicknessJudgeParams()
TARGET = 100.0


# ═══════════════════════ ZeroConfirm(순수) ═══════════════════════
def test_zero_confirm_xtal_fail_is_immediate():
    """XTAL FAIL(통신 정상) — 5초 기다리지 않고 바로 '기준점 없음 — XTAL FAIL'."""
    zc = ZeroConfirm(P, start_t=0.0, seq0=0, zero_ok=True)
    r = zc.tick({"seq": 0, "thickness_nm": None, "crystal_ok": False, "link_ok": True, "stale": True}, 0.1)
    assert r is not None and r.base is None and "XTAL FAIL" in r.log and "XTAL FAIL" in r.warning
    zc = ZeroConfirm(P, start_t=0.0, seq0=0, zero_ok=True)                # 통신이 안 되면 기다린다
    assert zc.tick({"seq": 0, "crystal_ok": False, "link_ok": False, "stale": True}, 0.1) is None


def _confirm(values, *, zero_ok=True, dt=0.5, gap_after=None):
    """영점 뒤 dt 초마다 새 값. values 를 다 쓰면 마지막 값 반복(gap_after 면 그 뒤 새 값 없음). (결과, 시각)."""
    zc = ZeroConfirm(P, start_t=0.0, seq0=0, zero_ok=zero_ok)
    t, seq = 0.0, 0
    vals = list(values)
    while t < 20:
        t = round(t + dt, 6)
        if gap_after is not None and seq >= gap_after:
            r = zc.tick({"seq": seq, "thickness_nm": None}, t)
        else:
            seq += 1
            v = vals[min(seq - 1, len(vals) - 1)]
            r = zc.tick({"seq": seq, "thickness_nm": v}, t)
        if r is not None:
            return r, t
    raise AssertionError("결과 없음")


def test_immediate_zero():
    r, t = _confirm([0.0])
    assert r.base == 0.0 and t == 0.5 and r.warning == "" and "영점 확인" in r.log


@pytest.mark.parametrize("n_old", [1, 2, 3])
def test_old_thickness_then_zero(n_old):
    r, t = _confirm([350.0] * n_old + [0.1])
    assert r.base == 0.0 and t == 0.5 * (n_old + 1) and r.warning == ""


@pytest.mark.parametrize("v", [-3.0, 1.5, 2.0])
def test_negative_and_small_positive_confirm(v):
    r, _t = _confirm([v])
    assert r.base == 0.0


def test_not_confirmed_in_5s_uses_last_value_with_warning():
    r, t = _confirm([50.0, 50.1, 49.9, 50.2])
    assert t == pytest.approx(5.0) and r.base == pytest.approx(50.2)
    assert r.warning.startswith("영점 확인 안 됨 — 50.20 nm 를 기준점으로")


def test_zero_command_failed_takes_first_value_with_warning():
    r, t = _confirm([350.0, 0.0], zero_ok=False)
    assert r.base == 350.0 and t == 0.5 and r.warning.startswith("영점 명령 실패 — 첫 값 350.00 nm")


def test_no_values_means_no_base():
    r, t = _confirm([0.0], gap_after=0)
    assert r.base is None and t == pytest.approx(5.0) and "기준점 없음" in r.warning
    r, _t = _confirm([0.0], zero_ok=False, gap_after=0)
    assert r.base is None


def test_values_lost_after_old_reading_uses_last():
    """350 nm 한 번 읽고 끊김 — 5초 뒤 그 값을 기준점(경고). 영점이 안 먹었는지 알 수 없으므로 늘어난 양으로 판정."""
    r, _t = _confirm([350.0], gap_after=1)
    assert r.base == 350.0 and "영점 확인 안 됨" in r.warning


# ═══════════════════════ 셔터 연 뒤 열 충격(판정기) ═══════════════════════
def sim(rate=0.05, *, sigma=0.0, seed=0, dip=0.0, dip_at=1.0, recover_s=30.0):
    """기준점 0(셔터 열기 전 영점 확인). 셔터 연 뒤 dip_at 초에 dip nm 하락 → recover_s 초에 걸쳐 회복. (이벤트, 실제 두께, 판정기)"""
    rng = random.Random(seed)
    j = ThicknessEndJudge(TARGET, rate, P, start_t=0.0, base=0.0, seq0=0)
    limit = max_time_s(expected_seconds(TARGET, rate), P)
    t, seq = 0.0, 0
    while t < limit:
        t = round(t + 0.5, 6)
        true_nm = rate * t
        v = true_nm
        if dip and t >= dip_at:
            v -= dip * max(0.0, 1.0 - (t - dip_at) / recover_s)
        v = round(round((v + rng.gauss(0.0, sigma)) / 0.1) * 0.1, 6)
        seq += 1
        ev = j.tick({"seq": seq, "thickness_nm": v, "rate_nm_s": rate, "crystal_ok": True, "reset_seen": 0}, t)
        if ev is not None and ev.kind == "done":
            return ev, true_nm, j
    return None, rate * t, j


@pytest.mark.parametrize("dip", [1.0, 3.0, 5.0])
@pytest.mark.parametrize("sigma", [0.0, 0.1, 0.2])
@pytest.mark.parametrize("dip_at", [0.2, 1.0, 3.0])
def test_thermal_shock_does_not_end_early(dip, sigma, dip_at):
    for seed in range(4):
        ev, real, _j = sim(0.05, sigma=sigma, seed=seed, dip=dip, dip_at=dip_at)
        assert ev is not None and ev.kind == "done"
        assert real >= TARGET - (4 * sigma + 0.3), (dip, sigma, dip_at, seed, real)
        assert real - TARGET <= max(0.05 * 1.0, 0.3) + 4 * sigma


def test_negative_warning_uses_neg_warn_nm():
    _ev, _real, j = sim(0.05, dip=5.0, dip_at=1.0)
    assert any("음수" in e for e in j.events)                        # 기록만
    _ev, _real, j = sim(0.05, dip=1.0, dip_at=1.0)
    assert not any("음수" in e for e in j.events)


# ═══════════════════════ 런타임 THICKNESS_ZERO 콜백 ═══════════════════════
def _runtime(zero_ok=True):
    from test_thickness_runtime import FakeSvc, _rt
    svc = FakeSvc()

    async def zero(mode="B", why=""):
        svc.zero_calls.append(mode)
        return (True, "") if zero_ok else (False, "응답 없음")
    svc.zero = zero
    rt, pc, rec = _rt(svc)
    rt.cfg.mod.THICKNESS_ZERO_CONFIRM_S = 1.0
    return rt, pc, rec, svc


async def _feed(svc, values, period=0.1):
    """영점 뒤 period 초마다 새 값(values 를 다 쓰면 마지막 값 반복). values=None 이면 새 값 없음."""
    i = 0
    while True:
        await asyncio.sleep(period)
        if values is None:
            continue
        svc.st["seq"] += 1
        svc.st["thickness_nm"] = values[min(i, len(values) - 1)]
        i += 1


async def _zero_step(rt, pc, rec, svc, values, timeout=5.0):
    feeder = asyncio.ensure_future(_feed(svc, values))
    try:
        pc._thickness_zero(1.0)
        for _ in range(int(timeout / 0.05)):
            if "on_thickness_zero_done" in rec.names():
                break
            await asyncio.sleep(0.05)
    finally:
        feeder.cancel()
    assert "on_thickness_zero_done" in rec.names()
    return rt._thk_zero


def test_runtime_zero_step_confirms_and_sets_base_zero():
    async def go():
        rt, pc, rec, svc = _runtime()
        z = await _zero_step(rt, pc, rec, svc, [350.0, 0.0])
        assert svc.zero_calls == ["B"] and z["base"] == 0.0 and rt._run_warnings == []
        assert any("영점 확인" in m for _s, m in rt.logs)
        rec.current_params = {"dep_rate": 0.05}
        pc._thickness_start(100.0, 2000.0, 4000.0)                      # 셔터 연 뒤 판정 시작 — 기준점 0
        assert rt._thk_judge is not None and rt._thk_judge.base == 0.0
        assert "on_thickness_fallback" not in rec.names()
    asyncio.run(go())


def test_runtime_zero_step_not_confirmed_warns_and_uses_value():
    async def go():
        rt, pc, rec, svc = _runtime()
        z = await _zero_step(rt, pc, rec, svc, [50.0])
        assert z["base"] == 50.0 and any(w.startswith("영점 확인 안 됨") for w in rt._run_warnings)
    asyncio.run(go())


def test_runtime_zero_command_failed_warns_and_uses_first_value():
    async def go():
        rt, pc, rec, svc = _runtime(zero_ok=False)
        z = await _zero_step(rt, pc, rec, svc, [12.0, 0.0])
        assert z["base"] == 12.0 and any(w.startswith("영점 명령 실패") for w in rt._run_warnings)
    asyncio.run(go())


def test_runtime_zero_step_comm_lost_then_permanent_time_fallback():
    async def go():
        rt, pc, rec, svc = _runtime()
        z = await _zero_step(rt, pc, rec, svc, None)
        assert z["base"] is None and any("기준점 없음" in w for w in rt._run_warnings)
        rec.current_params = {"dep_rate": 0.05}
        pc._thickness_start(100.0, 2000.0, 4000.0)
        fb = [c for c in rec.calls if c[0] == "on_thickness_fallback"]
        assert fb and fb[0][1][0] == pytest.approx(2000.0)               # 예상시간 그대로(시간으로 진행)
        assert rt._thk_judge.permanent
    asyncio.run(go())


if __name__ == "__main__":
    print("셔터 연 뒤 열 충격(0.05 nm/s, 목표 100 nm, 30초 회복, 기준점 0, 도달 즉시 종료, 시드 4개) — 종료 시 실제 두께 최소/최대")
    for dip in (1.0, 3.0, 5.0):
        for sigma in (0.0, 0.1, 0.2):
            reals = [sim(0.05, sigma=sigma, seed=s, dip=dip, dip_at=da)[1] for s in range(4) for da in (0.2, 1.0, 3.0)]
            print(f"하락 {dip:g} nm · σ {sigma:g}: {min(reals):.2f} ~ {max(reals):.2f} nm")

# -*- coding: utf-8 -*-
"""RF 연속(device/rf_power.py) 안전장치 A~D — 2026-11-27 CH2 SiN_90W 사고(목표 90 W, FWD 62~71 W 정체, REF 49 W 고정,
요구값이 RF_MAX_POWER 600 까지 올라가고 인터락 뒤 600 W 부터 1 W 씩 11분 ramp-down).

A. 요구값 상한 + 추종 실패  B. 목표 도달 뒤 반사파 비율 한계  C. Blind 도달 연속 N회  D. ramp-down 시작값 = 실제 출력.
각 플래그가 False 면 예전 동작. direct_mode(플라즈마 클리닝)는 해당 없음. 장치·네트워크 접근 없음(가짜 발생기).
시간 기준(대기 초)만 줄여서 본다 — 판정식·단계 크기는 실제와 같다(속도를 위해 ramp 단계만 키운 시험이 있다).
"""
import asyncio
import contextlib
import os
import re
import sys
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

from device.rf_power import RFPowerAsync                         # noqa: E402
from lib import config_common, config_ch1, config_ch2            # noqa: E402
from lib import _config_loader as L                              # noqa: E402

REPO_JSON = Path(_ROOT) / "config" / "settings.json"
OFF = dict(_rf_follow_guard_enabled=False, _rf_ref_ratio_guard_enabled=False,
           _rf_blind_reach_consec_enabled=False, _rf_rampdown_from_actual_enabled=False)


def _mk(read=None, **attrs):
    """가짜 발생기. st['req'] = 요구값(W, 보정 전 좌표) 기록, st['unv'] = no-reply 쓰기(ramp-down·0 W)."""
    st = {"req": [], "unv": [], "enabled": None, "events": []}

    async def send(v):
        pass

    async def unv(v):
        st["unv"].append(float(v))

    async def toggle(on):
        st["enabled"] = bool(on)

    rf = RFPowerAsync(send_rf_power=send, send_rf_power_unverified=unv, request_status_read=read, toggle_enable=toggle)
    rf._poll_interval_ms = 20                   # 시험 속도(1초 → 20ms)
    rf._rampdown_interval_ms = 1
    rf._ref_th_w = 50.0                         # settings.json RF_REFLECTED_THRESHOLD_W
    for k, v in attrs.items():
        setattr(rf, k, v)
    orig = rf._send_rf_power

    async def rec(w):
        st["req"].append(float(w))
        await orig(w)
    rf._send_rf_power = rec
    return rf, st


def _drain(rf, st):
    while not rf._event_q.empty():
        e = rf._event_q.get_nowait()
        st["events"].append((e.kind, e.message or ""))
    return st["events"]


def _msgs(st, kind="status"):
    return [m for k, m in st["events"] if k == kind]


async def _feed(rf, fwd, ref=0.0, n=1, dt=0.005):
    for _ in range(n):
        rf._ingest_status_result({"forward": fwd, "reflected": ref})
        await asyncio.sleep(dt)


async def _until(cond, timeout=5.0, rf=None, st=None):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if rf is not None:
            _drain(rf, st)
        if cond():
            return True
        await asyncio.sleep(0.005)
    return False


def _failed(st):
    return [m for k, m in st["events"] if k == "target_failed"]


async def _stop(rf):
    rf._is_running = False
    for t in (rf._poll_task, rf._adjust_task, rf._rampdown_task):
        if t and not t.done():
            t.cancel()
    await asyncio.sleep(0.02)


async def _reach(rf, st, target=90.0):
    """blind 없음(임계 0) + ramp 50 W — 1 → 51 → 목표 → MAINTAINING."""
    await rf.start_process(target)
    for f in (1.0, 51.0, target):
        await _feed(rf, f)
    _drain(rf, st)
    assert rf.state == "MAINTAINING", st["events"][-5:]


# ═══════════════════════ 사고 재현 ═══════════════════════
_INCIDENT = [62.0, 65.0, 71.0, 68.0, 70.2, 64.0, 66.0, 69.0, 63.0, 71.0, 67.0]   # 70 이상이 연속 3회 되지 않음


def _incident_rf(**attrs):
    box = {"i": 0, "rf": None}

    async def read():
        rf = box["rf"]
        sp = rf._last_sent_w or 0.0
        v = _INCIDENT[box["i"] % len(_INCIDENT)]
        box["i"] += 1
        return {"forward": round(min(sp, v), 1), "reflected": 49.0}
    rf, st = _mk(read, _rf_ramp_step=5.0, _rf_blind_ramp_reach_timeout_s=2.0, **attrs)
    box["rf"] = rf
    return rf, st


def test_incident_capped_and_blind_timeout():
    async def go():
        rf, st = _incident_rf()
        await rf.start_process(90.0)
        assert await _until(lambda: _failed(st), 6.0, rf, st), _msgs(st)[-6:]
        assert await rf.wait_power_off(5.0)
        _drain(rf, st)
        await _stop(rf)
        return rf, st
    rf, st = asyncio.run(go())
    assert max(st["req"]) == pytest.approx(117.0)                     # cap = max(90×1.3, 90+10) = 117, 600 까지 안 감
    assert "Blind ramp 도달 시간" in _failed(st)[0]
    assert not any("Blind ramp 완료" in m for m in _msgs(st))           # 70.2 한 번으로 통과하지 않음
    assert any("최근 FWD 최댓값" in m for m in _msgs(st))                # D: 117 이 아니라 실제 출력 근처부터 ramp-down
    first = [m for m in _msgs(st) if m.startswith("Ramp-Down step")][0]
    assert float(re.search(r"target=([\d.]+)W", first).group(1)) < 71.0, first
    assert any(k == "power_off_finished" for k, _ in st["events"])


def test_incident_all_flags_off_is_old_behavior():
    async def go():
        rf, st = _incident_rf(_rf_max_power=150.0, _rf_blind_ramp_settle_s=0.05, **OFF)
        await rf.start_process(90.0)
        ok = await _until(lambda: st["req"] and max(st["req"]) >= 150.0, 6.0, rf, st)
        await asyncio.sleep(0.2)
        _drain(rf, st)
        failed = list(_failed(st))
        await rf.cleanup()
        assert await rf.wait_power_off(5.0)
        _drain(rf, st)
        await _stop(rf)
        return ok, failed, st
    ok, failed, st = asyncio.run(go())
    assert ok and max(st["req"]) == pytest.approx(150.0)              # 요구값이 RF_MAX_POWER 까지 오른다(예전 그대로)
    assert failed == []                                               # REF 49 < 50 — 한계 50 만 적용
    assert any(m.startswith("Blind ramp 완료: FWD=7") for m in _msgs(st))   # 1회 샘플로 Blind 통과
    steps = [m for m in _msgs(st) if m.startswith("Ramp-Down step")]
    assert steps[0].startswith("Ramp-Down step: target=145.0W")       # _last_sent_w(150) 에서 시작
    assert not any("실제 출력" in m or "REF.p 감시 ON (한계" in m or "Blind 임계 도달" in m for m in _msgs(st))


# ═══════════════════════ A. 요구값 상한 + 추종 실패 ═══════════════════════
def test_a_cap_formula():
    rf, _ = _mk()
    for t, cap in ((90.0, 117.0), (20.0, 30.0), (500.0, 600.0), (10.0, 20.0)):
        rf.target_power = t
        assert rf._follow_cap_w() == pytest.approx(cap)
    rf._rf_follow_guard_enabled = False
    assert rf._follow_cap_w() is None


def test_a_ramping_stops_at_cap_and_fails_after_wait():
    async def go():
        rf, st = _mk(_rf_blind_ramp_fwd_threshold_w=0.0, _rf_ramp_step=50.0, _rf_follow_wait_s=0.25)
        await rf.start_process(90.0)
        await _feed(rf, 1.0)
        await _feed(rf, 51.0)
        await _feed(rf, 80.0, n=12)                                   # 90 → 93 … 117 에서 멈춤
        _drain(rf, st)
        assert max(st["req"]) == pytest.approx(117.0) and not _failed(st)
        await _feed(rf, 80.0, ref=60.0, n=10, dt=0.04)                # REF 대기 중(0.4초)은 시간을 진행하지 않음
        _drain(rf, st)
        assert rf.state == "REF_P_WAITING" and not _failed(st)
        await _feed(rf, 80.0, n=3, dt=0.04)                           # 재개(대기 전 누적 ~0 + 0.08초)
        _drain(rf, st)
        assert not _failed(st)
        await _feed(rf, 89.5)                                         # 조건 풀림(목표−허용오차 이상) → 리셋
        await _feed(rf, 80.0, n=5, dt=0.04)
        _drain(rf, st)
        assert not _failed(st)
        await _feed(rf, 80.0, n=10, dt=0.04)
        _drain(rf, st)
        failed = list(_failed(st))
        await _stop(rf)
        return failed, st
    failed, st = asyncio.run(go())
    assert failed, _msgs(st)[-5:]
    m = failed[0]
    assert "요구값=117.0W" in m and "상한 117.0W" in m and "목표의 130%" in m and "목표=90.0W" in m and "FWD=80.0W" in m
    assert max(st["req"]) == pytest.approx(117.0)


def test_a_maintaining_never_exceeds_cap():
    async def go():
        rf, st = _mk(_rf_blind_ramp_fwd_threshold_w=0.0, _rf_ramp_step=50.0, _rf_follow_wait_s=0.3,
                     _rf_maintain_step=5.0, _maintain_need_consecutive=1)
        await _reach(rf, st)
        await _feed(rf, 80.0, n=40, dt=0.02)
        _drain(rf, st)
        await _stop(rf)
        return st
    st = asyncio.run(go())
    assert max(st["req"]) == pytest.approx(117.0)
    assert _failed(st) and "요구값=117.0W" in _failed(st)[0]


def test_a_off_maintaining_goes_to_max():
    async def go():
        rf, st = _mk(_rf_blind_ramp_fwd_threshold_w=0.0, _rf_ramp_step=50.0, _rf_maintain_step=50.0,
                     _maintain_need_consecutive=1, **OFF)
        await _reach(rf, st)
        await _feed(rf, 80.0, n=15)
        _drain(rf, st)
        await _stop(rf)
        return st
    st = asyncio.run(go())
    assert max(st["req"]) == pytest.approx(600.0) and not _failed(st)


# ═══════════════════════ B. 목표 도달 뒤 반사파 비율 ═══════════════════════
@pytest.mark.parametrize("target, lim", [(90.0, 27.0), (300.0, 50.0), (10.0, 5.0)])
def test_b_limit_after_reach(target, lim):
    rf, _ = _mk()
    rf.target_power = target
    rf.state = "RAMPING_UP"
    assert rf._ref_limit() == (50.0, "")                              # 도달 전 = TH 그대로
    rf.state = "MAINTAINING"
    got, why = rf._ref_limit()
    assert got == pytest.approx(lim) and f"{lim:.1f}W" in why
    rf.state, rf.previous_state = "REF_P_WAITING", "MAINTAINING"
    assert rf._ref_limit()[0] == pytest.approx(lim)
    rf.previous_state = "RAMPING_UP"
    assert rf._ref_limit()[0] == 50.0
    rf.state = "MAINTAINING"
    rf._rf_ref_ratio_guard_enabled = False
    assert rf._ref_limit() == (50.0, "")


@pytest.mark.parametrize("ref, fail", [(30.0, True), (20.0, False)])
def test_b_target_90_ref_after_reach(ref, fail):
    async def go():
        rf, st = _mk(_rf_blind_ramp_fwd_threshold_w=0.0, _rf_ramp_step=50.0, _ref_wait_to_s=0.2)
        await _reach(rf, st)
        await _feed(rf, 90.0, ref=ref, n=12, dt=0.04)
        _drain(rf, st)
        await _stop(rf)
        return st
    st = asyncio.run(go())
    if fail:
        m = _failed(st)
        assert m and "TH=27.0W" in m[0] and "30%" in m[0], st["events"][-4:]
        assert any("안정화 대기 시작" in s and "27.0W" in s for s in _msgs(st))
    else:
        assert not _failed(st) and not any("안정화 대기" in s for s in _msgs(st))


def test_b_ramp_up_ref_49_not_stopped():
    async def go():
        rf, st = _mk(_rf_blind_ramp_fwd_threshold_w=0.0, _rf_ramp_step=50.0, _ref_wait_to_s=0.1)
        await rf.start_process(90.0)
        for f in (1.0, 51.0):
            await _feed(rf, f, ref=49.0, dt=0.06)
        await _feed(rf, 90.0, ref=49.0)
        _drain(rf, st)
        await _stop(rf)
        return st
    st = asyncio.run(go())
    assert any(k == "target_reached" for k, _ in st["events"]) and not _failed(st)
    assert not any("안정화 대기" in s for s in _msgs(st))


def test_b_arms_ref_watch_when_reached_unarmed():
    """목표 71 (Blind 임계 70 바로 위) — 연속 3회 전에 목표 허용오차 안에 들면 REF 감시가 안 켜진 채 MAINTAINING → B 가 켠다."""
    async def go(flags):
        rf, st = _mk(**flags)
        await rf.start_process(71.0)
        rf.current_power_step = rf._last_sent_w = 70.0
        await _feed(rf, 70.5)
        _drain(rf, st)
        await _stop(rf)
        return rf, st
    rf, st = asyncio.run(go({}))
    assert rf.state == "MAINTAINING" and rf._ref_check_armed
    assert any("REF.p 감시 ON (한계 21.3W)" in m for m in _msgs(st))
    rf, st = asyncio.run(go(dict(_rf_ref_ratio_guard_enabled=False)))
    assert rf.state == "MAINTAINING" and not rf._ref_check_armed     # B 를 끄면 예전처럼 그대로


# ═══════════════════════ C. Blind 도달 연속 N회 ═══════════════════════
def test_c_needs_three_consecutive():
    async def go(flags, seq):
        rf, st = _mk(**flags)
        await rf.start_process(100.0)
        rf.current_power_step = rf._last_sent_w = 70.0
        states = []
        for f in seq:
            await _feed(rf, f)
            states.append(rf.state)
        _drain(rf, st)
        await _stop(rf)
        return states, st
    states, st = asyncio.run(go({}, [70.5, 71.0, 65.0, 70.5, 71.0, 0.0, 71.0, 71.5]))
    assert "BLIND_SETTLE" not in states                               # 1~2회 뒤 미만(0.0 포함)이면 넘어가지 않음
    assert sum("Blind 임계 도달" in m for m in _msgs(st)) >= 4
    states, _ = asyncio.run(go({}, [70.5, 71.0, 72.0]))
    assert states == ["RAMPING_UP", "RAMPING_UP", "BLIND_SETTLE"]
    states, _ = asyncio.run(go(dict(_rf_blind_reach_consec_enabled=False), [70.5]))
    assert states == ["BLIND_SETTLE"]                                 # 꺼짐 = 1회(예전)
    states, _ = asyncio.run(go(dict(_rf_blind_reach_consec_n=1), [70.5]))
    assert states == ["BLIND_SETTLE"]                                 # N ≤ 1 = 1회


@pytest.mark.parametrize("target", [71.0, 72.0, 73.0])
def test_c_normal_runs_near_threshold_reach_target(target):
    """FWD = 요구값(정상 발생기), 1 W 씩 — 목표 71~73 W 가 실패하지 않고 도달."""
    box = {"rf": None}

    async def read():
        return {"forward": round(box["rf"]._last_sent_w or 0.0, 1), "reflected": 0.0}

    async def go():
        rf, st = _mk(read)
        box["rf"] = rf
        await rf.start_process(target)
        ok = await _until(lambda: rf.state == "MAINTAINING", 8.0, rf, st)
        await asyncio.sleep(0.2)
        _drain(rf, st)
        await _stop(rf)
        return ok, rf, st
    ok, rf, st = asyncio.run(go())
    assert ok and not _failed(st), _msgs(st)[-5:]
    assert any(k == "target_reached" for k, _ in st["events"]) and rf._ref_check_armed


# ═══════════════════════ D. ramp-down 시작값 = 실제 출력 ═══════════════════════
def _steps(st):
    return [m for m in _msgs(st) if m.startswith("Ramp-Down step")]


def test_d_no_output_fail_turns_off_without_ramp():
    async def go():
        rf, st = _mk(_rf_blind_ramp_reach_timeout_s=0.1)
        await rf.start_process(90.0)
        await _feed(rf, 0.0, n=30, dt=0.01)
        ok = await rf.wait_power_off(3.0)
        _drain(rf, st)
        await _stop(rf)
        return ok, rf, st
    ok, rf, st = asyncio.run(go())
    assert ok and _failed(st) and "출력 없음" in _failed(st)[0]
    assert any("ramp-down 없이 0 W 확인 후 SET OFF" in m for m in _msgs(st))
    assert _steps(st) == [] and st["unv"] and set(st["unv"]) == {0.0}
    assert "RF 파워 ramp-down 완료" in _msgs(st) and any(k == "power_off_finished" for k, _ in st["events"])
    assert st["enabled"] is False and rf._power_off_evt.is_set()


def test_d_last_two_low_readings_turn_off_without_ramp():
    async def go():
        rf, st = _mk(_rf_blind_ramp_fwd_threshold_w=0.0, _rf_ramp_step=50.0)
        await _reach(rf, st)
        await _feed(rf, 0.5, n=2)                                     # 저출력 2회(실패 전) 뒤 정지
        await rf.cleanup()
        ok = await rf.wait_power_off(3.0)
        _drain(rf, st)
        await _stop(rf)
        return ok, st
    ok, st = asyncio.run(go())
    assert ok and _steps(st) == [] and any("최근 2회 FWD ≤ 1.0W" in m for m in _msgs(st))


@pytest.mark.parametrize("flags, fresh, first", [
    ({}, True, "Ramp-Down step: target=64.0W"),                       # 요구값 117·FWD 65 → 65 부터
    ({}, False, "Ramp-Down step: target=116.0W"),                     # 측정이 오래됨 → 예전(요구값부터)
    (OFF, True, "Ramp-Down step: target=116.0W"),                     # 꺼짐 → 예전
])
def test_d_rampdown_start(flags, fresh, first):
    async def go():
        rf, st = _mk(**flags)
        await rf.start_process(90.0)
        await _feed(rf, 65.0, n=2)
        rf.current_power_step = rf._last_sent_w = 117.0
        if not fresh:
            await asyncio.sleep(0.15)                                 # 폴링 주기(20ms) × 3 넘김
        await rf.cleanup()
        ok = await rf.wait_power_off(5.0)
        _drain(rf, st)
        await _stop(rf)
        return ok, st
    ok, st = asyncio.run(go())
    assert ok and _steps(st)[0].startswith(first), _steps(st)[:2]
    assert ("최근 FWD 최댓값 65.0W" in " ".join(_msgs(st))) is (first.endswith("64.0W"))
    assert "RF 파워 ramp-down 완료" in _msgs(st)


def test_d_normal_stop_same_as_before():
    async def go():
        rf, st = _mk(_rf_blind_ramp_fwd_threshold_w=0.0, _rf_ramp_step=50.0)
        await _reach(rf, st)
        await rf.cleanup()
        assert await rf.wait_power_off(5.0)
        _drain(rf, st)
        await _stop(rf)
        return st
    st = asyncio.run(go())
    assert _steps(st)[0].startswith("Ramp-Down step: target=40.0W")  # 90 − 50(ramp 단계) — 요구값 90 에서 시작
    assert not any("실제 출력" in m or "ramp-down 없이" in m for m in _msgs(st))


# ═══════════════════════ direct_mode(플라즈마 클리닝) — 해당 없음 ═══════════════════════
def test_direct_mode_guards_inactive():
    rf, _ = _mk()
    rf._direct_mode = True
    rf.target_power = 90.0
    rf.state = "MAINTAINING"
    assert rf._follow_cap_w() is None and rf._blind_consec_need() == 1 and rf._ref_limit() == (50.0, "")


# ═══════════════════════ 설정 전달(settings.json → config_common → 두 인스턴스) ═══════════════════════
@pytest.fixture
def restore_cfg():
    mods = (config_common, config_ch1, config_ch2)
    snap = [dict(vars(md)) for md in mods]
    yield
    for md, d in zip(mods, snap):
        for k in [k for k in vars(md) if k not in d]:
            delattr(md, k)
        for k, v in d.items():
            setattr(md, k, v)


_NEW = {"RF_FOLLOW_GUARD_ENABLED": ("_rf_follow_guard_enabled", True, "false", False),
        "RF_FOLLOW_MAX_RATIO": ("_rf_follow_max_ratio", 1.3, "1.5", 1.5),
        "RF_FOLLOW_MIN_MARGIN_W": ("_rf_follow_min_margin_w", 10.0, "12.0", 12.0),
        "RF_FOLLOW_WAIT_S": ("_rf_follow_wait_s", 60.0, "45.0", 45.0),
        "RF_REF_RATIO_GUARD_ENABLED": ("_rf_ref_ratio_guard_enabled", True, "false", False),
        "RF_REF_RATIO_AFTER_REACH": ("_rf_ref_ratio_after_reach", 0.30, "0.25", 0.25),
        "RF_REF_RATIO_MIN_W": ("_rf_ref_ratio_min_w", 5.0, "6.0", 6.0),
        "RF_BLIND_REACH_CONSEC_ENABLED": ("_rf_blind_reach_consec_enabled", True, "false", False),
        "RF_BLIND_REACH_CONSEC_N": ("_rf_blind_reach_consec_n", 3, "4", 4),
        "RF_RAMPDOWN_FROM_ACTUAL_ENABLED": ("_rf_rampdown_from_actual_enabled", True, "false", False)}


def _instances():
    async def nop(*a):
        return None
    ch2 = RFPowerAsync(send_rf_power=nop, send_rf_power_unverified=nop)                          # 챔버 CH2 RF 연속(cfg 없음)
    pc = RFPowerAsync(send_rf_power=nop, send_rf_power_unverified=nop, direct_mode=True, cfg=config_common)  # PC(CH1/CH2)
    return ch2, pc


def test_repo_settings_defaults_reach_instances(restore_cfg):
    res = L.check_and_load_settings(REPO_JSON)
    assert res.ok, res.problems
    for key, (attr, default, _txt, _val) in _NEW.items():
        assert getattr(config_common, key) == default, key
        for rf in _instances():
            assert getattr(rf, attr) == default, (key, attr)


def test_changed_settings_reach_instances(restore_cfg, tmp_path):
    txt = REPO_JSON.read_text(encoding="utf-8")
    for key, (_a, _d, new_txt, _v) in _NEW.items():
        txt, n = re.subn(rf'("{key}"\s*:\s*)[^,\n]+', lambda mt: mt.group(1) + new_txt, txt, count=1)
        assert n == 1, key
    p = tmp_path / "settings.json"
    p.write_text(txt, encoding="utf-8")
    res = L.check_and_load_settings(p)
    assert res.ok, res.problems
    for rf in _instances():
        for key, (attr, _d, _t, val) in _NEW.items():
            assert getattr(rf, attr) == val, (key, attr)
    with open(os.path.join(_ROOT, "apps", "rf_service", "rf_worker.py"), encoding="utf-8") as fp:
        assert "RF_FOLLOW" not in fp.read()                           # 워커는 이 키를 쓰지 않는다(모르는 키로 무시)


def test_workers_ignore_new_keys(restore_cfg):
    """옛 워커 exe(config_common 에 새 이름이 없음)가 새 settings.json 을 읽어도 멈추지 않고 무시한다."""
    for k in _NEW:
        delattr(config_common, k)
    for rel in (("apps", "log_sync_service", "log_sync.py"), ("apps", "rf_service", "rf_worker.py")):
        src = open(os.path.join(_ROOT, *rel), encoding="utf-8").read()
        keys = re.findall(r'"([A-Z0-9_]+)"', re.search(r"SETTINGS_KEYS = \(([^)]*)\)", src, re.S).group(1))
        res = L.check_and_load_settings(REPO_JSON, worker_keys=keys)
        assert res.ok, (rel, res.problems)
        assert all(f"common.{k}" in res.ignored for k in _NEW), (rel, res.ignored)


def test_old_exe_rejects_new_settings_json(restore_cfg):
    """옛 메인 exe(새 이름 없음) + 새 settings.json → 엄격 검사가 '프로그램에 없는 설정 이름' 으로 시작을 막는다(배포 주의)."""
    for k in _NEW:
        delattr(config_common, k)
    res = L.check_and_load_settings(REPO_JSON)
    assert not res.ok and sum("프로그램에 없는 설정 이름" in p for p in res.problems) == len(_NEW)

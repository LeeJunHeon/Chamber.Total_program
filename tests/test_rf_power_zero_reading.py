# -*- coding: utf-8 -*-
"""RF 연속(CH2 RF·PC) — PLC 가 FWD 를 정확히 0.0 W 로 읽어도 측정으로 받는다.

예전: _ingest_status_result 가 `res.get("forward") or …` 로 이어 0.0 을 '값 없음'으로 보고 측정을 버렸다.
PLC 변환은 음수를 0.0 으로 자르므로(CH2 raw 0~4, PC 는 4 W 제로 오프셋 뒤) 발생기가 출력을 안 낼 때 정확히 0.0 이다
→ 저출력 인터락(FWD ≤ 1 W 3회 연속)·blind ramp 도달 감시가 그 상황에서만 돌지 않아, 공정 중 RF 가 꺼지면 시간대로
'성공' 종료, 처음부터 안 켜지면 RAMPING 에서 오류 없이 무한 대기였다.
쓰기 역변환(CH2: 1.6546·W + 2.6323)으로 명령 W ≈ 읽은 FWD 라 정상 램프(1 W 부터 1 W 씩)는 그대로 목표에 닿는다.
장치·네트워크 접근 없음(가짜 발생기).
"""
import os
import sys
import asyncio
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from device.rf_power import RFPowerAsync   # noqa: E402

INV_A, INV_B = 1.6546, 2.6323               # settings.json CHAMBER_RF_CONT_WRITE_INV_A/B


def _rf(plant):
    async def send(dac):
        plant["dac"] = dac

    async def toggle(on):
        plant["enabled"] = bool(on)

    async def read():                        # AsyncPLC.rf_read_fwd_ref 와 같은 모양·반올림·음수 0 자르기
        f = 0.0 if plant["dead"] else max(0.0, (plant["dac"] - INV_B) / INV_A)
        return {"forward": round(f, 1), "reflected": 0.0}

    rf = RFPowerAsync(send_rf_power=send, send_rf_power_unverified=send, request_status_read=read,
                      toggle_enable=toggle, write_inv_a=INV_A, write_inv_b=INV_B)
    rf._poll_interval_ms = 20                # 시험 속도만(1초 → 20ms). 램프 단계·판정 기준은 그대로
    rf._rf_blind_ramp_fwd_threshold_w = 0.0  # blind ramp 안정화 대기(60초) 생략 — 아래 개별 시험에서 따로 확인
    return rf


def _kinds(rf):
    out = []
    while not rf._event_q.empty():
        e = rf._event_q.get_nowait()
        out.append((e.kind, getattr(e, "message", "") or ""))
    return out


async def _until(cond, timeout=10.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if cond():
            return True
        await asyncio.sleep(0.01)
    return False


async def _collect_until_failed(rf, timeout=5.0):
    """target_failed 가 나올 때까지(최대 timeout 초) 이벤트를 모은다."""
    got = []
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        got.extend(_kinds(rf))
        if any(k == "target_failed" for k, _ in got):
            break
        await asyncio.sleep(0.01)
    return got


async def _stop(rf):
    rf._is_running = False
    for t in (rf._poll_task, rf._adjust_task):
        if t and not t.done():
            t.cancel()
    await asyncio.sleep(0.05)


def test_zero_reading_is_a_measurement():
    async def go():
        plant = {"dac": 0.0, "dead": False, "enabled": False}
        rf = _rf(plant)
        rf._is_running = True
        rf.target_power = 100.0
        rf.forward_w = 100.0
        rf._ingest_status_result({"forward": 0.0, "reflected": 0.0})
        assert rf.forward_w == 0.0                                 # 예전: 100.0 그대로(측정 버림)
        assert ("display", "") in _kinds(rf)
        rf._ingest_status_result({"P": 12.5})                      # 다른 키 이름 대체는 그대로
        assert rf.forward_w == 12.5
        rf._ingest_status_result({"forward": None, "power": 7.0})  # None 은 '값 없음' → 다음 키
        assert rf.forward_w == 7.0
        rf._ingest_status_result((3.0, 220.0, 0.5))                # 튜플 경로 그대로
        assert rf.forward_w == 3.0
        await _stop(rf)
    asyncio.run(go())


def test_normal_ramp_from_1w_reaches_target_without_low_power_fail():
    """(그대로) 1 W 부터 1 W 씩 — 첫 단계 1.0 W 는 저출력 1회로 세지만 다음 단계에서 리셋, 목표 도달."""
    async def go():
        plant = {"dac": 0.0, "dead": False, "enabled": False}
        rf = _rf(plant)
        await rf.start_process(30.0)
        ok = await _until(lambda: rf.state == "MAINTAINING", 15.0)
        ev = _kinds(rf)
        await _stop(rf)
        return ok, ev
    ok, ev = asyncio.run(go())
    assert ok, ev[-5:]
    assert any(k == "target_reached" for k, _ in ev)
    assert not any(k == "target_failed" for k, _ in ev), ev


def test_trip_while_maintaining_fails_after_3_zero_readings():
    async def go():
        plant = {"dac": 0.0, "dead": False, "enabled": False}
        rf = _rf(plant)
        await rf.start_process(30.0)
        assert await _until(lambda: rf.state == "MAINTAINING", 15.0)
        _kinds(rf)
        plant["dead"] = True                                       # 발생기 정지 → PLC 0.0 W
        got = await _collect_until_failed(rf)
        await _stop(rf)
        return got
    got = asyncio.run(go())
    failed = [m for k, m in got if k == "target_failed"]
    assert failed and "3회 연속" in failed[0], got[-6:]    # 예전: 이벤트 0건, 화면은 마지막 값, 계속 진행


def test_rf_never_lights_fails_instead_of_waiting_forever():
    async def go():
        plant = {"dac": 0.0, "dead": True, "enabled": False}      # 처음부터 출력 없음
        rf = _rf(plant)
        await rf.start_process(30.0)
        got = await _collect_until_failed(rf)
        await _stop(rf)
        return got
    got = asyncio.run(go())
    assert any(k == "target_failed" for k, _ in got), got[-6:]    # 예전: RAMPING_UP 에서 이벤트 없이 대기


def test_blind_ramp_reach_timeout_now_fires_on_zero():
    """blind ramp 도달 감시(설정 150초)도 0.0 측정을 받아야 돈다 — 시간을 줄여 확인."""
    async def go():
        plant = {"dac": 0.0, "dead": True, "enabled": False}
        rf = _rf(plant)
        rf._rf_blind_ramp_fwd_threshold_w = 70.0
        rf._rf_blind_ramp_reach_timeout_s = 0.2
        rf._rf_low_power_count_max_n = 10_000                      # 저출력 판정과 분리해서 본다
        await rf.start_process(100.0)
        got = await _collect_until_failed(rf)
        await _stop(rf)
        return got
    got = asyncio.run(go())
    failed = [m for k, m in got if k == "target_failed"]
    assert failed and "Blind ramp" in failed[0], got[-6:]

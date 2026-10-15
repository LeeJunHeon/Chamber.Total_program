# -*- coding: utf-8 -*-
"""레시피 TEST 행(# = test) — 러너가 붙인 test_mode 가 정규화·검증을 지나 공정 컨트롤러까지 가서
장비 제어 없이 DELAY 한 단계만 만드는지(2026-03-04 러너 재구성 뒤 정규화에서 빠지던 경로)."""
import os
import sys
import asyncio
import contextlib

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402
from runtime.chamber_runtime import ChamberRuntime               # noqa: E402
from controller.process_controller import ActionType             # noqa: E402
from controller.runtime_state import runtime_state               # noqa: E402


def _rt(ch=2):
    """런타임 최소 인스턴스 + 실제 공정 컨트롤러(_bind_process_controller). 실행 중인 루프 안에서 부른다."""
    rt = ChamberRuntime.__new__(ChamberRuntime)
    rt.ch = ch
    rt.logs = []
    rt.append_log = lambda s, m: rt.logs.append((s, m))
    rt._get_text = lambda name: ""
    rt.supports_rf_pulse = True
    rt.supports_dc_pulse = True
    rt.supports_dc_cont = True
    rt.supports_rf_cont = True
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
    return rt, rt.process_controller


def test_normalize_keeps_test_mode_and_skips_validation():
    async def go():
        rt, _pc = _rt()
        n = rt._normalize_params_for_process({"#": "test", "time": "90s", "Process_name": "T1",
                                              "test_mode": True, "test_duration_sec": 90.0})
        assert n["test_mode"] is True and n["test_duration_sec"] == 90.0 and n["time"] == "90s"
        assert rt._validate_norm_params(n) == []                     # 가스·건·파워 없음도 통과(장비를 안 씀)
        normal = rt._normalize_params_for_process({"Ar": "T", "Ar_flow": "10", "gun1": "T"})
        assert "test_mode" not in normal and "test_duration_sec" not in normal
        assert any("가스" in e for e in rt._validate_norm_params(
            rt._normalize_params_for_process({"Process_name": "x"})))   # 일반 행 검증은 그대로
    asyncio.run(go())


@pytest.mark.parametrize("params,want_ms", [
    ({"test_mode": True, "test_duration_sec": 10.0, "time": "10s"}, 10000),
    ({"test_mode": True, "test_duration_sec": 0.0, "time": "1m"}, 60000),
    ({"test_mode": True, "test_duration_sec": 0.0, "time": "", "process_time": 0.5}, 30000),
])
def test_controller_makes_single_delay(params, want_ms):
    async def go():
        _rt_, pc = _rt()
        steps = pc._create_process_sequence(dict(params))
        assert len(steps) == 1 and steps[0].action == ActionType.DELAY and steps[0].duration == want_ms
        assert steps[0].message.startswith("TEST MODE")
    asyncio.run(go())


@pytest.mark.parametrize("stop", [False, True])
def test_finished_detail_marks_test_mode(stop):
    """TEST 행 공정의 finished 상세에 test_mode 가 실린다 — 러너가 TEST 정지 때 장비 정리를 건너뛰는 근거."""
    async def go():
        _r, pc = _rt()
        pc.start_process({"test_mode": True, "test_duration_sec": 1.0, "time": "1s", "process_note": "T"})
        if stop:
            await asyncio.sleep(0.2)
            pc.request_stop()
        fin = None
        while fin is None:
            ev = await asyncio.wait_for(pc.event_q.get(), timeout=5.0)
            if ev.kind == "finished":
                fin = ev.payload
        assert fin["detail"]["test_mode"] is True
        assert fin["detail"]["stopped"] is stop and fin["ok"] is (not stop)
    asyncio.run(go())


@pytest.mark.parametrize("test_mode,stopped,cleanup", [
    (True, True, False),        # TEST 행 정지 → 장비 정리 건너뜀(장비를 켠 적 없음)
    (True, False, True),        # TEST 행 정상 끝 → 평소처럼 정리
    (False, True, True),        # 실제 공정 정지 → 정리
])
def test_after_finish_cleanup_on_test_stop(test_mode, stopped, cleanup):
    async def go():
        rt, _pc = _rt()
        calls = []

        async def _stop_wd(light=False):
            calls.append(light)
        rt._stop_device_watchdogs = _stop_wd
        rt._clear_queue_and_reset_ui = lambda: None
        rt._runner_queue_mode = False
        await rt._runner_stage_after_finish(not stopped, {"stopped": stopped, "test_mode": test_mode})
        assert bool(calls) is cleanup
        assert bool([m for s, m in rt.logs if "[TEST] STOP finished" in m]) is (not cleanup)
    asyncio.run(go())


@pytest.mark.parametrize("row", [
    {"#": "test", "time": "10s", "Process_name": "TEST1"},                                  # 값 없는 TEST 행
    {"#": "test", "time": "10s", "Process_name": "copy", "Ar": "T", "Ar_flow": "10", "gun1": "T",
     "G1 Target": "Ti", "use_dc_pulse": "T", "dc_pulse_power": "50"},                       # 공정 값이 채워진 TEST 행
])
def test_runner_test_row_reaches_controller(monkeypatch, row):
    """큐 러너(_runner_stage_advance_queue)에서 TEST 행 — 검증 오류 없이 시작 단계로 가고, 넘긴 값으로 DELAY 한 단계.
    공정 값이 채워진 행도 실제 공정(IG·MFC…) 시퀀스가 아니라 DELAY 하나여야 한다."""
    async def go():
        rt, pc = _rt()
        started = []

        async def _fake_start(norm, gen):
            started.append(norm)

        async def _no_targets():
            return None
        rt._update_ui_from_params = lambda p: None
        rt._load_gun_targets = _no_targets
        rt._open_run_log = lambda p: None
        rt._host_report_start = lambda ok, msg: rt.logs.append(("HOST", f"{ok}:{msg}"))
        rt._host_run_set_explicit = lambda *a, **k: None
        rt._clear_queue_and_reset_ui = lambda: None
        rt._set_state_text = lambda s: None
        rt._start_after_preflight = _fake_start
        rt.process_queue = [dict(row)]
        rt.current_process_index = -1
        monkeypatch.setattr(runtime_state, "mark_started", lambda *a, **k: None)
        await rt._runner_stage_advance_queue(True)
        assert not [m for s, m in rt.logs if s == "Validate"]
        assert len(started) == 1
        norm = started[0]
        assert norm["test_mode"] is True and norm["test_duration_sec"] == 10.0 and norm["time"] == "10s"
        steps = pc._create_process_sequence(norm)
        assert len(steps) == 1 and steps[0].action == ActionType.DELAY and steps[0].duration == 10000
    asyncio.run(go())


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

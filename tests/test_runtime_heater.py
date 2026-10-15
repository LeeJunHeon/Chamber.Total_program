# -*- coding: utf-8 -*-
"""ChamberRuntime ↔ CH2 히터(2단계): 레시피 정규화·검증, 공정 콜백(설정·승온 대기·유지 측정·취소),
공정 중 이상 감시, 다행 레시피 유지/끄기, 큐 종료 안전망. 실제 HeaterService + 래더 에뮬레이터를 쓴다."""
import os
import sys
import asyncio
import contextlib

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402
from runtime.chamber_runtime import ChamberRuntime               # noqa: E402
import heater_ladder_sim as sim                                  # noqa: E402


class _PCRec:
    """공정 컨트롤러 대신 on_heater_* 호출만 기록."""

    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        if name.startswith("on_heater"):
            return lambda *a, **k: self.calls.append((name, a, k))
        raise AttributeError(name)

    def names(self):
        return [c[0] for c in self.calls]

    def codes(self):
        return [c[2].get("code") for c in self.calls if c[0] == "on_heater_failed"]


def _rt(ch=2):
    """런타임 최소 인스턴스 + 실제 _bind_process_controller 의 콜백. (rt, 콜백을 가진 진짜 PC, 기록기)"""
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
    pc = rt.process_controller
    rec = _PCRec()
    rt.process_controller = rec
    return rt, pc, rec


def _hlogs(rt, word):
    return [m for s, m in rt.logs if word in m]


async def _tick(n=5):
    for _ in range(n):
        await asyncio.sleep(0)


async def _until(svc, plc, clk, cond, limit_s=20000, step=5):
    """에뮬레이터는 실제로 쉬지 않으므로(가짜 PLC 가 바로 돌려줌) 매 묶음마다 루프에 양보해 띄운 태스크가 돌게 한다."""
    t0 = clk.t
    while not cond():
        await sim.run(svc, plc, clk, step)
        await _tick()
        assert clk.t - t0 < limit_s, "시간 초과"


# ───────────────────────── 정규화·검증 ─────────────────────────
def test_normalize_and_validate():
    async def go():
        rt, _pc, _rec = _rt()
        svc, plc, clk, logs, chat = sim.make()
        n = rt._normalize_params_for_process({"Ar": "T", "Ar_flow": "10", "gun1": "T",
                                              "use_heater": "T", "heater_temp": "250", "heater_ramp": ""})
        assert n["use_heater"] is True and n["heater_temp"] == 250.0 and n["heater_ramp"] is None
        n = rt._normalize_params_for_process({"Ar": "T", "Ar_flow": "10", "gun1": "T", "heater_temp": "250"})
        assert n["use_heater"] is False and n["heater_temp"] is None and _hlogs(rt, "use_heater=F")
        n = rt._normalize_params_for_process({"Ar": "T", "Ar_flow": "10", "gun1": "T"})
        assert n["use_heater"] is False and n["heater_temp"] is None and n["heater_ramp"] is None

        base = dict(use_ar=True, ar_flow=10, use_g1=True, use_dc_pulse=True, dc_pulse_power=50)
        assert any("히터 기능이 꺼져" in e for e in rt._validate_norm_params(dict(base, use_heater=True, heater_temp=200.0)))
        rt._heater = svc
        assert rt._validate_norm_params(dict(base, use_heater=True, heater_temp=200.0, heater_ramp=None)) == []
        assert rt._validate_norm_params(dict(base, use_heater=True, heater_temp=200.0, heater_ramp=6.0)) == []
        assert any("heater_temp" in e for e in rt._validate_norm_params(dict(base, use_heater=True, heater_temp=None)))
        assert any("heater_temp" in e for e in rt._validate_norm_params(dict(base, use_heater=True, heater_temp=501.0)))
        assert any("heater_ramp" in e for e in rt._validate_norm_params(dict(base, use_heater=True, heater_temp=200.0, heater_ramp=0)))
        assert any("heater_ramp" in e for e in rt._validate_norm_params(dict(base, use_heater=True, heater_temp=200.0, heater_ramp=61)))
        assert rt._validate_norm_params(dict(base)) == []                 # 히터 없는 레시피는 그대로
        rt1, _p, _r = _rt(ch=1)
        rt1._heater = svc
        errs = rt1._validate_norm_params(dict(use_ar=True, ar_flow=10, use_dc_pulse=True, dc_pulse_power=50,
                                              use_heater=True, heater_temp=100.0))
        assert any("CH1" in e for e in errs)
        raw = rt._validate_raw_rows([{"use_heater": "Y?", "heater_temp": "abc", "heater_ramp": "1x"}])
        assert len(raw) == 3 and all(("숫자" in e) or ("T/F" in e) for e in raw)
        assert rt._validate_raw_rows([{"use_heater": "T", "heater_temp": "200", "heater_ramp": ""}]) == []
    asyncio.run(go())


# ───────────────────────── 다행 레시피 ─────────────────────────
def test_next_row_lookahead():
    async def go():
        rt, _pc, _rec = _rt()
        rt.current_process_index = 0
        cases = [
            ([{"use_heater": "T"}, {"use_heater": "T"}], True),
            ([{"use_heater": "T"}, {"Process_name": "x"}], False),
            ([{"use_heater": "T"}, {"Process_name": "w", "delay": "20m"}, {"use_heater": "t"}], True),
            ([{"use_heater": "T"}, {"Process_name": "delay 10m"}, {"use_heater": "F"}], False),
            ([{"use_heater": "T"}, {"#": "test", "use_heater": "T"}], False),
            ([{"use_heater": "T"}], False),
        ]
        for q, want in cases:
            rt.process_queue = q
            assert rt._next_queue_row_uses_heater() is want, q
    asyncio.run(go())


async def _process_owned_running(rt, pc):
    svc, plc, clk, logs, chat = sim.make(mv_limit=700)
    rt.attach_heater(svc)
    await sim.run(svc, plc, clk, 5)
    pc._heater_set(200.0, None)
    await _tick()
    await sim.run(svc, plc, clk, 30)
    assert svc.owner == "process" and svc.status()["run"]
    return svc, plc, clk


@pytest.mark.parametrize("ok,stopped,queue,next_rows,keep", [
    (True, False, True, [{"use_heater": "T"}], True),
    (True, False, True, [{"Process_name": "w", "delay": "5m"}, {"use_heater": "T"}], True),
    (True, False, True, [{"Process_name": "x"}], False),
    (True, False, True, [], False),
    (False, False, True, [{"use_heater": "T"}], False),          # 실패
    (False, True, True, [{"use_heater": "T"}], False),           # 정지
    (True, False, False, [{"use_heater": "T"}], False),          # 큐 모드 아님
])
def test_after_row_keep_or_off(ok, stopped, queue, next_rows, keep):
    async def go():
        rt, pc, rec = _rt()
        svc, plc, clk = await _process_owned_running(rt, pc)
        rt._runner_queue_mode = queue
        rt.process_queue = [{"use_heater": "T"}] + next_rows
        rt.current_process_index = 0
        await rt._heater_after_row(ok=ok, stopped=stopped)
        await sim.run(svc, plc, clk, 3)
        if keep:
            assert svc.status()["run"] and svc.owner == "process" and _hlogs(rt, "히터 유지")
        else:
            assert not svc.status()["run"] and svc.owner is None
    asyncio.run(go())


def test_after_row_ignores_manual_heater():
    async def go():
        rt, pc, rec = _rt()
        svc, plc, clk, logs, chat = sim.make()
        rt.attach_heater(svc)
        await sim.run(svc, plc, clk, 5)
        ok, why = await svc.turn_on(150.0, owner="manual")
        assert ok, why
        await sim.run(svc, plc, clk, 5)
        await rt._heater_after_row(ok=False, stopped=False)
        await sim.run(svc, plc, clk, 3)
        assert svc.status()["run"] and svc.owner == "manual"
    asyncio.run(go())


def test_queue_end_safety_net_turns_off():
    async def go():
        rt, pc, rec = _rt()
        svc, plc, clk = await _process_owned_running(rt, pc)
        rt._host_run_finalize = lambda: None
        rt._reset_ui_after_process = lambda: None
        rt._close_run_log = lambda: None
        rt._log_file_path = None

        async def _noop(**_k):
            return None
        rt._shutdown_log_writer = _noop
        rt._prestart_buf = []
        rt._cleanup_timed_out = False
        spawned = []
        rt._spawn_detached = lambda coro, **k: spawned.append(asyncio.ensure_future(coro))
        rt._clear_queue_and_reset_ui()
        await asyncio.gather(*spawned)
        await sim.run(svc, plc, clk, 3)
        assert not svc.status()["run"] and svc.owner is None
    asyncio.run(go())


# ───────────────────────── 공정 콜백 + 리스너 ─────────────────────────
def test_set_wait_hold_then_fault_in_process():
    async def go():
        rt, pc, rec = _rt()
        svc, plc, clk, logs, chat = sim.make(mv_limit=700)
        rt.attach_heater(svc)
        await sim.run(svc, plc, clk, 5)
        pc._heater_set(200.0, None)
        await _tick()
        await sim.run(svc, plc, clk, 2)
        assert rec.names() == ["on_heater_set_ok"]
        assert svc.owner == "process" and not svc.hold.armable and not svc.sat.clamp_enabled
        pc._heater_wait(200.0)
        await _until(svc, plc, clk, lambda: "on_heater_ready" in rec.names())
        assert not svc.hold.is_holding()                      # 진공에서는 잡지 않는다
        await sim.run(svc, plc, clk, 300)
        plc.gas_loss, plc.plasma_heat = 0.25, 20.0
        await sim.run(svc, plc, clk, 30)
        pc._heater_hold()
        await _tick()
        await _until(svc, plc, clk, lambda: "on_heater_hold_ok" in rec.names(), limit_s=600, step=1)
        assert svc.hold.is_holding() and 500 <= svc.hold.value <= 512 and plc.D[118] == svc.hold.value
        assert rt._heater_watch
        plc.M[644] = True                                     # 과온 트립
        await sim.run(svc, plc, clk, 3)
        assert rec.codes() == ["E832"] and not rt._heater_watch
    asyncio.run(go())


def test_take_over_manual_run_uses_apply_target():
    async def go():
        rt, pc, rec = _rt()
        svc, plc, clk, logs, chat = sim.make(mv_limit=700)
        rt.attach_heater(svc)
        await sim.run(svc, plc, clk, 5)
        ok, why = await svc.turn_on(150.0, owner="manual")
        assert ok, why
        await sim.run(svc, plc, clk, 1200)
        assert svc.hold.is_holding()                          # 수동 운전은 지금처럼 진공에서 잡힘
        pc._heater_set(180.0, 6.0)
        await _tick()
        await sim.run(svc, plc, clk, 2)
        assert rec.names() == ["on_heater_set_ok"] and svc.owner == "process"
        assert any("넘겨받음" in m for t, m in logs)
        assert not svc.hold.is_holding() and plc.D[118] == 700
        assert svc.profiler.is_active() and abs(svc.profiler.rate() - 6.0) < 1e-9
    asyncio.run(go())


def test_set_failures_report_e830_and_release_owner():
    async def go():
        rt, pc, rec = _rt()
        pc._heater_set(200.0, None)                           # 서비스 없음
        await _tick()
        assert rec.codes() == ["E830"]
        rt2, pc2, rec2 = _rt()
        svc, plc, clk, logs, chat = sim.make()
        rt2.attach_heater(svc)
        await sim.run(svc, plc, clk, 5)
        plc.M[644] = True                                     # 이상 상태 → is_ready False
        await sim.run(svc, plc, clk, 2)
        pc2._heater_set(200.0, None)
        await _tick()
        assert rec2.codes() == ["E830"] and svc.owner is None and not svc.status()["run"]
    asyncio.run(go())


def test_cancel_during_set_does_not_touch_process():
    async def go():
        rt, pc, rec = _rt()
        svc, plc, clk, logs, chat = sim.make()
        rt.attach_heater(svc)
        await sim.run(svc, plc, clk, 5)
        pc._heater_set(200.0, None)
        pc._cancel_heater_cb()                                # 정지가 먼저 옴(세대 +1)
        await _tick(10)
        await sim.run(svc, plc, clk, 2)
        assert rec.names() == [] and not rt._heater_watch     # 공정은 건드리지 않음
        assert svc.owner == "process"                         # 정리는 공정 종료 후 러너(after_row·안전망)가 한다
        await rt._heater_after_row(ok=False, stopped=True)
        await sim.run(svc, plc, clk, 3)
        assert not svc.status()["run"] and svc.owner is None
    asyncio.run(go())


@pytest.mark.parametrize("action,mode,want", [
    ("abort", "timeout", "E833"),
    ("force", "timeout", "hold_ok_forced"),
    ("force", "give_up", "hold_ok_no_force"),
    ("abort", "give_up", "E833"),
])
def test_hold_failure_handling(action, mode, want):
    async def go():
        rt, pc, rec = _rt()
        over = {"mv_limit": 700 if mode == "timeout" else 520, "hold_fail_action": action,
                "hold_wait_s": 120.0}
        if mode == "timeout":
            over["hold_enter_sec"] = 600.0                    # 대기(120초) 안에는 못 잡는다
        svc, plc, clk, logs, chat = sim.make(**over)
        rt.attach_heater(svc)
        await sim.run(svc, plc, clk, 5)
        pc._heater_set(200.0, None)
        await _tick()
        pc._heater_wait(200.0)
        await _until(svc, plc, clk, lambda: "on_heater_ready" in rec.names())
        await sim.run(svc, plc, clk, 300)
        plc.gas_loss = 0.25
        plc.plasma_heat = 20.0 if mode == "timeout" else 0.0
        await sim.run(svc, plc, clk, 120)
        pc._heater_hold()
        await _tick()
        await _until(svc, plc, clk, lambda: ("on_heater_hold_ok" in rec.names()) or rec.codes(),
                     limit_s=400, step=1)
        await _tick(10)
        if want == "E833":
            assert rec.codes() == ["E833"]
        elif want == "hold_ok_forced":
            assert "on_heater_hold_ok" in rec.names() and svc.hold.is_holding()
            assert 500 <= svc.hold.value <= 512 and [c for c in chat.cards if "강제 진입" in c[1]]
        else:
            assert "on_heater_hold_ok" in rec.names() and not svc.hold.is_holding() and plc.D[118] == 520
            assert _hlogs(rt, "상한이 곧 출력 제한")
    asyncio.run(go())


def test_hold_mode_off_passes_and_rearms_guard():
    async def go():
        rt, pc, rec = _rt()
        svc, plc, clk, logs, chat = sim.make(hold_mode="off")
        rt.attach_heater(svc)
        await sim.run(svc, plc, clk, 5)
        pc._heater_set(150.0, None)
        await _tick()
        assert not svc.sat.clamp_enabled
        pc._heater_hold()
        await _tick()
        assert "on_heater_hold_ok" in rec.names() and svc.sat.clamp_enabled
    asyncio.run(go())

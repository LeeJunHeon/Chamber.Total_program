# -*- coding: utf-8 -*-
"""종료 절차의 전원 OFF 실패는 '그 전원의 단계'만 끝낸다 — 다른 단계 대기는 유지.

재현했던 문제: DC1 은 공정 중 target_failed 로 자체 cleanup(루프 A)을, 종료 절차의 DC_POWER_STOP 이
다시 cleanup(루프 B)을 부른다. PLC 쓰기가 10초 넘게 안 되면 A·B 의 power_off_failed 가 따로 도착하고,
늦은 쪽이 RF_POWER_STOP(670초) 대기 중에 오면 RF 대기가 끊겨 램프다운 전에 가스 밸브를 닫았다.
"""
import os
import sys
import time
import asyncio
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                        # noqa: E402
from controller.process_controller import (                          # noqa: E402
    ProcessController, ProcessStep, ActionType, ExpectToken,
)


# ───────────────────────── 하네스 ─────────────────────────
class _Calls:
    def __init__(self):
        self.plc: list[tuple] = []
        self.stops: list[str] = []


def _mk(seq: list[ProcessStep]):
    c = _Calls()

    def _noop(*a, **k):
        return None

    pc = ProcessController(
        send_plc=lambda n, v, ch: c.plc.append((n, v, ch)),
        send_mfc=_noop,
        send_dc_power=_noop, stop_dc_power=lambda: c.stops.append("dc"),
        send_rf_power=_noop, stop_rf_power=lambda: c.stops.append("rf"),
        send_dc_power2=_noop, stop_dc_power2=lambda: c.stops.append("dc2"),
        start_dc_pulse=_noop, stop_dc_pulse=lambda: c.stops.append("dcp"),
        start_rf_pulse=_noop, stop_rf_pulse=lambda: c.stops.append("rfp"),
        ig_wait=_noop, cancel_ig=_noop, rga_scan=_noop, oes_run=_noop,
        ch=1, supports_dc_cont=True, supports_rf_cont=True,
        supports_dc_pulse=True, supports_rf_pulse=True, supports_dc_cont2=True,
    )
    pc.process_sequence = list(seq)
    pc.current_params = {"process_name": "T"}
    pc.is_running = True
    pc._shutdown_in_progress = True          # 종료 절차 중으로 고정
    pc._current_step_idx = -1
    pc.calls = c
    pc.logs = []
    pc._emit_log = lambda src, msg: pc.logs.append(msg)
    return pc, c


def _seq_dc_rf_gas():
    return [
        ProcessStep(action=ActionType.DC_POWER_STOP, message="DC off"),
        ProcessStep(action=ActionType.RF_POWER_STOP, message="RF off"),
        ProcessStep(action=ActionType.PLC_CMD, params=("MV", False, 1), message="가스 밸브"),
    ]


async def _wait_step(pc, action, timeout=3.0):
    """해당 action 단계에서 대기 상태(expect group 존재)가 되기를 기다린다."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        cur = pc.current_step
        if cur is not None and cur.action == action and pc._expect_group is not None:
            return True
        await asyncio.sleep(0.01)
    return False


# ───────────────────────── (a) 남의 단계 → 대기 유지 ─────────────────────────
def test_a_other_power_failure_keeps_current_wait():
    async def _main():
        pc, c = _mk(_seq_dc_rf_gas())
        runner = asyncio.create_task(pc._runner())
        try:
            assert await _wait_step(pc, ActionType.DC_POWER_STOP)
            pc.on_dc_off_finished()                       # DC 단계 정상 통과
            assert await _wait_step(pc, ActionType.RF_POWER_STOP)
            assert c.plc == [], c.plc

            t0 = time.perf_counter()
            pc.on_power_off_failed("DC Power", "DC_OFF", "DC 0W 쓰기 실패 — 출력 상태 미확인")
            await asyncio.sleep(0.5)
            dt = time.perf_counter() - t0
            # 가스 밸브로 넘어가지 않았다
            assert c.plc == [], f"대기가 끊겼다: {c.plc}"
            assert pc.current_step.action == ActionType.RF_POWER_STOP
            assert pc._expect_group is not None
            assert pc._shutdown_error is True
            assert any("DC Power" in s for s in pc._shutdown_failures), pc._shutdown_failures
            assert any("대기는 유지" in m for m in pc.logs), pc.logs[-3:]

            # RF 가 제 신호로 끝나면 가스 밸브 진행
            pc.on_rf_off_finished()
            assert await _wait_step(pc, ActionType.PLC_CMD)
            assert c.plc and c.plc[0][0].upper().startswith("MV"), c.plc
            pc._match_token(ExpectToken("PLC", c.plc[0][0]))
            await asyncio.wait_for(runner, timeout=3.0)
            assert pc.is_running is False
            print(f"  (a) {dt:.2f}s 대기 유지 → RF 완료 후 가스 밸브, 결과 실패 기록 OK")
        finally:
            runner.cancel()
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await runner
    asyncio.run(_main())


# ───────────────────────── (b)(c) 자기 단계 → 즉시 종료 ─────────────────────────
def test_b_own_step_dc_ends_immediately():
    async def _main():
        pc, c = _mk(_seq_dc_rf_gas())
        runner = asyncio.create_task(pc._runner())
        try:
            assert await _wait_step(pc, ActionType.DC_POWER_STOP)
            pc.on_power_off_failed("DC Power", "DC_OFF", "DC 0W 쓰기 실패")
            assert await _wait_step(pc, ActionType.RF_POWER_STOP), "DC 단계가 즉시 끝나야 한다"
            assert pc._shutdown_error is True
            print("  (b) 자기 단계 → 즉시 다음 단계")
        finally:
            runner.cancel()
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await runner
    asyncio.run(_main())


def test_c_own_step_rf_ends_immediately():
    async def _main():
        pc, c = _mk(_seq_dc_rf_gas())
        runner = asyncio.create_task(pc._runner())
        try:
            assert await _wait_step(pc, ActionType.DC_POWER_STOP)
            pc.on_dc_off_finished()
            assert await _wait_step(pc, ActionType.RF_POWER_STOP)
            pc.on_power_off_failed("RF Power", "RF_OFF", "RF 0W 쓰기 실패")
            assert await _wait_step(pc, ActionType.PLC_CMD), "RF 단계가 즉시 끝나야 한다"
            print("  (c) 자기 단계 → 즉시 다음 단계")
        finally:
            runner.cancel()
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await runner
    asyncio.run(_main())


# ───────────────────────── (d) DC2 / DCPulse / RFPulse ─────────────────────────
@pytest.mark.parametrize("action,source,kind,other_kind", [
    (ActionType.DC2_POWER_STOP, "DC2 Power", "DC2_OFF", "DC_OFF"),
    (ActionType.DC_PULSE_STOP, "DCPulse", "DCPULSE_OFF", "RF_OFF"),
    (ActionType.RF_PULSE_STOP, "RFPulse", "RFPULSE_OFF", "DC_OFF"),
])
def test_d_each_power_owns_its_step(action, source, kind, other_kind):
    async def _main():
        seq = [ProcessStep(action=action, message="off"),
               ProcessStep(action=ActionType.PLC_CMD, params=("MV", False, 1), message="가스")]
        pc, c = _mk(seq)
        runner = asyncio.create_task(pc._runner())
        try:
            assert await _wait_step(pc, action)
            # 남의 토큰 → 대기 유지
            pc.on_power_off_failed("X Power", other_kind, "다른 전원 실패")
            await asyncio.sleep(0.3)
            assert c.plc == [], f"{kind}: 남의 실패로 대기가 끊겼다"
            # 자기 토큰 → 즉시 종료
            pc.on_power_off_failed(source, kind, "내 전원 실패")
            assert await _wait_step(pc, ActionType.PLC_CMD), f"{kind}: 자기 실패인데 단계가 안 끝났다"
            print(f"  (d) {kind}: 남의 실패 유지 / 자기 실패 종료 OK")
        finally:
            runner.cancel()
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await runner
    asyncio.run(_main())


# ───────────────────────── (e) 정상 모드 ─────────────────────────
def test_e_normal_mode_starts_shutdown():
    async def _main():
        pc, c = _mk(_seq_dc_rf_gas())
        pc._shutdown_in_progress = False
        called = []
        pc._start_normal_shutdown = lambda: called.append(1)
        pc.on_power_off_failed("DC Power", "DC_OFF", "DC 0W 쓰기 실패")
        assert called == [1], "정상 모드는 종료 절차를 시작해야 한다"
        assert pc._process_failed is True
        assert any("종료 절차를 시작" in m for m in pc.logs)
    asyncio.run(_main())


# ───────────────────────── (f) is_running False ─────────────────────────
def test_f_not_running_is_noop():
    async def _main():
        pc, c = _mk(_seq_dc_rf_gas())
        pc.is_running = False
        pc._shutdown_failures.clear()
        pc.on_power_off_failed("DC Power", "DC_OFF", "무시돼야 한다")
        assert pc._shutdown_failures == [] and pc._shutdown_error is False
        assert pc.logs == []
    asyncio.run(_main())


# ───────────────────────── (g) 회귀: 기존 on_*_failed ─────────────────────────
def test_g_legacy_callback_still_cancels_current_step():
    async def _main():
        pc, c = _mk(_seq_dc_rf_gas())
        runner = asyncio.create_task(pc._runner())
        try:
            assert await _wait_step(pc, ActionType.DC_POWER_STOP)
            pc.on_dc_off_finished()
            assert await _wait_step(pc, ActionType.RF_POWER_STOP)
            pc.on_dc_target_failed("과전류")               # 기존 콜백 = 지금처럼 현재 단계 종료
            assert await _wait_step(pc, ActionType.PLC_CMD), "기존 동작(현재 단계 종료)이 유지돼야 한다"
            print("  (g) 기존 on_dc_target_failed 는 현재 단계를 끝낸다(회귀 OK)")
        finally:
            runner.cancel()
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await runner
    asyncio.run(_main())


# ───────────────────────── (h) chamber_runtime 펌프 인자 ─────────────────────────
def test_h_chamber_pumps_pass_source_and_kind():
    """5개 펌프가 on_power_off_failed 를 올바른 source/off_kind 로 부르고 _dc_failed_flag 를 리셋하는지."""
    from runtime.chamber_runtime import ChamberRuntime

    class _PC:
        def __init__(self):
            self.calls = []

        def on_power_off_failed(self, source, off_kind, why, *, code=None, meta=None):
            self.calls.append((source, off_kind, why, (meta or {}).get("safety")))

        def __getattr__(self, name):
            def _f(*a, **k):
                self.calls.append((name,) + a)
            return _f

    class _Ev:
        def __init__(self, **kw):
            self.kind = kw.pop("kind")
            self.message = kw.pop("message", None)
            self.reason = kw.pop("reason", None)
            self.cmd = kw.pop("cmd", None)
            for k, v in kw.items():
                setattr(self, k, v)

    class _Dev:
        def __init__(self, evs):
            self._evs = evs

        async def events(self):
            for e in self._evs:
                yield e

    def _rt():
        c = ChamberRuntime.__new__(ChamberRuntime)
        c.ch = 1
        c.chat = None
        c.process_controller = _PC()
        c.logs = []
        c.append_log = lambda s, m: c.logs.append((s, m))
        c._run_warnings = []
        c._dc_failed_flag = True
        c._dc2_failed_flag = True
        c._display_dc = lambda *a, **k: None
        c._display_rf = lambda *a, **k: None
        c.data_logger = None
        c._dl_fire_and_forget = lambda *a, **k: None
        return c

    async def _main():
        # DC1
        c = _rt()
        c.dc_power = _Dev([_Ev(kind="power_off_failed", message="DC1 0W 실패")])
        await c._pump_dc_events()
        assert ("DC Power", "DC_OFF", "DC1 0W 실패", "output_state_unconfirmed") in c.process_controller.calls
        assert c._dc_failed_flag is False
        assert any("DC1" in s for s, _ in c.logs)

        # DC2
        c = _rt()
        c.dc_power2 = _Dev([_Ev(kind="power_off_failed", message="DC2 0W 실패")])
        await c._pump_dc2_events()
        assert ("DC2 Power", "DC2_OFF", "DC2 0W 실패", "output_state_unconfirmed") in c.process_controller.calls
        assert c._dc2_failed_flag is False

        # RF 연속
        c = _rt()
        c.rf_power = _Dev([_Ev(kind="power_off_failed", message="RF 0W 실패")])
        await c._pump_rf_events()
        assert ("RF Power", "RF_OFF", "RF 0W 실패", "output_state_unconfirmed") in c.process_controller.calls

        # RF Pulse: RF_OFF 만 on_power_off_failed, 다른 cmd 는 기존 콜백
        c = _rt()
        c.rf_pulse = _Dev([_Ev(kind="command_failed", cmd="RF_OFF", reason="RF Pulse OFF 미확인"),
                           _Ev(kind="command_failed", cmd="START_SEQUENCE", reason="시작 실패")])
        await c._pump_rfpulse_events()
        assert ("RFPulse", "RFPULSE_OFF", "RF Pulse OFF 미확인", "output_state_unconfirmed") \
            in c.process_controller.calls
        assert any(x[0] == "on_rf_pulse_failed" for x in c.process_controller.calls if isinstance(x[0], str))
        print("  (h) DC1/DC2/RF/RFPulse 펌프 인자 OK")
    asyncio.run(_main())

    # DC Pulse OUTPUT_OFF 분기는 펌프 구조가 복잡해 소스로 확인
    src = open(os.path.join(_ROOT, "runtime", "chamber_runtime.py"), encoding="utf-8").read()
    i = src.index("OUTPUT_OFF 미확인(출력 상태 미확인)")
    head = src[i - 200:i]
    assert 'on_power_off_failed(' in head and '"DCPulse", "DCPULSE_OFF"' in head, head[-200:]
    print("  (h) DCPulse 분기 소스 확인 OK")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-s"]))

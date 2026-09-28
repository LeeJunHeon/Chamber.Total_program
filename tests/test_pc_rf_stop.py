# -*- coding: utf-8 -*-
"""플라즈마 클리닝 강제 RF OFF: 램프다운 먼저 정지 → 0W 확인 → 그다음 SET OFF.

가짜 PLC 는 실제와 같게 모델링한다.
  · asyncio.Lock(FIFO) + 쓰기 지연 → 진행 중 쓰기가 끝난 뒤에 다음 쓰기가 들어간다
  · SET 이 켜져 있을 때만 WRITE 가 D/A 값에 반영되고, SET OFF 면 '이전값' 이 유지된다(XBF-DV04A 설정)
수정 전에는 강제 0W 와 SET OFF 사이에 램프다운 쓰기가 끼어들어 최종 D/A 에 0 이 아닌 값이 남았다.
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
from device.rf_power import RFPowerAsync                             # noqa: E402
from runtime.plasma_cleaning_runtime import (                        # noqa: E402
    PlasmaCleaningRuntime, apply_rf_off_unconfirmed,
)


# ───────────────────────── 가짜 PLC (FIFO 락 + '이전값' 유지) ─────────────────────────
class FakePLC:
    def __init__(self, *, write_delay=0.0, write_fail_always=False, set_off_raises=False):
        self._lock = asyncio.Lock()
        self.write_delay = float(write_delay)
        self.write_fail_always = bool(write_fail_always)
        self.set_off_raises = bool(set_off_raises)
        self.set_on = True             # DCV_SET_1
        self.da_value = 0.0            # D/A 채널 1 (SET ON 일 때만 갱신, OFF 면 이전값 유지)
        self.calls: list[tuple] = []

    async def power_write(self, w, *, family="DCV", write_idx=1):
        async with self._lock:
            if self.write_delay:
                await asyncio.sleep(self.write_delay)
            if self.write_fail_always:
                self.calls.append(("write_FAIL", float(w)))
                raise RuntimeError("PLCError E402: no response")
            self.calls.append(("write", float(w)))
            if self.set_on:
                self.da_value = float(w)          # SET OFF 면 D/A 는 이전값 유지

    async def power_enable(self, on, *, family="DCV", set_idx=1):
        async with self._lock:
            if (not on) and self.set_off_raises:
                self.calls.append(("set_RAISE", bool(on)))
                raise RuntimeError("PLCError: coil write failed")
            self.calls.append(("set", bool(on)))
            self.set_on = bool(on)

    def names(self):
        return [n for n, *_ in self.calls]

    def nonzero_write_between_zero_and_setoff(self) -> bool:
        """강제 0W 이후 SET OFF 전에 0 이 아닌 쓰기가 있었는가(= 수정 전 증상)."""
        i0 = next((i for i, c in enumerate(self.calls)
                   if c[0] == "write" and c[1] == 0.0), None)
        if i0 is None:
            return False
        for c in self.calls[i0 + 1:]:
            if c[0] == "set" and c[1] is False:
                return False
            if c[0] == "write" and c[1] != 0.0:
                return True
        return False


class _CfgPC:
    PC_RF_CLEANUP_TIMEOUT_S = 0.5
    PC_RF_WAIT_POWER_OFF_TIMEOUT_S = 2.0
    POWER_OFF_ZERO_DEADLINE_S = 1.0
    PC_RF_DIRECT_MODE = True
    RF_MAX_POWER = 600
    PC_RF_RAMP_STEP = 50.0
    PC_RF_RAMP_KICK_THRESHOLD_W = 100.0
    PC_RF_RAMP_SHUTDOWN_CUT_W = 50.0
    PC_RF_RAMPDOWN_INTERVAL_MS = 10
    DEBUG_PRINT = False


class _Chat:
    def __init__(self):
        self.cards = []

    def notify_error_with_src(self, src, msg):
        self.cards.append((src, msg))

    def flush(self):
        pass


def _mk_pc(plc, rf=None, *, cfg=None):
    p = PlasmaCleaningRuntime.__new__(PlasmaCleaningRuntime)
    p._cfg_mod = cfg or _CfgPC()
    p.plc = plc
    p.rf = rf
    p.chat = _Chat()
    p.logs = []
    p.append_log = lambda src, msg: p.logs.append(f"[{src}] {msg}")
    p._process_timer_active = True
    p._rf_off_unconfirmed_reason = None
    p._selected_ch = 1
    p._shutdown_called = 0

    async def _rest():
        p._shutdown_called += 1
    p._shutdown_rest_devices = _rest
    return p


def _mk_rf(plc, cfg):
    return RFPowerAsync(send_rf_power=plc.power_write,
                        send_rf_power_unverified=plc.power_write,
                        toggle_enable=lambda on: plc.power_enable(on, family="DCV", set_idx=1),
                        cfg=cfg, direct_mode=True)


# ───────────────────────── (a) 정상: 강제 경로 없음 ─────────────────────────
def test_a_normal_wait_ok_no_force_path():
    async def _main():
        plc = FakePLC()
        cfg = _CfgPC()
        rf = _mk_rf(plc, cfg)
        p = _mk_pc(plc, rf, cfg=cfg)
        rf._power_off_evt.set()                          # wait_power_off 즉시 성공
        forced = []
        p._force_rf_zero_then_set_off = lambda: forced.append(1)
        t0 = time.perf_counter()
        await p._safe_rf_stop()
        dt = time.perf_counter() - t0
        assert forced == [], "완료 신호가 왔으면 강제 경로로 가지 않는다"
        assert p._rf_off_unconfirmed_reason is None
        assert p.chat.cards == []
        assert p._shutdown_called == 1
        print(f"  (a) {dt:.2f}s 강제 경로 없음, 알림 없음")
    asyncio.run(_main())


# ───────────────────────── (b) 순서: 램프다운 먼저 정지 ─────────────────────────
def test_b_force_path_order_rampdown_stopped_first():
    async def _main():
        plc = FakePLC(write_delay=1.1)                   # 쓰기 1.1초 (FIFO 락)
        cfg = _CfgPC()
        rf = _mk_rf(plc, cfg)
        p = _mk_pc(plc, rf, cfg=cfg)
        # kick 300W 램프다운을 실제로 돌린다(스텝 50W, 인터벌 10ms)
        rf._is_running = True
        rf._enabled = True
        rf.target_power = 300.0
        rf._last_sent_w = 300.0
        rf.current_power_step = 300.0
        rf._power_off_evt.clear()
        rf._is_ramping_down = True
        rf._rampdown_w = 300.0
        rf._rampdown_task = asyncio.create_task(rf._rampdown_loop_kick())
        await asyncio.sleep(0.05)                        # 램프다운 쓰기 진행 중

        t0 = time.perf_counter()
        await p._safe_rf_stop()                          # wait 2초 후 강제 경로
        dt = time.perf_counter() - t0

        assert plc.nonzero_write_between_zero_and_setoff() is False, plc.calls
        assert ("set", False) in plc.calls, plc.calls
        assert plc.set_on is False
        assert plc.da_value == 0.0, f"최종 D/A={plc.da_value} (0 이 아닌 값이 유지됐다)"
        assert rf._rampdown_task is None
        print(f"  (b) {dt:.2f}s 최종 SET OFF + D/A={plc.da_value} calls={plc.names()}")
    asyncio.run(_main())


# ───────────────────────── (c) 강제 0W 항상 실패 ─────────────────────────
def test_c_force_zero_always_fails():
    async def _main():
        plc = FakePLC(write_fail_always=True)
        cfg = _CfgPC()
        p = _mk_pc(plc, None, cfg=cfg)                   # rf 없음 → wait 없이 강제 경로만 보려면 rf 필요

        # rf 는 wait_power_off 가 False 인 가짜로 둔다(강제 경로 즉시 진입)
        class _RF:
            _rampdown_task = None
            _adjust_task = None
            _poll_task = None
            _is_ramping_down = False
            _is_running = False
            _enabled = False
            state = "IDLE"
            target_power = 0.0
            current_power_step = 0.0
            _last_sent_w = 0.0
            _power_off_evt = asyncio.Event()

            async def cleanup(self):
                return None

            async def wait_power_off(self, timeout_s=8.0):
                return False
        p.rf = _RF()

        t0 = time.perf_counter()
        await p._safe_rf_stop()
        dt = time.perf_counter() - t0
        assert ("set", False) not in plc.calls, "0W 미확인인데 SET OFF 했다"
        assert plc.set_on is True
        assert p._rf_off_unconfirmed_reason is not None
        assert len(p.chat.cards) == 1 and "출력 상태 미확인" in p.chat.cards[0][1]
        assert p.chat.cards[0][0] == "PC"
        # 같은 런에서 한 번 더 불려도 알림은 1회
        await p._safe_rf_stop()
        assert len(p.chat.cards) == 1
        print(f"  (c) {dt:.2f}s SET OFF 없음, 알림 1회 (재호출에도 추가 없음)")
    asyncio.run(_main())


# ───────────────────────── (d) 0W 성공 + SET OFF 예외 ─────────────────────────
def test_d_force_zero_ok_but_set_off_raises():
    async def _main():
        plc = FakePLC(set_off_raises=True)
        cfg = _CfgPC()
        p = _mk_pc(plc, None, cfg=cfg)

        class _RF:
            _rampdown_task = None
            _adjust_task = None
            _poll_task = None
            _is_ramping_down = False
            _is_running = False
            _enabled = False
            state = "IDLE"
            target_power = 0.0
            current_power_step = 0.0
            _last_sent_w = 0.0
            _power_off_evt = asyncio.Event()

            async def cleanup(self):
                return None

            async def wait_power_off(self, timeout_s=8.0):
                return False
        p.rf = _RF()

        await p._safe_rf_stop()
        assert ("write", 0.0) in plc.calls
        assert p._rf_off_unconfirmed_reason is None, "0W 는 확인됐으므로 미확인이 아니다"
        assert p.chat.cards == []
        assert any("SET OFF 실패 — 0W 는 확인됨" in m for m in p.logs), p.logs[-3:]
        print(f"  (d) 0W 확인 → SET OFF 실패는 경고만, 알림 없음")
    asyncio.run(_main())


# ───────────────────────── (e) 결과 반영 순수 함수 ─────────────────────────
def test_e_apply_rf_off_unconfirmed():
    assert apply_rf_off_unconfirmed(True, False, None, "사유") == (False, False, "사유")
    assert apply_rf_off_unconfirmed(False, False, "원래", "사유") == (False, False, "원래")
    assert apply_rf_off_unconfirmed(False, True, "STOP", "사유") == (False, True, "STOP")
    assert apply_rf_off_unconfirmed(True, False, None, None) == (True, False, None)
    assert apply_rf_off_unconfirmed(True, True, None, "사유") == (False, False, "사유")
    print("  (e) 결과 반영 규칙 OK")
    # finally 에서 _final_cleanup 직후·_reset_ui_state 전에 적용되는지(소스)
    src = open(os.path.join(_ROOT, "runtime", "plasma_cleaning_runtime.py"), encoding="utf-8").read()
    i_cl = src.index("await self._final_cleanup()\n\n            # [A2]")
    i_ap = src.index("apply_rf_off_unconfirmed(", i_cl)
    i_ui = src.index("self._reset_ui_state(restore_time_min=self._last_process_time_min)", i_cl)
    assert i_cl < i_ap < i_ui


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-s"]))

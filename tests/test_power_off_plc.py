# -*- coding: utf-8 -*-
"""PLC D/A 전원(DC1/DC2/CH2 RF 연속/플라즈마 클리닝 RF) OFF 순서 검증.

래더에서 DCV_SET_n 하나가 D/A 출력허용과 WRITE MOV 를 같이 켜고, XBF-DV04A 채널출력상태가 '이전값'이라
0W 가 들어가지 않은 채 SET 을 끄면 마지막 출력값이 유지될 수 있다 → "0W 확인 → 그다음 SET OFF" 가 핵심.
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

import pytest                                              # noqa: E402
from device.dc_power import DCPowerAsync                    # noqa: E402
from device.rf_power import RFPowerAsync                    # noqa: E402


# ───────────────────────── 가짜 PLC 콜백 ─────────────────────────
class Rec:
    """호출 순서와 값을 기록하고 실패/예외를 주입한다."""

    def __init__(self, *, unverified_fail_n=0, unverified_always_fail=False,
                 toggle_raises=False):
        self.calls: list[tuple[str, object]] = []
        self._uf_left = int(unverified_fail_n)
        self._uf_always = bool(unverified_always_fail)
        self._toggle_raises = bool(toggle_raises)

    # 검증 전송(램프업 등)
    async def send(self, w):
        self.calls.append(("send", float(w)))

    # no-reply 전송(램프다운/0W)
    async def send_unverified(self, w):
        if self._uf_always or self._uf_left > 0:
            if not self._uf_always:
                self._uf_left -= 1
            self.calls.append(("send_unverified_FAIL", float(w)))
            raise RuntimeError("Modbus write timeout")
        self.calls.append(("send_unverified", float(w)))

    async def toggle(self, on):
        if self._toggle_raises and not on:
            self.calls.append(("toggle_RAISE", bool(on)))
            raise RuntimeError("PLC coil write failed")
        self.calls.append(("toggle", bool(on)))

    def names(self):
        return [n for n, _ in self.calls]

    def zero_writes(self):
        return [v for n, v in self.calls if n == "send_unverified" and v == 0.0]

    def nonzero_after_zero(self):
        """0W 쓰기 성공 뒤에 0 이 아닌 쓰기가 있었는가."""
        seen0 = False
        for n, v in self.calls:
            if n == "send_unverified" and v == 0.0:
                seen0 = True
            elif seen0 and n in ("send", "send_unverified") and v != 0.0:
                return True
        return False

    def order_zero_before_toggle_off(self) -> bool:
        i0 = next((i for i, (n, v) in enumerate(self.calls)
                   if n == "send_unverified" and v == 0.0), None)
        it = next((i for i, (n, v) in enumerate(self.calls)
                   if n == "toggle" and v is False), None)
        return i0 is not None and it is not None and i0 < it


class _CfgDC:
    POWER_OFF_ZERO_DEADLINE_S = 1.0
    DC_MAX_POWER = 600.0
    DC_INTERVAL_MS = 50
    DEBUG_PRINT = False


class _CfgRF:
    """CH2 RF 연속(비 direct)."""
    POWER_OFF_ZERO_DEADLINE_S = 1.0
    RF_MAX_POWER = 600
    RF_RAMP_STEP = 1000.0                          # 한 스텝에 0 까지 내려가게
    CHAMBER_RF_CONT_RAMPDOWN_INTERVAL_MS = 10
    CHAMBER_RF_CONT_DIRECT_MODE = False
    DEBUG_PRINT = False


class _CfgRFDirect:
    """플라즈마 클리닝 RF(direct_mode)."""
    POWER_OFF_ZERO_DEADLINE_S = 1.0
    RF_MAX_POWER = 600
    PC_RF_DIRECT_MODE = True
    PC_RF_RAMP_STEP = 1000.0
    PC_RF_RAMP_KICK_THRESHOLD_W = 100.0
    PC_RF_RAMP_SHUTDOWN_CUT_W = 50.0
    PC_RF_RAMPDOWN_INTERVAL_MS = 10
    DEBUG_PRINT = False


def _mk_dc(rec: Rec) -> DCPowerAsync:
    return DCPowerAsync(send_dc_power=rec.send, send_dc_power_unverified=rec.send_unverified,
                        toggle_enable=rec.toggle, cfg=_CfgDC(), name="DCTEST")


def _mk_rf(rec: Rec, cfg, *, direct=False) -> RFPowerAsync:
    return RFPowerAsync(send_rf_power=rec.send, send_rf_power_unverified=rec.send_unverified,
                        toggle_enable=rec.toggle, cfg=cfg, direct_mode=direct)


def _drain(dev):
    evs = []
    while not dev._event_q.empty():
        evs.append(dev._event_q.get_nowait())
    return evs


def _kinds(evs, kind):
    return [e for e in evs if e.kind == kind]


async def _wait_task(dev, attr="_rampdown_task", timeout=6.0):
    t = getattr(dev, attr, None)
    if t is None:
        return
    with contextlib.suppress(Exception):
        await asyncio.wait_for(asyncio.shield(t), timeout=timeout)


# ═════════════ DCPowerAsync ═════════════
def test_a_dc_normal_zero_then_set_off():
    async def _main():
        rec = Rec(); d = _mk_dc(rec)
        t0 = time.perf_counter()
        await d.start_process(100.0)
        rec.calls.clear()
        await d.cleanup()
        await _wait_task(d)
        dt = time.perf_counter() - t0
        evs = _drain(d)
        assert rec.zero_writes() == [0.0], rec.calls
        assert ("toggle", False) in rec.calls
        assert rec.order_zero_before_toggle_off(), rec.calls
        assert len(_kinds(evs, "power_off_finished")) == 1
        assert _kinds(evs, "power_off_failed") == []
        assert d.output_off_unconfirmed is False and d._enabled is False
        print(f"  (a) {dt:.2f}s calls={rec.names()}")
    asyncio.run(_main())


def test_b_dc_zero_retry_then_success():
    async def _main():
        rec = Rec(unverified_fail_n=2); d = _mk_dc(rec)
        await d.start_process(100.0)
        rec.calls.clear()
        rec._uf_left = 2
        t0 = time.perf_counter()
        await d.cleanup(); await _wait_task(d)
        dt = time.perf_counter() - t0
        evs = _drain(d)
        assert rec.names().count("send_unverified_FAIL") == 2
        assert rec.zero_writes() == [0.0]
        assert rec.order_zero_before_toggle_off(), rec.calls
        assert len(_kinds(evs, "power_off_finished")) == 1
        print(f"  (b) {dt:.2f}s calls={rec.names()}")
    asyncio.run(_main())


def test_c_dc_zero_always_fails():
    async def _main():
        rec = Rec(); d = _mk_dc(rec)
        await d.start_process(100.0)
        rec.calls.clear(); rec._uf_always = True
        t0 = time.perf_counter()
        await d.cleanup(); await _wait_task(d)
        dt = time.perf_counter() - t0
        evs = _drain(d)
        assert len(_kinds(evs, "power_off_failed")) == 1
        assert _kinds(evs, "power_off_finished") == []
        assert ("toggle", False) not in rec.calls, "0W 미확인인데 SET OFF 하면 안 된다"
        assert d._last_sent_power not in (None, 0.0), d._last_sent_power
        assert d.output_off_unconfirmed is True
        # 다시 cleanup 하면 0W 를 다시 시도한다
        rec.calls.clear(); rec._uf_always = False
        await d.cleanup(); await _wait_task(d)
        evs2 = _drain(d)
        assert rec.zero_writes() == [0.0], rec.calls
        assert len(_kinds(evs2, "power_off_finished")) == 1
        print(f"  (c) {dt:.2f}s (deadline {_CfgDC.POWER_OFF_ZERO_DEADLINE_S}s) 재시도 OK")
    asyncio.run(_main())


def test_d_dc_set_off_raises_still_finishes():
    async def _main():
        rec = Rec(toggle_raises=True); d = _mk_dc(rec)
        with contextlib.suppress(Exception):
            await d.start_process(100.0)
        rec.calls.clear()
        await d.cleanup(); await _wait_task(d)          # 예외가 밖으로 나오면 여기서 실패
        evs = _drain(d)
        assert rec.zero_writes() == [0.0]
        assert len(_kinds(evs, "power_off_finished")) == 1
        assert any("SET OFF 실패" in (e.message or "") for e in _kinds(evs, "status"))
        assert d._enabled is True, "SET OFF 실패면 래치는 유지"
        print(f"  (d) calls={rec.names()}")
    asyncio.run(_main())


def test_e_dc_never_started():
    async def _main():
        rec = Rec(); d = _mk_dc(rec)
        await d.cleanup(); await _wait_task(d)
        evs = _drain(d)
        assert rec.calls == [], rec.calls
        assert len(_kinds(evs, "power_off_finished")) == 1
        assert _kinds(evs, "power_off_failed") == []
        print("  (e) 쓰기·SET 호출 없음 → 완료")
    asyncio.run(_main())


# ═════════════ RFPowerAsync — 비 direct (CH2 RF 연속) ═════════════
async def _rf_start(d, rec, w=100.0):
    d._is_running = True
    d._enabled = True
    d.target_power = float(w)
    d._last_sent_w = float(w)
    d.current_power_step = float(w)
    rec.calls.clear()


def test_f_rf_cont_rampdown_then_off():
    async def _main():
        rec = Rec(); d = _mk_rf(rec, _CfgRF())
        await _rf_start(d, rec, 100.0)
        t0 = time.perf_counter()
        await d.cleanup(); await _wait_task(d)
        dt = time.perf_counter() - t0
        evs = _drain(d)
        # 램프 스텝이 정확히 0 에 착지하면 스텝 쓰기 1회 + 최종 확인 1회가 될 수 있다(기존 동작)
        assert len(rec.zero_writes()) >= 1, rec.calls
        assert rec.order_zero_before_toggle_off(), rec.calls
        assert len(_kinds(evs, "power_off_finished")) == 1
        assert d._power_off_evt.is_set() is True
        assert d.output_off_unconfirmed is False
        print(f"  (f) {dt:.2f}s calls={rec.names()}")
    asyncio.run(_main())


def test_g_rf_cont_zero_always_fails():
    async def _main():
        rec = Rec(); d = _mk_rf(rec, _CfgRF())
        await _rf_start(d, rec, 100.0)
        rec._uf_always = True
        t0 = time.perf_counter()
        await d.cleanup(); await _wait_task(d)
        dt = time.perf_counter() - t0
        evs = _drain(d)
        assert len(_kinds(evs, "power_off_failed")) == 1
        assert _kinds(evs, "power_off_finished") == []
        assert d._power_off_evt.is_set() is False, "완료 신호를 주면 안 된다"
        assert ("toggle", False) not in rec.calls
        assert d.output_off_unconfirmed is True
        # 다시 cleanup → 재시도
        rec._uf_always = False; rec.calls.clear()
        d._is_running = False
        await d.cleanup(); await _wait_task(d)
        evs2 = _drain(d)
        assert rec.zero_writes() == [0.0], rec.calls
        assert len(_kinds(evs2, "power_off_finished")) == 1
        print(f"  (g) {dt:.2f}s 재시도 OK")
    asyncio.run(_main())


def test_h_rf_cont_never_started_no_false_alarm():
    async def _main():
        rec = Rec(unverified_always_fail=True); d = _mk_rf(rec, _CfgRF())
        d._is_running = True                       # cleanup 진입 조건만
        await d.cleanup(); await _wait_task(d)
        evs = _drain(d)
        assert _kinds(evs, "power_off_failed") == [], "안 쓴 전원은 오경보 없음"
        assert len(_kinds(evs, "power_off_finished")) == 1
        print("  (h) 오경보 없음")
    asyncio.run(_main())


# ═════════════ RFPowerAsync — direct_mode (플라즈마 클리닝) ═════════════
def test_i_rf_direct_immediate_off():
    async def _main():
        rec = Rec(); d = _mk_rf(rec, _CfgRFDirect(), direct=True)
        await _rf_start(d, rec, 80.0)              # ≤ kick 임계(100W) → 즉시 OFF 경로
        t0 = time.perf_counter()
        await d.cleanup()
        dt = time.perf_counter() - t0
        evs = _drain(d)
        assert rec.zero_writes() == [0.0], rec.calls
        assert rec.order_zero_before_toggle_off(), rec.calls
        assert len(_kinds(evs, "power_off_finished")) == 1
        assert d._power_off_evt.is_set() is True
        print(f"  (i) {dt:.2f}s calls={rec.names()}")
    asyncio.run(_main())


def test_j_rf_direct_set_off_raises():
    async def _main():
        rec = Rec(toggle_raises=True); d = _mk_rf(rec, _CfgRFDirect(), direct=True)
        await _rf_start(d, rec, 80.0)
        await d.cleanup()                          # 예외가 나오면 실패
        evs = _drain(d)
        assert rec.zero_writes() == [0.0]
        assert len(_kinds(evs, "power_off_finished")) == 1
        assert any("SET OFF 실패" in (e.message or "") for e in _kinds(evs, "status"))
        print(f"  (j) calls={rec.names()}")
    asyncio.run(_main())


def test_k_rf_direct_zero_always_fails():
    async def _main():
        rec = Rec(unverified_always_fail=True); d = _mk_rf(rec, _CfgRFDirect(), direct=True)
        await _rf_start(d, rec, 80.0)
        t0 = time.perf_counter()
        await d.cleanup()
        dt = time.perf_counter() - t0
        evs = _drain(d)
        assert len(_kinds(evs, "power_off_failed")) == 1
        assert _kinds(evs, "power_off_finished") == []
        assert d._power_off_evt.is_set() is False
        assert ("toggle", False) not in rec.calls
        print(f"  (k) {dt:.2f}s evt={d._power_off_evt.is_set()}")
    asyncio.run(_main())


def test_l_rf_direct_no_nonzero_write_after_zero():
    async def _main():
        rec = Rec(); d = _mk_rf(rec, _CfgRFDirect(), direct=True)
        await _rf_start(d, rec, 80.0)
        d.state = "MAINTAINING"
        d.update_measurements(40.0, 0.0)           # 보정 태스크 기동
        await d.cleanup()
        await asyncio.sleep(0.2)
        assert len(rec.zero_writes()) >= 1, rec.calls
        assert rec.nonzero_after_zero() is False, rec.calls
        print(f"  (l) calls={rec.names()}")
    asyncio.run(_main())


def test_m_rf_direct_kick_path():
    async def _main():
        rec = Rec(); d = _mk_rf(rec, _CfgRFDirect(), direct=True)
        await _rf_start(d, rec, 300.0)             # > 임계 → kick 램프다운 경로
        t0 = time.perf_counter()
        await d.cleanup(); await _wait_task(d, timeout=10.0)
        dt = time.perf_counter() - t0
        evs = _drain(d)
        assert len(rec.zero_writes()) >= 1, rec.calls
        assert rec.order_zero_before_toggle_off(), rec.calls
        assert len(_kinds(evs, "power_off_finished")) == 1
        assert d._power_off_evt.is_set() is True
        print(f"  (m) {dt:.2f}s calls={rec.names()[-4:]}")
    asyncio.run(_main())


# ═════════════ process_controller: RF_POWER_STOP 한도 ═════════════
def test_n_rf_power_stop_timeout():
    from controller.process_controller import rf_power_stop_timeout_ms
    from lib import config_common as cfgc
    base = 240_000
    v = rf_power_stop_timeout_ms(cfgc, base)
    assert v >= 670_000, v                          # 600W/1W/1000ms + (10+60)s
    assert v == int((600 / 1.0) * 1000 + (10.0 + 60.0) * 1000)

    class _Fast:                                    # 램프가 빠르면 base 유지
        RF_MAX_POWER = 10
        RF_RAMP_STEP = 10.0
        CHAMBER_RF_CONT_RAMPDOWN_INTERVAL_MS = 10
        POWER_OFF_ZERO_DEADLINE_S = 1.0
    assert rf_power_stop_timeout_ms(_Fast(), base) == base

    class _Bad:                                     # 0/음수/누락 → base
        RF_MAX_POWER = 0
        RF_RAMP_STEP = -1.0
        CHAMBER_RF_CONT_RAMPDOWN_INTERVAL_MS = 0
    assert rf_power_stop_timeout_ms(_Bad(), base) == base
    assert rf_power_stop_timeout_ms(None, base) >= 670_000     # config_common 폴백
    print(f"  (n) 기본 {v/1000:.0f}s, 빠른 램프 {base/1000:.0f}s 유지")
    # _execute_step 이 RF_POWER_STOP 에만 이 한도를 쓰는지(소스)
    src = open(os.path.join(_ROOT, "controller", "process_controller.py"), encoding="utf-8").read()
    i = src.index("hard_wait_actions = {")
    blk = src[i:i + 1400]
    assert "if step.action == ActionType.RF_POWER_STOP:" in blk
    assert "rf_power_stop_timeout_ms(self._cfg, POWER_OFF_TIMEOUT_MS)" in blk


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-s"]))

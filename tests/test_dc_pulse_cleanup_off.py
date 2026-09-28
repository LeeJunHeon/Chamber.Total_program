# -*- coding: utf-8 -*-
"""DC Pulse cleanup 안전망 검증 — output_off() 자체는 바꾸지 않았으므로 가짜 코루틴으로 대체해 안전망만 본다.

러너 예외/취소처럼 종료 절차 없이 정리되면 기존 cleanup() 은 OFF 없이 연결만 닫았다(확인된 사실 5-a).
CH1 은 RF Pulse 와 192.168.1.50:4007 을 공유하므로 '이번 런에서 안 쓴' 장치는 새로 연결하지 않아야 한다.
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

import pytest                                           # noqa: E402
from device.dc_pulse import AsyncDCPulse                # noqa: E402


class _Cfg:
    DCP_CLEANUP_OFF_WAIT_S = 0.5
    DEBUG_PRINT = False


def _mk(*, out_on=False, maybe_on=False, worker_alive=True, off_impl=None, cfg=None):
    """cleanup() 안전망만 돌 수 있는 최소 인스턴스."""
    d = AsyncDCPulse.__new__(AsyncDCPulse)
    d._cfg = cfg or _Cfg()
    d._cfg_float = lambda k, v: float(getattr(d._cfg, k, v))
    d._cfg_int = lambda k, v: int(getattr(d._cfg, k, v))
    d._cfg_bool = lambda k, v: bool(getattr(d._cfg, k, v))
    d.status = []

    async def _emit(msg):
        d.status.append(msg)
    d._emit_status = _emit

    d._out_on = bool(out_on)
    d._output_maybe_on = bool(maybe_on)
    d._off_in_flight = 0
    d._off_idle_evt = asyncio.Event()
    d._off_idle_evt.set()
    d.output_off_unconfirmed = False
    d.cleanup_off_unconfirmed = False
    d._want_connected = True
    d._purge_pending = lambda reason="": 0

    # cleanup 뒷부분(태스크/TCP 정리)은 건드리지 않게 최소 스텁
    d._poll_task = None
    d._watchdog_task = None
    d._reader_task = None
    d._writer = None
    d._reader = None
    d._connected = True
    d._just_reopened = False
    d._last_io_mono = 0.0
    d._frame_q = asyncio.Queue()

    async def _cancel(name):
        setattr(d, name, None)
    d._cancel_task = _cancel

    if worker_alive:
        d._cmd_worker_task = asyncio.get_event_loop().create_future()   # done() == False
    else:
        d._cmd_worker_task = None

    d.off_calls = []
    if off_impl is None:
        async def off_impl():
            d.off_calls.append(time.monotonic())
            d._out_on = False
            d._output_maybe_on = False
            return True
    d.output_off = off_impl if off_impl is not None else off_impl
    if off_impl is None:
        async def _default_off():
            d.off_calls.append(time.monotonic())
            d._out_on = False
            d._output_maybe_on = False
            return True
        d.output_off = _default_off
    return d


async def _cleanup_tail_safe(d):
    """cleanup() 전체를 돌린다(뒷부분은 스텁이라 안전)."""
    await AsyncDCPulse.cleanup(d)


def test_a_out_on_with_worker_calls_output_off():
    async def _main():
        d = _mk(out_on=True, worker_alive=True)
        await _cleanup_tail_safe(d)
        assert len(d.off_calls) == 1, d.off_calls
        assert d.cleanup_off_unconfirmed is False
        assert d._out_on is False                     # cleanup 뒷부분에서 리셋
        assert any("정리 전 DC Pulse 출력 OFF 확인" in s for s in d.status)
        print(f"  (a) output_off {len(d.off_calls)}회, status={[s for s in d.status][:2]}")
    asyncio.run(_main())


def test_b_waits_for_in_flight_output_off():
    async def _main():
        done = []

        async def _slow_off():
            await asyncio.sleep(0.2)
            done.append(1)
            return True
        d = _mk(out_on=True, worker_alive=True, off_impl=_slow_off)
        # 이미 진행 중인 output_off() 를 흉내
        d._off_in_flight = 1
        d._off_idle_evt.clear()

        async def _runner():
            await asyncio.sleep(0.2)
            d._off_in_flight = 0
            d._out_on = False
            d._off_idle_evt.set()
        t = asyncio.create_task(_runner())
        t0 = time.perf_counter()
        await _cleanup_tail_safe(d)
        dt = time.perf_counter() - t0
        await t
        assert d.off_calls == [], "진행 중이면 새로 호출하지 않는다"
        assert d.cleanup_off_unconfirmed is False
        assert 0.15 <= dt < 0.5, dt
        print(f"  (b) {dt:.2f}s 대기 후 중복 호출 없음")
    asyncio.run(_main())


def test_c_no_worker_no_output_off():
    async def _main():
        d = _mk(out_on=True, maybe_on=True, worker_alive=False)
        await _cleanup_tail_safe(d)
        assert d.off_calls == [], "이번 런에서 안 쓴 장치는 새로 연결/전송하지 않는다"
        assert d.cleanup_off_unconfirmed is False, "안 쓴 런마다 경고가 반복되면 안 된다"
        print("  (c) output_off 호출 없음, 경고 없음")
    asyncio.run(_main())


def test_d_output_off_exceeds_wait_limit():
    async def _main():
        async def _hang():
            await asyncio.sleep(10.0)
            return True
        d = _mk(out_on=True, worker_alive=True, off_impl=_hang)
        t0 = time.perf_counter()
        await _cleanup_tail_safe(d)
        dt = time.perf_counter() - t0
        assert d.cleanup_off_unconfirmed is True
        assert dt < _Cfg.DCP_CLEANUP_OFF_WAIT_S + 1.0, dt
        assert any("정리 대기 초과" in s for s in d.status)
        print(f"  (d) {dt:.2f}s (한도 {_Cfg.DCP_CLEANUP_OFF_WAIT_S}s) → cleanup_off_unconfirmed True")
    asyncio.run(_main())


def test_e_output_off_returns_false_no_duplicate_warning():
    async def _main():
        async def _fail_fast():
            return False                              # output_off 가 이미 OUTPUT_OFF 실패 경고를 냈다
        d = _mk(out_on=True, worker_alive=True, off_impl=_fail_fast)
        await _cleanup_tail_safe(d)
        assert d.cleanup_off_unconfirmed is False, "중복 경고 금지"
        assert not any("정리 대기 초과" in s for s in d.status)
        print("  (e) False 반환 → cleanup_off_unconfirmed False")
    asyncio.run(_main())


def test_f_maybe_on_only_also_triggers():
    """OUTPUT_ON 을 보냈지만 ACK/검증이 안 돼 _out_on 이 False 인 경우도 안전망이 돈다(확인된 사실 5-c)."""
    async def _main():
        d = _mk(out_on=False, maybe_on=True, worker_alive=True)
        await _cleanup_tail_safe(d)
        assert len(d.off_calls) == 1, d.off_calls
        print("  (f) _output_maybe_on 만으로도 OFF 확인 시도")
    asyncio.run(_main())


def test_g_output_maybe_on_flag_points_in_source():
    """_output_maybe_on 이 OUTPUT_ON write 직전 True, OUTPUT_OFF 확인 3경로에서 False 인지(소스)."""
    src = open(os.path.join(_ROOT, "device", "dc_pulse.py"), encoding="utf-8").read()
    i_w = src.index("self._writer.write(cmd.payload)")
    head = src[i_w - 400:i_w]
    assert '.split("[", 1)[0].strip() == "OUTPUT_ON"' in head
    assert "self._output_maybe_on = True" in head
    # OUTPUT_OFF 확인 성공 3경로에서 False
    i_off = src.index("# ===== OUTPUT_OFF =====")
    blk = src[i_off:src.index("# ---- 일반 write(REF_POWER", i_off)]
    assert blk.count("self._output_maybe_on = False") == 3, blk.count("self._output_maybe_on = False")
    assert blk.count("self.output_off_unconfirmed = False") == 3
    # 최종 실패/복구불가에서 True + 사유 문구
    assert "OUTPUT_OFF 미확인 — 복구 불가(재연결 실패/정리 중 등) (출력 상태 미확인)" in blk
    print("  (g) 소스 확인 OK (write 직전 True, 확인 3경로 False)")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-s"]))

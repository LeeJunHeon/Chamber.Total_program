# tests/test_oes_worker_cyusb_safety.py
# -*- coding: utf-8 -*-
"""OES 워커(CyUSB) 안전 보완 검증 — 연결 확인 전에 USB 명령이 나가지 않는지. pytest / 직접 실행 모두 가능.

가짜 DLL·도우미는 tests/test_oes_worker_cyusb.py 의 것을 그대로 쓴다.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from test_oes_worker_cyusb import (                           # noqa: E402
    FakeDLL, LIST_A, NPIX, W, _Env, _csv_rows, _init_with, _ready_oes, patched,
)

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

SETTINGS = [("spSetTrgEx", (11, 0)), ("spSetTEC", (1, 0)), ("spSetDblIntEx", (50.0, 0))]


class SeqDevDLL(FakeDLL):
    """spDevInfo 첫 호출(사전 읽기)만 rc=-1, 이후는 serial 을 돌려준다."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.dev_calls = 0

    def _dev_info(self, mb, sb, ch):
        self.dev_calls += 1
        if self.dev_calls == 1:
            return -1
        return super()._dev_info(mb, sb, ch)


class SlowDevDLL(FakeDLL):
    """지정한 번째 spDevInfo 호출에서 멈춘다(단계 타임아웃 재현). 그 전 호출은 rc=-1."""

    def __init__(self, hang_on: int, **kw):
        super().__init__(**kw)
        self.hang_on = hang_on
        self.dev_calls = 0

    def _dev_info(self, mb, sb, ch):
        self.dev_calls += 1
        if self.dev_calls == self.hang_on:
            time.sleep(1.0)
        return -1


def _count(dll, name):
    return dll.names().count(name)


# ═══════════════ A) 측정 시작: 목록 확인 → 설정 순서 ═══════════════
def test_A_start_blocked_sends_no_commands():
    with _Env() as env:
        dll = FakeDLL()
        o = _ready_oes(env.tmp, dll)
        o._link_baseline = sorted(LIST_A)
        with patched(W, _list_cyusb_interfaces=lambda: list(LIST_A[:1]),
                     _LINK_WAIT_AT_START_S=0.2, _LINK_POLL_S=0.01, _LINK_SETTLE_S=0.01):
            try:
                asyncio.run(W._daemon_measure_once(
                    oes=o, ch=1, usb=0, duration_s=1.0, integration_ms=50,
                    sample_interval_s=0.05, avg_count=1, out_dir=None, out_csv=env.tmp / "a" / "x.csv",
                ))
                raise AssertionError("ensure_at_start 가 예외를 내야 함")
            except RuntimeError as e:
                assert "[link]" in str(e), e
        assert dll.calls == [], dll.calls
        assert not (env.tmp / "a" / "x.csv").exists()


def test_A2_settings_after_ensure_in_order():
    with _Env() as env:
        dll = FakeDLL()
        o = _ready_oes(env.tmp, dll)
        o._link_baseline = sorted(LIST_A)
        orig = W._LinkGuard.ensure_at_start

        async def _marked(self):
            dll.calls.append(("ENSURE", ()))
            return await orig(self)

        with patched(W, _list_cyusb_interfaces=lambda: list(LIST_A)), patched(W._LinkGuard, ensure_at_start=_marked):
            rc = asyncio.run(W._daemon_measure_once(
                oes=o, ch=1, usb=0, duration_s=0.1, integration_ms=50,
                sample_interval_s=0.05, avg_count=1, out_dir=None, out_csv=env.tmp / "a" / "y.csv",
            ))
        assert rc == 0
        assert dll.calls[0] == ("ENSURE", ()), dll.calls[:5]
        assert dll.calls[1:4] == SETTINGS, dll.calls[:5]


# ═══════════════ B) 초기화 실패 시 닫힘 유지 + 다음 측정은 읽기 전에 재초기화 ═══════════════
def _assert_closed_then_measure_blocked(env, o, dll):
    assert o.sChannel == -1
    assert _count(dll, "spCloseGivenChannel") == 0

    # 실패 뒤 정리도 닫기 명령을 보내지 않는다
    asyncio.run(o.cleanup())
    assert _count(dll, "spCloseGivenChannel") == 0

    reinits = []

    async def _fail_reset(oes, *, ch, usb):
        reinits.append(1)
        return False

    dll.calls.clear()
    with patched(W, _list_cyusb_interfaces=lambda: list(LIST_A), _daemon_reset_device=_fail_reset):
        try:
            asyncio.run(W._daemon_measure_once(
                oes=o, ch=1, usb=0, duration_s=0.5, integration_ms=50,
                sample_interval_s=0.05, avg_count=1, out_dir=None, out_csv=env.tmp / "b" / "m.csv",
            ))
            raise AssertionError("재초기화 실패면 예외로 끝나야 함")
        except RuntimeError as e:
            assert "재초기화 실패" in str(e), e
    assert reinits == [1]
    assert _count(dll, "spReadDataEx") == 0, dll.calls
    assert dll.calls == [], dll.calls   # 설정 명령도 나가지 않는다


def test_B_a_post_read_serial_mismatch_keeps_closed():
    with _Env() as env:
        dll = SeqDevDLL(serial="SN-B2")
        o, ok = _init_with(env, dll, config={"expected_serial": {"1": "SN-A1"}})
        assert ok is False
        assert o._last_error == "분광기 일련번호 불일치: CH1 기대=SN-A1, 실제=SN-B2", o._last_error
        assert dll.dev_calls == 2
        _assert_closed_then_measure_blocked(env, o, dll)


def test_B_b_final_link_check_fail_keeps_closed():
    with _Env() as env:
        dll = FakeDLL()
        o, ok = _init_with(env, dll, lists=[LIST_A, LIST_A[:1]])
        assert ok is False and o._last_error == "초기화 중 분광기 연결 변화", o._last_error
        _assert_closed_then_measure_blocked(env, o, dll)


def test_B_c_dev_info_timeouts_keep_closed():
    for hang_on, stage in ((1, "dev_info_pre"), (2, "dev_info")):
        with _Env() as env:
            os.environ["OES_INIT_TIMEOUT_S"] = "0.5"
            try:
                dll = SlowDevDLL(hang_on)
                o, ok = _init_with(env, dll)
            finally:
                os.environ.pop("OES_INIT_TIMEOUT_S", None)
            assert ok is False
            assert f"stage={stage}" in o._last_error, o._last_error
            time.sleep(1.1)   # 멈춘 DLL 스레드가 늦게 끝나도 sChannel 이 되살아나지 않아야 함
            _assert_closed_then_measure_blocked(env, o, dll)


# ═══════════════ C) cleanup 닫기 명령 ═══════════════
def _cleanup_with(env, listing, baseline=True):
    dll = FakeDLL()
    o = _ready_oes(env.tmp, dll)
    o._link_baseline = sorted(LIST_A) if baseline else None
    with patched(W, _list_cyusb_interfaces=listing):
        asyncio.run(o.cleanup())
    assert o.sChannel == -1 and o.sp_dll is None
    return dll


def test_C_cleanup_close_rules():
    with _Env() as env:
        logs = []
        with patched(W, _runlog=lambda m: logs.append(m)):
            dll = _cleanup_with(env, lambda: list(LIST_A[:1]))
        assert _count(dll, "spCloseGivenChannel") == 0, dll.calls
        assert "[cleanup] 연결 목록이 기준과 다름 → 닫기 명령 생략" in logs, logs

        dll = _cleanup_with(env, lambda: list(LIST_A))
        assert dll.calls == [("spCloseGivenChannel", (0,))], dll.calls

        dll = _cleanup_with(env, lambda: None)
        assert dll.calls == [("spCloseGivenChannel", (0,))], dll.calls

        dll = _cleanup_with(env, lambda: list(LIST_A[:1]), baseline=False)
        assert dll.calls == [("spCloseGivenChannel", (0,))], dll.calls


# ═══════════════ D) 다른 챔버 분광기만 연결: 포트 리셋 전 일련번호 사전 확인 ═══════════════
def test_D_other_chamber_only_fails_before_reset():
    with _Env() as env:
        dll = FakeDLL(serial="SN-B2", tests=1)
        o, ok = _init_with(env, dll, config={"expected_serial": {"1": "SN-A1"}}, lists=[LIST_A[:1]])
        assert ok is False and o.sChannel == -1
        assert o._last_error == "분광기 일련번호 불일치: CH1 기대=SN-A1, 실제=SN-B2", o._last_error
        assert sorted(set(dll.names())) == ["spDevInfo", "spTestAllChannels"], dll.names()
        for n in ("spSetupGivenChannel", "spInitGivenChannel", "spReadDataEx", "spGetWLTable"):
            assert _count(dll, n) == 0, n


def test_D2_pre_read_fail_falls_back_to_post_read():
    with _Env() as env:
        dll = SeqDevDLL(serial="SN-B2", tests=1)
        o, ok = _init_with(env, dll, config={"expected_serial": {"1": "SN-A1"}}, lists=[LIST_A[:1]])
        assert ok is False
        assert o._last_error == "분광기 일련번호 불일치: CH1 기대=SN-A1, 실제=SN-B2", o._last_error
        assert _count(dll, "spSetupGivenChannel") == 1 and _count(dll, "spDevInfo") == 2
        names = dll.names()
        last_dev = len(names) - 1 - names[::-1].index("spDevInfo")
        assert names.index("spSetupGivenChannel") < last_dev, names   # 두 번째 읽기는 채널 설정 뒤
        assert any("확인=불일치(기대 SN-A1)" in m and "읽은 시점=채널 설정 후" in m for m in env.statuses())

    with _Env() as env:
        dll = SeqDevDLL(serial="SN-A1")
        o, ok = _init_with(env, dll, config={"expected_serial": {"1": "SN-A1"}})
        assert ok is True and o.sChannel == 0, o._last_error
        assert any("읽은 시점=채널 설정 후, 확인=일치(기대 SN-A1)" in m for m in env.statuses()), env.statuses()


def test_D3_pre_read_ok_reads_once():
    with _Env() as env:
        dll = FakeDLL(serial="SN-A1")
        o, ok = _init_with(env, dll, config={"expected_serial": {"1": "SN-A1"}})
        assert ok is True and o.sChannel == 0, o._last_error
        assert _count(dll, "spDevInfo") == 1
        assert dll.names().index("spDevInfo") < dll.names().index("spSetupGivenChannel")
        assert any("읽은 시점=사전, 확인=일치(기대 SN-A1)" in m for m in env.statuses()), env.statuses()


# ═══════════════ E) 확인 상태 표시 ═══════════════
def test_E_config_status():
    with _Env() as env:
        (env.tmp / "oes_config.json").write_text("{not json", encoding="utf-8")
        o, ok = _init_with(env, FakeDLL())
        assert ok is True
        msgs = env.statuses()
        assert "[init] oes_config.json 읽기 실패 → 일련번호 확인 꺼짐" in msgs, msgs
        assert any(m.endswith("확인=꺼짐(expected_serial 없음)") for m in msgs), msgs

    with _Env() as env:
        o, ok = _init_with(env, FakeDLL(), config={"enabled": True})
        msgs = env.statuses()
        assert ok is True and not any("읽기 실패 → 일련번호" in m for m in msgs)
        assert any(m.startswith("[init] 분광기 확인") and m.endswith("확인=꺼짐(expected_serial 없음)") for m in msgs), msgs


# ═══════════════ F) one-shot 루프(cmd_measure) 직접 검증 ═══════════════
def _fake_init_for(dll):
    async def _fake_init(self):
        self.sp_dll = dll
        self._bind_functions()
        self._npix = NPIX
        self._roi_start, self._roi_end = W.ROI_START_DEFAULT, W.ROI_END_DEFAULT
        self._link_baseline = sorted(LIST_A)
        self.sChannel = int(self._usb_index)
        dll.calls.clear()
        return True
    return _fake_init


def _run_cmd_measure(env, out_csv, *, duration_s, sample_interval_s):
    return asyncio.run(W.cmd_measure(
        ch=1, usb=0, duration_s=duration_s, integration_ms=50, sample_interval_s=sample_interval_s,
        avg_count=1, out_dir=None, out_csv=out_csv, dll_path=None,
    ))


def test_F1_cmd_measure_link_drop_and_recover():
    with _Env() as env:
        state = {"list": list(LIST_A), "missing_calls": 0, "reads_while_missing": 0}
        DROP_AT = 4

        def _on_read(n):
            if state["list"] != LIST_A:
                state["reads_while_missing"] += 1
            if n == DROP_AT:
                state["list"] = LIST_A[:1]

        def _fake_list():
            if state["list"] != LIST_A:
                state["missing_calls"] += 1
                if state["missing_calls"] >= 6:
                    state["list"] = list(LIST_A)
            return list(state["list"])

        reinits = []

        async def _fake_reset(oes, *, ch, usb):
            reinits.append(list(state["list"]))
            return True

        dll = FakeDLL(on_read=_on_read)
        out_csv = env.tmp / "f" / "one.csv"
        with patched(W.OESAsync, initialize_device=_fake_init_for(dll)), \
             patched(W, _list_cyusb_interfaces=_fake_list, _daemon_reset_device=_fake_reset,
                     _LINK_SETTLE_S=0.05, _LINK_POLL_S=0.01):
            rc = _run_cmd_measure(env, out_csv, duration_s=1.5, sample_interval_s=0.05)

        assert rc == 0
        ids = [int(float(r[1])) for r in _csv_rows(out_csv)[1:]]
        assert DROP_AT not in ids, ids
        assert state["reads_while_missing"] == 0
        assert reinits == [LIST_A], reinits
        assert any(i > DROP_AT for i in ids) and ids == sorted(ids), ids
        fin = env.finished()
        assert fin["ok"] is True and fin["rows"] == len(ids)
        assert (fin["link_pause_count"], fin["reinit_count"], fin["reset_count"]) == (1, 1, 0), fin
        assert fin["link_pause_s"] > 0


def test_F2_cmd_measure_read_fail_reset_then_reinit():
    with _Env() as env:
        dll = FakeDLL(read_ok=lambda n: n == 1)

        async def _fake_reset(oes, *, ch, usb):
            dll.calls.append(("REINIT", ()))
            return True

        with patched(W.OESAsync, initialize_device=_fake_init_for(dll)), \
             patched(W, _list_cyusb_interfaces=lambda: list(LIST_A), _daemon_reset_device=_fake_reset):
            rc = _run_cmd_measure(env, env.tmp / "f" / "two.csv", duration_s=0.8, sample_interval_s=0.02)

        assert rc == 0
        names = dll.names()
        failed_before = lambda marker: names[:names.index(marker)].count("spReadDataEx") - 1   # 첫 프레임(성공) 제외
        assert failed_before("spSetupGivenChannel") == 3, names[:20]
        assert failed_before("REINIT") == 10, names[:30]
        assert names.count("REINIT") == 1
        fin = env.finished()
        assert fin["rows"] == 1 and fin["reinit_count"] == 1 and fin["reset_count"] >= 1, fin


def _run_all():
    fns = [(n, f) for n, f in sorted(globals().items())
           if n.startswith("test_") and callable(f) and f.__module__ == __name__]
    fails = []
    for n, f in fns:
        try:
            f()
            print(f"  OK   {n}")
        except Exception as e:
            import traceback
            fails.append(n)
            print(f"  FAIL {n}: {type(e).__name__}: {e}")
            traceback.print_exc()
    print("=" * 56)
    print(f"실패 {len(fails)}건" + (": " + ", ".join(fails) if fails else " — 전부 통과"))
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(_run_all())

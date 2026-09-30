# tests/test_oes_worker_cyusb.py
# -*- coding: utf-8 -*-
"""OES 워커(CyUSB DLL 3.0.7 대응) 검증. pytest / 직접 실행 모두 가능.

가짜 DLL 객체(호출 기록)와 속성 바꿔치기로 실제 장치·G: 드라이브 없이 확인한다.
"""
from __future__ import annotations

import asyncio
import contextlib
import csv
import ctypes
import importlib.util
import json
import os
import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

_WORKER = _ROOT / "apps" / "oes_service" / "oes_api.py"
_spec = importlib.util.spec_from_file_location("oes_api_cyusb_under_test", _WORKER)
W = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(W)                                  # type: ignore[union-attr]

LIST_A = [r"\\?\usb#vid_0547&pid_1002#a#{ae18aa60-7f6a-11d4-97dd-00010229b959}",
          r"\\?\usb#vid_0547&pid_1002#b#{ae18aa60-7f6a-11d4-97dd-00010229b959}"]
NPIX = 1056


# ─────────────── 도우미 ───────────────
def _val(a):
    return getattr(a, "value", a)


class _FakeFn:
    def __init__(self, dll, name, impl=None):
        self.dll, self.name, self.impl = dll, name, impl

    def __call__(self, *args):
        self.dll.calls.append((self.name, tuple(_val(a) for a in args)))
        return self.impl(*args) if self.impl else 0


class FakeDLL:
    """호출을 (이름, 인자값) 으로 기록하는 가짜 SPdbUSBm.dll."""

    def __init__(self, *, read_ok=lambda n: True, on_read=None, serial="SN-A1", tests=2):
        self.calls = []
        self.reads = 0
        self._read_ok = read_ok
        self._on_read = on_read
        self._serial = serial
        self._tests = tests

    def _read(self, buf, ch):
        self.reads += 1
        n = self.reads
        if self._on_read:
            self._on_read(n)
        if not self._read_ok(n):
            return -1
        for i in range(len(buf)):
            buf[i] = n
        return 0

    def _dev_info(self, mb, sb, ch):
        if self._serial is None:
            return -1
        ctypes.memmove(mb, b"SM303\x00", 6)
        s = self._serial.encode() + b"\x00"
        ctypes.memmove(sb, s, len(s))
        return 0

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        impl = {
            "spReadDataEx": self._read,
            "spDevInfo": self._dev_info,
            "spTestAllChannels": lambda *_a: self._tests,
            "spGetWLTable": lambda *_a: -1,
        }.get(name)
        fn = _FakeFn(self, name, impl)
        object.__setattr__(self, name, fn)
        return fn

    def names(self):
        return [c[0] for c in self.calls]


@contextlib.contextmanager
def patched(obj, **attrs):
    old = {k: getattr(obj, k) for k in attrs}
    try:
        for k, v in attrs.items():
            setattr(obj, k, v)
        yield
    finally:
        for k, v in old.items():
            setattr(obj, k, v)


class _Env:
    """워커 로그/stdout/NAS/stop 폴더를 가로채는 공통 환경."""

    def __init__(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="oes_cyusb_"))
        self.out = []
        self.stack = contextlib.ExitStack()

    def __enter__(self):
        os.environ["OES_STOP_DIR"] = str(self.tmp / "stop")

        async def _fake_nas(local_csv, ch, *, timeout_s=120.0):
            return False, None, "test: nas skipped", False

        W._DDL_LOCK = None
        self.stack.enter_context(patched(
            W,
            _print_json=lambda obj: self.out.append(obj),
            _runlog=lambda msg: None,
            _errlog=lambda msg: None,
            _errlog_exc=lambda msg: None,
            _copy_csv_to_nas=_fake_nas,
            _worker_base_dir=lambda: self.tmp,
        ))
        return self

    def __exit__(self, *exc):
        self.stack.close()
        os.environ.pop("OES_STOP_DIR", None)
        return False

    def statuses(self):
        return [o.get("message", "") for o in self.out if o.get("kind") == "status"]

    def finished(self):
        fs = [o for o in self.out if o.get("kind") == "finished"]
        assert len(fs) == 1, fs
        return fs[0]


def _ready_oes(tmp: Path, dll: FakeDLL, *, avg=1):
    o = W.OESAsync(chamber=1, usb_index=0, save_directory=str(tmp / "save"), avg_count=avg)
    o.sp_dll = dll
    o._bind_functions()
    o.sChannel = 0
    o._npix = NPIX
    o._roi_start, o._roi_end = W.ROI_START_DEFAULT, W.ROI_END_DEFAULT
    dll.calls.clear()
    return o


def _csv_rows(path: Path):
    with open(path, newline="", encoding="utf-8") as fp:
        return list(csv.reader(fp))


# ═══════════════ 1) 매 샘플 포트 리셋 제거 ═══════════════
def test_1_slice_avg_reads_only():
    with _Env() as env:
        dll = FakeDLL()
        o = _ready_oes(env.tmp, dll, avg=3)
        x, y = o._acquire_one_slice_avg()
        assert x is not None and y is not None
        assert dll.names() == ["spReadDataEx"] * 3, dll.names()
        assert "spSetupGivenChannel" not in dll.names()


# ═══════════════ 2) baseline 호출 제거, 나머지 설정 순서·인자 유지 ═══════════════
def test_2_apply_settings_without_baseline():
    with _Env() as env:
        dll = FakeDLL()
        o = _ready_oes(env.tmp, dll)
        assert not hasattr(o, "_set_baseline") or getattr(o, "_set_baseline") is None
        o._apply_device_settings_blocking(0, 50)
        assert dll.calls == [
            ("spSetTrgEx", (11, 0)),
            ("spSetTEC", (1, 0)),
            ("spSetDblIntEx", (50.0, 0)),
        ], dll.calls
        assert "spSetBaseLineCorrection" not in dll.names()


# ═══════════════ 3) 정상 → 1대 빠짐 → 정상: 일시 중지·재초기화·재개 ═══════════════
def test_3_daemon_link_drop_and_recover():
    with _Env() as env:
        state = {"list": list(LIST_A), "missing_calls": 0, "reads_while_missing": 0}
        DROP_AT = 4   # 4번째 읽기 도중 1대 빠짐 → 그 샘플은 버려져야 함

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
            reinits.append((ch, usb, list(state["list"])))
            return True

        dll = FakeDLL(on_read=_on_read)
        o = _ready_oes(env.tmp, dll)
        o._link_baseline = sorted(LIST_A)

        with patched(W, _list_cyusb_interfaces=_fake_list, _daemon_reset_device=_fake_reset,
                     _LINK_SETTLE_S=0.05, _LINK_POLL_S=0.01):
            out_csv = env.tmp / "out" / "d.csv"
            rc = asyncio.run(W._daemon_measure_once(
                oes=o, ch=1, usb=0, duration_s=1.5, integration_ms=50,
                sample_interval_s=0.05, avg_count=1, out_dir=None, out_csv=out_csv,
            ))

        assert rc == 0
        rows = _csv_rows(out_csv)
        ids = [int(float(r[1])) for r in rows[1:]]
        assert DROP_AT not in ids, f"빠진 순간 읽은 샘플이 기록됨: {ids}"
        assert state["reads_while_missing"] == 0, "끊김 중 DLL 읽기 호출이 있으면 안 됨"
        assert len(reinits) == 1 and reinits[0][2] == LIST_A, reinits
        assert any(i > DROP_AT for i in ids), f"복귀 뒤 행 기록이 이어져야 함: {ids}"
        assert ids == sorted(ids)

        fin = env.finished()
        assert fin["ok"] is True and fin["rows"] == len(ids)
        assert fin["link_pause_count"] == 1
        assert fin["reinit_count"] == 1
        assert fin["reset_count"] == 0
        assert fin["link_pause_s"] > 0
        msgs = env.statuses()
        assert any("[link] 분광기 연결 변화 감지(현재 1대/기준 2대)" in m for m in msgs), msgs
        assert any("[link] 분광기 재연결 → 재초기화 완료, 측정 재개" in m for m in msgs), msgs


# ═══════════════ 4) 목록 정상 + 연속 읽기 실패: 3번째 리셋, 10번째 재초기화 ═══════════════
def test_4_read_fail_reset_then_reinit():
    with _Env() as env:
        events = []

        async def _fake_reset(oes, *, ch, usb):
            events.append("reinit")
            return True

        dll = FakeDLL()
        o = _ready_oes(env.tmp, dll)
        o._link_baseline = sorted(LIST_A)
        orig_setup = o._setup_channel_blocking
        o._setup_channel_blocking = lambda usb: (events.append("reset"), orig_setup(usb))[1]

        async def _run():
            g = W._LinkGuard(o, ch=1, usb=0, integration_ms=50)
            for _ in range(12):
                events.append("fail")
                keep = await g.after_read(False)
                assert keep is False
            return g

        with patched(W, _list_cyusb_interfaces=lambda: list(LIST_A), _daemon_reset_device=_fake_reset):
            g = asyncio.run(_run())

        seq = [e for e in events]
        assert seq.index("reset") == 3, seq            # fail×3 다음
        assert seq.index("reinit") == 3 + 1 + 7, seq   # fail×3, reset, fail×7(=10번째) 다음
        assert seq.count("reinit") == 1, seq           # 30초 제한
        assert ("spSetupGivenChannel", (0,)) in dll.calls
        assert g.reset_count == 1 and g.reinit_count == 1
        msgs = env.statuses()
        assert any("[read] 연속 읽기 실패 3회 → 포트 리셋" in m for m in msgs), msgs
        assert any("[read] 연속 읽기 실패 10회 → 재초기화" in m for m in msgs), msgs


def test_4b_daemon_read_fail_recovery_in_loop():
    """루프 연결 확인: 첫 프레임 뒤 계속 실패하면 리셋·재초기화가 루프에서 일어난다."""
    with _Env() as env:
        reinits = []

        async def _fake_reset(oes, *, ch, usb):
            reinits.append(1)
            return True

        dll = FakeDLL(read_ok=lambda n: n == 1)
        o = _ready_oes(env.tmp, dll)
        o._link_baseline = sorted(LIST_A)
        with patched(W, _list_cyusb_interfaces=lambda: list(LIST_A), _daemon_reset_device=_fake_reset):
            rc = asyncio.run(W._daemon_measure_once(
                oes=o, ch=1, usb=0, duration_s=0.8, integration_ms=50,
                sample_interval_s=0.02, avg_count=1, out_dir=None, out_csv=env.tmp / "o" / "f.csv",
            ))
        assert rc == 0
        fin = env.finished()
        assert fin["rows"] == 1 and fin["reinit_count"] == 1 and fin["reset_count"] >= 1, fin
        assert len(reinits) == 1


def test_4c_first_frame_reset_every_third_empty():
    with _Env() as env:
        dll = FakeDLL(read_ok=lambda n: n >= 7)
        o = _ready_oes(env.tmp, dll)
        g = W._LinkGuard(o, ch=1, usb=0, integration_ms=50)
        x, y = asyncio.run(W._acquire_first_frame(o, delay_s=0.0, guard=g))
        assert x is not None
        assert dll.names().count("spSetupGivenChannel") == 2, dll.names()   # 3번째·6번째 실패
        assert g.reset_count == 2


# ═══════════════ 5) expected_serial ═══════════════
def _init_with(env, dll, *, config=None, lists=None):
    if config is not None:
        (env.tmp / "oes_config.json").write_text(json.dumps(config), encoding="utf-8")
    seq = list(lists or [LIST_A])

    def _fake_list():
        cur = seq.pop(0) if len(seq) > 1 else seq[0]
        return None if cur is None else list(cur)

    o = W.OESAsync(chamber=1, usb_index=0, save_directory=str(env.tmp / "save"))

    def _load():
        o.sp_dll = dll
        o._bind_functions()

    o._load_and_bind_blocking = _load
    with patched(W, _list_cyusb_interfaces=_fake_list):
        ok = asyncio.run(o.initialize_device())
    return o, ok


def test_5_expected_serial_mismatch_fails():
    with _Env() as env:
        o, ok = _init_with(env, FakeDLL(serial="SN-B2"),
                           config={"enabled": False, "expected_serial": {"1": " sn-a1 ", "2": "SN-B2"}})
        assert ok is False
        assert o._last_error == "분광기 일련번호 불일치: CH1 기대=sn-a1, 실제=SN-B2", o._last_error


def test_5b_expected_serial_match_ignores_case_and_space():
    with _Env() as env:
        o, ok = _init_with(env, FakeDLL(serial="SN-A1"),
                           config={"expected_serial": {"1": " sn-a1 "}})
        assert ok is True, o._last_error
        assert o._serial == "SN-A1"


def test_5c_expected_serial_unreadable_fails():
    with _Env() as env:
        o, ok = _init_with(env, FakeDLL(serial=None), config={"expected_serial": {"1": "SN-A1"}})
        assert ok is False and "실제=(읽기 실패)" in o._last_error, o._last_error


def test_5d_no_expected_serial_passes_and_logs():
    with _Env() as env:
        o, ok = _init_with(env, FakeDLL(serial="SN-A1"), config={"enabled": True})
        assert ok is True, o._last_error
        assert o._link_baseline == sorted(LIST_A)
        msgs = env.statuses()
        assert any(m.startswith("[init] 분광기 확인 USB0: 모델=SM303(PDA), 일련번호=SN-A1, 연결 목록=") for m in msgs), msgs
    with _Env() as env:   # 설정 파일 자체가 없어도 통과, 일련번호 못 읽어도 통과
        o, ok = _init_with(env, FakeDLL(serial=None))
        assert ok is True and o._serial == "", o._last_error


def test_5e_link_change_during_init_fails():
    with _Env() as env:
        o, ok = _init_with(env, FakeDLL(), lists=[LIST_A, LIST_A[:1]])
        assert ok is False and o._last_error == "초기화 중 분광기 연결 변화", o._last_error


# ═══════════════ 6) 목록 조회 None(옛 드라이버) → 감시 끔 ═══════════════
def test_6_legacy_driver_watch_off():
    with _Env() as env:
        o, ok = _init_with(env, FakeDLL(), lists=[None])
        assert ok is True and o._link_baseline is None
        assert o.link_ok() is True

    with _Env() as env:
        dll = FakeDLL()
        o = _ready_oes(env.tmp, dll)
        o._link_baseline = None
        with patched(W, _list_cyusb_interfaces=lambda: None):
            rc = asyncio.run(W._daemon_measure_once(
                oes=o, ch=1, usb=0, duration_s=0.4, integration_ms=50,
                sample_interval_s=0.05, avg_count=1, out_dir=None, out_csv=env.tmp / "o" / "l.csv",
            ))
        assert rc == 0
        fin = env.finished()
        assert fin["ok"] is True and fin["rows"] >= 3
        assert (fin["link_pause_count"], fin["reinit_count"], fin["reset_count"]) == (0, 0, 0)
        assert "spSetupGivenChannel" not in dll.names()
        assert not any("[link]" in m for m in env.statuses())


def test_6b_transient_none_is_ok():
    with _Env() as env:
        o = _ready_oes(env.tmp, FakeDLL())
        o._link_baseline = sorted(LIST_A)
        with patched(W, _list_cyusb_interfaces=lambda: None):
            assert o.link_ok() is True
        with patched(W, _list_cyusb_interfaces=lambda: list(reversed(LIST_A))):
            assert o.link_ok() is True
        with patched(W, _list_cyusb_interfaces=lambda: []):
            assert o.link_ok() is False


def _run_all():
    fns = [(n, f) for n, f in sorted(globals().items())
           if n.startswith("test_") and callable(f)]
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

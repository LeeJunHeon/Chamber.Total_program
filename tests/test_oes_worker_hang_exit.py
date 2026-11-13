# tests/test_oes_worker_hang_exit.py
# -*- coding: utf-8 -*-
"""OES 워커 — 읽기 버퍼 크기, DLL 이 갇혔을 때의 하드 종료(첫 프레임·reset), 진단 기록. pytest / 직접 실행 모두 가능.

가짜 DLL·도우미는 tests/test_oes_worker_cyusb.py 의 것을 그대로 쓴다. os._exit 는 패치해서 예외로 바꾼다.

배경(2026-11-12 21:07:24): daemon 첫 프레임 타임아웃 → 일반 except 의 auto-reset 이 갇힌 DLL 스레드 뒤에 줄을 서서
5초 뒤 ok=False, daemon 은 갇힌 스레드와 열린 CSV 핸들을 가진 채 살아남았다.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import sys
import time
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from test_oes_worker_cyusb import (                           # noqa: E402
    FakeDLL, LIST_A, NPIX, W, _Env, _FakeFn, _csv_rows, _ready_oes, patched,
)
from test_oes_worker_cyusb_safety import _fake_init_for       # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


# ─────────────── 도우미 ───────────────
class _Exit(BaseException):
    """패치한 os._exit — 실제처럼 그 자리에서 흐름을 끊는다(except Exception 에 잡히지 않음)."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


def _fake_exit(code):
    raise _Exit(code)


class LenDLL(FakeDLL):
    """spReadDataEx 에 넘어온 버퍼 길이를 기록한다."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.buf_lens = []

    def _read(self, buf, ch):
        self.buf_lens.append(len(buf))
        return super()._read(buf, ch)


class HangDLL(FakeDLL):
    """hang_from 번째 읽기부터 hang_s 초 동안 멈춘다(DLL 스레드가 갇힌 상황 재현)."""

    def __init__(self, hang_from=1, hang_s=0.6, **kw):
        super().__init__(**kw)
        self.hang_from, self.hang_s = hang_from, hang_s

    def _read(self, buf, ch):
        if self.reads + 1 >= self.hang_from:
            self.reads += 1
            time.sleep(self.hang_s)
            return -1
        return super()._read(buf, ch)


class _FakeMutex:
    log = []

    def __init__(self, name):
        self.name = name

    def acquire(self, timeout_ms=60_000):
        _FakeMutex.log.append("acquire")
        return True

    def release(self):
        _FakeMutex.log.append("release")


@contextlib.contextmanager
def _env_vars(**kv):
    old = {k: os.environ.get(k) for k in kv}
    os.environ.update({k: str(v) for k, v in kv.items()})
    try:
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@contextlib.contextmanager
def _spy_open():
    """워커가 여는 파일 객체를 모은다(모듈 전역 open 을 잠시 얹는다)."""
    files = []

    def _open(*a, **k):
        fp = open(*a, **k)
        files.append(fp)
        return fp

    W.open = _open
    try:
        yield files
    finally:
        del W.open


@contextlib.contextmanager
def _capture(env):
    """run 로그·error 로그를 모은다(_Env 는 둘 다 버린다)."""
    run, err = [], []
    with patched(W, _runlog=run.append, _errlog=err.append):
        yield run, err


def _cmds(*objs):
    """cmd_daemon 의 stdin 명령을 순서대로 돌려주고, 다 쓰면 EOF(None)."""
    seq = list(objs)

    async def _read():
        return seq.pop(0) if seq else None
    return _read


def _measure_cmd(out_csv, duration_s=0.3):
    return {"cmd": "measure", "duration_s": duration_s, "integration_ms": 50,
            "sample_interval_s": 0.05, "avg_count": 1, "out_csv": str(out_csv)}


def _run_daemon(env, dll, cmds, *, init=None, **extra):
    """cmd_daemon 을 가짜 초기화·가짜 뮤텍스·패치한 os._exit 로 돌린다. return: (rc 또는 None, 종료 코드 또는 None)"""
    _FakeMutex.log = []
    with patched(W.OESAsync, initialize_device=init or _fake_init_for(dll)), \
         patched(W, _list_cyusb_interfaces=lambda: list(LIST_A), _WinMutex=_FakeMutex,
                 _daemon_read_cmd=_cmds(*cmds), **extra), \
         patched(os, _exit=_fake_exit):
        try:
            return asyncio.run(W.cmd_daemon(1, 0, None, env.tmp / "base")), None
        except _Exit as e:
            return None, e.code


def _wait_dll_thread():
    """갇혔던 가짜 DLL 스레드가 끝나기를 기다린다(다음 테스트가 executor 를 깨끗하게 쓰도록)."""
    W._OES_EXECUTOR.submit(lambda: None).result(timeout=5.0)


# ═══════════════ 1) 읽기 버퍼 크기 ═══════════════
def test_1_buffer_always_covers_full_detector_on_retry_path():
    """1056픽셀 모델: 첫 읽기가 실패해 후보(1056, 1024, 512 …)를 차례로 넘겨도 버퍼는 항상 1056칸 이상."""
    with _Env() as env:
        for fail_until, expect_npix in ((2, 1024), (3, 512), (0, 1056)):
            dll = LenDLL(read_ok=lambda n, k=fail_until: n > k)
            o = _ready_oes(env.tmp, dll)
            npix = o._ensure_npixels(0, W.SP_CCD_PDA)
            assert npix == expect_npix, (fail_until, npix)          # 반환 길이(앞에서 npix 칸) 판단은 그대로
            assert dll.buf_lens and min(dll.buf_lens) >= 1056, dll.buf_lens
            assert min(dll.buf_lens) >= max(W.CANDIDATE_PIXELS), dll.buf_lens


def test_1b_read_pixels_returns_first_npix_only():
    with _Env() as env:
        dll = LenDLL()
        o = _ready_oes(env.tmp, dll)
        for n in (512, 1024, 1056, 4096, 5000):
            rc, arr = o._read_pixels(0, n)
            assert rc == 0 and arr is not None and arr.size == n, (n, arr.size)
            assert dll.buf_lens[-1] == max(n, max(W.CANDIDATE_PIXELS)) >= 1056
        bad = LenDLL(read_ok=lambda n: False)
        o2 = _ready_oes(env.tmp, bad)
        assert o2._read_pixels(0, 512) == (-1, None)
        assert bad.buf_lens == [max(W.CANDIDATE_PIXELS)]


def test_1c_measure_reads_use_full_buffer():
    with _Env() as env:
        dll = LenDLL()
        o = _ready_oes(env.tmp, dll, avg=3)
        x, y = o._acquire_one_slice_avg()
        assert x is not None and len(y) == W.ROI_END_DEFAULT - W.ROI_START_DEFAULT
        assert len(dll.buf_lens) == 3 and min(dll.buf_lens) >= NPIX


# ═══════════════ 2) daemon 첫 프레임 타임아웃 → 하드 종료 ═══════════════
def test_2_daemon_first_frame_timeout_exits_125_without_reset():
    with _Env() as env, _capture(env) as (run, err), _spy_open() as files, \
            _env_vars(OES_FIRST_FRAME_TIMEOUT_S=0.1, OES_CLEANUP_TIMEOUT_S=0.1):
        resets = []

        async def _spy_reset(oes, *, ch, usb):
            resets.append(1)
            return True

        dll = HangDLL()
        out_csv = env.tmp / "d" / "ff.csv"
        stop_dir = Path(os.environ["OES_STOP_DIR"])
        stop_dir.mkdir(parents=True, exist_ok=True)
        rc, code = _run_daemon(env, dll, [_measure_cmd(out_csv), {"cmd": "ping"}], _daemon_reset_device=_spy_reset)

        assert code == 125 and rc is None
        assert resets == []                                           # 갇힌 스레드 뒤에 줄 서는 reset 을 부르지 않는다
        assert "spCloseGivenChannel" not in dll.names()               # 종료 전에 DLL cleanup 을 기다리지 않는다
        assert not [o for o in env.out if o.get("kind") == "pong"]    # 다음 명령을 받지 않는다(daemon 이 살아남지 않음)

        fin = env.finished()
        assert fin["ok"] is False and fin["stop_reason"] == "first_frame_timeout" and fin["stopped"] is True
        assert fin["out_csv"] == str(out_csv.resolve())               # 메인의 finished 매칭 키
        assert fin["rows"] == 0 and "first frame timeout" in fin["error"]
        assert fin["error"].endswith("(daemon ch=1 usb=0)"), fin["error"]
        for k in ("kind", "ok", "stopped", "stop_reason", "ch", "usb", "out_csv", "nas_ok", "nas_csv", "nas_error",
                  "local_deleted", "rows", "elapsed_s", "link_pause_count", "link_pause_s", "reinit_count",
                  "reset_count", "error"):
            assert k in fin, k                                        # 기존 finished(hard_abort) 와 같은 키

        csvs = [fp for fp in files if fp.name == str(out_csv.resolve())]
        assert len(csvs) == 1 and csvs[0].closed                      # CSV 핸들 닫힘
        assert not out_csv.exists()                                   # 0바이트 파일 삭제
        assert any("OES first frame timeout" in m and "(daemon ch=1 usb=0)" in m for m in err), err
        assert not any("auto-reset" in m for m in env.statuses())
        _wait_dll_thread()


def test_2b_daemon_measure_once_timeout_clears_stop_flags():
    with _Env() as env, _env_vars(OES_FIRST_FRAME_TIMEOUT_S=0.1):
        dll = HangDLL()
        o = _ready_oes(env.tmp, dll)
        out_csv = env.tmp / "d" / "ff2.csv"
        stop_dir = Path(os.environ["OES_STOP_DIR"])
        stop_dir.mkdir(parents=True, exist_ok=True)
        g_flag = stop_dir / ".stop_usb0.flag"
        g_flag.write_text("stop\n", encoding="utf-8")
        with patched(os, _exit=_fake_exit):
            try:
                asyncio.run(W._daemon_measure_once(
                    oes=o, ch=1, usb=0, duration_s=0.3, integration_ms=50,
                    sample_interval_s=0.05, avg_count=1, out_dir=None, out_csv=out_csv,
                ))
                raise AssertionError("os._exit(125) 가 호출돼야 함")
            except _Exit as e:
                assert e.code == 125
        assert not g_flag.exists() and not out_csv.exists()
        assert env.finished()["stop_reason"] == "first_frame_timeout"
        _wait_dll_thread()


def test_2c_first_frame_reset_call_timeout_also_exits_125():
    """첫 프레임 중 포트 리셋(spSetupGivenChannel) 타임아웃도 DLL 이 갇힌 것 → 같은 경로, stop_reason 은 reset_timeout."""
    with _Env() as env:
        dll = FakeDLL(read_ok=lambda n: False)
        o = _ready_oes(env.tmp, dll)
        o._setup_channel_blocking = lambda usb: time.sleep(0.5)
        out_csv = env.tmp / "d" / "ff3.csv"
        orig = W._acquire_first_frame

        async def _fast_first(oes, guard=None):
            return await orig(oes, delay_s=0.0, guard=guard)

        with patched(W, _RESET_CALL_TIMEOUT_S=0.1, _acquire_first_frame=_fast_first), patched(os, _exit=_fake_exit):
            try:
                asyncio.run(W._daemon_measure_once(
                    oes=o, ch=1, usb=0, duration_s=0.3, integration_ms=50,
                    sample_interval_s=0.05, avg_count=1, out_dir=None, out_csv=out_csv,
                ))
                raise AssertionError("os._exit(125) 가 호출돼야 함")
            except _Exit as e:
                assert e.code == 125
        fin = env.finished()
        assert fin["stop_reason"] == "reset_timeout" and "spSetupGivenChannel" in fin["error"], fin
        assert fin["reset_count"] == 1 and not out_csv.exists()
        _wait_dll_thread()


def test_2d_daemon_non_timeout_failure_keeps_daemon_and_closes_csv():
    """타임아웃이 아닌 첫 프레임 실패(RuntimeError)는 지금처럼: 예외 finished → auto-reset → daemon 유지. 단 CSV 핸들은 닫힌다."""
    with _Env() as env, _spy_open() as files:
        resets = []

        async def _spy_reset(oes, *, ch, usb):
            resets.append(1)
            return True

        async def _fail_first(oes, guard=None):
            raise RuntimeError("first frame failed: None")

        dll = FakeDLL()
        out_csv = env.tmp / "d" / "rt.csv"
        rc, code = _run_daemon(env, dll, [_measure_cmd(out_csv), {"cmd": "ping"}, {"cmd": "close"}],
                               _daemon_reset_device=_spy_reset, _acquire_first_frame=_fail_first)
        assert (rc, code) == (0, None)                                # 하드 종료 없음
        assert resets == [1]
        fin = env.finished()
        assert fin["ok"] is False and fin["error"] == "RuntimeError: first frame failed: None"
        assert "stop_reason" not in fin and "trace" in fin            # 기존 예외 finished 형식 그대로
        assert any("auto-reset after exception ok=True" in m for m in env.statuses())
        assert [o for o in env.out if o.get("kind") == "pong"]        # 다음 명령을 계속 받는다
        csvs = [fp for fp in files if fp.name == str(out_csv.resolve())]
        assert len(csvs) == 1 and csvs[0].closed


def test_2e_cmd_measure_first_frame_timeout_exits_125_without_cleanup_wait():
    """일회성 경로도 같은 방식: DLL cleanup 을 기다리지 않고 finished 출력 후 125."""
    with _Env() as env, _capture(env) as (run, err), _spy_open() as files, _env_vars(OES_FIRST_FRAME_TIMEOUT_S=0.1):
        dll = HangDLL()
        out_csv = env.tmp / "m" / "ff.csv"
        _FakeMutex.log = []
        t0 = time.time()
        with patched(W.OESAsync, initialize_device=_fake_init_for(dll)), \
             patched(W, _list_cyusb_interfaces=lambda: list(LIST_A), _WinMutex=_FakeMutex), \
             patched(os, _exit=_fake_exit):
            try:
                asyncio.run(W.cmd_measure(ch=1, usb=0, duration_s=0.3, integration_ms=50, sample_interval_s=0.05,
                                          avg_count=1, out_dir=None, out_csv=out_csv, dll_path=None))
                raise AssertionError("os._exit(125) 가 호출돼야 함")
            except _Exit as e:
                assert e.code == 125
        assert time.time() - t0 < 3.0                                 # cleanup 타임아웃(5초)을 기다리지 않았다
        assert "spCloseGivenChannel" not in dll.names()
        fin = env.finished()
        assert fin["ok"] is False and fin["stop_reason"] == "first_frame_timeout", fin
        assert fin["out_csv"] == str(out_csv.resolve()) and fin["rows"] == 0
        assert fin["error"].endswith("(ch=1, usb=0)") and "first frame timeout" in fin["error"]
        assert _FakeMutex.log[:2] == ["acquire", "release"]           # 종료 전에 뮤텍스를 놓았다
        csvs = [fp for fp in files if fp.name == str(out_csv.resolve())]
        assert len(csvs) == 1 and csvs[0].closed and not out_csv.exists()
        assert any("first frame timeout" in m for m in err), err
        _wait_dll_thread()


# ═══════════════ 3) reset 타임아웃 → daemon 하드 종료 ═══════════════
def _init_seq(dll, later):
    """첫 초기화(daemon 기동)는 성공, 그 뒤(reset 의 재초기화)는 later(self) 가 결정."""
    first = _fake_init_for(dll)
    n = {"k": 0}

    async def _init(self):
        n["k"] += 1
        if n["k"] == 1:
            return await first(self)
        return await later(self)
    return _init


async def _hang(self):
    await asyncio.sleep(1.0)
    return True


def test_3_reset_cmd_cleanup_timeout_exits_125():
    with _Env() as env, _capture(env) as (run, err), _env_vars(OES_CLEANUP_TIMEOUT_S=0.1):
        dll = FakeDLL()
        with patched(W.OESAsync, cleanup=_hang):
            rc, code = _run_daemon(env, dll, [{"cmd": "reset"}, {"cmd": "ping"}])
        assert code == 125 and rc is None
        assert [o for o in env.out if o.get("kind") == "reset"] == [{"kind": "reset", "ok": False, "ch": 1, "usb": 0}]
        assert not [o for o in env.out if o.get("kind") == "pong"]
        assert _FakeMutex.log[:2] == ["acquire", "release"]           # 결과 JSON → 뮤텍스 해제 → 종료
        assert any("reset cleanup timeout" in m for m in err) and any("daemon exit 125" in m for m in err), err


def test_3b_reset_cmd_init_timeout_exits_125():
    with _Env() as env, _env_vars(OES_INIT_TIMEOUT_S=0.1):
        dll = FakeDLL()
        rc, code = _run_daemon(env, dll, [{"cmd": "reset"}, {"cmd": "ping"}], init=_init_seq(dll, _hang))
        assert code == 125 and rc is None
        assert [o["ok"] for o in env.out if o.get("kind") == "reset"] == [False]
        assert not [o for o in env.out if o.get("kind") == "pong"]


def test_3c_reset_failure_without_timeout_keeps_daemon():
    async def _false(self):
        return False

    async def _boom(self):
        raise OSError("usb gone")

    for later in (_false, _boom):
        with _Env() as env:
            dll = FakeDLL()
            rc, code = _run_daemon(env, dll, [{"cmd": "reset"}, {"cmd": "ping"}], init=_init_seq(dll, later))
            assert (rc, code) == (0, None), later.__name__            # 끝나지 않고 다음 명령을 받는다
            assert [o["ok"] for o in env.out if o.get("kind") == "reset"] == [False]
            assert len([o for o in env.out if o.get("kind") == "pong"]) == 1


def test_3d_reset_ok_keeps_daemon():
    with _Env() as env:
        dll = FakeDLL()
        rc, code = _run_daemon(env, dll, [{"cmd": "reset"}, {"cmd": "ping"}])
        assert (rc, code) == (0, None)
        assert [o["ok"] for o in env.out if o.get("kind") == "reset"] == [True]


def test_3e_auto_reset_timeout_after_exception_prints_status_then_exits_125():
    with _Env() as env, _env_vars(OES_CLEANUP_TIMEOUT_S=0.1):
        async def _boom_measure(**kw):
            raise RuntimeError("boom")

        dll = FakeDLL()
        with patched(W.OESAsync, cleanup=_hang):
            rc, code = _run_daemon(env, dll, [_measure_cmd(env.tmp / "x.csv"), {"cmd": "ping"}],
                                   _daemon_measure_once=_boom_measure)
        assert code == 125 and rc is None
        assert any("auto-reset after exception ok=False" in m for m in env.statuses()), env.statuses()
        assert env.finished()["error"] == "RuntimeError: boom"
        assert not [o for o in env.out if o.get("kind") == "pong"]
        assert _FakeMutex.log[:2] == ["acquire", "release"]


def test_3f_auto_reset_failure_without_timeout_keeps_daemon():
    with _Env() as env:
        async def _boom_measure(**kw):
            raise RuntimeError("boom")

        async def _false(self):
            return False

        dll = FakeDLL()
        rc, code = _run_daemon(env, dll, [_measure_cmd(env.tmp / "x.csv"), {"cmd": "ping"}],
                               init=_init_seq(dll, _false), _daemon_measure_once=_boom_measure)
        assert (rc, code) == (0, None)
        assert any("auto-reset after exception ok=False" in m for m in env.statuses())
        assert len([o for o in env.out if o.get("kind") == "pong"]) == 1


def test_3g_reset_timeout_flag_is_cleared_on_next_reset():
    with _Env() as env, _env_vars(OES_CLEANUP_TIMEOUT_S=0.1):
        dll = FakeDLL()
        o = _ready_oes(env.tmp, dll)
        with patched(W.OESAsync, cleanup=_hang):
            assert asyncio.run(W._daemon_reset_device(o, ch=1, usb=0)) is False
        assert o._reset_timeout is True
        with patched(W.OESAsync, initialize_device=_fake_init_for(dll)), \
             patched(W, _list_cyusb_interfaces=lambda: list(LIST_A)):
            assert asyncio.run(W._daemon_reset_device(o, ch=1, usb=0)) is True
        assert o._reset_timeout is False


# ═══════════════ 4) 진단 기록 ═══════════════
def test_4_settings_rc_and_exception_are_logged():
    with _Env() as env, _capture(env) as (run, err):
        dll = FakeDLL()
        dll.spSetTEC = _FakeFn(dll, "spSetTEC", lambda *a: -3)

        def _raise(*a):
            raise OSError("access violation")

        dll.spSetDblIntEx = _FakeFn(dll, "spSetDblIntEx", _raise)
        o = _ready_oes(env.tmp, dll)
        o._apply_device_settings_blocking(0, 50)                      # 예외는 지금처럼 무시된다
        assert dll.calls == [("spSetTrgEx", (11, 0)), ("spSetTEC", (1, 0)), ("spSetDblIntEx", (50.0, 0))], dll.calls
        s = [m for m in run if m.startswith("[settings]")]
        assert len(s) == 3, s
        assert "spSetTrgEx rc=0" in s[0] and "spSetTEC rc=-3" in s[1], s
        assert "spSetDblIntEx 예외: OSError: access violation" in s[2], s


def test_4b_daemon_measure_logs_start_first_frame_end_but_not_every_sample():
    with _Env() as env, _capture(env) as (run, err):
        dll = FakeDLL()
        o = _ready_oes(env.tmp, dll, avg=1)
        out_csv = env.tmp / "g" / "a.csv"
        rc = asyncio.run(W._daemon_measure_once(
            oes=o, ch=1, usb=0, duration_s=0.5, integration_ms=50,
            sample_interval_s=0.02, avg_count=2, out_dir=None, out_csv=out_csv,
        ))
        assert rc == 0
        rows = len(_csv_rows(out_csv)) - 1
        assert rows >= 5
        d = [m for m in run if m.startswith("[daemon]") and "open csv" not in m]
        assert d[0] == "[daemon] measure start ch=1 usb=0 integration_ms=50 duration_s=0.5 avg_count=2", d
        assert d[1].startswith("[daemon] first frame ok elapsed="), d
        assert d[-1].startswith(f"[daemon] measure end rows={rows} elapsed=") and d[-1].endswith("stop_reason=None"), d
        assert len(d) == 3, d                                         # 샘플마다 찍지 않는다
        assert len([m for m in run if m.startswith("[settings]")]) == 3


def test_4c_slow_read_is_logged_with_rows():
    with _Env() as env, _capture(env) as (run, err):
        dll = FakeDLL(on_read=lambda n: time.sleep(1.05) if n == 3 else None)
        o = _ready_oes(env.tmp, dll, avg=1)
        rc = asyncio.run(W._daemon_measure_once(
            oes=o, ch=1, usb=0, duration_s=1.4, integration_ms=50,
            sample_interval_s=0.05, avg_count=1, out_dir=None, out_csv=env.tmp / "g" / "s.csv",
        ))
        assert rc == 0
        slow = [m for m in run if "slow read" in m]
        assert len(slow) == 1 and slow[0].startswith("[daemon] slow read 1.") and slow[0].endswith("rows=2"), slow


def test_4d_read_timeout_logs_rows_and_time_since_last_ok():
    with _Env() as env, _capture(env) as (run, err), _env_vars(OES_READ_TIMEOUT_S=0.2):
        dll = HangDLL(hang_from=4, hang_s=0.6)
        o = _ready_oes(env.tmp, dll, avg=1)
        out_csv = env.tmp / "g" / "t.csv"
        with patched(os, _exit=_fake_exit):
            try:
                asyncio.run(W._daemon_measure_once(
                    oes=o, ch=1, usb=0, duration_s=3.0, integration_ms=50,
                    sample_interval_s=0.02, avg_count=1, out_dir=None, out_csv=out_csv,
                ))
                raise AssertionError("os._exit(125) 가 호출돼야 함")
            except _Exit as e:
                assert e.code == 125
        t = [m for m in run if "read timeout" in m]
        assert len(t) == 1 and t[0].startswith("[daemon] read timeout rows=3 since_last_ok=0."), t
        end = [m for m in run if "measure end" in m]
        assert len(end) == 1 and "rows=3" in end[0] and end[0].endswith("stop_reason=read_timeout"), end
        fin = env.finished()
        assert fin["stop_reason"] == "read_timeout" and fin["rows"] == 3 and len(_csv_rows(out_csv)) == 4
        _wait_dll_thread()


def test_4e_cmd_measure_logs_start_first_frame_end():
    with _Env() as env, _capture(env) as (run, err):
        dll = FakeDLL()
        out_csv = env.tmp / "g" / "m.csv"
        with patched(W.OESAsync, initialize_device=_fake_init_for(dll)), \
             patched(W, _list_cyusb_interfaces=lambda: list(LIST_A), _WinMutex=_FakeMutex):
            rc = asyncio.run(W.cmd_measure(ch=1, usb=0, duration_s=0.4, integration_ms=50, sample_interval_s=0.02,
                                           avg_count=1, out_dir=None, out_csv=out_csv, dll_path=None))
        assert rc == 0 and env.finished()["ok"] is True
        rows = len(_csv_rows(out_csv)) - 1
        m = [x for x in run if x.startswith("[measure]")]
        assert m[0] == "[measure] start ch=1 usb=0 integration_ms=50 duration_s=0.4 avg_count=1", m
        assert m[1].startswith("[measure] first frame ok elapsed="), m
        assert m[-1].startswith(f"[measure] end rows={rows} elapsed=") and m[-1].endswith("stop_reason=None"), m
        assert len(m) == 3, m


# ═══════════════ 5) 소스 모양 ═══════════════
def test_5_no_code_between_decorator_and_def():
    """데코레이터와 클래스/함수 사이에 코드가 끼어들지 않았는지(끼면 문법 오류이거나 다른 대상을 꾸민다)."""
    import ast
    src = (Path(W.__file__)).read_text(encoding="utf-8")
    lines = src.splitlines()
    tree = ast.parse(src)
    for node in ast.walk(tree):                                       # (지금 워커에는 데코레이터가 없다 — 생기면 검사된다)
        decos = getattr(node, "decorator_list", None)
        if not decos:
            continue
        last = max(d.end_lineno for d in decos)
        between = [ln.strip() for ln in lines[last:node.lineno - 1]]
        assert all((not ln) or ln.startswith("#") for ln in between), (node.name, between)


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

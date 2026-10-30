"""pytest 공통 — 표식 slow: 골든 하니스·전체 공정 sim·bench 도구 시험대처럼 오래 걸리는 시험(사용자 결정 2026-10-20 ⑦).

전체 시험: python -m pytest v2\tests   (빠른 시험만: -m "not slow")

시험마다 만든 가상 시계를 시험이 끝나면 닫는다(_close_virtual_clocks) — 실패한 시험이 worker·정지 순서 스레드를 멈추지
못하고 끝나도 그 스레드가 다음 시험 중에 ClockDeadlock 경고(PytestUnhandledThreadExceptionWarning)를 남기지 않게.

진단(사용자 지시 5-6 이어서 ■1):
- 세션 끝에 이 pytest 프로세스의 최대 메모리(Windows: GetProcessMemoryInfo 의 PeakWorkingSetSize·PeakPagefileUsage,
  그 밖: resource ru_maxrss)·살아 있는 스레드 수, 시험 파일별 메모리(전용 메모리) 증가 상위 5개, 파일 순서대로의 메모리.
- 실제 시계를 쓰는 시험(RealClock 을 만들었거나 test_app)은 끝난 뒤 그 시험이 만든 스레드가 남으면(최대 2 s 기다림) 실패
  — 앱·로컬 서버·SSE·숨은 창(WM_ENDSESSION) 스레드가 정리되는지.
"""
import collections
import sys
import threading
import time

import pytest

from v2.common import clock as _clock

LEFT_THREAD_WAIT_S = 2.0
_file_growth: "collections.OrderedDict[str, list]" = collections.OrderedDict()   # 파일 → [증가 합, 끝난 뒤 메모리]


def _mem() -> dict:
    """{"ws", "peak_ws", "private", "peak_private"} 바이트(없는 값은 None)."""
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class PMC(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                        ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                        ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t)]
        k = ctypes.WinDLL("kernel32")
        k.GetCurrentProcess.restype = wintypes.HANDLE
        k.K32GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(PMC), wintypes.DWORD]
        c = PMC()
        c.cb = ctypes.sizeof(PMC)
        if k.K32GetProcessMemoryInfo(k.GetCurrentProcess(), ctypes.byref(c), c.cb):
            return {"ws": c.WorkingSetSize, "peak_ws": c.PeakWorkingSetSize,
                    "private": c.PagefileUsage, "peak_private": c.PeakPagefileUsage}
        return {"ws": None, "peak_ws": None, "private": None, "peak_private": None}
    import resource
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024)
    cur = None
    try:
        with open("/proc/self/statm") as f:
            cur = int(f.read().split()[1]) * resource.getpagesize()
    except OSError:
        pass
    return {"ws": cur, "peak_ws": peak, "private": cur, "peak_private": peak}


def _mb(v) -> str:
    return "?" if v is None else f"{v / 1048576:.0f} MB"


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: 오래 걸리는 시험(골든 하니스·전체 공정 sim 등) — 빠른 시험에서 뺀다")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item, nextitem):
    before = _mem()["private"]
    yield
    after = _mem()["private"]
    if before is not None and after is not None:
        g = _file_growth.setdefault(item.nodeid.split("::")[0], [0, 0])
        g[0] += after - before
        g[1] = after


def pytest_terminal_summary(terminalreporter):
    tr, m = terminalreporter, _mem()
    left = [t.name for t in threading.enumerate() if t is not threading.main_thread()]
    tr.section("메모리·스레드 진단")
    tr.write_line(f"pytest 프로세스 최대 메모리: 작업 집합 {_mb(m['peak_ws'])} · 전용 {_mb(m['peak_private'])}"
                  f" (끝: 작업 집합 {_mb(m['ws'])} · 전용 {_mb(m['private'])})")
    tr.write_line(f"살아 있는 스레드: {len(left) + 1}(주 스레드 포함)" + (f" — {sorted(left)[:20]}" if left else ""))
    top = sorted(_file_growth.items(), key=lambda kv: -kv[1][0])[:5]
    tr.write_line("파일별 전용 메모리 증가 상위 5: " + ", ".join(f"{k.split('/')[-1]} {v[0] / 1048576:+.1f} MB" for k, v in top))
    tr.write_line("파일 순서대로 끝난 뒤 전용 메모리(MB): " + " ".join(f"{v[1] / 1048576:.0f}" for v in _file_growth.values()))


@pytest.fixture(autouse=True)
def _close_virtual_clocks(monkeypatch, _real_clock_threads_cleaned):    # 스레드 확인은 가상 시계를 닫은 뒤에
    made = []
    init = _clock.VirtualClock.__init__

    def tracked(self, *a, **k):
        init(self, *a, **k)
        made.append(self)
    monkeypatch.setattr(_clock.VirtualClock, "__init__", tracked)
    yield
    for c in made:
        c.close()


@pytest.fixture(autouse=True)
def _real_clock_threads_cleaned(request, monkeypatch):
    """실제 시계 시험이 만든 스레드가 끝난 뒤에도 남으면 실패."""
    before = set(threading.enumerate())
    real = [request.node.nodeid.split("::")[0].endswith("test_app.py")]
    init = _clock.RealClock.__init__

    def tracked(self, *a, **k):
        init(self, *a, **k)
        real[0] = True
    monkeypatch.setattr(_clock.RealClock, "__init__", tracked)
    yield
    if not real[0]:
        return
    t_end = time.monotonic() + LEFT_THREAD_WAIT_S
    while True:
        left = [t for t in threading.enumerate() if t not in before and t.is_alive()]
        if not left or time.monotonic() >= t_end:
            break
        time.sleep(0.05)
    if left:
        pytest.fail(f"시험이 만든 스레드가 {LEFT_THREAD_WAIT_S:.0f} s 뒤에도 남음: {sorted(t.name for t in left)}")

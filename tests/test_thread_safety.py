# -*- coding: utf-8 -*-
"""스레드 안전성 + 루프 지연 (6d54e95 후속) 검증. Qt offscreen.

1) 워커 스레드에서 PlasmaCleaningRuntime.append_log 호출 → 위젯은 그 스레드에서 직접 안 불리고
   call_soon_threadsafe 로 루프 스레드에서 실행된다
2) update_oes_plot 이 append 대신 replace 를 쓴다 (2048점 → replace 1회, append 0회)
3) OES 쓰로틀: 최소 간격 이내 연속 호출은 건너뛴다
4) 카메라 start 가 호출부에서 to_thread 로 감싸져 루프를 막지 않는다
5) RGA 갱신이 시리즈 풀을 재사용한다(removeSeries 0회, 두 번째 갱신에 addSeries 0회)
"""
import os
import sys
import time
import asyncio
import threading
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                   # noqa: E402


def _qapp():
    from PySide6.QtWidgets import QApplication
    return QApplication.instance() or QApplication([])


# ───────────────────────── 1 ─────────────────────────
def test_1_pc_append_log_marshals_to_loop_thread():
    from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime

    class _SB:
        def value(self): return 0

        def maximum(self): return 0

        def setValue(self, v): pass

    class _FakeWidget:
        def __init__(self):
            self.calls = []          # (thread_id, line)

        def verticalScrollBar(self): return _SB()

        def appendPlainText(self, line):
            self.calls.append((threading.get_ident(), line))

    async def _main():
        loop = asyncio.get_running_loop()
        p = PlasmaCleaningRuntime.__new__(PlasmaCleaningRuntime)
        p._loop = loop
        p._w_log = _FakeWidget()
        p._w_state = None
        p._log_autoscroll_pending = False
        files = []
        p._queue_run_log_line = lambda line: files.append((threading.get_ident(), line))
        loop_tid = threading.get_ident()
        worker_tid = {}

        def _worker():
            worker_tid["id"] = threading.get_ident()
            p.append_log("PLC(Global)", "WARN PLC 재접속 성공")     # 워커 스레드에서 호출
        t = threading.Thread(target=_worker, name="FakePLCWorker")
        t.start()
        t.join(2.0)
        assert p._w_log.calls == [], "워커 스레드에서 위젯을 직접 만지면 안 된다"
        for _ in range(20):
            if p._w_log.calls:
                break
            await asyncio.sleep(0.01)
        assert len(p._w_log.calls) == 1
        assert p._w_log.calls[0][0] == loop_tid and p._w_log.calls[0][0] != worker_tid["id"]
        assert "WARN PLC 재접속 성공" in p._w_log.calls[0][1]
        assert files and files[0][0] == loop_tid            # 파일 기록도 루프 스레드에서 1회
        # 루프 스레드에서 직접 호출하면 즉시 처리(지연 없음)
        p.append_log("PC", "direct")
        assert len(p._w_log.calls) == 2 and p._w_log.calls[1][0] == loop_tid
    asyncio.run(_main())


def test_1b_pc_append_log_without_loop_writes_file_only():
    from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime
    p = PlasmaCleaningRuntime.__new__(PlasmaCleaningRuntime)
    p._loop = None
    hits = []
    p._w_log = type("W", (), {"appendPlainText": lambda self, l: hits.append(l),
                              "verticalScrollBar": lambda self: None})()
    p._w_state = None
    files = []
    p._queue_run_log_line = lambda line: files.append(line)
    p.append_log("PC", "no loop")
    assert hits == [] and len(files) == 1 and "no loop" in files[0]


# ───────────────────────── 2, 3, 5 ─────────────────────────
def _mk_graph():
    _qapp()
    from PySide6.QtWidgets import QWidget
    from controller.graph_controller import GraphController
    w1, w2 = QWidget(), QWidget()
    g = GraphController(w1, w2)
    g._keep = (w1, w2)
    return g


def test_2_oes_uses_replace_once(monkeypatch):
    from lib import config_common as cfgc
    monkeypatch.setattr(cfgc, "OES_PLOT_MIN_INTERVAL_S", 0.0, raising=False)
    g = _mk_graph()
    calls = {"replace": 0, "append": 0}
    orig_replace = g.oes_series.replace
    monkeypatch.setattr(g.oes_series, "replace", lambda pts: (calls.__setitem__("replace", calls["replace"] + 1), orig_replace(pts)))
    monkeypatch.setattr(g.oes_series, "append", lambda *a: calls.__setitem__("append", calls["append"] + 1))
    n = 2048
    xs = [100.0 + i * (1100.0 / (n - 1)) for i in range(n)]
    ys = [float(i % 100) for i in range(n)]
    g.update_oes_plot(xs, ys)
    assert calls == {"replace": 1, "append": 0}, calls
    assert g.oes_series.count() == n


def test_3_oes_throttle(monkeypatch):
    from lib import config_common as cfgc
    monkeypatch.setattr(cfgc, "OES_PLOT_MIN_INTERVAL_S", 0.5, raising=False)
    g = _mk_graph()
    calls = {"n": 0}
    orig = g.oes_series.replace
    monkeypatch.setattr(g.oes_series, "replace", lambda pts: (calls.__setitem__("n", calls["n"] + 1), orig(pts)))
    xs = [100.0 + i for i in range(50)]
    ys = [1.0] * 50
    g.update_oes_plot(xs, ys)
    g.update_oes_plot(xs, ys)
    g.update_oes_plot(xs, ys)
    assert calls["n"] == 1, "0.5초 이내 연속 호출은 건너뛴다"
    g._oes_last_draw_ts = time.monotonic() - 0.6
    g.update_oes_plot(xs, ys)
    assert calls["n"] == 2


def test_5_rga_series_pool_reused(monkeypatch):
    g = _mk_graph()
    n_add = {"n": 0}
    n_rm = {"n": 0}
    orig_add = g.rga_chart.addSeries
    monkeypatch.setattr(g.rga_chart, "addSeries", lambda s: (n_add.__setitem__("n", n_add["n"] + 1), orig_add(s)))
    monkeypatch.setattr(g.rga_chart, "removeSeries", lambda s: n_rm.__setitem__("n", n_rm["n"] + 1))
    xs = list(range(1, 66))
    ys = [1e-8 * (i + 1) for i in range(65)]
    g.update_rga_plot(xs, ys)
    assert n_add["n"] == 65 and len(g._rga_stem_series) == 65
    g.update_rga_plot(xs[:40], ys[:40])              # 두 번째: 풀 재사용, 추가/제거 없음
    assert n_add["n"] == 65 and n_rm["n"] == 0
    assert g.rga_scatter.count() == 40
    assert all(s.count() == 0 for s in g._rga_stem_series[40:])
    assert all(s.count() == 2 for s in g._rga_stem_series[:40])
    g.clear_rga_plot()
    assert n_rm["n"] == 0 and all(s.count() == 0 for s in g._rga_stem_series)


# ───────────────────────── 4 ─────────────────────────
def test_4_camera_start_wrapped_in_to_thread():
    """호출부와 같은 형태(await asyncio.to_thread(recorder.start, owner=...))가 루프를 막지 않는다."""
    import re
    for path in ("runtime/chamber_runtime.py", "runtime/plasma_cleaning_runtime.py"):
        src = open(os.path.join(_ROOT, path), encoding="utf-8").read()
        assert re.search(r"await asyncio\.to_thread\(recorder\.start,\s*owner=", src), path
        assert not re.search(r"^\s*recorder\.start\(", src, re.M), f"{path}: 동기 start 호출이 남아 있다"

    class _Rec:
        def start(self, owner=""):
            time.sleep(1.0)              # join(3.0) 에 물린 상황을 흉내
            return True

    async def _main():
        rec = _Rec()
        ticks = {"n": 0}

        async def _other():
            while True:
                ticks["n"] += 1
                await asyncio.sleep(0.05)
        t = asyncio.create_task(_other())
        t0 = time.perf_counter()
        ok = await asyncio.to_thread(rec.start, owner="chamber1")
        dt = time.perf_counter() - t0
        t.cancel()
        assert ok is True and dt >= 0.9
        assert ticks["n"] >= 10, f"start 대기 중 다른 태스크가 진행돼야 한다 (ticks={ticks['n']})"
    asyncio.run(_main())


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

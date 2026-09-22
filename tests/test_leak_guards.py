# -*- coding: utf-8 -*-
"""무제한 누적 4곳 차단 + 자기 감시(self_watch) 검증. 네트워크/장치 접근 없음.

1) _spawn_detached(store=True) 로 즉시 끝나는/예외 코루틴 각 100개 → _bg_tasks 가 비어 있다
2) server_page submit 실패 → _daily_flush_inflight 복구, _daily_buf 유지
3) _queue_daily_line 상한의 2배 → 버퍼는 상한 이하
4) chat_notifier _post_async 무한 대기 + 카드 1000장 → _pending ≤ CHAT_PENDING_MAX
5) self_watch 샘플러가 psutil 없이도 예외 없이 동작
6) host_process_log _mem_queue 상한
"""
import os
import sys
import asyncio
import builtins
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                   # noqa: E402


# ───────────────────────── 1 ─────────────────────────
def test_1_spawn_detached_forgets_done_tasks():
    from runtime.chamber_runtime import ChamberRuntime

    async def _main():
        c = ChamberRuntime.__new__(ChamberRuntime)
        c.ch = 1
        c._loop = asyncio.get_running_loop()
        c._bg_tasks = []
        c.logs = []
        c.append_log = lambda src, msg: c.logs.append(msg)

        async def _ok():
            return 1

        async def _boom():
            raise RuntimeError("boom")

        ts = []
        for i in range(100):
            ts.append(c._spawn_detached(_ok(), store=True, name=f"ok{i}"))
            ts.append(c._spawn_detached(_boom(), store=True, name=f"boom{i}"))
        assert len(c._bg_tasks) == 200
        await asyncio.gather(*ts, return_exceptions=True)
        await asyncio.sleep(0)            # done_callback 소비
        assert len(c._bg_tasks) == 0, len(c._bg_tasks)
        assert sum(1 for m in c.logs if "crashed" in m) == 100
    asyncio.run(_main())


# ───────────────────────── 2, 3 ─────────────────────────
def _mk_server_page():
    from PySide6.QtWidgets import QApplication
    app = QApplication.instance() or QApplication([])
    from runtime.server_page import ServerPage
    sp = ServerPage(log_root=None)
    sp._daily_flush_timer.stop()
    return sp


def test_2_submit_failure_recovers_inflight():
    sp = _mk_server_page()
    try:
        class _BadExec:
            def submit(self, *a, **k):
                raise RuntimeError("executor shut down")
        sp._daily_io_exec.shutdown(wait=True)
        sp._daily_io_exec = _BadExec()
        sp._daily_buf = ["a", "b", "c"]
        sp._flush_daily_log()
        assert sp._daily_flush_inflight is False
        assert sp._daily_buf == ["a", "b", "c"]
        # 새 줄이 뒤에 붙고 순서 유지
        sp._queue_daily_line("d")
        assert sp._daily_buf == ["a", "b", "c", "d"]
    finally:
        with contextlib.suppress(Exception):
            sp.deleteLater()


def test_3_daily_buf_hard_cap():
    sp = _mk_server_page()
    try:
        class _BadExec:
            def submit(self, *a, **k):
                raise RuntimeError("executor shut down")
        sp._daily_io_exec.shutdown(wait=True)
        sp._daily_io_exec = _BadExec()
        cap = sp._DAILY_BUF_HARD_MAX
        for i in range(cap * 2):
            sp._queue_daily_line(f"line {i}")
        assert len(sp._daily_buf) <= cap, len(sp._daily_buf)
        assert sp._daily_buf_overflow_logged is True      # 경고는 1회만(플래그)
    finally:
        with contextlib.suppress(Exception):
            sp.deleteLater()


# ───────────────────────── 4 ─────────────────────────
def test_4_chat_pending_capped(monkeypatch):
    from lib import config_common as cfgc
    import controller.chat_notifier as CN
    monkeypatch.setattr(cfgc, "CHAT_PENDING_MAX", 200, raising=False)
    monkeypatch.setattr(cfgc, "CHAT_POST_CONCURRENCY", 4, raising=False)
    monkeypatch.setattr(cfgc, "DEV_MODE", False, raising=False)

    async def _main():
        n = CN.ChatNotifier(webhook_url=None)
        n.webhook_default = "http://127.0.0.1:9/dummy"      # 더미(실제 전송 안 함)
        n._defer = False
        gate = asyncio.Event()

        async def _forever(payload, url):
            await gate.wait()
        monkeypatch.setattr(n, "_post_async", _forever)
        for i in range(1000):
            n._post_card("t", subtitle=f"s{i}", urgent=False)      # 일반 카드(긴급은 test_9/10)
        assert len(n._pending) <= 200, len(n._pending)
        assert n._dropped == 800
        # 자리가 나면 다음 카드에 드롭 건수가 1회 표기된다
        gate.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert len(n._pending) == 0
        sent = []
        monkeypatch.setattr(n, "_post_async", lambda pl, url: _capture(sent, pl))
        n._post_card("next", subtitle="x", urgent=True)
        await asyncio.sleep(0)
        assert sent and "생략된 알림 800건" in __import__("json").dumps(sent[0], ensure_ascii=False)
        assert n._dropped == 0

    async def _capture(sent, pl):
        sent.append(pl)
    asyncio.run(_main())


def test_4b_post_semaphore_limits_concurrency(monkeypatch):
    from lib import config_common as cfgc
    import controller.chat_notifier as CN
    monkeypatch.setattr(cfgc, "CHAT_POST_CONCURRENCY", 2, raising=False)
    monkeypatch.setattr(cfgc, "DEV_MODE", False, raising=False)

    async def _main():
        n = CN.ChatNotifier(webhook_url=None)
        active = {"n": 0, "max": 0}
        gate = asyncio.Event()

        async def _inner(payload, url):
            active["n"] += 1
            active["max"] = max(active["max"], active["n"])
            await gate.wait()
            active["n"] -= 1
        monkeypatch.setattr(n, "_post_async_inner", _inner)
        ts = [asyncio.create_task(n._post_async({"text": "x"}, "http://127.0.0.1:9/d")) for _ in range(6)]
        await asyncio.sleep(0.01)
        assert active["max"] == 2
        gate.set()
        await asyncio.gather(*ts)
    asyncio.run(_main())


# ───────────────────────── 5 ─────────────────────────
@pytest.mark.skipif(sys.platform != "win32", reason="ctypes 폴백은 Windows 전용")
def test_5_selfwatch_sample_without_psutil(monkeypatch, tmp_path):
    real_import = builtins.__import__

    def _imp(name, *a, **k):
        if name == "psutil":
            raise ImportError("blocked")
        return real_import(name, *a, **k)
    monkeypatch.setattr(builtins, "__import__", _imp)
    import util.self_watch as sw
    loop = asyncio.new_event_loop()
    try:
        s = sw.sample(loop)
        assert set(sw._CSV_FIELDS) <= set(s.keys())
        assert s["threads"] >= 1 and s["tasks"] == 0
        assert s["working_set_mb"] != -1          # ctypes 폴백
        # 임계/덤프도 예외 없이
        r = sw.SelfWatch.check_thresholds({"private_mb": 9999, "tasks": 0, "handles": 0, "user": 0})
        assert r and r[0].startswith("private_mb=")
        p = sw.write_dump(r, s, loop, out_dir=tmp_path)
        assert p is not None and p.exists()
        txt = p.read_text(encoding="utf-8")
        assert "[sample]" in txt and "[threads" in txt
    finally:
        loop.close()


def test_5b_selfwatch_tick_and_cooldown(monkeypatch, tmp_path):
    from lib import config_common as cfgc
    import util.self_watch as sw
    monkeypatch.setattr(cfgc, "SELFWATCH_DIR", str(tmp_path), raising=False)
    monkeypatch.setattr(cfgc, "SELFWATCH_TASKS_MAX", 1, raising=False)
    monkeypatch.setattr(cfgc, "SELFWATCH_DUMP_COOLDOWN_S", 600.0, raising=False)
    dumps = []
    monkeypatch.setattr(sw, "write_dump", lambda reasons, s, loop, out_dir=None, **kw: dumps.append(reasons) or None)

    class _Chat:
        cards = []

        def notify_error_event(self, src, code, msg, **k):
            _Chat.cards.append((src, msg))

    async def _main():
        loop = asyncio.get_running_loop()
        w = sw.SelfWatch(loop, chat=_Chat(), log=lambda m: None)
        keep = [asyncio.create_task(asyncio.sleep(1)) for _ in range(3)]   # tasks > 1
        s = w.tick()
        assert s is not None and s["tasks"] >= 3
        w.tick()                                   # 쿨다운 안 → 덤프 1회만
        for _ in range(20):
            await asyncio.sleep(0.01)
        assert len(dumps) == 1 and dumps[0][0].startswith("tasks=")
        assert len(_Chat.cards) == 1 and "자원 임계 초과" in _Chat.cards[0][1]
        for t in keep:
            t.cancel()
        # CSV 가 한 줄 이상 기록됐다
        csvs = list(tmp_path.glob("selfwatch_*.csv"))
        assert csvs and csvs[0].read_text(encoding="utf-8").startswith("ts,")
    asyncio.run(_main())


# ───────────────────────── 6 ─────────────────────────
def test_6_host_process_log_mem_queue_cap():
    from util.host_process_log import HostProcessLog
    h = HostProcessLog.__new__(HostProcessLog)
    h._mem_queue = []
    h._mem_queue_max = 100
    h._mem_queue_dropped = 0
    h._mem_queue_drop_warned = False
    for i in range(250):
        h._mem_queue.append(f"line{i}\n")
        h._trim_mem_queue()
    assert len(h._mem_queue) == 100
    assert h._mem_queue[0] == "line150\n"          # 가장 오래된 것부터 버림
    assert h._mem_queue_dropped == 150 and h._mem_queue_drop_warned is True


# ───────────────────────── 후속 보완(987ab9e 이후) ─────────────────────────
def test_7_selfwatch_thresholds_survive_thread_failure(monkeypatch, tmp_path):
    """스레드 생성이 실패해도(메모리 고갈 상황) 임계 판정과 덤프는 수행된다."""
    import threading
    from lib import config_common as cfgc
    import util.self_watch as sw
    monkeypatch.setattr(cfgc, "SELFWATCH_DIR", str(tmp_path), raising=False)
    monkeypatch.setattr(cfgc, "SELFWATCH_TASKS_MAX", 1, raising=False)

    def _no_start(self):
        raise RuntimeError("can't start new thread")
    monkeypatch.setattr(threading.Thread, "start", _no_start)

    async def _main():
        loop = asyncio.get_running_loop()
        w = sw.SelfWatch(loop, chat=None, log=lambda m: None)
        w._io_exec = None                       # 워커 없음 → CSV 도 동기 폴백
        keep = [asyncio.create_task(asyncio.sleep(1)) for _ in range(3)]
        s = w.tick()
        assert s is not None and s["tasks"] >= 3
        for t in keep:
            t.cancel()
    asyncio.run(_main())
    dumps = list(tmp_path.glob("dump_*.txt"))
    assert len(dumps) == 1, dumps
    txt = dumps[0].read_text(encoding="utf-8")
    assert "tasks=" in txt and "[sample]" in txt and "동기 폴백" in txt
    assert list(tmp_path.glob("selfwatch_*.csv")), "CSV 동기 폴백"


def test_8_selfwatch_csv_written_when_submit_fails(monkeypatch, tmp_path):
    from lib import config_common as cfgc
    import util.self_watch as sw
    monkeypatch.setattr(cfgc, "SELFWATCH_DIR", str(tmp_path), raising=False)

    class _BadExec:
        def submit(self, *a, **k):
            raise RuntimeError("executor down")

        def shutdown(self, wait=False):
            pass

    async def _main():
        w = sw.SelfWatch(asyncio.get_running_loop(), chat=None, log=lambda m: None)
        w._io_exec = _BadExec()
        assert w.tick() is not None
        w.stop()
    asyncio.run(_main())
    csvs = list(tmp_path.glob("selfwatch_*.csv"))
    assert csvs
    lines = csvs[0].read_text(encoding="utf-8").splitlines()
    assert lines[0].startswith("ts,") and len(lines) == 2


def test_9_urgent_card_passes_soft_cap(monkeypatch):
    from lib import config_common as cfgc
    import controller.chat_notifier as CN
    monkeypatch.setattr(cfgc, "CHAT_PENDING_MAX", 50, raising=False)
    monkeypatch.setattr(cfgc, "CHAT_PENDING_HARD_MAX", 1000, raising=False)
    monkeypatch.setattr(cfgc, "DEV_MODE", False, raising=False)

    async def _main():
        n = CN.ChatNotifier(webhook_url=None)
        n.webhook_default = "http://127.0.0.1:9/dummy"
        n._defer = False
        gate = asyncio.Event()

        async def _forever(payload, url):
            await gate.wait()
        monkeypatch.setattr(n, "_post_async", _forever)
        for i in range(50):
            n._post_card("t", subtitle=f"s{i}", urgent=False)
        assert len(n._pending) == 50
        n._post_card("normal", subtitle="x", urgent=False)
        assert len(n._pending) == 50 and n._dropped == 1          # 일반 카드는 드롭
        n._post_card("urgent", subtitle="y", urgent=True)
        assert len(n._pending) == 51 and n._dropped == 0          # 긴급 카드는 통과(드롭 건수 표기 소진)
        gate.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
    asyncio.run(_main())


def test_10_urgent_card_dropped_at_hard_cap(monkeypatch):
    from lib import config_common as cfgc
    import controller.chat_notifier as CN
    monkeypatch.setattr(cfgc, "CHAT_PENDING_MAX", 10, raising=False)
    monkeypatch.setattr(cfgc, "CHAT_PENDING_HARD_MAX", 30, raising=False)
    monkeypatch.setattr(cfgc, "DEV_MODE", False, raising=False)

    async def _main():
        n = CN.ChatNotifier(webhook_url=None)
        n.webhook_default = "http://127.0.0.1:9/dummy"
        n._defer = False
        gate = asyncio.Event()

        async def _forever(payload, url):
            await gate.wait()
        monkeypatch.setattr(n, "_post_async", _forever)
        for i in range(100):
            n._post_card("u", subtitle=f"s{i}", urgent=True)
        assert len(n._pending) == 30 and n._dropped == 70
        gate.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
    asyncio.run(_main())


def test_11_overflow_flag_resets_after_successful_flush(tmp_path):
    sp = _mk_server_page()
    try:
        sp._daily_buf_overflow_logged = True
        sp._daily_log_dir = lambda: tmp_path            # type: ignore[assignment]
        sp._flush_daily_log_sync(["a", "b"])
        assert sp._daily_buf_overflow_logged is False
        assert sp._daily_flush_inflight is False
        # 실패 경로에서는 리셋되지 않는다
        sp._daily_buf_overflow_logged = True
        sp._daily_log_dir = lambda: (_ for _ in ()).throw(OSError("nas down"))   # type: ignore[assignment]
        sp._flush_daily_log_sync(["c"])
        assert sp._daily_buf_overflow_logged is True
    finally:
        with contextlib.suppress(Exception):
            if sp._daily_fp:
                sp._daily_fp.close()
            sp.deleteLater()


def test_12_coil_log_summary_low_fail_is_interval_counter():
    """저우선 실패 N회 뒤 요약의 low_fail == N, 요약 후 0."""
    import time as _t
    import device.plc as PLC
    from tests.test_plc_link_policy import _mk, _reset_cfg, _set_cfg
    _reset_cfg()
    _set_cfg(PLC_COIL_LOG_BACKOFF_AFTER=0, PLC_COIL_LOG_SUMMARY_S=0.0)
    p, logs = _mk()
    asyncio.run(p.read_coil(1))                       # 연결 상태
    N = 4
    calls = {"n": 0}

    async def _snap(keys=None):
        calls["n"] += 1
        if calls["n"] <= N:
            raise PLC.PLCError("E402", "no resp", op="snap")
        return {k: 0 for k in keys}
    p.snapshot_all_coils_fast = _snap                 # type: ignore[assignment]
    p._plc_coil_log_stop = asyncio.Event()
    p._plc_coil_log_interval = 0.01
    p._plc_coil_log_keys = list(PLC.PLC_COIL_MAP.keys())[:4]
    p._plc_reg_log_keys = []
    p._consec_timeouts = 0
    p._consec_timeouts_low = 0

    class _W:                                          # CSV 기록 스텁(파일 없음)
        def append_row(self, **k): pass

        def consume_switched_flag(self): return False
    p._plc_coil_csv_writer = _W()

    async def _run():
        t = asyncio.create_task(p._plc_coil_log_loop())
        await asyncio.sleep(0.2)                      # N회 실패 + 이후 성공 tick 들
        p._consec_timeouts_low = 0                    # high 성공을 흉내내 게이지를 0 으로
        await asyncio.sleep(1.05)                     # 요약 주기 하한(1초) 경과 → 요약 1회
        p._plc_coil_log_stop.set()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(t, timeout=1.0)
    asyncio.run(_run())
    summ = [m for m in logs if "PLC COIL LOG summary" in m]
    assert summ, logs[-5:]
    assert f"low_fail={N}" in summ[0], summ[0]
    # 요약 후 리셋: 다음 요약(있다면)은 low_fail=0 이어야 한다
    for m in summ[1:]:
        assert "low_fail=0" in m, m
    _reset_cfg()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

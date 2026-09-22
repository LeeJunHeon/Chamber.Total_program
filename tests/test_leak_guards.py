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
            n._post_card("t", subtitle=f"s{i}", urgent=True)
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
    monkeypatch.setattr(sw, "write_dump", lambda reasons, s, loop, out_dir=None: dumps.append(reasons) or None)

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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

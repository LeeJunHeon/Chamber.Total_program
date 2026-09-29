# -*- coding: utf-8 -*-
"""런 로그 마감(END)과 마지막 장치 줄이 유실되던 문제.

장비 로그(9/28~29):
 · CH2_STO_#15-6_20260928_190619_897.txt — 정상 종료인데 파일 끝에 "# ==== END ====" 가 없다
 · CH1_Pre-sputter_20260929_035127_018.txt — "RFPulse 연결 종료됨" 줄이 없다(다른 런에서는 END 바로 앞)
원인 (i) _log_writer_loop 가 꺼낸 묶음을 run_in_executor 로 넘긴 직후 writer 가 cancel 되면 wait_for 가
아직 시작 전인 executor 작업까지 취소해 그 묶음이 버려진다. (ii) END 를 동기로 바로 넣어, 이벤트 펌프 →
append_log → call_soon 으로 몇 틱 늦게 오는 장치 상태 줄이 END 뒤로 밀리거나 writer 종료와 겹쳐 버려진다.
"""
import os
import sys
import asyncio
import contextlib
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                        # noqa: E402
import runtime.chamber_runtime as CR                                 # noqa: E402
from runtime.chamber_runtime import ChamberRuntime                   # noqa: E402
from util.log_hub import SessionTextAppender                         # noqa: E402

END = "# ==== END ====\n"


def _mk_logger(tmp_path: Path, ch: int = 1) -> ChamberRuntime:
    """실제 로그 writer 경로만 살린 최소 인스턴스."""
    c = ChamberRuntime.__new__(ChamberRuntime)
    c.ch = ch
    c._local_log_dir = Path(tmp_path) / "local"
    c._local_log_dir.mkdir(parents=True, exist_ok=True)
    c._log_file_path = Path(tmp_path) / f"CH{ch}_run.txt"
    c._run_log_appender = SessionTextAppender(fallback_dir=c._local_log_dir, encoding="utf-8")
    c._log_q = asyncio.Queue(maxsize=4096)
    c._log_writer_task = None
    c._log_shutdown_lock = asyncio.Lock()
    c._log_io_exec = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"LogIO.CH{ch}")
    c._ui_log_buf = __import__("collections").deque(maxlen=5000)
    c.logs = []
    c._enqueue_ui_log = lambda line: c.logs.append(line)
    c._soon = lambda fn, *a, **k: fn(*a, **k)
    return c


async def _close_logger(c: ChamberRuntime):
    with contextlib.suppress(Exception):
        await c._shutdown_log_writer()
    # Windows 임시 폴더 정리 시 파일 잠금 방지
    with contextlib.suppress(Exception):
        c._log_io_exec.shutdown(wait=True)


def _read(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines(keepends=True)


# ───────────────────────── (a) writer 가 쉬는 상태에서 END ─────────────────────────
def test_a_end_marker_never_lost(tmp_path, monkeypatch):
    monkeypatch.setattr(CR, "_system_log_append", lambda *a, **k: None)
    N = 30

    async def _one(i: int):
        d = tmp_path / f"r{i}"
        d.mkdir()
        c = _mk_logger(d)
        # 실제 파일 기록은 그대로 하되, 조금 느리게 만들어
        # "executor 에 제출됐지만 아직 끝나지 않은" 순간에 writer 가 cancel 되게 한다(장비 증상 조건).
        _real_write = c._log_write_sync

        def _slow_write(path, text):
            time.sleep(0.05)
            return _real_write(path, text)
        c._log_write_sync = _slow_write

        c._log_writer_task = asyncio.create_task(c._log_writer_loop())
        c._log_enqueue_nowait("line-1\n")
        await asyncio.sleep(0.12)                 # 첫 줄 기록 끝 → writer 가 쉬는 상태
        path = c._log_file_path                   # _shutdown_log_writer 가 경로를 비우므로 먼저 보관
        c._close_run_log()                        # END 를 큐에 넣고
        for _ in range(3):                        # writer 가 꺼내 executor 에 제출할 틈만 준다
            await asyncio.sleep(0)
        await _close_logger(c)                    # 기록이 끝나기 전에 writer 를 종료
        return _read(path)

    async def _main():
        bad = []
        for i in range(N):
            lines = await _one(i)
            if not lines or lines[-1] != END or "line-1\n" not in lines:
                bad.append((i, lines))
        assert bad == [], f"{len(bad)}/{N} 회 유실: {bad[:2]}"
        print(f"  (a) {N}회 모두 END 가 마지막 줄, 앞 줄 유실 없음")
    asyncio.run(_main())


# ───────────────────────── (b)(c) heavy cleanup 전체 경로 ─────────────────────────
class _FakePulse:
    """cleanup 끝에 상태 이벤트를 큐로 내보내는 가짜 펄스 장치(실제 장치와 같은 순서)."""

    def __init__(self):
        self.q: asyncio.Queue = asyncio.Queue()

    def set_process_status(self, on): pass

    async def cleanup(self):
        await self.q.put("대기 중 명령 0개 폐기 (shutdown)")

        async def _late():
            await asyncio.sleep(0.01)
            await self.q.put("RFPulse 연결 종료됨")
        asyncio.get_running_loop().create_task(_late())


class _FakeMFC:
    def __init__(self):
        self.calls = []

    def set_process_status(self, on): self.calls.append(("set_process_status", on))

    def on_process_finished(self, ok, *, reason=None): self.calls.append(("on_process_finished", ok, reason))

    def on_process_cleanup(self): self.calls.append(("on_process_cleanup",))

    async def cleanup(self): pass


class _FakeIG:
    def __init__(self):
        self.calls = []

    async def cancel_wait(self, *, reason="user cancel / stop"):
        self.calls.append(("cancel_wait", reason))

    async def cleanup(self): pass


def _prep_heavy(c: ChamberRuntime, pulse, mfc, ig):
    c.rf_pulse = pulse
    c.dc_pulse = None
    c.mfc = mfc
    c.ig = ig
    c.dc_power = c.dc_power2 = c.rf_power = c.oes = c.rga = None
    c.chat = None
    c._bg_tasks = []
    c._cleanup_timed_out = False
    c._keepalive_tasks = []
    c.append_log = lambda src, msg: c._log_enqueue_nowait(f"[{src}] {msg}\n")
    c._loop_from_anywhere = lambda: asyncio.get_event_loop()
    c._skip_mfc_finalize_due_to_pc = lambda: False
    c._close_run_log_orig = c._close_run_log
    c._shutdown_log_writer_called = False

    async def _noop(*a, **k):
        return None
    c._shutdown_log_writer_real = c._shutdown_log_writer
    c._shutdown_log_writer = _noop          # writer 종료는 테스트가 직접 한다
    c._clear_queue_and_reset_ui = lambda *a, **k: None
    c._release_run_locks = lambda *a, **k: None
    return c


def test_bc_last_device_line_before_end_and_cleanup_labels(tmp_path, monkeypatch):
    monkeypatch.setattr(CR, "_system_log_append", lambda *a, **k: None)
    N = 10

    async def _one(i: int):
        d = tmp_path / f"h{i}"
        d.mkdir()
        c = _mk_logger(d)
        pulse, mfc, ig = _FakePulse(), _FakeMFC(), _FakeIG()
        _prep_heavy(c, pulse, mfc, ig)
        c._log_writer_task = asyncio.create_task(c._log_writer_loop())

        async def _pump():
            with contextlib.suppress(asyncio.CancelledError):
                while True:
                    msg = await pulse.q.get()
                    c.append_log(f"RFPulse{c.ch}", msg)
        pump = asyncio.create_task(_pump())

        path = c._log_file_path                       # 정리 뒤에는 경로가 비워진다
        c._log_enqueue_nowait("공정 종료\n")
        await asyncio.sleep(0.02)                     # writer 가 쉬는 상태로 만든다
        await c._stop_device_watchdogs(light=False)   # 실제 정리 경로(END 포함)
        pump.cancel()
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await pump
        c._shutdown_log_writer = c._shutdown_log_writer_real
        c._log_file_path = path                       # drain 기록이 같은 파일로 가도록 복원
        await _close_logger(c)
        return _read(path), mfc, ig

    async def _main():
        bad = []
        last_mfc = last_ig = None
        for i in range(N):
            lines, mfc, ig = await _one(i)
            last_mfc, last_ig = mfc, ig
            joined = "".join(lines)
            ok = (lines and lines[-1] == END
                  and "RFPulse 연결 종료됨" in joined
                  and len(lines) >= 2 and "RFPulse 연결 종료됨" in lines[-2])
            if not ok:
                bad.append((i, lines[-3:]))
        assert bad == [], f"{len(bad)}/{N} 회 실패: {bad[:2]}"
        print(f"  (b) {N}회 모두 'RFPulse 연결 종료됨' 이 END 바로 앞")

        # (c) 정리 라벨
        assert ("on_process_cleanup",) in last_mfc.calls, last_mfc.calls
        assert not any(x[0] == "on_process_finished" for x in last_mfc.calls), last_mfc.calls
        assert ("cancel_wait", "cleanup") in last_ig.calls, last_ig.calls
        print(f"  (c) MFC on_process_cleanup / IG cancel_wait(reason='cleanup') OK")
    asyncio.run(_main())


# ───────────────────────── (d) 실제 AsyncMFC 라벨 ─────────────────────────
def test_d_real_mfc_cleanup_label():
    from device.mfc import AsyncMFC

    async def _main():
        m = AsyncMFC()
        m.last_setpoints = {1: 5.0, 2: 3.0, 3: 1.0}
        m._flow_on_flags = {1: True, 2: True, 3: False}
        m.flow_error_counters = {1: 2, 2: 0, 3: 1}
        m.on_process_cleanup()
        await asyncio.sleep(0)
        msgs = []
        while not m._event_q.empty():
            ev = m._event_q.get_nowait()
            if getattr(ev, "message", None):
                msgs.append(ev.message)
        assert any("대기 중 명령 0개 폐기 (cleanup)" in s for s in msgs), msgs
        assert m.last_setpoints == {1: 0.0, 2: 0.0, 3: 0.0}
        assert m._flow_on_flags == {1: False, 2: False, 3: False}
        assert m.flow_error_counters == {1: 0, 2: 0, 3: 0}

        # 기존 라벨은 그대로
        for ok, label in ((True, "(process finished (ok))"), (False, "(process finished (fail))")):
            m2 = AsyncMFC()
            m2.on_process_finished(ok)
            await asyncio.sleep(0)
            got = []
            while not m2._event_q.empty():
                ev = m2._event_q.get_nowait()
                if getattr(ev, "message", None):
                    got.append(ev.message)
            assert any(label in s for s in got), (ok, got)
        print("  (d) 실제 AsyncMFC: cleanup / ok / fail 라벨 OK")
    asyncio.run(_main())


# ───────────────────────── (e) 실제 AsyncIG 라벨 ─────────────────────────
def test_e_real_ig_cancel_label():
    from device.ig import AsyncIG

    async def _main():
        for reason, label in ((None, "(user cancel / stop)"), ("cleanup", "(cleanup)")):
            ig = AsyncIG()

            async def _off():
                return None
            ig._send_off_best_effort = _off
            if reason is None:
                await ig.cancel_wait()
            else:
                await ig.cancel_wait(reason=reason)
            await asyncio.sleep(0)
            msgs = []
            while not ig._event_q.empty():
                ev = ig._event_q.get_nowait()
                if getattr(ev, "message", None):
                    msgs.append(ev.message)
            assert any(label in s for s in msgs), (reason, msgs)
        print("  (e) 실제 AsyncIG: user cancel / cleanup 라벨 OK")
    asyncio.run(_main())


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-s"]))

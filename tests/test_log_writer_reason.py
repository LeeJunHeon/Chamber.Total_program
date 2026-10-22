# -*- coding: utf-8 -*-
"""공정 로그 기록 실패 경로 — 스레드에서 도는 내부 함수가 except 변수(e)를 직접 잡지 않는다.

예전: _log_writer_loop._write_emergency / _shutdown_log_writer._write_local_same_name 이 e 를 클로저로 잡아,
except 블록이 먼저 끝나면(취소·2초 대기 초과) e 가 지워져 이미 실행 중인 스레드가 사유 줄에서 NameError 로 멈췄다
(실행 중에는 그 묶음 로그, 종료 때는 마지막 주석 한 줄이 빠짐). 지금은 정의할 때 사유 문자열을 값으로 잡는다.
사유 문구·파일 내용은 예전과 같다. 장치·네트워크 접근 없음.
"""
import os
import sys
import types
import asyncio
import contextlib
from concurrent.futures import ThreadPoolExecutor

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from runtime.chamber_runtime import ChamberRuntime     # noqa: E402


def _nested(fn, name):
    for c in fn.__code__.co_consts:
        if isinstance(c, types.CodeType) and c.co_name == name:
            return c
    raise AssertionError(f"내부 함수 없음: {name}")


def test_thread_closures_do_not_capture_except_variable():
    for outer, inner in ((ChamberRuntime._log_writer_loop, "_write_emergency"),
                         (ChamberRuntime._shutdown_log_writer, "_write_local_same_name")):
        code = _nested(outer, inner)
        assert "e" not in code.co_freevars, (inner, code.co_freevars)


class _App:
    def close(self):
        return None

    def consume_switched_flag(self):
        return False


def _rt(tmp_path):
    rt = ChamberRuntime.__new__(ChamberRuntime)
    rt.ch = 2
    rt._local_log_dir = tmp_path / "local"
    rt._log_file_path = tmp_path / "run" / "CH2_run.txt"
    rt._log_io_exec = ThreadPoolExecutor(max_workers=1)
    rt.ui_msgs = []
    rt._soon = lambda fn, *a, **k: rt.ui_msgs.append(a[0] if a else None)
    rt._enqueue_ui_log = lambda *a, **k: None
    rt._w_log = None
    rt._log_writer_task = None
    rt._run_log_appender = _App()

    def _fail(path, text):
        raise RuntimeError("boom")
    rt._log_write_sync = _fail
    return rt


def test_shutdown_flush_failure_keeps_text_and_reason(tmp_path):
    async def go():
        rt = _rt(tmp_path)
        rt._log_shutdown_lock = asyncio.Lock()
        rt._log_q = asyncio.Queue()
        rt._log_q.put_nowait("line-1\n")
        rt._log_q.put_nowait("line-2\n")
        await rt._shutdown_log_writer()
        rt._log_io_exec.shutdown(wait=True)
    asyncio.run(go())
    target = tmp_path / "local" / "CH2_run.txt"
    assert target.read_text(encoding="utf-8") == (
        "line-1\nline-2\n# [shutdown] final flush failed: RuntimeError('boom')\n")


def test_writer_failure_emergency_file_keeps_reason_and_text(tmp_path):
    async def go():
        rt = _rt(tmp_path)
        rt._log_q = asyncio.Queue()
        task = asyncio.create_task(rt._log_writer_loop())
        rt._log_q.put_nowait("row-A\n")
        for _ in range(500):                                   # 긴급 보관 기록이 끝날 때까지(최대 5초)
            await asyncio.sleep(0.01)
            if any("긴급 보관" in str(m) for m in rt.ui_msgs):
                break
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        rt._log_io_exec.shutdown(wait=True)
        return rt
    rt = asyncio.run(go())
    files = list((tmp_path / "local").glob("CH2_*_writer_pending.txt"))
    assert len(files) == 1, rt.ui_msgs
    assert files[0].read_text(encoding="utf-8") == (
        "# [log-writer pending]\n"
        f"# source_path = {tmp_path / 'run' / 'CH2_run.txt'}\n"
        "# reason = RuntimeError('boom')\n"
        "row-A\n")

# -*- coding: utf-8 -*-
"""CH1 TSP 실행 로그 — 사용 중 필라멘트(671)·에러 코드(206)를 시작 1번·종료 1번 기록.
가짜 TSP(AsyncTSP 의 read_win 만 스텁)·가짜 공정 컨트롤러로 TSPRuntime._run 을 실제 구동해 로그 파일 내용을 본다."""
from __future__ import annotations

import asyncio
import contextlib
import math
import os
import sys
import types
from collections import deque
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

import device.tsp as T                                           # noqa: E402
import runtime.tsp_runtime as R                                  # noqa: E402
from controller.runtime_state import runtime_state              # noqa: E402
from controller.tsp_controller import TSPRunResult               # noqa: E402

# 2026-11-02 실장비 671 응답(ADDR 0x81) — 3번 필라멘트
REAL_671 = bytes.fromhex("02 81 36 37 31 30 30 30 30 30 30 33 03 38 31".replace(" ", ""))


# ═══════════════ 장치 ═══════════════
def test_parse_real_671_response_and_get_active_filament():
    t = T.AsyncTSP(host="x", port=1)
    assert t._parse_response(REAL_671) == ("DATA", "000003")

    async def go():
        async def _rw(win):
            assert win == T.WIN_ACTIVE_FILAMENT
            kind, data = t._parse_response(REAL_671)
            return data
        t.read_win = _rw
        return await t.get_active_filament()
    assert asyncio.run(go()) == 3


@pytest.mark.parametrize("raw,val", [("000003", 3), ("000000", 0), ("", None), ("  ", None), ("abc", None), (None, None)])
def test_strict_decode(raw, val):
    t = T.AsyncTSP(host="x", port=1)

    async def _rw(win):
        return raw
    t.read_win = _rw
    assert asyncio.run(t.get_active_filament()) == val
    assert asyncio.run(t.get_error_code()) == val


def test_labels():
    assert [T.filament_label(n) for n in (0, 1, 2, 3, None, 7)] == [
        "Mini Ti-Ball", "필라멘트 1", "필라멘트 2", "필라멘트 3", "읽기 실패", "알 수 없음(7)"]
    assert [T.error_label(c) for c in range(6)] == ["없음", "과열", "Mini Ti-Ball 끊김", "TSP 필라멘트 끊김",
                                                    "TSP 필라멘트 전체 소진", "단락"]
    assert T.error_label(None) == "읽기 실패" and T.error_label(9) == "알 수 없음(9)"


# ═══════════════ 런타임 _run ═══════════════
class FakeTSP(T.AsyncTSP):
    """read_win 만 스텁 — 671 은 시작/종료 순서대로, 206 은 고정. 값이 Exception 이면 던지고 'hang' 이면 멈춘다."""
    script_671: list = []
    resp_206: object = "000000"
    calls: list = []

    def __init__(self, host, port):
        super().__init__(host=host, port=port)
        self._671 = list(type(self).script_671)

    async def read_win(self, win3):
        type(self).calls.append(("read", win3))
        v = self._671.pop(0) if win3 == T.WIN_ACTIVE_FILAMENT else type(self).resp_206
        if v == "hang":
            await asyncio.sleep(60)
        if isinstance(v, BaseException):
            raise v
        return v

    async def off(self, *, wait_ok=1.5):
        type(self).calls.append(("off",))

    async def aclose(self):
        type(self).calls.append(("aclose",))


class FakeCtrl:
    mode = "ok"

    def __init__(self, **kw):
        self.log = kw["log_cb"]

    async def run(self, cfg):
        FakeTSP.calls.append(("run",))
        self.log("[TSP] cycle work")
        if FakeCtrl.mode == "error":
            raise RuntimeError("장비 오류")
        if FakeCtrl.mode == "hang":
            await asyncio.sleep(60)
        return TSPRunResult(success=True, cycles_done=1, final_pressure=1e-7, reason="ok")


class FakeIG:
    pass


def _rt(tmp_path):
    rt = R.TSPPageController.__new__(R.TSPPageController)
    rt.ui = types.SimpleNamespace()
    rt.host, rt.tsp_port, rt.ig_port = "127.0.0.1", 4004, 4001
    rt.chat = None
    rt._log_root = tmp_path
    rt._log_file_path = None
    rt._prestart_buf = deque(maxlen=1000)
    rt._log_dir = tmp_path / "TSP"
    rt._log_q = asyncio.Queue(maxsize=4096)
    rt._log_fp = None
    rt._log_writer_task = None
    rt._ig_ext = True
    rt.ig = FakeIG()
    rt.tsp = None
    rt._task = None
    rt._busy = True
    rt._defaults = {"target": "2.5e-07", "cycles": "10"}
    rt._post_warning = lambda *a, **k: None
    return rt


@pytest.fixture
def run_tsp(tmp_path, monkeypatch):
    monkeypatch.setattr(R, "AsyncTSP", FakeTSP)
    monkeypatch.setattr(R, "TSPProcessController", FakeCtrl)
    monkeypatch.setattr(R.TSPPageController, "_FIL_READ_TIMEOUT_S", 0.2)

    def go(script_671, resp_206="000000", mode="ok", cancel_after=None):
        FakeTSP.script_671, FakeTSP.resp_206, FakeTSP.calls = list(script_671), resp_206, []
        FakeCtrl.mode = mode
        rt = _rt(tmp_path)
        with contextlib.suppress(Exception):
            runtime_state.mark_finished("tsp")

        async def main():
            t = asyncio.create_task(rt._run(2.5e-7, 1))
            if cancel_after is not None:
                await asyncio.sleep(cancel_after)
                t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t
        asyncio.run(main())
        files = list((tmp_path / "TSP").glob("TSP_*.txt"))
        assert len(files) == 1
        text = files[0].read_text(encoding="utf-8")
        for f in files:
            f.unlink()
        assert rt._busy is False and rt._task is None and rt.tsp is None and rt._log_writer_task is None
        assert not runtime_state.is_running("tsp")
        return text, FakeTSP.calls
    return go


def _fil(text):
    return [ln.split("[TSP] ", 1)[1] for ln in text.splitlines() if "[FILAMENT]" in ln]


def test_normal_end_same_filament(run_tsp):
    text, calls = run_tsp(["000003", "000003"])
    assert _fil(text) == ["[FILAMENT] 시작 시 사용 중 필라멘트: 3번",
                          "[FILAMENT] 종료 시 사용 중 필라멘트: 3번 (에러 코드 0 = 없음)",
                          "[FILAMENT] 실행 중 필라멘트 변경 없음"]
    lines = text.splitlines()
    i_start = next(i for i, ln in enumerate(lines) if "=== TSP 공정 시작 ===" in ln)
    i_fil = next(i for i, ln in enumerate(lines) if "시작 시 사용 중 필라멘트" in ln)
    i_res = next(i for i, ln in enumerate(lines) if "=== RESULT ===" in ln)
    assert i_start < i_fil < i_res                                    # 시작 줄 직후, 공정 전
    names = [c[0] if c[0] != "read" else c[1] for c in calls]
    assert names == ["671", "run", "off", "671", "206", "aclose"]      # off 다음, aclose 전


def test_filament_changed_during_run(run_tsp):
    text, _ = run_tsp(["000001", "000002"], resp_206="000003")
    assert _fil(text)[1:] == ["[FILAMENT] 종료 시 사용 중 필라멘트: 2번 (에러 코드 3 = TSP 필라멘트 끊김)",
                              "[FILAMENT] 실행 중 필라멘트 변경: 1번 → 2번 (컨트롤러 자동 전환 추정, 에러 코드 3 = TSP 필라멘트 끊김)"]


def test_user_stop_still_logs_end(run_tsp):
    text, _ = run_tsp(["000002", "000002"], mode="hang", cancel_after=0.3)
    assert "[사용자 중단]" in text
    assert _fil(text)[1:] == ["[FILAMENT] 종료 시 사용 중 필라멘트: 2번 (에러 코드 0 = 없음)",
                              "[FILAMENT] 실행 중 필라멘트 변경 없음"]


def test_error_end_still_logs_end(run_tsp):
    text, _ = run_tsp(["000001", "000001"], mode="error")
    assert "[ERROR] 실행 실패" in text and "종료 시 사용 중 필라멘트: 1번" in text


def test_read_exception_and_timeout_do_not_block(run_tsp):
    text, calls = run_tsp([OSError("연결 거부"), "hang"], resp_206=OSError("x"))
    f = _fil(text)
    assert f[0].startswith("[FILAMENT] 시작 시 필라멘트 번호 읽기 실패(OSError(")
    assert f[1].startswith("[FILAMENT] 종료 시 필라멘트 번호 읽기 실패(0.2초 응답 없음) (에러 코드 읽기 실패(OSError(")
    assert len(f) == 2                                                 # 둘 중 하나라도 실패면 변경 줄 없음
    assert ("run",) in calls and "=== RESULT === ok=True" in text      # 공정은 그대로 진행


def test_all_filaments_used_up_warns_cartridge(run_tsp):
    text, _ = run_tsp(["000003", "000003"], resp_206="000004")
    assert _fil(text)[-1] == "[FILAMENT] ⚠ TSP 카트리지 필라멘트 전체 소진 — 카트리지 교체 필요"
    assert "(에러 코드 4 = TSP 필라멘트 전체 소진)" in text


def test_race_guard_return_reads_nothing(tmp_path, monkeypatch):
    monkeypatch.setattr(R, "AsyncTSP", FakeTSP)
    FakeTSP.calls = []
    rt = _rt(tmp_path)
    runtime_state.mark_started("tsp")
    try:
        asyncio.run(rt._run(2.5e-7, 1))
    finally:
        runtime_state.mark_finished("tsp")
    assert FakeTSP.calls == [] and rt.tsp is None                     # 장비 인스턴스도 만들기 전 — 읽지 않음
    assert not (tmp_path / "TSP").exists() or not list((tmp_path / "TSP").glob("*.txt"))


def test_drain_writes_last_lines_before_cancel(tmp_path):
    """종료 직전 큐에 넣은 줄이 라이터 취소 전에 파일에 쓰인다."""
    rt = _rt(tmp_path)
    (tmp_path / "TSP").mkdir()
    rt._log_file_path = tmp_path / "TSP" / "TSP_x.txt"

    async def go():
        rt._log_writer_task = asyncio.create_task(rt._log_writer_loop())
        for i in range(300):
            rt._log_enqueue_nowait(f"line {i}\n")
        await rt._drain_log_queue(2.0)
        rt._log_writer_task.cancel()
        await rt._log_writer_task
    asyncio.run(go())
    lines = rt._log_file_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 300 and lines[-1] == "line 299"

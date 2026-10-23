# -*- coding: utf-8 -*-
"""Plasma Cleaning 실행 중 런의 채널이 바뀌지 않는다 — 다른 채널 START 거절·Process List 불러오기·종료 정리.

예전: CH1 PC 실행 중 로봇이 use_ch=2 레시피로 START_PLASMA_CLEANING 을 보내면, 레시피 적용(화면 값·
set_selected_ch(2))이 먼저 돌고 _on_click_start 에서 E205 로 거절됐다. 실행 중인 런은 끝날 때 바뀐 채널(CH2)로
실행 표시를 풀어 pc[1] 이 '실행 중'으로 남았다(재시작 전까지 CH1 스퍼터·PC·TSP 시작 거절).
Process List 로 use_ch=2 파일을 불러와도 같았다(main.py 라디오는 되돌리지만 set_selected_ch 는 직접 호출).
로봇 응답(E205·문구)과 레시피 파일 오류 코드가 먼저인 순서는 그대로. 장치·네트워크 접근 없음.
"""
import os
import sys
import asyncio
import inspect
import contextlib

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime   # noqa: E402
from controller.runtime_state import runtime_state                  # noqa: E402
from lib import config_common as cfgc                               # noqa: E402
from host.context import HostContext                                # noqa: E402
from host.handlers import HostHandlers                              # noqa: E402

HEADER = "use_ch,process_name,base_pressure,gas_flow,working_pressure,rf_power,process_time"


class _Chat:
    def __init__(self):
        self.cards = []

    def notify_process_finished_detail(self, ok, payload):
        self.cards.append((ok, dict(payload)))

    def flush(self):
        return None


def _pc(*, running=True, ch=1):
    pc = PlasmaCleaningRuntime.__new__(PlasmaCleaningRuntime)
    pc.logs = []
    pc.append_log = lambda s, m: pc.logs.append(m)
    pc.warnings = []
    pc._post_warning = lambda *a, **k: pc.warnings.append(a)
    pc._post_critical = lambda *a, **k: None
    pc._cfg_mod = cfgc
    pc._host_start_future = None
    pc._host_start_code = None
    pc.chat = None
    pc.pc = None
    pc._pc_host_key = None
    pc.ui = None
    pc._selected_ch = ch
    pc._running = running
    pc._final_notified = False
    pc._loaded_recipe_row = {"process_name": "RUNNING"}
    pc._host_recipe_path = "running.csv"
    pc._host_recipe_rows = 3
    pc.applied = []
    pc._apply_recipe_row_to_ui = lambda row: pc.applied.append(dict(row))
    return pc


def _handler(pc):
    ctx = HostContext(log=lambda t, m: None, plc=None, ch1=None, ch2=None, pc=pc, runtime_state=runtime_state,
                      lock_plc=asyncio.Lock(), lock_ch1=asyncio.Lock(), lock_ch2=asyncio.Lock())
    return HostHandlers(ctx)


def _csv(tmp_path, use_ch=2):
    p = tmp_path / f"pc_ch{use_ch}.csv"
    p.write_text(HEADER + "\r\n" + f"{use_ch},PC-CH{use_ch},5e-6,20,3,100,5\r\n", encoding="utf-8-sig")
    return str(p)


def test_host_start_while_running_rejected_before_touching_recipe(tmp_path):
    pc = _pc(running=True, ch=1)

    async def go():
        return await _handler(pc)._start_plasma_cleaning_impl({"recipe": _csv(tmp_path, 2)})
    res = asyncio.run(go())
    assert res["result"] == "fail" and res["error_code"] == "E205" and res["client_action"] == "RETRY", res
    assert res["detail"] == "이미 Plasma Cleaning이 실행 중입니다.", res
    assert pc.warnings == [("실행 중", "이미 Plasma Cleaning이 실행 중입니다.")]      # 경고창은 예전과 같다
    assert pc.applied == []                                                          # 화면·채널에 넣지 않음
    assert pc._selected_ch == 1
    assert pc._loaded_recipe_row == {"process_name": "RUNNING"}
    assert (pc._host_recipe_path, pc._host_recipe_rows) == ("running.csv", 3)


def test_recipe_file_error_still_comes_first_while_running(tmp_path):
    pc = _pc(running=True, ch=1)

    async def go():
        return await _handler(pc)._start_plasma_cleaning_impl({"recipe": str(tmp_path / "nope.csv")})
    res = asyncio.run(go())
    assert res["result"] == "fail" and res["error_code"] == "E207", res              # 순서 그대로
    assert pc._selected_ch == 1 and pc.applied == []


def test_host_start_when_idle_still_applies_recipe(tmp_path):
    """(그대로) 실행 중이 아니면 레시피를 화면에 넣고 시작으로 넘어간다."""
    pc = _pc(running=False, ch=1)
    started = []

    async def fake_start(**kw):
        started.append(kw)
        pc._host_report_start(True, "preflight OK")
    pc._on_click_start = fake_start

    async def go():
        return await _handler(pc)._start_plasma_cleaning_impl({"recipe": _csv(tmp_path, 2)})
    res = asyncio.run(go())
    assert res["result"] == "success", res
    assert [r.get("use_ch") for r in pc.applied] == ["2"] and len(started) == 1


def test_set_selected_ch_ignored_while_running():
    pc = _pc(running=True, ch=1)
    pc.set_selected_ch(2)
    assert pc._selected_ch == 1
    assert any("실행 중" in m and "CH2" in m for m in pc.logs), pc.logs
    pc._running = False
    pc.set_selected_ch(2)
    assert pc._selected_ch == 2


def test_finish_releases_channel_marked_at_start():
    """종료 정리는 시작 때 표시한 채널로 — 선택 채널이 그 사이 바뀌었어도 pc[1] 이 풀린다."""
    runtime_state.mark_started("pc", 1)
    runtime_state.set_running("pc", True, 1)
    try:
        pc = _pc(running=False, ch=2)                 # 어떤 경로로든 선택 채널이 2 가 된 상태
        pc.chat = _Chat()

        async def go():
            await pc._notify_finish_once(ok=True, reason=None, stopped=False, ch=1)
        asyncio.run(go())
        assert runtime_state.is_running("pc", 1) is False
        assert pc.chat.cards and pc.chat.cards[0][1]["prefix"] == "CH1 Plasma Cleaning"
        assert pc.chat.cards[0][1]["ch"] == 1
    finally:
        runtime_state.mark_finished("pc", 1)


def test_finish_without_ch_keeps_old_behavior():
    runtime_state.mark_started("pc", 2)
    runtime_state.set_running("pc", True, 2)
    try:
        pc = _pc(running=False, ch=2)

        async def go():
            await pc._notify_finish_once(ok=True, reason=None, stopped=False)
        asyncio.run(go())
        assert runtime_state.is_running("pc", 2) is False
    finally:
        runtime_state.mark_finished("pc", 2)


@pytest.mark.parametrize("needle", [
    "await self._notify_finish_once(ok=ok_final, reason=final_reason, stopped=stopped_final, ch=ch)",
])
def test_run_paths_pass_start_channel(needle):
    src = inspect.getsource(PlasmaCleaningRuntime._on_click_start)
    assert src.count(needle) == 2                     # TEST MODE·실공정 두 곳
    assert "_selected_ch" not in src.split("# [C] 마지막에 종료 통지")[1]   # 종료 뒤 기록·콜백도 시작 채널

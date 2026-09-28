# -*- coding: utf-8 -*-
"""Loadlock_Chamber 판정 = '마지막으로 끝난 플라즈마 클리닝의 결과' (2026-09-23 5일 error 고착 사고).

실제 runtime_state 싱글톤을 실제 코드와 같은 순서(mark_started → set_error → mark_finished)로 조작해 검증한다.
"""
import os
import re
import sys
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                              # noqa: E402
from controller.runtime_state import runtime_state         # noqa: E402
from host.handlers import loadlock_error_channel           # noqa: E402


def _reset():
    for ch in (1, 2):
        runtime_state.clear_error("pc", ch)
        runtime_state._running.setdefault("pc", {})[ch] = False
        runtime_state._last_finish_mono.setdefault("pc", {}).pop(ch, None)
        runtime_state._last_start_mono.setdefault("pc", {}).pop(ch, None)


@pytest.fixture(autouse=True)
def _clean():
    _reset()
    yield
    _reset()


def _run(ch: int, ok: bool, msg: str = "PC 실패"):
    """실제 코드와 같은 순서로 한 런을 흉내낸다."""
    runtime_state.mark_started("pc", ch)          # 시작 시 그 채널 에러 해제
    if not ok:
        runtime_state.set_error("pc", ch, msg)
    else:
        runtime_state.clear_error("pc", ch)       # 정상/STOP 종료 경로
    runtime_state.mark_finished("pc", ch)


# ───────────────────────── (a)~(f) 판정 ─────────────────────────
def test_a_incident_ch1_fail_then_ch2_success():
    """★ 사고 재현: ch1 실패 → 이후 ch2 성공 → error 아님."""
    _run(1, ok=False, msg="20:09 CH1 PC 실패")
    assert loadlock_error_channel(runtime_state) == 1      # ch2 성공 전에는 error
    for _ in range(4):                                     # 9/24~9/28 CH2 성공 4회
        _run(2, ok=True)
    assert loadlock_error_channel(runtime_state) is None
    assert runtime_state.has_error("pc", 1) is True         # ch1 이력은 남아 있어도 판정에는 안 쓴다


def test_b_ch2_success_then_ch1_fail():
    _run(2, ok=True)
    _run(1, ok=False)
    assert loadlock_error_channel(runtime_state) == 1


def test_c_ch1_fail_only():
    _run(1, ok=False)
    assert loadlock_error_channel(runtime_state) == 1


def test_d_both_success():
    _run(1, ok=True)
    _run(2, ok=True)
    assert loadlock_error_channel(runtime_state) is None


def test_e_no_history():
    assert loadlock_error_channel(runtime_state) is None


def test_f_both_fail_latest_wins():
    _run(1, ok=False, msg="ch1 fail")
    _run(2, ok=False, msg="ch2 fail")
    assert loadlock_error_channel(runtime_state) == 2       # 더 최근에 끝난 채널
    _run(1, ok=False, msg="ch1 fail again")
    assert loadlock_error_channel(runtime_state) == 1


def test_f2_error_without_finish_history():
    """종료 이력이 없는데 에러만 있는 상태(보수적으로 error)."""
    runtime_state.set_error("pc", 2, "종료 기록 전 실패")
    assert loadlock_error_channel(runtime_state) == 2


def test_g_running_wins_before_error_branch():
    """(g) 실행 중이면 상위 분기에서 running — 에러 판정까지 가지 않는다(소스 순서로 고정)."""
    _run(1, ok=False)
    runtime_state.mark_started("pc", 1)                    # 다시 실행 중(에러는 여기서 해제됨)
    assert runtime_state.is_running("pc", 1) is True
    src = open(os.path.join(_ROOT, "host", "handlers.py"), encoding="utf-8").read()
    i_def = src.index("def _loadlock_state() -> str:")
    i_run = src.index('return "running"', i_def)
    i_err = src.index("loadlock_error_channel(rs)", i_def)
    assert i_run < i_err, "is_running 조기 반환이 에러 판정보다 앞이어야 한다"
    # running 판정/폴백 경로는 수정 전과 동일한 형태로 남아 있다
    blk = src[i_def:src.index('return "idle"', i_def)]
    assert 'if rs.is_running("pc", ch):' in blk and "_ch1_is_waiting_ig()" in blk
    assert 'pc = getattr(self.ctx, "pc", None)' in blk and 'return "running" if cleaning else "idle"' in blk
    assert blk.count('return "error"') == 3                # 조회 예외 2 + 판정 1 (기존과 동일 개수)


def test_h_exception_propagates_to_caller():
    """조회 중 예외는 삼키지 않는다 → 호출부가 기존처럼 'error' 로 처리."""
    class _Boom:
        def last_finished(self, *a, **k):
            raise RuntimeError("boom")

        def has_error(self, *a, **k):
            return False
    with pytest.raises(RuntimeError):
        loadlock_error_channel(_Boom())


# ───────────────────────── 수정 2: 팝업 종료 처리 ─────────────────────────
def test_i_popup_close_clears_error_regardless_of_result():
    """X/ESC 로 닫아도 해제된다 — 결과 코드 분기가 사라졌는지 소스로 확인."""
    pc = open(os.path.join(_ROOT, "runtime", "plasma_cleaning_runtime.py"), encoding="utf-8").read()
    i = pc.index("def _on_closed(result: int) -> None:")
    blk = pc[i:i + 700]
    assert "if clear_status_to_idle:" in blk
    assert "QMessageBox.Ok" not in blk, "종료 결과 코드 조건이 남아 있다"
    assert 'runtime_state.clear_error("pc", _ch)' in blk
    assert "self._msg_boxes.remove(box)" in blk and "except ValueError" in blk

    cr = open(os.path.join(_ROOT, "runtime", "chamber_runtime.py"), encoding="utf-8").read()
    i = cr.index("def _ack_to_idle(_res: int):")
    blk = cr[i:i + 400]
    assert "QMessageBox.Ok" not in blk, "종료 결과 코드 조건이 남아 있다"
    assert 'runtime_state.clear_error("chamber", self.ch)' in blk
    # clear_status_to_idle=False 면 연결 자체를 하지 않는다(아무것도 해제 안 됨)
    assert re.search(r"if clear_status_to_idle:\s*\n\s*def _ack_to_idle", cr)


def test_j_on_closed_semantics_executed():
    """_on_closed 와 동일한 로직을 결과 코드별로 실행해 해제 여부를 확인."""
    def _on_closed(result, clear_status_to_idle, ch):
        if clear_status_to_idle:
            runtime_state.clear_error("pc", ch)

    for result in (0, 1024, -1):                  # X, Ok, ESC 등 어떤 코드든
        runtime_state.set_error("pc", 1, "fail")
        _on_closed(result, True, 1)
        assert runtime_state.has_error("pc", 1) is False, result
    # clear_status_to_idle=False 면 그대로 남는다
    runtime_state.set_error("pc", 1, "fail")
    _on_closed(1024, False, 1)
    assert runtime_state.has_error("pc", 1) is True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

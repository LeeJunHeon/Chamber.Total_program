# -*- coding: utf-8 -*-
"""플라즈마 클리닝 시작 거절이 '진행 중인 런'의 결과 플래그를 지우지 않는다.

호스트 START_PLASMA_CLEANING 은 start_with_recipe_string → create_task(_on_click_start) 로
실행 중인지 보지 않고 들어온다. 초기화가 거절 검사보다 먼저 있으면 요청은 거절되면서도
진행 중인 런의 _rf_off_unconfirmed_reason / _forced_fail / _cleanup_started 가 지워졌다.
"""
import os
import sys
import asyncio
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                        # noqa: E402
import runtime.plasma_cleaning_runtime as PCR                        # noqa: E402
from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime    # noqa: E402

# 진행 중인 런이 남겨 둔 상태(지워지면 안 되는 값들)
LIVE = {
    "_cleanup_started": True,
    "_forced_fail": True,
    "_forced_fail_reason": "RF OFF 미확인",
    "_rf_off_unconfirmed_reason": "강제 0W 쓰기 실패 (10.0초, 21회)",
}


class _RS:
    """runtime_state 스텁."""

    def __init__(self, *, cooldown_ok=True, remain=0.0, pc_running=False, chamber_running=False):
        self.cooldown_ok = cooldown_ok
        self.remain = remain
        self.pc_running = pc_running
        self.chamber_running = chamber_running
        self.started = []

    def pc_block_reason(self, ch, cooldown_s=60.0):
        return (self.cooldown_ok, self.remain, "")

    def is_running(self, kind, ch=None):
        return self.pc_running if kind == "pc" else self.chamber_running

    def mark_started(self, kind, ch=None):
        self.started.append((kind, ch))


def _mk(*, running=False, rs=None):
    p = PlasmaCleaningRuntime.__new__(PlasmaCleaningRuntime)
    p._selected_ch = 1
    p._running = bool(running)
    p._loaded_recipe_row = {}                # TEST MODE 아님
    p.warns = []
    p.host_reports = []
    p._post_warning = lambda title, text, **k: p.warns.append((title, text))
    p._host_report_start = lambda ok, reason="": p.host_reports.append((bool(ok), reason))
    for k, v in LIVE.items():
        setattr(p, k, v)
    return p


def _snapshot(p):
    return {k: getattr(p, k, "<missing>") for k in LIVE}


def _run(p, rs, monkeypatch):
    monkeypatch.setattr(PCR, "runtime_state", rs)
    asyncio.run(p._on_click_start())


# ───────────────────────── (a)(b) 거절 4종 ─────────────────────────
@pytest.mark.parametrize("name,kw,rs_kw,expect_msg", [
    ("이미 실행 중",        dict(running=True),  {},                          "이미 Plasma Cleaning이 실행 중"),
    ("쿨다운",              dict(),              dict(cooldown_ok=False, remain=12.0), "1분 대기 필요"),
    ("같은 CH PC 실행 중",  dict(),              dict(pc_running=True),       "Plasma Cleaning이 이미 실행 중"),
    ("같은 CH 챔버 실행 중", dict(),             dict(chamber_running=True),  "다른 공정이 실행 중"),
])
def test_ab_rejected_start_keeps_live_flags(monkeypatch, name, kw, rs_kw, expect_msg):
    rs = _RS(**rs_kw)
    p = _mk(rs=rs, **kw)
    _run(p, rs, monkeypatch)

    # 거절됐다 (host 에 False 보고, mark_started 없음)
    assert p.host_reports and p.host_reports[-1][0] is False, (name, p.host_reports)
    assert expect_msg in p.host_reports[-1][1], (name, p.host_reports[-1][1])
    assert rs.started == [], (name, rs.started)
    # 진행 중인 런의 플래그가 그대로 남아 있다
    assert _snapshot(p) == LIVE, (name, _snapshot(p))
    print(f"  ({name}) 거절 + 플래그 보존 OK")


# ───────────────────────── (c) 수락되면 초기화 ─────────────────────────
def test_c_accepted_start_resets_flags_before_mark_started(monkeypatch):
    """거절 조건이 없으면 mark_started 시점에 네 값이 이미 초기화돼 있다."""
    class _Stop(BaseException):
        """이후 진행을 멈추기 위한 신호(Exception 이 아니어서 suppress 에 걸리지 않는다)."""

    seen = {}

    class _RSCapture(_RS):
        def mark_started(self, kind, ch=None):
            seen.update(_snapshot(p))
            raise _Stop()

    rs = _RSCapture()
    p = _mk(rs=rs)
    monkeypatch.setattr(PCR, "runtime_state", rs)
    with pytest.raises(_Stop):
        asyncio.run(p._on_click_start())

    assert seen == {
        "_cleanup_started": False,
        "_forced_fail": False,
        "_forced_fail_reason": None,
        "_rf_off_unconfirmed_reason": None,
    }, seen
    print("  (c) 수락 시 mark_started 앞에서 초기화됨 OK")


# ───────────────────────── (c2) 소스 순서 ─────────────────────────
def test_c2_source_order_init_after_last_reject():
    src = open(os.path.join(_ROOT, "runtime", "plasma_cleaning_runtime.py"), encoding="utf-8").read()
    i_def = src.index("async def _on_click_start(")
    i_init = src.index("self._cleanup_started = False", i_def)
    i_mark = src.index('runtime_state.mark_started("pc", ch)', i_def)
    i_last_reject = src.index('is_running("chamber", ch)', i_def)
    i_running_chk = src.index('if getattr(self, "_running", False):', i_def)
    # 초기화가 마지막 거절 분기 뒤, mark_started 앞
    assert i_last_reject < i_init < i_mark, (i_last_reject, i_init, i_mark)
    # '이미 실행 중' 검사보다도 뒤
    assert i_running_chk < i_init
    # 초기화 줄은 _on_click_start 안에 1곳만(__init__ 은 타입 주석이 붙은 다른 형태)
    assert src.count("self._rf_off_unconfirmed_reason = None") == 1
    assert src.count("self._rf_off_unconfirmed_reason: Optional[str] = None") == 1
    print("  (c2) 소스 순서: 마지막 거절 → 초기화 → mark_started OK")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-s"]))

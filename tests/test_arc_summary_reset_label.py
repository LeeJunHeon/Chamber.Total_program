# -*- coding: utf-8 -*-
"""런 요약 '[arc] 런 요약 … 0x8C 동작=' 표시가 dc_pulse 의 3값 판정(예/아니오/확인불가)과 맞는지 검증.

chamber_runtime 의 런 요약 블록을 원문 그대로 실행한다(tests/test_log_gates.py 와 같은 방식).
(a) reset_state="unknown" → 확인불가   (b) "no" → 아니오   (c) "yes" → 예
(d) reset_state 없음(옛 arc_summary) → reset_ok 로 예/아니오 (하위 호환)
엑셀 기록 값(soft/hard_arc_count)은 그대로인지도 함께 본다. 장치·네트워크 접근 없음.
"""
import os
import sys
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                  # noqa: E402

_BASE = {"run_total": 4, "run_soft": 3, "run_hard": 1, "ign_total": 2, "ign_soft": 2,
         "ign_hard": 0, "depo_total": 2, "depo_soft": 1, "depo_hard": 1,
         "final_read": True, "ran": True}


def _run_summary_block(summary: dict):
    """chamber_runtime 의 런 요약 블록을 원문 그대로 실행."""
    from runtime.chamber_runtime import ChamberRuntime
    src = open(os.path.join(_ROOT, ChamberRuntime.__module__.replace(".", "/") + ".py"),
               encoding="utf-8").read()
    i0 = src.index("if self.dc_pulse is not None:\n                                    s, h = self.dc_pulse.arc_counts")
    i1 = src.index("_pname = str(", i0)
    lines = src[i0:i1].splitlines()
    ind = len(lines[0]) - len(lines[0].lstrip())
    code = "\n".join(l[ind:] if len(l) >= ind else l for l in lines)

    class _DL:
        process_params = {}

    class _DP:
        arc_counts = (3, 1)
        arc_summary = summary

    class _RT:
        ch = 2
        dc_pulse = _DP()
        data_logger = _DL()
        logs = []

        def append_log(self, src, msg):
            self.logs.append((src, msg))
    rt = _RT()
    rt.logs = []
    exec(compile(code, "<block>", "exec"), {"contextlib": contextlib, "self": rt})
    return rt


@pytest.mark.parametrize("extra,label", [
    ({"reset_state": "unknown", "reset_ok": False}, "확인불가"),     # (a)
    ({"reset_state": "no", "reset_ok": False}, "아니오"),            # (b)
    ({"reset_state": "yes", "reset_ok": True}, "예"),                # (c)
    ({"reset_ok": True}, "예"),                                      # (d) 옛 요약 — reset_state 없음
    ({"reset_ok": False}, "아니오"),                                 # (d)
    ({"reset_state": "???", "reset_ok": True}, "예"),                # 모르는 값 → reset_ok 로
])
def test_reset_label(extra, label):
    s = dict(_BASE)
    s.update(extra)
    rt = _run_summary_block(s)
    assert len(rt.logs) == 1, rt.logs
    msg = rt.logs[0][1]
    assert msg.startswith("[arc] 런 요약: 합계=4 (soft=3 hard=1) | 점화=2 (soft=2 hard=0) | 증착=2 (soft=1 hard=1) | "), msg
    assert f"0x8C 동작={label}, 종료값 확정=예" in msg, msg
    assert rt.data_logger.process_params["soft_arc_count"] == 3     # 엑셀 기록 값 불변
    assert rt.data_logger.process_params["hard_arc_count"] == 1


def test_not_logged_when_not_ran():
    s = dict(_BASE, ran=False, reset_state="unknown", reset_ok=False)
    rt = _run_summary_block(s)
    assert rt.logs == []


def test_labels_match_dc_pulse():
    """런타임의 표시 문구가 dc_pulse 의 _ARC_RESET_LABEL 과 같다."""
    from device.dc_pulse import _ARC_RESET_LABEL
    for st, label in _ARC_RESET_LABEL.items():
        rt = _run_summary_block(dict(_BASE, reset_state=st, reset_ok=(st == "yes")))
        assert f"0x8C 동작={label}," in rt.logs[0][1]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

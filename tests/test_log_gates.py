# -*- coding: utf-8 -*-
"""로그 출력 게이트 3건 검증 (장비 접근 없음).

1) arc_summary["ran"]: output_on 전(=_arc_run_start_ts 0) False, 설정 후 True
2) "[arc] 런 요약" 은 ran=False 런에서 안 찍히고 ran=True 에서 찍힌다 (엑셀 soft/hard 기록은 둘 다 유지)
3) _raw_log_on: [POLL …] 태그만 False, 그 외/None/"" True, RFPULSE_RAW_LOG=True 면 전부 True
4) _overcurr_n 초기화가 output_on() 호출 '앞' 에 있다 (활성화 감시 카운트 유실 방지)
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

import pytest                                   # noqa: E402
import device.dc_pulse as DCP                   # noqa: E402
from device.dc_pulse import AsyncDCPulse        # noqa: E402
from device.rf_pulse import RFPulseAsync        # noqa: E402


def _mk_dcp():
    d = AsyncDCPulse.__new__(AsyncDCPulse)
    d._cfg = {}
    d._cfg_float = lambda k, v: float(v)
    d._soft_arc_total = 0
    d._hard_arc_total = 0
    d._arc_baseline_soft = 0
    d._arc_baseline_hard = 0
    d._arc_ign_soft = None
    d._arc_ign_hard = None
    d._arc_final_soft = None
    d._arc_final_hard = None
    d._arc_reset_ok = False
    d._arc_run_start_ts = 0.0
    return d


def test_1_arc_summary_ran_flag():
    d = _mk_dcp()
    assert d.arc_summary["ran"] is False
    import time
    d._arc_run_start_ts = time.monotonic()
    assert d.arc_summary["ran"] is True
    # reset_arc_counts 가 0 으로 되돌린다
    d.reset_arc_counts()
    assert d._arc_run_start_ts == 0.0 and d.arc_summary["ran"] is False


def _run_summary_block(ran: bool):
    """chamber_runtime 의 해당 블록을 원문 그대로 실행 (RF Pulse 전용 런 vs DC Pulse 런)."""
    from runtime.chamber_runtime import ChamberRuntime
    src = open(ChamberRuntime.__module__.replace(".", "/") + ".py", encoding="utf-8").read()
    i0 = src.index("if self.dc_pulse is not None:\n                                    s, h = self.dc_pulse.arc_counts")
    i1 = src.index("_pname = str(", i0)
    block = src[i0:i1]
    # 들여쓰기 정규화
    lines = block.splitlines()
    ind = len(lines[0]) - len(lines[0].lstrip())
    code = "\n".join(l[ind:] if len(l) >= ind else l for l in lines)

    class _DL:
        process_params = {}

    class _DP:
        arc_counts = (3, 1)
        arc_summary = {"run_total": 4, "run_soft": 3, "run_hard": 1, "ign_total": 2, "ign_soft": 2,
                       "ign_hard": 0, "depo_total": 2, "depo_soft": 1, "depo_hard": 1,
                       "reset_ok": True, "final_read": True, "ran": ran}

    class _RT:
        ch = 1
        dc_pulse = _DP()
        data_logger = _DL()
        logs = []

        def append_log(self, src, msg):
            self.logs.append((src, msg))
    rt = _RT()
    exec(compile(code, "<block>", "exec"), {"contextlib": contextlib, "self": rt})
    return rt


def test_2_arc_summary_log_only_when_ran():
    rt = _run_summary_block(False)
    assert rt.logs == []
    assert rt.data_logger.process_params["soft_arc_count"] == 3     # 엑셀 기록은 유지
    assert rt.data_logger.process_params["hard_arc_count"] == 1
    rt = _run_summary_block(True)
    assert len(rt.logs) == 1 and rt.logs[0][1].startswith("[arc] 런 요약: 합계=4")
    assert "0x8C 동작=예, 종료값 확정=예" in rt.logs[0][1]


def _mk_rfp(raw_log: bool):
    r = RFPulseAsync.__new__(RFPulseAsync)
    r._rfp_raw_log = raw_log
    r._inflight = None
    return r


def test_3_raw_log_gate():
    r = _mk_rfp(False)
    for t in ("[POLL WAKE]", "[POLL FWD]", "[POLL REF]", "[POLL"):
        assert r._raw_log_on(t) is False, t
    for t in ("[START SETP 95W]", "[READ PULSE FREQ]", None, "", "[RF_ON]"):
        assert r._raw_log_on(t) is True, t
    r2 = _mk_rfp(True)
    for t in ("[POLL WAKE]", "[POLL FWD]", "[START SETP 95W]", None, ""):
        assert r2._raw_log_on(t) is True, t
    # 수신 측: in-flight 태그로 판정, 없으면 남긴다
    assert r._raw_log_on_inflight() is True

    class _Cmd:
        tag = "[POLL FWD]"
    r._inflight = _Cmd()
    assert r._raw_log_on_inflight() is False
    _Cmd.tag = "[SET_SETPOINT]"
    assert r._raw_log_on_inflight() is True


def test_3b_raw_log_sites_gated_and_exceptions_not():
    src = open(DCP.__file__.replace("dc_pulse.py", "rf_pulse.py"), encoding="utf-8").read()
    # TX / ACK / FRAME 은 게이트 안쪽
    i = src.index('"[RFP][RAW][TX] addr=')
    assert "if self._raw_log_on(cmd.tag):" in src[i - 300:i]
    i = src.index('"[RFP][RAW][RX] ACK(0x06)"')
    assert "if self._raw_log_on_inflight():" in src[i - 200:i]
    i = src.index('"[RFP][RAW][RX] FRAME addr=')
    assert "if self._raw_log_on_inflight():" in src[i - 400:i]
    # NAK / cs_bad 는 게이트 없음(항상 남김)
    i = src.index('"[RFP][RAW][RX] NAK(0x15)"')
    assert "_raw_log_on" not in src[i - 150:i]
    i = src.index('"[RFP][RAW][RX] FRAME(cs_bad)')
    assert "_raw_log_on" not in src[i - 200:i]


def test_4_overcurr_reset_before_output_on():
    src = open(DCP.__file__, encoding="utf-8").read()
    i_def = src.index("async def _prepare_and_start_impl")
    i_on = src.index("ok2 = await self.output_on()", i_def)
    i_rst = src.index("self._overcurr_n = 0", i_def)
    assert i_rst < i_on, "_overcurr_n 초기화는 output_on() 호출 앞이어야 한다"
    i_ret = src.index("return True", i_on)
    assert "self._overcurr_n = 0" not in src[i_on:i_ret], "감시 뒤에서 다시 0 으로 밀면 안 된다"
    # _arc_run_start_ts / _piv_scale_checked 는 여전히 output_on 뒤
    assert src.index("self._arc_run_start_ts = time.monotonic()", i_def) > i_on
    assert src.index("self._piv_scale_checked = False", i_def) > i_on


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

# -*- coding: utf-8 -*-
"""ProcessController 두께 모니터(2단계) — Time 모드 스텝·ETA 가 변경 전 코드와 같고(골든), Thickness 모드는 메인 공정 DELAY 대신
THICKNESS_WAIT 가 들어가며(중간 펄스 변경 충돌 거부), THICKNESS_WAIT 의 상한·시간 대체 타이머가 리스너 없이도 끝난다.

골든 tests/data/thickness_time_mode_golden.json 은 변경 전 process_controller.py(커밋 6cd7d1a)로 만든다:
    git show 6cd7d1a:controller/process_controller.py > <임시>/pc_before.py
    python tests/test_thickness_process_steps.py --write-golden <임시>/pc_before.py
"""
import asyncio
import contextlib
import importlib.util
import json
import os
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "tests"))
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

import test_process_heater_steps as hs                           # noqa: E402  (같은 _pc 도우미·설정 고정값)
from controller.process_controller import (ActionType, ProcessController, ProcessStep,  # noqa: E402
                                           eta_min_total_seconds, eta_row_min_seconds)
from errors.app_error import AppError                            # noqa: E402

GOLDEN = os.path.join(_ROOT, "tests", "data", "thickness_time_mode_golden.json")

# Time 모드(thickness 가 있어도 thickness_mode 키 없음) — 런타임 정규화처럼 process_time 은 역산값이 들어 있다
TIME_CASES = {
    "ch2_thk_dcp": (2, dict(base_pressure=7e-6, working_pressure=5, process_time=100 / 0.05 / 60, shutter_delay=5,
                            thickness=100.0, dep_rate=0.05, use_ar=True, ar_flow=20, use_dc_pulse=True,
                            dc_pulse_power=80, dc_pulse_freq=20, dc_pulse_off_time=10.0, use_ms=True, use_g1=True)),
    "ch2_thk_change_main": (2, dict(base_pressure=7e-6, working_pressure=60, process_time=20, shutter_delay=5,
                                    thickness=60.0, dep_rate=0.05, use_ar=True, ar_flow=50, use_dc_pulse=True,
                                    dc_pulse_power=80, dc_pulse_freq=20, dc_pulse_off_time=10.0, use_ms=True,
                                    use_g1=True, power_change_time="10m", change_power_value="100")),
    "ch1_thk_rfp": (1, dict(base_pressure=1e-5, working_pressure=3, process_time=150 / 0.042972222 / 60,
                            shutter_delay=2, thickness=150.0, dep_rate=0.042972222, use_ar=True, ar_flow=20,
                            use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True)),
    "ch2_thk_heater_rf": (2, dict(base_pressure=7e-6, working_pressure=2.0, process_time=10, shutter_delay=0,
                                  thickness=30.0, dep_rate=0.05, use_ar=True, ar_flow=20, use_rf_power=True,
                                  rf_power=150, use_ms=True, use_g2=True, use_heater=True, heater_temp=300.0)),
}
ETA_ROWS = [
    {"Process_name": "a", "shutter_delay": "5", "process_time": "10"},
    {"Process_name": "heat", "delay": "20m"},
    {"Process_name": "b", "shutter_delay": "1", "process_time": "", "thickness": "100", "dep_rate": "0.05"},
    {"Process_name": "c", "shutter_delay": "2", "process_time": "7", "thickness": "40", "dep_rate": "0.05"},
]


def _pc_of(cls, ch, **cb):
    if True:
        kw = dict(send_plc=hs._nop, send_mfc=hs._nop, send_dc_power=hs._nop, stop_dc_power=hs._nop,
                  send_rf_power=hs._nop, stop_rf_power=hs._nop, send_dc_power2=hs._nop, stop_dc_power2=hs._nop,
                  start_dc_pulse=hs._nop, stop_dc_pulse=hs._nop, set_dc_pulse_power=hs._nop, start_rf_pulse=hs._nop,
                  stop_rf_pulse=hs._nop, set_rf_pulse_power=hs._nop, ig_wait=hs._nop, cancel_ig=hs._nop,
                  rga_scan=hs._nop, oes_run=hs._nop, ch=ch, supports_dc_cont=(ch == 2), supports_rf_cont=(ch == 2),
                  supports_dc_pulse=True, supports_rf_pulse=True, supports_dc_cont2=(ch == 2))
        kw.update(cb)
        pc = cls(**kw)
    import types
    pc._cfg = types.SimpleNamespace(**hs._CFG_COMMON, **hs._CFG_CH[ch])
    pc._gas_info = dict(hs._CFG_COMMON["PROCESS_GAS_INFO"])
    return pc


def _snapshot(cls):
    async def go():
        out = {name: hs._rows(_pc_of(cls, ch)._create_process_sequence(dict(p))) for name, (ch, p) in TIME_CASES.items()}
        out["eta_rows"] = _pc_of(cls, 2).estimate_min_total_s(ETA_ROWS)
        return out
    return json.loads(json.dumps(asyncio.run(go()), ensure_ascii=False))


# ═══════════════════════ Time 모드 무변화 ═══════════════════════
def test_time_mode_steps_and_eta_equal_before_change():
    with open(GOLDEN, encoding="utf-8") as f:
        golden = json.load(f)
    assert _snapshot(ProcessController) == golden


def test_thickness_supported_but_time_rows_unchanged():
    """두께 모니터가 붙은 채널이라도 thickness 가 없는 행의 ETA 하한은 그대로, thickness 행은 메인 공정을 빼고 하한."""
    rows_time = ETA_ROWS[:2]
    pc = _pc_of(ProcessController, 2, thickness_supported=lambda: True)
    assert pc.estimate_min_total_s(rows_time) == _pc_of(ProcessController, 2).estimate_min_total_s(rows_time)
    assert eta_row_min_seconds(ETA_ROWS[2], True) == 60.0                       # shutter_delay 만
    assert eta_row_min_seconds(ETA_ROWS[2], False) == pytest.approx((1 + 100 / 0.05 / 60) * 60)
    full = pc.estimate_min_total_s(ETA_ROWS)
    assert full == round(eta_min_total_seconds(ETA_ROWS, 90.0, True)) == round((5 + 10) * 60 + 1200 + 60 + 120 + 90)
    assert full < _pc_of(ProcessController, 2).estimate_min_total_s(ETA_ROWS)   # 하한은 더 작아진다(빨리 끝날 수 있음)


# ═══════════════════════ Thickness 모드 스텝 ═══════════════════════
def _thk_params(**kw):
    p = dict(TIME_CASES["ch2_thk_dcp"][1], thickness_mode=True)
    p.update(kw)
    return p


def test_thickness_mode_replaces_main_delay():
    async def go():
        base = hs._rows(_pc_of(ProcessController, 2)._create_process_sequence(dict(TIME_CASES["ch2_thk_dcp"][1])))
        seq = _pc_of(ProcessController, 2)._create_process_sequence(_thk_params())
        rows = hs._rows(seq)
        names = [r[0] for r in rows]
        i = names.index("THICKNESS_WAIT")
        st = seq[i]
        assert names[i - 1] == "OES_RUN" and names[i - 2] == "PLC_CMD" and seq[i - 2].params[0] == "MS"
        assert st.value == 100.0 and st.polling and st.message.startswith("메인 공정 진행")
        assert st.params == (pytest.approx(2000.0), pytest.approx(4000.0))       # 예상 thickness/dep_rate, 상한 ×2
        assert seq[i - 1].params[0] == pytest.approx(2000.0) and seq[i - 1].no_wait   # OES 는 예상시간
        assert "DELAY" not in [r[0] for r in rows[i:]] or all(not r[1].startswith("메인 공정") for r in rows[i:] if r[0] == "DELAY")
        # 메인 DELAY 한 줄 → OES 문구·THICKNESS_WAIT 로 바뀐 것 말고는 같다
        j = [r[0] for r in base].index("OES_RUN")
        assert rows[:j] == base[:j] and rows[i + 1:] == base[j + 2:]
    asyncio.run(go())


def test_thickness_mode_rejects_mid_pulse_change_in_main_allows_in_shutter_delay():
    async def go():
        pc = _pc_of(ProcessController, 2)
        bad = _thk_params(power_change_time="6m", change_power_value="100")       # shutter_delay 5분 뒤 → 메인
        with pytest.raises(AppError) as ei:
            pc._create_process_sequence(bad)
        assert ei.value.code == "E844"
        assert pc.thickness_mid_change_conflict(bad)
        late = _thk_params(power_change_time="300m", change_power_value="100")    # 예상 끝 뒤라도 메인 구간
        assert pc.thickness_mid_change_conflict(late)
        ok = _thk_params(power_change_time="2m", change_power_value="100")        # Shutter Delay 안 → 허용
        assert pc.thickness_mid_change_conflict(ok) == ""
        seq = pc._create_process_sequence(ok)
        names = [s.action.name for s in seq]
        assert names.count("DC_PULSE_SET") == 1 and names.index("DC_PULSE_SET") < names.index("THICKNESS_WAIT")
        assert pc.thickness_mid_change_conflict(_thk_params(use_dc_pulse=False, use_rf_power=True, rf_power=100,
                                                            power_change_time="6m", change_power_value="100")) == ""
    asyncio.run(go())


def test_thickness_step_requires_params():
    with pytest.raises(ValueError):
        ProcessStep(action=ActionType.THICKNESS_WAIT, message="x", params=(1.0, 2.0))
    with pytest.raises(AppError):
        ProcessStep(action=ActionType.THICKNESS_WAIT, message="x", value=1.0)


# ═══════════════════════ THICKNESS_WAIT 타이머 ═══════════════════════
def _wait_case(on_start=None, limit=0.6, expected=0.3):
    async def go():
        ends = []
        box = {}

        def start(t, e, l):
            box["args"] = (t, e, l)
            if on_start:
                on_start(box["pc"], asyncio.get_running_loop())
        pc = _pc_of(ProcessController, 2, thickness_start=start, thickness_end=ends.append)
        box["pc"] = pc
        step = ProcessStep(action=ActionType.THICKNESS_WAIT, value=10.0, params=(expected, limit),
                           message="메인 공정 진행 (두께 10nm)", polling=True)
        pc.process_sequence = [step]
        pc._current_step_idx = 0
        pc.is_running = True
        t0 = time.monotonic()
        await asyncio.wait_for(pc._execute_step(step), timeout=10)
        return ends, time.monotonic() - t0, pc, box
    return asyncio.run(go())


def test_limit_timer_ends_without_listener():
    ends, el, pc, box = _wait_case()                       # 리스너가 아무것도 안 함(서비스 죽음)
    assert ends == ["상한"] and 0.55 <= el < 2.0
    assert box["args"] == (10.0, 0.3, 0.6)
    assert 500 <= pc._actual_process_time_ms <= 700        # 메인 공정 실제 시간 누적(DELAY 와 같게)
    assert not pc._thk_wait_active


def test_done_token_ends():
    ends, el, pc, _ = _wait_case(on_start=lambda pc, lp: lp.call_later(0.1, pc.on_thickness_done), limit=5)
    assert ends == ["Thickness"] and el < 1.0


def test_fallback_timer_ends_and_resume_cancels():
    ends, el, _pc, _ = _wait_case(on_start=lambda pc, lp: pc.on_thickness_fallback(0.2, "시험"), limit=5)
    assert ends == ["시간대체"] and 0.15 <= el < 1.0

    def fb_then_resume(pc, lp):
        pc.on_thickness_fallback(0.2, "시험")
        lp.call_later(0.1, pc.on_thickness_resume)
    ends, el, _pc, _ = _wait_case(on_start=fb_then_resume, limit=0.6)
    assert ends == ["상한"] and el >= 0.55


def test_start_callback_failure_uses_expected_time():
    def boom(pc, lp):
        raise RuntimeError("x")
    ends, el, _pc, _ = _wait_case(on_start=boom, limit=5, expected=0.2)
    assert ends == ["시간대체"] and el < 1.0


def test_abort_ends_wait():
    ends, el, _pc, _ = _wait_case(on_start=lambda pc, lp: lp.call_later(0.1, pc._abort_evt.set), limit=5)
    assert ends == ["중단"] and el < 1.0


def test_eta_not_confirmed_in_thickness_mode():
    async def go():
        pc = _pc_of(ProcessController, 2)
        pc.process_sequence = pc._create_process_sequence(_thk_params())
        sd = [s for s in pc.process_sequence if s.message.startswith("Shutter Delay")][0]
        pc._current_step_idx = pc.process_sequence.index(sd)
        pc.is_running = True
        pc._try_confirm_eta(sd)
        assert pc._eta_end_ns == 0 and pc.eta_remaining_s() is None
        tm = _pc_of(ProcessController, 2)                   # Time 모드는 예전처럼 확정
        tm.process_sequence = tm._create_process_sequence(dict(TIME_CASES["ch2_thk_dcp"][1]))
        sd = [s for s in tm.process_sequence if s.message.startswith("Shutter Delay")][0]
        tm._current_step_idx = tm.process_sequence.index(sd)
        tm.is_running = True
        tm._try_confirm_eta(sd)
        assert tm.eta_remaining_s() == round((300 + 2000 + 120))
    asyncio.run(go())


# ═══════════════════════ 실제 러너 흐름 ═══════════════════════
def _flow(mode):
    """가짜 장비로 Thickness 모드 공정 전체를 돌린다. mode: done / limit / fallback / stop."""
    async def go():
        calls = []
        box = {}
        loop = asyncio.get_running_loop()

        def later(fn, *a):
            loop.call_later(0.01, lambda: fn(*a))

        def plc(name, on, ch=None):
            calls.append(("plc", name, on))
            later(box["pc"].on_plc_confirmed, name)

        def mfc(cmd, args):
            later(box["pc"].on_mfc_confirmed, cmd)

        def dcp(power, f, off):
            later(box["pc"].on_dc_pulse_target_reached)

        def dcp_stop():
            calls.append(("dcp_off",))
            later(box["pc"].on_dc_pulse_off_finished)

        def tstart(t, e, l):
            calls.append(("thk_start", round(e, 3), round(l, 3)))
            if mode == "done":
                loop.call_later(0.05, box["pc"].on_thickness_done)
            elif mode == "fallback":
                box["pc"].on_thickness_fallback(0.05, "시험")
            elif mode == "stop":
                loop.call_later(0.05, box["pc"].request_stop)

        pc = _pc_of(ProcessController, 2, send_plc=plc, send_mfc=mfc, start_dc_pulse=dcp, stop_dc_pulse=dcp_stop,
                    ig_wait=lambda bp: later(box["pc"].on_ig_ok), rga_scan=lambda: later(box["pc"].on_rga_finished),
                    oes_run=lambda d, i: calls.append(("oes", round(d, 3))),
                    thickness_start=tstart, thickness_end=lambda k: calls.append(("thk_end", k)),
                    cancel_thickness=lambda: calls.append(("thk_cancel",)))
        pc._cfg.THICKNESS_MAX_TIME_RATIO = 1.0
        pc._cfg.THICKNESS_MAX_TIME_MARGIN_S = 0.0
        box["pc"] = pc
        import controller.process_controller as m
        gap = m.SHUTDOWN_STEP_GAP_MS
        m.SHUTDOWN_STEP_GAP_MS = 0
        try:
            pc.start_process(dict(base_pressure=7e-6, working_pressure=60, process_time=0.003, shutter_delay=0.002,
                                  use_ar=True, ar_flow=50, use_dc_pulse=True, dc_pulse_power=80, use_ms=True,
                                  use_g1=True, thickness=0.3, dep_rate=1.0, thickness_mode=True))
            fin = None
            while fin is None:
                ev = await asyncio.wait_for(pc.event_q.get(), timeout=30)
                if ev.kind == "finished":
                    fin = ev.payload
        finally:
            m.SHUTDOWN_STEP_GAP_MS = gap
        return fin, calls
    return asyncio.run(go())


@pytest.mark.parametrize("mode,end", [("done", "Thickness"), ("limit", "상한"), ("fallback", "시간대체")])
def test_flow_ends_normally(mode, end):
    fin, calls = _flow(mode)
    assert fin["ok"] and fin["detail"]["result"] == "SUCCESS"            # 상한·시간 대체도 실패가 아니다
    assert ("thk_start", 0.3, 0.3) in calls and ("oes", 0.3) in calls
    assert ("thk_end", end) in calls
    i = calls.index(("thk_end", end))
    ms_close = [k for k, c in enumerate(calls) if c[:3] == ("plc", "MS", False)]
    assert ms_close and ms_close[0] > i and calls.index(("dcp_off",)) > ms_close[0]   # 셔터 닫기 → 전원 OFF
    assert fin["detail"]["actual_process_time_min"] > 0


def test_flow_stop_uses_existing_path():
    fin, calls = _flow("stop")
    assert not fin["ok"] and fin["detail"]["result"] == "STOP"
    assert ("thk_end", "중단") in calls and ("thk_cancel",) in calls and ("dcp_off",) in calls


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--write-golden":
        spec = importlib.util.spec_from_file_location("pc_before", sys.argv[2])
        mod = importlib.util.module_from_spec(spec)
        sys.modules["pc_before"] = mod               # @dataclass 가 모듈을 찾는다
        spec.loader.exec_module(mod)
        with open(GOLDEN, "w", encoding="utf-8") as f:
            json.dump(_snapshot(mod.ProcessController), f, ensure_ascii=False, indent=1)
        print("written", GOLDEN)
        sys.exit(0)
    sys.exit(pytest.main([__file__, "-q"]))

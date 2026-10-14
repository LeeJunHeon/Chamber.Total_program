# -*- coding: utf-8 -*-
"""ProcessController 히터 단계(2단계) — 히터를 안 쓰는 레시피는 시퀀스·ETA 가 수정 전과 같고(골든),
use_heater=T 면 '승온 먼저' + 'SP1 뒤 유지 측정' 만 더해진다. 토큰 흐름·실패·정지 경로.

골든 파일 tests/data/process_sequences_golden.json 은 수정 전 코드로 만든다:
    python tests/test_process_heater_steps.py --write-golden
"""
import os
import sys
import json
import types
import asyncio
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from controller.process_controller import ProcessController   # noqa: E402

GOLDEN = os.path.join(_ROOT, "tests", "data", "process_sequences_golden.json")

# 시퀀스가 읽는 설정을 코드 기본값으로 고정한다 — settings.json(배포값)·다른 테스트의 load_settings() 와 무관하게.
_CFG_COMMON = dict(
    PROCESS_GAS_INFO={"AR": {"channel": 1}, "O2": {"channel": 2}, "N2": {"channel": 3}},
    PROCESS_DEFAULT_BASE_PRESSURE=1e-5, PROCESS_DEFAULT_OES_INTEGRATION_MS=60,
    PC_PRESSURE_WAIT_TIMEOUT_S=180.0, PC_RF_PULSE_POST_ON_DELAY_MS=20_000,
    PC_WORKING_PRESSURE_BOOST_TARGET=10.0, PC_POWER_SELECT_SETTLE_MS=7_000,
    ETA_ENABLED=True, ETA_TAIL_MIN_S=90.0, ETA_TAIL_S=120.0,
)
_CFG_CH = {
    1: dict(PROCESS_GUN_SHUTTERS=[], PROCESS_PRESSURE_CONTROL_SP_ON_CMD="SP3_ON",
            PROCESS_PRESSURE_CONTROL_SP_LABEL="SP3", PROCESS_PRESSURE_CONTROL_SP_INDEX=3),
    2: dict(PROCESS_GUN_SHUTTERS=["G1", "G2", "G3"], PROCESS_PRESSURE_CONTROL_SP_ON_CMD="SP4_ON",
            PROCESS_PRESSURE_CONTROL_SP_LABEL="SP4", PROCESS_PRESSURE_CONTROL_SP_INDEX=4),
}

CASES = {
    "ch2_dc_rf_boost": (2, dict(base_pressure=7e-6, working_pressure=2.0, process_time=10, shutter_delay=5,
                                use_ar=True, ar_flow=20, use_dc_power=True, dc_power=100, use_rf_power=True,
                                rf_power=150, use_ms=True, use_g1=True, use_g2=True)),
    "ch2_dcp_change": (2, dict(base_pressure=7e-6, working_pressure=60, process_time=20, shutter_delay=5,
                               use_ar=True, ar_flow=50, use_dc_pulse=True, dc_pulse_power=80, dc_pulse_freq=20,
                               dc_pulse_off_time=10.0, use_ms=True, use_g1=True,
                               power_change_time="10m", change_power_value="100")),
    "ch2_rfp_ps": (2, dict(base_pressure=1e-5, working_pressure=5, process_time=3, shutter_delay=0,
                           use_ar=True, use_o2=True, ar_flow=20, o2_flow=2, use_rf_pulse=True, rf_pulse_power=100,
                           rf_pulse_freq=1.0, rf_pulse_duty=50, use_power_select=True, use_g3=True)),
    "ch1_rfp": (1, dict(base_pressure=1e-5, working_pressure=3, process_time=5, shutter_delay=2,
                        use_ar=True, use_o2=True, ar_flow=20, o2_flow=1, use_rf_pulse=True, rf_pulse_power=120,
                        rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True)),
    "ch1_dcp_change_sd": (1, dict(base_pressure=1e-5, working_pressure=12, process_time=5, shutter_delay=3,
                                  use_ar=True, ar_flow=30, use_dc_pulse=True, dc_pulse_power=60, dc_pulse_freq=50,
                                  dc_pulse_off_time="DC", use_ms=True, power_change_time="1m",
                                  change_power_value="90")),
    "ch2_test": (2, dict(test_mode=True, test_duration_sec=30)),
}
ETA_ROWS = [
    {"Process_name": "a", "shutter_delay": "5", "process_time": "10"},
    {"Process_name": "heat", "delay": "20m"},
    {"Process_name": "b", "shutter_delay": "1", "process_time": "", "thickness": "100", "dep_rate": "0.05"},
]


def _nop(*_a, **_k):
    return None


def _pc(ch, **cb):
    kw = dict(send_plc=_nop, send_mfc=_nop, send_dc_power=_nop, stop_dc_power=_nop, send_rf_power=_nop,
              stop_rf_power=_nop, send_dc_power2=_nop, stop_dc_power2=_nop, start_dc_pulse=_nop, stop_dc_pulse=_nop,
              set_dc_pulse_power=_nop, start_rf_pulse=_nop, stop_rf_pulse=_nop, set_rf_pulse_power=_nop,
              ig_wait=_nop, cancel_ig=_nop, rga_scan=_nop, oes_run=_nop, ch=ch, supports_dc_cont=(ch == 2),
              supports_rf_cont=(ch == 2), supports_dc_pulse=True, supports_rf_pulse=True, supports_dc_cont2=(ch == 2))
    kw.update(cb)
    pc = ProcessController(**kw)
    pc._cfg = types.SimpleNamespace(**_CFG_COMMON, **_CFG_CH[ch])
    pc._gas_info = dict(_CFG_COMMON["PROCESS_GAS_INFO"])
    return pc


def _rows(seq):
    return [[s.action.name, s.message, s.value, repr(s.params), s.duration, s.parallel, s.polling, s.no_wait]
            for s in seq]


def _snapshot():
    async def go():
        out = {name: _rows(_pc(ch)._create_process_sequence(dict(p))) for name, (ch, p) in CASES.items()}
        out["eta_rows"] = _pc(2).estimate_min_total_s(ETA_ROWS)
        return out
    return asyncio.run(go())


def test_golden_sequences_unchanged_without_heater():
    with open(GOLDEN, encoding="utf-8") as f:
        golden = json.load(f)
    now = json.loads(json.dumps(_snapshot(), ensure_ascii=False))
    assert now == golden


def test_use_heater_false_is_identical():
    async def go():
        for name, (ch, p) in CASES.items():
            a = _rows(_pc(ch)._create_process_sequence(dict(p)))
            b = _rows(_pc(ch)._create_process_sequence(dict(p, use_heater=False, heater_temp=200.0)))
            assert a == b, name
    asyncio.run(go())


def test_heater_steps_only_added_at_start_and_after_sp1():
    async def go():
        for name in ("ch2_dc_rf_boost", "ch2_dcp_change", "ch2_rfp_ps"):
            ch, p = CASES[name]
            base = _rows(_pc(ch)._create_process_sequence(dict(p)))
            seq = _pc(ch)._create_process_sequence(dict(p, use_heater=True, heater_temp=200.0, heater_ramp=6.0))
            rows = _rows(seq)
            names = [r[0] for r in rows]
            assert names[:3] == ["HEATER_SET", "HEATER_WAIT", "IG_CMD"]
            assert seq[0].value == 200.0 and seq[0].params == (6.0,) and seq[1].value == 200.0
            i = names.index("HEATER_HOLD")
            assert seq[i - 1].message.startswith("압력 제어(SP1) 시작")
            assert names.count("HEATER_HOLD") == 1 and not seq[i].polling
            assert [r for r in rows if not r[0].startswith("HEATER")] == base
            assert not any(s.message.startswith(ProcessController._ETA_CONFIRM_PREFIXES)
                           for s in seq if s.action.name.startswith("HEATER"))
        seq = _pc(2)._create_process_sequence(dict(CASES["ch2_test"][1], use_heater=True, heater_temp=200.0))
        assert [s.action.name for s in seq] == ["DELAY"]          # TEST MODE 는 장비 제어 없음
    asyncio.run(go())


def _flow(fail_at=None, stop_at=None, fault_in_main=False, emergency_at=None):
    """가짜 장비 콜백으로 실제 러너를 돌린다. (ok, error_code, result, 호출 기록)"""
    async def go():
        calls = []
        box = {}
        loop = asyncio.get_running_loop()

        def later(fn, *a, **k):
            loop.call_later(0.01, lambda: fn(*a, **k))

        def plc(name, on, ch=None):
            calls.append(("plc", name, on))
            later(box["pc"].on_plc_confirmed, name)

        def mfc(cmd, args):
            calls.append(("mfc", cmd))
            later(box["pc"].on_mfc_confirmed, cmd)

        def dcp(power, f, off):
            calls.append(("dcp_on",))
            later(box["pc"].on_dc_pulse_target_reached)
            if fault_in_main:
                loop.call_later(0.05, lambda: box["pc"].on_heater_failed("공정 중 히터 이상(FAULT)", code="E832"))

        def dcp_stop():
            calls.append(("dcp_off",))
            later(box["pc"].on_dc_pulse_off_finished)

        def hset(t, r):
            calls.append(("heater_set", t, r))
            if fail_at == "set":
                later(box["pc"].on_heater_failed, "히터 사용 불가 — 시험", code="E830")
            else:
                later(box["pc"].on_heater_set_ok)

        def hwait(t):
            calls.append(("heater_wait", t))
            if stop_at == "wait":
                later(box["pc"].request_stop)
            elif emergency_at == "wait":
                later(box["pc"].emergency_stop)
            elif fail_at == "wait":
                later(box["pc"].on_heater_failed, "히터 승온 시간 초과", code="E831")
            else:
                later(box["pc"].on_heater_ready)

        def hhold():
            calls.append(("heater_hold",))
            if fail_at == "hold":
                later(box["pc"].on_heater_failed, "유지 모드 진입 시간 초과", code="E833")
            else:
                later(box["pc"].on_heater_hold_ok)

        pc = _pc(2, send_plc=plc, send_mfc=mfc, start_dc_pulse=dcp, stop_dc_pulse=dcp_stop,
                 ig_wait=lambda bp: (calls.append(("ig",)), later(box["pc"].on_ig_ok)),
                 rga_scan=lambda: (calls.append(("rga",)), later(box["pc"].on_rga_finished)),
                 heater_set=hset, heater_wait=hwait, heater_hold=hhold,
                 cancel_heater=lambda: calls.append(("heater_cancel",)))
        box["pc"] = pc
        import controller.process_controller as m
        gap = m.SHUTDOWN_STEP_GAP_MS
        m.SHUTDOWN_STEP_GAP_MS = 0
        try:
            pc.start_process(dict(base_pressure=7e-6, working_pressure=60, process_time=0.005, shutter_delay=0.002,
                                  use_ar=True, ar_flow=50, use_dc_pulse=True, dc_pulse_power=80, use_ms=True,
                                  use_g1=True, use_heater=True, heater_temp=200.0, heater_ramp=None))
            fin = None
            while fin is None:
                ev = await asyncio.wait_for(pc.event_q.get(), timeout=30)
                if ev.kind == "finished":
                    fin = ev.payload
        finally:
            m.SHUTDOWN_STEP_GAP_MS = gap
        d = fin["detail"]
        return fin["ok"], d.get("error_code"), d.get("result"), [c[0] for c in calls]
    return asyncio.run(go())


def test_flow_normal_order():
    ok, code, res, seq = _flow()
    assert ok and res == "SUCCESS"
    assert seq[:3] == ["heater_set", "heater_wait", "ig"]
    i = seq.index("heater_hold")
    assert "dcp_on" in seq[:i]                       # 전원 ON 뒤에 유지 측정
    assert seq.count("heater_cancel") >= 1           # 종료 때 대기·감시 해제


def test_flow_set_failure_stops_before_ig():
    ok, code, res, seq = _flow(fail_at="set")
    assert not ok and code == "E830" and res == "FAIL"
    assert "ig" not in seq and "heater_wait" not in seq and "heater_cancel" in seq


def test_flow_wait_timeout():
    ok, code, res, seq = _flow(fail_at="wait")
    assert not ok and code == "E831" and "ig" not in seq


def test_flow_hold_abort():
    ok, code, res, seq = _flow(fail_at="hold")
    assert not ok and code == "E833" and "dcp_off" in seq


def test_flow_stop_during_wait_cancels():
    ok, code, res, seq = _flow(stop_at="wait")
    assert not ok and res == "STOP" and "ig" not in seq
    assert seq.index("heater_cancel") > seq.index("heater_wait")


def test_flow_emergency_during_wait_cancels():
    ok, code, res, seq = _flow(emergency_at="wait")
    assert not ok and "heater_cancel" in seq and "ig" not in seq


def test_flow_heater_fault_during_process():
    ok, code, res, seq = _flow(fault_in_main=True)
    assert not ok and code == "E832" and res == "FAIL"


if __name__ == "__main__":
    if "--write-golden" in sys.argv:
        os.makedirs(os.path.dirname(GOLDEN), exist_ok=True)
        with open(GOLDEN, "w", encoding="utf-8", newline="\n") as f:
            json.dump(_snapshot(), f, ensure_ascii=False, indent=1, sort_keys=True)
            f.write("\n")
        print(f"golden written: {GOLDEN}")

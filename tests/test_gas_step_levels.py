# -*- coding: utf-8 -*-
"""CH2 O2 단계 설정(PROCESS_GAS_STEP_LEVELS) — 공정 시퀀스의 가스 주입 단계.

목표보다 작은 중간값만 작은 순서대로 먼저 거친다(머무는 시간 없음, 각 단계는 FLOW_ON 3회 연속 도달 후 다음).
중간값이 없으면 단계 개수·문구가 예전과 글자 하나까지 같다. 설정이 잘못되면 고쳐 쓰지 않고 시작 거부(E211).
설정 파일(settings.json) 로드 결과도 확인한다. 장치 접근 없음.
"""
import os
import re
import sys
import types
import asyncio
import contextlib
from pathlib import Path

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

from controller.process_controller import ProcessController      # noqa: E402
from errors.app_error import AppError                            # noqa: E402
from lib import config_common, config_ch1, config_ch2            # noqa: E402
from lib import _config_loader as L                              # noqa: E402

REPO_JSON = Path(_ROOT) / "config" / "settings.json"

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
_UNSET = object()


@pytest.fixture(autouse=True)
def restore_cfg():
    """settings 로더가 바꾼 config 모듈 값을 원래대로 돌린다."""
    mods = (config_common, config_ch1, config_ch2)
    snap = [dict(vars(md)) for md in mods]
    yield
    for md, d in zip(mods, snap):
        for k in [k for k in vars(md) if k not in d]:
            delattr(md, k)
        for k, v in d.items():
            setattr(md, k, v)


def _nop(*_a, **_k):
    return None


def _pc(ch, levels=_UNSET, **cb):
    kw = dict(send_plc=_nop, send_mfc=_nop, send_dc_power=_nop, stop_dc_power=_nop, send_rf_power=_nop,
              stop_rf_power=_nop, send_dc_power2=_nop, stop_dc_power2=_nop, start_dc_pulse=_nop, stop_dc_pulse=_nop,
              set_dc_pulse_power=_nop, start_rf_pulse=_nop, stop_rf_pulse=_nop, set_rf_pulse_power=_nop,
              ig_wait=_nop, cancel_ig=_nop, rga_scan=_nop, oes_run=_nop, ch=ch, supports_dc_cont=(ch == 2),
              supports_rf_cont=(ch == 2), supports_dc_pulse=True, supports_rf_pulse=True, supports_dc_cont2=(ch == 2))
    kw.update(cb)
    pc = ProcessController(**kw)
    extra = {} if levels is _UNSET else {"PROCESS_GAS_STEP_LEVELS": levels}
    pc._cfg = types.SimpleNamespace(**_CFG_COMMON, **_CFG_CH[ch], **extra)
    pc._gas_info = dict(_CFG_COMMON["PROCESS_GAS_INFO"])
    return pc


def _recipe(**kw):
    p = dict(base_pressure=7e-6, working_pressure=5.0, process_time=10, shutter_delay=5,
             use_ar=True, ar_flow=20.0, use_dc_power=True, dc_power=100, use_ms=True, use_g1=True)
    p.update(kw)
    return p


def _seq(pc, params):
    async def go():
        return pc._create_process_sequence(dict(params))
    return asyncio.run(go())


def _rows(seq):
    return [[s.action.name, s.message, s.value, repr(s.params), s.duration, s.parallel, s.polling, s.no_wait]
            for s in seq]


def _gas_part(seq, ch):
    """메인 밸브 열기 다음부터 압력 제어 시작 전까지 중 채널 ch 의 FLOW_SET/FLOW_ON (메시지, 명령, 값)."""
    out = []
    started = False
    for s in seq:
        if s.message == "메인 밸브 열기":
            started = True
            continue
        if started and s.message.startswith("압력 제어("):
            break
        if started and s.action.name == "MFC_CMD" and s.params[0] in ("FLOW_SET", "FLOW_ON") \
                and s.params[1].get("channel") == ch:
            out.append((s.message, s.params[0], s.params[1].get("value")))
    return out


O2_LV = {"O2": [9.0, 9.5]}

TABLE = {
    8.0: [("Ch2(O2) GAS 8.0sccm 설정", "FLOW_SET", 8.0),
          ("Ch2(O2) GAS 공급 시작", "FLOW_ON", None)],
    9.0: [("Ch2(O2) GAS 9.0sccm 설정", "FLOW_SET", 9.0),
          ("Ch2(O2) GAS 공급 시작", "FLOW_ON", None)],
    9.2: [("Ch2(O2) GAS 9.0sccm 설정 (단계 1/2)", "FLOW_SET", 9.0),
          ("Ch2(O2) GAS 공급 시작 (9.0sccm 도달 대기)", "FLOW_ON", None),
          ("Ch2(O2) GAS 9.2sccm 설정 (단계 2/2)", "FLOW_SET", 9.2),
          ("Ch2(O2) GAS 9.2sccm 도달 대기", "FLOW_ON", None)],
    9.5: [("Ch2(O2) GAS 9.0sccm 설정 (단계 1/2)", "FLOW_SET", 9.0),
          ("Ch2(O2) GAS 공급 시작 (9.0sccm 도달 대기)", "FLOW_ON", None),
          ("Ch2(O2) GAS 9.5sccm 설정 (단계 2/2)", "FLOW_SET", 9.5),
          ("Ch2(O2) GAS 9.5sccm 도달 대기", "FLOW_ON", None)],
    10.0: [("Ch2(O2) GAS 9.0sccm 설정 (단계 1/3)", "FLOW_SET", 9.0),
           ("Ch2(O2) GAS 공급 시작 (9.0sccm 도달 대기)", "FLOW_ON", None),
           ("Ch2(O2) GAS 9.5sccm 설정 (단계 2/3)", "FLOW_SET", 9.5),
           ("Ch2(O2) GAS 9.5sccm 도달 대기", "FLOW_ON", None),
           ("Ch2(O2) GAS 10.0sccm 설정 (단계 3/3)", "FLOW_SET", 10.0),
           ("Ch2(O2) GAS 10.0sccm 도달 대기", "FLOW_ON", None)],
}


# ───────────────────────── 시퀀스: 표대로 ─────────────────────────
@pytest.mark.parametrize("o2", sorted(TABLE))
def test_ch2_o2_steps_follow_table(o2):
    seq = _seq(_pc(2, O2_LV), _recipe(use_o2=True, o2_flow=o2))
    assert _gas_part(seq, 2) == TABLE[o2]
    # O2 밸브 열기 바로 뒤에 단계들이 붙는다
    names = [s.message for s in seq]
    i = names.index("O2 밸브 열기")
    assert [s.message for s in seq[i + 1:i + 1 + len(TABLE[o2])]] == [t[0] for t in TABLE[o2]]


@pytest.mark.parametrize("o2", [8.0, 9.0])
def test_no_mid_levels_identical_to_before(o2):
    """중간값이 없으면 단계 개수·문구·값이 설정이 아예 없을 때(예전 코드)와 글자 하나까지 같다."""
    p = _recipe(use_o2=True, o2_flow=o2)
    assert _rows(_seq(_pc(2, O2_LV), p)) == _rows(_seq(_pc(2), p)) == _rows(_seq(_pc(2, {}), p))


def test_o2_unused_no_steps():
    p = _recipe()
    seq = _seq(_pc(2, O2_LV), p)
    assert _gas_part(seq, 2) == []
    assert _rows(seq) == _rows(_seq(_pc(2), p))
    assert not any("(단계 " in s.message or "sccm 도달 대기" in s.message for s in seq)


def test_ar_unchanged():
    p = _recipe(use_o2=True, o2_flow=10.0)
    assert _gas_part(_seq(_pc(2, O2_LV), p), 1) == [("Ch1(AR) GAS 20.0sccm 설정", "FLOW_SET", 20.0),
                                                     ("Ch1(AR) GAS 공급 시작", "FLOW_ON", None)]
    # O2 단계 외에는 전부 그대로
    a = [r for r in _rows(_seq(_pc(2, O2_LV), p)) if "Ch2(O2) GAS" not in r[1]]
    b = [r for r in _rows(_seq(_pc(2), p)) if "Ch2(O2) GAS" not in r[1]]
    assert a == b


def test_ch1_default_is_unchanged_even_for_o2_10():
    """CH1 은 config_ch1 의 기본값({}) → O2 10.0 이어도 그대로."""
    assert config_ch1.PROCESS_GAS_STEP_LEVELS == {}
    p = _recipe(use_o2=True, o2_flow=10.0, use_dc_power=False, use_g1=False, use_dc_pulse=True, dc_pulse_power=80)
    pc_real = _pc(1)
    pc_real._cfg = types.SimpleNamespace(**_CFG_COMMON, **_CFG_CH[1],
                                         PROCESS_GAS_STEP_LEVELS=config_ch1.PROCESS_GAS_STEP_LEVELS)
    seq = _seq(pc_real, p)
    assert _gas_part(seq, 2) == [("Ch2(O2) GAS 10.0sccm 설정", "FLOW_SET", 10.0),
                                 ("Ch2(O2) GAS 공급 시작", "FLOW_ON", None)]
    assert _rows(seq) == _rows(_seq(_pc(1), p))


def test_real_ch2_module_default():
    """실제 config_ch2 모듈 값(settings.json 이 없어도 이 값)으로 10.0 → 9.0 → 9.5 → 10.0."""
    assert config_ch2.PROCESS_GAS_STEP_LEVELS == {"O2": [9.0, 9.5]}
    pc = _pc(2, config_ch2.PROCESS_GAS_STEP_LEVELS)
    assert _gas_part(_seq(pc, _recipe(use_o2=True, o2_flow=10.0)), 2) == TABLE[10.0]


def test_int_levels_are_floats():
    seq = _seq(_pc(2, {"O2": [9, 9.5]}), _recipe(use_o2=True, o2_flow=10.0))
    assert _gas_part(seq, 2) == TABLE[10.0]


# ───────────────────────── 잘못된 설정 → 시작 거부(E211) ─────────────────────────
BAD = [
    {"O2": [9.5, 9.0]},           # 내림차순
    {"O2": [9.0, 9.0]},           # 중복
    {"O2": [0, 9.0]},             # 0
    {"O2": [-1.0]},               # 음수
    {"O2": [9.0, "9.5"]},         # 숫자 아님
    {"O2": [True]},               # bool
    {"O2": 9.0},                  # 목록 아님
    {"o2": [9.0]},                # 모르는 가스 이름(대소문자 다름)
    {"H2": [9.0]},                # 모르는 가스 이름
    [9.0, 9.5],                   # dict 아님
]


@pytest.mark.parametrize("bad", BAD, ids=[repr(b) for b in BAD])
def test_bad_levels_reject(bad):
    pc = _pc(2, bad)
    with pytest.raises(AppError) as ei:
        _seq(pc, _recipe(use_o2=True, o2_flow=10.0))
    assert ei.value.code == "E211" and "PROCESS_GAS_STEP_LEVELS" in ei.value.detail
    assert pc.gas_step_levels_error()                         # 런타임 사전 점검도 같은 판정
    # O2 를 안 쓰는 레시피도 막는다(설정 자체가 잘못됨)
    with pytest.raises(AppError):
        _seq(_pc(2, bad), _recipe())


def test_bad_levels_block_start_process():
    async def go():
        pc = _pc(2, {"O2": [9.5, 9.0]})
        logs = []
        pc._emit_log = lambda src, msg: logs.append(msg)
        pc.start_process(_recipe(use_o2=True, o2_flow=10.0))
        assert not pc.is_running and pc.process_sequence == []
        assert any("공정 시작 오류" in m and "PROCESS_GAS_STEP_LEVELS" in m for m in logs)
    asyncio.run(go())


def test_good_levels_no_error():
    assert _pc(2, O2_LV).gas_step_levels_error() == ""
    assert _pc(2, {}).gas_step_levels_error() == ""
    assert _pc(2).gas_step_levels_error() == ""


# ───────────────────────── 실제 러너: 중간 단계 실패 시 중단 / 정상이면 순서대로 ─────────────────────────
def _run(fail_flow_on_at=None):
    """가짜 장비 콜백으로 실제 러너를 돌린다. fail_flow_on_at = 직전 FLOW_SET 값이 이것인 FLOW_ON 을 실패시킴."""
    async def go():
        calls = []
        box = {"last_set": {}}
        loop = asyncio.get_running_loop()

        def later(fn, *a, **k):
            loop.call_later(0.005, lambda: fn(*a, **k))

        def plc(name, on, ch=None):
            calls.append(("plc", name, on))
            later(box["pc"].on_plc_confirmed, name)

        def mfc(cmd, args):
            calls.append(("mfc", cmd, dict(args)))
            ch = args.get("channel")
            if cmd == "FLOW_SET":
                box["last_set"][ch] = args.get("value")
            if cmd == "FLOW_ON" and fail_flow_on_at is not None and box["last_set"].get(ch) == fail_flow_on_at:
                later(box["pc"].on_mfc_failed, "FLOW_ON", "GAS 안정화 시간 초과")
            else:
                later(box["pc"].on_mfc_confirmed, cmd)

        def dcp(power, f, off):
            calls.append(("dcp_on",))
            later(box["pc"].on_dc_pulse_target_reached)

        def dcp_stop():
            calls.append(("dcp_off",))
            later(box["pc"].on_dc_pulse_off_finished)

        pc = _pc(2, O2_LV, send_plc=plc, send_mfc=mfc, start_dc_pulse=dcp, stop_dc_pulse=dcp_stop,
                 ig_wait=lambda bp: (calls.append(("ig",)), later(box["pc"].on_ig_ok)),
                 rga_scan=lambda: (calls.append(("rga",)), later(box["pc"].on_rga_finished)))
        box["pc"] = pc
        import controller.process_controller as m
        gap = m.SHUTDOWN_STEP_GAP_MS
        m.SHUTDOWN_STEP_GAP_MS = 0
        try:
            pc.start_process(dict(base_pressure=7e-6, working_pressure=60, process_time=0.005, shutter_delay=0.002,
                                  use_ar=True, ar_flow=20.0, use_o2=True, o2_flow=10.0, use_dc_pulse=True,
                                  dc_pulse_power=80, use_ms=True, use_g1=True))
            fin = None
            while fin is None:
                ev = await asyncio.wait_for(pc.event_q.get(), timeout=30)
                if ev.kind == "finished":
                    fin = ev.payload
        finally:
            m.SHUTDOWN_STEP_GAP_MS = gap
        return fin["ok"], fin["detail"].get("result"), calls
    return asyncio.run(go())


def _ch2_mfc(calls):
    return [(c[1], c[2].get("value")) for c in calls if c[0] == "mfc" and c[2].get("channel") == 2
            and c[1] in ("FLOW_SET", "FLOW_ON", "FLOW_OFF")]


def test_runner_sends_steps_in_order():
    ok, res, calls = _run()
    assert ok and res == "SUCCESS"
    ch2 = _ch2_mfc(calls)
    i = ch2.index(("FLOW_SET", 9.0))
    assert ch2[i:i + 6] == [("FLOW_SET", 9.0), ("FLOW_ON", None), ("FLOW_SET", 9.5), ("FLOW_ON", None),
                            ("FLOW_SET", 10.0), ("FLOW_ON", None)]


def test_mid_step_failure_aborts_before_next_step():
    ok, res, calls = _run(fail_flow_on_at=9.0)
    assert not ok and res == "FAIL"
    ch2 = _ch2_mfc(calls)
    assert ("FLOW_SET", 9.0) in ch2
    assert ("FLOW_SET", 9.5) not in ch2 and ("FLOW_SET", 10.0) not in ch2      # 다음 단계로 가지 않음
    assert "dcp_on" not in [c[0] for c in calls]                               # 파워도 켜지 않음
    # 종료 절차는 예전 그대로(O2 FLOW_OFF 가 나간다)
    assert ("FLOW_OFF", None) in ch2[ch2.index(("FLOW_SET", 9.0)):]


# ───────────────────────── settings.json ─────────────────────────
def test_repo_settings_load():
    res = L.check_and_load_settings(REPO_JSON)
    assert res.problems == []
    assert config_ch2.PROCESS_GAS_STEP_LEVELS == {"O2": [9.0, 9.5]}
    assert config_ch1.PROCESS_GAS_STEP_LEVELS == {}
    assert config_ch1.MFC_FLOW_STABLE_COUNT == 3 and config_ch2.MFC_FLOW_STABLE_COUNT == 3


def test_settings_without_new_keys_use_defaults(tmp_path):
    txt = REPO_JSON.read_text(encoding="utf-8")
    txt, n1 = re.subn(r'^\s*"MFC_FLOW_STABLE_COUNT":[^\n]*\n', "", txt, flags=re.M)
    txt, n2 = re.subn(r'^\s*"PROCESS_GAS_STEP_LEVELS":[^\n]*\n', "", txt, flags=re.M)
    assert (n1, n2) == (2, 1)
    p = tmp_path / "settings.json"
    p.write_text(txt, encoding="utf-8")
    res = L.check_and_load_settings(p)
    assert res.problems == []
    assert config_ch2.PROCESS_GAS_STEP_LEVELS == {"O2": [9.0, 9.5]}
    assert config_ch1.PROCESS_GAS_STEP_LEVELS == {}
    assert config_ch1.MFC_FLOW_STABLE_COUNT == 3 and config_ch2.MFC_FLOW_STABLE_COUNT == 3
    pc = _pc(2, config_ch2.PROCESS_GAS_STEP_LEVELS)
    assert _gas_part(_seq(pc, _recipe(use_o2=True, o2_flow=10.0)), 2) == TABLE[10.0]


def test_settings_bad_levels_type_is_reported(tmp_path):
    """settings.json 에서 목록 대신 숫자를 넣으면 로더가 타입 불일치로 막는다(값 검사는 시작 시 E211)."""
    txt = REPO_JSON.read_text(encoding="utf-8").replace('"PROCESS_GAS_STEP_LEVELS": {"O2": [9.0, 9.5]}',
                                                        '"PROCESS_GAS_STEP_LEVELS": 9.0')
    p = tmp_path / "settings.json"
    p.write_text(txt, encoding="utf-8")
    assert L.check_and_load_settings(p).problems


# ───────────────────────── 런타임: Chuck 이동 전 거부(E211) ─────────────────────────
def test_runtime_reject_flow():
    from runtime.chamber_runtime import ChamberRuntime
    from controller.runtime_state import runtime_state
    from errors.app_error import CodedRuntimeError

    async def go():
        rt = ChamberRuntime.__new__(ChamberRuntime)
        rt.ch = 2
        ev, logs = [], []
        rt.append_log = lambda s, m: logs.append(m)
        rt.chat = types.SimpleNamespace(notify_error_event=lambda *a: ev.append(a), flush=lambda: None)

        async def stopw(light=False):
            ev.append("stop")
        rt._stop_device_watchdogs = stopw
        rt._on_process_status_changed = lambda on: ev.append(("status", on))
        rt._auto_connect_enabled = True
        msg = _pc(2, {"O2": [9.5, 9.0]}).gas_step_levels_error()
        assert "PROCESS_GAS_STEP_LEVELS" in msg
        with pytest.raises(CodedRuntimeError) as ei:
            await rt._gas_step_reject({"process_note": "t"}, msg)
        assert ei.value.code == "E211"
        assert ev[0][1] == "E211" and "stop" in ev and ("status", False) in ev and not rt._auto_connect_enabled
        assert any("공정 시작 차단" in m for m in logs)
        assert runtime_state.has_error("chamber", 2)
        runtime_state.clear_error("chamber", 2)
    asyncio.run(go())

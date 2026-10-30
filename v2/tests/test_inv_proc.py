"""전수 목록 PROC(공정 실행) 항목 중 다른 시험이 덮지 않던 동작 — v1 기준 커밋 66efc7b 의 문구·값·순서를 실제로 돌려 본다.

항목 번호는 시험 독스트링에 적는다(v2/ch12/spec/inv_proc.py 의 test 칸이 여기를 가리킨다).
- 레시피 파일·열 정규화·값 검사(PROC-004~060)
- 시작 가드·단일(화면) 시작 입력 검사·시작 예외(PROC-056~068·079·090·107)
- 시험대(Ch12Rig — 시뮬레이터 + 실제 드라이버 + 가상 시계)에서 시작 전 단계·실행·STOP·끝 처리(PROC-083~233)
"""
from __future__ import annotations

import csv
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from v2.common.clock import VirtualClock
from v2.common.commands import Source
from v2.ch12 import process_steps as ps
from v2.ch12.recipe import TEMPLATE_COLUMNS, load_recipe_file, normalize_row, plan_row
from v2.ch12.recipe_check import (check_norm_params, check_raw_rows, check_single_inputs, single_run_defaults)
from v2.tests import proc_rig as R

CAPS1 = ps.Caps(dc=False, dc2=False, rf=False, dcp=True, rfp=True)
CAPS2 = ps.Caps(dc=True, dc2=False, rf=True, dcp=True, rfp=True, heater=True, thickness=True)


def _row(**kw):
    r = {c: "" for c in TEMPLATE_COLUMNS}
    r.update(kw)
    return r


def _csv(path: Path, rows, enc="utf-8-sig"):
    with open(path, "w", encoding=enc, newline="") as f:
        w = csv.writer(f)
        for r in rows:
            w.writerow(r)
    return path


def _full(tmp, cfg=None):
    from v2.ch12.sim.rig import Ch12FullRig
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = Ch12FullRig(c, Path(tmp), cfg=dict({"CAMERA_MIN_FREE_GB": 0.0}, **(cfg or {})), robot_api=False)
    assert rig.start()
    rig.loop.run(2)
    return rig


def _close(rig):
    rig.stop()
    rig.clock.close()


def _drop(rig):
    """시작하지 않은 시험대 정리 — 엔진 기록 스레드·출력 스레드만 멈춘다(rig.stop 은 시작한 시험대용)."""
    for e in rig.eng.values():
        e.close()
    rig.outputs.stop()
    rig.clock.close()


# ═══════════════════════ 레시피 파일 ═══════════════════════
def test_proc_004_ui_recipe_load_30s_timeout_and_exception():
    """PROC-004: 화면 파일 열기 30 s 제한 — 넘으면 'CSV 로드 30초 timeout (NAS 응답 지연): {path}' 로그 후 적재 안 함,
    그 밖 예외는 '파일 처리 오류: {e}'. 호스트 쪽 E227 은 assembly._load_file(test_proc_004_host_load_timeout_e227)."""
    from v2.ch12 import core as CORE
    from v2.ch12 import recipe as RC
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    logs = []
    eng = SimpleNamespace(log=lambda s, m: logs.append((s, m)))
    orig = RC.load_recipe_file
    try:
        RC.load_recipe_file = lambda path, origin="ui": c.wait_until(None, lambda: False)       # NAS 무응답
        t0 = c.monotonic()
        r = CORE.Ch12Core._load_recipe_ui(SimpleNamespace(clock=c), eng, r"G:\NAS\a.csv")
        assert c.monotonic() - t0 == pytest.approx(30.0)
        assert not r.ok and r.code == "E227" and not r.rows
        assert logs == [("File", r"CSV 로드 30초 timeout (NAS 응답 지연): G:\NAS\a.csv")]

        def boom(path, origin="ui"):
            raise OSError("잠김")
        RC.load_recipe_file = boom
        r = CORE.Ch12Core._load_recipe_ui(SimpleNamespace(clock=c), eng, "b.csv")
        assert r.message == "파일 처리 오류: 잠김" and r.logs == [("File", "파일 처리 오류: 잠김")]
    finally:
        RC.load_recipe_file = orig
        c.close()


def test_proc_004_host_load_timeout_e227(tmp_path):
    """PROC-004(호스트): START_SPUTTER 레시피 읽기 30 s 넘으면 CodedRuntimeError E227 'CSV 로드 30초 timeout (NAS 응답 지연): …'."""
    from v2.ch12 import assembly as A
    from v2.ch12 import recipe as RC
    from v2.ch12.errors import CodedRuntimeError
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    ad = A.EngineAdapter.__new__(A.EngineAdapter)
    ad.a = SimpleNamespace(system=SimpleNamespace(clock=c))
    orig = RC.load_recipe_file
    try:
        RC.load_recipe_file = lambda path, origin="host": c.wait_until(None, lambda: False)
        with pytest.raises(CodedRuntimeError) as ei:
            ad._load_file(r"\\nas\r.csv")
        assert ei.value.code == "E227" and str(ei.value).startswith(r"CSV 로드 30초 timeout (NAS 응답 지연): \\nas\r.csv")
    finally:
        RC.load_recipe_file = orig
        c.close()


def test_proc_005_host_file_error_texts(tmp_path):
    """PROC-005: E208 'CSV에 공정 데이터가 없습니다.'·형식 E209 문구(파일 없음 E207·인코딩 E209 는 test_recipe)."""
    p = _csv(tmp_path / "only_header.csv", [list(TEMPLATE_COLUMNS)])
    r = load_recipe_file(str(p))
    assert (r.code, r.message) == ("E208", "CSV에 공정 데이터가 없습니다.")
    r = load_recipe_file("recipe.json")
    assert (r.code, r.message) == ("E209", "지원하지 않는 레시피 형식입니다. CSV/XLSX 파일 경로만 허용됩니다.")


def test_proc_006_ui_e230_popup_and_not_queued(tmp_path):
    """PROC-006: 화면 파일 적재의 E230 — 로그 '레시피 오류 N건:\\n - …' + 경고 창 '레시피 오류', 큐에 넣지 않음."""
    p = _csv(tmp_path / "bad.csv", [list(TEMPLATE_COLUMNS), ["1", "a"] + [""] * 10 + ["x"]])
    rig = _full(tmp_path / "rig")
    try:
        e = rig.asm.proc["ch1"]
        r = rig.command("RECIPE_LOAD", {"ch": 1, "path": str(p)}, source=Source.WEB)
        assert not r.ok and r.reason == "E230"
        pop = [x for x in e.events if x.kind == "popup" and x.data.get("title") == "레시피 오류"]
        assert pop and pop[-1].detail == "레시피 오류 1건:\n- 2행('a') base_pressure='x' → 숫자가 아닙니다"
        assert R.has_log(e, "레시피 오류 1건:\n - 2행('a') base_pressure='x' → 숫자가 아닙니다")
        assert e.rows is None and not e.runs
    finally:
        _close(rig)


def test_proc_010_016_raw_check_columns():
    """PROC-010: 레거시 정수 열 dc_pulse_duty_cycle 도 int(float) 검사. PROC-016: delay·'#'·Process_name·G1~G3 Target·time 은
    검사하지 않는다(delay 'abc' 는 대기 0 → 공정 행, 결정 D163)."""
    e = check_raw_rows([_row(Process_name="p", dc_pulse_duty_cycle="7x")])
    assert e == ["2행('p') dc_pulse_duty_cycle='7x' → 숫자가 아닙니다"]
    assert check_raw_rows([_row(Process_name="p", dc_pulse_duty_cycle="70.5")]) == []
    raw = _row(Process_name="p", delay="abc", **{"#": "zz", "G1 Target": "??", "time": "q"})
    assert check_raw_rows([raw]) == []
    assert plan_row(raw).kind == "process"


# ═══════════════════════ 열 정규화 ═══════════════════════
def test_proc_021_030_032_normalize_columns():
    """PROC-021 chuck_position 그 밖 값 → ''. PROC-030 dep_rate/dep.rate(>0 만). PROC-031 thickness 0·공란 None.
    PROC-032 tooling·density·z_factor 공란 None. PROC-037 연속 전원 기본 F/0. PROC-041 rf freq 실수·duty 정수."""
    p = normalize_row(_row(chuck_position="top"), 2, CAPS2)
    assert p["chuck_position"] == ""
    assert normalize_row({"dep.rate": "0.05"}, 2, CAPS2)["dep_rate"] == 0.05
    assert normalize_row({"dep_rate": "0", "dep.rate": "0.05"}, 2, CAPS2)["dep_rate"] is None     # 첫 열이 숫자면 거기서 끝
    assert normalize_row({"dep_rate": "-1"}, 2, CAPS2)["dep_rate"] is None
    assert normalize_row({"thickness": "0"}, 2, CAPS2)["thickness"] is None
    assert normalize_row({"thickness": ""}, 2, CAPS2)["thickness"] is None
    p = normalize_row(_row(tooling="", density="2.5", z_factor=""), 2, CAPS2)
    assert (p["tooling"], p["density"], p["z_factor"]) == (None, 2.5, None)
    p = normalize_row(_row(), 2, CAPS2)
    assert (p["use_dc_power"], p["dc_power"], p["use_dc_power2"], p["dc_power2"], p["use_rf_power"], p["rf_power"]) == \
        (False, 0.0, False, 0.0, False, 0.0)
    p = normalize_row(_row(use_rf_pulse="T", rf_pulse_power="100", rf_pulse_freq="0.5", rf_pulse_duty_cycle="40.9"), 2, CAPS2)
    assert p["rf_pulse_freq"] == 0.5 and p["rf_pulse_duty"] == 40


def test_proc_038_040_dc_pulse_columns():
    """PROC-038 off_time 소수 1자리·'DC'·공란 None·해석 불가 경고. PROC-039 레거시 duty 환산·freq 없으면 경고 keep.
    PROC-040 표시 duty: DC → 100.0, off·freq → (period−off)/period×100, 그 외 None."""
    log = []
    p = normalize_row(_row(use_dc_pulse="T", dc_pulse_power="80", dc_pulse_freq="20", dc_pulse_off_time="9.96"), 2, CAPS2,
                      log=log)
    assert p["dc_pulse_off_time"] == 10.0 and p["dc_pulse_duty"] == 80.0 and p["dc_pulse_freq"] == 20
    p = normalize_row(_row(use_dc_pulse="T", dc_pulse_power="80", dc_pulse_off_time="x1"), 2, CAPS2, log=log)
    assert p["dc_pulse_off_time"] is None and p["dc_pulse_duty"] is None
    assert ("Params", "[경고] dc_pulse_off_time='x1' 해석 불가 → 유지") in log
    p = normalize_row(_row(use_dc_pulse="T", dc_pulse_power="80", dc_pulse_off_time="dc"), 2, CAPS2)
    assert p["dc_pulse_off_time"] == "DC" and p["dc_pulse_duty"] == 100.0
    log = []
    p = normalize_row(_row(use_dc_pulse="T", dc_pulse_power="80", dc_pulse_duty_cycle="70"), 2, CAPS2, log=log)
    assert p["dc_pulse_off_time"] is None
    assert log == [("Params", "[경고] dc_pulse_duty_cycle=70% 가 있으나 dc_pulse_freq 가 비어 Off Time 으로 환산할 수 없습니다 "
                              "→ Off Time 유지(keep)")]


def test_proc_043_045_046_change_power_and_test_keys():
    """PROC-043 power_change_time·change_power_value 는 문자열 그대로. PROC-045 TEST 행 키(test_mode·test_duration_sec(실패 0)·time).
    PROC-046 time 열은 러너 기간 해석(_parse_duration_seconds)."""
    p = normalize_row(_row(power_change_time=" 1h30m ", change_power_value="90"), 2, CAPS2)
    assert (p["power_change_time"], p["change_power_value"]) == ("1h30m", "90")
    p = normalize_row({"test_mode": True, "test_duration_sec": "x", "time": " 5m "}, 2, CAPS2)
    assert p["test_mode"] is True and p["test_duration_sec"] == 0.0 and p["time"] == "5m"
    r = plan_row({"#": "TEST", "time": "1m30s", "Process_name": "t"})
    assert r.test and r.raw["test_duration_sec"] == 90.0 and r.raw["process_time"] == 1.5 and r.raw["process_note"] == "t"


def test_proc_047_054_norm_check_rules():
    """PROC-047 TEST 행은 검사 안 함. PROC-048 유량 None/''/'None' → 0, 음수 금지. PROC-051 DC2 파워 음수·DC2 폐지.
    PROC-052 RF Pulse freq 상한 RFPULSE_FREQ_MAX_HZ/1000(0 이하면 30 kHz)·duty 1..99. PROC-053 DC Pulse off 상한(f 공란 10.0).
    PROC-054 히터 ramp ≤ 60·soak 0~480·CH1 히터 오류."""
    base = dict(use_ar=True, ar_flow="None", o2_flow="", n2_flow=None, use_g1=True)
    p = dict(base, use_rf_power=True, n2_flow=-1)
    assert check_norm_params(p, 2, CAPS2, {}) == ["n2_flow는 음수 불가"] and p["ar_flow"] == 0.0 and p["o2_flow"] == 0.0
    assert check_norm_params({"test_mode": True}, 2, CAPS2, {}) == []
    e = check_norm_params(dict(base, use_dc_power2=True, dc_power2=-5), 2, CAPS2, {})
    assert e == ["DC2 Target Power(W)는 0 이상이어야 합니다.", "DC2 Power 는 사용할 수 없습니다(DC2 폐지)"]
    e = check_norm_params(dict(base, use_rf_pulse=True, rf_pulse_power=10, rf_pulse_freq=31.0, rf_pulse_duty=0), 2, CAPS2,
                          {"CH2_RFPULSE_FREQ_MAX_HZ": 0})
    assert e == ["RF Pulse Freq 31kHz 는 허용 범위 0.001~30kHz 밖입니다 (CESAR 사양 3-5: 1Hz~30kHz — 초과 시 W40으로 출력 차단)",
                 "RF Pulse Duty(%)는 1..99"]
    e = check_norm_params(dict(base, use_rf_pulse=True, rf_pulse_power=10, rf_pulse_freq=31.0), 2, CAPS2,
                          {"RFPULSE_FREQ_MAX_HZ": 40000})
    assert e == []
    e = check_norm_params(dict(base, use_dc_pulse=True, dc_pulse_power=10, dc_pulse_off_time=10.5), 2, CAPS2, {})
    assert e == ["DC Pulse Off Time 10.5µs 는 (주파수 미지정 → 절대 상한) 상한 10.0µs 초과(매뉴얼 p.13 그림3)"]
    e = check_norm_params(dict(base, use_rf_power=True, use_heater=True, heater_temp=100, heater_ramp=61, heater_soak=481),
                          2, CAPS2, {})
    assert e == ["heater_ramp 는 0 초과 60°C/min 이하여야 합니다 (현재 61)", "heater_soak 는 0~480분이어야 합니다 (현재 481)"]
    e = check_norm_params(dict(use_ar=True, use_rf_pulse=True, rf_pulse_power=5, use_heater=True), 1, CAPS1, {})
    assert e == ["CH1 에는 히터가 없습니다 (use_heater=T)"]


# ═══════════════════════ 단일(화면) 시작 입력 검사(PROC-056~060) ═══════════════════════
def test_proc_056_057_single_inputs_ch1():
    """PROC-056: 가스 1개 이상('선택 오류')·유량 숫자·0 이상('가스 유량 입력을 확인하세요.'). PROC-057: CH1 펄스 둘 다 금지·하나 필수
    (지원에 따라 문구 3종)·DC Pulse 파워>0·freq 20..150·Off Time·RF Pulse 파워·freq·duty, 연속 전원·건 미사용."""
    ok = dict(use_ar=True, ar_flow="20", use_rf_pulse=True, rf_pulse_power="120", rf_pulse_freq="0.5", rf_pulse_duty="40")
    assert check_single_inputs(dict(ok, use_ar=False), 1, CAPS1, {}) == ("선택 오류", "가스를 하나 이상 선택해야 합니다.")
    assert check_single_inputs(dict(ok, ar_flow="-1"), 1, CAPS1, {}) == ("입력값 확인", "가스 유량 입력을 확인하세요.")
    assert check_single_inputs(dict(ok, ar_flow="x"), 1, CAPS1, {}) == ("입력값 확인", "가스 유량 입력을 확인하세요.")
    assert check_single_inputs(dict(ok, use_dc_pulse=True), 1, CAPS1, {})[1] == \
        "RF Pulse와 DC Pulse는 동시에 사용할 수 없습니다 (같은 시리얼 포트, 입력칸 공유). 하나만 선택하세요."
    none = dict(ok, use_rf_pulse=False)
    assert check_single_inputs(dict(none), 1, CAPS1, {})[1] == "CH1은 RF-Pulse(또는 DC-Pulse) 중 하나를 선택해야 합니다."
    assert check_single_inputs(dict(none), 1, ps.Caps(dc=False, dc2=False, rf=False, dcp=True, rfp=False), {})[1] == \
        "CH1은 DC-Pulse를 선택해야 합니다."
    assert check_single_inputs(dict(none), 1, ps.Caps(dc=False, dc2=False, rf=False, dcp=False, rfp=False), {})[1] == \
        "CH1에서 사용 가능한 파워 장치가 없습니다."
    assert check_single_inputs(dict(ok, rf_pulse_power="0"), 1, CAPS1, {})[1] == "RF Pulse Target Power(W)를 확인하세요."
    assert check_single_inputs(dict(ok, rf_pulse_freq="31"), 1, CAPS1, {})[1] == \
        "RF Pulse Freq(kHz)는 0.001..30 범위입니다.\n(CESAR 사양 3-5: 1Hz~30kHz — 초과 시 W40으로 출력 차단)"
    assert check_single_inputs(dict(ok, rf_pulse_duty="100"), 1, CAPS1, {})[1] == "RF Pulse Duty(%) 1..99"
    dcp = dict(use_ar=True, ar_flow="20", use_dc_pulse=True, dc_pulse_power="60", dc_pulse_freq="50")
    assert check_single_inputs(dict(dcp, dc_pulse_power=""), 1, CAPS1, {})[1] == "DC-Pulse Target Power(W)를 확인하세요."
    assert check_single_inputs(dict(dcp, dc_pulse_freq="151"), 1, CAPS1, {})[1] == "DC-Pulse Freq(kHz)는 20..150 범위입니다."
    assert check_single_inputs(dict(dcp, dc_pulse_off_time="z"), 1, CAPS1, {})[1] == \
        "DC-Pulse Off Time 은 숫자(µs) 또는 'DC' 여야 합니다.\n공란이면 장비 현재값을 유지합니다."
    assert check_single_inputs(dict(dcp, dc_pulse_off_time="8.1"), 1, CAPS1, {})[1] == \
        "DC-Pulse Off Time 8.1µs 는 허용 범위 밖입니다.\n50kHz 기준 1.0 ~ 8.0µs (매뉴얼 p.13 그림3: 상한 = min(10.0, 400/f))"
    p = dict(dcp, dc_pulse_off_time="DC", use_g1=True, use_rf_power=True, use_o2=True, o2_flow="")
    assert check_single_inputs(p, 1, CAPS1, {}) is None
    assert (p["use_g1"], p["use_rf_power"], p["dc_pulse_duty"], p["o2_flow"], p["ar_flow"]) == (False, False, 100.0, 0.0, 20.0)


def test_proc_058_single_inputs_ch2():
    """PROC-058: CH2 DC/DC2/RF 파워>0, 펄스 동시 금지, 펄스 값('RF-Pulse' 문구), 전원 1개 이상, power_select 는 막지 않음."""
    base = dict(use_ar=True, ar_flow=20, use_power_select=True)
    assert check_single_inputs(dict(base, use_dc_power=True, dc_power=0), 2, CAPS2, {})[1] == "DC Power(W)를 확인하세요."
    assert check_single_inputs(dict(base, use_dc_power2=True, dc_power2="x"), 2, CAPS2, {})[1] == "DC2 Power(W)를 확인하세요."
    assert check_single_inputs(dict(base, use_rf_power=True), 2, CAPS2, {})[1] == "RF Power(W)를 확인하세요."
    assert check_single_inputs(dict(base), 2, CAPS2, {}) == \
        ("선택 오류", "DC1/DC2/RF Power 또는 DC/RF Pulse 중 하나 이상 선택해야 합니다.")
    rfp = dict(base, use_rf_pulse=True, rf_pulse_power=100, rf_pulse_freq=1.0, rf_pulse_duty=50)
    assert check_single_inputs(dict(rfp, rf_pulse_power=-1), 2, CAPS2, {})[1] == "RF-Pulse Target Power(W)를 확인하세요."
    assert check_single_inputs(dict(rfp, rf_pulse_duty=0), 2, CAPS2, {})[1] == "RF-Pulse Duty(%)는 1..99 범위입니다."
    assert check_single_inputs(dict(rfp, rf_pulse_freq=0), 2, CAPS2, {})[1].startswith("RF-Pulse Freq(kHz)는 0.001..30 범위")
    p = dict(rfp, use_dc_power=True, dc_power="100")
    assert check_single_inputs(p, 2, CAPS2, {}) is None and p["use_power_select"] is True and p["dc_power"] == 100.0
    nop = ps.Caps(dc=True, dc2=False, rf=True, dcp=True, rfp=False)
    p = dict(rfp)
    assert check_single_inputs(p, 2, nop, {})[1].startswith("DC1/DC2/RF Power 또는")        # 미지원 펄스는 선택 안 된 것


def test_proc_059_060_single_defaults():
    """PROC-059: base 공란 1e-5·integration_time 60 고정·이름 비면 'Single CH{n}'·t0_pressed_wall/ns·G Target 동기.
    PROC-060: dep_rate·thickness 둘 다 > 0 이면 process_time = thickness/dep_rate/60(소수 3자리)."""
    p = single_run_defaults({"base_pressure": "", "integration_time": 200, "G1_target_name": "Ti", "dep_rate": "0.07",
                             "thickness": "100", "process_time": "9"}, 2, "2026-10-30T10:00:00", 123)
    assert p["base_pressure"] == 1e-5 and p["integration_time"] == 60 and p["process_note"] == p["Process_name"] == "Single CH2"
    assert p["process_time"] == 23.81 and p["t0_pressed_wall"] == "2026-10-30T10:00:00" and p["t0_pressed_ns"] == 123
    assert p["G1 Target"] == "Ti" and p["G2 Target"] == ""
    p = single_run_defaults({"process_note": "A", "process_time": 5, "dep_rate": 0.05}, 1, "t", 0)
    assert p["process_note"] == "A" and p["process_time"] == 5.0 and p["working_pressure"] == 0.0


def test_proc_056_059_engine_single_start_reject_and_log(tmp_path):
    """단일 시작: 입력칸 검사 실패 → 경고 창(v1 제목·문구)·호스트 'invalid inputs'·시작 안 함(공정 txt 도 없음),
    통과 → '입력 검증 통과 → Runner START'·공정 이름 'Single CH1'."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    host = []
    e.on_host_start = lambda ok, msg: host.append((ok, msg))
    p = dict(base_pressure=1e-5, working_pressure=3, process_time=0.05, shutter_delay=0.05, use_ar=True, ar_flow="x",
             use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40)
    got = {}
    R.run(rig, R.idle(e), max_ticks=400,
          hooks=[R.at(2, lambda: got.setdefault("bad", e.start(params=dict(p), source="local", owner="local"))),
                 R.at(4, lambda: got.setdefault("ok", e.start(params=dict(p, ar_flow=20), source="local", owner="local")))])
    assert got == {"bad": "RECIPE_PARAM_ERROR", "ok": None}
    pops = [(x.data.get("title"), x.detail) for x in e.events if x.kind == "popup"]
    assert ("입력값 확인", "가스 유량 입력을 확인하세요.") in pops and host[0] == (False, "invalid inputs")
    assert R.has_log(e, "입력 검증 통과 → Runner START") and R.ends(e) == [("done", None)]
    assert [x.detail for x in e.events if x.kind == "run_start"] == ["Single CH1"]
    assert len(sorted((tmp_path / "logs" / "CH1").glob("CH1_*.txt"))) == 1


# ═══════════════════════ 장비 이벤트 → 중단·확인(v1 펌프 — process_events) ═══════════════════════
class _Runner:
    def __init__(self):
        self.calls = []

    def match(self, k, spec=None):
        self.calls.append(("match", k, spec))

    def step_failed(self, src, why, code=None, meta=None, **kw):
        self.calls.append(("fail", src, str(why), code))

    def mfc_failed(self, cmd, why, code=None, meta=None):
        self.calls.append(("mfc_failed", cmd, str(why)))

    def power_off_failed(self, src, tok, why, code=None, meta=None):
        self.calls.append(("off_failed", src, tok, str(why)))


def _acts(n=2, pc_ig=False):
    from v2.ch12.outputs.run_stats import RunStats
    from v2.ch12.process_events import ProcessEventsMixin

    class A(ProcessEventsMixin):
        pass
    a = A()
    a.n, a.ch = n, f"ch{n}"
    a.owner = f"chamber{n}"
    a.sources = {f"ch{n}.mfc", f"ch{n}.ig", f"ch{n}.dcpulse", f"ch{n}.rfpulse"} | ({"ch2.dc", "ch2.rf"} if n == 2 else set())
    a.lines, a.chats = [], []
    a.runner = _Runner()
    a.mfc_polling = False
    a.dc_failed = {1: False, 2: False}
    a.e = SimpleNamespace(stats=RunStats(), busy=True, ig_in_use_by_pc=lambda: pc_ig, run_warnings=[],
                          chat=a.chats.append, params={"process_name": "P"}, run_id="r1")
    a.log = lambda s, m: a.lines.append((s, m))
    a._match = lambda k, spec=None: a.runner.match(k, spec)
    a._fail = lambda src, why, code=None, meta=None: a.runner.step_failed(src, why, code=code, meta=meta)
    return a


def _ev(a, source, kind, **kw):
    a.on_event(SimpleNamespace(source=source, kind=kind, event=SimpleNamespace(**dict(dict(
        cmd=None, detail="", reason="", owner=None, gas=None, value=None, text=None, code=None, data={}, pressure=None), **kw))))


def test_proc_164_201_mfc_owner_and_failure_routing():
    """PROC-201: MFC 확인·실패는 owner 가 다른 것(PC 등)을 넘기지 않고 owner 없는 것은 넘긴다. PROC-164: 실패는 실행기 mfc_failed
    (지금 기다리는 명령일 때만 중단 — test_process_stop::test_mfc_failure_only_when_waited). PROC-191: 압력 표본은 메인 폴링 구간만,
    '[poll] ChamberP' 줄은 항상. GAPB-006: '[poll] {gas}: x.xx sccm'."""
    a = _acts(2)
    _ev(a, "ch2.mfc", "command_confirmed", cmd="FLOW_ON", owner="pc2")
    _ev(a, "ch2.mfc", "command_failed", cmd="FLOW_ON", owner="pc2", detail="x")
    assert a.runner.calls == []
    _ev(a, "ch2.mfc", "command_confirmed", cmd="FLOW_ON", owner="chamber2")
    _ev(a, "ch2.mfc", "command_confirmed", cmd="VALVE_OPEN")
    _ev(a, "ch2.mfc", "command_failed", cmd="FLOW_SET", detail="안정화 실패")
    assert a.runner.calls == [("match", "MFC", "FLOW_ON"), ("match", "MFC", "VALVE_OPEN"), ("mfc_failed", "FLOW_SET", "안정화 실패")]
    _ev(a, "ch2.mfc", "flow", gas="Ar", value=19.996)
    _ev(a, "ch2.mfc", "pressure", text="2.95", value=2.95)
    assert ("MFC2", "[poll] Ar: 20.00 sccm") in a.lines and ("MFC2", "[poll] ChamberP: 2.95") in a.lines
    a.e.stats.collecting = True
    _ev(a, "ch2.mfc", "pressure", text="2.96", value=2.96)
    assert list(a.e.stats.pressure) == []                                       # 메인 폴링 구간 밖 — 표본 없음
    a.mfc_polling = True
    _ev(a, "ch2.mfc", "pressure", text="3.01", value=3.01)
    assert ("MFC2", "[poll] ChamberP: 3.01") in a.lines and list(a.e.stats.pressure) == [3.01]


def test_proc_165_202_ig_base_and_pc_share():
    """PROC-165: IG base_failed → 중단(IG). PROC-202: 같은 CH 플라즈마 클리닝이 IG 사용 중이면 status·pressure 줄은 기록하지 않고
    Base 도달/실패는 넘긴다."""
    a = _acts(1, pc_ig=True)
    _ev(a, "ch1.ig", "status", detail="IG ON")
    _ev(a, "ch1.ig", "pressure", pressure=1e-5)
    _ev(a, "ch1.ig", "base_reached")
    _ev(a, "ch1.ig", "base_failed", detail="Base 미도달(timeout)", code="E302")
    assert a.lines == [] and a.runner.calls == [("match", "IG_OK", None), ("fail", "IG", "Base 미도달(timeout)", "E302")]
    b = _acts(1, pc_ig=False)
    _ev(b, "ch1.ig", "status", detail="IG ON")
    assert b.lines == [("IG1", "IG ON")]


def test_proc_166_158_159_dc_target_failed_and_off():
    """PROC-166: DC/DC2 target_failed → 채팅 '⚠️ CH2 DC Power 과전류 차단 → 전체 공정 중단 | msg' 또는 '… 출력 이상 → …' 후 중단.
    PROC-159: target_failed 뒤 첫 power_off_finished 는 OFF 확인으로 넘기지 않음(1회 — 결정 D164), power_off_failed 면 플래그 해제.
    PROC-158: OFF 실패 경고 문구(로그·런 경고·채팅) + OFF 실패 경로. GAPB-006 '측정: P W, V V, I A'."""
    a = _acts(2)
    _ev(a, "ch2.dc", "target_failed", detail="과전류 감지", data={"unit": "DC1"})
    assert a.chats == ["⚠️ CH2 DC Power 과전류 차단 → 전체 공정 중단 | 과전류 감지"]
    _ev(a, "ch2.dc", "power_off_finished", data={"unit": "DC1"})
    _ev(a, "ch2.dc", "power_off_finished", data={"unit": "DC1"})
    assert a.runner.calls == [("fail", "DC Power", "과전류 감지", None), ("match", "DC_OFF", None)]
    _ev(a, "ch2.dc", "target_failed", detail="low power", data={"unit": "DC2"})
    assert a.chats[-1] == "CH2 DC2 출력 이상 → 전체 공정 중단 | low power"
    _ev(a, "ch2.dc", "power_off_failed", detail="OFF 미확인", data={"unit": "DC2"})
    assert not a.dc_failed[2]
    assert a.chats[-1] == ("⚠️ CH2 DC2 Power OFF 실패 - 출력 상태 미확인 (켜져 있을 가능성 있음). 전원 장비 출력과 PLC D/A(SET/WRITE)를 "
                           "즉시 확인하세요.")
    assert a.runner.calls[-1] == ("off_failed", "DC2 Power", "DC2_OFF", "OFF 미확인")
    assert a.e.run_warnings == ["DC2 Power OFF 실패(출력 상태 미확인)"]
    _ev(a, "ch2.dc", "display", data={"unit": "DC1", "power": 100.04, "voltage": 351.25, "current": 0.2847})
    assert ("DC1", "측정: 100.0 W, 351.2 V, 0.285 A") in a.lines


def test_proc_167_168_rf_events():
    """PROC-167: RF 연속 target_failed → 중단. PROC-168: RF Pulse command_failed(RF_OFF 제외) → 중단, RF_OFF 실패는 OFF 실패 경로
    + '⚠️ CH{n} RF Pulse OFF 실패 - …'. GAPB-006 '[poll] fwd=x.xW, ref=x.xW'."""
    a = _acts(2)
    _ev(a, "ch2.rf", "target_failed", detail="반사파 과다")
    _ev(a, "ch2.rf", "display", data={"forward": 150.04, "reflected": 2.0})
    _ev(a, "ch2.rfpulse", "command_failed", cmd="SET_SETPOINT", detail="NAK")
    _ev(a, "ch2.rfpulse", "command_failed", cmd="RF_OFF", detail="응답 없음")
    assert a.runner.calls == [("fail", "RF Power", "반사파 과다", None), ("fail", "RFPulse", "NAK", None),
                              ("off_failed", "RFPulse", "RFPULSE_OFF", "응답 없음")]
    assert ("RF2", "[poll] fwd=150.0W, ref=2.0W") in a.lines
    assert a.chats == ["⚠️ CH2 RF Pulse OFF 실패 - 출력 상태 미확인 (켜져 있을 가능성 있음). RF Pulse 장비의 출력 상태를 즉시 확인하세요."]


def test_proc_169_170_dc_pulse_events():
    """PROC-169: DC Pulse command_failed — OUTPUT_OFF 는 OFF 실패 경로(문구), phase=prepare·READ_*·FAULT_RESET 무시, AUTO_STOP/
    target_failed 는 사유별 채팅 후 중단. 확인: OUTPUT_ON → DC_PULSE_TARGET, REF_POWER → DC_PULSE_SET, OUTPUT_OFF → DCPULSE_OFF.
    GAPB-006 '[telemetry] P=… W, V=… V, I=… A (raw P/I/V=…)'."""
    a = _acts(1)
    for cmd in ("OUTPUT_ON", "REF_POWER", "OUTPUT_OFF"):
        _ev(a, "ch1.dcpulse", "command_confirmed", cmd=cmd)
    _ev(a, "ch1.dcpulse", "command_failed", cmd="REF_POWER", detail="x", data={"phase": "prepare"})
    _ev(a, "ch1.dcpulse", "command_failed", cmd="READ_POWER", detail="x")
    _ev(a, "ch1.dcpulse", "command_failed", cmd="FAULT_RESET", detail="x")
    assert a.runner.calls == [("match", "DC_PULSE_TARGET", None), ("match", "DC_PULSE_SET", None), ("match", "DCPULSE_OFF", None)]
    _ev(a, "ch1.dcpulse", "command_failed", cmd="AUTO_STOP", detail="overcurrent")
    _ev(a, "ch1.dcpulse", "command_failed", cmd="AUTO_STOP", detail="low_current")
    _ev(a, "ch1.dcpulse", "command_failed", cmd="X", detail="target_failed sp")
    assert a.chats == ["⚠️ CH1 DC Pulse 과전류 차단 → 전체 공정 중단 | overcurrent",
                       "CH1 DC Pulse 저전류 감지 → 전체 공정 중단 | low_current",
                       "CH1 DC Pulse 세트포인트 이탈(연속) 또는 P=0W 감지 → 전체 공정 중단 | target_failed sp"]
    assert [c[0] for c in a.runner.calls[3:]] == ["fail", "fail", "fail"]
    _ev(a, "ch1.dcpulse", "command_failed", cmd="OUTPUT_OFF", detail="timeout")
    assert a.runner.calls[-1] == ("off_failed", "DCPulse", "DCPULSE_OFF", "OUTPUT_OFF 미확인(출력 상태 미확인)")
    assert a.e.run_warnings == ["DC Pulse OUTPUT OFF 실패(출력 상태 미확인)"]
    _ev(a, "ch1.dcpulse", "telemetry", data={"P_W": 60.04, "V_V": 300.123, "I_A": 0.2, "raw": {"P": 600, "I": 20, "V": 3001}})
    assert ("DCPulse1", "[telemetry] P=60.0 W, V=300.12 V, I=0.200 A (raw P/I/V=600/20/3001)") in a.lines


# ═══════════════════════ 실행기(StepRunner — 가짜 장비, 가상 시계) ═══════════════════════
from v2.tests.test_process_stop import Host as _Host, _go  # noqa: E402
from v2.ch12.process_steps import ProcessStep as S  # noqa: E402


def test_proc_153_154_request_stop_and_shutdown_texts():
    """PROC-153: 실행 중 아님 '정지 요청: 실행 중 공정 없음(이미 종료됨) → 무시'. PROC-154: 종료 절차 무시 '종료 절차 무시: …',
    시작 문구·폴링 OFF·'종료 절차 생성 완료: N단계', 종료 절차가 없으면 '종료 절차가 없어서 즉시 완료합니다.' → 실패."""
    from v2.ch12.process_runner import StepRunner
    h = _Host()
    r = StepRunner(h)
    r.request_stop()
    r._start_shutdown()
    assert h.lines == ["정지 요청: 실행 중 공정 없음(이미 종료됨) → 무시", "종료 절차 무시: 실행 중 공정 없음(이미 종료됨)"]
    h.clock.close()
    h = _Host()
    h.shutdown_steps = lambda: []
    r, res = _go(h, at=5.0, act=lambda r: (r.request_stop(), r._start_shutdown()))
    assert "종료 절차가 없어서 즉시 완료합니다." in h.lines and "종료 절차 무시: 이미 종료 절차 진행 중입니다." in h.lines
    assert not res.ok and res.detail["result"] == "STOP"


def test_proc_160_161_162_failure_attribution():
    """PROC-160: 매 단계 앞에서 정지 요청 확인 '정지 요청 감지 - 종료 절차를 시작합니다.'. PROC-161: 첫 실패만 대표 원인(code 대문자),
    'Step n ACTION: [src - code: detail]'·코드 없으면 '[src - detail]'·detail 비면 'unknown'. PROC-162: source → 토큰 종류로 가장 최근
    소유 단계(없으면 현재 단계)."""
    from v2.ch12.process_runner import StepRunner
    h = _Host(delays={"DC_PULSE_START": 30.0})

    def act(r):
        r.step_failed("RFPulse", "", code=None)                       # 소유 단계 없음 → 현재 단계(DC_PULSE_START)
        r.step_failed("DCPulse", "두 번째", code="e999")
    r, res = _go(h, at=0.5, act=act)
    d = res.detail
    assert d["errors"][0] == "Step 3 DC_PULSE_START: [RFPulse - unknown]" and d["error_source"] == "RFPulse"
    assert d["error_code"] is None and d["error_detail"] == "unknown"
    h2 = _Host(delays={"PLC_CMD": 0.1})
    r2 = StepRunner(h2)
    steps = [S("PLC_CMD", "메인 밸브 열기", params=("MV", True, 1)), S("DELAY", "대기", duration=5000)]
    out = {}
    h2.clock.start_thread(lambda: out.setdefault("r", r2.run({"process_note": "x"}, steps)), "run")
    h2.clock.wait_until(h2.clock.monotonic() + 1.0, None)
    r2.step_failed("PLC", "늦은 실패", code="e401")                 # 지난 PLC 단계(1) 소유
    h2.clock.wait_until(h2.clock.monotonic() + 60, lambda: "r" in out)
    h2.clock.close()
    assert out["r"].detail["errors"][0] == "Step 1 PLC_CMD: [PLC - e401: 늦은 실패]" and out["r"].detail["error_code"] == "E401"
    h3 = _Host()
    r3, res3 = _go(h3, steps=[S("DELAY", "a", duration=2000), S("DELAY", "b", duration=2000)], at=0.5,
                   act=lambda r: setattr(r, "stop_requested", True))
    assert "정지 요청 감지 - 종료 절차를 시작합니다." in h3.lines


def test_proc_177_178_parallel_block_and_tokens():
    """PROC-177: 연속한 parallel 단계를 한꺼번에 보내고 토큰이 모두 올 때까지 기다림. PROC-178: no_wait·토큰 없는 단계는 즉시 다음,
    새 대기는 이전 대기를 취소(replaced). PROC-179: 단계별 기대 토큰 표."""
    from v2.ch12.process_runner import tokens_for
    from v2.ch12.process_steps import norm_plc_name
    h = _Host(delays={"DC_POWER_SET": 3.0, "RF_POWER_SET": 5.0})
    steps = [S("DC_POWER_SET", "DC", value=100.0, parallel=True), S("RF_POWER_SET", "RF", value=150.0, parallel=True),
             S("OES_RUN", "OES", params=(60.0, 60), no_wait=True), S("DELAY", "끝", duration=1000)]
    r, res = _go(h, steps=steps)
    sent = {a: t for t, a, _m in h.actions.sent}
    tr = {x[2]: x[0] for x in r.trace}
    assert sent["DC_POWER_SET"] == sent["RF_POWER_SET"]                       # 같이 보냄
    assert tr["OES_RUN"] - sent["DC_POWER_SET"] == pytest.approx(5.0, abs=0.05)  # 둘 다 확인된 뒤
    assert tr["DELAY"] == pytest.approx(tr["OES_RUN"], abs=0.01) and res.ok      # no_wait → 즉시
    need = {"DC_PULSE_START": dict(value=1.0, params=(None, None)), "RF_PULSE_START": dict(value=1.0, params=(None, None)),
            "OES_RUN": dict(params=(60.0, 60))}
    tk = lambda a, **kw: tokens_for(S(a, "m", **dict(dict(value=1.0), **need.get(a, {}), **kw)), norm_plc_name)  # noqa: E731
    assert tk("DC_POWER_SET") == [("DC_TARGET", None)] and tk("DC2_POWER_STOP") == [("DC2_OFF", None)]
    assert tk("DC_PULSE_START") == [("DC_PULSE_TARGET", None)] and tk("DC_PULSE_SET") == [("DC_PULSE_SET", None)]
    assert tk("DC_PULSE_STOP") == [("DCPULSE_OFF", None)] and tk("RF_PULSE_START") == [("RF_TARGET", None)]
    assert tk("RF_PULSE_STOP") == [("RFPULSE_OFF", None)] and tk("IG_CMD") == [("IG_OK", None)]
    assert tk("RGA_SCAN") == [("RGA_OK", None)] and tk("PLC_CMD", params=("mainvalve", True)) == [("PLC", "MV")]
    assert tk("MFC_CMD", params=("FLOW_ON", {})) == [("MFC", "FLOW_ON")] and tk("OES_RUN") == []
    assert tk("HEATER_WAIT") == [("HEATER_READY", None)] and tk("HEATER_HOLD") == [("HEATER_HOLD", None)]
    from v2.ch12.process_tokens import Expect
    ex = Expect([("PLC", "MV")])
    r2 = _Host()
    from v2.ch12.process_runner import StepRunner
    sr = StepRunner(r2)
    sr.expect = ex
    sr._set_expect([("PLC", "AR")])
    assert ex.cancelled == "replaced"
    r2.clock.close()


def test_proc_181_182_183_delay_countdown_and_actual_time():
    """PROC-181: 평시 단계 대기에는 컨트롤러 제한이 없다(장비가 30분 늦어도 기다림). PROC-182: DELAY '{msg} 시작 (N초 대기)'(올림),
    상태 '{msg} (남은 시간: M분 S초 | S초)', 끝 '{msg} 완료'. PROC-183: 'Shutter Delay'·'메인 공정 진행' 실제 경과 누적(분, 소수 3자리)."""
    h = _Host(delays={"PLC_CMD": 1800.0})
    states = []
    h.state = states.append
    steps = [S("PLC_CMD", "메인 밸브 열기", params=("MV", True, 1)), S("DELAY", "Shutter Delay 1.5분", duration=90_500),
             S("DELAY", "메인 공정 진행 (0.5분)", duration=30_000, polling=True)]
    r, res = _go(h, steps=steps)
    tr = {x[2] + x[3]: x[0] for x in r.trace}
    assert tr["DELAYShutter Delay 1.5분"] - tr["PLC_CMD메인 밸브 열기"] == pytest.approx(1800.0, abs=0.01)
    assert "Shutter Delay 1.5분 시작 (91초 대기)" in h.lines and "Shutter Delay 1.5분 완료" in h.lines
    assert "Shutter Delay 1.5분 (남은 시간: 1분 31초)" in states and "Shutter Delay 1.5분 (남은 시간: 59초)" in states
    assert res.ok and res.detail["actual_shutter_delay_min"] == round(90.5 / 60, 3) and res.detail["actual_process_time_min"] == 0.5


def test_proc_186_187_188_finish_detail_and_exception():
    """PROC-186·187: 끝 판정·detail 키·문구(성공/중단 + '[종료 중 실패 요약] 총 N건')·상태 '공정 완료'/'공정 중단됨'.
    PROC-188: 러너 예외 '러너 예외: …' → finish(False). PROC-108: 실행 중 start '오류: 이미 다른 공정이 실행 중입니다.'."""
    h = _Host()
    states = []
    h.state = states.append
    r, res = _go(h, steps=[S("DELAY", "a", duration=1000)])
    assert res.ok and res.detail["result"] == "SUCCESS" and set(res.detail) >= {
        "process_name", "result", "stopped", "test_mode", "aborting", "errors", "error_code", "error_source", "error_detail",
        "error_meta", "actual_shutter_delay_min", "actual_process_time_min"}
    assert states[-1] == "공정 완료" and "=== 공정이 성공적으로 완료되었습니다 ===" in h.lines
    h = _Host(delays={"DC_PULSE_STOP": None})
    states = []
    h.state = states.append
    r, res = _go(h, at=5.0, act=lambda r: r.step_failed("PLC", "x", code="E401"))
    assert res.detail["result"] == "FAIL" and "=== 공정이 중단되었습니다 ===" in h.lines and states[-1] == "공정 중단됨"
    assert f"[종료 중 실패 요약] 총 {len(res.detail['errors'])}건" in h.lines and len(res.detail["errors"]) == 2
    h = _Host()

    def bad_send(step, runner):
        raise RuntimeError("장비 객체 없음")
    h.actions.send = bad_send
    r, res = _go(h, steps=[S("PLC_CMD", "메인 밸브 열기", params=("MV", True, 1))])
    assert "러너 예외: 장비 객체 없음" in h.lines and not res.ok
    from v2.ch12.process_runner import StepRunner
    h = _Host()
    sr = StepRunner(h)
    sr.is_running = True
    assert sr.run({}, []).detail["reason"] == "busy" and h.lines == ["오류: 이미 다른 공정이 실행 중입니다."]
    h.clock.close()


def test_proc_189_polling_targets_and_logs():
    """PROC-189: 폴링 로그는 바뀔 때만('폴링 시작'/'폴링 중지', 처음 None→False 생략), 타깃 = mfc True + 사용 전원(펄스·연속 동시 허용),
    off 면 전부 False."""
    h = _Host()
    applied = []
    h.actions.apply_polling = lambda active, targets: applied.append((active, dict(targets)))
    steps = [S("DELAY", "a", duration=1000), S("DELAY", "b", duration=1000, polling=True),
             S("DELAY", "c", duration=1000, polling=True), S("DELAY", "d", duration=1000)]
    _go(h, steps=steps)
    assert [l for l in h.lines if l.startswith("폴링")] == ["폴링 시작", "폴링 중지"]
    assert [a for a, _t in applied] == [False, True, False]
    from v2.ch12.process_actions import ProcessActions
    fake = SimpleNamespace(e=SimpleNamespace(caps=CAPS2))
    t = ProcessActions.polling_targets(fake, True, dict(use_rf_pulse=True, rf_pulse_power=100, use_dc_power=True, dc_power=50))
    assert t == {"mfc": True, "dc_pulse": False, "rf_pulse": True, "dc": True, "dc2": False, "rf": False}
    assert set(ProcessActions.polling_targets(fake, False, {}).values()) == {False}


# ═══════════════════════ 시작 전 단계 — Gate·Main Valve·Chuck(가짜 PLC, 가상 시계) ═══════════════════════
class _PlcEng:
    """process_preflight 함수들이 보는 엔진 자리 — read_bit/write_switch 를 표로 답한다(호출 시각 기록)."""

    def __init__(self, n=1, bits=None, cfg=None):
        from v2.common.worker import Status
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.clock, self.n, self.cfg = self.c, n, dict(cfg or {})
        self.bits = dict(bits or {})
        self.reads, self.writes, self.lines, self.states = [], [], [], []
        ok = Status.OK
        eng = self

        class Plc:
            def read_bit(self, name, on_done):
                eng.reads.append((round(eng.c.monotonic(), 3), name))
                v = eng.bits.get(name, False)
                v = v(eng) if callable(v) else v
                if isinstance(v, Exception):
                    on_done(SimpleNamespace(status="ERROR", value=None, error=str(v), reason="io"))
                else:
                    on_done(SimpleNamespace(status=ok, value=bool(v), error=None, reason=None))

            def write_switch(self, name, on, on_done=None):
                eng.writes.append((round(eng.c.monotonic(), 3), name, bool(on)))
                if on_done:
                    on_done(SimpleNamespace(status=ok, value=None, error=None, reason=None))
        self.plc = Plc()

    def cget(self, k, d=None):
        return self.cfg.get(k, d)

    def log(self, s, m):
        self.lines.append(m)

    def state(self, t):
        self.states.append(t)

    def stage_cancelled(self):
        return False


def test_proc_099_gate_check_states_and_retries():
    """PROC-099: CLOSED(close=T∧open=F)만 통과. OPEN → 'Gate OPEN 상태', 둘 다 T → 'Gate lamp 이상 (OPEN/CLOSE 모두 True)',
    둘 다 F(이동·불명)·읽기 오류는 0.2 s 뒤 재확인 최대 5회 → 'Gate 이동 중 / 상태 불명 (재시도 초과)', 키 없음 → 'PLC 주소맵 키 없음'.
    ('PLC 연결 없음'·'잘못된 CH 번호' 는 v2 에서 PLC 객체·챔버 번호가 늘 있어 생기지 않음.)"""
    from v2.ch12 import process_preflight as PF
    cases = [({"G_V_1_CLOSE_LAMP": True}, (True, "")),
             ({"G_V_1_OPEN_LAMP": True}, (False, "Gate OPEN 상태")),
             ({"G_V_1_OPEN_LAMP": True, "G_V_1_CLOSE_LAMP": True}, (False, "Gate lamp 이상 (OPEN/CLOSE 모두 True)"))]
    for bits, want in cases:
        e = _PlcEng(bits=bits)
        assert PF.gate_check(e) == want
        e.c.close()
    e = _PlcEng(bits={})
    assert PF.gate_check(e) == (False, "Gate 이동 중 / 상태 불명 (재시도 초과)")
    t = [x[0] for x in e.reads if x[1] == "G_V_1_OPEN_LAMP"]
    assert len(t) == 5 and all(b - a == pytest.approx(0.2) for a, b in zip(t, t[1:]))
    assert e.lines[-1] == "[CH1] Gate 상태=moving_or_unknown (OPEN/CLOSE 모두 FALSE) → 시작 차단"
    e.c.close()
    e = _PlcEng(bits={"G_V_1_OPEN_LAMP": RuntimeError("timeout")})
    assert PF.gate_check(e)[1] == "Gate 이동 중 / 상태 불명 (재시도 초과)"
    assert "[CH1] Gate lamp 읽기 실패: RuntimeError: timeout → 재시도 중" in e.lines
    e.c.close()
    e = _PlcEng(n=3)
    ok, why = PF.gate_check(e)
    assert not ok and why.startswith("PLC 주소맵 키 없음 (")
    e.c.close()
    flip = {"n": 0}

    def closed_after_two(eng):
        flip["n"] += 1
        return flip["n"] > 2
    e = _PlcEng(bits={"G_V_1_CLOSE_LAMP": closed_after_two})
    assert PF.gate_check(e) == (True, "")
    e.c.close()


def test_proc_101_main_valve_reasons():
    """PROC-101: M_V_{ch}_SW ∧ M_V_{ch}_인터락 둘 다 T 만 통과(재확인 5회·간격 0.2 s). 사유 3종."""
    from v2.ch12 import process_preflight as PF
    for bits, want in (({"M_V_2_SW": True, "M_V_2_인터락": True}, (True, "")),
                       ({"M_V_2_SW": True}, (False, "Main Valve 인터락 불충족 (벤트 상태이거나 터보/러프펌프/압력 조건 미충족)")),
                       ({"M_V_2_인터락": True}, (False, "Main Valve가 열려있지 않음 (인터락 충족, 밸브 미개방)"))):
        e = _PlcEng(n=2, bits=bits)
        assert PF.main_valve_check(e) == want
        if not want[0]:
            assert len([x for x in e.reads if x[1] == "M_V_2_SW"]) == 5
        e.c.close()
    e = _PlcEng(n=2, bits={"M_V_2_SW": RuntimeError("io")})
    assert PF.main_valve_check(e) == (False, "Main Valve 인터락 불충족 (벤트 상태이거나 터보/러프펌프/압력 조건 미충족)")
    assert "[CH2] Main Valve 읽기 실패: RuntimeError: io → 재시도 중" in e.lines
    e.c.close()


def test_proc_104_chuck_move_sequence_and_timeout():
    """PROC-104: 이미 목표 램프면 생략; 전원 SW ON → 0.2 s → 방향 SW ON → 램프 0.3 s 폴링, 성공·타임아웃 모두 스위치 OFF,
    상태 'Chuck MID 이동 중…'. 타임아웃(CHAMBER_CHUCK_MOVE_TIMEOUT_S) → params chuck_position = 실측, False."""
    from v2.ch12 import process_preflight as PF
    e = _PlcEng(bits={"Z1_UP_LOCATION": True})
    assert PF.chuck_move(e, {"chuck_position": "up"}) and e.writes == []
    e.c.close()
    hits = {"n": 0}

    def arrive(eng):
        hits["n"] += 1
        return hits["n"] >= 4
    e = _PlcEng(bits={"Z1_MID_LOCATION": arrive})
    assert PF.chuck_move(e, {"chuck_position": "MID"}) is True
    assert [(n, on) for _t, n, on in e.writes] == [("Z_M_P_1_SW", True), ("Z_M_P_1_MID_SW", True),
                                                    ("Z_M_P_1_MID_SW", False), ("Z_M_P_1_SW", False)]
    assert e.writes[1][0] - e.writes[0][0] == pytest.approx(0.2)
    polls = [t for t, n in e.reads if n == "Z1_MID_LOCATION"][1:]
    assert all(b - a == pytest.approx(0.3) for a, b in zip(polls, polls[1:])) and e.states == ["Chuck MID 이동 중…"]
    e.c.close()
    e = _PlcEng(bits={"Z1_DOWN_LOCATION": False, "Z1_UP_LOCATION": True}, cfg={"CHAMBER_CHUCK_MOVE_TIMEOUT_S": 3.0})
    p = {"chuck_position": "down"}
    t0 = e.c.monotonic()
    assert PF.chuck_move(e, p) is False and p["chuck_position"] == "up"
    assert e.c.monotonic() - t0 >= 3.0 and [n for _t, n, on in e.writes if not on] == ["Z_M_P_1_CCW_SW", "Z_M_P_1_SW"]
    assert e.lines[-1] == "[CH1] Chuck 'down' 타임아웃(3s) — 실측 위치: up"
    e.c.close()
    e = _PlcEng(bits={"Z1_UP_LOCATION": True, "Z1_MID_LOCATION": True})
    assert PF.read_chuck_pos(e) == "unknown"                                    # 정확히 하나만 T 일 때만
    e.c.close()


# ═══════════════════════ 시험대 — 단일 공정 한 번(CH1 RF Pulse, Chuck mid) ═══════════════════════
S1_P = dict(base_pressure=1e-5, working_pressure=3, process_time=0.1, shutter_delay=0.1, use_ar=True, ar_flow=20,
            use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True, process_note="s1",
            chuck_position="mid")


@pytest.fixture(scope="module")
def s1(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("s1")
    rig = R.make(tmp, chambers=("ch1",))
    e = rig.eng["ch1"]
    seen = {"states": set(), "owners": set(), "status": []}

    def watch(t):
        seen["states"].add(e.state_text)
        if e.busy:
            seen["owners"].add(rig.shared.pulse.owner("rfpulse", "ch1"))
    R.run(rig, R.idle(e), max_ticks=1500, hooks=[R.at(2, lambda: e.start(params=dict(S1_P), owner="local", source="local")),
                                                 watch])
    e.writer.flush(10)
    return SimpleNamespace(rig=rig, e=e, tmp=tmp, **seen)


def test_proc_093_096_097_106_start_path_logs(s1):
    """PROC-093: 이번 런 펄스 선택 = use_*_pulse ∧ 지원(점유는 고른 RF-Pulse 만, 끝나면 해제). PROC-097: 1초마다 '연결 대기 중: …'.
    PROC-106: '장비 연결 확인 완료 → 공정 시작'. GAPB-001: 장비 기동 순서 PLC → MFC → IG → (고른 펄스), PLC 로그는 소유 챔버(CH1)."""
    e = s1.e
    assert s1.owners <= {("ch1", "rfpulse"), None} and ("ch1", "rfpulse") in s1.owners       # 같은 4007 — 종류는 고른 RF-Pulse
    assert s1.rig.shared.pulse.owner("rfpulse", "ch1") is None
    lines = R.logs(e)
    seq = [l.split("] ", 2)[1] for l in lines if "already connected → skip" in l]
    assert seq == ["[CH1:PLC", "[CH1:MFC", "[CH1:IG", "[CH1:RFPulse"]
    assert R.has_log(e, "연결 대기 중: 모두 연결됨") and R.has_log(e, "장비 연결 확인 완료 → 공정 시작")
    i = next(k for k, l in enumerate(lines) if "장비 연결 확인 완료" in l)
    assert i < next(k for k, l in enumerate(lines) if "=== 's1' 공정 시작" in l)


def test_proc_104_105_chuck_in_run_and_result(s1):
    """PROC-104·105(시험대): Chuck 'mid' 이동 성공 로그·상태 문구, 종료 결과의 chuck_position(종료 카드)."""
    e = s1.e
    assert R.has_log(e, "[CH1] Chuck 'mid' 이동 시작: Z_M_P_1_SW → Z_M_P_1_MID_SW → Z1_MID_LOCATION 폴링")
    assert R.has_log(e, "[CH1] Chuck 'mid' 이동 성공 (Z1_MID_LOCATION=True)") and "Chuck MID 이동 중…" in s1.states
    end = [x for x in e.events if x.kind == "run_end"][-1]
    assert end.data["result_detail"]["chuck_position"] == "mid" and "warnings" not in end.data["result_detail"]


def test_proc_172_174_rga_oes_absent_do_not_stop(s1):
    """PROC-172: RGA 어댑터 없음 = 스캔 실패 → '예외로 RGA 스캔 실패: … → 다음 단계'(중단 아님). PROC-174: OES 실패 → 'OES 실패: …',
    채팅 '[OES] 실패: …', 컨트롤러 '오류 무시하고 계속: …' — 공정은 끝까지(done)."""
    e = s1.e
    assert R.has_log(e, "예외로 RGA 스캔 실패: RuntimeError('RGA 어댑터 없음') → 다음 단계")
    assert R.has_log(e, "OES 실패: RuntimeError('OES 링크 없음')") and R.has_log(e, "오류 무시하고 계속: OES 링크 없음")
    assert any(x.kind == "chat" and x.detail == "[OES] 실패: RuntimeError('OES 링크 없음')" for x in e.events)
    assert R.ends(e) == [("done", None)]


def test_proc_182_193_197_210_run_texts(s1):
    """PROC-182: DELAY 상태 문구 '(남은 시간: N초)'. PROC-193: 'Sputter Calib 로그 세션 시작'·'▶️ CH1 's1' 시작 (t=0.1s)'(값은 분 —
    결정 D160)·t0_wall=started_at. PROC-197: 성공이면 오류 없음·팝업 없음. PROC-210: '[Runner] finished → device cleanup'.
    PROC-203: RF Pulse 공정은 Main Shutter 열 때 카메라 참여."""
    e = s1.e
    assert "Power Delay 20초 (남은 시간: 20초)" in s1.states and "Shutter Delay 0.1분 (남은 시간: 6초)" in s1.states
    assert R.has_log(e, "Sputter Calib 로그 세션 시작") and R.has_log(e, "▶️ CH1 's1' 시작 (t=0.1s)")
    st = [x for x in e.events if x.kind == "run_start"][-1].data["params"]
    assert st["t0_wall"] == st["started_at"] and st["process_name"] == st["Process_name"] == st["process_note"] == "s1"
    assert e.error is None and not any(x.kind == "popup" for x in e.events)
    assert R.has_log(e, "[Runner] finished → device cleanup") and R.has_log(e, "[CH1] 카메라 녹화 시작 (mode=")
    assert e.state_text == "대기 중"


def test_proc_209_plc_routing_and_logs(s1):
    """PROC-209·GAPB-004: PLC 단계 '[CH1] 요청: X -> b (raw=…, ch=1)'·'[CH1] 완료: X -> b (N ms)', MV·AR·MS 코일.
    PROC-208: CH1 N2 는 PLC 로 보내지 않고 '[CH1] 'N2' 명령은 무시(N2 라인 없음).'"""
    e = s1.e
    assert R.has_log(e, "[CH1] 요청: MS -> True (raw='MS', ch=1)") and R.has_log(e, "[CH1] 완료: MS -> True (")
    assert R.has_log(e, "[CH1] 'N2' 명령은 무시(N2 라인 없음).")
    addrs = {a for a, _on in R.plc_writes(s1.rig.sims["plc"])}
    assert {R.PM.resolve("MAIN_1_GAS_SW"), R.PM.resolve("Ar_1_GAS_SW"), R.PM.resolve("MAIN_SHUTTER_1_SW")} <= addrs
    assert R.PM.resolve("N2_1_GAS_SW") not in addrs if "N2_1_GAS_SW" in R.PM.COIL_MAP else True


# ═══════════════════════ OES·RGA 단계(엔진 동작 — 가짜 링크) ═══════════════════════
def test_proc_096_174_175_oes_init_and_finished(tmp_path):
    """PROC-096: 시작 때 OES 초기화를 미리(CHAMBER_OES_INIT_TIMEOUT_S 30) — '[OES] init (background) begin/OK/FAIL/EXC'.
    PROC-174: 초기화 실패 → 'OES 실패: …'·채팅·'오류 무시하고 계속'. PROC-175: 이번 런만 처리('이전 런 잔여 'finished' 이벤트 무시'),
    성공 '{msg} (csv=…)', 실패 '측정 실패: {msg} → 종료 절차로 전환' + '워커 stderr tail'(2000자), 다음 런 재초기화. 결정 D159."""
    from v2.ch12.drivers.oes_link import OesResult
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    a = e.actions
    runner = _Runner()
    runner.oes_failed = lambda src, why, code=None, meta=None: runner.calls.append(("oes_failed", src, str(why)))
    a.runner = runner
    box = {}

    class Oes:
        daemon_enabled = False

        def __init__(self, mode):
            self.mode = mode

        def initialize(self, timeout_s, force, on_done):
            box["init_t"] = timeout_s
            if self.mode == "raise":
                raise RuntimeError("포트 없음")
            on_done(self.mode == "ok", None if self.mode == "ok" else "spawn 실패")

        def measure(self, dur, integ, on_done):
            box["measure"] = (dur, integ, on_done)

        def stop(self, wait=False):
            pass
    try:
        t = float(e.cget("CHAMBER_OES_INIT_TIMEOUT_S", 30.0))                 # 코드 기본 30, 운영 설정(config_common) 20
        for mode, want in (("ok", f"[OES] init (background) OK (timeout={t:.1f}s)"),
                           ("fail", f"[OES] init (background) FAIL: spawn 실패 (timeout={t:.1f}s)"),
                           ("raise", "[OES] init (background) EXC: RuntimeError: 포트 없음")):
            a.oes, a.oes_initialized = Oes(mode), False
            a.oes_init_background()
            assert R.has_log(e, "[OES] init (background) begin") and R.has_log(e, want) and box["init_t"] == t
        a.oes, a.oes_initialized = Oes("fail"), False
        a._a_oes_run(S("OES_RUN", "OES", params=(30.0, 60), no_wait=True))
        assert R.has_log(e, "OES 실패: RuntimeError('OES 초기화 실패: spawn 실패')")
        assert runner.calls[-1][0] == "oes_failed"
        a.oes, a.oes_initialized = Oes("ok"), False
        a._a_oes_run(S("OES_RUN", "OES", params=(30.0, 60), no_wait=True))
        assert box["measure"][:2] == (30.0, 60)
        box["measure"][2](OesResult(emitted=True, success=True, message="측정 완료", out_csv="C:/o.csv"))
        assert R.has_log(e, "측정 완료 (csv=C:/o.csv)") and R.has_log(e, "OES 측정 종료(정상).")
        box["measure"][2](OesResult(emitted=True, success=True, message="늦은 것"))
        assert R.has_log(e, "이전 런 잔여 'finished' 이벤트 무시")
        a._a_oes_run(S("OES_RUN", "OES", params=(30.0, 60), no_wait=True))
        box["measure"][2](OesResult(emitted=True, success=False, message="measure failed", error="E" * 2100))
        assert R.has_log(e, "측정 실패: measure failed → 종료 절차로 전환")
        tail = [l for l in R.logs(e) if "워커 stderr tail:" in l][-1]
        assert tail.endswith("E" * 2000 + "...") and not a.oes_initialized
        assert runner.calls[-1] == ("oes_failed", "OES", "measure failed")
    finally:
        _drop(rig)


def test_proc_172_173_rga_failure_and_budget(tmp_path):
    """PROC-172: 워커 실패 '측정 실패: … → 다음 단계' + stderr/stdout 끝 800자 + 채팅 '[RGA{n}] 측정 실패: … → 건너뜀', 예외도 같은 처리 —
    어느 경우든 RGA 단계 완료. PROC-173: 단계 예산 3×60×1000 + 2×1×1000 + 5000 = 187000 ms, 넘으면 '… 예산 187s) → 그래프 스킵,
    다음 단계 진행', 늦은 finished 는 stale 무시."""
    from v2.ch12.drivers.rga_link import step_timeout_ms
    from v2.ch12.process_runner import StepRunner
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    a = e.actions
    runner = _Runner()
    runner.rga_finished = lambda: runner.calls.append(("rga_finished",))
    a.runner = runner
    try:
        a.rga = SimpleNamespace(scan=lambda t, on_done: on_done(SimpleNamespace(ok=False, message="워커 오류",
                                                                                  fail={"stderr": "s" * 900, "stdout": "o"})))
        a._a_rga_scan(S("RGA_SCAN", "RGA"))
        assert R.has_log(e, "측정 실패: 워커 오류 → 다음 단계") and R.has_log(e, "stderr: " + "s" * 800)
        assert not R.has_log(e, "s" * 801) and R.has_log(e, "stdout: o")
        assert any(x.kind == "chat" and x.detail == "[RGA1] 측정 실패: 워커 오류 → 건너뜀" for x in e.events)

        def boom(t, on_done):
            raise RuntimeError("COM 없음")
        a.rga = SimpleNamespace(scan=boom)
        a._a_rga_scan(S("RGA_SCAN", "RGA"))
        assert R.has_log(e, "예외로 RGA 스캔 실패: RuntimeError('COM 없음') → 다음 단계")
        assert runner.calls == [("rga_finished",), ("rga_finished",)]
    finally:
        _drop(rig)
    assert step_timeout_ms({}) == 187000
    h = _Host(delays={"RGA_SCAN": None})
    holder = {}
    r, res = _go(h, steps=[S("RGA_SCAN", "RGA 스캔 및 그래프 출력 대기"), S("DELAY", "다음", duration=1000)],
                 at=200.0, act=lambda r: holder.setdefault("x", r.rga_finished()))
    tr = {x[2]: x[0] for x in r.trace}
    assert tr["DELAY"] - tr["RGA_SCAN"] == pytest.approx(187.0, abs=0.01)
    assert "RGA 스캔 대기 시간 초과(예산 187s) → 그래프 스킵, 다음 단계 진행" in h.lines and res.ok
    h2 = _Host(delays={"RGA_SCAN": None})
    r2 = StepRunner(h2)
    r2.is_running, r2.rga_abandoned = True, True
    r2.rga_finished()
    assert h2.lines == ["[RGA] 예산 초과 후 도착한 finished 무시(stale)"]
    h2.clock.close()


# ═══════════════════════ 시험대 — 시작 전 실패·시작 예외 ═══════════════════════
RFP = dict(base_pressure=1e-5, working_pressure=3, process_time=0.05, shutter_delay=0.05, use_ar=True, ar_flow=20,
           use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True)


def _run1(tmp, hooks_after=None, max_ticks=600, start_at=2, extra=(), **start):
    rig = R.make(tmp, chambers=("ch1",))
    e = rig.eng["ch1"]
    host = []
    e.on_host_start = lambda ok, msg: host.append((ok, msg))
    if hooks_after:
        hooks_after(rig, e)
    p = dict(RFP, **start.pop("p", {}))
    R.run(rig, lambda: not e.busy and any(x.kind == "run_end" for x in e.events), max_ticks=max_ticks,
          hooks=[R.at(start_at, lambda: e.start(params=p, owner="local", source="local", **start))] + list(extra))
    return rig, e, host


def test_proc_097_098_preflight_connect_fail(tmp_path):
    """PROC-097: 펄스를 쓰면 CHAMBER_PREFLIGHT_TIMEOUT_WITH_PULSE_S(10 s) 동안 0.2 s 간격 확인, 1초마다 '연결 대기 중: IG',
    장비별 '연결 확인 실패(타임아웃 10.0s)'. PROC-098: '필수 장비 연결 실패: IG → 시작 중단'·오류 창 '장비 연결 실패'·장비 정리·
    호스트 실패·결과 사유 'preflight connect failed: IG'(runtime_state 오류 래치 문구)."""
    cut = R.at(1, lambda: rig_box["r"].sims["ch1.ig"].add_fault("disconnect", rig_box["r"].clock.monotonic(), 1e9))
    rig_box = {}
    rig, e, host = _run1(tmp_path, lambda r, e: rig_box.setdefault("r", r), start_at=6, extra=[cut])
    t = float(e.cget("CHAMBER_PREFLIGHT_TIMEOUT_WITH_PULSE_S", 10.0))
    assert R.has_log(e, "연결 대기 중: IG") and R.has_log(e, f"연결 확인 실패(타임아웃 {t:.1f}s)")
    assert sum("연결 대기 중: IG" in l for l in R.logs(e)) == int(t) + 1
    assert R.has_log(e, "필수 장비 연결 실패: IG → 시작 중단")
    pop = [x for x in e.events if x.kind == "popup" and x.data.get("title") == "장비 연결 실패"]
    assert pop and pop[0].detail == "다음 장비 연결을 확인하지 못했습니다:\n - IG\n\n케이블/전원/포트 설정 확인 후 재시도"
    end = [x for x in e.events if x.kind == "run_end"][-1]
    assert (end.code, end.detail) == ("PREFLIGHT_FAILED", "preflight connect failed: IG")
    assert host[0] == (False, "장비 연결 실패: IG") and rig.shared.pulse.owner("rfpulse", "ch1") is None
    assert not R.has_log(e, "장비 연결 확인 완료")


def test_proc_100_102_105_interlock_fail_and_chuck_warning(tmp_path):
    """PROC-102: Main Valve 실패 → '[CH1] Main Valve 체크 실패 (…) → 공정 시작 차단'·채팅 E301 'Main Valve 체크 실패: … (공정: …)'·
    결과 'main valve check failed: …'. PROC-105: Chuck 실패는 공정을 막지 않음 '⚠️ Chuck 위치 이동 실패 (target='mid') → 공정은 계속
    진행' + 런 경고(종료 카드), chuck_position = 실측. (Gate 는 test_process_engine::test_gate_open_blocks_start.)"""
    def mv(rig, e):
        e.preflight_hooks["main_valve"] = lambda eng: (False, "Main Valve 상태 불명")
    rig, e, host = _run1(tmp_path / "a", mv, p={"process_note": "MVX"})
    assert R.has_log(e, "[CH1] Main Valve 체크 실패 (Main Valve 상태 불명) → 공정 시작 차단")
    ch = [x for x in e.events if x.kind == "chat" and x.code == "E301"]
    assert ch and ch[0].detail == "Main Valve 체크 실패: Main Valve 상태 불명 → 공정 시작 차단 (공정: MVX)"
    end = [x for x in e.events if x.kind == "run_end"][-1]
    assert (end.code, end.detail) == ("MAIN_VALVE_CHECK_FAILED", "main valve check failed: Main Valve 상태 불명")

    def ck(rig, e):
        def bad(eng, p):
            p["chuck_position"] = "unknown"
            return False
        e.preflight_hooks["chuck"] = bad
    rig, e, host = _run1(tmp_path / "b", ck, max_ticks=900, p={"chuck_position": "MID", "process_note": "CK"})
    assert R.has_log(e, "⚠️ Chuck 위치 이동 실패 (target='mid') → 공정은 계속 진행")
    end = [x for x in e.events if x.kind == "run_end"][-1]
    assert end.data["result"] == "done" and end.data["result_detail"]["warnings"] == ["Chuck 위치 이동 실패 (target='mid')"]
    assert end.data["result_detail"]["chuck_position"] == "unknown"


def test_proc_079_107_start_exception_single(tmp_path):
    """PROC-107: 시작 단계 예외 → "오류: '{note}' 시작 실패. ({e})" 로그·오류 창 '오류'·호스트 실패 응답·장비 정리(점유 해제).
    PROC-079: 그 뒤 '[Runner] START_SINGLE failed: …'·결과 '시작 실패: …'(Robot CSV '실패' 사유)·IDLE."""
    def boom(rig, e):
        def g(eng):
            raise RuntimeError("PLC 객체 없음")
        e.preflight_hooks["gate"] = g
    rig, e, host = _run1(tmp_path, boom, p={"process_note": "EXC"})
    msg = "오류: 'EXC' 시작 실패. (PLC 객체 없음)"
    assert R.has_log(e, msg) and any(x.kind == "popup" and x.data.get("title") == "오류" and x.detail == msg for x in e.events)
    assert (False, msg) in host and R.has_log(e, f"[Runner] START_SINGLE failed: RuntimeError({msg!r})")
    end = [x for x in e.events if x.kind == "run_end"][-1]
    assert (end.code, end.detail) == ("PREFLIGHT_FAILED", f"시작 실패: {msg}")
    assert not e.busy and e.state_name == "IDLE" and rig.shared.pulse.owner("rfpulse", "ch1") is None


ROW = {"Process_name": "A", "Ar": "T", "Ar_flow": "20", "base_pressure": "1e-5", "working_pressure": "3",
       "shutter_delay": "0.05", "process_time": "0.05", "use_rf_pulse": "T", "rf_pulse_power": "120",
       "rf_pulse_freq": "0.5", "rf_pulse_duty_cycle": "40", "main_shutter": "T"}


def test_proc_090_queue_start_exception(tmp_path):
    """PROC-090: 큐 행 시작 예외 → '[Runner] ADVANCE_QUEUE failed: …'·호스트 실패 '공정 시작 실패: …'·큐 중단·장비 정리·리셋."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    host = []
    e.on_host_start = lambda ok, msg: host.append((ok, msg))

    def g(eng):
        raise RuntimeError("x")
    e.preflight_hooks["gate"] = g
    assert e.load([dict(ROW, Process_name="A"), dict(ROW, Process_name="B")], source="local") is None
    R.run(rig, R.idle(e), max_ticks=300, hooks=[R.at(2, lambda: e.start(source="local"))])
    msg = "오류: 'A' 시작 실패. (x)"
    assert R.has_log(e, f"[Runner] ADVANCE_QUEUE failed: RuntimeError({msg!r})") and (False, f"공정 시작 실패: {msg}") in host
    assert [x.detail for x in e.events if x.kind == "run_end"] == [f"공정 시작 실패: {msg}"]
    assert not R.has_log(e, "자동 공정 (2/2)") and e.rows is None and not e.busy


def test_proc_067_083_088_089_queue_rows(tmp_path):
    """PROC-067 '[CH1] 파일 기반 자동 공정 시작'. PROC-083 '[CH1] 자동 공정 (i/N) 준비: '이름''. PROC-088 행마다 새 공정 txt.
    PROC-089 행 사이 쿨다운 '[Runner] cooldown wait: X.Xs (쿨다운 대기)'·'다음 공정 대기중 (쿨다운 대기) · 남은 시간 HH:MM:SS'.
    PROC-081·082 '[Runner] queue advance (ok)'·'[Runner] queue finished → reset'. PROC-091 큐 종료 → 비움(다음 Start 는 단일).
    PROC-192 run_start 에 process_index·process_total(시작 카드 '(i/N)')."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    seen = set()
    assert e.load([dict(ROW, Process_name="A"), dict(ROW, Process_name="B")], source="local") is None
    R.run(rig, R.idle(e), max_ticks=1500, hooks=[R.at(2, lambda: e.start(source="local")),
                                                 lambda t: seen.add(e.state_text)])
    assert R.has_log(e, "[CH1] 파일 기반 자동 공정 시작") and R.has_log(e, "[CH1] 자동 공정 (1/2) 준비: 'A'")
    assert R.has_log(e, "[CH1] 자동 공정 (2/2) 준비: 'B'") and R.has_log(e, "[Runner] queue advance (ok)")
    assert R.has_log(e, "[Runner] queue finished → reset") and R.has_log(e, "[Runner] cooldown wait: ")
    assert any(s.startswith("다음 공정 대기중 (쿨다운 대기) · 남은 시간 00:00:") for s in seen)
    files = sorted((tmp_path / "logs" / "CH1").glob("CH1_*.txt"))
    assert [f.name.split("_")[1] for f in files] == ["A", "B"]
    st = [x.data for x in e.events if x.kind == "run_start"]
    assert [(d["process_index"], d["process_total"]) for d in st] == [(1, 2), (2, 2)]
    assert e.rows is None and e.runs == [] and R.ends(e) == [("done", None), ("done", None)]


# ═══════════════════════ 시험대 — STOP·이상 중단 끝 처리 ═══════════════════════
def test_proc_149_151_176_194_stop_during_main(tmp_path):
    """PROC-148·149: 챔버 STOP = Runner STOP 하나('[Runner] STOP 요청(user=True) state=RUNNING'), 큐 진행 끊기.
    PROC-151: 공정 중이면 상태 'STOP 요청 중... (공정 종료 대기)' + 실행기 request_stop 만(정리는 끝 처리에서).
    PROC-176: 종료 절차 '[종료절차 i/N]'. PROC-194: Calib CSV 'Sputter Calib CSV 기록 요청 (ok=False, result=stop)'.
    PROC-197: STOP 이면 오류 해제(팝업 없음)."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    seen = set()
    main = lambda: e.cur_inst is not None and e.cur_inst.type == "main" and e.runner.cd is not None   # noqa: E731
    R.run(rig, R.idle(e), max_ticks=900, hooks=[R.at(2, lambda: e.start(params=dict(RFP, process_time=1.0), owner="local")),
                                                R.when(main, lambda: e.stop(source="local")),
                                                lambda t: seen.add(e.state_text)])
    assert R.has_log(e, "[Runner] STOP 요청(user=True) state=RUNNING") and "STOP 요청 중... (공정 종료 대기)" in seen
    assert R.has_log(e, "[종료절차 1/") and R.has_log(e, "Sputter Calib CSV 기록 요청 (ok=False, result=stop)")
    lines = R.logs(e)
    assert next(i for i, l in enumerate(lines) if "[종료절차 1/" in l) < \
        next(i for i, l in enumerate(lines) if "[Runner] finished → device cleanup" in l)
    assert R.ends(e) == [("aborted", "USER_STOP")] and e.error is None and not any(x.kind == "popup" for x in e.events)


def test_proc_195_197_failure_popup_and_reason(tmp_path):
    """PROC-195: 진짜 실패(not ok ∧ not stopped)면 채팅 '❌ CH1 공정 실패 이유: {errors[0]}'. PROC-197: 실패면 오류 래치(사유 코드)·
    오류 창 'CH1 공정 실패' '{이름}\\n\\n사유: …\\n\\n확인을 누르면 상태 표시가 Idle로 변경됩니다.'."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    gas = lambda: e.cur_inst is not None and e.cur_inst.type == "pressure_ctrl"                       # noqa: E731
    R.run(rig, R.idle(e), max_ticks=900, hooks=[R.at(2, lambda: e.start(params=dict(RFP, process_note="F1"), owner="local")),
                                                R.when(gas, lambda: e.abort("E401", "PLC 쓰기 실패", source="PLC"))])
    end = [x for x in e.events if x.kind == "run_end"][-1]
    first = end.data["result_detail"]["errors"][0]
    assert first.endswith("[PLC - E401: PLC 쓰기 실패]")
    assert any(x.kind == "chat" and x.detail == f"❌ CH1 공정 실패 이유: {first}" for x in e.events)
    pop = [x for x in e.events if x.kind == "popup" and x.data.get("title") == "CH1 공정 실패"]
    assert pop and pop[0].detail == f"F1\n\n사유: {first}\n\n확인을 누르면 상태 표시가 Idle로 변경됩니다."
    assert e.error == "E401" and R.has_log(e, "Sputter Calib CSV 기록 요청 (ok=False, result=실패)")


# ═══════════════════════ 그 밖 엔진 단위 ═══════════════════════
def test_proc_019_name_keys_and_untitled(tmp_path):
    """PROC-019: 공정 이름 → process_name·Process_name·process_note 같은 값, 비면 'Untitled'(컨트롤러가 먼저 — 'Run CHn' 은 v1 에서
    실제로 나오지 않음)."""
    from v2.ch12.process_runner import StepRunner
    h = _Host()
    r = StepRunner(h)
    out = {}
    p = {"Process_name": "  "}
    h.clock.start_thread(lambda: out.setdefault("r", r.run(p, [S("DELAY", "a", duration=100)])), "run")
    h.clock.wait_until(h.clock.monotonic() + 10, lambda: "r" in out)
    h.clock.close()
    assert p["process_name"] == p["Process_name"] == p["process_note"] == "Untitled"
    assert "=== 'Untitled' 공정 시작 (총 1단계) ===" in h.lines
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    try:
        q = {}
        e.run_id = "x"
        e._run_started(q)
        assert q["process_name"] == q["Process_name"] == q["process_note"] == "Untitled"
        assert R.has_log(e, "▶️ CH1 'Untitled' 시작 (t=0.0s)")
    finally:
        _drop(rig)


def test_proc_055_queue_row_param_error(tmp_path):
    """PROC-055: 큐 행 시작 전 검사 실패 → 'CSV 공정 파라미터 오류:\\n - …' 로그, 호스트 실패, 큐 자동 진행 중단, 결과
    RECIPE_PARAM_ERROR(Robot CSV '레시피 파라미터 오류: …'), IDLE. 다음 행은 시작하지 않는다."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    host = []
    e.on_host_start = lambda ok, msg: host.append((ok, msg))
    bad = dict(ROW, Process_name="BAD", use_rf_pulse="F", rf_pulse_power="", rf_pulse_freq="", rf_pulse_duty_cycle="")
    assert e.load([bad, dict(ROW, Process_name="B")], source="local") is None
    R.run(rig, R.idle(e), max_ticks=300, hooks=[R.at(2, lambda: e.start(source="local"))])
    msg = "CH1은 RF Pulse 또는 DC Pulse 중 하나를 반드시 선택해야 합니다."
    assert R.has_log(e, "CSV 공정 파라미터 오류:\n - " + msg) and (False, "CSV 공정 파라미터 오류: " + msg) in host
    assert R.ends(e) == [("error", "RECIPE_PARAM_ERROR")] and not R.has_log(e, "자동 공정 (2/2)")
    assert not e.busy and e.rows is None


def test_proc_063_066_064_guard_popups(tmp_path):
    """PROC-063: Runner 가 IDLE 아님 → 'runner busy: {state}' + 경고 '대기 중'. PROC-066: 컨트롤러 실행 중 → 'process controller busy'
    + '실행 오류' '다른 공정이 실행 중입니다.'. PROC-064: v2 의 챔버 실행 표시는 엔진 상태라 같은 거부(RUNNER_BUSY)로."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    try:
        e.state_name = "PREFLIGHT"
        assert e.start_guard() == "RUNNER_BUSY" and e.reject_detail == "runner busy: PREFLIGHT"
        e.state_name = "IDLE"
        e.runner.is_running = True
        assert e.start_guard() == "RUNNER_BUSY" and e.reject_detail == "process controller busy"
        e.runner.is_running = False
        pops = [(x.data.get("title"), x.detail) for x in e.events if x.kind == "popup"]
        assert pops == [("대기 중", "이전 공정/정리가 아직 진행 중입니다. 잠시 후 다시 시도하세요."),
                        ("실행 오류", "다른 공정이 실행 중입니다.")]
        assert e.start_guard() is None
    finally:
        _drop(rig)


def test_proc_068_start_exception_in_core(tmp_path):
    """PROC-068: 시작 처리 예외 → '_handle_start_clicked 예외:\\n{traceback}' 로그, 호스트 'exception: …', 오류 창 '실행 오류'
    '공정 시작 준비 중 내부 오류가 발생했습니다.\\n로그를 확인하세요.'."""
    rig = _full(tmp_path)
    try:
        e = rig.asm.proc["ch1"]
        got = []

        def boom(**kw):
            raise ValueError("깨진 값")
        e.start = boom
        from v2.ch12.core import Rejected
        with pytest.raises(Rejected) as ei:
            rig.asm.core.start_chamber(1, params={}, host_cb=lambda ok, code, d: got.append((ok, d)))
        assert ei.value.reason == "INTERNAL_ERROR" and got == [(False, "exception: ValueError('깨진 값')")]
        assert R.has_log(e, "_handle_start_clicked 예외:\nTraceback (most recent call last):")
        assert any(x.kind == "popup" and x.data.get("title") == "실행 오류"
                   and x.detail == "공정 시작 준비 중 내부 오류가 발생했습니다.\n로그를 확인하세요." for x in e.events)
    finally:
        _close(rig)


def test_proc_069_host_start_wait_timeout():
    """PROC-069: 호스트 시작은 시작 가드 결과를 CHAMBER_HOST_START_WAIT_TIMEOUT_S 동안 기다리고, 넘으면
    'preflight timeout (X.Xs) (start guard 또는 내부 대기로 인해 프리플라이트 미도달)'(→ E700)."""
    from v2.ch12.recipe_check import host_error_code
    from v2.ch12.robot_status import StatusOps
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    me = SimpleNamespace(clock=c, cfg={"CHAMBER_HOST_START_WAIT_TIMEOUT_S": 20.0}, request_meta=lambda k: {"key": k},
                         engines=SimpleNamespace(start_sputter=lambda ch, recipe, meta, box: None))
    try:
        t0 = c.monotonic()
        with pytest.raises(RuntimeError) as ei:
            StatusOps._wait_chamber_start(me, 1, "a.csv", "k")
        assert c.monotonic() - t0 == pytest.approx(20.0)
        assert str(ei.value) == "preflight timeout (20.0s) (start guard 또는 내부 대기로 인해 프리플라이트 미도달)"
        assert host_error_code(str(ei.value)) == "E700"
    finally:
        c.close()


def test_proc_070_086_test_row_and_delay_signals(tmp_path):
    """PROC-070: 호스트 '시작 성공' 시점 — TEST 행 'TEST MODE: {time}', delay 행은 대기 진입 'delay accepted: {label}', 공정 행은
    'preflight OK'(s1·시험대). PROC-086: TEST 행 = 장비 제어·프리플라이트 없이 DELAY 1단계 'TEST MODE (N s)' + 컨트롤러 로그
    '[TEST MODE] 장비제어 스킵 / 시뮬레이션 X.Xs 진행', 러너 로그 '[TEST MODE] '{note}' 장비 제어 스킵 / X.Xs 시뮬레이션'."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    host = []
    e.on_host_start = lambda ok, msg: host.append((ok, msg))
    assert e.load([{"Process_name": "w", "delay": "3s"}, {"#": "test", "Process_name": "T1", "time": "4s"}], source="local") is None
    R.run(rig, R.idle(e), max_ticks=300, hooks=[R.at(2, lambda: e.start(source="local"))])
    assert host[:2] == [(True, "delay accepted: 3s"), (True, "TEST MODE: 4s")]
    assert R.has_log(e, "[TEST MODE] 장비제어 스킵 / 시뮬레이션 4.0s 진행") and R.has_log(e, "[TEST MODE] 'T1' 장비 제어 스킵 / 4.0s 시뮬레이션")
    assert [a for _t, _i, a, m, _sd in e.traces[-1]] == ["DELAY"] and e.traces[-1][0][3] == "TEST MODE (4s)"
    assert not R.has_log(e, "연결 대기 중") and R.ends(e) == [("done", None)]


def test_proc_072_presputter_ui_start(tmp_path):
    """PROC-072: Pre-Sputter 'UI 현재값' 시작 — 공정 중이면 '[CHn] PreSputter: 이미 공정 중입니다.' False, 아니면 버튼과 같은 경로
    (origin=presputter) '[CHn] PreSputter 자동 시작 (UI 현재값)' True."""
    rig = _full(tmp_path)
    try:
        core, e = rig.asm.core, rig.asm.proc["ch1"]
        core.last_ui_params["ch1"] = dict(RFP)
        assert core.presputter_start_ui("ch1") is True
        assert R.has_log(e, "[CH1] PreSputter 자동 시작 (UI 현재값)") and e.origin == "presputter"
        rig.loop.run(30)
        assert e.is_running
        assert core.presputter_start_ui("ch1") is False and R.has_log(e, "[CH1] PreSputter: 이미 공정 중입니다.")
        e.stop(source="local")
        rig.loop.run(60)
    finally:
        _close(rig)


def test_proc_074_075_076_080_one_runner(tmp_path):
    """PROC-074·075·080: 챔버당 러너 스레드 하나('Runner.CH1'), 시작 명령은 한 칸 — 진행 중 START 는 거부(RUNNER_BUSY)되어 중복 실행·
    세대 경쟁이 생기지 않는다. STOP 은 어느 상태에서나 받는다. PROC-076: 상태 전이 PREFLIGHT → RUNNING → CLEANUP → IDLE(큐 행 사이 COOLDOWN)."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    seen, got = [], {}

    def watch(t):
        if not seen or seen[-1] != e.state_name:
            seen.append(e.state_name)
    R.run(rig, R.idle(e), max_ticks=900,
          hooks=[R.at(2, lambda: e.start(params=dict(RFP), owner="local")), watch,
                 R.at(4, lambda: got.setdefault("second", e.start(params=dict(RFP), owner="local"))),
                 R.at(5, lambda: got.setdefault("thr", [t.name for t in threading.enumerate() if t.name.startswith("Runner.")]))])
    assert got["second"] == "RUNNER_BUSY" and got["thr"] == ["Runner.CH1"]
    assert seen[:4] == ["IDLE", "PREFLIGHT", "RUNNING", "CLEANUP"] and seen[-1] == "IDLE"
    assert len([x for x in e.events if x.kind == "run_start"]) == 1


def test_proc_119_139_140_step_helpers():
    """PROC-119: 미지원 전원 요청 안내 5종. PROC-139: PLC 이름 정규화(별칭). PROC-140: 단계 기준 use_* = 플래그 ∧ 파워>0 ∧ 지원,
    지원 기본(DC 연속 CH2·DC2 False(히터와 배타)·RF 연속 CH2 강제·DC Pulse CH1·RF Pulse CH2)·레거시 키."""
    info = {"req_dc": True, "use_dc": False, "req_dc2": True, "use_dc2": False, "req_rf": True, "use_rf": False,
            "req_dcp": True, "use_dc_pulse": False, "req_rfp": True, "use_rf_pulse": False}
    assert ps.unsupported_logs(info) == ["주의: 이 챔버는 DC 연속 파워 미지원 → DC 단계 스킵",
                                         "주의: 이 챔버는 DC2 연속 파워 미지원 → DC2 단계 스킵",
                                         "주의: 이 챔버는 RF 연속 파워 미지원 → RF 단계 스킵",
                                         "주의: 이 챔버는 DC Pulse 미지원 → Pulse 단계 스킵",
                                         "주의: 이 챔버는 RF Pulse 미지원 → Pulse 단계 스킵"]
    assert [ps.norm_plc_name(x) for x in (" argon", "Ar_2", "main valve", "MAIN_SHUTTER", "g1", "SW_RF_SELECT")] == \
        ["AR", "AR", "MV", "MS", "G1", "SW_RF_SELECT"]
    c1, c2 = ps.caps_from_cfg({}, 1), ps.caps_from_cfg({}, 2)
    assert (c1.dc, c1.dc2, c1.rf, c1.dcp, c1.rfp) == (False, False, False, True, False)
    assert (c2.dc, c2.dc2, c2.rf, c2.dcp, c2.rfp) == (True, False, True, False, True)
    assert ps.caps_from_cfg({"SUPPORTS_DCPULSE": True, "SUPPORTS_RF_PULSE": True, "SUPPORTS_DC": False}, 1).rfp is True
    assert ps.caps_from_cfg({"CH2_SUPPORTS_DC2": True}, 2).dc2 and not ps.caps_from_cfg(
        {"CH2_SUPPORTS_DC2": True, "CH2_SUPPORTS_HEATER": True}, 2).dc2
    i = ps.common_info(dict(use_dc_power=True, dc_power=0, use_rf_power=True, rf_power=10), ps.Caps(dc=True, dc2=False, rf=False,
                                                                                                      dcp=False, rfp=False))
    assert (i["use_dc"], i["use_rf"], i["req_rf"]) == (False, False, True)


def test_proc_147_next_step_message(tmp_path):
    """PROC-147: next_step_message = 다음 단계 문구(없으면 '') — 히터 도달 카드의 '다음 단계'."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    try:
        e.runner.steps = [S("DELAY", "a", duration=1), S("DELAY", "다음 것", duration=1)]
        e.runner.idx = 0
        assert e._next_step_message() == "다음 것"
        e.runner.idx = 1
        assert e._next_step_message() == ""
    finally:
        _drop(rig)


def test_proc_152_stop_before_start_texts(tmp_path):
    """PROC-152: 공정 시작 전 STOP → 즉시 정리 '[Runner] STOP → 공정 시작 전 상태, 즉시 정리(cleanup)', 결과 '사용자 STOP (시작 전)'
    (Robot CSV STOP 사유), 큐·화면 리셋, IDLE."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    e.load([{"Process_name": "delay 2m"}, dict(ROW)], source="local")
    R.run(rig, R.idle(e), max_ticks=300, hooks=[R.at(2, lambda: e.start(source="local")), R.at(10, lambda: e.stop(source="local"))])
    end = [x for x in e.events if x.kind == "run_end"][-1]
    assert (end.code, end.detail, end.data["result"]) == ("USER_STOP", "사용자 STOP (시작 전)", "aborted")
    assert R.has_log(e, "[Runner] STOP 요청(user=True) state=COOLDOWN") and e.rows is None and e.state_text == "대기 중"


def test_proc_155_shutdown_ms_values():
    """PROC-155: 종료 절차 대기값은 실행 시점 설정 — 숫자 아님·bool·음수·NaN·inf 면 기본."""
    from v2.ch12.process_tokens import _ms
    for bad in ("x", True, -1, float("nan"), float("inf"), None):
        assert _ms({"SHUTDOWN_STEP_GAP_MS": bad}, "SHUTDOWN_STEP_GAP_MS", 500) == 500.0
    assert _ms({"SHUTDOWN_STEP_GAP_MS": "250"}, "SHUTDOWN_STEP_GAP_MS", 500) == 250.0 and _ms({}, "X", 2500) == 2500.0


def test_proc_163_plc_failure_meta_and_log(tmp_path):
    """PROC-163: PLC 명령 실패는 현재 단계와 무관하게 늘 중단 — meta cmd/raw_cmd/ch/requested_on/op/addr, 로그 '명령 실패: {raw} -> {on}: {e}'.
    PROC-208: CH1 G1~G3·N2 는 보내지 않고 바로 확인, CH1 MFC N2 요청 '[CH1] N2 요청 무시 (라인 없음)'."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    a = e.actions
    seen = []

    class Run(_Runner):
        def step_failed(self, src, why, code=None, meta=None, **kw):
            seen.append((src, str(why), code, meta))
    a.runner = Run()
    try:
        a.plc = SimpleNamespace(write_switch=lambda name, on, on_done: on_done(SimpleNamespace(status="ERROR", error="timeout",
                                                                                                 reason="io", code="E401")))
        a._a_plc_cmd(S("PLC_CMD", "메인 밸브 열기", params=("mv", True)))
        assert seen == [("PLC", "timeout", "E401", {"cmd": "MV", "raw_cmd": "mv", "ch": 1, "requested_on": True, "op": None,
                                                    "addr": None})]
        assert R.has_log(e, "명령 실패: mv -> True: 'timeout'")
        a._a_plc_cmd(S("PLC_CMD", "Gun Shutter G2 열기", params=("G2", True)))
        assert R.has_log(e, "[CH1] 'G2' 명령은 무시(건 셔터 없음).") and a.runner.calls[-1] == ("match", "PLC", "G2")
        a._a_mfc_cmd(S("MFC_CMD", "Ch3(N2) Flow Off", params=("FLOW_OFF", {"channel": 3, "gas": "N2"})))
        assert R.has_log(e, "[CH1] N2 요청 무시 (라인 없음)") and a.runner.calls[-1] == ("match", "MFC", "FLOW_OFF")
    finally:
        _drop(rig)


def test_proc_185_gapb005_events_always_delivered(s1):
    """PROC-185·GAPB-005(v2 구조): 장비 이벤트는 system.event_listeners 로 상시 받는다(큐·펌프 태스크 없음 — 넘쳐 버리거나 공정 끝
    정리에 같이 취소될 일이 없음). 처리 예외는 '장비 이벤트 처리 예외: …' 한 줄로 남기고 다음 이벤트를 계속 받는다."""
    e = s1.e
    assert not e.busy and s1.rig.eng["ch1"] is e                               # 공정이 끝난 뒤에도 같은 처리기가 받는다
    n0 = len(e.stats.flows.get("Ar", [])) if hasattr(e.stats, "flows") else 0
    e.actions.on_event(SimpleNamespace(source="ch1.dcpulse", kind="telemetry", event=SimpleNamespace(data=5)))
    assert R.has_log(e, "장비 이벤트 처리 예외: ") and R.has_log(e, "(src=ch1.dcpulse, kind=telemetry)")
    for i in range(2500):
        e.actions.on_event(SimpleNamespace(source="ch1.mfc", kind="flow", event=SimpleNamespace(gas="Ar", value=float(i), owner=None)))
    assert R.has_log(e, "[poll] Ar: 2499.00 sccm") and n0 >= 0


def test_proc_196_198_200_shared_mfc_cleanup(tmp_path):
    """PROC-198: 공정 상태 True → runtime_state.begin_run_use(chamber{n}, [MFC 자원 키]), False → end_run_use(+ 펄스 점유 해제).
    PROC-196·200: 다른 사용자(폴링 사용자 ∪ 런 사용자 − 나)가 있으면 전체 정리 대신 이 챔버 몫만(release_owner) —
    'finished' 처리 '… → mfc on_process_finished 생략(공유 자원 보호), 이 챔버 몫만 정리'·장비 정리 '… → mfc 폴링/상태 리셋 생략…'."""
    from v2.ch12.run_state import RunState
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    rig.run_state = RunState(rig.clock)
    calls = []
    m = e.actions.mfc
    m.release_owner = lambda owner, reason="": calls.append(("release_owner", owner, reason))
    m.on_process_finished = lambda ok, reason=None: calls.append(("finished", ok))
    m.on_process_cleanup = lambda: calls.append(("cleanup",))
    users = []

    def watch(t):
        if e.state_name == "RUNNING" and not users:
            users.append(set(rig.run_state.run_users("MFC1")))
            rig.run_state.begin_run_use("pc1", ["MFC1"])                     # 같은 MFC 를 PC 가 같이 씀
    R.run(rig, R.idle(e), max_ticks=900, hooks=[R.at(2, lambda: e.start(params=dict(RFP), owner="local")), watch])
    assert users == [{"chamber1"}] and rig.run_state.run_users("MFC1") == {"pc1"}
    assert ("release_owner", "chamber1", "finished") in calls and ("release_owner", "chamber1", "cleanup") in calls
    assert ("finished", True) not in calls and ("cleanup",) not in calls
    assert R.has_log(e, "다른 공정이 MFC 사용 중(pc1) → mfc on_process_finished 생략(공유 자원 보호), 이 챔버 몫만 정리")
    assert R.has_log(e, "다른 공정이 MFC 사용 중(pc1) → mfc 폴링/상태 리셋 생략(공유 자원 보호), 이 챔버 몫만 정리")


def test_gapb_005_listener_lives_until_close(tmp_path):
    """GAPB-005: 엔진의 장비 이벤트 처리기는 만들 때 붙고 close() 때만 떨어진다(공정 끝 정리와 무관)."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    assert e.actions.on_event in rig.event_listeners
    e.actions.cleanup_devices()
    assert e.actions.on_event in rig.event_listeners
    _drop(rig)
    assert e.actions.on_event not in rig.event_listeners


def test_gapb_001_device_start_order_and_plc_log_owner(tmp_path):
    """GAPB-001: 장비 기동 순서 PLC → MFC → IG → (고른 펄스). 이미 연결이면 '{라벨} already connected → skip', PLC 는 소유 챔버(CH1)만 로그.
    v2 는 worker 가 상시 연결·재연결하므로 start 호출의 시간 제한(20 s)·'메서드 없음' 경우가 없다(연결 확인은 PROC-097)."""
    from v2.ch12 import process_preflight as PF
    rig = R.make(tmp_path, chambers=("ch1", "ch2"))
    try:
        for ch in ("ch1", "ch2"):
            e = rig.eng[ch]
            PF.start_devices(e, {"dcpulse": ch == "ch2"})
        l1 = [l for l in R.logs(rig.eng["ch1"]) if "→ skip" in l or "start 호출 완료" in l]
        l2 = [l for l in R.logs(rig.eng["ch2"]) if "→ skip" in l or "start 호출 완료" in l]
        assert [x.split("] ", 2)[1] for x in l1] == ["[CH1:PLC", "[CH1:MFC", "[CH1:IG"]
        assert [x.split("] ", 2)[1] for x in l2] == ["[CH2:MFC", "[CH2:IG", "[CH2:DCPulse"]          # CH2 는 PLC 로그 없음
        assert l1[0].endswith("start 호출 완료") or l1[0].endswith("already connected → skip")
    finally:
        _drop(rig)


def test_gapb_002_supports_flags_fixed_until_restart(tmp_path):
    """GAPB-002(v2): SUPPORTS_* 는 설치값(재시작 뒤 적용) — 실행 중 설정 적용은 엔진 지원 플래그(caps)를 바꾸지 않는다(결정 후보:
    v1 은 IDLE 일 때 OFF 를 바로, 장비 객체가 있을 때만 ON 을 바로 반영)."""
    from v2.ch12.params import table
    from v2.common.paramdef import Apply
    t = {p.key: p for p in table().params}
    for k in ("SUPPORTS_DC", "SUPPORTS_DC2", "SUPPORTS_RF_CONT", "SUPPORTS_DCPULSE", "SUPPORTS_RFPULSE", "CH2_SUPPORTS_DC2",
              "CH2_SUPPORTS_RF_CONT", "CH2_SUPPORTS_DCPULSE"):
        assert t[k].apply is Apply.RESTART, k
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    try:
        before = e.caps
        rig.cfg["CH1_SUPPORTS_RFPULSE"] = False
        assert e.caps == before and e.caps.rfp
    finally:
        _drop(rig)


def test_gapb_003_actual_pulse_params_written_back(tmp_path):
    """GAPB-003: 펄스 시작 성공 뒤 실측 되읽기를 기록 params 에 덮어씀 — DC Pulse dc_pulse_freq·dc_pulse_duty(·_cycle)·
    dc_pulse_off_time_us·dc_pulse_off_time(None → 'DC') + '[실측] f=…kHz, off=…us|DC, duty=…%', RF Pulse 는 freq·duty·off 만(로그 없음),
    되읽기 실패·빈 결과는 조용히 무시."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    a = e.actions
    a.runner = _Runner()
    try:
        for pr, off_txt in (({"freq_khz": 50, "duty_pct": 80.0, "off_time_us": 4.0}, "4.0us"),
                            ({"freq_khz": 50, "duty_pct": 100.0, "off_time_us": None}, "DC")):
            a.dcp = SimpleNamespace(prepare_and_start=lambda p, freq, off_time_us, on_done: on_done(None, True),
                                    read_actual_pulse_params=lambda got, pr=pr: got(None, pr))
            a._a_dc_pulse_start(S("DC_PULSE_START", "DC", value=60.0, params=(50, None)))
            pp = e.stats.params
            assert (pp["dc_pulse_freq"], pp["dc_pulse_duty"], pp["dc_pulse_duty_cycle"]) == (50, pr["duty_pct"], pr["duty_pct"])
            assert pp["dc_pulse_off_time_us"] == pr["off_time_us"] and pp["dc_pulse_off_time"] == (pr["off_time_us"] or "DC")
            assert R.has_log(e, f"[실측] f=50kHz, off={off_txt}, duty={pr['duty_pct']}%")
        n = len(R.logs(e))
        a.rfp = SimpleNamespace(start_pulse_process=lambda p, f, d, on_done: on_done(None, True),
                                read_actual_pulse_params=lambda got: got(None, {"freq_khz": 0.5, "duty_pct": 40, "off_time_us": 1200.0}))
        a._a_rf_pulse_start(S("RF_PULSE_START", "RF", value=120.0, params=(500, 40)))
        pp = e.stats.params
        assert (pp["rf_pulse_freq"], pp["rf_pulse_duty_cycle"], pp["rf_pulse_off_time_us"]) == (0.5, 40, 1200.0)
        assert not [l for l in R.logs(e)[n:] if "[실측]" in l]
        a.dcp = SimpleNamespace(prepare_and_start=lambda p, freq, off_time_us, on_done: on_done(None, True),
                                read_actual_pulse_params=lambda got: got(None, None))
        pp["dc_pulse_freq"] = "keep"
        a._a_dc_pulse_start(S("DC_PULSE_START", "DC", value=60.0, params=(50, None)))
        assert pp["dc_pulse_freq"] == "keep" and a.runner.calls == []
    finally:
        _drop(rig)


def test_proc_205_206_run_state():
    """PROC-205: runtime_state — kind chamber/pc/tsp, 채널 1·2(tsp 0), mark_started 때 직전 오류 해제·시작 시각, mark_finished 종료 시각,
    오류 래치. PROC-206: remaining_cooldown = 60 − (지금 − 마지막 종료)(여러 채널이면 최대). 챔버 시작·행 사이는 자기 챔버 종료만 본다
    (엔진 last_end_t — test_process_list::test_delay_test_and_cooldown_rows)."""
    from v2.ch12.run_state import RunState
    c = SimpleNamespace(t=100.0)
    c.monotonic = lambda: c.t
    rs = RunState(c)
    rs.set_error("chamber", 1, "gate check failed: Gate OPEN 상태")
    assert rs.has_error("chamber", 1) and rs.last_error("chamber", 1) == "gate check failed: Gate OPEN 상태"
    rs.mark_started("chamber", 1)
    assert rs.is_running("chamber", 1) and not rs.has_error("chamber", 1)
    rs.mark_finished("chamber", 1)
    c.t = 110.0
    rs.mark_finished("pc", 2)
    c.t = 130.0
    assert rs.remaining_cooldown("chamber", 1, 60.0) == 30.0 and rs.remaining_cooldown("pc", [1, 2], 60.0) == 40.0
    assert rs.remaining_cooldown("chamber", 2, 60.0) == 0.0
    rs.mark_started("tsp")
    assert rs.is_running("tsp") and rs.any_running()


def test_proc_203_camera_rf_only(tmp_path):
    """PROC-203: Main Shutter 열 때 RF 연속·RF Pulse 공정만 카메라 참여, 아니면 '[CH{n}] RF 미사용 공정 → 카메라 건너뜀';
    닫을 때 떠남('카메라 녹화 정지' 또는 '카메라 사용 종료 — 다른 공정이 계속 촬영 중')."""
    rig = R.make(tmp_path, chambers=("ch2",))
    e = rig.eng["ch2"]
    a = e.actions
    try:
        e.params = {"use_dc_pulse": True}
        a._camera(True)
        assert R.has_log(e, "[CH2] RF 미사용 공정 → 카메라 건너뜀") and not a.camera_joined
        cam = rig.shared.camera
        cam.leave = lambda owner: False
        a.camera_joined = True
        a._camera(False)
        assert R.has_log(e, "[CH2] 카메라 사용 종료 — 다른 공정이 계속 촬영 중") and not a.camera_joined
        cam.leave = lambda owner: True
        a.camera_joined = True
        a.camera_leave()
        assert R.has_log(e, "[CH2] 카메라 녹화 정지")
    finally:
        _drop(rig)


def test_proc_209_sw_rf_select_readback_and_shutter(tmp_path):
    """PROC-209: SW_RF_SELECT ON 은 write 뒤 read_bit 실측을 기록 use_power_select 에('SW_RF_SELECT 실측 기록(use_power_select=…)',
    읽기 실패 '… 레시피 값 유지'), 그 뒤 확인. MS 는 main_shutter(ch) + 셔터 알림(공정 모니터·Calib)."""
    from v2.common.worker import Status
    rig = R.make(tmp_path, chambers=("ch2",))
    e = rig.eng["ch2"]
    a = e.actions
    a.runner = _Runner()
    ok = SimpleNamespace(status=Status.OK, value=None, error=None, reason=None)
    shut = []
    e.shutter_listeners.append(shut.append)
    try:
        for val, want in ((SimpleNamespace(status=Status.OK, value=True, error=None, reason=None),
                           "[CH2] SW_RF_SELECT 실측 기록(use_power_select=True)"),
                          (SimpleNamespace(status="ERROR", value=None, error="timeout", reason="io"),
                           "[CH2] SW_RF_SELECT 읽기 실패('timeout') → 레시피 값 유지(use_power_select=True)")):
            a.plc = SimpleNamespace(write_switch=lambda name, on, on_done: on_done(ok),
                                    read_bit=lambda name, on_done, v=val: on_done(v))
            a._a_plc_cmd(S("PLC_CMD", "Power_select: Power Select ON (SW_RF_SELECT)", params=("SW_RF_SELECT", True)))
            assert R.has_log(e, want) and a.runner.calls[-1] == ("match", "PLC", "SW_RF_SELECT")
        assert e.stats.params["use_power_select"] is True
        a.plc = SimpleNamespace(main_shutter=lambda n, open, on_done: on_done(ok))
        e.params = {}
        a._a_plc_cmd(S("PLC_CMD", "Main Shutter 열기", params=("MS", True)))
        assert shut == [True] and a.runner.calls[-1] == ("match", "PLC", "MS")
    finally:
        _drop(rig)


def test_proc_210_cleanup_off_unconfirmed_warning(tmp_path):
    """PROC-210: 공정 뒤 장비 정리 — 펄스 cleanup(OFF 안전망)을 기다리고, OFF 미확인이면 '⚠ 정리 중 {nm} 출력 OFF 미확인 — 장비 출력
    상태 확인 필요'(로그 + 채팅), 펄스 점유 해제·IG 대기 취소·OES 정지 요청. v2 는 MFC·IG 연결을 끊지 않고 강제 복구가 없다(결정 D37·D86)."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    a = e.actions
    try:
        a.rfp = SimpleNamespace(worker=object(), cleanup=lambda on_done: on_done(True))
        a.dcp = SimpleNamespace(worker=object(), cleanup=lambda on_done: on_done(False))
        stops = []
        a.oes = SimpleNamespace(stop=lambda wait=False: stops.append(wait), daemon_enabled=False)
        a.oes_initialized = True
        a.cleanup_devices()
        w = "⚠ 정리 중 RF Pulse 출력 OFF 미확인 — 장비 출력 상태 확인 필요"
        assert R.has_log(e, w) and any(x.kind == "chat" and x.detail == w for x in e.events)
        assert not R.has_log(e, "정리 중 DC Pulse 출력 OFF 미확인") and stops == [False] and not a.oes_initialized
    finally:
        _drop(rig)

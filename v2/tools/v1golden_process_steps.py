"""v1 공정 스텝·레시피 골든 생성 — v1 코드를 실제로 불러 돌린다(v2 시험은 이 파일을 import 하지 않고 골든만 읽는다).

    cd <저장소 루트> && QT_QPA_PLATFORM=offscreen python v2/tools/v1golden_process_steps.py

만드는 것(v2/tests/golden/)
- process_combos.json : 레시피 열 조합(챔버 CH1/CH2 × 전원 DC/DC2/RF/DC Pulse/RF Pulse/조합 × 셔터·딜레이·중간 변경·
  두께·히터·TEST 행·delay 행·빈 값 …)마다 v1 의 행 처리 → 정규화(ChamberRuntime._normalize_params_for_process)
  → 시작 전 검사(_validate_norm_params) → ProcessController._create_process_sequence(종료 절차 포함)·긴급 종료 절차·
  validate_process_sequence 결과와 v1 로그. 설정은 config/settings.json 그대로(lib._config_loader.load_settings).
- recipe_check.json   : 행 값 검사(E230, _validate_raw_rows)·시작 전 검사(_validate_norm_params) 결과.
- recipe_files.json + recipe_files/ : CSV·XLSX 파일을 v1 읽기 규칙(openpyxl read_only·data_only, csv.DictReader)으로 읽은 행.

v1 을 그대로 부를 수 없는 곳(메서드 안 코드 — 손으로 옮김, 근거 줄 표시)
- 러너의 delay 행 판정·TEST 표시(runtime/chamber_runtime.py:5100-5158) — 기간 해석은 v1 _parse_duration_seconds 를 실제로 부름.
- 파일 읽기 클로저 _load_csv_rows(3383-3426) — openpyxl·csv·_is_blank_recipe_row 는 v1 것을 실제로 부름.
- 지원 플래그(ChamberRuntime.__init__ 512-536, main.py:479) — _CfgAdapter·_dc2_allowed 는 v1 것을 실제로 부름.
- Thickness 모드 표시(4044·4073) — thickness_mode_of 는 v1 것.
ChamberRuntime 은 __init__ 없이(object.__new__) 만들고 검사·정규화가 쓰는 속성만 채운다(ch·supports_*·cfg·_heater·append_log·_get_text).
"""
from __future__ import annotations

import asyncio
import copy
import csv
import importlib
import json
import os
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "v2" / "tests" / "golden"
FILES = GOLDEN / "recipe_files"
V1_BASE = "66efc7b1d079dfb137dd487c4b444160744fc9cb"


def _v1():
    sys.path.insert(0, str(ROOT))
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    m = types.SimpleNamespace()
    m.loader = importlib.import_module("lib._config_loader")
    m.loader.load_settings()
    m.cc = importlib.import_module("lib.config_common")
    m.c1 = importlib.import_module("lib.config_ch1")
    m.c2 = importlib.import_module("lib.config_ch2")
    m.pc = importlib.import_module("controller.process_controller")
    m.rt = importlib.import_module("runtime.chamber_runtime")
    m.heat = importlib.import_module("lib.heater_config")
    m.thk = importlib.import_module("controller.thickness_process")
    m.openpyxl = importlib.import_module("openpyxl")
    return m


# ───────────── v1 설정 → v2 cfg ─────────────
CH_KEYS = ("PROCESS_GAS_INFO", "PROCESS_GUN_SHUTTERS", "PROCESS_PRESSURE_CONTROL_SP_ON_CMD", "PROCESS_PRESSURE_CONTROL_SP_LABEL",
           "PROCESS_PRESSURE_CONTROL_SP_INDEX", "PROCESS_DEFAULT_BASE_PRESSURE", "PROCESS_DEFAULT_OES_INTEGRATION_MS",
           "PC_PRESSURE_WAIT_TIMEOUT_S", "PC_RF_PULSE_POST_ON_DELAY_MS", "PC_WORKING_PRESSURE_BOOST_TARGET",
           "PC_POWER_SELECT_SETTLE_MS", "THICKNESS_ZERO_CONFIRM_S", "THICKNESS_ZERO_CONFIRM_NM", "THICKNESS_MAX_TIME_RATIO",
           "THICKNESS_MAX_TIME_MARGIN_S", "RFPULSE_FREQ_MAX_HZ", "SUPPORTS_DC_CONT", "SUPPORTS_DC", "SUPPORTS_DC_CONT2",
           "SUPPORTS_DC2", "SUPPORTS_RF_CONT", "SUPPORTS_DC_PULSE", "SUPPORTS_DCPULSE", "SUPPORTS_RFPULSE",
           "SUPPORTS_RF_PULSE", "SUPPORTS_HEATER", "SUPPORTS_THICKNESS")


def v2_cfg(m) -> dict:
    """v1 이 실제로 보는 값(채널 모듈 → 없으면 config_common, _CfgAdapter._get 과 같은 순서) → v2 키 CHn_키."""
    out = {}
    for ch, mod in ((1, m.c1), (2, m.c2)):
        ad = m.rt._CfgAdapter(mod, ch)
        for k in CH_KEYS:
            v = ad._get(k, None)
            if v is not None:
                out[f"CH{ch}_{k}"] = v
    hc, _ = m.heat.load_heater_config(m.c2)
    out["CH2_HEATER_MAX_TEMP"] = float(hc.max_temp)
    out["CH2_HEATER_SV_LIMIT_C"] = float(hc.sv_limit_c)
    return json.loads(json.dumps(out))


def v1_caps(m, ch: int) -> dict:
    """ChamberRuntime.__init__ 512-536(+ main.py:479 CH2 supports_rf_cont=True) — _CfgAdapter·_dc2_allowed 는 v1 것."""
    ad = m.rt._CfgAdapter(m.c1 if ch == 1 else m.c2, ch)

    def cb(*names):
        for n in names:
            v = ad._get(n, None)
            if v is not None:
                return bool(v)
        return None
    v = cb("SUPPORTS_DC_CONT", "SUPPORTS_DC")
    dc = v if v is not None else (ch == 2)
    v = cb("SUPPORTS_DC_CONT2", "SUPPORTS_DC2")
    dc2 = m.rt._dc2_allowed(ch, v if v is not None else False, cb("SUPPORTS_HEATER") is True)
    v = cb("SUPPORTS_RF_CONT")
    rf = True if ch == 2 else (v if v is not None else False)
    v = cb("SUPPORTS_DC_PULSE", "SUPPORTS_DCPULSE")
    dcp = v if v is not None else (ch == 1)
    v = cb("SUPPORTS_RFPULSE", "SUPPORTS_RF_PULSE")
    rfp = v if v is not None else (ch == 2)
    heater = ch == 2 and bool(cb("SUPPORTS_HEATER"))
    return {"dc": bool(dc), "dc2": bool(dc2), "rf": bool(rf), "dcp": bool(dcp), "rfp": bool(rfp), "heater": heater,
            "thickness": bool(cb("SUPPORTS_THICKNESS"))}


def _runtime(m, ch: int, caps: dict, logs: list):
    rt = object.__new__(m.rt.ChamberRuntime)
    rt.ch = ch
    rt.supports_dc_cont, rt.supports_dc_cont2, rt.supports_rf_cont = caps["dc"], caps["dc2"], caps["rf"]
    rt.supports_dc_pulse, rt.supports_rf_pulse = caps["dcp"], caps["rfp"]
    rt.cfg = m.rt._CfgAdapter(m.c1 if ch == 1 else m.c2, ch)
    hc, _ = m.heat.load_heater_config(m.c2)
    rt._heater = types.SimpleNamespace(cfg=hc) if (ch == 2 and caps["heater"]) else None
    rt.append_log = lambda src, msg: logs.append([src, msg])
    rt._get_text = lambda name: ""
    return rt


def _controller(m, ch: int, caps: dict):
    nop = lambda *a, **k: None
    return m.pc.ProcessController(
        send_plc=nop, send_mfc=nop, send_dc_power=nop, stop_dc_power=nop, send_rf_power=nop, stop_rf_power=nop,
        send_dc_power2=nop, stop_dc_power2=nop, start_dc_pulse=nop, stop_dc_pulse=nop, set_dc_pulse_power=nop,
        start_rf_pulse=nop, stop_rf_pulse=nop, set_rf_pulse_power=nop, ig_wait=nop, cancel_ig=nop, rga_scan=nop,
        oes_run=nop, ch=ch, supports_dc_cont=caps["dc"], supports_rf_cont=caps["rf"], supports_dc_pulse=caps["dcp"],
        supports_rf_pulse=caps["rfp"], supports_dc_cont2=caps["dc2"])


def _rows(seq):
    return [[s.action.name, s.message, s.value, repr(s.params), s.duration, s.parallel, s.polling, s.no_wait] for s in seq]


def _drain(pc) -> list:
    out = []
    while not pc.event_q.empty():
        ev = pc.event_q.get_nowait()
        if ev.kind == "log":
            out.append([ev.payload["src"], ev.payload["msg"]])
    return out


def run_row(m, ch: int, raw: dict, caps: dict, thk: bool) -> dict:
    """v1 러너가 큐 행 하나를 처리하는 길(5100-5177 → 3799-4092 → 컨트롤러)."""
    logs: list = []
    rt = _runtime(m, ch, caps, logs)
    params = dict(raw)
    # (A) delay 행 — chamber_runtime.py:5100-5126 손으로 옮김(기간 해석은 v1 함수)
    import re
    name = str(params.get("Process_name") or params.get("process_note", "")).strip()
    duration_s, label = 0.0, ""
    _dcol = str(params.get("delay", "") or "").strip()
    if _dcol:
        try:
            duration_s = float(_dcol) * 60.0
            label = f"{_dcol}분"
        except Exception:
            duration_s = float(rt._parse_duration_seconds(_dcol.lower()))
            label = _dcol
    else:
        mm = re.match(r"^\s*delay\s*(\d+)\s*([smhd]?)\s*$", name, re.IGNORECASE) if name else None
        if mm:
            amount = int(mm.group(1))
            unit = (mm.group(2) or "m").lower()
            duration_s = float(amount) * {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}[unit]
            label = f"{amount}" + {"s": "초", "m": "분", "h": "시간", "d": "일"}[unit]
    if duration_s > 0:
        return {"kind": "delay", "sec": duration_s, "label": label, "disp": name if name else f"delay {label}",
                "ticks": int(duration_s)}
    # (B) TEST 표시 — 5148-5158
    marker = str(params.get("#") or "").strip().lower()
    test = marker == "test"
    if test:
        time_str = str(params.get("time") or "").strip()
        tds = rt._parse_duration_seconds(time_str.lower())
        params["test_mode"] = True
        params["test_duration_sec"] = tds
        if tds > 0:
            params.setdefault("process_time", round(tds / 60.0, 3))
        params.setdefault("process_note", params.get("Process_name") or "TEST")
    norm = rt._normalize_params_for_process(params)
    norm_errors = rt._validate_norm_params(copy.deepcopy(norm))
    if not test and m.thk.thickness_mode_of(norm.get("thickness"), thk):
        norm["thickness_mode"] = True
    out = {"kind": "test" if test else "process", "norm": json.loads(json.dumps(norm)), "norm_errors": norm_errors}

    async def go():
        pc = _controller(m, ch, caps)
        try:
            seq = pc._create_process_sequence(dict(norm))
        except m.pc.AppError as e:
            out["error"] = e.code
            out["logs"] = logs + _drain(pc)
            return
        except Exception as e:                       # 예: dep_rate 0 인 Thickness 행(v1 은 시작 전 점검이 먼저 거부)
            out["exception"] = type(e).__name__
            out["logs"] = logs + _drain(pc)
            return
        out["steps"] = _rows(seq)
        pc.process_sequence = seq
        out["validate"] = list(pc.validate_process_sequence())
        out["estimated_ms"] = pc.get_estimated_duration()
        out["logs"] = logs + _drain(pc)
        pc.current_params = dict(norm)
        out["emergency"] = _rows(pc._create_emergency_shutdown_sequence())
        out["emergency_logs"] = _drain(pc)
    asyncio.run(go())
    return json.loads(json.dumps(out, ensure_ascii=False))


# ───────────── 조합 ─────────────
def _row(**kw) -> dict:
    tmpl = [h.strip() for h in (ROOT / "recipes" / "Recipe_Template.csv").read_text(encoding="utf-8-sig").splitlines()[0].split(",")]
    r = {h: "" for h in tmpl}
    r.update({k.replace("__", " "): str(v) for k, v in kw.items()})
    return r


T, F = "T", "F"
POWERS = {
    "dc": dict(use_dc_power=T, dc_power=100), "dc2": dict(use_dc_power2=T, dc_power2=80), "rf": dict(use_rf_power=T, rf_power=150),
    "dcp_DC": dict(use_dc_pulse=T, dc_pulse_power=60, dc_pulse_freq=50, dc_pulse_off_time="DC"),
    "dcp_num": dict(use_dc_pulse=T, dc_pulse_power=80, dc_pulse_freq=20, dc_pulse_off_time=10),
    "dcp_keep": dict(use_dc_pulse=T, dc_pulse_power=80),
    "dcp_legacy": dict(use_dc_pulse=T, dc_pulse_power=80, dc_pulse_freq=40, dc_pulse_duty_cycle=70),
    "dcp_legacy_nof": dict(use_dc_pulse=T, dc_pulse_power=80, dc_pulse_duty_cycle=70),
    "dcp_off_bad": dict(use_dc_pulse=T, dc_pulse_power=80, dc_pulse_freq=100, dc_pulse_off_time=9.5),
    "rfp": dict(use_rf_pulse=T, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty_cycle=40),
    "rfp_keep": dict(use_rf_pulse=T, rf_pulse_power=100),
    "rfp_hi": dict(use_rf_pulse=T, rf_pulse_power=100, rf_pulse_freq=31, rf_pulse_duty_cycle=0),
    "dc_rf": dict(use_dc_power=T, dc_power=100, use_rf_power=T, rf_power=150),
    "dc_rfp": dict(use_dc_power=T, dc_power=90, use_rf_pulse=T, rf_pulse_power=110, rf_pulse_freq=1, rf_pulse_duty_cycle=50),
    "dc2_rf": dict(use_dc_power2=T, dc_power2=70, use_rf_power=T, rf_power=120),
    "dcp_rfp": dict(use_dc_pulse=T, dc_pulse_power=80, dc_pulse_freq=20, use_rf_pulse=T, rf_pulse_power=90, rf_pulse_freq=2),
    "dc_dcp": dict(use_dc_power=T, dc_power=50, use_dc_pulse=T, dc_pulse_power=60, dc_pulse_freq=30, dc_pulse_off_time=5),
    "rf_rfp": dict(use_rf_power=T, rf_power=100, use_rf_pulse=T, rf_pulse_power=100, rf_pulse_freq=0.25, rf_pulse_duty_cycle=30),
    "none": dict(),
    "dcp_F_vals": dict(use_dc_pulse=F, dc_pulse_power=80, dc_pulse_freq=20, dc_pulse_off_time=10),
    "rfp_F_vals": dict(use_rf_pulse=F, rf_pulse_power=100, rf_pulse_freq=1, rf_pulse_duty_cycle=50),
    "all": dict(use_dc_power=T, dc_power=100, use_dc_power2=T, dc_power2=50, use_rf_power=T, rf_power=100, use_rf_pulse=T,
                rf_pulse_power=100),
    "zero": dict(use_dc_power=T, dc_power=0, use_rf_pulse=T, rf_pulse_power=0),
    "dc_blank_rf": dict(use_dc_power=T, use_rf_power=T, rf_power=200),
}
TIMES = [dict(shutter_delay=5, process_time=10, working_pressure=2), dict(shutter_delay=0, process_time=3, working_pressure=60),
         dict(shutter_delay=0.5, process_time=0.25, working_pressure=10), dict(shutter_delay=3, process_time="", working_pressure=5)]
GASES = [dict(Ar=T, Ar_flow=20), dict(Ar=T, Ar_flow=20, O2=T, O2_flow=2), dict(Ar="", Ar_flow=""), dict(N2=T, N2_flow=5),
         dict(O2=T, O2_flow=1.5), dict(Ar=T, Ar_flow="", N2=T, N2_flow=3)]
GUNS = [dict(gun1=T), dict(gun1=T, gun2=T), dict(), dict(gun3=T), dict(gun1=T, gun2=T, gun3=T)]
MISC = [dict(main_shutter=T), dict(main_shutter=F), dict(main_shutter=T, power_select=T), dict(main_shutter=T, integration_time=100),
        dict(main_shutter=T, integration_time="abc", base_pressure=""), dict(main_shutter=F, power_select=T, chuck_position="UP")]
CHANGES = [dict(), dict(power_change_time="1m", change_power_value=90), dict(power_change_time="7m", change_power_value=100),
           dict(power_change_time="30", change_power_value=70), dict(power_change_time="5m", change_power_value=95),
           dict(power_change_time="2h", change_power_value=90), dict(power_change_time="1m", change_power_value=""),
           dict(power_change_time="abc", change_power_value=90), dict(power_change_time="1m", change_power_value=0),
           dict(power_change_time="0.5m", change_power_value="88.5"), dict(power_change_time="1h0m30s", change_power_value=90)]
HEATERS = [dict(use_heater=T, heater_temp=200, heater_ramp=6, heater_soak=30), dict(use_heater=T, heater_temp=300),
           dict(use_heater=T, heater_temp=150, heater_ramp="", heater_soak=0), dict(use_heater=F, heater_temp=200, heater_ramp=6),
           dict(use_heater=T, heater_temp="", heater_ramp=0), dict(use_heater=T, heater_temp=1000, heater_soak=500)]
THICK = [dict(thickness=100, dep_rate=0.05, process_time=""), dict(thickness=60, dep_rate=0.05, process_time=20),
         dict(thickness=30, **{"dep.rate": 0.05}), dict(thickness=100, dep_rate=0, process_time=""),
         dict(thickness=100, dep_rate=0.05, process_time="", power_change_time="1m", change_power_value=90),
         dict(thickness=100, dep_rate=0.05, process_time="", power_change_time="10m", change_power_value=90)]
SPECIAL = [
    dict(**{"#": "test"}, time="30s"), dict(**{"#": "TEST"}, time="30"), dict(**{"#": "test"}, time=""),
    dict(**{"#": "test"}, time="1m", process_time=2), dict(**{"#": "test"}, time="1h30"), dict(**{"#": "test"}, process_time=0.5),
    dict(delay=5), dict(delay="20m"), dict(delay="1h30m"), dict(delay="abc"), dict(delay=0), dict(delay="90"),
    dict(delay="30s"), dict(Process_name="delay 10s"), dict(Process_name="delay 2"), dict(Process_name="Delay 1H"),
    dict(Process_name="delay 3d"), dict(Process_name="delay5m"), dict(Process_name="delay 1.5m"),
    dict(**{"#": "test"}, delay="1m", time="5s"),
]


def combos() -> list:
    cases = []

    def add(ch, name, caps_over=None, thk=None, **cols):
        base = dict(Process_name=f"P{len(cases) + 1}", base_pressure="7.00E-06", Ar=T, Ar_flow=20)
        base.update(cols)
        cases.append({"name": name, "ch": ch, "raw": _row(**base), "caps_override": caps_over or {}, "thk": thk})
    i = 0
    for ch in (1, 2):
        for pn, pw in POWERS.items():
            for t in TIMES[:3]:
                i += 1
                add(ch, f"ch{ch}_{pn}_t{TIMES.index(t)}", **{**GASES[i % 6], **GUNS[i % 5], **MISC[i % 6], **t, **pw})
        for pn in ("dcp_num", "dcp_DC", "rfp", "dc_rf", "dc_rfp", "dcp_rfp", "rfp_keep"):
            for ci, c in enumerate(CHANGES):
                i += 1
                add(ch, f"ch{ch}_{pn}_chg{ci}", **{**GUNS[i % 5], **MISC[i % 3], **TIMES[ci % 2 if ci else 0], **POWERS[pn], **c})
        for hi, h in enumerate(HEATERS):
            for pn in ("dc", "rfp", "dcp_num"):
                add(ch, f"ch{ch}_heat{hi}_{pn}", **{**GUNS[hi % 5], **TIMES[hi % 4], **POWERS[pn], **h, "main_shutter": T})
        for ti, th in enumerate(THICK):
            for pn in ("dcp_num", "rf", "rfp"):
                add(ch, f"ch{ch}_thk{ti}_{pn}", **{**GUNS[ti % 5], **POWERS[pn], "shutter_delay": 2, "working_pressure": 3,
                                                      "main_shutter": T if ti % 2 == 0 else F, **th})
            add(ch, f"ch{ch}_thk{ti}_nosupport", thk=False, **{**POWERS["dcp_num"], "shutter_delay": 2, "gun1": T, **th})
        for si, sp in enumerate(SPECIAL):
            add(ch, f"ch{ch}_special{si}", **{**POWERS["dcp_num"], "shutter_delay": 1, "process_time": 1, "gun1": T, **sp})
        for ti, t in enumerate(TIMES):
            add(ch, f"ch{ch}_times{ti}_rfp", **{**POWERS["rfp"], **t, "gun2": T})
    add(2, "ch2_noheater_dc2", caps_over={"heater": False, "dc2": True}, **{**POWERS["dc2_rf"], "gun1": T, **TIMES[0]})
    add(2, "ch2_noheater_heatrow", caps_over={"heater": False, "dc2": True}, **{**POWERS["dc"], "gun1": T, **TIMES[0], **HEATERS[0]})
    add(2, "ch2_nodcp", caps_over={"dcp": False}, **{**POWERS["dcp_num"], "gun1": T, **TIMES[0]})
    add(1, "ch1_norfp", caps_over={"rfp": False}, **{**POWERS["rfp"], **TIMES[0]})
    add(2, "ch2_blank_all", **{k: "" for k in ("Ar", "Ar_flow", "base_pressure")})
    return cases


def build_combos(m) -> dict:
    out = []
    for c in combos():
        caps = dict(v1_caps(m, c["ch"]), **c["caps_override"])
        thk = caps["thickness"] if c["thk"] is None else c["thk"]
        res = run_row(m, c["ch"], dict(c["raw"]), caps, thk)
        out.append({**c, "caps": caps, "thk": thk, "v1": res})
    return {"v1_base": V1_BASE, "cfg": v2_cfg(m), "cases": out}


# ───────────── 레시피 값 검사 ─────────────
def build_check(m) -> dict:
    raw_sets = {
        "ok_template": [_row(**{"#": 1, "Process_name": "a", "main_shutter": T, "Ar": T, "Ar_flow": 50, "base_pressure": "7.00E-06"})],
        "numbers": [_row(Process_name="n", base_pressure="1e-5x", working_pressure="abc", process_time="5분", Ar_flow="-",
                         tooling=5, density=100, z_factor="9.999", dep_rate="0,05", **{"dep.rate": "x"}, heater_temp="2OO")],
        "ints_tf_enum": [_row(Process_name="", integration_time="6.5", dc_pulse_freq="20k", rf_pulse_duty_cycle="",
                              gun1="yes", gun2="ON", Ar="t", O2="True", N2="2", chuck_position="Middle",
                              dc_pulse_off_time="dc", use_heater="X")],
        "durations": [_row(Process_name="d1", power_change_time="30", change_power_value=1),
                      _row(Process_name="d2", power_change_time="1h30", change_power_value=1),
                      _row(Process_name="d3", power_change_time="0s", change_power_value=1),
                      _row(Process_name="d4", power_change_time=" 5M ", change_power_value=""),
                      _row(Process_name="d5", power_change_time="", change_power_value=10),
                      _row(Process_name="d6", dc_pulse_off_time="3us", power_change_time="1.5h", change_power_value=5)],
        "overflow": [_row(Process_name=f"o{i}", base_pressure="x", working_pressure="y", Ar="Q") for i in range(9)],
        "ranges": [_row(Process_name="r1", tooling=10, density=0.5, z_factor=0.1), _row(Process_name="r2", tooling=399.5,
                                                                                          density=99.991, z_factor=10)],
    }
    nums = {"overflow": [2 + 3 * i for i in range(9)]}
    raw = {}
    for k, rows in raw_sets.items():
        rt = _runtime(m, 2, v1_caps(m, 2), [])
        rn = nums.get(k)
        errs = rt._validate_raw_rows([dict(r) for r in rows], row_numbers=rn)
        raw[k] = {"rows": rows, "row_numbers": rn, "errors": errs,
                  # host/handlers 경로의 문구(chamber_runtime.py:8079-8081 f-string 그대로)
                  "host_message": (f"레시피 오류 {len(errs)}건:\n - " + "\n - ".join(errs)) if errs else ""}
    norm_cases = []
    base = dict(use_ar=True, ar_flow=20.0, o2_flow=0.0, n2_flow=0.0, use_g1=True)
    variants = [
        (1, {}), (1, dict(use_rf_pulse=True, rf_pulse_power=0.0, rf_pulse_freq=0.0005, rf_pulse_duty=0)),
        (1, dict(use_dc_pulse=True, dc_pulse_power=50.0, dc_pulse_freq=10, dc_pulse_off_time=0.5)),
        (1, dict(use_dc_pulse=True, dc_pulse_power=50.0, dc_pulse_freq=100, dc_pulse_off_time=4.5)),
        (1, dict(use_dc_pulse=True, dc_pulse_power=50.0, dc_pulse_freq=None, dc_pulse_off_time=10.5)),
        (1, dict(use_dc_pulse=True, dc_pulse_power=50.0, dc_pulse_off_time="DC", use_rf_pulse=True, rf_pulse_power=10.0)),
        (1, dict(use_rf_pulse=True, rf_pulse_power=10.0, rf_pulse_freq=30.0, rf_pulse_duty=99, use_heater=True, heater_temp=100.0)),
        (1, dict(use_ar=False, ar_flow=-1.0, o2_flow="abc", n2_flow=None, use_dc_pulse=True, dc_pulse_power=1.0)),
        (2, {}), (2, dict(use_g1=True, use_g2=True, use_g3=True, use_dc_power=True, dc_power=-5.0)),
        (2, dict(use_g1=False, use_dc_power2=True, dc_power2=10.0)),
        (2, dict(use_rf_pulse=True, rf_pulse_power=100.0, rf_pulse_freq=31.0, rf_pulse_duty=100)),
        (2, dict(use_dc_pulse=True, dc_pulse_power=0.0, dc_pulse_freq=151, dc_pulse_off_time="x")),
        (2, dict(use_rf_power=True, rf_power=100.0, use_heater=True, heater_temp=None, heater_ramp=0.0, heater_soak=-1.0)),
        (2, dict(use_rf_power=True, rf_power=100.0, use_heater=True, heater_temp=950.0, heater_ramp=61.0, heater_soak=481.0)),
        (2, dict(use_rf_power=True, rf_power=100.0, use_heater=True, heater_temp=900.0, heater_ramp=60.0, heater_soak=480.0)),
        (2, dict(test_mode=True, use_ar=False)),
    ]
    for ch, v in variants:
        for over in ({}, {"heater": False}, {"rfp": False, "dcp": False}):
            caps = dict(v1_caps(m, ch), **over)
            rt = _runtime(m, ch, caps, [])
            p = dict(base, **v)
            errs = rt._validate_norm_params(copy.deepcopy(p))
            norm_cases.append({"ch": ch, "caps": caps, "params": p, "errors": errs})
    return {"v1_base": V1_BASE, "cfg": v2_cfg(m), "raw": raw, "norm": norm_cases}


# ───────────── 파일 읽기 ─────────────
def _write_files(m) -> list:
    FILES.mkdir(parents=True, exist_ok=True)
    names = []
    for src in ("Recipe_Template.csv", "Recipe_Template.xlsx"):
        (FILES / src).write_bytes((ROOT / "recipes" / src).read_bytes())
        names.append(src)
    hdr = list(_row().keys())
    with open(FILES / "blank_short.csv", "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(hdr)
        w.writerow(["1", "a", "", "down"] + [""] * (len(hdr) - 4))
        w.writerow([""] * len(hdr))
        f.write(",,,\n")
        w.writerow(["", "", "20m"])
        w.writerow(["test", "", ""] + [""] * (len(hdr) - 3) + ["x", "y"])
        w.writerow(["", "  ", "", "up"])
    names.append("blank_short.csv")
    wb = m.openpyxl.Workbook()
    ws = wb.active
    ws.title = "Recipe"
    ws.append(["#", "Process_name", "delay", "", "base_pressure", "process_time", "main_shutter", "Ar_flow", " heater_temp "])
    ws.append([1, "숫자", None, "무시", 7e-06, 10, True, 20.5, 200])
    ws.append([None, None, None, None, None, None, None, None, None])
    ws["A5"] = "test"
    ws["B5"] = "  공백  "
    ws["E5"] = 1e-5
    import datetime
    ws["F6"] = datetime.datetime(2026, 10, 30, 12, 0)
    ws["G6"] = False
    ws["H6"] = "=1+1"
    ws["A9"].number_format = "0.00"            # 서식만 있는 행(max_row 함정)
    ws["C12"].number_format = "@"
    ws2 = wb.create_sheet("설명")
    ws2.append(["무시", "되는", "시트"])
    wb.save(FILES / "mixed.xlsx")
    names.append("mixed.xlsx")
    wb = m.openpyxl.Workbook()
    wb.active.title = "Empty"
    wb.save(FILES / "empty.xlsx")
    names.append("empty.xlsx")
    return names


def _v1_load(m, path: Path) -> dict:
    """_load_csv_rows(3383-3426) 손으로 옮김 — openpyxl·csv·_is_blank_recipe_row 는 v1 것."""
    rows, row_nos, blank = [], [], 0

    def push(row, lineno):
        nm = (str(row.get('Process_name') or row.get('#') or '').strip() or f"공정 {len(rows) + 1}")
        row['Process_name'] = nm
        rows.append(row)
        row_nos.append(int(lineno))
    if str(path).lower().endswith(".xlsx"):
        wb = m.openpyxl.load_workbook(path, read_only=True, data_only=True)
        try:
            it = wb.worksheets[0].iter_rows(values_only=True)
            header = [str(h).strip() if h is not None else "" for h in next(it, [])]
            for lineno, vals in enumerate(it, start=2):
                row, empty = {}, True
                for h, v in zip(header, vals):
                    if not h:
                        continue
                    s = "" if v is None else str(v).strip()
                    if s:
                        empty = False
                    row[h] = s
                if not empty:
                    push(row, lineno)
        finally:
            wb.close()
    else:
        with open(path, mode='r', encoding='utf-8-sig', newline='') as f:
            for lineno, row in enumerate(csv.DictReader(f), start=2):
                if m.rt._is_blank_recipe_row(row):
                    blank += 1
                    continue
                push(row, lineno)
    # 행은 (열, 값) 쌍 목록 — csv.DictReader 의 넘친 칸은 열 이름 None(restkey) 이라 JSON 객체 키가 될 수 없다
    return {"rows": [[[k, v] for k, v in r.items()] for r in rows], "row_nos": row_nos, "blank": blank}


def build_files(m) -> dict:
    return {"v1_base": V1_BASE, "files": {n: _v1_load(m, FILES / n) for n in _write_files(m)}}


def main() -> None:
    m = _v1()
    GOLDEN.mkdir(parents=True, exist_ok=True)
    for name, fn in (("process_combos.json", build_combos), ("recipe_check.json", build_check),
                     ("recipe_files.json", build_files)):
        data = fn(m)
        with open(GOLDEN / name, "w", encoding="utf-8", newline="\n") as f:
            if "cases" in data:                       # 큰 파일 — 경우 하나가 한 줄
                head = {k: v for k, v in data.items() if k != "cases"}
                f.write(json.dumps(head, ensure_ascii=False)[:-1] + ', "cases": [\n')
                f.write(",\n".join(json.dumps(c, ensure_ascii=False) for c in data["cases"]))
                f.write("\n]}\n")
            else:
                json.dump(data, f, ensure_ascii=False, indent=1)
                f.write("\n")
        n = len(data.get("cases") or data.get("norm") or data.get("files") or [])
        print(f"golden written: {GOLDEN / name} ({n})")


if __name__ == "__main__":
    main()

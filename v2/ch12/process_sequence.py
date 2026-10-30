"""챔버12 공정 순서 — v1 _create_process_sequence(controller/process_controller.py:2153-2735)를 블록 없이 그대로 잇는 비교 기준.

process_steps.py 의 스텝 조각을 v1 순서로 잇는다. blocks.template_blocks → blocks.expand 가 이것과 같은 스텝을 내는지,
그리고 이것이 v1 골든과 같은지를 시험이 확인한다(v2/tests/test_process_steps_golden.py).
- 중간 펄스 전력 변경 계획(2195-2285), Thickness 모드 충돌(E844, 1083-1108), TEST 행 시간(2157-2172)
- 순서 검사(validate_process_sequence 2987-3018), DELAY 합(get_estimated_duration 3020-3021)
"""
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

from v2.ch12.process_steps import (Caps, ProcessStep, S, StepError, SHUTDOWN_LOG, base_wait_steps, boost_steps,
                                   caps_from_cfg, common_info, delay_steps, gas_intro_steps, gas_prep_steps,
                                   heater_hold_steps, heater_set_steps, heater_text, heater_wait_steps, main_steps,
                                   parse_duration_pc, power_on_steps, power_select_steps, pressure_ctrl_steps,
                                   pulse_set_message, pulse_set_steps, rga_steps, shutdown_steps, shutter_steps,
                                   sp1_steps, step_cfg, row_test_steps, thickness_main_steps, thickness_zero_steps,
                                   unsupported_logs, uses_from_params)


# ───────────── 중간 펄스 전력 변경 계획 ─────────────
def mid_change_plan(params: Mapping[str, Any], info: Mapping[str, Any], caps: Caps, log: list) -> Optional[Dict[str, Any]]:
    """2220-2285: 쓰는 펄스 ∧ 두 칸 값 ∧ 전력>0 ∧ 0<t<(SD+PT)초 ∧ 콜백. 둘 다면 DC 우선. 무시 경고 로그."""
    sd_sec = float(params.get("shutter_delay", 0)) * 60.0
    pt_sec = float(params.get("process_time", 0)) * 60.0
    raw_t = str(params.get("power_change_time", "") or "").strip()
    raw_p = str(params.get("change_power_value", "") or "").strip()
    t = parse_duration_pc(raw_t) if raw_t else 0.0
    try:
        pw = float(raw_p) if raw_p else 0.0
    except Exception:
        pw = 0.0
    window = float(sd_sec + pt_sec)
    ok = raw_t != "" and raw_p != "" and pw > 0.0 and 0.0 < t < window
    do_dc = bool(info["use_dc_pulse"]) and ok and caps.set_dcp
    do_rf = bool(info["use_rf_pulse"]) and ok and caps.set_rfp
    if do_dc and do_rf:
        log.append(("Process", "⚠ Pulse 중간 Power 변경: DC/RF Pulse가 동시에 활성 → DC 우선, RF는 무시"))
        do_rf = False
    for use, do, lab, cb in ((info["use_dc_pulse"], do_dc, "DC", caps.set_dcp), (info["use_rf_pulse"], do_rf, "RF", caps.set_rfp)):
        if use and (raw_t != "" or raw_p != "") and not do:
            log.append(("Process", f"⚠ {lab} Pulse 중간 Power 변경 무시: power_change_time='{raw_t}', "
                                   f"change_power_value='{raw_p}', window={window:.1f}s, callback={'OK' if cb else 'MISSING'}"))
    if not (do_dc or do_rf):
        return None
    return {"kind": "dc" if do_dc else "rf", "t": t, "power": pw, "sd_sec": sd_sec, "pt_sec": pt_sec}


def thickness_conflict(params: Mapping[str, Any], caps: Caps) -> str:
    """thickness_mid_change_conflict(1083-1108) — 사유 문구 또는 ""."""
    try:
        info = common_info(params, caps)
        use_dcp = bool(info.get("use_dc_pulse")) and caps.set_dcp
        use_rfp = bool(info.get("use_rf_pulse")) and caps.set_rfp
        if not (use_dcp or use_rfp):
            return ""
        raw_t = str(params.get("power_change_time", "") or "").strip()
        raw_p = str(params.get("change_power_value", "") or "").strip()
        if not raw_t or not raw_p:
            return ""
        try:
            pw = float(raw_p)
        except Exception:
            return ""
        t = parse_duration_pc(raw_t)
        sd = float(params.get("shutter_delay", 0) or 0) * 60.0
        if pw <= 0.0 or t <= 0.0 or t < sd:
            return ""
        return (f"중간 펄스 전력 변경(power_change_time={raw_t}, {pw:g}W)이 메인 공정 구간에 있습니다 — "
                f"Thickness 모드는 메인 구간 길이가 정해지지 않아 함께 쓸 수 없습니다")
    except Exception as e:
        return f"중간 펄스 전력 변경 확인 실패({e!r})"


def row_test_duration_sec(params: Mapping[str, Any]) -> float:
    """TEST 행 시간(2157-2172): test_duration_sec → time(컨트롤러 기간 규칙 — 숫자만 = 초) → process_time×60, 최소 1초."""
    try:
        dur = float(params.get("test_duration_sec") or 0.0)
    except Exception:
        dur = 0.0
    if dur <= 0:
        ts = str(params.get("time", "")).strip()
        if ts:
            dur = parse_duration_pc(ts)
    if dur <= 0:
        dur = float(params.get("process_time") or 0.0) * 60.0
    return max(1.0, float(dur))


# ───────────── v1 순서 그대로(블록 없이) ─────────────
def build_sequence(params: Mapping[str, Any], chamber: int, cfg: Mapping[str, Any], *, caps: Optional[Caps] = None,
                   log: Optional[list] = None) -> List[ProcessStep]:
    """v1 _create_process_sequence(2153-2735) 와 같은 스텝 목록. log 에 v1 _emit_log 와 같은 (src, msg)."""
    caps = caps or caps_from_cfg(cfg, chamber)
    sc = step_cfg(cfg, chamber)
    log = log if log is not None else []
    if bool(params.get("test_mode", False)):
        dur = row_test_duration_sec(params)
        log.append(("Process", f"[TEST MODE] 장비제어 스킵 / 시뮬레이션 {dur:.1f}s 진행"))
        return row_test_steps(dur)
    info = common_info(params, caps)
    bp = float(params.get("base_pressure", sc.base_pressure))
    wp = float(params.get("working_pressure", 0))
    pt_min = float(params.get("process_time", 0))
    sd_min = float(params.get("shutter_delay", 0))
    sd_sec, pt_sec = sd_min * 60.0, pt_min * 60.0
    thk = bool(params.get("thickness_mode", False))
    if thk:
        c = thickness_conflict(params, caps)
        if c:
            raise StepError("E844", c)
    try:
        ims = int(float(params.get("integration_time", sc.integration_ms)))
    except Exception:
        ims = sc.integration_ms
    mid = mid_change_plan(params, info, caps, log)
    steps: List[ProcessStep] = []
    log.append(("Process", "공정 시작"))
    if bool(params.get("use_heater", False)):
        temp = float(params.get("heater_temp") or 0.0)
        _hr, _hs = params.get("heater_ramp"), params.get("heater_soak")
        ramp = float(_hr) if _hr not in (None, "") else None
        soak = float(_hs) if _hs not in (None, "") else None
        log.append(("Process", heater_text(temp, ramp, soak)))
        steps += heater_set_steps(temp, ramp) + heater_wait_steps(temp, soak)
    steps += base_wait_steps(bp) + rga_steps()
    used = {g: bool(params.get(f"use_{g.lower()}", False)) for g in ("AR", "O2", "N2")}
    used.update({g.upper(): bool(params.get(f"use_{g.lower()}", False)) for g in sc.gas_info})
    flows = {g.upper(): params.get(f"{g.lower()}_flow", 0) for g in sc.gas_info}
    steps += gas_prep_steps(sc, used) + gas_intro_steps(sc, used, flows)
    steps += pressure_ctrl_steps(sc, wp, sc.pressure_wait_timeout_s)
    guns = [g.lower() for g in sc.gun_shutters if params.get(f"use_{g.lower()}", False)]
    steps += shutter_steps(sc, "open", guns)
    if bool(params.get("use_power_select", False)) and sc.chamber == 2:
        steps += power_select_steps()
        if sc.power_select_settle_ms > 0:
            ms = sc.power_select_settle_ms
            steps += [S("DELAY", f"Power Select 안정화 대기 ({ms / 1000:.0f}초)", duration=ms, polling=False, holding=True)]
    log += [("Process", m) for m in unsupported_logs(info)]
    steps += power_on_steps(sc, **power_kwargs(params, info))
    if info["use_rf_pulse"]:
        ms = sc.rf_pulse_post_on_delay_ms
        steps += [S("DELAY", f'Power Delay {ms / 1000.0:.0f}초', duration=ms, polling=False, holding=True)]
    if wp < sc.boost_target:
        steps += boost_steps(sc.boost_target, sc.pressure_wait_timeout_s)
    steps += sp1_steps(wp)
    if bool(params.get("use_heater", False)):
        steps += heater_hold_steps()
    if sd_sec > 0:
        if mid and mid["t"] < sd_sec:
            p1, p2 = float(mid["t"]), float(sd_sec - mid["t"])
            if p1 > 0:
                steps += delay_steps(p1, f'Shutter Delay (변경 전) {p1 / 60.0:.2f}분')
            steps += pulse_set_steps(mid["kind"], mid["power"], pulse_set_message(mid["kind"], mid["power"], mid["t"], "ShutterDelay"), False)
            if p2 > 0:
                steps += delay_steps(p2, f'Shutter Delay (변경 후) {p2 / 60.0:.2f}분')
        else:
            steps += delay_steps(sd_sec, f'Shutter Delay {sd_min}분')
    if thk:
        steps += thickness_zero_steps(sc)
    if info["use_ms"]:
        steps += shutter_steps(sc, "open", ["ms"])
    if thk:
        steps += thickness_main_steps(sc, float(params.get("thickness") or 0.0), float(params.get("dep_rate") or 0.0), ims)
    elif pt_sec > 0:
        ch_ = None
        if mid and sd_sec <= mid["t"] < sd_sec + pt_sec:
            ch_ = {"after_sec": float(mid["t"] - sd_sec), "kind": mid["kind"], "power": mid["power"],
                   "message": pulse_set_message(mid["kind"], mid["power"], mid["t"], "Main")}
        steps += main_steps(pt_min, ims, ch_)
    steps += shutdown_steps(sc, uses_from_params(params, caps, sc))
    log.append(("Process", SHUTDOWN_LOG))
    return steps


def power_kwargs(params: Mapping[str, Any], info: Mapping[str, Any]) -> Dict[str, Any]:
    """행 params → power_on_steps 인자(쓰는 전원만 값). v1 은 쓰지 않는 펄스의 freq·off·duty 도 변환한다(int() 예외 포함)."""
    f, off = params.get("dc_pulse_freq", None), params.get("dc_pulse_off_time", None)
    if f is not None:
        f = int(f)
    if off is not None and not isinstance(off, str):
        off = round(float(off), 1)
    rf_khz, duty = params.get("rf_pulse_freq", None), params.get("rf_pulse_duty", None)
    if rf_khz is not None:
        int(round(float(rf_khz) * 1000.0))
    if duty is not None:
        duty = int(duty)
    return dict(dc=float(params.get("dc_power", 0)) if info["use_dc"] else None,
                dc2=float(params.get("dc_power2", 0)) if info["use_dc2"] else None,
                dcp=float(params.get("dc_pulse_power", 0)) if info["use_dc_pulse"] else None, dcp_freq=f, dcp_off=off,
                rfp=float(params.get("rf_pulse_power", 0)) if info["use_rf_pulse"] else None, rfp_freq_khz=rf_khz,
                rfp_duty=duty, rf=float(params.get("rf_power", 0)) if info["use_rf"] else None)


def validate_sequence(steps: List[ProcessStep]) -> tuple:
    """validate_process_sequence(2987-3018) — (ok, 오류 문구 목록)."""
    errors: List[str] = []
    try:
        for i, st in enumerate(steps):
            n = i + 1
            a = st.action
            if a == "DELAY" and st.duration is None:
                errors.append(f"Step {n}: DELAY 액션에 duration이 없습니다.")
            if a in ("DC_POWER_SET", "DC2_POWER_SET", "RF_POWER_SET", "IG_CMD", "DC_PULSE_SET", "RF_PULSE_SET") and st.value is None:
                errors.append(f"Step {n}: {a} 액션에 value가 없습니다.")
            if a == "DC_PULSE_START":
                if st.value is None:
                    errors.append(f"Step {n}: DC_PULSE_START에 value(파워)가 없습니다.")
                if st.params is None or len(st.params) != 2:
                    errors.append(f"Step {n}: DC_PULSE_START params=(freq_kHz, off_time_us) 필요.")
            if a == "RF_PULSE_START":
                if st.value is None:
                    errors.append(f"Step {n}: RF_PULSE_START에 value(파워)가 없습니다.")
                if st.params is None or len(st.params) != 2:
                    errors.append(f"Step {n}: RF_PULSE_START params=(freq, duty) 필요.")
            if a in ("PLC_CMD", "MFC_CMD", "OES_RUN") and st.params is None:
                errors.append(f"Step {n}: {a} 액션에 params가 없습니다.")
    except Exception as e:
        errors.append(f"검증 중 오류 발생: {e}")
    return len(errors) == 0, errors


def estimated_duration_ms(steps: List[ProcessStep]) -> int:
    """get_estimated_duration(3020-3021) — DELAY 합."""
    return sum((s.duration or 0) for s in steps if s.action == "DELAY")

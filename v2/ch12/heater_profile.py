"""CH2 히터 — 감속 접근 프로파일(순수 함수)과 히터 설정 묶음(HeaterCfg). v1 lib/heater_profile.py + lib/heater_config.py.

[감속 접근 — lib/heater_profile.py:1-73 그대로]
  남은 거리 d, 본 구간 속도 rate, 접근 구간 zone, 도착 속도 rmin:
      r(d) = rmin + (rate − rmin) · d / zone   (d < zone),   r(d) = rate   (d ≥ zone)
  거리 span 전체 시간(분) = (span − d₁)/rate + ln((rmin + b·d₁)/rmin)/b, b = (rate − rmin)/zone, d₁ = min(span, zone).
  solve_rate_for_minutes: 정해진 시간에 올리는 본 구간 속도(이분법). v1 안에서 부르는 곳 없음(HEAT-122 — 정의만 옮김).

[설정 — lib/heater_config.py:28-280]
  HeaterCfg 의 필드·기본값은 v1 HeaterConfig 와 같다(기본값은 v1 시험·에뮬레이터 기준값 — 운전값은 cfg 사전에서 온다).
  load_heater_cfg(cfg) 는 v1 load_heater_config 를 그대로 옮겼다 — 키는 v2 이름(CH2_ 접두어: CH2_SUPPORTS_HEATER,
  CH2_HEATER_*). v2 파라미터 표(params/core_heater.py)가 범위 밖 값을 이미 거부하므로(결정 D11) 보통은 경고가 없지만,
  v1 의 '호출 시점에 읽어 래더 규칙으로 고쳐 쓰기'(경고 문구 포함)는 그대로 남겨 둔다(같은 규칙을 두 군데서 다르게 들고
  있지 않게 — PLC 래더와 같은 클램프). 예외를 던지지 않는다.
REAL(IEEE754 단정도) 2워드 변환은 drivers/heater.py(float_to_words·words_to_float).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, fields
from typing import Any, List, Mapping, Tuple

# 래더 고정값/경계(lib/heater_config.py:16-25)
LADDER_MV_MIN = 400
LADDER_MV_ABS_MAX = 1600
LADDER_SV_MAX_C = 900.0
LADDER_OT_MAX_C = 950.0
KP_MIN, KP_MAX = 0.1, 50.0
TI_MIN, TI_MAX = 10.0, 3000.0
COUNTS_C_PER_MIN = 6.0
REG_BLOCK_FIRST, REG_BLOCK_LAST = 110, 160
REG_HEATER_LOW, REG_HEATER_HIGH = 110, 149


# ───────────────────────── 감속 접근(lib/heater_profile.py:28-73) ─────────────────────────
def approach_rate(dist: float, rate: float, zone: float, rmin: float) -> float:
    """남은 거리 dist(°C)에서 지금 내야 할 속도(°C/min). 접근 구간 밖이면 rate 그대로."""
    z, m = float(zone), float(rmin)
    rate = float(rate)
    dist = float(dist)
    if rate <= m or z <= 0 or dist >= z:
        return rate
    return m + (rate - m) * max(0.0, dist) / z


def ramp_minutes(span: float, rate: float, zone: float, rmin: float) -> float:
    """거리 span(°C)을 rate(°C/min)로 올리는 데 걸리는 시간(분). 접근 구간 감속을 적분으로 반영한다."""
    z, m = float(zone), float(rmin)
    span = abs(float(span))
    rate = float(rate)
    if rate <= 0 or span <= 0:
        return 0.0
    if z <= 0 or rate <= m or m <= 0:
        return span / rate
    d1 = min(span, z)
    b = (rate - m) / z
    return (span - d1) / rate + math.log((m + b * d1) / m) / b


def solve_rate_for_minutes(span: float, minutes: float, zone: float, rmin: float) -> float:
    """span(°C)을 minutes(분)에 올리는 '본 구간' 속도(°C/min). 답은 항상 산술값(span/minutes) 이상이다."""
    z, m = float(zone), float(rmin)
    span = abs(float(span))
    minutes = float(minutes)
    if minutes <= 0 or span <= 0:
        return 0.0
    r0 = span / minutes
    if z <= 0 or r0 <= m:
        return r0
    lo = r0
    hi = r0 * 10.0
    while ramp_minutes(span, hi, z, m) > minutes and hi < 600.0:
        hi *= 2.0
    hi = min(hi, 600.0)
    for _ in range(80):
        mid = (lo + hi) / 2.0
        if ramp_minutes(span, mid, z, m) > minutes:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


# ───────────────────────── 설정(lib/heater_config.py:28-91) ─────────────────────────
@dataclass(frozen=True)
class HeaterCfg:
    """v1 HeaterConfig 와 같은 필드·기본값. 운전값은 load_heater_cfg(cfg 사전)."""
    enabled: bool = False
    temp_scale: float = 0.1
    poll_interval_s: float = 1.0
    poll_idle_s: float = 3.0
    wd_period_s: float = 5.0
    stale_sec: float = 10.0
    max_temp: float = 500.0
    sv_limit_c: float = 500.0
    ot_limit_c: float = 550.0
    ramp_rate: float = 12.0
    holdback_c: float = 10.0
    slow_zone_c: float = 0.0
    slow_rate: float = 60.0
    mv_min: int = 400
    mv_abs_max: int = 1600
    mv_limit: int = 520
    kp: float = 3.0
    ti_s: float = 300.0
    kp_valid: bool = True
    ti_valid: bool = True
    push_config: bool = True
    session_mark_reg: int = 160
    approach_zone: float = 20.0
    approach_min_rate: float = 1.0
    approach_lead: float = 1.0
    soak_tol: float = 2.0
    hold_mode: str = "dac"
    hold_enter_tol: float = 3.0
    hold_enter_sec: float = 60.0
    hold_arrive_tol: float = 1.0
    hold_drift_pv: float = 0.5
    hold_drift_mv: float = 15.0
    sat_sec: float = 120.0
    soak_time_s: float = 60.0
    wait_timeout_s: float = 3600.0
    hold_wait_s: float = 300.0
    hold_fail_action: str = "force"
    log_enabled: bool = True
    log_period_s: float = 3.0
    log_dir: str = ""
    run_log_period_s: float = 10.0
    noresp_enable: bool = True
    noresp_mv: int = 600
    noresp_sec: float = 300.0
    noresp_rise_c: float = 5.0
    noresp_loss_sec: float = 180.0
    noresp_loss_below_c: float = 10.0
    noresp_loss_fall_c: float = 10.0

    def c_to_raw(self, c: float) -> int:
        """°C → 0.1°C 카운트(0~32767)."""
        return max(0, min(32767, int(round(float(c) / float(self.temp_scale)))))

    @staticmethod
    def rate_counts(c_per_min: float) -> int:
        """°C/min → 래더 램프 카운트/초(최소 1)."""
        return max(1, int(round(float(c_per_min) / COUNTS_C_PER_MIN)))

    def replace(self, **kw) -> "HeaterCfg":
        d = {f.name: getattr(self, f.name) for f in fields(self)}
        d.update(kw)
        return HeaterCfg(**d)


def _v2_key(key: str, prefix: str) -> str:
    return prefix + key


def load_heater_cfg(cfg: Mapping[str, Any], prefix: str = "CH2_") -> Tuple[HeaterCfg, List[str]]:
    """cfg 사전에서 히터 설정을 읽어 검증한다(v1 load_heater_config — lib/heater_config.py:105-280). (HeaterCfg, 경고 목록)."""
    warns: List[str] = []
    d = HeaterCfg()
    missing = object()

    def _get(key: str, default, cast):
        raw = cfg.get(_v2_key(key, prefix), missing) if hasattr(cfg, "get") else missing
        v = default if raw is missing else raw
        try:
            if cast is bool:
                if isinstance(v, str):
                    return v.strip().lower() in ("1", "true", "yes", "on")
                return bool(v)
            return cast(v)
        except Exception:
            warns.append(f"{key}={None if raw is missing else raw!r} 형식 오류 → 기본값 {default!r}")
            return default

    def _clamp(name: str, v: float, lo: float, hi: float) -> float:
        if v < lo or v > hi:
            nv = min(max(v, lo), hi)
            warns.append(f"{name}={v:g} 범위({lo:g}~{hi:g}) 밖 → {nv:g}")
            return nv
        return v

    enabled = _get("SUPPORTS_HEATER", False, bool)
    scale = _get("HEATER_TEMP_SCALE", d.temp_scale, float)
    if scale <= 0:
        warns.append(f"HEATER_TEMP_SCALE={scale:g} → 0.1")
        scale = 0.1
    sv_lim = _get("HEATER_SV_LIMIT_C", d.sv_limit_c, float)
    if sv_lim <= 0:
        warns.append(f"HEATER_SV_LIMIT_C={sv_lim:g} ≤ 0 → 500"); sv_lim = 500.0
    elif sv_lim > LADDER_SV_MAX_C:
        warns.append(f"HEATER_SV_LIMIT_C={sv_lim:g} > {LADDER_SV_MAX_C:g} → {LADDER_SV_MAX_C:g}"); sv_lim = LADDER_SV_MAX_C
    ot = _get("HEATER_OT_LIMIT_C", d.ot_limit_c, float)
    if ot <= 0:
        warns.append(f"HEATER_OT_LIMIT_C={ot:g} ≤ 0 → 550"); ot = 550.0
    elif ot > LADDER_OT_MAX_C:
        warns.append(f"HEATER_OT_LIMIT_C={ot:g} > {LADDER_OT_MAX_C:g} → {LADDER_OT_MAX_C:g}"); ot = LADDER_OT_MAX_C
    if ot <= sv_lim:
        warns.append(f"HEATER_OT_LIMIT_C={ot:g} ≤ SV 상한 {sv_lim:g} → {sv_lim + 30:g} (래더와 같은 규칙)")
        ot = sv_lim + 30.0
    max_temp = _get("HEATER_MAX_TEMP", d.max_temp, float)
    if max_temp <= 0 or max_temp > sv_lim:
        warns.append(f"HEATER_MAX_TEMP={max_temp:g} → {sv_lim:g} (0 초과 SV 상한 이하)")
        max_temp = sv_lim

    mv_min = int(_get("HEATER_MV_MIN", d.mv_min, float))
    if mv_min != LADDER_MV_MIN:
        warns.append(f"HEATER_MV_MIN={mv_min} 이 래더 고정값 {LADDER_MV_MIN} 과 다름 — 표시·판정이 실제와 어긋날 수 있음")
    mv_abs = int(_get("HEATER_MV_ABS_MAX", d.mv_abs_max, float))
    if mv_abs > LADDER_MV_ABS_MAX:
        warns.append(f"HEATER_MV_ABS_MAX={mv_abs} > {LADDER_MV_ABS_MAX} → {LADDER_MV_ABS_MAX}"); mv_abs = LADDER_MV_ABS_MAX
    if mv_abs < mv_min + 20:
        warns.append(f"HEATER_MV_ABS_MAX={mv_abs} < MV_MIN+20 → {LADDER_MV_ABS_MAX}"); mv_abs = LADDER_MV_ABS_MAX
    mv_lim = int(_clamp("HEATER_MV_LIMIT", float(_get("HEATER_MV_LIMIT", d.mv_limit, float)), mv_min + 20, mv_abs))

    ramp = _get("HEATER_RAMP_RATE_C_PER_MIN", d.ramp_rate, float)
    if ramp < COUNTS_C_PER_MIN:
        warns.append(f"HEATER_RAMP_RATE_C_PER_MIN={ramp:g} < 6 → 6 (래더 최소 1카운트/초)"); ramp = COUNTS_C_PER_MIN
    slow_rate = _get("HEATER_SLOW_RATE_C_PER_MIN", d.slow_rate, float)
    if slow_rate < COUNTS_C_PER_MIN:
        warns.append(f"HEATER_SLOW_RATE_C_PER_MIN={slow_rate:g} < 6 → 6"); slow_rate = COUNTS_C_PER_MIN
    slow_zone = _get("HEATER_SLOW_ZONE_C", d.slow_zone_c, float)
    if slow_zone < 0:
        warns.append(f"HEATER_SLOW_ZONE_C={slow_zone:g} < 0 → 0"); slow_zone = 0.0
    hb = _get("HEATER_HOLDBACK_C", d.holdback_c, float)
    if hb <= 0:
        warns.append(f"HEATER_HOLDBACK_C={hb:g} ≤ 0 → 10"); hb = 10.0

    kp = _get("HEATER_KP", d.kp, float)
    ti = _get("HEATER_TI_S", d.ti_s, float)
    kp_ok = KP_MIN <= kp <= KP_MAX
    ti_ok = TI_MIN <= ti <= TI_MAX
    if not kp_ok:
        warns.append(f"HEATER_KP={kp:g} 범위({KP_MIN:g}~{KP_MAX:g}) 밖 → 쓰지 않음(래더 기본값 3.0)")
    if not ti_ok:
        warns.append(f"HEATER_TI_S={ti:g} 범위({TI_MIN:g}~{TI_MAX:g}) 밖 → 쓰지 않음(래더 기본값 300)")

    zone = _get("HEATER_APPROACH_ZONE_C", d.approach_zone, float)
    if zone < 0:
        warns.append(f"HEATER_APPROACH_ZONE_C={zone:g} < 0 → 0"); zone = 0.0
    rmin = _get("HEATER_APPROACH_MIN_RATE_C_PER_MIN", d.approach_min_rate, float)
    if rmin <= 0:
        warns.append(f"HEATER_APPROACH_MIN_RATE_C_PER_MIN={rmin:g} ≤ 0 → 1.0"); rmin = 1.0
    elif rmin > 6.0:
        warns.append(f"HEATER_APPROACH_MIN_RATE_C_PER_MIN={rmin:g} > 6 → 6.0"); rmin = 6.0
    lead = _get("HEATER_APPROACH_LEAD_C", d.approach_lead, float)
    if lead < 0.2:
        warns.append(f"HEATER_APPROACH_LEAD_C={lead:g} < 0.2 → 0.2"); lead = 0.2
    soak = _get("HEATER_SOAK_TOLERANCE", d.soak_tol, float)
    if soak <= 0:
        warns.append(f"HEATER_SOAK_TOLERANCE={soak:g} ≤ 0 → 2.0"); soak = 2.0

    mode = str(_get("HEATER_HOLD_MODE", d.hold_mode, str)).strip().lower()
    if mode not in ("off", "dac"):
        warns.append(f"HEATER_HOLD_MODE={mode!r} 미지원(CH2 는 TC 1개) → 'dac'"); mode = "dac"
    enter_tol = _clamp("HEATER_HOLD_MV_ENTER_TOL_C", _get("HEATER_HOLD_MV_ENTER_TOL_C", d.hold_enter_tol, float), 0.5, 20.0)
    enter_sec = _clamp("HEATER_HOLD_MV_ENTER_SEC", _get("HEATER_HOLD_MV_ENTER_SEC", d.hold_enter_sec, float), 10.0, 600.0)
    arrive = _clamp("HEATER_HOLD_MV_ARRIVE_TOL_C", _get("HEATER_HOLD_MV_ARRIVE_TOL_C", d.hold_arrive_tol, float), 0.2, 10.0)
    dpv = _clamp("HEATER_HOLD_MV_DRIFT_PV_C", _get("HEATER_HOLD_MV_DRIFT_PV_C", d.hold_drift_pv, float), 0.1, 10.0)
    dmv = _clamp("HEATER_HOLD_MV_DRIFT_MV", _get("HEATER_HOLD_MV_DRIFT_MV", d.hold_drift_mv, float), 3.0, 200.0)
    sat = _get("HEATER_MV_SAT_SEC", d.sat_sec, float)
    if sat < 10:
        warns.append(f"HEATER_MV_SAT_SEC={sat:g} < 10 → 10"); sat = 10.0

    soak_t = _clamp("HEATER_SOAK_TIME_SEC", _get("HEATER_SOAK_TIME_SEC", d.soak_time_s, float), 0.0, 3600.0)
    wait_to = _clamp("HEATER_WAIT_TIMEOUT_SEC", _get("HEATER_WAIT_TIMEOUT_SEC", d.wait_timeout_s, float), 600.0, 43200.0)
    hold_w = _clamp("HEATER_HOLD_WAIT_SEC", _get("HEATER_HOLD_WAIT_SEC", d.hold_wait_s, float), 60.0, 1800.0)
    fail_act = str(_get("HEATER_HOLD_FAIL_ACTION", d.hold_fail_action, str)).strip().lower()
    if fail_act not in ("force", "abort"):
        warns.append(f"HEATER_HOLD_FAIL_ACTION={fail_act!r} 미지원 → 'force'"); fail_act = "force"

    poll = _clamp("HEATER_POLL_INTERVAL_S", _get("HEATER_POLL_INTERVAL_S", d.poll_interval_s, float), 0.5, 5.0)
    idle = _clamp("HEATER_POLL_IDLE_S", _get("HEATER_POLL_IDLE_S", d.poll_idle_s, float), poll, 10.0)
    wd = _clamp("HEATER_WD_PERIOD_S", _get("HEATER_WD_PERIOD_S", d.wd_period_s, float), 1.0, 9.0)
    stale = _get("HEATER_STALE_SEC", d.stale_sec, float)
    if stale < 2 * idle:
        warns.append(f"HEATER_STALE_SEC={stale:g} < 2×정지 폴링 {idle:g} → {2 * idle:g}"); stale = 2 * idle
    log_period = _clamp("HEATER_LOG_PERIOD_S", _get("HEATER_LOG_PERIOD_S", d.log_period_s, float), 1.0, 60.0)
    run_log = _get("HEATER_RUN_LOG_PERIOD_S", d.run_log_period_s, float)
    if run_log < 0:
        warns.append(f"HEATER_RUN_LOG_PERIOD_S={run_log:g} < 0 → 0(끔)"); run_log = 0.0
    elif run_log > 0:
        run_log = _clamp("HEATER_RUN_LOG_PERIOD_S", run_log, 2.0, 600.0)

    nr_mv = int(_clamp("HEATER_NORESP_MV", float(_get("HEATER_NORESP_MV", d.noresp_mv, float)),
                       LADDER_MV_MIN + 50, LADDER_MV_ABS_MAX))
    nr_sec = _clamp("HEATER_NORESP_SEC", _get("HEATER_NORESP_SEC", d.noresp_sec, float), 60.0, 1800.0)
    nr_rise = _clamp("HEATER_NORESP_RISE_C", _get("HEATER_NORESP_RISE_C", d.noresp_rise_c, float), 1.0, 50.0)
    nr_lsec = _clamp("HEATER_NORESP_LOSS_SEC", _get("HEATER_NORESP_LOSS_SEC", d.noresp_loss_sec, float), 60.0, 1800.0)
    nr_below = _clamp("HEATER_NORESP_LOSS_BELOW_C",
                      _get("HEATER_NORESP_LOSS_BELOW_C", d.noresp_loss_below_c, float), 2.0, 200.0)
    nr_fall = _clamp("HEATER_NORESP_LOSS_FALL_C",
                     _get("HEATER_NORESP_LOSS_FALL_C", d.noresp_loss_fall_c, float), 0.5, 100.0)

    mreg = int(_get("HEATER_SESSION_MARK_REG", d.session_mark_reg, float))
    if mreg and REG_HEATER_LOW <= mreg <= REG_HEATER_HIGH:
        warns.append(f"HEATER_SESSION_MARK_REG=D{mreg} 가 히터 영역 D110~D149 와 겹침 → 0(마커 끔)"); mreg = 0
    elif mreg and not (REG_HEATER_HIGH < mreg <= REG_BLOCK_LAST):
        warns.append(f"HEATER_SESSION_MARK_REG=D{mreg} 가 블록 읽기 범위 D150~D160 밖 → 0(마커 끔)"); mreg = 0
    elif mreg < 0:
        mreg = 0

    out = HeaterCfg(
        enabled=bool(enabled), temp_scale=float(scale),
        poll_interval_s=float(poll), poll_idle_s=float(idle), wd_period_s=float(wd), stale_sec=float(stale),
        max_temp=float(max_temp), sv_limit_c=float(sv_lim), ot_limit_c=float(ot),
        ramp_rate=float(ramp), holdback_c=float(hb), slow_zone_c=float(slow_zone), slow_rate=float(slow_rate),
        mv_min=int(mv_min), mv_abs_max=int(mv_abs), mv_limit=int(mv_lim),
        kp=float(kp), ti_s=float(ti), kp_valid=bool(kp_ok), ti_valid=bool(ti_ok),
        push_config=bool(_get("HEATER_PUSH_CONFIG", d.push_config, bool)), session_mark_reg=int(mreg),
        approach_zone=float(zone), approach_min_rate=float(rmin), approach_lead=float(lead), soak_tol=float(soak),
        hold_mode=mode, hold_enter_tol=float(enter_tol), hold_enter_sec=float(enter_sec),
        hold_arrive_tol=float(arrive), hold_drift_pv=float(dpv), hold_drift_mv=float(dmv), sat_sec=float(sat),
        soak_time_s=float(soak_t), wait_timeout_s=float(wait_to), hold_wait_s=float(hold_w), hold_fail_action=fail_act,
        log_enabled=bool(_get("HEATER_LOG_ENABLED", d.log_enabled, bool)), log_period_s=float(log_period),
        log_dir=str(_get("HEATER_LOG_DIR", d.log_dir, str) or ""),
        run_log_period_s=float(run_log),
        noresp_enable=bool(_get("HEATER_NORESP_ENABLE", d.noresp_enable, bool)),
        noresp_mv=int(nr_mv), noresp_sec=float(nr_sec), noresp_rise_c=float(nr_rise),
        noresp_loss_sec=float(nr_lsec), noresp_loss_below_c=float(nr_below), noresp_loss_fall_c=float(nr_fall),
    )
    return out, warns


# HeaterCfg 필드 → v2 설정 키(접두어 뒤) — 시험대·변환이 HeaterCfg 를 cfg 사전으로 바꿀 때
CFG_KEYS = {
    "enabled": "SUPPORTS_HEATER", "temp_scale": "HEATER_TEMP_SCALE", "poll_interval_s": "HEATER_POLL_INTERVAL_S",
    "poll_idle_s": "HEATER_POLL_IDLE_S", "wd_period_s": "HEATER_WD_PERIOD_S", "stale_sec": "HEATER_STALE_SEC",
    "max_temp": "HEATER_MAX_TEMP", "sv_limit_c": "HEATER_SV_LIMIT_C", "ot_limit_c": "HEATER_OT_LIMIT_C",
    "ramp_rate": "HEATER_RAMP_RATE_C_PER_MIN", "holdback_c": "HEATER_HOLDBACK_C", "slow_zone_c": "HEATER_SLOW_ZONE_C",
    "slow_rate": "HEATER_SLOW_RATE_C_PER_MIN", "mv_min": "HEATER_MV_MIN", "mv_abs_max": "HEATER_MV_ABS_MAX",
    "mv_limit": "HEATER_MV_LIMIT", "kp": "HEATER_KP", "ti_s": "HEATER_TI_S", "push_config": "HEATER_PUSH_CONFIG",
    "session_mark_reg": "HEATER_SESSION_MARK_REG", "approach_zone": "HEATER_APPROACH_ZONE_C",
    "approach_min_rate": "HEATER_APPROACH_MIN_RATE_C_PER_MIN", "approach_lead": "HEATER_APPROACH_LEAD_C",
    "soak_tol": "HEATER_SOAK_TOLERANCE", "hold_mode": "HEATER_HOLD_MODE", "hold_enter_tol": "HEATER_HOLD_MV_ENTER_TOL_C",
    "hold_enter_sec": "HEATER_HOLD_MV_ENTER_SEC", "hold_arrive_tol": "HEATER_HOLD_MV_ARRIVE_TOL_C",
    "hold_drift_pv": "HEATER_HOLD_MV_DRIFT_PV_C", "hold_drift_mv": "HEATER_HOLD_MV_DRIFT_MV", "sat_sec": "HEATER_MV_SAT_SEC",
    "soak_time_s": "HEATER_SOAK_TIME_SEC", "wait_timeout_s": "HEATER_WAIT_TIMEOUT_SEC", "hold_wait_s": "HEATER_HOLD_WAIT_SEC",
    "hold_fail_action": "HEATER_HOLD_FAIL_ACTION", "log_enabled": "HEATER_LOG_ENABLED", "log_period_s": "HEATER_LOG_PERIOD_S",
    "log_dir": "HEATER_LOG_DIR", "run_log_period_s": "HEATER_RUN_LOG_PERIOD_S", "noresp_enable": "HEATER_NORESP_ENABLE",
    "noresp_mv": "HEATER_NORESP_MV", "noresp_sec": "HEATER_NORESP_SEC", "noresp_rise_c": "HEATER_NORESP_RISE_C",
    "noresp_loss_sec": "HEATER_NORESP_LOSS_SEC", "noresp_loss_below_c": "HEATER_NORESP_LOSS_BELOW_C",
    "noresp_loss_fall_c": "HEATER_NORESP_LOSS_FALL_C",
}


def cfg_to_dict(c: HeaterCfg, prefix: str = "CH2_") -> dict:
    """HeaterCfg → v2 설정 사전(kp_valid·ti_valid 는 값 범위로 다시 정해진다)."""
    return {prefix + k: getattr(c, f) for f, k in CFG_KEYS.items()}

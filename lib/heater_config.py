# lib/heater_config.py
# -*- coding: utf-8 -*-
"""CH2 히터(PLC 내장 PID) 설정 — config 모듈(config_ch2)에서 '호출 시점에' 읽어 검증·클램프한다.

규칙은 PLC 래더와 같게 맞춘다(래더가 어차피 같은 클램프를 하므로, 파이썬이 다른 값을 들고 있으면
되읽기 불일치·상한 계산 오류가 난다). 예외를 던지지 않고 경고 문구를 돌려준 뒤 안전한 쪽으로 고친다.

REAL(IEEE754 단정도) 2워드 변환도 여기 둔다 — 하위 워드가 앞 주소(D142=하위, D143=상위).
"""
from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any, List, Tuple

# 래더 고정값/경계(주소표 A 기준)
LADDER_MV_MIN = 400           # PID MV_min 고정
LADDER_MV_ABS_MAX = 1600      # D118 상한 클램프
KP_MIN, KP_MAX = 0.1, 50.0    # 래더가 D142/D143 을 적용하는 범위(밖이면 기본 3.0)
TI_MIN, TI_MAX = 10.0, 3000.0  # 래더가 D144/D145 를 적용하는 범위(밖이면 기본 300)
COUNTS_C_PER_MIN = 6.0        # 래더 램프 1카운트/초 = 6°C/min
REG_BLOCK_FIRST, REG_BLOCK_LAST = 110, 160   # 블록 읽기 범위
REG_HEATER_LOW, REG_HEATER_HIGH = 110, 149   # 히터 데이터 영역(마커와 겹치면 안 됨)


@dataclass(frozen=True)
class HeaterConfig:
    enabled: bool = False
    temp_scale: float = 0.1
    poll_interval_s: float = 1.0
    poll_idle_s: float = 3.0
    wd_period_s: float = 5.0
    stale_sec: float = 10.0
    max_temp: float = 500.0
    sv_limit_c: float = 500.0
    ot_limit_c: float = 550.0
    ramp_rate: float = 12.0              # °C/min
    holdback_c: float = 10.0
    slow_zone_c: float = 0.0
    slow_rate: float = 60.0              # °C/min
    mv_min: int = 400
    mv_abs_max: int = 1600
    mv_limit: int = 520
    kp: float = 3.0
    ti_s: float = 300.0
    kp_valid: bool = True                # False 면 쓰지 않음(래더 기본값 사용)
    ti_valid: bool = True
    push_config: bool = True
    session_mark_reg: int = 160          # 0 = 마커 끔
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
    log_enabled: bool = True
    log_period_s: float = 3.0
    log_dir: str = ""

    # ── 환산 ──
    def c_to_raw(self, c: float) -> int:
        """°C → 0.1°C 카운트(0~32767)."""
        return max(0, min(32767, int(round(float(c) / float(self.temp_scale)))))

    @staticmethod
    def rate_counts(c_per_min: float) -> int:
        """°C/min → 래더 램프 카운트/초(최소 1)."""
        return max(1, int(round(float(c_per_min) / COUNTS_C_PER_MIN)))


def float_to_words(v: float) -> Tuple[int, int]:
    """IEEE754 단정도 → (하위 워드, 상위 워드). 하위 워드가 앞 주소다."""
    bits = struct.unpack(">I", struct.pack(">f", float(v)))[0]
    return bits & 0xFFFF, (bits >> 16) & 0xFFFF


def words_to_float(low: int, high: int) -> float:
    """(하위 워드, 상위 워드) → float."""
    bits = ((int(high) & 0xFFFF) << 16) | (int(low) & 0xFFFF)
    return struct.unpack(">f", struct.pack(">I", bits))[0]


def load_heater_config(cfg_module: Any) -> Tuple[HeaterConfig, List[str]]:
    """config 모듈에서 HEATER_* 를 읽어 검증한다. (HeaterConfig, 경고 목록) — 예외를 던지지 않는다."""
    warns: List[str] = []
    d = HeaterConfig()

    def _get(key: str, default, cast):
        try:
            v = getattr(cfg_module, key, default)
            if cast is bool:
                if isinstance(v, str):
                    return v.strip().lower() in ("1", "true", "yes", "on")
                return bool(v)
            return cast(v)
        except Exception:
            warns.append(f"{key}={getattr(cfg_module, key, None)!r} 형식 오류 → 기본값 {default!r}")
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

    # ── 온도 한계(래더와 같은 규칙) ──
    sv_lim = _get("HEATER_SV_LIMIT_C", d.sv_limit_c, float)
    if sv_lim <= 0:
        warns.append(f"HEATER_SV_LIMIT_C={sv_lim:g} ≤ 0 → 500"); sv_lim = 500.0
    elif sv_lim > 700:
        warns.append(f"HEATER_SV_LIMIT_C={sv_lim:g} > 700 → 700"); sv_lim = 700.0
    ot = _get("HEATER_OT_LIMIT_C", d.ot_limit_c, float)
    if ot <= 0:
        warns.append(f"HEATER_OT_LIMIT_C={ot:g} ≤ 0 → 550"); ot = 550.0
    elif ot > 750:
        warns.append(f"HEATER_OT_LIMIT_C={ot:g} > 750 → 750"); ot = 750.0
    if ot <= sv_lim:
        warns.append(f"HEATER_OT_LIMIT_C={ot:g} ≤ SV 상한 {sv_lim:g} → {sv_lim + 30:g} (래더와 같은 규칙)")
        ot = sv_lim + 30.0
    max_temp = _get("HEATER_MAX_TEMP", d.max_temp, float)
    if max_temp <= 0 or max_temp > sv_lim:
        warns.append(f"HEATER_MAX_TEMP={max_temp:g} → {sv_lim:g} (0 초과 SV 상한 이하)")
        max_temp = sv_lim

    # ── 출력(DAC) ──
    mv_min = int(_get("HEATER_MV_MIN", d.mv_min, float))
    if mv_min != LADDER_MV_MIN:
        warns.append(f"HEATER_MV_MIN={mv_min} 이 래더 고정값 {LADDER_MV_MIN} 과 다름 — 표시·판정이 실제와 어긋날 수 있음")
    mv_abs = int(_get("HEATER_MV_ABS_MAX", d.mv_abs_max, float))
    if mv_abs > LADDER_MV_ABS_MAX:
        warns.append(f"HEATER_MV_ABS_MAX={mv_abs} > {LADDER_MV_ABS_MAX} → {LADDER_MV_ABS_MAX}"); mv_abs = LADDER_MV_ABS_MAX
    if mv_abs < mv_min + 20:
        warns.append(f"HEATER_MV_ABS_MAX={mv_abs} < MV_MIN+20 → {LADDER_MV_ABS_MAX}"); mv_abs = LADDER_MV_ABS_MAX
    mv_lim = int(_clamp("HEATER_MV_LIMIT", float(_get("HEATER_MV_LIMIT", d.mv_limit, float)), mv_min + 20, mv_abs))

    # ── 램프·홀드백 ──
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

    # ── PID ──
    kp = _get("HEATER_KP", d.kp, float)
    ti = _get("HEATER_TI_S", d.ti_s, float)
    kp_ok = KP_MIN <= kp <= KP_MAX
    ti_ok = TI_MIN <= ti <= TI_MAX
    if not kp_ok:
        warns.append(f"HEATER_KP={kp:g} 범위({KP_MIN:g}~{KP_MAX:g}) 밖 → 쓰지 않음(래더 기본값 3.0)")
    if not ti_ok:
        warns.append(f"HEATER_TI_S={ti:g} 범위({TI_MIN:g}~{TI_MAX:g}) 밖 → 쓰지 않음(래더 기본값 300)")

    # ── 접근 감속 ──
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

    # ── 유지 모드(dac 전용) ──
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

    # ── 주기 ──
    poll = _clamp("HEATER_POLL_INTERVAL_S", _get("HEATER_POLL_INTERVAL_S", d.poll_interval_s, float), 0.5, 5.0)
    idle = _clamp("HEATER_POLL_IDLE_S", _get("HEATER_POLL_IDLE_S", d.poll_idle_s, float), poll, 10.0)
    wd = _clamp("HEATER_WD_PERIOD_S", _get("HEATER_WD_PERIOD_S", d.wd_period_s, float), 1.0, 9.0)
    stale = _get("HEATER_STALE_SEC", d.stale_sec, float)
    if stale < 2 * idle:
        warns.append(f"HEATER_STALE_SEC={stale:g} < 2×정지 폴링 {idle:g} → {2 * idle:g}"); stale = 2 * idle
    log_period = _clamp("HEATER_LOG_PERIOD_S", _get("HEATER_LOG_PERIOD_S", d.log_period_s, float), 1.0, 60.0)

    # ── 세션 마커 ──
    mreg = int(_get("HEATER_SESSION_MARK_REG", d.session_mark_reg, float))
    if mreg and REG_HEATER_LOW <= mreg <= REG_HEATER_HIGH:
        warns.append(f"HEATER_SESSION_MARK_REG=D{mreg} 가 히터 영역 D110~D149 와 겹침 → 0(마커 끔)"); mreg = 0
    elif mreg and not (REG_HEATER_HIGH < mreg <= REG_BLOCK_LAST):
        # 마커는 블록 읽기(D110~D160)로 되읽는다 — 블록 밖이면 되읽을 수 없다
        warns.append(f"HEATER_SESSION_MARK_REG=D{mreg} 가 블록 읽기 범위 D150~D160 밖 → 0(마커 끔)"); mreg = 0
    elif mreg < 0:
        mreg = 0

    cfg = HeaterConfig(
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
        log_enabled=bool(_get("HEATER_LOG_ENABLED", d.log_enabled, bool)), log_period_s=float(log_period),
        log_dir=str(_get("HEATER_LOG_DIR", d.log_dir, str) or ""),
    )
    return cfg, warns

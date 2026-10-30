"""챔버12 레시피 값 검사 — v1 과 같은 조건·문구·코드.

1) 행 값 검사(E230) — 파일을 읽은 직후 원본 행 전수(runtime/chamber_runtime.py:8584-8751 _validate_raw_rows).
   빈 칸은 늘 합법. 오류가 있으면 큐에 넣지 않는다. 문구 '{행}행(\'{이름}\') {열}=\'{값}\' → …', 최대 20건 + '... 외 N건'.
   호스트(START_SPUTTER)는 RuntimeError('레시피 오류 N건:\n - …') → host/handlers.py:917-921 이 앞머리로 E230 분류
   (errors/error_registry.py:261-269 '레시피 값 오류', STOP). N 은 '... 외' 줄까지 센 목록 길이다(v1 그대로).
2) 시작 전 값 검사 — 정규화한 공정 params(8753-8919 _validate_norm_params·_validate_rf_pulse_params·_validate_dc_pulse_params).
   큐 행이면 'CSV 공정 파라미터 오류:\n - …'(엔진이 쓴다). TEST 행은 검사하지 않는다.
   ⚠ v1 처럼 ar_flow·o2_flow·n2_flow 를 float 로 바꿔 p 에 다시 넣는다(p 를 바꾼다).
"""
from __future__ import annotations

import re
from typing import Any, List, Mapping, Optional, Sequence

from v2.ch12.process_steps import Caps

E230 = "E230"
RECIPE_ERROR_PREFIX = "레시피 오류"
MAX_SHOWN = 20

RAW_FLOAT_KEYS = ("base_pressure", "working_pressure", "process_time", "shutter_delay", "thickness", "dep_rate", "dep.rate",
                  "dc_power", "dc_power2", "rf_power", "dc_pulse_power", "rf_pulse_power", "Ar_flow", "O2_flow", "N2_flow",
                  "change_power_value", "rf_pulse_freq", "heater_temp", "heater_ramp", "heater_soak",
                  "tooling", "density", "z_factor")
RAW_RANGE_KEYS = {"tooling": (10.0, 399.0, "%"), "density": (0.5, 99.99, "g/cm3"), "z_factor": (0.1, 9.999, "")}
RAW_INT_KEYS = ("integration_time", "dc_pulse_freq", "dc_pulse_duty_cycle", "rf_pulse_duty_cycle")
RAW_OFF_TIME_KEYS = ("dc_pulse_off_time",)
RAW_TF_KEYS = ("gun1", "gun2", "gun3", "use_dc_pulse", "use_rf_pulse", "use_rf_power", "use_dc_power", "use_dc_power2",
               "Ar", "O2", "N2", "main_shutter", "power_select", "use_heater")
TF_TRUE_TOKENS = ("T", "TRUE", "1", "Y", "YES")
TF_FALSE_TOKENS = ("F", "FALSE", "0", "N", "NO")
RAW_ENUM_KEYS = {"chuck_position": ("up", "mid", "down")}
RAW_DURATION_KEYS = ("power_change_time",)
_NUM_RE = re.compile(r"\d+(?:\.\d+)?")
_HMS_RE = re.compile(r"(?:(\d+(?:\.\d+)?)h)?(?:(\d+(?:\.\d+)?)m)?(?:(\d+(?:\.\d+)?)s)?")

HEATER_RAMP_MAX_C_PER_MIN = 60.0     # chamber_runtime.py:385
HEATER_SOAK_MAX_MIN = 480.0          # chamber_runtime.py:386
DCP_FREQ_MIN_KHZ, DCP_FREQ_MAX_KHZ = 20, 150
DCP_OFF_MIN_US, DCP_OFF_ABS_MAX_US = 1.0, 10.0


def parse_duration_for_validate(s: Any) -> float:
    """_parse_duration_for_validate(8636-8651) = 컨트롤러 규칙."""
    s = (s or "").strip().lower()
    if not s:
        return 0.0
    if _NUM_RE.fullmatch(s):
        return float(s)
    m = _HMS_RE.fullmatch(s)
    if not m:
        return 0.0
    return float(float(m.group(1) or 0.0) * 3600.0 + float(m.group(2) or 0.0) * 60.0 + float(m.group(3) or 0.0))


def check_raw_rows(rows: Sequence[Any], *, row_numbers: Optional[Sequence[int]] = None) -> List[str]:
    """E230 행 값 검사 — 오류 문구 목록(비면 통과)."""
    errs: List[str] = []
    overflow = 0

    def add(msg: str) -> None:
        nonlocal overflow
        if len(errs) < MAX_SHOWN:
            errs.append(msg)
        else:
            overflow += 1

    for idx, row in enumerate(rows or []):
        if not isinstance(row, dict):
            continue
        lineno = int(row_numbers[idx]) if row_numbers is not None and idx < len(row_numbers) else idx + 2
        name = str(row.get("Process_name", "") or "").strip()
        where = f"{lineno}행" + (f"('{name}')" if name else "")

        def val(k):
            return str(row.get(k, "") or "").strip()
        for key in RAW_FLOAT_KEYS:
            v = val(key)
            if v == "":
                continue
            try:
                fv = float(v)
            except Exception:
                add(f"{where} {key}='{v}' → 숫자가 아닙니다")
                continue
            rng = RAW_RANGE_KEYS.get(key)
            if rng is not None and not (rng[0] <= fv <= rng[1]):
                unit = f" {rng[2]}" if rng[2] else ""
                add(f"{where} {key}='{v}' → 범위 {rng[0]:g}~{rng[1]:g}{unit} 밖입니다")
        for key in RAW_INT_KEYS:
            v = val(key)
            if v == "":
                continue
            try:
                int(float(v))
            except Exception:
                add(f"{where} {key}='{v}' → 숫자가 아닙니다")
        for key in RAW_TF_KEYS:
            v = val(key)
            if v == "":
                continue
            u = v.upper()
            if u not in TF_TRUE_TOKENS and u not in TF_FALSE_TOKENS:
                add(f"{where} {key}='{v}' → T/F 값이 아닙니다")
        for key, allowed in RAW_ENUM_KEYS.items():
            v = val(key)
            if v and v.lower() not in allowed:
                add(f"{where} {key}='{v}' → {'/'.join(allowed)} 중 하나여야 합니다")
        for key in RAW_OFF_TIME_KEYS:
            v = val(key)
            if v == "" or v.upper() == "DC":
                continue
            try:
                float(v)
            except Exception:
                add(f"{where} {key}='{v}' → 숫자(µs) 또는 DC 여야 합니다")
        for key in RAW_DURATION_KEYS:
            v = val(key)
            if v and parse_duration_for_validate(v) <= 0.0:
                add(f"{where} {key}='{v}' → 기간 형식이 아닙니다 (예: 30s, 5m, 1h30m)")
        cpv, pct = val("change_power_value"), val("power_change_time")
        if bool(cpv) != bool(pct):
            add(f"{where} change_power_value 와 power_change_time 은 함께 입력해야 합니다 "
                f"(현재: change_power_value='{cpv}', power_change_time='{pct}')")
    if overflow > 0:
        errs.append(f"... 외 {overflow}건")
    return errs


def e230_message(errs: List[str]) -> str:
    """호스트 RuntimeError 문구·UI 로그 줄(8079-8081·3445)."""
    return f"{RECIPE_ERROR_PREFIX} {len(errs)}건:\n - " + "\n - ".join(errs)


def e230_popup(errs: List[str]) -> str:
    """UI 경고 팝업 '레시피 오류' 본문(3446-3449)."""
    return f"{RECIPE_ERROR_PREFIX} {len(errs)}건:\n" + "\n".join(f"- {e}" for e in errs)


def host_error_code(message: str, code: Optional[str] = None) -> str:
    """host/handlers.py:915-929 의 START_SPUTTER 실패 분류 — 코드가 없으면 앞머리 '레시피 오류' → E230,
    '1분 대기'·cooldown·preflight timeout → E700, 그 밖 E511."""
    if code:
        return code
    if message.lstrip().startswith(RECIPE_ERROR_PREFIX):
        return E230
    low = message.lower()
    if "1분 대기" in message or "cooldown" in low or "preflight timeout" in low:
        return "E700"
    return "E511"


# ───────────── 시작 전 값 검사 ─────────────
def rfp_freq_max_khz(cfg: Mapping[str, Any], chamber: int) -> float:
    """_rfp_freq_max_khz(3182-3188) — RFPULSE_FREQ_MAX_HZ(채널 → 공통, 기본 30000) / 1000, 0 이하·오류면 30 kHz."""
    from v2.ch12.process_steps import _cget
    try:
        hz = float(_cget(cfg, chamber, "RFPULSE_FREQ_MAX_HZ", 30000.0))
    except Exception:
        hz = 30000.0
    return (hz if hz > 0 else 30000.0) / 1000.0


def dcp_max_off_us(f_khz: float) -> float:
    """device/dc_pulse.py:128-139 dcp_max_off_time_us."""
    f = float(f_khz)
    if f <= 0.0:
        return DCP_OFF_ABS_MAX_US
    return round(min(DCP_OFF_ABS_MAX_US, 400.0 / f), 1)


def check_rf_pulse(p: Mapping[str, Any], fmax_khz: float) -> List[str]:
    errs: List[str] = []
    f, d = p.get("rf_pulse_freq"), p.get("rf_pulse_duty")
    if f is not None and not (0.001 <= float(f) <= fmax_khz):
        errs.append(f"RF Pulse Freq {float(f):g}kHz 는 허용 범위 0.001~{fmax_khz:g}kHz 밖입니다 "
                    "(CESAR 사양 3-5: 1Hz~30kHz — 초과 시 W40으로 출력 차단)")
    if d is not None and not (1 <= d <= 99):
        errs.append("RF Pulse Duty(%)는 1..99")
    return errs


def check_dc_pulse(p: Mapping[str, Any]) -> List[str]:
    errs: List[str] = []
    f = p.get("dc_pulse_freq")
    if f is not None and not (DCP_FREQ_MIN_KHZ <= f <= DCP_FREQ_MAX_KHZ):
        errs.append("DC Pulse Freq(kHz)는 20..150")
    off = p.get("dc_pulse_off_time")
    if off is None:
        return errs
    if isinstance(off, str):
        if off.strip().upper() != "DC":
            errs.append(f"DC Pulse Off Time '{off}' 은 숫자(µs) 또는 DC 여야 합니다")
        return errs
    try:
        off_v = float(off)
    except Exception:
        errs.append(f"DC Pulse Off Time '{off}' 은 숫자(µs) 또는 DC 여야 합니다")
        return errs
    mx = dcp_max_off_us(float(f)) if f else 10.0
    if off_v < 1.0:
        errs.append(f"DC Pulse Off Time {off_v:.1f}µs 는 최소 1.0µs 미만입니다 (매뉴얼 p.12/13)")
    elif off_v > mx:
        fx = f"{float(f):g}kHz" if f else "(주파수 미지정 → 절대 상한)"
        errs.append(f"DC Pulse Off Time {off_v:.1f}µs 는 {fx} 상한 {mx:.1f}µs 초과(매뉴얼 p.13 그림3)")
    return errs


def heater_limit_c(cfg: Mapping[str, Any]) -> float:
    """히터 목표 상한 = min(max_temp, sv_limit_c) — lib/heater_config.py:136-152 의 클램프(SV ≤0 → 500, MAX_TEMP 가 0 이하거나
    SV 상한보다 크면 SV 상한). 래더 상한(LADDER_SV_MAX_C) 클램프는 v2 파라미터 범위(params/core_heater.py ≤900)가 대신한다.
    v2 키 CH2_HEATER_SV_LIMIT_C·CH2_HEATER_MAX_TEMP(기본 = 운영값 900·900, settings.json ch2)."""
    sv = float(cfg.get("CH2_HEATER_SV_LIMIT_C", 900.0))
    if sv <= 0:
        sv = 500.0
    mt = float(cfg.get("CH2_HEATER_MAX_TEMP", 900.0))
    if mt <= 0 or mt > sv:
        mt = sv
    return min(mt, sv)


def check_norm_params(p: dict, chamber: int, caps: Caps, cfg: Mapping[str, Any], *,
                      heater_available: Optional[bool] = None, heater_limit: Optional[float] = None) -> List[str]:
    """_validate_norm_params(8805-8919). heater_available = 히터 서비스가 붙어 있음(v1 self._heater is not None —
    기본 caps.heater), heater_limit = min(max_temp, sv_limit_c)(기본 heater_limit_c(cfg))."""
    errs: List[str] = []
    if bool(p.get("test_mode", False)):
        return errs
    if not (p.get("use_ar") or p.get("use_o2") or p.get("use_n2")):
        errs.append("가스를 하나 이상 선택해야 합니다.")
    for k in ("ar_flow", "o2_flow", "n2_flow"):
        raw = p.get(k, 0)
        if raw is None:
            v = 0.0
        else:
            s = str(raw).strip()
            if s == "" or s.upper() == "NONE":
                v = 0.0
            else:
                try:
                    v = float(s)
                except (TypeError, ValueError):
                    v = 0.0
                    errs.append(f"{k} 값이 숫자가 아니라 0으로 처리(raw={raw!r}).")
        if v < 0:
            errs.append(f"{k}는 음수 불가")
        p[k] = v
    if bool(p.get("use_rf_pulse")) and bool(p.get("use_dc_pulse")):
        errs.append("RF Pulse와 DC Pulse는 동시에 사용할 수 없습니다 (같은 시리얼 포트)")
    fmax = rfp_freq_max_khz(cfg, chamber)
    if int(chamber) == 1:
        use_rf, use_dc = bool(p.get("use_rf_pulse")), bool(p.get("use_dc_pulse"))
        if not (use_rf or use_dc):
            errs.append("CH1은 RF Pulse 또는 DC Pulse 중 하나를 반드시 선택해야 합니다.")
        if use_rf:
            if p.get("rf_pulse_power", 0) <= 0:
                errs.append("RF Pulse Target Power(W)는 0보다 커야 합니다.")
            errs.extend(check_rf_pulse(p, fmax))
        if use_dc:
            if p.get("dc_pulse_power", 0) <= 0:
                errs.append("DC Pulse Target Power(W)는 0보다 커야 합니다.")
            errs.extend(check_dc_pulse(p))
    else:
        checked = int(p.get("use_g1", False)) + int(p.get("use_g2", False)) + int(p.get("use_g3", False))
        if checked == 0 or checked == 3:
            errs.append("G1~G3 중 1개 또는 2개만 선택")
        use_rfp, use_dcp = bool(p.get("use_rf_pulse")), bool(p.get("use_dc_pulse"))
        if use_rfp:
            if not caps.rfp:
                errs.append("이 챔버는 RF-Pulse를 지원하지 않습니다.")
            else:
                if p.get("rf_pulse_power", 0) <= 0:
                    errs.append("RF Pulse Target Power(W)는 0보다 커야 합니다.")
                errs.extend(check_rf_pulse(p, fmax))
        if use_dcp:
            if not caps.dcp:
                errs.append("이 챔버는 DC-Pulse를 지원하지 않습니다.")
            else:
                if p.get("dc_pulse_power", 0) <= 0:
                    errs.append("DC Pulse Target Power(W)는 0보다 커야 합니다.")
                errs.extend(check_dc_pulse(p))
        if not (p.get("use_dc_power") or p.get("use_dc_power2") or p.get("use_rf_power") or use_dcp or use_rfp):
            errs.append("CH2는 DC1/DC2/RF Power 또는 DC/RF Pulse 중 하나 이상 선택 필요")
        if p.get("use_dc_power") and p.get("dc_power", 0) < 0:
            errs.append("DC Target Power(W)는 0 이상이어야 합니다.")
        if p.get("use_dc_power2") and p.get("dc_power2", 0) < 0:
            errs.append("DC2 Target Power(W)는 0 이상이어야 합니다.")
        if p.get("use_dc_power2") and not caps.dc2:
            errs.append("DC2 Power 는 사용할 수 없습니다(DC2 폐지)")
    if p.get("use_heater"):
        avail = caps.heater if heater_available is None else heater_available
        if int(chamber) != 2:
            errs.append("CH1 에는 히터가 없습니다 (use_heater=T)")
        elif not avail:
            errs.append("히터 기능이 꺼져 있어 use_heater=T 공정을 실행할 수 없습니다 "
                        "(settings.json ch2 SUPPORTS_HEATER=false 또는 히터 초기화 실패)")
        else:
            lim = heater_limit_c(cfg) if heater_limit is None else float(heater_limit)
            t = p.get("heater_temp")
            if t is None or not (0.0 < float(t) <= lim):
                errs.append(f"heater_temp 는 0 초과 {lim:g}°C 이하여야 합니다 (현재 {t})")
            r = p.get("heater_ramp")
            if r is not None and not (0.0 < float(r) <= HEATER_RAMP_MAX_C_PER_MIN):
                errs.append(f"heater_ramp 는 0 초과 {HEATER_RAMP_MAX_C_PER_MIN:g}°C/min 이하여야 합니다 (현재 {r})")
            sk = p.get("heater_soak")
            if sk is not None and not (0.0 <= float(sk) <= HEATER_SOAK_MAX_MIN):
                errs.append(f"heater_soak 는 0~{HEATER_SOAK_MAX_MIN:g}분이어야 합니다 (현재 {sk})")
    return errs


def queue_param_error_log(errs: List[str]) -> str:
    """큐 행 시작 전 검사 실패 로그(5166) — 'Validate' 줄."""
    return "CSV 공정 파라미터 오류:\n - " + "\n - ".join(errs)

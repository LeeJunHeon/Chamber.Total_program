"""챔버12 v1 행 레시피 — 열 정의·파일 읽기(CSV·XLSX)·행 종류(delay·TEST)·정규화(공정 params).

v1 근거(기준 커밋 66efc7b, runtime/chamber_runtime.py)
- 파일 읽기: 3383-3426(UI 파일 열기) · 8011-8107(호스트 START_SPUTTER) — 같은 규칙. 빈 행 _is_blank_recipe_row 187-209.
  Process_name 채우기: 비면 '#' 값, 그것도 비면 '공정 {순번}'. 행 번호 = 원래 파일 행(헤더 = 1행).
- 파일 오류 코드(호스트): 파일 없음 E207, 읽기 실패 E209(CSV UTF-8 안내), 공정 0행 E208, csv/xlsx 아닌 문자열 E209,
  NAS 30초 시간 초과 E227(시간 제한은 부르는 쪽 — 엔진이 executor + 30 s 로 감싼다).
- 행 값 검사(E230)·시작 전 값 검사: recipe_check.py.
- delay 행(5100-5126): delay 열 — 숫자만이면 '분'(label "N분"), 아니면 기간 문자열(parse_duration_runtime, label 원문).
  delay 열이 비면 Process_name 'delay N[s|m|h|d]'(단위 생략 = 분). 대기 초 > 0 이면 대기 행(다른 열 무시).
- TEST 행(5148-5158): '#' 열이 test(대소문자 무시) → test_mode·test_duration_sec(time 열, parse_duration_runtime)·
  process_time·process_note setdefault.
- 정규화(_normalize_params_for_process 6159-6461) → 공정 params(v1 ProcessController 입력과 같은 키·값).

v1 기간 해석은 세 가지이고 열마다 뜻이 다르다(v1 그대로 둔다 — 결정 D101):
  · parse_duration_runtime(chamber_runtime.py:8134-8151, delay 열·time 열): re.match 가 늘 맞아서 단위 없는 숫자는 0 초,
    'abc' 도 0 초, '1h30' 은 3600 초(뒤 30 무시). delay 열은 그 전에 float() 로 '분' 을 먼저 본다.
  · process_steps.parse_duration_pc(process_controller.py:3178-3198, power_change_time·TEST time 의 두 번째 해석):
    숫자만 = 초, h/m/s 전체 일치, 그 밖 0.
  · recipe_check 의 E230 검사(8630-8651) = parse_duration_pc 와 같다.
  그래서 TEST 행 time='30' 은 러너에서 0 초 → 컨트롤러가 time 을 다시 읽어 30 초가 된다.

결정 D04: 펄스 사용 여부는 use_*_pulse 열만으로 정한다(F 면 freq·duty·off 값을 지운다 — 6389-6443). plan.py 의
'알려진 이상 동작(F 인데 값이 있으면 펄스 켜짐)' 은 기준 커밋 v1 에 없다.
"""
from __future__ import annotations

import csv
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple

from v2.ch12.process_steps import Caps

# recipes/Recipe_Template.csv:1 — 열 48개(순서)
TEMPLATE_COLUMNS: tuple = (
    "#", "Process_name", "delay", "chuck_position", "main_shutter", "integration_time", "Ar", "O2", "N2", "Ar_flow",
    "O2_flow", "N2_flow", "base_pressure", "working_pressure", "shutter_delay", "process_time", "dep_rate", "thickness",
    "tooling", "density", "z_factor", "gun1", "gun2", "gun3", "G1 Target", "G2 Target", "G3 Target", "power_select",
    "use_dc_power", "dc_power", "use_dc_power2", "dc_power2", "use_rf_power", "rf_power", "use_dc_pulse", "dc_pulse_power",
    "dc_pulse_freq", "dc_pulse_off_time", "use_rf_pulse", "rf_pulse_power", "rf_pulse_freq", "rf_pulse_duty_cycle",
    "power_change_time", "change_power_value", "use_heater", "heater_temp", "heater_ramp", "heater_soak",
)
# 템플릿 밖이지만 v1 이 읽는 열 — dep.rate(점 표기), dc_pulse_duty_cycle(레거시), time(TEST 행), G*_target_name, use_g*
EXTRA_COLUMNS: tuple = ("dep.rate", "dc_pulse_duty_cycle", "time", "G1_target_name", "G2_target_name", "G3_target_name",
                        "use_g1", "use_g2", "use_g3", "process_note")

# 열 → (뜻, 단위, 공란일 때, 정규화 키) — 화면·설명용(값 검사는 recipe_check, 동작은 normalize_row)
COLUMN_DOC: Dict[str, tuple] = {
    "#": ("'test' 면 TEST 행(장비 제어 없이 time 만큼 대기), 그 밖은 번호(Process_name 이 비면 이름)", "", "", "test_mode"),
    "Process_name": ("공정 이름(비면 '#' → '공정 N'). 'delay 10m' 꼴이면 대기 행", "", "", "process_note"),
    "delay": ("대기 행 — 숫자만이면 분, 아니면 30s·5m·1h30m(단위 없는 숫자 문자열 해석 0 초)", "분", "대기 행 아님", ""),
    "chuck_position": ("공정 전 Chuck 위치 up/mid/down", "", "이동 안 함", "chuck_position"),
    "main_shutter": ("T 면 Main Shutter 열기(닫기는 항상)", "T/F", "F", "use_ms"),
    "integration_time": ("OES 적분 시간", "ms", "60", "integration_time"),
    "Ar": ("Ar 사용", "T/F", "F", "use_ar"), "O2": ("O2 사용", "T/F", "F", "use_o2"),
    "N2": ("N2 사용(CH1 은 라인 없음 — 무시)", "T/F", "F", "use_n2"),
    "Ar_flow": ("Ar 유량", "sccm", "0", "ar_flow"), "O2_flow": ("O2 유량", "sccm", "0", "o2_flow"),
    "N2_flow": ("N2 유량", "sccm", "0", "n2_flow"),
    "base_pressure": ("베이스 압력 목표(IG)", "Torr", "1e-5", "base_pressure"),
    "working_pressure": ("작업 압력(SP1·채널 SP 대기 목표, < 부스트 목표면 SP2 먼저)", "mTorr", "0", "working_pressure"),
    "shutter_delay": ("Shutter Delay", "분", "0(단계 없음)", "shutter_delay"),
    "process_time": ("메인 공정 시간(비면 thickness÷dep_rate 역산)", "분", "0", "process_time"),
    "dep_rate": ("증착률(헤더 dep.rate 도 됨, >0 만)", "nm/s", "없음", "dep_rate"),
    "thickness": ("목표 두께(두께 모니터 채널이면 Thickness 모드)", "nm", "없음", "thickness"),
    "tooling": ("Thickness 모드 재료값 tooling(10~399)", "%", "Factor 설정", "tooling"),
    "density": ("재료 밀도(0.5~99.99)", "g/cm3", "Factor 설정", "density"),
    "z_factor": ("Z-factor(0.1~9.999)", "", "Factor 설정", "z_factor"),
    "gun1": ("Gun1 사용(CH2 건 셔터)", "T/F", "F", "use_g1"), "gun2": ("Gun2 사용", "T/F", "F", "use_g2"),
    "gun3": ("Gun3 사용", "T/F", "F", "use_g3"),
    "G1 Target": ("G1 타겟 이름(비면 재고 DB·화면값)", "", "", "G1 Target"), "G2 Target": ("G2 타겟 이름", "", "", "G2 Target"),
    "G3 Target": ("G3 타겟 이름", "", "", "G3 Target"),
    "power_select": ("CH2 SW_RF_SELECT 켜기 + 안정화 대기", "T/F", "F", "use_power_select"),
    "use_dc_power": ("DC1 연속", "T/F", "F", "use_dc_power"), "dc_power": ("DC1 파워", "W", "0", "dc_power"),
    "use_dc_power2": ("DC2 연속(폐지 — CH2 히터와 배타)", "T/F", "F", "use_dc_power2"), "dc_power2": ("DC2 파워", "W", "0", "dc_power2"),
    "use_rf_power": ("RF 연속", "T/F", "F", "use_rf_power"), "rf_power": ("RF 파워", "W", "0", "rf_power"),
    "use_dc_pulse": ("DC Pulse(이 열만으로 사용 여부 — D04)", "T/F", "F", "use_dc_pulse"),
    "dc_pulse_power": ("DC Pulse 파워", "W", "0", "dc_pulse_power"),
    "dc_pulse_freq": ("DC Pulse 주파수(정수 20~150)", "kHz", "장비값 유지", "dc_pulse_freq"),
    "dc_pulse_off_time": ("DC Pulse Off Time(숫자 µs 소수 1자리 | DC)", "µs", "장비값 유지", "dc_pulse_off_time"),
    "use_rf_pulse": ("RF Pulse(이 열만으로 사용 여부 — D04)", "T/F", "F", "use_rf_pulse"),
    "rf_pulse_power": ("RF Pulse 파워", "W", "0", "rf_pulse_power"),
    "rf_pulse_freq": ("RF Pulse 주파수(실수, 장비에는 Hz)", "kHz", "장비값 유지", "rf_pulse_freq"),
    "rf_pulse_duty_cycle": ("RF Pulse 듀티(정수 1~99)", "%", "장비값 유지", "rf_pulse_duty"),
    "power_change_time": ("중간 펄스 파워 변경 시점(숫자만 = 초, 30s·5m·1h30m) — Shutter Delay 시작부터", "", "변경 없음",
                          "power_change_time"),
    "change_power_value": ("중간 변경 파워(power_change_time 과 함께)", "W", "변경 없음", "change_power_value"),
    "use_heater": ("CH2 히터 사용", "T/F", "F", "use_heater"), "heater_temp": ("히터 목표", "°C", "없음", "heater_temp"),
    "heater_ramp": ("히터 램프", "°C/min", "기본 램프", "heater_ramp"), "heater_soak": ("도달 뒤 SOAK", "분", "없음", "heater_soak"),
}

TF_TRUE = ("T", "TRUE", "1", "Y", "YES")
TF_FALSE = ("F", "FALSE", "0", "N", "NO")


def tf(v: Any) -> bool:
    return str(v).strip().upper() in TF_TRUE


# ───────────── 파일 읽기 ─────────────
class RecipeFileError(Exception):
    """호스트 경로의 레시피 파일 오류(v1 CodedRuntimeError(msg, code))."""

    def __init__(self, message: str, code: str):
        super().__init__(message)
        self.code = code


def is_blank_row(row: Any) -> bool:
    """_is_blank_recipe_row(187-209) — dict·list 모두, None·빈 글은 빈 칸, 리스트 값(restkey)은 원소 확인."""
    def has(v):
        if v is None:
            return False
        if isinstance(v, (list, tuple)):
            return any(has(x) for x in v)
        return str(v).strip() != ""
    try:
        if isinstance(row, dict):
            vals = row.values()
        elif isinstance(row, (list, tuple)):
            vals = row
        else:
            return False
        return not any(has(v) for v in vals)
    except Exception:
        return False


def read_rows(path: str) -> Tuple[List[dict], List[int], int]:
    """(행 목록, 원래 행 번호, 건너뛴 빈 행 수) — v1 _load_csv_rows 그대로(예외는 그대로 올린다)."""
    rows: List[dict] = []
    row_nos: List[int] = []
    blank = 0

    def push(row: dict, lineno: int) -> None:
        name = (str(row.get('Process_name') or row.get('#') or '').strip() or f"공정 {len(rows) + 1}")
        row['Process_name'] = name
        rows.append(row)
        row_nos.append(int(lineno))

    if str(path).lower().endswith(".xlsx"):
        from v2.ch12.recipe_xlsx import iter_first_sheet
        it = iter_first_sheet(path)
        header = [str(h).strip() if h is not None else "" for h in next(it, [])]
        for lineno, vals in enumerate(it, start=2):
            row: Dict[str, str] = {}
            empty = True
            for h, v in zip(header, vals):
                if not h:
                    continue
                s = "" if v is None else str(v).strip()
                if s:
                    empty = False
                row[h] = s
            if not empty:                               # xlsx 빈 행은 세지 않는다(v1 — 로그 없음)
                push(row, lineno)
    else:
        with open(path, mode='r', encoding='utf-8-sig', newline='') as f:
            for lineno, row in enumerate(csv.DictReader(f), start=2):
                if is_blank_row(row):
                    blank += 1
                    continue
                push(row, lineno)
    return rows, row_nos, blank


@dataclass
class RecipeLoad:
    path: str
    rows: List[dict] = field(default_factory=list)
    row_nos: List[int] = field(default_factory=list)
    blank: int = 0
    logs: List[tuple] = field(default_factory=list)      # (src, msg) — v1 append_log 와 같은 줄
    errors: List[str] = field(default_factory=list)      # E230 행 값 오류(있으면 큐에 넣지 않음)
    code: str = ""                                       # 실패 코드(E207·E208·E209·E230) — UI 경로는 빈 문자열일 수 있음
    message: str = ""                                    # 실패 문구(호스트 응답·경고 팝업 본문)

    @property
    def ok(self) -> bool:
        return not self.code and not self.message


def load_recipe_file(path: str, *, origin: str = "host") -> RecipeLoad:
    """파일 → 행·검사. origin='host'(START_SPUTTER 8011-8107) 는 오류 코드, 'ui'(3383-3467) 는 로그·경고 문구."""
    from v2.ch12.recipe_check import check_raw_rows, e230_message, e230_popup
    s = (path or "").strip()
    r = RecipeLoad(path=s)
    host = origin == "host"
    if host and not s.lower().endswith((".csv", ".xlsx")):
        r.code, r.message = "E209", "지원하지 않는 레시피 형식입니다. CSV/XLSX 파일 경로만 허용됩니다."
        return r
    if host and not os.path.exists(s):
        r.code, r.message = "E207", f"레시피 파일을 찾을 수 없습니다: {s}"
        return r
    try:
        r.rows, r.row_nos, r.blank = read_rows(s)
    except Exception as e:
        if host:
            r.code = "E209"
            r.message = (f"레시피 파일을 읽지 못했습니다({type(e).__name__}: {e}) — 엑셀에서 'CSV UTF-8(쉼표로 분리)' 로 "
                         f"저장했는지, 다른 프로그램에서 열려 있지 않은지 확인하십시오: {s}")
        else:
            r.message = f"파일 처리 오류: {e}"
            r.logs.append(("File", r.message))
        return r
    if r.blank:
        r.logs.append(("File", f"빈 행 {r.blank}개 건너뜀 (모든 칸이 빈 행)"))
    errs = check_raw_rows(r.rows, row_numbers=r.row_nos)
    if errs:
        r.errors = errs
        r.code = "E230"
        r.message = e230_message(errs) if host else e230_popup(errs)
        if not host:
            r.logs.append(("File", e230_message(errs)))
        return r
    if not r.rows:
        if host:
            r.code, r.message = "E208", "CSV에 공정 데이터가 없습니다."
        else:
            r.message = "파일에 공정이 없습니다."
            r.logs.append(("File", r.message))
        return r
    r.logs.append(("File", f"CSV 로드 완료: {s} (총 {len(r.rows)}개)" if host else f"총 {len(r.rows)}개 공정 읽음."))
    return r


# ───────────── 기간·행 종류 ─────────────
_RT_DURATION_RE = re.compile(r"(?:(\d+(?:\.\d+)?)h)?(?:(\d+(?:\.\d+)?)m)?(?:(\d+(?:\.\d+)?)s)?")
_DELAY_NAME_RE = re.compile(r"^\s*delay\s*(\d+)\s*([smhd]?)\s*$", re.IGNORECASE)


def parse_duration_runtime(s: Any) -> float:
    """chamber_runtime._parse_duration_seconds(8134-8151) 그대로 — 단위 없는 숫자는 0 초(v1 이상 동작, D101)."""
    if not s:
        return 0.0
    s = s.replace(" ", "").lower()
    m = _RT_DURATION_RE.match(s)
    if not m:                                           # re.match 가 빈 문자열로 늘 맞아 여기는 오지 않는다(v1 그대로)
        try:
            return float(s) * 60.0
        except Exception:
            return 0.0
    return float(m.group(1) or 0) * 3600 + float(m.group(2) or 0) * 60 + float(m.group(3) or 0)


def delay_of(row: Mapping[str, Any]) -> Tuple[float, str, str]:
    """(대기 초, label, 표시 이름) — 대기 초 0 이면 대기 행이 아니다(5100-5126)."""
    name = str(row.get("Process_name") or row.get("process_note", "")).strip()
    duration_s, label = 0.0, ""
    dcol = str(row.get("delay", "") or "").strip()
    if dcol:
        try:
            duration_s = float(dcol) * 60.0
            label = f"{dcol}분"
        except Exception:
            duration_s = float(parse_duration_runtime(dcol.lower()))
            label = dcol
    else:
        m = _DELAY_NAME_RE.match(name) if name else None
        if m:
            amount = int(m.group(1))
            unit = (m.group(2) or "m").lower()
            duration_s = float(amount) * {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}[unit]
            unit_txt = {"s": "초", "m": "분", "h": "시간", "d": "일"}[unit]
            label = f"{amount}{unit_txt}"
    disp = name if name else f"delay {label}"
    return duration_s, label, disp


def mark_test_row(row: dict) -> bool:
    """'#'=test 행이면 v1 러너처럼 표시를 붙이고 True(5148-5158). row 를 바꾼다."""
    if str(row.get("#") or "").strip().lower() != "test":
        return False
    time_str = str(row.get("time") or "").strip()
    sec = parse_duration_runtime(time_str.lower())
    row["test_mode"] = True
    row["test_duration_sec"] = sec
    if sec > 0:
        row.setdefault("process_time", round(sec / 60.0, 3))
    row.setdefault("process_note", row.get("Process_name") or "TEST")
    return True


def thickness_mode_of(thickness: Any, supported: bool) -> bool:
    """controller/thickness_process.py:63-68 — 두께 모니터 채널이고 thickness > 0."""
    try:
        t = float(str(thickness).strip()) if thickness is not None and str(thickness).strip() else 0.0
    except Exception:
        t = 0.0
    return bool(supported) and t > 0.0


# ───────────── 정규화 ─────────────
def _dcp_duty_to_off(f_khz: float, duty: float) -> float:
    return round(max(0.0, (1000.0 / max(1e-6, f_khz)) * (1.0 - duty / 100.0)), 1)     # device/dc_pulse.py:153-156


def _dcp_off_to_duty(f_khz: float, off: float) -> float:
    p = 1000.0 / max(1e-6, f_khz)
    return round((p - off) / p * 100.0, 1)                                           # device/dc_pulse.py:159-162


def normalize_row(raw: Mapping[str, Any], chamber: int, caps: Caps, *, ui_targets: Optional[Mapping[str, str]] = None,
                  log: Optional[list] = None) -> Dict[str, Any]:
    """_normalize_params_for_process(6159-6461). ui_targets = 화면 G1~G3 Target 칸(재고 DB 값) — 레시피가 비었을 때."""
    log = log if log is not None else []
    ui = ui_targets or {}
    ch = int(chamber)

    def fget(key, default="0"):
        try:
            return float(str(raw.get(key, default)).strip())
        except Exception:
            return float(default)

    def iget(key, default="0"):
        try:
            return int(float(str(raw.get(key, default)).strip()))
        except Exception:
            return int(default)

    def s_(k):
        return str(raw.get(k, "")).strip()

    gr = [str(raw.get(f"G{i} Target", "") or raw.get(f"G{i}_target_name", "")).strip() or str(ui.get(f"G{i}", "") or "")
          for i in (1, 2, 3)]
    use_g = [bool(raw.get(f"use_g{i}", False)) or tf(raw.get(f"gun{i}", "F")) for i in (1, 2, 3)]
    if ch == 1:
        g1t, g2t, g3t = gr[0], "", ""
    else:
        g1t, g2t, g3t = (gr[i] if use_g[i] else "" for i in range(3))
    pos = s_("chuck_position").lower()
    if pos not in ("up", "mid", "down"):
        pos = ""

    def opt_int(k):
        s = s_(k)
        if s != "":
            try:
                return int(float(s))
            except Exception:
                return None
        return None

    def float_or(default, k):
        s = s_(k)
        if s != "":
            try:
                return float(s)
            except Exception:
                pass
        return float(default)

    def opt_float(k):
        s = s_(k)
        if s != "":
            try:
                return float(s)
            except Exception:
                return None
        return None

    def off_time(k):
        sv = s_(k)
        if sv == "":
            return None
        if sv.upper() == "DC":
            return "DC"
        try:
            return round(float(sv), 1)
        except Exception:
            log.append(("Params", f"[경고] {k}='{sv}' 해석 불가 → 유지"))
            return None

    use_dc_pulse = tf(raw.get("use_dc_pulse", "F"))
    dc_pulse_power = float_or("0", "dc_pulse_power")
    dc_pulse_freq = opt_int("dc_pulse_freq")
    dc_pulse_off = off_time("dc_pulse_off_time")
    duty_legacy = opt_int("dc_pulse_duty_cycle")
    use_rf_pulse = tf(raw.get("use_rf_pulse", "F"))
    rf_pulse_power = float_or("0", "rf_pulse_power")
    rf_pulse_freq = opt_float("rf_pulse_freq")
    rf_pulse_duty = opt_int("rf_pulse_duty_cycle")
    if dc_pulse_off is None and duty_legacy is not None:
        if dc_pulse_freq:
            dc_pulse_off = _dcp_duty_to_off(float(dc_pulse_freq), float(duty_legacy))
            log.append(("Params", f"[호환] dc_pulse_duty_cycle {duty_legacy}% @{dc_pulse_freq}kHz "
                                  f"→ dc_pulse_off_time {dc_pulse_off:.1f}us 로 환산"))
        else:
            log.append(("Params", f"[경고] dc_pulse_duty_cycle={duty_legacy}% 가 있으나 dc_pulse_freq 가 비어 "
                                  "Off Time 으로 환산할 수 없습니다 → Off Time 유지(keep)"))
    if isinstance(dc_pulse_off, str):
        dc_pulse_duty = 100.0
    elif dc_pulse_off is not None and dc_pulse_freq:
        dc_pulse_duty = _dcp_off_to_duty(float(dc_pulse_freq), float(dc_pulse_off))
    else:
        dc_pulse_duty = None
    dep_rate = None
    for key in ("dep_rate", "dep.rate"):
        s = s_(key)
        if s:
            try:
                v = float(s)
                dep_rate = v if v > 0 else None
                break
            except Exception:
                pass
    thickness = fget("thickness", "0") or None
    process_time = fget("process_time", "0")
    if process_time <= 0.0 and dep_rate and thickness:
        process_time = float(thickness) / float(dep_rate) / 60.0
        log.append(("Params", f"process_time 자동 계산: {thickness}nm ÷ {dep_rate}nm/s = {process_time:.3f}분"))
    res: Dict[str, Any] = {
        "base_pressure": fget("base_pressure", "1e-5"), "working_pressure": fget("working_pressure", "0"),
        "process_time": process_time, "dep_rate": dep_rate, "thickness": thickness,
        "shutter_delay": fget("shutter_delay", "0"), "integration_time": iget("integration_time", "60"),
        "dc_power": fget("dc_power", "0"), "dc_power2": fget("dc_power2", "0"), "rf_power": fget("rf_power", "0"),
        "use_dc_pulse": use_dc_pulse, "dc_pulse_power": dc_pulse_power, "dc_pulse_freq": dc_pulse_freq,
        "dc_pulse_off_time": dc_pulse_off, "dc_pulse_duty": dc_pulse_duty,
        "use_rf_pulse": use_rf_pulse, "rf_pulse_power": rf_pulse_power, "rf_pulse_freq": rf_pulse_freq,
        "rf_pulse_duty": rf_pulse_duty,
        "use_rf_power": tf(raw.get("use_rf_power", "F")), "use_dc_power": tf(raw.get("use_dc_power", "F")),
        "use_dc_power2": tf(raw.get("use_dc_power2", "F")),
        "use_ar": tf(raw.get("Ar", "F")), "use_o2": tf(raw.get("O2", "F")), "use_n2": tf(raw.get("N2", "F")),
        "ar_flow": fget("Ar_flow", "0"), "o2_flow": fget("O2_flow", "0"), "n2_flow": fget("N2_flow", "0"),
        "use_g1": use_g[0], "use_g2": use_g[1], "use_g3": use_g[2], "use_ms": tf(raw.get("main_shutter", "F")),
        "process_note": raw.get("Process_name", raw.get("process_note", "")),
        "G1_target_name": g1t, "G2_target_name": g2t, "G3_target_name": g3t, "G1 Target": g1t, "G2 Target": g2t,
        "G3 Target": g3t, "use_power_select": tf(raw.get("power_select", "F")), "chuck_position": pos,
        "use_heater": tf(raw.get("use_heater", "F")), "heater_temp": opt_float("heater_temp"),
        "heater_ramp": opt_float("heater_ramp"), "heater_soak": opt_float("heater_soak"),
        "tooling": opt_float("tooling"), "density": opt_float("density"), "z_factor": opt_float("z_factor"),
    }
    if not res["use_heater"] and (res["heater_temp"] is not None or res["heater_ramp"] is not None
                                  or res["heater_soak"] is not None):
        log.append(("Params", f"use_heater=F 이므로 히터를 쓰지 않습니다 (heater_temp={res['heater_temp']}, "
                              f"heater_ramp={res['heater_ramp']}, heater_soak={res['heater_soak']} 은 무시)"))
        res["heater_temp"] = res["heater_ramp"] = res["heater_soak"] = None
    res["power_change_time"] = str(raw.get("power_change_time", "") or "").strip()
    res["change_power_value"] = str(raw.get("change_power_value", "") or "").strip()

    def norm_pulse(kind: str, supported: bool) -> None:
        use_key, p_key, f_key, d_key = f"use_{kind}_pulse", f"{kind}_pulse_power", f"{kind}_pulse_freq", f"{kind}_pulse_duty"
        label = "RF-Pulse" if kind == "rf" else "DC-Pulse"
        power, freq, duty = res.get(p_key, 0.0) or 0.0, res.get(f_key), res.get(d_key)

        def clear():
            res[use_key], res[p_key], res[f_key], res[d_key] = False, 0.0, None, None
            if kind == "dc":
                res["dc_pulse_off_time"] = None
        if bool(res.get(use_key)):
            if supported:
                return
            clear()
            log.append(("Params", f"CH{ch}: {label} 미지원 설정 → OFF 처리합니다."))
            return
        try:
            had = float(power) > 0.0
        except Exception:
            had = False
        had = had or (freq is not None) or (duty is not None)
        clear()
        if had:
            log.append(("Params", f"{label}: {use_key}=F 이므로 사용하지 않습니다 "
                                  f"(레시피의 power={power}, freq={freq}, duty={duty} 은 무시)."))

    norm_pulse("rf", bool(caps.rfp))
    norm_pulse("dc", bool(caps.dcp))
    if ch == 1:
        if res.get("use_n2") or (res.get("n2_flow", 0.0) or 0.0) > 0.0:
            log.append(("Params", "CH1은 N2 미지원 → N2 설정을 무시합니다."))
        res["use_n2"], res["n2_flow"] = False, 0.0
    if bool(raw.get("test_mode", False)):
        res["test_mode"] = True
        try:
            res["test_duration_sec"] = float(raw.get("test_duration_sec") or 0.0)
        except (TypeError, ValueError):
            res["test_duration_sec"] = 0.0
        res["time"] = str(raw.get("time", "") or "").strip()
    return res


@dataclass
class RowPlan:
    """레시피 한 행의 종류 — 엔진(큐)이 쓴다."""
    kind: str                          # "delay" · "process"(TEST 행 포함)
    raw: dict
    delay_s: float = 0.0
    label: str = ""
    disp: str = ""
    test: bool = False


def plan_row(raw: Mapping[str, Any]) -> RowPlan:
    """v1 _runner_stage_advance_queue 의 (A) delay 판정 → (B) TEST 표시(5100-5158). raw 는 복사해서 바꾼다."""
    row = dict(raw)
    sec, label, disp = delay_of(row)
    if sec > 0:
        return RowPlan("delay", row, sec, label, disp)
    return RowPlan("process", row, test=mark_test_row(row))

# util/gdrive_logger.py
"""
Google Drive 공정 로그 저장 모듈

동작:
- 공정 종료 시 G:/공유 드라이브/VanaM_Sputter/Process_log/CH{n}.xlsx 에 행 append
- 파워 소스가 여러 개면 파워별로 행 분리, 나머지 데이터는 동일하게 채움
- 파일 없으면 헤더/스타일 포함 신규 생성
- 이상치 셀 자동 강조 (Arc, Ref.p, Base Pressure, SP vs Avg 10% 초과)
- Arc 임계값 이상 시 Google Chat 알림 (공정당 1회)
- 메인 공정과 완전 독립: 예외 절대 전파 안 함, 실패 시 로컬 fallback
"""

import asyncio
import threading
import contextlib
import urllib.request
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from functools import partial

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ── 설정 기본값 (config_common.py 에서 오버라이드) ──────────────
_DEFAULT_GDRIVE_DIR     = Path("G:/공유 드라이브/VanaM_Sputter/Process_log")
_DEFAULT_ARC_THRESH     = 5
_DEFAULT_REFP_WARN_W    = 20.0
# [수정 3] SP vs Avg 편차 강조 기준 (비율)
_SP_DIFF_RATIO          = 0.10   # 10% 이상 편차 시 강조

# ── 색상 ────────────────────────────────────────────────────────
_T = {
    "기본 정보"       : ("1C2833", "2C3E50", "ABB2B9"),
    "Plasma Cleaning" : ("2E4057", "4A6278", "BDC3C7"),
    "Main Process"    : ("2E4057", "4A6278", "BDC3C7"),  # ← 엑셀 포맷과 동일하게 변경
}
_WHITE    = "FFFFFF"
_ROW_ODD  = "FFFFFF"
_ROW_EVEN = "F2F3F4"
_WARN_ARC = "FADBD8"   # 이상치 — 빨강으로 통일
_FG       = "1C2833"

# ── Main Process 시트 컬럼 정의 ─────────────────────────────────
_COLS_MAIN: List[Tuple] = [
    # 기본 정보 (13개, A~M)
    ("날짜",           "YYYY-MM-DD HH:MM:SS", 19, "기본 정보",    "timestamp"),
    ("담당자",         "",              8,  "기본 정보",    "operator"),
    ("Process Name",   "",             16,  "기본 정보",    "process_name"),
    ("비고",           "",             18,  "기본 정보",    "note"),
    ("기판",           "",             12,  "기본 정보",    "substrate"),
    ("Main Shutter",   "T/F",           9,  "기본 정보",    "main_shutter"),
    ("Power Select",   "T/F",           9,  "기본 정보",    "power_select"),
    ("G1 Target",      "",              9,  "기본 정보",    "G1 Target"),
    ("G2 Target",      "",              9,  "기본 정보",    "G2 Target"),
    ("G3 Target",      "",              9,  "기본 정보",    "G3 Target"),
    ("Dep.rate",       "nm/s",         10,  "기본 정보",    "dep_rate"),
    ("Thickness",      "nm",            9,  "기본 정보",    "thickness"),
    ("Chuck",          "up/mid/down",  11,  "기본 정보",    "chuck_position"),
    # Main Process (25개, N~AL)
    ("Shutter Delay",  "min",           8,  "Main Process", "shutter_delay"),
    ("Process Time",   "min",           8,  "Main Process", "process_time"),
    ("Base Pressure",  "Torr",         12,  "Main Process", "base_pressure"),
    ("SP Ar",          "sccm",          8,  "Main Process", "sp_ar"),
    ("Avg Ar",         "sccm",          8,  "Main Process", "avg_ar"),
    ("SP N2",          "sccm",          8,  "Main Process", "sp_n2"),
    ("Avg N2",         "sccm",          8,  "Main Process", "avg_n2"),
    ("SP O2",          "sccm",          8,  "Main Process", "sp_o2"),
    ("Avg O2",         "sccm",          8,  "Main Process", "avg_o2"),
    ("SP Pressure",    "mTorr",         9,  "Main Process", "sp_pressure"),
    ("Avg Pressure",   "mTorr",         9,  "Main Process", "avg_pressure"),
    ("Power Source",   "DC/RF/DCPulse/RFPulse", 19, "Main Process", "power_source"),
    ("SP Power",       "W",             8,  "Main Process", "sp_power"),
    ("Avg Power",      "W",             9,  "Main Process", "avg_power"),
    ("Avg For.p",      "W",             9,  "Main Process", "avg_forp"),
    ("Avg Ref.p",      "W",             9,  "Main Process", "avg_refp"),
    ("Avg Load",       "a.u.",          8,  "Main Process", "avg_load"),
    ("Avg Tune",       "a.u.",          8,  "Main Process", "avg_tune"),
    ("Avg Voltage",    "V",             9,  "Main Process", "avg_voltage"),
    ("Avg Current",    "A",             9,  "Main Process", "avg_current"),
    ("Duty Cycle",     "%",             8,  "Main Process", "duty_cycle"),
    ("Frequency",      "kHz",           8,  "Main Process", "frequency"),
    ("Off Time",       "μs",            8,  "Main Process", "off_time"),
    ("Soft Arc",       "count",         8,  "Main Process", "soft_arc"),
    ("Hard Arc",       "count",         8,  "Main Process", "hard_arc"),
]

# ── Plasma Cleaning 시트 컬럼 정의 ──────────────────────────────
_COLS_PC: List[Tuple] = [
    # 기본 정보 (5개 — Main Shutter/Power Select/Gun/Chuck 없음)
    ("날짜",           "YYYY-MM-DD HH:MM:SS", 19, "기본 정보",       "timestamp"),
    ("담당자",         "",              8,  "기본 정보",       "operator"),
    ("Process Name",   "",             16,  "기본 정보",       "process_name"),
    ("비고",           "",             18,  "기본 정보",       "note"),
    ("기판",           "",             12,  "기본 정보",       "substrate"),
    # Plasma Cleaning (11개)
    ("Time",           "min",           7,  "Plasma Cleaning", "pc_time"),
    ("Base Pressure",  "Torr",         12,  "Plasma Cleaning", "pc_base_pressure"),
    ("SP Ar",          "sccm",          8,  "Plasma Cleaning", "pc_sp_ar"),
    ("Avg Ar",         "sccm",          8,  "Plasma Cleaning", "pc_avg_ar"),
    ("SP Pressure",    "mTorr",         9,  "Plasma Cleaning", "pc_sp_pressure"),
    ("Avg Pressure",   "mTorr",         9,  "Plasma Cleaning", "pc_avg_pressure"),
    ("SP Power",       "W",             8,  "Plasma Cleaning", "pc_sp_power"),
    ("Avg For.p",      "W",             9,  "Plasma Cleaning", "pc_avg_forp"),
    ("Avg Ref.p",      "W",             9,  "Plasma Cleaning", "pc_avg_refp"),
    ("Avg Load",       "a.u.",          8,  "Plasma Cleaning", "pc_avg_load"),
    ("Avg Tune",       "a.u.",          8,  "Plasma Cleaning", "pc_avg_tune"),
]

# 시트별 그룹 인덱스 계산
_GRP_COLS_MAIN: Dict[str, List[int]] = {}
for _ci, (*_, _grp, _key) in enumerate(_COLS_MAIN, 1):
    _GRP_COLS_MAIN.setdefault(_grp, []).append(_ci)
_GRP_STARTS_MAIN = {cols[0] for cols in _GRP_COLS_MAIN.values()}

_GRP_COLS_PC: Dict[str, List[int]] = {}
for _ci, (*_, _grp, _key) in enumerate(_COLS_PC, 1):
    _GRP_COLS_PC.setdefault(_grp, []).append(_ci)
_GRP_STARTS_PC = {cols[0] for cols in _GRP_COLS_PC.values()}

# ── 내부 락 ──────────────────────────────────────────────────────
_write_lock = threading.Lock()


# ════════════════════════════════════════════════════════════════
# 스타일 헬퍼
# ════════════════════════════════════════════════════════════════

def _cs(ws, r: int, c: int, val=None, bg: Optional[str] = None,
        fg: str = _FG, bold: bool = False, size: int = 9,
        ha: str = "center", wrap: bool = False,
        nf: Optional[str] = None, italic: bool = False):
    cell = ws.cell(row=r, column=c)
    cell.value = val
    cell.font = Font(name="Arial", bold=bold, size=size,
                     color=fg, italic=italic)
    cell.alignment = Alignment(horizontal=ha, vertical="center",
                                wrap_text=wrap)
    if bg:
        cell.fill = PatternFill("solid", fgColor=bg)
    if nf:
        cell.number_format = nf
    return cell


def _build_headers(ws, cols: List[Tuple], grp_cols: Dict, grp_starts: set) -> None:
    """Row 1 그룹 / Row 2 컬럼명 / Row 3 단위 헤더 생성."""
    thin  = Side(style="thin",   color="E0E0E0")
    thick = Side(style="medium", color="FFFFFF")

    for grp, gcols in grp_cols.items():
        c1, c2 = gcols[0], gcols[-1]
        bg = _T[grp][0]
        if c1 < c2:
            ws.merge_cells(
                f"{get_column_letter(c1)}1:{get_column_letter(c2)}1")
        cell = ws.cell(row=1, column=c1)
        cell.value = grp
        cell.font = Font(name="Arial", bold=True, size=9, color=_WHITE)
        cell.fill = PatternFill("solid", fgColor=bg)
        cell.alignment = Alignment(horizontal="center", vertical="center")
        for c in gcols[1:]:
            with contextlib.suppress(AttributeError):
                ws.cell(row=1, column=c).fill = \
                    PatternFill("solid", fgColor=bg)
    ws.row_dimensions[1].height = 16

    for ci, (name, unit, width, grp, _) in enumerate(cols, 1):
        ws.column_dimensions[get_column_letter(ci)].width = width
        _cs(ws, 2, ci, name, bg=_T[grp][1], fg=_WHITE,
            bold=True, size=9, wrap=True)
        _cs(ws, 3, ci, unit or "", bg=_T[grp][2], fg="4A4A4A",
            size=8, italic=True)
    ws.row_dimensions[2].height = 32
    ws.row_dimensions[3].height = 13

    for r in range(1, 4):
        for ci in range(1, len(cols) + 1):
            left = thick if ci in grp_starts else thin
            ws.cell(row=r, column=ci).border = Border(
                left=left, right=thin, top=thin, bottom=thin)


def _get_or_create_wb(path: Path, sheet_name: str,
                       cols: List[Tuple], grp_cols: Dict, grp_starts: set):
    """파일 열기(기존) 또는 신규 생성. 시트명 기준으로 시트 선택. (wb, ws) 반환."""
    if path.exists():
        try:
            wb = load_workbook(str(path))
            if sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
            else:
                # 파일은 있는데 해당 시트가 없으면 새로 추가
                ws = wb.create_sheet(sheet_name)
                ws.sheet_view.showGridLines = False
                ws.freeze_panes = "A4"
                _build_headers(ws, cols, grp_cols, grp_starts)
            return wb, ws
        except Exception:
            broken = path.with_suffix(".broken.xlsx")
            with contextlib.suppress(Exception):
                path.rename(broken)

    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name
    ws.sheet_view.showGridLines = False
    ws.freeze_panes = "A4"
    _build_headers(ws, cols, grp_cols, grp_starts)
    return wb, ws


def _next_data_row(ws) -> int:
    """다음 빈 데이터 행 번호 반환 (헤더 3행 다음부터).

    [수정] ws.max_row는 서식(폰트/배경색)만 있고 값이 없는 phantom row도
    포함하므로 그대로 사용하면 데이터가 수백 행 아래로 밀린다.
    바닥에서 위로 스캔해 실제 값이 있는 마지막 행을 찾아 +1을 반환한다.
    """
    for r in range(ws.max_row, 3, -1):
        if any(cell.value is not None for cell in ws[r]):
            return r + 1
    return 4


# [수정 3] SP vs Avg 비교 쌍 정의
_SP_AVG_PAIRS = {
    "avg_ar"         : "sp_ar",
    "avg_n2"         : "sp_n2",
    "avg_o2"         : "sp_o2",
    "avg_pressure"   : "sp_pressure",
    "avg_forp"       : "sp_power",
    "avg_power"      : "sp_power",
    # pc_avg_* 항목 제거 — Main Process 시트에 PC 컬럼 없으므로
}


def _cell_bg(key: str, value: Any, row_bg: str,
             arc_thresh: int, refp_warn: float,
             data: Dict[str, Any]) -> str:
    """
    이상치 여부로 배경색 결정 (빨강으로 통일).
    - Arc 누적 >= arc_thresh → 빨강
    - SP vs Avg 10% 초과 편차 → 빨강
    """
    # Arc 누적 임계값 이상
    if key in ("soft_arc", "hard_arc"):
        if isinstance(value, (int, float)) and value >= arc_thresh:
            return _WARN_ARC

    # SP vs Avg 편차 (10% 초과 시 강조)
    if key in _SP_AVG_PAIRS:
        sp_key = _SP_AVG_PAIRS[key]
        sp_val = data.get(sp_key)
        if (isinstance(value, (int, float))
                and isinstance(sp_val, (int, float))
                and sp_val > 0):
            if abs(value - sp_val) / sp_val > _SP_DIFF_RATIO:
                return _WARN_ARC

    return row_bg


def _write_data_row(ws, row_num: int, data: Dict[str, Any],
                    arc_thresh: int, refp_warn: float,
                    cols: List[Tuple]) -> None:
    """데이터 딕셔너리를 ws의 row_num 행에 기록.

    [수정] 모든 값이 None인 빈 행에는 서식도 적용하지 않는다.
    서식만 적용된 행(phantom row)은 ws.max_row에 카운트되어
    _next_data_row()가 잘못된 행 번호를 반환하는 원인이 된다.
    """
    # 실제 쓸 값이 하나도 없으면 phantom row 방지를 위해 아무것도 하지 않음
    values = {key: data.get(key) for _, __, ___, ____, key in cols}
    if not any(v is not None and v != "" for v in values.values()):
        return

    thin = Side(style="thin", color="CCCCCC")
    brd  = Border(left=thin, right=thin, top=thin, bottom=thin)
    row_bg = _ROW_ODD if row_num % 2 == 0 else _ROW_EVEN
    left_cols = {3, 4, 5}   # Process Name, 비고, 기판

    for ci, (_, __, ___, ____, key) in enumerate(cols, 1):
        value = values[key]
        if value == "":
            value = None

        # [수정 3] data 딕셔너리 전달
        bg = _cell_bg(key, value, row_bg, arc_thresh, refp_warn, data)

        cell = ws.cell(row=row_num, column=ci)
        cell.value = value
        cell.font = Font(name="Arial", size=9, color=_FG)
        cell.fill = PatternFill("solid", fgColor=bg)
        cell.border = brd
        cell.alignment = Alignment(
            horizontal="left" if ci in left_cols else "center",
            vertical="center")

        if key == "timestamp" and isinstance(value, datetime):
            cell.number_format = "YYYY-MM-DD HH:MM:SS"
        if key in ("base_pressure", "pc_base_pressure") \
                and isinstance(value, (int, float)):
            cell.number_format = "0.00E+00"

    ws.row_dimensions[row_num].height = 18


# ════════════════════════════════════════════════════════════════
# 알림
# ════════════════════════════════════════════════════════════════

def _send_arc_chat(ch: int, process_name: str,
                   soft: int, hard: int,
                   webhook_url: str) -> None:
    """Google Chat Arc 경고 알림 (동기, 스레드 내)."""
    if not webhook_url:
        return
    with contextlib.suppress(Exception):
        body = json.dumps({
            "text": (
                f"⚠️ *CH{ch} Arc 경고*\n"
                f"공정: `{process_name}`\n"
                f"Soft Arc: {soft}회 / Hard Arc: {hard}회 "
                f"(합계 {soft + hard}회)"
            )
        }).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)


# ════════════════════════════════════════════════════════════════
# 동기 저장 (스레드에서 실행)
# ════════════════════════════════════════════════════════════════

def _save_sync(
    ch: int,
    rows: List[Dict[str, Any]],
    log_dir: Path,
    arc_thresh: int,
    refp_warn: float,
    arc_alert_sent: bool,
    webhook_url: str,
    local_fallback_dir: Path,
    sheet_name: str,
    cols: List[Tuple],
    grp_cols: Dict,
    grp_starts: set,
) -> bool:
    """
    실제 Excel 저장. sheet_name 기준으로 해당 시트에 기록.
    반환값: 이번 호출에서 Arc 알림을 발송했으면 True.
    """
    def _do_save(target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        with _write_lock:
            wb, ws = _get_or_create_wb(target, sheet_name, cols, grp_cols, grp_starts)
            row_num = _next_data_row(ws)
            for row_data in rows:
                _write_data_row(ws, row_num, row_data, arc_thresh, refp_warn, cols)
                row_num += 1
            wb.save(str(target))

    main_path = log_dir / f"CH{ch}.xlsx"
    try:
        _do_save(main_path)
    except Exception:
        with contextlib.suppress(Exception):
            fb_path = local_fallback_dir / f"CH{ch}_pending.xlsx"
            _do_save(fb_path)

    # Arc 알림 — 첫 번째 행 기준 (Main Process 시트에만 Arc 있음)
    soft = int(rows[0].get("soft_arc") or 0)
    hard = int(rows[0].get("hard_arc") or 0)
    if (soft + hard) >= arc_thresh and not arc_alert_sent and webhook_url:
        with contextlib.suppress(Exception):
            _send_arc_chat(
                ch,
                str(rows[0].get("process_name", "")),
                soft, hard, webhook_url,
            )
        return True

    return False


# ════════════════════════════════════════════════════════════════
# 데이터 빌드 헬퍼
# ════════════════════════════════════════════════════════════════

def _avg(seq: list) -> Optional[float]:
    """리스트 평균. 빈 리스트면 None."""
    if not seq:
        return None
    return round(sum(seq) / len(seq), 4)


def _build_data(
    ch: int,
    data_logger,
    operator: str,
    substrate: str,
    note: str,
    pc_params: Optional[Dict],
) -> List[Dict[str, Any]]:   # [수정 4] 반환 타입: 단일 dict → 리스트
    """
    DataLogger 인스턴스에서 Excel 행 데이터 생성.
    [수정 4] 파워 소스가 여러 개면 파워별 행 분리, 나머지 데이터 동일하게 복사.
    """
    dl = data_logger
    pp = dl.process_params

    # 타임스탬프
    ts = getattr(dl, "_session_started_at", None) or datetime.now()

    # Base Pressure: 공정 중 IG 최솟값
    if getattr(dl, "ig_pressure_readings", []):
        base_pres = min(dl.ig_pressure_readings)
    else:
        base_pres = pp.get("base_pressure")

    # ── 공통 데이터 (파워 소스와 무관한 모든 필드) ──────────────
    base_data: Dict[str, Any] = {
        # 기본 정보
        "timestamp"      : ts,
        "operator"       : operator or pp.get("operator", ""),
        "process_name"   : (pp.get("process_name")
                            or pp.get("Process_name", "")),
        "note"           : note,
        "substrate"      : substrate,
        "main_shutter"   : "T" if pp.get("use_ms") else "F",
        "power_select"   : "T" if pp.get("use_power_select") else "F",
        "G1 Target"      : pp.get("G1 Target", ""),
        "G2 Target"      : pp.get("G2 Target", ""),
        "G3 Target"      : pp.get("G3 Target", ""),
        "chuck_position" : pp.get("chuck_position", ""),
        # Main Process — 가스/압력 (파워와 무관)
        "shutter_delay"  : pp.get("shutter_delay"),
        "process_time"   : pp.get("process_time"),
        "dep_rate"       : pp.get("dep_rate"),
        "thickness"      : pp.get("thickness"),
        "base_pressure"  : base_pres,
        # [수정 2] 실제 TypedDict 키명으로 수정: Ar_flow(대문자), N2_flow, O2_flow
        "sp_ar"          : pp.get("Ar_flow") or pp.get("ar_flow"),
        "sp_n2"          : pp.get("N2_flow") or pp.get("n2_flow"),
        "sp_o2"          : pp.get("O2_flow") or pp.get("o2_flow"),
        "sp_pressure"    : pp.get("working_pressure") or pp.get("sp_pressure"),
        "avg_ar"         : _avg(dl.mfc_flow_readings.get("Ar", [])),
        "avg_n2"         : _avg(dl.mfc_flow_readings.get("N2", [])),
        "avg_o2"         : _avg(dl.mfc_flow_readings.get("O2", [])),
        "avg_pressure"   : _avg(dl.mfc_pressure_readings),
        # Arc
        "soft_arc"       : int(pp.get("soft_arc_count") or 0),
        "hard_arc"       : int(pp.get("hard_arc_count") or 0),
    }

    # ── [수정 4] 파워 소스별 행 분리 ────────────────────────────
    power_rows = []

    if pp.get("use_dc_pulse"):
        power_rows.append({
            "power_source" : "DC Pulse",
            "sp_power"     : pp.get("dc_pulse_power"),
            "avg_power"    : _avg(getattr(dl, "dc_pulse_power_readings", [])),
            "avg_forp"     : None,   # ← DC Pulse는 Forward Power 없음
            "avg_refp"     : None,
            "avg_load"     : None,
            "avg_tune"     : None,
            "avg_voltage"  : _avg(getattr(dl, "dc_pulse_voltage_readings", [])),
            "avg_current"  : _avg(getattr(dl, "dc_pulse_current_readings", [])),
            # [수정 2] dc_pulse_duty_cycle (TypedDict 실제 키명)
            "duty_cycle"   : pp.get("dc_pulse_duty_cycle") or pp.get("dc_pulse_duty"),
            "frequency"    : pp.get("dc_pulse_freq"),
            "off_time"     : pp.get("dc_pulse_off_time_us"),
        })
    elif pp.get("use_dc_power"):
        power_rows.append({
            "power_source" : "DC",
            "sp_power"     : pp.get("dc_power"),
            "avg_power"    : _avg(getattr(dl, "dc_power_readings", [])),
            "avg_forp"     : None,
            "avg_refp"     : None,
            "avg_load"     : None,
            "avg_tune"     : None,
            "avg_voltage"  : _avg(getattr(dl, "dc_voltage_readings", [])),
            "avg_current"  : _avg(getattr(dl, "dc_current_readings", [])),
            "duty_cycle"   : None,
            "frequency"    : None,
            "off_time"     : None,
        })

    if pp.get("use_rf_pulse"):
        power_rows.append({
            "power_source" : "RF Pulse",
            "sp_power"     : pp.get("rf_pulse_power"),
            "avg_forp"     : _avg(getattr(dl, "rf_pulse_for_p_readings", [])),
            "avg_refp"     : _avg(getattr(dl, "rf_pulse_ref_p_readings", [])),
            "avg_load"     : _avg(getattr(dl, "rf_load_readings", [])),
            "avg_tune"     : _avg(getattr(dl, "rf_tune_readings", [])),
            "avg_voltage"  : None,
            "avg_current"  : None,
            # [수정 2] rf_pulse_duty_cycle (TypedDict 실제 키명)
            "duty_cycle"   : pp.get("rf_pulse_duty_cycle"),
            "frequency"    : pp.get("rf_pulse_freq"),
            "off_time"     : pp.get("rf_pulse_off_time_us"),
        })
    elif pp.get("use_rf_power"):
        power_rows.append({
            "power_source" : "RF",
            "sp_power"     : pp.get("rf_power"),
            "avg_forp"     : _avg(getattr(dl, "rf_for_p_readings", [])),
            "avg_refp"     : _avg(getattr(dl, "rf_ref_p_readings", [])),
            "avg_load"     : _avg(getattr(dl, "rf_load_readings", [])),
            "avg_tune"     : _avg(getattr(dl, "rf_tune_readings", [])),
            "avg_voltage"  : None,
            "avg_current"  : None,
            "duty_cycle"   : None,
            "frequency"    : None,
            "off_time"     : None,
        })

    # 파워 소스가 없으면 base_data 그대로 1행
    if not power_rows:
        return [base_data]

    # 파워별로 base_data 복사 후 파워 필드만 덮어쓰기
    result = []
    for pr in power_rows:
        row = dict(base_data)
        row.update(pr)
        result.append(row)
    return result


# ════════════════════════════════════════════════════════════════
# PC 단독 행 저장 (Plasma Cleaning 데이터만 있을 때)
# ════════════════════════════════════════════════════════════════

def _build_pc_only_data(pc_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """PC 데이터만으로 1개 행 생성 — Plasma Cleaning 시트 전용."""
    _pc = pc_params or {}
    row: Dict[str, Any] = {
        # 기본 정보 (5개만 — Plasma Cleaning 시트 스펙)
        "timestamp"       : datetime.now(),
        "operator"        : "",
        "process_name"    : _pc.get("process_name", "Plasma Cleaning"),
        "note"            : "",
        "substrate"       : "",
        # Plasma Cleaning
        "pc_time"         : _pc.get("time"),
        "pc_base_pressure": _pc.get("base_pressure"),
        "pc_sp_ar"        : _pc.get("sp_ar"),
        "pc_avg_ar"       : _pc.get("avg_ar"),
        "pc_sp_pressure"  : _pc.get("sp_pressure"),
        "pc_avg_pressure" : _pc.get("avg_pressure"),
        "pc_sp_power"     : _pc.get("sp_power"),
        "pc_avg_forp"     : _pc.get("avg_forp"),
        "pc_avg_refp"     : _pc.get("avg_refp"),
        "pc_avg_load"     : _pc.get("avg_load"),
        "pc_avg_tune"     : _pc.get("avg_tune"),
    }
    return [row]


def save_pc_only(
    ch: int,
    pc_params: Dict[str, Any],
    log_dir: Path,
    arc_thresh: int = _DEFAULT_ARC_THRESH,
    refp_warn: float = _DEFAULT_REFP_WARN_W,
) -> None:
    """
    PC 단독 행을 xlsx에 동기 저장.
    main.py의 _save_pc_only_row → run_in_executor에서 호출.
    예외를 절대 전파하지 않음.
    """
    # [수정] 예외를 전파하지 않음 — PC 저장 실패가 메인 공정에 영향을 주면 안 됨
    try:
        rows = _build_pc_only_data(pc_params)
        fb_dir = log_dir / "LocalFallback"
        _save_sync(
            ch, rows, log_dir,
            arc_thresh, refp_warn,
            False, "",
            fb_dir,
            "Plasma Cleaning", _COLS_PC, _GRP_COLS_PC, _GRP_STARTS_PC,
        )
    except Exception as e:
        with contextlib.suppress(Exception):
            import logging
            logging.getLogger(__name__).warning("GDrive save_pc_only 실패: %s", e)


# ════════════════════════════════════════════════════════════════
# 공개 API
# ════════════════════════════════════════════════════════════════

async def save_process_log(
    ch: int,
    data_logger,
    *,
    operator:         str = "",
    substrate:        str = "",
    note:             str = "",
    pc_params:        Optional[Dict] = None,
    log_dir:          Optional[Path] = None,
    arc_alert_sent:   bool = False,
    webhook_url:      str = "",
    local_fallback:   Optional[Path] = None,
    arc_thresh:       int = _DEFAULT_ARC_THRESH,
    refp_warn:        float = _DEFAULT_REFP_WARN_W,
) -> bool:
    """
    비동기 진입점 — chamber_runtime.py 에서 asyncio.create_task() 로 호출.
    메인 공정에 영향 없음: 모든 예외를 내부에서 처리.
    반환값: 이번 호출에서 Arc 알림 발송 여부.
    """
    # [수정] 예외를 전파하지 않음 — GDrive 저장 실패가 메인 공정에 영향을 주면 안 됨
    try:
        rows = _build_data(ch, data_logger, operator, substrate,
                           note, pc_params)
    except Exception as e:
        with contextlib.suppress(Exception):
            import logging
            logging.getLogger(__name__).warning("GDrive _build_data 실패: %s", e)
        return False

    target_dir = log_dir or _DEFAULT_GDRIVE_DIR
    fb_dir     = local_fallback or (Path.cwd() / "Logs_LocalFallback")

    loop = asyncio.get_event_loop()
    try:
        sent = await loop.run_in_executor(
            None,
            partial(
                _save_sync,
                ch, rows, target_dir,
                arc_thresh, refp_warn,
                arc_alert_sent, webhook_url,
                fb_dir,
                "Main Process", _COLS_MAIN, _GRP_COLS_MAIN, _GRP_STARTS_MAIN,
            ),
        )
        return sent
    except Exception as e:
        with contextlib.suppress(Exception):
            import logging
            logging.getLogger(__name__).warning("GDrive _save_sync 실패: %s", e)
        return False
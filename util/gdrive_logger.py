# util/gdrive_logger.py
"""
Google Drive 공정 로그 저장 모듈

동작:
- 공정 종료 시 G:/공유 드라이브/VanaM_Sputter/Process_log/CH{n}.xlsx 에 1행 append
- 파일 없으면 헤더/스타일 포함 신규 생성
- 이상치 셀 자동 강조 (Arc >= 임계값, Ref.p 초과, Base Pressure 불량)
- Arc 임계값 이상 시 Google Chat 알림 (공정당 1회)
- 메인 공정과 완전 독립 — 저장 실패 시 로컬 fallback, 예외 절대 전파 안 함
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

# ── 색상 팔레트 ──────────────────────────────────────────────────
_T = {
    "기본 정보"       : ("1C2833", "2C3E50", "ABB2B9"),
    "Plasma Cleaning" : ("2E4057", "4A6278", "BDC3C7"),
    "Main Process"    : ("1C2833", "2C3E50", "ABB2B9"),
}
_WHITE    = "FFFFFF"
_ROW_ODD  = "FFFFFF"
_ROW_EVEN = "F2F3F4"
_WARN_ARC = "FADBD8"
_WARN_REF = "FDEBD0"
_WARN_PRE = "FFF9C4"
_FG       = "1C2833"

# ── 컬럼 정의: (헤더, 단위, 너비, 그룹, data_key) ────────────────
_COLS: List[Tuple] = [
    # 기본 정보
    ("날짜",            "YYYY-MM-DD HH:MM:SS", 19, "기본 정보",       "timestamp"),
    ("담당자",          "",              8,  "기본 정보",       "operator"),
    ("Process Name",    "",             16,  "기본 정보",       "process_name"),
    ("비고",            "",             18,  "기본 정보",       "note"),
    ("기판",            "",             12,  "기본 정보",       "substrate"),
    ("Main Shutter",    "T/F",           8,  "기본 정보",       "main_shutter"),
    ("Power Select",    "T/F",           8,  "기본 정보",       "power_select"),
    ("G1 Target",       "",              9,  "기본 정보",       "G1 Target"),
    ("G2 Target",       "",              9,  "기본 정보",       "G2 Target"),
    ("G3 Target",       "",              9,  "기본 정보",       "G3 Target"),
    ("Chuck",           "up/mid/down",  11,  "기본 정보",       "chuck_position"),
    # Plasma Cleaning
    ("Time",            "min",           7,  "Plasma Cleaning", "pc_time"),
    ("Base Pressure",   "Torr",         12,  "Plasma Cleaning", "pc_base_pressure"),
    ("SP Ar",           "sccm",          8,  "Plasma Cleaning", "pc_sp_ar"),
    ("Avg Ar",          "sccm",          8,  "Plasma Cleaning", "pc_avg_ar"),
    ("SP Pressure",     "mTorr",         9,  "Plasma Cleaning", "pc_sp_pressure"),
    ("Avg Pressure",    "mTorr",         9,  "Plasma Cleaning", "pc_avg_pressure"),
    ("SP Power",        "W",             8,  "Plasma Cleaning", "pc_sp_power"),
    ("Avg For.p",       "W",             9,  "Plasma Cleaning", "pc_avg_forp"),
    ("Avg Ref.p",       "W",             9,  "Plasma Cleaning", "pc_avg_refp"),
    ("Avg Load",        "a.u.",          8,  "Plasma Cleaning", "pc_avg_load"),
    ("Avg Tune",        "a.u.",          8,  "Plasma Cleaning", "pc_avg_tune"),
    # Main Process
    ("Shutter Delay",   "min",           9,  "Main Process",    "shutter_delay"),
    ("Process Time",    "min",           9,  "Main Process",    "process_time"),
    ("Base Pressure",   "Torr",         12,  "Main Process",    "base_pressure"),
    ("SP Ar",           "sccm",          8,  "Main Process",    "sp_ar"),
    ("Avg Ar",          "sccm",          8,  "Main Process",    "avg_ar"),
    ("SP N2",           "sccm",          8,  "Main Process",    "sp_n2"),
    ("Avg N2",          "sccm",          8,  "Main Process",    "avg_n2"),
    ("SP O2",           "sccm",          8,  "Main Process",    "sp_o2"),
    ("Avg O2",          "sccm",          8,  "Main Process",    "avg_o2"),
    ("SP Pressure",     "mTorr",         9,  "Main Process",    "sp_pressure"),
    ("Avg Pressure",    "mTorr",         9,  "Main Process",    "avg_pressure"),
    ("Power Source",    "DC/RF/Pulse",  10,  "Main Process",    "power_source"),
    ("SP Power",        "W",             8,  "Main Process",    "sp_power"),
    ("Avg For.p",       "W",             9,  "Main Process",    "avg_forp"),
    ("Avg Ref.p",       "W",             9,  "Main Process",    "avg_refp"),
    ("Avg Load",        "a.u.",          8,  "Main Process",    "avg_load"),
    ("Avg Tune",        "a.u.",          8,  "Main Process",    "avg_tune"),
    ("Avg Voltage",     "V",             9,  "Main Process",    "avg_voltage"),
    ("Avg Current",     "A",             9,  "Main Process",    "avg_current"),
    ("Duty Cycle",      "%",             8,  "Main Process",    "duty_cycle"),
    ("Frequency",       "kHz",           8,  "Main Process",    "frequency"),
    ("Soft Arc",        "count",         8,  "Main Process",    "soft_arc"),
    ("Hard Arc",        "count",         8,  "Main Process",    "hard_arc"),
]

# 그룹별 컬럼 인덱스 목록 (1-based)
_GRP_COLS: Dict[str, List[int]] = {}
for _ci, (*_, _grp, _key) in enumerate(_COLS, 1):
    _GRP_COLS.setdefault(_grp, []).append(_ci)

_GRP_STARTS = {cols[0] for cols in _GRP_COLS.values()}

# ── 내부 락 (프로세스 내 동시 쓰기 방지) ──────────────────────────
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


def _build_headers(ws) -> None:
    """Row 1 그룹 / Row 2 컬럼명 / Row 3 단위 헤더 생성."""
    thin  = Side(style="thin",   color="E0E0E0")
    thick = Side(style="medium", color="FFFFFF")

    # Row 1: 그룹 헤더 (병합 + 색상)
    for grp, cols in _GRP_COLS.items():
        c1, c2 = cols[0], cols[-1]
        bg = _T[grp][0]
        if c1 < c2:
            ws.merge_cells(
                f"{get_column_letter(c1)}1:{get_column_letter(c2)}1")
        cell = ws.cell(row=1, column=c1)
        cell.value = grp
        cell.font = Font(name="Arial", bold=True, size=9, color=_WHITE)
        cell.fill = PatternFill("solid", fgColor=bg)
        cell.alignment = Alignment(horizontal="center", vertical="center")
        for c in cols[1:]:
            with contextlib.suppress(AttributeError):
                ws.cell(row=1, column=c).fill = \
                    PatternFill("solid", fgColor=bg)
    ws.row_dimensions[1].height = 16

    # Row 2: 컬럼 헤더 / Row 3: 단위
    for ci, (name, unit, width, grp, _) in enumerate(_COLS, 1):
        ws.column_dimensions[get_column_letter(ci)].width = width
        _cs(ws, 2, ci, name, bg=_T[grp][1], fg=_WHITE,
            bold=True, size=9, wrap=True)
        _cs(ws, 3, ci, unit or "", bg=_T[grp][2], fg="4A4A4A",
            size=8, italic=True)
    ws.row_dimensions[2].height = 32
    ws.row_dimensions[3].height = 13

    # 경계선: 그룹 시작 굵게, 나머지 얇게
    for r in range(1, 4):
        for ci in range(1, len(_COLS) + 1):
            left = thick if ci in _GRP_STARTS else thin
            ws.cell(row=r, column=ci).border = Border(
                left=left, right=thin, top=thin, bottom=thin)


def _get_or_create_wb(path: Path):
    """파일 열기(기존) 또는 신규 생성. (wb, ws) 반환."""
    if path.exists():
        try:
            wb = load_workbook(str(path))
            ws = wb.active
            return wb, ws
        except Exception:
            # 파일 손상 시 백업 후 신규 생성
            broken = path.with_suffix(".broken.xlsx")
            with contextlib.suppress(Exception):
                path.rename(broken)

    wb = Workbook()
    ws = wb.active
    ws.title = "공정 로그"
    ws.sheet_view.showGridLines = False
    ws.freeze_panes = "A4"
    _build_headers(ws)
    return wb, ws


def _next_data_row(ws) -> int:
    """다음 빈 데이터 행 번호 반환 (헤더 3행 다음부터)."""
    return max(4, ws.max_row + 1)


def _cell_bg(key: str, value: Any,
             row_bg: str, arc_thresh: int, refp_warn: float) -> str:
    """이상치 여부로 배경색 결정."""
    if key in ("soft_arc", "hard_arc"):
        if isinstance(value, (int, float)) and value > 0:
            return _WARN_ARC
    if key == "avg_refp":
        if isinstance(value, (int, float)) and value > refp_warn:
            return _WARN_REF
    if key in ("base_pressure", "pc_base_pressure"):
        if isinstance(value, (int, float)) and value > 1e-5:
            return _WARN_PRE
    return row_bg


def _write_data_row(ws, row_num: int, data: Dict[str, Any],
                    arc_thresh: int, refp_warn: float) -> None:
    """데이터 딕셔너리를 ws의 row_num 행에 기록."""
    thin = Side(style="thin", color="CCCCCC")
    brd  = Border(left=thin, right=thin, top=thin, bottom=thin)
    row_bg = _ROW_ODD if row_num % 2 == 0 else _ROW_EVEN
    left_cols = {3, 4, 5}   # Process Name, 비고, 기판

    for ci, (_, __, ___, ____, key) in enumerate(_COLS, 1):
        value = data.get(key)
        if value == "":
            value = None

        bg = _cell_bg(key, value, row_bg, arc_thresh, refp_warn)

        cell = ws.cell(row=row_num, column=ci)
        cell.value = value
        cell.font = Font(name="Arial", size=9, color=_FG)
        cell.fill = PatternFill("solid", fgColor=bg)
        cell.border = brd
        cell.alignment = Alignment(
            horizontal="left" if ci in left_cols else "center",
            vertical="center")

        # 숫자 포맷
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
    data: Dict[str, Any],
    log_dir: Path,
    arc_thresh: int,
    refp_warn: float,
    arc_alert_sent: bool,
    webhook_url: str,
    local_fallback_dir: Path,
) -> bool:
    """
    실제 Excel 저장.
    반환값: 이번 호출에서 Arc 알림을 발송했으면 True.
    예외를 절대 전파하지 않음 — 실패 시 로컬 fallback.
    """
    def _do_save(target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        with _write_lock:
            wb, ws = _get_or_create_wb(target)
            row_num = _next_data_row(ws)
            _write_data_row(ws, row_num, data, arc_thresh, refp_warn)
            wb.save(str(target))

    # 1) Google Drive 경로에 저장 시도
    main_path = log_dir / f"CH{ch}.xlsx"
    save_ok = False
    try:
        _do_save(main_path)
        save_ok = True
    except Exception as e:
        # 2) 실패 시 로컬 fallback 저장 (메인 공정에 영향 없음)
        with contextlib.suppress(Exception):
            fb_path = local_fallback_dir / f"CH{ch}_pending.xlsx"
            _do_save(fb_path)

    # 3) Arc 알림 (공정당 1회, 저장 성공 여부와 무관)
    soft = int(data.get("soft_arc") or 0)
    hard = int(data.get("hard_arc") or 0)
    if (soft + hard) >= arc_thresh and not arc_alert_sent and webhook_url:
        with contextlib.suppress(Exception):
            _send_arc_chat(
                ch,
                str(data.get("process_name", "")),
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
) -> Dict[str, Any]:
    """DataLogger 인스턴스에서 Excel 1행 데이터 딕셔너리 생성."""
    dl = data_logger
    pp = dl.process_params

    # 타임스탬프
    ts = getattr(dl, "_session_started_at", None) or datetime.now()

    # Power Source 문자열 생성
    sources = []
    if pp.get("use_dc_pulse"):
        sources.append("DC Pulse")
    elif pp.get("use_dc_power"):
        sources.append("DC")
    if pp.get("use_rf_pulse"):
        sources.append("RF Pulse")
    elif pp.get("use_rf_power"):
        sources.append("RF")
    power_source = " + ".join(sources)

    # DC Pulse 우선, 없으면 DC Continuous
    avg_v   = _avg(getattr(dl, "dc_pulse_voltage_readings", []) or
                   getattr(dl, "dc_voltage_readings",       []))
    avg_i   = _avg(getattr(dl, "dc_pulse_current_readings", []) or
                   getattr(dl, "dc_current_readings",       []))

    # RF Pulse 우선, 없으면 RF Continuous
    avg_fp  = _avg(getattr(dl, "rf_pulse_for_p_readings", []) or
                   getattr(dl, "rf_for_p_readings",        []))
    avg_rp  = _avg(getattr(dl, "rf_pulse_ref_p_readings", []) or
                   getattr(dl, "rf_ref_p_readings",        []))

    avg_load = _avg(getattr(dl, "rf_load_readings", []))
    avg_tune = _avg(getattr(dl, "rf_tune_readings", []))

    # SP Power: DC Pulse > RF Pulse > DC > RF 우선순위
    sp_power = (pp.get("dc_pulse_power") or pp.get("rf_pulse_power")
                or pp.get("dc_power")    or pp.get("rf_power"))

    # Base Pressure: 공정 중 IG 최솟값
    if getattr(dl, "ig_pressure_readings", []):
        base_pres = min(dl.ig_pressure_readings)
    else:
        base_pres = pp.get("base_pressure")

    _pc = pc_params or {}

    return {
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
        # Plasma Cleaning (chamber_runtime.py 에서 pc_params 로 전달)
        "pc_time"          : _pc.get("time"),
        "pc_base_pressure" : _pc.get("base_pressure"),
        "pc_sp_ar"         : _pc.get("sp_ar"),
        "pc_avg_ar"        : _pc.get("avg_ar"),
        "pc_sp_pressure"   : _pc.get("sp_pressure"),
        "pc_avg_pressure"  : _pc.get("avg_pressure"),
        "pc_sp_power"      : _pc.get("sp_power"),
        "pc_avg_forp"      : _pc.get("avg_forp"),
        "pc_avg_refp"      : _pc.get("avg_refp"),
        "pc_avg_load"      : _pc.get("avg_load"),
        "pc_avg_tune"      : _pc.get("avg_tune"),
        # Main Process 설정값
        "shutter_delay"  : pp.get("shutter_delay"),
        "process_time"   : pp.get("process_time"),
        "base_pressure"  : base_pres,
        "sp_ar"          : pp.get("ar_flow") or pp.get("sp_ar"),
        "sp_n2"          : pp.get("n2_flow") or pp.get("sp_n2"),
        "sp_o2"          : pp.get("o2_flow") or pp.get("sp_o2"),
        "sp_pressure"    : pp.get("working_pressure") or pp.get("sp_pressure"),
        "power_source"   : power_source,
        "sp_power"       : sp_power,
        "duty_cycle"     : pp.get("dc_pulse_duty") or pp.get("rf_pulse_duty"),
        "frequency"      : pp.get("dc_pulse_freq")  or pp.get("rf_pulse_freq"),
        # Main Process 실측 평균
        "avg_ar"         : _avg(dl.mfc_flow_readings.get("Ar", [])),
        "avg_n2"         : _avg(dl.mfc_flow_readings.get("N2", [])),
        "avg_o2"         : _avg(dl.mfc_flow_readings.get("O2", [])),
        "avg_pressure"   : _avg(dl.mfc_pressure_readings),
        "avg_voltage"    : avg_v,
        "avg_current"    : avg_i,
        "avg_forp"       : avg_fp,
        "avg_refp"       : avg_rp,
        "avg_load"       : avg_load,
        "avg_tune"       : avg_tune,
        # Arc (dc_pulse.py 의 arc_counts 에서 전달됨)
        "soft_arc"       : int(pp.get("soft_arc_count") or 0),
        "hard_arc"       : int(pp.get("hard_arc_count") or 0),
    }


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
    try:
        data = _build_data(ch, data_logger, operator, substrate,
                           note, pc_params)
    except Exception:
        return False   # 데이터 빌드 실패 → 조용히 종료

    target_dir = log_dir or _DEFAULT_GDRIVE_DIR
    fb_dir     = local_fallback or (Path.cwd() / "Logs_LocalFallback")

    loop = asyncio.get_event_loop()
    try:
        sent = await loop.run_in_executor(
            None,
            partial(
                _save_sync,
                ch, data, target_dir,
                arc_thresh, refp_warn,
                arc_alert_sent, webhook_url,
                fb_dir,
            ),
        )
        return sent
    except Exception:
        return False
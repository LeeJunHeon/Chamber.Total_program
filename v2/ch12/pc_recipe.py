"""플라즈마 클리닝 — 화면 입력값·CSV 레시피·TEST 모드·호스트(로봇) START 응답 코드. v1 runtime/plasma_cleaning_runtime.py 그대로.

화면 입력값(UI_KEYS — v1 위젯 PC_processName_edit·PC_targetPressure_edit·PC_gasFlow_edit·PC_workingPressure_edit·PC_rfPower_edit·
PC_ProcessTime_edit 의 글자): 엔진이 글자 그대로 들고 있다가 시작할 때 read_params 로 읽는다(비었거나 숫자가 아니면 config 기본값 —
2190-2258). gas_idx 3·tol 0.2·wait 90 s·GV 램프 대기 5 s·IG 간격 10 s 는 config 기본값만(pc_cfg 는 항상 빈 dict).
CSV 레시피(3040-3171): 첫 '비어 있지 않은' 데이터 행(utf-8-sig DictReader, 키·값 strip). 행 → 화면: use_ch(float→int, 이상값 1),
  process_name|Process_name, base_pressure·gas_flow·working_pressure·rf_power·process_time — 빈 칸은 '0.0'(기본값이 아니라 0 이 된다 —
  v1 그대로), 과학 표기는 글자 그대로.
TEST 모드(1218-1231): 마지막으로 불러온 행의 '#' 열이 'test' 면 TEST, 시간은 time/Time/TIME 을 parse_duration_seconds 로, 0 이하면 60 s.
  [이상] 단위 없는 숫자('5')는 정규식이 빈 문자열로 맞아 0 → 60 s(PC-093), 불러온 행은 지워지지 않는다(PC-094) — v1 그대로(D101·D102).
호스트 START(3190-3301): CSV 확인 순서·문구·코드(E207 없음·E209 읽기 실패/형식·E227 30 s·E208 데이터 행 없음), 데이터 행 수(쉼표만 있는
  행 제외, 최소 1). 코드 없는 거절은 핸들러가 문구로 E700('1분 대기'·'cooldown'·'preflight timeout') 아니면 E511(host/handlers.py:991-1001).
"""
from __future__ import annotations

import csv
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Tuple

UI_KEYS = ("process_name", "base_pressure", "gas_flow", "working_pressure", "rf_power", "process_time")
UI_WIDGET = {"process_name": "PC_processName_edit", "base_pressure": "PC_targetPressure_edit",
             "gas_flow": "PC_gasFlow_edit", "working_pressure": "PC_workingPressure_edit",
             "rf_power": "PC_rfPower_edit", "process_time": "PC_ProcessTime_edit"}
CSV_LOAD_TIMEOUT_S = 30.0               # runtime/plasma_cleaning_runtime.py:3253 — executor 30 s
HOST_E700_WORDS = ("1분 대기",)         # host/handlers.py:994-997 (+ 'cooldown'·'preflight timeout' 소문자 비교)


@dataclass
class PcParams:
    """controller/plasma_cleaning_controller.py:12-28 PCParams."""
    gas_idx: int = 3
    gas_flow_sccm: float = 0.0
    target_pressure: float = 5.0e-6
    tol_mTorr: float = 0.2
    wait_timeout_s: float = 90.0
    sp4_setpoint_mTorr: float = 2.0
    rf_power_w: float = 100.0
    process_time_min: float = 1.0
    gv_open_lamp_delay_s: float = 5.0
    ig_interval_ms: int = 10_000

    def as_dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)


class HostStartError(RuntimeError):
    """호스트 START 거절 — code 가 있으면 로봇 응답 코드(v1 CodedRuntimeError), 없으면 핸들러가 문구로 분류."""

    def __init__(self, msg: str, code: Optional[str] = None) -> None:
        super().__init__(msg)
        self.code = code


def host_error_code(msg: str, code: Optional[str]) -> str:
    """host/handlers.py:989-1001 — 코드가 있으면 그것, 없으면 '1분 대기'·'cooldown'·'preflight timeout' 이면 E700, 아니면 E511."""
    if code:
        return str(code)
    m = str(msg or "")
    if "1분 대기" in m or "cooldown" in m.lower() or "preflight timeout" in m.lower():
        return "E700"
    return "E511"


def parse_duration_seconds(s: str) -> float:
    """runtime/plasma_cleaning_runtime.py:3306-3321 그대로(모든 묶음이 선택이라 늘 맞는다 — float 폴백은 닿지 않음)."""
    if not s:
        return 0.0
    s = s.replace(" ", "").lower()
    m = re.match(r"(?:(\d+(?:\.\d+)?)h)?(?:(\d+(?:\.\d+)?)m)?(?:(\d+(?:\.\d+)?)s)?", s)
    if not m:
        try:
            return float(s) * 60.0
        except Exception:
            return 0.0
    return float(m.group(1) or 0) * 3600 + float(m.group(2) or 0) * 60 + float(m.group(3) or 0)


def test_mode_of(row: Optional[Mapping[str, Any]]) -> Tuple[bool, float]:
    """(TEST 인가, TEST 시간 s) — 1218-1231."""
    try:
        row = row or {}
        marker = str(row.get("#", "")).strip().lower()
        time_str = str(row.get("time", "") or row.get("Time", "") or row.get("TIME", "")).strip()
        if marker == "test":
            dur = parse_duration_seconds(time_str.lower())
            return True, (60.0 if dur <= 0 else dur)
    except Exception:
        pass
    return False, 0.0


def blank_ui() -> Dict[str, str]:
    """시작 때 화면 입력값(v1 .ui 의 빈 칸 — 읽을 때 기본값)."""
    return {k: "" for k in UI_KEYS}


def apply_row(ui: Dict[str, str], row: Mapping[str, Any]) -> Optional[int]:
    """행 → 화면 값(3040-3088). 돌려주는 값 = use_ch 가 있으면 1|2(부르는 쪽이 set_selected_ch), 없으면 None."""
    use = str(row.get("use_ch", "")).strip()
    ch = None
    if use:
        try:
            ch = int(float(use))
        except Exception:
            ch = 1

    def f(key: str, default: float = 0.0) -> str:
        s = str(row.get(key, "")).strip()
        return str(default) if s == "" else s
    ui["process_name"] = str(row.get("process_name", "") or row.get("Process_name", "")).strip()
    ui["base_pressure"] = f("base_pressure")
    ui["gas_flow"] = f("gas_flow")
    ui["working_pressure"] = f("working_pressure")
    ui["rf_power"] = f("rf_power")
    ui["process_time"] = f("process_time")
    return ch


def _num(text: Any, default: float) -> float:
    if text is None:
        return default
    if isinstance(text, (int, float)) and not isinstance(text, bool):
        return float(text)
    try:
        t = str(text).strip()
        return float(t) if t else default
    except Exception:
        return default


def read_params(ui: Mapping[str, Any], cfg: Mapping[str, Any]) -> PcParams:
    """_read_params_from_ui(2190-2258) — 화면 글자, 비었거나 숫자가 아니면 config 기본값."""
    def c(k, d):
        v = cfg.get(k, d)
        return d if v is None else v
    return PcParams(
        gas_idx=int(c("PC_DEFAULT_GAS_IDX", 3)),
        gas_flow_sccm=_num(ui.get("gas_flow"), float(c("PC_DEFAULT_GAS_FLOW_SCCM", 0.0))),
        target_pressure=_num(ui.get("base_pressure"), float(c("PC_DEFAULT_TARGET_PRESSURE_TORR", 5.0e-6))),
        tol_mTorr=float(c("PC_DEFAULT_TOL_MTORR", 0.2)),
        wait_timeout_s=float(c("PC_DEFAULT_WAIT_TIMEOUT_S", 90.0)),
        sp4_setpoint_mTorr=_num(ui.get("working_pressure"), float(c("PC_DEFAULT_SP4_SETPOINT_MTORR", 2.0))),
        rf_power_w=_num(ui.get("rf_power"), float(c("PC_DEFAULT_RF_POWER_W", 100.0))),
        process_time_min=_num(ui.get("process_time"), float(c("PC_DEFAULT_PROCESS_TIME_MIN", 1.0))),
        gv_open_lamp_delay_s=float(c("PC_DEFAULT_GV_OPEN_LAMP_DELAY_S", 5.0)),
        ig_interval_ms=int(c("PC_DEFAULT_IG_INTERVAL_MS", 10_000)),
    )


def is_blank_row(values) -> bool:
    return not any(str(v or "").strip() for v in (values or []))


def first_row_of(rows) -> Optional[Dict[str, str]]:
    """행 목록(dict) 중 첫 '비어 있지 않은' 행 — 키·값 strip(_read_first_row_from_csv 와 같은 규칙)."""
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        if any(str(v or "").strip() for v in row.values()):
            return {str(k or "").strip(): str(v or "").strip() for k, v in row.items()}
    return None


def read_first_row(path: str, log=None) -> Optional[Dict[str, str]]:
    """_read_first_row_from_csv(3090-3101) — 읽기 실패는 로그('File', 'CSV 읽기 실패: …') 후 None."""
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                if any((v or "").strip() for v in row.values()):          # 남는 칸(restkey 리스트)은 v1 처럼 예외 → None
                    return {(k or "").strip(): (v or "").strip() for k, v in row.items()}
    except Exception as e:
        if log is not None:
            log("File", f"CSV 읽기 실패: {e!r}")
    return None


def count_data_rows(path: str) -> int:
    """호스트 공정 로그용 데이터 행 수 — 헤더 제외, 쉼표만 있는 행·빈 줄 제외, 최소 1(3227-3237)."""
    n = 1
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            rdr = csv.reader(f)
            next(rdr, None)
            n = max(1, sum(1 for r in rdr if any(str(c).strip() for c in (r or []))))
    except Exception:
        pass
    return n


def load_host_csv(path: str, log=None) -> Tuple[Optional[Dict[str, str]], int]:
    """executor 안의 _load_csv_sync(3218-3238) — 없음 E207, 읽기 확인 실패는 부르는 쪽이 E209 로."""
    if not os.path.exists(path):
        raise HostStartError(f"CSV 파일을 찾을 수 없습니다: {path}", "E207")
    with open(path, "r", encoding="utf-8-sig", newline="") as chk:
        chk.read()
    rows = count_data_rows(path)
    return read_first_row(path, log), rows


def e209_read_failed(path: str, e: BaseException) -> HostStartError:
    return HostStartError(f"레시피 파일을 읽지 못했습니다({type(e).__name__}: {e}) — 엑셀에서 'CSV UTF-8(쉼표로 분리)' 로 "
                          f"저장했는지, 다른 프로그램에서 열려 있지 않은지 확인하십시오: {path}", "E209")


@dataclass
class HostStartResult:
    """로봇 START_PLASMA_CLEANING 결과 — ok 면 응답 'PLASMA CLEANING START OK'(핸들러), 아니면 message·code(분류 끝난 코드)."""
    ok: bool
    message: str = ""
    code: Optional[str] = None
    raw_code: Optional[str] = None            # 런타임이 실은 코드(없으면 None — 핸들러 분류 전)
    meta: Dict[str, Any] = field(default_factory=dict)

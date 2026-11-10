"""sim 격리 — v1 이 켜진 장비 노트북에서 sim 을 함께 띄워도 운영 파일·NAS·포트에 닿지 않게(사용자 지시 2026-11-09 B, 치명).

규칙(RUN_MODE=sim 일 때만, 설정값과 무관하게 강제)
- 쓰는 곳은 모두 C:\\CH12_v2\\sim_out\\ 아래(enforce): 1초 기록·로그·저널(LOCAL_DATA_DIR), v1 기록(LOG_ROOT_DIR — 공정 txt·SYSTEM·ERROR·
  RUNTIME_DUMP·selfwatch·히터), Logs_LocalFallback, Calib·엑셀·PLC 코일·두께·카메라·OES·RGA CSV, 로봇 CSV(open_sputter.json·pending 포함 —
  HOST_LOG_LOCAL_DIR·HOST_LOG_NAS_DIR 둘 다), 로봇 응답 캐시, 레시피 폴더(읽기도 — 운영 NAS 를 보지 않게), log_sync·RF 리더 상태, OES 워커 폴더(log·stop).
  NAS·UNC(\\\\…) 경로는 sim 에서 절대 쓰지 않는다(NAS 사본 끔·log_sync 쌍 비움).
- OES 가짜 데몬: 뮤텍스 이름 접두어(OES_MUTEX_PREFIX=CH12SIM_ → Local\\CH12SIM_VanaM_OES_USB{n}, v1 은 Local\\VanaM_OES_USB{n})와 정지 플래그 폴더
  (OES_STOP_DIR)·OES_LOCAL_BASE 를 환경변수로 sim_out 아래로 덮는다(PC 에 v1 용 환경변수가 있어도 이긴다). 가짜 워커(OES·RGA)만 쓴다.
- 포트: 127.0.0.1 만. 로봇 서버 포트는 50070 이 아니라 HOST_SERVER_SIM_PORT(기본 50170 — 시험용 로봇 서버).
- 시작 자기 검사(check): 쓰기 대상 경로가 C:\\CH12_v2 밖이거나 UNC 이거나, 포트가 v1 것(50070)·0.0.0.0 이면 시작 거부(사유 출력, 종료 코드 2).
기준 폴더는 환경변수 CH12_SIM_BASE 로 바꿀 수 있다(시험 전용 — 기본 C:\\CH12_v2). 리눅스·맥에서는 윈도우 경로 변환(winpaths)이 뒤따른다.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Tuple

from v2.common import winpaths

SIM_BASE_DEFAULT = "C:\\CH12_v2"
SIM_OUT_NAME = "sim_out"
V1_ROBOT_PORT = 50070
SIM_ROBOT_PORT_DEFAULT = 50170
LOOPBACK = "127.0.0.1"
OES_MUTEX_PREFIX = "CH12SIM_"

# 키 → sim_out 아래 하위 경로(빈 문자열 = 값 비움)
FORCED: Dict[str, str] = {
    "LOCAL_DATA_DIR": "data",
    "LOG_ROOT_DIR": "logs",
    "LOCAL_FALLBACK_ROOT": "fallback",
    "LOCAL_FALLBACK_SERVER_DIR": "fallback\\Server",
    "LOCAL_FALLBACK_RGA_DIR": "fallback\\RGA",
    "OUTPUT_LOCAL_FIRST_DIR": "local_first",
    "SPUTTER_CALIB_DB_DIR": "calib_db",
    "GDRIVE_LOG_DIR": "Process_log",
    "PLC_COIL_LOG_NAS_DIR": "plc_coil",
    "PLC_COIL_LOG_LOCAL_DIR": "plc_coil_local",
    "CH1_THICKNESS_LOG_DIR": "thickness",
    "CH2_THICKNESS_LOG_DIR": "thickness",
    "CH2_HEATER_LOG_DIR": "heater",
    "SELFWATCH_DIR": "selfwatch",
    "MFC_STATE_LOG_DIR": "mfc_state",
    "CAMERA_LOG_ROOT": "camera",
    "HOST_LOG_NAS_DIR": "robot_nas",
    "HOST_LOG_LOCAL_DIR": "robot_local",
    "ROBOT_CACHE_PATH": "state\\robot_last_replies.json",
    "ROBOT_RECIPE_ROOT_DIR": "recipes",
    "PROCESS_LIST_START_DIR": "recipes",
    "OES_LOCAL_DIR": "oes",
    "OES_WORKER_HOME": "oes_worker",
    "CH1_RGA_CSV_PATH": "rga\\Ch.1\\RGA_spectrums.csv",
    "CH2_RGA_CSV_PATH": "rga\\Ch.2\\RGA_spectrums.csv",
    "RF_READER_STATE_DIR": "rf_reader\\_state",
    "RF_READER_CSV_DIR": "rf_reader\\_csv",
    "RF_READER_NAS_DIR": "rf_reader\\nas",
    "LOG_SYNC_STATE_DIR": "_sync_state",
    "NAS_COPY_DIR": None,                      # 빈 값 — 사본을 보내지 않는다
    "CH1_THICKNESS_FACTOR_FILE": None,         # 빈 값 — 기본(설정 폴더 = sim 데이터 아래)
    "CH2_THICKNESS_FACTOR_FILE": None,
    "LOG_SYNC_PAIRS": "[]",                    # 글자 그대로(NAS 쌍 없음)
}
# 읽기만 하는 값 — 검사에서 뺀다
READ_ONLY_KEYS = frozenset({"OES_DLL_PATH", "LOCAL_HMI_STATIC_DIR", "OES_WORKER_EXE", "RGA_WORKER_EXE"})


def sim_base() -> str:
    return (os.environ.get("CH12_SIM_BASE") or SIM_BASE_DEFAULT).strip()


def _join(base: str, *parts: str) -> str:
    win = bool(re.match(r"^[A-Za-z]:[\\/]|^\\\\", base))
    out = base
    for p in parts:
        for seg in re.split(r"[\\/]+", p):
            if seg:
                out = (out.rstrip("\\/") + ("\\" if win else os.sep) + seg)
    return out


def sim_out() -> str:
    return _join(sim_base(), SIM_OUT_NAME)


def sim_port(cfg: Mapping[str, Any] = None) -> int:
    try:
        return int((cfg or {}).get("HOST_SERVER_SIM_PORT") or SIM_ROBOT_PORT_DEFAULT)
    except Exception:
        return SIM_ROBOT_PORT_DEFAULT


def enforce(cfg: Dict[str, Any]) -> List[str]:
    """cfg(실행 값 복사본)를 제자리에서 sim 값으로 덮는다. 바꾼 키 목록. winpaths 변환보다 먼저 부른다."""
    out = sim_out()
    changed = []
    for k, sub in FORCED.items():
        v = "" if sub is None else (sub if k == "LOG_SYNC_PAIRS" else _join(out, sub))
        if cfg.get(k) != v:
            cfg[k] = v
            changed.append(k)
    for k, v in (("HOST_SERVER_HOST", LOOPBACK), ("HOST_SERVER_PORT", sim_port(cfg))):
        if cfg.get(k) != v:
            cfg[k] = v
            changed.append(k)
    cfg.update({"OES_WORKER_FAKE": True, "RGA_WORKER_FAKE": True})
    cfg["SIM_OUT_ROOT"] = out                       # 엑셀 메인 사본처럼 설정 키가 없는 쓰는 곳이 따른다(outputs.paths)
    return changed


def apply_env() -> Dict[str, str]:
    """워커가 상속하는 환경변수를 sim_out 으로(v1 이 PC 에 둔 값도 덮는다) — OES 폴더·정지 플래그·뮤텍스 접두어."""
    out = sim_out()
    env = {"OES_LOCAL_BASE": winpaths.runtime_path(_join(out, "oes")),
           "OES_STOP_DIR": winpaths.runtime_path(_join(out, "oes_worker", "stop")),
           "OES_MUTEX_PREFIX": OES_MUTEX_PREFIX}
    for k in ("OES_FAKE_LOG", "RGA_FAKE_LOG"):
        if os.environ.get(k):
            env[k] = winpaths.runtime_path(_join(out, "fake_logs", k.lower() + ".log"))
    os.environ.update(env)
    return env


def _norm(p: str) -> str:
    return os.path.normcase(os.path.normpath(winpaths.runtime_path(str(p))))


def inside(path: str, base: str) -> bool:
    b, p = _norm(base), _norm(path)
    return p == b or p.startswith(b.rstrip("\\/") + os.sep)


def _is_pathy(v: str) -> bool:
    return winpaths.is_windows_path(v) or os.path.isabs(v) or v.startswith("\\\\")


def _scan(prefix: str, v: Any, out: List[Tuple[str, str]]) -> None:
    if isinstance(v, str):
        out.append((prefix, v))
    elif isinstance(v, (list, tuple)):
        for i, x in enumerate(v):
            _scan(f"{prefix}[{i}]", x, out)
    elif isinstance(v, dict):
        for k, x in v.items():
            _scan(f"{prefix}.{k}", x, out)


def check(cfg: Mapping[str, Any], config_dir: Any = None) -> List[str]:
    """시작 자기 검사 — 문제 목록(비면 통과). cfg 는 enforce·winpaths 변환을 거친 실행 값."""
    base = sim_base()
    problems: List[str] = []
    strs: List[Tuple[str, str]] = []
    for k, v in cfg.items():
        if k == "SIM_OUT_ROOT":
            continue
        _scan(k, v, strs)
        if isinstance(v, str) and k.startswith("LOG_SYNC_PAIRS"):    # JSON 글자 안의 경로도
            try:
                _scan(k, json.loads(v), strs)
            except Exception:
                pass
    for name, v in strs:
        top = name.split("[")[0].split(".")[0]
        if not v or top in READ_ONLY_KEYS:
            continue
        if v.startswith("\\\\") or v.startswith("//") or "VanaM_NAS" in v:
            problems.append(f"{name} = {v!r} — NAS·UNC 경로는 sim 에서 쓰지 않음")
        elif _is_pathy(v) and top != "LOG_SYNC_PAIRS" and not inside(v, base):
            problems.append(f"{name} = {v!r} — 쓰기 대상이 {base} 밖")
    if config_dir is not None and not inside(str(config_dir), base):
        problems.append(f"--config-dir {config_dir} — {base} 밖(운영 설정과 섞이지 않게 sim 설정은 {base} 아래에 둔다)")
    host, port = str(cfg.get("HOST_SERVER_HOST", LOOPBACK)), int(cfg.get("HOST_SERVER_PORT", sim_port(cfg)) or 0)
    if host != LOOPBACK:
        problems.append(f"HOST_SERVER_HOST = {host} — sim 은 {LOOPBACK} 만")
    if port == V1_ROBOT_PORT:
        problems.append(f"HOST_SERVER_PORT = {port} — v1 로봇 서버 포트(sim 은 {sim_port(cfg)} 같은 시험용 포트)")
    if cfg.get("RUN_MODE") not in (None, "", "sim"):
        problems.append(f"RUN_MODE = {cfg.get('RUN_MODE')} — sim 격리 검사는 sim 에서만")
    return problems


def mutex_name(suffix: str) -> str:
    """OES 워커 뮤텍스 이름 — 기본 Local\\VanaM_OES_<suffix>(v1 과 같음), sim 은 접두어로 다르게."""
    return "Local\\" + (os.environ.get("OES_MUTEX_PREFIX") or "") + "VanaM_OES_" + suffix

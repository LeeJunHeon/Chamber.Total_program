# apps/oes_service/oes_api.py
# -*- coding: utf-8 -*-
"""
OES worker process (standalone exe)

요구사항/목표:
- "OES 실측(DLL 호출/장비 스캔/데이터 획득/CSV append)"은 이 워커에서만 수행한다.
- 메인 프로그램은 워커를 실행하고, 워커가 기록하는 로컬 CSV를 tail해서 그래프만 갱신한다.
- 워커는 stdout으로 JSON 1줄씩 출력한다(메인에서 파싱 가능).

stdout JSON:
  - init:     {"kind":"init", "ok":true, "ch":1, "usb":0, "resolved_usb":0, "model":"...", "pixels":1024}
  - started:  {"kind":"started", "ok":true, "out_csv":"...", "cols":1024, ...}
  - finished: {"kind":"finished","ok":true, "out_csv":"...", "rows":1234, "elapsed_s":33.2}
  - failed:   {"kind":"finished","ok":false, "error":"...", "trace":"..."}
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import csv
import json
import os
import sys
import time
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import ctypes
from concurrent.futures import ThreadPoolExecutor

import logging                  # ✅ 추가
from threading import Lock      # ✅ 추가

import numpy as np
import ssl
import urllib.request


# ── OES Anomaly Detection ──────────────────────────────────
_AR_WAVELENGTH_NM = 818.8223079
_AR_WAVELENGTH_MIN = 818.0
_AR_WAVELENGTH_MAX = 819.0
_ANOMALY_SKIP_FIRST = 10
_ANOMALY_SKIP_LAST = 10
_ANOMALY_THRESHOLD = 0.10
_ANOMALY_WEBHOOK_URL = ""
_ANOMALY_ENABLED = False

def _load_anomaly_config() -> None:
    """worker exe 옆의 oes_config.json을 읽어서 전역 설정 반영."""
    global _AR_WAVELENGTH_NM, _AR_WAVELENGTH_MIN, _AR_WAVELENGTH_MAX, _ANOMALY_SKIP_FIRST, _ANOMALY_SKIP_LAST, _ANOMALY_THRESHOLD, _ANOMALY_WEBHOOK_URL, _ANOMALY_ENABLED
    cfg_path = _worker_base_dir() / "oes_config.json"
    try:
        if not cfg_path.exists():
            return
        with open(cfg_path, "r", encoding="utf-8") as fp:
            cfg = json.loads(fp.read())
        _ANOMALY_ENABLED = bool(cfg.get("enabled", False))
        _ANOMALY_WEBHOOK_URL = str(cfg.get("webhook_url", "") or "").strip()
        _AR_WAVELENGTH_NM = float(cfg.get("ar_wavelength", _AR_WAVELENGTH_NM))
        _AR_WAVELENGTH_MIN = float(cfg.get("ar_wavelength_min", _AR_WAVELENGTH_MIN))
        _AR_WAVELENGTH_MAX = float(cfg.get("ar_wavelength_max", _AR_WAVELENGTH_MAX))
        _ANOMALY_SKIP_FIRST = int(cfg.get("skip_first", _ANOMALY_SKIP_FIRST))
        _ANOMALY_SKIP_LAST = int(cfg.get("skip_last", _ANOMALY_SKIP_LAST))
        _ANOMALY_THRESHOLD = float(cfg.get("threshold", _ANOMALY_THRESHOLD))
    except Exception:
        pass

def _expected_serial_for(ch: int) -> str:
    """oes_config.json 의 선택 키 expected_serial({"1": "...", "2": "..."})에서 이 챔버 값. 없거나 읽기 실패면 ""."""
    cfg_path = _worker_base_dir() / "oes_config.json"
    try:
        if not cfg_path.exists():
            return ""
        try:
            with open(cfg_path, "r", encoding="utf-8") as fp:
                cfg = json.loads(fp.read())
        except Exception:
            _status("[init] oes_config.json 읽기 실패 → 일련번호 확인 꺼짐")
            return ""
        m = cfg.get("expected_serial") or {}
        if not isinstance(m, dict):
            return ""
        return str(m.get(str(int(ch))) or "").strip()
    except Exception:
        return ""

def _find_ar_index(x_list: list) -> Optional[int]:
    """x_list(파장 리스트)에서 818nm 대역(818.0~819.0) 파장 인덱스 반환."""
    if not x_list:
        return None
    try:
        arr = np.array(x_list, dtype=float)
        candidates = np.where((arr >= _AR_WAVELENGTH_MIN) & (arr < _AR_WAVELENGTH_MAX))[0]
        if len(candidates) == 0:
            return None
        return int(candidates[np.argmin(np.abs(arr[candidates] - _AR_WAVELENGTH_NM))])
    except Exception:
        return None

def _post_anomaly_webhook(msg: str) -> None:
    """Google Chat webhook으로 이상 알림 전송 (blocking, 실패 무시)."""
    url = _ANOMALY_WEBHOOK_URL
    if not url:
        return
    try:
        payload = json.dumps({"text": msg}).encode("utf-8")
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=5, context=ctx) as resp:
            resp.read()
    except Exception:
        pass
# ── OES Anomaly Detection END ──────────────────────────────


# 저장 경로 (Google Drive 동기화 폴더 → NAS 자동 반영)
_NAS_OES_ROOT = Path(r"G:\공유 드라이브\VanaM_Sputter\OES")
_NAS_CH_DIR = {
    1: _NAS_OES_ROOT / "CH1",
    2: _NAS_OES_ROOT / "CH2",
}


def _mutex_timeout_ms() -> int:
    # 기본 5초: 공정 멈추지 않게 빨리 실패시키기
    return int(os.environ.get("OES_MUTEX_TIMEOUT_MS", "5000"))


def _nas_dir_for_ch(ch: int) -> Path:
    try:
        return _NAS_CH_DIR[int(ch)]
    except Exception:
        raise ValueError(f"Invalid chamber: {ch} (expected 1 or 2)")


async def _copy_csv_to_nas(local_csv: Path, ch: int, *, timeout_s: float = 120.0):
    """
    로컬 CSV -> NAS(CH별 고정 폴더)로 복사(검증 포함)
    return: (nas_ok:bool, nas_csv:Path|None, nas_error:str|None, local_deleted:bool)
    """
    try:
        dest_dir = _nas_dir_for_ch(int(ch))
    except Exception as e:
        return False, None, f"nas_dir error: {e}", False

    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        return False, None, f"nas mkdir failed: {e}", False

    dest_csv = dest_dir / local_csv.name

    # Windows: robocopy가 네트워크에서 가장 안정적
    if os.name == "nt":
        CREATE_NO_WINDOW = 0x08000000
        cmd = [
            "robocopy",
            str(local_csv.parent),
            str(dest_dir),
            local_csv.name,
            "/R:2", "/W:1",
            "/NFL", "/NDL", "/NJH", "/NJS", "/NP",
        ]
        try:
            p = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                creationflags=CREATE_NO_WINDOW,
            )
            try:
                out_b, err_b = await asyncio.wait_for(p.communicate(), timeout=timeout_s)
            except asyncio.TimeoutError:
                with contextlib.suppress(Exception):
                    p.kill()
                return False, None, "robocopy timeout", False

            rc = p.returncode if p.returncode is not None else 999
            # robocopy: 0~7 = 성공 범주, 8 이상 = 실패
            if rc >= 8:
                out = (out_b or b"").decode(errors="ignore")[-2000:]
                err = (err_b or b"").decode(errors="ignore")[-2000:]
                return False, None, f"robocopy failed rc={rc} out={out} err={err}", False
        except Exception as e:
            return False, None, f"robocopy exception: {e}", False

    else:
        # 비-Windows: shutil.copy2 사용(필요시)
        try:
            import shutil
            shutil.copy2(local_csv, dest_csv)
        except Exception as e:
            return False, None, f"copy2 failed: {e}", False

    # 검증: 파일 존재 + 크기 동일
    try:
        if not dest_csv.exists():
            return False, None, f"nas file not found: {dest_csv}", False
        if dest_csv.stat().st_size != local_csv.stat().st_size:
            return False, None, "size mismatch after copy", False
    except Exception as e:
        return False, None, f"verify failed: {e}", False

    # 로컬 삭제 시도(※ oes.py tail이 열고 있으면 Windows에서 실패할 수 있음)
    local_deleted = False
    try:
        local_csv.unlink()
        local_deleted = True
    except Exception:
        local_deleted = False

    return True, dest_csv, None, local_deleted


# ✅ 워커는 메인/프로젝트 설정에 의존하지 않도록 고정값 사용
OES_AVG_COUNT = 3
DEBUG_PRINT = False

# ── 분광기 연결 감시 / 읽기 실패 복구 ──
# 새 DLL(CyUSB)은 호출마다 '지금 연결된 CyUSB 장치 목록의 N번째'를 연다.
# 목록 앞쪽 분광기가 빠지면 번호가 밀려 다른 챔버 분광기를 오류 없이 읽으므로,
# 측정 중 연결 목록이 초기화 때(기준)와 다르면 읽기를 멈추고, 돌아오면 재초기화한다.
_CYUSB_IFACE_GUID = "{AE18AA60-7F6A-11D4-97DD-00010229B959}"  # spusb3.sys(CyUSB) 분광기 인터페이스 GUID
_CM_LIST_RETRIES = 5              # CM_Get_Device_Interface_ListW 가 CR_BUFFER_SMALL 일 때 재시도 횟수
_LINK_WAIT_AT_START_S = 10.0      # 측정 시작 시 목록이 기준과 다르면 기준으로 돌아오기를 기다리는 최대 시간(초)
_LINK_SETTLE_S = 2.0              # 목록이 기준으로 돌아온 뒤 재초기화 전에 기다리는 시간(초)
_LINK_STATUS_EVERY_S = 30.0       # 끊김이 이어질 때 '재연결 대기 중' 상태를 남기는 간격(초)
_LINK_POLL_S = 0.5                # 시작 대기·재초기화 뮤텍스 대기 중 확인 간격(초)
_REINIT_RETRY_S = 30.0            # 재초기화 실패 후 다음 시도까지, 연속 읽기 실패 재초기화의 최소 간격(초)
_READ_FAIL_RESET_AT = 3           # 연속 읽기 실패 이 횟수째에 포트 리셋(spSetupGivenChannel) 1회
_READ_FAIL_REINIT_AT = 10         # 연속 읽기 실패 이 횟수째에 재초기화
_RESET_CALL_TIMEOUT_S = 4.0       # 포트 리셋 호출 타임아웃(초). 넘으면 읽기 타임아웃과 같은 hard_abort
_REINIT_MUTEX_NAME = "Local\\VanaM_OES_REINIT"  # 두 워커가 동시에 재초기화하지 않도록 잡는 뮤텍스
_REINIT_MUTEX_WAIT_S = 30.0       # 재초기화 뮤텍스 최대 대기(초). 못 잡으면 다음 기회에
_DEV_INFO_BUF = 512               # spDevInfo 모델명/일련번호 버퍼 크기(바이트)

_CM_FNS = None  # (size_fn, list_fn, guid) 지연 바인딩


def _list_cyusb_interfaces() -> Optional[list]:
    """
    지금 연결된 CyUSB 분광기 인터페이스 경로 목록(cfgmgr32 조회, USB 통신 없음).
    Windows 가 아니거나 조회 실패면 None.
    """
    global _CM_FNS
    if os.name != "nt":
        return None
    try:
        if _CM_FNS is None:
            class _GUID(ctypes.Structure):
                _fields_ = [
                    ("Data1", ctypes.c_uint32),
                    ("Data2", ctypes.c_uint16),
                    ("Data3", ctypes.c_uint16),
                    ("Data4", ctypes.c_ubyte * 8),
                ]
            guid = _GUID.from_buffer_copy(uuid.UUID(_CYUSB_IFACE_GUID).bytes_le)
            cm = ctypes.WinDLL("cfgmgr32")
            size_fn = cm.CM_Get_Device_Interface_List_SizeW
            size_fn.argtypes = [ctypes.POINTER(ctypes.c_ulong), ctypes.POINTER(_GUID), ctypes.c_wchar_p, ctypes.c_ulong]
            size_fn.restype = ctypes.c_ulong
            list_fn = cm.CM_Get_Device_Interface_ListW
            list_fn.argtypes = [ctypes.POINTER(_GUID), ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_ulong, ctypes.c_ulong]
            list_fn.restype = ctypes.c_ulong
            _CM_FNS = (size_fn, list_fn, guid)

        size_fn, list_fn, guid = _CM_FNS
        CR_SUCCESS = 0x0
        CR_BUFFER_SMALL = 0x1A
        CM_GET_DEVICE_INTERFACE_LIST_PRESENT = 0x0
        for _ in range(_CM_LIST_RETRIES):
            n = ctypes.c_ulong(0)
            cr = size_fn(ctypes.byref(n), ctypes.byref(guid), None, CM_GET_DEVICE_INTERFACE_LIST_PRESENT)
            if cr != CR_SUCCESS:
                return None
            if n.value <= 1:
                return []
            buf = ctypes.create_unicode_buffer(n.value)
            cr = list_fn(ctypes.byref(guid), None, buf, n.value, CM_GET_DEVICE_INTERFACE_LIST_PRESENT)
            if cr == CR_BUFFER_SMALL:
                continue  # 조회 사이에 장치가 늘었음 → 크기부터 다시
            if cr != CR_SUCCESS:
                return None
            raw = ctypes.wstring_at(ctypes.addressof(buf), n.value)
            return [s for s in raw.split("\x00") if s]
        return None
    except Exception:
        return None


def _add_dll_search_dir(dll_path: str) -> None:
    if os.name != "nt":
        return
    try:
        os.add_dll_directory(str(Path(dll_path).resolve().parent))
    except Exception:
        pass


_STDOUT_BROKEN = False
_JSON_LOCK = Lock()

def _print_json(obj) -> None:
    """
    JSONL 출력(부모 프로세스가 stdout pipe로 파싱).

    Windows/PyInstaller 환경에서 stdout 핸들이 깨져 있거나(Invalid argument 등),
    부모가 먼저 종료되어 pipe가 닫힌 경우에도 워커가 여기서 '크래시' 하지 않도록
    반드시 예외를 삼킨다.
    """
    global _STDOUT_BROKEN
    s = json.dumps(obj, ensure_ascii=False)

    try:
        with _JSON_LOCK:
            if _STDOUT_BROKEN:
                raise OSError(22, "stdout already marked broken")

            payload = (s + "\n").encode("utf-8", errors="backslashreplace")

            # buffer가 있으면(대부분) 인코딩 문제도 줄고 더 안전
            if getattr(sys.stdout, "buffer", None):
                sys.stdout.buffer.write(payload)
                sys.stdout.buffer.flush()
            else:
                sys.stdout.write(s + "\n")
                sys.stdout.flush()

    except Exception as e:
        _STDOUT_BROKEN = True
        # 워커는 절대 여기서 죽으면 안 됨 → 파일 로그로만 남김
        with contextlib.suppress(Exception):
            _errlog_exc(f"_print_json failed: {type(e).__name__}: {e} kind={obj.get('kind') if isinstance(obj, dict) else None}")
        # 마지막 시도: stderr (부모가 stderr를 읽는다면 확인 가능)
        with contextlib.suppress(Exception):
            sys.stderr.write(s + "\n")
            sys.stderr.flush()


# ================= 오류 로거 =================
_ERR_LOGGER = None
_ERR_LOCK = Lock()

def _worker_base_dir() -> Path:
    """
    워커가 있는 '같은 위치' 기준:
    - PyInstaller exe(frozen): exe가 있는 폴더
    - python 실행: 이 파일(oes_api.py)이 있는 폴더
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

def _err_log_path() -> Path:
    log_dir = _worker_base_dir() / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    d = datetime.now().strftime("%Y%m%d")
    return log_dir / f"{d}.log"

def _get_err_logger() -> logging.Logger:
    global _ERR_LOGGER
    if _ERR_LOGGER is not None:
        return _ERR_LOGGER

    with _ERR_LOCK:
        if _ERR_LOGGER is not None:
            return _ERR_LOGGER

        logger = logging.getLogger("OES_WORKER_ERR")
        logger.setLevel(logging.ERROR)
        logger.propagate = False

        fh = logging.FileHandler(_err_log_path(), encoding="utf-8")
        fh.setLevel(logging.ERROR)
        fh.setFormatter(logging.Formatter(
            "%(asctime)s [pid=%(process)d] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        logger.addHandler(fh)

        _ERR_LOGGER = logger
        return logger

def _errlog(msg: str) -> None:
    # ✅ 이 함수가 호출되는 순간에만 log 폴더/파일이 생성됨
    try:
        _get_err_logger().error(msg)
    except Exception:
        pass

def _errlog_exc(msg: str) -> None:
    try:
        _get_err_logger().error(msg, exc_info=True)
    except Exception:
        pass
# ================= 오류 로거 =================


# ================= 실행 로거(INFO) =================
_RUN_LOGGER = None
_RUN_LOCK = Lock()

def _run_log_path() -> Path:
    log_dir = _worker_base_dir() / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    d = datetime.now().strftime("%Y%m%d")
    return log_dir / f"{d}.run.log"

def _get_run_logger() -> logging.Logger:
    global _RUN_LOGGER
    if _RUN_LOGGER is not None:
        return _RUN_LOGGER
    with _RUN_LOCK:
        if _RUN_LOGGER is not None:
            return _RUN_LOGGER
        logger = logging.getLogger("OES_WORKER_RUN")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        fh = logging.FileHandler(_run_log_path(), encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter(
            "%(asctime)s [pid=%(process)d] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        logger.addHandler(fh)
        _RUN_LOGGER = logger
        return logger

def _runlog(msg: str) -> None:
    try:
        _get_run_logger().info(msg)
    except Exception:
        pass

def _status(msg: str) -> None:
    _runlog(msg)
    _print_json({"kind": "status", "message": msg})

    # ✅ 콘솔 디버깅용: OES_WORKER_CONSOLE=1이면 stderr에도 출력
    if os.environ.get("OES_WORKER_CONSOLE", "0") == "1":
        try:
            sys.stderr.write(msg + "\n")
            sys.stderr.flush()
        except Exception:
            pass

# ================= 실행 로거(INFO) =================


# ===== 크로스-프로세스 뮤텍스(USB 채널 단위) =====
# - 같은 USB 채널(예: USB0)을 두 워커가 동시에 잡지 못하도록 방지
# - CH1(USB0) + CH2(USB1) 동시 측정은 가능(뮤텍스 이름이 다름)
class _WinMutex:
    def __init__(self, name: str):
        self.name = name
        self.handle = None

    def acquire(self, timeout_ms: int = 60_000) -> bool:
        if os.name != "nt":
            return True
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        k32.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_wchar_p]
        k32.CreateMutexW.restype = ctypes.c_void_p
        k32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        k32.WaitForSingleObject.restype = ctypes.c_uint32

        self.handle = k32.CreateMutexW(None, 0, self.name)
        if not self.handle:
            return False
        WAIT_OBJECT_0 = 0x0
        WAIT_ABANDONED = 0x80
        WAIT_TIMEOUT = 0x102

        r = k32.WaitForSingleObject(self.handle, ctypes.c_uint32(timeout_ms))
        if r in (WAIT_OBJECT_0, WAIT_ABANDONED):
            return True
        if r == WAIT_TIMEOUT:
            return False
        return False

    def release(self) -> None:
        if os.name != "nt":
            return
        if not self.handle:
            return
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        k32.ReleaseMutex.argtypes = [ctypes.c_void_p]
        k32.ReleaseMutex.restype = ctypes.c_int
        k32.CloseHandle.argtypes = [ctypes.c_void_p]
        k32.CloseHandle.restype = ctypes.c_int
        with contextlib.suppress(Exception):
            k32.ReleaseMutex(self.handle)
        with contextlib.suppress(Exception):
            k32.CloseHandle(self.handle)
        self.handle = None


# ── 모델 정의 (device/oes.py와 동일) ─────────────────────────
SP_CCD_SONY    = 0
SP_CCD_TOSHIBA = 1
SP_CCD_PDA     = 2   # SM303-Si (Hamamatsu S7031)
SP_CCD_G9212   = 3   # SM303-InGaAs
SP_CCD_S10420  = 4

PIXELS = {
    SP_CCD_PDA:     1056,
    SP_CCD_G9212:    512,
    SP_CCD_SONY:    2080,
    SP_CCD_S10420:  2080,
    SP_CCD_TOSHIBA: 3680,
}

CANDIDATE_PIXELS = [1056, 1024, 512, 2048, 4096, 3680, 2080]
ORDER_USB = 0


# ====== OES Direct-DLL core (기존 device/oes.py 로직을 워커 내부로 이관) ======

ROI_START_DEFAULT = 10   # 이전 코드와 동일
ROI_END_DEFAULT   = 1034 # 이전 코드와 동일

# 전역 실행자 (DLL 블로킹 호출을 스레드로 보내기)
_OES_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="OES_DLL")

# 프로세스 내 DLL 직렬화 락(동일 프로세스 내 동시 진입 방지)
_DDL_LOCK: Optional[asyncio.Lock] = None

def _get_dll_lock() -> asyncio.Lock:
    global _DDL_LOCK
    if _DDL_LOCK is None:
        _DDL_LOCK = asyncio.Lock()
    return _DDL_LOCK


def _resolve_oes_dll_path(dll_path: Optional[str]) -> str:
    """
    worker 단독 실행 기준:
    1) --dll_path가 있으면 그 경로(존재할 때만)
    2) oes_worker.exe가 있는 폴더(= sys.executable 폴더)에서 SPdbUSBm.dll 탐색
    3) (개발용) oes_api.py가 있는 폴더에서도 탐색
    4) 최후: "SPdbUSBm.dll" (PATH/현재폴더 의존)
    """
    # 1) 사용자 지정 경로
    if dll_path:
        p = Path(dll_path).expanduser().resolve()
        if p.is_file():
            return str(p)

    exe_dir = _worker_base_dir()                 # frozen이면 exe 폴더
    here_dir = Path(__file__).resolve().parent   # 개발 실행이면 스크립트 폴더

    candidates = [
        exe_dir / "SPdbUSBm.dll",
        exe_dir / "_internal" / "SPdbUSBm.dll",
        here_dir / "SPdbUSBm.dll",
        here_dir / "_internal" / "SPdbUSBm.dll",
    ]

    for p in candidates:
        if p.is_file():
            return str(p)

    return "SPdbUSBm.dll"


class OESAsync:
    """
    ⚠️ 워커 내부용 OES 실측 클래스(Direct DLL)
    - 기존 device/oes.py 측정 방식/장비 스캔 방식을 그대로 유지
    """

    def __init__(
        self,
        *,
        dll_path: Optional[str] = None,
        save_directory: str = r"C:\Users\vanam\Desktop\OES",
        sample_interval_s: float = 1.0,
        avg_count: int = OES_AVG_COUNT,
        debug_print: bool = DEBUG_PRINT,
        chamber: int = 2,
        usb_index: Optional[int] = None,
    ):
        self._dll_path = _resolve_oes_dll_path(dll_path)

        p = Path(save_directory)
        if p.name.upper() in {"CH1", "CH2"}:
            p = p.parent / f"CH{int(chamber)}"
        else:
            p = p / f"CH{int(chamber)}"
        self._save_dir = p
        self._save_dir.mkdir(parents=True, exist_ok=True)

        self._sample_interval_s = float(sample_interval_s)
        self._avg_count = int(max(1, avg_count))
        self._debug = bool(debug_print)

        self.sp_dll: Optional[ctypes.WinDLL] = None
        self.sChannel: int = -1
        self._npix: int = 0
        self._wl: Optional[np.ndarray] = None
        self._model_name: str = "UNKNOWN"
        self._model: Optional[int] = None

        self._chamber = int(chamber)
        self._usb_index = int(usb_index) if usb_index is not None else (0 if self._chamber == 1 else 1)

        self._roi_start = ROI_START_DEFAULT
        self._roi_end   = ROI_END_DEFAULT

        self._auto_dark = None
        self._set_trg = None
        self._set_tec = None
        self._set_dbl_int = None
        self._get_wl = None
        self._dev_info = None

        # 분광기 일련번호/모델명(spDevInfo), 연결 목록 기준(정렬). 기준이 None/빈 목록이면 감시 끔(옛 EZUSB 드라이버 등)
        self._serial: str = ""
        self._dev_model: str = ""
        self._link_baseline: Optional[list] = None

        # __init__에 멤버 추가
        self._last_scan_code: int = 0
        self._last_scan_msg: str = ""
        self._last_error: str = ""
        self._detected_channels: int = 0
        self._last_scan: dict = {}   # ✅ 명시

    async def _call(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        lock = _get_dll_lock()
        async with lock:
            return await loop.run_in_executor(_OES_EXECUTOR, lambda: func(*args, **kwargs))

    def _bind_functions(self):
        assert self.sp_dll is not None
        L = self.sp_dll

        L.spTestAllChannels.argtypes = [ctypes.c_int16]
        L.spTestAllChannels.restype  = ctypes.c_int16

        L.spSetupGivenChannel.argtypes = [ctypes.c_int16]
        L.spSetupGivenChannel.restype  = ctypes.c_int16

        L.spInitGivenChannel.argtypes = [ctypes.c_int16, ctypes.c_int16]
        L.spInitGivenChannel.restype  = ctypes.c_int16

        L.spReadDataEx.argtypes = [ctypes.POINTER(ctypes.c_int32), ctypes.c_int16]
        L.spReadDataEx.restype  = ctypes.c_int16

        L.spCloseGivenChannel.argtypes = [ctypes.c_int16]
        L.spCloseGivenChannel.restype  = ctypes.c_int16

        self._auto_dark = getattr(L, "spAutoDark", None)
        if self._auto_dark:
            with contextlib.suppress(Exception):
                self._auto_dark.argtypes = [ctypes.c_int16]
                self._auto_dark.restype  = ctypes.c_int16

        self._set_trg = getattr(L, "spSetTrgEx", None)
        if self._set_trg:
            with contextlib.suppress(Exception):
                self._set_trg.argtypes = [ctypes.c_int16, ctypes.c_int16]
                self._set_trg.restype  = ctypes.c_int16

        self._set_tec = getattr(L, "spSetTEC", None)
        if self._set_tec:
            with contextlib.suppress(Exception):
                self._set_tec.argtypes = [ctypes.c_int32, ctypes.c_int16]
                self._set_tec.restype  = ctypes.c_int16

        self._set_dbl_int = getattr(L, "spSetDblIntEx", None)
        if self._set_dbl_int:
            with contextlib.suppress(Exception):
                self._set_dbl_int.argtypes = [ctypes.c_double, ctypes.c_int16]
                self._set_dbl_int.restype  = ctypes.c_int16

        self._get_wl = getattr(L, "spGetWLTable", None)
        if self._get_wl:
            with contextlib.suppress(Exception):
                self._get_wl.argtypes = [ctypes.POINTER(ctypes.c_double), ctypes.c_int16]
                self._get_wl.restype  = ctypes.c_int16

        self._dev_info = getattr(L, "spDevInfo", None)
        if self._dev_info:
            with contextlib.suppress(Exception):
                self._dev_info.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int16]
                self._dev_info.restype  = ctypes.c_int16

    def _pick_model_with_probe(self, ch: int) -> Optional[int]:
        # device/oes.py와 동일: PDA → G9212 → SONY → TOSHIBA → S10420
        for m in (SP_CCD_PDA, SP_CCD_G9212, SP_CCD_SONY, SP_CCD_TOSHIBA, SP_CCD_S10420):
            try:
                _runlog(f"[scan] spInitGivenChannel begin model={m} ch={ch}")
                r = int(self.sp_dll.spInitGivenChannel(ctypes.c_int16(m), ctypes.c_int16(ch)))  # type: ignore
                _runlog(f"[scan] spInitGivenChannel rc={r} model={m} ch={ch}")
            except Exception as e:
                _runlog(f"[scan] spInitGivenChannel EXC model={m} ch={ch}: {type(e).__name__}: {e}")
                r = -1

            if r >= 0:
                self._model_name = {
                    SP_CCD_PDA: "PDA",
                    SP_CCD_G9212: "G9212",
                    SP_CCD_SONY: "SONY",
                    SP_CCD_TOSHIBA: "TOSHIBA",
                    SP_CCD_S10420: "S10420",
                }.get(m, "UNKNOWN")
                self._model = int(m)
                return int(m)

        return None

    def _scan_and_open(self) -> Tuple[int, str]:
        info = {
            "dll_path": self._dll_path,
            "dll_exists": Path(self._dll_path).is_file(),
            "target_usb_index": int(self._usb_index),
        }

        try:
            _runlog(f"[scan] load dll begin path={self._dll_path}")
            _add_dll_search_dir(self._dll_path)

            self.sp_dll = ctypes.WinDLL(self._dll_path)
            _runlog("[scan] load dll ok")

            self._bind_functions()
            _runlog("[scan] bind ok")

            try:
                _runlog(f"[scan] spTestAllChannels begin order={ORDER_USB}")
                n = int(self.sp_dll.spTestAllChannels(ctypes.c_int16(ORDER_USB)))  # type: ignore
                _runlog(f"[scan] spTestAllChannels rc={n}")
            except Exception as e:
                _runlog(f"[scan] spTestAllChannels EXC: {type(e).__name__}: {e}")
                raise

            info["detected_count"] = int(n)

            opened: list[int] = []
            usb = int(self._usb_index)

            try:
                _runlog(f"[scan] spSetupGivenChannel begin usb={usb}")
                rr = int(self.sp_dll.spSetupGivenChannel(ctypes.c_int16(usb)))  # type: ignore
                _runlog(f"[scan] spSetupGivenChannel rc={rr} usb={usb}")

                if rr >= 0:
                    opened.append(usb)
                info["setup_target_rc"] = int(rr)
            except Exception as e:
                info["setup_target_exc"] = f"{type(e).__name__}: {e}"
                _runlog(f"[scan] spSetupGivenChannel EXC usb={usb}: {type(e).__name__}: {e}")
                raise

            info["opened"] = opened

            if n <= 0:
                msg = f"장치 스캔 실패: detected={n} opened={opened}"
                self._last_scan = info
                self._last_error = msg
                return -10, msg

            if usb < 0 or usb >= n:
                msg = f"usb_index out of range: usb_index={usb}, detected={n}, opened={opened}"
                self._last_scan = info
                self._last_error = msg
                return -11, msg

            if usb not in opened:
                msg = f"대상 USB{usb} 오픈 실패 (opened={opened})"
                self._last_scan = info
                self._last_error = msg
                return -12, msg

            _runlog(f"[scan] probe begin usb={usb}")
            model = self._pick_model_with_probe(usb)
            _runlog(f"[scan] probe end usb={usb} model={self._model_name} model_id={model}")

            info["model"] = self._model_name
            if model is None:
                msg = f"초기화 실패: USB{usb} — 모든 모델 probe 실패"
                self._last_scan = info
                self._last_error = msg
                return -13, msg

            self._npix = self._ensure_npixels(usb, int(model))
            _runlog(f"[scan] ensure_npixels ok usb={usb} npix={self._npix}")

            lim = self._npix if self._wl is None else int(min(self._npix, self._wl.size))
            self._roi_end = min(ROI_END_DEFAULT, int(lim))
            self._roi_start = min(ROI_START_DEFAULT, max(0, self._roi_end - 1))

            self.sChannel = int(usb)
            msg = f"open ok: USB{usb}, model={self._model_name}, pixels={self._npix}" + (", wl=nm" if self._wl is not None else ", wl=pixel")
            self._last_scan = info
            self._last_error = ""
            return 0, msg

        except Exception as e:
            msg = f"scan/open exception: {type(e).__name__}: {e}"
            info["exception"] = msg
            self._last_scan = info
            self._last_error = msg
            _errlog_exc(msg)
            return -99, msg
        
    def _load_and_bind_blocking(self) -> None:
        _runlog(f"[scan] load dll begin path={self._dll_path}")
        _add_dll_search_dir(self._dll_path)
        self.sp_dll = ctypes.WinDLL(self._dll_path)
        _runlog("[scan] load dll ok")

        self._bind_functions()
        _runlog("[scan] bind ok")

    def _test_all_channels_blocking(self) -> int:
        assert self.sp_dll is not None
        _runlog(f"[scan] spTestAllChannels begin order={ORDER_USB}")
        n = int(self.sp_dll.spTestAllChannels(ctypes.c_int16(ORDER_USB)))  # type: ignore
        _runlog(f"[scan] spTestAllChannels rc={n}")
        return n

    def _setup_channel_blocking(self, usb: int) -> int:
        assert self.sp_dll is not None
        _runlog(f"[scan] spSetupGivenChannel begin usb={usb}")
        rr = int(self.sp_dll.spSetupGivenChannel(ctypes.c_int16(usb)))  # type: ignore
        _runlog(f"[scan] spSetupGivenChannel rc={rr} usb={usb}")
        return rr

    def _probe_model_blocking(self, usb: int) -> Optional[int]:
        _runlog(f"[scan] probe begin usb={usb}")
        model = self._pick_model_with_probe(usb)
        _runlog(f"[scan] probe end usb={usb} model={self._model_name} model_id={model}")
        return model

    def _finalize_channel_blocking(self, usb: int, model: int) -> None:
        self._npix = self._ensure_npixels(usb, int(model))
        _runlog(f"[scan] ensure_npixels ok usb={usb} npix={self._npix}")

        lim = self._npix if self._wl is None else int(min(self._npix, self._wl.size))
        self._roi_end = min(ROI_END_DEFAULT, int(lim))
        self._roi_start = min(ROI_START_DEFAULT, max(0, self._roi_end - 1))
        # ✅ sChannel 은 initialize_device 가 모든 확인을 통과한 뒤에만 넣는다(실패·타임아웃 시 '닫힘' 유지)

    def _read_pixels(self, ch: int, npix: int) -> Tuple[int, Optional[np.ndarray]]:
        assert self.sp_dll is not None
        buf = (ctypes.c_int32 * npix)()
        r = self.sp_dll.spReadDataEx(buf, ctypes.c_int16(ch))  # type: ignore
        if r < 0:
            return int(r), None
        arr = np.asarray(buf[:npix], dtype=float)
        return int(r), arr

    def _try_fetch_wl(self, ch: int, length_hint: int) -> Optional[np.ndarray]:
        if not self._get_wl:
            return None
        n = max(1, int(length_hint))
        buf = (ctypes.c_double * n)()
        try:
            r = int(self._get_wl(buf, ctypes.c_int16(ch)))  # type: ignore
        except Exception:
            return None
        if r < 0:
            return None
        return np.asarray(buf, dtype=float)

    def _ensure_npixels(self, ch: int, model: int) -> int:
        # 1) 모델 기본 픽셀 길이로 1차 확정
        guess = int(PIXELS.get(model, 2048))
        rc, arr = self._read_pixels(ch, guess)
        if rc >= 0 and arr is not None and arr.size > 10:
            npix = int(arr.size)
        else:
            npix = guess
            for cand in CANDIDATE_PIXELS:
                rc2, arr2 = self._read_pixels(ch, cand)
                if rc2 >= 0 and arr2 is not None and arr2.size > 10:
                    npix = int(arr2.size)
                    break

        # 2) 그 길이에 맞춰 WL 테이블 재획득(성공 시 nm축 사용)
        wl = self._try_fetch_wl(ch, npix)
        if wl is not None and wl.size >= min(npix, ROI_END_DEFAULT):
            self._wl = wl[:npix]
        else:
            self._wl = None

        return int(npix)

    def _read_dev_info_blocking(self, ch: int) -> Tuple[int, str, str]:
        """spDevInfo 로 장치의 모델명·일련번호를 읽는다. return: (rc, model, serial)"""
        if not self._dev_info:
            return -1, "", ""
        mb = ctypes.create_string_buffer(_DEV_INFO_BUF)
        sb = ctypes.create_string_buffer(_DEV_INFO_BUF)
        rc = int(self._dev_info(mb, sb, ctypes.c_int16(ch)))  # type: ignore
        model = mb.value.decode("ascii", errors="replace").strip()
        serial = sb.value.decode("ascii", errors="replace").strip()
        _runlog(f"[init] spDevInfo rc={rc} ch={ch} model={model!r} serial={serial!r}")
        return rc, model, serial

    def link_ok(self) -> bool:
        """연결 목록 판정: 감시가 꺼져 있으면 항상 정상, 켜져 있으면 현재 목록(정렬)이 기준과 같을 때만 정상."""
        if not self._link_baseline:
            return True
        cur = _list_cyusb_interfaces()
        if cur is None:
            _runlog("[link] 연결 목록 조회 실패(일시) → 정상으로 간주")
            return True
        return sorted(cur) == self._link_baseline

    def _apply_device_settings_blocking(self, ch: int, integration_ms: int) -> None:
        # ✅ 기본은 기존과 동일하게 AutoDark 실행
        #    단, 공정 중 시작되는 경우를 대비해서 env로 OFF 가능
        # do_autodark = os.environ.get("OES_AUTODARK", "1") == "1"
        # if do_autodark and self._auto_dark:
        #     with contextlib.suppress(Exception):
        #         self._auto_dark(ctypes.c_int16(ch))

        if self._set_trg:
            with contextlib.suppress(Exception):
                self._set_trg(ctypes.c_int16(11), ctypes.c_int16(ch))
        if self._set_tec:
            with contextlib.suppress(Exception):
                self._set_tec(ctypes.c_int32(1), ctypes.c_int16(ch))
        if self._set_dbl_int:
            with contextlib.suppress(Exception):
                self._set_dbl_int(ctypes.c_double(float(integration_ms)), ctypes.c_int16(ch))

    async def initialize_device(self) -> bool:
        info = {
            "dll_path": self._dll_path,
            "dll_exists": Path(self._dll_path).is_file(),
            "target_usb_index": int(self._usb_index),
        }

        usb = int(self._usb_index)
        total_timeout_s = _env_float("OES_INIT_TIMEOUT_S", 25.0)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + float(total_timeout_s)
        current_stage = "start"

        async def _run_step(stage: str, max_timeout_s: float, func, *args):
            nonlocal current_stage
            current_stage = stage

            remain = deadline - loop.time()
            if remain <= 0:
                raise asyncio.TimeoutError()

            timeout_s = min(float(max_timeout_s), float(remain))
            _runlog(f"[init] step={stage} timeout={timeout_s:.1f}s")

            return await asyncio.wait_for(self._call(func, *args), timeout=timeout_s)

        # ✅ 모든 확인을 통과하기 전까지는 '닫힘'(sChannel=-1)
        self.sChannel = -1

        try:
            await _run_step("load_bind", 3.0, self._load_and_bind_blocking)

            n = int(await _run_step("test_all_channels", 4.0, self._test_all_channels_blocking))
            info["detected_count"] = int(n)
            self._detected_channels = int(n)

            # ✅ spTestAllChannels 직후 연결 목록을 기준으로 저장.
            #    재초기화(같은 객체)에서는 최초 기준과 비교만 한다: 분광기가 빠진 상태로 번호를 다시 매기면
            #    다른 챔버 분광기를 열 수 있으므로 그때는 실패시킨다(워커 재기동 시 새 기준).
            link_snap = _list_cyusb_interfaces()
            link_snap = sorted(link_snap) if link_snap else None
            _runlog(f"[init] link list={link_snap} baseline={self._link_baseline}")
            if self._link_baseline and link_snap is not None and link_snap != self._link_baseline:
                msg = f"초기화 중 분광기 연결 변화 (기준 {len(self._link_baseline)}대, 현재 {len(link_snap)}대)"
                self._last_scan = info
                self._last_error = msg
                return False
            if not self._link_baseline:
                self._link_baseline = link_snap

            if n <= 0:
                msg = f"장치 스캔 실패: detected={n}"
                self._last_scan = info
                self._last_error = msg
                return False

            if usb < 0 or usb >= n:
                msg = f"usb_index out of range: usb_index={usb}, detected={n}"
                self._last_scan = info
                self._last_error = msg
                return False

            # ✅ 일련번호 사전 확인: 포트 리셋·probe 전에 spDevInfo 로 읽어, 다른 챔버 분광기면 명령 없이 실패.
            #    읽기 실패(rc<0, 예외, 빈 값)면 기존 순서대로 진행하고 채널 설정 뒤에 다시 읽는다. 타임아웃은 실패.
            expected = _expected_serial_for(self._chamber)
            dev_rc, dev_model, dev_serial = -1, "", ""
            try:
                dev_rc, dev_model, dev_serial = await _run_step("dev_info_pre", 4.0, self._read_dev_info_blocking, usb)
            except asyncio.TimeoutError:
                raise
            except Exception as e:
                _runlog(f"[init] 사전 spDevInfo EXC usb={usb}: {type(e).__name__}: {e}")
            dev_pre_ok = int(dev_rc) >= 0 and bool(str(dev_serial).strip())
            if dev_pre_ok:
                self._dev_model = str(dev_model)
                self._serial = str(dev_serial)
                info["serial"] = self._serial
                if expected and self._serial.strip().lower() != expected.strip().lower():
                    msg = f"분광기 일련번호 불일치: CH{self._chamber} 기대={expected}, 실제={self._serial}"
                    self._last_scan = info
                    self._last_error = msg
                    return False
            else:
                _runlog(f"[init] 사전 spDevInfo 읽기 실패 rc={dev_rc} usb={usb} → 채널 설정 뒤 다시 읽음")

            rr = int(await _run_step("setup_channel", 4.0, self._setup_channel_blocking, usb))
            info["setup_target_rc"] = int(rr)
            info["opened"] = [usb] if rr >= 0 else []

            if rr < 0:
                msg = f"대상 USB{usb} 오픈 실패 (setup_rc={rr})"
                self._last_scan = info
                self._last_error = msg
                return False

            model = await _run_step("probe_model", 8.0, self._probe_model_blocking, usb)
            info["model"] = self._model_name

            if model is None:
                msg = f"초기화 실패: USB{usb} — 모든 모델 probe 실패"
                self._last_scan = info
                self._last_error = msg
                return False

            await _run_step("finalize_channel", 4.0, self._finalize_channel_blocking, usb, int(model))

            if not self._npix or int(self._npix) <= 0:
                msg = f"npixels invalid: {self._npix}"
                self._last_scan = info
                self._last_error = msg
                return False

            # ✅ 분광기 모델명·일련번호 기록(읽기 실패는 초기화를 실패시키지 않음. 단계 타임아웃은 기존대로 실패)
            #    사전 확인에서 이미 읽었으면 다시 읽지 않는다.
            if not dev_pre_ok:
                dev_rc, dev_model, dev_serial = -1, "", ""
                try:
                    dev_rc, dev_model, dev_serial = await _run_step("dev_info", 4.0, self._read_dev_info_blocking, usb)
                except asyncio.TimeoutError:
                    raise
                except Exception as e:
                    _runlog(f"[init] spDevInfo EXC usb={usb}: {type(e).__name__}: {e}")
                if int(dev_rc) < 0:
                    _runlog(f"[init] spDevInfo 읽기 실패 rc={dev_rc} usb={usb}")
                    dev_model, dev_serial = "", ""
                self._dev_model = str(dev_model)
                self._serial = str(dev_serial)
                info["serial"] = self._serial

            serial_match = (not expected) or self._serial.strip().lower() == expected.strip().lower()
            if not expected:
                check_txt = "확인=꺼짐(expected_serial 없음)"
            elif serial_match:
                check_txt = f"확인=일치(기대 {expected})"
            else:
                check_txt = f"확인=불일치(기대 {expected})"

            link_now = _list_cyusb_interfaces()
            _status(
                f"[init] 분광기 확인 USB{usb}: 모델={self._dev_model or '?'}({self._model_name}), "
                f"일련번호={self._serial or '(읽기 실패)'}, 연결 목록={link_now if link_now is not None else '(조회 불가)'}, "
                f"읽은 시점={'사전' if dev_pre_ok else '채널 설정 후'}, {check_txt}"
            )

            if not serial_match:
                msg = f"분광기 일련번호 불일치: CH{self._chamber} 기대={expected}, 실제={self._serial or '(읽기 실패)'}"
                self._last_scan = info
                self._last_error = msg
                return False

            if self._link_baseline and link_now is not None and sorted(link_now) != self._link_baseline:
                msg = "초기화 중 분광기 연결 변화"
                self._last_scan = info
                self._last_error = msg
                return False

            msg = (
                f"open ok: USB{usb}, model={self._model_name}, pixels={self._npix}"
                + (", wl=nm" if self._wl is not None else ", wl=pixel")
            )
            self._last_scan = info
            self._last_error = ""
            self.sChannel = int(usb)
            _runlog(f"[init] success usb={usb} msg={msg}")
            return True

        except asyncio.TimeoutError:
            info["timeout_stage"] = current_stage
            msg = f"initialize timeout at stage={current_stage} usb={usb} scan={info}"
            self._last_scan = info
            self._last_error = msg
            _errlog(msg)
            return False

        except Exception as e:
            msg = f"initialize exception: {type(e).__name__}: {e}"
            info["exception"] = msg
            info["failed_stage"] = current_stage
            self._last_scan = info
            self._last_error = msg
            _errlog_exc(msg)
            return False

    def _acquire_one_slice_avg(self):
        if self.sp_dll is None or self.sChannel < 0 or self._npix <= 0:
            raise RuntimeError("device not ready")

        ch = int(self.sChannel)
        npix = int(self._npix)

        intensity_sum = np.zeros(npix, dtype=float)
        valid = 0
        for _ in range(self._avg_count):
            r, arr = self._read_pixels(ch, npix)
            if r >= 0 and arr is not None and arr.size >= min(self._roi_end, npix):
                intensity_sum += arr
                valid += 1

        if valid == 0:
            return None, None

        avg = intensity_sum / float(valid)
        start = self._roi_start
        end   = min(self._roi_end, npix)

        if self._wl is not None and self._wl.size >= end:
            x = np.asarray(self._wl[start:end], dtype=float)
        else:
            x = np.arange(start, end, dtype=float)
        y = np.asarray(avg[start:end], dtype=float)
        return x, y

    async def cleanup(self) -> None:
        if self.sp_dll is None or self.sChannel < 0:
            return

        ch = int(self.sChannel)
        dll = self.sp_dll
        h = getattr(dll, "_handle", None)

        # ✅ 목록이 기준과 다르면 같은 번호가 다른 챔버 분광기일 수 있으므로 닫기 명령을 보내지 않는다
        #    (감시 꺼짐·조회 실패(None)·목록 같음이면 기존대로 닫는다)
        if self._link_baseline and not self.link_ok():
            _runlog("[cleanup] 연결 목록이 기준과 다름 → 닫기 명령 생략")
        else:
            with contextlib.suppress(Exception):
                await self._call(self._safe_close_channel_blocking, ch)

        # ✅ DLL 언로드는 기본 OFF(크래시 리스크 줄임). 정말 필요할 때만 켜기.
        do_unload = os.environ.get("OES_DLL_UNLOAD", "0") == "1"
        if do_unload and os.name == "nt" and h:
            with contextlib.suppress(Exception):
                k32 = ctypes.WinDLL("kernel32", use_last_error=True)
                k32.FreeLibrary.argtypes = [ctypes.c_void_p]
                k32.FreeLibrary.restype = ctypes.c_int
                k32.FreeLibrary(ctypes.c_void_p(h))

            # (선택) ctypes 객체가 나중에 다시 FreeLibrary 시도하는 걸 피하려는 안전장치
            # 완벽 보장은 아니지만, 옵션일 때만 쓰는 게 낫다.
            with contextlib.suppress(Exception):
                setattr(dll, "_handle", None)

        self.sChannel = -1
        self.sp_dll = None

    def _safe_close_channel_blocking(self, ch: int) -> None:
        if self.sp_dll is None:
            return
        with contextlib.suppress(Exception):
            self.sp_dll.spCloseGivenChannel(ctypes.c_int16(ch))  # type: ignore


# ====== 워커 측정 로직(append+flush CSV) ======

def _default_out_dir(ch: int) -> Path:
    base = Path(os.environ.get("OES_LOCAL_BASE", str(Path.home() / "Desktop" / "OES")))
    return base / f"CH{int(ch)}"


def _make_default_filename() -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"OES_Data_{ts}.csv"


def _env_float(name: str, default: float) -> float:
    try:
        v = (os.environ.get(name, "") or "").strip()
        return float(v) if v else float(default)
    except Exception:
        return float(default)


def _stop_dir() -> Path:
    # ✅ 메인이 out_dir 몰라도 stop 보낼 수 있게 "고정" stop 폴더 사용
    base = Path(os.environ.get("OES_STOP_DIR", str(_worker_base_dir() / "stop")))
    try:
        base.mkdir(parents=True, exist_ok=True)
    except Exception:
        # 최후: TEMP
        base = Path(os.environ.get("TEMP", str(Path.home()))) / "VanaM_OES_STOP"
        base.mkdir(parents=True, exist_ok=True)
    return base


class _ResetCallTimeout(TimeoutError):
    """포트 리셋(spSetupGivenChannel) 호출 타임아웃 → 읽기 타임아웃과 같은 hard_abort 경로."""


async def _acquire_named_mutex_async(mtx: _WinMutex, max_wait_s: float) -> bool:
    # 뮤텍스는 잡은 스레드에서 풀어야 하므로 이벤트 루프 스레드에서 0ms 대기로 반복 확인한다
    loop = asyncio.get_running_loop()
    end = loop.time() + float(max_wait_s)
    while True:
        if mtx.acquire(timeout_ms=0):
            return True
        mtx.release()  # 소유하지 못한 핸들 닫기(ReleaseMutex 실패는 무시됨)
        if loop.time() >= end:
            return False
        await asyncio.sleep(_LINK_POLL_S)


class _LinkGuard:
    """
    측정 루프(daemon / one-shot) 공용: 분광기 연결 목록 감시 + 연속 읽기 실패 복구.
    - 목록이 기준과 다르면 DLL 을 부르지 않고 '연결 끊김'(일시 중지) 상태로 둔다.
    - 목록이 돌아오면 잠시 기다린 뒤 재초기화(뮤텍스)하고 측정을 재개한다.
    - 목록은 정상인데 읽기가 연속 실패하면 N번째에 포트 리셋, M번째에 재초기화.
    """

    def __init__(self, oes: OESAsync, *, ch: int, usb: int, integration_ms: int):
        self.oes = oes
        self.ch = int(ch)
        self.usb = int(usb)
        self.integration_ms = int(integration_ms)

        self.paused = False
        self._pause_t0 = 0.0
        self._last_wait_log = 0.0
        self._next_reinit_try = 0.0
        self._fail_streak = 0
        self._last_fail_reinit: Optional[float] = None

        # finished 페이로드용
        self.link_pause_count = 0     # 일시 중지(연결 끊김) 진입 횟수
        self.link_pause_s = 0.0       # 일시 중지 후 재개까지 걸린 시간 합(초)
        self.reinit_count = 0         # 재초기화 수행 횟수(뮤텍스 획득 후 시도 기준)
        self.reset_count = 0          # 포트 리셋(spSetupGivenChannel) 횟수

    def payload(self) -> dict:
        return {
            "link_pause_count": int(self.link_pause_count),
            "link_pause_s": round(float(self.link_pause_s), 1),
            "reinit_count": int(self.reinit_count),
            "reset_count": int(self.reset_count),
        }

    def _link_changed_msg(self) -> str:
        cur = _list_cyusb_interfaces()
        n = len(cur) if cur is not None else "?"
        m = len(self.oes._link_baseline or [])
        return f"[link] 분광기 연결 변화 감지(현재 {n}대/기준 {m}대) → 측정 일시 중지"

    def _enter_pause(self, msg: str) -> None:
        if self.paused:
            return
        now = time.time()
        self.paused = True
        self._pause_t0 = now
        self._last_wait_log = now
        self._next_reinit_try = 0.0
        self.link_pause_count += 1
        _status(msg)

    def _resume(self) -> None:
        dur = max(0.0, time.time() - self._pause_t0)
        self.paused = False
        self.link_pause_s += dur
        self._fail_streak = 0
        _status(f"[link] 분광기 재연결 → 재초기화 완료, 측정 재개(중단 {dur:.0f}초)")

    async def _reinit(self) -> bool:
        mtx = _WinMutex(_REINIT_MUTEX_NAME)
        if not await _acquire_named_mutex_async(mtx, _REINIT_MUTEX_WAIT_S):
            _status(f"[link] 재초기화 뮤텍스 대기 초과({_REINIT_MUTEX_WAIT_S:.0f}초) → 다음 기회에 재시도")
            return False
        try:
            self.reinit_count += 1
            ok = await _daemon_reset_device(self.oes, ch=self.ch, usb=self.usb)
            if not ok:
                _status(f"[link] 재초기화 실패: {self.oes._last_error or 'initialize_device failed'}")
                return False
            with contextlib.suppress(Exception):
                await self.oes._call(self.oes._apply_device_settings_blocking, int(self.oes.sChannel), self.integration_ms)
            return True
        finally:
            mtx.release()

    async def reset_port(self, msg: str) -> None:
        """spSetupGivenChannel 1회(포트 리셋). 호출 타임아웃이면 _ResetCallTimeout."""
        self.reset_count += 1
        _status(msg)
        try:
            await asyncio.wait_for(
                self.oes._call(self.oes._setup_channel_blocking, int(self.oes.sChannel)),
                timeout=_RESET_CALL_TIMEOUT_S,
            )
        except asyncio.TimeoutError as e:
            raise _ResetCallTimeout(
                f"OES reset(spSetupGivenChannel) timeout after {_RESET_CALL_TIMEOUT_S}s (ch={self.ch}, usb={self.usb})"
            ) from e
        except Exception as e:
            _runlog(f"[read] 포트 리셋 예외: {type(e).__name__}: {e}")

    async def ensure_at_start(self) -> None:
        """측정 시작(첫 프레임 전): 목록이 기준과 다르면 복귀를 기다렸다가 재초기화. 안 되면 RuntimeError."""
        if self.oes.link_ok():
            if self.oes.sp_dll is not None and int(self.oes.sChannel) >= 0:
                return
            # 이전 재초기화 실패로 장치가 닫혀 있음 → 목록은 정상이므로 재초기화부터
            if not await self._reinit():
                raise RuntimeError("[link] 측정 시작 전 분광기 재초기화 실패")
            return
        self._enter_pause(self._link_changed_msg())
        end = time.time() + _LINK_WAIT_AT_START_S
        while not self.oes.link_ok():
            if time.time() >= end:
                raise RuntimeError(
                    f"[link] 측정 시작 전 분광기 연결 목록이 기준과 다름({_LINK_WAIT_AT_START_S:.0f}초 대기 후에도 복귀 안 됨)"
                )
            await asyncio.sleep(_LINK_POLL_S)
        await asyncio.sleep(_LINK_SETTLE_S)
        if not self.oes.link_ok() or not await self._reinit():
            raise RuntimeError("[link] 측정 시작 전 분광기 재초기화 실패")
        self._resume()

    async def _tick_paused(self) -> None:
        now = time.time()
        if not self.oes.link_ok():
            if now - self._last_wait_log >= _LINK_STATUS_EVERY_S:
                self._last_wait_log = now
                _status(f"[link] 재연결 대기 중(경과 {now - self._pause_t0:.0f}초)")
            return
        if now < self._next_reinit_try:
            return
        await asyncio.sleep(_LINK_SETTLE_S)
        if not self.oes.link_ok():
            return
        if await self._reinit():
            self._resume()
        else:
            self._next_reinit_try = time.time() + _REINIT_RETRY_S
            _status(f"[link] {_REINIT_RETRY_S:.0f}초 뒤 재초기화 재시도")

    async def before_read(self) -> bool:
        """샘플 읽기 직전. False 면 DLL 호출 없이 이번 샘플을 건너뛴다."""
        if self.paused:
            await self._tick_paused()
            return not self.paused
        if not self.oes.link_ok():
            self._enter_pause(self._link_changed_msg())
            return False
        return True

    async def after_read(self, got: bool) -> bool:
        """샘플 읽기 직후. True 면 행을 기록한다(목록이 바뀌었으면 방금 샘플은 버린다)."""
        if not self.oes.link_ok():
            self._enter_pause(self._link_changed_msg())
            return False
        if got:
            self._fail_streak = 0
            return True

        self._fail_streak += 1
        n = self._fail_streak
        if n == _READ_FAIL_RESET_AT:
            await self.reset_port(f"[read] 연속 읽기 실패 {n}회 → 포트 리셋")
        if n >= _READ_FAIL_REINIT_AT:
            now = time.time()
            if self._last_fail_reinit is None or now - self._last_fail_reinit >= _REINIT_RETRY_S:
                self._last_fail_reinit = now
                _status(f"[read] 연속 읽기 실패 {n}회 → 재초기화")
                if await self._reinit():
                    self._fail_streak = 0
                    _status("[read] 재초기화 완료, 측정 계속")
                else:
                    # 장치가 닫힌 상태라 읽을 수 없음 → 일시 중지하고 끊김과 같은 규칙으로 재시도
                    self._enter_pause(f"[read] 재초기화 실패 → 측정 일시 중지, {_REINIT_RETRY_S:.0f}초 뒤 재시도")
                    self._next_reinit_try = time.time() + _REINIT_RETRY_S
        return False


async def _acquire_first_frame(oes: OESAsync, retries: int = 20, delay_s: float = 0.2, guard: Optional[_LinkGuard] = None):
    # ✅ 첫 프레임도 DLL hang 가능 → 타임아웃으로 끊기
    call_timeout_s = _env_float("OES_FIRST_FRAME_TIMEOUT_S", 2.0)

    last_err = None
    empty = 0
    for _ in range(max(1, retries)):
        try:
            x, y = await asyncio.wait_for(
                oes._call(oes._acquire_one_slice_avg),
                timeout=call_timeout_s,
            )
            if x is not None and y is not None:
                return x, y
        except asyncio.TimeoutError as e:
            # ✅ 이건 DLL 스레드 hang 가능성이 높으니 즉시 상위로 올려서 워커가 '자가 종료' 루트로 가게 함
            raise TimeoutError(f"first frame timeout after {call_timeout_s}s") from e
        except Exception as e:
            last_err = e
        else:
            # (None, None): 3번째 실패마다 포트 리셋 1회(타임아웃이면 TimeoutError 로 올라감)
            empty += 1
            if guard is not None and empty % _READ_FAIL_RESET_AT == 0:
                await guard.reset_port(f"[read] 첫 프레임 읽기 실패 {empty}회 → 포트 리셋")
        await asyncio.sleep(delay_s)

    raise RuntimeError(f"first frame failed: {last_err}")


async def cmd_init(ch: int, usb: int, dll_path: Optional[str], out_dir: Optional[Path], out_csv: Optional[Path]) -> int:
    mtx = _WinMutex(f"Local\\VanaM_OES_USB{int(usb)}")
    mutex_ms = _mutex_timeout_ms()

    _status(f"[worker] init: acquiring mutex name={mtx.name} timeout_ms={mutex_ms}")
    acquired = mtx.acquire(timeout_ms=mutex_ms)

    try:
        if not acquired:
            _errlog(f"cmd=init mutex timeout ch={ch} usb={usb}")
            _print_json({"kind":"init","ok":False,"ch":int(ch),"usb":int(usb),"error":"mutex timeout"})
            return 4
        
        _status(f"[worker] init: mutex acquired name={mtx.name}")

        # ✅ out_dir/out_csv 반영 + dll 실제 resolve 정보까지 남김
        if out_csv:
            temp_dir = Path(out_csv).expanduser().resolve().parent
        elif out_dir:
            temp_dir = Path(out_dir).expanduser().resolve()
        else:
            temp_dir = _default_out_dir(ch)

        dll_resolved = _resolve_oes_dll_path(dll_path)
        dll_exists = Path(dll_resolved).is_file()

        _status(f"[worker] init begin ch={ch} usb={usb} dir={temp_dir} dll_arg={dll_path} dll_resolved={dll_resolved} dll_exists={dll_exists}")

        oes = OESAsync(chamber=int(ch), usb_index=int(usb), dll_path=dll_path, save_directory=str(temp_dir))
        init_timeout_s = _env_float("OES_INIT_TIMEOUT_S", 25.0)
        try:
            ok = await asyncio.wait_for(oes.initialize_device(), timeout=init_timeout_s)
        except asyncio.TimeoutError:
            msg = f"OES init timeout after {init_timeout_s}s (ch={ch}, usb={usb})"
            _errlog(msg)
            _print_json({"kind":"init","ok":False,"ch":int(ch),"usb":int(usb),"error":msg})
            if acquired:
                with contextlib.suppress(Exception):
                    mtx.release()
            os._exit(124)

        payload = {
            "kind": "init",
            "ok": bool(ok),
            "ch": int(ch),
            "usb": int(usb),
            "resolved_usb": int(getattr(oes, "sChannel", -1)),
            "pixels": int(getattr(oes, "_npix", 0) or 0),
            "model": str(getattr(oes, "_model_name", "UNKNOWN")),   # ✅ 추가
            "dll_resolved": str(getattr(oes, "_dll_path", "")),
            "dll_exists": bool(Path(getattr(oes, "_dll_path", "")).is_file()),
            "serial": str(getattr(oes, "_serial", "") or ""),
        }

        if not ok:
            payload["error"] = str(getattr(oes, "_last_error", "")) or "initialize_device failed"
            payload["scan"] = getattr(oes, "_last_scan", {}) or {}

        # ✅ 1) 결과를 먼저 부모에게 알림(디버깅/상태판단에 유리)
        _print_json(payload)

        # ✅ 2) 그 다음 정리(정리가 hang이면 워커가 알아서 종료)
        cleanup_timeout_s = _env_float("OES_CLEANUP_TIMEOUT_S", 5.0)
        try:
            await asyncio.wait_for(oes.cleanup(), timeout=cleanup_timeout_s)
        except asyncio.TimeoutError:
            msg = f"OES cleanup timeout after {cleanup_timeout_s}s (cmd=init ch={ch} usb={usb})"
            _errlog(msg)
            with contextlib.suppress(Exception):
                mtx.release()
            os._exit(126)
        except Exception:
            pass

        return 0 if ok else 2

    except Exception as e:
        _errlog_exc(f"cmd=init exception ch={ch} usb={usb} dll_path={dll_path}")
        _print_json({
            "kind": "init",
            "ok": False,
            "ch": int(ch),
            "usb": int(usb),
            "error": f"{type(e).__name__}: {e}",
            "trace": traceback.format_exc(),
        })
        return 3

    finally:
        # ✅ acquired 성공했을 때만 release (혹은 release가 내부적으로 안전해도 가드는 두는 게 깔끔)
        if acquired:
            with contextlib.suppress(Exception):
                mtx.release()


async def cmd_measure(
    ch: int,
    usb: int,
    duration_s: float,
    integration_ms: int,
    sample_interval_s: float,
    avg_count: int,
    out_dir: Optional[Path],
    out_csv: Optional[Path],
    dll_path: Optional[str],
) -> int:
    mtx = _WinMutex(f"Local\\VanaM_OES_USB{int(usb)}")
    mutex_ms = _mutex_timeout_ms()

    _status(f"[worker] measure: acquiring mutex name={mtx.name} timeout_ms={mutex_ms}")
    acquired = mtx.acquire(timeout_ms=mutex_ms)

    t0 = time.time()
    rows = 0
    f = None
    oes = None

    # ✅ out_dir_final/out_csv는 아직 확정 전 → 먼저 None/기본값으로 선언
    out_dir_final: Optional[Path] = None
    out_csv_final: Optional[Path] = None

    # ✅ stop flag도 아직 확정 전
    stop_flag_usb: Optional[Path] = None
    stop_flag_csv: Optional[Path] = None
    stop_flag_usb_global: Optional[Path] = None  # ✅ 추가: finally에서 안전하게 쓰기 위해

    stopped = False
    stop_reason = None

    # ✅ DLL hang 의심 상황에서 워커가 최종적으로 '자가 종료(os._exit)'하도록 하는 플래그들
    hard_abort = False
    hard_abort_error: Optional[str] = None
    hard_abort_exit_code: int = 0

    # ✅ mutex를 "한 번만" 해제하기 위한 가드 (acquired=False면 해제하지 않음)
    mutex_released = False
    def _release_mutex_once() -> None:
        nonlocal mutex_released
        if mutex_released:
            return
        mutex_released = True
        if not acquired:
            return
        with contextlib.suppress(Exception):
            mtx.release()

    try:
        if not acquired:
            _errlog(f"cmd=measure mutex timeout ch={ch} usb={usb} timeout_ms={mutex_ms}")
            _print_json({"kind":"finished","ok":False,"ch":int(ch),"usb":int(usb),"error":f"mutex timeout ({mutex_ms}ms)"})
            return 4

        # ✅ (1) out_dir_final/out_csv 확정: out_csv 우선, 그다음 out_dir, 없으면 default
        if out_csv:
            out_csv_final = Path(out_csv).expanduser().resolve()
            out_dir_final = out_csv_final.parent
        else:
            out_dir_final = Path(out_dir).expanduser().resolve() if out_dir else _default_out_dir(int(ch))
            out_csv_final = out_dir_final / _make_default_filename()

        # ✅ 확정된 값을 이후 코드가 쓰도록 덮어쓰기(아래 코드 수정 최소화)
        out_csv = out_csv_final

        # ✅ (2) 이제 out_dir_final을 만들 수 있음
        out_dir_final.mkdir(parents=True, exist_ok=True)

        # ✅ (3) stop flag 경로 확정
        #  - local(기존 호환)
        stop_flag_usb = out_dir_final / f".stop_usb{int(usb)}.flag"
        stop_flag_csv = Path(str(out_csv) + ".stop")

        #  - global(신규): 메인이 out_dir 몰라도 stop 보낼 수 있도록 고정 경로 추가
        stop_dir = _stop_dir()
        stop_flag_usb_global = stop_dir / f".stop_usb{int(usb)}.flag"

        # ✅ 워커가 뜨기 전/초기화 직전에 들어온 stop도 살리기 (메인은 기다리지 않으니까 중요)
        if stop_flag_usb_global.exists():
            stopped = True
            stop_reason = "pre_stop"
            _status("[worker] stop already requested before init (global flag exists)")

            _print_json({
                "kind": "finished",
                "ok": True,
                "stopped": True,
                "stop_reason": stop_reason,
                "ch": int(ch),
                "usb": int(usb),
                "out_csv": str(out_csv) if out_csv else None,
                "rows": int(rows),
                "elapsed_s": float(time.time() - t0),
                "nas_ok": False,
                "nas_csv": None,
                "nas_error": "pre_stop before init (no data)",
                "local_deleted": False,
            })
            return 0

        # 이전 실행 잔재 제거(스테일 stop 방지)
        # ✅ local stop만 제거 (global stop은 메인이 먼저 보낸 stop이 씹히지 않도록 유지)
        for p in (stop_flag_usb, stop_flag_csv):
            with contextlib.suppress(Exception):
                p.unlink()

        oes = OESAsync(
            chamber=int(ch),
            usb_index=int(usb),
            dll_path=dll_path,
            save_directory=str(out_dir_final),
            sample_interval_s=float(sample_interval_s),
            avg_count=int(avg_count),
            debug_print=False,
        )

        _print_json({"kind": "status", "message": f"[worker] init start ch={ch} usb={usb} dll_path={dll_path}"})

        init_timeout_s = _env_float("OES_INIT_TIMEOUT_S", 25.0)
        try:
            ok = await asyncio.wait_for(oes.initialize_device(), timeout=init_timeout_s)
        except asyncio.TimeoutError:
            msg = f"OES initialize_device timeout after {init_timeout_s}s (ch={ch}, usb={usb})"
            _errlog(msg)

            # ✅ 메인은 절대 기다리지 않게: 워커가 스스로 '확실히' 종료해야 함
            _print_json({"kind": "finished", "ok": False, "ch": int(ch), "usb": int(usb), "out_csv": str(out_csv), "error": msg})

            # mutex/stop파일 정리 후 하드 종료(스레드 hang으로 sys.exit가 먹지 않을 수 있음)
            with contextlib.suppress(Exception):
                stop_flag_usb.unlink()
            with contextlib.suppress(Exception):
                stop_flag_csv.unlink()
            with contextlib.suppress(Exception):
                stop_flag_usb_global.unlink()
            _release_mutex_once()
            os._exit(124)

        _print_json({"kind": "status", "message": f"[worker] init done ok={ok} resolved_usb={getattr(oes,'sChannel',-1)} pixels={getattr(oes,'_npix',0)}"})

        if not ok or getattr(oes, "sChannel", -1) < 0:
            init_err = str(getattr(oes, "_last_error", "")) or "OES initialize_device() failed"
            raise RuntimeError(init_err)

        # ✅ 분광기 연결 감시/읽기 실패 복구(daemon 과 같은 규칙)
        #    목록 확인을 먼저 해서, 기준과 다를 때 설정 명령이 다른 챔버 분광기로 나가지 않게 한다
        guard = _LinkGuard(oes, ch=int(ch), usb=int(usb), integration_ms=int(integration_ms))
        await guard.ensure_at_start()

        with contextlib.suppress(Exception):
            await oes._call(oes._apply_device_settings_blocking, int(oes.sChannel), int(integration_ms))

        _print_json({"kind": "status", "message": f"[worker] open csv: {out_csv}"})
        f = open(str(out_csv), "w", newline="", encoding="utf-8")
        w = csv.writer(f)

        x, y = await _acquire_first_frame(oes, guard=guard)
        x_list = x.tolist() if hasattr(x, "tolist") else list(x)
        y_list = y.tolist() if hasattr(y, "tolist") else list(y)

        w.writerow(["Time"] + [float(v) for v in x_list])
        f.flush()

        _print_json({
            "kind": "started",
            "ok": True,
            "ch": int(ch),
            "usb": int(usb),
            "resolved_usb": int(getattr(oes, "sChannel", -1)),
            "out_csv": str(out_csv),
            "cols": int(len(x_list)),
            "model": str(getattr(oes, "_model_name", "UNKNOWN")),   # ✅ 추가
            "sample_interval_s": float(sample_interval_s),
            "avg_count": int(avg_count),
            "integration_ms": int(integration_ms),
        })

        now_s = datetime.now().strftime("%H:%M:%S")
        w.writerow([now_s] + [float(v) for v in y_list])
        rows += 1
        f.flush()

        # ── OES Anomaly: Ar 감시 초기화 (처음/마지막 N행 제외) ──
        _ar_idx = _find_ar_index(x_list) if _ANOMALY_ENABLED else None
        _ar_sum = 0.0
        _ar_count = 0
        _ar_row_num = 0
        _ar_cutoff = time.time() + max(0.0, float(duration_s)) - (_ANOMALY_SKIP_LAST * float(sample_interval_s))

        deadline = time.time() + max(0.0, float(duration_s))
        while time.time() < deadline:
            # ✅ stop 요청 감지(USB 기반 / CSV 기반)
            if stop_flag_usb_global.exists() or stop_flag_usb.exists() or (stop_flag_csv and stop_flag_csv.exists()):
                stopped = True
                stop_reason = "stop_flag"
                _status(
                    f"[worker] stop requested "
                    f"(usb_global={stop_flag_usb_global.exists()} usb_local={stop_flag_usb.exists()} "
                    f"csv_flag={(stop_flag_csv.exists() if stop_flag_csv else None)})"
                )
                break

            await asyncio.sleep(float(sample_interval_s))

            # sleep 직후 한번 더(반응성)
            if stop_flag_usb_global.exists() or stop_flag_usb.exists() or (stop_flag_csv and stop_flag_csv.exists()):
                stopped = True
                stop_reason = "stop_flag"
                _status(
                    f"[worker] stop requested "
                    f"(usb_global={stop_flag_usb_global.exists()} usb_local={stop_flag_usb.exists()} "
                    f"csv_flag={(stop_flag_csv.exists() if stop_flag_csv else None)})"
                )
                break

            # ✅ 연결 목록이 기준과 다르면(또는 끊김 중이면) DLL 호출 없이 이번 샘플 건너뜀
            if not await guard.before_read():
                continue

            read_timeout_s = _env_float("OES_READ_TIMEOUT_S", 2.0)
            try:
                x2, y2 = await asyncio.wait_for(oes._call(oes._acquire_one_slice_avg), timeout=read_timeout_s)
            except asyncio.TimeoutError:
                msg = f"OES read timeout after {read_timeout_s}s (ch={ch}, usb={usb})"
                _errlog(msg)

                # ✅ DLL hang 의심: 이후 DLL cleanup await는 더 위험 → cleanup 스킵하고 파일/NAS 처리 후 자가 종료 루트로
                hard_abort = True
                hard_abort_error = msg
                hard_abort_exit_code = 125
                stopped = True
                stop_reason = "read_timeout"
                break

            # ✅ 읽은 직후 목록 재확인(바뀌었으면 버림) + 연속 실패 복구(리셋/재초기화)
            try:
                keep = await guard.after_read(x2 is not None and y2 is not None)
            except _ResetCallTimeout as e:
                msg = str(e)
                _errlog(msg)
                hard_abort = True
                hard_abort_error = msg
                hard_abort_exit_code = 125
                stopped = True
                stop_reason = "reset_timeout"
                break
            if not keep:
                continue

            if x2 is None or y2 is None:
                continue

            y2_list = y2.tolist() if hasattr(y2, "tolist") else list(y2)
            if len(y2_list) != len(x_list):
                continue

            now_s = datetime.now().strftime("%H:%M:%S")
            w.writerow([now_s] + [float(v) for v in y2_list])
            rows += 1
            f.flush()

            # ── OES Anomaly: Ar 파장 감시 (처음/마지막 N행 제외) ──
            if _ar_idx is not None:
                try:
                    ar_val = float(y2_list[_ar_idx])
                    _ar_row_num += 1
                    if _ar_row_num <= _ANOMALY_SKIP_FIRST:
                        pass  # 처음 N행 스킵
                    elif time.time() > _ar_cutoff:
                        pass  # 마지막 N초 구간 스킵
                    else:
                        if _ar_count == 0:
                            _ar_sum = ar_val
                            _ar_count = 1
                        else:
                            ar_mean = _ar_sum / _ar_count
                            if ar_mean != 0 and abs(ar_val - ar_mean) / abs(ar_mean) >= _ANOMALY_THRESHOLD:
                                pct = ((ar_val - ar_mean) / ar_mean) * 100
                                direction = "급등" if pct > 0 else "급락"
                                alert_msg = (
                                    f"⚠️ OES 이상 감지 (CH{ch})\n"
                                    f"Ar {_AR_WAVELENGTH_NM:.1f}nm {direction}: "
                                    f"{ar_val:.1f} (평균 {ar_mean:.1f}, {pct:+.1f}%)\n"
                                    f"측정 행: {rows}"
                                )
                                _status(f"[worker] ANOMALY: {alert_msg}")
                                await asyncio.to_thread(_post_anomaly_webhook, alert_msg)
                            else:
                                _ar_sum += ar_val
                                _ar_count += 1
                except Exception:
                    pass

        elapsed = time.time() - t0

        with contextlib.suppress(Exception):
            if f:
                f.flush()
                os.fsync(f.fileno())
                f.close()
                f = None

        cleanup_timeout_s = _env_float("OES_CLEANUP_TIMEOUT_S", 5.0)

        if oes:
            # ✅ read_timeout으로 hard_abort가 된 경우엔 cleanup 자체를 시도하지 않는 게 더 안전
            if not hard_abort:
                try:
                    await asyncio.wait_for(oes.cleanup(), timeout=cleanup_timeout_s)
                except asyncio.TimeoutError:
                    msg = f"OES cleanup timeout after {cleanup_timeout_s}s (ch={ch}, usb={usb})"
                    _errlog(msg)
                    hard_abort = True
                    hard_abort_error = hard_abort_error or msg
                    hard_abort_exit_code = hard_abort_exit_code or 126
                    stop_reason = stop_reason or "cleanup_timeout"
                except Exception:
                    pass

            oes = None

        # ✅ 장비/DLL 정리 끝났으면 USB mutex는 즉시 해제 (NAS 복사는 mutex 없이)
        _release_mutex_once()

        nas_ok, nas_csv, nas_error, local_deleted = await _copy_csv_to_nas(out_csv, int(ch))

        ok_final = (not hard_abort)

        payload = {
            "kind": "finished",
            "ok": bool(ok_final),
            "stopped": bool(stopped),
            "stop_reason": stop_reason,
            "ch": int(ch),
            "usb": int(usb),
            "out_csv": str(out_csv),
            "nas_ok": bool(nas_ok),
            "nas_csv": str(nas_csv) if nas_csv else None,
            "nas_error": nas_error,
            "local_deleted": bool(local_deleted),
            "rows": int(rows),
            "elapsed_s": float(elapsed),
        }
        payload.update(guard.payload())

        if not ok_final and hard_abort_error:
            payload["error"] = hard_abort_error

        _print_json(payload)

        # ✅ hard_abort면 DLL 스레드가 걸렸을 수 있으니, stop 파일/뮤텍스 정리 후 워커 자가 종료로 끝낸다
        if hard_abort and hard_abort_exit_code:
            # stop 파일은 finally를 기대하면 안 됨(os._exit) → 여기서 직접 정리
            for p in (stop_flag_usb, stop_flag_csv, stop_flag_usb_global):
                if p is not None:
                    with contextlib.suppress(Exception):
                        p.unlink()
            _release_mutex_once()
            os._exit(int(hard_abort_exit_code))

        return 0 if ok_final else 10

    except Exception as e:
        elapsed = time.time() - t0

        # ✅ 1) 실패여도 파일/버퍼를 최대한 정리해서 'size mismatch' 확률 줄이기
        with contextlib.suppress(Exception):
            if f:
                f.flush()
                os.fsync(f.fileno())
                f.close()
                f = None

        # ✅ 2) DLL/채널 정리(실패했더라도 다음 런에 영향 최소화)
        cleanup_timeout_s = _env_float("OES_CLEANUP_TIMEOUT_S", 5.0)

        if oes:
            try:
                await asyncio.wait_for(oes.cleanup(), timeout=cleanup_timeout_s)
            except asyncio.TimeoutError:
                # 예외 경로에서 cleanup hang이면 워커가 계속 떠있을 수 있음 → 자가 종료 쪽으로
                msg = f"OES cleanup timeout after {cleanup_timeout_s}s (exception path ch={ch} usb={usb})"
                _errlog(msg)
                # stop 파일/뮤텍스 정리 후 종료(마찬가지로 finally 기대 X)
                for p in (stop_flag_usb, stop_flag_csv, stop_flag_usb_global):
                    if p is not None:
                        with contextlib.suppress(Exception):
                            p.unlink()
                _release_mutex_once()
                os._exit(126)
            except Exception:
                pass
            finally:
                oes = None

        # ✅ 3) 그 다음 mutex 해제(다음 측정 막지 않기)
        _release_mutex_once()

        # ✅ 4) 로컬 CSV가 있으면 NAS 복사 시도
        nas_ok = False
        nas_csv = None
        nas_error = None
        local_deleted = False
        try:
            if out_csv and Path(out_csv).exists():
                nas_ok, nas_csv, nas_error, local_deleted = await _copy_csv_to_nas(Path(out_csv), int(ch))
        except Exception as _e:
            nas_ok = False
            nas_error = f"nas copy exception: {type(_e).__name__}: {_e}"

        _print_json({
            "kind": "finished",
            "ok": False,
            "stopped": bool(stopped),
            "stop_reason": stop_reason,
            "ch": int(ch),
            "usb": int(usb),
            "out_csv": str(out_csv) if out_csv else None,
            "nas_ok": bool(nas_ok),
            "nas_csv": str(nas_csv) if nas_csv else None,
            "nas_error": nas_error,
            "local_deleted": bool(local_deleted),
            "rows": int(rows),
            "elapsed_s": float(elapsed),
            "error": f"{type(e).__name__}: {e}",
            "trace": traceback.format_exc(),
        })
        return 10

    finally:
        if stop_flag_usb is not None:
            with contextlib.suppress(Exception):
                stop_flag_usb.unlink()
        if stop_flag_csv is not None:
            with contextlib.suppress(Exception):
                stop_flag_csv.unlink()
        if stop_flag_usb_global is not None:
            with contextlib.suppress(Exception):
                stop_flag_usb_global.unlink()

        # ✅ 앞에서 이미 해제했을 수도 있으므로 1회 해제로 통일
        _release_mutex_once()


# ====== Daemon mode (persistent OES) ======
# - 워커 프로세스를 1회만 띄워 OES 채널을 열린 상태로 유지
# - stdin(JSONL)로 명령을 받아 측정만 수행 (프로그램 종료 시에만 cleanup)
#
# stdin JSONL 예시:
#   {"cmd":"ping"}
#   {"cmd":"measure","duration_s":10,"integration_ms":50,"sample_interval_s":1.0,"avg_count":3,"out_dir":"C:/.../OES/CH1"}
#   {"cmd":"reset"}
#   {"cmd":"close"}
#
# stdout JSON은 기존과 동일하게 kind=status/started/finished 등을 사용한다.

async def _stdin_readline_async() -> Optional[str]:
    loop = asyncio.get_running_loop()
    # Windows에서도 가장 안정적인 방식: blocking readline을 executor로 보냄
    return await loop.run_in_executor(None, sys.stdin.readline)

async def _daemon_read_cmd() -> Optional[dict]:
    """stdin에서 JSON 한 줄을 읽어 dict로 반환. EOF면 None."""
    try:
        line = await _stdin_readline_async()
    except Exception as e:
        _errlog_exc(f"[daemon] stdin readline failed: {type(e).__name__}: {e}")
        return None

    if not line:
        return None  # EOF

    s = (line or "").strip()
    if not s:
        return {}

    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {"cmd": "_invalid", "raw": s[:500]}
    except Exception as e:
        _print_json({"kind": "status", "message": f"[daemon] invalid json: {type(e).__name__}: {e} raw={s[:200]}"})
        return {}

async def _daemon_reset_device(oes: OESAsync, *, ch: int, usb: int) -> bool:
    """cleanup + initialize_device 재시도. (daemon 내부 복구용)"""
    cleanup_timeout_s = _env_float("OES_CLEANUP_TIMEOUT_S", 5.0)
    init_timeout_s = _env_float("OES_INIT_TIMEOUT_S", 25.0)

    # cleanup
    try:
        await asyncio.wait_for(oes.cleanup(), timeout=cleanup_timeout_s)
    except asyncio.TimeoutError:
        _errlog(f"[daemon] reset cleanup timeout after {cleanup_timeout_s}s (ch={ch} usb={usb})")
        return False
    except Exception:
        pass

    # re-init (새로 객체를 만들지 않고 같은 객체 재사용)
    try:
        ok = await asyncio.wait_for(oes.initialize_device(), timeout=init_timeout_s)
        return bool(ok)
    except asyncio.TimeoutError:
        _errlog(f"[daemon] reset init timeout after {init_timeout_s}s (ch={ch} usb={usb})")
        return False
    except Exception as e:
        _errlog_exc(f"[daemon] reset init exception: {type(e).__name__}: {e} (ch={ch} usb={usb})")
        return False

async def _daemon_measure_once(
    *,
    oes: OESAsync,
    ch: int,
    usb: int,
    duration_s: float,
    integration_ms: int,
    sample_interval_s: float,
    avg_count: int,
    out_dir: Optional[Path],
    out_csv: Optional[Path],
) -> int:
    """daemon에서 1회 측정. (OES 채널은 열어둔 채 CSV만 생성/복사)"""
    t0 = time.time()
    rows = 0
    f = None

    # ✅ (1) out_dir/out_csv 확정
    if out_csv:
        out_csv_final = Path(out_csv).expanduser().resolve()
        out_dir_final = out_csv_final.parent
    else:
        out_dir_final = Path(out_dir).expanduser().resolve() if out_dir else _default_out_dir(int(ch))
        out_csv_final = out_dir_final / _make_default_filename()

    out_dir_final.mkdir(parents=True, exist_ok=True)

    # ✅ (2) stop flag 경로 확정
    stop_flag_usb_local = out_dir_final / f".stop_usb{int(usb)}.flag"
    stop_flag_csv = Path(str(out_csv_final) + ".stop")
    stop_flag_usb_global = _stop_dir() / f".stop_usb{int(usb)}.flag"

    # ✅ (3) 이전 잔재 제거(스테일 stop 방지)
    # - global stop은 '실시간 STOP 신호'로도 쓰이므로 여기서 지우지 않는다.
    for p in (stop_flag_usb_local, stop_flag_csv):
        with contextlib.suppress(Exception):
            p.unlink()

    stopped = False
    stop_reason = None

    # ✅ (4) 장비 설정 적용(측정마다 integration/avg를 반영)
    try:
        oes._avg_count = int(max(1, int(avg_count)))  # type: ignore[attr-defined]
    except Exception:
        pass

    # ✅ 분광기 연결 감시/읽기 실패 복구: 시작 전 목록 확인(다르면 복귀 대기 → 재초기화, 안 되면 예외)
    #    목록 확인을 먼저 해서, 기준과 다를 때 설정 명령이 다른 챔버 분광기로 나가지 않게 한다
    guard = _LinkGuard(oes, ch=int(ch), usb=int(usb), integration_ms=int(integration_ms))
    await guard.ensure_at_start()

    with contextlib.suppress(Exception):
        await oes._call(oes._apply_device_settings_blocking, int(oes.sChannel), int(integration_ms))

    # ✅ (5) CSV open + header + 첫 프레임
    _print_json({"kind": "status", "message": f"[daemon] open csv: {out_csv_final}"})
    f = open(str(out_csv_final), "w", newline="", encoding="utf-8")
    w = csv.writer(f)

    x, y = await _acquire_first_frame(oes, guard=guard)
    x_list = x.tolist() if hasattr(x, "tolist") else list(x)
    y_list = y.tolist() if hasattr(y, "tolist") else list(y)

    w.writerow(["Time"] + [float(v) for v in x_list])
    f.flush()

    _print_json({
        "kind": "started",
        "ok": True,
        "ch": int(ch),
        "usb": int(usb),
        "resolved_usb": int(getattr(oes, "sChannel", -1)),
        "out_csv": str(out_csv_final),
        "cols": int(len(x_list)),
        "model": str(getattr(oes, "_model_name", "UNKNOWN")),
        "sample_interval_s": float(sample_interval_s),
        "avg_count": int(avg_count),
        "integration_ms": int(integration_ms),
    })

    now_s = datetime.now().strftime("%H:%M:%S")
    w.writerow([now_s] + [float(v) for v in y_list])
    rows += 1
    f.flush()

    # ── OES Anomaly: Ar 감시 초기화 (처음/마지막 N행 제외) ──
    _ar_idx = _find_ar_index(x_list) if _ANOMALY_ENABLED else None
    _ar_sum = 0.0
    _ar_count = 0
    _ar_row_num = 0

    hard_abort = False
    hard_abort_error = None
    hard_abort_exit_code = 0

    deadline = time.time() + max(0.0, float(duration_s))
    _ar_cutoff = deadline - (_ANOMALY_SKIP_LAST * float(sample_interval_s))
    while time.time() < deadline:
        # ✅ stop 요청 감지(USB 기반 / CSV 기반)
        if stop_flag_usb_global.exists() or stop_flag_usb_local.exists() or stop_flag_csv.exists():
            stopped = True
            stop_reason = "stop_flag"
            _status(
                f"[daemon] stop requested (usb_global={stop_flag_usb_global.exists()} usb_local={stop_flag_usb_local.exists()} csv_flag={stop_flag_csv.exists()})"
            )
            break

        await asyncio.sleep(float(sample_interval_s))

        # sleep 직후 한번 더(반응성)
        if stop_flag_usb_global.exists() or stop_flag_usb_local.exists() or stop_flag_csv.exists():
            stopped = True
            stop_reason = "stop_flag"
            _status(
                f"[daemon] stop requested (usb_global={stop_flag_usb_global.exists()} usb_local={stop_flag_usb_local.exists()} csv_flag={stop_flag_csv.exists()})"
            )
            break

        # ✅ 연결 목록이 기준과 다르면(또는 끊김 중이면) DLL 호출 없이 이번 샘플 건너뜀
        if not await guard.before_read():
            continue

        read_timeout_s = _env_float("OES_READ_TIMEOUT_S", 2.0)
        try:
            x2, y2 = await asyncio.wait_for(oes._call(oes._acquire_one_slice_avg), timeout=read_timeout_s)
        except asyncio.TimeoutError:
            msg = f"OES read timeout after {read_timeout_s}s (daemon ch={ch} usb={usb})"
            _errlog(msg)
            hard_abort = True
            hard_abort_error = msg
            hard_abort_exit_code = 125
            stopped = True
            stop_reason = "read_timeout"
            break

        # ✅ 읽은 직후 목록 재확인(바뀌었으면 버림) + 연속 실패 복구(리셋/재초기화)
        try:
            keep = await guard.after_read(x2 is not None and y2 is not None)
        except _ResetCallTimeout as e:
            msg = str(e)
            _errlog(msg)
            hard_abort = True
            hard_abort_error = msg
            hard_abort_exit_code = 125
            stopped = True
            stop_reason = "reset_timeout"
            break
        if not keep:
            continue

        if x2 is None or y2 is None:
            continue

        y2_list = y2.tolist() if hasattr(y2, "tolist") else list(y2)
        if len(y2_list) != len(x_list):
            continue

        now_s = datetime.now().strftime("%H:%M:%S")
        w.writerow([now_s] + [float(v) for v in y2_list])
        rows += 1
        f.flush()

        # ── OES Anomaly: Ar 파장 감시 (처음/마지막 N행 제외) ──
        if _ar_idx is not None:
            try:
                ar_val = float(y2_list[_ar_idx])
                _ar_row_num += 1
                if _ar_row_num <= _ANOMALY_SKIP_FIRST:
                    pass  # 처음 N행 스킵
                elif time.time() > _ar_cutoff:
                    pass  # 마지막 N초 구간 스킵
                else:
                    if _ar_count == 0:
                        _ar_sum = ar_val
                        _ar_count = 1
                    else:
                        ar_mean = _ar_sum / _ar_count
                        if ar_mean != 0 and abs(ar_val - ar_mean) / abs(ar_mean) >= _ANOMALY_THRESHOLD:
                            pct = ((ar_val - ar_mean) / ar_mean) * 100
                            direction = "급등" if pct > 0 else "급락"
                            alert_msg = (
                                f"⚠️ OES 이상 감지 (CH{ch})\n"
                                f"Ar {_AR_WAVELENGTH_NM:.1f}nm {direction}: "
                                f"{ar_val:.1f} (평균 {ar_mean:.1f}, {pct:+.1f}%)\n"
                                f"측정 행: {rows}"
                            )
                            _status(f"[daemon] ANOMALY: {alert_msg}")
                            await asyncio.to_thread(_post_anomaly_webhook, alert_msg)
                        else:
                            _ar_sum += ar_val
                            _ar_count += 1
            except Exception:
                pass

    elapsed = time.time() - t0

    # ✅ 파일 flush/close
    with contextlib.suppress(Exception):
        if f:
            f.flush()
            os.fsync(f.fileno())
            f.close()
            f = None

    # ✅ NAS 복사 (daemon은 USB mutex를 계속 잡고 있으므로, 여기서 mutex release는 하지 않음)
    nas_ok, nas_csv, nas_error, local_deleted = await _copy_csv_to_nas(out_csv_final, int(ch))

    ok_final = (not hard_abort)

    payload = {
        "kind": "finished",
        "ok": bool(ok_final),
        "stopped": bool(stopped),
        "stop_reason": stop_reason,
        "ch": int(ch),
        "usb": int(usb),
        "out_csv": str(out_csv_final),
        "nas_ok": bool(nas_ok),
        "nas_csv": str(nas_csv) if nas_csv else None,
        "nas_error": nas_error,
        "local_deleted": bool(local_deleted),
        "rows": int(rows),
        "elapsed_s": float(elapsed),
    }
    payload.update(guard.payload())
    if not ok_final and hard_abort_error:
        payload["error"] = hard_abort_error

    _print_json(payload)

    # ✅ hard_abort면 DLL hang 의심이므로 daemon 자체를 자가 종료시키는 게 안전 (메인이 재기동/폴백)
    if hard_abort and hard_abort_exit_code:
        for p in (stop_flag_usb_local, stop_flag_csv, stop_flag_usb_global):
            with contextlib.suppress(Exception):
                p.unlink()
        os._exit(int(hard_abort_exit_code))

    # stop flag 정리
    for p in (stop_flag_usb_local, stop_flag_csv, stop_flag_usb_global):
        with contextlib.suppress(Exception):
            p.unlink()

    return 0 if ok_final else 10

async def cmd_daemon(ch: int, usb: int, dll_path: Optional[str], out_dir: Optional[Path]) -> int:
    """OES를 1회 initialize 후 살아있는 상태로 유지하는 daemon."""
    mtx = _WinMutex(f"Local\\VanaM_OES_USB{int(usb)}")
    mutex_ms = _mutex_timeout_ms()

    _status(f"[worker] daemon: acquiring mutex name={mtx.name} timeout_ms={mutex_ms}")
    acquired = mtx.acquire(timeout_ms=mutex_ms)
    if not acquired:
        _errlog(f"cmd=daemon mutex timeout ch={ch} usb={usb} timeout_ms={mutex_ms}")
        _print_json({"kind": "daemon", "ok": False, "ch": int(ch), "usb": int(usb), "error": f"mutex timeout ({mutex_ms}ms)"})
        return 4

    oes = None
    try:
        # ✅ daemon 기본 출력 디렉토리(측정 명령에서 out_dir/out_csv 없을 때 사용)
        base_dir = Path(out_dir).expanduser().resolve() if out_dir else _default_out_dir(int(ch))
        base_dir.mkdir(parents=True, exist_ok=True)

        oes = OESAsync(
            chamber=int(ch),
            usb_index=int(usb),
            dll_path=dll_path,
            save_directory=str(base_dir),
            sample_interval_s=1.0,
            avg_count=int(OES_AVG_COUNT),
            debug_print=False,
        )

        init_timeout_s = _env_float("OES_INIT_TIMEOUT_S", 25.0)
        _status(f"[worker] daemon init begin ch={ch} usb={usb} dir={base_dir} dll_path={dll_path}")
        try:
            ok = await asyncio.wait_for(oes.initialize_device(), timeout=init_timeout_s)
        except asyncio.TimeoutError:
            msg = f"OES init timeout after {init_timeout_s}s (cmd=daemon ch={ch} usb={usb})"
            _errlog(msg)
            _print_json({"kind": "daemon", "ok": False, "ch": int(ch), "usb": int(usb), "error": msg})
            os._exit(124)

        payload = {
            "kind": "daemon",
            "ok": bool(ok),
            "ch": int(ch),
            "usb": int(usb),
            "resolved_usb": int(getattr(oes, "sChannel", -1)),
            "pixels": int(getattr(oes, "_npix", 0) or 0),
            "model": str(getattr(oes, "_model_name", "UNKNOWN")),
            "dll_resolved": str(getattr(oes, "_dll_path", "")),
            "dll_exists": bool(Path(getattr(oes, "_dll_path", "")).is_file()),
            "out_dir": str(base_dir),
            "serial": str(getattr(oes, "_serial", "") or ""),
        }
        if not ok:
            payload["error"] = str(getattr(oes, "_last_error", "")) or "initialize_device failed"
            payload["scan"] = getattr(oes, "_last_scan", {}) or {}

        _print_json(payload)

        if not ok or getattr(oes, "sChannel", -1) < 0:
            # ✅ init 실패 시 executor 스레드가 DLL 호출에 갇혀 있을 수 있음.
            #    정상 return이면 인터프리터 종료가 비데몬 스레드 join에서 무한 대기
            #    → 워커 좀비화 → 디바이스 핸들 점유 → 이후 모든 시도가 같은 지점에서 hang.
            #    measure / total-timeout 경로(os._exit)와 동일하게 하드 종료로 대칭화.
            with contextlib.suppress(Exception):
                mtx.release()
            os._exit(2)

        _status(f"[worker] daemon READY ch={ch} usb={usb} resolved={getattr(oes,'sChannel',-1)} pixels={getattr(oes,'_npix',0)} serial={getattr(oes,'_serial','') or '?'}")

        # ✅ daemon READY 직후: 이전 실행에서 남은 global stop 잔재만 1회 정리
        with contextlib.suppress(Exception):
            (_stop_dir() / f".stop_usb{int(usb)}.flag").unlink()

        # ✅ 측정 동시 실행 방지
        measure_lock = asyncio.Lock()

        while True:
            cmd_obj = await _daemon_read_cmd()
            if cmd_obj is None:
                _status("[daemon] stdin closed (EOF) → exit")
                break

            cmd = str(cmd_obj.get("cmd") or "").strip().lower()
            if not cmd:
                continue

            if cmd in {"ping", "health"}:
                _print_json({"kind": "pong", "ok": True, "ch": int(ch), "usb": int(usb), "ts": time.time()})
                continue

            if cmd in {"close", "exit", "quit"}:
                _status("[daemon] close requested → cleanup & exit")
                break

            if cmd == "reset":
                _status("[daemon] reset requested")
                ok_reset = await _daemon_reset_device(oes, ch=int(ch), usb=int(usb))
                _print_json({"kind": "reset", "ok": bool(ok_reset), "ch": int(ch), "usb": int(usb)})
                continue

            if cmd == "measure":
                if measure_lock.locked():
                    _print_json({"kind": "finished", "ok": False, "ch": int(ch), "usb": int(usb), "error": "busy"})
                    continue

                # cmd 파라미터
                dur = float(cmd_obj.get("duration_s") or cmd_obj.get("duration") or 0.0)
                integ = int(cmd_obj.get("integration_ms") or 0)
                if integ <= 0:
                    integ = int(os.environ.get("OES_DEFAULT_INTEGRATION_MS", "50"))  # 기본 50ms
                si = float(cmd_obj.get("sample_interval_s") or 1.0)
                ac = int(cmd_obj.get("avg_count") or OES_AVG_COUNT)

                od = cmd_obj.get("out_dir")
                oc = cmd_obj.get("out_csv")

                od_p = Path(str(od)) if od else base_dir
                oc_p = Path(str(oc)) if oc else None

                async with measure_lock:
                    try:
                        rc = await _daemon_measure_once(
                            oes=oes,
                            ch=int(ch),
                            usb=int(usb),
                            duration_s=dur,
                            integration_ms=integ,
                            sample_interval_s=si,
                            avg_count=ac,
                            out_dir=od_p,
                            out_csv=oc_p,
                        )
                        # 측정 실패(비-fatal)면 다음 측정을 위해 자동 reset 시도
                        if rc != 0:
                            with contextlib.suppress(Exception):
                                ok_reset = await _daemon_reset_device(oes, ch=int(ch), usb=int(usb))
                                _status(f"[daemon] auto-reset after measure rc={rc} ok={ok_reset}")
                    except Exception as e:
                        _errlog_exc(f"[daemon] measure exception: {type(e).__name__}: {e}")
                        _print_json({"kind": "finished", "ok": False, "ch": int(ch), "usb": int(usb), "error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc()})
                        with contextlib.suppress(Exception):
                            ok_reset = await _daemon_reset_device(oes, ch=int(ch), usb=int(usb))
                            _status(f"[daemon] auto-reset after exception ok={ok_reset}")
                continue

            _print_json({"kind": "status", "message": f"[daemon] unknown cmd={cmd} obj={cmd_obj}"})

        return 0

    except Exception as e:
        _errlog_exc(f"cmd=daemon fatal exception ch={ch} usb={usb}")
        _print_json({"kind": "fatal", "ok": False, "ch": int(ch), "usb": int(usb), "error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc()})
        return 99

    finally:
        # ✅ daemon 종료 시에만 cleanup
        if oes is not None:
            cleanup_timeout_s = _env_float("OES_CLEANUP_TIMEOUT_S", 5.0)
            try:
                await asyncio.wait_for(oes.cleanup(), timeout=cleanup_timeout_s)
            except asyncio.TimeoutError:
                _errlog(f"[daemon] cleanup timeout after {cleanup_timeout_s}s (exit ch={ch} usb={usb})")
            except Exception:
                pass

        with contextlib.suppress(Exception):
            mtx.release()    


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--cmd", required=True, choices=["init", "measure", "daemon"])
    p.add_argument("--ch", required=True, type=int)
    p.add_argument("--usb", required=True, type=int)
    p.add_argument("--dll_path", type=str, default=None)

    p.add_argument("--duration", dest="duration_s", type=float, default=0.0)
    p.add_argument("--integration_ms", type=int, default=0)
    p.add_argument("--sample_interval_s", type=float, default=1.0)
    p.add_argument("--avg_count", type=int, default=3)
    p.add_argument("--out_dir", type=str, default=None)
    p.add_argument("--out_csv", type=str, default=None)
    return p


async def _amain(argv=None) -> int:
    args = build_parser().parse_args(argv)
    _load_anomaly_config()
    _status(f"[worker] START argv={sys.argv} frozen={getattr(sys,'frozen',False)} base={_worker_base_dir()} anomaly={_ANOMALY_ENABLED}")

    if args.cmd == "init":
        out_dir = Path(args.out_dir) if args.out_dir else None
        out_csv = Path(args.out_csv) if args.out_csv else None
        return await cmd_init(args.ch, args.usb, args.dll_path, out_dir, out_csv)

    if args.cmd == "daemon":
        out_dir = Path(args.out_dir) if args.out_dir else None
        return await cmd_daemon(args.ch, args.usb, args.dll_path, out_dir)

    out_dir = Path(args.out_dir) if args.out_dir else None
    out_csv = Path(args.out_csv) if args.out_csv else None

    return await cmd_measure(
        ch=int(args.ch),
        usb=int(args.usb),
        duration_s=float(args.duration_s),
        integration_ms=int(args.integration_ms),
        sample_interval_s=float(args.sample_interval_s),
        avg_count=int(args.avg_count),
        out_dir=out_dir,
        out_csv=out_csv,
        dll_path=args.dll_path,
    )


def main(argv=None) -> int:
    try:
        return asyncio.run(_amain(argv))
    except KeyboardInterrupt:
        _print_json({"kind": "finished", "ok": False, "error": "KeyboardInterrupt"})
        return 130
    except Exception as e:
        _errlog_exc("fatal exception in main()")
        _print_json({"kind": "fatal", "ok": False, "error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc()})
        return 99


if __name__ == "__main__":
    # ✅ 워커는 절대 '정상 인터프리터 종료'를 하지 않는다.
    #    DLL 호출에 갇힌 executor 스레드가 있으면 join에서 영구 대기(좀비)하므로,
    #    모든 finally/뮤텍스 해제가 끝난 뒤(asyncio.run 반환 후) 하드 종료로 프로세스를 확실히 회수한다.
    #    _print_json은 매 호출마다 flush하므로 부모가 읽는 JSON 유실 없음.
    _rc = main()
    with contextlib.suppress(Exception):
        sys.stdout.flush()
        sys.stderr.flush()
    os._exit(int(_rc))

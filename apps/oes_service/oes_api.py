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
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import ctypes
from concurrent.futures import ThreadPoolExecutor

import numpy as np


# NAS 저장 경로 (고정)
_NAS_OES_ROOT = Path(r"\\VanaM_NAS\VanaM_Sputter\OES")
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


def _add_dll_search_dir(dll_path: str) -> None:
    if os.name != "nt":
        return
    try:
        os.add_dll_directory(str(Path(dll_path).resolve().parent))
    except Exception:
        pass


_STDOUT_BROKEN = False

def _print_json(obj) -> None:
    global _STDOUT_BROKEN
    s = json.dumps(obj, ensure_ascii=False) + "\n"

    if _STDOUT_BROKEN:
        return

    try:
        sys.stdout.write(s)
        sys.stdout.flush()
    except (BrokenPipeError, OSError):
        # stdout이 깨졌으면 워커가 여기서 죽지 않게 막는다
        _STDOUT_BROKEN = True


# ================= 오류 로거 =================
import logging
from threading import Lock

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

        self._chamber = int(chamber)
        self._usb_index = int(usb_index) if usb_index is not None else (0 if self._chamber == 1 else 1)

        self._roi_start = ROI_START_DEFAULT
        self._roi_end   = ROI_END_DEFAULT

        self._set_baseline = None
        self._auto_dark = None
        self._set_trg = None
        self._set_tec = None
        self._set_dbl_int = None
        self._get_wl = None

        # __init__에 멤버 추가
        self._last_scan_code: int = 0
        self._last_scan_msg: str = ""
        self._last_error: str = ""
        self._detected_channels: int = 0

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

        self._set_baseline = getattr(L, "spSetBaseLineCorrection", None)
        if self._set_baseline:
            with contextlib.suppress(Exception):
                self._set_baseline.argtypes = [ctypes.c_int16]
                self._set_baseline.restype  = ctypes.c_int16

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

    def _scan_and_open(self) -> Tuple[int, str]:
        try:
            _add_dll_search_dir(self._dll_path)
            self.sp_dll = ctypes.WinDLL(self._dll_path)
            self._bind_functions()

            n = int(self.sp_dll.spTestAllChannels(ctypes.c_int16(0)))
            self._detected_channels = int(n)

            if n <= 0:
                return -1, f"spTestAllChannels failed (n={n})"

            ch = int(self._usb_index)
            if ch < 0 or ch >= n:
                return -2, f"target USB{ch} out of range (detected={n})"

            rr = int(self.sp_dll.spSetupGivenChannel(ctypes.c_int16(ch)))
            if rr < 0:
                return -3, f"target USB{ch} open failed (rr={rr})"

            self.sChannel = ch
            return 0, f"open ok: USB{ch}"
        except Exception as e:
            return -9, f"scan/open exception: {type(e).__name__}: {e}"
        
    def _read_pixels(self, ch: int, npix: int) -> Tuple[int, Optional[np.ndarray]]:
        assert self.sp_dll is not None
        buf = (ctypes.c_int32 * npix)()
        r = self.sp_dll.spReadDataEx(buf, ctypes.c_int16(ch))  # type: ignore
        if r < 0:
            return int(r), None
        arr = np.frombuffer(buf, dtype=np.int32, count=npix).astype(float)
        return int(r), arr

    def _try_fetch_wl(self, ch: int, npix: int) -> Optional[np.ndarray]:
        if not self._get_wl or self.sp_dll is None:
            return None
        try:
            wl_buf = (ctypes.c_double * npix)()
            r = self._get_wl(wl_buf, ctypes.c_int16(ch))
            if r < 0:
                return None
            wl = np.frombuffer(wl_buf, dtype=np.float64, count=npix).astype(float)
            if np.all(np.isfinite(wl)) and (wl.max() > wl.min()):
                return wl
        except Exception:
            return None
        return None

    def _ensure_npixels(self, ch: int) -> int:
        for npix in (2048, 1024, 512, 4096):
            try:
                r, arr = self._read_pixels(ch, npix)
                if r >= 0 and arr is not None and arr.size == npix:
                    return int(npix)
            except Exception:
                continue
        raise RuntimeError("cannot determine pixel count")

    def _apply_device_settings_blocking(self, ch: int, integration_ms: int) -> None:
        if self._set_baseline:
            with contextlib.suppress(Exception):
                self._set_baseline(ctypes.c_int16(ch))
        if self._auto_dark:
            with contextlib.suppress(Exception):
                self._auto_dark(ctypes.c_int16(ch))
        if self._set_trg:
            with contextlib.suppress(Exception):
                self._set_trg(ctypes.c_int16(11), ctypes.c_int16(ch))
        if self._set_tec:
            with contextlib.suppress(Exception):
                self._set_tec(ctypes.c_int32(1), ctypes.c_int16(ch))
        if self._set_dbl_int and integration_ms > 0:
            with contextlib.suppress(Exception):
                self._set_dbl_int(ctypes.c_double(float(integration_ms)), ctypes.c_int16(ch))

    async def initialize_device(self) -> bool:
        r, msg = await self._call(self._scan_and_open)
        self._last_scan_code = int(r)
        self._last_scan_msg = str(msg)

        if r < 0 or self.sChannel < 0 or self.sp_dll is None:
            self._last_error = self._last_scan_msg
            return False

        try:
            self._npix = await self._call(self._ensure_npixels, int(self.sChannel))
        except Exception as e:
            self._last_error = f"ensure_npixels failed: {type(e).__name__}: {e}"
            return False

        self._last_error = ""
        return True

    def _acquire_one_slice_avg(self):
        if self.sp_dll is None or self.sChannel < 0 or self._npix <= 0:
            raise RuntimeError("device not ready")

        ch = int(self.sChannel)
        npix = int(self._npix)

        with contextlib.suppress(Exception):
            self.sp_dll.spSetupGivenChannel(ctypes.c_int16(ch))  # type: ignore

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
        with contextlib.suppress(Exception):
            await self._call(self._safe_close_channel_blocking, ch)

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


async def _acquire_first_frame(oes: OESAsync, retries: int = 20, delay_s: float = 0.2):
    last_err = None
    for _ in range(max(1, retries)):
        try:
            x, y = await oes._call(oes._acquire_one_slice_avg)
            if x is not None and y is not None:
                return x, y
        except Exception as e:
            last_err = e
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
        ok = await oes.initialize_device()

        _print_json({
            "kind": "init",
            "ok": bool(ok),
            "ch": int(ch),
            "usb": int(usb),
            "resolved_usb": int(getattr(oes, "sChannel", -1)),
            "model": str(getattr(oes, "_model_name", "UNKNOWN")),
            "pixels": int(getattr(oes, "_npix", 0)),
            "dll_resolved": dll_resolved,
            "dll_exists": bool(dll_exists),
            
            # ✅ 추가
            "detected_channels": int(getattr(oes, "_detected_channels", 0)),
            "scan_code": int(getattr(oes, "_last_scan_code", 0)),
            "scan_msg": str(getattr(oes, "_last_scan_msg", "")),
            "error": str(getattr(oes, "_last_error", "")),
        })

        with contextlib.suppress(Exception):
            await oes.cleanup()

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
        # ✅ acquired 실패든 성공이든 핸들 정리
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

    # ✅ out_dir_final / out_csv 확정(네가 작성한 로직 유지)
    if out_csv:
        out_csv = Path(out_csv).expanduser().resolve()
        out_dir_final = out_csv.parent
    elif out_dir:
        out_dir_final = Path(out_dir).expanduser().resolve()
        out_csv = out_dir_final / _make_default_filename()
    else:
        out_dir_final = _default_out_dir(ch).resolve()
        out_csv = out_dir_final / _make_default_filename()

    try:
        if not acquired:
            _errlog(f"cmd=measure mutex timeout ch={ch} usb={usb} timeout_ms={mutex_ms}")
            _print_json({"kind":"finished","ok":False,"ch":int(ch),"usb":int(usb),"error":f"mutex timeout ({mutex_ms}ms)"})
            return 4

        out_dir_final.mkdir(parents=True, exist_ok=True)

        _print_json({"kind": "status", "message": f"[worker] measure begin ch={ch} usb={usb} out_csv={out_csv} dir={out_dir_final}"})

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
        ok = await oes.initialize_device()
        _print_json({"kind": "status", "message": f"[worker] init done ok={ok} resolved_usb={getattr(oes,'sChannel',-1)} pixels={getattr(oes,'_npix',0)}"})

        if not ok or getattr(oes, "sChannel", -1) < 0:
            raise RuntimeError("OES initialize_device() failed")

        with contextlib.suppress(Exception):
            await oes._call(oes._apply_device_settings_blocking, int(oes.sChannel), int(integration_ms))

        _print_json({"kind": "status", "message": f"[worker] open csv: {out_csv}"})
        f = open(str(out_csv), "w", newline="", encoding="utf-8")
        w = csv.writer(f)

        x, y = await _acquire_first_frame(oes)
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
            "sample_interval_s": float(sample_interval_s),
            "avg_count": int(avg_count),
            "integration_ms": int(integration_ms),
        })

        now_s = datetime.now().strftime("%H:%M:%S")
        w.writerow([now_s] + [float(v) for v in y_list])
        rows += 1
        f.flush()

        deadline = time.time() + max(0.0, float(duration_s))
        while time.time() < deadline:
            await asyncio.sleep(float(sample_interval_s))

            x2, y2 = await oes._call(oes._acquire_one_slice_avg)
            if x2 is None or y2 is None:
                continue

            y2_list = y2.tolist() if hasattr(y2, "tolist") else list(y2)
            if len(y2_list) != len(x_list):
                continue

            now_s = datetime.now().strftime("%H:%M:%S")
            w.writerow([now_s] + [float(v) for v in y2_list])
            rows += 1
            f.flush()

        elapsed = time.time() - t0

        with contextlib.suppress(Exception):
            if f:
                f.flush()
                os.fsync(f.fileno())
                f.close()
                f = None

        with contextlib.suppress(Exception):
            if oes:
                await oes.cleanup()
                oes = None

        nas_ok, nas_csv, nas_error, local_deleted = await _copy_csv_to_nas(Path(out_csv), int(ch))

        _print_json({
            "kind": "finished",
            "ok": True,
            "ch": int(ch),
            "usb": int(usb),
            "out_csv": str(out_csv),
            "nas_ok": bool(nas_ok),
            "nas_csv": str(nas_csv) if nas_csv else None,
            "nas_error": nas_error,
            "local_deleted": bool(local_deleted),
            "rows": int(rows),
            "elapsed_s": float(elapsed),
        })
        return 0

    except Exception as e:
        _errlog_exc(
            f"cmd=measure exception ch={ch} usb={usb} out_csv={out_csv} "
            f"duration_s={duration_s} integration_ms={integration_ms} sample_interval_s={sample_interval_s} avg_count={avg_count}"
        )
        elapsed = time.time() - t0
        _print_json({
            "kind": "finished",
            "ok": False,
            "ch": int(ch),
            "usb": int(usb),
            "out_csv": str(out_csv) if out_csv else None,
            "rows": int(rows),
            "elapsed_s": float(elapsed),
            "error": f"{type(e).__name__}: {e}",
            "trace": traceback.format_exc(),
        })
        return 10

    finally:
        with contextlib.suppress(Exception):
            if f:
                f.close()
        with contextlib.suppress(Exception):
            if oes:
                await oes.cleanup()
        mtx.release()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--cmd", required=True, choices=["init", "measure"])
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
    _status(f"[worker] START argv={sys.argv} frozen={getattr(sys,'frozen',False)} base={_worker_base_dir()}")

    if args.cmd == "init":
        out_dir = Path(args.out_dir) if args.out_dir else None
        out_csv = Path(args.out_csv) if args.out_csv else None
        return await cmd_init(args.ch, args.usb, args.dll_path, out_dir, out_csv)

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
    raise SystemExit(main())

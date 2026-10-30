r"""OES 워커 공통 — 출력(JSONL)·로그·환경값·뮤텍스·경로·연결 목록 조회·Ar 이상 감지 설정.

v1 apps/oes_service/oes_api.py 1-460 줄(모듈 앞부분)을 옮긴 것. 장비·OS 호출(cfgmgr32·kernel32 뮤텍스)은 그대로다.
v2 에서 바뀐 것(메인과의 연결부만):
- _print_json: 줄마다 v2 봉투 {"v": 2, "id": <지금 처리 중인 명령 id>} 를 더한다(v1 필드는 그대로). id 는 set_command_id 로.
- _worker_base_dir: 메인이 --home 으로 준 폴더가 있으면 그것(log\·stop\·oes_config.json 위치). 없으면 v1 과 같음
  (frozen=exe 폴더, 소스 실행=이 파일 폴더). 메인은 v1 배치(DLL 이 있는 apps\oes_service)를 넘긴다 — v1 과 같은 곳에 남는다.
- load_dll: 보통은 ctypes.WinDLL(v1 그대로). --fake 면 가짜 DLL(oes_fake.FakeDll) — 시험용.
- 연결 목록 조회(_list_cyusb_interfaces)도 가짜가 갈아 끼울 수 있다(LINK_LIST_HOOK).
"""
from __future__ import annotations

import contextlib
import ctypes
import json
import logging
import os
import ssl
import sys
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Optional

import numpy as np

# ── v2 연결부 상태 ──
_HOME_DIR: Optional[Path] = None          # --home (없으면 v1 규칙)
_CUR_ID: Any = None                       # 지금 처리 중인 메인 명령 id(응답 줄마다 실음)
DLL_FACTORY: Optional[Callable[[str], Any]] = None    # --fake 일 때 가짜 DLL 생성기
LINK_LIST_HOOK: Optional[Callable[[], Optional[list]]] = None


def set_home(path: Optional[str]) -> None:
    global _HOME_DIR
    _HOME_DIR = Path(path).expanduser().resolve() if path else None


def set_command_id(cid: Any) -> None:
    global _CUR_ID
    _CUR_ID = cid


def load_dll(path: str):
    """DLL 로드 — v1 은 ctypes.WinDLL(self._dll_path) 를 바로 불렀다(oes_api.py:784). --fake 면 가짜."""
    if DLL_FACTORY is not None:
        return DLL_FACTORY(path)
    return ctypes.WinDLL(path)


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
    """worker 폴더의 oes_config.json을 읽어서 전역 설정 반영."""
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


def _report_anomaly(msg: str) -> None:
    """Ar 이상 감지 알림(결정 D28) — 워커가 웹훅으로 직접 보내지 않고 메인으로 JSONL {"kind": "anomaly", "text"} 을 올린다
    (메인 oes_link 가 내부 이벤트 source '<ch>.oes' kind 'anomaly' 로 → 포털이 v1 과 같은 문구의 카드를 보낸다). 중복 억제 없음."""
    _print_json({"kind": "anomaly", "text": msg})


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


def _mutex_timeout_ms() -> int:
    # 기본 5초: 공정 멈추지 않게 빨리 실패시키기
    return int(os.environ.get("OES_MUTEX_TIMEOUT_MS", "5000"))


async def _copy_csv_to_nas(local_csv: Path, ch: int, *, timeout_s: float = 120.0):
    """NAS(G:) 복사는 하지 않는다 — 로컬 CSV 를 그대로 둔다. 반환 형식만 예전과 같다.
    return: (nas_ok=False, nas_csv=None, nas_error=None, local_deleted=False)"""
    return False, None, None, False


# ✅ 워커는 메인/프로젝트 설정에 의존하지 않도록 고정값 사용
OES_AVG_COUNT = 3
DEBUG_PRINT = False

# ── 분광기 연결 감시 / 읽기 실패 복구 (v1 oes_api.py:135-160) ──
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
    """지금 연결된 CyUSB 분광기 인터페이스 경로 목록(cfgmgr32 조회, USB 통신 없음). Windows 가 아니거나 조회 실패면 None."""
    global _CM_FNS
    if LINK_LIST_HOOK is not None:
        return LINK_LIST_HOOK()
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
    """JSONL 출력(부모 프로세스가 stdout pipe로 파싱). stdout 이 깨져도 워커는 죽지 않는다(v1 oes_api.py:228-262).
    v2: 줄마다 {"v": 2, "id": 지금 명령 id} 를 덧붙인다(v1 필드 이름·값은 그대로)."""
    global _STDOUT_BROKEN
    if isinstance(obj, dict):
        obj = {**obj, "v": 2, "id": obj.get("id", _CUR_ID)}
    s = json.dumps(obj, ensure_ascii=False)

    try:
        with _JSON_LOCK:
            if _STDOUT_BROKEN:
                raise OSError(22, "stdout already marked broken")

            payload = (s + "\n").encode("utf-8", errors="backslashreplace")
            if getattr(sys.stdout, "buffer", None):
                sys.stdout.buffer.write(payload)
                sys.stdout.buffer.flush()
            else:
                sys.stdout.write(s + "\n")
                sys.stdout.flush()

    except Exception as e:
        _STDOUT_BROKEN = True
        with contextlib.suppress(Exception):
            _errlog_exc(f"_print_json failed: {type(e).__name__}: {e} kind={obj.get('kind') if isinstance(obj, dict) else None}")
        with contextlib.suppress(Exception):
            sys.stderr.write(s + "\n")
            sys.stderr.flush()


# ================= 오류 로거 =================
_ERR_LOGGER = None
_ERR_LOCK = Lock()


def _worker_base_dir() -> Path:
    """워커 기준 폴더: --home(v2) → PyInstaller exe(frozen) 폴더 → 이 파일 폴더."""
    if _HOME_DIR is not None:
        return _HOME_DIR
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
        fh.setFormatter(logging.Formatter("%(asctime)s [pid=%(process)d] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
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
        fh.setFormatter(logging.Formatter("%(asctime)s [pid=%(process)d] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
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


# ===== 크로스-프로세스 뮤텍스(USB 채널 단위) — v1 oes_api.py:375-419 그대로 =====
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


# ====== 경로·환경값 (v1 oes_api.py:1115-1140) ======
def _default_out_dir(ch: int) -> Path:
    # 메인이 out_dir/out_csv 를 넘기지 않았을 때만 쓴다. 메인 기본값(OES_LOCAL_DIR)과 같은 곳
    base = Path(os.environ.get("OES_LOCAL_BASE", r"C:\VanaM_Logs\OES"))
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

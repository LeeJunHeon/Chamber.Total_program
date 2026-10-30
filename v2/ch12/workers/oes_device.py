r"""OES 분광기 DLL 핵심(워커 안) — v1 apps/oes_service/oes_api.py:423-1109 의 OESAsync 를 옮긴 것.

장비 호출 순서·인자는 v1 그대로다(바꾸지 않는다):
  초기화 load_bind → spTestAllChannels(ORDER_USB=0) → 연결 목록 기준 → spDevInfo(사전) → spSetupGivenChannel(usb)
         → 모델 probe spInitGivenChannel PDA(2)→G9212(3)→SONY(0)→TOSHIBA(1)→S10420(4) → 픽셀(spReadDataEx)·파장표(spGetWLTable)
         → (사전 실패 때) spDevInfo → 일련번호·연결 목록 재확인
  측정마다 spSetTrgEx(11, ch)·spSetTEC(1, ch)·spSetDblIntEx(integration_ms, ch) — spAutoDark 는 보내지 않는다(주석 처리 그대로)
  샘플 = spReadDataEx 를 avg_count 번 읽어 평균, ROI [10, 1034)
  정리 spCloseGivenChannel(목록이 기준과 다르면 생략), OES_DLL_UNLOAD=1 일 때만 FreeLibrary
v2 에서 바뀐 것: DLL 로드가 oes_common.load_dll(보통은 ctypes.WinDLL 그대로, --fake 면 가짜) 을 거친다.
뺀 것: _scan_and_open(옛 한 번에 열기) — v1 에서도 호출부가 없는 죽은 코드(전수 목록 APP-046 na).
"""
from __future__ import annotations

import asyncio
import contextlib
import ctypes
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Tuple

import numpy as np

from v2.ch12.workers.oes_common import (
    DEBUG_PRINT, OES_AVG_COUNT, _DEV_INFO_BUF, _add_dll_search_dir, _env_float, _errlog, _errlog_exc,
    _expected_serial_for, _list_cyusb_interfaces, _runlog, _status, _worker_base_dir, load_dll,
)

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


    def _load_and_bind_blocking(self) -> None:
        _runlog(f"[scan] load dll begin path={self._dll_path}")
        _add_dll_search_dir(self._dll_path)
        self.sp_dll = load_dll(self._dll_path)   # v2: --fake 면 가짜 DLL(oes_common.load_dll)
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



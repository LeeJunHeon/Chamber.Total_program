# device/oes.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import asyncio, ctypes, csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator, Literal, Optional, List, Union
from concurrent.futures import ThreadPoolExecutor

import numpy as np
from lib.config_ch2 import OES_AVG_COUNT, DEBUG_PRINT

EventKind = Literal["status", "data", "finished"]

@dataclass
class OESEvent:
    kind: EventKind
    message: Optional[str] = None
    x: Optional[List[float]] = None
    y: Optional[List[float]] = None
    success: Optional[bool] = None

# ─────────────────────────────────────────────────────
# 모델 정의 (SM303 계열)
SP_CCD_SONY    = 0
SP_CCD_TOSHIBA = 1
SP_CCD_PDA     = 2   # SM303-Si (Hamamatsu S7031)
SP_CCD_G9212   = 3   # SM303-InGaAs
SP_CCD_S10420  = 4

# 모델별 기본 픽셀 길이 (WL 실패 시 폴백)
PIXELS = {
    SP_CCD_PDA:     1056,
    SP_CCD_G9212:    512,
    SP_CCD_SONY:    2080,
    SP_CCD_S10420:  2080,
    SP_CCD_TOSHIBA: 3680,
}

# WL 실패 시 1회 읽기로 픽셀 길이를 유추할 때 쓸 후보
CANDIDATE_PIXELS = [1056, 1024, 512, 2048, 4096, 3680, 2080]

ORDER_USB = 0            # spTestAllChannels 인자
ROI_START_DEFAULT = 10   # 이전 코드와 동일
ROI_END_DEFAULT   = 1034 # 이전 코드와 동일

# ─────────────────────────────────────────────────────
class OESAsync:
    """
    - CH→USB 매핑: 기본 CH1→USB0, CH2→USB1 (usb_index 지정 시 우선)
    - 초기화: USB 스캔→open 확인→모델 자동판정(spInitGivenChannel(model,ch))→WL 테이블 읽기
    - 측정: 이전 버전과 동일한 설정 적용(Baseline/AutoDark/Trigger=11/TEC ON/Integration ms)
    - x축: WL(nm) 사용. WL 실패 시 자동으로 픽셀 인덱스로 대체
    - ROI: [10:1034]을 장비 픽셀/테이블 길이에 맞춰 안전 클램프
    """

    def __init__(
        self,
        *,
        dll_path: str = r"\\VanaM_NAS\VanaM_Sputter\OES\SDKs\DLL\x64\stdcall\SPdbUSBm.dll",
        save_directory: str = r"\\VanaM_NAS\VanaM_Sputter\OES\CH2",
        sample_interval_s: float = 1.0,
        avg_count: int = OES_AVG_COUNT,
        debug_print: bool = DEBUG_PRINT,
        chamber: int = 2,                 # 1→USB 0, 2→USB 1
        usb_index: Optional[int] = None,  # 지정 시 우선
    ):
        self._dll_path = dll_path

        # 저장 경로 CH{n} 정규화 (기존 로직 유지)
        p = Path(save_directory)
        if p.name.upper() in {"CH1", "CH2"}:
            p = p.parent / f"CH{int(chamber)}"
        else:
            p = p / f"CH{int(chamber)}"
        self._save_dir = p; self._save_dir.mkdir(parents=True, exist_ok=True)

        self._sample_interval_s = float(sample_interval_s)
        self._avg_count = int(max(1, avg_count))
        self._debug = debug_print

        # DLL/장치 상태
        self.sp_dll: Optional[ctypes.WinDLL] = None
        self.sChannel: int = -1
        self._npix: int = 0
        self._wl: Optional[np.ndarray] = None  # nm축 (성공 시)
        self._model_name: str = "UNKNOWN"

        # 실행 상태
        self.is_running = False
        self._stopping = False
        self._acq_task: Optional[asyncio.Task] = None
        self._deadline_task: Optional[asyncio.Task] = None

        # 데이터 저장
        self.measured_rows: list[list[Union[str, float]]] = []
        self._start_time_str: str = ""

        # 이벤트 큐
        self._ev_q: asyncio.Queue[OESEvent] = asyncio.Queue(maxsize=256)

        # DLL 호출 전용 워커(단일 스레드)
        self._exec = ThreadPoolExecutor(max_workers=1, thread_name_prefix="OESWorker")

        # CH→USB 매핑 확정
        self._chamber = int(chamber)
        self._usb_index = int(usb_index) if usb_index is not None else (0 if self._chamber == 1 else 1)

        # ROI(장비 픽셀 수에 맞춰 런타임 클램프)
        self._roi_start = ROI_START_DEFAULT
        self._roi_end   = ROI_END_DEFAULT

    # ========== 공용 헬퍼 ==========
    async def _call(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._exec, lambda: func(*args, **kwargs))

    def _bind_functions(self):
        assert self.sp_dll is not None
        L = self.sp_dll

        # 콘솔과 동일: "값 인자" 시그니처로 고정
        # short spTestAllChannels(short sOrderType)
        L.spTestAllChannels.argtypes = [ctypes.c_int16]
        L.spTestAllChannels.restype  = ctypes.c_int16

        # short spSetupGivenChannel(short sChannel)
        L.spSetupGivenChannel.argtypes = [ctypes.c_int16]
        L.spSetupGivenChannel.restype  = ctypes.c_int16

        # short spInitGivenChannel(short sCCDType, short sChannel)
        L.spInitGivenChannel.argtypes = [ctypes.c_int16, ctypes.c_int16]
        L.spInitGivenChannel.restype  = ctypes.c_int16

        # short spReadDataEx(long* pArray, short sChannel)
        L.spReadDataEx.argtypes = [ctypes.POINTER(ctypes.c_int32), ctypes.c_int16]
        L.spReadDataEx.restype  = ctypes.c_int16

        # short spCloseGivenChannel(short sChannel)
        L.spCloseGivenChannel.argtypes = [ctypes.c_int16]
        L.spCloseGivenChannel.restype  = ctypes.c_int16

        # (이전 버전과 동일) 장비 설정용 함수
        self._set_baseline = getattr(L, "spSetBaseLineCorrection", None)
        if self._set_baseline:
            try:
                self._set_baseline.argtypes = [ctypes.c_int16]
                self._set_baseline.restype  = ctypes.c_int16
            except Exception:
                self._set_baseline = None

        self._auto_dark = getattr(L, "spAutoDark", None)
        if self._auto_dark:
            try:
                self._auto_dark.argtypes = [ctypes.c_int16]
                self._auto_dark.restype  = ctypes.c_int16
            except Exception:
                self._auto_dark = None

        self._set_trg = getattr(L, "spSetTrgEx", None)   # (mode, ch)
        if self._set_trg:
            try:
                self._set_trg.argtypes = [ctypes.c_int16, ctypes.c_int16]
                self._set_trg.restype  = ctypes.c_int16
            except Exception:
                self._set_trg = None

        self._set_tec = getattr(L, "spSetTEC", None)     # (on/off, ch)
        if self._set_tec:
            try:
                self._set_tec.argtypes = [ctypes.c_int32, ctypes.c_int16]
                self._set_tec.restype  = ctypes.c_int16
            except Exception:
                self._set_tec = None

        self._set_dbl_int = getattr(L, "spSetDblIntEx", None)  # (ms, ch)
        if self._set_dbl_int:
            try:
                self._set_dbl_int.argtypes = [ctypes.c_double, ctypes.c_int16]
                self._set_dbl_int.restype  = ctypes.c_int16
            except Exception:
                self._set_dbl_int = None

        # WL 테이블(이전 방식과 동일, 성공 시 nm축 사용)
        self._get_wl = getattr(L, "spGetWLTable", None)
        if self._get_wl:
            try:
                self._get_wl.argtypes = [ctypes.POINTER(ctypes.c_double), ctypes.c_int16]
                self._get_wl.restype  = ctypes.c_int16
            except Exception:
                self._get_wl = None

    # ========== 워커 스레드용 블로킹 ==========
    def _scan_and_open(self) -> tuple[int, list[int]]:
        """USB 기준 스캔 후 open 가능한 ch 목록을 반환"""
        try:
            n = int(self.sp_dll.spTestAllChannels(ctypes.c_int16(ORDER_USB)))  # type: ignore
        except Exception:
            n = 0
        opened: list[int] = []
        for ch in range(max(0, n)):
            try:
                if int(self.sp_dll.spSetupGivenChannel(ctypes.c_int16(ch))) >= 0:  # type: ignore
                    opened.append(ch)
            except Exception:
                pass
        return n, opened

    def _read_pixels(self, ch: int, npix: int) -> tuple[int, Optional[np.ndarray]]:
        buf = (ctypes.c_int32 * npix)()
        r = int(self.sp_dll.spReadDataEx(buf, ctypes.c_int16(ch)))  # type: ignore
        if r < 0:
            return r, None
        return r, np.asarray(buf[:npix], dtype=float)

    # after  ← r >= 0 이면 성공으로 보고, 길이는 'length_hint'로 고정
    def _try_fetch_wl(self, ch: int, length_hint: int) -> Optional[np.ndarray]:
        if not self._get_wl:
            return None
        n = max(1, int(length_hint))
        buf = (ctypes.c_double * n)()
        try:
            r = int(self._get_wl(buf, ctypes.c_int16(ch)))  # type: ignore
        except Exception:
            return None
        if r < 0:  # 음수면 실패
            return None
        return np.asarray(buf, dtype=float)

    def _pick_model_with_probe(self, ch: int) -> int | None:
        """PDA → G9212 → SONY → TOSHIBA → S10420 순서로 init 시도"""
        for m in (SP_CCD_PDA, SP_CCD_G9212, SP_CCD_SONY, SP_CCD_TOSHIBA, SP_CCD_S10420):
            try:
                r = int(self.sp_dll.spInitGivenChannel(ctypes.c_int16(m), ctypes.c_int16(ch)))  # type: ignore
            except Exception:
                r = -1
            if r >= 0:
                self._model_name = {SP_CCD_PDA:"PDA", SP_CCD_G9212:"G9212",
                                    SP_CCD_SONY:"SONY", SP_CCD_TOSHIBA:"TOSHIBA",
                                    SP_CCD_S10420:"S10420"}[m]
                return m
        return None

    def _ensure_npixels(self, ch: int, model: int) -> int:
        # 1) 실제 픽셀 길이부터 확정
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

        # 2) 그 길이에 맞춰 WL 재획득(성공 시 nm축 사용)
        wl = self._try_fetch_wl(ch, npix)
        if wl is not None and wl.size >= min(npix, ROI_END_DEFAULT):
            self._wl = wl[:npix]
        else:
            self._wl = None
        return npix

    def _apply_device_settings_blocking(self, ch: int, integration_time_ms: int):
        """이전 버전과 동일한 설정: Baseline/AutoDark/Trigger=11/TEC ON/Integration"""
        ch16 = ctypes.c_int16(ch)
        if self._set_baseline:
            try: self._set_baseline(ch16)  # type: ignore
            except Exception: pass
        if self._auto_dark:
            try: self._auto_dark(ch16)     # type: ignore
            except Exception: pass
        if self._set_trg:
            try: self._set_trg(ctypes.c_int16(11), ch16)  # type: ignore
            except Exception: pass
        if self._set_tec:
            try: self._set_tec(ctypes.c_int32(1), ch16)   # type: ignore
            except Exception: pass
        if self._set_dbl_int:
            try: self._set_dbl_int(ctypes.c_double(float(integration_time_ms)), ch16)  # type: ignore
            except Exception: pass

    # ========== 초기화(워커) ==========
    def _init_in_worker(self):
        # DLL 로드: 호출 규약 일치(WinDLL)
        self.sp_dll = ctypes.WinDLL(self._dll_path)
        self._bind_functions()

        # ① USB 스캔 & open 목록 확보
        n, opened = self._scan_and_open()
        target_ch = int(self._usb_index) if self._usb_index is not None else (0 if self._chamber == 1 else 1)
        if target_ch < 0:
            target_ch = 0 if self._chamber == 1 else 1

        if n <= 0 or target_ch >= n:
            return False, f"장치 스캔 실패 또는 대상 USB{target_ch} 미존재 (감지 {n}대, OPEN={opened})"
        if target_ch not in opened:
            return False, f"대상 USB{target_ch} 오픈 실패 (OPEN={opened})"

        # ② 모델 자동판정 (정상 반환되는 첫 모델 사용)
        model = self._pick_model_with_probe(target_ch)
        if model is None:
            return False, f"초기화 실패: USB{target_ch} — 모든 모델 시도 실패"

        # ③ 픽셀 수 확정 + WL(nm) 확보(가능 시)
        self._npix = self._ensure_npixels(target_ch, model)

        # ④ ROI 클램프 (이전 코드와 동일 [10:1034]을 길이에 맞춤)
        self._roi_end   = min(ROI_END_DEFAULT, self._npix if self._wl is None else int(min(self._npix, self._wl.size)))
        self._roi_start = min(ROI_START_DEFAULT, max(0, self._roi_end - 1))

        # ⑤ 상태 반영
        self.sChannel = target_ch
        pix_info = f"pixels={self._npix}" + (", wl=nm" if self._wl is not None else ", wl=pixel")
        return True, f"초기화 성공: CH{self._chamber}→USB{self.sChannel}, model={self._model_name}, {pix_info}"

    # ========== 외부 API ==========
    async def initialize_device(self) -> bool:
        await self._safe_close_channel()
        try:
            ok, msg = await self._call(self._init_in_worker)
            await self._status(msg)
            return bool(ok)
        except Exception as e:
            await self._status(f"초기화 중 예외: {e}")
            await self._safe_close_channel()
            return False

    async def run_measurement(self, duration_sec: float, integration_time_ms: int):
        if self.is_running:
            await self._status("[경고] 측정 시작 불가: 이미 실행 중. 공정은 계속 진행됩니다.")
            return

        if self.sChannel < 0:
            ok = await self.initialize_device()
            if not ok or self.sChannel < 0:
                await self._status("[경고] 측정 시작 불가: OES 초기화 실패")
                return

        # 이전 버전과 동일 설정 적용
        try:
            await self._call(self._apply_device_settings_blocking, self.sChannel, integration_time_ms)
        except Exception:
            pass  # 실패해도 측정은 진행

        self.is_running = True
        self._stopping = False
        self.measured_rows = []
        self._start_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        await self._status(f"{duration_sec/60:.1f}분 동안 측정을 시작합니다. (Ch{self._chamber} / USB{self.sChannel})")

        self._acq_task = asyncio.create_task(self._acquisition_loop(), name=f"OES_Acquire_CH{self._chamber}")
        self._deadline_task = asyncio.create_task(self._deadline_after(duration_sec), name=f"OES_Deadline_CH{self._chamber}")

    async def stop_measurement(self):
        await self._end_measurement(True)

    async def cleanup(self):
        if self.is_running:
            await self._status("중단 요청 수신됨")
            await self._end_measurement(False, "사용자 중단")
        else:
            await self._safe_close_channel()
            await self._status("중단 요청 수신됨 (실행 중 아님)")

    async def events(self) -> AsyncGenerator[OESEvent, None]:
        while True:
            ev = await self._ev_q.get()
            yield ev

    # ========== 내부 루프/종료 ==========
    async def _acquisition_loop(self):
        try:
            while self.is_running:
                try:
                    x, y = await self._call(self._acquire_one_slice_avg)
                except Exception as e:
                    await self._status(f"데이터 수집 중 오류: {e}")
                    await self._end_measurement(False, f"데이터 수집 오류: {e}")
                    return

                if x is not None and y is not None:
                    x_list = np.asarray(x, dtype=float).ravel().tolist()
                    y_list = np.asarray(y, dtype=float).ravel().tolist()

                    current_time = datetime.now().strftime("%H:%M:%S")
                    self.measured_rows.append([current_time] + y_list)
                    self._ev_nowait(OESEvent(kind="data", x=x_list, y=y_list))

                await asyncio.sleep(self._sample_interval_s)
        except asyncio.CancelledError:
            pass

    def _acquire_one_slice_avg(self):
        if self.sp_dll is None or self.sChannel < 0 or self._npix <= 0:
            raise RuntimeError("장치/픽셀 수가 유효하지 않습니다.")

        ch = int(self.sChannel)
        npix = int(self._npix)

        # 일부 DLL은 매번 채널 셀렉트 필요 → 안전하게 한 번 호출
        try: self.sp_dll.spSetupGivenChannel(ctypes.c_int16(ch))  # type: ignore
        except Exception: pass

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

        # x축: WL(nm) 우선, 실패 시 픽셀 인덱스
        if self._wl is not None and self._wl.size >= end:
            x = np.asarray(self._wl[start:end], dtype=float)
        else:
            x = np.arange(start, end, dtype=float)
        y = np.asarray(avg[start:end], dtype=float)
        return x, y

    async def _deadline_after(self, duration_sec: float):
        try:
            await asyncio.sleep(max(0.0, float(duration_sec)))
            if self.is_running:
                await self._end_measurement(True)
        except asyncio.CancelledError:
            pass

    async def _end_measurement(self, was_successful: bool, reason: str = ""):
        if self._stopping: return
        self._stopping = True
        await self._cancel_task("_deadline_task")
        await self._cancel_task("_acq_task")

        # 성공/중단과 무관하게, 한 줄이라도 있으면 저장
        if self.measured_rows:
            try:
                await self._call(self._save_data_to_csv_wide_blocking)
            except Exception as e:
                await self._status(f"[OES_CSV] 저장 실패: {e}")

        await self._safe_close_channel()
        self.is_running = False

        if was_successful:
            await self._status("측정 완료 및 장비 연결 종료")
        else:
            await self._status(f"[경고] 측정 실패({reason}). 공정은 계속 진행됩니다.")
        self._ev_nowait(OESEvent(kind="finished", success=was_successful))
        self._stopping = False

    def _save_data_to_csv_wide_blocking(self):
        if not self.measured_rows:
            return
        # CSV 헤더 x축: WL 성공 시 nm, 실패 시 픽셀 인덱스
        start, end = self._roi_start, self._roi_end
        if self._wl is not None and self._wl.size >= end:
            header_x = self._wl[start:end].tolist()
        else:
            header_x = list(np.arange(start, end, dtype=float))
        header = ["Time"] + header_x
        filename = f"OES_Data_{self._start_time_str}.csv"
        full_path = self._save_dir / filename
        with open(full_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(self.measured_rows)

        self._ev_nowait(OESEvent(kind="status", message=f"[OES_CSV] 저장 완료: {full_path} (rows={len(self.measured_rows)})"))

    def _safe_close_channel_blocking(self):
        try:
            if self.sp_dll and self.sChannel >= 0:
                try: self.sp_dll.spCloseGivenChannel(ctypes.c_int16(self.sChannel))  # type: ignore
                except Exception: pass
        finally:
            self.sChannel = -1

    async def _safe_close_channel(self):
        try:
            await self._call(self._safe_close_channel_blocking)
        except Exception:
            self.sChannel = -1

    # ========== 유틸 ==========
    async def _status(self, msg: str):
        if self._debug:
            print(f"[OES][status] {msg}")
        await self._ev_q.put(OESEvent(kind="status", message=msg))

    def _ev_nowait(self, ev: OESEvent):
        try: self._ev_q.put_nowait(ev)
        except Exception: pass

    async def _cancel_task(self, name: str):
        t: Optional[asyncio.Task] = getattr(self, name)
        if t:
            t.cancel()
            try: await t
            except Exception: pass
            setattr(self, name, None)

    def shutdown_executor(self):
        try: self._exec.shutdown(wait=False, cancel_futures=True)
        except Exception: pass
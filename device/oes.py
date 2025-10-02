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
    x: Optional[List[float]] = None   # ← 변경
    y: Optional[List[float]] = None   # ← 변경
    success: Optional[bool] = None

class OESAsync:
    def __init__(
        self,
        *,
        dll_path: str = r"\\VanaM_NAS\VanaM_Sputter\OES\SDKs\DLL\x64\stdcall\SPdbUSBm.dll",
        save_directory: str = r"\\VanaM_NAS\VanaM_Sputter\OES\CH2",
        sample_interval_s: float = 1.0,
        avg_count: int = OES_AVG_COUNT,
        debug_print: bool = DEBUG_PRINT,
        # === 추가: 챔버/USB 인덱스 매핑 ===
        chamber: int = 2,                 # 1→USB 0, 2→USB 1
        usb_index: Optional[int] = None,  # 지정 시 이 값을 우선 사용
    ):
        self._dll_path = dll_path

        # 저장 경로를 CH{chamber}로 정규화
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
        self.sp_dll: Optional[ctypes.CDLL] = None
        self.sChannel: int = -1
        self.wl_table: Optional[np.ndarray] = None

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

        # 전용 워커 스레드(모든 DLL 호출은 여기서만)
        self._exec = ThreadPoolExecutor(max_workers=1, thread_name_prefix="OESWorker")

        # === 추가: 챔버→USB 인덱스 매핑 확정
        self._chamber = int(chamber)
        self._usb_index = int(usb_index) if usb_index is not None else (0 if self._chamber == 1 else 1)

    # -------- 공용 헬퍼 --------
    async def _call(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._exec, lambda: func(*args, **kwargs))

    def _setup_dll_functions(self):
        assert self.sp_dll is not None
        self.sp_dll.spTestAllChannels.argtypes = [ctypes.POINTER(ctypes.c_int16)]
        self.sp_dll.spTestAllChannels.restype = ctypes.c_int16

        self.sp_dll.spSetupGivenChannel.argtypes = [ctypes.c_int16]
        self.sp_dll.spSetupGivenChannel.restype = ctypes.c_int16

        self.sp_dll.spGetModel.argtypes = [ctypes.c_int16, ctypes.POINTER(ctypes.c_int16)]
        self.sp_dll.spGetModel.restype = ctypes.c_int16

        self.sp_dll.spInitGivenChannel.argtypes = [ctypes.c_int16, ctypes.c_int32]
        self.sp_dll.spInitGivenChannel.restype = ctypes.c_int16

        self.sp_dll.spGetWLTable.argtypes = [ctypes.POINTER(ctypes.c_double), ctypes.c_int16]
        self.sp_dll.spGetWLTable.restype = ctypes.c_int16

        self.sp_dll.spSetBaseLineCorrection.argtypes = [ctypes.c_int16]
        self.sp_dll.spSetBaseLineCorrection.restype = ctypes.c_int16

        self.sp_dll.spAutoDark.argtypes = [ctypes.c_int16]
        self.sp_dll.spAutoDark.restype = ctypes.c_int16

        self.sp_dll.spGetDevIsNew.argtypes = [ctypes.c_int16]
        self.sp_dll.spGetDevIsNew.restype = ctypes.c_int16

        self.sp_dll.spSetTrgEx.argtypes = [ctypes.c_int16, ctypes.c_int16]
        self.sp_dll.spSetTrgEx.restype = ctypes.c_int16

        self.sp_dll.spSetTEC.argtypes = [ctypes.c_int32, ctypes.c_int16]
        self.sp_dll.spSetTEC.restype = ctypes.c_int16

        self.sp_dll.spSetDblIntEx.argtypes = [ctypes.c_double, ctypes.c_int16]
        self.sp_dll.spSetDblIntEx.restype = ctypes.c_int16

        self.sp_dll.spReadDataEx.argtypes = [ctypes.POINTER(ctypes.c_int32), ctypes.c_int16]
        self.sp_dll.spReadDataEx.restype = ctypes.c_int16

        self.sp_dll.spCloseGivenChannel.argtypes = [ctypes.c_uint16]
        self.sp_dll.spCloseGivenChannel.restype = ctypes.c_int16

    # -------- 워커 스레드에서만 돌려야 하는 블로킹 함수들 --------
    def _test_all_channels(self):
        sOrderType = ctypes.c_int16()
        result = self.sp_dll.spTestAllChannels(ctypes.byref(sOrderType))  # type: ignore
        return result, sOrderType.value

    def _get_model(self, sChannel: int):
        model = ctypes.c_int16()
        result = self.sp_dll.spGetModel(sChannel, ctypes.byref(model))  # type: ignore
        return result, model.value

    def _get_wavelength_table(self, channel: int):
        dWLTable = (ctypes.c_double * 3680)()
        result = self.sp_dll.spGetWLTable(dWLTable, channel)  # type: ignore
        return result, np.array(dWLTable)

    def _read_data_ex(self, sChannel: int):
        temp_intensity = (ctypes.c_int32 * 3680)()
        result = self.sp_dll.spReadDataEx(temp_intensity, sChannel)  # type: ignore
        return result, np.array(temp_intensity)

    # --- 초기화 전체를 워커 스레드에서 원샷으로 수행 ---
    def _init_in_worker(self):
        # DLL 로드 ~ 파장테이블 획득까지 한 스레드에서
        self.sp_dll = ctypes.CDLL(self._dll_path)
        self._setup_dll_functions()

        # 장치 검색은 참고용(에러 무시 가능) — 채널 선택은 우리가 강제
        try:
            _res, _order = self._test_all_channels()
        except Exception:
            _res, _order = (0, 0)

        # === 핵심: 챔버→USB 인덱스 강제 매핑
        self.sChannel = int(self._usb_index)

        r = self.sp_dll.spSetupGivenChannel(self.sChannel)
        if r < 0:
            self._safe_close_channel_blocking()
            return False, f"채널 설정 실패: USB{self.sChannel} (코드 {r})"

        r_model, model = self._get_model(self.sChannel)
        if r_model < 0:
            self._safe_close_channel_blocking()
            return False, f"모델 조회 실패(USB{self.sChannel}): {r_model}"

        r = self.sp_dll.spInitGivenChannel(self.sChannel, model)
        if r < 0:
            self._safe_close_channel_blocking()
            return False, f"채널 초기화 실패(USB{self.sChannel}): {r}"

        res_wl, wl = self._get_wavelength_table(self.sChannel)
        if res_wl < 0 or wl is None or len(wl) < 1034:
            self._safe_close_channel_blocking()
            return False, f"파장 테이블 로드 실패(USB{self.sChannel})"

        self.wl_table = wl
        return True, f"초기화 성공: Chamber {self._chamber} → USB{self.sChannel}"

    def _apply_device_settings_blocking(self, integration_time_ms: int):
        self.sp_dll.spSetBaseLineCorrection(self.sChannel)    # type: ignore
        self.sp_dll.spAutoDark(self.sChannel)                  # type: ignore
        self.sp_dll.spSetTrgEx(11, self.sChannel)              # type: ignore
        self.sp_dll.spSetTEC(1, self.sChannel)                 # type: ignore
        self.sp_dll.spSetDblIntEx(float(integration_time_ms), self.sChannel)  # type: ignore

    def _acquire_one_slice_avg(self):
        if self.sp_dll is None or self.sChannel < 0:
            raise RuntimeError("장치가 초기화되지 않았습니다.")
        if self.wl_table is None or len(self.wl_table) < 1034:
            raise RuntimeError("파장 테이블이 유효하지 않습니다.")

        intensity_sum = np.zeros(3680, dtype=float)
        valid = 0
        for _ in range(self._avg_count):
            res, arr = self._read_data_ex(self.sChannel)
            if res > 0 and arr is not None and arr.size >= 1034:
                intensity_sum += np.asarray(arr, dtype=float)
                valid += 1

        if valid == 0:
            return None, None
        avg = intensity_sum / float(valid)
        x = self.wl_table[10:1034]
        y = avg[10:1034]
        return x, y

    def _save_data_to_csv_wide_blocking(self):
        if not self.measured_rows:
            return
        header = ["Time"] + self.wl_table[10:1034].tolist()  # type: ignore
        filename = f"OES_Data_{self._start_time_str}.csv"
        full_path = self._save_dir / filename
        with open(full_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(self.measured_rows)

    def _safe_close_channel_blocking(self):
        try:
            if self.sp_dll and self.sChannel >= 0:
                try: self.sp_dll.spSetTEC(0, self.sChannel)
                except Exception: pass
                try: self.sp_dll.spCloseGivenChannel(self.sChannel)
                except Exception: pass
        finally:
            self.sChannel = -1

    # ========== 외부 API (async) ==========
    async def initialize_device(self) -> bool:
        await self._safe_close_channel()  # 혹시 열려있으면 정리
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

        self.is_running = True
        self._stopping = False
        self.measured_rows = []
        self._start_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        await self._status(f"{duration_sec/60:.1f}분 동안 측정을 시작합니다. (Ch{self._chamber} / USB{self.sChannel})")

        try:
            await self._call(self._apply_device_settings_blocking, integration_time_ms)
        except Exception as e:
            await self._status(f"장치 설정 실패: {e}")
            await self._end_measurement(False, f"장치 설정 실패: {e}")
            return

        self._acq_task = asyncio.create_task(self._acquisition_loop(), name="OES_Acquire")
        self._deadline_task = asyncio.create_task(self._deadline_after(duration_sec), name="OES_Deadline")

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

    # -------- 내부 루프/종료 --------
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
                    # 안전하게 1D float 리스트로 변환
                    x_list = np.asarray(x, dtype=float).ravel().tolist()
                    y_list = np.asarray(y, dtype=float).ravel().tolist()

                    current_time = datetime.now().strftime("%H:%M:%S")
                    self.measured_rows.append([current_time] + y_list)

                    # ← 여기서 리스트로 넣어주면 소비 측에서 bool 평가해도 문제 없음
                    self._ev_nowait(OESEvent(kind="data", x=x_list, y=y_list))

                await asyncio.sleep(self._sample_interval_s)
        except asyncio.CancelledError:
            pass

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

        if was_successful and self.measured_rows:
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

    async def _safe_close_channel(self):
        try:
            await self._call(self._safe_close_channel_blocking)
        except Exception:
            self.sChannel = -1

    # -------- 유틸 --------
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

    # (선택) 앱 완전 종료 시 호출하면 깔끔
    def shutdown_executor(self):
        try: self._exec.shutdown(wait=False, cancel_futures=True)
        except Exception: pass


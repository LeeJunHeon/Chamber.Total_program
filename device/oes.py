# device/oes_async.py
# -*- coding: utf-8 -*-
"""
oes.py — asyncio 기반 OES 컨트롤러

핵심:
  - Qt 의존 제거(시그널/타이머 없음). asyncio 태스크로 수집/종료 관리
  - DLL 호출은 모두 blocking → asyncio.to_thread 로 루프 비블로킹
  - run_measurement(duration, integration_ms) 동안 1초마다 평균 수집(OES_AVG_COUNT)
  - 종료 시 와이드 포맷 CSV 저장(Time, λ10..λ1033)
  - 상위(UI/브리지)에는 async 제너레이터 events()로 status/data/finished 이벤트 전달

의존: numpy
"""

from __future__ import annotations
import asyncio
import ctypes
import csv
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator, Literal, Optional

import numpy as np
from lib.config_ch2 import OES_AVG_COUNT

# ===== 이벤트 모델 =====
EventKind = Literal["status", "data", "finished"]

@dataclass
class OESEvent:
    kind: EventKind
    message: Optional[str] = None
    # data
    x: Optional[np.ndarray] = None
    y: Optional[np.ndarray] = None
    # finished
    success: Optional[bool] = None


class OESAsync:
    def __init__(
        self,
        *,
        dll_path: str = r"\\VanaM_NAS\VanaM_Sputter\OES\SDKs\DLL\x64\stdcall\SPdbUSBm.dll",
        save_directory: str = r"\\VanaM_NAS\VanaM_Sputter\OES\CH2",
        sample_interval_s: float = 1.0,
        avg_count: int = OES_AVG_COUNT,
        debug_print: bool = True,
    ):
        self._dll_path = dll_path
        self._save_dir = Path(save_directory)
        self._sample_interval_s = float(sample_interval_s)
        self._avg_count = int(max(1, avg_count))
        self._debug = debug_print

        # DLL/장치 상태
        self.sp_dll: Optional[ctypes.CDLL] = None
        self.sChannel: int = -1
        self.wl_table: Optional[np.ndarray] = None

        # 실행 상태
        self.is_running: bool = False
        self._stop_reason: str = ""
        self._acq_task: Optional[asyncio.Task] = None
        self._deadline_task: Optional[asyncio.Task] = None
        self._stopping: bool = False  # 중복 stop 방지

        # 데이터 축적(저장용)
        self.measured_rows: list[list[float]] = []
        self._start_time_str: str = ""

        # 이벤트 큐
        self._ev_q: asyncio.Queue[OESEvent] = asyncio.Queue(maxsize=256)

        # 저장 폴더 준비
        try:
            self._save_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self._ev_nowait(OESEvent(kind="status", message=f"[OES_CSV] 저장 폴더 생성 실패: {e}"))

    # ========== 외부 API ==========
    async def initialize_device(self) -> bool:
        """DLL 로드 + 채널 초기화 + 파장 테이블 획득."""
        # 열린 채널이 있으면 정리
        await self._safe_close_channel()

        try:
            self.sp_dll = ctypes.CDLL(self._dll_path)
            self._setup_dll_functions()

            result, sChannel = await asyncio.to_thread(self._test_all_channels)
            if result < 0:
                await self._status(f"장비 검색 실패, 코드: {result}")
                return False

            self.sChannel = int(sChannel)

            result = await asyncio.to_thread(self.sp_dll.spSetupGivenChannel, self.sChannel)
            if result < 0:
                await self._status(f"채널 설정 실패: {result}")
                await self._safe_close_channel()
                return False

            result_model, model = await asyncio.to_thread(self._get_model, self.sChannel)
            if result_model < 0:
                await self._status(f"모델 조회 실패: {result_model}")
                await self._safe_close_channel()
                return False

            result = await asyncio.to_thread(self.sp_dll.spInitGivenChannel, model, self.sChannel)
            if result < 0:
                await self._status(f"채널 초기화 실패: {result}")
                await self._safe_close_channel()
                return False

            res_wl, wl = await asyncio.to_thread(self._get_wavelength_table, self.sChannel)
            if res_wl < 0 or wl is None or len(wl) < 1034:
                await self._status("파장 테이블 로드 실패.")
                await self._safe_close_channel()
                return False
            self.wl_table = wl

            await self._status("초기화 성공")
            return True
        except Exception as e:
            await self._status(f"초기화 중 예외: {e}")
            await self._safe_close_channel()
            return False

    async def run_measurement(self, duration_sec: float, integration_time_ms: int):
        """측정 시작 → duration 종료까지 주기 수집."""
        if self.is_running or self.sChannel < 0:
            reason = "이미 실행 중" if self.is_running else "초기화 실패"
            await self._status(f"[경고] 측정 시작 불가: {reason}. 공정은 계속 진행됩니다.")
            return

        # 세션 초기화
        self.is_running = True
        self._stopping = False
        self._stop_reason = ""
        self.measured_rows = []
        self._start_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")

        await self._status(f"{duration_sec/60:.1f}분 동안 측정을 시작합니다.")

        # 사전 장치 설정
        try:
            await asyncio.to_thread(self.sp_dll.spSetBaseLineCorrection, self.sChannel)  # type: ignore
            await asyncio.to_thread(self.sp_dll.spAutoDark, self.sChannel)               # type: ignore
            await asyncio.to_thread(self.sp_dll.spSetTrgEx, 11, self.sChannel)          # type: ignore
            await asyncio.to_thread(self.sp_dll.spSetTEC, 1, self.sChannel)             # type: ignore
            await asyncio.to_thread(self.sp_dll.spSetDblIntEx, float(integration_time_ms), self.sChannel)  # type: ignore
        except Exception as e:
            await self._status(f"장치 설정 실패: {e}")
            await self._end_measurement(False, f"장치 설정 실패: {e}")
            return

        # 수집 루프 & 종료 타이머 시작
        self._acq_task = asyncio.create_task(self._acquisition_loop(), name="OES_Acquire")
        self._deadline_task = asyncio.create_task(self._deadline_after(duration_sec), name="OES_Deadline")

    async def stop_measurement(self):
        """성공 종료(사용자 요청)."""
        await self._end_measurement(True)

    async def cleanup(self):
        """중단/종료: 실행 중이면 실패로 종료, 아니어도 열린 채널 정리."""
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

    # ========== 내부 루프 ==========
    async def _acquisition_loop(self):
        try:
            while self.is_running:
                try:
                    x, y = await asyncio.to_thread(self._acquire_one_slice_avg)
                except Exception as e:
                    await self._status(f"데이터 수집 중 오류: {e}")
                    await self._end_measurement(False, f"데이터 수집 오류: {e}")
                    return

                if x is not None and y is not None:
                    # CSV용 행 추가
                    current_time = datetime.now().strftime("%H:%M:%S")
                    self.measured_rows.append([current_time] + y.tolist())

                    # UI 업데이트
                    self._ev_nowait(OESEvent(kind="data", x=x, y=y))

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

    # ========== 종료 공통 처리 ==========
    async def _end_measurement(self, was_successful: bool, reason: str = ""):
        if self._stopping:
            return
        self._stopping = True

        # 태스크 정리
        await self._cancel_task("_deadline_task")
        await self._cancel_task("_acq_task")

        # 성공 시 저장
        if was_successful and self.measured_rows:
            try:
                await asyncio.to_thread(self._save_data_to_csv_wide)
            except Exception as e:
                await self._status(f"[OES_CSV] 저장 실패: {e}")

        # 장치 해제
        await self._safe_close_channel()

        self.is_running = False

        # 알림 (원 코드와 동일하게 실패 시에도 finished만 알림)
        if was_successful:
            await self._status("측정 완료 및 장비 연결 종료")
        else:
            await self._status(f"[경고] 측정 실패({reason}). 공정은 계속 진행됩니다.")
        self._ev_nowait(OESEvent(kind="finished", success=was_successful))

    # ========== 블로킹 I/O 래퍼 ==========
    def _acquire_one_slice_avg(self) -> tuple[Optional[np.ndarray], Optional[np.ndarray]]:
        """OES_AVG_COUNT회 읽어 평균. (blocking; to_thread에서 호출)"""
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

    def _save_data_to_csv_wide(self):
        if not self.measured_rows:
            return
        filename = f"OES_Data_{self._start_time_str}.csv"
        full_path = self._save_dir / filename

        # 헤더: ["Time", λ10..λ1033]
        header = ["Time"] + self.wl_table[10:1034].tolist()  # type: ignore

        with open(full_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(self.measured_rows)

        if self._debug:
            print(f"[OES_CSV] 저장 완료: {full_path}")

    async def _safe_close_channel(self):
        try:
            if self.sp_dll and self.sChannel >= 0:
                try:
                    await asyncio.to_thread(self.sp_dll.spSetTEC, 0, self.sChannel)
                except Exception:
                    pass
                try:
                    await asyncio.to_thread(self.sp_dll.spCloseGivenChannel, self.sChannel)
                except Exception:
                    pass
        finally:
            self.sChannel = -1

    # ========== DLL 시그니처/호출 ==========
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

    # ========== 유틸 ==========
    async def _status(self, msg: str):
        if self._debug:
            print(f"[OES][status] {msg}")
        await self._ev_q.put(OESEvent(kind="status", message=msg))

    def _ev_nowait(self, ev: OESEvent):
        try:
            self._ev_q.put_nowait(ev)
        except Exception:
            pass

    async def _cancel_task(self, name: str):
        t: Optional[asyncio.Task] = getattr(self, name)
        if t:
            t.cancel()
            try:
                await t
            except Exception:
                pass
            setattr(self, name, None)

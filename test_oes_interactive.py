#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OES 단독 대화형(Interactive) 테스트 — device/oes.py 미사용 버전
 - DLL 직접 호출(ctypes) / 시그니처 보정 / 초기화 폴백 포함
 - CH1→USB0, CH2→USB1 기본 매핑(수동 USB 인덱스 지정 가능)
 - Ctrl+C로 중단 가능, 종료 시 TEC Off + 채널 Close

사용:
  > python oes_standalone_interactive.py
"""

import os
import sys
import time
import csv
import math
from pathlib import Path
from typing import Optional, Tuple

import ctypes
from ctypes import (
    c_short, c_int16, c_uint16, c_int32, c_double,
    POINTER, byref
)


# ─────────────────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────────────────

def ask(prompt: str, caster, default):
    s = input(f"{prompt} [{default}]: ").strip()
    if s == "":
        return default
    try:
        return caster(s)
    except Exception:
        print("⚠️  입력이 올바르지 않습니다. 기본값 사용.")
        return default


def load_dll(dll_path: str) -> ctypes.CDLL:
    """
    x64 stdcall DLL이면 WinDLL이 자연스럽고, 일부 빌드/경로에선 CDLL도 동작함.
    우선 WinDLL 시도 후 실패하면 CDLL로 폴백.
    """
    try:
        return ctypes.WinDLL(dll_path)  # stdcall 빌드 경로 기본
    except Exception:
        return ctypes.CDLL(dll_path)    # cdecl 빌드 또는 호환 폴백


# ─────────────────────────────────────────────────────────
# DLL 래핑 (시그니처 보정 포함)
# ─────────────────────────────────────────────────────────

class OESDLL:
    WL_LEN = 3680
    USE_START = 10
    USE_END = 1034  # 파장/데이터 슬라이스 범위 [10:1034)

    def __init__(self, dll_path: str):
        self.dll_path = dll_path
        self.dll = load_dll(dll_path)
        self._bind()

    def _bind(self):
        d = self.dll

        # === 올바른 시그니처 ===
        # short spTestAllChannels(short sOrderType);
        d.spTestAllChannels.argtypes = [c_int16]
        d.spTestAllChannels.restype  = c_int16

        # short spSetupGivenChannel(short sChannel);
        d.spSetupGivenChannel.argtypes = [c_int16]
        d.spSetupGivenChannel.restype  = c_int16

        # short spGetModel(short sChannel, short* model);
        d.spGetModel.argtypes = [c_int16, POINTER(c_int16)]
        d.spGetModel.restype  = c_int16

        # (변종 존재) short spInitGivenChannel(...);
        # → restype만 고정하고, 호출 시 두 형태를 모두 시도
        d.spInitGivenChannel.restype  = c_int16  # argtypes 지정하지 않음(폴리콜)

        # short spGetWLTable(double* dWLTable, short sChannel);
        d.spGetWLTable.argtypes = [POINTER(c_double), c_int16]
        d.spGetWLTable.restype  = c_int16

        # short spSetBaseLineCorrection(short sChannel);
        d.spSetBaseLineCorrection.argtypes = [c_int16]
        d.spSetBaseLineCorrection.restype  = c_int16

        # short spAutoDark(short sChannel);
        d.spAutoDark.argtypes = [c_int16]
        d.spAutoDark.restype  = c_int16

        # short spSetTrgEx(short trig, short sChannel);
        d.spSetTrgEx.argtypes = [c_int16, c_int16]
        d.spSetTrgEx.restype  = c_int16

        # short spSetTEC(int onoff, short sChannel);
        d.spSetTEC.argtypes = [c_int32, c_int16]
        d.spSetTEC.restype  = c_int16

        # short spSetDblIntEx(double ms, short sChannel);
        d.spSetDblIntEx.argtypes = [c_double, c_int16]
        d.spSetDblIntEx.restype  = c_int16

        # short spReadDataEx(int* pInt32, short sChannel);
        d.spReadDataEx.argtypes = [POINTER(c_int32), c_int16]
        d.spReadDataEx.restype  = c_int16

        # short spCloseGivenChannel(unsigned short sChannel);
        d.spCloseGivenChannel.argtypes = [c_uint16]
        d.spCloseGivenChannel.restype  = c_int16

    # ====== DLL 함수 래퍼 ======

    def test_all_channels(self, order_type: int = 0) -> int:
        """0 = USB 포트 순서, 1 = 채널ID 순서"""
        return int(self.dll.spTestAllChannels(c_int16(int(order_type))))

    def setup_channel(self, ch: int) -> int:
        return int(self.dll.spSetupGivenChannel(c_int16(ch)))

    def get_model(self, ch: int) -> Tuple[int, int]:
        model = c_int16()
        rc = int(self.dll.spGetModel(c_int16(ch), byref(model)))
        return rc, int(model.value)

    def init_channel(self, ch: int, model: Optional[int] = None) -> int:
        """
        1차: (채널, 모델)
        2차: (CCD타입, 채널) — 기본 SONY(0)
        """
        rc = -1

        # (1) (채널,모델) 시도
        if model is not None and model >= 0:
            try:
                rc = int(self.dll.spInitGivenChannel(c_int16(ch), c_int32(model)))
            except Exception:
                rc = -1

        # (2) 폴백: (CCD타입, 채널)
        if rc < 0:
            SP_CCD_SONY = 0
            try:
                rc2 = int(self.dll.spInitGivenChannel(c_int16(SP_CCD_SONY), c_int16(ch)))
            except Exception:
                rc2 = -9999
            rc = rc2

        return rc

    def get_wl_table(self, ch: int):
        arr = (c_double * self.WL_LEN)()
        rc = int(self.dll.spGetWLTable(arr, c_int16(ch)))
        return rc, [float(arr[i]) for i in range(self.WL_LEN)]

    def set_baseline(self, ch: int) -> int:
        return int(self.dll.spSetBaseLineCorrection(c_int16(ch)))

    def auto_dark(self, ch: int) -> int:
        return int(self.dll.spAutoDark(c_int16(ch)))

    def set_trigger(self, trig: int, ch: int) -> int:
        return int(self.dll.spSetTrgEx(c_int16(trig), c_int16(ch)))

    def set_tec(self, onoff: int, ch: int) -> int:
        return int(self.dll.spSetTEC(c_int32(onoff), c_int16(ch)))

    def set_integration_ms(self, ms: float, ch: int) -> int:
        return int(self.dll.spSetDblIntEx(c_double(float(ms)), c_int16(ch)))

    def read_data(self, ch: int):
        arr = (c_int32 * self.WL_LEN)()
        rc = int(self.dll.spReadDataEx(arr, c_int16(ch)))
        return rc, [int(arr[i]) for i in range(self.WL_LEN)]

    def close_channel(self, ch: int) -> int:
        return int(self.dll.spCloseGivenChannel(c_uint16(ch)))


# ─────────────────────────────────────────────────────────
# 측정 루틴 (동기)
# ─────────────────────────────────────────────────────────

def run_measurement(
    dll_path: str,
    chamber: int,
    duration_sec: float,
    integ_ms: float,
    usb_index_manual: Optional[int],
    save_root: Optional[str],
    avg_count: int = 1,
) -> int:
    """
    반환 0=성공, 그 외=실패 코드
    """
    print(f"\n[설정] DLL={dll_path}")
    print(f"[설정] CH={chamber} (기본 매핑: CH1→USB0, CH2→USB1)")
    if usb_index_manual is None:
        usb_index = 0 if chamber == 1 else 1
        print(f"[설정] USB index(auto) = {usb_index}")
    else:
        usb_index = int(usb_index_manual)
        print(f"[설정] USB index(manual) = {usb_index}")

    # 저장 경로 정규화
    if save_root and save_root.strip():
        base = Path(save_root.strip())
    else:
        base = Path.cwd()
    save_dir = base / f"CH{chamber}"
    save_dir.mkdir(parents=True, exist_ok=True)

    # DLL 로드
    try:
        oes = OESDLL(dll_path)
    except Exception as e:
        print(f"❌ DLL 로드 실패: {e!r}")
        return 11

    # 채널 스캔(USB 포트 순 고정)
    try:
        num = oes.test_all_channels(order_type=0)
    except Exception as e:
        print(f"⚠️ 채널 스캔 예외: {e!r}")
        num = 0

    if num <= 0:
        print("⚠️ 연결된 장치를 찾지 못했습니다. 케이블/전원/드라이버 확인.")
    else:
        print(f"[INFO] 연결된 장치 수: {num}")

    # 채널 범위 보정
    if num > 0 and not (0 <= usb_index < num):
        print(f"⚠️ 요청한 USB index {usb_index}가 범위를 벗어남 → 0으로 보정")
        usb_index = 0

    # 채널 오픈
    rc = oes.setup_channel(usb_index)
    if rc < 0:
        print(f"❌ spSetupGivenChannel({usb_index}) 실패: rc={rc}")
        return 21

    # 모델 조회
    rc_model, model = oes.get_model(usb_index)
    if rc_model < 0:
        print(f"❌ spGetModel({usb_index}) 실패: rc={rc_model}")
        try:
            oes.close_channel(usb_index)
        except Exception:
            pass
        return 22
    print(f"[INFO] 모델 코드: {model}")

    # 초기화(시그니처 폴백 포함)
    rc = oes.init_channel(usb_index, model=model)
    if rc < 0:
        print(f"❌ spInitGivenChannel 실패: rc={rc}")
        try:
            oes.close_channel(usb_index)
        except Exception:
            pass
        return 23

    # 파장 테이블
    rc_wl, wl = oes.get_wl_table(usb_index)
    if rc_wl < 0 or not wl or len(wl) < OESDLL.USE_END:
        print(f"❌ spGetWLTable 실패 또는 파장 길이 부족: rc={rc_wl}, len={len(wl) if wl else 0}")
        try:
            oes.set_tec(0, usb_index)
            oes.close_channel(usb_index)
        except Exception:
            pass
        return 24

    wl_slice = wl[OESDLL.USE_START:OESDLL.USE_END]

    # 장치 설정
    def _chk(tag, code):
        if code < 0:
            raise RuntimeError(f"{tag} 실패(rc={code})")

    try:
        _chk("SetBaseline",   oes.set_baseline(usb_index))
        _chk("AutoDark",      oes.auto_dark(usb_index))
        _chk("SetTrigger",    oes.set_trigger(11, usb_index))
        _chk("SetTEC(ON)",    oes.set_tec(1, usb_index))
        _chk("SetIntegration",oes.set_integration_ms(integ_ms, usb_index))
    except Exception as e:
        print(f"❌ 장치 설정 실패: {e}")
        try:
            oes.set_tec(0, usb_index)
            oes.close_channel(usb_index)
        except Exception:
            pass
        return 25

    # 측정 루프
    print(f"\n▶ 측정을 시작합니다: {duration_sec:.1f}s, integ={integ_ms:.0f}ms, avg={avg_count}")
    t0 = time.monotonic()
    rows = []
    samples = 0
    try:
        while True:
            if time.monotonic() - t0 >= duration_sec:
                break

            # 평균 N회 취득
            acc = None
            valid = 0
            for _ in range(max(1, int(avg_count))):
                rc_rd, data = oes.read_data(usb_index)
                if rc_rd > 0 and data and len(data) >= OESDLL.USE_END:
                    seg = data[OESDLL.USE_START:OESDLL.USE_END]
                    if acc is None:
                        acc = [float(v) for v in seg]
                    else:
                        for i in range(len(acc)):
                            acc[i] += float(seg[i])
                    valid += 1
                else:
                    # 읽기 실패 시 한 텀 쉼
                    time.sleep(0.05)

            if not valid:
                print("⚠️  데이터 읽기 실패(모두 실패). 계속 시도합니다.")
                time.sleep(0.2)
                continue

            # 평균화
            for i in range(len(acc)):
                acc[i] /= float(valid)

            samples += 1
            # 화면 요약
            y0 = acc[0]
            ymax = max(acc)
            print(f"[DATA] sample={samples} points={len(acc)} y0={y0:.1f} yMax={ymax:.1f}")

            # CSV 누적
            now_str = time.strftime("%H:%M:%S")
            rows.append([now_str] + acc)

            # 샘플 간 간격(roughly integ_ms 기준; 필요 시 조정)
            time.sleep(max(0.0, (float(integ_ms)/1000.0)*0.2))

    except KeyboardInterrupt:
        print("\n⏹  사용자 중단 요청 — 종료를 진행합니다.")
    finally:
        # 안전 종료
        try:
            oes.set_tec(0, usb_index)
        except Exception:
            pass
        try:
            oes.close_channel(usb_index)
        except Exception:
            pass

    # CSV 저장
    if rows:
        ts = time.strftime("%Y%m%d_%H%M%S")
        out = save_dir / f"OES_Data_{ts}.csv"
        try:
            with out.open("w", newline="", encoding="utf-8") as fp:
                w = csv.writer(fp)
                w.writerow(["Time"] + wl_slice)
                w.writerows(rows)
            print(f"✅ 저장 완료: {out}")
        except Exception as e:
            print(f"⚠️ CSV 저장 실패: {e!r}")

    print("✅ 측정 종료")
    return 0


# ─────────────────────────────────────────────────────────
# 엔트리포인트(대화형)
# ─────────────────────────────────────────────────────────

def main():
    if os.name == "nt":
        # Windows 콘솔 UTF-8 (선택)
        try:
            os.system("chcp 65001 >NUL")
        except Exception:
            pass

    print("\n==== OES 단독 테스트 (device/oes.py 미사용) ====\n"
          "엔터를 누르면 대괄호[]의 기본값이 적용됩니다.\n")

    # 기본값
    default_dll = r"\\VanaM_NAS\VanaM_Sputter\OES\SDKs\DLL\x64\stdcall\SPdbUSBm.dll"

    chamber = ask("챔버 번호 (1 또는 2)", int, 1)
    if chamber not in (1, 2):
        print("⚠️  챔버 번호는 1 또는 2만 가능합니다. 1로 대체합니다.")
        chamber = 1

    duration = ask("측정 시간 (초)", float, 10.0)
    integ_ms = ask("적분 시간 (ms)", float, 1000.0)

    dll_in = input(f"DLL 경로 (비우면 기본 사용)\n  기본: {default_dll}\n> ").strip()
    dll_path = dll_in or default_dll

    save_root = input(r"저장 루트 폴더 (예: \\VanaM_NAS\VanaM_Sputter\OES, 비우면 현재폴더)\n> ").strip() or None

    usb_idx_in = input("USB 인덱스 수동 지정 (0 또는 1, 비우면 자동: CH1→0, CH2→1)\n> ").strip()
    if usb_idx_in == "":
        usb_manual = None
    else:
        try:
            val = int(usb_idx_in)
            if val not in (0, 1):
                print("⚠️  USB 인덱스는 0 또는 1만 가능합니다. 자동으로 진행합니다.")
                usb_manual = None
            else:
                usb_manual = val
        except Exception:
            print("⚠️  USB 인덱스 해석 실패. 자동으로 진행합니다.")
            usb_manual = None

    try:
        code = run_measurement(
            dll_path=dll_path,
            chamber=chamber,
            duration_sec=duration,
            integ_ms=integ_ms,
            usb_index_manual=usb_manual,
            save_root=save_root,
            avg_count=1,  # 필요하면 2~4로 올리세요
        )
    except Exception as e:
        print(f"❌ 실행 중 예외: {e!r}")
        code = 99

    sys.exit(code)


if __name__ == "__main__":
    main()

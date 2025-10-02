#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OES 단독 대화형(Interactive) 테스트 — device/oes.py 미사용 · Baseline 옵션 처리판
 - DLL 직접 호출(ctypes)
 - Baseline은 '가능 시'만 수행, 실패(-7 등)는 경고 후 계속
 - 설정 순서: Trg→Int→TEC→AutoDark→(옵션)Baseline
 - CH1→USB0, CH2→USB1 기본 매핑(수동 USB 인덱스 지정 가능)
"""

import os
import sys
import time
import csv
from pathlib import Path
from typing import Optional, Tuple

import ctypes
from ctypes import c_int16, c_uint16, c_int32, c_double, POINTER, byref


# ─────────────────────────────────────────────────────────
# 로딩 유틸
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
    # x64에선 호출규약 차이가 거의 없지만, 우선 WinDLL 시도 후 실패 시 CDLL 폴백
    try:
        return ctypes.WinDLL(dll_path)
    except Exception:
        return ctypes.CDLL(dll_path)


# ─────────────────────────────────────────────────────────
# DLL 래퍼
# ─────────────────────────────────────────────────────────

class OESDLL:
    WL_LEN = 3680
    USE_START = 10
    USE_END = 1034  # [10:1034)

    def __init__(self, dll_path: str):
        self.dll = load_dll(dll_path)
        self._bind()

    def _bind(self):
        d = self.dll

        # 채널/초기화/읽기
        d.spTestAllChannels.argtypes = [c_int16]      # short sOrderType (0=USB순)
        d.spTestAllChannels.restype  = c_int16

        d.spSetupGivenChannel.argtypes = [c_int16]    # short sChannel
        d.spSetupGivenChannel.restype  = c_int16

        d.spGetModel.argtypes = [c_int16, POINTER(c_int16)]
        d.spGetModel.restype  = c_int16

        # 변종 대응: (채널,모델) 또는 (CCD,채널)
        d.spInitGivenChannel.restype  = c_int16       # argtypes 미고정

        d.spGetWLTable.argtypes = [POINTER(c_double), c_int16]
        d.spGetWLTable.restype  = c_int16

        d.spReadDataEx.argtypes = [POINTER(c_int32), c_int16]
        d.spReadDataEx.restype  = c_int16

        d.spCloseGivenChannel.argtypes = [c_uint16]
        d.spCloseGivenChannel.restype  = c_int16

        # 설정 관련
        d.spSetTrgEx.argtypes = [c_int16, c_int16]    # (trig, ch)
        d.spSetTrgEx.restype  = c_int16

        d.spSetDblIntEx.argtypes = [c_double, c_int16]
        d.spSetDblIntEx.restype  = c_int16

        d.spSetTEC.argtypes = [c_int32, c_int16]
        d.spSetTEC.restype  = c_int16

        d.spAutoDark.argtypes = [c_int16]
        d.spAutoDark.restype  = c_int16

        # 일부 모델 전용: Baseline (미지원이면 음수 반환 가능)
        d.spSetBaseLineCorrection.argtypes = [c_int16]  # (ch)로 문서화된 빌드가 다수
        d.spSetBaseLineCorrection.restype  = c_int16

        # 신형 기기 여부 체크(문서화: short spGetDevIsNew(short ch))
        try:
            d.spGetDevIsNew.argtypes = [c_int16]
            d.spGetDevIsNew.restype  = c_int16
            self._has_get_dev_is_new = True
        except Exception:
            self._has_get_dev_is_new = False

    # ---- 래퍼들
    def test_all_channels(self, order_type: int = 0) -> int:
        return int(self.dll.spTestAllChannels(c_int16(int(order_type))))

    def setup_channel(self, ch: int) -> int:
        return int(self.dll.spSetupGivenChannel(c_int16(ch)))

    def get_model(self, ch: int) -> Tuple[int, int]:
        model = c_int16()
        rc = int(self.dll.spGetModel(c_int16(ch), byref(model)))
        return rc, int(model.value)

    def init_channel(self, ch: int, model: Optional[int] = None) -> int:
        rc = -1
        if model is not None and model >= 0:
            try:
                rc = int(self.dll.spInitGivenChannel(c_int16(ch), c_int32(model)))
            except Exception:
                rc = -1
        if rc < 0:
            # 폴백: (CCD타입, 채널) — SONY(0) 가정
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

    def set_trigger(self, trig: int, ch: int) -> int:
        return int(self.dll.spSetTrgEx(c_int16(int(trig)), c_int16(ch)))

    def set_integration_ms(self, ms: float, ch: int) -> int:
        return int(self.dll.spSetDblIntEx(c_double(float(ms)), c_int16(ch)))

    def set_tec(self, onoff: int, ch: int) -> int:
        return int(self.dll.spSetTEC(c_int32(int(onoff)), c_int16(ch)))

    def auto_dark(self, ch: int) -> int:
        return int(self.dll.spAutoDark(c_int16(ch)))

    def baseline(self, ch: int) -> int:
        return int(self.dll.spSetBaseLineCorrection(c_int16(ch)))

    def get_dev_is_new(self, ch: int) -> Optional[int]:
        if getattr(self, "_has_get_dev_is_new", False):
            try:
                return int(self.dll.spGetDevIsNew(c_int16(ch)))
            except Exception:
                return None
        return None

    def read_data(self, ch: int):
        arr = (c_int32 * self.WL_LEN)()
        rc = int(self.dll.spReadDataEx(arr, c_int16(ch)))
        return rc, [int(arr[i]) for i in range(self.WL_LEN)]

    def close_channel(self, ch: int) -> int:
        return int(self.dll.spCloseGivenChannel(c_uint16(ch)))


# ─────────────────────────────────────────────────────────
# 동기 측정 루틴
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
    print(f"\n[설정] DLL={dll_path}")
    print(f"[설정] CH={chamber} (기본 매핑: CH1→USB0, CH2→USB1)")
    usb_index = (0 if chamber == 1 else 1) if usb_index_manual is None else int(usb_index_manual)
    print(f"[설정] USB index={'auto' if usb_index_manual is None else 'manual'} = {usb_index}")

    base = Path(save_root.strip()) if save_root and save_root.strip() else Path.cwd()
    save_dir = base / f"CH{chamber}"
    save_dir.mkdir(parents=True, exist_ok=True)

    # DLL 로드
    try:
        oes = OESDLL(dll_path)
    except Exception as e:
        print(f"❌ DLL 로드 실패: {e!r}")
        return 11

    # 채널 스캔(USB 포트 순)
    try:
        num = oes.test_all_channels(order_type=0)
        print(f"[INFO] 연결된 장치 수: {num}")
    except Exception as e:
        print(f"⚠️ 채널 스캔 예외: {e!r}")
        num = 0

    if num > 0 and not (0 <= usb_index < num):
        print(f"⚠️ 요청한 USB index {usb_index}가 범위를 벗어남 → 0으로 보정")
        usb_index = 0

    # 채널 오픈
    rc = oes.setup_channel(usb_index)
    print(f"[API] spSetupGivenChannel({usb_index}) → rc={rc}")
    if rc < 0:
        print("❌ Setup 실패")
        return 21

    # 모델 조회
    rc_m, model = oes.get_model(usb_index)
    print(f"[API] spGetModel({usb_index}) → rc={rc_m}, model={model}")
    if rc_m < 0:
        try: oes.close_channel(usb_index)
        except Exception: pass
        return 22

    # 초기화
    rc = oes.init_channel(usb_index, model=model)
    print(f"[API] spInitGivenChannel → rc={rc}")
    if rc < 0:
        try: oes.close_channel(usb_index)
        except Exception: pass
        return 23

    # 파장 테이블
    rc_wl, wl = oes.get_wl_table(usb_index)
    print(f"[API] spGetWLTable → rc={rc_wl}, len={len(wl) if wl else 0}")
    if rc_wl < 0 or not wl or len(wl) < OESDLL.USE_END:
        try:
            oes.set_tec(0, usb_index)
            oes.close_channel(usb_index)
        except Exception: pass
        return 24
    wl_slice = wl[OESDLL.USE_START:OESDLL.USE_END]

    # --- 설정 순서 재배치 ---
    # (1) 트리거 / (2) 적분시간 / (3) TEC / (지연) / (4) AutoDark / (5) Baseline(옵션)
    def _pr(tag, code):
        print(f"[API] {tag} → rc={code}")
        return code

    try:
        _pr("spSetTrgEx(11)", oes.set_trigger(11, usb_index))
        _pr("spSetDblIntEx(ms)", oes.set_integration_ms(integ_ms, usb_index))
        _pr("spSetTEC(1)",       oes.set_tec(1, usb_index))
        time.sleep(0.15)  # 초기화 직후 안정화 대기

        rc_ad = _pr("spAutoDark", oes.auto_dark(usb_index))
        if rc_ad < 0:
            print("⚠️  AutoDark 실패 — 경고만 출력하고 계속합니다.")

        # Baseline은 모델/펌웨어 따라 미지원 → 실패해도 경고 후 계속
        dev_new = oes.get_dev_is_new(usb_index)
        do_baseline = (dev_new == 1)  # 신형에서만 시도 (정보 없으면 보수적으로 Skip)
        if do_baseline:
            rc_bl = _pr("spSetBaseLineCorrection", oes.baseline(usb_index))
            if rc_bl < 0:
                print("⚠️  Baseline 실패(미지원 가능) — 계속 진행합니다.")
        else:
            print(f"[INFO] Baseline 건너뜀 (DevIsNew={dev_new})")

    except Exception as e:
        print(f"❌ 장치 설정 중 예외: {e!r}")
        try:
            oes.set_tec(0, usb_index)
            oes.close_channel(usb_index)
        except Exception: pass
        return 25

    # 측정 루프
    print(f"\n▶ 측정 시작: duration={duration_sec:.1f}s, integ={integ_ms:.0f}ms, avg={avg_count}")
    t0 = time.monotonic()
    rows = []
    samples = 0
    try:
        while time.monotonic() - t0 < duration_sec:
            acc = None; valid = 0
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
                    time.sleep(0.03)

            if not valid:
                print("⚠️  데이터 읽기 실패(이번 샘플). 계속 시도합니다.")
                time.sleep(0.1)
                continue

            for i in range(len(acc)):
                acc[i] /= float(valid)

            samples += 1
            y0 = acc[0]; ymax = max(acc)
            print(f"[DATA] sample={samples} points={len(acc)} y0={y0:.1f} yMax={ymax:.1f}")

            now_str = time.strftime("%H:%M:%S")
            rows.append([now_str] + acc)

            # 샘플 간 간격: 통상 integ_ms에 의해 결정되지만 과도한 sleep은 줄임
            time.sleep(max(0.0, float(integ_ms) / 1000.0 * 0.2))

    except KeyboardInterrupt:
        print("\n⏹  사용자 중단 요청 — 종료합니다.")
    finally:
        try: oes.set_tec(0, usb_index)
        except Exception: pass
        try: oes.close_channel(usb_index)
        except Exception: pass

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
# 엔트리포인트
# ─────────────────────────────────────────────────────────

def main():
    if os.name == "nt":
        try: os.system("chcp 65001 >NUL")
        except Exception: pass

    print("\n==== OES 단독 테스트 (device/oes.py 미사용 · Baseline 옵션 처리) ====\n"
          "엔터를 누르면 대괄호[]의 기본값이 적용됩니다.\n")

    default_dll = r"\\VanaM_NAS\VanaM_Sputter\OES\SDKs\DLL\x64\stdcall\SPdbUSBm.dll"

    chamber = ask("챔버 번호 (1 또는 2)", int, 1)
    if chamber not in (1, 2):
        print("⚠️  챔버 번호는 1 또는 2만 가능합니다. 1로 대체합니다.")
        chamber = 1

    duration = ask("측정 시간 (초)", float, 10.0)
    integ_ms = ask("적분 시간 (ms)", float, 1000.0)

    dll_in = input(f"DLL 경로 (비우면 기본)\n  기본: {default_dll}\n> ").strip()
    dll_path = dll_in or default_dll

    save_root = None

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
            avg_count=1,
        )
    except Exception as e:
        print(f"❌ 실행 중 예외: {e!r}")
        code = 99

    sys.exit(code)


if __name__ == "__main__":
    main()

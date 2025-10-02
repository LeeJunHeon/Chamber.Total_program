#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OES 단독 대화형(Interactive) 테스트 스크립트 — CH2 실패 폴백 강화판
 - 기본: CH1→USB0, CH2→USB1
 - 실패 시: USB 인덱스 스왑 자동 폴백(예: CH2→USB0)
 - 사용자 임의: USB 인덱스 수동 지정도 가능(빈 입력 시 자동)

사용:
  > python test_oes_interactive.py
  (질문에 답변 입력)
"""

import asyncio
import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List

# ── OESAsync 가져오기 (프로젝트 구조에 맞춰 우선 device/oes.py 시도)
try:
    from device.oes import OESAsync
except ImportError:
    # 같은 폴더에 oes.py 가 있을 때
    from oes import OESAsync  # type: ignore


def _ask(prompt: str, caster, default):
    """터미널에서 입력을 받아 타입 변환. 빈 입력은 default."""
    s = input(f"{prompt} [{default}]: ").strip()
    if s == "":
        return default
    try:
        return caster(s)
    except Exception:
        print("⚠️  입력이 올바르지 않습니다. 기본값을 사용합니다.")
        return default


async def _pump_events(oes: OESAsync) -> None:
    """OES 이벤트 수신 루프: status/data/finished 출력."""
    try:
        idx = 0
        async for ev in oes.events():
            k = getattr(ev, "kind", "")
            if k == "status":
                print(f"[STATUS] {ev.message}")
            elif k == "data":
                idx += 1
                x = getattr(ev, "x", None)
                y = getattr(ev, "y", None)
                if x and y:
                    # 과도한 로그 방지: 매 샘플마다 요약 1줄
                    try:
                        y0 = y[0]
                        ymax = max(y)
                        print(f"[DATA] points={len(x)}  y0={y0:.1f}  yMax={ymax:.1f}")
                    except Exception:
                        print(f"[DATA] points={len(x)}")
            elif k == "finished":
                ok = getattr(ev, "success", None)
                print(f"[FINISHED] {'성공' if ok else '실패/중단'}")
                return
    except asyncio.CancelledError:
        # 종료 시 정상 취소
        pass


def _build_candidate_kwargs(
    chamber: int,
    dll_in: Optional[str],
    save_dir_in: Optional[str],
    usb_index_manual: Optional[int],
) -> List[Dict[str, Any]]:
    """
    초기화 시도 후보군 생성:
      1) 사용자가 usb_index를 직접 지정한 경우 → 그것만 시도
      2) 아니면 자동:
         - 기본 매핑(ch->usb)
         - 스왑 폴백(usb 뒤바뀐 경우 대비)
         - (옵션) 최후의 수단: 반대쪽 또 스왑
    """
    base_kwargs: Dict[str, Any] = {"chamber": chamber}
    if dll_in:
        base_kwargs["dll_path"] = str(Path(dll_in))
    if save_dir_in:
        base_kwargs["save_directory"] = save_dir_in

    cands: List[Dict[str, Any]] = []

    if usb_index_manual is not None:
        # 수동 지정이면 그것만 시도
        k = dict(base_kwargs)
        k["usb_index"] = int(usb_index_manual)
        cands.append(k)
        return cands

    # 자동: 기본 매핑
    default_usb = 0 if chamber == 1 else 1
    k1 = dict(base_kwargs)
    k1["usb_index"] = default_usb
    cands.append(k1)

    # 자동: 스왑 폴백(윈도우가 USB 순서를 뒤바꿔 잡는 경우)
    swap_usb = 1 - default_usb
    k2 = dict(base_kwargs)
    k2["usb_index"] = swap_usb
    cands.append(k2)

    # (선택) 최후 수단: 혹시라도 장치 개수가 1대만 잡히는 특수 상황에서 0/1 둘 다 재시도
    # 위 두 개로 이미 0/1을 커버하므로 보통은 불필요하지만, 명시적으로 한 번 더 추가
    # k3 = dict(base_kwargs); k3["usb_index"] = 0; cands.append(k3)
    # k4 = dict(base_kwargs); k4["usb_index"] = 1; cands.append(k4)

    return cands


async def _try_one_config(kw: Dict[str, Any], duration: float, integ_ms: int) -> bool:
    """한 가지 설정으로 OES 초기화→측정 시도. 성공 시 True 반환."""
    # 개별 시도마다 새 인스턴스 생성(내부 워커/핸들 상태 분리)
    oes = OESAsync(**kw)

    desc = f"CH{kw.get('chamber')} / USB{kw.get('usb_index','auto')} / " \
           f"DLL={kw.get('dll_path','default')} / SAVE={kw.get('save_directory','default')}"
    print(f"\n[TRY] 초기화 시도: {desc}")

    try:
        ok = await oes.initialize_device()
        if not ok:
            print("❌ 초기화 실패. 다음 후보로 넘어갑니다.")
            # 안전 종료
            with contextlib.suppress(Exception):
                await oes.cleanup()
            return False

        print("▶ 측정을 시작합니다. (중단: Ctrl+C)")
        pump_task = asyncio.create_task(_pump_events(oes), name="pump_oes_events")
        run_task = asyncio.create_task(oes.run_measurement(duration, integ_ms), name="run_oes")
        await asyncio.wait({pump_task, run_task}, return_when=asyncio.ALL_COMPLETED)
        # 정상 종료
        with contextlib.suppress(Exception):
            await oes.cleanup()
        return True

    except KeyboardInterrupt:
        print("\n⏹  사용자 중단 요청(Ctrl+C) 수신 — 종료를 진행합니다.")
        with contextlib.suppress(Exception):
            await oes.stop_measurement()
        with contextlib.suppress(Exception):
            await oes.cleanup()
        return False

    except Exception as e:
        print(f"❌ 예외 발생: {e!r}. 다음 후보로 넘어갑니다.")
        with contextlib.suppress(Exception):
            await oes.cleanup()
        return False


async def main() -> int:
    if os.name == "nt":
        # Windows 콘솔을 UTF-8로 (선택)
        try:
            os.system("chcp 65001 >NUL")
        except Exception:
            pass

    print("\n==== OES 대화형 테스트 (CH2 폴백 강화) ====\n")
    print("엔터를 누르면 대괄호[]의 기본값이 적용됩니다.\n")

    # ── 사용자 입력(기본값은 합리적 값으로 설정)
    chamber = _ask("챔버 번호 (1 또는 2)", int, 1)
    if chamber not in (1, 2):
        print("⚠️  챔버 번호는 1 또는 2만 가능합니다. 1로 대체합니다.")
        chamber = 1

    duration = _ask("측정 시간 (초)", float, 10.0)
    integ_ms = _ask("적분 시간 (ms)", int, 1000)

    # DLL 경로/저장 경로는 빈 입력이면 OESAsync 기본값 사용
    dll_in = input("DLL 경로 (비우면 OESAsync 기본값 사용): ").strip()
    save_dir_in = input(r"저장 루트 폴더 (예: \\VanaM_NAS\VanaM_Sputter\OES, 비우면 OESAsync 기본): ").strip()

    # ── (선택) USB 인덱스 수동 지정
    usb_manual_in = input("USB 인덱스 수동 지정 (0 또는 1, 비우면 자동): ").strip()
    if usb_manual_in == "":
        usb_index_manual = None
    else:
        try:
            val = int(usb_manual_in)
            if val not in (0, 1):
                print("⚠️  USB 인덱스는 0 또는 1만 가능합니다. 자동으로 진행합니다.")
                usb_index_manual = None
            else:
                usb_index_manual = val
        except Exception:
            print("⚠️  USB 인덱스 해석 실패. 자동으로 진행합니다.")
            usb_index_manual = None

    # ── 초기화 후보군 구성
    cands = _build_candidate_kwargs(
        chamber=chamber,
        dll_in=dll_in or None,
        save_dir_in=save_dir_in or None,
        usb_index_manual=usb_index_manual,
    )

    # ── 순차 시도
    for i, kw in enumerate(cands, 1):
        ok = await _try_one_config(kw, duration, integ_ms)
        if ok:
            print("✅ 완료되었습니다.")
            return 0
        else:
            print(f"[INFO] 후보 {i}/{len(cands)} 시도 실패")

    print("❌ 모든 시도가 실패했습니다. 케이블/드라이버/전원/USB 포트 위치를 확인해 주세요.")
    return 2


# ── 안전한 import를 위해 contextlib 필요
import contextlib

if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except RuntimeError as e:
        # Windows 파워셸에서 이벤트 루프 중복 생성 이슈 방어
        print(f"[오류] 실행 실패: {e}")
        sys.exit(1)

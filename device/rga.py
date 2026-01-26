# device/rga.py
# -*- coding: utf-8 -*-
"""
SRS RGA (LAN) 컨트롤러 — srsinst.rga 기반 + asyncio 래퍼
- 동작: filament.on → get_histogram_scan → filament.off → partial pressure 보정(계산만)
- 이벤트 스트림(events): status / data / finished / failed
- CSV 저장: scan + 저장을 한 번에 수행하는 scan_histogram_to_csv(csv_path)
- 두 대 동시: IP별 인스턴스를 각각 생성하여 병렬 실행(await gather)
필수: pip install srsinst
"""

from __future__ import annotations
import asyncio, datetime, csv
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Literal, Tuple, Dict
import numpy as np

try:
    from srsinst.rga import RGA100
except ModuleNotFoundError as e:
    if e.name == "matplotlib":
        raise RuntimeError(
            "RGA 드라이버 import 실패: matplotlib 누락\n"
            "해결: pip install matplotlib (그리고 PyInstaller 빌드시 --collect-all matplotlib 권장)"
        ) from e
    raise RuntimeError(
        f"RGA 드라이버 import 실패: 누락 모듈={e.name}\n"
        "해결: pip install srsinst.rga"
    ) from e
except Exception as e:
    raise RuntimeError(f"RGA 드라이버 import 실패: {type(e).__name__}: {e}") from e


# ──────────────────────────────────────────────────────────────────────
# 이벤트 모델
# ──────────────────────────────────────────────────────────────────────
EventKind = Literal["status", "data", "finished", "failed"]

@dataclass
class RGAEvent:
    kind: EventKind
    message: Optional[str] = None
    # 그래프용: 부분압 보정 스펙트럼 [Torr]
    pressures: Optional[np.ndarray] = None
    # X축(히스토그램용): mass axis
    mass_axis: Optional[np.ndarray] = None
    # CSV 저장용: 원본 히스토그램(숫자값)
    histogram_raw: Optional[np.ndarray] = None
    timestamp: Optional[str] = None

# 파일별 잠금 (동시 쓰기 안전)
_FILE_LOCKS: Dict[Path, asyncio.Lock] = {}

# ──────────────────────────────────────────────────────────────────────
# RGA 어댑터
# ──────────────────────────────────────────────────────────────────────
class RGA100AsyncAdapter:
    """
    SRS 공식 드라이버(RGA100) 동기 API를 asyncio 친화적으로 감싼 래퍼.
    - LAN 접속: RGA100('tcpip', ip, user, password)
    - scan_histogram_once()          : 이벤트만 발생(그래프 갱신용)
    - scan_histogram_to_csv(path)    : 스캔 + CSV 저장(스니펫 포맷) + 이벤트
    """
    def __init__(self, host: str, *, user: str = "admin", password: str = "admin",
                 name: str = "", debug: bool = False, csv_encoding: str = "utf-8-sig"):
        self.host = host
        self.user = user
        self.password = password
        self.name = name or host
        self.debug = debug
        self.csv_encoding = csv_encoding
        self._ev_q: asyncio.Queue[RGAEvent] = asyncio.Queue(maxsize=256)

        # 추가: 동시 스캔 방지 & 연결상태 플래그
        self._scan_lock: asyncio.Lock = asyncio.Lock()
        self._connected: bool = False

    # ── 라이프사이클(ChamberRuntime 워치독과 호환) ─────────────────────
    async def start(self) -> None:
        """워치독/프리플라이트 호환을 위한 준비 표식."""
        self._connected = True
        await self._status(f"[{self.name}] RGA adapter ready")

    async def cleanup(self) -> None:
        """상태 초기화(열린 연결은 동기 호출마다 닫으므로 별도 자원 없음)."""
        self._connected = False
        await self._status(f"[{self.name}] RGA adapter cleanup")

    # ChamberRuntime._is_dev_connected가 호출할 수 있도록 메서드 제공
    def is_connected(self) -> bool:
        return self._connected

    # ── 이벤트 소비 ────────────────────────────────────────────────
    async def events(self):
        while True:
            yield await self._ev_q.get()

    # ── 공개 API: 스니펫과 동일 동작(저장 X) ─────────────────────────
    async def scan_histogram_once(self, *, timeout_s: float = 30.0) -> None:
        await self._status(f"[{self.name}] histogram scan 시작")
        try:
            async with self._scan_lock:
                mass_axis, hist_raw, part_press = await asyncio.wait_for(
                    asyncio.to_thread(self._blocking_histogram_once),
                    timeout=max(0.1, float(timeout_s)),
                )
            ts = _now_str()
            self._ev_nowait(RGAEvent(
                kind="data",
                mass_axis=mass_axis,
                histogram_raw=hist_raw,
                pressures=part_press,
                timestamp=ts
            ))
            await self._status(f"[{self.name}] histogram scan 완료 ({len(mass_axis)} bins)")
            await self._finished()
        except asyncio.TimeoutError:
            await self._failed("SCAN_HIST", "timeout")
        except Exception as e:
            await self._failed("SCAN_HIST", str(e))

    # ── 공개 API: 스니펫과 동일 동작 + CSV 저장 ───────────────────────
    async def scan_histogram_to_csv(self, csv_path: str | Path, *, suffix: str = "e-12", timeout_s: float = 30.0) -> None:
        """
        CSV 저장 규칙(스니펫과 동일):
          row = [timestamp] + [f"{value}e-12" for value in histogram_spectrum]
        헤더는 쓰지 않는다 (원본 스니펫과 동일).
        """
        await self._status(f"[{self.name}] histogram scan + CSV 저장 시작")
        try:
            async with self._scan_lock:
                mass_axis, hist_raw, part_press = await asyncio.wait_for(
                    asyncio.to_thread(self._blocking_histogram_once),
                    timeout=max(0.1, float(timeout_s)),
                )
            ts = _now_str()

            # 1) CSV 저장 (동일 포맷)
            path = Path(csv_path)
            lock = _FILE_LOCKS.setdefault(path, asyncio.Lock())
            async with lock:
                await asyncio.to_thread(self._append_row_sync, path, ts, hist_raw, suffix)

            # 2) 이벤트(그래프/로그)
            self._ev_nowait(RGAEvent(
                kind="data",
                mass_axis=mass_axis,
                histogram_raw=hist_raw,
                pressures=part_press,
                timestamp=ts
            ))
            await self._status(f"[{self.name}] CSV 저장 완료: {path} ({len(hist_raw)} points)")
            await self._finished()
        except asyncio.TimeoutError:
            await self._failed("SCAN_HIST_SAVE", "timeout")
        except Exception as e:
            await self._failed("SCAN_HIST_SAVE", str(e))

    # ── 내부: 동기 드라이버 호출(스레드에서 실행) ─────────────────────
    def _blocking_histogram_once(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        절대 파라미터를 변경하지 않는다.
        filament.on → get_histogram_scan → filament.off → partial pressure 보정(계산만)
        mass_axis(for_analog_scan=False)로 히스토그램용 X축을 얻는다.
        """
        rga = RGA100('tcpip', self.host, self.user, self.password)
        try:
            rga.filament.turn_on()
            histogram = np.asarray(rga.scan.get_histogram_scan(), dtype=float)  # 원본
            rga.filament.turn_off()
            part_press = np.asarray(
                rga.scan.get_partial_pressure_corrected_spectrum(histogram),
                dtype=float
            )  # 그래프용(Torr)
            mass_axis = np.asarray(
                rga.scan.get_mass_axis(for_analog_scan=False),
                dtype=float
            )
            return mass_axis, histogram, part_press
        finally:
            try:
                rga.disconnect()
            except Exception:
                pass

    # ── 내부: CSV append(헤더 없음, 스니펫 동일) ──────────────────────
    def _append_row_sync(self, path: Path, timestamp: str, hist_raw: np.ndarray, suffix: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, mode="a", newline="", encoding=self.csv_encoding) as f:
            w = csv.writer(f)
            row = [timestamp] + [f"{v}{suffix}" for v in hist_raw.tolist()]
            w.writerow(row)

    # ── 이벤트/로그 ────────────────────────────────────────────────
    def _ev_nowait(self, ev: RGAEvent) -> None:
        try:
            self._ev_q.put_nowait(ev)
        except Exception:
            pass

    async def _status(self, msg: str) -> None:
        if self.debug:
            print(msg)
        await self._ev_q.put(RGAEvent(kind="status", message=msg))

    async def _finished(self) -> None:
        await self._ev_q.put(RGAEvent(kind="finished", message="done"))

    async def _failed(self, cmd: str, why: str) -> None:
        await self._ev_q.put(RGAEvent(kind="failed", message=f"{cmd}: {why}"))

# ──────────────────────────────────────────────────────────────────────
def _now_str() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

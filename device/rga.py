# device/rga.py
# -*- coding: utf-8 -*-
"""
rga_async.py — asyncio 기반 RGA 컨트롤러 (외부 프로그램 실행 + CSV 파싱)

핵심:
  - Qt 제거(QProcess/QTimer/Signals 없음), 완전 asyncio
  - 외부 프로그램 실행: asyncio.create_subprocess_exec + timeout
  - 프로그램 종료 후 CSV 마지막 행만 파싱(타임스탬프 중복 처리)
  - 상위(UI/브리지)에는 async 제너레이터 events()로 status/data/finished/failed 이벤트 전달
  - 파일 I/O/CSV 파싱은 asyncio.to_thread로 루프 비블로킹

의존: numpy
"""

from __future__ import annotations
import asyncio
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncGenerator, Literal, Optional

import numpy as np
from lib.config_ch2 import RGA_PROGRAM_PATH, RGA_CSV_PATH


# ===== 이벤트 모델 =====
EventKind = Literal["status", "data", "finished", "failed"]

@dataclass
class RGAEvent:
    kind: EventKind
    # status/failed
    message: Optional[str] = None
    # data
    mass_axis: Optional[np.ndarray] = None
    pressures: Optional[np.ndarray] = None
    timestamp: Optional[str] = None


class RGAAsync:
    def __init__(
        self,
        *,
        program_path: str | Path = RGA_PROGRAM_PATH,
        csv_path: str | Path = RGA_CSV_PATH,
        debug_print: bool = True,
    ):
        self.program_path = Path(program_path)
        self.csv_path = Path(csv_path)
        self.debug_print = debug_print

        self.last_processed_timestamp: Optional[str] = None
        self._ev_q: asyncio.Queue[RGAEvent] = asyncio.Queue(maxsize=256)

    # ---------- 외부 API ----------
    async def scan_once(self, *, timeout_s: float = 60.0, post_wait_s: float = 2.0):
        """외부 RGA 프로그램 1회 실행 → CSV 최신 데이터 파싱 → 이벤트 방출."""
        if not self.program_path.exists():
            msg = f"외부 프로그램을 찾을 수 없습니다: {self.program_path}"
            await self._status(msg)
            await self._failed("EXTERNAL_RGA_SCAN", msg)
            return

        await self._status(f"외부 RGA 프로그램 실행 (대기 {int(post_wait_s)}s): {self.program_path}")

        try:
            proc = await asyncio.create_subprocess_exec(
                str(self.program_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
            except asyncio.TimeoutError:
                with contextlib.suppress(Exception):
                    proc.kill()
                await proc.wait()
                msg = f"외부 프로그램 실행 시간 초과 ({int(timeout_s)}초)."
                await self._status(msg)
                await self._failed("EXTERNAL_RGA_SCAN", msg)
                return

            rc = proc.returncode or 0
            if rc != 0:
                err = (stderr or b"").decode(errors="ignore").strip()
                msg = f"외부 프로그램 비정상 종료 (Code: {rc}). {err}"
                await self._status(msg)
                await self._failed("EXTERNAL_RGA_SCAN", msg)
                return

            await self._status("외부 프로그램 정상 종료. CSV 파일 분석 대기 중...")
            await asyncio.sleep(max(0.0, float(post_wait_s)))  # 파일 flush 여유

            parsed = await asyncio.to_thread(self._parse_csv_latest_sync)
            if parsed is None:
                await self._status("새 데이터가 없습니다. 분석을 건너뜁니다.")
                await self._finished()
                return

            mass_axis, pressures, ts = parsed
            # 중복 방지: 마지막 처리 타임스탬프 비교
            if ts and ts == self.last_processed_timestamp:
                await self._status("새로운 데이터가 없습니다. (동일 타임스탬프)")
                await self._finished()
                return

            self.last_processed_timestamp = ts
            await self._status(f"새 데이터 감지: {ts}")
            self._ev_nowait(RGAEvent(kind="data", mass_axis=mass_axis, pressures=pressures, timestamp=ts))
            await self._status("그래프 업데이트 완료.")
            await self._finished()

        except FileNotFoundError:
            msg = f"외부 프로그램을 찾을 수 없습니다: {self.program_path}"
            await self._status(msg)
            await self._failed("EXTERNAL_RGA_SCAN", msg)
        except Exception as e:
            msg = f"외부 프로그램 실행 중 예외: {e}"
            await self._status(msg)
            await self._failed("EXTERNAL_RGA_SCAN", msg)

    async def events(self) -> AsyncGenerator[RGAEvent, None]:
        """상위(UI/브리지)에서 소비하는 이벤트 스트림."""
        while True:
            ev = await self._ev_q.get()
            yield ev

    # ---------- CSV 파서 (blocking; to_thread에서 호출) ----------
    def _parse_csv_latest_sync(self) -> Optional[tuple[np.ndarray, np.ndarray, str]]:
        """
        CSV 파일에서 마지막 데이터 행만 읽어 (mass_axis, pressures, timestamp) 반환.
        헤더: Time, Mass 1, Mass 2, ...
        """
        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {self.csv_path}")

        with open(self.csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)

            try:
                header_row = next(reader)
            except StopIteration:
                raise ValueError("CSV 파일이 비어 있습니다 (헤더 없음).")

            last_data_row: Optional[list[str]] = None
            for row in reader:
                if row:
                    last_data_row = row
            if last_data_row is None:
                raise ValueError("CSV 파일에 데이터 행이 없습니다.")

            ts = last_data_row[0].strip()

            # x축: 'Mass n' → n
            try:
                mass_axis = np.array([int(h.replace("Mass ", "")) for h in header_row[1:]], dtype=int)
            except Exception as e:
                raise ValueError(f"헤더 파싱 실패: {e}")

            # y축: 마지막 행의 값들 float
            try:
                pressures = np.array([float(v) for v in last_data_row[1:]], dtype=float)
            except Exception as e:
                raise ValueError(f"데이터 파싱 실패: {e}")

            if mass_axis.size != pressures.size:
                raise ValueError(f"열 개수 불일치: mass={mass_axis.size}, data={pressures.size}")

            return mass_axis, pressures, ts

    # ---------- 이벤트/로그 ----------
    async def _status(self, msg: str):
        if self.debug_print:
            print(f"[RGA][status] {msg}")
        await self._ev_q.put(RGAEvent(kind="status", message=msg))

    async def _finished(self):
        await self._ev_q.put(RGAEvent(kind="finished", message="scan finished"))

    async def _failed(self, cmd: str, why: str):
        await self._ev_q.put(RGAEvent(kind="failed", message=f"{cmd}: {why}"))

    def _ev_nowait(self, ev: RGAEvent):
        try:
            self._ev_q.put_nowait(ev)
        except Exception:
            pass

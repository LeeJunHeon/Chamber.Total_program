# controller/tsp_controller.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Callable
import asyncio, math, contextlib

from device.tsp import AsyncTSP
from device.ig import AsyncIG

@dataclass
class TSPRunConfig:
    target_pressure: float            # 목표 압력(이하 도달 시 종료)
    cycles: int                       # TSP ON/OFF 반복 횟수
    on_sec: float = 120.0             # TSP ON 유지 시간(2분)
    off_sec: float = 150.0            # TSP OFF 유지 시간(2분 30초)
    poll_sec: float = 10.0            # IG RDI 폴링 주기(10초)
    first_check_delay_sec: float = 5.0  # IG ON 후 첫 판정 전 대기(5초)
    verify_with_status: bool = True   # TSP on/off 후 205 상태확인

@dataclass
class TSPRunResult:
    success: bool
    cycles_done: int
    final_pressure: float
    reason: Optional[str] = None   # "initial_below_target" / "reached_target_on" / "reached_target_off" / "cycles_exhausted" ...

class TSPProcessController:
    """
    1) IG ensure_on → first_check_delay_sec 대기 → 1회 RDI 선판정(≤ target이면 즉시 종료)
    2) i=1..cycles:
         ON  on_sec 동안  poll_sec 간격 RDI(중간 달성 시 즉시 종료)
         OFF off_sec 동안 poll_sec 간격 RDI(중간 달성 시 즉시 종료)
       (ON+OFF 완료 시 cycles_done += 1)
    3) cycles 소진 시 종료
    """
    def __init__(self, tsp: AsyncTSP, ig: AsyncIG, *,
                 log_cb: Optional[Callable[[str], None]] = None,
                 pressure_cb: Optional[Callable[[float], None]] = None,
                 cycle_cb: Optional[Callable[[int,int], None]] = None,
                 state_cb: Optional[Callable[[str], None]] = None,
                 turn_off_ig_on_finish: bool = True) -> None:
        self.tsp = tsp
        self.ig = ig
        self.log_cb = log_cb
        self.pressure_cb = pressure_cb
        self.cycle_cb = cycle_cb
        self.state_cb = state_cb
        self.turn_off_ig_on_finish = turn_off_ig_on_finish
        self._last_pressure: float = math.nan

    def _log(self, msg: str) -> None:
        if self.log_cb: self.log_cb(msg)
    def _emit_state(self, s: str) -> None:
        if self.state_cb: self.state_cb(s)
    def _emit_pressure(self, p: float) -> None:
        if self.pressure_cb: self.pressure_cb(p)
    def _emit_cycle(self, cur: int, total: int) -> None:
        if self.cycle_cb: self.cycle_cb(cur, total)

    async def _poll_until(self, *, target: float, duration: float, poll_sec: float) -> tuple[bool, float]:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + max(0.0, duration)
        last_p = self._last_pressure
        while True:
            try:
                p = await self.ig.read_pressure()
                last_p = p
                self._last_pressure = p
                self._emit_pressure(p)
                if p <= target:
                    return True, p
            except Exception as e:
                self._log(f"[IG] 압력 읽기 오류: {e!r}")
            now = loop.time()
            if now >= deadline:
                return False, last_p
            await asyncio.sleep(min(poll_sec, max(0.0, deadline - now)))

    async def run(self, cfg: TSPRunConfig) -> TSPRunResult:
        cycles_done = 0
        self._last_pressure = math.nan
        reason: Optional[str] = None

        # 옵션 전달
        self.tsp.verify_with_status = cfg.verify_with_status

        try:
            self._emit_state("prepare")
            self._log(f"[TSP] 공정 시작: target={cfg.target_pressure}, cycles={cfg.cycles}, "
                      f"on={cfg.on_sec}s, off={cfg.off_sec}s, poll={cfg.poll_sec}s, "
                      f"first_wait={cfg.first_check_delay_sec}s")

            # 1) IG ON → 5초 대기 → 1회 RDI 선판정
            self._emit_state("ig_on")
            await self.ig.ensure_on()
            self._log("[IG] ensure_on 완료")
            await asyncio.sleep(cfg.first_check_delay_sec)
            try:
                p0 = await self.ig.read_pressure()
                self._last_pressure = p0
                self._emit_pressure(p0)
                self._log(f"[IG] 초기 판정 P0={p0:.3e}")
                if p0 <= cfg.target_pressure:
                    return TSPRunResult(True, cycles_done, self._last_pressure, "initial_below_target")
            except Exception as e:
                self._log(f"[IG] 초기 판정 RDI 실패: {e!r}")

            # 2) 사이클 반복
            for i in range(1, cfg.cycles + 1):
                # ON 단계
                self._emit_state("tsp_on")
                self._log(f"[TSP] ON (cycle {i}/{cfg.cycles})")
                await self.tsp.on()
                reached, _ = await self._poll_until(target=cfg.target_pressure,
                                                    duration=cfg.on_sec,
                                                    poll_sec=cfg.poll_sec)
                if reached:
                    return TSPRunResult(True, cycles_done, self._last_pressure, "reached_target_on")

                # OFF 단계
                self._emit_state("tsp_off")
                self._log(f"[TSP] OFF (cycle {i}/{cfg.cycles})")
                await self.tsp.off()
                reached, _ = await self._poll_until(target=cfg.target_pressure,
                                                    duration=cfg.off_sec,
                                                    poll_sec=cfg.poll_sec)
                if reached:
                    return TSPRunResult(True, cycles_done, self._last_pressure, "reached_target_off")

                # 사이클 완주
                cycles_done = i
                self._emit_cycle(cycles_done, cfg.cycles)

            # 3) cycles 소진 (목표 미달)
            return TSPRunResult(False, cycles_done, self._last_pressure, "cycles_exhausted")

        except asyncio.CancelledError:
            self._log("[TSP] run() 취소됨")
            return TSPRunResult(False, cycles_done, self._last_pressure, "cancelled")

        except Exception as e:
            self._log(f"[TSP] 예외: {e!r}")
            return TSPRunResult(False, cycles_done, self._last_pressure, f"exception:{e!r}")

        finally:
            self._emit_state("cleanup")
            with contextlib.suppress(Exception):
                await self.tsp.off()
            if self.turn_off_ig_on_finish:
                with contextlib.suppress(Exception):
                    await self.ig.ensure_off()
            self._emit_state("finished")

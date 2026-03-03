# controller/tsp_controller.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass, replace
from typing import Optional, Callable
import asyncio, math, contextlib

from lib import config_common as cfgc

from device.tsp import AsyncTSP
from device.ig import AsyncIG

@dataclass
class TSPRunConfig:
    target_pressure: float
    cycles: int
    on_sec: Optional[float] = None   # ✅ None이면 config_common(TSP_*)에서 채움
    off_sec: Optional[float] = None
    poll_sec: Optional[float] = None
    first_check_delay_sec: Optional[float] = None
    verify_with_status: Optional[bool] = None

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

    def _resolve_cfg(self, cfg: TSPRunConfig) -> TSPRunConfig:
        """
        cfg의 None 필드를 config_common(TSP_*) 값으로 채운다.
        -> TSP 설정은 이제 config_common.py에서만 관리.
        """
        on_sec = cfg.on_sec if cfg.on_sec is not None else float(getattr(cfgc, "TSP_ON_SEC", 120.0))
        off_sec = cfg.off_sec if cfg.off_sec is not None else float(getattr(cfgc, "TSP_OFF_SEC", 150.0))
        poll_sec = cfg.poll_sec if cfg.poll_sec is not None else float(getattr(cfgc, "TSP_POLL_SEC", 10.0))
        first_wait = cfg.first_check_delay_sec if cfg.first_check_delay_sec is not None else float(getattr(cfgc, "TSP_FIRST_CHECK_DELAY_SEC", 5.0))
        verify = cfg.verify_with_status if cfg.verify_with_status is not None else bool(getattr(cfgc, "TSP_VERIFY_WITH_STATUS", True))

        # 안전장치(0/음수 방지)
        on_sec = max(0.0, float(on_sec))
        off_sec = max(0.0, float(off_sec))
        poll_sec = max(0.1, float(poll_sec))  # 0이면 busy loop 방지
        first_wait = max(0.0, float(first_wait))

        return replace(cfg,
                    on_sec=on_sec,
                    off_sec=off_sec,
                    poll_sec=poll_sec,
                    first_check_delay_sec=first_wait,
                    verify_with_status=verify)

    async def _poll_until(self, *, target: float, duration: float, poll_sec: float) -> tuple[bool, float]:
        loop = asyncio.get_running_loop()
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
            
            poll_s = max(0.1, float(poll_sec))  # ✅ 최소 폴링 간격
            await asyncio.sleep(min(poll_s, max(0.0, deadline - now)))

    async def run(self, cfg: TSPRunConfig) -> TSPRunResult:
        cycles_done = 0
        self._last_pressure = math.nan

        # ✅ config_common 기반으로 None 필드 채움
        cfg = self._resolve_cfg(cfg)

        # ✅ cycles 유효성(0이면 바로 실패 처리)
        if int(cfg.cycles) <= 0:
            return TSPRunResult(False, 0, self._last_pressure, "invalid_cycles")

        # 옵션 전달(그리고 원복을 위해 백업)
        old_verify = getattr(self.tsp, "verify_with_status", None)
        self.tsp.verify_with_status = bool(cfg.verify_with_status)

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

            # ✅ verify_with_status 원복(존재할 때만)
            with contextlib.suppress(Exception):
                if old_verify is not None:
                    self.tsp.verify_with_status = old_verify

            with contextlib.suppress(Exception):
                await self.tsp.off()
            if self.turn_off_ig_on_finish:
                with contextlib.suppress(Exception):
                    await self.ig.ensure_off()
            self._emit_state("finished")

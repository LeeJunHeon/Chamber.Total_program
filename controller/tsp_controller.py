# tsp_controller.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Callable, Protocol
import asyncio
import math


class IGControllerLike(Protocol):
    async def ensure_on(self) -> None: ...
    async def ensure_off(self) -> None: ...
    async def read_pressure(self) -> float: ...


@dataclass
class TSPProcessConfig:
    target_pressure_torr: float
    tsp_on_seconds: float = 75.0     # 1분 15초(요청: 1분 10~20초 범위)
    off_wait_seconds: float = 150.0  # 2분 30초
    max_cycles: int = 10
    ig_poll_interval: float = 1.0

    def __post_init__(self):
        if not (self.target_pressure_torr > 0):
            raise ValueError("target_pressure_torr > 0 이어야 함")
        self.max_cycles = max(1, int(self.max_cycles))
        self.tsp_on_seconds = max(1.0, float(self.tsp_on_seconds))
        self.off_wait_seconds = max(1.0, float(self.off_wait_seconds))
        self.ig_poll_interval = max(0.2, float(self.ig_poll_interval))


@dataclass
class TSPRunResult:
    ok: bool
    cycles_used: int
    last_pressure_torr: float
    reason: str


class TSPBurstRunner:
    """
    사이클:
      1) IG ON + 측정
      2) TSP ON (tsp_on_seconds 동안), 폴링 도중 목표압력 도달하면 즉시 TSP/IG OFF 후 종료
      3) TSP OFF, off_wait_seconds 대기하며 IG 폴링(도달 시 종료)
      4) 최대 max_cycles 반복, 미도달이면 종료
    Stop(취소) 시 모든 장비 OFF
    """

    def __init__(self,
                 tsp, ig: IGControllerLike,
                 *,
                 log_cb: Optional[Callable[[str, str], None]] = None,
                 state_cb: Optional[Callable[[str], None]] = None,
                 pressure_cb: Optional[Callable[[float], None]] = None,
                 cycle_cb: Optional[Callable[[int, int], None]] = None,
                 ) -> None:
        self.tsp = tsp
        self.ig = ig
        self.log_cb = log_cb
        self.state_cb = state_cb
        self.pressure_cb = pressure_cb
        self.cycle_cb = cycle_cb
        self._last_pressure: float = math.nan

    # ======== 내부 헬퍼 ========
    def _log(self, src: str, msg: str) -> None:
        if self.log_cb:
            try:
                self.log_cb(src, msg)
            except Exception:
                pass

    def _state(self, s: str) -> None:
        self._log("STATE", s)
        if self.state_cb:
            try:
                self.state_cb(s)
            except Exception:
                pass

    def _tick_pressure(self, p: float) -> None:
        self._last_pressure = p
        if self.pressure_cb:
            try:
                self.pressure_cb(p)
            except Exception:
                pass

    async def _poll_ig_until(self, *, seconds: float, target: float,
                             cancel_event: Optional[asyncio.Event]) -> bool:
        """seconds 동안 폴링. 도달 시 True."""
        t_end = asyncio.get_event_loop().time() + seconds
        while True:
            if cancel_event and cancel_event.is_set():
                return False
            try:
                p = await self.ig.read_pressure()
                self._tick_pressure(p)
                self._log("IG", f"현재 압력: {p:.3e} Torr (목표 {target:.3e})")
                if p <= target:
                    return True
            except Exception as e:
                self._log("IG", f"압력 읽기 오류: {e!r}")
            now = asyncio.get_event_loop().time()
            if now >= t_end:
                return False
            await asyncio.sleep(min(1.0, seconds, 0.5))  # 빠르게 깨어 IG 주기는 run()에서 제어

    # ======== 메인 루프 ========
    async def run(self, cfg: TSPProcessConfig, *, cancel_event: Optional[asyncio.Event] = None) -> TSPRunResult:
        ok = False
        reason = "max_cycles_exhausted"
        cycles_used = 0
        self._state("프로세스 시작")

        try:
            # 시작: IG 켜고 초기압력 기록
            await self.ig.ensure_on()
            self._state("IG ON")
            try:
                p0 = await self.ig.read_pressure()
                self._tick_pressure(p0)
                self._log("IG", f"시작 압력: {p0:.3e} Torr")
                if p0 <= cfg.target_pressure_torr:
                    ok = True
                    reason = "already_at_target"
                    return TSPRunResult(ok, 0, p0, reason)
            except Exception as e:
                self._log("IG", f"초기 압력 읽기 오류: {e!r}")

            # 사이클 반복
            for cycle in range(1, cfg.max_cycles + 1):
                if cancel_event and cancel_event.is_set():
                    reason = "canceled"
                    return TSPRunResult(False, cycles_used, self._last_pressure, reason)

                cycles_used = cycle
                if self.cycle_cb:
                    try:
                        self.cycle_cb(cycle, cfg.max_cycles)
                    except Exception:
                        pass
                self._state(f"[{cycle}/{cfg.max_cycles}] TSP ON")

                # 2) TSP ON + 동안 폴링
                await self.tsp.start()
                on_end = asyncio.get_event_loop().time() + cfg.tsp_on_seconds
                while asyncio.get_event_loop().time() < on_end:
                    if cancel_event and cancel_event.is_set():
                        reason = "canceled"
                        return TSPRunResult(False, cycles_used, self._last_pressure, reason)
                    try:
                        p = await self.ig.read_pressure()
                        self._tick_pressure(p)
                        self._log("IG", f"현재 압력: {p:.3e} Torr")
                        if p <= cfg.target_pressure_torr:
                            self._state("목표 압력 도달 → TSP/IG OFF")
                            ok = True
                            reason = "target_reached_during_on"
                            await self.tsp.ensure_off()
                            await self.ig.ensure_off()
                            return TSPRunResult(ok, cycles_used, p, reason)
                    except Exception as e:
                        self._log("IG", f"압력 읽기 오류: {e!r}")
                    await asyncio.sleep(cfg.ig_poll_interval)

                # 3) TSP OFF + 대기 구간 폴링
                await self.tsp.ensure_off()
                self._state(f"[{cycle}/{cfg.max_cycles}] TSP OFF (대기 {cfg.off_wait_seconds:.0f}s)")
                reached = await self._poll_ig_until(
                    seconds=cfg.off_wait_seconds,
                    target=cfg.target_pressure_torr,
                    cancel_event=cancel_event
                )
                if reached:
                    self._state("목표 압력 도달 → IG OFF")
                    ok = True
                    reason = "target_reached_during_off_wait"
                    await self.ig.ensure_off()
                    return TSPRunResult(ok, cycles_used, self._last_pressure, reason)

            # 4) 미도달 종료
            self._state("최대 사이클 도달(미도달) → 장비 OFF")
            ok = False
            reason = "not_reached_after_max_cycles"
            await self.tsp.ensure_off()
            await self.ig.ensure_off()
            return TSPRunResult(ok, cycles_used, self._last_pressure, reason)

        except Exception as e:
            self._log("ERR", f"예외 발생: {e!r}")
            reason = f"exception:{e!r}"
            return TSPRunResult(False, cycles_used, self._last_pressure, reason)

        finally:
            # 안전 정리 (혹시 남았으면)
            try:
                await self.tsp.ensure_off()
            except Exception:
                pass
            try:
                await self.ig.ensure_off()
            except Exception:
                pass

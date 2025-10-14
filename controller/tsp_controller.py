# controller/tsp_controller.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Callable
import asyncio, math, contextlib
from datetime import datetime

# 장비
from device.tsp import AsyncTSP
from device.ig import AsyncIG  # IG는 CH1 장비 사용

# ─────────────────────────────────────────────────────────────
@dataclass
class TSPRunConfig:
    target_pressure: float          # 목표 압력(이하 도달 시 종료)
    cycles: int                     # TSP ON/OFF 반복 횟수
    dwell_sec: float = 150.0        # ON/OFF 사이 대기 (2분 30초 = 150초)
    poll_sec: float = 5.0           # IG 압력 폴링 주기
    verify_with_status: bool = True # TSP on/off 시 205 확인 여부 (tsp.py 옵션 연결)

@dataclass
class TSPRunResult:
    success: bool
    cycles_done: int
    final_pressure: float
    reason: Optional[str] = None

# ─────────────────────────────────────────────────────────────
class TSPProcessController:
    """
    공정 순서:
      1) IG ensure_on → poll_sec 간격으로 압력 읽기 시작
      2) TSP on → dwell_sec → TSP off → dwell_sec = 1사이클
      3) cycles 만큼 반복. 단, 압력이 target 이하가 되면 즉시 종료
    """
    def __init__(
        self,
        tsp: AsyncTSP,
        ig: AsyncIG,
        *,
        log_cb: Optional[Callable[[str], None]] = None,
        pressure_cb: Optional[Callable[[float], None]] = None,
        cycle_cb: Optional[Callable[[int, int], None]] = None,
        state_cb: Optional[Callable[[str], None]] = None,
        turn_off_ig_on_finish: bool = True,
    ) -> None:
        self.tsp = tsp
        self.ig = ig
        self.log_cb = log_cb
        self.pressure_cb = pressure_cb
        self.cycle_cb = cycle_cb
        self.state_cb = state_cb
        self.turn_off_ig_on_finish = turn_off_ig_on_finish

        self._last_pressure: float = math.nan

    # ── 내부 로깅 헬퍼 ──────────────────────────────────────
    def _log(self, msg: str) -> None:
        if self.log_cb:
            self.log_cb(msg)

    def _emit_state(self, s: str) -> None:
        if self.state_cb:
            self.state_cb(s)

    def _emit_pressure(self, p: float) -> None:
        if self.pressure_cb:
            self.pressure_cb(p)

    def _emit_cycle(self, cur: int, total: int) -> None:
        if self.cycle_cb:
            self.cycle_cb(cur, total)

    # ── 공정 실행 ──────────────────────────────────────────
    async def run(self, cfg: TSPRunConfig) -> TSPRunResult:
        """
        반환: TSPRunResult(success, cycles_done, final_pressure, reason)
        """
        stop_event = asyncio.Event()
        cycles_done = 0
        self._last_pressure = math.nan

        # TSP 옵션 연결(원치 않으면 cfg에서 False로)
        self.tsp.verify_with_status = cfg.verify_with_status

        try:
            self._emit_state("prepare")
            self._log(f"[TSP] 공정 시작: target={cfg.target_pressure}, "
                      f"cycles={cfg.cycles}, dwell={cfg.dwell_sec}s, poll={cfg.poll_sec}s")

            # 1) IG 켜기
            self._emit_state("ig_on")
            await self.ig.ensure_on()
            self._log("[IG] ensure_on 완료")

            # 2) 압력 폴링 태스크
            async def pressure_task():
                nonlocal stop_event
                while not stop_event.is_set():
                    try:
                        p = await self.ig.read_pressure()
                        self._last_pressure = p
                        self._emit_pressure(p)
                        self._log(f"[IG] P={p:.3e}")
                        if p <= cfg.target_pressure:
                            self._log(f"[IG] 목표 압력 도달({p:.3e} ≤ {cfg.target_pressure:.3e}) → 종료 신호")
                            stop_event.set()
                            return
                    except Exception as e:
                        self._log(f"[IG] 압력 읽기 오류: {e!r}")
                    await asyncio.sleep(cfg.poll_sec)

            # 3) TSP 토글 태스크
            async def toggle_task():
                nonlocal cycles_done, stop_event
                # 토글 반복 (ON → 대기 → OFF → 대기)
                for i in range(1, cfg.cycles + 1):
                    if stop_event.is_set():
                        break
                    self._emit_state("tsp_on")
                    self._log(f"[TSP] ON (cycle {i}/{cfg.cycles})")
                    await self.tsp.on()
                    await self._sleep_or_stop(cfg.dwell_sec, stop_event)
                    if stop_event.is_set():
                        break

                    self._emit_state("tsp_off")
                    self._log(f"[TSP] OFF (cycle {i}/{cfg.cycles})")
                    await self.tsp.off()
                    await self._sleep_or_stop(cfg.dwell_sec, stop_event)

                    cycles_done = i
                    self._emit_cycle(cycles_done, cfg.cycles)

            # 동시 실행
            pr_task = asyncio.create_task(pressure_task(), name="tsp_pr")
            tg_task = asyncio.create_task(toggle_task(),   name="tsp_tg")

            # 누가 먼저 끝나든 정리
            await asyncio.wait({pr_task, tg_task}, return_when=asyncio.FIRST_COMPLETED)
            stop_event.set()
            with contextlib.suppress(Exception):
                await tg_task
            with contextlib.suppress(Exception):
                await pr_task

            # 종료 사유/성공판단
            if self._last_pressure == self._last_pressure and self._last_pressure <= cfg.target_pressure:
                reason = "reached_target"
                success = True
            else:
                # 토글 정상 완료(모든 사이클 소화)면 success로 볼지 선택.
                # 여기서는 목표 미달이면 False로 표기(원하면 정책 변경 가능)
                reason = "target_not_reached"
                success = False

            return TSPRunResult(success, cycles_done, self._last_pressure, reason)

        except asyncio.CancelledError:
            self._log("[TSP] run() 취소됨")
            return TSPRunResult(False, cycles_done, self._last_pressure, "cancelled")

        except Exception as e:
            self._log(f"[TSP] 예외: {e!r}")
            return TSPRunResult(False, cycles_done, self._last_pressure, f"exception:{e!r}")

        finally:
            # 안전 종료: TSP OFF, IG OFF(옵션)
            self._emit_state("cleanup")
            with contextlib.suppress(Exception):
                await self.tsp.off()
            if self.turn_off_ig_on_finish:
                with contextlib.suppress(Exception):
                    await self.ig.ensure_off()
            self._emit_state("finished")

    # ── 보조: stop_event 감시 대기 ──────────────────────────
    async def _sleep_or_stop(self, sec: float, stop_event: asyncio.Event):
        end = asyncio.get_event_loop().time() + sec
        while not stop_event.is_set():
            remain = end - asyncio.get_event_loop().time()
            if remain <= 0:
                return
            await asyncio.wait({asyncio.create_task(stop_event.wait())}, timeout=min(0.5, remain))

# -*- coding: utf-8 -*-
"""
process_tsp.py — TSP 버스트 공정 러너 (asyncio)

UI의 "TSP Start" 버튼이 트리거하는 공정을 별도 파일로 분리.
아래 순서를 비동기로 안전하게 수행합니다.

1) TSP 시작 → IG ON & 압력 측정 시작
2) TSP를 일정 시간(기본 75초) 구동하는 동안 목표 압력 도달 여부를 모니터링
   - 도달하면 즉시 TSP/IG OFF → 성공 종료
   - 도달 못하면 TSP OFF 후 대기(기본 150초) 동안 다시 모니터링
3) 2)의 ON/OFF 사이클을 최대 N회(기본 10) 반복
   - 반복 중 도달하면 성공 종료, 끝까지 못 도달하면 실패 종료

DI(의존성 주입) 방식: 장비 제어는 콜백/객체로 주입
- TSP: start()/stop() → bool 를 제공하는 async 객체
- IG: ensure_on()/ensure_off()/read_pressure() → float(Torr) 를 제공하는 async 객체

Cancel(중지 버튼) 지원: cancel_event가 set()되면 즉시 안전 종료(TSP/IG OFF) 후 중단.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Callable, Optional

# ─────────────────────────────────────────────────────────────
# 설정/결과 구조체
# ─────────────────────────────────────────────────────────────

@dataclass
class TSPProcessConfig:
    target_pressure_torr: float                 # 목표 압력(이하 도달 시 성공)
    tsp_on_seconds: float = 75.0                # "1분 10~20초" 권장 → 기본 75초
    off_wait_seconds: float = 150.0             # TSP OFF 후 대기시간(2분 30초)
    max_cycles: int = 10                        # 최대 반복 횟수
    ig_poll_interval: float = 1.0               # IG 폴링 간격(초)

@dataclass
class TSPProcessResult:
    ok: bool                                    # 목표 도달 여부
    cycles_used: int                            # 실제 수행한 사이클 수
    last_pressure_torr: Optional[float]         # 마지막으로 읽은 압력
    reason: str                                 # 종료 사유(성공/취소/실패 등)

# ─────────────────────────────────────────────────────────────
# 장비 인터페이스(덕 타이핑) — 실제 구현은 DI로 주입
# ─────────────────────────────────────────────────────────────

class TSPControllerLike:
    async def start(self) -> bool: ...
    async def stop(self) -> bool: ...

class IGControllerLike:
    async def ensure_on(self) -> bool: ...
    async def ensure_off(self) -> bool: ...
    async def read_pressure(self) -> float: ...

# ─────────────────────────────────────────────────────────────
# 메인 러너
# ─────────────────────────────────────────────────────────────

class TSPBurstRunner:
    def __init__(
        self,
        tsp: TSPControllerLike,
        ig: IGControllerLike,
        *,
        log_cb: Optional[Callable[[str, str], None]] = None,   # (src, msg)
        state_cb: Optional[Callable[[str], None]] = None,       # 공정 상태 텍스트
    ) -> None:
        self.tsp = tsp
        self.ig = ig
        self.log = log_cb or (lambda src, msg: None)
        self.state = state_cb or (lambda text: None)
        self._last_pressure: Optional[float] = None

    def _log(self, src: str, msg: str) -> None:
        self.log(src, msg)

    def _state(self, text: str) -> None:
        self.state(text)

    async def _pressure_watcher(self, *, stop_event: asyncio.Event,
                                goal_event: asyncio.Event, target_torr: float,
                                poll_interval: float) -> None:
        """IG를 주기적으로 읽어 목표 압력 도달 시 goal_event set.
        stop_event가 set되면 종료.
        """
        self._log("IG", f"Watcher 시작 (interval={poll_interval}s, target={target_torr:.3e} Torr)")
        try:
            while not stop_event.is_set():
                try:
                    p = await self.ig.read_pressure()
                    self._last_pressure = p
                    self._log("IG", f"현재 압력: {p:.3e} Torr")
                    if p <= target_torr:
                        self._log("IG", f"목표 압력 도달: {p:.3e} ≤ {target_torr:.3e}")
                        goal_event.set()
                        await asyncio.sleep(poll_interval)
                except Exception as e:
                    self._log("IG", f"읽기 오류: {e!r}")
                await asyncio.sleep(poll_interval)
        finally:
            self._log("IG", "Watcher 종료")

    async def _safe_tsp_off(self) -> None:
        try:
            ok = await self.tsp.stop()
            self._log("TSP", f"OFF 명령 전송 → {'OK' if ok else 'NG'}")
        except Exception as e:
            self._log("TSP", f"OFF 예외: {e!r}")

    async def _ensure_ig_off(self) -> None:
        try:
            ok = await self.ig.ensure_off()
            self._log("IG", f"OFF → {'OK' if ok else 'NG'}")
        except Exception as e:
            self._log("IG", f"OFF 예외: {e!r}")

    async def run(self, cfg: TSPProcessConfig, *,
                  cancel_event: Optional[asyncio.Event] = None) -> TSPProcessResult:
        cancel_event = cancel_event or asyncio.Event()
        self._state("TSP 공정 준비 중… (IG ON)")

        # 0) IG ON 보장
        try:
            ok = await self.ig.ensure_on()
        except Exception as e:
            self._log("IG", f"ensure_on 예외: {e!r}")
            return TSPProcessResult(False, 0, self._last_pressure, "IG ensure_on exception")
        if not ok:
            return TSPProcessResult(False, 0, self._last_pressure, "IG ensure_on failed")
        self._log("IG", "ON 보장 완료")

        stop_watch = asyncio.Event()
        goal = asyncio.Event()
        watcher = asyncio.create_task(
            self._pressure_watcher(stop_event=stop_watch, goal_event=goal,
                                   target_torr=cfg.target_pressure_torr,
                                   poll_interval=cfg.ig_poll_interval)
        )

        try:
            for cycle in range(1, cfg.max_cycles + 1):
                if cancel_event.is_set():
                    self._state("사용자 취소: 안전 종료 중…")
                    await self._safe_tsp_off()
                    await self._ensure_ig_off()
                    return TSPProcessResult(False, cycle - 1, self._last_pressure, "cancelled")

                self._state(f"[{cycle}/{cfg.max_cycles}] TSP ON (목표={cfg.target_pressure_torr:.3e} Torr)")
                try:
                    on_ok = await self.tsp.start()
                except Exception as e:
                    self._log("TSP", f"ON 예외: {e!r}")
                    on_ok = False
                if not on_ok:
                    self._state("TSP ON 실패 → 공정 중단")
                    await self._ensure_ig_off()
                    return TSPProcessResult(False, cycle - 1, self._last_pressure, "tsp start failed")

                # ON 구간
                self._log("TSP", f"ON 유지 {cfg.tsp_on_seconds:.1f}s 동안 모니터링")
                try:
                    done, _ = await asyncio.wait({goal.wait()}, timeout=cfg.tsp_on_seconds)
                except Exception as e:
                    self._log("PROC", f"wait 예외: {e!r}")
                    done = set()

                if goal.is_set():
                    self._state("목표 압력 도달 → 즉시 OFF")
                    await self._safe_tsp_off()
                    await self._ensure_ig_off()
                    return TSPProcessResult(True, cycle, self._last_pressure, "goal reached during ON")

                # 목표 미달 → OFF + 대기
                self._state(f"[{cycle}] TSP OFF & 대기 {cfg.off_wait_seconds:.1f}s")
                await self._safe_tsp_off()

                try:
                    done, _ = await asyncio.wait({goal.wait()}, timeout=cfg.off_wait_seconds)
                except Exception as e:
                    self._log("PROC", f"wait 예외: {e!r}")
                    done = set()

                if goal.is_set():
                    self._state("대기 중 목표 압력 도달 → 종료")
                    await self._ensure_ig_off()
                    return TSPProcessResult(True, cycle, self._last_pressure, "goal reached during OFF wait")

                self._log("PROC", f"[{cycle}] 목표 미달 → 다음 사이클 진행")

            # 최대 반복 도달
            self._state("최대 반복에 도달 (목표 미달) → 종료")
            await self._ensure_ig_off()
            return TSPProcessResult(False, cfg.max_cycles, self._last_pressure, "max cycles reached")

        finally:
            stop_watch.set()
            try:
                await asyncio.wait_for(watcher, timeout=2.0)
            except Exception:
                watcher.cancel()
            await self._safe_tsp_off()

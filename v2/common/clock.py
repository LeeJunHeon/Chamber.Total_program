"""시계와 1초 tick 스케줄러 (장비 공통).

- RealClock   : time.monotonic / time.time. 정해진 시각까지는 time.sleep(고해상도, 3.11+)으로 기다린다.
- VirtualClock: 시험용 가상 시각. 등록된 스레드(register)가 모두 기다리는 중이면 가장 이른 기한으로 시각을 옮긴다
                — 시험은 실제 시간을 기다리지 않고, 장비 지연·마감·백오프가 가상 시각으로 정확히 재현된다.
두 시계는 같은 조건 변수(cond)를 공유 상태 보호에 쓴다(ClockQueue·스냅샷 묶기). wait_until(target, predicate) 가
유일한 대기 방법이다 — 상태를 바꾼 쪽은 clock.notify() 를 부른다.

VirtualClock.close(): 시험이 끝날 때(conftest) 남은 참가자 스레드를 끝낸다 — 기다리던 스레드는 ClockClosed 로 빠져나와
start_thread 의 감싸개가 조용히 끝낸다. 실패한 시험이 worker 를 멈추지 못하고 끝나면 그 worker 가 다음 시험 중에
"진행 없음"(ClockDeadlock, 10 s)으로 경고를 남기던 문제(5-3 중간 실행) — 시험 정리의 문제이고 운전 코드의 잠금 문제가 아니다.

TickScheduler: t_n = anchor + n·period (anchor = 시작 뒤 첫 정각 초). 매 tick 의 목표 monotonic 시각을
그때의 (wall − monotonic) 차이로 다시 계산한다 — 상대 sleep 을 누적하지 않으므로 밀림이 쌓이지 않는다.
처리가 밀려 목표를 한 period 이상 지나면 놓친 tick 을 건너뛰고 skipped 로 기록한다.
"""
from __future__ import annotations

import heapq
import itertools
import math
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional


class ClockDeadlock(RuntimeError):
    """가상 시계에서 모든 스레드가 기한 없이 기다림 — 시험 설계 오류."""


class ClockClosed(BaseException):
    """close() 한 가상 시계에서 기다림 — 참가자 스레드를 끝내는 신호(Exception 이 아니라 일반 except 에 잡히지 않는다)."""


class RealClock:
    virtual = False

    def __init__(self) -> None:
        self.cond = threading.Condition(threading.RLock())

    def monotonic(self) -> float:
        return time.monotonic()

    def wall(self) -> float:
        return time.time()

    def register(self) -> None:          # 가상 시계와 같은 API
        pass

    def unregister(self) -> None:
        pass

    def hold(self):
        import contextlib
        return contextlib.nullcontext()

    def start_thread(self, target: Callable[[], None], name: str) -> threading.Thread:
        t = threading.Thread(target=target, name=name, daemon=True)
        t.start()
        return t

    def notify(self) -> None:
        with self.cond:
            self.cond.notify_all()

    def sleep_until(self, target: float) -> None:
        """정확한 시각 대기(조건 없음) — 틱 시작에 쓴다."""
        while True:
            rem = target - time.monotonic()
            if rem <= 0:
                return
            time.sleep(rem)

    def wait_until(self, target: Optional[float], predicate: Optional[Callable[[], bool]] = None) -> bool:
        """predicate 가 참이 되면 True, target 을 지나면 False. target=None 이면 predicate 만 기다림."""
        with self.cond:
            while True:
                if predicate is not None and predicate():
                    return True
                if target is None:
                    self.cond.wait(1.0)
                    continue
                rem = target - time.monotonic()
                if rem <= 0:
                    return False
                self.cond.wait(rem)


class VirtualClock:
    virtual = True

    def __init__(self, start_wall: float = 1_760_000_000.25, deadlock_real_sec: float = 10.0) -> None:
        self.cond = threading.Condition(threading.RLock())
        self._now = 0.0
        self._wall0 = float(start_wall)
        self._parts: set = set()
        self._waiting: dict = {}
        self._pending = 0          # start_thread 로 시작했지만 아직 등록 전인 스레드
        self._deadlock_real_sec = deadlock_real_sec
        self._closed = False

    def monotonic(self) -> float:
        return self._now

    def wall(self) -> float:
        return self._wall0 + self._now

    def register(self) -> None:
        with self.cond:
            self._parts.add(threading.get_ident())

    def unregister(self) -> None:
        with self.cond:
            ident = threading.get_ident()
            self._parts.discard(ident)
            self._waiting.pop(ident, None)
            self._maybe_advance()
            self.cond.notify_all()

    def notify(self) -> None:
        with self.cond:
            self.cond.notify_all()

    def start_thread(self, target: Callable[[], None], name: str) -> threading.Thread:
        """참가자 스레드 시작 — 등록 전에도 '실행 중'으로 세어 시각이 먼저 넘어가지 않게 한다."""
        with self.cond:
            self._pending += 1

        def run():
            with self.cond:
                self._pending -= 1
                self._parts.add(threading.get_ident())
            try:
                target()
            except ClockClosed:
                pass                       # 시험이 끝나 시계를 닫음 — 조용히 끝
            finally:
                self.unregister()

        t = threading.Thread(target=run, name=name, daemon=True)
        t.start()
        return t

    def hold(self):
        """with clock.hold(): 블록 안에서는 시각을 옮기지 않는다 — 여러 스레드를 함께 시작할 때."""
        clock = self

        class _Hold:
            def __enter__(self_inner):
                with clock.cond:
                    clock._pending += 1

            def __exit__(self_inner, *exc):
                with clock.cond:
                    clock._pending -= 1
                    clock._maybe_advance()
                    clock.cond.notify_all()
                return False

        return _Hold()

    def close(self) -> None:
        """기다리는 참가자 스레드를 모두 끝낸다(시험 정리용). 닫힌 뒤의 wait_until 은 ClockClosed."""
        with self.cond:
            self._closed = True
            self.cond.notify_all()

    def advance(self, dt: float) -> None:
        """등록 안 된 시험 코드가 직접 시각을 옮길 때."""
        with self.cond:
            self._now += dt
            self.cond.notify_all()

    def sleep_until(self, target: float) -> None:
        self.wait_until(target)

    def wait_until(self, target: Optional[float], predicate: Optional[Callable[[], bool]] = None) -> bool:
        ident = threading.get_ident()
        with self.cond:
            if ident not in self._parts:
                raise RuntimeError("VirtualClock: register() 하지 않은 스레드가 기다림")
            try:
                while True:
                    if self._closed:
                        raise ClockClosed()
                    if predicate is not None and predicate():
                        return True
                    if target is not None and self._now >= target:
                        return False
                    self._waiting[ident] = (target, predicate)
                    self._maybe_advance()
                    if predicate is not None and predicate():
                        return True
                    if target is not None and self._now >= target:
                        return False
                    if not self.cond.wait(self._deadlock_real_sec) and not self._closed:
                        raise ClockDeadlock(f"가상 시계 {self._now:.3f}s 에서 {self._deadlock_real_sec}s 동안 진행 없음")
                    self._waiting.pop(ident, None)
            finally:
                self._waiting.pop(ident, None)

    def _maybe_advance(self) -> None:
        # 등록된 스레드가 모두 기다리는 중일 때만 시각을 옮긴다
        if self._pending or not self._parts:
            return
        if any(p not in self._waiting for p in self._parts):
            return
        for tgt, pred in self._waiting.values():
            if pred is not None and pred():
                self.cond.notify_all()
                return
        targets = [t for t, _ in self._waiting.values() if t is not None]
        if not targets:
            raise ClockDeadlock("가상 시계: 모든 스레드가 기한 없이 기다림")
        nxt = min(targets)
        if nxt > self._now:
            self._now = nxt
        self.cond.notify_all()


class ClockQueue:
    """우선순위 + not_before 가 있는 큐. 숫자가 작을수록 먼저. 같은 우선순위는 넣은 순서."""

    def __init__(self, clock) -> None:
        self._clock = clock
        self._heap: list = []
        self._cnt = itertools.count()

    def put(self, priority: int, item: Any, not_before: Optional[float] = None) -> None:
        with self._clock.cond:
            heapq.heappush(self._heap, (priority, next(self._cnt), not_before, item))
            self._clock.cond.notify_all()

    def _ready_index(self) -> Optional[int]:
        now = self._clock.monotonic()
        best = None
        for i, (p, c, nb, _) in enumerate(self._heap):
            if nb is not None and nb > now:
                continue
            if best is None or (p, c) < self._heap[best][:2]:
                best = i
        return best

    def _next_not_before(self) -> Optional[float]:
        now = self._clock.monotonic()
        nbs = [nb for _, _, nb, _ in self._heap if nb is not None and nb > now]
        return min(nbs) if nbs else None

    def get(self, until: Optional[float], stop: Callable[[], bool] = lambda: False) -> Optional[Any]:
        """준비된 항목 하나. until 까지 없으면 None. stop() 이 참이면 None."""
        with self._clock.cond:
            while True:
                if stop():
                    return None
                i = self._ready_index()
                if i is not None:
                    item = self._heap[i][3]
                    self._heap[i] = self._heap[-1]
                    self._heap.pop()
                    heapq.heapify(self._heap)
                    return item
                nb = self._next_not_before()
                target = until if nb is None else (nb if until is None else min(until, nb))
                got = self._clock.wait_until(target, lambda: stop() or self._ready_index() is not None)
                if not got and until is not None and self._clock.monotonic() >= until:
                    return None

    def drain(self, keep: Callable[[Any], bool]) -> list:
        """keep 이 거짓인 항목을 빼서 돌려준다(안전 요청이 쓰기·읽기를 버릴 때)."""
        with self._clock.cond:
            out = [e[3] for e in self._heap if not keep(e[3])]
            self._heap = [e for e in self._heap if keep(e[3])]
            heapq.heapify(self._heap)
            return out

    def __len__(self) -> int:
        with self._clock.cond:
            return len(self._heap)


@dataclass(frozen=True)
class Tick:
    seq: int            # anchor 뒤 n
    t_wall: float       # 정각 시각(벽시계)
    t_mono: float       # 같은 시각의 monotonic
    jitter_ms: float    # 실제 깨어난 시각 − 목표
    skipped: int        # 직전 tick 뒤 놓친 tick 수


class TickScheduler:
    def __init__(self, clock, period: float = 1.0) -> None:
        self.clock = clock
        self.period = float(period)
        self.anchor: Optional[float] = None    # 첫 next_tick 때 정한다(만든 시각이 아니라 — 그 사이 준비 시간이 있어도 첫 tick 이 늦지 않게)
        self._n = -1

    def _offset(self) -> float:
        return self.clock.wall() - self.clock.monotonic()

    def next_tick(self) -> Tick:
        if self.anchor is None:
            w = self.clock.wall()
            self.anchor = math.floor(w / self.period + 1e-9) * self.period + self.period   # 다음 정각
        n = self._n + 1
        target_wall = self.anchor + n * self.period
        self.clock.sleep_until(target_wall - self._offset())
        now_wall = self.clock.wall()
        skipped = 0
        late = now_wall - target_wall
        if late >= self.period:
            n_now = int(math.floor((now_wall - self.anchor) / self.period + 1e-9))
            skipped = n_now - n
            n = n_now
            target_wall = self.anchor + n * self.period
            late = now_wall - target_wall
        self._n = n
        return Tick(seq=n, t_wall=target_wall, t_mono=target_wall - self._offset(),
                    jitter_ms=late * 1000.0, skipped=skipped)

    def ticks(self, count: Optional[int] = None) -> Iterator[Tick]:
        i = 0
        while count is None or i < count:
            yield self.next_tick()
            i += 1

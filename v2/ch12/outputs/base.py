"""기록 출력 공통 — 큐에 넣기만 하는 호출과 쓰기 스레드 하나(v2/common/recorder.py 방식).

- put(fn, *args): 큐에 넣기만 한다(가득 차면 버리고 dropped 를 센다 — tick·제어는 절대 기다리지 않음).
- 쓰기 스레드: 넣은 순서대로 fn(*args) 를 부른다. 예외는 삼키고 errors 에 남긴다(기록 실패가 공정을 막지 않는다 — v1 도
  모든 기록 진입점이 예외를 삼켰다: util/log_hub.py·controller/data_logger.py:633-641·util/system_log.py:57-68).
- flush(timeout): 큐가 다 쓰일 때까지(시험·종료용). stop(): 남은 것을 쓰고 끝낸다.
- 여러 출력이 쓰기 스레드 하나를 같이 쓸 수 있다(조립에서 하나 만들어 나눠 줌). 따로 주지 않으면 자기 것을 만든다.

시각: 각 모듈은 now(벽시계 datetime 을 돌려주는 함수)를 받는다. 시각은 '부른 쪽에서' 잡아 큐에 함께 넣는다 — v1 도
줄을 만든 순간의 시각을 썼다(runtime/chamber_runtime.py:7186-7190). 시험은 고정 시계를 넣어 바이트 비교한다.
"""
from __future__ import annotations

import collections
import queue
import threading
import time
from datetime import datetime
from typing import Any, Callable, Deque, Optional

MAX_QUEUE = 20000          # 1초 1행 출력 여럿이 몇 시간 막혀도 버리지 않는 양(v2/common/recorder.py 와 같은 값)
ERRORS_KEEP = 200          # 오류 목록은 최근 이만큼만


def wall_now() -> datetime:
    """기본 벽시계(시험은 고정 시계로 바꾼다)."""
    return datetime.now()


class QueueWriter:
    """쓰기 스레드 하나. put 은 기다리지 않는다."""

    def __init__(self, name: str = "outputs", max_queue: int = MAX_QUEUE) -> None:
        self.name = name
        self._q: "queue.Queue" = queue.Queue(maxsize=max_queue)
        self.dropped = 0
        self.done = 0
        self.errors: Deque[str] = collections.deque(maxlen=ERRORS_KEEP)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name=f"out-{name}", daemon=True)
        self._thread.start()

    def put(self, fn: Callable[..., Any], *args: Any) -> bool:
        if self._stop.is_set():
            self.dropped += 1
            return False
        try:
            self._q.put_nowait((fn, args))
            return True
        except queue.Full:
            self.dropped += 1
            return False

    def _loop(self) -> None:
        while True:
            try:
                item = self._q.get(timeout=0.2)
            except queue.Empty:
                if self._stop.is_set():
                    return
                continue
            try:
                if item is None:
                    return
                fn, args = item
                try:
                    fn(*args)
                    self.done += 1
                except Exception as e:      # 기록 실패는 삼킨다(공정 영향 없음)
                    self.errors.append(f"{getattr(fn, '__name__', fn)}: {e!r}")
            finally:
                self._q.task_done()

    def flush(self, timeout_s: float = 10.0) -> bool:
        t0 = time.monotonic()
        while self._q.unfinished_tasks and time.monotonic() - t0 < timeout_s:
            time.sleep(0.002)
        return not self._q.unfinished_tasks

    @property
    def pending(self) -> int:
        return self._q.unfinished_tasks

    def stop(self, timeout_s: float = 5.0) -> None:
        if self._stop.is_set():
            return
        self.flush(timeout_s)
        self._stop.set()
        try:
            self._q.put_nowait(None)
        except queue.Full:
            pass
        self._thread.join(timeout_s)

    def is_alive(self) -> bool:
        return self._thread.is_alive()


class Owned:
    """writer 를 받거나 자기 것을 만드는 출력의 공통 부분."""

    def __init__(self, writer: Optional[QueueWriter], name: str) -> None:
        self._own_writer = writer is None
        self.writer = writer if writer is not None else QueueWriter(name)

    def flush(self, timeout_s: float = 10.0) -> bool:
        return self.writer.flush(timeout_s)

    def stop_writer(self, timeout_s: float = 5.0) -> None:
        if self._own_writer:
            self.writer.stop(timeout_s)
        else:
            self.writer.flush(timeout_s)

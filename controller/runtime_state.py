# controller/runtime_state.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
from typing import Callable, Dict, List

class RuntimeState:
    """
    CH별 공정 실행 상태 전역 레지스트리.
    - set_running(ch, running): 상태 갱신(변경 시 리스너 호출)
    - is_running(ch): 현재 상태 반환
    - get_all(): {ch: bool} 사본 반환
    - running_chambers(): 실행 중인 CH 리스트
    - any_running(): 하나라도 실행 중인지
    - subscribe(cb), unsubscribe(cb): 상태 변경 콜백 (cb(ch:int, running:bool))
    """
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._running: Dict[int, bool] = {1: False, 2: False}
        self._listeners: List[Callable[[int, bool], None]] = []

    def set_running(self, ch: int, running: bool) -> None:
        ch = int(ch)
        with self._lock:
            prev = self._running.get(ch)
            self._running[ch] = bool(running)
        if prev != running:
            # 락을 잡지 않은 상태에서 콜백 호출
            for cb in list(self._listeners):
                try:
                    cb(ch, bool(running))
                except Exception:
                    pass

    def is_running(self, ch: int) -> bool:
        with self._lock:
            return bool(self._running.get(int(ch), False))

    def get_all(self) -> Dict[int, bool]:
        with self._lock:
            return dict(self._running)

    def running_chambers(self) -> list[int]:
        with self._lock:
            return [ch for ch, v in self._running.items() if v]

    def any_running(self) -> bool:
        with self._lock:
            return any(self._running.values())

    def subscribe(self, cb: Callable[[int, bool], None]) -> None:
        with self._lock:
            if cb not in self._listeners:
                self._listeners.append(cb)

    def unsubscribe(self, cb: Callable[[int, bool], None]) -> None:
        with self._lock:
            if cb in self._listeners:
                self._listeners.remove(cb)

# 전역 인스턴스
runtime_state = RuntimeState()

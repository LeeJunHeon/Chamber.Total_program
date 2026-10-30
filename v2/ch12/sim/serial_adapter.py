"""시뮬레이터를 pyserial 처럼 보이게 하는 가짜 포트 — SerialTransport(실제 시계) 경로를 실제 포트 없이 시험할 때.

in_waiting·read·write·reset_input_buffer·close 만. 시뮬레이터 시각은 공통 기준(origin)에서 잰 실제 경과 초.
"""
from __future__ import annotations

import time
from typing import List, Tuple


class SimSerial:
    def __init__(self, device, origin: float = 0.0) -> None:
        self.dev = device
        self.origin = origin
        self.buf = b""
        self.sched: List[Tuple[float, bytes]] = []
        self.closed = False
        self.written: List[bytes] = []

    def _now(self) -> float:
        return time.monotonic() - self.origin

    def _pull(self) -> None:
        now = self._now()
        keep = []
        for t, b in self.sched:
            if t <= now:
                self.buf += b
            else:
                keep.append((t, b))
        self.sched = keep

    @property
    def in_waiting(self) -> int:
        self._pull()
        return len(self.buf)

    def read(self, n: int) -> bytes:
        self._pull()
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def write(self, data: bytes) -> int:
        now = self._now()
        if not self.dev.is_connected(now):
            raise OSError(f"{self.dev.name}: 장치 끊김")
        self.written.append(bytes(data))
        self.sched += self.dev.respond(bytes(data), now)
        self.sched.sort(key=lambda x: x[0])
        return len(data)

    def reset_input_buffer(self) -> None:
        self.buf = b""
        self.sched = []

    def close(self) -> None:
        self.closed = True

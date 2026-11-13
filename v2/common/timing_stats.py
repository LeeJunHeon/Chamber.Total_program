"""장비 명령별 응답 시간 모으기(2026-11-13 D409) — 보냄 → 응답 끝, 장비·명령별.

드라이버(MFC·IG·RF Pulse·DC Pulse·TSP·STM-100)와 PLC(Modbus 트랜잭션)가 질의 하나를 끝낼 때 rec() 로 시간을 낸다. rec() 는 통신 경로를 늦추지 않는다 —
설치된 모음통(install)이 없으면 아무것도 안 하고, 있으면 deque.append 하나(잠금 없음)다. 집계·파일 쓰기는 1초 행 구독자(outputs/device_timing_csv)가
주기마다 drain() 해 기록 스레드(QueueWriter)에 넘긴다.
시간은 호출한 쪽 시계의 monotonic 차이(실장비는 실시간, 시험은 가상 시계).

집계(summarize): 장비·명령마다 개수(시간 초과 포함)·최소·평균·최대·p95(ms, 정상 응답만 — 가장 가까운 순위 방식 ceil(0.95·n))·시간 초과 수.
정상 응답이 하나도 없으면 시간 칸은 빈 칸.
"""
from __future__ import annotations

import math
from collections import deque
from typing import Deque, Dict, List, Optional, Sequence, Tuple

_SINK: Optional["TimingStats"] = None


def install(stats: Optional["TimingStats"]) -> None:
    global _SINK
    _SINK = stats


def uninstall(stats: Optional["TimingStats"] = None) -> None:
    global _SINK
    if stats is None or _SINK is stats:
        _SINK = None


def rec(device: str, cmd: str, dur_s: float, timeout: bool = False) -> None:
    s = _SINK
    if s is not None:
        s.add(device, cmd, dur_s, timeout)


class TimingStats:
    def __init__(self, maxlen: int = 200_000) -> None:
        self._q: Deque[Tuple[str, str, float, bool]] = deque(maxlen=maxlen)
        self.added = 0

    def add(self, device: str, cmd: str, dur_s: float, timeout: bool = False) -> None:
        self._q.append((str(device), str(cmd), float(dur_s), bool(timeout)))     # deque.append 는 원자적 — 잠금 없음
        self.added += 1

    def drain(self) -> List[Tuple[str, str, float, bool]]:
        out = []
        q = self._q
        while True:
            try:
                out.append(q.popleft())
            except IndexError:
                return out


def percentile95(sorted_ms: Sequence[float]) -> float:
    n = len(sorted_ms)
    return sorted_ms[max(0, math.ceil(0.95 * n) - 1)]


def summarize(samples: Sequence[Tuple[str, str, float, bool]]) -> List[Dict[str, object]]:
    """[{device, cmd, count, min_ms, avg_ms, max_ms, p95_ms, timeouts}] — 장비·명령 이름순."""
    by: Dict[Tuple[str, str], List[Tuple[float, bool]]] = {}
    for dev, cmd, dur, to in samples:
        by.setdefault((dev, cmd), []).append((dur, to))
    rows: List[Dict[str, object]] = []
    for (dev, cmd) in sorted(by):
        items = by[(dev, cmd)]
        ok = sorted(d * 1000.0 for d, to in items if not to)
        row: Dict[str, object] = {"device": dev, "cmd": cmd, "count": len(items), "timeouts": sum(1 for _d, to in items if to),
                                  "min_ms": None, "avg_ms": None, "max_ms": None, "p95_ms": None}
        if ok:
            row.update(min_ms=ok[0], avg_ms=sum(ok) / len(ok), max_ms=ok[-1], p95_ms=percentile95(ok))
        rows.append(row)
    return rows

"""간단한 가짜 장비 (장비 공통) — worker·스냅샷 묶기·기록 시험과 tick_bench 용. 실제 장비 모델은 다음 단계.

LineDevice: 줄 단위 ASCII 프로토콜
  "R <이름>\\r"         → "<이름>=<값>\\r"           (값은 values 의 함수(now) 또는 상수)
  "W <이름>=<값>\\r"    → "OK\\r"                      (writes 에 기록)
  "M <이름> <이름>…\\r" → "<값>,<값>…\\r"              (여러 값 한 번에)
고장 주입(Fault): 시각 구간 [start, end) 동안
  no_response — 응답 없음 / delay — 응답 지연 추가 / disconnect — 장치 없음(열기·쓰기 실패) /
  garbage — 깨진 응답(종료 문자는 유지)
응답 지연 = latency + jitter(now) + delay 고장.
"""
from __future__ import annotations

import collections
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

Value = Union[float, int, str, Callable[[float], Any]]
SIM_LOG_KEEP = 50_000      # 시뮬레이터가 받은 요청 기록(시험용) 상한 — 실행 프로그램 sim 을 며칠 켜 둬도 늘지 않게(PLC 약 1 시간치)


def sim_log() -> "collections.deque":
    """받은 요청 기록 — (시각, 바이트) 길이 제한 링 버퍼."""
    return collections.deque(maxlen=SIM_LOG_KEEP)


@dataclass(frozen=True)
class Fault:
    kind: str            # no_response / delay / disconnect / garbage
    start: float
    end: float
    extra_s: float = 0.0

    def active(self, now: float) -> bool:
        return self.start <= now < self.end


class SimDevice:
    def __init__(self, name: str, latency_s: float = 0.05,
                 jitter: Optional[Callable[[float], float]] = None) -> None:
        self.name = name
        self.latency_s = latency_s
        self.jitter = jitter
        self.faults: List[Fault] = []
        self.rx_log: "collections.deque[Tuple[float, bytes]]" = sim_log()

    def add_fault(self, kind: str, start: float, end: float, extra_s: float = 0.0) -> None:
        self.faults.append(Fault(kind, start, end, extra_s))

    def _active(self, kind: str, now: float) -> List[Fault]:
        return [f for f in self.faults if f.kind == kind and f.active(now)]

    def is_connected(self, now: float) -> bool:
        return not self._active("disconnect", now)

    def next_link_change(self, now: float) -> Optional[float]:
        ts = [t for f in self.faults if f.kind == "disconnect" for t in (f.start, f.end) if t > now]
        return min(ts) if ts else None

    def respond(self, data: bytes, now: float) -> List[Tuple[float, bytes]]:
        self.rx_log.append((now, data))
        if self._active("no_response", now):
            return []
        out = self.handle(data, now)
        if out is None:
            return []
        if self._active("garbage", now):
            out = b"#" * max(1, len(out) - 1) + out[-1:]     # 종료 문자는 그대로, 내용만 깨짐
        delay = self.latency_s + (self.jitter(now) if self.jitter else 0.0)
        delay += sum(f.extra_s for f in self._active("delay", now))
        return [(now + delay, out)]

    def handle(self, data: bytes, now: float) -> Optional[bytes]:
        raise NotImplementedError


class LineDevice(SimDevice):
    def __init__(self, name: str, values: Dict[str, Value], latency_s: float = 0.05,
                 jitter: Optional[Callable[[float], float]] = None) -> None:
        super().__init__(name, latency_s, jitter)
        self.values = dict(values)
        self.writes: List[Tuple[float, str, str]] = []

    def _val(self, k: str, now: float) -> str:
        v = self.values[k]
        return str(v(now) if callable(v) else v)

    def handle(self, data: bytes, now: float) -> Optional[bytes]:
        try:
            line = data.decode("ascii").strip()
        except UnicodeDecodeError:
            return b"ERR\r"
        if line.startswith("R "):
            k = line[2:]
            return f"{k}={self._val(k, now)}\r".encode() if k in self.values else b"ERR\r"
        if line.startswith("M "):
            ks = line[2:].split()
            if not all(k in self.values for k in ks):
                return b"ERR\r"
            return (",".join(self._val(k, now) for k in ks) + "\r").encode()
        if line.startswith("W ") and "=" in line:
            k, v = line[2:].split("=", 1)
            self.values[k] = v
            self.writes.append((now, k, v))
            return b"OK\r"
        return b"ERR\r"


# ───────────── 가짜 드라이버 함수(Session 을 받는다) ─────────────
def line_read_many(session, block: str, names: List[str]) -> Dict[str, float]:
    """"M a b c" 한 번으로 여러 값 → {"block.a": float, …}."""
    session.write(("M " + " ".join(names) + "\r").encode())
    raw = session.read_until(b"\r").decode("ascii").strip()
    parts = raw.split(",")
    if len(parts) != len(names):
        raise ValueError(f"응답 형식 오류: {raw!r}")
    return {f"{block}.{n}": float(p) for n, p in zip(names, parts)}


def line_write(session, name: str, value: Any) -> None:
    session.write(f"W {name}={value}\r".encode())
    if session.read_until(b"\r") != b"OK\r":
        raise ValueError("쓰기 거부")

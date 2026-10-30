r"""OES 링크 — 워커가 쓰는 로컬 CSV 를 따라 읽기(tail). v1 device/oes.py:1328-1458 _tail_csv 를 스레드로.

- CSV 가 생기고 크기 > 0 일 때까지 OES_CSV_TAIL_S(0.1 s) 마다 확인. 워커/daemon 이 먼저 끝나면 원인 로그 후 끝.
  5 s 마다 '[OES] CSV 생성 대기중...', OES_CSV_CREATE_TIMEOUT_S(60 s) 넘으면 워커(kill)/daemon(강제 종료).
- 첫 줄 머리글의 2번째 칸부터 x(파장 — 하나라도 숫자가 아니면 인덱스), 이후 줄마다 2번째 칸부터 y → on_data(x, y)(목록).
  표본을 볼 때마다 행 수·마지막 표본 시각(1초 행 sample_age_s) 갱신. one-shot 워커가 끝나고 3 s 새 줄이 없으면 끝.
"""
from __future__ import annotations

import csv
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class _Tail:
    path: Path
    stop: bool = False
    thread: Optional[threading.Thread] = None
    x: Optional[list] = None
    extra: Dict[str, Any] = field(default_factory=dict)


class TailMixin:
    def _start_tail(self, path: Path) -> None:
        tail = _Tail(path)
        self._tail = tail
        tail.thread = self.clock.start_thread(lambda: self._tail_run(tail), f"oes{self.ch}-tail")

    def _stop_tail(self) -> None:
        tail, self._tail = self._tail, None
        if tail is None:
            return
        tail.stop = True
        self.clock.notify()
        if tail.thread is not None and tail.thread is not threading.current_thread():
            tail.thread.join(2.0)

    def _sleep(self, tail: _Tail, s: float) -> bool:
        """s 초 쉼. 그 사이 멈추라면 False."""
        self.clock.wait_until(self.clock.monotonic() + s, lambda: tail.stop)
        return not tail.stop

    def _tail_run(self, tail: _Tail) -> None:
        poll = self._f("OES_CSV_TAIL_S", 0.1)
        t0 = last_log = self.clock.monotonic()
        path = tail.path
        while not tail.stop:
            try:
                if path.exists() and path.stat().st_size > 0:
                    break
            except OSError:
                pass
            p = self._proc
            if p is not None and p.exited:
                self._status(f"[OES] CSV 미생성 상태로 워커 종료됨 rc={p.returncode} path={path}")
                return
            d = self._daemon
            if d is not None and d.exited:
                self._status(f"[OES] CSV 미생성 상태로 daemon 종료됨 rc={d.returncode} path={path}")
                return
            now = self.clock.monotonic()
            if now - last_log > 5.0:
                last_log = now
                self._status(f"[OES] CSV 생성 대기중... {(last_log - t0):.1f}s path={path}")
            if now - t0 > self._f("OES_CSV_CREATE_TIMEOUT_S", 60.0):
                self._status(f"[OES] CSV 생성 대기 TIMEOUT({self._f('OES_CSV_CREATE_TIMEOUT_S', 60.0):g}s) path={path} → 워커/daemon kill")
                if p is not None and p.alive():
                    p.kill()
                    p.wait(5.0)
                    return
                if d is not None and d.alive():
                    with self._daemon_lock:
                        self._shutdown_daemon(graceful=False)
                return
            if not self._sleep(tail, poll):
                return
        try:
            with open(path, "r", encoding="utf-8", newline="") as f:
                header: List[str] = []
                while not tail.stop:
                    line = f.readline()
                    if not line:
                        if not self._sleep(tail, poll):
                            return
                        continue
                    try:
                        header = [c.strip() for c in next(csv.reader([line]))]
                    except Exception:
                        continue
                    break
                if len(header) < 2:
                    return
                try:
                    x = [float(c) for c in header[1:]]
                except ValueError:
                    x = [float(i) for i in range(len(header) - 1)]   # 하나라도 실패하면 인덱스(길이 보장)
                tail.x = x
                last_data = self.clock.monotonic()
                while not tail.stop:
                    line = f.readline()
                    if not line:
                        p = self._proc
                        if p is not None and p.exited and self.clock.monotonic() - last_data > 3.0:
                            return
                        if not self._sleep(tail, poll):
                            return
                        continue
                    last_data = self.clock.monotonic()
                    try:
                        parts = [c.strip() for c in next(csv.reader([line]))]
                        ys = [float(c) for c in parts[1:]]
                    except Exception:
                        continue
                    if len(parts) < 2:
                        continue
                    n = min(len(x), len(ys))
                    self._rows_seen += 1
                    self._last_sample = self.clock.monotonic()
                    if self.on_data:
                        try:
                            self.on_data(x[:n], ys[:n])
                        except Exception:
                            pass
        except OSError:
            return

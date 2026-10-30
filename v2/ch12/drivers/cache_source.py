"""포트에 닿지 않는 1초 읽기 소스(챔버12 공통 도우미) — 드라이버가 이미 들고 있는 마지막 값을 행에 싣는다.

쓰는 곳: 두께 모니터 STM-100(0.5 s 읽기는 worker 안 자기 루프 — 1초 행은 마지막 값과 나이),
카메라(시리얼 포트 없음 — 촬영 스레드 상태).

SnapshotBus 는 소스마다 worker 의 submit·suppressed·connected 만 본다. CacheWorker 는 그 모양만 갖추고
읽기 함수를 bus 스레드에서 바로 실행해 결과를 돌려준다(입출력 없음 — 장비 worker 가 긴 명령 중이어도 행이 늦지 않는다).
읽기 함수가 예외를 내면 그 행은 fail(값은 마지막 값 유지), Source.enabled=False 면 off.
"""
from __future__ import annotations

from typing import Callable, Optional

from v2.common.worker import Request, Result, Status


class CacheWorker:
    """SnapshotBus 용 가짜 worker — 읽기를 그 자리에서 실행한다(세션 없음)."""

    def __init__(self, clock, connected: Optional[Callable[[], bool]] = None, name: str = "cache") -> None:
        self.clock = clock
        self.name = name
        self._connected = connected or (lambda: True)

    @property
    def connected(self) -> bool:
        try:
            return bool(self._connected())
        except Exception:
            return False

    @property
    def ready(self) -> bool:
        return self.connected

    def suppressed(self, now: Optional[float] = None) -> bool:
        return False

    def submit(self, req: Request) -> None:
        now = self.clock.monotonic()
        res = Result(req, Status.FAIL, started=now)
        try:
            res.value = req.fn(None)
            res.status = Status.OK
        except Exception as e:          # 값 없음·미연결 — 그 행은 fail
            res.status, res.reason, res.error = Status.FAIL, "LINK_DOWN", repr(e)
        res.finished = self.clock.monotonic()
        if req.on_done:
            req.on_done(res)

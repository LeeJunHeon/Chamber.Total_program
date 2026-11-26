"""절전 막기(G — 사용자 결정 2026-11-26) — 노트북이 시스템 절전(대기 모드)에 들어가 로봇 요청·공정 기록이 끊기지 않게 한다.

윈도우 SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED) — 부른 스레드가 살아 있는 동안만 유효하므로 이 모듈의 스레드 하나
(keep-awake)가 걸고 그 스레드가 푼다(ES_CONTINUOUS 만). 화면 꺼짐(ES_DISPLAY_REQUIRED)은 막지 않는다. 챔버K v2/chk/keep_awake.py 와 같은 방식.
설정 KEEP_AWAKE: always(기본 — 프로그램이 켜져 있는 동안. 로봇 요청을 늘 받아야 한다) · run(공정·Plasma Cleaning·TSP·히터 운전 중만 —
system.busy) · off. 윈도우가 아니면 아무것도 하지 않는다. 프로그램 종료 때 푼다. 걸 때·풀 때 상세 사건 한 줄씩(selfwatch keep_awake_on·
keep_awake_off). 시험은 가짜 API(api = 호출 기록 함수)로.
"""
from __future__ import annotations

import sys
import threading
from typing import Any, Callable, Mapping, Optional

ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
CHECK_S = 1.0
MODES = ("always", "run", "off")
WHY = {"always": "프로그램이 켜져 있는 동안", "run": "운전 중"}


def windows_api() -> Optional[Callable[[int], int]]:
    """윈도우면 kernel32.SetThreadExecutionState, 아니면 None."""
    if not sys.platform.startswith("win"):
        return None
    try:
        import ctypes
        f = ctypes.windll.kernel32.SetThreadExecutionState          # type: ignore[attr-defined]
        f.restype = ctypes.c_uint
        f.argtypes = [ctypes.c_uint]
        return f
    except Exception:
        return None


class KeepAwake:
    def __init__(self, cfg: Mapping[str, Any], busy: Callable[[], bool],
                 on_event: Optional[Callable[[str, str], None]] = None,
                 api: Optional[Callable[[int], int]] = None) -> None:
        self.cfg, self.busy, self.on_event = cfg, busy, on_event
        self.api = api                   # None = 윈도우가 아님(아무것도 하지 않음)
        self.held = False
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def mode(self) -> str:
        m = str(self.cfg.get("KEEP_AWAKE") or "always")
        return m if m in MODES else "always"

    def want(self) -> bool:
        m = self.mode()
        if m == "always":
            return True
        if m == "off":
            return False
        try:
            return bool(self.busy())
        except Exception:
            return self.held             # 판단 실패 — 지금 상태 그대로

    def _event(self, kind: str, detail: str) -> None:
        if self.on_event:
            try:
                self.on_event(kind, detail)
            except Exception:
                pass

    def tick(self) -> None:
        """한 번 판단 — keep-awake 스레드가 1초마다 부른다(시험은 직접)."""
        if self.api is None:
            return
        w = self.want()
        if w == self.held:
            return
        self.api(ES_CONTINUOUS | ES_SYSTEM_REQUIRED if w else ES_CONTINUOUS)
        self.held = w
        if w:
            self._event("keep_awake_on", f"윈도우 절전 막기 시작({WHY.get(self.mode(), '운전 중')})")
        else:
            self._event("keep_awake_off", "윈도우 절전 막기 끝")

    def release(self) -> None:
        """걸려 있으면 푼다 — 같은 스레드에서(_loop 끝)."""
        if self.held and self.api is not None:
            self.api(ES_CONTINUOUS)
            self.held = False
            self._event("keep_awake_off", "윈도우 절전 막기 끝(프로그램 종료)")

    def _loop(self) -> None:
        try:
            while not self._stop.is_set():
                self.tick()
                self._stop.wait(CHECK_S)
        finally:
            self.release()

    def start(self) -> None:
        if self.api is None:
            return
        self._thread = threading.Thread(target=self._loop, name="keep-awake", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)

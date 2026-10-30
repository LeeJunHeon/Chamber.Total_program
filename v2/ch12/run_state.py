"""전역 실행 상태·쿨다운·에러 래치·런 단위 MFC 사용자 — v1 controller/runtime_state.py(RuntimeState) 그대로(시계만 주입).

v1 과 같은 것
- kind "chamber"/"pc"/"tsp", 채널은 chamber·pc 1|2(2 가 아니면 1), tsp 는 0(전역 하나).
- mark_started: 실행 표시 + 시작 시각 + 직전 에러 해제. mark_finished: 실행 해제 + 종료 시각(쿨다운 기준 — 프리플라이트
  실패처럼 아무것도 안 돈 런도 찍는다, PC-101). set_running(True/False) = mark_started/mark_finished.
- 에러 래치(set_error·clear_error·has_error·last_error) — 화면 'error' 표시, 팝업을 닫으면 clear_error.
- remaining_cooldown(kind, 채널들, 초) = 그 채널들 중 가장 긴 남은 쿨다운(기록 없으면 0).
- pc_block_reason(ch, 60): PC 전역(1·2) 60 s · 같은 CH 챔버 60 s · CH1 일 때만 TSP 60 s(실행 중 판정은 하지 않음 — 부르는 쪽).
- chamber_block_reason(ch, 60): 같은 챔버·PC(1·2)·(CH1 이면) TSP 실행 중이면 거절, 쿨다운 같은 규칙.
- 런 단위 사용자(begin_run_use·end_run_use·run_users) — 시작 수락 ~ 자기 MFC 정리 끝까지. 이 등록이 남아 있는 동안 다른
  주체는 공유 MFC 를 전체 정리(대기열 폐기·초기화)하지 않는다(runtime/plasma_cleaning_runtime.py:1417-1422).
v2 에서 다른 것
- 폴링 사용자 집합(v1 acquire_shared·release_shared)은 MFC 드라이버의 poll_users 가 주인이다(drivers/mfc.py acquire_poll —
  shared.MfcPoll). 여기서는 다루지 않는다(같은 집합이 두 곳에 있지 않게). 펄스 엔드포인트 클레임은 drivers/pulse_port.PulsePorts.
- 시각은 clock.monotonic()(가상 시계 시험). 전역 싱글톤이 아니라 조립(system)이 하나 만들어 엔진들에 준다.
"""
from __future__ import annotations

import threading
from typing import Dict, Iterable, Optional, Set, Tuple

KINDS = ("chamber", "pc", "tsp")


def norm_kind(kind: str) -> str:
    k = str(kind).strip().lower()
    if k in KINDS:
        return k
    raise ValueError(f"unknown kind: {kind!r}")


def norm_channel(kind: str, ch: Optional[int]) -> int:
    if norm_kind(kind) == "tsp":
        return 0
    return 1 if int(ch or 1) != 2 else 2


class RunState:
    def __init__(self, clock) -> None:
        self.clock = clock
        self._lock = threading.RLock()
        self._running: Dict[str, Dict[int, bool]] = {"chamber": {1: False, 2: False}, "pc": {1: False, 2: False},
                                                     "tsp": {0: False}}
        self._last_start: Dict[str, Dict[int, float]] = {k: {} for k in KINDS}
        self._last_finish: Dict[str, Dict[int, float]] = {k: {} for k in KINDS}
        self._last_error: Dict[str, Dict[int, str]] = {k: {} for k in KINDS}
        self._run_users: Dict[str, Set[str]] = {}

    def _now(self) -> float:
        return float(self.clock.monotonic())

    # ───────── 실행 표시 ─────────
    def mark_started(self, kind: str, channel: Optional[int] = None) -> None:
        k = norm_kind(kind)
        ch = norm_channel(k, channel)
        with self._lock:
            self._last_error[k].pop(ch, None)
            self._running[k][ch] = True
            self._last_start[k][ch] = self._now()

    def mark_finished(self, kind: str, channel: Optional[int] = None) -> None:
        k = norm_kind(kind)
        ch = norm_channel(k, channel)
        with self._lock:
            self._running[k][ch] = False
            self._last_finish[k][ch] = self._now()

    def set_running(self, kind: str, running: bool, channel: Optional[int] = None) -> None:
        if running:
            self.mark_started(kind, channel)
        else:
            self.mark_finished(kind, channel)

    def is_running(self, kind: str, channel: Optional[int] = None) -> bool:
        k = norm_kind(kind)
        with self._lock:
            return bool(self._running[k].get(norm_channel(k, channel), False))

    def any_running(self) -> bool:
        with self._lock:
            return any(v for m in self._running.values() for v in m.values())

    def last_finished(self, kind: str, channel: Optional[int] = None) -> Optional[float]:
        k = norm_kind(kind)
        with self._lock:
            return self._last_finish[k].get(norm_channel(k, channel))

    # ───────── 에러 래치 ─────────
    def set_error(self, kind: str, channel: Optional[int], message: str) -> None:
        k = norm_kind(kind)
        with self._lock:
            self._last_error[k][norm_channel(k, channel)] = str(message)

    def clear_error(self, kind: str, channel: Optional[int] = None) -> None:
        k = norm_kind(kind)
        with self._lock:
            self._last_error[k].pop(norm_channel(k, channel), None)

    def has_error(self, kind: str, channel: Optional[int] = None) -> bool:
        k = norm_kind(kind)
        with self._lock:
            return norm_channel(k, channel) in self._last_error[k]

    def last_error(self, kind: str, channel: Optional[int] = None) -> Optional[str]:
        k = norm_kind(kind)
        with self._lock:
            return self._last_error[k].get(norm_channel(k, channel))

    # ───────── 쿨다운 ─────────
    def remaining_cooldown(self, kind: str, channels, cooldown_s: float) -> float:
        k = norm_kind(kind)
        chs = [channels] if isinstance(channels, int) else list(channels)
        now = self._now()
        out = []
        with self._lock:
            for c in chs:
                fin = self._last_finish[k].get(norm_channel(k, c))
                if fin is None:
                    continue
                rem = float(cooldown_s) - (now - fin)
                if rem > 0:
                    out.append(rem)
        return max(out) if out else 0.0

    def pc_block_reason(self, ch: int, cooldown_s: float = 60.0) -> Tuple[bool, float, str]:
        """controller/runtime_state.py:289-312."""
        rem_pc = self.remaining_cooldown("pc", [1, 2], cooldown_s)
        rem_ch = self.remaining_cooldown("chamber", ch, cooldown_s)
        rem_tsp = self.remaining_cooldown("tsp", 0, cooldown_s) if int(ch) == 1 else 0.0
        remain = max(rem_pc, rem_ch, rem_tsp, 0.0)
        if remain > 0:
            why = []
            if rem_pc > 0:
                why.append(f"PC {rem_pc:.0f}s")
            if rem_ch > 0:
                why.append(f"CH{ch} {rem_ch:.0f}s")
            if rem_tsp > 0:
                why.append(f"TSP {rem_tsp:.0f}s")
            return False, remain, "이전 공정 종료 후 1분 대기 필요: " + ", ".join(why)
        return True, 0.0, ""

    def chamber_block_reason(self, ch: int, cooldown_s: float = 60.0) -> Tuple[bool, float, str]:
        """controller/runtime_state.py:314-343."""
        if self.is_running("chamber", ch):
            return False, 0.0, f"CH{ch} 공정이 이미 실행 중"
        if self.is_running("pc", 1) or self.is_running("pc", 2):
            return False, 0.0, "Plasma Cleaning 실행 중"
        if int(ch) == 1 and self.is_running("tsp", 0):
            return False, 0.0, "TSP 실행 중"
        rem_ch = self.remaining_cooldown("chamber", ch, cooldown_s)
        rem_pc = self.remaining_cooldown("pc", [1, 2], cooldown_s)
        rem_tsp = self.remaining_cooldown("tsp", 0, cooldown_s) if int(ch) == 1 else 0.0
        remain = max(rem_ch, rem_pc, rem_tsp, 0.0)
        if remain > 0:
            why = []
            if rem_ch > 0:
                why.append(f"CH{ch} {rem_ch:.0f}s")
            if rem_pc > 0:
                why.append(f"PC {rem_pc:.0f}s")
            if rem_tsp > 0:
                why.append(f"TSP {rem_tsp:.0f}s")
            return False, remain, "쿨다운 대기: " + ", ".join(why)
        return True, 0.0, ""

    # ───────── 런 단위 MFC 사용자 ─────────
    def begin_run_use(self, owner: str, resources: Iterable[str]) -> None:
        rs = {str(r) for r in (resources or []) if str(r)}
        with self._lock:
            if rs:
                self._run_users[str(owner)] = rs
            else:
                self._run_users.pop(str(owner), None)

    def end_run_use(self, owner: str) -> None:
        with self._lock:
            self._run_users.pop(str(owner), None)

    def run_users(self, resource: str) -> Set[str]:
        with self._lock:
            return {o for o, rs in self._run_users.items() if str(resource) in rs}

    # ───────── 보기 ─────────
    def snapshot(self) -> dict:
        with self._lock:
            return {"running": {k: dict(v) for k, v in self._running.items()},
                    "last_start_mono": {k: dict(v) for k, v in self._last_start.items()},
                    "last_finish_mono": {k: dict(v) for k, v in self._last_finish.items()},
                    "last_error": {k: dict(v) for k, v in self._last_error.items()},
                    "run_users": {o: sorted(rs) for o, rs in self._run_users.items() if rs}}

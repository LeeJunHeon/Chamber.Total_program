# -*- coding: utf-8 -*-
"""
runtime_state.py — 전역 런타임 상태/쿨다운 관리

목표
- CHAMBER(1/2), PC(Plasma Cleaning, 1/2), TSP(글로벌) 현재 동작 여부와
  최근 종료 시각을 일관되게 기록/조회
- 외부(Host 서버/핸들러)에서 "현재 다른 공정이 돌고 있는지" 빠르게 판정
- PC 시작 전 60초 대기(요청사항)와 교차 검사(CH1/CH2) 지원

특징
- thread-safe(RLock) + time.monotonic 기반
- kind는 "chamber" / "pc" / "tsp"
- channel은 chamber/pc는 1 또는 2, tsp는 0(글로벌)로 통일
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Optional, Iterable, Tuple
import time
import threading


# ---- 내부 유틸 --------------------------------------------------------------

def _now_monotonic() -> float:
    return time.monotonic()


def _norm_kind(kind: str) -> str:
    k = str(kind).strip().lower()
    if k in ("chamber", "pc", "tsp"):
        return k
    raise ValueError(f"unknown kind: {kind!r}")


def _norm_channel(kind: str, ch: Optional[int]) -> int:
    k = _norm_kind(kind)
    if k == "tsp":
        # TSP는 글로벌 단일 트랙으로 관리 (채널 0)
        return 0
    c = 1 if int(ch or 1) != 2 else 2
    return c


# ---- 상태 컨테이너 ----------------------------------------------------------

@dataclass
class RuntimeState:
    """
    전역 싱글톤으로 사용.
    - _running[kind][ch] = bool
    - _last_start_mono[kind][ch] = monotonic timestamp
    - _last_finish_mono[kind][ch] = monotonic timestamp
    """
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False)

    _running: Dict[str, Dict[int, bool]] = field(
        default_factory=lambda: {"chamber": {1: False, 2: False}, "pc": {1: False, 2: False}, "tsp": {0: False}}
    )
    _last_start_mono: Dict[str, Dict[int, float]] = field(
        default_factory=lambda: {"chamber": {}, "pc": {}, "tsp": {}}
    )
    _last_finish_mono: Dict[str, Dict[int, float]] = field(
        default_factory=lambda: {"chamber": {}, "pc": {}, "tsp": {}}
    )

    # ---------- 기본 마킹 API ----------

    def mark_started(self, kind: str, channel: Optional[int] = None) -> None:
        k = _norm_kind(kind)
        ch = _norm_channel(k, channel)
        now = _now_monotonic()
        with self._lock:
            self._running.setdefault(k, {})[ch] = True
            self._last_start_mono.setdefault(k, {})[ch] = now

    def mark_finished(self, kind: str, channel: Optional[int] = None) -> None:
        k = _norm_kind(kind)
        ch = _norm_channel(k, channel)
        now = _now_monotonic()
        with self._lock:
            self._running.setdefault(k, {})[ch] = False
            self._last_finish_mono.setdefault(k, {})[ch] = now

    def set_running(self, kind: str, running: bool, channel: Optional[int] = None) -> None:
        """상태를 직접 지정(특수 상황용). 가능하면 mark_started/mark_finished 권장."""
        if running:
            self.mark_started(kind, channel)
        else:
            self.mark_finished(kind, channel)

    # ---------- 조회/스냅샷 ----------

    def is_running(self, kind: str, channel: Optional[int] = None) -> bool:
        k = _norm_kind(kind)
        ch = _norm_channel(k, channel)
        with self._lock:
            return bool(self._running.get(k, {}).get(ch, False))

    def any_running(self) -> bool:
        """모든 kind/ch 중 하나라도 동작 중이면 True"""
        with self._lock:
            for k, chmap in self._running.items():
                for _, v in chmap.items():
                    if v:
                        return True
        return False

    def get_states(self) -> Dict[str, Dict[int, bool]]:
        """깊은 복사 대신 안전한 얕은 복사로 반환(읽기 전용 용도)."""
        with self._lock:
            return {
                k: dict(v) for k, v in self._running.items()
            }

    def last_finished(self, kind: str, channel: Optional[int] = None) -> Optional[float]:
        """최근 종료 모노토닉 시각(없으면 None)."""
        k = _norm_kind(kind)
        ch = _norm_channel(k, channel)
        with self._lock:
            return self._last_finish_mono.get(k, {}).get(ch)

    # ---------- 쿨다운 계산 ----------

    def remaining_cooldown(self, kind: str, channels: int | Iterable[int], cooldown_s: float) -> float:
        """
        kind(예: "pc" 또는 "chamber")에 대해 채널들의 남은 쿨다운(s) 중 최대값을 반환.
        channels: 단일 int 또는 int iterable
        """
        k = _norm_kind(kind)
        now = _now_monotonic()
        if isinstance(channels, int):
            channels = [channels]
        remain_max = 0.0
        with self._lock:
            for ch in channels:
                last = self._last_finish_mono.get(k, {}).get(int(ch))
                if last is None:
                    continue
                elapsed = now - last
                remain = max(0.0, float(cooldown_s) - elapsed)
                if remain > remain_max:
                    remain_max = remain
        return remain_max

    # ---------- 요청사항: PC 시작 전 고급 차단 로직 ----------

    def pc_block_reason(
        self,
        selected_ch: int,
        cooldown_s: float = 60.0,
    ) -> Tuple[bool, float, str]:
        """
        Plasma Cleaning 시작 전 차단 사유/남은 시간 계산:
        - (1) PC 자체: selected_ch의 최근 PC 종료 쿨다운
        - (2) Chamber 교차 검사:
            CH1 선택 → CH1 Chamber
            CH2 선택 → CH1, CH2 Chamber
        반환: (ok, remain, reason)
        """
        ch = 1 if int(selected_ch) != 2 else 2

        # 1) PC 자신의 쿨다운
        remain_pc = self.remaining_cooldown("pc", ch, cooldown_s)

        # 2) Chamber 쿨다운 (선택 CH에 따라 범위 다름)
        check_chs = [1] if ch == 1 else [1, 2]
        remain_chm = self.remaining_cooldown("chamber", check_chs, cooldown_s)

        remain = max(remain_pc, remain_chm)
        if remain <= 0.0:
            return True, 0.0, ""

        # 사유 문자열 구성
        secs = int(remain + 0.999)
        if remain_pc >= remain_chm:
            reason = f"이전 Plasma Cleaning 종료 후 {secs}초 추가 대기 필요"
        else:
            if ch == 1:
                reason = f"최근 CH1 공정 종료 후 {secs}초 추가 대기 필요"
            else:
                reason = f"최근 CH1/CH2 공정 종료 후 {secs}초 추가 대기 필요"
        return False, float(secs), reason

    # ---------- 덤프/디버그 ----------

    def snapshot(self) -> dict:
        """외부(Host 응답 등)로 내보내기 좋은 형태."""
        with self._lock:
            return {
                "running": {k: dict(v) for k, v in self._running.items()},
                "last_start_mono": {k: dict(v) for k, v in self._last_start_mono.items()},
                "last_finish_mono": {k: dict(v) for k, v in self._last_finish_mono.items()},
            }


# 전역 싱글톤
runtime_state = RuntimeState()

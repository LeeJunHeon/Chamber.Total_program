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
        channels: int(단일) 또는 Iterable[int] (여러 채널)
        반환: 지정된 채널들 중 '가장 긴' 남은 쿨다운(초). 기록 없으면 0.0
        """
        k = _norm_kind(kind)
        ch_list = [channels] if isinstance(channels, int) else list(channels)
        now = _now_monotonic()
        remains: list[float] = []
        with self._lock:
            for ch in ch_list:
                chn = _norm_channel(k, ch)
                last_fin = self._last_finish_mono.get(k, {}).get(chn)
                if last_fin is None:
                    continue
                rem = float(cooldown_s) - (now - last_fin)
                if rem > 0:
                    remains.append(rem)
        return max(remains) if remains else 0.0

    # ---------- PC 시작 전 차단 로직 ----------

    def pc_block_reason(self, ch: int, cooldown_s: float = 60.0) -> tuple[bool, float, str]:
        """
        Plasma Cleaning 시작 가능 여부/대기시간/사유(텍스트).
        요구사항: 동일 채널만 금지(교차 채널 허용).
        - 실행중 차단: 여기서는 하지 않음(런타임에서 동일 채널만 별도로 체크)
        - 쿨다운:
            · PC 글로벌(1,2)           : 60초
            · CH '동일 채널'(ch)        : 60초
            · (선택) TSP→CH1 영향       : ch==1일 때만 60초
        """
        # 실행중 여부는 여기서 판단하지 않는다(런타임에서 동일채널만 차단)
        rem_pc  = self.remaining_cooldown("pc", [1, 2], cooldown_s)
        rem_ch  = self.remaining_cooldown("chamber", ch, cooldown_s)
        rem_tsp = self.remaining_cooldown("tsp", 0, cooldown_s) if int(ch) == 1 else 0.0

        remain = max(rem_pc, rem_ch, rem_tsp, 0.0)
        if remain > 0:
            reasons = []
            if rem_pc  > 0: reasons.append(f"PC {rem_pc:.0f}s")
            if rem_ch  > 0: reasons.append(f"CH{ch} {rem_ch:.0f}s")
            if rem_tsp > 0: reasons.append(f"TSP {rem_tsp:.0f}s")
            return (False, remain, "이전 공정 종료 후 1분 대기 필요: " + ", ".join(reasons))

        return (True, 0.0, "")
    
    # ---------- Chamber 시작 전 차단 로직 ----------
    def chamber_block_reason(self, ch: int, cooldown_s: float = 60.0) -> tuple[bool, float, str]:
        """
        챔버 공정 시작 가능 여부/대기시간/사유를 반환.
        - 실행 중 차단: 같은 챔버 공정, PC 글로벌, (ch=1일 때) TSP
        - 쿨다운:
            · CH 자기 자신(ch)        : 60초
            · PC 글로벌(1,2)           : 60초 (PC 끝나고 1분 후 챔버 가능)
            · TSP (ch=1만)             : 60초 (TSP 끝나고 1분 후 CH1 가능)
        """
        if self.is_running("chamber", ch):
            return (False, 0.0, f"CH{ch} 공정이 이미 실행 중")
        if self.is_running("pc", 1) or self.is_running("pc", 2):
            return (False, 0.0, "Plasma Cleaning 실행 중")
        if int(ch) == 1 and self.is_running("tsp", 0):
            return (False, 0.0, "TSP 실행 중")

        rem_ch  = self.remaining_cooldown("chamber", ch, cooldown_s)
        rem_pc  = self.remaining_cooldown("pc", [1, 2], cooldown_s)
        rem_tsp = self.remaining_cooldown("tsp", 0, cooldown_s) if int(ch) == 1 else 0.0

        remain = max(rem_ch, rem_pc, rem_tsp, 0.0)
        if remain > 0:
            reasons = []
            if rem_ch  > 0: reasons.append(f"CH{ch} {rem_ch:.0f}s")
            if rem_pc  > 0: reasons.append(f"PC {rem_pc:.0f}s")
            if rem_tsp > 0: reasons.append(f"TSP {rem_tsp:.0f}s")
            return (False, remain, "쿨다운 대기: " + ", ".join(reasons))

        return (True, 0.0, "")

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

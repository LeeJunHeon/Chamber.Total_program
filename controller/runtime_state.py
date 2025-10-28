# controller/runtime_state.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import time
import threading
from dataclasses import dataclass
from typing import Dict, Optional, Iterable, Literal, Tuple

Kind = Literal["chamber", "pc"]  # 공정 종류 태그

@dataclass
class _RunFlag:
    running: bool = False

class RuntimeState:
    """
    전역 런타임 상태/쿨다운 관리.
    - get_running/set_running: 기존 호환 유지 (Chamber 런타임 등에서 사용)
    - mark_started/mark_finished: 공정 시작/종료 시각(모노토닉) 기록
    - remaining_cooldown: 최근 종료 후 남은 쿨다운(sec) 계산 (단일/복수 채널 지원)
    """
    def __init__(self) -> None:
        self._lock = threading.RLock()

        # 기존 running 플래그 (채널별)
        self._running: Dict[int, _RunFlag] = {
            1: _RunFlag(False),
            2: _RunFlag(False),
        }

        # 최근 종료 시각(모노토닉) {kind: {ch: t_mono}}
        self._last_finish_mono: Dict[Kind, Dict[int, float]] = {
            "chamber": {},
            "pc": {},
        }
        # 최근 시작 시각(선택적으로 참고) {kind: {ch: t_mono}}
        self._last_start_mono: Dict[Kind, Dict[int, float]] = {
            "chamber": {},
            "pc": {},
        }

    # ───────── 기존 호환 API ─────────
    def get_running(self, ch: int) -> bool:
        with self._lock:
            return bool(self._running.get(int(ch), _RunFlag(False)).running)

    def set_running(self, ch: int, running: bool) -> None:
        with self._lock:
            self._running.setdefault(int(ch), _RunFlag(False)).running = bool(running)

    # ───────── 신규: 시작/종료 마킹 ─────────
    def mark_started(self, kind: Kind, ch: int) -> None:
        t = time.monotonic()
        with self._lock:
            self._last_start_mono.setdefault(kind, {})[int(ch)] = t

    def mark_finished(self, kind: Kind, ch: int) -> None:
        t = time.monotonic()
        with self._lock:
            self._last_finish_mono.setdefault(kind, {})[int(ch)] = t

    # ───────── 조회/쿨다운 헬퍼 ─────────
    def last_finished(self, kind: Kind, ch: int) -> Optional[float]:
        with self._lock:
            return self._last_finish_mono.get(kind, {}).get(int(ch))

    def remaining_cooldown(
        self,
        kind: Kind,
        channels: int | Iterable[int],
        cooldown_s: float,
    ) -> float:
        """
        kind/channels 기준으로 '최근 종료 후 남은 쿨다운(sec)'의 최대값을 반환.
        - channels가 리스트면 그 중 '가장 많이 남은' 값을 리턴 (한 개라도 막히면 시작 금지용)
        - 기록이 없으면 0 반환
        """
        now = time.monotonic()
        if isinstance(channels, int):
            channels = [channels]
        remain_max = 0.0
        with self._lock:
            for ch in channels:
                last = self._last_finish_mono.get(kind, {}).get(int(ch))
                if last is None:
                    continue
                elapsed = now - last
                remain = max(0.0, float(cooldown_s) - elapsed)
                if remain > remain_max:
                    remain_max = remain
        return remain_max

    # ───────── PC 전용 고급 검사 ─────────
    def pc_block_reason(
        self,
        selected_ch: int,
        cooldown_s: float = 60.0,
    ) -> Tuple[bool, float, str]:
        """
        PC 시작 전 차단 사유/남은 시간 계산:
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

# 전역 싱글톤
runtime_state = RuntimeState()

# -*- coding: utf-8 -*-
"""PLC 링크 다운 구간 공유 플래그 — 끊김 중 중복 채팅 알림을 한 곳에서 막기 위한 것.

device(plc) 가 세팅하고 controller(chat_notifier) 가 읽는다. lib 는 device/controller 를
import 하지 않으므로 순환참조가 없다.
"""
from __future__ import annotations

import threading
from typing import Dict, Tuple

_lock = threading.Lock()
_plc_link_down: bool = False
_suppressed: Dict[str, int] = {}     # code -> 링크 다운 중 채팅 전송을 건너뛴 건수


def set_plc_link_down(down: bool) -> None:
    """링크 상태 세팅. 같은 값으로 여러 번 불려도 무해(idempotent)."""
    global _plc_link_down
    with _lock:
        _plc_link_down = bool(down)


def is_plc_link_down() -> bool:
    with _lock:
        return _plc_link_down


def note_suppressed(code: str) -> None:
    """링크 다운 동안 채팅 전송을 건너뛴 건수를 코드별로 누적."""
    key = (code or "").strip().upper() or "?"
    with _lock:
        _suppressed[key] = _suppressed.get(key, 0) + 1


def suppressed_total() -> int:
    """리셋 없이 현재 총합만 조회."""
    with _lock:
        return sum(_suppressed.values())


def consume_suppressed() -> Tuple[int, Dict[str, int]]:
    """(총합, {code: count}) 스냅샷을 반환하고 카운터를 0 으로 리셋."""
    with _lock:
        snap = dict(_suppressed)
        _suppressed.clear()
    return sum(snap.values()), snap


def format_suppressed(total: int, by_code: Dict[str, int]) -> str:
    """"E401 12건, E402 3건" 한 줄. total==0 이면 빈 문자열."""
    if not total:
        return ""
    return ", ".join(f"{k} {v}건" for k, v in sorted(by_code.items()) if v)

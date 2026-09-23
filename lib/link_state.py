# -*- coding: utf-8 -*-
"""PLC 링크 상태의 단일 소스 — down/up 전이 판정과 링크 챗 발송을 여기 한 곳에서만 한다.

device(plc) 가 전이를 알리고(note_link_down/up), main 이 발송기를 등록(set_emitter)하며,
controller(chat_notifier) 는 억제 판정에 쓴다. lib 는 device/controller 를 import 하지 않으므로
순환참조가 없다.

정책(2026-09-23):
  · 끊김 1회당 챗은 정확히 2장 — 끊기는 즉시 1장, 다시 연결되는 즉시 1장. 대기 임계 없음
  · down 은 "소켓을 잃은 첫 순간"(E401/reset, 소켓 재생성, 접속 최종 실패)에 선다.
    plc.py 가 예외를 올리기 '전' 에 전이하므로 호스트 경로가 응답/리포트할 때는 이미 down 이다
  · 개별 "장비 오류" 카드(CHAT_PLC_LINK_CODES)는 항상 억제하고 건수만 세어 재연결 카드에 싣는다
  · 발송은 반드시 이벤트 루프 스레드에서(emitter 가 마샬링). 전이 기록은 스레드 안전
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple

_lock = threading.Lock()
_plc_link_down: bool = False
_suppressed: Dict[str, int] = {}     # code -> 링크 다운 중 채팅 전송을 건너뛴 건수
_down_since: float = 0.0             # 첫 감지 시각(monotonic)
_down_reason: str = ""
_down_op: str = ""
_emitter: Optional[Callable[[str, Dict[str, Any]], None]] = None


def set_emitter(fn: Optional[Callable[[str, Dict[str, Any]], None]]) -> None:
    """링크 카드 발송기 등록. fn(kind, info) — kind 는 "down"/"up".
    ⚠ fn 은 워커 스레드에서 불릴 수 있다. 발송기 내부에서 루프 스레드로 마샬링할 것."""
    global _emitter
    with _lock:
        _emitter = fn


def _emit(kind: str, info: Dict[str, Any]) -> None:
    fn = _emitter
    if fn is None:
        return
    try:
        fn(kind, info)
    except Exception:
        pass


def note_link_down(reason: str = "", op: str = "") -> bool:
    """소켓을 잃은 첫 순간에 호출. 전이했으면 True(카드 1장 발송), 이미 down 이면 False."""
    global _plc_link_down, _down_since, _down_reason, _down_op
    with _lock:
        if _plc_link_down:
            return False
        _plc_link_down = True
        _down_since = time.monotonic()
        _down_reason = str(reason or "")
        _down_op = str(op or "")
        info = {"reason": _down_reason, "op": _down_op, "at": time.time()}
    _emit("down", info)
    return True


def note_link_up(detail: str = "", diag: str = "") -> bool:
    """접속에 성공한 즉시 호출. down 이었으면 True(카드 1장 발송), 아니면 False(카드 없음)."""
    global _plc_link_down, _down_since, _down_reason, _down_op
    with _lock:
        if not _plc_link_down:
            return False
        elapsed = max(0.0, time.monotonic() - _down_since) if _down_since else 0.0
        reason, op = _down_reason, _down_op
        snap = dict(_suppressed)
        _suppressed.clear()
        _plc_link_down = False
        _down_since = 0.0
        _down_reason = ""
        _down_op = ""
    info = {"detail": str(detail or ""), "diag": str(diag or ""), "elapsed_s": elapsed,
            "reason": reason, "op": op,
            "suppressed_total": sum(snap.values()), "suppressed_by_code": snap}
    _emit("up", info)
    return True


def down_info() -> Tuple[bool, float, str, str]:
    """(down 여부, 첫 감지 시각(monotonic), 사유, op) — 조회 전용."""
    with _lock:
        return _plc_link_down, _down_since, _down_reason, _down_op


def set_plc_link_down(down: bool) -> None:
    """링크 상태만 세팅(카드 없음). 전이 발송이 필요하면 note_link_down/up 을 쓸 것."""
    global _plc_link_down, _down_since, _down_reason, _down_op
    with _lock:
        _plc_link_down = bool(down)
        if not _plc_link_down:
            _down_since = 0.0
            _down_reason = ""
            _down_op = ""
        elif _down_since == 0.0:
            _down_since = time.monotonic()


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


def suppressed_snapshot() -> Tuple[int, Dict[str, int]]:
    """(총합, {code: count}) 조회 — 리셋하지 않는다(로그용). 리셋은 note_link_up/consume_suppressed 가 한다."""
    with _lock:
        snap = dict(_suppressed)
    return sum(snap.values()), snap


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

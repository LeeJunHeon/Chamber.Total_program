"""터보 펌프 꺼짐 지속 알림 — v1 lib/turbo_watch.py(main a6e8d46)의 판정기(TurboOffWatch, 순수)를 그대로 다시 씀.

v1: PLC 코일 로거가 스냅샷이 성공한 행마다 CH1/CH2 판정값(TURBO_n_SW and TURBO_n_인터락 — 래더 TURBO_n_START_OUT 과 같은 식)을
넘기고, main 이 발송기를 등록해 PLC 끊김 전용 방(chat_plc)으로 카드를 보낸다. v2: 1초 행(RowFeed.on_row)이 같은 값을 넘기고,
이벤트는 system.emit('plc', 'turbo_off_long'|'turbo_back_on', card=제목·부제·status) — 카드는 포털이 PLC 웹훅으로 보낸다.

판정(채널별, v1 그대로)
  · on=None(읽기 실패·끊김) — 상태·타이머를 바꾸지 않는다
  · 켜짐 → 꺼짐 전이 시각부터 잰다. 프로그램 시작 뒤 첫 값이 꺼짐이면 그 시각부터
  · 꺼짐이 TURBO_OFF_ALERT_S 이상 지속되면 "off_long" 을 그 꺼짐 구간에서 한 번만
  · off_long 뒤 다시 켜지면 "back_on"(꺼져 있던 시간 포함) 한 번 — off_long 전에 켜지면 아무것도 내지 않는다
  · TURBO_OFF_ALERT_S ≤ 0 이면 기능 끔(상태도 비운다)
시간은 monotonic 기준, 카드에 쓸 꺼진 시각은 벽시계로 따로 든다.
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Tuple


def fmt_duration(sec: float) -> str:
    """경과 시간 문구 — 'X시간 Y분' / 'X시간' / 'Y분' / 'Z초'."""
    s = max(0, int(round(float(sec))))
    h, rem = divmod(s, 3600)
    m = rem // 60
    if h > 0:
        return f"{h}시간 {m}분" if m else f"{h}시간"
    if m > 0:
        return f"{m}분"
    return f"{s}초"


def card_text(ev: Dict[str, Any]) -> Tuple[str, str, str]:
    """(제목, 부제, status) — 기존 PLC 카드와 같은 모양."""
    ch = int(ev.get("ch") or 0)
    if ev.get("kind") == "off_long":
        at = datetime.fromtimestamp(float(ev.get("off_at") or 0.0)).strftime("%H:%M:%S")
        return ("터보 꺼짐 지속",
                f"[CH{ch}] 터보 펌프 꺼짐 {fmt_duration(ev.get('alert_s') or 0.0)} 경과 | 꺼진 시각 {at} (PLC 기동 출력 기준)",
                "FAIL")
    return ("터보 다시 켜짐",
            f"[CH{ch}] 터보 펌프 다시 켜짐 | 꺼져 있던 시간 {fmt_duration(ev.get('off_s') or 0.0)}",
            "SUCCESS")


class TurboOffWatch:
    """채널별 터보 꺼짐 지속 판정. update() 는 보낼 이벤트(dict) 또는 None."""

    def __init__(self, alert_s: "Callable[[], float] | float" = 3600.0, wall: Callable[[], float] = time.time):
        self._alert_s = alert_s if callable(alert_s) else (lambda v=float(alert_s): v)
        self._wall = wall
        self._st: Dict[int, Dict[str, Any]] = {}

    def alert_s(self) -> float:
        try:
            return float(self._alert_s())
        except Exception:
            return 0.0

    def update(self, ch: int, on: Optional[bool], now: float) -> Optional[Dict[str, Any]]:
        lim = self.alert_s()
        if lim <= 0:
            self._st.pop(int(ch), None)
            return None
        if on is None:
            return None
        st = self._st.setdefault(int(ch), {"on": None, "off_since": None, "off_at": None, "alerted": False})
        now = float(now)
        if on:
            ev = None
            if st["alerted"] and st["off_since"] is not None:
                ev = {"kind": "back_on", "ch": int(ch), "off_at": st["off_at"],
                      "off_s": max(0.0, now - st["off_since"]), "alert_s": lim}
            st.update(on=True, off_since=None, off_at=None, alerted=False)
            return ev
        if st["off_since"] is None:                  # 켜짐 → 꺼짐, 또는 시작 뒤 첫 값이 꺼짐
            st.update(off_since=now, off_at=float(self._wall()), alerted=False)
        st["on"] = False
        if not st["alerted"] and (now - st["off_since"]) >= lim:
            st["alerted"] = True
            return {"kind": "off_long", "ch": int(ch), "off_at": st["off_at"],
                    "off_s": now - st["off_since"], "alert_s": lim}
        return None

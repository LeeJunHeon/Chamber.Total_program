"""CH2 히터 패널 글(v1 ui/heater_panel.py 의 _fault_text·_state_line·_status_text·배지·소유자 글, 그대로) — 화면(포털·노트북)이 보인다.

panel_texts(st, ramp, hold, owner) → {status_text, status_tone, state_line, badge, owner_text, sv_show, dev}
- 상태 문구 우선순위(heater_panel.py:287-309): 이상('과온 트립'/'센서 이상'/'워치독 트립'/'이상 발생', 빨강) >
  '인터락 — {빠진 조건} 꺼짐'(주황 — 읽은 값이 있고 stale 아님) > 'PLC 응답 없음[ · N초 전 값]'(빨강) >
  '운전 중 · 램프'/'운전 중 · 유지'/'운전 중'(초록) > '정지'(회색).
- 상태 줄(:268-285): 램프 중 '램프 {rate:g}°C/min → {target:.1f}°C[ · 약 N분]', RUN·유지 '유지 · DAC 상한 {value} 고정', '운전 중', '정지'.
- 배지(:216-226): FAULT > ITL(인터락 꺼짐) > RUN > STOP. 소유자(:47): manual 수동 · process 공정 · external 외부 · 그 밖 '—'.
- SV 표시(:197): RUN 이면 cur_sv, 아니면 sv. Δ = PV − SV 표시(RUN 이고 둘 다 있을 때).
tone: red(#c62828)·orange(#ef6c00)·green(#2e7d32)·gray(#616161) — 화면이 같은 색으로.
"""
from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Tuple

from v2.ch12.heater_process import itl_missing_text

OWNER_TEXT = {"manual": "수동", "process": "공정", "external": "외부"}
TONE_COLOR = {"red": "#c62828", "orange": "#ef6c00", "green": "#2e7d32", "gray": "#616161"}


def fault_text(st: Mapping[str, Any]) -> str:
    if st.get("ot"):
        return "과온 트립"
    if st.get("tc_err"):
        return "센서 이상"
    if st.get("wd_err"):
        return "워치독 트립"
    return "이상 발생"


def state_line(st: Mapping[str, Any], ramp: Tuple[bool, Optional[float], float, Optional[float]],
               hold: Mapping[str, Any]) -> str:
    active, target, rate, remain = ramp
    if active and target is not None:
        tail = f" · 약 {remain:.0f}분" if remain is not None else ""
        return f"램프 {rate:g}°C/min → {float(target):.1f}°C{tail}"
    if st.get("run") and hold.get("holding"):
        return f"유지 · DAC 상한 {hold.get('value')} 고정"
    if st.get("run"):
        return "운전 중"
    return "정지"


def status_text(st: Mapping[str, Any], ramp_active: bool, holding: bool) -> Tuple[str, str]:
    if st.get("fault"):
        return fault_text(st), "red"
    if st.get("ok") is not None and not st.get("itl") and not st.get("stale"):
        return f"인터락 — {itl_missing_text(dict(st))} 꺼짐", "orange"
    if st.get("stale"):
        age = st.get("age_s")
        return ("PLC 응답 없음" + (f" · {float(age):.0f}초 전 값" if age is not None else "")), "red"
    if st.get("run"):
        if ramp_active:
            return "운전 중 · 램프", "green"
        if holding:
            return "운전 중 · 유지", "green"
        return "운전 중", "green"
    return "정지", "gray"


def badge(st: Mapping[str, Any]) -> str:
    if st.get("fault"):
        return "FAULT"
    if not st.get("itl"):
        return "ITL"
    if st.get("run"):
        return "RUN"
    return "STOP"


def panel_texts(st: Mapping[str, Any], ramp: Tuple[bool, Optional[float], float, Optional[float]],
                hold: Mapping[str, Any], owner: Optional[str]) -> Dict[str, Any]:
    text, tone = status_text(st, bool(ramp[0]), bool(hold.get("holding")))
    run = bool(st.get("run"))
    sv_show = st.get("cur_sv") if run else st.get("sv")
    pv = st.get("pv")
    dev = None
    if run and pv is not None and sv_show is not None:
        dev = round(float(pv) - float(sv_show), 1)
    return {"status_text": text, "status_tone": tone, "status_color": TONE_COLOR[tone],
            "state_line": state_line(st, ramp, hold), "badge": badge(st), "owner_text": OWNER_TEXT.get(str(owner), "—"),
            "sv_show": sv_show, "dev": dev, "stale": bool(st.get("stale"))}


def engine_panel(eng) -> Dict[str, Any]:
    """HeaterEngine → panel_texts(ramp_info·hold_snapshot·owner 를 엔진에서). 실패하면 v1 처럼 램프·유지 없음으로."""
    st = eng.status()
    try:
        ramp = eng.ramp_info()
    except Exception:
        ramp = (False, None, 0.0, None)
    try:
        hold = eng.hold_snapshot()
    except Exception:
        hold = {}
    return panel_texts(st, ramp, hold, getattr(eng, "owner", None))

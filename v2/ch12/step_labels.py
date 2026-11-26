"""공정 단계의 화면용 이름(2026-11-24 묶음 16 ④ — 챔버K 묶음 21 용어표).

공정 스텝 문구(step.message)는 v1 그대로 둔다 — 공정 txt·로그·골든이 그 글이다('메인 밸브 열기',
'Power Select OFF (SW_POWER_SELECT)'). 운전 화면에는 이 함수가 용어표로 쓴 이름을 따로 싣는다(스냅샷 <c>.proc.step_label·
엔진 화면 상태 step_label·사건 process/step). 이름은 동작(action)·인자(params)에서 만들고, 숫자는 스텝 문구에서 읽는다.
모르는 동작은 스텝 문구를 그대로 돌려준다(시험이 골든의 모든 동작이 이름을 갖는지 본다). 순수 함수 — 장비·기록에 닿지 않는다.
대상 'MV'(v1 문구 '메인 밸브 열기/닫기')는 진공 MV(M_V_n_SW)가 아니라 가스 Main 밸브 MAIN_n_GAS_SW(PLC 이름 'Main 가스')를 여닫는다
(process_actions — W-21·22 대조 E) → 화면 이름 'Main 가스 열기/닫기'. v1 문구와 진공 MV 이름(PLC 버튼 'CHn MV')은 그대로.
"""
from __future__ import annotations

import re
from typing import Any, Optional

_GAS_PLC = {"AR": "Ar 밸브", "O2": "O₂ 밸브", "N2": "N₂ 밸브", "MAIN": "Main 가스"}
_MAIN_GAS = "Main 가스"           # 대상 'MV' = MAIN_n_GAS_SW(PLC 버튼 이름 'CHn Main 가스') — 진공 MV 가 아님
_GAS_CH = {1: "Ar", 2: "O₂", 3: "N₂"}
_GUN = {"G1": "Gun1 셔터", "G2": "Gun2 셔터", "G3": "Gun3 셔터"}
_POWER_SET = {"DC_POWER_SET": "DC1", "DC2_POWER_SET": "DC2", "RF_POWER_SET": "RF Power"}
_POWER_STOP = {"DC_POWER_STOP": "DC1", "DC2_POWER_STOP": "DC2", "RF_POWER_STOP": "RF Power", "DC_PULSE_STOP": "DC Pulse",
               "RF_PULSE_STOP": "RF Pulse"}


def _num(text: str, pat: str) -> Optional[str]:
    m = re.search(pat, text or "")
    return m.group(1) if m else None


def _g(v: str) -> str:
    """'10.0' → '10', '1.5' → '1.5', '33.333333333333336' → '33.33'."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    return f"{round(f, 2):g}"


def _plc(params: tuple, msg: str) -> Optional[str]:
    if not params:
        return None
    tgt, on = str(params[0]), bool(params[1]) if len(params) > 1 else True
    open_close = "열기" if on else "닫기"
    on_off = "켜기" if on else "끄기"
    if tgt == "MV":
        name = f"{_MAIN_GAS} {open_close}"
    elif tgt == "MS":
        name = f"Main 셔터 {open_close}"
    elif tgt in _GUN:
        name = f"{_GUN[tgt]} {open_close}"
    elif tgt in _GAS_PLC:
        name = f"{_GAS_PLC[tgt]} {open_close}"
    elif tgt == "SW_RF_SELECT":
        name = f"RF Select {on_off}"
    elif tgt == "SW_POWER_SELECT":
        name = f"Power Select {on_off}"
    else:
        return None
    return name


def _mfc(params: tuple, msg: str) -> Optional[str]:
    if not params:
        return None
    cmd = str(params[0])
    args = params[1] if len(params) > 1 and isinstance(params[1], dict) else {}
    gas = _GAS_CH.get(int(args["channel"])) if "channel" in args else None
    step = _num(msg, r"단계 (\d+/\d+)")
    tail = f" · 단계 {step}" if step else ""
    if cmd == "FLOW_OFF":
        return f"{gas} 흐름 끄기" if gas else "가스 흐름 끄기"
    if cmd == "FLOW_SET":
        return f"{gas} 목표 {_g(args.get('value'))} sccm{tail}"
    if cmd == "FLOW_ON":
        wait = _num(msg, r"\(([\d.]+)sccm 도달 대기\)")
        reach = _num(msg, r"GAS ([\d.]+)sccm 도달 대기")       # 가스 단계의 2번째부터(흐름은 이미 켜짐)
        if reach:
            return f"{gas} {_g(reach)} sccm 도달 대기"
        return f"{gas} 흐름 켜기" + (f" · {_g(wait)} sccm 도달 대기" if wait else "")
    if cmd == "VALVE_OPEN":
        return "MFC 밸브 열기"
    if cmd == "PS_ZEROING":
        return "압력 센서 영점"
    if cmd == "MFC_ZEROING":
        return f"{gas} 영점"
    m = re.fullmatch(r"SP(\d)_SET", cmd)
    if m:
        return f"SP{m.group(1)} 목표 {float(args.get('value') or 0):.2f} mTorr"
    m = re.fullmatch(r"SP(\d)_ON", cmd)
    if m:
        return f"압력 제어 SP{m.group(1)} 시작"
    if cmd == "WAIT_PRESSURE":
        sp = _num(msg, r"SP(\d)")
        return f"SP{sp} 압력 도달 대기" if sp else "압력 도달 대기"
    if cmd in ("WAIT_FLOW", "WAIT_FLOW_STABLE"):
        v = _num(msg, r"GAS ([\d.]+)sccm")
        return f"{gas} {_g(v)} sccm 도달 대기" if v else f"{gas} 유량 도달 대기"
    if "도달 대기" in msg:
        v = _num(msg, r"GAS ([\d.]+)sccm")
        if gas and v:
            return f"{gas} {_g(v)} sccm 도달 대기"
    return None


def _delay(msg: str) -> Optional[str]:
    tag = " · 변경 전" if "(변경 전)" in msg else " · 변경 후" if "(변경 후)" in msg else ""
    sec = _num(msg, r"([\d.]+)초")
    minutes = _g(_num(msg, r"([\d.]+)분"))
    if msg.startswith("Power Select 안정화 대기"):
        return f"Power Select 안정화 {sec}초"
    if msg.startswith("Power Delay"):
        return f"파워 안정화 대기 {sec}초"
    if msg.startswith("Shutter Delay"):
        return f"셔터 지연 {minutes}분{tag}"
    if msg.startswith("TEST MODE"):
        test_s = _num(msg, r"\(([\d.]+)s\)")
        return f"TEST 모드 {test_s}초"
    if msg.startswith("메인 공정 진행"):
        return f"메인 공정 {minutes}분{tag}"
    return None


def step_label(step: Any) -> str:
    """스텝 → 화면 이름(용어표). 만들 수 없으면 스텝 문구 그대로."""
    action = str(getattr(step, "action", "") or "")
    msg = str(getattr(step, "message", "") or "")
    params = getattr(step, "params", None) or ()
    urgent = msg.startswith("[긴급]")
    name: Optional[str] = None
    if action == "PLC_CMD":
        name = _plc(params, msg)
    elif action == "MFC_CMD":
        name = _mfc(params, msg)
    elif action in _POWER_SET:
        w = _num(msg, r"([\d.]+)W")
        name = f"{_POWER_SET[action]} 목표 {_g(w)} W" if w else f"{_POWER_SET[action]} 켜기"
    elif action in _POWER_STOP:
        name = f"{_POWER_STOP[action]} 끄기"
    elif action in ("DC_PULSE_START", "RF_PULSE_START"):
        w = _num(msg, r"P=([\d.]+)W")
        dev = "DC Pulse" if action.startswith("DC") else "RF Pulse"
        name = f"{dev} 켜기" + (f" {_g(w)} W" if w else "")
    elif action in ("DC_PULSE_SET", "RF_PULSE_SET"):
        w = _num(msg, r"→ ([\d.]+)W")
        dev = "DC Pulse" if action.startswith("DC") else "RF Pulse"
        name = f"{dev} 파워 바꾸기" + (f" {_g(w)} W" if w else "")
    elif action == "DELAY":
        name = _delay(msg)
    elif action == "HEATER_SET":
        temp = _num(msg, r"([\d.]+)°C")
        name = f"히터 목표 {temp}°C"
    elif action == "HEATER_WAIT":
        soak = _num(msg, r"SOAK ([\d.]+)분")
        temp = _num(msg, r"([\d.]+)°C")
        name = f"히터 {temp}°C 도달 대기" + (f" · 유지 {soak}분" if soak and soak != "0" else "")
    elif action == "HEATER_HOLD":
        name = "히터 유지 측정 대기"
    elif action == "IG_CMD":
        p = _num(msg, r"\(([\d.eE+-]+)\)")
        name = f"베이스 압력 {p} Torr 도달 대기" if p and "도달" in msg else None
    elif action == "OES_RUN":
        name = "OES 측정 시작"
    elif action == "RGA_SCAN":
        name = "RGA 스캔"
    elif action == "THICKNESS_WAIT":
        nm = _num(msg, r"두께 ([\d.]+)nm")
        name = f"메인 공정 · 두께 {_g(nm)} nm" if nm else "메인 공정 · 두께 기준"
    elif action == "THICKNESS_ZERO":
        name = "두께 영점 확인"
    if name is None:
        return msg
    return f"긴급 {name}" if urgent else name

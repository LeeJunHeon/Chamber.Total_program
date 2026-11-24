"""PLC 코일·레지스터의 화면 이름(2026-11-24 묶음 16 — 챔버K 묶음 21 용어표).

스냅샷 필드 plc.<코일>(snapshot_fields)과 PLC 버튼(commands.PLC_BUTTONS short·label)이 이 표 하나를 쓴다 — 같은 대상은 같은 이름.
래더 이름(M_V_1_SW 등)·주소는 화면 이름에 넣지 않고 필드 desc·버튼 desc 로(상세 보기). 데이터만 — import 해도 아무 일도 하지 않는다.

용어(챔버 구분은 앞에 CH1·CH2·Loadlock):
  R_P Rotary · R_V RV(Roughing Valve) · F_V FV(Foreline Valve) · M_V MV(Main Valve) · VENT Vent · TURBO Turbo · DOOR Door ·
  G_V Gate · MAIN_SHUTTER Main 셔터 · SHUTTER_n Gunn 셔터 · Ar/O2/N2_n_GAS Ar·O₂·N₂ 밸브 · MAIN_n_GAS Main 가스(가스 줄 끝의 공통 밸브) ·
  L_PIN 4Pin · Z_M_P Chuck(CW 올림·CCW 내림·MID 중간) · Zn_UP/MID/DOWN Chuck 올림·중간·내림 요청, *_LOCATION 그 위치 ·
  *_LAMP 표시등 · LOADING_n_SENSOR 로딩 센서 · LP_STEPn Loadlock 배기 n단계 · GAUGE_n_A/B Gauge A·B ·
  DCV 채널: 0 = CH2 DC1, 1 = Plasma Cleaning RF Power, 2 = CH2 RF Power, 3 = CH2 DC2(drivers/dc.py·rf.py UNITS) ·
  SW_RF_SELECT RF Select(RF 전용 릴레이) · SW_POWER_SELECT Power Select.
"""
from __future__ import annotations

from typing import Dict

PC = "Plasma Cleaning"

COIL_LABEL: Dict[str, str] = {
    # 꺼짐 대기 타이머
    "R_P_1_OFF_TIMER": "CH1 Rotary 끄기 대기", "R_P_2_OFF_TIMER": "CH2 Rotary 끄기 대기",
    "L_R_P_OFF_TIMER": "Loadlock Rotary 끄기 대기",
    # 인터락
    "R_V_1_인터락": "CH1 RV 인터락", "F_V_1_인터락": "CH1 FV 인터락", "M_V_1_인터락": "CH1 MV 인터락",
    "VENT_1_인터락": "CH1 Vent 인터락", "TURBO_1_인터락": "CH1 Turbo 인터락", "DOOR_1_인터락": "CH1 Door 인터락",
    "G_V_1_인터락": "CH1 Gate 인터락", "MAIN_SHUTTER_1_인터락": "CH1 Main 셔터 인터락",
    "Ar_1_GAS_인터락": "CH1 Ar 밸브 인터락", "O2_1_GAS_인터락": "CH1 O₂ 밸브 인터락", "MAIN_1_GAS_인터락": "CH1 Main 가스 인터락",
    "L_R_V_인터락": "Loadlock RV 인터락", "L_PIN_인터락": "Loadlock 4Pin 인터락", "L_VENT_인터락": "Loadlock Vent 인터락",
    "R_V_2_인터락": "CH2 RV 인터락", "F_V_2_인터락": "CH2 FV 인터락", "M_V_2_인터락": "CH2 MV 인터락",
    "VENT_2_인터락": "CH2 Vent 인터락", "TURBO_2_인터락": "CH2 Turbo 인터락", "DOOR_2_인터락": "CH2 Door 인터락",
    "G_V_2_인터락": "CH2 Gate 인터락", "MAIN_SHUTTER_2_인터락": "CH2 Main 셔터 인터락",
    "Ar_2_GAS_인터락": "CH2 Ar 밸브 인터락", "O2_2_GAS_인터락": "CH2 O₂ 밸브 인터락", "N2_2_GAS_인터락": "CH2 N₂ 밸브 인터락",
    "MAIN_2_GAS_인터락": "CH2 Main 가스 인터락",
    # D/A 출력 허용(채널별)
    "DCV_SET_0": "CH2 DC1 출력", "DCV_SET_1": f"{PC} RF Power 출력", "DCV_SET_2": "CH2 RF Power 출력",
    "DCV_SET_3": "CH2 DC2 출력",
    # Loadlock 배기
    "LP_STEP1": "Loadlock 배기 1단계", "LP_STEP2": "Loadlock 배기 2단계", "L_VAC_READY_SW": "Loadlock 진공 준비",
    "L_ATM": "Loadlock 대기압", "L_VAC_NOT_READY": "Loadlock 진공 준비 전",
    # Chuck 위치 요청·위치
    "Z1_UP": "CH1 Chuck 올림 요청", "Z1_MID": "CH1 Chuck 중간 요청", "Z1_DOWN": "CH1 Chuck 내림 요청",
    "Z2_UP": "CH2 Chuck 올림 요청", "Z2_MID": "CH2 Chuck 중간 요청", "Z2_DOWN": "CH2 Chuck 내림 요청",
    "Z1_UP_LOCATION": "CH1 Chuck 올림 위치", "Z1_MID_LOCATION": "CH1 Chuck 중간 위치",
    "Z1_DOWN_LOCATION": "CH1 Chuck 내림 위치", "Z2_UP_LOCATION": "CH2 Chuck 올림 위치",
    "Z2_MID_LOCATION": "CH2 Chuck 중간 위치", "Z2_DOWN_LOCATION": "CH2 Chuck 내림 위치",
    "Z_M_P_1_STOP_SW": "CH1 Chuck 정지", "Z_M_P_1_MID_SW": "CH1 Chuck 중간", "Z_M_P_1_MID_CW": "CH1 Chuck 중간 올림",
    "Z_M_P_1_MID_CCW": "CH1 Chuck 중간 내림", "Z_M_P_2_MID_SW": "CH2 Chuck 중간", "Z_M_P_2_MID_CW": "CH2 Chuck 중간 올림",
    "Z_M_P_2_MID_CCW": "CH2 Chuck 중간 내림",
    # CH1 스위치
    "R_P_1_SW": "CH1 Rotary", "R_V_1_SW": "CH1 RV", "F_V_1_SW": "CH1 FV", "M_V_1_SW": "CH1 MV", "VENT_1_SW": "CH1 Vent",
    "TURBO_1_SW": "CH1 Turbo", "DOOR_1_OPEN_SW": "CH1 Door 열기", "DOOR_1_CLOSE_SW": "CH1 Door 닫기",
    "G_V_1_OPEN_SW": "CH1 Gate 열기", "G_V_1_CLOSE_SW": "CH1 Gate 닫기", "MAIN_SHUTTER_1_SW": "CH1 Main 셔터",
    "Ar_1_GAS_SW": "CH1 Ar 밸브", "O2_1_GAS_SW": "CH1 O₂ 밸브", "MAIN_1_GAS_SW": "CH1 Main 가스",
    # Loadlock·공유 스위치
    "L_R_P_SW": "Loadlock Rotary", "L_R_V_SW": "Loadlock RV", "L_PIN_UP_SW": "Loadlock 4Pin 올림",
    "L_PIN_DOWN_SW": "Loadlock 4Pin 내림", "L_VENT_SW": "Loadlock Vent", "SW_RF_SELECT": "RF Select",
    "SW_POWER_SELECT": "Power Select", "SHUTTER_1_SW": "Gun1 셔터", "SHUTTER_2_SW": "Gun2 셔터", "SHUTTER_3_SW": "Gun3 셔터",
    "Z_M_P_1_SW": "CH1 Chuck", "Z_M_P_1_CW_SW": "CH1 Chuck 올림", "Z_M_P_1_CCW_SW": "CH1 Chuck 내림",
    "Z_M_P_2_SW": "CH2 Chuck", "Z_M_P_2_CW_SW": "CH2 Chuck 올림", "Z_M_P_2_CCW_SW": "CH2 Chuck 내림",
    "Z_M_P_2_STOP_SW": "CH2 Chuck 정지", "BUZZER_STOP_SW": "부저 정지",
    # CH2 스위치
    "R_P_2_SW": "CH2 Rotary", "R_V_2_SW": "CH2 RV", "F_V_2_SW": "CH2 FV", "M_V_2_SW": "CH2 MV", "VENT_2_SW": "CH2 Vent",
    "TURBO_2_SW": "CH2 Turbo", "DOOR_2_OPEN_SW": "CH2 Door 열기", "DOOR_2_CLOSE_SW": "CH2 Door 닫기",
    "G_V_2_OPEN_SW": "CH2 Gate 열기", "G_V_2_CLOSE_SW": "CH2 Gate 닫기", "MAIN_SHUTTER_2_SW": "CH2 Main 셔터",
    "Ar_2_GAS_SW": "CH2 Ar 밸브", "O2_2_GAS_SW": "CH2 O₂ 밸브", "N2_2_GAS_SW": "CH2 N₂ 밸브", "MAIN_2_GAS_SW": "CH2 Main 가스",
    # 표시등
    "VENT_1_LAMP": "CH1 Vent 표시등", "DOOR_1_OPEN_LAMP": "CH1 Door 열림 표시등", "DOOR_1_CLOSE_LAMP": "CH1 Door 닫힘 표시등",
    "G_V_1_OPEN_LAMP": "CH1 Gate 열림 표시등", "G_V_1_CLOSE_LAMP": "CH1 Gate 닫힘 표시등",
    "L_PIN_UP_LAMP": "Loadlock 4Pin 올림 표시등", "L_PIN_DOWN_LAMP": "Loadlock 4Pin 내림 표시등",
    "L_VENT_LAMP": "Loadlock Vent 표시등", "VENT_2_LAMP": "CH2 Vent 표시등", "DOOR_2_OPEN_LAMP": "CH2 Door 열림 표시등",
    "DOOR_2_CLOSE_LAMP": "CH2 Door 닫힘 표시등", "G_V_2_OPEN_LAMP": "CH2 Gate 열림 표시등",
    "G_V_2_CLOSE_LAMP": "CH2 Gate 닫힘 표시등",
    "LOADING_1_SENSOR_LAMP": "CH1 로딩 센서", "LOADING_2_SENSOR_LAMP": "CH2 로딩 센서",
    # Gauge 인터락
    "GAUGE_1_A_INTERLOCK": "CH1 Gauge A 인터락", "GAUGE_1_B_INTERLOCK": "CH1 Gauge B 인터락",
    "L_GAUGE_A_INTERLOCK": "Loadlock Gauge A 인터락", "GAUGE_2_A_INTERLOCK": "CH2 Gauge A 인터락",
    "GAUGE_2_B_INTERLOCK": "CH2 Gauge B 인터락",
    # P 미러(읽기 전용 입력·출력)
    "L_GAUGE_A": "Loadlock Gauge A", "L_ATM_SENSOR": "Loadlock 대기압 센서", "L_R_P_OUT": "Loadlock Rotary 출력",
    "L_R_V_OUT": "Loadlock RV 출력", "L_VENT_OUT": "Loadlock Vent 출력",
    # 레지스터(D) — 단위 count(원시 값)
    "DCV_READ_0": "CH2 DC1 전압", "DCV_READ_1": "CH2 DC1 전류", "DCV_READ_2": f"{PC} RF Forward",
    "DCV_READ_3": f"{PC} RF Reflected", "DCV_READ_4": "CH2 RF Forward", "DCV_READ_5": "CH2 RF Reflected",
    "DCV_READ_6": "CH2 DC2 전압", "DCV_READ_7": "CH2 DC2 전류",
    "DCV_WRITE_0": "CH2 DC1 목표", "DCV_WRITE_1": f"{PC} RF Power 목표", "DCV_WRITE_2": "CH2 RF Power 목표",
    "DCV_WRITE_3": "CH2 DC2 목표",
}
COIL_PLAN_LABEL = "코일 읽기 계획"


def label(name: str) -> str:
    """래더 이름 → 화면 이름(표에 없으면 래더 이름 그대로 — 시험이 표에 다 있는지 본다)."""
    return COIL_LABEL.get(name, name)

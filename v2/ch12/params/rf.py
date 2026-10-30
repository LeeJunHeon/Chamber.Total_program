"""RF 연속 전원(CH2, PLC D/A·A/D) 파라미터 — v2/ch12/drivers/rf.py 와 plc.py(RF 피드백 보정).

v1 RFPowerAsync 는 cfg 없이 만들어져 config_common(= settings.json common) 값을 쓴다(runtime/chamber_runtime.py:776-786,
device/rf_power.py:67-68). 그래서 ch2.RF_REFLECTED_THRESHOLD_W 는 읽히지 않고 common 50.0 이 쓰인다(inv_cfg ⚠).
일반 모드의 fine_up 3.0·overshoot ramp_down 1.0 W 는 reload 에서 갱신되지 않는 생성자 기본값이라 키가 없다(device/rf_power.py:113-117).
코드 기본값과 설정값이 다른 곳은 v1 이 실제로 쓰던 설정값: blind 도달 시간 150 s(코드 60), rampdown 간격 1000 ms(코드 50).
"""
from __future__ import annotations

from ._base import B, F, H, I, L, M, Dropped, p

_G = "rf"

PARAMS: tuple = (
    p("RF_MAX_POWER", _G, "RF 최대 출력", "W", F, 600.0, 0.0, 1000.0, safety=H, src="lib/config_common.py:173 · device/rf_power.py:88"),
    p("RF_RAMP_STEP", _G, "RF 램프 단계", "W", F, 1.0, 0.1, 100.0, safety=H,
      src="lib/config_common.py:174 · device/rf_power.py:89", desc="램프업 측정마다(1 s) +이 값, 정지 램프다운 1 s 마다 −이 값"),
    p("RF_MAINTAIN_STEP", _G, "RF 유지 보정 단계", "W", F, 0.1, 0.0, 10.0, src="lib/config_common.py:175"),
    p("RF_TOLERANCE_POWER", _G, "RF 목표 허용 오차", "W", F, 1.0, 0.0, 100.0, src="lib/config_common.py:176"),
    p("RF_LOW_POWER_THRESH_W", _G, "RF 저출력 기준", "W", F, 1.0, 0.0, 600.0, safety=H, src="lib/config_common.py:179"),
    p("RF_LOW_POWER_COUNT_MAX_N", _G, "RF 저출력 연속 횟수", "회", I, 3, 1, 100, safety=H, src="lib/config_common.py:180"),
    p("RF_REFLECTED_THRESHOLD_W", _G, "RF 반사 감시 임계", "W", F, 50.0, 0.0, 600.0, safety=H, v1_default=20.0,
      src="config/settings.json:94 · device/rf_power.py:187", desc="REF 감시 중 반사 > 이 값이면 Ref.p 안정화 대기"),
    p("RF_REFLECTED_WAIT_TIMEOUT_S", _G, "Ref.p 안정화 최대 대기", "s", F, 60.0, 1.0, 3600.0, safety=H, src="lib/config_common.py:184"),
    p("RF_BLIND_RAMP_FWD_THRESHOLD_W", _G, "Blind ramp FWD 임계", "W", F, 70.0, 0.0, 600.0, safety=H,
      src="lib/config_common.py:187", desc="FWD 가 이 값에 닿기 전까지 REF 감시 OFF. 0 이거나 목표 ≤ 이 값이면 처음부터 감시"),
    p("RF_BLIND_RAMP_SETTLE_S", _G, "Blind ramp 안정화 대기", "s", F, 60.0, 0.0, 3600.0, safety=H, src="lib/config_common.py:188"),
    p("RF_BLIND_SETTLE_EARLY_EXIT_REF_W", _G, "안정화 조기 종료 REF", "W", F, 1.0, 0.0, 600.0, src="lib/config_common.py:189"),
    p("RF_BLIND_RAMP_REACH_TIMEOUT_S", _G, "Blind ramp 도달 시간 한도", "s", F, 150.0, 0.0, 3600.0, safety=H,
      v1_default=60.0, src="config/settings.json:106 · device/rf_power.py:205",
      desc="REF 감시 전 이 시간이 넘도록 FWD 가 임계에 못 닿으면 실패(시작 뒤 FWD 0.0 이 계속돼도 같은 시간). 0=끔"),
    p("PLC_RF_CH1_FWD_A", _G, "RF CH1(PC) Forward 기울기", "", F, 0.1503488383, -100.0, 100.0, safety=H, src="lib/config_common.py:703"),
    p("PLC_RF_CH1_FWD_B", _G, "RF CH1(PC) Forward 오프셋", "W", F, 3.0664228165, -1000.0, 1000.0, safety=H, src="lib/config_common.py:704"),
    p("PLC_RF_CH1_REF_A", _G, "RF CH1(PC) Reflected 기울기", "", F, 0.1565388751, -100.0, 100.0, safety=H, src="lib/config_common.py:705"),
    p("PLC_RF_CH1_REF_B", _G, "RF CH1(PC) Reflected 오프셋", "W", F, 12.2067054791, -1000.0, 1000.0, safety=H, src="lib/config_common.py:706"),
    p("PLC_RF_CH2_FWD_A", _G, "RF CH2 Forward 기울기", "", F, 0.15059, -100.0, 100.0, safety=H, src="lib/config_common.py:709"),
    p("PLC_RF_CH2_FWD_B", _G, "RF CH2 Forward 오프셋", "W", F, -0.598, -1000.0, 1000.0, safety=H, src="lib/config_common.py:710"),
    p("PLC_RF_CH2_REF_A", _G, "RF CH2 Reflected 기울기", "", F, 0.12940, -100.0, 100.0, safety=H, src="lib/config_common.py:711"),
    p("PLC_RF_CH2_REF_B", _G, "RF CH2 Reflected 오프셋", "W", F, -0.267, -1000.0, 1000.0, safety=H, src="lib/config_common.py:712"),
    p("PLC_RF_FORWARD_ZERO_W", _G, "Forward 제로 보정", "W", F, 4.0, -100.0, 100.0, safety=H, src="lib/config_common.py:715"),
    p("PLC_RF_REFLECTED_ZERO_W", _G, "Reflected 제로 보정", "W", F, 14.0, -100.0, 100.0, safety=H, src="lib/config_common.py:716"),
    p("CHAMBER_RF_CONT_POLL_INTERVAL_MS", _G, "RF 연속 상태 확인 주기", "ms", I, 1000, 50, 60000,
      src="lib/config_common.py:731 · device/rf_power.py:210", desc="측정 반영 주기 — 1초 PLC 묶음 읽기에서 받는다"),
    p("CHAMBER_RF_CONT_RAMPDOWN_INTERVAL_MS", _G, "RF 연속 정지 램프 간격", "ms", I, 1000, 10, 60000, safety=H,
      v1_default=50, src="config/settings.json:351 · device/rf_power.py:213", desc="정지 때 RF_RAMP_STEP 씩 내리는 간격"),
    p("CHAMBER_RF_CONT_DIRECT_MODE", _G, "RF 연속 직접 모드", "", B, False, safety=H, src="lib/config_common.py:733 · device/rf_power.py:221"),
    p("CHAMBER_RF_CONT_ZEROING", _G, "RF 연속 제로 보정 사용", "", B, False, safety=H, src="lib/config_common.py:734 · runtime/chamber_runtime.py:767",
      note="v1 은 채널 모듈(config_ch2 — json 적용 전 .py 기본값 False 복사)을 읽어 common 값이 효과 없었다(⚠) — 기본 False 로 같게"),
    p("CHAMBER_RF_CONT_WRITE_INV_A", _G, "RF 연속 쓰기 역변환 기울기", "", F, 1.6546, 0.01, 100.0, safety=H, src="lib/config_common.py:735"),
    p("CHAMBER_RF_CONT_WRITE_INV_B", _G, "RF 연속 쓰기 역변환 오프셋", "W", F, 2.6323, -1000.0, 1000.0, safety=H, src="lib/config_common.py:736"),
)

_UNUSED = "v1 코드에서 읽는 곳 없음(설정 로더의 이름·형식 검사만)"
DROPPED: tuple = (
    Dropped("common.RF_PARAM_WATT_TO_DAC", _UNUSED, "lib/config_common.py:194"),
    Dropped("common.RF_OFFSET_WATT_TO_DAC", _UNUSED, "lib/config_common.py:195"),
    Dropped("common.RF_PARAM_ADC_TO_WATT", _UNUSED, "lib/config_common.py:196"),
    Dropped("common.RF_OFFSET_ADC_TO_WATT", _UNUSED, "lib/config_common.py:197"),
    Dropped("common.RF_WATT_PER_VOLT", _UNUSED, "lib/config_common.py:198"),
    Dropped("ch2.RF_REFLECTED_THRESHOLD_W", "v1 이 읽지 않음 — RFPowerAsync 가 config_common(common 50.0)을 읽는다(inv_cfg ⚠). "
            "값은 common 과 같다", "device/rf_power.py:187"),
)

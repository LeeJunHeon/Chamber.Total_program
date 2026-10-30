"""DC 연속 전원(CH2 DC1·DC2, PLC D/A·A/D) 파라미터 — v2/ch12/drivers/dc.py 와 plc.py(DAC·스케일).

v1 DCPowerAsync 는 cfg 없이 만들어져(runtime/chamber_runtime.py:721-726) config_common 값을 쓴다 → 공통 키 그대로.
DC_OVERCURRENT_* 는 v1 이 매번 config_common 에서 읽는다(device/dc_power.py:280-292). POWER_OFF_ZERO_DEADLINE_S 는
settings.json 에 없는 코드 상수(RF 연속·PC RF 도 같은 값을 쓴다 — 정의는 여기 하나).
"""
from __future__ import annotations

from ._base import F, H, I, B, L, M, Dropped, inst, p

_G = "dc"

PARAMS: tuple = (
    p("DC_MAX_POWER", _G, "DC 최대 출력", "W", F, 1000.0, 0.0, 1000.0, safety=H, src="lib/config_common.py:204 · device/dc_power.py:134"),
    p("DC_INTERVAL_MS", _G, "DC 상태 확인(판정) 주기", "ms", I, 5000, 50, 60000,
      src="lib/config_common.py:205 · device/dc_power.py:137",
      desc="측정·보정·저전력/저전류/과전류 판정 주기. 값은 1초 PLC 묶음 읽기에서 받는다(최소 50)"),
    p("DC_MAINTAIN_STEP", _G, "DC 유지 보정 단계", "W", F, 1.0, 0.0, 100.0, src="lib/config_common.py:207 · device/dc_power.py:136"),
    p("DC_TOLERANCE_POWER", _G, "DC 목표 허용 오차", "W", F, 1.0, 0.0, 100.0, src="lib/config_common.py:208 · device/dc_power.py:135"),
    p("DC_LOW_W_THRESH", _G, "DC 저전력 기준", "W", F, 1.0, 0.0, 1000.0, safety=H, src="lib/config_common.py:211 · device/dc_power.py:140"),
    p("DC_LOW_STREAK_N", _G, "DC 저전력 연속 횟수", "회", I, 3, 1, 100, safety=H, src="lib/config_common.py:212"),
    p("DC_LOW_CURRENT_THRESH_A", _G, "DC 저전류 기준", "A", F, 0.05, 0.0, 10.0, safety=H, src="lib/config_common.py:213"),
    p("DC_LOW_CURRENT_STREAK_N", _G, "DC 저전류 연속 횟수", "회", I, 3, 1, 100, safety=H, src="lib/config_common.py:214"),
    p("DC_WATT_DEADBAND", _G, "DC 재전송 데드밴드", "W", F, 0.5, 0.0, 100.0, src="lib/config_common.py:215 · device/dc_power.py:146"),
    p("DC_OVERCURRENT_ENABLE_CONT", _G, "DC(PLC 경유) 과전류 인터락", "", B, True, safety=H,
      src="config/settings.json:123 · device/dc_power.py:292"),
    p("DC_OVERCURRENT_P1_W", _G, "과전류 한계 P1", "W", F, 300.0, 0.0, 1000.0, safety=H, src="lib/power_limits.py:35"),
    p("DC_OVERCURRENT_I1_A", _G, "과전류 한계 I1", "A", F, 1.0, 0.01, 10.0, safety=H, src="lib/power_limits.py:29"),
    p("DC_OVERCURRENT_P2_W", _G, "과전류 한계 P2", "W", F, 600.0, 0.0, 1000.0, safety=H, src="lib/power_limits.py:36"),
    p("DC_OVERCURRENT_I2_A", _G, "과전류 한계 I2", "A", F, 2.0, 0.01, 10.0, safety=H, src="lib/power_limits.py:37"),
    p("DC_OVERCURRENT_STREAK_N", _G, "과전류 연속 횟수", "회", I, 1, 1, 100, safety=H, src="device/dc_power.py:327"),
    p("DC_OVERCURRENT_EXTRAPOLATE", _G, "P2 이상 외삽", "", B, True, safety=H, src="lib/power_limits.py:38"),
    inst("PLC_DC_POWER_MIN_W", _G, "PLC D/A 최소 W", F, 0.0, 0.0, 1000.0, unit="W", safety=H, src="lib/config_common.py:691 · device/plc.py:556"),
    inst("PLC_DC_POWER_MAX_W", _G, "PLC D/A 최대 W", F, 1000.0, 1.0, 10000.0, unit="W", safety=H, src="lib/config_common.py:692"),
    inst("PLC_DC_DAC_FULL_SCALE", _G, "PLC DAC 풀스케일", I, 4000, 1, 65535, safety=H, src="lib/config_common.py:693"),
    inst("PLC_DC_DAC_OFFSET", _G, "PLC DAC 오프셋", I, 0, -4000, 4000, safety=H, src="lib/config_common.py:694"),
    inst("PLC_DC_WRITE_INDEX", _G, "기본 D/A 채널", I, 0, 0, 3, safety=H, src="lib/config_common.py:695 · device/plc.py:2093"),
    inst("PLC_DC_V_SCALE", _G, "DC 전압 스케일(V/count)", F, 0.50316, 0.0, 10.0, safety=H, src="lib/config_common.py:698"),
    inst("PLC_DC_I_SCALE", _G, "DC 전류 스케일(A/count)", F, 0.00097405, 0.0, 1.0, safety=H, src="lib/config_common.py:699",
         note="v1 실측 미검증(lib/config_common.py:220-221) — 과전류 트립 시 raw 를 같이 남김"),
    p("POWER_OFF_ZERO_DEADLINE_S", _G, "D/A 전원 OFF 0 W 확인 최대 시간", "s", F, 10.0, 0.5, 120.0, safety=H,
      src="lib/config_common.py:554 · device/dc_power.py:437-439 · device/rf_power.py:875-877",
      desc="DC1·DC2·RF 연속·PC RF OFF 때 0 W 쓰기를 0.5 s 간격으로 이 시간까지 재시도. 실패면 출력 상태 미확인"),
)

_UNUSED = "v1 코드에서 읽는 곳 없음(설정 로더의 이름·형식 검사만)"
DROPPED: tuple = (
    Dropped("common.DC_RAMP_STEP", _UNUSED + " — DC 연속은 램프 없이 목표 즉시 전송", "lib/config_common.py:206"),
    Dropped("common.DC_PARAM_WATT_TO_DAC", _UNUSED, "lib/config_common.py:236"),
    Dropped("common.DC_OFFSET_WATT_TO_DAC", _UNUSED, "lib/config_common.py:237"),
    Dropped("common.DC_PARAM_ADC_TO_VOLT", _UNUSED, "lib/config_common.py:238"),
    Dropped("common.DC_OFFSET_ADC_TO_VOLT", _UNUSED, "lib/config_common.py:239"),
    Dropped("common.DC_PARAM_ADC_TO_AMP", _UNUSED, "lib/config_common.py:240"),
    Dropped("common.DC_OFFSET_ADC_TO_AMP", _UNUSED, "lib/config_common.py:241"),
)

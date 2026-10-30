"""PLC 값 변환(챔버12) — W↔DAC 코드, DC V·I 스케일, RF 피드백 보정, 명령 실행 상한. 순수 함수(v1 device/plc.py 근거)."""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple


def linear_to_dac(value: float, vmin: float, vmax: float, full_scale: int, offset: int = 0) -> int:
    """W → DAC 코드(device/plc.py:2074-2082): 클램프 → round((v−vmin)/span×FS + offset) → 0~FS."""
    v = max(vmin, min(vmax, float(value)))
    span = max(1e-9, vmax - vmin)
    return max(0, min(int(full_scale), int(round(((v - vmin) / span) * full_scale + offset))))


def dac_code(cfg: Dict[str, Any], power_w: float) -> int:
    return linear_to_dac(power_w, float(cfg["PLC_DC_POWER_MIN_W"]), float(cfg["PLC_DC_POWER_MAX_W"]),
                         int(cfg["PLC_DC_DAC_FULL_SCALE"]), int(cfg["PLC_DC_DAC_OFFSET"]))


def dc_scale(cfg: Dict[str, Any], v_raw: int, i_raw: int) -> Tuple[float, float, float]:
    """(P, V, I) = (V×I, raw×PLC_DC_V_SCALE, raw×PLC_DC_I_SCALE) (device/plc.py:2160-2164)."""
    v = float(v_raw) * float(cfg["PLC_DC_V_SCALE"])
    i = float(i_raw) * float(cfg["PLC_DC_I_SCALE"])
    return v * i, v, i


def rf_regs(rf_ch: int) -> Tuple[str, str]:
    return ("DCV_READ_2", "DCV_READ_3") if int(rf_ch) == 1 else ("DCV_READ_4", "DCV_READ_5")


def rf_scale(cfg: Dict[str, Any], f_raw: int, r_raw: int, rf_ch: int, zeroing: Optional[bool] = None) -> Dict[str, float]:
    """RF 피드백 a·raw+b, 제로 보정(기본 CH1 만), 음수 0, 소수 1자리(device/plc.py:2186-2218)."""
    if int(rf_ch) == 2:
        f_w = cfg["PLC_RF_CH2_FWD_A"] * float(f_raw) + cfg["PLC_RF_CH2_FWD_B"]
        r_w = cfg["PLC_RF_CH2_REF_A"] * float(r_raw) + cfg["PLC_RF_CH2_REF_B"]
    else:
        f_w = cfg["PLC_RF_CH1_FWD_A"] * float(f_raw) + cfg["PLC_RF_CH1_FWD_B"]
        r_w = cfg["PLC_RF_CH1_REF_A"] * float(r_raw) + cfg["PLC_RF_CH1_REF_B"]
    if (int(rf_ch) == 1) if zeroing is None else bool(zeroing):
        f_w -= float(cfg["PLC_RF_FORWARD_ZERO_W"])
        r_w -= float(cfg["PLC_RF_REFLECTED_ZERO_W"])
    return {"forward": round(max(0.0, f_w), 1), "reflected": round(max(0.0, r_w), 1)}


def cmd_grace_s(cfg: Dict[str, Any]) -> float:
    """명령 하나의 실행 상한 = v1 취소 유예 timeout×(retry+2) + delay×(retry+1) + 1 = 10.5 s(device/plc.py:1109-1115)."""
    r = int(cfg["PLC_RECONNECT_RETRY"])
    return float(cfg["PLC_TIMEOUT_S"]) * (r + 2) + float(cfg["PLC_RECONNECT_DELAY_S"]) * (r + 1) + 1.0

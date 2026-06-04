# -*- coding: utf-8 -*-
# plc.py
"""
plc.py — Modbus-TCP PLC 컨트롤러 — 단일 클래스 통합판

개요
----
- 코일 래치/타이머 동작을 고려한 안전 IO
- 동기 pymodbus 클라이언트를 asyncio에서 안전하게 사용(직렬화, 스레드 위임, 간격 보장, 하트비트, 자동 재연결).
- pymodbus 2.x / 3.x 호환: 'unit' / 'slave' 자동 판별.
- 고수준 API(door/gate/main_shutter/vent/turbo/lift_pin/gas) + 저수준 IO를 **AsyncPLC 하나**로 제공.
"""

from __future__ import annotations

import asyncio
import inspect
import time
import os
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, Optional, Iterable
from contextlib import asynccontextmanager

from pymodbus.pdu import ExceptionResponse
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException

from lib import config_common as cfgc   # ✅ 추가: Config 팝업에서 바뀐 값 소스
from util.log_hub import DailyCsvListAppender

# ======================================================
# 주소 맵 (단독 CLI에서 사용한 것과 동일)
# ======================================================

# === M (Coils) — FC1/FC5 ===
PLC_COIL_MAP: Dict[str, int] = {
    # 인터락/기타
    "R_P_1_OFF_TIMER": 1,        # M00001
    "R_P_2_OFF_TIMER": 2,        # M00002
    "L_R_P_OFF_TIMER": 3,        # M00003
    "R_V_1_인터락": 33,          # M00021
    "F_V_1_인터락": 34,          # M00022
    "M_V_1_인터락": 35,          # M00023
    "VENT_1_인터락": 36,         # M00024
    "TURBO_1_인터락": 37,        # M00025
    "DOOR_1_인터락": 38,         # M00026
    "G_V_1_인터락": 40,          # M00028
    "MAIN_SHUTTER_1_인터락": 42, # M0002A
    "Ar_1_GAS_인터락": 43,       # M0002B
    "O2_1_GAS_인터락": 44,       # M0002C
    "MAIN_1_GAS_인터락": 45,     # M0002D
    "L_R_V_인터락": 49,          # M00031
    "L_PIN_인터락": 50,          # M00032
    "L_VENT_인터락": 52,         # M00034
    "R_V_2_인터락": 65,          # M00041
    "F_V_2_인터락": 66,          # M00042
    "M_V_2_인터락": 67,          # M00043
    "VENT_2_인터락": 68,         # M00044
    "TURBO_2_인터락": 69,        # M00045
    "DOOR_2_인터락": 70,         # M00046
    "G_V_2_인터락": 72,          # M00048
    "MAIN_SHUTTER_2_인터락": 74, # M0004A
    "Ar_2_GAS_인터락": 75,       # M0004B
    "O2_2_GAS_인터락": 76,       # M0004C
    "N2_2_GAS_인터락": 77,       # M0004D
    "MAIN_2_GAS_인터락": 78,     # M0004E

    # Power SET (M00050~M00053)
    "DCV_SET_0": 80,            # M00050
    "DCV_SET_1": 81,            # M00051
    "DCV_SET_2": 82,            # M00052
    "DCV_SET_3": 83,            # M00053

    "LP_STEP1": 96,             # M00060
    "LP_STEP2": 97,             # M00061
    "L_VAC_READY_SW": 98,       # M00062
    "L_ATM": 99,                # M00063
    "L_VAC_NOT_READY": 100,     # M00064

    "Z1_UP": 112,               # M00070	
    "Z1_MID": 113,              # M00071	
    "Z1_DOWN": 114,             # M00072	
    "Z2_UP": 115,               # M00073	
    "Z2_MID": 116,              # M00074
    "Z2_DOWN":117,              # M00075

    # Z-MOTION 현재 위치
    "Z1_UP_LOCATION":   128,    # M00080          
    "Z1_MID_LOCATION":  129,    # M00081  
    "Z1_DOWN_LOCATION": 130,    # M00082  
    "Z2_UP_LOCATION":   131,    # M00083  
    "Z2_MID_LOCATION":  132,    # M00084
    "Z2_DOWN_LOCATION": 133,    # M00085  

    # Z/MID 영역 — 워드/비트 재매핑 적용
    "Z_M_P_1_STOP_SW": 160,    # M00100
    "Z_M_P_1_MID_SW":  176,    # M00110
    "Z_M_P_1_MID_CW":  177,    # M00111
    "Z_M_P_1_MID_CCW": 178,    # M00112
    "Z_M_P_2_MID_SW":  179,    # M00113
    "Z_M_P_2_MID_CW":  180,    # M00114
    "Z_M_P_2_MID_CCW": 181,    # M00115

    # ===== SW(명령) 영역 =====
    # M0012y
    "R_P_1_SW":          192,  # M00120
    "R_V_1_SW":          193,  # M00121
    "F_V_1_SW":          194,  # M00122
    "M_V_1_SW":          195,  # M00123
    "VENT_1_SW":         196,  # M00124
    "TURBO_1_SW":        197,  # M00125
    "DOOR_1_OPEN_SW":    198,  # M00126
    "DOOR_1_CLOSE_SW":   199,  # M00127
    "G_V_1_OPEN_SW":     200,  # M00128
    "G_V_1_CLOSE_SW":    201,  # M00129
    "MAIN_SHUTTER_1_SW": 202,  # M0012A
    "Ar_1_GAS_SW":       203,  # M0012B
    "O2_1_GAS_SW":       204,  # M0012C
    "MAIN_1_GAS_SW":     205,  # M0012D

    # M0013y
    "L_R_P_SW":          208,  # M00130
    "L_R_V_SW":          209,  # M00131
    "L_PIN_UP_SW":       210,  # M00132
    "L_PIN_DOWN_SW":     211,  # M00133
    "L_VENT_SW":         212,  # M00134
    "SW_RF_SELECT":      214,  # M00136
    "SW_POWER_SELECT":   215,  # M00137 
    "SHUTTER_2_SW":      216,  # M00138
    "SHUTTER_3_SW":      217,  # M00139
    "Z_M_P_1_SW":        218,  # M0013A
    "Z_M_P_1_CW_SW":     219,  # M0013B
    "Z_M_P_1_CCW_SW":    220,  # M0013C
    "Z_M_P_2_SW":        221,  # M0013D
    "Z_M_P_2_CW_SW":     222,  # M0013E
    "Z_M_P_2_CCW_SW":    223,  # M0013F

    # M0014y
    "R_P_2_SW":          224,  # M00140
    "R_V_2_SW":          225,  # M00141
    "F_V_2_SW":          226,  # M00142
    "M_V_2_SW":          227,  # M00143
    "VENT_2_SW":         228,  # M00144
    "TURBO_2_SW":        229,  # M00145
    "DOOR_2_OPEN_SW":    230,  # M00146
    "DOOR_2_CLOSE_SW":   231,  # M00147
    "G_V_2_OPEN_SW":     232,  # M00148
    "G_V_2_CLOSE_SW":    233,  # M00149
    "MAIN_SHUTTER_2_SW": 234,  # M0014A
    "Ar_2_GAS_SW":       235,  # M0014B
    "O2_2_GAS_SW":       236,  # M0014C
    "N2_2_GAS_SW":       237,  # M0014D
    "MAIN_2_GAS_SW":     238,  # M0014E
    "SHUTTER_1_SW":      239,  # M0014F

    # 램프/기타
    "Z_M_P_2_STOP_SW":   336,   # M00200
    "VENT_1_LAMP":       356,   # M00224
    "DOOR_1_OPEN_LAMP":  358,   # M00226
    "DOOR_1_CLOSE_LAMP": 359,   # M00227
    "G_V_1_OPEN_LAMP":   360,   # M00228
    "G_V_1_CLOSE_LAMP":  361,   # M00229
    "L_PIN_UP_LAMP":     370,   # M00232
    "L_PIN_DOWN_LAMP":   371,   # M00233
    "L_VENT_LAMP":       372,   # M00234
    "VENT_2_LAMP":       388,   # M00244
    "DOOR_2_OPEN_LAMP":  390,   # M00246
    "DOOR_2_CLOSE_LAMP": 391,   # M00247
    "G_V_2_OPEN_LAMP":   392,   # M00248
    "G_V_2_CLOSE_LAMP":  393,   # M00249

    # Loading Sensor Lamp
    "LOADING_1_SENSOR_LAMP": 480, # M00300
    "LOADING_2_SENSOR_LAMP": 481, # M00301

    "BUZZER_STOP_SW":      1616,  # M01000
    "GAUGE_1_A_INTERLOCK": 2416,  # M01500
    "GAUGE_1_B_INTERLOCK": 2576,  # M01600
    "L_GAUGE_A_INTERLOCK": 2736,  # M01700
    "GAUGE_2_A_INTERLOCK": 2896,  # M01800
    "GAUGE_2_B_INTERLOCK": 3056,  # M01900
}

# === D (Holding Registers) — FC3/FC6 ===
PLC_REG_MAP: Dict[str, int] = {
    "DCV_READ_0": 0,
    "DCV_READ_1": 1,
    "DCV_READ_2": 2,
    "DCV_READ_3": 3,
    "DCV_READ_4": 8,
    "DCV_READ_5": 9,
    "DCV_READ_6": 10,
    "DCV_READ_7": 11,
    "DCV_WRITE_0": 4,
    "DCV_WRITE_1": 5,
    "DCV_WRITE_2": 6,
    "DCV_WRITE_3": 7,
}

# === T (Timers as registers) — FC3/FC6 ===
PLC_TIMER_MAP: Dict[str, int] = {
    "AIR_ALARM_TIMER": 0,
    "Z_M_P_1_UP_DELAY": 1,
    "Z_M_P_1_DOWN_DELAY": 2,
    "Z_M_P_1_MID_DELAY": 3,
    "Z_M_P_2_UP_DELAY": 4,
    "Z_M_P_2_DOWN_DELAY": 5,
    "Z_M_P_2_MID_DELAY": 6,
    "VENT_1_EXT_TIME": 7,
    "VENT_2_EXT_TIME": 8,
    "L_VENT_EXT_TIME": 9,
    "GAUGE_1_SP1_ON_DELAY": 16,
    "GAUGE_1_SP1_OFF_DELAY": 17,
    "GAUGE_1_SP2_ON_DELAY": 18,
    "GAUGE_1_SP2_OFF_DELAY": 19,
    "GAUGE_1_SP3_ON_DELAY": 20,
    "GAUGE_1_SP3_OFF_DELAY": 21,
    "GAUGE_2_SP1_ON_DELAY": 22,
    "GAUGE_2_SP1_OFF_DELAY": 23,
    "GAUGE_2_SP2_ON_DELAY": 24,
    "GAUGE_2_SP2_OFF_DELAY": 25,
    "GAUGE_2_SP3_ON_DELAY": 26,
    "GAUGE_2_SP3_OFF_DELAY": 27,
    "TURBO_1_TIMER_인터락": 32,
    "TURBO_2_TIMER_인터락": 33,
    "ATM_ALARM_TIMER": 34,
}

# ======================================================
# 에러코드
# ======================================================

class PLCError(RuntimeError):
    def __init__(self, code: str, message: str, *, op: str | None = None, addr: int | None = None, cause: Exception | None = None):
        super().__init__(message)
        self.code = code
        self.op = op
        self.addr = addr
        self.cause = cause


# ======================================================
# 설정
# ======================================================

@dataclass
class PLCConfig:
    ip: str = "192.168.1.2"
    port: int = 502
    unit: int = 1
    timeout_s: float = 2.0
    inter_cmd_gap_s: float = 0.15
    heartbeat_s: float = 15.0
    pulse_ms: int = 180  # momentary 기본 펄스폭(ms)

    # ✅ connect 재시도 (총 시도 횟수 = 1 + connect_retry)
    connect_retry: int = 2
    connect_retry_delay_s: float = 0.5

    # ⬇️ 추가: 성능/경합 모니터링 임계치(ms)
    lock_warn_ms: float = 1000.0   # 락 획득 대기시간 경고 임계
    io_warn_ms: float   = 1500.0   # 락 내부 I/O 소요시간 경고 임계

    # ── DC Power 설정 ───────────────────────────────────────────
    # 원하는 파워[W] → DAC 코드 변환용. 직선 스케일(0~FULL)
    dc_power_min_w: float = 0.0
    dc_power_max_w: float = 1000.0      # 장비 정격에 맞춰 수정
    dc_dac_full_scale: int = 4000      # DAC 스케일 4000
    dc_dac_offset: int = 0             # 필요 시 오프셋

    # WRITE 인덱스(D00004=WRITE_0)를 기본으로 사용
    dc_write_index: int = 0            # 0→D00004, 1→D00005 ...

    # READ 스케일: 0..4000 카운트 → 0..2000 V, 0..4 A
    # (필요시 현장 값에 맞게 두 수치만 조정하세요:  dc_v_scale=V_FS/4000, dc_i_scale=I_FS/4000)
    dc_v_scale: float = 0.50316      # ≈ 2012.6 V / 4000 ct
    dc_i_scale: float = 0.00097405   # ≈ 3.90 A / 4000 ct

    # RF 피드백(ADC→W) 보정 계수 — 기본(=CH1용)
    rf_fwd_a: float = 0.1503488383
    rf_fwd_b: float = 3.0664228165
    rf_ref_a: float = 0.1565388751
    rf_ref_b: float = 12.2067054791

    # ✅ CH2 전용 보정 계수 (rfpower 스케일.xlsx 기반)
    rf2_fwd_a: float = 0.15059   # Forward slope
    rf2_fwd_b: float = -0.598    # Forward intercept
    rf2_ref_a: float = 0.12940   # Reflected slope
    rf2_ref_b: float = -0.267    # Reflected intercept

    # 🔧 현장 제로 보정치(패널 idle 보정). 필요 시 현장에서 수치만 바꾸세요.
    rf_forward_zero_w: float = 4.0   # forp idle offset
    rf_reflected_zero_w: float = 14.0  # refp idle offset

# ======================================================
# 단일 클래스: AsyncPLC (저수준+고수준)
# ======================================================

class AsyncPLC:
    """
    Modbus 저수준 + 고수준 제어를 하나로 제공.
    - 저수준: connect/close, read/write coil/reg, 직렬화, 하트비트, 재연결
    - 고수준: door/gate/main_shutter/vent/turbo/lift_pin/gas, snapshot 등
    - DI용: set(name,on,ch=1)로 'MV/MS/AR/O2/N2/MAIN/G1/G2/G3' 논리명 처리
    """

    def __init__(self, ip: str = "192.168.1.2", port: int = 502, unit: int = 1,
                 timeout_s: float = 2.0, inter_cmd_gap_s: float = 0.15,
                 heartbeat_s: float = 15.0, pulse_ms: int = 180, logger=None):

        self.cfg = PLCConfig(ip=ip, port=port, unit=unit, timeout_s=timeout_s,
                             inter_cmd_gap_s=inter_cmd_gap_s, heartbeat_s=heartbeat_s,
                             pulse_ms=pulse_ms)
        
        # ✅ config_common 값이 있으면 덮어써서 “초기값”을 config 기준으로 맞춤
        self._apply_cfg_from_config()

        self._client: Optional[ModbusTcpClient] = None
        self._uid_kw: Optional[str] = None  # 'unit' 또는 'slave'
        self._lock = asyncio.Lock()

        # ✅ 양보 메커니즘: 외부 우선순위 PLC I/O 대기자 카운터
        #    snapshot loop가 매 block 직전 이 값을 확인하여 양보 여부 결정
        self._priority_waiters: int = 0
        self._last_io_ts = 0.0

        self._last_io_ts = 0.0
        self._hb_task: Optional[asyncio.Task] = None
        self._closed = False
        self._hb_paused: bool = False   # ← 추가
        self.log = logger or (lambda *a, **k: None)

        # 혼합 대/소문자/논리명 별칭
        self._ALIASES: Dict[str, str] = {}
        for ch in (1, 2):
            self._ALIASES[f"AR_{ch}_GAS_SW"] = f"Ar_{ch}_GAS_SW"
            self._ALIASES[f"AR_{ch}_GAS_인터락"] = f"Ar_{ch}_GAS_인터락"
        # 논리명 → 실제 코일 이름
        self._LOGICAL: Dict[str, str] = {
            "MV@1": "M_V_1_SW",
            "MV@2": "M_V_2_SW",
            "MS@1": "MAIN_SHUTTER_1_SW",
            "MS@2": "MAIN_SHUTTER_2_SW",
            "G1": "SHUTTER_1_SW",
            "G2": "SHUTTER_2_SW",
            "G3": "SHUTTER_3_SW",
        }

    # ============== UI로 파라미터 수정 ==============
    def _apply_cfg_from_config(self) -> None:
        """
        config_common(cfgc)에 정의된 값이 있으면 self.cfg에 덮어쓴다.
        - cfgc에 키가 없으면 기존 self.cfg 값을 유지(= 안전한 fallback)
        """
        # ---- 연결/통신 기본 ----
        self.cfg.ip = str(getattr(cfgc, "PLC_TCP_HOST", self.cfg.ip))
        self.cfg.port = int(getattr(cfgc, "PLC_TCP_PORT", self.cfg.port))
        self.cfg.unit = int(getattr(cfgc, "PLC_UNIT", self.cfg.unit))
        self.cfg.timeout_s = float(getattr(cfgc, "PLC_TIMEOUT_S", self.cfg.timeout_s))

        # inter_cmd_gap: ms로 관리하고 싶으면 PLC_CMD_GAP_MS를 쓰고, 없으면 기존 값(s) 유지
        if hasattr(cfgc, "PLC_CMD_GAP_MS"):
            self.cfg.inter_cmd_gap_s = float(getattr(cfgc, "PLC_CMD_GAP_MS")) / 1000.0

        # watchdog/heartbeat
        self.cfg.heartbeat_s = float(getattr(cfgc, "PLC_WATCHDOG_INTERVAL_S", self.cfg.heartbeat_s))

        # 재연결 정책
        self.cfg.connect_retry = int(getattr(cfgc, "PLC_RECONNECT_RETRY", self.cfg.connect_retry))
        self.cfg.connect_retry_delay_s = float(getattr(cfgc, "PLC_RECONNECT_DELAY_S", self.cfg.connect_retry_delay_s))

        # momentary pulse 폭
        self.cfg.pulse_ms = int(getattr(cfgc, "PLC_CMD_PULSE_MS", self.cfg.pulse_ms))

        # 성능 경고 임계(ms)
        self.cfg.lock_warn_ms = float(getattr(cfgc, "PLC_LOCK_WARN_MS", self.cfg.lock_warn_ms))
        self.cfg.io_warn_ms = float(getattr(cfgc, "PLC_IO_WARN_MS", self.cfg.io_warn_ms))

        # ------------------------------------------------------
        # ✅ PLC 보정계수 / 스케일 (UI에서 수정 가능)
        # ------------------------------------------------------

        # DC write/scale
        self.cfg.dc_power_min_w = float(getattr(cfgc, "PLC_DC_POWER_MIN_W", self.cfg.dc_power_min_w))
        self.cfg.dc_power_max_w = float(getattr(cfgc, "PLC_DC_POWER_MAX_W", self.cfg.dc_power_max_w))
        self.cfg.dc_dac_full_scale = int(getattr(cfgc, "PLC_DC_DAC_FULL_SCALE", self.cfg.dc_dac_full_scale))
        self.cfg.dc_dac_offset = int(getattr(cfgc, "PLC_DC_DAC_OFFSET", self.cfg.dc_dac_offset))
        self.cfg.dc_write_index = int(getattr(cfgc, "PLC_DC_WRITE_INDEX", self.cfg.dc_write_index))

        self.cfg.dc_v_scale = float(getattr(cfgc, "PLC_DC_V_SCALE", self.cfg.dc_v_scale))
        self.cfg.dc_i_scale = float(getattr(cfgc, "PLC_DC_I_SCALE", self.cfg.dc_i_scale))

        # RF CH1
        self.cfg.rf_fwd_a = float(getattr(cfgc, "PLC_RF_CH1_FWD_A", self.cfg.rf_fwd_a))
        self.cfg.rf_fwd_b = float(getattr(cfgc, "PLC_RF_CH1_FWD_B", self.cfg.rf_fwd_b))
        self.cfg.rf_ref_a = float(getattr(cfgc, "PLC_RF_CH1_REF_A", self.cfg.rf_ref_a))
        self.cfg.rf_ref_b = float(getattr(cfgc, "PLC_RF_CH1_REF_B", self.cfg.rf_ref_b))

        # RF CH2
        self.cfg.rf2_fwd_a = float(getattr(cfgc, "PLC_RF_CH2_FWD_A", self.cfg.rf2_fwd_a))
        self.cfg.rf2_fwd_b = float(getattr(cfgc, "PLC_RF_CH2_FWD_B", self.cfg.rf2_fwd_b))
        self.cfg.rf2_ref_a = float(getattr(cfgc, "PLC_RF_CH2_REF_A", self.cfg.rf2_ref_a))
        self.cfg.rf2_ref_b = float(getattr(cfgc, "PLC_RF_CH2_REF_B", self.cfg.rf2_ref_b))

        # RF zero offsets
        self.cfg.rf_forward_zero_w = float(getattr(cfgc, "PLC_RF_FORWARD_ZERO_W", self.cfg.rf_forward_zero_w))
        self.cfg.rf_reflected_zero_w = float(getattr(cfgc, "PLC_RF_REFLECTED_ZERO_W", self.cfg.rf_reflected_zero_w))


    async def apply_config(self, *, reconnect: bool = False) -> None:
        """
        Config 팝업에서 Apply 누른 뒤 호출용.
        reconnect=True면 ip/port 변경 시 즉시 재연결.
        """
        old_ip, old_port = self.cfg.ip, self.cfg.port
        self._apply_cfg_from_config()

        if reconnect and (self.cfg.ip != old_ip or self.cfg.port != old_port):
            await self.set_endpoint(self.cfg.ip, self.cfg.port, reconnect=True)

    # ---------- 연결/수명주기 ----------
    async def connect(self) -> None:
        self._closed = False

        # ✅ connect 직전에 config 값을 재적용 (Apply 후 재연결/다음 연결에 반영)
        self._apply_cfg_from_config()

        # ✅ 연결 성공 여부와 무관하게 하트비트 태스크는 살아있게(백그라운드 재연결용)
        if self._hb_task is None or self._hb_task.done():
            self._hb_task = asyncio.create_task(self._heartbeat_loop(), name="PLCHeartbeat")

        async with self._io_lock("connect"):
            await asyncio.to_thread(self._connect_sync)

        self.log("TCP 연결 성공: %s:%s (unit=%s)", self.cfg.ip, self.cfg.port, self.cfg.unit)

    async def close(self) -> None:
        self._closed = True
        # 하트비트 먼저 중지
        if self._hb_task:
            self._hb_task.cancel()
            try:
                await self._hb_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._hb_task = None
        # 🔒 모든 I/O와 동기화하여 안전 종료
        async with self._io_lock("close"):
            await asyncio.to_thread(self._close_sync)
        self.log("TCP 연결 종료")

    def is_connected(self) -> bool:
        try:
            return bool(self._client) and self._is_connected()
        except Exception:
            return False
        
    def is_busy(self) -> bool:
        """공정/메인 제어가 PLC I/O 중인지(락 점유 중인지)"""
        return self._lock.locked()

    # ---------- 내부 저수준 헬퍼 ----------
    def _is_connected(self) -> bool:
        c = self._client
        if not c:
            return False
        if hasattr(c, "connected"):
            try:
                return bool(getattr(c, "connected"))
            except Exception:
                pass
        if hasattr(c, "is_socket_open"):
            try:
                return bool(c.is_socket_open())  # type: ignore[attr-defined]
            except Exception:
                pass
        return False

    def _detect_uid_kw(self, method) -> Optional[str]:
        try:
            params = inspect.signature(method).parameters
            if "slave" in params:
                return "slave"
            if "unit" in params:
                return "unit"
        except Exception:
            pass
        return None

    def _uid_kwargs(self) -> dict:
        if self._uid_kw:
            return {self._uid_kw: self.cfg.unit}
        return {}

    def _ensure_ok(self, resp, *, op: str, addr: int | None = None):
        if resp is None:
            raise PLCError("E402", "PLC 응답 없음(None)", op=op, addr=addr)
        if isinstance(resp, ExceptionResponse):
            raise PLCError("E403", f"PLC Modbus ExceptionResponse: {resp}", op=op, addr=addr)
        if hasattr(resp, "isError") and resp.isError():
            raise PLCError("E403", f"PLC Modbus isError(): {resp}", op=op, addr=addr)
        return resp

    def _is_reset_err(self, e: Exception) -> bool:
        s = str(e).lower()
        return (
            ("10054" in s)
            or ("reset by peer" in s)
            or ("connectionreseterror" in s)
            or ("broken pipe" in s)
            or ("connection aborted" in s)
            or ("not connected" in s)
        )

    async def _throttle_and_heartbeat(self):
        now = time.monotonic()
        delta = now - self._last_io_ts
        if delta < self.cfg.inter_cmd_gap_s:
            await asyncio.sleep(self.cfg.inter_cmd_gap_s - delta)
        # ✅ 여기서는 heartbeat I/O 하지 않음 (중복 제거)
        self._last_io_ts = time.monotonic()

    def _connect_sync(self) -> None:
        if self._client is None:
            self._client = ModbusTcpClient(self.cfg.ip, port=self.cfg.port, timeout=self.cfg.timeout_s)

        if not self._is_connected():
            ok = False
            last_exc: Optional[Exception] = None

            for _ in range(int(getattr(self.cfg, "connect_retry", 0)) + 1):
                try:
                    ok = bool(self._client.connect())
                except Exception as e:
                    last_exc = e
                    ok = False

                if ok:
                    break

                # 실패 → close + client 재생성
                try:
                    self._client.close()
                except Exception:
                    pass
                self._client = None
                self._uid_kw = None

                self._client = ModbusTcpClient(self.cfg.ip, port=self.cfg.port, timeout=self.cfg.timeout_s)
                time.sleep(max(0.0, float(getattr(self.cfg, "connect_retry_delay_s", 0.0))))

            if not ok:
                # 최종 실패 → 상태 정리 후 E401
                try:
                    if self._client is not None:
                        self._client.close()
                except Exception:
                    pass
                self._client = None
                self._uid_kw = None

                if last_exc is not None:
                    raise PLCError("E401", f"Modbus TCP 연결 실패 ({self.cfg.ip}:{self.cfg.port}) - {last_exc!r}", op="connect")
                raise PLCError("E401", f"Modbus TCP 연결 실패 ({self.cfg.ip}:{self.cfg.port})", op="connect")

            # 성공 시 keepalive
            try:
                import socket
                if getattr(self._client, "socket", None):
                    self._client.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            except Exception:
                pass

            self._last_io_ts = time.monotonic()

        if self._uid_kw is None and self._client is not None:
            self._uid_kw = self._detect_uid_kw(self._client.read_coils)

    def _close_sync(self):
        if self._client is not None:
            try:
                self._client.close()
            finally:
                self._client = None
                self._uid_kw = None

    async def _heartbeat_loop(self):
        try:
            while not self._closed:
                await asyncio.sleep(max(1.0, self.cfg.heartbeat_s * 0.75))
                if self._closed:
                    break

                # ✅ 0) pause 상태면 워치독 tick 스킵 (cancel 없이 멈춤)
                if self._hb_paused:
                    continue

                # ✅ 1) PLC가 이미 바쁘면(락 점유 중) 워치독은 이번 tick 스킵
                if self._lock.locked():
                    continue

                try:
                    # ✅ 2) 워치독은 "가벼운 ping"만. (여기서는 재연결까지 하지 않음)
                    #    가능하면 read_coil(0) 대신 low-level read_coils 1개가 더 안전.
                    async with self._io_lock("heartbeat", addr=0):
                        await asyncio.to_thread(self._connect_sync)
                        if self._client is None:
                            continue
                        await asyncio.to_thread(self._client.read_coils, 0, count=1, **self._uid_kwargs())

                except Exception:
                    # ✅ 3) 실패 시 재연결을 락 안에서 길게 하지 말고,
                    #    소켓 꼬임 방지를 위해 close만 조용히 시도(선택)
                    try:
                        if not self._lock.locked():
                            async with self._io_lock("hb_close"):
                                await asyncio.to_thread(self._close_sync)
                    except Exception:
                        pass

        except asyncio.CancelledError:
            return

    # ---------- 저수준 IO(직렬화) ----------
    def _to_plc_error(self, op: str, addr: int | None, e: Exception) -> PLCError:
        if isinstance(e, PLCError):
            return e
        s = str(e).lower()

        # 연결/끊김 계열
        if self._is_reset_err(e) or ("connection" in s and "reset" in s) or ("refused" in s) or ("no route" in s):
            return PLCError("E401", f"PLC 연결 오류: {type(e).__name__}: {e}", op=op, addr=addr, cause=e)

        # timeout/응답없음 계열
        if ("timeout" in s) or ("timed out" in s) or ("no answer" in s) or ("응답" in s and "없" in s):
            return PLCError("E402", f"PLC timeout/응답없음: {type(e).__name__}: {e}", op=op, addr=addr, cause=e)

        # Modbus 계열
        if isinstance(e, ModbusException) or ("modbus" in s):
            return PLCError("E403", f"PLC Modbus 오류: {type(e).__name__}: {e}", op=op, addr=addr, cause=e)

        return PLCError("E403", f"PLC 통신 오류: {type(e).__name__}: {e}", op=op, addr=addr, cause=e)

    async def read_coil(self, addr: int) -> bool:
        op = "read_coil"
        async with self._io_lock(op, addr=addr):
            try:
                await asyncio.to_thread(self._connect_sync)
                await self._throttle_and_heartbeat()
                try:
                    resp = await asyncio.to_thread(self._client.read_coils, addr, count=1, **self._uid_kwargs())
                except Exception as e:
                    pe = self._to_plc_error(op, addr, e)
                    if pe.code in ("E401", "E402") or self._is_reset_err(e):
                        await asyncio.to_thread(self._close_sync)
                        await asyncio.to_thread(self._connect_sync)
                        await self._throttle_and_heartbeat()
                        resp = await asyncio.to_thread(self._client.read_coils, addr, count=1, **self._uid_kwargs())
                    else:
                        raise pe from e

                self._ensure_ok(resp, op=op, addr=addr)
                return bool(resp.bits[0])

            except Exception as e:
                raise self._to_plc_error(op, addr, e) from e

    async def write_coil(self, addr: int, state: bool) -> None:
        op = "write_coil"
        async with self._io_lock(op, addr=addr):
            try:
                await asyncio.to_thread(self._connect_sync)
                await self._throttle_and_heartbeat()

                try:
                    resp = await asyncio.to_thread(self._client.write_coil, addr, bool(state), **self._uid_kwargs())
                except Exception as e:
                    pe = self._to_plc_error(op, addr, e)
                    # ✅ reset 뿐 아니라 timeout/연결계열(E401/E402)도 1회 재연결 후 재시도
                    if pe.code in ("E401", "E402") or self._is_reset_err(e):
                        await asyncio.to_thread(self._close_sync)
                        await asyncio.to_thread(self._connect_sync)
                        await self._throttle_and_heartbeat()
                        resp = await asyncio.to_thread(self._client.write_coil, addr, bool(state), **self._uid_kwargs())
                    else:
                        raise pe from e

                self._ensure_ok(resp, op=op, addr=addr)

            except Exception as e:
                raise self._to_plc_error(op, addr, e) from e

    async def read_reg(self, addr: int) -> int:
        op = "read_reg"
        async with self._io_lock(op, addr=addr):
            try:
                await asyncio.to_thread(self._connect_sync)
                await self._throttle_and_heartbeat()
                try:
                    resp = await asyncio.to_thread(self._client.read_holding_registers, addr, count=1, **self._uid_kwargs())
                except Exception as e:
                    pe = self._to_plc_error(op, addr, e)
                    if pe.code in ("E401", "E402") or self._is_reset_err(e):
                        await asyncio.to_thread(self._close_sync)
                        await asyncio.to_thread(self._connect_sync)
                        await self._throttle_and_heartbeat()
                        resp = await asyncio.to_thread(self._client.read_holding_registers, addr, count=1, **self._uid_kwargs())
                    else:
                        raise pe from e

                self._ensure_ok(resp, op=op, addr=addr)
                return int(resp.registers[0])

            except Exception as e:
                raise self._to_plc_error(op, addr, e) from e

    async def write_reg(self, addr: int, value: int) -> None:
        op = "write_reg"
        async with self._io_lock(op, addr=addr):
            try:
                await asyncio.to_thread(self._connect_sync)
                await self._throttle_and_heartbeat()

                try:
                    resp = await asyncio.to_thread(self._client.write_register, addr, int(value), **self._uid_kwargs())
                except Exception as e:
                    pe = self._to_plc_error(op, addr, e)
                    if pe.code in ("E401", "E402") or self._is_reset_err(e):
                        await asyncio.to_thread(self._close_sync)
                        await asyncio.to_thread(self._connect_sync)
                        await self._throttle_and_heartbeat()
                        resp = await asyncio.to_thread(self._client.write_register, addr, int(value), **self._uid_kwargs())
                    else:
                        raise pe from e

                self._ensure_ok(resp, op=op, addr=addr)

            except Exception as e:
                raise self._to_plc_error(op, addr, e) from e
            
    # ---------- 블록(배열) 읽기 ----------
    async def read_coils_block(self, start_addr: int, count: int, *, priority: str = "high") -> list[bool]:
        """
        FC1: 연속 코일을 한 번에 읽고 resp.bits[] 배열로 받는다.
        (코일을 하나씩 read_coil 반복하는 방식이 아니라, PLC가 배열로 응답)

        priority: "high" (기본, 외부) / "low" (snapshot 등 양보 가능 백그라운드)
        """
        if count <= 0:
            return []

        op = "read_coils_block"
        async with self._io_lock(op, addr=int(start_addr), priority=priority):
            try:
                await asyncio.to_thread(self._connect_sync)
                await self._throttle_and_heartbeat()

                try:
                    resp = await asyncio.to_thread(
                        self._client.read_coils,
                        int(start_addr),
                        count=int(count),
                        **self._uid_kwargs(),
                    )
                except Exception as e:
                    pe = self._to_plc_error(op, int(start_addr), e)
                    if pe.code in ("E401", "E402") or self._is_reset_err(e):
                        await asyncio.to_thread(self._close_sync)
                        await asyncio.to_thread(self._connect_sync)
                        await self._throttle_and_heartbeat()
                        resp = await asyncio.to_thread(
                            self._client.read_coils,
                            int(start_addr),
                            count=int(count),
                            **self._uid_kwargs(),
                        )
                    else:
                        raise pe from e

                self._ensure_ok(resp, op=op, addr=int(start_addr))
                bits = list(getattr(resp, "bits", []) or [])
                if len(bits) < count:
                    bits.extend([False] * (count - len(bits)))
                return [bool(x) for x in bits[:count]]

            except Exception as e:
                raise self._to_plc_error(op, int(start_addr), e) from e
            
    def _build_sparse_ranges(
        self,
        addrs: Iterable[int],
        *,
        max_gap: int = 8,
        max_span: int = 64,
    ) -> list[tuple[int, int]]:
        """
        실제 주소들만 기반으로, 가까운 주소끼리만 묶어서
        (start, count) 범위를 만든다.
        - max_gap: 다음 주소와의 간격이 이 값 이하일 때만 같은 블록으로 묶음
        - max_span: 한 블록 최대 길이
        """
        xs = sorted({int(a) for a in addrs})
        if not xs:
            return []

        ranges: list[tuple[int, int]] = []
        start = xs[0]
        prev = xs[0]

        for a in xs[1:]:
            span_if_extend = a - start + 1
            gap = a - prev

            if gap <= max_gap and span_if_extend <= max_span:
                prev = a
                continue

            ranges.append((start, prev - start + 1))
            start = prev = a

        ranges.append((start, prev - start + 1))
        return ranges
            
    async def snapshot_all_coils_fast(
        self,
        *,
        keys: Optional[Iterable[str]] = None,
        max_coils_per_req: int = 64,
        max_gap: int = 8,
        skip_if_busy: bool = True,
    ) -> Dict[str, bool]:
        """
        PLC_COIL_MAP에 있는 실제 사용 코일만 sparse block read로 스냅샷.
        - 기존처럼 min~max 전체를 훑지 않음
        - 가까운 주소끼리만 작은 블록으로 읽음
        - skip_if_busy=True면 공정 제어 중에는 스킵
        """
        if skip_if_busy and self.is_busy():
            return {}

        use_keys = list(keys) if keys is not None else list(PLC_COIL_MAP.keys())
        addr_map = {k: PLC_COIL_MAP[k] for k in use_keys if k in PLC_COIL_MAP}
        if not addr_map:
            return {}

        out: Dict[str, bool] = {}
        ranges = self._build_sparse_ranges(
            addr_map.values(),
            max_gap=max(0, int(max_gap)),
            max_span=max(1, int(max_coils_per_req)),
        )

        for start, cnt in ranges:
            # ✅ 양보 체크: 외부 우선순위 PLC I/O 대기자가 있으면 즉시 종료
            #    (사용자 정책: 이번 tick은 csv 비워두고, 다음 tick에서 재시도)
            if self._priority_waiters > 0:
                return {}

            bits = await self.read_coils_block(start, cnt, priority="low")
            end = start + cnt

            for k, a in addr_map.items():
                if start <= a < end:
                    out[k] = bool(bits[a - start])

        return out

    async def read_coils(self, addrs: Iterable[int]) -> Dict[int, bool]:
        out: Dict[int, bool] = {}
        for a in addrs:
            out[a] = await self.read_coil(a)
        return out

    async def pulse(self, addr: int, ms: Optional[int] = None) -> None:
        ms = self.cfg.pulse_ms if ms is None else ms
        await self.write_coil(addr, True)
        await asyncio.sleep(max(0.03, ms / 1000.0))
        await self.write_coil(addr, False)

    # ---------- 이름/주소 해석 ----------
    def _addr(self, name_or_addr: Any) -> int:
        # 정수 주소 직접 지원
        if isinstance(name_or_addr, int):
            return name_or_addr
        # 별칭 보정(혼합 대/소문자 등)
        key = str(name_or_addr)
        if key in self._ALIASES:
            key = self._ALIASES[key]
        # 맵 조회
        if key in PLC_COIL_MAP:
            return PLC_COIL_MAP[key]
        if key in PLC_REG_MAP:
            return PLC_REG_MAP[key]
        raise KeyError(f"알 수 없는 주소/이름: {name_or_addr}")

    # ---------- 공통 고수준 ----------
    async def write_switch(self, name_or_addr: Any, on: bool,
                           *, momentary: bool = False, pulse_ms: Optional[int] = None) -> None:
        addr = self._addr(name_or_addr)
        if momentary:
            self.log("pulse %s (addr=%d, %sms)", name_or_addr, addr, str(pulse_ms or self.cfg.pulse_ms))
            await self.pulse(addr, ms=pulse_ms)
        else:
            self.log("set %s <- %s (addr=%d)", name_or_addr, on, addr)
            await self.write_coil(addr, bool(on))

    async def press_switch(self, name_or_addr: Any, pulse_ms: Optional[int] = None) -> None:
        await self.write_switch(name_or_addr, True, momentary=True, pulse_ms=pulse_ms)

    async def read_bit(self, name_or_addr: Any) -> bool:
        addr = self._addr(name_or_addr)
        v = await self.read_coil(addr)
        self.log("read %s (addr=%d) -> %s", name_or_addr, addr, v)
        return v

    async def write_reg_name(self, name_or_addr: Any, value: int) -> None:
        addr = self._addr(name_or_addr)
        await self.write_reg(addr, int(value))
        self.log("write reg %s (addr=%d) <- %d", name_or_addr, addr, value)

    async def read_reg_name(self, name_or_addr: Any) -> int:
        addr = self._addr(name_or_addr)
        v = await self.read_reg(addr)
        #self.log("read reg %s (addr=%d) -> %d", name_or_addr, addr, v)
        return v
    
    async def read_regs_block(self, start_addr: int, count: int, *, priority: str = "high") -> list[int]:
        """
        FC3: 연속 holding register를 한 번에 읽는다.

        priority: "high" (기본, 외부) / "low" (snapshot 등 양보 가능 백그라운드)
        """
        if count <= 0:
            return []

        op = "read_regs_block"
        async with self._io_lock(op, addr=int(start_addr), priority=priority):
            try:
                await asyncio.to_thread(self._connect_sync)
                await self._throttle_and_heartbeat()

                try:
                    resp = await asyncio.to_thread(
                        self._client.read_holding_registers,
                        int(start_addr),
                        count=int(count),
                        **self._uid_kwargs(),
                    )
                except Exception as e:
                    pe = self._to_plc_error(op, int(start_addr), e)
                    if pe.code in ("E401", "E402") or self._is_reset_err(e):
                        await asyncio.to_thread(self._close_sync)
                        await asyncio.to_thread(self._connect_sync)
                        await self._throttle_and_heartbeat()
                        resp = await asyncio.to_thread(
                            self._client.read_holding_registers,
                            int(start_addr),
                            count=int(count),
                            **self._uid_kwargs(),
                        )
                    else:
                        raise pe from e

                self._ensure_ok(resp, op=op, addr=int(start_addr))
                regs = list(getattr(resp, "registers", []) or [])
                if len(regs) < count:
                    regs.extend([0] * (count - len(regs)))
                return [int(x) for x in regs[:count]]

            except Exception as e:
                raise self._to_plc_error(op, int(start_addr), e) from e
            
    async def snapshot_regs_fast(
        self,
        *,
        keys: Optional[Iterable[str]] = None,
        skip_if_busy: bool = True,
    ) -> Dict[str, int]:
        """
        PLC_REG_MAP에 있는 holding register 스냅샷.
        현재 DCV 계열은 D00000~D00011로 연속이라 1회 block read로 충분.
        """
        if skip_if_busy and self.is_busy():
            return {}

        use_keys = list(keys) if keys is not None else list(PLC_REG_MAP.keys())
        addr_map = {k: PLC_REG_MAP[k] for k in use_keys if k in PLC_REG_MAP}
        if not addr_map:
            return {}

        # ✅ 양보 체크: 외부 우선순위 대기자 있으면 즉시 빈 dict
        if self._priority_waiters > 0:
            return {}

        mn = min(addr_map.values())
        mx = max(addr_map.values())

        regs = await self.read_regs_block(mn, mx - mn + 1, priority="low")

        out: Dict[str, int] = {}
        for k, a in addr_map.items():
            out[k] = int(regs[a - mn])
        return out

    # ---------- Faduino 스타일 고수준 ----------
    async def door(self, chamber: int, *, open: bool, momentary: bool = False) -> None:
        name = f"DOOR_{chamber}_{'OPEN' if open else 'CLOSE'}_SW"
        await self.write_switch(name, True, momentary=momentary)
        self.log("Door%d %s", chamber, "OPEN" if open else "CLOSE")

    async def gate_valve(self, chamber: int, *, open: bool, momentary: bool = False) -> None:
        name = f"G_V_{chamber}_{'OPEN' if open else 'CLOSE'}_SW"
        await self.write_switch(name, True, momentary=momentary)
        self.log("GV%d %s", chamber, "OPEN" if open else "CLOSE")

    async def main_shutter(self, chamber: int, *, open: bool, momentary: bool = False) -> None:
        name = f"MAIN_SHUTTER_{chamber}_SW"
        await self.write_switch(name, bool(open), momentary=momentary)
        self.log("MainShutter%d %s", chamber, "OPEN(ON)" if open else "CLOSE(OFF)")

    async def vent(self, chamber: int, *, on: bool, momentary: bool = False) -> None:
        name = f"VENT_{chamber}_SW"
        await self.write_switch(name, bool(on), momentary=momentary)
        self.log("Vent%d %s", chamber, "ON" if on else "OFF")

    async def turbo(self, chamber: int, *, on: bool, momentary: bool = False) -> None:
        name = f"TURBO_{chamber}_SW"
        await self.write_switch(name, bool(on), momentary=momentary)
        self.log("Turbo%d %s", chamber, "ON" if on else "OFF")

    async def lift_pin(self, *, up: bool, momentary: bool = False) -> None:
        await self.write_switch("L_PIN_UP_SW" if up else "L_PIN_DOWN_SW", True, momentary=momentary)
        self.log("LiftPin %s", "UP" if up else "DOWN")

    async def lift_pin_lamp(self, *, up: bool) -> bool:
        return await self.read_bit("L_PIN_UP_LAMP" if up else "L_PIN_DOWN_LAMP")

    async def lr_valve(self, *, on: bool, momentary: bool = False) -> None:
        await self.write_switch("L_R_V_SW", bool(on), momentary=momentary)
        self.log("L_R_V %s", "ON" if on else "OFF")

    async def rf_select(self, *, rf_mode: bool, momentary: bool = False) -> None:
        await self.write_switch("SW_RF_SELECT", bool(rf_mode), momentary=momentary)
        self.log("RF_SELECT <- %s", rf_mode)

    async def power_select(self, *, on: bool, momentary: bool = False) -> None:
        await self.write_switch("SW_POWER_SELECT", bool(on), momentary=momentary)
        self.log("POWER_SELECT(SW) <- %s", on)

    async def gas(self, chamber: int, gas: str, *, on: bool, momentary: bool = False) -> None:
        g = (gas or "").strip().upper()
        valid = {"AR", "O2", "N2", "MAIN"}
        if g not in valid:
            raise ValueError(f"gas must be one of {valid}")
        if g == "AR":
            key = f"Ar_{chamber}_GAS_SW"   # 혼합 표기
        elif g == "MAIN":
            key = f"MAIN_{chamber}_GAS_SW"
        else:
            key = f"{g}_{chamber}_GAS_SW"

        # ✅ 코일 존재 여부를 미리 확인하여 설명적 에러로
        if key not in PLC_COIL_MAP:
            raise ValueError(f"가스 '{g}'는 CH{chamber}에 존재하지 않습니다 (키: {key})")

        await self.write_switch(key, bool(on), momentary=momentary)
        self.log("Gas %s@Ch%d %s", g, chamber, "ON" if on else "OFF")

    async def buzzer_stop(self, *, momentary: bool = False) -> None:
        await self.write_switch("BUZZER_STOP_SW", True, momentary=momentary)

    async def read_interlocks(self) -> Dict[str, bool]:
        keys = [k for k in PLC_COIL_MAP.keys() if "인터락" in k]
        res: Dict[str, bool] = {}
        for k in keys:
            res[k] = await self.read_bit(k)
        return res

    async def read_lamps(self) -> Dict[str, bool]:
        keys = [k for k in PLC_COIL_MAP.keys() if k.endswith("_LAMP")]
        res: Dict[str, bool] = {}
        for k in keys:
            res[k] = await self.read_bit(k)
        return res

    async def snapshot_status(self) -> Dict[str, Any]:
        interlocks = await self.read_interlocks()
        lamps = await self.read_lamps()
        return {"interlocks": interlocks, "lamps": lamps}
    
    # ---------- PLC COIL CSV LOGGER ----------
    def _default_local_plc_log_dir(self) -> Path:
        return Path(
            getattr(
                cfgc,
                "PLC_COIL_LOG_LOCAL_DIR",
                Path.cwd() / "Logs_LocalFallback" / "PLC_Coil",
            )
        )

    async def start_plc_coil_csv_logger(
        self, *, interval_s: Optional[float] = None,
        nas_dir: Optional[str] = None,
        local_dir: Optional[str] = None,
        keys: Optional[Iterable[str]] = None,
        reg_keys: Optional[Iterable[str]] = None,
    ) -> None:
        """
        프로그램 시작 시 호출:
        await plc.start_plc_coil_csv_logger(...)
        - 절대 공정에 영향 주지 않도록:
        * PLC 락이 잡혀있으면 이번 tick 스킵
        * 예외는 내부에서 삼키고 계속
        """
        if interval_s is None:
            interval_s = float(getattr(cfgc, "PLC_COIL_LOG_INTERVAL_S", 1.0))
        if nas_dir is None:
            nas_dir = str(getattr(cfgc, "PLC_COIL_LOG_NAS_DIR",
                                r"G:\공유 드라이브\VanaM_Sputter\Sputter\Logs\CH1&2\CH1&2_PLC"))
        if local_dir is None and hasattr(cfgc, "PLC_COIL_LOG_LOCAL_DIR"):
            local_dir = getattr(cfgc, "PLC_COIL_LOG_LOCAL_DIR")

        if getattr(self, "_plc_coil_log_task", None) and not self._plc_coil_log_task.done():
            return

        self._plc_coil_log_stop = asyncio.Event()
        self._plc_coil_log_interval = max(0.5, float(interval_s))  # 너무 짧으면 0.5s로 제한
        self._plc_coil_log_keys = list(keys) if keys is not None else list(PLC_COIL_MAP.keys())

        self._plc_coil_log_nas_dir = Path(nas_dir)
        self._plc_coil_log_local_dir = Path(local_dir) if local_dir else self._default_local_plc_log_dir()

        # ✅ keep-handle CSV writer 생성 (NAS 우선 → 실패 시 로컬 폴백, 이후 주기적으로 NAS 재시도)
        self._plc_coil_csv_writer = DailyCsvListAppender(
            primary_dir=self._plc_coil_log_nas_dir,
            fallback_dir=self._plc_coil_log_local_dir,
            filename_builder=lambda dt: f"{dt.strftime('%Y%m%d')}.csv",
            encoding="utf-8-sig",
            retry_primary_every_s=10.0,
        )

        self._plc_reg_log_keys = list(reg_keys) if reg_keys is not None else [
            "DCV_READ_0", "DCV_READ_1", "DCV_READ_2", "DCV_READ_3",
            "DCV_WRITE_0", "DCV_WRITE_1", "DCV_WRITE_2", "DCV_WRITE_3",
            "DCV_READ_4", "DCV_READ_5", "DCV_READ_6", "DCV_READ_7",
        ]

        self._plc_coil_log_task = asyncio.create_task(self._plc_coil_log_loop(), name="PLCCoilCSVLogger")

    async def stop_plc_coil_csv_logger(self) -> None:
        evt = getattr(self, "_plc_coil_log_stop", None)
        task = getattr(self, "_plc_coil_log_task", None)

        if evt is not None:
            evt.set()

        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        self._plc_coil_log_task = None

        # ✅ keep-handle writer close
        w = getattr(self, "_plc_coil_csv_writer", None)
        if w is not None:
            try:
                await asyncio.to_thread(w.close)
            except Exception:
                pass
        self._plc_coil_csv_writer = None

    async def _plc_coil_log_loop(self) -> None:
        evt: asyncio.Event = self._plc_coil_log_stop
        interval = float(self._plc_coil_log_interval)
        keys = list(self._plc_coil_log_keys)
        reg_keys = list(getattr(self, "_plc_reg_log_keys", []))

        while not evt.is_set():
            t0 = time.perf_counter()
            dt = datetime.now()

            # ✅ PLC가 끊긴 상태에서는 로거가 connect/재시도를 하지 않음 → 공정 영향 0에 더 가까워짐
            if not self.is_connected():
                await asyncio.sleep(interval)
                continue

            # ✅ 공정/메인 제어가 PLC 사용 중이면 스킵(공정 영향 0)
            if self.is_busy():
                await asyncio.sleep(interval)
                continue

            # ✅ 공정 task가 먼저 락을 잡을 기회를 주기(우선순위 체감 개선)
            await asyncio.sleep(0)

            if self.is_busy():
                await asyncio.sleep(interval)
                continue

            # ✅ 코일 스냅샷(블록 읽기). 실패해도 공정 영향 없게 예외 삼킴.
            try:
                snap = await self.snapshot_all_coils_fast(
                    keys=keys,
                    skip_if_busy=True,
                    max_coils_per_req=64,
                    max_gap=8,
                )
            except Exception as e:
                self.log("PLC COIL LOG: snapshot failed (ignored): %r", e)
                await asyncio.sleep(interval)
                continue

            if not snap:
                await asyncio.sleep(interval)
                continue

            reg_snap: Dict[str, int] = {}
            if reg_keys:
                try:
                    reg_snap = await self.snapshot_regs_fast(keys=reg_keys, skip_if_busy=True)
                except Exception as e:
                    self.log("PLC REG LOG: snapshot failed (ignored): %r", e)
                    reg_snap = {}

            row = [dt.isoformat(timespec="seconds")]
            for k in keys:
                row.append("TRUE" if snap.get(k, False) else "FALSE")

            for k in reg_keys:
                row.append(str(reg_snap.get(k, "")))

            header = ["Timestamp", *keys, *reg_keys]

            w = getattr(self, "_plc_coil_csv_writer", None)
            if w is None:
                # start가 호출되지 않았거나(비정상 흐름), 예외로 writer가 비워졌을 때 방어
                w = DailyCsvListAppender(
                    primary_dir=self._plc_coil_log_nas_dir,
                    fallback_dir=self._plc_coil_log_local_dir,
                    filename_builder=lambda dt: f"{dt.strftime('%Y%m%d')}.csv",
                    encoding="utf-8-sig",
                    retry_primary_every_s=10.0,
                )
                self._plc_coil_csv_writer = w

            try:
                await asyncio.to_thread(w.append_row, dt=dt, header=header, row=row)

                # (선택) 이번 write에서 NAS→LOCAL 폴백이 발생했으면 한 줄 남김
                if w.consume_switched_flag():
                    self.log("PLC COIL LOG: NAS write failed -> switched to LOCAL (keep-handle)")

            except Exception as e:
                # ✅ 최종 실패는 공정 영향 없게 무시
                self.log("PLC COIL LOG: write failed (ignored): %r", e)

            # 주기 맞추기
            elapsed = time.perf_counter() - t0
            await asyncio.sleep(max(0.0, interval - elapsed))

    # ──────────────────────────────────────────────────────────
    # Power 공통 (DC/RF 등): family + index 기반
    # ──────────────────────────────────────────────────────────

    def _clamp(self, v: float, lo: float, hi: float) -> float:
        return lo if v < lo else hi if v > hi else v

    def _linear_to_dac(self, value: float, *, vmin: float, vmax: float,
                       full_scale: int, offset: int = 0) -> int:
        v = self._clamp(float(value), vmin, vmax)
        span = max(1e-9, (vmax - vmin))
        code = int(round(((v - vmin) / span) * full_scale + offset))
        return max(0, min(full_scale, code))

    async def power_enable(self, on: bool = True, *, family: str = "DCV",
                        set_idx: Optional[int] = None, set_key: Optional[str] = None) -> None:
        """
        SET 코일 래치 ON/OFF.
        - 기본: family='DCV', set_idx=cfg.dc_write_index → 'DCV_SET_{idx}'
        - RF 등은 set_key='RFP_SET_0' 처럼 직접 키를 지정해도 됨.
        """
        idx = int(self.cfg.dc_write_index) if set_idx is None else int(set_idx)
        key = set_key or f"{family}_SET_{idx}"
        if key not in PLC_COIL_MAP:
            raise KeyError(f"PLC_COIL_MAP에 '{key}' 없음 (family={family}, idx={idx})")
        self.log("POWER SET (%s)[%d] <- %s", family, idx, on)
        await self.write_switch(key, bool(on), momentary=False)

    async def power_write(self, power_w: float, *, family: str = "DCV",
                        write_idx: Optional[int] = None, write_key: Optional[str] = None,
                        vmin: Optional[float] = None, vmax: Optional[float] = None,
                        full_scale: Optional[int] = None, offset: Optional[int] = None) -> int:
        """
        원하는 파워[W] → DAC 코드 → WRITE 레지스터 기록.
        - 기본: family='DCV', write_idx=cfg.dc_write_index → 'DCV_WRITE_{idx}'(=D00004+idx)
        - RF 등은 write_key='RFP_WRITE_0' 식으로 직접 키 지정 가능.
        """
        vmin  = self.cfg.dc_power_min_w if vmin  is None else float(vmin)
        vmax  = self.cfg.dc_power_max_w if vmax  is None else float(vmax)
        fs    = self.cfg.dc_dac_full_scale if full_scale is None else int(full_scale)
        off   = self.cfg.dc_dac_offset if offset is None else int(offset)

        code = self._linear_to_dac(power_w, vmin=vmin, vmax=vmax, full_scale=fs, offset=off)

        if write_key is None:
            idx = int(self.cfg.dc_write_index) if write_idx is None else int(write_idx)
            write_key = f"{family}_WRITE_{idx}"
        if write_key not in PLC_REG_MAP:
            raise KeyError(f"PLC_REG_MAP에 '{write_key}' 없음 (family={family})")
        await self.write_reg_name(write_key, code)
        self.log("POWER WRITE (%s) %s <- W=%.3f (DAC=%d)", family, write_key, power_w, code)
        return code

    async def power_apply(self, power_w: float, *, family: str = "DCV",
                        channel: Optional[int] = None, ensure_set: bool = True,
                        vmin: Optional[float] = None, vmax: Optional[float] = None,
                        full_scale: Optional[int] = None, offset: Optional[int] = None) -> int:
        """
        (추천) 한 번에: SET(선택) → WRITE. channel=None이면 cfg.dc_write_index.
        """
        idx = int(self.cfg.dc_write_index) if channel is None else int(channel)
        if ensure_set:
            await self.power_enable(True, family=family, set_idx=idx)
        return await self.power_write(power_w, family=family, write_idx=idx,
                                      vmin=vmin, vmax=vmax, full_scale=full_scale, offset=offset)

    async def power_read(self, *, family: str = "DCV",
                        v_idx: Optional[int] = None, i_idx: Optional[int] = None,
                        v_key: Optional[str] = None, i_key: Optional[str] = None,
                        v_scale: Optional[float] = None,
                        i_scale: Optional[float] = None) -> tuple[float, float, float]:
        """
        V/I 읽고 스케일 적용 → (P[W], V[V], I[A]).
        - 기본: family='DCV', v_idx=0 → 'DCV_READ_0'(D00000), i_idx=1 → 'DCV_READ_1'(D00001)
        - RF 등은 v_key/i_key로 직접 레지스터 키 지정 가능.
        """
        if v_key is None:
            vi = 0 if v_idx is None else int(v_idx)
            v_key = f"{family}_READ_{vi}"
        if i_key is None:
            ii = 1 if i_idx is None else int(i_idx)
            i_key = f"{family}_READ_{ii}"
        if v_key not in PLC_REG_MAP:
            raise KeyError(f"PLC_REG_MAP에 '{v_key}' 없음 (family={family})")
        if i_key not in PLC_REG_MAP:
            raise KeyError(f"PLC_REG_MAP에 '{i_key}' 없음 (family={family})")

        v_raw = await self.read_reg_name(v_key)
        i_raw = await self.read_reg_name(i_key)

        V = float(v_raw) * (self.cfg.dc_v_scale if v_scale is None else float(v_scale))
        I = float(i_raw) * (self.cfg.dc_i_scale if i_scale is None else float(i_scale))
        P = V * I
        #self.log("POWER READ (%s) V=%.3f, I=%.3f, P=%.3f (keys=%s/%s)", family, V, I, P, v_key, i_key)
        return P, V, I
    
    # RF Enable (SET 래치) — rf_ch=1→DCV_SET_1, rf_ch=2→DCV_SET_2
    async def rf_enable(self, on: bool = True, *, rf_ch: int = 1) -> None:
        ch = 1 if int(rf_ch) != 2 else 2
        await self.power_enable(on, family="DCV", set_idx=ch)  # → DCV_SET_{1|2}

    # RF 목표 W 쓰기 — rf_ch=1→DCV_WRITE_1, rf_ch=2→DCV_WRITE_2
    async def rf_write_w(self, power_w: float, *, rf_ch: int = 1) -> int:
        ch = 1 if int(rf_ch) != 2 else 2
        return await self.power_write(power_w, family="DCV", write_idx=ch)  # → DCV_WRITE_{1|2}

    # RF 목표 W 적용(필요 시 SET 보장) — rf_ch별로 SET/WRITE 분기
    async def rf_apply(self, power_w: float, *, ensure_set: bool = True, rf_ch: int = 1) -> int:
        ch = 1 if int(rf_ch) != 2 else 2
        if ensure_set:
            await self.rf_enable(True, rf_ch=ch)   # DCV_SET_{1|2} = True
        return await self.rf_write_w(power_w, rf_ch=ch)  # DCV_WRITE_{1|2}

    # RF 피드백(Forward/Reflected) 읽기
    #  - rf_ch=1: DCV_READ_2(forward), DCV_READ_3(reflected)
    #  - rf_ch=2: DCV_READ_4(forward), DCV_READ_5(reflected)
    async def rf_read_fwd_ref(self, *, rf_ch: int = 1, zeroing: bool | None = None) -> dict[str, float]:
        # ch=1 → D00002/3(=DCV_READ_2/3), ch=2 → D00008/9(=DCV_READ_4/5)
        if int(rf_ch) == 1:
            f_key, r_key = "DCV_READ_2", "DCV_READ_3"
        else:
            f_key, r_key = "DCV_READ_4", "DCV_READ_5"

        # 1) 원시값(레지스터) 읽기
        f_raw = await self.read_reg_name(f_key)
        r_raw = await self.read_reg_name(r_key)

        # 2) 스케일링 (a·raw + b) → W (CH1/CH2 분리)
        if int(rf_ch) == 2:
            # ✅ CH2 전용 보정값
            f_w = self.cfg.rf2_fwd_a * float(f_raw) + self.cfg.rf2_fwd_b
            r_w = self.cfg.rf2_ref_a * float(r_raw) + self.cfg.rf2_ref_b
        else:
            # 기본(CH1)
            f_w = self.cfg.rf_fwd_a * float(f_raw) + self.cfg.rf_fwd_b
            r_w = self.cfg.rf_ref_a * float(r_raw) + self.cfg.rf_ref_b

        # 3) 제로 오프셋 보정
        # 제로잉 적용 정책: 기본(CH1=적용, CH2=미적용)
        use_zero = (int(rf_ch) == 1) if (zeroing is None) else bool(zeroing)
        if use_zero:
            f_w -= float(self.cfg.rf_forward_zero_w)
            r_w -= float(self.cfg.rf_reflected_zero_w)

        # 4) 음수 방지 & 보기 좋게 반올림
        f_w = round(max(0.0, f_w), 1)
        r_w = round(max(0.0, r_w), 1)

        return {"forward": f_w, "reflected": r_w}

    # ---------- DI용 논리명 셋(set) ----------
    async def set(self, name: str, on: bool, *, ch: int = 1, momentary: bool = False) -> None:
        """
        process_ch2.py에서의 PLC_CMD용 범용 엔드포인트.
        name:
          - 'MV', 'MS'        : 메인 밸브 / 메인 셔터 (ch 사용)
          - 'AR','O2','N2','MAIN' : 가스 라인 (ch 사용)
          - 'G1','G2','G3'    : 건 셔터 1/2/3
          - (그 외) PLC_COIL_MAP 키를 직접 주면 그대로 동작
        """
        if not name:
            raise ValueError("name required")

        key = name.strip().upper().replace(" ", "").replace("_", "")
        # 가스
        if key in {"AR", "O2", "N2", "MAIN"}:
            await self.gas(int(ch), key, on=bool(on), momentary=momentary)
            return
        # 메인 밸브/셔터
        if key == "MV":
            await self.write_switch(self._LOGICAL.get(f"MV@{int(ch)}", "M_V_1_SW"), bool(on), momentary=momentary)
            return
        if key == "MS":
            await self.write_switch(self._LOGICAL.get(f"MS@{int(ch)}", "MAIN_SHUTTER_1_SW"), bool(on), momentary=momentary)
            return
        # 건 셔터들
        if key in {"G1", "G2", "G3"}:
            await self.write_switch(self._LOGICAL[key], bool(on), momentary=momentary)
            return
        # 맵 이름 직접
        try:
            await self.write_switch(name, bool(on), momentary=momentary)
        except KeyError:
            raise KeyError(f"지원하지 않는 PLC 논리명/키: {name}")
        
    # =============== 유틸 ===============
    @asynccontextmanager
    async def _io_lock(self, op: str, *, addr: Optional[int] = None, priority: str = "high"):
        """
        락 획득 대기(wait)와 락 내부 실행(in-lock) 시간을 분리 계측하고,
        임계치 초과 시 self.log로 WARN을 남긴다.

        priority:
          - "high" (기본): 외부 read/write. 락 대기 시 _priority_waiters를 +1 하여
                          snapshot loop에 양보 요청을 알린다.
          - "low"        : snapshot 등 백그라운드 작업. 카운터 미영향.
        """
        loop = asyncio.get_running_loop()
        t_wait_start = loop.time()

        # ✅ 외부 우선순위 요청은 대기 큐 진입 시점부터 카운터 +1
        is_priority = (priority == "high")
        if is_priority:
            self._priority_waiters += 1
        try:
            await self._lock.acquire()
        except BaseException:
            if is_priority:
                self._priority_waiters -= 1
            raise

        waited_ms = (loop.time() - t_wait_start) * 1000.0

        try:
            if waited_ms >= self.cfg.lock_warn_ms:
                if addr is None:
                    self.log("WARN lock-wait %.0f ms (op=%s)", waited_ms, op)
                else:
                    self.log("WARN lock-wait %.0f ms (op=%s, addr=%s)", waited_ms, op, addr)

            t_in_start = loop.time()
            try:
                yield
            finally:
                io_ms = (loop.time() - t_in_start) * 1000.0
                if io_ms >= self.cfg.io_warn_ms:
                    if addr is None:
                        self.log("WARN in-lock IO %.0f ms (op=%s)", io_ms, op)
                    else:
                        self.log("WARN in-lock IO %.0f ms (op=%s, addr=%s)", io_ms, op, addr)

        finally:
            try:
                self._lock.release()
            except RuntimeError:
                pass
            # ✅ 락 해제 후 카운터 감소 (다음 tick의 snapshot이 다시 진행 가능하게)
            if is_priority:
                self._priority_waiters -= 1
    # =============== 유틸 ===============

    # =============== chamber_runtime.py 호환용 함수 ===============
    # chamber_runtime: start()/connect 호환
    async def start(self) -> None:
        """connect() 별칭 — 장치 생명주기 통일."""
        await self.connect()

    # chamber_runtime: cleanup()/cleanup_quick 호환
    async def cleanup(self) -> None:
        """close() 별칭 — 정상 종료."""
        await self.close()

    async def cleanup_quick(self) -> None:
        """빠른 종료(현재는 close와 동일)."""
        await self.close()

    async def pause_watchdog(self) -> None:
        """
        하트비트 워치독 '일시정지'.
        ✅ Task를 cancel하지 않는다 (CancelledError 전파/레이스 방지).
        - _heartbeat_loop가 _hb_paused를 보고 I/O를 스킵한다.
        """
        self._hb_paused = True
        # cancel/await 금지: 여기서 CancelledError가 섞이면 상위(handlers/server)가 못 잡고 연결이 깨질 수 있음.

    async def resume_watchdog(self) -> None:
        """워치독 재개 (pause flag 해제)."""
        self._hb_paused = False
        if self._closed:
            return
        # pause에서는 task를 죽이지 않으므로 보통은 살아있다.
        # 혹시 초기 상태/예외로 task가 없다면만 생성
        if self._hb_task is None or self._hb_task.done():
            self._hb_task = asyncio.create_task(self._heartbeat_loop())

    # chamber_runtime: 공정 on/off 신호에 맞춰 폴링/워치독 제어(옵션)
    def set_process_status(self, should_poll: bool) -> None:
        """
        공정 상태 알림 훅.
        False면 워치독(하트비트/자동재연결) 잠시 멈춰 로그 소음/경합 줄임.
        True면 다시 재개.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        
        if not should_poll:
            loop.create_task(self.pause_watchdog())
        else:
            loop.create_task(self.resume_watchdog())

    # IG의 set_endpoint와 대칭 — ip/port 런타임 변경
    async def set_endpoint(self, ip: str, port: int, *, reconnect: bool = True) -> None:
        """
        런타임에서 PLC 엔드포인트 변경. reconnect=True면 즉시 재연결.
        """
        self.cfg.ip = str(ip)
        self.cfg.port = int(port)
        if reconnect:
            # 하트비트 일시 정지 후 재연결
            await self.pause_watchdog()
            try:
                await self.close()
            except Exception:
                pass
            await self.connect()
            await self.resume_watchdog()
    # =============== chamber_runtime.py 호환용 함수 ===============

__all__ = [
    "PLC_COIL_MAP", "PLC_REG_MAP", "PLC_TIMER_MAP",
    "PLCConfig", "AsyncPLC", "PLCError",
]

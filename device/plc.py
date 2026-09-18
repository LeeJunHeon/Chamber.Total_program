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
import contextlib
import inspect
import socket
import subprocess
import threading
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
try:
    # pymodbus 3.x 예외 계층. 타입 기준 분류에 사용한다.
    #  - ConnectionException : 연결 불가 / 소켓 없음 / 상대가 끊음 / Not connected
    #  - ModbusIOException   : 응답 없음 / transaction id 불일치 / 디코드 실패
    from pymodbus.exceptions import ConnectionException, ModbusIOException
except Exception:      # pragma: no cover - 구버전/설치 이상 시 문자열 판정으로 폴백
    ConnectionException = ()   # type: ignore[assignment,misc]
    ModbusIOException = ()     # type: ignore[assignment,misc]

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
    "Z_M_P_2_STOP_SW":   320,   # M00200
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

    "BUZZER_STOP_SW":      1600,  # M01000
    "GAUGE_1_A_INTERLOCK": 2400,  # M01500
    "GAUGE_1_B_INTERLOCK": 2560,  # M01600
    "L_GAUGE_A_INTERLOCK": 2720,  # M01700
    "GAUGE_2_A_INTERLOCK": 2880,  # M01800
    "GAUGE_2_B_INTERLOCK": 3040,  # M01900
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

def _fallback_log_root() -> Path:
    """로그 폴백 뿌리. 호출 시점에 config_common 을 읽는다(DEC-033).
    cwd 는 쓰지 않는다 — 관리자 바로가기의 '시작 위치'가 비면 System32 가 된다."""
    import sys as _sys
    if getattr(_sys, "frozen", False):
        _base = Path(_sys.executable).resolve().parent
    else:
        _base = Path(__file__).resolve().parents[1]
    try:
        from lib import config_common as _cc
        return Path(getattr(_cc, "LOCAL_FALLBACK_ROOT", _base / "Logs_LocalFallback"))
    except Exception:
        return _base / "Logs_LocalFallback"


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
        
        # 끊김 알림 대기 시간 — _apply_cfg_from_config()가 getattr fallback 기본값으로
        # 이 값을 읽으므로, 반드시 호출 '전에' 초기화해야 함
        self._disconnect_alert_after_s: float = 60.0
        
        # ✅ config_common 값이 있으면 덮어써서 “초기값”을 config 기준으로 맞춤
        self._apply_cfg_from_config()

        self._client: Optional[ModbusTcpClient] = None
        self._uid_kw: Optional[str] = None  # 'unit' 또는 'slave'
        self._lock = asyncio.Lock()

        # ✅ 양보 메커니즘: 외부 우선순위 PLC I/O 대기자 카운터
        #    snapshot loop가 매 block 직전 이 값을 확인하여 양보 여부 결정
        self._priority_waiters: int = 0
        self._last_io_ts = 0.0

        # ✅ 코일 스냅샷 블록 계획(적응형). 공격적 → 보수적 순서.
        self._coil_plan_idx: int = self._default_coil_plan_idx()
        # 연속 성공 횟수(승격 판정용)
        self._coil_plan_ok_streak: int = 0

        # ✅ 재연결 백오프: 이 시각 전에는 소켓 connect 를 다시 시도하지 않는다.
        self._next_connect_attempt_at: float = 0.0

        # ✅ 링크 정책 상태
        #    PLC 가 수 초 무응답일 때 소켓을 닫고 재접속하면, 이 PLC 는 그 뒤 1.5~3분간
        #    새 SYN 에 응답하지 않아 분 단위 단절이 된다. 그래서 타임아웃 1회로는
        #    소켓을 닫지 않고 같은 소켓으로 재시도한다.
        self._consec_timeouts: int = 0        # 연속 타임아웃(E402) 횟수
        self._reconnect_count: int = 0        # 소켓 재생성 누적
        self._connected_since: float = 0.0    # 현재 소켓이 연결된 시각(monotonic)
        self._last_success_ts: float = 0.0    # 마지막 성공 I/O 시각
        self._connect_fail_streak: int = 0    # 연속 접속 실패 횟수
        self._disconnected_at: float = 0.0    # 소켓이 없어진 시각
        self._last_connect_diag: str = ""     # 마지막 진단 분류
        self._last_diag_ts: float = 0.0       # 마지막 진단 시각
        self._diag_running: bool = False      # 진단 스레드 중복 실행 방지
        self._sock_lock = threading.Lock()    # _connect_sync/_close_sync 동시 실행 방지

        self._last_io_ts = 0.0
        self._hb_task: Optional[asyncio.Task] = None
        self._closed = False
        self._hb_paused: bool = False   # ← 추가

        # ── 연결 상태 변화 알림(구글챗)용 ──────────────────────────
        # main.py에서 set_conn_change_callback()으로 주입.
        # 시그니처: cb(connected: bool, detail: str)
        self._conn_change_cb = None
        # True=연결정상, False=끊김, None=아직 판단 전(부팅 직후)
        self._conn_alert_state: Optional[bool] = None
        # 끊김을 처음 감지한 시각(monotonic). 끊김 알림 발송 후 None로 리셋하지 않고
        # 재연결 시에만 리셋. 0.0이면 "현재 끊김 추적 안 함".
        self._disconnect_since: float = 0.0
        # "끊김" 알림을 이미 보냈는지 (중복 방지)
        self._disconnect_alerted: bool = False

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

        # 끊김 알림 대기 시간
        self._disconnect_alert_after_s = float(
            getattr(cfgc, "PLC_DISCONNECT_ALERT_AFTER_S", self._disconnect_alert_after_s)
        )

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
    # ✅ 코일 스냅샷 블록 계획표: (max_gap, max_span)
    #    앞쪽일수록 공격적(블록 수가 적다), 마지막은 기존 동작과 동일한 안전값.
    _COIL_BLOCK_PLANS: tuple[tuple[int, int], ...] = (
        (2000, 2000), (256, 512), (64, 256), (8, 64),
    )
    # FC1 한도: 한 블록 최대 코일 수
    _COIL_BLOCK_HARD_MAX = 2000

    @classmethod
    def _default_coil_plan_idx(cls) -> int:
        """설정값을 읽고 범위를 벗어나면 마지막(가장 안전) 인덱스로 클램프."""
        last = len(cls._COIL_BLOCK_PLANS) - 1
        try:
            i = int(getattr(cfgc, "PLC_COIL_LOG_BLOCK_PLAN", 0))
        except Exception:
            return last
        if i < 0 or i > last:
            return last
        return i

    def _coil_ranges_for_plan(self, addrs, idx: int):
        """계획 idx 부터 시작해, 블록 길이가 FC1 한도를 넘지 않는
        첫 계획의 (인덱스, ranges) 를 돌려준다."""
        last = len(self._COIL_BLOCK_PLANS) - 1
        i = max(0, min(int(idx), last))
        while True:
            gap, span = self._COIL_BLOCK_PLANS[i]
            ranges = self._build_sparse_ranges(
                addrs, max_gap=max(0, int(gap)), max_span=max(1, int(span))
            )
            if not ranges or max(c for _, c in ranges) <= self._COIL_BLOCK_HARD_MAX:
                return i, ranges
            if i >= last:
                # 마지막 계획이라도 한도를 넘으면 그대로 두고 반환(방어적 종료)
                return i, ranges
            i += 1

    async def connect(self) -> None:
        self._closed = False
        # ✅ 명시적 connect (사용자/부팅/set_endpoint) 는 의도적 행위이므로 백오프를 무시한다
        self._next_connect_attempt_at = 0.0

        # ✅ connect 직전에 config 값을 재적용 (Apply 후 재연결/다음 연결에 반영)
        self._apply_cfg_from_config()

        # ✅ 연결 성공 여부와 무관하게 하트비트 태스크는 살아있게(백그라운드 재연결용)
        if self._hb_task is None or self._hb_task.done():
            self._hb_task = asyncio.create_task(self._heartbeat_loop(), name="PLCHeartbeat")

        async with self._io_lock("connect"):
            await self._locked_thread(self._connect_sync, full=True)

        # ✅ 재연결 시 계획을 즉시 되돌리지는 않는다(강등↔승격 왕복 방지).
        #    복구는 _maybe_promote_coil_plan 의 "연속 성공" 규칙으로만 한다.
        self._coil_plan_ok_streak = 0
        self._next_connect_attempt_at = 0.0
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
            await self._locked_thread(self._close_sync)
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
    
    def set_conn_change_callback(self, cb) -> None:
        """연결 상태 변화 알림 콜백 등록.
        cb(connected: bool, detail: str) 형태. main.py에서 ChatNotifier에 연결.
        """
        self._conn_change_cb = cb

    def _fire_conn_change(self, connected: bool, detail: str = "") -> None:
        """콜백을 안전하게 호출(예외는 삼킴)."""
        cb = self._conn_change_cb
        if cb is None:
            return
        try:
            cb(bool(connected), str(detail))
        except Exception:
            pass

    def _new_client(self) -> "ModbusTcpClient":
        """pymodbus 클라이언트 생성 지점을 한 곳으로 모은다.

        ⚠ retries=0 이 핵심이다. pymodbus 기본 retries=3 이면 무응답 요청 1회가
          timeout x4 로 늘어나 우리 타임아웃 정책과 어긋난다(재시도는 우리만 한다).
          3.6.x 는 **kwargs 로 받고 3.11.x 는 명시 인자라 둘 다 통과한다.
        """
        try:
            return ModbusTcpClient(self.cfg.ip, port=self.cfg.port,
                                   timeout=self.cfg.timeout_s, retries=0)
        except TypeError:
            # retries 를 모르는 버전 방어
            return ModbusTcpClient(self.cfg.ip, port=self.cfg.port, timeout=self.cfg.timeout_s)

    def _detect_uid_kw(self, method) -> Optional[str]:
        """유닛 ID 키워드 탐지. pymodbus 3.6=slave, 3.11=device_id."""
        try:
            params = inspect.signature(method).parameters
            for k in ("slave", "unit", "device_id"):
                if k in params:
                    return k
        except Exception:
            pass
        return None

    def _uid_kwargs(self, method=None) -> dict:
        """유닛 ID kwargs. method 를 주면 그 메서드의 signature 로 다시 확인한다
        (버전에 따라 메서드마다 키워드가 다를 수 있다)."""
        if method is not None:
            try:
                kw = self._detect_uid_kw(method)
                if kw:
                    return {kw: self.cfg.unit}
                # signature 에 없고 **kwargs 만 있으면 기존 탐지값을 쓴다
                params = inspect.signature(method).parameters
                if not any(pp.kind == inspect.Parameter.VAR_KEYWORD for pp in params.values()):
                    return {}
            except Exception:
                pass
        if self._uid_kw:
            return {self._uid_kw: self.cfg.unit}
        return {}

    def _note_io_success(self) -> None:
        """원시 연산 성공 시: 연속 타임아웃 리셋 + pymodbus 내부 카운터 원복."""
        self._consec_timeouts = 0
        self._last_success_ts = time.monotonic()
        # pymodbus 동기 클라이언트는 성공해도 count_until_disconnect 를 되돌리지 않아
        # 누적되면 로그가 "CLOSING CONNECTION" 으로 바뀐다(소켓을 닫진 않는다).
        with contextlib.suppress(Exception):
            tr = getattr(self._client, "transaction", None)
            if tr is not None:
                mx = getattr(tr, "max_until_disconnect", None)
                if mx is not None:
                    tr.count_until_disconnect = mx

    def _drain_socket(self) -> int:
        """늦게 도착한 응답/부분 프레임 찌꺼기를 버린다. 최대 8회 / 50 ms.

        pymodbus 는 transaction id 로 늦은 '완성 프레임'은 걸러내지만
        부분 프레임은 걸러내지 못해 다음 응답과 섞인다.
        """
        n = 0
        try:
            cli = self._client
            sock = getattr(cli, "socket", None) if cli is not None else None
            if sock is None:
                return 0
            deadline = time.monotonic() + 0.05
            with contextlib.suppress(Exception):
                sock.setblocking(False)
            for _ in range(8):
                if time.monotonic() >= deadline:
                    break
                try:
                    b = sock.recv(4096)
                except Exception:
                    break
                if not b:
                    break
                n += len(b)
        except Exception:
            pass
        return n

    def _ensure_ok(self, resp, *, op: str, addr: int | None = None):
        if resp is None:
            raise PLCError("E402", "PLC 응답 없음(None)", op=op, addr=addr)
        if isinstance(resp, ExceptionResponse):
            raise PLCError("E403", f"PLC Modbus ExceptionResponse: {resp}", op=op, addr=addr)
        if hasattr(resp, "isError") and resp.isError():
            raise PLCError("E403", f"PLC Modbus isError(): {resp}", op=op, addr=addr)
        return resp

    def _reconnect_backoff_s(self) -> float:
        """연결 실패 후 재연결을 미룰 시간(초). 0이면 백오프 사용 안 함."""
        try:
            v = float(getattr(cfgc, "PLC_RECONNECT_BACKOFF_S", 5.0))
        except Exception:
            v = 5.0
        return v if v > 0.0 else 0.0

    def _coil_plan_promote_after(self) -> int:
        """연속 성공 이 횟수마다 블록 계획을 한 단계 승격. 0이면 승격 안 함."""
        try:
            v = int(getattr(cfgc, "PLC_COIL_LOG_PROMOTE_AFTER", 12))
        except Exception:
            v = 12
        return v if v > 0 else 0

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

    def _connect_sync(self, *, full: bool = True) -> None:
        """소켓을 보장한다.

        full=True  : 백그라운드/명시적 경로 — connect_retry+1 회 시도(기존 동작)
        full=False : 공정/명령 앞단 — PLC_FG_CONNECT_ATTEMPTS(기본 1)회만 시도
                     (끊긴 상태에서 로봇 명령이 7.5초를 통째로 떠안지 않게 한다)
        """
        with self._sock_lock:
            if self._client is None:
                self._client = self._new_client()

            if not self._is_connected():
                # ✅ 재연결 백오프: 끊긴 동안 매 명령이 접속 시도를 반복하지 않게 한다.
                #    (소켓이 살아 있으면 위 _is_connected() 에서 이미 반환되므로
                #     "읽기만 늦는" 경우에는 절대 개입하지 않는다)
                _bo = self._reconnect_backoff_s()
                if _bo > 0.0:
                    _now = time.monotonic()
                    if _now < float(getattr(self, "_next_connect_attempt_at", 0.0) or 0.0):
                        raise PLCError("E401", "PLC 재연결 대기 중 (backoff)", op="connect")
                    # 시도 '전'에 먼저 걸어 동시 진입을 막고,
                    # 실패 후에도 다시 걸어 "시도 종료 시점부터" _bo 초를 쉬게 한다.
                    self._next_connect_attempt_at = _now + _bo

                if full:
                    attempts = int(getattr(self.cfg, "connect_retry", 0)) + 1
                else:
                    try:
                        attempts = max(1, int(getattr(cfgc, "PLC_FG_CONNECT_ATTEMPTS", 1)))
                    except Exception:
                        attempts = 1

                ok = False
                last_exc: Optional[Exception] = None
                delay = max(0.0, float(getattr(self.cfg, "connect_retry_delay_s", 0.0)))

                for i in range(attempts):
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
                    self._client = self._new_client()

                    # ✅ 시도 '사이'에만 쉰다(마지막 실패 뒤의 불필요한 sleep 제거)
                    if delay > 0.0 and i < attempts - 1:
                        time.sleep(delay)

                if not ok:
                    # 최종 실패 → 상태 정리 후 E401
                    try:
                        if self._client is not None:
                            self._client.close()
                    except Exception:
                        pass
                    self._client = None
                    self._uid_kw = None
                    if self._disconnected_at == 0.0:
                        self._disconnected_at = time.monotonic()
                    self._connect_fail_streak += 1

                    # ✅ 실패 확정 → 지금부터 _bo 초 동안은 소켓을 건드리지 않는다
                    if _bo > 0.0:
                        self._next_connect_attempt_at = time.monotonic() + _bo

                    # ✅ 원인 진단(별도 스레드, 최소 간격 제한)
                    self._diag_connect_failure()

                    _diag = self._last_connect_diag or "미확인"
                    _suffix = f" (시도 {attempts}회, 분류={_diag})"
                    if last_exc is not None:
                        raise PLCError(
                            "E401",
                            f"Modbus TCP 연결 실패 ({self.cfg.ip}:{self.cfg.port}) - {last_exc!r}{_suffix}",
                            op="connect",
                        )
                    raise PLCError(
                        "E401",
                        f"Modbus TCP 연결 실패 ({self.cfg.ip}:{self.cfg.port}){_suffix}",
                        op="connect",
                    )

                # ✅ 연결 성공 → 백오프 즉시 해제
                self._next_connect_attempt_at = 0.0
                self._connected_since = time.monotonic()
                if self._connect_fail_streak > 0:
                    _down = (time.monotonic() - self._disconnected_at) if self._disconnected_at else 0.0
                    with contextlib.suppress(Exception):
                        self.log("WARN PLC 재접속 성공 (실패 %d회, 끊긴 뒤 %.0f초)",
                                 self._connect_fail_streak, _down)
                self._connect_fail_streak = 0
                self._disconnected_at = 0.0

                # 성공 시 keepalive
                try:
                    if getattr(self._client, "socket", None):
                        self._client.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                except Exception:
                    pass

                self._last_io_ts = time.monotonic()

            if self._uid_kw is None and self._client is not None:
                self._uid_kw = self._detect_uid_kw(self._client.read_coils)

    def _close_sync(self):
        with self._sock_lock:
            if self._client is not None:
                try:
                    self._client.close()
                finally:
                    self._client = None
                    self._uid_kw = None
                    self._connected_since = 0.0
                    if self._disconnected_at == 0.0:
                        self._disconnected_at = time.monotonic()

    # ---------- 재접속 / 진단 ----------
    def _log_reconnect(self, reason: str, op: str) -> None:
        """소켓을 닫기 '직전' 에 항상 남긴다(왜 재생성했는지 추적용)."""
        with contextlib.suppress(Exception):
            self._reconnect_count += 1
            held = (time.monotonic() - self._connected_since) if self._connected_since else 0.0
            self.log("WARN PLC 소켓 재생성 #%d (사유=%s, op=%s, 이전 연결 유지 %.0f초)",
                     self._reconnect_count, reason, op, held)

    def _diag_connect_failure(self) -> None:
        """접속 최종 실패의 원인을 별도 스레드에서 한 줄로 남긴다.
        락과 무관하게 돌며 어떤 예외도 밖으로 내지 않는다."""
        try:
            if not bool(getattr(cfgc, "PLC_DIAG_PROBE", True)):
                return
            if self._diag_running:
                return
            try:
                min_gap = float(getattr(cfgc, "PLC_DIAG_MIN_INTERVAL_S", 60.0))
            except Exception:
                min_gap = 60.0
            now = time.monotonic()
            if self._last_diag_ts and (now - self._last_diag_ts) < min_gap:
                return
            self._last_diag_ts = now
            self._diag_running = True

            host, port, tmo = self.cfg.ip, int(self.cfg.port), float(self.cfg.timeout_s)
            moxa = str(getattr(cfgc, "MFC_TCP_HOST", "192.168.1.50"))

            def _work():
                try:
                    cls = self._probe_connect(host, port, tmo)
                    self._last_connect_diag = cls
                    p_plc = self._probe_ping(host)
                    p_moxa = self._probe_ping(moxa)
                    with contextlib.suppress(Exception):
                        self.log("WARN PLC 진단: connect=%s | ping PLC(%s)=%s | ping MOXA(%s)=%s",
                                 cls, host, p_plc, moxa, p_moxa)
                except Exception:
                    pass
                finally:
                    self._diag_running = False

            threading.Thread(target=_work, name="PLCConnectDiag", daemon=True).start()
        except Exception:
            self._diag_running = False

    @staticmethod
    def _probe_connect(host: str, port: int, timeout_s: float) -> str:
        """raw socket 으로 접속해 errno 를 사람이 읽을 분류로 바꾼다."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sk:
                sk.settimeout(max(0.5, float(timeout_s)))
                rc = sk.connect_ex((host, int(port)))
            if rc == 0:
                return "접속됨"
            if rc in (110, 10060):          # ETIMEDOUT / WSAETIMEDOUT
                return "SYN 무응답(타임아웃)"
            if rc in (111, 10061):          # ECONNREFUSED / WSAECONNREFUSED
                return "접속 거부(RST)"
            if rc in (113, 101, 10065, 10051):   # EHOSTUNREACH/ENETUNREACH/WSA*
                return "도달 불가"
            return f"errno={rc}"
        except socket.timeout:
            return "SYN 무응답(타임아웃)"
        except Exception as e:
            return f"probe 실패: {type(e).__name__}"

    @staticmethod
    def _probe_ping(host: str) -> str:
        """Windows 에서만 ping 1회. 그 외 OS 는 생략."""
        if os.name != "nt":
            return "생략"
        try:
            cf = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            r = subprocess.run(["ping", "-n", "1", "-w", "500", str(host)],
                               capture_output=True, timeout=3, creationflags=cf)
            return "응답" if r.returncode == 0 else "무응답"
        except Exception as e:
            return f"실패({type(e).__name__})"

    async def _locked_thread(self, fn, *args, **kwargs):
        """_io_lock 안에서 실행하는 to_thread 래퍼.

        ⚠ 호출부(runtime)는 wait_for(plc.read_bit(...), 0.6) 로 취소를 건다.
          그냥 to_thread 를 await 하면 취소 즉시 _io_lock 의 finally 가 락을 놓는데,
          스레드는 아직 소켓을 쓰는 중이라 다른 태스크가 그 소켓을 close/reconnect 할 수 있다.
          여기서는 취소되더라도 스레드가 끝날 때까지 기다린 뒤 취소를 전파한다
          (= 락이 스레드 종료 전에 풀리지 않는다). 취소 자체는 그대로 전파된다.
        """
        fut = asyncio.ensure_future(asyncio.to_thread(fn, *args, **kwargs))
        try:
            return await asyncio.shield(fut)
        except asyncio.CancelledError:
            grace = (
                float(self.cfg.timeout_s) * (int(getattr(self.cfg, "connect_retry", 0)) + 2)
                + float(getattr(self.cfg, "connect_retry_delay_s", 0.0))
                * (int(getattr(self.cfg, "connect_retry", 0)) + 1)
                + 1.0
            )
            with contextlib.suppress(Exception):
                await asyncio.wait_for(asyncio.shield(fut), timeout=grace)
            raise

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
                #    ⚠ 단, "락이 잡혀 있다"가 곧 "연결됨"은 아니다.
                #       재접속 시도 중에도 락은 잡혀 있으므로, 실제 연결일 때만 OK 로 본다.
                if self._lock.locked():
                    if self.is_connected():
                        self._mark_conn_ok()
                    continue

                ping_ok = False
                try:
                    # ✅ 2) 워치독은 "가벼운 ping"만.
                    #    접속은 백그라운드라 full=True(3회), 이후 I/O 는 _run_io 정책을
                    #    그대로 따른다 → 타임아웃 1회로는 소켓을 닫지 않는다.
                    async with self._io_lock("heartbeat", addr=0):
                        await self._locked_thread(self._connect_sync, full=True)
                        await self._throttle_and_heartbeat()
                        if self._client is None:
                            ping_ok = False
                        else:
                            await self._run_io("heartbeat", 0, "read_coils", 0, count=1)
                            ping_ok = True

                except Exception:
                    # ✅ 3) 여기서는 소켓을 닫지 않는다.
                    #    닫기는 _run_io 안에서만(E401/reset 또는 연속 타임아웃 임계) 일어난다.
                    ping_ok = False

                # ✅ 4) ping 결과로 연결 상태/알림 처리
                if ping_ok:
                    self._mark_conn_ok()
                else:
                    self._mark_conn_fail()

        except asyncio.CancelledError:
            return

    # ---------- 연결 상태/알림 처리 ----------
    def _mark_conn_ok(self) -> None:
        """heartbeat ping 성공 시 호출. 끊김 상태였으면 '재연결' 알림 1회 발송."""
        # 끊김 알림을 이미 보낸 상태에서 복구된 경우에만 '재연결' 알림
        if self._disconnect_alerted:
            self._fire_conn_change(True, f"[CH1&2] PLC 재연결 성공 ({self.cfg.ip}:{self.cfg.port})")
        # 상태 리셋 (다음 끊김을 새 사이클로 추적)
        self._disconnect_since = 0.0
        self._disconnect_alerted = False
        self._conn_alert_state = True

    def _mark_conn_fail(self) -> None:
        """heartbeat ping 실패 시 호출. 최초 끊김 시각을 기록하고,
        대기 시간 경과 + 미발송 상태면 '끊김' 알림 1회 발송."""
        now = time.monotonic()
        if self._disconnect_since == 0.0:
            # 끊김 추적 시작
            self._disconnect_since = now
        self._conn_alert_state = False

        # 아직 알림 안 보냈고, 경과 시간이 임계 넘으면 1회 발송
        if (not self._disconnect_alerted) and \
           (now - self._disconnect_since >= float(self._disconnect_alert_after_s)):
            self._disconnect_alerted = True
            elapsed = int(now - self._disconnect_since)
            self._fire_conn_change(
                False,
                f"[CH1&2] PLC 연결 끊김 {elapsed}초 경과, 재연결 실패 ({self.cfg.ip}:{self.cfg.port})"
            )

    # ---------- 저수준 IO(직렬화) ----------
    def _to_plc_error(self, op: str, addr: int | None, e: Exception) -> PLCError:
        if isinstance(e, PLCError):
            return e
        s = str(e).lower()

        # ✅ 1순위: pymodbus 예외 "타입"으로 분류한다.
        #    (문자열 판정만 쓰면 pymodbus 3.11 의 "No response received ..." 가
        #     E403 으로 새어 재연결·재시도 안전망이 통째로 죽는다)
        if ConnectionException and isinstance(e, ConnectionException):
            return PLCError("E401", f"PLC 연결 오류: {type(e).__name__}: {e}", op=op, addr=addr, cause=e)

        if ModbusIOException and isinstance(e, ModbusIOException):
            # 응답 없음 / transaction id 불일치 / 디코드 실패 — 소켓이 오염된 상태이므로
            # "닫고 재연결 후 1회 재시도"가 올바른 대응이다.
            return PLCError("E402", f"PLC 응답 없음/프레임 오류: {type(e).__name__}: {e}",
                            op=op, addr=addr, cause=e)

        # 연결/끊김 계열 (OSError·socket 계열 보조 경로)
        if self._is_reset_err(e) or ("connection" in s and "reset" in s) or ("refused" in s) or ("no route" in s):
            return PLCError("E401", f"PLC 연결 오류: {type(e).__name__}: {e}", op=op, addr=addr, cause=e)

        # 응답없음/타임아웃 계열 (pymodbus 아닌 경로에서 올라온 예외용 안전망)
        if (("timeout" in s) or ("timed out" in s) or ("no answer" in s)
                or ("no response" in s) or ("client cannot connect" in s)
                or ("connection unexpectedly closed" in s)
                or ("응답" in s and "없" in s)):
            return PLCError("E402", f"PLC 응답 없음/타임아웃: {type(e).__name__}: {e}",
                            op=op, addr=addr, cause=e)

        # Modbus 계열
        if isinstance(e, ModbusException) or ("modbus" in s):
            return PLCError("E403", f"PLC Modbus 오류: {type(e).__name__}: {e}", op=op, addr=addr, cause=e)

        return PLCError("E403", f"PLC 통신 오류: {type(e).__name__}: {e}", op=op, addr=addr, cause=e)

    async def _run_io(self, op: str, addr, fn_name: str,
                      *args, priority: str = "high", **kwargs):
        """_io_lock 안에서 원시 연산 1회를 수행한다(재시도/재접속 정책 포함).

        정책(이 PLC 는 소켓을 닫으면 1.5~3분간 새 SYN 에 응답하지 않는다):
          - E403            : 그대로 raise (기존과 동일)
          - E401 / reset    : 소켓이 죽었다 → close + connect(full=False) + 1회 재시도
          - E402(무응답)
              priority=low  : 재시도도 close 도 하지 않고 즉시 raise (코일 로거는 이 tick 만 포기)
              연속 임계 도달 : close + connect + 1회 재시도
              그 외          : drain 후 '같은 소켓' 으로 1회 재시도 (소켓 유지)
        반환값은 pymodbus 응답. _ensure_ok 는 호출부에서 기존처럼 수행한다.
        """
        await self._locked_thread(self._connect_sync, full=False)
        await self._throttle_and_heartbeat()

        def _bound():
            cli = self._client
            if cli is None:
                raise PLCError("E401", "PLC 연결 없음", op=op, addr=addr)
            return getattr(cli, fn_name)

        def _kw(m):
            return {**kwargs, **self._uid_kwargs(m)}

        def _check(resp):
            """pymodbus 는 타임아웃을 '예외'가 아니라 ModbusIOException '객체'로
            돌려주기도 한다. 그대로 두면 _ensure_ok 가 isError()==True 를 보고
            E403(장비가 거절)으로 분류해 타임아웃 정책을 통째로 건너뛴다.
            여기서 다시 raise 해 E402 경로를 타게 한다."""
            if ModbusIOException and isinstance(resp, ModbusIOException):
                raise resp
            return resp

        try:
            m = _bound()
            resp = _check(await self._locked_thread(m, *args, **_kw(m)))
            self._note_io_success()
            return resp
        except Exception as e:
            pe = self._to_plc_error(op, addr, e)

            if pe.code == "E403":
                raise pe from e

            if pe.code == "E401" or self._is_reset_err(e):
                # 소켓이 죽었거나 상대가 끊었다 — 재생성이 유일한 복구다
                self._log_reconnect(f"{pe.code}/reset", op)
                await self._locked_thread(self._close_sync)
                await self._locked_thread(self._connect_sync, full=False)
                await self._throttle_and_heartbeat()
                m = _bound()
                resp = _check(await self._locked_thread(m, *args, **_kw(m)))
                self._note_io_success()
                return resp

            # ── E402: 응답 없음/프레임 오류 ──
            self._consec_timeouts += 1

            if priority == "low":
                # 코일 로거 등 백그라운드는 절대 소켓을 닫지 않는다
                raise pe from e

            try:
                close_after = max(1, int(getattr(cfgc, "PLC_TIMEOUT_CLOSE_AFTER", 3)))
            except Exception:
                close_after = 3

            if self._consec_timeouts >= close_after:
                self._log_reconnect(f"연속 타임아웃 {self._consec_timeouts}회", op)
                await self._locked_thread(self._close_sync)
                await self._locked_thread(self._connect_sync, full=False)
                await self._throttle_and_heartbeat()
                m = _bound()
                resp = _check(await self._locked_thread(m, *args, **_kw(m)))
                self._note_io_success()
                return resp

            # 임계 미만 — 소켓을 지키고 같은 소켓으로 1회만 재시도
            await self._locked_thread(self._drain_socket)
            await self._throttle_and_heartbeat()
            try:
                m = _bound()
                resp = _check(await self._locked_thread(m, *args, **_kw(m)))
            except Exception as e2:
                pe2 = self._to_plc_error(op, addr, e2)
                if pe2.code == "E402":
                    self._consec_timeouts += 1
                raise pe2 from e2
            self._note_io_success()
            return resp

    async def read_coil(self, addr: int) -> bool:
        op = "read_coil"
        async with self._io_lock(op, addr=addr):
            try:
                resp = await self._run_io(op, addr, "read_coils", addr, count=1)
                self._ensure_ok(resp, op=op, addr=addr)
                return bool(resp.bits[0])

            except Exception as e:
                raise self._to_plc_error(op, addr, e) from e

    async def write_coil(self, addr: int, state: bool) -> None:
        op = "write_coil"
        async with self._io_lock(op, addr=addr):
            try:
                # 같은 값의 재기록이므로 재시도는 안전하다(기존도 재접속 후 재시도했다)
                resp = await self._run_io(op, addr, "write_coil", addr, bool(state))
                self._ensure_ok(resp, op=op, addr=addr)

            except Exception as e:
                raise self._to_plc_error(op, addr, e) from e

    async def read_reg(self, addr: int) -> int:
        op = "read_reg"
        async with self._io_lock(op, addr=addr):
            try:
                resp = await self._run_io(op, addr, "read_holding_registers", addr, count=1)
                self._ensure_ok(resp, op=op, addr=addr)
                return int(resp.registers[0])

            except Exception as e:
                raise self._to_plc_error(op, addr, e) from e

    async def write_reg(self, addr: int, value: int) -> None:
        op = "write_reg"
        async with self._io_lock(op, addr=addr):
            try:
                resp = await self._run_io(op, addr, "write_register", addr, int(value))
                self._ensure_ok(resp, op=op, addr=addr)

            except Exception as e:
                raise self._to_plc_error(op, addr, e) from e

    async def read_coils_block(self, start_addr: int, count: int, *, priority: str = "high") -> list[bool]:
        """
        FC1: 연속 코일을 한 번에 읽고 resp.bits[] 배열로 받는다.
        (코일을 하나씩 read_coil 반복하는 방식이 아니라, PLC가 배열로 응답)

        priority: "high" (기본, 외부) / "low" (snapshot 등 양보 가능 백그라운드)
                  low 는 타임아웃 시 재시도·재접속을 하지 않고 즉시 포기한다.
        """
        if count <= 0:
            return []

        op = "read_coils_block"
        async with self._io_lock(op, addr=int(start_addr), priority=priority):
            try:
                resp = await self._run_io(
                    op, int(start_addr), "read_coils",
                    int(start_addr), count=int(count), priority=priority,
                )
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
            
    def _snapshot_budget_s(self) -> float:
        try:
            v = float(getattr(cfgc, "PLC_COIL_LOG_BUDGET_S", 4.0))
        except Exception:
            v = 4.0
        return v if v > 0.0 else 4.0

    async def _yield_to_priority(self, deadline: float) -> bool:
        """우선순위 대기자가 빌 때까지 짧게 폴링하며 양보한다.
        ★ 반드시 락을 놓은 상태(블록 사이)에서만 호출할 것.
        True=진행 가능, False=예산 초과
        """
        while self._priority_waiters > 0:
            if time.perf_counter() >= deadline:
                return False
            await asyncio.sleep(0.02)
        return time.perf_counter() < deadline

    async def snapshot_all_coils_fast(
        self,
        *,
        keys: Optional[Iterable[str]] = None,
        max_coils_per_req: Optional[int] = None,
        max_gap: Optional[int] = None,
        skip_if_busy: bool = False,
    ) -> Dict[str, bool]:
        """
        PLC_COIL_MAP에 있는 실제 사용 코일만 sparse block read로 스냅샷.
        - 기존처럼 min~max 전체를 훑지 않음
        - max_gap/max_coils_per_req 를 명시하지 않으면 현재 블록 계획
          (self._coil_plan_idx)을 사용한다. 명시하면 그 값이 우선한다.
        - 우선순위 I/O 대기자가 생기면 포기하지 않고 양보한 뒤
          같은 블록부터 이어서 읽는다. 예산 초과 시에만 {} 반환.
        """
        if skip_if_busy and self.is_busy():
            return {}

        use_keys = list(keys) if keys is not None else list(PLC_COIL_MAP.keys())
        addr_map = {k: PLC_COIL_MAP[k] for k in use_keys if k in PLC_COIL_MAP}
        if not addr_map:
            return {}

        out: Dict[str, bool] = {}

        if max_gap is not None or max_coils_per_req is not None:
            # 호출자가 명시한 값 우선 (기존 기본값 사용)
            ranges = self._build_sparse_ranges(
                addr_map.values(),
                max_gap=max(0, int(8 if max_gap is None else max_gap)),
                max_span=max(1, int(64 if max_coils_per_req is None else max_coils_per_req)),
            )
        else:
            _idx, ranges = self._coil_ranges_for_plan(addr_map.values(), self._coil_plan_idx)
            if _idx != self._coil_plan_idx:
                # FC1 한도 방어로 계획이 밀린 경우 그 결과를 고정
                self._coil_plan_idx = _idx

        deadline = time.perf_counter() + self._snapshot_budget_s()

        for start, cnt in ranges:
            # ✅ 양보: 대기자가 0이 될 때까지 기다렸다가 같은 블록부터 재개.
            #    락은 블록 사이라 이미 놓은 상태이므로 락을 쥐고 기다리지 않는다.
            if not await self._yield_to_priority(deadline):
                return {}

            try:
                bits = await self.read_coils_block(start, cnt, priority="low")
            except PLCError as e:
                if getattr(e, "code", None) == "E403":
                    # 블록이 PLC 주소 범위를 벗어난 것으로 보고 한 단계 보수적으로
                    self._demote_coil_plan(addr_map.values(), reason=str(e))
                raise

            end = start + cnt

            for k, a in addr_map.items():
                if start <= a < end:
                    out[k] = bool(bits[a - start])

        return out

    def _note_coil_plan_result(self, addrs, ok: bool) -> None:
        """스냅샷 tick 결과를 반영한다.
        ok=False 면 연속 성공 카운터를 리셋, ok=True 면 누적해 승격을 판정한다."""
        if not ok:
            self._coil_plan_ok_streak = 0
            return

        base = self._default_coil_plan_idx()
        if int(self._coil_plan_idx) <= base:
            # 이미 기본 계획 이상(=덜 보수적)이면 승격할 것이 없다
            self._coil_plan_ok_streak = 0
            return

        need = self._coil_plan_promote_after()
        if need <= 0:
            return

        self._coil_plan_ok_streak = int(self._coil_plan_ok_streak) + 1
        if self._coil_plan_ok_streak < need:
            return

        self._coil_plan_ok_streak = 0
        old_idx = int(self._coil_plan_idx)
        new_idx = max(base, old_idx - 1)
        if new_idx == old_idx:
            return
        try:
            addrs = list(addrs)
            _, old_ranges = self._coil_ranges_for_plan(addrs, old_idx)
            _chk, new_ranges = self._coil_ranges_for_plan(addrs, new_idx)
            if _chk != new_idx:
                # FC1 한도 방어로 되밀린 계획이면 승격하지 않는다
                return
            self._coil_plan_idx = new_idx
            self.log(
                "WARN PLC COIL LOG: block plan %d(blocks=%d, max_span=%d) -> %d(blocks=%d, max_span=%d) "
                "[promote after %d ok]",
                old_idx, len(old_ranges), self._COIL_BLOCK_PLANS[old_idx][1],
                new_idx, len(new_ranges), self._COIL_BLOCK_PLANS[new_idx][1],
                need,
            )
        except Exception:
            pass

    def _demote_coil_plan(self, addrs, *, reason: str = "") -> None:
        """E403(주소 범위 초과)에서 한 단계 보수적인 계획으로 내린다.
        계획이 실제로 바뀐 순간에만 로그를 한 줄 남긴다."""
        last = len(self._COIL_BLOCK_PLANS) - 1
        old_idx = int(self._coil_plan_idx)
        if old_idx >= last:
            return
        try:
            addrs = list(addrs)
            _, old_ranges = self._coil_ranges_for_plan(addrs, old_idx)
            new_idx, new_ranges = self._coil_ranges_for_plan(addrs, old_idx + 1)
            if new_idx == old_idx:
                return
            self._coil_plan_idx = new_idx
            self._coil_plan_ok_streak = 0
            self.log(
                "WARN PLC COIL LOG: block plan %d(blocks=%d, max_span=%d) -> %d(blocks=%d, max_span=%d) [E403] %s",
                old_idx, len(old_ranges), self._COIL_BLOCK_PLANS[old_idx][1],
                new_idx, len(new_ranges), self._COIL_BLOCK_PLANS[new_idx][1],
                reason,
            )
        except Exception:
            self._coil_plan_idx = min(old_idx + 1, last)

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
                  low 는 타임아웃 시 재시도·재접속을 하지 않고 즉시 포기한다.
        """
        if count <= 0:
            return []

        op = "read_regs_block"
        async with self._io_lock(op, addr=int(start_addr), priority=priority):
            try:
                resp = await self._run_io(
                    op, int(start_addr), "read_holding_registers",
                    int(start_addr), count=int(count), priority=priority,
                )
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
        skip_if_busy: bool = False,
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

        # ✅ 양보: 대기자가 빌 때까지 기다렸다가 1회 재시도. 예산 초과 시에만 {}.
        if not await self._yield_to_priority(time.perf_counter() + self._snapshot_budget_s()):
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
                _fallback_log_root() / "PLC_Coil",
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

        # ✅ 스킵 사유 계측
        try:
            summary_s = float(getattr(cfgc, "PLC_COIL_LOG_SUMMARY_S", 60.0))
        except Exception:
            summary_s = 60.0
        stats = dict(ok=0, disconnected=0, empty_snapshot=0,
                     budget_timeout=0, plc_error=0, write_failed=0, backoff=0)
        last_summary = time.perf_counter()

        def _emit_summary(force: bool = False) -> None:
            nonlocal last_summary
            now = time.perf_counter()
            if not force and (now - last_summary) < max(1.0, summary_s):
                return
            last_summary = now
            skips = sum(v for k, v in stats.items() if k != "ok")
            if skips > 0:
                try:
                    n_blocks = len(self._coil_ranges_for_plan(
                        [PLC_COIL_MAP[k] for k in keys if k in PLC_COIL_MAP],
                        self._coil_plan_idx)[1])
                except Exception:
                    n_blocks = -1
                self.log(
                    "WARN PLC COIL LOG summary(%.0fs): ok=%d disconnected=%d empty=%d "
                    "budget_timeout=%d plc_error=%d write_failed=%d backoff=%d (plan=%d, blocks=%d)",
                    max(1.0, summary_s), stats["ok"], stats["disconnected"],
                    stats["empty_snapshot"], stats["budget_timeout"],
                    stats["plc_error"], stats["write_failed"], stats["backoff"],
                    self._coil_plan_idx, n_blocks,
                )
            for k in stats:
                stats[k] = 0

        while not evt.is_set():
            t0 = time.perf_counter()
            dt = datetime.now()

            # ✅ PLC가 끊긴 상태에서는 로거가 connect/재시도를 하지 않음 → 공정 영향 0에 더 가까워짐
            if not self.is_connected():
                stats["disconnected"] += 1
                self._note_coil_plan_result([PLC_COIL_MAP[k] for k in keys if k in PLC_COIL_MAP], False)
                _emit_summary()
                await asyncio.sleep(interval)
                continue

            # ✅ PLC 가 흔들리는 동안(연속 타임아웃 진행 중)에는 로거가 손을 뗀다.
            #    하트비트나 다른 명령이 성공해 카운터가 0 이 되면 자동 재개된다.
            if int(getattr(self, "_consec_timeouts", 0)) > 0:
                stats["backoff"] += 1
                self._note_coil_plan_result([PLC_COIL_MAP[k] for k in keys if k in PLC_COIL_MAP], False)
                _emit_summary()
                await asyncio.sleep(interval)
                continue

            # ✅ 공정 task가 먼저 락을 잡을 기회를 주기(우선순위 체감 개선)
            #    is_busy() 선차단은 제거 — 실제 경합 회피는 snapshot 쪽
            #    _priority_waiters 양보가 담당한다.
            await asyncio.sleep(0)

            # ✅ 코일 스냅샷(블록 읽기). 실패해도 공정 영향 없게 예외 삼킴.
            _snap_t0 = time.perf_counter()
            try:
                snap = await self.snapshot_all_coils_fast(keys=keys)
            except Exception as e:
                stats["plc_error"] += 1
                self._note_coil_plan_result([PLC_COIL_MAP[k] for k in keys if k in PLC_COIL_MAP], False)
                _emit_summary()
                self.log("WARN PLC COIL LOG: snapshot failed (ignored): %r", e)
                await asyncio.sleep(interval)
                continue

            if not snap:
                # 예산 초과(양보 지속)인지, 대상 키가 없어 빈 것인지 구분
                if (time.perf_counter() - _snap_t0) >= self._snapshot_budget_s():
                    stats["budget_timeout"] += 1
                else:
                    stats["empty_snapshot"] += 1
                self._note_coil_plan_result([PLC_COIL_MAP[k] for k in keys if k in PLC_COIL_MAP], False)
                _emit_summary()
                await asyncio.sleep(interval)
                continue

            reg_snap: Dict[str, int] = {}
            if reg_keys:
                try:
                    reg_snap = await self.snapshot_regs_fast(keys=reg_keys)
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

                stats["ok"] += 1
                # ✅ 완전 성공(오류 없음 + 예산 초과 없음 + 빈 결과 아님)
                self._note_coil_plan_result([PLC_COIL_MAP[k] for k in keys if k in PLC_COIL_MAP], True)

            except Exception as e:
                # ✅ 최종 실패는 공정 영향 없게 무시
                stats["write_failed"] += 1
                self.log("PLC COIL LOG: write failed (ignored): %r", e)

            _emit_summary()

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

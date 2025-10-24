# lib/config_common.py
import os
from pathlib import Path

# ===== TCP 유휴재연결(초) =====
IG_INACTIVITY_REOPEN_S  = 0   # 0 → 사용 안함
MFC_INACTIVITY_REOPEN_S = 0
TSP_INACTIVITY_REOPEN_S = 0
DCP_INACTIVITY_REOPEN_S = 0   # dc_pulse가 TCP-Serial이면 동일 적용
RFP_INACTIVITY_REOPEN_S = 0

# ===== TCP Keepalive 사용 여부(대부분 False 권장: Inactivity와 충돌 방지) =====
IG_TCP_KEEPALIVE  = False
MFC_TCP_KEEPALIVE = False
TSP_TCP_KEEPALIVE = False
DCP_TCP_KEEPALIVE = False
RFP_TCP_KEEPALIVE = False

# === 디버그 프린트 여부 ===
DEBUG_PRINT = False

# ===== 종료 시퀀스 대기 정책 =====
SHUTDOWN_STEP_TIMEOUT_MS = 2500   # 각 종료 스텝의 '확인 대기' 최대 시간
SHUTDOWN_STEP_GAP_MS     = 500    # 종료 스텝 간 최소 간격(물리 반영 시간)

# ===== 공통 통신 타이밍/타임아웃 =====
ACK_TIMEOUT_MS         = 2000
QUERY_TIMEOUT_MS       = 4500
RECV_FRAME_TIMEOUT_MS  = 4000
CMD_GAP_MS             = 1500
POST_WRITE_DELAY_MS    = 1500
ACK_FOLLOWUP_GRACE_MS  = 500

# 폴링
POLL_INTERVAL_MS       = 30_000
POLL_QUERY_TIMEOUT_MS  = 9000
POLL_START_DELAY_AFTER_RF_ON_MS = 800

# ======================================================================
# RGA (외부 프로그램 실행 + CSV 저장)
# ======================================================================
# 채널별 CSV 기본 저장 경로(필요 시 main에서 ch를 선택해 사용)
RGA_XLSX_PATH = {
    "ch1": r"\\VanaM_NAS\VanaM_Sputter\RGA\Ch.1\RGA_spectrums.csv",
    "ch2": r"\\VanaM_NAS\VanaM_Sputter\RGA\Ch.2\RGA_spectrums.csv",
}
RGA_XLSX_SHEET = "Histogram"

# (선택) RGA 장비 LAN 접근 정보(외부 앱이 아닌 직접 접근 시 사용)
RGA_NET = {
    "ch1": {"ip": "192.168.1.20", "user": "admin", "password": "admin"},
    "ch2": {"ip": "192.168.1.21", "user": "admin", "password": "admin"},
}

# ======================================================================
# TSP (공통; 포트만 공통 사용)
# ======================================================================
TSP_TCP_HOST = "192.168.1.50"
TSP_TCP_PORT = 4004
TSP_ADDR = 0x01
TSP_CONNECT_TIMEOUT_S = 1.0
TSP_WRITE_TIMEOUT_S   = 1.0
TSP_POST_SEND_DELAY_MS = 10

# ======================================================================
# IG (공통 상수; 포트는 채널 파일에서 오버라이드)
# ======================================================================
IG_TCP_HOST = "192.168.1.50"
IG_TX_EOL   = b"\r"
IG_SKIP_ECHO = True
IG_WAIT_TIMEOUT = 600   # 목표 압력 대기 총 한도(초)
IG_CONNECT_TIMEOUT_S = 3.0
IG_TIMEOUT_MS = 3000
IG_GAP_MS = 1000
IG_POLLING_INTERVAL_MS = 10_000
IG_WATCHDOG_INTERVAL_MS = 2_000
IG_RECONNECT_BACKOFF_START_MS = 1000
IG_RECONNECT_BACKOFF_MAX_MS = 20_000
IG_REIGNITE_MAX_ATTEMPTS = 3
IG_REIGNITE_BACKOFF_MS = [2000, 5000, 10000]

# ======================================================================
# RF Power 보정 및 제어 설정(공통)
# ======================================================================
RF_MAX_POWER = 600
RF_RAMP_STEP = 3.0
RF_MAINTAIN_STEP = 0.1
RF_TOLERANCE_POWER = 1
# 보정계수
RF_PARAM_WATT_TO_DAC = 6.79
RF_OFFSET_WATT_TO_DAC = 6.93
RF_PARAM_ADC_TO_WATT = 0.0236431
RF_OFFSET_ADC_TO_WATT = -1.3362
RF_WATT_PER_VOLT = 63.49

# ======================================================================
# DC Power 보정 및 제어 설정(공통)
# ======================================================================
DC_MAX_POWER = 1000
DC_INTERVAL_MS = 3000
DC_RAMP_STEP = 5
DC_MAINTAIN_STEP = 1
DC_TOLERANCE_POWER = 1
# 보정계수
DC_PARAM_WATT_TO_DAC = 4.0835
DC_OFFSET_WATT_TO_DAC = 5.275
DC_PARAM_ADC_TO_VOLT = 0.076112
DC_OFFSET_ADC_TO_VOLT = -6.8453
DC_PARAM_ADC_TO_AMP  = 0.000150567
DC_OFFSET_ADC_TO_AMP = -0.003118

# ======================================================================
# OES
# ======================================================================
OES_AVG_COUNT = 3

# ======================================================================
# MFC (공통; 포트는 채널 파일에서 오버라이드)
# ======================================================================
MFC_TCP_HOST = "192.168.1.50"
MFC_TX_EOL   = b"\r"
MFC_SKIP_ECHO = True
MFC_CONNECT_TIMEOUT_S = 3.0

FLOW_ERROR_TOLERANCE = 0.05
FLOW_ERROR_MAX_COUNT = 3

MFC_POST_OPEN_QUIET_MS = 800
MFC_ALLOW_NO_REPLY_DRAIN_MS = 80
MFC_FIRST_CMD_EXTRA_TIMEOUT_MS = 2000

MFC_POLLING_INTERVAL_MS       = 2000
MFC_STABILIZATION_INTERVAL_MS = 1000
MFC_WATCHDOG_INTERVAL_MS      = 1500

MFC_RECONNECT_BACKOFF_START_MS = 1000
MFC_RECONNECT_BACKOFF_MAX_MS   = 20_000

MFC_TIMEOUT   = 1000
MFC_GAP_MS    = 1000
MFC_DELAY_MS  = 1000
MFC_DELAY_MS_VALVE = 5000

# 채널별 유량 스케일(필요시 조정)
MFC_SCALE_FACTORS = {1: 1.0, 2: 10.0, 3: 10.0}

# 압력 값 스케일 및 표기
MFC_PRESSURE_SCALE   = 0.1
MFC_PRESSURE_DECIMALS = 3
MFC_SP1_VERIFY_TOL   = 0.1

# 장비 ASCII 명령 템플릿(장비 레이어에서 사용)
MFC_COMMANDS = {
    'SET_ONOFF_MASK': lambda bits: f"L0{bits}",
    'FLOW_ON':  lambda channel: f"L{int(channel)}1",
    'FLOW_OFF': lambda channel: f"L{int(channel)}0",
    'MFC_ZEROING': lambda channel: f"L{4+int(channel)}1",
    'FLOW_SET': lambda channel, value: f"Q{int(channel)} {value}",
    'READ_FLOW_ALL': "R60",
    'READ_FLOW':     lambda channel: f"R6{int(channel)}",
    'READ_MFC_ON_OFF_STATUS': "R69",
    'READ_PRESSURE': "R5",
    'READ_SP1_VALUE': "R1",
    'READ_VALVE_POSITION': "R6",
    'READ_SYSTEM_STATUS': "R7",
    'READ_FLOW_SET': lambda channel: f"R6{4+int(channel)}",
    'VALVE_OPEN': "O",
    'VALVE_CLOSE': "C",
    'PS_ZEROING': "Z1",

    # --- Set-Point 실행/설정 ---
    'SP1_ON':  "D1",
    'SP2_ON':  "D2",
    'SP3_ON':  "D3",
    'SP4_ON':  "D4",
    'SP1_SET': lambda value: f"S1 {value}",
    'SP2_SET': lambda value: f"S2 {value}",
    'SP3_SET': lambda value: f"S3 {value}",
    'SP4_SET': lambda value: f"S4 {value}",
}

# ======================================================================
# RF Pulse / DC Pulse (공통 기본값; 주소는 채널 파일에서 필요 시 오버라이드)
# ======================================================================
RFPULSE_PORT = "192.168.1.50:4005"
RFPULSE_BAUD = 9600
RFPULSE_ADDR = 1
RFPULSE_DEFAULT_DELAY_MS = 180

DCPULSE_PORT = "192.168.1.50:4007"
DCPULSE_BAUD = 9600
DCPULSE_ADDR = 1
DCPULSE_DEFAULT_DELAY_MS = 180

# ======================================================================
# (선택) 프로세스 컨트롤러 기능 지원 플래그 기본값
#  - 실제 장비 구성에 맞춰 채널 파일에서 오버라이드
# ======================================================================
SUPPORTS_DC = True
SUPPORTS_RF_CONT = False
SUPPORTS_RFPULSE = True

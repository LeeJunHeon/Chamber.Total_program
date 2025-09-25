MFC# lib/config_ch2.py
import os
from pathlib import Path

# === Google Chat 알림 ===
def _get_local(name, default=None):
    try:
        from . import config_local as _cl  # 없으면 ImportError
        return getattr(_cl, name, default)
    except Exception:
        return default
    
CHAT_WEBHOOK_URL = os.environ.get("CHAT_WEBHOOK_URL") or _get_local("CHAT_WEBHOOK_URL") or None
ENABLE_CHAT_NOTIFY  = True  # 끄고 싶을 때 False

# === 디버그 프린트 여부 ===
DEBUG_PRINT = False

# ===== 종료 시퀀스 대기 정책(필요시 조정) =====
SHUTDOWN_STEP_TIMEOUT_MS = 2500   # 각 종료 스텝의 '확인 대기' 최대 시간
SHUTDOWN_STEP_GAP_MS     = 500    # 종료 스텝 간 최소 간격(물리 반영 시간)

# ===== RGA (LAN) =====
RGA_NET = {
    "ch1": {"ip": "192.168.1.20", "user": "admin", "password": "admin"},
    "ch2": {"ip": "192.168.1.21", "user": "admin", "password": "admin"},
}

# NAS 엑셀 경로(히스토그램 저장)
RGA_XLSX_PATH = {
    "ch1": r"\\VanaM_NAS\VanaM_Sputter\RGA\Ch.1\RGA_spectrums.csv",
    "ch2": r"\\VanaM_NAS\VanaM_Sputter\RGA\Ch.2\RGA_spectrums.csv",
}
RGA_XLSX_SHEET = "Histogram"


# === 시리얼 포트 설정 ===






# ===== 타이밍/타임아웃 상수 =====
ACK_TIMEOUT_MS         = 2000   # 쓰기(설정) 명령 후 ACK/프레임 대기 시간
QUERY_TIMEOUT_MS       = 4500   # 읽기(리드백) 명령 후 전체 대기 시간
RECV_FRAME_TIMEOUT_MS  = 4000   # 저수준 프레임 대기 타임아웃
CMD_GAP_MS             = 1500   # 명령 간 최소 간격(밀리초)
POST_WRITE_DELAY_MS    = 1500   # 각 쓰기 명령 후 여유 대기(밀리초)

# ★ ACK 뒤 CSR/데이터 프레임을 잠깐 더 기다리는 그레이스 윈도우
ACK_FOLLOWUP_GRACE_MS  = 500

# 폴링
POLL_INTERVAL_MS       = 30_000
POLL_QUERY_TIMEOUT_MS  = 9000
POLL_START_DELAY_AFTER_RF_ON_MS = 800

# 워치독/재연결(지수 백오프)
DCPULSE_WATCHDOG_INTERVAL_MS        = 1000
DCPULSE_RECONNECT_BACKOFF_START_MS  = 1000
DCPULSE_RECONNECT_BACKOFF_MAX_MS    = 20_000

# ======================================================================
# RGA
# ======================================================================
# RGA_PORT = "COM17" RGA를 직접 연결하지 않고 외부 프로그램을 사용
# RGA_BAUD = 9600
RGA_PROGRAM_PATH = r"\\VanaM_NAS\VanaM_Sputter\Programs\RGA_Ch2.exe"
RGA_CSV_PATH = r"\\VanaM_NAS\VanaM_Sputter\RGA\Ch.2\RGA_spectrums.csv"
RGA_PROGRAM_PATH = Path(RGA_PROGRAM_PATH)
RGA_CSV_PATH = Path(RGA_CSV_PATH)

# ======================================================================
# TSP
# ======================================================================
TSP_TCP_HOST = "192.168.1.50"
TSP_TCP_PORT = 4004

# TSP 프레임 주소(헤더: 0x80 | (addr & 0x0F))
TSP_ADDR = 0x01

# 연결/쓰기 타임아웃(초)
TSP_CONNECT_TIMEOUT_S = 1.0
TSP_WRITE_TIMEOUT_S   = 1.0

# 전송 후 짧은 대기(ms) — 장비가 응답을 주지 않는 모델이므로 최소 대기만
TSP_POST_SEND_DELAY_MS = 10

# ======================================================================
# IG
# ======================================================================
IG_TCP_HOST = "192.168.1.50"       # NPort IP
IG_TCP_PORT = 4001                 # NPort Port(예: IG=4002)
IG_TX_EOL   = b"\r"                # IG 명령 줄끝(EOL)
IG_SKIP_ECHO = True                # 장비가 에코하면 True
IG_WAIT_TIMEOUT = 600              # 목표 압력 대기 총 한도(초). 예: 10분

IG_CONNECT_TIMEOUT_S = 3.0         # IG (MOXA TCP Server 연결) 전용 소켓 연결 타임아웃(초)
IG_TIMEOUT_MS = 3000               # 명령 응답 타임아웃(ms). 예: 1.5초
IG_GAP_MS = 1000                   # 명령 간 인터커맨드 gap(ms). RDI 반복 간격 등
IG_POLLING_INTERVAL_MS = 10_000    # 주기 폴링 간격(ms). 요구사항 기본 10초
IG_WATCHDOG_INTERVAL_MS = 2_000    # 연결 상태 감시 주기(ms)
IG_RECONNECT_BACKOFF_START_MS = 1000   # 재연결 백오프 시작값(ms)
IG_RECONNECT_BACKOFF_MAX_MS = 20_000   # 재연결 백오프 최대(ms)

# ✅ IG 재점등(ON 재시도) 제어
IG_REIGNITE_MAX_ATTEMPTS = 3                    # 총 재점등 시도 횟수 상한
IG_REIGNITE_BACKOFF_MS = [2000, 5000, 10000]    # 각 시도 사이 대기(ms): 2s, 5s, 10s


# ======================================================================
# RF Power 보정 및 제어 설정
# ======================================================================
RF_MAX_POWER = 600      # 최대 설정 가능 파워 (W)
RF_RAMP_STEP = 1.0      # Ramp-up 시 한 스텝당 올릴 파워 (W)
RF_MAINTAIN_STEP = 0.1  # 파워 유지 시 미세조정할 파워 (W)
RF_TOLERANCE_POWER = 1  # 목표 파워 도달로 인정할 허용 오차 (± W)

# --- 보정 계수 (Calibration Coefficients) ---
# 1. 목표 파워(W) -> FADUINO DAC 값 변환용 (in RFpower.py)
# 수식: DAC = (RF_PARAM_WATT_TO_DAC * target_watt) + RF_OFFSET_WATT_TO_DAC
RF_PARAM_WATT_TO_DAC = 6.79   # 기울기 (param)
RF_OFFSET_WATT_TO_DAC = 6.93  # y절편 (offset)

# 2. FADUINO ADC 값 -> 실제 파워(W) 변환용 (in Faduino.py)
# 수식: Watts = (RF_PARAM_ADC_TO_WATT * adc_raw_value) + RF_OFFSET_ADC_TO_WATT
RF_PARAM_ADC_TO_WATT = 0.0236431  # 기울기 (param)
RF_OFFSET_ADC_TO_WATT = -1.3362   # y절편 (offset)

# RF Reflected: 전압(볼트) -> 와트 환산 상수 (필요시 조정)
RF_WATT_PER_VOLT = 63.49


# ======================================================================
# DC Power 보정 및 제어 설정
# ======================================================================
# --- 제어 로직 파라미터 ---
DC_MAX_POWER = 1000       # 최대 설정 가능 파워 (W)
DC_INTERVAL_MS = 1000     # 제어 루프 실행 간격 (milliseconds)
DC_RAMP_STEP = 5          # 파워를 올릴 때 DAC 값 조정 단계
DC_MAINTAIN_STEP = 1      # 파워를 유지할 때 DAC 값 미세 조정 단계
DC_TOLERANCE_POWER = 1    # 목표 파워 도달로 간주할 허용 오차 (W)

# --- 보정 계수 (Calibration Coefficients) ---
# 1. 목표 파워(W) -> FADUINO DAC 값 변환용 (in DCpower.py)
# 수식: DAC = (DC_PARAM_WATT_TO_DAC * target_watt) + DC_OFFSET_WATT_TO_DAC
DC_PARAM_WATT_TO_DAC = 4.0835   # 기울기 (param)
DC_OFFSET_WATT_TO_DAC = 5.275   # y절편 (offset)

# 2. FADUINO 전압 ADC 값 -> 실제 전압(V) 변환용 (in Faduino.py)
# 수식: Voltage = (DC_PARAM_ADC_TO_VOLT * adc_v_raw) + DC_OFFSET_ADC_TO_VOLT
DC_PARAM_ADC_TO_VOLT = 0.076112 # 전압 기울기 (param)
DC_OFFSET_ADC_TO_VOLT = -6.8453 # 전압 y절편 (offset)

# 3. FADUINO 전류 ADC 값 -> 실제 전류(A) 변환용 (in Faduino.py)
# 수식: Current = (DC_PARAM_ADC_TO_AMP * adc_c_raw) + DC_OFFSET_ADC_TO_AMP
DC_PARAM_ADC_TO_AMP = 0.000150567 # 전류 기울기 (param)
DC_OFFSET_ADC_TO_AMP = -0.003118  # 전류 y절편 (offset)


# ======================================================================
# OES
# ======================================================================
OES_AVG_COUNT = 3 # OES 측정 시 평균을 낼 횟수


# ======================================================================
# MFC
# ======================================================================
MFC_TCP_HOST = "192.168.1.50"     # NPort IP (IG와 동일 IP)
MFC_TCP_PORT = 4003               # NPort Port (채널에 맞춰 4006)
MFC_TX_EOL   = b"\r"              # 줄끝
MFC_SKIP_ECHO = True              # 장비 에코가 있으면 True
MFC_CONNECT_TIMEOUT_S = 3.0       # TCP connect 타임아웃(초)

FLOW_ERROR_TOLERANCE = 0.05  # 5% 오차 허용
FLOW_ERROR_MAX_COUNT = 3     # 3회 연속 불일치 시 경고

# Qt가 해주던 여유·드레인 구간을 asyncio에서도 보장
MFC_POST_OPEN_QUIET_MS = 800   # 포트 오픈 직후 quiet(잔여 라인 토해내는 시간)
MFC_ALLOW_NO_REPLY_DRAIN_MS = 80  # no-reply 후 늦은 에코 흡수
MFC_FIRST_CMD_EXTRA_TIMEOUT_MS = 2000  # 오픈 직후 첫 응답 여유

# === MFC 타이밍/간격 상수 ===
# 주기/타이머
MFC_POLLING_INTERVAL_MS       = 2000    # polling 주기
MFC_STABILIZATION_INTERVAL_MS = 1000    # 1초마다 목표 대비 실제값 확인
MFC_WATCHDOG_INTERVAL_MS      = 1500    # 포트가 닫혔는지 주기적으로 점검

# 재연결 백오프
MFC_RECONNECT_BACKOFF_START_MS = 1000    # 포트 오류/타임아웃 시 첫 재연결 대기시간
MFC_RECONNECT_BACKOFF_MAX_MS   = 20_000  # 지수 백오프 최대 상한

# === 전역 통일 상수 ===
MFC_TIMEOUT   = 1000         # 모든 명령 timeout
MFC_GAP_MS    = 4000         # 모든 인터커맨드 간격(gap)
MFC_DELAY_MS  = 1000         # 모든 검증/재시도 지연
MFC_DELAY_MS_VALVE = 5000    # 밸브 이동/재전송 대기(5초)

# [신규] 채널별 유량 스케일 팩터 정의
MFC_SCALE_FACTORS = {
    1: 1.0,   # Channel 1 (Ar): 1:1 스케일
    2: 10.0,  # Channel 2 (O2): 10배 스케일
    3: 10.0,  # Channel 3 (N2): 10배 스케일
}

# UI ↔ HW 스케일 (SP1/압력 공용)
# - 장비값(HW) → UI:  ui = hw / MFC_PRESSURE_SCALE
# - UI → 장비(HW):   hw = ui * MFC_PRESSURE_SCALE
MFC_PRESSURE_SCALE = 0.1        # 예) UI 2.00 ↔ HW 0.20
MFC_PRESSURE_DECIMALS = 3       # UI 표시에 사용할 소수 자리
MFC_SP1_VERIFY_TOL = 0.1        # SP1_SET 검증 허용 오차(장비 단위)

# 명령어는 ASCII 문자로 전송해야 되며, \r으로 끝나야 함.
MFC_COMMANDS = {
    # --- MFC 쓰기(Write) 명령어 ---
    # 일괄 ON/OFF: L0 뒤에 비트마스크(예: '1010')
    'SET_ONOFF_MASK': lambda bits: f"L0{bits}",

    # (선택) 단일 채널 ON/OFF는 유지해도 되지만 내부 로직에선 쓰지 않음(폴백용)
    'FLOW_ON':  lambda channel: f"L{int(channel)} 1",
    'FLOW_OFF': lambda channel: f"L{int(channel)} 0",

    'MFC_ZEROING': lambda channel: f"L{4+channel} 1",         # 지정된 채널의 MFC를 Zeroing합니다 (Ch1=L5, Ch2=L6 ...). 
    'FLOW_SET': lambda channel, value: f"Q{channel} {value}", # 지정된 채널의 Flow 값을 설정합니다 (% of Full Scale). 

    # === 읽기 ===
    'READ_FLOW_ALL': "R60",                    # 모든 채널 유량
    'READ_FLOW':     lambda channel: f"R6{int(channel)}",  # 폴백/디버깅용
    'READ_MFC_ON_OFF_STATUS': "R69",
    'READ_PRESSURE': "R5",
    'READ_SP1_VALUE': "R1",
    'READ_VALVE_POSITION': "R6",
    'READ_SYSTEM_STATUS': "R7",
    'READ_FLOW_SET': lambda channel: f"R6{4+int(channel)}", # 지정된 채널의 Flow 설정 값을 읽습니다 (Ch1=R65, Ch2=R66 ...). 

    # --- 공통 명령어 (채널 지정 불필요) ---
    'VALVE_OPEN': "O",  # Throttle Valve를 엽니다. 
    'VALVE_CLOSE': "C", # Throttle Valve를 닫습니다. 
    'PS_ZEROING': "Z1", # 압력 센서(게이지)를 Zeroing합니다. 
    'SP4_ON': "D4",     # Set-point 4를 실행합니다. 
    'SP1_ON': "D1",     # Set-point 1을 실행합니다. 
    'SP1_SET': lambda value: f"S1 {value}", # Set-point 1의 목표 압력 값을 설정합니다. 
}

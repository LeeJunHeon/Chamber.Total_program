# lib/config_common.py

# === 외부 제어 호스트 서버 설정 ===
HOST_SERVER_HOST = "0.0.0.0"   # 로컬만 쓰면 "127.0.0.1"
HOST_SERVER_PORT = 50070       # 방화벽 인바운드 허용 필요

# === 내부 브릿지(메인 공정 프로그램 ↔ 로봇 서버 프록시) 설정 ===
# - 메인 공정 프로그램이 로봇 서버에게 제공하는 "로컬 전용" Host(IPC) 포트
# - 반드시 127.0.0.1 로 고정(외부 접근 차단)
# PROCESS_HOST_HOST = "127.0.0.1"
# PROCESS_HOST_PORT = 50071

# # (추천) 로봇 서버(프록시) → 공정 프로그램(업스트림) 요청 타임아웃
# PROCESS_HOST_CONNECT_TIMEOUT_S = 5.0

# # ✅ 상태 전용(짧게): GET_SPUTTER_STATUS 같은 폴링
# PROCESS_HOST_STATUS_REQUEST_TIMEOUT_S = 5.0

# # ✅ 명령 전용(길게): CHUCK_UP 같이 오래 걸리는 동작
# PROCESS_HOST_CMD_REQUEST_TIMEOUT_S = 900.0

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
POLL_INTERVAL_MS       = 5_000 #5초 rf pulse만 사용
POLL_QUERY_TIMEOUT_MS  = 9000
POLL_START_DELAY_AFTER_RF_ON_MS = 800


# ======================================================================
# RGA (외부 프로그램 실행 + CSV 저장)
# ======================================================================
# ✅ RGA worker 실행/응답 타임아웃(초) - 요청대로 1분
RGA_WORKER_TIMEOUT_S = 60.0

# ✅ ProcessController가 RGA_OK 토큰을 기다리는 최대 시간(ms)
#    worker timeout(60s) + 이벤트/그래프 처리 여유 5s
RGA_STEP_TIMEOUT_MS = int(RGA_WORKER_TIMEOUT_S * 1000) + 5_000

# 채널별 CSV 기본 저장 경로(필요 시 main에서 ch를 선택해 사용)
RGA_XLSX_PATH = {
    "ch1": r"\\VanaM_NAS\VanaM_Sputter\RGA\Ch.1\RGA_spectrums.csv",
    "ch2": r"\\VanaM_NAS\VanaM_Sputter\RGA\Ch.2\RGA_spectrums.csv",
}
RGA_XLSX_SHEET = "Histogram"

# ✅ chamber_runtime.py가 찾는 키는 RGA_CSV_PATH라서 호환 키 추가
#    (dict 형태 허용: ch1/ch2에서 꺼내 쓰게 chamber_runtime에서 처리)
RGA_CSV_PATH = RGA_XLSX_PATH

# (선택) RGA 장비 LAN 접근 정보(외부 앱이 아닌 직접 접근 시 사용)
RGA_NET = {
    "ch1": {"ip": "192.168.1.20", "user": "admin", "password": "admin"},
    "ch2": {"ip": "192.168.1.21", "user": "admin", "password": "admin"},
}


# ======================================================================
# IG (공통 상수; 포트는 채널 파일에서 오버라이드)
# ======================================================================
IG_TCP_HOST = "192.168.1.50"              # IG TCP Host (MOXA/NPort)
IG_TX_EOL   = b"\r"                       # IG 명령 EOL (기본 CR)
IG_SKIP_ECHO = True                       # 장비가 보낸 에코라인을 응답에서 무시

IG_CONNECT_TIMEOUT_S = 3.0                # TCP connect 타임아웃(초)
IG_TIMEOUT_MS = 3000                      # 명령 1회 응답 타임아웃(ms)
IG_GAP_MS = 1000                          # 명령 간 최소 간격(ms)

IG_POLLING_INTERVAL_MS = 10_000           # RDI 폴링 기본 주기(ms)
IG_WATCHDOG_INTERVAL_MS = 2_000           # 연결 상태 체크 주기(ms)
IG_RECONNECT_BACKOFF_START_MS = 1000      # 재연결 backoff 시작(ms)
IG_RECONNECT_BACKOFF_MAX_MS = 20_000      # 재연결 backoff 최대(ms)

IG_WAIT_TIMEOUT = 600                     # base pressure 대기 총 한도(초)
IG_REIGNITE_MAX_ATTEMPTS = 3              # "IG OFF" 자동 재점등 최대 횟수
IG_REIGNITE_BACKOFF_MS = [2000, 5000, 10000]  # 재점등 실패 시 backoff(ms)

IG_DRAIN_TIMEOUT_S = 2.0                  # ★ writer.drain() 최대 대기(초) (멈춤 방지)

IG_FIRST_READ_DELAY_MS = 5000             # (선택) SIG 1 후 첫 RDI 전 대기(ms)


# ======================================================================
# RF Power 보정 및 제어 설정(공통)
# ======================================================================
RF_MAX_POWER = 600
RF_RAMP_STEP = 1.0
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
DC_INTERVAL_MS = 5000
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

MFC_POLLING_INTERVAL_MS       = 3000
MFC_STABILIZATION_INTERVAL_MS = 1000
MFC_WATCHDOG_INTERVAL_MS      = 1500

MFC_RECONNECT_BACKOFF_START_MS = 1000
MFC_RECONNECT_BACKOFF_MAX_MS   = 20_000

MFC_TIMEOUT   = 2000
MFC_GAP_MS    = 1000
MFC_DELAY_MS  = 1000
MFC_DELAY_MS_VALVE = 5000

# 채널별 유량 스케일(필요시 조정)
MFC_SCALE_FACTORS = {1: 1.0, 2: 10.0, 3: 1.0}

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
    'READ_SP2_VALUE': "R2",
    'READ_SP3_VALUE': "R3",
    'READ_SP4_VALUE': "R4",
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


# ======================================================================
# ROBOT GET_RECIPE (NAS 레시피 폴더 스캔)
# ======================================================================
ROBOT_RECIPE_ROOT_DIR = r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Recipe"

# 허용 폴더(클라이언트가 이 중 하나만 요청 가능)
ROBOT_RECIPE_FOLDERS = ("CH1", "CH2", "ALD")

# NAS가 순간 느릴 때 무한 대기 방지
RECIPE_SCAN_TIMEOUT_S = 8.0


# =========================
# TSP (Turbo/Trap process)
# =========================
TSP_TCP_HOST = "192.168.1.50"
TSP_TCP_PORT = 4004
TSP_ADDR = 0x01
TSP_CONNECT_TIMEOUT_S = 1.0
TSP_WRITE_TIMEOUT_S   = 1.0
TSP_POST_SEND_DELAY_MS = 10

TSP_IG_TCP_PORT = 4001                    # (선택) IG 포트. 없으면 config_ch1.IG_TCP_PORT 사용

TSP_ON_SEC = 120.0                        # TSP ON 유지 시간(초)
TSP_OFF_SEC = 150.0                       # TSP OFF 유지 시간(초)
TSP_POLL_SEC = 10.0                       # IG RDI 폴링 간격(초)
TSP_FIRST_CHECK_DELAY_SEC = 5.0           # IG ON 후 첫 체크까지 대기(초)
TSP_VERIFY_WITH_STATUS = True             # TSP on/off 후 status(예: 205)로 확인 여부

TSP_COOLDOWN_S = 60.0                     # 다른 공정 종료 후 시작 쿨다운(초)
TSP_TOTAL_TIMEOUT_MARGIN_S = 300.0        # 전체 타임아웃 여유(초)

TSP_DAILY_ENABLE = True                   # 매일 자동 실행
TSP_DAILY_HH = 5                          # 자동 실행 시(HH)
TSP_DAILY_MM = 0                          # 자동 실행 분(MM)

# (선택) UI 기본값
TSP_UI_DEFAULT_TARGET = "2.5e-07"         # TSP 페이지 target 기본 표시값
TSP_UI_DEFAULT_CYCLES = 10                # TSP 페이지 cycles 기본 표시값


# ======================================================================
# PLC (Modbus-TCP)
# ======================================================================
PLC_TCP_HOST = "192.168.1.2"
PLC_TCP_PORT = 502
PLC_UNIT = 1

PLC_TIMEOUT_S = 2.0
PLC_CMD_GAP_MS = 150          # plc.py inter_cmd_gap_s(0.15s)와 매칭
PLC_WATCHDOG_INTERVAL_S = 15.0

PLC_RECONNECT_RETRY = 2
PLC_RECONNECT_DELAY_S = 0.5

PLC_CMD_PULSE_MS = 180

PLC_LOCK_WARN_MS = 1000.0
PLC_IO_WARN_MS = 1500.0

# --- PLC COIL CSV LOGGER ---
PLC_COIL_LOG_INTERVAL_S = 1.0
PLC_COIL_LOG_NAS_DIR = r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\CH1&2\CH1&2_PLC"
# PLC_COIL_LOG_LOCAL_DIR = r"C:\...\Logs\CH1&2\CH1&2_PLC"   # 필요 시

# ======================================================================
# PLC (Calibration / Scaling)  - UI에서 수정 가능
# ======================================================================

# --- DC Power -> DAC 변환 (PLC power_write/power_apply에서 사용) ---
PLC_DC_POWER_MIN_W = 0.0
PLC_DC_POWER_MAX_W = 1000.0
PLC_DC_DAC_FULL_SCALE = 4000
PLC_DC_DAC_OFFSET = 0
PLC_DC_WRITE_INDEX = 0  # 0→WRITE_0, 1→WRITE_1 ...

# --- DC READ 스케일 (power_read에서 사용: raw * scale) ---
PLC_DC_V_SCALE = 0.50316
PLC_DC_I_SCALE = 0.00097405

# --- RF Feedback 보정 (rf_read_fwd_ref에서 사용: a*raw + b) ---
# CH1
PLC_RF_CH1_FWD_A = 0.1503488383
PLC_RF_CH1_FWD_B = 3.0664228165
PLC_RF_CH1_REF_A = 0.1565388751
PLC_RF_CH1_REF_B = 12.2067054791

# CH2
PLC_RF_CH2_FWD_A = 0.15059
PLC_RF_CH2_FWD_B = -0.598
PLC_RF_CH2_REF_A = 0.12940
PLC_RF_CH2_REF_B = -0.267

# --- RF 제로 오프셋(패널 idle 보정) ---
PLC_RF_FORWARD_ZERO_W = 4.0
PLC_RF_REFLECTED_ZERO_W = 14.0
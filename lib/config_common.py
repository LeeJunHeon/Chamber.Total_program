# lib/config_common.py


from pathlib import Path


# =========================================================
# Local fallback paths
# - 폴더는 실제 write 시점에만 생성되도록, 여기서는 Path만 정의
# =========================================================
LOCAL_FALLBACK_ROOT = Path.cwd() / "Logs_LocalFallback"

LOCAL_FALLBACK_CH1_DIR = LOCAL_FALLBACK_ROOT / "CH1"
LOCAL_FALLBACK_CH2_DIR = LOCAL_FALLBACK_ROOT / "CH2"
LOCAL_FALLBACK_SERVER_DIR = LOCAL_FALLBACK_ROOT / "Server"
PLC_COIL_LOG_LOCAL_DIR = LOCAL_FALLBACK_ROOT / "PLC_Coil"


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

# forward power 저출력 감시 파라미터 (UI에서 조정 가능)
RF_LOW_POWER_THRESH_W = 1.0      # 이 W 이하이면 '너무 낮다'로 판단
RF_LOW_POWER_COUNT_MAX_N = 3     # 연속 허용 횟수

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

# 저전력/저전류 감시 파라미터 (UI에서 조정 가능)
DC_LOW_W_THRESH = 1.0               # W 이하이면 '사실상 0W'로 간주
DC_LOW_STREAK_N = 3                 # 연속 N회 기준
DC_LOW_CURRENT_THRESH_A = 0.05      # A 이하를 "전류 거의 0"으로 간주
DC_LOW_CURRENT_STREAK_N = 3         # 연속 N회 기준
DC_WATT_DEADBAND = 0.5              # 연속 전송 억제 데드밴드(W) (init 기본값과 통일)

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
MFC_TCP_PORT = 4003               # ✅ 기본값(채널 파일에서 오버라이드)

MFC_TX_EOL   = b"\r"
MFC_SKIP_ECHO = True
MFC_CONNECT_TIMEOUT_S = 3.0
MFC_DRAIN_TIMEOUT_S = 2.0          # ✅ writer.drain() 최대 대기(초) (멈춤 방지)

FLOW_ERROR_TOLERANCE = 0.05
FLOW_ERROR_MAX_COUNT = 3

MFC_POST_OPEN_QUIET_MS = 800
MFC_ALLOW_NO_REPLY_DRAIN_MS = 80
MFC_FIRST_CMD_EXTRA_TIMEOUT_MS = 2000
MFC_ZEROING_GAP_MS = 1000          # ✅ MFC_ZEROING 후 대기(ms). 기본은 GAP과 동일하게 유지

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

# ✅ 압력 도달 판정(기본값) - UI 단위(mTorr) 기준
MFC_PRESSURE_TOL_ABS = 0.02
MFC_PRESSURE_TOL_REL = 0.05
MFC_PRESSURE_STABLE_COUNT = 3
MFC_PRESSURE_TIMEOUT_SEC = 60.0
MFC_PRESSURE_CHECK_INTERVAL_SEC = 1.0
MFC_PRESSURE_READ_FAIL_STREAK_MAX = 3

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
# DC Pulse (공통 기본값; 주소는 채널 파일에서 필요 시 오버라이드)
# ======================================================================
# (레거시 유지) - 기존 코드 호환용
DCPULSE_PORT = "192.168.1.50:4007"
DCPULSE_BAUD = 9600
DCPULSE_ADDR = 1
DCPULSE_DEFAULT_DELAY_MS = 180

# (권장) dc_pulse.py가 endpoint로 사용하는 키
DCPULSE_TCP_HOST = "192.168.1.50"
DCPULSE_TCP_PORT = 4007

# ----------------------------------------------------------------------
# dc_pulse 런타임 튜닝(공정/통신 안정성 영향) - UI에서 수정 대상
# ----------------------------------------------------------------------
DCP_MAX_POWER_W = 1000

DCP_P_SET_TOL_PCT = 0.05
DCP_P_SET_TOL_W = 15.0
DCP_P_SET_DEVIATE_MAX_N = 3

DCP_I_LOW_THRESH_A = 0.05
DCP_I_LOW_COUNT_MAX_N = 3

DCP_CMD_MAX_RETRIES = 5
DCP_RECOVER_MAX_ATTEMPTS = 5
DCP_WRITE_WORKER_RETRIES = 0
DCP_ENABLE_FAULT_RECOVER = True

DCP_ACTIVATION_CHECK_DELAY_S = 5.0
DCP_POLL_INTERVAL_S = 5.0
DCP_CONNECT_TIMEOUT_S = 3.0

DCP_TIMEOUT_MS = 2500
DCP_GAP_MS = 1000
DCP_WATCHDOG_INTERVAL_MS = 1000
DCP_RECONNECT_BACKOFF_START_MS = 1000
DCP_RECONNECT_BACKOFF_MAX_MS = 10000
DCP_FIRST_CMD_EXTRA_TIMEOUT_MS = 2000

DCP_POST_OPEN_QUIET_S = 0.8
DCP_DRAIN_TIMEOUT_S = 1.0

# ----------------------------------------------------------------------
# 스케일/스텝(프로토콜/보정값) - config로 이관 가능하지만 UI 노출은 비추천
# ----------------------------------------------------------------------
DCP_V_MEAS_V_PER_LSB = 1.468815
DCP_I_MEAS_A_PER_LSB = 0.01
DCP_P_MEAS_W_PER_LSB = 10.0

DCP_RAMP_MS_PER_LSB = 1.0
DCP_ARC_US_PER_LSB  = 1.0

DCP_V_SET_STEP_V = 1.0
DCP_I_SET_STEP_A = 0.1
DCP_P_SET_STEP_W = 10.0


# ======================================================================
# RF Pulse (공통 기본값)
# ======================================================================
RFPULSE_TCP_HOST = "192.168.1.50"
RFPULSE_TCP_PORT = 4005

RFPULSE_ADDR = 1

# 연결/워치독/재연결
RFPULSE_CONNECT_TIMEOUT_S = 1.5
RFPULSE_DRAIN_TIMEOUT_S = 2.0
RFPULSE_WATCHDOG_INTERVAL_MS = 3000
RFPULSE_RECONNECT_BACKOFF_START_MS = 2000
RFPULSE_RECONNECT_BACKOFF_MAX_MS = 30_000

# 폴링/모니터링(필요시 UI에서 조절)
POLL_INTERVAL_MS = 5_000
POLL_QUERY_TIMEOUT_MS = 9000
POLL_START_DELAY_AFTER_RF_ON_MS = 800

RFPULSE_PULSE_MODE = 1  # 0~5 중 사용(네 장비 프로토콜에 맞게)

RFPULSE_FORP_TOLERANCE_PERCENT = 5.0
RFPULSE_FORP_CONSECUTIVE_LIMIT = 3
RFPULSE_REFP_LIMIT_WATTS = 20.0
RFPULSE_REFP_CONSECUTIVE_LIMIT = 3


# ======================================================================
#  - 프로세스 컨트롤러 기능 지원 플래그 기본값
#  - 실제 장비 구성에 맞춰 채널 파일에서 오버라이드
# ======================================================================
SUPPORTS_DC = True
SUPPORTS_RF_CONT = False
SUPPORTS_RFPULSE = True


# ======================================================================
# Chamber Process 공통 기본값
#  - CH1 / CH2에서 동일하게 사용하는 공정 기본값
#  - 채널 파일에는 "다른 값만" 남긴다.
# ======================================================================
PC_PRESSURE_WAIT_TIMEOUT_S = 180.0        # WAIT_PRESSURE 최대 대기 시간(초)
PC_WORKING_PRESSURE_BOOST_TARGET = 10.0   # working_pressure가 이 값보다 작으면 SP2로 먼저 부스팅
PC_RF_PULSE_POST_ON_DELAY_MS = 20_000     # RF Pulse ON 직후 안정화 대기(ms)
PC_POWER_OFF_TIMEOUT_MS = 240_000         # shutdown 중 power off 토큰 대기 최소(ms)


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
TSP_ADDR = 0x80
TSP_CONNECT_TIMEOUT_S = 2.0
TSP_WRITE_TIMEOUT_S   = 1.2
TSP_POST_SEND_DELAY_MS = 20

# ✅ keepalive: 기존은 항상 ON이었으니 True로 맞춤
TSP_TCP_KEEPALIVE = True

# ✅ (추가) tsp.py 런타임 제어용
TSP_TOLERATE_SHORT_RESP = True         # 1바이트 ACK/NACK만 오는 구현 허용 여부
TSP_STATUS_POLL_INTERVAL_S = 0.05      # on/off verify 시 status 폴링 주기(초)
TSP_RS232_ADDR = 0x80                  # device/tsp.py의 RS232 고정주소(기본 0x80)

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
PLC_COIL_LOG_NAS_DIR = r"\\VanaM_NAS\VanaM_Sputter\Sputter\Logs\CH1&2\CH1&2_PLC"
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


# ======================================================================
# chamber_runtime.py 공통 런타임 기본값
# - 현재 chamber_runtime.py의 하드코딩 값을 그대로 이관
# - 동작 변경 없이, 나중에 UI/외부설정으로 조정하기 위한 준비
# ======================================================================

PROCESS_LIST_START_DIR = r"\\VanaM_NAS\VanaM_toShare"
SPUTTER_CALIB_DB_DIR = r"\\VanaM_NAS\VanaM_Sputter\Sputter\Calib\Database"

CHAMBER_OES_USB_INDEX_CH1 = 0
CHAMBER_OES_USB_INDEX_CH2 = 1

CHAMBER_RF_CONT_POLL_INTERVAL_MS = 1000
CHAMBER_RF_CONT_RAMPDOWN_INTERVAL_MS = 50
CHAMBER_RF_CONT_DIRECT_MODE = False
CHAMBER_RF_CONT_ZEROING = False
CHAMBER_RF_CONT_WRITE_INV_A = 1.6546
CHAMBER_RF_CONT_WRITE_INV_B = 2.6323

CHAMBER_GATE_RECHECK_COUNT = 5
CHAMBER_GATE_READ_TIMEOUT_S = 0.6
CHAMBER_GATE_RECHECK_INTERVAL_S = 0.2

CHAMBER_CHUCK_MOVE_TIMEOUT_S = 60.0
CHAMBER_CHUCK_POWER_ON_SETTLE_S = 0.2
CHAMBER_CHUCK_POLL_INTERVAL_S = 0.3

CHAMBER_PULSE_RECONNECT_TIMEOUT_S = 2.0
CHAMBER_PREFLIGHT_TIMEOUT_S = 8.0
CHAMBER_PREFLIGHT_TIMEOUT_WITH_PULSE_S = 10.0
CHAMBER_OES_INIT_TIMEOUT_S = 20.0
CHAMBER_HOST_START_WAIT_TIMEOUT_S = 10.0


# ======================================================================
# process_controller.py의 파라미터
# ======================================================================

PROCESS_GAS_INFO = {
    "AR": {"channel": 1},
    "O2": {"channel": 2},
    "N2": {"channel": 3},
}

PROCESS_DEFAULT_BASE_PRESSURE = 1e-5
PROCESS_DEFAULT_OES_INTEGRATION_MS = 60


# ======================================================================
# plasma_cleaning_runtime.py 공통 런타임 기본값
# - 현재 plasma_cleaning_runtime.py의 하드코딩 값을 그대로 이관
# - 동작 변경 없이, 나중에 UI/외부설정으로 조정하기 위한 준비
# ======================================================================

# Plasma Cleaning 기본 fallback 값
PC_DEFAULT_GAS_IDX = 3
PC_DEFAULT_GAS_FLOW_SCCM = 0.0
PC_DEFAULT_TARGET_PRESSURE_TORR = 5.0e-6
PC_DEFAULT_TOL_MTORR = 0.2
PC_DEFAULT_WAIT_TIMEOUT_S = 90.0
PC_DEFAULT_SP4_SETPOINT_MTORR = 2.0
PC_DEFAULT_RF_POWER_W = 100.0
PC_DEFAULT_PROCESS_TIME_MIN = 1.0
PC_DEFAULT_GV_OPEN_LAMP_DELAY_S = 5.0
PC_DEFAULT_IG_INTERVAL_MS = 10_000

# Plasma Cleaning 프리플라이트
PC_PREFLIGHT_TIMEOUT_S = 10.0
PC_PREFLIGHT_PLC_HANDSHAKE_TIMEOUT_S = 1.0
PC_PREFLIGHT_DEVICE_CONNECT_TIMEOUT_S = 3.0
PC_PREFLIGHT_CONNECT_POLL_INTERVAL_S = 0.5

# Plasma Cleaning RF (PLC DCV ch=1 경로)
PC_RF_POLL_INTERVAL_MS = 1000
PC_RF_RAMPDOWN_INTERVAL_MS = 50
PC_RF_DIRECT_MODE = True
PC_RF_WRITE_INV_A = 1.74
PC_RF_WRITE_INV_B = 0.0

PC_RF_TARGET_WAIT_TIMEOUT_S = 120.0  # 50W→300W@3W/s ≈ 83s + 여유 → 120s
PC_RF_TARGET_WAIT_EXTRA_S = 5.0
PC_RF_FAIL_REF_THRESHOLD_W = 20.0

# Plasma Cleaning RF kick+ramp (target > threshold 일 때)
PC_RF_RAMP_KICK_THRESHOLD_W = 100.0  # 이 값 초과 시 kick+ramp 방식 적용
PC_RF_RAMP_INITIAL_KICK_W   = 50.0   # 첫 kick 전송값(W)
PC_RF_RAMP_STEP             = 50.0   # ramp-up 증가량(W/poll, poll=1000ms → 50W/s)
PC_RF_RAMP_DOWN_STEP        = 1.0    # overshoot 시 ramp-down 감소량(W/poll → 1W/s)
PC_RF_RAMP_SHUTDOWN_CUT_W   = 50.0   # 종료 ramp-down 중 이 값 이하 도달 시 즉시 OFF(W)
PC_RF_RAMP_FINE_UP_STEP     = 3.0    # setpoint 도달 후 FWD 못 미칠 때 상승 단계(W)

# Plasma Cleaning 기타 timeout
PC_RF_CLEANUP_TIMEOUT_S = 5.0
PC_RF_WAIT_POWER_OFF_TIMEOUT_S = 15.0
PC_HOST_START_WAIT_TIMEOUT_S = 10.0

# ======================================================================
# Google Drive 공정 로그 저장 설정
# ======================================================================
GDRIVE_LOG_DIR       = "G:/공유 드라이브/VanaM_Sputter/Process_log"
GDRIVE_ARC_ALERT_THRESH = 5      # Arc 누적 이 값 이상 시 Chat 알림
GDRIVE_REF_P_WARN_W     = 20.0  # Reflected Power 경고 기준 (W)
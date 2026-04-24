# lib/config_ch1.py
from .config_common import *

# 채널 식별
CH_ID = 1
CH_NAME = "ch1"

# RGA CSV 저장 경로 (CH1)
RGA_CSV_PATH = r"\\VanaM_NAS\VanaM_Sputter\RGA\Ch.1\RGA_spectrums.csv"

# === 채널별 포트/프로그램/경로 오버라이드 ===
# IG / MFC
IG_TCP_PORT  = 4001
MFC_TCP_PORT = 4003

# ★ RF Pulse (CH1 전용) — HOST는 config_common의 192.168.1.50 사용
RFPULSE_TCP_PORT = 4007  # ← RS-232 허브 신규 포트 확정 후 여기만 수정

# ★ CH1 전용 스케일: 2번 가스(O2)만 1.0로 덮어쓰기
MFC_SCALE_FACTORS = {1: 1.0, 2: 1.0, 3: 1.0}

# process_controller.py의 파라미터
PROCESS_GUN_SHUTTERS = []
PROCESS_PRESSURE_CONTROL_SP_ON_CMD = "SP3_ON"
PROCESS_PRESSURE_CONTROL_SP_LABEL = "SP3"
PROCESS_PRESSURE_CONTROL_SP_INDEX = 3
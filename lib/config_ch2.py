# lib/config_ch2.py
from .config_common import *

# 채널 식별
CH_ID = 2
CH_NAME = "ch2"

# RGA CSV 저장 경로 (CH2)
RGA_CSV_PATH = r"G:\공유 드라이브\VanaM_Sputter\RGA\Ch.2\RGA_spectrums.csv"

# === 채널별 포트/프로그램/경로 오버라이드 ===
# IG / MFC
IG_TCP_PORT  = 4002
MFC_TCP_PORT = 4006


# ★ CH2 전용 스케일: 3번 가스(N2)는 UI(sccm) → 장비(%FS) ×2.0
MFC_SCALE_FACTORS = {1: 1.0, 2: 10.0, 3: 2.0}

# 컨트롤러 지원 플래그
SUPPORTS_RF_CONT = True

# process_controller.py의 파라미터
PROCESS_GUN_SHUTTERS = ["G1", "G2", "G3"]
PROCESS_PRESSURE_CONTROL_SP_ON_CMD = "SP4_ON"
PROCESS_PRESSURE_CONTROL_SP_LABEL = "SP4"
PROCESS_PRESSURE_CONTROL_SP_INDEX = 4
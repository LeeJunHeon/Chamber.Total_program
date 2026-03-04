# lib/config_ch2.py
from .config_common import *

# 채널 식별
CH_ID = 2
CH_NAME = "ch2"

# RGA CSV 저장 경로 (CH2)
RGA_CSV_PATH = r"\\VanaM_NAS\VanaM_Sputter\RGA\Ch.2\RGA_spectrums.csv"

# === 채널별 포트/프로그램/경로 오버라이드 ===
# IG / MFC
IG_TCP_PORT  = 4002
MFC_TCP_PORT = 4006

# ★ CH2 전용 스케일: 3번 가스(N2)는 UI(sccm) → 장비(%FS) ×2.0
MFC_SCALE_FACTORS = {1: 1.0, 2: 10.0, 3: 2.0}

# RGA LAN 접근 정보(필요 시 조정)
# RGA_NET.update({
#     "ch2": {"ip": "192.168.1.21", "user": "admin", "password": "admin"},
# })

# 컨트롤러 지원 플래그
SUPPORTS_DC = True
SUPPORTS_RF_CONT = True
SUPPORTS_RFPULSE = True


# ============================================================
# Process (공정 영향 파라미터)
# ============================================================
PC_PRESSURE_WAIT_TIMEOUT_S = 180.0        # WAIT_PRESSURE 최대 대기 시간(초)
PC_WORKING_PRESSURE_BOOST_TARGET = 10.0   # working_pressure가 이 값보다 작으면 SP2로 먼저 부스팅
PC_RF_PULSE_POST_ON_DELAY_MS = 20_000     # RF Pulse ON 직후 안정화 대기(ms)
PC_POWER_OFF_TIMEOUT_MS = 240_000         # shutdown 중 power off 토큰 대기 최소(ms)
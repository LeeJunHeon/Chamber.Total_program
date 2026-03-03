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
DCPULSE_TCP_HOST = "192.168.1.50"
DCPULSE_TCP_PORT = 4007

# ★ CH1 전용 스케일: 2번 가스(O2)만 1.0로 덮어쓰기
MFC_SCALE_FACTORS = {1: 1.0, 2: 1.0, 3: 1.0}

# RGA LAN 접근 정보(두 채널 모두 공통에 있지만, 필요 시 오버라이드 예시)
# RGA_NET.update({
#     "ch1": {"ip": "192.168.1.20", "user": "admin", "password": "admin"},
# })

# (선택) 컨트롤러 지원 플래그(장비 스펙에 맞춰 설정)
#  - CH1: RF 연속파 없음 / RF Pulse 사용 가능(동시 사용은 전역 락으로 차단)
SUPPORTS_DC = True
SUPPORTS_RF_CONT = False
SUPPORTS_RFPULSE = True

# ============================================================
# Process (공정 영향 파라미터)
# ============================================================
PC_PRESSURE_WAIT_TIMEOUT_S = 180.0        # WAIT_PRESSURE 최대 대기 시간(초)
PC_WORKING_PRESSURE_BOOST_TARGET = 10.0   # working_pressure가 이 값보다 작으면 SP2로 먼저 부스팅
PC_RF_PULSE_POST_ON_DELAY_MS = 20_000     # RF Pulse ON 직후 안정화 대기(ms)
PC_POWER_OFF_TIMEOUT_MS = 240_000         # shutdown 중 power off 토큰 대기 최소(ms)
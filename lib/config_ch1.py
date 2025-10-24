# lib/config_ch1.py
from pathlib import Path
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
MFC_SCALE_FACTORS = {1: 1.0, 2: 1.0, 3: 10.0}

# RGA LAN 접근 정보(두 채널 모두 공통에 있지만, 필요 시 오버라이드 예시)
RGA_NET.update({
    "ch1": {"ip": "192.168.1.20", "user": "admin", "password": "admin"},
})

# (선택) 컨트롤러 지원 플래그(장비 스펙에 맞춰 설정)
#  - CH1: RF 연속파 없음, RF Pulse 없음 (예시) / DC만 사용한다면 아래처럼
SUPPORTS_DC = True
SUPPORTS_RF_CONT = False
SUPPORTS_RFPULSE = False

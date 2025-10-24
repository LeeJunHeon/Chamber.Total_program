# lib/config_ch2.py
from pathlib import Path
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
RFPULSE_TCP_HOST = "192.168.1.50"
RFPULSE_TCP_PORT = 4005

# ★ CH2 전용 스케일: 3번 가스(N2)는 UI(sccm) → 장비(%FS) ×2.0
MFC_SCALE_FACTORS = {1: 1.0, 2: 10.0, 3: 2.0}

# RGA LAN 접근 정보(필요 시 조정)
RGA_NET.update({
    "ch2": {"ip": "192.168.1.21", "user": "admin", "password": "admin"},
})

# (선택) 컨트롤러 지원 플래그(장비 스펙에 맞춰 설정)
#  - CH2: RF Pulse 지원, RF 연속파는 미지원(예시)
SUPPORTS_DC = True
SUPPORTS_RF_CONT = False
SUPPORTS_RFPULSE = True

RFPULSE_WATCHDOG_INTERVAL_MS        = 1000
RFPULSE_RECONNECT_BACKOFF_START_MS  = 1000
RFPULSE_RECONNECT_BACKOFF_MAX_MS    = 20_000

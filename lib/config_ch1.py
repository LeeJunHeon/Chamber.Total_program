# lib/config_ch1.py
from pathlib import Path
from .config_common import *

# 채널 식별
CH_ID = 1
CH_NAME = "ch1"

# === 채널별 포트/프로그램/경로 오버라이드 ===
# IG / MFC
IG_TCP_PORT  = 4001
MFC_TCP_PORT = 4003

# RGA LAN 접근 정보(두 채널 모두 공통에 있지만, 필요 시 오버라이드 예시)
RGA_NET.update({
    "ch1": {"ip": "192.168.1.20", "user": "admin", "password": "admin"},
})

# (선택) 펄스 장비 주소(필요하면 조정)
RFPULSE_PORT = "192.168.1.50:4005"
DCPULSE_PORT = "192.168.1.50:4007"

# (선택) 컨트롤러 지원 플래그(장비 스펙에 맞춰 설정)
#  - CH1: RF 연속파 없음, RF Pulse 없음 (예시) / DC만 사용한다면 아래처럼
SUPPORTS_DC = True
SUPPORTS_RF_CONT = False
SUPPORTS_RFPULSE = False

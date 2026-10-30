"""RF Pulse(AE CESAR, AE Bus) 파라미터 — v2/ch12/drivers/rfpulse.py·pulse_port.py 가 읽는다.

v1 RFPulseAsync 는 채널 cfg(config_ch1/ch2) → config_common 순서(device/rf_pulse.py:229-236). 채널 모듈은 json 적용 전에
config_common 의 .py 값을 복사하므로 default 는 .py 값(지금 settings.json common 값과 같다). ch 구역 값은 CH1_/CH2_ 키.
CH1 RF Pulse 포트 4007 은 lib/config_ch1.py:17 에만 있다(settings common 4005 는 CH1 모듈에 가지 않는다 — inv_cfg CFG-555).
RFPULSE_REFP_WARN_WATTS 는 CH1 0.0(꺼짐 — .py 기본값), CH2 5.0(settings.json:641).
"""
from __future__ import annotations

from ._base import B, F, H, I, L, S, Dropped, inst, p

_G = "rfpulse"
_CC = "lib/config_common.py"

PARAMS: tuple = (
    inst("RFPULSE_TCP_HOST", _G, "RF Pulse NPort 주소", S, "192.168.1.50", pattern=r"[0-9.]+", safety=H, src=f"{_CC}:414"),
    inst("RFPULSE_TCP_PORT", _G, "RF Pulse TCP 포트(공통 기본)", I, 4005, 1, 65535, safety=H, src=f"{_CC}:415",
         desc="채널 키가 있으면 그 값(CH1 4007·CH2 4005)"),
    inst("CH1_RFPULSE_TCP_PORT", _G, "CH1 RF Pulse TCP 포트", I, 4007, 1, 65535, safety=H, src="lib/config_ch1.py:17",
         v1_key="ch1.RFPULSE_TCP_PORT", desc="DC Pulse 와 같은 포트(pulse_port 가 점유를 가린다)"),
    inst("CH2_RFPULSE_TCP_PORT", _G, "CH2 RF Pulse TCP 포트", I, 4005, 1, 65535, safety=H, src="config/settings.json:566"),
    inst("RFPULSE_ADDR", _G, "AE Bus 주소", I, 1, 1, 31, safety=H, src=f"{_CC}:417", desc="0 은 브로드캐스트(응답 없음)"),
    inst("RFPULSE_CONNECT_TIMEOUT_S", _G, "연결 제한", F, 1.5, 0.3, 30.0, unit="s", src=f"{_CC}:420 · device/rf_pulse.py:1046"),
    p("CMD_GAP_MS", _G, "명령 간격(앞 명령 끝난 뒤 쉼)", "ms", I, 1500, 0, 10000, safety=H, src=f"{_CC}:84 · device/rf_pulse.py:1249"),
    p("ACK_TIMEOUT_MS", _G, "설정 명령 응답 한도", "ms", I, 2000, 100, 60000, src=f"{_CC}:81", desc="RF ON/OFF 는 max(이 값, 2500)"),
    p("QUERY_TIMEOUT_MS", _G, "질의 응답 한도", "ms", I, 4500, 100, 60000, src=f"{_CC}:82"),
    p("POLL_QUERY_TIMEOUT_MS", _G, "폴링 질의 응답 한도", "ms", I, 9000, 100, 60000, src=f"{_CC}:432",
      desc="1초 읽기에서는 tick 마감이 더 짧아 그 안에서 끝난다"),
    p("POLL_INTERVAL_MS", _G, "폴링 바퀴 사이 쉼", "ms", I, 5000, 0, 600000, src=f"{_CC}:431 · device/rf_pulse.py:1497",
      desc="162→165→166 한 바퀴 뒤 쉼 — FORP/REFP 판정 주기(v1 그대로)"),
    p("POLL_START_DELAY_AFTER_RF_ON_MS", _G, "RF ON 뒤 첫 폴링까지", "ms", I, 800, 0, 60000, src=f"{_CC}:433"),
    p("RFPULSE_RECONNECT_BACKOFF_START_MS", _G, "재연결 시작 간격", "ms", I, 2000, 200, 600000, safety=L, src=f"{_CC}:427"),
    p("RFPULSE_RECONNECT_BACKOFF_MAX_MS", _G, "재연결 최대 간격", "ms", I, 30000, 200, 600000, safety=L, src=f"{_CC}:428"),
    p("RFPULSE_PULSE_MODE", _G, "펄싱 모드(명령 27)", "", I, 1, 0, 5, safety=H, src=f"{_CC}:435",
      desc="0 끔·1 내부·2 외부·3 외부 반전·4 게이트 내부·5 게이트 내부 반전"),
    p("RFPULSE_FORP_TOLERANCE_PERCENT", _G, "순방향 허용 오차", "%", F, 5.0, 0.0, 100.0, safety=H, src=f"{_CC}:437"),
    p("RFPULSE_FORP_CONSECUTIVE_LIMIT", _G, "순방향 이탈 연속 횟수", "회", I, 3, 1, 100, safety=H, src=f"{_CC}:438"),
    p("RFPULSE_REFP_LIMIT_WATTS", _G, "반사파 상한", "W", F, 20.0, 0.0, 1000.0, safety=H, src=f"{_CC}:439"),
    p("RFPULSE_REFP_CONSECUTIVE_LIMIT", _G, "반사파 초과 연속 횟수", "회", I, 3, 1, 100, safety=H, src=f"{_CC}:440"),
    p("RFPULSE_REFP_WARN_WATTS", _G, "반사파 경고(문구만, CH1)", "W", F, 0.0, 0.0, 1000.0, src=f"{_CC}:441", desc="0 = 끔"),
    p("CH2_RFPULSE_REFP_WARN_WATTS", _G, "CH2 반사파 경고(문구만)", "W", F, 5.0, 0.0, 1000.0, src="config/settings.json:641"),
    p("RFPULSE_FREQ_MAX_HZ", _G, "펄스 주파수 상한(레시피 검사)", "Hz", I, 30000, 1, 100000, safety=H, src=f"{_CC}:379",
      desc="사양 3-5 1 Hz~30 kHz(넘으면 W40 출력 차단) — 공정 엔진이 시작 전에 검사(runtime/chamber_runtime.py:3185)"),
    p("CH1_RFPULSE_FREQ_MAX_HZ", _G, "CH1 펄스 주파수 상한", "Hz", I, 30000, 1, 100000, safety=H, src="config/settings.json:481"),
    p("CH2_RFPULSE_FREQ_MAX_HZ", _G, "CH2 펄스 주파수 상한", "Hz", I, 30000, 1, 100000, safety=H, src="config/settings.json:550"),
    p("RFPULSE_RAW_LOG", _G, "원시 프레임 문구(폴링 포함)", "", B, False, safety=L, src=f"{_CC}:425",
      desc="false 면 '[POLL' 폴링 프레임만 생략 — 설정·제어 명령과 NAK·체크섬 오류는 늘 남김"),
    p("RFPULSE_OFF_DEADLINE_S", _G, "확인형 RF OFF 최대 시간", "s", F, 20.0, 1.0, 300.0, safety=H, src=f"{_CC}:422"),
    p("RFPULSE_UNUSED_OFF_WAIT_S", _G, "켠 적 없는 출력 RF OFF 1회 대기", "s", F, 3.0, 0.5, 60.0, src=f"{_CC}:424"),
    p("RFPULSE_CLEANUP_OFF_WAIT_S", _G, "정리 전 OFF 확인 대기", "s", F, 8.0, 0.0, 60.0, src=f"{_CC}:423",
      desc="공정 엔진이 끝 정리 때 확인형 OFF 를 기다리는 한도(v1 cleanup)"),
)

_UNUSED = "v1 코드에서 읽는 곳 없음(설정 로더의 이름·형식 검사만)"
DROPPED: tuple = (
    Dropped("common.RFP_TCP_KEEPALIVE", "v1 RF Pulse 는 이 값과 무관하게 SO_KEEPALIVE 1 — v2 도 1 고정", "device/rf_pulse.py:1057-1063"),
    Dropped("common.RFP_INACTIVITY_REOPEN_S", _UNUSED + "(값 0)", f"{_CC}:64"),
    Dropped("common.RECV_FRAME_TIMEOUT_MS", _UNUSED, f"{_CC}:83"),
    Dropped("common.POST_WRITE_DELAY_MS", _UNUSED, f"{_CC}:85"),
    Dropped("common.ACK_FOLLOWUP_GRACE_MS", _UNUSED, f"{_CC}:86"),
    Dropped("common.RFPULSE_DRAIN_TIMEOUT_S", "asyncio writer.drain 한도 — v2 TcpTransport 는 sendall", "device/rf_pulse.py:1209"),
    Dropped("common.RFPULSE_WATCHDOG_INTERVAL_MS", "v1 워치독 루프의 연결 확인 주기 — v2 는 worker 가 끊김을 즉시 안다", "device/rf_pulse.py:1033"),
)

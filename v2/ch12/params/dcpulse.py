"""DC Pulse(EnerPulse EP5) 파라미터 — v2/ch12/drivers/dcpulse.py·dcpulse_ops.py·dcpulse_monitor.py·pulse_port.py 가 읽는다.

v1 AsyncDCPulse 는 채널 cfg(config_ch1/ch2) → config_common 순서로 읽는다(device/dc_pulse.py:392-405). 채널 모듈은
config_common 의 .py 값을 json 적용 전에 복사해 갖고 있어 common 구역 값은 채널에 넘어가지 않는다(inv_cfg ⚠) — 그래서 default
는 .py 값이고(지금 settings.json common 값과 같다), ch1/ch2 구역에 있는 키는 CH1_/CH2_ 키로 따로 둔다(드라이버는 채널 키 먼저).
예외: DC_OVERCURRENT_* 는 config_common(json 적용 뒤)에서만 읽는다(dc_pulse.py:404-411) → DC_OVERCURRENT_ENABLE 운영값 false.
과전류 한계 P1·I1·P2·I2·STREAK·EXTRAPOLATE 는 DC 연속과 같이 쓰므로 params/dc.py 에 있다.
"""
from __future__ import annotations

from ._base import B, F, H, I, L, S, Dropped, inst, p

_G = "dcpulse"
_CC = "lib/config_common.py"


def _arc(prefix: str, sec: str) -> tuple:
    """ch1/ch2 구역에 같은 값으로 들어 있는 아크·HOST 키(config/settings.json:472-479 ch1, 541-548 ch2)."""
    src = f"config/settings.json {sec} · {_CC}:367-376"
    return (
        p(f"{prefix}DCP_REQUIRE_HOST_MODE", _G, f"{sec} HOST 모드 아니면 시작 거부", "", B, True, safety=H, src=src),
        p(f"{prefix}DCP_ARC_ALERT_RATE_PER_S", _G, f"{sec} 아크율 경고 기준", "/s", F, 20.0, 0.0, 100000.0, src=src),
        p(f"{prefix}DCP_ARC_ALERT_RATE_N", _G, f"{sec} 아크율 연속 횟수", "회", I, 2, 0, 100, src=src),
        p(f"{prefix}DCP_ARC_ALERT_RUN_TOTAL", _G, f"{sec} (사용 안 함) 런 누적 경고", "개", I, 1000, 0, 10 ** 9, safety=L, src=src,
          desc="v1 에서 폐지된 기준 — 경고 데이터의 run_total_limit 표시에만"),
        p(f"{prefix}DCP_ARC_IGN_WINDOW_S", _G, f"{sec} 점화 구간", "s", F, 30.0, 0.0, 600.0, src=src),
        p(f"{prefix}DCP_ARC_FAST_INTERVAL_S", _G, f"{sec} 점화 구간 판정 주기", "s", F, 1.0, 0.2, 60.0, src=src),
        p(f"{prefix}DCP_ARC_IGN_WARN", _G, f"{sec} 점화 구간 아크 경고", "개", I, 3000, 0, 10 ** 9, src=src),
        p(f"{prefix}DCP_ARC_DEPO_WARN", _G, f"{sec} 증착 구간 아크 경고", "개", I, 500, 0, 10 ** 9, src=src),
    )


PARAMS: tuple = (
    inst("DCPULSE_TCP_HOST", _G, "DC Pulse NPort 주소", S, "192.168.1.50", pattern=r"[0-9.]+", safety=H, src=f"{_CC}:343"),
    inst("DCPULSE_TCP_PORT", _G, "DC Pulse TCP 포트(CH1)", I, 4007, 1, 65535, safety=H, src=f"{_CC}:344",
         desc="CH1 은 RF Pulse 와 같은 4007(pulse_port 가 점유를 가린다)"),
    inst("CH2_DCPULSE_TCP_PORT", _G, "CH2 DC Pulse TCP 포트", I, 4005, 1, 65535, safety=H, src="config/settings.json:567",
         desc="MOXA Port 5 — RF Pulse 와 케이블을 바꿔 씀(Parity None + Null Modem)"),
    inst("DCP_CONNECT_TIMEOUT_S", _G, "연결 제한", F, 3.0, 0.5, 30.0, unit="s", src=f"{_CC}:380 · device/dc_pulse.py:484"),
    inst("DCP_TCP_KEEPALIVE", _G, "SO_KEEPALIVE", B, False, src=f"{_CC}:70 · device/dc_pulse.py:2161-2169"),
    p("DCP_GAP_MS", _G, "명령 간격", "ms", I, 150, 0, 5000, safety=H, src=f"{_CC}:383",
      desc="보낸 시각 기준 최소 간격(매뉴얼 권장 100 ms 이상)"),
    p("DCP_TIMEOUT_MS", _G, "응답 한도(+2 s)", "ms", I, 2500, 200, 60000, src=f"{_CC}:382 · device/dc_pulse.py:2262"),
    p("DCP_FIRST_CMD_EXTRA_TIMEOUT_MS", _G, "연결 직후 2 s 안 명령 추가 한도", "ms", I, 2000, 0, 60000, src=f"{_CC}:389"),
    p("DCP_POST_OPEN_QUIET_S", _G, "연결 직후 조용히", "s", F, 0.8, 0.0, 10.0, src=f"{_CC}:391"),
    p("DCP_RECONNECT_BACKOFF_START_MS", _G, "재연결 시작 간격", "ms", I, 1000, 200, 600000, safety=L, src=f"{_CC}:387"),
    p("DCP_RECONNECT_BACKOFF_MAX_MS", _G, "재연결 최대 간격", "ms", I, 10000, 200, 600000, safety=L, src=f"{_CC}:388"),
    p("DCP_RECOVER_MAX_ATTEMPTS", _G, "쓰기 시도 횟수(복구 포함)", "회", I, 5, 1, 20, src=f"{_CC}:359 · device/dc_pulse.py:1815"),
    p("DCP_ENABLE_FAULT_RECOVER", _G, "쓰기 실패 시 fault 확인·리셋", "", B, True, src=f"{_CC}:361"),
    p("DCP_MAX_POWER_W", _G, "최대 출력", "W", F, 1000.0, 0.0, 5000.0, safety=H, src=f"{_CC}:349 · device/dc_pulse.py:416"),
    p("DCP_P_SET_STEP_W", _G, "전력 설정 단위", "W", F, 10.0, 0.1, 1000.0, safety=H, src=f"{_CC}:408",
      desc="0x83 raw = round(W / 단위), 0 ~ 최대출력 // 단위"),
    p("DCP_P_MEAS_W_PER_LSB", _G, "PIV 전력 LSB", "W", F, 10.0, 0.0, 1000.0, safety=H, src=f"{_CC}:401"),
    p("DCP_I_MEAS_A_PER_LSB", _G, "PIV 전류 LSB", "A", F, 0.01, 0.0, 10.0, safety=H, src=f"{_CC}:400"),
    p("DCP_V_MEAS_V_PER_LSB", _G, "PIV 전압 LSB", "V", F, 1.0, 0.0, 100.0, safety=H, src=f"{_CC}:399"),
    p("DCP_ACTIVATION_CHECK_DELAY_S", _G, "출력 ON 직후 활성화 대기", "s", F, 5.0, 0.0, 60.0, safety=H, src=f"{_CC}:363"),
    p("DCP_POLL_INTERVAL_S", _G, "출력 중 판정 주기(점화 창 밖)", "s", F, 5.0, 0.1, 60.0, safety=H, src=f"{_CC}:364",
      desc="값은 1초마다 읽지만 연속 N회 판정은 이 주기의 표본으로만 센다(v1 폴링 주기)"),
    p("DCP_P_SET_TOL_PCT", _G, "세트포인트 허용(비율)", "", F, 0.05, 0.0, 1.0, safety=H, src=f"{_CC}:351"),
    p("DCP_P_SET_TOL_W", _G, "세트포인트 허용(W)", "W", F, 15.0, 0.0, 1000.0, safety=H, src=f"{_CC}:352"),
    p("DCP_P_SET_DEVIATE_MAX_N", _G, "세트포인트 이탈 연속 횟수", "회", I, 3, 1, 100, safety=H, src=f"{_CC}:353", v1_period=5.0),
    p("DCP_I_LOW_THRESH_A", _G, "저전류 기준", "A", F, 0.05, 0.0, 100.0, safety=H, src=f"{_CC}:355"),
    p("DCP_I_LOW_COUNT_MAX_N", _G, "저전류 연속 횟수", "회", I, 3, 1, 100, safety=H, src=f"{_CC}:356", v1_period=5.0),
    p("DC_OVERCURRENT_ENABLE", _G, "DC Pulse 과전류 인터락", "", B, False, safety=H, v1_default=True,
      src="config/settings.json:122 · device/dc_pulse.py:2488", desc="장비 배포값 false(2026-10-15) — config_common 에서만 읽음"),
    p("DC_OVERCURRENT_ARM_ON_START", _G, "출력 ON 직후 활성화 대기 중 과전류 감시", "", B, True, safety=H,
      src="config/settings.json:130 · device/dc_pulse.py:2545"),
    p("DC_OVERCURRENT_ARM_INTERVAL_S", _G, "활성화 대기 중 확인 주기", "s", F, 0.5, 0.05, 10.0, safety=H,
      src="config/settings.json:131 · device/dc_pulse.py:2546"),
    p("DCP_FREQ_SETTLE_MS", _G, "주파수 변경 뒤 Off Time 쓰기 전 대기", "ms", I, 500, 0, 10000, src=f"{_CC}:384 · device/dc_pulse.py:868"),
    p("DCP_UNUSED_OFF_WAIT_S", _G, "켠 적 없는 출력 OFF 1회 대기", "s", F, 3.0, 0.5, 60.0, src=f"{_CC}:556 · device/dc_pulse.py:1281"),
    p("DCP_CLEANUP_OFF_WAIT_S", _G, "정리 전 OFF 확인 대기", "s", F, 8.0, 0.0, 60.0, src=f"{_CC}:555 · device/dc_pulse.py:569",
      desc="공정 엔진이 끝 정리 때 확인형 OFF 를 기다리는 한도(v1 cleanup)"),
    *_arc("", "common"),
    *_arc("CH1_", "ch1"),
    *_arc("CH2_", "ch2"),
)

_UNUSED = "v1 코드에서 읽는 곳 없음(설정 로더의 이름·형식 검사만)"
_DEAD = "v1 에서 읽지만 그 값을 쓰는 함수가 어디서도 불리지 않음"
DROPPED: tuple = (
    Dropped("common.DCPULSE_PORT", _UNUSED + " — 레거시 'host:port' 문자열(DCPULSE_TCP_HOST/PORT 를 씀)", f"{_CC}:337"),
    Dropped("common.DCPULSE_BAUD", _UNUSED + " — 시리얼 설정은 NPort 쪽(미확인)", f"{_CC}:338"),
    Dropped("common.DCPULSE_ADDR", _UNUSED + " — RS-232 프레임에는 주소가 없다(RS-485 전용)", f"{_CC}:339"),
    Dropped("common.DCPULSE_DEFAULT_DELAY_MS", _UNUSED, f"{_CC}:340"),
    Dropped("common.DCP_CMD_MAX_RETRIES", _DEAD + "(_write_simple)", "device/dc_pulse.py:2079-2097"),
    Dropped("common.DCP_WRITE_WORKER_RETRIES", "v1 값 0(쓰기는 1회 전송 — 재시도는 DCP_RECOVER_MAX_ATTEMPTS) — v2 는 0 고정",
            "device/dc_pulse.py:478"),
    Dropped("common.DCP_WATCHDOG_INTERVAL_MS", "v1 워치독 루프의 연결 확인 주기 — v2 는 worker 가 끊김을 즉시 안다", "device/dc_pulse.py:2144"),
    Dropped("common.DCP_DRAIN_TIMEOUT_S", "asyncio writer.drain 한도 — v2 TcpTransport 는 sendall", "device/dc_pulse.py:2269"),
    Dropped("common.DCP_INACTIVITY_REOPEN_S", "v1 값 0(무통신 재연결 끔) — v2 도 하지 않음", "device/dc_pulse.py:3164-3170"),
    Dropped("common.DCP_RAMP_MS_PER_LSB", _DEAD + "(set_ramp_and_ignition)", "device/dc_pulse.py:1439-1441"),
    Dropped("common.DCP_ARC_US_PER_LSB", _DEAD + "(set_arc_params)", "device/dc_pulse.py:1417-1423"),
    Dropped("common.DCP_V_SET_STEP_V", _DEAD + "(set_reference V·set_limits·set_arc_params)", "device/dc_pulse.py:1185-1194"),
    Dropped("common.DCP_I_SET_STEP_A", _DEAD + "(set_reference I·set_limits·set_arc_params)", "device/dc_pulse.py:1185-1194"),
)

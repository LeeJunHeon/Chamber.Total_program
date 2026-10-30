"""PLC(Modbus TCP 직결) 파라미터 — v2/ch12/drivers/plc.py·plc_io.py 가 쓰는 키. 기본값 = v1 운영값(settings.json common).

v1 은 AsyncPLC 가 connect() 직전마다 config_common 값으로 cfg 를 덮었다(device/plc.py:517-580) — 같은 키를 그대로 쓴다.
코일 CSV 로거의 주기·폴더(PLC_COIL_LOG_INTERVAL_S·SUMMARY_S·NAS_DIR·LOCAL_DIR)는 outputs 조각.
"""
from __future__ import annotations

from ._base import IPV4, H, I, F, B, L, LIVE, M, S, Dropped, inst, p

_G = "plc"

PARAMS: tuple = (
    inst("PLC_TCP_HOST", _G, "PLC IP 주소", S, "192.168.1.2", pattern=IPV4, safety=H,
         src="lib/config_common.py:612 · device/plc.py:523", desc="PLC Modbus TCP 주소(MOXA 미경유 직결)"),
    inst("PLC_TCP_PORT", _G, "PLC Modbus 포트", I, 502, 1, 65535, safety=H,
         src="lib/config_common.py:613 · device/plc.py:524"),
    inst("PLC_UNIT", _G, "PLC Unit ID", I, 1, 0, 255, safety=H, src="lib/config_common.py:614 · device/plc.py:525"),
    inst("PLC_TIMEOUT_S", _G, "PLC 응답 타임아웃", F, 2.0, 0.1, 30.0, unit="s",
         src="lib/config_common.py:616 · device/plc.py:526",
         desc="원시 연산 1회 응답 제한·접속 제한(pymodbus timeout, retries=0). 1초 묶음 읽기는 tick 마감(900 ms)이 더 이르면 그것"),
    inst("PLC_CMD_GAP_MS", _G, "PLC 명령 사이 최소 간격", I, 150, 0, 2000, unit="ms",
         src="lib/config_common.py:617 · device/plc.py:528-530 · device/plc.py:838-844",
         desc="원시 연산 시작 사이 최소 간격 — 1초 읽기·쓰기·하트비트 모두"),
    p("PLC_DISCONNECT_ALERT_AFTER_S", _G, "PLC 끊김 알림 대기", "s", F, 0.0, 0.0, 3600.0, safety=L, apply=LIVE,
      src="lib/config_common.py:630 · device/plc.py:541",
      desc="하트비트 실패 뒤 conn_change(끊김) 를 내기까지. 0 = 첫 실패 즉시"),
    inst("PLC_WATCHDOG_INTERVAL_S", _G, "PLC 하트비트 기준 주기", F, 8.0, 1.0, 60.0, unit="s",
         src="lib/config_common.py:621 · device/plc.py:533 · device/plc.py:1125",
         desc="실제 하트비트 주기 = max(1, 이 값 × 0.75) = 6 s(PLC FEnet 수신 타임아웃 15 s 안에 2회)"),
    inst("PLC_RECONNECT_RETRY", _G, "PLC 백그라운드 접속 재시도", I, 2, 0, 10,
         src="lib/config_common.py:623 · device/plc.py:536", desc="백그라운드 접속 시도 = 이 값 + 1 회"),
    inst("PLC_RECONNECT_DELAY_S", _G, "PLC 접속 시도 사이 대기", F, 0.5, 0.0, 10.0, unit="s",
         src="lib/config_common.py:624 · device/plc.py:537"),
    p("PLC_CMD_PULSE_MS", _G, "PLC 순간 스위치 펄스 폭", "ms", I, 180, 30, 5000, safety=M,
      src="lib/config_common.py:637 · device/plc.py:545 · device/plc.py:1633-1637",
      desc="momentary: ON → max(30 ms, 이 값) → OFF"),
    p("PLC_LOCK_WARN_MS", _G, "PLC 요청 대기 경고", "ms", F, 2500.0, 0.0, 60000.0, safety=L, apply=LIVE,
      src="lib/config_common.py:639 · device/plc.py:2286-2290",
      desc="요청이 큐에서 이만큼 넘게 기다리면 'WARN lock-wait' 로그(v1 락 대기)"),
    p("PLC_IO_WARN_MS", _G, "PLC 요청 실행 시간 경고", "ms", F, 2500.0, 0.0, 60000.0, safety=L, apply=LIVE,
      src="lib/config_common.py:640 · device/plc.py:2296-2301",
      desc="요청 실행이 이만큼 넘게 걸리면 'WARN in-lock IO' 로그"),
    p("PLC_COIL_LOG_BLOCK_PLAN", _G, "코일 읽기 시작 블록 계획", "", I, 1, 0, 3, safety=L,
      v1_default=0, src="config/settings.json:267 · device/plc.py:599-607",
      desc="(max_gap, max_span) 계획 0=(2000,2000) 1=(256,512) 2=(64,256) 3=(8,64). E403 이면 강등, 연속 성공이면 이 값까지 승격"),
    p("PLC_COIL_LOG_BACKOFF_AFTER", _G, "코일 읽기 자기 억제 기준", "회", I, 2, 0, 100, safety=L,
      src="lib/config_common.py:668 · device/plc.py:1976-1985",
      desc="1초 읽기의 low 연속 실패가 이 횟수 이상이거나 high 연속 타임아웃 > 0 이면 그 tick 코일 블록을 건너뜀(0=안 함)"),
    p("PLC_COIL_LOG_PROMOTE_AFTER", _G, "코일 블록 계획 승격 기준", "회", I, 12, 0, 1000, safety=L,
      src="lib/config_common.py:669 · device/plc.py:819-825", desc="연속 완전 성공 이 횟수마다 한 단계 승격(0=안 함)"),
    inst("PLC_RECONNECT_BACKOFF_S", _G, "PLC 재연결 백오프", F, 5.0, 0.0, 300.0, unit="s",
         src="lib/config_common.py:670 · device/plc.py:811-817", desc="접속 실패 뒤 이 시간 동안 다시 접속하지 않음(0=안 씀)"),
    inst("PLC_TIMEOUT_CLOSE_AFTER", _G, "소켓 재생성 연속 타임아웃 수", I, 3, 1, 20,
         src="lib/config_common.py:671 · device/plc.py:1336-1350",
         desc="연속 무응답이 이 횟수에 닿아야 소켓을 닫고 다시 연다(이 PLC 는 닫으면 1.5~3분 SYN 무응답)"),
    inst("PLC_FG_CONNECT_ATTEMPTS", _G, "명령 경로 접속 시도", I, 1, 1, 10,
         src="lib/config_common.py:672 · device/plc.py:874-879", desc="소켓 재생성 때 접속 시도 횟수"),
    p("PLC_DIAG_PROBE", _G, "접속 실패 원인 진단", "", B, True, safety=L, src="lib/config_common.py:673 · device/plc.py:1020",
      desc="접속 최종 실패 때 connect_ex·ping 진단(조립이 진단 함수를 넣을 때만)"),
    p("PLC_DIAG_MIN_INTERVAL_S", _G, "진단 최소 간격", "s", F, 60.0, 1.0, 3600.0, safety=L,
      src="lib/config_common.py:674 · device/plc.py:1008"),
)

DROPPED: tuple = (
    Dropped("common.PLC_COIL_LOG_BUDGET_S", "v1 코일 스냅샷 총 예산(4 s, 우선순위 양보) — v2 는 1초 읽기 마감(900 ms)과 worker "
            "우선순위 큐가 대신한다(읽기는 쓰기·하트비트 뒤로 밀림)", "device/plc.py:1478-1494"),
)

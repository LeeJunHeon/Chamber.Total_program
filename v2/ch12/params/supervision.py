"""감시 프로그램(멈춤 감지 재시작)·장비 응답 시간 기록 설정 — 2026-11-13 D408·D409(사용자 지시 3·4장).

감시 프로그램(v2/supervisor)은 제어 프로그램이 켜 둔 건강 확인 파일(heartbeat.json)에서 아래 값을 읽는다 — 설정을 바꾸면 다음 확인부터 적용(LIVE).
감시 프로그램은 범위를 따로 검사하지 않는다(파일의 값은 설정 적용·읽기 검사(D11)를 지난 값, 명령줄 --hang-timeout·--health-interval 은 시험용).

멈춤 판단 한도의 범위(2026-11-16 D413 — D408 보완): 명령 처리 번호는 명령 코어 잠금(Ch12Core._lock)을 0.5 s 안에 잡을 때만 오른다.
잠금 안에서 정상으로 가장 오래 기다리는 것은 화면 레시피 읽기(RECIPE_LOAD path)·PC CSV 읽기(PC_START·PC_RECIPE_LOAD path) — NAS 응답을
최대 30 s(core.UI_RECIPE_LOAD_TIMEOUT_S) 기다린다. 한도 하한 60 s 는 그 2배. 히터 패널 명령(HEATER_ONOFF·APPLY·RESET)은 히터 엔진 잠금을
PLC 명령 실행 상한(cmd_grace_s — 기본 10.5 s)까지 기다릴 수 있어 키 사이 규칙 SUP01(한도 ≥ 그 2배)로 막는다.
확인 주기 상한 30 s 는 한도 하한의 절반 — 주기가 늘 한도의 절반 이하라 두 값을 비교하는 규칙은 두지 않는다.
호스트 START_SPUTTER 의 CSV 30 s·시작 대기 10 s 는 로봇 명령(잠금 밖)이라 명령 처리 번호를 멈추지 않는다.
"""
from __future__ import annotations

from v2.ch12.drivers.plc_conv import cmd_grace_s

from ._base import B, F, LIVE, L, M, p, rule, need

_G = "supervisor"
_D = "v2(사용자 지시 2026-11-13"

PARAMS: tuple = (
    p("SUPERVISOR_HANG_ENABLED", _G, "멈춤 감지 재시작 사용", "", B, True, safety=M, apply=LIVE, src=f"{_D} 3장 D408)",
      desc="켜면 감시 프로그램이 제어 프로그램의 핵심 루프(1초 tick 번호·명령 처리)가 SUPERVISOR_HANG_TIMEOUT_S 동안 진행이 없을 때 "
           "스레드 덤프를 남기고 강제 종료 → 다시 켠다(재시작 한도에 포함, 사유 '멈춤'). 끄면 꺼질 때만 재시작(이전 동작)"),
    p("SUPERVISOR_HEALTH_INTERVAL_S", _G, "건강 확인 주기", "s", F, 10.0, 1.0, 30.0, safety=L, apply=LIVE, src=f"{_D} 3장 D408·D413)",
      desc="감시 프로그램이 건강 확인 파일을 보는 주기. 멈춤 판단은 이 주기 단위로(실제 감지 시각 ≤ 한도 + 이 값). "
           "상한 30 s = 멈춤 판단 한도 하한(60 s)의 절반 — 주기가 늘 한도의 절반 이하(D413)"),
    p("SUPERVISOR_HANG_TIMEOUT_S", _G, "멈춤 판단 한도", "s", F, 60.0, 60.0, 3600.0, safety=M, apply=LIVE, src=f"{_D} 3장 D408·D413)",
      desc="1초 tick 번호 또는 명령 처리(코어 잠금 확인)가 이 시간 동안 진행이 없으면 멈춘 것으로 본다. 정상 때는 1 s 마다 움직인다. "
           "하한 60 s: 화면 레시피 읽기·PC CSV 읽기가 NAS 응답을 최대 30 s 기다리는 동안 코어 잠금을 잡아 명령 처리 번호가 멈춘다(그 2배 — D413). "
           "히터 패널 명령은 PLC 명령 실행 상한(기본 10.5 s)까지 잠금을 잡을 수 있어 그 2배 이상이어야 한다(키 사이 규칙 SUP01)"),
    p("SUPERVISOR_START_GRACE_S", _G, "시작 준비 시간(멈춤 판단 제외)", "s", F, 300.0, 30.0, 3600.0, safety=L, apply=LIVE, src=f"{_D} 3장 D408)",
      desc="제어 프로그램을 켠 뒤 이 시간 안에는 멈춤을 판단하지 않는다(장비 연결·이어가기 판단 시간). 이 시간이 지나도 '준비됨' 표시가 없으면 멈춤으로 본다"),
    p("DEVICE_TIMING_LOG_ENABLED", _G, "장비 응답 시간 기록 사용", "", B, True, safety=L, apply=LIVE, src=f"{_D} 4장 D409)",
      desc="켜면 모든 직렬 장비(MFC·IG·RF Pulse·DC Pulse·TSP·STM-100)와 PLC 의 명령별 응답 시간(보냄 → 응답 끝)을 1분마다 한 줄씩 "
           "data/device_timing/timing_YYYYMMDD.csv 에 남긴다(sim 은 sim_out 아래). 통신 경로는 늦추지 않는다(기록 스레드)"),
    p("DEVICE_TIMING_LOG_INTERVAL_S", _G, "장비 응답 시간 기록 주기", "s", F, 60.0, 10.0, 3600.0, safety=L, apply=LIVE, src=f"{_D} 4장 D409)",
      desc="모아서 한 줄로 쓰는 주기(기본 1분)"),
    p("DEVICE_TIMING_RETENTION_DAYS", _G, "장비 응답 시간 기록 보관", "일", F, 14.0, 1.0, 3650.0, safety=L, apply=LIVE, src=f"{_D} 4장 D409)",
      desc="이보다 오래된 날짜 파일은 지운다"),
)


def _hang_vs_plc(v) -> "str | None":
    """SUP01 — 멈춤 판단 한도 ≥ 2 × PLC 명령 실행 상한(drivers/plc_conv.cmd_grace_s): 히터 패널 명령이 코어 잠금 안에서
    히터 엔진 잠금(PLC worker 가 히터 요청을 실행하는 동안 잡음)을 그만큼 기다릴 수 있다(D413)."""
    g = cmd_grace_s(v)
    return need(float(v["SUPERVISOR_HANG_TIMEOUT_S"]) >= 2.0 * g,
                f"멈춤 판단 한도 SUPERVISOR_HANG_TIMEOUT_S 는 PLC 명령 실행 상한({g:g} s = PLC_TIMEOUT_S×(PLC_RECONNECT_RETRY+2)"
                f"+PLC_RECONNECT_DELAY_S×(PLC_RECONNECT_RETRY+1)+1)의 2배({2.0 * g:g} s) 이상이어야 한다 — 히터 패널 명령이 그동안 코어 잠금을 잡는다")


CROSS_RULES: tuple = (
    rule("SUP01", ("SUPERVISOR_HANG_TIMEOUT_S", "PLC_TIMEOUT_S", "PLC_RECONNECT_RETRY", "PLC_RECONNECT_DELAY_S"),
         "멈춤 판단 한도 ≥ 2 × PLC 명령 실행 상한(히터 패널 명령이 코어 잠금 안에서 기다리는 최대 시간, 기본 10.5 s)", _hang_vs_plc),
)

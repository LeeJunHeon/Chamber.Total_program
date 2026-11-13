"""감시 프로그램(멈춤 감지 재시작)·장비 응답 시간 기록 설정 — 2026-11-13 D408·D409(사용자 지시 3·4장).

감시 프로그램(v2/supervisor)은 제어 프로그램이 켜 둔 건강 확인 파일(heartbeat.json)에서 아래 값을 읽는다 — 설정을 바꾸면 다음 확인부터 적용(LIVE).
"""
from __future__ import annotations

from ._base import B, F, LIVE, L, M, p

_G = "supervisor"
_D = "v2(사용자 지시 2026-11-13"

PARAMS: tuple = (
    p("SUPERVISOR_HANG_ENABLED", _G, "멈춤 감지 재시작 사용", "", B, True, safety=M, apply=LIVE, src=f"{_D} 3장 D408)",
      desc="켜면 감시 프로그램이 제어 프로그램의 핵심 루프(1초 tick 번호·명령 처리)가 SUPERVISOR_HANG_TIMEOUT_S 동안 진행이 없을 때 "
           "스레드 덤프를 남기고 강제 종료 → 다시 켠다(재시작 한도에 포함, 사유 '멈춤'). 끄면 꺼질 때만 재시작(이전 동작)"),
    p("SUPERVISOR_HEALTH_INTERVAL_S", _G, "건강 확인 주기", "s", F, 10.0, 1.0, 600.0, safety=L, apply=LIVE, src=f"{_D} 3장 D408)",
      desc="감시 프로그램이 건강 확인 파일을 보는 주기. 멈춤 판단은 이 주기 단위로(실제 감지 시각 ≤ 한도 + 이 값)"),
    p("SUPERVISOR_HANG_TIMEOUT_S", _G, "멈춤 판단 한도", "s", F, 60.0, 10.0, 3600.0, safety=M, apply=LIVE, src=f"{_D} 3장 D408)",
      desc="1초 tick 번호 또는 명령 처리(코어 잠금 확인)가 이 시간 동안 진행이 없으면 멈춘 것으로 본다. 정상 때는 1 s 마다 움직이므로 60 s 는 충분히 넉넉하다"),
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

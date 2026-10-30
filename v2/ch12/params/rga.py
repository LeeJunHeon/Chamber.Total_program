r"""RGA 링크(v2/ch12/drivers/rga_link.py)가 쓰는 설정 키 — 기본값 = v1 이 실제로 쓰던 값(근거 파일:줄).

장비 계정(v1 코드 고정 RGA_NET — admin/admin)은 비밀값 RGA_NET 으로만 둔다(기본 "" — 값은 커밋하지 않는다, 장비 로컬 파일에만).
비어 있으면 워커가 측정하지 않고 실패 응답('RGA 계정 설정 없음') — 설치 때 넣어야 한다.
"""
from __future__ import annotations

from v2.ch12.params._base import ANY, B, F, I, IDLE, S, Dropped, inst, p, secret

G = "rga"
_S = "config/settings.json"

PARAMS: tuple = (
    p("RGA_WORKER_TIMEOUT_S", G, "RGA 시도당 제한", "s", F, 60.0, 5.0, 600.0, apply=IDLE,
      src=f"{_S}:63;lib/config_common.py:93;runtime/chamber_runtime.py:643;runtime/chamber_runtime.py:1438",
      note="v1 common 값은 효과 없음(채널 모듈 .py 기본값 60.0 이 쓰임 — 값은 같다)"),
    p("RGA_MAX_ATTEMPTS", G, "RGA 시도 횟수", "", I, 3, 1, 10, src=f"{_S}:64;lib/config_common.py:96;device/rga.py:172"),
    p("RGA_RETRY_DELAY_S", G, "RGA 재시도 전 대기", "s", F, 1.0, 0.0, 60.0, src=f"{_S}:65;lib/config_common.py:97;device/rga.py:176"),
    inst("CH1_RGA_CSV_PATH", G, "CH1 RGA CSV(정본)", S, r"G:\공유 드라이브\VanaM_Sputter\RGA\Ch.1\RGA_spectrums.csv", pattern=ANY,
         v1_key="ch1.RGA_CSV_PATH", src=f"{_S}:490;lib/config_common.py:128-136;device/rga.py:134-156",
         note="워커가 이 경로에 직접 붙여 쓴다(v1 그대로 — G: 직접 기록, D171·D07 사용자 확인 필요)"),
    inst("CH2_RGA_CSV_PATH", G, "CH2 RGA CSV(정본)", S, r"G:\공유 드라이브\VanaM_Sputter\RGA\Ch.2\RGA_spectrums.csv", pattern=ANY,
         v1_key="ch2.RGA_CSV_PATH", src=f"{_S}:557;lib/config_common.py:128-136;device/rga.py:134-156"),
    secret("RGA_NET", G, "RGA 장비 계정(비밀값)", src="lib/config_common.py:139-142;apps/rga_service/rga_api.py:30-44",
           desc='JSON 글자 {"ch1": {"user": .., "password": ..}, "ch2": {..}} — 워커에 명령 줄(stdin)로만 넘긴다'),
    # ── v2 연결부 ──
    inst("RGA_WORKER_EXE", G, "RGA 워커 exe(onedir)", S, "", pattern=ANY,
         desc="비면 소스 실행(python -m v2.ch12.workers.rga_worker). 값이 있는데 파일이 없으면 v1 처럼 'not found' 실패"),
    inst("RGA_WORKER_FAKE", G, "RGA 가짜 장비(sim)", B, False, desc="워커를 --fake 로(srsinst 없이 같은 JSONL 계약)"),
    inst("RGA_HEARTBEAT_S", G, "RGA 워커 하트비트 간격", F, 1.0, 0.0, 60.0, unit="s"),
)

DROPPED: tuple = (
    Dropped("common.RGA_XLSX_SHEET", "v1 코드에서 읽는 곳 없음", f"{_S}:66"),
    Dropped("(code).RGA_XLSX_PATH", "RGA_CSV_PATH 와 같은 값(dict) — 채널 키로", "lib/config_common.py:128-136",
            replaced_by="CH1_RGA_CSV_PATH·CH2_RGA_CSV_PATH"),
    Dropped("(code).RGA_STEP_TIMEOUT_MS", "호환용 상수 — 계산 함수로(rga_link.step_timeout_ms)", "lib/config_common.py:122-125",
            replaced_by="RGA_MAX_ATTEMPTS·RGA_WORKER_TIMEOUT_S·RGA_RETRY_DELAY_S"),
)

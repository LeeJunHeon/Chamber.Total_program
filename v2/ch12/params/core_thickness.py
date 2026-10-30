"""챔버12 파라미터 조각 — core_thickness: 두께 모니터(STM-100) 판정·기록·Factor 파일(장비 통신값은 stm100 조각).

v1 은 lib/thickness_config.py 가 채널 모듈(config_ch1/2)에서 SUPPORTS_THICKNESS·THICKNESS_* 를 읽었다.
SUPPORTS_THICKNESS·THICKNESS_STALE_S(서비스·판정 함께 씀)와 통신값은 stm100 조각, 두께 CSV 폴더는 outputs 조각.
v1 settings.json 의 ch1 구역에는 일부만 있다 — 나머지 CH1 키의 기본값은 v1 이 실제로 쓰던 값(채널 모듈이 복사한 config_common
.py 값, 예: CH1_THICKNESS_DONE_CONSECUTIVE 1 · CH2 는 json 2). 판정 범위는 v1 load_judge_params 의 클램프 범위.
Factor(tooling·재료 이름)·재료 목록은 파라미터가 아니라 설정 폴더의 데이터 파일(thickness_factors_ch{N}.json·thickness_materials.json,
v1 과 같은 형식) — 표 형태이고 화면 [Factor 설정]·[재료]에서 원자적으로 저장하기 때문(결정 근거: v2/tools/convert_v1_config.py).
"""
from __future__ import annotations

from typing import Any, Mapping

from v2.ch12.params._base import (
    ANY, B, Dropped, E, F, H, I, IPV4, JSON_LIST, L, LIVE, RESTART, S, V2, inst, need, p, rule, secret,
)

_W = ('사용자 확인 필요: v1 은 settings.json common 값이 채널 모듈에 닿지 않았다(config_ch1/2 가 import 때 config_common .py 값을 복사 — inv_cfg ⚠). 기본값은 v1 이 실제로 쓰던 .py 값, v2 는 이 값이 채널 키가 없는 채널에 쓰인다')
_MIX = ('사용자 확인 필요: v1 은 읽는 곳마다 출처가 달랐다(채널 모듈 경로는 common 값 무효, 나머지는 config_common) — 지금 값이 같아 동작 차이 없음, v2 는 키 하나')


PARAMS: tuple = (
    inst("CH1_THICKNESS_FACTOR_FILE", "thickness", "CH1 tooling·재료 파일(비우면 설정 폴더 thickness_factors_ch1.json)", S, "",
       pattern=ANY, src="lib/config_common.py:527 · lib/thickness_config.py:149",
       desc="tooling·재료값 파일 (exe 옆 config 폴더 기준)",
       note="v1 값 \"config/thickness_factors_chN.json\"(exe 옆 config 기준)과 v1 기본 \"\"(같은 파일)은 v2 에서 \"\" = 설정 폴더의 thickness_factors_chN.json — 변환 도구가 파일을 옮긴다(v2/tools/convert_v1_config.py)"),
    p("CH1_THICKNESS_READY_FRESH_S", "thickness", "CH1 시작 전 최근 두께 값 필요 시간", "s", F, 5.0, 1.0, 60.0,
       src="lib/config_common.py:529 · lib/thickness_config.py:86 (ch1 구역에 없음 — 채널 모듈이 복사한 config_common .py 값)",
       desc="시작 전: 최근 이 시간(초) 안에 정상 두께 값이 있어야 시작 (아니면 E840)"),
    p("CH1_THICKNESS_LIFE_WARN_PCT", "thickness", "CH1 크리스탈 수명 경고 기준", "%", F, 10.0, 0.0, 100.0, safety=L,
       src="lib/config_common.py:530 · lib/thickness_config.py:87 (ch1 구역에 없음 — 채널 모듈이 복사한 config_common .py 값)",
       desc="시작 전: 크리스탈 수명(%)이 이보다 낮으면 경고만(종료 카드)"),
    p("CH1_THICKNESS_MAX_TIME_RATIO", "thickness", "CH1 최대 시간 배수", "배", F, 2.0, 1.0, 10.0,
       src="lib/config_common.py:531 · lib/thickness_config.py:88 (ch1 구역에 없음 — 채널 모듈이 복사한 config_common .py 값)",
       desc="최대 시간 상한 = max(예상 × 이 배수, 예상 + MARGIN), 예상 = thickness ÷ dep_rate — 도달하면 공정 실패(E848 두께 목표 미도달), 종료 절차 첫 단계 Main Shutter 닫기"),
    p("CH1_THICKNESS_MAX_TIME_MARGIN_S", "thickness", "CH1 최대 시간 여유", "s", F, 300.0, 0.0, 7200.0,
       src="lib/config_common.py:532 · lib/thickness_config.py:89 (ch1 구역에 없음 — 채널 모듈이 복사한 config_common .py 값)",
       desc="(초)"),
    p("CH1_THICKNESS_ZERO_CONFIRM_NM", "thickness", "CH1 영점 확인 기준", "nm", F, 2.0, 0.1, 100.0,
       src="lib/config_common.py:533 · lib/thickness_config.py:90",
       desc="영점 확인(Shutter Delay 뒤·Main Shutter 열기 전 영점 B): 새 값 ≤ 이 값(nm, 음수 포함)을 한 번 보면 기준점 0"),
    p("CH1_THICKNESS_ZERO_CONFIRM_S", "thickness", "CH1 영점 확인 시간", "s", F, 5.0, 1.0, 60.0,
       src="lib/config_common.py:534 · lib/thickness_config.py:91",
       desc="이 시간(초) 안에 확인 안 되면 그때 값을 기준점으로(경고, 종료 카드). 값이 하나도 없으면 기준점 없이 시간 대체"),
    p("CH1_THICKNESS_NEG_WARN_NM", "thickness", "CH1 두께 음수 경고 기준", "nm", F, 2.0, 0.1, 100.0, safety=L,
       src="lib/config_common.py:535 · lib/thickness_config.py:92",
       desc="기록만: 유효두께가 −이 값(nm)보다 낮으면 '두께 음수' 경고"),
    p("CH1_THICKNESS_DONE_CONSECUTIVE", "thickness", "CH1 목표 도달 연속 횟수", "회", I, 1, 1, 10,
       src="lib/config_common.py:536 · lib/thickness_config.py:93 (ch1 구역에 없음 — 채널 모듈이 복사한 config_common .py 값)",
       desc="2 = 새 두께 값 2번 연속 목표 이상이면 종료(값 하나가 튀어도 종료 안 함, 약 0.5초 늦음)"),
    p("CH1_THICKNESS_JUMP_MIN_NM", "thickness", "CH1 큰 튐 최소 증가량(기록만)", "nm", F, 1.0, 0.1, 100.0, safety=L,
       src="lib/config_common.py:537 · lib/thickness_config.py:94",
       desc="기록만: 증가량 > max(이 값 nm, 레시피 dep_rate × JUMP_RATE_K × Δt) 면 '큰 튐'(판정에 영향 없음)"),
    p("CH1_THICKNESS_JUMP_RATE_K", "thickness", "CH1 큰 튐 배수(기록만)", "배", F, 10.0, 1.0, 1000.0, safety=L,
       src="lib/config_common.py:538 · lib/thickness_config.py:95",
       desc="(배수)"),
    p("CH1_THICKNESS_MEASURED_RATE_MIN_S", "thickness", "CH1 실측 증착률 최소 측정 시간", "s", F, 30.0, 5.0, 3600.0,
       src="lib/config_common.py:539 · lib/thickness_config.py:96 (ch1 구역에 없음 — 채널 모듈이 복사한 config_common .py 값)",
       desc="시간 대체 증착률: 셔터 연 뒤 측정이 이 시간(초) 이상이면 실측 평균, 아니면 레시피 dep_rate"),
    p("CH1_THICKNESS_RATE_ZERO_WARN_S", "thickness", "CH1 두께 정체 경고 시간(기록만)", "s", F, 60.0, 5.0, 3600.0, safety=L,
       src="lib/config_common.py:540 · lib/thickness_config.py:97 (ch1 구역에 없음 — 채널 모듈이 복사한 config_common .py 값)",
       desc="두께가 이 시간(초) 동안 늘지 않으면 경고(기록만)"),
    p("CH1_THICKNESS_RATE_SPIKE_K", "thickness", "CH1 증착률 급등 경고 배수(기록만)", "배", F, 5.0, 1.5, 100.0, safety=L,
       src="lib/config_common.py:541 · lib/thickness_config.py:98 (ch1 구역에 없음 — 채널 모듈이 복사한 config_common .py 값)",
       desc="장비 증착률 > 레시피 dep_rate × 배수면 경고(기록만)"),
    inst("CH2_THICKNESS_FACTOR_FILE", "thickness", "CH2 tooling·재료 파일(비우면 설정 폴더 thickness_factors_ch2.json)", S, "",
       pattern=ANY, src="lib/config_common.py:527 · lib/thickness_config.py:149",
       desc="tooling·재료값 파일 (exe 옆 config 폴더 기준, 화면 [Factor 설정]에서 수정)",
       note="v1 값 \"config/thickness_factors_chN.json\"(exe 옆 config 기준)과 v1 기본 \"\"(같은 파일)은 v2 에서 \"\" = 설정 폴더의 thickness_factors_chN.json — 변환 도구가 파일을 옮긴다(v2/tools/convert_v1_config.py)"),
    p("CH2_THICKNESS_READY_FRESH_S", "thickness", "CH2 시작 전 최근 두께 값 필요 시간", "s", F, 5.0, 1.0, 60.0,
       src="lib/config_common.py:529 · lib/thickness_config.py:86",
       desc="시작 전: 최근 이 시간(초) 안에 정상 두께 값이 있어야 시작 (아니면 E840)"),
    p("CH2_THICKNESS_LIFE_WARN_PCT", "thickness", "CH2 크리스탈 수명 경고 기준", "%", F, 10.0, 0.0, 100.0, safety=L,
       src="lib/config_common.py:530 · lib/thickness_config.py:87",
       desc="시작 전: 크리스탈 수명(%)이 이보다 낮으면 경고만(종료 카드)"),
    p("CH2_THICKNESS_MAX_TIME_RATIO", "thickness", "CH2 최대 시간 배수", "배", F, 2.0, 1.0, 10.0,
       src="lib/config_common.py:531 · lib/thickness_config.py:88",
       desc="최대 시간 상한 = max(예상 × 이 배수, 예상 + MARGIN), 예상 = thickness ÷ dep_rate — 도달하면 공정 실패(E848 두께 목표 미도달), 종료 절차 첫 단계 Main Shutter 닫기"),
    p("CH2_THICKNESS_MAX_TIME_MARGIN_S", "thickness", "CH2 최대 시간 여유", "s", F, 300.0, 0.0, 7200.0,
       src="lib/config_common.py:532 · lib/thickness_config.py:89",
       desc="(초)"),
    p("CH2_THICKNESS_ZERO_CONFIRM_NM", "thickness", "CH2 영점 확인 기준", "nm", F, 2.0, 0.1, 100.0,
       src="lib/config_common.py:533 · lib/thickness_config.py:90",
       desc="영점 확인(Shutter Delay 뒤·Main Shutter 열기 전 영점 B): 새 값 ≤ 이 값(nm, 음수 포함)을 한 번 보면 기준점 0"),
    p("CH2_THICKNESS_ZERO_CONFIRM_S", "thickness", "CH2 영점 확인 시간", "s", F, 5.0, 1.0, 60.0,
       src="lib/config_common.py:534 · lib/thickness_config.py:91",
       desc="이 시간(초) 안에 확인 안 되면 그때 값을 기준점으로(경고, 종료 카드). 값이 하나도 없으면 기준점 없이 시간 대체"),
    p("CH2_THICKNESS_NEG_WARN_NM", "thickness", "CH2 두께 음수 경고 기준", "nm", F, 2.0, 0.1, 100.0, safety=L,
       src="lib/config_common.py:535 · lib/thickness_config.py:92",
       desc="기록만: 유효두께가 −이 값(nm)보다 낮으면 '두께 음수' 경고"),
    p("CH2_THICKNESS_DONE_CONSECUTIVE", "thickness", "CH2 목표 도달 연속 횟수", "회", I, 2, 1, 10,
       src="lib/config_common.py:536 · lib/thickness_config.py:93",
       desc="2 = 새 두께 값 2번 연속 목표 이상이면 종료(값 하나가 튀어도 종료 안 함, 약 0.5초 늦음)"),
    p("CH2_THICKNESS_JUMP_MIN_NM", "thickness", "CH2 큰 튐 최소 증가량(기록만)", "nm", F, 1.0, 0.1, 100.0, safety=L,
       src="lib/config_common.py:537 · lib/thickness_config.py:94",
       desc="기록만: 증가량 > max(이 값 nm, 레시피 dep_rate × JUMP_RATE_K × Δt) 면 '큰 튐'(판정에 영향 없음)"),
    p("CH2_THICKNESS_JUMP_RATE_K", "thickness", "CH2 큰 튐 배수(기록만)", "배", F, 10.0, 1.0, 1000.0, safety=L,
       src="lib/config_common.py:538 · lib/thickness_config.py:95",
       desc="(배수)"),
    p("CH2_THICKNESS_MEASURED_RATE_MIN_S", "thickness", "CH2 실측 증착률 최소 측정 시간", "s", F, 30.0, 5.0, 3600.0,
       src="lib/config_common.py:539 · lib/thickness_config.py:96",
       desc="시간 대체 증착률: 셔터 연 뒤 측정이 이 시간(초) 이상이면 실측 평균, 아니면 레시피 dep_rate"),
    p("CH2_THICKNESS_RATE_ZERO_WARN_S", "thickness", "CH2 두께 정체 경고 시간(기록만)", "s", F, 60.0, 5.0, 3600.0, safety=L,
       src="lib/config_common.py:540 · lib/thickness_config.py:97",
       desc="두께가 이 시간(초) 동안 늘지 않으면 경고(기록만)"),
    p("CH2_THICKNESS_RATE_SPIKE_K", "thickness", "CH2 증착률 급등 경고 배수(기록만)", "배", F, 5.0, 1.5, 100.0, safety=L,
       src="lib/config_common.py:541 · lib/thickness_config.py:98",
       desc="장비 증착률 > 레시피 dep_rate × 배수면 경고(기록만)"),
)

CROSS_RULES: tuple = ()
DROPPED: tuple = ()

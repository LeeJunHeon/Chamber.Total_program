"""CH2 히터 초기화 규칙(HEAT-084) — v1 main.py _init_ch2_heater(1257-1292)의 켜기·끄기 판단과 로그 문구.

init_ch2_heater(cfg, build, log) → 히터 엔진 또는 None
- 설정 읽기(heater_profile.load_heater_cfg(cfg, 'CH2_'))가 예외면 HEATER '히터 설정 읽기 실패 — 히터 기능 끔: {e!r}' 후 None.
- CH2_SUPPORTS_HEATER=false 면 아무것도 만들지 않는다(None — PLC 히터 I/O·worker 요청 없음, 로그 없음).
- build() 로 엔진을 만들고(조립이 드라이버·CSV·이벤트를 꽂는 함수) start() — 설정 경고 '⚠ 히터 설정: …'·'CH2 히터 서비스 시작'
  로그는 HeaterEngine.start 가 남긴다. 만들기·시작이 예외면 HEATER 'CH2 히터 초기화 실패: {e!r}' 후 None.
  v1 은 두 실패 문구를 모든 채널 로그로 냈다(_broadcast_log) — log(src, 문구) 하나로 넘기고 어디에 남길지는 조립이 정한다.
CH1 은 히터가 없다(v1 도 CH2 설정만 읽음). 조립(4단계)이 CH2 공정 엔진을 만들기 전에 불러 ProcessEngine(heater=…) 로 넘긴다.
"""
from __future__ import annotations

from typing import Any, Callable, Mapping, Optional

from v2.ch12.heater_profile import load_heater_cfg


def init_ch2_heater(cfg: Mapping[str, Any], build: Callable[[], Any],
                    log: Callable[[str, str], None]) -> Optional[Any]:
    try:
        hcfg, _warns = load_heater_cfg(cfg, "CH2_")
    except Exception as e:
        log("HEATER", f"히터 설정 읽기 실패 — 히터 기능 끔: {e!r}")
        return None
    if not hcfg.enabled:
        return None
    try:
        eng = build()
        start = getattr(eng, "start", None)
        if callable(start):
            start()
        return eng
    except Exception as e:
        log("HEATER", f"CH2 히터 초기화 실패: {e!r}")
        return None

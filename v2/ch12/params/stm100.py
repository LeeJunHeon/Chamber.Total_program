"""두께 모니터 STM-100 드라이버(v2/ch12/drivers/stm100.py)가 쓰는 설정 키 — 기본값 = v1 이 실제로 쓰던 값(근거 파일:줄).

v1 은 settings.json 의 ch1/ch2 구역(config_ch1/config_ch2)에서 읽는다(lib/thickness_config.load_thickness_config —
common 에만 두면 채널 모듈로 넘어가지 않는다). v2 키 = "CH1_"/"CH2_" + v1 이름. 범위·클램프는 v1 load_thickness_config 그대로
(드라이버 settings() 가 같은 클램프를 한다 — 범위 밖 값은 v1 처럼 고쳐 쓰고 경고 이벤트).
판정 상수(THICKNESS_READY_FRESH_S·DONE_CONSECUTIVE·ZERO_CONFIRM 등)·로그 폴더·Factor 파일은 2·3단계(기록·thickness.py) 몫.
"""
from __future__ import annotations

from v2.common.paramdef import Apply, ParamDef, PType, Role, Safety, Scope

_S = "config/settings.json"
_C = "lib/config_common.py"
_TC = "lib/thickness_config.py"
_LINES = {   # v1 settings.json 줄(ch1 구역, ch2 구역)
    "SUPPORTS_THICKNESS": (513, 644), "THICKNESS_TCP_HOST": (514, 645), "THICKNESS_TCP_PORT": (515, 646),
    "THICKNESS_POLL_INTERVAL_S": (516, 647), "THICKNESS_SLOW_POLL_S": (517, 648), "THICKNESS_STALE_S": (518, 649),
    "THICKNESS_CMD_TIMEOUT_S": (519, 650),
}
_CODE = {"SUPPORTS_THICKNESS": 462, "THICKNESS_TCP_HOST": 520, "THICKNESS_TCP_PORT": 521,
         "THICKNESS_POLL_INTERVAL_S": 522, "THICKNESS_SLOW_POLL_S": 523, "THICKNESS_STALE_S": 524,
         "THICKNESS_CMD_TIMEOUT_S": 525}


def _defs(ch: str, enabled: bool, port: int, port_note: str) -> tuple:
    n = 0 if ch == "ch1" else 1
    P = ch.upper() + "_"

    def p(key, label, type_, default, *, unit="", lo=None, hi=None, desc="", scope=Scope.INSTALL, note="",
          v1_default=None, pattern=None, safety=Safety.M) -> ParamDef:
        src = f"{_S}:{_LINES[key][n]};{_C}:{_CODE[key]};{_TC}:110-159"
        return ParamDef(key=P + key, group="stm100", label=f"{ch.upper()} {label}", desc=desc or label, unit=unit,
                        type=type_, default=default, min=lo, max=hi, pattern=pattern, safety=safety,
                        apply=Apply.RESTART, scope=scope, role=Role.ADMIN, v1_source=src, note=note,
                        v1_default=v1_default)

    return (
        p("SUPPORTS_THICKNESS", "두께 모니터 사용", PType.BOOL, enabled, v1_default=False,
          desc="켜면 상시 읽기와 공정 연동(Thickness 모드). 연결 안 돼도 다른 장비·공정에 영향 없음(백오프 재연결)"),
        p("THICKNESS_TCP_HOST", "두께 모니터 MOXA 주소", PType.STR, "192.168.1.50", pattern=r"[0-9A-Za-z.\-]{0,64}"),
        p("THICKNESS_TCP_PORT", "두께 모니터 MOXA 포트", PType.INT, port, lo=0, hi=65535,
          desc="0 = 미설정(사용으로 켜도 끈다 — v1 load_thickness_config)", note=port_note),
        p("THICKNESS_POLL_INTERVAL_S", "두께·증착률 읽기 주기", PType.FLOAT, 0.5, unit="s", lo=0.25, hi=5.0,
          desc="S·T 읽기 주기. 1초 행보다 빠르다 — worker 안 자기 루프"),
        p("THICKNESS_SLOW_POLL_S", "주파수·수명·크리스탈 읽기 주기", PType.FLOAT, 5.0, unit="s", lo=0.25, hi=60.0,
          desc="U·V·M 읽기 주기(v1: 두께 주기보다 작으면 두께 주기로)"),
        p("THICKNESS_STALE_S", "두께 값 오래됨 기준", PType.FLOAT, 10.0, unit="s", lo=0.0, hi=600.0,
          desc="마지막 정상 S 뒤 이 시간을 넘으면 stale·응답 없음(v1: 최소 max(2, 4×읽기 주기))"),
        p("THICKNESS_CMD_TIMEOUT_S", "명령 응답 대기", PType.FLOAT, 1.0, unit="s", lo=0.3, hi=5.0,
          desc="명령 1회 응답 대기 — 응답이 없으면 2번 더 보낸다"),
    )


PARAMS: tuple = (
    _defs("ch1", False, 0, "CH1 은 미설정(SUPPORTS_THICKNESS false)")
    + _defs("ch2", True, 4008, "⚠ 임시값 — 4001~4007 사용 중이라 4008 로 둠(settings.json:646). 실제 MOXA 포트 미확인")
)

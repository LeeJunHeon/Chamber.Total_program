"""TSP 드라이버(v2/ch12/drivers/tsp.py)가 쓰는 설정 키 — 기본값 = v1 이 실제로 쓰던 값(근거 파일:줄).

모두 v1 settings.json common 구역 키(이름 그대로). 읽는 쪽은 v1 device/tsp.py reload_runtime_cfg(cfg 없이 만들므로 config_common
— json common 값이 그대로 쓰인다, inv_cfg CFG-019·295~303). TSP 운전(사이클·IG 연동·쿨다운·매일 실행) 키는 3단계 tsp_engine 몫.
"""
from __future__ import annotations

from v2.common.paramdef import Apply, ParamDef, PType, Role, Safety, Scope

_S = "config/settings.json"


def _p(key, label, type_, default, *, unit="", lo=None, hi=None, src="", desc="", apply=Apply.IDLE,
       scope=Scope.INSTALL, safety=Safety.M, note="", v1_default=None, pattern=None) -> ParamDef:
    return ParamDef(key=key, group="tsp", label=label, desc=desc or label, unit=unit, type=type_, default=default,
                    min=lo, max=hi, pattern=pattern, safety=safety, apply=apply, scope=scope,
                    role=Role.ADMIN, v1_source=src, note=note, v1_default=v1_default)


PARAMS: tuple = (
    _p("TSP_TCP_HOST", "TSP MOXA 주소", PType.STR, "192.168.1.50", pattern=r"[0-9A-Za-z.\-]{1,64}",
       src=f"{_S}:386;lib/config_common.py:574;device/tsp.py:138",
       desc="TSP 가 붙은 MOXA NPort IP. 공정(begin_use) 시작 때 바뀐 값을 쓴다(v1 은 실행마다 새 AsyncTSP)"),
    _p("TSP_TCP_PORT", "TSP MOXA 포트", PType.INT, 4004, lo=1, hi=65535, src=f"{_S}:387;lib/config_common.py:575;device/tsp.py:139"),
    _p("TSP_RS232_ADDR", "TSP RS-232 주소", PType.INT, 128, lo=0, hi=255, apply=Apply.LIVE,
       src=f"{_S}:394;lib/config_common.py:587;device/tsp.py:176-184", desc="프레임 ADDR 바이트(0x80 = 128). 이 키가 있으면 TSP_ADDR 보다 먼저"),
    _p("TSP_ADDR", "TSP 주소(레거시 키)", PType.INT, 128, lo=0, hi=255, apply=Apply.LIVE,
       src=f"{_S}:388;lib/config_common.py:576;device/tsp.py:178",
       note="v1 레거시 — TSP_RS232_ADDR 가 있으면 쓰이지 않는다(v1 과 같은 우선순위)"),
    _p("TSP_CONNECT_TIMEOUT_S", "TSP 연결 제한", PType.FLOAT, 2.0, unit="s", lo=0.1, hi=30.0,
       src=f"{_S}:389;lib/config_common.py:577;device/tsp.py:142-145", note="v1 은 0.1 미만을 0.1 로(device/tsp.py:192)"),
    _p("TSP_WRITE_TIMEOUT_S", "TSP 응답 바이트 제한", PType.FLOAT, 1.2, unit="s", lo=0.1, hi=30.0, apply=Apply.LIVE,
       src=f"{_S}:390;lib/config_common.py:578;device/tsp.py:148-151;device/tsp.py:273-293",
       desc="v1 io_timeout — 송신 drain·수신 한 바이트마다의 제한"),
    _p("TSP_POST_SEND_DELAY_MS", "TSP 보낸 뒤 쉼", PType.INT, 20, unit="ms", lo=0, hi=1000, apply=Apply.LIVE,
       src=f"{_S}:391;lib/config_common.py:579;device/tsp.py:153-157;device/tsp.py:263-264"),
    _p("TSP_TCP_KEEPALIVE", "TSP TCP keepalive", PType.BOOL, False, v1_default=True,
       src=f"{_S}:42;lib/config_common.py:69;lib/config_common.py:582;device/tsp.py:170-173;device/tsp.py:221-225",
       note="실효값 false — .py 기본값 True 를 settings.json common 이 false 로 덮는다(inv_cfg CFG-019). "
            "devices_v1facts 의 so_keepalive=1 은 .py 기본값 기준"),
    _p("TSP_TOLERATE_SHORT_RESP", "TSP 1바이트 응답 허용", PType.BOOL, True, apply=Apply.LIVE,
       src=f"{_S}:392;lib/config_common.py:585;device/tsp.py:164-167;device/tsp.py:278"),
    _p("TSP_STATUS_POLL_INTERVAL_S", "TSP 상태 확인 간격", PType.FLOAT, 0.05, unit="s", lo=0.01, hi=1.0, apply=Apply.LIVE,
       src=f"{_S}:393;lib/config_common.py:586;device/tsp.py:186-197;device/tsp.py:431",
       desc="on/off 뒤 205 폴링 간격(창 1.5 s 는 코드 상수)"),
    _p("TSP_VERIFY_WITH_STATUS", "TSP on/off 뒤 상태 확인", PType.BOOL, True, apply=Apply.LIVE, scope=Scope.PROCESS,
       src=f"{_S}:400;lib/config_common.py:595;device/tsp.py:160-163;controller/tsp_controller.py:121"),
    _p("TSP_INACTIVITY_REOPEN_S", "TSP 유휴 재연결", PType.INT, 0, unit="s", lo=0, hi=3600,
       src=f"{_S}:33;lib/config_common.py:62", note="v1 에서 읽는 곳 없음(선언만) — v2 도 쓰지 않는다(0 = 사용 안 함)"),
)

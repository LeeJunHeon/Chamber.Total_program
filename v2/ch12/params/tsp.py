"""TSP 드라이버(v2/ch12/drivers/tsp.py)가 쓰는 설정 키 — 기본값 = v1 이 실제로 쓰던 값(근거 파일:줄).

모두 v1 settings.json common 구역 키(이름 그대로, ownership.OWNERS["tsp"]). 읽는 쪽은 v1 device/tsp.py reload_runtime_cfg
(cfg 없이 만들므로 config_common — json common 값이 그대로 쓰인다, inv_cfg CFG-019·295~303).
TSP 운전(사이클·IG 연동·쿨다운·매일 실행) 키는 core 조각(3단계 tsp_engine).
"""
from __future__ import annotations

from v2.ch12.params._base import B, F, I, IDLE, LIVE, S, inst, p

_S = "config/settings.json"
_C = "lib/config_common.py"


def _g(key, label, unit, type_, default, lo=None, hi=None, *, src, desc="", apply=LIVE, note=""):
    return p(key, "tsp", label, unit, type_, default, lo, hi, src=src, desc=desc, apply=apply, note=note)


PARAMS: tuple = (
    inst("TSP_TCP_HOST", "tsp", "TSP MOXA 주소", S, "192.168.1.50", pattern=r"[0-9A-Za-z.\-]{1,64}",
         src=f"{_S}:386;{_C}:574;device/tsp.py:138",
         desc="TSP 가 붙은 MOXA NPort IP(v1 은 TSP 실행마다 새 AsyncTSP — v2 는 begin_use 때 바뀐 값으로 다시 연결)"),
    inst("TSP_TCP_PORT", "tsp", "TSP MOXA 포트", I, 4004, lo=1, hi=65535, src=f"{_S}:387;{_C}:575;device/tsp.py:139"),
    inst("TSP_RS232_ADDR", "tsp", "TSP RS-232 주소", I, 128, lo=0, hi=255, src=f"{_S}:394;{_C}:587;device/tsp.py:176-184",
         desc="프레임 ADDR 바이트(0x80 = 128). 이 키가 있으면 TSP_ADDR 보다 먼저"),
    inst("TSP_ADDR", "tsp", "TSP 주소(레거시 키)", I, 128, lo=0, hi=255, src=f"{_S}:388;{_C}:576;device/tsp.py:178",
         note="v1 레거시 — TSP_RS232_ADDR 가 있으면 쓰이지 않는다(v1 과 같은 우선순위)"),
    inst("TSP_CONNECT_TIMEOUT_S", "tsp", "TSP 연결 제한", F, 2.0, lo=0.1, hi=30.0, unit="s",
         src=f"{_S}:389;{_C}:577;device/tsp.py:142-145;device/tsp.py:192"),
    inst("TSP_TCP_KEEPALIVE", "tsp", "TSP TCP keepalive", B, False, v1_default=True,
         src=f"{_S}:42;{_C}:69;{_C}:582;device/tsp.py:170-173;device/tsp.py:221-225",
         note="실효값 false — .py 기본값 True 를 settings.json common 이 false 로 덮는다(inv_cfg CFG-019). "
              "devices_v1facts 의 so_keepalive=1 은 .py 기본값 기준"),
    inst("TSP_INACTIVITY_REOPEN_S", "tsp", "TSP 유휴 재연결", I, 0, lo=0, hi=3600, unit="s", src=f"{_S}:33;{_C}:62",
         note="v1 에서 읽는 곳 없음(선언만) — v2 도 쓰지 않는다(0 = 사용 안 함)"),
    _g("TSP_WRITE_TIMEOUT_S", "TSP 응답 바이트 제한", "s", F, 1.2, 0.1, 30.0,
       src=f"{_S}:390;{_C}:578;device/tsp.py:148-151;device/tsp.py:273-293",
       desc="v1 io_timeout — 송신 drain·수신 한 바이트마다의 제한"),
    _g("TSP_POST_SEND_DELAY_MS", "TSP 보낸 뒤 쉼", "ms", I, 20, 0, 1000,
       src=f"{_S}:391;{_C}:579;device/tsp.py:153-157;device/tsp.py:263-264"),
    _g("TSP_TOLERATE_SHORT_RESP", "TSP 1바이트 응답 허용", "", B, True, src=f"{_S}:392;{_C}:585;device/tsp.py:164-167;device/tsp.py:278"),
    _g("TSP_STATUS_POLL_INTERVAL_S", "TSP 상태 확인 간격", "s", F, 0.05, 0.01, 1.0,
       src=f"{_S}:393;{_C}:586;device/tsp.py:186-197;device/tsp.py:431", desc="on/off 뒤 205 폴링 간격(창 1.5 s 는 코드 상수)"),
    _g("TSP_VERIFY_WITH_STATUS", "TSP on/off 뒤 상태 확인", "", B, True, apply=IDLE,
       src=f"{_S}:400;{_C}:595;device/tsp.py:160-163;controller/tsp_controller.py:121"),
)

"""IG 드라이버(v2/ch12/drivers/ig.py)가 쓰는 설정 키 — 기본값 = v1 이 실제로 쓰던 값(근거 파일:줄).

읽는 쪽 v1: AsyncIG(cfg=config_ch1/config_ch2, main.py:439-449) → 채널 모듈 → (없을 때만) config_common. TSP 런타임은 cfg 없이 만들어
기본 cfg = config_ch2(device/ig.py:23 — D189). 채널 모듈은 config_common 의 .py 기본값 복사본이라 settings.json common 값은 IG 에
넘어가지 않는다(inv_cfg ⚠ CFG-012·035~048). 지금 운영값은 IG_WAIT_TIMEOUT 말고 모두 같은 값이다 — IG_WAIT_TIMEOUT 은 common json 900 이
효과 없고 ch1·ch2 구역 900 이 쓰인다(.py 기본값 600 — D137). TSP_IG_TCP_PORT 는 core 조각(ownership.py).
"""
from __future__ import annotations

from v2.ch12.params._base import Dropped, F, I, B, IDLE, L, LIVE, M, S, inst, p

_S = "config/settings.json"
_C = "lib/config_common.py"
_W = "(⚠ common json 값은 효과 없음 — 채널 모듈의 .py 기본값이 쓰임, 지금은 같은 값)"


def _g(key, label, unit, type_, default, lo=None, hi=None, *, src, desc="", apply=IDLE, safety=M, note="", pattern=None,
       v1_default=None):
    return p(key, "ig", label, unit, type_, default, lo, hi, src=src, desc=desc, apply=apply, safety=safety, note=note,
             pattern=pattern, v1_default=v1_default)


PARAMS: tuple = (
    inst("IG_TCP_HOST", "ig", "IG MOXA 주소", S, "192.168.1.50", pattern=r"[0-9A-Za-z.\-]{1,64}",
         src=f"{_S}:70;{_C}:148;main.py:440", note=_W),
    inst("CH1_IG_TCP_PORT", "ig", "CH1 IG 포트", I, 4001, lo=1, hi=65535, src=f"{_S}:493;lib/config_ch1.py:13;main.py:441"),
    inst("CH2_IG_TCP_PORT", "ig", "CH2 IG 포트", I, 4002, lo=1, hi=65535, src=f"{_S}:560;lib/config_ch2.py:13;main.py:447"),
    inst("IG_CONNECT_TIMEOUT_S", "ig", "IG 연결 제한", F, 3.0, lo=0.5, hi=30.0, unit="s", src=f"{_S}:72;{_C}:152;device/ig.py:521-524", note=_W),
    inst("IG_RECONNECT_BACKOFF_START_MS", "ig", "IG 재연결 첫 간격", I, 1000, lo=100, hi=60000, unit="ms",
         src=f"{_S}:77;{_C}:158;device/ig.py:509", note=_W),
    inst("IG_RECONNECT_BACKOFF_MAX_MS", "ig", "IG 재연결 최대 간격", I, 20000, lo=1000, hi=600000, unit="ms",
         src=f"{_S}:78;{_C}:159;device/ig.py:563", note=_W),
    _g("IG_TIMEOUT_MS", "IG 응답 제한", "ms", I, 3000, 200, 20000, src=f"{_S}:73;{_C}:153;device/ig.py:741",
       desc="질의 한 줄을 기다리는 시간(무응답이면 None — 연결 유지). 호출 쪽 대기는 + 0.5 s", note=_W),
    _g("IG_GAP_MS", "IG 명령 간격", "ms", I, 1000, 0, 10000, src=f"{_S}:74;{_C}:154;device/ig.py:735;device/ig.py:760",
       desc="응답(또는 무응답) 뒤 다음 명령까지. 응답 없는 명령(SIG 0)은 min(150, gap) 흡수 + gap", note=_W),
    _g("IG_SKIP_ECHO", "IG 에코 줄 버림", "", B, True, src=f"{_S}:71;{_C}:150;device/ig.py:772", note=_W),
    _g("IG_POLLING_INTERVAL_MS", "IG v1 폴링 주기", "ms", I, 10000, 500, 600000, src=f"{_S}:75;{_C}:156;device/ig.py:292;device/ig.py:1300",
       desc="베이스 대기 RDI 기본 간격·공정 중 RDI(pressure 이벤트) 간격(1초 읽기는 이와 별개로 늘)", note=_W),
    _g("IG_FIRST_READ_DELAY_MS", "IG 점등 뒤 첫 읽기 지연", "ms", I, 5000, 0, 60000, src=f"{_S}:83;{_C}:167;device/ig.py:325-326",
       desc="SIG 1 'OK' 뒤 첫 RDI 까지(재점등 뒤에도). 이 동안 1초 읽기도 보내지 않는다", note=_W),
    _g("CH1_IG_WAIT_TIMEOUT", "CH1 베이스 대기 한도", "s", F, 900.0, 10.0, 86400.0, src=f"{_S}:497;device/ig.py:785;device/ig.py:808",
       desc="베이스 압력 대기 총 한도(넘으면 '목표 압력 도달 실패'). 하드 한도 = 한도 + 첫 지연 + 5 s", v1_default=600),
    _g("CH2_IG_WAIT_TIMEOUT", "CH2 베이스 대기 한도", "s", F, 900.0, 10.0, 86400.0, src=f"{_S}:572;device/ig.py:785;device/ig.py:808",
       desc="CH2 와 TSP IG(cfg 없음 → config_ch2) 가 쓴다", v1_default=600),
    _g("IG_REIGNITE_MAX_ATTEMPTS", "IG 자동 재점등 최대 횟수", "회", I, 3, 0, 20, src=f"{_S}:80;{_C}:162;device/ig.py:857", note=_W),
    _g("IG_REIGNITE_BACKOFF_MS", "IG 재점등 실패 간격", "ms 목록", S, "[2000, 5000, 10000]", pattern=r"\[\s*\d+(\s*,\s*\d+)*\s*\]",
       src=f"{_S}:81;{_C}:163;device/ig.py:914-916", desc="SIG 1(3회) 실패마다 이 간격을 차례로(JSON 목록 글자)", note=_W),
)

DROPPED: tuple = (
    Dropped("common.IG_WAIT_TIMEOUT", "효과 없음 — ch1·ch2 구역 값(CH1_/CH2_IG_WAIT_TIMEOUT 900)이 쓰인다(D137)", f"{_S}:79;{_C}:161",
            replaced_by="CH1_IG_WAIT_TIMEOUT"),
    Dropped("common.IG_INACTIVITY_REOPEN_S", "v1 값 0(끔) — 1초 읽기가 늘 통신", f"{_S}:31;{_C}:60;device/ig.py:1374-1386"),
    Dropped("common.IG_TCP_KEEPALIVE", "v2 TcpTransport 가 SO_KEEPALIVE 를 늘 켠다(v1 False → 0) — 사용자 확인 필요 후보",
            f"{_S}:40;{_C}:67;device/ig.py:533-541"),
    Dropped("common.IG_DRAIN_TIMEOUT_S", "v2 TcpTransport 는 sendall(쓰기 오류는 연결 끊김)", f"{_S}:82;{_C}:165"),
    Dropped("common.IG_WATCHDOG_INTERVAL_MS", "v2 worker 가 끊김 즉시 백오프 재연결(감시 주기 없음)", f"{_S}:76;{_C}:157;device/ig.py:513"),
    Dropped("(code).IG_TX_EOL", "코드 상수 '\\r' — 드라이버에 고정", f"{_C}:149"),
)

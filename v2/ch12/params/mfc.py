"""MFC 드라이버(v2/ch12/drivers/mfc.py)가 쓰는 설정 키 — 기본값 = v1 이 실제로 쓰던 값(근거 파일:줄).

읽는 쪽 v1: AsyncMFC(cfg=config_ch1/config_ch2, main.py:419-436) → 채널 모듈 → (없을 때만) config_common.
채널 모듈은 import 때 config_common 의 .py 기본값을 복사해 갖고 있어(from .config_common import *) settings.json common 구역 값은
MFC 에 넘어가지 않는다(inv_cfg ⚠ CFG-013·018·096~127). 지금 운영값은 common json 값과 .py 기본값이 모두 같아서 실효값이 같다 —
v2 는 공통 키 하나로 둔다(채널마다 다르게 쓰던 키는 ch1/ch2 구역 값: MFC_TCP_PORT·MFC_SCALE_FACTORS).
"""
from __future__ import annotations

from v2.ch12.params._base import Dropped, F, I, B, IDLE, L, LIVE, M, RESTART, S, inst, p

_S = "config/settings.json"
_C = "lib/config_common.py"
_W = "(⚠ common json 값은 효과 없음 — 채널 모듈의 .py 기본값이 쓰임, 지금은 같은 값)"


def _m(key, label, unit, type_, default, lo=None, hi=None, *, src, desc="", apply=IDLE, safety=M, note=""):
    return p(key, "mfc", label, unit, type_, default, lo, hi, src=src, desc=desc, apply=apply, safety=safety, note=note)


PARAMS: tuple = (
    # ───────── 연결(install) ─────────
    inst("MFC_TCP_HOST", "mfc", "MFC MOXA 주소", S, "192.168.1.50", pattern=r"[0-9A-Za-z.\-]{1,64}",
         src=f"{_S}:148;{_C}:255;main.py:420", desc="CH1·CH2 MFC 가 붙은 MOXA NPort IP"),
    inst("CH1_MFC_TCP_PORT", "mfc", "CH1 MFC 포트", I, 4003, lo=1, hi=65535, src=f"{_S}:494;lib/config_ch1.py:14;main.py:421"),
    inst("CH2_MFC_TCP_PORT", "mfc", "CH2 MFC 포트", I, 4006, lo=1, hi=65535, src=f"{_S}:561;lib/config_ch2.py:14;main.py:431"),
    inst("MFC_CONNECT_TIMEOUT_S", "mfc", "MFC 연결 제한", F, 3.0, lo=0.5, hi=30.0, unit="s",
         src=f"{_S}:151;{_C}:260;device/mfc.py:1264-1267", desc="TCP 접속 제한(v1 max(0.5, 값))", note=_W),
    inst("MFC_RECONNECT_BACKOFF_START_MS", "mfc", "MFC 재연결 첫 간격", I, 1000, lo=100, hi=60000, unit="ms",
         src=f"{_S}:162;{_C}:275;device/mfc.py:1249-1301", note=_W),
    inst("MFC_RECONNECT_BACKOFF_MAX_MS", "mfc", "MFC 재연결 최대 간격", I, 20000, lo=1000, hi=600000, unit="ms",
         src=f"{_S}:163;{_C}:276;device/mfc.py:1301", desc="실패마다 ×2, 이 값까지(장기 두절 뒤에도 이 간격)", note=_W),
    # ───────── 통신 간격·응답 ─────────
    _m("MFC_TIMEOUT", "MFC 질의 응답 제한", "ms", I, 2000, 200, 10000, src=f"{_S}:164;{_C}:278;device/mfc.py:1414-1437",
       desc="질의 한 줄을 기다리는 시간. 넘으면(연결 뒤 2 s 밖) 연결을 끊고 재연결(v1 — D182)", note=_W),
    _m("MFC_GAP_MS", "MFC 명령 간격", "ms", I, 1000, 0, 5000, src=f"{_S}:165;{_C}:279;device/mfc.py:1407;device/mfc.py:1443",
       desc="응답을 받은 뒤(응답 없는 명령은 보낸 뒤) 다음 명령까지. SPn_SET·SPn_ON 확정 대기에도 쓴다", note=_W),
    _m("MFC_ALLOW_NO_REPLY_DRAIN_MS", "MFC 응답 없는 명령 뒤 흡수", "ms", I, 80, 0, 1000,
       src=f"{_S}:156;{_C}:267;device/mfc.py:1409-1410", desc="응답 없는 명령 뒤 간격에 더하는 늦은 줄 흡수 시간", note=_W),
    _m("MFC_POST_OPEN_QUIET_MS", "MFC 연결 직후 쉼", "ms", I, 800, 0, 5000, src=f"{_S}:155;{_C}:266;device/mfc.py:1370-1377",
       desc="연결 직후 첫 명령 전 1회 기다림(v1 주석의 '밸브 오픈 후'는 잘못된 설명 — 실제는 TCP 연결 직후)", note=_W),
    _m("MFC_FIRST_CMD_EXTRA_TIMEOUT_MS", "MFC 연결 직후 질의 추가 대기", "ms", I, 2000, 0, 10000,
       src=f"{_S}:157;{_C}:268;device/mfc.py:2107-2110", desc="연결 뒤 2 s 안에 낸 질의는 호출 쪽 대기에 이만큼 더", note=_W),
    _m("MFC_SKIP_ECHO", "MFC 에코 줄 버림", "", B, True, src=f"{_S}:150;{_C}:259;device/mfc.py:1456-1458", note=_W),
    _m("MFC_DELAY_MS", "MFC FLOW ON/OFF 확정 대기", "ms", I, 1000, 0, 10000, src=f"{_S}:166;{_C}:280;device/mfc.py:469-470;device/mfc.py:514-515",
       desc="FLOW_ON·FLOW_OFF 확정 = 등록 + max(이 값, 200 ms)", note=_W),
    _m("MFC_DELAY_MS_VALVE", "MFC 밸브 확정 대기", "ms", I, 5000, 0, 60000, src=f"{_S}:167;{_C}:281;device/mfc.py:645-660",
       desc="VALVE_OPEN·VALVE_CLOSE 확정 = 등록 + 이 값", note=_W),
    _m("MFC_ZEROING_GAP_MS", "MFC 영점 뒤 대기", "ms", I, 1000, 0, 10000, src=f"{_S}:158;{_C}:269;device/mfc.py:1028-1039",
       desc="PS_ZEROING·MFC_ZEROING 뒤 간격과 확정 대기", note=_W),
    _m("MFC_POLLING_INTERVAL_MS", "MFC v1 폴링 주기", "ms", I, 3000, 500, 60000, src=f"{_S}:159;{_C}:271;device/mfc.py:1583",
       desc="공정 중(폴링 사용자 있음) 유량 이탈 감시·flow/pressure 이벤트 주기(1초 읽기는 이와 별개로 늘)", note=_W),
    _m("MFC_STABILIZATION_INTERVAL_MS", "MFC 유량 안정화 확인 주기", "ms", I, 1000, 100, 10000,
       src=f"{_S}:160;{_C}:272;device/mfc.py:1665", desc="FLOW_ON 뒤 R60 확인 간격(최대 180회 — 코드 고정값)", note=_W),
    # ───────── 환산·판정 ─────────
    _m("CH1_MFC_SCALE_FACTORS_1", "CH1 ch1(Ar) 유량 스케일", "%FS/sccm", F, 1.0, 0.0, 1000.0,
       src=f"{_S}:504;lib/config_ch1.py:20", desc="장비값(%FS) = sccm × 스케일"),
    _m("CH1_MFC_SCALE_FACTORS_2", "CH1 ch2(O2) 유량 스케일", "%FS/sccm", F, 1.0, 0.0, 1000.0, src=f"{_S}:504;lib/config_ch1.py:20"),
    _m("CH1_MFC_SCALE_FACTORS_3", "CH1 ch3(N2) 유량 스케일", "%FS/sccm", F, 1.0, 0.0, 1000.0, src=f"{_S}:504;lib/config_ch1.py:20"),
    _m("CH2_MFC_SCALE_FACTORS_1", "CH2 ch1(Ar) 유량 스케일", "%FS/sccm", F, 1.0, 0.0, 1000.0, src=f"{_S}:579;lib/config_ch2.py:18"),
    _m("CH2_MFC_SCALE_FACTORS_2", "CH2 ch2(O2) 유량 스케일", "%FS/sccm", F, 10.0, 0.0, 1000.0, src=f"{_S}:579;lib/config_ch2.py:18"),
    _m("CH2_MFC_SCALE_FACTORS_3", "CH2 ch3(N2) 유량 스케일", "%FS/sccm", F, 2.0, 0.0, 1000.0, src=f"{_S}:579;lib/config_ch2.py:18"),
    _m("MFC_PRESSURE_SCALE", "MFC 압력 스케일", "", F, 0.1, 0.0001, 1000.0, src=f"{_S}:171;{_C}:287;device/mfc.py:678;device/mfc.py:1931",
       desc="장비값 = UI(mTorr) × 이 값(SPn_SET), UI = 장비값 ÷ 이 값(R5·R1~R4)", note=_W),
    _m("MFC_PRESSURE_DECIMALS", "MFC 압력 자릿수", "", I, 3, 0, 6, src=f"{_S}:172;{_C}:288;device/mfc.py:681",
       desc="SPn_SET 반올림 자릿수·압력 문자열 자릿수", note=_W),
    _m("FLOW_ERROR_TOLERANCE", "유량 허용 비율", "비율", F, 0.05, 0.0, 1.0, apply=LIVE,
       src=f"{_S}:153;{_C}:263;device/mfc.py:1637;device/mfc.py:1962", desc="안정화 판정·공정 중 이탈 감시(|실측−목표| ≤ 목표 × 이 값)", note=_W),
    _m("FLOW_ERROR_MAX_COUNT", "유량 이탈 경고 연속 횟수", "회", I, 3, 1, 100, apply=LIVE, safety=L,
       src=f"{_S}:154;{_C}:264;device/mfc.py:1965", desc="v1 폴링 주기마다 센다 — 넘으면 경고(status)만, 공정 계속", note=_W),
    _m("MFC_PRESSURE_TOL_ABS", "압력 도달 절대 오차", "mTorr", F, 0.02, 0.0, 100.0, src=f"{_S}:174;{_C}:292;device/mfc.py:814-822", note=_W),
    _m("MFC_PRESSURE_TOL_REL", "압력 도달 상대 오차", "비율", F, 0.05, 0.0, 1.0, src=f"{_S}:175;{_C}:293;device/mfc.py:814-822", note=_W),
    _m("MFC_PRESSURE_STABLE_COUNT", "압력 안정 연속 횟수", "회", I, 3, 1, 100, src=f"{_S}:176;{_C}:294;device/mfc.py:913", note=_W),
    _m("MFC_PRESSURE_TIMEOUT_SEC", "압력 도달 대기 한도", "s", F, 60.0, 1.0, 3600.0, src=f"{_S}:177;{_C}:295;device/mfc.py:908",
       desc="v1 은 확인 간격의 합으로 잰다(읽는 시간은 넣지 않음)", note=_W),
    _m("MFC_PRESSURE_CHECK_INTERVAL_SEC", "압력 확인 간격", "s", F, 1.0, 0.1, 60.0, src=f"{_S}:178;{_C}:296;device/mfc.py:910", note=_W),
    _m("MFC_PRESSURE_READ_FAIL_STREAK_MAX", "압력 읽기 연속 실패 한도", "회", I, 3, 1, 100, src=f"{_S}:179;{_C}:297;device/mfc.py:914", note=_W),
)

DROPPED: tuple = (
    Dropped("common.MFC_TCP_PORT", "채널 파일이 덮는 기본값(4003) — CH1_/CH2_MFC_TCP_PORT 가 실효값", f"{_S}:149;{_C}:256",
            replaced_by="CH1_MFC_TCP_PORT"),
    Dropped("common.MFC_SCALE_FACTORS", "채널 파일이 덮음 — CH1_/CH2_MFC_SCALE_FACTORS_n 이 실효값", f"{_S}:170;{_C}:284"),
    Dropped("common.MFC_SCALE_FACTORS.1", "채널 파일이 덮음", f"{_S}:170"),
    Dropped("common.MFC_SCALE_FACTORS.2", "채널 파일이 덮음", f"{_S}:170"),
    Dropped("common.MFC_SCALE_FACTORS.3", "채널 파일이 덮음", f"{_S}:170"),
    Dropped("ch1.MFC_SCALE_FACTORS", "묶음 — 하위 키 CH1_MFC_SCALE_FACTORS_1~3 으로", f"{_S}:504", replaced_by="CH1_MFC_SCALE_FACTORS_1"),
    Dropped("ch2.MFC_SCALE_FACTORS", "묶음 — 하위 키 CH2_MFC_SCALE_FACTORS_1~3 으로", f"{_S}:579", replaced_by="CH2_MFC_SCALE_FACTORS_1"),
    Dropped("common.MFC_INACTIVITY_REOPEN_S", "v1 값 0(끔) — 1초 읽기가 늘 통신해 60 s 무통신이 없다", f"{_S}:32;{_C}:61;device/mfc.py:2325-2341"),
    Dropped("common.MFC_TCP_KEEPALIVE", "v2 TcpTransport 가 SO_KEEPALIVE 를 늘 켠다(v1 False → 0) — 사용자 확인 필요 후보",
            f"{_S}:41;{_C}:68;device/mfc.py:1276-1283"),
    Dropped("common.MFC_DRAIN_TIMEOUT_S", "v2 TcpTransport 는 sendall(쓰기 오류는 연결 끊김) — drain 대기 없음", f"{_S}:152;{_C}:261"),
    Dropped("common.MFC_WATCHDOG_INTERVAL_MS", "v2 worker 가 끊김 즉시 백오프 재연결(감시 주기 없음)", f"{_S}:161;{_C}:273;device/mfc.py:1253"),
    Dropped("common.MFC_SP1_VERIFY_TOL", "검증 경로(enable_verify=True)에서만 — v1 운영은 main.py:422·432 에서 꺼짐",
            f"{_S}:173;{_C}:289;device/mfc.py:1750"),
    Dropped("(code).MFC_TX_EOL", "코드 상수 '\\r' — 드라이버에 고정", f"{_C}:258"),
    Dropped("(code).MFC_COMMANDS", "명령 글자 표 — v2/ch12/drivers/mfc.py MFC_COMMANDS(같은 글자)", f"{_C}:300-330"),
)

"""MFC 드라이버(v2/ch12/drivers/mfc.py)가 쓰는 설정 키 — 기본값 = v1 이 실제로 쓰던 값(근거 파일:줄).

읽는 쪽 v1: AsyncMFC(cfg=config_ch1/config_ch2, main.py:419-436) → 채널 모듈 → (없을 때만) config_common.
채널 모듈은 import 때 config_common 의 .py 기본값을 복사해 갖고 있어(from .config_common import *) settings.json common 구역 값은
MFC 에 넘어가지 않는다(inv_cfg ⚠ CFG-013·018·096~127). 지금 운영값은 common json 값과 .py 기본값이 모두 같아서 실효값이 같다 —
v2 는 공통 키 하나로 둔다(채널마다 다르게 쓰던 키는 ch1/ch2 구역 값: MFC_TCP_PORT·MFC_SCALE_FACTORS).
"""
from __future__ import annotations

from v2.ch12.params._base import Dropped, E, F, H, I, B, IDLE, L, LIVE, M, RESTART, S, inst, need, p, rule

_S = "config/settings.json"
_C = "lib/config_common.py"
_W = "(⚠ common json 값은 효과 없음 — 채널 모듈의 .py 기본값이 쓰임, 지금은 같은 값)"


_T = r"R(?:60|5|6|69|1|2|3|4|7)"
_TICKS = rf"{_T}(?:,{_T})*"
_TICKS_OPT = rf"(?:{_T}(?:,{_T})*)?"


def _tick_rule(v):
    ev = [t for t in str(v["MFC_TICK_EVERY"]).split(",") if t]
    ro = [t for t in str(v["MFC_TICK_ROTATE"]).split(",") if t]
    if "R60" not in ev or "R5" not in ev:
        return need(False, "MFC_TICK_EVERY 에 R60·R5 가 꼭 있어야 한다(v1 폴링 판정이 씀)")
    if len(set(ev)) != len(ev) or len(set(ro)) != len(ro):
        return need(False, "같은 질의가 한 목록에 두 번 있다")
    return need(not (set(ev) & set(ro)), "같은 질의가 MFC_TICK_EVERY 와 MFC_TICK_ROTATE 양쪽에 있다")


CROSS_RULES: tuple = (
    rule("MFC01", ("MFC_TICK_EVERY", "MFC_TICK_ROTATE"), "매 tick 목록에 R60·R5 포함, 두 목록에 중복 없음", _tick_rule),
)


def _m(key, label, unit, type_, default, lo=None, hi=None, *, src, desc="", apply=IDLE, safety=M, note="", enum=None, pattern=None):
    return p(key, "mfc", label, unit, type_, default, lo, hi, src=src, desc=desc, apply=apply, safety=safety, note=note,
             enum=enum, pattern=pattern)


PARAMS: tuple = (
    # ───────── 연결(install) ─────────
    inst("MFC_TCP_HOST", "mfc", "MFC MOXA 주소", S, "192.168.1.50", pattern=r"[0-9A-Za-z.\-]{1,64}",
         src=f"{_S}:148;{_C}:255;main.py:420", desc="CH1·CH2 MFC 가 붙은 MOXA NPort IP"),
    inst("CH1_MFC_TCP_PORT", "mfc", "CH1 MFC 포트", I, 4003, lo=1, hi=65535, src=f"{_S}:494;lib/config_ch1.py:14;main.py:421"),
    inst("CH2_MFC_TCP_PORT", "mfc", "CH2 MFC 포트", I, 4006, lo=1, hi=65535, src=f"{_S}:561;lib/config_ch2.py:14;main.py:431"),
    inst("MFC_CONNECT_TIMEOUT_S", "mfc", "MFC 연결 제한", F, 3.0, lo=0.5, hi=30.0, unit="s",
         src=f"{_S}:151;{_C}:260;device/mfc.py:1264-1267", desc="TCP 접속 제한(v1 max(0.5, 값))", note=_W),
    inst("MFC_DRAIN_TIMEOUT_S", "mfc", "MFC 쓰기(drain) 한도", F, 2.0, lo=0.0, hi=30.0, unit="s",
         src=f"{_S}:152;{_C}:261;device/mfc.py:1385-1386", desc="보낼 바이트가 모두 소켓 버퍼로 들어갈 때까지의 한도(v1 writer.drain 한도) — 넘으면 끊고 재연결", note=_W),
    inst("MFC_RECONNECT_BACKOFF_START_MS", "mfc", "MFC 재연결 첫 간격", I, 1000, lo=100, hi=60000, unit="ms",
         src=f"{_S}:162;{_C}:275;device/mfc.py:1249-1301", note=_W),
    inst("MFC_RECONNECT_BACKOFF_MAX_MS", "mfc", "MFC 재연결 최대 간격", I, 20000, lo=1000, hi=600000, unit="ms",
         src=f"{_S}:163;{_C}:276;device/mfc.py:1301", desc="실패마다 ×2, 이 값까지(오래 끊긴 뒤에도 이 간격)", note=_W),
    # ───────── 통신 간격·응답 ─────────
    _m("MFC_TIMEOUT", "MFC 질의 응답 제한", "ms", I, 2000, 200, 10000, src=f"{_S}:164;{_C}:278;device/mfc.py:1414-1437",
       desc="질의 한 줄을 기다리는 시간. 넘으면(연결 뒤 2 s 밖) 연결을 끊고 재연결(v1 — D182)", note=_W),
    _m("MFC_GAP_MS", "MFC 명령 간격", "ms", I, 50, 0, 5000, src=f"{_S}:165;{_C}:279;device/mfc.py:1407;device/mfc.py:1443",
       desc="응답을 받은 뒤(응답 없는 명령은 보낸 뒤) 다음 명령까지. SPn_SET·SPn_ON 확정 대기에도 쓴다. "
            "사용자 결정 2026-11-02(D36): v1 의 1000 ms 는 RS-232 안정화용이었고 GPC3000 매뉴얼(p.72: 9600 8N1·CR 끝)에 최소 간격 규정이 없어 "
            "R60·R5 를 매 tick 읽을 수 있는 50 ms 로 — 현장 bench 에서 응답 시간·오류율로 확인", note=_W),
    _m("ZERO_MIN_IDLE_SEC", "영점 전 가스 멈춤 시간", "s", F, 10.0, 0.0, 600.0, src="v2(W-21·22 대조 C — 챔버K 실장비 시험 2026-11-25)",
       apply=LIVE, desc="압력 센서·MFC 영점을 보내기 전에 그 챔버의 모든 채널 유량 꺼짐(R69)·MFC 밸브 열림(R6)이 이 시간 이상 이어지고 PLC 가스 밸브"
       "(Ar·O₂·N₂·Main 가스)가 닫혀 있어야 한다. 시간만 모자라면 남은 만큼 기다렸다 보내고, 다른 조건이 안 맞으면 그 영점만 건너뛰고 경고. 0 = 시간 조건 없음"),
    _m("ZERO_WAIT_MAX_SEC", "영점 대기 한도", "s", F, 30.0, 0.0, 300.0, src="v2(H2 — 챔버K W-23 과 같게, 사용자 결정 2026-11-26)",
       apply=LIVE, desc="영점 조건이 될 때까지 기다리는 가장 긴 시간입니다. 넘으면 그 영점만 건너뛰고 걸린 조건을 경고로 남긴 뒤 공정을 이어 갑니다."),
    _m("MFC_COMM_LOSS_ABORT_SEC", "MFC 통신 끊김 공정 중단", "s", F, 30.0, 0.0, 120.0, safety=H,
       src="v2(F 통신 끊김 중단 — 사용자 결정 2026-11-26, v1 에 없음 · 규칙 S37)",
       desc="공정이 전원을 켠 뒤 그 챔버 MFC 가 이 시간 넘게 읽히지 않으면 공정을 멈춥니다. 0 이면 쓰지 않습니다"),
    _m("MFC_READ_MODE", "MFC 1초 읽기 방식", "", E, "split",
       src="v2(사용자 결정 2026-11-13 D407 — D405 개정)", apply=IDLE,
       desc="split(기본) = MFC_TICK_EVERY 는 매 tick, MFC_TICK_ROTATE 는 tick 마다 1개씩 차례로. all = 9개(R60·R5·R6·R69·R1~R4·R7)를 매 tick 전부(2026-11-09 E1 — 1초 예산 위험, bench 로 확인)",
       enum=("split", "all")),
    _m("MFC_TICK_EVERY", "MFC 매 tick 읽는 질의", "", S, "R60,R5,R6", src="v2(사용자 결정 2026-11-13 D407)", apply=IDLE, pattern=_TICKS,
       desc="split 일 때 매 tick 읽는 질의(쉼표). 허용: R60(유량)·R5(압력)·R6(밸브 위치)·R69(ON/OFF)·R1~R4(SP 값)·R7(선택 SP). R60·R5 는 꼭 넣는다(v1 폴링 판정이 씀)"),
    _m("MFC_TICK_ROTATE", "MFC 돌아가며 읽는 질의", "", S, "R69,R1,R2,R3,R4,R7", src="v2(사용자 결정 2026-11-13 D407)", apply=IDLE, pattern=_TICKS_OPT,
       desc="split 일 때 tick 마다 1개씩 차례로 읽는 질의(쉼표, 비우면 없음). 마지막 읽은 실제 값은 (개수 + 1) 초까지 유효 — 넘으면 빈 값(기억값으로 채우지 않음). "
            "값마다 읽은 뒤 나이(…_age_s)가 1초 기록에 남는다. 쓰기 직후 되읽기는 차례와 무관하게 즉시"),
    _m("MFC_TICK_GAP_WAIT_MS", "MFC 1초 읽기 간격 대기 한도", "ms", I, 500, 100, 2000, src="v2(사용자 지시 2026-11-13 D410 — 코드 상수 TICK_GAP_WAIT_S 를 설정으로)",
       apply=RESTART, desc="1초 읽기가 명령 간격(MFC_GAP_MS)이 끝나기를 기다려 주는 최대 시간. 넘으면 이번 tick 은 읽지 않는다(그 tick 마감 = 이 값 + MFC_TIMEOUT + 100 ms 라 재시작 뒤 적용)"),
    _m("MFC_IDLE_HOLD_MS", "MFC 제어 동작 뒤 1초 읽기 쉼", "ms", I, 300, 0, 2000, src="v2(사용자 지시 2026-11-13 D410 — 코드 상수 IDLE_HOLD_S)",
       desc="공정 엔진의 제어 명령(쓰기·확인)이 끝난 뒤 1초 읽기가 쉬는 시간(엔진의 다음 명령에 길을 양보)"),
    _m("MFC_CACHE_FRESH_S", "MFC 읽은 값 유효 시간(매 tick 질의)", "s", F, 1.0, 0.5, 10.0, src="v2(사용자 지시 2026-11-13 D410 — 코드 상수 CACHE_FRESH_S)",
       desc="매 tick 읽는 질의(R60·R5·R6)의 마지막 값을 1초 행에 쓸 수 있는 최대 나이. 돌아가며 읽는 질의는 (개수 + 1) s"),
    _m("MFC_SKIP_IF_READ_WITHIN_MS", "MFC 제어가 방금 읽은 질의 건너뜀", "ms", I, 400, 0, 2000, src="v2(사용자 지시 2026-11-13 D410 — 코드 상수 SKIP_IF_READ_WITHIN_S)",
       desc="제어(안정화 등)가 이 시간 안에 읽은 질의는 이번 tick 에 다시 보내지 않는다"),
    _m("MFC_READBACK_ENABLED", "MFC 쓴 값 확인", "", B, True, src="v2(사용자 결정 2026-11-02 D36)",
       desc="값을 바꾸는 명령(FLOW_SET·FLOW_ON/OFF·밸브·SPn_SET·SPn_ON) 뒤 되읽기 질의로 확인(꺼면 v1 과 같게 확인 없이 확정). "
            "R64~R69·R1~R4·R6·R7 응답 형식이 현장에서 다르면 bench 에서 끄고 보고"),
    _m("MFC_REWRITE_MAX", "MFC 다시 쓰기 횟수", "회", I, 3, 0, 10, src="v2(사용자 결정 2026-11-02 D36)",
       desc="되읽기가 기대와 다르면 같은 명령을 다시 쓰는 최대 횟수. 그래도 다르면 사유 코드 VERIFY_MISMATCH 로 실패"),
    _m("MFC_REWRITE_GAP_MS", "MFC 쓴 뒤 확인까지 간격", "ms", I, 300, 0, 5000, src="v2(사용자 결정 2026-11-02 D36)",
       desc="명령을 쓴 뒤 되읽기 질의까지(그리고 다시 쓰기 사이) 기다리는 시간 — 장비가 값을 반영할 시간"),
    _m("MFC_ALLOW_NO_REPLY_DRAIN_MS", "MFC 응답 없는 명령 뒤 흡수", "ms", I, 80, 0, 1000,
       src=f"{_S}:156;{_C}:267;device/mfc.py:1409-1410", desc="응답 없는 명령 뒤 간격에 더하는 늦은 줄 흡수 시간", note=_W),
    _m("MFC_POST_OPEN_QUIET_MS", "MFC 연결 직후 쉼", "ms", I, 800, 0, 5000, src=f"{_S}:155;{_C}:266;device/mfc.py:1370-1377",
       desc="연결 직후 첫 명령 전 1회 기다림(v1 주석의 '밸브 오픈 후'는 잘못된 설명 — 실제는 TCP 연결 직후)", note=_W),
    _m("MFC_FIRST_CMD_EXTRA_TIMEOUT_MS", "MFC 연결 직후 질의 추가 대기", "ms", I, 2000, 0, 10000,
       src=f"{_S}:157;{_C}:268;device/mfc.py:2107-2110", desc="연결 뒤 2 s 안에 낸 질의는 호출 쪽 대기에 이만큼 더", note=_W),
    _m("MFC_SKIP_ECHO", "MFC 에코 줄 버림", "", B, True, src=f"{_S}:150;{_C}:259;device/mfc.py:1456-1458", note=_W),
    _m("MFC_DELAY_MS", "MFC 흐름 켜기·끄기 확정 대기", "ms", I, 1000, 200, 10000, src=f"{_S}:166;{_C}:280;device/mfc.py:469-470;device/mfc.py:514-515",
       desc="FLOW_ON·FLOW_OFF 확정 = 등록 + max(이 값, 200 ms)", note=_W),
    _m("MFC_DELAY_MS_VALVE", "MFC 밸브 확정 대기", "ms", I, 5000, 0, 60000, src=f"{_S}:167;{_C}:281;device/mfc.py:645-660",
       desc="VALVE_OPEN·VALVE_CLOSE 확정 = 등록 + 이 값", note=_W),
    _m("MFC_ZEROING_GAP_MS", "MFC 영점 뒤 대기", "ms", I, 1000, 0, 10000, src=f"{_S}:158;{_C}:269;device/mfc.py:1028-1039",
       desc="PS_ZEROING·MFC_ZEROING 뒤 간격과 확정 대기", note=_W),
    _m("MFC_POLLING_INTERVAL_MS", "MFC v1 폴링 주기", "ms", I, 3000, 500, 60000, src=f"{_S}:159;{_C}:271;device/mfc.py:1583",
       desc="공정 중(폴링 사용자 있음) 유량 이탈 감시·flow/pressure 이벤트 주기(1초 읽기는 이와 별개로 늘)", note=_W),
    _m("MFC_STABILIZATION_INTERVAL_MS", "MFC 유량 안정화 확인 주기", "ms", I, 1000, 100, 10000,
       src=f"{_S}:160;{_C}:272;device/mfc.py:1665", desc="FLOW_ON 뒤 R60 확인 간격(최대 180회 — 코드 고정값)", note=_W),
    # 가스 도달 판정 연속 횟수(v1 main 27decba) — v1 은 settings.json ch1·ch2 구역에 두고 채널 모듈에서 읽는다(채널마다 따로)
    _m("CH1_MFC_FLOW_STABLE_COUNT", "CH1 가스 도달 판정 연속 횟수", "회", I, 3, 1, 180, apply=LIVE,
       src="config/settings.json(ch1) MFC_FLOW_STABLE_COUNT;lib/config_common.py MFC_FLOW_STABLE_COUNT;device/mfc.py _flow_stable_need(main 27decba)",
       desc="FLOW_ON 뒤 R60 유량이 목표 ±FLOW_ERROR_TOLERANCE 안에 이 횟수만큼 연속(확인 주기마다) 들어와야 확정. "
            "범위 밖·읽기 실패면 0 부터 다시, 확정 안 된 회차가 180 이면 시간 초과(그래서 상한 180). 1 이상 정수(v1 — 아니면 FLOW_ON 설정 오류)"),
    _m("CH2_MFC_FLOW_STABLE_COUNT", "CH2 가스 도달 판정 연속 횟수", "회", I, 3, 1, 180, apply=LIVE,
       src="config/settings.json(ch2) MFC_FLOW_STABLE_COUNT;lib/config_common.py MFC_FLOW_STABLE_COUNT;device/mfc.py _flow_stable_need(main 27decba)",
       desc="CH1 과 같은 규칙 — CH2 MFC(플라즈마 클리닝이 CH2 MFC 를 쓸 때 포함)"),
    # ───────── 환산·판정 ─────────
    _m("CH1_MFC_SCALE_FACTORS_1", "CH1 ch1(Ar) 유량 스케일", "%FS/sccm", F, 1.0, 0.0, 1000.0,
       src=f"{_S}:504;lib/config_ch1.py:20", desc="장비값(%FS) = sccm × 스케일"),
    _m("CH1_MFC_SCALE_FACTORS_2", "CH1 ch2(O₂) 유량 스케일", "%FS/sccm", F, 1.0, 0.0, 1000.0, src=f"{_S}:504;lib/config_ch1.py:20"),
    _m("CH1_MFC_SCALE_FACTORS_3", "CH1 ch3(N₂) 유량 스케일", "%FS/sccm", F, 1.0, 0.0, 1000.0, src=f"{_S}:504;lib/config_ch1.py:20"),
    _m("CH2_MFC_SCALE_FACTORS_1", "CH2 ch1(Ar) 유량 스케일", "%FS/sccm", F, 1.0, 0.0, 1000.0, src=f"{_S}:579;lib/config_ch2.py:18"),
    _m("CH2_MFC_SCALE_FACTORS_2", "CH2 ch2(O₂) 유량 스케일", "%FS/sccm", F, 10.0, 0.0, 1000.0, src=f"{_S}:579;lib/config_ch2.py:18"),
    _m("CH2_MFC_SCALE_FACTORS_3", "CH2 ch3(N₂) 유량 스케일", "%FS/sccm", F, 2.0, 0.0, 1000.0, src=f"{_S}:579;lib/config_ch2.py:18"),
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
    # ───────── sim 전용(2026-11-18 D415) ─────────
    inst("SIM_MFC_START_SP_MTORR", "debug", "sim 가상 MFC 시작 설정점 SP1~SP5", S, "0,0,30,30,0",
         pattern=r"\s*\d+(\.\d+)?\s*(,\s*\d+(\.\d+)?\s*){4}", unit="mTorr", safety=L, src="v2(사용자 지시 2026-11-18 4장 D415)",
         desc="sim 에서만 — 가상 MFC(sim/mfc_sim)가 켜질 때 들고 있는 SP1~SP5(mTorr, 쉼표 5개). 실장비는 장비 패널에 저장된 값을 R1~R4 로 읽어 쓰고 "
              "이 값은 쓰지 않는다. 기본 SP3·SP4 = 30 mTorr(CH1 압력 제어 SP3·CH2 SP4 — 사용자 기억 30, 챔버K 같은 GPC3000 SP4 자연 흡입 약 30 mTorr 기록과 일치, "
              "실장비 패널 값은 미확인 UNVERIFIED — 2026-11-19 D421). 0 이면 압력 대기가 "
              "'setpoint 읽기 실패'로 멈춘다"),
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
    Dropped("common.MFC_INACTIVITY_REOPEN_S", "v1 값 0(사용하지 않음) — 1초 읽기가 늘 통신해 60 s 무통신이 없다", f"{_S}:32;{_C}:61;device/mfc.py:2325-2341"),
    Dropped("common.MFC_TCP_KEEPALIVE", "v2 TcpTransport 가 SO_KEEPALIVE 를 늘 켠다(v1 False → 0) — 사용자 확인 필요 후보",
            f"{_S}:41;{_C}:68;device/mfc.py:1276-1283"),
    Dropped("common.MFC_WATCHDOG_INTERVAL_MS", "v2 worker 가 끊김 즉시 백오프 재연결(감시 주기 없음)", f"{_S}:161;{_C}:273;device/mfc.py:1253"),
    Dropped("common.MFC_SP1_VERIFY_TOL", "검증 경로(enable_verify=True)에서만 — v1 운영은 main.py:422·432 에서 꺼짐",
            f"{_S}:173;{_C}:289;device/mfc.py:1750"),
    Dropped("(code).MFC_TX_EOL", "코드 상수 '\\r' — 드라이버에 고정", f"{_C}:258"),
    Dropped("(code).MFC_COMMANDS", "명령 글자 표 — v2/ch12/drivers/mfc.py MFC_COMMANDS(같은 글자)", f"{_C}:300-330"),
)

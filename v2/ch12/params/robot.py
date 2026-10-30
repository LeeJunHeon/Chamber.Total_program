"""로봇 서버(v2/robot_server) 파라미터 정의.

로봇 서버는 별도 프로그램이라 제어 프로그램과 같은 설정 사전(cfg)에서 자기 키만 읽는다(키 이름 = v1 settings.json 공통 키
그대로, 새 키는 ROBOT_ 접두어). 기본값은 v1 이 실제로 쓰던 값이다.

- PARAMS      : 로봇 서버만 쓰는 키(이 파일이 원본).
- SHARED_USES : 로봇 서버가 읽기만 하는 공유 키(정의 원본은 다른 params 파일 — 같은 키를 두 번 정의하지 않게 여기엔
                (기본값, v1 근거)만 둔다). 로봇 서버는 cfg 에 없으면 이 기본값을 쓴다.

내부 명령 API 포트(ROBOT_API_PORT 50071): v1 lib/config_common.py:44-57 의 PROCESS_HOST_HOST/PORT(127.0.0.1:50071)·
PROCESS_HOST_*_TIMEOUT_S 는 모두 주석 처리돼 있어 v1 에 실제 키가 없다(apps/robot_server 프록시용 구상 — v1 미사용,
inv_robot ROB-147). 같은 포트 번호를 새 키로 쓴다.
"""
from __future__ import annotations

from v2.common.paramdef import Apply, ParamDef, PType, Role, Safety, Scope

_G = "robot_server"


def _inst(key, label, type_, default, *, desc, src, unit="", lo=None, hi=None, pattern=None, note="") -> ParamDef:
    """설치값(재시작 뒤 적용, admin)."""
    return ParamDef(key=key, group=_G, label=label, desc=desc, unit=unit, type=type_, default=default,
                    min=lo, max=hi, pattern=pattern, safety=Safety.M, apply=Apply.RESTART, scope=Scope.INSTALL,
                    role=Role.ADMIN, v1_source=src, note=note)


PARAMS: tuple = (
    _inst("HOST_SERVER_HOST", "로봇 서버 바인드 주소", PType.STR, "0.0.0.0",
          desc="로봇(호스트) TCP 서버가 듣는 주소. 0.0.0.0 = 모든 접속 허용",
          src="lib/config_common.py:35; config/settings.json:25"),
    _inst("HOST_SERVER_PORT", "로봇 서버 포트", PType.INT, 50070, lo=1, hi=65535,
          desc="로봇(호스트) TCP 서버 포트(방화벽 인바운드 허용 필요)",
          src="lib/config_common.py:36; config/settings.json:26"),
    _inst("ROBOT_API_PORT", "내부 명령 API 포트", PType.INT, 50071, lo=1, hi=65535,
          desc="제어 프로그램 내부 명령 API(HTTP/1.1, 127.0.0.1 전용) 포트 — 로봇 서버가 등록 명령을 여기로 넘긴다",
          src="v2 (v1 lib/config_common.py:47-48 주석 처리된 PROCESS_HOST_PORT 50071 — v1 에 실제 키 없음)"),
    _inst("ROBOT_API_CONNECT_TIMEOUT_S", "내부 API 접속 한도", PType.FLOAT, 5.0, lo=0.1, hi=60.0, unit="s",
          desc="내부 명령 API TCP 접속 시간 한도. 넘으면 '제어 프로그램 꺼짐'으로 처리",
          src="v2 (v1 lib/config_common.py:51 주석 PROCESS_HOST_CONNECT_TIMEOUT_S 5.0)"),
    _inst("ROBOT_API_STATUS_TIMEOUT_S", "상태 질의 응답 한도", PType.FLOAT, 5.0, lo=0.5, hi=60.0, unit="s",
          desc="GET_SPUTTER_STATUS·GET_LOADING_1/2_SENSOR 응답 한도(v1 은 메모리 판정 + PLC 1비트라 즉시 응답)",
          src="v2 (v1 lib/config_common.py:54 주석 PROCESS_HOST_STATUS_REQUEST_TIMEOUT_S 5.0; robot_v1facts reply_timing immediate)"),
    _inst("ROBOT_API_TIMEOUT_MARGIN_S", "명령 응답 한도 여유", PType.FLOAT, 30.0, lo=0.0, hi=600.0, unit="s",
          desc="시작·이동 명령 응답 한도 = v1 시퀀스 최대 시간 + 이 여유(명령별 계산은 v2/robot_server/forward.py)",
          src="v2", note="v1 은 같은 프로세스라 한도가 없었다 — 제어 프로그램이 멈췄을 때만 쓰인다"),
    _inst("ROBOT_CACHE_PATH", "마지막 응답 캐시 파일", PType.STR, r"C:\Ch12_v2\state\robot_last_replies.json",
          desc="상태 질의의 마지막 정상 응답을 저장하는 파일(제어 프로그램 꺼짐·로봇 서버 재시작 뒤 첫 질의 대비)",
          src="v2"),
    _inst("ROBOT_LOG_ECHO", "로그 화면 출력", PType.BOOL, True,
          desc="서버 로그 줄을 콘솔에도 출력(v1 서버 페이지 화면 대신)", src="v2 (v1 runtime/server_page.py 화면 표시)"),
)

# 읽기만 하는 공유 키 → (기본값, v1 근거)
SHARED_USES: dict = {
    "LOG_ROOT_DIR": (r"C:\VanaM_Logs\CH1&2", "lib/config_common.py:645; config/settings.json:289"),
    "LOCAL_FALLBACK_SERVER_DIR": ("", "lib/config_common.py:20-24 (<exe 폴더>\\Logs_LocalFallback\\Server — 빈 값이면 "
                                      "<저장소 루트>\\Logs_LocalFallback\\Server)"),
    "RECIPE_SCAN_TIMEOUT_S": (20.0, "lib/config_common.py:568; host/handlers.py:706"),
    "CHAMBER_HOST_START_WAIT_TIMEOUT_S": (20.0, "lib/config_common.py:763; runtime/chamber_runtime.py:8111"),
    "PC_HOST_START_WAIT_TIMEOUT_S": (20.0, "lib/config_common.py:826; runtime/plasma_cleaning_runtime.py:3280"),
    "CHUCK_POSITION_RESOLVE_TIMEOUT_S": (90.0, "lib/config_common.py:757; host/handlers.py:2252"),
}


def robot_cfg(cfg: dict | None = None) -> dict:
    """cfg 에서 로봇 서버가 쓰는 키만 골라 기본값으로 채운 사전."""
    cfg = dict(cfg or {})
    out = {p.key: cfg.get(p.key, p.default) for p in PARAMS}
    for k, (d, _src) in SHARED_USES.items():
        out[k] = cfg.get(k, d)
    return out

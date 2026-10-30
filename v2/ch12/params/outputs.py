"""v1 기록 출력(v2/ch12/outputs)이 쓰는 설정 키 — 이름은 v1 settings.json 키 그대로, 기본값은 v1 이 실제로 쓰던 값.

키 이름 규칙(공통 규칙 2): settings.json common 키는 그대로, ch1/ch2 구역 키는 "CH1_"/"CH2_" 를 붙인다.
기본값 근거: 운영 settings.json(config/settings.json:줄) 값 — 없으면 v1 코드값(lib/config_common.py 등:줄).
경로 키는 v1 이 '호출 시점'에 읽었지만(DEC-033) 일부는 다시 읽어도 반영되지 않았다(LOG-020 — 재시작해야 반영).
v2 는 경로 키를 모두 apply=restart 로 둔다(같은 결과, 규칙은 하나).

이 파일에 없는 키(다른 담당이 정의): DEV_MODE(app), CHn_SUPPORTS_THICKNESS(두께), PLC_COIL_LOG_BLOCK_PLAN·BUDGET_S·
PROMOTE_AFTER·BACKOFF_AFTER(PLC 드라이버 — 백오프 판정은 드라이버가 하고 기록기는 결과만 받는다), OES_LOCAL_DIR·
CHn_RGA_CSV_PATH(워커 — 경로 규칙은 outputs/paths.py 에 v1 기본값으로 둔다).
"""
from __future__ import annotations

from typing import Any, Mapping

from v2.common.paramdef import Apply, ParamDef, PType, Role, Safety, Scope

F, I, B, S = PType.FLOAT, PType.INT, PType.BOOL, PType.STR
_J = "config/settings.json"
_CC = "lib/config_common.py"


def _p(key, label, type_, default, src, *, unit="", lo=None, hi=None, apply=Apply.RESTART,
       scope=Scope.INSTALL, desc="", note="", v1_default=None, pattern=None) -> ParamDef:
    if type_ is S and pattern is None:
        pattern = r".*"
    return ParamDef(key=key, group="outputs", label=label, desc=desc or label, unit=unit, type=type_,
                    default=default, min=lo, max=hi, pattern=pattern, safety=Safety.L, apply=apply,
                    scope=scope, role=Role.ADMIN, v1_default=v1_default, v1_source=src, note=note)


_LIVE = Apply.LIVE
_PROC = Scope.PROCESS

PARAMS = (
    # ── 공통 경로 ──
    _p("LOG_ROOT_DIR", "로그 뿌리 폴더", S, r"C:\VanaM_Logs\CH1&2", f"{_J}:289; {_CC}:645",
       v1_default=r"G:\공유 드라이브\VanaM_Sputter\Sputter\Logs\CH1&2",
       note="공정 txt(CH1·CH2)·SYSTEM·ERROR·RUNTIME_DUMP·selfwatch·CH2_Heater 의 뿌리. NAS 반영은 v1 log_sync 워커"),
    _p("LOCAL_FALLBACK_ROOT", "로컬 폴백 뿌리", S, "", f"{_CC}:20-31",
       note="비우면 exe(소스 실행이면 저장소) 폴더\\Logs_LocalFallback — v1 은 settings.json 키가 아니라 계산값"),
    _p("OUTPUT_LOCAL_FIRST_DIR", "G:·NAS 출력의 로컬 정본 폴더", S, r"C:\VanaM_Logs\CH1&2\LocalFirst", "v2",
       note="D07: G: 로 가는 출력(Calib CSV·엑셀)을 여기에 먼저 쓰고 G: 에는 사본만 보낸다(아래 Calib·Process_log)"),
    _p("OUTPUT_COPY_RETRY_S", "사본 보내기 재시도 간격", F, 30.0, "v2", unit="s", lo=1.0, hi=3600.0, apply=_LIVE,
       note="사본(G:·NAS) 실패는 이 간격으로 다시 — 정본은 로컬(D07). 값은 v1 HOST_LOG_RETRY_S 와 같게"),
    # ── Sputter Calib CSV ──
    _p("SPUTTER_CALIB_DB_DIR", "Sputter Calib CSV 폴더", S, r"G:\공유 드라이브\VanaM_Sputter\Sputter\Calib\Database",
       f"{_J}:347; {_CC}:726; controller/data_logger.py:62-65"),
    # ── CH1/CH2 엑셀 ──
    _p("GDRIVE_LOG_DIR", "Plasma Cleaning 엑셀 폴더", S, "G:/공유 드라이브/VanaM_Sputter/Process_log",
       f"{_J}:445; {_CC}:831; main.py:599-603",
       note="v1 은 PC 시트만 이 값을 쓰고 Main Process 시트는 하드코딩 G:/공유 드라이브/VanaM_Sputter/Process_log"
            "(util/gdrive_logger.py:30,860 — log_dir=None) — v1 그대로(LOG-041)"),
    _p("GDRIVE_ARC_ALERT_THRESH", "Arc 강조·알림 기준", I, 5000, f"{_J}:446; {_CC}:832", unit="회", lo=0,
       apply=_LIVE, scope=_PROC, v1_default=5),
    _p("GDRIVE_REF_P_WARN_W", "Ref.P 경고 기준(받기만 함)", F, 20.0, f"{_J}:447; {_CC}:833", unit="W", lo=0.0,
       apply=_LIVE, scope=_PROC, note="v1 은 인자로 받지만 강조에 쓰지 않는다(util/gdrive_logger.py:382-405) — 그대로"),
    # ── PLC 코일 CSV ──
    _p("PLC_COIL_LOG_INTERVAL_S", "PLC 코일 CSV 주기", F, 5.0, f"{_J}:266; {_CC}:664; device/plc.py:1856-1868",
       unit="s", lo=0.5, hi=3600.0, v1_default=1.0, note="v1 최소 0.5 s(max(0.5, 값)). v2 는 1초 행에서 이 주기마다 뽑아 씀"),
    _p("PLC_COIL_LOG_NAS_DIR", "PLC 코일 CSV 폴더", S, r"C:\VanaM_Logs\CH1&2\CH1&2_PLC", f"{_J}:288; {_CC}:683",
       v1_default=r"G:\공유 드라이브\VanaM_Sputter\Sputter\Logs\CH1&2\CH1&2_PLC"),
    _p("PLC_COIL_LOG_LOCAL_DIR", "PLC 코일 CSV 폴백 폴더", S, "", f"{_CC}:25; device/plc.py:1833-1840",
       note="비우면 LOCAL_FALLBACK_ROOT\\PLC_Coil"),
    _p("PLC_COIL_LOG_SUMMARY_S", "PLC 코일 CSV 건너뜀 요약 주기", F, 60.0, f"{_J}:269; {_CC}:667", unit="s",
       lo=1.0, apply=_LIVE),
    # ── 두께 연속 CSV(채널 구역) ──
    _p("CH1_THICKNESS_LOG_DIR", "CH1 두께 연속 CSV 폴더", S, r"C:\VanaM_Logs\CH1&2\Thickness",
       f"{_J}:520; lib/thickness_config.py:143-146", note="G: 로 시작하면 기본값으로 바꾼다(v1 그대로)"),
    _p("CH2_THICKNESS_LOG_DIR", "CH2 두께 연속 CSV 폴더", S, r"C:\VanaM_Logs\CH1&2\Thickness",
       f"{_J}:651; lib/thickness_config.py:143-146", note="G: 로 시작하면 기본값으로 바꾼다(v1 그대로)"),
    # ── CH2 히터 운전 CSV·공정 txt 히터 줄(ch2 구역) ──
    _p("CH2_HEATER_LOG_ENABLED", "히터 운전 CSV 기록", B, True, f"{_J}:621; lib/heater_config.py:273", apply=_LIVE,
       scope=_PROC),
    _p("CH2_HEATER_LOG_PERIOD_S", "히터 운전 CSV 주기", F, 3.0, f"{_J}:622; lib/heater_config.py:233", unit="s",
       lo=1.0, hi=60.0, apply=_LIVE, scope=_PROC),
    _p("CH2_HEATER_LOG_DIR", "히터 운전 CSV 폴더", S, "", f"{_J}:623; lib/heater_config.py:274",
       note="비우면 LOG_ROOT_DIR\\CH2_Heater"),
    _p("CH2_HEATER_RUN_LOG_PERIOD_S", "공정 txt 히터 상태 줄 간격", F, 10.0, f"{_J}:624; lib/heater_config.py:234-238",
       unit="s", lo=0.0, hi=600.0, apply=_LIVE, scope=_PROC, note="0 = 끔, 0 초과는 2~600 으로 맞춤(v1 그대로)"),
    # ── 카메라 사진 ──
    _p("CAMERA_LOG_ROOT", "카메라 사진 루트", S, r"C:\VanaM_Logs\Camera_Logs", f"{_J}:304; {_CC}:865"),
    _p("CAMERA_MIN_FREE_GB", "카메라 디스크 여유 하한", F, 20.0, f"{_J}:305; {_CC}:866", unit="GB", lo=0.0,
       apply=_LIVE),
    # ── selfwatch ──
    _p("SELFWATCH_ENABLE", "자기 감시 기록", B, True, f"{_J}:295; {_CC}:653"),
    _p("SELFWATCH_INTERVAL_S", "자기 감시 기록 주기", F, 30.0, f"{_J}:296; {_CC}:654", unit="s", lo=5.0,
       apply=_LIVE, note="v1 최소 5 s(max(5, 값))"),
    _p("SELFWATCH_DIR", "자기 감시 폴더", S, "", f"{_J}:297; {_CC}:655", note="비우면 LOG_ROOT_DIR\\selfwatch"),
    _p("SELFWATCH_PRIVATE_MB", "전용 메모리 임계", F, 2000.0, f"{_J}:298; {_CC}:656", unit="MB", apply=_LIVE,
       note="0 이하면 끔"),
    _p("SELFWATCH_TASKS_MAX", "태스크 수 임계", I, 500, f"{_J}:299; {_CC}:657", apply=_LIVE,
       note="v2 는 asyncio 가 없어 tasks=-1(값을 주면 그 값으로 판정)"),
    _p("SELFWATCH_HANDLES_MAX", "핸들 수 임계", I, 5000, f"{_J}:300; {_CC}:658", apply=_LIVE),
    _p("SELFWATCH_USER_OBJ_MAX", "USER 객체 수 임계", I, 5000, f"{_J}:301; {_CC}:659", apply=_LIVE),
    _p("SELFWATCH_DUMP_COOLDOWN_S", "임계 덤프 쿨다운", F, 600.0, f"{_J}:302; {_CC}:660", unit="s", lo=0.0,
       apply=_LIVE),
    _p("SELFWATCH_TRACEMALLOC", "tracemalloc 켜기", B, False, f"{_J}:303; {_CC}:661", v1_default=True),
    # ── Robot_YYYYMMDD.csv(ALD 공용, 규약 v3.1) ──
    _p("HOST_LOG_ENABLED", "Robot CSV 기록", B, True, f"{_J}:282; {_CC}:677"),
    _p("HOST_LOG_NAS_DIR", "Robot CSV NAS 폴더", S, r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\Robot",
       f"{_J}:283; {_CC}:678"),
    _p("HOST_LOG_LOCAL_DIR", "Robot CSV 로컬 폴더", S, r"C:\VanaM_Logs\Robot", f"{_J}:284; {_CC}:679"),
    _p("HOST_LOG_RETRY_S", "Robot CSV NAS 재시도 주기", F, 30.0, f"{_J}:285; {_CC}:680", unit="s", lo=1.0,
       apply=_LIVE, note="v1 최소 1 s, 읽기 예외면 30"),
    _p("HOST_LOG_LOCK_STALE_S", "Robot CSV 낡은 잠금 기준", F, 120.0, f"{_J}:287; {_CC}:681", unit="s", lo=1.0,
       apply=_LIVE, note="v1 최소 1 s, 읽기 예외면 30(기본 120 과 다름 — v1 이상 동작 그대로, util/host_process_log.py:189-196)"),
)

DEFAULTS = {p.key: p.default for p in PARAMS}


def get(cfg: Mapping[str, Any], key: str) -> Any:
    """cfg 에 있으면 그 값, 없으면 이 표의 기본값."""
    if cfg is not None and key in cfg:
        return cfg[key]
    return DEFAULTS[key]

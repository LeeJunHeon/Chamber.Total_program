"""카메라 드라이버(v2/ch12/drivers/camera.py)가 쓰는 설정 키 — 기본값 = v1 이 실제로 쓰던 값(근거 파일:줄).

CAMERA_LOG_ROOT·CAMERA_MIN_FREE_GB 는 v1 settings.json common 키(읽는 쪽 config_common — 값이 그대로 쓰인다, inv_cfg CFG-231·232).
CAMERA_INDEX·CAMERA_INTERVAL_S 는 v1 에서 설정 키가 아니라 코드 상수(main.py:591 CameraRecorder(camera_index=1, interval=1.0))
— v2 에서 설정으로 꺼냈다(값은 같다). v1 은 실행 폴더에 rf_config.json 이 있으면 그 camera_index 가 우선했다
(util/camera_recorder.py:748-769) — 그 파일이 장비 노트북에 있는지는 미확인(변환 도구가 있으면 옮긴다).
RF 판독 워커 키(RF_READER_*)는 apps/rf_service(독립 도구) 몫.
"""
from __future__ import annotations

from v2.common.paramdef import Apply, ParamDef, PType, Role, Safety, Scope

_S = "config/settings.json"


def _p(key, label, type_, default, *, unit="", lo=None, hi=None, src="", desc="", apply=Apply.IDLE,
       scope=Scope.INSTALL, note="", v1_key=None) -> ParamDef:
    return ParamDef(key=key, group="camera", label=label, desc=desc or label, unit=unit, type=type_, default=default,
                    min=lo, max=hi, safety=Safety.L, apply=apply, scope=scope, role=Role.ADMIN, v1_key=v1_key,
                    v1_source=src, note=note)


PARAMS: tuple = (
    _p("CAMERA_LOG_ROOT", "카메라 사진 저장 루트", PType.STR, r"C:\VanaM_Logs\Camera_Logs",
       src=f"{_S}:304;lib/config_common.py:865;util/camera_recorder.py:609-619",
       desc="그 아래 {모드}/{YYYYmmdd_HHMMSS}/{HHMMSS}_{번호}.jpg. 만들기 실패면 Logs_LocalFallback/Camera_Logs. 다음 촬영부터 적용"),
    _p("CAMERA_MIN_FREE_GB", "카메라 디스크 여유 하한", PType.FLOAT, 20.0, unit="GB", lo=0.0, hi=10000.0, apply=Apply.LIVE,
       src=f"{_S}:305;lib/config_common.py:866;util/camera_recorder.py:848-857;util/camera_recorder.py:906-913",
       desc="이 값 미만이면 촬영 시작 안 함, 촬영 중 300장마다 다시 보고 미만이면 중단"),
    _p("CAMERA_INDEX", "카메라 장치 번호", PType.INT, 1, lo=0, hi=16, apply=Apply.IDLE,
       src="main.py:591;util/camera_recorder.py:654;util/camera_recorder.py:768",
       note="v1 코드 상수(설정 키 아님) — rf_config.json camera_index 가 있으면 v1 은 그 값(미확인)"),
    _p("CAMERA_INTERVAL_S", "카메라 촬영 간격", PType.FLOAT, 1.0, unit="s", lo=0.2, hi=60.0, apply=Apply.IDLE,
       src="main.py:591;util/camera_recorder.py:653;util/camera_recorder.py:923-929",
       note="v1 코드 상수(설정 키 아님, 최소 0.2 s)"),
)

"""카메라 드라이버(v2/ch12/drivers/camera.py)가 쓰는 설정 키 — v2 에서 설정으로 꺼낸 v1 코드 상수만.

드라이버는 CAMERA_LOG_ROOT·CAMERA_MIN_FREE_GB(v1 settings.json common 키)도 읽지만 그 둘은 outputs 조각이 정의한다
(ownership.OWNERS["outputs"] — 카메라 기록 경로). 여기 두 키는 v1 에서 설정 키가 아니라 코드 상수
(main.py:591 CameraRecorder(camera_index=1, interval=1.0)) — 값은 같다. v1 은 실행 폴더에 rf_config.json 이 있으면 그
camera_index 가 우선했다(util/camera_recorder.py:748-769) — 장비 노트북에 그 파일이 있는지는 미확인.
"""
from __future__ import annotations

from v2.ch12.params._base import F, I, IDLE, inst, p

PARAMS: tuple = (
    inst("CAMERA_INDEX", "camera", "카메라 장치 번호", I, 1, lo=0, hi=16,
         src="v2 (v1 코드 상수 main.py:591 · util/camera_recorder.py:654·768)",
         note="v1 설정 키 아님 — rf_config.json camera_index 가 있으면 v1 은 그 값(미확인)"),
    p("CAMERA_INTERVAL_S", "camera", "카메라 촬영 간격", "s", F, 1.0, 0.2, 60.0, apply=IDLE,
      src="v2 (v1 코드 상수 main.py:591 · util/camera_recorder.py:653·923-929)", note="v1 설정 키 아님(최소 0.2 s)"),
)

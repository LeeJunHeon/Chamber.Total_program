r"""OES 링크(v2/ch12/drivers/oes_link.py)·워커가 쓰는 설정 키 — 기본값 = v1 이 실제로 쓰던 값(근거 파일:줄).

v1 settings.json 키(이름 그대로): OES_LOCAL_DIR·CHAMBER_OES_USB_INDEX_CH1/CH2·CHAMBER_OES_INIT_TIMEOUT_S·OES_PLOT_MIN_INTERVAL_S·
PROCESS_DEFAULT_OES_INTEGRATION_MS. v1 에서 환경변수였던 것(OES_DAEMON·OES_DEFAULT_INTEGRATION_MS·OES_PREEMPT_WAIT_S·
OES_CLEANUP_DETACH·OES_STOP_WAIT_S·OES_WORKER_HIDE/SHOW_CONSOLE)은 같은 이름의 설정 키 — 설정에 없으면 링크는 v1 처럼
환경변수를 본다. v1 코드 상수(CSV tail 0.1 s·CSV 생성 60 s·측정 대기 max(10, dur+60)·daemon 종료 단계 2 s)도 키로 꺼냈다
(값은 v1 그대로 — 시험이 짧게 줄여 쓰려고). 워커 안의 환경변수(OES_INIT_TIMEOUT_S·OES_MUTEX_TIMEOUT_MS·OES_READ_TIMEOUT_S 등)는
v1 처럼 환경으로 넘어간다(키 아님).
"""
from __future__ import annotations

from v2.ch12.params._base import B, F, I, IDLE, LIVE, S, ANY, Dropped, inst, p

G = "oes"
_S = "config/settings.json"

PARAMS: tuple = (
    inst("OES_LOCAL_DIR", G, "OES CSV 저장 폴더", S, r"C:\VanaM_Logs\OES", pattern=ANY,
         src=f"{_S}:144;lib/config_common.py:249;device/oes.py:35-50",
         desc=r"측정 CSV 를 <폴더>\CH1·CH2 에 남긴다(환경변수 OES_LOCAL_BASE 가 있으면 그것이 먼저 — v1 과 같음). "
              "NAS 사본은 log_sync(LOG_SYNC_PAIRS 의 OES 쌍)"),
    inst("CHAMBER_OES_USB_INDEX_CH1", G, "CH1 OES USB 번호", I, 0, 0, 7,
         src=f"{_S}:348;lib/config_common.py:728;runtime/chamber_runtime.py:629-637",
         note="v1 common 값은 효과 없음(채널 모듈 .py 기본값 0 이 쓰임 — 값은 같다)"),
    inst("CHAMBER_OES_USB_INDEX_CH2", G, "CH2 OES USB 번호", I, 1, 0, 7,
         src=f"{_S}:349;lib/config_common.py:729;runtime/chamber_runtime.py:629-637",
         note="v1 common 값은 효과 없음(채널 모듈 .py 기본값 1 이 쓰임 — 값은 같다)"),
    p("CHAMBER_OES_INIT_TIMEOUT_S", G, "OES 초기화 제한", "s", F, 20.0, 5.0, 120.0, apply=IDLE, v1_default=30.0,
      src=f"{_S}:369;lib/config_common.py:762;runtime/chamber_runtime.py:1398;device/oes.py:650",
      desc="daemon READY(또는 init 워커) 대기. 워커에는 max(5, 이 값−2) 를 OES_INIT_TIMEOUT_S 로 준다"),
    p("OES_PLOT_MIN_INTERVAL_S", G, "OES 그래프 최소 갱신 간격", "s", F, 0.5, 0.0, 10.0, apply=LIVE,
      src=f"{_S}:143;controller/graph_controller.py:264", note="화면 그래프 몫 — 링크는 on_data 를 모두 넘긴다"),
    p("PROCESS_DEFAULT_OES_INTEGRATION_MS", G, "OES 기본 적분 시간", "ms", I, 60, 1, 60000,
      src=f"{_S}:374;controller/process_controller.py:2214", note="레시피 값이 비었을 때(공정 엔진 몫)"),
    # ── v1 환경변수(같은 이름) ──
    inst("OES_DAEMON", G, "OES daemon(상주) 모드", B, True, src="device/oes.py:215-219",
         desc="켜짐 = 워커를 상주시켜 측정마다 stdin 명령, 꺼짐 = 측정마다 one-shot 워커(v1 환경변수 OES_DAEMON=0)"),
    p("OES_DEFAULT_INTEGRATION_MS", G, "적분 시간 0 이하일 때", "ms", I, 50, 1, 60000, src="device/oes.py:700-702",
      desc="daemon 측정 명령의 integration_ms ≤ 0 이면 이 값(v1 환경변수, 기본 50)"),
    p("OES_PREEMPT_WAIT_S", G, "이전 워커 정리 대기", "s", F, 5.0, 0.2, 60.0, src="device/oes.py:855-866"),
    p("OES_CLEANUP_DETACH", G, "정리 때 기다리지 않음", "", B, False, src="device/oes.py:1188-1195"),
    p("OES_STOP_WAIT_S", G, "정리 때 측정 종료 대기", "s", F, 60.0, 1.0, 600.0, src="device/oes.py:1194"),
    inst("OES_WORKER_HIDE_CONSOLE", G, "워커 콘솔 숨김(Windows)", B, False, src="device/oes.py:54-76"),
    inst("OES_WORKER_SHOW_CONSOLE", G, "워커 새 콘솔(Windows, 디버그)", B, False, src="device/oes.py:54-76"),
    # ── v1 코드 상수 ──
    p("OES_CSV_TAIL_S", G, "CSV tail 간격", "s", F, 0.1, 0.01, 2.0, src="device/oes.py:1371;device/oes.py:1414"),
    p("OES_CSV_CREATE_TIMEOUT_S", G, "CSV 생성 대기 제한", "s", F, 60.0, 1.0, 600.0, src="device/oes.py:1352"),
    p("OES_MEASURE_TIMEOUT_MIN_S", G, "측정 대기 최소", "s", F, 10.0, 1.0, 600.0, src="device/oes.py:707;device/oes.py:991",
      desc="측정 결과 대기 = max(이 값, 측정 시간 + OES_MEASURE_TIMEOUT_MARGIN_S)"),
    p("OES_MEASURE_TIMEOUT_MARGIN_S", G, "측정 대기 여유", "s", F, 60.0, 0.0, 600.0, src="device/oes.py:707;device/oes.py:991"),
    inst("OES_CLOSE_STEP_S", G, "daemon 종료 단계 대기", F, 2.0, 0.1, 30.0, unit="s", src="device/oes.py:558-577",
         desc="close 명령 → 이 시간 → terminate → 이 시간 → kill → 이 시간"),
    # ── v2 연결부 ──
    inst("OES_DLL_PATH", G, "분광기 DLL 경로", S, "apps/oes_service/SPdbUSBm.dll", pattern=ANY,
         desc="워커에 --dll_path 로 준다. 상대 경로는 저장소 루트 기준 — v1 파일을 그대로 가리킨다(복사하지 않음)"),
    inst("OES_WORKER_HOME", G, "OES 워커 기준 폴더", S, "", pattern=ANY,
         desc=r"워커의 log\·stop\·oes_config.json(일련번호 확인·Ar 이상 감지) 위치. 비면 DLL 폴더(v1 배치 apps\oes_service 와 같음)"),
    inst("OES_WORKER_EXE", G, "OES 워커 exe(onedir)", S, "", pattern=ANY,
         desc="비면 소스 실행(python -m v2.ch12.workers.oes_worker). 값이 있는데 파일이 없으면 v1 처럼 '워커 exe 없음'"),
    inst("OES_WORKER_FAKE", G, "OES 가짜 분광기(sim)", B, False, desc="워커를 --fake 로(DLL 없이 같은 JSONL 계약) — sim·시험용"),
    inst("OES_HEARTBEAT_S", G, "OES 워커 하트비트 간격", F, 1.0, 0.0, 60.0, unit="s", desc="0 이면 끔. 나이는 링크 status() 에"),
)

DROPPED: tuple = (
    Dropped("common.OES_AVG_COUNT", "v1 메인이 읽지 않음 — 생성자 기본값 3 고정(D177). v2 링크도 3(oes_link.AVG_COUNT)",
            f"{_S}:142;device/oes.py:156"),
)

"""v1 전수 목록 — 빠진 항목 찾기 C(gap).

방법: 전수 목록(inv_*.py)을 보지 않고 v1 코드만 읽어 관찰 가능한 동작 275개를 독립적으로 적은 뒤
(host/ 전부, util/ 전부, apps/ 전부(파이썬), controller/chat_notifier.py·data_logger.py — 기준 커밋 66efc7b),
all_items() 2,346개와 대조해 어디에도 없는 동작만 여기에 둔다.
대부분은 이미 ROB·LOG·APP·CHAT 영역에 있었다. 여기 남은 것은 주로 문구·세부 규칙·독립 도구 내부 흐름이다.
"""
from __future__ import annotations

ITEMS = [
    dict(id="GAPC-001", area="gap",
         desc="remote_cmd CSV 의 req_data_json·res_data_json 칸: json.dumps(ensure_ascii=False, default=str) 가 실패하면 "
              "행을 버리지 않고 '<<JSON_DUMP_ERROR {예외 repr}>> {repr(obj) 앞 300자}' 문자열을 그 칸에 넣는다",
         v1="host/server.py:175-179; host/server.py:302; host/server.py:409; host/server.py:413",
         v2="v2/robot_server/", test="", status="todo", note=""),
    dict(id="GAPC-002", area="gap",
         desc="ERROR 일자 로그의 시스템 메시지 문구: 'UNCAUGHT EXCEPTION (sys.excepthook)'(CRITICAL, 그 뒤 원래 sys.__excepthook__ 호출), "
              "'UNCAUGHT EXCEPTION (threading.excepthook) thread=..', 'UNRAISABLE EXCEPTION: {err_msg}', "
              "'ASYNCIO EXCEPTION: {msg}'(예외 없으면 '| context=..'), 'PYTHON WARNING {file}:{line} {Category}: {msg}', "
              "'QT {msg} ({file}:{line} {function})', 'PROCESS SIGNAL RECEIVED: {이름} ({번호})' 뒤 이전 신호 처리기 호출. "
              "설치 기록 'system logging ready. root=..', 'faulthandler enabled -> ..', 'hang dump enabled: ..', "
              "'asyncio exception handler installed', 'loop.create_task patched for immediate task crash logging', "
              "'warnings.showwarning hooked', 'Qt message handler installed', 'signal handler installed: {이름}'. "
              "종료 직전 Qt 처리기는 이전 것으로 되돌림('Qt message handler uninstalled/restored')",
         v1="util/app_logging.py:296-306; util/app_logging.py:338; util/app_logging.py:347-389; util/app_logging.py:484-499; "
            "util/app_logging.py:539-543; util/app_logging.py:546-621; util/app_logging.py:623-651",
         v2="v2/ch12/outputs/", test="", status="todo", note=""),
    dict(id="GAPC-003", area="gap",
         desc="런타임 덤프 스냅샷 세부: ui 위젯을 plain_text·text·line_edit·spin{value,text}·combo{index,text}·"
              "buttons{text,checked,enabled,visible}·labels·misc(실패 문구)로 분류(이름에 logMessage 가 든 창은 마지막 300줄), "
              "asyncio 태스크마다 repr·name·done·cancelled·coro·stack(최대 50프레임), 객체마다 type·is_connected/connected/_running/running "
              "+ vars 상태 repr(ui·_loop·loop·app·qapp·chat·plc·_plc·mfc·mfc_gas·mfc_pressure·ig 속성 제외, repr 400자 넘으면 "
              "' ...[truncated]'), meta(timestamp·pid·python·executable·argv·platform·cwd)·threads 목록. "
              "JSON 은 indent 2·ensure_ascii False, 파일 쓰기는 데몬 스레드 'runtime-dump-writer'(경로를 먼저 돌려준다)",
         v1="util/runtime_dump.py:41-44; util/runtime_dump.py:61-202; util/runtime_dump.py:224-244",
         v2="v2/ch12/outputs/", test="", status="todo", note=""),
    dict(id="GAPC-004", area="gap",
         desc="apps/robot_server RAW 프록시 수명 로그·규칙: 시작 'RobotHost started on {addr} (RAW proxy -> {host}:{port})', "
              "정지 'RobotHost stopped (RAW proxy)'. 이미 떠 있으면 start 무시, stop 은 서버를 닫고 모든 연결 태스크를 취소·대기, "
              "Restart = stop 후 start. 업스트림 연결 실패면 클라이언트 연결을 바로 닫고 '[CONN_ERR] cid=.. peer=.. err=..'"
              "(정지 중에는 CONN_ERR 생략), cid = id(writer) & 0xFFFF",
         v1="apps/robot_server/main_server.py:99-127; apps/robot_server/main_server.py:151-206; apps/robot_server/main_server.py:250-261",
         v2="v2/robot_server/", test="", status="todo", note=""),
    dict(id="GAPC-005", area="gap",
         desc="apps/robot_server/state_api.py 응답 세부: /healthz 는 인증 없이 {'ok': true}; 그 밖은 X-Api-Token 불일치 401 "
              "{'ok': false, 'error': 'unauthorized'}, 없는 경로 404 'not found', 처리 예외 400 str(e); GET /api/state?ch_id=X → "
              "{'ok', 'state': {..}|null}, ch_id 없으면 {'ok', 'states': {ch: {..}}}; POST /api/state 는 ch_id·run_id 필수(새 run_id 면 "
              "기록 새로 만듦, 빠진 필드는 이전 값 유지, heartbeat 도 갱신), POST /api/heartbeat 는 ch_id 필수·run_id 있으면 교체; "
              "OFFLINE/HEARTBEAT_TIMEOUT 전환은 읽을 때 적용; 시작 시 '[STATE_API] http://h:p  token=..' 출력",
         v1="apps/robot_server/state_api.py:42-100; apps/robot_server/state_api.py:106-171",
         v2="v2/robot_server/", test="", status="todo",
         note="v1 에서 run_state_api 를 부르는 곳이 없음(전역 검색 0건) — na 후보, 사용자 확인 필요"),
    dict(id="GAPC-006", area="gap",
         desc="rf_worker 판독 알고리즘(목록이 읽지 않은 240-333줄): 칸마다 3슬롯 R채널 크롭 → 밝은 획 무게중심 정렬(align_com) → "
              "ONNX CNN 11클래스(10=빈칸이면 그 칸 판독 실패), TTA 7개(원본+±1px 이동 4개+배율 0.92·1.08) softmax 평균, "
              "CPUExecutionProvider·RF_THREADS(2) 스레드; 값 = 세 자리 + 부호 검출 시 '-', 신뢰도 = 세 자리 중 최소. "
              "시계열 보정: 신뢰도 ≥ 0.90 프레임은 그대로, 아니면 ±3프레임 가중 다수결(신뢰도 ≥ 0.90 이면 가중 2), 최빈 비율 0.6. "
              "사진 보존: 한 칸이라도 신뢰도 < RF_KEEP_CONF(0.50, RF3_TUNE 0.30, 환경변수 RF_KEEP_CONF_RF3_TUNE·_CH2_LOAD·_CH1_LOAD)",
         v1="apps/rf_service/rf_worker.py:69-94; apps/rf_service/rf_worker.py:240-329; apps/rf_service/rf_worker.py:366-375; "
            "apps/rf_service/preprocess.py:18-32; apps/rf_service/extract.py:46-85",
         v2="v2/ch12/outputs/", test="", status="todo", note="독립 도구 그대로 사용 — v2 사진이 이 판독에 맞는지 확인 대상"),
    dict(id="GAPC-007", area="gap",
         desc="rf_worker 명령줄·환경변수·종료 코드: --root(RF_CAMERA_LOGS), --model(RF_MODEL, 기본 models/cnn.onnx), "
              "--state-dir(RF_STATE_DIR), --csv-dir(RF_CSV_DIR), --nas-dir(RF_NAS_DIR), --stale-min(RF_STALE_MIN), "
              "--since(RF_SINCE — 세션 이름이 이보다 작으면 건너뜀), --no-tta(RF_TTA=0), --dry-run(사진 삭제·매니페스트·디스크 가드·"
              "NAS 전송·보관 정리 없이 '[dry-run] ..' 집계만). 모델·보정 파일·루트가 없으면 '! 모델 없음|보정 파일 없음|루트 없음: ..' "
              "후 종료 코드 2. 예상 못 한 예외는 run.log(상태 폴더를 못 정하면 exe 옆 run_error.log)에 traceback, "
              "구글챗 '[RF-Reader] 워커 비정상 종료: {마지막 줄}' 후 종료 코드 1",
         v1="apps/rf_service/rf_worker.py:615-669; apps/rf_service/rf_worker.py:690-691; apps/rf_service/rf_worker.py:713-717; "
            "apps/rf_service/rf_worker.py:730-752",
         v2="v2/ch12/outputs/", test="", status="todo", note="독립 도구 그대로 사용"),
    dict(id="GAPC-008", area="gap",
         desc="rf_worker 한 번 실행 흐름·기록: 순서 = 디스크 가드 → 세션 판독 → manifest 저장 → NAS 전송 → 보관 정리. "
              "판독 결과 empty·no_valid_frames 도 매니페스트에 남겨 다시 시도하지 않고, 처리 예외('! 처리 실패 {mode}/{세션}: ..')는 "
              "남기지 않아 다음 실행에 다시 시도. 세션마다 '{mode}/{세션}: frames=n CSV작성 보존=k 삭제=d'. CSV 줄 수 불일치면 tmp 삭제·"
              "사진 삭제 없음. 디스크 가드가 다 지워도 부족하면 구글챗 '[RF-Reader] 디스크 여유 부족 xGB < yGB (정리 가능한 완료 세션 "
              "n개 삭제 후에도 부족)'. run.log 머리 줄(settings·root·state_dir·csv_dir·nas_dir·retention·min_free·upload_alert·notify·"
              "model·slots·tta·stale_min·since·dry_run)과 끝 줄 '[rf-worker] 완료 Ns 처리= 건너뜀= 보류= 삭제사진= 보존사진= "
              "업로드성공= 미전송= 정리한세션= 디스크여유=a→bGB'",
         v1="apps/rf_service/rf_worker.py:394-417; apps/rf_service/rf_worker.py:591-612; apps/rf_service/rf_worker.py:637-723",
         v2="v2/ch12/outputs/", test="", status="todo", note="독립 도구 그대로 사용"),
    dict(id="GAPC-009", area="gap",
         desc="log_sync 실패 집계·문구: src 폴더 없음 '! src 없음: ..'(쌍 실패), dst 를 만들 수 없거나 폴더가 아님 "
              "'! dst 접근 불가: ..: .. → 이 쌍 건너뜀'(쌍 실패), stat 실패·파일 복사 실패('실패 src → dst: ..')는 실패 수; "
              "쌍 실패나 실패 1건 이상이면 그 실행은 실패로 연속 실패 수 +1. LOG_SYNC_ENABLED=false 면 '[log-sync] LOG_SYNC_ENABLED=false → "
              "종료'(state 손대지 않음), --dry-run 은 '[dry-run] 복사 대상: ..' 만 세고 state 를 바꾸지 않음. run.log 줄: 머리 "
              "'[log-sync] settings=.. state_dir=.. enabled=.. pairs=.. alert_after=.. notify=on|off dry_run=..', 쌍마다 '  src → dst' 와 "
              "'검사= 복사= 건너뜀= 실패=( (쌍 전체 실패))', 끝 '[log-sync] 완료 쌍= 검사= 복사= 건너뜀= 실패= 소요=x.xs', state 저장 실패 "
              "'! state 저장 실패: ..'",
         v1="apps/log_sync_service/log_sync.py:247-289; apps/log_sync_service/log_sync.py:308-366",
         v2="v2/ch12/outputs/", test="", status="todo", note="독립 도구 그대로 사용"),
    dict(id="GAPC-010", area="gap",
         desc="recovery_service 런처 세부: main.exe 가 없으면 stderr '[launcher] main.exe not found: ..'·'Tip: --main-exe ..' 후 "
              "종료 코드 2; 상대 --main-exe 는 프로젝트 루트 기준; --workdir 기본 = exe 폴더; '--' 뒤 인자를 그대로 넘김('--' 자체는 뺌); "
              "stdout '[launcher] start: [..] (cwd=..)', '[launcher] exited: code=N', '[launcher] reached max restarts (N) -> stop'; "
              "재시작 대기 최소 0.1 s; CH12_RECOVERY_BASE_DIR 가 상대 경로면 프로젝트 루트 기준",
         v1="apps/recovery_service/launcher.py:14-39; apps/recovery_service/launcher.py:60-75; apps/recovery_service/launcher.py:96-120; apps/recovery_service/paths.py:14-27",
         v2="v2/supervisor/", test="", status="todo", note="v1 메인이 쓰지 않는 도구 — 참고만(APP-069 와 같은 처리)"),
    dict(id="GAPC-011", area="gap",
         desc="Calib CSV pending 파일은 실제 본파일 머리글이 아니라 현재 정의 머리글(self.header — CH2 히터·두께 열 포함 전체)로 만들고, "
              "병합 때도 행을 self.header 키로 옮긴 뒤 본파일 appender 의 열(머리글 업그레이드 실패 시 옛 열 순서)만 남긴다 — "
              "본파일이 옛 머리글이면 pending 에만 있던 새 열 값은 병합 때 버려진다",
         v1="controller/data_logger.py:154-157; controller/data_logger.py:670-676; controller/data_logger.py:737-742; util/log_hub.py:430-437",
         v2="v2/ch12/outputs/", test="", status="todo", note=""),
    dict(id="GAPC-012", area="gap",
         desc="selfwatch 표본 출처: psutil 이 있으면 rss → working_set_mb, private(없으면 vms) → private_mb(소수 1자리), "
              "없거나 실패하면 Windows ctypes GetProcessMemoryInfo(WorkingSetSize·PrivateUsage); handles = GetProcessHandleCount, "
              "gdi·user = GetGuiResources(0·1); Windows 가 아니면 -1. threads = threading.active_count(), tasks = len(all_tasks(loop)), "
              "gc0~2 = gc.get_count(). CSV 머리글은 프로세스 안에서 파일당 한 번만 판정(_header_written)",
         v1="util/self_watch.py:51-135; util/self_watch.py:381-394",
         v2="v2/ch12/outputs/", test="", status="todo", note="v2 는 1초 행의 프로그램 상태로 대체 — 같은 값 출처인지 확인"),
    dict(id="GAPC-013", area="gap",
         desc="OES 워커 status 문구(메인이 '[OES] ..' 로 공정 로그에 남김): '[worker] init|measure|daemon: acquiring mutex name=.. timeout_ms=..', "
              "'[worker] init: mutex acquired name=..', '[worker] init begin ch= usb= dir= dll_arg= dll_resolved= dll_exists=', "
              "'[worker] init start ch= usb= dll_path=', '[worker] init done ok= resolved_usb= pixels=', '[worker] open csv: ..', "
              "'[daemon] open csv: ..', '[worker] daemon init begin ch= usb= dir= dll_path=', '[worker|daemon] stop requested (usb_global= "
              "usb_local= csv_flag=)', '[worker] stop already requested before init (global flag exists)', '[daemon] stdin closed (EOF) → exit', "
              "'[daemon] close requested → cleanup & exit', '[daemon] reset requested', '[daemon] auto-reset after measure rc= ok=', "
              "'[daemon] auto-reset after exception ok=', '[daemon] unknown cmd=.. obj=..', '[link] .. 재초기화 실패: ..', "
              "'[link] 30초 뒤 재초기화 재시도', '[read] 재초기화 완료, 측정 계속'",
         v1="apps/oes_service/oes_api.py:1219-1327; apps/oes_service/oes_api.py:1363-1385; apps/oes_service/oes_api.py:1468; "
            "apps/oes_service/oes_api.py:1534-1609; apps/oes_service/oes_api.py:1648-1670; apps/oes_service/oes_api.py:2021; "
            "apps/oes_service/oes_api.py:2064-2082; apps/oes_service/oes_api.py:2215-2354",
         v2="v2/ch12/workers/", test="", status="todo", note=""),
]

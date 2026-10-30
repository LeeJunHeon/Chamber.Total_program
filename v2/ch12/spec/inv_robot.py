"""v1 전수 목록 — 로봇(호스트) 영역(area="robot", id 접두어 "ROB-").

덮는 것
- 로봇과 주고받는 프레임 형식(16바이트 헤더 필드 바이트 단위·JSON 바디), 연결·프레이밍·응답 명령 이름,
  서버 로그(PLC_HOST·NET·PLC_REMOTE 태그)와 remote_cmd_YYYYMMDD.csv 명령 기록.
- host/setup.py 에 등록된 18개 명령 각각의 인자·기본값·응답 필드·오류 코드·ETA·응답 시점
  (즉시 응답 / 시퀀스 완료 후 응답 / 프리플라이트 신호까지 대기)·인터락(게이트 상태·busy·잠금)·
  시퀀스(진공·벤트·게이트·척·핀)·타임아웃·폴링 간격.
- 실패 응답(fail payload) 형식과 코드 결정 규칙, 실패 때 log/chat/popup 보고.
- Robot_YYYYMMDD.csv(ALD 프로그램과 같이 쓰는 공용 파일) — util/host_process_log.py 규약 v3.1 전부
  (경로·파일명·15열·결과 문자열·기록키·잠금 파일·중복 검사·로컬 pending/open 복구·워커) 와
  핸들러·챔버 런타임·플라즈마 클리닝 런타임이 그 규약의 어느 단계를 언제 부르는지.
- apps/robot_server(RAW TCP 프록시·서버 화면·state_api) — v1 에서 쓰이지 않는 근거와 함께 na.

읽은 v1 파일(끝까지)
- host/__init__.py, host/context.py, host/protocol.py, host/request_ctx.py, host/router.py,
  host/setup.py, host/server.py, host/handlers.py(2327줄)
- apps/robot_server/__init__.py, apps/robot_server/main_server.py, apps/robot_server/state_api.py,
  apps/robot_server/runtime/__init__.py, apps/robot_server/runtime/server_page.py
- util/host_process_log.py(755줄)
근거 확인을 위해 부분적으로 읽은 파일
- errors/error_payload.py, errors/error_reporter.py, errors/error_registry.py(get_error_definition·코드별 정의)
- util/log_hub.py(DailyCsvDictAppender), device/plc.py(주소맵·pulse·write_switch·main_shutter·log_route·watchdog)
- controller/runtime_state.py, controller/process_controller.py(ETA 함수)
- runtime/chamber_runtime.py(start_with_recipe_string·시작 가드·호스트 요청 공정 로그 호출부)
- runtime/plasma_cleaning_runtime.py(start_with_recipe_string·_host_reject·호스트 요청 공정 로그 호출부)
- main.py(_netlog·_boot_host·_stop_host·호스트 팝업·클라이언트 공정 챗 sink), lib/config_common.py, config/settings.json

명령별 사실(인자·응답 필드·오류 코드·시퀀스)을 표로 모은 것은 robot_v1facts.py(COMMANDS·FRAME).
"""
from __future__ import annotations

_P = "v2/robot_server/protocol.py"
_S = "v2/robot_server/server.py"
_API = "v2/ch12/robot_api.py"
_OPS = "v2/ch12/robot_ops.py"
_CSV = "v2/ch12/outputs/robot_csv.py"


def _i(n: int, desc: str, v1: str, v2: str, note: str = "", status: str = "todo", test: str = "") -> dict:
    return dict(id=f"ROB-{n:03d}", area="robot", desc=desc, v1=v1, v2=v2,
                test=test, status=status, note=note)


ITEMS = [
    # ───────────────────────── 프레임 형식 ─────────────────────────
    _i(1, "헤더 16바이트 struct '!BBHIQ'(네트워크 바이트 순서·빅엔디언): [0] version u8, [1] flags u8, "
          "[2:4] cmd_len u16, [4:8] body_len u32, [8:16] timestamp u64. 헤더 뒤에 body_len 바이트 JSON 바디",
       "host/protocol.py:5-6; host/protocol.py:13-14", _P),
    _i(2, "응답 헤더 값: version=1(PROTOCOL_VERSION), flags=0, cmd_len=응답 명령 문자열의 UTF-8 바이트 수, "
          "body_len=바디 바이트 수, timestamp=int(time.time())(초 단위 유닉스 시각)",
       "host/protocol.py:15; host/protocol.py:33-38", _P),
    _i(3, "응답 바디 = {\"request_id\", \"command\", \"data\"} 이 순서의 객체를 json.dumps(ensure_ascii=False, "
          "default=str, 기본 구분자 ', ' ': ')로 UTF-8 인코딩. command 는 바디 안에만 있고 헤더 뒤에 따로 붙지 않음",
       "host/protocol.py:20-31", _P,
       note="바이트 단위 동일성: 한글은 이스케이프하지 않음, 직렬화 불가 값은 str()"),
    _i(4, "요청 해석: 헤더 16바이트를 정확히 읽은 뒤 body_len 바이트를 정확히 읽는다(EOF 면 ConnectionError). "
          "요청 헤더의 flags·cmd_len·timestamp 는 쓰지 않는다(명령은 JSON 의 command). 바디 길이 상한 없음",
       "host/server.py:162-169; host/server.py:186-209", _S),
    _i(5, "요청 version != 1 → E102 보고(log/chat/popup, detail 'Unsupported protocol version: N', meta peer) 후 "
          "응답 명령 'VERSION_ERROR_RESULT', request_id \"\" 로 보내고 그 연결을 닫는다(break)",
       "host/server.py:188-206", _S),
    _i(6, "바디 JSON 파싱 실패 → PLC_HOST 로그 '[REQ] peer=.. INVALID_JSON body=b'..' error=..', remote_cmd CSV 1줄"
          "(req_command '__INVALID_JSON__', req_data_json=바디 500자, res_command 'PARSE_ERROR_RESULT', res_result 'fail', "
          "res_message 'Invalid JSON: ..' 500자, duration_ms 0), E104 보고 후 'PARSE_ERROR_RESULT'·request_id \"\" 응답. "
          "연결은 유지(continue)",
       "host/server.py:211-268", _S),
    _i(7, "요청 필드: request_id=str(obj.get('request_id','')), command=str(obj.get('command','')), "
          "data=obj['data'] 가 dict 가 아니면 {}. 응답에 request_id 를 그대로 되돌린다",
       "host/server.py:270-273; host/server.py:436", _S),
    _i(8, "바디가 올바른 JSON 이지만 객체가 아니면(list·숫자 등) obj.get 에서 AttributeError → 응답 없이 "
          "NET 'Client error from {peer}: ..' 로그 후 연결 종료",
       "host/server.py:270; host/server.py:479-500", _S,
       note="알려진 이상 동작(v1 그대로 둘지 decisions 에 기록)"),
    _i(9, "요청 로그: PLC_HOST 태그 '[REQ] peer={peer} id={req_id} cmd={cmd} data={data}' (로그 실패 시 NET '[LOG_ERROR] ..')",
       "host/server.py:275-291", _S),
    _i(10, "command 가 빈 문자열 → remote_cmd CSV 1줄(req_command '__MISSING_COMMAND__', req_data_json=요청 전문 JSON, "
           "res_command 'UNKNOWN_RESULT', 'fail', \"Missing 'command' in request\"), E105 보고(meta peer·request_id), "
           "'UNKNOWN_RESULT' 응답(request_id 유지), 연결 유지",
       "host/server.py:293-329", _S),
    _i(11, "요청 컨텍스트: 바디 파싱이 끝난 시각을 received_at(datetime.now())으로, request_id·peer(str(peername))와 함께 "
           "ContextVar 에 넣고 dispatch 뒤 되돌린다 — Robot CSV 의 '요청시각'·request_id·peer 가 이 값",
       "host/server.py:331-342; host/server.py:362-363; host/request_ctx.py:18-54", _S),
    _i(12, "라우터: 명령을 upper().strip() 으로 정규화해 찾고, 응답 명령은 '{정규화된 명령}_RESULT' "
           "(예 'get_sputter_status' → 'GET_SPUTTER_STATUS_RESULT')",
       "host/router.py:19-20; host/router.py:35-37; host/router.py:63", _S),
    _i(13, "공백만 있는 명령(서버 검사는 통과) → 라우터가 'UNKNOWN_RESULT' + E105(detail 'Missing command', meta command) "
           "payload 를 돌려준다. 이 경로는 build_error_payload_from_code 라 log/chat/popup 보고가 없다",
       "host/router.py:39-44; errors/error_payload.py:27-62", _S),
    _i(14, "미등록 명령 → '{C}_RESULT' + E106(detail 'Unknown command: {원문}', meta {'command': 원문}). "
           "log/chat/popup 보고 없음",
       "host/router.py:46-51", _S),
    _i(15, "핸들러 예외 → NET 'Handler error for {cmd}: {e}\\n{traceback}' 로그, notify_unknown_error(E999, AppError/코드 "
           "예외면 그 코드, meta peer·request_id·cmd) 보고, 응답 명령은 정규화하지 않은 원문 f'{cmd}_RESULT'",
       "host/server.py:343-361; errors/error_payload.py:120-133", _S,
       note="알려진 이상 동작: 소문자로 온 명령이 예외 나면 응답 명령도 소문자+_RESULT"),
    _i(16, "핸들러가 dict 가 아닌 값을 돌려주면 라우터가 AppError E111(detail 'Handler returned non-dict: ..') → "
           "서버 except 에서 E111 로 응답. 서버 쪽에도 같은 방어(E111 notify)",
       "host/router.py:56-61; host/server.py:369-383", _S),
    _i(17, "result=='fail' 인데 error_code 가 없으면 NET '[WARN] fail without error_code: ..' 로그 후 E110 payload 로 교체",
       "host/server.py:385-400", _S),
    _i(18, "응답 직렬화 실패 → E107 보고 payload 로 다시 pack(같은 응답 명령)",
       "host/server.py:435-452", _S),
    _i(19, "연결마다 별도 태스크(asyncio.start_server), 한 연결 안 요청은 순차 처리(응답을 보낸 뒤 다음 헤더를 읽음) — "
           "VACUUM_ON 처럼 긴 명령 중에는 같은 연결의 다른 요청이 기다리고, 다른 연결의 요청은 동시에 처리됨",
       "host/server.py:145-148; host/server.py:184-186; host/server.py:343-344; host/server.py:454-455", _S),
    _i(20, "remote_cmd CSV 행은 응답 write/drain 이 끝난 뒤에 기록(응답이 CSV 쓰기를 기다리지 않음). "
           "duration_ms 는 dispatch 소요(perf_counter, int ms)",
       "host/server.py:331; host/server.py:365; host/server.py:402-415; host/server.py:454-462", _S),
    _i(21, "응답 로그: 응답 전송·CSV 뒤 PLC_HOST '[RES] peer={peer} id={req_id} cmd={res_cmd} data={res_data}'",
       "host/server.py:464-478", _S),
    _i(22, "연결 로그(NET): 'Client connected: {peer}', 정상 끊김(ConnectionError·ConnectionResetError·BrokenPipeError·"
           "IncompleteReadError·OSError 문구 '지정된 네트워크 이름을 더 이상 사용할 수 없습니다')은 'Client disconnected: {peer}', "
           "그 밖은 'Client error from {peer}: {e!r}', 끝에 항상 'Client closed: {peer}'",
       "host/server.py:172-173; host/server.py:479-505", _S),
    _i(23, "서버 시작 로그 'Host started on {sockname}', 종료 시 'Host closed' + 명령 CSV 핸들 닫기",
       "host/server.py:145-160", _S),
    _i(24, "바인드 주소 HOST_SERVER_HOST '0.0.0.0', HOST_SERVER_PORT 50070 — 메인 프로그램이 직접 이 포트로 호스트 서버를 띄운다",
       "lib/config_common.py:35-36; config/settings.json:25-26; main.py:1854-1856", _S),
    _i(25, "remote_cmd CSV 경로: <LOG_ROOT_DIR>\\CH1&2_Server\\remote_cmd_YYYYMMDD.csv (LOG_ROOT_DIR 기본 C:\\VanaM_Logs\\CH1&2), "
           "쓰기 실패 시 LOCAL_FALLBACK_SERVER_DIR(<exe 폴더>\\Logs_LocalFallback\\Server) 로 전환, 10 s 마다 primary 재시도, "
           "utf-8-sig, 핸들 유지, 빈 파일이면 헤더. 파일 날짜는 append 하는 순간의 날짜",
       "host/server.py:64-84; host/server.py:91-98; lib/config_common.py:20; lib/config_common.py:24; lib/config_common.py:645; "
       "util/log_hub.py:184-305", _S),
    _i(26, "remote_cmd CSV 10열: server_time, peer, request_id, req_command, req_data_json, res_command, res_result, "
           "res_message, res_data_json, duration_ms",
       "host/server.py:51-62", _S),
    _i(27, "remote_cmd 정상 행: server_time '%Y-%m-%d %H:%M:%S', peer=str((ip,port)), req_command=원문, req_data_json=data JSON, "
           "res_command, res_result=data.result, res_message=data.message, res_data_json=응답 data 전체 JSON(ensure_ascii=False, default=str)",
       "host/server.py:175-179; host/server.py:404-415", _S),
    _i(28, "remote_cmd 행 만들기 실패 시 res_command '__LOGGING_EXCEPTION__', res_result 'fail', res_message "
           "'Failed to build/append csv row: ..'(500자 절단) 행",
       "host/server.py:417-433", _S),
    _i(29, "primary→로컬 전환이 처음 일어난 그 쓰기 뒤 마커 행 1줄: req_command '__NAS_MISSING__', res_result 'WARN', "
           "res_message 'NAS write failed -> switched to LOCAL (keep-handle). reason=..'(500자 절단)",
       "host/server.py:100-119", _S),
    _i(30, "remote_cmd CSV 기록 실패는 모두 무시(응답·연결에 영향 없음)",
       "host/server.py:121-123; host/server.py:458-462", _S),

    # ───────────────────────── 로그·보고 ─────────────────────────
    _i(31, "PLC 입출력 로그: 핸들러의 PLC 호출 구간 동안 그 요청 태스크의 PLC 로그만 PLC_REMOTE 태그로 "
           "'{YYYY-mm-dd HH:MM:SS} [{명령 태그}] {msg}'(예 'read L_ATM (addr=99) -> True'). 다른 작업의 PLC 로그는 건드리지 않음",
       "host/handlers.py:171-195; host/handlers.py:261-277; device/plc.py:492-512; device/plc.py:1658-1675", _API),
    _i(32, "_plc_call: PLC I/O 한 구간마다 lock_plc 를 잡고 PLC 하트비트 워치독을 일시정지했다가 반드시 재개",
       "host/handlers.py:261-293; device/plc.py:2326-2342", _OPS),
    _i(33, "명령 태그는 ContextVar(코루틴별) — 동시 요청끼리 태그가 섞이지 않음, 중첩 시 이전 태그 복원",
       "host/handlers.py:25-28; host/handlers.py:104-112; host/handlers.py:247-259", _API),
    _i(34, "로그 라우팅: PLC_HOST·PLC_REMOTE·NET·HOST 태그와 '/HOST' 로 끝나는 태그(ERROR/HOST)는 전역 로그+서버 페이지에만, "
           "다른 페이지로 방송하지 않음",
       "main.py:652-673", _S),
    _i(35, "모든 실패 보고(notify_*·_fail): log('ERROR/HOST', '[코드 / client_action] title | 상세: detail'), "
           "chat_host.notify_error_event('HOST', 코드, '[코드] title | 상세: detail'), popup('오류(HOST, 코드)', 같은 본문)",
       "errors/error_reporter.py:27-107; errors/error_reporter.py:188-212; host/handlers.py:347-377; main.py:228", _API,
       note="구글챗은 v2 에서 포털 몫(plan 3-(7)) — 같은 계기·내용 유지 여부 decisions 에"),
    _i(36, "호스트 오류 팝업: QMessageBox Critical, 같은 (제목, 본문) 창이 열려 있으면 억제(HOST 로그 '(중복 억제) ..'), "
           "동시 5개 상한('(팝업 상한 초과) ..'), 15 s 뒤 자동 닫힘",
       "main.py:1910-1955", _API, note="v2 노트북 화면/포털 알람으로 대응 — decisions 에"),
    _i(37, "실패 응답 data 형식(키 순서): result 'fail', error_code, client_action, category, severity, "
           "message(=오류 레지스트리 title, 핸들러 문구 아님), detail(=핸들러 문구, 비면 레지스트리 cause), guide(=fix), "
           "[meta](있을 때만), [cause_type](예외 원인 있을 때). 모르는 코드는 E999 정의로",
       "errors/error_payload.py:27-117; errors/error_registry.py:1861-1867", _API),
    _i(38, "성공 응답 data 형식: {'result': 'success', 'message': 문구, **추가 필드(넘긴 순서)}",
       "host/handlers.py:296-300", _API),
    _i(39, "_fail 코드 결정: 명시 code > 예외의 .code/.error_code > KeyError 면 E411 > 그 밖 예외는 notify_handler_error"
           "(AppError 면 그 코드, 아니면 E110) > 코드 없는 문자열은 E110('Handler failure'). 이미 fail payload 면 그대로",
       "host/handlers.py:302-377; errors/error_payload.py:136-152", _API),
    _i(40, "fail meta: _plc_command(태그) 블록 안에서 난 실패에만 meta {'cmd': 태그}(태그 예 START_SPUTTER_CH1·"
           "START_PLASMA_CLEANING·VACUUM_ON·VACUUM_OFF·4PIN_UP·4PIN_DOWN·GATE_OPEN_CH2·GATE_CLOSE_CH1·CHUCK_MID_CH1·CHUCK_DOWN_CH2·"
           "GET_SPUTTER_STATUS); 예외 meta 에 다른 cmd 가 있으면 host_cmd 로. 블록 밖 실패(인자·busy 1차 검사·except 경로 일부)는 meta 없음",
       "host/handlers.py:247-259; host/handlers.py:331-345", _API,
       note="바이트 비교 대상 — 명령·실패 경로별 meta 유무는 robot_v1facts.COMMANDS 의 interlocks 설명 참조"),
    _i(41, "_log_client_request/_log_client_response/_append_line_nonblocking/_write_line_sync 와 PLC_Remote 폴더: "
           "_plc_cmd_file 이 항상 None 이라 아무것도 쓰지 않음(죽은 코드)",
       "host/handlers.py:84-93; host/handlers.py:114-169; host/handlers.py:198-245", _API, status="na",
       note="na 근거: host/handlers.py:84-85 주석과 :93·:259 에서 _plc_cmd_file=None 고정, :208·:229 에서 즉시 return; "
            "_append_line_nonblocking 은 호출부 없음(grep)"),

    # ───────────────────────── 등록·잠금 ─────────────────────────
    _i(42, "등록 명령 18개: GET_SPUTTER_STATUS, GET_LOADING_1_SENSOR, GET_LOADING_2_SENSOR, GET_RECIPE, START_SPUTTER, "
           "START_PLASMA_CLEANING, VACUUM_ON, VACUUM_OFF, 4PIN_UP, 4PIN_DOWN, CH1_GATE_OPEN, CH2_GATE_OPEN, CH1_GATE_CLOSE, "
           "CH2_GATE_CLOSE, CH1_CHUCK_UP, CH2_CHUCK_UP, CH1_CHUCK_DOWN, CH2_CHUCK_DOWN",
       "host/setup.py:49-66", _API),
    _i(43, "CHx_GATE_OPEN/CLOSE·CHx_CHUCK_UP/DOWN 은 라우터가 data 에 ch 를 덮어써 강제({**d, 'ch': x}) — 요청 data 의 ch 무시",
       "host/setup.py:59-66", _API),
    _i(44, "잠금: lock_plc(PLC I/O 직렬화), lock_ch1·lock_ch2(START_SPUTTER·게이트·척 채널별), "
           "_loadlock_transition_lock(VACUUM_ON/OFF 동시 진입 방지, 진행 중 태그 보관), _loadlock_gate_lock(게이트 OPEN/CLOSE 채널 공용 직렬화)",
       "host/setup.py:39-44; host/handlers.py:97-102", _OPS),
    _i(45, "busy 판정(_fail_if_ch_busy): runtime_state chamber(ch)·pc(ch)·(ch==1 이면 tsp) 실행, Runner _runner_state != IDLE "
           "(PREFLIGHT '공정 시작 준비 중'·RUNNING '공정 실행 중'·CLEANUP '공정 종료 처리 중'·STOPPING '공정 정지 중'·COOLDOWN '다음 공정 대기 중'·"
           "DELAY '공정 간 대기 중'·그 외 '처리 중 (st)'), stage task 살아있음(ADVANCE_QUEUE '리스트 공정 대기 중 (다음 공정 예약됨)'·PROCESS·PREFLIGHT·CLEANUP). "
           "사유를 ' / ' 로 이어 '{action} 불가 — ..' E205. 판정 중 예외면 통과(None)",
       "host/handlers.py:389-474", _OPS),
    _i(46, "로드락 전환 busy: _loadlock_transition_lock 이 잡혀 있으면 '{action} 불가 — {VACUUM_ON|VACUUM_OFF} 진행 중' E321",
       "host/handlers.py:379-386", _OPS),

    # ───────────────────────── GET_SPUTTER_STATUS ─────────────────────────
    _i(47, "GET_SPUTTER_STATUS 응답: message 'OK', Chamber_1, Chamber_2, Loadlock_Chamber(각 'idle'|'running'|'error'), "
           "vacuum(= not L_ATM, coil 99 M00063), eta. 즉시 응답(메모리 판정 + PLC 1비트 읽기, 채널 잠금 안 씀)",
       "host/handlers.py:532-685; device/plc.py:94", _API),
    _i(48, "챔버 상태: runtime_state chamber(ch) 또는 pc(ch) 실행 → running; Runner state != IDLE 또는 stage task 살아있음 → running; "
           "running 아니고 has_error('chamber', ch) → error; 조회 예외 → error; 그 외 idle. TSP 는 상태에 넣지 않음",
       "host/handlers.py:543-584", _API),
    _i(49, "Loadlock 상태: pc(1|2) 실행 → running; loadlock_error_channel 이 있으면 error; CH1 공정이 IG 대기(process_controller "
           "실행 중이고 current_step 이 None 이거나 action 값 'IG_CMD') → running; pc 런타임 is_running()(TypeError 면 _running) → "
           "running/idle; 예외 → error",
       "host/handlers.py:586-665", _API),
    _i(50, "loadlock_error_channel: 두 채널 중 last_finished('pc') 가 가장 최근인 채널이 has_error 면 그 채널(아니면 None); "
           "종료 이력이 하나도 없으면 has_error 인 채널(보수적). 채널 OR 가 아님(2026-09-23 사고)",
       "host/handlers.py:45-67; controller/runtime_state.py:232-259", _API),
    _i(51, "eta = {'CH1': {'remaining_s': ..}, 'CH2': {..}}: 런타임/컨트롤러 없음 → null, 큐 모드에서 마지막 행 전이면 null, "
           "아니면 eta_remaining_s() — ETA_ENABLED 이고 실행 중이고 확정됐고 종료절차·STOP·중단·비상·실패가 아니면 max(0, round(남은 초)), 그 외 null",
       "host/handlers.py:502-529; controller/process_controller.py:1763-1780", _API, note="계산은 v2/ch12/eta.py"),
    _i(52, "ETA 확정: 스텝이 DELAY 이고 문구가 'Shutter Delay' 또는 '메인 공정 진행' 으로 시작할 때 1회, 남은 DELAY(no_wait 제외) 합 + "
           "ETA_TAIL_S 120 s; 종료절차·중단·비상 중이나 두께 모드(THICKNESS_WAIT 스텝 있음)면 확정 안 함; 확정 실패 시 0",
       "controller/process_controller.py:1717-1761; controller/process_controller.py:1168-1170; lib/config_common.py:851-855", "v2/ch12/eta.py"),
    _i(53, "GET_SPUTTER_STATUS 의 PLC 읽기 실패 → _fail(e, code=e.code) (KeyError E411, 그 밖 E110), meta 없음",
       "host/handlers.py:671-688", _API),

    # ───────────────────────── GET_LOADING_n_SENSOR ─────────────────────────
    _i(54, "GET_LOADING_1_SENSOR / GET_LOADING_2_SENSOR: LOADING_1_SENSOR_LAMP(coil 480, M00300) / LOADING_2_SENSOR_LAMP(481, M00301) "
           "1비트 읽어 {'message': 'OK', 'value': bool}. KeyError → E411 'PLC 주소맵에 {key}가 없습니다: ..', 그 밖 → e.code 또는 E412. "
           "즉시 응답, 읽기는 태그 블록 밖(실패 meta 없음)",
       "host/handlers.py:732-762; device/plc.py:190-191", _API),

    # ───────────────────────── GET_RECIPE ─────────────────────────
    _i(55, "GET_RECIPE: data.folder 를 strip().upper(); ROBOT_RECIPE_FOLDERS(기본 CH1, CH2, ALD) 에 없으면 E226 "
           "'folder는 (..) 중 하나여야 합니다. (입력=..)'. ROBOT_RECIPE_ROOT_DIR(\\\\VanaM_NAS\\VanaM_toShare\\JH_Lee\\Recipe)\\folder 를 "
           "재귀 없이 스캔해 확장자 .csv/.xlsx(대소문자 무시) 파일 이름만, 소문자 기준 정렬. 응답 message 'OK', base_dir, folder, files, count",
       "host/handlers.py:691-724; lib/config_common.py:562-565; config/settings.json:381-382", _API),
    _i(56, "GET_RECIPE 스캔은 스레드에서, RECIPE_SCAN_TIMEOUT_S(20 s, 코드 기본 8.0) 넘으면 E227 'GET_RECIPE timeout (20.0s)'; "
           "폴더 없음/폴더 아님 → FileNotFoundError/NotADirectoryError → E110(detail 'Recipe folder not found: ..')",
       "host/handlers.py:706-729; lib/config_common.py:568; config/settings.json:383", _API),

    # ───────────────────────── START_SPUTTER ─────────────────────────
    _i(57, "START_SPUTTER 인자: ch=int(data.ch or 0)(문자 '1'·실수 1.9 도 int 변환), recipe=str(data.recipe or '').strip(). "
           "검사 순서: ch∉{1,2} E201 'ch는 1 또는 2만 허용합니다.' → recipe 빈칸 E202 → 런타임 없음 E203 'Chamber CHn runtime not ready' → "
           "busy E205(action 'START_SPUTTER_CHn') → lock_chN 획득 → busy 재확인 E205",
       "host/handlers.py:839-869", _API),
    _i(58, "START_SPUTTER ch 가 int 로 안 바뀌는 값('abc') → impl 에서 ValueError → 래퍼가 Robot CSV 거절 '핸들러 예외: ..' 기록 후 "
           "다시 던짐 → 서버 E999 응답",
       "host/handlers.py:817-827; host/handlers.py:840; host/server.py:345-361", _API, note="알려진 이상 동작"),
    _i(59, "START_SPUTTER 인터락(lock_chN·태그 START_SPUTTER_CHn 안): 게이트 램프 상태가 closed 가 아니면 E301 "
           "'START_SPUTTER 불가 — CHn gate가 CLOSED가 아님(state)'; Main Valve(M_V_n_SW AND M_V_n_인터락) 가 open 이 아니면 E301 "
           "'.. Main Valve가 열려있지 않음(state=.., sw=.., interlock=..)'. 읽기 KeyError E411, 그 밖 E412",
       "host/handlers.py:871-897; host/handlers.py:1035-1060", _OPS),
    _i(60, "START_SPUTTER 성공 응답: message 'SPUTTER START OK', ch, min_total_s. 런타임 start_with_recipe_string(origin='host', "
           "origin_meta {key: Robot CSV 기록키, request_id, peer}) 이 돌아온 뒤 응답",
       "host/handlers.py:899-914", _API),
    _i(61, "START_SPUTTER 응답 시점: 시작 가드 결과(_host_report_start — 프리플라이트 장비 연결 OK/실패, TEST MODE, delay 행 수락)를 "
           "CHAMBER_HOST_START_WAIT_TIMEOUT_S(20 s, 코드 기본 10) 동안 기다림. 넘으면 RuntimeError 'preflight timeout (20.0s) ..' → E700. "
           "OK 면 응답하고 공정은 계속 진행. 기다리는 동안 lock_chN 을 쥐고 있음",
       "runtime/chamber_runtime.py:7943-7946; runtime/chamber_runtime.py:8109-8129; runtime/chamber_runtime.py:4202-4206; "
       "runtime/chamber_runtime.py:3837; runtime/chamber_runtime.py:5133; lib/config_common.py:763; host/handlers.py:865", _API),
    _i(62, "START_SPUTTER RuntimeError 코드 분류: 예외 .code 우선 → 문구가 '레시피 오류' 로 시작하면 E230 → '1분 대기'/'cooldown'/"
           "'preflight timeout' 포함이면 E700(RETRY_LATER) → 그 밖 E511. RuntimeError 아닌 예외는 _fail(e)",
       "host/handlers.py:915-931", _API),
    _i(63, "챔버 런타임 시작 거절 문구(→ 코드): 'cooldown Ns remaining'(종료 후 60 s, E700), 'cleanup pending', 'runner busy: ..', "
           "'this chamber already running', 'CHn Plasma Cleaning 실행 중', 'process controller busy', 'invalid inputs', 입력 검증 오류 문구, "
           "'exception: ..', '장비 연결 실패: ..'(모두 E511)",
       "runtime/chamber_runtime.py:4375-4416; runtime/chamber_runtime.py:4435; runtime/chamber_runtime.py:4478; "
       "runtime/chamber_runtime.py:4493; runtime/chamber_runtime.py:4206", _API),
    _i(64, "START_SPUTTER 레시피 경로: .csv/.xlsx 만(그 외 E209 '지원하지 않는 레시피 형식..'), 파일 없음 E207, 로드 30 s 초과 E227, "
           "읽기 실패(인코딩·잠김) E209, 데이터 없음 E208, 레시피 값 오류 → '레시피 오류 N건:' RuntimeError(E230). "
           "xlsx 는 첫 시트, 빈 행 건너뜀, Process_name 없으면 '#' 열 또는 '공정 N'",
       "runtime/chamber_runtime.py:8003-8107", _API),
    _i(65, "min_total_s(START_SPUTTER 하한 보증 초): 남은 큐 rows=queue[max(0, current_process_index):] 에 대해 Σ(delay 행이면 대기 초 / "
           "두께 모드 행이면 shutter_delay×60 / 그 외 (shutter_delay + process_time, process_time≤0 이고 dep_rate·thickness 유효하면 "
           "thickness÷dep_rate÷60)×60) + ETA_TAIL_MIN_S 90 → round, 최소 0. ETA_ENABLED=False·큐 없음·예외면 null",
       "host/handlers.py:477-500; controller/process_controller.py:94-198; controller/process_controller.py:1782-1807; "
       "lib/config_common.py:859", _API, note="계산은 v2/ch12/eta.py"),
    _i(66, "preflight timeout 으로 E700 을 응답해도 asyncio.wait_for 는 Future 만 취소하고 Runner 는 계속 진행 → 응답은 실패인데 공정이 "
           "시작될 수 있음(Robot CSV 키는 이미 러너 소유라 거절 줄 없이 런타임이 나중에 결과 기록)",
       "runtime/chamber_runtime.py:8114-8125; runtime/chamber_runtime.py:4426; runtime/chamber_runtime.py:4485; host/handlers.py:794-803",
       _API, note="알려진 이상 동작 — v1 과 같게 두고 decisions 에 기록"),

    # ───────────────────────── START_PLASMA_CLEANING ─────────────────────────
    _i(67, "START_PLASMA_CLEANING: recipe 빈칸 E202, pc 런타임 없음 E204 'Plasma Cleaning runtime not ready'. 핸들러는 busy·게이트·"
           "채널 잠금 검사를 하지 않고 런타임 결과만 돌려준다. 성공 message 'PLASMA CLEANING START OK'(추가 필드·ETA 없음)",
       "host/handlers.py:951-989", _API),
    _i(68, "START_PLASMA_CLEANING RuntimeError 분류: .code 우선 → '1분 대기'/'cooldown'/'preflight timeout' → E700 → 그 밖 E511 "
           "(E230 분류 없음). 다른 예외 _fail(e)",
       "host/handlers.py:990-1003", _API),
    _i(69, "PC 런타임 호스트 시작: 이전 Host start 대기 중이면 E206; recipe 는 .csv 만(.xlsx 포함 그 외 E209), 없음 E207, 로드 30 s 초과 E227, "
           "읽기 실패 E209, 데이터 행 없음 E208, 실행 중 거절 E205(_host_reject), 게이트 인터락 E302; 결과 대기 PC_HOST_START_WAIT_TIMEOUT_S 20 s "
           "넘으면 'preflight timeout(20.0s) ..' → E700; 거절 코드 없으면 E511",
       "runtime/plasma_cleaning_runtime.py:3205-3301; runtime/plasma_cleaning_runtime.py:1197-1275; "
       "runtime/plasma_cleaning_runtime.py:1494; runtime/plasma_cleaning_runtime.py:1510; lib/config_common.py:826", _API),

    # ───────────────────────── VACUUM_ON ─────────────────────────
    _i(70, "VACUUM_ON 인자 timeout_s 기본 660 s(PLC T0052 600 s + 60 s). 숫자가 아니면 try 밖 ValueError → E999. "
           "응답은 시퀀스가 끝난 뒤(완료 후 응답)",
       "host/handlers.py:1239", _OPS),
    _i(71, "VACUUM_ON: 다른 VACUUM_ON/OFF 진행 중이면 E321 즉시 거절(원복 없음, meta cmd VACUUM_ON)",
       "host/handlers.py:1266-1271", _OPS),
    _i(72, "VACUUM_ON/OFF 공통 게이트 인터락: CH1·CH2 게이트가 모두 closed 여야 함. 아니면 E301 'VACUUM_ON/OFF 불가 — CHn gate가 "
           "CLOSED가 아님(state)', 읽기 KeyError E411, 그 밖 'Gate 상태 조회 실패: ..' e.code 또는 E412",
       "host/handlers.py:1062-1078; host/handlers.py:1277-1279; host/handlers.py:1702-1704", _OPS),
    _i(73, "VACUUM_ON 이미 L_VAC_READY_SW=TRUE: 러핑(L_R_P_SW·L_R_V_SW) 둘 다 OFF 면 즉시 성공 'VACUUM_ON: 이미 L_VAC_READY_SW=TRUE 상태'; "
           "아니면 L_R_V_SW OFF → 5 s → L_R_P_SW OFF 후 10 s 동안 1 s 간격 확인 → 성공 '.. 정리 완료' / E312",
       "host/handlers.py:1281-1313", _OPS),
    _i(74, "VACUUM_ON 시작 시퀀스: L_VENT_SW OFF → 0.3 s → L_R_P_OFF_TIMER TRUE 면 E309 '러핑펌프 OFF 타이머 진행 중 → 잠시 후 재시도' → "
           "LP_STEP1 또는 LP_STEP2 TRUE 면 PLC 시퀀스 진행 중으로 보고 쓰기 전부 건너뜀 → 아니면 L_R_P_SW ON → 5 s → L_R_V_인터락 FALSE 면 "
           "러핑 정리 후 E310 → L_R_V_SW ON",
       "host/handlers.py:1315-1356", _OPS),
    _i(75, "VACUUM_ON 폴링: timeout_s 까지 1 s 간격으로 6비트 스냅샷(L_VAC_READY_SW 98, L_VAC_NOT_READY 100, LP_STEP1 96, LP_STEP2 97, "
           "L_R_P_SW 208, L_R_V_SW 209 — 개별 read_bit, 원자적 아님)",
       "host/handlers.py:1106-1132; host/handlers.py:1358-1378; host/handlers.py:1624; device/plc.py:91-95; device/plc.py:139-140", _OPS),
    _i(76, "VACUUM_ON READY 판정(성공 우선): PLC 가 러핑 OFF 하는지 5 s 동안 0.2 s 간격 확인 → 성공 'VACUUM_ON 완료 — PLC의 L_VAC_READY_SW=TRUE 및 "
           "러핑 OFF 완료 확인'; 안 되면 fallback(L_R_V_SW OFF→5 s→L_R_P_SW OFF) 후 10 s/1 s 확인 → 성공 '.. fallback으로 .. 정리 완료' / E312",
       "host/handlers.py:1387-1422", _OPS),
    _i(77, "VACUUM_ON L_VAC_NOT_READY=TRUE → E312 '.. PLC가 L_VAC_NOT_READY=TRUE로 판정 (LP_STEP1=.., ..)'",
       "host/handlers.py:1424-1431", _OPS),
    _i(78, "VACUUM_ON 러핑 중 게이트 감시: 매 폴링 CH1·CH2 게이트가 closed 아니면 0.5 s 뒤 그 채널 재확인, 여전히면 진단 로그 "
           "'[VACUUM_ON_DIAG/gate_not_closed]' 후 E301 'VACUUM_ON 중단 — 러핑 진행 중 CHn gate가 CLOSED가 아님(..)'. 감시 읽기 예외는 무시",
       "host/handlers.py:1433-1471", _OPS),
    _i(79, "VACUUM_ON 펌프 ON·밸브 OFF 상태: 5 s grace(transition_grace_s) 넘게 지속되면 진단 로그 '[VACUUM_ON_DIAG/stop_seq_stall]' 후 "
           "L_R_P_OFF_TIMER=TRUE → E312(에어압 알람), LP_STEP1 & !LP_STEP2 → E312(T0050 정상 경로 아님), 그 외 E312(READY 미도달). "
           "펌프·밸브 둘 다 ON 이면 grace 초기화",
       "host/handlers.py:1473-1519", _OPS),
    _i(80, "VACUUM_ON 펌프·밸브 둘 다 OFF: LP_STEP1/2 진행 중이면 별도 15 s 한도(넘으면 '[VACUUM_ON_DIAG/lp_step_stall]' + E312), "
           "아니면 5 s grace(both_off_grace_s) 뒤 재스냅샷 — READY 면 0.2 s 뒤 계속, NOT_READY 면 E312, 그 외 E312(원인 미확정, "
           "L_GAUGE_A_INTERLOCK 값은 '미매핑'이 아닐 때만 문구에)",
       "host/handlers.py:1521-1606", _OPS),
    _i(81, "VACUUM_ON 펌프 OFF·밸브 ON → 진단 '[VACUUM_ON_DIAG/pump_off_valve_on]' 후 즉시 E312",
       "host/handlers.py:1608-1622", _OPS),
    _i(82, "VACUUM_ON 타임아웃: 스냅샷+진단 '[VACUUM_ON_DIAG/timeout]' 후 NOT_READY 면 E312 '{N}s 타임아웃, .. L_VAC_NOT_READY ..', "
           "스냅샷 있으면 E312 'VACUUM_ON 타임아웃 — {N}s 내 진공 미도달 ..', 스냅샷 실패면 E312 '(상태 스냅샷 읽기 실패)'",
       "host/handlers.py:1626-1664", _OPS),
    _i(83, "VACUUM_ON 진단 12비트(L_GAUGE_A·L_GAUGE_A_INTERLOCK·L_R_P_OUT·L_R_V_OUT·L_VENT_OUT·L_ATM_SENSOR·L_R_P_OFF_TIMER·L_R_V_인터락·"
           "G_V_1/2_OPEN/CLOSE_LAMP): 주소맵에 없으면 '미매핑'(읽지 않음), 읽기 예외 '읽기실패'. P 영역 5개는 PLC 미러 코일(803·848·849·852·809)",
       "host/handlers.py:1134-1203; device/plc.py:227-232", _OPS),
    _i(84, "VACUUM_ON 실패·예외 원복: 성공이 아니고 정리를 아직 안 했으면 L_R_V_SW OFF → 5 s → L_R_P_SW OFF(finally, 전환 잠금 해제 뒤). "
           "게이트 거절 E301·E309 같은 쓰기 전 실패에도 돌아 응답이 5 s 늦어짐",
       "host/handlers.py:1244-1264; host/handlers.py:1672-1676", _OPS, note="알려진 이상 동작(거절에도 5 s 지연·OFF 쓰기)"),
    _i(85, "VACUUM_ON 예외 → _fail(e)(KeyError E411, 그 밖 E110/AppError 코드), meta cmd VACUUM_ON",
       "host/handlers.py:1669-1670", _OPS),

    # ───────────────────────── VACUUM_OFF ─────────────────────────
    _i(86, "VACUUM_OFF 인자 timeout_s 기본 240 s. 전환 busy E321, 게이트 인터락 E301(ROB-072). 이미 L_ATM=TRUE 면 L_VENT_SW OFF·L_R_V_SW OFF → "
           "5 s → L_R_P_SW OFF 후 성공 'VACUUM_OFF: 이미 대기압 상태 (L_ATM=TRUE)'. 완료 후 응답",
       "host/handlers.py:1687-1718", _OPS),
    _i(87, "VACUUM_OFF 시퀀스: L_R_V_SW OFF → 5 s → L_R_P_SW OFF → L_VENT_인터락 FALSE 면 E311 'L_VENT_인터락=FALSE → 벤트 불가' → L_VENT_SW ON → "
           "0.5 s 간격 L_ATM 폴링 → TRUE 면 L_VENT_SW OFF 후 성공 'VACUUM_OFF 완료 (L_ATM=TRUE, L_VENT_SW=FALSE)' → 시간 초과면 L_VENT_SW OFF 후 "
           "E313 'VACUUM_OFF 타임아웃: {N}s 내 L_ATM TRUE 미도달 (N2 gas 부족)'",
       "host/handlers.py:1720-1762", _OPS),
    _i(88, "VACUUM_OFF 실패·예외 원복: 성공이 아니면 L_VENT_SW OFF. 예외 → _fail(e). L_R_P_OFF_TIMER 는 검사하지 않음",
       "host/handlers.py:1767-1774", _OPS),

    # ───────────────────────── 4PIN ─────────────────────────
    _i(89, "4PIN_UP: 인자 wait_s 20, poll_s 1.0, settle_s 5.0. L_PIN_인터락(coil 50) FALSE → E314 'L_PIN_인터락=FALSE → 4PIN_UP 불가'; "
           "L_PIN_UP_SW(210) 펄스 → settle_s 대기 → L_PIN_UP_LAMP(370) 폴링 → 성공 '4PIN_UP 완료 — L_PIN_UP_LAMP=TRUE (timeout 20s, poll 1.0s)' / "
           "E316 '4PIN_UP 실패 — 20s 내 ..'. busy·잠금·게이트 검사 없음. 완료 후 응답(최대 약 25 s)",
       "host/handlers.py:1777-1819; device/plc.py:70; device/plc.py:141; device/plc.py:180", _OPS),
    _i(90, "4PIN_DOWN: 같은 구조, L_PIN_DOWN_SW(211)·L_PIN_DOWN_LAMP(371), 인터락 FALSE E315, 시간 초과 E317. 예외 _fail(e)",
       "host/handlers.py:1821-1862; device/plc.py:142; device/plc.py:181", _OPS),
    _i(91, "_poll_bit_until_true: 읽어서 TRUE 면 즉시 True, 마감 지났으면 False, 아니면 min(interval, 남은 시간) 만큼 대기(마감 직전 1회 더 읽음)",
       "host/handlers.py:1864-1886", _OPS),
    _i(92, "PLC 펄스 명령(press_switch): 코일 ON → max(0.03 s, PLC_CMD_PULSE_MS 180 ms) → OFF. write_switch(on) 는 유지 쓰기",
       "device/plc.py:1633-1637; device/plc.py:1658-1669; lib/config_common.py:637; config/settings.json:263", _OPS),

    # ───────────────────────── GATE OPEN ─────────────────────────
    _i(93, "CHx_GATE_OPEN 검사 순서: 인자 wait_s 기본 5 s → busy E205 → 로드락 전환 busy E321 → (ch 1·2 아니면 E201) → lock_chN → busy 재확인 → "
           "_loadlock_gate_lock → 전환 busy 재확인 E321 → 태그 GATE_OPEN_CHn",
       "host/handlers.py:1897-1932", _OPS),
    _i(94, "게이트 상태 판정: CLOSE_LAMP & !OPEN_LAMP → closed, OPEN & !CLOSE → open, 둘 다 FALSE → moving_or_unknown, 둘 다 TRUE → invalid_both_true. "
           "CH1 G_V_1_OPEN/CLOSE_LAMP 360/361, CH2 392/393",
       "host/handlers.py:1006-1033; device/plc.py:178-179; device/plc.py:186-187", _OPS),
    _i(95, "GATE_OPEN: invalid_both_true → E306; 이미 open → CH1 성공 'CH1_GATE_OPEN: 이미 OPEN 상태' + current(상태 dict); CH2 는 "
           "MAIN_SHUTTER_2_인터락(74) FALSE 면 E330, 아니면 MAIN_SHUTTER_2_SW(234) ON(유지) 후 성공 'CH2_GATE_OPEN: 이미 OPEN + MAIN_SHUTTER_OPEN' + current "
           "(로드락·반대 게이트 검사 생략)",
       "host/handlers.py:1935-1954; device/plc.py:1757-1760", _OPS),
    _i(96, "GATE_OPEN 로드락 인터락: L_VENT_SW·L_R_P_SW·L_R_V_SW·L_ATM 중 하나라도 TRUE 면 E321 'Loadlock 상태로 인해 GATE_OPEN 불가 (k=TRUE, ..)' "
           "(L_VAC_READY_SW 는 검사 안 함); 반대 챔버 게이트 closed 아니면 E303; G_V_n_인터락(40/72) FALSE 면 E302",
       "host/handlers.py:1080-1104; host/handlers.py:1956-1974", _OPS),
    _i(97, "GATE_OPEN 시퀀스: G_V_n_OPEN_SW(200/232) 펄스 → wait_s 대기(락 없음) → OPEN_LAMP 1회 읽기, FALSE 면 E304 'CHn_GATE_OPEN 실패 — .. (대기 5s)' "
           "(셔터 안 염) → CH2 는 MAIN_SHUTTER_2_인터락 확인(E330) 후 셔터 ON, 성공 'CH2_GATE_OPEN 완료 — G_V_2_OPEN_LAMP=TRUE + MAIN_SHUTTER_OPEN (대기 5s)'; "
           "CH1 'CH1_GATE_OPEN 완료 — G_V_1_OPEN_LAMP=TRUE (대기 5s)'. 완료 후 응답",
       "host/handlers.py:1976-2000", _OPS),
    _i(98, "GATE_OPEN 예외 → e.code 또는 E412(KeyError 도 E412, E411 아님), 태그 블록 밖이라 meta cmd 없음",
       "host/handlers.py:2002-2006", _OPS, note="다른 명령은 KeyError→E411 — 차이 유지"),

    # ───────────────────────── GATE CLOSE ─────────────────────────
    _i(99, "CHx_GATE_CLOSE 검사 순서: wait_s 기본 5 → busy E205 → (E201) → lock_chN → busy 재확인 → _loadlock_gate_lock → 태그 GATE_CLOSE_CHn. "
           "로드락 전환(VACUUM 진행 중) 검사는 하지 않음",
       "host/handlers.py:2018-2051", _OPS),
    _i(100, "GATE_CLOSE: 이미 closed → CH2 는 MAIN_SHUTTER_2_SW OFF 후 'CH2_GATE_CLOSE: 이미 CLOSED + MAIN_SHUTTER_CLOSE' + current, "
            "CH1 'CH1_GATE_CLOSE: 이미 CLOSED 상태' + current; invalid_both_true → E306",
       "host/handlers.py:2053-2068", _OPS),
    _i(101, "GATE_CLOSE 인터락: LOADING_n_SENSOR_LAMP FALSE 면 닫기 명령을 보내지 않고 E305 'CHn_GATE_CLOSE 불가 — .. (arm/loading sensor 미감지, "
            "gate close 명령 미전송)'; 읽기 KeyError E411, 그 밖 E412",
       "host/handlers.py:2070-2090", _OPS),
    _i(102, "GATE_CLOSE 시퀀스: G_V_n_CLOSE_SW(201/233) 펄스 → wait_s → CLOSE_LAMP·LOADING 센서 읽기 → CH2 는 MAIN_SHUTTER_2_SW OFF(실패해도 계속) → "
            "성공(CH2 '.. + MAIN_SHUTTER_CLOSE (대기 5s)'), 성공인데 셔터 실패 E331, 램프 FALSE 면 E305('.. LOADING_n_SENSOR_LAMP=.. (대기 5s)', CH2 '+ MAIN_SHUTTER_CLOSE 시도 완료'), "
            "램프 FALSE + 셔터 실패 E331. 예외 → _fail(e)(E411/E110), meta 있음",
       "host/handlers.py:2092-2142", _OPS),

    # ───────────────────────── CHUCK ─────────────────────────
    _i(103, "CHx_CHUCK_UP = MID 위치로 이동(UP 아님): CH1 Z_M_P_1_SW(218)+Z_M_P_1_MID_SW(176)→Z1_MID_LOCATION(129), CH2 Z_M_P_2_SW(221)+"
            "Z_M_P_2_MID_SW(179)→Z2_MID_LOCATION(132). 인자 wait_s 기본 90 s. busy E205(action 'CHn_CHUCK_UP')",
       "host/handlers.py:2145-2168; device/plc.py:105-117; device/plc.py:148-153", _OPS),
    _i(104, "CHx_CHUCK_DOWN: Z_M_P_n_SW + Z_M_P_n_CCW_SW(220/223) → Zn_DOWN_LOCATION(130/133), wait_s 기본 90 s, busy E205",
       "host/handlers.py:2170-2193", _OPS),
    _i(105, "척 위치 판정: Zn_UP/MID/DOWN_LOCATION 램프 3개 중 정확히 하나만 TRUE 일 때 up/mid/down, 아니면 unknown. 응답 current = "
            "{'position': .., 'lamp': {'up','mid','down'}}",
       "host/handlers.py:2195-2216", _OPS),
    _i(106, "척 이동 앞부분: lock_chN → busy 재확인(action 'CHn_CHUCK_MID'/'CHn_CHUCK_DOWN') → 태그 CHUCK_{MID|DOWN}_CHn → 현재 위치 읽기 실패 E412 → "
            "MID 목표인데 unknown 이면 1 s 간격 CHUCK_POSITION_RESOLVE_TIMEOUT_S(90 s) 동안 확정 대기 → 이미 목표면 성공 'CHn Chuck OK — 이미 MID 위치' + current → "
            "MID 목표인데 끝까지 unknown 이면 E318('.. 위치 불명 상태 지속(90s ..) → MID 이동 불가 ..')",
       "host/handlers.py:2226-2274; lib/config_common.py:757; config/settings.json:365", _OPS),
    _i(107, "척 이동: Z-POWER ON → 0.2 s → 방향 ON(유지) → 1 s 간격 목표 램프 폴링 → TRUE 면 같은 PLC 구간에서 방향 OFF·POWER OFF 후 위치 읽어 "
            "성공 'CHn Chuck MID 도달'(또는 DOWN) + current → timeout_s 넘으면 둘 다 OFF 후 E318 'CHn Chuck MID 타임아웃(90s) — .. snapshot=..' → "
            "예외면 둘 다 OFF 후 E412. 완료 후 응답",
       "host/handlers.py:2276-2327", _OPS),
    _i(108, "척 명령은 게이트·로드락·핀 상태를 검사하지 않고 _loadlock_gate_lock 도 쓰지 않음(채널 잠금과 busy 만)",
       "host/handlers.py:2145-2327", _OPS),

    # ───────────────────────── 호스트 수명·화면 연결 ─────────────────────────
    _i(109, "호스트 서버 기동: 프로그램 시작 시 _boot_host(이미 떠 있으면 'Host already running'), 성공 시 서버 페이지 RUNNING·NET 'Host started on "
            "0.0.0.0:50070', 실패 시 'Host start failed: ..'. 서버 페이지 Start/Stop/Restart 버튼 → request_host_start/stop/restart(이전 작업 취소)",
       "main.py:642-682; main.py:1841-1894; main.py:1896-1908; main.py:1957-1977; main.py:390-393", _S),
    _i(110, "프로그램 종료 시 호스트 서버 종료를 최대 3 s 기다림",
       "main.py:1386-1388", _S),

    # ───────────────────────── Robot_YYYYMMDD.csv (규약 v3.1) ─────────────────────────
    _i(111, "Robot CSV 파일 배치: NAS <HOST_LOG_NAS_DIR>\\Robot_YYYYMMDD.csv(기본 \\\\VanaM_NAS\\VanaM_toShare\\JH_Lee\\Logs\\Robot, ALD 와 공용·append 전용), "
            "잠금 <NAS>\\_lock\\Robot.lock; 로컬 <HOST_LOG_LOCAL_DIR>(기본 C:\\VanaM_Logs\\Robot)\\Robot_YYYYMMDD_sputter.csv(내 줄 사본 15칸), "
            "pending_sputter.csv(NAS 미전송 16칸), open_sputter.json(미종료 요청)",
       "util/host_process_log.py:12-17; util/host_process_log.py:46-47; util/host_process_log.py:177-209; lib/config_common.py:677-681; "
       "config/settings.json:282-287", _CSV),
    _i(112, "Robot CSV 15열(ALD 공통, 변경 금지): 대상, 요청시각, 시작시각, 종료시각, 소요(분), 결과, 사유, 공정명, 레시피, 행개수, request_id, "
            "peer, 로그파일, 프로그램, 기록키",
       "util/host_process_log.py:40-44", _CSV),
    _i(113, "결과 문자열: '성공', '실패', 'STOP', '거절', '중단(재시작)', '미확인'. 프로그램 열 값 'sputter'",
       "util/host_process_log.py:38; util/host_process_log.py:49-54", _CSV),
    _i(114, "파일 날짜 = 요청 수신 날짜(자정을 넘겨 끝나도 요청 날짜 파일), received_at 없으면 종료 시각 날짜",
       "util/host_process_log.py:19; util/host_process_log.py:398-401; util/host_process_log.py:540-541", _CSV),
    _i(115, "기록키 발급(request): 'sputter-YYYYMMDDHHMMSS{ms 3자리}-{request_id 정제}' — 정제는 [A-Za-z0-9_-] 외 '_', 64자 절단; open·pending·직전 발급 키와 "
            "겹치면 '-2','-3'.. 접미사. open 에 {key,target,request_id,peer,received_at(ISO ms),started_at '',recipe_name,row_count,process_names,log_files []} 등록",
       "util/host_process_log.py:56-63; util/host_process_log.py:285-321", _CSV),
    _i(116, "update: open 항목의 recipe_name·row_count·process_names·log_files 만 갱신(None 은 무시)",
       "util/host_process_log.py:323-340", _CSV),
    _i(117, "mark_started: started_at 은 처음 한 번만(ISO ms), log_file 은 중복 없이 추가; 처음일 때 'started' 이벤트 1회",
       "util/host_process_log.py:342-370", _CSV),
    _i(118, "mark_owned/is_owned: 런타임이 요청을 수락했다는 메모리 표시(재시작하면 사라짐)",
       "util/host_process_log.py:372-383", _CSV),
    _i(119, "finalize(멱등 — open 에 키 없으면 False): 줄 생성 → ① pending 에 [요청날짜]+15칸 append(끊긴 줄 보정·flush·fsync) → 실패하면 메모리 큐로 넘기고 "
            "open 유지 → ② 로컬 사본 append(실패 무시) → ③ open 에서 제거·owned 해제 → ④ 워커 깨움. 'finished' 이벤트 기록키당 1회(락 밖)",
       "util/host_process_log.py:385-439", _CSV),
    _i(120, "줄 생성 규칙: 시각은 HH:MM:SS(없으면 빈칸), 시작 안 한 요청은 종료시각·소요 빈칸, 소요 = (종료-시작)/60 소수 1자리, 사유는 성공이면 빈칸 아니면 "
            "CR/LF 를 ' | ' 로 한 줄, 로그파일은 ' / ' 로 연결, 프로그램 'sputter', 마지막 열 기록키",
       "util/host_process_log.py:66-75; util/host_process_log.py:445-471", _CSV),
    _i(121, "reject(key, 사유) = finalize(key, '거절', 사유)",
       "util/host_process_log.py:441-442", _CSV),
    _i(122, "CSV 한 줄은 csv.writer RFC4180, 줄끝 CRLF. pending/사본/NAS 모두 같은 방식",
       "util/host_process_log.py:108-113", _CSV),
    _i(123, "끊긴 줄 보정: 이어쓰기 전에 파일 끝이 '\\n' 이 아니면 '\\r\\n' 먼저 씀(없는 파일·빈 파일·BOM 만 있는 파일은 손대지 않음, 실패해도 진행)",
       "util/host_process_log.py:116-138", _CSV),
    _i(124, "로컬 사본(Robot_YYYYMMDD_sputter.csv)은 새 파일이면 utf-8-sig(BOM)+헤더, 있으면 utf-8 로 끊긴 줄 보정 후 append",
       "util/host_process_log.py:487-497", _CSV),
    _i(125, "open_sputter.json 과 pending 재작성은 원자적(.tmp 에 쓰고 flush·fsync → os.replace). open 은 json indent=1, ensure_ascii=False",
       "util/host_process_log.py:98-105; util/host_process_log.py:212-229; util/host_process_log.py:511-517", _CSV),
    _i(126, "pending 읽기: 16칸 이상인 행만, 파일 순서 그대로; pending 키 = 각 행 마지막 칸",
       "util/host_process_log.py:499-520", _CSV),
    _i(127, "startup_recover(프로그램 시작 시, 로컬만 봄): open 중 pending 에 이미 있는 키는 open 에서만 제거, 나머지는 '중단(재시작)' 줄(사유 "
            "'프로그램 재시작 (공정 중)' 또는 '(시작 전)', 종료·소요 빈칸)을 pending·사본에 쓰고 open 제거, 'finished' 이벤트; 끝에 워커 기동",
       "util/host_process_log.py:523-562", _CSV),
    _i(128, "호스트 서버 기동 직후 startup_recover 를 별도 데몬 스레드(HostLogStartupRecover)로 실행",
       "main.py:1882-1888", _CSV),
    _i(129, "NAS 전송 워커: 데몬 스레드 HostProcessLogWorker, flush 1회 후 kick 이벤트 또는 HOST_LOG_RETRY_S(30 s, 최소 1 s) 대기 반복",
       "util/host_process_log.py:183-187; util/host_process_log.py:575-593", _CSV),
    _i(130, "flush: (프로세스 안 한 번에 하나) 메모리 큐 줄을 먼저 pending 에 다시 쓰고 open 에서 제거 → pending 을 앞에서부터 한 줄씩 NAS 전송, "
            "실패하면 그 라운드 중단(순서 유지) → 보낸 개수만큼 pending 을 다시 읽어 앞부분 제거 후 재작성",
       "util/host_process_log.py:595-658", _CSV),
    _i(131, "메모리 큐 상한 10000 줄, 넘으면 가장 오래된 것부터 버리고 경고는 1회만(SYSTEM 로그 HOSTLOG)",
       "util/host_process_log.py:151-154; util/host_process_log.py:601-613", _CSV),
    _i(132, "NAS 한 줄 전송(_send_one): NAS·_lock 폴더 생성(실패면 False) → 잠금 파일 os.O_CREAT|O_EXCL|O_WRONLY 로 만들고 'sputter {pid} {YYYY-mm-dd HH:MM:SS}' 기록, "
            "이미 있으면 mtime 기준 HOST_LOG_LOCK_STALE_S(120 s) 넘었으면 지우고 바로 재시도, 아니면 0.2 s 대기, 최대 25회; 그 밖 오류면 False",
       "util/host_process_log.py:660-696; util/host_process_log.py:189-196", _CSV),
    _i(133, "중복 검사: NAS 파일(utf-8-sig)에 줄 끝이 ','+기록키 인 줄이 있으면 이미 기록된 것으로 성공 처리; 읽기 실패면 False",
       "util/host_process_log.py:698-711", _CSV),
    _i(134, "NAS append: 새 파일이면 utf-8-sig(BOM)+헤더 후 줄, 있으면 잠금 안에서 끊긴 줄 보정 후 utf-8 로 줄 append. 잠금은 항상 close+unlink 로 해제",
       "util/host_process_log.py:713-736", _CSV),
    _i(135, "HOST_LOG_ENABLED=False 면 모든 공개 API no-op(request 는 '' 반환). 모든 공개 API 는 예외를 밖으로 내보내지 않음, 디버그는 SYSTEM 로그 HOSTLOG",
       "util/host_process_log.py:9-10; util/host_process_log.py:78-84; util/host_process_log.py:171-175; util/host_process_log.py:290-291", _CSV),
    _i(136, "기록 이벤트 sink: 'started'(mark_started 첫 회)·'finished'(finalize·reject·startup_recover, 기록키당 1회, 최근 512개 기억) 를 락 밖에서 "
            "알림 → 메인이 클라이언트 공정 구글챗 카드(CHAT_WEBHOOK_CLIENT_PROCESS_URL 이 있을 때만)로 이벤트 루프에서 전송",
       "util/host_process_log.py:231-282; main.py:245-276", _CSV, note="카드 전송은 v2 에서 포털 몫 — 이벤트만 넘김"),
    _i(137, "종료 시 Robot CSV 워커 close(최대 2 s 대기)",
       "util/host_process_log.py:564-572; main.py:1460-1463", _CSV),
    _i(138, "핸들러 요청 단계: START_SPUTTER 는 대상 'CH1'|'CH2'(ch 이상하면 'CH?'), START_PLASMA_CLEANING 은 'PC(CH{pc._selected_ch})', 레시피 이름 = "
            "basename(recipe) 로 요청 수신 시 request() 호출(요청시각 = 서버 컨텍스트 received_at). 실패해도 '' 만",
       "host/handlers.py:765-792; host/handlers.py:814-822; host/handlers.py:933-941", _CSV),
    _i(139, "핸들러 거절 단계: 응답이 fail 이고 런타임이 아직 소유(mark_owned)하지 않은 키면 reject — 사유 '{error_code} {message}'(message 는 레지스트리 title), "
            "핸들러 예외면 '핸들러 예외: {e!r}' 후 예외 다시 던짐",
       "host/handlers.py:794-812; host/handlers.py:823-830; host/handlers.py:942-949", _CSV),
    _i(140, "챔버 런타임: origin=='host' 인 런만 기록. 러너가 요청을 수락할 때(_host_run_begin) mark_owned; 직전 런이 확정 없이 남아 있으면 먼저 "
            "'미확인'·'다음 요청 수락 전 종료 미확인' 으로 finalize",
       "runtime/chamber_runtime.py:7625-7660; runtime/chamber_runtime.py:4426; runtime/chamber_runtime.py:4485", _CSV),
    _i(141, "챔버 런타임: 레시피 검증 통과 직후 update(recipe_name=basename, row_count=행 수, process_names=각 행 Process_name 을 ' / ' 로)",
       "runtime/chamber_runtime.py:8083-8096", _CSV),
    _i(142, "챔버 런타임: 각 행 start_process 직후 첫 회 mark_started(지금, 런 로그 파일 basename), 두 번째 행부터는 로그 파일만 update",
       "runtime/chamber_runtime.py:4092-4096; runtime/chamber_runtime.py:7662-7687", _CSV),
    _i(143, "챔버 런타임: 행마다 결과(ok·stopped·reason=detail.reason|error_detail|errors[0]) 기억, 러너 종료 지점에서 명시 결과(시작 전 STOP 'STOP'·"
            "'사용자 STOP (시작 전)', 시작 실패 '실패'·'시작 실패: ..', AFTER_FINISH 예외 '미확인', 큐 끝 '성공', 파라미터 오류 '실패'·'레시피 파라미터 오류: ..', "
            "공정 시작 실패 '실패'·'공정 시작 실패: ..')",
       "runtime/chamber_runtime.py:1823-1839; runtime/chamber_runtime.py:4834; runtime/chamber_runtime.py:4971; runtime/chamber_runtime.py:5038; "
       "runtime/chamber_runtime.py:5070; runtime/chamber_runtime.py:5173; runtime/chamber_runtime.py:5237; runtime/chamber_runtime.py:7693-7700", _CSV),
    _i(144, "챔버 런타임 확정(_clear_queue_and_reset_ui 에서): 명시 결과 우선, 없으면 마지막 행 stopped → 'STOP'(사유 없으면 '사용자 STOP'), ok → '성공', "
            "아니면 '실패'(사유); 마지막 행 없고 시작 안 함 → '성공'·'공정 행 없음(대기만)'; 그 외 '미확인'·'종료 경로 미확인'",
       "runtime/chamber_runtime.py:7702-7743", _CSV),
    _i(145, "PC 런타임: 프리플라이트 통과 후 _running 시점에 mark_owned → update(recipe basename, 데이터 행 수(빈 행 제외, 최소 1), 첫 행 Process_name) → "
            "mark_started(지금, 런 로그 파일). 종료 통지에서 stopped 'STOP'(사유 없으면 '사용자 STOP'), ok '성공', 그 외 '실패'(사유 없으면 'error')",
       "runtime/plasma_cleaning_runtime.py:2688-2728; runtime/plasma_cleaning_runtime.py:2739-2752; runtime/plasma_cleaning_runtime.py:3235-3246", _CSV),
    _i(146, "PC 는 프리플라이트 뒤에야 키를 소유하므로 preflight timeout(E700) 응답 때 핸들러가 '거절' 줄을 먼저 쓰고, 뒤늦게 시작된 런의 update·mark_started·"
            "finalize 는 open 에 키가 없어 아무것도 쓰지 않음(그 런은 Robot CSV 에 결과 줄이 없음)",
       "host/handlers.py:942-949; runtime/plasma_cleaning_runtime.py:3283-3289; util/host_process_log.py:330-332; util/host_process_log.py:395-396",
       _CSV, note="알려진 이상 동작(코드 흐름 기준, 실기 확인은 미확인)"),

    # ───────────────────────── apps/robot_server (v1 미사용) ─────────────────────────
    _i(147, "apps/robot_server RawTcpProxy: HOST_SERVER_HOST:HOST_SERVER_PORT 에서 받아 업스트림(PROCESS_SERVER_HOST 등 후보 키, 기본 127.0.0.1:50071)으로 "
            "바이트를 그대로 중계, 64 KiB 단위, TCP keepalive, 한쪽 끝나면 반대쪽 취소, [CONN_OPEN]/[CONN_CLOSE]/[CONN_ERR]/[PIPE_ERR] 로그",
       "apps/robot_server/main_server.py:24-206", _S, status="na",
       note="na 근거: 메인이 같은 50070 에 직접 호스트를 띄움(main.py:1854-1856), 업스트림 키 PROCESS_HOST_HOST/PORT 는 주석 처리"
            "(lib/config_common.py:44-57) 라 50071 을 듣는 곳이 없음, 빌드 스크립트·다른 모듈에서 참조 없음(grep). v2 robot_server 는 plan 3-(5) 새 설계"),
    _i(148, "apps/robot_server 화면(ServerPage): Bind Host/Port·Running, Start/Stop/Restart, 'Client connected/..' 로그 기반 접속 목록, "
            "Auto-scroll·Pause·Hide GET_SPUTTER_STATUS 필터, 5000줄 넘으면 4000줄로, 1 s 마다 <G:\\..\\Logs>\\CH1&2_Server\\RobotServer\\robot_server_page_YYYYMMDD.log "
            "(실패 시 cwd\\Logs\\..) 저장, Save → robot_server_comm_YYYYMMDD_HHMMSS.log",
       "apps/robot_server/runtime/server_page.py:29-308; apps/robot_server/main_server.py:209-284", _S, status="na",
       note="na 근거: ROB-147 과 같음(프로그램 자체 미사용). 또 프록시 로그는 '[CONN_OPEN] cid=..' 라 _PEER_RE('Client connected') "
            "와 맞지 않아 접속 목록이 갱신되지 않음(server_page.py:40, main_server.py:154)"),
    _i(149, "apps/robot_server/state_api.py: 127.0.0.1:9010 HTTP(/healthz, GET/POST /api/state, POST /api/heartbeat, X-Api-Token 'CHANGE_ME', "
            "RUNNING 이 10 s heartbeat 없으면 OFFLINE)",
       "apps/robot_server/state_api.py:13-171", _S, status="na",
       note="na 근거: run_state_api·STORE 를 import·호출하는 곳 없음(grep — 자기 파일만)"),
]

# ───────────────────────── 6단계 로봇 명령 처리(제어 프로그램 쪽)에서 끝낸 항목 ─────────────────────────
# v1 handlers 를 실제로 돌린 골든(v2/tools/v1golden_robot_handlers.py → v2/tests/golden/robot_handlers.json, 213 사례)과
# 명령 × 상태 비교(test_robot_ops::test_matches_v1 — 응답 명령·body 바이트·로그 줄·코일 쓰기 순서·Robot CSV 행·응답 시점).
_T_CMP = "v2/tests/test_robot_ops.py::test_matches_v1"
_B, _ST, _VAC, _MOT = ("v2/ch12/robot_base.py", "v2/ch12/robot_status.py", "v2/ch12/robot_vacuum.py",
                       "v2/ch12/robot_motion.py")
_DONE = {
    31: (_B, _T_CMP), 32: (_B, "v2/tests/test_robot_ops.py::test_heartbeat_paused_during_plc_calls"),
    33: (_B, f"{_T_CMP}; v2/tests/test_robot_ops.py::test_clock_lock_waits_on_virtual_clock"),
    35: (f"{_B}; v2/ch12/robot_api.py", f"{_T_CMP}; v2/tests/test_robot_api.py::test_event_endpoint_emits"),
    37: (_B, _T_CMP), 38: (_B, _T_CMP), 39: (_B, _T_CMP), 40: (_B, _T_CMP),
    42: ("v2/ch12/robot_ops.py", f"v2/tests/test_robot_ops.py::test_golden_shape; {_T_CMP}"),
    43: ("v2/ch12/robot_ops.py", _T_CMP), 44: (f"{_B}; {_MOT}", _T_CMP), 45: (_B, _T_CMP), 46: (_B, _T_CMP),
    58: ("v2/ch12/robot_ops.py", _T_CMP), 138: (_ST, _T_CMP), 139: (_ST, _T_CMP),
}
for _n in (47, 48, 49, 50, 51, 53, 54, 55, 56, 57, 59, 60, 61, 62, 67, 68):
    _DONE[_n] = (_ST, _T_CMP)
for _n in range(70, 89):
    _DONE[_n] = (_VAC, _T_CMP)
for _n in range(89, 109):
    _DONE[_n] = (_MOT, _T_CMP)
for _it in ITEMS:
    _k = int(_it["id"].split("-")[1])
    if _k in _DONE and _it["status"] == "todo":
        _it["v2"], _it["test"] = _DONE[_k]
        _it["status"] = "done"

# ───────────────────────── 전수 목록 마감(로봇 서버·Robot CSV·런타임 기록 단계) ─────────────────────────
# 로봇 서버 v1 골든(v2/tools/v1golden_robot_server.py 가 v1 HostServer 를 실제로 띄워 만든 robot_server_frames.json —
# 받은 바이트·서버 로그·명령 CSV 행 비교)과 Robot CSV v1 골든(v2/tools/v1golden_outputs.py — v1 HostProcessLog 실제 실행),
# 그리고 그 골든이 닿지 않는 부분은 v2/tests/test_inv_robot.py.
_RS = "v2/tests/test_robot_server.py"
_RSE = "v2/tests/test_robot_server_events.py"
_G = _RS + "::test_v1_golden_case"
_OR = "v2/tests/test_outputs_robot.py"
_RG = _OR + "::test_robot_golden"
_IR = "v2/tests/test_inv_robot.py"
_LOG = "v2/robot_server/log.py"


def _g(*names: str) -> str:
    return "; ".join(f"{_G}[{n}]" for n in names)


def _rg(*names: str) -> str:
    return "; ".join(f"{_RG}[robot_{n}]" for n in names)


_DONE2 = {
    1: (_P, f"{_RS}::test_pack_message_bytes; {_g('status_ok', 'all18')}", ""),
    2: (_P, f"{_RS}::test_pack_message_bytes; {_g('status_ok')}", ""),
    3: (_P, f"{_RS}::test_pack_message_bytes; {_g('korean_fail')}", ""),
    4: (_S, _g("partial", "header_junk_ignored", "two_in_one", "eof_mid_body", "eof_mid_header"), ""),
    5: (_S, f"{_g('bad_version')}; {_RSE}::test_version_error_event_then_close", "보고는 D09(로그 + 제어 프로그램 이벤트)"),
    6: (f"{_S}; {_LOG}", f"{_g('invalid_json_then_ok', 'invalid_utf8', 'empty_body')}; "
                         f"{_RSE}::test_parse_error_and_missing_command_send_events", ""),
    7: (_S, _g("data_not_dict", "numeric_ids"), ""),
    8: (_S, _g("not_object", "not_object_string"), "v1 이상 동작 그대로 — D123"),
    9: (_S, _g("status_ok", "korean_fail"), ""),
    10: (f"{_S}; {_LOG}", f"{_g('empty_cmd', 'missing_cmd')}; {_RSE}::test_parse_error_and_missing_command_send_events", ""),
    11: (f"{_S}; v2/robot_server/forward.py; v2/ch12/robot_api.py",
         f"{_g('status_ok')}; v2/tests/test_robot_ops.py::test_matches_v1[start_ok]",
         "received_at·peer 는 내부 API 요청 본문으로 넘긴다(forward.py 계약)"),
    12: (_S, _g("lowercase_spaces", "all18"), ""),
    13: (_S, _g("blank_cmd"), ""),
    14: (_S, _g("unknown_cmd", "numeric_ids"), ""),
    15: (f"{_S}; v2/ch12/robot_api.py", f"{_g('handler_exception')}; v2/tests/test_robot_ops.py::test_reports_and_adapter_protocol",
         "v1 이상 동작 그대로 — D124(제어 프로그램 쪽이 원문+_RESULT 를 만든다)"),
    16: (_S, f"{_g('non_dict_return')}; {_RS}::test_server_defense_non_dict_body; {_RSE}::test_server_defense_e110_e111_send_events", ""),
    17: (_S, f"{_g('fail_without_code')}; {_RSE}::test_server_defense_e110_e111_send_events", ""),
    18: (_S, f"{_RSE}::test_serialization_failure_e107_sends_event; {_IR}::test_logging_exception_row_and_json_dump_error_cell", ""),
    19: (_S, f"{_RS}::test_connections_concurrent_requests_sequential", ""),
    20: (_S, f"{_IR}::test_command_csv_written_after_reply_duration_ms", ""),
    21: (_S, _g("status_ok", "two_in_one"), ""),
    22: (_S, _g("eof_mid_body", "eof_mid_header", "not_object", "bad_version"), ""),
    23: (f"{_S}; v2/robot_server/app.py", f"{_RS}::test_cli_runs_as_separate_program; {_IR}::test_bind_defaults_and_boot_logs", ""),
    24: ("v2/ch12/params/robot.py; v2/robot_server/app.py", f"{_IR}::test_bind_defaults_and_boot_logs",
         "v2 는 메인이 아니라 따로 도는 로봇 서버 프로그램(python -m v2.robot_server)이 이 포트를 듣는다(plan 3-(5))"),
    25: (_LOG, f"{_RS}::test_command_csv_fallback_marker; {_IR}::test_command_csv_header_and_primary_retry", ""),
    26: (_LOG, f"{_IR}::test_command_csv_header_and_primary_retry; {_g('status_ok', 'invalid_json_then_ok')}", ""),
    27: (_S, _g("status_ok", "korean_fail", "data_not_dict"), ""),
    28: (_S, f"{_IR}::test_logging_exception_row_and_json_dump_error_cell", ""),
    29: (_LOG, f"{_RS}::test_command_csv_fallback_marker; {_IR}::test_command_csv_header_and_primary_retry", ""),
    30: (f"{_S}; {_LOG}", f"{_IR}::test_command_csv_failure_never_affects_reply", ""),
    34: (_LOG, f"{_g('status_ok')}; v2/tests/test_robot_api.py::test_end_to_end_robot_server; {_RS}::test_server_log_file_daily",
         "v2 로봇 서버는 따로 도는 프로그램 — 이 태그들은 서버 로그(server_page_YYYYMMDD.log)에만 남고 다른 화면으로 가지 않는다"),
    36: ("v2/ch12/robot_api.py; vanam-process lib/ops2/ch12/chat.ts",
         "v2/tests/test_robot_api.py::test_event_endpoint_emits; "
         "vanam-process/scripts/ops2-chat-ch12.mjs::CHAT_PLC_LINK_SUPPRESS",
         "D09 — 팝업(QMessageBox·중복 억제·5개 상한·15 s 자동 닫힘)은 포털 오류 카드(같은 문구·중복 억제는 CHAT_ERR_DEDUP_S)로. "
         "노트북 화면 팝업 상한·자동 닫힘은 v2 에 없음(결정 후보)"),
    52: ("v2/ch12/eta.py", "v2/tests/test_eta.py::test_confirm_once_at_shutter_delay_and_invalidate; "
                           "v2/tests/test_eta.py::test_no_confirm_with_thickness_wait_or_disabled", ""),
    63: ("v2/ch12/process_engine.py; v2/ch12/core.py; v2/ch12/robot_status.py",
         f"{_IR}::test_chamber_host_run_stop_and_reject_texts; v2/tests/test_recipe.py::test_raw_check_messages_and_overflow; "
         "v2/tests/test_robot_ops.py::test_matches_v1[start_raise_cooldown]; v2/tests/test_robot_ops.py::test_matches_v1[start_preflight_reject]", ""),
    64: ("v2/ch12/recipe.py; v2/ch12/assembly.py",
         f"v2/tests/test_recipe.py::test_load_recipe_file_codes; v2/tests/test_recipe.py::test_xlsx_stdlib_reader_matches_openpyxl_rules; "
         f"{_IR}::test_recipe_load_timeout_e227; v2/tests/test_robot_ops.py::test_matches_v1[start_raise_recipe_error]", ""),
    65: ("v2/ch12/eta.py; v2/ch12/robot_status.py",
         "v2/tests/test_eta.py::test_1_thickness_recipe_matches_runtime_inversion; v2/tests/test_eta.py::test_3_delay_rows; "
         "v2/tests/test_eta.py::test_5_edge_cases; v2/tests/test_robot_ops.py::test_matches_v1[start_ok]", ""),
    66: ("v2/ch12/robot_status.py; v2/ch12/assembly.py", "v2/tests/test_robot_ops.py::test_matches_v1[start_preflight_timeout]",
         "v1 이상 동작 그대로 — D126"),
    69: ("v2/ch12/pc_host.py; v2/ch12/robot_status.py",
         "v2/tests/test_plasma_cleaning.py::test_host_recipe_file_codes; "
         "v2/tests/test_plasma_cleaning.py::test_host_duplicate_start_is_e206_and_csv_applied; "
         "v2/tests/test_plasma_cleaning.py::test_channel_switch_rejected_while_running_and_host_start_e205_before_recipe; "
         "v2/tests/test_plasma_cleaning.py::test_gv_interlock_false_is_e302_and_cleanup_sends_device_commands; "
         "v2/tests/test_plasma_cleaning.py::test_preflight_connect_failure_no_device_commands_and_preflight_timeout_e700; "
         "v2/tests/test_robot_ops.py::test_matches_v1[pc_timeout]; v2/tests/test_robot_ops.py::test_matches_v1[pc_double_e206]", ""),
    109: ("v2/robot_server/app.py; v2/robot_server/server.py",
          f"{_IR}::test_bind_defaults_and_boot_logs; {_RS}::test_cli_runs_as_separate_program",
          "v2 로봇 서버는 따로 도는 프로그램 — 'Host started on {HOST}:{PORT}'·'Host start failed: ..'(이때 종료 코드 1, v1 은 메인이 "
          "호스트 없이 계속). 서버 페이지 Start/Stop/Restart 는 프로그램 시작·종료로(D09 — 결정 후보)"),
    110: ("v2/robot_server/server.py; v2/robot_server/app.py",
          f"{_IR}::test_shutdown_closes_open_connections_quickly; {_RS}::test_cli_runs_as_separate_program",
          "v2 는 열린 연결을 먼저 닫고 wait_closed 최대 5 s(v1 은 메인 종료에서 3 s)"),
    111: (_CSV, f"{_rg('basic', 'nas_blocked')}; v2/tests/test_outputs_paths.py::test_v1_default_paths", ""),
    112: (_CSV, f"{_rg('basic')}; {_OR}::test_robot_concurrent_with_ald_process", ""),
    113: (_CSV, _rg("basic", "nas_blocked", "nas_dup_truncated"), ""),
    114: (_CSV, _rg("basic"), ""),
    115: (_CSV, f"{_rg('basic')}; {_OR}::test_robot_calls_never_block", ""),
    116: (_CSV, _rg("basic"), ""),
    117: (_CSV, f"{_rg('basic')}; {_IR}::test_robot_csv_event_sink_started_finished_once", ""),
    118: (_CSV, f"{_IR}::test_robot_csv_late_writes_after_reject_are_ignored; v2/tests/test_robot_ops.py::test_matches_v1[pc_timeout]", ""),
    119: (_CSV, f"{_rg('basic', 'nas_blocked')}; {_OR}::test_robot_calls_never_block", "D201(메모리 큐 경로 멱등) 그대로"),
    120: (_CSV, _rg("basic", "nas_blocked"), ""),
    121: (_CSV, _rg("basic"), ""),
    122: (_CSV, f"{_rg('basic')}; {_OR}::test_robot_disabled_noop", ""),
    123: (_CSV, _rg("nas_dup_truncated"), ""),
    124: (_CSV, _rg("basic"), ""),
    125: (_CSV, f"{_rg('basic')}; {_OR}::test_robot_recover_skips_keys_already_pending", ""),
    126: (_CSV, _rg("nas_blocked"), ""),
    127: (_CSV, f"{_rg('basic')}; {_OR}::test_robot_recover_skips_keys_already_pending", ""),
    128: (f"{_CSV}; v2/ch12/assembly.py", f"{_IR}::test_assembly_startup_recover_at_build",
          "v2 는 조립 때 startup_recover 를 쓰기 스레드에 넣는다(v1 은 데몬 스레드 HostLogStartupRecover — 막지 않는 것은 같다)"),
    129: (_CSV, f"{_IR}::test_robot_csv_worker_thread_retry_and_close", ""),
    130: (_CSV, f"{_rg('nas_blocked')}; {_OR}::test_robot_concurrent_with_ald_process", ""),
    131: (_CSV, f"{_IR}::test_robot_csv_mem_queue_cap_warns_once", ""),
    132: (_CSV, f"{_rg('stale_lock', 'fresh_lock', 'stale_cfg_exc')}; {_OR}::test_robot_concurrent_with_ald_process",
          "설정 읽기 예외면 30 s — D131"),
    133: (_CSV, _rg("nas_dup_truncated"), ""),
    134: (_CSV, f"{_rg('basic')}; {_OR}::test_robot_concurrent_with_ald_process", ""),
    135: (_CSV, f"{_OR}::test_robot_disabled_noop", ""),
    136: (f"{_CSV}; v2/ch12/robot_api.py", f"{_IR}::test_robot_csv_event_sink_started_finished_once",
          "D46 — 이벤트는 호출한 때. 카드는 포털(client_process 이벤트)"),
    137: (f"{_CSV}; v2/ch12/assembly.py", f"{_IR}::test_robot_csv_worker_thread_retry_and_close", ""),
    140: ("v2/ch12/assembly.py", f"{_IR}::test_chamber_host_run_robot_csv_steps", ""),
    141: ("v2/ch12/assembly.py", f"{_IR}::test_chamber_host_run_robot_csv_steps", ""),
    142: ("v2/ch12/assembly.py", f"{_IR}::test_chamber_host_run_robot_csv_steps", ""),
    143: ("v2/ch12/assembly.py; v2/ch12/process_engine.py; v2/ch12/process_records.py",
          f"{_IR}::test_chamber_host_run_stop_and_reject_texts; {_IR}::test_chamber_host_run_robot_csv_steps",
          "명시 결과는 run_end 로: 시작 전 STOP '사용자 STOP (시작 전)', RECIPE_PARAM_ERROR '레시피 파라미터 오류: ..', 행 사유 = "
          "reason|error_detail|errors[0]. v1 '시작 실패: ..'·'공정 시작 실패: ..'·'AFTER_FINISH 예외' 접두는 v2 예외 경로가 "
          "다르므로 행 사유 그대로(결정 후보)"),
    144: ("v2/ch12/assembly.py", f"{_IR}::test_chamber_host_run_stop_and_reject_texts; {_IR}::test_chamber_host_run_robot_csv_steps", ""),
    145: ("v2/ch12/pc_host.py; v2/ch12/plasma_cleaning.py", f"{_IR}::test_pc_host_run_robot_csv", "로그 파일 늘 빈칸 — PC-098(D106)"),
    146: (_CSV, f"{_IR}::test_robot_csv_late_writes_after_reject_are_ignored; v2/tests/test_robot_ops.py::test_matches_v1[pc_timeout]",
          "v1 이상 동작 그대로 — D127"),
}
for _it in ITEMS:
    _k = int(_it["id"].split("-")[1])
    if _k in _DONE2 and _it["status"] == "todo":
        _it["v2"], _it["test"], _n2 = _DONE2[_k]
        if _n2:
            _it["note"] = (_it["note"] + " / " if _it["note"] else "") + _n2
        _it["status"] = "done"

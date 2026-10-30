"""v1 로봇(호스트) 명령 사실 표 — v2 robot_server·robot_api·robot_ops 가 바이트 단위로 맞춰야 할 v1 동작.

근거: host/protocol.py, host/server.py, host/router.py, host/setup.py, host/handlers.py, errors/error_payload.py,
device/plc.py(코일 번호), lib/config_common.py(기본값), runtime/chamber_runtime.py·runtime/plasma_cleaning_runtime.py
(START 응답 시점). 항목별 전수 목록은 inv_robot.py(ROB-xxx).

표기
- args: 요청 data 키 → (형식, 기본값, 설명). 없으면 {}. 라우터가 덮어쓰는 ch 는 'ch(forced)'.
- response_fields: 성공 응답 data 의 키(순서대로). 실패 응답은 FAIL_FIELDS.
- error_codes: 이 명령이 낼 수 있는 코드(핸들러·런타임·서버 공통 경로 E999/E110/E111 은 COMMON_ERROR_CODES).
- reply_timing: 'immediate'(메모리·PLC 몇 비트 읽고 바로) | 'after_completion'(시퀀스 끝까지 기다린 뒤) |
  'after_start_guard'(런타임 시작 가드 신호까지 기다린 뒤, 공정은 응답 후 계속).
- sequence: PLC 동작 순서(코일 번호는 device/plc.py PLC_COIL_MAP). 'pulse' = ON→180 ms→OFF, 'set'/'clear' = 유지 쓰기.
- interlocks: 거절 조건을 검사 순서대로 '조건 → 코드'.
"""
from __future__ import annotations

# ───────────────────────── 프레임 ─────────────────────────
FRAME = dict(
    header_struct="!BBHIQ",          # 네트워크 바이트 순서(빅엔디언)
    header_size=16,
    fields=(                          # (이름, 오프셋, 길이, 형식)
        ("version", 0, 1, "uint8"),
        ("flags", 1, 1, "uint8"),
        ("cmd_len", 2, 2, "uint16"),
        ("body_len", 4, 4, "uint32"),
        ("timestamp", 8, 8, "uint64"),
    ),
    protocol_version=1,
    response_header=dict(version=1, flags=0, cmd_len="len(응답 명령.encode('utf-8'))",
                         body_len="len(body)", timestamp="int(time.time()) 초"),
    request_header_used=("version", "body_len"),       # flags·cmd_len·timestamp 는 읽고 버림
    body_encoding="utf-8",
    body_request_keys=("request_id", "command", "data"),
    body_response_order=("request_id", "command", "data"),
    json_dumps=dict(ensure_ascii=False, default="str", separators="기본(', ', ': ')"),
    response_command="'{upper(strip(command))}_RESULT' (핸들러 예외 때만 원문 command + '_RESULT')",
    special_responses=dict(
        VERSION_ERROR_RESULT="version != 1 — E102, request_id '', 응답 후 연결 종료",
        PARSE_ERROR_RESULT="JSON 파싱 실패 — E104, request_id '', 연결 유지",
        UNKNOWN_RESULT="command 빈칸(E105, 서버) 또는 공백만(E105, 라우터·보고 없음), request_id 유지",
    ),
    per_connection="한 연결 안 요청은 순차(응답 뒤 다음 헤더), 연결끼리는 동시",
    bind=("0.0.0.0", 50070),
    v1="host/protocol.py:13-46; host/server.py:162-505; host/router.py:19-63; lib/config_common.py:35-36",
)

# 실패 응답 data 키(순서). meta·cause_type 은 있을 때만.
FAIL_FIELDS = ("result", "error_code", "client_action", "category", "severity", "message", "detail", "guide",
               "meta", "cause_type")
# 성공 응답 data 앞 두 키
OK_FIELDS = ("result", "message")
COMMON_ERROR_CODES = ["E102", "E104", "E105", "E106", "E107", "E110", "E111", "E999"]

# 오류 코드 → (client_action, category, severity, title) — errors/error_registry.py 값(호스트 경로에서 쓰는 것만)
ERROR_DEFS = {
    "E102": ("STOP", "TRANSPORT", "ERROR", "지원하지 않는 프로토콜 버전"),
    "E104": ("STOP", "TRANSPORT", "ERROR", "잘못된 JSON 본문"),
    "E105": ("STOP", "REQUEST", "ERROR", "명령 누락"),
    "E106": ("STOP", "REQUEST", "ERROR", "알 수 없는 명령"),
    "E107": ("STOP", "TRANSPORT", "ERROR", "응답 직렬화 실패"),
    "E110": ("STOP", "INTERNAL", "ERROR", "핸들러 내부 예외"),
    "E111": ("STOP", "INTERNAL", "ERROR", "핸들러 반환 형식 오류"),
    "E201": ("STOP", "REQUEST", "ERROR", "챔버 값 오류"),
    "E202": ("STOP", "REQUEST", "ERROR", "레시피 누락"),
    "E203": ("RETRY", "RUNTIME", "WARN", "챔버 런타임 미준비"),
    "E204": ("RETRY", "RUNTIME", "WARN", "플라즈마 클리닝 런타임 미준비"),
    "E205": ("RETRY", "STATE", "WARN", "공정 실행 중"),
    "E226": ("STOP", "REQUEST", "ERROR", "레시피 폴더 값 오류"),
    "E227": ("RETRY", "RUNTIME", "WARN", "레시피 조회 시간 초과"),
    "E230": ("STOP", "RECIPE", "ERROR", "레시피 값 오류"),
    "E301": ("STOP", "SAFETY_GATE", "ERROR", "게이트 닫힘 미확인"),
    "E302": ("STOP", "SAFETY_GATE", "ERROR", "게이트 인터락 불일치"),
    "E303": ("STOP", "SAFETY_GATE", "ERROR", "반대쪽 게이트 닫힘 미확인"),
    "E304": ("STOP", "SAFETY_GATE", "ERROR", "게이트 열림 램프 시간 초과"),
    "E305": ("STOP", "SAFETY_GATE", "ERROR", "게이트 닫힘 램프 시간 초과"),
    "E306": ("STOP", "SAFETY_GATE", "FATAL", "게이트 센서 모순"),
    "E309": ("RETRY", "SAFETY_VACUUM", "WARN", "러핑펌프 오프 타이머 진행 중"),
    "E310": ("STOP", "SAFETY_VACUUM", "ERROR", "러핑 밸브 인터락 불일치"),
    "E311": ("STOP", "SAFETY_VACUUM", "ERROR", "벤트 인터락 불일치"),
    "E312": ("STOP", "SAFETY_VACUUM", "ERROR", "진공 형성 시간 초과"),
    "E313": ("STOP", "SAFETY_VACUUM", "ERROR", "대기압 복귀 시간 초과"),
    "E314": ("STOP", "SAFETY_PIN_CHUCK", "ERROR", "핀 상승 인터락 불일치"),
    "E315": ("STOP", "SAFETY_PIN_CHUCK", "ERROR", "핀 하강 인터락 불일치"),
    "E316": ("STOP", "SAFETY_PIN_CHUCK", "ERROR", "핀 상승 램프 시간 초과"),
    "E317": ("STOP", "SAFETY_PIN_CHUCK", "ERROR", "핀 하강 램프 시간 초과"),
    "E318": ("STOP", "SAFETY_PIN_CHUCK", "ERROR", "척 위치 이동 시간 초과"),
    "E321": ("STOP", "SAFETY_GATE", "ERROR", "로드락 상태상 게이트 열기 불가"),
    "E330": ("STOP", "SAFETY_GATE", "ERROR", "메인 셔터 인터락 불일치"),
    "E331": ("STOP", "SAFETY_GATE", "ERROR", "메인 셔터 닫힘 실패"),
    "E411": ("STOP", "PLC_MAP", "ERROR", "필수 PLC 키 누락"),
    "E412": ("RETRY", "PLC_COMM", "WARN", "PLC 비트 읽기 실패"),
    "E511": ("STOP", "RUNTIME", "ERROR", "사전 연결 점검 실패"),
    "E700": ("RETRY_LATER", "BUSY", "WARN", "공정 쿨다운 대기 중"),
    "E999": ("STOP", "INTERNAL", "ERROR", "정의되지 않은 내부 예외"),
}

_BUSY = ("busy(_fail_if_ch_busy): runtime chamber(ch)/pc(ch)/(CH1 이면 tsp) 실행 또는 Runner state != IDLE 또는 "
         "stage task 살아있음 → E205 (메시지 '{action} 불가 — 사유 / 사유')")
_LL_BUSY = "VACUUM_ON/OFF 진행 중(_loadlock_transition_lock) → E321 '{action} 불가 — {VACUUM_ON|VACUUM_OFF} 진행 중'"


def _gate_open(ch: int) -> dict:
    sw, lamp, il = (200, 360, 40) if ch == 1 else (232, 392, 72)
    other = 2 if ch == 1 else 1
    seq = [
        f"read G_V_{ch}_OPEN_LAMP/G_V_{ch}_CLOSE_LAMP",
        "이미 open: " + ("MAIN_SHUTTER_2_인터락(74) 확인 → set MAIN_SHUTTER_2_SW(234) → 성공" if ch == 2 else "성공"),
        "read L_VENT_SW(212)·L_R_P_SW(208)·L_R_V_SW(209)·L_ATM(99)",
        f"read G_V_{other}_OPEN/CLOSE_LAMP",
        f"read G_V_{ch}_인터락({il})",
        f"pulse G_V_{ch}_OPEN_SW({sw})",
        "sleep wait_s(5 s)",
        f"read G_V_{ch}_OPEN_LAMP({lamp}) 1회",
    ]
    if ch == 2:
        seq += ["read MAIN_SHUTTER_2_인터락(74)", "set MAIN_SHUTTER_2_SW(234)"]
    return dict(
        args={"ch(forced)": ("int", ch, "라우터가 강제"), "wait_s": ("float", 5.0, "펄스 뒤 램프 확인까지 대기")},
        response_fields=["result", "message", "current(이미 open 일 때만)"],
        error_codes=["E205", "E321", "E201", "E306", "E303", "E302", "E304", "E412"] + (["E330"] if ch == 2 else []),
        eta=None,
        reply_timing="after_completion (펄스 + wait_s 5 s 뒤 램프 1회 확인)",
        sequence=seq,
        interlocks=[
            _BUSY + " (lock 전·후 2회, meta 없음)", _LL_BUSY + " (lock 전·_loadlock_gate_lock 후 2회)",
            "lock_ch{ch} → _loadlock_gate_lock 순서로 잡음".replace("{ch}", str(ch)),
            "게이트 램프 둘 다 TRUE → E306",
            "L_VENT_SW·L_R_P_SW·L_R_V_SW·L_ATM 중 하나라도 TRUE → E321 (L_VAC_READY_SW 는 안 봄)",
            f"CH{other} 게이트 closed 아님 → E303",
            f"G_V_{ch}_인터락 FALSE → E302",
            f"OPEN_LAMP FALSE → E304 (셔터 안 염)",
        ] + (["MAIN_SHUTTER_2_인터락 FALSE → E330"] if ch == 2 else []) + [
            "예외 → e.code 또는 E412 (KeyError 도 E412), meta 없음",
        ],
        v1="host/setup.py:59-60; host/handlers.py:1889-2006",
    )


def _gate_close(ch: int) -> dict:
    sw, lamp, sensor = (201, 361, 480) if ch == 1 else (233, 393, 481)
    seq = [f"read G_V_{ch}_OPEN/CLOSE_LAMP",
           "이미 closed: " + ("clear MAIN_SHUTTER_2_SW(234) → 성공" if ch == 2 else "성공"),
           f"read LOADING_{ch}_SENSOR_LAMP({sensor})",
           f"pulse G_V_{ch}_CLOSE_SW({sw})",
           "sleep wait_s(5 s)",
           f"read G_V_{ch}_CLOSE_LAMP({lamp}) + LOADING_{ch}_SENSOR_LAMP"]
    if ch == 2:
        seq.append("clear MAIN_SHUTTER_2_SW(234) (best-effort)")
    return dict(
        args={"ch(forced)": ("int", ch, "라우터가 강제"), "wait_s": ("float", 5.0, "펄스 뒤 램프 확인까지 대기")},
        response_fields=["result", "message", "current(이미 closed 일 때만)"],
        error_codes=["E205", "E201", "E306", "E411", "E412", "E305", "E110"] + (["E331"] if ch == 2 else []),
        eta=None,
        reply_timing="after_completion (펄스 + wait_s 5 s 뒤 램프 확인)",
        sequence=seq,
        interlocks=[
            _BUSY + " (lock 전·후 2회)", f"lock_ch{ch} → _loadlock_gate_lock (VACUUM 전환 검사 없음)",
            "게이트 램프 둘 다 TRUE → E306",
            f"LOADING_{ch}_SENSOR_LAMP FALSE → E305 (닫기 명령 미전송), 읽기 KeyError E411 / 그 밖 E412",
            "CLOSE_LAMP FALSE → E305" + (" (셔터 OFF 도 실패면 E331)" if ch == 2 else ""),
        ] + (["CLOSE_LAMP TRUE 인데 셔터 OFF 실패 → E331"] if ch == 2 else []) + [
            "예외 → _fail(e) (KeyError E411, 그 밖 E110), meta cmd GATE_CLOSE_CH%d" % ch,
        ],
        v1="host/setup.py:61-62; host/handlers.py:2008-2142",
    )


def _chuck(ch: int, up: bool) -> dict:
    power = 218 if ch == 1 else 221
    if up:
        move, move_name, target, target_name = (176 if ch == 1 else 179), f"Z_M_P_{ch}_MID_SW", (129 if ch == 1 else 132), f"Z{ch}_MID_LOCATION"
    else:
        move, move_name, target, target_name = (220 if ch == 1 else 223), f"Z_M_P_{ch}_CCW_SW", (130 if ch == 1 else 133), f"Z{ch}_DOWN_LOCATION"
    pos = "MID" if up else "DOWN"
    seq = [f"read Z{ch}_UP/MID/DOWN_LOCATION",
           ("unknown 이면 1 s 간격 최대 CHUCK_POSITION_RESOLVE_TIMEOUT_S 90 s 확정 대기" if up else "(DOWN 은 확정 대기 없음)"),
           f"이미 {pos} → 성공",
           f"set Z_M_P_{ch}_SW({power})", "sleep 0.2 s", f"set {move_name}({move})",
           f"1 s 간격 read {target_name}({target}) — TRUE 면 같은 PLC 구간에서 clear {move_name}, clear Z_M_P_{ch}_SW",
           "위치 다시 읽어 성공 / 시간 초과면 둘 다 clear 후 E318 / 예외면 둘 다 clear 후 E412"]
    return dict(
        args={"ch(forced)": ("int", ch, "라우터가 강제"), "wait_s": ("float", 90.0, "목표 램프 대기 한도(timeout_s)")},
        response_fields=["result", "message", "current({'position','lamp':{'up','mid','down'}})"],
        error_codes=["E205", "E201", "E412", "E318"],
        eta=None,
        reply_timing="after_completion (목표 램프 TRUE 또는 wait_s 90 s 초과" + (", 앞에 위치 확정 대기 최대 90 s)" if up else ")"),
        sequence=seq,
        interlocks=[
            _BUSY + f" (lock 전 action 'CH{ch}_CHUCK_{'UP' if up else 'DOWN'}', lock 후 'CH{ch}_CHUCK_{pos}')",
            f"lock_ch{ch} 만 잡음 — 게이트·로드락·핀 상태 검사 없음",
            "현재 위치 읽기 실패 → E412",
        ] + (["MID 목표인데 90 s 뒤에도 unknown → E318"] if up else []),
        v1=("host/setup.py:63-64; host/handlers.py:2145-2168; host/handlers.py:2195-2327" if up
            else "host/setup.py:65-66; host/handlers.py:2170-2327"),
        note=("CHx_CHUCK_UP 은 UP 이 아니라 MID 로 이동(v1 정의)" if up else ""),
    )


COMMANDS = {
    "GET_SPUTTER_STATUS": dict(
        args={},
        response_fields=["result", "message('OK')", "Chamber_1", "Chamber_2", "Loadlock_Chamber", "vacuum", "eta"],
        error_codes=["E411", "E110"],
        eta="eta={'CH1':{'remaining_s':int|null},'CH2':{...}} — Shutter Delay(없으면 메인 공정) 진입 때 확정(남은 DELAY 합 + "
            "ETA_TAIL_S 120), 확정 전·큐 마지막 행 전·정지/중단/실패·두께 모드·ETA_ENABLED=False 면 null",
        reply_timing="immediate (메모리 판정 + PLC L_ATM 1비트, 채널 잠금 없음)",
        sequence=["read L_ATM(99) → vacuum = not L_ATM"],
        interlocks=["없음(조회)", "Chamber_n: chamber/pc 실행·Runner 비IDLE·stage task → running, 아니면 has_error → error, 예외 → error",
                    "Loadlock: pc 실행 → running, 마지막 종료 pc 채널 error → error, CH1 IG 대기 → running, pc.is_running → running/idle"],
        v1="host/setup.py:49; host/handlers.py:476-688; controller/process_controller.py:1717-1780",
    ),
    "GET_LOADING_1_SENSOR": dict(
        args={},
        response_fields=["result", "message('OK')", "value(bool)"],
        error_codes=["E411", "E412"],
        eta=None,
        reply_timing="immediate",
        sequence=["read LOADING_1_SENSOR_LAMP(480, M00300)"],
        interlocks=["없음(조회)"],
        v1="host/setup.py:50; host/handlers.py:732-762",
    ),
    "GET_LOADING_2_SENSOR": dict(
        args={},
        response_fields=["result", "message('OK')", "value(bool)"],
        error_codes=["E411", "E412"],
        eta=None,
        reply_timing="immediate",
        sequence=["read LOADING_2_SENSOR_LAMP(481, M00301)"],
        interlocks=["없음(조회)"],
        v1="host/setup.py:51; host/handlers.py:736-762",
    ),
    "GET_RECIPE": dict(
        args={"folder": ("str", "", "strip().upper() 후 ROBOT_RECIPE_FOLDERS(CH1, CH2, ALD) 중 하나")},
        response_fields=["result", "message('OK')", "base_dir", "folder", "files", "count"],
        error_codes=["E226", "E227", "E110"],
        eta=None,
        reply_timing="immediate (스레드 스캔, RECIPE_SCAN_TIMEOUT_S 20 s 한도)",
        sequence=["ROBOT_RECIPE_ROOT_DIR\\folder 비재귀 scandir — 파일이고 .csv/.xlsx(대소문자 무시)인 이름, 소문자 기준 정렬"],
        interlocks=["folder 허용 목록 밖 → E226", "20 s 초과 → E227", "폴더 없음/폴더 아님 → E110"],
        v1="host/setup.py:52; host/handlers.py:691-729; lib/config_common.py:562-568",
    ),
    "START_SPUTTER": dict(
        args={"ch": ("int", 0, "int(data.ch or 0) — 1 또는 2"),
              "recipe": ("str", "", "CSV/XLSX 경로(strip)")},
        response_fields=["result", "message('SPUTTER START OK')", "ch", "min_total_s"],
        error_codes=["E201", "E202", "E203", "E205", "E301", "E411", "E412", "E207", "E208", "E209", "E227",
                     "E230", "E700", "E511", "E110", "E999"],
        eta="min_total_s = Σ행 최소 초(delay 행 대기초 / 두께 모드 shutter_delay×60 / (shutter_delay+process_time 또는 "
            "thickness÷dep_rate÷60)×60) + ETA_TAIL_MIN_S 90, round; ETA_ENABLED=False·실패면 null",
        reply_timing="after_start_guard (프리플라이트 OK/거절·delay 행 수락 신호, CHAMBER_HOST_START_WAIT_TIMEOUT_S 20 s 넘으면 E700; "
                     "공정은 응답 뒤 계속 — timeout 응답 뒤에도 공정이 시작될 수 있음)",
        sequence=["read G_V_{ch}_OPEN/CLOSE_LAMP", "read M_V_{ch}_SW(195/227)·M_V_{ch}_인터락(35/67)",
                  "레시피 로드(30 s 한도)·검증 → Runner 시작 가드 대기"],
        interlocks=["ch∉{1,2} → E201", "recipe 빈칸 → E202", "런타임 없음 → E203",
                    _BUSY + " (lock 전·후 2회, meta 없음)", "lock_ch{ch} 를 응답까지 쥠",
                    "게이트 closed 아님 → E301 (읽기 KeyError E411, 그 밖 E412)",
                    "Main Valve open(M_V_SW AND M_V_인터락) 아님 → E301",
                    "런타임 RuntimeError: .code → '레시피 오류' 시작 E230 → '1분 대기'/'cooldown'/'preflight timeout' E700 → E511",
                    "레시피: 형식 E209 / 없음 E207 / 30 s E227 / 읽기 실패 E209 / 데이터 없음 E208",
                    "ch 가 int 로 안 바뀌면 → 서버 E999"],
        v1="host/setup.py:53; host/handlers.py:814-931; runtime/chamber_runtime.py:7991-8129",
    ),
    "START_PLASMA_CLEANING": dict(
        args={"recipe": ("str", "", "CSV 경로만(strip)")},
        response_fields=["result", "message('PLASMA CLEANING START OK')"],
        error_codes=["E202", "E204", "E205", "E206", "E207", "E208", "E209", "E227", "E302", "E700", "E511", "E110", "E999"],
        eta=None,
        reply_timing="after_start_guard (프리플라이트 결과, PC_HOST_START_WAIT_TIMEOUT_S 20 s 넘으면 E700; 공정은 응답 뒤 계속)",
        sequence=["CSV 로드(30 s 한도)·첫 행 UI 적용 → PC 시작 가드 대기"],
        interlocks=["recipe 빈칸 → E202", "pc 런타임 없음 → E204", "핸들러 busy·게이트·잠금 검사 없음(런타임 판단)",
                    "다른 Host start 대기 중 → E206", "PC 실행 중 → E205", "PC 게이트 인터락 → E302",
                    "RuntimeError: .code → cooldown/1분/preflight E700 → E511 (E230 없음)"],
        v1="host/setup.py:54; host/handlers.py:933-1003; runtime/plasma_cleaning_runtime.py:3190-3301",
    ),
    "VACUUM_ON": dict(
        args={"timeout_s": ("float", 660.0, "폴링 한도(PLC T0052 600 s + 60 s)")},
        response_fields=["result", "message"],
        error_codes=["E321", "E301", "E411", "E412", "E309", "E310", "E312", "E110"],
        eta=None,
        reply_timing="after_completion (L_VAC_READY_SW + 러핑 OFF 확인 또는 실패까지, 최대 timeout_s 660 s + 정리; "
                     "실패면 finally 원복 5 s 뒤 응답)",
        sequence=[
            "전환 잠금 획득(tag VACUUM_ON)", "CH1·CH2 게이트 closed 확인",
            "6비트 스냅샷 — 이미 READY: 러핑 둘 다 OFF 면 성공, 아니면 clear L_R_V_SW(209) → 5 s → clear L_R_P_SW(208) → 10 s/1 s 확인",
            "clear L_VENT_SW(212)", "sleep 0.3 s", "read L_R_P_OFF_TIMER(3)",
            "read LP_STEP1(96)·LP_STEP2(97) — 둘 다 FALSE 일 때만: set L_R_P_SW(208) → sleep 5 s → read L_R_V_인터락(49) → set L_R_V_SW(209)",
            "1 s 간격 스냅샷(L_VAC_READY_SW 98·L_VAC_NOT_READY 100·LP_STEP1·LP_STEP2·L_R_P_SW·L_R_V_SW) + 게이트 감시",
            "READY → 5 s/0.2 s 러핑 OFF 확인 → (안 되면 V OFF → 5 s → P OFF → 10 s/1 s 확인)",
            "실패/예외 finally: (정리 안 했으면) clear L_R_V_SW → 5 s → clear L_R_P_SW",
        ],
        interlocks=[_LL_BUSY + " (원복 없음)",
                    "CH1·CH2 게이트 closed 아님 → E301 (읽기 KeyError E411, 그 밖 E412)",
                    "L_R_P_OFF_TIMER TRUE → E309", "L_R_V_인터락 FALSE → 러핑 정리 후 E310",
                    "L_VAC_NOT_READY TRUE → E312",
                    "러핑 중 게이트 closed 아님(0.5 s 뒤 재확인) → E301",
                    "펌프 ON·밸브 OFF 5 s 초과 → E312 (에어압/LP_STEP1 유지/그 외)",
                    "둘 다 OFF: LP_STEP 진행 중 15 s 초과 → E312, 아니면 5 s grace 뒤 READY 아니면 E312",
                    "펌프 OFF·밸브 ON → E312", "timeout_s 초과 → E312",
                    "READY 뒤 러핑 OFF 확인 실패 → E312"],
        v1="host/setup.py:55; host/handlers.py:1106-1203; host/handlers.py:1205-1676",
    ),
    "VACUUM_OFF": dict(
        args={"timeout_s": ("float", 240.0, "L_ATM 대기 한도")},
        response_fields=["result", "message"],
        error_codes=["E321", "E301", "E411", "E412", "E311", "E313", "E110"],
        eta=None,
        reply_timing="after_completion (L_ATM TRUE 또는 timeout_s 240 s)",
        sequence=["전환 잠금 획득(tag VACUUM_OFF)", "CH1·CH2 게이트 closed 확인",
                  "read L_ATM(99) — TRUE 면 clear L_VENT_SW(212)·clear L_R_V_SW(209) → 5 s → clear L_R_P_SW(208) → 성공",
                  "clear L_R_V_SW", "sleep 5 s", "clear L_R_P_SW", "read L_VENT_인터락(52)", "set L_VENT_SW",
                  "0.5 s 간격 read L_ATM — TRUE 면 clear L_VENT_SW → 성공", "시간 초과 clear L_VENT_SW → E313",
                  "실패/예외 finally: clear L_VENT_SW"],
        interlocks=[_LL_BUSY, "CH1·CH2 게이트 closed 아님 → E301", "L_VENT_인터락 FALSE → E311",
                    "timeout_s 내 L_ATM 미도달 → E313 '(N2 gas 부족)'", "L_R_P_OFF_TIMER 는 검사 안 함"],
        v1="host/setup.py:56; host/handlers.py:1678-1774",
    ),
    "4PIN_UP": dict(
        args={"wait_s": ("float", 20.0, "램프 폴링 한도"), "poll_s": ("float", 1.0, "폴링 간격"),
              "settle_s": ("float", 5.0, "펄스 뒤 첫 확인까지 대기")},
        response_fields=["result", "message"],
        error_codes=["E314", "E316", "E411", "E110"],
        eta=None,
        reply_timing="after_completion (settle 5 s + 램프 폴링 최대 20 s)",
        sequence=["read L_PIN_인터락(50)", "pulse L_PIN_UP_SW(210)", "sleep settle_s", "poll L_PIN_UP_LAMP(370) every poll_s until wait_s"],
        interlocks=["L_PIN_인터락 FALSE → E314", "램프 미도달 → E316", "busy·채널 잠금·게이트·로드락 검사 없음"],
        v1="host/setup.py:57; host/handlers.py:1777-1819; host/handlers.py:1864-1886",
    ),
    "4PIN_DOWN": dict(
        args={"wait_s": ("float", 20.0, "램프 폴링 한도"), "poll_s": ("float", 1.0, "폴링 간격"),
              "settle_s": ("float", 5.0, "펄스 뒤 첫 확인까지 대기")},
        response_fields=["result", "message"],
        error_codes=["E315", "E317", "E411", "E110"],
        eta=None,
        reply_timing="after_completion (settle 5 s + 램프 폴링 최대 20 s)",
        sequence=["read L_PIN_인터락(50)", "pulse L_PIN_DOWN_SW(211)", "sleep settle_s", "poll L_PIN_DOWN_LAMP(371) every poll_s until wait_s"],
        interlocks=["L_PIN_인터락 FALSE → E315", "램프 미도달 → E317", "busy·채널 잠금·게이트·로드락 검사 없음"],
        v1="host/setup.py:58; host/handlers.py:1821-1886",
    ),
    "CH1_GATE_OPEN": _gate_open(1),
    "CH2_GATE_OPEN": _gate_open(2),
    "CH1_GATE_CLOSE": _gate_close(1),
    "CH2_GATE_CLOSE": _gate_close(2),
    "CH1_CHUCK_UP": _chuck(1, True),
    "CH2_CHUCK_UP": _chuck(2, True),
    "CH1_CHUCK_DOWN": _chuck(1, False),
    "CH2_CHUCK_DOWN": _chuck(2, False),
}

assert len(COMMANDS) == 18, len(COMMANDS)

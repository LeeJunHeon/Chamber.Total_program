"""의도된 차이(v1 과 다르게 동작하는 곳) — 사용자 지시 3장의 '합의된 변경'과 그로 인해 생기는 차이만.

항목 = dict(id, what, v1, v2, why, agreed(지시 3장 근거 또는 decisions id))
최종 보고에 전부 나온다.
"""
from __future__ import annotations

DIFFERENCES = [
    dict(id="X01", what="장비 읽기 주기",
         v1="장비마다 다름(IG 10 s, DC·DC Pulse·RF Pulse 5 s, MFC 3 s, PLC 코일 로그 5 s, TSP 10 s 등)",
         v2="정각 1초마다 모든 장비를 전부 읽어 한 행 — MFC R60·R5, IG RDI, RF Pulse 162·165·166, DC Pulse PIV·SAT·ANT·AE·AF 를 매 tick 둘 다/전부"
            "(번갈아 읽기·stale 없음). 장비마다 '응답 뒤 다음 명령까지' 간격 설정 키(MFC_GAP_MS 50·IG_GAP_MS 200·CMD_GAP_MS 0·DCP_GAP_MS 150). "
            "빠른 제어·판정은 v1 속도 유지(FORP/REFP 판정은 v1 한 바퀴 주기 POLL_INTERVAL_MS)",
         why="1초 동기 읽기·기록 — v1 의 큰 간격(MFC·IG 1000 ms, RF Pulse 1500 ms)은 RS-232 안정화용이었고 매뉴얼에 근거가 없다",
         agreed="지시 3장 (2) · 사용자 결정 2026-11-02(D36·D50)"),
    dict(id="X02", what="PLC Modbus TCP 라이브러리",
         v1="pymodbus ModbusTcpClient", v2="직접 MBAP 프레임(v2/common/modbus_tcp.py) — PDU 동일, 트랜잭션 id 시작 값만 다를 수 있음",
         why="의존성 최소", agreed="D02"),
]

DIFFERENCES += [
    dict(id="X03", what="구글 공유 드라이브 → NAS, 로컬 먼저 기록",
         v1="Calib CSV·엑셀·RGA CSV 정본을 구글 공유 드라이브에 직접 씀", v2="G 드라이브는 쓰지 않고 NAS(\\\\VanaM_NAS\\VanaM_Sputter)의 같은 경로·이름·형식. Calib CSV·엑셀은 로컬에 먼저 쓰고 사본만 NAS 로, RGA CSV 는 v1 처럼 NAS 직접 + 실패 시 로컬 폴백",
         why="구글 드라이브 스트리밍에 새 파일이 사라지는 문제(지시 4장) + NAS 가 그 드라이브와 동기화됨(사용자 결정 2026-11-02)", agreed="D07"),
    dict(id="X04", what="ALL STOP", v1="없음(챔버별 Stop 만)", v2="CH1·CH2·플라즈마 클리닝·TSP STOP 을 한꺼번에",
         why="지시 3장 (7)·5장 4단계", agreed="D05"),
]

DIFFERENCES += [
    dict(id="X05", what="로봇 명령 응답의 PLC 통신 오류 문구(E401·E402·E403 detail)",
         v1="pymodbus 예외 문구('PLC 응답 없음/프레임 오류: ModbusIOException: Modbus Error: [Input/Output] ..' — 라이브러리가 정하는 문구)",
         v2="무응답(E402·timeout)은 v1(pymodbus 3.11.2·retries=0)과 같은 문구 'PLC 응답 없음/프레임 오류: ModbusIOException: Modbus Error: [Input/Output] "
            "No response received after 0 retries, continue with next request'(2026-11-09 D6 — v1 의 '(op=, addr=)' 꼬리도 없다). 그 밖(연결 오류 E401·프레임 불일치·"
            "Modbus 거절 E403)은 pymodbus 가 만드는 예외 문구(연결 끊김 종류·transaction id 불일치 문구 등)를 같게 만들 수 없어 v2 드라이버 문구 — "
            "코드·메시지(제목)·필드·순서·응답 시점은 같다",
         why="pymodbus 를 쓰지 않음(X02) — 무응답만 라이브러리 문구를 알 수 있어 맞추고(v1 requirements pymodbus==3.11.2 소스 확인), 나머지는 같을 수 없다. "
             "시험(test_robot_ops)은 골든 비교에서 자리표로 가리고 무응답 문구는 따로 확인", agreed="D02; 사용자 지시 2026-11-09 D6"),
    dict(id="X06", what="로봇 명령의 시작 가드 대기 위치",
         v1="ChamberRuntime·PlasmaCleaningRuntime.start_with_recipe_string 안(asyncio.wait_for)",
         v2="robot_ops(robot_status._wait_chamber_start·_wait_pc_start) — 엔진은 on_preflight(ok, code, detail) 로 결과만. "
            "문구·한도(CHAMBER/PC_HOST_START_WAIT_TIMEOUT_S)·E206 은 v1 그대로",
         why="로봇 서버 분리·명령 처리 한 곳(지시 3장 (4)(5))", agreed="지시 3장 (5)"),
]

DIFFERENCES += [
    dict(id="X07", what="자동 재시작·공정 이어가기", v1="없음(apps/recovery_service 는 메인이 쓰지 않음)",
         v2="감시 프로그램(10분 3회 초과 포기) + 1초 저널(CH1·CH2·PC·TSP) + 같은 규칙으로 이어가기(정지 순서 도중=정지 순서 끝까지·설정 쓰기 도중=그 블록 설정 처음부터·기다리는 중=남은 만큼, "
            "켜짐/꺼짐만 확인, 로봇 이동 명령은 E205), 조건이 안 맞으면 정지 순서·run_end error PROGRAM_RESTART",
         why="사용자와 합의한 규칙", agreed="지시 3장 (6); D79·D93·D94 (사용자 결정 2026-11-02)"),
    dict(id="X08", what="의도적 종료", v1="공정 중이면 종료 거부(확인창)·콘솔 닫기 처리 없음", v2="저널에 '의도적 종료'를 먼저 적고 파워 출력만 끔(챔버K 결정 4)",
         why="지시 3장 (6)", agreed="지시 3장 (6)"),
    dict(id="X09", what="블록 레시피·공정 중 자유 제어", v1="행 레시피만, 공정 중 수정·건너뛰기·일시정지·편집 없음",
         v2="레시피 = 블록 목록(v1 행은 기본 템플릿 — 같은 스텝), PROCESS_ADJUST·건너뛰기·일시정지·시퀀스 편집", why="지시 3장 (3)", agreed="지시 3장 (3)"),
    dict(id="X10", what="명령 경로·출처 표", v1="화면 버튼·로봇 핸들러가 각자 장비를 부름", v2="웹·로컬·로봇 명령이 core.command 한 곳을 지나고 명령마다 받는 출처를 표에",
         why="지시 3장 (4)", agreed="지시 3장 (4)"),
    dict(id="X11", what="로봇 서버 분리", v1="메인 프로그램 안 서버(0.0.0.0:50070)", v2="별도 프로그램 v2/robot_server(판단 없이 내부 API 로 전달 — 응답이 느리면 끝까지 기다림, 연결 불가·진행 중 끊김일 때만 E205·GET_SPUTTER_STATUS 마지막 실제 상태 "
            "그대로(센서 값은 캐시하지 않고 E205 — 2026-11-09 D1~D3, D08 수정))",
         why="지시 3장 (5)", agreed="지시 3장 (5); D08; D09"),
    dict(id="X12", what="화면·구글챗", v1="PySide6 화면, 프로그램이 웹훅으로 직접 보냄", v2="포털 화면 + 같은 화면의 노트북 local 빌드, 카드는 포털이 보냄(재시작·이어가기 카드 1장 추가)",
         why="지시 3장 (7)", agreed="지시 3장 (7)"),
]

DIFFERENCES += [
    dict(id="X30", what="값을 바꾸는 명령의 되읽기 확인",
         v1="장비마다 달랐다(MFC·IG·RF Pulse 는 확인 없이 확정하거나 경고만, DC Pulse 는 1회 재기록, PLC 는 에코만)",
         v2="쓰기 → 되읽기 확인 → 다르면 다시 쓰기(장비별 *_REWRITE_MAX·*_REWRITE_GAP_MS) → 그래도 다르면 사유 코드 VERIFY_MISMATCH 로 오류. "
            "되읽기 질의가 없는 명령은 장비 사실 표 READBACK_NONE 에 적고 v1 과 같게",
         why="값을 바꾸는 모든 명령을 같은 방식으로 통일", agreed="사용자 결정 2026-11-02(D36)"),
]


DIFFERENCES += [
    dict(id="X40", what="MFC 1초 읽기", v1="R60·R5 를 따로(저우선 R5·R6) 읽고 SP·ON/OFF 는 기억값",
         v2="기본 split — R60·R5·R6 매 tick + R69·R1~R4·R7 은 tick 마다 1개씩 돌아가며(마지막 실제 값을 (개수+1) 초까지, 읽은 뒤 나이 칸), MFC_READ_MODE=all 이면 9개 매 tick",
         why="1초 예산(응답 시간 합 ≤ 0.6 s)과 실제값 기록의 절충 — 설정으로 바꿈", agreed="사용자 결정 2026-11-13(D407)"),
    dict(id="X41", what="멈춘 프로그램 되살리기", v1="v1 은 recovery_service 가 메인이 쓰지 않아 동작 안 함(꺼질 때만 사람이 다시 켬)",
         v2="감시 프로그램이 tick·명령 코어 진행을 건강 확인 파일로 보고 한도(60 s)동안 멈추면 덤프 → 강제 종료 → 재시작(재시작 한도 포함)",
         why="로봇 서버가 끝까지 기다려 멈춘 프로그램이 로봇도 멈추는 문제", agreed="사용자 지시 2026-11-13 3장(D408)"),
    dict(id="X42", what="장비 응답 시간 기록", v1="없음", v2="장비·명령별 1분 집계 CSV(data/device_timing) + 웹 상태 요약", why="bench 에서 간격·예산을 숫자로 맞추기 위해",
         agreed="사용자 지시 2026-11-13 4장(D409)"),
]

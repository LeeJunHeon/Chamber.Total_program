"""의도된 차이(v1 과 다르게 동작하는 곳) — 사용자 지시 3장의 '합의된 변경'과 그로 인해 생기는 차이만.

항목 = dict(id, what, v1, v2, why, agreed(지시 3장 근거 또는 decisions id))
최종 보고에 전부 나온다.
"""
from __future__ import annotations

DIFFERENCES = [
    dict(id="X01", what="장비 읽기 주기",
         v1="장비마다 다름(IG 10 s, DC·DC Pulse·RF Pulse 5 s, MFC 3 s, PLC 코일 로그 5 s, TSP 10 s 등)",
         v2="정각 1초마다 모든 장비를 한 번 읽어 한 행(1초에 못 들면 번갈아 읽기). 빠른 제어·판정은 v1 속도 유지",
         why="1초 동기 읽기·기록", agreed="지시 3장 (2)"),
    dict(id="X02", what="PLC Modbus TCP 라이브러리",
         v1="pymodbus ModbusTcpClient", v2="직접 MBAP 프레임(v2/common/modbus_tcp.py) — PDU 동일, 트랜잭션 id 시작 값만 다를 수 있음",
         why="의존성 최소", agreed="D02"),
]

DIFFERENCES += [
    dict(id="X03", what="G: 드라이브 기록 방식",
         v1="Calib CSV·엑셀·RGA CSV 정본을 G: 에 직접 씀", v2="같은 경로·이름·형식, 로컬에 먼저 쓰고 사본만 G:/NAS 로",
         why="G: 스트리밍에 새 파일이 사라지는 문제(지시 4장)", agreed="D07"),
    dict(id="X04", what="ALL STOP", v1="없음(챔버별 Stop 만)", v2="CH1·CH2·플라즈마 클리닝·TSP STOP 을 한꺼번에",
         why="지시 3장 (7)·5장 4단계", agreed="D05"),
]

DIFFERENCES += [
    dict(id="X05", what="로봇 명령 응답의 PLC 통신 오류 문구(E401·E402·E403 detail)",
         v1="pymodbus 예외 문구('PLC 응답 없음/프레임 오류: ModbusIOException: Modbus Error: [Input/Output] ..')",
         v2="v2 PLC 드라이버 문구('PLC 요청 실패(timeout): PLC 응답 없음/프레임 오류: ReadTimeout: .. (op=read_coil, addr=N)')"
            " — 코드·메시지(제목)·필드·순서·응답 시점은 같다",
         why="pymodbus 를 쓰지 않음(X02) — 예외 문구가 라이브러리 것이라 같을 수 없다. 시험(test_robot_ops)은 자리표로 가림",
         agreed="D02"),
    dict(id="X06", what="로봇 명령의 시작 가드 대기 위치",
         v1="ChamberRuntime·PlasmaCleaningRuntime.start_with_recipe_string 안(asyncio.wait_for)",
         v2="robot_ops(robot_status._wait_chamber_start·_wait_pc_start) — 엔진은 on_preflight(ok, code, detail) 로 결과만. "
            "문구·한도(CHAMBER/PC_HOST_START_WAIT_TIMEOUT_S)·E206 은 v1 그대로",
         why="로봇 서버 분리·명령 처리 한 곳(지시 3장 (4)(5))", agreed="지시 3장 (5)"),
]

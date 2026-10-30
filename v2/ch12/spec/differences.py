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

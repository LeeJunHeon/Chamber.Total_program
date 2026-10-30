"""결정 목록(사용자 지시 6장 — 사람에게 묻지 않고 v1 과 같은 쪽·더 안전한 쪽으로 정한 것).

항목 = dict(id, title, decision, reason, confirm(사용자 확인 필요 여부), where(관련 v2 파일·v1 근거))
confirm=True 인 항목은 최종 보고의 '사용자 확인 필요'에 모두 나온다.
"""
from __future__ import annotations

DECISIONS = [
    dict(id="D01", title="실행·배포 방식",
         decision="기본은 챔버K 처럼 소스 실행(run_ch12_v2.cmd → python -m v2.supervisor → python -m v2.ch12). "
                  "PyInstaller onedir 묶음 도구(v2/tools/build_exe.py)도 둔다.",
         reason="챔버K 와 같은 운영 방식, 소스 실행은 고치기·되돌리기가 쉽다. v1 의 onedir 경험(워커 kill)도 살린다.",
         confirm=True, where="v2/tools/build_exe.py; run_ch12_v2.cmd"),
    dict(id="D02", title="Modbus TCP 트랜잭션 id",
         decision="PDU 바이트는 v1(pymodbus)과 같고, MBAP 트랜잭션 id 는 1 부터 하나씩 늘린다.",
         reason="pymodbus 의 시작 값은 판에 따라 다르고 PLC 는 트랜잭션 id 를 되돌려 줄 뿐 해석하지 않는다.",
         confirm=False, where="v2/common/modbus_tcp.py"),
]

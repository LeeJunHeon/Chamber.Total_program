"""명령과 결과, 사유 코드 (장비 공통).

- 웹·로컬 화면·내부(안전 엔진·공정 엔진) 명령은 모두 Command 로 core 큐에 들어간다.
- core 가 상태·인터록을 검사해 accepted/rejected 를 정하고, 실행이 끝나면 done/failed 를 돌려준다.
- 사유는 문자열 문구가 아니라 코드(reason_code)다 — v1 의 "재시작" 로그 레벨 같은 문자열 채널을 없앤다.
- 장비별 명령 종류·허용 표는 장비 패키지(v2.chk.commands)가 정한다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class Source(str, Enum):
    WEB = "web"            # vanam-process
    HMI = "hmi"            # 노트북 로컬 화면
    INTERNAL = "internal"  # 안전 엔진·공정 엔진


class ResultStatus(str, Enum):
    ACCEPTED = "accepted"  # 검사 통과, 실행 시작
    REJECTED = "rejected"  # 검사 실패 — 실행 안 함
    DONE = "done"          # 실행 완료
    FAILED = "failed"      # 실행 중 실패


@dataclass(frozen=True)
class Command:
    id: str
    source: Source
    kind: str
    args: dict = field(default_factory=dict)
    requested_by: str = ""     # 웹 사용자 이메일 / "hmi" / 규칙 id
    requested_at: str = ""     # ISO 8601


@dataclass(frozen=True)
class CommandResult:
    id: str
    status: ResultStatus
    reason_code: Optional[str] = None
    detail: str = ""


@dataclass(frozen=True)
class ArgDef:
    name: str
    type: str                  # "float" / "int" / "bool" / "str" / "enum" / "rows"
    required: bool = True
    param_min: Optional[str] = None   # 범위를 ParamDef 값에서 가져올 때 키
    param_max: Optional[str] = None
    min: Optional[float] = None
    max: Optional[float] = None
    enum: Optional[tuple] = None
    desc: str = ""


@dataclass(frozen=True)
class Reason:
    code: str
    label: str
    category: str              # reject / abort / fail / info


# 장비 공통 사유 코드 — 장비 패키지가 자기 코드를 더한다.
COMMON_REASONS: tuple = (
    Reason("UNKNOWN_COMMAND", "알 수 없는 명령", "reject"),
    Reason("BAD_ARGS", "인자 형식·범위 오류", "reject"),
    Reason("SOURCE_NOT_ALLOWED", "이 경로(웹/로컬)에서 허용되지 않는 명령", "reject"),
    Reason("PERMISSION", "권한 없음", "reject"),
    Reason("STATE_NOT_ALLOWED", "지금 상태에서 허용되지 않음", "reject"),
    Reason("EXPIRED", "유효 시간이 지난 명령", "reject"),
    Reason("COMMAND_EXPIRED", "유효 시간이 지난 웹 명령 — 실행하지 않음(장비 시계 기준)", "reject"),
    Reason("DUPLICATE", "이미 처리한 명령 id", "reject"),
    Reason("LINK_DOWN", "장비 통신 끊김", "reject"),
    Reason("DEVICE_DISABLED", "설정에서 꺼진 장비", "reject"),
    Reason("CONFIG_INVALID", "설정 검사 실패", "reject"),
    Reason("CONFIG_HASH_MISMATCH", "설정 정의(스키마)가 장비와 다름", "reject"),
    Reason("CONFIG_VERSION_STALE", "이미 적용한 버전보다 오래된 설정", "reject"),
    Reason("CONFIG_SECRET_FROM_WEB", "비밀값은 웹에서 받지 않음", "reject"),
    Reason("CONFIG_LOCKED_KEY", "실행 모드·장비 ID 는 웹에서 바꾸지 않음(노트북 설정 파일로만)", "reject"),
    Reason("CONFIG_NO_PREVIOUS", "되돌릴 이전 설정 버전 없음", "reject"),
    # bench 는 STOP·ALL STOP 같은 safety 쓰기까지 거부한다(사용자 결정 유지). 그래서 로컬 화면 단계에서
    # bench 모드일 때 "읽기 전용 — 조작은 장비 앞면으로" 를 크게 표시하고 STOP·ALL STOP 버튼을 비활성 표시해야 한다.
    Reason("DC_LAST_STATE_FULLY", "DC Last State 가 FULLY — 전원 복귀 때 출력까지 되살아남(앞면에서 바꿀 것)", "info"),
    Reason("DC_PROTECTION_TRIPPED", "DC OVP·OCP 트립 중 — 출력이 나오지 않음", "info"),
    Reason("MFC_SCALE_MISMATCH", "MFC 압력 FS·유량 단위·Remote 가 설정과 다름 — 공정 시작 거부", "reject"),
    Reason("MFC_SCALE_UNREAD", "MFC 압력 레인지·단위·Remote 를 못 읽음 — 설정값 사용", "info"),
    Reason("COMMAND_BLOCKED", "통신층 가드가 막은 명령(허용 목록 밖·장비 설정 변경) — 포트에 보내지 않음", "reject"),
    Reason("BENCH_READ_ONLY", "bench 모드 — 장비 쓰기 금지(읽기 전용 — 조작은 장비 앞면으로)", "reject"),
    Reason("QUEUE_FULL", "장비 요청 큐가 가득 참", "reject"),
    Reason("CANCELLED", "안전 요청이 앞질러 취소됨", "fail"),
    Reason("TIMEOUT", "시간 초과", "fail"),
    Reason("DEVICE_NO_RESPONSE", "장비 무응답", "fail"),
    Reason("DEVICE_REJECTED", "장비가 명령을 거부", "fail"),
    Reason("VERIFY_MISMATCH", "되읽기 값 불일치", "fail"),
    Reason("INTERNAL_ERROR", "프로그램 내부 오류", "fail"),
    Reason("USER_STOP", "사용자 STOP", "abort"),
    Reason("ALL_STOP", "ALL STOP", "abort"),
)

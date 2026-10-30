"""1초 스냅샷 형식 (장비 공통).

한 행 = 정각 초 t 의 모든 장비 값.
- tick 정보: ts(정각 초, 로컬 ISO), seq(시작 뒤 tick 번호), jitter_ms(실제 읽기 시작 − t), skipped(직전 행 뒤 건너뛴 tick 수)
- 장비마다 품질 1쌍: <dev>.q, <dev>.age_s. 값을 번갈아 읽는 필드는 자기 품질 1쌍(<field>.q, <field>.age_s)을 더 갖는다.
- 값: <dev>.<name>. 값이 없으면 빈 칸(None).

품질
- ok         : 이번 tick 마감 안에 받은 값
- late       : 앞 tick 에 보낸 요청이 그 tick 마감 뒤 도착 — 그 값이 처음 쓰이는 행(보통 다음 행). 그 뒤로는 stale
               (요청을 보낸 tick 의 행은 새 값이 없으므로 stale — 3단계에서 정함)
- stale      : 이번 tick 에 새 값 없음 — 마지막 값과 나이(age_s)
- fail       : 통신 오류(값 없음 또는 마지막 값 + 나이)
- off        : 쓰지 않음·연결 안 함(설정에서 끔, 포트 없음)
- suppressed : 예정된 무응답 구간(예: 이온 게이지 필라멘트 전환) — 실패로 세지 않는다
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Sequence


class Quality(str, Enum):
    OK = "ok"
    LATE = "late"
    STALE = "stale"
    FAIL = "fail"
    OFF = "off"
    SUPPRESSED = "suppressed"


class FType(str, Enum):
    FLOAT = "float"
    INT = "int"
    BOOL = "bool"
    STR = "str"


@dataclass(frozen=True)
class FieldDef:
    name: str                 # 장비 안 이름(소문자, 점 없음)
    type: FType
    unit: str
    label: str
    fmt: str = ""             # CSV 표시 형식(파이썬 format spec). 빈 값이면 str()
    source: str = ""          # 읽는 곳(레지스터·명령) — 근거
    own_quality: bool = False # 번갈아 읽기 등으로 장비 품질과 따로 품질을 가진다
    summary: str = "last"     # 1분 요약 방식: last / mean / min / max / any(bool OR) / none
    detail: bool = False      # 화면 카드에서 평소 접어 둘 값(5-13)
    desc: str = ""            # 개발용 설명(레지스터 등) — 화면은 '상세'에서만
    enum_labels: Optional[tuple] = None   # str 값 → 화면용 이름 ((값, 이름), ...)
    state: str = ""           # 같은 블록의 상태 열 이름(5-14). 값이 비어 있고 상태가 ok 가 아니면 화면은 그 상태 이름을 보인다


@dataclass(frozen=True)
class DeviceBlock:
    name: str                 # 열 접두어(소문자)
    label: str
    fields: tuple
    desc: str = ""            # 개발용 설명(장비 모델 등)

    def columns(self) -> list:
        cols = [f"{self.name}.q", f"{self.name}.age_s"]
        for f in self.fields:
            cols.append(f"{self.name}.{f.name}")
            if f.own_quality:
                cols += [f"{self.name}.{f.name}.q", f"{self.name}.{f.name}.age_s"]
        return cols


TICK_COLUMNS: tuple = (
    FieldDef("ts", FType.STR, "", "시각", source="clock", detail=True, desc="정각 초, 로컬 ISO 8601"),
    FieldDef("seq", FType.INT, "", "읽기 번호", source="clock", detail=True, desc="tick 번호(시작 뒤)"),
    FieldDef("jitter_ms", FType.INT, "ms", "읽기 시작 지연", source="clock", detail=True, desc="실제 − 정각"),
    FieldDef("skipped", FType.INT, "", "건너뛴 읽기 수", source="clock", detail=True, desc="직전 행 뒤 건너뛴 tick 수"),
)


def row_columns(blocks: Sequence[DeviceBlock], extra: Sequence[FieldDef] = ()) -> list:
    """CSV·SQLite 열 순서: tick → extra(공정 상태 등) → 장비 블록 순서대로."""
    cols = [f.name for f in TICK_COLUMNS] + [f.name for f in extra]
    for b in blocks:
        cols += b.columns()
    return cols


@dataclass
class Reading:
    """장비 worker 가 tick 하나에 돌려주는 결과."""
    device: str
    quality: Quality
    age_s: Optional[float]            # 값의 나이(행 시각 기준). ok 면 0~1
    values: dict = field(default_factory=dict)
    field_quality: dict = field(default_factory=dict)   # {field: (Quality, age_s)} own_quality 필드
    error: str = ""

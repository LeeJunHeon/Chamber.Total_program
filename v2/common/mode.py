"""실행 모드 (장비 공통).

sim   : 가짜 장비(SimTransport). 실제 포트·웹·NAS 없음. 장비 쓰기는 가짜 장비에만.
bench : 실제 포트를 열어 읽기만 — 장비 쓰기 요청은 worker 가 BENCH_READ_ONLY 로 거부(안전 요청 포함).
        v1 을 끈 상태에서 응답 시간·간격을 재는 단계. 웹 상태 보고와 NAS 사본은 한다(명령은 쓰기라 거부).
prod  : 운전. 설정에서 명시적으로만(RUN_MODE 기본값은 sim, 설정 파일이 없어도 sim).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


class RunMode(str, Enum):
    SIM = "sim"
    BENCH = "bench"
    PROD = "prod"


@dataclass(frozen=True)
class ModePolicy:
    mode: RunMode
    real_ports: bool       # SerialTransport 를 쓰는가(아니면 SimTransport)
    device_writes: bool    # worker 가 쓰기·안전 요청을 실행하는가
    uplink: bool           # 웹 보고
    nas_copy: bool         # NAS 사본


POLICIES = {
    RunMode.SIM: ModePolicy(RunMode.SIM, real_ports=False, device_writes=True, uplink=False, nas_copy=False),
    RunMode.BENCH: ModePolicy(RunMode.BENCH, real_ports=True, device_writes=False, uplink=True, nas_copy=True),
    RunMode.PROD: ModePolicy(RunMode.PROD, real_ports=True, device_writes=True, uplink=True, nas_copy=True),
}


def resolve_mode(value: Optional[Any]) -> RunMode:
    """설정값 → 모드. 없거나 비면 sim. 모르는 값은 오류(조용히 바꾸지 않는다)."""
    if value is None or value == "":
        return RunMode.SIM
    return RunMode(value)


def policy(mode: RunMode) -> ModePolicy:
    return POLICIES[mode]

"""챔버12 엔진 상태 이름 — 웹 묶음 states(web_schema.states_part)·명령 표 허용 상태(commands)·안전 규칙 states 가 같은 이름을 쓴다.

챔버K v2/chk/process_states.py 의 StateDef(name·label·kind) 모양. 챔버12 는 상태 기계가 엔진마다 따로 있으므로(v1 도 Runner·
PlasmaCleaningRuntime·TSPPageController 가 따로) 이름 앞에 엔진을 붙이지 않고 엔진(engine) 칸으로 구분한다.
- chamber : ProcessEngine.state_name(v1 Runner _runner_state 와 같은 뜻 — IDLE·PREFLIGHT·COOLDOWN(쿨다운·대기 행)·RUNNING·CLEANUP)
            + 정지 순서 중(STOPPING — runner.shutdown, v1 '종료 절차' 단계)
- pc      : PlasmaCleaning.phase(idle·preflight·test·run·stopping)
- tsp     : TspEngine.phase(tsp_cycle.PHASES 그대로)
- heater  : HeaterEngine 공정 연동 phase(''·watch·wait·soak·hold) — 공정 밖은 ''
kind: idle(아무것도 안 함) · active(공정·운전 중 — 시작 명령 거부) · stopping(정지 순서 중) · end(끝 정리 — 곧 idle)
chamber_state(engine) 는 엔진 객체에서 명령 표가 쓰는 상태 이름을 읽는다(core 가 부른다).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Tuple


class Kind(str, Enum):
    IDLE = "idle"
    ACTIVE = "active"
    STOPPING = "stopping"
    END = "end"


@dataclass(frozen=True)
class StateDef:
    name: str
    label: str
    kind: Kind
    engine: str
    v1: str = ""
    desc: str = ""


S = StateDef
I, A, ST, E = Kind.IDLE, Kind.ACTIVE, Kind.STOPPING, Kind.END

STATES: Tuple[StateDef, ...] = (
    # 챔버 공정(CH1·CH2 — 엔진은 챔버마다 하나, 이름은 같다)
    S("IDLE", "대기", I, "chamber", "runtime/chamber_runtime.py _runner_state IDLE"),
    S("PREFLIGHT", "시작 전 점검", A, "chamber", "_runner_state PREFLIGHT(_start_after_preflight)",
      "장비 연결·게이트·Main Valve·펄스 포트 확인"),
    S("COOLDOWN", "쿨다운·대기 행", A, "chamber", "_runner_state COOLDOWN/DELAY", "큐 행 사이 60 s · delay 행"),
    S("RUNNING", "공정 중", A, "chamber", "_runner_state RUNNING(process_controller 실행)"),
    S("STOPPING", "정지 순서 중", ST, "chamber", "process_controller 종료 절차(_start_normal_shutdown)",
      "STOP·이상 중단 — 정지 순서는 고정(safety_rules.STOP_SEQUENCE)"),
    S("CLEANUP", "끝 정리", E, "chamber", "AFTER_FINISH(히터·장비 정리)"),
    # 플라즈마 클리닝
    S("pc.idle", "대기", I, "pc", "runtime/plasma_cleaning_runtime.py"),
    S("pc.preflight", "시작 전 점검", A, "pc", "_run_real 프리플라이트"),
    S("pc.test", "TEST 모드", A, "pc", "TEST 행"),
    S("pc.run", "클리닝 중", A, "pc", "PlasmaCleaningController 실행"),
    S("pc.stopping", "정지 중", ST, "pc", "_safe_rf_stop·_final_cleanup"),
    # TSP(tsp_cycle.PHASES)
    S("tsp.idle", "대기", I, "tsp", "runtime/tsp_runtime.py"),
    S("tsp.prepare", "준비", A, "tsp", "tsp_controller.py:108"),
    S("tsp.ig_on", "IG ON·첫 확인", A, "tsp", "tsp_controller.py:120"),
    S("tsp.tsp_on", "TSP ON(승화)", A, "tsp", "tsp_controller.py:140"),
    S("tsp.tsp_off", "TSP OFF(사이클 간격)", A, "tsp", "tsp_controller.py:160"),
    S("tsp.cleanup", "정리", ST, "tsp", "tsp_controller.py finally"),
    S("tsp.finished", "정리 끝", E, "tsp", "tsp_runtime.py finally"),
    S("tsp.closing", "종료 정리(OFF·IG OFF)", ST, "tsp", "tsp_runtime.py close"),
    # CH2 히터 공정 연동(공정 밖 운전은 히터 패널 — 상태 이름 없음)
    S("heater.watch", "공정 감시", A, "heater", "controller/heater_service.py 공정 연동"),
    S("heater.wait", "승온 대기", A, "heater", "HEATER_WAIT"),
    S("heater.soak", "도달 유지(soak)", A, "heater", "HEATER_WAIT soak"),
    S("heater.hold", "유지 모드 대기", A, "heater", "HEATER_HOLD"),
)

ACTIVE_STATES: Tuple[str, ...] = tuple(s.name for s in STATES if s.engine == "chamber" and s.kind is A)
CHAMBER_STATES: Tuple[str, ...] = tuple(s.name for s in STATES if s.engine == "chamber")
IDLE_STATES: Tuple[str, ...] = ("IDLE",)
STOPPABLE: Tuple[str, ...] = ACTIVE_STATES + ("STOPPING", "CLEANUP")


def by_name() -> dict:
    return {s.name: s for s in STATES}


def chamber_state(eng: Any) -> str:
    """ProcessEngine → 명령 표 상태 이름. 정지 순서 중이면 STOPPING(state_name 은 RUNNING 그대로라 runner 로 본다)."""
    if eng is None:
        return "IDLE"
    r = getattr(eng, "runner", None)
    if r is not None and getattr(r, "is_running", False) and (getattr(r, "shutdown", False)
                                                             or getattr(r, "stop_requested", False)):
        return "STOPPING"
    name = str(getattr(eng, "state_name", "IDLE") or "IDLE")
    if name == "IDLE" and getattr(eng, "_cmd", None) is not None:
        return "PREFLIGHT"                     # 시작 명령을 받고 러너가 아직 집어 가지 않은 순간 — 시작 전 단계로 본다
    return name if name in CHAMBER_STATES else "RUNNING"


def is_active(eng: Any) -> bool:
    return chamber_state(eng) in ACTIVE_STATES

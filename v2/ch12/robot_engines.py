"""로봇 명령이 공정 쪽에서 받아야 하는 것 — 어댑터 인터페이스(RobotEngines)와 기본 레시피 폴더 스캔.

robot_ops 는 공정 엔진을 직접 모른다. core 가 이 Protocol 을 만족하는 어댑터를 붙인다(지금은 시험의 가짜 어댑터).
v1 host/handlers.py 가 runtime_state·ChamberRuntime·PlasmaCleaningRuntime 속성에서 읽던 '사실'만 넘기고, 그 사실로
상태 문자열·busy 사유·오류 코드를 만드는 판단은 robot_ops 가 v1 그대로 한다(판단이 두 곳에 생기지 않게).

메서드(예외를 내면 robot_ops 는 v1 이 그 자리 예외를 다루던 대로 — 아래 '예외' 칸)
- chamber_state(ch) -> dict  예외: GET_SPUTTER_STATUS 의 Chamber_n = 'error', busy 검사는 통과(v1 handlers.py:470-472)
    ready           : 챔버 런타임이 있음(없으면 START_SPUTTER E203 — v1 ctx.chN 이 None)
    chamber_running : runtime_state.is_running('chamber', ch)
    pc_running      : runtime_state.is_running('pc', ch)
    tsp_running     : runtime_state.is_running('tsp', 0) — ch 1 의 busy 검사만 본다
    runner_state    : Runner 상태 'IDLE'|'PREFLIGHT'|'RUNNING'|'CLEANUP'|'STOPPING'|'COOLDOWN'|'DELAY'|… (없으면 None)
    stage_alive     : Runner stage 작업이 살아 있음, stage_kind: 'ADVANCE_QUEUE'|'PROCESS'|'PREFLIGHT'|'CLEANUP'|None
    has_error       : runtime_state.has_error('chamber', ch)(마지막 공정 실패 이력)
- eta(ch) -> int|None            GET_SPUTTER_STATUS eta.CHn.remaining_s(확정 전·큐 마지막 행 전은 None — v1 _build_eta). 예외 → None
- min_total_s(ch) -> int|None    START_SPUTTER 성공 응답의 하한 보증 초(v1 _eta_min_total_s). 예외 → None
- start_sputter(ch, recipe, request_meta, on_preflight) -> None
    레시피 적재·검증(v1 start_with_recipe_string 의 로드 30 s·E207·E208·E209·E227·'레시피 오류 N건' 포함)을 하고 Runner 에
    시작을 넘긴다. 적재·검증 실패는 예외로(.code 가 있으면 그 코드 — v1 CodedRuntimeError, 없으면 RuntimeError 문구로 분류).
    시작 가드 결과는 on_preflight(ok, code, detail) 로 한 번(이 호출 안에서 불러도 된다). robot_ops 가
    CHAMBER_HOST_START_WAIT_TIMEOUT_S 까지 기다리고 넘으면 E700(v1 문구). request_meta = {key(Robot CSV 기록키), request_id, peer}
    — 엔진이 요청을 수락하면 Robot CSV mark_owned(key)·update·mark_started·finalize 를 부른다(v1 런타임 몫).
- pc_state() -> dict  예외: Loadlock 'error', START_PLASMA_CLEANING 대상은 'PC(CH1)'
    ready: 플라즈마 클리닝 런타임 있음(없으면 E204), running: {1: bool, 2: bool}(runtime_state.is_running('pc', ch)),
    ch1_ig_wait: CH1 공정이 IG 대기(IG_CMD) 중, cleaning: PC 런타임 is_running(없으면 None), selected_ch: PC 채널 선택
- start_pc(ch, recipe, request_meta, on_preflight) -> None   start_sputter 와 같은 모양(PC_HOST_START_WAIT_TIMEOUT_S, E206 은 robot_ops)
- last_pc_result() -> dict  {'finished': {1: t|None, 2: t|None}, 'error': {1: bool, 2: bool}} — 채널별 마지막 PC 종료 시각
    (monotonic)·실패 이력. Loadlock 'error' 판정(loadlock_error_channel). 예외 → 'error'
- recipe_folder_scan(folder, timeout_s) -> list[str]   GET_RECIPE — folder 는 전체 경로. 시간 초과는 TimeoutError(→ E227),
    폴더 없음 FileNotFoundError·폴더 아님 NotADirectoryError(→ E110, v1 문구). 어댑터에 없으면 scan_recipe_folder 를 쓴다.
"""
from __future__ import annotations

import os
import threading
from typing import Any, Callable, Dict, List, Optional, Protocol, runtime_checkable

PreflightFn = Callable[[bool, Optional[str], str], None]


@runtime_checkable
class RobotEngines(Protocol):
    def chamber_state(self, ch: int) -> Dict[str, Any]: ...

    def eta(self, ch: int) -> Optional[int]: ...

    def min_total_s(self, ch: int) -> Optional[int]: ...

    def start_sputter(self, ch: int, recipe: str, request_meta: Dict[str, Any], on_preflight: PreflightFn) -> None: ...

    def pc_state(self) -> Dict[str, Any]: ...

    def start_pc(self, ch: int, recipe: str, request_meta: Dict[str, Any], on_preflight: PreflightFn) -> None: ...

    def last_pc_result(self) -> Dict[str, Any]: ...

    def recipe_folder_scan(self, folder: str, timeout_s: float) -> List[str]: ...


ADAPTER_METHODS = ("chamber_state", "eta", "min_total_s", "start_sputter", "pc_state", "start_pc", "last_pc_result",
                   "recipe_folder_scan")


def scan_recipe_folder_sync(target_dir: str) -> List[str]:
    """v1 _scan_sync(host/handlers.py:708-720) — 재귀 없이 .csv/.xlsx(대소문자 무시) 파일 이름, 소문자 기준 정렬."""
    from pathlib import Path
    p = Path(target_dir)
    if not p.exists():
        raise FileNotFoundError(f"Recipe folder not found: {p}")
    if not p.is_dir():
        raise NotADirectoryError(f"Not a directory: {p}")
    files: List[str] = []
    with os.scandir(p) as it:
        for ent in it:
            if ent.is_file() and ent.name.lower().endswith((".csv", ".xlsx")):
                files.append(ent.name)
    files.sort(key=str.lower)
    return files


def scan_recipe_folder(target_dir: str, timeout_s: float) -> List[str]:
    """스레드에서 스캔하고 timeout_s 넘으면 TimeoutError(v1 asyncio.wait_for(to_thread(_scan_sync))). 늦은 스레드는 버린다."""
    box: Dict[str, Any] = {}

    def run() -> None:
        try:
            box["v"] = scan_recipe_folder_sync(target_dir)
        except BaseException as e:      # noqa: BLE001 — 그대로 부른 쪽에 다시 낸다
            box["e"] = e
    t = threading.Thread(target=run, name="recipe-scan", daemon=True)
    t.start()
    t.join(max(0.0, float(timeout_s)))
    if t.is_alive():
        raise TimeoutError(f"recipe scan timeout ({timeout_s}s)")
    if "e" in box:
        raise box["e"]
    return box["v"]


def loadlock_error_channel(last: Dict[str, Any]) -> Optional[int]:
    """v1 loadlock_error_channel(host/handlers.py:45-67) — 마지막으로 끝난 PC 가 실패였던 채널(채널 OR 아님).
    종료 이력이 하나도 없으면 실패 이력이 있는 채널(보수적)."""
    fin = last.get("finished") or {}
    err = last.get("error") or {}
    finished = [(float(fin[ch]), ch) for ch in (1, 2) if fin.get(ch) is not None]
    if finished:
        _, last_ch = max(finished, key=lambda t: t[0])
        return last_ch if bool(err.get(last_ch)) else None
    for ch in (1, 2):
        if bool(err.get(ch)):
            return ch
    return None

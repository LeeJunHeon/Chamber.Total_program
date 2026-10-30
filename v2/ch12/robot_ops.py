"""로봇 명령 처리(제어 프로그램 쪽) — v1 host/handlers.py(2,327줄)·host/router.py·host/setup.py 동작 그대로.

나눔(파일마다 책임 하나): robot_base(잠금·태그·PLC 구간·응답·busy) → robot_status(상태 질의·공정 시작) →
robot_vacuum(VACUUM_ON/OFF) → robot_motion(게이트·척·4핀) → 여기(RobotOps — 18개 명령 표·처리 경계).

처리 경계 handle(cmd, data, raw, peer, received_at, request_id) → (reply_cmd, body, log)
- 명령 이름은 upper().strip() 로 찾고 응답은 '{명령}_RESULT'(v1 router). 게이트·척 명령은 data 의 ch 를 덮어쓴다(v1 setup.py:59-66).
- 등록 안 된 명령은 '{C}_RESULT' E106, 빈 명령은 'UNKNOWN_RESULT' E105(v1 router — 보고 없음). 로봇 서버가 먼저 거르므로
  보통 오지 않는다.
- 핸들러 예외(v1 host/server.py:343-361): 로그 NET 'Handler error for {원문 명령}: {e}\n{traceback}', E999(AppError·.code
  예외면 그 코드 — notify_unknown_error) + meta {peer, request_id, cmd(원문)} 보고, 응답 명령은 정규화하지 않은 원문 + '_RESULT'.
- log = 이 요청이 남긴 (태그, 본문) 줄: PLC_REMOTE(PLC 입출력·VACUUM_ON 진단), ERROR/HOST(실패 보고), NET(핸들러 예외).
- 요청 정보(request_id·peer·received_at)는 Robot CSV 기록키·요청 공정 로그(origin_meta)에 쓰인다. peer 는 v1 str(peername)
  모양("('127.0.0.1', 50123)")으로 되돌린다.
동시 요청: 스레드마다 따로 처리(v1 은 연결마다 태스크) — 잠금은 robot_base.ClockLock(v1 asyncio.Lock 과 같은 자리).
"""
from __future__ import annotations

import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

from v2.ch12 import errors as ER
from v2.ch12.robot_base import Json, Scope, _scope_var, _tag_var, parse_received_at, peer_text
from v2.ch12.robot_motion import MotionOps

COMMANDS: Tuple[str, ...] = (
    "GET_SPUTTER_STATUS", "GET_LOADING_1_SENSOR", "GET_LOADING_2_SENSOR", "GET_RECIPE", "START_SPUTTER",
    "START_PLASMA_CLEANING", "VACUUM_ON", "VACUUM_OFF", "4PIN_UP", "4PIN_DOWN", "CH1_GATE_OPEN", "CH2_GATE_OPEN",
    "CH1_GATE_CLOSE", "CH2_GATE_CLOSE", "CH1_CHUCK_UP", "CH2_CHUCK_UP", "CH1_CHUCK_DOWN", "CH2_CHUCK_DOWN",
)   # v1 host/setup.py:49-66 등록 순서

# 명령 → (메서드 이름, 덮어쓸 ch | None)
_ROUTES: Dict[str, Tuple[str, Optional[int]]] = {
    "GET_SPUTTER_STATUS": ("get_sputter_status", None),
    "GET_LOADING_1_SENSOR": ("get_loading_1_sensor", None), "GET_LOADING_2_SENSOR": ("get_loading_2_sensor", None),
    "GET_RECIPE": ("get_recipe", None), "START_SPUTTER": ("start_sputter", None),
    "START_PLASMA_CLEANING": ("start_plasma_cleaning", None),
    "VACUUM_ON": ("vacuum_on", None), "VACUUM_OFF": ("vacuum_off", None),
    "4PIN_UP": ("four_pin_up", None), "4PIN_DOWN": ("four_pin_down", None),
    "CH1_GATE_OPEN": ("gate_open", 1), "CH2_GATE_OPEN": ("gate_open", 2),
    "CH1_GATE_CLOSE": ("gate_close", 1), "CH2_GATE_CLOSE": ("gate_close", 2),
    "CH1_CHUCK_UP": ("chuck_up", 1), "CH2_CHUCK_UP": ("chuck_up", 2),
    "CH1_CHUCK_DOWN": ("chuck_down", 1), "CH2_CHUCK_DOWN": ("chuck_down", 2),
}
assert tuple(_ROUTES) == COMMANDS


class RobotOps(MotionOps):
    """생성: RobotOps(clock, plc_link, cfg, engines=None, robot_csv=None, on_report=None, log=None)
    - plc_link: shared.PlcLink(proxy('ROBOT')·begin_robot/end_robot), cfg: 공용 설정 사전,
    - engines: robot_engines.RobotEngines 어댑터(core 가 붙인다 — 없으면 상태는 idle, 공정 시작은 E203/E204),
    - robot_csv: outputs.robot_csv.RobotCsv(없으면 기록 안 함), on_report(payload, src): 실패 보고(포털 카드 — D09),
    - log(tag, text): 줄을 바로 내보낼 곳(선택 — 응답 log 에는 늘 담긴다)."""

    def get_loading_1_sensor(self, payload: Json) -> Json:
        return self.get_loading_sensor(1, payload)

    def get_loading_2_sensor(self, payload: Json) -> Json:
        return self.get_loading_sensor(2, payload)

    def handler_for(self, cmd: str) -> Optional[Callable[[Json], Json]]:
        r = _ROUTES.get(str(cmd or "").upper().strip())
        if r is None:
            return None
        fn = getattr(self, r[0])
        if r[1] is None:
            return fn
        ch = r[1]
        return lambda d: fn({**d, "ch": ch})

    def handle(self, cmd: str, data: Any, raw: Any = None, peer: str = "", received_at: Any = None,
               request_id: Optional[str] = None) -> Tuple[str, Any, List[List[str]]]:
        raw = raw if isinstance(raw, dict) else {}
        rid = str(raw.get("request_id", "")) if request_id is None else str(request_id)
        raw_cmd = str(raw.get("command", cmd if cmd is not None else ""))
        scope = Scope(request_id=rid, peer=peer_text(peer), received_at=parse_received_at(received_at))
        tok_s = _scope_var.set(scope)
        tok_t = _tag_var.set(None)
        try:
            reply_cmd, body = self._dispatch(cmd, data if isinstance(data, dict) else {}, raw_cmd, scope)
        finally:
            _tag_var.reset(tok_t)
            _scope_var.reset(tok_s)
        return reply_cmd, body, [[t, x] for t, x in scope.lines]

    def _dispatch(self, cmd: str, data: Json, raw_cmd: str, scope: Scope) -> Tuple[str, Any]:
        c = str(cmd or "").upper().strip()
        if not c:
            return "UNKNOWN_RESULT", ER.build_error_payload_from_code("E105", detail="Missing command",
                                                                     meta={"command": cmd})
        h = self.handler_for(c)
        if h is None:
            return f"{c}_RESULT", ER.build_error_payload_from_code("E106", detail=f"Unknown command: {cmd}",
                                                                  meta={"command": cmd})
        try:
            res = h(data)
            if not isinstance(res, dict):
                raise ER.AppError("E111", detail=f"Handler returned non-dict: {type(res).__name__} raw={res!r}",
                                  meta={"command": c})
            return f"{c}_RESULT", res
        except Exception as e:                       # v1 host/server.py:343-361
            tb = traceback.format_exc()
            _tag_var.set(None)
            self.log("NET", f"Handler error for {raw_cmd}: {e}\n{tb}")
            fail = self.notify_unknown(e, meta={"peer": scope.peer, "request_id": scope.request_id, "cmd": raw_cmd})
            return f"{raw_cmd}_RESULT", fail

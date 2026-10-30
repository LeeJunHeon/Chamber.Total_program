"""제어 프로그램이 꺼져 있거나 재시작 중일 때의 응답 — 마지막 상태 캐시와 거부 응답(판단은 하지 않는다).

규칙(사용자 지시 3장 (5) — 결정 목록에 올린다)
- 로봇 연결은 끊지 않는다(서버는 평소처럼 응답을 보내고 다음 요청을 기다린다).
- 상태 질의(GET_SPUTTER_STATUS·GET_LOADING_1/2_SENSOR·GET_RECIPE): 마지막으로 받은 정상 응답(result 'success')을 기존
  형식 그대로 준다. GET_SPUTTER_STATUS 는 '공정 중'으로 바꾼다 — v1 응답에서 busy 를 나타내는 필드는
  Chamber_1·Chamber_2·Loadlock_Chamber('idle'|'running'|'error', host/handlers.py:543-665 — Runner 가 PREFLIGHT·COOLDOWN·
  DELAY 등 바쁘면 'running')뿐이라 셋 다 'running' 으로, eta 의 remaining_s 는 null 로(지난 값은 이미 틀리고, null 은 v1 의
  '확정 전' 값 — controller/process_controller.py:1717-1780). vacuum 은 마지막 값 그대로.
  GET_LOADING_1/2_SENSOR·GET_RECIPE 에는 busy 필드가 없어 마지막 응답 그대로(value·files).
- 캐시가 없으면(한 번도 못 받음): v1 오류 코드 중 RETRY 계열로 — GET_SPUTTER_STATUS → E203 '챔버 런타임 미준비',
  GET_LOADING_n_SENSOR → E412 'PLC 비트 읽기 실패'(그 명령의 v1 읽기 실패 코드), GET_RECIPE → E227 '레시피 조회 시간 초과'
  (그 명령의 v1 시간 초과 코드).
- 시작·이동 명령(START_SPUTTER·START_PLASMA_CLEANING·VACUUM_ON/OFF·4PIN_UP/DOWN·게이트·척): E205 '공정 실행 중'
  (RETRY / STATE / WARN) — 상태 질의가 '공정 중'을 보여 주는 것과 같은 뜻이고, v1 이 busy 거절에 쓰는 코드
  (_fail_if_ch_busy, host/handlers.py:389-474)라 로봇이 이미 '잠시 뒤 다시'로 처리한다. 문구는 v1 busy 문구 모양
  '{명령} 불가 — 제어 프로그램 응답 없음(재시작 중)', meta 없음(v1 1차 busy 거절과 같은 모양).
- 캐시는 디스크에도(ROBOT_CACHE_PATH, 원자적 저장 — 임시 파일 → os.replace). 내용이 바뀔 때만 쓴다. 키 = 명령
  (GET_RECIPE 는 folder.strip().upper() 별).
"""
from __future__ import annotations

import copy
import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from v2.robot_server.forward import STATUS_COMMANDS
from v2.robot_server.protocol import error_payload

Json = Dict[str, Any]
DOWN_DETAIL = "{cmd} 불가 — 제어 프로그램 응답 없음(재시작 중)"
NO_CACHE_CODE = {"GET_SPUTTER_STATUS": "E203", "GET_LOADING_1_SENSOR": "E412", "GET_LOADING_2_SENSOR": "E412",
                 "GET_RECIPE": "E227"}
BUSY_CODE = "E205"
RUNNING = "running"


def cache_key(cmd: str, data: Any) -> str:
    if cmd == "GET_RECIPE":
        folder = ""
        if isinstance(data, dict):
            folder = str(data.get("folder") or "").strip().upper()
        return f"GET_RECIPE|{folder}"
    return cmd


def as_busy(cmd: str, body: Json) -> Json:
    """마지막 응답을 '공정 중'으로 — 같은 키·순서, 값만 바꾼다."""
    b = copy.deepcopy(body)
    if cmd == "GET_SPUTTER_STATUS":
        for k in ("Chamber_1", "Chamber_2", "Loadlock_Chamber"):
            if k in b:
                b[k] = RUNNING
        eta = b.get("eta")
        if isinstance(eta, dict):
            for v in eta.values():
                if isinstance(v, dict) and "remaining_s" in v:
                    v["remaining_s"] = None
    return b


class ReplyCache:
    def __init__(self, path: Optional[Path]) -> None:
        self.path = Path(path) if path else None
        self._lock = threading.Lock()
        self._mem: Dict[str, Json] = {}
        self.load_error: Optional[str] = None
        self._load()

    def _load(self) -> None:
        if self.path is None or not self.path.exists():
            return
        try:
            obj = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(obj, dict) and isinstance(obj.get("replies"), dict):
                self._mem = {k: v for k, v in obj["replies"].items()
                             if isinstance(v, dict) and isinstance(v.get("body"), dict)}
        except Exception as e:      # 깨진 캐시는 없는 것과 같게(기록만)
            self.load_error = repr(e)
            self._mem = {}

    def get(self, key: str) -> Optional[Json]:
        with self._lock:
            e = self._mem.get(key)
            return copy.deepcopy(e) if e else None

    def put(self, cmd: str, data: Any, body: Any) -> bool:
        """상태 질의의 정상 응답만 저장. 바뀌었으면 True(디스크에도 씀)."""
        if cmd not in STATUS_COMMANDS or not isinstance(body, dict) or body.get("result") != "success":
            return False
        key = cache_key(cmd, data)
        with self._lock:
            old = self._mem.get(key)
            if old is not None and old.get("body") == body:
                return False
            self._mem[key] = {"body": copy.deepcopy(body), "at": datetime.now().isoformat(timespec="seconds")}
            snap = copy.deepcopy(self._mem)
        self._save(snap)
        return True

    def _save(self, snap: Dict[str, Json]) -> None:
        if self.path is None:
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_name(self.path.name + ".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"version": 1, "replies": snap}, f, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        except Exception:
            pass

    def down_reply(self, cmd: str, data: Any) -> Tuple[str, Json, bool]:
        """제어 프로그램 꺼짐 때 응답 (reply_cmd, data, 캐시 사용 여부)."""
        reply_cmd = f"{cmd}_RESULT"
        if cmd in STATUS_COMMANDS:
            e = self.get(cache_key(cmd, data))
            if e is not None:
                return reply_cmd, as_busy(cmd, e["body"]), True
            return reply_cmd, error_payload(NO_CACHE_CODE[cmd], detail=DOWN_DETAIL.format(cmd=cmd)), False
        return reply_cmd, error_payload(BUSY_CODE, detail=DOWN_DETAIL.format(cmd=cmd)), False

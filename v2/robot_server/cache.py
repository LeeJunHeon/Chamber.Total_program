"""제어 프로그램이 꺼져 있거나 재시작 중일 때의 응답 — 마지막 상태 캐시와 거부 응답(판단은 하지 않는다).

규칙(사용자 지시 3장 (5) + 2026-11-09 D2·D3 — 결정 목록에 올린다). 이 규칙은 '내부 명령 API 에 연결 자체가 안 될 때'(연결 거부·접속 시간 초과·진행 중
연결 끊김)만 쓴다 — 응답이 느릴 뿐이면 서버는 끝까지 기다린다(forward.py).
- 로봇 연결은 끊지 않는다(서버는 평소처럼 응답을 보내고 다음 요청을 기다린다).
- GET_SPUTTER_STATUS: 마지막으로 받은 정상 응답(result 'success')을 v1 형식 그대로 — 값을 바꾸지 않는다(이전에는 Chamber_n·Loadlock 을 'running',
  remaining_s 를 null 로 바꿨다 — 로봇이 지난 상태를 '공정 중'으로 착각하지 않게 했지만 응답을 꾸미는 것이라 없앴다). 그 상태가 몇 초 전 것인지는
  서버 로그에만('last reply age=Ns'). 캐시가 없으면 E203 '챔버 런타임 미준비'(RETRY).
- GET_LOADING_1/2_SENSOR: 캐시 값을 절대 보내지 않는다(센서 값은 지난 값이 안전하지 않음) — 디스크 캐시에도 저장하지 않고 옛 캐시 파일에 있어도 읽지 않는다.
  연결 불가면 E205(공정 실행 중 모양 — RETRY).
- GET_RECIPE: 마지막 정상 응답(폴더별) 그대로, 없으면 E227 — 사용자 확인 필요(레시피 목록은 지난 값이 틀릴 수 있다).
- 시작·이동 명령(START_SPUTTER·START_PLASMA_CLEANING·VACUUM_ON/OFF·4PIN_UP/DOWN·게이트·척): E205 '공정 실행 중'(RETRY / STATE / WARN) —
  v1 이 busy 거절에 쓰는 코드(_fail_if_ch_busy, host/handlers.py:389-474)라 로봇이 이미 '잠시 뒤 다시'로 처리한다. 문구는 v1 busy 문구 모양
  '{명령} 불가 — 제어 프로그램 응답 없음(재시작 중)', meta 없음.
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
NO_CACHE_CODE = {"GET_SPUTTER_STATUS": "E203", "GET_RECIPE": "E227"}
CACHED_COMMANDS = ("GET_SPUTTER_STATUS", "GET_RECIPE")      # 센서 값은 캐시하지 않는다(11-09 D2)
BUSY_CODE = "E205"


def cache_key(cmd: str, data: Any) -> str:
    if cmd == "GET_RECIPE":
        folder = ""
        if isinstance(data, dict):
            folder = str(data.get("folder") or "").strip().upper()
        return f"GET_RECIPE|{folder}"
    return cmd


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
                             if isinstance(v, dict) and isinstance(v.get("body"), dict)
                             and k.split("|")[0] in CACHED_COMMANDS}      # 옛 파일의 센서 값은 읽지 않는다
        except Exception as e:      # 깨진 캐시는 없는 것과 같게(기록만)
            self.load_error = repr(e)
            self._mem = {}

    def get(self, key: str) -> Optional[Json]:
        with self._lock:
            e = self._mem.get(key)
            return copy.deepcopy(e) if e else None

    def put(self, cmd: str, data: Any, body: Any) -> bool:
        """상태 질의의 정상 응답만 저장. 바뀌었으면 True(디스크에도 씀)."""
        if cmd not in CACHED_COMMANDS or not isinstance(body, dict) or body.get("result") != "success":
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
        """제어 프로그램 연결 불가 때 응답 (reply_cmd, data, 캐시 사용 여부) — 캐시 값은 v1 형식 그대로(바꾸지 않음)."""
        reply_cmd = f"{cmd}_RESULT"
        if cmd in CACHED_COMMANDS:
            e = self.get(cache_key(cmd, data))
            if e is not None:
                return reply_cmd, e["body"], True
            return reply_cmd, error_payload(NO_CACHE_CODE[cmd], detail=DOWN_DETAIL.format(cmd=cmd)), False
        return reply_cmd, error_payload(BUSY_CODE, detail=DOWN_DETAIL.format(cmd=cmd)), False

    def age_s(self, cmd: str, data: Any) -> Optional[float]:
        """캐시된 마지막 응답의 나이(초) — 서버 로그에만 쓴다."""
        e = self.get(cache_key(cmd, data))
        try:
            return None if e is None else max(0.0, (datetime.now() - datetime.fromisoformat(e["at"])).total_seconds())
        except Exception:
            return None

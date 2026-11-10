"""내부 명령 API 클라이언트 — 등록된 로봇 명령을 제어 프로그램으로 넘긴다(판단하지 않는다).

계약(제어 프로그램 쪽 v2/ch12/robot_api.py 가 지킨다)
- HTTP/1.1, 127.0.0.1:ROBOT_API_PORT(기본 50071), POST /robot/v1/command, Content-Type application/json; charset=utf-8.
- 요청 본문: {"id": "<uuid4 hex>", "cmd": "<정규화된 명령 — upper().strip()>", "data": {요청 data — dict 가 아니면 {}},
             "raw": {요청 JSON 전체}, "peer": "ip:port", "received_at": "ISO 8601(로컬 시각, 바디 파싱 직후)"}
- 응답: 200 {"reply_cmd": "<응답 헤더에 넣을 명령>", "body": {응답 data — v1 handlers 와 같은 필드·순서},
             "log": ["[TAG] 본문", ...]}
  · reply_cmd: 보통 '{cmd}_RESULT'. 핸들러 예외(v1 host/server.py:343-361)면 원문 명령(raw["command"]) + '_RESULT',
    body 는 E999(AppError 면 그 코드) — v1 이상 동작을 제어 프로그램이 그대로 만든다.
  · log: 처리 중 서버 로그에 남길 줄(PLC_REMOTE·ERROR/HOST·'Handler error for ..' 등). 각 줄은 '[TAG] 본문' 문자열
    (본문에 줄바꿈 가능) 또는 [tag, 본문] 쌍. 태그 없는 문자열은 HOST 태그로 남긴다.
  · body 직렬화: 로봇 서버가 받은 객체를 다시 json.dumps(ensure_ascii=False, default=str) 하므로 키 순서만 지키면 된다.
- 200 이 아니거나 본문이 위 모양이 아니면 '연결 안 됨'과 같게 다룬다(ForwardDown).
- 실패 보고(결정 D09): POST /robot/v1/event 본문 {"kind": "error_event", "code": "E###", "detail": "[E###] 제목 | 상세: …"
  (v1 챗·팝업 문구 _build_user_text), "level": "warn", "src": "HOST", "title": "오류(HOST, E###)"(v1 팝업 제목),
  "payload": {실패 data}} → 200 {"ok": true}. send_event 는 실패해도 예외 없이 False(제어 프로그램이 꺼져 있으면 로그만).

응답을 기다리는 시간(11-09 D1): 한도를 두지 않는다 — v1 처럼 실제 결과를 끝까지 기다린다(상태 질의·긴 시퀀스 모두). 시간 한도로 E205 를 내면
  제어 프로그램이 아직 일하는 중인데 로봇이 '실패'로 알고 다시 보내 같은 동작이 겹칠 수 있다. E205(제어 프로그램 응답 없음)는 내부 API 에
  연결 자체가 안 될 때(연결 거부·접속 시간 초과 ROBOT_API_CONNECT_TIMEOUT_S)와 진행 중 연결이 끊겼을 때(HTTP 중간 끊김·비정상 응답)만.
  (각 명령이 걸리는 시간은 v1 응답 시점 — robot_v1facts.COMMANDS[*]['reply_timing'] — 그대로이고 로봇이 자기 한도로 기다린다.)
"""
from __future__ import annotations

import asyncio
import json
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

from v2.ch12.params.robot import robot_cfg

Json = Dict[str, Any]

COMMANDS = (
    "GET_SPUTTER_STATUS", "GET_LOADING_1_SENSOR", "GET_LOADING_2_SENSOR", "GET_RECIPE",
    "START_SPUTTER", "START_PLASMA_CLEANING", "VACUUM_ON", "VACUUM_OFF", "4PIN_UP", "4PIN_DOWN",
    "CH1_GATE_OPEN", "CH2_GATE_OPEN", "CH1_GATE_CLOSE", "CH2_GATE_CLOSE",
    "CH1_CHUCK_UP", "CH2_CHUCK_UP", "CH1_CHUCK_DOWN", "CH2_CHUCK_DOWN",
)   # v1 host/setup.py:49-66 등록 순서
STATUS_COMMANDS = ("GET_SPUTTER_STATUS", "GET_LOADING_1_SENSOR", "GET_LOADING_2_SENSOR", "GET_RECIPE")
API_PATH = "/robot/v1/command"
EVENT_PATH = "/robot/v1/event"
EVENT_TIMEOUT_S = 3.0          # 실패 보고 한 건의 한도(응답·로그를 막지 않는 별도 태스크)
API_HOST = "127.0.0.1"
_LOG_RE = re.compile(r"^\[(?P<tag>[A-Za-z0-9_/]+)\] ?(?P<text>.*)$", re.DOTALL)


class ForwardDown(Exception):
    """제어 프로그램에 닿지 못함(연결 거부·시간 초과·잘못된 응답)."""


def parse_log_entries(entries: Any) -> List[Tuple[str, str]]:
    """응답 log 목록 → [(tag, text)]."""
    out: List[Tuple[str, str]] = []
    if not isinstance(entries, list):
        return out
    for e in entries:
        if isinstance(e, (list, tuple)) and len(e) == 2:
            out.append((str(e[0]), str(e[1])))
            continue
        s = str(e)
        m = _LOG_RE.match(s)
        out.append((m.group("tag"), m.group("text")) if m else ("HOST", s))
    return out


class Forwarder:
    """내부 명령 API 로 한 명령을 넘기고 (reply_cmd, body, log) 를 돌려준다."""

    def __init__(self, cfg: Optional[dict] = None, host: str = API_HOST) -> None:
        self.cfg = robot_cfg(cfg)
        self.host = host
        self.port = int(self.cfg["ROBOT_API_PORT"])
        self.connect_timeout_s = float(self.cfg["ROBOT_API_CONNECT_TIMEOUT_S"])

    async def forward(self, cmd: str, data: Json, raw: Any, peer: str, received_at: str,
                      timeout_s: Optional[float] = None) -> Tuple[str, Any, List[Tuple[str, str]]]:
        """timeout_s 는 시험·진단용(기본 None = 끝까지 기다림 — 11-09 D1)."""
        req = {"id": uuid.uuid4().hex, "cmd": cmd, "data": data, "raw": raw, "peer": peer, "received_at": received_at}
        body = json.dumps(req, ensure_ascii=False, default=str).encode("utf-8")
        try:
            if timeout_s is None:
                status, resp = await self._post(body)
            else:
                status, resp = await asyncio.wait_for(self._post(body), timeout=float(timeout_s))
        except ForwardDown:
            raise
        except asyncio.TimeoutError:
            raise ForwardDown(f"응답 시간 초과({float(timeout_s or 0):.1f}s)")
        except (OSError, ConnectionError) as e:
            raise ForwardDown(f"연결 실패: {e!r}")
        except Exception as e:
            raise ForwardDown(f"통신 오류: {e!r}")
        if status != 200:
            raise ForwardDown(f"HTTP {status}")
        try:
            obj = json.loads(resp.decode("utf-8"))
        except Exception as e:
            raise ForwardDown(f"응답 JSON 오류: {e!r}")
        if not isinstance(obj, dict) or not isinstance(obj.get("reply_cmd"), str) or "body" not in obj:
            raise ForwardDown("응답 형식 오류(reply_cmd·body 없음)")
        return obj["reply_cmd"], obj["body"], parse_log_entries(obj.get("log", []))

    async def send_event(self, event: Json, timeout_s: float = EVENT_TIMEOUT_S) -> bool:
        """실패 보고 이벤트 한 건(D09) — 받았으면 True. 연결 안 됨·시간 초과·200 아님은 False(예외 없음)."""
        body = json.dumps(event, ensure_ascii=False, default=str).encode("utf-8")
        try:
            status, _ = await asyncio.wait_for(self._post(body, EVENT_PATH), timeout=float(timeout_s))
        except (Exception, asyncio.TimeoutError):
            return False
        return status == 200

    async def _post(self, body: bytes, path: str = API_PATH) -> Tuple[int, bytes]:
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(self.host, self.port),
                                                    timeout=self.connect_timeout_s)
        except asyncio.TimeoutError:
            raise ForwardDown(f"접속 시간 초과({self.connect_timeout_s:.1f}s)")
        try:
            head = (f"POST {path} HTTP/1.1\r\nHost: {self.host}:{self.port}\r\n"
                    f"Content-Type: application/json; charset=utf-8\r\nContent-Length: {len(body)}\r\n"
                    f"Connection: close\r\n\r\n").encode("ascii")
            writer.write(head + body)
            await writer.drain()
            status_line = await reader.readline()
            parts = status_line.decode("latin-1").split(None, 2)
            if len(parts) < 2 or not parts[0].startswith("HTTP/"):
                raise ForwardDown(f"HTTP 상태 줄 오류: {status_line[:80]!r}")
            status = int(parts[1])
            length: Optional[int] = None
            chunked = False
            while True:
                line = await reader.readline()
                if not line:
                    raise ForwardDown("HTTP 헤더 중 연결 끊김")
                if line in (b"\r\n", b"\n"):
                    break
                k, _, v = line.decode("latin-1").partition(":")
                k = k.strip().lower()
                if k == "content-length":
                    length = int(v.strip())
                elif k == "transfer-encoding" and "chunked" in v.lower():
                    chunked = True
            if chunked:
                data = b""
                while True:
                    size = int((await reader.readline()).split(b";")[0].strip() or b"0", 16)
                    if size == 0:
                        break
                    data += await reader.readexactly(size)
                    await reader.readline()
            elif length is not None:
                data = await reader.readexactly(length)
            else:
                data = await reader.read()
            return status, data
        except asyncio.IncompleteReadError as e:
            raise ForwardDown(f"HTTP 본문 중 연결 끊김: {e!r}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

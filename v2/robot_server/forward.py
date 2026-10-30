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

응답 한도(명령마다, v1 응답 시점 = robot_v1facts.COMMANDS[*]['reply_timing'] 근거, 여유 ROBOT_API_TIMEOUT_MARGIN_S 30 s)
- GET_SPUTTER_STATUS·GET_LOADING_1/2_SENSOR: ROBOT_API_STATUS_TIMEOUT_S 5 s(즉시 응답 — 메모리 판정 + PLC 1비트)
- GET_RECIPE: RECIPE_SCAN_TIMEOUT_S 20 + 여유
- START_SPUTTER: CSV 로드 30 s(chamber_runtime.py:8059-8065) + CHAMBER_HOST_START_WAIT_TIMEOUT_S 20 + 여유
- START_PLASMA_CLEANING: CSV 로드 30 s(plasma_cleaning_runtime.py:3249-3255) + PC_HOST_START_WAIT_TIMEOUT_S 20 + 여유
- VACUUM_ON: timeout_s(기본 660) + 뒤처리 최대 35 s(READY 뒤 러핑 OFF 확인 5 + 재정리 5+10 + finally 5 + 시작 펌프 5 s
  + 0.3) + 여유  (handlers.py:1205-1676)
- VACUUM_OFF: timeout_s(기본 240) + 앞 정리 5 s(handlers.py:1678-1774) + 여유
- 4PIN_UP/DOWN: settle_s(5) + wait_s(20) + 펄스 0.18 + 여유 (handlers.py:1777-1886)
- CHx_GATE_OPEN/CLOSE: 펄스 0.18 + wait_s(5) + 여유 (handlers.py:1889-2142)
- CHx_CHUCK_UP: CHUCK_POSITION_RESOLVE_TIMEOUT_S 90 + 0.2 + wait_s(90) + 여유 (handlers.py:2195-2327)
- CHx_CHUCK_DOWN: 0.2 + wait_s(90) + 여유
  요청 data 의 timeout_s·wait_s·settle_s 가 숫자면 그 값으로 계산한다(v1 핸들러가 그 값을 쓰므로). 다른 명령이 같은 잠금
  (lock_ch·게이트 잠금)을 쥐고 있어 기다리는 시간은 넣지 않았다 — 여유 30 s 를 넘는 잠금 대기는 '응답 없음'이 된다.
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
API_HOST = "127.0.0.1"
_PULSE_S = 0.18      # PLC_CMD_PULSE_MS 180 (lib/config_common.py:636)
_LOG_RE = re.compile(r"^\[(?P<tag>[A-Za-z0-9_/]+)\] ?(?P<text>.*)$", re.DOTALL)


class ForwardDown(Exception):
    """제어 프로그램에 닿지 못함(연결 거부·시간 초과·잘못된 응답)."""


def _num(data: Json, key: str, default: float) -> float:
    try:
        v = data.get(key, default)
        if isinstance(v, bool):
            return default
        return float(v)
    except Exception:
        return default


def reply_timeout_s(cmd: str, data: Json, cfg: Optional[dict] = None) -> float:
    """명령별 응답 한도(초) — 모듈 독스트링 표."""
    c = robot_cfg(cfg)
    margin = float(c["ROBOT_API_TIMEOUT_MARGIN_S"])
    data = data if isinstance(data, dict) else {}
    if cmd in ("GET_SPUTTER_STATUS", "GET_LOADING_1_SENSOR", "GET_LOADING_2_SENSOR"):
        return float(c["ROBOT_API_STATUS_TIMEOUT_S"])
    if cmd == "GET_RECIPE":
        return float(c["RECIPE_SCAN_TIMEOUT_S"]) + margin
    if cmd == "START_SPUTTER":
        return 30.0 + float(c["CHAMBER_HOST_START_WAIT_TIMEOUT_S"]) + margin
    if cmd == "START_PLASMA_CLEANING":
        return 30.0 + float(c["PC_HOST_START_WAIT_TIMEOUT_S"]) + margin
    if cmd == "VACUUM_ON":
        return _num(data, "timeout_s", 660.0) + 35.3 + margin
    if cmd == "VACUUM_OFF":
        return _num(data, "timeout_s", 240.0) + 5.0 + margin
    if cmd in ("4PIN_UP", "4PIN_DOWN"):
        return _num(data, "settle_s", 5.0) + _num(data, "wait_s", 20.0) + _PULSE_S + margin
    if cmd.endswith("_GATE_OPEN") or cmd.endswith("_GATE_CLOSE"):
        return _PULSE_S + _num(data, "wait_s", 5.0) + margin
    if cmd.endswith("_CHUCK_UP"):
        return float(c["CHUCK_POSITION_RESOLVE_TIMEOUT_S"]) + 0.2 + _num(data, "wait_s", 90.0) + margin
    if cmd.endswith("_CHUCK_DOWN"):
        return 0.2 + _num(data, "wait_s", 90.0) + margin
    return float(c["ROBOT_API_STATUS_TIMEOUT_S"]) + margin


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

    def timeout_for(self, cmd: str, data: Json) -> float:
        return reply_timeout_s(cmd, data, self.cfg)

    async def forward(self, cmd: str, data: Json, raw: Any, peer: str, received_at: str,
                      timeout_s: Optional[float] = None) -> Tuple[str, Any, List[Tuple[str, str]]]:
        req = {"id": uuid.uuid4().hex, "cmd": cmd, "data": data, "raw": raw, "peer": peer, "received_at": received_at}
        body = json.dumps(req, ensure_ascii=False, default=str).encode("utf-8")
        t = self.timeout_for(cmd, data) if timeout_s is None else float(timeout_s)
        try:
            status, resp = await asyncio.wait_for(self._post(body), timeout=t)
        except ForwardDown:
            raise
        except asyncio.TimeoutError:
            raise ForwardDown(f"응답 시간 초과({t:.1f}s)")
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

    async def _post(self, body: bytes) -> Tuple[int, bytes]:
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(self.host, self.port),
                                                    timeout=self.connect_timeout_s)
        except asyncio.TimeoutError:
            raise ForwardDown(f"접속 시간 초과({self.connect_timeout_s:.1f}s)")
        try:
            head = (f"POST {API_PATH} HTTP/1.1\r\nHost: {self.host}:{self.port}\r\n"
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

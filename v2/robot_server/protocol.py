"""로봇(호스트) 프레임 — 고정 16바이트 헤더 + UTF-8 JSON 바디 (v1 host/protocol.py 그대로).

헤더 '!BBHIQ'(빅엔디언): version u8, flags u8, cmd_len u16, body_len u32, timestamp u64.
- 응답: version=1, flags=0, cmd_len=len(응답 명령.encode('utf-8')), body_len=len(바디), timestamp=int(time.time()) 초.
- 바디: {"request_id", "command", "data"} 이 순서, json.dumps(ensure_ascii=False, default=str, 기본 구분자).
- 요청 헤더에서 쓰는 것은 version·body_len 뿐(flags·cmd_len·timestamp 는 읽고 버림 — v1 host/server.py:187).

여기에는 프레임 만들기/풀기와 서버가 직접 만드는 실패 응답(errors 표)만 둔다. 판단은 하지 않는다.
v1 근거: host/protocol.py:13-46, errors/error_payload.py:27-62, errors/error_reporter.py:27-107.
"""
from __future__ import annotations

import json
import struct
import time
from typing import Any, Callable, Dict, Optional, Tuple

HEADER_FMT = "!BBHIQ"
HEADER_SIZE = struct.calcsize(HEADER_FMT)
PROTOCOL_VERSION = 1

Json = Dict[str, Any]

# 서버·꺼짐 처리가 직접 만드는 오류 코드 정의 — v1 errors/error_registry.py 값 그대로
# (코드 → title, category, client_action, severity, cause, fix). 시험이 v1 골든(error_defs)과 비교한다.
ERROR_DEFS: Dict[str, Tuple[str, str, str, str, str, str]] = {
    "E102": ("지원하지 않는 프로토콜 버전", "TRANSPORT", "STOP", "ERROR",
             "서버가 지원하지 않는 프로토콜 버전으로 요청이 들어왔습니다.", "클라이언트와 서버의 프로토콜 버전을 일치시키십시오."),
    "E104": ("잘못된 JSON 본문", "TRANSPORT", "STOP", "ERROR",
             "요청 본문 JSON 디코딩에 실패했습니다.", "클라이언트가 전송한 JSON 형식과 인코딩을 확인하십시오."),
    "E105": ("명령 누락", "REQUEST", "STOP", "ERROR",
             "요청 본문에 command 필드가 없습니다.", "클라이언트가 command를 포함하도록 수정하십시오."),
    "E106": ("알 수 없는 명령", "REQUEST", "STOP", "ERROR",
             "등록되지 않은 command가 요청되었습니다.", "클라이언트 명령명을 서버 라우터 등록값과 일치시키십시오."),
    "E107": ("응답 직렬화 실패", "TRANSPORT", "STOP", "ERROR",
             "응답 객체를 JSON으로 직렬화하는 중 실패했습니다.", "응답 payload 안에 직렬화 불가능한 객체가 포함되었는지 확인하십시오."),
    "E110": ("핸들러 내부 예외", "INTERNAL", "STOP", "ERROR",
             "핸들러 내부에서 예외가 발생했지만, 명시적인 에러코드로 정규화되지 않았습니다.",
             "해당 핸들러 내부 예외를 AppError로 명시적으로 변환하십시오."),
    "E111": ("핸들러 반환 형식 오류", "INTERNAL", "STOP", "ERROR",
             "핸들러가 dict가 아닌 응답을 반환했습니다.", "모든 핸들러가 dict 형태의 응답을 반환하도록 수정하십시오."),
    "E203": ("챔버 런타임 미준비", "RUNTIME", "RETRY", "WARN",
             "챔버 런타임이 아직 준비되지 않았습니다.", "런타임 초기화 완료 후 다시 시도하십시오."),
    "E205": ("공정 실행 중", "STATE", "RETRY", "WARN",
             "이미 다른 공정이 실행 중이어서 현재 명령을 처리할 수 없습니다.", "현재 공정 종료 후 다시 시도하십시오."),
    "E227": ("레시피 조회 시간 초과", "RUNTIME", "RETRY", "WARN",
             "GET_RECIPE 처리 또는 공정 시작(START) 때 레시피 파일 읽기가 제한 시간을 초과했습니다.",
             "폴더 접근 상태와 저장소 응답 상태를 확인한 뒤 다시 시도하십시오."),
    "E412": ("PLC 비트 읽기 실패", "PLC_COMM", "RETRY", "WARN",
             "PLC 비트 읽기 과정에서 예외가 발생했습니다.", "PLC 통신 상태를 확인한 뒤 다시 시도하십시오."),
    "E999": ("정의되지 않은 내부 예외", "INTERNAL", "STOP", "ERROR",
             "명시적으로 분류되지 않은 내부 예외가 발생했습니다.", "원본 예외를 확인하고 적절한 AppError 코드로 정규화하십시오."),
}


def pack_message(command: str, payload: Json, now: Callable[[], float] = time.time) -> bytes:
    """응답 패킷(헤더+바디). payload = {"request_id", "data"} (v1 host/protocol.py:19-39)."""
    body_obj = {
        "request_id": payload.get("request_id", ""),
        "command": command,
        "data": payload.get("data", {}),
    }
    body = json.dumps(body_obj, ensure_ascii=False, default=str).encode("utf-8")
    header = struct.pack(HEADER_FMT, PROTOCOL_VERSION, 0, len(command.encode("utf-8")), len(body), int(now()))
    return header + body


def unpack_header(header: bytes) -> Tuple[int, int, int, int, int]:
    """version, flags, cmd_len, body_len, ts (v1 host/protocol.py:42-46)."""
    if len(header) != HEADER_SIZE:
        raise ValueError(f"Invalid header size: {len(header)}")
    return struct.unpack(HEADER_FMT, header)


def pack_request(command: Any, data: Any = None, request_id: Any = "r", *, version: int = PROTOCOL_VERSION,
                 flags: int = 0, ts: int = 0, body: Optional[bytes] = None) -> bytes:
    """요청 패킷(시험·도구용 — 로봇 쪽 클라이언트 모양). body 를 주면 그대로 쓴다."""
    if body is None:
        obj: Json = {"request_id": request_id, "command": command, "data": {} if data is None else data}
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    cmd_b = str(command).encode("utf-8")
    return struct.pack(HEADER_FMT, version, flags, len(cmd_b) & 0xFFFF, len(body), ts) + body


def error_payload(code: str, *, detail: str = "", meta: Optional[Json] = None) -> Json:
    """표준 실패 data (v1 errors/error_payload.py:27-62 build_error_payload_from_code). 모르는 코드는 E999 정의로."""
    c = str(code or "").strip().upper()
    if c not in ERROR_DEFS:
        c = "E999"
    title, category, action, severity, cause, fix = ERROR_DEFS[c]
    d = "" if detail is None else str(detail).strip()
    payload: Json = {
        "result": "fail", "error_code": c, "client_action": action, "category": category, "severity": severity,
        "message": title, "detail": d or cause, "guide": fix,
    }
    if isinstance(meta, dict) and meta:
        payload["meta"] = dict(meta)
    return payload


def _one_line(text: str, limit: int = 2000) -> str:
    s = " ".join((text or "").replace("\r", "\n").splitlines()).strip()
    return s[:limit] + "…" if len(s) > limit else s


def _to_text(v: Any) -> str:
    try:
        return "" if v is None else str(v)
    except Exception:
        return repr(v)


def report_log_text(payload: Json) -> str:
    """실패 보고 로그 본문 '[코드 / action] title | 상세: detail' (v1 errors/error_reporter.py:27-45)."""
    code = str(payload.get("error_code", "") or "").strip()
    action = str(payload.get("client_action", "") or "").strip()
    message = _one_line(_to_text(payload.get("message", "")))
    detail = _one_line(_to_text(payload.get("detail", "")))
    text = f"{message} | 상세: {detail}" if detail and detail != message else message
    prefix = [x for x in (code, action) if x]
    if prefix:
        text = f"[{' / '.join(prefix)}] {text}"
    return text.strip()

# host/protocol.py
# -*- coding: utf-8 -*-
"""
통신 명세: 고정 16바이트 헤더 + JSON 바디
- Header format: !BBHIQ  (Version, Flags, CommandLen, BodyLen, Timestamp)
- Body: UTF-8 JSON: {"request_id": "...", "command": "CMD", "data": {...}}
본 모듈은 '패킷 직렬화/역직렬화'만 책임진다.
"""
from __future__ import annotations
import struct, json, time
from typing import Tuple, Dict, Any

HEADER_FMT = "!BBHIQ"
HEADER_SIZE = struct.calcsize(HEADER_FMT)
PROTOCOL_VERSION = 1

Json = Dict[str, Any]


def pack_message(command: str, payload: Json) -> bytes:
    """
    응답 패킷(헤더+바디) 생성.
    payload 예: {"request_id": "...", "data": {...}}
    """
    body_obj = {
        "request_id": payload.get("request_id", ""),
        "command": command,
        "data": payload.get("data", {}),
    }
    body = json.dumps(body_obj, ensure_ascii=False).encode("utf-8")
    version = PROTOCOL_VERSION
    flags = 0
    cmd_len = len(command.encode("utf-8"))
    body_len = len(body)
    ts = int(time.time())
    header = struct.pack(HEADER_FMT, version, flags, cmd_len, body_len, ts)
    return header + body


def unpack_header(header: bytes) -> Tuple[int, int, int, int, int]:
    """version, flags, cmd_len, body_len, ts 반환"""
    if len(header) != HEADER_SIZE:
        raise ValueError(f"Invalid header size: {len(header)}")
    return struct.unpack(HEADER_FMT, header)

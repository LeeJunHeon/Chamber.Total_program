"""로봇 서버 조립·실행 — python -m v2.robot_server [--config 파일] [--host H] [--port P] [--api-port N] [--log-root D] [--fake-api].

설정: --config 는 JSON 파일(평평한 {키: 값} 이나 v2 설정 저장소의 config_active.json({"values": {...}}) 둘 다 받는다).
로봇 서버는 자기 키(v2/ch12/params/robot.py PARAMS·SHARED_USES)만 읽는다. 명령줄 값이 파일 값보다 우선.
--fake-api: 127.0.0.1:ROBOT_API_PORT 에 가짜 내부 API(모든 명령 success)를 같이 띄운다(sim·수동 점검용).
종료: Ctrl+C(콘솔 닫기) → 서버 닫기('Host closed') → 로그 flush. 종료 코드 0.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Optional, Sequence

from v2.ch12.params.robot import robot_cfg
from v2.robot_server.cache import ReplyCache
from v2.robot_server.forward import Forwarder
from v2.robot_server.log import CommandCsv, ServerLog
from v2.robot_server.server import RobotServer


def load_cfg(path: Optional[str]) -> dict:
    if not path:
        return {}
    obj = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if isinstance(obj, dict) and isinstance(obj.get("values"), dict):
        obj = obj["values"]
    if not isinstance(obj, dict):
        raise ValueError(f"설정 파일 형식 오류: {path}")
    return obj


def build(cfg: dict, *, log: Optional[ServerLog] = None, now: Any = None) -> tuple:
    """(RobotServer, ServerLog) — 시험도 같은 조립을 쓴다."""
    c = robot_cfg(cfg)
    root = Path(c["LOG_ROOT_DIR"]) if c["LOG_ROOT_DIR"] else None
    fb = Path(c["LOCAL_FALLBACK_SERVER_DIR"]) if c["LOCAL_FALLBACK_SERVER_DIR"] else None
    slog = log or ServerLog(root, fb, echo=bool(c["ROBOT_LOG_ECHO"]))
    cache = ReplyCache(Path(c["ROBOT_CACHE_PATH"]) if c["ROBOT_CACHE_PATH"] else None)
    kw = {} if now is None else {"now": now}
    srv = RobotServer(str(c["HOST_SERVER_HOST"]), int(c["HOST_SERVER_PORT"]), Forwarder(c), slog,
                      csv=CommandCsv(root, fb), cache=cache, **kw)
    if cache.load_error:
        slog.log("NET", f"[WARN] robot reply cache unreadable (ignored): {cache.load_error}")
    return srv, slog


async def _run(cfg: dict, fake_api: bool) -> None:
    srv, slog = build(cfg)
    slog.start()
    fake = None
    if fake_api:
        from v2.robot_server.fake_api import FakeApi
        fake = FakeApi(port=int(robot_cfg(cfg)["ROBOT_API_PORT"])).start()
    try:
        await srv.start()
        await asyncio.Event().wait()
    finally:
        await srv.aclose()
        if fake is not None:
            fake.stop()
        slog.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m v2.robot_server", description="CH1&2 로봇 서버(v2)")
    ap.add_argument("--config")
    ap.add_argument("--host")
    ap.add_argument("--port", type=int)
    ap.add_argument("--api-port", type=int)
    ap.add_argument("--log-root")
    ap.add_argument("--fake-api", action="store_true")
    a = ap.parse_args(argv)
    cfg = load_cfg(a.config)
    for k, v in (("HOST_SERVER_HOST", a.host), ("HOST_SERVER_PORT", a.port), ("ROBOT_API_PORT", a.api_port),
                 ("LOG_ROOT_DIR", a.log_root)):
        if v is not None:
            cfg[k] = v
    try:
        asyncio.run(_run(cfg, a.fake_api))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())

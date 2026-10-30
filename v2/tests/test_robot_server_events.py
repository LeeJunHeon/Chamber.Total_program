"""로봇 서버 쪽 실패 보고 → 제어 프로그램 이벤트(결정 D09) — POST /robot/v1/event.

- E102(버전)·E104(JSON)·E105(command 없음)·E110(fail 인데 코드 없음)·E111(dict 아님)·E107(직렬화 실패)마다 이벤트 1건
  {kind host_error, code, detail = v1 챗·팝업 문구, title = v1 팝업 제목, payload}. 응답 바이트·로그 줄은 그대로(v1 골든 시험은
  test_robot_server 가 같은 서버로 계속 확인).
- 제어 프로그램이 꺼져 있으면 보내지 못하고 로그만(예외·지연 없음, 연결 유지).
- 실제 내부 명령 API(v2/ch12/robot_api.RobotApi) 까지: emit('robot_server', 'host_error', 문구, level='warn', code).
"""
from __future__ import annotations

import asyncio
import json
import socket
import struct

import pytest

from v2.robot_server import protocol
from v2.robot_server.fake_api import FakeApi
from v2.robot_server.forward import EVENT_PATH, Forwarder
from v2.robot_server.log import ServerLog
from v2.robot_server.server import RobotServer

HDR = "!BBHIQ"


def _frames(rx: bytes) -> list:
    out, i = [], 0
    while len(rx) - i >= 16:
        h = struct.unpack(HDR, rx[i:i + 16])
        if len(rx) - i - 16 < h[3]:
            break
        out.append((h, json.loads(rx[i + 16:i + 16 + h[3]].decode("utf-8"))))
        i += 16 + h[3]
    return out


class Rig:
    def __init__(self, tmp, respond=None, api_port=None):
        self.api = FakeApi(respond).start() if api_port is None else None
        port = self.api.port if self.api is not None else api_port
        self.lines = []
        self.log = ServerLog(tmp / "logroot", tmp / "fallback", on_line=lambda t, x, line: self.lines.append([t, x]))
        self.srv = RobotServer("127.0.0.1", 0, Forwarder({"ROBOT_API_PORT": port, "ROBOT_API_CONNECT_TIMEOUT_S": 1.0}),
                               self.log, now=lambda: 1_700_000_000)

    async def __aenter__(self):
        await self.srv.start()
        return self

    async def __aexit__(self, *a):
        await self.srv.aclose()
        self.log.close()
        if self.api is not None:
            self.api.stop()

    async def connect(self):
        r, w = await asyncio.open_connection("127.0.0.1", self.srv.bound_port)
        return r, w, str(w.get_extra_info("sockname")[:2])

    async def ask(self, r, w, packet: bytes, n: int = 1):
        w.write(packet)
        await w.drain()
        buf = b""
        while len(_frames(buf)) < n:
            ch = await asyncio.wait_for(r.read(65536), timeout=10.0)
            assert ch
            buf += ch
        return _frames(buf)


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _check_event(ev: dict, code: str, payload: dict) -> None:
    assert ev["kind"] == "host_error" and ev["code"] == code and ev["level"] == "warn" and ev["src"] == "HOST"
    assert ev["title"] == f"오류(HOST, {code})"                                   # v1 팝업 제목
    assert ev["detail"] == f"[{code}] {payload['message']} | 상세: {payload['detail']}"   # v1 _build_user_text
    assert ev["payload"] == payload


def test_parse_error_and_missing_command_send_events(tmp_path):
    async def run():
        async with Rig(tmp_path) as rig:
            r, w, peer = await rig.connect()
            (_h, m), = await rig.ask(r, w, protocol.pack_request("X", body=b"{not json"))
            assert m["command"] == "PARSE_ERROR_RESULT" and m["data"]["error_code"] == "E104"
            (_h, m2), = await rig.ask(r, w, protocol.pack_request("", {}, "id7"))
            assert m2["command"] == "UNKNOWN_RESULT" and m2["data"]["error_code"] == "E105"
            await rig.srv.drain_events()
            evs = list(rig.api.events)
            w.close()
            return m["data"], m2["data"], evs, rig.srv.events_sent, list(rig.lines), rig.api.requests
    d104, d105, evs, sent, lines, reqs = asyncio.run(run())
    assert [e["code"] for e in evs] == ["E104", "E105"] and sent == 2
    _check_event(evs[0], "E104", d104)
    _check_event(evs[1], "E105", d105)
    assert reqs == []                                                # 명령 전달은 없음(이벤트만)
    assert ["ERROR/HOST", protocol.report_log_text(d104)] in lines   # 로그는 v1 그대로


def test_version_error_event_then_close(tmp_path):
    async def run():
        async with Rig(tmp_path) as rig:
            r, w, _ = await rig.connect()
            w.write(protocol.pack_request("GET_SPUTTER_STATUS", {}, "a", version=2))
            await w.drain()
            buf = b""
            while True:
                ch = await asyncio.wait_for(r.read(65536), timeout=5.0)
                if not ch:
                    break
                buf += ch
            await rig.srv.drain_events()
            return _frames(buf), list(rig.api.events)
    fr, evs = asyncio.run(run())
    assert fr[0][1]["command"] == "VERSION_ERROR_RESULT"
    assert len(evs) == 1 and evs[0]["code"] == "E102"
    assert evs[0]["detail"].startswith("[E102] ") and "Unsupported protocol version: 2" in evs[0]["detail"]


def test_server_defense_e110_e111_send_events(tmp_path):
    def respond(req):
        if req["cmd"] == "GET_LOADING_1_SENSOR":
            return {"reply_cmd": "GET_LOADING_1_SENSOR_RESULT", "body": [1, 2], "log": []}
        return {"reply_cmd": f"{req['cmd']}_RESULT", "body": {"result": "fail", "message": "x"}, "log": []}

    async def run():
        async with Rig(tmp_path, respond) as rig:
            r, w, _ = await rig.connect()
            (_h, a), = await rig.ask(r, w, protocol.pack_request("GET_LOADING_1_SENSOR", {}, "q"))
            (_h, b), = await rig.ask(r, w, protocol.pack_request("VACUUM_ON", {}, "v"))
            await rig.srv.drain_events()
            w.close()
            return a["data"], b["data"], list(rig.api.events)
    a, b, evs = asyncio.run(run())
    assert a["error_code"] == "E111" and b["error_code"] == "E110"
    assert [e["code"] for e in evs] == ["E111", "E110"]
    _check_event(evs[0], "E111", a)
    _check_event(evs[1], "E110", b)
    assert evs[0]["payload"]["meta"]["request_id"] == "q"


def test_serialization_failure_e107_sends_event(tmp_path):
    def respond(req):
        return {"reply_cmd": "GET_LOADING_2_SENSOR_RESULT", "body": {"result": "success", "message": "OK"}, "log": []}

    async def run():
        async with Rig(tmp_path, respond) as rig:
            real = rig.srv._pack
            calls = []

            def pack(cmd, payload):
                calls.append(cmd)
                if len(calls) == 1:
                    raise TypeError("boom")
                return real(cmd, payload)
            rig.srv._pack = pack
            r, w, _ = await rig.connect()
            (_h, m), = await rig.ask(r, w, protocol.pack_request("GET_LOADING_2_SENSOR", {}, "s"))
            await rig.srv.drain_events()
            w.close()
            return m, list(rig.api.events)
    m, evs = asyncio.run(run())
    assert m["data"]["error_code"] == "E107"
    assert [e["code"] for e in evs] == ["E107"] and "Response serialization failed: TypeError: boom" in evs[0]["detail"]


def test_control_program_down_logs_only(tmp_path):
    async def run():
        async with Rig(tmp_path, api_port=_free_port()) as rig:
            r, w, _ = await rig.connect()
            (_h, m), = await rig.ask(r, w, protocol.pack_request("X", body=b"[["))
            (_h, m2), = await rig.ask(r, w, protocol.pack_request("GET_SPUTTER_STATUS", {}, "s"))   # 연결 유지
            await rig.srv.drain_events()
            w.close()
            return m["data"], m2["data"], rig.srv.events_sent, rig.srv.events_failed, list(rig.lines)
    d, d2, sent, failed, lines = asyncio.run(run())
    assert d["error_code"] == "E104" and d2["error_code"] == "E203"
    assert sent == 0 and failed == 1
    assert ["ERROR/HOST", protocol.report_log_text(d)] in lines
    assert not any("event" in x.lower() for _t, x in lines)          # 못 보낸 이벤트로 새 로그 줄을 만들지 않는다


def test_forwarder_send_event_to_real_robot_api():
    """실제 내부 명령 API(제어 프로그램 쪽)까지 — emit 로 넘어간다."""
    from v2.ch12.robot_api import RobotApi

    class Ops:
        on_report = None

    got = []
    with RobotApi(Ops(), port=0, emit=lambda *a, **k: got.append((a, k))) as api:
        fwd = Forwarder({"ROBOT_API_PORT": api.port})
        p = protocol.error_payload("E104", detail="Invalid JSON: x", meta={"peer": "p"})
        ok = asyncio.run(fwd.send_event(protocol.report_event(p)))
        assert ok
        assert api.events[-1]["code"] == "E104"
    assert got == [(("robot_server", "host_error", protocol.user_text(p)), {"level": "warn", "code": "E104"})]


def test_send_event_unreachable_is_false_quickly():
    fwd = Forwarder({"ROBOT_API_PORT": _free_port(), "ROBOT_API_CONNECT_TIMEOUT_S": 0.5})
    assert asyncio.run(fwd.send_event({"kind": "host_error"}, timeout_s=1.0)) is False


@pytest.mark.parametrize("code", ["E102", "E104", "E105", "E107", "E110", "E111"])
def test_report_event_shape_for_d09_codes(code):
    p = protocol.error_payload(code, detail="d", meta={"peer": "x"})
    ev = protocol.report_event(p)
    assert set(ev) == {"kind", "code", "detail", "level", "src", "title", "payload"}
    assert ev["code"] == code and ev["detail"] == protocol.user_text(p) and EVENT_PATH == "/robot/v1/event"

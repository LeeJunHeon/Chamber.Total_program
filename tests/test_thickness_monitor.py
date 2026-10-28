# -*- coding: utf-8 -*-
"""device/thickness_monitor.py — 프레임·파싱 순수 함수 + 가짜 STM-100 TCP 서버(asyncio)로 드라이버 동작.
pytest / 직접 실행 모두 가능. 장비 없이 돈다."""
import asyncio
import contextlib
import os
import sys
from collections import deque

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

from device.thickness_monitor import (                           # noqa: E402
    STX, AsyncThicknessMonitor, ThicknessChecksumError, ThicknessCommandRejected, ThicknessCommError,
    ThicknessProtocolError, ThicknessTimeout, ThicknessValueUnavailable, ThicknessVerifyError,
    build_frame, check_material, checksum, extract_frame, fmt_compact, parse_crystal_ok,
    parse_frequency_hz, parse_life_pct, parse_rate_a_s, parse_thickness_a, strip_echo)


# ═══════════════════════ 순수 함수 ═══════════════════════
def test_build_frame_and_checksum():
    assert build_frame("S") == bytes([STX, 1]) + b"S" + bytes([ord("S")])
    f = build_frame("E=1.23")
    assert f[0] == STX and f[1] == 6 and f[2:8] == b"E=1.23" and f[8] == checksum(b"E=1.23")
    assert checksum(b"\xff\xff") == 0xFE
    with pytest.raises(ValueError):
        build_frame("")
    with pytest.raises(ValueError):
        build_frame("E=123456789")           # 11바이트(최대 10)
    assert len(build_frame("E=12345678")) == 2 + 10 + 1


def test_extract_frame_garbage_partial_and_errors():
    buf = bytearray(b"\x00\xffzz") + bytearray(build_frame("A+0001234"))
    assert extract_frame(buf) == "A+0001234" and buf == bytearray()
    full = build_frame("A12.3")
    buf = bytearray(full[:3])
    assert extract_frame(buf) is None        # 덜 옴
    buf.extend(full[3:])
    assert extract_frame(buf) == "A12.3"
    bad = bytearray(full)
    bad[-1] ^= 0x01
    with pytest.raises(ThicknessChecksumError):
        extract_frame(bad)
    assert bad == bytearray()                # 틀린 프레임은 버린다
    with pytest.raises(ThicknessChecksumError):
        extract_frame(bytearray([STX, 0, 0x41]))   # LEN 0
    assert extract_frame(bytearray(b"no stx")) is None


def test_parsers():
    assert parse_thickness_a("+0001234") == 1234.0
    assert parse_thickness_a("-0001595") == -1595.0
    assert parse_thickness_a("S 0000100") == 100.0              # 명령 글자 echo
    assert parse_rate_a_s("+012.3") == 12.3
    assert parse_rate_a_s("T-000.5") == -0.5
    assert parse_frequency_hz("U 5319234") == 5319234
    assert parse_life_pct("V 012.4") == 12.4
    assert parse_crystal_ok("@") is True
    assert parse_crystal_ok("M !") is False
    assert parse_crystal_ok("M @") is True
    for bad in ("--------", "", "  ", "S --------", "N/A"):
        with pytest.raises(ThicknessValueUnavailable):
            parse_thickness_a(bad)
    with pytest.raises(ThicknessProtocolError):
        parse_thickness_a("12.5")             # 두께는 정수
    with pytest.raises(ThicknessProtocolError):
        parse_frequency_hz("0")
    with pytest.raises(ThicknessProtocolError):
        parse_life_pct("120.0")
    with pytest.raises(ThicknessProtocolError):
        parse_crystal_ok("?")
    assert strip_echo("U 5", "U") == "5" and strip_echo("@", "@") == "@"


def test_fmt_and_material_range():
    assert fmt_compact(1.230, 3) == "1.23" and fmt_compact(0.75, 3) == "0.75" and fmt_compact(100.0, 1) == "100"
    assert check_material(2.7, 1.08, 100) == ("2.7", "1.08", "100")
    for args in ((0.4, 1.0, 100), (100.0, 1.0, 100), (2.7, 0.05, 100), (2.7, 10.0, 100),
                 (2.7, 1.0, 9.9), (2.7, 1.0, 400)):
        with pytest.raises(ValueError):
            check_material(*args)
    for d, z, t in ((99.99, 9.999, 399), (0.5, 0.1, 10)):
        for s in ("E=" + fmt_compact(d, 3), "F=" + fmt_compact(z, 3), "J=" + fmt_compact(t, 1)):
            assert len(s) <= 10


# ═══════════════════════ 가짜 STM-100 서버 ═══════════════════════
class FakeSTM:
    """질의-응답만. script[cmd] 에 동작을 넣으면 그 명령에 한 번씩 꺼내 쓴다:
    ("silent",) 무응답 · ("badchk",) 체크섬 틀린 응답 · ("raw", payload) 그대로 · ("drop",) 연결 끊기."""

    def __init__(self):
        self.thk = 1234
        self.rate = 12.3
        self.freq = 5912345
        self.life = 12.4
        self.xtal = "@"
        self.blank = None                   # 문자열이면 S·T·U·V 에 그 빈칸 본문으로 답한다(XTAL FAIL 흉내)
        self.mute = False                   # True = TCP 는 받지만 어떤 명령에도 답하지 않음(장비 전원 끔·MOXA 켬)
        self.density, self.z, self.tooling = 1.0, 1.0, 100.0
        self.echo = False
        self.reset_flag = False
        self.tooling_offset = 0.0           # J= 를 이만큼 틀리게 저장(되읽기 불일치)
        self.script = {}
        self.rx = []
        self.server = None
        self.port = None
        self.conns = 0
        self._writers = []

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def close(self):
        for w in list(self._writers):
            with contextlib.suppress(Exception):
                w.close()
        self.server.close()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self.server.wait_closed(), 1.0)

    def drop_all(self):
        for w in list(self._writers):
            with contextlib.suppress(Exception):
                w.transport.abort()

    def _answer(self, cmd: str) -> str:
        code = "B" if self.reset_flag else "A"
        e = (lambda t: t + " ") if self.echo else (lambda t: "")
        if self.blank is not None and cmd in ("S", "T", "U", "V"):
            return code + e(cmd) + self.blank
        if cmd == "S":
            return code + e("S") + f"{self.thk:+08d}"
        if cmd == "T":
            return code + e("T") + f"{self.rate:+06.1f}"
        if cmd == "U":
            return code + e("U") + str(self.freq)
        if cmd == "V":
            return code + e("V") + f"{self.life:05.1f}"
        if cmd == "M":
            return code + e("M") + self.xtal
        if cmd == "L":
            self.reset_flag = False
            return "A"
        if cmd in ("B", "C"):
            self.thk = 0
            return code
        if cmd == "@":
            return code + "V1.23"
        if cmd == "E?":
            return code + f"{self.density:.3f}"
        if cmd == "F?":
            return code + f"{self.z:.3f}"
        if cmd == "J?":
            return code + f"{self.tooling:.1f}"
        if cmd.startswith(("E=", "F=", "J=")):
            try:
                v = float(cmd[2:])
            except ValueError:
                return "H"
            if cmd[0] == "E":
                self.density = v
            elif cmd[0] == "F":
                self.z = v
            else:
                self.tooling = v + self.tooling_offset
            return code
        return "F"

    async def _handle(self, reader, writer):
        self.conns += 1
        self._writers.append(writer)
        buf = bytearray()
        try:
            while True:
                chunk = await reader.read(64)
                if not chunk:
                    break
                buf.extend(chunk)
                while True:
                    try:
                        cmd = extract_frame(buf)
                    except ThicknessChecksumError:
                        continue                  # 장비는 틀린 프레임에 응답하지 않는다
                    if cmd is None:
                        break
                    self.rx.append(cmd)
                    if self.mute:
                        continue
                    q = self.script.get(cmd)
                    act = q.popleft() if q else None
                    if act and act[0] == "silent":
                        continue
                    if act and act[0] == "drop":
                        writer.transport.abort()
                        return
                    payload = act[1] if (act and act[0] == "raw") else self._answer(cmd)
                    frame = bytearray(build_frame(payload)) if len(payload) <= 10 else \
                        bytearray([STX, len(payload)]) + payload.encode() + bytes([checksum(payload.encode())])
                    if act and act[0] == "badchk":
                        frame[-1] ^= 0x01
                    writer.write(bytes(frame))
                    await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            with contextlib.suppress(ValueError):
                self._writers.remove(writer)


def _run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=20.0))


async def _setup(**kw):
    srv = FakeSTM()
    await srv.start()
    logs = []
    dev = AsyncThicknessMonitor("127.0.0.1", srv.port, cmd_timeout_s=kw.pop("cmd_timeout_s", 0.2),
                                backoff_start_s=0.05, backoff_max_s=0.2, settle_s=0.0, log=logs.append, **kw)
    await dev.start()
    for _ in range(100):
        if dev.is_connected:
            break
        await asyncio.sleep(0.02)
    assert dev.is_connected
    return srv, dev, logs


async def _teardown(srv, dev):
    await dev.stop()
    await srv.close()


def test_normal_reads_and_echo():
    async def go():
        srv, dev, _ = await _setup()
        try:
            assert await dev.read_thickness_a() == 1234.0
            assert await dev.read_rate_a_s() == 12.3
            assert await dev.read_frequency_hz() == 5912345
            assert await dev.read_crystal_life() == 12.4
            assert await dev.read_crystal_ok() is True
            assert await dev.read_version() == "V1.23"
            srv.echo = True
            srv.xtal = "!"
            assert await dev.read_frequency_hz() == 5912345
            assert await dev.read_crystal_life() == 12.4
            assert await dev.read_crystal_ok() is False
            assert await dev.read_thickness_a() == 1234.0
        finally:
            await _teardown(srv, dev)
    _run(go())


@pytest.mark.parametrize("body", ["    . ", "-------", " ---.-", "", "   ", "T    . ", "T -----"])
def test_blank_reply_is_value_unavailable_for_all_values(body):
    """XTAL FAIL 중 빈칸 응답 — S/T/U/V/E?/F?/J? 공통으로 '값 없음'(형식 오류 아님)."""
    from device.thickness_monitor import (parse_frequency_hz, parse_life_pct, parse_number, parse_rate_a_s,
                                          parse_thickness_a)
    for fn in (parse_thickness_a, parse_rate_a_s, parse_frequency_hz, parse_life_pct,
               lambda b: parse_number(b, "E"), lambda b: parse_number(b, "J")):
        with pytest.raises(ThicknessValueUnavailable):
            fn(body.replace("T", "", 1) if fn is not parse_rate_a_s else body)


def test_partly_blank_number_is_still_format_error():
    from device.thickness_monitor import parse_rate_a_s, parse_thickness_a
    for fn, body in ((parse_rate_a_s, "12 .4"), (parse_thickness_a, "+00 1234")):
        with pytest.raises(ThicknessProtocolError) as ei:
            fn(body)
        assert not isinstance(ei.value, ThicknessValueUnavailable)


def test_dashes_is_value_unavailable_not_comm_error():
    async def go():
        srv, dev, _ = await _setup()
        try:
            srv.script["S"] = deque([("raw", "A--------"), ("raw", "A")])
            with pytest.raises(ThicknessValueUnavailable):
                await dev.read_thickness_a()
            with pytest.raises(ThicknessValueUnavailable):
                await dev.read_thickness_a()
            assert dev.is_connected
            assert await dev.read_thickness_a() == 1234.0
        finally:
            await _teardown(srv, dev)
    _run(go())


def test_checksum_error_retries_then_ok_and_exhausts():
    async def go():
        srv, dev, _ = await _setup()
        try:
            srv.script["S"] = deque([("badchk",)])
            assert await dev.read_thickness_a() == 1234.0
            assert srv.rx.count("S") == 2                 # 1번 틀리고 재송신
            srv.rx.clear()
            srv.script["S"] = deque([("badchk",)] * 3)
            with pytest.raises(ThicknessChecksumError):
                await dev.read_thickness_a()
            assert srv.rx.count("S") == 3                 # 1 + 재시도 2
            assert dev.is_connected
        finally:
            await _teardown(srv, dev)
    _run(go())


def test_no_response_timeout_and_retry():
    async def go():
        srv, dev, _ = await _setup()
        try:
            srv.script["T"] = deque([("silent",)])
            assert await dev.read_rate_a_s() == 12.3       # 무응답 1번 → 재송신으로 성공
            srv.script["T"] = deque([("silent",)] * 3)
            with pytest.raises(ThicknessTimeout):
                await dev.read_rate_a_s()
            assert isinstance(ThicknessTimeout("x"), ThicknessCommError)
            assert dev.is_connected                         # 응답 없음만으로 끊지 않는다(서비스가 횟수로 판단)
            assert await dev.read_rate_a_s() == 12.3
        finally:
            await _teardown(srv, dev)
    _run(go())


def test_late_reply_does_not_leak_into_next_command():
    async def go():
        srv, dev, _ = await _setup(retries=0)
        try:
            # S 의 응답이 늦게 와도(여기서는 무응답 뒤 다른 명령) 다음 명령은 자기 응답을 읽는다
            srv.script["S"] = deque([("silent",)])
            with pytest.raises(ThicknessTimeout):
                await dev.read_thickness_a()
            assert await dev.read_rate_a_s() == 12.3
        finally:
            await _teardown(srv, dev)
    _run(go())


def test_reset_flag_sends_L_and_counts_once():
    async def go():
        srv, dev, logs = await _setup()
        try:
            srv.reset_flag = True
            assert await dev.read_thickness_a() == 1234.0   # B 도 정상
            assert "L" in srv.rx and srv.reset_flag is False
            assert dev.reset_seen == 1
            assert await dev.read_thickness_a() == 1234.0
            assert dev.reset_seen == 1                       # 이미 확인 — 다시 세지 않음
            srv.reset_flag = True                            # 새 리셋
            await dev.read_rate_a_s()
            assert dev.reset_seen == 2
            assert any("리셋" in m for m in logs)
        finally:
            await _teardown(srv, dev)
    _run(go())


def test_rejected_code_raises_command_rejected():
    async def go():
        srv, dev, _ = await _setup()
        try:
            srv.script["S"] = deque([("raw", "F")])
            with pytest.raises(ThicknessCommandRejected) as ei:
                await dev.read_thickness_a()
            assert ei.value.code == "F"
            srv.script["S"] = deque([("raw", "Z123")])       # 모르는 코드 → 재시도 후 프로토콜 오류
            srv.script["S"].extend([("raw", "Z123"), ("raw", "Z123")])
            with pytest.raises(ThicknessProtocolError):
                await dev.read_thickness_a()
        finally:
            await _teardown(srv, dev)
    _run(go())


def test_disconnect_then_reconnect():
    async def go():
        srv, dev, logs = await _setup()
        try:
            assert await dev.read_thickness_a() == 1234.0
            srv.drop_all()
            for _ in range(100):
                if not dev.is_connected:
                    break
                await asyncio.sleep(0.01)
            assert not dev.is_connected
            with pytest.raises(ThicknessCommError):
                await dev.read_thickness_a()
            for _ in range(200):
                if dev.is_connected:
                    break
                await asyncio.sleep(0.02)
            assert dev.is_connected and srv.conns >= 2
            assert await dev.read_thickness_a() == 1234.0
            assert any("끊김" in m for m in logs)
            # 서버가 명령 도중 끊어도 예외로 끝나고 다시 붙는다
            srv.script["T"] = deque([("drop",)])
            with pytest.raises(ThicknessCommError):
                await dev.read_rate_a_s()
            for _ in range(200):
                if dev.is_connected:
                    break
                await asyncio.sleep(0.02)
            assert await dev.read_rate_a_s() == 12.3
        finally:
            await _teardown(srv, dev)
    _run(go())


def test_connect_failure_backoff_logs_once():
    async def go():
        srv = FakeSTM()
        await srv.start()
        port = srv.port
        await srv.close()                                    # 아무도 듣지 않는 포트
        logs = []
        dev = AsyncThicknessMonitor("127.0.0.1", port, connect_timeout_s=0.5, backoff_start_s=0.05,
                                    backoff_max_s=0.1, log=logs.append)
        await dev.start()
        await asyncio.sleep(2.5)                             # Windows 는 거부된 연결도 ~2초 걸려 실패한다
        assert not dev.is_connected
        assert sum("연결 실패" in m for m in logs) == 1     # 계속 실패해도 로그는 한 번
        await dev.stop()
    _run(go())


def test_set_material_ok_mismatch_and_range():
    async def go():
        srv, dev, _ = await _setup()
        try:
            res = await dev.set_material(2.7, 1.08, 120.5)
            assert res == {"density": 2.7, "z_factor": 1.08, "tooling": 120.5}
            assert "E=2.7" in srv.rx and "F=1.08" in srv.rx and "J=120.5" in srv.rx
            srv.rx.clear()
            srv.tooling_offset = 1.0                         # 되읽기가 1% 다르다(허용 0.2)
            with pytest.raises(ThicknessVerifyError):
                await dev.set_material(2.7, 1.08, 100.0)
            assert srv.rx.count("J=100") == 2                 # 1회 재시도
            srv.tooling_offset = 0.1                         # 허용오차 안
            assert (await dev.set_material(2.7, 1.08, 100.0))["tooling"] == pytest.approx(100.1)
            srv.rx.clear()
            with pytest.raises(ValueError):
                await dev.set_material(2.7, 1.08, 500.0)
            with pytest.raises(ValueError):
                await dev.set_material(0.1, 1.08, 100.0)
            assert srv.rx == []                               # 범위 밖은 보내지 않는다
            await dev.zero("C")
            assert srv.rx[-1] == "C" and await dev.read_thickness_a() == 0.0
            with pytest.raises(ValueError):
                await dev.zero("X")
        finally:
            await _teardown(srv, dev)
    _run(go())


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

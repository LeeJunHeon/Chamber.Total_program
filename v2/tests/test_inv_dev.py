"""전수 목록 dev 영역(v2/ch12/spec/inv_dev.py) 중 다른 시험이 직접 확인하지 않던 동작 — 실제 실행 시험(가상 시계·sim).

항목 id 는 각 시험 독스트링에 적었다. v1 근거는 inv_dev.py 의 v1 칸.
"""
import json
import socket
from pathlib import Path

import pytest

from v2.common.worker import Status
from v2.ch12.drivers import plc_map as PM
from v2.ch12.drivers.plc_conv import cmd_grace_s
from v2.ch12.drivers.plc_io import (LinkState, PlcIo, PlcLinkError, PlcModbusError, PlcTimeout, code_of,
                                    format_suppressed, probe_connect)
from v2.ch12.sim.plc_rig import PlcRig
from v2.ch12.spec.devices_v1facts import FACTS

GOLD = Path(__file__).parent / "golden"


# ───────────────────────── PLC ─────────────────────────
class _Clock:
    def __init__(self) -> None:
        import threading
        self.cond = threading.Condition()
        self.t = 100.0

    def monotonic(self) -> float:
        return self.t

    def wall(self) -> float:
        return 1_760_000_000.0 + self.t


def test_plc_link_state_down_up_suppressed_and_cards():
    """DEV-001·002·003·004·005 — down/up 전이는 1회씩, 억제 건수(코드 대문자·빈값 '?')는 재연결 카드에, 조회·직접 설정 API."""
    c = _Clock()
    ev = []
    ls = LinkState(c, lambda k, d, data: ev.append((k, d, data)), "192.168.1.2:502")
    assert ls.note_up("x") is False and ev == []                       # down 이 아니면 up 없음
    assert ls.note_down("하트비트 실패", "heartbeat") is True
    assert ls.note_down("다른 사유", "x") is False and len(ev) == 1   # 이미 down — 카드 없음
    k, sub, data = ev[0]
    assert k == "link_down" and data["reason"] == "하트비트 실패" and data["op"] == "heartbeat"
    assert data["at"] == pytest.approx(c.wall())
    assert sub.startswith("[CH1&2] PLC 연결 끊김 (192.168.1.2:502) | 감지 ") and sub.endswith("| 사유=하트비트 실패 | op=heartbeat")
    assert data["card"] == dict(title="PLC 연결 끊김", subtitle=sub, status="FAIL", urgent=True)
    assert ls.down_info() == (True, 100.0, "하트비트 실패", "heartbeat")
    for code in ("e401", "E401", "E402", ""):
        ls.note_suppressed(code)
    assert ls.suppressed_snapshot() == (4, {"E401": 2, "E402": 1, "?": 1})
    assert ls.suppressed_snapshot()[0] == 4                              # 스냅샷은 리셋 없음
    assert format_suppressed(4, {"E402": 1, "E401": 2, "?": 1}) == "? 1건, E401 2건, E402 1건"
    assert format_suppressed(0, {}) == ""
    c.t = 112.5
    assert ls.note_up("[CH1&2] PLC 재연결 성공 (192.168.1.2:502)", "미확인") is True
    k, sub, data = ev[1]
    assert k == "link_up" and data["elapsed_s"] == pytest.approx(12.5) and data["suppressed_total"] == 4
    assert data["suppressed_by_code"] == {"E401": 2, "E402": 1, "?": 1} and data["reason"] == "하트비트 실패"
    assert sub == ("[CH1&2] PLC 재연결 성공 (192.168.1.2:502) | 끊김 12.5초 | 끊긴 동안 억제된 연결 알림 4건 "
                   "(? 1건, E401 2건, E402 1건)")                         # 진단 '미확인' 은 생략
    assert data["card"]["title"] == "PLC 재연결" and data["card"]["status"] == "SUCCESS"
    assert ls.suppressed_snapshot() == (0, {}) and ls.down_info() == (False, 0.0, "", "")
    ls.note_down("a", "")
    ls.note_up("", "접속 거부(RST, errno=111)")
    assert ev[-1][1].endswith("| 진단=접속 거부(RST, errno=111)") and "op=" not in ev[-2][1]
    n = len(ev)
    ls.set_down(True)                                                     # 카드 없이 상태만
    assert ls.down and len(ev) == n
    ls.note_suppressed("E402")
    assert ls.consume_suppressed() == (1, {"E402": 1}) and ls.suppressed_snapshot() == (0, {})
    ls.set_down(False)
    assert not ls.down and len(ev) == n


def test_plc_probe_connect_refused_on_closed_local_port():
    """DEV-014 — raw socket connect_ex errno 분류(리눅스 111 = 접속 거부). ping 은 Windows 만이라 여기서는 안 함."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    assert probe_connect("127.0.0.1", port, 0.2) == "접속 거부(RST, errno=111)"
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        assert probe_connect("127.0.0.1", s.getsockname()[1], 0.2) == "접속됨"


def test_plc_error_classification_codes():
    """DEV-015 — 예외 → E401(연결)·E402(무응답·프레임)·E403(Modbus 예외·그 밖)."""
    from v2.common.modbus_rtu import FrameError, ModbusException
    from v2.common.transport import ReadTimeout, TransportError
    conv = PlcIo._convert
    e = conv(TransportError("reset by peer"), "read_reg", 8)
    assert isinstance(e, PlcLinkError) and code_of(e) == "E401" and str(e).startswith("PLC 연결 오류")
    e = conv(ReadTimeout(b""), "read_reg", 8)
    assert isinstance(e, PlcTimeout) and code_of(e) == "E402" and str(e).startswith("PLC 응답 없음/프레임 오류")
    e = conv(FrameError("tid"), "read_reg", 8)
    assert code_of(e) == "E402"
    e = conv(ModbusException(3, 2), "read_reg", 8)
    assert isinstance(e, PlcModbusError) and code_of(e) == "E403" and "ExceptionResponse" in str(e)
    e = conv(ValueError("x"), "read_reg", 8)
    assert code_of(e) == "E403" and str(e).startswith("PLC 통신 오류")
    assert code_of(TransportError("x")) == "E401" and code_of(ReadTimeout(b"")) == "E402" and code_of(KeyError()) == "E403"


def test_plc_reconnect_logs_connect_success_and_fail_streak():
    """DEV-012·013·029 — 접속 성공 로그 'TCP 연결 성공: ip:port (unit=1)', 실패 이력 뒤 'WARN PLC 재접속 성공 (실패 n회, 끊긴 뒤 s초)',
    최종 실패는 E401 'Modbus TCP 연결 실패 (ip:port) - … (시도 3회, 분류=미확인)'."""
    r = PlcRig()
    logs = lambda: [e.detail for e in r.events if e.kind == "log"]      # noqa: E731
    assert "TCP 연결 성공: 192.168.1.2:502 (unit=1)" in logs()
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t + 0.1, t + 14.0)
    r.after(0.2)
    r.call(r.d.write_coil, 234, True)
    r.after(25.0)
    fails = [e for e in r.events if e.kind == "connect_failed"]
    assert fails and all(e.code == "E401" for e in fails)
    assert fails[0].detail.startswith("Modbus TCP 연결 실패 (192.168.1.2:502) - ") and "(시도 3회, 분류=미확인)" in fails[0].detail
    again = [m for m in logs() if m.startswith("WARN PLC 재접속 성공 (실패 ")]
    assert len(again) == 1 and again[0].endswith("초)")
    assert logs().count("TCP 연결 성공: 192.168.1.2:502 (unit=1)") == 2
    r.done()


def test_plc_cmd_grace_and_lock_wait_io_warn_logs():
    """DEV-019·020 — 명령 실행 상한 = 2.0×4+0.5×3+1 = 10.5 s, 큐 대기·실행 ≥ 임계면 'WARN lock-wait'·'WARN in-lock IO'."""
    r = PlcRig(cfg=dict(PLC_LOCK_WARN_MS=0.0, PLC_IO_WARN_MS=0.0))
    assert cmd_grace_s(r.cfg) == pytest.approx(10.5)
    assert r.call(r.d.write_coil, 234, True).status == Status.OK
    logs = [e.detail for e in r.events if e.kind == "log"]
    assert any(m.startswith("WARN lock-wait ") and m.endswith("ms (op=plc:-:234=1)") for m in logs)
    assert any(m.startswith("WARN in-lock IO ") for m in logs)
    r.done()
    r = PlcRig()                                    # 기본 2500 ms — 짧은 요청은 로그 없음
    r.call(r.d.write_coil, 234, True)
    assert not any(e.detail.startswith("WARN lock-wait") for e in r.events if e.kind == "log")
    r.done()


def test_plc_settings_read_live_each_op():
    """DEV-028 — 간격·펄스 폭 같은 설정은 연산 때마다 cfg 에서 다시 읽는다(v1 은 connect 직전 config 재적용)."""
    r = PlcRig()
    r.cfg["PLC_CMD_GAP_MS"] = 400
    t = r.c.monotonic()
    for i in range(3):
        r.d.write_coil(234, i % 2 == 0)
    r.after(3.0)
    ts = [tt for tt, _ in r.frames(t)]
    assert len(ts) == 3 and all(b - a >= 0.4 - 1e-6 for a, b in zip(ts, ts[1:]))
    r.done()


def test_plc_address_tables_equal_v1_fact_tables():
    """DEV-035~046 — 코일 124개(정의 순서 그대로 = 코일 CSV 열 순서)·P 미러·레지스터·타이머 표가 v1 표와 같다."""
    f = FACTS["plc"]
    assert [(k, v) for k, v in PM.COIL_MAP.items()] == [(k, v[0]) for k, v in f["coil_map"].items()]
    assert {k: PM.COIL_M_ADDR[k] for k in PM.COIL_MAP} == {k: v[1] for k, v in f["coil_map"].items()}
    assert PM.P_MIRROR_MAP == {k: v[0] for k, v in f["p_mirror_map"].items()}
    assert PM.REG_MAP == {k: v[0] for k, v in f["reg_map"].items()}
    assert PM.TIMER_MAP == {k: v[0] for k, v in f["timer_map"].items()}
    for name, addr in PM.COIL_MAP.items():
        m = PM.COIL_M_ADDR[name]
        assert int(m[1:5]) * 16 + int(m[5], 16) == addr               # 코일 번호 = M 워드×16 + 비트(16진)
    assert PM.p_mirror_coil("P00003") == 803 and PM.p_mirror_coil("P0004F") == 879
    with pytest.raises(KeyError):
        PM.resolve("NO_SUCH")


def test_plc_high_level_logs_same_as_v1_golden():
    """DEV-023·025·026·057·058·061 — v1 AsyncPLC 를 실제로 돌린 로그(골든)가 v2 로그에 같은 순서로 있다
    ('Gas AR@Ch2 ON', 'MainShutter1 OPEN(ON)', 'GV2 CLOSE', 'pulse … (addr=, 180ms)', 'POWER WRITE (DCV) … (DAC=)' …).
    v2 는 저수준 함수도 한 줄씩 더 남긴다(읽은 값은 sim 메모리라 비교에서 뺀다)."""
    from v2.tests.test_plc import FORCED, SIMPLE, _call
    g = json.loads((GOLD / "dev_plc.json").read_text(encoding="utf-8"))["plc"]["logs"]
    want = [m for m in g if not m.startswith(("TCP 연결", "WARN", "read "))]
    r = PlcRig()
    r.sim.forced_regs.update(FORCED)
    for c in SIMPLE:
        try:
            _call(r, c["name"], c["args"])()
        except ValueError:
            pass
    got = [e.detail for e in r.events if e.kind == "log"]
    it = iter(got)
    missing = [m for m in want if not any(m == x for x in it)]
    assert missing == []
    assert "Gas AR@Ch2 ON" in got and "LiftPin UP" in got and "POWER_SELECT(SW) <- False" in got
    r.done()


def test_plc_high_level_log_only_after_success():
    """DEV-025·026 — v1 은 write_switch 가 실패하면 예외라 'GV…/Gas…' 줄이 없다."""
    r = PlcRig(writes=False)
    r.call(r.d.gate_valve, 1, open=True)
    r.call(r.d.gas, 1, "AR", on=True)
    logs = [e.detail for e in r.events if e.kind == "log"]
    assert not any(m.startswith(("GV1", "Gas AR")) for m in logs)
    with pytest.raises(ValueError, match="가스 'N2'는 CH1에 존재하지 않습니다 \\(키: N2_1_GAS_SW\\)"):
        r.d.gas(1, "N2", on=True)
    with pytest.raises(ValueError):
        r.d.gas(1, "HE", on=True)
    r.done()


def test_plc_close_log_on_system_stop(tmp_path):
    """DEV-029 — 프로그램 종료(worker 정지) 뒤 v1 close() 처럼 하트비트를 멈추고 'TCP 연결 종료' 한 줄."""
    from v2.common.clock import VirtualClock
    from v2.ch12.sim.rig import Ch12Rig
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    r = Ch12Rig(c, tmp_path)
    got = []
    r.plc.on_event, prev = (lambda e: (got.append(e), prev and prev(e))), r.plc.on_event
    assert r.start()
    r.loop.run(2)
    r.stop()
    assert [e.detail for e in got if e.kind == "log"][-1] == "TCP 연결 종료" and r.plc.hb_paused

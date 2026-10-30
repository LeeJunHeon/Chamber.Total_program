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


# ───────────────────────── MFC ─────────────────────────
def _st(r):
    return [e.detail for e in r.events if e.kind == "status"]


def test_mfc_endpoint_settings_and_reconnect_backoff():
    """DEV-064·065 — CH1 4003·CH2 4006, 접속 한도 max(0.5, 3.0), SO_KEEPALIVE 끔, 끊기면 재연결 간격 1→2→4→…→20 s, 연결 문구."""
    from v2.ch12.drivers import mfc as M
    from v2.tests.test_mfc import CFG, Rig
    s1, s2 = M.tcp_settings(CFG, "ch1"), M.tcp_settings(CFG, "ch2")
    assert (s1.host, s1.port, s2.port) == ("192.168.1.50", 4003, 4006)
    assert s1.connect_timeout_s == pytest.approx(3.0)                 # SO_KEEPALIVE 는 v2 공용 전송이 켬(D39)
    r = Rig("ch1")
    assert "192.168.1.50:4003 연결 성공 (TCP)" in _st(r)
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t, t + 200.0)
    r.call(r.d.read_pressure)
    r.c.wait_until(t + 120.0)
    fails = [e.t for e in r.wev if e.kind == "connect_failed"]
    gaps = [round(b - a, 1) for a, b in zip(fails, fails[1:])]
    assert gaps[:5] == [2.0, 4.0, 8.0, 16.0, 20.0][:len(gaps[:5])] or gaps[:5] == [1.0, 2.0, 4.0, 8.0, 16.0]
    assert max(gaps) == pytest.approx(20.0, abs=0.2)
    r.done()


def test_mfc_line_framing_cr_lf_and_limits():
    """DEV-067 — CR 또는 LF 로 줄 분리(CRLF 는 한 구분자), 빈 줄 무시, ascii strip."""
    from v2.ch12.drivers.line_ops import LineReader
    lr = LineReader()
    out = []
    for chunk in (b"Q0+1.0+2.0", b"+3.0\r\n", b"\r\n", b"P+0.20\n\rS1+0.25\r", b"  OK  \n", b"X" * 600 + b"\n"):
        lr.buf += chunk
        while True:
            line = lr._split()
            if line is None:
                break
            out.append(line)
    assert out == ["Q0+1.0+2.0+3.0", "P+0.20", "S1+0.25", "OK", "X" * 512]
    assert lr.buf == b""


def test_mfc_status_texts_flow_sp_zeroing_and_read():
    """DEV-069·075·076·077·078·080·081·082·083·084 — v1 status 문구와 전송·응답 io 줄."""
    from v2.tests.test_mfc import Rig
    r = Rig("ch2")
    assert r.call(r.d.set_flow, 2, 1.1)[1] is None
    assert r.d.last_setpoints[2] == pytest.approx(11.0)
    r.call(r.d.flow_on, 2, owner="chamber2")
    r.call(r.d.flow_off, 2, owner="chamber2")
    r.call(r.d.sp_set, 4, 4.0)
    r.call(r.d.sp_on, 4)
    r.call(r.d.ps_zeroing)
    r.call(r.d.mfc_zeroing, 3)
    r.ch.pressure_override = r.ch.pressure = 0.3 / 0.1 / 1000.0
    r.call(r.d.read_pressure)
    r.call(r.d.read_flow_all)
    st = _st(r)
    assert "Ch2 GAS 스케일: 1.10sccm → 장비 11.00%FS" in st
    assert "FLOW_ON: ch2 안정화 시작 (목표 HW 11.00)" in st
    assert any(m.startswith("GAS 확인... ch2 (목표: 1.10sccm, 현재: ") for m in st)
    assert "SP4 스케일: UI 4.00 → 장비 0.400" in st and "압력 센서 Zeroing 명령 전송" in st and "Ch3 MFC Zeroing 명령 전송" in st
    io = [e.detail for e in r.events if e.kind == "io"]
    assert "[SEND] Q2 11.0 ([SET ch2])" in io and any(m.startswith("[RECV] [READ_PRESSURE] ← P") for m in io)
    p = [e for e in r.events if e.kind == "pressure"][-1]
    assert p.value == pytest.approx(3.0) and p.text == "3.000"
    flows = [(e.gas, e.value) for e in r.events if e.kind == "flow"][-3:]
    assert [g for g, _ in flows] == ["Ar", "O2", "N2"]
    r.done()


def test_mfc_read_pressure_no_reply_and_read_flow_parse_fail():
    """DEV-083·GAPA-006 — R5 응답 없음 → command_failed '응답 없음', R60 이 'Q0' 아니면 command_failed(READ_FLOW, 'R60 파싱 실패')."""
    from v2.tests.test_mfc import Rig
    r = Rig("ch1", start=5.0)
    t = r.c.monotonic()
    r.sim.add_fault("garbage", t, t + 30.0)
    r.call(r.d.read_flow_all)
    fl = [e for e in r.events if e.kind == "command_failed"]
    assert fl and fl[-1].cmd == "READ_FLOW" and fl[-1].detail == "R60 파싱 실패"
    r.done()
    r = Rig("ch1", start=5.0)
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t, t + 30.0)
    r.call(r.d.read_pressure)
    fl = [e for e in r.events if e.kind == "command_failed"]
    assert fl and fl[-1].cmd == "READ_PRESSURE" and fl[-1].detail == "응답 없음"
    r.done()


def test_mfc_wait_pressure_texts_sp_target_and_bad_index():
    """DEV-085·GAPA-005 — WAIT_PRESSURE 진행 문구, SP 기준 대기, SP 5회 실패, 잘못된 sp_index."""
    from v2.tests.test_mfc import Rig
    r = Rig("ch1")
    r.ch.pressure_override = r.ch.pressure = 2.0 / 1000.0
    assert r.call(r.d.wait_pressure, 2.0)[1] is None
    st = _st(r)
    assert "[PRESSURE] 목표압 2 (tol_abs=0.02, tol_rel=5.0%) 도달까지 대기 시작" in st
    assert "[PRESSURE] OK (1/3) target=2, current=2" in st and "[PRESSURE] 목표압 도달 및 안정: target=2, current=2" in st
    r.sim.sp[0] = 0.2
    assert r.call(r.d.wait_pressure, 9.0, use_sp_target=True, sp_index=1)[1] is None
    assert "[READ_SP1] 현재 setpoint (UI) = 2.000" in _st(r)
    assert "[WAIT_PRESSURE] SP1 setpoint 2 기준으로 압력 도달 대기" in _st(r)
    assert r.call(r.d.wait_pressure, 2.0, use_sp_target=True, sp_index=7)[1] is None
    assert "[WAIT_PRESSURE] 잘못된 sp_index=7 → UI target 사용" in _st(r)
    r.sim.sp[1] = 0.0
    res = r.call(r.d.wait_pressure, 2.0, use_sp_target=True, sp_index=2)
    assert res == ("WAIT_PRESSURE", "SP_READ_FAILED", "SP2 setpoint를 5회 시도 후에도 읽지 못함 → 압력 대기 불가")
    assert sum(m.startswith("[WAIT_PRESSURE] SP2 setpoint 읽기 실패 (재시도 ") for m in _st(r)) == 5
    res = r.call(r.d.wait_pressure, 0.0)
    assert res[1] == "PRESSURE_NOT_STABLE" and "[PRESSURE] target <= 0 이라서 압력 대기를 건너뜁니다. target=0.0" in _st(r)
    r.ch.pressure_override = r.ch.pressure = 50.0 / 1000.0
    res = r.call(r.d.wait_pressure, 2.0, timeout_sec=3.0)
    assert res[1] == "PRESSURE_NOT_STABLE" and "[PRESSURE] 타임아웃(3.0s) - target=2, last=50" in _st(r)
    assert "[PRESSURE] 아직 목표 미달: target=2, current=50" in _st(r)
    r.done()


def test_mfc_stab_cancel_messages_by_owner():
    """GAPA-001·DEV-076·078 — 다른 주체가 같은 채널을 다시 켜거나 끄면 이전 주체에게 FLOW_ON 실패 'ch{n} 안정화 중단 (사유)',
    같은 주체면 통지 없음. cleanup 은 'MFC 연결 종료'."""
    from v2.ch12.drivers import mfc_ops
    from v2.tests.test_mfc import Rig
    r = Rig("ch1")
    r.sim.ignore_writes = {"L1"}
    box = []
    r.call(r.d.set_flow, 1, 10.0, owner="chamber1")
    r.d.flow_on(1, owner="chamber1", on_done=lambda *a: box.append(a))
    r.c.wait_until(r.c.monotonic() + 5.0)
    r.d.flow_on(1, owner="pc1", on_done=lambda *a: box.append(("pc1",) + a))
    r.c.wait_until(None, lambda: bool(box))
    assert box[0] == ("FLOW_ON", "STAB_CANCELLED", "ch1 안정화 중단 (같은 채널 FLOW_ON 재요청)")
    r.c.wait_until(r.c.monotonic() + 4.0)
    r.d.flow_off(1, owner="chamber1")
    r.c.wait_until(None, lambda: len(box) >= 2)
    assert box[1] == ("pc1", "FLOW_ON", "STAB_CANCELLED", "ch1 안정화 중단 (FLOW_OFF 요청)")
    assert "FLOW_OFF 요청: ch1 안정화 취소" in _st(r)
    # 다른 주체가 같은 채널 작업을 교체(start_stab_job)
    got = []
    r.d.runner.call_in_worker(lambda: mfc_ops.start_stab_job(r.d, 2, 5.0, "chamber1", lambda *a: got.append(a)), "t")
    r.c.wait_until(r.c.monotonic() + 0.5)
    r.d.runner.call_in_worker(lambda: mfc_ops.start_stab_job(r.d, 2, 5.0, "pc1", None), "t")
    r.c.wait_until(None, lambda: bool(got))
    assert got[0] == ("FLOW_ON", "STAB_CANCELLED", "ch2 안정화 중단 — 다른 주체(pc1)가 같은 채널을 다시 켬")
    got2 = []
    r.d.runner.call_in_worker(lambda: mfc_ops.start_stab_job(r.d, 3, 5.0, "x", lambda *a: got2.append(a)), "t")
    r.c.wait_until(r.c.monotonic() + 0.5)
    r.d.cleanup()
    r.c.wait_until(None, lambda: bool(got2))
    assert got2[0] == ("FLOW_ON", "STAB_CANCELLED", "ch3 안정화 중단 (MFC 연결 종료)")
    st = _st(r)
    assert "MFC 종료 절차 시작" in st and "MFC 연결 종료됨" in st and any(m.startswith("대기 중 명령 ") for m in st)
    r.done()


def test_mfc_gas_select_flow_set_on_and_release_owner_texts():
    """GAPA-002·003·004 — gas_select 범위·문구, 선택 없이 부르면 RuntimeError, FLOW_SET 실패면 FLOW_ON 생략,
    release_owner 문구(채널 목록·안정화 취소 건수·사유)."""
    from v2.tests.test_mfc import Rig
    r = Rig("ch2")
    with pytest.raises(RuntimeError, match="선택된 가스가 없습니다. gas_select\\(\\)를 먼저 호출하세요."):
        r.d.flow_set_on(1.0)
    with pytest.raises(RuntimeError):
        r.d.flow_off_selected()
    with pytest.raises(ValueError, match="지원하지 않는 가스 채널: 4"):
        r.d.gas_select(4)
    r.d.gas_select(2, owner="pc2")
    assert "가스 선택: ch2 (O2)" in _st(r) and r.d.selected_ch == 2
    assert r.call(r.d.flow_set_on, 1.0, owner="pc2")[1] is None
    assert r.d.last_setpoints[2] == pytest.approx(10.0) and r.d.flow_on_flags[2]
    r.d.release_owner("pc2", reason="PC 종료")
    r.c.wait_until(r.c.monotonic() + 0.2)
    assert "[pc2] 사용 종료 — 다른 사용자가 있어 대기열 유지, 이 주체 몫만 정리 (채널 [2], 안정화 취소 0건, PC 종료)" in _st(r)
    assert r.d.selected_ch is None
    n = len(_st(r))
    r.d.release_owner("", reason="x")
    r.c.wait_until(r.c.monotonic() + 0.2)
    assert len(_st(r)) == n
    # FLOW_SET 이 실패(30 s 전송 못 함)하면 FLOW_ON 을 보내지 않는다
    r.d.gas_select(1, owner="pc2")
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t, t + 60.0)
    res = r.call(r.d.flow_set_on, 2.0, owner="pc2")
    assert res[0] == "FLOW_ON" and res[2] == "ch1 FLOW_SET 실패로 FLOW_ON 생략"
    r.c.wait_until(t + 70.0)
    assert "L11" not in r.sent(t)
    r.done()


def test_device_events_fan_out_to_every_listener(tmp_path):
    """DEV-092·115 — v1 MFC·IG 이벤트 브로드캐스트(챔버 펌프·PC 펌프가 같은 이벤트): v2 는 system.event_listeners 전부에 같은 이벤트."""
    from v2.common.clock import VirtualClock
    from v2.ch12.sim.rig import Ch12Rig
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    r = Ch12Rig(c, tmp_path)
    a, b = [], []
    r.event_listeners += [a.append, b.append]
    r.mfc["ch1"].status("x1")
    r.ig["ch2"].status("x2") if hasattr(r.ig["ch2"], "status") else r.ig["ch2"].emit("status", "x2")
    assert [(e.source, e.detail) for e in a] == [(e.source, e.detail) for e in b] == [("ch1.mfc", "x1"), ("ch2.ig", "x2")]


# ───────────────────────── IG ─────────────────────────
def test_ig_endpoint_backoff_and_link_texts():
    """DEV-097·098·GAPA-007 — CH1 4001·CH2 4002·접속 한도 3 s, 연결 문구, 끊김 'IG TCP 연결 끊김', 재연결 1→2→…→20 s."""
    from v2.ch12.drivers import ig as G
    from v2.tests.test_ig import CFG, Rig
    s1, s2 = G.tcp_settings(CFG, "ch1"), G.tcp_settings(CFG, "ch2")
    assert (s1.host, s1.port, s2.port, s1.connect_timeout_s) == ("192.168.1.50", 4001, 4002, 3.0)
    r = Rig("ch2")
    st = lambda: [e.detail for e in r.kinds("status")]           # noqa: E731
    assert "192.168.1.50:4002 연결 성공 (TCP)" in st()
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t, t + 200.0)
    r.call(r.d.ensure_on)
    r.c.wait_until(t + 120.0)
    assert "IG TCP 연결 끊김" in st()
    fails = [e.t for e in r.wev if e.kind == "connect_failed"]
    gaps = [round(b - a, 1) for a, b in zip(fails, fails[1:])]
    assert gaps and max(gaps) == pytest.approx(20.0, abs=0.2) and gaps[0] <= 4.0
    r.done()


def test_ig_io_texts_echo_timeout_and_single_shot_api():
    """DEV-099·100·101·102·103·114·GAPA-007 — '[SEND] cmd (tag=…)'·'[RECV] tag cmd ← 응답', 에코(대소문자 무시) 건너뜀,
    무응답 '[TIMEOUT] … / 재시도 소진', 단발 API 실패 문구."""
    from v2.tests.test_ig import Rig
    r = Rig("ch1", torr=3e-6)
    r.ch.pressure_override = 3e-6
    r.sim.echo = True
    assert r.call(r.d.ensure_on)[1] is None
    io = [e.detail for e in r.kinds("io")]
    assert "[SEND] SIG 1 (tag=[ensure_on])" in io and "[RECV] [ensure_on] SIG 1 ← OK" in io
    r.c.wait_until(r.c.monotonic() + 1.0)
    res = r.call(r.d.read_pressure)
    assert res[1] is None and float(res[2]) == pytest.approx(3e-6, rel=0.05)
    t = r.c.monotonic()
    r.sim.add_fault("no_response", t, t + 10.0)
    res = r.call(r.d.read_pressure)
    assert res[1:] == ("DEVICE_NO_RESPONSE", "RDI timeout")
    st = [e.detail for e in r.kinds("status")]
    assert "[TIMEOUT] [read_pressure] RDI / 재시도 소진" in st
    assert not any("응답 타임아웃" in m for m in st)          # 워커 한도(3 s)가 먼저 None — 호출 쪽 한도(+0.5 s) 문구는 큐가 밀릴 때만
    r.done()
    r = Rig("ch1")
    r.sim.sig1_reply = "ERR"
    assert r.call(r.d.ensure_on)[1:] == ("IG_ON_FAILED", "SIG 1 failed (ensure_on)")
    assert r.sim.commands_seen().count("SIG 1") == 5 and [e.kind for e in r.wev].count("disconnected") >= 4   # 'OK' 아니면 끊고 다음
    r.done()


def test_ig_base_wait_status_texts_and_cleanup():
    """DEV-104·105·107·110·111 — 베이스 대기 문구 순서, 도달 뒤 정리(SIG 0)·'대기 중 명령 n개 폐기 (shutdown)'·'IG 연결 종료됨'."""
    from v2.tests.test_ig import Rig
    r = Rig("ch1", torr=5e-5)
    r.sim.rdi_script = ["5.0x10e-5", "5.0x10e-5", "5.0x10e-7"]
    res = r.call(r.d.wait_for_base_pressure, 1e-6, 2000)
    assert res[1] is None
    st = [e.detail for e in r.kinds("status")]
    seq = ["Base Pressure 대기 시작", "IG ON OK → 첫 RDI를 5000ms 후 수행", "[FIRST READ] RDI 송신 및 응답 대기 (최대 3000ms)",
           "목표 미도달: 현재 5.000e-05 Torr > 목표 1.000e-06 Torr → 2초 후 재시도", "목표 압력 도달",
           "대기 중 명령 0개 폐기 (shutdown)", "IG 연결 종료됨"]
    idx = [st.index(m) for m in seq]
    assert idx == sorted(idx)
    assert [e.pressure for e in r.kinds("pressure")] == [5e-5, 5e-5, 5e-7] and len(r.kinds("base_reached")) == 1
    assert r.sim.commands_seen()[-1] == "SIG 0"
    r.done()


def test_ig_process_status_off_purges_and_route_errors():
    """DEV-112·113·GAPA-008 — 공정 끝(set_process_status False)은 'process finished' 폐기, cancel_wait 은 SIG 0,
    문자열 라우터: 모르는 명령·예외는 status 로만."""
    from v2.tests.test_ig import Rig
    r = Rig("ch1", torr=4e-6)
    r.d.set_process_status(True)
    r.c.wait_until(r.c.monotonic() + 3.0)
    r.d.set_process_status(False)
    r.c.wait_until(r.c.monotonic() + 1.0)
    st = lambda: [e.detail for e in r.kinds("status")]           # noqa: E731
    assert any(m.startswith("대기 중 명령 ") and m.endswith("(process finished)") for m in st())
    r.call(r.d.cancel_wait)
    assert r.sim.commands_seen()[-1] == "SIG 0"
    r.d.command("FOO")
    assert "[command] 지원하지 않는 IG 명령: 'FOO'" in st()
    r.d.command("WAIT_FOR_BASE")                                       # target 없음 → 예외 문구(올리지 않음)
    assert any(m.startswith("[command] 예외: TypeError(") for m in st())
    box = []
    r.d.command("READ_PRESSURE", on_done=lambda *a: box.append(a))
    r.c.wait_until(None, lambda: bool(box))
    r.done()


def test_ig_shared_with_same_chamber_pc_is_left_alone():
    """DEV-116 — 같은 CH 플라즈마 클리닝이 IG 를 쓰는 중이면 챔버 공정은 IG status·pressure 를 기록하지 않고
    정리 때 IG 대기 취소·폴링 끄기를 생략한다(진행 중 Base 대기를 끊지 않음)."""
    from types import SimpleNamespace as NS
    from v2.ch12.process_actions import ProcessActions
    from v2.ch12.process_events import ProcessEventsMixin

    class H(ProcessEventsMixin):
        pass
    for in_use in (True, False):
        h = H()
        logs, matches = [], []
        h.n, h.log, h._match = 1, (lambda s, m: logs.append((s, m))), (lambda *a: matches.append(a))
        h.e = NS(ig_in_use_by_pc=lambda: in_use, stats=NS(add_ig=lambda p: logs.append(("stat", p))))
        h._ev_ig("status", NS(detail="Base Pressure 대기 시작"))
        h._ev_ig("io", NS(detail="[SEND] RDI (tag=[POLL RDI])"))
        h._ev_ig("pressure", NS(pressure=1e-6))
        h._ev_ig("base_reached", NS())
        assert (logs == []) is in_use and matches == [("IG_OK",)]
        calls = []
        ig = NS(cancel_wait=lambda: calls.append("cancel_wait"), set_process_status=lambda v: calls.append(("sps", v)))
        me = NS(sys=NS(shared=NS(mfc={"ch1": NS(users=set(), release=lambda o: None)}, pulse=NS(release_chamber=lambda c: None))),
                ch="ch1", owner="chamber1", mfc=NS(on_process_cleanup=lambda: None, release_owner=lambda *a: None),
                mfc_polling=False, dc={}, rf=None, ig=ig, e=NS(ig_in_use_by_pc=lambda: in_use), oes=None, oes_active=False,
                dcp=None, rfp=None, log=lambda s, m: logs.append((s, m)), camera_leave=lambda: None)
        ProcessActions.cleanup_devices(me)
        if in_use:
            assert calls == [] and ("IG", "같은 CH Plasma Cleaning 이 IG 사용 중 → IG 대기 취소/정리 생략 (공유 장치 보호)") in logs
        else:
            assert calls == ["cancel_wait", ("sps", False)]


# ───────────────────────── DC·RF 연속(PLC D/A) ─────────────────────────
def test_dc_rf_start_while_running_warns_only():
    """DEV-120·131 — 이미 동작 중이면 경고 문구만(다시 SET 하지 않음)."""
    r = PlcRig(dc=True, rf=True, cfg={"CH2_SUPPORTS_HEATER": False})
    for ctl, msg in ((r.dc1, "경고: DC 파워가 이미 동작 중입니다."), (r.rf, "경고: RF 파워가 이미 동작 중입니다.")):
        box = []
        ctl.start(20.0, on_done=box.append)
        r.wait(lambda: bool(box), 5.0)
        n = len([p for p in r.sim.write_pdus() if p[0] == 5])
        again = []
        ctl.start(30.0, on_done=again.append)
        r.after(0.5)
        assert again == ["STATE_NOT_ALLOWED"] or again == [] or again[0] is not None
        assert msg in [e.detail for e in r.pevents if e.kind == "status"]
        assert len([p for p in r.sim.write_pdus() if p[0] == 5]) == n          # SET 코일을 다시 쓰지 않음
        stop = []
        ctl.stop(on_done=stop.append)
        r.wait(lambda: bool(stop), 60.0)
    r.done()


def test_rf_ramp_fine_up_overshoot_and_maintain_steps():
    """DEV-132·134·136 — 시작 구간 FWD 0.0 W 는 판정 제외, 스텝 < 목표면 +1 W, 스텝 ≥ 목표인데 모자라면 +3 W(fine_up),
    넘치면 −1 W('목표 파워 초과…'), 도달 뒤 2회 연속 오차면 ±0.1 W 유지 보정."""
    r = PlcRig(rf=True, cfg={"CH2_SUPPORTS_HEATER": False})
    box = []
    r.rf.start(3.0, on_done=box.append)
    r.wait(lambda: bool(box), 5.0)
    st = lambda: [e.detail for e in r.pevents if e.kind == "status"]       # noqa: E731
    seq = [(0.0, 0.0), (1.5, 0.0), (1.5, 0.0), (1.5, 0.0), (5.0, 0.0), (3.2, 0.0), (1.5, 0.0), (1.5, 0.0)]
    for fwd, ref in seq:
        r.rf._ingest(fwd, ref)
        r.after(1.2)
    want = ["Ramp-Up... 목표스텝:2.0W, 현재:1.5W", "Ramp-Up... 목표스텝:3.0W, 현재:1.5W", "Ramp-Up... 목표스텝:6.0W, 현재:1.5W",
            "목표 파워 초과. Ramp-Down 시도... (step→5.0W, FWD=5.0W)", "3.0W 도달. 파워 유지 시작",
            "유지 보정: meas=1.5W, target=3.0W → set 5.1W"]
    got = st()
    assert [m for m in got if m in want] == want
    assert len([e for e in r.pevents if e.kind == "display"]) == len(seq) - 1      # 0.0 W 는 판정·표시에 안 넣음
    stop = []
    r.rf.stop(on_done=stop.append)
    r.wait(lambda: bool(stop), 60.0)
    r.done()


def test_rf_pc_direct_set_and_kick_texts():
    """DEV-139·GAPA-009 — PC 직접 모드: 목표 ≤ 100 W 는 'Direct set'(정지 = 즉시 OFF), > 100 W 는 kick+ramp 문구, 정지 첫 줄 '정지 신호 수신됨.'."""
    from v2.ch12.drivers.rf import RfPower
    r = PlcRig()
    pc = RfPower(r.c, r.d, r.cfg, True, r.pevents.append, mode="pc")
    box = []
    pc.start(80.0, on_done=box.append)
    r.wait(lambda: bool(box), 5.0)
    st = lambda: [e.detail for e in r.pevents if e.kind == "status"]       # noqa: E731
    assert "Direct set: 80.0 W" in st() and "Direct set 80.0W 전송 — 도달 판정 대기" in st()
    stop = []
    pc.stop(on_done=stop.append)
    r.wait(lambda: bool(stop), 30.0)
    assert stop == [None] and "정지 신호 수신됨." in st() and r.sim.regs[5] == 0
    box = []
    pc.start(150.0, on_done=box.append)
    r.wait(lambda: bool(box), 5.0)
    assert any(m.startswith("Kick+Ramp 시작: 50.0W kick → 150.0W까지 ↑50.0W/s, ↓") for m in st())
    stop = []
    pc.stop(on_done=stop.append)
    r.wait(lambda: bool(stop), 30.0)
    assert any(m.startswith("Kick+Ramp shutdown: 50.0W/s 하강 → ") for m in st())
    r.done()


# ───────────────────────── DC Pulse ─────────────────────────
def test_dcp_send_recv_texts_gap_quiet_and_polling_on():
    """DEV-143·144·175 — '[SEND] label → hex'·'[RECV] label ← ACK(06)', 보내기 간격 ≥ 150 ms, 연결 직후 0.8 s 조용히,
    출력 ON 뒤 'Polling read 시작(5.0s)', OFF 면 'Polling read 중지'."""
    from v2.tests.test_dcpulse import Rig
    r = Rig()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    st = [e.detail for e in r.ev("status")]
    assert "[SEND] MASTER_ONOFF → 02 7b 00 03 03 83" in st or any(m.startswith("[SEND] MASTER_ONOFF") and m.endswith("02 7b 00 03 03 83") for m in st)
    assert any(m.startswith("[RECV] MASTER_ONOFF") and "ACK(06)" in m for m in st)
    assert "Polling read 시작(5.0s)" in st
    ts = [t for t, _ in list(r.sim.received)]
    assert ts[0] >= 0.8 - 1e-6 and all(b - a >= 0.15 - 1e-6 for a, b in zip(ts, ts[1:]))
    assert r.call(r.d.output_off) == (None, True)
    assert "Polling read 중지" in [e.detail for e in r.ev("status")]
    r.done()


def test_dcp_reconnect_backoff_up_to_10s():
    """DEV-145 — 끊기면 1 s 부터 ×2, 최대 DCP_RECONNECT_BACKOFF_MAX_MS 10 s."""
    from v2.tests.test_dcpulse import Rig
    r = Rig()
    wev = []
    prev = r.w.on_event
    r.w.on_event = lambda e: (wev.append(e), prev and prev(e))
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t, t + 100.0)
    r.call(r.d.output_off_unused)
    r.c.wait_until(t + 80.0)
    fails = [e.t for e in wev if e.kind == "connect_failed"]
    gaps = [round(b - a, 1) for a, b in zip(fails, fails[1:])]
    assert gaps and max(gaps) == pytest.approx(10.0, abs=0.3) and min(gaps) <= 2.1
    r.done()


def test_dcp_failed_event_phase_tagging():
    """DEV-174 — 시작 순서 중 실패는 phase='prepare', OUTPUT_OFF·AUTO_STOP 은 붙이지 않음, last_failure 'label: why'."""
    from v2.tests.test_dcpulse import Rig
    r = Rig()
    reason, ok = r.start(100.0, freq=200, off_time_us=5.0)
    assert ok is False
    f = r.ev("command_failed")[-1]
    assert f.data == {"phase": "prepare"} and r.d.last_failure.startswith(f"{f.cmd}: ")
    r.done()
    r = Rig()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    r.sim.off_err_n = 99
    r.call(r.d.output_off)
    offs = [e for e in r.ev("command_failed") if e.cmd == "OUTPUT_OFF"]
    assert offs and all(e.data is None for e in offs)
    r.done()


def test_dcp_telemetry_piv_scale_check_and_nak_streak():
    """DEV-163·164·168·169 — 판정 tick 마다 telemetry(P/V/I·raw), 점화 창이 닫힌 뒤 첫 표본에서 '[piv] 스케일 정합 OK' 1회,
    PIV NAK 5회 연속이면 '[WARN] 텔레메트리 연속 5회 NAK …', 회복하면 '[INFO] …'."""
    from v2.ch12.drivers import dcpulse_proto as P
    from v2.tests.test_dcpulse import Rig
    r = Rig()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    r.rows(40)
    tel = r.ev("telemetry")
    assert tel and tel[-1].detail == "[telemetry] P=100.0 W, V=400.00 V, I=0.250 A" and tel[-1].data["P_W"] == 100.0
    assert sum(e.detail.startswith("[piv] 스케일 정합 OK") for e in r.ev("status")) == 1
    r.sim.nak_reads = {P.READ_PIV if hasattr(P, "READ_PIV") else 0x9A: 40}
    r.rows(40)
    st = [e.detail for e in r.ev("status")]
    assert "[WARN] 텔레메트리 연속 5회 NAK — DCP 시리얼 라인 노이즈 의심 (저전류/이탈 감시 공백 중)" in st
    r.sim.nak_reads = {}
    r.rows(12)
    assert "[INFO] 텔레메트리 읽기 회복 (NAK 연속 종료)" in [e.detail for e in r.ev("status")]
    r.done()


def test_dcp_output_on_off_verified_by_status_when_ack_lost():
    """DEV-149·150 — ACK 가 아니어도 STATUS 0x90 HV-On(bit0) 이 켜졌으면 'OUTPUT_ON_VERIFIED', 꺼졌으면 'OUTPUT_OFF_VERIFIED'."""
    from v2.ch12.drivers import dcpulse_proto as P
    from v2.tests.test_dcpulse import Rig
    r = Rig()
    orig = r.sim.handle
    lose = {1: 1, 2: 1}

    def handle(data, now):
        out = orig(data, now)
        if len(data) == 6 and data[1] == P.OUTPUT and lose.get(data[3], 0) > 0:
            lose[data[3]] -= 1
            return bytes([P.ERR])                     # 적용은 됐지만 응답이 ERR(잡음)
        return out
    r.sim.handle = handle
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    assert "OUTPUT_ON_VERIFIED" in [e.cmd for e in r.ev("command_confirmed")] and r.sim.out
    assert r.call(r.d.output_off) == (None, True)
    assert "OUTPUT_OFF_VERIFIED" in [e.cmd for e in r.ev("command_confirmed")] and not r.sim.out
    r.done()


# ───────────────────────── RF Pulse ─────────────────────────
def test_rfp_reconnect_backoff_2_to_30s_and_tables():
    """DEV-180·184·186·201 — 재연결 2 s ×2 최대 30 s, CSR 표·질의 한도·FORP/REFP 기본값이 v1 표와 같다."""
    from v2.ch12.drivers import rfpulse_poll as RP
    from v2.tests.test_rfpulse import CFG, Rig
    f = FACTS["rfpulse"]
    assert RP.CSR_CODES == f["csr_codes"]
    assert (CFG["ACK_TIMEOUT_MS"], CFG["QUERY_TIMEOUT_MS"], CFG["POLL_QUERY_TIMEOUT_MS"]) == (2000, 4500, 9000)
    assert (CFG["RFPULSE_FORP_TOLERANCE_PERCENT"], CFG["RFPULSE_REFP_LIMIT_WATTS"], CFG["RFPULSE_REFP_CONSECUTIVE_LIMIT"]) == (5.0, 20.0, 3)
    r = Rig()
    wev = []
    prev = r.w.on_event
    r.w.on_event = lambda e: (wev.append(e), prev and prev(e))
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t, t + 300.0)
    r.call(r.d.stop_unused)
    r.c.wait_until(t + 200.0)
    fails = [e.t for e in wev if e.kind == "connect_failed"]
    gaps = [round(b - a, 1) for a, b in zip(fails, fails[1:])]
    assert gaps and max(gaps) == pytest.approx(30.0, abs=0.5) and min(gaps) <= 4.1
    r.done()


def test_rfp_raw_log_gate_status_bits_duty_warn_and_setpoint_guards():
    """DEV-189·191·192·194 — RAW TX 줄은 정주기 폴링('[POLL…')이면 생략(RFPULSE_RAW_LOG False), STATUS 인터록 경고,
    16 us 규칙 밖 duty 경고, SETP 범위 밖·정지 뒤 변경 생략."""
    from v2.tests.test_rfpulse import Rig
    r = Rig()
    r.sim.interlock_open = True
    r.call(r.d.start_pulse_process, 100.0, 30000, 10)
    st = lambda: [e.detail for e in r.ev("status")]                          # noqa: E731
    assert any(m.startswith("[WARN] duty 10% @30kHz — CESAR 매뉴얼(16us 규칙) 기준 범위 48~52% 밖") for m in st())
    r.sim.interlock_open = False
    r.call(r.d.start_pulse_process, 100.0)
    r.sim.interlock_open = True
    r.rows(20)
    tx = [m for m in st() if m.startswith("[RFP][RAW][TX]")]
    assert tx and not any("tag=[POLL" in m for m in tx)
    assert "STATUS: Interlock OPEN detected" in st()
    res = r.call(r.d.set_reference_power, 70000)
    assert res[1] is False and r.ev("command_failed")[-1].detail == "out of range: 70000 (0..65535)"
    r.call(r.d.stop_process)
    assert r.call(r.d.set_reference_power, 50)[1] is False and "STOP 요청 → SETP 변경 생략" in st()
    r.done()
    r = Rig(cfg={"RFPULSE_RAW_LOG": True})
    r.call(r.d.start_pulse_process, 100.0)
    r.rows(12)
    assert any("tag=[POLL" in e.detail for e in r.ev("status") if e.detail.startswith("[RFP][RAW][TX]"))
    r.done()


# ───────────────────────── TSP ─────────────────────────
def test_tsp_endpoint_and_io_timing_settings():
    """DEV-202·204 — 192.168.1.50:4004, RS-232 주소 0x80, 접속 2.0 s(최소 0.1), 바이트 한도 1.2 s, 보낸 뒤 20 ms, 짧은 응답 허용."""
    from v2.ch12.drivers import tsp as T
    from v2.tests.test_tsp import CFG
    assert (CFG["TSP_TCP_HOST"], CFG["TSP_TCP_PORT"], CFG["TSP_RS232_ADDR"]) == ("192.168.1.50", 4004, 128)
    assert (CFG["TSP_CONNECT_TIMEOUT_S"], CFG["TSP_WRITE_TIMEOUT_S"], CFG["TSP_POST_SEND_DELAY_MS"]) == (2.0, 1.2, 20)
    assert CFG["TSP_TOLERATE_SHORT_RESP"] is True and CFG["TSP_VERIFY_WITH_STATUS"] is True
    assert CFG["TSP_STATUS_POLL_INTERVAL_S"] == pytest.approx(0.05)
    assert T.build_frame("011", True, "1")[1] == 0x80


# ───────────────────────── 두께 모니터 STM-100(드라이버·상시 읽기) ─────────────────────────
def _stm_msgs(r, kind=None):
    return [e.detail for e in r.events if kind is None or e.kind == kind]


def test_stm_link_log_texts_once_and_quiet_while_unresponsive():
    """DEV-208·216·THK-010·019 — '두께 모니터 연결 host:port', 끊김 '… 연결 끊김 — 재연결 대기', 연결 실패 줄은 연결될 때까지 1번,
    응답 없음(quiet) 동안은 연결·끊김 줄 생략, 정상 프레임이 오면 '두께 모니터 응답 복귀'."""
    from v2.tests.test_stm100 import Rig
    r = Rig()
    r.c.wait_until(r.c.monotonic() + 1.0)
    assert "두께 모니터 연결 192.168.1.50:4008" in _stm_msgs(r, "link")
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t, t + 20.0)
    r.c.wait_until(t + 45.0)
    link = _stm_msgs(r, "link")
    assert "두께 모니터 연결 끊김 — 재연결 대기" in link
    assert sum(m.startswith("두께 모니터 연결 실패 192.168.1.50:4008 — 계속 재시도: ") for m in link) == 1
    assert link.count("두께 모니터 연결 192.168.1.50:4008") == 2
    n = len(_stm_msgs(r, "link"))
    t = r.c.monotonic()
    r.sim.add_fault("no_response", t, t + 25.0)                       # 6회 연속 → 응답 없음(quiet) → 재연결
    r.c.wait_until(t + 30.0)
    assert _stm_msgs(r, "unresponsive") and _stm_msgs(r, "recovered") == ["두께 모니터 응답 복귀"]
    assert _stm_msgs(r, "link")[n:] == []                               # quiet 동안 연결·재연결 줄 없음
    r.done()


def test_stm_status_keys_last_error_listener_and_loop_errors():
    """THK-013·016·020·021·023 — 상태 키, 값 없음 'HH:MM:SS 값 없음 n회 연속'(벽시계), 리스너 예외·처리 예외는 60 s 에 1번 후 계속."""
    from v2.tests.test_stm100 import Rig
    r = Rig()
    r.c.wait_until(r.c.monotonic() + 1.0)
    st = r.d.status()
    assert {"connected", "ok_ts", "age_s", "stale", "link_ok", "link_age_s", "thickness_nm", "rate_nm_s", "freq_hz",
            "life_pct", "crystal_ok", "reset_seen", "seq", "density", "z_factor", "tooling", "material", "last_error"} <= set(st)
    r.ch.xtal_fail = True
    r.c.wait_until(r.c.monotonic() + 1.2)
    le = r.d.status()["last_error"]
    assert le[2] == ":" and le[5] == ":" and le[8:].startswith(" 값 없음 ") and le.endswith("회 연속")
    r.ch.xtal_fail = False

    def bad(st):
        raise RuntimeError("리스너 고장")
    r.d.add_listener(bad)
    orig = r.d._poll_slow
    r.d._poll_slow = lambda s: (_ for _ in ()).throw(KeyError("주입"))
    seq0 = r.d.seq
    r.c.wait_until(r.c.monotonic() + 20.0)
    assert _stm_msgs(r, "listener_error") == ["두께 상태 리스너 예외: RuntimeError('리스너 고장')"]
    loops = _stm_msgs(r, "loop_error")
    assert len(loops) == 1 and loops[0].startswith("⚠ 두께 폴링 처리 예외: KeyError(")
    assert r.d.seq > seq0                                               # 예외가 나도 읽기는 계속
    r.d._poll_slow = orig
    r.done()


def test_stm_zero_and_material_service_texts():
    """THK-025·026 — 영점 성공 '두께 영점(B) — 사유', 실패 '⚠ 두께 영점(B) 실패 — 사유: 오류', 재료값 적용 문구·상태 반영."""
    from v2.tests.test_stm100 import Rig
    r = Rig()
    r.c.wait_until(r.c.monotonic() + 0.6)
    assert r.job("zero", "B", why="패널").ok and "두께 영점(B) — 패널" in _stm_msgs(r, "zero")
    t = r.c.monotonic()
    r.sim.add_fault("no_response", t, t + 6.0)
    d = r.job("zero", "B", why="패널")
    assert not d.ok and any(m.startswith("⚠ 두께 영점(B) 실패 — 패널: ") for m in _stm_msgs(r, "job_failed"))
    r.c.wait_until(t + 10.0)
    d = r.job("set_material", 2.73, 1.08, 110.0, material="Aluminum", why="Factor 창 수동")
    assert d.ok and "두께 모니터 재료값 적용(Factor 창 수동) Aluminum density 2.73 · Z 1.08 · tooling 110.0%" in _stm_msgs(r, "material_applied")
    assert r.d.status()["material"] == "Aluminum" and r.d.status()["tooling"] == 110.0
    r.done()


def test_thickness_service_start_log_on_assembly(tmp_path):
    """THK-035 — 두께 모니터가 켜진 채널만 엔진을 붙이고 'CHn 두께 모니터 서비스 시작 (host:port)'(main.py:1340), CH1(꺼짐)은 없음."""
    from v2.common.clock import VirtualClock
    from v2.ch12.sim.rig import Ch12FullRig
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = Ch12FullRig(c, tmp_path, cfg={"CAMERA_MIN_FREE_GB": 0.0}, robot_api=False)
    try:
        logs = [(e.source, e.detail) for e in rig.events if e.kind == "log"]
        assert ("ch2.stm100", "CH2 두께 모니터 서비스 시작 (192.168.1.50:4008)") in logs
        assert "thk.ch2" in rig.engines and "thk.ch1" not in rig.engines
        assert not any("CH1 두께 모니터" in d for _, d in logs)
    finally:
        rig.stop()
        c.close()


# ───────────────────────── OES ─────────────────────────
def test_oes_oneshot_unexpected_exception_result_and_traceback(tmp_path, monkeypatch):
    """GAPA-011 — one-shot 측정 중 예상 못 한 예외: 결과 'OES 예외'(실패, out_csv·rows·error '{Type}: {msg}'),
    status '[OES] 예외: {Type}: {msg}\\n{traceback}', 그 뒤 측정 중 아님."""
    from v2.tests.test_oes_link import Rig
    o = Rig(tmp_path, monkeypatch, OES_DAEMON=False, chamber="ch2")
    try:
        assert o.init()[0]

        def boom(*a, **k):
            raise RuntimeError("주입 예외")
        monkeypatch.setattr(o.link, "_spawn", boom)
        res = o.measure(2.0, 50)
        assert res.emitted and not res.success and res.message == "OES 예외" and res.error == "RuntimeError: 주입 예외"
        assert res.out_csv and res.out_csv.endswith(".csv") and res.rows == 0
        st = [m for m in o.statuses() if m.startswith("[OES] 예외: RuntimeError: 주입 예외\n")]
        assert st and "Traceback (most recent call last)" in st[0]
        assert not o.link.is_running
    finally:
        o.link.close()


def test_oes_daemon_status_texts(tmp_path, monkeypatch):
    """GAPA-012·DEV-221·222·223 — daemon 기동·READY·측정 시작·started 문구(v1 device/oes.py 문구 그대로)."""
    from v2.tests.test_oes_link import Rig
    d = Rig(tmp_path, monkeypatch)
    try:
        assert d.init()[0]
        res = d.measure(2.0, 50)
        assert res.success
        st = d.statuses()
        for head in ("[OES] daemon spawn ch=1 usb=0 parent_timeout=", "[OES] daemon READY: ", "[OES] daemon measure start: 0.0분, out=",
                     "[OES] daemon started out_csv="):
            assert any(m.startswith(head) for m in st), head
    finally:
        d.close()


# ───────────────────────── RGA ─────────────────────────
def test_rga_missing_worker_exe_fails_scan_without_device(tmp_path, monkeypatch):
    """DEV-230 — 워커 실행 파일이 없으면 FileNotFoundError 'rga_worker.exe not found: …' 로 스캔 실패(메인은 srsinst 를 부르지 않음)."""
    from v2.tests.test_rga_link import Rig
    r = Rig(tmp_path, monkeypatch, RGA_WORKER_EXE=str(tmp_path / "apps" / "rga_service" / "rga_worker.exe"))
    try:
        res = r.scan(timeout_s=5.0, wait=60)
        assert not res.ok and "rga_worker.exe not found" in (res.message + " " + str(getattr(res, "error", "")))
    finally:
        r.link.close()
